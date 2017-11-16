from lxml import etree

from cStringIO import StringIO
import itertools
import datetime
import os
import re
import logging
from flask.ext.babel import lazy_gettext as _

from nose.tools import set_trace

from sqlalchemy import or_

from circulation import (
    FulfillmentInfo,
    HoldInfo,
    LoanInfo,
    BaseCirculationAPI,
)
from core.model import (
    CirculationEvent,
    Collection,
    DataSource,
    DeliveryMechanism,
    Edition,
    ExternalIntegration,
    get_one,
    Identifier,
    LicensePool,
    Representation,
    get_one_or_create,
    Loan,
    Hold,
    Session,
    Timestamp,
)

from core.monitor import (
    CollectionMonitor,
    IdentifierSweepMonitor,
)
from core.util.xmlparser import XMLParser
from core.util.http import (
    BadResponseException
)
from core.bibliotheca import (
    MockBibliothecaAPI as BaseMockBibliothecaAPI,
    BibliothecaAPI as BaseBibliothecaAPI,
    BibliothecaBibliographicCoverageProvider
)

from circulation_exceptions import *
from core.analytics import Analytics

class BibliothecaAPI(BaseBibliothecaAPI, BaseCirculationAPI):

    NAME = ExternalIntegration.BIBLIOTHECA
    SETTINGS = [
        { "key": ExternalIntegration.USERNAME, "label": _("Account ID") },
        { "key": ExternalIntegration.PASSWORD, "label": _("Account Key") },
        { "key": Collection.EXTERNAL_ACCOUNT_ID_KEY, "label": _("Library ID") },
    ] + BaseCirculationAPI.SETTINGS

    LIBRARY_SETTINGS = BaseCirculationAPI.LIBRARY_SETTINGS + [
        BaseCirculationAPI.DEFAULT_LOAN_DURATION_SETTING
    ]

    MAX_AGE = datetime.timedelta(days=730).seconds
    CAN_REVOKE_HOLD_WHEN_RESERVED = False
    SET_DELIVERY_MECHANISM_AT = None

    SERVICE_NAME = "Bibliotheca"

    # Create a lookup table between common DeliveryMechanism identifiers
    # and Overdrive format types.
    adobe_drm = DeliveryMechanism.ADOBE_DRM
    findaway_drm = DeliveryMechanism.FINDAWAY_DRM
    delivery_mechanism_to_internal_format = {
        (Representation.EPUB_MEDIA_TYPE, adobe_drm): 'ePub',
        (Representation.PDF_MEDIA_TYPE, adobe_drm): 'PDF',
        (Representation.MP3_MEDIA_TYPE, findaway_drm) : 'MP3'
    }
    internal_format_to_delivery_mechanism = dict(
        [v,k] for k, v in delivery_mechanism_to_internal_format.items()
    )

    def get_events_between(self, start, end, cache_result=False):
        """Return event objects for events between the given times."""
        start = start.strftime(self.ARGUMENT_TIME_FORMAT)
        end = end.strftime(self.ARGUMENT_TIME_FORMAT)
        url = "data/cloudevents?startdate=%s&enddate=%s" % (start, end)
        if cache_result:
            max_age = self.MAX_AGE
        else:
            max_age = None
        response = self.request(url, max_age=max_age)
        if cache_result:
            self._db.commit()
        try:
            events = EventParser().process_all(response.content)
        except Exception, e:
            self.log.error(
                "Error parsing Bibliotheca response content: %s", response.content,
                exc_info=e
            )
            raise e
        return events

    def circulation_request(self, identifiers):
        url = "/circulation/items/" + ",".join(identifiers)
        response = self.request(url)
        if response.status_code != 200:
            raise BadResponseException.bad_status_code(
                self.full_url(url), response
            )
        return response

    def get_circulation_for(self, identifiers):
        """Return circulation objects for the selected identifiers."""
        response = self.circulation_request(identifiers)
        for circ in CirculationParser().process_all(response.content):
            if circ:
                yield circ

    def update_availability(self, licensepool):
        """Update the availability information for a single LicensePool."""
        monitor = BibliothecaCirculationSweep(
            self._db, licensepool.collection, api_class=self
        )
        return monitor.process_items([licensepool.identifier])

    def patron_activity(self, patron, pin):
        patron_id = patron.authorization_identifier
        path = "circulation/patron/%s" % patron_id
        response = self.request(path)
        collection = self.collection
        return PatronCirculationParser(self.collection).process_all(response.content)

    TEMPLATE = "<%(request_type)s><ItemId>%(item_id)s</ItemId><PatronId>%(patron_id)s</PatronId></%(request_type)s>"

    def checkout(
            self, patron_obj, patron_password, licensepool, 
            delivery_mechanism
    ):

        """Check out a book on behalf of a patron.

        :param patron_obj: a Patron object for the patron who wants
        to check out the book.

        :param patron_password: The patron's alleged password.  Not
        used here since Bibliotheca trusts Simplified to do the check ahead of
        time.

        :param licensepool: LicensePool for the book to be checked out.

        :return: a LoanInfo object
        """
        bibliotheca_id = licensepool.identifier.identifier
        patron_identifier = patron_obj.authorization_identifier
        args = dict(request_type='CheckoutRequest',
                    item_id=bibliotheca_id, patron_id=patron_identifier)
        body = self.TEMPLATE % args 
        response = self.request('checkout', body, method="PUT")
        if response.status_code == 201:
            # New loan
            start_date = datetime.datetime.utcnow()
        elif response.status_code == 200:
            # Old loan -- we don't know the start date
            start_date = None
        else:
            # Error condition.
            error = ErrorParser().process_all(response.content)
            if isinstance(error, AlreadyCheckedOut):
                # It's already checked out. No problem.
                pass
            else:
                raise error

        # At this point we know we have a loan.
        loan_expires = CheckoutResponseParser().process_all(response.content)
        loan = LoanInfo(
            licensepool.collection, DataSource.BIBLIOTHECA,
            licensepool.identifier.type,
            licensepool.identifier.identifier,
            start_date=None,
            end_date=loan_expires,
        )
        return loan

    def fulfill(self, patron, password, pool, internal_delivery):
        media_type, drm_scheme = self.internal_format_to_delivery_mechanism.get(
            internal_delivery, internal_delivery
        )
        if drm_scheme == DeliveryMechanism.FINDAWAY_DRM:
            fulfill_method = self.get_audio_fulfillment_file
            # The provided media type is application/json, which
            # is too vague for us.
            force_content_type = DeliveryMechanism.FINDAWAY_DRM
        else:
            fulfill_method = self.get_fulfillment_file
            force_content_type = None
        response = fulfill_method(
            patron.authorization_identifier, pool.identifier.identifier
        )
        return FulfillmentInfo(
            pool.collection, DataSource.BIBLIOTHECA,
            pool.identifier.type,
            pool.identifier.identifier,
            content_link=None,
            content_type=(force_content_type
                          or response.headers.get('Content-Type')),
            content=response.content,
            content_expires=None,
        )

    def get_fulfillment_file(self, patron_id, bibliotheca_id):
        args = dict(request_type='ACSMRequest',
                   item_id=bibliotheca_id, patron_id=patron_id)
        body = self.TEMPLATE % args 
        return self.request('GetItemACSM', body, method="PUT")

    def get_audio_fulfillment_file(self, patron_id, bibliotheca_id):
        args = dict(request_type='AudioFulfillmentRequest', 
                    item_id=bibliotheca_id, patron_id=patron_id)
        body = self.TEMPLATE % args 
        return self.request('GetItemAudioFulfillment', body, method="POST")
    
    def checkin(self, patron, pin, licensepool):
        patron_id = patron.authorization_identifier
        item_id = licensepool.identifier.identifier
        args = dict(request_type='CheckinRequest',
                   item_id=item_id, patron_id=patron_id)
        body = self.TEMPLATE % args 
        return self.request('checkin', body, method="PUT")

    def place_hold(self, patron, pin, licensepool, 
                   hold_notification_email=None):
        """Place a hold.

        :return: a HoldInfo object.
        """
        patron_id = patron.authorization_identifier
        item_id = licensepool.identifier.identifier
        args = dict(request_type='PlaceHoldRequest',
                   item_id=item_id, patron_id=patron_id)
        body = self.TEMPLATE % args 
        response = self.request('placehold', body, method="PUT")
        if response.status_code in (200, 201):
            start_date = datetime.datetime.utcnow()
            end_date = HoldResponseParser().process_all(response.content)
            return HoldInfo(
                licensepool.collection, DataSource.BIBLIOTHECA,
                licensepool.identifier.type,
                licensepool.identifier.identifier,
                start_date=start_date, 
                end_date=end_date,
                hold_position=None
            )
        else:
            if not response.content:
                raise CannotHold()
            error = ErrorParser().process_all(response.content)
            if isinstance(error, Exception):
                raise error
            else:
                raise CannotHold(error)

    def release_hold(self, patron, pin, licensepool):
        patron_id = patron.authorization_identifier
        item_id = licensepool.identifier.identifier        
        args = dict(request_type='CancelHoldRequest',
                   item_id=item_id, patron_id=patron_id)
        body = self.TEMPLATE % args 
        response = self.request('cancelhold', body, method="PUT")
        if response.status_code in (200, 404):
            return True
        else:
            raise CannotReleaseHold()

    def apply_circulation_information_to_licensepool(self, circ, pool, analytics=None):
        """Apply the output of CirculationParser.process_one() to a
        LicensePool.
        
        TODO: It should be possible to have CirculationParser yield 
        CirculationData objects instead and to replace this code with
        CirculationData.apply(pool)
        """
        if pool.presentation_edition:
            e = pool.presentation_edition
            self.log.info("Updating %s (%s)", e.title, e.author)
        else:
            self.log.info(
                "Updating unknown work %s", pool.identifier
            )
        # Update availability and send out notifications.
        pool.update_availability(
            circ.get(LicensePool.licenses_owned, 0),
            circ.get(LicensePool.licenses_available, 0),
            circ.get(LicensePool.licenses_reserved, 0),
            circ.get(LicensePool.patrons_in_hold_queue, 0),
            analytics,
        )


class DummyBibliothecaAPIResponse(object):

    def __init__(self, response_code, headers, content):
        self.status_code = response_code
        self.headers = headers
        self.content = content

class MockBibliothecaAPI(BaseMockBibliothecaAPI, BibliothecaAPI):
    pass


class BibliothecaParser(XMLParser):

    INPUT_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"

    def parse_date(self, value):
        """Parse the string Bibliotheca sends as a date.

        Usually this is a string in INPUT_TIME_FORMAT, but it might be None.
        """
        if not value:
            value = None
        else:
            try:
                value = datetime.datetime.strptime(
                    value, self.INPUT_TIME_FORMAT
                )
            except ValueError, e:
                logging.error(
                    'Unable to parse Bibliotheca date: "%s"', value,
                    exc_info=e
                )
                value = None
        return value

    def date_from_subtag(self, tag, key, required=True):
        if required:
            value = self.text_of_subtag(tag, key)
        else:
            value = self.text_of_optional_subtag(tag, key)
        return self.parse_date(value)


class CirculationParser(BibliothecaParser):

    """Parse Bibliotheca's circulation XML dialect into something we can apply to a LicensePool."""

    def process_all(self, string):
        for i in super(CirculationParser, self).process_all(
                string, "//ItemCirculation"):
            yield i

    def process_one(self, tag, namespaces):
        if not tag.xpath("ItemId"):
            # This happens for events associated with books
            # no longer in our collection.
            return None

        def value(key):
            return self.text_of_subtag(tag, key)

        def intvalue(key):
            return self.int_of_subtag(tag, key)

        identifiers = {}
        item = { Identifier : identifiers }

        identifiers[Identifier.BIBLIOTHECA_ID] = value("ItemId")
        identifiers[Identifier.ISBN] = value("ISBN13")
        
        item[LicensePool.licenses_owned] = intvalue("TotalCopies")
        try:
            item[LicensePool.licenses_available] = intvalue("AvailableCopies")
        except IndexError:
            logging.warn("No information on available copies for %s",
                         identifiers[Identifier.BIBLIOTHECA_ID]
                     )

        # Counts of patrons who have the book in a certain state.
        for bibliotheca_key, simplified_key in [
                ("Holds", LicensePool.patrons_in_hold_queue),
                ("Reserves", LicensePool.licenses_reserved)
        ]:
            t = tag.xpath(bibliotheca_key)
            if t:
                t = t[0]
                value = int(t.xpath("count(Patron)"))
                item[simplified_key] = value
            else:
                logging.warn("No circulation information provided for %s %s",
                             identifiers[Identifier.BIBLIOTHECA_ID], bibliotheca_key)
        return item


class BibliothecaException(Exception):
    pass

class WorkflowException(BibliothecaException):
    def __init__(self, actual_status, statuses_that_would_work):
        self.actual_status = actual_status
        self.statuses_that_would_work = statuses_that_would_work

    def __str__(self):
        return "Book status is %s, must be: %s" % (
            self.actual_status, ", ".join(self.statuses_that_would_work))

class ErrorParser(BibliothecaParser):
    """Turn an error document from the Bibliotheca web service into a CheckoutException"""

    wrong_status = re.compile(
        "the patron document status was ([^ ]+) and not one of ([^ ]+)")

    loan_limit_reached = re.compile(
        "Patron cannot loan more than [0-9]+ document"
    )
    
    hold_limit_reached = re.compile(
        "Patron cannot have more than [0-9]+ hold"
    )

    error_mapping = {
        "The patron does not have the book on hold" : NotOnHold,
        "The patron has no eBooks checked out" : NotCheckedOut,
    }

    def process_all(self, string):
        try:
            for i in super(ErrorParser, self).process_all(
                    string, "//Error"):
                return i
        except Exception, e:
            # The server sent us an error with an incorrect or
            # nonstandard syntax.
            return RemoteInitiatedServerError(
                string, BibliothecaAPI.SERVICE_NAME
            )

        # We were not able to interpret the result as an error.
        # The most likely cause is that the Bibliotheca app server is down.
        return RemoteInitiatedServerError(
            "Unknown error", BibliothecaAPI.SERVICE_NAME,
        )

    def process_one(self, error_tag, namespaces):
        message = self.text_of_optional_subtag(error_tag, "Message")
        if not message:
            return RemoteInitiatedServerError(
                "Unknown error", BibliothecaAPI.SERVICE_NAME,
            )

        if message in self.error_mapping:
            return self.error_mapping[message](message)
        if message in ('Authentication failed', 'Unknown error'):
            # 'Unknown error' is an unknown error on the Bibliotheca side.
            #
            # 'Authentication failed' could _in theory_ be an error on
            # our side, but if authentication is set up improperly we
            # actually get a 401 and no body. When we get a real error
            # document with 'Authentication failed', it's always a
            # transient error on the Bibliotheca side. Possibly some
            # authentication internal to Bibliotheca has failed? Anyway, it
            # happens relatively frequently.
            return RemoteInitiatedServerError(
                message, BibliothecaAPI.SERVICE_NAME
            )

        m = self.loan_limit_reached.search(message)
        if m:
            return PatronLoanLimitReached(message)

        m = self.hold_limit_reached.search(message)
        if m:
            return PatronHoldLimitReached(message)

        m = self.wrong_status.search(message)
        if not m:
            return BibliothecaException(message)
        actual, expected = m.groups()
        expected = expected.split(",")

        if actual == 'CAN_WISH':
            return NoLicenses(message)

        if 'CAN_LOAN' in expected and actual == 'CAN_HOLD':
            return NoAvailableCopies(message)

        if 'CAN_LOAN' in expected and actual == 'HOLD':
            return AlreadyOnHold(message)

        if 'CAN_LOAN' in expected and actual == 'LOAN':
            return AlreadyCheckedOut(message)

        if 'CAN_HOLD' in expected and actual == 'CAN_LOAN':
            return CurrentlyAvailable(message)

        if 'CAN_HOLD' in expected and actual == 'HOLD':
            return AlreadyOnHold(message)

        if 'CAN_HOLD' in expected:
            return CannotHold(message)

        if 'CAN_LOAN' in expected:
            return CannotLoan(message)

        return BibliothecaException(message)

class PatronCirculationParser(BibliothecaParser):

    """Parse Bibliotheca's patron circulation status document into a list of
    LoanInfo and HoldInfo objects.
    """

    id_type = Identifier.BIBLIOTHECA_ID

    def __init__(self, collection, *args, **kwargs):
        super(PatronCirculationParser, self).__init__(*args, **kwargs)
        self.collection = collection
    
    def process_all(self, string):
        parser = etree.XMLParser()
        root = etree.parse(StringIO(string), parser)
        sup = super(PatronCirculationParser, self)
        loans = sup.process_all(
            root, "//Checkouts/Item", handler=self.process_one_loan)
        holds = sup.process_all(
            root, "//Holds/Item", handler=self.process_one_hold)
        reserves = sup.process_all(
            root, "//Reserves/Item", handler=self.process_one_reserve)

        everything = itertools.chain(loans, holds, reserves)
        return [x for x in everything if x]

    def process_one_loan(self, tag, namespaces):
        return self.process_one(tag, namespaces, LoanInfo)

    def process_one_hold(self, tag, namespaces):
        return self.process_one(tag, namespaces, HoldInfo)

    def process_one_reserve(self, tag, namespaces):
        hold_info = self.process_one(tag, namespaces, HoldInfo)
        hold_info.hold_position = 0
        return hold_info

    def process_one(self, tag, namespaces, source_class):
        if not tag.xpath("ItemId"):
            # This happens for events associated with books
            # no longer in our collection.
            return None

        def datevalue(key):
            value = self.text_of_subtag(tag, key)
            return datetime.datetime.strptime(
                value, BibliothecaAPI.ARGUMENT_TIME_FORMAT)

        identifier = self.text_of_subtag(tag, "ItemId")
        start_date = datevalue("EventStartDateInUTC")
        end_date = datevalue("EventEndDateInUTC")
        a = [self.collection, DataSource.BIBLIOTHECA, self.id_type, identifier,
             start_date, end_date]
        if source_class is HoldInfo:
            hold_position = self.int_of_subtag(tag, "Position")
            a.append(hold_position)
        else:
            # Fulfillment info -- not available from this API
            a.append(None)
        return source_class(*a)

class DateResponseParser(BibliothecaParser):
    """Extract a date from a response."""
    RESULT_TAG_NAME = None
    DATE_TAG_NAME = None

    def process_all(self, string):
        parser = etree.XMLParser()
        root = etree.parse(StringIO(string), parser)
        m = root.xpath("/%s/%s" % (self.RESULT_TAG_NAME, self.DATE_TAG_NAME))
        if not m:
            return None
        due_date = m[0].text
        if not due_date:
            return None
        return datetime.datetime.strptime(
                due_date, EventParser.INPUT_TIME_FORMAT)


class CheckoutResponseParser(DateResponseParser):

    """Extract due date from a checkout response."""
    RESULT_TAG_NAME = "CheckoutResult"
    DATE_TAG_NAME = "DueDateInUTC"


class HoldResponseParser(DateResponseParser):

    """Extract availability date from a hold response."""
    RESULT_TAG_NAME = "PlaceHoldResult"
    DATE_TAG_NAME = "AvailabilityDateInUTC"


class EventParser(BibliothecaParser):

    """Parse Bibliotheca's event file format into our native event objects."""

    EVENT_SOURCE = "Bibliotheca"

    SET_DELIVERY_MECHANISM_AT = BaseCirculationAPI.BORROW_STEP

    # Map Bibliotheca's event names to our names.
    EVENT_NAMES = {
        "CHECKOUT" : CirculationEvent.DISTRIBUTOR_CHECKOUT,
        "CHECKIN" : CirculationEvent.DISTRIBUTOR_CHECKIN,
        "HOLD" : CirculationEvent.DISTRIBUTOR_HOLD_PLACE,
        "RESERVED" : CirculationEvent.DISTRIBUTOR_AVAILABILITY_NOTIFY,
        "PURCHASE" : CirculationEvent.DISTRIBUTOR_LICENSE_ADD,
        "REMOVED" : CirculationEvent.DISTRIBUTOR_LICENSE_REMOVE,
    }

    def process_all(self, string):
        for i in super(EventParser, self).process_all(
                string, "//CloudLibraryEvent"):
            yield i

    def process_one(self, tag, namespaces):
        isbn = self.text_of_subtag(tag, "ISBN")
        bibliotheca_id = self.text_of_subtag(tag, "ItemId")
        patron_id = self.text_of_optional_subtag(tag, "PatronId")

        start_time = self.date_from_subtag(tag, "EventStartDateTimeInUTC")
        end_time = self.date_from_subtag(
            tag, "EventEndDateTimeInUTC", required=False
        )

        bibliotheca_event_type = self.text_of_subtag(tag, "EventType")
        internal_event_type = self.EVENT_NAMES[bibliotheca_event_type]

        return (bibliotheca_id, isbn, patron_id, start_time, end_time,
                internal_event_type)

class BibliothecaCirculationSweep(IdentifierSweepMonitor):
    """Check on the current circulation status of each Bibliotheca book in our
    collection.

    In some cases this will lead to duplicate events being logged,
    because this monitor and the main Bibliotheca circulation monitor will
    count the same event.  However it will greatly improve our current
    view of our Bibliotheca circulation, which is more important.
    """
    SERVICE_NAME = "Bibliotheca Circulation Sweep"
    DEFAULT_BATCH_SIZE = 25
    PROTOCOL = ExternalIntegration.BIBLIOTHECA

    def __init__(self, _db, collection, api_class=BibliothecaAPI, **kwargs):
        _db = Session.object_session(collection)
        super(BibliothecaCirculationSweep, self).__init__(
            _db, collection, **kwargs
        )
        if isinstance(api_class, BibliothecaAPI):
            self.api = api_class
        else:
            self.api = api_class(_db, collection)
        self.analytics = Analytics(_db)
    
    def process_items(self, identifiers):
        identifiers_by_bibliotheca_id = dict()
        bibliotheca_ids = set()
        for identifier in identifiers:
            bibliotheca_ids.add(identifier.identifier)
            identifiers_by_bibliotheca_id[identifier.identifier] = identifier

        identifiers_not_mentioned_by_bibliotheca = set(identifiers)
        now = datetime.datetime.utcnow()

        for circ in self.api.get_circulation_for(bibliotheca_ids):
            if not circ:
                continue
            self._process_circulation_data(
                circ, identifiers_by_bibliotheca_id,
                identifiers_not_mentioned_by_bibliotheca,
            )

        # At this point there may be some license pools left over
        # that Bibliotheca doesn't know about.  This is a pretty reliable
        # indication that we no longer own any licenses to the
        # book.
        for identifier in identifiers_not_mentioned_by_bibliotheca:
            pools = [lp for lp in identifier.licensed_through
                     if lp.data_source.name==DataSource.BIBLIOTHECA
                     and lp.collection == self.collection]
            if not pools:
                continue
            for pool in pools:
                if pool.licenses_owned > 0:
                    if pool.presentation_edition:
                        self.log.warn("Removing %s (%s) from circulation",
                                      pool.presentation_edition.title, pool.presentation_edition.author)
                    else:
                        self.log.warn(
                            "Removing unknown work %s from circulation.",
                            identifier.identifier
                        )
                pool.update_availability(0, 0, 0, 0, self.analytics)
                pool.last_checked = now

    def _process_circulation_data(
        self, circ, identifiers_by_bibliotheca_id, 
        identifiers_not_mentioned_by_bibliotheca
    ):
        """Process a single CirculationData object retrieved from
        Bibliotheca.
        """
        bibliotheca_id = circ[Identifier][Identifier.BIBLIOTHECA_ID]
        identifier = identifiers_by_bibliotheca_id[bibliotheca_id]
        identifiers_not_mentioned_by_bibliotheca.remove(identifier)
        pools = [lp for lp in identifier.licensed_through
                 if lp.data_source.name==DataSource.BIBLIOTHECA
                 and lp.collection == self.collection]
        if not pools:
            # We don't have a license pool for this work. That
            # shouldn't happen--how did we know about the
            # identifier?--but it shouldn't be a big deal to
            # create one.
            pool, ignore = LicensePool.for_foreign_id(
                self._db, self.collection.data_source, identifier.type,
                identifier.identifier, collection=self.collection
            )

            # Bibliotheca books are never open-access.
            pool.open_access = False

            for library in self.collection.libraries:
                self.analytics.collect_event(
                    library, pool, CirculationEvent.DISTRIBUTOR_TITLE_ADD, 
                    datetime.datetime.utcnow()
                )
        else:
            [pool] = pools
                
        self.api.apply_circulation_information_to_licensepool(
            circ, pool, self.analytics
        )


class BibliothecaEventMonitor(CollectionMonitor):

    """Register CirculationEvents for Bibliotheca titles.

    Most of the time we will just be finding out that someone checked
    in or checked out a copy of a book we already knew about.

    But when a new book comes on the scene, this is where we first
    find out about it. When this happens, we create a LicensePool and
    immediately ensure that we get coverage from the
    BibliothecaBibliographicCoverageProvider. 

    But getting up-to-date circulation data for that new book requires
    either that we process further events, or that we encounter it in
    the BibliothecaCirculationSweep.
    """

    SERVICE_NAME = "Bibliotheca Event Monitor"
    DEFAULT_START_TIME = datetime.timedelta(365*3)
    PROTOCOL = ExternalIntegration.BIBLIOTHECA

    def __init__(self, _db, collection, api_class=BibliothecaAPI, 
                 cli_date=None, analytics=None):
        self.analytics = analytics or Analytics(_db)
        super(BibliothecaEventMonitor, self).__init__(_db, collection)
        if isinstance(api_class, BibliothecaAPI):
            # We were given an actual API object. Just use it.
            self.api = api_class
        else:
            self.api = api_class(_db, collection)
        self.bibliographic_coverage_provider = BibliothecaBibliographicCoverageProvider(
            collection, self.api
        )
        if cli_date:
            self.default_start_time = self.create_default_start_time(
                _db,  cli_date
            )

    def create_default_start_time(self, _db, cli_date):
        """Sets the default start time if it's passed as an argument.

        The command line date argument should have the format YYYY-MM-DD.
        """
        initialized = get_one(_db, Timestamp, service=self.service_name)
        default_start_time = datetime.datetime.utcnow() - self.DEFAULT_START_TIME

        if cli_date:
            try:
                if isinstance(cli_date, basestring):
                    date = cli_date
                else:
                    date = cli_date[0]
                return datetime.datetime.strptime(date, "%Y-%m-%d")
            except ValueError as e:
                # Date argument wasn't in the proper format.
                self.log.warn(
                    "%r. Using default date instead: %s.", e,
                    default_start_time.strftime("%B %d, %Y")
                )
                return default_start_time
        if not initialized:
            self.log.info(
                "Initializing %s from date: %s.", self.service_name,
                default_start_time.strftime("%B %d, %Y")
            )
            return default_start_time
        return None

    def slice_timespan(self, start, cutoff, increment):
        """Slice a span of time into segements no large than [increment].

        This lets you divide up a task like "gather the entire
        circulation history for a collection" into chunks of one day.
        """
        slice_start = start
        while slice_start < cutoff:
            full_slice = True
            slice_cutoff = slice_start + increment
            if slice_cutoff > cutoff:
                slice_cutoff = cutoff
                full_slice = False
            yield slice_start, slice_cutoff, full_slice
            slice_start = slice_start + increment

    def run_once(self, start, cutoff):
        added_books = 0
        i = 0
        one_day = datetime.timedelta(days=1)
        most_recent_timestamp = start
        for start, cutoff, full_slice in self.slice_timespan(
                start, cutoff, one_day):
            most_recent_timestamp = start
            self.log.info("Asking for events between %r and %r", start, cutoff)
            try:
                event = None
                events = self.api.get_events_between(start, cutoff, full_slice)
                for event in events:
                    event_timestamp = self.handle_event(*event)
                    if (not most_recent_timestamp or
                        (event_timestamp > most_recent_timestamp)):
                        most_recent_timestamp = event_timestamp
                    i += 1
                    if not i % 1000:
                        self._db.commit()
                self._db.commit()
            except Exception, e:
                if event:
                    self.log.error(
                        "Fatal error processing Bibliotheca event %r.", event,
                        exc_info=e
                    )
                else:
                    self.log.error(
                        "Fatal error getting list of Bibliotheca events.",
                        exc_info=e
                    )
                raise e
        self.log.info("Handled %d events total", i)
        return most_recent_timestamp

    def handle_event(self, bibliotheca_id, isbn, foreign_patron_id,
                     start_time, end_time, internal_event_type):
        # Find or lookup the LicensePool for this event.
        license_pool, is_new = LicensePool.for_foreign_id(
            self._db, self.api.source, Identifier.BIBLIOTHECA_ID, 
            bibliotheca_id, collection=self.collection
        )

        if is_new:
            # This is a new book. Immediately acquire bibliographic
            # coverage for it.  This will set the
            # DistributionMechanisms and make the book
            # presentation-ready. However, its circulation information
            # might not be up to date until we process some more
            # events.
            record = self.bibliographic_coverage_provider.ensure_coverage(
                license_pool.identifier, force=True
            )

        bibliotheca_identifier = license_pool.identifier
        isbn, ignore = Identifier.for_foreign_id(
            self._db, Identifier.ISBN, isbn)

        edition, ignore = Edition.for_foreign_id(
            self._db, self.api.source, Identifier.BIBLIOTHECA_ID, bibliotheca_id)

        # The ISBN and the Bibliotheca identifier are exactly equivalent.
        bibliotheca_identifier.equivalent_to(self.api.source, isbn, strength=1)

        # Log the event.
        start = start_time or CirculationEvent.NO_DATE

        # Make sure the effects of the event reported by Bibliotheca
        # are made visible on the LicensePool and turned into
        # analytics events. This is not 100% reliable, but it
        # should be mostly accurate, and the BibliothecaCirculationSweep
        # will periodically correct the errors.
        license_pool.update_availability_from_delta(
            internal_event_type, start_time, 1, self.analytics
        )

        if is_new:
            # This is our first time seeing this LicensePool. Log its
            # occurance as a separate event.
            license_pool.collect_analytics_event(
                self.analytics, CirculationEvent.DISTRIBUTOR_TITLE_ADD,
                license_pool.last_checked or start_time,
                0, 1
            )
        title = edition.title or "[no title]"
        self.log.info("%r %s: %s", start_time, title, internal_event_type)
        return start_time
