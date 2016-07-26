from lxml import etree

from cStringIO import StringIO
import itertools
import datetime
import os
import re
import logging

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
    DataSource,
    DeliveryMechanism,
    Edition,
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
    Monitor,
    IdentifierSweepMonitor,
)
from core.util.xmlparser import XMLParser
from core.util.http import (
    BadResponseException
)
from core.threem import (
    MockThreeMAPI as BaseMockThreeMAPI,
    ThreeMAPI as BaseThreeMAPI,
    ThreeMBibliographicCoverageProvider
)

from circulation_exceptions import *
from core.analytics import Analytics

class ThreeMAPI(BaseThreeMAPI, BaseCirculationAPI):

    MAX_AGE = datetime.timedelta(days=730).seconds
    CAN_REVOKE_HOLD_WHEN_RESERVED = False
    SET_DELIVERY_MECHANISM_AT = None

    SERVICE_NAME = "3M"

    # Create a lookup table between common DeliveryMechanism identifiers
    # and Overdrive format types.
    adobe_drm = DeliveryMechanism.ADOBE_DRM
    delivery_mechanism_to_internal_format = {
        (Representation.EPUB_MEDIA_TYPE, adobe_drm): 'ePub',
        (Representation.PDF_MEDIA_TYPE, adobe_drm): 'PDF',
        (Representation.MP3_MEDIA_TYPE, adobe_drm) : 'MP3'
    }

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
                "Error parsing 3M response content: %s", response.content,
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
        return ThreeMCirculationSweep(self._db, api=self).process_batch(
            [licensepool.identifier]
        )

    def patron_activity(self, patron, pin):
        patron_id = patron.authorization_identifier
        path = "circulation/patron/%s" % patron_id
        response = self.request(path)
        return PatronCirculationParser().process_all(response.content)

    TEMPLATE = "<%(request_type)s><ItemId>%(item_id)s</ItemId><PatronId>%(patron_id)s</PatronId></%(request_type)s>"

    def checkout(
            self, patron_obj, patron_password, licensepool, 
            delivery_mechanism
    ):

        """Check out a book on behalf of a patron.

        :param patron_obj: a Patron object for the patron who wants
        to check out the book.

        :param patron_password: The patron's alleged password.  Not
        used here since 3M trusts Simplified to do the check ahead of
        time.

        :param licensepool: LicensePool for the book to be checked out.

        :return: a LoanInfo object
        """
        threem_id = licensepool.identifier.identifier
        patron_identifier = patron_obj.authorization_identifier
        args = dict(request_type='CheckoutRequest',
                    item_id=threem_id, patron_id=patron_identifier)
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
            licensepool.identifier.type,
            licensepool.identifier.identifier,
            start_date=None,
            end_date=loan_expires,
        )
        return loan

    def fulfill(self, patron, password, pool, delivery_mechanism):
        response = self.get_fulfillment_file(
            patron.authorization_identifier, pool.identifier.identifier)
        return FulfillmentInfo(
            pool.identifier.type,
            pool.identifier.identifier,
            content_link=None,
            content_type=response.headers.get('Content-Type'),
            content=response.content,
            content_expires=None,
        )

    def get_fulfillment_file(self, patron_id, threem_id):
        args = dict(request_type='ACSMRequest',
                   item_id=threem_id, patron_id=patron_id)
        body = self.TEMPLATE % args 
        return self.request('GetItemACSM', body, method="PUT")

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

    def apply_circulation_information_to_licensepool(self, circ, pool):
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
            circ.get(LicensePool.patrons_in_hold_queue, 0)
        )


class DummyThreeMAPIResponse(object):

    def __init__(self, response_code, headers, content):
        self.status_code = response_code
        self.headers = headers
        self.content = content

class MockThreeMAPI(BaseMockThreeMAPI, ThreeMAPI):
    pass


class ThreeMParser(XMLParser):

    INPUT_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"

    def parse_date(self, value):
        """Parse the string 3M sends as a date.

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
                    'Unable to parse 3M date: "%s"', value,
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


class CirculationParser(ThreeMParser):

    """Parse 3M's circulation XML dialect into something we can apply to a LicensePool."""

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

        identifiers[Identifier.THREEM_ID] = value("ItemId")
        identifiers[Identifier.ISBN] = value("ISBN13")
        
        item[LicensePool.licenses_owned] = intvalue("TotalCopies")
        try:
            item[LicensePool.licenses_available] = intvalue("AvailableCopies")
        except IndexError:
            logging.warn("No information on available copies for %s",
                         identifiers[Identifier.THREEM_ID]
                     )

        # Counts of patrons who have the book in a certain state.
        for threem_key, simplified_key in [
                ("Holds", LicensePool.patrons_in_hold_queue),
                ("Reserves", LicensePool.licenses_reserved)
        ]:
            t = tag.xpath(threem_key)
            if t:
                t = t[0]
                value = int(t.xpath("count(Patron)"))
                item[simplified_key] = value
            else:
                logging.warn("No circulation information provided for %s %s",
                             identifiers[Identifier.THREEM_ID], threem_key)
        return item


class ThreeMException(Exception):
    pass

class WorkflowException(ThreeMException):
    def __init__(self, actual_status, statuses_that_would_work):
        self.actual_status = actual_status
        self.statuses_that_would_work = statuses_that_would_work

    def __str__(self):
        return "Book status is %s, must be: %s" % (
            self.actual_status, ", ".join(self.statuses_that_would_work))

class ErrorParser(ThreeMParser):
    """Turn an error document from the 3M web service into a CheckoutException"""

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
                string, ThreeMAPI.SERVICE_NAME
            )

        # We were not able to interpret the result as an error.
        # The most likely cause is that the 3M app server is down.
        return RemoteInitiatedServerError(
            "Unknown error", ThreeMAPI.SERVICE_NAME,
        )

    def process_one(self, error_tag, namespaces):
        message = self.text_of_optional_subtag(error_tag, "Message")
        if not message:
            return RemoteInitiatedServerError(
                "Unknown error", ThreeMAPI.SERVICE_NAME,
            )

        if message in self.error_mapping:
            return self.error_mapping[message](message)
        if message in ('Authentication failed', 'Unknown error'):
            # 'Unknown error' is an unknown error on the 3M side.
            #
            # 'Authentication failed' could _in theory_ be an error on
            # our side, but if authentication is set up improperly we
            # actually get a 401 and no body. When we get a real error
            # document with 'Authentication failed', it's always a
            # transient error on the 3M side. Possibly some
            # authentication internal to 3M has failed? Anyway, it
            # happens relatively frequently.
            return RemoteInitiatedServerError(
                message, ThreeMAPI.SERVICE_NAME
            )

        m = self.loan_limit_reached.search(message)
        if m:
            return PatronLoanLimitReached(message)

        m = self.hold_limit_reached.search(message)
        if m:
            return PatronHoldLimitReached(message)

        m = self.wrong_status.search(message)
        if not m:
            return ThreeMException(message)
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

        return ThreeMException(message)

class PatronCirculationParser(ThreeMParser):

    """Parse 3M's patron circulation status document into a list of
    LoanInfo and HoldInfo objects.
    """

    id_type = Identifier.THREEM_ID

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
                value, ThreeMAPI.ARGUMENT_TIME_FORMAT)

        identifier = self.text_of_subtag(tag, "ItemId")
        start_date = datevalue("EventStartDateInUTC")
        end_date = datevalue("EventEndDateInUTC")
        a = [self.id_type, identifier, start_date, end_date]
        if source_class is HoldInfo:
            hold_position = self.int_of_subtag(tag, "Position")
            a.append(hold_position)
        else:
            # Fulfillment info -- not available from this API
            a.append(None)
        return source_class(*a)

class DateResponseParser(ThreeMParser):
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


class EventParser(ThreeMParser):

    """Parse 3M's event file format into our native event objects."""

    EVENT_SOURCE = "3M"

    SET_DELIVERY_MECHANISM_AT = BaseCirculationAPI.BORROW_STEP

    # Map 3M's event names to our names.
    EVENT_NAMES = {
        "CHECKOUT" : CirculationEvent.CHECKOUT,
        "CHECKIN" : CirculationEvent.CHECKIN,
        "HOLD" : CirculationEvent.HOLD_PLACE,
        "RESERVED" : CirculationEvent.AVAILABILITY_NOTIFY,
        "PURCHASE" : CirculationEvent.LICENSE_ADD,
        "REMOVED" : CirculationEvent.LICENSE_REMOVE,
    }

    def process_all(self, string):
        for i in super(EventParser, self).process_all(
                string, "//CloudLibraryEvent"):
            yield i

    def process_one(self, tag, namespaces):
        isbn = self.text_of_subtag(tag, "ISBN")
        threem_id = self.text_of_subtag(tag, "ItemId")
        patron_id = self.text_of_optional_subtag(tag, "PatronId")

        start_time = self.date_from_subtag(tag, "EventStartDateTimeInUTC")
        end_time = self.date_from_subtag(
            tag, "EventEndDateTimeInUTC", required=False
        )

        threem_event_type = self.text_of_subtag(tag, "EventType")
        internal_event_type = self.EVENT_NAMES[threem_event_type]

        return (threem_id, isbn, patron_id, start_time, end_time,
                internal_event_type)

class ThreeMCirculationSweep(IdentifierSweepMonitor):
    """Check on the current circulation status of each 3M book in our
    collection.

    In some cases this will lead to duplicate events being logged,
    because this monitor and the main 3M circulation monitor will
    count the same event.  However it will greatly improve our current
    view of our 3M circulation, which is more important.
    """
    def __init__(self, _db, testing=False, api=None):
        super(ThreeMCirculationSweep, self).__init__(
            _db, "3M Circulation Sweep", batch_size=25)
        self._db = _db
        if not api:
            api = ThreeMAPI(self._db, testing=testing)
        self.api = api
        self.data_source = DataSource.lookup(self._db, DataSource.THREEM)

    def identifier_query(self):
        return self._db.query(Identifier).filter(
            Identifier.type==Identifier.THREEM_ID)

    def process_batch(self, identifiers):
        identifiers_by_threem_id = dict()
        threem_ids = set()
        for identifier in identifiers:
            threem_ids.add(identifier.identifier)
            identifiers_by_threem_id[identifier.identifier] = identifier

        identifiers_not_mentioned_by_threem = set(identifiers)
        now = datetime.datetime.utcnow()

        for circ in self.api.get_circulation_for(threem_ids):
            if not circ:
                continue
            threem_id = circ[Identifier][Identifier.THREEM_ID]
            identifier = identifiers_by_threem_id[threem_id]
            identifiers_not_mentioned_by_threem.remove(identifier)

            pool = identifier.licensed_through
            if not pool:
                # We don't have a license pool for this work. That
                # shouldn't happen--how did we know about the
                # identifier?--but it shouldn't be a big deal to
                # create one.
                pool, ignore = LicensePool.for_foreign_id(
                    self._db, self.data_source, identifier.type,
                    identifier.identifier)

                # 3M books are never open-access.
                pool.open_access = False
                Analytics.collect_event(
                    self._db, pool, CirculationEvent.TITLE_ADD, now)

            self.api.apply_circulation_information_to_licensepool(circ, pool)

        # At this point there may be some license pools left over
        # that 3M doesn't know about.  This is a pretty reliable
        # indication that we no longer own any licenses to the
        # book.
        for identifier in identifiers_not_mentioned_by_threem:
            pool = identifier.licensed_through
            if not pool:
                continue
            if pool.licenses_owned > 0:
                if pool.presentation_edition:
                    self.log.warn("Removing %s (%s) from circulation",
                                  pool.presentation_edition.title, pool.presentation_edition.author)
                else:
                    self.log.warn(
                        "Removing unknown work %s from circulation.",
                        identifier.identifier
                    )
            pool.licenses_owned = 0
            pool.licenses_available = 0
            pool.licenses_reserved = 0
            pool.patrons_in_hold_queue = 0
            pool.last_checked = now


class ThreeMEventMonitor(Monitor):

    """Register CirculationEvents for 3M titles.

    Most of the time we will just be finding out that someone checked
    in or checked out a copy of a book we already knew about.

    But when a new book comes on the scene, this is where we first
    find out about it. When this happens, we create a LicensePool and
    immediately ensure that we get coverage from the
    ThreeMBibliographicCoverageProvider. 

    But getting up-to-date circulation data for that new book requires
    either that we process further events, or that we encounter it in
    the ThreeMCirculationSweep.
    """

    TWO_YEARS_AGO = datetime.timedelta(365*2)

    def __init__(self, _db, default_start_time=None,
                 account_id=None, library_id=None, account_key=None,
                 cli_date=None, testing=False):
        self.service_name = "3M Event Monitor"
        if not default_start_time:
            default_start_time = self.create_default_start_time(_db, cli_date)
        super(ThreeMEventMonitor, self).__init__(
            _db, self.service_name, default_start_time=default_start_time)
        self.api = ThreeMAPI(self._db, testing=testing)
        self.bibliographic_coverage_provider = ThreeMBibliographicCoverageProvider(
            self._db, threem_api=self.api
        )

    def create_default_start_time(self, _db, cli_date):
        """Sets the default start time if it's passed as an argument.

        The command line date argument should have the format YYYY-MM-DD.
        """
        initialized = get_one(_db, Timestamp, self.service_name)
        two_years_ago = datetime.datetime.utcnow() - self.TWO_YEARS_AGO

        if cli_date:
            try:
                date = cli_date[0]
                return datetime.datetime.strptime(date, "%Y-%m-%d")
            except ValueError as e:
                # Date argument wasn't in the proper format.
                self.log.warn(
                    "%r. Using default date instead: %s.", e,
                    two_years_ago.strftime("%B %d, %Y")
                )
                return two_years_ago
        if not initialized:
            self.log.info(
                "Initializing %s from date: %s.", self.service_name,
                two_years_ago.strftime("%B %d, %Y")
            )
            return two_years_ago
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
                        "Fatal error processing 3M event %r.", event,
                        exc_info=e
                    )
                else:
                    self.log.error(
                        "Fatal error getting list of 3M events.",
                        exc_info=e
                    )
                raise e
            self.timestamp.timestamp = most_recent_timestamp
        self.log.info("Handled %d events total", i)
        return most_recent_timestamp

    def handle_event(self, threem_id, isbn, foreign_patron_id,
                     start_time, end_time, internal_event_type):
        # Find or lookup the LicensePool for this event.
        license_pool, is_new = LicensePool.for_foreign_id(
            self._db, self.api.source, Identifier.THREEM_ID, threem_id)

        if is_new:
            # Immediately acquire bibliographic coverage for this book.
            # This will set the DistributionMechanisms and make the
            # book presentation-ready. However, its circulation information
            # might not be up to date until we process some more events.
            record = self.bibliographic_coverage_provider.ensure_coverage(
                license_pool.identifier, force=True
            )

        threem_identifier = license_pool.identifier
        isbn, ignore = Identifier.for_foreign_id(
            self._db, Identifier.ISBN, isbn)

        edition, ignore = Edition.for_foreign_id(
            self._db, self.api.source, Identifier.THREEM_ID, threem_id)

        # The ISBN and the 3M identifier are exactly equivalent.
        threem_identifier.equivalent_to(self.api.source, isbn, strength=1)

        # Log the event.
        event, was_new = get_one_or_create(
            self._db, CirculationEvent, license_pool=license_pool,
            type=internal_event_type, start=start_time,
            foreign_patron_id=foreign_patron_id,
            create_method_kwargs=dict(delta=1,end=end_time)
            )

        # If this is our first time seeing this LicensePool, log its
        # occurance as a separate event
        if is_new:
            event = get_one_or_create(
                self._db, CirculationEvent,
                type=CirculationEvent.TITLE_ADD,
                license_pool=license_pool,
                create_method_kwargs=dict(
                    start=license_pool.last_checked or start_time,
                    delta=1,
                    end=license_pool.last_checked or end_time,
                )
            )
        title = edition.title or "[no title]"
        self.log.info("%r %s: %s", start_time, title, internal_event_type)
        return start_time
