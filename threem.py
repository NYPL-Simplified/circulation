from lxml import etree
from cStringIO import StringIO
import datetime
import os
import re

from nose.tools import set_trace

from sqlalchemy import or_

from core.model import (
    CirculationEvent,
    DataSource,
    Edition,
    Identifier,
    LicensePool,
    get_one_or_create,
    Loan,
    Hold,
    Session,
)

from core.monitor import Monitor
from core.util.xmlparser import XMLParser
from core.threem import ThreeMAPI as BaseThreeMAPI
from circulation_exceptions import *

class ThreeMAPI(BaseThreeMAPI):

    MAX_AGE = datetime.timedelta(days=730).seconds

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
        if response.status_code in (500, 501, 502):
            raise Exception(
                "Server sent status code %s" % response.status_code)
        if cache_result:
            self._db.commit()
        try:
            events = EventParser().process_all(response.content)
        except Exception, e:
            print response.content
            raise e
        return events

    def get_circulation_for(self, identifiers):
        """Return circulation objects for the selected identifiers."""
        url = "/circulation/items/" + ",".join(identifiers)
        response = self.request(url)
        for circ in CirculationParser().process_all(response.content):
            if circ:
                yield circ

    def get_patron_checkouts(self, patron_obj):
        patron_id = patron_obj.authorization_identifier
        path = "circulation/patron/%s" % patron_id
        response = self.request(path)
        return PatronCirculationParser().process_all(response.content)

    TEMPLATE = "<%(request_type)s><ItemId>%(item_id)s</ItemId><PatronId>%(patron_id)s</PatronId></%(request_type)s>"

    def checkout(self, patron_obj, patron_password, identifier, format=None):

        threem_id = identifier.identifier
        patron_identifier = patron_obj.authorization_identifier
        args = dict(request_type='CheckoutRequest',
                    item_id=threem_id, patron_id=patron_identifier)
        body = self.TEMPLATE % args 
        response = self.request('checkout', body, method="PUT")
        loan_expires = CheckoutResponseParser().process_all(response.content)
        if response.status_code in (200, 201):
            response = self.get_fulfillment_file(
                patron_identifier, threem_id)
            return None, response.headers.get('Content-Type'), response.content, loan_expires
        else:
            raise CheckoutException(response.content)

    def fulfill(self, patron, password, identifier, format):
        response = self.get_fulfillment_file(
            patron.authorization_identifier, identifier.identifier)
        return None, response.headers.get('Content-Type'), response.content

    def get_fulfillment_file(self, patron_id, threem_id):
        args = dict(request_type='ACSMRequest',
                   item_id=threem_id, patron_id=patron_id)
        body = self.TEMPLATE % args 
        return self.request('GetItemACSM', body, method="PUT")

    def checkin(self, patron_id, threem_id):
        args = dict(request_type='CheckinRequest',
                   item_id=threem_id, patron_id=patron_id)
        body = self.TEMPLATE % args 
        return self.request('checkin', body, method="PUT")

    def place_hold(self, patron_id, threem_id):
        args = dict(request_type='PlaceHoldRequest',
                   item_id=threem_id, patron_id=patron_id)
        body = self.TEMPLATE % args 
        return self.request('placehold', body, method="PUT")

    def release_hold(self, patron_id, threem_id):
        args = dict(request_type='CancelHoldRequest',
                   item_id=threem_id, patron_id=patron_id)
        body = self.TEMPLATE % args 
        return self.request('cancelhold', body, method="PUT")

    @classmethod
    def sync_bookshelf(cls, patron, remote_loans, remote_holds, remote_reserves):
        """Synchronize 3M's view of the patron's bookshelf with our view.
        """

        _db = Session.object_session(patron)
        threem_source = DataSource.lookup(_db, DataSource.THREEM)
        active_loans = []
        active_holds = []

        # Treat reserves as holds where position=0
        for reserve in remote_reserves:
            reserve[Hold.position] = 0
            remote_holds.append(reserve)

        loans = _db.query(Loan).join(Loan.license_pool).filter(
            LicensePool.data_source==threem_source)
        holds = _db.query(Hold).join(Hold.license_pool).filter(
            LicensePool.data_source==threem_source)

        loans_by_identifier = dict()
        for loan in loans:
            loans_by_identifier[loan.license_pool.identifier.identifier] = loan
        holds_by_identifier = dict()
        for hold in holds:
            holds_by_identifier[hold.license_pool.identifier.identifier] = hold

        for checkout in remote_loans:
            start = checkout[Loan.start]
            end = checkout[Loan.end]
            threem_identifier = checkout[Identifier][Identifier.THREEM_ID]
            identifier, new = Identifier.for_foreign_id(
                _db, Identifier.THREEM_ID, threem_identifier)
            if identifier.identifier in loans_by_identifier:
                # We have a corresponding local loan. Just make sure the
                # data matches up.
                loan = loans_by_identifier[identifier.identifier]
                loan.start = start
                loan.end = end
                active_loans.append(loan)

                # Remove the loan from the list so that we don't
                # delete it later.
                del loans_by_identifier[identifier.identifier]
            else:
                # We never heard of this loan. Create it locally.
                pool, new = LicensePool.for_foreign_id(
                    _db, threem_source, identifier.type,
                    identifier.identifier)
                loan, new = pool.loan_to(patron, start, end)
                active_loans.append(loan)

        for hold in remote_holds:
            start = hold[Hold.start]
            end = hold[Hold.end]
            threem_identifier = hold[Identifier][Identifier.THREEM_ID]
            position = hold[Hold.position]
            identifier, new = Identifier.for_foreign_id(
                _db, Identifier.THREEM_ID, threem_identifier)
            if identifier.identifier in holds_by_identifier:
                # We have a corresponding hold. Just make sure the
                # data matches up.
                hold = holds_by_identifier[identifier.identifier]
                hold.update(start, end, position)
                active_holds.append(hold)

                # Remove the hold from the list so that we don't
                # delete it later.
                del holds_by_identifier[identifier.identifier]
            else:
                # We never heard of this hold. Create it locally.
                pool, new = LicensePool.for_foreign_id(
                    _db, threem_source, identifier.type,
                    identifier.identifier)
                hold, new = pool.on_hold_to(patron, start, end, position)
                active_holds.append(loan)

        # Every hold remaining in holds_by_identifier is a hold that
        # 3M doesn't know about, which means it's expired and we
        # should get rid of it.
        for hold in holds_by_identifier.values():
            _db.delete(hold)

        return active_loans, active_holds


class DummyThreeMAPIResponse(object):

    def __init__(self, response_code, headers, content):
        self.status_code = response_code
        self.headers = headers
        self.content = content

class DummyThreeMAPI(ThreeMAPI):

    def __init__(self, *args, **kwargs):
        super(DummyThreeMAPI, self).__init__(*args, **kwargs)
        self.responses = []

    def queue_response(self, response_code=200, media_type="applicaion/xml",
                       other_headers=None, content=''):
        headers = {"content-type": media_type}
        if other_headers:
            for k, v in other_headers.items():
                headers[k.lower()] = v
        self.responses.append((response_code, headers, content))

    def request(self, *args, **kwargs):
        response_code, headers, content = self.responses.pop()
        if kwargs.get('method') == 'GET':
            return content
        else:
            return DummyThreeMAPIResponse(response_code, headers, content)


class CirculationParser(XMLParser):

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
        item[LicensePool.licenses_available] = intvalue("AvailableCopies")

        # Counts of patrons who have the book in a certain state.
        for threem_key, simplified_key in [
                ("Holds", LicensePool.patrons_in_hold_queue),
                ("Reserves", LicensePool.licenses_reserved)
        ]:
            t = tag.xpath(threem_key)[0]
            value = int(t.xpath("count(Patron)"))
            item[simplified_key] = value

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

class ErrorParser(XMLParser):
    """Turn an error document from the 3M web service into a CheckoutException"""

    wrong_status = re.compile(
        "the patron document status was ([^ ]+) and not one of ([^ ]+)")
    
    error_mapping = {
        "The patron does not have the book on hold" : NotOnHold,
        "The patron has no eBooks checked out" : NotCheckedOut,
    }

    def process_all(self, string):
        for i in super(ErrorParser, self).process_all(
                string, "//Error"):
            return i

    def process_one(self, error_tag, namespaces):
        message = self.text_of_subtag(error_tag, "Message")
        if not message:
            return ThreeMException("Unknown error")

        if message in self.error_mapping:
            return self.error_mapping[message](message)

        m = self.wrong_status.search(message)
        if not m:
            return ThreeMException(message)
        actual, expected = m.groups()
        expected = expected.split(",")

        if 'CAN_LOAN' in expected and actual == 'CAN_HOLD':
            return NoAvailableCopies(message)

        if 'CAN_LOAN' in expected and actual == 'LOAN':
            return AlreadyCheckedOut(message)

        if 'CAN_HOLD' in expected and actual == 'CAN_WISH':
            return CurrentlyAvailable(message)

        if 'CAN_HOLD' in expected and actual == 'HOLD':
            return AlreadyOnHold(message)

        if 'CAN_HOLD' in expected:
            return CannotHold(message)

        if 'CAN_LOAN' in expected:
            return CannotLoan(message)

        return ThreeMException(message)

class PatronCirculationParser(XMLParser):

    """Parse 3M's patron circulation status document into something we can apply to a Patron."""

    def process_all(self, string):
        parser = etree.XMLParser()
        root = etree.parse(StringIO(string), parser)
        sup = super(PatronCirculationParser, self)
        loans = [x for x in sup.process_all(
            root, "//Checkouts/Item", handler=self.process_one_loan) if x]
        holds = [x for x in sup.process_all(
            root, "//Holds/Item", handler=self.process_one_hold) if x]
        reserves = [x for x in sup.process_all(
            root, "//Reserves/Item", handler=self.process_one_hold) if x]
        return loans, holds, reserves

    def process_one_loan(self, tag, namespaces):
        return self.process_one(tag, namespaces, Loan)

    def process_one_hold(self, tag, namespaces):
        return self.process_one(tag, namespaces, Hold)

    def process_one(self, tag, namespaces, source_class):
        if not tag.xpath("ItemId"):
            # This happens for events associated with books
            # no longer in our collection.
            return None

        def datevalue(key):
            value = self.text_of_subtag(tag, key)
            return datetime.datetime.strptime(
                value, ThreeMAPI.ARGUMENT_TIME_FORMAT)

        identifiers = {}
        item = { Identifier : identifiers }
        identifiers[Identifier.THREEM_ID] = self.text_of_subtag(tag, "ItemId")
        
        item[source_class.start] = datevalue("EventStartDateInUTC")
        item[source_class.end] = datevalue("EventEndDateInUTC")
        if source_class == Hold:
            item[source_class.position] = self.int_of_subtag(tag, "Position")
        return item

class CheckoutResponseParser(XMLParser):

    """Extract due date from a checkout response."""

    def process_all(self, string):
        parser = etree.XMLParser()
        root = etree.parse(StringIO(string), parser)
        m = root.xpath("/CheckoutResult/DueDateInUTC")
        if not m:
            return None
        due_date = m[0].text
        if not due_date:
            return None
        return datetime.datetime.strptime(
                due_date, EventParser.INPUT_TIME_FORMAT)


class EventParser(XMLParser):

    """Parse 3M's event file format into our native event objects."""

    EVENT_SOURCE = "3M"
    INPUT_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"

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
        patron_id = self.text_of_subtag(tag, "PatronId")

        start_time = self.text_of_subtag(tag, "EventStartDateTimeInUTC")
        start_time = datetime.datetime.strptime(
                start_time, self.INPUT_TIME_FORMAT)
        end_time = self.text_of_subtag(tag, "EventEndDateTimeInUTC")
        end_time = datetime.datetime.strptime(
            end_time, self.INPUT_TIME_FORMAT)

        threem_event_type = self.text_of_subtag(tag, "EventType")
        internal_event_type = self.EVENT_NAMES[threem_event_type]

        return (threem_id, isbn, patron_id, start_time, end_time,
                internal_event_type)

class ThreeMCirculationSweep(Monitor):
    """Check on the current circulation status of each 3M book in our
    collection.

    In some cases this will lead to duplicate events being logged,
    because this monitor and the main 3M circulation monitor will
    count the same event.  However it will greatly improve our current
    view of our 3M circulation, which is more important.
    """
    def __init__(self, _db, account_id=None, library_id=None, account_key=None):
        super(ThreeMCirculationSweep, self).__init__(
            _db, "3M Circulation Sweep")
        self._db = _db
        self.api = ThreeMAPI(self._db, account_id, library_id,
                             account_key)
        self.data_source = DataSource.lookup(self._db, DataSource.THREEM)

    def run_once(self, start, cutoff):
        offset = 0
        limit = 25
        q = self._db.query(Identifier).filter(
            Identifier.type==Identifier.THREEM_ID).order_by(Identifier.id)
        identifiers = True
        while identifiers:
            identifiers = q.offset(offset).limit(limit).all()
            self.process_batch(identifiers)
            self._db.commit()
            offset += limit

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
                CirculationEvent.log(
                    self._db, pool, CirculationEvent.TITLE_ADD,
                    None, None, start=now)

            if pool.edition:
                m = "Updating %s (%s)" % (
                    pool.edition.title, pool.edition.author)
                print m.encode("utf8")
            else:
                print "Updating unknown work %s" % identifier.identifier
            # Update availability and send out notifications.
            pool.update_availability(
                circ[LicensePool.licenses_owned],
                circ[LicensePool.licenses_available],
                circ[LicensePool.licenses_reserved],
                circ[LicensePool.patrons_in_hold_queue])


        # At this point there may be some license pools left over
        # that 3M doesn't know about.  This is a pretty reliable
        # indication that we no longer own any licenses to the
        # book.
        for identifier in identifiers_not_mentioned_by_threem:
            pool = identifier.licensed_through
            if not pool:
                continue
            if pool.edition:
                m = "Removing %s (%s) from circulation" % (
                    pool.edition.title, pool.edition.author)
                print m.encode("utf8")
            else:
                print "Removing unknown work %s from circulation." % (
                    identifier.identifier)
            pool.licenses_owned = 0
            pool.licenses_available = 0
            pool.licenses_reserved = 0
            pool.patrons_in_hold_queue = 0
            pool.last_checked = now


class ThreeMEventMonitor(Monitor):

    """Register CirculationEvents for 3M titles.

    When a new book comes on the scene, we find out about it here and
    we create a LicensePool.  But the bibliographic data isn't
    inserted into those LicensePools until the
    ThreeMBibliographicMonitor runs. And the circulation data isn't
    associated with it until the ThreeMCirculationMonitor runs.
    """

    def __init__(self, _db, default_start_time=None,
                 account_id=None, library_id=None, account_key=None):
        super(ThreeMEventMonitor, self).__init__(
            _db, "3M Event Monitor", default_start_time=default_start_time)
        self.account_id = account_id
        self.library_id = library_id
        self.account_key = account_key
        self.api = ThreeMAPI(self._db, self.account_id, self.library_id,
                             self.account_key)

    def slice_timespan(self, start, cutoff, increment):
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
            print "Asking for events between %r and %r" % (start, cutoff)
            try:
                events = self.api.get_events_between(start, cutoff, full_slice)
                for event in events:
                    event_timestamp = self.handle_event(*event)
                    if (not most_recent_timestamp or
                        (event_timestamp > most_recent_timestamp)):
                        most_recent_timestamp = event_timestamp
                    i += 1
                    if not i % 1000:
                        print i
                        self._db.commit()
                self._db.commit()
            except Exception, e:
                print "Error: %s, will try again next time." % str(e)
            self.timestamp.timestamp = most_recent_timestamp
        print "Handled %d events total" % i
        return most_recent_timestamp

    def handle_event(self, threem_id, isbn, foreign_patron_id,
                     start_time, end_time, internal_event_type):
        # Find or lookup the LicensePool for this event.
        license_pool, is_new = LicensePool.for_foreign_id(
            self._db, self.api.source, Identifier.THREEM_ID, threem_id)

        # Force the ThreeMCirculationMonitor to check on this book the
        # next time it runs.
        license_pool.last_checked = None

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
                    start=license_pool.last_checked,
                    delta=1,
                    end=license_pool.last_checked,
                )
            )
        print "%r %s: %s" % (start_time, edition.title, internal_event_type)
        return start_time


class ThreeMCirculationMonitor(Monitor):

    MAX_STALE_TIME = datetime.timedelta(seconds=3600 * 24 * 30)

    def __init__(self, _db, account_id=None, library_id=None, account_key=None):
        super(ThreeMCirculationMonitor, self).__init__("3M Circulation Monitor")
        self._db = _db
        self.api = ThreeMAPI(_db, account_id, library_id, account_key)

    def run_once(self, _db, start, cutoff):
        stale_at = start - self.MAX_STALE_TIME
        clause = or_(LicensePool.last_checked==None,
                    LicensePool.last_checked <= stale_at)
        q = _db.query(LicensePool).filter(clause).filter(
            LicensePool.data_source==self.api.source)
        current_batch = []
        most_recent_timestamp = None
        for pool in q:
            current_batch.append(pool)
            if len(current_batch) == 25:
                most_recent_timestamp = self.process_batch(_db, current_batch)
                current_batch = []
        if current_batch:
            most_recent_timestamp = self.process_batch(_db, current_batch)
        return most_recent_timestamp

    def process_batch(self, _db, pools):
        identifiers = []
        pool_for_identifier = dict()
        for p in pools:
            pool_for_identifier[p.identifier.identifier] = p
            identifiers.append(p.identifier.identifier)
        for item in self.api.get_circulation_for(identifiers):
            identifier = item[Identifier][Identifier.THREEM_ID]
            pool = pool_for_identifier[identifier]
            self.process_pool(_db, pool, item)
        _db.commit()
        return most_recent_timestamp
        
    def process_pool(self, _db, pool, item):
        pool.update_availability(
            item[LicensePool.licenses_owned],
            item[LicensePool.licenses_available],
            item[LicensePool.licenses_reserved],
            item[LicensePool.patrons_in_hold_queue])
        print "%r: %d owned, %d available, %d reserved, %d queued" % (pool.edition(), pool.licenses_owned, pool.licenses_available, pool.licenses_reserved, pool.patrons_in_hold_queue)
