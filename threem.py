import datetime
import os

from nose.tools import set_trace

from sqlalchemy import or_

from core.model import (
    CirculationEvent,
    Edition,
    Identifier,
    LicensePool,
    get_one_or_create,
)

from core.monitor import Monitor
from core.util.xmlparser import XMLParser
from core.threem import ThreeMAPI as BaseThreeMAPI

class ThreeMAPI(BaseThreeMAPI):

    # def get_patron_circulation(self, patron_id):
    #     path = "circulation/patron/%s" % patron_id
    #     return self.request(path)

    # def place_hold(self, item_id, patron_id):
    #     path = "placehold"
    #     body = "<PlaceHoldRequest><ItemId>%s</ItemId><PatronId>%s</PatronId></PlaceHoldRequest>" % (item_id, patron_id)
    #     return self.request(path, body, method="PUT")

    # def cancel_hold(self, item_id, patron_id):
    #     path = "cancelhold"
    #     body = "<CancelHoldRequest><ItemId>%s</ItemId><PatronId>%s</PatronId></CancelHoldRequest>" % (item_id, patron_id)
    #     return self.request(path, body, method="PUT")

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
        data = self.request(url, max_age=max_age)
        if cache_result:
            self._db.commit()
        events = EventParser().process_all(data)
        return events

    def get_circulation_for(self, identifiers):
        """Return circulation objects for the selected identifiers."""
        url = "/circulation/items/" + ",".join(identifiers)
        # We don't cache this data--it changes too frequently.
        data = self.request(url, cache_result=False)
        for circ in CirculationParser().process_all(data):
            if circ:
                yield circ

     
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
