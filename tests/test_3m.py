from nose.tools import set_trace, eq_
import datetime
import pkgutil
from api.threem import (
    CirculationParser,
    EventParser,
    ErrorParser,
    ThreeMEventMonitor,
)
from core.model import (
    CirculationEvent,
    Contributor,
    DataSource,
    LicensePool,
    Resource,
    Identifier,
    Edition,
    Timestamp
)
from . import DatabaseTest
from api.circulation_exceptions import *
from api.threem import MockThreeMAPI

class Test3MEventParser(object):

    # Sample event feed to test out the parser.
    TWO_EVENTS = """<LibraryEventBatch xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <PublishId>1b0d6667-a10e-424a-9f73-fb6f6d41308e</PublishId>
  <PublishDateTimeInUTC>2014-04-14T13:59:05.6920303Z</PublishDateTimeInUTC>
  <LastEventDateTimeInUTC>2014-04-03T00:00:34</LastEventDateTimeInUTC>
  <Events>
    <CloudLibraryEvent>
      <LibraryId>test-library</LibraryId>
      <EventId>event-1</EventId>
      <EventType>CHECKIN</EventType>
      <EventStartDateTimeInUTC>2014-04-03T00:00:23</EventStartDateTimeInUTC>
      <EventEndDateTimeInUTC>2014-04-03T00:00:23</EventEndDateTimeInUTC>
      <ItemId>theitem1</ItemId>
      <ISBN>900isbn1</ISBN>
      <PatronId>patronid1</PatronId>
      <EventPublishDateTimeInUTC>2014-04-14T13:59:05</EventPublishDateTimeInUTC>
    </CloudLibraryEvent>
    <CloudLibraryEvent>
      <LibraryId>test-library</LibraryId>
      <EventId>event-2</EventId>
      <EventType>CHECKOUT</EventType>
      <EventStartDateTimeInUTC>2014-04-03T00:00:34</EventStartDateTimeInUTC>
      <EventEndDateTimeInUTC>2014-04-02T23:57:37</EventEndDateTimeInUTC>
      <ItemId>theitem2</ItemId>
      <ISBN>900isbn2</ISBN>
      <PatronId>patronid2</PatronId>
      <EventPublishDateTimeInUTC>2014-04-14T13:59:05</EventPublishDateTimeInUTC>
    </CloudLibraryEvent>
  </Events>
</LibraryEventBatch>
"""

    def test_parse_event_batch(self):
        # Parsing the XML gives us two events.
        event1, event2 = EventParser().process_all(self.TWO_EVENTS)

        (threem_id, isbn, patron_id, start_time, end_time,
         internal_event_type) = event1

        eq_("theitem1", threem_id)
        eq_("900isbn1", isbn)
        eq_("patronid1", patron_id)
        eq_(CirculationEvent.DISTRIBUTOR_CHECKIN, internal_event_type)
        eq_(start_time, end_time)

        (threem_id, isbn, patron_id, start_time, end_time,
         internal_event_type) = event2
        eq_("theitem2", threem_id)
        eq_("900isbn2", isbn)
        eq_("patronid2", patron_id)
        eq_(CirculationEvent.DISTRIBUTOR_CHECKOUT, internal_event_type)

        # Verify that start and end time were parsed correctly.
        correct_start = datetime.datetime(2014, 4, 3, 0, 0, 34)
        correct_end = datetime.datetime(2014, 4, 2, 23, 57, 37)
        eq_(correct_start, start_time)
        eq_(correct_end, end_time)


class Test3MCirculationParser(object):

    # Sample circulation feed for testing the parser.

    TWO_CIRCULATION_STATUSES = """
<ArrayOfItemCirculation xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
<ItemCirculation>
  <ItemId>item1</ItemId>
  <ISBN13>900isbn1</ISBN13>
  <TotalCopies>2</TotalCopies>
  <AvailableCopies>0</AvailableCopies>
  <Checkouts>
    <Patron>
      <PatronId>patron1</PatronId>
      <EventStartDateInUTC>2014-03-24T13:10:51</EventStartDateInUTC>
      <EventEndDateInUTC>2014-04-15T13:10:51</EventEndDateInUTC>
      <Position>1</Position>
    </Patron>
  </Checkouts>
  <Holds/>
  <Reserves>
    <Patron>
      <PatronId>patron2</PatronId>
      <EventStartDateInUTC>2014-03-24T13:10:51</EventStartDateInUTC>
      <EventEndDateInUTC>2014-04-15T13:10:51</EventEndDateInUTC>
      <Position>1</Position>
    </Patron>
  </Reserves>
</ItemCirculation>

<ItemCirculation>
  <ItemId>item2</ItemId>
  <ISBN13>900isbn2</ISBN13>
  <TotalCopies>1</TotalCopies>
  <AvailableCopies>0</AvailableCopies>
  <Checkouts>
    <Patron>
      <PatronId>patron3</PatronId>
      <EventStartDateInUTC>2014-04-23T22:14:02</EventStartDateInUTC>
      <EventEndDateInUTC>2014-05-14T22:14:02</EventEndDateInUTC>
      <Position>1</Position>
    </Patron>
  </Checkouts>
  <Holds>
    <Patron>
      <PatronId>patron4</PatronId>
      <EventStartDateInUTC>2014-04-24T18:10:44</EventStartDateInUTC>
      <EventEndDateInUTC>2014-04-24T18:11:02</EventEndDateInUTC>
      <Position>1</Position>
    </Patron>
  </Holds>
  <Reserves/>
</ItemCirculation>
</ArrayOfItemCirculation>
"""

    def test_parse_circulation_batch(self):
        event1, event2 = CirculationParser().process_all(
            self.TWO_CIRCULATION_STATUSES)

        eq_('item1', event1[Identifier][Identifier.THREEM_ID])
        eq_('900isbn1', event1[Identifier][Identifier.ISBN])
        eq_(2, event1[LicensePool.licenses_owned])
        eq_(0, event1[LicensePool.licenses_available])
        eq_(1, event1[LicensePool.licenses_reserved])
        eq_(0, event1[LicensePool.patrons_in_hold_queue])

        eq_('item2', event2[Identifier][Identifier.THREEM_ID])
        eq_('900isbn2', event2[Identifier][Identifier.ISBN])
        eq_(1, event2[LicensePool.licenses_owned])
        eq_(0, event2[LicensePool.licenses_available])
        eq_(0, event2[LicensePool.licenses_reserved])
        eq_(1, event2[LicensePool.patrons_in_hold_queue])


class TestErrorParser(object):

    # Some sample error documents.

    NOT_LOANABLE = '<Error xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><Code>Gen-001</Code><Message>the patron document status was CAN_HOLD and not one of CAN_LOAN,RESERVATION</Message></Error>'

    ALREADY_ON_LOAN = '<Error xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><Code>Gen-001</Code><Message>the patron document status was LOAN and not one of CAN_LOAN,RESERVATION</Message></Error>'

    TRIED_TO_RETURN_UNLOANED_BOOK = '<Error xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><Code>Gen-001</Code><Message>The patron has no eBooks checked out</Message></Error>'

    TRIED_TO_HOLD_LOANABLE_BOOK = '<Error xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><Code>Gen-001</Code><Message>the patron document status was CAN_LOAN and not one of CAN_HOLD</Message></Error>'

    TRIED_TO_HOLD_BOOK_ON_LOAN = '<Error xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><Code>Gen-001</Code><Message>the patron document status was LOAN and not one of CAN_HOLD</Message></Error>'

    ALREADY_ON_HOLD = '<Error xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><Code>Gen-001</Code><Message>the patron document status was HOLD and not one of CAN_HOLD</Message></Error>'

    TRIED_TO_CANCEL_NONEXISTENT_HOLD = '<Error xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><Code>Gen-001</Code><Message>The patron does not have the book on hold</Message></Error>'

    TOO_MANY_LOANS = '<Error xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><Code>Gen-001</Code><Message>Patron cannot loan more than 12 documents</Message></Error>'

    def test_exception(self):
        parser = ErrorParser()

        error = parser.process_all(self.NOT_LOANABLE)
        assert isinstance(error, NoAvailableCopies)

        error = parser.process_all(self.ALREADY_ON_LOAN)
        assert isinstance(error, AlreadyCheckedOut)

        error = parser.process_all(self.ALREADY_ON_HOLD)
        assert isinstance(error, AlreadyOnHold)

        error = parser.process_all(self.TOO_MANY_LOANS)
        assert isinstance(error, PatronLoanLimitReached)

        error = parser.process_all(self.TRIED_TO_CANCEL_NONEXISTENT_HOLD)
        assert isinstance(error, NotOnHold)

        error = parser.process_all(self.TRIED_TO_RETURN_UNLOANED_BOOK)
        assert isinstance(error, NotCheckedOut)

        error = parser.process_all(self.TRIED_TO_HOLD_LOANABLE_BOOK)
        assert isinstance(error, CurrentlyAvailable)

        # This is such a weird case we don't have a special
        # exception for it.
        error = parser.process_all(self.TRIED_TO_HOLD_BOOK_ON_LOAN)
        assert isinstance(error, CannotHold)


class TestThreeMEventMonitor(DatabaseTest):

    def test_default_start_time(self):
        api = MockThreeMAPI(self._db)
        monitor = ThreeMEventMonitor(self._db, api=api)
        two_years_ago = datetime.datetime.utcnow() - monitor.TWO_YEARS_AGO

        # Returns a date two years ago if the monitor has never been run before.
        default_start_time = monitor.create_default_start_time(self._db, [])
        assert (two_years_ago - default_start_time).total_seconds() <= 1

        # After ThreeM has been initialized, it returns None if no
        # arguments are passed
        Timestamp.stamp(self._db, monitor.service_name)
        eq_(None, monitor.create_default_start_time(self._db, []))

        # Returns a date two years ago if args are formatted improperly or the
        # monitor has never been run before
        not_date_args = ['initialize']
        too_many_args = ['2013', '04', '02']
        for args in [not_date_args, too_many_args]:
            default_start_time = monitor.create_default_start_time(self._db, args)
            eq_(True, isinstance(default_start_time, datetime.datetime))
            assert (two_years_ago - default_start_time).total_seconds() <= 1

        # Returns an appropriate date if command line arguments are passed
        # as expected
        proper_args = ['2013-04-02']
        default_start_time = monitor.create_default_start_time(self._db, proper_args)
        eq_(datetime.datetime(2013, 4, 2), default_start_time)
