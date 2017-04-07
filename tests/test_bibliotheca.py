# encoding: utf-8
from nose.tools import (
    set_trace, 
    eq_,
    assert_raises,
)
import datetime
import os
import pkgutil

from . import (
    DatabaseTest,
    sample_data
)

from core.model import (
    CirculationEvent,
    Collection,
    Contributor,
    DataSource,
    DataSource,
    Edition,
    Hold,
    Identifier,
    Identifier,
    LicensePool,
    Loan,
    Resource,
    Timestamp
)
from core.util.http import (
    BadResponseException,
)

from api.circulation import (
    CirculationAPI,
    HoldInfo,
    LoanInfo,
)
from api.circulation_exceptions import *
from api.bibliotheca import (
    CheckoutResponseParser,
    CirculationParser,
    ErrorParser,
    EventParser,
    MockBibliothecaAPI,
    PatronCirculationParser,
    BibliothecaAPI,
    BibliothecaEventMonitor,
    BibliothecaParser,
)


class BibliothecaAPITest(DatabaseTest):

    def setup(self):
        super(BibliothecaAPITest,self).setup()
        self.collection = self._collection(protocol=Collection.BIBLIOTHECA)
        self.api = MockBibliothecaAPI(self.collection)

    @classmethod
    def sample_data(self, filename):
        return sample_data(filename, 'threem')

class TestBibliothecaAPI(BibliothecaAPITest):      

    def test_get_events_between_success(self):
        data = self.sample_data("empty_end_date_event.xml")
        self.api.queue_response(200, content=data)
        now = datetime.datetime.now()
        an_hour_ago = now - datetime.timedelta(minutes=3600)
        response = self.api.get_events_between(an_hour_ago, now)
        [event] = list(response)
        eq_('d5rf89', event[0])

    def test_get_events_between_failure(self):
        self.api.queue_response(500)
        now = datetime.datetime.now()
        an_hour_ago = now - datetime.timedelta(minutes=3600)
        assert_raises(
            BadResponseException,
            self.api.get_events_between, an_hour_ago, now
        )

    def test_get_circulation_for_success(self):
        self.api.queue_response(200, content=self.sample_data("item_circulation.xml"))
        data = list(self.api.get_circulation_for(['id1', 'id2']))
        eq_(2, len(data))

    def test_get_circulation_for_returns_empty_list(self):
        self.api.queue_response(200, content=self.sample_data("empty_item_circulation.xml"))
        data = list(self.api.get_circulation_for(['id1', 'id2']))
        eq_(0, len(data))

    def test_get_circulation_for_failure(self):
        self.api.queue_response(500)
        assert_raises(
            BadResponseException,
            list, self.api.get_circulation_for(['id1', 'id2'])
        )

    def test_update_availability(self):
        """Test the 3M implementation of the update_availability
        method defined by the CirculationAPI interface.
        """

        # Create a LicensePool that needs updating.
        edition, pool = self._edition(
            identifier_type=Identifier.THREEM_ID,
            data_source_name=DataSource.THREEM,
            with_license_pool=True
        )

        # We have never checked the circulation information for this
        # LicensePool. Put some random junk in the pool to verify
        # that it gets changed.
        pool.licenses_owned = 10
        pool.licenses_available = 5
        pool.patrons_in_hold_queue = 3
        eq_(None, pool.last_checked)

        # Prepare availability information.
        data = self.sample_data("item_circulation_single.xml")
        # Change the ID in the test data so it looks like it's talking
        # about the LicensePool we just created.
        data = data.replace("d5rf89", pool.identifier.identifier)

        # Update availability using that data.
        self.api.queue_response(200, content=data)
        self.api.update_availability(pool)

        # The availability information has been updated, as has the
        # date the availability information was last checked.
        eq_(1, pool.licenses_owned)
        eq_(1, pool.licenses_available)
        eq_(0, pool.patrons_in_hold_queue)

        old_last_checked = pool.last_checked
        assert old_last_checked is not None

        # Now let's try update_availability again, with a file that
        # makes it look like the book has been removed from the
        # collection.
        data = self.sample_data("empty_item_circulation.xml")
        self.api.queue_response(200, content=data)
        self.api.update_availability(pool)

        eq_(0, pool.licenses_owned)
        eq_(0, pool.licenses_available)
        eq_(0, pool.patrons_in_hold_queue)

        assert pool.last_checked is not old_last_checked

    def test_sync_bookshelf(self):
        patron = self._patron()        
        self.api.queue_response(200, content=self.sample_data("checkouts.xml"))
        circulation = CirculationAPI(self._db, threem=self.api)
        circulation.sync_bookshelf(patron, "dummy pin")

        # The patron should have two loans and two holds.
        l1, l2 = patron.loans
        h1, h2 = patron.holds

        eq_(datetime.datetime(2015, 3, 20, 18, 50, 22), l1.start)
        eq_(datetime.datetime(2015, 4, 10, 18, 50, 22), l1.end)

        eq_(datetime.datetime(2015, 3, 13, 13, 38, 19), l2.start)
        eq_(datetime.datetime(2015, 4, 3, 13, 38, 19), l2.end)

        # This hold has no end date because there's no decision to be
        # made. The patron is fourth in line.
        eq_(datetime.datetime(2015, 3, 24, 15, 6, 56), h1.start)
        eq_(None, h1.end)
        eq_(4, h1.position)

        # The hold has an end date. It's time for the patron to decide
        # whether or not to check out this book.
        eq_(datetime.datetime(2015, 5, 25, 17, 5, 34), h2.start)
        eq_(datetime.datetime(2015, 5, 27, 17, 5, 34), h2.end)
        eq_(0, h2.position)

    def test_place_hold(self):
        patron = self._patron()        
        edition, pool = self._edition(with_license_pool=True)
        self.api.queue_response(200, content=self.sample_data("successful_hold.xml"))
        response = self.api.place_hold(patron, 'pin', pool)
        eq_(pool.identifier.type, response.identifier_type)
        eq_(pool.identifier.identifier, response.identifier)

    def test_place_hold_fails_if_exceeded_hold_limit(self):
        patron = self._patron()        
        edition, pool = self._edition(with_license_pool=True)
        self.api.queue_response(400, content=self.sample_data("error_exceeded_hold_limit.xml"))
        assert_raises(PatronHoldLimitReached, self.api.place_hold,
                      patron, 'pin', pool)

# Tests of the various parser classes.
#

class TestBibliothecaParser(BibliothecaAPITest):

    def test_parse_date(self):
        parser = BibliothecaParser()
        v = parser.parse_date("2016-01-02T12:34:56")
        eq_(datetime.datetime(2016, 1, 2, 12, 34, 56), v)

        eq_(None, parser.parse_date(None))
        eq_(None, parser.parse_date("Some weird value"))


class TestEventParser(BibliothecaAPITest):

    def test_parse_empty_end_date_event(self):
        data = self.sample_data("empty_end_date_event.xml")
        [event] = list(EventParser().process_all(data))
        (threem_id, isbn, patron_id, start_time, end_time,
         internal_event_type) = event
        eq_('d5rf89', threem_id)
        eq_(u'9781101190623', isbn)
        eq_(None, patron_id)
        eq_(datetime.datetime(2016, 4, 28, 11, 4, 6), start_time)
        eq_(None, end_time)
        eq_('distributor_license_add', internal_event_type)


class TestPatronCirculationParser(BibliothecaAPITest):

    def test_parse(self):
        data = self.sample_data("checkouts.xml")
        loans_and_holds = PatronCirculationParser().process_all(data)
        loans = [x for x in loans_and_holds if isinstance(x, LoanInfo)]
        holds = [x for x in loans_and_holds if isinstance(x, HoldInfo)]
        eq_(2, len(loans))
        eq_(2, len(holds))
        [l1, l2] = sorted(loans, key=lambda x: x.identifier)
        eq_("1ad589", l1.identifier)
        eq_("cgaxr9", l2.identifier)
        expect_loan_start = datetime.datetime(2015, 3, 20, 18, 50, 22)
        expect_loan_end = datetime.datetime(2015, 4, 10, 18, 50, 22)
        eq_(expect_loan_start, l1.start_date)
        eq_(expect_loan_end, l1.end_date)

        [h1, h2] = sorted(holds, key=lambda x: x.identifier)

        # This is the book on reserve.
        eq_("9wd8", h1.identifier)
        expect_hold_start = datetime.datetime(2015, 5, 25, 17, 5, 34)
        expect_hold_end = datetime.datetime(2015, 5, 27, 17, 5, 34)
        eq_(expect_hold_start, h1.start_date)
        eq_(expect_hold_end, h1.end_date)
        eq_(0, h1.hold_position)

        # This is the book on hold.
        eq_("d4o8r9", h2.identifier)
        expect_hold_start = datetime.datetime(2015, 3, 24, 15, 6, 56)
        expect_hold_end = datetime.datetime(2015, 3, 24, 15, 7, 51)
        eq_(expect_hold_start, h2.start_date)
        eq_(expect_hold_end, h2.end_date)
        eq_(4, h2.hold_position)


class TestCheckoutResponseParser(BibliothecaAPITest):
    def test_parse(self):
        data = self.sample_data("successful_checkout.xml")
        due_date = CheckoutResponseParser().process_all(data)
        eq_(datetime.datetime(2015, 4, 16, 0, 32, 36), due_date)


class TestErrorParser(BibliothecaAPITest):

    def test_exceeded_limit(self):
        """The normal case--we get a helpful error message which we turn into
        an appropriate circulation exception.
        """
        msg=self.sample_data("error_exceeded_limit.xml")
        error = ErrorParser().process_all(msg)
        assert isinstance(error, PatronLoanLimitReached)
        eq_(u'Patron cannot loan more than 12 documents', error.message)

    def test_exceeded_hold_limit(self):
        msg=self.sample_data("error_exceeded_hold_limit.xml")
        error = ErrorParser().process_all(msg)
        assert isinstance(error, PatronHoldLimitReached)
        eq_(u'Patron cannot have more than 15 holds', error.message)

    def test_wrong_status(self):
        msg=self.sample_data("error_no_licenses.xml")
        error = ErrorParser().process_all(msg)
        assert isinstance(error, NoLicenses)
        eq_(
            u'the patron document status was CAN_WISH and not one of CAN_LOAN,RESERVATION',
            error.message
        )
        
        problem = error.as_problem_detail_document()
        eq_("The library currently has no licenses for this book.",
            problem.detail)
        eq_(404, problem.status_code)

    def test_internal_server_error_beomces_remote_initiated_server_error(self):
        """Simulate the message we get when the server goes down."""
        msg = "The server has encountered an error"
        error = ErrorParser().process_all(msg)
        assert isinstance(error, RemoteInitiatedServerError)
        eq_(BibliothecaAPI.SERVICE_NAME, error.service_name)
        eq_(502, error.status_code)
        eq_(msg, error.message)
        doc = error.as_problem_detail_document()
        eq_(502, doc.status_code)
        eq_("Integration error communicating with 3M", doc.detail)

    def test_unknown_error_becomes_remote_initiated_server_error(self):
        """Simulate the message we get when ¯\_(ツ)_/¯."""
        msg=self.sample_data("error_unknown.xml")
        error = ErrorParser().process_all(msg)
        assert isinstance(error, RemoteInitiatedServerError)
        eq_(BibliothecaAPI.SERVICE_NAME, error.service_name)
        eq_("Unknown error", error.message)

    def test_remote_authentication_failed_becomes_remote_initiated_server_error(self):
        """Simulate the message we get when the error message is
        'Authentication failed' but our authentication information is
        set up correctly.
        """
        msg=self.sample_data("error_authentication_failed.xml")
        error = ErrorParser().process_all(msg)
        assert isinstance(error, RemoteInitiatedServerError)
        eq_(BibliothecaAPI.SERVICE_NAME, error.service_name)
        eq_("Authentication failed", error.message)

    def test_malformed_error_message_becomes_remote_initiated_server_error(self):
        msg = """<weird>This error does not follow the standard set out by 3M.</weird>"""
        error = ErrorParser().process_all(msg)
        assert isinstance(error, RemoteInitiatedServerError)
        eq_(BibliothecaAPI.SERVICE_NAME, error.service_name)
        eq_("Unknown error", error.message)

    def test_blank_error_message_becomes_remote_initiated_server_error(self):
        msg = """<Error xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><Message/></Error>"""
        error = ErrorParser().process_all(msg)
        assert isinstance(error, RemoteInitiatedServerError)
        eq_(BibliothecaAPI.SERVICE_NAME, error.service_name)
        eq_("Unknown error", error.message)

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


class TestBibliothecaEventMonitor(DatabaseTest):

    def test_default_start_time(self):
        api = MockBibliothecaAPI(self._db)
        monitor = BibliothecaEventMonitor(self._db, api=api)
        two_years_ago = datetime.datetime.utcnow() - monitor.TWO_YEARS_AGO

        # Returns a date two years ago if the monitor has never been run before.
        default_start_time = monitor.create_default_start_time(self._db, [])
        assert (two_years_ago - default_start_time).total_seconds() <= 1

        # After Bibliotheca has been initialized, it returns None if no
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
