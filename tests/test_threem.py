import datetime
import os
from nose.tools import (
    set_trace, 
    eq_,
    assert_raises,
)

from api.threem import (
    ThreeMAPI,
    MockThreeMAPI,
    ThreeMParser,
    EventParser,
    PatronCirculationParser,
    CheckoutResponseParser,
    ErrorParser,
)

from api.circulation import (
    CirculationAPI,
    HoldInfo,
    LoanInfo,
)
from api.circulation_exceptions import *

from . import (
    DatabaseTest,
    sample_data
)

from core.model import (
    Identifier,
    Loan,
    Hold,
)

from core.util.http import (
    BadResponseException,
)

class ThreeMAPITest(DatabaseTest):

    def setup(self):
        super(ThreeMAPITest,self).setup()
        self.api = MockThreeMAPI(self._db)

    @classmethod
    def sample_data(self, filename):
        return sample_data(filename, 'threem')

class TestThreeMAPI(ThreeMAPITest):      

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


# Tests of the various parser classes.
#

class TestThreeMParser(ThreeMAPITest):

    def test_parse_date(self):
        parser = ThreeMParser()
        v = parser.parse_date("2016-01-02T12:34:56")
        eq_(datetime.datetime(2016, 1, 2, 12, 34, 56), v)

        eq_(None, parser.parse_date(None))
        eq_(None, parser.parse_date("Some weird value"))


class TestEventParser(ThreeMAPITest):

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
        eq_('license_add', internal_event_type)


class TestPatronCirculationParser(ThreeMAPITest):

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


class TestCheckoutResponseParser(ThreeMAPITest):
    def test_parse(self):
        data = self.sample_data("successful_checkout.xml")
        due_date = CheckoutResponseParser().process_all(data)
        eq_(datetime.datetime(2015, 4, 16, 0, 32, 36), due_date)


class TestErrorParser(ThreeMAPITest):

    def test_exceeded_limit(self):
        """The normal case--we get a helpful error message which we turn into
        an appropriate circulation exception.
        """
        msg=self.sample_data("error_exceeded_limit.xml")
        error = ErrorParser().process_all(msg)
        assert isinstance(error, PatronLoanLimitReached)
        eq_(u'Patron cannot loan more than 12 documents', error.message)

    def test_internal_server_error_beomces_remote_initiated_server_error(self):
        """Simulate the message we get when the server goes down."""
        msg = "The server has encountered an error"
        error = ErrorParser().process_all(msg)
        assert isinstance(error, RemoteInitiatedServerError)
        eq_(ThreeMAPI.SERVICE_NAME, error.service_name)
        eq_(502, error.status_code)
        eq_(msg, error.message)

    def test_malformed_error_message_becomes_remote_initiated_server_error(self):
        msg = """<weird>This error does not follow the standard set out by 3M.</weird>"""
        error = ErrorParser().process_all(msg)
        assert isinstance(error, RemoteInitiatedServerError)
        eq_(ThreeMAPI.SERVICE_NAME, error.service_name)
        eq_("Unknown error", error.message)

    def test_blank_error_message_becomes_remote_initiated_server_error(self):
        msg = """<Error xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><Message/></Error>"""
        error = ErrorParser().process_all(msg)
        assert isinstance(error, RemoteInitiatedServerError)
        eq_(ThreeMAPI.SERVICE_NAME, error.service_name)
        eq_("Unknown error", error.message)
