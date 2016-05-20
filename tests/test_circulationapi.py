"""Test the CirculationAPI."""
from nose.tools import (
    assert_raises_regexp,
    set_trace,
    eq_,
)

from datetime import (
    datetime, 
    timedelta,
)

from api.circulation_exceptions import *
from api.circulation import (
    BaseCirculationAPI,
    CirculationAPI,
    LoanInfo,
    HoldInfo,
)

from core.model import (
    DataSource,
    Identifier,
    Loan,
    Hold,
)

from . import DatabaseTest

class MockRemoteAPI(object):
    def __init__(self, set_delivery_mechanism_at, can_revoke_hold_when_reserved, dummy):
        self.SET_DELIVERY_MECHANISM_AT = set_delivery_mechanism_at
        self.CAN_REVOKE_HOLD_WHEN_RESERVED = can_revoke_hold_when_reserved
        self.dummy = dummy

    def checkout(
            self, patron_obj, patron_password, licensepool, 
            delivery_mechanism
    ):
        # Should be a LoanInfo.
        return self.dummy._return_or_raise('checkout')
                
    def place_hold(self, patron, pin, licensepool, 
                   hold_notification_email=None):
        # Should be a HoldInfo.
        return self.dummy._return_or_raise('hold')

    def fulfill(self, patron, password, pool, delivery_mechanism):
        # Should be a FulfillmentInfo.
        return self.dummy._return_or_raise('fulfill')

    def checkin(self, patron, pin, licensepool):
        # Return value is not checked.
        return self.dummy._return_or_raise('checkin')

    def release_hold(self, patron, pin, licensepool):
        # Return value is not checked.
        return self.dummy._return_or_raise('release_hold')

    def internal_format(self, delivery_mechanism):
        return delivery_mechanism

class MockCirculationAPI(CirculationAPI):

    def __init__(self, db):
        super(MockCirculationAPI, self).__init__(db)
        self.responses = defaultdict(list)
        self.active_loans = []
        self.active_holds = []
        self.identifier_type_to_data_source_name = {
            Identifier.GUTENBERG_ID: DataSource.GUTENBERG,
            Identifier.OVERDRIVE_ID: DataSource.OVERDRIVE,
            Identifier.THREEM_ID: DataSource.THREEM,
            Identifier.AXIS_360_ID: DataSource.AXIS_360,
        }

    def queue_checkout(self, response):
        self._queue('checkout', response)

    def queue_hold(self, response):
        self._queue('hold', response)

    def queue_fulfill(self, response):
        self._queue('fulfill', response)

    def queue_checkin(self, response):
        self._queue('checkin', response)

    def queue_release_hold(self, response):
        self._queue('release_hold', response)

    def _queue(self, k, v):
        self.responses[k].append(v)

    def set_patron_activity(self, loans, holds):
        self.active_loans = loans
        self.active_holds = holds

    def patron_activity(self, patron, pin):
        # Should be a 2-tuple containing a list of LoanInfo and a
        # list of HoldInfo.
        return self.active_loans, self.active_holds

    def _return_or_raise(self, k):
        logging.debug(k)
        l = self.responses[k]
        v = l.pop()
        if isinstance(v, Exception):
            raise v
        return v

    def api_for_license_pool(self, licensepool):
        set_delivery_mechanism_at = BaseCirculationAPI.FULFILL_STEP
        can_revoke_hold_when_reserved = True
        if licensepool.data_source.name == DataSource.AXIS_360:
            set_delivery_mechanism_at = BaseCirculationAPI.BORROW_STEP
        if licensepool.data_source.name == DataSource.THREEM:
            can_revoke_hold_when_reserved = False
        
        return MockRemoteAPI(set_delivery_mechanism_at, can_revoke_hold_when_reserved, self)


class DummyRemoteAPI(object):

    """The equivalent of the OverdriveAPI or ThreeMAPI, rigged for testing."""

    SET_DELIVERY_MECHANISM_AT = BaseCirculationAPI.FULFILL_STEP

    def __init__(self):
        self.loans = []
        self.holds = []
        self.responses = []

    def internal_format(self, delivery_mechanism):
        return "doesn't matter"

    def queue_response(self, response):
        self.responses.insert(0, response)

    def patron_activity(self, patron, pin):
        return self.loans + self.holds

    def respond(self, *args, **kwargs):
        set_trace()
        response = self.responses.pop()
        if isinstance(response, Exception):
            raise response
        else:
            return response

    checkout = respond
    place_hold = respond


class DummyCirculationAPI(CirculationAPI):

    """A testable superclass of CirculationAPI that sends requests to
    DummyRemoteAPI instead of to real APIs.
    """

    def __init__(self, _db):
        super(DummyCirculationAPI, self).__init__(_db)
        self.remote = DummyRemoteAPI()
        self.apis = [self.remote]
        self.data_source_ids_for_sync = [x.id for x in _db.query(DataSource)]

    def api_for_license_pool(self, licensepool):
        return self.remote

    def add_remote_loan(self, *args, **kwargs):
        loaninfo = LoanInfo(*args, **kwargs)
        self.remote.loans.append(loaninfo)

    def add_remote_hold(self, *args, **kwargs):
        loaninfo = HoldInfo(*args, **kwargs)
        self.remote.holds.append(loaninfo)

    def local_loans(self, patron):
        return self._db.query(Loan).filter(Loan.patron==patron)

    def local_holds(self, patron):
        return self._db.query(Hold).filter(Hold.patron==patron)


class TestCirculationAPI(DatabaseTest):

    YESTERDAY = datetime.utcnow() - timedelta(days=1) 
    IN_TWO_WEEKS = datetime.utcnow() + timedelta(days=14) 

    def setup(self):
        super(TestCirculationAPI, self).setup()
        edition, self.pool = self._edition(with_license_pool=True)
        self.pool.open_access = False
        self.identifier = self.pool.identifier
        [self.delivery_mechanism] = self.pool.delivery_mechanisms
        self.patron = self.default_patron
        self.circulation = DummyCirculationAPI(self._db)
        self.remote = self.circulation.remote
        self.email = 'foo@example.com'

    def borrow(self):
        return self.circulation.borrow(
            self.patron, '1234', self.pool, self.delivery_mechanism, self.email
        )

    def test_attempt_borrow_with_existing_remote_loan(self):
        """The patron has a remote loan that the circ manager doesn't know
        about, and they just tried to borrow a book they already have
        a loan for.
        """
        # Remote loan.
        self.circulation.add_remote_loan(
            self.identifier.type, self.identifier.identifier, self.YESTERDAY,
            self.IN_TWO_WEEKS
        )

        self.remote.queue_response(AlreadyCheckedOut())
        now = datetime.utcnow()
        loan, hold, is_new = self.borrow()

        # There is now a new local loan representing the remote loan.
        eq_(True, is_new)
        eq_(self.pool, loan.license_pool)
        eq_(self.patron, loan.patron)
        eq_(None, hold)

        # The server told us 'there's already a loan for this book'
        # but didn't give us any useful information on when that loan
        # was created. We've faked it with values that should be okay
        # until the next sync.
        assert abs((loan.start-now).seconds) < 2
        eq_(3600, (loan.end-loan.start).seconds)

    def test_attempt_borrow_with_existing_remote_hold(self):
        """The patron has a remote hold that the circ manager doesn't know
        about, and they just tried to borrow a book they already have
        on hold.
        """
        # Remote hold.
        self.circulation.add_remote_hold(
            self.identifier.type, self.identifier.identifier, self.YESTERDAY,
            self.IN_TWO_WEEKS, 10
        )

        self.remote.queue_response(AlreadyOnHold())
        now = datetime.utcnow()
        loan, hold, is_new = self.borrow()

        # There is now a new local hold representing the remote hold.
        eq_(True, is_new)
        eq_(None, loan)
        eq_(self.pool, hold.license_pool)
        eq_(self.patron, hold.patron)

        # The server told us 'you already have this book on hold' but
        # didn't give us any useful information on when that hold was
        # created. We've set the hold start time to the time we found
        # out about it. We'll get the real information the next time
        # we do a sync.
        assert abs((hold.start-now).seconds) < 2
        eq_(None, hold.end)
        eq_(None, hold.position)
        
    def test_attempt_premature_renew_with_local_loan(self):
        """We have a local loan and a remote loan but the patron tried to
        borrow again -- probably to renew their loan.
        """
        # Local loan.
        loan, ignore = self.pool.loan_to(self.patron)        

        # Remote loan.
        self.circulation.add_remote_loan(
            self.identifier.type, self.identifier.identifier, self.YESTERDAY,
            self.IN_TWO_WEEKS
        )

        # This is the expected behavior in most cases--you tried to
        # renew the loan and failed because it's not time yet.
        self.remote.queue_response(CannotRenew())
        assert_raises_regexp(CannotRenew, '^$', self.borrow)

    def test_attempt_renew_with_local_loan_and_no_available_copies(self):
        """We have a local loan and a remote loan but the patron tried to
        borrow again -- probably to renew their loan.
        """
        # Local loan.
        loan, ignore = self.pool.loan_to(self.patron)        

        # Remote loan.
        self.circulation.add_remote_loan(
            self.identifier.type, self.identifier.identifier, self.YESTERDAY,
            self.IN_TWO_WEEKS
        )

        # NoAvailableCopies can happen if there are already people
        # waiting in line for the book. This case gives a more
        # specific error message.
        #
        # Contrast with the way NoAvailableCopies is handled in 
        # test_loan_becomes_hold_if_no_available_copies.
        self.remote.queue_response(NoAvailableCopies())
        assert_raises_regexp(
            CannotRenew, 
            "You cannot renew a loan if other patrons have the work on hold.",
            self.borrow
        )

    def test_loan_becomes_hold_if_no_available_copies(self):
        # Once upon a time, we had a loan for this book.
        loan, ignore = self.pool.loan_to(self.patron)        

        # But no longer! What's more, other patrons have taken all the
        # copies!
        self.remote.queue_response(NoAvailableCopies())
        self.remote.queue_response(
            HoldInfo(self.identifier.type, self.identifier.identifier,
                     None, None, 10)
        )

        # As such, an attempt to renew our loan results in us actually
        # placing a hold on the book.
        loan, hold, is_new = self.borrow()
        eq_(None, loan)
        eq_(True, is_new)
        eq_(self.pool, hold.license_pool)
        eq_(self.patron, hold.patron)
