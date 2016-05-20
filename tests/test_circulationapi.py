"""Test the CirculationAPI."""
from nose.tools import (
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

from . import DatabaseTest

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

    def respond(self, *args, **kwargs):
        response = self.responses.pop()
        if isinstance(response, Exception):
            raise response
        else:
            return response

    checkout = respond
        

class DummyCirculationAPI(CirculationAPI):

    """A testable superclass of CirculationAPI that sends requests to
    DummyRemoteAPI instead of to real APIs.
    """

    def __init__(self, _db):
        super(DummyCirculationAPI, self).__init__(_db)
        self.remote = DummyRemoteAPI()
        self.apis = [self.remote]

    def api_for_license_pool(self, licensepool):
        return self.remote

    def add_remote_loan(self, *args, **kwargs):
        loaninfo = LoanInfo(*args, **kwargs)
        self.remote.loans.append(loaninfo)

    def add_remote_hold(self, *args, **kwargs):
        loaninfo = HoldInfo(*args, **kwargs)
        self.remote.holds.append(loaninfo)


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
        
    def test_attempt_renew_with_local_loan(self):
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
        new_loan, hold, is_new = self.borrow()

        # We get our preexisting local loan back.
        eq_(loan, new_loan)
        eq_(None, hold)
        eq_(False, is_new)

        # NoAvailableCopies can happen if renewals are prohibited when
        # there are already people waiting in line for the book.
        self.remote.queue_response(NoAvailableCopies())
        new_loan, hold, is_new = self.borrow()

        # Again, we get our preexisting local loan back.
        eq_(loan, new_loan)
        eq_(None, hold)
        eq_(False, is_new)


