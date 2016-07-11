"""Test the CirculationAPI."""
from nose.tools import (
    assert_raises,
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
from api.testing import MockCirculationAPI


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
        self.circulation = MockCirculationAPI(self._db)
        self.remote = self.circulation.api_for_license_pool(self.pool)

    def borrow(self):
        return self.circulation.borrow(
            self.patron, '1234', self.pool, self.delivery_mechanism
        )

    def sync_bookshelf(self):
        return self.circulation.sync_bookshelf(
            self.patron, '1234'
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

        self.remote.queue_checkout(AlreadyCheckedOut())
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

        self.remote.queue_checkout(AlreadyOnHold())
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
        self.remote.queue_checkout(CannotRenew())
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
        self.remote.queue_checkout(NoAvailableCopies())
        assert_raises_regexp(
            CannotRenew, 
            "You cannot renew a loan if other patrons have the work on hold.",
            self.borrow
        )

    def test_loan_becomes_hold_if_no_available_copies(self):
        # We want to borrow this book but there are no copies.
        self.remote.queue_checkout(NoAvailableCopies())
        self.remote.queue_hold(
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

    def test_loan_becomes_hold_if_no_available_copies_and_preexisting_loan(self):
        # Once upon a time, we had a loan for this book.
        loan, ignore = self.pool.loan_to(self.patron)        
        loan.start = self.YESTERDAY

        # But no longer! What's more, other patrons have taken all the
        # copies!
        self.remote.queue_checkout(NoAvailableCopies())
        self.remote.queue_hold(
            HoldInfo(self.identifier.type, self.identifier.identifier,
                     None, None, 10)
        )

        eq_([], self.remote.availability_updated_for)

        # As such, an attempt to renew our loan results in us actually
        # placing a hold on the book.
        loan, hold, is_new = self.borrow()
        eq_(None, loan)
        eq_(True, is_new)
        eq_(self.pool, hold.license_pool)
        eq_(self.patron, hold.patron)

        # When NoAvailableCopies was raised, the circulation
        # information for the book was immediately updated, to reduce
        # the risk that other patrons would encounter the same
        # problem.
        eq_([self.pool], self.remote.availability_updated_for)

    def test_no_licenses_prompts_availability_update(self):
        # Once the library offered licenses for this book, but
        # the licenses just expired.
        self.remote.queue_checkout(NoLicenses())
        eq_([], self.remote.availability_updated_for)

        # We're not able to borrow the book...
        assert_raises(NoLicenses, self.borrow)

        # But the availability of the book gets immediately updated,
        # so that we don't keep offering the book.
        eq_([self.pool], self.remote.availability_updated_for)

    def test_sync_bookshelf_with_old_local_loan_and_no_remote_loan_deletes_local_loan(self):
        # Local loan that was created yesterday.
        loan, ignore = self.pool.loan_to(self.patron)
        loan.start = self.YESTERDAY

        # The loan is in the db.
        loans = self._db.query(Loan).all()
        eq_([loan], loans)
        
        self.sync_bookshelf()

        # Now the local loan is gone.
        loans = self._db.query(Loan).all()
        eq_([], loans)
        
    def test_sync_bookshelf_with_new_local_loan_and_no_remote_loan_keeps_local_loan(self):
        # Local loan that was just created.
        loan, ignore = self.pool.loan_to(self.patron)
        loan.start = datetime.utcnow()

        # The loan is in the db.
        loans = self._db.query(Loan).all()
        eq_([loan], loans)
        
        self.sync_bookshelf()

        # The loan is still in the db, since it was just created.
        loans = self._db.query(Loan).all()
        eq_([loan], loans)

