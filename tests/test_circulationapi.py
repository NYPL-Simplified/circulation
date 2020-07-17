"""Test the CirculationAPI."""
from nose.tools import (
    assert_raises,
    assert_raises_regexp,
    set_trace,
    eq_,
)

import flask
from flask import Flask

from api.config import (
    Configuration,
    temp_config,
)

from datetime import (
    datetime,
    timedelta,
)

from api.authenticator import (
    LibraryAuthenticator,
    PatronData,
)
from api.circulation_exceptions import *
from api.circulation import (
    APIAwareFulfillmentInfo,
    BaseCirculationAPI,
    CirculationAPI,
    CirculationInfo,
    DeliveryMechanismInfo,
    FulfillmentInfo,
    LoanInfo,
    HoldInfo,
)

from core.config import CannotLoadConfiguration
from core.model import (
    CirculationEvent,
    ConfigurationSetting,
    DataSource,
    DeliveryMechanism,
    ExternalIntegration,
    Hyperlink,
    Identifier,
    Loan,
    Hold,
    Representation,
    RightsStatus,
)
from core.mock_analytics_provider import MockAnalyticsProvider

from . import DatabaseTest, sample_data
from api.testing import MockCirculationAPI
from api.bibliotheca import MockBibliothecaAPI


class TestCirculationAPI(DatabaseTest):

    YESTERDAY = datetime.utcnow() - timedelta(days=1)
    TODAY = datetime.utcnow()
    TOMORROW = datetime.utcnow() + timedelta(days=1)
    IN_TWO_WEEKS = datetime.utcnow() + timedelta(days=14)

    def setup(self):
        super(TestCirculationAPI, self).setup()
        self.collection = MockBibliothecaAPI.mock_collection(self._db)
        edition, self.pool = self._edition(
            data_source_name=DataSource.BIBLIOTHECA,
            identifier_type=Identifier.BIBLIOTHECA_ID,
            with_license_pool=True, collection=self.collection
        )
        self.pool.open_access = False
        self.identifier = self.pool.identifier
        [self.delivery_mechanism] = self.pool.delivery_mechanisms
        self.patron = self._patron()
        self.analytics = MockAnalyticsProvider()
        self.circulation = MockCirculationAPI(
            self._db, self._default_library, analytics=self.analytics, api_map = {
                ExternalIntegration.BIBLIOTHECA : MockBibliothecaAPI
            }
        )
        self.remote = self.circulation.api_for_license_pool(self.pool)

    def borrow(self):
        return self.circulation.borrow(
            self.patron, '1234', self.pool, self.delivery_mechanism
        )

    def sync_bookshelf(self):
        return self.circulation.sync_bookshelf(
            self.patron, '1234'
        )

    def test_circulationinfo_collection_id(self):
        # It's possible to instantiate CirculationInfo (the superclass of all
        # other circulation-related *Info classes) with either a
        # Collection-like object or a numeric collection ID.
        cls = CirculationInfo
        other_args = [None] * 3

        info = cls(100, *other_args)
        eq_(100, info.collection_id)

        info = cls(self.pool.collection, *other_args)
        eq_(self.pool.collection.id, info.collection_id)

    def test_borrow_sends_analytics_event(self):
        now = datetime.utcnow()
        loaninfo = LoanInfo(
            self.pool.collection, self.pool.data_source,
            self.pool.identifier.type,
            self.pool.identifier.identifier,
            now, now + timedelta(seconds=3600),
            external_identifier=self._str,
        )
        self.remote.queue_checkout(loaninfo)
        now = datetime.utcnow()

        loan, hold, is_new = self.borrow()

        # The Loan looks good.
        eq_(loaninfo.identifier, loan.license_pool.identifier.identifier)
        eq_(self.patron, loan.patron)
        eq_(None, hold)
        eq_(True, is_new)
        eq_(loaninfo.external_identifier, loan.external_identifier)

        # An analytics event was created.
        eq_(1, self.analytics.count)
        eq_(CirculationEvent.CM_CHECKOUT,
            self.analytics.event_type)

        # Try to 'borrow' the same book again.
        self.remote.queue_checkout(AlreadyCheckedOut())
        loan, hold, is_new = self.borrow()
        eq_(False, is_new)
        eq_(loaninfo.external_identifier, loan.external_identifier)

        # Since the loan already existed, no new analytics event was
        # sent.
        eq_(1, self.analytics.count)

        # Now try to renew the book.
        self.remote.queue_checkout(loaninfo)
        loan, hold, is_new = self.borrow()
        eq_(False, is_new)

        # Renewals are counted as loans, since from an accounting
        # perspective they _are_ loans.
        eq_(2, self.analytics.count)

        # Loans of open-access books go through a different code
        # path, but they count as loans nonetheless.
        self.pool.open_access = True
        self.remote.queue_checkout(loaninfo)
        loan, hold, is_new = self.borrow()
        eq_(3, self.analytics.count)

    def test_borrowing_of_self_hosted_book_succeeds(self):
        # Arrange
        self.pool.self_hosted = True

        # Act
        loan, hold, is_new = self.borrow()

        # Assert
        eq_(True, is_new)
        eq_(self.pool, loan.license_pool)
        eq_(self.patron, loan.patron)
        assert hold is None

    def test_attempt_borrow_with_existing_remote_loan(self):
        """The patron has a remote loan that the circ manager doesn't know
        about, and they just tried to borrow a book they already have
        a loan for.
        """
        # Remote loan.
        self.circulation.add_remote_loan(
            self.pool.collection, self.pool.data_source, self.identifier.type,
            self.identifier.identifier, self.YESTERDAY, self.IN_TWO_WEEKS
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
            self.pool.collection, self.pool.data_source,
            self.identifier.type, self.identifier.identifier,
            self.YESTERDAY, self.IN_TWO_WEEKS, 10
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
            self.pool.collection, self.pool.data_source,
            self.identifier.type, self.identifier.identifier,
            self.YESTERDAY, self.IN_TWO_WEEKS
        )

        # This is the expected behavior in most cases--you tried to
        # renew the loan and failed because it's not time yet.
        self.remote.queue_checkout(CannotRenew())
        assert_raises_regexp(CannotRenew, 'CannotRenew', self.borrow)

    def test_attempt_renew_with_local_loan_and_no_available_copies(self):
        """We have a local loan and a remote loan but the patron tried to
        borrow again -- probably to renew their loan.
        """
        # Local loan.
        loan, ignore = self.pool.loan_to(self.patron)

        # Remote loan.
        self.circulation.add_remote_loan(
            self.pool.collection, self.pool.data_source,
            self.identifier.type, self.identifier.identifier,
            self.YESTERDAY, self.IN_TWO_WEEKS
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
        holdinfo = HoldInfo(
            self.pool.collection, self.pool.data_source,
            self.identifier.type, self.identifier.identifier,
            None, None, 10
        )
        self.remote.queue_hold(holdinfo)

        # As such, an attempt to renew our loan results in us actually
        # placing a hold on the book.
        loan, hold, is_new = self.borrow()
        eq_(None, loan)
        eq_(True, is_new)
        eq_(self.pool, hold.license_pool)
        eq_(self.patron, hold.patron)

    def test_borrow_creates_hold_if_api_returns_hold_info(self):
        # There are no available copies, but the remote API
        # places a hold for us right away instead of raising
        # an error.
        holdinfo = HoldInfo(
            self.pool.collection, self.pool.data_source,
            self.identifier.type, self.identifier.identifier,
            None, None, 10
        )
        self.remote.queue_checkout(holdinfo)

        # As such, an attempt to borrow results in us actually
        # placing a hold on the book.
        loan, hold, is_new = self.borrow()
        eq_(None, loan)
        eq_(True, is_new)
        eq_(self.pool, hold.license_pool)
        eq_(self.patron, hold.patron)

    def test_hold_sends_analytics_event(self):
        self.remote.queue_checkout(NoAvailableCopies())
        holdinfo = HoldInfo(
            self.pool.collection, self.pool.data_source,
            self.identifier.type, self.identifier.identifier,
            None, None, 10
        )
        self.remote.queue_hold(holdinfo)

        loan, hold, is_new = self.borrow()

        # The Hold looks good.
        eq_(holdinfo.identifier, hold.license_pool.identifier.identifier)
        eq_(self.patron, hold.patron)
        eq_(None, loan)
        eq_(True, is_new)

        # An analytics event was created.
        eq_(1, self.analytics.count)
        eq_(CirculationEvent.CM_HOLD_PLACE,
            self.analytics.event_type)

        # Try to 'borrow' the same book again.
        self.remote.queue_checkout(AlreadyOnHold())
        loan, hold, is_new = self.borrow()
        eq_(False, is_new)

        # Since the hold already existed, no new analytics event was
        # sent.
        eq_(1, self.analytics.count)

    def test_loan_becomes_hold_if_no_available_copies_and_preexisting_loan(self):
        # Once upon a time, we had a loan for this book.
        loan, ignore = self.pool.loan_to(self.patron)
        loan.start = self.YESTERDAY

        # But no longer! What's more, other patrons have taken all the
        # copies!
        self.remote.queue_checkout(NoAvailableCopies())
        holdinfo = HoldInfo(
            self.pool.collection, self.pool.data_source,
            self.identifier.type, self.identifier.identifier,
            None, None, 10
        )
        self.remote.queue_hold(holdinfo)

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

    def test_borrow_with_expired_card_fails(self):
        # This checkout would succeed...
        now = datetime.now()
        loaninfo = LoanInfo(
            self.pool.collection, self.pool.data_source,
            self.pool.identifier.type,
            self.pool.identifier.identifier,
            now, now + timedelta(seconds=3600),
        )
        self.remote.queue_checkout(loaninfo)

        # ...except the patron's library card has expired.
        old_expires = self.patron.authorization_expires
        yesterday = now - timedelta(days=1)
        self.patron.authorization_expires = yesterday

        assert_raises(AuthorizationExpired, self.borrow)
        self.patron.authorization_expires = old_expires

    def test_borrow_with_outstanding_fines(self):
        # This checkout would succeed...
        now = datetime.now()
        loaninfo = LoanInfo(
            self.pool.collection, self.pool.data_source,
            self.pool.identifier.type,
            self.pool.identifier.identifier,
            now, now + timedelta(seconds=3600),
        )
        self.remote.queue_checkout(loaninfo)

        # ...except the patron has too many fines.
        old_fines = self.patron.fines
        self.patron.fines = 1000
        setting = ConfigurationSetting.for_library(
            Configuration.MAX_OUTSTANDING_FINES,
            self._default_library
        )
        setting.value = "$0.50"

        assert_raises(OutstandingFines, self.borrow)

        # Test the case where any amount of fines are too much.
        setting.value = "$0"
        assert_raises(OutstandingFines, self.borrow)


        # Remove the fine policy, and borrow succeeds.
        setting.value = None
        loan, i1, i2 = self.borrow()
        assert isinstance(loan, Loan)

        self.patron.fines = old_fines

    def test_borrow_with_block_fails(self):
        # This checkout would succeed...
        now = datetime.now()
        loaninfo = LoanInfo(
            self.pool.collection, self.pool.data_source,
            self.pool.identifier.type,
            self.pool.identifier.identifier,
            now, now + timedelta(seconds=3600),
        )
        self.remote.queue_checkout(loaninfo)

        # ...except the patron is blocked
        self.patron.block_reason = "some reason"
        assert_raises(AuthorizationBlocked, self.borrow)
        self.patron.block_reason = None

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

    def test_borrow_calls_enforce_limits(self):
        # Verify that the normal behavior of CirculationAPI.borrow()
        # is to call enforce_limits() before trying to check out the
        # book.
        class MockVendorAPI(BaseCirculationAPI):
            # Short-circuit the borrowing process -- we just need to make sure
            # enforce_limits is called before checkout()
            def __init__(self):
                self.availability_updated = []

            def internal_format(self, *args, **kwargs):
                return "some format"

            def checkout(self, *args, **kwargs):
                raise NotImplementedError()
        api = MockVendorAPI()

        class MockCirculationAPI(CirculationAPI):
            def __init__(self, *args, **kwargs):
                super(MockCirculationAPI, self).__init__(*args, **kwargs)
                self.enforce_limits_calls = []

            def enforce_limits(self, patron, licensepool):
                self.enforce_limits_calls.append((patron, licensepool))

            def api_for_license_pool(self, pool):
                # Always return the same mock MockVendorAPI.
                return api
        self.circulation = MockCirculationAPI(self._db, self._default_library)

        # checkout() raised the expected NotImplementedError
        assert_raises(NotImplementedError, self.borrow)

        # But before that happened, enforce_limits was called once.
        eq_([(self.patron, self.pool)], self.circulation.enforce_limits_calls)

    def test_patron_at_loan_limit(self):
        # The loan limit is a per-library setting.
        setting = self.patron.library.setting(Configuration.LOAN_LIMIT)

        future = datetime.utcnow() + timedelta(hours=1)

        # This patron has two loans that count towards the loan limit
        patron = self.patron
        self.pool.loan_to(self.patron, end=future)
        pool2 = self._licensepool(None)
        pool2.loan_to(self.patron, end=future)

        # An open-access loan doesn't count towards the limit.
        open_access_pool = self._licensepool(None, with_open_access_download=True)
        open_access_pool.loan_to(self.patron)

        # A loan of indefinite duration (no end date) doesn't count
        # towards the limit.
        indefinite_pool = self._licensepool(None)
        indefinite_pool.loan_to(self.patron, end=None)

        # Another patron's loans don't affect your limit.
        patron2 = self._patron()
        self.pool.loan_to(patron2)

        # patron_at_loan_limit returns True if your number of relevant
        # loans equals or exceeds the limit.
        m = self.circulation.patron_at_loan_limit
        eq_(None, setting.value)
        eq_(False, m(patron))

        setting.value = 1
        eq_(True, m(patron))
        setting.value = 2
        eq_(True, m(patron))
        setting.value = 3
        eq_(False, m(patron))

        # Setting the loan limit to 0 is treated the same as disabling it.
        setting.value = 0
        eq_(False, m(patron))

        # Another library's setting doesn't affect your limit.
        other_library = self._library()
        other_library.setting(Configuration.LOAN_LIMIT).value = 1
        eq_(False, m(patron))

    def test_patron_at_hold_limit(self):
        # The hold limit is a per-library setting.
        setting = self.patron.library.setting(Configuration.HOLD_LIMIT)

        # Unlike the loan limit, it's pretty simple -- every hold counts towards your limit.
        patron = self.patron
        self.pool.on_hold_to(self.patron)
        pool2 = self._licensepool(None)
        pool2.on_hold_to(self.patron)

        # Another patron's holds don't affect your limit.
        patron2 = self._patron()
        self.pool.on_hold_to(patron2)

        # patron_at_hold_limit returns True if your number of holds
        # equals or exceeds the limit.
        m = self.circulation.patron_at_hold_limit
        eq_(None, setting.value)
        eq_(False, m(patron))

        setting.value = 1
        eq_(True, m(patron))
        setting.value = 2
        eq_(True, m(patron))
        setting.value = 3
        eq_(False, m(patron))

        # Setting the hold limit to 0 is treated the same as disabling it.
        setting.value = 0
        eq_(False, m(patron))

        # Another library's setting doesn't affect your limit.
        other_library = self._library()
        other_library.setting(Configuration.HOLD_LIMIT).value = 1
        eq_(False, m(patron))

    def test_enforce_limits(self):
        # Verify that enforce_limits works whether the patron is at one, both,
        # or neither of their loan limits.

        class MockVendorAPI(object):
            # Simulate a vendor API so we can watch license pool
            # availability being updated.
            def __init__(self):
                self.availability_updated = []

            def update_availability(self, pool):
                self.availability_updated.append(pool)

        # Set values for loan and hold limit, so we can verify those
        # values are propagated to the circulation exceptions raised
        # when a patron would exceed one of the limits.
        #
        # Both limits are set to the same value for the sake of
        # convenience in testing.
        self._default_library.setting(Configuration.LOAN_LIMIT).value = 12
        self._default_library.setting(Configuration.HOLD_LIMIT).value = 12

        api = MockVendorAPI()

        class Mock(MockCirculationAPI):
            # Mock the loan and hold limit settings, and return a mock
            # CirculationAPI as needed.
            def __init__(self, *args, **kwargs):
                super(Mock, self).__init__(*args, **kwargs)
                self.api = api
                self.api_for_license_pool_calls = []
                self.patron_at_loan_limit_calls = []
                self.patron_at_hold_limit_calls = []
                self.at_loan_limit = False
                self.at_hold_limit = False

            def api_for_license_pool(self, pool):
                # Always return the same mock vendor API
                self.api_for_license_pool_calls.append(pool)
                return self.api

            def patron_at_loan_limit(self, patron):
                # Return the value set for self.at_loan_limit
                self.patron_at_loan_limit_calls.append(patron)
                return self.at_loan_limit

            def patron_at_hold_limit(self, patron):
                # Return the value set for self.at_hold_limit
                self.patron_at_hold_limit_calls.append(patron)
                return self.at_hold_limit

        circulation = Mock(self._db, self._default_library)

        # Sub-test 1: patron has reached neither limit.
        #
        patron = self.patron
        pool = object()
        circulation.at_loan_limit = False
        circulation.at_hold_limit = False

        eq_(None, circulation.enforce_limits(patron, pool))

        # To determine that the patron is under their limit, it was
        # necessary to call patron_at_loan_limit and
        # patron_at_hold_limit.
        eq_(patron, circulation.patron_at_loan_limit_calls.pop())
        eq_(patron, circulation.patron_at_hold_limit_calls.pop())

        # But it was not necessary to update the availability for the
        # LicensePool, since the patron was not at either limit.
        eq_([], api.availability_updated)

        # Sub-test 2: patron has reached both limits.
        #
        circulation.at_loan_limit = True
        circulation.at_hold_limit = True

        # We can't use assert_raises here because we need to examine the
        # exception object to make sure it was properly instantiated.
        def assert_enforce_limits_raises(expected_exception):
            try:
                circulation.enforce_limits(patron, pool)
                raise Exception("Expected a %r" % expected_exception)
            except Exception, e:
                assert isinstance(e, expected_exception)
                # If .limit is set it means we were able to find a
                # specific limit in the database, which means the
                # exception was instantiated correctly.
                #
                # The presence of .limit will let us give a more specific
                # error message when the exception is converted to a
                # problem detail document.
                eq_(12, e.limit)
        assert_enforce_limits_raises(PatronLoanLimitReached)

        # We were able to deduce that the patron can't do anything
        # with this book, without having to ask the API about
        # availability.
        eq_(patron, circulation.patron_at_loan_limit_calls.pop())
        eq_(patron, circulation.patron_at_hold_limit_calls.pop())
        eq_([], api.availability_updated)

        # At this point we need to start using a real LicensePool.
        pool = self.pool

        # Sub-test 3: patron is at loan limit but not hold limit.
        #
        circulation.at_loan_limit = True
        circulation.at_hold_limit = False

        # If the book is available, we get PatronLoanLimitReached
        pool.licenses_available = 1
        assert_enforce_limits_raises(PatronLoanLimitReached)

        # Reaching this conclusion required checking both patron
        # limits and asking the remote API for updated availability
        # information for this LicensePool.
        eq_(patron, circulation.patron_at_loan_limit_calls.pop())
        eq_(patron, circulation.patron_at_hold_limit_calls.pop())
        eq_(pool, api.availability_updated.pop())

        # If the LicensePool is not available, we pass the
        # test. Placing a hold is fine here.
        pool.licenses_available = 0
        eq_(None, circulation.enforce_limits(patron, pool))
        eq_(patron, circulation.patron_at_loan_limit_calls.pop())
        eq_(patron, circulation.patron_at_hold_limit_calls.pop())
        eq_(pool, api.availability_updated.pop())

        # Sub-test 3: patron is at hold limit but not loan limit
        #
        circulation.at_loan_limit = False
        circulation.at_hold_limit = True

        # If the book is not available, we get PatronHoldLimitReached
        pool.licenses_available = 0
        assert_enforce_limits_raises(PatronHoldLimitReached)

        # Reaching this conclusion required checking both patron
        # limits and asking the remote API for updated availability
        # information for this LicensePool.
        eq_(patron, circulation.patron_at_loan_limit_calls.pop())
        eq_(patron, circulation.patron_at_hold_limit_calls.pop())
        eq_(pool, api.availability_updated.pop())

        # If the book is available, we're fine -- we're not at our loan limit.
        pool.licenses_available = 1
        eq_(None, circulation.enforce_limits(patron, pool))
        eq_(patron, circulation.patron_at_loan_limit_calls.pop())
        eq_(patron, circulation.patron_at_hold_limit_calls.pop())
        eq_(pool, api.availability_updated.pop())

    def test_borrow_hold_limit_reached(self):
        # Verify that you can't place a hold on an unavailable book
        # if you're at your hold limit.
        #
        # NOTE: This is redundant except as an end-to-end test.

        # The hold limit is 1, and the patron has a previous hold.
        self.patron.library.setting(Configuration.HOLD_LIMIT).value = 1
        other_pool = self._licensepool(None)
        other_pool.open_access = False
        other_pool.on_hold_to(self.patron)

        # The patron wants to take out a loan on an unavailable title.
        self.pool.licenses_available = 0
        try:
            self.borrow()
        except Exception, e:
            # The result is a PatronHoldLimitReached configured with the
            # library's hold limit.
            assert isinstance(e, PatronHoldLimitReached)
            eq_(1, e.limit)

        # If we increase the limit, borrow succeeds.
        self.patron.library.setting(Configuration.HOLD_LIMIT).value = 2
        self.remote.queue_checkout(NoAvailableCopies())
        now = datetime.now()
        holdinfo = HoldInfo(
            self.pool.collection, self.pool.data_source,
            self.pool.identifier.type,
            self.pool.identifier.identifier,
            now, now + timedelta(seconds=3600), 10
        )
        self.remote.queue_hold(holdinfo)
        loan, hold, is_new = self.borrow()
        assert hold != None

    def test_fulfill_open_access(self):
        # Here's an open-access title.
        self.pool.open_access = True

        # The patron has the title on loan.
        self.pool.loan_to(self.patron)

        # It has a LicensePoolDeliveryMechanism that is broken (has no
        # associated Resource).
        broken_lpdm = self.delivery_mechanism
        eq_(None, broken_lpdm.resource)
        i_want_an_epub = broken_lpdm.delivery_mechanism

        # fulfill_open_access() and fulfill() will both raise
        # FormatNotAvailable.
        assert_raises(FormatNotAvailable, self.circulation.fulfill_open_access,
                      self.pool, i_want_an_epub)

        assert_raises(FormatNotAvailable, self.circulation.fulfill,
                      self.patron, '1234', self.pool,
                      broken_lpdm,
                      sync_on_failure=False
        )

        # Let's add a second LicensePoolDeliveryMechanism of the same
        # type which has an associated Resource.
        link, new = self.pool.identifier.add_link(
            Hyperlink.OPEN_ACCESS_DOWNLOAD, self._url,
            self.pool.data_source
        )

        working_lpdm = self.pool.set_delivery_mechanism(
            i_want_an_epub.content_type,
            i_want_an_epub.drm_scheme,
            RightsStatus.GENERIC_OPEN_ACCESS,
            link.resource,
        )

        # It's still not going to work because the Resource has no
        # Representation.
        eq_(None, link.resource.representation)
        assert_raises(FormatNotAvailable, self.circulation.fulfill_open_access,
                      self.pool, i_want_an_epub)

        # Let's add a Representation to the Resource.
        representation, is_new = self._representation(
            link.resource.url, i_want_an_epub.content_type,
            "Dummy content", mirrored=True
        )
        link.resource.representation = representation

        # We can finally fulfill a loan.
        result = self.circulation.fulfill_open_access(
            self.pool, broken_lpdm
        )
        assert isinstance(result, FulfillmentInfo)
        eq_(result.content_link, link.resource.representation.public_url)
        eq_(result.content_type, i_want_an_epub.content_type)

        # Now, if we try to call fulfill() with the broken
        # LicensePoolDeliveryMechanism we get a result from the
        # working DeliveryMechanism with the same format.
        result = self.circulation.fulfill(
            self.patron, '1234', self.pool, broken_lpdm
        )
        assert isinstance(result, FulfillmentInfo)
        eq_(result.content_link, link.resource.representation.public_url)
        eq_(result.content_type, i_want_an_epub.content_type)

        # We get the right result even if the code calling
        # fulfill_open_access() is incorrectly written and passes in
        # the broken LicensePoolDeliveryMechanism (as opposed to its
        # generic DeliveryMechanism).
        result = self.circulation.fulfill_open_access(
            self.pool, broken_lpdm
        )
        assert isinstance(result, FulfillmentInfo)
        eq_(result.content_link, link.resource.representation.public_url)
        eq_(result.content_type, i_want_an_epub.content_type)

        # If we change the working LPDM so that it serves a different
        # media type than the one we're asking for, we're back to
        # FormatNotAvailable errors.
        irrelevant_delivery_mechanism, ignore = DeliveryMechanism.lookup(
            self._db, "application/some-other-type",
            DeliveryMechanism.NO_DRM
        )
        working_lpdm.delivery_mechanism = irrelevant_delivery_mechanism
        assert_raises(FormatNotAvailable, self.circulation.fulfill_open_access,
                      self.pool, i_want_an_epub)

    def test_fulfill(self):
        self.pool.loan_to(self.patron)

        fulfillment = self.pool.delivery_mechanisms[0]
        fulfillment.content = "Fulfilled."
        fulfillment.content_link = None
        self.remote.queue_fulfill(fulfillment)

        result = self.circulation.fulfill(self.patron, '1234', self.pool,
                                          self.pool.delivery_mechanisms[0])

        # The fulfillment looks good.
        eq_(fulfillment, result)

        # An analytics event was created.
        eq_(1, self.analytics.count)
        eq_(CirculationEvent.CM_FULFILL,
            self.analytics.event_type)

    def test_fulfill_without_loan(self):
        # By default, a title cannot be fulfilled unless there is an active
        # loan for the title (tested above, in test_fulfill).
        fulfillment = self.pool.delivery_mechanisms[0]
        fulfillment.content = "Fulfilled."
        fulfillment.content_link = None
        self.remote.queue_fulfill(fulfillment)

        def try_to_fulfill():
            # Note that we're passing None for `patron`.
            return self.circulation.fulfill(
                None, '1234', self.pool, self.pool.delivery_mechanisms[0]
            )

        assert_raises(NoActiveLoan, try_to_fulfill)

        # However, if CirculationAPI.can_fulfill_without_loan() says it's
        # okay, the title will be fulfilled anyway.
        def yes_we_can(*args, **kwargs):
            return True
        self.circulation.can_fulfill_without_loan = yes_we_can
        result = try_to_fulfill()
        eq_(fulfillment, result)

    def test_revoke_loan_sends_analytics_event(self):
        self.pool.loan_to(self.patron)
        self.remote.queue_checkin(True)

        result = self.circulation.revoke_loan(self.patron, '1234', self.pool)

        eq_(True, result)

        # An analytics event was created.
        eq_(1, self.analytics.count)
        eq_(CirculationEvent.CM_CHECKIN,
            self.analytics.event_type)

    def test_release_hold_sends_analytics_event(self):
        self.pool.on_hold_to(self.patron)
        self.remote.queue_release_hold(True)

        result = self.circulation.release_hold(self.patron, '1234', self.pool)

        eq_(True, result)

        # An analytics event was created.
        eq_(1, self.analytics.count)
        eq_(CirculationEvent.CM_HOLD_RELEASE,
            self.analytics.event_type)

    def test__collect_event(self):
        # Test the _collect_event method, which gathers information
        # from the current request and sends out the appropriate
        # circulation events.
        class MockAnalytics(object):
            def __init__(self):
                self.events = []

            def collect_event(self, library, licensepool, name, neighborhood):
                self.events.append(
                    (library, licensepool, name, neighborhood)
                )
                return True

        analytics = MockAnalytics()

        l1 = self._default_library
        l2 = self._library()

        p1 = self._patron(library=l1)
        p2 = self._patron(library=l2)

        lp1 = self._licensepool(edition=None)
        lp2 = self._licensepool(edition=None)

        api = CirculationAPI(self._db, l1, analytics)

        def assert_event(inp, outp):
            # Assert that passing `inp` into the mock _collect_event
            # method calls collect_event() on the MockAnalytics object
            # with `outp` as the arguments

            # Call the method
            api._collect_event(*inp)

            # Check the 'event' that was created inside the method.
            eq_(outp, analytics.events.pop())

            # Validate that only one 'event' was created.
            eq_([], analytics.events)

        # Worst case scenario -- the only information we can find is
        # the Library associated with the CirculationAPI object itself.
        assert_event(
            (None, None, 'event'),
            (l1, None, 'event', None)
        )

        # If a LicensePool is provided, it's passed right through
        # to Analytics.collect_event.
        assert_event(
            (None, lp2, 'event'),
            (l1, lp2, 'event', None)
        )

        # If a Patron is provided, their Library takes precedence over
        # the Library associated with the CirculationAPI (though this
        # shouldn't happen).
        assert_event(
            (p2, None, 'event'),
            (l2, None, 'event', None)
        )

        # We must run the rest of the tests in a simulated Flask request
        # context.
        app = Flask(__name__)
        with app.test_request_context():
            # The request library takes precedence over the Library
            # associated with the CirculationAPI (though this
            # shouldn't happen).
            flask.request.library = l2
            assert_event(
                (None, None, 'event'),
                (l2, None, 'event', None)
            )

        with app.test_request_context():
            # The library of the request patron also takes precedence
            # over both (though again, this shouldn't happen).
            flask.request.library = l1
            flask.request.patron = p2
            assert_event(
                (None, None, 'event'),
                (l2, None, 'event', None)
            )

        # Now let's check neighborhood gathering.
        p2.neighborhood = "Compton"
        with app.test_request_context():
            # Neighborhood is only gathered if we explicitly ask for
            # it.
            flask.request.patron = p2
            assert_event(
                (p2, None, 'event'),
                (l2, None, 'event', None)
            )
            assert_event(
                (p2, None, 'event', False),
                (l2, None, 'event', None)
            )
            assert_event(
                (p2, None, 'event', True),
                (l2, None, 'event', "Compton")
            )

            # Neighborhood is not gathered if the request's active
            # patron is not the patron who triggered the event.
            assert_event(
                (p1, None, 'event', True),
                (l1, None, 'event', None)
            )

        with app.test_request_context():
            # Even if we ask for it, neighborhood is not gathered if
            # the data isn't available.
            flask.request.patron = p1
            assert_event(
                (p1, None, 'event', True),
                (l1, None, 'event', None)
            )

        # Finally, remove the mock Analytics object entirely and
        # verify that calling _collect_event doesn't cause a crash.
        api.analytics = None
        api._collect_event(p1, None, 'event')

    def test_sync_bookshelf_ignores_local_loan_with_no_identifier(self):
        loan, ignore = self.pool.loan_to(self.patron)
        loan.start = self.YESTERDAY
        self.pool.identifier = None

        # Verify that we can sync without crashing.
        self.sync_bookshelf()

        # The invalid loan was ignored and is still there.
        loans = self._db.query(Loan).all()
        eq_([loan], loans)

        # Even worse - the loan has no license pool!
        loan.license_pool = None

        # But we can still sync without crashing.
        self.sync_bookshelf()

    def test_sync_bookshelf_ignores_local_hold_with_no_identifier(self):
        hold, ignore = self.pool.on_hold_to(self.patron)
        self.pool.identifier = None

        # Verify that we can sync without crashing.
        self.sync_bookshelf()

        # The invalid hold was ignored and is still there.
        holds = self._db.query(Hold).all()
        eq_([hold], holds)

        # Even worse - the hold has no license pool!
        hold.license_pool = None

        # But we can still sync without crashing.
        self.sync_bookshelf()

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

    def test_sync_bookshelf_with_incomplete_remotes_keeps_local_loan(self):
        loan, ignore = self.pool.loan_to(self.patron)
        loan.start = self.YESTERDAY

        class IncompleteCirculationAPI(MockCirculationAPI):
            def patron_activity(self, patron, pin):
                # A remote API failed, and we don't know if
                # the patron has any loans or holds.
                return [], [], False

        circulation = IncompleteCirculationAPI(
            self._db, self._default_library,
            api_map={ExternalIntegration.BIBLIOTHECA : MockBibliothecaAPI})
        circulation.sync_bookshelf(self.patron, "1234")

        # The loan is still in the db, since there was an
        # error from one of the remote APIs and we don't have
        # complete loan data.
        loans = self._db.query(Loan).all()
        eq_([loan], loans)

        class CompleteCirculationAPI(MockCirculationAPI):
            def patron_activity(self, patron, pin):
                # All the remote API calls succeeded, so
                # now we know the patron has no loans.
                return [], [], True

        circulation = CompleteCirculationAPI(
            self._db, self._default_library,
            api_map={ExternalIntegration.BIBLIOTHECA : MockBibliothecaAPI})
        circulation.sync_bookshelf(self.patron, "1234")

        # Now the loan is gone.
        loans = self._db.query(Loan).all()
        eq_([], loans)

    def test_sync_bookshelf_updates_local_loan_and_hold_with_modified_timestamps(self):
        # We have a local loan that supposedly runs from yesterday
        # until tomorrow.
        loan, ignore = self.pool.loan_to(self.patron)
        loan.start = self.YESTERDAY
        loan.end = self.TOMORROW

        # But the remote thinks the loan runs from today until two
        # weeks from today.
        self.circulation.add_remote_loan(
            self.pool.collection, self.pool.data_source, self.identifier.type,
            self.identifier.identifier, self.TODAY, self.IN_TWO_WEEKS
        )

        # Similar situation for this hold on a different LicensePool.
        edition, pool2 = self._edition(
            data_source_name=DataSource.BIBLIOTHECA,
            identifier_type=Identifier.BIBLIOTHECA_ID,
            with_license_pool=True, collection=self.collection
        )

        hold, ignore = pool2.on_hold_to(self.patron)
        hold.start = self.YESTERDAY
        hold.end = self.TOMORROW
        hold.position = 10

        self.circulation.add_remote_hold(
            pool2.collection, pool2.data_source, pool2.identifier.type,
            pool2.identifier.identifier, self.TODAY, self.IN_TWO_WEEKS,
            0
        )
        self.circulation.sync_bookshelf(self.patron, "1234")

        # Our local loans and holds have been updated to reflect the new
        # data from the source of truth.
        eq_(self.TODAY, loan.start)
        eq_(self.IN_TWO_WEEKS, loan.end)

        eq_(self.TODAY, hold.start)
        eq_(self.IN_TWO_WEEKS, hold.end)
        eq_(0, hold.position)

    def test_sync_bookshelf_applies_locked_delivery_mechanism_to_loan(self):

        # By the time we hear about the patron's loan, they've already
        # locked in an oddball delivery mechanism.
        mechanism = DeliveryMechanismInfo(
            Representation.TEXT_HTML_MEDIA_TYPE, DeliveryMechanism.NO_DRM
        )
        pool = self._licensepool(None)
        self.circulation.add_remote_loan(
            pool.collection, pool.data_source.name,
            pool.identifier.type,
            pool.identifier.identifier,
            datetime.utcnow(),
            None,
            locked_to=mechanism
        )
        self.circulation.sync_bookshelf(self.patron, "1234")

        # The oddball delivery mechanism is now associated with the loan...
        [loan] = self.patron.loans
        delivery = loan.fulfillment.delivery_mechanism
        eq_(Representation.TEXT_HTML_MEDIA_TYPE, delivery.content_type)
        eq_(DeliveryMechanism.NO_DRM, delivery.drm_scheme)

        # ... and (once we commit) with the LicensePool.
        self._db.commit()
        assert loan.fulfillment in pool.delivery_mechanisms

    def test_patron_activity(self):
        # Get a CirculationAPI that doesn't mock out its API's patron activity.
        circulation = CirculationAPI(
            self._db, self._default_library, api_map={
            ExternalIntegration.BIBLIOTHECA : MockBibliothecaAPI
        })
        mock_bibliotheca = circulation.api_for_collection[self.collection.id]

        data = sample_data("checkouts.xml", "bibliotheca")
        mock_bibliotheca.queue_response(200, content=data)

        loans, holds, complete = circulation.patron_activity(self.patron, "1234")
        eq_(2, len(loans))
        eq_(2, len(holds))
        eq_(True, complete)

        mock_bibliotheca.queue_response(500, content="Error")

        loans, holds, complete = circulation.patron_activity(self.patron, "1234")
        eq_(0, len(loans))
        eq_(0, len(holds))
        eq_(False, complete)

    def test_can_fulfill_without_loan(self):
        """Can a title can be fulfilled without an active loan?  It depends on
        the BaseCirculationAPI implementation for that title's colelction.
        """
        class Mock(BaseCirculationAPI):
            def can_fulfill_without_loan(self, patron, pool, lpdm):
                return "yep"

        pool = self._licensepool(None)
        circulation = CirculationAPI(self._db, self._default_library)
        circulation.api_for_collection[pool.collection.id] = Mock()
        eq_(
            "yep",
            circulation.can_fulfill_without_loan(None, pool, object())
        )

        # If format data is missing or the BaseCirculationAPI cannot
        # be found, we assume the title cannot be fulfilled.
        eq_(False, circulation.can_fulfill_without_loan(None, pool, None))
        eq_(False, circulation.can_fulfill_without_loan(None, None, object()))

        circulation.api_for_collection = {}
        eq_(False, circulation.can_fulfill_without_loan(None, pool, None))

        # An open access pool can be fulfilled even without the BaseCirculationAPI.
        pool.open_access = True
        eq_(True, circulation.can_fulfill_without_loan(None, pool, object()))

class TestBaseCirculationAPI(DatabaseTest):

    def test_default_notification_email_address(self):
        # Test the ability to get the default notification email address
        # for a patron or a library.
        self._default_library.setting(
            Configuration.DEFAULT_NOTIFICATION_EMAIL_ADDRESS).value = (
                "help@library"
            )
        m = BaseCirculationAPI.default_notification_email_address
        eq_("help@library", m(self._default_library, None))
        eq_("help@library", m(self._patron(), None))
        other_library = self._library()
        eq_(None, m(other_library, None))

    def test_can_fulfill_without_loan(self):
        """By default, there is a blanket prohibition on fulfilling a title
        when there is no active loan.
        """
        api = BaseCirculationAPI()
        eq_(False, api.can_fulfill_without_loan(object(), object(), object()))

    def test_patron_email_address(self):
        # Test the method that looks up a patron's actual email address
        # (the one they shared with the library) on demand.
        class Mock(BaseCirculationAPI):
            @classmethod
            def _library_authenticator(self, library):
                self._library_authenticator_called_with = library
                value = BaseCirculationAPI._library_authenticator(library)
                self._library_authenticator_returned = value
                return value

        api = Mock()
        patron = self._patron()
        library = patron.library

        # In a non-test scenario, a real LibraryAuthenticator is
        # created and used as a source of knowledge about a patron's
        # email address.
        #
        # However, the default library has no authentication providers
        # set up, so the patron has no email address -- there's no one
        # capable of providing an address.
        eq_(None, api.patron_email_address(patron))
        eq_(patron.library, api._library_authenticator_called_with)
        assert isinstance(
            api._library_authenticator_returned, LibraryAuthenticator
        )

        # Now we're going to pass in our own LibraryAuthenticator,
        # which we've populated with mock authentication providers,
        # into a real BaseCirculationAPI.
        api = BaseCirculationAPI()
        authenticator = LibraryAuthenticator(_db=self._db, library=library)

        # This OAuth authentication provider doesn't implement
        # remote_patron_lookup (it raises NotImplementedError),
        # so it's no help.
        class MockOAuth(object):
            NAME = "mock oauth"
            def remote_patron_lookup(self, patron):
                self.called_with = patron
                raise NotImplementedError()
        mock_oauth = MockOAuth()
        authenticator.register_oauth_provider(mock_oauth)
        eq_(
            None,
            api.patron_email_address(
                patron, library_authenticator=authenticator
            )
        )
        # But we can verify that remote_patron_lookup was in fact
        # called.
        eq_(patron, mock_oauth.called_with)

        # This basic authentication provider _does_ implement
        # remote_patron_lookup, but doesn't provide the crucial
        # information, so still no help.
        class MockBasic(object):
            def remote_patron_lookup(self, patron):
                self.called_with = patron
                return PatronData(authorization_identifier="patron")
        basic = MockBasic()
        authenticator.register_basic_auth_provider(basic)
        eq_(
            None,
            api.patron_email_address(
                patron, library_authenticator=authenticator
            )
        )
        eq_(patron, basic.called_with)

        # This basic authentication provider gives us the information
        # we're after.
        class MockBasic(object):
            def remote_patron_lookup(self, patron):
                self.called_with = patron
                return PatronData(email_address="me@email")
        basic = MockBasic()
        authenticator.basic_auth_provider = basic
        eq_(
            "me@email",
            api.patron_email_address(
                patron, library_authenticator=authenticator
            )
        )
        eq_(patron, basic.called_with)


class TestDeliveryMechanismInfo(DatabaseTest):

    def test_apply(self):

        # Here's a LicensePool with one non-open-access delivery mechanism.
        pool = self._licensepool(None)
        eq_(False, pool.open_access)
        [mechanism] = [
            lpdm.delivery_mechanism for lpdm in pool.delivery_mechanisms
        ]
        eq_(Representation.EPUB_MEDIA_TYPE, mechanism.content_type)
        eq_(DeliveryMechanism.ADOBE_DRM, mechanism.drm_scheme)

        # This patron has the book out on loan, but as far as we no,
        # no delivery mechanism has been set.
        patron = self._patron()
        loan, ignore = pool.loan_to(patron)

        # When consulting with the source of the loan, we learn that
        # the patron has locked the delivery mechanism to a previously
        # unknown mechanism.
        info = DeliveryMechanismInfo(
            Representation.PDF_MEDIA_TYPE, DeliveryMechanism.NO_DRM
        )
        info.apply(loan)

        # This results in the addition of a new delivery mechanism to
        # the LicensePool.
        [new_mechanism] = [
            lpdm.delivery_mechanism for lpdm in pool.delivery_mechanisms
            if lpdm.delivery_mechanism != mechanism
        ]
        eq_(Representation.PDF_MEDIA_TYPE, new_mechanism.content_type)
        eq_(DeliveryMechanism.NO_DRM, new_mechanism.drm_scheme)
        eq_(new_mechanism, loan.fulfillment.delivery_mechanism)
        eq_(RightsStatus.IN_COPYRIGHT, loan.fulfillment.rights_status.uri)

        # Calling apply() again with the same arguments does nothing.
        info.apply(loan)
        eq_(2, len(pool.delivery_mechanisms))

        # Although it's extremely unlikely that this will happen in
        # real life, it's possible for this operation to reveal a new
        # *open-access* delivery mechanism for a LicensePool.
        link, new = pool.identifier.add_link(
            Hyperlink.OPEN_ACCESS_DOWNLOAD, self._url,
            pool.data_source, Representation.EPUB_MEDIA_TYPE
        )

        info = DeliveryMechanismInfo(
            Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.NO_DRM,
            RightsStatus.CC0, link.resource
        )

        # Calling apply() on the loan we were using before will update
        # its associated LicensePoolDeliveryMechanism.
        info.apply(loan)
        [oa_lpdm] = [
            lpdm for lpdm in pool.delivery_mechanisms
            if lpdm.delivery_mechanism not in (mechanism, new_mechanism)
        ]
        eq_(oa_lpdm, loan.fulfillment)

        # The correct resource and rights status have been associated
        # with the new LicensePoolDeliveryMechanism.
        eq_(RightsStatus.CC0, oa_lpdm.rights_status.uri)
        eq_(link.resource, oa_lpdm.resource)

        # The LicensePool is now considered an open-access LicensePool,
        # since it has an open-access delivery mechanism.
        eq_(True, pool.open_access)


class TestConfigurationFailures(DatabaseTest):

    class MisconfiguredAPI(object):

        def __init__(self, _db, collection):
            raise CannotLoadConfiguration("doomed!")

    def test_configuration_exception_is_stored(self):
        """If the initialization of an API object raises
        CannotLoadConfiguration, the exception is stored with the
        CirculationAPI rather than being propagated.
        """
        api_map = {self._default_collection.protocol : self.MisconfiguredAPI}
        circulation = CirculationAPI(
            self._db, self._default_library, api_map=api_map
        )

        # Although the CirculationAPI was created, it has no functioning
        # APIs.
        eq_({}, circulation.api_for_collection)

        # Instead, the CannotLoadConfiguration exception raised by the
        # constructor has been stored in initialization_exceptions.
        e = circulation.initialization_exceptions[self._default_collection.id]
        assert isinstance(e, CannotLoadConfiguration)
        eq_("doomed!", e.message)


class TestFulfillmentInfo(DatabaseTest):

    def test_as_response(self):
        # The default behavior of as_response is to do nothing
        # and let controller code turn the FulfillmentInfo
        # into a Flask Response.
        info = FulfillmentInfo(
            self._default_collection, None,
            None, None, None, None, None, None
        )
        eq_(None, info.as_response)


class TestAPIAwareFulfillmentInfo(DatabaseTest):
    # The APIAwareFulfillmentInfo class has the same properties as a
    # regular FulfillmentInfo -- content_link and so on -- but their
    # values are filled dynamically the first time one of them is
    # accessed, by calling the do_fetch() method.

    class MockAPIAwareFulfillmentInfo(APIAwareFulfillmentInfo):
        """An APIAwareFulfillmentInfo that implements do_fetch() by delegating
        to its API object.
        """
        def do_fetch(self):
            return self.api.do_fetch()

    class MockAPI(object):
        """An API class that sets a flag when do_fetch()
        is called.
        """
        def __init__(self, collection):
            self.collection = collection
            self.fetch_happened = False

        def do_fetch(self):
            self.fetch_happened = True

    def setup(self):
        super(TestAPIAwareFulfillmentInfo, self).setup()
        self.collection = self._default_collection

        # Create a bunch of mock objects which will be used to initialize
        # the instance variables of MockAPIAwareFulfillmentInfo objects.
        self.mock_data_source_name = object()
        self.mock_identifier_type = object()
        self.mock_identifier = object()
        self.mock_key = object()

    def make_info(self, api=None):
        # Create a MockAPIAwareFulfillmentInfo with
        # well-known mock values for its properties.
        return self.MockAPIAwareFulfillmentInfo(
            api, self.mock_data_source_name, self.mock_identifier_type,
            self.mock_identifier, self.mock_key
        )

    def test_constructor(self):
        # The constructor sets the instance variables appropriately,
        # but does not call do_fetch() or set any of the variables
        # that imply do_fetch() has happened.

        # Create a MockAPI
        api = self.MockAPI(self.collection)

        # Create an APIAwareFulfillmentInfo based on that API.
        info = self.make_info(api)
        eq_(api, info.api)
        eq_(self.mock_key, info.key)
        eq_(self.collection, api.collection)
        eq_(api.collection, info.collection(self._db))
        eq_(self.mock_data_source_name, info.data_source_name)
        eq_(self.mock_identifier_type, info.identifier_type)
        eq_(self.mock_identifier, info.identifier)

        # The fetch has not happened.
        eq_(False, api.fetch_happened)
        eq_(None, info._content_link)
        eq_(None, info._content_type)
        eq_(None, info._content)
        eq_(None, info._content_expires)

    def test_fetch(self):
        # Verify that fetch() calls api.do_fetch()
        api = self.MockAPI(self.collection)
        info = self.make_info(api)
        eq_(False, info._fetched)
        eq_(False, api.fetch_happened)
        info.fetch()
        eq_(True, info._fetched)
        eq_(True, api.fetch_happened)

        # We don't check that values like _content_link were set,
        # because our implementation of do_fetch() doesn't set any of
        # them. Different implementations may set different subsets
        # of these values.

    def test_properties_fetch_on_demand(self):
        # Verify that accessing each of the properties calls fetch()
        # if it hasn't been called already.
        api = self.MockAPI(self.collection)
        info = self.make_info(api)
        eq_(False, info._fetched)
        info.content_link
        eq_(True, info._fetched)

        info = self.make_info(api)
        eq_(False, info._fetched)
        info.content_type
        eq_(True, info._fetched)

        info = self.make_info(api)
        eq_(False, info._fetched)
        info.content
        eq_(True, info._fetched)

        info = self.make_info(api)
        eq_(False, info._fetched)
        info.content_expires
        eq_(True, info._fetched)

        # Once the data has been fetched, accessing one of the properties
        # doesn't call fetch() again.
        info.fetch_happened = False
        info.content_expires
        eq_(False, info.fetch_happened)
