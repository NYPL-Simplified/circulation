"""Test the CirculationAPI."""
from nose.tools import (
    assert_raises,
    assert_raises_regexp,
    set_trace,
    eq_,
)

from api.config import (
    Configuration,
    temp_config,
)

from datetime import (
    datetime, 
    timedelta,
)

from api.circulation_exceptions import *
from api.circulation import (
    BaseCirculationAPI,
    CirculationAPI,
    FulfillmentInfo,
    LoanInfo,
    HoldInfo,
)

from core.analytics import Analytics
from core.model import (
    CirculationEvent,
    DataSource,
    DeliveryMechanism,
    Hyperlink,
    Identifier,
    Loan,
    Hold,
    RightsStatus,
)
from core.mock_analytics_provider import MockAnalyticsProvider

from . import DatabaseTest, sample_data
from api.testing import MockCirculationAPI
from api.threem import MockThreeMAPI


class TestCirculationAPI(DatabaseTest):

    YESTERDAY = datetime.utcnow() - timedelta(days=1) 
    IN_TWO_WEEKS = datetime.utcnow() + timedelta(days=14) 

    def setup(self):
        super(TestCirculationAPI, self).setup()
        edition, self.pool = self._edition(with_license_pool=True)
        self.pool.open_access = False
        self.identifier = self.pool.identifier
        [self.delivery_mechanism] = self.pool.delivery_mechanisms
        self.patron = self._patron()
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

    def test_borrow_sends_analytics_event(self):
        now = datetime.utcnow()
        loaninfo = LoanInfo(
            self.pool.identifier.type,
            self.pool.identifier.identifier,
            now, now + timedelta(seconds=3600),
        )
        self.remote.queue_checkout(loaninfo)
        now = datetime.utcnow()

        config = {
            Configuration.POLICIES: {
                Configuration.ANALYTICS_POLICY: ["core.mock_analytics_provider"]
            }
        }
        with temp_config(config) as config:
            provider = MockAnalyticsProvider()
            analytics = Analytics.initialize(
                ['core.mock_analytics_provider'], config
            )
            loan, hold, is_new = self.borrow()

            # The Loan looks good.
            eq_(loaninfo.identifier, loan.license_pool.identifier.identifier)
            eq_(self.patron, loan.patron)
            eq_(None, hold)
            eq_(True, is_new)

            # An analytics event was created.
            mock = Analytics.instance().providers[0]
            eq_(1, mock.count)
            eq_(CirculationEvent.CM_CHECKOUT,
                mock.event_type)
            
            # Try to 'borrow' the same book again.
            self.remote.queue_checkout(AlreadyCheckedOut())
            loan, hold, is_new = self.borrow()
            eq_(False, is_new)

            # Since the loan already existed, no new analytics event was
            # sent.
            eq_(1, mock.count)
            
            # Now try to renew the book.
            self.remote.queue_checkout(loaninfo)
            loan, hold, is_new = self.borrow()
            eq_(False, is_new)

            # Renewals are counted as loans, since from an accounting
            # perspective they _are_ loans.
            eq_(2, mock.count)

            # Loans of open-access books go through a different code
            # path, but they count as loans nonetheless.
            self.pool.open_access = True
            self.remote.queue_checkout(loaninfo)
            loan, hold, is_new = self.borrow()
            eq_(3, mock.count)
            
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

    def test_hold_sends_analytics_event(self):
        self.remote.queue_checkout(NoAvailableCopies())
        holdinfo = HoldInfo(self.identifier.type, self.identifier.identifier,
                            None, None, 10)
        self.remote.queue_hold(holdinfo)

        config = {
            Configuration.POLICIES: {
                Configuration.ANALYTICS_POLICY: ["core.mock_analytics_provider"]
            }
        }
        with temp_config(config) as config:
            provider = MockAnalyticsProvider()
            analytics = Analytics.initialize(
                ['core.mock_analytics_provider'], config
            )
            loan, hold, is_new = self.borrow()

            # The Hold looks good.
            eq_(holdinfo.identifier, hold.license_pool.identifier.identifier)
            eq_(self.patron, hold.patron)
            eq_(None, loan)
            eq_(True, is_new)

            # An analytics event was created.
            mock = Analytics.instance().providers[0]
            eq_(1, mock.count)
            eq_(CirculationEvent.CM_HOLD_PLACE,
                mock.event_type)
            
            # Try to 'borrow' the same book again.
            self.remote.queue_checkout(AlreadyOnHold())
            loan, hold, is_new = self.borrow()
            eq_(False, is_new)

            # Since the hold already existed, no new analytics event was
            # sent.
            eq_(1, mock.count)
            
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

    def test_borrow_with_expired_card_fails(self):
        # This checkout would succeed...
        now = datetime.now()
        loaninfo = LoanInfo(
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

    def test_borrow_with_fines_fails(self):
        # This checkout would succeed...
        now = datetime.now()
        loaninfo = LoanInfo(
            self.pool.identifier.type,
            self.pool.identifier.identifier,
            now, now + timedelta(seconds=3600),
        )
        self.remote.queue_checkout(loaninfo)

        # ...except the patron has too many fines.
        old_fines = self.patron.fines
        self.patron.fines = 1000

        with temp_config() as config:
            config[Configuration.POLICIES] = {
                Configuration.MAX_OUTSTANDING_FINES : "$0.50"
            }
            assert_raises(OutstandingFines, self.borrow)
        self.patron.fines = old_fines

    def test_borrow_with_block_fails(self):
        # This checkout would succeed...
        now = datetime.now()
        loaninfo = LoanInfo(
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
            self.pool.data_source, self.pool
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
        eq_(result.content_link, link.resource.url)
        eq_(result.content_type, i_want_an_epub.content_type)

        # Now, if we try to call fulfill() with the broken
        # LicensePoolDeliveryMechanism we get a result from the
        # working DeliveryMechanism with the same format.
        result = self.circulation.fulfill(
            self.patron, '1234', self.pool, broken_lpdm
        )
        assert isinstance(result, FulfillmentInfo)
        eq_(result.content_link, link.resource.url)
        eq_(result.content_type, i_want_an_epub.content_type)
        
        # We get the right result even if the code calling
        # fulfill_open_access() is incorrectly written and passes in
        # the broken LicensePoolDeliveryMechanism (as opposed to its
        # generic DeliveryMechanism).
        result = self.circulation.fulfill_open_access(
            self.pool, broken_lpdm
        )
        assert isinstance(result, FulfillmentInfo)
        eq_(result.content_link, link.resource.url)
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
        
        
    def test_fulfill_sends_analytics_event(self):
        self.pool.loan_to(self.patron)

        fulfillment = self.pool.delivery_mechanisms[0]
        fulfillment.content = "Fulfilled."
        fulfillment.content_link = None
        self.remote.queue_fulfill(fulfillment)

        config = {
            Configuration.POLICIES: {
                Configuration.ANALYTICS_POLICY: ["core.mock_analytics_provider"]
            }
        }
        with temp_config(config) as config:
            provider = MockAnalyticsProvider()
            analytics = Analytics.initialize(
                ['core.mock_analytics_provider'], config
            )
            result = self.circulation.fulfill(self.patron, '1234', self.pool,
                                              self.pool.delivery_mechanisms[0])

            # The fulfillment looks good.
            eq_(fulfillment, result)

            # An analytics event was created.
            mock = Analytics.instance().providers[0]
            eq_(1, mock.count)
            eq_(CirculationEvent.CM_FULFILL,
                mock.event_type)
            
    def test_revoke_loan_sends_analytics_event(self):
        self.pool.loan_to(self.patron)
        self.remote.queue_checkin(True)

        config = {
            Configuration.POLICIES: {
                Configuration.ANALYTICS_POLICY: ["core.mock_analytics_provider"]
            }
        }
        with temp_config(config) as config:
            provider = MockAnalyticsProvider()
            analytics = Analytics.initialize(
                ['core.mock_analytics_provider'], config
            )
            result = self.circulation.revoke_loan(self.patron, '1234', self.pool)

            eq_(True, result)

            # An analytics event was created.
            mock = Analytics.instance().providers[0]
            eq_(1, mock.count)
            eq_(CirculationEvent.CM_CHECKIN,
                mock.event_type)

    def test_release_hold_sends_analytics_event(self):
        self.pool.on_hold_to(self.patron)
        self.remote.queue_release_hold(True)

        config = {
            Configuration.POLICIES: {
                Configuration.ANALYTICS_POLICY: ["core.mock_analytics_provider"]
            }
        }
        with temp_config(config) as config:
            provider = MockAnalyticsProvider()
            analytics = Analytics.initialize(
                ['core.mock_analytics_provider'], config
            )
            result = self.circulation.release_hold(self.patron, '1234', self.pool)

            eq_(True, result)

            # An analytics event was created.
            mock = Analytics.instance().providers[0]
            eq_(1, mock.count)
            eq_(CirculationEvent.CM_HOLD_RELEASE,
                mock.event_type)
            
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

        circulation = IncompleteCirculationAPI(self._db)
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

        circulation = CompleteCirculationAPI(self._db)
        circulation.sync_bookshelf(self.patron, "1234")

        # Now the loan is gone.
        loans = self._db.query(Loan).all()
        eq_([], loans)

    def test_patron_activity(self):
        threem = MockThreeMAPI(self._db)

        circulation = CirculationAPI(self._db, threem=threem)

        data = sample_data("checkouts.xml", "threem")

        threem.queue_response(200, content=data)

        loans, holds, complete = circulation.patron_activity(self.patron, "1234")
        eq_(2, len(loans))
        eq_(2, len(holds))
        eq_(True, complete)

        threem.queue_response(500, content="Error")

        loans, holds, complete = circulation.patron_activity(self.patron, "1234")
        eq_(0, len(loans))
        eq_(0, len(holds))
        eq_(False, complete)


class TestBaseCirculationAPI(DatabaseTest):
    def test_patron_identifier(self):

        # By default, patron_identifier() returns the patron's
        # authorization identifier.
        api = BaseCirculationAPI()
        patron = self._patron()
        eq_(patron.authorization_identifier, api.patron_identifier(patron))

        # However, if the API client defines PSEUDONYM_DATA_SOURCE_NAME,
        # patron_identifier() returns a pseudonym for the patron,
        # creating one if necessary.
        api.PSEUDONYM_DATA_SOURCE_NAME = DataSource.AXIS_360
        pseudonym = api.patron_identifier(patron)
        assert pseudonym != patron.authorization_identifier

        [credential] = patron.credentials
        eq_(api.PSEUDONYM_DATA_SOURCE_NAME, credential.data_source.name)
        eq_(api.PSEUDONYM_CREDENTIAL, credential.type)
        eq_(pseudonym, credential.credential)

        # The pseudonym is reused over time.
        pseudonym2 = api.patron_identifier(patron)
        eq_(pseudonym, pseudonym2)

        # But a different data source will get a different pseudonym.
        api.PSEUDONYM_DATA_SOURCE_NAME = DataSource.BIBLIOTHECA
        pseudonym3 = api.patron_identifier(patron)
        assert pseudonym != pseudonym3
