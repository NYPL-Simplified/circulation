import datetime
from decimal import Decimal
from nose.tools import (
    set_trace, eq_,
    assert_raises,
)

from . import (
    DatabaseTest,
)

from api.config import Configuration, temp_config
from api.authenticator import PatronData
from api.util.patron import PatronUtility
from api.circulation_exceptions import *
from core.model import ConfigurationSetting
from core.util import MoneyUtility


class TestPatronUtility(DatabaseTest):

    def test_needs_external_sync(self):
        """Test the method that encapsulates the determination
        of whether or not a patron needs to have their account
        synced with the remote.
        """
        now = datetime.datetime.utcnow()
        one_hour_ago = now - datetime.timedelta(hours=1)
        six_seconds_ago = now - datetime.timedelta(seconds=6)
        three_seconds_ago = now - datetime.timedelta(seconds=3)
        yesterday = now - datetime.timedelta(days=1)

        patron = self._patron()

        # Patron has never been synced.
        patron.last_external_sync = None
        eq_(True, PatronUtility.needs_external_sync(patron))

        # Patron was synced recently.
        patron.last_external_sync = one_hour_ago
        eq_(False, PatronUtility.needs_external_sync(patron))

        # Patron was synced more than 12 hours ago.
        patron.last_external_sync = yesterday
        eq_(True, PatronUtility.needs_external_sync(patron))

        # Patron was synced recently but has no borrowing
        # privileges. Timeout is five seconds instead of 12 hours.
        patron.authorization_expires = yesterday
        patron.last_external_sync = three_seconds_ago
        eq_(False, PatronUtility.needs_external_sync(patron))

        patron.last_external_sync = six_seconds_ago
        eq_(True, PatronUtility.needs_external_sync(patron))

    def test_has_borrowing_privileges(self):
        """Test the methods that encapsulate the determination
        of whether or not a patron can borrow books.
        """
        now = datetime.datetime.utcnow()
        one_day_ago = now - datetime.timedelta(days=1)
        patron = self._patron()

        # Most patrons have borrowing privileges.
        eq_(True, PatronUtility.has_borrowing_privileges(patron))
        PatronUtility.assert_borrowing_privileges(patron)

        # If your card expires you lose borrowing privileges.
        patron.authorization_expires = one_day_ago
        eq_(False, PatronUtility.has_borrowing_privileges(patron))
        assert_raises(
            AuthorizationExpired,
            PatronUtility.assert_borrowing_privileges, patron
        )
        patron.authorization_expires = None

        # If has_excess_fines returns True, you lose borrowing privileges.
        # has_excess_fines itself is tested in a separate method.
        class Mock(PatronUtility):
            @classmethod
            def has_excess_fines(cls, patron):
                cls.called_with = patron
                return True
        eq_(False, Mock.has_borrowing_privileges(patron))
        eq_(patron, Mock.called_with)
        assert_raises(
            OutstandingFines,
            Mock.assert_borrowing_privileges, patron
        )

        # Even if the circulation manager is not configured to know
        # what "excessive fines" are, the authentication mechanism
        # might know, and might store that information in the
        # patron's block_reason.
        patron.block_reason = PatronData.EXCESSIVE_FINES
        assert_raises(
            OutstandingFines,
            PatronUtility.assert_borrowing_privileges, patron
        )

        # If your card is blocked for any reason you lose borrowing
        # privileges.
        patron.block_reason = "some reason"
        eq_(False, PatronUtility.has_borrowing_privileges(patron))
        assert_raises(
            AuthorizationBlocked,
            PatronUtility.assert_borrowing_privileges, patron
        )

        patron.block_reason = None
        eq_(True, PatronUtility.has_borrowing_privileges(patron))

    def test_has_excess_fines(self):
        # Test the has_excess_fines method.
        patron = self._patron()

        # If you accrue excessive fines you lose borrowing privileges.
        setting = ConfigurationSetting.for_library(
            Configuration.MAX_OUTSTANDING_FINES,
            self._default_library
        )

        # Verify that all these tests work no matter what data type has been stored in
        # patron.fines.
        for patron_fines in ("1", "0.75", 1, 1.0, Decimal(1), MoneyUtility.parse("1")):
            patron.fines = patron_fines

            # Test cases where the patron's fines exceed a well-defined limit,
            # or when any amount of fines is too much.
            for max_fines in (
                ["$0.50", "0.5", .5] +   # well-defined limit
                ["$0", "$0.00", "0", 0]  # any fines is too much
            ):
                setting.value = max_fines
                eq_(True, PatronUtility.has_excess_fines(patron))

            # Test cases where the patron's fines are below a
            # well-defined limit, or where fines are ignored
            # altogether.
            for max_fines in (
                ["$100", 100] + # well-defined-limit
                [None, ""]      # fines ignored
            ):
                setting.value = max_fines
                eq_(False, PatronUtility.has_excess_fines(patron))

        # Test various cases where fines in any amount deny borrowing
        # privileges, but the patron has no fines.
        for patron_fines in ("0", "$0", 0, None, MoneyUtility.parse("$0")):
            patron.fines = patron_fines
            for max_fines in ["$0", "$0.00", "0", 0]:
                setting.value = max_fines
                eq_(False, PatronUtility.has_excess_fines(patron))
