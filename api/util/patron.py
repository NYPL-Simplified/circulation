import datetime
from money import Money
from api.config import Configuration
from api.circulation_exceptions import *
from core.model.patron import Patron
from core.util import MoneyUtility

class PatronUtility(object):
    """Apply circulation-specific logic to Patron model objects."""

    @classmethod
    def needs_external_sync(cls, patron):
        """Could this patron stand to have their metadata synced with the
        remote?

        By default, all patrons get synced once every twelve
        hours. Patrons who lack borrowing privileges can always stand
        to be synced, since their privileges may have just been
        restored.
        """
        if not patron.last_external_sync:
            # This patron has never been synced.
            return True

        now = datetime.datetime.utcnow()
        if cls.has_borrowing_privileges(patron):
            # A patron who has borrowing privileges gets synced every twelve
            # hours. Their account is unlikely to change rapidly.
            check_every = Patron.MAX_SYNC_TIME
        else:
            # A patron without borrowing privileges might get synced
            # every time they make a request. It's likely they are
            # taking action to get their account reinstated and we
            # don't want to make them wait twelve hours to get access.
            check_every = datetime.timedelta(seconds=5)
        expired_at = patron.last_external_sync + check_every
        if now > expired_at:
            return True
        return False

    @classmethod
    def has_borrowing_privileges(cls, patron):
        """Is the given patron allowed to check out books?

        :return: A boolean
        """
        try:
            cls.assert_borrowing_privileges(patron)
            return True
        except CannotLoan as e:
            return False

    @classmethod
    def assert_borrowing_privileges(cls, patron):
        """Raise an exception unless the patron currently has borrowing
        privileges.

        :raises AuthorizationExpired: If the patron's authorization has expired.
        :raises OutstandingFines: If the patron has too many outstanding fines.

        """
        if not cls.authorization_is_active(patron):
            # The patron's card has expired.
            raise AuthorizationExpired()

        if cls.has_excess_fines(patron):
            raise OutstandingFines()

        from api.authenticator import PatronData
        if patron.block_reason is not None:
            if patron.block_reason is PatronData.EXCESSIVE_FINES:
                # The authentication mechanism itself may know that
                # the patron has outstanding fines, even if the circulation
                # manager is not configured to make that deduction.
                raise OutstandingFines()
            raise AuthorizationBlocked()

    @classmethod
    def has_excess_fines(cls, patron):
        """Does this patron have fines in excess of the maximum fine amount set for their library?

        :param a Patron:
        :return: A boolean
        """
        if not patron.fines:
            return False

        if isinstance(patron.fines, Money):
            patron_fines = patron.fines
        else:
            patron_fines = MoneyUtility.parse(patron.fines)
        actual_fines = patron_fines.amount
        max_fines = Configuration.max_outstanding_fines(patron.library)
        if max_fines is not None and actual_fines > max_fines.amount:
            return True
        return False

    @classmethod
    def authorization_is_active(cls, patron):
        """Return True unless the patron's authorization has expired."""
        # Unlike pretty much every other place in this app, we use
        # (server) local time here instead of UTC. This is to make it
        # less likely that a patron's authorization will expire before
        # they think it should.
        now = datetime.datetime.now()
        if (patron.authorization_expires
            and cls._to_date(patron.authorization_expires)
            < cls._to_date(now)):
            return False
        return True

    @classmethod
    def _to_date(cls, x):
        """Convert a datetime into a date. Leave a date alone."""
        if isinstance(x, datetime.datetime):
            return x.date()
        return x
