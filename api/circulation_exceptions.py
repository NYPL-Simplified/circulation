from flask_babel import lazy_gettext as _

from api.config import Configuration
from core.config import IntegrationException
from core.problem_details import (
    INTEGRATION_ERROR,
    INTERNAL_SERVER_ERROR,
)
from problem_details import *


class CirculationException(IntegrationException):
    """An exception occured when carrying out a circulation operation.

    `status_code` is the status code that should be returned to the patron.
    """
    status_code = 400

    def __init__(self, message=None, debug_info=None):
        message = message or self.__class__.__name__
        super(CirculationException, self).__init__(message, debug_info)


class InternalServerError(IntegrationException):
    status_code = 500

    def as_problem_detail_document(self, debug=False):
        """Return a suitable problem detail document."""
        return INTERNAL_SERVER_ERROR

class RemoteInitiatedServerError(InternalServerError):
    """One of the servers we communicate with had an internal error."""
    status_code = 502

    def __init__(self, message, service_name):
        super(RemoteInitiatedServerError, self).__init__(message)
        self.service_name = service_name

    def as_problem_detail_document(self, debug=False):
        """Return a suitable problem detail document."""
        msg = _("Integration error communicating with %(service_name)s", service_name=self.service_name)
        return INTEGRATION_ERROR.detailed(msg)

class NoOpenAccessDownload(CirculationException):
    """We expected a book to have an open-access download, but it didn't."""
    status_code = 500

class AuthorizationFailedException(CirculationException):
    status_code = 401

class PatronAuthorizationFailedException(AuthorizationFailedException):
    status_code = 400

class RemotePatronCreationFailedException(CirculationException):
    status_code = 500

class LibraryAuthorizationFailedException(CirculationException):
    status_code = 500

class InvalidInputException(CirculationException):
    """The patron gave invalid input to the library."""
    status_code = 400

class LibraryInvalidInputException(InvalidInputException):
    """The library gave invalid input to the book provider."""
    status_code = 500

class DeliveryMechanismError(InvalidInputException):
    status_code = 400
    """The patron broke the rules about delivery mechanisms."""

class DeliveryMechanismMissing(DeliveryMechanismError):
    """The patron needed to specify a delivery mechanism and didn't."""

class DeliveryMechanismConflict(DeliveryMechanismError):
    """The patron specified a delivery mechanism that conflicted with
    one already set in stone.
    """

class CannotLoan(CirculationException):
    status_code = 500

class OutstandingFines(CannotLoan):
    """The patron has outstanding fines above the limit in the library's
    policy."""
    status_code = 403

class AuthorizationExpired(CannotLoan):
    """The patron's authorization has expired."""
    status_code = 403

    def as_problem_detail_document(self, debug=False):
        """Return a suitable problem detail document."""
        return EXPIRED_CREDENTIALS

class AuthorizationBlocked(CannotLoan):
    """The patron's authorization is blocked for some reason other than
    fines or an expired card.

    For instance, the patron has been banned from the library.
    """
    status_code = 403

    def as_problem_detail_document(self, debug=False):
        """Return a suitable problem detail document."""
        return BLOCKED_CREDENTIALS

class LimitReached(CirculationException):
    """The patron cannot carry out an operation because it would push them above
    some limit set by library policy.

    This exception cannot be used on its own. It must be subclassed and the following constants defined:
        * `BASE_DOC`: A ProblemDetail, used as the basis for conversion of this exception into a
           problem detail document.
        * `SETTING_NAME`: Then name of the library-specific ConfigurationSetting whose numeric
          value is the limit that cannot be exceeded.
        * `MESSAGE_WITH_LIMIT` A string containing the interpolation value "%(limit)s", which
          offers a more specific explanation of the limit exceeded.
    """
    status_code = 403
    BASE_DOC = None
    SETTING_NAME = None
    MESSAGE_WITH_LIMIT = None

    def as_problem_detail_document(self, debug=False, library=None):
        """Return a suitable problem detail document."""
        doc = self.BASE_DOC
        if not library:
            return doc
        limit = library.setting(self.SETTING_NAME).int_value
        if limit:
            detail = self.MESSAGE_WITH_LIMIT % dict(limit=limit)
            return doc.detailed(detail=detail)
        return doc

class PatronLoanLimitReached(CannotLoan, LimitReached):
    BASE_DOC = LOAN_LIMIT_REACHED
    MESSAGE_WITH_LIMIT = SPECIFIC_LOAN_LIMIT_MESSAGE
    SETTING_NAME = Configuration.LOAN_LIMIT

class CannotReturn(CirculationException):
    status_code = 500

class CannotHold(CirculationException):
    status_code = 500

class PatronHoldLimitReached(CannotHold, LimitReached):
    BASE_DOC = HOLD_LIMIT_REACHED
    MESSAGE_WITH_LIMIT = SPECIFIC_HOLD_LIMIT_MESSAGE
    SETTING_NAME = Configuration.HOLD_LIMIT

class CannotReleaseHold(CirculationException):
    status_code = 500

class CannotFulfill(CirculationException):
    status_code = 500

class CannotPartiallyFulfill(CannotFulfill):
    status_code = 400

class FormatNotAvailable(CannotFulfill):
    """Our format information for this book was outdated, and it's
    no longer available in the requested format."""
    status_code = 502

class NotFoundOnRemote(CirculationException):
    """We know about this book but the remote site doesn't seem to."""
    status_code = 404

class NoLicenses(NotFoundOnRemote):
    """The library no longer has licenses for this book."""

    def as_problem_detail_document(self, debug=False):
        """Return a suitable problem detail document."""
        return NO_LICENSES

class CannotRenew(CirculationException):
    """The patron can't renew their loan on this book.

    Probably because it's not available for renewal.
    """
    status_code = 400

class NoAvailableCopies(CannotLoan):
    """The patron can't check this book out because all available
    copies are already checked out.
    """
    status_code = 400

class AlreadyCheckedOut(CannotLoan):
    """The patron can't put check this book out because they already have
    it checked out.
    """
    status_code = 400

class AlreadyOnHold(CannotHold):
    """The patron can't put this book on hold because they already have
    it on hold.
    """
    status_code = 400

class NotCheckedOut(CannotReturn):
    """The patron can't return this book because they don't
    have it checked out in the first place.
    """
    status_code = 400

class RemoteRefusedReturn(CannotReturn):
    """The remote refused to count this book as returned.
    """
    status_code = 500

class NotOnHold(CannotReleaseHold):
    """The patron can't release a hold for this book because they don't
    have it on hold in the first place.
    """
    status_code = 400

class CurrentlyAvailable(CannotHold):
    """The patron can't put this book on hold because it's available now."""
    status_code = 400

class NoAcceptableFormat(CannotFulfill):
    """We can't fulfill the patron's loan because the book is not available
    in an acceptable format.
    """
    status_code = 400

class FulfilledOnIncompatiblePlatform(CannotFulfill):
    """We can't fulfill the patron's loan because the loan was already
    fulfilled on an incompatible platform (i.e. Kindle) in a way that's
    exclusive to that platform.
    """
    status_code = 451

class NoActiveLoan(CannotFulfill):
    """We can't fulfill the patron's loan because they don't have an
    active loan.
    """
    status_code = 400

class PatronNotFoundOnRemote(NotFoundOnRemote):
    status_code = 404


