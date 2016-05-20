from core.problem_details import (
    INTEGRATION_ERROR,
    INTERNAL_SERVER_ERROR,
)

class CirculationException(Exception):
    """An exception occured when carrying out a circulation operation.

    `status_code` is the status code that should be returned to the patron.
    """
    status_code = 400

class InternalServerError(Exception):
    status_code = 500

    @property
    def as_problem_detail_document(self):
        """Return a suitable problem detail document."""
        return INTERNAL_SERVER_ERROR

class RemoteInitiatedServerError(InternalServerError):
    """One of the servers we communicate with had an internal error."""
    status_code = 502

    def __init__(self, message, service_name):
        super(RemoteInitiatedServerError, self).__init__(message)
        self.service_name = service_name

    @property
    def as_problem_detail_document(self):
        """Return a suitable problem detail document."""
        msg = "Integration error communicating with %s" % self.service_name
        return INTEGRATION_ERROR.detailed(msg)

class NoOpenAccessDownload(CirculationException):
    """We expected a book to have an open-access download, but it didn't."""
    status_code = 500

class AuthorizationFailedException(CirculationException):
    status_code = 401

class PatronAuthorizationFailedException(AuthorizationFailedException):
    status_code = 400

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

class PatronLoanLimitReached(CannotLoan):
    status_code = 403

class CannotReturn(CirculationException):
    status_code = 500

class CannotHold(CirculationException):
    status_code = 500

class PatronHoldLimitReached(CannotHold):
    status_code = 403

class CannotReleaseHold(CirculationException):
    status_code = 500

class CannotFulfill(CirculationException):
    status_code = 500

class NotFoundOnRemote(CirculationException):
    """We know about this book but the remote site doesn't seem to."""
    status_code = 404

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

class CouldCheckOut(CannotHold):
    """The patron can't put this book on hold because they could
    just check it out.
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
    status_code = 500

class NoActiveLoan(CannotFulfill):
    """We can't fulfill the patron's loan because they don't have an
    active loan.
    """
    status_code = 400
