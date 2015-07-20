class NoOpenAccessDownload(Exception):
    """We expected a book to have an open-access download, but it didn't."""

class CirculationException(Exception):
    pass

class CannotLoan(CirculationException):
    pass

class CannotReturn(CirculationException):
    pass

class CannotHold(CirculationException):
    pass

class CannotReleaseHold(CirculationException):
    pass

class CannotFulfill(CirculationException):
    pass

class CannotRenew(CirculationException):
    """The patron can't renew their loan on this book."""

class NoAvailableCopies(CannotLoan):
    """The patron can't check this book out because all available
    copies are already checked out.
    """

class AlreadyCheckedOut(CannotLoan):
    """The patron can't put check this book out because they already have
    it checked out.
    """

class AlreadyOnHold(CannotHold):
    """The patron can't put this book on hold because they already have
    it on hold.
    """

class CouldCheckOut(CannotHold):
    """The patron can't put this book on hold because they could
    just check it out.
    """

class NotCheckedOut(CannotReturn):
    """The patron can't return this book because they don't
    have it checked out in the first place.
    """

class RemoteRefusedReturn(CannotReturn):
    """The remote refused to count this book as returned.
    """

class NotOnHold(CannotReleaseHold):
    """The patron can't release a hold for this book because they don't
    have it on hold in the first place.
    """

class CurrentlyAvailable(CannotHold):
    """The patron can't put this book on hold because it's available now."""

class NoAcceptableFormat(CannotFulfill):
    """We can't fulfill the patron's loan because the book is not available
    in an acceptable format.
    """

class NoActiveLoan(CannotFulfill):
    """We can't fulfill the patron's loan because they don't have an
    active loan.
    """
