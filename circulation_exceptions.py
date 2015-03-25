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

class NotCheckedOut(CannotReturn):
    """The patron can't return this book because they don't
    have it checked out in the first place.
    """

class NotOnHold(CannotReleaseHold):
    """The patron can't release a hold for this book because they don't
    have it on hold in the first place.
    """

class CurrentlyAvailable(CannotHold):
    """The patron can't put this book on hold because it's available now."""

