class CheckoutException(Exception):
    pass

class NoAvailableCopies(CheckoutException):
    pass
