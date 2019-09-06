class Dialect:
    """Describe a SIP2 dialect.
    """

    # Constants for each class
    GENERIC_ILS = 'GenericILS'
    AG_VERSO = 'AutoGraphicsVerso'

    # Settings defined in each class
    sendEndSession = None

    # Map a string to the correct class
    @staticmethod
    def load_dialect(dialect):
        if dialect == Dialect.GENERIC_ILS:
            return GenericILS
        elif dialect == Dialect.AG_VERSO:
            return AutoGraphicsVerso
        else:
            return None

class GenericILS(Dialect):
    sendEndSession = True

class AutoGraphicsVerso(Dialect):
    sendEndSession = False
