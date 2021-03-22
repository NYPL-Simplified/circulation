class BaseError(Exception):
    """Base class for all errors"""

    def __init__(self, message=None, inner_exception=None):
        """Initializes a new instance of BaseError class

        :param message: String containing description of the error occurred
        :param inner_exception: (Optional) Inner exception
        """
        if inner_exception and not message:
            message = str(inner_exception)

        super(BaseError, self).__init__(message)

        self._inner_exception = str(inner_exception)

    @property
    def inner_exception(self):
        """Returns an inner exception

        :return: Inner exception
        :rtype: Exception
        """
        return self._inner_exception

    def __eq__(self, other):
        """Compares two BaseError objects

        :param other: BaseError object
        :type other: BaseError

        :return: Boolean value indicating whether two items are equal
        :rtype: bool
        """
        if not isinstance(other, BaseError):
            return False

        return str(self) == str(other)

    def __repr__(self):
        return '<BaseError(message={0}, inner_exception={1})>'.format(
            (self),
            self.inner_exception
        )

