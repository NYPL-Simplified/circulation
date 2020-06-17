class SAMLError(Exception):
    """Base class for all SAML-related errors"""

    def __init__(self, message=None, inner_exception=None):
        """Initializes a new instance of SAMLError class

        :param message: String containing description of the error occurred
        :param inner_exception: (Optional) Inner exception
        """
        if inner_exception and not message:
            message = inner_exception.message

        super(SAMLError, self).__init__(message)

        self._inner_exception = inner_exception

    @property
    def inner_exception(self):
        """Returns an inner exception

        :return: Inner exception
        :rtype: Exception
        """
        return self._inner_exception
