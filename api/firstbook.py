from flask_babel import lazy_gettext as _
import requests
import logging
from .authenticator import (
    BasicAuthenticationProvider,
    PatronData,
)
from .config import (
    Configuration,
    CannotLoadConfiguration,
)
from .circulation_exceptions import RemoteInitiatedServerError
import urllib.parse
from core.model import (
    get_one_or_create,
    ExternalIntegration,
    Patron,
)


class FirstBookAuthenticationAPI(BasicAuthenticationProvider):

    NAME = 'First Book (New 8/2022)'

    DESCRIPTION = _("""
        An authentication service for Open eBooks that authenticates
        using access codes and PINs. (This is the new version as of 8/2022.)""")

    DISPLAY_NAME = NAME
    DEFAULT_IDENTIFIER_LABEL = _("Access Code")
    LOGIN_BUTTON_IMAGE = "FirstBookLoginButton280.png"

    # If FirstBook sends this message it means they accepted the
    # patron's credentials.
    SUCCESS_MESSAGE = 'Valid Code Pin Pair'

    # Server-side validation happens before the identifier
    # is converted to uppercase, which means lowercase characters
    # are valid.
    DEFAULT_IDENTIFIER_REGULAR_EXPRESSION = '^[A-Za-z0-9@]+$'
    DEFAULT_PASSWORD_REGULAR_EXPRESSION = '^[0-9]+$'

    API_PATH = 'rest/V1/serialcode?'

    SETTINGS = [
        {"key": ExternalIntegration.URL, "format": "url",
            "label": _("URL"), "required": True},
        {"key": ExternalIntegration.PASSWORD,
            "label": _("Key"), "required": True},
    ] + BasicAuthenticationProvider.SETTINGS

    log = logging.getLogger("First Book authentication API")

    def __init__(self, library_id, integration, analytics=None, root=None, secret=None):
        super(FirstBookAuthenticationAPI, self).__init__(
            library_id, integration, analytics)
        self.key = secret or integration.password
        if not root:
            root = integration.url
            if not (root and self.key):
                raise CannotLoadConfiguration(
                    "First Book server not configured."
                )
        self.root = root

    # Begin implementation of BasicAuthenticationProvider abstract
    # methods.

    def remote_authenticate(self, username, password):
        # All FirstBook credentials are in upper-case.
        username = username.upper()

        # If they fail a PIN test, there is no authenticated patron.
        if not self.remote_pin_test(username, password):
            return None

        # FirstBook keeps track of absolutely no information
        # about the patron other than the permanent ID,
        # which is also the authorization identifier.
        return PatronData(
            permanent_id=username,
            authorization_identifier=username,
        )

    # End implementation of BasicAuthenticationProvider abstract methods.

    def remote_pin_test(self, barcode, pin):
        url = self.root + self.API_PATH + "code=%s&pin=%s" % (barcode, pin)
        header = {'Authorization': 'Bearer %s' % self.key}
        try:
            response = self.request(url, header)
        except requests.exceptions.ConnectionError as e:
            raise RemoteInitiatedServerError(
                str(e),
                self.NAME
            )
        content = response.content.decode("utf8")
        if response.status_code != 200:
            msg = "Got unexpected response code %d. Content: %s" % (
                response.status_code, content
            )
            raise RemoteInitiatedServerError(msg, self.NAME)
        if self.SUCCESS_MESSAGE in content:
            return True
        return False

    def request(self, url, header={}):
        """Make an HTTP request.

        Defined to be overridden in test mock.
        """
        return requests.get(url, headers=header)


class MockFirstBookResponse(object):

    def __init__(self, status_code, content):
        self.status_code = status_code
        # Guarantee that the response content is always a bytestring,
        # as it would be in real life.
        if isinstance(content, str):
            content = content.encode("utf8")
        self.content = content


class MockFirstBookAuthenticationAPI(FirstBookAuthenticationAPI):

    SUCCESS = '"Valid Code Pin Pair"'
    FAILURE = '{"code":404,"message":"Access Code Pin Pair not found"}'

    def __init__(self, library, integration, valid={}, bad_connection=False,
                 failure_status_code=None):
        super(MockFirstBookAuthenticationAPI, self).__init__(
            library, integration, root="http://example.com/"
        )
        self.identifier_re = None
        self.password_re = None
        self.valid = valid
        self.bad_connection = bad_connection
        self.failure_status_code = failure_status_code

    def request(self, url, header):
        if not header:
            raise RemoteInitiatedServerError
        if self.bad_connection:
            # Simulate a bad connection.
            raise requests.exceptions.ConnectionError("Could not connect!")
        elif self.failure_status_code:
            # Simulate a server returning an unexpected error code.
            return MockFirstBookResponse(
                self.failure_status_code, "Error %s" % self.failure_status_code
            )
        qa = urllib.parse.parse_qs(url)
        for key in qa:
            if key == 'pin':
                (pin,) = qa['pin']
            else:
                (code,) = qa[key]
        if code in self.valid and self.valid[code] == pin:
            return MockFirstBookResponse(200, self.SUCCESS)
        else:
            return MockFirstBookResponse(200, self.FAILURE)


# Specify which of the classes defined in this module is the
# authentication provider.
AuthenticationProvider = FirstBookAuthenticationAPI
