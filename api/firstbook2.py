from flask_babel import lazy_gettext as _
import jwt
from jwt.algorithms import HMACAlgorithm
import requests
import logging
import time

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

    NAME = 'First Book'

    DESCRIPTION = _("""
        An authentication service for Open eBooks that authenticates
        using access codes and PINs. (This is the new version.)""")

    DISPLAY_NAME = NAME
    DEFAULT_IDENTIFIER_LABEL = _("Access Code")
    LOGIN_BUTTON_IMAGE = "FirstBookLoginButton280.png"

    # The algorithm used to sign JWTs.
    ALGORITHM = 'HS256'

    # If FirstBook sends this message it means they accepted the
    # patron's credentials.
    SUCCESS_MESSAGE = 'Valid Code Pin Pair'

    # Server-side validation happens before the identifier
    # is converted to uppercase, which means lowercase characters
    # are valid.
    DEFAULT_IDENTIFIER_REGULAR_EXPRESSION = '^[A-Za-z0-9@]+$'
    DEFAULT_PASSWORD_REGULAR_EXPRESSION = '^[0-9]+$'

    SETTINGS = [
        {
            "key": ExternalIntegration.URL, "format": "url", "label": _("URL"),
            "default": "https://ebooksprod.firstbook.org/api/",
            "required": True
        },
        { "key": ExternalIntegration.PASSWORD, "label": _("Key"), "required": True },
    ] + BasicAuthenticationProvider.SETTINGS

    log = logging.getLogger("First Book JWT authentication API")

    def __init__(self, library_id, integration, analytics=None, root=None,
                 secret=None):
        super(FirstBookAuthenticationAPI, self).__init__(library_id, integration, analytics)
        root = root or integration.url
        secret = secret or integration.password
        if not (root and secret):
            raise CannotLoadConfiguration(
                "First Book server not configured."
            )
        self.root = root
        self.secret = secret

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
        jwt = self.jwt(barcode, pin)
        url = self.root + jwt
        try:
            response = self.request(url)
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

    def jwt(self, barcode, pin):
        """Create and sign a JWT with the payload expected by the
        First Book API.
        """
        now = str(int(time.time()))
        payload = dict(
            barcode=barcode,
            pin=pin,
            iat=now,
        )
        return jwt.encode(payload, self.secret, algorithm=self.ALGORITHM).decode("utf-8")

    def request(self, url):
        """Make an HTTP request.

        Defined solely so it can be overridden in the mock.
        """
        return requests.get(url)


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
            library, integration, root="http://example.com/",
            secret="secret"
        )
        self.identifier_re = None
        self.password_re = None

        self.valid = valid
        self.bad_connection = bad_connection
        self.failure_status_code = failure_status_code

        self.request_urls = []

    def request(self, url):
        self.request_urls.append(url)
        if self.bad_connection:
            # Simulate a bad connection.
            raise requests.exceptions.ConnectionError("Could not connect!")
        elif self.failure_status_code:
            # Simulate a server returning an unexpected error code.
            return MockFirstBookResponse(
                self.failure_status_code, "Error %s" % self.failure_status_code
            )
        parsed = urllib.parse.urlparse(url)
        token = parsed.path.split("/")[-1]
        barcode, pin = self._decode(token)

        # The barcode and pin must be present in self.valid.
        if barcode in self.valid and self.valid[barcode] == pin:
            return MockFirstBookResponse(200, self.SUCCESS)
        else:
            return MockFirstBookResponse(200, self.FAILURE)

    def _decode(self, token):
        # Decode a JWT. Only used in tests -- in production, this is
        # First Book's job.

        # The JWT must be signed with the shared secret.
        payload = jwt.decode(token, self.secret, algorithms=self.ALGORITHM)

        # The 'iat' field in the payload must be a recent timestamp.
        assert (time.time()-int(payload['iat'])) < 2

        return payload['barcode'], payload['pin']



# Specify which of the classes defined in this module is the
# authentication provider.
AuthenticationProvider = FirstBookAuthenticationAPI
