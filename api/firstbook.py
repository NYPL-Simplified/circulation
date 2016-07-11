from nose.tools import set_trace
import requests
import logging
from authenticator import BasicAuthAuthenticator
from config import Configuration
from circulation_exceptions import RemoteInitiatedServerError
import urlparse
import urllib
from core.model import (
    get_one_or_create,
    Patron,
)

class FirstBookAuthenticationAPI(BasicAuthAuthenticator):

    NAME = 'First Book'

    LOGIN_LABEL = "Access Code"

    SUCCESS_MESSAGE = 'Valid Code Pin Pair'

    SECRET_KEY = 'key'

    log = logging.getLogger("First Book authentication API")

    def __init__(self, host, key, test_username=None, test_password=None):
        if '?' in host:
            host += '&'
        else:
            host += '?'
        self.root = host + 'key=' + key
        self.test_username = test_username
        self.test_password = test_password

    @classmethod
    def from_config(cls):
        config = Configuration.integration(cls.NAME, required=True)
        host = config.get(Configuration.URL)
        key = config.get(cls.SECRET_KEY)
        if not host:
            cls.log.warning("No First Book client configured.")
            return None
        test_username = config.get(Configuration.AUTHENTICATION_TEST_USERNAME)
        test_password = config.get(Configuration.AUTHENTICATION_TEST_PASSWORD)
        return cls(host, key, test_username=test_username, 
                   test_password=test_password)

    def request(self, url):
        return requests.get(url)

    def dump(self, barcode):
        return {}

    def patron_info(self, barcode):
        return dict(barcode=barcode)

    def pintest(self, barcode, pin):
        url = self.root + "&accesscode=%s&pin=%s" % tuple(map(
            urllib.quote, (barcode, pin)
        ))
        try:
            response = self.request(url)
        except requests.exceptions.ConnectionError, e:
            raise RemoteInitiatedServerError(str(e.message))
        if response.status_code != 200:
            msg = "Got unexpected response code %d. Content: %s" % (
                response.status_code, response.content
            )
            raise RemoteInitiatedServerError(msg)
        if self.SUCCESS_MESSAGE in response.content:
            return True
        return False

    def authenticated_patron(self, _db, header):
        identifier = header.get('username')
        password = header.get('password')

        # If they fail basic validation, there is no authenticated patron.
        if not self.server_side_validation(identifier, password):
            return None

        # All FirstBook credentials are in upper-case.
        identifier = identifier.upper()

        # If they fail a PIN test, there is no authenticated patron.
        if not self.pintest(identifier, password):
            return None

        # First Book thinks this is a valid patron. Find or create a
        # corresponding Patron in our database.
        kwargs = {Patron.authorization_identifier.name: identifier}
        __transaction = _db.begin_nested()
        patron, is_new = get_one_or_create(
            _db, Patron, external_identifier=identifier,
            authorization_identifier=identifier,
        )
        __transaction.commit()
        return patron

class DummyFirstBookResponse(object):

    def __init__(self, status_code, content):
        self.status_code = status_code
        self.content = content

class DummyFirstBookAuthentationAPI(FirstBookAuthenticationAPI):

    SUCCESS = '"Valid Code Pin Pair"'
    FAILURE = '{"code":404,"message":"Access Code Pin Pair not found"}'

    def __init__(self, valid={}):
        self.root = "http://example.com/"
        self.valid = valid

    def request(self, url):
        qa = urlparse.parse_qs(url)
        if 'accesscode' in qa and 'pin' in qa:
            [code] = qa['accesscode']
            [pin] = qa['pin']
            if code in self.valid and self.valid[code] == pin:
                return DummyFirstBookResponse(200, self.SUCCESS)
            else:
                return DummyFirstBookResponse(200, self.FAILURE)


AuthenticationAPI = FirstBookAuthenticationAPI
