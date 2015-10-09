from nose.tools import set_trace
import requests
import logging
from authenticator import Authenticator
from config import Configuration
import urlparse
import urllib
from core.model import (
    get_one_or_create,
    Patron,
)

class FirstBookAuthenticationAPI(Authenticator):

    SUCCESS_MESSAGE = 'Valid Code Pin Pair'

    SECRET_KEY = 'key'
    FIRSTBOOK = 'First Book'

    log = logging.getLogger("First Book authentication API")

    def __init__(self, host, key):
        if '?' in host:
            host += '&'
        else:
            host += '?'
        self.root = host + 'key=' + key

    @classmethod
    def from_config(cls):
        config = Configuration.integration(cls.FIRSTBOOK, required=True)
        host = config.get(Configuration.URL)
        key = config.get(cls.SECRET_KEY)
        if not host:
            cls.log.warning("No First Book client configured.")
            return None
        return cls(host, key)

    def request(self, url):
        return requests.get(url)

    def dump(self, barcode):
        return {}

    def pintest(self, barcode, pin):
        url = self.root + "&accesscode=%s&pin=%s" % tuple(map(
            urllib.quote, (barcode, pin)
        ))
        print url
        response = self.request(url)
        print response.content
        if response.status_code != 200:
            msg = "Got unexpected response code %d. Content: %s" % (
                response.status_code, response.content
            )
            raise Exception(msg)
        if self.SUCCESS_MESSAGE in response.content:
            return True
        return False

    def authenticated_patron(self, _db, identifier, password):
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


