from nose.tools import set_trace
import requests
from authenticator import Authenticator
from config import Configuration
import urlparse
import urllib

class FirstBookAuthenticationAPI(Authenticator):

    SUCCESS_MESSAGE = 'Valid Code Pin Pair'

    def __init__(self, host, key):
        if '?' in host:
            host += '&'
        else:
            host += '?'
        self.host = host + 'key=' + key

    SECRET_KEY = 'key'

    @classmethod
    def from_config(cls):
        config = Configuration.integration(
            Configuration.FIRSTBOOK, required=True)
        host = config.get(Configuration.URL)
        key = config.get(cls.SECRET_KEY)
        if not host:
            cls.log.info("No First Book client configured.")
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
        response = self.request(url)
        if response.status_code != 200:
            msg = "Got unexpected response code %d. Content: %s" % (
                response.status_code, response.content
            )
            raise Exception(msg)
        print response.content
        if self.SUCCESS_MESSAGE in response.content:
            return True
        return False


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


