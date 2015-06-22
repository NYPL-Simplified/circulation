from nose.tools import set_trace
import base64
import requests
import os
import json

from util.xmlparser import XMLParser

class Axis360API(object):

    DEFAULT_BASE_URL = "http://axis360apiqa.baker-taylor.com/Services/VendorAPI/"
    
    DATE_FORMAT = "%m-%d-%Y %H:%M:%S"

    access_token_endpoint = 'accesstoken'
    availability_endpoint = 'availability/v2'

    def __init__(self, _db, username=None, library_id=None, password=None,
                 base_url=DEFAULT_BASE_URL):
        self._db = _db
        self.library_id = library_id or os.environ['AXIS_360_LIBRARY_ID']
        self.username = username or os.environ['AXIS_360_USERNAME']
        self.password = password or os.environ['AXIS_360_PASSWORD']
        self.base_url = base_url
        self.token = None
        #self.source = DataSource.lookup(self._db, DataSource.AXIS_360)

    @property
    def authorization_headers(self):
        authorization = u":".join([self.username, self.password, self.library_id])
        authorization = authorization.encode("utf_16_le")
        print authorization
        authorization = base64.b64encode(authorization)
        return dict(Authorization="Basic " + authorization)

    def refresh_bearer_token(self):
        url = self.base_url + self.access_token_endpoint
        headers = self.authorization_headers
        response = self._make_request(url, 'post', headers)
        if response.status_code != 200:
            raise Exception(
                "Could not acquire bearer token: %s, %s" % (
                    response.status_code, response.content))
        return self.parse_token(response.content)

    def request(self, url, method='get', extra_headers={}, data=None,
                exception_on_401=False):
        """Make an HTTP request, acquiring/refreshing a bearer token
        if necessary.
        """
        if not self.token:
            self.token = self.refresh_bearer_token()

        headers = dict(extra_headers)
        headers['Authorization'] = "Bearer " + self.token
        headers['Library'] = self.library_id
        response = self._make_request(url, method, headers, data)
        if response.status_code == 401:
            if exception_on_401:
                # This is our second try. Give up.
                raise Exception(
                    "Something's wrong with the OAuth Bearer Token!")
            else:
                # The token has expired. Get a new token and try again.
                self.token = None
                return self.request(method, url, extra_headers, data, True)
        else:
            return response

    def availability(self, since=None):
        url = self.base_url + self.availability_endpoint
        if since:
            since = since.strftime(self.DATE_FORMAT)
            url += "?updatedDate=%s" % since
        print url
        return self.request(url)

    @classmethod
    def parse_token(cls, token):
        data = json.loads(token)
        return data['access_token']

    def _make_request(self, url, method, headers, data=None):
        """Actually make an HTTP request."""
        print url, headers
        return requests.request(
            url=url, method=method, headers=headers, data=data)


class AvailabilityParser(XMLParser):

    NS = {"bt": "http://axis360api.baker-taylor.com/vendorAPI"}

    def __init__(self, include_availability=True, include_metadata=True):
        self.include_availability = include_availability
        self.include_metadata = include_metadata

    def process_all(self, string):

        for i in super(AvailabilityParser, self).process_all(
                string, "//bt:title", self.NS):
            print i

    def process_one(self, element, ns):
        identifier = self.text_of_subtag(element, 'bt:titleId', ns)
        title = self.text_of_subtag(element, 'bt:productTitle', ns)
        isbn = self.text_of_optional_subtag(element, 'bt:isbn', ns)
        subject = self.text_of_optional_subtag(element, 'bt:subject', ns)
        publication_date = self.text_of_optional_subtag(
            element, 'bt:publicationDate', ns)
        series = self.text_of_optional_subtag('series')
        publisher = self.text_of_optional_subtag('publisher')
        imprint = self.text_of_optional_subtag('imprint')
        audience = self.text_of_optional_subtag('audience')
        contributor = self.text_of_optional_subtag('contributor')

        # TODO: 
        from lxml import etree
        print etree.tostring(element)
        set_trace()
        pass

data = open("tests/files/axis/collection.xml").read()
parser = AvailabilityParser()
parser.process_all(data)

# from datetime import datetime, timedelta
# one_year_ago = datetime.utcnow() - timedelta(days=365)
# api = Axis360API(None)
# response = api.availability(one_year_ago)
