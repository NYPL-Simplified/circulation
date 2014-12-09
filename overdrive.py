from nose.tools import set_trace
import base64
import datetime
import os
import json
import requests
import urlparse
import urllib
import sys

from model import (
    Credential,
    DataSource,
)

class OverdriveAPI(object):

    TOKEN_ENDPOINT = "https://oauth.overdrive.com/token"
    PATRON_TOKEN_ENDPOINT = "https://oauth-patron.overdrive.com/patrontoken"

    LIBRARY_ENDPOINT = "http://api.overdrive.com/v1/libraries/%(library_id)s"
    ALL_PRODUCTS_ENDPOINT = "http://api.overdrive.com/v1/collections/%(collection_token)s/products"
    METADATA_ENDPOINT = "http://api.overdrive.com/v1/collections/%(collection_token)s/products/%(item_id)s/metadata"
    EVENTS_ENDPOINT = "http://api.overdrive.com/v1/collections/%(collection_name)s/products?lastupdatetime=%(lastupdatetime)s&sort=%(sort)s&limit=%(limit)s"
    AVAILABILITY_ENDPOINT = "http://api.overdrive.com/v1/collections/%(collection_name)s/products/%(product_id)s/availability"

    CHECKOUTS_ENDPOINT = "http://patron.api.overdrive.com/v1/patrons/me/checkouts"
    ME_ENDPOINT = "http://patron.api.overdrive.com/v1/patrons/me"

    MAX_CREDENTIAL_AGE = 50 * 60

    PAGE_SIZE_LIMIT = 300
    EVENT_SOURCE = "Overdrive"

    EVENT_DELAY = datetime.timedelta(minutes=120)
    #EVENT_DELAY = datetime.timedelta(minutes=0)

    # The ebook formats we care about.
    FORMATS = "ebook-epub-open,ebook-epub-adobe,ebook-pdf-adobe,ebook-pdf-open"

    TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
    
    def __init__(self, _db):
        self._db = _db
        self.source = DataSource.lookup(_db, DataSource.OVERDRIVE)

        # Set some stuff from environment variables
        self.client_key = os.environ['OVERDRIVE_CLIENT_KEY']
        self.client_secret = os.environ['OVERDRIVE_CLIENT_SECRET']
        self.website_id = os.environ['OVERDRIVE_WEBSITE_ID']
        self.library_id = os.environ['OVERDRIVE_LIBRARY_ID']
        self.collection_name = os.environ['OVERDRIVE_COLLECTION_NAME']

        # Get set up with up-to-date credentials from the API.
        self.check_creds()
        self.collection_token = self.get_library()['collectionToken']

    def check_creds(self, force_refresh=False):
        """If the Bearer Token has expired, update it."""
        if force_refresh:
            refresh_on_lookup = lambda x: x
        else:
            refresh_on_lookup = self.refresh_creds

        credential = Credential.lookup(
            self._db, DataSource.OVERDRIVE, None, refresh_on_lookup)
        if force_refresh:
            self.refresh_creds(credential)
        self.token = credential.credential

    def refresh_creds(self, credential):
        """Fetch a new Bearer Token and update the given Credential object."""
        response = self.token_post(
            self.TOKEN_ENDPOINT,
            dict(grant_type="client_credentials"))
        data = response.json()
        self._update_credential(credential, data)
        self.token = credential.credential

    def get(self, url, extra_headers, exception_on_401=False):
        """Make an HTTP GET request using the active Bearer Token."""
        headers = dict(Authorization="Bearer %s" % self.token)
        headers.update(extra_headers)
        status_code, headers, content = Representation.simple_http_get(
            url, headers)
        if status_code == 401:
            if exception_on_401:
                # This is our second try. Give up.
                raise Exception("Something's wrong with the OAuth Bearer Token!")
            else:
                # Refresh the token and try again.
                return self.get(url, extra_headers, True)
        else:
            return status_code, headers, content

    def token_post(self, url, payload, headers={}):
        """Make an HTTP POST request for purposes of getting an OAuth token."""
        s = "%s:%s" % (self.client_key, self.client_secret)
        auth = base64.encodestring(s).strip()
        headers = dict(headers)
        headers['Authorization'] = "Basic %s" % auth
        return requests.post(url, payload, headers=headers)

    def _update_credential(self, credential, overdrive_data):
        """Copy Overdrive OAuth data into a Credential object."""
        credential.credential = overdrive_data['access_token']
        expires_in = (overdrive_data['expires_in'] * 0.9)
        credential.expires = datetime.datetime.utcnow() + datetime.timedelta(
            seconds=expires_in)
        self._db.commit()

    def get_library(self):
        url = self.LIBRARY_ENDPOINT % dict(library_id=self.library_id)
        representation, cached = Representation.get(
            self._db, url, self.get, data_source=self.source)
        return json.loads(representation.content)

    @classmethod
    def make_link_safe(self, url):
        """Turn a server-provided link into a link the server will accept!

        This is completely obnoxious and I have complained about it to
        Overdrive.
        """
        parts = list(urlparse.urlsplit(url))
        parts[2] = urllib.quote(parts[2])
        query_string = parts[3]
        query_string = query_string.replace("+", "%2B")
        query_string = query_string.replace(":", "%3A")
        query_string = query_string.replace("{", "%7B")
        query_string = query_string.replace("}", "%7D")
        parts[3] = query_string
        return urlparse.urlunsplit(tuple(parts))


class OverdriveRepresentationExtractor(object):

    """Extract useful information from Overdrive's JSON representations."""

    @classmethod
    def availability_link_list(self, book_list):
        """:return: A list of dictionaries with keys `id`, `title`,
        `availability_link`.
        """
        l = []
        if not 'products' in book_list:
            return []

        products = book_list['products']
        for product in products:
            data = dict(id=product['id'],
                        title=product['title'],
                        author_name=None)
            
            if 'primaryCreator' in product:
                creator = product['primaryCreator']
                if creator.get('role') == 'Author':
                    data['author_name'] = creator.get('name')
            links = product.get('links', [])
            if 'availability' in links:
                link = links['availability']['href']
                data['availability_link'] = OverdriveAPI.make_link_safe(link)
            else:
                log.warn("No availability link for %s" % book_id)
            l.append(data)
        return l

    @classmethod
    def link(self, page, rel):
        if 'links' in page and rel in page['links']:
            raw_link = page['links'][rel]['href']
            link = OverdriveAPI.make_link_safe(raw_link)
        else:
            link = None
        return link
