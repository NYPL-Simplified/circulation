from nose.tools import set_trace
import base64
import datetime
import os
import json
import logging
import requests
import urlparse
import urllib
import sys

from model import (
    Credential,
    DataSource,
    Representation,
)
from config import (
    Configuration,
    CannotLoadConfiguration,
)

class OverdriveAPI(object):

    log = logging.getLogger("Overdrive API")

    TOKEN_ENDPOINT = "https://oauth.overdrive.com/token"
    PATRON_TOKEN_ENDPOINT = "https://oauth-patron.overdrive.com/patrontoken"

    LIBRARY_ENDPOINT = "http://api.overdrive.com/v1/libraries/%(library_id)s"
    ALL_PRODUCTS_ENDPOINT = "http://api.overdrive.com/v1/collections/%(collection_token)s/products"
    METADATA_ENDPOINT = "http://api.overdrive.com/v1/collections/%(collection_token)s/products/%(item_id)s/metadata"
    EVENTS_ENDPOINT = "http://api.overdrive.com/v1/collections/%(collection_name)s/products?lastupdatetime=%(lastupdatetime)s&sort=%(sort)s&limit=%(limit)s"
    AVAILABILITY_ENDPOINT = "http://api.overdrive.com/v1/collections/%(collection_name)s/products/%(product_id)s/availability"

    CHECKOUTS_ENDPOINT = "http://patron.api.overdrive.com/v1/patrons/me/checkouts"
    CHECKOUT_ENDPOINT = "http://patron.api.overdrive.com/v1/patrons/me/checkouts/%(overdrive_id)s"
    FORMATS_ENDPOINT = "http://patron.api.overdrive.com/v1/patrons/me/checkouts/%(overdrive_id)s/formats"
    HOLDS_ENDPOINT = "http://patron.api.overdrive.com/v1/patrons/me/holds"
    HOLD_ENDPOINT = "http://patron.api.overdrive.com/v1/patrons/me/holds/%(product_id)s"
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
        values = self.environment_values()
        if len([x for x in values if not x]):
            self.log.info(
                "No Overdrive client configured."
            )
            raise CannotLoadConfiguration("No Overdrive client configured.")

        (self.client_key, self.client_secret, self.website_id, 
         self.library_id, self.collection_name) = values

        # Get set up with up-to-date credentials from the API.
        self.check_creds()
        self.collection_token = self.get_library()['collectionToken']


    @classmethod
    def environment_values(cls):
        value = Configuration.integration('Overdrive')
        values = []
        for name in [
                'client_key',
                'client_secret',
                'website_id',
                'library_id',
                'collection_name',
        ]:
            var = value.get(name)
            if var:
                var = var.encode("utf8")
            values.append(var)
        return values

    @classmethod
    def from_environment(cls, _db):
        # Make sure all environment values are present. If any are missing,
        # return None. Otherwise return an OverdriveAPI object.
        values = cls.environment_values()
        if len([x for x in values if not x]):
            cls.log.info(
                "No Overdrive client configured."
            )
            return None
        return cls(_db)

    def check_creds(self, force_refresh=False):
        """If the Bearer Token has expired, update it."""
        if force_refresh:
            refresh_on_lookup = lambda x: x
        else:
            refresh_on_lookup = self.refresh_creds

        credential = Credential.lookup(
            self._db, DataSource.OVERDRIVE, None, None, refresh_on_lookup)
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
                self.check_creds(True)
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
        representation, cached = Representation.get(self._db, url, self.get)
        return json.loads(representation.content)

    def all_ids(self):
        """Get IDs for every book in the system, with (more or less) the most
        recent ones at the front.
        """
        params = dict(collection_token=self.collection_token)
        starting_link = self.make_link_safe(
            self.ALL_PRODUCTS_ENDPOINT % params)

        # Get the first page so we can find the 'last' link.
        status_code, headers, content = self.get(starting_link, {})
        try:
            data = json.loads(content)
        except Exception, e:
            self.log.error("OVERDRIVE ERROR: %r %r %r",
                          status_code, headers, content)
            return
        previous_link = OverdriveRepresentationExtractor.link(data, 'last')

        while previous_link:
            page_inventory, previous_link = self._get_book_list_page(
                previous_link, 'prev')
            for i in page_inventory:
                yield i


    def recently_changed_ids(self, start, cutoff):
        """Get IDs of books whose status has changed between the start time
        and now.
        """
        # `cutoff` is not supported by Overdrive, so we ignore it. All
        # we can do is get events between the start time and now.

        last_update_time = start-self.EVENT_DELAY
        self.log.info("Now: %s Asking for: %s", start, last_update_time)
        params = dict(lastupdatetime=last_update_time,
                      sort="popularity:desc",
                      limit=self.PAGE_SIZE_LIMIT,
                      collection_name=self.collection_name)
        next_link = self.make_link_safe(self.EVENTS_ENDPOINT % params)
        while next_link:
            page_inventory, next_link = self._get_book_list_page(next_link)
            # We won't be sending out any events for these books yet,
            # because we don't know if anything changed, but we will
            # be putting them on the list of inventory items to
            # refresh. At that point we will send out events.
            for i in page_inventory:
                yield i

    def metadata_lookup(self, identifier):
        """Look up metadata for an Overdrive identifier.
        """
        url = self.METADATA_ENDPOINT % dict(
            collection_token=self.collection_token,
            item_id=identifier.identifier
        )
        status_code, headers, content = self.get(url, {})
        return json.loads(content)

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
                logging.getLogger("Overdrive API").warn(
                    "No availability link for %s", book_id)
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
