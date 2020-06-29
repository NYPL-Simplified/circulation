from nose.tools import set_trace
import datetime
import isbnlib
import os
import json
import logging
import urlparse
import urllib
import sys

from sqlalchemy.orm.exc import (
    NoResultFound,
)
from sqlalchemy.orm.session import Session

from classifier import Classifier
from config import (
    temp_config,
    CannotLoadConfiguration,
    Configuration,
)

from model import (
    get_one,
    get_one_or_create,
    Classification,
    Collection,
    ConfigurationSetting,
    Contributor,
    Credential,
    DataSource,
    DeliveryMechanism,
    Edition,
    ExternalIntegration,
    Hyperlink,
    Identifier,
    Library,
    Measurement,
    MediaTypes,
    Representation,
    Subject,
)

from metadata_layer import (
    CirculationData,
    ContributorData,
    FormatData,
    IdentifierData,
    Metadata,
    MeasurementData,
    LinkData,
    SubjectData,
)

from coverage import (
    BibliographicCoverageProvider,
)

from testing import DatabaseTest

from util.http import (
    HTTP,
    BadResponseException,
)
from util.string_helpers import base64
from util.worker_pools import RLock

from testing import MockRequestsResponse

class OverdriveAPI(object):

    log = logging.getLogger("Overdrive API")

    # A lock for threaded usage.
    lock = RLock()

    # Production and testing have different host names for some of the
    # API endpoints. This is configurable on the collection level.
    SERVER_NICKNAME = "server_nickname"
    PRODUCTION_SERVERS = "production"
    TESTING_SERVERS = "testing"
    HOSTS = {
        PRODUCTION_SERVERS : dict(
            host="https://api.overdrive.com",
            patron_host="https://patron.api.overdrive.com",
        ),
        TESTING_SERVERS : dict(
            host="https://integration.api.overdrive.com",
            patron_host="https://integration-patron.api.overdrive.com",
        )
    }

    # Production and testing setups use the same URLs for Client
    # Authentication and Patron Authentication, but we use the same
    # system as for other hostnames to give a consistent look to the
    # templates.
    for host in HOSTS.values():
        host['oauth_patron_host'] = "https://oauth-patron.overdrive.com"
        host['oauth_host'] = "https://oauth.overdrive.com"

    # Each of these endpoint URLs has a slot to plug in one of the
    # appropriate servers. This will be filled in either by a call to
    # the endpoint() method (if there are other variables in the
    # template), or by the _do_get or _do_post methods (if there are
    # no other variables).
    TOKEN_ENDPOINT = "%(oauth_host)s/token"
    PATRON_TOKEN_ENDPOINT = "%(oauth_patron_host)s/patrontoken"

    LIBRARY_ENDPOINT = "%(host)s/v1/libraries/%(library_id)s"
    ADVANTAGE_LIBRARY_ENDPOINT = "%(host)s/v1/libraries/%(parent_library_id)s/advantageAccounts/%(library_id)s"
    ALL_PRODUCTS_ENDPOINT = "%(host)s/v1/collections/%(collection_token)s/products?sort=%(sort)s"
    METADATA_ENDPOINT = "%(host)s/v1/collections/%(collection_token)s/products/%(item_id)s/metadata"
    EVENTS_ENDPOINT = "%(host)s/v1/collections/%(collection_token)s/products?lastUpdateTime=%(lastupdatetime)s&sort=%(sort)s&limit=%(limit)s"
    AVAILABILITY_ENDPOINT = "%(host)s/v1/collections/%(collection_token)s/products/%(product_id)s/availability"

    PATRON_INFORMATION_ENDPOINT = "%(patron_host)s/v1/patrons/me"
    CHECKOUTS_ENDPOINT = "%(patron_host)s/v1/patrons/me/checkouts"
    CHECKOUT_ENDPOINT = "%(patron_host)s/v1/patrons/me/checkouts/%(overdrive_id)s"
    FORMATS_ENDPOINT = "%(patron_host)s/v1/patrons/me/checkouts/%(overdrive_id)s/formats"
    HOLDS_ENDPOINT = "%(patron_host)s/v1/patrons/me/holds"
    HOLD_ENDPOINT = "%(patron_host)s/v1/patrons/me/holds/%(product_id)s"
    ME_ENDPOINT = "%(patron_host)s/v1/patrons/me"

    MAX_CREDENTIAL_AGE = 50 * 60

    PAGE_SIZE_LIMIT = 300
    EVENT_SOURCE = "Overdrive"

    EVENT_DELAY = datetime.timedelta(minutes=120)

    # The formats we care about.
    FORMATS = "ebook-epub-open,ebook-epub-adobe,ebook-pdf-adobe,ebook-pdf-open,audiobook-overdrive".split(",")

    # The formats that can be read by the default Library Simplified reader.
    DEFAULT_READABLE_FORMATS = set(
        ["ebook-epub-open", "ebook-epub-adobe", "ebook-pdf-open", 
         "audiobook-overdrive"]
    )

    # The formats that indicate the book has been fulfilled on an
    # incompatible platform and just can't be fulfilled on Simplified
    # in any format.
    INCOMPATIBLE_PLATFORM_FORMATS = set(["ebook-kindle"])

    OVERDRIVE_READ_FORMAT = "ebook-overdrive"

    TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

    WEBSITE_ID = u"website_id"

    # When associating an Overdrive account with a library, it's
    # necessary to also specify an "ILS name" obtained from
    # Overdrive. Components that don't authenticate patrons (such as
    # the metadata wrangler) don't need to set this value.
    ILS_NAME_KEY = u"ils_name"
    ILS_NAME_DEFAULT = u"default"

    def __init__(self, _db, collection):
        if collection.protocol != ExternalIntegration.OVERDRIVE:
            raise ValueError(
                "Collection protocol is %s, but passed into OverdriveAPI!" %
                collection.protocol
            )
        self._db = _db
        self.library_id = collection.external_account_id
        self.collection_id = collection.id
        if collection.parent:
            # This is an Overdrive Advantage account.
            self.parent_library_id = collection.parent.external_account_id

            # We're going to inherit all of the Overdrive credentials
            # from the parent (the main Overdrive account), except for the
            # library ID, which we already set.
            collection = collection.parent
        else:
            self.parent_library_id = None

        integration = collection.external_integration
        self.client_key = integration.username
        self.client_secret = integration.password
        self.website_id = integration.setting(self.WEBSITE_ID).value
        if (not self.client_key or not self.client_secret or not self.website_id
            or not self.library_id):
            raise CannotLoadConfiguration(
                "Overdrive configuration is incomplete."
            )

        # Figure out which hostnames we'll be using when constructing
        # endpoint URLs.
        server_nickname = (
            integration.setting(self.SERVER_NICKNAME).value
            or self.PRODUCTION_SERVERS
        )
        if server_nickname not in self.HOSTS:
            server_nickname = self.PRODUCTION_SERVERS

        # Set the hostnames we'll be using. Make a new dictionary just
        # to be safe.
        self.hosts = dict(self.HOSTS[server_nickname])

        # Use utf8 instead of unicode encoding
        settings = [self.client_key, self.client_secret, self.website_id]
        self.client_key, self.client_secret, self.website_id = (
            setting.encode('utf8') for setting in settings
        )

        # This is set by an access to .token, or by a call to
        # check_creds() or refresh_creds().
        self._token = None

        # This is set by an access to .collection_token
        self._collection_token = None

    def endpoint(self, url, **kwargs):
        """Create the URL to an Overdrive API endpoint.

        :param url: A template for the URL.
        :param kwargs: Arguments to be interpolated into the template.
           The server hostname will be interpolated automatically; you
           don't have to pass it in.
        """
        if not '%(' in url:
            # Nothing to interpolate.
            return url
        kwargs.update(self.hosts)
        return url % kwargs

    @property
    def token(self):
        if not self._token:
            self.check_creds()
        return self._token

    @property
    def collection_token(self):
        """Get the token representing this particular Overdrive collection.

        As a side effect, this will verify that the Overdrive
        credentials are working.
        """
        if not self._collection_token:
            self.check_creds()
            library = self.get_library()
            error = library.get('errorCode')
            if error:
                message = library.get('message')
                raise CannotLoadConfiguration(
                    "Overdrive credentials are valid but could not fetch library: %s"
                    % message
                )
            self._collection_token = library['collectionToken']
        return self._collection_token

    @property
    def collection(self):
        return Collection.by_id(self._db, id=self.collection_id)

    @property
    def source(self):
        return DataSource.lookup(self._db, DataSource.OVERDRIVE)

    def ils_name(self, library):
        """Determine the ILS name to use for the given Library.
        """
        return self.ils_name_setting(
            self._db, self.collection, library
        ).value_or_default(self.ILS_NAME_DEFAULT)

    @classmethod
    def ils_name_setting(cls, _db, collection, library):
        """Find the ConfigurationSetting controlling the ILS name
        for the given collection and library.
        """
        return ConfigurationSetting.for_library_and_externalintegration(
            _db, cls.ILS_NAME_KEY, library, collection.external_integration
        )

    def check_creds(self, force_refresh=False):
        """If the Bearer Token has expired, update it."""
        with self.lock:
            refresh_on_lookup = self.refresh_creds
            if force_refresh:
                refresh_on_lookup = lambda x: x

            credential = self.credential_object(refresh_on_lookup)
            if force_refresh:
                self.refresh_creds(credential)
            self._token = credential.credential

    def credential_object(self, refresh):
        """Look up the Credential object that allows us to use
        the Overdrive API.
        """
        return Credential.lookup(
            self._db, DataSource.OVERDRIVE, None, None, refresh
        )

    def refresh_creds(self, credential):
        """Fetch a new Bearer Token and update the given Credential object."""
        response = self.token_post(
            self.TOKEN_ENDPOINT,
            dict(grant_type="client_credentials"),
            allowed_response_codes=[200]
        )
        data = response.json()
        self._update_credential(credential, data)
        self._token = credential.credential

    def get(self, url, extra_headers, exception_on_401=False):
        """Make an HTTP GET request using the active Bearer Token."""
        headers = dict(Authorization="Bearer %s" % self.token)
        headers.update(extra_headers)
        status_code, headers, content = self._do_get(url, headers)
        if status_code == 401:
            if exception_on_401:
                # This is our second try. Give up.
                raise BadResponseException.from_response(
                    url,
                    "Something's wrong with the Overdrive OAuth Bearer Token!",
                    (status_code, headers, content)
                )
            else:
                # Refresh the token and try again.
                self.check_creds(True)
                return self.get(url, extra_headers, True)
        else:
            return status_code, headers, content

    def token_post(self, url, payload, headers={}, **kwargs):
        """Make an HTTP POST request for purposes of getting an OAuth token."""
        s = "%s:%s" % (self.client_key, self.client_secret)
        auth = base64.standard_b64encode(s).strip()
        headers = dict(headers)
        headers['Authorization'] = "Basic %s" % auth
        return self._do_post(url, payload, headers, **kwargs)

    def _update_credential(self, credential, overdrive_data):
        """Copy Overdrive OAuth data into a Credential object."""
        credential.credential = overdrive_data['access_token']
        expires_in = (overdrive_data['expires_in'] * 0.9)
        credential.expires = datetime.datetime.utcnow() + datetime.timedelta(
            seconds=expires_in)

    @property
    def _library_endpoint(self):
        """Which URL should we go to to get information about this collection?

        If this is an ordinary Overdrive account, we get information
        from LIBRARY_ENDPOINT.

        If this is an Overdrive Advantage account, we get information
        from LIBRARY_ADVANTAGE_ENDPOINT.
        """
        args = dict(library_id=self.library_id)
        if self.parent_library_id:
            # This is an Overdrive advantage account.
            args['parent_library_id'] = self.parent_library_id
            endpoint = self.ADVANTAGE_LIBRARY_ENDPOINT
        else:
            endpoint = self.LIBRARY_ENDPOINT
        return self.endpoint(endpoint, **args)

    def get_library(self):
        """Get basic information about the collection, including
        a link to the titles in the collection.
        """
        url = self._library_endpoint
        with self.lock:
            representation, cached = Representation.get(
                self._db, url, self.get,
                exception_handler=Representation.reraise_exception,
            )
            return json.loads(representation.content)

    def get_advantage_accounts(self):
        """Find all the Overdrive Advantage accounts managed by this library.

        :yield: A sequence of OverdriveAdvantageAccount objects.
        """
        library = self.get_library()
        links = library.get('links', {})
        advantage = links.get('advantageAccounts')
        if not advantage:
            return []
        if advantage:
            # This library has Overdrive Advantage accounts, or at
            # least a link where some may be found.
            advantage_url = advantage.get('href')
            if not advantage_url:
                return
            representation, cached = Representation.get(
                self._db, advantage_url, self.get,
                exception_handler=Representation.reraise_exception,
            )
            return OverdriveAdvantageAccount.from_representation(
                representation.content
            )

    def all_ids(self):
        """Get IDs for every book in the system, with the most recently added
        ones at the front.
        """
        next_link = self._all_products_link
        while next_link:
            page_inventory, next_link = self._get_book_list_page(
                next_link, 'next'
            )

            for i in page_inventory:
                yield i

    @property
    def _all_products_link(self):
        url = self.endpoint(
            self.ALL_PRODUCTS_ENDPOINT,
            collection_token=self.collection_token,
            sort="dateAdded:desc"
        )
        return self.make_link_safe(url)

    def _get_book_list_page(self, link, rel_to_follow='next'):
        """Process a page of inventory whose circulation we need to check.

        Returns a 2-tuple: (availability_info, next_link).
        `availability_info` is a list of dictionaries, each containing
           basic availability and bibliographic information about
           one book.
        `next_link` is a link to the next page of results.
        """
        # We don't cache this because it changes constantly.
        status_code, headers, content = self.get(link, {})
        if isinstance(content, basestring):
            content = json.loads(content)

        # Find the link to the next page of results, if any.
        next_link = OverdriveRepresentationExtractor.link(
            content, rel_to_follow
        )

        # Prepare to get availability information for all the books on
        # this page.
        availability_queue = (
            OverdriveRepresentationExtractor.availability_link_list(content)
        )
        return availability_queue, next_link


    def recently_changed_ids(self, start, cutoff):
        """Get IDs of books whose status has changed between the start time
        and now.
        """
        # `cutoff` is not supported by Overdrive, so we ignore it. All
        # we can do is get events between the start time and now.

        last_update_time = start-self.EVENT_DELAY
        self.log.info(
            "Asking for circulation changes since %s",
            last_update_time
        )
        last_update = last_update_time.strftime(self.TIME_FORMAT)

        next_link = self.endpoint(
            self.EVENTS_ENDPOINT,
            lastupdatetime=last_update,
            sort="popularity:desc",
            limit=self.PAGE_SIZE_LIMIT,
            collection_token=self.collection_token
        )
        next_link = self.make_link_safe(next_link)
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
        url = self.endpoint(
            self.METADATA_ENDPOINT,
            collection_token=self.collection_token,
            item_id=identifier.identifier
        )
        status_code, headers, content = self.get(url, {})
        if isinstance(content, basestring):
            content = json.loads(content)
        return content

    def metadata_lookup_obj(self, identifier):
        url = self.endpoint(
            self.METADATA_ENDPOINT,
            collection_token=self.collection_token,
            item_id=identifier
        )
        status_code, headers, content = self.get(url, {})
        if isinstance(content, (bytes, unicode)):
            content = json.loads(content)
        return OverdriveRepresentationExtractor.book_info_to_metadata(content)


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

    def _do_get(self, url, headers):
        """This method is overridden in MockOverdriveAPI."""
        url = self.endpoint(url)
        return Representation.simple_http_get(
            url, headers
        )

    def _do_post(self, url, payload, headers, **kwargs):
        """This method is overridden in MockOverdriveAPI."""
        url = self.endpoint(url)
        return HTTP.post_with_timeout(url, payload, headers=headers, **kwargs)


class MockOverdriveAPI(OverdriveAPI):

    @classmethod
    def mock_collection(self, _db, library=None,
                        name="Test Overdrive Collection",
                        client_key=u"a", client_secret=u"b",
                        library_id=u"c", website_id="d",
                        ils_name="e",
                        ):
        """Create a mock Overdrive collection for use in tests."""
        if library is None:
            library = DatabaseTest.make_default_library(_db)
        collection, ignore = get_one_or_create(
            _db, Collection,
                name=name,
                create_method_kwargs=dict(
                    external_account_id=library_id
                )
            )
        integration = collection.create_external_integration(
            protocol=ExternalIntegration.OVERDRIVE
        )
        integration.username = client_key
        integration.password = client_secret
        integration.set_setting('website_id', website_id)
        library.collections.append(collection)
        OverdriveAPI.ils_name_setting(_db, collection, library).value = ils_name
        return collection

    def __init__(self, _db, collection, *args, **kwargs):
        self.access_token_requests = []
        self.requests = []
        self.responses = []

        # Almost all tests will try to request the access token, so
        # set the response that will be returned if an attempt is
        # made.
        self.access_token_response = self.mock_access_token_response(
            "bearer token"
        )
        super(MockOverdriveAPI, self).__init__(_db, collection, *args, **kwargs)

    def queue_collection_token(self):
        # Many tests immediately try to access the
        # collection token. This is a helper method to make it easy to
        # queue up the response.
        self.queue_response(
            200, content=self.mock_collection_token("collection token")
        )

    def token_post(self, url, payload, headers={}, **kwargs):
        """Mock the request for an OAuth token.

        We mock the method by looking at the access_token_response
        property, rather than inserting a mock response in the queue,
        because only the first MockOverdriveAPI instantiation in a
        given test actually makes this call. By mocking the response
        to this method separately we remove the need to figure out
        whether to queue a response in a given test.
        """
        url = self.endpoint(url)
        self.access_token_requests.append((url, payload, headers, kwargs))
        response = self.access_token_response
        return HTTP._process_response(url, response, **kwargs)

    def mock_access_token_response(self, credential):
        token = dict(access_token=credential, expires_in=3600)
        return MockRequestsResponse(200, {}, json.dumps(token))

    def mock_collection_token(self, token):
        return json.dumps(dict(collectionToken=token))

    def queue_response(self, status_code, headers={}, content=None):
        self.responses.insert(
            0, MockRequestsResponse(status_code, headers, content)
        )

    def _do_get(self, url, *args, **kwargs):
        """Simulate Representation.simple_http_get."""
        response = self._make_request(url, *args, **kwargs)
        return response.status_code, response.headers, response.content

    def _do_post(self, url, *args, **kwargs):
        return self._make_request(url, *args, **kwargs)

    def _make_request(self, url, *args, **kwargs):
        url = self.endpoint(url)
        response = self.responses.pop()
        self.requests.append((url, args, kwargs))
        return HTTP._process_response(
            url, response, kwargs.get('allowed_response_codes'),
            kwargs.get('disallowed_response_codes')
        )


class OverdriveRepresentationExtractor(object):

    """Extract useful information from Overdrive's JSON representations."""

    log = logging.getLogger("Overdrive representation extractor")

    @classmethod
    def availability_link_list(cls, book_list):
        """:return: A list of dictionaries with keys `id`, `title`, `availability_link`.
        """
        l = []
        if not 'products' in book_list:
            return []

        products = book_list['products']
        for product in products:
            if not 'id' in product:
                cls.log.warn("No ID found in %r", product)
                continue
            book_id = product['id']
            data = dict(
                id=book_id,
                title=product.get('title'),
                author_name=None,
                date_added=product.get('dateAdded')
            )
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

    format_data_for_overdrive_format = {

        "ebook-pdf-adobe" : (
            Representation.PDF_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM
        ),
        "ebook-pdf-open" : (
            Representation.PDF_MEDIA_TYPE, DeliveryMechanism.NO_DRM
        ),
        "ebook-epub-adobe" : (
            Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM
        ),
        "ebook-epub-open" : (
            Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.NO_DRM
        ),
        "audiobook-mp3" : (
            "application/x-od-media", DeliveryMechanism.OVERDRIVE_DRM
        ),
        "music-mp3" : (
            "application/x-od-media", DeliveryMechanism.OVERDRIVE_DRM
        ),
        "ebook-overdrive" : [
            (
                MediaTypes.OVERDRIVE_EBOOK_MANIFEST_MEDIA_TYPE,
                DeliveryMechanism.LIBBY_DRM
            ),
            (
                DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
                DeliveryMechanism.STREAMING_DRM
            ),
        ],
        "audiobook-overdrive" : [
            (
                MediaTypes.OVERDRIVE_AUDIOBOOK_MANIFEST_MEDIA_TYPE,
                DeliveryMechanism.LIBBY_DRM,
            ),
            (
                DeliveryMechanism.STREAMING_AUDIO_CONTENT_TYPE,
                DeliveryMechanism.STREAMING_DRM
            ),
        ],
        'video-streaming' : (
            DeliveryMechanism.STREAMING_VIDEO_CONTENT_TYPE,
            DeliveryMechanism.STREAMING_DRM
        ),
        "ebook-kindle" : (
            DeliveryMechanism.KINDLE_CONTENT_TYPE,
            DeliveryMechanism.KINDLE_DRM
        ),
        "periodicals-nook" : (
            DeliveryMechanism.NOOK_CONTENT_TYPE,
            DeliveryMechanism.NOOK_DRM
        ),
    }

    @classmethod
    def internal_formats(cls, overdrive_format):
        """Yield all internal formats for the given Overdrive format.

        Some Overdrive formats become multiple internal formats.

        :yield: A sequence of (content type, DRM system) 2-tuples
        """
        result = cls.format_data_for_overdrive_format.get(overdrive_format)
        if not result:
            return
        if isinstance(result, list):
            for i in result:
                yield i
        else:
            yield result

    ignorable_overdrive_formats = set([])

    overdrive_role_to_simplified_role = {
        "actor" : Contributor.ACTOR_ROLE,
        "artist" : Contributor.ARTIST_ROLE,
        "book producer" : Contributor.PRODUCER_ROLE,
        "associated name" : Contributor.ASSOCIATED_ROLE,
        "author" : Contributor.AUTHOR_ROLE,
        "author of introduction" : Contributor.INTRODUCTION_ROLE,
        "author of foreword" : Contributor.FOREWORD_ROLE,
        "author of afterword" : Contributor.AFTERWORD_ROLE,
        "contributor" : Contributor.CONTRIBUTOR_ROLE,
        "colophon" : Contributor.COLOPHON_ROLE,
        "adapter" : Contributor.ADAPTER_ROLE,
        "etc." : Contributor.UNKNOWN_ROLE,
        "cast member" : Contributor.ACTOR_ROLE,
        "collaborator" : Contributor.COLLABORATOR_ROLE,
        "compiler" : Contributor.COMPILER_ROLE,
        "composer" : Contributor.COMPOSER_ROLE,
        "copyright holder" : Contributor.COPYRIGHT_HOLDER_ROLE,
        "director" : Contributor.DIRECTOR_ROLE,
        "editor" : Contributor.EDITOR_ROLE,
        "engineer" : Contributor.ENGINEER_ROLE,
        "executive producer" : Contributor.EXECUTIVE_PRODUCER_ROLE,
        "illustrator" : Contributor.ILLUSTRATOR_ROLE,
        "musician" : Contributor.MUSICIAN_ROLE,
        "narrator" : Contributor.NARRATOR_ROLE,
        "other" : Contributor.UNKNOWN_ROLE,
        "performer" : Contributor.PERFORMER_ROLE,
        "producer" : Contributor.PRODUCER_ROLE,
        "translator" : Contributor.TRANSLATOR_ROLE,
        "photographer" : Contributor.PHOTOGRAPHER_ROLE,
        "lyricist" : Contributor.LYRICIST_ROLE,
        "transcriber" : Contributor.TRANSCRIBER_ROLE,
        "designer" : Contributor.DESIGNER_ROLE,
    }

    overdrive_medium_to_simplified_medium = {
        "eBook" : Edition.BOOK_MEDIUM,
        "Video" : Edition.VIDEO_MEDIUM,
        "Audiobook" : Edition.AUDIO_MEDIUM,
        "Music" : Edition.MUSIC_MEDIUM,
        "Periodicals" : Edition.PERIODICAL_MEDIUM,
    }

    DATE_FORMAT = "%Y-%m-%d"

    @classmethod
    def parse_roles(cls, id, rolestring):
        rolestring = rolestring.lower()
        roles = [x.strip() for x in rolestring.split(",")]
        if ' and '  in roles[-1]:
            roles = roles[:-1] + [x.strip() for x in roles[-1].split(" and ")]
        processed = []
        for x in roles:
            if x not in cls.overdrive_role_to_simplified_role:
                cls.log.error(
                    "Could not process role %s for %s", x, id)
            else:
                processed.append(cls.overdrive_role_to_simplified_role[x])
        return processed


    @classmethod
    def book_info_to_circulation(cls, book):
        """ Note:  The json data passed into this method is from a different file/stream
        from the json data that goes into the book_info_to_metadata() method.
        """
        # In Overdrive, 'reserved' books show up as books on
        # hold. There is no separate notion of reserved books.
        licenses_reserved = 0

        licenses_owned = None
        licenses_available = None
        patrons_in_hold_queue = None

        if not 'id' in book:
            return None
        overdrive_id = book['id']
        primary_identifier = IdentifierData(
            Identifier.OVERDRIVE_ID, overdrive_id
        )
        if (book.get('isOwnedByCollections') is not False):
            # We own this book.
            for collection in book['collections']:
                if 'copiesOwned' in collection:
                    if licenses_owned is None:
                        licenses_owned = 0
                    licenses_owned += int(collection['copiesOwned'])
                if 'copiesAvailable' in collection:
                    if licenses_available is None:
                        licenses_available = 0
                    licenses_available += int(collection['copiesAvailable'])
                if 'numberOfHolds' in collection:
                    if patrons_in_hold_queue is None:
                        patrons_in_hold_queue = 0
                    patrons_in_hold_queue += collection['numberOfHolds']
        return CirculationData(
            data_source=DataSource.OVERDRIVE,
            primary_identifier=primary_identifier,
            licenses_owned=licenses_owned,
            licenses_available=licenses_available,
            licenses_reserved=licenses_reserved,
            patrons_in_hold_queue=patrons_in_hold_queue,
        )

    @classmethod
    def image_link_to_linkdata(cls, link, rel):
        if not link or not 'href' in link:
            return None
        href = link['href']
        if '00000000-0000-0000-0000' in href:
            # This is a stand-in cover for preorders. It's better not
            # to have a cover at all -- we might be able to get one
            # later, or from another source.
            return None
        href = OverdriveAPI.make_link_safe(href)
        media_type = link.get('type', None)
        return LinkData(rel=rel, href=href, media_type=media_type)


    @classmethod
    def book_info_to_metadata(cls, book, include_bibliographic=True, include_formats=True):
        """Turn Overdrive's JSON representation of a book into a Metadata
        object.

        Note:  The json data passed into this method is from a different file/stream
        from the json data that goes into the book_info_to_circulation() method.
        """
        if not 'id' in book:
            return None
        overdrive_id = book['id']
        primary_identifier = IdentifierData(
            Identifier.OVERDRIVE_ID, overdrive_id
        )

        # If we trust classification data, we'll give it this weight.
        # Otherwise we'll probably give it a fraction of this weight.
        trusted_weight = Classification.TRUSTED_DISTRIBUTOR_WEIGHT

        if include_bibliographic:
            title = book.get('title', None)
            sort_title = book.get('sortTitle')
            subtitle = book.get('subtitle', None)
            series = book.get('series', None)
            publisher = book.get('publisher', None)
            imprint = book.get('imprint', None)

            if 'publishDate' in book:
                published = datetime.datetime.strptime(
                    book['publishDate'][:10], cls.DATE_FORMAT)
            else:
                published = None

            languages = [l['code'] for l in book.get('languages', [])]
            if 'eng' in languages or not languages:
                language = 'eng'
            else:
                language = sorted(languages)[0]

            contributors = []
            for creator in book.get('creators', []):
                sort_name = creator['fileAs']
                display_name = creator['name']
                role = creator['role']
                roles = cls.parse_roles(overdrive_id, role) or [Contributor.UNKNOWN_ROLE]
                contributor = ContributorData(
                    sort_name=sort_name, display_name=display_name,
                    roles=roles, biography = creator.get('bioText', None)
                )
                contributors.append(contributor)

            subjects = []
            for sub in book.get('subjects', []):
                subject = SubjectData(
                    type=Subject.OVERDRIVE, identifier=sub['value'],
                    weight=trusted_weight,
                )
                subjects.append(subject)

            for sub in book.get('keywords', []):
                subject = SubjectData(
                    type=Subject.TAG, identifier=sub['value'],
                    # We don't use TRUSTED_DISTRIBUTOR_WEIGHT because
                    # we don't know where the tags come from --
                    # probably Overdrive users -- and they're
                    # frequently wrong.
                    weight=1
                )
                subjects.append(subject)

            extra = dict()
            if 'grade_levels' in book:
                # n.b. Grade levels are measurements of reading level, not
                # age appropriateness. We can use them as a measure of age
                # appropriateness in a pinch, but we weight them less
                # heavily than TRUSTED_DISTRIBUTOR_WEIGHT.
                for i in book['grade_levels']:
                    subject = SubjectData(
                        type=Subject.GRADE_LEVEL,
                        identifier=i['value'],
                        weight=trusted_weight / 10
                    )
                    subjects.append(subject)

            overdrive_medium = book.get('mediaType', None)
            if overdrive_medium and overdrive_medium not in cls.overdrive_medium_to_simplified_medium:
                cls.log.error(
                    "Could not process medium %s for %s", overdrive_medium, overdrive_id)

            medium = cls.overdrive_medium_to_simplified_medium.get(
                overdrive_medium, Edition.BOOK_MEDIUM
            )

            measurements = []
            if 'awards' in book:
                extra['awards'] = book.get('awards', [])
                num_awards = len(extra['awards'])
                measurements.append(
                    MeasurementData(
                        Measurement.AWARDS, str(num_awards)
                    )
                )

            for name, subject_type in (
                ('ATOS', Subject.ATOS_SCORE),
                ('lexileScore', Subject.LEXILE_SCORE),
                ('interestLevel', Subject.INTEREST_LEVEL)
            ):
                if not name in book:
                    continue
                identifier = str(book[name])
                subjects.append(
                    SubjectData(type=subject_type, identifier=identifier,
                                weight=trusted_weight
                            )
                )

            for grade_level_info in book.get('gradeLevels', []):
                grade_level = grade_level_info.get('value')
                subjects.append(
                    SubjectData(type=Subject.GRADE_LEVEL, identifier=grade_level,
                                weight=trusted_weight)
                )

            identifiers = []
            links = []
            for format in book.get('formats', []):
                for new_id in format.get('identifiers', []):
                    t = new_id['type']
                    v = new_id['value']
                    orig_v = v
                    type_key = None
                    if t == 'ASIN':
                        type_key = Identifier.ASIN
                    elif t == 'ISBN':
                        type_key = Identifier.ISBN
                        if len(v) == 10:
                            v = isbnlib.to_isbn13(v)
                        if v is None or not isbnlib.is_isbn13(v):
                            # Overdrive sometimes uses invalid values
                            # like "n/a" as placeholders. Ignore such
                            # values to avoid a situation where hundreds of
                            # books appear to have the same ISBN. ISBNs
                            # which fail check digit checks or are invalid
                            # also can occur. Log them for review.
                            cls.log.info(
                                "Bad ISBN value provided: %s", orig_v
                            )
                            continue
                    elif t == 'DOI':
                        type_key = Identifier.DOI
                    elif t == 'UPC':
                        type_key = Identifier.UPC
                    elif t == 'PublisherCatalogNumber':
                        continue
                    if type_key and v:
                        identifiers.append(
                            IdentifierData(type_key, v, 1)
                        )

                # Samples become links.
                if 'samples' in format:
                    overdrive_name = format['id']
                    internal_names = list(cls.internal_formats(overdrive_name))
                    if not internal_names:
                        # Useless to us.
                        continue
                    for content_type, drm_scheme in internal_names:
                        if Representation.is_media_type(content_type):
                            for sample_info in format['samples']:
                                href = sample_info['url']
                                links.append(
                                    LinkData(
                                        rel=Hyperlink.SAMPLE,
                                        href=href,
                                        media_type=content_type
                                    )
                                )

            # A cover and its thumbnail become a single LinkData.
            if 'images' in book:
                images = book['images']
                image_data = cls.image_link_to_linkdata(
                    images.get('cover'), Hyperlink.IMAGE
                )
                for name in ['cover300Wide', 'cover150Wide', 'thumbnail']:
                    # Try to get a thumbnail that's as close as possible
                    # to the size we use.
                    image = images.get(name)
                    thumbnail_data = cls.image_link_to_linkdata(
                        image, Hyperlink.THUMBNAIL_IMAGE
                    )
                    if not image_data:
                        image_data = cls.image_link_to_linkdata(
                            image, Hyperlink.IMAGE
                        )
                    if thumbnail_data:
                        break

                if image_data:
                    if thumbnail_data:
                        image_data.thumbnail = thumbnail_data
                    links.append(image_data)

            # Descriptions become links.
            short = book.get('shortDescription')
            full = book.get('fullDescription')
            if full:
                links.append(
                    LinkData(
                        rel=Hyperlink.DESCRIPTION,
                        content=full,
                        media_type="text/html",
                    )
                )

            if short and (not full or not full.startswith(short)):
                links.append(
                    LinkData(
                        rel=Hyperlink.SHORT_DESCRIPTION,
                        content=short,
                        media_type="text/html",
                    )
                )

            # Add measurements: rating and popularity
            if book.get('starRating') is not None and book['starRating'] > 0:
                measurements.append(
                    MeasurementData(
                        quantity_measured=Measurement.RATING,
                        value=book['starRating']
                    )
                )

            if book.get('popularity'):
                measurements.append(
                    MeasurementData(
                        quantity_measured=Measurement.POPULARITY,
                        value=book['popularity']
                    )
                )

            metadata = Metadata(
                data_source=DataSource.OVERDRIVE,
                title=title,
                subtitle=subtitle,
                sort_title=sort_title,
                language=language,
                medium=medium,
                series=series,
                publisher=publisher,
                imprint=imprint,
                published=published,
                primary_identifier=primary_identifier,
                identifiers=identifiers,
                subjects=subjects,
                contributors=contributors,
                measurements=measurements,
                links=links,
            )
        else:
            metadata = Metadata(
                data_source=DataSource.OVERDRIVE,
                primary_identifier=primary_identifier,
            )

        if include_formats:
            formats = []
            for format in book.get('formats', []):
                format_id = format['id']
                internal_formats = list(cls.internal_formats(format_id))
                if internal_formats:
                    for content_type, drm_scheme in internal_formats:
                        formats.append(FormatData(content_type, drm_scheme))
                elif format_id not in cls.ignorable_overdrive_formats:
                    cls.log.error(
                        "Could not process Overdrive format %s for %s",
                        format_id, overdrive_id
                    )

            # Also make a CirculationData so we can write the formats,
            circulationdata = CirculationData(
                data_source=DataSource.OVERDRIVE,
                primary_identifier=primary_identifier,
                formats=formats,
            )

            metadata.circulation = circulationdata

        return metadata


class OverdriveAdvantageAccount(object):
    """Holder and parser for data associated with Overdrive Advantage.
    """

    def __init__(self, parent_library_id, library_id, name):
        """Constructor.

        :param parent_library_id: The library ID of the parent Overdrive
            account.
        :param library_id: The library ID of the Overdrive Advantage account.
        :param name: The name of the library whose Advantage account this is.
        """
        self.parent_library_id = parent_library_id
        self.library_id = library_id
        self.name = name

    @classmethod
    def from_representation(cls, content):
        """Turn the representation of an advantageAccounts link into a list of
        OverdriveAdvantageAccount objects.

        :param content: The data obtained by following an advantageAccounts
            link.
        :yield: A sequence of OverdriveAdvantageAccount objects.
        """
        data = json.loads(content)
        parent_id = str(data.get('id'))
        accounts = data.get('advantageAccounts', {})
        for account in accounts:
            name = account['name']
            products_link = account['links']['products']['href']
            library_id = str(account.get('id'))
            name = account.get('name')
            yield cls(parent_library_id=parent_id, library_id=library_id,
                      name=name)

    def to_collection(self, _db):
        """Find or create a Collection object for this Overdrive Advantage
        account.

        :return: a 2-tuple of Collections (primary Overdrive
            collection, Overdrive Advantage collection)
        """
        # First find the parent Collection.
        try:
            parent = Collection.by_protocol(_db, ExternalIntegration.OVERDRIVE).filter(
                Collection.external_account_id==self.parent_library_id
            ).one()
        except NoResultFound, e:
            # Without the parent's credentials we can't access the child.
            raise ValueError(
                "Cannot create a Collection whose parent does not already exist."
            )
        name = parent.name + " / " + self.name
        child, is_new = get_one_or_create(
            _db, Collection, parent_id=parent.id,
            external_account_id=self.library_id,
            create_method_kwargs=dict(name=name)
        )
        if is_new:
            # Make sure the child has its protocol set appropriately.
            integration = child.create_external_integration(
                ExternalIntegration.OVERDRIVE
            )

        # Set or update the name of the collection to reflect the name of
        # the library, just in case that name has changed.
        child.name = name
        return parent, child


class OverdriveBibliographicCoverageProvider(BibliographicCoverageProvider):
    """Fill in bibliographic metadata for Overdrive records.

    This will occasionally fill in some availability information for a
    single Collection, but we rely on Monitors to keep availability
    information up to date for all Collections.
    """

    SERVICE_NAME = "Overdrive Bibliographic Coverage Provider"
    DATA_SOURCE_NAME = DataSource.OVERDRIVE
    PROTOCOL = ExternalIntegration.OVERDRIVE
    INPUT_IDENTIFIER_TYPES = Identifier.OVERDRIVE_ID

    def __init__(self, collection, api_class=OverdriveAPI, **kwargs):
        """Constructor.

        :param collection: Provide bibliographic coverage to all
            Overdrive books in the given Collection.
        :param api_class: Instantiate this class with the given Collection,
            rather than instantiating OverdriveAPI.
        """
        super(OverdriveBibliographicCoverageProvider, self).__init__(
            collection, **kwargs
        )
        if isinstance(api_class, OverdriveAPI):
            # Use a previously instantiated OverdriveAPI instance
            # rather than creating a new one.
            self.api = api_class
        else:
            # A web application should not use this option because it
            # will put a non-scoped session in the mix.
            _db = Session.object_session(collection)
            self.api = api_class(_db, collection)

    def process_item(self, identifier):
        info = self.api.metadata_lookup(identifier)
        error = None
        if info.get('errorCode') == 'NotFound':
            error = "ID not recognized by Overdrive: %s" % identifier.identifier
        elif info.get('errorCode') == 'InvalidGuid':
            error = "Invalid Overdrive ID: %s" % identifier.identifier

        if error:
            return self.failure(identifier, error, transient=False)

        metadata = OverdriveRepresentationExtractor.book_info_to_metadata(
            info
        )

        if not metadata:
            e = "Could not extract metadata from Overdrive data: %r" % info
            return self.failure(identifier, e)

        self.metadata_pre_hook(metadata)
        return self.set_metadata(identifier, metadata)

    def metadata_pre_hook(self, metadata):
        """A hook method that allows subclasses to modify a Metadata
        object derived from Overdrive before it's applied.
        """
        return metadata
