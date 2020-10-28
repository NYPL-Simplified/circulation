from nose.tools import set_trace
from io import (
    StringIO,
    BytesIO,
)
from collections import (
    defaultdict,
    Counter,
)
import datetime
import dateutil
import feedparser
import logging
import traceback
import urllib
from urlparse import urlparse, urljoin
from sqlalchemy.orm import aliased
from sqlalchemy.orm.session import Session
from flask_babel import lazy_gettext as _

from lxml import etree

from monitor import CollectionMonitor
from util import LanguageCodes
from util.xmlparser import XMLParser
from config import (
    CannotLoadConfiguration,
    Configuration,
    IntegrationException,
)
from metadata_layer import (
    CirculationData,
    Metadata,
    IdentifierData,
    ContributorData,
    LinkData,
    MeasurementData,
    SubjectData,
    ReplacementPolicy,
    TimestampData,
)

from model import (
    Collection,
    CoverageRecord,
    DataSource,
    Edition,
    Equivalency,
    ExternalIntegration,
    Hyperlink,
    Identifier,
    LicensePool,
    Measurement,
    Representation,
    RightsStatus,
    Subject,
    get_one,
)
from model.configuration import ExternalIntegrationLink
from model.constants import MediaTypes
from coverage import CoverageFailure
from util.http import (
    BadResponseException,
    HTTP,
)
from util.opds_writer import (
    OPDSFeed,
    OPDSMessage,
)
from util.string_helpers import base64
from mirror import MirrorUploader
from selftest import (
    HasSelfTests,
    SelfTestResult,
)


class AccessNotAuthenticated(Exception):
    """No authentication is configured for this service"""
    pass


class SimplifiedOPDSLookup(object):
    """Tiny integration class for the Simplified 'lookup' protocol."""

    LOOKUP_ENDPOINT = "lookup"

    @classmethod
    def check_content_type(cls, response):
        content_type = response.headers.get('content-type')
        if content_type != OPDSFeed.ACQUISITION_FEED_TYPE:
            raise BadResponseException.from_response(
                response.url,
                "Wrong media type: %s" % content_type,
                response
            )

    @classmethod
    def from_protocol(cls, _db, protocol,
                      goal=ExternalIntegration.LICENSE_GOAL, library=None
    ):
        integration = ExternalIntegration.lookup(
            _db, protocol, goal, library=library
        )
        if not integration or not integration.url:
            return None
        return cls(integration.url)

    def __init__(self, base_url):
        if not base_url.endswith('/'):
            base_url += "/"
        self.base_url = base_url

    @property
    def lookup_endpoint(self):
        return self.LOOKUP_ENDPOINT

    def _get(self, url, **kwargs):
        """Make an HTTP request. This method is overridden in the mock class."""
        kwargs['timeout'] = kwargs.get('timeout', 300)
        kwargs['allowed_response_codes'] = kwargs.get('allowed_response_codes', [])
        kwargs['allowed_response_codes'] += ['2xx', '3xx']
        return HTTP.get_with_timeout(url, **kwargs)

    def urn_args(self, identifiers):
        return "&".join(set("urn=%s" % i.urn for i in identifiers))

    def lookup(self, identifiers):
        """Retrieve an OPDS feed with metadata for the given identifiers."""
        args = self.urn_args(identifiers)
        url = self.base_url + self.lookup_endpoint + "?" + args
        logging.info("Lookup URL: %s", url)
        return self._get(url)


class MetadataWranglerOPDSLookup(SimplifiedOPDSLookup, HasSelfTests):

    PROTOCOL = ExternalIntegration.METADATA_WRANGLER
    NAME = _("Library Simplified Metadata Wrangler")
    CARDINALITY = 1

    SETTINGS = [
        { "key": ExternalIntegration.URL,
          "label": _("URL"),
          "default": "http://metadata.librarysimplified.org/",
          "required": True,
          "format": "url",
        },
    ]

    SITEWIDE = True

    ADD_ENDPOINT = 'add'
    ADD_WITH_METADATA_ENDPOINT = 'add_with_metadata'
    METADATA_NEEDED_ENDPOINT = 'metadata_needed'
    REMOVE_ENDPOINT = 'remove'
    UPDATES_ENDPOINT = 'updates'
    CANONICALIZE_ENDPOINT = 'canonical-author-name'

    @classmethod
    def from_config(cls, _db, collection=None):
        integration = ExternalIntegration.lookup(
            _db, ExternalIntegration.METADATA_WRANGLER,
            ExternalIntegration.METADATA_GOAL
        )

        if not integration:
            raise CannotLoadConfiguration(
                "No ExternalIntegration found for the Metadata Wrangler.")

        if not integration.url:
            raise CannotLoadConfiguration("Metadata Wrangler improperly configured.")

        return cls(
            integration.url, shared_secret=integration.password,
            collection=collection
        )

    @classmethod
    def external_integration(cls, _db):
        return ExternalIntegration.lookup(
            _db, ExternalIntegration.METADATA_WRANGLER,
            ExternalIntegration.METADATA_GOAL
        )

    def _run_self_tests(self, _db, lookup_class=None):
        """Run self-tests on every eligible Collection.

        :param _db: A database connection.
        :param lookup_class: Pass in a mock class to instantiate that
           class as needed instead of MetadataWranglerOPDSLookup.
        :return: A dictionary mapping Collection objects to lists of
           SelfTestResult objects.
        """
        lookup_class = lookup_class or MetadataWranglerOPDSLookup
        results = dict()

        # Find all eligible Collections on the system, instantiate a
        # _new_ MetadataWranglerOPDSLookup for each, and call
        # its _run_collection_self_tests method.
        for c in _db.query(Collection):
            try:
                metadata_identifier = c.metadata_identifier
            except ValueError, e:
                continue

            lookup = lookup_class.from_config(_db, c)
            for i in lookup._run_collection_self_tests():
                yield i

    def _run_collection_self_tests(self):
        """Run the self-test suite on the Collection associated with this
        MetadataWranglerOPDSLookup.
        """
        if not self.collection:
            return
        metadata_identifier = None
        try:
            metadata_identifier = self.collection.metadata_identifier
        except ValueError, e:
            # This collection has no metadata identifier. It's
            # probably a "Manual intervention" collection. It cannot
            # interact with the metadata wrangler and there's no need
            # to test it.
            return

        # Check various endpoints that yield OPDS feeds.
        one_day_ago = datetime.datetime.utcnow() - datetime.timedelta(days=1)
        for title, m, args in (
            (
                "Metadata updates in last 24 hours",
                self.updates, [one_day_ago]
            ),
            (
                "Titles where we could (but haven't) provide information to the metadata wrangler",
                self.metadata_needed, []
            )
        ):
            yield self._feed_self_test(title, m, *args)

    def _feed_self_test(self, name, method, *args):
        """Retrieve a feed from the metadata wrangler and
        turn it into a SelfTestResult.
        """
        result = SelfTestResult(name)
        result.collection = self.collection

        # If the server returns a 500 error we don't want to raise an
        # exception -- we want to record it as part of the test
        # result.
        kwargs = dict(allowed_response_codes=['%sxx' % f for f in range(1,6)])

        response = method(*args, **kwargs)
        self._annotate_feed_response(result, response)

        # We're all done.
        result.end = datetime.datetime.utcnow()
        return result

    @classmethod
    def _annotate_feed_response(cls, result, response):
        """Parse an OPDS feed and annotate a SelfTestResult with some
        information about it:

        * How the feed was requested.
        * What the response code was.
        * The number of items on the first page.
        * The title of each item on the page, if any.
        * The total number of items in the feed, if available.

        :param result: A SelfTestResult object.
        :param response: A requests Response object.
        """
        lines = []
        lines.append("Request URL: %s" % response.url)
        lines.append(
            "Request authorization: %s" %
            response.request.headers.get('Authorization')
        )
        lines.append("Status code: %d" % response.status_code)
        result.success = response.status_code == 200
        if result.success:
            feed = feedparser.parse(response.content)
            total_results = feed['feed'].get('opensearch_totalresults')
            if total_results is not None:
                lines.append(
                    "Total identifiers registered with this collection: %s" % (
                        total_results
                    )
                )
            lines.append("Entries on this page: %d" % len(feed['entries']))
            for i in feed['entries']:
                lines.append(" " + i['title'])
        result.result = lines

    def __init__(self, url, shared_secret=None, collection=None):
        super(MetadataWranglerOPDSLookup, self).__init__(url)
        self.shared_secret = shared_secret
        self.collection = collection

    @property
    def authenticated(self):
        return bool(self.shared_secret)

    @property
    def authorization(self):
        if self.authenticated:
            token = 'Bearer ' + base64.b64encode(self.shared_secret)
            return { 'Authorization' : token }
        return None

    @property
    def lookup_endpoint(self):
        if not (self.authenticated and self.collection):
            return self.LOOKUP_ENDPOINT
        return self.collection.metadata_identifier + '/' + self.LOOKUP_ENDPOINT

    def _get(self, url, **kwargs):
        if self.authenticated:
            headers = kwargs.get('headers', {})
            headers.update(self.authorization)
            kwargs['headers'] = headers
        return super(MetadataWranglerOPDSLookup, self)._get(url, **kwargs)

    def _post(self, url, data="", **kwargs):
        """Make an HTTP request. This method is overridden in the mock class."""
        if self.authenticated:
            headers = kwargs.get('headers', {})
            headers.update(self.authorization)
            kwargs['headers'] = headers
        kwargs['timeout'] = kwargs.get('timeout', 120)
        kwargs['allowed_response_codes'] = kwargs.get('allowed_response_codes', [])
        kwargs['allowed_response_codes'] += ['2xx', '3xx']
        return HTTP.post_with_timeout(url, data, **kwargs)

    def add_args(self, url, arg_string):
        joiner = '?'
        if joiner in url:
            # This URL already has an argument (namely: data_source), so
            # append the new arguments.
            joiner = '&'
        return url + joiner + arg_string

    def get_collection_url(self, endpoint):
        if not self.authenticated:
            raise AccessNotAuthenticated("Metadata Wrangler access not authenticated.")
        if not self.collection:
            raise ValueError("No Collection provided.")

        data_source = ''
        if self.collection.protocol == ExternalIntegration.OPDS_IMPORT:
            # Open access OPDS_IMPORT collections need to send a DataSource to
            # allow OPDS lookups on the Metadata Wrangler.
            data_source = '?data_source=' + urllib.quote(self.collection.data_source.name)

        return (self.base_url
            + self.collection.metadata_identifier
            + '/' + endpoint + data_source)

    def add(self, identifiers):
        """Add items to an authenticated Metadata Wrangler Collection"""
        add_url = self.get_collection_url(self.ADD_ENDPOINT)
        url = self.add_args(add_url, self.urn_args(identifiers))

        logging.info("Metadata Wrangler Collection Addition URL: %s", url)
        return self._post(url)

    def add_with_metadata(self, feed):
        """Add a feed of items with metadata to an authenticated Metadata Wrangler Collection."""
        add_with_metadata_url = self.get_collection_url(self.ADD_WITH_METADATA_ENDPOINT)
        return self._post(add_with_metadata_url, unicode(feed))

    def metadata_needed(self, **kwargs):
        """Get a feed of items that need additional metadata to be processed
        by the Metadata Wrangler.
        """
        metadata_needed_url = self.get_collection_url(
            self.METADATA_NEEDED_ENDPOINT
        )
        return self._get(metadata_needed_url, **kwargs)

    def remove(self, identifiers):
        """Remove items from an authenticated Metadata Wrangler Collection"""
        remove_url = self.get_collection_url(self.REMOVE_ENDPOINT)
        url = self.add_args(remove_url, self.urn_args(identifiers))

        logging.info("Metadata Wrangler Collection Removal URL: %s", url)
        return self._post(url)

    def updates(self, last_update_time, **kwargs):
        """Retrieve updated items from an authenticated Metadata
        Wrangler Collection

        :param last_update_time: DateTime representing the last time
            an update was fetched. May be None.
        """
        url = self.get_collection_url(self.UPDATES_ENDPOINT)
        if last_update_time:
            formatted_time = last_update_time.strftime('%Y-%m-%dT%H:%M:%SZ')
            url = self.add_args(url, ('last_update_time='+formatted_time))
        logging.info("Metadata Wrangler Collection Updates URL: %s", url)
        return self._get(url, **kwargs)

    def canonicalize_author_name(self, identifier, working_display_name):
        """Attempt to find the canonical name for the author of a book.

        :param identifier: an ISBN-type Identifier.

        :param working_display_name: The display name of the author
            (i.e. the name format human being used as opposed to the name
            that goes into library records).
        """
        args = "display_name=%s" % (
            urllib.quote(working_display_name.encode("utf8"))
        )
        if identifier:
            args += "&urn=%s" % urllib.quote(identifier.urn)
        url = self.base_url + self.CANONICALIZE_ENDPOINT + "?" + args
        logging.info("GET %s", url)
        return self._get(url)


class MockSimplifiedOPDSLookup(SimplifiedOPDSLookup):

    def __init__(self, *args, **kwargs):
        self.requests = []
        self.responses = []
        super(MockSimplifiedOPDSLookup, self).__init__(*args, **kwargs)

    def queue_response(self, status_code, headers={}, content=None):
        from testing import MockRequestsResponse
        self.responses.insert(
            0, MockRequestsResponse(status_code, headers, content)
        )

    def _get(self, url, *args, **kwargs):
        self.requests.append((url, args, kwargs))
        response = self.responses.pop()
        return HTTP._process_response(
            url, response, kwargs.get('allowed_response_codes'),
            kwargs.get('disallowed_response_codes')
        )


class MockMetadataWranglerOPDSLookup(MockSimplifiedOPDSLookup, MetadataWranglerOPDSLookup):

    def _post(self, url, *args, **kwargs):
        self.requests.append((url, args, kwargs))
        response = self.responses.pop()
        return HTTP._process_response(
            url, response, kwargs.get('allowed_response_codes'),
            kwargs.get('disallowed_response_codes')
        )


class OPDSXMLParser(XMLParser):

    NAMESPACES = { "simplified": "http://librarysimplified.org/terms/",
                   "app" : "http://www.w3.org/2007/app",
                   "dcterms" : "http://purl.org/dc/terms/",
                   "dc" : "http://purl.org/dc/elements/1.1/",
                   "opds": "http://opds-spec.org/2010/catalog",
                   "schema" : "http://schema.org/",
                   "atom" : "http://www.w3.org/2005/Atom",
                   "drm": "http://librarysimplified.org/terms/drm",
    }


class OPDSImporter(object):
    """ Imports editions and license pools from an OPDS feed.
    Creates Edition, LicensePool and Work rows in the database, if those
    don't already exist.

    Should be used when a circulation server asks for data from
    our internal content server, and also when our content server asks for data
    from external content servers.
    """

    COULD_NOT_CREATE_LICENSE_POOL = (
        "No existing license pool for this identifier and no way of creating one.")

    NAME = ExternalIntegration.OPDS_IMPORT
    DESCRIPTION = _("Import books from a publicly-accessible OPDS feed.")

    # These settings are used by all OPDS-derived import methods.
    BASE_SETTINGS = [
        {
            "key": Collection.EXTERNAL_ACCOUNT_ID_KEY,
            "label": _("URL"),
            "required": True,
            "format": "url"
        },
        {
            "key": Collection.DATA_SOURCE_NAME_SETTING,
            "label": _("Data source name"),
            "required": True,
        },
    ]

    # These settings are used by 'regular' OPDS but not by OPDS For
    # Distributors, which has its own way of doing authentication.
    SETTINGS = BASE_SETTINGS + [
        {
            "key": ExternalIntegration.USERNAME,
            "label": _("Username"),
            "description": _("If HTTP Basic authentication is required to access the OPDS feed (it usually isn't), enter the username here."),
        },
        {
            "key": ExternalIntegration.PASSWORD,
            "label": _("Password"),
            "description": _("If HTTP Basic authentication is required to access the OPDS feed (it usually isn't), enter the password here."),
        },
    ]

    # Subclasses of OPDSImporter may define a different parser class that's
    # a subclass of OPDSXMLParser. For example, a subclass may want to use
    # tags from an additional namespace.
    PARSER_CLASS = OPDSXMLParser

    # Subclasses of OPDSImporter may define a list of status codes
    # that should be treated as indicating success, rather than failure,
    # when they show up in <simplified:message> tags.
    SUCCESS_STATUS_CODES = None

    def __init__(self, _db, collection, data_source_name=None,
                 identifier_mapping=None, http_get=None,
                 metadata_client=None, content_modifier=None,
                 map_from_collection=None, mirrors=None
    ):
        """:param collection: LicensePools created by this OPDS import
        will be associated with the given Collection. If this is None,
        no LicensePools will be created -- only Editions.

        :param data_source_name: Name of the source of this OPDS feed.
        All Editions created by this import will be associated with
        this DataSource. If there is no DataSource with this name, one
        will be created. NOTE: If `collection` is provided, its
        .data_source will take precedence over any value provided
        here. This is only for use when you are importing OPDS
        metadata without any particular Collection in mind.

        :param mirrors: A dictionary of different MirrorUploader objects for
        different purposes.

        :param http_get: Use this method to make an HTTP GET request. This
        can be replaced with a stub method for testing purposes.

        :param metadata_client: A SimplifiedOPDSLookup object that is used
        to fill in missing metadata.

        :param content_modifier: A function that may modify-in-place
        representations (such as images and EPUB documents) as they
        come in from the network.
        """
        self._db = _db
        self.log = logging.getLogger("OPDS Importer")
        self.collection = collection
        if self.collection and not data_source_name:
            # Use the Collection data_source for OPDS import.
            data_source = self.collection.data_source
            if data_source:
                data_source_name = data_source.name
            else:
                raise ValueError(
                    "Cannot perform an OPDS import on a Collection that has no associated DataSource!"
                )
        else:
            # Use the given data_source or default to the Metadata
            # Wrangler.
            data_source_name = data_source_name or DataSource.METADATA_WRANGLER
        self.data_source_name = data_source_name
        self.identifier_mapping = identifier_mapping
        try:
            self.metadata_client = metadata_client or MetadataWranglerOPDSLookup.from_config(_db, collection=collection)
        except CannotLoadConfiguration, e:
            # The Metadata Wrangler isn't configured, but we can import without it.
            self.log.warn("Metadata Wrangler integration couldn't be loaded, importing without it.")
            self.metadata_client = None

        # Check to see if a mirror for each purpose was passed in.
        # If not, then attempt to create one.
        covers_mirror = mirrors.get(ExternalIntegrationLink.COVERS, None) if mirrors else None
        books_mirror = mirrors.get(ExternalIntegrationLink.OPEN_ACCESS_BOOKS, None) if mirrors else None
        if collection:
            if not covers_mirror:
                # If this Collection is configured to mirror the assets it
                # discovers, this will create a MirrorUploader for that
                # Collection for its purpose. Otherwise, this will return None.
                covers_mirror = MirrorUploader.for_collection(
                    collection, ExternalIntegrationLink.COVERS
                )
            if not books_mirror:
                books_mirror = MirrorUploader.for_collection(
                    collection, ExternalIntegrationLink.OPEN_ACCESS_BOOKS
                )

        self.mirrors = dict(covers_mirror=covers_mirror, books_mirror=books_mirror)
        self.content_modifier = content_modifier

        # In general, we are cautious when mirroring resources so that
        # we don't, e.g. accidentally get our IP banned from
        # gutenberg.org.
        self.http_get = http_get or Representation.cautious_http_get
        self.map_from_collection = map_from_collection

    @property
    def data_source(self):
        """Look up or create a DataSource object representing the
        source of this OPDS feed.
        """
        offers_licenses = (self.collection is not None)
        return DataSource.lookup(
            self._db, self.data_source_name, autocreate=True,
            offers_licenses = offers_licenses
        )

    def assert_importable_content(self, feed, feed_url, max_get_attempts=5):
        """Raise an exception if the given feed contains nothing that can,
        even theoretically, be turned into a LicensePool.

        By default, this means the feed must link to open-access content
        that can actually be retrieved.
        """
        metadata, failures = self.extract_feed_data(feed, feed_url)
        get_attempts = 0

        # Find an open-access link, and try to GET it just to make
        # sure OPDS feed isn't hiding non-open-access stuff behind an
        # open-access link.
        #
        # To avoid taking forever or antagonizing API providers, we'll
        # give up after `max_get_attempts` failures.
        for link in self._open_access_links(metadata.values()):
            url = link.href
            success = self._is_open_access_link(url, link.media_type)
            if success:
                return success
            get_attempts += 1
            if get_attempts >= max_get_attempts:
                error = "Was unable to GET supposedly open-access content such as %s (tried %s times)" % (
                    url, get_attempts
                )
                explanation = "This might be an OPDS For Distributors feed, or it might require different authentication credentials."
                raise IntegrationException(error, explanation)

        raise IntegrationException(
            "No open-access links were found in the OPDS feed.",
            "This might be an OPDS for Distributors feed."
        )

    @classmethod
    def _open_access_links(cls, metadatas):
        """Find all open-access links in a list of Metadata objects.

        :param metadatas: A list of Metadata objects.
        :yield: A sequence of `LinkData` objects.
        """
        for item in metadatas:
            if not item.circulation:
                continue
            for link in item.circulation.links:
                if link.rel == Hyperlink.OPEN_ACCESS_DOWNLOAD:
                    yield link

    def _is_open_access_link(self, url, type):
        """Is `url` really an open-access link?

        That is, can we make a normal GET request and get something
        that looks like a book?
        """
        headers = {}
        if type:
            headers["Accept"] = type
        status, headers, body = self.http_get(url, headers=headers)
        if status == 200 and len(body) > 1024 * 10:
            # We could also check the media types, but this is good
            # enough for now.
            return "Found a book-like thing at %s" % url
        self.log.error(
            "Supposedly open-access link %s didn't give us a book. Status=%s, body length=%s",
            url, status, len(body)
        )
        return False

    def import_from_feed(self, feed, feed_url=None):

        # Keep track of editions that were imported. Pools and works
        # for those editions may be looked up or created.
        imported_editions = {}
        pools = {}
        works = {}
        # CoverageFailures that note business logic errors and non-success download statuses
        failures = {}

        # If parsing the overall feed throws an exception, we should address that before
        # moving on. Let the exception propagate.
        metadata_objs, failures = self.extract_feed_data(feed, feed_url)
        # make editions.  if have problem, make sure associated pool and work aren't created.
        for key, metadata in metadata_objs.iteritems():
            # key is identifier.urn here

            # If there's a status message about this item, don't try to import it.
            if key in failures.keys():
                continue

            try:
                # Create an edition. This will also create a pool if there's circulation data.
                edition = self.import_edition_from_metadata(metadata)
                if edition:
                    imported_editions[key] = edition
            except Exception, e:
                # Rather than scratch the whole import, treat this as a failure that only applies
                # to this item.
                self.log.error("Error importing an OPDS item", exc_info=e)
                identifier, ignore = Identifier.parse_urn(self._db, key)
                data_source = self.data_source
                failure = CoverageFailure(identifier, traceback.format_exc(), data_source=data_source, transient=False)
                failures[key] = failure
                # clean up any edition might have created
                if key in imported_editions:
                    del imported_editions[key]
                # Move on to the next item, don't create a work.
                continue

            try:
                pool, work = self.update_work_for_edition(edition)
                if pool:
                    pools[key] = pool
                if work:
                    works[key] = work
            except Exception, e:
                identifier, ignore = Identifier.parse_urn(self._db, key)
                data_source = self.data_source
                failure = CoverageFailure(identifier, traceback.format_exc(), data_source=data_source, transient=False)
                failures[key] = failure

        return imported_editions.values(), pools.values(), works.values(), failures

    def import_edition_from_metadata(
            self, metadata
    ):
        """ For the passed-in Metadata object, see if can find or create an Edition
            in the database. Also create a LicensePool if the Metadata has
            CirculationData in it.
        """
        # Locate or create an Edition for this book.
        edition, is_new_edition = metadata.edition(self._db)

        policy = ReplacementPolicy(
            subjects=True,
            links=True,
            contributions=True,
            rights=True,
            link_content=True,
            even_if_not_apparently_updated=True,
            mirrors=self.mirrors,
            content_modifier=self.content_modifier,
            http_get=self.http_get,
        )
        metadata.apply(
            edition=edition, collection=self.collection,
            metadata_client=self.metadata_client, replace=policy
        )

        return edition

    def update_work_for_edition(self, edition):
        """If possible, ensure that there is a presentation-ready Work for the
        given edition's primary identifier.
        """
        work = None

        # Find a LicensePool for the primary identifier. Any LicensePool will
        # do--the collection doesn't have to match, since all
        # LicensePools for a given identifier have the same Work.
        #
        # If we have CirculationData, a pool was created when we
        # imported the edition. If there was already a pool from a
        # different data source or a different collection, that's fine
        # too.
        pool = get_one(
            self._db, LicensePool, identifier=edition.primary_identifier,
            on_multiple='interchangeable'
        )

        if pool:
            if not pool.work or not pool.work.presentation_ready:
                # There is no presentation-ready Work for this
                # LicensePool. Try to create one.
                work, ignore = pool.calculate_work()
            else:
                # There is a presentation-ready Work for this LicensePool.
                # Use it.
                work = pool.work

        # If a presentation-ready Work already exists, there's no
        # rush. We might have new metadata that will change the Work's
        # presentation, but when we called Metadata.apply() the work
        # was set up to have its presentation recalculated in the
        # background, and that's good enough.
        return pool, work

    @classmethod
    def extract_next_links(self, feed):
        if isinstance(feed, basestring):
            parsed = feedparser.parse(feed)
        else:
            parsed = feed
        feed = parsed['feed']
        next_links = []
        if feed and 'links' in feed:
            next_links = [
                link['href'] for link in feed['links']
                if link['rel'] == 'next'
            ]
        return next_links

    def extract_last_update_dates(self, feed):
        if isinstance(feed, basestring):
            parsed_feed = feedparser.parse(feed)
        else:
            parsed_feed = feed
        dates = [
            self.last_update_date_for_feedparser_entry(entry)
            for entry in parsed_feed['entries']
        ]
        return [x for x in dates if x and x[1]]

    def build_identifier_mapping(self, external_urns):
        """Uses the given Collection and a list of URNs to reverse
        engineer an identifier mapping.

        NOTE: It would be better if .identifier_mapping weren't
        instance data, since a single OPDSImporter might import
        multiple pages of a feed. However, the code as written should
        work.
        """
        if not self.collection:
            return

        mapping = dict()
        identifiers_by_urn, failures = Identifier.parse_urns(
            self._db, external_urns, autocreate=False
        )
        external_identifiers = identifiers_by_urn.values()

        internal_identifier = aliased(Identifier)
        qu = self._db.query(Identifier, internal_identifier)\
            .join(Identifier.inbound_equivalencies)\
            .join(internal_identifier, Equivalency.input)\
            .join(internal_identifier.licensed_through)\
            .filter(
                Identifier.id.in_([x.id for x in external_identifiers]),
                LicensePool.collection==self.collection
            )

        for external_identifier, internal_identifier in qu:
            mapping[external_identifier] = internal_identifier

        self.identifier_mapping = mapping

    def extract_feed_data(self, feed, feed_url=None):
        """Turn an OPDS feed into lists of Metadata and CirculationData objects,
        with associated messages and next_links.
        """
        data_source = self.data_source
        fp_metadata, fp_failures = self.extract_data_from_feedparser(feed=feed, data_source=data_source)
        # gets: medium, measurements, links, contributors, etc.
        xml_data_meta, xml_failures = self.extract_metadata_from_elementtree(
            feed, data_source=data_source, feed_url=feed_url, do_get=self.http_get
        )

        if self.map_from_collection:
            # Build the identifier_mapping based on the Collection.
            self.build_identifier_mapping(fp_metadata.keys() + fp_failures.keys())

        # translate the id in failures to identifier.urn
        identified_failures = {}
        for urn, failure in fp_failures.items() + xml_failures.items():
            identifier, failure = self.handle_failure(urn, failure)
            identified_failures[identifier.urn] = failure

        # Use one loop for both, since the id will be the same for both dictionaries.
        metadata = {}
        circulationdata = {}
        for id, m_data_dict in fp_metadata.items():
            external_identifier, ignore = Identifier.parse_urn(self._db, id)
            if self.identifier_mapping:
                internal_identifier = self.identifier_mapping.get(
                    external_identifier, external_identifier)
            else:
                internal_identifier = external_identifier

            # Don't process this item if there was already an error
            if internal_identifier.urn in identified_failures.keys():
                continue

            identifier_obj = IdentifierData(
                type=internal_identifier.type,
                identifier=internal_identifier.identifier
            )

            # form the Metadata object
            xml_data_dict = xml_data_meta.get(id, {})
            combined_meta = self.combine(m_data_dict, xml_data_dict)
            if combined_meta.get('data_source') is None:
                combined_meta['data_source'] = self.data_source_name

            combined_meta['primary_identifier'] = identifier_obj

            metadata[internal_identifier.urn] = Metadata(**combined_meta)

            # Form the CirculationData that would correspond to this Metadata,
            # assuming there is a Collection to hold the LicensePool that
            # would result.
            c_data_dict = None
            if self.collection:
                c_circulation_dict = m_data_dict.get('circulation')
                xml_circulation_dict = xml_data_dict.get('circulation', {})
                c_data_dict = self.combine(c_circulation_dict, xml_circulation_dict)

            # Unless there's something useful in c_data_dict, we're
            # not going to put anything under metadata.circulation,
            # and any partial data that got added to
            # metadata.circulation is going to be removed.
            metadata[internal_identifier.urn].circulation = None
            if c_data_dict:
                circ_links_dict = {}
                # extract just the links to pass to CirculationData constructor
                if 'links' in xml_data_dict:
                    circ_links_dict['links'] = xml_data_dict['links']
                combined_circ = self.combine(c_data_dict, circ_links_dict)
                if combined_circ.get('data_source') is None:
                    combined_circ['data_source'] = self.data_source_name

                combined_circ['primary_identifier'] = identifier_obj
                circulation = CirculationData(**combined_circ)

                self._add_format_data(circulation)

                if circulation.formats:
                    metadata[internal_identifier.urn].circulation = circulation
                else:
                    # If the CirculationData has no formats, it
                    # doesn't really offer any way to actually get the
                    # book, and we don't want to create a
                    # LicensePool. All the circulation data is
                    # useless.
                    #
                    # TODO: This will need to be revisited when we add
                    # ODL support.
                    pass
        return metadata, identified_failures

    def handle_failure(self, urn, failure):
        """Convert a URN and a failure message that came in through
        an OPDS feed into an Identifier and a CoverageFailure object.

        The Identifier may not be the one designated by `urn` (if it's
        found in self.identifier_mapping) and the 'failure' may turn out not
        to be a CoverageFailure at all -- if it's an Identifier, that means
        that what a normal OPDSImporter would consider 'failure' is
        considered success.
        """
        external_identifier, ignore = Identifier.parse_urn(self._db, urn)
        if self.identifier_mapping:
            # The identifier found in the OPDS feed is different from
            # the identifier we want to export.
            internal_identifier = self.identifier_mapping.get(
                external_identifier, external_identifier)
        else:
            internal_identifier = external_identifier
        if isinstance(failure, Identifier):
            # The OPDSImporter does not actually consider this a
            # failure. Signal success by returning the internal
            # identifier as the 'failure' object.
            failure = internal_identifier
        else:
            # This really is a failure. Associate the internal
            # identifier with the CoverageFailure object.
            failure.obj = internal_identifier
        return internal_identifier, failure

    @classmethod
    def _add_format_data(cls, circulation):
        """Subclasses that specialize OPDS Import can implement this
        method to add formats to a CirculationData object with
        information that allows a patron to actually get a book
        that's not open access.
        """
        pass

    @classmethod
    def combine(self, d1, d2):
        """Combine two dictionaries that can be used as keyword arguments to
        the Metadata constructor.
        """
        if not d1 and not d2:
            return dict()
        if not d1:
            return dict(d2)
        if not d2:
            return dict(d1)
        new_dict = dict(d1)
        for k, v in d2.items():
            if k not in new_dict:
                # There is no value from d1. Even if the d2 value
                # is None, we want to set it.
                new_dict[k] = v
            elif v != None:
                # d1 provided a value, and d2 provided a value other
                # than None.
                if isinstance(v, list):
                    # The values are lists. Merge them.
                    new_dict[k].extend(v)
                elif isinstance(v, dict):
                    # The values are dicts. Merge them by with
                    # a recursive combine() call.
                    new_dict[k] = self.combine(new_dict[k], v)
                else:
                    # Overwrite d1's value with d2's value.
                    new_dict[k] = v
            else:
                # d1 provided a value and d2 provided None.  Do
                # nothing.
                pass
        return new_dict

    def extract_data_from_feedparser(self, feed, data_source):
        feedparser_parsed = feedparser.parse(feed)
        values = {}
        failures = {}
        for entry in feedparser_parsed['entries']:
            identifier, detail, failure = self.data_detail_for_feedparser_entry(entry=entry, data_source=data_source)

            if identifier:
                if failure:
                    failures[identifier] = failure
                else:
                    if detail:
                        values[identifier] = detail
            else:
                # That's bad. Can't make an item-specific error message, but write to
                # log that something very wrong happened.
                logging.error("Tried to parse an element without a valid identifier.  feed=%s" % feed)
        return values, failures


    @classmethod
    def extract_metadata_from_elementtree(cls, feed, data_source, feed_url=None, do_get=None):
        """Parse the OPDS as XML and extract all author and subject
        information, as well as ratings and medium.

        All the stuff that Feedparser can't handle so we have to use lxml.

        :return: a dictionary mapping IDs to dictionaries. The inner
            dictionary can be used as keyword arguments to the Metadata
            constructor.
        """
        values = {}
        failures = {}
        parser = cls.PARSER_CLASS()
        if isinstance(feed, bytes):
            inp = BytesIO(feed)
        else:
            # NOTE: etree will not parse certain Unicode strings.
            # It's generally better to feed it a bytestring.
            inp = StringIO(feed)
        root = etree.parse(inp)

        # Some OPDS feeds (eg Standard Ebooks) contain relative urls,
        # so we need the feed's self URL to extract links. If none was
        # passed in, we still might be able to guess.
        #
        # TODO: Section 2 of RFC 4287 says we should check xml:base
        # for this, so if anyone actually uses that we'll get around
        # to checking it.
        if not feed_url:
            links = [child.attrib for child in root.getroot() if 'link' in child.tag]
            self_links = [link['href'] for link in links if link.get('rel') == 'self']
            if self_links:
                feed_url = self_links[0]

        # First, turn Simplified <message> tags into CoverageFailure
        # objects.
        for failure in cls.coveragefailures_from_messages(
                data_source, parser, root
        ):
            if isinstance(failure, Identifier):
                # The Simplified <message> tag does not actually
                # represent a failure -- it was turned into an
                # Identifier instead of a CoverageFailure.
                urn = failure.urn
            else:
                urn = failure.obj.urn
            failures[urn] = failure

        # Then turn Atom <entry> tags into Metadata objects.
        for entry in parser._xpath(root, '/atom:feed/atom:entry'):
            identifier, detail, failure = cls.detail_for_elementtree_entry(
                parser, entry, data_source, feed_url, do_get=do_get
            )
            if identifier:
                if failure:
                    failures[identifier] = failure
                if detail:
                    values[identifier] = detail
        return values, failures

    @classmethod
    def _datetime(cls, entry, key):
        value = entry.get(key, None)
        if not value:
            return value
        return datetime.datetime(*value[:6])

    def last_update_date_for_feedparser_entry(self, entry):
        identifier = entry.get('id')
        updated = self._datetime(entry, 'updated_parsed')
        return (identifier, updated)

    @classmethod
    def data_detail_for_feedparser_entry(cls, entry, data_source):
        """Turn an entry dictionary created by feedparser into dictionaries of data
        that can be used as keyword arguments to the Metadata and CirculationData constructors.

        :return: A 3-tuple (identifier, kwargs for Metadata constructor, failure)
        """
        identifier = entry.get('id')
        if not identifier:
            return None, None, None

        # At this point we can assume that we successfully got some
        # metadata, and possibly a link to the actual book.
        try:
            kwargs_meta = cls._data_detail_for_feedparser_entry(entry, data_source)
            return identifier, kwargs_meta, None
        except Exception, e:
            _db = Session.object_session(data_source)
            identifier_obj, ignore = Identifier.parse_urn(_db, identifier)
            failure = CoverageFailure(
                identifier_obj, traceback.format_exc(), data_source,
                transient=True
            )
            return identifier, None, failure

    @classmethod
    def _data_detail_for_feedparser_entry(cls, entry, metadata_data_source):
        """Helper method that extracts metadata and circulation data from a feedparser
        entry. This method can be overridden in tests to check that callers handle things
        properly when it throws an exception.
        """
        title = entry.get('title', None)
        if title == OPDSFeed.NO_TITLE:
            title = None
        subtitle = entry.get('schema_alternativeheadline', None)

        # Generally speaking, a data source will provide either
        # metadata (e.g. the Simplified metadata wrangler) or both
        # metadata and circulation data (e.g. a publisher's ODL feed).
        #
        # However there is at least one case (the Simplified
        # open-access content server) where one server provides
        # circulation data from a _different_ data source
        # (e.g. Project Gutenberg).
        #
        # In this case we want the data source of the LicensePool to
        # be Project Gutenberg, but the data source of the pool's
        # presentation to be the open-access content server.
        #
        # The open-access content server uses a
        # <bibframe:distribution> tag to keep track of which data
        # source provides the circulation data.
        circulation_data_source = metadata_data_source
        circulation_data_source_tag = entry.get('bibframe_distribution')
        if circulation_data_source_tag:
            circulation_data_source_name = circulation_data_source_tag.get(
                'bibframe:providername'
            )
            if circulation_data_source_name:
                _db = Session.object_session(metadata_data_source)
                # We know this data source offers licenses because
                # that's what the <bibframe:distribution> is there
                # to say.
                circulation_data_source = DataSource.lookup(
                    _db, circulation_data_source_name, autocreate=True,
                    offers_licenses=True
                )
                if not circulation_data_source:
                    raise ValueError(
                        "Unrecognized circulation data source: %s" % (
                            circulation_data_source_name
                        )
                    )
        last_opds_update = cls._datetime(entry, 'updated_parsed')

        publisher = entry.get('publisher', None)
        if not publisher:
            publisher = entry.get('dcterms_publisher', None)

        language = entry.get('language', None)
        if not language:
            language = entry.get('dcterms_language', None)

        links = []

        def summary_to_linkdata(detail):
            if not detail:
                return None
            if not 'value' in detail or not detail['value']:
                return None

            content = detail['value']
            media_type = detail.get('type', 'text/plain')
            return cls.make_link_data(
                rel=Hyperlink.DESCRIPTION,
                media_type=media_type,
                content=content
            )

        summary_detail = entry.get('summary_detail', None)
        link = summary_to_linkdata(summary_detail)
        if link:
            links.append(link)

        for content_detail in entry.get('content', []):
            link = summary_to_linkdata(content_detail)
            if link:
                links.append(link)

        rights_uri = cls.rights_uri_from_feedparser_entry(entry)

        kwargs_meta = dict(
            title=title,
            subtitle=subtitle,
            language=language,
            publisher=publisher,
            links=links,
            # refers to when was updated in opds feed, not our db
            data_source_last_updated=last_opds_update,
        )

        # Although we always provide the CirculationData, it will only
        # be used if the OPDSImporter has a Collection to hold the
        # LicensePool that will result from importing it.
        kwargs_circ = dict(
            data_source=circulation_data_source.name,
            links=list(links),
            default_rights_uri=rights_uri,
        )
        kwargs_meta['circulation'] = kwargs_circ
        return kwargs_meta

    @classmethod
    def rights_uri(cls, rights_string):
        """Determine the URI that best encapsulates the rights status of
        the downloads associated with this book.
        """
        return RightsStatus.rights_uri_from_string(rights_string)

    @classmethod
    def rights_uri_from_feedparser_entry(cls, entry):
        """Extract a rights URI from a parsed feedparser entry.

        :return: A rights URI.
        """
        rights = entry.get('rights', "")
        return cls.rights_uri(rights)

    @classmethod
    def rights_uri_from_entry_tag(cls, entry):
        """Extract a rights string from an lxml <entry> tag.

        :return: A rights URI.
        """
        rights = cls.PARSER_CLASS._xpath1(entry, 'rights')
        if rights:
            return cls.rights_uri(rights)

    @classmethod
    def extract_messages(cls, parser, feed_tag):
        """Extract <simplified:message> tags from an OPDS feed and convert
        them into OPDSMessage objects.
        """
        path = '/atom:feed/simplified:message'
        for message_tag in parser._xpath(feed_tag, path):

            # First thing to do is determine which Identifier we're
            # talking about.
            identifier_tag = parser._xpath1(message_tag, 'atom:id')
            if identifier_tag is None:
                urn = None
            else:
                urn = identifier_tag.text

            # What status code is associated with the message?
            status_code_tag = parser._xpath1(message_tag, 'simplified:status_code')
            if status_code_tag is None:
                status_code = None
            else:
                try:
                    status_code = int(status_code_tag.text)
                except ValueError:
                    status_code = None

            # What is the human-readable message?
            description_tag = parser._xpath1(message_tag, 'schema:description')
            if description_tag is None:
                description = ''
            else:
                description = description_tag.text

            yield OPDSMessage(urn, status_code, description)

    @classmethod
    def coveragefailures_from_messages(cls, data_source, parser, feed_tag):
        """Extract CoverageFailure objects from a parsed OPDS document. This
        allows us to determine the fate of books which could not
        become <entry> tags.
        """
        for message in cls.extract_messages(parser, feed_tag):
            failure = cls.coveragefailure_from_message(data_source, message)
            if failure:
                yield failure

    @classmethod
    def coveragefailure_from_message(cls, data_source, message):
        """Turn a <simplified:message> tag into a CoverageFailure."""

        _db = Session.object_session(data_source)

        # First thing to do is determine which Identifier we're
        # talking about. If we can't do that, we can't create a
        # CoverageFailure object.
        urn = message.urn
        try:
            identifier, ignore = Identifier.parse_urn(_db, urn)
        except ValueError, e:
            identifier = None

        if not identifier:
            # We can't associate this message with any particular
            # Identifier so we can't turn it into a CoverageFailure.
            return None

        if (cls.SUCCESS_STATUS_CODES
            and message.status_code in cls.SUCCESS_STATUS_CODES):
            # This message is telling us that nothing went wrong. It
            # should be treated as a success.
            return identifier

        if message.status_code == 200:
            # By default, we treat a message with a 200 status code
            # as though nothing had been returned at all.
            return None

        description = message.message
        status_code = message.status_code
        if description and status_code:
            exception = u"%s: %s" % (status_code, description)
        elif status_code:
            exception = unicode(status_code)
        elif description:
            exception = description
        else:
            exception = 'No detail provided.'

        # All these CoverageFailures are transient because ATM we can
        # only assume that the server will eventually have the data.
        return CoverageFailure(
            identifier, exception, data_source, transient=True
        )

    @classmethod
    def detail_for_elementtree_entry(
            cls, parser, entry_tag, data_source, feed_url=None, do_get=None
    ):

        """Turn an <atom:entry> tag into a dictionary of metadata that can be
        used as keyword arguments to the Metadata contructor.

        :return: A 2-tuple (identifier, kwargs)
        """

        identifier = parser._xpath1(entry_tag, 'atom:id')
        if identifier is None or not identifier.text:
            # This <entry> tag doesn't identify a book so we
            # can't derive any information from it.
            return None, None, None
        identifier = identifier.text

        try:
            data = cls._detail_for_elementtree_entry(
                parser, entry_tag, feed_url, do_get=do_get
            )
            return identifier, data, None

        except Exception, e:
            _db = Session.object_session(data_source)
            identifier_obj, ignore = Identifier.parse_urn(_db, identifier)
            failure = CoverageFailure(
                identifier_obj, traceback.format_exc(), data_source,
                transient=True
            )
            return identifier, None, failure

    @classmethod
    def _detail_for_elementtree_entry(cls, parser, entry_tag, feed_url=None, do_get=None):
        """Helper method that extracts metadata and circulation data from an elementtree
        entry. This method can be overridden in tests to check that callers handle things
        properly when it throws an exception.
        """
        # We will fill this dictionary with all the information
        # we can find.
        data = dict()

        alternate_identifiers = []
        for id_tag in parser._xpath(entry_tag, "dcterms:identifier"):
            v = cls.extract_identifier(id_tag)
            if v:
                alternate_identifiers.append(v)
        data['identifiers'] = alternate_identifiers

        data['contributors'] = []
        for author_tag in parser._xpath(entry_tag, 'atom:author'):
            contributor = cls.extract_contributor(parser, author_tag)
            if contributor is not None:
                data['contributors'].append(contributor)

        data['subjects'] = [
            cls.extract_subject(parser, category_tag)
            for category_tag in parser._xpath(entry_tag, 'atom:category')
        ]

        ratings = []
        for rating_tag in parser._xpath(entry_tag, 'schema:Rating'):
            v = cls.extract_measurement(rating_tag)
            if v:
                ratings.append(v)
        data['measurements'] = ratings
        rights_uri = cls.rights_uri_from_entry_tag(entry_tag)

        data['links'] = cls.consolidate_links([
            cls.extract_link(link_tag, feed_url, rights_uri)
            for link_tag in parser._xpath(entry_tag, 'atom:link')
        ])

        derived_medium = cls.get_medium_from_links(data['links'])
        data['medium'] = cls.extract_medium(entry_tag, derived_medium)

        series_tag = parser._xpath(entry_tag, 'schema:Series')
        if series_tag:
            data['series'], data['series_position'] = cls.extract_series(series_tag[0])

        issued_tag = parser._xpath(entry_tag, 'dcterms:issued')
        if issued_tag:
            date_string = issued_tag[0].text
            # By default, the date for strings that only have a year will
            # be set to January 1 rather than the current date.
            default = datetime.datetime(datetime.datetime.now().year, 1, 1)
            try:
                data["published"] = dateutil.parser.parse(date_string, default=default)
            except Exception, e:
                # This entry had an issued tag, but it was in a format we couldn't parse.
                pass

        return data

    @classmethod
    def get_medium_from_links(cls, links):
        """Get medium if derivable from information in an acquisition link."""
        derived = None
        for link in links:
            if (not link.rel
                or not link.media_type
                or not link.rel.startswith('http://opds-spec.org/acquisition/')
            ):
                continue
            derived = Edition.medium_from_media_type(link.media_type)
            if derived:
                break
        return derived

    @classmethod
    def extract_identifier(cls, identifier_tag):
        """Turn a <dcterms:identifier> tag into an IdentifierData object."""
        try:
            type, identifier = Identifier.type_and_identifier_for_urn(identifier_tag.text.lower())
            return IdentifierData(type, identifier)
        except ValueError:
            return None

    @classmethod
    def extract_medium(cls, entry_tag, default=Edition.BOOK_MEDIUM):
        """Derive a value for Edition.medium from schema:additionalType or
        from a <dcterms:format> subtag.

        :param entry_tag: A <atom:entry> tag.
        :param default: The value to use if nothing is found.
        """
        if not entry_tag:
            return default

        medium = None
        additional_type = entry_tag.get('{http://schema.org/}additionalType')
        if additional_type:
            medium = Edition.additional_type_to_medium.get(
                additional_type, None
            )
        if not medium:
            format_tag = entry_tag.find('{http://purl.org/dc/terms/}format')
            if format_tag is not None:
                media_type = format_tag.text
                medium = Edition.medium_from_media_type(media_type)
        return medium or default

    @classmethod
    def extract_contributor(cls, parser, author_tag):
        """Turn an <atom:author> tag into a ContributorData object."""
        subtag = parser.text_of_optional_subtag
        sort_name = subtag(author_tag, 'simplified:sort_name')
        display_name = subtag(author_tag, 'atom:name')
        family_name = subtag(author_tag, "simplified:family_name")
        wikipedia_name = subtag(author_tag, "simplified:wikipedia_name")
        # TODO: we need a way of conveying roles. I believe Bibframe
        # has the answer.

        # TODO: Also collect VIAF and LC numbers if present.  This
        # requires parsing the URIs. Only the metadata wrangler will
        # provide this information.

        viaf = None
        if sort_name or display_name or viaf:
            return ContributorData(
                sort_name=sort_name, display_name=display_name,
                family_name=family_name,
                wikipedia_name=wikipedia_name,
                roles=None
            )

        logging.info("Refusing to create ContributorData for contributor with no sort name, display name, or VIAF.")
        return None

    @classmethod
    def extract_subject(cls, parser, category_tag):
        """Turn an <atom:category> tag into a SubjectData object."""
        attr = category_tag.attrib

        # Retrieve the type of this subject - FAST, Dewey Decimal,
        # etc.
        scheme = attr.get('scheme')
        subject_type = Subject.by_uri.get(scheme)
        if not subject_type:
            # We can't represent this subject because we don't
            # know its scheme. Just treat it as a tag.
            subject_type = Subject.TAG

        # Retrieve the term (e.g. "827") and human-readable name
        # (e.g. "English Satire & Humor") for this subject.
        term = attr.get('term')
        name = attr.get('label')
        default_weight = 1

        weight = attr.get('{http://schema.org/}ratingValue', default_weight)
        try:
            weight = int(weight)
        except ValueError, e:
            weight = default_weight

        return SubjectData(
            type=subject_type,
            identifier=term,
            name=name,
            weight=weight
        )

    @classmethod
    def extract_link(cls, link_tag, feed_url=None, entry_rights_uri=None):
        """Convert a <link> tag into a LinkData object.

        :param feed_url: The URL to the enclosing feed, for use in resolving
            relative links.

        :param entry_rights_uri: A URI describing the rights advertised
            in the entry. Unless this specific link says otherwise, we
            will assume that the representation on the other end of the link
            if made available on these terms.
        """
        attr = link_tag.attrib
        rel = attr.get('rel')
        media_type = attr.get('type')
        href = attr.get('href')
        if not href or not rel:
            # The link exists but has no destination, or no specified
            # relationship to the entry.
            return None
        rights = attr.get('{%s}rights' % OPDSXMLParser.NAMESPACES["dcterms"])
        if rights:
            # Rights associated with the link override rights
            # associated with the entry.
            rights_uri = cls.rights_uri(rights)
        else:
            rights_uri = entry_rights_uri
        if feed_url and not urlparse(href).netloc:
            # This link is relative, so we need to get the absolute url
            href = urljoin(feed_url, href)
        return cls.make_link_data(rel, href, media_type, rights_uri)

    @classmethod
    def make_link_data(cls, rel, href=None, media_type=None, rights_uri=None,
                       content=None):
        """Hook method for creating a LinkData object.

        Intended to be overridden in subclasses.
        """
        return LinkData(rel=rel, href=href, media_type=media_type,
                        rights_uri=rights_uri, content=content
        )

    @classmethod
    def consolidate_links(cls, links):
        """Try to match up links with their thumbnails.

        If link n is an image and link n+1 is a thumbnail, then the
        thumbnail is assumed to be the thumbnail of the image.

        Similarly if link n is a thumbnail and link n+1 is an image.
        """
        # Strip out any links that didn't get turned into LinkData objects
        # due to missing `href` or whatever.
        new_links = [x for x in links if x]

        # Make a new list of links from that list, to iterate over --
        # we'll be modifying new_links in place so we can't iterate
        # over it.
        links = list(new_links)

        next_link_already_handled = False
        for i, link in enumerate(links):

            if link.rel not in (Hyperlink.THUMBNAIL_IMAGE, Hyperlink.IMAGE):
                # This is not any kind of image. Ignore it.
                continue

            if next_link_already_handled:
                # This link and the previous link were part of an
                # image-thumbnail pair.
                next_link_already_handled = False
                continue

            if i == len(links)-1:
                # This is the last link. Since there is no next link
                # there's nothing to do here.
                continue

            # Peek at the next link.
            next_link = links[i+1]


            if (link.rel == Hyperlink.THUMBNAIL_IMAGE
                and next_link.rel == Hyperlink.IMAGE):
                # This link is a thumbnail and the next link is
                # (presumably) the corresponding image.
                thumbnail_link = link
                image_link = next_link
            elif (link.rel == Hyperlink.IMAGE
                  and next_link.rel == Hyperlink.THUMBNAIL_IMAGE):
                thumbnail_link = next_link
                image_link = link
            else:
                # This link and the next link do not form an
                # image-thumbnail pair. Do nothing.
                continue

            image_link.thumbnail = thumbnail_link
            new_links.remove(thumbnail_link)
            next_link_already_handled = True

        return new_links

    @classmethod
    def extract_measurement(cls, rating_tag):
        type = rating_tag.get('{http://schema.org/}additionalType')
        value = rating_tag.get('{http://schema.org/}ratingValue')
        if not value:
            value = rating_tag.attrib.get('{http://schema.org}ratingValue')
        if not type:
            type = Measurement.RATING
        try:
            value = float(value)
            return MeasurementData(
                quantity_measured=type,
                value=value,
            )
        except ValueError:
            return None

    @classmethod
    def extract_series(cls, series_tag):
        attr = series_tag.attrib
        series_name = attr.get('{http://schema.org/}name', None)
        series_position = attr.get('{http://schema.org/}position', None)
        return series_name, series_position


class OPDSImportMonitor(CollectionMonitor, HasSelfTests):
    """Periodically monitor a Collection's OPDS archive feed and import
    every title it mentions.
    """

    SERVICE_NAME = "OPDS Import Monitor"

    # The first time this Monitor is invoked we want to get the
    # entire OPDS feed.
    DEFAULT_START_TIME = CollectionMonitor.NEVER

    # The protocol this Monitor works with. Subclasses that
    # specialize OPDS import should override this.
    PROTOCOL = ExternalIntegration.OPDS_IMPORT

    def __init__(self, _db, collection, import_class,
                 force_reimport=False, **import_class_kwargs):
        if not collection:
            raise ValueError(
                "OPDSImportMonitor can only be run in the context of a Collection."
            )

        if collection.protocol != self.PROTOCOL:
            raise ValueError(
                "Collection %s is configured for protocol %s, not %s." % (
                    collection.name, collection.protocol, self.PROTOCOL
                )
            )

        data_source = self.data_source(collection)
        if not data_source:
            raise ValueError(
                "Collection %s has no associated data source." % collection.name
            )

        self.external_integration_id = collection.external_integration.id
        self.feed_url = self.opds_url(collection)
        self.force_reimport = force_reimport
        self.username = collection.external_integration.username
        self.password = collection.external_integration.password
        self.importer = import_class(
            _db, collection=collection, **import_class_kwargs
        )
        super(OPDSImportMonitor, self).__init__(_db, collection)

    def external_integration(self, _db):
        return get_one(_db, ExternalIntegration,
                       id=self.external_integration_id)

    def _run_self_tests(self, _db):
        """Retrieve the first page of the OPDS feed"""
        first_page = self.run_test(
            "Retrieve the first page of the OPDS feed (%s)" % self.feed_url,
            self.follow_one_link, self.feed_url
        )
        yield first_page
        if not first_page.result:
            return

        # We got a page, but does it have anything the importer can
        # turn into a Work?
        #
        # By default, this means it must contain an open-access link.
        url, content = first_page.result
        yield self.run_test(
            "Checking for importable content",
            self.importer.assert_importable_content,
            content, url
        )

    def _get(self, url, headers):
        """Make the sort of HTTP request that's normal for an OPDS feed.

        Long timeout, raise error on anything but 2xx or 3xx.
        """
        headers = self._update_headers(headers)
        kwargs = dict(timeout=120, allowed_response_codes=['2xx', '3xx'])
        response = HTTP.get_with_timeout(url, headers=headers, **kwargs)
        return response.status_code, response.headers, response.content

    def _get_accept_header(self):
        types = dict(
            opds_acquisition=OPDSFeed.ACQUISITION_FEED_TYPE,
            atom="application/atom+xml",
            xml="application/xml",
            anything="*/*",
        )
        accept_header = "%(opds_acquisition)s, %(atom)s;q=0.9, %(xml)s;q=0.8, %(anything)s;q=0.1" % types

        return accept_header

    def _update_headers(self, headers):
        headers = dict(headers or {})
        if self.username and self.password and not 'Authorization' in headers:
            creds = "%s:%s" % (self.username, self.password)
            auth_header = "Basic %s" % base64.b64encode(creds)
            headers['Authorization'] = auth_header

        if not 'Accept' in headers:
            headers['Accept'] = self._get_accept_header()
        return headers

    def opds_url(self, collection):
        """Returns the OPDS import URL for the given collection.

        By default, this URL is stored as the external account ID, but
        subclasses may override this.
        """
        return collection.external_account_id

    def data_source(self, collection):
        """Returns the data source name for the given collection.

        By default, this URL is stored as a setting on the collection, but
        subclasses may hard-code it.
        """
        return collection.data_source

    def feed_contains_new_data(self, feed):
        """Does the given feed contain any entries that haven't been imported
        yet?
        """
        if self.force_reimport:
            # We don't even need to check. Always treat the feed as
            # though it contained new data.
            return True

        # For every item in the last page of the feed, check when that
        # item was last updated.
        last_update_dates = self.importer.extract_last_update_dates(feed)

        new_data = False
        for identifier, remote_updated in last_update_dates:

            identifier, ignore = Identifier.parse_urn(self._db, identifier)
            if not identifier:
                # Maybe this is new, maybe not, but we can't associate
                # the information with an Identifier, so we can't do
                # anything about it.
                self.log.info(
                    "Ignoring %s because unable to turn into an Identifier."
                )
                continue

            if self.identifier_needs_import(identifier, remote_updated):
                new_data = True
                break
        return new_data

    def identifier_needs_import(self, identifier, last_updated_remote):
        """Does the remote side have new information about this Identifier?

        :param identifier: An Identifier.
        :param last_update_remote: The last time the remote side updated
            the OPDS entry for this Identifier.
        """
        if not identifier:
            return False

        record = CoverageRecord.lookup(
            identifier, self.importer.data_source,
            operation=CoverageRecord.IMPORT_OPERATION
        )

        if not record:
            # We have no record of importing this Identifier. Import
            # it now.
            self.log.info(
                "Counting %s as new because it has no CoverageRecord.",
                identifier
            )
            return True

        # If there was a transient failure last time we tried to
        # import this book, try again regardless of whether the
        # feed has changed.
        if record.status == CoverageRecord.TRANSIENT_FAILURE:
            self.log.info(
                "Counting %s as new because previous attempt resulted in transient failure: %s",
                identifier, record.exception
            )
            return True

        # If our last attempt was a success or a persistent
        # failure, we only want to import again if something
        # changed since then.

        if record.timestamp:
            # We've imported this entry before, so don't import it
            # again unless it's changed.

            if not last_updated_remote:
                # The remote isn't telling us whether the entry
                # has been updated. Import it again to be safe.
                self.log.info(
                    "Counting %s as new because remote has no information about when it was updated.",
                    identifier
                )
                return True

            if last_updated_remote >= record.timestamp:
                # This book has been updated.
                self.log.info(
                    "Counting %s as new because its coverage date is %s and remote has %s.",
                    identifier, record.timestamp, last_updated_remote
                )
                return True

    def _verify_media_type(self, url, status_code, headers, feed):
        # Make sure we got an OPDS feed, and not an error page that was
        # sent with a 200 status code.
        media_type = headers.get('content-type')
        if not media_type or not any(
            x in media_type for x in (OPDSFeed.ATOM_LIKE_TYPES)
        ):
            message = "Expected Atom feed, got %s" % media_type
            raise BadResponseException(
                url, message=message, debug_message=feed,
                status_code=status_code
            )

    def follow_one_link(self, url, do_get=None):
        """Download a representation of a URL and extract the useful
        information.

        :return: A 2-tuple (next_links, feed). `next_links` is a list of
            additional links that need to be followed. `feed` is the content
            that needs to be imported.
        """
        self.log.info("Following next link: %s", url)
        get = do_get or self._get
        status_code, headers, feed = get(url, {})

        self._verify_media_type(url, status_code, headers, feed)

        new_data = self.feed_contains_new_data(feed)

        if new_data:
            # There's something new on this page, so we need to check
            # the next page as well.
            next_links = self.importer.extract_next_links(feed)
            return next_links, feed
        else:
            # There's nothing new, so we don't need to import this
            # feed or check the next page.
            self.log.info("No new data.")
            return [], None

    def import_one_feed(self, feed):
        """Import every book mentioned in an OPDS feed."""

        # Because we are importing into a Collection, we will immediately
        # mark a book as presentation-ready if possible.
        imported_editions, pools, works, failures = self.importer.import_from_feed(
            feed,
            feed_url=self.opds_url(self.collection)
        )

        # Create CoverageRecords for the successful imports.
        for edition in imported_editions:
            record = CoverageRecord.add_for(
                edition, self.importer.data_source,
                CoverageRecord.IMPORT_OPERATION,
                status=CoverageRecord.SUCCESS
            )

        # Create CoverageRecords for the failures.
        for urn, failure in failures.items():
            failure.to_coverage_record(
                operation=CoverageRecord.IMPORT_OPERATION
            )
        return imported_editions, failures

    def run_once(self, progress_ignore):
        feeds = []
        queue = [self.feed_url]
        seen_links = set([])

        # First, follow the feed's next links until we reach a page with
        # nothing new. If any link raises an exception, nothing will be imported.

        total_imported = 0
        total_failures = 0

        while queue:
            new_queue = []

            for link in queue:
                if link in seen_links:
                    continue
                next_links, feed = self.follow_one_link(link)
                new_queue.extend(next_links)
                if feed:
                    feeds.append((link, feed))
                seen_links.add(link)

            queue = new_queue

        # Start importing at the end. If something fails, it will be easier to
        # pick up where we left off.
        for link, feed in reversed(feeds):
            self.log.info("Importing next feed: %s", link)
            imported_editions, failures = self.import_one_feed(feed)
            total_imported += len(imported_editions)
            total_failures += len(failures)
            self._db.commit()

        achievements = "Items imported: %d. Failures: %d." % (
            total_imported, total_failures
        )

        return TimestampData(achievements=achievements)
