from nose.tools import set_trace
import base64
import urlparse
import time
import hmac
import hashlib
import os
import re
import logging
from datetime import datetime, timedelta
from sqlalchemy.orm.session import Session

from config import (
    Configuration,
    CannotLoadConfiguration,
    temp_config,
)
from coverage import (
    BibliographicCoverageProvider
)
from model import (
    get_one,
    get_one_or_create,
    Collection,
    Contributor,
    DataSource,
    DeliveryMechanism,
    ExternalIntegration,
    Representation,
    Hyperlink,
    Identifier,
    Library,
    Measurement,
    Edition,
    Subject,
)

from metadata_layer import (
    ContributorData,
    CirculationData,
    Metadata,
    LinkData,
    IdentifierData,
    FormatData,
    MeasurementData,
    SubjectData,
)

from testing import DatabaseTest

from util.http import HTTP
from util.xmlparser import XMLParser

class BibliothecaAPI(object):

    # TODO: %a and %b are localized per system, but 3M requires
    # English.
    AUTH_TIME_FORMAT = "%a, %d %b %Y %H:%M:%S GMT"
    ARGUMENT_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
    AUTHORIZATION_FORMAT = "3MCLAUTH %s:%s"

    DATETIME_HEADER = "3mcl-Datetime"
    AUTHORIZATION_HEADER = "3mcl-Authorization"
    VERSION_HEADER = "3mcl-Version"

    MAX_METADATA_AGE = timedelta(days=180)

    log = logging.getLogger("Bibliotheca API")

    DEFAULT_VERSION = "2.0"
    DEFAULT_BASE_URL = "https://partner.yourcloudlibrary.com/"

    def __init__(self, _db, collection):

        if collection.protocol != ExternalIntegration.BIBLIOTHECA:
            raise ValueError(
                "Collection protocol is %s, but passed into BibliothecaAPI!" %
                collection.protocol
            )

        self._db = _db
        self.version = (
            collection.external_integration.setting('version').value or self.DEFAULT_VERSION
        )
        self.account_id = collection.external_integration.username
        self.account_key = collection.external_integration.password
        self.library_id = collection.external_account_id
        self.base_url = collection.external_integration.url or self.DEFAULT_BASE_URL

        if not self.account_id or not self.account_key or not self.library_id:
            raise CannotLoadConfiguration(
                "Bibliotheca configuration is incomplete."
            )

        # Use utf8 instead of unicode encoding
        settings = [self.account_id, self.account_key, self.library_id]
        self.account_id, self.account_key, self.library_id = (
            setting.encode('utf8') for setting in settings
        )

        self.item_list_parser = ItemListParser()
        self.collection_id = collection.id

    @property
    def collection(self):
        return Collection.by_id(self._db, id=self.collection_id)

    @property
    def source(self):
        return DataSource.lookup(self._db, DataSource.BIBLIOTHECA)

    def now(self):
        """Return the current GMT time in the format 3M expects."""
        return time.strftime(self.AUTH_TIME_FORMAT, time.gmtime())

    def sign(self, method, headers, path):
        """Add appropriate headers to a request."""
        authorization, now = self.authorization(method, path)
        headers[self.DATETIME_HEADER] = now
        headers[self.VERSION_HEADER] = self.version
        headers[self.AUTHORIZATION_HEADER] = authorization

    def authorization(self, method, path):
        signature, now = self.signature(method, path)
        auth = self.AUTHORIZATION_FORMAT % (self.account_id, signature)
        return auth, now

    def signature(self, method, path):
        now = self.now()
        signature_components = [now, method, path]
        signature_string = "\n".join(signature_components)
        digest = hmac.new(self.account_key, msg=signature_string,
                    digestmod=hashlib.sha256).digest()
        signature = base64.standard_b64encode(digest)
        return signature, now

    def full_url(self, path):
        if not path.startswith("/cirrus"):
            path = self.full_path(path)
        return urlparse.urljoin(self.base_url, path)

    def full_path(self, path):
        if not path.startswith("/"):
            path = "/" + path
        if not path.startswith("/cirrus"):
            path = "/cirrus/library/%s%s" % (self.library_id, path)
        return path

    def request(self, path, body=None, method="GET", identifier=None,
                max_age=None):
        path = self.full_path(path)
        url = self.full_url(path)
        if method == 'GET':
            headers = {"Accept" : "application/xml"}
        else:
            headers = {"Content-Type" : "application/xml"}
        self.sign(method, headers, path)
        # print headers
        # self.log.debug("3M request: %s %s", method, url)
        if max_age and method=='GET':
            representation, cached = Representation.get(
                self._db, url, extra_request_headers=headers,
                do_get=self._simple_http_get, max_age=max_age,
                exception_handler=Representation.reraise_exception,
            )
            content = representation.content
            return content
        else:
            return self._request_with_timeout(
                method, url, data=body, headers=headers,
                allow_redirects=False
            )

    def get_bibliographic_info_for(self, editions, max_age=None):
        results = dict()
        for edition in editions:
            identifier = edition.primary_identifier
            metadata = self.bibliographic_lookup(identifier, max_age)
            if metadata:
                results[identifier] = (edition, metadata)
        return results

    def bibliographic_lookup_request(self, identifier, max_age=None):
        return self.request(
            "/items/%s" % identifier.identifier,
            max_age=max_age or self.MAX_METADATA_AGE
        )


    def bibliographic_lookup(self, identifier, max_age=None):
        data = self.bibliographic_lookup_request(identifier, max_age)
        response = list(self.item_list_parser.parse(data))
        if not response:
            return None
        else:
            [metadata] = response
        return metadata

    def _request_with_timeout(self, method, url, *args, **kwargs):
        """This will be overridden in MockBibliothecaAPI."""
        return HTTP.request_with_timeout(method, url, *args, **kwargs)

    def _simple_http_get(self, url, headers, *args, **kwargs):
        """This will be overridden in MockBibliothecaAPI."""
        return Representation.simple_http_get(url, headers, *args, **kwargs)


class MockBibliothecaAPI(BibliothecaAPI):

    @classmethod
    def mock_collection(self, _db):
        """Create a mock Bibliotheca collection for use in tests."""
        library = DatabaseTest.make_default_library(_db)
        collection, ignore = get_one_or_create(
            _db, Collection,
            name="Test Bibliotheca Collection", create_method_kwargs=dict(
                external_account_id=u'c',
            )
        )
        integration = collection.create_external_integration(
            protocol=ExternalIntegration.BIBLIOTHECA
        )
        integration.username = u'a'
        integration.password = u'b'
        integration.url = "http://bibliotheca.test"
        library.collections.append(collection)
        return collection

    def __init__(self, _db, collection, *args, **kwargs):
        self.responses = []
        self.requests = []
        super(MockBibliothecaAPI, self).__init__(
            _db, collection, *args, **kwargs
        )

    def now(self):
        """Return an unvarying time in the format Bibliotheca expects."""
        return datetime.strftime(
            datetime(2016, 1, 1), self.AUTH_TIME_FORMAT
        )

    def queue_response(self, status_code, headers={}, content=None):
        from testing import MockRequestsResponse
        self.responses.insert(
            0, MockRequestsResponse(status_code, headers, content)
        )

    def _request_with_timeout(self, method, url, *args, **kwargs):
        """Simulate HTTP.request_with_timeout."""
        self.requests.append([method, url, args, kwargs])
        response = self.responses.pop()
        return HTTP._process_response(
            url, response, kwargs.get('allowed_response_codes'),
            kwargs.get('disallowed_response_codes')
        )

    def _simple_http_get(self, url, headers, *args, **kwargs):
        """Simulate Representation.simple_http_get."""
        response = self._request_with_timeout('GET', url, *args, **kwargs)
        return response.status_code, response.headers, response.content




class ItemListParser(XMLParser):

    DATE_FORMAT = "%Y-%m-%d"
    YEAR_FORMAT = "%Y"

    NAMESPACES = {}

    def parse(self, xml):
        for i in self.process_all(xml, "//Item"):
            yield i

    parenthetical = re.compile(" \([^)]+\)$")


    format_data_for_bibliotheca_format = {
        "EPUB" : (
            Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM
        ),
        "EPUB3" : (
            Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM
        ),
        "PDF" : (
            Representation.PDF_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM
        ),
        "MP3" : (
            Representation.MP3_MEDIA_TYPE, DeliveryMechanism.FINDAWAY_DRM
        ),
    }

    @classmethod
    def contributors_from_string(cls, string, role=Contributor.AUTHOR_ROLE):
        contributors = []
        if not string:
            return contributors

        for sort_name in string.split(';'):
            sort_name = cls.parenthetical.sub("", sort_name.strip())
            contributors.append(
                ContributorData(
                    sort_name=sort_name.strip(),
                    roles=[role]
                )
            )
        return contributors

    @classmethod
    def parse_genre_string(self, s):
        genres = []
        if not s:
            return genres
        for i in s.split(","):
            i = i.strip()
            if not i:
                continue
            i = i.replace("&amp;amp;", "&amp;").replace("&amp;", "&").replace("&#39;", "'")
            genres.append(SubjectData(Subject.BISAC, None, i, weight=15))
        return genres


    def process_one(self, tag, namespaces):
        """Turn an <item> tag into a Metadata and an encompassed CirculationData
        objects, and return the Metadata."""

        def value(bibliotheca_key):
            return self.text_of_optional_subtag(tag, bibliotheca_key)

        links = dict()
        identifiers = dict()
        subjects = []

        primary_identifier = IdentifierData(
            Identifier.BIBLIOTHECA_ID, value("ItemId")
        )

        identifiers = []
        for key in ('ISBN13', 'PhysicalISBN'):
            v = value(key)
            if v:
                identifiers.append(
                    IdentifierData(Identifier.ISBN, v)
                )

        subjects = self.parse_genre_string(value("Genre"))

        title = value("Title")
        subtitle = value("SubTitle")
        publisher = value("Publisher")
        language = value("Language")

        authors = list(self.contributors_from_string(value('Authors')))
        narrators = list(
            self.contributors_from_string(
                value('Narrator'), Contributor.NARRATOR_ROLE
            )
        )

        published_date = None
        published = value("PubDate")
        if published:
            formats = [self.DATE_FORMAT, self.YEAR_FORMAT]
        else:
            published = value("PubYear")
            formats = [self.YEAR_FORMAT]

        for format in formats:
            try:
                published_date = datetime.strptime(published, format)
            except ValueError, e:
                pass

        links = []
        description = value("Description")
        if description:
            links.append(
                LinkData(rel=Hyperlink.DESCRIPTION, content=description)
            )

        cover_url = value("CoverLinkURL").replace("&amp;", "&")
        links.append(LinkData(rel=Hyperlink.IMAGE, href=cover_url))

        alternate_url = value("BookLinkURL").replace("&amp;", "&")
        links.append(LinkData(rel='alternate', href=alternate_url))

        measurements = []
        pages = value("NumberOfPages")
        if pages:
            pages = int(pages)
            measurements.append(
                MeasurementData(quantity_measured=Measurement.PAGE_COUNT,
                                value=pages)
            )

        book_format = value("BookFormat")
        medium, formats = self.internal_formats(book_format)

        metadata = Metadata(
            data_source=DataSource.BIBLIOTHECA,
            title=title,
            subtitle=subtitle,
            language=language,
            medium=medium,
            publisher=publisher,
            published=published_date,
            primary_identifier=primary_identifier,
            identifiers=identifiers,
            subjects=subjects,
            contributors=authors+narrators,
            measurements=measurements,
            links=links,
        )

        # Also make a CirculationData so we can write the formats,
        circulationdata = CirculationData(
            data_source=DataSource.BIBLIOTHECA,
            primary_identifier=primary_identifier,
            formats=formats,
            links=links,
        )

        metadata.circulation = circulationdata
        return metadata

    @classmethod
    def internal_formats(cls, book_format):
        """Convert the term Bibliotheca uses to refer to a book
        format into a (medium [formats]) 2-tuple.
        """
        medium = Edition.BOOK_MEDIUM
        format = None
        if book_format not in cls.format_data_for_bibliotheca_format:
            logging.error("Unrecognized BookFormat: %s", book_format)
            return medium, []

        content_type, drm_scheme = cls.format_data_for_bibliotheca_format[
            book_format
        ]

        format = FormatData(content_type=content_type, drm_scheme=drm_scheme)
        if book_format == 'MP3':
            medium = Edition.AUDIO_MEDIUM
        else:
            medium = Edition.BOOK_MEDIUM
        return medium, [format]


class BibliothecaBibliographicCoverageProvider(BibliographicCoverageProvider):
    """Fill in bibliographic metadata for Bibliotheca records.

    This will occasionally fill in some availability information for a
    single Collection, but we rely on Monitors to keep availability
    information up to date for all Collections.
    """
    SERVICE_NAME = "Bibliotheca Bibliographic Coverage Provider"
    DATA_SOURCE_NAME = DataSource.BIBLIOTHECA
    PROTOCOL = ExternalIntegration.BIBLIOTHECA
    INPUT_IDENTIFIER_TYPES = Identifier.BIBLIOTHECA_ID

    # 25 is the maximum batch size for the Bibliotheca API.
    DEFAULT_BATCH_SIZE = 25

    def __init__(self, collection, api_class=BibliothecaAPI, **kwargs):
        """Constructor.

        :param collection: Provide bibliographic coverage to all
            Bibliotheca books in the given Collection.
        :param api_class: Instantiate this class with the given Collection,
            rather than instantiating BibliothecaAPI.
        :param input_identifiers: Passed in by RunCoverageProviderScript.
            A list of specific identifiers to get coverage for.
        """
        super(BibliothecaBibliographicCoverageProvider, self).__init__(
            collection, **kwargs
        )
        if isinstance(api_class, BibliothecaAPI):
            # This is an already instantiated API object. Use it
            # instead of creating a new one.
            self.api = api_class
        else:
            # A web application should not use this option because it
            # will put a non-scoped session in the mix.
            _db = Session.object_session(collection)
            self.api = api_class(_db, collection)

    def process_item(self, identifier):
        # We don't accept a representation from the cache because
        # either this is being run for the first time (in which case
        # there is nothing in the cache) or it's being run to correct
        # for an earlier failure (in which case the representation
        # in the cache might be wrong).
        metadata = self.api.bibliographic_lookup(identifier, max_age=0)
        if not metadata:
            return self.failure(
                identifier, "Bibliotheca bibliographic lookup failed."
            )
        return self.set_metadata(identifier, metadata)
