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

from config import (
    Configuration,
    CannotLoadConfiguration,
    temp_config,
)
from coverage import (
    BibliographicCoverageProvider,
    CoverageFailure,
)
from model import (
    get_one_or_create,
    Collection,
    Contributor,
    DataSource,
    DeliveryMechanism,
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

from util.http import HTTP
from util.xmlparser import XMLParser

class ThreeMAPI(object):

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
        if collection.protocol != collection.BIBLIOTHECA:
            raise ValueError(
                "Collection protocol is %s, but passed into BibliothecaAPI!" %
                collection.protocol
            )

        self._db = _db
        self.version = (
            collection.setting('version').value or self.DEFAULT_VERSION
        )
        self.account_id = collection.username.encode("utf8")
        self.account_key = collection.password.encode("utf8")
        self.library_id = collection.external_account_id.encode("utf8")
        self.base_url = collection.url or self.DEFAULT_BASE_URL
        
        if not self.account_id or not self.account_key or not self.library_id:
            raise CannotLoadConfiguration(
                "Bibliotheca configuration is incomplete."
            )

        self.item_list_parser = ItemListParser()

    @classmethod
    def from_environment(cls, _db):
        """Load a ThreeMAPI instance for the 'default' Bibliotheca
        collection.
        """
        library = Library.instance(_db)
        collections = [x for x in library.collections
                      if x.protocol == Collection.BIBLIOTHECA]
        if len(collections) == 0:
            # There are no Bibliotheca collections configured.
            return None

        if len(collections) > 1:
            raise ValueError(
                "Multiple Bibliotheca collections found for one library. This is not yet supported."
            )
        [collection] = collections

        try:
            return cls(_db, collection)
        except CannotLoadConfiguration, e:
            return None

    @property
    def source(self):
        return DataSource.lookup(self._db, DataSource.THREEM)

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
        """This will be overridden in MockThreeMAPI."""
        return HTTP.request_with_timeout(method, url, *args, **kwargs)

    def _simple_http_get(self, url, headers, *args, **kwargs):
        """This will be overridden in MockThreeMAPI."""
        return Representation.simple_http_get(url, headers, *args, **kwargs)


class MockThreeMAPI(ThreeMAPI):

    def __init__(self, _db, *args, **kwargs):
        self.responses = []
        self.requests = []

        library = Library.instance(_db)
        collection, ignore = get_one_or_create(
            _db, Collection,
            name="Test Bibliotheca Collection",
            protocol=Collection.BIBLIOTHECA, create_method_kwargs=dict(
                username=u'a', password=u'b', external_account_id=u'c',
                url="http://bibliotheca.test"
            )
        )
        library.collections.append(collection)
        super(MockThreeMAPI, self).__init__(
            _db, collection, *args, **kwargs
        )

    def now(self):
        """Return an unvarying time in the format 3M expects."""
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

    @classmethod
    def contributors_from_string(cls, string):
        contributors = []
        if not string:
            return contributors
        
        for sort_name in string.split(';'):
            sort_name = cls.parenthetical.sub("", sort_name.strip())
            contributors.append(
                ContributorData(
                    sort_name=sort_name.strip(),
                    roles=[Contributor.AUTHOR_ROLE]
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
            genres.append(SubjectData(Subject.THREEM, i, weight=15))
        return genres


    def process_one(self, tag, namespaces):
        """Turn an <item> tag into a Metadata and an encompassed CirculationData 
        objects, and return the Metadata."""

        def value(threem_key):
            return self.text_of_optional_subtag(tag, threem_key)

        links = dict()
        identifiers = dict()
        subjects = []

        primary_identifier = IdentifierData(
            Identifier.THREEM_ID, value("ItemId")
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

        contributors = list(self.contributors_from_string(value('Authors')))

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

        medium = Edition.BOOK_MEDIUM

        book_format = value("BookFormat")
        format = None
        if book_format == 'EPUB':
            format = FormatData(
                content_type=Representation.EPUB_MEDIA_TYPE,
                drm_scheme=DeliveryMechanism.ADOBE_DRM
            )
        elif book_format == 'PDF':
            format = FormatData(
                content_type=Representation.PDF_MEDIA_TYPE,
                drm_scheme=DeliveryMechanism.ADOBE_DRM
            )
        elif book_format == 'MP3':
            format = FormatData(
                content_type=Representation.MP3_MEDIA_TYPE,
                drm_scheme=DeliveryMechanism.ADOBE_DRM
            )
            medium = Edition.AUDIO_MEDIUM

        formats = [format]

        metadata = Metadata(
            data_source=DataSource.THREEM,
            title=title,
            subtitle=subtitle,
            language=language,
            medium=medium,
            publisher=publisher,
            published=published_date,
            primary_identifier=primary_identifier,
            identifiers=identifiers,
            subjects=subjects,
            contributors=contributors,
            measurements=measurements,
            links=links,
        )

        # Also make a CirculationData so we can write the formats, 
        circulationdata = CirculationData(
            data_source=DataSource.THREEM,
            primary_identifier=primary_identifier,
            formats=formats,
            links=links,
        )

        metadata.circulation = circulationdata
        return metadata


class ThreeMBibliographicCoverageProvider(BibliographicCoverageProvider):
    """Fill in bibliographic metadata for 3M records.

    Then mark the works as presentation-ready.
    """

    def __init__(self, _db, metadata_replacement_policy=None, threem_api=None,
                 input_identifier_types=None, input_identifiers=None, **kwargs
    ):
        """
        :param input_identifier_types: Passed in by RunCoverageProviderScript, data sources to get coverage for.
        :param input_identifiers: Passed in by RunCoverageProviderScript, specific identifiers to get coverage for.
        """
        threem_api = threem_api or ThreeMAPI(_db)
        super(ThreeMBibliographicCoverageProvider, self).__init__(
            _db, threem_api, DataSource.THREEM,
            batch_size=25, metadata_replacement_policy=metadata_replacement_policy, **kwargs
        )

    def process_item(self, identifier):
        # We don't accept a representation from the cache because
        # either this is being run for the first time (in which case
        # there is nothing in the cache) or it's being run to correct
        # for an earlier failure (in which case the representation
        # in the cache might be wrong).
        metadata = self.api.bibliographic_lookup(identifier, max_age=0)
        if not metadata:
            return CoverageFailure(
                identifier, "3M bibliographic lookup failed.",
                data_source=self.output_source, transient=True
            )
        return self.set_metadata(identifier, metadata)
