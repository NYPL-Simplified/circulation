from pdb import set_trace
import base64
import urlparse
import time
import hmac
import hashlib
import os
import re
import requests
import logging
from datetime import datetime, timedelta

from config import (
    Configuration,
    CannotLoadConfiguration,
)
from model import (
    Contributor,
    DataSource,
    DeliveryMechanism,
    Representation,
    Hyperlink,
    Identifier,
    Measurement,
    Edition,
    Subject,
)

from metadata_layer import (
    ContributorData,
    Metadata,
    LinkData,
    IdentifierData,
    FormatData,
    MeasurementData,
    SubjectData,
)

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

    log = logging.getLogger("3M API")

    def __init__(self, _db, account_id=None, library_id=None, account_key=None,
                 base_url = "https://cloudlibraryapi.3m.com/",
                 version="2.0", testing=False):
        self._db = _db
        self.version = version
        self.base_url = base_url
        self.source = DataSource.lookup(self._db, DataSource.THREEM)
        self.item_list_parser = ItemListParser()

        if testing:
            return

        if not account_id or not library_id or not account_key:
            values = self.environment_values()
            if len([x for x in values if not x]):
                raise CannotLoadConfiguration(
                    "3M integration has incomplete configuration.")

        (env_library_id, env_account_id, 
         env_account_key) = values
        self.library_id = library_id or env_library_id
        self.account_id = account_id or env_account_id
        self.account_key = account_key or env_account_key

    @classmethod
    def environment_values(
            self, client_key=None, client_secret=None,
            website_id=None, library_id=None, collection_name=None):
        value = Configuration.integration('3M')
        values = []
        for name in [
                'library_id',
                'account_id',
                'account_key',
            ]:
            var = value.get(name)
            if var:
                var = var.encode("utf8")
            values.append(var)
        return values

    @classmethod
    def from_environment(cls, _db):
        # Make sure all environment values are present. If any are missing,
        # return None
        values = cls.environment_values()
        if len([x for x in values if not x]):
            cls.log.info(
                "No 3M client configured."
            )
            return None
        return cls(_db)

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
        signature = base64.b64encode(digest)
        return signature, now

    def request(self, path, body=None, method="GET", identifier=None,
                max_age=None):
        if not path.startswith("/"):
            path = "/" + path
        if not path.startswith("/cirrus"):
            path = "/cirrus/library/%s%s" % (self.library_id, path)
        url = urlparse.urljoin(self.base_url, path)
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
                do_get=Representation.http_get_no_timeout, max_age=max_age)
            content = representation.content
            return content
        else:
            response = requests.request(
                method, url, data=body, headers=headers, allow_redirects=False)
            return response
        
    def get_bibliographic_info_for(self, editions, max_age=None):
        results = dict()
        for edition in editions:
            identifier = edition.primary_identifier
            metadata = self.bibliographic_lookup(identifier, max_age)
            if metadata:
                results[identifier] = (edition, metadata)
        return results

    def bibliographic_lookup(self, identifier, max_age=None):
        data = self.request(
            "/items/%s" % identifier.identifier,
            max_age=max_age or self.MAX_METADATA_AGE)
        response = list(self.item_list_parser.parse(data))
        if not response:
            return None
        else:
            [metadata] = response
        return metadata

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
            genres.append(SubjectData(Subject.THREEM, i))
        return genres

    def process_one(self, tag, namespaces):
        """Turn an <item> tag into a Metadata object."""

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

        return Metadata(
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
            formats=formats,
            measurements=measurements,
            links=links,
        )



