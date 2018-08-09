from nose.tools import set_trace
from collections import defaultdict
import datetime
import base64
import os
import json
import logging
import re
from sqlalchemy.orm.session import Session

from config import (
    Configuration,
    temp_config,
)

from util import LanguageCodes
from util.xmlparser import XMLParser
from util.http import (
    HTTP,
    RemoteIntegrationException,
)
from coverage import CoverageFailure
from model import (
    get_one,
    get_one_or_create,
    Collection,
    Contributor,
    DataSource,
    DeliveryMechanism,
    ExternalIntegration,
    Hyperlink,
    LicensePool,
    Edition,
    Identifier,
    Library,
    Representation,
    Subject,
)

from metadata_layer import (
    SubjectData,
    ContributorData,
    FormatData,
    IdentifierData,
    CirculationData,
    LinkData,
    Metadata,
)

from config import (
    CannotLoadConfiguration,
    Configuration,
)
from coverage import BibliographicCoverageProvider

from testing import DatabaseTest


class Axis360API(object):

    PRODUCTION_BASE_URL = "https://axis360api.baker-taylor.com/Services/VendorAPI/"
    QA_BASE_URL = "http://axis360apiqa.baker-taylor.com/Services/VendorAPI/"

    # Map simple nicknames to server URLs.
    SERVER_NICKNAMES = {
        "production" : PRODUCTION_BASE_URL,
        "qa" : QA_BASE_URL,
    }

    DATE_FORMAT = "%m-%d-%Y %H:%M:%S"

    access_token_endpoint = 'accesstoken'
    availability_endpoint = 'availability/v2'

    log = logging.getLogger("Axis 360 API")

    def __init__(self, _db, collection):
        if collection.protocol != ExternalIntegration.AXIS_360:
            raise ValueError(
                "Collection protocol is %s, but passed into Axis360API!" %
                collection.protocol
            )
        self._db = _db
        self.library_id = collection.external_account_id
        self.username = collection.external_integration.username
        self.password = collection.external_integration.password

        # Convert the nickname for a server into an actual URL.
        base_url = collection.external_integration.url or self.PRODUCTION_BASE_URL
        if base_url in self.SERVER_NICKNAMES:
            base_url = self.SERVER_NICKNAMES[base_url]
        self.base_url = base_url

        if (not self.library_id or not self.username
            or not self.password):
            raise CannotLoadConfiguration(
                "Axis 360 configuration is incomplete."
            )

        # Use utf8 instead of unicode encoding
        settings = [self.library_id, self.username, self.password]
        self.library_id, self.username, self.password = (
            setting.encode('utf8') for setting in settings
        )

        self.token = None
        self.collection_id = collection.id

    @property
    def collection(self):
        return Collection.by_id(self._db, id=self.collection_id)

    @property
    def source(self):
        return DataSource.lookup(self._db, DataSource.AXIS_360)

    @property
    def authorization_headers(self):
        authorization = u":".join([self.username, self.password, self.library_id])
        authorization = authorization.encode("utf_16_le")
        authorization = base64.standard_b64encode(authorization)
        return dict(Authorization="Basic " + authorization)

    def refresh_bearer_token(self):
        url = self.base_url + self.access_token_endpoint
        headers = self.authorization_headers
        response = self._make_request(
            url, 'post', headers, allowed_response_codes=[200]
        )
        return self.parse_token(response.content)

    def request(self, url, method='get', extra_headers={}, data=None,
                params=None, exception_on_401=False):
        """Make an HTTP request, acquiring/refreshing a bearer token
        if necessary.
        """
        if not self.token:
            self.token = self.refresh_bearer_token()

        headers = dict(extra_headers)
        headers['Authorization'] = "Bearer " + self.token
        headers['Library'] = self.library_id
        if exception_on_401:
            disallowed_response_codes = ["401"]
        else:
            disallowed_response_codes = None
        response = self._make_request(
            url=url, method=method, headers=headers,
            data=data, params=params,
            disallowed_response_codes=disallowed_response_codes
        )
        if response.status_code == 401:
            # This must be our first 401, since our second 401 will
            # make _make_request raise a RemoteIntegrationException.
            #
            # The token has expired. Get a new token and try again.
            self.token = None
            return self.request(
                url=url, method=method, extra_headers=extra_headers,
                data=data, params=params, exception_on_401=True
            )
        else:
            return response

    def availability(self, patron_id=None, since=None, title_ids=[]):
        url = self.base_url + self.availability_endpoint
        args = dict()
        if since:
            since = since.strftime(self.DATE_FORMAT)
            args['updatedDate'] = since
        if patron_id:
            args['patronId'] = patron_id
        if title_ids:
            args['titleIds'] = ','.join(title_ids)
        response = self.request(url, params=args)
        return response

    @classmethod
    def create_identifier_strings(cls, identifiers):
        identifier_strings = []
        for i in identifiers:
            if isinstance(i, Identifier):
                value = i.identifier
            else:
                value = i
            identifier_strings.append(value)

        return identifier_strings

    @classmethod
    def parse_token(cls, token):
        data = json.loads(token)
        return data['access_token']

    def _make_request(self, url, method, headers, data=None, params=None,
                      **kwargs):
        """Actually make an HTTP request."""
        return HTTP.request_with_timeout(
            method, url, headers=headers, data=data,
            params=params, **kwargs
        )


class MockAxis360API(Axis360API):

    @classmethod
    def mock_collection(self, _db):
        """Create a mock Axis 360 collection for use in tests."""
        library = DatabaseTest.make_default_library(_db)
        collection, ignore = get_one_or_create(
            _db, Collection,
            name="Test Axis 360 Collection",
            create_method_kwargs=dict(
                external_account_id=u'c',
            )
        )
        integration = collection.create_external_integration(
            protocol=ExternalIntegration.AXIS_360
        )
        integration.username = u'a'
        integration.password = u'b'
        integration.url = u"http://axis.test/"
        library.collections.append(collection)
        return collection

    def __init__(self, _db, collection, with_token=True, **kwargs):
        """Constructor.

        :param collection: Get Axis 360 credentials from this
            Collection.

        :param with_token: If True, this class will assume that
            it already has a valid token, and will not go through
            the motions of negotiating one with the mock server.
        """
        super(MockAxis360API, self).__init__(_db, collection, **kwargs)
        if with_token:
            self.token = "mock token"
        self.responses = []
        self.requests = []

    def queue_response(self, status_code, headers={}, content=None):
        from testing import MockRequestsResponse
        self.responses.insert(
            0, MockRequestsResponse(status_code, headers, content)
        )

    def _make_request(self, url, *args, **kwargs):
        self.requests.append([url, args, kwargs])
        response = self.responses.pop()
        return HTTP._process_response(
            url, response, kwargs.get('allowed_response_codes'),
            kwargs.get('disallowed_response_codes')
        )


class Axis360BibliographicCoverageProvider(BibliographicCoverageProvider):
    """Fill in bibliographic metadata for Axis 360 records.

    Currently this is only used by BibliographicRefreshScript. It's
    not normally necessary because the Axis 360 API combines
    bibliographic and availability data. We rely on Monitors to fetch
    availability data and fill in the bibliographic data as necessary.
    """

    SERVICE_NAME = "Axis 360 Bibliographic Coverage Provider"
    DATA_SOURCE_NAME = DataSource.AXIS_360
    PROTOCOL = ExternalIntegration.AXIS_360
    INPUT_IDENTIFIER_TYPES = Identifier.AXIS_360_ID
    DEFAULT_BATCH_SIZE = 25

    def __init__(self, collection, api_class=Axis360API, **kwargs):
        """Constructor.

        :param collection: Provide bibliographic coverage to all
            Axis 360 books in the given Collection.
        :param api_class: Instantiate this class with the given Collection,
            rather than instantiating Axis360API.
        """
        super(Axis360BibliographicCoverageProvider, self).__init__(
            collection, **kwargs
        )
        if isinstance(api_class, Axis360API):
            # We were given a specific Axis360API instance to use.
            self.api = api_class
        else:
            # A web application should not use this option because it
            # will put a non-scoped session in the mix.
            _db = Session.object_session(collection)
            self.api = api_class(_db, collection)
        self.parser = BibliographicParser()

    def process_batch(self, identifiers):
        identifier_strings = self.api.create_identifier_strings(identifiers)
        response = self.api.availability(title_ids=identifier_strings)
        seen_identifiers = set()
        batch_results = []
        for metadata, availability in self.parser.process_all(response.content):
            identifier, is_new = metadata.primary_identifier.load(self._db)
            if not identifier in identifiers:
                # Axis 360 told us about a book we didn't ask
                # for. This shouldn't happen, but if it does we should
                # do nothing further.
                continue
            seen_identifiers.add(identifier.identifier)
            result = self.set_metadata(identifier, metadata)
            if not isinstance(result, CoverageFailure):
                result = self.handle_success(identifier)
            batch_results.append(result)

        # Create a CoverageFailure object for each original identifier
        # not mentioned in the results.
        for identifier_string in identifier_strings:
            if identifier_string not in seen_identifiers:
                identifier, ignore = Identifier.for_foreign_id(
                    self._db, Identifier.AXIS_360_ID, identifier_string
                )
                result = self.failure(
                    identifier, "Book not in collection", transient=False
                )
                batch_results.append(result)
        return batch_results

    def handle_success(self, identifier):
        return self.set_presentation_ready(identifier)

    def process_item(self, identifier):
        results = self.process_batch([identifier])
        return results[0]


class Axis360Parser(XMLParser):

    NS = {"axis": "http://axis360api.baker-taylor.com/vendorAPI"}

    SHORT_DATE_FORMAT = "%m/%d/%Y"
    FULL_DATE_FORMAT_IMPLICIT_UTC = "%m/%d/%Y %I:%M:%S %p"
    FULL_DATE_FORMAT = "%m/%d/%Y %I:%M:%S %p +00:00"

    def _xpath1_boolean(self, e, target, ns, default=False):
        text = self.text_of_optional_subtag(e, target, ns)
        if text is None:
            return default
        if text == 'true':
            return True
        else:
            return False

    def _xpath1_date(self, e, target, ns):
        value = self.text_of_optional_subtag(e, target, ns)
        if value is None:
            return value
        try:
            attempt = datetime.datetime.strptime(
                value, self.FULL_DATE_FORMAT_IMPLICIT_UTC)
            value += ' +00:00'
        except ValueError:
            pass
        return datetime.datetime.strptime(value, self.FULL_DATE_FORMAT)


class BibliographicParser(Axis360Parser):

    DELIVERY_DATA_FOR_AXIS_FORMAT = {
        "Blio" : None,
        "Acoustik" : None,
        "AxisNow": None,
        "ePub" : (Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM),
        "PDF" : (Representation.PDF_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM),
    }

    log = logging.getLogger("Axis 360 Bibliographic Parser")

    @classmethod
    def parse_list(self, l):
        """Turn strings like this into lists:

        FICTION / Thrillers; FICTION / Suspense; FICTION / General
        Ursu, Anne ; Fortune, Eric (ILT)
        """
        return [x.strip() for x in l.split(";")]

    def __init__(self, include_availability=True, include_bibliographic=True):
        self.include_availability = include_availability
        self.include_bibliographic = include_bibliographic

    def process_all(self, string):
        for i in super(BibliographicParser, self).process_all(
                string, "//axis:title", self.NS):
            yield i

    def extract_availability(self, circulation_data, element, ns):
        identifier = self.text_of_subtag(element, 'axis:titleId', ns)
        primary_identifier = IdentifierData(Identifier.AXIS_360_ID, identifier)

        if not circulation_data:
            circulation_data = CirculationData(
                data_source=DataSource.AXIS_360,
                primary_identifier=primary_identifier,
            )

        availability = self._xpath1(element, 'axis:availability', ns)
        total_copies = self.int_of_subtag(availability, 'axis:totalCopies', ns)
        available_copies = self.int_of_subtag(
            availability, 'axis:availableCopies', ns)
        size_of_hold_queue = self.int_of_subtag(
            availability, 'axis:holdsQueueSize', ns)

        availability_updated = self.text_of_optional_subtag(
            availability, 'axis:updateDate', ns)
        if availability_updated:
            try:
                attempt = datetime.datetime.strptime(
                    availability_updated, self.FULL_DATE_FORMAT_IMPLICIT_UTC)
                availability_updated += ' +00:00'
            except ValueError:
                pass
            availability_updated = datetime.datetime.strptime(
                    availability_updated, self.FULL_DATE_FORMAT)

        circulation_data.licenses_owned=total_copies
        circulation_data.licenses_available=available_copies
        circulation_data.licenses_reserved=0
        circulation_data.patrons_in_hold_queue=size_of_hold_queue

        return circulation_data


    # Axis authors with a special role have an abbreviation after their names,
    # e.g. "San Ruby (FRW)"
    role_abbreviation = re.compile("\(([A-Z][A-Z][A-Z])\)$")
    generic_author = object()
    role_abbreviation_to_role = dict(
        INT=Contributor.INTRODUCTION_ROLE,
        EDT=Contributor.EDITOR_ROLE,
        PHT=Contributor.PHOTOGRAPHER_ROLE,
        ILT=Contributor.ILLUSTRATOR_ROLE,
        TRN=Contributor.TRANSLATOR_ROLE,
        FRW=Contributor.FOREWORD_ROLE,
        ADP=generic_author, # Author of adaptation
        COR=generic_author, # Corporate author
    )

    @classmethod
    def parse_contributor(cls, author, primary_author_found=False,
                          force_role=None):
        """Parse an Axis 360 contributor string.

        The contributor string looks like "Butler, Octavia" or "Walt
        Disney Pictures (COR)" or "Rex, Adam (ILT)". The optional
        three-letter code describes the contributor's role in the
        book.

        :param author: The string to parse.

        :param primary_author_found: If this is false, then a
            contributor with no three-letter code will be treated as
            the primary author. If this is true, then a contributor
            with no three-letter code will be treated as just a
            regular author.

        :param force_role: If this is set, the contributor will be
            assigned this role, no matter what. This takes precedence
            over the value implied by primary_author_found.
        """
        if primary_author_found:
            default_author_role = Contributor.AUTHOR_ROLE
        else:
            default_author_role = Contributor.PRIMARY_AUTHOR_ROLE
        role = default_author_role
        match = cls.role_abbreviation.search(author)
        if match:
            role_type = match.groups()[0]
            role = cls.role_abbreviation_to_role.get(
                role_type, Contributor.UNKNOWN_ROLE)
            if role is cls.generic_author:
                role = default_author_role
            author = author[:-5].strip()
        if force_role:
            role = force_role
        return ContributorData(
            sort_name=author, roles=[role]
        )

    def extract_bibliographic(self, element, ns):
        """Turn bibliographic metadata into a Metadata and a CirculationData objects,
        and return them as a tuple."""

        # TODO: These are consistently empty (some are clearly for
        # audiobooks) so I don't know what they do and/or what format
        # they're in.
        #
        # edition
        # runtime

        identifier = self.text_of_subtag(element, 'axis:titleId', ns)
        isbn = self.text_of_optional_subtag(element, 'axis:isbn', ns)
        title = self.text_of_subtag(element, 'axis:productTitle', ns)

        contributor = self.text_of_optional_subtag(
            element, 'axis:contributor', ns)
        contributors = []
        found_primary_author = False
        if contributor:
            for c in self.parse_list(contributor):
                contributor = self.parse_contributor(
                    c, found_primary_author)
                if Contributor.PRIMARY_AUTHOR_ROLE in contributor.roles:
                    found_primary_author = True
                contributors.append(contributor)

        narrator = self.text_of_optional_subtag(
            element, 'axis:narrator', ns
        )
        if narrator:
            for n in self.parse_list(narrator):
                contributor = self.parse_contributor(
                    n, force_role=Contributor.NARRATOR_ROLE
                )
                contributors.append(contributor)

        links = []
        description = self.text_of_optional_subtag(
            element, 'axis:annotation', ns
        )
        if description:
            links.append(
                LinkData(
                    rel=Hyperlink.DESCRIPTION,
                    content=description,
                    media_type=Representation.TEXT_PLAIN,
                )
            )

        subject = self.text_of_optional_subtag(element, 'axis:subject', ns)
        subjects = []
        if subject:
            for subject_identifier in self.parse_list(subject):
                subjects.append(
                    SubjectData(
                        type=Subject.BISAC, identifier=None,
                        name=subject_identifier, weight=1
                    )
                )

        publication_date = self.text_of_optional_subtag(
            element, 'axis:publicationDate', ns)
        if publication_date:
            publication_date = datetime.datetime.strptime(
                publication_date, self.SHORT_DATE_FORMAT)

        series = self.text_of_optional_subtag(element, 'axis:series', ns)
        publisher = self.text_of_optional_subtag(element, 'axis:publisher', ns)
        imprint = self.text_of_optional_subtag(element, 'axis:imprint', ns)

        audience = self.text_of_optional_subtag(element, 'axis:audience', ns)
        if audience:
            subjects.append(
                SubjectData(
                    type=Subject.AXIS_360_AUDIENCE,
                    identifier=audience,
                    weight=1,
                )
            )

        language = self.text_of_subtag(element, 'axis:language', ns)

        # We don't use this for anything.
        # file_size = self.int_of_optional_subtag(element, 'axis:fileSize', ns)
        primary_identifier = IdentifierData(Identifier.AXIS_360_ID, identifier)
        identifiers = []
        if isbn:
            identifiers.append(IdentifierData(Identifier.ISBN, isbn))

        formats = []
        acceptable = False
        seen_formats = []
        for format_tag in self._xpath(
                element, 'axis:availability/axis:availableFormats/axis:formatName',
                ns
        ):
            informal_name = format_tag.text
            seen_formats.append(informal_name)
            if informal_name not in self.DELIVERY_DATA_FOR_AXIS_FORMAT:
                self.log.warn("Unrecognized Axis format name for %s: %s" % (
                    identifier, informal_name
                ))
            elif self.DELIVERY_DATA_FOR_AXIS_FORMAT.get(informal_name):
                content_type, drm_scheme = self.DELIVERY_DATA_FOR_AXIS_FORMAT[
                    informal_name
                ]
                formats.append(
                    FormatData(content_type=content_type, drm_scheme=drm_scheme)
                )

        if not formats:
            self.log.error(
                "No supported format for %s (%s)! Saw: %s", identifier,
                title, ", ".join(seen_formats)
            )

        metadata = Metadata(
            data_source=DataSource.AXIS_360,
            title=title,
            language=language,
            medium=Edition.BOOK_MEDIUM,
            series=series,
            publisher=publisher,
            imprint=imprint,
            published=publication_date,
            primary_identifier=primary_identifier,
            identifiers=identifiers,
            subjects=subjects,
            contributors=contributors,
            links=links,
        )

        circulationdata = CirculationData(
            data_source=DataSource.AXIS_360,
            primary_identifier=primary_identifier,
            formats=formats,
        )

        metadata.circulation = circulationdata
        return metadata


    def process_one(self, element, ns):
        if self.include_bibliographic:
            bibliographic = self.extract_bibliographic(element, ns)
        else:
            bibliographic = None

        passed_availability = None
        if bibliographic and bibliographic.circulation:
            passed_availability = bibliographic.circulation

        if self.include_availability:
            availability = self.extract_availability(circulation_data=passed_availability, element=element, ns=ns)
        else:
            availability = None

        return bibliographic, availability
