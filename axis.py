from nose.tools import set_trace
from collections import defaultdict
import datetime
import base64
import requests
import os
import json
import logging
import re

from util import LanguageCodes
from util.xmlparser import XMLParser
from coverage import CoverageFailure
from model import (
    Contributor,
    DataSource,
    DeliveryMechanism,
    LicensePool,
    Edition,
    Identifier,
    Representation,
    Subject,
)

from metadata_layer import (
    SubjectData,
    ContributorData,
    FormatData,
    IdentifierData,
    CirculationData,
    Metadata,
)

from config import Configuration
from coverage import BibliographicCoverageProvider

class Axis360API(object):

    PRODUCTION_BASE_URL = "https://axis360api.baker-taylor.com/Services/VendorAPI/"
    QA_BASE_URL = "http://axis360apiqa.baker-taylor.com/Services/VendorAPI/"
    
    DATE_FORMAT = "%m-%d-%Y %H:%M:%S"

    access_token_endpoint = 'accesstoken'
    availability_endpoint = 'availability/v2'

    log = logging.getLogger("Axis 360 API")

    def __init__(self, _db, username=None, library_id=None, password=None,
                 base_url=None):
        self._db = _db
        (env_library_id, env_username, 
         env_password, env_base_url) = self.environment_values()
            
        self.library_id = library_id or env_library_id
        self.username = username or env_username
        self.password = password or env_password
        self.base_url = base_url or env_base_url
        if self.base_url == 'qa':
            self.base_url = self.QA_BASE_URL
        elif self.base_url == 'production':
            self.base_url = self.PRODUCTION_BASE_URL
        print self.base_url
        self.token = None
        self.source = DataSource.lookup(self._db, DataSource.AXIS_360)

    @classmethod
    def environment_values(cls):
        value = Configuration.integration('Axis 360')
        return [
            value[var] for var in [
                'library_id',
                'username',
                'password',
                'server',
            ]
        ]

    @classmethod
    def from_environment(cls, _db):
        # Make sure all environment values are present. If any are missing,
        # return None
        values = cls.environment_values()
        if len([x for x in values if not x]):
            cls.log.info(
                "No Axis 360 client configured."
            )
            return None
        return cls(_db)

    @property
    def authorization_headers(self):
        authorization = u":".join([self.username, self.password, self.library_id])
        authorization = authorization.encode("utf_16_le")
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
                params=None, exception_on_401=False):
        """Make an HTTP request, acquiring/refreshing a bearer token
        if necessary.
        """
        if not self.token:
            self.token = self.refresh_bearer_token()

        headers = dict(extra_headers)
        headers['Authorization'] = "Bearer " + self.token
        headers['Library'] = self.library_id
        response = self._make_request(
            url=url, method=method, headers=headers,
            data=data, params=params)
        if response.status_code == 401:
            if exception_on_401:
                # This is our second try. Give up.
                raise Exception(
                    "Something's wrong with the OAuth Bearer Token!")
            else:
                # The token has expired. Get a new token and try again.
                self.token = None
                return self.request(
                    url=url, method=method, extra_headers=extra_headers,
                    data=data, params=params, exception_on_401=True)
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

    def _make_request(self, url, method, headers, data=None, params=None):
        """Actually make an HTTP request."""
        # self.log.debug("Making Axis 360 request to %s params=%r", url, params)
        return requests.request(
            url=url, method=method, headers=headers, data=data,
            params=params)

class Axis360BibliographicCoverageProvider(BibliographicCoverageProvider):
    """Fill in bibliographic metadata for Axis 360 records.

    Currently this is only used by BibliographicRefreshScript. It's
    not normally necessary because the Axis 360 API combines
    bibliographic and availability data.
    """
    def __init__(self, _db):
        self.parser = BibliographicParser()
        super(Axis360BibliographicCoverageProvider, self).__init__(
            _db, Axis360API(_db), DataSource.AXIS_360,
            workset_size=25
        )

    def process_batch(self, identifiers):
        identifier_strings = self.api.create_identifier_strings(identifiers)
        response = self.api.availability(title_ids=identifier_strings)
        seen_identifiers = set()
        batch_results = []
        for metadata, availability in self.parser.process_all(response.content):
            identifier, is_new = metadata.primary_identifier.load(self._db)
            seen_identifiers.add(identifier.identifier)
            result = self.set_metadata(identifier, metadata)
            if not isinstance(result, CoverageFailure):
                result = self.set_presentation_ready(identifier)
            batch_results.append(result)

        # Create a CoverageFailure object for each original identifier
        # not mentioned in the results.
        for identifier_string in identifier_strings:
            if identifier_string not in seen_identifiers:
                identifier, ignore = Identifier.for_foreign_id(
                    self._db, Identifier.AXIS_360_ID, identifier_string
                )
                result = CoverageFailure(
                    self, identifier, "Book not in collection", transient=True
                )
                batch_results.append(result)
        return batch_results

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

    def extract_availability(self, element, ns):
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

        return CirculationData(
            licenses_owned=total_copies,
            licenses_available=available_copies,
            licenses_reserved=0,
            patrons_in_hold_queue=size_of_hold_queue,
            last_checked=availability_updated,
        )

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
    def parse_contributor(cls, author, primary_author_found=False):
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
        return ContributorData(
            sort_name=author, roles=role)

    def extract_bibliographic(self, element, ns):
        """Turn bibliographic metadata into a Metadata object."""

        # TODO: These are consistently empty (some are clearly for
        # audiobooks) so I don't know what they do and/or what format
        # they're in.
        #
        # annotation
        # edition
        # narrator
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

        subject = self.text_of_optional_subtag(element, 'axis:subject', ns)
        subjects = []
        if subject:
            for subject_identifier in self.parse_list(subject):
                subjects.append(
                    SubjectData(
                        type=Subject.BISAC, identifier=subject_identifier,
                        weight=1
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
                self.log("Unrecognized Axis format name for %s: %s" % (
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

        data = Metadata(
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
            formats=formats,
        )
        return data

    def process_one(self, element, ns):
        if self.include_bibliographic:
            bibliographic = self.extract_bibliographic(element, ns)
        else:
            bibliographic = None
        if self.include_availability:
            availability = self.extract_availability(element, ns)
        else:
            availability = None
        return bibliographic, availability
