from nose.tools import set_trace
from collections import defaultdict
import datetime
import base64
import os
import json
import logging
import re

from sqlalchemy.orm import contains_eager

from lxml import etree

from authenticator import Authenticator

from config import (
    Configuration,
    temp_config,
)

from circulation import (
    LoanInfo,
    FulfillmentInfo,
    HoldInfo,
    BaseCirculationAPI
)

from circulation_exceptions import *

from core.util import LanguageCodes
from core.util.http import (
    HTTP,
    RemoteIntegrationException,
)

from core.coverage import (
    BibliographicCoverageProvider,
    CoverageFailure,
)

from core.model import (
    get_one_or_create,
    Collection,
    Contributor,
    DataSource,
    DeliveryMechanism,
    LicensePool,
    Edition,
    Identifier,
    Library,
    Representation,
    Subject,
)

from core.metadata_layer import (
    SubjectData,
    ContributorData,
    FormatData,
    IdentifierData,
    CirculationData,
    Metadata,
    ReplacementPolicy,
)

from core.monitor import (
    Monitor,
    IdentifierSweepMonitor,
)

from core.opds_import import SimplifiedOPDSLookup

#TODO: Remove unnecessary imports (once the classes are more or less complete)

class EnkiAPI(object):
    PRODUCTION_BASE_URL = "http://enkilibrary.org/API/"
    availability_endpoint = "ListAPI"
    item_endpoint = "ItemAPI"
    lib = 1

    SET_DELIVERY_MECHANISM_AT = BaseCirculationAPI.BORROW_STEP
    SERVICE_NAME = "Enki"
    log = logging.getLogger("Enki API")
    log.setLevel(logging.DEBUG)

    def __init__(self, _db, username=None, library_id=None, password=None,
                 base_url=None):
        self._db = _db
        (env_library_id, env_username,
         env_password, env_base_url) = self.environment_values()
        self.library_id = library_id or env_library_id
        self.username = username or env_username
        self.password = password or env_password
        self.base_url = base_url or env_base_url
        if self.base_url == 'production':
            self.base_url = self.PRODUCTION_BASE_URL
        self.token = "mock_token"

    @classmethod
    def environment_values(cls):
        config = Configuration.integration('Enki')
        values = []
        for name in [
                'library_id',
		'username',
		'password',
		'url'
        ]:
            value = config.get(name)
            if value:
                value = value.encode("utf8")
            values.append(value)
        return values

    @classmethod
    def from_environment(cls, _db):
	values = cls.environment_values()
	if len([x for x in values if not x]):
	    cls.log.info( "No Enki client configured" )
	    return None
        return cls(_db)

    @property
    def source(self):
        return DataSource.lookup(self._db, DataSource.ENKI)

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

    def availability(self, patron_id=None, since=None, title_ids=[], strt=0, qty=2000):
        self.log.debug ("requesting : "+ str(qty) + " books starting at econtentRecord" +  str(strt))
        url = str(self.base_url) + str(self.availability_endpoint)
        args = dict()
	args['method'] = "getAllTitles"
	args['id'] = "secontent"
        args['strt'] = strt
        args['qty'] = qty
        args['lib'] = self.lib
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

    def reaper_request(self, identifier):
        self.log.debug ("Checking availability for " + str(identifier))
        now = datetime.datetime.utcnow()
        url = str(self.base_url) + str(self.item_endpoint)
        args = dict()
        args['method'] = "getItem"
        args['recordid'] = identifier
        args['size'] = "small"
        args['lib'] = self.lib
        response = self.request(url, method='get', params=args)
        if not(response.content.startswith("{\"result\":{\"id\":\"")):
            pool = identifier.licensed_through
            if pool and (pool.licenses_owned > 0):
                if pool.presentation_edition:
                    self.log.warn("Removing %s (%s) from circulation",
                                  pool.presentation_edition.title, pool.presentation_edition.author)
                else:
                    self.log.warn(
                        "Removing unknown work %s from circulation.",
                        identifier.identifier
                    )
            pool.licenses_owned = 0
            pool.licenses_available = 0
            pool.licenses_reserved = 0
            pool.patrons_in_hold_queue = 0
            pool.last_checked = now
        else:
            self.log.debug ("Keeping existing book: " + str(identifier))

class MockEnkiAPI(EnkiAPI):
    def __init__(self, _db, *args, **kwargs):
        self.responses = []
        self.requests = []

        library = Library.instance(_db)
        collection, ignore = get_one_or_create(
            _db, Collection,
            name="Test Enki Collection",
            protocol=Collection.ENKI, create_method_kwargs=dict(
                external_account_id=u'c',
            )
        )
        collection.external_integration.username = u'a'
        collection.external_integration.password = u'b'
        collection.external_integration.url = "http://enki.test/"
        library.collections.append(collection)
        super(MockEnkiAPI, self).__init__(
            _db, collection, *args, **kwargs
        )

    def queue_response(self, status_code, headers={}, content=None):
        from core.testing import MockRequestsResponse
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

class EnkiBibliographicCoverageProvider(BibliographicCoverageProvider):
    #TODO
    """Fill in bibliographic metadata for Enki records.

    Currently this is only used by BibliographicRefreshScript. It's
    not normally necessary because the Enki API combines
    bibliographic and availability data.
    """
    def __init__(self, _db, metadata_replacement_policy=None, enki_api=None,
                 input_identifier_types=None, input_identifiers=None, **kwargs):
        """
        :param input_identifier_types: Passed in by RunCoverageProviderScript, data sources to get coverage for.
        :param input_identifiers: Passed in by RunCoverageProviderScript, specific identifiers to get coverage for.
        """
        self.parser = BibliographicParser()
        super(EnkiBibliographicCoverageProvider, self).__init__(
            _db, enki_api, DataSource.ENKI,
            batch_size=25,
            metadata_replacement_policy=metadata_replacement_policy,
            **kwargs
        )

    def process_batch(self, identifiers):
        identifier_strings = self.api.create_identifier_strings(identifiers)
        response = self.api.availability(title_ids=identifier_strings)
        seen_identifiers = set()
        batch_results = []
        for metadata, availability in self.parser.process_all(response.content):
            identifier, is_new = metadata.primary_identifier.load(self._db)
            if not identifier in identifiers:
                # Enki told us about a book we didn't ask
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
                    self._db, Identifier.ENKI_ID, identifier_string
                )
                result = CoverageFailure(
                    identifier, "Book not found in Enki", data_source=self.output_source, transient=True
                )
                batch_results.append(result)
        return batch_results

    def handle_success(self, identifier):
        return self.set_presentation_ready(identifier)

    def process_item(self, identifier):
        results = self.process_batch([identifier])
        return results[0]

class BibliographicParser(object):

    """Helper function to parse JSON"""
    def process_all(self, json_data):
        data = json.loads(json_data)
        returned_titles = data["result"]["titles"]
	titles = returned_titles
	for book in returned_titles:
	    data = self.process_one(book)
            if data:
                yield data
    log = logging.getLogger("Enki Bibliographic Parser")
    log.setLevel(logging.DEBUG)

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

    def extract_bibliographic(self, element):
        identifiers = []
        contributors = []
        identifiers.append(IdentifierData(Identifier.ISBN, element["isbn"]))
        sort_name = element["author"]
        if not sort_name:
            sort_name = "Unknown"
        contributors.append(ContributorData(sort_name=sort_name))
        primary_identifier = IdentifierData(Identifier.ENKI_ID, element["id"])
	metadata = Metadata(
        data_source=DataSource.ENKI,
        title=element["title"],
        language="ENGLISH",
        medium=Edition.BOOK_MEDIUM,
        #series=series,
        publisher=element["publisher"],
        #imprint=imprint,
        #published=publication_date,
        primary_identifier=primary_identifier,
        identifiers=identifiers,
        #subjects=subjects,
        contributors=contributors,
        )
        licenses_owned=element["availability"]["totalCopies"]
        licenses_available=element["availability"]["availableCopies"]
        hold=element["availability"]["onHold"]
        formats = []
        formats.append(FormatData(content_type=Representation.EPUB_MEDIA_TYPE, drm_scheme=DeliveryMechanism.ADOBE_DRM))

        circulationdata = CirculationData(
            data_source=DataSource.ENKI,
            primary_identifier=primary_identifier,
            formats=formats,
            licenses_owned = int(licenses_owned),
            licenses_available = int(licenses_available),
            patrons_in_hold_queue = int(hold)
        )

        metadata.circulation = circulationdata
        return metadata


    def process_one(self, element):
        if self.include_bibliographic:
            bibliographic = self.extract_bibliographic(element)
        else:
            bibliographic = None

        availability = None
        if self.include_availability:
            availability = bibliographic.circulation

        return bibliographic, availability

class EnkiImport(Monitor):
    """Import Enki titles.
    """

    VERY_LONG_AGO = datetime.datetime(1970, 1, 1)
    FIVE_MINUTES = datetime.timedelta(minutes=5)

    def __init__(self, _db, name="Enki Import",
                 interval_seconds=60, batch_size=50, api=None):
	super(EnkiImport, self).__init__(
            _db, name, interval_seconds=interval_seconds,
            default_start_time = self.VERY_LONG_AGO
        )
        self.batch_size = batch_size
        self.api = api or EnkiAPI.from_environment(self._db)
        self.bibliographic_coverage_provider = (
            EnkiBibliographicCoverageProvider(self._db, enki_api=api)
        )

    def run_once(self, start, cutoff):
        # Give us five minutes of overlap because it's very important
        # we don't miss anything.
        since = start-self.FIVE_MINUTES
        id_start = 0
        while True:
            availability = self.api.availability(since=since, strt=id_start, qty=self.batch_size)
            if availability.status_code != 200:
                self.log.error(
                    "Could not contact Enki server for content availability. Status: %d",
                    availability.status_code
                )
            content = availability.content
            count = 0
            for bibliographic, circulation in BibliographicParser().process_all(content):
                self.process_book(bibliographic, circulation)
                count += 1
            if count == 0:
                break
            self._db.commit()
            id_start += self.batch_size

    def process_book(self, bibliographic, availability):

        license_pool, new_license_pool = availability.license_pool(self._db)
        edition, new_edition = bibliographic.edition(self._db)
        license_pool.edition = edition
        policy = ReplacementPolicy(
            identifiers=False,
            subjects=True,
            contributions=True,
            formats=True,
        )
        availability.apply(
            pool=license_pool,
            replace=policy,
        )
        if new_edition:
            bibliographic.apply(edition, replace=policy)

        if new_license_pool or new_edition:
            # At this point we have done work equivalent to that done by
            # the EnkiBibliographicCoverageProvider. Register that the
            # work has been done so we don't have to do it again.
            identifier = edition.primary_identifier
            self.bibliographic_coverage_provider.handle_success(identifier)
            self.bibliographic_coverage_provider.add_coverage_record_for(
                identifier
            )

        return edition, license_pool

class EnkiCollectionReaper(IdentifierSweepMonitor):
    """Check for books that are in the local collection but have left the Enki collection."""
    def __init__(self, _db, api=None, interval_seconds=3600*4):
        super(EnkiCollectionReaper, self).__init__(_db, "Enki Collection Reaper", interval_seconds)
        self._db = _db
        if not api:
            api = EnkiAPI.from_environment(_db)
        self.api = api
        self.data_source = DataSource.lookup(self._db, DataSource.ENKI)

    def run(self):
        super(EnkiCollectionReaper, self).run()

    def identifier_query(self):
        return self._db.query(Identifier).filter(
            Identifier.type==Identifier.ENKI_ID)

    def process_batch(self, identifiers):
        for identifier in identifiers:
            self.api.reaper_request(identifier.identifier)

