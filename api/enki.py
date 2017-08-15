from nose.tools import set_trace
from collections import defaultdict
import datetime
import base64
import os
import json
import logging
import re
from flask.ext.babel import lazy_gettext as _

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
    ExternalIntegration,
    Session,
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
    CollectionMonitor,
)

from core.opds_import import SimplifiedOPDSLookup
from core.analytics import Analytics
from core.testing import DatabaseTest

#TODO: Remove unnecessary imports (once the classes are more or less complete)

class EnkiAPI(BaseCirculationAPI):

    DESCRIPTION = _("Integrate an Enki collection.")
    SETTINGS = [
        { "key": Collection.EXTERNAL_ACCOUNT_ID_KEY, "label": _("Library ID") },
        { "key": ExternalIntegration.URL, "label": _("URL") },
    ] + BaseCirculationAPI.SETTINGS

    PRODUCTION_BASE_URL = "http://enkilibrary.org/API/"
    availability_endpoint = "ListAPI"
    item_endpoint = "ItemAPI"
    user_endpoint = "UserAPI"

    NAME = u"Enki"
    ENKI = NAME
    ENKI_EXTERNAL = NAME
    ENKI_ID = u"Enki ID"

    SET_DELIVERY_MECHANISM_AT = BaseCirculationAPI.BORROW_STEP
    SERVICE_NAME = "Enki"
    log = logging.getLogger("Enki API")
    log.setLevel(logging.DEBUG)

    def __init__(self, _db, collection):
        self._db = _db

        if collection.protocol != self.ENKI:
            raise ValueError(
                "Collection protocol is %s, but passed into EnkiAPI!" %
                collection.protocol
            )
        self.collection = collection
        self.library_id = collection.external_account_id.encode("utf8")
        self.base_url = collection.external_integration.url or self.PRODUCTION_BASE_URL

        if not self.library_id or not self.base_url:
            raise CannotLoadConfiguration(
                "Enki configuration is incomplete."
            )
        self.enki_bibliographic_coverage_provider = (
            EnkiBibliographicCoverageProvider(
                collection, api_class=self
            )
        )

    def request(self, url, method='get', extra_headers={}, data=None,
                params=None, exception_on_401=False):
        """Make an HTTP request, acquiring/refreshing a bearer token
        if necessary.
        """
        headers = dict(extra_headers)
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
        args['lib'] = self.library_id
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
        self.log.debug ("Checking availability for " + str(identifier.identifier))
        now = datetime.datetime.utcnow()
        url = str(self.base_url) + str(self.item_endpoint)
        args = dict()
        args['method'] = "getItem"
        args['recordid'] = identifier.identifier
        args['size'] = "small"
        args['lib'] = self.library_id
        response = self.request(url, method='get', params=args)

        try:
            # If a book doesn't exist in Enki, we'll just get an HTML page saying we did something wrong.
            data = json.loads(response.content)
            self.log.debug ("Keeping existing book: " + str(identifier))
        except:
            # Get the license pool for the ID, but make sure it's the one belonging to Enki
            pool = [x for x in identifier.licensed_through if x.data_source.name == DataSource.ENKI][0]
            if pool and (pool.licenses_owned > 0):
                if pool.presentation_edition:
                    self.log.warn("Removing %s (%s) from circulation",
                                  pool.presentation_edition.title, pool.presentation_edition.author)
                else:
                    self.log.warn(
                        "Removing unknown work %s from circulation.",
                        identifier.identifier
                    )

            circulationdata = CirculationData(
                data_source=DataSource.ENKI,
                primary_identifier= IdentifierData(EnkiAPI.ENKI_ID, identifier.identifier),
                licenses_owned = 0,
                licenses_available = 0,
                patrons_in_hold_queue = 0,
                last_checked = now
            )

            circulationdata.apply(
                self._db,
                self.collection,
                replace=ReplacementPolicy.from_license_source(self._db)
            )

            return circulationdata

    def checkout(self, patron, pin, licensepool, internal_format):
        # WIP.
        return None

        # Create the loan info. We don't know the expiration 
        loan = LoanInfo(
            licensepool.collection,
            licensepool.data_source.name,
            licensepool.identifier.type,
            licensepool.identifier.identifier,
            None,
            expires,
            None,
        )
        return loan

    def get_loan(barcode, pin, book_id):
        self.log.debug ("Sending checkout request for %d" % book_id)
        now = datetime.datetime.utcnow()
        url = str(self.base_url) + str(self.user_endpoint)
        args = dict()
        args['method'] = "getSELink"
        args['username'] = barcide
        args['password'] = pin
        args['lib'] = self.library_id
        args['id'] = book_id
        response = self.request(url, method='get', params=args)

class MockEnkiAPI(EnkiAPI):
    def __init__(self, _db, *args, **kwargs):
        self.responses = []
        self.requests = []

        library = DatabaseTest.make_default_library(_db)
        collection, ignore = Collection.by_name_and_protocol(
            _db, name="Test Enki Collection", protocol=EnkiAPI.ENKI
        )
        collection.protocol=EnkiAPI.ENKI
        collection.external_account_id=u'c';
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
    """Fill in bibliographic metadata for Enki records.

    Currently this is only used by BibliographicRefreshScript. It's
    not normally necessary because the Enki API combines
    bibliographic and availability data.
    """

    SERVICE_NAME = "Enki Bibliographic Coverage Provider"
    DATA_SOURCE_NAME = DataSource.ENKI
    PROTOCOL = EnkiAPI.ENKI
    INPUT_IDENTIFIER_TYPES = EnkiAPI.ENKI_ID

    def __init__(self, collection, api_class=EnkiAPI, **kwargs):
        """Constructor.

        :param collection: Provide bibliographic coverage to all
            Axis 360 books in the given Collection.
        :param api_class: Instantiate this class with the given Collection,
            rather than instantiating Axis360API.
        """
        _db = Session.object_session(collection)
        super(EnkiBibliographicCoverageProvider, self).__init__(
            collection, **kwargs
        )
        if isinstance(api_class, EnkiAPI):
            # We were given a specific EnkiAPI instance to use.
            self.api = api_class
        else:
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
                    self._db, api.ENKI_ID, identifier_string
                )
                result = CoverageFailure(
                    identifier, "Book not found in Enki", data_source=self.output_source, transient=True
                )
                batch_results.append(result)
        return batch_results

    def process_item(self, identifier):
        results = self.process_batch([identifier])
        return results[0]

    def handle_success(self, identifier):
        return self.set_presentation_ready(identifier)

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
        primary_identifier = IdentifierData(EnkiAPI.ENKI_ID, element["id"])
        metadata = Metadata(
        data_source=DataSource.ENKI,
        title=element["title"],
        language="eng",
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

class EnkiImport(CollectionMonitor):
    """Import content from Enki that we don't yet have in our collection
    """
    SERVICE_NAME = "Enki Circulation Monitor"
    INTERVAL_SECONDS = 500
    PROTOCOL = EnkiAPI.ENKI_EXTERNAL
    DEFAULT_BATCH_SIZE = 100 
    FIVE_MINUTES = datetime.timedelta(minutes=5)
    
    def __init__(self, _db, collection, api_class=EnkiAPI):
        """Constructor."""
        super(EnkiImport, self).__init__(_db, collection)
        self.api = api_class(_db, collection)
        self.analytics = Analytics(_db)
        self.bibliographic_coverage_provider = (
            EnkiBibliographicCoverageProvider(_db, collection, api_class=self.api)
        )

    def recently_changed_ids(self, start, cutoff):
        return self.api.recently_changed_ids(start, cutoff)

    def run_once(self, start, cutoff):
        # Give us five minutes of overlap because it's very important
        # we don't miss anything.
        since = start-self.FIVE_MINUTES
        id_start = 0
        while True:
            availability = self.api.availability(since=since, strt=id_start, qty=self.DEFAULT_BATCH_SIZE)
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
            id_start += self.DEFAULT_BATCH_SIZE

    def process_book(self, bibliographic, availability):
        license_pool, new_license_pool = availability.license_pool(self._db, self.collection)
        edition, new_edition = bibliographic.edition(self._db)
        license_pool.edition = edition
        policy = ReplacementPolicy(
            identifiers=False,
            subjects=True,
            contributions=True,
            formats=True,
        )
        availability.apply(
            self._db,
            license_pool.collection,
            replace=policy,
        )
        if new_edition:
            bibliographic.apply(edition, self.collection, replace=policy)

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

    SERVICE_NAME = "Enki Collection Reaper"
    INTERVAL_SECONDS = 3600*4
    PROTOCOL = "Enki"

    def __init__(self, _db, collection, api_class=EnkiAPI):
        self._db = _db
        super(EnkiCollectionReaper, self).__init__(self._db, collection)
        self.api = api_class(self._db, collection)

    def process_item(self, identifier):
        self.api.reaper_request(identifier)
