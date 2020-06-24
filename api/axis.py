import base64
import json
import logging
import os
import re
import urlparse

from collections import defaultdict
from datetime import datetime, timedelta
from flask_babel import lazy_gettext as _
from lxml import etree
from nose.tools import set_trace
from sqlalchemy.orm import contains_eager
from sqlalchemy.orm.session import Session

from core.analytics import Analytics

from core.config import (
    CannotLoadConfiguration,
    Configuration,
    temp_config,
)

from core.coverage import (
    BibliographicCoverageProvider,
    CoverageFailure,
)

from core.metadata_layer import (
    CirculationData,
    ContributorData,
    FormatData,
    IdentifierData,
    LinkData,
    Metadata,
    ReplacementPolicy,
    SubjectData,
)

from core.model import (
    CirculationEvent,
    Classification,
    Collection,
    Contributor,
    DataSource,
    DeliveryMechanism,
    Edition,
    ExternalIntegration,
    get_one,
    get_one_or_create,
    Hyperlink,
    Identifier,
    Library,
    LicensePool,
    LinkRelations,
    MediaTypes,
    Representation,
    Session,
    Subject,
)

from core.monitor import (
    CollectionMonitor,
    IdentifierSweepMonitor,
    TimelineMonitor,
)

from core.opds_import import (
    MetadataWranglerOPDSLookup
)

from core.testing import DatabaseTest

from core.util import LanguageCodes
from core.util.xmlparser import XMLParser
from core.util.http import (
    HTTP,
    RemoteIntegrationException,
)


from authenticator import Authenticator

from circulation import (
    APIAwareFulfillmentInfo,
    LoanInfo,
    FulfillmentInfo,
    HoldInfo,
    BaseCirculationAPI
)
from circulation_exceptions import *

from selftest import (
    HasCollectionSelfTests,
    SelfTestResult,
)

from web_publication_manifest import (
    FindawayManifest,
    SpineItem,
)


class Axis360API(Authenticator, BaseCirculationAPI, HasCollectionSelfTests):

    NAME = ExternalIntegration.AXIS_360

    SET_DELIVERY_MECHANISM_AT = BaseCirculationAPI.BORROW_STEP

    SERVICE_NAME = "Axis 360"
    PRODUCTION_BASE_URL = "https://axis360api.baker-taylor.com/Services/VendorAPI/"
    QA_BASE_URL = "http://axis360apiqa.baker-taylor.com/Services/VendorAPI/"
    SERVER_NICKNAMES = {
        "production" : PRODUCTION_BASE_URL,
        "qa" : QA_BASE_URL,
    }
    DATE_FORMAT = "%m-%d-%Y %H:%M:%S"

    SETTINGS = [
        { "key": ExternalIntegration.USERNAME, "label": _("Username"), "required": True },
        { "key": ExternalIntegration.PASSWORD, "label": _("Password"), "required": True },
        { "key": Collection.EXTERNAL_ACCOUNT_ID_KEY, "label": _("Library ID"), "required": True },
        { "key": ExternalIntegration.URL,
          "label": _("Server"),
          "default": PRODUCTION_BASE_URL,
          "required": True,
          "format": "url",
          "allowed": SERVER_NICKNAMES.keys(),
        },
    ] + BaseCirculationAPI.SETTINGS

    LIBRARY_SETTINGS = BaseCirculationAPI.LIBRARY_SETTINGS + [
        BaseCirculationAPI.DEFAULT_LOAN_DURATION_SETTING
    ]

    access_token_endpoint = 'accesstoken'
    availability_endpoint = 'availability/v2'
    fulfillment_endpoint = 'getfullfillmentInfo/v2'
    audiobook_metadata_endpoint = 'getaudiobookmetadata/v2'

    log = logging.getLogger("Axis 360 API")

    # Create a lookup table between common DeliveryMechanism identifiers
    # and Axis 360 format types.
    epub = Representation.EPUB_MEDIA_TYPE
    pdf = Representation.PDF_MEDIA_TYPE
    adobe_drm = DeliveryMechanism.ADOBE_DRM
    findaway_drm = DeliveryMechanism.FINDAWAY_DRM
    no_drm = DeliveryMechanism.NO_DRM

    # The name Axis 360 gives for the web interface.
    AXISNOW = "AxisNow"

    delivery_mechanism_to_internal_format = {
        (epub, no_drm): 'ePub',
        (epub, adobe_drm): 'ePub',
        (pdf, no_drm): 'PDF',
        (pdf, adobe_drm): 'PDF',
        (None, findaway_drm): 'Acoustik',
        (Representation.UNZIPPED_EPUB_MEDIA_TYPE, 
         DeliveryMechanism.AXISNOW_DRM): AXISNOW
    }

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
        if not base_url.endswith('/'):
            base_url += '/'
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

    def external_integration(self, _db):
        return self.collection.external_integration

    def _run_self_tests(self, _db):
        result = self.run_test(
            "Refreshing bearer token", self.refresh_bearer_token
        )
        yield result
        if not result.success:
            # If we can't get a bearer token, there's no point running
            # the rest of the tests.
            return

        def _count_events():
            now = datetime.utcnow()
            five_minutes_ago = now - timedelta(minutes=5)
            count = len(list(self.recent_activity(since=five_minutes_ago)))
            return "Found %d event(s)" % count

        yield self.run_test(
            "Asking for circulation events for the last five minutes",
            _count_events
        )

        for result in self.default_patrons(self.collection):
            if isinstance(result, SelfTestResult):
                yield result
                continue
            library, patron, pin = result
            def _count_activity():
                result = self.patron_activity(patron, pin)
                return "Found %d loans/holds" % len(result)
            yield self.run_test(
                "Checking activity for test patron for library %s" % library.name,
                _count_activity
            )

        # Run the tests defined by HasCollectionSelfTests
        for result in super(Axis360API, self)._run_self_tests():
            yield result

    def refresh_bearer_token(self):
        url = self.base_url + self.access_token_endpoint
        headers = self.authorization_headers
        response = self._make_request(
            url, 'post', headers, allowed_response_codes=[200]
        )
        return self.parse_token(response.content)

    def request(self, url, method='get', extra_headers={}, data=None,
                params=None, exception_on_401=False, **kwargs):
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
            disallowed_response_codes=disallowed_response_codes,
            **kwargs
        )
        if response.status_code == 401:
            # This must be our first 401, since our second 401 will
            # make _make_request raise a RemoteIntegrationException.
            #
            # The token has expired. Get a new token and try again.
            self.token = None
            return self.request(
                url=url, method=method, extra_headers=extra_headers,
                data=data, params=params, exception_on_401=True,
                **kwargs
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
        response = self.request(url, params=args, timeout=None)
        return response

    def get_fulfillment_info(self, transaction_id):
        """Make a call to the getFulfillmentInfoAPI."""
        url = self.base_url + self.fulfillment_endpoint
        params = dict(TransactionID=transaction_id)
        return self.request(url, "POST", params=params)

    def get_audiobook_metadata(self, findaway_content_id):
        """Make a call to the getaudiobookmetadata endpoint."""
        base_url = self.base_url
        url = base_url + self.audiobook_metadata_endpoint
        params = dict(fndcontentid=findaway_content_id)
        response = self.request(url, "POST", params=params)
        return response

    def checkout(self, patron, pin, licensepool, internal_format):
        title_id = licensepool.identifier.identifier
        patron_id = patron.authorization_identifier
        response = self._checkout(title_id, patron_id, internal_format)
        try:
            return CheckoutResponseParser(
                licensepool.collection).process_all(response.content)
        except etree.XMLSyntaxError, e:
            raise RemoteInitiatedServerError(
                response.content, self.SERVICE_NAME
            )

    def _checkout(self, title_id, patron_id, internal_format):
        url = self.base_url + "checkout/v2"
        args = dict(titleId=title_id, patronId=patron_id,
                    format=internal_format)
        response = self.request(url, data=args, method="POST")
        return response

    def fulfill(self, patron, pin, licensepool, internal_format, **kwargs):
        """Fulfill a patron's request for a specific book.

        :param kwargs: A container for arguments to fulfill()
           which are not relevant to this vendor.

        :return: a FulfillmentInfo object.
        """
        identifier = licensepool.identifier
        # This should include only one 'activity'.
        activities = self.patron_activity(patron, pin, licensepool.identifier, internal_format)
        for loan in activities:
            if not isinstance(loan, LoanInfo):
                continue
            if not (loan.identifier_type == identifier.type
                    and loan.identifier == identifier.identifier):
                continue
            # We've found the remote loan corresponding to this
            # license pool.
            fulfillment = loan.fulfillment_info
            if not fulfillment or not isinstance(fulfillment, FulfillmentInfo):
                raise CannotFulfill()

            return fulfillment

        # If we made it to this point, the patron does not have this
        # book checked out.
        raise NoActiveLoan()

    def checkin(self, patron, pin, licensepool):
        pass

    def place_hold(self, patron, pin, licensepool, hold_notification_email):
        if not hold_notification_email:
            hold_notification_email = self.default_notification_email_address(
                patron, pin
            )

        url = self.base_url + "addtoHold/v2"
        identifier = licensepool.identifier
        title_id = identifier.identifier
        patron_id = patron.authorization_identifier
        params = dict(titleId=title_id, patronId=patron_id,
                      email=hold_notification_email)
        response = self.request(url, params=params)
        hold_info = HoldResponseParser(licensepool.collection).process_all(
            response.content)
        if not hold_info.identifier:
            # The Axis 360 API doesn't return the identifier of the
            # item that was placed on hold, so we have to fill it in
            # based on our own knowledge.
            hold_info.identifier_type = identifier.type
            hold_info.identifier = identifier.identifier
        return hold_info

    def release_hold(self, patron, pin, licensepool):
        url = self.base_url + "removeHold/v2"
        identifier = licensepool.identifier
        title_id = identifier.identifier
        patron_id = patron.authorization_identifier
        params = dict(titleId=title_id, patronId=patron_id)
        response = self.request(url, params=params)
        try:
            HoldReleaseResponseParser(licensepool.collection).process_all(
                response.content)
        except NotOnHold:
            # Fine, it wasn't on hold and now it's still not on hold.
            pass
        # If we didn't raise an exception, we're fine.
        return True

    def patron_activity(self, patron, pin, identifier=None, internal_format=None):
        if identifier:
            title_ids = [identifier.identifier]
        else:
            title_ids = None
        availability = self.availability(
            patron_id=patron.authorization_identifier,
            title_ids=title_ids)
        return list(AvailabilityResponseParser(self, internal_format).process_all(
            availability.content))

    def update_availability(self, licensepool):
        """Update the availability information for a single LicensePool.

        Part of the CirculationAPI interface.
        """
        self.update_licensepools_for_identifiers([licensepool.identifier])

    def update_licensepools_for_identifiers(self, identifiers):
        """Update availability and bibliographic information for
        a list of books.

        If the book has never been seen before, a new LicensePool
        will be created for the book.

        The book's LicensePool will be updated with current
        circulation information.
        """
        remainder = set(identifiers)
        for bibliographic, availability in self._fetch_remote_availability(
                identifiers
        ):
            edition, ignore1, license_pool, ignore2 = self.update_book(
                bibliographic, availability
            )
            identifier = license_pool.identifier
            if identifier in remainder:
                remainder.remove(identifier)

        # We asked Axis about n books. It sent us n-k responses. Those
        # k books are the identifiers in `remainder`. These books have
        # been removed from the collection without us being notified.
        for removed_identifier in remainder:
            self._reap(removed_identifier)

    def update_book(self, bibliographic, availability, analytics=None):
        """Create or update a single book based on bibliographic
        and availability data from the Axis 360 API.

        :param bibliographic: A Metadata object containing
            bibliographic data about this title.
        :param availability: A CirculationData object containing
            availability data about this title.
        """
        analytics = analytics or Analytics(self._db)
        license_pool, new_license_pool = availability.license_pool(
            self._db, self.collection, analytics
        )
        edition, new_edition = bibliographic.edition(self._db)
        license_pool.edition = edition
        policy = ReplacementPolicy(
            identifiers=False,
            subjects=True,
            contributions=True,
            formats=True,
            links=True,
            analytics=analytics,
        )

        # NOTE: availability is bibliographic.circulation, so it's a
        # little redundant to call availability.apply() -- it's taken
        # care of inside bibliographic.apply().
        bibliographic.apply(edition, self.collection, replace=policy)
        availability.apply(self._db, self.collection, replace=policy)
        return edition, new_edition, license_pool, new_license_pool

    def _fetch_remote_availability(self, identifiers):
        """Retrieve availability information for the specified identifiers.

        :yield: A stream of (Metadata, CirculationData) 2-tuples.
        """
        identifier_strings = self.create_identifier_strings(identifiers)
        response = self.availability(title_ids=identifier_strings)
        parser = BibliographicParser(self.collection)
        return parser.process_all(response.content)

    def _reap(self, identifier):
        """Update our local circulation information to reflect the fact that
        the identified book has been removed from the remote
        collection.
        """
        collection = self.collection
        pool = identifier.licensed_through_collection(collection)
        if not pool:
            self.log.warn(
                "Was about to reap %r but no local license pool in this collection.",
                identifier
            )
            return
        if pool.licenses_owned == 0:
            # Already reaped.
            return
        self.log.info("Reaping %r", identifier)

        availability = CirculationData(
            data_source=pool.data_source,
            primary_identifier=identifier,
            licenses_owned=0,
            licenses_available=0,
            licenses_reserved=0,
            patrons_in_hold_queue=0,
        )
        availability.apply(
            self._db, collection,
            ReplacementPolicy.from_license_source(self._db)
        )

    def recent_activity(self, since):
        """Find books that have had recent activity.

        :yield: A sequence of (Metadata, CirculationData) 2-tuples
        """
        availability = self.availability(since=since)
        content = availability.content
        for bibliographic, circulation in BibliographicParser(self.collection).process_all(
                content):
            yield bibliographic, circulation

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

class Axis360CirculationMonitor(CollectionMonitor, TimelineMonitor):

    """Maintain LicensePools for Axis 360 titles.
    """
    SERVICE_NAME = "Axis 360 Circulation Monitor"
    INTERVAL_SECONDS = 60
    DEFAULT_BATCH_SIZE = 50

    PROTOCOL = ExternalIntegration.AXIS_360

    DEFAULT_START_TIME = datetime(1970, 1, 1)

    def __init__(self, _db, collection, api_class=Axis360API):
        super(Axis360CirculationMonitor, self).__init__(_db, collection)
        if isinstance(api_class, Axis360API):
            # Use a preexisting Axis360API instance rather than
            # creating a new one.
            self.api = api_class
        else:
            self.api = api_class(_db, collection)

        self.batch_size = self.DEFAULT_BATCH_SIZE
        self.bibliographic_coverage_provider = (
            Axis360BibliographicCoverageProvider(collection, api_class=self.api)
        )

    def catch_up_from(self, start, cutoff, progress):
        """Find Axis 360 books that changed recently.

        :progress: A TimestampData representing the time previously
            covered by this Monitor.
        """
        count = 0
        for bibliographic, circulation in self.api.recent_activity(start):
            self.process_book(bibliographic, circulation)
            count += 1
            if count % self.batch_size == 0:
                self._db.commit()
        progress.achievements = "Modified titles: %d." % count

    def process_book(self, bibliographic, circulation):
        edition, new_edition, license_pool, new_license_pool = self.api.update_book(
            bibliographic, circulation
        )
        if new_license_pool or new_edition:
            # At this point we have done work equivalent to that done by
            # the Axis360BibliographicCoverageProvider. Register that the
            # work has been done so we don't have to do it again.
            identifier = edition.primary_identifier
            self.bibliographic_coverage_provider.handle_success(identifier)
            self.bibliographic_coverage_provider.add_coverage_record_for(
                identifier
            )


        return edition, license_pool


class MockAxis360API(Axis360API):
    @classmethod
    def mock_collection(self, _db, name="Test Axis 360 Collection"):
        """Create a mock Axis 360 collection for use in tests."""
        library = DatabaseTest.make_default_library(_db)
        collection, ignore = get_one_or_create(
            _db, Collection,
            name=name,    
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

class AxisCollectionReaper(IdentifierSweepMonitor):
    """Check for books that are in the local collection but have left our
    Axis 360 collection.
    """
    SERVICE_NAME = "Axis Collection Reaper"
    INTERVAL_SECONDS = 3600*12
    PROTOCOL = ExternalIntegration.AXIS_360

    def __init__(self, _db, collection, api_class=Axis360API):
        super(AxisCollectionReaper, self).__init__(_db, collection)
        if isinstance(api_class, Axis360API):
            # Use a preexisting Axis360API instance rather than
            # creating a new one.
            self.api = api_class
        else:
            self.api = api_class(_db, collection)

    def process_items(self, identifiers):
        self.api.update_licensepools_for_identifiers(identifiers)


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
            attempt = datetime.strptime(
                value, self.FULL_DATE_FORMAT_IMPLICIT_UTC)
            value += ' +00:00'
        except ValueError:
            pass
        return datetime.strptime(value, self.FULL_DATE_FORMAT)

class BibliographicParser(Axis360Parser):

    DELIVERY_DATA_FOR_AXIS_FORMAT = {
        "Blio" : None,   # Unknown ebook format
        "Acoustik" : (None, DeliveryMechanism.FINDAWAY_DRM), # Audiobooks
        "AxisNow" : None, # Handled specially, for ebooks only.
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
                attempt = datetime.strptime(
                    availability_updated, self.FULL_DATE_FORMAT_IMPLICIT_UTC)
                availability_updated += ' +00:00'
            except ValueError:
                pass
            availability_updated = datetime.strptime(
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
                        name=subject_identifier,
                        weight=Classification.TRUSTED_DISTRIBUTOR_WEIGHT
                    )
                )

        publication_date = self.text_of_optional_subtag(
            element, 'axis:publicationDate', ns)
        if publication_date:
            publication_date = datetime.strptime(
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
                    weight=Classification.TRUSTED_DISTRIBUTOR_WEIGHT,
                )
            )

        language = self.text_of_subtag(element, 'axis:language', ns)

        thumbnail_url = self.text_of_optional_subtag(
            element, 'axis:imageUrl', ns
        )
        if thumbnail_url:
            # We presume all images from this service are JPEGs.
            media_type = MediaTypes.JPEG_MEDIA_TYPE
            if '/Medium/' in thumbnail_url:
                # We know about a URL hack for this service that lets us
                # get a larger image.
                full_size_url = thumbnail_url.replace("/Medium/", "/Large/")
            else:
                # If the URL hack won't work, treat the image we got
                # as both the full-sized image and its thumbnail.
                # This won't happen unless B&T changes the service.
                full_size_url = thumbnail_url

            thumbnail = LinkData(
                rel=LinkRelations.THUMBNAIL_IMAGE,
                href=thumbnail_url,
                media_type=media_type
            )
            image = LinkData(
                rel=LinkRelations.IMAGE,
                href=full_size_url,
                media_type=media_type,
                thumbnail=thumbnail
            )
            links.append(image)

        # We don't use this for anything.
        # file_size = self.int_of_optional_subtag(element, 'axis:fileSize', ns)
        primary_identifier = IdentifierData(Identifier.AXIS_360_ID, identifier)
        identifiers = []
        if isbn:
            identifiers.append(IdentifierData(Identifier.ISBN, isbn))

        formats = []
        acceptable = False
        seen_formats = []

        # All of the formats we don't support, like Blio, are ebook
        # formats. If this is an audiobook format (Acoustik), we'll
        # hear about it below.
        medium = Edition.BOOK_MEDIUM

        # If AxisNow is mentioned as a format, and this turns out to be a book,
        # we'll be adding an extra delivery mechanism.
        axisnow_seen = False

        for format_tag in self._xpath(
            element, 'axis:availability/axis:availableFormats/axis:formatName',
            ns
        ):
            informal_name = format_tag.text
            seen_formats.append(informal_name)

            if informal_name == Axis360API.AXISNOW:
                # We will only be adding an AxisNow FormatData if this
                # turns out to be an ebook.
                axisnow_seen = True
                continue

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

                if drm_scheme == DeliveryMechanism.FINDAWAY_DRM:
                    medium = Edition.AUDIO_MEDIUM
                else:
                    medium = Edition.BOOK_MEDIUM
        if medium == Edition.BOOK_MEDIUM and axisnow_seen:
            # This ebook is available through AxisNow. Add an
            # appropriate FormatData.
            #
            # Audiobooks may also be available through AxisNow, but we
            # currently ignore that fact.
            formats.append(
                FormatData(
                    content_type=Representation.UNZIPPED_EPUB_MEDIA_TYPE,
                    drm_scheme=DeliveryMechanism.AXISNOW_DRM
                )
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
            medium=medium,
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

class ResponseParser(Axis360Parser):

    id_type = Identifier.AXIS_360_ID

    SERVICE_NAME = "Axis 360"

    # Map Axis 360 error codes to our circulation exceptions.
    code_to_exception = {
        315  : InvalidInputException, # Bad password
        316  : InvalidInputException, # DRM account already exists
        1000 : PatronAuthorizationFailedException,
        1001 : PatronAuthorizationFailedException,
        1002 : PatronAuthorizationFailedException,
        1003 : PatronAuthorizationFailedException,
        2000 : LibraryAuthorizationFailedException,
        2001 : LibraryAuthorizationFailedException,
        2002 : LibraryAuthorizationFailedException,
        2003 : LibraryAuthorizationFailedException, # "Encoded input parameters exceed limit", whatever that meaus
        2004 : LibraryAuthorizationFailedException,
        2005 : LibraryAuthorizationFailedException, # Invalid credentials
        2005 : LibraryAuthorizationFailedException, # Wrong library ID
        2007 : LibraryAuthorizationFailedException, # Invalid library ID
        2008 : LibraryAuthorizationFailedException, # Invalid library ID
        3100 : LibraryInvalidInputException, # Missing title ID
        3101 : LibraryInvalidInputException, # Missing patron ID
        3102 : LibraryInvalidInputException, # Missing email address (for hold notification)
        3103 : NotFoundOnRemote, # Invalid title ID
        3104 : LibraryInvalidInputException, # Invalid Email Address (for hold notification)
        3105 : PatronAuthorizationFailedException, # Invalid Account Credentials
        3106 : InvalidInputException, # Loan Period is out of bounds
        3108 : InvalidInputException, # DRM Credentials Required
        3109 : InvalidInputException, # Hold already exists or hold does not exist, depending.
        3110 : AlreadyCheckedOut,
        3111 : CurrentlyAvailable,
        3112 : CannotFulfill,
        3113 : CannotLoan,
        (3113, "Title ID is not available for checkout") : NoAvailableCopies,
        3114 : PatronLoanLimitReached,
        3115 : LibraryInvalidInputException, # Missing DRM format
        3117 : LibraryInvalidInputException, # Invalid DRM format
        3118 : LibraryInvalidInputException, # Invalid Patron credentials
        3119 : LibraryAuthorizationFailedException, # No Blio account
        3120 : LibraryAuthorizationFailedException, # No Acoustikaccount
        3123 : PatronAuthorizationFailedException, # Patron Session ID expired
        3126 : LibraryInvalidInputException, # Invalid checkout format
        3127 : InvalidInputException, # First name is required
        3128 : InvalidInputException, # Last name is required
        3130 : LibraryInvalidInputException, # Invalid hold format (?)
        3131 : RemoteInitiatedServerError, # Custom error message (?)
        3132 : LibraryInvalidInputException, # Invalid delta datetime format
        3134 : LibraryInvalidInputException, # Delta datetime format must not be in the future
        3135 : NoAcceptableFormat,
        3136 : LibraryInvalidInputException, # Missing checkout format
        5000 : RemoteInitiatedServerError,
        5003 : LibraryInvalidInputException, # Missing TransactionID
        5004 : LibraryInvalidInputException, # Missing TransactionID
    }

    def __init__(self, collection):
        """Constructor.

        :param collection: A Collection, in case parsing this document
        results in the creation of LoanInfo or HoldInfo objects.
        """
        self.collection = collection

    def raise_exception_on_error(self, e, ns, custom_error_classes={}):
        """Raise an error if the given lxml node represents an Axis 360 error
        condition.
        """
        code = self._xpath1(e, '//axis:status/axis:code', ns)
        message = self._xpath1(e, '//axis:status/axis:statusMessage', ns)
        if message is None:
            message = etree.tostring(e)
        else:
            message = message.text
        if code is None:
            # Something is so wrong that we don't know what to do.
            raise RemoteInitiatedServerError(message, self.SERVICE_NAME)
        return self._raise_exception_on_error(
            code.text, message, custom_error_classes
        )

    @classmethod
    def _raise_exception_on_error(cls, code, message, custom_error_classes={}):
        try:
            code = int(code)
        except ValueError:
            # Non-numeric code? Inconcievable!
            raise RemoteInitiatedServerError(
                "Invalid response code from Axis 360: %s" % code,
                cls.SERVICE_NAME
            )

        for d in custom_error_classes, cls.code_to_exception:
            if (code, message) in d:
                raise d[(code, message)]
            elif code in d:
                # Something went wrong and we know how to turn it into a
                # specific exception.
                error_class = d[code]
                if error_class is RemoteInitiatedServerError:
                    e = error_class(message, cls.SERVICE_NAME)
                else:
                    e = error_class(message)
                raise e
        return code, message


class CheckoutResponseParser(ResponseParser):

    def process_all(self, string):
        for i in super(CheckoutResponseParser, self).process_all(
                string, "//axis:checkoutResult", self.NS):
            return i

    def process_one(self, e, namespaces):

        """Either turn the given document into a LoanInfo
        object, or raise an appropriate exception.
        """
        self.raise_exception_on_error(e, namespaces)

        # If we get to this point it's because the checkout succeeded.
        expiration_date = self._xpath1(e, '//axis:expirationDate', namespaces)
        fulfillment_url = self._xpath1(e, '//axis:url', namespaces)
        if fulfillment_url is not None:
            fulfillment_url = fulfillment_url.text

        if expiration_date is not None:
            expiration_date = expiration_date.text
            expiration_date = datetime.strptime(
                expiration_date, self.FULL_DATE_FORMAT)

        loan_start = datetime.utcnow()
        loan = LoanInfo(
            collection=self.collection, data_source_name=DataSource.AXIS_360,
            identifier_type=self.id_type, identifier=None,
            start_date=loan_start,
            end_date=expiration_date,
        )
        return loan

class HoldResponseParser(ResponseParser):

    def process_all(self, string):
        for i in super(HoldResponseParser, self).process_all(
                string, "//axis:addtoholdResult", self.NS):
            return i

    def process_one(self, e, namespaces):
        """Either turn the given document into a HoldInfo
        object, or raise an appropriate exception.
        """
        self.raise_exception_on_error(
            e, namespaces, {3109 : AlreadyOnHold})

        # If we get to this point it's because the hold place succeeded.
        queue_position = self._xpath1(
            e, '//axis:holdsQueuePosition', namespaces)
        if queue_position is None:
            queue_position = None
        else:
            try:
                queue_position = int(queue_position.text)
            except ValueError:
                print "Invalid queue position: %s" % queue_position
                queue_position = None

        hold_start = datetime.utcnow()
        # NOTE: The caller needs to fill in Collection -- we have no idea
        # what collection this is.
        hold = HoldInfo(
            collection=self.collection, data_source_name=DataSource.AXIS_360,
            identifier_type=self.id_type, identifier=None,
            start_date=hold_start, end_date=None, hold_position=queue_position)
        return hold

class HoldReleaseResponseParser(ResponseParser):

    def process_all(self, string):
        for i in super(HoldReleaseResponseParser, self).process_all(
                string, "//axis:removeholdResult", self.NS):
            return i

    def post_process(self, i):
        """Unlike other ResponseParser subclasses, we don't return any type of
            \*Info object, so there's no need to do any post-processing.
        """
        return i

    def process_one(self, e, namespaces):
        # There's no data to gather here. Either there was an error
        # or we were successful.
        self.raise_exception_on_error(
            e, namespaces, {3109 : NotOnHold})
        return True

class AvailabilityResponseParser(ResponseParser):

    def __init__(self, api, internal_format=None):
        """Constructor.

        :param api: An Axis360API instance, in case the parsing of an
        availability document triggers additional API requests.
        """
        self.api = api
        self.internal_format = internal_format
        super(AvailabilityResponseParser, self).__init__(api.collection)

    def process_all(self, string):
        for info in super(AvailabilityResponseParser, self).process_all(
                string, "//axis:title", self.NS):
            # Filter out books where nothing in particular is
            # happening.
            if info:
                yield info

    def process_one(self, e, ns):

        # Figure out which book we're talking about.
        axis_identifier = self.text_of_subtag(e, "axis:titleId", ns)
        availability = self._xpath1(e, 'axis:availability', ns)
        if availability is None:
            return None
        reserved = self._xpath1_boolean(availability, 'axis:isReserved', ns)
        checked_out = self._xpath1_boolean(availability, 'axis:isCheckedout', ns)
        on_hold = self._xpath1_boolean(availability, 'axis:isInHoldQueue', ns)

        info = None
        if checked_out:
            start_date = self._xpath1_date(
                availability, 'axis:checkoutStartDate', ns)
            end_date = self._xpath1_date(
                availability, 'axis:checkoutEndDate', ns)
            download_url = self.text_of_optional_subtag(
                availability, 'axis:downloadUrl', ns)
            transaction_id = self.text_of_optional_subtag(
                availability, 'axis:transactionID', ns) or ""
            isbn = self.text_of_optional_subtag(
                availability, 'axis:isbn', ns) or ""

            # Arguments common to FulfillmentInfo and
            # Axis360FulfillmentInfo.
            kwargs = dict(
                data_source_name=DataSource.AXIS_360,
                identifier_type=self.id_type,
                identifier=axis_identifier
            )

            if download_url and self.internal_format != self.api.AXISNOW:
                # The patron wants a direct link to the book, which we can deliver
                # immediately, without making any more API requests.
                fulfillment = FulfillmentInfo(
                    collection=self.collection,
                    content_link=download_url,
                    content_type=DeliveryMechanism.ADOBE_DRM,
                    content=None,
                    content_expires=None,
                    **kwargs
                )
            elif transaction_id:
                # We need to make another API request -- possibly two
                # requests -- to fulfill this book. Fortunately this
                # is possible since we have a transaction ID

                # This is an audiobook. If necessary we
                # are prepared to make another API request to
                # turn the transaction ID into a Findaway
                # audio manifest.
                fulfillment = Axis360FulfillmentInfo(
                    api=self.api, key=transaction_id, **kwargs
                )
            else:
                # We're out of luck -- we can't fulfill this loan.
                fulfillment = None
            info = LoanInfo(
                collection=self.collection,
                data_source_name=DataSource.AXIS_360,
                identifier_type=self.id_type,
                identifier=axis_identifier,
                start_date=start_date, end_date=end_date,
                fulfillment_info=fulfillment
            )

        elif reserved:
            end_date = self._xpath1_date(
                availability, 'axis:reservedEndDate', ns)
            info = HoldInfo(
                collection=self.collection,
                data_source_name=DataSource.AXIS_360,
                identifier_type=self.id_type,
                identifier=axis_identifier,
                start_date=None,
                end_date=end_date,
                hold_position=0
            )
        elif on_hold:
            position = self.int_of_optional_subtag(
                availability, 'axis:holdsQueuePosition', ns)
            info = HoldInfo(
                collection=self.collection,
                data_source_name=DataSource.AXIS_360,
                identifier_type=self.id_type,
                identifier=axis_identifier,
                start_date=None, end_date=None,
                hold_position=position)
        return info


class JSONResponseParser(ResponseParser):
    """Most ResponseParsers parse XML documents; subclasses of
    JSONResponseParser parse JSON documents.

    This only subclasses ResponseParser so it can reuse
    _raise_exception_on_error.
    """

    @classmethod
    def _required_key(cls, key, json_obj):
        """Raise an exception if the given key is not present in the given
        object.
        """
        if json_obj is None or key not in json_obj:
            raise RemoteInitiatedServerError(
                "Required key %s not present in Axis 360 fulfillment document: %s" % (
                    key, json_obj,
                ),
                cls.SERVICE_NAME
            )
        return json_obj[key]

    @classmethod
    def verify_status_code(cls, parsed):
        """Assert that the incoming JSON document represents a successful
        response.
        """
        k = cls._required_key
        status = k('Status', parsed)
        code = k('Code', status)
        message = status.get('Message')

        # If the document describes an error condition, raise
        # an appropriate exception immediately.
        cls._raise_exception_on_error(code, message)

    def parse(self, data, *args, **kwargs):
        """Parse a JSON document."""
        if isinstance(data, dict):
            parsed = data # already parsed
        else:
            try:
                parsed = json.loads(data)
            except ValueError, e:
                # It's not JSON.
                raise RemoteInitiatedServerError(
                    "Invalid response from Axis 360 (was expecting JSON): %s" % data,
                    self.SERVICE_NAME
                )

        # If the response indicates an error condition, don't continue --
        # raise an exception immediately.
        self.verify_status_code(parsed)
        return self._parse(parsed, *args, **kwargs)

    def _parse(self, parsed, *args, **kwargs):
        """Parse a document we know to represent success on the
        API level. Called by parse() once the high-level details
        have been worked out.
        """
        raise NotImplementedError()


class Axis360FulfillmentInfoResponseParser(JSONResponseParser):
    """Parse JSON documents into Findaway audiobook manifests or AxisNow manifests."""

    def __init__(self, api):
        """Constructor.

        :param api: An Axis360API instance, in case the parsing of
        a fulfillment document triggers additional API requests.
        """
        self.api = api
        super(Axis360FulfillmentInfoResponseParser, self).__init__(
            self.api.collection
        )

    def _parse(self, parsed, license_pool):
        """Extract all useful information from a parsed FulfillmentInfo
        response.

        :param parsed: A dictionary corresponding to a parsed JSON
        document.

        :param license_pool: The LicensePool for the book that's
        being fulfilled.

        :return: A 2-tuple (manifest, expiration_date). `manifest` is either
            a FindawayManifest (for an audiobook) or an AxisNowManifest (for an ebook).
        """
        set_trace()
        if 'FNDTransactionID' in parsed:
            manifest = self.parse_findaway(parsed)
        else:
            manifest = self.parse_axisnow(parsed)

        expiration_date = self._required_key('ExpirationDate', parsed)

        expiration_date = self.parse_date(expiration_date)
        return manifest, expiration_date

    def parse_date(self, date):
        if '.' in date:
            # Remove 7(?!) decimal places of precision and
            # UTC timezone, which are more trouble to parse
            # than they're worth.
            date = date[:date.rindex('.')]

        try:
            date = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            raise RemoteInitiatedServerError(
                "Could not parse expiration date: %s" % date,
                self.SERVICE_NAME
            )
        return date


    def parse_findaway(self, parsed):
        k = self._required_key
        fulfillmentId = k('FNDContentID', parsed)
        licenseId = k('FNDLicenseID', parsed)
        sessionKey = k('FNDSessionKey', parsed)

        # Acquire the TOC information
        metadata_response = self.api.get_audiobook_metadata(fulfillmentId)
        parser = AudiobookMetadataParser(self.api.collection)
        accountId, spine_items = parser.parse(metadata_response.content)

        return FindawayManifest(
            license_pool, accountId=accountId, checkoutId=checkoutId,
            fulfillmentId=fulfillmentId, licenseId=licenseId,
            sessionKey=sessionKey, spine_items=spine_items
        )

    def parse_axisnow(self, parsed):
        k = self._required_key
        isbn = k('ISBN', parsed)
        book_vault_uuid = k('BookVaultUUID', parsed)
        return AxisNowManifest(isbn, book_vault_uuid)


class AudiobookMetadataParser(JSONResponseParser):
    """Parse the results of Axis 360's audiobook metadata API call.
    """

    @classmethod
    def _parse(cls, parsed):
        spine_items = []
        accountId = parsed.get('fndaccountid', None)
        for item in parsed.get('readingOrder', []):
            spine_item = cls._extract_spine_item(item)
            if spine_item:
                spine_items.append(spine_item)
        return accountId, spine_items

    @classmethod
    def _extract_spine_item(cls, part):
        """Convert an element of the 'readingOrder' list to a SpineItem."""
        title = part.get('title')
        # Incoming duration is measured in seconds.
        duration = part.get('duration', 0)
        part_number = int(part.get('fndpart', 0))
        sequence = int(part.get('fndsequence', 0))
        return SpineItem(title, duration, part_number, sequence)


class AxisNowManifest(object):

    MEDIA_TYPE = MediaTypes.AXISNOW_MANIFEST_MEDIA_TYPE

    def __init__(self, isbn, book_vault_uuid):
        self.isbn = isbn
        self.book_vault_uuid = book_vault_uuid

    def __unicode__(self):
        return json.dumps(dict(isbn=self.isbn, book_vault_uuid=self.book_vault_uuid))


class Axis360FulfillmentInfo(APIAwareFulfillmentInfo):
    """An Axis 360-specific FulfillmentInfo implementation for audiobooks.

    We use these instead of normal FulfillmentInfo objects because
    generating a real FulfillmentInfo object would require two extra
    HTTP requests, and there's often no need to make those requests.
    """
    def do_fetch(self):
        _db = self.api._db
        license_pool = self.license_pool(_db)
        transaction_id = self.key
        response = self.api.get_fulfillment_info(transaction_id)
        set_trace()
        parser = Axis360FulfillmentInfoResponseParser(self.api)
        manifest, expires = parser.parse(response.content, license_pool)
        self._content = unicode(manifest)
        self._content_type = manifest.MEDIA_TYPE
        self._content_expires = expires
