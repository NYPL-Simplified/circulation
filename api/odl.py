from nose.tools import set_trace

import base64
import json
import uuid
import datetime
from flask.ext.babel import lazy_gettext as _
import urlparse

from core.opds_import import (
    OPDSXMLParser,
    OPDSImporter,
    OPDSImportMonitor,
)
from core.monitor import CollectionMonitor
from core.model import (
    Collection,
    Credential,
    DataSource,
    DeliveryMechanism,
    ExternalIntegration,
    Identifier,
    LicensePool,
    Loan,
    RightsStatus,
    Session,
    get_one,
    get_one_or_create,
)
from core.metadata_layer import (
    CirculationData,
    FormatData,
    IdentifierData,
    ReplacementPolicy,
)
from circulation import (
    BaseCirculationAPI,
    LoanInfo,
    FulfillmentInfo,
)
from core.analytics import Analytics
from core.util.http import (
    HTTP,
    BadResponseException,
)
from flask import url_for
from core.testing import (
    DatabaseTest,
    MockRequestsResponse,
)
from circulation_exceptions import *

class ODLWithConsolidatedCopiesAPI(BaseCirculationAPI):
    """ODL (Open Distribution to Libraries) is a specification that allows
    libraries to manage their own loans and holds. It offers a deeper level
    of control to the library, but implementing full ODL support will require
    changing the circulation manager to keep track of individual copies
    rather than license pools, and manage its own holds queues.

    'ODL With Consolidated Copies' builds on ODL to provide an API that is
    more consistent with what other distributors provide. In addition to an
    ODL feed, the 'ODL With Consolidated Copies' distributor provides an endpoint
    to get a consolidated copies feed. Each consolidated copy has the total number of
    licenses owned and available across all the library's copies. In addition, the
    distributor provides an endpoint to create a loan for a consolidated copy, rather
    than an individual copy. That endpoint returns an License Status Document
    (https://readium.github.io/readium-lsd-specification/) and can also be used to
    check the status of an existing loan.

    Holds are not supported yet.

    When the circulation manager has full ODL support, the consolidated copies
    code can be removed.
    """

    NAME = "ODL with Consolidated Copies"
    DESCRIPTION = _("Import books from a distributor that uses ODL (Open Distribution to Libraries) and has a consolidated copies API.")
    CONSOLIDATED_COPIES_URL_KEY = "consolidated_copies_url"
    CONSOLIDATED_LOAN_URL_KEY = "consolidated_loan_url"

    SETTINGS = [
        {
            "key": Collection.EXTERNAL_ACCOUNT_ID_KEY,
            "label": _("Metadata URL (ODL feed)"),
        },
        {
            "key": CONSOLIDATED_COPIES_URL_KEY,
            "label": _("Consolidated Copies URL"),
        },
        {
            "key": CONSOLIDATED_LOAN_URL_KEY,
            "label": _("Consolidated Loan URL"),
        },
        {
            "key": ExternalIntegration.USERNAME,
            "label": _("Library's API username"),
        },
        {
            "key": ExternalIntegration.PASSWORD,
            "label": _("Library's API password"),
        },
        {
            "key": Collection.DATA_SOURCE_NAME_SETTING,
            "label": _("Data source name"),
        }
    ]

    LIBRARY_SETTINGS = BaseCirculationAPI.LIBRARY_SETTINGS + [
        BaseCirculationAPI.EBOOK_LOAN_DURATION_SETTING
    ]

    SET_DELIVERY_MECHANISM_AT = BaseCirculationAPI.FULFILL_STEP

    TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

    # Possible status values in the License Status Document:

    # The license is available but the user hasn't fulfilled it yet.
    READY_STATUS = "ready"

    # The license is available and has been fulfilled on at least one device.
    ACTIVE_STATUS = "active"

    # The license has been revoked by the distributor.
    REVOKED_STATUS = "revoked"

    # The license has been returned early by the user.
    RETURNED_STATUS = "returned"

    # The license was returned early and was never fulfilled.
    CANCELLED_STATUS = "cancelled"

    # The license has expired.
    EXPIRED_STATUS = "expired"

    STATUS_VALUES = [
        READY_STATUS,
        ACTIVE_STATUS,
        REVOKED_STATUS,
        RETURNED_STATUS,
        CANCELLED_STATUS,
        EXPIRED_STATUS,
    ]

    def __init__(self, _db, collection):
        if collection.protocol != self.NAME:
            raise ValueError(
                "Collection protocol is %s, but passed into ODLWithConsolidatedCopiesAPI!" %
                collection.protocol
            )
        self.collection_id = collection.id
        self.data_source_name = collection.external_integration.setting(Collection.DATA_SOURCE_NAME_SETTING).value
        # Create the data source if it doesn't exist yet.
        DataSource.lookup(_db, self.data_source_name, autocreate=True)

        self.username = collection.external_integration.username
        self.password = collection.external_integration.password
        self.consolidated_loan_url = collection.external_integration.setting(self.CONSOLIDATED_LOAN_URL_KEY).value

    def internal_format(self, delivery_mechanism):
        """Each consolidated copy is only available in one format, so we don't need
        a mapping to internal formats.
        """
        return delivery_mechanism

    def collection(self, _db):
        return get_one(_db, Collection, id=self.collection_id)

    def _get(self, url, headers=None):
        """Make a normal HTTP request, but include an authentication
        header with the credentials for the collection.
        """

        username = self.username
        password = self.password
        headers = dict(headers or {})
        auth_header = "Basic %s" % base64.b64encode("%s:%s" % (username, password))
        headers['Authorization'] = auth_header

        return HTTP.get_with_timeout(url, headers=headers)

    def _url_for(self, *args, **kwargs):
        """Wrapper around flask's url_for to be overridden for tests.
        """
        return url_for(*args, **kwargs)

    def get_license_status_document(self, loan):
        """Get the License Status Document for a loan.

        For a new loan, create a local loan with no external identifier and
        pass it in to this method.

        This will create the remote loan if one doesn't exist yet. The loan's
        internal database id will be used to receive notifications from the
        distributor when the loan's status changes.
        """
        _db = Session.object_session(loan)

        if loan.external_identifier:
            url = loan.external_identifier
        else:
            id = loan.license_pool.identifier.identifier
            checkout_id = str(uuid.uuid1())
            default_loan_period = self.collection(_db).default_loan_period(
                loan.patron.library
            )
            expires = datetime.datetime.utcnow() + datetime.timedelta(
                days=default_loan_period
            )
            # The patron UUID is generated randomly on each loan, so the distributor
            # doesn't know when multiple loans come from the same patron.
            patron_id = str(uuid.uuid1())
            notification_url = self._url_for(
                "odl_notify",
                library_short_name=loan.patron.library.short_name,
                loan_id=loan.id,
                _external=True,
            )

            params = dict(
                url=self.consolidated_loan_url,
                id=id,
                checkout_id=checkout_id,
                patron_id=patron_id,
                expires=(expires.isoformat() + 'Z'),
                notification_url=notification_url,
            )
            url = "%(url)s?id=%(id)s&checkout_id=%(checkout_id)s&patron_id=%(patron_id)s&expires=%(expires)s&notification_url=%(notification_url)s" % params

        response = self._get(url)

        try:
            status_doc = json.loads(response.content)
        except ValueError, e:
            raise BadResponseException(url, "License Status Document was not valid JSON.")
        if status_doc.get("status") not in self.STATUS_VALUES:
            raise BadResponseException(url, "License Status Document had an unknown status value.")
        return status_doc

    def checkin(self, patron, pin, licensepool):
        """Return a loan early."""
        _db = Session.object_session(patron)

        loan = _db.query(Loan).filter(
            Loan.patron==patron
        ).filter(
            Loan.license_pool_id==licensepool.id
        )
        if loan.count() < 1:
            raise NotCheckedOut()
        loan = loan.one()

        doc = self.get_license_status_document(loan)
        status = doc.get("status")
        if status in [self.REVOKED_STATUS, self.RETURNED_STATUS, self.CANCELLED_STATUS, self.EXPIRED_STATUS]:
            # This loan was already returned early or revoked by the distributor, or it expired.
            self.update_loan(loan, doc)
            raise NotCheckedOut()
        elif status == self.ACTIVE_STATUS:
            # This loan has already been fulfilled, so it needs to be returned through the DRM system.
            # Do nothing.
            return

        return_url = doc.get("links", {}).get("return", {}).get("href")
        if return_url:
            # Hit the distributor's return link.
            self._get(return_url)
            # Get the status document again to make sure the return was successful,
            # and if so update the pool availability and delete the local loan.
            self.update_loan(loan)
        else:
            # The distributor didn't provide a link to return this loan.
            raise CannotReturn()

    def checkout(self, patron, pin, licensepool, internal_format):
        """Create a new loan."""
        if licensepool.licenses_owned < 1:
            raise NoLicenses()

        if licensepool.licenses_available < 1:
            raise NoAvailableCopies()

        _db = Session.object_session(patron)
        loan = _db.query(Loan).filter(
            Loan.patron==patron
        ).filter(
            Loan.license_pool_id==licensepool.id
        )
        if loan.count() > 0:
            raise AlreadyCheckedOut()

        # Create a local loan so it's database id can be used to
        # receive notifications from the distributor.
        loan, ignore = get_one_or_create(_db, Loan, patron=patron, license_pool_id=licensepool.id)

        doc = self.get_license_status_document(loan)
        status = doc.get("status")

        if status not in [self.READY_STATUS, self.ACTIVE_STATUS]:
            # Something went wrong with this loan and we don't actually
            # have the book checked out. This should never happen.
            # Remove the loan we created.
            _db.delete(loan)
            raise CannotLoan()

        # We have successfully borrowed this book.
        licensepool.licenses_available -= 1

        external_identifier = doc.get("links", {}).get("self", {}).get("href")
        if not external_identifier:
            _db.delete(loan)
            raise CannotLoan()
        expires = doc.get("potential_rights", {}).get("end")
        if expires:
            expires = datetime.datetime.strptime(expires, self.TIME_FORMAT)
        return LoanInfo(
            licensepool.collection,
            licensepool.data_source.name,
            licensepool.identifier.type,
            licensepool.identifier.identifier,
            datetime.datetime.utcnow(),
            expires,
            external_identifier=external_identifier,
        )

    def fulfill(self, patron, pin, licensepool, internal_format):
        _db = Session.object_session(patron)

        loan = _db.query(Loan).filter(
            Loan.patron==patron
        ).filter(
            Loan.license_pool_id==licensepool.id
        )
        loan = loan.one()

        doc = self.get_license_status_document(loan)
        status = doc.get("status")

        if status not in [self.READY_STATUS, self.ACTIVE_STATUS]:
            # This loan isn't available for some reason. It's possible
            # the distributor revoked it or the patron already returned it
            # through the DRM system, and we didn't get a notification
            # from the distributor yet.
            self.update_loan(loan, doc)
            raise CannotFulfill()

        expires = doc.get("potential_rights", {}).get("end")
        expires = datetime.datetime.strptime(expires, self.TIME_FORMAT)
        content_link = doc.get("links", {}).get("license", {}).get("href")
        content_type = doc.get("links", {}).get("license", {}).get("type")
        return FulfillmentInfo(
            licensepool.collection,
            licensepool.data_source.name,
            licensepool.identifier.type,
            licensepool.identifier.identifier,
            content_link,
            content_type,
            None,
            expires,
        )

    def place_hold(self, patron, pin, licensepool, notification_email_address):
        """Holds aren't supported yet, so attempting to place one will
        raise an exception.
        """
        raise CannotHold()

    def release_hold(self, patron, pin, licensepool):
        """Holds aren't supported yet, so attempting to release one will
        raise an exception.
        """
        raise CannotReleaseHold()

    def patron_activity(self, patron, pin):
        """Look up non-expired loans for this collection in the database."""
        _db = Session.object_session(patron)
        loans = _db.query(Loan).join(Loan.license_pool).filter(
            LicensePool.collection_id==self.collection_id
        ).filter(
            Loan.patron==patron
        ).filter(
            Loan.end>=datetime.datetime.utcnow()
        )

        return [
            LoanInfo(
                loan.license_pool.collection,
                loan.license_pool.data_source.name,
                loan.license_pool.identifier.type,
                loan.license_pool.identifier.identifier,
                loan.start,
                loan.end,
                external_identifier=loan.external_identifier,
            ) for loan in loans]

    def update_consolidated_copy(self, _db, copy_info, analytics=None):
        """Process information about the current status of a consolidated
        copy from the consolidated copies feed.
        """
        identifier = copy_info.get("identifier")
        licenses = copy_info.get("licenses")
        available = copy_info.get("available")
            
        identifier_data = IdentifierData(Identifier.URI, identifier)
        circulation_data = CirculationData(
            data_source=self.data_source_name,
            primary_identifier=identifier_data,
            licenses_owned=licenses,
            licenses_available=available,
        )

        replacement_policy = ReplacementPolicy(analytics=analytics)
        pool, ignore = circulation_data.apply(_db, self.collection(_db), replacement_policy)

    def update_loan(self, loan, status_doc=None):
        """Check a loan's status, and if it is no longer active, delete the loan
        and update its pool's availability.
        """
        _db = Session.object_session(loan)

        if not status_doc:
            status_doc = self.get_license_status_document(loan)

        status = status_doc.get("status")
        # We already check that the status is valid in get_license_status_document,
        # but if the document came from a notification it hasn't been checked yet.
        if status not in self.STATUS_VALUES:
            raise BadResponseException("The License Status Document had an unknown status value.")

        if status in [self.REVOKED_STATUS, self.RETURNED_STATUS, self.CANCELLED_STATUS, self.EXPIRED_STATUS]:
            # This loan is no longer active. Update the pool's availability
            # and delete the loan.
            loan.license_pool.licenses_available += 1
            _db.delete(loan)
        

class ODLXMLParser(OPDSXMLParser):
    NAMESPACES = dict(OPDSXMLParser.NAMESPACES,
                      odl="http://opds-spec.org/odl")

class ODLBibliographicImporter(OPDSImporter):
    """Import bibliographic information and formats from an ODL feed.

    The only change from OPDSImporter is that this importer extracts
    format information from 'odl:license' tags.
    """
    NAME = ODLWithConsolidatedCopiesAPI.NAME
    PARSER_CLASS = ODLXMLParser

    @classmethod
    def _detail_for_elementtree_entry(cls, parser, entry_tag, feed_url=None):
        subtag = parser.text_of_optional_subtag
        data = OPDSImporter._detail_for_elementtree_entry(parser, entry_tag, feed_url)
        formats = []
        odl_license_tags = parser._xpath(entry_tag, 'odl:license') or []
        for odl_license_tag in odl_license_tags:
            content_type = subtag(odl_license_tag, 'dcterms:format')
            drm_schemes = []
            protection_tags = parser._xpath(odl_license_tag, 'odl:protection') or []
            for protection_tag in protection_tags:
                drm_scheme = subtag(protection_tag, 'dcterms:format')
                drm_schemes.append(drm_scheme)
            if not drm_schemes:
                formats.append(FormatData(
                    content_type=content_type,
                    drm_scheme=None,
                    rights_uri=RightsStatus.IN_COPYRIGHT,
                ))
            for drm_scheme in drm_schemes:
                formats.append(FormatData(
                    content_type=content_type,
                    drm_scheme=drm_scheme,
                    rights_uri=RightsStatus.IN_COPYRIGHT,
                ))
            if not data.get('circulation'):
                data['circulation'] = dict()
            if not data['circulation'].get('formats'):
                data['circulation']['formats'] = []
            data['circulation']['formats'].extend(formats)
        return data

class ODLBibliographicImportMonitor(OPDSImportMonitor):
    """Import bibliographic information from an ODL feed."""
    PROTOCOL = ODLBibliographicImporter.NAME
    SERVICE_NAME = "ODL Bibliographic Import Monitor"

    def __init__(self, _db, collection, import_class, **kwargs):
        super(ODLBibliographicImportMonitor, self).__init__(_db, collection, import_class, **kwargs)

        self.api = ODLWithConsolidatedCopiesAPI(_db, collection)

    def _get(self, url, headers):
        """Make a normal HTTP request, but add in an auth header
        with the credentials for the collection.
        """

        username = self.api.username
        password = self.api.password
        headers = dict(headers or {})
        auth_header = "Basic %s" % base64.b64encode("%s:%s" % (username, password))
        headers['Authorization'] = auth_header

        return super(ODLBibliographicImportMonitor, self)._get(url, headers)

class ODLConsolidatedCopiesMonitor(CollectionMonitor):
    """Monitor a consolidated copies feed for circulation information changes.

    This is primarily used to set up availability information when new copies
    are purchased by the library. When the availability of an existing copy
    changes, the circulation manager already knows, either because it made the
    change or because it received a notification from the distributor.

    If a book is returned or revoked outside the circulation manager, and this
    monitor hears about it before the circulation manager receives a notification,
    the license pool's availability will be incorrectly incremented when the
    notification arrives. Hopefully this will be rare, and it won't be a problem
    once we have full ODL support.
    """

    SERVICE_NAME = "ODL Consolidated Copies Monitor"
    PROTOCOL = ODLWithConsolidatedCopiesAPI.NAME

    # If it's the first time we're running this monitor, we need availability
    # information for every consolidated copy.
    DEFAULT_START_TIME = CollectionMonitor.NEVER

    OVERLAP = datetime.timedelta(minutes=5)

    def __init__(self, _db, collection=None, api=None, **kwargs):
        super(ODLConsolidatedCopiesMonitor, self).__init__(_db, collection, **kwargs)

        self.api = api or ODLWithConsolidatedCopiesAPI(_db, collection)
        self.start_url = collection.external_integration.setting(ODLWithConsolidatedCopiesAPI.CONSOLIDATED_COPIES_URL_KEY).value
        self.analytics = Analytics(_db)

    def run_once(self, start, cutoff):
        url = self.start_url
        if start:
            # Add a small overlap with the previous run to make sure
            # we don't miss anything.
            start = start - self.OVERLAP

            url += "?since=%s" % (start.isoformat() + 'Z')

        # Go through the consolidated copies feed until we get to a page
        # with no next link.
        while url:
            response = self.api._get(url)
            next_url = self.process_one_page(response)
            if next_url:
                # Make sure the next url is an absolute url.
                url = urlparse.urljoin(url, next_url)
            else:
                url = None

    def process_one_page(self, response):
        content = json.loads(response.content)

        # Process each copy in the response and return the next link
        # if there is one.
        next_url = None
        links = content.get("links") or []
        for link in links:
            if link.get("rel") == "next":
                next_url = link.get("href")

        copies = content.get("copies") or []
        for copy in copies:
            self.api.update_consolidated_copy(self._db, copy, self.analytics)

        return next_url


class MockODLWithConsolidatedCopiesAPI(ODLWithConsolidatedCopiesAPI):
    """Mock API for tests that overrides _get and _url_for and tracks requests."""

    @classmethod
    def mock_collection(self, _db):
        """Create a mock ODL collection to use in tests."""
        library = DatabaseTest.make_default_library(_db)
        collection, ignore = get_one_or_create(
            _db, Collection,
            name="Test ODL With Consolidated Copies Collection", create_method_kwargs=dict(
                external_account_id=u"http://odl",
            )
        )
        integration = collection.create_external_integration(
            protocol=ODLWithConsolidatedCopiesAPI.NAME
        )
        integration.username = u'a'
        integration.password = u'b'
        integration.url = u'http://metadata'
        integration.set_setting(ODLWithConsolidatedCopiesAPI.CONSOLIDATED_COPIES_URL_KEY, u'http://copies')
        integration.set_setting(ODLWithConsolidatedCopiesAPI.CONSOLIDATED_LOAN_URL_KEY, u'http://loan')
        library.collections.append(collection)
        return collection

    def __init__(self, _db, collection, *args, **kwargs):
        self.responses = []
        self.requests = []
        super(MockODLWithConsolidatedCopiesAPI, self).__init__(
            _db, collection, *args, **kwargs
        )

    def queue_response(self, status_code, headers={}, content=None):
        self.responses.insert(
            0, MockRequestsResponse(status_code, headers, content)
        )

    def _get(self, url, headers=None):
        self.requests.append([url, headers])
        response = self.responses.pop()
        return HTTP._process_response(url, response)

    def _url_for(self, *args, **kwargs):
        del kwargs["_external"]
        return "http://%s?%s" % ("/".join(args), "&".join(["%s=%s" % (key, val) for key, val in kwargs.items()]))

