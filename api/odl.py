from nose.tools import set_trace

import base64
import json
import uuid
import datetime
from flask_babel import lazy_gettext as _
import urlparse
from collections import defaultdict

from sqlalchemy.sql.expression import or_

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
    Hold,
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
    HoldInfo,
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
        },
        {
            "key": Collection.DEFAULT_RESERVATION_PERIOD_KEY,
            "label": _("Default Reservation Period (in Days)"),
            "description": _("The number of days a patron has to check out a book after a hold becomes available."),
            "type": "number",
            "default": Collection.STANDARD_DEFAULT_RESERVATION_PERIOD,
        },
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

        _db = Session.object_session(patron)

        loan = _db.query(Loan).filter(
            Loan.patron==patron
        ).filter(
            Loan.license_pool_id==licensepool.id
        )
        if loan.count() > 0:
            raise AlreadyCheckedOut()

        # Make sure pool info is updated.
        self.update_hold_queue(licensepool)

        hold = get_one(_db, Hold, patron=patron, license_pool_id=licensepool.id)
        if hold:
            self._update_hold_end_date(hold)

        # If there's a holds queue, the patron must be at position 0 and have a
        # non-expired hold to check out the book.
        if ((not hold or
             hold.position > 0 or
             (hold.end and hold.end < datetime.datetime.utcnow())) and
            licensepool.licenses_available < 1
            ):
            raise NoAvailableCopies()

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

        external_identifier = doc.get("links", {}).get("self", {}).get("href")
        if not external_identifier:
            _db.delete(loan)
            raise CannotLoan()

        start = datetime.datetime.utcnow()
        expires = doc.get("potential_rights", {}).get("end")
        if expires:
            expires = datetime.datetime.strptime(expires, self.TIME_FORMAT)

        # We need to set the start and end dates on our local loan since
        # the code that calls this only sets them when a new loan is created.
        loan.start = start
        loan.end = expires

        # We have successfully borrowed this book.
        if hold:
            _db.delete(hold)
        self.update_hold_queue(licensepool)

        return LoanInfo(
            licensepool.collection,
            licensepool.data_source.name,
            licensepool.identifier.type,
            licensepool.identifier.identifier,
            start,
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

    def _count_holds_before(self, hold):
        # Count holds on the license pool that started before this hold and
        # aren't expired.
        _db = Session.object_session(hold)
        return _db.query(Hold).filter(
            Hold.license_pool_id==hold.license_pool_id
        ).filter(
            Hold.start<hold.start
        ).filter(
            or_(
                Hold.end==None,
                Hold.end>datetime.datetime.utcnow(),
                Hold.position>0,
            )
        ).count()

    def _update_hold_end_date(self, hold):
        _db = Session.object_session(hold)
        pool = hold.license_pool

        # First make sure the hold position is up-to-date, since we'll
        # need it to calculate the end date.
        original_position = hold.position
        self._update_hold_position(hold)

        default_loan_period = self.collection(_db).default_loan_period(
            hold.patron.library
        )
        default_reservation_period = self.collection(_db).default_reservation_period

        # If the hold was already to check out and already has an end date,
        # it doesn't need an update.
        if hold.position == 0 and original_position == 0 and hold.end:
            return

        # If the patron is in the queue, we need to estimate when the book
        # will be available for check out. We can do slightly better than the
        # default calculation since we know when all current loans will expire,
        # but we're still calculating the worst case.
        elif hold.position > 0:
            # Find the current loans and reserved holds for the licenses.
            current_loans = _db.query(Loan).filter(
                Loan.license_pool_id==pool.id
            ).filter(
                or_(
                    Loan.end==None,
                    Loan.end>datetime.datetime.utcnow()
                )
            ).order_by(Loan.start).all()
            current_holds = _db.query(Hold).filter(
                Hold.license_pool_id==pool.id
            ).filter(
                or_(
                    Hold.end==None,
                    Hold.end>datetime.datetime.utcnow(),
                    Hold.position>0,
                )
            ).order_by(Hold.start).all()
            licenses_reserved = min(pool.licenses_owned - len(current_loans), len(current_holds))
            current_reservations = current_holds[:licenses_reserved]

            # The licenses will have to go through some number of cycles
            # before one of them gets to this hold. This leavs out the first cycle -
            # it's already started so we'll handle it separately.
            cycles = (hold.position - licenses_reserved - 1) / pool.licenses_owned

            # Each of the owned licenses is currently either on loan or reserved.
            # Figure out which license this hold will eventually get if every
            # patron keeps their loans and holds for the maximum time.
            copy_index = (hold.position - licenses_reserved - 1)  % pool.licenses_owned

            # In the worse case, the first cycle ends when a current loan expires, or
            # after a current reservation is checked out and then expires.
            if len(current_loans) > copy_index:
                next_cycle_start = current_loans[copy_index].end
            else:
                reservation = current_reservations[copy_index - len(current_loans)]
                next_cycle_start = reservation.end + datetime.timedelta(days=default_loan_period)

            # Assume all cycles after the first cycle take the maximum time.
            cycle_period = default_loan_period + default_reservation_period
            hold.end = next_cycle_start + datetime.timedelta(days=(cycle_period * cycles))

        # If the end date isn't set yet or the position just became 0, the
        # hold just became available. The patron's reservation period starts now.
        else:
            hold.end = datetime.datetime.utcnow() + datetime.timedelta(days=default_reservation_period)

    def _update_hold_position(self, hold):
        _db = Session.object_session(hold)
        pool = hold.license_pool
        loans_count = _db.query(Loan).filter(
            Loan.license_pool_id==pool.id,
        ).filter(
            or_(
                Loan.end==None,
                Loan.end > datetime.datetime.utcnow()
            )
        ).count()
        holds_count = self._count_holds_before(hold)

        remaining_licenses = pool.licenses_owned - loans_count

        if remaining_licenses > holds_count:
            # The hold is ready to check out.
            hold.position = 0

        else:
            # Add 1 since position 0 indicates the hold is ready.
            hold.position = holds_count + 1

    def update_hold_queue(self, licensepool):
        # Update the pool and the next holds in the queue when a license is reserved.
        _db = Session.object_session(licensepool)

        loans_count = _db.query(Loan).filter(
            Loan.license_pool_id==licensepool.id
        ).filter(
            or_(
                Loan.end==None,
                Loan.end>datetime.datetime.utcnow()
            )
        ).count()
        remaining_licenses = licensepool.licenses_owned - loans_count

        holds = _db.query(Hold).filter(
            Hold.license_pool_id==licensepool.id
        ).filter(
            or_(
                Hold.end==None,
                Hold.end>datetime.datetime.utcnow(),
                Hold.position>0,
            )
        ).order_by(
            Hold.start
        ).all()

        if len(holds) > remaining_licenses:
            licensepool.licenses_available = 0
            licensepool.licenses_reserved = remaining_licenses
            licensepool.patrons_in_hold_queue = len(holds)
        else:
            licensepool.licenses_available = remaining_licenses - len(holds)
            licensepool.licenses_reserved = len(holds)
            licensepool.patrons_in_hold_queue = len(holds)

        for hold in holds[:licensepool.licenses_reserved]:
            if hold.position != 0:
                # This hold just got a reserved license.
                self._update_hold_end_date(hold)

    def place_hold(self, patron, pin, licensepool, notification_email_address):
        """Create a new hold."""
        _db = Session.object_session(patron)

        # Make sure pool info is updated.
        self.update_hold_queue(licensepool)

        if licensepool.licenses_available > 0:
            raise CurrentlyAvailable()

        # Create local hold.
        hold, is_new = get_one_or_create(
            _db, Hold,
            license_pool=licensepool,
            patron=patron,
            create_method_kwargs=dict(start=datetime.datetime.utcnow()),
        )

        if not is_new:
            raise AlreadyOnHold()

        licensepool.patrons_in_hold_queue += 1
        self._update_hold_end_date(hold)

        return HoldInfo(
            licensepool.collection,
            licensepool.data_source.name,
            licensepool.identifier.type,
            licensepool.identifier.identifier,
            start_date=hold.start,
            end_date=hold.end,
            hold_position=hold.position,
        )

    def release_hold(self, patron, pin, licensepool):
        """Cancel a hold."""
        _db = Session.object_session(patron)

        hold = get_one(
            _db, Hold,
            license_pool_id=licensepool.id,
            patron=patron,
        )
        if not hold:
            raise NotOnHold()

        # If the book was ready and the patron revoked the hold instead
        # of checking it out, but no one else had the book on hold, the
        # book is now available for anyone to check out. If someone else
        # had a hold, the license is now reserved for the next patron.
        # If someone else had a hold, the license is now reserved for the
        # next patron, and we need to update that hold.
        _db.delete(hold)
        self.update_hold_queue(licensepool)
        return True

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

        # Get the patron's holds. If there are any expired holds, delete them.
        # Update the end date and position for the remaining holds.
        holds = _db.query(Hold).join(Hold.license_pool).filter(
            LicensePool.collection_id==self.collection_id
        ).filter(
            Hold.patron==patron
        )
        remaining_holds = []
        for hold in holds:
            if hold.end and hold.end < datetime.datetime.utcnow():
                _db.delete(hold)
                self.update_hold_queue(hold.license_pool)
            else:
                self._update_hold_end_date(hold)
                remaining_holds.append(hold)

        return [
            LoanInfo(
                loan.license_pool.collection,
                loan.license_pool.data_source.name,
                loan.license_pool.identifier.type,
                loan.license_pool.identifier.identifier,
                loan.start,
                loan.end,
                external_identifier=loan.external_identifier,
            ) for loan in loans
        ] + [
            HoldInfo(
                hold.license_pool.collection,
                hold.license_pool.data_source.name,
                hold.license_pool.identifier.type,
                hold.license_pool.identifier.identifier,
                start_date=hold.start,
                end_date=hold.end,
                hold_position=hold.position,
            ) for hold in remaining_holds
        ]

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

        # Update licenses reserved if there are holds.
        if len(pool.holds) > 0 and pool.licenses_available > 0:
            self.update_hold_queue(pool)

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

            # If there are holds, the license is reserved for the next patron.
            _db.delete(loan)
            self.update_hold_queue(loan.license_pool)

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

class ODLHoldReaper(CollectionMonitor):
    """Check for holds that have expired and delete them, and update
    the holds queues for their pools."""

    SERVICE_NAME = "ODL Hold Reaper"
    PROTOCOL = ODLWithConsolidatedCopiesAPI.NAME

    def __init__(self, _db, collection=None, api=None, **kwargs):
        super(ODLHoldReaper, self).__init__(_db, collection, **kwargs)
        self.api = api or ODLWithConsolidatedCopiesAPI(_db, collection)

    def run_once(self, start, cutoff):
        # Find holds that have expired.
        expired_holds = self._db.query(Hold).join(
            Hold.license_pool
        ).filter(
            LicensePool.collection_id==self.api.collection_id
        ).filter(
            Hold.end<datetime.datetime.utcnow()
        ).filter(
            Hold.position==0
        )

        changed_pools = set()
        for hold in expired_holds:
            changed_pools.add(hold.license_pool)
            self._db.delete(hold)

        for pool in changed_pools:
            self.api.update_hold_queue(pool)


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

