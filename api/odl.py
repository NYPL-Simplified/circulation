from nose.tools import set_trace

import base64
import json
import uuid
import datetime
from flask_babel import lazy_gettext as _
import urlparse
from collections import defaultdict
import flask
from flask import Response
import feedparser
from lxml import etree
from problem_details import NO_LICENSES
from StringIO import StringIO
import re
from uritemplate import URITemplate

from sqlalchemy.sql.expression import or_

from core.opds_import import (
    OPDSXMLParser,
    OPDSImporter,
    OPDSImportMonitor,
)
from core.monitor import (
    CollectionMonitor,
    TimelineMonitor,
)
from core.model import (
    Collection,
    ConfigurationSetting,
    Credential,
    DataSource,
    DeliveryMechanism,
    Edition,
    ExternalIntegration,
    Hold,
    Hyperlink,
    Identifier,
    IntegrationClient,
    LicensePool,
    Loan,
    MediaTypes,
    RightsStatus,
    Session,
    create,
    get_one,
    get_one_or_create,
)
from core.metadata_layer import (
    CirculationData,
    FormatData,
    IdentifierData,
    LicenseData,
    TimestampData,
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
    RemoteIntegrationException,
)
from flask import url_for
from core.testing import (
    DatabaseTest,
    MockRequestsResponse,
)
from circulation_exceptions import *
from shared_collection import BaseSharedCollectionAPI

class ODLAPI(BaseCirculationAPI, BaseSharedCollectionAPI):
    """ODL (Open Distribution to Libraries) is a specification that allows
    libraries to manage their own loans and holds. It offers a deeper level
    of control to the library, but it requires the circulation manager to
    keep track of individual copies rather than just license pools, and
    manage its own holds queues.

    In addition to circulating books to patrons of a library on the current circulation
    manager, this API can be used to circulate books to patrons of external libraries.
    Only one circulation manager per ODL collection should use an ODLAPI
    - the others should use a SharedODLAPI and configure it to connect to the main
    circulation manager.
    """

    NAME = "ODL"
    DESCRIPTION = _("Import books from a distributor that uses ODL (Open Distribution to Libraries).")

    SETTINGS = [
        {
            "key": Collection.EXTERNAL_ACCOUNT_ID_KEY,
            "label": _("ODL feed URL"),
            "required": True,
            "format": "url",
        },
        {
            "key": ExternalIntegration.USERNAME,
            "label": _("Library's API username"),
            "required": True,
        },
        {
            "key": ExternalIntegration.PASSWORD,
            "label": _("Library's API password"),
            "required": True,
        },
        {
            "key": Collection.DATA_SOURCE_NAME_SETTING,
            "label": _("Data source name"),
            "required": True,
        },
        {
            "key": Collection.DEFAULT_RESERVATION_PERIOD_KEY,
            "label": _("Default Reservation Period (in Days)"),
            "description": _("The number of days a patron has to check out a book after a hold becomes available."),
            "type": "number",
            "default": Collection.STANDARD_DEFAULT_RESERVATION_PERIOD,
        },
    ] + BaseSharedCollectionAPI.SETTINGS

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
                "Collection protocol is %s, but passed into ODLAPI!" %
                collection.protocol
            )
        self.collection_id = collection.id
        self.data_source_name = collection.external_integration.setting(Collection.DATA_SOURCE_NAME_SETTING).value
        # Create the data source if it doesn't exist yet.
        DataSource.lookup(_db, self.data_source_name, autocreate=True)

        self.username = collection.external_integration.username
        self.password = collection.external_integration.password
        self.analytics = Analytics(_db)

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
            id = loan.license.identifier
            checkout_id = str(uuid.uuid1())
            if loan.patron:
                default_loan_period = self.collection(_db).default_loan_period(
                    loan.patron.library
                )
            else:
                # TODO: should integration clients be able to specify their own loan period?
                default_loan_period = self.collection(_db).default_loan_period(
                    loan.integration_client
                 )
            expires = datetime.datetime.utcnow() + datetime.timedelta(
                days=default_loan_period
            )
            # The patron UUID is generated randomly on each loan, so the distributor
            # doesn't know when multiple loans come from the same patron.
            patron_id = str(uuid.uuid1())

            if loan.patron:
                library_short_name = loan.patron.library.short_name
            else:
                # If this is for an integration client, choose an arbitrary library.
                library_short_name = self.collection(_db).libraries[0].short_name
            notification_url = self._url_for(
                "odl_notify",
                library_short_name=library_short_name,
                loan_id=loan.id,
                _external=True,
            )

            url_template = URITemplate(loan.license.checkout_url)
            url = url_template.expand(
                id=id,
                checkout_id=checkout_id,
                patron_id=patron_id,
                expires=(expires.isoformat() + 'Z'),
                notification_url=notification_url,
            )
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
        return self._checkin(loan)

    def _checkin(self, loan):
        _db = Session.object_session(loan)
        doc = self.get_license_status_document(loan)
        status = doc.get("status")
        if status in [self.REVOKED_STATUS, self.RETURNED_STATUS, self.CANCELLED_STATUS, self.EXPIRED_STATUS]:
            # This loan was already returned early or revoked by the distributor, or it expired.
            self.update_loan(loan, doc)
            raise NotCheckedOut()

        return_url = None
        links = doc.get("links", [])
        for link in links:
            if link.get("rel") == "return":
                return_url = link.get("href")
                break

        if not return_url:
            # The distributor didn't provide a link to return this loan.
            # This may be because the book has already been fulfilled and
            # must be returned through the DRM system. If that's true, the
            # app will already be doing that on its own, so we'll silently
            # do nothing.
            return

        # Hit the distributor's return link.
        self._get(return_url)
        # Get the status document again to make sure the return was successful,
        # and if so update the pool availability and delete the local loan.
        self.update_loan(loan)

        # At this point, if the loan still exists, something went wrong.
        # However, it might be because the loan has already been fulfilled
        # and must be returned through the DRM system, which the app will
        # do on its own, so we can ignore the problem.
        loan = get_one(_db, Loan, id=loan.id)
        if loan:
            return
        return True

    def checkout(self, patron, pin, licensepool, internal_format):
        """Create a new loan."""
        _db = Session.object_session(patron)

        loan = _db.query(Loan).filter(
            Loan.patron==patron
        ).filter(
            Loan.license_pool_id==licensepool.id
        )
        if loan.count() > 0:
            raise AlreadyCheckedOut()

        hold = get_one(_db, Hold, patron=patron, license_pool_id=licensepool.id)
        loan = self._checkout(patron, licensepool, hold)
        return LoanInfo(
            licensepool.collection,
            licensepool.data_source.name,
            licensepool.identifier.type,
            licensepool.identifier.identifier,
            loan.start,
            loan.end,
            external_identifier=loan.external_identifier,
        )

    def _checkout(self, patron_or_client, licensepool, hold=None):
        _db = Session.object_session(patron_or_client)

        unexpired_licenses = (l for l in licensepool.licenses if not l.is_expired)
        if not any(unexpired_licenses):
            raise NoLicenses()

        # Make sure pool info is updated.
        self.update_hold_queue(licensepool)

        if hold:
            self._update_hold_end_date(hold)

        # If there's a holds queue, the patron or client must have a non-expired hold
        # with position 0 to check out the book.
        if ((not hold or
             hold.position > 0 or
             (hold.end and hold.end < datetime.datetime.utcnow())) and
            licensepool.licenses_available < 1
            ):
            raise NoAvailableCopies()

        # Create a local loan so its database id can be used to
        # receive notifications from the distributor.
        license = licensepool.best_available_license()
        if not license:
            raise NoAvailableCopies()
        loan, ignore = license.loan_to(patron_or_client)

        doc = self.get_license_status_document(loan)
        status = doc.get("status")

        if status not in [self.READY_STATUS, self.ACTIVE_STATUS]:
            # Something went wrong with this loan and we don't actually
            # have the book checked out. This should never happen.
            # Remove the loan we created.
            _db.delete(loan)
            raise CannotLoan()

        links = doc.get("links", [])
        external_identifier = None
        for link in links:
            if link.get("rel") == "self":
                external_identifier = link.get("href")
                break
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
        loan.external_identifier = external_identifier

        # We also need to update the remaining checkouts for the license.
        if loan.license.remaining_checkouts:
            loan.license.remaining_checkouts = loan.license.remaining_checkouts - 1

        # We have successfully borrowed this book.
        if hold:
            _db.delete(hold)
        self.update_hold_queue(licensepool)
        return loan

    def fulfill(self, patron, pin, licensepool, internal_format, **kwargs):
        """Get the actual resource file to the patron.

        :param kwargs: A container for arguments to fulfill()
           which are not relevant to this vendor.

        :return: a FulfillmentInfo object.
        """
        _db = Session.object_session(patron)

        loan = _db.query(Loan).filter(
            Loan.patron==patron
        ).filter(
            Loan.license_pool_id==licensepool.id
        )
        loan = loan.one()
        return self._fulfill(loan)

    def _fulfill(self, loan):
        licensepool = loan.license_pool
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

        links = doc.get("links", [])
        content_link = None
        content_type = None
        for link in links:
            # Depending on the format being served, the crucial information
            # may be in 'manifest' or in 'license'.
            #
            # TODO: when both links are present, access to the
            # DeliveryMechanism would be great for figuring out which
            # one to use.
            if link.get("rel") in ("manifest", "license"):
                content_link = link.get("href")
                content_type = link.get("type")
                break

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
            hold.library or hold.integration_client
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
            new_licenses_available = 0
            new_licenses_reserved = remaining_licenses
            new_patrons_in_hold_queue = len(holds)
        else:
            new_licenses_available = remaining_licenses - len(holds)
            new_licenses_reserved = len(holds)
            new_patrons_in_hold_queue = len(holds)
        licensepool.update_availability(
            licensepool.licenses_owned,
            new_licenses_available,
            new_licenses_reserved,
            new_patrons_in_hold_queue,
            analytics=self.analytics,
            as_of=datetime.datetime.utcnow(),
        )

        for hold in holds[:licensepool.licenses_reserved]:
            if hold.position != 0:
                # This hold just got a reserved license.
                self._update_hold_end_date(hold)

    def place_hold(self, patron, pin, licensepool, notification_email_address):
        """Create a new hold."""
        hold = self._place_hold(patron, licensepool)
        return HoldInfo(
            licensepool.collection,
            licensepool.data_source.name,
            licensepool.identifier.type,
            licensepool.identifier.identifier,
            start_date=hold.start,
            end_date=hold.end,
            hold_position=hold.position,
        )

    def _place_hold(self, patron_or_client, licensepool):
        _db = Session.object_session(patron_or_client)

        # Make sure pool info is updated.
        self.update_hold_queue(licensepool)

        if licensepool.licenses_available > 0:
            raise CurrentlyAvailable()

        # Create local hold.
        hold, is_new = licensepool.on_hold_to(patron_or_client)

        if not is_new:
            raise AlreadyOnHold()

        licensepool.patrons_in_hold_queue += 1
        self._update_hold_end_date(hold)
        return hold

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
        return self._release_hold(hold)

    def _release_hold(self, hold):
        # If the book was ready and the patron revoked the hold instead
        # of checking it out, but no one else had the book on hold, the
        # book is now available for anyone to check out. If someone else
        # had a hold, the license is now reserved for the next patron.
        # If someone else had a hold, the license is now reserved for the
        # next patron, and we need to update that hold.
        _db = Session.object_session(hold)
        licensepool = hold.license_pool
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

    def checkout_to_external_library(self, client, licensepool, hold=None):
        try:
            return self._checkout(client, licensepool, hold)
        except NoAvailableCopies, e:
            return self._place_hold(client, licensepool)

    def checkin_from_external_library(self, client, loan):
        self._checkin(loan)

    def fulfill_for_external_library(self, client, loan, mechanism):
        return self._fulfill(loan)

    def release_hold_from_external_library(self, client, hold):
        return self._release_hold(hold)


class ODLXMLParser(OPDSXMLParser):
    NAMESPACES = dict(OPDSXMLParser.NAMESPACES,
                      odl="http://opds-spec.org/odl")

class ODLImporter(OPDSImporter):
    """Import information and formats from an ODL feed.

    The only change from OPDSImporter is that this importer extracts
    format information from 'odl:license' tags.
    """
    NAME = ODLAPI.NAME
    PARSER_CLASS = ODLXMLParser

    # The media type for a License Info Docuemnt, used to get information
    # about the license.
    LICENSE_INFO_DOCUMENT_MEDIA_TYPE = 'application/vnd.odl.info+json'

    @classmethod
    def _detail_for_elementtree_entry(cls, parser, entry_tag, feed_url=None, do_get=None):
        do_get = do_get or Representation.cautious_http_get

        # TODO: Review for consistency when updated ODL spec is ready.
        subtag = parser.text_of_optional_subtag
        data = OPDSImporter._detail_for_elementtree_entry(parser, entry_tag, feed_url)
        formats = []
        licenses = []

        licenses_owned = 0
        licenses_available = 0
        odl_license_tags = parser._xpath(entry_tag, 'odl:license') or []
        medium = None
        for odl_license_tag in odl_license_tags:
            identifier = subtag(odl_license_tag, 'dcterms:identifier')
            full_content_type = subtag(odl_license_tag, 'dcterms:format')

            if not medium:
                medium = Edition.medium_from_media_type(full_content_type)

            # By default, dcterms:format includes the media type of a
            # DRM-free resource.
            content_type = full_content_type
            drm_schemes = []

            # But it may instead describe an audiobook protected with
            # the Feedbooks access-control scheme.
            feedbooks_audio = "%s; protection=%s"  % (
                MediaTypes.AUDIOBOOK_MANIFEST_MEDIA_TYPE,
                DeliveryMechanism.FEEDBOOKS_AUDIOBOOK_DRM
            )
            if full_content_type == feedbooks_audio:
                content_type = MediaTypes.AUDIOBOOK_MANIFEST_MEDIA_TYPE
                drm_schemes.append(DeliveryMechanism.FEEDBOOKS_AUDIOBOOK_DRM)

            # Additional DRM schemes may be described in <odl:protection>
            # tags.
            protection_tags = parser._xpath(
                odl_license_tag, 'odl:protection'
            ) or []
            for protection_tag in protection_tags:
                drm_scheme = subtag(protection_tag, 'dcterms:format')
                if drm_scheme:
                    drm_schemes.append(drm_scheme)

            for drm_scheme in (drm_schemes or [None]):
                formats.append(
                    FormatData(
                        content_type=content_type,
                        drm_scheme=drm_scheme,
                        rights_uri=RightsStatus.IN_COPYRIGHT,
                    )
                )

            data['medium'] = medium

            expires = None
            remaining_checkouts = None
            available_checkouts = None
            concurrent_checkouts = None

            checkout_link = None
            for link_tag in parser._xpath(odl_license_tag, 'odl:tlink') or []:
                rel = link_tag.attrib.get("rel")
                if rel == Hyperlink.BORROW:
                    checkout_link = link_tag.attrib.get("href")
                    break

            # Look for a link to the License Info Document for this license.
            odl_status_link = None
            for link_tag in parser._xpath(odl_license_tag, 'atom:link') or []:
                attrib = link_tag.attrib
                rel = attrib.get("rel")
                type = attrib.get("type", "")
                if (rel == 'self'
                    and type.startswith(cls.LICENSE_INFO_DOCUMENT_MEDIA_TYPE)):
                    odl_status_link = attrib.get("href")
                    break

            # If we found one, retrieve it and get licensing information about this book.
            if odl_status_link:
                ignore, ignore, response = do_get(odl_status_link, headers={})
                status = json.loads(response)
                checkouts = status.get("checkouts", {})
                remaining_checkouts = checkouts.get("left")
                available_checkouts = checkouts.get("available")

            terms = parser._xpath(odl_license_tag, "odl:terms")
            if terms:
                concurrent_checkouts = subtag(terms[0], "odl:concurrent_checkouts")
                expires = subtag(terms[0], "odl:expires")

            licenses_owned += int(concurrent_checkouts or 0)
            licenses_available += int(available_checkouts or 0)

            licenses.append(LicenseData(
                identifier=identifier,
                checkout_url=checkout_link,
                status_url=odl_status_link,
                expires=expires,
                remaining_checkouts=remaining_checkouts,
                concurrent_checkouts=concurrent_checkouts,
            ))

        if not data.get('circulation'):
            data['circulation'] = dict()
        if not data['circulation'].get('formats'):
            data['circulation']['formats'] = []
        data['circulation']['formats'].extend(formats)
        if not data['circulation'].get('licenses'):
            data['circulation']['licenses'] = []
        data['circulation']['licenses'].extend(licenses)
        data['circulation']['licenses_owned'] = licenses_owned
        data['circulation']['licenses_available'] = licenses_available
        return data

class ODLImportMonitor(OPDSImportMonitor):
    """Import information from an ODL feed."""
    PROTOCOL = ODLImporter.NAME
    SERVICE_NAME = "ODL Import Monitor"

class ODLHoldReaper(CollectionMonitor):
    """Check for holds that have expired and delete them, and update
    the holds queues for their pools."""

    SERVICE_NAME = "ODL Hold Reaper"
    PROTOCOL = ODLAPI.NAME

    def __init__(self, _db, collection=None, api=None, **kwargs):
        super(ODLHoldReaper, self).__init__(_db, collection, **kwargs)
        self.api = api or ODLAPI(_db, collection)

    def run_once(self, progress):
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
        total_deleted_holds = 0
        for hold in expired_holds:
            changed_pools.add(hold.license_pool)
            self._db.delete(hold)
            total_deleted_holds += 1

        for pool in changed_pools:
            self.api.update_hold_queue(pool)

        message = "Holds deleted: %d. License pools updated: %d" % (
            total_deleted_holds,
            len(changed_pools)
        )
        progress = TimestampData(achievements=message)
        return progress

class MockODLAPI(ODLAPI):
    """Mock API for tests that overrides _get and _url_for and tracks requests."""

    @classmethod
    def mock_collection(self, _db):
        """Create a mock ODL collection to use in tests."""
        library = DatabaseTest.make_default_library(_db)
        collection, ignore = get_one_or_create(
            _db, Collection,
            name="Test ODL Collection", create_method_kwargs=dict(
                external_account_id=u"http://odl",
            )
        )
        integration = collection.create_external_integration(
            protocol=ODLAPI.NAME
        )
        integration.username = u'a'
        integration.password = u'b'
        integration.url = u'http://metadata'
        library.collections.append(collection)
        return collection

    def __init__(self, _db, collection, *args, **kwargs):
        self.responses = []
        self.requests = []
        super(MockODLAPI, self).__init__(
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


class SharedODLAPI(BaseCirculationAPI):
    """An API for circulation managers to use to connect to an ODL collection that's shared
    by another circulation manager.
    """
    NAME = "Shared ODL For Consortia"
    DESCRIPTION = _("Import books from an ODL collection that's hosted by another circulation manager in the consortium. If this circulation manager will be the main host for the collection, select %(odl_name)s instead.", odl_name=ODLAPI.NAME)

    SETTINGS = [
        {
            "key": Collection.EXTERNAL_ACCOUNT_ID_KEY,
            "label": _("Base URL"),
            "description": _("The base URL for the collection on the other circulation manager."),
            "required": True,
        },
        {
            "key": Collection.DATA_SOURCE_NAME_SETTING,
            "label": _("Data source name"),
            "required": True,
        },
    ]

    SUPPORTS_REGISTRATION = True
    SUPPORTS_STAGING = False

    TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

    def __init__(self, _db, collection):
        if collection.protocol != self.NAME:
            raise ValueError(
                "Collection protocol is %s, but passed into SharedODLPI!" %
                collection.protocol
            )
        self.collection_id = collection.id
        self.data_source_name = collection.external_integration.setting(Collection.DATA_SOURCE_NAME_SETTING).value
        # Create the data source if it doesn't exist yet.
        DataSource.lookup(_db, self.data_source_name, autocreate=True)

        self.base_url = collection.external_account_id

    def internal_format(self, delivery_mechanism):
        """Each consolidated copy is only available in one format, so we don't need
        a mapping to internal formats.
        """
        return delivery_mechanism

    def collection(self, _db):
        return get_one(_db, Collection, id=self.collection_id)

    def _get(self, url, headers=None, patron=None, allowed_response_codes=None, do_get=HTTP.get_with_timeout):
        """Make a normal HTTP request, but include an authentication
        header with the credentials for the collection.
        """

        allowed_response_codes = allowed_response_codes or ["2xx", "3xx"]
        patron = patron or flask.request.patron
        _db = Session.object_session(patron)
        collection = self.collection(_db)
        shared_secret = ConfigurationSetting.for_library_and_externalintegration(
            _db, ExternalIntegration.PASSWORD, patron.library, collection.external_integration
        ).value
        if not shared_secret:
            raise LibraryAuthorizationFailedException(_("Library %(library)s is not registered with the collection.", library=patron.library.name))
        headers = dict(headers or {})
        auth_header = 'Bearer ' + base64.b64encode(shared_secret)
        headers['Authorization'] = auth_header

        return do_get(url, headers=headers, allowed_response_codes=allowed_response_codes)

    def checkout(self, patron, pin, licensepool, internal_format):
        _db = Session.object_session(patron)

        loans = _db.query(Loan).filter(
            Loan.patron==patron
        ).filter(
            Loan.license_pool_id==licensepool.id
        )
        if loans.count() > 0:
            raise AlreadyCheckedOut()

        holds = _db.query(Hold).filter(
            Hold.patron==patron
        ).filter(
            Hold.license_pool_id==licensepool.id
        )
        if holds.count() > 0:
            hold = holds.one()
            try:
                hold_info_response = self._get(hold.external_identifier)
            except RemoteIntegrationException, e:
                raise CannotLoan()
            feed = feedparser.parse(unicode(hold_info_response.content))
            entries = feed.get("entries")
            if len(entries) < 1:
                raise CannotLoan()
            entry = entries[0]
            availability = entry.get("opds_availability", {})
            if availability.get("status") != "ready":
                raise NoAvailableCopies()
            checkout_links = [link for link in entry.get("links") if link.get("rel") == Hyperlink.BORROW]
            if len(checkout_links) < 1:
                raise NoAvailableCopies()
            checkout_url = checkout_links[0].get("href")
        else:
            borrow_links = [link for link in licensepool.identifier.links if link.rel == Hyperlink.BORROW]
            if not borrow_links:
                raise CannotLoan()
            checkout_url = borrow_links[0].resource.url
        try:
            response = self._get(checkout_url, allowed_response_codes=["2xx", "3xx", "403", "404"])
        except RemoteIntegrationException, e:
            raise CannotLoan()
        if response.status_code == 403:
            raise NoAvailableCopies()
        elif response.status_code == 404:
            if hasattr(response, 'json') and response.json().get('type', '') == NO_LICENSES.uri:
                raise NoLicenses()
        feed = feedparser.parse(unicode(response.content))
        entries = feed.get("entries")
        if len(entries) < 1:
            raise CannotLoan()
        entry = entries[0]
        availability = entry.get("opds_availability", {})
        start = datetime.datetime.strptime(availability.get("since"), self.TIME_FORMAT)
        end = datetime.datetime.strptime(availability.get("until"), self.TIME_FORMAT)
        # Get the loan base url from a link.
        info_links = [link for link in entry.get("links") if link.get("rel") == "self"]
        if len(info_links) < 1:
            raise CannotLoan()
        external_identifier = info_links[0].get("href")

        if availability.get("status") == "available":
            return LoanInfo(
                licensepool.collection,
                licensepool.data_source.name,
                licensepool.identifier.type,
                licensepool.identifier.identifier,
                start,
                end,
                external_identifier=external_identifier
            )
        elif availability.get("status") in ["ready", "reserved"]:
            # We tried to borrow this book but it wasn't available,
            # so we got a hold.
            position = entry.get("opds_holds", {}).get("position")
            if position:
                position = int(position)
            return HoldInfo(
                licensepool.collection,
                licensepool.data_source.name,
                licensepool.identifier.type,
                licensepool.identifier.identifier,
                start,
                end,
                hold_position=position,
                external_identifier=external_identifier
            )
        else:
            # We didn't get an error, but something went wrong and we don't have a
            # loan or hold either.
            raise CannotLoan()

    def checkin(self, patron, pin, licensepool):
        _db = Session.object_session(patron)

        loan = _db.query(Loan).filter(
            Loan.patron==patron
        ).filter(
            Loan.license_pool_id==licensepool.id
        )
        if loan.count() < 1:
            raise NotCheckedOut()
        loan = loan.one()

        info_url = loan.external_identifier
        try:
            response = self._get(info_url, allowed_response_codes=["2xx", "3xx", "404"])
        except RemoteIntegrationException, e:
            raise CannotReturn()
        if response.status_code == 404:
            raise NotCheckedOut()
        feed = feedparser.parse(unicode(response.content))
        entries = feed.get("entries")
        if len(entries) < 1:
            raise CannotReturn()
        entry = entries[0]
        revoke_links = [link for link in entry.get("links") if link.get("rel") == "http://librarysimplified.org/terms/rel/revoke"]
        if len(revoke_links) < 1:
            raise CannotReturn()
        revoke_url = revoke_links[0].get("href")
        try:
            self._get(revoke_url)
        except RemoteIntegrationException, e:
            raise CannotReturn()
        return True

    def fulfill(self, patron, pin, licensepool, internal_format, **kwargs):
        """Get the actual resource file to the patron.

        :param kwargs: A container for arguments to fulfill()
           which are not relevant to this vendor.

        :return: a FulfillmentInfo object.
        """
        _db = Session.object_session(patron)

        loan = _db.query(Loan).filter(
            Loan.patron==patron
        ).filter(
            Loan.license_pool_id==licensepool.id
        )
        if loan.count() < 1:
            raise NotCheckedOut()
        loan = loan.one()

        info_url = loan.external_identifier
        try:
            response = self._get(info_url, allowed_response_codes=["2xx", "3xx", "404"])
        except RemoteIntegrationException, e:
            raise CannotFulfill()
        if response.status_code == 404:
            raise NotCheckedOut()

        requested_content_type = internal_format.delivery_mechanism.content_type
        requested_drm_scheme = internal_format.delivery_mechanism.drm_scheme

        feed = feedparser.parse(unicode(response.content))
        entries = feed.get("entries")
        if len(entries) < 1:
            raise CannotFulfill()
        entry = entries[0]
        availability = entry.get("opds_availability")
        if availability.get("status") != "available":
            raise CannotFulfill()
        expires = datetime.datetime.strptime(availability.get("until"), self.TIME_FORMAT)

        # The entry is parsed with etree to get indirect acquisitions
        parser = SharedODLImporter.PARSER_CLASS()
        root = etree.parse(StringIO(unicode(response.content)))

        fulfill_url = SharedODLImporter.get_fulfill_url(response.content, requested_content_type, requested_drm_scheme)
        if not fulfill_url:
            raise FormatNotAvailable()

        # We need to hit the fulfill link here instead of returning it so we can
        # authenticate the library.
        try:
            response = self._get(fulfill_url)
        except RemoteIntegrationException, e:
            raise CannotFulfill()
        return FulfillmentInfo(
            licensepool.collection,
            licensepool.data_source.name,
            licensepool.identifier.type,
            licensepool.identifier.identifier,
            None,
            response.headers.get("Content-Type"),
            response.content,
            expires,
        )

    def place_hold(self, patron, pin, licensepool, notification_email_address):
        # Just try to check out the book. If it's not available, we'll get a hold.
        return self.checkout(patron, pin, licensepool, None)

    def release_hold(self, patron, pin, licensepool):
        _db = Session.object_session(patron)

        hold = get_one(
            _db, Hold,
            license_pool_id=licensepool.id,
            patron=patron,
        )
        if not hold:
            raise NotOnHold()

        info_url = hold.external_identifier
        try:
            response = self._get(info_url, allowed_response_codes=["2xx", "3xx", "404"])
        except RemoteIntegrationException, e:
            raise CannotReleaseHold()
        if response.status_code == 404:
            raise NotOnHold()
        feed = feedparser.parse(unicode(response.content))
        entries = feed.get("entries")
        if len(entries) < 1:
            raise CannotReleaseHold()
        entry = entries[0]
        availability = entry.get("opds_availability", {})
        if availability.get("status") not in ["reserved", "ready"]:
            raise CannotReleaseHold()
        revoke_links = [link for link in entry.get("links") if link.get("rel") == "http://librarysimplified.org/terms/rel/revoke"]
        if len(revoke_links) < 1:
            raise CannotReleaseHold()
        revoke_url = revoke_links[0].get("href")
        try:
            self._get(revoke_url)
        except RemoteIntegrationException, e:
            raise CannotReleaseHold()
        return True

    def patron_activity(self, patron, pin):
        _db = Session.object_session(patron)
        loans = _db.query(Loan).join(Loan.license_pool).filter(
            LicensePool.collection_id==self.collection_id
        ).filter(
            Loan.patron==patron
        )

        holds = _db.query(Hold).join(Hold.license_pool).filter(
            LicensePool.collection_id==self.collection_id
        ).filter(
            Hold.patron==patron
        )

        activity = []
        for loan in loans:
            info_url = loan.external_identifier
            response = self._get(info_url, patron=patron, allowed_response_codes=["2xx", "3xx", "404"])
            if response.status_code == 404:
                # 404 is returned when the loan has been deleted. Leave this loan out of the result.
                continue
            feed = feedparser.parse(unicode(response.content))
            entries = feed.get("entries")
            if len(entries) < 1:
                raise CirculationException()
            entry = entries[0]
            availability = entry.get("opds_availability", {})
            if availability.get("status") != "available":
                # This loan might be expired.
                continue
            start = datetime.datetime.strptime(availability.get("since"), self.TIME_FORMAT)
            end = datetime.datetime.strptime(availability.get("until"), self.TIME_FORMAT)

            activity.append(
                LoanInfo(
                    loan.license_pool.collection,
                    loan.license_pool.data_source.name,
                    loan.license_pool.identifier.type,
                    loan.license_pool.identifier.identifier,
                    start,
                    end,
                    external_identifier=loan.external_identifier,
                )
            )
        for hold in holds:
            info_url = hold.external_identifier
            response = self._get(info_url, patron=patron, allowed_response_codes=["2xx", "3xx", "404"])
            if response.status_code == 404:
                # 404 is returned when the hold has been deleted. Leave this hold out of the result.
                continue
            feed = feedparser.parse(unicode(response.content))
            entries = feed.get("entries")
            if len(entries) < 1:
                raise CirculationException()
            entry = entries[0]
            availability = entry.get("opds_availability", {})
            if availability.get("status") not in ["ready", "reserved"]:
                # This hold might be expired.
                continue
            start = datetime.datetime.strptime(availability.get("since"), self.TIME_FORMAT)
            end = datetime.datetime.strptime(availability.get("until"), self.TIME_FORMAT)
            position = entry.get("opds_holds", {}).get("position")

            activity.append(
                HoldInfo(
                    hold.license_pool.collection,
                    hold.license_pool.data_source.name,
                    hold.license_pool.identifier.type,
                    hold.license_pool.identifier.identifier,
                    start,
                    end,
                    hold_position=position,
                    external_identifier=hold.external_identifier,
                )
            )
        return activity


class SharedODLImporter(OPDSImporter):
    NAME = SharedODLAPI.NAME

    @classmethod
    def get_fulfill_url(cls, entry, requested_content_type, requested_drm_scheme):
        parser = cls.PARSER_CLASS()
        root = etree.parse(StringIO(unicode(entry)))

        fulfill_url = None
        for link_tag in parser._xpath(root, 'atom:link'):
            if link_tag.attrib.get("rel") == Hyperlink.GENERIC_OPDS_ACQUISITION:
                content_type = None
                drm_scheme = link_tag.attrib.get("type")

                indirect_acquisition = parser._xpath(link_tag, "opds:indirectAcquisition")
                if indirect_acquisition:
                    content_type = indirect_acquisition[0].get("type")
                else:
                    content_type = drm_scheme
                    drm_scheme = None

                if content_type == requested_content_type and drm_scheme == requested_drm_scheme:
                    fulfill_url = link_tag.attrib.get("href")
                    break
        return fulfill_url


    @classmethod
    def _detail_for_elementtree_entry(cls, parser, entry_tag, feed_url=None, do_get=None):
        data = OPDSImporter._detail_for_elementtree_entry(parser, entry_tag, feed_url)
        borrow_links = [link for link in data.get("links") if link.rel == Hyperlink.BORROW]

        licenses_available = 0
        licenses_owned = 0
        patrons_in_hold_queue = 0
        formats = []

        for link_tag in parser._xpath(entry_tag, 'atom:link'):
            if link_tag.attrib.get("rel") == Hyperlink.BORROW:
                content_type = None
                drm_scheme = None

                indirect_acquisition = parser._xpath(link_tag, "opds:indirectAcquisition")
                if indirect_acquisition:
                    drm_scheme = indirect_acquisition[0].attrib.get("type")

                    second_indirect_acquisition = parser._xpath(indirect_acquisition[0], "opds:indirectAcquisition")
                    if second_indirect_acquisition:
                        content_type = second_indirect_acquisition[0].attrib.get("type")
                    else:
                        content_type = drm_scheme
                        drm_scheme = None


                copies_tags = parser._xpath(link_tag, 'opds:copies')
                if copies_tags:
                    copies_tag = copies_tags[0]
                    licenses_available = copies_tag.attrib.get("available")
                    if licenses_available != None:
                        licenses_available = int(licenses_available)
                    licenses_owned = copies_tag.attrib.get("total")
                    if licenses_owned != None:
                        licenses_owned = int(licenses_owned)
                holds_tags = parser._xpath(link_tag, 'opds:holds')
                if holds_tags:
                    holds_tag = holds_tags[0]
                    patrons_in_hold_queue = holds_tag.attrib.get("total")
                    if patrons_in_hold_queue != None:
                        patrons_in_hold_queue = int(patrons_in_hold_queue)

                format = FormatData(
                    content_type=content_type,
                    drm_scheme=drm_scheme,
                    link=borrow_links[0],
                    rights_uri=RightsStatus.IN_COPYRIGHT,
                )
                formats.append(format)
        circulation = dict(
            licenses_available=licenses_available,
            licenses_owned=licenses_owned,
            patrons_in_hold_queue=patrons_in_hold_queue,
            formats=formats,
        )

        data['circulation'] = circulation
        return data

class SharedODLImportMonitor(OPDSImportMonitor):
    PROTOCOL = SharedODLImporter.NAME
    SERVICE_NAME = "Shared ODL Import Monitor"

    def opds_url(self, collection):
        base_url = collection.external_account_id
        return base_url + "/crawlable"

class MockSharedODLAPI(SharedODLAPI):
    """Mock API for tests that overrides _get and tracks requests."""

    @classmethod
    def mock_collection(self, _db):
        """Create a mock ODL collection to use in tests."""
        library = DatabaseTest.make_default_library(_db)
        collection, ignore = get_one_or_create(
            _db, Collection,
            name="Test Shared ODL Collection", create_method_kwargs=dict(
                external_account_id=u"http://shared-odl",
            )
        )
        integration = collection.create_external_integration(
            protocol=SharedODLAPI.NAME
        )
        library.collections.append(collection)
        return collection

    def __init__(self, _db, collection, *args, **kwargs):
        self.responses = []
        self.requests = []
        self.request_args = []
        super(MockSharedODLAPI, self).__init__(
            _db, collection, *args, **kwargs
        )

    def queue_response(self, status_code, headers={}, content=None):
        self.responses.insert(
            0, MockRequestsResponse(status_code, headers, content)
        )

    def _get(self, url, patron=None, headers=None, allowed_response_codes=None):
        allowed_response_codes = allowed_response_codes or ["2xx", "3xx"]
        self.requests.append(url)
        self.request_args.append((patron, headers, allowed_response_codes))
        response = self.responses.pop()
        return HTTP._process_response(url, response, allowed_response_codes=allowed_response_codes)
