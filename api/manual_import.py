import datetime

from flask_babel import lazy_gettext as _

from api.circulation import BaseCirculationAPI, LoanInfo
from api.circulation_exceptions import NoAcceptableFormat
from api.selftest import HasSelfTests
from core.mirror import MirrorUploader
from core.model import ExternalIntegration, Session, Loan, LicensePool, Representation, \
    ExternalIntegrationLink, LicensePoolDeliveryMechanism


class ManualImportAPI(BaseCirculationAPI, HasSelfTests):
    SIGN_URLS = 'use_presigned_urls'
    SIGN_URLS_DEFAULT_VALUE = str(False)

    SETTINGS = [
        {
            'key': SIGN_URLS,
            'label': _('Sign URLs'),
            'description': _('Sign URLs and make them expirable'),
            'type': 'select',
            'options': [
                {'key': str(True), 'label': _('Sign URLs and make them expirable')},
                {'key': str(False), 'label': _('Use original URLs')},
            ],
            'default': SIGN_URLS_DEFAULT_VALUE
         },
    ] + BaseCirculationAPI.SETTINGS

    """Used for processing collections which were manually imported into Circulation Manager
    and signs URLs during fulfilment process
    """

    delivery_mechanism_to_internal_format = {
        (media_type, None): media_type
        for media_type in Representation.SUPPORTED_BOOK_MEDIA_TYPES + Representation.IMAGE_MEDIA_TYPES
    }

    NAME = ExternalIntegration.MANUAL
    DESCRIPTION = _(
        "Books will be manually added to the circulation manager, not imported automatically through a protocol.")

    def __init__(self, db, collection, circulation):
        """Initializes a new instance of ManualImportAPI class

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param collection: Collection
        :type collection: core.model.collection.Collection

        :param circulation: Circulation API
        :type circulation: api.admin.circulation.CirculationAPI
        """
        self._circulation = circulation

    def checkout(self, patron, pin, licensepool, internal_format):
        now = datetime.datetime.utcnow()

        return LoanInfo(
            licensepool.collection,
            licensepool.data_source.name,
            licensepool.identifier.type,
            licensepool.identifier.identifier,
            start_date=now,
            end_date=None,
        )

    def fulfill(self, patron, pin, licensepool, internal_format=None, part=None, fulfill_part_url=None):
        # NOTE: We assume that the license pool contains the single LicensePoolDeliveryMechanism instance
        # It seems to be true for collections imported using bin/directory_import script
        if not licensepool.delivery_mechanisms or \
                not isinstance(licensepool.delivery_mechanisms[0], LicensePoolDeliveryMechanism):
            raise NoAcceptableFormat()

        delivery_mechanism = licensepool.delivery_mechanisms[0]
        fulfillment = self._circulation.fulfill_open_access(
            licensepool, delivery_mechanism.delivery_mechanism
        )
        mirror = MirrorUploader.for_collection(licensepool.collection, ExternalIntegrationLink.BOOKS)
        signed_url = mirror.sign_url(fulfillment.content_link)

        fulfillment.content_link = signed_url

        return fulfillment

    def patron_activity(self, patron, pin):
        # Look up loans for this collection in the database.
        _db = Session.object_session(patron)
        loans = _db.query(Loan).join(Loan.license_pool).filter(
            LicensePool.collection_id == self.collection_id
        ).filter(
            Loan.patron == patron
        )
        return [
            LoanInfo(
                loan.license_pool.collection,
                loan.license_pool.data_source.name,
                loan.license_pool.identifier.type,
                loan.license_pool.identifier.identifier,
                loan.start,
                loan.end
            ) for loan in loans]

    def place_hold(self, patron, pin, licensepool, notification_email_address):
        raise NotImplementedError()

    def release_hold(self, patron, pin, licensepool):
        raise NotImplementedError()

    def _run_self_tests(self, _db):
        pass
