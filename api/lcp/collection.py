import datetime
import json
from io import BytesIO

from flask import send_file
from sqlalchemy import or_

from api.circulation import FulfillmentInfo, BaseCirculationAPI, LoanInfo
from api.lcp.encrypt import LCPEncryptionConfiguration
from api.lcp.hash import HasherFactory
from api.lcp.server import LCPServerConfiguration, LCPServer
from core.lcp.credential import LCPCredentialFactory
from core.model import ExternalIntegration, LicensePoolDeliveryMechanism, get_one, Loan, Collection, LicensePool, \
    DeliveryMechanism
from core.model.configuration import HasExternalIntegration, ConfigurationStorage, ConfigurationFactory
from core.util.datetime_helpers import (
    utc_now,
)


class LCPFulfilmentInfo(FulfillmentInfo):
    """Sends LCP licenses as fulfilment info"""

    def __init__(
            self,
            identifier,
            collection,
            data_source_name,
            identifier_type,
            content_link=None,
            content_type=None,
            content=None,
            content_expires=None):
        """Initializes a new instance of LCPFulfilmentInfo class

        :param identifier: Identifier
        :type identifier: string

        :param collection: Collection
        :type collection: Collection

        :param data_source_name: Data source's name
        :type data_source_name: string

        :param identifier_type: Identifier's type
        :type identifier_type: string

        :param content_link: Content link
        :type content_link: Optional[string]

        :param content_link: Identifier's type
        :type content_link: string

        :param content: Identifier's type
        :type content: Any

        :param content_expires: Time when the content expires
        :type content_expires: Optional[datetime.datetime]
        """
        super(LCPFulfilmentInfo, self).__init__(
            collection,
            data_source_name,
            identifier_type,
            identifier,
            content_link,
            content_type,
            content,
            content_expires
        )

    @property
    def as_response(self):
        """Returns LCP license as a Flask response

        :return: LCP license as a Flask response
        :rtype: Response
        """
        return send_file(
            BytesIO(json.dumps(self.content)),
            mimetype=DeliveryMechanism.LCP_DRM,
            as_attachment=True,
            attachment_filename='{0}.lcpl'.format(self.identifier)
        )


class LCPAPI(BaseCirculationAPI, HasExternalIntegration):
    """Implements LCP workflow"""

    NAME = ExternalIntegration.LCP
    SERVICE_NAME = 'LCP'
    DESCRIPTION = 'Manually imported collection protected using Readium LCP DRM'

    SETTINGS = LCPServerConfiguration.to_settings() + LCPEncryptionConfiguration.to_settings()

    def __init__(self, db, collection):
        """Initializes a new instance of LCPAPI class

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param collection: Book collection
        :type collection: Collection
        """
        if collection.protocol != ExternalIntegration.LCP:
            raise ValueError(
                'Collection protocol is {0} but must be LCPAPI'.format(collection.protocol)
            )

        self._db = db
        self._collection_id = collection.id
        self._lcp_server_instance = None

    def internal_format(self, delivery_mechanism):
        """Look up the internal format for this delivery mechanism or
        raise an exception.

        :param delivery_mechanism: A LicensePoolDeliveryMechanism
        :type delivery_mechanism: LicensePoolDeliveryMechanism
        """
        return delivery_mechanism

    @property
    def collection(self):
        """Returns an associated Collection object

        :return: Associated Collection object
        :rtype: Collection
        """
        return Collection.by_id(self._db, id=self._collection_id)

    def external_integration(self, db):
        """Returns an external integration associated with this object

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :return: External integration associated with this object
        :rtype: core.model.configuration.ExternalIntegration
        """
        return self.collection.external_integration

    def _create_lcp_server(self):
        """Creates a new instance of LCPServer

        :return: New instance of LCPServer
        :rtype: LCPServer
        """
        configuration_storage = ConfigurationStorage(self)
        configuration_factory = ConfigurationFactory()
        hasher_factory = HasherFactory()
        credential_factory = LCPCredentialFactory()
        lcp_server = LCPServer(configuration_storage, configuration_factory, hasher_factory, credential_factory)

        return lcp_server

    @property
    def _lcp_server(self):
        """Returns an instance of LCPServer

        :return: Instance of LCPServer
        :rtype: LCPServer
        """
        if self._lcp_server_instance is None:
            self._lcp_server_instance = self._create_lcp_server()

        return self._lcp_server_instance

    def checkout(self, patron, pin, licensepool, internal_format):
        """Checks out a book on behalf of a patron

        :param patron: A Patron object for the patron who wants to check out the book
        :type patron: Patron

        :param pin: The patron's alleged password
        :type pin: string

        :param licensepool: Contains lending info as well as link to parent Identifier
        :type licensepool: LicensePool

        :param internal_format: Represents the patron's desired book format.
        :type internal_format: Any

        :return: a LoanInfo object
        :rtype: LoanInfo
        """
        days = self.collection.default_loan_period(patron.library)
        today = utc_now()
        expires = today + datetime.timedelta(days=days)
        loan = get_one(self._db, Loan, patron=patron, license_pool=licensepool, on_multiple='interchangeable')

        if loan:
            license = self._lcp_server.get_license(self._db, loan.external_identifier, patron)
        else:
            license = self._lcp_server.generate_license(
                self._db,
                licensepool.identifier.identifier,
                patron,
                today,
                expires
            )

        loan = LoanInfo(
            licensepool.collection,
            licensepool.data_source.name,
            identifier_type=licensepool.identifier.type,
            identifier=licensepool.identifier.identifier,
            start_date=today,
            end_date=expires,
            fulfillment_info=None,
            external_identifier=license['id']
        )

        return loan

    def fulfill(self, patron, pin, licensepool, internal_format=None, part=None, fulfill_part_url=None):
        """Get the actual resource file to the patron.

        :param patron: A Patron object for the patron who wants to check out the book
        :type patron: Patron

        :param pin: The patron's alleged password
        :type pin: string

        :param licensepool: Contains lending info as well as link to parent Identifier
        :type licensepool: LicensePool

        :param internal_format: A vendor-specific name indicating the format requested by the patron
        :type internal_format:

        :param part: A vendor-specific identifier indicating that the
            patron wants to fulfill one specific part of the book
            (e.g. one chapter of an audiobook), not the whole thing
        :type part: Any

        :param fulfill_part_url: A function that takes one argument (a
            vendor-specific part identifier) and returns the URL to use
            when fulfilling that part
        :type fulfill_part_url: Any

        :return: a FulfillmentInfo object
        :rtype: FulfillmentInfo
        """
        loan = get_one(self._db, Loan, patron=patron, license_pool=licensepool, on_multiple='interchangeable')
        license = self._lcp_server.get_license(self._db, loan.external_identifier, patron)
        fulfillment_info = LCPFulfilmentInfo(
            licensepool.identifier.identifier,
            licensepool.collection,
            licensepool.data_source.name,
            licensepool.identifier.type,
            content_link=None,
            content_type=DeliveryMechanism.LCP_DRM,
            content=license,
            content_expires=None,
        )

        return fulfillment_info

    def patron_activity(self, patron, pin):
        """Returns patron's loans

        :param patron: A Patron object for the patron who wants to check out the book
        :type patron: Patron

        :param pin: The patron's alleged password
        :type pin: string

        :return: List of patron's loans
        :rtype: List[LoanInfo]
        """
        now = utc_now()
        loans = self._db\
            .query(Loan)\
            .join(LicensePool)\
            .join(Collection)\
            .filter(
                Collection.id == self._collection_id,
                Loan.patron == patron,
                or_(
                    Loan.start is None,
                    Loan.start <= now
                ),
                or_(
                    Loan.end is None,
                    Loan.end > now
                )
            )

        loan_info_objects = []

        for loan in loans:
            licensepool = get_one(self._db, LicensePool, id=loan.license_pool_id)

            loan_info_objects.append(LoanInfo(
                collection=self.collection,
                data_source_name=licensepool.data_source.name,
                identifier_type=licensepool.identifier.type,
                identifier=licensepool.identifier.identifier,
                start_date=loan.start,
                end_date=loan.end,
                fulfillment_info=None,
                external_identifier=loan.external_identifier
            ))

        return loan_info_objects

    # TODO: Implement place_hold and release_hold (https://jira.nypl.org/browse/SIMPLY-3013)
