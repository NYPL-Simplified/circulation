import datetime
import logging

from api.saml.metadata.federations.model import SAMLFederation
from core.monitor import Monitor


class SAMLMetadataMonitor(Monitor):
    SERVICE_NAME = "SAML Metadata Monitor"

    MAX_AGE = datetime.timedelta(days=1)

    def __init__(self, db, loader):
        """Initialize a new instance of SAMLMetadataMonitor class.

        :param loader: IdP loader
        :type loader: api.saml.loader.SAMLFederatedIdPLoader
        """
        super(SAMLMetadataMonitor, self).__init__(db)

        self._loader = loader
        self._logger = logging.getLogger(__name__)

    def _update_saml_federation_idps_metadata(self, saml_federation):
        """Update IdPs' metadata belonging to the specified SAML federation.

        :param saml_federation: SAML federation
        :type saml_federation: api.saml.metadata.federations.model.SAMLFederation
        """
        self._logger.info("Started processing {0}".format(saml_federation))

        for existing_identity_provider in saml_federation.identity_providers:
            self._db.delete(existing_identity_provider)

        new_identity_providers = self._loader.load(saml_federation)

        for new_identity_provider in new_identity_providers:
            self._db.add(new_identity_provider)

        saml_federation.last_updated_at = datetime.datetime.utcnow()

        self._logger.info("Finished processing {0}".format(saml_federation))

    def run_once(self, progress):
        self._logger.info("Started running the SAML metadata monitor")

        with self._db.begin(subtransactions=True):
            saml_federations = self._db.query(SAMLFederation).all()

            self._logger.info(
                "Found {0} SAML federations".format(len(saml_federations))
            )

            for outdated_saml_federation in saml_federations:
                self._update_saml_federation_idps_metadata(outdated_saml_federation)

        self._logger.info("Finished running the SAML metadata monitor")
