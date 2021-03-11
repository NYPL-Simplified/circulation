from mock import MagicMock, create_autospec

from api.saml.metadata.federations.loader import SAMLFederatedIdentityProviderLoader
from api.saml.metadata.federations.model import (
    SAMLFederatedIdentityProvider,
    SAMLFederation,
)
from api.saml.metadata.monitor import SAMLMetadataMonitor
from tests.saml import fixtures
from tests.saml.database_test import DatabaseTest


class TestSAMLMetadataMonitor(DatabaseTest):
    def test(self):
        # Arrange
        expected_federation = SAMLFederation(
            "Test federation", "http://incommon.org/metadata"
        )
        expected_federated_identity_providers = [
            SAMLFederatedIdentityProvider(
                expected_federation,
                fixtures.IDP_1_ENTITY_ID,
                fixtures.IDP_1_UI_INFO_EN_DISPLAY_NAME,
                fixtures.CORRECT_XML_WITH_IDP_1,
            ),
            SAMLFederatedIdentityProvider(
                expected_federation,
                fixtures.IDP_2_ENTITY_ID,
                fixtures.IDP_2_UI_INFO_EN_DISPLAY_NAME,
                fixtures.CORRECT_XML_WITH_IDP_2,
            ),
        ]

        self._db.add_all([expected_federation])
        self._db.add_all(expected_federated_identity_providers)

        loader = create_autospec(spec=SAMLFederatedIdentityProviderLoader)
        loader.load = MagicMock(return_value=expected_federated_identity_providers)

        monitor = SAMLMetadataMonitor(self._db, loader)

        # Act
        monitor.run_once(None)

        # Assert
        identity_providers = self._db.query(SAMLFederatedIdentityProvider).all()
        assert expected_federated_identity_providers == identity_providers
