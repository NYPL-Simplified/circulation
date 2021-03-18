import json

from mock import PropertyMock, create_autospec

from api.admin.controller.collection_settings import CollectionSettingsController
from api.controller import CirculationManager
from api.proquest.importer import ProQuestOPDS2ImporterConfiguration
from api.saml.metadata.model import SAMLAttributeType
from core.model import ConfigurationSetting
from core.testing import DatabaseTest


class TestCollectionSettingsController(DatabaseTest):
    def test_load_settings_correctly_loads_menu_values(self):
        # Arrange
        manager = create_autospec(spec=CirculationManager)
        manager._db = PropertyMock(return_value=self._db)
        controller = CollectionSettingsController(manager)

        # We'll be using affiliation_attributes configuration setting defined in the ProQuest integration.
        affiliation_attributes_key = (
            ProQuestOPDS2ImporterConfiguration.affiliation_attributes.key
        )
        expected_affiliation_attributes = [
            SAMLAttributeType.eduPersonPrincipalName.name,
            SAMLAttributeType.eduPersonScopedAffiliation.name,
        ]
        protocol_settings = [
            ProQuestOPDS2ImporterConfiguration.affiliation_attributes.to_settings()
        ]
        collection_settings = None
        collection = self._default_collection

        # We need to explicitly set the value of "affiliation_attributes" configuration setting.
        ConfigurationSetting.for_externalintegration(
            affiliation_attributes_key, collection.external_integration
        ).value = json.dumps(expected_affiliation_attributes)

        # Act
        settings = controller.load_settings(
            protocol_settings, collection, collection_settings
        )

        # Assert
        assert True == (affiliation_attributes_key in settings)

        # We want to make sure that the result setting array contains a correct value in a list format.
        saved_affiliation_attributes = settings[affiliation_attributes_key]
        assert expected_affiliation_attributes == saved_affiliation_attributes
