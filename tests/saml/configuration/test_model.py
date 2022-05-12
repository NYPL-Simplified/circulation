import json

import sqlalchemy
from mock import MagicMock, PropertyMock, call, create_autospec
from parameterized import parameterized

from api.app import initialize_database
from api.authenticator import BaseSAMLAuthenticationProvider
from api.saml.configuration.model import (
    SAMLConfiguration,
    SAMLConfigurationFactory,
    SAMLOneLoginConfiguration,
)
from api.saml.metadata.federations import incommon
from api.saml.metadata.federations.model import (
    SAMLFederatedIdentityProvider,
    SAMLFederation,
)
from api.saml.metadata.model import (
    SAMLIdentityProviderMetadata,
    SAMLNameIDFormat,
    SAMLOrganization,
    SAMLService,
    SAMLServiceProviderMetadata,
    SAMLUIInfo,
)
from api.saml.metadata.parser import SAMLMetadataParser
from core.model.configuration import (
    ConfigurationStorage,
    ExternalIntegration,
    HasExternalIntegration,
)
from tests.saml import fixtures
from tests.saml.database_test import DatabaseTest

SERVICE_PROVIDER_WITHOUT_CERTIFICATE = SAMLServiceProviderMetadata(
    fixtures.SP_ENTITY_ID,
    SAMLUIInfo(),
    SAMLOrganization(),
    SAMLNameIDFormat.UNSPECIFIED.value,
    SAMLService(fixtures.SP_ACS_URL, fixtures.SP_ACS_BINDING),
)

SERVICE_PROVIDER_WITH_CERTIFICATE = SAMLServiceProviderMetadata(
    fixtures.SP_ENTITY_ID,
    SAMLUIInfo(),
    SAMLOrganization(),
    SAMLNameIDFormat.UNSPECIFIED.value,
    SAMLService(fixtures.SP_ACS_URL, fixtures.SP_ACS_BINDING),
    certificate=fixtures.SIGNING_CERTIFICATE,
    private_key=fixtures.PRIVATE_KEY,
)

IDENTITY_PROVIDERS = [
    SAMLIdentityProviderMetadata(
        fixtures.IDP_1_ENTITY_ID,
        SAMLUIInfo(),
        SAMLOrganization(),
        SAMLNameIDFormat.UNSPECIFIED.value,
        SAMLService(fixtures.IDP_1_SSO_URL, fixtures.IDP_1_SSO_BINDING),
    ),
    SAMLIdentityProviderMetadata(
        fixtures.IDP_2_ENTITY_ID,
        SAMLUIInfo(),
        SAMLOrganization(),
        SAMLNameIDFormat.UNSPECIFIED.value,
        SAMLService(fixtures.IDP_2_SSO_URL, fixtures.IDP_2_SSO_BINDING),
    ),
]


class TestSAMLConfiguration(DatabaseTest):
    def setup_method(self):
        super(TestSAMLConfiguration, self).setup_method()

        self._saml_provider_integration = self._external_integration(
            "api.saml.provider", ExternalIntegration.PATRON_AUTH_GOAL
        )
        self._saml_integration_association = create_autospec(
            spec=HasExternalIntegration
        )
        self._saml_integration_association.external_integration = MagicMock(
            return_value=self._saml_provider_integration
        )

    def test_get_service_provider_returns_correct_value(self):
        # Arrange
        service_provider_metadata = fixtures.CORRECT_XML_WITH_ONE_SP

        metadata_parser = SAMLMetadataParser()
        metadata_parser.parse = MagicMock(side_effect=metadata_parser.parse)

        configuration_storage = ConfigurationStorage(self._saml_integration_association)
        configuration_storage.load = MagicMock(side_effect=configuration_storage.load)

        saml_configuration_factory = SAMLConfigurationFactory(metadata_parser)

        with saml_configuration_factory.create(
            configuration_storage, self._db, SAMLConfiguration
        ) as configuration:
            configuration.service_provider_xml_metadata = service_provider_metadata

            # Act
            service_provider = configuration.get_service_provider(self._db)

            # Assert
            assert True == isinstance(service_provider, SAMLServiceProviderMetadata)
            assert fixtures.SP_ENTITY_ID == service_provider.entity_id

            configuration_storage.load.assert_has_calls(
                [
                    call(self._db, SAMLConfiguration.service_provider_xml_metadata.key),
                    call(self._db, SAMLConfiguration.service_provider_private_key.key),
                ]
            )
            metadata_parser.parse.assert_called_once_with(service_provider_metadata)

    def test_get_identity_providers_returns_non_federated_idps(self):
        # Arrange
        identity_providers_metadata = fixtures.CORRECT_XML_WITH_MULTIPLE_IDPS

        metadata_parser = SAMLMetadataParser()
        metadata_parser.parse = MagicMock(side_effect=metadata_parser.parse)

        configuration_storage = ConfigurationStorage(self._saml_integration_association)
        configuration_storage.load = MagicMock(side_effect=configuration_storage.load)

        saml_configuration_factory = SAMLConfigurationFactory(metadata_parser)

        with saml_configuration_factory.create(
            configuration_storage, self._db, SAMLConfiguration
        ) as configuration:
            configuration.non_federated_identity_provider_xml_metadata = (
                identity_providers_metadata
            )

            # Act
            identity_providers = configuration.get_identity_providers(self._db)

            # Assert
            assert 2 == len(identity_providers)

            assert True == isinstance(identity_providers[0], SAMLIdentityProviderMetadata)
            assert fixtures.IDP_1_ENTITY_ID == identity_providers[0].entity_id

            assert True == isinstance(identity_providers[1], SAMLIdentityProviderMetadata)
            assert fixtures.IDP_2_ENTITY_ID == identity_providers[1].entity_id

            configuration_storage.load.assert_has_calls(
                [
                    call(
                        self._db,
                        SAMLConfiguration.non_federated_identity_provider_xml_metadata.key,
                    ),
                    call(
                        self._db,
                        SAMLConfiguration.federated_identity_provider_entity_ids.key,
                    ),
                ]
            )
            metadata_parser.parse.assert_called_once_with(identity_providers_metadata)

    def test_get_identity_providers_returns_federated_idps(self):
        # Arrange
        federated_identity_provider_entity_ids = json.dumps(
            [fixtures.IDP_1_ENTITY_ID, fixtures.IDP_2_ENTITY_ID]
        )

        metadata_parser = SAMLMetadataParser()
        metadata_parser.parse = MagicMock(side_effect=metadata_parser.parse)

        configuration_storage = ConfigurationStorage(self._saml_integration_association)
        configuration_storage.load = MagicMock(side_effect=configuration_storage.load)

        saml_configuration_factory = SAMLConfigurationFactory(metadata_parser)

        federation = SAMLFederation("Test federation", "http://localhost")
        federated_idp_1 = SAMLFederatedIdentityProvider(
            federation,
            fixtures.IDP_1_ENTITY_ID,
            fixtures.IDP_1_UI_INFO_EN_DISPLAY_NAME,
            fixtures.CORRECT_XML_WITH_IDP_1,
        )
        federated_idp_2 = SAMLFederatedIdentityProvider(
            federation,
            fixtures.IDP_2_ENTITY_ID,
            fixtures.IDP_2_UI_INFO_EN_DISPLAY_NAME,
            fixtures.CORRECT_XML_WITH_IDP_2,
        )

        self._db.add_all([federation, federated_idp_1, federated_idp_2])

        with saml_configuration_factory.create(
            configuration_storage, self._db, SAMLConfiguration
        ) as configuration:
            configuration.federated_identity_provider_entity_ids = (
                federated_identity_provider_entity_ids
            )

            # Act
            identity_providers = configuration.get_identity_providers(self._db)

            # Assert
            assert 2 == len(identity_providers)
            assert True == isinstance(identity_providers[0], SAMLIdentityProviderMetadata)
            assert fixtures.IDP_1_ENTITY_ID == identity_providers[0].entity_id

            assert True == isinstance(identity_providers[1], SAMLIdentityProviderMetadata)
            assert fixtures.IDP_2_ENTITY_ID == identity_providers[1].entity_id

            configuration_storage.load.assert_has_calls(
                [
                    call(
                        self._db,
                        SAMLConfiguration.non_federated_identity_provider_xml_metadata.key,
                    ),
                    call(
                        self._db,
                        SAMLConfiguration.federated_identity_provider_entity_ids.key,
                    ),
                ]
            )
            metadata_parser.parse.assert_has_calls(
                [call(federated_idp_1.xml_metadata), call(federated_idp_2.xml_metadata)]
            )

    def test_get_identity_providers_returns_both_non_federated_and_federated_idps(self):
        # Arrange
        non_federated_identity_providers_metadata = (
            fixtures.CORRECT_XML_WITH_MULTIPLE_IDPS
        )

        federated_identity_provider_entity_ids = json.dumps(
            [fixtures.IDP_1_ENTITY_ID, fixtures.IDP_2_ENTITY_ID]
        )

        metadata_parser = SAMLMetadataParser()
        metadata_parser.parse = MagicMock(side_effect=metadata_parser.parse)

        configuration_storage = ConfigurationStorage(self._saml_integration_association)
        configuration_storage.load = MagicMock(side_effect=configuration_storage.load)

        saml_configuration_factory = SAMLConfigurationFactory(metadata_parser)

        federation = SAMLFederation("Test federation", "http://localhost")
        federated_idp_1 = SAMLFederatedIdentityProvider(
            federation,
            fixtures.IDP_1_ENTITY_ID,
            fixtures.IDP_1_UI_INFO_EN_DISPLAY_NAME,
            fixtures.CORRECT_XML_WITH_IDP_1,
        )
        federated_idp_2 = SAMLFederatedIdentityProvider(
            federation,
            fixtures.IDP_2_ENTITY_ID,
            fixtures.IDP_2_UI_INFO_EN_DISPLAY_NAME,
            fixtures.CORRECT_XML_WITH_IDP_2,
        )

        self._db.add_all([federation, federated_idp_1, federated_idp_2])

        with saml_configuration_factory.create(
            configuration_storage, self._db, SAMLConfiguration
        ) as configuration:
            configuration.non_federated_identity_provider_xml_metadata = (
                non_federated_identity_providers_metadata
            )
            configuration.federated_identity_provider_entity_ids = (
                federated_identity_provider_entity_ids
            )

            # Act
            identity_providers = configuration.get_identity_providers(self._db)

            # Assert
            assert 4 == len(identity_providers)
            assert True == isinstance(identity_providers[0], SAMLIdentityProviderMetadata)
            assert fixtures.IDP_1_ENTITY_ID == identity_providers[0].entity_id

            assert True == isinstance(identity_providers[1], SAMLIdentityProviderMetadata)
            assert fixtures.IDP_2_ENTITY_ID == identity_providers[1].entity_id

            assert True == isinstance(identity_providers[2], SAMLIdentityProviderMetadata)
            assert fixtures.IDP_1_ENTITY_ID == identity_providers[2].entity_id

            assert True == isinstance(identity_providers[3], SAMLIdentityProviderMetadata)
            assert fixtures.IDP_2_ENTITY_ID == identity_providers[3].entity_id

            configuration_storage.load.assert_has_calls(
                [
                    call(
                        self._db,
                        SAMLConfiguration.non_federated_identity_provider_xml_metadata.key,
                    ),
                    call(
                        self._db,
                        SAMLConfiguration.federated_identity_provider_entity_ids.key,
                    ),
                ]
            )
            metadata_parser.parse.assert_has_calls(
                [
                    call(non_federated_identity_providers_metadata),
                    call(federated_idp_1.xml_metadata),
                    call(federated_idp_2.xml_metadata),
                ]
            )


class TestSAMLSettings(DatabaseTest):
    def test(self):
        # Arrange

        # Act, assert
        [federated_identity_provider_entity_ids] = [
            setting
            for setting in BaseSAMLAuthenticationProvider.SETTINGS
            if setting["key"]
            == SAMLConfiguration.federated_identity_provider_entity_ids.key
        ]

        # Without an active database session there are no federated IdPs and no options
        assert None == federated_identity_provider_entity_ids["options"]

        initialize_database(autoinitialize=False)

        federation = SAMLFederation(
            incommon.FEDERATION_TYPE, "http://incommon.org/metadata"
        )
        federated_identity_provider = SAMLFederatedIdentityProvider(
            federation,
            fixtures.IDP_1_ENTITY_ID,
            fixtures.IDP_1_UI_INFO_EN_DISPLAY_NAME,
            fixtures.CORRECT_XML_WITH_IDP_1,
        )

        from api.app import app

        app._db.add_all([federation, federated_identity_provider])

        [federated_identity_provider_entity_ids] = [
            setting
            for setting in BaseSAMLAuthenticationProvider.SETTINGS
            if setting["key"]
            == SAMLConfiguration.federated_identity_provider_entity_ids.key
        ]

        # After getting an active database session options get initialized
        assert 1 == len(federated_identity_provider_entity_ids["options"])


class TestSAMLOneLoginConfiguration(object):
    def test_get_identity_provider_settings_returns_correct_result(self):
        # Arrange
        configuration = create_autospec(spec=SAMLConfiguration)
        configuration.get_identity_providers = MagicMock(
            return_value=IDENTITY_PROVIDERS
        )
        onelogin_configuration = SAMLOneLoginConfiguration(configuration)
        expected_result = {
            "idp": {
                "entityId": IDENTITY_PROVIDERS[0].entity_id,
                "singleSignOnService": {
                    "url": IDENTITY_PROVIDERS[0].sso_service.url,
                    "binding": IDENTITY_PROVIDERS[0].sso_service.binding.value,
                },
            },
            "security": {
                "authnRequestsSigned": IDENTITY_PROVIDERS[0].want_authn_requests_signed
            },
        }
        db = create_autospec(spec=sqlalchemy.orm.session.Session)

        # Act
        result = onelogin_configuration.get_identity_provider_settings(
            db, IDENTITY_PROVIDERS[0].entity_id
        )

        # Assert
        assert result == expected_result
        configuration.get_identity_providers.assert_called_once_with(db)

    @parameterized.expand(
        [
            (
                "service_provider_without_certificates",
                SERVICE_PROVIDER_WITHOUT_CERTIFICATE,
                {
                    "sp": {
                        "entityId": SERVICE_PROVIDER_WITH_CERTIFICATE.entity_id,
                        "assertionConsumerService": {
                            "url": SERVICE_PROVIDER_WITH_CERTIFICATE.acs_service.url,
                            "binding": SERVICE_PROVIDER_WITH_CERTIFICATE.acs_service.binding.value,
                        },
                        "NameIDFormat": SERVICE_PROVIDER_WITH_CERTIFICATE.name_id_format,
                        "x509cert": "",
                        "privateKey": "",
                    },
                    "security": {
                        "authnRequestsSigned": SERVICE_PROVIDER_WITH_CERTIFICATE.authn_requests_signed
                    },
                },
            ),
            (
                "service_provider_with_certificate",
                SERVICE_PROVIDER_WITH_CERTIFICATE,
                {
                    "sp": {
                        "entityId": SERVICE_PROVIDER_WITH_CERTIFICATE.entity_id,
                        "assertionConsumerService": {
                            "url": SERVICE_PROVIDER_WITH_CERTIFICATE.acs_service.url,
                            "binding": SERVICE_PROVIDER_WITH_CERTIFICATE.acs_service.binding.value,
                        },
                        "NameIDFormat": SERVICE_PROVIDER_WITH_CERTIFICATE.name_id_format,
                        "x509cert": fixtures.strip_certificate(
                            SERVICE_PROVIDER_WITH_CERTIFICATE.certificate
                        ),
                        "privateKey": SERVICE_PROVIDER_WITH_CERTIFICATE.private_key,
                    },
                    "security": {
                        "authnRequestsSigned": SERVICE_PROVIDER_WITH_CERTIFICATE.authn_requests_signed
                    },
                },
            ),
        ]
    )
    def test_get_service_provider_settings_returns_correct_result(
        self, _, service_provider, expected_result
    ):
        # Arrange
        configuration = create_autospec(spec=SAMLConfiguration)
        configuration.get_service_provider = MagicMock(return_value=service_provider)
        onelogin_configuration = SAMLOneLoginConfiguration(configuration)
        db = create_autospec(spec=sqlalchemy.orm.session.Session)

        # Act
        result = onelogin_configuration.get_service_provider_settings(db)

        # Assert
        result["sp"]["x509cert"] = fixtures.strip_certificate(result["sp"]["x509cert"])

        assert result == expected_result
        configuration.get_service_provider.assert_called_once_with(db)

    def test_get_settings_returns_correct_result(self):
        # Arrange
        debug = False
        strict = False

        service_provider_debug_mode_mock = PropertyMock(return_value=debug)
        service_provider_strict_mode_mock = PropertyMock(return_value=strict)

        configuration = create_autospec(spec=SAMLConfiguration)
        type(
            configuration
        ).service_provider_debug_mode = service_provider_debug_mode_mock
        type(
            configuration
        ).service_provider_strict_mode = service_provider_strict_mode_mock
        configuration.get_service_provider = MagicMock(
            return_value=SERVICE_PROVIDER_WITH_CERTIFICATE
        )
        configuration.get_identity_providers = MagicMock(
            return_value=IDENTITY_PROVIDERS
        )

        onelogin_configuration = SAMLOneLoginConfiguration(configuration)

        expected_result = {
            "debug": debug,
            "strict": strict,
            "idp": {
                "entityId": IDENTITY_PROVIDERS[0].entity_id,
                "singleSignOnService": {
                    "url": IDENTITY_PROVIDERS[0].sso_service.url,
                    "binding": IDENTITY_PROVIDERS[0].sso_service.binding.value,
                },
                "singleLogoutService": {},
                "x509cert": "",
                "certFingerprint": "",
                "certFingerprintAlgorithm": "sha1",
            },
            "sp": {
                "entityId": SERVICE_PROVIDER_WITH_CERTIFICATE.entity_id,
                "assertionConsumerService": {
                    "url": SERVICE_PROVIDER_WITH_CERTIFICATE.acs_service.url,
                    "binding": SERVICE_PROVIDER_WITH_CERTIFICATE.acs_service.binding.value,
                },
                "attributeConsumingService": {},
                "singleLogoutService": {
                    "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect"
                },
                "NameIDFormat": SERVICE_PROVIDER_WITH_CERTIFICATE.name_id_format,
                "x509cert": fixtures.strip_certificate(
                    SERVICE_PROVIDER_WITH_CERTIFICATE.certificate
                ),
                "privateKey": SERVICE_PROVIDER_WITH_CERTIFICATE.private_key,
            },
            "security": {
                "failOnAuthnContextMismatch": False,
                "requestedAuthnContextComparison": "exact",
                "wantNameIdEncrypted": False,
                "authnRequestsSigned": SERVICE_PROVIDER_WITH_CERTIFICATE.authn_requests_signed
                or IDENTITY_PROVIDERS[0].want_authn_requests_signed,
                "logoutResponseSigned": False,
                "wantMessagesSigned": False,
                "metadataCacheDuration": None,
                "requestedAuthnContext": True,
                "logoutRequestSigned": False,
                "wantAttributeStatement": True,
                "signMetadata": False,
                "digestAlgorithm": "http://www.w3.org/2000/09/xmldsig#sha1",
                "metadataValidUntil": None,
                "wantAssertionsSigned": False,
                "wantNameId": True,
                "wantAssertionsEncrypted": False,
                "nameIdEncrypted": False,
                "signatureAlgorithm": "http://www.w3.org/2000/09/xmldsig#rsa-sha1",
            },
        }
        db = create_autospec(spec=sqlalchemy.orm.session.Session)

        # Act
        result = onelogin_configuration.get_settings(
            db, IDENTITY_PROVIDERS[0].entity_id
        )

        # Assert
        result["sp"]["x509cert"] = fixtures.strip_certificate(result["sp"]["x509cert"])

        assert result == expected_result
        service_provider_debug_mode_mock.assert_called_with()
        service_provider_strict_mode_mock.assert_called_with()
        configuration.get_service_provider.assert_called_with(db)
        configuration.get_identity_providers.assert_called_with(db)
