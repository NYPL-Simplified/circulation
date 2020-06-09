# FIXME: Required to get rid of the circular import error
import api.app

from mock import MagicMock, PropertyMock, create_autospec
from nose.tools import eq_
from parameterized import parameterized

from api.saml.configuration import SAMLConfiguration, SAMLOneLoginConfiguration, SAMLMetadataSerializer, SAMLConfigurationSerializer
from api.saml.metadata import ServiceProviderMetadata, UIInfo, Service, NameIDFormat, IdentityProviderMetadata
from tests.saml import fixtures
from tests.saml.database_test import DatabaseTest


SERVICE_PROVIDER = ServiceProviderMetadata(
    fixtures.SP_ENTITY_ID,
    UIInfo(),
    NameIDFormat.UNSPECIFIED.value,
    Service(fixtures.SP_ACS_URL, fixtures.SP_ACS_BINDING)
)

IDENTITY_PROVIDERS = [
    IdentityProviderMetadata(
        fixtures.IDP_1_ENTITY_ID,
        UIInfo(),
        NameIDFormat.UNSPECIFIED.value,
        Service(fixtures.IDP_1_SSO_URL, fixtures.IDP_1_SSO_BINDING)
    ),
    IdentityProviderMetadata(
        fixtures.IDP_2_ENTITY_ID,
        UIInfo(),
        NameIDFormat.UNSPECIFIED.value,
        Service(fixtures.IDP_2_SSO_URL, fixtures.IDP_2_SSO_BINDING)
    )
]


class SAMLMetadataSerializerTest(DatabaseTest):
    @parameterized.expand([
        ('service_provider', 'sp_metadata', SERVICE_PROVIDER),
        ('list_of_identity_providers', 'idp_metadata', IDENTITY_PROVIDERS)
    ])
    def test_deserialize_can_deserialize(self, name, setting_name, expected_result):
        # Arrange
        configuration_serializer = SAMLMetadataSerializer(self._integration)

        # Act
        configuration_serializer.serialize(setting_name, expected_result)
        result = configuration_serializer.deserialize(setting_name)

        # Assert
        eq_(result, expected_result)


class SAMLConfigurationTest(object):
    def test_service_provider_returns_correct_value(self):
        # Arrange
        expected_result = SERVICE_PROVIDER
        configuration_serializer = create_autospec(spec=SAMLConfigurationSerializer)
        metadata_serializer = create_autospec(spec=SAMLMetadataSerializer)
        metadata_serializer.deserialize = MagicMock(return_value=expected_result)
        configuration = SAMLConfiguration(configuration_serializer, metadata_serializer)

        # Act
        result = configuration.service_provider

        # Assert
        eq_(result, expected_result)
        metadata_serializer.deserialize.assert_called_once_with(SAMLConfiguration.SP_METADATA)

    def test_identity_providers_returns_correct_value(self):
        # Arrange
        expected_result = IDENTITY_PROVIDERS
        configuration_serializer = create_autospec(spec=SAMLConfigurationSerializer)
        metadata_serializer = create_autospec(spec=SAMLMetadataSerializer)
        metadata_serializer.deserialize = MagicMock(return_value=expected_result)
        configuration = SAMLConfiguration(configuration_serializer, metadata_serializer)

        # Act
        result = configuration.identity_providers

        # Assert
        eq_(result, expected_result)
        metadata_serializer.deserialize.assert_called_once_with(SAMLConfiguration.IDP_METADATA)


class SAMLOneLoginConfigurationTest(object):
    def test_get_identity_provider_settings_returns_correct_result(self):
        # Arrange
        configuration = MagicMock()
        type(configuration).identity_providers = PropertyMock(return_value=IDENTITY_PROVIDERS)
        onelogin_configuration = SAMLOneLoginConfiguration(configuration)
        expected_result = {
            'idp': {
                'entityId': IDENTITY_PROVIDERS[0].entity_id,
                'singleSignOnService': {
                    'url': IDENTITY_PROVIDERS[0].sso_service.url,
                    'binding': IDENTITY_PROVIDERS[0].sso_service.binding.value
                }
            },
            'security': {
                'authnRequestsSigned': IDENTITY_PROVIDERS[0].want_authn_requests_signed
            }
        }

        # Act
        result = onelogin_configuration.get_identity_provider_settings(IDENTITY_PROVIDERS[0].entity_id)

        # Assert
        eq_(result, expected_result)

    def test_get_service_provider_settings_returns_correct_result(self):
        # Arrange
        configuration = MagicMock()
        type(configuration).service_provider = PropertyMock(return_value=SERVICE_PROVIDER)
        onelogin_configuration = SAMLOneLoginConfiguration(configuration)
        expected_result = {
            'sp': {
                'entityId': SERVICE_PROVIDER.entity_id,
                'assertionConsumerService': {
                    'url': SERVICE_PROVIDER.acs_service.url,
                    'binding': SERVICE_PROVIDER.acs_service.binding.value
                },
                'NameIDFormat': SERVICE_PROVIDER.name_id_format,
                'x509cert': SERVICE_PROVIDER.certificate,
                'privateKey': ''
            }
        }

        # Act
        result = onelogin_configuration.get_service_provider_settings()

        # Assert
        eq_(result, expected_result)

    def test_get_settings_returns_correct_result(self):
        # Arrange
        configuration = MagicMock()
        debug = False
        strict = False
        type(configuration).debug = PropertyMock(return_value=False)
        type(configuration).strict = PropertyMock(return_value=False)
        type(configuration).service_provider = PropertyMock(return_value=SERVICE_PROVIDER)
        type(configuration).identity_providers = PropertyMock(return_value=IDENTITY_PROVIDERS)
        onelogin_configuration = SAMLOneLoginConfiguration(configuration)
        expected_idp_data = {
            'entityId': IDENTITY_PROVIDERS[0].entity_id,
            'singleSignOnService': {
                'url': IDENTITY_PROVIDERS[0].sso_service.url,
                'binding': IDENTITY_PROVIDERS[0].sso_service.binding.value
            },
            'x509cert': '',
            'certFingerprint': '',
            'certFingerprintAlgorithm': 'sha1'
        }
        expected_sp_data = {
            'entityId': SERVICE_PROVIDER.entity_id,
            'assertionConsumerService': {
                'url': SERVICE_PROVIDER.acs_service.url,
                'binding': SERVICE_PROVIDER.acs_service.binding.value
            },
            'attributeConsumingService': {},
            'singleLogoutService': {
                'binding': 'urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect'
            },
            'NameIDFormat': SERVICE_PROVIDER.name_id_format,
            'x509cert': SERVICE_PROVIDER.certificate,
            'privateKey': ''
        }
        expected_result = {
            'debug': debug,
            'strict': strict,
            'idp': {
                'entityId': IDENTITY_PROVIDERS[0].entity_id,
                'singleSignOnService': {
                    'url': IDENTITY_PROVIDERS[0].sso_service.url,
                    'binding': IDENTITY_PROVIDERS[0].sso_service.binding.value
                },
                'x509cert': '',
                'certFingerprint': '',
                'certFingerprintAlgorithm': 'sha1'
            },
            'sp': {
                'entityId': SERVICE_PROVIDER.entity_id,
                'assertionConsumerService': {
                    'url': SERVICE_PROVIDER.acs_service.url,
                    'binding': SERVICE_PROVIDER.acs_service.binding.value
                },
                'attributeConsumingService': {},
                'singleLogoutService': {
                    'binding': 'urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect'
                },
                'NameIDFormat': SERVICE_PROVIDER.name_id_format,
                'x509cert': SERVICE_PROVIDER.certificate,
                'privateKey': ''
            },
            'security': {
                'failOnAuthnContextMismatch': False,
                'requestedAuthnContextComparison': 'exact',
                'wantNameIdEncrypted': False,
                'authnRequestsSigned': False,
                'logoutResponseSigned': False,
                'wantMessagesSigned': False,
                'metadataCacheDuration': None,
                'rejectUnsolicitedResponsesWithInResponseTo': False,
                'requestedAuthnContext': True,
                'logoutRequestSigned': False,
                'wantAttributeStatement': True,
                'signMetadata': False,
                'digestAlgorithm': 'http://www.w3.org/2000/09/xmldsig#sha1',
                'metadataValidUntil': None,
                'wantAssertionsSigned': False,
                'wantNameId': True,
                'wantAssertionsEncrypted': False,
                'nameIdEncrypted': False,
                'signatureAlgorithm': 'http://www.w3.org/2000/09/xmldsig#rsa-sha1'
            }
        }

        # Act
        result = onelogin_configuration.get_settings(IDENTITY_PROVIDERS[0].entity_id)

        # Assert
        eq_(result, expected_result)
