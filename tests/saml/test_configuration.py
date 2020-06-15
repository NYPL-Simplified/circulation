# FIXME: Required to get rid of the circular import error
import api.app

from mock import MagicMock, PropertyMock, create_autospec
from nose.tools import eq_

from api.saml.configuration import SAMLConfiguration, SAMLOneLoginConfiguration, SAMLConfigurationStorage
from api.saml.metadata import ServiceProviderMetadata, UIInfo, Service, NameIDFormat, IdentityProviderMetadata
from api.saml.parser import SAMLMetadataParser
from tests.saml import fixtures


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


class SAMLConfigurationTest(object):
    def test_service_provider_returns_correct_value(self):
        # Arrange
        expected_result = SERVICE_PROVIDER
        configuration_storage = create_autospec(spec=SAMLConfigurationStorage)
        metadata_parser = create_autospec(spec=SAMLMetadataParser)
        metadata_parser.parse = MagicMock(return_value=expected_result)
        configuration = SAMLConfiguration(configuration_storage, metadata_parser)

        # Act
        result = configuration.service_provider

        # Assert
        eq_(result, expected_result)
        metadata_parser.parse.assert_called_once_with(SAMLConfiguration.SP_XML_METADATA)

    def test_identity_providers_returns_correct_value(self):
        # Arrange
        expected_result = IDENTITY_PROVIDERS
        configuration_storage = create_autospec(spec=SAMLConfigurationStorage)
        metadata_parser = create_autospec(spec=SAMLMetadataParser)
        metadata_parser.parse = MagicMock(return_value=expected_result)
        configuration = SAMLConfiguration(configuration_storage, metadata_parser)

        # Act
        result = configuration.identity_providers

        # Assert
        eq_(result, expected_result)
        metadata_parser.parse.assert_called_once_with(SAMLConfiguration.IDP_XML_METADATA)


class SAMLOneLoginConfigurationTest(object):
    def test_get_identity_provider_settings_returns_correct_result(self):
        # Arrange
        configuration = create_autospec(spec=SAMLConfiguration)
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
        configuration = create_autospec(spec=SAMLConfiguration)
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
            },
            'security': {
                'authnRequestsSigned': SERVICE_PROVIDER.authn_requests_signed
            }
        }

        # Act
        result = onelogin_configuration.get_service_provider_settings()

        # Assert
        eq_(result, expected_result)

    def test_get_settings_returns_correct_result(self):
        # Arrange
        configuration = create_autospec(spec=SAMLConfiguration)
        debug = False
        strict = False
        type(configuration).debug = PropertyMock(return_value=False)
        type(configuration).strict = PropertyMock(return_value=False)
        type(configuration).service_provider = PropertyMock(return_value=SERVICE_PROVIDER)
        type(configuration).identity_providers = PropertyMock(return_value=IDENTITY_PROVIDERS)
        onelogin_configuration = SAMLOneLoginConfiguration(configuration)
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
                'authnRequestsSigned':
                    SERVICE_PROVIDER.authn_requests_signed or IDENTITY_PROVIDERS[0].want_authn_requests_signed,
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
