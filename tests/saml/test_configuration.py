import sqlalchemy
from mock import MagicMock, create_autospec, call
from nose.tools import eq_

from api.saml.configuration import SAMLConfiguration, SAMLOneLoginConfiguration, SAMLConfigurationStorage
from api.saml.metadata import ServiceProviderMetadata, UIInfo, Service, NameIDFormat, IdentityProviderMetadata, \
    Organization
from api.saml.parser import SAMLMetadataParser
from tests.saml import fixtures

SERVICE_PROVIDER = ServiceProviderMetadata(
    fixtures.SP_ENTITY_ID,
    UIInfo(),
    Organization(),
    NameIDFormat.UNSPECIFIED.value,
    Service(fixtures.SP_ACS_URL, fixtures.SP_ACS_BINDING),
    private_key=fixtures.PRIVATE_KEY
)

IDENTITY_PROVIDERS = [
    IdentityProviderMetadata(
        fixtures.IDP_1_ENTITY_ID,
        UIInfo(),
        Organization(),
        NameIDFormat.UNSPECIFIED.value,
        Service(fixtures.IDP_1_SSO_URL, fixtures.IDP_1_SSO_BINDING)
    ),
    IdentityProviderMetadata(
        fixtures.IDP_2_ENTITY_ID,
        UIInfo(),
        Organization(),
        NameIDFormat.UNSPECIFIED.value,
        Service(fixtures.IDP_2_SSO_URL, fixtures.IDP_2_SSO_BINDING)
    )
]


class SAMLConfigurationTest(object):
    def test_service_provider_returns_correct_value(self):
        # Arrange
        service_provider_metadata = ''
        expected_result = SERVICE_PROVIDER
        configuration_storage = create_autospec(spec=SAMLConfigurationStorage)
        configuration_storage.load = MagicMock(return_value=service_provider_metadata)
        metadata_parser = create_autospec(spec=SAMLMetadataParser)
        metadata_parser.parse = MagicMock(return_value=[SERVICE_PROVIDER])
        configuration = SAMLConfiguration(configuration_storage, metadata_parser)
        db = create_autospec(spec=sqlalchemy.orm.session.Session)

        # Act
        result = configuration.get_service_provider(db)

        # Assert
        eq_(result, expected_result)
        configuration_storage.load.assert_has_calls([
            call(db, SAMLConfiguration.SP_XML_METADATA),
            call(db, SAMLConfiguration.SP_PRIVATE_KEY)
        ])
        metadata_parser.parse.assert_called_once_with(service_provider_metadata)

    def test_identity_providers_returns_correct_value(self):
        # Arrange
        identity_providers_metadata = ''
        expected_result = IDENTITY_PROVIDERS
        configuration_storage = create_autospec(spec=SAMLConfigurationStorage)
        configuration_storage.load = MagicMock(return_value=identity_providers_metadata)
        metadata_parser = create_autospec(spec=SAMLMetadataParser)
        metadata_parser.parse = MagicMock(return_value=expected_result)
        configuration = SAMLConfiguration(configuration_storage, metadata_parser)
        db = create_autospec(spec=sqlalchemy.orm.session.Session)

        # Act
        result = configuration.get_identity_providers(db)

        # Assert
        eq_(result, expected_result)
        configuration_storage.load.assert_called_once_with(db, SAMLConfiguration.IDP_XML_METADATA)
        metadata_parser.parse.assert_called_once_with(identity_providers_metadata)


class SAMLOneLoginConfigurationTest(object):
    def test_get_identity_provider_settings_returns_correct_result(self):
        # Arrange
        configuration = create_autospec(spec=SAMLConfiguration)
        configuration.get_identity_providers = MagicMock(return_value=IDENTITY_PROVIDERS)
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
        db = create_autospec(spec=sqlalchemy.orm.session.Session)

        # Act
        result = onelogin_configuration.get_identity_provider_settings(db, IDENTITY_PROVIDERS[0].entity_id)

        # Assert
        eq_(result, expected_result)
        configuration.get_identity_providers.assert_called_once_with(db)

    def test_get_service_provider_settings_returns_correct_result(self):
        # Arrange
        configuration = create_autospec(spec=SAMLConfiguration)
        configuration.get_service_provider = MagicMock(return_value=SERVICE_PROVIDER)
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
                'privateKey': SERVICE_PROVIDER.private_key
            },
            'security': {
                'authnRequestsSigned': SERVICE_PROVIDER.authn_requests_signed
            }
        }
        db = create_autospec(spec=sqlalchemy.orm.session.Session)

        # Act
        result = onelogin_configuration.get_service_provider_settings(db)

        # Assert
        eq_(result, expected_result)
        configuration.get_service_provider.assert_called_once_with(db)

    def test_get_settings_returns_correct_result(self):
        # Arrange
        configuration = create_autospec(spec=SAMLConfiguration)
        debug = False
        strict = False
        configuration.get_debug = MagicMock(return_value=False)
        configuration.get_strict = MagicMock(return_value=False)
        configuration.get_service_provider = MagicMock(return_value=SERVICE_PROVIDER)
        configuration.get_identity_providers = MagicMock(return_value=IDENTITY_PROVIDERS)
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
        db = create_autospec(spec=sqlalchemy.orm.session.Session)

        # Act
        result = onelogin_configuration.get_settings(db, IDENTITY_PROVIDERS[0].entity_id)

        # Assert
        eq_(result, expected_result)
        configuration.get_debug.assert_called_with(db)
        configuration.get_strict.assert_called_with(db)
        configuration.get_service_provider.assert_called_with(db)
        configuration.get_identity_providers.assert_called_with(db)
