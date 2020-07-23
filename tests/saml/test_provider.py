import datetime
import json

from freezegun import freeze_time
from mock import patch, create_autospec, MagicMock
from nose.tools import eq_
from parameterized import parameterized

from api.authenticator import PatronData
from api.saml.auth import SAMLAuthenticationManager, SAMLAuthenticationManagerFactory
from api.saml.configuration import SAMLConfiguration, SAMLOneLoginConfiguration
from api.saml.metadata import ServiceProviderMetadata, NameIDFormat, UIInfo, Service, IdentityProviderMetadata, \
    LocalizableMetadataItem, Subject, AttributeStatement, SAMLAttributes, SubjectJSONEncoder, Organization
from api.saml.provider import SAMLWebSSOAuthenticationProvider, SAML_INVALID_SUBJECT
from core.util.problem_detail import ProblemDetail
from tests.saml import fixtures
from tests.saml.controller_test import ControllerTest

SERVICE_PROVIDER = ServiceProviderMetadata(
    fixtures.SP_ENTITY_ID,
    UIInfo(),
    Organization(),
    NameIDFormat.UNSPECIFIED.value,
    Service(fixtures.SP_ACS_URL, fixtures.SP_ACS_BINDING)
)

IDENTITY_PROVIDER_WITH_DISPLAY_NAME = IdentityProviderMetadata(
    fixtures.IDP_2_ENTITY_ID,
    UIInfo(
        display_names=[
            LocalizableMetadataItem(fixtures.IDP_1_UI_INFO_EN_DISPLAY_NAME, 'en'),
            LocalizableMetadataItem(fixtures.IDP_1_UI_INFO_ES_DISPLAY_NAME, 'es')
        ],
        descriptions=[
            LocalizableMetadataItem(fixtures.IDP_1_UI_INFO_DESCRIPTION, 'en'),
            LocalizableMetadataItem(fixtures.IDP_1_UI_INFO_DESCRIPTION, 'es')
        ],
        information_urls=[
            LocalizableMetadataItem(fixtures.IDP_1_UI_INFO_INFORMATION_URL, 'en'),
            LocalizableMetadataItem(fixtures.IDP_1_UI_INFO_INFORMATION_URL, 'es')
        ],
        privacy_statement_urls=[
            LocalizableMetadataItem(fixtures.IDP_1_UI_INFO_PRIVACY_STATEMENT_URL, 'en'),
            LocalizableMetadataItem(fixtures.IDP_1_UI_INFO_PRIVACY_STATEMENT_URL, 'es')
        ],
        logo_urls=[
            LocalizableMetadataItem(fixtures.IDP_1_UI_INFO_LOGO_URL, 'en'),
            LocalizableMetadataItem(fixtures.IDP_1_UI_INFO_LOGO_URL, 'es')
        ]
    ),
    Organization(),
    NameIDFormat.UNSPECIFIED.value,
    Service(fixtures.IDP_2_SSO_URL, fixtures.IDP_2_SSO_BINDING)
)

IDENTITY_PROVIDER_WITH_ORGANIZATION_DISPLAY_NAME = IdentityProviderMetadata(
    fixtures.IDP_2_ENTITY_ID,
    UIInfo(),
    Organization(
        organization_display_names=[
            LocalizableMetadataItem(fixtures.IDP_1_ORGANIZATION_EN_ORGANIZATION_DISPLAY_NAME, 'en'),
            LocalizableMetadataItem(fixtures.IDP_1_ORGANIZATION_ES_ORGANIZATION_DISPLAY_NAME, 'es')
        ]
    ),
    NameIDFormat.UNSPECIFIED.value,
    Service(fixtures.IDP_2_SSO_URL, fixtures.IDP_2_SSO_BINDING)
)

IDENTITY_PROVIDER_WITHOUT_DISPLAY_NAMES = IdentityProviderMetadata(
    fixtures.IDP_1_ENTITY_ID,
    UIInfo(),
    Organization(),
    NameIDFormat.UNSPECIFIED.value,
    Service(fixtures.IDP_1_SSO_URL, fixtures.IDP_1_SSO_BINDING)
)


class SAMLWebSSOAuthenticationProviderTest(ControllerTest):
    @parameterized.expand([
        (
            'identity_provider_with_display_name',
            [
                IDENTITY_PROVIDER_WITH_DISPLAY_NAME
            ],
            {
                'type': SAMLWebSSOAuthenticationProvider.FLOW_TYPE,
                'description': SAMLWebSSOAuthenticationProvider.NAME,
                'links': [
                    {
                        'rel': 'authenticate',
                        'href': 'http://localhost/default/saml_authenticate?idp_entity_id=http%3A%2F%2Fidp2.hilbertteam.net%2Fidp%2Fshibboleth&provider=SAML+2.0+Web+SSO',
                        'display_names': [
                            {
                                'value': fixtures.IDP_1_UI_INFO_EN_DISPLAY_NAME,
                                'language': 'en'
                            },
                            {
                                'value': fixtures.IDP_1_UI_INFO_ES_DISPLAY_NAME,
                                'language': 'es'
                            }
                        ],
                        'descriptions': [
                            {
                                'value': fixtures.IDP_1_UI_INFO_DESCRIPTION,
                                'language': 'en'
                            },
                            {
                                'value': fixtures.IDP_1_UI_INFO_DESCRIPTION,
                                'language': 'es'
                            }
                        ],
                        'information_urls': [
                            {
                                'value': fixtures.IDP_1_UI_INFO_INFORMATION_URL,
                                'language': 'en'
                            },
                            {
                                'value': fixtures.IDP_1_UI_INFO_INFORMATION_URL,
                                'language': 'es'
                            }
                        ],
                        'privacy_statement_urls': [
                            {
                                'value': fixtures.IDP_1_UI_INFO_PRIVACY_STATEMENT_URL,
                                'language': 'en'
                            },
                            {
                                'value': fixtures.IDP_1_UI_INFO_PRIVACY_STATEMENT_URL,
                                'language': 'es'
                            }
                        ],
                        'logo_urls': [
                            {
                                'value': fixtures.IDP_1_UI_INFO_LOGO_URL,
                                'language': 'en'
                            },
                            {
                                'value': fixtures.IDP_1_UI_INFO_LOGO_URL,
                                'language': 'es'
                            }
                        ]
                    }
                ]
            }
        ),
        (
            'identity_provider_with_organization_display_name',
            [
                IDENTITY_PROVIDER_WITH_ORGANIZATION_DISPLAY_NAME
            ],
            {
                'type': SAMLWebSSOAuthenticationProvider.FLOW_TYPE,
                'description': SAMLWebSSOAuthenticationProvider.NAME,
                'links': [
                    {
                        'rel': 'authenticate',
                        'href': 'http://localhost/default/saml_authenticate?idp_entity_id=http%3A%2F%2Fidp2.hilbertteam.net%2Fidp%2Fshibboleth&provider=SAML+2.0+Web+SSO',
                        'display_names': [
                            {
                                'value': fixtures.IDP_1_ORGANIZATION_EN_ORGANIZATION_DISPLAY_NAME,
                                'language': 'en'
                            },
                            {
                                'value': fixtures.IDP_1_ORGANIZATION_ES_ORGANIZATION_DISPLAY_NAME,
                                'language': 'es'
                            }
                        ],
                        'descriptions': [],
                        'information_urls': [],
                        'privacy_statement_urls': [],
                        'logo_urls': []
                    }
                ]
            }

        ),
        (
            'identity_provider_without_display_names_and_default_template',
            [
                IDENTITY_PROVIDER_WITHOUT_DISPLAY_NAMES,
                IDENTITY_PROVIDER_WITHOUT_DISPLAY_NAMES
            ],
            {
                'type': SAMLWebSSOAuthenticationProvider.FLOW_TYPE,
                'description': SAMLWebSSOAuthenticationProvider.NAME,
                'links': [
                    {
                        'rel': 'authenticate',
                        'href': 'http://localhost/default/saml_authenticate?idp_entity_id=http%3A%2F%2Fidp1.hilbertteam.net%2Fidp%2Fshibboleth&provider=SAML+2.0+Web+SSO',
                        'display_names': [
                            {
                                'value': SAMLConfiguration.IDP_DISPLAY_NAME_DEFAULT_TEMPLATE.format(1),
                                'language': 'en'
                            }
                        ],
                        'descriptions': [],
                        'information_urls': [],
                        'privacy_statement_urls': [],
                        'logo_urls': []
                    },
                    {
                        'rel': 'authenticate',
                        'href': 'http://localhost/default/saml_authenticate?idp_entity_id=http%3A%2F%2Fidp1.hilbertteam.net%2Fidp%2Fshibboleth&provider=SAML+2.0+Web+SSO',
                        'display_names': [
                            {
                                'value': SAMLConfiguration.IDP_DISPLAY_NAME_DEFAULT_TEMPLATE.format(2),
                                'language': 'en'
                            }
                        ],
                        'descriptions': [],
                        'information_urls': [],
                        'privacy_statement_urls': [],
                        'logo_urls': []
                    }
                ]
            }

        )
    ])
    def test_authentication_document(
            self,
            name,
            identity_providers,
            expected_result):
        # Arrange
        provider = SAMLWebSSOAuthenticationProvider(self._default_library, self._integration)
        configuration = create_autospec(spec=SAMLConfiguration)
        configuration.get_debug = MagicMock(return_value=False)
        configuration.get_strict = MagicMock(return_value=False)
        configuration.get_service_provider = MagicMock(return_value=SERVICE_PROVIDER)
        configuration.get_identity_providers = MagicMock(return_value=identity_providers)
        onelogin_configuration = SAMLOneLoginConfiguration(configuration)
        authentication_manager = SAMLAuthenticationManager(onelogin_configuration)

        authentication_manager_factory = create_autospec(spec=SAMLAuthenticationManagerFactory)
        authentication_manager_factory.create = MagicMock(return_value=authentication_manager)

        with patch('api.saml.provider.SAMLAuthenticationManagerFactory') \
                as authentication_manager_factory_constructor:
            authentication_manager_factory_constructor.return_value = authentication_manager_factory

            # Act
            with self.app.test_request_context('/'):
                result = provider.authentication_flow_document(self._db)

            # Assert
            eq_(result, expected_result)

    @parameterized.expand([
        (
            'empty_subject',
            None,
            SAML_INVALID_SUBJECT.detailed('Subject is empty')
        ),
        (
            'subject_is_patron_data',
            PatronData(permanent_id=12345),
            PatronData(permanent_id=12345)
        ),
        (
            'subject_does_not_have_unique_id',
            Subject(None, None),
            SAML_INVALID_SUBJECT.detailed('Subject does not have a unique ID')
        ),
        (
            'subject_has_unique_id',
            Subject(
                None,
                AttributeStatement({SAMLAttributes.eduPersonUniqueId.name: ['12345']})
            ),
            PatronData(
                permanent_id='12345',
                authorization_identifier='12345',
                external_type='A',
                complete=True
            )
        )
    ])
    def test_remote_patron_lookup(self, name, subject, expected_result):
        # Arrange
        provider = SAMLWebSSOAuthenticationProvider(self._default_library, self._integration)

        # Act
        result = provider.remote_patron_lookup(subject)

        # Assert
        if isinstance(result, ProblemDetail):
            eq_(result.response, expected_result.response)
        else:
            eq_(result, expected_result)

    @parameterized.expand([
        (
            'empty_subject',
            None,
            SAML_INVALID_SUBJECT.detailed('Subject is empty')
        ),
        (
            'subject_does_not_have_unique_id',
            Subject(None, None),
            SAML_INVALID_SUBJECT.detailed('Subject does not have a unique ID')
        ),
        (
            'subject_has_unique_id',
            Subject(
                None,
                AttributeStatement({
                    SAMLAttributes.eduPersonUniqueId.name: ['12345']
                })
            ),
            PatronData(
                permanent_id='12345',
                authorization_identifier='12345',
                external_type='A',
                complete=True
            )
        ),
        (
            'subject_has_unique_id_and_non_default_expiration_timeout',
            Subject(
                None,
                AttributeStatement({
                    SAMLAttributes.eduPersonUniqueId.name: ['12345']
                }),
                valid_till=datetime.timedelta(days=1)
            ),
            PatronData(
                permanent_id='12345',
                authorization_identifier='12345',
                external_type='A',
                complete=True
            )
        )
    ])
    @freeze_time("2020-01-01 00:00:00")
    def test_saml_callback(self, name, subject, expected_result):
        # Arrange
        provider = SAMLWebSSOAuthenticationProvider(self._default_library, self._integration)
        expected_credential = json.dumps(subject, cls=SubjectJSONEncoder)

        # Act
        result = provider.saml_callback(self._db, subject)

        # Assert
        if isinstance(result, ProblemDetail):
            eq_(result.response, expected_result.response)
        else:
            credential, patron, patron_data = result

            eq_(credential.credential, expected_credential)
            eq_(patron.external_identifier, expected_result.permanent_id)
            eq_(patron_data, expected_result)
            eq_(credential.expires, datetime.datetime.utcnow() + subject.valid_till)
