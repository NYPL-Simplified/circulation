# FIXME: Required to get rid of the circular import error
import api.app

import json

from nose.tools import eq_
from parameterized import parameterized

from api.authenticator import PatronData
from api.saml.configuration import SAMLMetadataSerializer, SAMLConfiguration
from api.saml.metadata import ServiceProviderMetadata, NameIDFormat, UIInfo, Service, IdentityProviderMetadata, \
    LocalizableMetadataItem, Subject, AttributeStatement, SAMLAttributes, SubjectJSONEncoder

from api.saml.provider import SAMLAuthenticationProvider, SAML_INVALID_SUBJECT
from core.model import ExternalIntegration
from core.util.problem_detail import ProblemDetail
from tests.saml import fixtures
from tests.test_controller import ControllerTest

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
        UIInfo(
            display_names=[
                LocalizableMetadataItem('Test Shibboleth IdP', 'en'),
                LocalizableMetadataItem('Test Shibboleth IdP', 'es')
            ],
            descriptions=[
                LocalizableMetadataItem('Test Shibboleth IdP', 'en'),
                LocalizableMetadataItem('Test Shibboleth IdP', 'es')
            ]
        ),
        NameIDFormat.UNSPECIFIED.value,
        Service(fixtures.IDP_2_SSO_URL, fixtures.IDP_2_SSO_BINDING)
    )
]


class SAMLAuthenticationProviderTest(ControllerTest):
    def setup(self, _db=None, set_up_circulation_manager=True):
        super(SAMLAuthenticationProviderTest, self).setup(_db, set_up_circulation_manager)

        self._library = self.make_default_library(self._db)
        self._integration = self._external_integration(
            protocol=SAMLAuthenticationProvider.NAME,
            goal=ExternalIntegration.PATRON_AUTH_GOAL
        )

    def test_authentication_document(self):
        # Arrange
        expected_result = {
            'type': SAMLAuthenticationProvider.FLOW_TYPE,
            'description': SAMLAuthenticationProvider.NAME,
            'links': [
                {
                    'rel': 'authenticate',
                    'href': 'http://localhost/default/saml_authenticate?idp_entity_id=http%3A%2F%2Fidp1.hilbertteam.net%2Fidp%2Fshibboleth&provider=SAML+2.0',
                    'display_names': [],
                    'descriptions': [],
                    'information_urls': [],
                    'privacy_statement_urls': [],
                    'logo_urls': []
                },
                {
                    'rel': 'authenticate',
                    'href': 'http://localhost/default/saml_authenticate?idp_entity_id=http%3A%2F%2Fidp2.hilbertteam.net%2Fidp%2Fshibboleth&provider=SAML+2.0',
                    'display_names': [
                        {
                            'value': 'Test Shibboleth IdP',
                            'language': 'en'
                        },
                        {
                            'value': 'Test Shibboleth IdP',
                            'language': 'es'
                        }
                    ],
                    'descriptions': [
                        {
                            'value': 'Test Shibboleth IdP',
                            'language': 'en'
                        },
                        {
                            'value': 'Test Shibboleth IdP',
                            'language': 'es'
                        }
                    ],
                    'information_urls': [],
                    'privacy_statement_urls': [],
                    'logo_urls': []
                }
            ]
        }
        provider = SAMLAuthenticationProvider(self._library, self._integration)
        metadata_serializer = SAMLMetadataSerializer(self._integration)

        metadata_serializer.serialize(SAMLConfiguration.SP_METADATA, SERVICE_PROVIDER)
        metadata_serializer.serialize(SAMLConfiguration.IDP_METADATA, IDENTITY_PROVIDERS)

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
        provider = SAMLAuthenticationProvider(self._library, self._integration)

        # Act
        result = provider.remote_patron_lookup(subject)

        # Assert
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
        )
    ])
    def test_saml_callback(self, name, subject, expected_result):
        # Arrange
        provider = SAMLAuthenticationProvider(self._library, self._integration)
        expected_credential = json.dumps(subject, cls=SubjectJSONEncoder)

        # Act
        result = provider.saml_callback(self._db, subject)

        # Assert
        if isinstance(result, ProblemDetail):
            eq_(result, expected_result)
        else:
            credential, patron, patron_data = result

            eq_(credential.credential, expected_credential)
            eq_(patron.external_identifier, expected_result.permanent_id)
            eq_(patron_data, expected_result)
