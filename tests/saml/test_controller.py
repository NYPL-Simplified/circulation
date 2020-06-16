import urllib
from base64 import b64encode

from flask import request
from mock import create_autospec, MagicMock, patch, PropertyMock
from nose.tools import eq_
from parameterized import parameterized

from api.authenticator import Authenticator
from api.saml.auth import SAMLAuthenticationManager, SAMLAuthenticationManagerFactory
from api.saml.configuration import SAMLConfiguration, SAMLOneLoginConfiguration
from api.saml.controller import SAMLController, SAML_INVALID_REQUEST
from api.saml.metadata import ServiceProviderMetadata, UIInfo, NameIDFormat, Service, IdentityProviderMetadata
from api.saml.provider import SAMLWebSSOAuthenticationProvider
from core.util.problem_detail import ProblemDetail
from tests.saml import fixtures
from tests.saml.controller_test import ControllerTest
from tests.saml.test_auth import SAML_RESPONSE

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
        Service(fixtures.IDP_1_SSO_URL, fixtures.IDP_1_SSO_BINDING),
        signing_certificates=[
            fixtures.SIGNING_CERTIFICATE
        ]
    ),
    IdentityProviderMetadata(
        fixtures.IDP_2_ENTITY_ID,
        UIInfo(),
        NameIDFormat.UNSPECIFIED.value,
        Service(fixtures.IDP_2_SSO_URL, fixtures.IDP_2_SSO_BINDING)
    )
]


class SAMLControllerTest(ControllerTest):
    @parameterized.expand([
        (
            'with_missing_provider_name',
            None,
            None,
            None,
            SAML_INVALID_REQUEST.detailed('Required parameter provider is missing'),
            None
        ),
        (
            'with_missing_idp_entity_id',
            SAMLWebSSOAuthenticationProvider.NAME,
            None,
            None,
            SAML_INVALID_REQUEST.detailed('Required parameter idp_entity_id is missing'),
            None
        ),
        (
            'with_missing_redirect_uri',
            SAMLWebSSOAuthenticationProvider.NAME,
            IDENTITY_PROVIDERS[0].entity_id,
            None,
            SAML_INVALID_REQUEST.detailed('Required parameter redirect_uri is missing'),
            'http://localhost?' + urllib.urlencode({
                SAMLController.LIBRARY_SHORT_NAME: 'default',
                SAMLController.PROVIDER_NAME: SAMLWebSSOAuthenticationProvider.NAME,
                SAMLController.IDP_ENTITY_ID: IDENTITY_PROVIDERS[0].entity_id
            })
        ),
        (
            'with_all_parameters_set',
            SAMLWebSSOAuthenticationProvider.NAME,
            IDENTITY_PROVIDERS[0].entity_id,
            'http://localhost',
            None,
            'http://localhost?' + urllib.urlencode({
                SAMLController.LIBRARY_SHORT_NAME: 'default',
                SAMLController.PROVIDER_NAME: SAMLWebSSOAuthenticationProvider.NAME,
                SAMLController.IDP_ENTITY_ID: IDENTITY_PROVIDERS[0].entity_id
            })
        )
    ])
    def test_saml_authentication_redirect(
            self,
            name,
            provider_name,
            idp_entity_id,
            redirect_uri,
            expected_problem,
            expected_relay_state):
        # Arrange
        expected_authentication_redirect_uri = 'https://idp.circulationmanager.org'
        authentication_manager = create_autospec(spec=SAMLAuthenticationManager)
        authentication_manager.start_authentication = MagicMock(return_value=expected_authentication_redirect_uri)
        provider = create_autospec(spec=SAMLWebSSOAuthenticationProvider)
        type(provider).NAME = PropertyMock(return_value=SAMLWebSSOAuthenticationProvider.NAME)
        provider.get_authentication_manager = MagicMock(return_value=authentication_manager)
        provider.library = MagicMock(return_value=self._default_library)
        authenticator = Authenticator(self._db)

        authenticator.library_authenticators['default'].register_saml_provider(provider)

        controller = SAMLController(self.app.manager, authenticator)
        params = {}

        if provider_name:
            params[SAMLController.PROVIDER_NAME] = provider_name
        if idp_entity_id:
            params[SAMLController.IDP_ENTITY_ID] = idp_entity_id
        if redirect_uri:
            params[SAMLController.REDIRECT_URI] = redirect_uri

        query = urllib.urlencode(params)

        with self.app.test_request_context('/saml_authenticate?' + query):
            request.library = self._default_library

            # Act
            result = controller.saml_authentication_redirect(request.args, self._db)

            # Assert
            if expected_problem:
                assert isinstance(result, ProblemDetail)
                eq_(result.response, expected_problem.response)
            else:
                eq_(result.status_code, 302)
                eq_(result.headers.get('Location'), expected_authentication_redirect_uri)

                authentication_manager.start_authentication.assert_called_once_with(
                    self._db, idp_entity_id, expected_relay_state)

    def test_saml_authentication_callback(self):
        configuration = create_autospec(spec=SAMLConfiguration)
        configuration.get_debug = MagicMock(return_value=False)
        configuration.get_strict = MagicMock(return_value=False)
        configuration.get_service_provider = MagicMock(return_value=SERVICE_PROVIDER)
        configuration.get_identity_providers = MagicMock(return_value=IDENTITY_PROVIDERS)
        onelogin_configuration = SAMLOneLoginConfiguration(configuration)
        authentication_manager = SAMLAuthenticationManager(onelogin_configuration)

        authentication_manager_factory = create_autospec(spec=SAMLAuthenticationManagerFactory)
        authentication_manager_factory.create = MagicMock(return_value=authentication_manager)

        with patch('api.saml.provider.SAMLAuthenticationManagerFactory', autospec=True) \
                as authentication_manager_factory_constructor:
            authentication_manager_factory_constructor.return_value = authentication_manager_factory

            provider = SAMLWebSSOAuthenticationProvider(self._default_library, self._integration)
            authenticator = Authenticator(self._db)

            authenticator.library_authenticators['default'].register_saml_provider(provider)

            controller = SAMLController(self.app.manager, authenticator)

            query = urllib.urlencode({
                SAMLController.LIBRARY_SHORT_NAME: self._default_library.short_name,
                SAMLController.PROVIDER_NAME: SAMLWebSSOAuthenticationProvider.NAME,
                SAMLController.IDP_ENTITY_ID: IDENTITY_PROVIDERS[0].entity_id
            })

            authenticator.bearer_token_signing_secret = 'test'
            authenticator.library_authenticators['default'].bearer_token_signing_secret = 'test'

            saml_response = b64encode(SAML_RESPONSE)
            with self.app.test_request_context('/', data={
                'SAMLResponse': saml_response,
                SAMLController.RELAY_STATE: 'http://localhost?' + query
            }):
                controller.saml_authentication_callback(request, self._db)
