# FIXME: Required to get rid of the circular import error
import api.app

import urllib
from base64 import b64encode

from mock import create_autospec, MagicMock, patch
from nose.tools import eq_

from flask import request

from api.authenticator import Authenticator
from api.saml.auth import SAMLAuthenticationManager, SAMLAuthenticationManagerFactory
from api.saml.configuration import SAMLConfiguration, SAMLOneLoginConfiguration
from api.saml.controller import SAMLController
from api.saml.metadata import ServiceProviderMetadata, UIInfo, NameIDFormat, Service, IdentityProviderMetadata
from api.saml.provider import SAMLWebSSOAuthenticationProvider
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
    def test_saml_authentication_redirect(self):
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

            provider = SAMLWebSSOAuthenticationProvider(self._library, self._integration)
            authenticator = Authenticator(self._db)

            authenticator.library_authenticators['default'].register_saml_provider(provider)

            controller = SAMLController(self.app.manager, authenticator)

            query = urllib.urlencode({
                'provider': SAMLWebSSOAuthenticationProvider.NAME,
                'idp_entity_id': IDENTITY_PROVIDERS[0].entity_id
            })

            with self.app.test_request_context('/saml_authenticate?' + query):
                request.library = self._default_library
                result = controller.saml_authentication_redirect(request.args, self._db)

                print(result)

                eq_(result.status_code, 302)
                location_header = result.headers.get('Location')

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

            provider = SAMLWebSSOAuthenticationProvider(self._library, self._integration)
            authenticator = Authenticator(self._db)

            authenticator.library_authenticators['default'].register_saml_provider(provider)

            controller = SAMLController(self.app.manager, authenticator)

            query = urllib.urlencode({
                SAMLController.LIBRARY_SHORT_NAME: self._library.short_name,
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
