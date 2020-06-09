# FIXME: Required to get rid of the circular import error
import api.app

import urllib
from base64 import b64encode

from mock import create_autospec, PropertyMock, MagicMock
from nose.tools import eq_

from flask import request

from api.authenticator import Authenticator, LibraryAuthenticator, BaseSAMLAuthenticationProvider
from api.saml.auth import SAMLAuthenticationManager, SAMLAuthenticationManagerFactory
from api.saml.configuration import SAMLConfiguration, SAMLOneLoginConfiguration
from api.saml.controller import SAMLController
from api.saml.metadata import ServiceProviderMetadata, UIInfo, NameIDFormat, Service, IdentityProviderMetadata
from api.saml.provider import SAMLAuthenticationProvider
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
        type(configuration).debug = PropertyMock(return_value=False)
        type(configuration).strict = PropertyMock(return_value=False)
        type(configuration).service_provider = PropertyMock(return_value=SERVICE_PROVIDER)
        type(configuration).identity_providers = PropertyMock(return_value=IDENTITY_PROVIDERS)
        onelogin_configuration = SAMLOneLoginConfiguration(configuration)
        authentication_manager = SAMLAuthenticationManager(onelogin_configuration)

        authentication_manager_factory = create_autospec(spec=SAMLAuthenticationManagerFactory)
        authentication_manager_factory.create = MagicMock(return_value=authentication_manager)

        provider = SAMLAuthenticationProvider(self._library, self._integration)
        authenticator = Authenticator(self._db)

        authenticator.library_authenticators['default'].register_saml_provider(provider)

        controller = SAMLController(self.app.manager, authenticator, authentication_manager_factory)

        query = urllib.urlencode({
            'provider': SAMLAuthenticationProvider.NAME,
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
        type(configuration).debug = PropertyMock(return_value=False)
        type(configuration).strict = PropertyMock(return_value=False)
        type(configuration).service_provider = PropertyMock(return_value=SERVICE_PROVIDER)
        type(configuration).identity_providers = PropertyMock(return_value=IDENTITY_PROVIDERS)
        onelogin_configuration = SAMLOneLoginConfiguration(configuration)
        authentication_manager = SAMLAuthenticationManager(onelogin_configuration)

        authentication_manager_factory = create_autospec(spec=SAMLAuthenticationManagerFactory)
        authentication_manager_factory.create = MagicMock(return_value=authentication_manager)

        provider = SAMLAuthenticationProvider(self._library, self._integration)
        authenticator = Authenticator(self._db)

        authenticator.library_authenticators['default'].register_saml_provider(provider)

        controller = SAMLController(self.app.manager, authenticator, authentication_manager_factory)

        query = urllib.urlencode({
            SAMLController.LIBRARY_SHORT_NAME: self._library.short_name,
            SAMLController.PROVIDER_NAME: SAMLAuthenticationProvider.NAME,
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
