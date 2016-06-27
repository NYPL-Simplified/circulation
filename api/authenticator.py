from nose.tools import set_trace
from config import (
    Configuration,
    CannotLoadConfiguration,
)
from core.util.opds_authentication_document import OPDSAuthenticationDocument
from core.util.problem_detail import ProblemDetail

import urlparse
import uuid
import json
import jwt
from flask import Response
from flask.ext.babel import lazy_gettext as _
import importlib


class Authenticator(object):

    BASIC_AUTH = 'basic_auth'
    OAUTH = 'oauth'

    @classmethod
    def initialize(cls, _db, test=False):
        if test:
            from millenium_patron import (
                DummyMilleniumPatronAPI,
            )
            return cls(DummyMilleniumPatronAPI(), [])

        providers = Configuration.policy("authentication")
        if not providers:
            raise CannotLoadConfiguration(
                "No authentication policy given."
            )
        if isinstance(providers, basestring):
            providers = [providers]
        basic_auth_provider = None
        oauth_providers = []

        for provider_string in providers:
            provider_module = importlib.import_module(provider_string)
            provider_class = getattr(provider_module, "AuthenticationAPI")
            if provider_class.TYPE == Authenticator.BASIC_AUTH:
                if basic_auth_provider != None:
                    raise CannotLoadConfiguration(
                        "Two basic auth providers configured"
                    )
                basic_auth_provider = provider_class.from_config()
            elif provider_class.TYPE == Authenticator.OAUTH:
                oauth_providers.append(provider_class.from_config())
            else:
                raise CannotLoadConfiguration(
                    "Unrecognized authentication provider: %s" % provider
                )

        if not basic_auth_provider and not oauth_providers:
            raise CannotLoadConfiguration(
                "No authentication provider configured"
            )
        return cls(basic_auth_provider, oauth_providers)

    def __init__(self, basic_auth_provider=None, oauth_providers=None):
        self.basic_auth_provider = basic_auth_provider
        self.oauth_providers = oauth_providers or []
        self.secret_key = Configuration.get(Configuration.SECRET_KEY)

    def server_side_validation(self, identifier, password):
        if not hasattr(self, 'identifier_re'):
            self.identifier_re = Configuration.policy(
                Configuration.IDENTIFIER_REGULAR_EXPRESSION,
                default=Configuration.DEFAULT_IDENTIFIER_REGULAR_EXPRESSION)
        if not hasattr(self, 'password_re'):
            self.password_re = Configuration.policy(
                Configuration.PASSWORD_REGULAR_EXPRESSION,
                default=Configuration.DEFAULT_PASSWORD_REGULAR_EXPRESSION)

        valid = True
        if self.identifier_re:
            valid = valid and (self.identifier_re.match(identifier) is not None)
        if self.password_re:
            valid = valid and (self.password_re.match(password) is not None)
        return valid

    def decode_token(self, token):
        """Extract auth provider name and access token from JSON web token."""
        decoded = jwt.decode(token, self.secret_key, algorithms=['HS256'])
        provider_name = decoded['iss']
        token = decoded['token']
        return (provider_name, token)

    def create_token(self, provider_name, provider_token):
        """Create a JSON web token with the provider name and access token."""
        payload = dict(
            token=provider_token,
            # I'm not sure this is the correct way to use an
            # Issuer claim (https://tools.ietf.org/html/rfc7519#section-4.1.1).
            # Maybe we should use something custom instead.
            iss=provider_name,
        )
        return jwt.encode(payload, self.secret_key, algorithm='HS256')

    def authenticated_patron(self, _db, header):
        if self.basic_auth_provider and 'password' in header:
            return self.basic_auth_provider.authenticated_patron(_db, header)
        elif self.oauth_providers and 'bearer' in header.lower():
            simplified_token = header.split(' ')[1]
            provider_name, provider_token = self.decode_token(simplified_token)
            for provider in self.oauth_providers:
                if provider_name == provider.NAME:
                    return provider.authenticated_patron(_db, provider_token)
        return None

    def oauth_callback(self, _db, params):
        if self.oauth_providers and params.get('code') and params.get('state'):
            for provider in self.oauth_providers:
                if params.get('state') == provider.NAME:
                    provider_token = provider.oauth_callback(_db, params)
                    
                    if isinstance(provider_token, ProblemDetail):
                        return provider_token

                    # Create an access token for our app that includes the provider
                    # as well as the provider's token.
                    simplified_token = self.create_token(provider.NAME, provider_token)

                    # TODO: should we have a mobile redirect?
                    # In a web application, this is where we'd redirect the client to the
                    # page they came from. A WebView in a mobile app doesn't need that,
                    # but we might want to redirect from a browser back to the app.
                    return Response(json.dumps(dict(access_token=simplified_token)), 200, {"Content-Type": "application/json"})

    def patron_info(self, header):
        if self.basic_auth_provider and 'password' in header:
            return self.basic_auth_provider.patron_info(header.get('username'))
        elif self.oauth_providers and 'bearer' in header.lower():
            simplified_token = header.split(' ')[1]
            provider_name, provider_token = self.decode_token(simplified_token)
            for provider in self.oauth_providers:
                if provider_name == provider.NAME:
                    return provider.patron_info(provider_token)
        return {}
            

    def create_authentication_document(self):
        """Create the OPDS authentication document to be used when
        there's a 401 error.
        """
        base_opds_document = Configuration.base_opds_authentication_document()
        auth_type = [OPDSAuthenticationDocument.BASIC_AUTH_FLOW]

        custom_auth_types = {}
        for provider in self.oauth_providers:
            type = "http://librarysimplified.org/authtype/%s" % provider.NAME
            custom_auth_types[type] = provider
            auth_type.append(type)

        circulation_manager_url = Configuration.integration_url(
            Configuration.CIRCULATION_MANAGER_INTEGRATION, required=True)
        scheme, netloc, path, parameters, query, fragment = (
            urlparse.urlparse(circulation_manager_url))
        opds_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, str(netloc)))

        links = {}
        for rel, value in (
                ("terms-of-service", Configuration.terms_of_service_url()),
                ("privacy-policy", Configuration.privacy_policy_url()),
                ("copyright", Configuration.acknowledgements_url()),
                ("about", Configuration.about_url()),
        ):
            if value:
                links[rel] = dict(href=value, type="text/html")

        doc = OPDSAuthenticationDocument.fill_in(
            base_opds_document, auth_type, unicode(_("Library")), opds_id, None, unicode(_("Barcode")),
            unicode(_("PIN")), links=links
            )

        for type, provider in custom_auth_types.items():
            provider_info = dict(
                authenticate=provider.authenticate_url(),
            )
            doc[type] = provider_info

        return json.dumps(doc)


class BasicAuthAuthenticator(Authenticator):

    TYPE = Authenticator.BASIC_AUTH

    def testing_patron(self, _db):
        """Return a real Patron object reserved for testing purposes.

        :return: A 2-tuple (Patron, password)
        """
        if self.test_username is None or self.test_password is None:
            return None, None
        header = dict(username=self.test_username, password=self.test_password)
        return self.authenticated_patron(_db, header), self.test_password
