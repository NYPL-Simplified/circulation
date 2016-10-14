"""Test the base authentication framework: that is, the classes that
don't interact with any particular source of truth.
"""

from flask.ext.babel import lazy_gettext as _
from nose.tools import (
    assert_raises_regexp,
    eq_,
    set_trace,
)

import datetime
import json
import os

from core.model import (
    Patron
)

from core.util.problem_detail import (
    ProblemDetail,
)
from core.util.opds_authentication_document import (
    OPDSAuthenticationDocument,
)


from api.authenticator import (
    Authenticator,
    AuthenticationProvider,
    BasicAuthenticationProvider,
    OAuthController,
    OAuthAuthenticationProvider,
    PatronData,
)

from api.config import (
    CannotLoadConfiguration,
    Configuration,
    temp_config,
)

from api.problem_details import *

from . import DatabaseTest

class MockAuthenticationProvider(object):
    """An AuthenticationProvider that always authenticates requests for
    the given Patron and always returns the given PatronData when
    asked to look up data.
    """
    def __init__(self, patron=None, patrondata=None):
        self.patron = patron
        self.patrondata = patrondata

    def authenticate(self, _db, header):
        return self.patrondata
           

class MockBasicAuthenticationProvider(
        BasicAuthenticationProvider,
        MockAuthenticationProvider
):
    def __init__(self, patron=None, patrondata=None):
        super(MockBasicAuthenticationProvider, self).__init__()
        self.patron = patron
        self.patrondata = patrondata

    def remote_authenticate(self, username, password):
        return self.patrondata

    def remote_patron_lookup(self, patrondata):
        return self.patrondata
    
class MockOAuthAuthenticationProvider(
        OAuthAuthenticationProvider,
        MockAuthenticationProvider
):
    def __init__(self, provider_name, patron=None, patrondata=None):
        self.NAME = provider_name
        self.patron = patron
        self.patrondata = patrondata

    def authenticated_patron(self, _db, provider_token):
        return self.patron
        
class TestPatronData(DatabaseTest):

    def setup(self):
        super(TestPatronData, self).setup()
        self.data = PatronData(
            permanent_id="1",
            authorization_identifier="2",
            username="3",
            personal_name="4",
            email_address="5",
            authorization_expires=datetime.datetime.utcnow(),
            fines="6",
            blocked=False,
        )
        
    
    def test_apply(self):
        patron = self._patron()

        self.data.apply(patron)
        eq_(self.data.permanent_id, patron.external_identifier)
        eq_(self.data.authorization_identifier, patron.authorization_identifier)
        eq_(self.data.username, patron.username)
        eq_(self.data.authorization_expires, patron.authorization_expires)
        eq_(self.data.fines, patron.fines)

        # TODO: blocked is not stored but should be.
        eq_(False, self.data.blocked)

        # This data is stored in PatronData but not applied to Patron.
        eq_("4", self.data.personal_name)
        eq_(False, hasattr(patron, 'personal_name'))
        eq_("5", self.data.email_address)
        eq_(False, hasattr(patron, 'email_address'))

    def test_to_response_parameters(self):

        params = self.data.to_response_parameters
        eq_(dict(name="4"), params)


class TestAuthenticator(DatabaseTest):

    def test_from_config(self):
        """TODO: Since registration happens by loading modules, do this
        after porting over some authorization providers.
        """

    def test_config_fails_when_no_providers_specified(self):
        with temp_config() as config:
            config[Configuration.POLICIES] = {
                Configuration.AUTHENTICATION_POLICY: []
            }
            assert_raises_regexp(
                CannotLoadConfiguration, "No authentication policy given."
            )
        
    def test_register_basic_auth_provider(self):
        """TODO: Since registration happens by loading a module, do this
        after porting over (say) FirstBook authorization provider.
        """

    def test_register_oauth_provider(self):
        """TODO: Since registration happens by loading a module, do this
        after porting over (say) Clever authorization provider.
        """
            
    def test_oauth_provider_requires_secret(self):
        basic = MockBasicAuthenticationProvider()
        oauth = MockOAuthAuthenticationProvider("provider1")

        # You can create an Authenticator that only uses Basic Auth
        # without providing a secret.
        Authenticator(basic_auth_provider=basic)

        # You can create an Authenticator that uses OAuth if you
        # provide a secret.
        Authenticator(
            oauth_providers=[oauth], bearer_token_signing_secret="foo"
        )
        
        # But you can't create an Authenticator that uses OAuth
        # without providing a secret.
        assert_raises_regexp(
            Authenticator,
            "OAuth providers are configured, but secret for signing bearer tokens is not.",
            oauth_providers=[oauth]
        )
        
    def test_providers(self):
        basic = MockBasicAuthenticationProvider()
        oauth1 = MockOAuthAuthenticationProvider("provider1")
        oauth2 = MockOAuthAuthenticationProvider("provider2")

        authenticator = Authenticator(
            basic_auth_provider=basic, oauth_providers=[oauth1, oauth2],
            bearer_token_signing_secret='foo'
        )
        eq_([basic, oauth1, oauth2], list(authenticator.providers))

    def test_provider_registration(self):
        """You can register the same provider multiple times,
        but you can't register two different basic auth providers,
        and you can't register two different OAuth providers
        with the same .NAME.
        """
        authenticator = Authenticator(bearer_token_signing_secret='foo')
        basic1 = MockBasicAuthenticationProvider()
        basic2 = MockBasicAuthenticationProvider()
        oauth1 = MockOAuthAuthenticationProvider("provider1")
        oauth2 = MockOAuthAuthenticationProvider("provider2")
        oauth1_dupe = MockOAuthAuthenticationProvider("provider1")

        authenticator.register_basic_auth_provider(basic1)
        authenticator.register_basic_auth_provider(basic1)

        assert_raises_regexp(
            CannotLoadConfiguration,
            "Two basic auth providers configured",
            authenticator.register_basic_auth_provider, basic2
        )

        authenticator.register_oauth_provider(oauth1)
        authenticator.register_oauth_provider(oauth1)
        authenticator.register_oauth_provider(oauth2)

        assert_raises_regexp(
            CannotLoadConfiguration,
            'Two different OAuth providers claim the name "provider1"',
            authenticator.register_oauth_provider, oauth1_dupe
        )
        
    def test_oauth_provider_lookup(self):

        # If there are no OAuth providers we cannot look one up.
        basic = MockBasicAuthenticationProvider()
        authenticator = Authenticator(basic_auth_provider=basic)
        problem = authenticator.oauth_provider_lookup("provider1")
        eq_(problem.uri, UNKNOWN_OAUTH_PROVIDER.uri)
        eq_(_("No OAuth providers are configured."), problem.detail)
        
        # We can look up registered providers but not unregistered providers.
        oauth1 = MockOAuthAuthenticationProvider("provider1")
        oauth2 = MockOAuthAuthenticationProvider("provider2")
        oauth3 = MockOAuthAuthenticationProvider("provider3")
        authenticator = Authenticator(
            oauth_providers=[oauth1, oauth2],
            bearer_token_signing_secret='foo'
        )

        provider = authenticator.oauth_provider_lookup("provider1")
        eq_(oauth1, provider)

        problem = authenticator.oauth_provider_lookup("provider3")
        eq_(problem.uri, UNKNOWN_OAUTH_PROVIDER.uri)
        eq_(
            _("The specified OAuth provider name isn't one of the known providers. The known providers are: provider1, provider2"),
            problem.detail
        )

    def test_authenticated_patron_basic(self):
        patron = self._patron()
        patrondata = PatronData(
            permanent_id=patron.external_identifier,
            authorization_identifier=patron.authorization_identifier,
            username=patron.username
        )
        basic = MockBasicAuthenticationProvider(
            patron=patron, patrondata=patrondata
        )
        authenticator = Authenticator(basic_auth_provider=basic)
        eq_(
            patron,
            authenticator.authenticated_patron(
                self._db, dict(username="foo", password="bar")
            )
        )        

        # OAuth doesn't work.
        problem = authenticator.authenticated_patron(
            self._db, "Bearer abcd"
        )
        eq_(UNSUPPORTED_AUTHENTICATION_MECHANISM, problem)
        
    def test_authenticated_patron_oauth(self):
        patron1 = self._patron()
        patron2 = self._patron()
        oauth1 = MockOAuthAuthenticationProvider("oauth1", patron=patron1)
        oauth2 = MockOAuthAuthenticationProvider("oauth2", patron=patron2)
        authenticator = Authenticator(
            oauth_providers=[oauth1, oauth2],
            bearer_token_signing_secret='foo'
        )

        # Ask oauth1 to create a bearer token.
        token = authenticator.create_bearer_token(oauth1.NAME, "some token")
        
        # The authenticator will decode the bearer token into a
        # provider and a provider token. It will look up the oauth1
        # provider (as opposed to oauth2) and ask it to authenticate
        # the provider token.
        #
        # This gives us patron1, as opposed to patron2.
        authenticated = authenticator.authenticated_patron(
            self._db, "Bearer " + token
        )
        eq_(patron1, authenticated)

        # Basic auth doesn't work.
        problem = authenticator.authenticated_patron(
            self._db, dict(username="foo", password="bar")
        )
        eq_(UNSUPPORTED_AUTHENTICATION_MECHANISM, problem)

    def test_authenticated_patron_unsupported_mechanism(self):
        authenticator = Authenticator()
        problem = authenticator.authenticated_patron(
            self._db, object()
        )
        eq_(UNSUPPORTED_AUTHENTICATION_MECHANISM, problem)
        
    def test_create_bearer_token(self):
        oauth1 = MockOAuthAuthenticationProvider("oauth1")
        oauth2 = MockOAuthAuthenticationProvider("oauth2")
        authenticator = Authenticator(
            oauth_providers=[oauth1, oauth2],
            bearer_token_signing_secret='foo'
        )

        # A token is created and signed with the bearer token.
        token1 = authenticator.create_bearer_token(oauth1.NAME, "some token")
        eq_("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvYXV0aDEiLCJ0b2tlbiI6InNvbWUgdG9rZW4ifQ.Ve-bbEN4mdWQdR-VA6gbrK2xOz2KRbmPhttmTTCA0ng",
            token1
        )

        # Varying the name of the OAuth provider varies the bearer
        # token.
        token2 = authenticator.create_bearer_token(oauth2.NAME, "some token")
        assert token1 != token2

        # Varying the token sent by the OAuth provider varies the
        # bearer token.
        token3 = authenticator.create_bearer_token(
            oauth1.NAME, "some other token"
        )
        assert token3 != token1
        
        # Varying the secret used to sign the token varies the bearer
        # token.
        authenticator.bearer_token_signing_secret = "a different secret"
        token4 = authenticator.create_bearer_token(oauth1.NAME, "some token")
        assert token4 != token1
        
    def test_decode_bearer_token(self):
        oauth = MockOAuthAuthenticationProvider("oauth")
        authenticator = Authenticator(
            oauth_providers=[oauth],
            bearer_token_signing_secret='secret'
        )

        # A token is created and signed with the secret.
        token_value = (oauth.NAME, "some token")
        encoded = authenticator.create_bearer_token(*token_value)
        decoded = authenticator.decode_bearer_token(encoded)
        eq_(token_value, decoded)

        decoded = authenticator.decode_bearer_token_from_header(
            "Bearer " + encoded
        )
        eq_(token_value, decoded)

    def test_create_authentication_document(self):
        basic = MockBasicAuthenticationProvider()
        oauth = MockOAuthAuthenticationProvider("oauth")
        oauth.URI = "http://example.org/"
        authenticator = Authenticator(
            basic_auth_provider=basic, oauth_providers=[oauth],
            bearer_token_signing_secret='secret'
        )

        # We're about to call url_for, so we must create an
        # application context.
        os.environ['AUTOINITIALIZE'] = "False"
        from api.app import app
        self.app = app
        del os.environ['AUTOINITIALIZE']

        with self.app.test_request_context("/"):        
            doc = json.loads(authenticator.create_authentication_document())
            # TODO: It would be good to verify other stuff such as the
            # links, but the main thing we need to test is that the
            # sub-documents are assembled properly and placed in the
            # right position.
            providers = doc['providers']
            basic_doc = providers[basic.URI]
            expect_basic = basic.authentication_provider_document
            eq_(expect_basic, basic_doc)
            
            oauth_doc = providers[oauth.URI]
            expect_oauth = oauth.authentication_provider_document
            eq_(expect_oauth, oauth_doc)

            # While we're in this context, let's also test
            # create_authentication_headers.

            # So long as the authenticator includes a basic auth
            # provider, that provider's AUTHENTICATION_HEADER is used
            # for WWW-Authenticate.
            headers = authenticator.create_authentication_headers()
            eq_(OPDSAuthenticationDocument.MEDIA_TYPE, headers['Content-Type'])
            eq_(basic.AUTHENTICATION_HEADER, headers['WWW-Authenticate'])

            # If the authenticator does not include a basic auth provider,
            # no WWW-Authenticate header is provided. 
            authenticator = Authenticator(
                oauth_providers=[oauth],
                bearer_token_signing_secret='secret'
            )
            headers = authenticator.create_authentication_headers()
            assert 'WWW-Authenticate' not in headers
