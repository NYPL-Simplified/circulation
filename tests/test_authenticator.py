from nose.tools import (
    eq_,
    set_trace,
)
import json
from api.config import (
    Configuration,
    temp_config,
)
from api.authenticator import Authenticator
from api.millenium_patron import MilleniumPatronAPI
from api.firstbook import FirstBookAuthenticationAPI
from api.clever import CleverAuthenticationAPI
from . import DatabaseTest

class DummyAuthAPI(Authenticator):
    """ An Auth API that keeps track of how many times it's called."""
    def __init__(self):
        self.count = 0

    def authenticated_patron(self, _db, header):
        self.count = self.count + 1
        return True

    def patron_info(self, header):
        self.count = self.count + 1
        return True

    def oauth_callback(self, _db, params):
        self.count = self.count + 1
        return "token"

class TestAuthenticator(DatabaseTest):

    def test_initialize(self):
        # Only a basic auth provider.
        with temp_config() as config:
            config[Configuration.POLICIES] = {
                Configuration.AUTHENTICATION_POLICY: 'api.millenium_patron'
            }
            config[Configuration.INTEGRATIONS] = {
                MilleniumPatronAPI.NAME: {
                    Configuration.URL: "http://url"
                }
            }
            
            auth = Authenticator.initialize(self._db)

            assert auth.basic_auth_provider != None
            assert isinstance(auth.basic_auth_provider, MilleniumPatronAPI)

            eq_(0, len(auth.oauth_providers))

        # A basic auth provider and an oauth provider.
        with temp_config() as config:
            config[Configuration.POLICIES] = {
                Configuration.AUTHENTICATION_POLICY: ['api.firstbook', 'api.clever']
            }
            config[Configuration.INTEGRATIONS] = {
                FirstBookAuthenticationAPI.NAME: {
                    Configuration.URL: "http://url",
                    FirstBookAuthenticationAPI.SECRET_KEY: "secret",
                },
                CleverAuthenticationAPI.NAME: {
                    Configuration.OAUTH_CLIENT_ID: 'client_id',
                    Configuration.OAUTH_CLIENT_SECRET: 'client_secret',
                }
            }

            auth = Authenticator.initialize(self._db)

            assert auth.basic_auth_provider != None
            assert isinstance(auth.basic_auth_provider, FirstBookAuthenticationAPI)

            eq_(1, len(auth.oauth_providers))
            assert isinstance(auth.oauth_providers[0], CleverAuthenticationAPI)

        # Only an oauth provider
        with temp_config() as config:
            config[Configuration.POLICIES] = {
                Configuration.AUTHENTICATION_POLICY: 'api.clever'
            }
            config[Configuration.INTEGRATIONS] = {
                CleverAuthenticationAPI.NAME: {
                    Configuration.OAUTH_CLIENT_ID: 'client_id',
                    Configuration.OAUTH_CLIENT_SECRET: 'client_secret',
                }
            }

            auth = Authenticator.initialize(self._db)
            eq_(None, auth.basic_auth_provider)

            eq_(1, len(auth.oauth_providers))
            assert isinstance(auth.oauth_providers[0], CleverAuthenticationAPI)

    def test_create_decode_token(self):
        auth = Authenticator.initialize(self._db, test=True)
        token = auth.create_token("Provider name", "Provider token")

        decoded_name, decoded_token = auth.decode_token(token)

        eq_("Provider name", decoded_name)
        eq_("Provider token", decoded_token)

    def test_authenticated_patron(self):

        # Check that the correct auth provider is called.
        basic_auth = DummyAuthAPI()
        oauth1 = DummyAuthAPI()
        oauth1.NAME = "oauth1"
        oauth2 = DummyAuthAPI()
        oauth2.NAME = "oauth2"

        auth = Authenticator.initialize(self._db, test=True)
        auth.basic_auth_provider = basic_auth
        auth.oauth_providers = [oauth1, oauth2]
        
        # Basic auth
        header = dict(username="foo", password="bar")
        auth.authenticated_patron(self._db, header)
        eq_(1, basic_auth.count)
        eq_(0, oauth1.count)
        eq_(0, oauth2.count)

        # Oauth 1
        token = auth.create_token("oauth1", "token")
        header = "Bearer: %s" % token
        auth.authenticated_patron(self._db, header)
        eq_(1, basic_auth.count)
        eq_(1, oauth1.count)
        eq_(0, oauth2.count)

        # Oauth 2
        token = auth.create_token("oauth2", "token")
        header = "Bearer: %s" % token
        auth.authenticated_patron(self._db, header)
        eq_(1, basic_auth.count)
        eq_(1, oauth1.count)
        eq_(1, oauth2.count)

    def test_oauth_callback(self):

        # Check that the correct auth provider is called.
        basic_auth = DummyAuthAPI()
        oauth1 = DummyAuthAPI()
        oauth1.NAME = "oauth1"
        oauth2 = DummyAuthAPI()
        oauth2.NAME = "oauth2"

        auth = Authenticator.initialize(self._db, test=True)
        auth.basic_auth_provider = basic_auth
        auth.oauth_providers = [oauth1, oauth2]
        
        # Oauth 1
        params = dict(code="foo", state="oauth1")
        response = auth.oauth_callback(self._db, params)
        eq_(0, basic_auth.count)
        eq_(1, oauth1.count)
        eq_(0, oauth2.count)
        eq_(200, response.status_code)
        token = json.loads(response.data).get("access_token")
        provider_name, provider_token = auth.decode_token(token)
        eq_("oauth1", provider_name)
        eq_("token", provider_token)
        
        # Oauth 2
        params = dict(code="foo", state="oauth2")
        response = auth.oauth_callback(self._db, params)
        eq_(0, basic_auth.count)
        eq_(1, oauth1.count)
        eq_(1, oauth2.count)
        eq_(200, response.status_code)
        token = json.loads(response.data).get("access_token")
        provider_name, provider_token = auth.decode_token(token)
        eq_("oauth2", provider_name)
        eq_("token", provider_token)

    def test_patron_info(self):

        # Check that the correct auth provider is called.
        basic_auth = DummyAuthAPI()
        oauth1 = DummyAuthAPI()
        oauth1.NAME = "oauth1"
        oauth2 = DummyAuthAPI()
        oauth2.NAME = "oauth2"

        auth = Authenticator.initialize(self._db, test=True)
        auth.basic_auth_provider = basic_auth
        auth.oauth_providers = [oauth1, oauth2]
        
        # Basic auth
        header = dict(username="foo", password="bar")
        auth.patron_info(header)
        eq_(1, basic_auth.count)
        eq_(0, oauth1.count)
        eq_(0, oauth2.count)

        # Oauth 1
        token = auth.create_token("oauth1", "token")
        header = "Bearer: %s" % token
        auth.patron_info(header)
        eq_(1, basic_auth.count)
        eq_(1, oauth1.count)
        eq_(0, oauth2.count)

        # Oauth 2
        token = auth.create_token("oauth2", "token")
        header = "Bearer: %s" % token
        auth.patron_info(header)
        eq_(1, basic_auth.count)
        eq_(1, oauth1.count)
        eq_(1, oauth2.count)

