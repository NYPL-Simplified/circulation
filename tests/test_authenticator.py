from nose.tools import (
    eq_,
    set_trace,
)
import json
from api.config import (
    Configuration,
    temp_config,
)
from api.authenticator import (
    Authenticator,
    BasicAuthAuthenticator,
    OAuthAuthenticator,
)
from api.millenium_patron import MilleniumPatronAPI
from api.firstbook import FirstBookAuthenticationAPI
from api.clever import CleverAuthenticationAPI
from core.util.opds_authentication_document import OPDSAuthenticationDocument
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

class DummyBasicAuthAPI(DummyAuthAPI, BasicAuthAuthenticator):
    pass


class DummyOAuthAPI(DummyAuthAPI, OAuthAuthenticator):
    def oauth_callback(self, _db, params):
        self.count = self.count + 1
        return "token", dict(name="Patron")

    def authenticate_url(self):
        return "http://authenticate"


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
        with temp_config() as config:
            config[Configuration.SECRET_KEY] = 'secret'

            auth = Authenticator.initialize(self._db, test=True)
            token = auth.create_token("Provider name", "Provider token")
            
            decoded_name, decoded_token = auth.decode_token(token)
            
            eq_("Provider name", decoded_name)
            eq_("Provider token", decoded_token)

    def test_authenticated_patron(self):
        with temp_config() as config:
            config[Configuration.SECRET_KEY] = 'secret'

            # Check that the correct auth provider is called.
            basic_auth = DummyBasicAuthAPI()
            oauth1 = DummyOAuthAPI()
            oauth1.NAME = "oauth1"
            oauth2 = DummyOAuthAPI()
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

    def test_get_credentials_from_header(self):
        with temp_config() as config:
            config[Configuration.SECRET_KEY] = 'secret'

            basic_auth = DummyBasicAuthAPI()
            oauth1 = DummyOAuthAPI()
            oauth1.NAME = "oauth1"
            oauth2 = DummyOAuthAPI()
            oauth2.NAME = "oauth2"

            auth = Authenticator.initialize(self._db, test=True)
            auth.basic_auth_provider = basic_auth
            auth.oauth_providers = [oauth1, oauth2]

            no_auth_header = ""
            eq_(None, auth.get_credential_from_header(no_auth_header))

            basic_auth_header = dict(username="foo", password="bar")
            eq_("bar", auth.get_credential_from_header(basic_auth_header))

            token = auth.create_token("oauth2", "token")
            oauth_header = "Bearer: %s" % token
            eq_("token", auth.get_credential_from_header(oauth_header))

    def test_oauth_callback(self):
        with temp_config() as config:
            config[Configuration.SECRET_KEY] = 'secret'

            # Check that the correct auth provider is called.
            basic_auth = DummyBasicAuthAPI()
            oauth1 = DummyOAuthAPI()
            oauth1.NAME = "oauth1"
            oauth2 = DummyOAuthAPI()
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
        with temp_config() as config:
            config[Configuration.SECRET_KEY] = 'secret'

            # Check that the correct auth provider is called.
            basic_auth = DummyBasicAuthAPI()
            oauth1 = DummyOAuthAPI()
            oauth1.NAME = "oauth1"
            oauth2 = DummyOAuthAPI()
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

    def test_create_authentication_document(self):
        with temp_config() as config:
            config[Configuration.SECRET_KEY] = 'secret'
            config[Configuration.INTEGRATIONS] = {
                Configuration.CIRCULATION_MANAGER_INTEGRATION: {
                    "url": "http://circulation"
                }
            }
            config[Configuration.LINKS] = {
                Configuration.TERMS_OF_SERVICE: "http://terms",
                Configuration.PRIVACY_POLICY: "http://privacy",
                Configuration.COPYRIGHT: "http://copyright",
                Configuration.ABOUT: "http://about",
            }

            basic_auth = DummyBasicAuthAPI()
            oauth1 = DummyOAuthAPI()
            oauth1.URI = "oauth 1 uri"
            oauth1.NAME = "oauth1"
            oauth1.METHOD = "oauth1 method"
            oauth2 = DummyOAuthAPI()
            oauth2.URI = "oauth 2 uri"
            oauth2.NAME = "oauth2"
            oauth2.METHOD = "oauth2 method"

            auth = Authenticator.initialize(self._db, test=True)
            auth.basic_auth_provider = basic_auth
            auth.oauth_providers = [oauth1, oauth2]

            auth_document = json.loads(auth.create_authentication_document())
            assert 'id' in auth_document
            eq_("Library", auth_document['name'])

            links = auth_document['links']
            eq_("http://terms", links['terms-of-service']['href'])
            eq_("http://privacy", links['privacy-policy']['href'])
            eq_("http://copyright", links['copyright']['href'])
            eq_("http://about", links['about']['href'])

            providers = auth_document['providers']
            eq_(3, len(providers.keys()))

            basic_auth_doc = providers[basic_auth.URI]
            eq_(basic_auth.NAME, basic_auth_doc['name'])
            methods = basic_auth_doc['methods']
            eq_(1, len(methods.keys()))
            basic_auth_method = methods[basic_auth.METHOD]
            eq_("Barcode", basic_auth_method['labels']['login'])
            eq_("PIN", basic_auth_method['labels']['password'])
            
            oauth1_doc = providers[oauth1.URI]
            eq_(oauth1.NAME, oauth1_doc['name'])
            methods = oauth1_doc['methods']
            eq_(1, len(methods.keys()))
            oauth1_method = methods[oauth1.METHOD]
            eq_("http://authenticate", oauth1_method['links']['authenticate'])
