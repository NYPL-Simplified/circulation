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
    DataSource,
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
        return self.patron
           

class MockBasicAuthenticationProvider(
        BasicAuthenticationProvider,
        MockAuthenticationProvider
):   
    """A mock basic authentication provider for use in testing the overall
    authentication process.
    """
    def __init__(self, patron=None, patrondata=None, *args, **kwargs):
        super(MockBasicAuthenticationProvider, self).__init__(*args, **kwargs)
        self.patron = patron
        self.patrondata = patrondata

    def authenticate(self, _db, header):
        return self.patron

    def remote_authenticate(self, username, password):
        return self.patrondata

    def remote_patron_lookup(self, patrondata):
        return self.patrondata

class MockBasic(BasicAuthenticationProvider):
    """A second mock basic authentication provider for use in testing
    the workflow around Basic Auth.
    """
    def __init__(self, patrondata=None, remote_patron_lookup_patrondata=None,
                 *args, **kwargs):
        super(MockBasic, self).__init__(*args, **kwargs)
        self.patrondata = patrondata
        self.remote_patron_lookup_patrondata = remote_patron_lookup_patrondata
        
    def remote_authenticate(self, username, password):
        return self.patrondata

    def remote_patron_lookup(self, patrondata):
        return self.remote_patron_lookup_patrondata

    
class MockOAuthAuthenticationProvider(
        OAuthAuthenticationProvider,
        MockAuthenticationProvider
):
    """A mock OAuth authentication provider for use in testing the overall
    authentication process.
    """
    def __init__(self, provider_name, patron=None, patrondata=None):
        self.NAME = provider_name
        self.patron = patron
        self.patrondata = patrondata

    def authenticated_patron(self, _db, provider_token):
        return self.patron


class MockOAuth(OAuthAuthenticationProvider):
    """A second mock basic authentication provider for use in testing
    the workflow around OAuth.
    """
    URI = "http://example.org/"
    NAME = "Mock provider"
    TOKEN_TYPE = "test token"
    TOKEN_DATA_SOURCE_NAME = DataSource.MANUAL

    def __init__(self):
        super(MockOAuth, self).__init__("", "", 20)

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

    def test_apply_sets_last_external_sync_if_data_is_complete(self):
        """Patron.last_external_sync is only updated when apply() is called on
        a PatronData object that represents a full set of metadata.
        What constitutes a 'full set' depends on the authentication
        provider.
        """
        patron = self._patron()
        self.data.complete = False
        self.data.apply(patron)
        eq_(None, patron.last_external_sync)
        self.data.complete = True
        self.data.apply(patron)
        assert None != patron.last_external_sync
        
        
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
            # provider, that provider's .authentication_header is used
            # for WWW-Authenticate.
            headers = authenticator.create_authentication_headers()
            eq_(OPDSAuthenticationDocument.MEDIA_TYPE, headers['Content-Type'])
            eq_(basic.authentication_header, headers['WWW-Authenticate'])

            # If the authenticator does not include a basic auth provider,
            # no WWW-Authenticate header is provided. 
            authenticator = Authenticator(
                oauth_providers=[oauth],
                bearer_token_signing_secret='secret'
            )
            headers = authenticator.create_authentication_headers()
            assert 'WWW-Authenticate' not in headers

class TestAuthenticationProvider(DatabaseTest):

    def test_authenticated_patron_passes_on_none(self):
        provider = MockBasic(patrondata=None)
        patron = provider.authenticated_patron(
            self._db, dict(username='', password='')
        )
        eq_(None, patron)
    
    def test_authenticated_patron_passes_on_problem_detail(self):
        provider = MockBasic(patrondata=UNSUPPORTED_AUTHENTICATION_MECHANISM)
        patron = provider.authenticated_patron(
            self._db, dict(username='', password='')
        )
        eq_(UNSUPPORTED_AUTHENTICATION_MECHANISM, patron)

    def test_authenticated_patron_updates_metadata_if_necessary(self):
        patron = self._patron()
        eq_(True, patron.needs_metadata_update)

        # If we authenticate this patron we find out their username
        # but not any other information about them.
        username = "user"
        barcode = "1234"
        incomplete_data = PatronData(
            permanent_id=patron.external_identifier,
            username=username, complete=False
        )

        # If we do a lookup for this patron we will get more complete
        # information.
        complete_data = PatronData(
            permanent_id=patron.external_identifier,
            authorization_identifier=barcode,
            username=username, complete=True
        )
        
        provider = MockBasic(
            patrondata=incomplete_data,
            remote_patron_lookup_patrondata=complete_data
        )
        patron2 = provider.authenticated_patron(
            self._db, dict(username='', password='')
        )

        # We found the right patron.
        eq_(patron, patron2)

        # We updated their metadata.
        eq_("user", patron.username)

        # We did a patron lookup, which means we updated
        # .last_external_sync.
        assert patron.last_external_sync != None

        # Looking up the patron a second time does not cause another
        # metadata refresh, because we just did a refresh and the
        # patron has borrowing privileges.
        last_sync = patron.last_external_sync
        eq_(False, patron.needs_metadata_update)
        patron = provider.authenticated_patron(
            self._db, dict(username='', password='')
        )
        eq_(last_sync, patron.last_external_sync)
        
    def test_update_patron_metadata(self):
        patron = self._patron()
        eq_(None, patron.last_external_sync)
        eq_(None, patron.username)
        
        patrondata = PatronData(username="user")
        provider = MockBasicAuthenticationProvider(patrondata=patrondata)
        provider.update_patron_metadata(patron)

        # The patron's username has been changed.
        eq_("user", patron.username)
        
        # last_external_sync has been updated.
        assert patron.last_external_sync != None
    
    def test_update_patron_metadata_noop_if_no_remote_metadata(self):

        patron = self._patron()
        provider = MockBasicAuthenticationProvider(patrondata=None)
        provider.update_patron_metadata(patron)

        # We can tell that update_patron_metadata was a no-op because
        # patron.last_external_sync didn't change.
        eq_(None, patron.last_external_sync)

    def test_remote_patron_lookup_is_noop(self):
        """The default implementation of remote_patron_lookup is a no-op."""
        provider = BasicAuthenticationProvider()
        eq_(None, provider.remote_patron_lookup(None))
        patron = self._patron()
        eq_(patron, provider.remote_patron_lookup(patron))
        patrondata = PatronData()
        eq_(patrondata, provider.remote_patron_lookup(patrondata))


class TestBasicAuthenticationProvider(DatabaseTest):

    def test_from_config(self):

        class ConfigAuthenticationProvider(BasicAuthenticationProvider):
            CONFIGURATION_NAME = "Config loading test"
        
        with temp_config() as config:
            data = {
                Configuration.IDENTIFIER_REGULAR_EXPRESSION : "idre",
                Configuration.PASSWORD_REGULAR_EXPRESSION : "pwre",
                Configuration.AUTHENTICATION_TEST_USERNAME : "username",
                Configuration.AUTHENTICATION_TEST_PASSWORD : "pw",
            }
            config[Configuration.INTEGRATIONS] = {
                ConfigAuthenticationProvider.CONFIGURATION_NAME : data
            }
            provider = ConfigAuthenticationProvider.from_config()
            eq_("idre", provider.identifier_re.pattern)
            eq_("pwre", provider.password_re.pattern)
            eq_("username", provider.test_username)
            eq_("pw", provider.test_password)

    def test_testing_patron(self):
        # You don't have to have a testing patron.
        no_testing_patron = BasicAuthenticationProvider()
        eq_((None, None), no_testing_patron.testing_patron(self._db))

        # We configure a testing patron but their username and
        # password don't actually authenticate anyone. We don't crash,
        # but we can't look up the testing patron either.
        missing_patron = MockBasicAuthenticationProvider(
            patron=None, test_username="1", test_password="2"
        )
        value = missing_patron.testing_patron(self._db)
        eq_((None, "2"), value)

        # Here, we configure a testing patron who is authenticated by
        # their username and password.
        patron = self._patron()
        present_patron = MockBasicAuthenticationProvider(
            patron=patron, test_username="1", test_password="2"
        )
        value = present_patron.testing_patron(self._db)
        eq_((patron, "2"), value)

    def test_server_side_validation(self):
        provider = BasicAuthenticationProvider(
            identifier_re="foo", password_re="bar"
        )
        eq_(True, provider.server_side_validation("food", "barbecue"))
        eq_(False, provider.server_side_validation("food", "arbecue"))
        eq_(False, provider.server_side_validation("ood", "barbecue"))
        eq_(False, provider.server_side_validation(None, None))

        # It's okay not to provide anything for server side validation.
        # Everything will be considered valid.
        provider = BasicAuthenticationProvider(
            identifier_re=None, password_re=None
        )
        eq_(True, provider.server_side_validation("food", "barbecue"))
        eq_(True, provider.server_side_validation(None, None))
        
    def test_local_patron_lookup(self):
        patron1 = self._patron("patron1_ext_id")
        patron1.authorization_identifier = "patron1_auth_id"
        patron1.username = "patron1"

        patron2 = self._patron("patron2_ext_id")
        patron2.authorization_identifier = "patron2_auth_id"
        patron2.username = "patron2"
        self._db.commit()
        
        provider = BasicAuthenticationProvider()

        # If we provide PatronData associated with patron1, we look up
        # patron1, even though we provided the username associated
        # with patron2.
        for patrondata_args in [
                dict(permanent_id=patron1.external_identifier),
                dict(authorization_identifier=patron1.authorization_identifier),
                dict(username=patron1.username),
        ]:
            patrondata = PatronData(**patrondata_args)
            eq_(
                patron1, provider.local_patron_lookup(
                    self._db, patron2.authorization_identifier, patrondata
                )
            )

        # If no PatronData is provided, we can look up patron1 either
        # by authorization identifier or username, but not by
        # permanent identifier.
        eq_(
            patron2, provider.local_patron_lookup(
                self._db, patron2.authorization_identifier, None
            )
        )
        eq_(
            patron2, provider.local_patron_lookup(
                self._db, patron2.username, None
            )
        )
        eq_(
            None, provider.local_patron_lookup(
                self._db, patron2.external_identifier, None
            )
        )        
        
    def test_authentication_provider_document(self):
        provider = BasicAuthenticationProvider()
        doc = provider.authentication_provider_document
        eq_(_(provider.DISPLAY_NAME), doc['name'])
        methods = doc['methods']
        eq_([provider.METHOD], methods.keys())
        method = methods[provider.METHOD]
        eq_(['labels'], method.keys())
        login = method['labels']['login']
        password = method['labels']['password']
        eq_(provider.LOGIN_LABEL, login)
        eq_(provider.PASSWORD_LABEL, password)


class TestBasicAuthenticationProviderAuthenticate(DatabaseTest):
    """Test the complex BasicAuthenticationProvider.authenticate method."""

    # A dummy set of credentials, for use when the exact details of
    # the credentials passed in are not important.
    credentials = dict(username="", password="")
    
    def test_success(self):
        patron = self._patron()
        patrondata = PatronData(permanent_id=patron.external_identifier)
        provider = MockBasic(patrondata)

        # authenticate() calls remote_authenticate(), which returns the
        # queued up PatronData object. The corresponding Patron is then
        # looked up in the database.
        eq_(patron, provider.authenticate(
            self._db, dict(username="", password=""))
        )

        # All the different ways the database lookup might go are covered in 
        # test_local_patron_lookup. This test only covers the case where
        # the server sends back the permanent ID of the patron.

    def test_failure_when_remote_authentication_returns_problemdetail(self):
        patron = self._patron()
        patrondata = PatronData(permanent_id=patron.external_identifier)
        provider = MockBasic(UNSUPPORTED_AUTHENTICATION_MECHANISM)
        eq_(UNSUPPORTED_AUTHENTICATION_MECHANISM,
            provider.authenticate(self._db, self.credentials))

    def test_failure_when_remote_authentication_returns_none(self):
        patron = self._patron()
        patrondata = PatronData(permanent_id=patron.external_identifier)
        provider = MockBasic(None)
        eq_(None,
            provider.authenticate(self._db, self.credentials))
        
    def test_server_side_validation_runs(self):
        patron = self._patron()
        patrondata = PatronData(permanent_id=patron.external_identifier)
        provider = MockBasic(
            patrondata, identifier_re="foo", password_re="bar"
        )

        # This would succeed, but we don't get to remote_authenticate()
        # because we fail the regex test.
        eq_(None, provider.authenticate(self._db, self.credentials))

        # This succeeds because we pass the regex test.
        eq_(patron, provider.authenticate(
            self._db, dict(username="food", password="barbecue"))
        )

    def test_authentication_succeeds_but_patronlookup_fails(self):
        """This case should never happen--it indicates a malfunctioning 
        authentication provider. But we handle it.
        """
        patrondata = PatronData(permanent_id=self._str)
        provider = MockBasic(patrondata)

        # When we call remote_authenticate(), we get patrondata, but
        # there is no corresponding local patron, so we call
        # remote_patron_lookup() for details, and we get nothing.  At
        # this point we give up -- there is no authenticated patron.
        eq_(None, provider.authenticate(self._db, self.credentials))


    def test_authentication_creates_missing_patron(self):
        # The authentication provider knows about this patron,
        # but this is the first we've heard about them.
        patrondata = PatronData(
            permanent_id=self._str,
            authorization_identifier=self._str,
            fines="$1.00",
        )
        provider = MockBasic(patrondata, patrondata)
        patron = provider.authenticate(self._db, self.credentials)

        # A server side Patron was created from the PatronData.
        assert isinstance(patron, Patron)
        eq_(patrondata.permanent_id, patron.external_identifier)
        eq_(patrondata.authorization_identifier,
            patron.authorization_identifier)

        # Information not relevant to the patron's identity was stored
        # in the Patron object after it was created.
        eq_("$1.00", patron.fines)
    
    def test_authentication_updates_outdated_patron_on_permanent_id_match(self):
        # A patron's permanent ID won't change.
        permanent_id = self._str

        # But this patron has not used the circulation manager in a
        # long time, and their other identifiers are out of date.
        old_identifier = "1234"
        old_username = "user1"
        patron = self._patron(old_identifier)
        patron.external_identifier = permanent_id
        patron.username = old_username
        
        # The authorization provider has all the new information about
        # this patron.
        new_identifier = "5678"
        new_username = "user2"
        patrondata = PatronData(
            permanent_id=permanent_id,
            authorization_identifier=new_identifier,
            username=new_username,
        )

        provider = MockBasic(patrondata)
        patron2 = provider.authenticate(self._db, self.credentials)

        # We were able to match our local patron to the patron held by the
        # authorization provider.
        eq_(patron2, patron)

        # And we updated our local copy of the patron to reflect their
        # new identifiers.
        eq_(new_identifier, patron.authorization_identifier)
        eq_(new_username, patron.username)

    def test_authentication_updates_outdated_patron_on_username_match(self):
        # This patron has no permanent ID. Their library card number has
        # changed but their username has not.
        old_identifier = "1234"
        new_identifier = "5678"
        username = "user1"
        patron = self._patron(old_identifier)
        patron.external_identifier = None
        patron.username = username
        
        # The authorization provider has all the new information about
        # this patron.
        patrondata = PatronData(
            authorization_identifier=new_identifier,
            username=username,
        )

        provider = MockBasic(patrondata)
        patron2 = provider.authenticate(self._db, self.credentials)

        # We were able to match our local patron to the patron held by the
        # authorization provider, based on the username match.
        eq_(patron2, patron)

        # And we updated our local copy of the patron to reflect their
        # new identifiers.
        eq_(new_identifier, patron.authorization_identifier)

    def test_authentication_updates_outdated_patron_on_username_match(self):
        # This patron has no permanent ID. Their username has
        # changed but their library card number has not.
        identifier = "1234"
        old_username = "user1"
        new_username = "user2"
        patron = self._patron()
        patron.external_identifier = None
        patron.authorization_identifier = identifier
        patron.username = old_username
        
        # The authorization provider has all the new information about
        # this patron.
        patrondata = PatronData(
            authorization_identifier=identifier,
            username=new_username,
        )

        provider = MockBasic(patrondata)
        patron2 = provider.authenticate(self._db, self.credentials)

        # We were able to match our local patron to the patron held by the
        # authorization provider, based on the username match.
        eq_(patron2, patron)

        # And we updated our local copy of the patron to reflect their
        # new identifiers.
        eq_(new_username, patron.username)

    # Notice what's missing: If a patron has no permanent identifier,
    # _and_ their username and authorization identifier both change,
    # then we have no way of locating them in our database. They will
    # appear no different to us than a patron who has never used the
    # circulation manager before.

class TestOAuthAuthenticationProvider(DatabaseTest):

    def test_from_config(self):
        class ConfigAuthenticationProvider(OAuthAuthenticationProvider):
            CONFIGURATION_NAME = "Config loading test"
        
        with temp_config() as config:
            data = {
                Configuration.OAUTH_CLIENT_ID : "client_id",
                Configuration.OAUTH_CLIENT_SECRET : "client_secret",
                Configuration.OAUTH_TOKEN_EXPIRATION_DAYS : 20,
            }
            config[Configuration.INTEGRATIONS] = {
                ConfigAuthenticationProvider.CONFIGURATION_NAME : data
            }
            provider = ConfigAuthenticationProvider.from_config()
            eq_("client_id", provider.client_id)
            eq_("client_secret", provider.client_secret)
            eq_(20, provider.token_expiration_days)

    def test_create_token(self):
        patron = self._patron()
        provider = MockOAuth()
        in_twenty_days = (
            datetime.datetime.utcnow() + datetime.timedelta(
                days=provider.token_expiration_days
            )
        )
        data_source = provider.token_data_source(self._db)
        token, is_new = provider.create_token(self._db, patron, "some token")
        eq_(True, is_new)
        eq_(patron, token.patron)
        eq_("some token", token.credential)

        # The token expires in twenty days.
        almost_no_time = abs(token.expires - in_twenty_days)
        assert almost_no_time.seconds < 2
            
    def test_authenticated_patron_success(self):
        patron = self._patron()
        provider = MockOAuth()
        data_source = provider.token_data_source(self._db)

        # Until we call create_token, this won't work.
        eq_(None, provider.authenticated_patron(self._db, "some other token"))

        token, is_new = provider.create_token(self._db, patron, "some token")
        eq_(True, is_new)
        eq_(patron, token.patron)

        # Now it works.
        eq_(patron, provider.authenticated_patron(self._db, "some token"))
        
    def test_authentication_provider_document(self):
        # We're about to call url_for, so we must create an
        # application context.
        os.environ['AUTOINITIALIZE'] = "False"
        from api.app import app
        self.app = app
        del os.environ['AUTOINITIALIZE']
        provider = MockOAuth()
        with self.app.test_request_context("/"):
            doc = provider.authentication_provider_document

            # There is only one way to authenticate with this type
            # of authentication.
            eq_([provider.METHOD], doc['methods'].keys())
            method = doc['methods'][provider.METHOD]

            # And it involves following the 'authenticate' link.
            link = method['links']['authenticate']
            eq_(link, provider._internal_authenticate_url())
        
class TestOAuthController:
    pass
