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
from money import Money
import re
import urllib
import urlparse
import flask
from flask import url_for

from core.opds import OPDSFeed
from core.model import (
    CirculationEvent,
    ConfigurationSetting,
    Credential,
    DataSource,
    ExternalIntegration,
    Library,
    Patron,
    create,
    Session,
)

from core.util.problem_detail import (
    ProblemDetail,
)
from core.util.authentication_for_opds import (
    AuthenticationForOPDSDocument,
)
from core.mock_analytics_provider import MockAnalyticsProvider

from api.millenium_patron import MilleniumPatronAPI
from api.firstbook import FirstBookAuthenticationAPI
from api.clever import CleverAuthenticationAPI
from api.util.patron import PatronUtility

from api.authenticator import (
    Authenticator,
    LibraryAuthenticator,
    AuthenticationProvider,
    BasicAuthenticationProvider,
    OAuthController,
    OAuthAuthenticationProvider,
    PatronData,
)
from api.simple_authentication import SimpleAuthenticationProvider
from api.millenium_patron import MilleniumPatronAPI
from api.opds import CirculationManagerAnnotator

from api.config import (
    CannotLoadConfiguration,
    Configuration,
    temp_config,
)

from api.problem_details import *

from . import DatabaseTest
from test_controller import ControllerTest

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
    def __init__(self, library, integration, analytics=None, patron=None, patrondata=None, *args, **kwargs):
        super(MockBasicAuthenticationProvider, self).__init__(
            library, integration, analytics, *args, **kwargs)
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
    NAME = 'Mock Basic Auth provider'
    def __init__(self, library, integration, analytics=None, patrondata=None,
                 remote_patron_lookup_patrondata=None,
                 *args, **kwargs):
        super(MockBasic, self).__init__(library, integration, analytics)
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
    def __init__(self, library, provider_name, patron=None, patrondata=None):
        self.library_id = library.id
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

    def __init__(self, library, name="Mock OAuth", integration=None, analytics=None):
        _db = Session.object_session(library)
        integration = integration or self._mock_integration(_db, name)
        super(MockOAuth, self).__init__(library, integration, analytics)

    @classmethod
    def _mock_integration(self, _db, name):
        integration, ignore = create(
            _db, ExternalIntegration, protocol="OAuth",
            goal=ExternalIntegration.PATRON_AUTH_GOAL,
        )
        integration.username = name
        integration.password = ""
        integration.setting(self.OAUTH_TOKEN_EXPIRATION_DAYS).value = 20
        return integration


class AuthenticatorTest(DatabaseTest):

    def mock_basic(self, *args, **kwargs):
        """Convenience method to instantiate a MockBasic object with the 
        default library.
        """
        integration = self._external_integration(
            self._str, ExternalIntegration.PATRON_AUTH_GOAL
        )
        return MockBasic(
            self._default_library, integration, *args, **kwargs
        )

        
class TestPatronData(AuthenticatorTest):

    def setup(self):
        super(TestPatronData, self).setup()
        self.data = PatronData(
            permanent_id="1",
            authorization_identifier="2",
            username="3",
            personal_name="4",
            email_address="5",
            authorization_expires=datetime.datetime.utcnow(),
            fines=Money(6, "USD"),
            block_reason=PatronData.NO_VALUE,
        )
        
    
    def test_apply(self):
        patron = self._patron()

        self.data.apply(patron)
        eq_(self.data.permanent_id, patron.external_identifier)
        eq_(self.data.authorization_identifier, patron.authorization_identifier)
        eq_(self.data.username, patron.username)
        eq_(self.data.authorization_expires, patron.authorization_expires)
        eq_(self.data.fines, patron.fines)
        eq_(None, patron.block_reason)

        # This data is stored in PatronData but not applied to Patron.
        eq_("4", self.data.personal_name)
        eq_(False, hasattr(patron, 'personal_name'))
        eq_("5", self.data.email_address)
        eq_(False, hasattr(patron, 'email_address'))


    def test_apply_block_reason(self):
        """If the PatronData has a reason why a patron is blocked,
        the reason is put into the Patron record.
        """
        self.data.block_reason = PatronData.UNKNOWN_BLOCK
        patron = self._patron()
        self.data.apply(patron)
        eq_(PatronData.UNKNOWN_BLOCK, patron.block_reason)
        
    def test_apply_multiple_authorization_identifiers(self):
        """If there are multiple authorization identifiers, the first
        one is chosen.
        """
        patron = self._patron()
        patron.authorization_identifier = None
        data = PatronData(
            authorization_identifier=["2", "3"],
            complete=True
        )
        data.apply(patron)
        eq_("2", patron.authorization_identifier)

        # If Patron.authorization_identifier is already set, it will
        # not be changed, so long as its current value is acceptable.
        data = PatronData(
            authorization_identifier=["3", "2"],
            complete=True
        )
        data.apply(patron)
        eq_("2", patron.authorization_identifier)

        # If Patron.authorization_identifier ever turns out not to be
        # an acceptable value, it will be changed.
        data = PatronData(
            authorization_identifier=["3", "4"],
            complete=True
        )
        data.apply(patron)
        eq_("3", patron.authorization_identifier)
        
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
        
    def test_apply_sets_first_valid_authorization_identifier(self):
        """If the ILS has multiple authorization identifiers for a patron, the
        first one is used.
        """
        patron = self._patron()
        patron.authorization_identifier = None
        self.data.set_authorization_identifier(["identifier 1", "identifier 2"])
        self.data.apply(patron)
        eq_("identifier 1", patron.authorization_identifier)
                
    def test_apply_leaves_valid_authorization_identifier_alone(self):
        """If the ILS says a patron has a new preferred authorization
        identifier, but our Patron record shows them using an
        authorization identifier that still works, we don't change it.
        """
        patron = self._patron()
        patron.authorization_identifier = "old identifier"
        self.data.set_authorization_identifier([
            "new identifier", patron.authorization_identifier
        ])
        self.data.apply(patron)
        eq_("old identifier", patron.authorization_identifier)

    def test_apply_overwrites_invalid_authorization_identifier(self):
        """If the ILS says a patron has a new preferred authorization
        identifier, and our Patron record shows them using an
        authorization identifier that no longer works, we change it.
        """
        patron = self._patron()
        self.data.set_authorization_identifier([
            "identifier 1", "identifier 2"
        ])
        self.data.apply(patron)
        eq_("identifier 1", patron.authorization_identifier)

    def test_apply_on_incomplete_information(self):
        """When we call apply() based on incomplete information (most
        commonly, the fact that a given string was successfully used
        to authenticate a patron), we are very careful about modifying
        data already in the database.
        """
        now = datetime.datetime.now()
        
        # If the only thing we know about a patron is that a certain
        # string authenticated them, we set
        # Patron.authorization_identifier to that string but we also
        # indicate that we need to perform an external sync on them
        # ASAP.
        authenticated = PatronData(
            authorization_identifier="1234", complete=False
        )
        patron = self._patron()
        patron.authorization_identifier = None
        patron.last_external_sync = now
        authenticated.apply(patron)
        eq_("1234", patron.authorization_identifier)
        eq_(None, patron.last_external_sync)

        # If a patron authenticates by username, we leave their Patron
        # record alone.
        patron = self._patron()
        patron.authorization_identifier = "1234"
        patron.username = "user"
        patron.last_external_sync = now
        patron.fines = Money(10, "USD")
        authenticated_by_username = PatronData(
            authorization_identifier="user", complete=False
        )
        authenticated_by_username.apply(patron)
        eq_(now, patron.last_external_sync)

        # If a patron authenticates with a string that is neither
        # their authorization identifier nor their username, we leave
        # their Patron record alone, except that we indicate that we
        # need to perform an external sync on them ASAP.
        patron.last_external_sync = now
        authenticated_by_weird_identifier = PatronData(
            authorization_identifier="5678", complete=False
        )
        authenticated_by_weird_identifier.apply(patron)
        eq_("1234", patron.authorization_identifier)
        eq_(None, patron.last_external_sync)

    def test_get_or_create_patron(self):
        analytics = MockAnalyticsProvider()

        # The patron didn't exist yet, so it was created
        # and an analytics event was sent.
        patron, is_new = self.data.get_or_create_patron(
            self._db, self._default_library.id, analytics
        )
        eq_('2', patron.authorization_identifier)
        eq_(self._default_library, patron.library)
        eq_(True, is_new)
        eq_(CirculationEvent.NEW_PATRON, analytics.event_type)
        eq_(1, analytics.count)

        # The same patron is returned, and no analytics
        # event was sent.
        patron, is_new = self.data.get_or_create_patron(
            self._db, self._default_library.id, analytics
        )
        eq_('2', patron.authorization_identifier)
        eq_(False, is_new)
        eq_(1, analytics.count)

    def test_to_response_parameters(self):

        params = self.data.to_response_parameters
        eq_(dict(name="4"), params)


class MockAuthenticator(Authenticator):
    """Allows testing Authenticator methods outside of a request context."""

    def __init__(self, current_library, authenticators, analytics=None):
        _db = Session.object_session(current_library)
        super(MockAuthenticator, self).__init__(_db, analytics)
        self.current_library_name = current_library.short_name
        self.library_authenticators = authenticators

    def populate_authenticators(self, *args, **kwargs):
        """Do nothing -- authenticators were set in the constructor."""

    @property
    def current_library_short_name(self):
        return self.current_library_name


class TestAuthenticator(ControllerTest):

    def test_init(self):
        # The default library has already been configured to use the
        # SimpleAuthenticationProvider for its basic auth.
        l1 = self._default_library
        l1.short_name = 'l1'

        # This library uses Millenium Patron.
        l2, ignore = create(self._db, Library, short_name="l2")
        integration = self._external_integration(
            "api.millenium_patron", goal=ExternalIntegration.PATRON_AUTH_GOAL
        )
        integration.url = "http://url/"
        l2.integrations.append(integration)

        self._db.commit()

        analytics = MockAnalyticsProvider()
        
        auth = Authenticator(self._db, analytics)

        # A LibraryAuthenticator has been created for each Library.
        assert 'l1' in auth.library_authenticators
        assert 'l2' in auth.library_authenticators
        assert isinstance(auth.library_authenticators['l1'], LibraryAuthenticator)
        assert isinstance(auth.library_authenticators['l2'], LibraryAuthenticator)

        # Each LibraryAuthenticator has been associated with an
        # appropriate AuthenticationProvider.

        assert isinstance(
            auth.library_authenticators['l1'].basic_auth_provider,
            SimpleAuthenticationProvider
        )
        assert isinstance(
            auth.library_authenticators['l2'].basic_auth_provider,
            MilleniumPatronAPI
        )

        # Each provider has the analytics set.
        eq_(analytics, auth.library_authenticators['l1'].basic_auth_provider.analytics)
        eq_(analytics, auth.library_authenticators['l2'].basic_auth_provider.analytics)

    def test_methods_call_library_authenticators(self):
        class MockLibraryAuthenticator(LibraryAuthenticator):
            def __init__(self, name):
                self.name = name
            def authenticated_patron(self, _db, header):
                return "authenticated patron for %s" % self.name
            def create_authentication_document(self):
                return "authentication document for %s" % self.name
            def create_authentication_headers(self):
                return "authentication headers for %s" % self.name
            def get_credential_from_header(self, header):
                return "credential for %s" % self.name
            def create_bearer_token(self, *args, **kwargs):
                return "bearer token for %s" % self.name
            def oauth_provider_lookup(self, *args, **kwargs):
                return "oauth provider for %s" % self.name
            def decode_bearer_token(self, *args, **kwargs):
                return "decoded bearer token for %s" % self.name


        l1, ignore = create(self._db, Library, short_name="l1")
        l2, ignore = create(self._db, Library, short_name="l2")

        auth = Authenticator(self._db)
        auth.library_authenticators['l1'] = MockLibraryAuthenticator("l1")
        auth.library_authenticators['l2'] = MockLibraryAuthenticator("l2")

        # This new library isn't in the authenticator.
        l3, ignore = create(self._db, Library, short_name="l3")

        with self.app.test_request_context("/"):
            flask.request.library = l3
            eq_(LIBRARY_NOT_FOUND, auth.authenticated_patron(self._db, {}))
            eq_(LIBRARY_NOT_FOUND, auth.create_authentication_document())
            eq_(LIBRARY_NOT_FOUND, auth.create_authentication_headers())
            eq_(LIBRARY_NOT_FOUND, auth.get_credential_from_header({}))
            eq_(LIBRARY_NOT_FOUND, auth.create_bearer_token())
            eq_(LIBRARY_NOT_FOUND, auth.oauth_provider_lookup())

        # The other libraries are in the authenticator.
        with self.app.test_request_context("/"):
            flask.request.library = l1
            eq_("authenticated patron for l1", auth.authenticated_patron(self._db, {}))
            eq_("authentication document for l1", auth.create_authentication_document())
            eq_("authentication headers for l1", auth.create_authentication_headers())
            eq_("credential for l1", auth.get_credential_from_header({}))
            eq_("bearer token for l1", auth.create_bearer_token())
            eq_("oauth provider for l1", auth.oauth_provider_lookup())
            eq_("decoded bearer token for l1", auth.decode_bearer_token())

        with self.app.test_request_context("/"):
            flask.request.library = l2
            eq_("authenticated patron for l2", auth.authenticated_patron(self._db, {}))
            eq_("authentication document for l2", auth.create_authentication_document())
            eq_("authentication headers for l2", auth.create_authentication_headers())
            eq_("credential for l2", auth.get_credential_from_header({}))
            eq_("bearer token for l2", auth.create_bearer_token())
            eq_("oauth provider for l2", auth.oauth_provider_lookup())
            eq_("decoded bearer token for l2", auth.decode_bearer_token())


class TestLibraryAuthenticator(AuthenticatorTest):

    def test_from_config_basic_auth_only(self):
        # Only a basic auth provider.
        millenium = self._external_integration(
            "api.millenium_patron", ExternalIntegration.PATRON_AUTH_GOAL,
        )
        millenium.url = "http://url/"
        self._default_library.integrations.append(millenium)

        auth = LibraryAuthenticator.from_config(self._db, self._default_library)

        assert auth.basic_auth_provider != None
        assert isinstance(auth.basic_auth_provider, MilleniumPatronAPI)
        eq_({}, auth.oauth_providers_by_name)

    def test_from_config_basic_auth_and_oauth(self):
        library = self._default_library
        # A basic auth provider and an oauth provider.
        firstbook = self._external_integration(
            "api.firstbook", ExternalIntegration.PATRON_AUTH_GOAL,
        )
        firstbook.url = "http://url/"
        firstbook.password = "secret"
        library.integrations.append(firstbook)
        
        oauth = self._external_integration(
            "api.clever", ExternalIntegration.PATRON_AUTH_GOAL,
        )
        oauth.username = "client_id"
        oauth.password = "client_secret"
        library.integrations.append(oauth)

        analytics = MockAnalyticsProvider()
        auth = LibraryAuthenticator.from_config(self._db, library, analytics)

        assert auth.basic_auth_provider != None
        assert isinstance(auth.basic_auth_provider,
                          FirstBookAuthenticationAPI)
        eq_(analytics, auth.basic_auth_provider.analytics)
            
        eq_(1, len(auth.oauth_providers_by_name))
        clever = auth.oauth_providers_by_name[
            CleverAuthenticationAPI.NAME
        ]
        assert isinstance(clever, CleverAuthenticationAPI)
        eq_(analytics, clever.analytics)
            
    def test_config_succeeds_when_no_providers_configured(self):
        """You can call from_config even when there are no authentication
        providers configured.

        This should not happen in normal usage, but there will be an
        interim period immediately after a library is created where
        this will be its configuration.
        """
        authenticator = LibraryAuthenticator.from_config(
            self._db, self._default_library
        )
        eq_([], list(authenticator.providers))

    def test_configuration_exception_during_from_config_stored(self):
        """If the initialization of an AuthenticationProvider from config
        raises CannotLoadConfiguration or ImportError, the exception
        is stored with the LibraryAuthenticator rather than being
        propagated.
        """

        # Create an integration destined to raise CannotLoadConfiguration..
        misconfigured = self._external_integration(
            "api.firstbook", ExternalIntegration.PATRON_AUTH_GOAL,
        )

        # ... and one destined to raise ImportError.
        unknown = self._external_integration(
            "unknown protocol", ExternalIntegration.PATRON_AUTH_GOAL
        )
        for integration in [misconfigured, unknown]:
            self._default_library.integrations.append(integration)
        auth = LibraryAuthenticator.from_config(self._db, self._default_library)

        # The LibraryAuthenticator exists but has no AuthenticationProviders.
        eq_(None, auth.basic_auth_provider)
        eq_({}, auth.oauth_providers_by_name)
        
        # Both integrations have left their trace in
        # initialization_exceptions.
        not_configured = auth.initialization_exceptions[misconfigured.id]
        assert isinstance(not_configured, CannotLoadConfiguration)
        eq_('First Book server not configured.', not_configured.message)

        not_found = auth.initialization_exceptions[unknown.id]
        assert isinstance(not_found, ImportError)
        eq_('No module named unknown protocol', not_found.message)
        
    def test_register_fails_when_integration_has_wrong_goal(self):
        integration = self._external_integration(
            "protocol", "some other goal"
        )
        auth = LibraryAuthenticator(_db=self._db, library=self._default_library)
        assert_raises_regexp(
            CannotLoadConfiguration,
            "Was asked to register an integration with goal=some other goal as though it were a way of authenticating patrons.",
            auth.register_provider, integration
        )

    def test_register_fails_when_integration_not_associated_with_library(self):
        integration = self._external_integration(
            "protocol", ExternalIntegration.PATRON_AUTH_GOAL
        )
        auth = LibraryAuthenticator(_db=self._db, library=self._default_library)
        assert_raises_regexp(
            CannotLoadConfiguration,
            "Was asked to register an integration with library .*, which doesn't use it.",
            auth.register_provider, integration
        )

    def test_register_fails_when_integration_module_does_not_contain_provider_class(self):
        library = self._default_library
        integration = self._external_integration(
            "api.lanes", ExternalIntegration.PATRON_AUTH_GOAL
        )
        library.integrations.append(integration)
        auth = LibraryAuthenticator(_db=self._db, library=library)
        assert_raises_regexp(
            CannotLoadConfiguration,
            "Loaded module api.lanes but could not find a class called AuthenticationProvider inside.",
            auth.register_provider, integration
        )        
        
    def test_register_provider_basic_auth(self):
        firstbook = self._external_integration(
            "api.firstbook", ExternalIntegration.PATRON_AUTH_GOAL,
        )
        firstbook.url = "http://url/"
        firstbook.password = "secret"
        self._default_library.integrations.append(firstbook)
        auth = LibraryAuthenticator(_db=self._db, library=self._default_library)
        auth.register_provider(firstbook)
        assert isinstance(
            auth.basic_auth_provider, FirstBookAuthenticationAPI
        )
        
    def test_register_oauth_provider(self):
        oauth = self._external_integration(
            "api.clever", ExternalIntegration.PATRON_AUTH_GOAL,
        )
        oauth.username = "client_id"
        oauth.password = "client_secret"
        self._default_library.integrations.append(oauth)
        auth = LibraryAuthenticator(_db=self._db, library=self._default_library)
        auth.register_provider(oauth)
        eq_(1, len(auth.oauth_providers_by_name))
        clever = auth.oauth_providers_by_name[
            CleverAuthenticationAPI.NAME
        ]
        assert isinstance(clever, CleverAuthenticationAPI)
            
    def test_oauth_provider_requires_secret(self):
        integration = self._external_integration(self._str)

        basic = MockBasicAuthenticationProvider(
            self._default_library, integration
        )
        oauth = MockOAuthAuthenticationProvider(
            self._default_library, "provider1"
        )

        # You can create an Authenticator that only uses Basic Auth
        # without providing a secret.
        LibraryAuthenticator(
            _db=self._db,
            library=self._default_library,
            basic_auth_provider=basic
        )

        # You can create an Authenticator that uses OAuth if you
        # provide a secret.
        LibraryAuthenticator(
            _db=self._db,
            library=self._default_library,
            oauth_providers=[oauth], bearer_token_signing_secret="foo"
        )
        
        # But you can't create an Authenticator that uses OAuth
        # without providing a secret.
        assert_raises_regexp(
            LibraryAuthenticator,
            "OAuth providers are configured, but secret for signing bearer tokens is not.",
            library=self._default_library,
            oauth_providers=[oauth]
        )
        
    def test_providers(self):
        integration = self._external_integration(self._str)
        basic = MockBasicAuthenticationProvider(
            self._default_library, integration
        )
        oauth1 = MockOAuthAuthenticationProvider(self._default_library, "provider1")
        oauth2 = MockOAuthAuthenticationProvider(self._default_library, "provider2")

        authenticator = LibraryAuthenticator(
            _db=self._db,
            library=self._default_library,
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
        authenticator = LibraryAuthenticator(
            _db=self._db,
            library=self._default_library,
            bearer_token_signing_secret='foo'
        )
        integration = self._external_integration(self._str)
        basic1 = MockBasicAuthenticationProvider(
            self._default_library, integration
        )
        basic2 = MockBasicAuthenticationProvider(
            self._default_library, integration
        )
        oauth1 = MockOAuthAuthenticationProvider(self._default_library, "provider1")
        oauth2 = MockOAuthAuthenticationProvider(self._default_library, "provider2")
        oauth1_dupe = MockOAuthAuthenticationProvider(self._default_library, "provider1")

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
        integration = self._external_integration(self._str)
        basic = MockBasicAuthenticationProvider(
            self._default_library, integration
        )
        authenticator = LibraryAuthenticator(
            _db=self._db,
            library=self._default_library,
            basic_auth_provider=basic
        )
        problem = authenticator.oauth_provider_lookup("provider1")
        eq_(problem.uri, UNKNOWN_OAUTH_PROVIDER.uri)
        eq_(_("No OAuth providers are configured."), problem.detail)
        
        # We can look up registered providers but not unregistered providers.
        oauth1 = MockOAuthAuthenticationProvider(self._default_library, "provider1")
        oauth2 = MockOAuthAuthenticationProvider(self._default_library, "provider2")
        oauth3 = MockOAuthAuthenticationProvider(self._default_library, "provider3")
        authenticator = LibraryAuthenticator(
            _db=self._db,
            library=self._default_library,
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
        integration = self._external_integration(self._str)
        basic = MockBasicAuthenticationProvider(
            self._default_library, integration, patron=patron,
            patrondata=patrondata
        )
        authenticator = LibraryAuthenticator(
            _db=self._db,
            library=self._default_library,
            basic_auth_provider=basic
        )
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
        oauth1 = MockOAuthAuthenticationProvider(self._default_library, "oauth1", patron=patron1)
        oauth2 = MockOAuthAuthenticationProvider(self._default_library, "oauth2", patron=patron2)
        authenticator = LibraryAuthenticator(
            _db=self._db,
            library=self._default_library,
            oauth_providers=[oauth1, oauth2],
            bearer_token_signing_secret='foo'
        )

        # Ask oauth1 to create a bearer token.
        token = authenticator.create_bearer_token(
            oauth1.NAME, "some token"
        )
        
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
        authenticator = LibraryAuthenticator(
            _db=self._db,
            library=self._default_library,
        )
        problem = authenticator.authenticated_patron(
            self._db, object()
        )
        eq_(UNSUPPORTED_AUTHENTICATION_MECHANISM, problem)

    def test_get_credential_from_header(self):
        integration = self._external_integration(self._str)
        basic = MockBasicAuthenticationProvider(self._default_library, integration)
        oauth = MockOAuthAuthenticationProvider(self._default_library, "oauth1")

        # We can pull the password out of a Basic Auth credential
        # if a Basic Auth authentication provider is configured.
        authenticator = LibraryAuthenticator(
            _db=self._db,
            library=self._default_library,
            basic_auth_provider=basic, oauth_providers=[oauth],
            bearer_token_signing_secret="secret"
        )
        credential = dict(password="foo")
        eq_("foo",
            authenticator.get_credential_from_header(credential)
        )

        # We can't pull the password out if only OAuth authentication
        # providers are configured.
        authenticator = LibraryAuthenticator(
            _db=self._db,
            library=self._default_library,
            basic_auth_provider=None, oauth_providers=[oauth],
            bearer_token_signing_secret="secret"
        )
        eq_(None,
            authenticator.get_credential_from_header(credential)
        )

        
    def test_create_bearer_token(self):
        oauth1 = MockOAuthAuthenticationProvider(self._default_library, "oauth1")
        oauth2 = MockOAuthAuthenticationProvider(self._default_library, "oauth2")
        authenticator = LibraryAuthenticator(
            _db=self._db,
            library=self._default_library,
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
        oauth = MockOAuthAuthenticationProvider(self._default_library, "oauth")
        authenticator = LibraryAuthenticator(
            _db=self._db,
            library=self._default_library,
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
        integration = self._external_integration(self._str)
        library = self._default_library
        basic = MockBasicAuthenticationProvider(library, integration)
        oauth = MockOAuthAuthenticationProvider(library, "oauth")
        oauth.URI = "http://example.org/"
        library.name = "A Fabulous Library"
        authenticator = LibraryAuthenticator(
            _db=self._db,
            library = library,
            basic_auth_provider=basic, oauth_providers=[oauth],
            bearer_token_signing_secret='secret'
        )

        # We're about to call url_for, so we must create an
        # application context.
        os.environ['AUTOINITIALIZE'] = "False"
        from api.app import app
        self.app = app
        del os.environ['AUTOINITIALIZE']
        
        # Set up configuration settings for links.
        link_config = {
            CirculationManagerAnnotator.TERMS_OF_SERVICE: "http://terms",
            CirculationManagerAnnotator.PRIVACY_POLICY: "http://privacy",
            CirculationManagerAnnotator.COPYRIGHT: "http://copyright",
            CirculationManagerAnnotator.ABOUT: "http://about",
            CirculationManagerAnnotator.LICENSE: "http://license/",
            CirculationManagerAnnotator.REGISTER: "custom-registration-hook://library/",
            Configuration.LOGO: "image data",
        }

        for rel, value in link_config.iteritems():
            ConfigurationSetting.for_library(rel, self._default_library).value = value

        ConfigurationSetting.for_library(
            Configuration.LIBRARY_DESCRIPTION, library
        ).value = "Just the best."
            
        # Set the URL to the library's web page.
        ConfigurationSetting.for_library(
            Configuration.WEBSITE_URL, library).value = "http://library/"

        # Set the color scheme a client should use.
        ConfigurationSetting.for_library(
            Configuration.COLOR_SCHEME, library).value = "plaid"
        
        # Configure the various ways a patron can get help.
        ConfigurationSetting.for_library(
            Configuration.HELP_EMAIL, library).value = "help@library"
        ConfigurationSetting.for_library(
            Configuration.HELP_WEB, library).value = "http://library.help/"
        ConfigurationSetting.for_library(
            Configuration.HELP_URI, library).value = "custom:uri"

        # Set up a public key.
        ConfigurationSetting.for_library(
            Configuration.PUBLIC_KEY, library).value = "public key"
        
        base_url = ConfigurationSetting.sitewide(self._db, Configuration.BASE_URL_KEY)
        base_url.value = u'http://circulation-manager/'
       
        with self.app.test_request_context("/"):
            url = authenticator.authentication_document_url(library)
            assert url.endswith(
                "/%s/authentication_document" % library.short_name
            )

            doc = json.loads(authenticator.create_authentication_document())
            # The main thing we need to test is that the
            # sub-documents are assembled properly and placed in the
            # right position.
            flows = doc['authentication']
            oauth_doc, basic_doc = sorted(flows, key=lambda x: x['type'])

            expect_basic = basic.authentication_flow_document(self._db)
            eq_(expect_basic, basic_doc)

            expect_oauth = oauth.authentication_flow_document(self._db)
            eq_(expect_oauth, oauth_doc)

            # We also need to test that the library's name and ID
            # were placed in the document.
            eq_("A Fabulous Library", doc['title'])
            eq_("Just the best.", doc['service_description'])
            eq_(url, doc['id'])

            # The color scheme is correctly reported.
            eq_("plaid", doc['color_scheme'])
            
            # We also need to test that the links got pulled in
            # from the configuration.
            (about, alternate, copyright, help_uri, help_web, help_email,
             license, logo, privacy_policy, register, start, terms_of_service) = sorted(
                 doc['links'], key=lambda x: (x['rel'], x['href'])
             )
            eq_("http://terms", terms_of_service['href'])
            eq_("http://privacy", privacy_policy['href'])
            eq_("http://copyright", copyright['href'])
            eq_("http://about", about['href'])
            eq_("http://license/", license['href'])
            eq_("image data", logo['href'])
            expect_start = url_for(
                "index", library_short_name=self._default_library.short_name, 
                _external=True
            )
            eq_(expect_start, start['href'])

            # The start link points to an OPDS feed.
            eq_(OPDSFeed.ACQUISITION_FEED_TYPE, start['type'])

            # Most of the other links have type='text/html'
            eq_("text/html", about['type'])

            # The registration link doesn't have a type, because it
            # uses a non-HTTP URI scheme.
            assert 'type' not in register
            eq_('custom-registration-hook://library/', register['href'])

            # The logo link has type "image/png".
            eq_("image/png", logo["type"])

            # We have three help links.
            eq_("custom:uri", help_uri['href'])
            eq_("http://library.help/", help_web['href'])
            eq_("text/html", help_web['type'])
            eq_("mailto:help@library", help_email['href'])

            # The public key is correct.
            eq_("public key", doc['public_key']['value'])
            eq_("RSA", doc['public_key']['type'])
            

            # The library's web page shows up as an HTML alternate
            # to the OPDS server.
            eq_(
                dict(rel="alternate", type="text/html", href="http://library/"),
                alternate
            )
            
            # Features that are enabled for this library are communicated
            # through the 'features' item.
            features = doc['features']
            eq_([], features['disabled'])
            eq_([Configuration.RESERVATIONS_FEATURE], features['enabled'])
            
            # While we're in this context, let's also test
            # create_authentication_headers.

            # So long as the authenticator includes a basic auth
            # provider, that provider's .authentication_header is used
            # for WWW-Authenticate.
            headers = authenticator.create_authentication_headers()
            eq_(AuthenticationForOPDSDocument.MEDIA_TYPE, headers['Content-Type'])
            eq_(basic.authentication_header, headers['WWW-Authenticate'])

            # The response contains a Link header pointing to the authentication
            # document
            expect = "<%s>; rel=%s" % (
                authenticator.authentication_document_url(self._default_library),
                AuthenticationForOPDSDocument.LINK_RELATION
            )
            eq_(expect, headers['Link'])


            # If the authenticator does not include a basic auth provider,
            # no WWW-Authenticate header is provided.
            authenticator = LibraryAuthenticator(
                _db=self._db,
                library=library,
                oauth_providers=[oauth],
                bearer_token_signing_secret='secret'
            )
            headers = authenticator.create_authentication_headers()
            assert 'WWW-Authenticate' not in headers

class TestAuthenticationProvider(AuthenticatorTest):

    credentials = dict(username='user', password='')
    
    def test_authenticated_patron_passes_on_none(self):
        provider = self.mock_basic(patrondata=None)
        patron = provider.authenticated_patron(
            self._db, self.credentials
        )
        eq_(None, patron)
    
    def test_authenticated_patron_passes_on_problem_detail(self):
        provider = self.mock_basic(
            patrondata=UNSUPPORTED_AUTHENTICATION_MECHANISM
        )
        patron = provider.authenticated_patron(
            self._db, self.credentials
        )
        eq_(UNSUPPORTED_AUTHENTICATION_MECHANISM, patron)

    def test_authenticated_patron_allows_access_to_expired_credentials(self):
        """Even if your card has expired, you can log in -- you just can't
        borrow books.
        """
        yesterday = datetime.datetime.utcnow() - datetime.timedelta(days=1)

        expired = PatronData(permanent_id="1", authorization_identifier="2",
                             authorization_expires=yesterday)
        provider = self.mock_basic(
                             patrondata=expired,
                             remote_patron_lookup_patrondata=expired
        )
        patron = provider.authenticated_patron(
            self._db, self.credentials
        )
        eq_("1", patron.external_identifier)
        eq_("2", patron.authorization_identifier)
        
    def test_authenticated_patron_updates_metadata_if_necessary(self):
        patron = self._patron()
        eq_(True, PatronUtility.needs_external_sync(patron))

        # If we authenticate this patron by username we find out their
        # permanent ID but not any other information about them.
        username = "user"
        barcode = "1234"
        incomplete_data = PatronData(
            permanent_id=patron.external_identifier,
            authorization_identifier=username,
            complete=False
        )

        # If we do a lookup for this patron we will get more complete
        # information.
        complete_data = PatronData(
            permanent_id=patron.external_identifier,
            authorization_identifier=barcode,
            username=username, complete=True
        )
        
        provider = self.mock_basic(
            patrondata=incomplete_data,
            remote_patron_lookup_patrondata=complete_data
        )
        patron2 = provider.authenticated_patron(
            self._db, self.credentials
        )

        # We found the right patron.
        eq_(patron, patron2)

        # We updated their metadata.
        eq_("user", patron.username)
        eq_(barcode, patron.authorization_identifier)
        
        # We did a patron lookup, which means we updated
        # .last_external_sync.
        assert patron.last_external_sync != None
        eq_(barcode, patron.authorization_identifier)
        eq_(username, patron.username)
        
        # Looking up the patron a second time does not cause another
        # metadata refresh, because we just did a refresh and the
        # patron has borrowing privileges.
        last_sync = patron.last_external_sync
        eq_(False, PatronUtility.needs_external_sync(patron))
        patron = provider.authenticated_patron(
            self._db, dict(username=username)
        )
        eq_(last_sync, patron.last_external_sync)
        eq_(barcode, patron.authorization_identifier)
        eq_(username, patron.username)
        
        # If we somehow authenticate with an identifier other than
        # the ones in the Patron record, we trigger another metadata
        # refresh to see if anything has changed.
        incomplete_data = PatronData(
            permanent_id=patron.external_identifier,
            authorization_identifier="some other identifier",
            complete=False
        )
        provider.patrondata = incomplete_data
        patron = provider.authenticated_patron(
            self._db, dict(username="someotheridentifier")
        )
        assert patron.last_external_sync > last_sync

        # But Patron.authorization_identifier doesn't actually change
        # to "some other identifier", because when we do the metadata
        # refresh we get the same data as before.
        eq_(barcode, patron.authorization_identifier)
        eq_(username, patron.username)
        
    def test_update_patron_metadata(self):
        patron = self._patron()
        patron.authorization_identifier="2345"
        eq_(None, patron.last_external_sync)
        eq_(None, patron.username)
        
        patrondata = PatronData(username="user")
        provider = self.mock_basic(remote_patron_lookup_patrondata=patrondata)
        provider.external_type_regular_expression = re.compile("^(.)")
        provider.update_patron_metadata(patron)

        # The patron's username has been changed.
        eq_("user", patron.username)
        
        # last_external_sync has been updated.
        assert patron.last_external_sync != None

        # external_type was updated based on the regular expression
        eq_("2", patron.external_type)
        
    def test_update_patron_metadata_noop_if_no_remote_metadata(self):

        patron = self._patron()
        provider = self.mock_basic(patrondata=None)
        provider.update_patron_metadata(patron)

        # We can tell that update_patron_metadata was a no-op because
        # patron.last_external_sync didn't change.
        eq_(None, patron.last_external_sync)

    def test_remote_patron_lookup_is_noop(self):
        """The default implementation of remote_patron_lookup is a no-op."""
        provider = BasicAuthenticationProvider(
            self._default_library, self._external_integration(self._str)
        )
        eq_(None, provider.remote_patron_lookup(None))
        patron = self._patron()
        eq_(patron, provider.remote_patron_lookup(patron))
        patrondata = PatronData()
        eq_(patrondata, provider.remote_patron_lookup(patrondata))

    def test_update_patron_external_type(self):
        patron = self._patron()
        patron.authorization_identifier = "A123"
        patron.external_type = "old value"
        library = patron.library
        integration = self._external_integration(self._str)

        class MockProvider(AuthenticationProvider):
            NAME = "Just a mock"
        
        setting = ConfigurationSetting.for_library_and_externalintegration(
            self._db, MockProvider.EXTERNAL_TYPE_REGULAR_EXPRESSION,
            library, integration
        )
        setting.value = None

        # If there is no EXTERNAL_TYPE_REGULAR_EXPRESSION, calling
        # update_patron_external_type does nothing.
        MockProvider(library, integration).update_patron_external_type(
            patron
        )
        eq_("old value", patron.external_type)
        
        setting.value = "([A-Z])"
        MockProvider(library, integration).update_patron_external_type(patron)
        eq_("A", patron.external_type)
        
        setting.value = "([0-9]$)"
        MockProvider(library, integration).update_patron_external_type(patron)
        eq_("3", patron.external_type)

        # These regexp has no groups, so it has no power to change
        # external_type.
        setting.value = "A"
        MockProvider(library, integration).update_patron_external_type(patron)
        eq_("3", patron.external_type)

        # This regexp is invalid, so it isn't used.
        setting.value = "(not a valid regexp"
        provider = MockProvider(library, integration)
        eq_(None, provider.external_type_regular_expression)

    def test_restriction_matches(self):
        """Test the behavior of the patron identifier restriction 
        algorithm.
        """
        m = AuthenticationProvider._restriction_matches
        eq_(True, m(None, None))
        eq_(True, m("12345a", "1234"))
        eq_(True, m("a1234", re.compile("1234")))
        eq_(True, m("123", re.compile("^(12|34)")))
        eq_(True, m("345", re.compile("^(12|34)")))
        
        eq_(False, m(None, "1234"))
        eq_(False, m(None, re.compile(".*")))
        eq_(False, m("a1234", "1234"))
        eq_(False, m("abc", re.compile("^bc")))
        
    def test_patron_identifier_restriction_matches(self):
        """Test the patron_identifier_restriction_matches method."""
        provider = self.mock_basic()
        provider.patron_identifier_restriction = re.compile("23[46]5")
        m = provider.patron_identifier_restriction_matches
        eq_(True, m("23456"))
        eq_(True, m("2365"))
        eq_(False, m("2375"))

        provider.patron_identifier_restriction = "2345"
        eq_(True, m("23456"))
        eq_(False, m("123456"))

    def test_patron_identifier_restriction(self):
        library = self._default_library
        integration = self._external_integration(self._str)

        class MockProvider(AuthenticationProvider):
            NAME = "Just a mock"
        
        setting = ConfigurationSetting.for_library_and_externalintegration(
            self._db, MockProvider.PATRON_IDENTIFIER_RESTRICTION,
            library, integration
        )

        # If the setting value starts with a carat, it's turned into a
        # regular expression.
        setting.value = "^abcd"
        provider = MockProvider(library, integration)
        eq_("^abcd", provider.patron_identifier_restriction.pattern)

        # Otherwise, it's a regular string that is used as a prefix.
        setting.value = "abcd"
        provider = MockProvider(library, integration)
        eq_("abcd", provider.patron_identifier_restriction)


class TestBasicAuthenticationProvider(AuthenticatorTest):

    def test_constructor(self):

        b = BasicAuthenticationProvider
        
        class ConfigAuthenticationProvider(b):
            NAME = "Config loading test"

        
        integration = self._external_integration(
            self._str, goal=ExternalIntegration.PATRON_AUTH_GOAL
        )
        self._default_library.integrations.append(integration)
        integration.setting(b.IDENTIFIER_REGULAR_EXPRESSION).value = "idre"
        integration.setting(b.PASSWORD_REGULAR_EXPRESSION).value = "pwre"
        integration.setting(b.TEST_IDENTIFIER).value = "username"
        integration.setting(b.TEST_PASSWORD).value = "pw"

        provider = ConfigAuthenticationProvider(
            self._default_library, integration
        )
        eq_("idre", provider.identifier_re.pattern)
        eq_("pwre", provider.password_re.pattern)
        eq_("username", provider.test_username)
        eq_("pw", provider.test_password)

        # Test the defaults.
        integration = self._external_integration(
            self._str, goal=ExternalIntegration.PATRON_AUTH_GOAL
        )
        provider = ConfigAuthenticationProvider(
            self._default_library, integration
        )
        eq_(b.DEFAULT_IDENTIFIER_REGULAR_EXPRESSION,
            provider.identifier_re)
        eq_(None, provider.password_re)
        
    
    def test_testing_patron(self):
        # You don't have to have a testing patron.
        integration = self._external_integration(self._str)
        no_testing_patron = BasicAuthenticationProvider(
            self._default_library, integration
        )
        eq_((None, None), no_testing_patron.testing_patron(self._db))

        # We configure a testing patron but their username and
        # password don't actually authenticate anyone. We don't crash,
        # but we can't look up the testing patron either.
        b = BasicAuthenticationProvider
        integration = self._external_integration(self._str)
        integration.setting(b.TEST_IDENTIFIER).value = '1'
        integration.setting(b.TEST_PASSWORD).value = '2'
        missing_patron = MockBasicAuthenticationProvider(
            self._default_library, integration, patron=None
        )
        value = missing_patron.testing_patron(self._db)
        eq_((None, "2"), value)

        # Here, we configure a testing patron who is authenticated by
        # their username and password.
        patron = self._patron()
        present_patron = MockBasicAuthenticationProvider(
            self._default_library, integration,
            patron=patron
        )
        value = present_patron.testing_patron(self._db)
        eq_((patron, "2"), value)

    def test_client_configuration(self):
        """Test that client-side configuration settings are retrieved from
        ConfigurationSetting objects.
        """
        b = BasicAuthenticationProvider
        integration = self._external_integration(self._str)
        integration.setting(
            b.IDENTIFIER_KEYBOARD).value = b.EMAIL_ADDRESS_KEYBOARD
        integration.setting(b.PASSWORD_KEYBOARD).value = b.NUMBER_PAD
        integration.setting(b.IDENTIFIER_LABEL).value = "Your Library Card"
        integration.setting(b.PASSWORD_LABEL).value = 'Password'
        
        provider = b(self._default_library, integration)

        eq_(b.EMAIL_ADDRESS_KEYBOARD, provider.identifier_keyboard)
        eq_(b.NUMBER_PAD, provider.password_keyboard)
        eq_("Your Library Card", provider.identifier_label)
        eq_("Password", provider.password_label)
        
    def test_server_side_validation(self):
        b = BasicAuthenticationProvider
        integration = self._external_integration(self._str)
        integration.setting(b.IDENTIFIER_REGULAR_EXPRESSION).value = 'foo'
        integration.setting(b.PASSWORD_REGULAR_EXPRESSION).value = 'bar'

        provider = b(self._default_library, integration)
        
        eq_(True, provider.server_side_validation("food", "barbecue"))
        eq_(False, provider.server_side_validation("food", "arbecue"))
        eq_(False, provider.server_side_validation("ood", "barbecue"))
        eq_(False, provider.server_side_validation(None, None))

        # Now test the identifier restriction for a specific library.
        integration.setting(b.IDENTIFIER_REGULAR_EXPRESSION).value = None
        integration.setting(b.PASSWORD_REGULAR_EXPRESSION).value = None
        identifier_restriction = ConfigurationSetting.for_library_and_externalintegration(
            self._db, b.PATRON_IDENTIFIER_RESTRICTION,
            self._default_library, integration
        )
        identifier_restriction.value = "food"
        provider = b(self._default_library, integration)
        eq_(True, provider.server_side_validation("food", "barbecue"))
        eq_(True, provider.server_side_validation("foodie", "barbecue"))
        eq_(PATRON_OF_ANOTHER_LIBRARY,
            provider.server_side_validation("foo", "bar"))
        
        # It's okay not to provide anything for server side validation.
        # The default settings will be used.
        integration.setting(b.IDENTIFIER_REGULAR_EXPRESSION).value = None
        integration.setting(b.PASSWORD_REGULAR_EXPRESSION).value = None
        identifier_restriction.value = None
        provider = b(self._default_library, integration)
        eq_(b.DEFAULT_IDENTIFIER_REGULAR_EXPRESSION.pattern,
            provider.identifier_re.pattern)
        eq_(None, provider.password_re)
        eq_(True, provider.server_side_validation("food", "barbecue"))
        eq_(True, provider.server_side_validation("a", None))
        eq_(False, provider.server_side_validation("!@#$", None))

        # Test maximum length of identifier and password.
        integration.setting(b.IDENTIFIER_MAXIMUM_LENGTH).value = "5"
        integration.setting(b.PASSWORD_MAXIMUM_LENGTH).value = "10"
        provider = b(self._default_library, integration)

        eq_(True, provider.server_side_validation("a", "1234"))
        eq_(False, provider.server_side_validation("a", "123456789012345"))
        eq_(False, provider.server_side_validation("abcdefghijklmnop", "1234"))

        # You can disable the password check altogether by setting maximum
        # length to zero.
        integration.setting(b.PASSWORD_MAXIMUM_LENGTH).value = "0"
        provider = b(self._default_library, integration)
        eq_(True, provider.server_side_validation("a", None))
        
    def test_local_patron_lookup(self):
        patron1 = self._patron("patron1_ext_id")
        patron1.authorization_identifier = "patron1_auth_id"
        patron1.username = "patron1"

        patron2 = self._patron("patron2_ext_id")
        patron2.authorization_identifier = "patron2_auth_id"
        patron2.username = "patron2"
        self._db.commit()
        
        provider = self.mock_basic()

        # If we provide PatronData associated with patron1, we look up
        # patron1, even though we provided the username associated
        # with patron2.
        for patrondata_args in [
                dict(permanent_id=patron1.external_identifier),
                dict(authorization_identifier=patron1.authorization_identifier),
                dict(username=patron1.username),
                dict(permanent_id=PatronData.NO_VALUE,
                     username=PatronData.NO_VALUE,
                     authorization_identifier=patron1.authorization_identifier)
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
            patron1, provider.local_patron_lookup(
                self._db, patron1.authorization_identifier, None
            )
        )
        eq_(
            patron1, provider.local_patron_lookup(
                self._db, patron1.username, None
            )
        )
        eq_(
            None, provider.local_patron_lookup(
                self._db, patron1.external_identifier, None
            )
        )        

    def test_get_credential_from_header(self):
        provider = self.mock_basic()
        eq_(None, provider.get_credential_from_header("Bearer [some token]"))
        eq_(None, provider.get_credential_from_header(dict()))
        eq_("foo", provider.get_credential_from_header(dict(password="foo")))
        
    def test_authentication_flow_document(self):
        """Test the default authentication provider document."""
        provider = self.mock_basic()
        doc = provider.authentication_flow_document(self._db)
        eq_(_(provider.DISPLAY_NAME), doc['description'])
        eq_(provider.FLOW_TYPE, doc['type'])

        labels = doc['labels']
        eq_(provider.identifier_label, labels['login'])
        eq_(provider.password_label, labels['password'])

        inputs = doc['inputs']
        eq_(provider.identifier_keyboard,
            inputs['login']['keyboard'])
        eq_(provider.password_keyboard,
            inputs['password']['keyboard'])
        
class TestBasicAuthenticationProviderAuthenticate(AuthenticatorTest):
    """Test the complex BasicAuthenticationProvider.authenticate method."""

    # A dummy set of credentials, for use when the exact details of
    # the credentials passed in are not important.
    credentials = dict(username="user", password="pass")
    
    def test_success(self):
        patron = self._patron()
        patrondata = PatronData(permanent_id=patron.external_identifier)
        provider = self.mock_basic(patrondata=patrondata)

        # authenticate() calls remote_authenticate(), which returns the
        # queued up PatronData object. The corresponding Patron is then
        # looked up in the database.
        eq_(patron, provider.authenticate(self._db, self.credentials))

        # All the different ways the database lookup might go are covered in 
        # test_local_patron_lookup. This test only covers the case where
        # the server sends back the permanent ID of the patron.

    def test_failure_when_remote_authentication_returns_problemdetail(self):
        patron = self._patron()
        patrondata = PatronData(permanent_id=patron.external_identifier)
        provider = self.mock_basic(patrondata=UNSUPPORTED_AUTHENTICATION_MECHANISM)
        eq_(UNSUPPORTED_AUTHENTICATION_MECHANISM,
            provider.authenticate(self._db, self.credentials))

    def test_failure_when_remote_authentication_returns_none(self):
        patron = self._patron()
        patrondata = PatronData(permanent_id=patron.external_identifier)
        provider = self.mock_basic(patrondata=None)
        eq_(None,
            provider.authenticate(self._db, self.credentials))
        
    def test_server_side_validation_runs(self):
        patron = self._patron()
        patrondata = PatronData(permanent_id=patron.external_identifier)

        b = MockBasic
        integration = self._external_integration(self._str)
        integration.setting(b.IDENTIFIER_REGULAR_EXPRESSION).value = 'foo'
        integration.setting(b.PASSWORD_REGULAR_EXPRESSION).value = 'bar'
        provider = b(
            self._default_library, integration, patrondata=patrondata
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
        provider = self.mock_basic(patrondata=patrondata)

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
            fines=Money(1, "USD"),
        )

        library = self._library()
        integration = self._external_integration(
            self._str, ExternalIntegration.PATRON_AUTH_GOAL
        )
        provider = MockBasic(library, integration, patrondata=patrondata, remote_patron_lookup_patrondata=patrondata)
        patron = provider.authenticate(self._db, self.credentials)

        # A server side Patron was created from the PatronData.
        assert isinstance(patron, Patron)
        eq_(library, patron.library)
        eq_(patrondata.permanent_id, patron.external_identifier)
        eq_(patrondata.authorization_identifier,
            patron.authorization_identifier)

        # Information not relevant to the patron's identity was stored
        # in the Patron object after it was created.
        eq_(1, patron.fines)
    
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

        provider = self.mock_basic(patrondata=patrondata)
        provider.external_type_regular_expression = re.compile("^(.)")
        patron2 = provider.authenticate(self._db, self.credentials)

        # We were able to match our local patron to the patron held by the
        # authorization provider.
        eq_(patron2, patron)

        # And we updated our local copy of the patron to reflect their
        # new identifiers.
        eq_(new_identifier, patron.authorization_identifier)
        eq_(new_username, patron.username)
        eq_(patron.authorization_identifier[0], patron.external_type)

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

        provider = self.mock_basic(patrondata=patrondata)
        patron2 = provider.authenticate(self._db, self.credentials)

        # We were able to match our local patron to the patron held by the
        # authorization provider, based on the username match.
        eq_(patron2, patron)

        # And we updated our local copy of the patron to reflect their
        # new identifiers.
        eq_(new_identifier, patron.authorization_identifier)

    def test_authentication_updates_outdated_patron_on_authorization_identifier_match(self):
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

        provider = self.mock_basic(patrondata=patrondata)
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

class TestOAuthAuthenticationProvider(AuthenticatorTest):

    def test_from_config(self):
        class ConfigAuthenticationProvider(OAuthAuthenticationProvider):
            NAME = "Config loading test"

        integration = self._external_integration(
            self._str, goal=ExternalIntegration.PATRON_AUTH_GOAL
        )
        integration.username = 'client_id'
        integration.password = 'client_secret'
        integration.setting(
            ConfigAuthenticationProvider.OAUTH_TOKEN_EXPIRATION_DAYS
        ).value = 20
        provider = ConfigAuthenticationProvider(
            self._default_library, integration
        )
        eq_("client_id", provider.client_id)
        eq_("client_secret", provider.client_secret)
        eq_(20, provider.token_expiration_days)

    def test_get_credential_from_header(self):
        """There is no way to get a credential from a bearer token that can 
        be passed on to a content provider like Overdrive.
        """
        provider = MockOAuth(self._default_library)
        eq_(None, provider.get_credential_from_header("Bearer abcd"))
            
    def test_create_token(self):
        patron = self._patron()
        provider = MockOAuth(self._default_library)
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
        provider = MockOAuth(self._default_library)
        data_source = provider.token_data_source(self._db)

        # Until we call create_token, this won't work.
        eq_(None, provider.authenticated_patron(self._db, "some other token"))

        token, is_new = provider.create_token(self._db, patron, "some token")
        eq_(True, is_new)
        eq_(patron, token.patron)

        # Now it works.
        eq_(patron, provider.authenticated_patron(self._db, "some token"))

    def test_oauth_callback(self):

        mock_patrondata = PatronData(
            authorization_identifier="1234",
            username="user",
            personal_name="The User"
        )

        class CallbackImplementation(MockOAuth):
            def remote_exchange_code_for_access_token(self, _db, access_code):
                self.used_code = access_code
                return "a token"

            def remote_patron_lookup(self, bearer_token):
                return mock_patrondata

        integration = CallbackImplementation._mock_integration(
            self._db, "Mock OAuth"
        )
        setting = ConfigurationSetting.for_library_and_externalintegration(
            self._db, CallbackImplementation.PATRON_IDENTIFIER_RESTRICTION,
            self._default_library, integration
        )
        setting.value="123"

        oauth = CallbackImplementation(
            self._default_library, integration=integration
        )
        credential, patron, patrondata = oauth.oauth_callback(
            self._db, "a code"
        )

        # remote_exchange_code_for_access_token was called with the
        # access code.
        eq_("a code", oauth.used_code)

        # The bearer token became a Credential object.
        assert isinstance(credential, Credential)
        eq_("a token", credential.credential)

        # Information that could go into the Patron record did.
        assert isinstance(patron, Patron)
        eq_("1234", patron.authorization_identifier)
        eq_("user", patron.username)

        # The PatronData returned from remote_patron_lookup
        # has been passed along.
        eq_(mock_patrondata, patrondata)
        eq_("The User", patrondata.personal_name)

        # A patron whose identifier doesn't match the patron
        # identifier restriction is treated as a patron of a different
        # library.
        mock_patrondata.set_authorization_identifier("abcd")
        eq_(PATRON_OF_ANOTHER_LIBRARY, oauth.oauth_callback(self._db, "a code"))
        
    def test_authentication_flow_document(self):
        # We're about to call url_for, so we must create an
        # application context.
        os.environ['AUTOINITIALIZE'] = "False"
        from api.app import app
        self.app = app
        del os.environ['AUTOINITIALIZE']
        provider = MockOAuth(self._default_library)
        with self.app.test_request_context("/"):
            doc = provider.authentication_flow_document(self._db)
            eq_(provider.FLOW_TYPE, doc['type'])
            eq_(provider.NAME, doc['description'])
            
            # To authenticate with this provider, you must follow the
            # 'authenticate' link.
            [link] = [x for x in doc['links'] if x['rel'] == 'authenticate']
            eq_(link['href'], provider._internal_authenticate_url(self._db))

    def test_token_data_source_can_create_new_data_source(self):
        class OAuthWithUnusualDataSource(MockOAuth):
            TOKEN_DATA_SOURCE_NAME = "Unusual data source"
        oauth = OAuthWithUnusualDataSource(self._default_library)
        source, is_new = oauth.token_data_source(self._db)
        eq_(True, is_new)
        eq_(oauth.TOKEN_DATA_SOURCE_NAME, source.name)

        source, is_new = oauth.token_data_source(self._db)
        eq_(False, is_new)
        eq_(oauth.TOKEN_DATA_SOURCE_NAME, source.name)

    def test_external_authenticate_url_parameters(self):
        """Verify that external_authenticate_url_parameters generates
        realistic results when run in a real application.
        """
        # We're about to call url_for, so we must create an
        # application context.
        my_api = MockOAuth(self._default_library)
        my_api.client_id = "clientid"
        os.environ['AUTOINITIALIZE'] = "False"
        from api.app import app
        del os.environ['AUTOINITIALIZE']

        with app.test_request_context("/"):        
            params = my_api.external_authenticate_url_parameters("state", self._db)
            eq_("state", params['state'])
            eq_("clientid", params['client_id'])
            expected_url = url_for("oauth_callback", library_short_name=self._default_library.short_name, _external=True)
            eq_(expected_url, params['oauth_callback_url'])
        
class TestOAuthController(AuthenticatorTest):

    def setup(self):
        super(TestOAuthController, self).setup()
        class MockOAuthWithExternalAuthenticateURL(MockOAuth):
            def __init__(self, library, _db, external_authenticate_url, patron):
                super(MockOAuthWithExternalAuthenticateURL, self).__init__(
                    library,
                )
                self.url = external_authenticate_url
                self.patron = patron
                self.token, ignore = self.create_token(
                    _db, self.patron, "a token"
                )
                self.patrondata = PatronData(personal_name="Abcd")
                
            def external_authenticate_url(self, state, _db):
                return self.url + "?state=" + state

            def oauth_callback(self, _db, params):
                return self.token, self.patron, self.patrondata
            
        patron = self._patron()
        self.basic = self.mock_basic()
        self.oauth1 = MockOAuthWithExternalAuthenticateURL(
            self._default_library, self._db, "http://oauth1.com/", patron
        )
        self.oauth1.NAME = "Mock OAuth 1"
        self.oauth2 = MockOAuthWithExternalAuthenticateURL(
            self._default_library, self._db, "http://oauth2.org/", patron
        )
        self.oauth2.NAME = "Mock OAuth 2"

        self.library_auth = LibraryAuthenticator(
            _db=self._db,
            library=self._default_library,
            basic_auth_provider=self.basic,
            oauth_providers=[self.oauth1, self.oauth2],
            bearer_token_signing_secret="a secret"
        )

        self.auth = MockAuthenticator(
            self._default_library, 
            { 
                self._default_library.short_name : self.library_auth
            }
        )
        self.controller = OAuthController(self.auth)
    
    def test_oauth_authentication_redirect(self):
        """Test the controller method that sends patrons off to the OAuth
        provider, where they're supposed to log in.
        """
        params = dict(provider=self.oauth1.NAME)
        response = self.controller.oauth_authentication_redirect(params, self._db)
        eq_(302, response.status_code)
        expected_state = dict(redirect_uri="", provider=self.oauth1.NAME)
        expected_state = urllib.quote(json.dumps(expected_state))
        eq_("http://oauth1.com/?state=" + expected_state, response.location)

        params = dict(provider=self.oauth2.NAME, redirect_uri="http://foo.com/")
        response = self.controller.oauth_authentication_redirect(params, self._db)
        eq_(302, response.status_code)
        expected_state = urllib.quote(json.dumps(params))
        eq_("http://oauth2.org/?state=" + expected_state, response.location)

        # If we don't recognize the OAuth provider you get sent to
        # the redirect URI with a fragment containing an encoded
        # problem detail document.
        params = dict(redirect_uri="http://foo.com/",
                      provider="not an oauth provider")
        response = self.controller.oauth_authentication_redirect(params, self._db)
        eq_(302, response.status_code)
        assert response.location.startswith("http://foo.com/#")
        fragments = urlparse.parse_qs(
            urlparse.urlparse(response.location).fragment
        )
        error = json.loads(fragments.get('error')[0])
        eq_(UNKNOWN_OAUTH_PROVIDER.uri, error.get('type'))

    def test_oauth_authentication_callback(self):
        """Test the controller method that the OAuth provider is supposed
        to send patrons to once they log in on the remote side.
        """
        
        # Successful callback through OAuth provider 1.
        params = dict(code="foo", state=json.dumps(dict(provider=self.oauth1.NAME)))
        response = self.controller.oauth_authentication_callback(self._db, params)
        eq_(302, response.status_code)
        fragments = urlparse.parse_qs(urlparse.urlparse(response.location).fragment)
        token = fragments.get("access_token")[0]
        provider_name, provider_token = self.auth.decode_bearer_token(token)
        eq_(self.oauth1.NAME, provider_name)
        eq_(self.oauth1.token.credential, provider_token)
        
        # Successful callback through OAuth provider 2.
        params = dict(code="foo", state=json.dumps(dict(provider=self.oauth2.NAME)))
        response = self.controller.oauth_authentication_callback(self._db, params)
        eq_(302, response.status_code)
        fragments = urlparse.parse_qs(urlparse.urlparse(response.location).fragment)
        token = fragments.get("access_token")[0]
        provider_name, provider_token = self.auth.decode_bearer_token(token)
        eq_(self.oauth2.NAME, provider_name)
        eq_(self.oauth2.token.credential, provider_token)
            
        # State is missing so we never get to check the code.
        params = dict(code="foo")
        response = self.controller.oauth_authentication_callback(self._db, params)
        eq_(INVALID_OAUTH_CALLBACK_PARAMETERS, response)

        # Code is missing so we never check the state.
        params = dict(state=json.dumps(dict(provider=self.oauth1.NAME)))
        response = self.controller.oauth_authentication_callback(self._db, params)
        eq_(INVALID_OAUTH_CALLBACK_PARAMETERS, response)

        # In this example we're pretending to be coming in after
        # authenticating with an OAuth provider that doesn't exist.
        params = dict(code="foo", state=json.dumps(dict(provider=("not_an_oauth_provider"))))
        response = self.controller.oauth_authentication_callback(self._db, params)
        eq_(302, response.status_code)
        fragments = urlparse.parse_qs(urlparse.urlparse(response.location).fragment)
        eq_(None, fragments.get('access_token'))
        error = json.loads(fragments.get('error')[0])
        eq_(UNKNOWN_OAUTH_PROVIDER.uri, error.get('type'))
