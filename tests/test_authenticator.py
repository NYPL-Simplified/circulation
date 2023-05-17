"""Test the base authentication framework: that is, the classes that
don't interact with any particular source of truth.
"""
import pytest
from flask_babel import lazy_gettext as _
import base64
import datetime
from decimal import Decimal
import json
import os
from money import Money
import re
import urllib.request, urllib.parse, urllib.error
import urllib.parse
import flask
from flask import url_for, Flask

from freezegun import freeze_time

from core.opds import OPDSFeed
from core.user_profile import ProfileController
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

from core.util.datetime_helpers import utc_now
from core.util.problem_detail import (
    ProblemDetail,
)
from core.util.authentication_for_opds import (
    AuthenticationForOPDSDocument,
)
from core.util.http import IntegrationException
from core.mock_analytics_provider import MockAnalyticsProvider

from api.app import app
from api.announcements import Announcements
from api.millenium_patron import MilleniumPatronAPI
from api.firstbook import FirstBookAuthenticationAPI
from api.clever import CleverAuthenticationAPI
from api.util.patron import PatronUtility
from api.annotations import AnnotationWriter
from api.authenticator import (
    Authenticator,
    CirculationPatronProfileStorage,
    LibraryAuthenticator,
    AuthenticationProvider,
    BasicAuthenticationProvider,
    BasicAuthTempTokenController,
    OAuthController,
    OAuthAuthenticationProvider,
    PatronData,
)
from api.problem_details import PATRON_OF_ANOTHER_LIBRARY
from api.simple_authentication import SimpleAuthenticationProvider
from api.millenium_patron import MilleniumPatronAPI
from api.opds import LibraryAnnotator

from api.config import (
    CannotLoadConfiguration,
    Configuration,
    temp_config,
)

from api.problem_details import *
from api.testing import VendorIDTest

from core.testing import DatabaseTest
from .test_controller import ControllerTest

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
    LOGIN_BUTTON_IMAGE = "BasicButton.png"
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
    LOGIN_BUTTON_IMAGE = "OAuthButton.png"

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
        self.mock_basic_integration = self._external_integration(
            self._str, ExternalIntegration.PATRON_AUTH_GOAL
        )
        return MockBasic(
            self._default_library, self.mock_basic_integration, *args, **kwargs
        )


class TestPatronData(AuthenticatorTest):

    def setup_method(self):
        super(TestPatronData, self).setup_method()
        self.expiration_time = utc_now()
        self.data = PatronData(
            permanent_id="1",
            authorization_identifier="2",
            username="3",
            personal_name="4",
            email_address="5",
            authorization_expires=self.expiration_time,
            fines=Money(6, "USD"),
            block_reason=PatronData.NO_VALUE,
        )

    def test_to_dict(self):
        data = self.data.to_dict
        expect = dict(
            permanent_id="1",
            authorization_identifier="2",
            authorization_identifiers=["2"],
            external_type=None,
            username="3",
            personal_name="4",
            email_address="5",
            authorization_expires=self.expiration_time.strftime("%Y-%m-%d"),
            fines="6",
            block_reason=None
        )
        assert data == expect

        # Test with an empty fines field
        self.data.fines = PatronData.NO_VALUE
        data = self.data.to_dict
        expect['fines'] = None
        assert data == expect

        # Test with a zeroed-out fines field
        self.data.fines = Decimal(0.0)
        data = self.data.to_dict
        expect['fines'] = '0'
        assert data == expect

        # Test with an empty expiration time
        self.data.authorization_expires = PatronData.NO_VALUE
        data = self.data.to_dict
        expect['authorization_expires'] = None
        assert data == expect

    def test_apply(self):
        patron = self._patron()
        self.data.cached_neighborhood = "Little Homeworld"

        self.data.apply(patron)
        assert self.data.permanent_id == patron.external_identifier
        assert self.data.authorization_identifier == patron.authorization_identifier
        assert self.data.username == patron.username
        assert self.data.authorization_expires == patron.authorization_expires
        assert self.data.fines == patron.fines
        assert None == patron.block_reason
        assert "Little Homeworld" == patron.cached_neighborhood

        # This data is stored in PatronData but not applied to Patron.
        assert "4" == self.data.personal_name
        assert False == hasattr(patron, 'personal_name')
        assert "5" == self.data.email_address
        assert False == hasattr(patron, 'email_address')

        # This data is stored on the Patron object as a convenience,
        # but it's not stored in the database.
        assert "Little Homeworld" == patron.neighborhood

    def test_apply_block_reason(self):
        """If the PatronData has a reason why a patron is blocked,
        the reason is put into the Patron record.
        """
        self.data.block_reason = PatronData.UNKNOWN_BLOCK
        patron = self._patron()
        self.data.apply(patron)
        assert PatronData.UNKNOWN_BLOCK == patron.block_reason

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
        assert "2" == patron.authorization_identifier

        # If Patron.authorization_identifier is already set, it will
        # not be changed, so long as its current value is acceptable.
        data = PatronData(
            authorization_identifier=["3", "2"],
            complete=True
        )
        data.apply(patron)
        assert "2" == patron.authorization_identifier

        # If Patron.authorization_identifier ever turns out not to be
        # an acceptable value, it will be changed.
        data = PatronData(
            authorization_identifier=["3", "4"],
            complete=True
        )
        data.apply(patron)
        assert "3" == patron.authorization_identifier

    def test_apply_sets_last_external_sync_if_data_is_complete(self):
        """Patron.last_external_sync is only updated when apply() is called on
        a PatronData object that represents a full set of metadata.
        What constitutes a 'full set' depends on the authentication
        provider.
        """
        patron = self._patron()
        self.data.complete = False
        self.data.apply(patron)
        assert None == patron.last_external_sync
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
        assert "identifier 1" == patron.authorization_identifier

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
        assert "old identifier" == patron.authorization_identifier

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
        assert "identifier 1" == patron.authorization_identifier

    def test_apply_on_incomplete_information(self):
        """When we call apply() based on incomplete information (most
        commonly, the fact that a given string was successfully used
        to authenticate a patron), we are very careful about modifying
        data already in the database.
        """
        now = utc_now()

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
        assert "1234" == patron.authorization_identifier
        assert None == patron.last_external_sync

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
        assert now == patron.last_external_sync

        # If a patron authenticates with a string that is neither
        # their authorization identifier nor their username, we leave
        # their Patron record alone, except that we indicate that we
        # need to perform an external sync on them ASAP.
        patron.last_external_sync = now
        authenticated_by_weird_identifier = PatronData(
            authorization_identifier="5678", complete=False
        )
        authenticated_by_weird_identifier.apply(patron)
        assert "1234" == patron.authorization_identifier
        assert None == patron.last_external_sync

    def test_get_or_create_patron(self):
        analytics = MockAnalyticsProvider()

        # The patron didn't exist yet, so it was created
        # and an analytics event was sent.
        patron, is_new = self.data.get_or_create_patron(
            self._db, self._default_library.id, analytics
        )
        assert '2' == patron.authorization_identifier
        assert self._default_library == patron.library
        assert True == is_new
        assert CirculationEvent.NEW_PATRON == analytics.event_type
        assert 1 == analytics.count

        # Patron.neighborhood was set, even though there is no
        # value and that's not a database field.
        assert None == patron.neighborhood

        # Set a neighborhood and try again.
        self.data.neighborhood = "Achewood"

        # The same patron is returned, and no analytics
        # event was sent.
        patron, is_new = self.data.get_or_create_patron(
            self._db, self._default_library.id, analytics
        )
        assert '2' == patron.authorization_identifier
        assert False == is_new
        assert "Achewood" == patron.neighborhood
        assert 1 == analytics.count

    def test_to_response_parameters(self):

        params = self.data.to_response_parameters
        assert dict(name="4") == params

        self.data.personal_name = None
        params = self.data.to_response_parameters
        assert dict() == params


class TestCirculationPatronProfileStorage(ControllerTest):

    def test_profile_document(self):
        def mock_url_for(endpoint, library_short_name, _external=True):
            return "http://host/" + endpoint + "?" + "library_short_name=" + library_short_name

        patron = self._patron()
        storage = CirculationPatronProfileStorage(patron, mock_url_for)
        doc = storage.profile_document
        assert 'settings' in doc
        #Since there's no authdata configured, the DRM fields are not present
        assert 'drm:vendor' not in doc
        assert 'drm:clientToken' not in doc
        assert 'drm:scheme' not in doc
        assert 'links' not in doc

        #Now there's authdata configured, and the DRM fields are populated with
        #the vendor ID and a short client token
        self.initialize_adobe(patron.library)

        doc = storage.profile_document
        [adobe] = doc['drm']
        assert adobe["drm:vendor"] == "vendor id"
        assert adobe["drm:clientToken"].startswith(
            patron.library.short_name.upper() + "TOKEN"
        )
        assert adobe["drm:scheme"] == "http://librarysimplified.org/terms/drm/scheme/ACS"
        [annotations_link] = doc['links']
        assert annotations_link['rel'] == "http://www.w3.org/ns/oa#annotationService"
        assert annotations_link['href'] == "http://host/annotations?library_short_name=default"
        assert annotations_link['type'] == AnnotationWriter.CONTENT_TYPE

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
        assert analytics == auth.library_authenticators['l1'].basic_auth_provider.analytics
        assert analytics == auth.library_authenticators['l2'].basic_auth_provider.analytics

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
            def bearer_token_provider_lookup(self, *args, **kwargs):
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
            assert LIBRARY_NOT_FOUND == auth.authenticated_patron(self._db, {})
            assert LIBRARY_NOT_FOUND == auth.create_authentication_document()
            assert LIBRARY_NOT_FOUND == auth.create_authentication_headers()
            assert LIBRARY_NOT_FOUND == auth.get_credential_from_header({})
            assert LIBRARY_NOT_FOUND == auth.create_bearer_token()
            assert LIBRARY_NOT_FOUND == auth.bearer_token_provider_lookup()

        # The other libraries are in the authenticator.
        with self.app.test_request_context("/"):
            flask.request.library = l1
            assert "authenticated patron for l1" == auth.authenticated_patron(self._db, {})
            assert "authentication document for l1" == auth.create_authentication_document()
            assert "authentication headers for l1" == auth.create_authentication_headers()
            assert "credential for l1" == auth.get_credential_from_header({})
            assert "bearer token for l1" == auth.create_bearer_token()
            assert "oauth provider for l1" == auth.bearer_token_provider_lookup()
            assert "decoded bearer token for l1" == auth.decode_bearer_token()

        with self.app.test_request_context("/"):
            flask.request.library = l2
            assert "authenticated patron for l2" == auth.authenticated_patron(self._db, {})
            assert "authentication document for l2" == auth.create_authentication_document()
            assert "authentication headers for l2" == auth.create_authentication_headers()
            assert "credential for l2" == auth.get_credential_from_header({})
            assert "bearer token for l2" == auth.create_bearer_token()
            assert "oauth provider for l2" == auth.bearer_token_provider_lookup()
            assert "decoded bearer token for l2" == auth.decode_bearer_token()


class TestLibraryAuthenticator(AuthenticatorTest):

    def test_from_config_basic_auth_only(self):
        # Only a basic auth provider.
        millenium = self._external_integration(
            "api.millenium_patron", ExternalIntegration.PATRON_AUTH_GOAL,
            libraries=[self._default_library]
        )
        millenium.url = "http://url/"
        auth = LibraryAuthenticator.from_config(self._db, self._default_library)

        assert auth.basic_auth_provider != None
        assert isinstance(auth.basic_auth_provider, MilleniumPatronAPI)
        assert {} == auth.providers_by_name

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
        assert analytics == auth.basic_auth_provider.analytics

        assert 1 == len(auth.providers_by_name)
        clever = auth.providers_by_name[
            CleverAuthenticationAPI.NAME
        ]
        assert isinstance(clever, CleverAuthenticationAPI)
        assert analytics == clever.analytics

    def test_with_custom_patron_catalog(self):
        """Instantiation of a LibraryAuthenticator may
        include instantiation of a CustomPatronCatalog.
        """
        mock_catalog = object()
        class MockCustomPatronCatalog(object):
            @classmethod
            def for_library(self, library):
                self.called_with = library
                return mock_catalog

        authenticator = LibraryAuthenticator.from_config(
            self._db, self._default_library,
            custom_catalog_source=MockCustomPatronCatalog
        )
        assert self._default_library == MockCustomPatronCatalog.called_with

        # The custom patron catalog is stored as
        # authentication_document_annotator.
        assert mock_catalog == authenticator.authentication_document_annotator

    def test_config_succeeds_when_no_providers_configured(self):
        # You can call from_config even when there are no authentication
        # providers configured.

        # This should not happen in normal usage, but there will be an
        # interim period immediately after a library is created where
        # this will be its configuration.

        authenticator = LibraryAuthenticator.from_config(
            self._db, self._default_library
        )
        assert [] == list(authenticator.providers)

    def test_configuration_exception_during_from_config_stored(self):
        # If the initialization of an AuthenticationProvider from config
        # raises CannotLoadConfiguration or ImportError, the exception
        # is stored with the LibraryAuthenticator rather than being
        # propagated.

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
        assert None == auth.basic_auth_provider
        assert {} == auth.providers_by_name

        # Both integrations have left their trace in
        # initialization_exceptions.
        not_configured = auth.initialization_exceptions[misconfigured.id]
        assert isinstance(not_configured, CannotLoadConfiguration)
        assert 'First Book server not configured.' == str(not_configured)

        not_found = auth.initialization_exceptions[unknown.id]
        assert isinstance(not_found, ImportError)
        assert "No module named 'unknown protocol'" == str(not_found)

    def test_register_fails_when_integration_has_wrong_goal(self):
        integration = self._external_integration(
            "protocol", "some other goal"
        )
        auth = LibraryAuthenticator(_db=self._db, library=self._default_library)
        with pytest.raises(CannotLoadConfiguration) as excinfo:
            auth.register_provider(integration)
        assert "Was asked to register an integration with goal=some other goal as though it were a way of authenticating patrons." in str(excinfo.value)

    def test_register_fails_when_integration_not_associated_with_library(self):
        integration = self._external_integration(
            "protocol", ExternalIntegration.PATRON_AUTH_GOAL
        )
        auth = LibraryAuthenticator(_db=self._db, library=self._default_library)
        with pytest.raises(CannotLoadConfiguration) as excinfo:
            auth.register_provider(integration)
        assert "Was asked to register an integration with library {}, which doesn't use it."\
                   .format(self._default_library.name) in str(excinfo.value)

    def test_register_fails_when_integration_module_does_not_contain_provider_class(self):
        library = self._default_library
        integration = self._external_integration(
            "api.lanes", ExternalIntegration.PATRON_AUTH_GOAL
        )
        library.integrations.append(integration)
        auth = LibraryAuthenticator(_db=self._db, library=library)
        with pytest.raises(CannotLoadConfiguration) as excinfo:
            auth.register_provider(integration)
        assert "Loaded module api.lanes but could not find a class called AuthenticationProvider inside." in str(excinfo.value)

    def test_register_provider_fails_but_does_not_explode_on_remote_integration_error(self):
        library = self._default_library
        # We're going to instantiate the a mock authentication provider that
        # immediately raises a RemoteIntegrationException, which will become
        # a CannotLoadConfiguration.
        integration = self._external_integration(
            "tests.mock_authentication_provider", ExternalIntegration.PATRON_AUTH_GOAL
        )
        library.integrations.append(integration)
        auth = LibraryAuthenticator(_db=self._db, library=library)
        with pytest.raises(CannotLoadConfiguration) as excinfo:
            auth.register_provider(integration)
        assert "Could not instantiate" in str(excinfo.value)
        assert "authentication provider for library {}, possibly due to a network connection problem."\
               .format(self._default_library.name) in str(excinfo.value)

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

    def test_register_bearer_token_auth_provider(self):
        oauth = self._external_integration(
            "api.clever", ExternalIntegration.PATRON_AUTH_GOAL,
        )
        oauth.username = "client_id"
        oauth.password = "client_secret"
        self._default_library.integrations.append(oauth)
        auth = LibraryAuthenticator(_db=self._db, library=self._default_library)
        auth.register_provider(oauth)
        assert 1 == len(auth.providers_by_name)
        clever = auth.providers_by_name[
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
        with pytest.raises(CannotLoadConfiguration) as excinfo:
            LibraryAuthenticator(_db=self._db, library=self._default_library, oauth_providers=[oauth])
        assert "The secret for signing bearer tokens is not configured." in str(excinfo.value)

    def test_supports_patron_authentication(self):
        authenticator = LibraryAuthenticator.from_config(
            self._db, self._default_library
        )

        # This LibraryAuthenticator does not actually support patron
        # authentication because it has no auth providers.
        #
        # (This isn't necessarily a deal breaker, but most libraries
        # do authenticate their patrons.)
        assert False == authenticator.supports_patron_authentication

        # Adding a basic auth provider will make it start supporting
        # patron authentication.
        authenticator.basic_auth_provider = object()
        assert True == authenticator.supports_patron_authentication
        authenticator.basic_auth_provider = None

        # So will adding an OAuth provider.
        authenticator.providers_by_name[object()] = object()
        assert True == authenticator.supports_patron_authentication

    def test_identifies_individuals(self):
        # This LibraryAuthenticator does not authenticate patrons at
        # all, so it does not identify patrons as individuals.
        authenticator = LibraryAuthenticator(
            _db=self._db, library=self._default_library,
        )

        # This LibraryAuthenticator has two Authenticators, but
        # neither of them identify patrons as individuals.
        class MockAuthenticator(object):
            NAME = "mock"
            IDENTIFIES_INDIVIDUALS = False
        basic = MockAuthenticator()
        oauth = MockAuthenticator()
        authenticator = LibraryAuthenticator(
            _db=self._db, library=self._default_library,
            basic_auth_provider=basic, oauth_providers=[oauth],
            bearer_token_signing_secret=self._str
        )
        assert False == authenticator.identifies_individuals

        # If some Authenticators identify individuals and some do not,
        # the library as a whole does not (necessarily) identify
        # individuals.
        basic.IDENTIFIES_INDIVIDUALS = True
        assert False == authenticator.identifies_individuals

        # If every Authenticator identifies individuals, then so does
        # the library as a whole.
        oauth.IDENTIFIES_INDIVIDUALS = True
        assert True == authenticator.identifies_individuals


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
        assert [basic, oauth1, oauth2] == list(authenticator.providers)

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

        with pytest.raises(CannotLoadConfiguration) as excinfo:
            authenticator.register_basic_auth_provider(basic2)
        assert "Two basic auth providers configured" in str(excinfo.value)

        authenticator.register_bearer_token_auth_provider(oauth1)
        authenticator.register_bearer_token_auth_provider(oauth1)
        authenticator.register_bearer_token_auth_provider(oauth2)

        with pytest.raises(CannotLoadConfiguration) as excinfo:
            authenticator.register_bearer_token_auth_provider(oauth1_dupe)
        assert 'Two different OAuth providers claim the name "provider1"' in str(excinfo.value)

    def test_bearer_token_provider_lookup(self):

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
        problem = authenticator.bearer_token_provider_lookup("provider1")
        assert problem.uri == UNKNOWN_OAUTH_PROVIDER.uri
        assert _("No relevant providers are configured.") == problem.detail

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

        provider = authenticator.bearer_token_provider_lookup("provider1")
        assert oauth1 == provider

        problem = authenticator.bearer_token_provider_lookup("provider3")
        assert problem.uri == UNKNOWN_OAUTH_PROVIDER.uri
        assert (
            _("The specified OAuth provider name isn't one of the known providers. The known providers are: provider1, provider2") ==
            problem.detail)

    def test_authenticated_patron_basic(self):
        patron = self._patron()
        patrondata = PatronData(
            permanent_id=patron.external_identifier,
            authorization_identifier=patron.authorization_identifier,
            username=patron.username, neighborhood="Achewood"
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
        assert (
            patron ==
            authenticator.authenticated_patron(
                self._db, dict(username="foo", password="bar")
            ))

        # Neighborhood information is being temporarily stored in the
        # Patron object for use elsewhere in request processing. It
        # won't be written to the database because there's no field in
        # `patrons` to store it.
        assert "Achewood" == patron.neighborhood

        # OAuth doesn't work.
        problem = authenticator.authenticated_patron(
            self._db, "Bearer abcd"
        )
        assert INVALID_OAUTH_BEARER_TOKEN == problem

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
        assert patron1 == authenticated

        # Basic auth doesn't work.
        problem = authenticator.authenticated_patron(
            self._db, dict(username="foo", password="bar")
        )
        assert UNSUPPORTED_AUTHENTICATION_MECHANISM == problem

    def test_authenticated_patron_unsupported_mechanism(self):
        authenticator = LibraryAuthenticator(
            _db=self._db,
            library=self._default_library,
        )
        problem = authenticator.authenticated_patron(
            self._db, object()
        )
        assert UNSUPPORTED_AUTHENTICATION_MECHANISM == problem

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
        assert ("foo" ==
            authenticator.get_credential_from_header(credential))

        # We can't pull the password out if only OAuth authentication
        # providers are configured.
        authenticator = LibraryAuthenticator(
            _db=self._db,
            library=self._default_library,
            basic_auth_provider=None, oauth_providers=[oauth],
            bearer_token_signing_secret="secret"
        )
        assert (None ==
            authenticator.get_credential_from_header(credential))


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
        assert ("eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ0b2tlbiI6InNvbWUgdG9rZW4iLCJpc3MiOiJvYXV0aDEifQ.toy4qdoziL99SN4q9DRMdN-3a0v81CfVjwJVFNUt_mk" ==
            token1)

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
        assert token_value == decoded

        decoded = authenticator.decode_bearer_token_from_header(
            "Bearer " + encoded
        )
        assert token_value == decoded

    def test_create_authentication_document_no_basic_oauth(self):
        class MockAuthenticator(LibraryAuthenticator):
            pass

        integration = self._external_integration(self._str)
        library = self._default_library

        basic = MockBasicAuthenticationProvider(library, integration)
        oauth = MockOAuthAuthenticationProvider(library, "oauth")
        authenticator = MockAuthenticator(
            _db=self._db,
            library = library,
            basic_auth_provider=basic, oauth_providers=[oauth],
            bearer_token_signing_secret='secret'
        )

        # We're about to call url_for, so we must create an application context.
        os.environ['AUTOINITIALIZE'] = "False"
        from api.app import app
        self.app = app
        del os.environ['AUTOINITIALIZE']
        with self.app.test_request_context("/"):

            doc = json.loads(authenticator.create_authentication_document())
            # The main thing we need to test is that there is no basic_oauth_doc
            flows = doc['authentication']
            assert len(flows) == 2
            oauth_doc, basic_doc = sorted(flows, key=lambda x: x['type'])

            expect_basic = basic.authentication_flow_document(self._db)
            assert expect_basic == [basic_doc]

            expect_oauth = oauth.authentication_flow_document(self._db)
            assert expect_oauth == oauth_doc

    def test_create_authentication_document(self):

        class MockAuthenticator(LibraryAuthenticator):
            """Mock the _geographic_areas method."""
            AREAS = ["focus area", "service area"]

            @classmethod
            def _geographic_areas(cls, library):
                return cls.AREAS

        integration = self._external_integration(self._str)
        library = self._default_library

        # Enable Basic Auth OAuth before passing into authenticators
        ConfigurationSetting.for_library_and_externalintegration(
            self._db,
            BasicAuthenticationProvider.HTTP_BASIC_OAUTH_ENABLED,
            library,
            integration
        ).value = "true"

        basic = MockBasicAuthenticationProvider(library, integration)
        oauth = MockOAuthAuthenticationProvider(library, "oauth")
        oauth.URI = "http://example.org/"
        library.name = "A Fabulous Library"
        authenticator = MockAuthenticator(
            _db=self._db,
            library = library,
            basic_auth_provider=basic, oauth_providers=[oauth],
            bearer_token_signing_secret='secret'
        )

        class MockAuthenticationDocumentAnnotator(object):
            def annotate_authentication_document(
                self, library, doc, url_for
            ):
                self.called_with = library, doc, url_for
                doc['modified'] = 'Kilroy was here'
                return doc

        annotator = MockAuthenticationDocumentAnnotator()
        authenticator.authentication_document_annotator = annotator

        # We're about to call url_for, so we must create an
        # application context.
        os.environ['AUTOINITIALIZE'] = "False"
        from api.app import app
        self.app = app
        del os.environ['AUTOINITIALIZE']

        # Set up configuration settings for links.
        link_config = {
            LibraryAnnotator.TERMS_OF_SERVICE: "http://terms",
            LibraryAnnotator.PRIVACY_POLICY: "http://privacy",
            LibraryAnnotator.COPYRIGHT: "http://copyright",
            LibraryAnnotator.ABOUT: "http://about",
            LibraryAnnotator.LICENSE: "http://license/",
            LibraryAnnotator.REGISTER: "custom-registration-hook://library/",
            Configuration.LOGO: "image data",
            Configuration.WEB_CSS_FILE: "http://style.css",
        }

        for rel, value in link_config.items():
            ConfigurationSetting.for_library(rel, self._default_library).value = value

        ConfigurationSetting.for_library(
            Configuration.LIBRARY_DESCRIPTION, library
        ).value = "Just the best."

        # Set the URL to the library's web page.
        ConfigurationSetting.for_library(
            Configuration.WEBSITE_URL, library).value = "http://library/"

        # Set the color scheme a mobile client should use.
        ConfigurationSetting.for_library(
            Configuration.COLOR_SCHEME, library).value = "plaid"

        # Set the colors a web client should use.
        ConfigurationSetting.for_library(
            Configuration.WEB_PRIMARY_COLOR, library).value = "#012345"
        ConfigurationSetting.for_library(
            Configuration.WEB_SECONDARY_COLOR, library).value = "#abcdef"

        # Configure the various ways a patron can get help.
        ConfigurationSetting.for_library(
            Configuration.HELP_EMAIL, library).value = "help@library"
        ConfigurationSetting.for_library(
            Configuration.HELP_WEB, library).value = "http://library.help/"
        ConfigurationSetting.for_library(
            Configuration.HELP_URI, library).value = "custom:uri"
        ConfigurationSetting.for_library(
            Configuration.HELP_UNSUBSCRIBE_URI, library).value = 'http://library.unsubscribe/'

        base_url = ConfigurationSetting.sitewide(self._db, Configuration.BASE_URL_KEY)
        base_url.value = 'http://circulation-manager/'

        # Configure three announcements: two active and one
        # inactive.
        format = '%Y-%m-%d'
        today = datetime.date.today()
        tomorrow = (today + datetime.timedelta(days=1)).strftime(format)
        yesterday = (today - datetime.timedelta(days=1)).strftime(format)
        two_days_ago = (today - datetime.timedelta(days=2)).strftime(format)
        today = today.strftime(format)
        announcements = [
            dict(
                id='a1', content='this is announcement 1',
                start=yesterday, finish=today,
            ),
            dict(
                id='a2', content='this is announcement 2',
                start=two_days_ago, finish=yesterday,
            ),
            dict(
                id='a3', content='this is announcement 3',
                start=yesterday, finish=today,
            ),
        ]
        announcement_setting = ConfigurationSetting.for_library(
            Announcements.SETTING_NAME, library
        )
        announcement_setting.value = json.dumps(announcements)

        with self.app.test_request_context("/"):
            url = authenticator.authentication_document_url(library)
            assert url.endswith(
                "/%s/authentication_document" % library.short_name
            )

            doc = json.loads(authenticator.create_authentication_document())
            # The main thing we need to test is that the
            # authentication sub-documents are assembled properly and
            # placed in the right position.
            flows = doc['authentication']
            basic_oauth_doc, oauth_doc, basic_doc = sorted(flows, key=lambda x: x['type'])

            expect_basic = basic.authentication_flow_document(self._db)
            assert expect_basic == [basic_doc, basic_oauth_doc]

            expect_oauth = oauth.authentication_flow_document(self._db)
            assert expect_oauth == oauth_doc

            # We also need to test that the library's name and ID
            # were placed in the document.
            assert "A Fabulous Library" == doc['title']
            assert "Just the best." == doc['service_description']
            assert url == doc['id']

            # The mobile color scheme and web colors are correctly reported.
            assert "plaid" == doc['color_scheme']
            assert "#012345" == doc['web_color_scheme']['primary']
            assert "#abcdef" == doc['web_color_scheme']['secondary']

            # _geographic_areas was called and provided the library's
            # focus area and service area.
            assert "focus area" == doc["focus_area"]
            assert "service area" == doc["service_area"]

            # We also need to test that the links got pulled in
            # from the configuration.
            (about, alternate, copyright, help_uri, help_web, help_email,
             copyright_agent, unsubscribe_link, profile, loans, license, logo,
             privacy_policy, register, start, stylesheet, terms_of_service)\
                 = sorted(doc['links'], key=lambda x: (x['rel'], x['href']))
            assert "http://terms" == terms_of_service['href']
            assert "http://privacy" == privacy_policy['href']
            assert "http://copyright" == copyright['href']
            assert "http://about" == about['href']
            assert "http://license/" == license['href']
            assert "image data" == logo['href']
            assert "http://style.css" == stylesheet['href']

            assert ("/loans" in loans['href'])
            assert "http://opds-spec.org/shelf" == loans['rel']
            assert OPDSFeed.ACQUISITION_FEED_TYPE == loans['type']

            assert ("/patrons/me" in profile['href'])
            assert ProfileController.LINK_RELATION == profile['rel']
            assert ProfileController.MEDIA_TYPE == profile['type']

            expect_start = url_for(
                "index", library_short_name=self._default_library.short_name,
                _external=True
            )
            assert expect_start == start['href']

            # The start link points to an OPDS feed.
            assert OPDSFeed.ACQUISITION_FEED_TYPE == start['type']

            # Most of the other links have type='text/html'
            assert "text/html" == about['type']

            # The registration link doesn't have a type, because it
            # uses a non-HTTP URI scheme.
            assert 'type' not in register
            assert 'custom-registration-hook://library/' == register['href']

            # The logo link has type "image/png".
            assert "image/png" == logo["type"]

            # We have three help links.
            assert "custom:uri" == help_uri['href']
            assert "http://library.help/" == help_web['href']
            assert "text/html" == help_web['type']
            assert "mailto:help@library" == help_email['href']

            # We also have an unsubscribe link
            assert 'http://library.unsubscribe/' == unsubscribe_link['href']

            # Since no special address was given for the copyright
            # designated agent, the help address was reused.
            copyright_rel = "http://librarysimplified.org/rel/designated-agent/copyright"
            assert copyright_rel == copyright_agent['rel']
            assert "mailto:help@library" == copyright_agent['href']

            # The public key is correct.
            assert authenticator.public_key == doc['public_key']['value']
            assert "RSA" == doc['public_key']['type']


            # The library's web page shows up as an HTML alternate
            # to the OPDS server.
            assert (
                dict(rel="alternate", type="text/html", href="http://library/") ==
                alternate)

            # Active announcements are published; inactive announcements are not.
            a1, a3 = doc['announcements']
            assert (
                dict(id='a1', content='this is announcement 1') ==
                a1)
            assert (
                dict(id='a3', content='this is announcement 3') ==
                a3)

            # Features that are enabled for this library are communicated
            # through the 'features' item.
            features = doc['features']
            assert [] == features['disabled']
            assert [Configuration.RESERVATIONS_FEATURE] == features['enabled']

            # If a separate copyright designated agent is configured,
            # that email address is used instead of the default
            # patron support address.
            ConfigurationSetting.for_library(
                Configuration.COPYRIGHT_DESIGNATED_AGENT_EMAIL, library).value = "mailto:dmca@library.org"
            doc = json.loads(authenticator.create_authentication_document())
            [agent] = [x for x in doc['links'] if x['rel'] == copyright_rel]
            assert "mailto:dmca@library.org" == agent["href"]

            # If no focus area or service area are provided, those fields
            # are not added to the document.
            MockAuthenticator.AREAS = [None, None]
            doc = json.loads(authenticator.create_authentication_document())
            for key in ('focus_area', 'service_area'):
                assert key not in doc

            # If there are no announcements, the list of announcements is present
            # but empty.
            announcement_setting.value = None
            doc = json.loads(authenticator.create_authentication_document())
            assert [] == doc['announcements']

            # The annotator's annotate_authentication_document method
            # was called and successfully modified the authentication
            # document.
            assert (library, doc, url_for) == annotator.called_with
            assert 'Kilroy was here' == doc['modified']

            # While we're in this context, let's also test
            # create_authentication_headers.

            # So long as the authenticator includes a basic auth
            # provider, that provider's .authentication_header is used
            # for WWW-Authenticate.
            headers = authenticator.create_authentication_headers()
            assert AuthenticationForOPDSDocument.MEDIA_TYPE == headers['Content-Type']
            assert basic.authentication_header == headers['WWW-Authenticate']

            # The response contains a Link header pointing to the authentication
            # document
            expect = "<%s>; rel=%s" % (
                authenticator.authentication_document_url(self._default_library),
                AuthenticationForOPDSDocument.LINK_RELATION
            )
            assert expect == headers['Link']


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

    def test_key_pair(self):
        """Test the public/private key pair associated with a library."""

        library = self._default_library

        # Initially, the KEY_PAIR setting is not set.
        def keys():
            return ConfigurationSetting.for_library(
                Configuration.KEY_PAIR, library
            ).json_value
        assert None == keys()

        # Instantiating a LibraryAuthenticator for a library automatically
        # generates a public/private key pair.
        auth = LibraryAuthenticator.from_config(self._db, library)
        public, private = keys()
        assert 'BEGIN PUBLIC KEY' in public
        assert 'BEGIN RSA PRIVATE KEY' in private

        # The public key is stored in the
        # LibraryAuthenticator.public_key property.
        assert public == auth.public_key

        # The private key is not stored in the LibraryAuthenticator
        # object, but it can be obtained from the database by
        # using the key_pair property.
        assert not hasattr(auth, 'private_key')
        assert (public, private) == auth.key_pair

        # Each library has its own key pair.
        library2 = self._library()
        auth2 = LibraryAuthenticator.from_config(self._db, library2)
        assert auth.public_key != auth2.public_key

    def test__geographic_areas(self):
        """Test the _geographic_areas helper method."""
        class Mock(LibraryAuthenticator):
            values = {
                Configuration.LIBRARY_FOCUS_AREA : "focus",
                Configuration.LIBRARY_SERVICE_AREA : "service",
            }
            @classmethod
            def _geographic_area(cls, key, library):
                cls.called_with = library
                return cls.values.get(key)

        # _geographic_areas calls _geographic_area twice and
        # reutrns the results in a 2-tuple.
        m = Mock._geographic_areas
        library = object()
        assert ("focus", "service") == m(library)
        assert library == Mock.called_with

        # If only one value is provided, the same value is given for both
        # areas.
        del Mock.values[Configuration.LIBRARY_FOCUS_AREA]
        assert ("service", "service") == m(library)

        Mock.values[Configuration.LIBRARY_FOCUS_AREA] = "focus"
        del Mock.values[Configuration.LIBRARY_SERVICE_AREA]
        assert ("focus", "focus") == m(library)

    def test__geographic_area(self):
        """Test the _geographic_area helper method."""
        library = self._default_library
        key = "a key"
        setting = ConfigurationSetting.for_library(key, library)

        def m():
            return LibraryAuthenticator._geographic_area(key, library)

        # A missing value is returned as None.
        assert None == m()

        # The literal string "everywhere" is returned as is.
        setting.value = "everywhere"
        assert "everywhere" == m()

        # A string that makes sense as JSON is returned as its JSON
        # equivalent.
        two_states = ["NY", "NJ"]
        setting.value = json.dumps(two_states)
        assert two_states == m()

        # A string that does not make sense as JSON is put in a
        # single-element list.
        setting.value = "Arvin, CA"
        assert ["Arvin, CA"] == m()


class TestAuthenticationProvider(AuthenticatorTest):

    credentials = dict(username='user', password='')

    def test_external_integration(self):
        provider = self.mock_basic(patrondata=None)
        assert (self.mock_basic_integration ==
            provider.external_integration(self._db))

    def test_authenticated_patron_passes_on_none(self):
        provider = self.mock_basic(patrondata=None)
        patron = provider.authenticated_patron(
            self._db, self.credentials
        )
        assert None == patron

    def test_authenticated_patron_passes_on_problem_detail(self):
        provider = self.mock_basic(
            patrondata=UNSUPPORTED_AUTHENTICATION_MECHANISM
        )
        patron = provider.authenticated_patron(
            self._db, self.credentials
        )
        assert UNSUPPORTED_AUTHENTICATION_MECHANISM == patron

    def test_authenticated_patron_allows_access_to_expired_credentials(self):
        """Even if your card has expired, you can log in -- you just can't
        borrow books.
        """
        yesterday = utc_now() - datetime.timedelta(days=1)

        expired = PatronData(permanent_id="1", authorization_identifier="2",
                             authorization_expires=yesterday)
        provider = self.mock_basic(
                             patrondata=expired,
                             remote_patron_lookup_patrondata=expired
        )
        patron = provider.authenticated_patron(
            self._db, self.credentials
        )
        assert "1" == patron.external_identifier
        assert "2" == patron.authorization_identifier

    def test_authenticated_patron_updates_metadata_if_necessary(self):
        patron = self._patron()
        assert True == PatronUtility.needs_external_sync(patron)

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
            username=username, cached_neighborhood="Little Homeworld",
            complete=True
        )

        provider = self.mock_basic(
            patrondata=incomplete_data,
            remote_patron_lookup_patrondata=complete_data
        )
        patron2 = provider.authenticated_patron(
            self._db, self.credentials
        )

        # We found the right patron.
        assert patron == patron2

        # We updated their metadata.
        assert "user" == patron.username
        assert barcode == patron.authorization_identifier
        assert "Little Homeworld" == patron.cached_neighborhood

        # .cached_neighborhood (stored in the database) was reused as
        # .neighborhood (destroyed at the end of the request)
        assert "Little Homeworld" == patron.neighborhood

        # We did a patron lookup, which means we updated
        # .last_external_sync.
        assert patron.last_external_sync != None
        assert barcode == patron.authorization_identifier
        assert username == patron.username

        # Looking up the patron a second time does not cause another
        # metadata refresh, because we just did a refresh and the
        # patron has borrowing privileges.
        last_sync = patron.last_external_sync
        assert False == PatronUtility.needs_external_sync(patron)
        patron = provider.authenticated_patron(
            self._db, dict(username=username)
        )
        assert last_sync == patron.last_external_sync
        assert barcode == patron.authorization_identifier
        assert username == patron.username

        # Here, patron.neighborhood was copied over from
        # patron.cached_neighborhood. It couldn't have been set by a
        # metadata refresh, because there was no refresh.
        assert "Little Homeworld" == patron.neighborhood

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
        assert barcode == patron.authorization_identifier
        assert username == patron.username

    def test_update_patron_metadata(self):
        patron = self._patron()
        patron.authorization_identifier="2345"
        assert None == patron.last_external_sync
        assert None == patron.username

        patrondata = PatronData(
            username="user", neighborhood="Little Homeworld"
        )
        provider = self.mock_basic(remote_patron_lookup_patrondata=patrondata)
        provider.external_type_regular_expression = re.compile("^(.)")
        provider.update_patron_metadata(patron)

        # The patron's username has been changed.
        assert "user" == patron.username

        # last_external_sync has been updated.
        assert patron.last_external_sync != None

        # external_type was updated based on the regular expression
        assert "2" == patron.external_type

        # .neighborhood was not stored in .cached_neighborhood.  In
        # this case, it must be cheap to get .neighborhood every time,
        # and it's better not to store information we can get cheaply.
        assert "Little Homeworld" == patron.neighborhood
        assert None == patron.cached_neighborhood

    def test_update_patron_metadata_noop_if_no_remote_metadata(self):

        patron = self._patron()
        provider = self.mock_basic(patrondata=None)
        provider.update_patron_metadata(patron)

        # We can tell that update_patron_metadata was a no-op because
        # patron.last_external_sync didn't change.
        assert None == patron.last_external_sync

    def test_remote_patron_lookup(self):
        """The default implementation of remote_patron_lookup returns whatever was passed in."""
        provider = BasicAuthenticationProvider(
            self._default_library, self._external_integration(self._str)
        )
        assert None == provider.remote_patron_lookup(None)
        patron = self._patron()
        assert patron == provider.remote_patron_lookup(patron)
        patrondata = PatronData()
        assert patrondata == provider.remote_patron_lookup(patrondata)

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
        assert "old value" == patron.external_type

        setting.value = "([A-Z])"
        MockProvider(library, integration).update_patron_external_type(patron)
        assert "A" == patron.external_type

        setting.value = "([0-9]$)"
        MockProvider(library, integration).update_patron_external_type(patron)
        assert "3" == patron.external_type

        # These regexp has no groups, so it has no power to change
        # external_type.
        setting.value = "A"
        MockProvider(library, integration).update_patron_external_type(patron)
        assert "3" == patron.external_type

        # This regexp is invalid, so it isn't used.
        setting.value = "(not a valid regexp"
        provider = MockProvider(library, integration)
        assert None == provider.external_type_regular_expression

    def test_restriction_matches(self):
        """Test the behavior of the library identifier restriction algorithm."""
        m = AuthenticationProvider._restriction_matches

        # If restriction is none, we always return True.
        assert True == m("123", None, AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_PREFIX)
        assert True == m("123", None, AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_STRING)
        assert True == m("123", None, AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_REGEX)
        assert True == m("123", None, AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_LIST)

        # If field is None we always return False.
        assert False == m(None, "1234", AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_PREFIX)
        assert False == m(None, "1234", AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_STRING)
        assert False == m(None, re.compile(".*"), AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_REGEX)
        assert False == m(None, ['1','2'], AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_LIST)

        # Test prefix
        assert True == m("12345a", "1234", AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_PREFIX)
        assert False == m("a1234", "1234", AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_PREFIX)

        # Test string
        assert False == m("12345a", "1234", AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_STRING)
        assert False == m("a1234", "1234", AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_STRING)
        assert True == m("1234", "1234", AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_STRING)

        # Test list
        assert True == m("1234", ["1234","4321"], AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_LIST)
        assert True == m("4321", ["1234", "4321"], AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_LIST)
        assert False == m("12345", ["1234", "4321"], AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_LIST)
        assert False == m("54321", ["1234", "4321"], AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_LIST)

        # Test Regex
        assert True == m("123", re.compile("^(12|34)"), AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_REGEX)
        assert True == m("345", re.compile("^(12|34)"), AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_REGEX)
        assert False == m("abc", re.compile("^bc"), AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_REGEX)

    def test_enforce_library_identifier_restriction(self):
        """Test the enforce_library_identifier_restriction method."""
        provider = self.mock_basic()
        m = provider.enforce_library_identifier_restriction
        patron = self._patron()
        patrondata = PatronData()

        #Test with patron rather than patrondata as argument
        assert patron == m(object(), patron)
        patron.library_id = -1
        assert False == m(object(), patron)

        # Test no restriction
        provider.library_identifier_restriction_type = MockBasic.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_NONE
        provider.library_identifier_restriction = "2345"
        provider.library_identifier_field = MockBasic.LIBRARY_IDENTIFIER_RESTRICTION_BARCODE
        assert patrondata == m("12365", patrondata)

        # Test regex against barcode
        provider.library_identifier_restriction_type = MockBasic.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_REGEX
        provider.library_identifier_restriction = re.compile("23[46]5")
        provider.library_identifier_field = MockBasic.LIBRARY_IDENTIFIER_RESTRICTION_BARCODE
        assert patrondata == m("23456", patrondata)
        assert patrondata == m("2365", patrondata)
        assert False == m("2375", provider.patrondata)

        # Test prefix against barcode
        provider.library_identifier_restriction_type = MockBasic.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_PREFIX
        provider.library_identifier_restriction = "2345"
        provider.library_identifier_field = MockBasic.LIBRARY_IDENTIFIER_RESTRICTION_BARCODE
        assert patrondata == m("23456", patrondata)
        assert False == m("123456", patrondata)

        # Test string against barcode
        provider.library_identifier_restriction_type = MockBasic.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_STRING
        provider.library_identifier_restriction = "2345"
        provider.library_identifier_field = MockBasic.LIBRARY_IDENTIFIER_RESTRICTION_BARCODE
        assert False == m("123456", patrondata)
        assert patrondata == m("2345", patrondata)

        # Test match applied to field on patrondata not barcode
        provider.library_identifier_restriction_type = MockBasic.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_STRING
        provider.library_identifier_restriction = "2345"
        provider.library_identifier_field = "agent"
        patrondata.library_identifier = "2345"
        assert patrondata == m("123456", patrondata)
        patrondata.library_identifier = "12345"
        assert False == m("2345", patrondata)

    def test_patron_identifier_restriction(self):
        library = self._default_library
        integration = self._external_integration(self._str)

        class MockProvider(AuthenticationProvider):
            NAME = "Just a mock"

        string_setting = ConfigurationSetting.for_library_and_externalintegration(
            self._db, MockProvider.LIBRARY_IDENTIFIER_RESTRICTION,
            library, integration
        )

        type_setting = ConfigurationSetting.for_library_and_externalintegration(
            self._db, MockProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE,
            library, integration
        )

        # If the type is regex its converted into a regular expression.
        type_setting.value = MockProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_REGEX
        string_setting.value = "^abcd"
        provider = MockProvider(library, integration)
        assert "^abcd" == provider.library_identifier_restriction.pattern

        # If its type is list, make sure its converted into a list
        type_setting.value = MockProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_LIST
        string_setting.value = "a,b,c"
        provider = MockProvider(library, integration)
        assert ['a', 'b', 'c'] == provider.library_identifier_restriction

        # If its type is prefix make sure its a string
        type_setting.value = MockProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_PREFIX
        string_setting.value = "abc"
        provider = MockProvider(library, integration)
        assert 'abc' == provider.library_identifier_restriction

        # If its type is string make sure its a string
        type_setting.value = MockProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_STRING
        string_setting.value = "abc"
        provider = MockProvider(library, integration)
        assert 'abc' == provider.library_identifier_restriction

        # If its type is none make sure its actually None
        type_setting.value = MockProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_NONE
        string_setting.value = "abc"
        provider = MockProvider(library, integration)
        assert None == provider.library_identifier_restriction


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
        assert "idre" == provider.identifier_re.pattern
        assert "pwre" == provider.password_re.pattern
        assert "username" == provider.test_username
        assert "pw" == provider.test_password

        # Test the defaults.
        integration = self._external_integration(
            self._str, goal=ExternalIntegration.PATRON_AUTH_GOAL
        )
        provider = ConfigAuthenticationProvider(
            self._default_library, integration
        )
        assert (b.DEFAULT_IDENTIFIER_REGULAR_EXPRESSION ==
            provider.identifier_re)
        assert None == provider.password_re


    def test_testing_patron(self):

        class MockAuthenticatedPatron(MockBasicAuthenticationProvider):
            def __init__(self, *args, **kwargs):
                self._authenticated_patron_returns = kwargs.pop("_authenticated_patron_returns", None)
                super(MockAuthenticatedPatron, self).__init__(*args, **kwargs)

            def authenticated_patron(self, *args, **kwargs):
                return self._authenticated_patron_returns

        # You don't have to have a testing patron.
        integration = self._external_integration(self._str)
        no_testing_patron = BasicAuthenticationProvider(
            self._default_library, integration
        )
        assert (None, None) == no_testing_patron.testing_patron(self._db)

        # But if you don't, testing_patron_or_bust() will raise an
        # exception.
        with pytest.raises(CannotLoadConfiguration) as excinfo:
            no_testing_patron.testing_patron_or_bust(self._db)
        assert "No test patron identifier is configured" in str(excinfo.value)

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
        assert (None, "2") == value

        # And testing_patron_or_bust() still doesn't work.
        with pytest.raises(IntegrationException) as excinfo:
            missing_patron.testing_patron_or_bust(self._db)
        assert "Remote declined to authenticate the test patron." in str(excinfo.value)

        # We configure a testing patron but authenticating them
        # results in a problem detail document.
        b = BasicAuthenticationProvider
        patron = self._patron()
        integration = self._external_integration(self._str)
        integration.setting(b.TEST_IDENTIFIER).value = '1'
        integration.setting(b.TEST_PASSWORD).value = '2'
        problem_patron = MockAuthenticatedPatron(
            self._default_library, integration, patron=patron,
            _authenticated_patron_returns=PATRON_OF_ANOTHER_LIBRARY
        )
        value = problem_patron.testing_patron(self._db)
        assert patron != PATRON_OF_ANOTHER_LIBRARY
        assert (PATRON_OF_ANOTHER_LIBRARY, "2") == value

        # And testing_patron_or_bust() still doesn't work.
        with pytest.raises(IntegrationException) as excinfo:
            problem_patron.testing_patron_or_bust(self._db)
        assert "Test patron lookup returned a problem detail" in str(excinfo.value)

        # We configure a testing patron but authenticating them
        # results in something (non None) that's not a Patron
        # or a problem detail document.
        not_a_patron = "<not a patron>"
        b = BasicAuthenticationProvider
        patron = self._patron()
        integration = self._external_integration(self._str)
        integration.setting(b.TEST_IDENTIFIER).value = '1'
        integration.setting(b.TEST_PASSWORD).value = '2'
        problem_patron = MockAuthenticatedPatron(
            self._default_library, integration, patron=patron,
            _authenticated_patron_returns=not_a_patron
        )
        value = problem_patron.testing_patron(self._db)
        assert patron != not_a_patron
        assert (not_a_patron, "2") == value

        # And testing_patron_or_bust() still doesn't work.
        with pytest.raises(IntegrationException) as excinfo:
            problem_patron.testing_patron_or_bust(self._db)
        assert "Test patron lookup returned invalid value for patron" in str(excinfo.value)

        # Here, we configure a testing patron who is authenticated by
        # their username and password.
        patron = self._patron()
        present_patron = MockBasicAuthenticationProvider(
            self._default_library, integration,
            patron=patron
        )
        value = present_patron.testing_patron(self._db)
        assert (patron, "2") == value

        # Finally, testing_patron_or_bust works, returning the same
        # value as testing_patron()
        assert value == present_patron.testing_patron_or_bust(self._db)


    def test__run_self_tests(self):
        _db = object()
        class CantAuthenticateTestPatron(BasicAuthenticationProvider):
            def __init__(self):
                pass
            def testing_patron_or_bust(self, _db):
                self.called_with = _db
                raise Exception("Nope")

        # If we can't authenticate a test patron, the rest of the tests
        # aren't even run.
        provider = CantAuthenticateTestPatron()
        [result] = list(provider._run_self_tests(_db))
        assert _db == provider.called_with
        assert False == result.success
        assert "Nope" == result.exception.args[0]

        # If we can authenticate a test patron, the patron and their
        # password are passed into the next test.

        class Mock(BasicAuthenticationProvider):
            def __init__(self, patron, password):
                self.patron = patron
                self.password = password

            def testing_patron_or_bust(self, _db):
                return self.patron, self.password

            def update_patron_metadata(self, patron):
                # The patron obtained from testing_patron_or_bust
                # is passed into update_patron_metadata.
                assert patron == self.patron
                return "some metadata"

        provider = Mock("patron", "password")
        [get_patron, update_metadata] = provider._run_self_tests(object())
        assert "Authenticating test patron" == get_patron.name
        assert True == get_patron.success
        assert (provider.patron, provider.password) == get_patron.result

        assert "Syncing patron metadata" == update_metadata.name
        assert True == update_metadata.success
        assert "some metadata" == update_metadata.result

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
        integration.setting(b.IDENTIFIER_BARCODE_FORMAT).value = 'some barcode'

        provider = b(self._default_library, integration)

        assert b.EMAIL_ADDRESS_KEYBOARD == provider.identifier_keyboard
        assert b.NUMBER_PAD == provider.password_keyboard
        assert "Your Library Card" == provider.identifier_label
        assert "Password" == provider.password_label
        assert "some barcode" == provider.identifier_barcode_format

    def test_server_side_validation(self):
        b = BasicAuthenticationProvider
        integration = self._external_integration(self._str)
        integration.setting(b.IDENTIFIER_REGULAR_EXPRESSION).value = 'foo'
        integration.setting(b.PASSWORD_REGULAR_EXPRESSION).value = 'bar'

        provider = b(self._default_library, integration)

        assert True == provider.server_side_validation("food", "barbecue")
        assert False == provider.server_side_validation("food", "arbecue")
        assert False == provider.server_side_validation("ood", "barbecue")
        assert False == provider.server_side_validation(None, None)

        # If this authenticator does not look at provided passwords,
        # then the only values that will pass validation are null
        # and the empty string.
        provider.password_keyboard = provider.NULL_KEYBOARD
        assert False == provider.server_side_validation("food", "barbecue")
        assert False == provider.server_side_validation("food", "is good")
        assert False == provider.server_side_validation("food", " ")
        assert True == provider.server_side_validation("food", None)
        assert True == provider.server_side_validation("food", "")
        provider.password_keyboard = provider.DEFAULT_KEYBOARD

        # It's okay not to provide anything for server side validation.
        # The default settings will be used.
        integration.setting(b.IDENTIFIER_REGULAR_EXPRESSION).value = None
        integration.setting(b.PASSWORD_REGULAR_EXPRESSION).value = None
        provider = b(self._default_library, integration)
        assert (b.DEFAULT_IDENTIFIER_REGULAR_EXPRESSION.pattern ==
            provider.identifier_re.pattern)
        assert None == provider.password_re
        assert True == provider.server_side_validation("food", "barbecue")
        assert True == provider.server_side_validation("a", None)
        assert False == provider.server_side_validation("!@#$", None)

        # Test maximum length of identifier and password.
        integration.setting(b.IDENTIFIER_MAXIMUM_LENGTH).value = "5"
        integration.setting(b.PASSWORD_MAXIMUM_LENGTH).value = "10"
        provider = b(self._default_library, integration)

        assert True == provider.server_side_validation("a", "1234")
        assert False == provider.server_side_validation("a", "123456789012345")
        assert False == provider.server_side_validation("abcdefghijklmnop", "1234")

        # You can disable the password check altogether by setting maximum
        # length to zero.
        integration.setting(b.PASSWORD_MAXIMUM_LENGTH).value = "0"
        provider = b(self._default_library, integration)
        assert True == provider.server_side_validation("a", None)

    def test_local_patron_lookup(self):
        # This patron of another library looks just like the patron
        # we're about to create, but will never be selected.
        other_library = self._library()
        other_library_patron = self._patron(
            "patron1_ext_id", library=other_library
        )
        other_library_patron.authorization_identifier = "patron1_auth_id"
        other_library_patron.username = "patron1"

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
            assert (
                patron1 == provider.local_patron_lookup(
                    self._db, patron2.authorization_identifier, patrondata
                ))

        # If no PatronData is provided, we can look up patron1 either
        # by authorization identifier or username, but not by
        # permanent identifier.
        assert (
            patron1 == provider.local_patron_lookup(
                self._db, patron1.authorization_identifier, None
            ))
        assert (
            patron1 == provider.local_patron_lookup(
                self._db, patron1.username, None
            ))
        assert (
            None == provider.local_patron_lookup(
                self._db, patron1.external_identifier, None
            ))

    def test_get_credential_from_header(self):
        provider = self.mock_basic()
        assert None == provider.get_credential_from_header("Bearer [some token]")
        assert None == provider.get_credential_from_header(dict())
        assert "foo" == provider.get_credential_from_header(dict(password="foo"))

    def test_authentication_flow_document(self):
        """Test the default authentication provider document."""
        provider = self.mock_basic()
        provider.identifier_maximum_length=22
        provider.password_maximum_length=7
        provider.identifier_barcode_format = provider.BARCODE_FORMAT_CODABAR
        provider.oauth_enabled = True

        # We're about to call url_for, so we must create an
        # application context.
        os.environ['AUTOINITIALIZE'] = "False"
        from api.app import app
        self.app = app
        del os.environ['AUTOINITIALIZE']
        with self.app.test_request_context("/"):
            # BasicAuthenticationProvider returns 2 authentication flow documents.
            # One is for Basic Auth and one is for OAuth, we need to check both.
            docs = provider.authentication_flow_document(self._db)
            assert len(docs) == 2

            for doc in docs:
                assert _(provider.DISPLAY_NAME) == doc['description']
                assert doc['type'] in [provider.FLOW_TYPE_BASIC, provider.FLOW_TYPE_OAUTH]

                labels = doc['labels']
                assert provider.identifier_label == labels['login']
                assert provider.password_label == labels['password']

                inputs = doc['inputs']
                assert (provider.identifier_keyboard ==
                    inputs['login']['keyboard'])
                assert (provider.password_keyboard ==
                    inputs['password']['keyboard'])

                assert provider.BARCODE_FORMAT_CODABAR == inputs['login']['barcode_format']

                assert (provider.identifier_maximum_length ==
                    inputs['login']['maximum_length'])
                assert (provider.password_maximum_length ==
                    inputs['password']['maximum_length'])

                if doc.get('type') == provider.FLOW_TYPE_BASIC:
                    [logo_link] = doc['links']
                    assert "logo" == logo_link["rel"]
                    assert "http://localhost/images/" + MockBasic.LOGIN_BUTTON_IMAGE == logo_link["href"]

                if doc.get('type') == provider.FLOW_TYPE_OAUTH:
                    logo_link, authenticate_link = doc['links']
                    assert 'authenticate' == authenticate_link['rel']
                    assert url_for('http_basic_auth_token', _external=True) == authenticate_link['href']
                    assert "logo" == logo_link["rel"]
                    assert "http://localhost/images/" + MockBasic.LOGIN_BUTTON_IMAGE == logo_link["href"]

    def test_remote_patron_lookup(self):
        #remote_patron_lookup does the lookup by calling _remote_patron_lookup,
        #then calls enforce_library_identifier_restriction to make sure that the patron
        #is associated with the correct library

        class Mock(BasicAuthenticationProvider):
            def _remote_patron_lookup(self, patron_or_patrondata):
                self._remote_patron_lookup_called_with = patron_or_patrondata
                return patron_or_patrondata
            def enforce_library_identifier_restriction(self, identifier, patrondata):
                self.enforce_library_identifier_restriction_called_with = (
                    identifier, patrondata
                )
                return "Result"

        integration = self._external_integration(
            self._str, ExternalIntegration.PATRON_AUTH_GOAL
        )
        provider = Mock(self._default_library, integration)
        patron = self._patron()
        assert "Result" == provider.remote_patron_lookup(patron)
        assert provider._remote_patron_lookup_called_with == patron
        assert provider.enforce_library_identifier_restriction_called_with == (
                patron.authorization_identifier, patron
            )

    def test_scrub_credential(self):
        # Verify that the scrub_credential helper method strips extra whitespace
        # and nothing else.
        integration = self._external_integration(
            self._str, ExternalIntegration.PATRON_AUTH_GOAL
        )
        provider = BasicAuthenticationProvider(self._default_library, integration)
        m = provider.scrub_credential

        assert None == provider.scrub_credential(None)
        assert 1 == provider.scrub_credential(1)
        o = object()
        assert o == provider.scrub_credential(o)
        assert "user" == provider.scrub_credential("user")
        assert "user" == provider.scrub_credential(" user")
        assert "user" == provider.scrub_credential(" user ")
        assert "user" == provider.scrub_credential("    \ruser\t     ")
        assert b"user" == provider.scrub_credential(b" user ")

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

        # BasicAuthenticationProvider scrubs leading and trailing spaces from
        # the credentials.
        credentials_with_spaces = dict(username="  user ", password=" pass \t ")
        for creds in (self.credentials, credentials_with_spaces):
            assert patron == provider.authenticate(self._db, self.credentials)

        # All the different ways the database lookup might go are covered in
        # test_local_patron_lookup. This test only covers the case where
        # the server sends back the permanent ID of the patron.

    def _inactive_patron(self):
        """Simulate a patron who has not logged in for a really long time.

        :return: A 2-tuple (Patron, PatronData). The Patron contains
        'out-of-date' data and the PatronData containing 'up-to-date'
        data.
        """
        now = utc_now()
        long_ago = now - datetime.timedelta(hours=10000)
        patron = self._patron()
        patron.last_external_sync = long_ago

        # All of their authorization information has changed in the
        # meantime, but -- crucially -- their permanent ID has not.
        patron.authorization_identifier = "old auth id"
        patron.username = "old username"

        # Here is the up-to-date information about this patron,
        # as found in the 'ILS'.
        patrondata = PatronData(
            permanent_id=patron.external_identifier,
            username="new username",
            authorization_identifier = "new authorization identifier",
            complete=True
        )

        return patron, patrondata

    def test_success_but_local_patron_needs_sync(self):
        # This patron has not logged on in a really long time.
        patron, complete_patrondata = self._inactive_patron()

        # The 'ILS' will respond to an authentication request with a minimal
        # set of information.
        #
        # It will respond to a patron lookup request with more detailed
        # information.
        minimal_patrondata = PatronData(
            permanent_id=patron.external_identifier,
            complete=False
        )
        provider = self.mock_basic(
            patrondata=minimal_patrondata,
            remote_patron_lookup_patrondata=complete_patrondata,
        )

        # The patron can be authenticated.
        assert patron == provider.authenticate(self._db, self.credentials)

        # The Authenticator noticed that the patron's account was out
        # of sync, and since the authentication response did not
        # provide a complete set of patron information, the
        # Authenticator performed a more detailed lookup to make sure
        # that the patron's details were correct going forward.
        assert "new username" == patron.username
        assert "new authorization identifier" == patron.authorization_identifier
        assert (
            utc_now()-patron.last_external_sync
        ).total_seconds() < 10

    def test_success_with_immediate_patron_sync(self):
        # This patron has not logged on in a really long time.
        patron, complete_patrondata = self._inactive_patron()

        # The 'ILS' will respond to an authentication request with a complete
        # set of information. If a remote patron lookup were to happen,
        # it would explode.
        provider = self.mock_basic(
            patrondata=complete_patrondata,
            remote_patron_lookup_patrondata=object()
        )

        # The patron can be authenticated.
        assert patron == provider.authenticate(self._db, self.credentials)

        # Since the authentication response provided a complete
        # overview of the patron, the Authenticator was able to sync
        # the account immediately, without doing a separate remote
        # patron lookup.
        assert "new username" == patron.username
        assert "new authorization identifier" == patron.authorization_identifier
        assert (
            utc_now()-patron.last_external_sync
        ).total_seconds() < 10

    def test_failure_when_remote_authentication_returns_problemdetail(self):
        patron = self._patron()
        patrondata = PatronData(permanent_id=patron.external_identifier)
        provider = self.mock_basic(patrondata=UNSUPPORTED_AUTHENTICATION_MECHANISM)
        assert (UNSUPPORTED_AUTHENTICATION_MECHANISM ==
            provider.authenticate(self._db, self.credentials))

    def test_failure_when_remote_authentication_returns_none(self):
        patron = self._patron()
        patrondata = PatronData(permanent_id=patron.external_identifier)
        provider = self.mock_basic(patrondata=None)
        assert (None ==
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
        assert None == provider.authenticate(self._db, self.credentials)

        # This succeeds because we pass the regex test.
        assert patron == provider.authenticate(
            self._db, dict(username="food", password="barbecue"))

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
        assert None == provider.authenticate(self._db, self.credentials)


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
        assert library == patron.library
        assert patrondata.permanent_id == patron.external_identifier
        assert (patrondata.authorization_identifier ==
            patron.authorization_identifier)

        # Information not relevant to the patron's identity was stored
        # in the Patron object after it was created.
        assert 1 == patron.fines

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
        assert patron2 == patron

        # And we updated our local copy of the patron to reflect their
        # new identifiers.
        assert new_identifier == patron.authorization_identifier
        assert new_username == patron.username
        assert patron.authorization_identifier[0] == patron.external_type

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
        assert patron2 == patron

        # And we updated our local copy of the patron to reflect their
        # new identifiers.
        assert new_identifier == patron.authorization_identifier

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
        assert patron2 == patron

        # And we updated our local copy of the patron to reflect their
        # new identifiers.
        assert new_username == patron.username

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
        assert "client_id" == provider.client_id
        assert "client_secret" == provider.client_secret
        assert 20 == provider.token_expiration_days

    def test_get_credential_from_header(self):
        """There is no way to get a credential from a bearer token that can
        be passed on to a content provider like Overdrive.
        """
        provider = MockOAuth(self._default_library)
        assert None == provider.get_credential_from_header("Bearer abcd")

    def test_create_token(self):
        patron = self._patron()
        provider = MockOAuth(self._default_library)
        in_twenty_days = (
            utc_now() + datetime.timedelta(
                days=provider.token_expiration_days
            )
        )
        data_source = provider.token_data_source(self._db)
        token, is_new = provider.create_token(self._db, patron, "some token")
        assert True == is_new
        assert patron == token.patron
        assert "some token" == token.credential

        # The token expires in twenty days.
        almost_no_time = abs(token.expires - in_twenty_days)
        assert almost_no_time.seconds < 2

    def test_authenticated_patron_success(self):
        patron = self._patron()
        provider = MockOAuth(self._default_library)
        data_source = provider.token_data_source(self._db)

        # Until we call create_token, this won't work.
        assert None == provider.authenticated_patron(self._db, "some token")

        token, is_new = provider.create_token(self._db, patron, "some token")
        assert True == is_new
        assert patron == token.patron

        # Now it works.
        assert patron == provider.authenticated_patron(self._db, "some token")

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

        ConfigurationSetting.for_library_and_externalintegration(
            self._db, CallbackImplementation.LIBRARY_IDENTIFIER_RESTRICTION,
            self._default_library, integration
        ).value = "123"
        ConfigurationSetting.for_library_and_externalintegration(
            self._db, CallbackImplementation.LIBRARY_IDENTIFIER_RESTRICTION_TYPE,
            self._default_library, integration
        ).value = CallbackImplementation.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_PREFIX
        ConfigurationSetting.for_library_and_externalintegration(
            self._db, CallbackImplementation.LIBRARY_IDENTIFIER_FIELD,
            self._default_library, integration
        ).value = CallbackImplementation.LIBRARY_IDENTIFIER_RESTRICTION_BARCODE

        oauth = CallbackImplementation(
            self._default_library, integration=integration
        )
        credential, patron, patrondata = oauth.oauth_callback(
            self._db, "a code"
        )

        # remote_exchange_code_for_access_token was called with the
        # access code.
        assert "a code" == oauth.used_code

        # The bearer token became a Credential object.
        assert isinstance(credential, Credential)
        assert "a token" == credential.credential

        # Information that could go into the Patron record did.
        assert isinstance(patron, Patron)
        assert "1234" == patron.authorization_identifier
        assert "user" == patron.username

        # The PatronData returned from remote_patron_lookup
        # has been passed along.
        assert mock_patrondata == patrondata
        assert "The User" == patrondata.personal_name

        # A patron whose identifier doesn't match the patron
        # identifier restriction is treated as a patron of a different
        # library.
        mock_patrondata.set_authorization_identifier("abcd")
        assert PATRON_OF_ANOTHER_LIBRARY == oauth.oauth_callback(self._db, "a code")

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
            assert provider.FLOW_TYPE == doc['type']
            assert provider.NAME == doc['description']

            # To authenticate with this provider, you must follow the
            # 'authenticate' link.
            [auth_link] = [x for x in doc['links'] if x['rel'] == 'authenticate']
            assert auth_link['href'] == provider._internal_authenticate_url(self._db)

            [logo_link] = [x for x in doc['links'] if x['rel'] == 'logo']
            assert "http://localhost/images/" + MockOAuth.LOGIN_BUTTON_IMAGE == logo_link["href"]

    def test_token_data_source_can_create_new_data_source(self):
        class OAuthWithUnusualDataSource(MockOAuth):
            TOKEN_DATA_SOURCE_NAME = "Unusual data source"
        oauth = OAuthWithUnusualDataSource(self._default_library)
        source, is_new = oauth.token_data_source(self._db)
        assert True == is_new
        assert oauth.TOKEN_DATA_SOURCE_NAME == source.name

        source, is_new = oauth.token_data_source(self._db)
        assert False == is_new
        assert oauth.TOKEN_DATA_SOURCE_NAME == source.name

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
            assert "state" == params['state']
            assert "clientid" == params['client_id']
            expected_url = url_for(
                "oauth_callback", library_short_name=self._default_library.short_name, _external=True, _scheme='https')
            assert expected_url == params['oauth_callback_url']

class TestOAuthController(AuthenticatorTest):

    def setup_method(self):
        super(TestOAuthController, self).setup_method()
        class MockOAuthWithExternalAuthenticateURL(MockOAuth):
            def __init__(self, library, _db, external_authenticate_url, patron, root_lane=None):
                super(MockOAuthWithExternalAuthenticateURL, self).__init__(
                    library,
                )
                self.url = external_authenticate_url
                self.patron = patron
                self.token, ignore = self.create_token(
                    _db, self.patron, "a token"
                )
                self.patrondata = PatronData(personal_name="Abcd")
                self.root_lane = root_lane

            def external_authenticate_url(self, state, _db):
                return self.url + "?state=" + state

            def oauth_callback(self, _db, params):
                return self.token, self.patron, self.patrondata

        patron = self._patron()
        lane = self._lane(library=self._default_library)
        lane.root_for_patron_type = '{E}'
        patron.external_type = 'E'
        self.basic = self.mock_basic()
        self.oauth1 = MockOAuthWithExternalAuthenticateURL(
            self._default_library, self._db, "http://oauth1.com/", patron, lane.id
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
        # Test the controller method that sends patrons off to the OAuth
        # provider, where they're supposed to log in.

        params = dict(provider=self.oauth1.NAME)
        response = self.controller.oauth_authentication_redirect(params, self._db)
        assert 302 == response.status_code
        expected_state = dict(provider=self.oauth1.NAME, redirect_uri="", )
        expected_state = urllib.parse.quote(json.dumps(expected_state))
        assert "http://oauth1.com/?state=" + expected_state == response.location

        params = dict(provider=self.oauth2.NAME, redirect_uri="http://foo.com/")
        response = self.controller.oauth_authentication_redirect(params, self._db)
        assert 302 == response.status_code
        expected_state = urllib.parse.quote(json.dumps(params))
        assert "http://oauth2.org/?state=" + expected_state == response.location

        # If we don't recognize the OAuth provider you get sent to
        # the redirect URI with a fragment containing an encoded
        # problem detail document.
        params = dict(redirect_uri="http://foo.com/",
                      provider="not an oauth provider")
        response = self.controller.oauth_authentication_redirect(params, self._db)
        assert 302 == response.status_code
        assert response.location.startswith("http://foo.com/#")
        fragments = urllib.parse.parse_qs(
            urllib.parse.urlparse(response.location).fragment
        )
        error = json.loads(fragments.get('error')[0])
        assert UNKNOWN_OAUTH_PROVIDER.uri == error.get('type')

    def test_oauth_authentication_callback(self):
        """Test the controller method that the OAuth provider is supposed
        to send patrons to once they log in on the remote side.
        """

        # Successful callback through OAuth provider 1.
        with app.app_context(), app.test_request_context():
            params = dict(code="foo", state=json.dumps(dict(provider=self.oauth1.NAME)))
            response = self.controller.oauth_authentication_callback(self._db, params)
            assert 302 == response.status_code
            fragments = urllib.parse.parse_qs(urllib.parse.urlparse(response.location).fragment)
            token = fragments.get("access_token")[0]
            provider_name, provider_token = self.auth.decode_bearer_token(token)
            assert self.oauth1.NAME == provider_name
            assert self.oauth1.token.credential == provider_token
            assert str(self.oauth1.root_lane) in fragments.get('root_lane')[0]

        # Successful callback through OAuth provider 2.
        params = dict(code="foo", state=json.dumps(dict(provider=self.oauth2.NAME)))
        self._default__library.lanes.clear()
        response = self.controller.oauth_authentication_callback(self._db, params)
        assert 302 == response.status_code
        fragments = urllib.parse.parse_qs(urllib.parse.urlparse(response.location).fragment)
        token = fragments.get("access_token")[0]
        provider_name, provider_token = self.auth.decode_bearer_token(token)
        assert self.oauth2.NAME == provider_name
        assert self.oauth2.token.credential == provider_token
        assert str(self.oauth2.root_lane) == fragments.get('root_lane')[0]

        # State is missing so we never get to check the code.
        params = dict(code="foo")
        response = self.controller.oauth_authentication_callback(self._db, params)
        assert INVALID_OAUTH_CALLBACK_PARAMETERS == response

        # Code is missing so we never check the state.
        params = dict(state=json.dumps(dict(provider=self.oauth1.NAME)))
        response = self.controller.oauth_authentication_callback(self._db, params)
        assert INVALID_OAUTH_CALLBACK_PARAMETERS == response

        # In this example we're pretending to be coming in after
        # authenticating with an OAuth provider that doesn't exist.
        params = dict(code="foo", state=json.dumps(dict(provider=("not_an_oauth_provider"))))
        response = self.controller.oauth_authentication_callback(self._db, params)
        assert 302 == response.status_code
        fragments = urllib.parse.parse_qs(urllib.parse.urlparse(response.location).fragment)
        assert None == fragments.get('access_token')
        error = json.loads(fragments.get('error')[0])
        assert UNKNOWN_OAUTH_PROVIDER.uri == error.get('type')

    def test_oauth_authentication_invalid_token(self):
        """If an invalid bearer token is provided, an appropriate problem
        detail is returned.
        """
        problem = self.library_auth.authenticated_patron(
            self._db, "Bearer - this is a bad token"
        )
        assert INVALID_OAUTH_BEARER_TOKEN == problem


class TestBasicAuthTempTokenController(AuthenticatorTest):

    def setup_method(self):
        super(TestBasicAuthTempTokenController, self).setup_method()

        patron = self._patron()
        patrondata = PatronData(
            permanent_id=patron.external_identifier,
            authorization_identifier=patron.authorization_identifier,
            username='unittestuser', neighborhood="Achewood"
        )
        integration = self._external_integration(self._str)
        basic = MockBasicAuthenticationProvider(
            self._default_library, integration, patron=patron,
            patrondata=patrondata
        )

        authenticator = LibraryAuthenticator(
            _db=self._db,
            library=self._default_library,
            basic_auth_provider=basic,
            bearer_token_signing_secret="a secret"
        )

        # Patch some methods that Authenticator would use
        class MockAuthenticator:
            providers = [basic, ]
        short_name = self._default_library.short_name
        setattr(authenticator, "current_library_short_name", short_name)
        setattr(authenticator, "library_authenticators", {short_name: MockAuthenticator})

        self.controller = BasicAuthTempTokenController(authenticator)

    def test_basic_auth_temp_token(self):
        """
        GIVEN: A valid Authorization header
        WHEN:  Requesting a token and re-authenticating with the acquired token
        THEN:  Patron is able to authenticate with the acquired token
        """
        valid_credentials = base64.b64encode(b"unittestuser:unittestpassword").decode("utf-8")
        headers_basic = dict(Authorization=f"Basic {valid_credentials}")

        with app.test_request_context("/http_basic_auth_token", headers=headers_basic):
            response = self.controller.basic_auth_temp_token({}, self._db)
            assert 200 == response.status_code

            token = response.json.get('access_token')
            assert token

            # Ensure the token is valid
            # TODO test this with app.test_client or something that can hit an authed route
            headers_bearer = f"Bearer {token}"
            patron = self.controller.authenticator.authenticated_patron(self._db, headers_bearer)
            assert 'unittestuser' == patron.username

    def test_basic_auth_temp_token_patron_root_lane(self):
        """
        GIVEN: A valid Authorization header for a Patron that has a root lane
        WHEN:  Requesting a token
        THEN:  A root_lane is included in the response
        """
        lane = self._lane(library=self.controller.authenticator.library)
        lane.root_for_patron_type = '{E}'
        self.controller.authenticator.basic_auth_provider.patron.external_type = 'E'

        valid_credentials = base64.b64encode(b"unittestuser:unittestpassword").decode("utf-8")
        headers_basic = dict(Authorization=f"Basic {valid_credentials}")

        with app.test_request_context("/http_basic_auth_token", headers=headers_basic):
            response = self.controller.basic_auth_temp_token({}, self._db)
            assert 200 == response.status_code

            token = response.json.get('access_token')
            assert token

            assert (
                str(self.controller.authenticator.basic_auth_provider.patron.root_lane.id)
                in response.json.get('root_lane')
            )

    @pytest.mark.parametrize(
        'delta',
        [
            pytest.param(1, id="after_1_second"),
            pytest.param(59, id="after_59_seconds"),
            pytest.param(60, id="after_60_seconds"),
        ]
    )
    def test_basic_auth_temp_token_returns_existing(self, delta):
        """"
        GIVEN: A request to authenticate a patron with a base64 encoded username:password to recieve a token
        WHEN:  Re-requesting authentication with the same credentials within a minute of the token's creation
        THEN:  The same token is returned
        """
        valid_credentials = base64.b64encode(b"unittestuser:unittestpassword").decode("utf-8")
        headers_basic = dict(Authorization=f"Basic {valid_credentials}")
        start_datetime = datetime.datetime(2022, 1, 1, 0, 0, 0)

        with freeze_time(start_datetime) as frozen_time:
            with app.test_request_context("/http_basic_auth_token", headers=headers_basic):
                response = self.controller.basic_auth_temp_token({}, self._db)
                assert 200 == response.status_code

                token = response.json.get('access_token')
                assert token

                frozen_time.move_to(start_datetime + datetime.timedelta(seconds=delta))
                another_response = self.controller.basic_auth_temp_token({}, self._db)
                assert another_response.status_code == 200

                another_token = another_response.json.get('access_token')
                assert another_token == token

    @pytest.mark.parametrize(
        'delta',
        [
            pytest.param(61, id="after_1_minute_and_1_second"),
            pytest.param(3601, id="after_1_hour_and_1_second"),
            pytest.param(86401, id="after_1_day_and_1_second")
        ]
    )
    def test_basic_auth_temp_token_returns_new_token(self, delta):
        """
        GIVEN: A request to authenticate a patron with a base64 encoded username:password to recieve a token
        WHEN:  Re-requesting authentication with the same credentials after a minute of the token's creation
        THEN:  A new token is returned
        """
        valid_credentials = base64.b64encode(b"unittestuser:unittestpassword").decode("utf-8")
        headers_basic = dict(Authorization=f"Basic {valid_credentials}")
        start_datetime = datetime.datetime(2022, 1, 1, 0, 0, 0)

        with freeze_time(start_datetime) as frozen_time:
            with app.test_request_context("/http_basic_auth_token", headers=headers_basic):
                response = self.controller.basic_auth_temp_token({}, self._db)
                assert 200 == response.status_code

                token = response.json.get('access_token')
                assert token

                frozen_time.move_to(start_datetime + datetime.timedelta(seconds=delta))
                another_response = self.controller.basic_auth_temp_token({}, self._db)
                assert another_response.status_code == 200

                another_token = another_response.json.get('access_token')
                assert another_token != token

    def test_basic_auth_temp_token_problem_detail(self):
        """
        GIVEN: An invalid Authorization header
        WHEN:  Requesting a token
        THEN:  An UNSUPPORTED_AUTHENTICATION_MECHANISM ProblemDetail is raised
        """
        headers_basic = dict(Authorization=f"Basic invalid_authentication")

        with app.test_request_context("/http_basic_auth_token", headers=headers_basic):
            response = self.controller.basic_auth_temp_token({}, self._db)
            assert response.status_code == 400
            assert response == UNSUPPORTED_AUTHENTICATION_MECHANISM
