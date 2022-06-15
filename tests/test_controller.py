# encoding=utf8
import calendar
import datetime
import email
import json
import os
import random
import time
from contextlib import contextmanager
from decimal import Decimal
from time import mktime
from wsgiref.handlers import format_date_time

import feedparser
import flask
import urllib.parse
import pytest
from flask import Response as FlaskResponse
from flask import url_for
from flask_sqlalchemy_session import current_session
from mock import MagicMock, patch
from werkzeug.datastructures import ImmutableMultiDict
from werkzeug.exceptions import NotFound

from api.util.short_client_token import (
    ShortClientTokenUtility,
    DeviceManagementProtocolController
)
from api.annotations import AnnotationWriter
from api.app import app, initialize_database
from api.authenticator import (
    BasicAuthTempTokenController,
    BearerTokenSigner,
    CirculationPatronProfileStorage,
    LibraryAuthenticator,
    OAuthController,
)
from api.circulation import FulfillmentInfo, HoldInfo, LoanInfo
from api.circulation_exceptions import *
from api.circulation_exceptions import RemoteInitiatedServerError
from api.config import Configuration, temp_config
from api.controller import CirculationManager, CirculationManagerController
from api.custom_index import CustomIndexView
from api.lanes import (
    ContributorFacets,
    ContributorLane,
    CrawlableCollectionBasedLane,
    CrawlableCustomListBasedLane,
    CrawlableFacets,
    DynamicLane,
    HasSeriesFacets,
    JackpotFacets,
    JackpotWorkList,
    RecommendationLane,
    RelatedBooksLane,
    SeriesFacets,
    SeriesLane,
    create_default_lanes,
)
from api.novelist import MockNoveListAPI
from api.odl import MockODLAPI
from api.opds import (
    CirculationManagerAnnotator,
    LibraryAnnotator,
    SharedCollectionAnnotator,
)
from api.problem_details import *
from api.registry import Registration
from api.shared_collection import SharedCollectionAPI
from api.simple_authentication import SimpleAuthenticationProvider
from api.testing import VendorIDTest
from core import model
from core.analytics import Analytics
from core.app_server import load_facets_from_request
from core.classifier import Classifier
from core.config import CannotLoadConfiguration
from core.entrypoint import (
    AudiobooksEntryPoint,
    EbooksEntryPoint,
    EntryPoint,
    EverythingEntryPoint,
)
from core.external_search import (
    MockExternalSearchIndex,
    MockSearchResult,
    SortKeyPagination,
    mock_search_index,
)
from core.lane import (
    BaseFacets,
    Facets,
    FeaturedFacets,
    Lane,
    Pagination,
    SearchFacets,
    WorkList,
)
from core.local_analytics_provider import LocalAnalyticsProvider
from core.metadata_layer import ContributorData, Metadata
from core.model import (
    Admin,
    Annotation,
    CachedFeed,
    CachedMARCFile,
    CirculationEvent,
    Collection,
    Complaint,
    ConfigurationSetting,
    DataSource,
    DeliveryMechanism,
    Edition,
    ExternalIntegration,
    Hold,
    Identifier,
    IntegrationClient,
    Library,
    LicensePoolDeliveryMechanism,
    LinkRelations,
    Loan,
    MediaTypes,
    Patron,
    Representation,
    Resource,
    RightsStatus,
    Session,
    create,
    get_one,
    get_one_or_create,
    tuple_to_numericrange,
)
from core.opds import AcquisitionFeed, NavigationFacets, NavigationFeed
from core.problem_details import *
from core.testing import DummyHTTPClient, MockRequestsResponse
from core.user_profile import ProfileController, ProfileStorage
from core.util.authentication_for_opds import AuthenticationForOPDSDocument
from core.util.datetime_helpers import (
    datetime_utc,
    from_timestamp,
    utc_now,
)
from core.util.flask_util import Response
from core.util.http import RemoteIntegrationException
from core.util.opds_writer import OPDSFeed
from core.util.problem_detail import ProblemDetail
from core.util.string_helpers import base64

class ControllerTest(VendorIDTest):
    """A test that requires a functional app server."""

    # Authorization headers that will succeed (or fail) against the
    # SimpleAuthenticationProvider set up in ControllerTest.setup().
    valid_auth = 'Basic ' + base64.b64encode(
        'unittestuser:unittestpassword'
    )
    invalid_auth = 'Basic ' + base64.b64encode('user1:password2')
    valid_credentials = dict(
        username="unittestuser", password="unittestpassword"
    )

    def setup_method(self):
        super(ControllerTest, self).setup_method()
        self.app = app

        if not hasattr(self, 'setup_circulation_manager'):
            self.setup_circulation_manager = True

        # PRESERVE_CONTEXT_ON_EXCEPTION needs to be off in tests
        # to prevent one test failure from breaking later tests as well.
        # When used with flask's test_request_context, exceptions
        # from previous tests would cause flask to roll back the db
        # when you entered a new request context, deleting rows that
        # were created in the test setup.
        app.config['PRESERVE_CONTEXT_ON_EXCEPTION'] = False

        Configuration.instance[Configuration.INTEGRATIONS][ExternalIntegration.CDN] = {
            "" : "http://cdn"
        }

        if self.setup_circulation_manager:
            # NOTE: Any reference to self._default_library below this
            # point in this method will cause the tests in
            # TestScopedSession to hang.
            self.set_base_url(self._db)
            app.manager = self.circulation_manager_setup(self._db)

    def set_base_url(self, _db):
        base_url = ConfigurationSetting.sitewide(_db, Configuration.BASE_URL_KEY)
        base_url.value = 'http://test-circulation-manager/'

    def circulation_manager_setup(self, _db):
        """Set up initial Library arrangements for this test.

        Most tests only need one library: self._default_library.
        Other tests need a different library (e.g. one created using the
        scoped database session), or more than one library. For that
        reason we call out to a helper method to create some number of
        libraries, then initialize each one.

        NOTE: Any reference to self._default_library within this
        method will cause the tests in TestScopedSession to hang.

        This method sets values for self.libraries, self.collections,
        and self.default_patrons. These data structures contain
        information for all libraries. It also sets values for a
        single library which can be used as a default: .library,
        .collection, and .default_patron.

        :param _db: The database session to use when creating the
            library objects.

        :return: a CirculationManager object.

        """
        self.libraries = self.make_default_libraries(_db)
        self.collections = [
            self.make_default_collection(_db, library)
            for library in self.libraries
        ]
        self.default_patrons = {}

        # The first library created is used as the default -- more of the
        # time this is the same as self._default_library.
        self.library = self.libraries[0]
        self.collection = self.collections[0]

        for library in self.libraries:
            self.library_setup(library)

        # The test's default patron is the default patron for the first
        # library returned by make_default_libraries.
        self.default_patron = self.default_patrons[self.library]

        self.authdata = ShortClientTokenUtility.from_config(self.library)

        self.manager = CirculationManager(
            _db, testing=True
        )

        # Set CirculationAPI and top-level lane for the default
        # library, for convenience in tests.
        self.manager.d_circulation = self.manager.circulation_apis[
            self.library.id
        ]
        self.manager.d_top_level_lane = self.manager.top_level_lanes[
            self.library.id
        ]
        self.controller = CirculationManagerController(self.manager)

        # Set a convenient default lane.
        [self.english_adult_fiction] = [
            x for x in self.library.lanes
            if x.display_name=='Fiction' and x.languages==['eng']
        ]

        return self.manager

    def library_setup(self, library):
        """Do some basic setup for a library newly created by test code."""
        _db = Session.object_session(library)
        # Create the patron used by the dummy authentication mechanism.
        default_patron, ignore = get_one_or_create(
            _db, Patron,
            library=library,
            authorization_identifier="unittestuser",
            create_method_kwargs=dict(
                external_identifier="unittestuser"
            )
        )
        self.default_patrons[library] = default_patron

        # Create a simple authentication integration for this library,
        # unless it already has a way to authenticate patrons
        # (in which case we would just screw things up).
        if not any([x for x in library.integrations if x.goal==
                    ExternalIntegration.PATRON_AUTH_GOAL]):
            integration, ignore = create(
                _db, ExternalIntegration,
                protocol="api.simple_authentication",
                goal=ExternalIntegration.PATRON_AUTH_GOAL
            )
            p = SimpleAuthenticationProvider
            integration.setting(p.TEST_IDENTIFIER).value = "unittestuser"
            integration.setting(p.TEST_PASSWORD).value = "unittestpassword"
            integration.setting(p.TEST_NEIGHBORHOOD).value = "Unit Test West"
            library.integrations.append(integration)

        for k, v in [
                (Configuration.LARGE_COLLECTION_LANGUAGES, []),
                (Configuration.SMALL_COLLECTION_LANGUAGES, ['eng']),
                (Configuration.TINY_COLLECTION_LANGUAGES, ['spa','chi','fre'])
        ]:
            ConfigurationSetting.for_library(k, library).value = json.dumps(v)
        create_default_lanes(_db, library)

    def make_default_libraries(self, _db):
        return [self._default_library]

    def make_default_collection(self, _db, library):
        return self._default_collection

    @contextmanager
    def request_context_with_library(self, route, *args, **kwargs):
        if 'library' in kwargs:
            library = kwargs.pop('library')
        else:
            library = self._default_library
        with self.app.test_request_context(route, *args, **kwargs) as c:
            flask.request.library = library
            yield c


class CirculationControllerTest(ControllerTest):

    # These tests generally need at least one Work created,
    # but some need more.
    BOOKS = [
        ["english_1", "Quite British", "John Bull", "eng", True],
    ]

    def setup_method(self):
        super(CirculationControllerTest, self).setup_method()
        self.works = []
        for (variable_name, title, author, language, fiction) in self.BOOKS:
            work = self._work(title, author, language=language, fiction=fiction,
                              with_open_access_download=True)
            setattr(self, variable_name, work)
            work.license_pools[0].collection = self.collection
            self.works.append(work)
        self.manager.external_search.bulk_update(self.works)

        # Enable the audiobook entry point for the default library -- a lot of
        # tests verify that non-default entry points can be selected.
        self._default_library.setting(
            EntryPoint.ENABLED_SETTING
        ).value = json.dumps(
            [EbooksEntryPoint.INTERNAL_NAME, AudiobooksEntryPoint.INTERNAL_NAME]
        )

    def assert_bad_search_index_gives_problem_detail(self, test_function):
        """Helper method to test that a controller method serves a problem
        detail document when the search index isn't set up.

        Mocking a broken search index is a lot of work; thus the helper method.
        """
        old_setup = self.manager.setup_external_search
        old_value = self.manager._external_search
        self.manager._external_search = None
        self.manager.setup_external_search = lambda: None
        with self.request_context_with_library("/"):
            response = test_function()
            assert 502 == response.status_code
            assert (
                "http://librarysimplified.org/terms/problem/remote-integration-failed" ==
                response.uri)
            assert ('The search index for this site is not properly configured.' ==
                response.detail)
        self.manager.setup_external_search = old_setup
        self.manager._external_search = old_value


class TestCirculationManager(CirculationControllerTest):
    """Test the CirculationManager object itself."""

    def test_initialization(self):
        # As soon as the CirculationManager object is created,
        # it sets a public/private key pair for the site.
        public, private = ConfigurationSetting.sitewide(
            self._db, Configuration.KEY_PAIR
        ).json_value
        assert 'BEGIN PUBLIC KEY' in public
        assert 'BEGIN RSA PRIVATE KEY' in private

    def test_load_settings(self):
        # Here's a CirculationManager which we've been using for a while.
        manager = self.manager

        # Certain fields of the CirculationManager have certain values
        # which are about to be reloaded.
        manager._external_search = object()
        manager.adobe_device_management = object()
        manager.basic_auth_token_controller = object()
        manager.oauth_controller = object()
        manager.auth = object()
        manager.shared_collection_api = object()
        manager.new_custom_index_views = object()
        manager.patron_web_domains = object()
        manager.admin_web_domains = object()

        # But some fields are _not_ about to be reloaded
        index_controller = manager.index_controller

        # The CirculationManager has a top-level lane and a CirculationAPI,
        # for the default library, but no others.
        assert 1 == len(manager.top_level_lanes)
        assert 1 == len(manager.circulation_apis)

        # The authentication document cache has a default value for
        # max_age.
        assert 0 == manager.authentication_for_opds_documents.max_age

        # WSGI debug is off by default.
        assert False == manager.wsgi_debug

        # Now let's create a brand new library, never before seen.
        library = self._library()
        self.library_setup(library)

        # In addition to the setup performed by library_setup(), give it
        # a registry integration with short client tokens so we can verify
        # that the DeviceManagementProtocolController is recreated.
        self.initialize_adobe(library, [library])

        # We also register a CustomIndexView for this new library.
        mock_custom_view = object()
        @classmethod
        def mock_for_library(cls, incoming_library):
            if incoming_library == library:
                return mock_custom_view
            return None
        old_for_library = CustomIndexView.for_library
        CustomIndexView.for_library = mock_for_library

        # We also set up some configuration settings that will
        # be loaded.
        ConfigurationSetting.sitewide(
            self._db, Configuration.PATRON_WEB_HOSTNAMES).value = "http://sitewide/1234"
        registry = self._external_integration(
            protocol="some protocol", goal=ExternalIntegration.DISCOVERY_GOAL
        )
        ConfigurationSetting.for_library_and_externalintegration(
            self._db, Registration.LIBRARY_REGISTRATION_WEB_CLIENT,
            library, registry).value = "http://registration"
        
        ConfigurationSetting.sitewide(
            self._db, Configuration.ADMIN_WEB_HOSTNAMES).value = "http://admin/1234"

        ConfigurationSetting.sitewide(
            self._db, Configuration.AUTHENTICATION_DOCUMENT_CACHE_TIME
        ).value = "60"

        ConfigurationSetting.sitewide(
            self._db, Configuration.WSGI_DEBUG_KEY
        ).value = "true"

        # Then reload the CirculationManager...
        self.manager.load_settings()

        # Now the new library has a top-level lane.
        assert library.id in manager.top_level_lanes

        # And a circulation API.
        assert library.id in manager.circulation_apis

        # And a CustomIndexView.
        assert mock_custom_view == manager.custom_index_views[library.id]
        assert None == manager.custom_index_views[self._default_library.id]

        # The Authenticator has been reloaded with information about
        # how to authenticate patrons of the new library.
        assert isinstance(
            manager.auth.library_authenticators[library.short_name],
            LibraryAuthenticator
        )

        # The ExternalSearch object has been reset.
        assert isinstance(manager.external_search, MockExternalSearchIndex)

        # The Basic Auth token controller has been recreated.
        assert isinstance(manager.basic_auth_token_controller, BasicAuthTempTokenController)

        # The OAuth controller has been recreated.
        assert isinstance(manager.oauth_controller, OAuthController)

        # So has the controller for the Device Management Protocol.
        assert isinstance(manager.adobe_device_management,
                          DeviceManagementProtocolController)

        # So has the SharecCollectionAPI.
        assert isinstance(manager.shared_collection_api,
                          SharedCollectionAPI)

        # So have the patron web domains, and their paths have been
        # removed.
        assert set(["http://sitewide", "http://registration"]) == manager.patron_web_domains

        assert set(["http://admin"]) == manager.admin_web_domains

        # The authentication document cache has been rebuilt with a
        # new max_age.
        assert 60 == manager.authentication_for_opds_documents.max_age

        # The WSGI debug setting has been changed.
        assert True == manager.wsgi_debug

        # Controllers that don't depend on site configuration
        # have not been reloaded.
        assert index_controller == manager.index_controller

        # The sitewide patron web domain can also be set to *.
        ConfigurationSetting.sitewide(
            self._db, Configuration.PATRON_WEB_HOSTNAMES).value = "*"
        self.manager.load_settings()
        assert set(["*", "http://registration"]) == manager.patron_web_domains

        # The sitewide patron web domain can have pipe separated domains, and will get spaces stripped
        ConfigurationSetting.sitewide(
            self._db, Configuration.PATRON_WEB_HOSTNAMES).value = "https://1.com|http://2.com |  http://subdomain.3.com|4.com"
        self.manager.load_settings()
        assert set(["https://1.com", "http://2.com",  "http://subdomain.3.com", "http://registration"]) == manager.patron_web_domains

        # The sitewide admin web domain can also be set to *.
        ConfigurationSetting.sitewide(
            self._db, Configuration.ADMIN_WEB_HOSTNAMES).value = "*"
        self.manager.load_settings()
        assert set(["*"]) == manager.admin_web_domains

        # The admin web domain can have pipe separated domains, and will get spaces stripped
        ConfigurationSetting.sitewide(
            self._db, Configuration.ADMIN_WEB_HOSTNAMES).value = "https://1.com|http://2.com |  http://subdomain.3.com|4.com"
        self.manager.load_settings()
        assert set(["https://1.com", "http://2.com",  "http://subdomain.3.com"]) == manager.admin_web_domains

        # Restore the CustomIndexView.for_library implementation
        CustomIndexView.for_library = old_for_library

    def test_exception_during_external_search_initialization_is_stored(self):

        class BadSearch(CirculationManager):

            @property
            def setup_search(self):
                raise Exception("doomed!")

        circulation = BadSearch(self._db, testing=True)

        # We didn't get a search object.
        assert None == circulation.external_search

        # The reason why is stored here.
        ex = circulation.external_search_initialization_exception
        assert isinstance(ex, Exception)
        assert "doomed!" == str(ex)

    def test_exception_during_short_client_token_initialization_is_stored(self):

        # Create an incomplete Short Client Token setup for our
        # library.
        registry_integration = self._external_integration(
            protocol=ExternalIntegration.OPDS_REGISTRATION,
            goal=ExternalIntegration.DISCOVERY_GOAL, libraries=[self.library]
        )
        registry_integration.username = "something"
        registry_integration.set_setting(ShortClientTokenUtility.VENDOR_ID_KEY, "vendorid")

        # Then try to set up the Adobe Vendor ID configuration for
        # that library.
        self.manager.setup_adobe_vendor_id(self._db, self.library)

        # The exception caused when we tried to load the incomplete
        # configuration was stored here.
        ex = self.manager.short_client_token_initialization_exceptions[self.library.id]
        assert isinstance(ex, CannotLoadConfiguration)
        assert str(ex).startswith("Short Client Token configuration is incomplete")

    def test_setup_adobe_vendor_id_does_not_override_existing_configuration(self):
        # Our circulation manager is perfectly happy with its Adobe Vendor ID
        # configuration, which it got from one of its libraries.
        obj = object()
        self.manager.adobe_vendor_id = obj

        # This library wants to set up an Adobe Vendor ID but it doesn't
        # actually have one configured.
        self.manager.setup_adobe_vendor_id(self._db, self._default_library)

        # The sitewide Adobe Vendor ID configuration is not changed by
        # the presence of another library that doesn't have a Vendor
        # ID configuration.
        assert obj == self.manager.adobe_vendor_id

    def test_sitewide_key_pair(self):
        # A public/private key pair was created when the
        # CirculationManager was initialized. Clear it out.
        pair = ConfigurationSetting.sitewide(self._db, Configuration.KEY_PAIR)
        pair.value = None

        # Calling sitewide_key_pair will create a new pair of keys.
        new_public, new_private = self.manager.sitewide_key_pair
        assert 'BEGIN PUBLIC KEY' in new_public
        assert 'BEGIN RSA PRIVATE KEY' in new_private

        # The new values are stored in the appropriate
        # ConfigurationSetting.
        assert [new_public, new_private] == pair.json_value

        # Calling it again will do nothing.
        assert (new_public, new_private) == self.manager.sitewide_key_pair

    def test_annotator(self):
        # Test our ability to find an appropriate OPDSAnnotator for
        # any request context.

        # The simplest case -- a Lane is provided and we build a
        # LibraryAnnotator for its library
        lane = self._lane()
        facets = Facets.default(self._default_library)
        annotator = self.manager.annotator(lane, facets)
        assert isinstance(annotator, LibraryAnnotator)
        assert (self.manager.circulation_apis[self._default_library.id] ==
            annotator.circulation)
        assert "All Books" == annotator.top_level_title()
        assert True == annotator.identifies_patrons

        # Try again using a library that has no patron authentication.
        library2 = self._library()
        lane2 = self._lane(library=library2)
        mock_circulation = object()
        self.manager.circulation_apis[library2.id] = mock_circulation

        annotator = self.manager.annotator(lane2, facets)
        assert isinstance(annotator, LibraryAnnotator)
        assert library2 == annotator.library
        assert lane2 == annotator.lane
        assert facets == annotator.facets
        assert mock_circulation == annotator.circulation

        # This LibraryAnnotator knows not to generate any OPDS that
        # implies it has any way of authenticating or differentiating
        # between patrons.
        assert False == annotator.identifies_patrons

        # Any extra positional or keyword arguments passed into annotator()
        # are propagated to the Annotator constructor.
        class MockAnnotator(object):
            def __init__(self, *args, **kwargs):
                self.positional = args
                self.keyword = kwargs
        annotator = self.manager.annotator(
            lane, facets, "extra positional",
            kw="extra keyword", annotator_class=MockAnnotator
        )
        assert isinstance(annotator, MockAnnotator)
        assert 'extra positional' == annotator.positional[-1]
        assert 'extra keyword' == annotator.keyword.pop('kw')

        # Now let's try more and more obscure ways of figuring out which
        # library should be used to build the LibraryAnnotator.

        # If a WorkList initialized with a library is provided, a
        # LibraryAnnotator for that library is created.
        worklist = WorkList()
        worklist.initialize(library2)
        annotator = self.manager.annotator(worklist, facets)
        assert isinstance(annotator, LibraryAnnotator)
        assert library2 == annotator.library
        assert worklist == annotator.lane
        assert facets == annotator.facets

        # If no library can be found through the WorkList,
        # LibraryAnnotator uses the library associated with the
        # current request.
        worklist = WorkList()
        worklist.initialize(None)
        with self.request_context_with_library("/"):
            annotator = self.manager.annotator(worklist, facets)
            assert isinstance(annotator, LibraryAnnotator)
            assert self._default_library == annotator.library
            assert worklist == annotator.lane

        # If there is absolutely no library associated with this
        # request, we get a generic CirculationManagerAnnotator for
        # the provided WorkList.
        with self.app.test_request_context("/"):
            annotator = self.manager.annotator(worklist, facets)
            assert isinstance(annotator, CirculationManagerAnnotator)
            assert worklist == annotator.lane

    def test_load_facets_from_request_disable_caching(self):
        # Only an authenticated admin can ask to disable caching,
        # and load_facets_from_request is where we enforce this.
        class MockAdminSignInController(object):
            # Pretend to be able to find (or not) an Admin authenticated
            # to make the current request.
            admin = None

            def authenticated_admin_from_request(self):
                return self.admin
        admin = Admin()
        controller = MockAdminSignInController()

        self.manager.admin_sign_in_controller = controller

        with self.request_context_with_library("/"):
            # If you don't specify a max cache age, nothing happens,
            # whether or not you're an admin.
            for value in INVALID_CREDENTIALS, admin:
                controller.admin = value
                facets = self.manager.load_facets_from_request()
                assert None == facets.max_cache_age

        with self.request_context_with_library("/?max_age=0"):
            # Not an admin, max cache age requested.
            controller.admin = INVALID_CREDENTIALS
            facets = self.manager.load_facets_from_request()
            assert None == facets.max_cache_age

            # Admin, max age requested. This is the only case where
            # nonstandard caching rules make it through
            # load_facets_from_request().
            controller.admin = admin
            facets = self.manager.load_facets_from_request()
            assert CachedFeed.IGNORE_CACHE == facets.max_cache_age

        # Since the admin sign-in controller is part of the admin
        # package and not the API proper, test a situation where, for
        # whatever reason, that controller was never initialized.
        del self.manager.admin_sign_in_controller

        # Now what controller.admin says doesn't matter, because the
        # controller's not associated with the CirculationManager.
        # But everything still basically works; you just can't
        # disable the cache.
        with self.request_context_with_library("/?max_age=0"):
            for value in (INVALID_CREDENTIALS, admin):
                controller.admin = value
                facets = self.manager.load_facets_from_request()
                assert None == facets.max_cache_age

    def test_load_facets_from_request_denies_access_to_inaccessible_worklist(self):
        """You can't access a WorkList that's inaccessible to your patron
        type, and load_facets_from_request (which is called when
        presenting the WorkList) is where we enforce this.
        """
        wl = WorkList()
        wl.accessible_to = MagicMock(return_value=True)

        # The authenticated patron, if any, is passed into
        # WorkList.accessible_to.
        with self.request_context_with_library("/"):
            facets = self.manager.load_facets_from_request(worklist=wl)
            assert isinstance(facets, Facets)
            wl.accessible_to.assert_called_once_with(None)

        with self.request_context_with_library(
            "/", headers=dict(Authorization=self.valid_auth)
        ):
            facets = self.manager.load_facets_from_request(worklist=wl)
            assert isinstance(facets, Facets)
            wl.accessible_to.assert_called_with(self.default_patron)

        # The request is short-circuited if accessible_to returns
        # False.
        wl.accessible_to = MagicMock(return_value=False)
        with self.request_context_with_library("/"):
            facets = self.manager.load_facets_from_request(worklist=wl)
            assert isinstance(facets, ProblemDetail)

            # Because the patron didn't ask for a specific title, we
            # respond that the lane doesn't exist rather than saying
            # they've been denied access to age-inappropriate content.
            assert NO_SUCH_LANE.uri == facets.uri

    def test_cdn_url_for(self):
        # Test the various rules for generating a URL for a view while
        # passing it through a CDN (or not).

        # The CDN configuration itself is handled inside the
        # cdn_url_for function imported from core.app_server. So
        # mainly we just need to check when a CirculationManager calls
        # that function (via self._cdn_url_for), versus when it
        # decides to bail on the CDN and call self.url_for instead.
        class Mock(CirculationManager):
            _cdn_url_for_calls = []
            url_for_calls = []

            def _cdn_url_for(self, view, *args, **kwargs):
                self._cdn_url_for_calls.append((view, args, kwargs))
                return "http://cdn/"

            def url_for(self, view, *args, **kwargs):
                self.url_for_calls.append((view, args, kwargs))
                return "http://url/"

        manager = Mock(self._db, testing=True)

        # Normally, cdn_url_for calls _cdn_url_for to generate a URL.
        args = ("arg1", "arg2")
        kwargs = dict(key="value")
        url = manager.cdn_url_for("view", *args, **kwargs)
        assert "http://cdn/" == url
        assert ("view", args, kwargs) == manager._cdn_url_for_calls.pop()
        assert [] == manager._cdn_url_for_calls

        # But if a faceting object is passed in as _facets, it's checked
        # to see if it wants to disable caching.
        class MockFacets(BaseFacets):
            max_cache_age = None
        kwargs_with_facets = dict(kwargs)
        kwargs_with_facets.update(_facets=MockFacets)
        url = manager.cdn_url_for("view", *args, **kwargs_with_facets)

        # Here, the faceting object has no opinion on the matter, so
        # _cdn_url_for is called again.
        assert "http://cdn/" == url
        assert ("view", args, kwargs) == manager._cdn_url_for_calls.pop()
        assert [] == manager._cdn_url_for_calls

        # Here, the faceting object does have an opinion: the document
        # being generated should not be stored in a cache. This
        # implies that the documents it links to should _also_ not be
        # stored in a cache.
        MockFacets.max_cache_age = CachedFeed.IGNORE_CACHE
        url = manager.cdn_url_for("view", *args, **kwargs_with_facets)

        # And so, url_for is called instead of _cdn_url_for.
        assert "http://url/" == url
        assert [] == manager._cdn_url_for_calls
        assert ("view", args, kwargs) == manager.url_for_calls.pop()
        assert [] == manager.url_for_calls


class TestBaseController(CirculationControllerTest):

    def test_unscoped_session(self):

        """Compare to TestScopedSession.test_scoped_session to see
        how database sessions will be handled in production.
        """
        # Both requests used the self._db session used by most unit tests.
        with self.request_context_with_library("/"):
            response1 = self.manager.index_controller()
            assert self.app.manager._db == self._db

        with self.request_context_with_library("/"):
            response2 = self.manager.index_controller()
            assert self.app.manager._db == self._db

    def test_request_patron(self):
        # Test the method that finds the currently authenticated patron
        # for the current request, triggering the authentication process
        # if necessary.

        # If flask.request.patron is present, whatever value is in
        # there is returned.
        o1 = object()
        with self.app.test_request_context("/"):
            flask.request.patron = o1
            assert o1 == self.controller.request_patron

        # If not, authenticated_patron_from_request is called; it's
        # supposed to set flask.request.patron.
        o2 = object()
        def set_patron():
            flask.request.patron = o2
        mock = MagicMock(side_effect = set_patron,
                         return_value = "return value will be ignored")
        self.controller.authenticated_patron_from_request = mock
        with self.app.test_request_context("/"):
            assert o2 == self.controller.request_patron

    def test_authenticated_patron_from_request(self):
        # Test the method that attempts to authenticate a patron
        # for the current request.

        # First, test success.
        with self.request_context_with_library(
            "/", headers=dict(Authorization=self.valid_auth)
        ):
            result = self.controller.authenticated_patron_from_request()
            assert self.default_patron == result
            assert self.default_patron == flask.request.patron

        # No authorization header -> 401 error.
        with patch(
                'api.base_controller.BaseCirculationManagerController.authorization_header',
                  lambda x: None
        ):
            with self.request_context_with_library("/"):
                result = self.controller.authenticated_patron_from_request()
                assert 401 == result.status_code
                assert None == flask.request.patron

        # Exception contacting the authentication authority -> ProblemDetail
        def remote_failure(self, header):
            raise RemoteInitiatedServerError("argh", "service")
        with patch(
                'api.base_controller.BaseCirculationManagerController.authenticated_patron',
                remote_failure
        ):
            with self.request_context_with_library(
                "/", headers=dict(Authorization=self.valid_auth)
            ):
                result = self.controller.authenticated_patron_from_request()
                assert isinstance(result, ProblemDetail)
                assert REMOTE_INTEGRATION_FAILED.uri == result.uri
                assert "Error in authentication service" == result.detail
                assert None == flask.request.patron

        # Credentials provided but don't identify anyone in particular
        # -> 401 error.
        with patch(
                'api.base_controller.BaseCirculationManagerController.authenticated_patron',
                lambda self, x: None
        ):
            with self.request_context_with_library(
                "/", headers=dict(Authorization=self.valid_auth)
            ):
                result = self.controller.authenticated_patron_from_request()
                assert 401 == result.status_code
                assert None == flask.request.patron

    def test_authenticated_patron_invalid_credentials(self):
        from api.problem_details import INVALID_CREDENTIALS

        with self.request_context_with_library("/"):
            value = self.controller.authenticated_patron(
                dict(username="user1", password="password2")
            )
            assert value == INVALID_CREDENTIALS

    def test_authenticated_patron_can_authenticate_with_expired_credentials(self):
        """A patron can authenticate even if their credentials have
        expired -- they just can't create loans or holds.
        """
        one_year_ago = utc_now() - datetime.timedelta(days=365)
        with self.request_context_with_library("/"):
            patron = self.controller.authenticated_patron(
                self.valid_credentials
            )
            patron.expires = one_year_ago

            patron = self.controller.authenticated_patron(
                self.valid_credentials
            )
            assert one_year_ago == patron.expires

    def test_authenticated_patron_correct_credentials(self):
        with self.request_context_with_library("/"):
            value = self.controller.authenticated_patron(self.valid_credentials)
            assert isinstance(value, Patron)

            # The test neighborhood configured in the SimpleAuthenticationProvider
            # has been associated with the authenticated Patron object for the
            # duration of this request.
            assert "Unit Test West" == value.neighborhood

    def test_authentication_sends_proper_headers(self):

        # Make sure the realm header has quotes around the realm name.
        # Without quotes, some iOS versions don't recognize the header value.

        base_url = ConfigurationSetting.sitewide(self._db, Configuration.BASE_URL_KEY)
        base_url.value = 'http://url'

        with self.request_context_with_library("/"):
            response = self.controller.authenticate()
            assert response.headers['WWW-Authenticate'] == 'Basic realm="Library card"'

        with self.request_context_with_library("/", headers={"X-Requested-With": "XMLHttpRequest"}):
            response = self.controller.authenticate()
            assert None == response.headers.get("WWW-Authenticate")

    def test_handle_conditional_request(self):

        # First, test success: the client provides If-Modified-Since
        # and it is _not_ earlier than the 'last modified' date known by
        # the server.

        now_datetime = utc_now()
        now_string = email.utils.format_datetime(now_datetime)

        # To make the test more realistic, set a meaningless
        # microseconds value of 'now'.
        now_datetime = now_datetime.replace(microsecond=random.randint(0, 999999))

        with self.app.test_request_context(
            headers={"If-Modified-Since": now_string}
        ):
            response = self.controller.handle_conditional_request(now_datetime)
            assert 304 == response.status_code

        # Try with a few specific values that comply to a greater or lesser
        # extent with the date-format spec.
        very_old = datetime_utc(2000, 1, 1)
        for value in [
                "Thu, 01 Aug 2019 10:00:40 -0000",
                "Thu, 01 Aug 2019 10:00:40",
                "01 Aug 2019 10:00:40",
        ]:
            with self.app.test_request_context(
                    headers={"If-Modified-Since": value}
            ):
                response = self.controller.handle_conditional_request(very_old)
                assert 304 == response.status_code

        # All remaining test cases are failures: for whatever reason,
        # the request is not a valid conditional request and the
        # method returns None.

        with self.app.test_request_context(
            headers={"If-Modified-Since": now_string}
        ):
            # This request _would_ be a conditional request, but the
            # precondition fails: If-Modified-Since is earlier than
            # the 'last modified' date known by the server.
            newer = now_datetime + datetime.timedelta(seconds=10)
            response = self.controller.handle_conditional_request(newer)
            assert None == response

            # Here, the server doesn't know what the 'last modified' date is,
            # so it can't evaluate the precondition.
            response = self.controller.handle_conditional_request(None)
            assert None == response

        # Here, the precondition string is not parseable as a datetime.
        with self.app.test_request_context(
            headers={"If-Modified-Since": "01 Aug 2019"}
        ):
            response = self.controller.handle_conditional_request(very_old)
            assert None == response

        # Here, the client doesn't provide a precondition at all.
        with self.app.test_request_context():
            response = self.controller.handle_conditional_request(very_old)
            assert None == response

    def test_load_licensepools(self):

        # Here's a Library that has two Collections.
        library = self.library
        [c1] = library.collections
        c2 = self._collection()
        library.collections.append(c2)

        # Here's a Collection not affiliated with any Library.
        c3 = self._collection()

        # All three Collections have LicensePools for this Identifier,
        # from various sources.
        i1 = self._identifier()
        e1, lp1 = self._edition(
            data_source_name=DataSource.GUTENBERG,
            identifier_type=i1.type,
            identifier_id=i1.identifier,
            with_license_pool = True,
            collection=c1
        )
        e2, lp2 = self._edition(
            data_source_name=DataSource.OVERDRIVE,
            identifier_type=i1.type,
            identifier_id=i1.identifier,
            with_license_pool = True,
            collection=c2
        )
        e3, lp3 = self._edition(
            data_source_name=DataSource.BIBLIOTHECA,
            identifier_type=i1.type,
            identifier_id=i1.identifier,
            with_license_pool = True,
            collection=c3
        )

        # The first collection also has a LicensePool for a totally
        # different Identifier.
        e4, lp4 = self._edition(
            data_source_name=DataSource.GUTENBERG,
            with_license_pool=True,
            collection=c1
        )

        # Same for the third collection
        e5, lp5 = self._edition(
            data_source_name=DataSource.GUTENBERG,
            with_license_pool=True,
            collection=c3
        )

        # Now let's try to load LicensePools for the first Identifier
        # from the default Library.
        loaded = self.controller.load_licensepools(
            self._default_library, i1.type, i1.identifier
        )

        # Two LicensePools were loaded: the LicensePool for the first
        # Identifier in Collection 1, and the LicensePool for the same
        # identifier in Collection 2.
        assert lp1 in loaded
        assert lp2 in loaded
        assert 2 == len(loaded)
        assert all([lp.identifier==i1 for lp in loaded])

        # Note that the LicensePool in c3 was not loaded, even though
        # the Identifier matches, because that collection is not
        # associated with this Library.

        # LicensePool l4 was not loaded, even though it's in a Collection
        # that matches, because the Identifier doesn't match.

        # Now we test various failures.

        # Try a totally bogus identifier.
        problem_detail = self.controller.load_licensepools(
            self._default_library, "bad identifier type", i1.identifier
        )
        assert NO_LICENSES.uri == problem_detail.uri
        expect = "The item you're asking about (bad identifier type/%s) isn't in this collection." % i1.identifier
        assert expect == problem_detail.detail

        # Try an identifier that would work except that it's not in a
        # Collection associated with the given Library.
        problem_detail = self.controller.load_licensepools(
            self._default_library, lp5.identifier.type,
            lp5.identifier.identifier
        )
        assert NO_LICENSES.uri == problem_detail.uri

    def test_load_work(self):

        # Create a Work with two LicensePools.
        work = self._work(with_license_pool=True)
        [pool1] = work.license_pools
        pool2 = self._licensepool(None)
        pool2.work = work

        # Either identifier suffices to identify the Work.
        for i in [pool1.identifier, pool2.identifier]:
            with self.request_context_with_library("/"):
                assert (
                    work ==
                    self.controller.load_work(
                        self._default_library, i.type, i.identifier
                    ))

        # If a patron is authenticated, the requested Work must be
        # age-appropriate for that patron, or this method will return
        # a problem detail.
        headers = dict(Authorization=self.valid_auth)
        for retval, expect in ((True, work), (False, NOT_AGE_APPROPRIATE)):
            work.age_appropriate_for_patron = MagicMock(return_value = retval)
            with self.request_context_with_library("/", headers=headers):
                assert (
                    expect ==
                    self.controller.load_work(
                        self._default_library, pool1.identifier.type,
                        pool1.identifier.identifier
                    ))
                work.age_appropriate_for_patron.called_with(self.default_patron)

    def test_load_licensepooldelivery(self):

        licensepool = self._licensepool(edition=None, with_open_access_download=True)

        # Set a delivery mechanism that we won't be looking up, so we
        # can demonstrate that we find the right match thanks to more
        # than random chance.
        licensepool.set_delivery_mechanism(
            Representation.MOBI_MEDIA_TYPE, None, None, None
        )

        # If there is one matching delivery mechanism that matches the
        # request, we load it.
        lpdm = licensepool.delivery_mechanisms[0]
        delivery = self.controller.load_licensepooldelivery(
            licensepool, lpdm.delivery_mechanism.id
        )
        assert lpdm == delivery

        # If there are multiple matching delivery mechanisms (that is,
        # multiple ways of getting a book with the same media type and
        # DRM scheme) we pick one arbitrarily.
        new_lpdm, is_new = create(
            self._db,
            LicensePoolDeliveryMechanism,
            identifier=licensepool.identifier,
            data_source=licensepool.data_source,
            delivery_mechanism=lpdm.delivery_mechanism,
        )
        assert True == is_new

        assert new_lpdm.delivery_mechanism == lpdm.delivery_mechanism
        underlying_mechanism = lpdm.delivery_mechanism

        delivery = self.controller.load_licensepooldelivery(
            licensepool, lpdm.delivery_mechanism.id
        )

        # We don't know which LicensePoolDeliveryMechanism this is,
        # but we know it's one of the matches.
        assert underlying_mechanism == delivery.delivery_mechanism

        # If there is no matching delivery mechanism, we return a
        # problem detail.
        adobe_licensepool = self._licensepool(
            edition=None, with_open_access_download=False
        )
        problem_detail = self.controller.load_licensepooldelivery(
            adobe_licensepool, lpdm.delivery_mechanism.id
        )
        assert BAD_DELIVERY_MECHANISM.uri == problem_detail.uri

    def test_apply_borrowing_policy_succeeds_for_unlimited_access_books(self):
        with self.request_context_with_library("/"):
            # Arrange
            patron = self.controller.authenticated_patron(self.valid_credentials)
            work = self._work(
                with_license_pool=True,
                with_open_access_download=False
            )
            [pool] = work.license_pools
            pool.open_access = False
            pool.self_hosted = False
            pool.unlimited_access = True

            # Act
            problem = self.controller.apply_borrowing_policy(patron, pool)

            # Assert
            assert problem is None

    def test_apply_borrowing_policy_succeeds_for_self_hosted_books(self):
        with self.request_context_with_library("/"):
            # Arrange
            patron = self.controller.authenticated_patron(self.valid_credentials)
            work = self._work(
                with_license_pool=True,
                with_open_access_download=False
            )
            [pool] = work.license_pools
            pool.licenses_available = 0
            pool.licenses_owned = 0
            pool.open_access = False
            pool.self_hosted = True

            # Act
            problem = self.controller.apply_borrowing_policy(patron, pool)

            # Assert
            assert problem is None

    def test_apply_borrowing_policy_when_holds_prohibited(self):
        with self.request_context_with_library("/"):
            patron = self.controller.authenticated_patron(self.valid_credentials)
            # This library does not allow holds.
            library = self._default_library
            library.setting(library.ALLOW_HOLDS).value = "False"

            # This is an open-access work.
            work = self._work(with_license_pool=True,
                              with_open_access_download=True)
            [pool] = work.license_pools
            pool.licenses_available = 0
            assert True == pool.open_access

            # It can still be borrowed even though it has no
            # 'licenses' available.
            problem = self.controller.apply_borrowing_policy(patron, pool)
            assert None == problem

            # If it weren't an open-access work, there'd be a big
            # problem.
            pool.open_access = False
            problem = self.controller.apply_borrowing_policy(patron, pool)
            assert FORBIDDEN_BY_POLICY.uri == problem.uri

    def test_apply_borrowing_policy_for_age_inappropriate_book(self):
        # apply_borrowing_policy() prevents patrons from checking out
        # books that are not age-appropriate.

        # Set up lanes for different patron types.
        children_lane = self._lane()
        children_lane.audiences = [Classifier.AUDIENCE_CHILDREN,
                              Classifier.AUDIENCE_YOUNG_ADULT]
        children_lane.target_age = tuple_to_numericrange((9, 12))
        children_lane.root_for_patron_type = ["child"]

        adults_lane = self._lane()
        adults_lane.audiences = [Classifier.AUDIENCE_ADULT]
        adults_lane.root_for_patron_type = ["adult"]

        # This book is age-appropriate for anyone 13 years old or older.
        work = self._work(with_license_pool=True)
        work.audience = Classifier.AUDIENCE_CHILDREN
        work.target_age = tuple_to_numericrange((13,15))
        [pool] = work.license_pools

        with self.request_context_with_library("/"):
            patron = self.controller.authenticated_patron(
                self.valid_credentials
            )
            # This patron is restricted to a lane in which the 13-year-old
            # book would not appear.
            patron.external_type = "child"

            # Therefore the book is not age-appropriate for the patron.
            problem = self.controller.apply_borrowing_policy(patron, pool)
            assert FORBIDDEN_BY_POLICY.uri == problem.uri

            # If the lane is expanded to allow the book's age range, there's
            # no problem.
            children_lane.target_age = tuple_to_numericrange((9,13))
            assert None == self.controller.apply_borrowing_policy(patron, pool)

            # Similarly if the patron has an external type
            # corresponding to a root lane in which the given book
            # _is_ age-appropriate.
            children_lane.target_age = tuple_to_numericrange((9, 12))
            patron.external_type = "adult"
            assert None == self.controller.apply_borrowing_policy(patron, pool)

    def test_library_for_request(self):
        with self.app.test_request_context("/"):
            value = self.controller.library_for_request("not-a-library")
            assert LIBRARY_NOT_FOUND == value

        with self.app.test_request_context("/"):
            value = self.controller.library_for_request(self._default_library.short_name)
            assert self._default_library == value
            assert self._default_library == flask.request.library

        # If you don't specify a library, the default library is used.
        with self.app.test_request_context("/"):
            value = self.controller.library_for_request(None)
            expect_default = Library.default(self._db)
            assert expect_default == value
            assert expect_default == flask.request.library

    def test_library_for_request_reloads_settings_if_necessary(self):

        # We're about to change the shortname of the default library.
        new_name = "newname" + self._str

        # Before we make the change, a request to the library's new name
        # will fail.
        assert new_name not in self.manager.auth.library_authenticators
        with self.app.test_request_context("/"):
            problem = self.controller.library_for_request(new_name)
            assert LIBRARY_NOT_FOUND == problem


        # Make the change.
        self._default_library.short_name = new_name
        self._db.commit()

        # Bypass the 1-second cooldown and make sure the site knows
        # the configuration has actually changed.
        model.site_configuration_has_changed(self._db, cooldown=0)

        # Just making the change and calling
        # site_configuration_has_changed was not enough to update the
        # CirculationManager's settings.
        assert new_name not in self.manager.auth.library_authenticators

        # But the first time we make a request that calls the library
        # by its new name, those settings are reloaded.
        with self.app.test_request_context("/"):
            value = self.controller.library_for_request(new_name)
            assert self._default_library == value

            # An assertion that would have failed before works now.
            assert new_name in self.manager.auth.library_authenticators

    def test_load_lane(self):
        # Verify that requests for specific lanes are mapped to
        # the appropriate lane.

        # TODO: The case where the top-level lane is a WorkList rather
        # than a Lane is not tested.

        lanes = self._default_library.lanes

        with self.request_context_with_library("/"):
            top_level = self.controller.load_lane(None)
            expect = self.controller.manager.top_level_lanes[
                self._default_library.id
            ]

            # expect and top_level are different ORM objects
            # representing the same lane. (They're different objects
            # because the lane stored across requests inside the
            # CirculationManager object was merged into the request's
            # database session.)
            assert isinstance(top_level, Lane)
            assert expect.id == top_level.id

            # A lane can be looked up by ID.
            for l in lanes:
                found = self.controller.load_lane(l.id)
                assert l == found

            # If a lane cannot be looked up by ID, a problem detail
            # is returned.
            for bad_id in ('nosuchlane', -1):
                not_found = self.controller.load_lane(bad_id)
                assert isinstance(not_found, ProblemDetail)
                assert not_found.uri == NO_SUCH_LANE.uri
                assert (
                    "Lane %s does not exist or is not associated with library %s" % (
                        bad_id, self._default_library.id
                    ) ==
                    not_found.detail)

        # If the requested lane exists but is not visible to the
        # authenticated patron, the server _acts_ like the lane does
        # not exist.

        # Any lane will do here.
        lane = lanes[0]

        # Mock Lane.accessible_to so that it always returns
        # false.
        lane.accessible_to = MagicMock(return_value=False)
        headers = dict(Authorization=self.valid_auth)
        with self.request_context_with_library(
            "/", headers=headers, library=self._default_library
        ):
            # The lane exists, but visible_to says it's not
            # visible to the authenticated patron, so the controller
            # denies it exists.
            result = self.controller.load_lane(lane.id)
            assert isinstance(result, ProblemDetail)
            assert result.uri == NO_SUCH_LANE.uri
            lane.accessible_to.assert_called_once_with(self.default_patron)


class TestIndexController(CirculationControllerTest):

    def test_simple_redirect(self):
        with self.app.test_request_context('/'):
            flask.request.library = self.library
            response = self.manager.index_controller()
            assert 302 == response.status_code
            assert "http://cdn/default/groups/" == response.headers['location']

    def test_custom_index_view(self):
        """If a custom index view is registered for a library,
        it is called instead of the normal IndexController code.
        """
        class MockCustomIndexView(object):
            def __call__(self, library, annotator):
                self.called_with = (library, annotator)
                return "fake response"

        # Set up our MockCustomIndexView as the custom index for
        # the default library.
        mock = MockCustomIndexView()
        self.manager.custom_index_views[self._default_library.id] = mock

        # Mock CirculationManager.annotator so it's easy to check
        # that it was called.
        mock_annotator = object()
        def make_mock_annotator(lane):
            assert lane == None
            return mock_annotator
        self.manager.annotator = make_mock_annotator

        # Make a request, and the custom index is invoked.
        with self.request_context_with_library(
            "/", headers=dict(Authorization=self.invalid_auth)):
            response = self.manager.index_controller()
        assert "fake response" == response

        # The custom index was invoked with the library associated
        # with the request + the output of self.manager.annotator()
        library, annotator = mock.called_with
        assert self._default_library == library
        assert mock_annotator == annotator

    def test_authenticated_patron_root_lane(self):
        root_1, root_2 = self._db.query(Lane).all()[:2]

        # Patrons of external type '1' and '2' have a certain root lane.
        root_1.root_for_patron_type = ["1", "2"]

        # Patrons of external type '3' have a different root.
        root_2.root_for_patron_type = ["3"]

        self.default_patron.external_type = "1"
        with self.request_context_with_library(
            "/", headers=dict(Authorization=self.invalid_auth)):
            response = self.manager.index_controller()
            assert 401 == response.status_code

        with self.request_context_with_library(
            "/", headers=dict(Authorization=self.valid_auth)):
            response = self.manager.index_controller()
            assert 302 == response.status_code
            assert ("http://cdn/default/groups/%s" % root_1.id ==
                response.headers['location'])

        self.default_patron.external_type = "2"
        with self.request_context_with_library(
            "/", headers=dict(Authorization=self.valid_auth)):
            response = self.manager.index_controller()
            assert 302 == response.status_code
            assert "http://cdn/default/groups/%s" % root_1.id == response.headers['location']

        self.default_patron.external_type = "3"
        with self.request_context_with_library(
            "/", headers=dict(Authorization=self.valid_auth)):
            response = self.manager.index_controller()
            assert 302 == response.status_code
            assert "http://cdn/default/groups/%s" % root_2.id == response.headers['location']

        # Patrons with a different type get sent to the top-level lane.
        self.default_patron.external_type = '4'
        with self.request_context_with_library(
            "/", headers=dict(Authorization=self.valid_auth)):
            response = self.manager.index_controller()
            assert 302 == response.status_code
            assert "http://cdn/default/groups/" == response.headers['location']

        # Patrons with no type get sent to the top-level lane.
        self.default_patron.external_type = None
        with self.request_context_with_library(
            "/", headers=dict(Authorization=self.valid_auth)):
            response = self.manager.index_controller()
            assert 302 == response.status_code
            assert "http://cdn/default/groups/" == response.headers['location']

    def test_authentication_document(self):
        # Test the ability to retrieve an Authentication For OPDS document.
        library_name = self.library.short_name
        with self.request_context_with_library(
                "/", headers=dict(Authorization=self.invalid_auth)):
            response = self.manager.index_controller.authentication_document()
            assert 200 == response.status_code
            assert AuthenticationForOPDSDocument.MEDIA_TYPE == response.headers['Content-Type']
            data = response.get_data(as_text=True)
            assert self.manager.auth.create_authentication_document() == data

            # Make sure we got the A4OPDS document for the right library.
            doc = json.loads(data)
            assert library_name == doc['title']

        # Currently, the authentication document cache is disabled by default.
        self.manager.authentication_for_opds_documents[library_name] = "Cached value"
        with self.request_context_with_library(
                "/", headers=dict(Authorization=self.invalid_auth)):
            response = self.manager.index_controller.authentication_document()
            assert "Cached value" != response.get_data(as_text=True)

        # Enable the A4OPDS document cache and verify that it's working.
        self.manager.authentication_for_opds_documents.max_age = 3600
        cached_value = json.dumps(dict(key="Cached document"))
        self.manager.authentication_for_opds_documents[library_name] = cached_value
        with self.request_context_with_library(
                "/?debug", headers=dict(Authorization=self.invalid_auth)):
            response = self.manager.index_controller.authentication_document()
            assert cached_value == response.get_data(as_text=True)

            # Note that WSGI debugging data was not provided, even
            # though we requested it, since WSGI debugging is
            # disabled.
            assert '_debug' not in response.get_data(as_text=True)

        # When WSGI debugging is enabled and requested, an
        # authentication document includes some extra information in a
        # special '_debug' section.
        self.manager.wsgi_debug = True
        with self.request_context_with_library(
                "/?debug", headers=dict(Authorization=self.invalid_auth)):
            response = self.manager.index_controller.authentication_document()
            doc = json.loads(response.data)
            assert doc['key'] == 'Cached document'
            debug = doc['_debug']
            assert all(x in debug for x in ('url', 'cache', 'environ'))

        # WSGI debugging is not provided unless requested.
        with self.request_context_with_library(
                "/", headers=dict(Authorization=self.invalid_auth)):
            response = self.manager.index_controller.authentication_document()
            assert '_debug' not in response.get_data(as_text=True)

    def test_public_key_integration_document(self):
        base_url = ConfigurationSetting.sitewide(self._db, Configuration.BASE_URL_KEY).value

        # When a sitewide key pair exists (which should be all the
        # time), all of its data is included.
        key_setting = ConfigurationSetting.sitewide(
            self._db, Configuration.KEY_PAIR
        )
        key_setting.value = json.dumps(['public key', 'private key'])
        with self.app.test_request_context('/'):
            response = self.manager.index_controller.public_key_document()

        assert 200 == response.status_code
        assert 'application/opds+json' == response.headers.get('Content-Type')

        data = json.loads(response.get_data(as_text=True))
        assert 'RSA' == data.get('public_key', {}).get('type')
        assert 'public key' == data.get('public_key', {}).get('value')

        # If there is no sitewide key pair (which should never
        # happen), a new one is created. Library-specific public keys
        # are ignored.
        key_setting.value = None
        ConfigurationSetting.for_library(
            Configuration.KEY_PAIR, self.library
        ).value = 'ignore me'

        with self.app.test_request_context('/'):
            response = self.manager.index_controller.public_key_document()

        assert 200 == response.status_code
        assert 'application/opds+json' == response.headers.get('Content-Type')

        data = json.loads(response.get_data(as_text=True))
        assert 'http://test-circulation-manager/' == data.get('id')
        key = data.get('public_key')
        assert 'RSA' == key['type']
        assert 'BEGIN PUBLIC KEY' in key['value']

class TestMultipleLibraries(CirculationControllerTest):

    def make_default_libraries(self, _db):
        return [self._library() for x in range(2)]

    def make_default_collection(self, _db, library):
        collection, ignore = get_one_or_create(
            _db, Collection, name=self._str + " (for multi-library test)",
        )
        collection.create_external_integration(ExternalIntegration.OPDS_IMPORT)
        library.collections.append(collection)
        return collection

    def test_authentication(self):
        """It's possible to authenticate with multiple libraries and make a
        request that runs in the context of each different library.
        """
        l1, l2 = self.libraries
        assert l1 != l2
        for library in self.libraries:
            headers = dict(Authorization=self.valid_auth)
            with self.request_context_with_library(
                    "/", headers=headers, library=library):
                patron = self.manager.loans.authenticated_patron_from_request()
                assert library == patron.library
                response = self.manager.index_controller()
                assert ("http://cdn/%s/groups/" % library.short_name ==
                    response.headers['location'])

class TestLoanController(CirculationControllerTest):
    def setup_method(self):
        super(TestLoanController, self).setup_method()
        self.pool = self.english_1.license_pools[0]
        [self.mech1] = self.pool.delivery_mechanisms
        self.mech2 = self.pool.set_delivery_mechanism(
            Representation.PDF_MEDIA_TYPE, DeliveryMechanism.NO_DRM,
            RightsStatus.CC_BY, None
        )
        self.edition = self.pool.presentation_edition
        self.data_source = self.edition.data_source
        self.identifier = self.edition.primary_identifier

    def test_can_fulfill_without_loan(self):
        """Test the circumstances under which a title can be fulfilled
        in the absence of an active loan for that title.
        """
        m = self.manager.loans.can_fulfill_without_loan

        # If the library has a way of authenticating patrons (as the
        # default library does), then fulfilling a title always
        # requires an active loan.
        patron = object()
        pool = object()
        lpdm = object()
        assert False == m(self._default_library, patron, pool, lpdm)

        # If the library does not authenticate patrons, then this
        # _may_ be possible, but
        # CirculationAPI.can_fulfill_without_loan also has to say it's
        # okay.
        class MockLibraryAuthenticator(object):
            identifies_individuals = False
        self.manager.auth.library_authenticators[
            self._default_library.short_name
        ] = MockLibraryAuthenticator()
        def mock_can_fulfill_without_loan(patron, pool, lpdm):
            self.called_with = (patron, pool, lpdm)
            return True
        with self.request_context_with_library("/"):
            self.manager.loans.circulation.can_fulfill_without_loan = (
                mock_can_fulfill_without_loan
            )
            assert True == m(self._default_library, patron, pool, lpdm)
            assert (patron, pool, lpdm) == self.called_with

    def test_patron_circulation_retrieval(self):
        """The controller can get loans and holds for a patron, even if
        there are multiple licensepools on the Work.
        """
        # Give the Work a second LicensePool.
        edition, other_pool = self._edition(
            with_open_access_download=True, with_license_pool=True,
            data_source_name=DataSource.BIBLIOTHECA,
            collection=self.pool.collection
        )
        other_pool.identifier = self.identifier
        other_pool.work = self.pool.work

        pools = self.manager.loans.load_licensepools(
            self.library, self.identifier.type, self.identifier.identifier
        )

        with self.request_context_with_library(
                "/", headers=dict(Authorization=self.valid_auth)):
            self.manager.loans.authenticated_patron_from_request()

            # Without a loan or a hold, nothing is returned.
            # No loans.
            result = self.manager.loans.get_patron_loan(
                self.default_patron, pools
            )
            assert (None, None) == result

            # No holds.
            result = self.manager.loans.get_patron_hold(
                self.default_patron, pools
            )
            assert (None, None) == result

            # When there's a loan, we retrieve it.
            loan, newly_created = self.pool.loan_to(self.default_patron)
            result = self.manager.loans.get_patron_loan(
                self.default_patron, pools
            )
            assert (loan, self.pool) == result

            # When there's a hold, we retrieve it.
            hold, newly_created = other_pool.on_hold_to(self.default_patron)
            result = self.manager.loans.get_patron_hold(
                self.default_patron, pools
            )
            assert (hold, other_pool) == result

    def test_borrow_success(self):
        with self.request_context_with_library(
                "/", headers=dict(Authorization=self.valid_auth)):
            self.manager.loans.authenticated_patron_from_request()
            response = self.manager.loans.borrow(
                self.identifier.type, self.identifier.identifier)

            # A loan has been created for this license pool.
            loan = get_one(self._db, Loan, license_pool=self.pool)
            assert loan != None
            # The loan has yet to be fulfilled.
            assert None == loan.fulfillment

            # We've been given an OPDS feed with one entry, which tells us how
            # to fulfill the license.
            assert 201 == response.status_code
            feed = feedparser.parse(response.get_data())
            [entry] = feed['entries']
            fulfillment_links = [x['href'] for x in entry['links']
                                if x['rel'] == OPDSFeed.ACQUISITION_REL]

            assert self.mech1.resource is not None

            # Make sure the two delivery mechanisms are incompatible.
            self.mech1.delivery_mechanism.drm_scheme = "DRM Scheme 1"
            self.mech2.delivery_mechanism.drm_scheme = "DRM Scheme 2"
            fulfillable_mechanism = self.mech1
            self._db.commit()

            expects = [url_for('fulfill',
                               license_pool_id=self.pool.id,
                               mechanism_id=mech.delivery_mechanism.id,
                               library_short_name=self.library.short_name,
                               _external=True) for mech in [self.mech1, self.mech2]]
            assert set(expects) == set(fulfillment_links)

            # Make sure the first delivery mechanism has the data necessary
            # to carry out an open source fulfillment.
            assert self.mech1.resource is not None
            assert self.mech1.resource.representation is not None
            assert self.mech1.resource.representation.url is not None

            # Now let's try to fulfill the loan using the first delivery mechanism.
            response = self.manager.loans.fulfill(
                self.pool.id, fulfillable_mechanism.delivery_mechanism.id,
            )
            if isinstance(response, ProblemDetail):
                j, status, headers = response.response
                raise Exception(repr(j))
            assert 302 == response.status_code
            assert fulfillable_mechanism.resource.representation.public_url == response.headers.get("Location")

            # The mechanism we used has been registered with the loan.
            assert fulfillable_mechanism == loan.fulfillment

            # Set the pool to be non-open-access, so we have to make an
            # external request to obtain the book.
            self.pool.open_access = False

            http = DummyHTTPClient()

            fulfillment = FulfillmentInfo(
                self.pool.collection,
                self.pool.data_source,
                self.pool.identifier.type,
                self.pool.identifier.identifier,
                content_link=fulfillable_mechanism.resource.url,
                content_type=fulfillable_mechanism.resource.representation.media_type,
                content=None,
                content_expires=None)

            # Now that we've set a mechanism, we can fulfill the loan
            # again without specifying a mechanism.
            self.manager.d_circulation.queue_fulfill(self.pool, fulfillment)
            http.queue_response(200, content="I am an ACSM file")

            response = self.manager.loans.fulfill(
                self.pool.id, do_get=http.do_get
            )
            assert 200 == response.status_code
            assert "I am an ACSM file" == response.get_data(as_text=True)
            assert http.requests == [fulfillable_mechanism.resource.url]

            # But we can't use some other mechanism -- we're stuck with
            # the first one we chose.
            response = self.manager.loans.fulfill(
                self.pool.id, self.mech2.delivery_mechanism.id
            )

            assert 409 == response.status_code
            assert "You already fulfilled this loan as application/epub+zip (DRM Scheme 1), you can't also do it as application/pdf (DRM Scheme 2)" in response.detail

            # If the remote server fails, we get a problem detail.
            def doomed_get(url, headers, **kwargs):
                raise RemoteIntegrationException("fulfill service", "Error!")
            self.manager.d_circulation.queue_fulfill(self.pool, fulfillment)

            response = self.manager.loans.fulfill(
                self.pool.id, do_get=doomed_get
            )
            assert isinstance(response, ProblemDetail)
            assert 502 == response.status_code

    def test_borrow_and_fulfill_with_streaming_delivery_mechanism(self):
        # Create a pool with a streaming delivery mechanism
        work = self._work(with_license_pool=True, with_open_access_download=False)
        edition = work.presentation_edition
        pool = work.license_pools[0]
        pool.open_access = False
        streaming_mech = pool.set_delivery_mechanism(
            DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE, DeliveryMechanism.OVERDRIVE_DRM,
            RightsStatus.IN_COPYRIGHT, None
        )
        identifier = edition.primary_identifier

        with self.request_context_with_library(
                "/", headers=dict(Authorization=self.valid_auth)):
            self.manager.loans.authenticated_patron_from_request()
            self.manager.d_circulation.queue_checkout(
                pool,
                LoanInfo(
                    pool.collection, pool.data_source.name,
                    pool.identifier.type,
                    pool.identifier.identifier,
                    utc_now(),
                    utc_now() + datetime.timedelta(seconds=3600),
                )
            )
            response = self.manager.loans.borrow(
                identifier.type, identifier.identifier)

            # A loan has been created for this license pool.
            loan = get_one(self._db, Loan, license_pool=pool)
            assert loan != None
            # The loan has yet to be fulfilled.
            assert None == loan.fulfillment

            # We've been given an OPDS feed with two delivery mechanisms, which tell us how
            # to fulfill the license.
            assert 201 == response.status_code
            feed = feedparser.parse(response.get_data())
            [entry] = feed['entries']
            fulfillment_links = [x['href'] for x in entry['links']
                                if x['rel'] == OPDSFeed.ACQUISITION_REL]
            [mech1, mech2] = sorted(
                pool.delivery_mechanisms,
                key=lambda x: x.delivery_mechanism.is_streaming
            )

            streaming_mechanism = mech2

            expects = [url_for('fulfill',
                               license_pool_id=pool.id,
                               mechanism_id=mech.delivery_mechanism.id,
                               library_short_name=self.library.short_name,
                               _external=True) for mech in [mech1, mech2]]
            assert set(expects) == set(fulfillment_links)

            # Now let's try to fulfill the loan using the streaming mechanism.
            self.manager.d_circulation.queue_fulfill(
                pool,
                FulfillmentInfo(
                    pool.collection, pool.data_source.name,
                    pool.identifier.type,
                    pool.identifier.identifier,
                    "http://streaming-content-link",
                    Representation.TEXT_HTML_MEDIA_TYPE + DeliveryMechanism.STREAMING_PROFILE,
                    None,
                    None,
                )
            )
            response = self.manager.loans.fulfill(
                pool.id, streaming_mechanism.delivery_mechanism.id
            )

            # We get an OPDS entry.
            assert 200 == response.status_code
            opds_entries = feedparser.parse(response.response[0])['entries']
            assert 1 == len(opds_entries)
            links = opds_entries[0]['links']

            # The entry includes one fulfill link.
            fulfill_links = [link for link in links if link['rel'] == "http://opds-spec.org/acquisition"]
            assert 1 == len(fulfill_links)

            assert (Representation.TEXT_HTML_MEDIA_TYPE + DeliveryMechanism.STREAMING_PROFILE ==
                fulfill_links[0]['type'])
            assert "http://streaming-content-link" == fulfill_links[0]['href']


            # The mechanism has not been set, since fulfilling a streaming
            # mechanism does not lock in the format.
            assert None == loan.fulfillment

            # We can still use the other mechanism too.
            http = DummyHTTPClient()
            http.queue_response(200, content="I am an ACSM file")

            self.manager.d_circulation.queue_fulfill(
                pool,
                FulfillmentInfo(
                    pool.collection, pool.data_source.name,
                    pool.identifier.type,
                    pool.identifier.identifier,
                    "http://other-content-link",
                    Representation.TEXT_HTML_MEDIA_TYPE,
                    None,
                    None,
                ),
            )
            response = self.manager.loans.fulfill(
                pool.id, mech1.delivery_mechanism.id, do_get=http.do_get
            )
            assert 200 == response.status_code

            # Now the fulfillment has been set to the other mechanism.
            assert mech1 == loan.fulfillment

            # But we can still fulfill the streaming mechanism again.
            self.manager.d_circulation.queue_fulfill(
                pool,
                FulfillmentInfo(
                    pool.collection, pool.data_source.name,
                    pool.identifier.type,
                    pool.identifier.identifier,
                    "http://streaming-content-link",
                    Representation.TEXT_HTML_MEDIA_TYPE + DeliveryMechanism.STREAMING_PROFILE,
                    None,
                    None,
                )
            )

            response = self.manager.loans.fulfill(
                pool.id, streaming_mechanism.delivery_mechanism.id
            )
            assert 200 == response.status_code
            opds_entries = feedparser.parse(response.response[0])['entries']
            assert 1 == len(opds_entries)
            links = opds_entries[0]['links']

            fulfill_links = [link for link in links if link['rel'] == "http://opds-spec.org/acquisition"]
            assert 1 == len(fulfill_links)

            assert (Representation.TEXT_HTML_MEDIA_TYPE + DeliveryMechanism.STREAMING_PROFILE ==
                fulfill_links[0]['type'])
            assert "http://streaming-content-link" == fulfill_links[0]['href']

    def test_borrow_nonexistent_delivery_mechanism(self):
        with self.request_context_with_library(
                "/", headers=dict(Authorization=self.valid_auth)):
            self.manager.loans.authenticated_patron_from_request()
            response = self.manager.loans.borrow(
                self.identifier.type, self.identifier.identifier,
                -100
            )
            assert BAD_DELIVERY_MECHANISM == response

    def test_borrow_creates_hold_when_no_available_copies(self):
        threem_edition, pool = self._edition(
            with_open_access_download=False,
            data_source_name=DataSource.THREEM,
            identifier_type=Identifier.THREEM_ID,
            with_license_pool=True,
        )
        threem_book = self._work(
            presentation_edition=threem_edition,
        )
        pool.licenses_available = 0
        pool.open_access = False

        with self.request_context_with_library(
                "/", headers=dict(Authorization=self.valid_auth)):
            self.manager.loans.authenticated_patron_from_request()
            self.manager.d_circulation.queue_checkout(
                pool, NoAvailableCopies()
            )
            self.manager.d_circulation.queue_hold(
                pool,
                HoldInfo(
                    pool.collection, pool.data_source.name,
                    pool.identifier.type,
                    pool.identifier.identifier,
                    utc_now(),
                    utc_now() + datetime.timedelta(seconds=3600),
                    1,
                )
            )
            response = self.manager.loans.borrow(
                pool.identifier.type, pool.identifier.identifier)
            assert 201 == response.status_code

            # A hold has been created for this license pool.
            hold = get_one(self._db, Hold, license_pool=pool)
            assert hold != None

    def test_borrow_nolicenses(self):
        edition, pool = self._edition(
            with_open_access_download=False,
            data_source_name=DataSource.GUTENBERG,
            identifier_type=Identifier.GUTENBERG_ID,
            with_license_pool=True,
        )

        with self.request_context_with_library(
                "/", headers=dict(Authorization=self.valid_auth)):
            self.manager.loans.authenticated_patron_from_request()
            self.manager.d_circulation.queue_checkout(pool, NoLicenses())

            response = self.manager.loans.borrow(
                pool.identifier.type, pool.identifier.identifier)
            assert 404 == response.status_code
            assert NOT_FOUND_ON_REMOTE == response

    def test_borrow_creates_local_hold_if_remote_hold_exists(self):
        """We try to check out a book, but turns out we already have it
        on hold.
        """
        threem_edition, pool = self._edition(
            with_open_access_download=False,
            data_source_name=DataSource.THREEM,
            identifier_type=Identifier.THREEM_ID,
            with_license_pool=True,
        )
        threem_book = self._work(
            presentation_edition=threem_edition,
        )
        pool.licenses_available = 0
        pool.open_access = False

        with self.request_context_with_library(
                "/", headers=dict(Authorization=self.valid_auth)):
            self.manager.loans.authenticated_patron_from_request()
            self.manager.d_circulation.queue_checkout(
                pool, AlreadyOnHold()
            )
            self.manager.d_circulation.queue_hold(
                pool, HoldInfo(
                    pool.collection, pool.data_source.name,
                    pool.identifier.type,
                    pool.identifier.identifier,
                    utc_now(),
                    utc_now() + datetime.timedelta(seconds=3600),
                    1,
                )
            )
            response = self.manager.loans.borrow(
                pool.identifier.type, pool.identifier.identifier)
            assert 201 == response.status_code

            # A hold has been created for this license pool.
            hold = get_one(self._db, Hold, license_pool=pool)
            assert hold != None

    def test_borrow_fails_when_work_not_present_on_remote(self):
         threem_edition, pool = self._edition(
             with_open_access_download=False,
             data_source_name=DataSource.THREEM,
             identifier_type=Identifier.THREEM_ID,
             with_license_pool=True,
         )
         threem_book = self._work(
             presentation_edition=threem_edition,
         )
         pool.licenses_available = 1
         pool.open_access = False

         with self.request_context_with_library(
                 "/", headers=dict(Authorization=self.valid_auth)):
             self.manager.loans.authenticated_patron_from_request()
             self.manager.d_circulation.queue_checkout(
                 pool, NotFoundOnRemote()
             )
             response = self.manager.loans.borrow(
                 pool.identifier.type, pool.identifier.identifier)
             assert 404 == response.status_code
             assert "http://librarysimplified.org/terms/problem/not-found-on-remote" == response.uri

    def test_borrow_succeeds_when_work_already_checked_out(self):
        # An attempt to borrow a book that's already on loan is
        # treated as success without even going to the remote API.
        loan, _ignore = get_one_or_create(
            self._db, Loan, license_pool=self.pool,
            patron=self.default_patron
        )


        with self.request_context_with_library(
                "/", headers=dict(Authorization=self.valid_auth)):
            self.manager.loans.authenticated_patron_from_request()

            # Set it up that going to the remote API would raise an
            # exception, to prove we're not going to do that.
            circulation = self.manager.d_circulation
            circulation.queue_checkout(loan.license_pool, NotFoundOnRemote())

            mock_remote = circulation.api_for_license_pool(loan.license_pool)
            assert 1 == len(mock_remote.responses['checkout'])
            response = self.manager.loans.borrow(
                self.identifier.type, self.identifier.identifier)

            # No checkout request was actually made to the remote.
            assert 1 == len(mock_remote.responses['checkout'])

            # We got an OPDS entry that includes at least one
            # fulfillment link, which is what we expect when we ask
            # about an active loan.
            assert 200 == response.status_code
            [entry] = feedparser.parse(response.response[0])['entries']
            assert any([x for x in entry['links'] if x['rel'] == 'http://opds-spec.org/acquisition'])

    def test_fulfill(self):
        # Verify that arguments to the fulfill() method are propagated
        # correctly to the CirculationAPI.
        class MockCirculationAPI(object):
            def fulfill(self, patron, credential, requested_license_pool,
                        mechanism, part, fulfill_part_url):
                self.called_with = (
                    patron, credential, requested_license_pool,
                    mechanism, part, fulfill_part_url
                )
                raise CannotFulfill()

        controller = self.manager.loans
        mock = MockCirculationAPI()
        library_short_name = self._default_library.short_name
        controller.manager.circulation_apis[self._default_library.id] = mock

        with self.request_context_with_library(
            "/", headers=dict(Authorization=self.valid_auth)
        ):
            authenticated = controller.authenticated_patron_from_request()
            loan, ignore = self.pool.loan_to(authenticated)

            # Try to fulfill a certain part of the loan.
            part = "part 1 million"
            controller.fulfill(
                self.pool.id, self.mech2.delivery_mechanism.id, part
            )

            # Verify that the right arguments were passed into
            # CirculationAPI.
            (patron, credential, pool, mechanism, part,
             fulfill_part_url) = mock.called_with
            assert authenticated == patron
            assert self.valid_credentials['password'] == credential
            assert self.pool == pool
            assert self.mech2 == mechanism
            assert "part 1 million" == part

            # The last argument is complicated -- it's a function for
            # generating partial fulfillment URLs. Let's try it out
            # and make sure it gives the result we expect.
            expect = url_for(
                "fulfill", license_pool_id=self.pool.id,
                mechanism_id=mechanism.delivery_mechanism.id,
                library_short_name=library_short_name,
                part=part, _external=True
            )
            part_url = fulfill_part_url(part)
            assert expect == part_url

            # Ensure that the library short name is the first segment
            # of the path of the fulfillment url. We cannot perform
            # patron authentication without it.
            expected_path = urllib.parse.urlparse(expect).path
            part_url_path = urllib.parse.urlparse(part_url).path
            assert expected_path.startswith("/{}/".format(library_short_name))
            assert part_url_path.startswith("/{}/".format(library_short_name))

    def test_fulfill_returns_fulfillment_info_implementing_as_response(self):
        # If CirculationAPI.fulfill returns a FulfillmentInfo that
        # defines as_response, the result of as_response is returned
        # directly and the normal process of converting a FulfillmentInfo
        # to a Flask response is skipped.
        class MockFulfillmentInfo(FulfillmentInfo):
            @property
            def as_response(self):
                return "Here's your response"

        class MockCirculationAPI(object):
            def fulfill(slf, *args, **kwargs):
                return MockFulfillmentInfo(
                    self._default_collection, None, None, None, None,
                    None, None, None
                )

        controller = self.manager.loans
        mock = MockCirculationAPI()
        controller.manager.circulation_apis[self._default_library.id] = mock

        with self.request_context_with_library(
            "/", headers=dict(Authorization=self.valid_auth)
        ):
            authenticated = controller.authenticated_patron_from_request()
            loan, ignore = self.pool.loan_to(authenticated)

            # Fulfill the loan.
            result = controller.fulfill(
                self.pool.id, self.mech2.delivery_mechanism.id
            )

            # The result of MockFulfillmentInfo.as_response was
            # returned directly.
            assert "Here's your response" == result

    def test_fulfill_without_active_loan(self):

        controller = self.manager.loans

        # Most of the time, it is not possible to fulfill a title if the
        # patron has no active loan for the title. This might be
        # because the patron never checked out the book...
        with self.request_context_with_library(
                "/", headers=dict(Authorization=self.valid_auth)):
            controller.authenticated_patron_from_request()
            response = controller.fulfill(
                self.pool.id, self.mech2.delivery_mechanism.id
            )

            assert NO_ACTIVE_LOAN.uri == response.uri

        # ...or it might be because there is no authenticated patron.
        with self.request_context_with_library("/"):
            response = controller.fulfill(
                self.pool.id, self.mech2.delivery_mechanism.id
            )
            assert isinstance(response, FlaskResponse)
            assert 401 == response.status_code

        # ...or it might be because of an error communicating
        # with the authentication provider.
        old_authenticated_patron = controller.authenticated_patron_from_request
        def mock_authenticated_patron():
            return INTEGRATION_ERROR
        controller.authenticated_patron_from_request = mock_authenticated_patron
        with self.request_context_with_library("/"):
            problem = controller.fulfill(
                self.pool.id, self.mech2.delivery_mechanism.id
            )
            assert INTEGRATION_ERROR == problem
        controller.authenticated_patron_from_request = old_authenticated_patron

        # However, if can_fulfill_without_loan returns True, then
        # fulfill() will be called. If fulfill() returns a
        # FulfillmentInfo, then the title is fulfilled, with no loan
        # having been created.
        #
        # To that end, we'll mock can_fulfill_without_loan and fulfill.
        def mock_can_fulfill_without_loan(*args, **kwargs):
            return True

        def mock_fulfill(*args, **kwargs):
            return FulfillmentInfo(
                self.collection,
                self.pool.data_source.name,
                self.pool.identifier.type,
                self.pool.identifier.identifier,
                None, "text/html", "here's your book",
                utc_now(),
            )

        # Now we're able to fulfill the book even without
        # authenticating a patron.
        with self.request_context_with_library("/"):
            controller.can_fulfill_without_loan = mock_can_fulfill_without_loan
            controller.circulation.fulfill = mock_fulfill
            response = controller.fulfill(
                self.pool.id, self.mech2.delivery_mechanism.id
            )

            assert "here's your book" == response.get_data(as_text=True)
            assert [] == self._db.query(Loan).all()

    def test_revoke_loan(self):
         with self.request_context_with_library(
                 "/", headers=dict(Authorization=self.valid_auth)):
             patron = self.manager.loans.authenticated_patron_from_request()
             loan, newly_created = self.pool.loan_to(patron)

             self.manager.d_circulation.queue_checkin(self.pool, True)

             response = self.manager.loans.revoke(self.pool.id)

             assert 200 == response.status_code

    def test_revoke_hold(self):
         with self.request_context_with_library(
                 "/", headers=dict(Authorization=self.valid_auth)):
             patron = self.manager.loans.authenticated_patron_from_request()
             hold, newly_created = self.pool.on_hold_to(patron, position=0)

             self.manager.d_circulation.queue_release_hold(self.pool, True)

             response = self.manager.loans.revoke(self.pool.id)

             assert 200 == response.status_code

    def test_revoke_hold_nonexistent_licensepool(self):
         with self.request_context_with_library(
                 "/", headers=dict(Authorization=self.valid_auth)):
            patron = self.manager.loans.authenticated_patron_from_request()
            response = self.manager.loans.revoke(-10)
            assert isinstance(response, ProblemDetail)
            assert INVALID_INPUT.uri == response.uri

    def test_hold_fails_when_patron_is_at_hold_limit(self):
        edition, pool = self._edition(with_license_pool=True)
        pool.open_access = False
        with self.request_context_with_library(
                "/", headers=dict(Authorization=self.valid_auth)):
            patron = self.manager.loans.authenticated_patron_from_request()
            self.manager.d_circulation.queue_checkout(
                pool, NoAvailableCopies()
            )
            self.manager.d_circulation.queue_hold(
                pool, PatronHoldLimitReached()
            )
            response = self.manager.loans.borrow(
                pool.identifier.type,
                pool.identifier.identifier
            )
            assert isinstance(response, ProblemDetail)
            assert HOLD_LIMIT_REACHED.uri == response.uri

    def test_borrow_fails_with_outstanding_fines(self):
        threem_edition, pool = self._edition(
            with_open_access_download=False,
            data_source_name=DataSource.THREEM,
            identifier_type=Identifier.THREEM_ID,
            with_license_pool=True,
        )
        threem_book = self._work(
            presentation_edition=threem_edition,
        )
        pool.open_access = False

        ConfigurationSetting.for_library(
            Configuration.MAX_OUTSTANDING_FINES, self._default_library).value = "$0.50"
        with self.request_context_with_library(
                "/", headers=dict(Authorization=self.valid_auth)):

            # The patron's credentials are valid, but they have a lot
            # of fines.
            patron = self.manager.loans.authenticated_patron_from_request()
            patron.fines = Decimal("12345678.90")
            response = self.manager.loans.borrow(
                pool.identifier.type, pool.identifier.identifier)

            assert 403 == response.status_code
            assert OUTSTANDING_FINES.uri == response.uri
            assert "$12345678.90 outstanding" in response.detail

        # Reduce the patron's fines, and there's no problem.
        with self.request_context_with_library(
                "/", headers=dict(Authorization=self.valid_auth)):
            patron = self.manager.loans.authenticated_patron_from_request()
            patron.fines = Decimal("0.49")
            self.manager.d_circulation.queue_checkout(
                pool,
                LoanInfo(
                    pool.collection, pool.data_source.name,
                    pool.identifier.type,
                    pool.identifier.identifier,
                    utc_now(),
                    utc_now() + datetime.timedelta(seconds=3600),
                )
            )
            response = self.manager.loans.borrow(
                pool.identifier.type, pool.identifier.identifier)

            assert 201 == response.status_code

    def test_3m_cant_revoke_hold_if_reserved(self):
         threem_edition, pool = self._edition(
             with_open_access_download=False,
             data_source_name=DataSource.THREEM,
             identifier_type=Identifier.THREEM_ID,
             with_license_pool=True,
         )
         threem_book = self._work(
             presentation_edition=threem_edition,
         )
         pool.open_access = False

         with self.request_context_with_library(
                 "/", headers=dict(Authorization=self.valid_auth)):
            patron = self.manager.loans.authenticated_patron_from_request()
            hold, newly_created = pool.on_hold_to(patron, position=0)
            response = self.manager.loans.revoke(pool.id)
            assert 400 == response.status_code
            assert CANNOT_RELEASE_HOLD.uri == response.uri
            assert "Cannot release a hold once it enters reserved state." == response.detail

    def test_active_loans(self):

        # First, verify that this controller supports conditional HTTP
        # GET by calling handle_conditional_request and propagating
        # any Response it returns.
        response_304 = Response(status=304)
        def handle_conditional_request(last_modified=None):
            return response_304
        original_handle_conditional_request = self.controller.handle_conditional_request
        self.manager.loans.handle_conditional_request = handle_conditional_request

        # Before making any requests, set the patron's last_loan_activity_sync
        # to a known value.
        patron = None
        with self.request_context_with_library("/"):
            patron = self.controller.authenticated_patron(
                self.valid_credentials
            )
        now = utc_now()
        patron.last_loan_activity_sync = now

        # Make a request -- it doesn't have If-Modified-Since, but our
        # mocked handle_conditional_request will treat it as a
        # successful conditional request.
        with self.request_context_with_library(
            "/", headers=dict(Authorization=self.valid_auth)
        ):
            patron = self.manager.loans.authenticated_patron_from_request()
            response = self.manager.loans.sync()
            assert response is response_304

        # Since the conditional request succeeded, we did not call out
        # to the vendor APIs, and patron.last_loan_activity_sync was
        # not updated.
        assert now == patron.last_loan_activity_sync

        # Leaving patron.last_loan_activity_sync alone will stop the
        # circulation manager from calling out to the external APIs,
        # since it was set to a recent time. We test this explicitly
        # later, but for now, clear it out.
        patron.last_loan_activity_sync = None

        # Un-mock handle_conditional_request. It will be called over
        # the course of this test, but it will not notice any more
        # conditional requests -- the detailed behavior of
        # handle_conditional_request is tested elsewhere.
        self.manager.loans.handle_conditional_request = (
            original_handle_conditional_request
        )

        # If the request is not conditional, an OPDS feed is returned.
        # This feed is empty because the patron has no loans.
        with self.request_context_with_library(
            "/", headers=dict(Authorization=self.valid_auth)
        ):
            patron = self.manager.loans.authenticated_patron_from_request()
            response = self.manager.loans.sync()
            assert not "<entry>" in response.get_data(as_text=True)
            assert response.headers['Cache-Control'].startswith('private,')

            # patron.last_loan_activity_sync was set to the moment the
            # LoanController started calling out to the remote APIs.
            new_sync_time = patron.last_loan_activity_sync
            assert new_sync_time > now

        # Set up a bunch of loans on the remote APIs.
        overdrive_edition, overdrive_pool = self._edition(
            with_open_access_download=False,
            data_source_name=DataSource.OVERDRIVE,
            identifier_type=Identifier.OVERDRIVE_ID,
            with_license_pool=True,
        )
        overdrive_book = self._work(
            presentation_edition=overdrive_edition,
        )
        overdrive_pool.open_access = False

        bibliotheca_edition, bibliotheca_pool = self._edition(
            with_open_access_download=False,
            data_source_name=DataSource.BIBLIOTHECA,
            identifier_type=Identifier.BIBLIOTHECA_ID,
            with_license_pool=True,
        )
        bibliotheca_book = self._work(
            presentation_edition=bibliotheca_edition,
        )
        bibliotheca_pool.licenses_available = 0
        bibliotheca_pool.open_access = False

        self.manager.d_circulation.add_remote_loan(
            overdrive_pool.collection, overdrive_pool.data_source,
            overdrive_pool.identifier.type,
            overdrive_pool.identifier.identifier,
            utc_now(),
            utc_now() + datetime.timedelta(seconds=3600)
        )
        self.manager.d_circulation.add_remote_hold(
            bibliotheca_pool.collection, bibliotheca_pool.data_source,
            bibliotheca_pool.identifier.type,
            bibliotheca_pool.identifier.identifier,
            utc_now(),
            utc_now() + datetime.timedelta(seconds=3600),
            0,
        )

        # Making a new request so soon after the last one means the
        # circulation manager won't actually call out to the vendor
        # APIs. The resulting feed won't reflect what we know to be
        # the reality.
        with self.request_context_with_library(
                "/", headers=dict(Authorization=self.valid_auth)):
            patron = self.manager.loans.authenticated_patron_from_request()
            response = self.manager.loans.sync()
            assert '<entry>' not in response.get_data(as_text=True)

        # patron.last_loan_activity_sync was not changed as the result
        # of this request, since we didn't go to the vendor APIs.
        assert patron.last_loan_activity_sync == new_sync_time

        # Change it now, to a timestamp far in the past.
        long_ago = datetime_utc(2000, 1, 1)
        patron.last_loan_activity_sync = long_ago

        # This ensures that when we request the loans feed again, the
        # LoanController actually goes out to the vendor APIs for new
        # information.
        with self.request_context_with_library(
                "/", headers=dict(Authorization=self.valid_auth)):
            patron = self.manager.loans.authenticated_patron_from_request()
            response = self.manager.loans.sync()

            # This time, the feed contains entries.
            feed = feedparser.parse(response.data)
            entries = feed['entries']

            overdrive_entry = [entry for entry in entries if entry['title'] == overdrive_book.title][0]
            bibliotheca_entry = [entry for entry in entries if entry['title'] == bibliotheca_book.title][0]

            assert overdrive_entry['opds_availability']['status'] == 'available'
            assert bibliotheca_entry['opds_availability']['status'] == 'ready'

            overdrive_links = overdrive_entry['links']
            fulfill_link = [x for x in overdrive_links if x['rel'] == 'http://opds-spec.org/acquisition'][0]['href']
            revoke_link = [x for x in overdrive_links if x['rel'] == OPDSFeed.REVOKE_LOAN_REL][0]['href']
            bibliotheca_links = bibliotheca_entry['links']
            borrow_link = [x for x in bibliotheca_links if x['rel'] == 'http://opds-spec.org/acquisition/borrow'][0]['href']
            bibliotheca_revoke_links = [x for x in bibliotheca_links if x['rel'] == OPDSFeed.REVOKE_LOAN_REL]

            assert urllib.parse.quote("%s/fulfill" % overdrive_pool.id) in fulfill_link
            assert urllib.parse.quote("%s/revoke" % overdrive_pool.id) in revoke_link
            assert urllib.parse.quote("%s/%s/borrow" % (bibliotheca_pool.identifier.type, bibliotheca_pool.identifier.identifier)) in borrow_link
            assert 0 == len(bibliotheca_revoke_links)

            # Since we went out the the vendor APIs,
            # patron.last_loan_activity_sync was updated.
            assert patron.last_loan_activity_sync > new_sync_time

class TestAnnotationController(CirculationControllerTest):
    def setup_method(self):
        super(TestAnnotationController, self).setup_method()
        self.pool = self.english_1.license_pools[0]
        self.edition = self.pool.presentation_edition
        self.identifier = self.edition.primary_identifier

    def test_get_empty_container(self):
        with self.request_context_with_library(
                "/", headers=dict(Authorization=self.valid_auth)):
            self.manager.loans.authenticated_patron_from_request()
            response = self.manager.annotations.container()
            assert 200 == response.status_code

            # We've been given an annotation container with no items.
            container = json.loads(response.get_data(as_text=True))
            assert [] == container['first']['items']
            assert 0 == container['total']

            # The response has the appropriate headers.
            allow_header = response.headers['Allow']
            for method in ['GET', 'HEAD', 'OPTIONS', 'POST']:
                assert method in allow_header

            assert AnnotationWriter.CONTENT_TYPE == response.headers['Accept-Post']
            assert AnnotationWriter.CONTENT_TYPE == response.headers['Content-Type']
            assert 'W/""' == response.headers['ETag']

    def test_get_container_with_item(self):
        self.pool.loan_to(self.default_patron)

        annotation, ignore = create(
            self._db, Annotation,
            patron=self.default_patron,
            identifier=self.identifier,
            motivation=Annotation.IDLING,
        )
        annotation.active = True
        annotation.timestamp = utc_now()

        with self.request_context_with_library(
                "/", headers=dict(Authorization=self.valid_auth)):
            self.manager.annotations.authenticated_patron_from_request()
            response = self.manager.annotations.container()
            assert 200 == response.status_code

            # We've been given an annotation container with one item.
            container = json.loads(response.get_data(as_text=True))
            assert 1 == container['total']
            item = container['first']['items'][0]
            assert annotation.motivation == item['motivation']

            # The response has the appropriate headers.
            allow_header = response.headers['Allow']
            for method in ['GET', 'HEAD', 'OPTIONS', 'POST']:
                assert method in allow_header

            assert AnnotationWriter.CONTENT_TYPE == response.headers['Accept-Post']
            assert AnnotationWriter.CONTENT_TYPE == response.headers['Content-Type']
            expected_etag = 'W/"%s"' % annotation.timestamp
            assert expected_etag == response.headers['ETag']
            expected_time = format_date_time(mktime(annotation.timestamp.timetuple()))
            assert expected_time == response.headers['Last-Modified']

    def test_get_container_for_work(self):
        self.pool.loan_to(self.default_patron)

        annotation, ignore = create(
            self._db, Annotation,
            patron=self.default_patron,
            identifier=self.identifier,
            motivation=Annotation.IDLING,
        )
        annotation.active = True
        annotation.timestamp = utc_now()

        other_annotation, ignore = create(
            self._db, Annotation,
            patron=self.default_patron,
            identifier=self._identifier(),
            motivation=Annotation.IDLING,
        )

        with self.request_context_with_library(
                "/", headers=dict(Authorization=self.valid_auth)):
            self.manager.annotations.authenticated_patron_from_request()
            response = self.manager.annotations.container_for_work(self.identifier.type, self.identifier.identifier)
            assert 200 == response.status_code

            # We've been given an annotation container with one item.
            container = json.loads(response.get_data(as_text=True))
            assert 1 == container['total']
            item = container['first']['items'][0]
            assert annotation.motivation == item['motivation']

            # The response has the appropriate headers - POST is not allowed.
            allow_header = response.headers['Allow']
            for method in ['GET', 'HEAD', 'OPTIONS']:
                assert method in allow_header

            assert 'Accept-Post' not in list(response.headers.keys())
            assert AnnotationWriter.CONTENT_TYPE == response.headers['Content-Type']
            expected_etag = 'W/"%s"' % annotation.timestamp
            assert expected_etag == response.headers['ETag']
            expected_time = format_date_time(mktime(annotation.timestamp.timetuple()))
            assert expected_time == response.headers['Last-Modified']

    def test_post_to_container(self):
        data = dict()
        data['@context'] = AnnotationWriter.JSONLD_CONTEXT
        data['type'] = "Annotation"
        data['motivation'] = Annotation.IDLING
        data['target'] = dict(source=self.identifier.urn, selector="epubcfi(/6/4[chap01ref]!/4[body01]/10[para05]/3:10)")

        with self.request_context_with_library(
            "/", headers=dict(Authorization=self.valid_auth), method='POST', data=json.dumps(data)):
            patron = self.manager.annotations.authenticated_patron_from_request()
            patron.synchronize_annotations = True
            # The patron doesn't have any annotations yet.
            annotations = self._db.query(Annotation).filter(Annotation.patron==patron).all()
            assert 0 == len(annotations)

            response = self.manager.annotations.container()

            # The patron doesn't have the pool on loan yet, so the request fails.
            assert 400 == response.status_code
            annotations = self._db.query(Annotation).filter(Annotation.patron==patron).all()
            assert 0 == len(annotations)

            # Give the patron a loan and try again, and the request creates an annotation.
            self.pool.loan_to(patron)
            response = self.manager.annotations.container()
            assert 200 == response.status_code

            annotations = self._db.query(Annotation).filter(Annotation.patron==patron).all()
            assert 1 == len(annotations)
            annotation = annotations[0]
            assert Annotation.IDLING == annotation.motivation
            selector = json.loads(annotation.target).get("http://www.w3.org/ns/oa#hasSelector")[0].get('@id')
            assert data['target']['selector'] == selector

            # The response contains the annotation in the db.
            item = json.loads(response.get_data(as_text=True))
            assert str(annotation.id) in item['id']
            assert annotation.motivation == item['motivation']

    def test_detail(self):
        self.pool.loan_to(self.default_patron)

        annotation, ignore = create(
            self._db, Annotation,
            patron=self.default_patron,
            identifier=self.identifier,
            motivation=Annotation.IDLING,
        )
        annotation.active = True

        with self.request_context_with_library(
                "/", headers=dict(Authorization=self.valid_auth)):
            self.manager.annotations.authenticated_patron_from_request()
            response = self.manager.annotations.detail(annotation.id)
            assert 200 == response.status_code

            # We've been given a single annotation item.
            item = json.loads(response.get_data(as_text=True))
            assert str(annotation.id) in item['id']
            assert annotation.motivation == item['motivation']

            # The response has the appropriate headers.
            allow_header = response.headers['Allow']
            for method in ['GET', 'HEAD', 'OPTIONS', 'DELETE']:
                assert method in allow_header

            assert AnnotationWriter.CONTENT_TYPE == response.headers['Content-Type']

    def test_detail_for_other_patrons_annotation_returns_404(self):
        patron = self._patron()
        self.pool.loan_to(patron)

        annotation, ignore = create(
            self._db, Annotation,
            patron=patron,
            identifier=self.identifier,
            motivation=Annotation.IDLING,
        )
        annotation.active = True

        with self.request_context_with_library(
                "/", headers=dict(Authorization=self.valid_auth)):
            self.manager.annotations.authenticated_patron_from_request()

            # The patron can't see that this annotation exists.
            response = self.manager.annotations.detail(annotation.id)
            assert 404 == response.status_code

    def test_detail_for_missing_annotation_returns_404(self):
        with self.request_context_with_library(
                "/", headers=dict(Authorization=self.valid_auth)):
            self.manager.annotations.authenticated_patron_from_request()

            # This annotation does not exist.
            response = self.manager.annotations.detail(100)
            assert 404 == response.status_code

    def test_detail_for_deleted_annotation_returns_404(self):
        self.pool.loan_to(self.default_patron)

        annotation, ignore = create(
            self._db, Annotation,
            patron=self.default_patron,
            identifier=self.identifier,
            motivation=Annotation.IDLING,
        )
        annotation.active = False

        with self.request_context_with_library(
                "/", headers=dict(Authorization=self.valid_auth)):
            self.manager.annotations.authenticated_patron_from_request()
            response = self.manager.annotations.detail(annotation.id)
            assert 404 == response.status_code

    def test_delete(self):
        self.pool.loan_to(self.default_patron)

        annotation, ignore = create(
            self._db, Annotation,
            patron=self.default_patron,
            identifier=self.identifier,
            motivation=Annotation.IDLING,
        )
        annotation.active = True

        with self.request_context_with_library(
                "/", method='DELETE', headers=dict(Authorization=self.valid_auth)):
            self.manager.annotations.authenticated_patron_from_request()
            response = self.manager.annotations.detail(annotation.id)
            assert 200 == response.status_code

            # The annotation has been marked inactive.
            assert False == annotation.active


class TestWorkController(CirculationControllerTest):
    def setup_method(self):
        super(TestWorkController, self).setup_method()
        [self.lp] = self.english_1.license_pools
        self.edition = self.lp.presentation_edition
        self.datasource = self.lp.data_source.name
        self.identifier = self.lp.identifier

    def test_contributor(self):
        m = self.manager.work_controller.contributor

        # Find a real Contributor put in the system through the setup
        # process.
        [contribution] = self.english_1.presentation_edition.contributions
        contributor = contribution.contributor

        # The contributor is created with both .sort_name and
        # .display_name, but we want to test what happens when both
        # pieces of data aren't avaiable, so unset .sort_name.
        contributor.sort_name = None

        # No contributor name -> ProblemDetail
        with self.request_context_with_library('/'):
            response = m('', None, None)
        assert 404 == response.status_code
        assert NO_SUCH_LANE.uri == response.uri
        assert "No contributor provided" == response.detail

        # Unable to load ContributorData from contributor name ->
        # ProblemDetail
        with self.request_context_with_library('/'):
            response = m('Unknown Author', None, None)
        assert 404 == response.status_code
        assert NO_SUCH_LANE.uri == response.uri
        assert "Unknown contributor: Unknown Author" == response.detail

        contributor = contributor.display_name

        # Search index misconfiguration -> Problem detail
        self.assert_bad_search_index_gives_problem_detail(
            lambda: self.manager.work_controller.series(
                contributor, None, None
            )
        )

        # Bad facet data -> ProblemDetail
        with self.request_context_with_library('/?order=nosuchorder'):
            response = m(contributor, None, None)
            assert 400 == response.status_code
            assert INVALID_INPUT.uri == response.uri

        # Bad pagination data -> ProblemDetail
        with self.request_context_with_library('/?size=abc'):
            response = m(contributor, None, None)
            assert 400 == response.status_code
            assert INVALID_INPUT.uri == response.uri

        # Test an end-to-end success (not including a test that the
        # search engine can actually find books by a given person --
        # that's tested in core/tests/test_external_search.py).
        with self.request_context_with_library('/'):
            response = m(contributor, 'eng,spa', 'Children,Young Adult')
        assert 200 == response.status_code
        assert OPDSFeed.ACQUISITION_FEED_TYPE == response.headers['Content-Type']
        feed = feedparser.parse(response.data)

        # The feed is named after the person we looked up.
        assert contributor == feed['feed']['title']

        # It's got one entry -- the book added to the search engine
        # during test setup.
        [entry] = feed['entries']
        assert self.english_1.title == entry['title']

        # The feed has facet links.
        links = feed['feed']['links']
        facet_links = [link for link in links
                       if link['rel'] == 'http://opds-spec.org/facet']
        assert 8 == len(facet_links)

        # The feed was cached.
        cached = self._db.query(CachedFeed).one()
        assert CachedFeed.CONTRIBUTOR_TYPE == cached.type
        assert (
            'John Bull-eng,spa-Children,Young+Adult' ==
            cached.unique_key)

        # At this point we don't want to generate real feeds anymore.
        # We can't do a real end-to-end test without setting up a real
        # search index, which is obnoxiously slow.
        #
        # Instead, we will mock AcquisitionFeed.page, and examine the objects
        # passed into it under different mock requests.
        #
        # Those objects, such as ContributorLane and
        # ContributorFacets, are tested elsewhere, in terms of their
        # effects on search objects such as Filter. Those search
        # objects are the things that are tested against a real search
        # index (in core).
        #
        # We know from the previous test that any results returned
        # from the search engine are converted into an OPDS feed. Now
        # we verify that an incoming request results in the objects
        # we'd expect to use to generate the feed for that request.
        class Mock(object):
            @classmethod
            def page(cls, **kwargs):
                self.called_with = kwargs
                return Response("An OPDS feed")

        # Test a basic request with custom faceting, pagination, and a
        # language and audience restriction. This will exercise nearly
        # all the functionality we need to check.
        languages = "some languages"
        audiences = "some audiences"
        sort_key = ["sort", "pagination", "key"]
        with self.request_context_with_library(
            "/?order=title&size=100&key=%s&entrypoint=Audio" % (
                json.dumps(sort_key)
            )
        ):
            response = m(contributor, languages, audiences, feed_class=Mock)

        # The Response served by Mock.page becomes the response to the
        # incoming request.
        assert 200 == response.status_code
        assert "An OPDS feed" == response.get_data(as_text=True)

        # Now check all the keyword arguments that were passed into
        # page().
        kwargs = self.called_with

        assert self._db == kwargs.pop('_db')
        assert self.manager._external_search == kwargs.pop('search_engine')

        # The feed is named after the contributor the request asked
        # about.
        assert contributor == kwargs.pop('title')

        # Query string arguments were taken into account when
        # creating the Facets and Pagination objects.
        facets = kwargs.pop('facets')
        assert isinstance(facets, ContributorFacets)
        assert AudiobooksEntryPoint == facets.entrypoint
        assert 'title' == facets.order

        pagination = kwargs.pop('pagination')
        assert isinstance(pagination, SortKeyPagination)
        assert sort_key == pagination.last_item_on_previous_page
        assert 100 == pagination.size

        lane = kwargs.pop('worklist')
        assert isinstance(lane, ContributorLane)
        assert isinstance(lane.contributor, ContributorData)

        # We don't know whether the incoming name is a sort name
        # or a display name, so we ask ContributorData.lookup to
        # try it both ways.
        assert contributor == lane.contributor.sort_name
        assert contributor == lane.contributor.display_name
        assert [languages] == lane.languages
        assert [audiences] == lane.audiences

        # Checking the URL is difficult because it requires a request
        # context, _plus_ the ContributorFacets, Pagination and Lane
        # created during the original request.
        library = self._default_library
        route, url_kwargs = lane.url_arguments
        url_kwargs.update(dict(list(facets.items())))
        url_kwargs.update(dict(list(pagination.items())))
        with self.request_context_with_library(""):
            expect_url = self.manager.opds_feeds.url_for(
                route, lane_identifier=None,
                library_short_name=library.short_name,
                **url_kwargs
            )
        assert kwargs.pop('url') == expect_url

        # The Annotator object was instantiated with the proper lane
        # and the newly created Facets object.
        annotator = kwargs.pop('annotator')
        assert lane == annotator.lane
        assert facets == annotator.facets

        # No other arguments were passed into page().
        assert {} == kwargs

    def test_age_appropriateness_end_to_end(self):
        # An end-to-end test of the idea that a patron can't access
        # feeds configured to include titles that would not be
        # age-appropriate for that patron.
        #
        # A similar test could be run for any of the other subclasses
        # of DynamicLane.
        m = self.manager.work_controller.contributor

        contributor, ignore = self._contributor()

        patron = self.default_patron
        patron.external_type = "child"
        children_lane = self._lane()
        children_lane.audiences = [Classifier.AUDIENCE_CHILDREN]
        children_lane.target_age = tuple_to_numericrange((4, 5))
        children_lane.root_for_patron_type = ["child"]

        with self.request_context_with_library(
            "/", headers=dict(Authorization=self.valid_auth)
        ):
            # If we ask for books for adults _or_ children by a given
            # author, we're denied access -- the authenticated
            # patron's root lane would make any adult books
            # age-inappropriate.
            audiences = ",".join([
                Classifier.AUDIENCE_ADULT, Classifier.AUDIENCE_CHILDREN
            ])
            response = m(contributor.sort_name, "eng", audiences)
            assert isinstance(response, ProblemDetail)
            assert NO_SUCH_LANE.uri == response.uri

            # If we only ask for children's books by the same author,
            # we're fine.
            response = m(contributor.sort_name, "eng",
                         Classifier.AUDIENCE_CHILDREN)
            assert 200 == response.status_code

        # We're also fine if we don't authenticate the request at all.
        with self.request_context_with_library("/"):
            response = m(contributor.sort_name, "eng", audiences)
            assert 200 == response.status_code

    def test_permalink(self):
        with self.request_context_with_library("/"):
            response = self.manager.work_controller.permalink(self.identifier.type, self.identifier.identifier)
            annotator = LibraryAnnotator(None, None, self._default_library)
            expect = AcquisitionFeed.single_entry(
                self._db, self.english_1, annotator
            ).data

        assert 200 == response.status_code
        assert expect == response.get_data()
        assert OPDSFeed.ENTRY_TYPE == response.headers['Content-Type']

    def test_permalink_does_not_return_fulfillment_links_for_authenticated_patrons_without_loans(self):
        with self.request_context_with_library("/"):
            # We have two patrons.
            patron_1 = self._patron()
            patron_2 = self._patron()

            # But the request was initiated by the first patron.
            flask.request.patron = patron_1

            identifier_type = Identifier.GUTENBERG_ID
            identifier = '1234567890'
            edition, _ = self._edition(
                title='Test Book',
                identifier_type=identifier_type,
                identifier_id=identifier,
                with_license_pool=True
            )
            work = self._work(
                'Test Book',
                presentation_edition=edition,
                with_license_pool=True
            )
            pool = work.license_pools[0]

            # Only the second patron has a loan.
            patron2_loan, _ = pool.loan_to(patron_2)

            # We want to make sure that the feed doesn't contain any fulfillment links.
            active_loans_by_work = {}
            annotator = LibraryAnnotator(
                None,
                None,
                self._default_library,
                active_loans_by_work=active_loans_by_work
            )
            expect = AcquisitionFeed.single_entry(
                self._db, work, annotator
            ).data

            response = self.manager.work_controller.permalink(identifier_type, identifier)

        assert 200 == response.status_code
        assert expect == response.get_data()
        assert OPDSFeed.ENTRY_TYPE == response.headers['Content-Type']

    def test_permalink_returns_fulfillment_links_for_authenticated_patrons_with_loans(self):
        with self.request_context_with_library("/"):
            # We have two patrons.
            patron_1 = self._patron()
            patron_2 = self._patron()

            # But the request was initiated by the first patron.
            flask.request.patron = patron_1

            identifier_type = Identifier.GUTENBERG_ID
            identifier = '1234567890'
            edition, _ = self._edition(
                title='Test Book',
                identifier_type=identifier_type,
                identifier_id=identifier,
                with_license_pool=True
            )
            work = self._work(
                'Test Book',
                presentation_edition=edition,
                with_license_pool=True
            )
            pool = work.license_pools[0]

            # Both patrons have loans.
            patron1_loan, _ = pool.loan_to(patron_1)
            patron2_loan, _ = pool.loan_to(patron_2)

            # We want to make sure that only the first patron's loan will be in the feed.
            active_loans_by_work = {
                work: patron1_loan
            }
            annotator = LibraryAnnotator(
                None,
                None,
                self._default_library,
                active_loans_by_work=active_loans_by_work
            )
            expect = AcquisitionFeed.single_entry(
                self._db, work, annotator
            ).data

            response = self.manager.work_controller.permalink(identifier_type, identifier)

        assert 200 == response.status_code
        assert expect == response.get_data()
        assert OPDSFeed.ENTRY_TYPE == response.headers['Content-Type']

    def test_permalink_returns_fulfillment_links_for_authenticated_patrons_with_fulfillment(self):
        auth = dict(Authorization=self.valid_auth)

        with self.request_context_with_library("/", headers=auth):
            content_link = 'https://content'

            # We have two patrons.
            patron_1 = self.controller.authenticated_patron(self.valid_credentials)
            patron_2 = self._patron()

            # But the request was initiated by the first patron.
            flask.request.patron = patron_1

            identifier_type = Identifier.GUTENBERG_ID
            identifier = '1234567890'
            edition, _ = self._edition(
                title='Test Book',
                identifier_type=identifier_type,
                identifier_id=identifier,
                with_license_pool=True
            )
            work = self._work(
                'Test Book',
                presentation_edition=edition,
                with_license_pool=True
            )
            pool = work.license_pools[0]
            [delivery_mechanism] = pool.delivery_mechanisms

            loan_info = LoanInfo(
                pool.collection, pool.data_source.name,
                pool.identifier.type,
                pool.identifier.identifier,
                utc_now(),
                utc_now() + datetime.timedelta(seconds=3600),
            )
            self.manager.d_circulation.queue_checkout(
                pool,
                loan_info
            )

            fulfillment = FulfillmentInfo(
                pool.collection,
                pool.data_source,
                pool.identifier.type,
                pool.identifier.identifier,
                content_link=content_link,
                content_type=MediaTypes.EPUB_MEDIA_TYPE,
                content=None,
                content_expires=None
            )
            self.manager.d_circulation.queue_fulfill(pool, fulfillment)

            # Both patrons have loans:
            # - the first patron's loan and fulfillment will be created via API.
            # - the second patron's loan will be created via loan_to method.
            self.manager.loans.borrow(
                pool.identifier.type, pool.identifier.identifier, delivery_mechanism.delivery_mechanism.id
            )
            self.manager.loans.fulfill(
                pool.id, delivery_mechanism.delivery_mechanism.id,
            )

            patron1_loan = pool.loans[0]
            # We have to create a Resource object manually
            # to assign a URL to the fulfillment that will be used to generate an acquisition link.
            patron1_loan.fulfillment.resource = Resource(url=fulfillment.content_link)

            patron2_loan, _ = pool.loan_to(patron_2)

            # We want to make sure that only the first patron's fulfillment will be in the feed.
            active_loans_by_work = {
                work: patron1_loan
            }
            annotator = LibraryAnnotator(
                None,
                None,
                self._default_library,
                active_loans_by_work=active_loans_by_work,
            )
            expect = AcquisitionFeed.single_entry(
                self._db, work, annotator
            ).data

            response = self.manager.work_controller.permalink(identifier_type, identifier)

        assert 200 == response.status_code
        assert expect == response.get_data()
        assert OPDSFeed.ENTRY_TYPE == response.headers['Content-Type']

    def test_recommendations(self):
        # Test the ability to get a feed of works recommended by an
        # external service.
        [self.lp] = self.english_1.license_pools
        self.edition = self.lp.presentation_edition
        self.datasource = self.lp.data_source.name
        self.identifier = self.lp.identifier

        # Prep an empty recommendation.
        source = DataSource.lookup(self._db, self.datasource)
        metadata = Metadata(source)
        mock_api = MockNoveListAPI(self._db)

        args = [self.identifier.type,
                self.identifier.identifier]
        kwargs = dict(novelist_api=mock_api)

        # We get a 400 response if the pagination data is bad.
        with self.request_context_with_library('/?size=abc'):
            response = self.manager.work_controller.recommendations(
                *args, **kwargs
            )
            assert 400 == response.status_code

        # Or if the facet data is bad.
        with self.request_context_with_library('/?order=nosuchorder'):
            response = self.manager.work_controller.recommendations(
                *args, **kwargs
            )
            assert 400 == response.status_code

        # Or if the search index is misconfigured.
        self.assert_bad_search_index_gives_problem_detail(
            lambda: self.manager.work_controller.recommendations(
                *args, **kwargs
            )
        )

        # If no NoveList API is configured, the lane does not exist.
        with self.request_context_with_library('/'):
            response = self.manager.work_controller.recommendations(
                *args, novelist_api=None
            )
        assert 404 == response.status_code
        assert "http://librarysimplified.org/terms/problem/unknown-lane" == response.uri
        assert "Recommendations not available" == response.detail

        # If the NoveList API is configured, the search index is asked
        # about its recommendations.
        #
        # In this test it doesn't matter whether NoveList actually
        # provides any recommendations. The Filter object will be
        # created with .return_nothing set, but our mock
        # ExternalSearchIndex will ignore that setting and return
        # everything in its index -- as it always does.
        with self.request_context_with_library('/'):
            response = self.manager.work_controller.recommendations(
                *args, **kwargs
            )

        # A feed is returned with the data from the
        # ExternalSearchIndex.
        assert 200 == response.status_code
        feed = feedparser.parse(response.data)
        assert 'Titles recommended by NoveList' == feed['feed']['title']
        [entry] = feed.entries
        assert self.english_1.title == entry['title']
        author = self.edition.author_contributors[0]
        expected_author_name = author.display_name or author.sort_name
        assert expected_author_name == entry.author

        # Now let's pass in a mocked AcquisitionFeed so we can check
        # the arguments used to invoke page().
        class Mock(object):
            @classmethod
            def page(cls, **kwargs):
                cls.called_with = kwargs
                return Response("A bunch of titles")

        kwargs['feed_class'] = Mock
        with self.request_context_with_library(
            '/?order=title&size=2&after=30&entrypoint=Audio'
        ):
            response = self.manager.work_controller.recommendations(
                *args, **kwargs
            )

        # The return value of Mock.page was used as the response
        # to the incoming request.
        assert 200 == response.status_code
        assert "A bunch of titles" == response.get_data(as_text=True)

        kwargs = Mock.called_with
        assert self._db == kwargs.pop('_db')
        assert 'Titles recommended by NoveList' == kwargs.pop('title')

        # The RecommendationLane is set up to ask for recommendations
        # for this book.
        lane = kwargs.pop('worklist')
        assert isinstance(lane, RecommendationLane)
        library = self._default_library
        assert library.id == lane.library_id
        assert self.english_1 == lane.work
        assert 'Recommendations for Quite British by John Bull' == lane.display_name
        assert mock_api == lane.novelist_api

        facets = kwargs.pop('facets')
        assert isinstance(facets, Facets)
        assert Facets.ORDER_TITLE == facets.order
        assert AudiobooksEntryPoint == facets.entrypoint

        pagination = kwargs.pop('pagination')
        assert 30 == pagination.offset
        assert 2 == pagination.size

        annotator = kwargs.pop('annotator')
        assert lane == annotator.lane

        # Checking the URL is difficult because it requires a request
        # context, _plus_ the Facets, Pagination and Lane created
        # during the original request.
        route, url_kwargs = lane.url_arguments
        url_kwargs.update(dict(list(facets.items())))
        url_kwargs.update(dict(list(pagination.items())))
        with self.request_context_with_library(""):
            expect_url = self.manager.work_controller.url_for(
                route, library_short_name=library.short_name,
                **url_kwargs
            )
        assert kwargs.pop('url') == expect_url

    def test_related_books(self):
        # Test the related_books controller.

        # Remove the contributor from the work created during setup.
        work = self.english_1
        edition = work.presentation_edition
        identifier = edition.primary_identifier
        [contribution] = edition.contributions
        contributor = contribution.contributor
        role = contribution.role
        self._db.delete(contribution)
        self._db.commit()
        assert None == edition.series

        # First, let's test a complex error case. We're asking about a
        # work with no contributors or series, and no NoveList
        # integration is configured. The 'related books' lane ends up
        # with no sublanes, so the controller acts as if the lane
        # itself does not exist.
        with self.request_context_with_library('/'):
            response = self.manager.work_controller.related(
                identifier.type, identifier.identifier,
            )
            assert 404 == response.status_code
            assert "http://librarysimplified.org/terms/problem/unknown-lane" == response.uri

        # Now test some error cases where the lane exists but
        # something else goes wrong.

        # Give the work a series and a contributor, so that it will
        # get sublanes for both types of recommendations.
        edition.series = "Around the World"
        edition.add_contributor(contributor, role)

        # A grouped feed is not paginated, so we don't check pagination
        # information and there's no chance of a problem detail.

        # Theoretically, if bad faceting information is provided we'll
        # get a problem detail. But the faceting class created is
        # FeaturedFacets, which can't raise an exception during the
        # creation process -- an invalid entrypoint will simply be
        # ignored.

        # Bad search index setup -> Problem detail
        self.assert_bad_search_index_gives_problem_detail(
            lambda: self.manager.work_controller.related(
                identifier.type, identifier.identifier
            )
        )

        # The mock search engine will return this Work for every
        # search. That means this book will show up as a 'same author'
        # recommendation, a 'same series' recommentation, and a
        # 'external service' recommendation.
        same_author_and_series = self._work(
            title="Same author and series", with_license_pool=True
        )
        self.manager.external_search.docs = {}
        self.manager.external_search.bulk_update([same_author_and_series])

        mock_api = MockNoveListAPI(self._db)

        # Create a fresh book, and set up a mock NoveList API to
        # recommend its identifier for any input.
        #
        # The mock API needs to return a list of Identifiers, so that
        # the RelatedWorksLane will ask the RecommendationLane to find
        # us a matching work instead of hiding it. But the search
        # index is also mocked, so within this test will return the
        # same book it always does -- same_author_and_series.
        overdrive = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        metadata = Metadata(overdrive)
        recommended_identifier = self._identifier()
        metadata.recommendations = [recommended_identifier]
        mock_api.setup_method(metadata)

        # Now, ask for works related to self.english_1.
        with mock_search_index(self.manager.external_search):
            with self.request_context_with_library('/?entrypoint=Book'):
                response = self.manager.work_controller.related(
                    self.identifier.type, self.identifier.identifier,
                    novelist_api=mock_api
                )
        assert 200 == response.status_code
        assert OPDSFeed.ACQUISITION_FEED_TYPE == response.headers['content-type']
        feed = feedparser.parse(response.data)
        assert "Related Books" == feed['feed']['title']

        # The feed contains three entries: one for each sublane.
        assert 3 == len(feed['entries'])

        # Group the entries by the sublane they're in.
        def collection_link(entry):
            [link] = [l for l in entry['links'] if l['rel']=='collection']
            return link['title'], link['href']
        by_collection_link = {}
        for entry in feed['entries']:
            title, href = collection_link(entry)
            by_collection_link[title] = (href, entry)

        # Here's the sublane for books in the same series.
        [same_series_href, same_series_entry] = by_collection_link[
            'Around the World'
        ]
        assert "Same author and series" == same_series_entry['title']
        expected_series_link = 'series/%s/eng/Adult' % urllib.parse.quote("Around the World")
        assert same_series_href.endswith(expected_series_link)

        # Here's the sublane for books by this contributor.
        [same_contributor_href, same_contributor_entry] = by_collection_link[
            'John Bull'
        ]
        assert "Same author and series" == same_contributor_entry['title']
        expected_contributor_link = urllib.parse.quote('contributor/John Bull/eng/')
        assert same_contributor_href.endswith(expected_contributor_link)

        # Here's the sublane for recommendations from NoveList.
        [recommended_href, recommended_entry] = by_collection_link[
            'Similar titles recommended by NoveList'
        ]
        assert "Same author and series" == recommended_entry['title']
        work_url = "/works/%s/%s/" % (identifier.type, identifier.identifier)
        expected = urllib.parse.quote(work_url + 'recommendations')
        assert True == recommended_href.endswith(expected)

        # Finally, let's pass in a mock feed class so we can look at the
        # objects passed into AcquisitionFeed.groups().
        class Mock(object):
            @classmethod
            def groups(cls, **kwargs):
                cls.called_with = kwargs
                return Response("An OPDS feed")

        mock_api.setup_method(metadata)
        with self.request_context_with_library('/?entrypoint=Audio'):
            response = self.manager.work_controller.related(
                self.identifier.type, self.identifier.identifier,
                novelist_api=mock_api, feed_class=Mock
            )

        # The return value of Mock.groups was used as the response
        # to the incoming request.
        assert 200 == response.status_code
        assert "An OPDS feed" == response.get_data(as_text=True)

        # Verify that groups() was called with the arguments we expect.
        kwargs = Mock.called_with
        assert self._db == kwargs.pop('_db')
        assert self.manager.external_search == kwargs.pop('search_engine')
        assert "Related Books" == kwargs.pop('title')

        # We're passing in a FeaturedFacets. Each lane will have a chance
        # to adapt it to a faceting object appropriate for that lane.
        facets = kwargs.pop('facets')
        assert isinstance(facets, FeaturedFacets)
        assert AudiobooksEntryPoint == facets.entrypoint

        # We're generating a grouped feed using a RelatedBooksLane
        # that has three sublanes.
        lane = kwargs.pop('worklist')
        assert isinstance(lane, RelatedBooksLane)
        contributor_lane, novelist_lane, series_lane = lane.children

        assert isinstance(contributor_lane, ContributorLane)
        assert contributor == contributor_lane.contributor

        assert isinstance(novelist_lane, RecommendationLane)
        assert [recommended_identifier] == novelist_lane.recommendations

        assert isinstance(series_lane, SeriesLane)
        assert "Around the World" == series_lane.series

        # The Annotator is associated with the parent RelatedBooksLane.
        annotator = kwargs.pop('annotator')
        assert isinstance(annotator, LibraryAnnotator)
        assert self._default_library == annotator.library
        assert lane == annotator.lane

        # Checking the URL is difficult because it requires a request
        # context, _plus_ the DatabaseBackedFacets and Lane
        # created during the original request.
        library = self._default_library
        route, url_kwargs = lane.url_arguments
        url_kwargs.update(dict(list(facets.items())))
        with self.request_context_with_library(""):
            expect_url = self.manager.work_controller.url_for(
                route, lane_identifier=None,
                library_short_name=library.short_name,
                **url_kwargs
            )
        assert kwargs.pop('url') == expect_url

        # That's it!
        assert {} == kwargs

    def test_report_problem_get(self):
        with self.request_context_with_library("/"):
            response = self.manager.work_controller.report(self.identifier.type, self.identifier.identifier)
        assert 200 == response.status_code
        assert "text/uri-list" == response.headers['Content-Type']
        for i in Complaint.VALID_TYPES:
            assert i in response.get_data(as_text=True)

    def test_report_problem_post_success(self):
        error_type = random.choice(list(Complaint.VALID_TYPES))
        data = json.dumps({ "type": error_type,
                            "source": "foo",
                            "detail": "bar"}
        )
        with self.request_context_with_library("/", method="POST", data=data):
            response = self.manager.work_controller.report(self.identifier.type, self.identifier.identifier)
        assert 201 == response.status_code
        [complaint] = self.lp.complaints
        assert error_type == complaint.type
        assert "foo" == complaint.source
        assert "bar" == complaint.detail

    def test_series(self):
        # Test the ability of the series() method to generate an OPDS
        # feed representing all the books in a given series, subject
        # to an optional language and audience restriction.
        series_name = "Like As If Whatever Mysteries"

        # If no series is given, a ProblemDetail is returned.
        with self.request_context_with_library('/'):
            response = self.manager.work_controller.series("", None, None)
        assert 404 == response.status_code
        assert "http://librarysimplified.org/terms/problem/unknown-lane" == response.uri

        # Similarly if the pagination data is bad.
        with self.request_context_with_library('/?size=abc'):
            response = self.manager.work_controller.series(series_name, None, None)
            assert 400 == response.status_code

        # Or if the facet data is bad
        with self.request_context_with_library('/?order=nosuchorder'):
            response = self.manager.work_controller.series(series_name, None, None)
            assert 400 == response.status_code

        # Or if the search index isn't set up.
        self.assert_bad_search_index_gives_problem_detail(
            lambda: self.manager.work_controller.series(series_name, None, None)
        )

        # Set up the mock search engine to return our work no matter
        # what query it's given. The fact that this book isn't
        # actually in the series doesn't matter, since determining
        # that is the job of a non-mocked search engine.
        work = self._work(with_open_access_download=True)
        search_engine = self.manager.external_search
        search_engine.docs = {}
        search_engine.bulk_update([work])

        # If a series is provided, a feed for that series is returned.
        with self.request_context_with_library('/'):
            response = self.manager.work_controller.series(
                series_name, "eng,spa", "Children,Young Adult",
            )
        assert 200 == response.status_code
        feed = feedparser.parse(response.data)

        # The book we added to the mock search engine is in the feed.
        # This demonstrates that series() asks the search engine for
        # books to put in the feed.
        assert series_name == feed['feed']['title']
        [entry] = feed['entries']
        assert work.title == entry['title']

        # The feed has facet links.
        links = feed['feed']['links']
        facet_links = [link for link in links
                       if link['rel'] == 'http://opds-spec.org/facet']
        assert 9 == len(facet_links)

        # The facet link we care most about is the default sort order,
        # put into place by SeriesFacets.
        [series_position] = [
            x for x in facet_links if x['title'] == 'Series Position'
        ]
        assert 'Sort by' == series_position['opds:facetgroup']
        assert 'true' == series_position['opds:activefacet']

        # The feed was cached.
        cached = self._db.query(CachedFeed).one()
        assert CachedFeed.SERIES_TYPE == cached.type
        assert (
            'Like As If Whatever Mysteries-eng,spa-Children,Young+Adult' ==
            cached.unique_key)

        # At this point we don't want to generate real feeds anymore.
        # We can't do a real end-to-end test without setting up a real
        # search index, which is obnoxiously slow.
        #
        # Instead, we will mock AcquisitionFeed.page, and examine the
        # objects passed into it under different mock requests.
        #
        # Those objects, such as SeriesLane and SeriesFacets, are
        # tested elsewhere, in terms of their effects on search
        # objects such as Filter. Those search objects are the things
        # that are tested against a real search index (in core).
        #
        # We know from the previous test that any results returned
        # from the search engine are converted into an OPDS feed. Now
        # we verify that an incoming request results in the objects
        # we'd expect to use to generate the feed for that request.
        class Mock(object):
            @classmethod
            def page(cls, **kwargs):
                self.called_with = kwargs
                return Response("An OPDS feed")

        # Test a basic request with custom faceting, pagination, and a
        # language and audience restriction. This will exercise nearly
        # all the functionality we need to check.
        sort_key = ["sort", "pagination", "key"]
        with self.request_context_with_library(
            "/?order=title&size=100&key=%s" % json.dumps(sort_key)
        ):
            response = self.manager.work_controller.series(
                series_name, "some languages", "some audiences",
                feed_class=Mock
            )

        # The return value of Mock.page() is the response to the
        # incoming request.
        assert 200 == response.status_code
        assert "An OPDS feed" == response.get_data(as_text=True)

        kwargs = self.called_with
        assert self._db == kwargs.pop('_db')

        # The feed is titled after the series.
        assert series_name == kwargs.pop('title')

        # A SeriesLane was created to ask the search index for
        # matching works.
        lane = kwargs.pop('worklist')
        assert isinstance(lane, SeriesLane)
        assert self._default_library.id == lane.library_id
        assert series_name == lane.series
        assert ["some languages"] == lane.languages
        assert ["some audiences"] == lane.audiences

        # A SeriesFacets was created to add an extra sort order and
        # to provide additional search index constraints that can only
        # be provided through the faceting object.
        facets = kwargs.pop('facets')
        assert isinstance(facets, SeriesFacets)

        # The 'order' in the query string went into the SeriesFacets
        # object.
        assert "title" == facets.order

        # The 'key' and 'size' went into a SortKeyPagination object.
        pagination = kwargs.pop('pagination')
        assert isinstance(pagination, SortKeyPagination)
        assert sort_key == pagination.last_item_on_previous_page
        assert 100 == pagination.size

        # The lane, facets, and pagination were all taken into effect
        # when constructing the feed URL.
        annotator = kwargs.pop('annotator')
        assert lane == annotator.lane
        with self.request_context_with_library("/"):
            assert (
                annotator.feed_url(lane, facets=facets, pagination=pagination) ==
                kwargs.pop('url'))

        # The (mocked) search engine associated with the CirculationManager was
        # passed in.
        assert self.manager.external_search == kwargs.pop('search_engine')

        # No other arguments were passed into Mock.page.
        assert {} == kwargs

        # In the previous request we provided a custom sort order (by
        # title) Let's end with one more test to verify that series
        # position is the *default* sort order.
        with self.request_context_with_library("/"):
            response = self.manager.work_controller.series(
                series_name, None, None, feed_class=Mock
            )
        facets = self.called_with.pop('facets')
        assert isinstance(facets, SeriesFacets)
        assert "series" == facets.order


class TestOPDSFeedController(CirculationControllerTest):
    """Test most of the methods of OPDSFeedController.

    Methods relating to crawlable feeds are tested in
    TestCrawlableFeed.
    """

    BOOKS = list(CirculationControllerTest.BOOKS) + [
        ["english_2", "Totally American", "Uncle Sam", "eng", False],
        ["french_1", "Très Français", "Marianne", "fre", False],
    ]

    def test_feed(self):
        # Test the feed() method.

        # First, test some common error conditions.

        # Bad lane -> Problem detail
        with self.request_context_with_library("/"):
            response = self.manager.opds_feeds.feed(-1)
            assert 404 == response.status_code
            assert (
                "http://librarysimplified.org/terms/problem/unknown-lane" ==
                response.uri)

        # Bad faceting information -> Problem detail
        lane_id = self.english_adult_fiction.id
        with self.request_context_with_library("/?order=nosuchorder"):
            response = self.manager.opds_feeds.feed(lane_id)
            assert 400 == response.status_code
            assert (
                "http://librarysimplified.org/terms/problem/invalid-input" ==
                response.uri)

        # Bad pagination -> Problem detail
        with self.request_context_with_library("/?size=abc"):
            response = self.manager.opds_feeds.feed(lane_id)
            assert 400 == response.status_code
            assert (
                "http://librarysimplified.org/terms/problem/invalid-input" ==
                response.uri)

        # Bad search index setup -> Problem detail
        self.assert_bad_search_index_gives_problem_detail(
            lambda: self.manager.opds_feeds.feed(lane_id)
        )

        # Now let's make a real feed.

        # Set up configuration settings for links and entry points
        library = self._default_library
        for rel, value in [(LibraryAnnotator.TERMS_OF_SERVICE, "a"),
                           (LibraryAnnotator.PRIVACY_POLICY, "b"),
                           (LibraryAnnotator.COPYRIGHT, "c"),
                           (LibraryAnnotator.ABOUT, "d"),
                           ]:
            ConfigurationSetting.for_library(rel, library).value = value

        # Make a real OPDS feed and poke at it.
        with self.request_context_with_library(
            "/?entrypoint=Book&size=10"
        ):
            response = self.manager.opds_feeds.feed(
                self.english_adult_fiction.id
            )

            # The mock search index returned every book it has, without
            # respect to which books _ought_ to show up on this page.
            #
            # So we'll need to do a more detailed test to make sure
            # the right arguments are being passed _into_ the search
            # index.

            assert 200 == response.status_code
            assert (
                'max-age=%d' % Lane.MAX_CACHE_AGE
                in response.headers['Cache-Control']
            )
            feed = feedparser.parse(response.data)
            assert (set([x.title for x in self.works]) ==
                set([x['title'] for x in feed['entries']]))

            # But the rest of the feed looks good.
            links = feed['feed']['links']
            by_rel = dict()

            # Put the links into a data structure based on their rel values.
            for i in links:
                rel = i['rel']
                href = i['href']
                if isinstance(by_rel.get(rel), (bytes, str)):
                    by_rel[rel] = [by_rel[rel]]
                if isinstance(by_rel.get(rel), list):
                    by_rel[rel].append(href)
                else:
                    by_rel[i['rel']] = i['href']

            assert "a" == by_rel[LibraryAnnotator.TERMS_OF_SERVICE]
            assert "b" == by_rel[LibraryAnnotator.PRIVACY_POLICY]
            assert "c" == by_rel[LibraryAnnotator.COPYRIGHT]
            assert "d" == by_rel[LibraryAnnotator.ABOUT]

            next_link = by_rel['next']
            lane_str = str(lane_id)
            assert lane_str in next_link
            assert 'entrypoint=Book' in next_link
            assert 'size=10' in next_link
            last_item = self.works[-1]

            # The pagination key for the next page is derived from the
            # sort fields of the last work in the current page.
            expected_pagination_key = [
                last_item.sort_title, last_item.sort_author, last_item.id
            ]
            expect = "key=%s" % urllib.parse.quote_plus(
                json.dumps(expected_pagination_key)
            )
            assert expect in next_link

            search_link = by_rel['search']
            assert lane_str in search_link
            assert 'entrypoint=Book' in search_link

            shelf_link = by_rel['http://opds-spec.org/shelf']
            assert shelf_link.endswith('/loans/')

            facet_links = by_rel['http://opds-spec.org/facet']
            assert all(lane_str in x for x in facet_links)
            assert all('entrypoint=Book' in x for x in facet_links)
            assert any('order=title' in x for x in facet_links)
            assert any('order=author' in x for x in facet_links)

        # Now let's take a closer look at what this controller method
        # passes into AcquisitionFeed.page(), by mocking page().
        class Mock(object):
            @classmethod
            def page(cls, **kwargs):
                self.called_with = kwargs
                return Response("An OPDS feed")

        sort_key = ["sort", "pagination", "key"]
        with self.request_context_with_library(
            "/?entrypoint=Audio&size=36&key=%s&order=added" % (
                json.dumps(sort_key)
            )
        ):
            response = self.manager.opds_feeds.feed(
                self.english_adult_fiction.id, feed_class=Mock
            )

            # While we're in request context, generate the URL we
            # expect to be used for this feed.
            expect_url = self.controller.cdn_url_for(
                "feed", lane_identifier=lane_id,
                library_short_name=self._default_library.short_name,
                _facets=load_facets_from_request()
            )

        assert isinstance(response, Response)
        assert "An OPDS feed" == response.get_data(as_text=True)

        # Now check all the keyword arguments that were passed into
        # page().
        kwargs = self.called_with
        assert kwargs.pop('url') == expect_url
        assert self._db == kwargs.pop('_db')
        assert self.english_adult_fiction.display_name == kwargs.pop('title')
        assert self.english_adult_fiction == kwargs.pop('worklist')

        # Query string arguments were taken into account when
        # creating the Facets and Pagination objects.
        facets = kwargs.pop('facets')
        assert AudiobooksEntryPoint == facets.entrypoint
        assert 'added' == facets.order

        pagination = kwargs.pop('pagination')
        assert isinstance(pagination, SortKeyPagination)
        assert 36 == pagination.size
        assert sort_key == pagination.last_item_on_previous_page

        # The Annotator object was instantiated with the proper lane
        # and the newly created Facets object.
        annotator = kwargs.pop('annotator')
        assert self.english_adult_fiction == annotator.lane
        assert facets == annotator.facets

        # The ExternalSearchIndex associated with the
        # CirculationManager was passed in; that way we don't have to
        # connect to the search engine again.
        assert self.manager.external_search == kwargs.pop('search_engine')

        # No other arguments were passed into page().
        assert {} == kwargs

    def test_groups(self):
        # AcquisitionFeed.groups is tested in core/test_opds.py, and a
        # full end-to-end test would require setting up a real search
        # index, so we're just going to test that groups() (or, in one
        # case, page()) is called properly.
        library = self._default_library
        library.setting(library.MINIMUM_FEATURED_QUALITY).value = 0.15
        library.setting(library.FEATURED_LANE_SIZE).value = 2

        # Patron with root lane -> redirect to root lane
        lane = self._lane()
        lane.root_for_patron_type = ["1"]
        self.default_patron.external_type = "1"
        auth = dict(Authorization=self.valid_auth)
        with self.request_context_with_library("/", headers=auth):
            controller = self.manager.opds_feeds
            response = controller.groups(None)
            assert 302 == response.status_code
            expect_url = controller.cdn_url_for(
                'acquisition_groups',
                library_short_name=self._default_library.short_name,
                lane_identifier=lane.id, _external=True
            )
            assert response.headers['Location'] == expect_url

        # Bad lane -> Problem detail
        with self.request_context_with_library("/"):
            response = self.manager.opds_feeds.groups(-1)
            assert 404 == response.status_code
            assert (
                "http://librarysimplified.org/terms/problem/unknown-lane" ==
                response.uri)

        # Bad search index setup -> Problem detail
        self.assert_bad_search_index_gives_problem_detail(
            lambda: self.manager.opds_feeds.groups(None)
        )

        # A grouped feed has no pagination, and the FeaturedFacets
        # constructor never raises an exception. So we don't need to
        # test for those error conditions.

        # Now let's see what goes into groups()
        class Mock(object):
            @classmethod
            def groups(cls, **kwargs):
                # This method ends up being called most of the time
                # the grouped feed controller is activated.
                self.groups_called_with = kwargs
                self.page_called_with = None
                return Response("A grouped feed")

            @classmethod
            def page(cls, **kwargs):
                # But for lanes that have no children, this method
                # ends up being called instead.
                self.groups_called_with = None
                self.page_called_with = kwargs
                return Response("A paginated feed")

        # Earlier we tested an authenticated request for a patron with an
        # external type. Now try an authenticated request for a patron with
        # no external type, just to verify that nothing unusual happens
        # for that kind of patron.
        self.default_patron.external_type = None
        with self.request_context_with_library("/?entrypoint=Audio", headers=auth):
            # In default_config, there are no LARGE_COLLECTION_LANGUAGES,
            # so the sole top-level lane is "World Languages", which covers the
            # SMALL and TINY_COLLECTION_LANGUAGES.
            #
            # Thus, when we pass lane=None into groups(), we're asking for a
            # feed for the sole top-level lane, "World Languages".
            expect_lane = self.manager.opds_feeds.load_lane(None)
            assert "World Languages" == expect_lane.display_name

            # Ask for that feed.
            response = self.manager.opds_feeds.groups(None, feed_class=Mock)

            # The Response returned by Mock.groups() has been converted
            # into a Flask response.
            assert 200 == response.status_code
            assert "A grouped feed" == response.get_data(as_text=True)

            # While we're in request context, generate the URL we
            # expect to be used for this feed.
            expect_url = self.manager.opds_feeds.cdn_url_for(
                "acquisition_groups", lane_identifier=None,
                library_short_name=library.short_name,
                _facets=load_facets_from_request()
            )

        kwargs = self.groups_called_with
        assert self._db == kwargs.pop('_db')
        lane = kwargs.pop('worklist')
        assert expect_lane == lane
        assert lane.display_name == kwargs.pop('title')
        assert expect_url == kwargs.pop('url')

        # A FeaturedFacets object was loaded from library, lane and
        # request configuration.
        facets = kwargs.pop('facets')
        assert isinstance(facets, FeaturedFacets)
        assert AudiobooksEntryPoint == facets.entrypoint
        assert 0.15 == facets.minimum_featured_quality

        # A LibraryAnnotator object was created from the Lane and
        # Facets objects.
        annotator = kwargs.pop('annotator')
        assert lane == annotator.lane
        assert facets == annotator.facets

        # Finally, let's try again with a specific lane rather than
        # None.

        # This lane has no sublanes, so our call to groups()
        # is going to become a call to page().
        with self.request_context_with_library("/?entrypoint=Audio"):
            response = self.manager.opds_feeds.groups(
                self.english_adult_fiction.id, feed_class=Mock
            )

            # While we're in request context, generate the URL we
            # expect to be used for this feed.
            expect_url = self.manager.opds_feeds.cdn_url_for(
                "feed", lane_identifier=self.english_adult_fiction.id,
                library_short_name=library.short_name,
                _facets=load_facets_from_request()
            )

        assert self.english_adult_fiction == self.page_called_with.pop('worklist')

        # The canonical URL for this feed is a page-type URL, not a
        # groups-type URL.
        assert expect_url == self.page_called_with.pop('url')

        # The faceting and pagination objects are typical for the
        # first page of a paginated feed.
        pagination = self.page_called_with.pop('pagination')
        assert isinstance(pagination, SortKeyPagination)
        facets = self.page_called_with.pop('facets')
        assert isinstance(facets, Facets)

        # groups() was never called.
        assert None == self.groups_called_with

        # Give this lane a sublane, and the call to groups() goes
        # through as normal.
        sublane = self._lane(parent=self.english_adult_fiction)
        with self.request_context_with_library("/?entrypoint=Audio"):
            response = self.manager.opds_feeds.groups(
                self.english_adult_fiction.id, feed_class=Mock
            )
        assert None == self.page_called_with
        assert self.english_adult_fiction == self.groups_called_with.pop('worklist')
        assert isinstance(self.groups_called_with.pop('facets'), FeaturedFacets)
        assert 'pagination' not in self.groups_called_with

    def test_navigation(self):
        library = self._default_library
        lane = self.manager.top_level_lanes[library.id]
        lane = self._db.merge(lane)

        # Mock NavigationFeed.navigation so we can see the arguments going
        # into it.
        old_navigation = NavigationFeed.navigation
        @classmethod
        def mock_navigation(cls, *args, **kwargs):
            self.called_with = (args, kwargs)
            return old_navigation(*args, **kwargs)
        NavigationFeed.navigation = mock_navigation

        with self.request_context_with_library("/"):
            response = self.manager.opds_feeds.navigation(lane.id)

            feed = feedparser.parse(response.data)
            entries = feed['entries']
            # The default top-level lane is "World Languages", which contains
            # sublanes for English, Spanish, Chinese, and French.
            assert len(lane.sublanes) == len(entries)

        # A NavigationFacets object was created and passed in to
        # NavigationFeed.navigation().
        args, kwargs = self.called_with
        facets = kwargs['facets']
        assert isinstance(facets, NavigationFacets)
        NavigationFeed.navigation = old_navigation

    def _set_update_times(self):
        """Set the last update times so we can create a crawlable feed."""
        now = utc_now()

        def _set(work, time):
            """Set all fields used when calculating a work's update date for
            purposes of the crawlable feed.
            """
            work.last_update_time = time
            for lp in work.license_pools:
                lp.availability_time = time
        the_far_future = now + datetime.timedelta(hours=2)
        the_future = now + datetime.timedelta(hours=1)
        the_past = now - datetime.timedelta(hours=1)
        _set(self.english_2, now + datetime.timedelta(hours=2))
        _set(self.french_1, now + datetime.timedelta(hours=1))
        _set(self.english_1, now - datetime.timedelta(hours=1))
        self._db.commit()

    def mock_search(self, *args, **kwargs):
        self.called_with = (args, kwargs)

    def test_search_document(self):
        # When you invoke the search controller but don't specify a search
        # term, you get an OpenSearch document.
        with self.request_context_with_library("/"):
            response = self.manager.opds_feeds.search(None)
            assert response.headers['Content-Type'] == 'application/opensearchdescription+xml'
            assert "OpenSearchDescription" in response.get_data(as_text=True)

    def test_search(self):
        # Test the search() controller method.

        # Bad lane -> problem detail
        with self.request_context_with_library("/"):
            response = self.manager.opds_feeds.search(-1)
            assert 404 == response.status_code
            assert (
                "http://librarysimplified.org/terms/problem/unknown-lane" ==
                response.uri)

        # Bad pagination -> problem detail
        with self.request_context_with_library("/?size=abc"):
            response = self.manager.opds_feeds.search(None)
            assert 400 == response.status_code
            assert (
                "http://librarysimplified.org/terms/problem/invalid-input" ==
                response.uri)

        # Bad search index setup -> Problem detail
        self.assert_bad_search_index_gives_problem_detail(
            lambda: self.manager.opds_feeds.search(None)
        )

        # Loading the SearchFacets object from a request can't return
        # a problem detail, so we can't test that case.

        # The AcquisitionFeed.search method is tested in core, so we're
        # just going to test that appropriate values are passed into that
        # method:

        class Mock(object):
            @classmethod
            def search(cls, **kwargs):
                self.called_with = kwargs
                return "An OPDS feed"

        with self.request_context_with_library(
            "/?q=t&size=99&after=22&media=Music"
        ):
            # Try the top-level lane, "World Languages"
            expect_lane = self.manager.opds_feeds.load_lane(None)
            response = self.manager.opds_feeds.search(None, feed_class=Mock)

        kwargs = self.called_with
        assert self._db == kwargs.pop('_db')

        # Unlike other types of feeds, here the argument is called
        # 'lane' instead of 'worklist', because a Lane is the _only_
        # kind of WorkList that is currently searchable.
        lane = kwargs.pop('lane')
        assert expect_lane == lane
        query = kwargs.pop("query")
        assert "t" == query
        assert "Search" == kwargs.pop("title")
        assert self.manager.external_search == kwargs.pop('search_engine')

        # A SearchFacets object was loaded from library, lane and
        # request configuration.
        facets = kwargs.pop('facets')
        assert isinstance(facets, SearchFacets)

        # There are multiple possible entry points, and the request
        # didn't specify, so the SearchFacets object is configured to
        # search all of them.
        assert EverythingEntryPoint == facets.entrypoint

        # The "media" query string parameter -- used only by
        # SearchFacets -- was picked up.
        assert [Edition.MUSIC_MEDIUM] == facets.media

        # Information from the query string was used to make a
        # Pagination object.
        pagination = kwargs.pop('pagination')
        assert 22 == pagination.offset
        assert 99 == pagination.size

        # A LibraryAnnotator object was created from the Lane and
        # Facets objects.
        annotator = kwargs.pop('annotator')
        assert lane == annotator.lane
        assert facets == annotator.facets

        # Checking the URL is difficult because it requires a request
        # context, _plus_ the SearchFacets object created during the
        # original request.
        library = self._default_library
        with self.request_context_with_library(""):
            expect_url = self.manager.opds_feeds.url_for(
                'lane_search', lane_identifier=None,
                library_short_name=library.short_name,
                **dict(list(facets.items())), q=query
            )
        assert expect_url == kwargs.pop('url')

        # No other arguments were passed into search().
        assert {} == kwargs

        # When a specific entry point is selected, the SearchFacets
        # object is configured with that entry point alone.
        with self.request_context_with_library("/?entrypoint=Audio&q=t"):
            # Search a specific lane rather than the top-level.
            response = self.manager.opds_feeds.search(
                self.english_adult_fiction.id, feed_class=Mock
            )
            kwargs = self.called_with

            # We're searching that lane.
            assert self.english_adult_fiction == kwargs['lane']

            # And we get the entry point we asked for.
            assert AudiobooksEntryPoint == kwargs['facets'].entrypoint

        # When only a single entry point is enabled, it's used as the
        # default.
        library.setting(EntryPoint.ENABLED_SETTING).value = json.dumps(
            [AudiobooksEntryPoint.INTERNAL_NAME]
        )
        with self.request_context_with_library("/?q=t"):
            response = self.manager.opds_feeds.search(None, feed_class=Mock)
            assert AudiobooksEntryPoint == self.called_with['facets'].entrypoint

    def test_misconfigured_search(self):

        class BadSearch(CirculationManager):

            @property
            def setup_search(self):
                raise Exception("doomed!")

        circulation = BadSearch(self._db, testing=True)

        # An attempt to call FeedController.search() will return a
        # problem detail.
        with self.request_context_with_library("/?q=t"):
            problem = circulation.opds_feeds.search(None)
            assert REMOTE_INTEGRATION_FAILED.uri == problem.uri
            assert ('The search index for this site is not properly configured.' ==
                problem.detail)

    def test__qa_feed(self):
        # Test the _qa_feed() controller method.

        # First, mock the hook functions that do the actual work.
        wl = WorkList()
        wl.initialize(self.library)
        worklist_factory = MagicMock(return_value=wl)
        feed_method = MagicMock(return_value="an OPDS feed")

        m = self.manager.opds_feeds._qa_feed
        args = (feed_method, "QA test feed", "qa_feed", Facets,
                worklist_factory)

        # Bad search index setup -> Problem detail
        self.assert_bad_search_index_gives_problem_detail(
            lambda: m(*args)
        )

        # Bad faceting information -> Problem detail
        with self.request_context_with_library("/?order=nosuchorder"):
            response = m(*args)
            assert 400 == response.status_code
            assert (
                "http://librarysimplified.org/terms/problem/invalid-input" ==
                response.uri)

        # Now test success.
        with self.request_context_with_library("/"):
            expect_url = self.manager.opds_feeds.url_for(
                'qa_feed', library_short_name=self._default_library.short_name,
            )

            response = m(*args)

        # The response is the return value of feed_method().
        assert "an OPDS feed" == response

        # The worklist factory was called once, with the Library
        # associated with the request and a freshly created Facets
        # object.
        [factory_call] = worklist_factory.mock_calls
        (library, facets) = factory_call.args
        assert self._default_library == library
        assert isinstance(facets, Facets)
        assert EverythingEntryPoint == facets.entrypoint

        # feed_method was called once, with a variety of arguments.
        [call] = feed_method.mock_calls
        kwargs = call.kwargs

        assert self._db == kwargs.pop('_db')
        assert "QA test feed" == kwargs.pop("title")
        assert self.manager.external_search == kwargs.pop('search_engine')
        assert expect_url == kwargs.pop('url')

        # These feeds are never to be cached.
        assert CachedFeed.IGNORE_CACHE == kwargs.pop('max_age')

        # To improve performance, a Pagination object was created that
        # limits each lane in the test feed to a single Work.
        pagination = kwargs.pop('pagination')
        assert isinstance(pagination, Pagination)
        assert 1 == pagination.size

        # The WorkList returned by worklist_factory was passed into
        # feed_method.
        assert wl == kwargs.pop('worklist')

        # So was a LibraryAnnotator object created from that WorkList.
        annotator = kwargs.pop('annotator')
        assert isinstance(annotator, LibraryAnnotator)
        assert wl == annotator.lane
        assert None == annotator.facets

        # The Facets object used to initialize the feed is the same
        # one passed into worklist_factory.
        assert facets == kwargs.pop('facets')

        # No other arguments were passed into feed_method().
        assert {} == kwargs

    def test_qa_feed(self):
        # Verify that the qa_feed controller creates a factory for a
        # JackpotWorkList and passes it into _qa_feed.

        mock = MagicMock(return_value="an OPDS feed")
        self.manager.opds_feeds._qa_feed = mock

        response = self.manager.opds_feeds.qa_feed()
        [call] = mock.mock_calls
        kwargs = call.kwargs

        # For the most part, we're verifying that the expected values
        # are passed in to _qa_feed.
        assert AcquisitionFeed.groups == kwargs.pop('feed_method')
        assert JackpotFacets == kwargs.pop('facet_class')
        assert "qa_feed" == kwargs.pop("controller_name")
        assert "QA test feed" == kwargs.pop("feed_title")
        factory = kwargs.pop("worklist_factory")
        assert {} == kwargs

        # However, one of those expected values is a function. We need
        # to call that function to verify that it builds the
        # JackpotWorkList that distinguishes this _qa_feed call from
        # other calls.
        with self.request_context_with_library("/"):
            facets = load_facets_from_request(
                base_class=JackpotFacets,
                default_entrypoint=EverythingEntryPoint
            )

        worklist = factory(self._default_library, facets)
        assert isinstance(worklist, JackpotWorkList)

        # Each child of the JackpotWorkList is based on the
        # JackpotFacets object we passed in to the factory method.
        for child in worklist.children:
            assert facets == child.facets

    def test_qa_feed(self):
        # Verify that the qa_feed controller creates a factory for a
        # JackpotWorkList and passes it into _qa_feed.

        mock = MagicMock(return_value="an OPDS feed")
        self.manager.opds_feeds._qa_feed = mock

        response = self.manager.opds_feeds.qa_feed()
        [call] = mock.mock_calls
        kwargs = call.kwargs

        # For the most part, we're verifying that the expected values
        # are passed in to _qa_feed.
        assert AcquisitionFeed.groups == kwargs.pop('feed_factory')
        assert JackpotFacets == kwargs.pop('facet_class')
        assert "qa_feed" == kwargs.pop("controller_name")
        assert "QA test feed" == kwargs.pop("feed_title")
        factory = kwargs.pop("worklist_factory")
        assert {} == kwargs

        # However, one of those expected values is a function. We need
        # to call that function to verify that it builds the
        # JackpotWorkList that distinguishes this _qa_feed call from
        # other calls.
        with self.request_context_with_library("/"):
            facets = load_facets_from_request(
                base_class=JackpotFacets,
                default_entrypoint=EverythingEntryPoint
            )

        worklist = factory(self._default_library, facets)
        assert isinstance(worklist, JackpotWorkList)

        # Each child of the JackpotWorkList is based on the
        # JackpotFacets object we passed in to the factory method.
        for child in worklist.children:
            assert facets == child.facets

    def test_qa_series_feed(self):
        # Verify that the qa_series_feed controller creates a factory
        # for a generic WorkList and passes it into _qa_feed with
        # instructions to use HasSeriesFacets.

        mock = MagicMock(return_value="an OPDS feed")
        self.manager.opds_feeds._qa_feed = mock

        response = self.manager.opds_feeds.qa_series_feed()
        [call] = mock.mock_calls
        kwargs = call.kwargs

        # For the most part, we're verifying that the expected values
        # are passed in to _qa_feed.

        # Note that the feed_method is different from the one in qa_feed.
        # We want to generate an ungrouped feed rather than a grouped one.
        assert AcquisitionFeed.page == kwargs.pop('feed_factory')
        assert HasSeriesFacets == kwargs.pop('facet_class')
        assert "qa_series_feed" == kwargs.pop("controller_name")
        assert "QA series test feed" == kwargs.pop("feed_title")
        factory = kwargs.pop("worklist_factory")
        assert {} == kwargs

        # One of those expected values is a function. We need to call
        # that function to verify that it builds a generic WorkList
        # with no special features. Unlike with qa_feed, the
        # HasSeriesFacets object is not used to build the WorkList;
        # instead it directly modifies the Filter object used to
        # generate the query.
        worklist = factory(self._default_library, object())
        assert isinstance(worklist, WorkList)
        assert self._default_library.id == worklist.library_id


class TestCrawlableFeed(CirculationControllerTest):

    @contextmanager
    def mock_crawlable_feed(self):
        """Temporarily mock _crawlable_feed with something
        that records the arguments used to call it.
        """
        controller = self.manager.opds_feeds
        original = controller._crawlable_feed
        def mock(title, url, worklist, annotator=None,
                 feed_class=AcquisitionFeed):
            self._crawlable_feed_called_with = dict(
                title=title, url=url, worklist=worklist, annotator=annotator,
                feed_class=feed_class
            )
            return "An OPDS feed."
        controller._crawlable_feed = mock
        yield
        controller._crawlable_feed = original

    def test_crawlable_library_feed(self):
        # Test the creation of a crawlable feed for everything in
        # a library.
        controller = self.manager.opds_feeds
        library = self._default_library
        with self.request_context_with_library("/"):
            with self.mock_crawlable_feed():
                response = controller.crawlable_library_feed()
                expect_url = controller.cdn_url_for(
                    "crawlable_library_feed",
                    library_short_name=library.short_name,
                )

        # The response of the mock _crawlable_feed was returned as-is;
        # creating a proper Response object is the job of the real
        # _crawlable_feed.
        assert "An OPDS feed." == response

        # Verify that _crawlable_feed was called with the right arguments.
        kwargs = self._crawlable_feed_called_with
        assert expect_url == kwargs.pop('url')
        assert library.name == kwargs.pop('title')
        assert None == kwargs.pop('annotator')
        assert AcquisitionFeed == kwargs.pop('feed_class')

        # A CrawlableCollectionBasedLane has been set up to show
        # everything in any of the requested library's collections.
        lane = kwargs.pop('worklist')
        assert isinstance(lane, CrawlableCollectionBasedLane)
        assert library.id == lane.library_id
        assert [x.id for x in library.collections] == lane.collection_ids
        assert {} == kwargs

    def test_crawlable_collection_feed(self):
        # Test the creation of a crawlable feed for everything in
        # a collection.
        controller = self.manager.opds_feeds
        library = self._default_library

        collection = self._collection()

        # Bad collection name -> Problem detail.
        with self.app.test_request_context("/"):
            response = controller.crawlable_collection_feed(
                collection_name="No such collection"
            )
            assert NO_SUCH_COLLECTION == response

        # Unlike most of these controller methods, this one does not
        # require a library context.
        with self.app.test_request_context("/"):
            with self.mock_crawlable_feed():
                response = controller.crawlable_collection_feed(
                    collection_name=collection.name
                )
                expect_url = controller.cdn_url_for(
                    "crawlable_collection_feed",
                    collection_name=collection.name,
                )

        # The response of the mock _crawlable_feed was returned as-is;
        # creating a proper Response object is the job of the real
        # _crawlable_feed.
        assert "An OPDS feed." == response

        # Verify that _crawlable_feed was called with the right arguments.
        kwargs = self._crawlable_feed_called_with
        assert expect_url == kwargs.pop('url')
        assert collection.name == kwargs.pop('title')

        # A CrawlableCollectionBasedLane has been set up to show
        # everything in the requested collection.
        lane = kwargs.pop('worklist')
        assert isinstance(lane, CrawlableCollectionBasedLane)
        assert None == lane.library_id
        assert [collection.id] == lane.collection_ids

        # No specific Annotator as created to build the OPDS
        # feed. We'll be using the default for a request with no
        # library context--a CirculationManagerAnnotator.
        assert None == kwargs.pop('annotator')

        # A specific annotator _is_ created for an ODL collection:
        # A SharedCollectionAnnotator that knows about the Collection
        # _and_ the WorkList.
        collection.protocol = MockODLAPI.NAME
        with self.app.test_request_context("/"):
            with self.mock_crawlable_feed():
                response = controller.crawlable_collection_feed(
                    collection_name=collection.name
                )
        kwargs = self._crawlable_feed_called_with
        annotator = kwargs['annotator']
        assert isinstance(annotator, SharedCollectionAnnotator)
        assert collection == annotator.collection
        assert kwargs['worklist'] == annotator.lane

    def test_crawlable_list_feed(self):
        # Test the creation of a crawlable feed for everything in
        # a custom list.
        controller = self.manager.opds_feeds
        library = self._default_library

        customlist, ignore = self._customlist(num_entries=0)
        customlist.library = library

        other_list, ignore = self._customlist(num_entries=0)

        # List does not exist, or not associated with library ->
        # ProblemDetail
        for bad_name in ("Nonexistent list", other_list.name):
            with self.request_context_with_library("/"):
                with self.mock_crawlable_feed():
                    response = controller.crawlable_list_feed(bad_name)
                    assert NO_SUCH_LIST == response

        with self.request_context_with_library("/"):
            with self.mock_crawlable_feed():
                response = controller.crawlable_list_feed(customlist.name)
                expect_url = controller.cdn_url_for(
                    "crawlable_list_feed",
                    list_name=customlist.name,
                    library_short_name=library.short_name,
                )

        # The response of the mock _crawlable_feed was returned as-is;
        # creating a proper Response object is the job of the real
        # _crawlable_feed.
        assert "An OPDS feed." == response

        # Verify that _crawlable_feed was called with the right arguments.
        kwargs = self._crawlable_feed_called_with
        assert expect_url == kwargs.pop('url')
        assert customlist.name == kwargs.pop('title')
        assert None == kwargs.pop('annotator')
        assert AcquisitionFeed == kwargs.pop('feed_class')

        # A CrawlableCustomListBasedLane was created to fetch only
        # the works in the custom list.
        lane = kwargs.pop('worklist')
        assert isinstance(lane, CrawlableCustomListBasedLane)
        assert [customlist.id] == lane.customlist_ids
        assert {} == kwargs

    def test__crawlable_feed(self):
        # Test the helper method called by all other feed methods.
        self.page_called_with = None
        class MockFeed(object):
            @classmethod
            def page(cls, **kwargs):
                self.page_called_with = kwargs
                return Response("An OPDS feed")

        work = self._work(with_open_access_download=True)
        class MockLane(DynamicLane):
            def works(self, _db, facets, pagination, *args, **kwargs):
                # We need to call page_loaded() (normally called by
                # the search engine after obtaining real search
                # results), because OPDSFeed.page will call it if it
                # wasn't already called.
                #
                # It's not necessary for this test to call it with a
                # realistic value, but we might as well.
                results = [
                    MockSearchResult(
                        work.sort_title, work.sort_author, {}, work.id
                    )
                ]
                pagination.page_loaded(results)
                return [work]

        mock_lane = MockLane()
        mock_lane.initialize(None)
        in_kwargs = dict(
            title="Lane title",
            url="Lane URL",
            worklist=mock_lane,
            feed_class=MockFeed
        )

        # Bad pagination data -> problem detail
        with self.app.test_request_context("/?size=a"):
            response = self.manager.opds_feeds._crawlable_feed(**in_kwargs)
            assert isinstance(response, ProblemDetail)
            assert INVALID_INPUT.uri == response.uri
            assert None == self.page_called_with

        # Bad search engine -> problem detail
        self.assert_bad_search_index_gives_problem_detail(
            lambda: self.manager.opds_feeds._crawlable_feed(**in_kwargs)
        )

        # Good pagination data -> feed_class.page() is called.
        sort_key = ["sort", "pagination", "key"]
        with self.app.test_request_context(
                "/?size=23&key=%s" % json.dumps(sort_key)
        ):
            response = self.manager.opds_feeds._crawlable_feed(**in_kwargs)

        # The result of page() was served as an OPDS feed.
        assert 200 == response.status_code
        assert "An OPDS feed" == response.get_data(as_text=True)

        # Verify the arguments passed in to page().
        out_kwargs = self.page_called_with
        assert self._db == out_kwargs.pop('_db')
        assert (self.manager.opds_feeds.search_engine ==
            out_kwargs.pop('search_engine'))
        assert in_kwargs['worklist'] == out_kwargs.pop('worklist')
        assert in_kwargs['title'] == out_kwargs.pop('title')
        assert in_kwargs['url'] == out_kwargs.pop('url')

        # Since no annotator was provided and the request did not
        # happen in a library context, a generic
        # CirculationManagerAnnotator was created.
        annotator = out_kwargs.pop('annotator')
        assert isinstance(annotator, CirculationManagerAnnotator)
        assert mock_lane == annotator.lane

        # There's only one way to configure CrawlableFacets, so it's
        # sufficient to check that our faceting object is in fact a
        # CrawlableFacets.
        facets = out_kwargs.pop('facets')
        assert isinstance(facets, CrawlableFacets)

        # Verify that pagination was picked up from the request.
        pagination = out_kwargs.pop('pagination')
        assert isinstance(pagination, SortKeyPagination)
        assert sort_key == pagination.last_item_on_previous_page
        assert 23 == pagination.size

        # We're done looking at the arguments.
        assert {} == out_kwargs

        # If a custom Annotator is passed in to _crawlable_feed, it's
        # propagated to the page() call.
        mock_annotator = object()
        with self.app.test_request_context("/"):
            response = self.manager.opds_feeds._crawlable_feed(
                annotator=mock_annotator, **in_kwargs
            )
            assert mock_annotator == self.page_called_with['annotator']

        # Finally, remove the mock feed class and verify that a real OPDS
        # feed is generated from the result of MockLane.works()
        del in_kwargs['feed_class']
        with self.request_context_with_library("/"):
            response = self.manager.opds_feeds._crawlable_feed(**in_kwargs)
        feed = feedparser.parse(response.data)

        # There is one entry with the expected title.
        [entry] = feed['entries']
        assert entry['title'] == work.title


class TestMARCRecordController(CirculationControllerTest):
    def test_download_page_with_exporter_and_files(self):
        now = utc_now()
        yesterday = now - datetime.timedelta(days=1)

        library = self._default_library
        lane = self._lane(display_name="Test Lane")

        exporter = self._external_integration(
            ExternalIntegration.MARC_EXPORT, ExternalIntegration.CATALOG_GOAL,
            libraries=[self._default_library])

        rep1, ignore = create(
            self._db, Representation,
            url="http://mirror1", mirror_url="http://mirror1",
            media_type=Representation.MARC_MEDIA_TYPE,
            mirrored_at=now)
        cache1, ignore = create(
            self._db, CachedMARCFile,
            library=self._default_library, lane=None,
            representation=rep1, end_time=now)

        rep2, ignore = create(
            self._db, Representation,
            url="http://mirror2", mirror_url="http://mirror2",
            media_type=Representation.MARC_MEDIA_TYPE,
            mirrored_at=yesterday)
        cache2, ignore = create(
            self._db, CachedMARCFile,
            library=self._default_library, lane=lane,
            representation=rep2, end_time=yesterday)

        rep3, ignore = create(
            self._db, Representation,
            url="http://mirror3", mirror_url="http://mirror3",
            media_type=Representation.MARC_MEDIA_TYPE,
            mirrored_at=now)
        cache3, ignore = create(
            self._db, CachedMARCFile,
            library=self._default_library, lane=None,
            representation=rep3, end_time=now,
            start_time=yesterday)


        with self.request_context_with_library("/"):
            response = self.manager.marc_records.download_page()
            assert 200 == response.status_code
            html = response.get_data(as_text=True)
            assert ("Download MARC files for %s" % library.name) in html

            assert "<h3>All Books</h3>" in html
            assert '<a href="http://mirror1">Full file - last updated %s</a>' % now.strftime("%B %-d, %Y") in html
            assert "<h4>Update-only files</h4>" in html
            assert '<a href="http://mirror3">Updates from %s to %s</a>' % (yesterday.strftime("%B %-d, %Y"), now.strftime("%B %-d, %Y")) in html

            assert '<h3>Test Lane</h3>' in html
            assert '<a href="http://mirror2">Full file - last updated %s</a>' % yesterday.strftime("%B %-d, %Y") in html

    def test_download_page_with_exporter_but_no_files(self):
        now = utc_now()
        yesterday = now - datetime.timedelta(days=1)

        library = self._default_library

        exporter = self._external_integration(
            ExternalIntegration.MARC_EXPORT, ExternalIntegration.CATALOG_GOAL,
            libraries=[self._default_library])

        with self.request_context_with_library("/"):
            response = self.manager.marc_records.download_page()
            assert 200 == response.status_code
            html = response.get_data(as_text=True)
            assert ("Download MARC files for %s" % library.name) in html
            assert "MARC files aren't ready" in html

    def test_download_page_no_exporter(self):
        library = self._default_library

        with self.request_context_with_library("/"):
            response = self.manager.marc_records.download_page()
            assert 200 == response.status_code
            html = response.get_data(as_text=True)
            assert ("Download MARC files for %s" % library.name) in html
            assert ("No MARC exporter is currently configured") in html

        # If the exporter was deleted after some MARC files were cached,
        # they will still be available to download.
        now = utc_now()
        rep, ignore = create(
            self._db, Representation,
            url="http://mirror1", mirror_url="http://mirror1",
            media_type=Representation.MARC_MEDIA_TYPE,
            mirrored_at=now)
        cache, ignore = create(
            self._db, CachedMARCFile,
            library=self._default_library, lane=None,
            representation=rep, end_time=now)

        with self.request_context_with_library("/"):
            response = self.manager.marc_records.download_page()
            assert 200 == response.status_code
            html = response.get_data(as_text=True)
            assert ("Download MARC files for %s" % library.name) in html
            assert "No MARC exporter is currently configured" in html
            assert '<h3>All Books</h3>' in html
            assert '<a href="http://mirror1">Full file - last updated %s</a>' % now.strftime("%B %-d, %Y") in html


class TestAnalyticsController(CirculationControllerTest):
    def setup_method(self):
        super(TestAnalyticsController, self).setup_method()
        [self.lp] = self.english_1.license_pools
        self.identifier = self.lp.identifier

    def test_track_event(self):
        integration, ignore = create(
            self._db, ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol="core.local_analytics_provider",
        )
        integration.setting(
            LocalAnalyticsProvider.LOCATION_SOURCE
        ).value = LocalAnalyticsProvider.LOCATION_SOURCE_NEIGHBORHOOD
        self.manager.analytics = Analytics(self._db)

        with self.request_context_with_library("/"):
            response = self.manager.analytics_controller.track_event(self.identifier.type, self.identifier.identifier, "invalid_type")
            assert 400 == response.status_code
            assert INVALID_ANALYTICS_EVENT_TYPE.uri == response.uri

        # If there is no active patron, or if the patron has no
        # associated neighborhood, the CirculationEvent is created
        # with no location.
        patron = self._patron()
        for request_patron in (None, patron):
            with self.request_context_with_library("/"):
                flask.request.patron = request_patron
                response = self.manager.analytics_controller.track_event(
                    self.identifier.type, self.identifier.identifier,
                    "open_book"
                )
                assert 200 == response.status_code

                circulation_event = get_one(
                    self._db, CirculationEvent,
                    type="open_book",
                    license_pool=self.lp
                )
                assert None == circulation_event.location
                self._db.delete(circulation_event)

        # If the patron has an associated neighborhood, and the
        # analytics controller is set up to use patron neighborhood as
        # event location, then the CirculationEvent is created with
        # that neighborhood as its location.
        patron.neighborhood = "Mars Grid 4810579"
        with self.request_context_with_library("/"):
            flask.request.patron = patron
            response = self.manager.analytics_controller.track_event(
                self.identifier.type, self.identifier.identifier, "open_book"
            )
            assert 200 == response.status_code

            circulation_event = get_one(
                self._db, CirculationEvent,
                type="open_book",
                license_pool=self.lp
            )
            assert patron.neighborhood == circulation_event.location
            self._db.delete(circulation_event)

class TestDeviceManagementProtocolController(ControllerTest):

    def setup_method(self):
        super(TestDeviceManagementProtocolController, self).setup_method()
        self.initialize_adobe(self.library, self.libraries)
        self.auth = dict(Authorization=self.valid_auth)

        # Since our library doesn't have its Adobe configuration
        # enabled, the Device Management Protocol controller has not
        # been enabled.
        assert None == self.manager.adobe_device_management

        # Set up the Adobe configuration for this library and
        # reload the CirculationManager configuration.
        self.manager.setup_adobe_vendor_id(self._db, self.library)
        self.manager.load_settings()

        # Now the controller is enabled and we can use it in this
        # test.
        self.controller = self.manager.adobe_device_management

    def _create_credential(self):
        """Associate a credential with the default patron which
        can have Adobe device identifiers associated with it,
        """
        return self._credential(
            DataSource.INTERNAL_PROCESSING,
            ShortClientTokenUtility.ADOBE_ACCOUNT_ID_PATRON_IDENTIFIER,
            self.default_patron
        )

    def test_link_template_header(self):
        """Test the value of the Link-Template header used in
        device_id_list_handler.
        """
        with self.request_context_with_library("/"):
            headers = self.controller.link_template_header
            assert 1 == len(headers)
            template = headers['Link-Template']
            expected_url = url_for("adobe_drm_device", library_short_name=self.library.short_name, device_id="{id}", _external=True)
            expected_url = expected_url.replace("%7Bid%7D", "{id}")
            assert '<%s>; rel="item"' % expected_url == template

    def test__request_handler_failure(self):
        """You cannot create a DeviceManagementRequestHandler
        without providing a patron.
        """
        result = self.controller._request_handler(None)

        assert isinstance(result, ProblemDetail)
        assert INVALID_CREDENTIALS.uri == result.uri
        assert "No authenticated patron" == result.detail

    def test_device_id_list_handler_post_success(self):
        # The patron has no credentials, and thus no registered devices.
        assert [] == self.default_patron.credentials
        headers = dict(self.auth)
        headers['Content-Type'] = self.controller.DEVICE_ID_LIST_MEDIA_TYPE
        with self.request_context_with_library(
            "/", method='POST', headers=headers, data="device"
        ):
            self.controller.authenticated_patron_from_request()
            response = self.controller.device_id_list_handler()
            assert 200 == response.status_code

            # We just registered a new device with the patron. This
            # automatically created an appropriate Credential for
            # them.
            [credential] = self.default_patron.credentials
            assert DataSource.INTERNAL_PROCESSING == credential.data_source.name
            assert (ShortClientTokenUtility.ADOBE_ACCOUNT_ID_PATRON_IDENTIFIER ==
                credential.type)

            assert (['device'] ==
                [x.device_identifier for x in credential.drm_device_identifiers])

    def test_device_id_list_handler_get_success(self):
        credential = self._create_credential()
        credential.register_drm_device_identifier("device1")
        credential.register_drm_device_identifier("device2")
        with self.request_context_with_library("/", headers=self.auth):
            self.controller.authenticated_patron_from_request()
            response = self.controller.device_id_list_handler()
            assert 200 == response.status_code

            # We got a list of device IDs.
            assert (self.controller.DEVICE_ID_LIST_MEDIA_TYPE ==
                response.headers['Content-Type'])
            assert "device1\ndevice2" == response.get_data(as_text=True)

            # We got a URL Template (see test_link_template_header())
            # that explains how to address any particular device ID.
            expect = self.controller.link_template_header
            for k, v in list(expect.items()):
                assert response.headers[k] == v

    def device_id_list_handler_bad_auth(self):
        with self.request_context_with_library("/"):
            self.controller.authenticated_patron_from_request()
            response = self.manager.adobe_vendor_id.device_id_list_handler()
            assert isinstance(response, ProblemDetail)
            assert 401 == response.status_code

    def device_id_list_handler_bad_method(self):
        with self.request_context_with_library(
            "/", method='DELETE', headers=self.auth
        ):
            self.controller.authenticated_patron_from_request()
            response = self.controller.device_id_list_handler()
            assert isinstance(response, ProblemDetail)
            assert 405 == response.status_code

    def test_device_id_list_handler_too_many_simultaneous_registrations(self):
        # We only allow registration of one device ID at a time.
        headers = dict(self.auth)
        headers['Content-Type'] = self.controller.DEVICE_ID_LIST_MEDIA_TYPE
        with self.request_context_with_library(
            "/", method='POST', headers=headers, data="device1\ndevice2"
        ):
            self.controller.authenticated_patron_from_request()
            response = self.controller.device_id_list_handler()
            assert 413 == response.status_code
            assert "You may only register one device ID at a time." == response.detail

    def test_device_id_list_handler_wrong_media_type(self):
        headers = dict(self.auth)
        headers['Content-Type'] = "text/plain"
        with self.request_context_with_library(
            "/", method='POST', headers=headers, data="device1\ndevice2"
        ):
            self.controller.authenticated_patron_from_request()
            response = self.controller.device_id_list_handler()
            assert 415 == response.status_code
            assert ("Expected vnd.librarysimplified/drm-device-id-list document." ==
                response.detail)

    def test_device_id_handler_success(self):
        credential = self._create_credential()
        credential.register_drm_device_identifier("device")

        with self.request_context_with_library(
                "/", method='DELETE', headers=self.auth
        ):
            patron = self.controller.authenticated_patron_from_request()
            response = self.controller.device_id_handler("device")
            assert 200 == response.status_code

    def test_device_id_handler_bad_auth(self):
        with self.request_context_with_library("/", method='DELETE'):
            with temp_config() as config:
                config[Configuration.INTEGRATIONS] = {
                    "Circulation Manager" : { "url" : "http://foo/" }
                }
                patron = self.controller.authenticated_patron_from_request()
                response = self.controller.device_id_handler("device")
            assert isinstance(response, ProblemDetail)
            assert 401 == response.status_code

    def test_device_id_handler_bad_method(self):
        with self.request_context_with_library("/", method='POST', headers=self.auth):
            patron = self.controller.authenticated_patron_from_request()
            response = self.controller.device_id_handler("device")
            assert isinstance(response, ProblemDetail)
            assert 405 == response.status_code
            assert "Only DELETE is supported." == response.detail


class TestODLNotificationController(ControllerTest):
    """Test that an ODL distributor can notify the circulation manager
    when a loan's status changes."""

    def test_notify_success(self):
        collection = MockODLAPI.mock_collection(self._db)
        patron = self._patron()
        pool = self._licensepool(None, collection=collection)
        pool.licenses_owned = 10
        pool.licenses_available = 5
        loan, ignore = pool.loan_to(patron)
        loan.external_identifier = self._str

        with self.request_context_with_library("/", method="POST"):
            flask.request.data = json.dumps({
               "id": loan.external_identifier,
               "status": "revoked",
            })
            response = self.manager.odl_notification_controller.notify(
                loan.id)
            assert 200 == response.status_code

            # The pool's availability has been updated.
            api = self.manager.circulation_apis[self._default_library.id].api_for_license_pool(loan.license_pool)
            assert [loan.license_pool] == api.availability_updated_for

    def test_notify_errors(self):
        # No loan.
        with self.request_context_with_library("/", method="POST"):
            response = self.manager.odl_notification_controller.notify(self._str)
            assert NO_ACTIVE_LOAN.uri == response.uri

        # Loan from a non-ODL collection.
        patron = self._patron()
        pool = self._licensepool(None)
        loan, ignore = pool.loan_to(patron)
        loan.external_identifier = self._str

        with self.request_context_with_library("/", method="POST"):
            response = self.manager.odl_notification_controller.notify(loan.id)
            assert INVALID_LOAN_FOR_ODL_NOTIFICATION == response

class TestSharedCollectionController(ControllerTest):
    """Test that other circ managers can register to borrow books
    from a shared collection."""

    def setup_method(self):
        self.setup_circulation_manager = False
        super(TestSharedCollectionController, self).setup_method()
        from api.odl import ODLAPI
        self.collection = self._collection(protocol=ODLAPI.NAME)
        self._default_library.collections = [self.collection]
        self.client, ignore = IntegrationClient.register(self._db, "http://library.org")
        self.app.manager = self.circulation_manager_setup(self._db)
        self.work = self._work(
            with_license_pool=True, collection=self.collection
        )
        self.pool = self.work.license_pools[0]
        [self.delivery_mechanism] = self.pool.delivery_mechanisms

    @contextmanager
    def request_context_with_client(self, route, *args, **kwargs):
        if 'client' in kwargs:
            client = kwargs.pop('client')
        else:
            client = self.client
        if 'headers' in kwargs:
            headers = kwargs.pop('headers')
        else:
            headers = dict()
        headers['Authorization'] = "Bearer " + base64.b64encode(client.shared_secret)
        kwargs['headers'] = headers
        with self.app.test_request_context(route, *args, **kwargs) as c:
            yield c

    def test_info(self):
        with self.app.test_request_context("/"):
            collection = self.manager.shared_collection_controller.info(self._str)
            assert NO_SUCH_COLLECTION == collection

            response = self.manager.shared_collection_controller.info(self.collection.name)
            assert 200 == response.status_code
            assert response.headers.get("Content-Type").startswith("application/opds+json")
            links = json.loads(response.get_data(as_text=True)).get("links")
            [register_link] = [link for link in links if link.get("rel") == "register"]
            assert "/collections/%s/register" % self.collection.name in register_link.get("href")

    def test_load_collection(self):
        with self.app.test_request_context("/"):
            collection = self.manager.shared_collection_controller.load_collection(self._str)
            assert NO_SUCH_COLLECTION == collection

            collection = self.manager.shared_collection_controller.load_collection(self.collection.name)
            assert self.collection == collection

    def test_register(self):
        with self.app.test_request_context("/"):
            api = self.app.manager.shared_collection_controller.shared_collection
            flask.request.form = ImmutableMultiDict([("url", "http://test")])

            api.queue_register(InvalidInputException())
            response = self.manager.shared_collection_controller.register(self.collection.name)
            assert 400 == response.status_code
            assert INVALID_REGISTRATION.uri == response.uri

            api.queue_register(AuthorizationFailedException())
            response = self.manager.shared_collection_controller.register(self.collection.name)
            assert 401 == response.status_code
            assert INVALID_CREDENTIALS.uri == response.uri

            api.queue_register(RemoteInitiatedServerError("Error", "Service"))
            response = self.manager.shared_collection_controller.register(self.collection.name)
            assert 502 == response.status_code
            assert INTEGRATION_ERROR.uri == response.uri

            api.queue_register(dict(shared_secret="secret"))
            response = self.manager.shared_collection_controller.register(self.collection.name)
            assert 200 == response.status_code
            assert "secret" == json.loads(response.get_data(as_text=True)).get("shared_secret")

    def test_loan_info(self):
        now = utc_now()
        tomorrow = utc_now() + datetime.timedelta(days=1)

        other_client, ignore = IntegrationClient.register(self._db, "http://otherlibrary")
        other_client_loan, ignore = create(
            self._db, Loan, license_pool=self.pool, integration_client=other_client,
        )

        ignore, other_pool = self._edition(
            with_license_pool=True, collection=self._collection(),
        )
        other_pool_loan, ignore = create(
            self._db, Loan, license_pool=other_pool, integration_client=self.client,
        )

        loan, ignore = create(
            self._db, Loan, license_pool=self.pool, integration_client=self.client,
            start=now, end=tomorrow,
        )
        with self.request_context_with_client("/"):
            # This loan doesn't exist.
            response = self.manager.shared_collection_controller.loan_info(self.collection.name, 1234567)
            assert LOAN_NOT_FOUND == response

            # This loan belongs to a different library.
            response = self.manager.shared_collection_controller.loan_info(self.collection.name, other_client_loan.id)
            assert LOAN_NOT_FOUND == response

            # This loan's pool belongs to a different collection.
            response = self.manager.shared_collection_controller.loan_info(self.collection.name, other_pool_loan.id)
            assert LOAN_NOT_FOUND == response

            # This loan is ours.
            response = self.manager.shared_collection_controller.loan_info(self.collection.name, loan.id)
            assert 200 == response.status_code
            feed = feedparser.parse(response.data)
            [entry] = feed.get("entries")
            availability = entry.get("opds_availability")
            since = availability.get("since")
            until = availability.get("until")
            assert datetime.datetime.strftime(now, "%Y-%m-%dT%H:%M:%S+00:00") == since
            assert datetime.datetime.strftime(tomorrow, "%Y-%m-%dT%H:%M:%S+00:00") == until
            [revoke_url] = [link.get("href") for link in entry.get("links") if link.get("rel") == "http://librarysimplified.org/terms/rel/revoke"]
            assert "/collections/%s/loans/%s/revoke" % (self.collection.name, loan.id) in revoke_url
            [fulfill_url] = [link.get("href") for link in entry.get("links") if link.get("rel") == "http://opds-spec.org/acquisition"]
            assert "/collections/%s/loans/%s/fulfill/%s" % (self.collection.name, loan.id, self.delivery_mechanism.delivery_mechanism.id) in fulfill_url
            [self_url] = [link.get("href") for link in entry.get("links") if link.get("rel") == "self"]
            assert "/collections/%s/loans/%s" % (self.collection.name, loan.id)

    def test_borrow(self):
        now = utc_now()
        tomorrow = utc_now() + datetime.timedelta(days=1)
        loan, ignore = create(
            self._db, Loan, license_pool=self.pool, integration_client=self.client,
            start=now, end=tomorrow,
        )

        hold, ignore = create(
            self._db, Hold, license_pool=self.pool, integration_client=self.client,
            start=now, end=tomorrow,
        )

        no_pool = self._identifier()
        with self.request_context_with_client("/"):
            response = self.manager.shared_collection_controller.borrow(self.collection.name, no_pool.type, no_pool.identifier, None)
            assert NO_LICENSES.uri == response.uri

            api = self.app.manager.shared_collection_controller.shared_collection

            # Attempt to borrow without a previous hold.
            api.queue_borrow(AuthorizationFailedException())
            response = self.manager.shared_collection_controller.borrow(self.collection.name, self.pool.identifier.type, self.pool.identifier.identifier, None)
            assert INVALID_CREDENTIALS.uri == response.uri

            api.queue_borrow(CannotLoan())
            response = self.manager.shared_collection_controller.borrow(self.collection.name, self.pool.identifier.type, self.pool.identifier.identifier, None)
            assert CHECKOUT_FAILED.uri == response.uri

            api.queue_borrow(NoAvailableCopies())
            response = self.manager.shared_collection_controller.borrow(self.collection.name, self.pool.identifier.type, self.pool.identifier.identifier, None)
            assert NO_AVAILABLE_LICENSE.uri == response.uri

            api.queue_borrow(RemoteIntegrationException("error!", "service"))
            response = self.manager.shared_collection_controller.borrow(self.collection.name, self.pool.identifier.type, self.pool.identifier.identifier, None)
            assert INTEGRATION_ERROR.uri == response.uri

            api.queue_borrow(loan)
            response = self.manager.shared_collection_controller.borrow(self.collection.name, self.pool.identifier.type, self.pool.identifier.identifier, None)
            assert 201 == response.status_code
            feed = feedparser.parse(response.data)
            [entry] = feed.get("entries")
            availability = entry.get("opds_availability")
            since = availability.get("since")
            until = availability.get("until")
            assert datetime.datetime.strftime(now, "%Y-%m-%dT%H:%M:%S+00:00") == since
            assert datetime.datetime.strftime(tomorrow, "%Y-%m-%dT%H:%M:%S+00:00") == until
            assert "available" == availability.get("status")
            [revoke_url] = [link.get("href") for link in entry.get("links") if link.get("rel") == "http://librarysimplified.org/terms/rel/revoke"]
            assert "/collections/%s/loans/%s/revoke" % (self.collection.name, loan.id) in revoke_url
            [fulfill_url] = [link.get("href") for link in entry.get("links") if link.get("rel") == "http://opds-spec.org/acquisition"]
            assert "/collections/%s/loans/%s/fulfill/%s" % (self.collection.name, loan.id, self.delivery_mechanism.delivery_mechanism.id) in fulfill_url
            [self_url] = [link.get("href") for link in entry.get("links") if link.get("rel") == "self"]
            assert "/collections/%s/loans/%s" % (self.collection.name, loan.id)

            # Now try to borrow when we already have a previous hold.
            api.queue_borrow(AuthorizationFailedException())
            response = self.manager.shared_collection_controller.borrow(self.collection.name, self.pool.identifier.type, self.pool.identifier.identifier, hold.id)
            assert INVALID_CREDENTIALS.uri == response.uri

            api.queue_borrow(CannotLoan())
            response = self.manager.shared_collection_controller.borrow(self.collection.name, None, None, hold.id)
            assert CHECKOUT_FAILED.uri == response.uri

            api.queue_borrow(NoAvailableCopies())
            response = self.manager.shared_collection_controller.borrow(self.collection.name, None, None, hold.id)
            assert NO_AVAILABLE_LICENSE.uri == response.uri

            api.queue_borrow(RemoteIntegrationException("error!", "service"))
            response = self.manager.shared_collection_controller.borrow(self.collection.name, None, None, hold.id)
            assert INTEGRATION_ERROR.uri == response.uri

            api.queue_borrow(loan)
            response = self.manager.shared_collection_controller.borrow(self.collection.name, None, None, hold.id)
            assert 201 == response.status_code
            feed = feedparser.parse(response.data)
            [entry] = feed.get("entries")
            availability = entry.get("opds_availability")
            since = availability.get("since")
            until = availability.get("until")
            assert "available" == availability.get("status")
            assert datetime.datetime.strftime(now, "%Y-%m-%dT%H:%M:%S+00:00") == since
            assert datetime.datetime.strftime(tomorrow, "%Y-%m-%dT%H:%M:%S+00:00") == until
            [revoke_url] = [link.get("href") for link in entry.get("links") if link.get("rel") == "http://librarysimplified.org/terms/rel/revoke"]
            assert "/collections/%s/loans/%s/revoke" % (self.collection.name, loan.id) in revoke_url
            [fulfill_url] = [link.get("href") for link in entry.get("links") if link.get("rel") == "http://opds-spec.org/acquisition"]
            assert "/collections/%s/loans/%s/fulfill/%s" % (self.collection.name, loan.id, self.delivery_mechanism.delivery_mechanism.id) in fulfill_url
            [self_url] = [link.get("href") for link in entry.get("links") if link.get("rel") == "self"]
            assert "/collections/%s/loans/%s" % (self.collection.name, loan.id)

            # Now try to borrow, but actually get a hold.
            api.queue_borrow(hold)
            response = self.manager.shared_collection_controller.borrow(self.collection.name, self.pool.identifier.type, self.pool.identifier.identifier, None)
            assert 201 == response.status_code
            feed = feedparser.parse(response.data)
            [entry] = feed.get("entries")
            availability = entry.get("opds_availability")
            since = availability.get("since")
            until = availability.get("until")
            assert datetime.datetime.strftime(now, "%Y-%m-%dT%H:%M:%S+00:00") == since
            assert datetime.datetime.strftime(tomorrow, "%Y-%m-%dT%H:%M:%S+00:00") == until
            assert "reserved" == availability.get("status")
            [revoke_url] = [link.get("href") for link in entry.get("links") if link.get("rel") == "http://librarysimplified.org/terms/rel/revoke"]
            assert "/collections/%s/holds/%s/revoke" % (self.collection.name, hold.id) in revoke_url
            assert [] == [link.get("href") for link in entry.get("links") if link.get("rel") == "http://opds-spec.org/acquisition"]
            [self_url] = [link.get("href") for link in entry.get("links") if link.get("rel") == "self"]
            assert "/collections/%s/holds/%s" % (self.collection.name, hold.id)

    def test_revoke_loan(self):
        now = utc_now()
        tomorrow = utc_now() + datetime.timedelta(days=1)
        loan, ignore = create(
            self._db, Loan, license_pool=self.pool, integration_client=self.client,
            start=now, end=tomorrow,
        )

        other_client, ignore = IntegrationClient.register(self._db, "http://otherlibrary")
        other_client_loan, ignore = create(
            self._db, Loan, license_pool=self.pool, integration_client=other_client,
        )

        ignore, other_pool = self._edition(
            with_license_pool=True, collection=self._collection(),
        )
        other_pool_loan, ignore = create(
            self._db, Loan, license_pool=other_pool, integration_client=self.client,
        )

        with self.request_context_with_client("/"):
            response = self.manager.shared_collection_controller.revoke_loan(self.collection.name, other_pool_loan.id)
            assert LOAN_NOT_FOUND.uri == response.uri

            response = self.manager.shared_collection_controller.revoke_loan(self.collection.name, other_client_loan.id)
            assert LOAN_NOT_FOUND.uri == response.uri

            api = self.app.manager.shared_collection_controller.shared_collection

            api.queue_revoke_loan(AuthorizationFailedException())
            response = self.manager.shared_collection_controller.revoke_loan(self.collection.name, loan.id)
            assert INVALID_CREDENTIALS.uri == response.uri

            api.queue_revoke_loan(CannotReturn())
            response = self.manager.shared_collection_controller.revoke_loan(self.collection.name, loan.id)
            assert COULD_NOT_MIRROR_TO_REMOTE.uri == response.uri

            api.queue_revoke_loan(NotCheckedOut())
            response = self.manager.shared_collection_controller.revoke_loan(self.collection.name, loan.id)
            assert NO_ACTIVE_LOAN.uri == response.uri

    def test_fulfill(self):
        now = utc_now()
        tomorrow = utc_now() + datetime.timedelta(days=1)
        loan, ignore = create(
            self._db, Loan, license_pool=self.pool, integration_client=self.client,
            start=now, end=tomorrow,
        )

        ignore, other_pool = self._edition(
            with_license_pool=True, collection=self._collection(),
        )
        other_pool_loan, ignore = create(
            self._db, Loan, license_pool=other_pool, integration_client=self.client,
        )

        with self.request_context_with_client("/"):
            response = self.manager.shared_collection_controller.fulfill(self.collection.name, other_pool_loan.id, None)
            assert LOAN_NOT_FOUND.uri == response.uri

            api = self.app.manager.shared_collection_controller.shared_collection

            # If the loan doesn't have a mechanism set, we need to specify one.
            response = self.manager.shared_collection_controller.fulfill(self.collection.name, loan.id, None)
            assert BAD_DELIVERY_MECHANISM.uri == response.uri

            loan.fulfillment = self.delivery_mechanism

            api.queue_fulfill(AuthorizationFailedException())
            response = self.manager.shared_collection_controller.fulfill(self.collection.name, loan.id, None)
            assert INVALID_CREDENTIALS.uri == response.uri

            api.queue_fulfill(CannotFulfill())
            response = self.manager.shared_collection_controller.fulfill(self.collection.name, loan.id, None)
            assert CANNOT_FULFILL.uri == response.uri

            api.queue_fulfill(RemoteIntegrationException("error!", "service"))
            response = self.manager.shared_collection_controller.fulfill(self.collection.name, loan.id, self.delivery_mechanism.delivery_mechanism.id)
            assert INTEGRATION_ERROR.uri == response.uri

            fulfillment_info = FulfillmentInfo(
                self.collection,
                self.pool.data_source.name,
                self.pool.identifier.type,
                self.pool.identifier.identifier,
                "http://content", "text/html", None,
                utc_now(),
            )

            api.queue_fulfill(fulfillment_info)
            def do_get_error(url):
                raise RemoteIntegrationException("error!", "service")
            response = self.manager.shared_collection_controller.fulfill(self.collection.name, loan.id, self.delivery_mechanism.delivery_mechanism.id, do_get=do_get_error)
            assert INTEGRATION_ERROR.uri == response.uri

            api.queue_fulfill(fulfillment_info)
            def do_get_success(url):
                return MockRequestsResponse(200, content="Content")
            response = self.manager.shared_collection_controller.fulfill(self.collection.name, loan.id, self.delivery_mechanism.delivery_mechanism.id, do_get=do_get_success)
            assert 200 == response.status_code
            assert "Content" == response.get_data(as_text=True)
            assert "text/html" == response.headers.get("Content-Type")

            fulfillment_info.content_link = None
            fulfillment_info.content = "Content"
            api.queue_fulfill(fulfillment_info)
            response = self.manager.shared_collection_controller.fulfill(self.collection.name, loan.id, self.delivery_mechanism.delivery_mechanism.id)
            assert 200 == response.status_code
            assert "Content" == response.get_data(as_text=True)
            assert "text/html" == response.headers.get("Content-Type")

    def test_hold_info(self):
        now = utc_now()
        tomorrow = utc_now() + datetime.timedelta(days=1)

        other_client, ignore = IntegrationClient.register(self._db, "http://otherlibrary")
        other_client_hold, ignore = create(
            self._db, Hold, license_pool=self.pool, integration_client=other_client,
        )

        ignore, other_pool = self._edition(
            with_license_pool=True, collection=self._collection(),
        )
        other_pool_hold, ignore = create(
            self._db, Hold, license_pool=other_pool, integration_client=self.client,
        )

        hold, ignore = create(
            self._db, Hold, license_pool=self.pool, integration_client=self.client,
            start=now, end=tomorrow,
        )
        with self.request_context_with_client("/"):
            # This hold doesn't exist.
            response = self.manager.shared_collection_controller.hold_info(self.collection.name, 1234567)
            assert HOLD_NOT_FOUND == response

            # This hold belongs to a different library.
            response = self.manager.shared_collection_controller.hold_info(self.collection.name, other_client_hold.id)
            assert HOLD_NOT_FOUND == response

            # This hold's pool belongs to a different collection.
            response = self.manager.shared_collection_controller.hold_info(self.collection.name, other_pool_hold.id)
            assert HOLD_NOT_FOUND == response

            # This hold is ours.
            response = self.manager.shared_collection_controller.hold_info(self.collection.name, hold.id)
            assert 200 == response.status_code
            feed = feedparser.parse(response.data)
            [entry] = feed.get("entries")
            availability = entry.get("opds_availability")
            since = availability.get("since")
            until = availability.get("until")
            assert datetime.datetime.strftime(now, "%Y-%m-%dT%H:%M:%S+00:00") == since
            assert datetime.datetime.strftime(tomorrow, "%Y-%m-%dT%H:%M:%S+00:00") == until
            [revoke_url] = [link.get("href") for link in entry.get("links") if link.get("rel") == "http://librarysimplified.org/terms/rel/revoke"]
            assert "/collections/%s/holds/%s/revoke" % (self.collection.name, hold.id) in revoke_url
            assert [] == [link.get("href") for link in entry.get("links") if link.get("rel") == "http://opds-spec.org/acquisition"]
            [self_url] = [link.get("href") for link in entry.get("links") if link.get("rel") == "self"]
            assert "/collections/%s/holds/%s" % (self.collection.name, hold.id)

    def test_revoke_hold(self):
        now = utc_now()
        tomorrow = utc_now() + datetime.timedelta(days=1)
        hold, ignore = create(
            self._db, Hold, license_pool=self.pool, integration_client=self.client,
            start=now, end=tomorrow,
        )

        other_client, ignore = IntegrationClient.register(self._db, "http://otherlibrary")
        other_client_hold, ignore = create(
            self._db, Hold, license_pool=self.pool, integration_client=other_client,
        )

        ignore, other_pool = self._edition(
            with_license_pool=True, collection=self._collection(),
        )
        other_pool_hold, ignore = create(
            self._db, Hold, license_pool=other_pool, integration_client=self.client,
        )

        with self.request_context_with_client("/"):
            response = self.manager.shared_collection_controller.revoke_hold(self.collection.name, other_pool_hold.id)
            assert HOLD_NOT_FOUND.uri == response.uri

            response = self.manager.shared_collection_controller.revoke_hold(self.collection.name, other_client_hold.id)
            assert HOLD_NOT_FOUND.uri == response.uri

            api = self.app.manager.shared_collection_controller.shared_collection

            api.queue_revoke_hold(AuthorizationFailedException())
            response = self.manager.shared_collection_controller.revoke_hold(self.collection.name, hold.id)
            assert INVALID_CREDENTIALS.uri == response.uri

            api.queue_revoke_hold(CannotReleaseHold())
            response = self.manager.shared_collection_controller.revoke_hold(self.collection.name, hold.id)
            assert CANNOT_RELEASE_HOLD.uri == response.uri

            api.queue_revoke_hold(NotOnHold())
            response = self.manager.shared_collection_controller.revoke_hold(self.collection.name, hold.id)
            assert NO_ACTIVE_HOLD.uri == response.uri


class TestURNLookupController(ControllerTest):
    """Test that a client can look up data on specific works."""

    def test_work_lookup(self):
        work = self._work(with_open_access_download=True)
        [pool] = work.license_pools
        urn = pool.identifier.urn
        with self.request_context_with_library("/?urn=%s" % urn):
            route_name = "work"

            # Look up a work.
            response = self.manager.urn_lookup.work_lookup(route_name)

            # We got an OPDS feed.
            assert 200 == response.status_code
            assert (
                OPDSFeed.ACQUISITION_FEED_TYPE ==
                response.headers['Content-Type'])

            # Parse it.
            feed = feedparser.parse(response.data)

            # The route name we passed into work_lookup shows up in
            # the feed-level link with rel="self".
            [self_link] = feed['feed']['links']
            assert '/' + route_name in self_link['href']

            # The work we looked up has an OPDS entry.
            [entry] = feed['entries']
            assert work.title == entry['title']

            # The OPDS feed includes an open-access acquisition link
            # -- something that only gets inserted by the
            # CirculationManagerAnnotator.
            [link] = entry.links
            assert LinkRelations.OPEN_ACCESS_DOWNLOAD == link['rel']


class TestProfileController(ControllerTest):
    """Test that a client can interact with the User Profile Management
    Protocol.
    """

    def setup_method(self):
        super(TestProfileController, self).setup_method()

        # Nothing will happen to this patron. This way we can verify
        # that a patron can only see/modify their own profile.
        self.other_patron = self._patron()
        self.other_patron.synchronize_annotations = False
        self.auth = dict(Authorization=self.valid_auth)

    def test_controller_uses_circulation_patron_profile_storage(self):
        """Verify that this controller uses circulation manager-specific extensions."""
        with self.request_context_with_library(
                "/", method='GET', headers=self.auth
        ):
            assert isinstance(self.manager.profiles._controller.storage, CirculationPatronProfileStorage)

    def test_get(self):
        """Verify that a patron can see their own profile."""
        with self.request_context_with_library(
                "/", method='GET', headers=self.auth
        ):
            patron = self.controller.authenticated_patron_from_request()
            patron.synchronize_annotations = True
            response = self.manager.profiles.protocol()
            assert "200 OK" == response.status
            data = json.loads(response.get_data(as_text=True))
            settings = data['settings']
            assert True == settings[ProfileStorage.SYNCHRONIZE_ANNOTATIONS]

    def test_put(self):
        """Verify that a patron can modify their own profile."""
        payload = {
            'settings': {
                ProfileStorage.SYNCHRONIZE_ANNOTATIONS: True
            }
        }

        request_patron = None
        identifier = self._identifier()
        with self.request_context_with_library(
                "/", method='PUT', headers=self.auth,
                content_type=ProfileController.MEDIA_TYPE,
                data=json.dumps(payload)
        ):
            # By default, a patron has no value for synchronize_annotations.
            request_patron = self.controller.authenticated_patron_from_request()
            assert None == request_patron.synchronize_annotations

            # This means we can't create annotations for them.
            pytest.raises(ValueError,  Annotation.get_one_or_create,
                self._db, patron=request_patron, identifier=identifier
            )

            # But by sending a PUT request...
            response = self.manager.profiles.protocol()

            # ...we can change synchronize_annotations to True.
            assert True == request_patron.synchronize_annotations

            # The other patron is unaffected.
            assert False == self.other_patron.synchronize_annotations

        # Now we can create an annotation for the patron who enabled
        # annotation sync.
        annotation = Annotation.get_one_or_create(
            self._db, patron=request_patron, identifier=identifier)
        assert 1 == len(request_patron.annotations)

        # But if we make another request and change their
        # synchronize_annotations field to False...
        payload['settings'][ProfileStorage.SYNCHRONIZE_ANNOTATIONS] = False
        with self.request_context_with_library(
                "/", method='PUT', headers=self.auth,
                content_type=ProfileController.MEDIA_TYPE,
                data=json.dumps(payload)
        ):
            response = self.manager.profiles.protocol()

            # ...the annotation goes away.
            self._db.commit()
            assert False == request_patron.synchronize_annotations
            assert 0 == len(request_patron.annotations)

    def test_problemdetail_on_error(self):
        """Verify that an error results in a ProblemDetail being returned
        from the controller.
        """
        with self.request_context_with_library(
                "/", method='PUT', headers=self.auth,
                content_type="text/plain",
        ):
            response = self.manager.profiles.protocol()
            assert isinstance(response, ProblemDetail)
            assert 415 == response.status_code
            assert ("Expected vnd.librarysimplified/user-profile+json" ==
                response.detail)


class TestScopedSession(ControllerTest):
    """Test that in production scenarios (as opposed to normal unit tests)
    the app server runs each incoming request in a separate database
    session.

    Compare to TestBaseController.test_unscoped_session, which tests
    the corresponding behavior in unit tests.
    """

    @classmethod
    def setup_class(cls):
        ControllerTest.setup_class()
        initialize_database(autoinitialize=False)

    def setup_method(self):
        # We will be calling circulation_manager_setup ourselves,
        # because we want objects like Libraries to be created in the
        # scoped session.
        self.setup_circulation_manager = False
        super(TestScopedSession, self).setup_method()
        self.set_base_url(app._db)

    def make_default_libraries(self, _db):
        libraries = []
        for i in range(2):
            name = self._str + " (library for scoped session)"
            library, ignore = create(_db, Library, short_name=name)
            libraries.append(library)
        return libraries

    def make_default_collection(self, _db, library):
        """We need to create a test collection that
        uses the scoped session.
        """
        collection, ignore = create(
            _db, Collection, name=self._str + " (collection for scoped session)",
        )
        collection.create_external_integration(ExternalIntegration.OPDS_IMPORT)
        library.collections.append(collection)
        return collection

    @contextmanager
    def test_request_context_and_transaction(self, *args):
        """Run a simulated Flask request in a transaction that gets rolled
        back at the end of the request.
        """
        with self.app.test_request_context(*args) as ctx:
            transaction = current_session.begin_nested()
            self.app.manager = self.circulation_manager_setup(
                current_session
            )
            yield ctx
            transaction.rollback()

    def test_scoped_session(self):
        # Start a simulated request to the Flask app server.

        # This sets up a bearer token signing secret outside the
        # transaction rollbacks that is needed to avoid ResourceClosedError
        BearerTokenSigner.bearer_token_signing_secret(self.app._db)

        with self.test_request_context_and_transaction("/"):
            # Each request is given its own database session distinct
            # from the one used by most unit tests or the one
            # associated with the CirculationManager object.
            session1 = current_session()
            assert session1 != self._db
            assert session1 != self.app.manager._db

            # Add an Identifier to the database.
            identifier = Identifier(type=DataSource.GUTENBERG, identifier="1024")
            session1.add(identifier)
            session1.flush()

            # The Identifier immediately shows up in the session that
            # created it.
            [identifier] = session1.query(Identifier).all()
            assert "1024" == identifier.identifier

            # It doesn't show up in self._db, the database session
            # used by most other unit tests, because it was created
            # within the (still-active) context of a Flask request,
            # which happens within a nested database transaction.
            assert [] == self._db.query(Identifier).all()

            # It shows up in the flask_scoped_session object that
            # created the request-scoped session, because within the
            # context of a request, running database queries on that object
            # actually runs them against your request-scoped session.
            [identifier] = self.app.manager._db.query(Identifier).all()
            assert "1024" == identifier.identifier

            # But if we were to use flask_scoped_session to create a
            # brand new session, it would not see the Identifier,
            # because it's running in a different database session.
            new_session = self.app.manager._db.session_factory()
            assert [] == new_session.query(Identifier).all()

            # When the index controller runs in the request context,
            # it doesn't store anything that's associated with the
            # scoped session.
            flask.request.library = self._default_library
            response = self.app.manager.index_controller()
            assert 302 == response.status_code

        # Once we exit the context of the Flask request, the
        # transaction is rolled back. The Identifier never actually
        # enters the database.
        #
        # If it did enter the database, it would never leave.  Changes
        # that happen through self._db happen inside a nested
        # transaction which is rolled back after the test is over.
        # But changes that happen through a session-scoped database
        # connection are actually written to the database when we
        # leave the scope of the request.
        #
        # To avoid this, we use test_request_context_and_transaction
        # to create a nested transaction that's rolled back just
        # before we leave the scope of the request.
        assert [] == self._db.query(Identifier).all()

        # Now create a different simulated Flask request
        with self.test_request_context_and_transaction("/"):
            session2 = current_session()
            assert session2 != self._db
            assert session2 != self.app.manager._db

            # The controller still works in the new request context -
            # nothing it needs is associated with the previous scoped
            # session.
            flask.request.library = self._default_library
            response = self.app.manager.index_controller()
            assert 302 == response.status_code

        # The two Flask requests got different sessions, neither of
        # which is the same as self._db, the unscoped database session
        # used by most other unit tests.
        assert session1 != session2

class TestStaticFileController(CirculationControllerTest):
    def test_static_file(self):
        cache_timeout = ConfigurationSetting.sitewide(
            self._db, Configuration.STATIC_FILE_CACHE_TIME
        )
        cache_timeout.value = 10

        directory = os.path.join(os.path.abspath(os.path.dirname(__file__)), "files", "images")
        filename = "blue.jpg"
        with open(os.path.join(directory, filename), "rb") as f:
            expected_content = f.read()

        with self.app.test_request_context("/"):
            response = self.app.manager.static_files.static_file(directory, filename)

        assert 200 == response.status_code
        assert 'public, max-age=10' == response.headers.get('Cache-Control')
        assert expected_content == response.response.file.read()

        with self.app.test_request_context("/"):
            pytest.raises(NotFound, self.app.manager.static_files.static_file,
                          directory, "missing.png")

    def test_image(self):
        directory = os.path.join(os.path.abspath(os.path.dirname(__file__)), "..", "resources", "images")
        filename = "CleverLoginButton280.png"
        with open(os.path.join(directory, filename), "rb") as f:
            expected_content = f.read()

        with self.app.test_request_context("/"):
            images_dir = f"{app.static_resources_dir}/images"
            response = self.app.manager.static_files.static_file(images_dir, filename)

        assert 200 == response.status_code
        assert expected_content == response.response.file.read()
