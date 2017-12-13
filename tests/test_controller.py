# encoding=utf8
from nose.tools import (
    assert_raises,
    eq_,
    set_trace,
)
from contextlib import contextmanager
import os
import datetime
import re
from wsgiref.handlers import format_date_time
from time import mktime
from decimal import Decimal

import flask
from flask import url_for
from flask_sqlalchemy_session import (
    current_session,
    flask_scoped_session,
)

from . import DatabaseTest
from api.app import app, initialize_database
from api.config import (
    Configuration,
    temp_config,
)
from collections import Counter
from api.controller import (
    CirculationManager,
    CirculationManagerController,
)
from api.lanes import create_default_lanes
from api.authenticator import (
    BasicAuthenticationProvider,
    OAuthController,
    LibraryAuthenticator,
)
from core.app_server import (
    load_lending_policy
)
from core.config import CannotLoadConfiguration
from core.external_search import DummyExternalSearchIndex
from core.metadata_layer import Metadata
from core import model
from core.model import (
    Annotation,
    Collection,
    ConfigurationSetting,
    ExternalIntegration,
    Patron,
    DeliveryMechanism,
    Representation,
    Loan,
    Hold,
    DataSource,
    Edition,
    Identifier,
    Complaint,
    Library,
    SessionManager,
    CachedFeed,
    Work,
    CirculationEvent,
    LicensePoolDeliveryMechanism,
    PresentationCalculationPolicy,
    RightsStatus,
    Session,
    get_one,
    get_one_or_create,
    create,
)
from core.lane import (
    Facets,
    Pagination,
    Lane,
)
from core.problem_details import *
from core.user_profile import (
    ProfileController,
    ProfileStorage,
)
from core.util.problem_detail import ProblemDetail
from core.util.http import RemoteIntegrationException
from core.testing import DummyHTTPClient

from api.problem_details import *
from api.circulation_exceptions import *
from api.circulation import (
    HoldInfo,
    LoanInfo,
    FulfillmentInfo,
)
from api.novelist import MockNoveListAPI
from api.adobe_vendor_id import (
    AuthdataUtility,
    DeviceManagementProtocolController,
)
from api.odl import MockODLWithConsolidatedCopiesAPI
import base64
import feedparser
from core.opds import (
    AcquisitionFeed,
)
from core.util.opds_writer import (    
    OPDSFeed,
)
from api.opds import CirculationManagerAnnotator
from api.annotations import AnnotationWriter
from api.testing import (
    VendorIDTest,
    MockCirculationAPI,
)
from lxml import etree
import random
import json
import urllib
from core.analytics import Analytics
from core.util.authentication_for_opds import AuthenticationForOPDSDocument


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
            
    def setup(self, _db=None, set_up_circulation_manager=True):
        super(ControllerTest, self).setup()
        _db = _db or self._db
        self.app = app
        
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
        base_url = ConfigurationSetting.sitewide(_db, Configuration.BASE_URL_KEY)
        base_url.value = u'http://test-circulation-manager/'
        
        # NOTE: Any reference to self._default_library below this
        # point in this method will cause the tests in
        # TestScopedSession to hang.
        if set_up_circulation_manager:
            app.manager = self.circulation_manager_setup(_db)

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

        self.authdata = AuthdataUtility.from_config(self.library)

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
            if x.display_name=='Fiction' and x.languages==[u'eng']
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
            p = BasicAuthenticationProvider
            integration.setting(p.TEST_IDENTIFIER).value = "unittestuser"
            integration.setting(p.TEST_PASSWORD).value = "unittestpassword"
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
    
    def setup(self):
        super(CirculationControllerTest, self).setup()
        for (variable_name, title, author, language, fiction) in self.BOOKS:
            work = self._work(title, author, language=language, fiction=fiction,
                              with_open_access_download=True)
            setattr(self, variable_name, work)
            work.license_pools[0].collection = self.collection


class TestCirculationManager(CirculationControllerTest):
    """Test the CirculationManager object itself."""

    def test_load_settings(self):
        # Here's a CirculationManager which we've been using for a while.
        manager = self.manager

        # Certain fields of the CirculationManager have certain values
        # which are about to be reloaded.
        manager.__external_search = object()
        manager.adobe_device_management = object()
        manager.oauth_controller = object()
        manager.auth = object()
        manager.lending_policy = object()

        # But some fields are _not_ about to be reloaded
        index_controller = manager.index_controller
        
        # The CirculationManager has a top-level lane and a
        # CirculationAPI for the default library, but no others.
        eq_(1, len(manager.top_level_lanes))
        eq_(1, len(manager.circulation_apis))

        # Now let's create a brand new library, never before seen.
        library = self._library()
        self.library_setup(library)

        # In addition to the setup performed by library_set(), give it
        # a registry integration with short client tokens so we can verify
        # that the DeviceManagementProtocolController is recreated.
        self.initialize_adobe(library, [library])

        # Then reload the CirculationManager...
        self.manager.load_settings()
        
        # Now the new library has a top-level lane.
        assert library.id in manager.top_level_lanes

        # And a circulation API.
        assert library.id in manager.circulation_apis

        # The Authenticator has been reloaded with information about
        # how to authenticate patrons of the new library.
        assert isinstance(
            manager.auth.library_authenticators[library.short_name],
            LibraryAuthenticator
        )
        
        # The ExternalSearch object has been reset.
        assert isinstance(manager.external_search, DummyExternalSearchIndex)
        
        # So has the lending policy.
        assert isinstance(manager.lending_policy, dict)
        
        # The OAuth controller has been recreated.
        assert isinstance(manager.oauth_controller, OAuthController)

        # So has the controller for the Device Management Protocol.
        assert isinstance(manager.adobe_device_management,
                          DeviceManagementProtocolController)

        # Controllers that don't depend on site configuration
        # have not been reloaded.
        eq_(index_controller, manager.index_controller)

    def test_exception_during_external_search_initialization_is_stored(self):

        class BadSearch(CirculationManager):

            @property
            def setup_search(self):
                raise CannotLoadConfiguration("doomed!")

        circulation = BadSearch(self._db, testing=True)

        # We didn't get a search object.
        eq_(None, circulation.external_search)

        # The reason why is stored here.
        ex = circulation.external_search_initialization_exception
        assert isinstance(ex, CannotLoadConfiguration)
        eq_("doomed!", ex.message)

    def test_exception_during_short_client_token_initialization_is_stored(self):

        # Create an incomplete Short Client Token setup for our
        # library.
        registry_integration = self._external_integration(
            protocol=ExternalIntegration.OPDS_REGISTRATION,
            goal=ExternalIntegration.DISCOVERY_GOAL, libraries=[self.library]
        )
        registry_integration.username = "something"
        registry_integration.set_setting(AuthdataUtility.VENDOR_ID_KEY, "vendorid")

        # Then try to set up the Adobe Vendor ID configuration for
        # that library.
        self.manager.setup_adobe_vendor_id(self._db, self.library)

        # The exception caused when we tried to load the incomplete
        # configuration was stored here.
        ex = self.manager.short_client_token_initialization_exceptions[self.library.id]
        assert isinstance(ex, CannotLoadConfiguration)
        assert ex.message.startswith("Short Client Token configuration is incomplete")

class TestBaseController(CirculationControllerTest):

    def test_unscoped_session(self):
        """Compare to TestScopedSession.test_scoped_session to see
        how database sessions will be handled in production.
        """
        # Both requests used the self._db session used by most unit tests.
        with self.request_context_with_library("/"):
            response1 = self.manager.index_controller()
            eq_(self.app.manager._db, self._db)

        with self.request_context_with_library("/"):
            response2 = self.manager.index_controller()
            eq_(self.app.manager._db, self._db)

    def test_authenticated_patron_invalid_credentials(self):
        with self.request_context_with_library("/"):
            value = self.controller.authenticated_patron(
                dict(username="user1", password="password2")
            )
            eq_(value, INVALID_CREDENTIALS)

    def test_authenticated_patron_can_authenticate_with_expired_credentials(self):
        """A patron can authenticate even if their credentials have
        expired -- they just can't create loans or holds.
        """
        one_year_ago = datetime.datetime.utcnow() - datetime.timedelta(days=365)
        with self.request_context_with_library("/"):
            patron = self.controller.authenticated_patron(
                self.valid_credentials
            )
            patron.expires = one_year_ago

            patron = self.controller.authenticated_patron(
                self.valid_credentials
            )
            eq_(one_year_ago, patron.expires)

    def test_authenticated_patron_correct_credentials(self):
        with self.request_context_with_library("/"):
            value = self.controller.authenticated_patron(self.valid_credentials)
            assert isinstance(value, Patron)

    def test_authentication_sends_proper_headers(self):

        # Make sure the realm header has quotes around the realm name.  
        # Without quotes, some iOS versions don't recognize the header value.

        base_url = ConfigurationSetting.sitewide(self._db, Configuration.BASE_URL_KEY)
        base_url.value = u'http://url'

        with self.request_context_with_library("/"):
            response = self.controller.authenticate()
            eq_(response.headers['WWW-Authenticate'], u'Basic realm="Library card"')

        with self.request_context_with_library("/", headers={"X-Requested-With": "XMLHttpRequest"}):
            response = self.controller.authenticate()
            eq_(None, response.headers.get("WWW-Authenticate"))

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
        eq_(2, len(loaded))
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
        eq_(NO_LICENSES.uri, problem_detail.uri)
        expect = u"The item you're asking about (bad identifier type/%s) isn't in this collection." % i1.identifier
        eq_(expect, problem_detail.detail)

        # Try an identifier that would work except that it's not in a
        # Collection associated with the given Library.
        problem_detail = self.controller.load_licensepools(
            self._default_library, lp5.identifier.type,
            lp5.identifier.identifier
        )
        eq_(NO_LICENSES.uri, problem_detail.uri)

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
        eq_(lpdm, delivery)

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
        eq_(True, is_new)

        eq_(new_lpdm.delivery_mechanism, lpdm.delivery_mechanism)
        underlying_mechanism = lpdm.delivery_mechanism

        delivery = self.controller.load_licensepooldelivery(
            licensepool, lpdm.delivery_mechanism.id
        )

        # We don't know which LicensePoolDeliveryMechanism this is, 
        # but we know it's one of the matches.
        eq_(underlying_mechanism, delivery.delivery_mechanism)

        # If there is no matching delivery mechanism, we return a
        # problem detail.
        adobe_licensepool = self._licensepool(
            edition=None, with_open_access_download=False
        )
        problem_detail = self.controller.load_licensepooldelivery(
            adobe_licensepool, lpdm.delivery_mechanism.id
        )
        eq_(BAD_DELIVERY_MECHANISM.uri, problem_detail.uri)

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
            eq_(True, pool.open_access)

            # It can still be borrowed even though it has no
            # 'licenses' available.
            problem = self.controller.apply_borrowing_policy(patron, pool)
            eq_(None, problem)

            # If it weren't an open-access work, there'd be a big
            # problem.
            pool.open_access = False
            problem = self.controller.apply_borrowing_policy(patron, pool)
            eq_(FORBIDDEN_BY_POLICY.uri, problem.uri)

    def test_apply_borrowing_policy_for_audience_restriction(self):
        with self.request_context_with_library("/"):
            patron = self.controller.authenticated_patron(self.valid_credentials)
            work = self._work(with_license_pool=True)
            [pool] = work.license_pools

            self.manager.lending_policy = load_lending_policy(
                {
                    "60": {"audiences": ["Children"]}, 
                    "152": {"audiences": ["Children"]}, 
                    "62": {"audiences": ["Children"]}
                }
            )

            patron.external_type = '10'
            eq_(None, self.controller.apply_borrowing_policy(patron, pool))

            patron.external_type = '152'
            problem = self.controller.apply_borrowing_policy(patron, pool)
            eq_(FORBIDDEN_BY_POLICY.uri, problem.uri)

    def test_library_for_request(self):
        with self.app.test_request_context("/"):
            value = self.controller.library_for_request("not-a-library")
            eq_(LIBRARY_NOT_FOUND, value)

        with self.app.test_request_context("/"):
            value = self.controller.library_for_request(self._default_library.short_name)
            eq_(self._default_library, value)
            eq_(self._default_library, flask.request.library)

        # If you don't specify a library, the default library is used.
        with self.app.test_request_context("/"):
            value = self.controller.library_for_request(None)
            expect_default = Library.default(self._db)
            eq_(expect_default, value)
            eq_(expect_default, flask.request.library)
            
    def test_library_for_request_reloads_settings_if_necessary(self):

        # We're about to change the shortname of the default library.
        new_name = "newname" + self._str

        # Before we make the change, a request to the library's new name
        # will fail.
        assert new_name not in self.manager.auth.library_authenticators
        with self.app.test_request_context("/"):
            problem = self.controller.library_for_request(new_name)
            eq_(LIBRARY_NOT_FOUND, problem)


        # Make the change.
        self._default_library.short_name = new_name
        self._db.commit()

        # Bypass the 1-second timeout and make sure the site knows
        # the configuration has actually changed.
        model.site_configuration_has_changed(self._db, timeout=0)        
        
        # Just making the change and calling
        # site_configuration_has_changed was not enough to update the
        # CirculationManager's settings.
        assert new_name not in self.manager.auth.library_authenticators
        
        # But the first time we make a request that calls the library
        # by its new name, those settings are reloaded.
        with self.app.test_request_context("/"):
            value = self.controller.library_for_request(new_name)
            eq_(self._default_library, value)
                
            # An assertion that would have failed before works now.
            assert new_name in self.manager.auth.library_authenticators


class FullLaneSetupTest(CirculationControllerTest):
    """Most lane-based tests don't need the full multi-tier setup of lanes
    that we would see in a real site. We use a smaller setup to save time
    when running the test.

    This class is for the tests that do need that full set of lanes.
    """
    def library_setup(self, library):
        super(FullLaneSetupTest, self).library_setup(library)
        for k, v in [
                (Configuration.LARGE_COLLECTION_LANGUAGES, ['eng']),
                (Configuration.SMALL_COLLECTION_LANGUAGES, ['spa', 'chi']),
                (Configuration.TINY_COLLECTION_LANGUAGES, [])
        ]:
            ConfigurationSetting.for_library(k, library).value = json.dumps(v)
        create_default_lanes(self._db, library)
    
    def test_load_lane(self):
        with self.request_context_with_library("/"):
            eq_(self.manager.d_top_level_lane,
                self.controller.load_lane(None, None))
            chinese = self.controller.load_lane('chi', None)
            eq_("Chinese", chinese.name)
            eq_("Chinese", chinese.display_name)
            eq_(["chi"], chinese.languages)

            english_sf = self.controller.load_lane('eng', "Science Fiction")
            eq_("Science Fiction", english_sf.display_name)
            eq_(["eng"], english_sf.languages)

            # __ is converted to /
            english_thriller = self.controller.load_lane('eng', "Suspense__Thriller")
            eq_("Suspense/Thriller", english_thriller.name)

            # Unlike with Chinese, there is no lane that contains all English books.
            english = self.controller.load_lane('eng', None)
            eq_(english.uri, NO_SUCH_LANE.uri)

            no_such_language = self.controller.load_lane('o10', None)
            eq_(no_such_language.uri, NO_SUCH_LANE.uri)
            eq_("Unrecognized language key: o10", no_such_language.detail)

            no_such_lane = self.controller.load_lane('eng', 'No such lane')
            eq_("No such lane: No such lane", no_such_lane.detail)

            

class TestIndexController(CirculationControllerTest):
    
    def test_simple_redirect(self):
        with self.app.test_request_context('/'):
            flask.request.library = self.library
            response = self.manager.index_controller()
            eq_(302, response.status_code)
            eq_("http://cdn/default/groups/", response.headers['location'])

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
            eq_(401, response.status_code)

        with self.request_context_with_library(
            "/", headers=dict(Authorization=self.valid_auth)):
            response = self.manager.index_controller()
            eq_(302, response.status_code)
            eq_("http://cdn/default/groups/%s" % root_1.id, 
                response.headers['location'])

        self.default_patron.external_type = "2"
        with self.request_context_with_library(
            "/", headers=dict(Authorization=self.valid_auth)):
            response = self.manager.index_controller()
            eq_(302, response.status_code)
            eq_("http://cdn/default/groups/%s" % root_1.id, response.headers['location'])

        self.default_patron.external_type = "3"
        with self.request_context_with_library(
            "/", headers=dict(Authorization=self.valid_auth)):
            response = self.manager.index_controller()
            eq_(302, response.status_code)
            eq_("http://cdn/default/groups/%s" % root_2.id, response.headers['location'])

        # Patrons with a different type get sent to the top-level lane.
        self.default_patron.external_type = '4'
        with self.request_context_with_library(
            "/", headers=dict(Authorization=self.valid_auth)):
            response = self.manager.index_controller()
            eq_(302, response.status_code)
            eq_("http://cdn/default/groups/", response.headers['location'])

        # Patrons with no type get sent to the top-level lane.
        self.default_patron.external_type = None
        with self.request_context_with_library(
            "/", headers=dict(Authorization=self.valid_auth)):
            response = self.manager.index_controller()
            eq_(302, response.status_code)
            eq_("http://cdn/default/groups/", response.headers['location'])

    def test_authentication_document(self):
        """Test the ability to retrieve an Authentication For OPDS document."""
        with self.request_context_with_library(
                "/", headers=dict(Authorization=self.invalid_auth)):
            response = self.manager.index_controller.authentication_document()
            eq_(200, response.status_code)
            eq_(AuthenticationForOPDSDocument.MEDIA_TYPE, response.headers['Content-Type'])
            data = response.data
            eq_(self.manager.auth.create_authentication_document(), data)

            # Make sure we got the A4OPDS document for the right library.
            doc = json.loads(data)
            eq_(self.library.short_name, doc['title'])

    def test_public_key_integration_document(self):
        base_url = ConfigurationSetting.sitewide(self._db, Configuration.BASE_URL_KEY).value

        # Without a sitewide public key, only the id and an empty dictionary are
        # returned. Library-specific public keys are ignored.
        ConfigurationSetting.for_library(Configuration.PUBLIC_KEY, self.library).value = u'banana'
        with self.app.test_request_context('/'):
            response = self.manager.index_controller.public_key_document()

        eq_(200, response.status_code)
        eq_('application/opds+json', response.headers.get('Content-Type'))

        data = json.loads(response.data)
        eq_('http://test-circulation-manager/', data.get('id'))
        eq_({}, data.get('public_key'))

        # When a sitewide public key exists, all of its data is included.
        ConfigurationSetting.sitewide(self._db, Configuration.PUBLIC_KEY).value = u'weird'
        with self.app.test_request_context('/'):
            response = self.manager.index_controller.public_key_document()

        eq_(200, response.status_code)
        eq_('application/opds+json', response.headers.get('Content-Type'))

        data = json.loads(response.data)
        eq_('RSA', data.get('public_key', {}).get('type'))
        eq_('weird', data.get('public_key', {}).get('value'))


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
                eq_(library, patron.library)
                response = self.manager.index_controller()
                eq_("http://cdn/%s/groups/" % library.short_name,
                    response.headers['location'])
            
class TestLoanController(CirculationControllerTest):
    def setup(self):
        super(TestLoanController, self).setup()
        self.pool = self.english_1.license_pools[0]
        self.mech2 = self.pool.set_delivery_mechanism(
            Representation.PDF_MEDIA_TYPE, DeliveryMechanism.NO_DRM,
            RightsStatus.CC_BY, None
        )
        self.edition = self.pool.presentation_edition
        self.data_source = self.edition.data_source
        self.identifier = self.edition.primary_identifier

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
            eq_((None, None), result)

            # No holds.
            result = self.manager.loans.get_patron_hold(
                self.default_patron, pools
            )
            eq_((None, None), result)

            # When there's a loan, we retrieve it.
            loan, newly_created = self.pool.loan_to(self.default_patron)
            result = self.manager.loans.get_patron_loan(
                self.default_patron, pools
            )
            eq_((loan, self.pool), result)

            # When there's a hold, we retrieve it.
            hold, newly_created = other_pool.on_hold_to(self.default_patron)
            result = self.manager.loans.get_patron_hold(
                self.default_patron, pools
            )
            eq_((hold, other_pool), result)

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
            eq_(None, loan.fulfillment)

            # We've been given an OPDS feed with one entry, which tells us how 
            # to fulfill the license.
            eq_(201, response.status_code)
            feed = feedparser.parse(response.get_data())
            [entry] = feed['entries']
            fulfillment_links = [x['href'] for x in entry['links']
                                if x['rel'] == OPDSFeed.ACQUISITION_REL]
            [mech1, mech2] = sorted(
                self.pool.delivery_mechanisms, 
                key=lambda x: x.delivery_mechanism.default_client_can_fulfill
            )

            fulfillable_mechanism = mech2

            expects = [url_for('fulfill',
                               license_pool_id=self.pool.id,
                               mechanism_id=mech.delivery_mechanism.id,
                               library_short_name=self.library.short_name,
                               _external=True) for mech in [mech1, mech2]]
            eq_(set(expects), set(fulfillment_links))

            http = DummyHTTPClient()

            # Now let's try to fulfill the loan.
            http.queue_response(200, content="I am an ACSM file")

            response = self.manager.loans.fulfill(
                self.pool.id, fulfillable_mechanism.delivery_mechanism.id,
                do_get=http.do_get
            )
            eq_(200, response.status_code)
            eq_(["I am an ACSM file"],
                response.response)
            eq_(http.requests, [fulfillable_mechanism.resource.url])

            # The mechanism we used has been registered with the loan.
            eq_(fulfillable_mechanism, loan.fulfillment)

            # Now that we've set a mechanism, we can fulfill the loan
            # again without specifying a mechanism.
            http.queue_response(200, content="I am an ACSM file")

            response = self.manager.loans.fulfill(
                self.pool.id, do_get=http.do_get
            )
            eq_(200, response.status_code)
            eq_(["I am an ACSM file"],
                response.response)
            eq_(http.requests, [fulfillable_mechanism.resource.url, fulfillable_mechanism.resource.url])

            # But we can't use some other mechanism -- we're stuck with
            # the first one we chose.
            response = self.manager.loans.fulfill(
                self.pool.id, mech1.delivery_mechanism.id
            )

            eq_(409, response.status_code)
            assert "You already fulfilled this loan as application/epub+zip (DRM-free), you can't also do it as application/pdf (DRM-free)" in response.detail

            # If the remote server fails, we get a problem detail.
            def doomed_get(url, headers, **kwargs):
                raise RemoteIntegrationException("fulfill service", "Error!")

            response = self.manager.loans.fulfill(
                self.pool.id, do_get=doomed_get
            )
            assert isinstance(response, ProblemDetail)
            eq_(502, response.status_code)

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
                    datetime.datetime.utcnow(),
                    datetime.datetime.utcnow() + datetime.timedelta(seconds=3600),
                )
            )
            response = self.manager.loans.borrow(
                identifier.type, identifier.identifier)

            # A loan has been created for this license pool.
            loan = get_one(self._db, Loan, license_pool=pool)
            assert loan != None
            # The loan has yet to be fulfilled.
            eq_(None, loan.fulfillment)

            # We've been given an OPDS feed with two delivery mechanisms, which tell us how 
            # to fulfill the license.
            eq_(201, response.status_code)
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
            eq_(set(expects), set(fulfillment_links))

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
            eq_(200, response.status_code)
            opds_entries = feedparser.parse(response.response[0])['entries']
            eq_(1, len(opds_entries))
            links = opds_entries[0]['links']
        
            # The entry includes one fulfill link.
            fulfill_links = [link for link in links if link['rel'] == "http://opds-spec.org/acquisition"]
            eq_(1, len(fulfill_links))

            eq_(Representation.TEXT_HTML_MEDIA_TYPE + DeliveryMechanism.STREAMING_PROFILE,
                fulfill_links[0]['type'])
            eq_("http://streaming-content-link", fulfill_links[0]['href'])


            # The mechanism has not been set, since fulfilling a streaming
            # mechanism does not lock in the format.
            eq_(None, loan.fulfillment)

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
            eq_(200, response.status_code)

            # Now the fulfillment has been set to the other mechanism.
            eq_(mech1, loan.fulfillment)

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
            eq_(200, response.status_code)
            opds_entries = feedparser.parse(response.response[0])['entries']
            eq_(1, len(opds_entries))
            links = opds_entries[0]['links']
        
            fulfill_links = [link for link in links if link['rel'] == "http://opds-spec.org/acquisition"]
            eq_(1, len(fulfill_links))

            eq_(Representation.TEXT_HTML_MEDIA_TYPE + DeliveryMechanism.STREAMING_PROFILE,
                fulfill_links[0]['type'])
            eq_("http://streaming-content-link", fulfill_links[0]['href'])

    def test_borrow_nonexistent_delivery_mechanism(self):
        with self.request_context_with_library(
                "/", headers=dict(Authorization=self.valid_auth)):
            self.manager.loans.authenticated_patron_from_request()
            response = self.manager.loans.borrow(
                self.identifier.type, self.identifier.identifier,
                -100
            )
            eq_(BAD_DELIVERY_MECHANISM, response) 

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
                    datetime.datetime.utcnow(),
                    datetime.datetime.utcnow() + datetime.timedelta(seconds=3600),
                    1,
                )
            )
            response = self.manager.loans.borrow(
                pool.identifier.type, pool.identifier.identifier)
            eq_(201, response.status_code)
            
            # A hold has been created for this license pool.
            hold = get_one(self._db, Hold, license_pool=pool)
            assert hold != None

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
                    datetime.datetime.utcnow(),
                    datetime.datetime.utcnow() + datetime.timedelta(seconds=3600),
                    1,
                )
            )
            response = self.manager.loans.borrow(
                pool.identifier.type, pool.identifier.identifier)
            eq_(201, response.status_code)

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
             eq_(404, response.status_code)
             eq_("http://librarysimplified.org/terms/problem/not-found-on-remote", response.uri)

    def test_borrow_fails_when_work_already_checked_out(self):
        loan, _ignore = get_one_or_create(
            self._db, Loan, license_pool=self.pool,
            patron=self.default_patron
        )

        with self.request_context_with_library(
                "/", headers=dict(Authorization=self.valid_auth)):
            self.manager.loans.authenticated_patron_from_request()
            response = self.manager.loans.borrow(
                self.identifier.type, self.identifier.identifier)

            eq_(ALREADY_CHECKED_OUT, response)

    def test_fulfill_fails_when_no_active_loan(self):
        with self.request_context_with_library(
                "/", headers=dict(Authorization=self.valid_auth)):
            self.manager.loans.authenticated_patron_from_request()
            response = self.manager.loans.fulfill(
                self.pool.id, self.mech2.id
            )

            eq_(NO_ACTIVE_LOAN.uri, response.uri)

    def test_revoke_loan(self):
         with self.request_context_with_library(
                 "/", headers=dict(Authorization=self.valid_auth)):
             patron = self.manager.loans.authenticated_patron_from_request()
             loan, newly_created = self.pool.loan_to(patron)

             self.manager.d_circulation.queue_checkin(self.pool, True)

             response = self.manager.loans.revoke(self.pool.id)

             eq_(200, response.status_code)
             
    def test_revoke_hold(self):
         with self.request_context_with_library(
                 "/", headers=dict(Authorization=self.valid_auth)):
             patron = self.manager.loans.authenticated_patron_from_request()
             hold, newly_created = self.pool.on_hold_to(patron, position=0)

             self.manager.d_circulation.queue_release_hold(self.pool, True)

             response = self.manager.loans.revoke(self.pool.id)

             eq_(200, response.status_code)

    def test_revoke_hold_nonexistent_licensepool(self):
         with self.request_context_with_library(
                 "/", headers=dict(Authorization=self.valid_auth)):
            patron = self.manager.loans.authenticated_patron_from_request()
            response = self.manager.loans.revoke(-10)
            assert isinstance(response, ProblemDetail)
            eq_(INVALID_INPUT.uri, response.uri)

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
            eq_(HOLD_LIMIT_REACHED.uri, response.uri)

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
                
            eq_(403, response.status_code)
            eq_(OUTSTANDING_FINES.uri, response.uri)
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
                    datetime.datetime.utcnow(),
                    datetime.datetime.utcnow() + datetime.timedelta(seconds=3600),
                )
            )
            response = self.manager.loans.borrow(
                pool.identifier.type, pool.identifier.identifier)
                
            eq_(201, response.status_code)

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
            eq_(400, response.status_code)
            eq_(CANNOT_RELEASE_HOLD.uri, response.uri)
            eq_("Cannot release a hold once it enters reserved state.", response.detail)

    def test_active_loans(self):
        with self.request_context_with_library(
                "/", headers=dict(Authorization=self.valid_auth)):
            patron = self.manager.loans.authenticated_patron_from_request()
            response = self.manager.loans.sync()
            assert not "<entry>" in response.data
            assert response.headers['Cache-Control'].startswith('private,')

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
            datetime.datetime.utcnow(),
            datetime.datetime.utcnow() + datetime.timedelta(seconds=3600)
        )
        self.manager.d_circulation.add_remote_hold(
            bibliotheca_pool.collection, bibliotheca_pool.data_source,
            bibliotheca_pool.identifier.type,
            bibliotheca_pool.identifier.identifier,
            datetime.datetime.utcnow(),
            datetime.datetime.utcnow() + datetime.timedelta(seconds=3600),
            0,
        )

        with self.request_context_with_library(
                "/", headers=dict(Authorization=self.valid_auth)):
            patron = self.manager.loans.authenticated_patron_from_request()
            response = self.manager.loans.sync()

            feed = feedparser.parse(response.data)
            entries = feed['entries']

            overdrive_entry = [entry for entry in entries if entry['title'] == overdrive_book.title][0]
            bibliotheca_entry = [entry for entry in entries if entry['title'] == bibliotheca_book.title][0]

            eq_(overdrive_entry['opds_availability']['status'], 'available')
            eq_(bibliotheca_entry['opds_availability']['status'], 'ready')
            
            overdrive_links = overdrive_entry['links']
            fulfill_link = [x for x in overdrive_links if x['rel'] == 'http://opds-spec.org/acquisition'][0]['href']
            revoke_link = [x for x in overdrive_links if x['rel'] == OPDSFeed.REVOKE_LOAN_REL][0]['href']
            bibliotheca_links = bibliotheca_entry['links']
            borrow_link = [x for x in bibliotheca_links if x['rel'] == 'http://opds-spec.org/acquisition/borrow'][0]['href']
            bibliotheca_revoke_links = [x for x in bibliotheca_links if x['rel'] == OPDSFeed.REVOKE_LOAN_REL]

            assert urllib.quote("%s/fulfill" % overdrive_pool.id) in fulfill_link
            assert urllib.quote("%s/revoke" % overdrive_pool.id) in revoke_link
            assert urllib.quote("%s/%s/borrow" % (bibliotheca_pool.identifier.type, bibliotheca_pool.identifier.identifier)) in borrow_link
            eq_(0, len(bibliotheca_revoke_links))


class TestAnnotationController(CirculationControllerTest):
    def setup(self):
        super(TestAnnotationController, self).setup()
        self.pool = self.english_1.license_pools[0]
        self.edition = self.pool.presentation_edition
        self.identifier = self.edition.primary_identifier

    def test_get_empty_container(self):
        with self.request_context_with_library(
                "/", headers=dict(Authorization=self.valid_auth)):
            self.manager.loans.authenticated_patron_from_request()
            response = self.manager.annotations.container()
            eq_(200, response.status_code)

            # We've been given an annotation container with no items.
            container = json.loads(response.data)
            eq_([], container['first']['items'])
            eq_(0, container['total'])

            # The response has the appropriate headers.
            allow_header = response.headers['Allow']
            for method in ['GET', 'HEAD', 'OPTIONS', 'POST']:
                assert method in allow_header

            eq_(AnnotationWriter.CONTENT_TYPE, response.headers['Accept-Post'])
            eq_(AnnotationWriter.CONTENT_TYPE, response.headers['Content-Type'])
            eq_('W/""', response.headers['ETag'])

    def test_get_container_with_item(self):
        self.pool.loan_to(self.default_patron)

        annotation, ignore = create(
            self._db, Annotation,
            patron=self.default_patron,
            identifier=self.identifier,
            motivation=Annotation.IDLING,
        )
        annotation.active = True
        annotation.timestamp = datetime.datetime.now()

        with self.request_context_with_library(
                "/", headers=dict(Authorization=self.valid_auth)):
            self.manager.annotations.authenticated_patron_from_request()
            response = self.manager.annotations.container()
            eq_(200, response.status_code)

            # We've been given an annotation container with one item.
            container = json.loads(response.data)
            eq_(1, container['total'])
            item = container['first']['items'][0]
            eq_(annotation.motivation, item['motivation'])

            # The response has the appropriate headers.
            allow_header = response.headers['Allow']
            for method in ['GET', 'HEAD', 'OPTIONS', 'POST']:
                assert method in allow_header

            eq_(AnnotationWriter.CONTENT_TYPE, response.headers['Accept-Post'])
            eq_(AnnotationWriter.CONTENT_TYPE, response.headers['Content-Type'])
            expected_etag = 'W/"%s"' % annotation.timestamp
            eq_(expected_etag, response.headers['ETag'])
            expected_time = format_date_time(mktime(annotation.timestamp.timetuple()))
            eq_(expected_time, response.headers['Last-Modified'])

    def test_get_container_for_work(self):
        self.pool.loan_to(self.default_patron)

        annotation, ignore = create(
            self._db, Annotation,
            patron=self.default_patron,
            identifier=self.identifier,
            motivation=Annotation.IDLING,
        )
        annotation.active = True
        annotation.timestamp = datetime.datetime.now()

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
            eq_(200, response.status_code)

            # We've been given an annotation container with one item.
            container = json.loads(response.data)
            eq_(1, container['total'])
            item = container['first']['items'][0]
            eq_(annotation.motivation, item['motivation'])

            # The response has the appropriate headers - POST is not allowed.
            allow_header = response.headers['Allow']
            for method in ['GET', 'HEAD', 'OPTIONS']:
                assert method in allow_header

            assert 'Accept-Post' not in response.headers.keys()
            eq_(AnnotationWriter.CONTENT_TYPE, response.headers['Content-Type'])
            expected_etag = 'W/"%s"' % annotation.timestamp
            eq_(expected_etag, response.headers['ETag'])
            expected_time = format_date_time(mktime(annotation.timestamp.timetuple()))
            eq_(expected_time, response.headers['Last-Modified'])

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
            eq_(0, len(annotations))

            response = self.manager.annotations.container()

            # The patron doesn't have the pool on loan yet, so the request fails.
            eq_(400, response.status_code)
            annotations = self._db.query(Annotation).filter(Annotation.patron==patron).all()
            eq_(0, len(annotations))

            # Give the patron a loan and try again, and the request creates an annotation.
            self.pool.loan_to(patron)
            response = self.manager.annotations.container()
            eq_(200, response.status_code)
            
            annotations = self._db.query(Annotation).filter(Annotation.patron==patron).all()
            eq_(1, len(annotations))
            annotation = annotations[0]
            eq_(Annotation.IDLING, annotation.motivation)
            selector = json.loads(annotation.target).get("http://www.w3.org/ns/oa#hasSelector")[0].get('@id')
            eq_(data['target']['selector'], selector)

            # The response contains the annotation in the db.
            item = json.loads(response.data)
            assert str(annotation.id) in item['id']
            eq_(annotation.motivation, item['motivation'])

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
            eq_(200, response.status_code)

            # We've been given a single annotation item.
            item = json.loads(response.data)
            assert str(annotation.id) in item['id']
            eq_(annotation.motivation, item['motivation'])

            # The response has the appropriate headers.
            allow_header = response.headers['Allow']
            for method in ['GET', 'HEAD', 'OPTIONS', 'DELETE']:
                assert method in allow_header

            eq_(AnnotationWriter.CONTENT_TYPE, response.headers['Content-Type'])

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
            eq_(404, response.status_code)

    def test_detail_for_missing_annotation_returns_404(self):
        with self.request_context_with_library(
                "/", headers=dict(Authorization=self.valid_auth)):
            self.manager.annotations.authenticated_patron_from_request()

            # This annotation does not exist.
            response = self.manager.annotations.detail(100)
            eq_(404, response.status_code)

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
            eq_(404, response.status_code)

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
            eq_(200, response.status_code)

            # The annotation has been marked inactive.
            eq_(False, annotation.active)


class TestWorkController(CirculationControllerTest):
    def setup(self):
        super(TestWorkController, self).setup()
        [self.lp] = self.english_1.license_pools
        self.edition = self.lp.presentation_edition
        self.datasource = self.lp.data_source.name
        self.identifier = self.lp.identifier

    def test_contributor(self):
        # Give the Contributor a display_name.
        [contribution] = self.english_1.presentation_edition.contributions
        contribution.contributor.display_name = u"John Bull"

        # For works without a contributor name, a ProblemDetail is returned.
        with self.request_context_with_library('/'):
            response = self.manager.work_controller.contributor('', None, None)
        eq_(404, response.status_code)
        eq_("http://librarysimplified.org/terms/problem/unknown-lane", response.uri)

        contributor = self.edition.contributions[0].contributor
        contributor.display_name = name = 'John Bull'
        
        # Similarly if the pagination data is bad.
        with self.request_context_with_library('/?size=abc'):
            response = self.manager.work_controller.contributor(name, None, None)
            eq_(400, response.status_code)

        # Or if the facet data is bad.
        with self.request_context_with_library('/?order=nosuchorder'):
            response = self.manager.work_controller.contributor(name, None, None)
            eq_(400, response.status_code)
        
        # If the work has a contributor, a feed is returned.
        SessionManager.refresh_materialized_views(self._db)
        with self.request_context_with_library('/'):
            response = self.manager.work_controller.contributor(name, None, None)

        eq_(200, response.status_code)
        feed = feedparser.parse(response.data)
        eq_(name, feed['feed']['title'])
        [entry] = feed['entries']
        eq_(self.english_1.title, entry['title'])

        # The feed has facet links.
        links = feed['feed']['links']
        facet_links = [link for link in links if link['rel'] == 'http://opds-spec.org/facet']
        eq_(9, len(facet_links))

        another_work = self._work("Not open access", name, with_license_pool=True)
        another_work.license_pools[0].open_access = False
        duplicate_contributor = another_work.presentation_edition.contributions[0].contributor
        duplicate_contributor.display_name = name

        # Facets work.
        SessionManager.refresh_materialized_views(self._db)
        with self.request_context_with_library("/?order=title"):
            response = self.manager.work_controller.contributor(name, None, None)

        eq_(200, response.status_code)
        feed = feedparser.parse(response.data)
        eq_(2, len(feed['entries']))

        with self.request_context_with_library("/?available=always"):
            response = self.manager.work_controller.contributor(name, None, None)

        eq_(200, response.status_code)
        feed = feedparser.parse(response.data)
        eq_(1, len(feed['entries']))
        [entry] = feed['entries']
        eq_(self.english_1.title, entry['title'])

        # Pagination works.
        with self.request_context_with_library("/?size=1"):
            response = self.manager.work_controller.contributor(name, None, None)

        eq_(200, response.status_code)
        feed = feedparser.parse(response.data)
        eq_(1, len(feed['entries']))
        [entry] = feed['entries']
        eq_(another_work.title, entry['title'])

        with self.request_context_with_library("/?after=1"):
            response = self.manager.work_controller.contributor(name, None, None)

        eq_(200, response.status_code)
        feed = feedparser.parse(response.data)
        eq_(1, len(feed['entries']))
        [entry] = feed['entries']
        eq_(self.english_1.title, entry['title'])

    def test_permalink(self):
        with self.request_context_with_library("/"):
            response = self.manager.work_controller.permalink(self.identifier.type, self.identifier.identifier)
            annotator = CirculationManagerAnnotator(None, None, self._default_library)
            expect = etree.tostring(
                AcquisitionFeed.single_entry(
                    self._db, self.english_1, annotator
                )
            )
        eq_(200, response.status_code)
        eq_(expect, response.data)
        eq_(OPDSFeed.ENTRY_TYPE, response.headers['Content-Type'])

    def test_recommendations(self):
        # TODO: This test creates its own work to avoid
        # Gutenberg books getting filtered out, since
        # the recommendation lanes have all audiences,
        # including children and ya.
        self.work = self._work("Quite British", "John Bull", with_license_pool=True, language="eng", data_source_name=DataSource.OVERDRIVE)
        [self.lp] = self.work.license_pools
        self.edition = self.lp.presentation_edition
        self.datasource = self.lp.data_source.name
        self.identifier = self.lp.identifier

        # Prep an empty recommendation.
        source = DataSource.lookup(self._db, self.datasource)
        metadata = Metadata(source)
        mock_api = MockNoveListAPI(self._db)
        mock_api.setup(metadata)

        SessionManager.refresh_materialized_views(self._db)
        args = [self.identifier.type,
                self.identifier.identifier]
        kwargs = dict(novelist_api=mock_api)
        
        # We get a 400 response if the pagination data is bad.
        with self.request_context_with_library('/?size=abc'):
            response = self.manager.work_controller.recommendations(
                *args, **kwargs
            )
            eq_(400, response.status_code)

        # Or if the facet data is bad.
        mock_api.setup(metadata)
        with self.request_context_with_library('/?order=nosuchorder'):
            response = self.manager.work_controller.recommendations(
                *args, **kwargs
            )
            eq_(400, response.status_code)

        # Show it working.
        mock_api.setup(metadata)
        with self.request_context_with_library('/'):
            response = self.manager.work_controller.recommendations(
                *args, **kwargs
            )
        eq_(200, response.status_code)
        feed = feedparser.parse(response.data)
        eq_('Recommended Books', feed['feed']['title'])
        eq_(0, len(feed['entries']))

       
        # Delete the cache and prep a recommendation result.
        [cached_empty_feed] = self._db.query(CachedFeed).all()
        self._db.delete(cached_empty_feed)
        metadata.recommendations = [self.work.license_pools[0].identifier]
        mock_api.setup(metadata)

        SessionManager.refresh_materialized_views(self._db)
        with self.request_context_with_library('/'):
            response = self.manager.work_controller.recommendations(
                self.identifier.type, self.identifier.identifier,
                novelist_api=mock_api
            )
        # A feed is returned with the proper recommendation.
        eq_(200, response.status_code)
        feed = feedparser.parse(response.data)

        eq_('Recommended Books', feed.feed.title)
        [entry] = feed.entries
        eq_(self.work.title, entry['title'])
        author = self.work.presentation_edition.author_contributors[0]
        expected_author_name = author.display_name or author.sort_name
        eq_(expected_author_name, entry.author)

        # The feed has facet links.
        links = feed['feed']['links']
        facet_links = [link for link in links if link['rel'] == 'http://opds-spec.org/facet']
        eq_(9, len(facet_links))

        with self.request_context_with_library('/'):
            response = self.manager.work_controller.recommendations(
                self.identifier.type, self.identifier.identifier
            )

        eq_(404, response.status_code)
        eq_("http://librarysimplified.org/terms/problem/unknown-lane", response.uri)

        another_work = self._work("Before Quite British", "Not Before John Bull", with_open_access_download=True, data_source_name=DataSource.OVERDRIVE)

        # Delete the cache again and prep a recommendation result.
        [cached_feed] = self._db.query(CachedFeed).all()
        self._db.delete(cached_feed)

        metadata.recommendations = [
            self.work.license_pools[0].identifier,
            another_work.license_pools[0].identifier,
        ]
        mock_api.setup(metadata)

        # Facets work.
        SessionManager.refresh_materialized_views(self._db)
        with self.request_context_with_library("/?order=title"):
            response = self.manager.work_controller.recommendations(
                self.identifier.type, self.identifier.identifier,
                novelist_api=mock_api
            )

        eq_(200, response.status_code)
        feed = feedparser.parse(response.data)
        eq_(2, len(feed['entries']))
        [entry1, entry2] = feed['entries']
        eq_(another_work.title, entry1['title'])
        eq_(self.work.title, entry2['title'])

        metadata.recommendations = [
            self.work.license_pools[0].identifier,
            another_work.license_pools[0].identifier,
        ]
        mock_api.setup(metadata)

        with self.request_context_with_library("/?order=author"):
            response = self.manager.work_controller.recommendations(
                self.identifier.type, self.identifier.identifier,
                novelist_api=mock_api
            )

        eq_(200, response.status_code)
        feed = feedparser.parse(response.data)
        eq_(2, len(feed['entries']))
        [entry1, entry2] = feed['entries']
        eq_(self.work.title, entry1['title'])
        eq_(another_work.title, entry2['title'])

        metadata.recommendations = [
            self.work.license_pools[0].identifier,
            another_work.license_pools[0].identifier,
        ]
        mock_api.setup(metadata)

        # Pagination works.
        with self.request_context_with_library("/?size=1&order=title"):
            response = self.manager.work_controller.recommendations(
                self.identifier.type, self.identifier.identifier,
                novelist_api=mock_api
            )

        eq_(200, response.status_code)
        feed = feedparser.parse(response.data)
        eq_(1, len(feed['entries']))
        [entry] = feed['entries']
        eq_(another_work.title, entry['title'])

        metadata.recommendations = [
            self.work.license_pools[0].identifier,
            another_work.license_pools[0].identifier,
        ]
        mock_api.setup(metadata)

        with self.request_context_with_library("/?after=1&order=title"):
            response = self.manager.work_controller.recommendations(
                self.identifier.type, self.identifier.identifier,
                novelist_api=mock_api
            )

        eq_(200, response.status_code)
        feed = feedparser.parse(response.data)
        eq_(1, len(feed['entries']))
        [entry] = feed['entries']
        eq_(self.work.title, entry['title'])

    def test_related_books(self):
        # A book with no related books returns a ProblemDetail.

        # TODO: This test creates its own work to avoid
        # Gutenberg books getting filtered out, since
        # the recommendation lanes have all audiences,
        # including children and ya.
        self.work = self._work("Quite British", "John Bull", with_license_pool=True, language="eng", data_source_name=DataSource.OVERDRIVE)
        [self.lp] = self.work.license_pools
        self.edition = self.lp.presentation_edition
        self.datasource = self.lp.data_source.name
        self.identifier = self.lp.identifier

        # Remove contribution.
        [contribution] = self.edition.contributions
        [original, role] = [contribution.contributor, contribution.role]
        self._db.delete(contribution)
        self._db.commit()

        with self.request_context_with_library('/'):
            response = self.manager.work_controller.related(
                self.identifier.type, self.identifier.identifier
            )

        eq_(404, response.status_code)
        eq_("http://librarysimplified.org/terms/problem/unknown-lane", response.uri)

        # Prep book with a contribution, a series, and a recommendation.
        self.lp.presentation_edition.add_contributor(original, role)
        same_author = self._work(
            "What is Sunday?", original.display_name,
            language="eng", fiction=True, with_open_access_download=True,
            data_source_name=DataSource.OVERDRIVE
        )
        duplicate = same_author.presentation_edition.contributions[0].contributor
        original.display_name = duplicate.display_name = u"John Bull"

        self.edition.series = u"Around the World"
        self.edition.series_position = 1

        same_series_work = self._work(
            title="ZZZ", authors="ZZZ ZZZ", with_license_pool=True,
            series="Around the World", data_source_name=DataSource.OVERDRIVE)
        same_series_work.presentation_edition.series_position = 0
        self.work.calculate_presentation(
            PresentationCalculationPolicy(regenerate_opds_entries=True),
            DummyExternalSearchIndex()
        )
        SessionManager.refresh_materialized_views(self._db)

        source = DataSource.lookup(self._db, self.datasource)
        metadata = Metadata(source)
        mock_api = MockNoveListAPI(self._db)
        metadata.recommendations = [same_author.license_pools[0].identifier]
        mock_api.setup(metadata)

        # A grouped feed is returned with all of the related books
        with self.request_context_with_library('/'):
            response = self.manager.work_controller.related(
                self.identifier.type, self.identifier.identifier,
                novelist_api=mock_api
            )
        eq_(200, response.status_code)
        feed = feedparser.parse(response.data)
        eq_(5, len(feed['entries']))

        def collection_link(entry):
            [link] = [l for l in entry['links'] if l['rel']=='collection']
            return link['title'], link['href']

        # This feed contains five books: one recommended,
        # one in the same series, and two by the same author.
        recommendations = []
        same_series = []
        same_contributor = []
        feeds_with_original_book = []
        for e in feed['entries']:
            for link in e['links']:
                if link['rel'] != 'collection':
                    continue
                if link['title'] == 'Recommendations for Quite British by John Bull':
                    recommendations.append(e)
                elif link['title'] == 'Around the World':
                    same_series.append(e)
                elif link['title'] == 'John Bull':
                    same_contributor.append(e)
                if e['title'] == self.work.title:
                    feeds_with_original_book.append(link['title'])

        [recommendation] = recommendations
        title, href = collection_link(recommendation)
        work_url = "/works/%s/%s/" % (self.identifier.type, self.identifier.identifier)
        expected = urllib.quote(work_url + 'recommendations')
        eq_(True, href.endswith(expected))

        # All books in the series are in the series feed.
        for book in same_series:
            title, href = collection_link(book)
            expected_series_link = 'series/%s/eng/Adult' % urllib.quote("Around the World")
            eq_(True, href.endswith(expected_series_link))

        # The other book by this contributor is in the contributor feed.
        for contributor in same_contributor:
            title, href = collection_link(contributor)
            expected_contributor_link = urllib.quote('contributor/John Bull/eng/')
            eq_(True, href.endswith(expected_contributor_link))

        # The book for which we got recommendations is itself listed in the
        # series feed and in the 'books by this author' feed.
        eq_(set(["John Bull", "Around the World"]),
            set(feeds_with_original_book))

        # The series feed is sorted by series position.
        [series_e1, series_e2] = same_series
        eq_(same_series_work.title, series_e1['title'])
        eq_(self.work.title, series_e2['title'])

    def test_report_problem_get(self):
        with self.request_context_with_library("/"):
            response = self.manager.work_controller.report(self.identifier.type, self.identifier.identifier)
        eq_(200, response.status_code)
        eq_("text/uri-list", response.headers['Content-Type'])
        for i in Complaint.VALID_TYPES:
            assert i in response.data

    def test_report_problem_post_success(self):
        error_type = random.choice(list(Complaint.VALID_TYPES))
        data = json.dumps({ "type": error_type,
                            "source": "foo",
                            "detail": "bar"}
        )
        with self.request_context_with_library("/", method="POST", data=data):
            response = self.manager.work_controller.report(self.identifier.type, self.identifier.identifier)
        eq_(201, response.status_code)
        [complaint] = self.lp.complaints
        eq_(error_type, complaint.type)
        eq_("foo", complaint.source)
        eq_("bar", complaint.detail)

    def test_series(self):
        # If no series is given, a ProblemDetail is returned.
        with self.request_context_with_library('/'):
            response = self.manager.work_controller.series("", None, None)
        eq_(404, response.status_code)
        eq_("http://librarysimplified.org/terms/problem/unknown-lane", response.uri)

        series_name = "Like As If Whatever Mysteries"
        work = self._work(with_open_access_download=True, series=series_name)

        # Similarly if the pagination data is bad.
        with self.request_context_with_library('/?size=abc'):
            response = self.manager.work_controller.series(series_name, None, None)
            eq_(400, response.status_code)

        # Or if the facet data is bad
        with self.request_context_with_library('/?order=nosuchorder'):
            response = self.manager.work_controller.series(series_name, None, None)
            eq_(400, response.status_code)
            
        # If the work is in a series, a feed is returned.
        SessionManager.refresh_materialized_views(self._db)
        with self.request_context_with_library('/'):
            response = self.manager.work_controller.series(series_name, None, None)
        eq_(200, response.status_code)
        feed = feedparser.parse(response.data)
        eq_(series_name, feed['feed']['title'])
        [entry] = feed['entries']
        eq_(work.title, entry['title'])

        # The feed has facet links.
        links = feed['feed']['links']
        facet_links = [link for link in links if link['rel'] == 'http://opds-spec.org/facet']
        eq_(10, len(facet_links))

        another_work = self._work(
            title="000", authors="After Default Work",
            with_open_access_download=True, series=series_name
        )

        # Delete the cache
        [cached_feed] = self._db.query(CachedFeed).all()
        self._db.delete(cached_feed)
        
        # Facets work.
        SessionManager.refresh_materialized_views(self._db)
        with self.request_context_with_library("/?order=title"):
            response = self.manager.work_controller.series(series_name, None, None)

        eq_(200, response.status_code)
        feed = feedparser.parse(response.data)
        eq_(2, len(feed['entries']))
        [entry1, entry2] = feed['entries']
        eq_(another_work.title, entry1['title'])
        eq_(work.title, entry2['title'])

        with self.request_context_with_library("/?order=author"):
            response = self.manager.work_controller.series(series_name, None, None)

        eq_(200, response.status_code)
        feed = feedparser.parse(response.data)
        eq_(2, len(feed['entries']))
        [entry1, entry2] = feed['entries']
        eq_(work.title, entry1['title'])
        eq_(another_work.title, entry2['title'])

        work.presentation_edition.series_position = 0
        another_work.presentation_edition.series_position = 1

        SessionManager.refresh_materialized_views(self._db)
        with self.request_context_with_library("/?order=series"):
            response = self.manager.work_controller.series(series_name, None, None)

        eq_(200, response.status_code)
        feed = feedparser.parse(response.data)
        eq_(2, len(feed['entries']))
        [entry1, entry2] = feed['entries']
        eq_(work.title, entry1['title'])
        eq_(another_work.title, entry2['title'])

        # Series is the default facet.
        with self.request_context_with_library("/"):
            response = self.manager.work_controller.series(series_name, None, None)

        eq_(200, response.status_code)
        feed = feedparser.parse(response.data)
        eq_(2, len(feed['entries']))
        [entry1, entry2] = feed['entries']
        eq_(work.title, entry1['title'])
        eq_(another_work.title, entry2['title'])

        # Pagination works.
        with self.request_context_with_library("/?size=1&order=title"):
            response = self.manager.work_controller.series(series_name, None, None)

        eq_(200, response.status_code)
        feed = feedparser.parse(response.data)
        eq_(1, len(feed['entries']))
        [entry] = feed['entries']
        eq_(another_work.title, entry['title'])

        with self.request_context_with_library("/?after=1&order=title"):
            response = self.manager.work_controller.series(series_name, None, None)

        eq_(200, response.status_code)
        feed = feedparser.parse(response.data)
        eq_(1, len(feed['entries']))
        [entry] = feed['entries']
        eq_(work.title, entry['title'])

        # Language restrictions can remove books that would otherwise be
        # in the feed.
        with self.request_context_with_library("/"):
            response = self.manager.work_controller.series(
                series_name, 'fre', None
            )
            feed = feedparser.parse(response.data)
            eq_(0, len(feed['entries']))


class TestFeedController(CirculationControllerTest):

    BOOKS = list(CirculationControllerTest.BOOKS) + [
        ["english_2", "Totally American", "Uncle Sam", "eng", False],
        ["french_1", u"Très Français", "Marianne", "fre", False],
    ]
    
    def test_feed(self):
        SessionManager.refresh_materialized_views(self._db)

        # Set up configuration settings for links.
        for rel, value in [(CirculationManagerAnnotator.TERMS_OF_SERVICE, "a"),
                           (CirculationManagerAnnotator.PRIVACY_POLICY, "b"),
                           (CirculationManagerAnnotator.COPYRIGHT, "c"),
                           (CirculationManagerAnnotator.ABOUT, "d"),
                           ]:
            ConfigurationSetting.for_library(rel, self._default_library).value = value

        with self.request_context_with_library("/"):
            response = self.manager.opds_feeds.feed(
                self.english_adult_fiction.id
            )

            assert self.english_1.title in response.data
            assert self.english_2.title not in response.data
            assert self.french_1.title not in response.data

            feed = feedparser.parse(response.data)
            links = feed['feed']['links']
            by_rel = dict()
            for i in links:
                by_rel[i['rel']] = i['href']

            eq_("a", by_rel[CirculationManagerAnnotator.TERMS_OF_SERVICE])
            eq_("b", by_rel[CirculationManagerAnnotator.PRIVACY_POLICY])
            eq_("c", by_rel[CirculationManagerAnnotator.COPYRIGHT])
            eq_("d", by_rel[CirculationManagerAnnotator.ABOUT])

    def test_multipage_feed(self):
        self._work("fiction work", language="eng", fiction=True, with_open_access_download=True)
        SessionManager.refresh_materialized_views(self._db)
        with self.request_context_with_library("/?size=1"):
            lane_id = self.english_adult_fiction.id
            response = self.manager.opds_feeds.feed(lane_id)

            feed = feedparser.parse(response.data)
            entries = feed['entries']
            
            eq_(1, len(entries))

            links = feed['feed']['links']
            next_link = [x for x in links if x['rel'] == 'next'][0]['href']
            assert 'after=1' in next_link
            assert 'size=1' in next_link

            facet_links = [x for x in links if x['rel'] == 'http://opds-spec.org/facet']
            assert any('order=title' in x['href'] for x in facet_links)
            assert any('order=author' in x['href'] for x in facet_links)

            search_link = [x for x in links if x['rel'] == 'search'][0]['href']
            assert search_link.endswith('/search/%s' % lane_id)

            shelf_link = [x for x in links if x['rel'] == 'http://opds-spec.org/shelf'][0]['href']
            assert shelf_link.endswith('/loans/')

    def test_bad_order_gives_problem_detail(self):
        with self.request_context_with_library("/?order=nosuchorder"):
            response = self.manager.opds_feeds.feed(
                self.english_adult_fiction.id
            )
            eq_(400, response.status_code)
            eq_(
                "http://librarysimplified.org/terms/problem/invalid-input", 
                response.uri
            )

    def test_bad_pagination_gives_problem_detail(self):
        with self.request_context_with_library("/?size=abc"):
            response = self.manager.opds_feeds.feed(
                self.english_adult_fiction.id
            )
            eq_(400, response.status_code)
            eq_(
                "http://librarysimplified.org/terms/problem/invalid-input", 
                response.uri
            )            

    def test_groups(self):
        ConfigurationSetting.sitewide(
            self._db, AcquisitionFeed.GROUPED_MAX_AGE_POLICY).value = 10
        library = self._default_library
        library.setting(library.MINIMUM_FEATURED_QUALITY).value = 0
        library.setting(library.FEATURED_LANE_SIZE).value = 2
        
        SessionManager.refresh_materialized_views(self._db)

        # Initial setup gave us two English works and a French work.
        # Load up with a couple more English works to show that
        # the groups lane cuts off at FEATURED_LANE_SIZE.
        for i in range(2):
            self._work("english work %i" % i, language="eng", fiction=True, with_open_access_download=True)
        
        with self.request_context_with_library("/"):
            response = self.manager.opds_feeds.groups(None)

            feed = feedparser.parse(response.data)
            entries = feed['entries']

            counter = Counter()
            for entry in entries:
                links = [x for x in entry.links if x['rel'] == 'collection']
                for link in links:
                    counter[link['title']] += 1

            # In default_config, the two top-level lanes are "English"
            # (a language in SMALL_COLLECTION_LANGUAGES) and "Other
            # Languages" (which covers TINY_COLLECTION_LANGUAGES).
            #
            # There are several English works, but we're cut off at
            # two due to FEATURED_LANE_SIZE. There is one "Other
            # Languages" work - the French work created when this test
            # was initialized.
            eq_(2, counter['English'])
            eq_(1, counter['Other Languages'])

    def test_search(self):
        # Update the index for two works.
        # english_1 is "Quite British" by John Bull
        # english_2 is "Totally American" by Uncle Sam
        self.manager.external_search.bulk_update(
            [self.english_1, self.english_2]
        )

        # Update the materialized view to make sure the works show up.
        SessionManager.refresh_materialized_views(self._db)

        # Execute a search query designed to find the second one.
        with self.request_context_with_library("/?q=t&size=1&after=1"):
            # First, try the top-level lane.
            response = self.manager.opds_feeds.search(None)
            feed = feedparser.parse(response.data)
            entries = feed['entries']
            eq_(1, len(entries))
            entry = entries[0]
            author = self.english_2.presentation_edition.author_contributors[0]
            expected_author_name = author.display_name or author.sort_name
            eq_(expected_author_name, entry.author)

            assert 'links' in entry
            assert len(entry.links) > 0

            borrow_links = [link for link in entry.links if link.rel == 'http://opds-spec.org/acquisition/borrow']
            eq_(1, len(borrow_links))

            next_links = [link for link in feed['feed']['links'] if link.rel == 'next']
            eq_(1, len(next_links))

            previous_links = [link for link in feed['feed']['links'] if link.rel == 'previous']
            eq_(1, len(previous_links))

            # The query also works in a different searchable lane.
            english = self._lane("English", languages=["eng"])
            response = self.manager.opds_feeds.search(english.id)
            feed = feedparser.parse(response.data)
            entries = feed['entries']
            eq_(1, len(entries))


class TestAnalyticsController(CirculationControllerTest):
    def setup(self):
        super(TestAnalyticsController, self).setup()
        [self.lp] = self.english_1.license_pools
        self.identifier = self.lp.identifier

    def test_track_event(self):
        integration, ignore = create(
            self._db, ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol="core.local_analytics_provider",
        )
        self.manager.analytics = Analytics(self._db)

        with self.request_context_with_library("/"):
            response = self.manager.analytics_controller.track_event(self.identifier.type, self.identifier.identifier, "invalid_type")
            eq_(400, response.status_code)
            eq_(INVALID_ANALYTICS_EVENT_TYPE.uri, response.uri)

        with self.request_context_with_library("/"):
            response = self.manager.analytics_controller.track_event(self.identifier.type, self.identifier.identifier, "open_book")
            eq_(200, response.status_code)
            
            circulation_event = get_one(
                self._db, CirculationEvent,
                type="open_book",
                license_pool=self.lp
            )
            assert circulation_event != None


class TestDeviceManagementProtocolController(ControllerTest):

    def setup(self):
        super(TestDeviceManagementProtocolController, self).setup()
        self.initialize_adobe(self.library, self.libraries)
        self.auth = dict(Authorization=self.valid_auth)

        # Since our library doesn't have its Adobe configuration
        # enabled, the Device Management Protocol controller has not
        # been enabled.
        eq_(None, self.manager.adobe_device_management)

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
            AuthdataUtility.ADOBE_ACCOUNT_ID_PATRON_IDENTIFIER,
            self.default_patron
        )
    
    def test_link_template_header(self):
        """Test the value of the Link-Template header used in 
        device_id_list_handler.
        """
        with self.request_context_with_library("/"):
            headers = self.controller.link_template_header
            eq_(1, len(headers))
            template = headers['Link-Template']
            expected_url = url_for("adobe_drm_device", library_short_name=self.library.short_name, device_id="{id}", _external=True)
            expected_url = expected_url.replace("%7Bid%7D", "{id}")
            eq_('<%s>; rel="item"' % expected_url, template)

    def test__request_handler_failure(self):
        """You cannot create a DeviceManagementRequestHandler
        without providing a patron.
        """
        result = self.controller._request_handler(None)

        assert isinstance(result, ProblemDetail)
        eq_(INVALID_CREDENTIALS.uri, result.uri)
        eq_("No authenticated patron", result.detail)
            
    def test_device_id_list_handler_post_success(self):
        # The patron has no credentials, and thus no registered devices.
        eq_([], self.default_patron.credentials)
        headers = dict(self.auth)
        headers['Content-Type'] = self.controller.DEVICE_ID_LIST_MEDIA_TYPE
        with self.request_context_with_library(
            "/", method='POST', headers=headers, data="device"
        ):
            self.controller.authenticated_patron_from_request()
            response = self.controller.device_id_list_handler()
            eq_(200, response.status_code)

            # We just registered a new device with the patron. This
            # automatically created an appropriate Credential for
            # them.
            [credential] = self.default_patron.credentials
            eq_(DataSource.INTERNAL_PROCESSING, credential.data_source.name)
            eq_(AuthdataUtility.ADOBE_ACCOUNT_ID_PATRON_IDENTIFIER,
                credential.type)

            eq_(['device'],
                [x.device_identifier for x in credential.drm_device_identifiers]
            )

    def test_device_id_list_handler_get_success(self):
        credential = self._create_credential()
        credential.register_drm_device_identifier("device1")
        credential.register_drm_device_identifier("device2")
        with self.request_context_with_library("/", headers=self.auth):
            self.controller.authenticated_patron_from_request()
            response = self.controller.device_id_list_handler()
            eq_(200, response.status_code)
            
            # We got a list of device IDs.
            eq_(self.controller.DEVICE_ID_LIST_MEDIA_TYPE,
                response.headers['Content-Type'])
            eq_("device1\ndevice2", response.data)

            # We got a URL Template (see test_link_template_header())
            # that explains how to address any particular device ID.
            expect = self.controller.link_template_header
            for k, v in expect.items():
                assert response.headers[k] == v

    def device_id_list_handler_bad_auth(self):
        with self.request_context_with_library("/"):
            self.controller.authenticated_patron_from_request()
            response = self.manager.adobe_vendor_id.device_id_list_handler()
            assert isinstance(response, ProblemDetail)
            eq_(401, response.status_code)

    def device_id_list_handler_bad_method(self):
        with self.request_context_with_library(
            "/", method='DELETE', headers=self.auth
        ):
            self.controller.authenticated_patron_from_request()
            response = self.controller.device_id_list_handler()
            assert isinstance(response, ProblemDetail)
            eq_(405, response.status_code)

    def test_device_id_list_handler_too_many_simultaneous_registrations(self):
        """We only allow registration of one device ID at a time."""
        headers = dict(self.auth)
        headers['Content-Type'] = self.controller.DEVICE_ID_LIST_MEDIA_TYPE
        with self.request_context_with_library(
            "/", method='POST', headers=headers, data="device1\ndevice2"
        ):
            self.controller.authenticated_patron_from_request()
            response = self.controller.device_id_list_handler()
            eq_(413, response.status_code)
            eq_("You may only register one device ID at a time.", response.detail)

    def test_device_id_list_handler_wrong_media_type(self):
        headers = dict(self.auth)
        headers['Content-Type'] = "text/plain"
        with self.request_context_with_library(
            "/", method='POST', headers=headers, data="device1\ndevice2"
        ):
            self.controller.authenticated_patron_from_request()
            response = self.controller.device_id_list_handler()
            eq_(415, response.status_code)
            eq_("Expected vnd.librarysimplified/drm-device-id-list document.",
                response.detail)

    def test_device_id_handler_success(self):
        credential = self._create_credential()
        credential.register_drm_device_identifier("device")

        with self.request_context_with_library(
                "/", method='DELETE', headers=self.auth
        ):
            patron = self.controller.authenticated_patron_from_request()
            response = self.controller.device_id_handler("device")
            eq_(200, response.status_code)

    def test_device_id_handler_bad_auth(self):
        with self.request_context_with_library("/", method='DELETE'):
            with temp_config() as config:
                config[Configuration.INTEGRATIONS] = {
                    "Circulation Manager" : { "url" : "http://foo/" }
                }
                patron = self.controller.authenticated_patron_from_request()
                response = self.controller.device_id_handler("device")
            assert isinstance(response, ProblemDetail)
            eq_(401, response.status_code)

    def test_device_id_handler_bad_method(self):
        with self.request_context_with_library("/", method='POST', headers=self.auth):
            patron = self.controller.authenticated_patron_from_request()
            response = self.controller.device_id_handler("device")
            assert isinstance(response, ProblemDetail)
            eq_(405, response.status_code)
            eq_("Only DELETE is supported.", response.detail)


class TestODLNotificationController(ControllerTest):
    """Test that an ODL distributor can notify the circulation manager
    when a loan's status changes."""

    def test_notify_success(self):
        collection = MockODLWithConsolidatedCopiesAPI.mock_collection(self._db)
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
            eq_(200, response.status_code)

            # The pool's availability has been updated.
            api = self.manager.circulation_apis[self._default_library.id].api_for_license_pool(loan.license_pool)
            eq_([loan.license_pool], api.availability_updated_for)

    def test_notify_errors(self):
        # No loan.
        with self.request_context_with_library("/", method="POST"):
            response = self.manager.odl_notification_controller.notify(self._str)
            eq_(NO_ACTIVE_LOAN.uri, response.uri)

        # Loan from a non-ODL collection.
        patron = self._patron()
        pool = self._licensepool(None)
        loan, ignore = pool.loan_to(patron)
        loan.external_identifier = self._str

        with self.request_context_with_library("/", method="POST"):
            response = self.manager.odl_notification_controller.notify(loan.id)
            eq_(INVALID_LOAN_FOR_ODL_NOTIFICATION, response)

class TestProfileController(ControllerTest):
    """Test that a client can interact with the User Profile Management
    Protocol.
    """

    def setup(self):
        super(TestProfileController, self).setup()

        # Nothing will happen to this patron. This way we can verify
        # that a patron can only see/modify their own profile.
        self.other_patron = self._patron()
        self.other_patron.synchronize_annotations = False
        self.auth = dict(Authorization=self.valid_auth)
        
    def test_get(self):
        """Verify that a patron can see their own profile."""
        with self.request_context_with_library(
                "/", method='GET', headers=self.auth
        ):
            patron = self.controller.authenticated_patron_from_request()
            patron.synchronize_annotations = True
            response = self.manager.profiles.protocol()
            eq_("200 OK", response.status)
            data = json.loads(response.data)
            settings = data['settings']
            eq_(True, settings[ProfileStorage.SYNCHRONIZE_ANNOTATIONS])

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
            eq_(None, request_patron.synchronize_annotations)

            # This means we can't create annotations for them.
            assert_raises(ValueError,  Annotation.get_one_or_create,
                self._db, patron=request_patron, identifier=identifier
            )
            
            # But by sending a PUT request...
            response = self.manager.profiles.protocol()

            # ...we can change synchronize_annotations to True.
            eq_(True, request_patron.synchronize_annotations)

            # The other patron is unaffected.
            eq_(False, self.other_patron.synchronize_annotations)
            
        # Now we can create an annotation for the patron who enabled
        # annotation sync.
        annotation = Annotation.get_one_or_create(
            self._db, patron=request_patron, identifier=identifier)
        eq_(1, len(request_patron.annotations))
        
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
            eq_(False, request_patron.synchronize_annotations)
            eq_(0, len(request_patron.annotations))

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
            eq_(415, response.status_code)
            eq_("Expected vnd.librarysimplified/user-profile+json",
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

    def setup(self):
        # We will be calling circulation_manager_setup ourselves,
        # because we want objects like Libraries to be created in the
        # scoped session.
        super(TestScopedSession, self).setup(
            app._db, set_up_circulation_manager=False
        )
        
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
            eq_("1024", identifier.identifier)

            # It doesn't show up in self._db, the database session
            # used by most other unit tests, because it was created
            # within the (still-active) context of a Flask request,
            # which happens within a nested database transaction.
            eq_([], self._db.query(Identifier).all())

            # It shows up in the flask_scoped_session object that
            # created the request-scoped session, because within the
            # context of a request, running database queries on that object
            # actually runs them against your request-scoped session.
            [identifier] = self.app.manager._db.query(Identifier).all()
            eq_("1024", identifier.identifier)

            # But if we were to use flask_scoped_session to create a
            # brand new session, it would not see the Identifier,
            # because it's running in a different database session.
            new_session = self.app.manager._db.session_factory()
            eq_([], new_session.query(Identifier).all())

            # When the index controller runs in the request context,
            # it doesn't store anything that's associated with the
            # scoped session.
            flask.request.library = self._default_library
            response = self.app.manager.index_controller()
            eq_(302, response.status_code)
            
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
        eq_([], self._db.query(Identifier).all())

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
            eq_(302, response.status_code)

        # The two Flask requests got different sessions, neither of
        # which is the same as self._db, the unscoped database session
        # used by most other unit tests.
        assert session1 != session2
