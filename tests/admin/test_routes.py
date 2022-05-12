import contextlib
import logging
import flask
from flask import Response
from werkzeug.exceptions import MethodNotAllowed
import os

from core.app_server import ErrorHandler
from core.model import (
    Admin,
    ConfigurationSetting,
    get_one_or_create
)

from api import app
from api import routes as api_routes
from api.config import Configuration
from api.controller import CirculationManager
from api.admin.controller import AdminController
from api.admin.controller import setup_admin_controllers
from api.admin.problem_details import *
from api.routes import (
    exception_handler,
    h as error_handler_object,
)
from api.admin import routes
from ..test_controller import ControllerTest
from ..test_routes import (
    MockApp,
    MockManager,
    MockController,
    RouteTest,
    RouteTestFixtures,
)

class MockAdminApp(object):
    """Pretends to be a Flask application with a configured
    CirculationManager and Admin routes.
    """
    def __init__(self):
        self.manager = MockAdminManager()

class MockAdminManager(MockManager):
    def __getattr__(self, controller_name):
        return self._cache.setdefault(
            controller_name, MockAdminController(controller_name)
        )

class MockAdminController(MockController):
    AUTHENTICATED_ADMIN = "i am a mock admin"

    def authenticated_admin_from_request(self):
        if self.authenticated:
            admin = object()
            flask.request.admin = self.AUTHENTICATED_ADMIN
            return self.AUTHENTICATED_ADMIN
        # For the redirect case we want to return a Problem Detail.
        elif self.authenticated_problem_detail:
            return INVALID_ADMIN_CREDENTIALS
        else:
            return Response(
                "authenticated_admin_from_request called without authorizing",
                401
            )

    def get_csrf_token(self):
        if self.csrf_token:
            return "some token"
        else:
            return INVALID_CSRF_TOKEN

    def bulk_circulation_events(self):
        return "data", "date", "date_end", "library"


class AdminRouteTest(ControllerTest, RouteTestFixtures):

    # The first time setup_method() is called, it will instantiate a real
    # CirculationManager object and store it in REAL_CIRCULATION_MANAGER.
    # We only do this once because it takes about a second to instantiate
    # this object. Calling any of this object's methods could be problematic,
    # since it's probably left over from a previous test, but we won't be
    # calling any methods -- we just want to verify the _existence_,
    # in a real CirculationManager, of the methods called in
    # routes.py.
    @classmethod
    def setup_class(cls):
        super(AdminRouteTest, cls).setup_class()
        cls.REAL_CIRCULATION_MANAGER = None

    def setup_method(self):
        self.setup_circulation_manager = False
        super(AdminRouteTest, self).setup_method()
        if not self.REAL_CIRCULATION_MANAGER:
            library = self._default_library
            # Set up the necessary configuration so that when we
            # instantiate the CirculationManager it gets an
            # adobe_vendor_id controller -- this wouldn't normally
            # happen because most circulation managers don't need such a
            # controller.
            self.initialize_adobe(library, [library])
            self.adobe_vendor_id.password = self.TEST_NODE_VALUE
            circ_manager = CirculationManager(self._db, testing=True)
            manager = AdminController(circ_manager)
            setup_admin_controllers(circ_manager)
            self.REAL_CIRCULATION_MANAGER = circ_manager

        app = MockAdminApp()
        # Also mock the api app in order to use functions from api/routes
        api_app = MockApp()
        self.routes = routes
        self.api_routes = api_routes
        self.manager = app.manager
        self.original_app = self.routes.app
        self.original_api_app = self.api_routes.app
        self.resolver = self.original_app.url_map.bind('', '/')

        # For convenience, set self.controller to a specific controller
        # whose routes are being tested.
        controller_name = getattr(self, 'CONTROLLER_NAME', None)
        if controller_name:
            self.controller = getattr(self.manager, controller_name)

            # Make sure there's a controller by this name in the real
            # CirculationManager.
            self.real_controller = getattr(
                self.REAL_CIRCULATION_MANAGER, controller_name
            )
        else:
            self.real_controller = None

        self.routes.app = app
        # Need to also mock the route app from /api/routes.
        self.api_routes.app = api_app

    def teardown_method(self):
        super(ControllerTest, self).teardown_method()
        self.routes.app = self.original_app
        self.api_routes.app = self.original_api_app

    def assert_authenticated_request_calls(self, url, method, *args, **kwargs):
        """First verify that an unauthenticated request fails. Then make an
        authenticated request to `url` and verify the results, as with
        assert_request_calls.
        """
        authentication_required = kwargs.pop("authentication_required", True)

        http_method = kwargs.pop('http_method', 'GET')
        response = self.request(url, http_method)
        if authentication_required:
            assert 401 == response.status_code
            assert ("authenticated_admin_from_request called without authorizing" ==
                response.data)
        else:
            assert 200 == response.status_code

        # Set a variable so that authenticated_admin_from_request
        # will succeed, and try again.
        self.manager.admin_sign_in_controller.authenticated = True
        try:
            kwargs['http_method'] = http_method
            # The file response case is specific to the bulk circulation
            # events route where a CSV file is returned.
            if kwargs.get('file_response', None) is not None:
                self.assert_file_response(url, *args, **kwargs)
            else:
                self.assert_request_calls(url, method, *args, **kwargs)
        finally:
            # Un-set authentication for the benefit of future
            # assertions in this test function.
            self.manager.admin_sign_in_controller.authenticated = False

    def assert_file_response(self, url, *args, **kwargs):
        http_method = kwargs.pop('http_method', 'GET')
        response = self.request(url, http_method)

        assert response.headers['Content-type'] == 'text/csv'

    def assert_redirect_call(self, url, *args, **kwargs):

        # Correctly render the sign in again template when the admin
        # is authenticated and there is a csrf token.
        self.manager.admin_sign_in_controller.csrf_token = True
        self.manager.admin_sign_in_controller.authenticated = True
        http_method = kwargs.pop('http_method', 'GET')
        response = self.request(url, http_method)

        # A Flask template string is returned.
        assert "You are now logged in" in response

        # Even if the admin is authenticated but there is no
        # csrf token, a redirect will occur to sign the admin in.
        self.manager.admin_sign_in_controller.csrf_token = False
        response = self.request(url, http_method)

        assert 302 == response.status_code
        assert "Redirecting..." in response.data

        # If there is a csrf token but the Admin is not authenticated,
        # redirect them.

        self.manager.admin_sign_in_controller.csrf_token = True
        self.manager.admin_sign_in_controller.authenticated = False
        # For this case we want the function to return a problem detail.
        self.manager.admin_sign_in_controller.authenticated_problem_detail = True
        response = self.request(url, http_method)

        assert 302 == response.status_code
        assert "Redirecting..." in response.data

        # Not being authenticated and not having a csrf token fail
        # redirects the admin to sign in again.
        self.manager.admin_sign_in_controller.csrf_token = False
        self.manager.admin_sign_in_controller.authenticated = False
        response = self.request(url, http_method)

        # No admin or csrf token so redirect.
        assert 302 == response.status_code
        assert "Redirecting..." in response.data

        self.manager.admin_sign_in_controller.authenticated_problem_detail = False


class TestAdminSignIn(AdminRouteTest):

    CONTROLLER_NAME = "admin_sign_in_controller"

    def test_google_auth_callback(self):
        url = '/admin/GoogleAuth/callback'
        self.assert_request_calls(url, self.controller.redirect_after_google_sign_in)

    def test_sign_in_with_password(self):
        url = '/admin/sign_in_with_password'
        self.assert_request_calls(url, self.controller.password_sign_in, http_method='POST')

        self.assert_supported_methods(url, 'POST')

    def test_sign_in(self):
        url = '/admin/sign_in'
        self.assert_request_calls(url, self.controller.sign_in)

    def test_sign_out(self):
        url = '/admin/sign_out'
        self.assert_authenticated_request_calls(url, self.controller.sign_out)

    def test_change_password(self):
        url = '/admin/change_password'
        self.assert_authenticated_request_calls(
            url, self.controller.change_password, http_method='POST'
        )
        self.assert_supported_methods(url, 'POST')

    def test_sign_in_again(self):
        url = '/admin/sign_in_again'
        self.assert_redirect_call(url)

    def test_redirect(self):
        url = '/admin'
        response = self.request(url)

        assert 302 == response.status_code
        assert "Redirecting..." in response.data

class TestAdminWork(AdminRouteTest):

    CONTROLLER_NAME = "admin_work_controller"

    def test_details(self):
        url = "/admin/works/<identifier_type>/an/identifier"
        self.assert_authenticated_request_calls(
            url, self.controller.details, '<identifier_type>', 'an/identifier'
        )
        self.assert_supported_methods(url, 'GET')

    def test_classifications(self):
        url = "/admin/works/<identifier_type>/an/identifier/classifications"
        self.assert_authenticated_request_calls(
            url, self.controller.classifications, '<identifier_type>', 'an/identifier'
        )
        self.assert_supported_methods(url, 'GET')

    def test_preview_book_cover(self):
        url = "/admin/works/<identifier_type>/an/identifier/preview_book_cover"
        self.assert_authenticated_request_calls(
            url, self.controller.preview_book_cover, '<identifier_type>', 'an/identifier',
            http_method='POST'
        )

    def test_change_book_cover(self):
        url = "/admin/works/<identifier_type>/an/identifier/change_book_cover"
        self.assert_authenticated_request_calls(
            url, self.controller.change_book_cover, '<identifier_type>',
            'an/identifier', http_method='POST'
        )

    def test_complaints(self):
        url = "/admin/works/<identifier_type>/an/identifier/complaints"
        self.assert_authenticated_request_calls(
            url, self.controller.complaints, '<identifier_type>', 'an/identifier'
        )
        self.assert_supported_methods(url, 'GET')

    def test_custom_lists(self):
        url = "/admin/works/<identifier_type>/an/identifier/lists"
        self.assert_authenticated_request_calls(
            url, self.controller.custom_lists, '<identifier_type>', 'an/identifier',
            http_method='POST'
        )
        self.assert_supported_methods(url, 'GET', 'POST')

    def test_edit(self):
        url = "/admin/works/<identifier_type>/an/identifier/edit"
        self.assert_authenticated_request_calls(
            url, self.controller.edit, '<identifier_type>', 'an/identifier',
            http_method='POST'
        )

    def test_suppress(self):
        url = "/admin/works/<identifier_type>/an/identifier/suppress"
        self.assert_authenticated_request_calls(
            url, self.controller.suppress, '<identifier_type>', 'an/identifier',
            http_method='POST'
        )

    def test_unsuppress(self):
        url = "/admin/works/<identifier_type>/an/identifier/unsuppress"
        self.assert_authenticated_request_calls(
            url, self.controller.unsuppress, '<identifier_type>', 'an/identifier',
            http_method='POST'
        )

    def test_refresh_metadata(self):
        url = "/admin/works/<identifier_type>/an/identifier/refresh"
        self.assert_authenticated_request_calls(
            url, self.controller.refresh_metadata, '<identifier_type>', 'an/identifier',
            http_method='POST'
        )

    def test_resolve_complaints(self):
        url = "/admin/works/<identifier_type>/an/identifier/resolve_complaints"
        self.assert_authenticated_request_calls(
            url, self.controller.resolve_complaints, '<identifier_type>', 'an/identifier',
            http_method='POST'
        )

    def test_edit_classifications(self):
        url = "/admin/works/<identifier_type>/an/identifier/edit_classifications"
        self.assert_authenticated_request_calls(
            url, self.controller.edit_classifications, '<identifier_type>', 'an/identifier',
            http_method='POST'
        )

    def test_roles(self):
        url = "/admin/roles"
        self.assert_request_calls(url, self.controller.roles)

    def test_languages(self):
        url = "/admin/languages"
        self.assert_request_calls(url, self.controller.languages)

    def test_media(self):
        url = "/admin/media"
        self.assert_request_calls(url, self.controller.media)

    def test_right_status(self):
        url = "/admin/rights_status"
        self.assert_request_calls(url, self.controller.rights_status)

class TestAdminFeed(AdminRouteTest):

    CONTROLLER_NAME = "admin_feed_controller"

    def test_complaints(self):
        url = "/admin/complaints"
        self.assert_authenticated_request_calls(url, self.controller.complaints)

    def test_suppressed(self):
        url = "/admin/suppressed"
        self.assert_authenticated_request_calls(url, self.controller.suppressed)

    def test_genres(self):
        url = "/admin/genres"
        self.assert_authenticated_request_calls(url, self.controller.genres)

class TestAdminDashboard(AdminRouteTest):

    CONTROLLER_NAME = "admin_dashboard_controller"

    def test_bulk_circulation_events(self):
        url = "/admin/bulk_circulation_events"
        self.assert_authenticated_request_calls(
            url, self.controller.bulk_circulation_events,
            file_response=True
        )

    def test_circulation_events(self):
        url = "/admin/circulation_events"
        self.assert_authenticated_request_calls(url, self.controller.circulation_events)

    def test_stats(self):
        url = "/admin/stats"
        self.assert_authenticated_request_calls(url, self.controller.stats)

class TestAdminLibrarySettings(AdminRouteTest):

    CONTROLLER_NAME = "admin_library_settings_controller"

    def test_process_libraries(self):
        url = "/admin/libraries"
        self.assert_authenticated_request_calls(
            url, self.controller.process_libraries
        )
        self.assert_supported_methods(url, 'GET', 'POST')

    def test_delete(self):
        url = "/admin/library/<library_uuid>"
        self.assert_authenticated_request_calls(
            url, self.controller.process_delete, '<library_uuid>', http_method='DELETE'
        )
        self.assert_supported_methods(url, 'DELETE')

class TestAdminCollectionSettings(AdminRouteTest):

    CONTROLLER_NAME = "admin_collection_settings_controller"

    def test_process_get(self):
        url = "/admin/collections"
        self.assert_authenticated_request_calls(
            url, self.controller.process_collections
        )
        self.assert_supported_methods(url, 'GET', 'POST')

    def test_process_post(self):
        url = "/admin/collection/<collection_id>"
        self.assert_authenticated_request_calls(
            url, self.controller.process_delete, '<collection_id>', http_method='DELETE'
        )
        self.assert_supported_methods(url, 'DELETE')

class TestAdminCollectionSelfTests(AdminRouteTest):

    CONTROLLER_NAME = "admin_collection_self_tests_controller"

    def test_process_collection_self_tests(self):
        url = "/admin/collection_self_tests/<identifier>"
        self.assert_authenticated_request_calls(
            url, self.controller.process_collection_self_tests, '<identifier>'
        )

class TestAdminCollectionLibraryRegistrations(AdminRouteTest):

    CONTROLLER_NAME = "admin_collection_library_registrations_controller"

    def test_process_collection_library_registrations(self):
        url = "/admin/collection_library_registrations"
        self.assert_authenticated_request_calls(
            url, self.controller.process_collection_library_registrations
        )
        self.assert_supported_methods(url, 'GET', 'POST')

class TestAdminAuthServices(AdminRouteTest):

    CONTROLLER_NAME = "admin_auth_services_controller"

    def test_process_admin_auth_services(self):
        url = "/admin/admin_auth_services"
        self.assert_authenticated_request_calls(
            url, self.controller.process_admin_auth_services
        )
        self.assert_supported_methods(url, 'GET', 'POST')

    def test_process_delete(self):
        url = "/admin/admin_auth_service/<protocol>"
        self.assert_authenticated_request_calls(
            url, self.controller.process_delete, '<protocol>', http_method='DELETE'
        )
        self.assert_supported_methods(url, 'DELETE')

class TestAdminIndividualAdminSettings(AdminRouteTest):

    CONTROLLER_NAME = "admin_individual_admin_settings_controller"

    def test_process_individual_admins(self):
        url = "/admin/individual_admins"
        self.assert_authenticated_request_calls(
            url, self.controller.process_individual_admins
        )
        self.assert_supported_methods(url, 'GET', 'POST')

    def test_process_delete(self):
        url = "/admin/individual_admin/<email>"
        self.assert_authenticated_request_calls(
            url, self.controller.process_delete, '<email>', http_method='DELETE'
        )
        self.assert_supported_methods(url, 'DELETE')

class TestAdminPatronAuthServices(AdminRouteTest):

    CONTROLLER_NAME = "admin_patron_auth_services_controller"

    def test_process_patron_auth_services(self):
        url = "/admin/patron_auth_services"
        self.assert_authenticated_request_calls(
            url, self.controller.process_patron_auth_services
        )
        self.assert_supported_methods(url, 'GET', 'POST')

    def test_process_delete(self):
        url = "/admin/patron_auth_service/<service_id>"
        self.assert_authenticated_request_calls(
            url, self.controller.process_delete, '<service_id>', http_method='DELETE'
        )
        self.assert_supported_methods(url, 'DELETE')

class TestAdminPatronAuthServicesSelfTests(AdminRouteTest):

    CONTROLLER_NAME = "admin_patron_auth_service_self_tests_controller"

    def test_process_patron_auth_service_self_tests(self):
        url = "/admin/patron_auth_service_self_tests/<identifier>"
        self.assert_authenticated_request_calls(
            url, self.controller.process_patron_auth_service_self_tests, '<identifier>'
        )
        self.assert_supported_methods(url, 'GET', 'POST')

class TestAdminPatron(AdminRouteTest):

    CONTROLLER_NAME = "admin_patron_controller"

    def test_lookup_patron(self):
        url = "/admin/manage_patrons"
        self.assert_authenticated_request_calls(
            url, self.controller.lookup_patron, http_method='POST'
        )
        self.assert_supported_methods(url, 'POST')

    def test_reset_adobe_id(self):
        url = "/admin/manage_patrons/reset_adobe_id"
        self.assert_authenticated_request_calls(
            url, self.controller.reset_adobe_id, http_method='POST'
        )
        self.assert_supported_methods(url, 'POST')


class TestAdminMetadataServices(AdminRouteTest):

    CONTROLLER_NAME = "admin_metadata_services_controller"

    def test_process_metadata_services(self):
        url = "/admin/metadata_services"
        self.assert_authenticated_request_calls(
            url, self.controller.process_metadata_services
        )
        self.assert_supported_methods(url, 'GET', 'POST')

    def test_process_delete(self):
        url = "/admin/metadata_service/<service_id>"
        self.assert_authenticated_request_calls(
            url, self.controller.process_delete, '<service_id>', http_method='DELETE'
        )
        self.assert_supported_methods(url, 'DELETE')

class TestAdminAnalyticsServices(AdminRouteTest):

    CONTROLLER_NAME = "admin_analytics_services_controller"

    def test_process_analytics_services(self):
        url = "/admin/analytics_services"
        self.assert_authenticated_request_calls(
            url, self.controller.process_analytics_services
        )
        self.assert_supported_methods(url, 'GET', 'POST')

    def test_process_delete(self):
        url = "/admin/analytics_service/<service_id>"
        self.assert_authenticated_request_calls(
            url, self.controller.process_delete, '<service_id>', http_method='DELETE'
        )
        self.assert_supported_methods(url, 'DELETE')

class TestAdminCDNServices(AdminRouteTest):

    CONTROLLER_NAME = "admin_cdn_services_controller"

    def test_process_cdn_services(self):
        url = "/admin/cdn_services"
        self.assert_authenticated_request_calls(
            url, self.controller.process_cdn_services
        )
        self.assert_supported_methods(url, 'GET', 'POST')

    def test_process_delete(self):
        url = "/admin/cdn_service/<service_id>"
        self.assert_authenticated_request_calls(
            url, self.controller.process_delete, '<service_id>', http_method='DELETE'
        )
        self.assert_supported_methods(url, 'DELETE')

class TestAdminSearchServices(AdminRouteTest):

    CONTROLLER_NAME = "admin_search_services_controller"

    def test_process_services(self):
        url = "/admin/search_services"
        self.assert_authenticated_request_calls(
            url, self.controller.process_services
        )
        self.assert_supported_methods(url, 'GET', 'POST')

    def test_process_delete(self):
        url = "/admin/search_service/<service_id>"
        self.assert_authenticated_request_calls(
            url, self.controller.process_delete, '<service_id>', http_method='DELETE'
        )
        self.assert_supported_methods(url, 'DELETE')

class TestAdminSearchServicesSelfTests(AdminRouteTest):

    CONTROLLER_NAME = "admin_search_service_self_tests_controller"

    def test_process_search_service_self_tests(self):
        url = "/admin/search_service_self_tests/<identifier>"
        self.assert_authenticated_request_calls(
            url, self.controller.process_search_service_self_tests, '<identifier>'
        )
        self.assert_supported_methods(url, 'GET', 'POST')

class TestAdminStorageServices(AdminRouteTest):

    CONTROLLER_NAME = "admin_storage_services_controller"

    def test_process_services(self):
        url = "/admin/storage_services"
        self.assert_authenticated_request_calls(
            url, self.controller.process_services
        )
        self.assert_supported_methods(url, 'GET', 'POST')

    def test_process_delete(self):
        url = "/admin/storage_service/<service_id>"
        self.assert_authenticated_request_calls(
            url, self.controller.process_delete, '<service_id>', http_method='DELETE'
        )
        self.assert_supported_methods(url, 'DELETE')

class TestAdminCatalogServices(AdminRouteTest):

    CONTROLLER_NAME = "admin_catalog_services_controller"

    def test_process_catalog_services(self):
        url = "/admin/catalog_services"
        self.assert_authenticated_request_calls(
            url, self.controller.process_catalog_services
        )
        self.assert_supported_methods(url, 'GET', 'POST')

    def test_process_delete(self):
        url = "/admin/catalog_service/<service_id>"
        self.assert_authenticated_request_calls(
            url, self.controller.process_delete, '<service_id>', http_method='DELETE'
        )
        self.assert_supported_methods(url, 'DELETE')

class TestAdminDiscoveryServices(AdminRouteTest):

    CONTROLLER_NAME = "admin_discovery_services_controller"

    def test_process_discovery_services(self):
        url = "/admin/discovery_services"
        self.assert_authenticated_request_calls(
            url, self.controller.process_discovery_services
        )
        self.assert_supported_methods(url, 'GET', 'POST')

    def test_process_delete(self):
        url = "/admin/discovery_service/<service_id>"
        self.assert_authenticated_request_calls(
            url, self.controller.process_delete, '<service_id>', http_method='DELETE'
        )
        self.assert_supported_methods(url, 'DELETE')

class TestAdminSitewideServices(AdminRouteTest):

    CONTROLLER_NAME = "admin_sitewide_configuration_settings_controller"

    def test_process_services(self):
        url = "/admin/sitewide_settings"
        self.assert_authenticated_request_calls(
            url, self.controller.process_services
        )
        self.assert_supported_methods(url, 'GET', 'POST')

    def test_process_delete(self):
        url = "/admin/sitewide_setting/<key>"
        self.assert_authenticated_request_calls(
            url, self.controller.process_delete, '<key>', http_method='DELETE'
        )
        self.assert_supported_methods(url, 'DELETE')

class TestAdminLoggingServices(AdminRouteTest):

    CONTROLLER_NAME = "admin_logging_services_controller"

    def test_process_services(self):
        url = "/admin/logging_services"
        self.assert_authenticated_request_calls(
            url, self.controller.process_services
        )
        self.assert_supported_methods(url, 'GET', 'POST')

    def test_process_delete(self):
        url = "/admin/logging_service/<key>"
        self.assert_authenticated_request_calls(
            url, self.controller.process_delete, '<key>', http_method='DELETE'
        )
        self.assert_supported_methods(url, 'DELETE')

class TestAdminDiscoveryServiceLibraryRegistrations(AdminRouteTest):

    CONTROLLER_NAME = "admin_discovery_service_library_registrations_controller"

    def test_process_discovery_service_library_registrations(self):
        url = "/admin/discovery_service_library_registrations"
        self.assert_authenticated_request_calls(
            url, self.controller.process_discovery_service_library_registrations
        )
        self.assert_supported_methods(url, 'GET', 'POST')

class TestAdminCustomListsServices(AdminRouteTest):

    CONTROLLER_NAME = "admin_custom_lists_controller"

    def test_custom_lists(self):
        url = "/admin/custom_lists"
        self.assert_authenticated_request_calls(
            url, self.controller.custom_lists
        )
        self.assert_supported_methods(url, 'GET', 'POST')

    def test_custom_list(self):
        url = "/admin/custom_list/<list_id>"
        self.assert_authenticated_request_calls(
            url, self.controller.custom_list, '<list_id>'
        )
        self.assert_supported_methods(url, 'GET', 'POST', 'DELETE')


class TestAdminLanes(AdminRouteTest):

    CONTROLLER_NAME = "admin_lanes_controller"

    def test_lanes(self):
        url = "/admin/lanes"
        self.assert_authenticated_request_calls(url, self.controller.lanes)
        self.assert_supported_methods(url, 'GET', 'POST')

    def test_lane(self):
        url = "/admin/lane/<lane_identifier>"
        self.assert_authenticated_request_calls(
            url, self.controller.lane, '<lane_identifier>', http_method='DELETE'
        )
        self.assert_supported_methods(url, 'DELETE')

    def test_show_lane(self):
        url = "/admin/lane/<lane_identifier>/show"
        self.assert_authenticated_request_calls(
            url, self.controller.show_lane, '<lane_identifier>', http_method='POST'
        )
        self.assert_supported_methods(url, 'POST')

    def test_hide_lane(self):
        url = "/admin/lane/<lane_identifier>/hide"
        self.assert_authenticated_request_calls(
            url, self.controller.hide_lane, '<lane_identifier>', http_method='POST'
        )
        self.assert_supported_methods(url, 'POST')

    def test_reset(self):
        url = "/admin/lanes/reset"
        self.assert_authenticated_request_calls(
            url, self.controller.reset, http_method='POST'
        )
        self.assert_supported_methods(url, 'POST')

    def test_change_order(self):
        url = "/admin/lanes/change_order"
        self.assert_authenticated_request_calls(
            url, self.controller.change_order, http_method='POST'
        )
        self.assert_supported_methods(url, 'POST')

class TestTimestamps(AdminRouteTest):

    CONTROLLER_NAME = "timestamps_controller"

    def test_diagnostics(self):
        url = "/admin/diagnostics"
        self.assert_authenticated_request_calls(url, self.controller.diagnostics)

class TestAdminView(AdminRouteTest):

    CONTROLLER_NAME = "admin_view_controller"

    def test_admin_view(self):
        url = "/admin/web/"
        self.assert_request_calls(
            url, self.controller, None, None, path=None
        )

        url = "/admin/web/collection/a/collection/book/a/book"
        self.assert_request_calls(
            url, self.controller, "a/collection", "a/book", path=None
        )

        url = "/admin/web/collection/a/collection"
        self.assert_request_calls(
            url, self.controller, "a/collection", None, path=None
        )

        url = "/admin/web/book/a/book"
        self.assert_request_calls(
            url, self.controller, None, "a/book", path=None
        )

        url = "/admin/web/a/path"
        self.assert_request_calls(
            url, self.controller, None, None, path="a/path"
        )

class TestAdminStatic(AdminRouteTest):

    CONTROLLER_NAME = "static_files"

    def test_static_file(self):
        url = "/admin/static/circulation-web.js"

        # Go to the back to the root folder to get the right
        # path for the static files.
        local_path = os.path.abspath(
            os.path.join(
                os.path.abspath(os.path.dirname(__file__)),
                "../..",
                "api/admin/node_modules/simplified-circulation-web/dist"
            )
        )

        self.assert_request_calls(
            url, self.controller.static_file, local_path, "circulation-web.js"
        )

        url = "/admin/static/circulation-web.css"
        self.assert_request_calls(
            url, self.controller.static_file, local_path, "circulation-web.css"
        )
