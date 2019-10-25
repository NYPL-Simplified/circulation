import contextlib
import logging
from nose.tools import (
    assert_raises,
    eq_,
    set_trace,
)
import flask
from flask import Response
from werkzeug.exceptions import MethodNotAllowed

from core.app_server import ErrorHandler
from core.model import (
    Admin,
    ConfigurationSetting,
    get_one_or_create
)

from api import app
from api.config import Configuration
from api.controller import CirculationManager
from api.admin.controller import AdminController
from api.admin.controller import setup_admin_controllers

from api.routes import (
    exception_handler,
    h as error_handler_object,
)
from api.admin import routes
from ..test_controller import ControllerTest
from ..test_routes import (
    MockManager,
    MockController,
    RouteTest
)

class MockApp(object):
    """Pretends to be a Flask application with a configured
    CirculationManager and Admin routes.
    """
    def __init__(self):
        self.manager = AdminMockManager()

class AdminMockManager(MockManager):
    def __getattr__(self, controller_name):
        return self._cache.setdefault(
            controller_name, AdminMockController(controller_name)
        )

class AdminMockController(MockController):
    AUTHENTICATED_ADMIN = "i am a mock admin"

    def authenticated_admin_from_request(self):
        if self.authenticated:
            admin = object()
            flask.request.admin = self.AUTHENTICATED_ADMIN
            return self.AUTHENTICATED_ADMIN
        else:
            return Response(
                "authenticated_admin_from_request called without authorizing",
                401
            )
    
    def bulk_circulation_events(self):
        return "date", "date", "date_end", "library"

class AdminRouteTest(RouteTest):
    def setup(self, _db=None):
        super(RouteTest, self).setup(_db=_db, set_up_circulation_manager=False)
        if not RouteTest.REAL_CIRCULATION_MANAGER:
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
            RouteTest.REAL_CIRCULATION_MANAGER = circ_manager

        app = MockApp()
        self.routes = routes
        self.manager = app.manager
        self.original_app = self.routes.app
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

    def assert_authenticated_request_calls(self, url, method, *args, **kwargs):
        """First verify that an unauthenticated request fails. Then make an
        authenticated request to `url` and verify the results, as with
        assert_request_calls
        """
        authentication_required = kwargs.pop("authentication_required", True)

        http_method = kwargs.pop('http_method', 'GET')
        response = self.request(url, http_method)
        if authentication_required:
            eq_(401, response.status_code)
            eq_("authenticated_admin_from_request called without authorizing",
                response.data)
        else:
            eq_(200, response.status_code)

        # Set a variable so that authenticated_admin_from_request
        # will succeed, and try again.
        self.manager.admin_sign_in_controller.authenticated = True
        try:
            kwargs['http_method'] = http_method
            self.assert_request_calls(url, method, *args, **kwargs)
        finally:
            # Un-set authentication for the benefit of future
            # assertions in this test function.
            self.manager.admin_sign_in_controller.authenticated = False

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
        self.assert_authenticated_request_calls(url, self.controller.change_password, http_method='POST')
        self.assert_supported_methods(url, 'POST')

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
        # self.assert_supported_methods(url, 'POST')

    def test_change_book_cover(self):
        url = "/admin/works/<identifier_type>/an/identifier/change_book_cover"
        self.assert_authenticated_request_calls(
            url, self.controller.change_book_cover, '<identifier_type>',
            'an/identifier', http_method='POST'
        )
        # self.assert_supported_methods(url, 'POST')

    def test_complaints(self):
        url = "/admin/works/<identifier_type>/an/identifier/complaints"
        self.assert_authenticated_request_calls(
            url, self.controller.complaints, '<identifier_type>', 'an/identifier'
        )
        # self.assert_supported_methods(url, 'GET')

    def test_custom_lists(self):
        url = "/admin/works/<identifier_type>/an/identifier/lists"
        self.assert_authenticated_request_calls(
            url, self.controller.custom_lists, '<identifier_type>', 'an/identifier',
            http_method='POST'
        )
        # self.assert_supported_methods(url, 'GET', 'POST')

    def test_edit(self):
        url = "/admin/works/<identifier_type>/an/identifier/edit"
        self.assert_authenticated_request_calls(
            url, self.controller.edit, '<identifier_type>', 'an/identifier',
            http_method='POST'
        )
        # self.assert_supported_methods(url, 'POST')

    def test_suppress(self):
        url = "/admin/works/<identifier_type>/an/identifier/suppress"
        self.assert_authenticated_request_calls(
            url, self.controller.suppress, '<identifier_type>', 'an/identifier',
            http_method='POST'
        )
        # self.assert_supported_methods(url, 'POST')

    def test_unsuppress(self):
        url = "/admin/works/<identifier_type>/an/identifier/unsuppress"
        self.assert_authenticated_request_calls(
            url, self.controller.unsuppress, '<identifier_type>', 'an/identifier',
            http_method='POST'
        )
        # self.assert_supported_methods(url, 'POST')

    def test_refresh_metadata(self):
        url = "/admin/works/<identifier_type>/an/identifier/refresh"
        self.assert_authenticated_request_calls(
            url, self.controller.refresh_metadata, '<identifier_type>', 'an/identifier',
            http_method='POST'
        )
        # self.assert_supported_methods(url, 'POST')

    def test_resolve_complaints(self):
        url = "/admin/works/<identifier_type>/an/identifier/resolve_complaints"
        self.assert_authenticated_request_calls(
            url, self.controller.resolve_complaints, '<identifier_type>', 'an/identifier',
            http_method='POST'
        )
        # self.assert_supported_methods(url, 'POST')

    def test_edit_classifications(self):
        url = "/admin/works/<identifier_type>/an/identifier/edit_classifications"
        self.assert_authenticated_request_calls(
            url, self.controller.edit_classifications, '<identifier_type>', 'an/identifier',
            http_method='POST'
        )
        # self.assert_supported_methods(url, 'POST')

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

    # def test_bulk_circulation_events(self):
    #     url = "/admin/bulk_circulation_events"
    #     self.assert_authenticated_request_calls(
    #         url, self.controller.bulk_circulation_events
    #     )

    def test_circulation_events(self):
        url = "/admin/circulation_events"
        self.assert_authenticated_request_calls(url, self.controller.circulation_events)

    def test_stats(self):
        url = "/admin/stats"
        self.assert_authenticated_request_calls(url, self.controller.stats)

class TestAdminLibrarySettings(AdminRouteTest):

    CONTROLLER_NAME = "admin_library_settings_controller"

    def test_process_get(self):
        url = "admin/libraries"
        self.assert_authenticated_request_calls(
            url, self.controller.process_get, http_method='GET'
        )

    # def test_process_post(self):
    #     url = "admin/libraries"
    #     set_trace()
    #     self.assert_authenticated_request_calls(
    #         url, self.controller.process_post, http_method='POST'
    #     )

    def test_delete(self):
        url = "/admin/library/<library_uuid>"
        self.assert_authenticated_request_calls(
            url, self.controller.process_delete, '<library_uuid>', http_method='DELETE'
        )

class TestAdminCollectionSettings(AdminRouteTest):

    CONTROLLER_NAME = "admin_collection_settings_controller"

    def test_process_get(self):
        url = "admin/collections"
        self.assert_authenticated_request_calls(
            url, self.controller.process_collections, http_method='GET'
        )

    def test_process_post(self):
        url = "admin/collection/<collection_id>"
        self.assert_authenticated_request_calls(
            url, self.controller.process_delete, '<collection_id>', http_method='DELETE'
        )

class TestAdminCollectionSelfTests(AdminRouteTest):

    CONTROLLER_NAME = "admin_collection_self_tests_controller"

    def test_process_collection_self_tests(self):
        url = "admin/collection_self_tests/<identifier>"
        self.assert_authenticated_request_calls(
            url, self.controller.process_collection_self_tests, '<identifier>'
        )
