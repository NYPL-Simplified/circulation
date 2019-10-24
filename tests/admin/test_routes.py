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
from api.admin.password_admin_authentication_provider import PasswordAdminAuthenticationProvider

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
            setup_admin_controllers(manager.manager)
            RouteTest.REAL_CIRCULATION_MANAGER = manager.manager

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

class TestIndex(AdminRouteTest):

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
