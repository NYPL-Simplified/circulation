import contextlib
from nose.tools import (
    eq_,
    set_trace,
)
from flask import Response
from api import app
from api import routes

from test_controller import ControllerTest

class MockApp(object):
    """Pretends to be a Flask application with a configured
    CirculationManager.
    """
    def __init__(self):
        self.manager = MockManager()


class MockManager(object):
    """Pretends to be a CirculationManager with configured controllers."""

    def __init__(self):
        self._cache = {}

        # This is used by the allows_patron_web annotator.
        self.patron_web_client_url = "http://patron/web"

    def __getattr__(self, controller_name):
        return self._cache.setdefault(
            controller_name, MockController(controller_name)
        )

class MockControllerMethod(object):
    """Pretends to be one of the methods of a controller class."""
    def __init__(self, controller, name):
        """Constructor.

        :param controller: A MockController.
        :param name: The name of this method.
        """
        self.controller = controller
        self.name = name

    def __call__(self, *args, **kwargs):
        """Simulate a successful method call.

        :return: A Response object, as required by Flask, with this
        method smuggled out as the 'method' attribute.
        """
        self.args = args
        self.kwargs = kwargs
        response = Response("", 200)
        response.method = self
        return response

class MockController(MockControllerMethod):
    """Pretends to be a controller.

    A controller has methods, but it may also be called _as_ a method,
    so this class subclasses MockControllerMethod.
    """
    def __init__(self, name):
        """Constructor.

        :param name: The name of the controller.
        """
        self.name = name
        self._cache = {}

    def __getattr__(self, method_name):
        """Locate a method of this controller as a MockControllerMethod."""
        return self._cache.setdefault(
            method_name, MockControllerMethod(self, method_name)
        )


class TestRoutes(ControllerTest):

    def setup(self, _db=None):
        super(TestRoutes, self).setup(_db=_db, set_up_circulation_manager=False)
        self.original_app = routes.app
        app = MockApp()
        routes.app = app
        self.manager = app.manager
        self.resolver = self.original_app.url_map.bind('', '/')

    def teardown(self):
        routes.app = self.original_app

    def request(self, url, method='GET'):
        """Simulate a request to a URL without triggering any code outside
        routes.py.
        """
        # Map an incoming URL to the name of a function within routes.py
        # and a set of arguments to the function.
        function_name, kwargs = self.resolver.match(url, method)

        # Locate the function itself.
        function = getattr(routes, function_name)

        # Call it in the context of our MockApp which simulates the
        # controller code.
        with self.app.test_request_context():
            return function(**kwargs)

    def assert_request_calls(self, url, method, *args, **kwargs):
        """Make a request to the given `url` and assert that
        the given controller `method` was called with the
        given `args` and `kwargs`.
        """
        http_method = kwargs.pop('method', 'GET')
        response = self.request(url, http_method)
        eq_(response.method, method)
        eq_(response.method.args, args)
        eq_(response.method.kwargs, kwargs)

    def test_index(self):
        for url in '/', '':
            self.assert_request_calls(url, self.manager.index_controller)

    def test_authentication_document(self):
        self.assert_request_calls(
            "/authentication_document",
            self.manager.index_controller.authentication_document
        )

    def test_public_key_document(self):
        self.assert_request_calls(
            "/public_key_document",
            self.manager.index_controller.public_key_document
        )

    def test_acquisition_groups(self):
        # An incoming lane identifier is passed in to the groups()
        # method.
        method = self.manager.opds_feeds.groups
        self.assert_request_calls("/groups", method, None)
        self.assert_request_calls("/groups/a-lane", method, 'a-lane')

    def test_feed(self):
        # An incoming lane identifier is passed in to the feed()
        # method.
        method = self.manager.opds_feeds.feed
        self.assert_request_calls("/feed", method, None)
        self.assert_request_calls("/feed/a-lane", method, 'a-lane')

