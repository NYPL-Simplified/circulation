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
    patron_web_client_url = "http://patron/web"

    def __init__(self):
        self._cache = {}

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

    def teardown(self):
        routes.app = self.original_app

    def test_index(self):
        with self.app.test_request_context():
            response = routes.index()
            eq_(self.manager.index_controller, response.method)

    def test_authentication_document(self):
        with self.app.test_request_context():
            response = routes.authentication_document()
            eq_(
                self.manager.index_controller.authentication_document,
                response.method
            )
    
    def test_public_key_document(self):
        with self.app.test_request_context():
            response = routes.public_key_document()
            eq_(
                self.manager.index_controller.public_key_document,
                response.method
            )

    def test_acquisition_groups(self):
        # The incoming lane identifier is passed in to the groups()
        # method.
        with self.app.test_request_context():
            response = routes.acquisition_groups(lane_identifier="a lane")
            called = response.method
            eq_(self.manager.opds_feeds.groups, method)
            eq_(("a lane",), method.args)
        
    def test_feed(self):
        # The incoming lane identifier is passed in to the groups()
        # method.
        with self.app.test_request_context():
            response = routes.feed(lane_identifier="a lane")
            called = response.method
            eq_(self.manager.opds_feeds.feed, called)
            eq_(("a lane",), called.args)
        
