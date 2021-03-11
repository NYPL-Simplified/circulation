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

from api import app
from api import routes
from api.opds import CirculationManagerAnnotator
from api.controller import CirculationManager
from api.routes import (
    exception_handler,
    h as error_handler_object,
)

from .test_controller import ControllerTest

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
        self.patron_web_domains = set(["http://patron/web"])

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
        self.callable_name = name

    def __call__(self, *args, **kwargs):
        """Simulate a successful method call.

        :return: A Response object, as required by Flask, with this
        method smuggled out as the 'method' attribute.
        """
        self.args = args
        self.kwargs = kwargs
        response = Response("I called %s" % repr(self), 200)
        response.method = self
        return response

    def __repr__(self):
        return "<MockControllerMethod %s.%s>" % (
            self.controller.name, self.name
        )

class MockController(MockControllerMethod):
    """Pretends to be a controller.

    A controller has methods, but it may also be called _as_ a method,
    so this class subclasses MockControllerMethod.
    """
    AUTHENTICATED_PATRON = "i am a mock patron"

    def __init__(self, name):
        """Constructor.

        :param name: The name of the controller.
        """
        self.name = name

        # If this controller were to be called as a method, the method
        # name would be __call__, not the name of the controller.
        self.callable_name = '__call__'

        self._cache = {}
        self.authenticated = False
        self.csrf_token = False
        self.authenticated_problem_detail = False

    def authenticated_patron_from_request(self):
        if self.authenticated:
            patron = object()
            flask.request.patron = self.AUTHENTICATED_PATRON
            return self.AUTHENTICATED_PATRON
        else:
            return Response(
                "authenticated_patron_from_request called without authorizing",
                401
            )

    def __getattr__(self, method_name):
        """Locate a method of this controller as a MockControllerMethod."""
        return self._cache.setdefault(
            method_name, MockControllerMethod(self, method_name)
        )

    def __repr__(self):
        return "<MockControllerMethod %s>" % self.name


class RouteTest(ControllerTest):
    """Test what happens when an HTTP request is run through the
    routes we've registered with Flask.
    """

    # The first time setup() is called, it will instantiate a real
    # CirculationManager object and store it here. We only do this
    # once because it takes about a second to instantiate this object.
    # Calling any of this object's methods could be problematic, since
    # it's probably left over from a previous test, but we won't be
    # calling any methods -- we just want to verify the _existence_,
    # in a real CirculationManager, of the methods called in
    # routes.py.
    REAL_CIRCULATION_MANAGER = None

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
            manager = CirculationManager(self._db, testing=True)
            RouteTest.REAL_CIRCULATION_MANAGER = manager
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

    def teardown(self):
        super(RouteTest, self).teardown()
        self.routes.app = self.original_app

    def request(self, url, method='GET'):
        """Simulate a request to a URL without triggering any code outside
        routes.py.
        """
        # Map an incoming URL to the name of a function within routes.py
        # and a set of arguments to the function.
        function_name, kwargs = self.resolver.match(url, method)
        # Locate the corresponding function in our mock app.
        mock_function = getattr(self.routes, function_name)

        # Call it in the context of the mock app.
        with self.app.test_request_context():
            return mock_function(**kwargs)

    def assert_request_calls(self, url, method, *args, **kwargs):
        """Make a request to the given `url` and assert that
        the given controller `method` was called with the
        given `args` and `kwargs`.
        """
        http_method = kwargs.pop('http_method', 'GET')
        response = self.request(url, http_method)
        eq_(response.method, method)
        eq_(response.method.args, args)
        eq_(response.method.kwargs, kwargs)

        # Make sure the real controller has a method by the name of
        # the mock method that was called. We won't call it, because
        # it would slow down these tests dramatically, but we can make
        # sure it exists.
        if self.real_controller:
            real_method = getattr(self.real_controller, method.callable_name)

            # TODO: We could use inspect.getarcspec to verify that the
            # argument names line up with the variables passed in to
            # the mock method. This might remove the need to call the
            # mock method at all.

    def assert_request_calls_method_using_identifier(self, url, method, *args, **kwargs):
        # Call an assertion method several times, using different
        # types of identifier in the URL, to make sure the identifier
        # is always passed through correctly.
        #
        # The url must contain the string '<identifier>' standing in
        # for the place where an identifier should be plugged in, and
        # the *args list must include the string '<identifier>'.
        authenticated = kwargs.pop('authenticated', False)
        if authenticated:
            assertion_method = self.assert_authenticated_request_calls
        else:
            assertion_method = self.assert_request_calls
        assert '<identifier>' in url
        args = list(args)
        identifier_index = args.index('<identifier>')
        for identifier in (
            '<identifier>', 'an/identifier/', 'http://an-identifier/', 'http://an-identifier',
        ):
            modified_url = url.replace('<identifier>', identifier)
            args[identifier_index] = identifier
            assertion_method(
                modified_url, method, *args, **kwargs
            )

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
            eq_("authenticated_patron_from_request called without authorizing",
                response.get_data(as_text=True))
        else:
            eq_(200, response.status_code)

        # Set a variable so that authenticated_patron_from_request
        # will succeed, and try again.
        self.manager.index_controller.authenticated = True
        try:
            kwargs['http_method'] = http_method
            self.assert_request_calls(url, method, *args, **kwargs)
        finally:
            # Un-set authentication for the benefit of future
            # assertions in this test function.
            self.manager.index_controller.authenticated = False

    def assert_supported_methods(self, url, *methods):
        """Verify that the given HTTP `methods` are the only ones supported
        on the given `url`.
        """
        # The simplest way to do this seems to be to try each of the
        # other potential methods and verify that MethodNotAllowed is
        # raised each time.
        check = set(['GET', 'POST', 'PUT', 'DELETE']) - set(methods)
        # Treat HEAD specially. Any controller that supports GET
        # automatically supports HEAD. So we only assert that HEAD
        # fails if the method supports neither GET nor HEAD.
        if 'GET' not in methods and 'HEAD' not in methods:
            check.add('HEAD')
        for method in check:
            logging.debug("MethodNotAllowed should be raised on %s", method)
            assert_raises(MethodNotAllowed, self.request, url, method)
            logging.debug("And it was.")


class TestAppConfiguration(object):

    # Test the configuration of the real Flask app.
    def test_configuration(self):
        eq_(False, routes.app.url_map.merge_slashes)


class TestIndex(RouteTest):

    CONTROLLER_NAME = "index_controller"

    def test_index(self):
        for url in '/', '':
            self.assert_request_calls(url, self.controller)

    def test_authentication_document(self):
        url = '/authentication_document'
        self.assert_request_calls(url, self.controller.authentication_document)

    def test_public_key_document(self):
        url = '/public_key_document'
        self.assert_request_calls(url, self.controller.public_key_document)


class TestOPDSFeed(RouteTest):

    CONTROLLER_NAME = 'opds_feeds'

    def test_acquisition_groups(self):
        # An incoming lane identifier is passed in to the groups()
        # method.
        method = self.controller.groups
        self.assert_request_calls("/groups", method, None)
        self.assert_request_calls(
            "/groups/<lane_identifier>", method, '<lane_identifier>'
        )

    def test_feed(self):
        # An incoming lane identifier is passed in to the feed()
        # method.
        url = '/feed'
        self.assert_request_calls(url, self.controller.feed, None)
        url = '/feed/<lane_identifier>'
        self.assert_request_calls(
            url, self.controller.feed, '<lane_identifier>'
        )

    def test_navigation_feed(self):
        # An incoming lane identifier is passed in to the navigation_feed()
        # method.
        url = '/navigation'
        self.assert_request_calls(url, self.controller.navigation, None)
        url = '/navigation/<lane_identifier>'
        self.assert_request_calls(
            url, self.controller.navigation, '<lane_identifier>'
        )

    def test_crawlable_library_feed(self):
        url = '/crawlable'
        self.assert_request_calls(url, self.controller.crawlable_library_feed)

    def test_crawlable_list_feed(self):
        url = '/lists/<list_name>/crawlable'
        self.assert_request_calls(
            url, self.controller.crawlable_list_feed, '<list_name>'
        )

    def test_crawlable_collection_feed(self):
        url = '/collections/<collection_name>/crawlable'
        self.assert_request_calls(
            url, self.manager.opds_feeds.crawlable_collection_feed,
            '<collection_name>'
        )

    def test_lane_search(self):
        url = '/search'
        self.assert_request_calls(url, self.controller.search, None)

        url = '/search/<lane_identifier>'
        self.assert_request_calls(
            url, self.controller.search, "<lane_identifier>"
        )

    def test_qa_feed(self):
        url = '/feed/qa'
        self.assert_authenticated_request_calls(url, self.controller.qa_feed)

    def test_qa_series_feed(self):
        url = '/feed/qa/series'
        self.assert_authenticated_request_calls(url, self.controller.qa_series_feed)


class TestMARCRecord(RouteTest):
    CONTROLLER_NAME = 'marc_records'

    def test_marc_page(self):
        url = "/marc"
        self.assert_request_calls(url, self.controller.download_page)

class TestSharedCollection(RouteTest):

    CONTROLLER_NAME = 'shared_collection_controller'

    def test_shared_collection_info(self):
        url = '/collections/<collection_name>'
        self.assert_request_calls(
            url, self.controller.info, '<collection_name>'
        )

    def test_shared_collection_register(self):
        url = '/collections/<collection_name>/register'
        self.assert_request_calls(
            url, self.controller.register, '<collection_name>',
            http_method='POST'
        )
        self.assert_supported_methods(url, 'POST')

    def test_shared_collection_borrow_identifier(self):
        url = '/collections/<collection_name>/<identifier_type>/<identifier>/borrow'
        self.assert_request_calls_method_using_identifier(
            url, self.controller.borrow, '<collection_name>',
            '<identifier_type>', "<identifier>", None
        )
        self.assert_supported_methods(url, 'GET', 'POST')

    def test_shared_collection_borrow_hold_id(self):
        url = '/collections/<collection_name>/holds/<hold_id>/borrow'
        self.assert_request_calls(
            url, self.controller.borrow, '<collection_name>', None, None,
            '<hold_id>'
        )
        self.assert_supported_methods(url, 'GET', 'POST')

    def test_shared_collection_loan_info(self):
        url = '/collections/<collection_name>/loans/<loan_id>'
        self.assert_request_calls(
            url, self.controller.loan_info, '<collection_name>', '<loan_id>'
        )

    def test_shared_collection_revoke_loan(self):
        url = '/collections/<collection_name>/loans/<loan_id>/revoke'
        self.assert_request_calls(
            url, self.controller.revoke_loan, '<collection_name>', '<loan_id>'
        )

    def test_shared_collection_fulfill_no_mechanism(self):
        url = '/collections/<collection_name>/loans/<loan_id>/fulfill'
        self.assert_request_calls(
            url, self.controller.fulfill, '<collection_name>', '<loan_id>',
            None
        )

    def test_shared_collection_fulfill_with_mechanism(self):
        url = '/collections/<collection_name>/loans/<loan_id>/fulfill/<mechanism_id>'
        self.assert_request_calls(
            url, self.controller.fulfill, '<collection_name>', '<loan_id>',
            '<mechanism_id>'
        )

    def test_shared_collection_hold_info(self):
        url = '/collections/<collection_name>/holds/<hold_id>'
        self.assert_request_calls(
            url, self.controller.hold_info, '<collection_name>',
            '<hold_id>'
        )

    def test_shared_collection_revoke_hold(self):
        url = '/collections/<collection_name>/holds/<hold_id>/revoke'
        self.assert_request_calls(
            url, self.controller.revoke_hold, '<collection_name>',
            '<hold_id>'
        )


class TestProfileController(RouteTest):

    CONTROLLER_NAME = "profiles"

    def test_patron_profile(self):
        url = '/patrons/me'
        self.assert_authenticated_request_calls(
            url, self.controller.protocol,
        )


class TestLoansController(RouteTest):

    CONTROLLER_NAME = "loans"

    def test_active_loans(self):
        url = '/loans'
        self.assert_authenticated_request_calls(
            url, self.controller.sync,
        )
        self.assert_supported_methods(url, 'GET', 'HEAD')

    def test_borrow(self):
        url = '/works/<identifier_type>/<identifier>/borrow'
        self.assert_request_calls_method_using_identifier(
            url, self.controller.borrow,
            "<identifier_type>", "<identifier>", None,
            authenticated=True
        )
        self.assert_supported_methods(url, 'GET', 'PUT')
        
        url = '/works/<identifier_type>/<identifier>/borrow/<mechanism_id>'
        self.assert_request_calls_method_using_identifier(
            url, self.controller.borrow,
            "<identifier_type>", "<identifier>", "<mechanism_id>",
            authenticated=True
        )
        self.assert_supported_methods(url, 'GET', 'PUT')

    def test_fulfill(self):
        # fulfill does *not* require authentication, because this
        # controller is how a no-authentication library fulfills
        # open-access titles.
        url = '/works/<license_pool_id>/fulfill'
        self.assert_request_calls(
            url, self.controller.fulfill, "<license_pool_id>", None, None
        )

        url = '/works/<license_pool_id>/fulfill/<mechanism_id>'
        self.assert_request_calls(
            url, self.controller.fulfill, "<license_pool_id>",
            "<mechanism_id>", None
        )

        url = '/works/<license_pool_id>/fulfill/<mechanism_id>/<part>'
        self.assert_request_calls(
            url, self.controller.fulfill, "<license_pool_id>",
            "<mechanism_id>", "<part>"
        )

    def test_revoke_loan_or_hold(self):
        url = '/loans/<license_pool_id>/revoke'
        self.assert_authenticated_request_calls(
            url, self.controller.revoke, '<license_pool_id>'
        )

        # TODO: DELETE shouldn't be in here, but "DELETE
        # /loans/<license_pool_id>/revoke" is interpreted as an attempt
        # to match /loans/<identifier_type>/<path:identifier>, the
        # method tested directly below, which does support DELETE.
        self.assert_supported_methods(url, 'GET', 'PUT', 'DELETE')

    def test_loan_or_hold_detail(self):
        url = '/loans/<identifier_type>/<identifier>'
        self.assert_request_calls_method_using_identifier(
            url, self.controller.detail,
            "<identifier_type>", "<identifier>", authenticated=True
        )
        self.assert_supported_methods(url, 'GET', 'DELETE')


class TestAnnotationsController(RouteTest):

    CONTROLLER_NAME = "annotations"

    def test_annotations(self):
        url = '/annotations/'
        self.assert_authenticated_request_calls(
            url, self.controller.container
        )
        self.assert_supported_methods(url, 'HEAD', 'GET', 'POST')

    def test_annotation_detail(self):
        url = '/annotations/<annotation_id>'
        self.assert_authenticated_request_calls(
            url, self.controller.detail, '<annotation_id>'
        )
        self.assert_supported_methods(url, 'HEAD', 'GET', 'DELETE')

    def test_annotations_for_work(self):
        url = '/annotations/<identifier_type>/<identifier>'
        self.assert_request_calls_method_using_identifier(
            url, self.controller.container_for_work,
            '<identifier_type>', "<identifier>",
            authenticated=True
        )
        self.assert_supported_methods(url, 'GET')


class TestURNLookupController(RouteTest):

    CONTROLLER_NAME = "urn_lookup"

    def test_work(self):
        url = '/works'
        self.assert_request_calls(url, self.controller.work_lookup, 'work')


class TestWorkController(RouteTest):

    CONTROLLER_NAME = "work_controller"

    def test_contributor(self):
        url = '/works/contributor/<contributor_name>'
        self.assert_request_calls(
            url, self.controller.contributor, "<contributor_name>", None, None
        )

    def test_contributor_language(self):
        url = '/works/contributor/<contributor_name>/<languages>'
        self.assert_request_calls(
            url, self.controller.contributor,
            "<contributor_name>", "<languages>", None
        )

    def test_contributor_language_audience(self):
        url = '/works/contributor/<contributor_name>/<languages>/<audiences>'
        self.assert_request_calls(
            url, self.controller.contributor,
            "<contributor_name>", "<languages>", "<audiences>"
        )

    def test_series(self):
        url = '/works/series/<series_name>'
        self.assert_request_calls(
            url, self.controller.series, "<series_name>", None, None
        )

    def test_series_language(self):
        url = '/works/series/<series_name>/<languages>'
        self.assert_request_calls(
            url, self.controller.series, "<series_name>", "<languages>", None
        )

    def test_series_language_audience(self):
        url = '/works/series/<series_name>/<languages>/<audiences>'
        self.assert_request_calls(
            url, self.controller.series, "<series_name>", "<languages>",
            "<audiences>"
        )

    def test_permalink(self):
        url = '/works/<identifier_type>/<identifier>'
        self.assert_request_calls_method_using_identifier(
            url, self.controller.permalink,
            "<identifier_type>", "<identifier>"
        )

    def test_recommendations(self):
        url = '/works/<identifier_type>/<identifier>/recommendations'
        self.assert_request_calls_method_using_identifier(
            url, self.controller.recommendations,
            "<identifier_type>", "<identifier>"
        )

    def test_related_books(self):
        url = '/works/<identifier_type>/<identifier>/related_books'
        self.assert_request_calls_method_using_identifier(
            url, self.controller.related, "<identifier_type>", "<identifier>"
        )

    def test_report(self):
        url = '/works/<identifier_type>/<identifier>/report'
        self.assert_request_calls_method_using_identifier(
            url, self.controller.report,
            "<identifier_type>", "<identifier>",
        )
        self.assert_supported_methods(url, 'GET', 'POST')


class TestAnalyticsController(RouteTest):
    CONTROLLER_NAME = "analytics_controller"

    def test_track_analytics_event(self):
        url = '/analytics/<identifier_type>/<identifier>/<event_type>'

        # This controller can be called either authenticated or
        # unauthenticated.
        self.assert_request_calls_method_using_identifier(
            url, self.controller.track_event,
            "<identifier_type>", "<identifier>", "<event_type>",
            authenticated=True,
            authentication_required=False
        )


class TestAdobeVendorID(RouteTest):

    CONTROLLER_NAME = "adobe_vendor_id"

    def test_adobe_vendor_id_get_token(self):
        url = '/AdobeAuth/authdata'
        self.assert_authenticated_request_calls(
            url, self.controller.create_authdata_handler,
            self.controller.AUTHENTICATED_PATRON
        )
        # TODO: test what happens when vendor ID is not configured.

    def test_adobe_vendor_id_signin(self):
        url = '/AdobeAuth/SignIn'
        self.assert_request_calls(
            url, self.controller.signin_handler, http_method='POST'
        )
        self.assert_supported_methods(url, 'POST')

    def test_adobe_vendor_id_accountinfo(self):
        url = '/AdobeAuth/AccountInfo'
        self.assert_request_calls(
            url, self.controller.userinfo_handler, http_method='POST'
        )
        self.assert_supported_methods(url, 'POST')

    def test_adobe_vendor_id_status(self):
        url = '/AdobeAuth/Status'
        self.assert_request_calls(
            url, self.controller.status_handler,
        )


class TestAdobeDeviceManagement(RouteTest):
    CONTROLLER_NAME = "adobe_device_management"

    def test_adobe_drm_devices(self):
        url = '/AdobeAuth/devices'
        self.assert_authenticated_request_calls(
            url, self.controller.device_id_list_handler
        )
        self.assert_supported_methods(url, 'GET', 'POST')

    def test_adobe_drm_device(self):
        url = '/AdobeAuth/devices/<device_id>'
        self.assert_authenticated_request_calls(
            url, self.controller.device_id_handler, "<device_id>",
            http_method='DELETE'
        )
        self.assert_supported_methods(url, 'DELETE')


class TestOAuthController(RouteTest):
    # TODO: We might be able to do a better job of checking that
    # flask.request.args are propagated through, instead of checking
    # an empty dict.
    CONTROLLER_NAME = "oauth_controller"

    def test_oauth_authenticate(self):
        url = '/oauth_authenticate'
        _db = self.manager._db
        self.assert_request_calls(
            url, self.controller.oauth_authentication_redirect, {}, _db
        )

    def test_oauth_callback(self):
        url = '/oauth_callback'
        _db = self.manager._db
        self.assert_request_calls(
            url, self.controller.oauth_authentication_callback, _db, {}
        )


class TestODLNotificationController(RouteTest):
    CONTROLLER_NAME = "odl_notification_controller"

    def test_odl_notify(self):
        url = '/odl_notify/<loan_id>'
        self.assert_request_calls(
            url, self.controller.notify, "<loan_id>"
        )
        self.assert_supported_methods(url, 'GET', 'POST')


class TestHeartbeatController(RouteTest):
    CONTROLLER_NAME = "heartbeat"

    def test_heartbeat(self):
        url = '/heartbeat'
        self.assert_request_calls(url, self.controller.heartbeat)


class TestHealthCheck(RouteTest):
    # This code isn't in a controller, and it doesn't really do anything,
    # so we check that it returns a specific result.
    def test_health_check(self):
        response = self.request("/healthcheck.html")
        eq_(200, response.status_code)

        # This is how we know we actually called health_check() and
        # not a mock method -- the Response returned by the mock
        # system would have an explanatory message in its .data.
        eq_("", response.get_data(as_text=True))


class TestExceptionHandler(RouteTest):

    def test_exception_handling(self):
        # The exception handler deals with most exceptions by running them
        # through ErrorHandler.handle()
        assert isinstance(error_handler_object, ErrorHandler)

        # Temporarily replace the ErrorHandler used by the
        # exception_handler function -- this is what we imported as
        # error_handler_object.
        class MockErrorHandler(object):
            def handle(self, exception):
                self.handled = exception
                return Response("handled it", 500)
        routes.h = MockErrorHandler()

        # Simulate a request that causes an unhandled exception.
        with self.app.test_request_context():
            value_error = ValueError()
            result = exception_handler(value_error)

            # The exception was passed into MockErrorHandler.handle.
            eq_(value_error, routes.h.handled)

            # The Response is created was passed along.
            eq_("handled it", result.get_data(as_text=True))
            eq_(500, result.status_code)

        # werkzeug HTTPExceptions are _not_ run through
        # handle(). werkzeug handles the conversion to a Response
        # object representing a more specific (and possibly even
        # non-error) HTTP response.
        with self.app.test_request_context():
            exception = MethodNotAllowed()
            response = exception_handler(exception)
            eq_(405, response.status_code)

        # Restore the normal error handler.
        routes.h = error_handler_object
