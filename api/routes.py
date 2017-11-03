from nose.tools import set_trace
from functools import wraps, update_wrapper
import os

import flask
from flask import (
    Response,
    redirect,
    request,
    make_response,
)
from flask_cors.core import get_cors_options, set_cors_headers

from app import app, babel

from config import Configuration
from core.app_server import (
    ErrorHandler,
    returns_problem_detail,
)
from core.model import ConfigurationSetting
from core.util.problem_detail import ProblemDetail
from opds import (
    CirculationManagerAnnotator,
)
from controller import CirculationManager
from problem_details import REMOTE_INTEGRATION_FAILED
from flask.ext.babel import lazy_gettext as _

# TODO: Without the monkeypatch below, Flask continues to process
# requests while before_first_request is running. Those requests will
# fail, since the app isn't completely set up yet.
#
# This is fixed in Flask 0.10.2, which is currently unreleased:
#  https://github.com/pallets/flask/issues/879
#
@app.before_first_request
def initialize_circulation_manager(): 
    if os.environ.get('AUTOINITIALIZE') == "False":
        # It's the responsibility of the importing code to set app.manager
        # appropriately.
        pass
    else:
        if getattr(app, 'manager', None) is None:
            app.manager = CirculationManager(app._db)
            # Make sure that any changes to the database (as might happen
            # on initial setup) are committed before continuing.
            app.manager._db.commit()

# Monkeypatch in a Flask fix that will be released in 0.10.2
def monkeypatch_try_trigger_before_first_request_functions(self):
    """Called before each request and will ensure that it triggers
    the :attr:`before_first_request_funcs` and only exactly once per
    application instance (which means process usually).
    
    :internal:
    """
    if self._got_first_request:
        return
    with self._before_request_lock:
        if self._got_first_request:
            return
        for func in self.before_first_request_funcs:
            func() 
        self._got_first_request = True

from flask import Flask
Flask.try_trigger_before_first_request_functions = monkeypatch_try_trigger_before_first_request_functions

@babel.localeselector
def get_locale():
    languages = Configuration.localization_languages()
    return request.accept_languages.best_match(languages)

@app.teardown_request
def shutdown_session(exception):
    if (hasattr(app, 'manager') 
        and hasattr(app.manager, '_db') 
        and app.manager._db
    ):
        if exception:
            app.manager._db.rollback()
        else:
            app.manager._db.commit()

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        patron = app.manager.index_controller.authenticated_patron_from_request()
        if isinstance(patron, ProblemDetail):
            return patron.response
        elif isinstance(patron, Response):
            return patron
        else:
            return f(*args, **kwargs)
    return decorated

# The allows_patron_web decorator will add Cross-Origin Resource Sharing
# (CORS) headers to routes that will be used by the patron web interface.
# This is necessary for a JS app on a different domain to make requests.
#
# This is mostly taken from the cross_origin decorator in flask_cors, but we
# can't use that decorator because we aren't able to look up the patron web
# client url configuration setting at the time we create the decorator.
def allows_patron_web(f):
    # Override Flask's default behavior and intercept the OPTIONS method for
    # every request so CORS headers can be added.
    f.required_methods = getattr(f, 'required_methods', set())
    f.required_methods.add("OPTIONS")
    f.provide_automatic_options = False

    def wrapped_function(*args, **kwargs):
        if request.method == "OPTIONS":
            resp = app.make_default_options_response()
        else:
            resp = make_response(f(*args, **kwargs))

        patron_web_client_url = app.manager.patron_web_client_url
        if patron_web_client_url:
            options = get_cors_options(
                app, dict(origins=[patron_web_client_url],
                          supports_credentials=True)
            )
            set_cors_headers(resp, options)

        return resp
    return update_wrapper(wrapped_function, f)

h = ErrorHandler(app, app.config['DEBUG'])
@app.errorhandler(Exception)
@allows_patron_web
def exception_handler(exception):
    return h.handle(exception)

def has_library(f):
    """Decorator to extract the library short name from the arguments."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'library_short_name' in kwargs:
            library_short_name = kwargs.pop("library_short_name")
        else:
            library_short_name = None
        library = app.manager.index_controller.library_for_request(library_short_name)
        if isinstance(library, ProblemDetail):
            return library.response
        else:
            return f(*args, **kwargs)
    return decorated

def library_route(path, *args, **kwargs):
    """Decorator to creates routes that have a library short name in either
    a subdomain or a url path prefix. If not used with @has_library, the view function
    must have a library_short_name argument.
    """
    def decorator(f):
        # This sets up routes for both the subdomain and the url path prefix.
        # The order of these determines which one will be used by url_for -
        # in this case it's the prefix route.
        # We may want to have a configuration option to specify whether to
        # use a subdomain or a url path prefix.
        prefix_route = app.route("/<library_short_name>" + path, *args, **kwargs)(f)
        subdomain_route = app.route(path, subdomain="<library_short_name>", *args, **kwargs)(prefix_route)
        default_library_route = app.route(path, *args, **kwargs)(subdomain_route)
        return default_library_route
    return decorator

def library_dir_route(path, *args, **kwargs):
    """Decorator to create library routes that work with or without a
    trailing slash."""
    if path.endswith("/"):
        path_without_slash = path[:-1]
    else:
        path_without_slash = path

    def decorator(f):
        # By default, creating a route with a slash will make flask redirect
        # requests without the slash, even if that route also exists.
        # Setting strict_slashes to False disables this behavior.
        # This is important for CORS because the redirects are not processed
        # by the CORS decorator and won't be valid CORS responses.

        # Decorate f with four routes, with and without the slash, with a prefix or subdomain
        prefix_slash = app.route("/<library_short_name>" + path_without_slash + "/", strict_slashes=False, *args, **kwargs)(f)
        prefix_no_slash = app.route("/<library_short_name>" + path_without_slash, *args, **kwargs)(prefix_slash)
        subdomain_slash = app.route(path_without_slash + "/", strict_slashes=False, subdomain="<library_short_name>", *args, **kwargs)(prefix_no_slash)
        subdomain_no_slash = app.route(path_without_slash, subdomain="<library_short_name>", *args, **kwargs)(subdomain_slash)
        default_library_slash = app.route(path_without_slash, *args, **kwargs)(subdomain_no_slash)
        default_library_no_slash = app.route(path_without_slash + "/", *args, **kwargs)(default_library_slash)
        return default_library_no_slash
    return decorator

@library_route("/")
@has_library
@allows_patron_web
@returns_problem_detail
def index():
    return app.manager.index_controller()

@library_route('/authentication_document')
@has_library
@returns_problem_detail
def authentication_document():
    return app.manager.index_controller.authentication_document()

@library_route('/public_key_document')
@returns_problem_detail
def public_key_document():
    return app.manager.index_controller.public_key_document()

@library_dir_route('/groups', defaults=dict(lane_identifier=None))
@library_route('/groups/<lane_identifier>')
@has_library
@allows_patron_web
@returns_problem_detail
def acquisition_groups(lane_identifier):
    return app.manager.opds_feeds.groups(lane_identifier)

@library_dir_route('/feed', defaults=dict(lane_identifier=None))
@library_route('/feed/<lane_identifier>')
@has_library
@allows_patron_web
@returns_problem_detail
def feed(lane_identifier):
    return app.manager.opds_feeds.feed(lane_identifier)

@library_dir_route('/search', defaults=dict(lane_identifier=None))
@library_route('/search/<lane_identifier>')
@has_library
@allows_patron_web
@returns_problem_detail
def lane_search(lane_identifier):
    return app.manager.opds_feeds.search(lane_identifier)

@library_dir_route('/patrons/me', methods=['GET', 'PUT'])
@has_library
@allows_patron_web
@requires_auth
@returns_problem_detail
def patron_profile():
    return app.manager.profiles.protocol()

@library_dir_route('/loans', methods=['GET', 'HEAD'])
@has_library
@allows_patron_web
@requires_auth
@returns_problem_detail
def active_loans():
    return app.manager.loans.sync()

@library_route('/annotations/', methods=['HEAD', 'GET', 'POST'])
@has_library
@allows_patron_web
@requires_auth
@returns_problem_detail
def annotations():
    return app.manager.annotations.container()

@library_route('/annotations/<annotation_id>', methods=['HEAD', 'GET', 'DELETE'])
@has_library
@allows_patron_web
@requires_auth
@returns_problem_detail
def annotation_detail(annotation_id):
    return app.manager.annotations.detail(annotation_id)

@library_route('/annotations/<identifier_type>/<path:identifier>/', methods=['GET'])
@has_library
@allows_patron_web
@requires_auth
@returns_problem_detail
def annotations_for_work(identifier_type, identifier):
    return app.manager.annotations.container_for_work(identifier_type, identifier)

@library_route('/works/<identifier_type>/<path:identifier>/borrow', methods=['GET', 'PUT'])
@library_route('/works/<identifier_type>/<path:identifier>/borrow/<mechanism_id>', 
           methods=['GET', 'PUT'])
@has_library
@allows_patron_web
@requires_auth
@returns_problem_detail
def borrow(identifier_type, identifier, mechanism_id=None):
    return app.manager.loans.borrow(identifier_type, identifier, mechanism_id)

@library_route('/works/<license_pool_id>/fulfill')
@library_route('/works/<license_pool_id>/fulfill/<mechanism_id>')
@has_library
@allows_patron_web
@requires_auth
@returns_problem_detail
def fulfill(license_pool_id, mechanism_id=None):
    return app.manager.loans.fulfill(license_pool_id, mechanism_id)

@library_route('/loans/<license_pool_id>/revoke', methods=['GET', 'PUT'])
@has_library
@allows_patron_web
@requires_auth
@returns_problem_detail
def revoke_loan_or_hold(license_pool_id):
    return app.manager.loans.revoke(license_pool_id)

@library_route('/loans/<identifier_type>/<path:identifier>', methods=['GET', 'DELETE'])
@has_library
@allows_patron_web
@requires_auth
@returns_problem_detail
def loan_or_hold_detail(identifier_type, identifier):
    return app.manager.loans.detail(identifier_type, identifier)

@library_dir_route('/works')
@has_library
@allows_patron_web
@returns_problem_detail
def work():
    annotator = CirculationManagerAnnotator(app.manager.circulation, None)
    return app.manager.urn_lookup.work_lookup(annotator, 'work')

@library_dir_route('/works/contributor/<contributor_name>', defaults=dict(languages=None, audiences=None))
@library_dir_route('/works/contributor/<contributor_name>/<languages>', defaults=dict(audiences=None))
@library_route('/works/contributor/<contributor_name>/<languages>/<audiences>')
@has_library
@allows_patron_web
@returns_problem_detail
def contributor(contributor_name, languages, audiences):
    return app.manager.work_controller.contributor(contributor_name, languages, audiences)

@library_dir_route('/works/series/<series_name>', defaults=dict(languages=None, audiences=None))
@library_dir_route('/works/series/<series_name>/<languages>', defaults=dict(audiences=None))
@library_route('/works/series/<series_name>/<languages>/<audiences>')
@has_library
@allows_patron_web
@returns_problem_detail
def series(series_name, languages, audiences):
    return app.manager.work_controller.series(series_name, languages, audiences)

@library_route('/works/<identifier_type>/<path:identifier>')
@has_library
@allows_patron_web
@returns_problem_detail
def permalink(identifier_type, identifier):
    return app.manager.work_controller.permalink(identifier_type, identifier)

@library_route('/works/<identifier_type>/<path:identifier>/recommendations')
@has_library
@allows_patron_web
@returns_problem_detail
def recommendations(identifier_type, identifier):
    return app.manager.work_controller.recommendations(identifier_type, identifier)

@library_route('/works/<identifier_type>/<path:identifier>/related_books')
@has_library
@allows_patron_web
@returns_problem_detail
def related_books(identifier_type, identifier):
    return app.manager.work_controller.related(identifier_type, identifier)

@library_route('/works/<identifier_type>/<path:identifier>/report', methods=['GET', 'POST'])
@has_library
@allows_patron_web
@returns_problem_detail
def report(identifier_type, identifier):
    return app.manager.work_controller.report(identifier_type, identifier)

@library_route('/analytics/<identifier_type>/<path:identifier>/<event_type>')
@has_library
@allows_patron_web
@returns_problem_detail
def track_analytics_event(identifier_type, identifier, event_type):
    return app.manager.analytics_controller.track_event(identifier_type, identifier, event_type)

# Adobe Vendor ID implementation
@library_route('/AdobeAuth/authdata')
@has_library
@requires_auth
@returns_problem_detail
def adobe_vendor_id_get_token():
    if not app.manager.adobe_vendor_id:
        return REMOTE_INTEGRATION_FAILED.detailed(
            _("This server does not have an Adobe Vendor ID server configured.")
        )
    return app.manager.adobe_vendor_id.create_authdata_handler(flask.request.patron)

@library_route('/AdobeAuth/SignIn', methods=['POST'])
@has_library
@returns_problem_detail
def adobe_vendor_id_signin():
    return app.manager.adobe_vendor_id.signin_handler()

@app.route('/AdobeAuth/AccountInfo', methods=['POST'])
@returns_problem_detail
def adobe_vendor_id_accountinfo():
    return app.manager.adobe_vendor_id.userinfo_handler()

@app.route('/AdobeAuth/Status')
@returns_problem_detail
def adobe_vendor_id_status():
    return app.manager.adobe_vendor_id.status_handler()

# DRM Device Management Protocol implementation for ACS.
@library_route('/AdobeAuth/devices', methods=['GET', 'POST'])
@has_library
@requires_auth
@returns_problem_detail
def adobe_drm_devices():
    return app.manager.adobe_device_management.device_id_list_handler()

@library_route('/AdobeAuth/devices/<device_id>', methods=['DELETE'])
@has_library
@requires_auth
@returns_problem_detail
def adobe_drm_device(device_id):
    return app.manager.adobe_device_management.device_id_handler(device_id)
    
# Route that redirects to the authentication URL for an OAuth provider
@library_route('/oauth_authenticate')
@has_library
@returns_problem_detail
def oauth_authenticate():
    return app.manager.oauth_controller.oauth_authentication_redirect(flask.request.args, app.manager._db)

# Redirect URI for OAuth providers, eg. Clever
@library_route('/oauth_callback')
@has_library
@returns_problem_detail
def oauth_callback():
    return app.manager.oauth_controller.oauth_authentication_callback(app.manager._db, flask.request.args)

# Loan notifications for ODL distributors, eg. Feedbooks
@library_route('/odl_notify/<loan_id>', methods=['GET', 'POST'])
@has_library
@returns_problem_detail
def odl_notify(loan_id):
    return app.manager.odl_notification_controller.notify(loan_id)

# Controllers used for operations purposes
@app.route('/heartbeat')
@returns_problem_detail
def heartbeat():
    return app.manager.heartbeat.heartbeat()

@library_route("/service_status")
@has_library
@returns_problem_detail
def service_status():
    return app.manager.service_status()

@app.route('/loadstorm-<code>.html')
@returns_problem_detail
def loadstorm_verify(code):
    c = Configuration.integration("Loadstorm", required=True)
    if code == c['verification_code']:
        return Response("", 200)
    else:
        return Response("", 404)

@app.route('/healthcheck.html')
def health_check():
    return Response("", 200)
