from nose.tools import set_trace
from functools import wraps
import os

import flask
from flask import (
    Response,
    redirect,
    request,
)

from app import app, _db, babel

from config import Configuration
from core.app_server import (
    ErrorHandler,
    returns_problem_detail,
)
from core.util.problem_detail import ProblemDetail
from opds import (
    CirculationManagerAnnotator,
)
from controller import CirculationManager

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
            app.manager = CirculationManager(_db)
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

h = ErrorHandler(app, app.config['DEBUG'])
@app.errorhandler(Exception)
def exception_handler(exception):
    return h.handle(exception)

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

@app.route('/')
@returns_problem_detail
def index():
    return app.manager.index_controller()

@app.route('/groups', defaults=dict(lane_name=None, languages=None))
@app.route('/groups/', defaults=dict(lane_name=None, languages=None))
@app.route('/groups/<languages>', defaults=dict(lane_name=None))
@app.route('/groups/<languages>/', defaults=dict(lane_name=None))
@app.route('/groups/<languages>/<lane_name>')
@returns_problem_detail
def acquisition_groups(languages, lane_name):
    return app.manager.opds_feeds.groups(languages, lane_name)

@app.route('/feed', defaults=dict(lane_name=None, languages=None))
@app.route('/feed/', defaults=dict(lane_name=None, languages=None))
@app.route('/feed/<languages>', defaults=dict(lane_name=None))
@app.route('/feed/<languages>/', defaults=dict(lane_name=None))
@app.route('/feed/<languages>/<lane_name>')
@returns_problem_detail
def feed(languages, lane_name):
    return app.manager.opds_feeds.feed(languages, lane_name)

@app.route('/search', defaults=dict(lane_name=None, languages=None))
@app.route('/search/', defaults=dict(lane_name=None, languages=None))
@app.route('/search/<languages>', defaults=dict(lane_name=None))
@app.route('/search/<languages>/', defaults=dict(lane_name=None))
@app.route('/search/<languages>/<lane_name>')
@returns_problem_detail
def lane_search(languages, lane_name):
    return app.manager.opds_feeds.search(languages, lane_name)

@app.route('/preload')
@returns_problem_detail
def preload():
    return app.manager.opds_feeds.preload()

@app.route('/me', methods=['GET'])
@requires_auth
@returns_problem_detail
def account():
    return app.manager.accounts.account()

@app.route('/loans/', methods=['GET', 'HEAD'])
@requires_auth
@returns_problem_detail
def active_loans():
    return app.manager.loans.sync()

@app.route('/works/<data_source>/<identifier_type>/<path:identifier>/borrow', methods=['GET', 'PUT'])
@app.route('/works/<data_source>/<identifier_type>/<path:identifier>/borrow/<mechanism_id>', 
           methods=['GET', 'PUT'])
@requires_auth
@returns_problem_detail
def borrow(data_source, identifier_type, identifier, mechanism_id=None):
    return app.manager.loans.borrow(data_source, identifier_type, identifier, mechanism_id)

@app.route('/works/<data_source>/<identifier_type>/<path:identifier>/fulfill/')
@app.route('/works/<data_source>/<identifier_type>/<path:identifier>/fulfill/<mechanism_id>')
@requires_auth
@returns_problem_detail
def fulfill(data_source, identifier_type, identifier, mechanism_id=None):
    return app.manager.loans.fulfill(data_source, identifier_type, identifier, mechanism_id)

@app.route('/loans/<data_source>/<identifier_type>/<path:identifier>/revoke', methods=['GET', 'PUT'])
@requires_auth
@returns_problem_detail
def revoke_loan_or_hold(data_source, identifier_type, identifier):
    return app.manager.loans.revoke(data_source, identifier_type, identifier)

@app.route('/loans/<data_source>/<identifier_type>/<path:identifier>', methods=['GET', 'DELETE'])
@requires_auth
@returns_problem_detail
def loan_or_hold_detail(data_source, identifier_type, identifier):
    return app.manager.loans.detail(data_source, identifier_type, identifier)

@app.route('/works/')
@returns_problem_detail
def work():
    annotator = CirculationManagerAnnotator(app.manager.circulation, None)
    return app.manager.urn_lookup.work_lookup(annotator, 'work')

@app.route('/works/series/<series_name>')
@returns_problem_detail
def series(series_name):
    return app.manager.work_controller.series(series_name)

@app.route('/works/<data_source>/<identifier_type>/<path:identifier>')
@returns_problem_detail
def permalink(data_source, identifier_type, identifier):
    return app.manager.work_controller.permalink(data_source, identifier_type, identifier)

@app.route('/works/<data_source>/<identifier_type>/<path:identifier>/recommendations')
@returns_problem_detail
def recommendations(data_source, identifier_type, identifier):
    return app.manager.work_controller.recommendations(data_source, identifier_type, identifier)

@app.route('/works/<data_source>/<identifier_type>/<path:identifier>/related_books')
@returns_problem_detail
def related_books(data_source, identifier_type, identifier):
    return app.manager.work_controller.related(data_source, identifier_type, identifier)

@app.route('/works/<data_source>/<identifier_type>/<path:identifier>/report', methods=['GET', 'POST'])
@returns_problem_detail
def report(data_source, identifier_type, identifier):
    return app.manager.work_controller.report(data_source, identifier_type, identifier)

@app.route('/analytics/<data_source>/<identifier_type>/<path:identifier>/<event_type>')
@returns_problem_detail
def track_analytics_event(data_source, identifier_type, identifier, event_type):
    return app.manager.analytics_controller.track_event(data_source, identifier_type, identifier, event_type)

# Adobe Vendor ID implementation
@app.route('/AdobeAuth/authdata')
@requires_auth
@returns_problem_detail
def adobe_vendor_id_get_token():
    return app.manager.adobe_vendor_id.create_authdata_handler(flask.request.patron)

@app.route('/AdobeAuth/SignIn', methods=['POST'])
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

# Redirect URI for OAuth providers, eg. Clever
@app.route('/oauth_callback')
@returns_problem_detail
def oauth_callback():
    return app.manager.auth.oauth_callback(app.manager._db, flask.request.args)


# Controllers used for operations purposes
@app.route('/heartbeat')
@returns_problem_detail
def heartbeat():
    return app.manager.heartbeat.heartbeat()

@app.route('/service_status')
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

