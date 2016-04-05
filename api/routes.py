from nose.tools import set_trace
from functools import wraps
import os

import flask
from flask import (
    Response,
    redirect,
)

from app import app

from config import Configuration
from core.app_server import (
    ErrorHandler,
)
from core.util.problem_detail import ProblemDetail
from opds import (
    CirculationManagerAnnotator,
)
from controller import CirculationManager


@app.before_first_request
def initialize_circulation_manager():
    if os.environ.get('AUTOINITIALIZE') == "False":
        pass
        # It's the responsibility of the importing code to set app.manager
        # appropriately.
    else:
        if getattr(app, 'manager', None) is None:
            app.manager = CirculationManager()
            # Make sure that any changes to the database (as might happen
            # on initial setup) are committed before continuing.
            app.manager._db.commit()



h = ErrorHandler(app, app.config['DEBUG'])
@app.errorhandler(Exception)
def exception_handler(exception):
    return h.handle(exception)

@app.teardown_request
def shutdown_session(exception):
    if app.manager._db:
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

def returns_problem_detail(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        v = f(*args, **kwargs)
        if isinstance(v, ProblemDetail):
            return v.response
        return v
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

@app.route('/works/<data_source>/<identifier>/borrow', methods=['GET', 'PUT'])
@app.route('/works/<data_source>/<identifier>/borrow/<mechanism_id>', 
           methods=['GET', 'PUT'])
@requires_auth
@returns_problem_detail
def borrow(data_source, identifier, mechanism_id=None):
    return app.manager.loans.borrow(data_source, identifier, mechanism_id)

@app.route('/works/<data_source>/<identifier>/fulfill/')
@app.route('/works/<data_source>/<identifier>/fulfill/<mechanism_id>')
@requires_auth
@returns_problem_detail
def fulfill(data_source, identifier, mechanism_id=None):
    return app.manager.loans.fulfill(data_source, identifier, mechanism_id)

@app.route('/loans/<data_source>/<identifier>/revoke', methods=['GET', 'PUT'])
@requires_auth
@returns_problem_detail
def revoke_loan_or_hold(data_source, identifier):
    return app.manager.loans.revoke(data_source, identifier)

@app.route('/loans/<data_source>/<identifier>', methods=['GET', 'DELETE'])
@requires_auth
@returns_problem_detail
def loan_or_hold_detail(data_source, identifier):
    return app.manager.loans.detail(data_source, identifier)

@app.route('/works/')
@returns_problem_detail
def work():
    annotator = CirculationManagerAnnotator(app.manager.circulation, None)
    return app.manager.urn_lookup.work_lookup(annotator, 'work')

@app.route('/works/<data_source>/<identifier>')
@returns_problem_detail
def permalink(data_source, identifier):
    return app.manager.work_controller.permalink(data_source, identifier)
    
@app.route('/works/<data_source>/<identifier>/report', methods=['GET', 'POST'])
@returns_problem_detail
def report(data_source, identifier):
    return app.manager.work_controller.report(data_source, identifier)

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

