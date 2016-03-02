from functools import wraps
from nose.tools import set_trace
import logging
import os
import urlparse

import flask
from flask import (
    Flask, 
    Response,
    redirect,
)

from config import Configuration
from core.app_server import (
    ErrorHandler,
)
from core.util.problem_detail import ProblemDetail
from core.util.flask_util import (
    problem,
)

from controller import CirculationManager
from opds import (
    CirculationManagerAnnotator,
)

app = Flask(__name__)
debug = Configuration.logging_policy().get("level") == 'DEBUG'
logging.getLogger().info("Application debug mode==%r" % debug)
app.config['DEBUG'] = debug
app.debug = debug

# The secret key is used for signing cookies for admin login
app.secret_key = Configuration.get(Configuration.SECRET_KEY)

if os.environ.get('AUTOINITIALIZE') == "False":
    pass
    # It's the responsibility of the importing code to set app.manager
    # appropriately.
else:
    if getattr(app, 'manager', None) is None:
        app.manager = CirculationManager()

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

def requires_admin(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        admin = app.manager.admin_controller.authenticated_admin_from_request()
        if isinstance(admin, ProblemDetail):
            return app.manager.admin_controller.error_response(admin)
        elif isinstance(admin, Response):
            return admin
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

@app.route('/GoogleAuth/callback')
@returns_problem_detail
def google_auth_callback():
    return app.manager.admin_controller.redirect_after_signin()

@app.route('/admin')
@returns_problem_detail
def admin():
    return app.manager.admin_controller.signin()

@app.route('/complaints')
@returns_problem_detail
def complaints():
    return app.manager.opds_feeds.complaints()

# Controllers used for operations purposes
@app.route('/heartbeat')
@returns_problem_detail
def hearbeat():
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

if __name__ == '__main__':
    debug = True
    url = Configuration.integration_url(
        Configuration.CIRCULATION_MANAGER_INTEGRATION, required=True)
    scheme, netloc, path, parameters, query, fragment = urlparse.urlparse(url)
    if ':' in netloc:
        host, port = netloc.split(':')
        port = int(port)
    else:
        host = netloc
        port = 80

    # Workaround for a "Resource temporarily unavailable" error when
    # running in debug mode with the global socket timeout set by isbnlib
    if debug:
        import socket
        socket.setdefaulttimeout(None)

    app.manager.log.info("Starting app on %s:%s", host, port)
    app.run(debug=debug, host=host, port=port)

