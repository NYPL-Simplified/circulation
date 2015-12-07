from nose.tools import set_trace
import logging
import os

import flask
from flask import (
    Flask, 
    Response,
)

from config import Configuration
from core.app_server import (
    ErrorHandler,
)

import urllib
from core.util.flask_util import (
    problem,
)
from opds import (
    CirculationManagerAnnotator,
)
from functools import wraps

app = Flask(__name__)
debug = Configuration.logging_policy().get("level") == 'DEBUG'
logging.getLogger().info("Application debug mode==%r" % debug)
app.config['DEBUG'] = debug
app.debug = debug

if os.environ.get('AUTOINITIALIZE') == "False":
    pass
    # It's the responsibility of the importing code to set app.manager
    # appropriately.
else:
    app.manager.testing = False
    Conf.initialize()

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
            return patron
        elif isinstance(patron, Response):
            return patron
        else:
            return f(*args, **kwargs)
    return decorated
        
@app.route('/')
def index():
    return app.manager.index_controller()

@app.route('/groups', defaults=dict(lane_name=None, languages=None))
@app.route('/groups/', defaults=dict(lane_name=None, languages=None))
@app.route('/groups/<languages>', defaults=dict(lane_name=None))
@app.route('/groups/<languages>/', defaults=dict(languages=None))
@app.route('/groups/<languages>/<lane_name>')
def acquisition_groups(languages, lane_name):
    return app.manager.opds_feeds.groups(languages, lane_name)

@app.route('/feed', defaults=dict(lane_name=None, languages=None))
@app.route('/feed/', defaults=dict(lane_name=None, languages=None))
@app.route('/feed/<languages>', defaults=dict(lane_name=None))
@app.route('/feed/<languages>/', defaults=dict(languages=None))
@app.route('/feed/<languages>/<lane_name>')
def feed(languages, lane_name):
    return app.manager.opds_feeds.feed(languages, lane_name)

@app.route('/search', defaults=dict(lane_name=None, languages=None))
@app.route('/search/', defaults=dict(lane_name=None, languages=None))
@app.route('/search/<languages>', defaults=dict(lane_name=None))
@app.route('/search/<languages>/', defaults=dict(languages=None))
@app.route('/search/<languages>/<lane_name>')
def lane_search(languages, lane_name):
    return app.manager.opds_feeds.search(languages, lane_name)

@app.route('/loans/', methods=['GET', 'HEAD'])
@requires_auth
def active_loans():
    return app.manager.loans.sync()

@app.route('/works/<data_source>/<identifier>/borrow', methods=['GET', 'PUT'])
@app.route('/works/<data_source>/<identifier>/borrow/<mechanism_id>', 
           methods=['GET', 'PUT'])
@requires_auth
def borrow(data_source, identifier, mechanism_id=None):
    return app.manager.loans.borrow(data_source, identifier, mechanism_id)

@app.route('/works/<data_source>/<identifier>/fulfill/')
@app.route('/works/<data_source>/<identifier>/fulfill/<mechanism_id>')
@requires_auth
def fulfill(data_source, identifier, mechanism_id=None):
    return app.manager.loans.fulfill(data_source, identifier, mechanism_id)

@app.route('/loans/<data_source>/<identifier>/revoke', methods=['GET', 'PUT'])
@requires_auth
def revoke_loan_or_hold(data_source, identifier):
    return app.manager.loans.revoke(data_source, identifier)

@app.route('/loans/<data_source>/<identifier>', methods=['GET', 'DELETE'])
@requires_auth
def loan_or_hold_detail(data_source, identifier):
    return app.manager.loans.detail(data_source, identifier)

@app.route('/works/')
def work():
    annotator = CirculationManagerAnnotator(app.manager.circulation, None)
    return app.manager.urn_lookup.work_lookup(annotator, 'work')

@app.route('/works/<data_source>/<identifier>')
def permalink(data_source, identifier):
    return app.manager.works_controller.permalink(data_source, identifier)
    
@app.route('/works/<data_source>/<identifier>/report', methods=['GET', 'POST'])
def report(data_source, identifier):
    return app.manager.works_controller.report(data_source, identifier)

# Adobe Vendor ID implementation
@app.route('/AdobeAuth/authdata')
@requires_auth
def adobe_vendor_id_get_token():
    return app.manager.adobe_vendor_id.create_authdata_handler(flask.request.patron)

@app.route('/AdobeAuth/SignIn', methods=['POST'])
def adobe_vendor_id_signin():
    return app.manager.adobe_vendor_id.signin_handler()

@app.route('/AdobeAuth/AccountInfo', methods=['POST'])
def adobe_vendor_id_accountinfo():
    return app.manager.adobe_vendor_id.userinfo_handler()

@app.route('/AdobeAuth/Status')
def adobe_vendor_id_status():
    return app.manager.adobe_vendor_id.status_handler()
# Controllers used for operations purposes
@app.route('/heartbeat')
def hearbeat():
    return app.manager.heartbeat.heartbeat()

@app.route('/service_status')
def service_status():
    return app.manager.service_status()

@app.route('/loadstorm-<code>.html')
def loadstorm_verify(code):
    c = Configuration.integration("Loadstorm", required=True)
    if code == c['verification_code']:
        return Response("", 200)
    else:
        return Response("", 404)

# @app.route('/force_error/<string>')
# def force_error(string):
#     raise Exception("Forced error: %s" % string)
#     # return problem(None, "Forced error: %s" % string, 500)

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
    app.manager = CirculationManager()
    app.manager.log.info("Starting app on %s:%s", host, port)
    app.run(debug=debug, host=host, port=port)
