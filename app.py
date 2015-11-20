from functools import wraps
from nose.tools import set_trace
from lxml import etree
import datetime
import json
import logging
import random
import time
import os
import sys
import traceback
import urlparse
import uuid

from sqlalchemy.orm.exc import (
    NoResultFound
)

import flask
from flask import Flask, url_for, redirect, Response, make_response

from config import Configuration, CannotLoadConfiguration
from core.external_search import (
    ExternalSearchIndex,
    DummyExternalSearchIndex,
)
from circulation import CirculationAPI
from circulation_exceptions import *
from authenticator import Authenticator
from core.app_server import (
    cdn_url_for,
    load_lending_policy,
    ComplaintController,
    HeartbeatController,
    URNLookupController,
    ErrorHandler,
)
from core.lane import (
    Lane,
)
from adobe_vendor_id import AdobeVendorIDController
from axis import (
    Axis360API,
)
from overdrive import (
    OverdriveAPI,
    DummyOverdriveAPI,
)
from threem import (
    ThreeMAPI,
    DummyThreeMAPI,
)

from core.model import (
    get_one,
    get_one_or_create,
    DataSource,
    production_session,
    Hold,
    LicensePool,
    LicensePoolDeliveryMechanism,
    Loan,
    Patron,
    Identifier,
    Representation,
    Work,
    Edition,
    )
from opds import (
    CirculationManagerAnnotator,
    CirculationManagerLoanAndHoldAnnotator,
)
from core.opds import (
    E,
    AcquisitionFeed,
    OPDSFeed,
)
import urllib
from core.util.flask_util import (
    problem,
    problem_raw,
)
from core.util.opds_authentication_document import OPDSAuthenticationDocument
from lanes import make_lanes

class CirculationManager(object):

    def __init__(self, _db=None, lanes=None, testing=False):

        self.log = logging.getLogger("Circulation manager web app")

        try:
            self.config = Configuration.load()
        except CannotLoadConfiguration, e:
            self.log.error("Could not load configuration file: %s" % e)
            sys.exit()

        if _db is None and not self.testing:
            _db = production_session()
        self._db = _db

        self.testing = testing
        self.lanes = make_lanes(_db, lanes)
        self.sublanes = self.lanes

        self.auth = Authenticator.initialize(self._db, test=testing)
        self.setup_circulation()
        self.search = self.setup_search()
        self.policy = self.setup_policy()

        self.setup_controllers()
        self.urn_lookup_controller = URNLookupController(self._db)
        self.setup_adobe_vendor_id()

        if self.testing:
            self.hold_notification_email_address = 'test@test'
        else:
            self.hold_notification_email_address = Configuration.default_notification_email_address()

        self.opds_authentication_document = self.create_authentication_document()
        self.log.debug("Lane layout:")
        self.log_lanes()

    def cdn_url_for(self, view, *args, **kwargs):
        return cdn_url_for(view, *args, **kwargs)

    def url_for(self, view, *args, **kwargs):
        kwargs['_external'] = True
        return url_for(view, *args, **kwargs)

    def log_lanes(self, lanelist=None, level=0):
        """Output information about the lane layout."""
        lanelist = lanelist or self.lanes
        for lane in lanelist.lanes:
            self.log.debug("%s%r", "-" * level, lane)
            self.log_lanes(lane.sublanes, level+1)

    def setup_search(self):
        """Set up a search client."""
        if self.testing:
            return DummyExternalSearchIndex()
        else:
            if Configuration.integration(
                    Configuration.ELASTICSEARCH_INTEGRATION):
                return ExternalSearchIndex()
            else:
                self.log.warn("No external search server configured.")
                return None

    def setup_policy(self):
        if self.testing:
            return {}
        else:
            return load_lending_policy(
                Configuration.policy('lending', {})
            )

    def setup_circulation(self):
        """Set up distributor APIs and a the Circulation object."""
        if self.testing:

            self.overdrive = DummyOverdriveAPI(self._db)
            self.threem = DummyThreeMAPI(self._db)
            self.axis = None
        else:
            self.overdrive = OverdriveAPI.from_environment(self._db)
            self.threem = ThreeMAPI.from_environment(self._db)
            self.axis = Axis360API.from_environment(self._db)
        self.circulation = CirculationAPI(
            _db=self._db, 
            threem=self.threem, 
            overdrive=self.overdrive,
            axis=self.axis
        )


    def setup_controllers(self):
        """Set up all the controllers that will be used by the web app."""
        self.index_controller = IndexController(self)
        self.opds_feeds = OPDSFeedController(self)
        self.loans = LoanController(self)
        self.urn_lookup = URNLookupController(self._db)
        self.works_controller = WorksController(self)

        self.heartbeat = HearbeatController()
        self.service_status = ServiceStatusController(self)

    def setup_adobe_vendor_id(self):
        """Set up the controller for Adobe Vendor ID."""
        adobe = Configuration.integration(
            Configuration.ADOBE_VENDOR_ID_INTEGRATION
        )
        vendor_id = adobe.get(Configuration.ADOBE_VENDOR_ID)
        node_value = adobe.get(Configuration.ADOBE_VENDOR_ID_NODE_VALUE)
        if vendor_id and node_value:
            self.adobe_vendor_id = AdobeVendorIDController(
                self.db,
                vendor_id,
                node_value,
                self.auth
            )
        else:
            cls.log.warn("Adobe Vendor ID controller is disabled due to missing or incomplete configuration.")
            self.adobe_vendor_id = None


    def create_authentication_document(self):
        """Create the OPDS authentication document to be used when
        there's a 401 error.
        """
        base_opds_document = Configuration.base_opds_authentication_document()
        auth_type = [OPDSAuthenticationDocument.BASIC_AUTH_FLOW]
        circulation_manager_url = Configuration.integration_url(
            Configuration.CIRCULATION_MANAGER_INTEGRATION, required=True)
        scheme, netloc, path, parameters, query, fragment = (
            urlparse.urlparse(circulation_manager_url))
        opds_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, str(netloc)))

        links = {}
        for rel, value in (
                ("terms-of-service", Configuration.terms_of_service_url()),
                ("privacy-policy", Configuration.privacy_policy_url()),
                ("copyright", Configuration.acknowledgements_url()),
        ):
            if value:
                links[rel] = dict(href=value, type="text/html")

        doc = OPDSAuthenticationDocument.fill_in(
            base_opds_document, auth_type, "Library", opds_id, None, "Barcode",
            "PIN", links=links
            )

        return json.dumps(doc)

if os.environ.get('TESTING') == "True":
    Conf.testing = True
    # It's the test's responsibility to call initialize()
else:
    Conf.testing = False
    Conf.initialize()

app = Flask(__name__)
debug = Configuration.logging_policy().get("level") == 'DEBUG'
logging.getLogger().info("Application debug mode==%r" % debug)
app.config['DEBUG'] = debug
app.debug = debug

h = ErrorHandler(app, app.config['DEBUG'])
@app.errorhandler(Exception)
def exception_handler(exception):
    return h.handle(exception)

@app.teardown_request
def shutdown_session(exception):
    if app.cm.db:
        if exception:
            app.cm.db.rollback()
        else:
            app.cm.db.commit()

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        header = flask.request.authorization
        if not header:
            # No credentials were provided.
            return authenticate(
                INVALID_CREDENTIALS_PROBLEM,
                INVALID_CREDENTIALS_TITLE)

        try:
            patron = authenticated_patron(header.username, header.password)
        except RemoteInitiatedServerError,e:
            return problem(REMOTE_INTEGRATION_FAILED, e.message, 500)
        if isinstance(patron, tuple):
            flask.request.patron = None
            return authenticate(*patron)
        else:
            flask.request.patron = patron
        return f(*args, **kwargs)
    return decorated

@app.route('/')
def index():
    return app.manager.index_controller()

@app.route('/groups', defaults=dict(lane_name=None, languages=None))
@app.route('/groups/', defaults=dict(lane_name=None, languages=None))
@app.route('/groups/<languages>', defaults=dict(lane_name=None)))
@app.route('/groups/<languages>/', defaults=dict(languages=None)))
@app.route('/groups/<languages>/<lane_name>')
def acquisition_groups(languages, lane_name):
    return app.manager.opds_feeds.groups(languages, lane_name)

@app.route('/feed', defaults=dict(lane_name=None, languages=None))
@app.route('/feed/', defaults=dict(lane_name=None, languages=None))
@app.route('/feed/<languages>', defaults=dict(lane_name=None)))
@app.route('/feed/<languages>/', defaults=dict(languages=None)))
@app.route('/feed/<languages>/<lane_name>')
def feed(languages, lane_name):
    return app.manager.opds_feeds.feed(languages, lane_name)

@app.route('/search', defaults=dict(lane_name=None, languages=None))
@app.route('/search/', defaults=dict(lane_name=None, languages=None))
@app.route('/search/<languages>', defaults=dict(lane_name=None)))
@app.route('/search/<languages>/', defaults=dict(languages=None)))
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
    annotator = CirculationManagerAnnotator(Conf.circulation, None)
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
