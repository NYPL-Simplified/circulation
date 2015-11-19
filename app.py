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
    load_lending_policy,
    cdn_url_for,
    entry_response,
    feed_response,
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
    Complaint,
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
from core.opensearch import OpenSearchDocument
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

feed_cache = dict()

class Conf:
    log = logging.getLogger("Circulation web app")

    db = None
    sublanes = None
    name = None
    display_name = None
    url_name = None
    parent = None
    urn_lookup_controller = None
    overdrive = None
    threem = None
    auth = None
    search = None
    policy = None

    configuration = None

    @classmethod
    def initialize(cls, _db=None, lanes=None):
        def log_lanes(lanelist, level=0):
            for lane in lanelist.lanes:
                cls.log.debug("%s%s", "-" * level, lane.name)
                log_lanes(lane.sublanes, level+1)

        try:
            cls.config = Configuration.load()
        except CannotLoadConfiguration, e:
            cls.log.error("Could not load configuration file: %s" % e)
            sys.exit()
        _db = _db or cls.db
        lane_list = Configuration.policy(Configuration.LANES_POLICY)

        if cls.testing:
            if not lanes:
                lanes = make_lanes(_db, lane_list)
            cls.db = _db
            cls.sublanes = lanes
            cls.overdrive = DummyOverdriveAPI(cls.db)
            cls.threem = DummyThreeMAPI(cls.db)
            cls.axis = None
            cls.auth = Authenticator.initialize(cls.db, test=True)
            cls.search = DummyExternalSearchIndex()
            cls.policy = {}
            cls.hold_notification_email_address = 'test@test'
        else:
            if cls.db is None:
                _db = production_session()
                cls.db = _db
            lanes = make_lanes(cls.db, lane_list)
            cls.sublanes = lanes
            cls.overdrive = OverdriveAPI.from_environment(cls.db)
            cls.threem = ThreeMAPI.from_environment(cls.db)
            cls.axis = Axis360API.from_environment(cls.db)
            cls.auth = Authenticator.initialize(cls.db, test=False)
            cls.policy = load_lending_policy(
                Configuration.policy('lending', {})
            )

            if Configuration.integration(
                    Configuration.ELASTICSEARCH_INTEGRATION):
                cls.search = ExternalSearchIndex()
            else:
                cls.log.warn("No external search server configured.")
                cls.search = None
        cls.urn_lookup_controller = URNLookupController(cls.db)
        cls.log.debug("Lane layout:")
        log_lanes(lanes)

        language_policy = Configuration.language_policy()

        cls.primary_collection_languages = language_policy.get(
            Configuration.LARGE_COLLECTION_LANGUAGES, ['eng']
        )
        cls.other_collection_languages = language_policy.get(
            Configuration.SMALL_COLLECTION_LANGUAGES, []
        )

        cls.hold_notification_email_address = Configuration.default_notification_email_address()

        cls.circulation = CirculationAPI(
            _db=cls.db, threem=cls.threem, overdrive=cls.overdrive,
            axis=cls.axis)
        cls.log = logging.getLogger("Circulation web app")

        adobe = Configuration.integration(
            Configuration.ADOBE_VENDOR_ID_INTEGRATION
        )
        vendor_id = adobe.get(Configuration.ADOBE_VENDOR_ID)
        node_value = adobe.get(Configuration.ADOBE_VENDOR_ID_NODE_VALUE)
        if vendor_id and node_value:
            cls.adobe_vendor_id = AdobeVendorIDController(
                cls.db,
                vendor_id,
                node_value,
                cls.auth
            )
        else:
            cls.log.warn("Adobe Vendor ID controller is disabled due to missing or incomplete configuration.")
            cls.adobe_vendor_id = None

        cls.make_authentication_document()

    @classmethod
    def make_authentication_document(cls):
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

        cls.opds_authentication_document = json.dumps(doc)

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

h = ErrorHandler(Conf, app.config['DEBUG'])
@app.errorhandler(Exception)
def exception_handler(exception):
    return h.handle(exception)

@app.teardown_request
def shutdown_session(exception):
    if Conf.db:
        if exception:
            Conf.db.rollback()
        else:
            Conf.db.commit()

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

index_controller = IndexController(Conf)
@app.route('/')
def index():
    return index_controller()

opds_feeds = OPDSFeedController(Conf)
@app.route('/groups', defaults=dict(lane_name=None, languages=None))
@app.route('/groups/', defaults=dict(lane_name=None, languages=None))
@app.route('/groups/<languages>', defaults=dict(lane_name=None)))
@app.route('/groups/<languages>/', defaults=dict(languages=None)))
@app.route('/groups/<languages>/<lane_name>')
def acquisition_groups(languages, lane_name):
    return opds_feed.groups(languages, lane_name)

@app.route('/feed', defaults=dict(lane_name=None, languages=None))
@app.route('/feed/', defaults=dict(lane_name=None, languages=None))
@app.route('/feed/<languages>', defaults=dict(lane_name=None)))
@app.route('/feed/<languages>/', defaults=dict(languages=None)))
@app.route('/feed/<languages>/<lane_name>')
def feed(languages, lane_name):
    return opds_feed.feed(languages, lane_name)

@app.route('/search', defaults=dict(lane_name=None, languages=None))
@app.route('/search/', defaults=dict(lane_name=None, languages=None))
@app.route('/search/<languages>', defaults=dict(lane_name=None)))
@app.route('/search/<languages>/', defaults=dict(languages=None)))
@app.route('/search/<languages>/<lane_name>')
def lane_search(languages, lane_name):
    return opds_feed.search(languages, lane_name)

loan_controller = LoanController(setup)
@app.route('/loans/', methods=['GET', 'HEAD'])
@requires_auth
def active_loans():
    return loan_controller.sync()

@app.route('/works/<data_source>/<identifier>/borrow', methods=['GET', 'PUT'])
@app.route('/works/<data_source>/<identifier>/borrow/<mechanism_id>', 
           methods=['GET', 'PUT'])
@requires_auth
def borrow(data_source, identifier, mechanism_id=None):
    return loan_controller.borrow(data_source, identifier, mechanism_id)

@app.route('/works/<data_source>/<identifier>/fulfill/')
@app.route('/works/<data_source>/<identifier>/fulfill/<mechanism_id>')
@requires_auth
def fulfill(data_source, identifier, mechanism_id=None):
    return loan_controller.fulfill(data_source, identifier, mechanism_id)

@app.route('/loans/<data_source>/<identifier>/revoke', methods=['GET', 'PUT'])
@requires_auth
def revoke_loan_or_hold(data_source, identifier):
    return loan_controller.revoke(data_source, identifier)

@app.route('/loans/<data_source>/<identifier>', methods=['GET', 'DELETE'])
@requires_auth
def loan_or_hold_detail(data_source, identifier):
    return loan_controller.detail(data_source, identifier)

@app.route('/works/')
def work():
    annotator = CirculationManagerAnnotator(Conf.circulation, None)
    return URNLookupController(Conf.db).work_lookup(annotator, 'work')
    # Conf.urn_lookup_controller.permalink(urn, annotator)

works_controller = WorksController(Conf)
@app.route('/works/<data_source>/<identifier>')
def permalink(data_source, identifier):
    return works_controller.permalink(data_source, identifier)
    
@app.route('/works/<data_source>/<identifier>/report', methods=['GET', 'POST'])
def report(data_source, identifier):
    return works_controller.report(data_source, identifier)

# Adobe Vendor ID implementation
@app.route('/AdobeAuth/authdata')
@requires_auth
def adobe_vendor_id_get_token():
    return Conf.adobe_vendor_id.create_authdata_handler(flask.request.patron)

@app.route('/AdobeAuth/SignIn', methods=['POST'])
def adobe_vendor_id_signin():
    return Conf.adobe_vendor_id.signin_handler()

@app.route('/AdobeAuth/AccountInfo', methods=['POST'])
def adobe_vendor_id_accountinfo():
    return Conf.adobe_vendor_id.userinfo_handler()

@app.route('/AdobeAuth/Status')
def adobe_vendor_id_status():
    return Conf.adobe_vendor_id.status_handler()

# Controllers used for operations purposes
@app.route('/heartbeat')
def hearbeat():
    return HeartbeatController().heartbeat()

@app.route('/service_status')
def service_status():
    return ServiceStatusController(Conf)()

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
    Conf.log.info("Starting app on %s:%s", host, port)
    app.run(debug=debug, host=host, port=port)
