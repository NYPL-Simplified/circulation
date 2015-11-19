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
    languages_for_request
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

    # When constructing URLs, this dictionary says which value for
    # 'order' to use, given a WorkFeed ordered by the given database
    # field.
    #
    # Once the database is intialized, MaterializedWork and
    # MaterializedWorkWithGenre will add to this dictionary.
    database_field_to_order_facet = {
        Edition.sort_title : "title",
        Edition.title : "title",
        Edition.sort_author : "author",
        Edition.author : "author",
    }

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
            cls.urn_lookup_controller = URNLookupController(cls.db)
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
            cls.urn_lookup_controller = URNLookupController(cls.db)
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

        # Now that the database is initialized, we can import the
        # classes based on materialized views and work with them.
        # 
        from core.model import (
            MaterializedWork,
            MaterializedWorkWithGenre,
        )
        df = Conf.database_field_to_order_facet
        df[MaterializedWork.sort_title] = "title"
        df[MaterializedWorkWithGenre.sort_title] = "title"
        df[MaterializedWork.sort_author] = "author"
        df[MaterializedWorkWithGenre.sort_author] = "author"

    @classmethod
    def languages_for_request(cls):
        languages = languages_for_request()

        # We're going to end up with one single language here,
        # unless Configuration.force_language intervenes.

        # By default prefer our primary collection languages.
        use_languages = [
            x for x in languages
            if x in cls.primary_collection_languages
        ]

        if not use_languages:
            # Fallback to one of our other language collections.
            use_languages = [
                x for x in languages
                if x in cls.other_collection_languages
            ]

        # Fallback to the originally specified languages.
        if not use_languages:
            use_languages = languages

        # Absolute final fallback is the list of primary collection
        # languages.
        if not use_languages:
            use_languages = cls.primary_collection_languages

        # For the time being we only accept one language (unless
        # force_language intervenes). 
        if use_languages:
            languages = [use_languages[0]]
        return Configuration.force_language(languages)


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

class CirculationManager(Flask):
    pass

    
app = CirculationManager(__name__)
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

REMOTE_INTEGRATION_FAILED = "http://librarysimplified.org/terms/problem/remote-integration-failed"
CANNOT_GENERATE_FEED_PROBLEM = "http://librarysimplified.org/terms/problem/cannot-generate-feed"
INVALID_CREDENTIALS_PROBLEM = "http://librarysimplified.org/terms/problem/credentials-invalid"
INVALID_CREDENTIALS_TITLE = "A valid library card barcode number and PIN are required."
EXPIRED_CREDENTIALS_PROBLEM = "http://librarysimplified.org/terms/problem/credentials-expired"
EXPIRED_CREDENTIALS_TITLE = "Your library card has expired. You need to renew it."
NO_LICENSES_PROBLEM = "http://librarysimplified.org/terms/problem/no-licenses"
NO_AVAILABLE_LICENSE_PROBLEM = "http://librarysimplified.org/terms/problem/no-available-license"
NO_ACCEPTABLE_FORMAT_PROBLEM = "http://librarysimplified.org/terms/problem/no-acceptable-format"
ALREADY_CHECKED_OUT_PROBLEM = "http://librarysimplified.org/terms/problem/loan-already-exists"
LOAN_LIMIT_REACHED_PROBLEM = "http://librarysimplified.org/terms/problem/loan-limit-reached"
CHECKOUT_FAILED = "http://librarysimplified.org/terms/problem/cannot-issue-loan"
HOLD_FAILED_PROBLEM = "http://librarysimplified.org/terms/problem/cannot-place-hold"
RENEW_FAILED_PROBLEM = "http://librarysimplified.org/terms/problem/cannot-renew-loan"
NO_ACTIVE_LOAN_PROBLEM = "http://librarysimplified.org/terms/problem/no-active-loan"
NO_ACTIVE_HOLD_PROBLEM = "http://librarysimplified.org/terms/problem/no-active-hold"
NO_ACTIVE_LOAN_OR_HOLD_PROBLEM = "http://librarysimplified.org/terms/problem/no-active-loan"
COULD_NOT_MIRROR_TO_REMOTE = "http://librarysimplified.org/terms/problem/cannot-mirror-to-remote"
NO_SUCH_LANE_PROBLEM = "http://librarysimplified.org/terms/problem/unknown-lane"
FORBIDDEN_BY_POLICY_PROBLEM = "http://librarysimplified.org/terms/problem/forbidden-by-policy"
CANNOT_FULFILL_PROBLEM = "http://librarysimplified.org/terms/problem/cannot-fulfill-loan"
BAD_DELIVERY_MECHANISM_PROBLEM = "http://librarysimplified.org/terms/problem/bad-delivery-mechanism"
CANNOT_RELEASE_HOLD_PROBLEM = "http://librarysimplified.org/terms/problem/cannot-release-hold"


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

def lane_url(cls, lane, order=None):
    return cdn_url_for('feed', lane_name=lane.name, order=order, _external=True)

index_controller = IndexController(Conf)
@app.route('/')
def index():
    return index_controller()

@app.route('/heartbeat')
def hearbeat():
    return HeartbeatController().heartbeat()

@app.route('/service_status')
def service_status():
    return ServiceStatusController(Conf)()

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

@app.route('/loans/<data_source>/<identifier>/revoke', methods=['GET', 'PUT'])
@requires_auth
def revoke_loan_or_hold(data_source, identifier):
    return loan_controller.revoke(data_source, identifier)

@app.route('/loans/<data_source>/<identifier>', methods=['GET', 'DELETE'])
@requires_auth
def loan_or_hold_detail(data_source, identifier):
    return loan_controller.detail(data_source, identifier)

def feed_url(lane, order_facet, offset, size, cdn=True):
    if not lane:
        lane_name = lane
    else:
        lane_name = lane.url_name
    if not isinstance(order_facet, basestring):
        order_facet = Conf.database_field_to_order_facet[order_facet]
    if cdn:
        m = cdn_url_for
    else:
        m = url_for
    return m('feed', lane_name=lane_name, order=order_facet,
             after=offset, size=size, _external=True)

@app.route('/works/')
def work():
    annotator = CirculationManagerAnnotator(Conf.circulation, None)
    return URNLookupController(Conf.db).work_lookup(annotator, 'work')
    # Conf.urn_lookup_controller.permalink(urn, annotator)

@app.route('/works/<data_source>/<identifier>')
def permalink(data_source, identifier):
    """Serve an entry for a single book.

    This does not include any loan or hold-specific information for
    the authenticated patron.

    This is different from the /works lookup protocol, in that it
    returns a single entry while the /works lookup protocol returns a
    feed containing any number of entries.
    """
    pool = _load_licensepool(data_source, identifier)
    work = pool.work
    annotator = CirculationManagerAnnotator(Conf.circulation, None)
    return entry_response(
        AcquisitionFeed.single_entry(Conf.db, work, annotator)
    )

@app.route('/works/<data_source>/<identifier>/fulfill/')
@app.route('/works/<data_source>/<identifier>/fulfill/<mechanism_id>')
@requires_auth
def fulfill(data_source, identifier, mechanism_id=None):
    """Fulfill a book that has already been checked out.

    If successful, this will serve the patron a downloadable copy of
    the book, or a DRM license file which can be used to get the
    book). Alternatively, it may serve an HTTP redirect that sends the
    patron to a copy of the book or a license file.
    """
    patron = flask.request.patron
    header = flask.request.authorization
    pin = header.password

    # Turn source + identifier into a LicensePool
    pool = _load_licensepool(data_source, identifier)
    if isinstance(pool, Response):
        return pool

    # Find the LicensePoolDeliveryMechanism they asked for.
    mechanism = None
    if mechanism_id:
        mechanism = _load_licensepooldelivery(pool, mechanism_id)
        if isinstance(mechanism, Response):
            return mechanism

    if not mechanism:
        # See if the loan already has a mechanism set. We can use that.
        loan = get_one(Conf.db, Loan, patron=patron, license_pool=pool)
        if loan and loan.fulfillment:
            mechanism =  loan.fulfillment
        else:
            return problem(
                BAD_DELIVERY_MECHANISM_PROBLEM,
                "You must specify a delivery mechanism to fulfill this loan.",
                400
            )

    try:
        fulfillment = Conf.circulation.fulfill(patron, pin, pool, mechanism)
    except NoActiveLoan, e:
        return problem(
            NO_ACTIVE_LOAN_PROBLEM, 
            "Can't fulfill request because you have no active loan for this work.",
            e.status_code)
    except CannotFulfill, e:
        return problem(CANNOT_FULFILL_PROBLEM, str(e), e.status_code)
    except DeliveryMechanismError, e:
        return problem(BAD_DELIVERY_MECHANISM_PROBLEM, str(e), e.status_code)

    headers = dict()
    if fulfillment.content_link:
        status_code = 302
        headers["Location"] = fulfillment.content_link
    else:
        status_code = 200
    if fulfillment.content_type:
        headers['Content-Type'] = fulfillment.content_type
    return Response(fulfillment.content, status_code, headers)


@app.route('/works/<data_source>/<identifier>/borrow', methods=['GET', 'PUT'])
@app.route('/works/<data_source>/<identifier>/borrow/<mechanism_id>', 
           methods=['GET', 'PUT'])
@requires_auth
def borrow(data_source, identifier, mechanism_id=None):
    """Create a new loan or hold for a book.

    Return an OPDS Acquisition feed that includes a link of rel
    "http://opds-spec.org/acquisition", which can be used to fetch the
    book or the license file.
    """
    headers = { "Content-Type" : OPDSFeed.ACQUISITION_FEED_TYPE }

    # Turn source + identifier into a LicensePool
    pool = _load_licensepool(data_source, identifier)
    if isinstance(pool, Response):
        # Something went wrong.
        return pool

    # Find the delivery mechanism they asked for, if any.
    mechanism = None
    if mechanism_id:
        mechanism = _load_licensepooldelivery(pool, mechanism_id)
        if isinstance(mechanism, Response):
            return mechanism

    if not pool:
        # I've never heard of this book.
        return problem(
            NO_LICENSES_PROBLEM, 
            "I don't have any licenses for that work.", 404)

    patron = flask.request.patron
    problem_doc = _apply_borrowing_policy(patron, pool)
    if problem_doc:
        # As a matter of policy, the patron is not allowed to check
        # this book out.
        return problem_doc

    pin = flask.request.authorization.password
    problem_doc = None
    try:
        loan, hold, is_new = Conf.circulation.borrow(
            patron, pin, pool, mechanism, Conf.hold_notification_email_address)
    except NoOpenAccessDownload, e:
        problem_doc = problem(
            NO_LICENSES_PROBLEM,
            "Sorry, couldn't find an open-access download link.", 404)
    except PatronAuthorizationFailedException, e:
        problem_doc = problem(
            INVALID_CREDENTIALS_PROBLEM, INVALID_CREDENTIALS_TITLE, 401)
    except PatronLoanLimitReached, e:
        problem_doc = problem(LOAN_LIMIT_REACHED_PROBLEM, str(e), 403)
    except DeliveryMechanismError, e:
        return problem(BAD_DELIVERY_MECHANISM_PROBLEM, str(e), e.status_code)
    except CannotLoan, e:
        problem_doc = problem(CHECKOUT_FAILED, str(e), 400)
    except CannotHold, e:
        problem_doc = problem(HOLD_FAILED_PROBLEM, str(e), 400)
    except CannotRenew, e:
        problem_doc = problem(RENEW_FAILED_PROBLEM, str(e), 400)
    except CirculationException, e:
        # Generic circulation error.
        problem_doc = problem(CHECKOUT_FAILED, str(e), 400)

    if problem_doc:
        return problem_doc

    # At this point we have either a loan or a hold. If a loan, serve
    # a feed that tells the patron how to fulfill the loan. If a hold,
    # serve a feed that talks about the hold.
    if loan:
        feed = CirculationManagerLoanAndHoldAnnotator.single_loan_feed(
            Conf.circulation, loan)
    elif hold:
        feed = CirculationManagerLoanAndHoldAnnotator.single_hold_feed(
            Conf.circulation, hold)
    else:
        # This should never happen -- we should have sent a more specific
        # error earlier.
        return problem(HOLD_FAILED_PROBLEM, "", 400)
    add_configuration_links(feed)
    if isinstance(feed, OPDSFeed):
        content = unicode(feed)
    else:
        content = etree.tostring(feed)
    if is_new:
        status_code = 201
    else:
        status_code = 200
    return Response(content, status_code, headers)

@app.route('/works/<data_source>/<identifier>/report', methods=['GET', 'POST'])
def report(data_source, identifier):
    """Report a problem with a book."""

    # Turn source + identifier into a LicensePool
    pool = _load_licensepool(data_source, identifier)
    if isinstance(pool, Response):
        # Something went wrong.
        return pool

    if flask.request.method == 'GET':
        # Return a list of valid URIs to use as the type of a problem detail
        # document.
        data = "\n".join(Complaint.VALID_TYPES)
        return Response(data, 200, {"Content-Type" : "text/uri-list"})

    data = flask.request.data
    controller = ComplaintController()
    return controller.register(pool, data)

# Loadstorm verification
@app.route('/loadstorm-<code>.html')
def loadstorm_verify(code):
    c = Configuration.integration("Loadstorm", required=True)
    if code == c['verification_code']:
        return Response("", 200)
    else:
        return Response("", 404)

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
