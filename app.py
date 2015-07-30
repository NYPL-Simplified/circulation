from functools import wraps
from nose.tools import set_trace
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

from core.external_search import (
    ExternalSearchIndex,
    DummyExternalSearchIndex,
)
from circulation import CirculationAPI
from circulation_exceptions import *
from core.app_server import (
    load_lending_policy,
    cdn_url_for,
    feed_response,
    HeartbeatController,
    URNLookupController,
    ErrorHandler,
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
    CustomListFeed,
    DataSource,
    production_session,
    Hold,
    LaneList,
    Lane,
    LicensePool,
    Loan,
    Patron,
    Identifier,
    Representation,
    Work,
    LaneFeed,
    CustomListFeed,
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
    NavigationFeed,
    OPDSFeed,
)
import urllib
from core.util.flask_util import (
    problem,
    problem_raw,
    languages_for_request
)
from core.util.opds_authentication_document import OPDSAuthenticationDocument
from millenium_patron import (
    DummyMilleniumPatronAPI,
    MilleniumPatronAPI,
)
from lanes import make_lanes

feed_cache = dict()

class Conf:
    db = None
    sublanes = None
    name = None
    display_name = None
    parent = None
    urn_lookup_controller = None
    overdrive = None
    threem = None
    auth = None
    search = None
    policy = None
    primary_collection_languages = json.loads(
        os.environ['PRIMARY_COLLECTION_LANGUAGES'])
    hold_notification_email_address = os.environ.get(
        'DEFAULT_NOTIFICATION_EMAIL_ADDRESS')

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
        if cls.testing:
            if not lanes:
                lanes = make_lanes(_db)
            cls.db = _db
            cls.sublanes = lanes
            cls.urn_lookup_controller = URNLookupController(cls.db)
            cls.overdrive = DummyOverdriveAPI(cls.db)
            cls.threem = DummyThreeMAPI(cls.db)
            cls.axis = None
            cls.auth = DummyMilleniumPatronAPI()
            cls.search = DummyExternalSearchIndex()
            cls.policy = {}
        else:
            _db = production_session()
            lanes = make_lanes(_db)
            #for lane in lanes.lanes:
            #    print lane.name
            #    for sublane in lane.sublanes:
            #        print "", sublane.display_name
            cls.db = _db
            cls.sublanes = lanes
            cls.urn_lookup_controller = URNLookupController(cls.db)
            cls.overdrive = OverdriveAPI(cls.db)
            cls.threem = ThreeMAPI(cls.db)
            cls.axis = Axis360API(cls.db)
            cls.auth = MilleniumPatronAPI()
            cls.search = ExternalSearchIndex()
            cls.policy = load_lending_policy()

        cls.circulation = CirculationAPI(
            _db=cls.db, threem=cls.threem, overdrive=cls.overdrive,
            axis=cls.axis)
        cls.log = logging.getLogger("Circulation web app")
        vendor_id = os.environ.get('ADOBE_VENDOR_ID')
        node_value = os.environ.get('ADOBE_VENDOR_ID_NODE_VALUE')
        if vendor_id and node_value:
            cls.adobe_vendor_id = AdobeVendorIDController(
                cls.db,
                vendor_id,
                node_value,
                cls.auth
            )
        else:
            Conf.log.warn("Adobe Vendor ID controller is disabled due to absence of ADOBE_VENDOR_ID or ADOBE_VENDOR_ID_NODE_VALUE environment variables.")
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
    def make_authentication_document(cls):
        base_opds_document = os.environ.get(
            'OPDS_AUTHENTICATION_DOCUMENT')
        if base_opds_document:
            base_opds_document = json.loads(base_opds_document)
        else:
            base_opds_document = {}

        auth_type = [OPDSAuthenticationDocument.BASIC_AUTH_FLOW]
        content_server_url = os.environ['CIRCULATION_WEB_APP_URL']
        scheme, netloc, path, parameters, query, fragment = (
            urlparse.urlparse(content_server_url))
        opds_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, netloc))

        doc = OPDSAuthenticationDocument.fill_in(
            base_opds_document, auth_type, "Library", opds_id, None, "Barcode",
            "PIN",
            )

        cls.opds_authentication_document = json.dumps(doc)

if os.environ.get('TESTING') == "True":
    Conf.testing = True
    # It's the test's responsibility to call initialize()
else:
    Conf.testing = False
    Conf.initialize()

app = Flask(__name__)
app.config['DEBUG'] = True
app.debug = True

h = ErrorHandler(Conf, app.config['DEBUG'])
@app.errorhandler(Exception)
def exception_handler(exception):
    return h.handle(exception)

CANNOT_GENERATE_FEED_PROBLEM = "http://librarysimplified.org/terms/problem/cannot-generate-feed"
INVALID_CREDENTIALS_PROBLEM = "http://librarysimplified.org/terms/problem/credentials-invalid"
INVALID_CREDENTIALS_TITLE = "A valid library card barcode number and PIN are required."
EXPIRED_CREDENTIALS_PROBLEM = "http://librarysimplified.org/terms/problem/credentials-expired"
EXPIRED_CREDENTIALS_TITLE = "Your library card has expired. You need to renew it."
NO_LICENSES_PROBLEM = "http://librarysimplified.org/terms/problem/no-licenses"
NO_AVAILABLE_LICENSE_PROBLEM = "http://librarysimplified.org/terms/problem/no-available-license"
NO_ACCEPTABLE_FORMAT_PROBLEM = "http://librarysimplified.org/terms/problem/no-acceptable-format"
ALREADY_CHECKED_OUT_PROBLEM = "http://librarysimplified.org/terms/problem/loan-already-exists"
CHECKOUT_FAILED = "http://librarysimplified.org/terms/problem/could-not-issue-loan"
HOLD_FAILED_PROBLEM = "http://librarysimplified.org/terms/problem/could-not-place-hold"
NO_ACTIVE_LOAN_PROBLEM = "http://librarysimplified.org/terms/problem/no-active-loan"
NO_ACTIVE_HOLD_PROBLEM = "http://librarysimplified.org/terms/problem/no-active-hold"
NO_ACTIVE_LOAN_OR_HOLD_PROBLEM = "http://librarysimplified.org/terms/problem/no-active-loan"
COULD_NOT_MIRROR_TO_REMOTE = "http://librarysimplified.org/terms/problem/could-not-mirror-to-remote"
NO_SUCH_LANE_PROBLEM = "http://librarysimplified.org/terms/problem/unknown-lane"
FORBIDDEN_BY_POLICY_PROBLEM = "http://librarysimplified.org/terms/problem/forbidden-by-policy"
CANNOT_FULFILL_PROBLEM = "http://librarysimplified.org/terms/problem/cannot-fulfill-loan"
CANNOT_RELEASE_HOLD_PROBLEM = "http://librarysimplified.org/terms/problem/cannot-release-hold"

def authenticated_patron(barcode, pin):
    """Look up the patron authenticated by the given barcode/pin.

    If there's a problem, return a 2-tuple (URI, title) for use in a
    Problem Detail Document.

    If there's no problem, return a Patron object.
    """
    patron = Conf.auth.authenticated_patron(Conf.db, barcode, pin)
    if not patron:
        return (INVALID_CREDENTIALS_PROBLEM,
                INVALID_CREDENTIALS_TITLE)

    # Okay, we know who they are and their PIN is valid. But maybe the
    # account has expired?
    if not patron.authorization_is_active:
        return (EXPIRED_CREDENTIALS_PROBLEM,
                EXPIRED_CREDENTIALS_TITLE)

    # No, apparently we're fine.
    return patron


def authenticate(uri, title):
    """Sends a 401 response that demands basic auth."""
    data = Conf.opds_authentication_document
    headers= { 'WWW-Authenticate' : 'Basic realm="Library card"',
               'Content-Type' : OPDSAuthenticationDocument.MEDIA_TYPE }
    return Response(data, 401, headers)

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        header = flask.request.authorization
        if not header:
            # No credentials were provided.
            return authenticate(
                INVALID_CREDENTIALS_PROBLEM,
                INVALID_CREDENTIALS_TITLE)

        patron = authenticated_patron(header.username, header.password)
        if isinstance(patron, tuple):
            flask.request.patron = None
            return authenticate(*patron)
        else:
            flask.request.patron = patron
        return f(*args, **kwargs)
    return decorated

def featured_feed_cache_url(annotator, lane, languages):
    url = annotator.featured_feed_url(lane, cdn=False)
    if '?' in url:
        url += '&'
    else:
        url += '?'
    if isinstance(languages, basestring):
        languages = [languages]
    return url + "languages=%s" % ",".join(languages)

def make_featured_feed(annotator, lane, languages):
    search_link = dict(
        rel="search",
        type="application/opensearchdescription+xml",
        href=url_for('lane_search', lane=lane.name,
                         _external=True))
    opds_feed = AcquisitionFeed.featured(
        languages, lane, annotator, quality_cutoff=0.0)
    opds_feed.add_link(**search_link)
    return 200, {"content-type": OPDSFeed.ACQUISITION_FEED_TYPE}, unicode(opds_feed)

def acquisition_groups_cache_url(annotator, lane, languages):
    if not lane:
        lane_name = lane
    else:
        lane_name = lane.name
    url = url_for('acquisition_groups', lane_name=lane_name, _external=True)
    if '?' in url:
        url += '&'
    else:
        url += '?'
    if isinstance(languages, basestring):
        languages = [languages]
    return url + "languages=%s" % ",".join(languages)

def make_acquisition_groups(annotator, lane, languages):
    if not lane:
        lane_name = lane
    else:
        lane_name = lane.name
    url = cdn_url_for("acquisition_groups", lane=lane_name, _external=True)
    best_sellers_url = cdn_url_for("popular_feed", lane=lane_name, _external=True)
    staff_picks_url = cdn_url_for("staff_picks_feed", lane=lane_name, _external=True)
    feed = AcquisitionFeed.featured_groups(
        url, best_sellers_url, staff_picks_url, languages, lane, annotator)
    feed.add_link(
        rel="search", 
        type="application/opensearchdescription+xml",
        href=url_for('lane_search', lane=lane_name, _external=True))
    feed.add_link(
        rel="http://opds-spec.org/shelf", 
        href=url_for('active_loans', _external=True))
    return (200,
            {"content-type": OPDSFeed.ACQUISITION_FEED_TYPE}, 
            unicode(feed)
    )

def popular_feed_cache_url(annotator, lane_name, languages, order_facet,
                           offset, size):
    if isinstance(lane_name, Lane):
        lane_name = lane_name.name

    url = url_for('popular_feed', lane_name=lane_name, order=order_facet,
                  after=offset, size=size,_external=True)
    if '?' in url:
        url += '&'
    else:
        url += '?'
    if isinstance(languages, basestring):
        languages = [languages]
    return url + "languages=%s" % ",".join(languages)

def make_popular_feed(_db, annotator, lane, languages):

    # Do some preliminary data checking to avoid generating expensive
    # feeds that contain nothing.
    if lane and lane.parent:
        # We only show a best-seller list for the top-level lanes.
        return problem(None, "No such feed", 404)

    if 'eng' not in languages:
        # We only have information about English best-sellers.
        return problem(None, "No such feed", 404)

    arg = flask.request.args.get
    order_facet = arg('order', 'title')

    size = arg('size', '50')
    try:
        size = int(size)
    except ValueError:
        return problem(None, "Invalid size: %s" % size, 400)
    size = min(size, 100)

    offset = arg('after', None)
    if offset:
        try:
            offset = int(offset)
        except ValueError:
            return problem(None, "Invalid offset: %s" % offset, 400)

    if not lane:
        lane_name = lane
        lane_display_name = lane
    else:
        lane_name = lane.name
        lane_display_name = lane.display_name

    if lane_display_name:
        title = "%s: Best Sellers" % lane_display_name
    else:
        title = "Best Sellers"
        lane = None

    as_of = (datetime.datetime.utcnow() - CustomListFeed.best_seller_cutoff)
    nyt = DataSource.lookup(_db, DataSource.NYT)
    work_feed = CustomListFeed(
        lane, nyt, languages, as_of, availability=CustomListFeed.ALL,
        order_facet=order_facet)
    page = work_feed.page_query(_db, offset, size).all()
    this_url = cdn_url_for('popular_feed', lane_name=lane_name, _external=True)
    opds_feed = AcquisitionFeed(_db, title, this_url, page,
                                annotator, work_feed.active_facet)

    # Add a 'next' link unless this page is empty.
    if len(page) == 0:
        offset = None
    else:
        offset = offset or 0
        offset += size
        next_url = cdn_url_for(
            'popular_feed', lane_name=lane_name, order=order_facet,
            after=offset, size=size, _external=True)
        opds_feed.add_link(rel="next", href=next_url)
    return (200,
            {"content-type": OPDSFeed.ACQUISITION_FEED_TYPE}, 
            unicode(opds_feed)
    )

def staff_picks_feed_cache_url(annotator, lane, languages):
    if not lane:
        lane_name = lane
    else:
        lane_name = lane.name
    url = url_for('staff_picks_feed', lane_name=lane_name, _external=True)
    if '?' in url:
        url += '&'
    else:
        url += '?'
    if isinstance(languages, basestring):
        languages = [languages]
    return url + "languages=%s" % ",".join(languages)

def make_staff_picks_feed(_db, annotator, lane, languages):
    # Do some preliminary data checking to avoid generating expensive
    # feeds that contain nothing.
    if lane and lane.parent:
        # We only show a best-seller list for the top-level lanes.
        return problem(None, "No such feed", 404)

    if 'eng' not in languages:
        # We only have information about English best-sellers.
        return problem(None, "No such feed", 404)

    if not lane:
        lane_name = lane
        lane_display_name = lane
    else:
        lane_name = lane.name
        lane_display_name = lane.display_name

    if lane_display_name:
        title = "%s: Staff Picks" % lane_display_name
    else:
        title = "Staff Picks"
        lane = None

    staff = DataSource.lookup(_db, DataSource.LIBRARY_STAFF)
    work_feed = CustomListFeed(
        lane, staff, languages, availability=CustomListFeed.ALL)
    page = work_feed.page_query(_db, None, None).all()

    this_url = cdn_url_for('staff_picks_feed', lane_name=lane_name, _external=True)
    opds_feed = AcquisitionFeed(_db, title, this_url, page,
                                annotator, work_feed.active_facet)
    return (200,
            {"content-type": OPDSFeed.ACQUISITION_FEED_TYPE}, 
            unicode(opds_feed)
    )


@app.route('/')
def index():    
    return redirect(cdn_url_for('acquisition_groups'))

@app.route('/heartbeat')
def hearbeat():
    return HeartbeatController().heartbeat()

ping_controller_path = os.environ.get('PING_CONTROLLER_PATH')
if ping_controller_path:
    Conf.log.info("Setting up ping controller at %s" % ping_controller_path)
    @app.route(ping_controller_path)
    def ping():
        return HeartbeatController().heartbeat()


@app.route('/service_status')
def service_status():
    barcode = os.environ['TEST_CREDENTIAL_USERNAME']
    pin = os.environ['TEST_CREDENTIAL_PASSWORD']

    template = """<!DOCTYPE HTML>
<html lang="en" class="">
<head>
<meta charset="utf8">
</head>
<body>
<ul>
%(statuses)s
</ul>
</body>
</html>
"""
    timings = dict()

    patrons = []
    def _add_timing(k, x):
        try:
            a = time.time()
            x()
            b = time.time()
            result = b-a
        except Exception, e:
            result = e
        if isinstance(result, float):
            timing = "SUCCESS: %.2fsec" % result
        else:
            timing = "FAILURE: %s" % result
        timings[k] = timing

    do_patron = lambda : patrons.append(Conf.auth.authenticated_patron(Conf.db, barcode, pin))
    _add_timing('Patron authentication', do_patron)

    patron = patrons[0]
    do_overdrive = lambda : Conf.overdrive.get_patron_checkouts(patron, pin)
    _add_timing('Overdrive patron account', do_overdrive)

    do_threem = lambda : Conf.threem.get_patron_checkouts(patron)
    _add_timing('3M patron account', do_threem)

    statuses = []
    for k, v in sorted(timings.items()):
        statuses.append(" <li><b>%s</b>: %s</li>" % (k, v))

    doc = template % dict(statuses="\n".join(statuses))
    return make_response(doc, 200, {"Content-Type": "text/html"})


@app.route('/lanes', defaults=dict(lane=None))
@app.route('/lanes/', defaults=dict(lane=None))
@app.route('/lanes/<lane>')
def navigation_feed(lane):
    lane_name = lane
    if lane is None:
        lane = Conf
    else:
        if lane not in Conf.sublanes.by_name:
            return problem(NO_SUCH_LANE_PROBLEM, "No such lane: %s" % lane, 404)
        lane = Conf.sublanes.by_name[lane]

    languages = languages_for_request()
    this_url = cdn_url_for("navigation_feed", lane=lane_name, _external=True)
    key = (",".join(languages), this_url)
    # This feed will not change unless the application is upgraded,
    # so there's no need to expire the cache.
    if key in feed_cache:
        return feed_response(feed_cache[key], acquisition=False, cache_for=7200)
        
    feed = NavigationFeed.main_feed(lane, CirculationManagerAnnotator(lane))

    if not lane.parent:
        # Top-level lanes are the only ones that have best-seller
        # and staff pick lanes.
        feed.add_link(
            rel=NavigationFeed.POPULAR_REL, title="Best Sellers",
            type=NavigationFeed.ACQUISITION_FEED_TYPE,
            href=cdn_url_for('popular_feed', lane_name=lane.name, _external=True))
        feed.add_link(
            rel=NavigationFeed.RECOMMENDED_REL, title="Staff Picks",
            type=NavigationFeed.ACQUISITION_FEED_TYPE,
            href=cdn_url_for('staff_picks_feed', lane_name=lane.name, _external=True))

    feed.add_link(
        rel="search", 
        type="application/opensearchdescription+xml",
        href=url_for('lane_search', lane=None, _external=True))
    feed.add_link(
        rel="http://opds-spec.org/shelf", 
        href=url_for('active_loans', _external=True))

    feed = unicode(feed)
    feed_cache[key] = feed
    return feed_response(feed, acquisition=False, cache_for=7200)

def lane_url(cls, lane, order=None):
    return cdn_url_for('feed', lane=lane.name, order=order, _external=True)

@app.route('/groups', defaults=dict(lane=None))
@app.route('/groups/', defaults=dict(lane=None))
@app.route('/groups/<lane>')
def acquisition_groups(lane):
    lane_name = lane
    if lane is None:
        lane = Conf
    else:
        if lane not in Conf.sublanes.by_name:
            return problem(NO_SUCH_LANE_PROBLEM, "No such lane: %s" % lane, 404)
        lane = Conf.sublanes.by_name[lane]

    languages = languages_for_request()
    annotator = CirculationManagerAnnotator(lane)

    cache_url = acquisition_groups_cache_url(annotator, lane, languages)
    def get(*args, **kwargs):
        for l in languages:
            if l in Conf.primary_collection_languages:
                # Attempting to create a groups feed for a primary
                # collection language will hang the database. It also
                # should never be necessary, since that stuff is
                # supposed to be precalculated by a script. It's
                # better to just refuse to do the work.
                return problem_raw(
                    CANNOT_GENERATE_FEED_PROBLEM,
                    "Refusing to dynamically create a groups feed for a primary collection language (%s). This feed must be precalculated." % l, 400)

        return make_acquisition_groups(annotator, lane, languages)
    a = time.time()
    feed_rep, cached = Representation.get(
        Conf.db, cache_url, get, accept=OPDSFeed.ACQUISITION_FEED_TYPE,
        max_age=None)
    feed_xml = feed_rep.content
    b = time.time()
    Conf.log.info("That took %.2f, cached=%r" % (b-a, cached))
    return feed_response(feed_xml, acquisition=True)


@app.route('/loans/', methods=['GET', 'HEAD'])
@requires_auth
def active_loans():

    if flask.request.method=='HEAD':
        return Response()

    patron = flask.request.patron

    # First synchronize our local list of loans and holds with all
    # third-party loan providers.
    if patron.authorization_identifier and len(patron.authorization_identifier) >= 7:
        # TODO: Barcodes less than 7 digits are dummy code that allow
        # the creation of arbitrary test accounts that are limited to
        # public domain books. We cannot ask Overdrive or 3M about
        # these barcodes.
        header = flask.request.authorization
        try:
            Conf.circulation.sync_bookshelf(patron, header.password)
        except Exception, e:
            # If anything goes wrong, omit the sync step and just
            # display the current active loans, as we understand them.
            Conf.log.error("ERROR DURING SYNC: %r", e, exc_info=e)

    # Then make the feed.
    feed = CirculationManagerLoanAndHoldAnnotator.active_loans_for(patron)
    return feed_response(feed, cache_for=None)

@app.route('/loans/<data_source>/<identifier>/revoke', methods=['GET', 'PUT'])
@requires_auth
def revoke_loan_or_hold(data_source, identifier):
    patron = flask.request.patron
    pool = _load_licensepool(data_source, identifier)
    if isinstance(pool, Response):
        return pool
    loan = get_one(Conf.db, Loan, patron=patron, license_pool=pool)
    if loan:
        hold = None
    else:
        hold = get_one(Conf.db, Hold, patron=patron, license_pool=pool)

    if not loan and not hold:
        if not pool.work:
            title = 'this book'
        else:
            title = '"%s"' % pool.work.title
        return problem(
            NO_ACTIVE_LOAN_OR_HOLD_PROBLEM, 
            'You have no active loan or hold for %s.' % title,
            404)

    pin = flask.request.authorization.password
    if loan:
        try:
            Conf.circulation.revoke_loan(patron, pin, pool)
        except RemoteRefusedReturn, e:
            uri = COULD_NOT_MIRROR_TO_REMOTE
            title = "Loan deleted locally but remote refused. Loan is likely to show up again on next sync."
            return problem(uri, title, 500)
        except CannotReturn, e:
            title = "Loan deleted locally but remote failed: %s" % str(e)
            return problem(uri, title, 500)
    elif hold:
        try:
            Conf.circulation.release_hold(patron, pin, pool, loan)
        except CannotRelease, e:
            title = "Hold released locally but remote failed: %s" % str(e)
            return problem(CANNOT_RELEASE_HOLD_PROBLEM, e, 500)
    return ""


@app.route('/loans/<data_source>/<identifier>', methods=['GET', 'DELETE'])
@requires_auth
def loan_or_hold_detail(data_source, identifier):
    patron = flask.request.patron
    pool = _load_licensepool(data_source, identifier)
    if isinstance(pool, Response):
        return pool
    loan = get_one(Conf.db, Loan, patron=patron, license_pool=pool)
    if loan:
        hold = None
    else:
        hold = get_one(Conf.db, Hold, patron=patron, license_pool=pool)

    if not loan and not hold:
        return problem(
            NO_ACTIVE_LOAN_OR_HOLD_PROBLEM, 
            'You have no active loan or hold for "%s".' % pool.work.title,
            404)

    if flask.request.method=='GET':
        if loan:
            feed = CirculationManagerLoanAndHoldAnnotator.single_loan_feed(
                loan)
        else:
            feed = CirculationManagerLoanAndHoldAnnotator.single_hold_feed(
            hold)
        return feed_response(feed, None)

    if flask.request.method=='DELETE':
        return revoke_loan_or_hold(data_source, identifier)

def feed_url(lane, order_facet, offset, size, cdn=True):
    if not lane:
        lane_name = lane
    else:
        lane_name = lane.name
    if not isinstance(order_facet, basestring):
        order_facet = Conf.database_field_to_order_facet[order_facet]
    if cdn:
        m = cdn_url_for
    else:
        m = url_for
    return m('feed', lane=lane_name, order=order_facet,
             after=offset, size=size, _external=True)

def feed_cache_url(lane, languages, order_facet, 
                   offset, size):
    url = feed_url(lane, order_facet, offset, size, cdn=False)
    if '?' in url:
        url += '&'
    else:
        url += '?'
    if isinstance(languages, basestring):
        languages = [languages]
    return url + "languages=%s" % ",".join(languages)
    

def make_feed(_db, annotator, lane, languages, order_facet,
              offset, size):

    from core.materialized_view import (
        MaterializedWorkLaneFeed,
    )
    work_feed = MaterializedWorkLaneFeed.factory(lane, languages, order_facet)
    if order_facet == 'title':
        title = "%s: By title" % lane.name
    elif order_facet == 'author':
        title = "%s: By author" % lane.name
    else:
        title = lane.name

    a = time.time()
    query = work_feed.page_query(_db, offset, size)
    from core.model import dump_query
    Conf.log.debug(dump_query(query))
    page = query.all()
    b = time.time()
    Conf.log.info("Got %d results in %.2fsec." % (len(page), b-a))

    # Turn the set of works into an OPDS feed.
    this_url = feed_url(lane, order_facet, offset, size)
    opds_feed = AcquisitionFeed(_db, title, this_url, page,
                                annotator, work_feed.active_facet)

    # Add a 'next' link unless this page is empty.
    if len(page) == 0:
        offset = None
    else:
        offset = offset or 0
        offset += size
        next_url = feed_url(lane, order_facet, offset, size)
        opds_feed.add_link(rel="next", href=next_url)

    # Add a 'search' link.
    search_link = dict(
        rel="search",
        type="application/opensearchdescription+xml",
        href=url_for('lane_search', lane=lane.name, _external=True))
    opds_feed.add_link(**search_link)

    return (200,
            {"content-type": OPDSFeed.ACQUISITION_FEED_TYPE}, 
            unicode(opds_feed),
        )


@app.route('/feed', defaults=dict(lane=None))
@app.route('/feed/', defaults=dict(lane=None))
@app.route('/feed/<lane>')
def feed(lane):
    languages = languages_for_request()
    arg = flask.request.args.get
    order_facet = arg('order', 'recommended')
    offset = arg('after', None)

    if lane not in Conf.sublanes.by_name:
        return problem(NO_SUCH_LANE_PROBLEM, "No such lane: %s" % lane, 404)

    lane = Conf.sublanes.by_name[lane]

    key = (lane, ",".join(languages), order_facet)
    feed_xml = None
    annotator = CirculationManagerAnnotator(lane)

    feed_xml = None
    if order_facet == 'recommended':
        cache_url = featured_feed_cache_url(annotator, lane, languages)
        def get(*args, **kwargs):
            return make_featured_feed(annotator, lane, languages)
        # Recommended feeds are cached until explicitly updated by 
        # something running outside of this web app.
        max_age = None
    else:
        if not order_facet in ('title', 'author'):
            return problem(
                None,
                "I don't know how to order a feed by '%s'" % order_facet,
                400)

        size = arg('size', '50')
        try:
            size = int(size)
        except ValueError:
            return problem(None, "Invalid size: %s" % size, 400)
        size = min(size, 100)

        offset = arg('after', None)
        if offset:
            try:
                offset = int(offset)
            except ValueError:
                return problem(None, "Invalid offset: %s" % offset, 400)

        status, media_type, feed_xml = make_feed(
            Conf.db, annotator, lane, languages, order_facet,
            offset, size)
        return feed_response(feed_xml)

    #print "Getting feed."
    #a = time.time()
    feed_rep, cached = Representation.get(
        Conf.db, cache_url, get, accept=OPDSFeed.ACQUISITION_FEED_TYPE,
        max_age=max_age)
    #b = time.time()
    #print "That took %.2f, cached=%r" % (b-a, cached)

    if feed_rep.fetch_exception:
        Conf.log.error("ERROR: getting feed %s: %s", cache_url, feed_rep.fetch_exception)
    feed_xml = feed_rep.content
    return feed_response(feed_xml)

@app.route('/staff_picks', defaults=dict(lane_name=None))
@app.route('/staff_picks/', defaults=dict(lane_name=None))
@app.route('/staff_picks/<lane_name>')
def staff_picks_feed(lane_name):
    """Return an acquisition feed of staff picks in this lane."""
    if lane_name:
        lane = Conf.sublanes.by_name[lane_name]
        lane_display_name = lane.display_name
    else:
        lane = None
        lane_display_name = None
    languages = languages_for_request()

    annotator = CirculationManagerAnnotator(lane)
    cache_url = staff_picks_feed_cache_url(annotator, lane, languages)
    def get(*args, **kwargs):
        return make_staff_picks_feed(Conf.db, annotator, lane, languages)
    feed_rep, cached = Representation.get(
        Conf.db, cache_url, get, accept=OPDSFeed.ACQUISITION_FEED_TYPE,
        max_age=None)
    feed_xml = feed_rep.content
    return feed_response(feed_xml)

@app.route('/popular', defaults=dict(lane_name=None))
@app.route('/popular/', defaults=dict(lane_name=None))
@app.route('/popular/<lane_name>')
def popular_feed(lane_name):
    """Return an acquisition feed of popular books in this lane.
    
    At the moment, 'popular' == 'NYT bestseller'.
    """

    if lane_name:
        lane = Conf.sublanes.by_name[lane_name]
        lane_display_name = lane.display_name
    else:
        lane = None
        lane_display_name = None
    languages = languages_for_request()

    annotator = CirculationManagerAnnotator(lane)
    arg = flask.request.args.get
    order_facet = arg('order', 'title')
    size = arg('size', '50')
    offset = arg('after', None)
    cache_url = popular_feed_cache_url(
        annotator, lane, languages, order_facet, offset, size)
    def get(*args, **kwargs):
        return make_popular_feed(Conf.db, annotator, lane, languages)
    feed_rep, cached = Representation.get(
        Conf.db, cache_url, get, accept=OPDSFeed.ACQUISITION_FEED_TYPE,
        max_age=None)
    feed_xml = feed_rep.content
    return feed_response(feed_xml)

@app.route('/search', defaults=dict(lane=None))
@app.route('/search/', defaults=dict(lane=None))
@app.route('/search/<lane>')
def lane_search(lane):
    languages = languages_for_request()
    query = flask.request.args.get('q')
    if lane:
        lane = Conf.sublanes.by_name[lane]    
        lane_name = lane.name
    else:
        # Create a synthetic Lane that includes absolutely everything.
        lane = Lane.everything(Conf.db)
        lane_name = None
    this_url = url_for('lane_search', lane=lane_name, _external=True)
    if not query:
        # Send the search form
        return OpenSearchDocument.for_lane(lane, this_url)
    # Run a search.    
    results = lane.search(languages, query, Conf.search, 30)
    info = OpenSearchDocument.search_info(lane)
    opds_feed = AcquisitionFeed(
        Conf.db, info['name'], 
        this_url + "?q=" + urllib.quote(query.encode("utf8")),
        results, CirculationManagerAnnotator(lane))
    return feed_response(opds_feed)

@app.route('/works/')
def work():
    annotator = CirculationManagerAnnotator(None)
    return URNLookupController(Conf.db).work_lookup(annotator, 'work')
    # Conf.urn_lookup_controller.permalink(urn, annotator)

def _load_licensepool(data_source, identifier):
    if isinstance(data_source, DataSource):
        source = data_source
    else:
        source = DataSource.lookup(Conf.db, data_source)
    if source is None:
        return problem(None, "No such data source: %s" % data_source, 404)

    if isinstance(identifier, Identifier):
        id_obj = identifier
    else:
        identifier_type = source.primary_identifier_type
        id_obj, ignore = Identifier.for_foreign_id(
            Conf.db, identifier_type, identifier, autocreate=False)
    if not id_obj:
        # TODO
        return problem(
            NO_LICENSES_PROBLEM, "I never heard of such a book.", 404)
    pool = id_obj.licensed_through
    return pool

def _apply_borrowing_policy(patron, license_pool):
    if not patron.can_borrow(license_pool.work, Conf.policy):
        return problem(
            FORBIDDEN_BY_POLICY_PROBLEM, 
            "Library policy prohibits us from lending you this book.",
            451
        )
    return None


@app.route('/works/<data_source>/<identifier>/fulfill')
@requires_auth
def fulfill(data_source, identifier):
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
    try:
        fulfillment = Conf.circulation.fulfill(patron, pin, pool)
    except NoActiveLoan, e:
        return problem(
            NO_ACTIVE_LOAN_PROBLEM, 
            "Can't fulfill request because you have no active loan for this work.",
            e.status_code)
    except CannotFulfill, e:
        return problem(CANNOT_FULFILL_PROBLEM, str(e), e.status_code)
    
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
@requires_auth
def borrow(data_source, identifier):
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
        loan, hold, fulfillment, is_new = Conf.circulation.borrow(
            patron, pin, pool, Conf.hold_notification_email_address)
    except NoOpenAccessDownload, e:
        problem_doc = problem(
            NO_LICENSES_PROBLEM,
            "Sorry, couldn't find an open-access download link.", 404)
    except CannotLoan, e:
        problem_doc = problem(CHECKOUT_FAILED, str(e), 400)
    except CannotHold, e:
        problem_doc = problem(HOLD_FAILED_PROBLEM, str(e), 400)

    if problem_doc:
        return problem_doc

    # At this point we have either a loan or a hold. If a loan, serve
    # a feed that tells the patron how to fulfill the loan. If a hold,
    # serve a feed that talks about the hold.
    if loan:
        feed = CirculationManagerLoanAndHoldAnnotator.single_loan_feed(loan)
    elif hold:
        feed = CirculationManagerLoanAndHoldAnnotator.single_hold_feed(hold)
    else:
        # This should never happen -- we should have sent a more specific
        # error earlier.
        return problem(HOLD_FAILED_PROBLEM, "", 400)
    content = unicode(feed)
    if is_new:
        status_code = 201
    else:
        status_code = 200
    return Response(content, status_code, headers)

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

if __name__ == '__main__':
    debug = True
    url = os.environ['CIRCULATION_WEB_APP_URL']
    scheme, netloc, path, parameters, query, fragment = urlparse.urlparse(url)
    if ':' in netloc:
        host, port = netloc.split(':')
        port = int(port)
    else:
        host = netloc
        port = 80
    Conf.log.info("Starting app on %s:%s", host, port)
    app.run(debug=debug, host=host, port=port)
