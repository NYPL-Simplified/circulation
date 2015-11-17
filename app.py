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
    CustomListFeed,
    DataSource,
    production_session,
    Hold,
    LaneList,
    Lane,
    LicensePool,
    LicensePoolDeliveryMechanism,
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

        cls.primary_collection_languages = language_policy[
            Configuration.PRIMARY_LANGUAGE_COLLECTIONS
        ]
        cls.other_collection_languages = language_policy.get(
            Configuration.OTHER_LANGUAGE_COLLECTIONS, []
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

def add_configuration_links(feed):
    for rel, value in (
            ("terms-of-service", Configuration.terms_of_service_url()),
            ("privacy-policy", Configuration.privacy_policy_url()),
            ("copyright", Configuration.acknowledgements_url()),
    ):
        if value:
            d = dict(href=value, type="text/html", rel=rel)
            if isinstance(feed, OPDSFeed):
                feed.add_link(**d)
            else:
                # This is an ElementTree object.
                link = E.link(**d)
                feed.append(link)


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
        href=url_for('lane_search', lane_name=lane.name,
                         _external=True))
    opds_feed = AcquisitionFeed.featured(
        languages, lane, annotator, quality_cutoff=0.0)
    opds_feed.add_link(**search_link)
    add_configuration_links(opds_feed)
    return 200, {"content-type": OPDSFeed.ACQUISITION_FEED_TYPE}, unicode(opds_feed)

def acquisition_groups_cache_url(annotator, lane, languages):
    if not lane:
        lane_name = lane
    else:
        lane_name = lane.url_name
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
        lane_name = lane.url_name
    url = cdn_url_for("acquisition_groups", lane_name=lane_name, _external=True)
    best_sellers_url = cdn_url_for("popular_feed", lane_name=lane_name, _external=True)
    staff_picks_url = cdn_url_for("staff_picks_feed", lane_name=lane_name, _external=True)
    feed = AcquisitionFeed.featured_groups(
        url, best_sellers_url, staff_picks_url, languages, lane, annotator)
    if feed is None:
        # The configuration says there should be books in
        # subcategories, but there are actually none, so a grouped
        # feed is not appropriate. Return a flat feed instead.
        return make_feed(Conf.db, annotator, lane, languages, 'author', 0, 50)
    feed.add_link(
        rel="search", 
        type="application/opensearchdescription+xml",
        href=url_for('lane_search', lane_name=lane_name, _external=True))
    feed.add_link(
        rel="http://opds-spec.org/shelf", 
        href=url_for('active_loans', _external=True))
    add_configuration_links(feed)
    return (200,
            {"content-type": OPDSFeed.ACQUISITION_FEED_TYPE}, 
            unicode(feed)
    )

def popular_feed_cache_url(annotator, lane_name, languages, order_facet,
                           offset, size):
    if isinstance(lane_name, Lane):
        lane_name = lane_name.url_name

    url = url_for('popular_feed', lane_name=lane_name, order=order_facet,
                  after=offset, size=size,_external=True)
    if '?' in url:
        url += '&'
    else:
        url += '?'
    if isinstance(languages, basestring):
        languages = [languages]
    return url + "languages=%s" % ",".join(languages)

def make_popular_feed(_db, annotator, lane, languages, order_facet,
                      offset, size):

    # Do some preliminary data checking to avoid generating expensive
    # feeds that contain nothing.
    if lane and lane.parent:
        # We only show a best-seller list for the top-level lanes.
        return problem_raw(None, "No such feed", 404)

    if 'eng' not in languages:
        # We only have information about English best-sellers.
        return problem_raw(None, "No such feed", 404)

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
    qu = work_feed.page_query(_db, offset, size)
    page = qu.all()
    this_url = cdn_url_for(
        'popular_feed', lane_name=lane_name, after=offset, size=size, 
        _external=True
    )
    opds_feed = AcquisitionFeed(_db, title, this_url, page,
                                annotator, work_feed.active_facet)

    # Add a 'next' link unless this page is empty.
    if len(page) == 0:
        offset = None
    else:
        offset = int(offset or 0)
        offset += size
        next_url = cdn_url_for(
            'popular_feed', lane_name=lane_name, order=order_facet,
            after=offset, size=size, _external=True)
        opds_feed.add_link(rel="next", href=next_url)
    add_configuration_links(opds_feed)
    return (200,
            {"content-type": OPDSFeed.ACQUISITION_FEED_TYPE}, 
            unicode(opds_feed)
    )

def staff_picks_feed_cache_url(annotator, lane, languages, order_facet,
                               offset, size):
    if isinstance(lane, Lane):
        lane_name = lane.url_name
    elif isinstance(lane, Conf):
        lane_name = None
    else:
        lane_name = lane

    kw = dict(lane_name=lane_name, order=order_facet, offset=offset, size=size)
    url = url_for('staff_picks_feed', _external=True, **kw)
    if '?' in url:
        url += '&'
    else:
        url += '?'
    if isinstance(languages, basestring):
        languages = [languages]
    return url + "languages=%s" % ",".join(languages)

def make_staff_picks_feed(_db, annotator, lane, languages, order_facet,
                          offset, size):
    # Do some preliminary data checking to avoid generating expensive
    # feeds that contain nothing.
    if lane and lane.parent:
        # We only show a best-seller list for the top-level lanes.
        return problem_raw(None, "No such feed", 404)

    if 'eng' not in languages:
        # We only have information about English best-sellers.
        return problem_raw(None, "No such feed", 404)

    if not lane:
        lane_name = lane
        lane_display_name = lane
    else:
        lane_name = lane.url_name
        lane_display_name = lane.display_name

    if lane_display_name:
        title = "%s: Staff Picks" % lane_display_name
    else:
        title = "Staff Picks"
        lane = None

    staff = DataSource.lookup(_db, DataSource.LIBRARY_STAFF)
    work_feed = CustomListFeed(
        lane, staff, languages, availability=CustomListFeed.ALL,
        order_facet=order_facet
    )
    qu = work_feed.page_query(_db, offset, size)
    page = qu.all()

    this_url = cdn_url_for(
        'staff_picks_feed', lane_name=lane_name, 
        after=offset, size=size,
        _external=True
    )
    # Conf.log.info("Found %d entries for %s", len(page), this_url)
    opds_feed = AcquisitionFeed(_db, title, this_url, page,
                                annotator, work_feed.active_facet)

        # Add a 'next' link unless this page is empty.
    if len(page) == 0:
        offset = None
    else:
        offset = int(offset) or 0
        offset += size
        next_url = cdn_url_for(
            'staff_picks_feed', lane_name=lane_name, order=order_facet,
            after=offset, size=size, _external=True)
        opds_feed.add_link(rel="next", href=next_url)
    add_configuration_links(opds_feed)

    return (200,
            {"content-type": OPDSFeed.ACQUISITION_FEED_TYPE}, 
            unicode(opds_feed)
    )


@app.route('/')
def index():    

    # The simple case: the app is equally open to all clients.
    policy = Configuration.root_lane_policy()
    if not policy:
        return redirect(cdn_url_for('acquisition_groups'))

    # The more complex case. We must authorize the patron, check
    # their type, and redirect them to an appropriate feed.
    return appropriate_index_for_patron_type()

@requires_auth
def authenticated_patron_root_lane():
    patron = flask.request.patron
    policy = Configuration.root_lane_policy()
    return policy.get(patron.external_type)

@requires_auth
def appropriate_index_for_patron_type():
    root_lane = authenticated_patron_root_lane()
    return redirect(cdn_url_for('acquisition_groups', lane_name=root_lane))

@app.route('/heartbeat')
def hearbeat():
    return HeartbeatController().heartbeat()

@app.route('/service_status')
def service_status():
    conf = Configuration.authentication_policy()
    username = conf[Configuration.AUTHENTICATION_TEST_USERNAME]
    password = conf[Configuration.AUTHENTICATION_TEST_PASSWORD]

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

    def do_patron():
        patron = Conf.auth.authenticated_patron(Conf.db, username, password)
        patrons.append(patron)
        if patron:
            return patron
        else:
            raise ValueError("Could not authenticate test patron!")

    _add_timing('Patron authentication', do_patron)

    patron = patrons[0]
    def do_overdrive():
        if not Conf.overdrive:
            raise ValueError("Overdrive not configured")
        return Conf.overdrive.patron_activity(patron, password)
    _add_timing('Overdrive patron account', do_overdrive)

    def do_threem():
        if not Conf.threem:
            raise ValueError("3M not configured")
        return Conf.threem.patron_activity(patron, password)
    _add_timing('3M patron account', do_threem)

    def do_axis():
        if not Conf.axis:
            raise ValueError("Axis not configured")
        return Conf.axis.patron_activity(patron, password)
    _add_timing('Axis patron account', do_axis)

    statuses = []
    for k, v in sorted(timings.items()):
        statuses.append(" <li><b>%s</b>: %s</li>" % (k, v))

    doc = template % dict(statuses="\n".join(statuses))
    return make_response(doc, 200, {"Content-Type": "text/html"})


@app.route('/lanes', defaults=dict(lane_name=None))
@app.route('/lanes/', defaults=dict(lane_name=None))
@app.route('/lanes/<lane_name>')
def navigation_feed(lane_name):
    if lane_name is None:
        lane = Conf
    else:
        if lane_name not in Conf.sublanes.by_name:
            return problem(
                NO_SUCH_LANE_PROBLEM, "No such lane: %s" % lane_name, 404)
        lane = Conf.sublanes.by_name[lane]

    languages = Conf.languages_for_request()
    this_url = cdn_url_for("navigation_feed", lane_name=lane_name, _external=True)
    key = (",".join(languages), this_url)
    # This feed will not change unless the application is upgraded,
    # so there's no need to expire the cache.
    if key in feed_cache:
        return feed_response(feed_cache[key], acquisition=False, cache_for=7200)
        
    feed = NavigationFeed.main_feed(
        lane, CirculationManagerAnnotator(Conf.circulation, lane))

    if not lane.parent:
        # Top-level lanes are the only ones that have best-seller
        # and staff pick lanes.
        feed.add_link(
            rel=NavigationFeed.POPULAR_REL, title="Best Sellers",
            type=NavigationFeed.ACQUISITION_FEED_TYPE,
            href=cdn_url_for('popular_feed', lane_name=lane.name, _external=True))
        if lane != Conf or Conf.configuration.show_staff_picks_on_top_level():
            feed.add_link(
                rel=NavigationFeed.RECOMMENDED_REL, title="Staff Picks",
                type=NavigationFeed.ACQUISITION_FEED_TYPE,
                href=cdn_url_for('staff_picks_feed', lane_name=lane.name, _external=True))

    feed.add_link(
        rel="search", 
        type="application/opensearchdescription+xml",
        href=url_for('lane_search', lane_name=None, _external=True))
    feed.add_link(
        rel="http://opds-spec.org/shelf", 
        href=url_for('active_loans', _external=True))

    add_configuration_links(feed)
    feed = unicode(feed)
    feed_cache[key] = feed
    return feed_response(feed, acquisition=False, cache_for=7200)

def lane_url(cls, lane, order=None):
    return cdn_url_for('feed', lane_name=lane.name, order=order, _external=True)

@app.route('/groups', defaults=dict(lane_name=None))
@app.route('/groups/', defaults=dict(lane_name=None))
@app.route('/groups/<lane_name>')
def acquisition_groups(lane_name):
    if lane_name is None:
        lane = Conf
    elif lane_name not in Conf.sublanes.by_name:
        return problem(NO_SUCH_LANE_PROBLEM, "No such lane: %s" % lane_name, 404)
    else:
        lane = Conf.sublanes.by_name[lane_name]

    languages = Conf.languages_for_request()
    annotator = CirculationManagerAnnotator(Conf.circulation, lane)

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
    Conf.log.info(
        "That took %.2f, cached=%r, size=%s", b-a, cached, len(feed_xml)
    )
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
    feed = CirculationManagerLoanAndHoldAnnotator.active_loans_for(
        Conf.circulation, patron)
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
        msg = "I don't see an active loan or hold for %s, but that's not a problem."
        return Response(msg, 200, {"content-type": "text/plain"})

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
        if not Conf.circulation.can_revoke_hold(pool, hold):
            title = "Cannot release a hold once it enters reserved state."
            return problem(CANNOT_RELEASE_HOLD_PROBLEM, title, 400)
        try:
            Conf.circulation.release_hold(patron, pin, pool)
        except CannotReleaseHold, e:
            title = "Hold released locally but remote failed: %s" % str(e)
            return problem(CANNOT_RELEASE_HOLD_PROBLEM, title, 500)

    work = pool.work
    annotator = CirculationManagerAnnotator(Conf.circulation, None)
    return entry_response(
        AcquisitionFeed.single_entry(Conf.db, work, annotator)
    )


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
                Conf.circulation, loan)
        else:
            feed = CirculationManagerLoanAndHoldAnnotator.single_hold_feed(
                Conf.circulation, hold)
        feed = unicode(feed)
        return feed_response(feed, None)

    if flask.request.method=='DELETE':
        return revoke_loan_or_hold(data_source, identifier)

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
        title = "%s: By title" % lane.display_name
    elif order_facet == 'author':
        title = "%s: By author" % lane.display_name
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
        offset = int(offset or 0)
        offset += size
        next_url = feed_url(lane, order_facet, offset, size)
        opds_feed.add_link(rel="next", href=next_url)

    # Add a 'search' link.
    search_link = dict(
        rel="search",
        type="application/opensearchdescription+xml",
        href=url_for('lane_search', lane_name=lane.name, _external=True))
    opds_feed.add_link(**search_link)
    add_configuration_links(opds_feed)
    return (200,
            {"content-type": OPDSFeed.ACQUISITION_FEED_TYPE}, 
            unicode(opds_feed),
        )


@app.route('/feed', defaults=dict(lane_name=None))
@app.route('/feed/', defaults=dict(lane_name=None))
@app.route('/feed/<lane_name>')
def feed(lane_name):
    lane_name = lane_name.replace("__", "/")
    languages = Conf.languages_for_request()
    arg = flask.request.args.get
    order_facet = arg('order', 'recommended')
    offset = arg('after', None)

    if lane_name not in Conf.sublanes.by_name:
        return problem(NO_SUCH_LANE_PROBLEM, "No such lane: %s" % lane_name, 404)

    lane = Conf.sublanes.by_name[lane_name]

    key = (lane, ",".join(languages), order_facet)
    feed_xml = None
    annotator = CirculationManagerAnnotator(Conf.circulation, lane)

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
    languages = Conf.languages_for_request()
    arg = flask.request.args.get
    order_facet = arg('order', 'title')
    offset = int(arg('after', 0))
    size = int(arg('size', 50))
    annotator = CirculationManagerAnnotator(
        Conf.circulation, lane, facet_view='staff_picks_feed'
    )
    cache_url = staff_picks_feed_cache_url(
        annotator, lane, languages, order_facet, offset, size)
    def get(*args, **kwargs):
        return make_staff_picks_feed(
            Conf.db, annotator, lane, languages, order_facet, offset, size)
    feed_rep, cached = Representation.get(
        Conf.db, cache_url, get, accept=OPDSFeed.ACQUISITION_FEED_TYPE,
        max_age=60*60*24)
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
    languages = Conf.languages_for_request()

    annotator = CirculationManagerAnnotator(Conf.circulation, lane)
    arg = flask.request.args.get
    order_facet = arg('order', 'title')
    size = arg('size', '50')

    try:
        size = int(size)
    except ValueError:
        return problem(None, "Invalid size: %s" % size, 400)
    size = min(size, 100)

    offset = arg('after', 0)
    if offset:
        try:
            offset = int(offset)
        except ValueError:
            return problem(None, "Invalid offset: %s" % offset, 400)

    cache_url = popular_feed_cache_url(
        annotator, lane, languages, order_facet, offset, size)
    def get(*args, **kwargs):
        return make_popular_feed(Conf.db, annotator, lane, languages, 
                                 order_facet, offset, size)
    feed_rep, cached = Representation.get(
        Conf.db, cache_url, get, accept=OPDSFeed.ACQUISITION_FEED_TYPE,
        max_age=60*60*24)
    feed_xml = feed_rep.content
    return feed_response(feed_xml)

@app.route('/search', defaults=dict(lane_name=None))
@app.route('/search/', defaults=dict(lane_name=None))
@app.route('/search/<lane_name>')
def lane_search(lane_name):
    languages = Conf.languages_for_request()
    query = flask.request.args.get('q')
    if lane_name:
        lane = Conf.sublanes.by_name[lane_name]
    else:
        # Create a synthetic Lane that includes absolutely everything.
        lane = Lane.everything(Conf.db)
        lane_name = None
    this_url = url_for('lane_search', lane_name=lane_name, _external=True)
    if not query:
        # Send the search form
        return OpenSearchDocument.for_lane(lane, this_url)
    # Run a search.    
    results = lane.search(languages, query, Conf.search, 30)
    info = OpenSearchDocument.search_info(lane)
    opds_feed = AcquisitionFeed(
        Conf.db, info['name'], 
        this_url + "?q=" + urllib.quote(query.encode("utf8")),
        results, CirculationManagerAnnotator(Conf.circulation, lane))
    return feed_response(opds_feed)

@app.route('/works/')
def work():
    annotator = CirculationManagerAnnotator(Conf.circulation, None)
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

def _load_licensepooldelivery(pool, mechanism_id):
    mechanism = get_one(
        Conf.db, LicensePoolDeliveryMechanism, license_pool=pool,
        delivery_mechanism_id=mechanism_id
    )

    if not mechanism:
        return problem(
            BAD_DELIVERY_MECHANISM_PROBLEM, 
            "Unsupported delivery mechanism for this book.",
            400
        )
    return mechanism


def _apply_borrowing_policy(patron, license_pool):
    if not patron.can_borrow(license_pool.work, Conf.policy):
        return problem(
            FORBIDDEN_BY_POLICY_PROBLEM, 
            "Library policy prohibits us from lending you this book.",
            451
        )

    if (license_pool.licenses_available == 0 and
        Configuration.hold_policy() !=
        Configuration.HOLD_POLICY_ALLOW
    ):
        return problem(
            FORBIDDEN_BY_POLICY_PROBLEM, 
            "Library policy prohibits the placement of holds.",
            403
        )        

    return None


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
