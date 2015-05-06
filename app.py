from functools import wraps
from nose.tools import set_trace
import datetime
import random
import time
import os
import sys
import traceback
import urlparse

from sqlalchemy.orm.exc import (
    NoResultFound
)

import flask
from flask import Flask, url_for, redirect, Response

from core.external_search import (
    ExternalSearchIndex,
    DummyExternalSearchIndex,
)
from circulation_exceptions import (
    CannotLoan,
    AlreadyCheckedOut,
    NoAvailableCopies,
)
from core.app_server import (
    feed_response,
    HeartbeatController,
    URNLookupController,
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
    AllCustomListsFromDataSourceFeed,
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
    languages_for_request
)
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
            cls.auth = DummyMilleniumPatronAPI()
            cls.search = DummyExternalSearchIndex()
        else:
            _db = production_session()
            lanes = make_lanes(_db)
            for lane in lanes.lanes:
                print lane.name
                for sublane in lane.sublanes:
                    print "", sublane.display_name
            cls.db = _db
            cls.sublanes = lanes
            cls.urn_lookup_controller = URNLookupController(cls.db)
            cls.overdrive = OverdriveAPI(cls.db)
            cls.threem = ThreeMAPI(cls.db)
            cls.auth = MilleniumPatronAPI()
            cls.search = ExternalSearchIndex()

if os.environ.get('TESTING') == "True":
    Conf.testing = True
    # It's the test's responsibility to call initialize()
else:
    Conf.testing = False
    Conf.initialize()

app = Flask(__name__)
app.config['DEBUG'] = True
app.debug = True

INVALID_CREDENTIALS_PROBLEM = "http://librarysimplified.org/terms/problem/credentials-invalid"
INVALID_CREDENTIALS_TITLE = "A valid library card barcode number and PIN are required."
EXPIRED_CREDENTIALS_PROBLEM = "http://librarysimplified.org/terms/problem/credentials-expired"
EXPIRED_CREDENTIALS_TITLE = "Your library card has expired. You need to renew it."
NO_LICENSES_PROBLEM = "http://librarysimplified.org/terms/problem/no-licenses"
NO_AVAILABLE_LICENSE_PROBLEM = "http://librarysimplified.org/terms/problem/no-available-license"
NO_ACCEPTABLE_FORMAT_PROBLEM = "http://librarysimplified.org/terms/problem/no-acceptable-format"
ALREADY_CHECKED_OUT_PROBLEM = "http://librarysimplified.org/terms/problem/loan-already-exists"
CHECKOUT_FAILED = "http://librarysimplified.org/terms/problem/could-not-issue-loan"
NO_ACTIVE_LOAN_PROBLEM = "http://librarysimplified.org/terms/problem/no-active-loan"
NO_ACTIVE_HOLD_PROBLEM = "http://librarysimplified.org/terms/problem/no-active-hold"
NO_ACTIVE_LOAN_OR_HOLD_PROBLEM = "http://librarysimplified.org/terms/problem/no-active-loan"
COULD_NOT_MIRROR_TO_REMOTE = "http://librarysimplified.org/terms/problem/could-not-mirror-to-remote"
NO_SUCH_LANE_PROBLEM = "http://librarysimplified.org/terms/problem/unknown-lane"

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
    """Sends a 401 response that enables basic auth"""
    return problem(
        uri, title, 401,
        headers= { 'WWW-Authenticate' : 'Basic realm="Library card"'})

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
    url = annotator.featured_feed_url(lane)
    if '?' in url:
        url += '&'
    else:
        url += '?'
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

def acquisition_blocks_cache_url(annotator, lane, languages):
    if not lane:
        lane_name = lane
    else:
        lane_name = lane.name
    url = url_for('acquisition_blocks', lane_name=lane_name, _external=True)
    if '?' in url:
        url += '&'
    else:
        url += '?'
    return url + "languages=%s" % ",".join(languages)

def make_acquisition_blocks(annotator, lane, languages):
    if not lane:
        lane_name = lane
    else:
        lane_name = lane.name
    url = url_for("acquisition_blocks", lane=lane_name)
    feed = AcquisitionFeed.featured_blocks(url, languages, lane, annotator)
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

def popular_feed_cache_url(annotator, lane, languages):
    if not lane:
        lane_name = lane
    else:
        lane_name = lane.name
    url = url_for('popular_feed', lane_name=lane_name, _external=True)
    if '?' in url:
        url += '&'
    else:
        url += '?'
    return url + "languages=%s" % ",".join(languages)

def make_popular_feed(_db, annotator, lane, languages):
    if not lane:
        lane_name = lane
        lane_display_name = lane
        title = "Best Sellers"
    else:
        lane_name = lane.name
        lane_display_name = lane.display_name
        title = "%s: Best Sellers" % lane_display_name
    work_feed = AllCustomListsFromDataSourceFeed(
        _db, [DataSource.NYT], languages,
        availability=AllCustomListsFromDataSourceFeed.ALL)
    a = time.time()
    page = work_feed.page_query(_db, None, 200).all()
    b = time.time()
    print "Best-seller feed created in %.2f sec" % (b-a)
    page = random.sample(page, min(len(page), 20))

    this_url = url_for('popular_feed', lane_name=lane_name, _external=True)
    opds_feed = AcquisitionFeed(_db, title, this_url, page,
                                annotator, work_feed.active_facet)
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
    return url + "languages=%s" % ",".join(languages)

def make_staff_picks_feed(_db, annotator, lane, languages):
    if not lane:
        lane_name = lane
        lane_display_name = lane
        title = "Staff Picks"
    else:
        lane_name = lane.name
        lane_display_name = lane.display_name
        title = "%s: Staff Picks" % lane_display_name

    custom_list = _db.query(CustomList).filter(
        CustomList.name==CustomList.STAFF_PICKS_NAME).one()
    if not custom_list:
        print "No staff picks list."
        return (200, {}, "")

    work_feed = CustomListFeed(
        _db, [custom_list], languages, availability=CustomListFeed.ALL)
    page = work_feed.page_query(_db, None, 100).all()
    page = random.sample(page, min(len(page), 20))

    this_url = url_for('staff_picks_feed', lane_name=lane_name, _external=True)
    opds_feed = AcquisitionFeed(_db, title, this_url, page,
                                annotator, work_feed.active_facet)
    return (200,
            {"content-type": OPDSFeed.ACQUISITION_FEED_TYPE}, 
            unicode(opds_feed)
    )


@app.route('/')
def index():    
    return redirect(url_for('.navigation_feed'))

@app.route('/heartbeat')
def hearbeat():
    return HeartbeatController().heartbeat()

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
    this_url = url_for("navigation_feed", lane=lane_name, _external=True)
    key = (",".join(languages), this_url)
    # This feed will not change unless the application is upgraded,
    # so there's no need to expire the cache.
    if key in feed_cache:
        return feed_response(feed_cache[key], acquisition=False)
        
    feed = NavigationFeed.main_feed(lane, CirculationManagerAnnotator(lane))

    if not lane.parent:
        # Top-level lanes are the only ones that have best-seller
        # and staff pick lanes.
        feed.add_link(
            rel=NavigationFeed.POPULAR_REL, title="Best Sellers",
            type=NavigationFeed.ACQUISITION_FEED_TYPE,
            href=url_for('popular_feed', lane_name=lane.name, _external=True))
        feed.add_link(
            rel=NavigationFeed.RECOMMENDED_REL, title="Staff Picks",
            type=NavigationFeed.ACQUISITION_FEED_TYPE,
            href=url_for('staff_picks_feed', lane_name=lane.name, _external=True))

    feed.add_link(
        rel="search", 
        type="application/opensearchdescription+xml",
        href=url_for('lane_search', lane=None, _external=True))
    feed.add_link(
        rel="http://opds-spec.org/shelf", 
        href=url_for('active_loans', _external=True))

    feed = unicode(feed)
    feed_cache[key] = feed
    return feed_response(feed, acquisition=False)

def lane_url(cls, lane, order=None):
    return url_for('feed', lane=lane.name, order=order, _external=True)

@app.route('/blocks', defaults=dict(lane=None))
@app.route('/blocks/', defaults=dict(lane=None))
@app.route('/blocks/<lane>')
def acquisition_blocks(lane):
    lane_name = lane
    if lane is None:
        lane = Conf
    else:
        if lane not in Conf.sublanes.by_name:
            return problem(NO_SUCH_LANE_PROBLEM, "No such lane: %s" % lane, 404)
        lane = Conf.sublanes.by_name[lane]

    languages = languages_for_request()
    annotator = CirculationManagerAnnotator(lane)

    cache_url = acquisition_blocks_cache_url(annotator, lane, languages)
    def get(*args, **kwargs):
        make_acquisition_blocks(annotator, lane, languages)
    feed_rep, cached = Representation.get(
        Conf.db, cache_url, get, accept=OPDSFeed.ACQUISITION_FEED_TYPE,
        max_age=None)
    feed_xml = feed_rep.content
    return feed_response(feed_xml, acquisition=True)


@app.route('/loans/', methods=['GET', 'HEAD'])
@requires_auth
def active_loans():

    if flask.request.method=='HEAD':
        return Response()

    patron = flask.request.patron

    # First synchronize our local list of loans and holds with all
    # third-party loan providers.
    if patron.authorization_identifier and len(patron.authorization_identifier) == 14:
        # TODO: Barcodes that are not 14 digits are dummy code
        # that allow the creation of arbitrary test accounts that
        # are limited to public domain books. We cannot
        # ask Overdrive or 3M about these barcodes. 
        header = flask.request.authorization
        try:
            overdrive_loans = Conf.overdrive.get_patron_checkouts(
                patron, header.password)
            overdrive_holds = Conf.overdrive.get_patron_holds(
                patron, header.password)
            threem_loans, threem_holds, threem_reserves = Conf.threem.get_patron_checkouts(
            flask.request.patron)

            Conf.overdrive.sync_bookshelf(patron, overdrive_loans, overdrive_holds)
            Conf.threem.sync_bookshelf(patron, threem_loans, threem_holds, threem_reserves)
            Conf.db.commit()
        except Exception, e:
            # If anything goes wrong, omit the sync step and just
            # display the current active loans, as we understand them.
            print "ERROR DURING SYNC"
            print traceback.format_exc()            

    # Then make the feed.
    feed = CirculationManagerLoanAndHoldAnnotator.active_loans_for(patron)
    return feed_response(feed)

@app.route('/loans/<data_source>/<identifier>/revoke')
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
        return problem(
            NO_ACTIVE_LOAN_OR_HOLD_PROBLEM, 
            'You have no active loan or hold for "%s".' % pool.work.title,
            404)

    pin = flask.request.authorization.password
    status_code = 200
    if loan:
        Conf.db.delete(loan)
        response = None
        if pool.data_source.name==DataSource.OVERDRIVE:
            # It probably won't work, but just to be thorough,
            # tell Overdrive to cancel the loan.
            response = Conf.overdrive.checkin(
                patron, pin, pool.identifier)
        elif pool.data_source.name==DataSource.THREEM:
            response = Conf.threem.checkin(patron.authorization_identifier,
                                               pool.identifier.identifier)
            
        if response and response.status_code == 400:
            uri = COULD_NOT_MIRROR_TO_REMOTE
            title = "Loan deleted locally but remote refused. Loan is likely to show up again on next sync."
            return problem(uri, title, 400)

    if hold:
        Conf.db.delete(hold)
        if pool.data_source.name==DataSource.OVERDRIVE:
            response = Conf.overdrive.release_hold(
                patron, pin, pool.identifier)
        elif pool.data_source.name==DataSource.THREEM:
            response = Conf.threem.release_hold(
                patron.authorization_identifier,
                pool.identifier.identifier)
    Conf.db.commit()
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
        return feed_response(feed)

    if flask.request.method=='DELETE':
        return revoke_loan_or_hold(data_source, identifier)

@app.route('/feed/<lane>')
def feed(lane):
    languages = languages_for_request()
    arg = flask.request.args.get
    order = arg('order', 'recommended')
    last_seen_id = arg('after', None)

    if lane not in Conf.sublanes.by_name:
        return problem(NO_SUCH_LANE_PROBLEM, "No such lane: %s" % lane, 404)

    lane = Conf.sublanes.by_name[lane]

    key = (lane, ",".join(languages), order)
    feed_xml = None
    if not last_seen_id and key in feed_cache:
        chance = random.random()
        feed, created_at = feed_cache.get(key)
        elapsed = time.time()-created_at
        # An old feed is almost certain to be regenerated.
        if elapsed > 1800:
            chance = chance / 5
        elif elapsed > 3600:
            chance = 0
        if chance > 0.10:
            # Return the cached version.
            return feed_response(feed)

    search_link = dict(
        rel="search",
        type="application/opensearchdescription+xml",
        href=url_for('lane_search', lane=lane.name, _external=True))

    annotator = CirculationManagerAnnotator(lane)
    work_feed = None
    if order == 'recommended':
        cache_url = featured_feed_cache_url(annotator, lane, languages)
        def get(*args, **kwargs):
            return make_featured_feed(annotator, lane, languages)
        feed_rep, cached = Representation.get(
            Conf.db, cache_url, get, accept=OPDSFeed.ACQUISITION_FEED_TYPE,
            max_age=60*60)
        feed_xml = feed_rep.content
    elif order == 'title':
        work_feed = LaneFeed(lane, languages, Edition.sort_title)
        title = "%s: By title" % lane.name
    elif order == 'author':
        work_feed = LaneFeed(lane, languages, Edition.sort_author)
        title = "%s: By author" % lane.name
    else:
        return problem(None, "I don't know how to order a feed by '%s'" % order, 400)

    if work_feed:
        # Turn the work feed into an acquisition feed.
        size = arg('size', '50')
        try:
            size = int(size)
        except ValueError:
            return problem(None, "Invalid size: %s" % size, 400)
        size = min(size, 100)

        last_work_seen = None
        last_id = arg('after', None)
        if last_id:
            try:
                last_id = int(last_id)
            except ValueError:
                return problem(None, "Invalid work ID: %s" % last_id, 400)
            try:
                last_work_seen = Conf.db.query(Work).filter(Work.id==last_id).one()
            except NoResultFound:
                return problem(None, "No such work id: %s" % last_id, 400)

        this_url = url_for('feed', lane=lane.name, order=order, _external=True)
        page = work_feed.page_query(Conf.db, last_work_seen, size).all()

        opds_feed = AcquisitionFeed(Conf.db, title, this_url, page,
                                    annotator, work_feed.active_facet)
        # Add a 'next' link if appropriate.
        if len(page) > 0:
            after = page[-1].id
            next_url = url_for(
                'feed', lane=lane.name, order=order, after=after, _external=True)
            opds_feed.add_link(rel="next", href=next_url)

        opds_feed.add_link(**search_link)

    if not feed_xml:
        feed_xml = unicode(opds_feed)
    if not last_seen_id:
        feed_cache[key] = (feed_xml, time.time())
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
    cache_url = popular_feed_cache_url(annotator, lane, languages)
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
    results = lane.search(languages, query, Conf.search, 50)
    info = OpenSearchDocument.search_info(lane)
    opds_feed = AcquisitionFeed(
        Conf.db, info['name'], 
        this_url + "?q=" + urllib.quote(query),
        results, CirculationManagerAnnotator(lane))
    return feed_response(opds_feed)

@app.route('/works/')
def work():
    annotator = CirculationManagerAnnotator(None)
    return URNLookupController(Conf.db).work_lookup(annotator, 'work')
    # Conf.urn_lookup_controller.permalink(urn, annotator)

def _load_licensepool(data_source, identifier):
    source = DataSource.lookup(Conf.db, data_source)
    if source is None:
        return problem(None, "No such data source!", 404)
    identifier_type = source.primary_identifier_type

    id_obj, ignore = Identifier.for_foreign_id(
        Conf.db, identifier_type, identifier, autocreate=False)
    if not id_obj:
        # TODO
        return problem(
            NO_LICENSES_PROBLEM, "I never heard of such a book.", 404)
    pool = id_obj.licensed_through
    return pool

def _api_for_license_pool(license_pool):
    if license_pool.data_source.name==DataSource.OVERDRIVE:
        api = Conf.overdrive
        possible_formats = ["ebook-epub-adobe", "ebook-epub-open"]
    else:
        api = Conf.threem
        possible_formats = [None]

    return api, possible_formats


@app.route('/works/<data_source>/<identifier>/fulfill')
@requires_auth
def fulfill(data_source, identifier):
    """Fulfill a book that has already been checked out.

    If successful, this will serve the patron a downloadable copy of
    the book, or a DRM license such as an ACSM file, or serve an HTTP
    redirect that sends the patron to a copy of the book or a license
    file.
    """
    patron = flask.request.patron
    header = flask.request.authorization

    # Turn source + identifier into a LicensePool
    pool = _load_licensepool(data_source, identifier)
    if isinstance(pool, Response):
        return pool

    # There must be an active loan. We'll try fulfilling even if the
    # loan has expired--they may have renewed it out-of-band.
    loan = get_one(Conf.db, Loan, patron=patron, license_pool=pool)

    content_link = None
    content_type = None
    status_code = None
    content = None

    if pool.open_access:
        best_pool, best_link = pool.best_license_link
        if not best_link:
            return problem(
                NO_LICENSES_PROBLEM,
                "Sorry, couldn't find an open-access download link.", 404)

        r = best_link.representation
        if r.url:
            content_link = r.url

        media_type = best_link.representation.media_type
    else:
        api, possible_formats = _api_for_license_pool(pool)

        for f in possible_formats:
            content_link, media_type, content = api.fulfill(
                patron, header.password, pool.identifier, f)
            if content_link or content:
                break
        else:
            return problem(
                NO_ACCEPTABLE_FORMAT_PROBLEM,
                "Could not find this book in a usable format.", 500)

    headers = dict()
    if content_link:
        status_code = 302
        headers["Location"] = content_link
    else:
        status_code = 200
    if media_type:
        headers['Content-Type'] = media_type
    return Response(content, status_code, headers)


@app.route('/works/<data_source>/<identifier>/borrow')
@requires_auth
def borrow(data_source, identifier):
    """Create a new loan for a book.

    Return an OPDS Acquisition feed that includes a link of rel
    "http://opds-spec.org/acquisition", which can be used to fetch the
    book or the license file.
    """
    patron = flask.request.patron

    headers = { "Content-Type" : OPDSFeed.ACQUISITION_FEED_TYPE }

    # Turn source + identifier into a LicensePool
    pool = _load_licensepool(data_source, identifier)
    if isinstance(pool, Response):
        return pool

    if not pool:
        return problem(
            NO_LICENSES_PROBLEM, 
            "I don't have any licenses for that book.", 404)

    # Try to find an existing loan.
    loan = get_one(Conf.db, Loan, patron=patron, license_pool=pool)
    header = flask.request.authorization
    content_link = None
    if loan:
        status_code = 200
    else:
        # There is no existing loan.
        status_code = 201 # Assuming this works, of course.
        if pool.open_access:
            best_pool, best_link = pool.best_license_link
            if not best_link:
                return problem(
                    NO_LICENSES_PROBLEM,
                    "Sorry, couldn't find an open-access download link.", 404)
            # Open-access loans never expire.
            content_expires = None
        else:
            api, possible_formats = _api_for_license_pool(pool)
            # At this point there must be a license free to assign to
            # this patron.
            if pool.licenses_available < 1:
                return problem(
                    NO_AVAILABLE_LICENSE_PROBLEM,
                    "Sorry, all copies of this book are checked out.", 400)

            try:
                header = flask.request.authorization

                format_to_use = possible_formats[0]
                content_link, content_type, content, content_expires = api.checkout(
                    patron, header.password, pool.identifier,
                    format_type=format_to_use)
            except NoAvailableCopies:
                # Most likely someone checked out the book and the
                # circulation manager is not yet aware of it.
                return problem(
                    NO_AVAILABLE_LICENSE_PROBLEM,
                    "Sorry, all copies of this book are checked out.", 400)
            except AlreadyCheckedOut:
                return problem(
                    ALREADY_CHECKED_OUT_PROBLEM,
                    "You have already checked out this book.", 400)
            except CannotLoan, e:
                return problem(CHECKOUT_FAILED_PROBLEM, str(e), 400)

        # We've done any necessary work on the back-end to secure the loan.
        # Now create it in our database.
        loan, ignore = pool.loan_to(patron, end=content_expires)

    # At this point we have a loan. (We may have had one to start
    # with, but whatever.) Serve a feed that tells the patron how to
    # fulfill the loan.
    
    # TODO: No, actually, we auto-fulfill the loan.
    return fulfill(data_source, identifier)

    #feed = CirculationManagerLoanAndHoldAnnotator.single_loan_feed(loan)
    #content = unicode(feed)
    #return Response(content, status_code, headers)

print __name__
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
    print host, port
    app.run(debug=debug, host=host, port=port)
