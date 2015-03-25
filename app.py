from functools import wraps
from nose.tools import set_trace
import random
import time
import os
import sys
import urlparse

from sqlalchemy.orm.exc import (
    NoResultFound
)

import flask
from flask import Flask, url_for, redirect, Response

from circulation_exceptions import (
    CannotLoan,
    AlreadyCheckedOut,
    NoAvailableCopies,
)
from core.app_server import (
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
    LaneList,
    Lane,
    LicensePool,
    Loan,
    Patron,
    Identifier,
    Work,
    LaneFeed,
    CustomListFeed,
    Edition,
    )
from core.opensearch import OpenSearchDocument
from opds import CirculationManagerAnnotator
from core.opds import (
    E,
    AcquisitionFeed,
    NavigationFeed,
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
    parent = None
    urn_lookup_controller = None
    overdrive = None
    threem = None
    auth = None

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
        else:
            _db = production_session()
            lanes = make_lanes(_db)
            cls.db = _db
            cls.sublanes = lanes
            cls.urn_lookup_controller = URNLookupController(cls.db)
            cls.overdrive = OverdriveAPI(cls.db)
            cls.threem = ThreeMAPI(cls.db)
            cls.auth = MilleniumPatronAPI()

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
ALREADY_CHECKED_OUT_PROBLEM = "http://librarysimplified.org/terms/problem/loan-already-exists"
CHECKOUT_FAILED = "http://librarysimplified.org/terms/problem/could-not-issue-loan"

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
    if lane is None:
        lane = Conf
    else:
        lane = Conf.sublanes.by_name[lane]

    languages = languages_for_request()
    key = (",".join(languages), 'navigation', lane)
    # This feed will not change unless the application is upgraded,
    # so there's no need to expire the cache.
    if key in feed_cache:
        return feed_cache[key]
        
    feed = NavigationFeed.main_feed(lane, CirculationManagerAnnotator(lane))

    feed.add_link(
        rel=NavigationFeed.POPULAR_REL, title="Best Sellers",
        type=NavigationFeed.ACQUISITION_FEED_TYPE,
        href=url_for('popular_feed', lane_name=lane.name, _external=True))

    feed.add_link(
        rel="search", 
        type="application/opensearchdescription+xml",
        href=url_for('lane_search', lane=None, _external=True))
    feed.add_link(
        rel="http://opds-spec.org/shelf", 
        href=url_for('active_loans', _external=True))

    feed = unicode(feed)
    feed_cache[key] = feed
    return feed

def lane_url(cls, lane, order=None):
    return url_for('feed', lane=lane.name, order=order, _external=True)

@app.route('/loans/')
@requires_auth
def active_loans():
    patron = flask.request.patron

    # First synchronize our local list of loans with all third-party
    # loan providers.
    if patron.authorization_identifier and len(patron.authorization_identifier) == 14:
        # TODO: Barcodes that are not 14 digits are dummy code
        # that allow the creation of arbitrary test accounts that
        # are limited to public domain books. We cannot
        # ask Overdrive or 3M about these barcodes. 
        header = flask.request.authorization
        overdrive_loans = Conf.overdrive.get_patron_checkouts(
            patron, header.password)
        threem_loans, threem_holds = Conf.threem.get_patron_checkouts(
            flask.request.patron)

        Conf.overdrive.sync_bookshelf(patron, overdrive_loans)
        Conf.threem.sync_bookshelf(patron, threem_loans, threem_holds)
        Conf.db.commit()

    # Then make the feed.
    feed = CirculationManagerAnnotator.active_loans_for(patron)
    return unicode(feed)

@app.route('/feed/<lane>')
def feed(lane):
    languages = languages_for_request()
    arg = flask.request.args.get
    order = arg('order', 'recommended')
    last_seen_id = arg('after', None)

    lane = Conf.sublanes.by_name[lane]

    key = (lane, ",".join(languages), order)
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
            return feed

    search_link = dict(
        rel="search",
        type="application/opensearchdescription+xml",
        href=url_for('lane_search', lane=lane.name, _external=True))

    annotator = CirculationManagerAnnotator(lane)
    if order == 'recommended':
        opds_feed = AcquisitionFeed.featured(
            languages, lane, annotator, quality_cutoff=0)
        opds_feed.add_link(**search_link)
        work_feed = None
    elif order == 'title':
        work_feed = LaneFeed(lane, languages, Edition.sort_title)
        title = "%s: By title" % lane.name
    elif order == 'author':
        work_feed = LaneFeed(lane, languages, Edition.sort_author)
        title = "%s: By author" % lane.name
    else:
        return "I don't know how to order a feed by '%s'" % order

    if work_feed:
        # Turn the work feed into an acquisition feed.
        size = arg('size', '50')
        try:
            size = int(size)
        except ValueError:
            return problem("Invalid size: %s" % size, 400)
        size = min(size, 100)

        last_work_seen = None
        last_id = arg('after', None)
        if last_id:
            try:
                last_id = int(last_id)
            except ValueError:
                return problem("Invalid work ID: %s" % last_id, 400)
            try:
                last_work_seen = Conf.db.query(Work).filter(Work.id==last_id).one()
            except NoResultFound:
                return problem("No such work id: %s" % last_id, 400)

        this_url = url_for('feed', lane=lane.name, order=order, _external=True)
        page = work_feed.page_query(Conf.db, last_work_seen, size).all()

        opds_feed = AcquisitionFeed(Conf.db, title, this_url, page,
                                    annotator, work_feed.active_facet)
        # Add a 'next' link if appropriate.
        if page and len(page) >= size:
            after = page[-1].id
            next_url = url_for(
                'feed', lane=lane.name, order=order, after=after, _external=True)
            opds_feed.add_link(rel="next", href=next_url)

        opds_feed.add_link(**search_link)

    feed_xml = unicode(opds_feed)
    if not last_seen_id:
        feed_cache[key] = (feed_xml, time.time())
    return feed_xml

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
    this_url = url_for('popular_feed', lane_name=lane_name, _external=True)

    key = ("popular", lane_name, ",".join(languages))
    if key in feed_cache:
        chance = random.random()
        feed, created_at = feed_cache.get(key)
        if chance > 0.10:
            # Return the cached version.
            return feed

    title = "%s: Best Sellers" % lane_display_name
    # TODO: Can't sort by most recent appearance.
    work_feed = AllCustomListsFromDataSourceFeed(
        Conf.db, [DataSource.NYT], languages, availability=AllCustomListsFromDataSourceFeed.ALL)
    annotator = CirculationManagerAnnotator(lane)
    page = work_feed.page_query(Conf.db, None, 100).all()
    page = random.sample(page, min(len(page), 20))
    opds_feed = AcquisitionFeed(Conf.db, title, this_url, page,
                                annotator, work_feed.active_facet)
    feed_xml = unicode(opds_feed)
    feed_cache[key] = (feed_xml, time.time())
    return unicode(feed_xml)

@app.route('/search', defaults=dict(lane=None))
@app.route('/search/', defaults=dict(lane=None))
@app.route('/search/<lane>')
def lane_search(lane):
    languages = languages_for_request()
    query = flask.request.args.get('q')
    if lane:
        lane = Conf.sublanes.by_name[lane]    
    else:
        # Create a synthetic Lane that includes absolutely everything.
        lane = Lane.everything(Conf.db)
    this_url = url_for('lane_search', lane=lane.name, _external=True)
    if not query:
        # Send the search form
        return OpenSearchDocument.for_lane(lane, this_url)
    # Run a search.    
    results = lane.search(languages, query).limit(50)
    info = OpenSearchDocument.search_info(lane)
    opds_feed = AcquisitionFeed(
        Conf.db, info['name'], 
        this_url + "?q=" + urllib.quote(query),
        results, CirculationManagerAnnotator(lane))
    return unicode(opds_feed)

@app.route('/works/')
def work():
    annotator = CirculationManagerAnnotator(None)
    return URNLookupController(Conf.db).work_lookup(annotator, 'work')
    # Conf.urn_lookup_controller.permalink(urn, annotator)

@app.route('/works/<data_source>/<identifier>/checkout')
@requires_auth
def checkout(data_source, identifier):

    patron = flask.request.patron

    # Turn source + identifier into a LicensePool
    source = DataSource.lookup(Conf.db, data_source)
    if source is None:
        return problem("No such data source!", 404)
    identifier_type = source.primary_identifier_type

    id_obj, ignore = Identifier.for_foreign_id(
        Conf.db, identifier_type, identifier, autocreate=False)
    if not id_obj:
        # TODO
        return problem(
            NO_LICENSES_PROBLEM, "I never heard of such a book.", 404)

    pool = id_obj.licensed_through
    if not pool:
        return problem(
            NO_LICENSES_PROBLEM, 
            "I don't have any licenses for that book.", 404)

    if pool.open_access:
        best_pool, best_link = pool.best_license_link
        if not best_link:
            return problem(
                NO_LICENSES_PROBLEM,
                "Sorry, couldn't find an open-access download link.", 404)
        best_pool.loan_to(patron)
        return redirect(best_link.representation.mirror_url)

    # This is not an open-access pool.

    possible_formats = [None]
    if pool.data_source.name==DataSource.OVERDRIVE:
        api = Conf.overdrive
        possible_formats = ["ebook-epub-adobe", "ebook-epub-open"]
    else:
        api = Conf.threem
    content_link = None
    content_type = None
    status_code = None

    # Try to find an existing loan.
    loan = get_one(Conf.db, Loan, patron=patron, license_pool=pool)
    header = flask.request.authorization
    location = None
    if loan:
        # The loan already exists. Try to send the patron a
        # fulfillment document, or a link to one.
        for f in possible_formats:
            location, media_type, content = api.fulfill(
                patron, header.password, id_obj, f)
            if location or content:
                break
        else:
            return problem(
                NO_ACCEPTABLE_FORMAT_PROBLEM,
                "Could not find this book in a usable format.", 500)
        if location:
            status_code = 302
        else:
            status_code = 200
    else:
        content_expires = None
        # There is no existing loan. At this point there must be a license
        # free to assign to this patron.
        if pool.licenses_available < 1:
            return problem(
                NO_AVAILABLE_LICENSE_PROBLEM,
                "Sorry, all copies of this book are checked out.", 400)

        try:
            header = flask.request.authorization
            format_to_use = possible_formats[0]
            content_link, content_type, content, content_expires = api.checkout(
                patron, header.password, pool.identifier, format_to_use)
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

        pool.loan_to(patron, end=content_expires)
        status_code = 201
    headers = {}
    if location:
        headers["Location"] = location
    if media_type:
        headers['Content-Type'] = media_type
    return Response(content, status_code, headers)

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
