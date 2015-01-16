from functools import wraps
from nose.tools import set_trace
import random
import time
import os
import sys

from sqlalchemy.orm.exc import (
    NoResultFound
)

import flask
from flask import Flask, url_for, redirect, Response

from circulation_exceptions import (
    NoAvailableCopies,
)
from core.app_server import URNLookupController
from core.overdrive import (
    OverdriveAPI
)

from core.model import (
    get_one_or_create,
    DataSource,
    production_session,
    LaneList,
    Lane,
    LicensePool,
    Patron,
    Identifier,
    Work,
    WorkFeed,
    Edition,
    )
from core.opensearch import OpenSearchDocument
from opds import CirculationManagerAnnotator
from core.opds import (
    E,
    AcquisitionFeed,
    NavigationFeed,
    URLRewriter,
)
import urllib
from core.util.flask_util import (
    problem,
    languages_for_request
)
from integration.millenium_patron import (
    DummyMilleniumPatronAPI as authenticator,
)
import core.classifier as genres
from core.classifier import (
    Classifier,
    GenreData,
)

auth = authenticator()

feed_cache = dict()

class Conf:
    db = None
    sublanes = None
    name = None
    parent = None
    urn_lookup_controller = None

    @classmethod
    def initialize(cls, _db, lanes):
        cls.db = _db
        cls.sublanes = lanes
        cls.urn_lookup_controller = URNLookupController(cls.db)

if os.environ.get('TESTING') == "True":
    Conf.testing = True
else:
    Conf.testing = False
    _db = production_session()

    # art = Lane(_db, name="Art & Design",
    #            genres = [genres.Art_Architecture_Design],
    #            include_subgenres=True,
    #            fiction=False,
    #            audience=Classifier.AUDIENCE_ADULT,
    #                sublanes=[
    #                    genres.Art,
    #                    genres.Design,
    #                    genres.Architecture,
    #                    genres.Fashion,
    #                    genres.Photography,
    #            ),

    fiction = Lane(_db, name="Fiction",
             fiction=True,
             audience=genres.Classifier.AUDIENCE_ADULT,
             genres=Lane.UNCLASSIFIED,
             sublanes=[
                 Lane(_db, name="General Fiction",
                      genres=Lane.UNCLASSIFIED),
                 Lane(_db, name="Literary Fiction",
                      genres=[genres.Literary_Fiction],
                      include_subgenres=False
                  ),
                 Lane(_db, name="Literary Collections",
                      genres=[genres.Literary_Collections]),
                 genres.Classics,
             ],
         )

    hobbies_and_games = Lane(
        _db, name="Hobbies & Home",
        genres = [
            genres.Crafts_Cooking_Garden,
            genres.Crafts_Hobbies_Games,
            genres.Antiques_Collectibles,
            genres.Gardening,
            genres.House_Home,
            genres.Pets,
        ],
        include_subgenres=False,
        fiction=False,
        audience=Classifier.AUDIENCE_ADULT,
        sublanes=[
            Lane(_db, name="Crafts, Hobbies & Games",
                 genres = [
                     genres.Crafts_Hobbies_Games,
                     genres.Antiques_Collectibles,
                 ],
                 include_subgenres=False,
                 fiction=False,
             ),
            genres.House_Home,
            genres.Gardening,
            genres.Pets,
        ],
    )

    religion = Lane(
        _db, name="Religion & Spirituality",
        genres = [genres.Religion_Spirituality],
        include_subgenres=True,
        fiction=Lane.BOTH_FICTION_AND_NONFICTION,
        audience=Classifier.AUDIENCE_ADULT,
        sublanes=[
            Lane(_db, name="General Religion & Spirituality",
                 genres=[genres.Religion_Spirituality, genres.Body_Mind_Spirit,
                         genres.Hinduism],
                 include_subgenres=False, fiction=False),
            genres.Buddhism,
            Lane(_db, name="Christianity",
                 genres=[genres.Christianity],
                 fiction=False),
            genres.Islam,
            genres.Judaism,
            genres.New_Age,
            Lane(_db, name="Religious Fiction",
                 genres=[genres.Religion_Spirituality,
                         genres.Body_Mind_Spirit,
                         genres.Religious_Fiction,
                         genres.Christianity],
                 include_subgenres=False,
                 fiction=True,
             ),
        ],
    )

    science = Lane(_db, name="Science & Tech",
                   genres = [genres.Science_Technology_Nature],
                   include_subgenres=True,
                   fiction=False,
                   audience=Classifier.AUDIENCE_ADULT,
                   sublanes=[
                       genres.Science,
                       genres.Nature,
                       Lane(_db, name="Technology",
                            genres=[genres.Technology_Engineering],
                            include_subgenres=False,
                            fiction=False),
                       genres.Computers,
                       Lane(_db, name="Social Science",
                            genres=[genres.Social_Science],
                            include_subgenres=False),
                       genres.Psychology,
                       genres.Mathematics,
                       genres.Medical,
                   ],
               )

    philosophy = Lane(
        _db, name="Criticism & Philosophy",
        genres = [genres.Criticism_Philosophy],
        include_subgenres=True,
        fiction=False,
        audience=Classifier.AUDIENCE_ADULT,
        sublanes=[
            genres.Literary_Criticism,
            genres.Philosophy,
            genres.Language_Arts_Disciplines,
        ])            

    mystery = Lane(_db, name="Crime, Thrillers & Mystery",
                   genres = [genres.Crime_Thrillers_Mystery],
                   include_subgenres=True,
                   fiction=Lane.BOTH_FICTION_AND_NONFICTION,
                   audience=Classifier.AUDIENCE_ADULT,
                   sublanes=[
                       genres.Mystery,
                       genres.Women_Detectives,
                       genres.Police_Procedurals,
                       genres.Thrillers,
                       Lane(_db, name="True Crime",
                            genres=[genres.True_Crime], fiction=False),
                   ],
               )

    non_erotica_romance = [
        genres.Romance, genres.Contemporary_Romance,
        genres.Historical_Romance, genres.Paranormal_Romance,
        genres.Regency_Romance, genres.Suspense_Romance
    ]
    romance = Lane(_db, name="Romance",
              genres=non_erotica_romance,
              include_subgenres=False,
              fiction=True,
              audience=Classifier.AUDIENCE_ADULT,
              sublanes=[
                  Lane(_db, name="General Romance",
                       genres=[genres.Romance, genres.Contemporary_Romance]),
                  Lane(_db, name="Story-Driven Romance",
                       genres=non_erotica_romance, include_subgenres=False,
                       appeal=Work.STORY_APPEAL),
                  Lane(_db, name="Character-Driven Romance",
                       genres=non_erotica_romance, include_subgenres=False,
                       appeal=Work.CHARACTER_APPEAL),
                  Lane(_db, name="Setting-Driven Romance",
                       genres=non_erotica_romance, include_subgenres=False,
                       appeal=Work.SETTING_APPEAL),
                  Lane(_db, name="Language-Driven Romance",
                       genres=non_erotica_romance, include_subgenres=False,
                       appeal=Work.LANGUAGE_APPEAL),

                  genres.Historical_Romance,
                  genres.Paranormal_Romance,
                  genres.Regency_Romance,
                  genres.Suspense_Romance,
                  genres.Erotica,
              ],
          )

    food = Lane(_db, name="Food and Health",
                genres=[
                    genres.Cooking,
                    genres.Health_Diet],
                fiction=False,
                include_subgenres=True,
                sublanes=[
                    Lane(_db, name="Cooking", genres=[genres.Cooking]),
                    genres.Health_Diet,
                    genres.Vegetarian_Vegan,
                    genres.Bartending_Cocktails,
                ]
            )

    family = Lane(
        _db, name="Parenting & Family",
        genres=[genres.Parenting_Family],
        include_subgenres=True,
        fiction=False,
        sublanes=[
            genres.Parenting,
            genres.Family_Relationships,
            genres.Education,
        ]
    )

    reference = Lane(
        _db, name="Reference & Study Aids",
        genres=[genres.Reference],
        include_subgenres=True,
        fiction=False,
        sublanes=[
            genres.Study_Aids,
            genres.Foreign_Language_Study,
            Lane(_db, name="General Reference",
                 genres=[genres.Reference, genres.Encyclopedias],
                 include_subgenres=False),
            genres.Law,
            genres.Dictionaries,
        ],

    )

    business = Lane(
        _db, name="Personal Finance & Business",
        genres=[genres.Business_Economics],
        include_subgenres=True,
        fiction=False,
        sublanes=[
            genres.Personal_Finance_Investing,
            Lane(_db, name="Business", genres=[genres.Business_Economics],
                 include_subgenres=False),
            genres.Economics,
            genres.Management_Leadership,
        ],
    )

    humor = Lane(
        _db, name="Humor & Entertainment",
        genres=[genres.Humor_Entertainment],
        include_subgenres=True,
        fiction=False,
        sublanes=[
            Lane(_db, name="Humor", genres=[genres.Humor],
                 fiction=Lane.BOTH_FICTION_AND_NONFICTION),
            Lane(_db, name="Music", genres=[genres.Music]),
            Lane(_db, name="Film & TV", genres=[genres.Film_TV]),
            Lane(_db, name="Performing Arts",
                 genres=[genres.Performing_Arts, genres.Dance]),
        ],
    )

    travel = Lane(
        _db, name="Travel, Adventure & Sports",
        genres=[genres.Travel_Adventure_Sports],
        include_subgenres=True,
        fiction=False,
        sublanes=[
            Lane(_db, name="Travel", genres=[genres.Travel],
                 fiction=Lane.BOTH_FICTION_AND_NONFICTION),
            Lane(_db, name="Adventure Fiction",
                 genres=[genres.Action_Adventure], fiction=True),
            genres.Sports,
            genres.Transportation,
        ],
    )

    poetry_drama = Lane(
        _db, name="Poetry & Drama",
        genres=[genres.Poetry, genres.Drama],
        fiction=Lane.BOTH_FICTION_AND_NONFICTION,
        sublanes=[
            genres.Poetry,
            genres.Drama,
        ],
    )

    YA = genres.Classifier.AUDIENCE_YOUNG_ADULT
    CHILDREN = genres.Classifier.AUDIENCE_CHILDREN
    lanes = LaneList.from_description(
        _db,
        None,
        [fiction,
         genres.Biography_Memoir,
         mystery,
         genres.Fantasy,
         genres.Science_Fiction,
         genres.Historical_Fiction,
         genres.Horror,
         food,
         romance,
         science,
         genres.Self_Help,
         genres.Graphic_Novels_Comics,
         reference,
         dict(
             name="Young Adult",
             fiction=Lane.BOTH_FICTION_AND_NONFICTION,
             audience=YA,
             genres=[],
             sublanes=[
                 Lane(_db, name="Young Adult Fiction", genres=Lane.UNCLASSIFIED,
                      fiction=True, audience=YA),
                 Lane(_db, name="Young Adult Fantasy", genres=[genres.Fantasy],
                      audience=YA),
                 Lane(_db, name="Young Adult Romance", genres=[genres.Romance],
                      audience=YA),
                 Lane(_db, name="Young Adult Nonfiction",
                      fiction=False, genres=[], audience=YA),
                 Lane(_db, name="Young Adult Science Fiction",
                      genres=[genres.Science_Fiction], audience=YA),
                 Lane(_db, name="Young Adult Mystery",
                      genres=[genres.Crime_Thrillers_Mystery],
                      audience=YA),
                 Lane(_db, name="Young Adult Historical Fiction",
                      genres=[genres.Historical_Fiction],
                      audience=YA),
                 Lane(_db, name="Young Adult Horror",
                      genres=[genres.Horror], audience=YA),
                 Lane(_db, name="Young Adult Graphic Novels & Comics",
                      genres=[genres.Graphic_Novels_Comics], audience=YA),

             ]),
         dict(
             name="Children",
             fiction=Lane.BOTH_FICTION_AND_NONFICTION,
             audience=genres.Classifier.AUDIENCE_CHILDREN,
             genres=[],
             sublanes=[
                 Lane(_db, name="Juvenile Fiction", genres=Lane.UNCLASSIFIED,
                      fiction=True, audience=CHILDREN),
                 Lane(_db, name="Juvenile Nonfiction",
                      fiction=False, genres=[], audience=CHILDREN),
                 Lane(_db, name="Fantasy (Children)", genres=[genres.Fantasy],
                      audience=CHILDREN),
                 Lane(_db, name="Humor (Children)", genres=[genres.Humor],
                      audience=CHILDREN),
                 Lane(_db, name="Mystery (Children)", genres=[genres.Mystery],
                      audience=CHILDREN),
                 Lane(_db, name="Historical Fiction (Children)", genres=[genres.Historical_Fiction],
                      audience=CHILDREN),
                 Lane(_db, name="Science (Children)",
                      fiction=False, genres=[genres.Science], audience=CHILDREN),
             ]),
         dict(
             fiction=Lane.BOTH_FICTION_AND_NONFICTION,
             name="African-American",
             genres=[genres.African_American, genres.Urban_Fiction],
         ),
         dict(
             name="Art & Design",
             genres=[genres.Art_Architecture_Design],
         ),
         hobbies_and_games,
         humor,
         family,
         religion,
         philosophy,
         business,
         genres.Politics_Current_Events,
         travel,
         poetry_drama,
         dict(name="Unclassified Nonfiction",
              fiction=False,
              audience=genres.Classifier.AUDIENCE_ADULT,
              genres=Lane.UNCLASSIFIED),
         dict(
             name="Unclassified General",
             fiction=Lane.UNCLASSIFIED,
             genres=Lane.UNCLASSIFIED,
             audience=genres.Classifier.AUDIENCE_ADULT),
     ]
    )
    Conf.initialize(_db, lanes)

app = Flask(__name__)
app.config['DEBUG'] = True
app.debug = True

INVALID_CREDENTIALS_PROBLEM = "http://library-simplified.com/problem/credentials-invalid"
INVALID_CREDENTIALS_TITLE = "A valid library card barcode number and PIN are required."
EXPIRED_CREDENTIALS_PROBLEM = "http://library-simplified.com/problem/credentials-expired"
EXPIRED_CREDENTIALS_TITLE = "Your library card has expired. You need to renew it."
NO_AVAILABLE_LICENSE_PROBLEM = "http://library-simplified.com/problem/no-license"

def authenticated_patron(barcode, pin):
    """Look up the patron authenticated by the given barcode/pin.

    If there's a problem, return a 2-tuple (URI, title) for use in a
    Problem Detail Document.

    If there's no problem, return a Patron object.
    """
    patron = auth.authenticated_patron(Conf.db, barcode, pin)
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

    # First synchronize our local list of loans with all third-party
    # loan providers.
    header = flask.request.authorization
    # TODO: this is a hack necessary so long as we use dummy auth,
    # because Overdrive always asks the real ILS for the real barcode.
    # If you use a test barcode, we want /loans to act like you have no
    # Overdrive loans; we don't want it to crash.
    try:
        overdrive = OverdriveAPI(Conf.db)
        overdrive_loans = overdrive.get_patron_checkouts(
            flask.request.patron, header.password)
        OverdriveAPI.sync_bookshelf(flask.request.patron, overdrive_loans)
        Conf.db.commit()
    except Exception, e:
        print e

    # Then make the feed.
    feed = CirculationManagerAnnotator.active_loans_for(flask.request.patron)
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
            languages, lane, annotator)
        opds_feed.add_link(**search_link)
        work_feed = None
    elif order == 'title':
        work_feed = WorkFeed(lane, languages, Edition.sort_title)
        title = "%s: By title" % lane.name
    elif order == 'author':
        work_feed = WorkFeed(lane, languages, Edition.sort_author)
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

@app.route('/works/<urn>')
def work(urn):
    Conf.urn_lookup_controller.permalink(urn)

@app.route('/works/<data_source>/<identifier>/checkout')
@requires_auth
def checkout(data_source, identifier):

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
            NO_AVAILABLE_LICENSE_PROBLEM, "I never heard of such a book.", 404)

    pool = id_obj.licensed_through
    if not pool:
        return problem(
            NO_AVAILABLE_LICENSE_PROBLEM, 
            "I don't have any licenses for that book.", 404)

    if pool.open_access:
        best_pool, best_link = pool.best_license_link
        if not best_link:
            return problem(
                NO_AVAILABLE_LICENSE_PROBLEM,
                "Sorry, couldn't find an available license.", 404)
        best_pool.loan_to(flask.request.patron)
        return redirect(URLRewriter.rewrite(best_link.href))

    # This is not an open-access pool.
    if pool.licenses_available < 1:
        return problem(
            NO_AVAILABLE_LICENSE_PROBLEM,
            "Sorry, couldn't find an available license.", 400)

    content_link = None
    content_type = None
    content_expires = None
    if pool.data_source.name==DataSource.OVERDRIVE:
        api = OverdriveAPI(_db)
        header = flask.request.authorization
        try:
            content_link, content_type, content_expires = api.checkout(
                flask.request.patron, header.password,
                pool.identifier.identifier)
        except NoAvailableCopies:
            return problem(
                NO_AVAILABLE_LICENSE_PROBLEM,
                "Sorry, couldn't find an available license.", 400)

    pool.loan_to(flask.request.patron, end=content_expires)
    headers = { "Location" : content_link }
    return Response(data, 201, headers)

print __name__
if __name__ == '__main__':

    debug = True
    host = "0.0.0.0"
    app.run(debug=debug, host=host)
