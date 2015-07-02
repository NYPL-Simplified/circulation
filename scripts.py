from nose.tools import set_trace
from cStringIO import StringIO
import csv
import json
import os
import sys
from datetime import timedelta
from sqlalchemy import or_
from lanes import make_lanes
import urlparse
from core.model import (
    Contribution,
    CustomList,
    DataSource,
    Edition,
    Hyperlink,
    Identifier,
    Representation,
)
from core.scripts import Script
from core.opds_import import (
    SimplifiedOPDSLookup,
    DetailedOPDSImporter,
    BaseOPDSImporter,
)
from core.opds import (
    OPDSFeed,
    AcquisitionFeed,
)
from core.external_list import CustomListFromCSV
from core.external_search import ExternalSearchIndex
from opds import CirculationManagerAnnotator
import app
import time

class CreateWorksForIdentifiersScript(Script):

    """Do the bare minimum to associate each Identifier with an Edition
    with title and author, so that we can calculate a permanent work
    ID.
    """
    to_check = [Identifier.OVERDRIVE_ID, Identifier.THREEM_ID,
                Identifier.GUTENBERG_ID]
    BATCH_SIZE = 100

    def __init__(self, metadata_web_app_url=None):
        self.metadata_url = (metadata_web_app_url
                             or os.environ['METADATA_WEB_APP_URL'])
        self.lookup = SimplifiedOPDSLookup(self.metadata_url)

    def run(self):

        # We will try to fill in Editions that are missing
        # title/author and as such have no permanent work ID.
        #
        # We will also try to create Editions for Identifiers that
        # have no Edition.

        either_title_or_author_missing = or_(
            Edition.title == None,
            Edition.sort_author == None,
        )
        edition_missing_title_or_author = self._db.query(Identifier).join(
            Identifier.primarily_identifies).filter(
                either_title_or_author_missing)

        no_edition = self._db.query(Identifier).filter(
            Identifier.primarily_identifies==None).filter(
                Identifier.type.in_(self.to_check))

        for q in (edition_missing_title_or_author, no_edition):
            batch = []
            print "%d total." % q.count()
            for i in q:
                batch.append(i)
                if len(batch) >= self.BATCH_SIZE:
                    self.process_batch(batch)
                    batch = []

    def process_batch(self, batch):
        print "%d batch" % len(batch)
        response = self.lookup.lookup(batch)
        print "Response!"

        if response.status_code != 200:
            raise Exception(response.text)
            
        content_type = response.headers['content-type']
        if content_type != OPDSFeed.ACQUISITION_FEED_TYPE:
            raise Exception("Wrong media type: %s" % content_type)

        importer = DetailedOPDSImporter(
            self._db, response.text,
            overwrite_rels=[Hyperlink.DESCRIPTION, Hyperlink.IMAGE])
        imported, messages_by_id = importer.import_from_feed()
        print "%d successes, %d failures." % (len(imported), len(messages_by_id))
        self._db.commit()

class MetadataCalculationScript(Script):

    """Force calculate_presentation() to be called on some set of Editions.

    This assumes that the metadata is in already in the database and
    will fall into place if we just call
    Edition.calculate_presentation() and Edition.calculate_work() and
    Work.calculate_presentation().

    Most of these will be data repair scripts that do not need to be run
    regularly.

    """

    def q(self):
        raise NotImplementedError()

    def run(self):
        q = self.q()
        search_index_client = ExternalSearchIndex()
        print "Attempting to repair %d" % q.count()

        success = 0
        failure = 0
        also_created_work = 0

        def checkpoint():
            self._db.commit()
            print "%d successes, %d failures, %d new works." % (
                success, failure, also_created_work)

        i = 0
        for edition in q:
            edition.calculate_presentation()
            if edition.sort_author:
                success += 1
                work, is_new = edition.license_pool.calculate_work(
                    search_index_client=search_index_client)
                if work:
                    work.calculate_presentation()
                    if is_new:
                        also_created_work += 1
            else:
                failure += 1
            i += 1
            if not i % 1000:
                checkpoint()
        checkpoint()

class FillInAuthorScript(MetadataCalculationScript):
    """Fill in Edition.sort_author for Editions that have a list of
    Contributors, but no .sort_author.

    This is a data repair script that should not need to be run
    regularly.
    """

    def q(self):
        return self._db.query(Edition).join(
            Edition.contributions).join(Contribution.contributor).filter(
                Edition.sort_author==None)

class UpdateStaffPicksScript(Script):

    DEFAULT_URL_TEMPLATE = "https://docs.google.com/spreadsheets/d/%s/export?format=csv"

    def run(self):
        key = os.environ['STAFF_PICKS_GOOGLE_SPREADSHEET_KEY']
        if key.startswith('https://') or key.startswith('http://'):
            # It's a custom URL, not a Google spreadsheet key.
            # Leave it alone.
            pass
        else:
            url = self.DEFAULT_URL_TEMPLATE % key
        metadata_client = None
        representation, cached = Representation.get(
            self._db, url, do_get=Representation.browser_http_get,
            accept="text/csv", max_age=timedelta(days=1))
        if representation.status_code != 200:
            raise ValueError("Unexpected status code %s" % 
                             representation.status_code)
            return
        if not representation.media_type.startswith("text/csv"):
            raise ValueError("Unexpected media type %s" % 
                             representation.media_type)
            return
        importer = CustomListFromCSV(
            DataSource.LIBRARY_STAFF, CustomList.STAFF_PICKS_NAME)
        reader = csv.DictReader(StringIO(representation.content))
        writer = csv.writer(sys.stdout)
        importer.to_customlist(self._db, reader, writer)
        self._db.commit()


class ContentOPDSImporter(BaseOPDSImporter):

    # The content server is the canonical source for open-access
    # links, but not for anything else.
    OVERWRITE_RELS = [Hyperlink.OPEN_ACCESS_DOWNLOAD]

    def __init__(self, _db, feed):
        super(ContentOPDSImporter, self).__init__(
            _db, feed, overwrite_rels=self.OVERWRITE_RELS)


class StandaloneApplicationConf(object):
    """A fake application config object.

    This lets us pretend to be a running application and generate
    the URLs the application would generate.
    """

    def __init__(self, _db):
        self.db = _db
        self.parent = None
        self.sublanes = make_lanes(self.db)
        self.name = None
        self.display_name = None


class LaneSweeperScript(Script):
    """Do something to each lane in the application."""

    # These languages are NYPL's four major ebook collections.
    PRIMARY_COLLECTIONS = json.loads(os.environ['PRIMARY_COLLECTION_LANGUAGES'])
    OTHER_COLLECTIONS = json.loads(os.environ['OTHER_COLLECTION_LANGUAGES'])

    def __init__(self, languages=None):
        self.conf = StandaloneApplicationConf(self._db)
        self.languages = languages or (
            self.PRIMARY_COLLECTIONS + self.OTHER_COLLECTIONS)
        self.base_url = os.environ['CIRCULATION_WEB_APP_URL']

    def run(self):
        begin = time.time()
        client = app.app.test_client()
        ctx = app.app.test_request_context(base_url=self.base_url)
        ctx.push()
        queue = [self.conf]
        while queue:
            new_queue = []
            print "!! Beginning of loop: %d lanes to process" % len(queue)
            for l in queue:
                if self.should_process_lane(l):
                    self.process_lane(l)
                    self._db.commit()
                for sublane in l.sublanes:
                    new_queue.append(sublane)
            queue = new_queue
        ctx.pop()
        end = time.time()
        print "!!! Entire process took %.2fsec" % (end-begin)

    def should_process_lane(self, lane):
        return True

    def process_lane(self, lane):
        pass


class CacheRepresentationPerLane(LaneSweeperScript):

    def cache_url(self, annotator, lane, languages):
        raise NotImplementedError()

    def generate_representation(self, *args, **kwargs):
        raise NotImplementedError()

    # The generated document will probably be an OPDS acquisition
    # feed.
    ACCEPT_HEADER = OPDSFeed.ACQUISITION_FEED_TYPE

    cache_url_method = None

    def generate_feed(self, cache_url, get_method, max_age=0):
        a = time.time()
        print "!!! Generating %s." % (cache_url)
        feed_rep, ignore = Representation.get(
            self._db, cache_url, get_method,
            accept=self.ACCEPT_HEADER, max_age=0)
        b = time.time()
        if feed_rep.fetch_exception:
            print "!!! EXCEPTION: %s" % feed_rep.fetch_exception
        if feed_rep.content:
            content_bytes = len(feed_rep.content)
        else:
            content_bytes = 0
        print "!!! Generated %r. Took %.2fsec to make %d bytes." % (
            cache_url, (b-a), content_bytes)
        print        

    def process_lane(self, lane):
        annotator = CirculationManagerAnnotator(lane)
        for languages in self.languages:
            cache_url = self.cache_url(annotator, lane, languages)
            get_method = self.make_get_method(annotator, lane, languages)
            self.generate_feed(cache_url, get_method)

class CacheFacetListsPerLane(CacheRepresentationPerLane):
    """Cache the first two pages of every facet list for this lane."""

    def should_process_lane(self, lane):
        return lane.name is not None

    def process_lane(self, lane):
        annotator = CirculationManagerAnnotator(lane)
        size = 50
        for languages in self.languages:
            for facet in app.order_field_to_database_field.keys():
                self.last_work_seen = None
                for i in range(2):
                    url = app.feed_cache_url(
                        lane, languages, facet, self.last_work_seen, size)
                    def get_method(*args, **kwargs):
                        feed, self.last_work_seen = app.feed_and_last_work_seen(
                            self._db, annotator, lane, languages, facet,
                            self.last_work_seen, size)
                        return feed
                    self.generate_feed(url, get_method, 10*60)


class CacheOPDSGroupFeedPerLane(CacheRepresentationPerLane):

    def should_process_lane(self, lane):
        # OPDS group feeds are only generated for lanes that have sublanes.
        return lane.sublanes

    def cache_url(self, annotator, lane, languages):
        return app.acquisition_groups_cache_url(annotator, lane, languages)

    def make_get_method(self, annotator, lane, languages):
        def get_method(*args, **kwargs):
            return app.make_acquisition_groups(annotator, lane, languages)
        return get_method


class CacheTopLevelOPDSGroupFeeds(CacheOPDSGroupFeedPerLane):
    """Refresh the cache of top-level OPDS groups.

    These are frequently accessed, so should be updated more often.
    """

    def should_process_lane(self, lane):
        # Only handle the top-level lanes
        return (super(
            CacheTopLevelOPDSGroupFeeds, self).should_process_lane(lane) 
                and not lane.parent)


class CacheLowLevelOPDSGroupFeeds(CacheOPDSGroupFeedPerLane):
    """Refresh the cache of lower-level OPDS groups.

    These are less frequently accessed, so can be updated less often.
    """

    def should_process_lane(self, lane):
        # Only handle the lower-level lanes
        return (super(
            CacheLowLevelOPDSGroupFeeds, self).should_process_lane(lane) 
                and lane.parent)


class CacheIndividualLaneFeaturedFeeds(CacheRepresentationPerLane):

    def cache_url(self, annotator, lane, languages):
        return app.featured_feed_cache_url(annotator, lane, languages)

    def make_get_method(self, annotator, lane, languages):
        def get_method(*args, **kwargs):
            return app.make_featured_feed(annotator, lane, languages)
        return get_method

    def should_process_lane(self, lane):
        # Every lane deserves a featured feed.
        return True

class CacheBestSellerFeeds(CacheRepresentationPerLane):
    """Cache the complete feed of best-sellers for each top-level lanes."""

    PRIMARY_COLLECTIONS = [[x] for x in AcquisitionFeed.BEST_SELLER_LANGUAGES]
    OTHER_COLLECTIONS = []

    def cache_url(self, annotator, lane, languages):
        return app.popular_feed_cache_url(annotator, lane, languages)

    def make_get_method(self, annotator, lane, languages):
        def get_method(*args, **kwargs):
            return app.make_popular_feed(self._db, annotator, lane, languages)
        return get_method

    def should_process_lane(self, lane):
        # Only process the top-level lanes.
        return not lane.parent

class CacheStaffPicksFeeds(CacheRepresentationPerLane):
    """Cache the complete feed of staff picks for each top-level lane."""

    PRIMARY_COLLECTIONS = [[x] for x in AcquisitionFeed.STAFF_PICKS_LANGUAGES]
    OTHER_COLLECTIONS = []

    def cache_url(self, annotator, lane, languages):
        return app.staff_picks_feed_cache_url(annotator, lane, languages)

    def make_get_method(self, annotator, lane, languages):
        def get_method(*args, **kwargs):
            return app.make_staff_picks_feed(
                self._db, annotator, lane, languages)
        return get_method

    def should_process_lane(self, lane):
        # Only process the top-level lanes.
        return not lane.parent
