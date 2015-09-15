from cStringIO import StringIO
from datetime import timedelta
from nose.tools import set_trace
import csv
import json
import os
import sys
import time
import urlparse

from sqlalchemy import or_

from lanes import make_lanes
from core import log
from core.model import (
    Contribution,
    CustomList,
    DataSource,
    Edition,
    Hyperlink,
    Identifier,
    Representation,
    Subject,
)
from core.scripts import Script as CoreScript
from config import Configuration
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

class Script(CoreScript):
    def load_config(self):
        if not Configuration.instance:
            Configuration.load()

class CreateWorksForIdentifiersScript(Script):

    """Do the bare minimum to associate each Identifier with an Edition
    with title and author, so that we can calculate a permanent work
    ID.
    """
    to_check = [Identifier.OVERDRIVE_ID, Identifier.THREEM_ID,
                Identifier.GUTENBERG_ID]
    BATCH_SIZE = 100
    name = "Create works for identifiers"

    def __init__(self, metadata_web_app_url=None):
        self.metadata_url = (
            metadata_web_app_url or Configuration.integration_url(
                Configuration.METADATA_WRANGLER_INTEGRATION
            )
        )
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

        for q, descr in (
                (edition_missing_title_or_author,
                 "identifiers whose edition is missing title or author"),
                (no_edition, "identifiers with no edition")):
            batch = []
            self.log.debug("Trying to fix %d %s", q.count(), descr)
            for i in q:
                batch.append(i)
                if len(batch) >= self.BATCH_SIZE:
                    self.process_batch(batch)
                    batch = []

    def process_batch(self, batch):
        response = self.lookup.lookup(batch)

        if response.status_code != 200:
            raise Exception(response.text)
            
        content_type = response.headers['content-type']
        if content_type != OPDSFeed.ACQUISITION_FEED_TYPE:
            raise Exception("Wrong media type: %s" % content_type)

        importer = DetailedOPDSImporter(
            self._db, response.text,
            overwrite_rels=[Hyperlink.DESCRIPTION, Hyperlink.IMAGE])
        imported, messages_by_id = importer.import_from_feed()
        self.log.info("%d successes, %d failures.", 
                      len(imported), len(messages_by_id))
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

    name = "Metadata calculation script"

    def q(self):
        raise NotImplementedError()

    def run(self):
        q = self.q()
        search_index_client = ExternalSearchIndex()
        self.log.info("Attempting to repair metadata for %d works" % q.count())

        success = 0
        failure = 0
        also_created_work = 0

        def checkpoint():
            self._db.commit()
            self.log.info("%d successes, %d failures, %d new works.",
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

    name = "Fill in missing authors"

    def q(self):
        return self._db.query(Edition).join(
            Edition.contributions).join(Contribution.contributor).filter(
                Edition.sort_author==None)

class UpdateStaffPicksScript(Script):

    DEFAULT_URL_TEMPLATE = "https://docs.google.com/spreadsheets/d/%s/export?format=csv"

    def run(self):
        inp = self.open()
        tag_fields = {
            'tags': Subject.NYPL_APPEAL,
        }

        integ = Configuration.integration(Configuration.STAFF_PICKS_INTEGRATION)
        fields = integ.get(Configuration.LIST_FIELDS, {})

        importer = CustomListFromCSV(
            DataSource.LIBRARY_STAFF, CustomList.STAFF_PICKS_NAME,
            **fields
        )
        reader = csv.DictReader(inp)
        importer.to_customlist(self._db, reader)
        self._db.commit()

    def open(self):
        if len(sys.argv) > 1:
            return open(sys.argv[1])

        url = Configuration.integration_url(
            Configuration.STAFF_PICKS_INTEGRATION, True
        )
        if not url.startswith('https://') or url.startswith('http://'):
            url = self.DEFAULT_URL_TEMPLATE % url
        self.log.info("Retrieving %s", url)
        representation, cached = Representation.get(
            self._db, url, do_get=Representation.browser_http_get,
            accept="text/csv", max_age=timedelta(days=1))
        if representation.status_code != 200:
            raise ValueError("Unexpected status code %s" % 
                             representation.status_code)
        if not representation.media_type.startswith("text/csv"):
            raise ValueError("Unexpected media type %s" % 
                             representation.media_type)
        return StringIO(representation.content)

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
        lane_list = Configuration.policy(Configuration.LANES_POLICY)
        self.sublanes = make_lanes(self.db, lane_list)
        self.name = None
        self.display_name = None

class LaneSweeperScript(Script):
    """Do something to each lane in the application."""

    PRIMARY_COLLECTIONS = 'primary'
    OTHER_COLLECTIONS = 'other'

    def __init__(self, languages=None):
        self.conf = StandaloneApplicationConf(self._db)
        language_policy = Configuration.policy("languages", {})
        primary_lang = language_policy['primary']
        other_lang = language_policy.get('other', [])
        if not languages:
            languages = primary_lang + other_lang
        elif languages == self.PRIMARY_COLLECTIONS:
            languages = primary_lang
        elif languages == self.OTHER_COLLECTIONS:
            languages = other_lang
        self.languages = languages
        self.base_url = Configuration.integration_url(
            Configuration.CIRCULATION_MANAGER_INTEGRATION, required=True
        )
        old_testing = os.environ.get('TESTING')
        # TODO: An awful hack to prevent the database from being
        # initialized twice.
        os.environ['TESTING'] = 'True'
        import app
        app.Conf.db = self._db
        if old_testing:
            os.environ['TESTING'] = old_testing
        else:
            del os.environ['TESTING']
        app.Conf.testing = False
        app.Conf.initialize()
        self.app = app

    def run(self):
        begin = time.time()
        client = self.app.app.test_client()
        ctx = self.app.app.test_request_context(base_url=self.base_url)
        ctx.push()
        queue = [self.conf]
        while queue:
            new_queue = []
            self.log.debug("Beginning of loop: %d lanes to process", len(queue))
            for l in queue:
                if self.should_process_lane(l):
                    self.process_lane(l)
                    self._db.commit()
                for sublane in l.sublanes:
                    new_queue.append(sublane)
            queue = new_queue
        ctx.pop()
        end = time.time()
        self.log.debug("Entire process took %.2fsec", (end-begin))

    def should_process_lane(self, lane):
        return True

    def process_lane(self, lane):
        pass


class CacheRepresentationPerLane(LaneSweeperScript):

    name = "Cache one representation per lane"

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
        self.log.debug("Generating %s.", cache_url)
        feed_rep, ignore = Representation.get(
            self._db, cache_url, get_method,
            accept=self.ACCEPT_HEADER, max_age=0)
        b = time.time()
        if feed_rep.fetch_exception:
            self.log.error(
                "Exception caching feed representation for %s: %s.",
                cache_url, feed_rep.fetch_exception
                )
        if feed_rep.content:
            content_bytes = len(feed_rep.content)
        else:
            content_bytes = 0
        self.log.info("Generated %s. Took %.2fsec to make %d bytes.",
            cache_url, (b-a), content_bytes)

    def process_lane(self, lane):
        annotator = CirculationManagerAnnotator(None, lane)
        for languages in self.languages:
            cache_url = self.cache_url(annotator, lane, languages)
            get_method = self.make_get_method(annotator, lane, languages)
            self.generate_feed(cache_url, get_method)

class CacheFacetListsPerLane(CacheRepresentationPerLane):
    """Cache the first two pages of every facet list for this lane."""

    name = "Cache first two pages of every facet list for each lane"

    def should_process_lane(self, lane):
        if lane.name is None:
            return False
            
        # TODO: This implies we're never showing "all young adult fiction",
        # which will change eventually.
        if lane.parent is None:
            return False

        return True

    def process_lane(self, lane):
        annotator = CirculationManagerAnnotator(None, lane)
        size = 50
        for languages in self.languages:
            for facet in ('title', 'author'):
                self.last_work_seen = None
                for offset in (0, size):
                    url = self.app.feed_cache_url(
                        lane, languages, facet, offset, size)
                    def get_method(*args, **kwargs):
                        return self.app.make_feed(
                            self._db, annotator, lane, languages, facet,
                            offset, size)
                    self.generate_feed(url, get_method, 10*60)


class CacheOPDSGroupFeedPerLane(CacheRepresentationPerLane):

    name = "Cache opds group feed for each lane"

    def should_process_lane(self, lane):
        # OPDS group feeds are only generated for lanes that have sublanes.
        return lane.sublanes

    def cache_url(self, annotator, lane, languages):
        return self.app.acquisition_groups_cache_url(annotator, lane, languages)

    def make_get_method(self, annotator, lane, languages):
        def get_method(*args, **kwargs):
            return self.app.make_acquisition_groups(annotator, lane, languages)
        return get_method


class CacheTopLevelOPDSGroupFeeds(CacheOPDSGroupFeedPerLane):
    """Refresh the cache of top-level OPDS groups.

    These are frequently accessed, so should be updated more often.
    """

    name = "Cache top-level OPDS group feeds"

    def should_process_lane(self, lane):
        # Only handle the top-level lanes
        return (super(
            CacheTopLevelOPDSGroupFeeds, self).should_process_lane(lane) 
                and not lane.parent)


class CacheLowLevelOPDSGroupFeeds(CacheOPDSGroupFeedPerLane):
    """Refresh the cache of lower-level OPDS groups.

    These are less frequently accessed, so can be updated less often.
    """
    name = "Cache low-level OPDS group feeds"

    def should_process_lane(self, lane):
        # Only handle the lower-level lanes
        return (super(
            CacheLowLevelOPDSGroupFeeds, self).should_process_lane(lane) 
                and lane.parent)


class CacheBestSellerFeeds(CacheRepresentationPerLane):
    """Cache the complete feed of best-sellers for each top-level lanes."""

    name = "Cache best-seller feeds"

    PRIMARY_COLLECTIONS = [[x] for x in AcquisitionFeed.BEST_SELLER_LANGUAGES]
    OTHER_COLLECTIONS = []

    def cache_url(self, annotator, lane, languages):
        return self.app.popular_feed_cache_url(annotator, lane, languages)

    def make_get_method(self, annotator, lane, languages):
        def get_method(*args, **kwargs):
            return self.app.make_popular_feed(self._db, annotator, lane, languages)
        return get_method

    def should_process_lane(self, lane):
        # Only process the top-level lanes.
        return not lane.parent

class CacheStaffPicksFeeds(CacheRepresentationPerLane):
    """Cache the complete feed of staff picks for each top-level lane."""

    name = "Cache staff picks feeds"

    PRIMARY_COLLECTIONS = [[x] for x in AcquisitionFeed.STAFF_PICKS_LANGUAGES]
    OTHER_COLLECTIONS = []

    def cache_url(self, annotator, lane, languages):
        return self.app.staff_picks_feed_cache_url(annotator, lane, languages)

    def process_lane(self, lane):
        annotator = CirculationManagerAnnotator(None, lane)
        max_size = 200
        page_size = 50
        if lane:
            lane_name = lane.name
        else:
            lane_name = None
        for languages in self.languages:
            for facet in ('title', 'author'):
                for offset in range(0, max_size, page_size):
                    url = self.app.staff_picks_feed_cache_url(
                        annotator, lane_name, languages, facet, offset, 
                        page_size
                    )
                    def get_method(*args, **kwargs):
                        return self.app.make_staff_picks_feed(
                            self._db, annotator, lane, languages, facet,
                            offset, page_size)
                    self.generate_feed(url, get_method, 10*60)

    def should_process_lane(self, lane):
        # Only process the top-level lanes.
        return not lane.parent
