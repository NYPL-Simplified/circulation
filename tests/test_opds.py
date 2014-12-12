from nose.tools import (
    set_trace,
    eq_,
)
import feedparser
from . import DatabaseTest

from ..core.model import (
    LaneList,
    Lane,
    Work,
)
from ..core.classifier import (
    Classifier,
    Fantasy,
)

from ..opds import CirculationManagerAnnotator
from ..core.opds import (
    AcquisitionFeed,
    OPDSFeed,
)
from ..circulation_manager import app

class TestOPDS(DatabaseTest):

    def setup(self):
        super(TestOPDS, self).setup()
        self.app = app.test_client()
        self.ctx = app.test_request_context()
        self.ctx.push()

        self.lanes = LaneList.from_description(
            self._db,
            None,
            [dict(name="Fiction",
                  fiction=True,
                  audience=Classifier.AUDIENCE_ADULT,
                  genres=[]),
             Fantasy,
             dict(
                 name="Young Adult",
                 fiction=Lane.BOTH_FICTION_AND_NONFICTION,
                 audience=Classifier.AUDIENCE_YOUNG_ADULT,
                 genres=[]),
             dict(name="Romance", fiction=True, genres=[],
                  sublanes=[
                      dict(name="Contemporary Romance")
                  ]
              ),
         ]
        )

        class FakeConf(object):
            name = None
            sublanes = None
            pass

        self.conf = FakeConf()
        self.conf.sublanes = self.lanes

    def test_acquisition_feed_includes_open_access_or_borrow_link(self):
        w1 = self._work(with_open_access_download=True)
        w2 = self._work(with_open_access_download=True)
        w2.license_pools[0].open_access = False
        w2.licenses_available = 10
        self._db.commit()

        works = self._db.query(Work)
        feed = AcquisitionFeed(
            self._db, "test", "url", works, CirculationManagerAnnotator(Fantasy))
        feed = feedparser.parse(unicode(feed))
        entries = sorted(feed['entries'], key = lambda x: int(x['title']))

        open_access_links, borrow_links = [x['links'] for x in entries]
        open_access_rels = [x['rel'] for x in open_access_links]
        assert OPDSFeed.OPEN_ACCESS_REL in open_access_rels
        assert not OPDSFeed.BORROW_REL in open_access_rels

        borrow_rels = [x['rel'] for x in borrow_links]
        assert not OPDSFeed.OPEN_ACCESS_REL in borrow_rels
        assert OPDSFeed.BORROW_REL in borrow_rels

    def test_active_loan_feed(self):
        patron = self.default_patron
        feed = CirculationManagerAnnotator.active_loans_for(patron)
        # Nothing in the feed.
        feed = feedparser.parse(unicode(feed))
        eq_(0, len(feed['entries']))

        work = self._work(language="eng", with_open_access_download=True)
        work.license_pools[0].loan_to(patron)
        unused = self._work(language="eng", with_open_access_download=True)

        # Get the feed.
        feed = CirculationManagerAnnotator.active_loans_for(patron)
        feed = feedparser.parse(unicode(feed))

        # The only entry in the feed is the work currently out on loan
        # to this patron.
        eq_(1, len(feed['entries']))
        eq_(work.title, feed['entries'][0]['title'])



    def test_acquisition_feed_includes_license_information(self):
        work = self._work(with_open_access_download=True)
        pool = work.license_pools[0]

        # These numbers are impossible, but it doesn't matter for
        # purposes of this test.
        pool.open_access = False
        pool.licenses_owned = 100
        pool.licenses_available = 50
        pool.patrons_in_hold_queue = 25
        self._db.commit()

        works = self._db.query(Work)
        feed = AcquisitionFeed(self._db, "test", "url", works,
                               CirculationManagerAnnotator(Fantasy))
        u = unicode(feed)
        feed = feedparser.parse(u)
        [entry] = feed['entries']
        eq_('100', entry['simplified_total_licenses'])
        eq_('50', entry['simplified_available_licenses'])
        eq_('25', entry['simplified_active_holds'])

