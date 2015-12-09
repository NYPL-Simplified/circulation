import os
import re
from nose.tools import (
    set_trace,
    eq_,
)
import feedparser
from . import DatabaseTest

from ..core.lane import (
    LaneList,
    Lane,
)
from ..core.model import (
    Work,
)

from ..core.classifier import (
    Classifier,
    Fantasy,
)

from ..circulation import CirculationAPI

from ..opds import (
    CirculationManagerAnnotator,
    CirculationManagerLoanAndHoldAnnotator,
)
from ..core.opds import (
    AcquisitionFeed,
    OPDSFeed,
)
class TestOPDS(DatabaseTest):

    def test_alternate_link_is_permalink(self):
        w1 = self._work(with_open_access_download=True)
        self._db.commit()

        works = self._db.query(Work)
        annotator = CirculationManagerAnnotator(None, Fantasy, test_mode=True)
        pool = annotator.active_licensepool_for(w1)

        feed = AcquisitionFeed(self._db, "test", "url", works, annotator)
        feed = feedparser.parse(unicode(feed))
        [entry] = feed['entries']
        eq_(entry['id'], pool.identifier.urn)

        [alternate] = [x['href'] for x in entry['links'] if x['rel'] == 'alternate']
        permalink = annotator.permalink_for(w1, pool, pool.identifier)
        eq_(alternate, permalink)

    def test_acquisition_feed_includes_problem_reporting_link(self):
        w1 = self._work(with_open_access_download=True)
        self._db.commit()
        feed = AcquisitionFeed(
            self._db, "test", "url", [w1], CirculationManagerAnnotator(
                None, Fantasy, test_mode=True))
        feed = feedparser.parse(unicode(feed))
        [entry] = feed['entries']
        [issues_link] = [x for x in entry['links'] if x['rel'] == 'issues']
        assert '/report' in issues_link['href']

    def test_acquisition_feed_includes_open_access_or_borrow_link(self):
        w1 = self._work(with_open_access_download=True)
        w2 = self._work(with_open_access_download=True)
        w2.license_pools[0].open_access = False
        w2.licenses_available = 10
        self._db.commit()

        works = self._db.query(Work)
        feed = AcquisitionFeed(
            self._db, "test", "url", works, CirculationManagerAnnotator(
                None, Fantasy, test_mode=True))
        feed = feedparser.parse(unicode(feed))
        entries = sorted(feed['entries'], key = lambda x: int(x['title']))

        open_access_links, borrow_links = [x['links'] for x in entries]
        open_access_rels = [x['rel'] for x in open_access_links]
        assert OPDSFeed.BORROW_REL in open_access_rels

        borrow_rels = [x['rel'] for x in borrow_links]
        assert OPDSFeed.BORROW_REL in borrow_rels

    def test_active_loan_feed(self):
        patron = self.default_patron
        feed = CirculationManagerLoanAndHoldAnnotator.active_loans_for(
            None, patron, test_mode=True)
        # Nothing in the feed.
        feed = feedparser.parse(unicode(feed))
        eq_(0, len(feed['entries']))

        work = self._work(language="eng", with_open_access_download=True)
        work.license_pools[0].loan_to(patron)
        unused = self._work(language="eng", with_open_access_download=True)

        # Get the feed.
        feed = CirculationManagerLoanAndHoldAnnotator.active_loans_for(
            None, patron, test_mode=True)
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
        feed = AcquisitionFeed(
            self._db, "test", "url", works,
            CirculationManagerAnnotator(None, Fantasy, test_mode=True)
        )
        u = unicode(feed)
        holds_re = re.compile('<opds:holds\W+total="25"\W*/>', re.S)
        assert holds_re.search(u) is not None
        
        copies_re = re.compile('<opds:copies[^>]+available="50"', re.S)
        assert copies_re.search(u) is not None

        copies_re = re.compile('<opds:copies[^>]+total="100"', re.S)
        assert copies_re.search(u) is not None
