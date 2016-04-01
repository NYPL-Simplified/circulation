import datetime
import os
import re
from lxml import etree
from nose.tools import (
    set_trace,
    eq_,
)
import feedparser
from . import DatabaseTest

from core.lane import (
    LaneList,
    Lane,
)
from core.model import (
    Work,
)

from core.classifier import (
    Classifier,
    Fantasy,
)

from core.opds import (
    _strftime
)

from core.opds_import import (
    OPDSXMLParser
)

from api.circulation import CirculationAPI
from api.config import (
    Configuration, 
    temp_config,
)

from api.opds import (
    CirculationManagerAnnotator,
    CirculationManagerLoanAndHoldAnnotator,
)
from core.opds import (
    AcquisitionFeed,
    OPDSFeed,
)

from core.util.cdn import cdnify

class TestCirculationManagerAnnotator(DatabaseTest):

    def setup(self):
        super(TestCirculationManagerAnnotator, self).setup()
        self.work = self._work(with_open_access_download=True)
        self.annotator = CirculationManagerAnnotator(
            None, Fantasy, test_mode=True
        )

    def test_open_access_link(self):

        # The resource URL associated with a LicensePoolDeliveryMechanism
        # becomes the `href` of an open-access `link` tag.
        [lpdm] = self.work.license_pools[0].delivery_mechanisms
        link_tag = self.annotator.open_access_link(lpdm)
        eq_(lpdm.resource.url, link_tag.get('href'))

        # If we have a CDN set up for open-access links, the CDN hostname
        # replaces the original hostname.
        with temp_config() as config:
            cdn_host = "https://cdn.com/"
            config[Configuration.INTEGRATIONS] = {
                Configuration.CDN_INTEGRATION : {
                    Configuration.CDN_OPEN_ACCESS_CONTENT : cdn_host
                }
            }
            link_tag = self.annotator.open_access_link(lpdm)
            link_url = link_tag.get('href')
            assert link_url.startswith(cdn_host)
            assert link_url == cdnify(lpdm.resource.url, cdn_host)

class TestOPDS(DatabaseTest):

    def test_default_lane_url(self):
        fantasy_lane = Lane(self._db, "Fantasy", genres=[Fantasy]);
        annotator = CirculationManagerAnnotator(None, fantasy_lane, test_mode=True)

        default_lane_url = annotator.default_lane_url()

        assert "groups" in default_lane_url
        assert "Fantasy" not in default_lane_url

    def test_groups_url(self):
        fantasy_lane = Lane(self._db, "Fantasy", genres=[Fantasy]);
        annotator = CirculationManagerAnnotator(None, fantasy_lane, test_mode=True)

        groups_url_no_lane = annotator.groups_url(None)

        assert "groups" in groups_url_no_lane
        assert "Fantasy" not in groups_url_no_lane

        groups_url_fantasy = annotator.groups_url(fantasy_lane)
        assert "groups" in groups_url_fantasy
        assert "Fantasy" in groups_url_fantasy

    def test_feed_url(self):
        fantasy_lane = Lane(self._db, "Fantasy", genres=[Fantasy]);
        annotator = CirculationManagerAnnotator(None, fantasy_lane, test_mode=True)

        feed_url_fantasy = annotator.feed_url(fantasy_lane, dict(), dict())
        assert "feed" in feed_url_fantasy
        assert "Fantasy" in feed_url_fantasy

    def test_search_url(self):
        fantasy_lane = Lane(self._db, "Fantasy", genres=[Fantasy]);
        annotator = CirculationManagerAnnotator(None, fantasy_lane, test_mode=True)

        search_url = annotator.search_url(fantasy_lane, "query", dict())
        assert "search" in search_url
        assert "query" in search_url
        assert "Fantasy" in search_url

    def test_facet_url(self):
        fantasy_lane = Lane(self._db, "Fantasy", genres=[Fantasy]);
        facets = dict(collection="main")
        annotator = CirculationManagerAnnotator(None, fantasy_lane, test_mode=True)

        facet_url = annotator.facet_url(facets)
        assert "collection=main" in facet_url
        assert "Fantasy" in facet_url

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


        [(alternate, type)] = [(x['href'], x['type']) for x in entry['links'] if x['rel'] == 'alternate']
        permalink = annotator.permalink_for(w1, pool, pool.identifier)
        eq_(alternate, permalink)
        eq_(OPDSFeed.ENTRY_TYPE, type)

        # Make sure we are using the 'permalink' controller -- we were using
        # 'work' and that was wrong.
        assert '/host/permalink' in permalink

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
        raw = CirculationManagerLoanAndHoldAnnotator.active_loans_for(
            None, patron, test_mode=True)
        # Nothing in the feed.
        raw = unicode(raw)
        feed = feedparser.parse(raw)
        eq_(0, len(feed['entries']))

        now = datetime.datetime.utcnow()
        tomorrow = now + datetime.timedelta(days=1)

        # A loan of an open-access book is open-ended.
        work1 = self._work(language="eng", with_open_access_download=True)
        loan1 = work1.license_pools[0].loan_to(patron, start=now)

        # A loan of some other kind of book
        work2 = self._work(language="eng", with_license_pool=True)
        loan2 = work2.license_pools[0].loan_to(patron, start=now, end=tomorrow)
        unused = self._work(language="eng", with_open_access_download=True)

        # Get the feed.
        feed_obj = CirculationManagerLoanAndHoldAnnotator.active_loans_for(
            None, patron, test_mode=True)
        raw = unicode(feed_obj)
        feed = feedparser.parse(raw)

        # The only entries in the feed is the work currently out on loan
        # to this patron.
        eq_(2, len(feed['entries']))
        e1, e2 = sorted(feed['entries'], key=lambda x: x['title'])
        eq_(work1.title, e1['title'])
        eq_(work2.title, e2['title'])

        # Make sure that the start and end dates from the loan are present
        # in an <opds:availability> child of the acquisition link.
        tree = etree.fromstring(raw)
        parser = OPDSXMLParser()
        acquisitions = parser._xpath(
            tree, "//atom:entry/atom:link[@rel='http://opds-spec.org/acquisition']"
        )
        eq_(2, len(acquisitions))

        now_s = _strftime(now)
        tomorrow_s = _strftime(tomorrow)
        availabilities = [
            parser._xpath1(x, "opds:availability") for x in acquisitions
        ]

        # One of these availability tags has 'since' but not 'until'.
        # The other one has both.
        [no_until] = [x for x in availabilities if 'until' not in x.attrib] 
        eq_(now_s, no_until.attrib['since'])

        [has_until] = [x for x in availabilities if 'until' in x.attrib]
        eq_(now_s, has_until.attrib['since'])
        eq_(tomorrow_s, has_until.attrib['until'])

    def test_loan_feed_includes_patron(self):
        patron = self._patron()
        patron.username = u'bellhooks'
        patron.authorization_identifier = u'987654321'

        feed_obj = CirculationManagerLoanAndHoldAnnotator.active_loans_for(
            None, patron, test_mode=True)
        raw = unicode(feed_obj)
        feed_details = feedparser.parse(raw)['feed']

        assert "simplified:authorizationIdentifier" in raw
        assert "simplified:username" in raw
        eq_(patron.username, feed_details['simplified_patron']['simplified:username'])
        eq_(u'987654321', feed_details['simplified_patron']['simplified:authorizationidentifier'])

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

