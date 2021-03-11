from nose.tools import (
    set_trace,
    eq_
)

import feedparser

from api.admin.opds import AdminAnnotator, AdminFeed
from api.opds import AcquisitionFeed
from core.model import (
    Complaint,
    DataSource,
    ExternalIntegration,
    Library,
    Measurement,
)
from core.model.configuration import ExternalIntegrationLink
from core.lane import Facets, Pagination
from core.opds import Annotator

from core.testing import DatabaseTest

class TestOPDS(DatabaseTest):

    def links(self, entry, rel=None):
        if 'feed' in entry:
            entry = entry['feed']
        links = sorted(entry['links'], key=lambda x: (x['rel'], x.get('title')))
        r = []
        for l in links:
            if (not rel or l['rel'] == rel or
                (isinstance(rel, list) and l['rel'] in rel)):
                r.append(l)
        return r

    def test_feed_includes_staff_rating(self):
        work = self._work(with_open_access_download=True)
        lp = work.license_pools[0]
        staff_data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        lp.identifier.add_measurement(staff_data_source, Measurement.RATING, 3, weight=1000)

        feed = AcquisitionFeed(self._db, "test", "url", [work], AdminAnnotator(None, self._default_library, test_mode=True))
        [entry] = feedparser.parse(unicode(feed))['entries']
        rating = entry['schema_rating']
        eq_(3, float(rating['schema:ratingvalue']))
        eq_(Measurement.RATING, rating['additionaltype'])

    def test_feed_includes_refresh_link(self):
        work = self._work(with_open_access_download=True)
        lp = work.license_pools[0]
        lp.suppressed = False
        self._db.commit()

        # If the metadata wrangler isn't configured, the link is left out.
        feed = AcquisitionFeed(self._db, "test", "url", [work], AdminAnnotator(None, self._default_library, test_mode=True))
        [entry] = feedparser.parse(unicode(feed))['entries']
        eq_([],
            [x for x in entry['links'] if x['rel'] == "http://librarysimplified.org/terms/rel/refresh"])

        # If we configure a metadata wrangler integration, the link appears.
        integration = self._external_integration(
            ExternalIntegration.METADATA_WRANGLER,
            goal=ExternalIntegration.METADATA_GOAL,
            settings={ ExternalIntegration.URL: "http://metadata" },
            password="pw")
        integration.collections += [self._default_collection]
        feed = AcquisitionFeed(self._db, "test", "url", [work], AdminAnnotator(None, self._default_library, test_mode=True))
        [entry] = feedparser.parse(unicode(feed))['entries']
        [refresh_link] = [x for x in entry['links'] if x['rel'] == "http://librarysimplified.org/terms/rel/refresh"]
        assert lp.identifier.identifier in refresh_link["href"]

    def test_feed_includes_suppress_link(self):
        work = self._work(with_open_access_download=True)
        lp = work.license_pools[0]
        lp.suppressed = False
        self._db.commit()

        feed = AcquisitionFeed(self._db, "test", "url", [work], AdminAnnotator(None, self._default_library, test_mode=True))
        [entry] = feedparser.parse(unicode(feed))['entries']
        [suppress_link] = [x for x in entry['links'] if x['rel'] == "http://librarysimplified.org/terms/rel/hide"]
        assert lp.identifier.identifier in suppress_link["href"]
        unsuppress_links = [x for x in entry['links'] if x['rel'] == "http://librarysimplified.org/terms/rel/restore"]
        eq_(0, len(unsuppress_links))

        lp.suppressed = True
        self._db.commit()

        feed = AcquisitionFeed(self._db, "test", "url", [work], AdminAnnotator(None, self._default_library, test_mode=True))
        [entry] = feedparser.parse(unicode(feed))['entries']
        [unsuppress_link] = [x for x in entry['links'] if x['rel'] == "http://librarysimplified.org/terms/rel/restore"]
        assert lp.identifier.identifier in unsuppress_link["href"]
        suppress_links = [x for x in entry['links'] if x['rel'] == "http://librarysimplified.org/terms/rel/hide"]
        eq_(0, len(suppress_links))

    def test_feed_includes_edit_link(self):
        work = self._work(with_open_access_download=True)
        lp = work.license_pools[0]

        feed = AcquisitionFeed(self._db, "test", "url", [work], AdminAnnotator(None, self._default_library, test_mode=True))
        [entry] = feedparser.parse(unicode(feed))['entries']
        [edit_link] = [x for x in entry['links'] if x['rel'] == "edit"]
        assert lp.identifier.identifier in edit_link["href"]

    def test_feed_includes_change_cover_link(self):
        work = self._work(with_open_access_download=True)
        lp = work.license_pools[0]
        library = self._default_library

        feed = AcquisitionFeed(self._db, "test", "url", [work], AdminAnnotator(None, library, test_mode=True))
        [entry] = feedparser.parse(unicode(feed))['entries']

        # Since there's no storage integration, the change cover link isn't included.
        eq_([], [x for x in entry['links'] if x['rel'] == "http://librarysimplified.org/terms/rel/change_cover"])

        # There is now a covers storage integration that is linked to the external
        # integration for a collection that the work is in. It will use that
        # covers mirror and the change cover link is included.
        storage = self._external_integration(ExternalIntegration.S3, ExternalIntegration.STORAGE_GOAL)
        storage.username = "user"
        storage.password = "pass"

        collection = self._collection()
        purpose = ExternalIntegrationLink.COVERS
        external_integration_link = self._external_integration_link(
            integration=collection._external_integration,
            other_integration=storage,
            purpose=purpose
        )
        library.collections.append(collection)
        work = self._work(with_open_access_download=True, collection=collection)
        lp = work.license_pools[0]
        feed = AcquisitionFeed(self._db, "test", "url", [work], AdminAnnotator(None, library, test_mode=True))
        [entry] = feedparser.parse(unicode(feed))['entries']

        [change_cover_link] = [x for x in entry['links'] if x['rel'] == "http://librarysimplified.org/terms/rel/change_cover"]
        assert lp.identifier.identifier in change_cover_link["href"]

    def test_complaints_feed(self):
        """Test the ability to show a paginated feed of works with complaints.
        """

        type = iter(Complaint.VALID_TYPES)
        type1 = next(type)
        type2 = next(type)

        work1 = self._work(
            "fiction work with complaint",
            language="eng",
            fiction=True,
            with_open_access_download=True)
        work1_complaint1 = self._complaint(
            work1.license_pools[0],
            type1,
            "work1 complaint1 source",
            "work1 complaint1 detail")
        work1_complaint2 = self._complaint(
            work1.license_pools[0],
            type1,
            "work1 complaint2 source",
            "work1 complaint2 detail")
        work1_complaint3 = self._complaint(
            work1.license_pools[0],
            type2,
            "work1 complaint3 source",
            "work1 complaint3 detail")
        work2 = self._work(
            "nonfiction work with complaint",
            language="eng",
            fiction=False,
            with_open_access_download=True)
        work2_complaint1 = self._complaint(
            work2.license_pools[0],
            type2,
            "work2 complaint1 source",
            "work2 complaint1 detail")
        work3 = self._work(
            "fiction work without complaint",
            language="eng",
            fiction=True,
            with_open_access_download=True)
        work4 = self._work(
            "nonfiction work without complaint",
            language="eng",
            fiction=False,
            with_open_access_download=True)

        facets = Facets.default(self._default_library)
        pagination = Pagination(size=1)
        annotator = MockAnnotator(self._default_library)

        def make_page(pagination):
            return AdminFeed.complaints(
                library=self._default_library, title="Complaints",
                url=self._url, annotator=annotator,
                pagination=pagination
            )

        first_page = make_page(pagination)
        parsed = feedparser.parse(unicode(first_page))
        eq_(1, len(parsed['entries']))
        eq_(work1.title, parsed['entries'][0]['title'])
        # Verify that the entry has acquisition links.
        links = parsed['entries'][0]['links']
        open_access_links = [l for l in links if l['rel'] == "http://opds-spec.org/acquisition/open-access"]
        eq_(1, len(open_access_links))

        # Make sure the links are in place.
        [start] = self.links(parsed, 'start')
        eq_(annotator.groups_url(None), start['href'])
        eq_(annotator.top_level_title(), start['title'])

        [up] = self.links(parsed, 'up')
        eq_(annotator.groups_url(None), up['href'])
        eq_(annotator.top_level_title(), up['title'])

        [next_link] = self.links(parsed, 'next')
        eq_(annotator.complaints_url(facets, pagination.next_page), next_link['href'])

        # This was the first page, so no previous link.
        eq_([], self.links(parsed, 'previous'))

        # Now get the second page and make sure it has a 'previous' link.
        second_page = make_page(pagination.next_page)
        parsed = feedparser.parse(unicode(second_page))
        [previous] = self.links(parsed, 'previous')
        eq_(annotator.complaints_url(facets, pagination), previous['href'])
        eq_(1, len(parsed['entries']))
        eq_(work2.title, parsed['entries'][0]['title'])

    def test_suppressed_feed(self):
        # Test the ability to show a paginated feed of suppressed works.

        work1 = self._work(with_open_access_download=True)
        work1.license_pools[0].suppressed = True

        work2 = self._work(with_open_access_download=True)
        work2.license_pools[0].suppressed = True

        # This work won't be included in the feed since its
        # suppressed pool is superceded.
        work3 = self._work(with_open_access_download=True)
        work3.license_pools[0].suppressed = True
        work3.license_pools[0].superceded = True

        pagination = Pagination(size=1)
        annotator = MockAnnotator(self._default_library)
        titles = [work1.title, work2.title]

        def make_page(pagination):
            return AdminFeed.suppressed(
                _db=self._db, title="Hidden works",
                url=self._url, annotator=annotator,
                pagination=pagination
            )

        first_page = make_page(pagination)
        parsed = feedparser.parse(unicode(first_page))
        eq_(1, len(parsed['entries']))
        assert parsed['entries'][0].title in titles
        titles.remove(parsed['entries'][0].title)
        [remaining_title] = titles

        # Make sure the links are in place.
        [start] = self.links(parsed, 'start')
        eq_(annotator.groups_url(None), start['href'])
        eq_(annotator.top_level_title(), start['title'])

        [up] = self.links(parsed, 'up')
        eq_(annotator.groups_url(None), up['href'])
        eq_(annotator.top_level_title(), up['title'])

        [next_link] = self.links(parsed, 'next')
        eq_(annotator.suppressed_url(pagination.next_page), next_link['href'])

        # This was the first page, so no previous link.
        eq_([], self.links(parsed, 'previous'))

        # Now get the second page and make sure it has a 'previous' link.
        second_page = make_page(pagination.next_page)
        parsed = feedparser.parse(unicode(second_page))
        [previous] = self.links(parsed, 'previous')
        eq_(annotator.suppressed_url(pagination), previous['href'])
        eq_(1, len(parsed['entries']))
        eq_(remaining_title, parsed['entries'][0]['title'])

        # The third page is empty.
        third_page = make_page(pagination.next_page.next_page)
        parsed = feedparser.parse(unicode(third_page))
        [previous] = self.links(parsed, 'previous')
        eq_(annotator.suppressed_url(pagination.next_page), previous['href'])
        eq_(0, len(parsed['entries']))


class MockAnnotator(AdminAnnotator):

    def __init__(self, library):
        super(MockAnnotator, self).__init__(None, library, test_mode=True)

    def groups_url(self, lane):
        if lane:
            name = lane.name
        else:
            name = ""
        return "http://groups/%s" % name

    def complaints_url(self, facets, pagination):
        base = "http://complaints/"
        sep = '?'
        if facets:
            base += sep + facets.query_string
            sep = '&'
        if pagination:
            base += sep + pagination.query_string
        return base

    def suppressed_url(self, pagination):
        base = "http://complaints/"
        sep = '?'
        if pagination:
            base += sep + pagination.query_string
        return base

    def annotate_feed(self, feed):
        super(MockAnnotator, self).annotate_feed(feed)

