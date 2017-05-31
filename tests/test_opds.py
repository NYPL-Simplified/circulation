from collections import defaultdict
import feedparser
import datetime
from lxml import etree
from nose.tools import (
    eq_,
    set_trace,
    assert_raises,
)

from . import (
    DatabaseTest,
)

from psycopg2.extras import NumericRange
from config import (
    Configuration, 
    temp_config,
)
from model import (
    CachedFeed,
    Contributor,
    DataSource,
    DeliveryMechanism,
    Genre,
    Measurement,
    Representation,
    SessionManager,
    Subject,
    Work,
)

from lane import (
    Facets,
    Pagination,
    Lane,
    LaneList,
)

from opds import (    
    AcquisitionFeed,
    Annotator,
    LookupAcquisitionFeed,
    OPDSFeed,
    UnfulfillableWork,
    VerboseAnnotator,
    TestAnnotator,
    TestAnnotatorWithGroup,
    TestUnfulfillableAnnotator
)

from util.opds_writer import (    
    AtomFeed,
    OPDSFeed,
    OPDSMessage,
)

from classifier import (
    Classifier,
    Epic_Fantasy,
    Fantasy,
    Urban_Fantasy,
    History,
    Mystery,
)

from external_search import DummyExternalSearchIndex
import xml.etree.ElementTree as ET
from flask_babel import lazy_gettext as _

class TestBaseAnnotator(DatabaseTest):

    def test_active_licensepool_for_ignores_superceded_licensepools(self):
        work = self._work(with_license_pool=True, 
                          with_open_access_download=True)
        [pool1] = work.license_pools
        edition, pool2 = self._edition(with_license_pool=True)
        work.license_pools.append(pool2)

        # Start off with neither LicensePool being open-access. pool1
        # will become open-access later on, which is why we created an
        # open-access download for it.
        pool1.open_access = False
        pool1.licenses_owned = 1

        pool2.open_access = False
        pool2.licenses_owned = 1

        # If there are multiple non-superceded non-open-access license
        # pools for a work, the active license pool is one of them,
        # though we don't really know or care which one.
        assert Annotator.active_licensepool_for(work) is not None

        # Neither license pool is open-access, and pool1 is superceded.
        # The active license pool is pool2.
        pool1.superceded = True
        eq_(pool2, Annotator.active_licensepool_for(work))

        # pool2 is superceded and pool1 is not. The active licensepool
        # is pool1.
        pool1.superceded = False
        pool2.superceded = True
        eq_(pool1, Annotator.active_licensepool_for(work))

        # If both license pools are superceded, there is no active license
        # pool for the book.
        pool1.superceded = True
        eq_(None, Annotator.active_licensepool_for(work))
        pool1.superceded = False
        pool2.superceded = False

        # If one license pool is open-access and the other is not, the
        # open-access pool wins.
        pool1.open_access = True
        eq_(pool1, Annotator.active_licensepool_for(work))
        pool1.open_access = False
        
        # pool2 is open-access but has no usable download. The other
        # pool wins.
        pool2.open_access = True
        eq_(pool1, Annotator.active_licensepool_for(work))
        pool2.open_access = False

        # If one license pool has no owned licenses and the other has
        # owned licenses, the one with licenses wins.
        pool1.licenses_owned = 0
        pool2.licenses_owned = 1
        eq_(pool2, Annotator.active_licensepool_for(work))
        pool1.licenses_owned = 1

        # If one license pool has a presentation edition that's missing
        # a title, and the other pool has a presentation edition with a title,
        # the one with a title wins.
        pool2.presentation_edition.title = None
        eq_(pool1, Annotator.active_licensepool_for(work))


class TestAnnotators(DatabaseTest):

    def test_all_subjects(self):
        work = self._work(genre="Fiction", with_open_access_download=True)
        edition = work.presentation_edition
        identifier = edition.primary_identifier
        source1 = DataSource.lookup(self._db, DataSource.GUTENBERG)
        source2 = DataSource.lookup(self._db, DataSource.OCLC)

        subjects = [
            (source1, Subject.FAST, "fast1", "name1", 1),
            (source1, Subject.LCSH, "lcsh1", "name2", 1),
            (source2, Subject.LCSH, "lcsh1", "name2", 1),
            (source1, Subject.LCSH, "lcsh2", "name3", 3),
            (source1, Subject.DDC, "300", "Social sciences, sociology & anthropology", 1),
        ]

        for source, subject_type, subject, name, weight in subjects:
            identifier.classify(source, subject_type, subject, name, weight=weight)
        category_tags = VerboseAnnotator.categories(work)

        ddc_uri = Subject.uri_lookup[Subject.DDC]
        rating_value = '{http://schema.org/}ratingValue'
        eq_([{'term': u'300',
              rating_value: 1,
              'label': u'Social sciences, sociology & anthropology'}],
            category_tags[ddc_uri])

        fast_uri = Subject.uri_lookup[Subject.FAST]
        eq_([{'term': u'fast1', 'label': u'name1', rating_value: 1}],
            category_tags[fast_uri])

        lcsh_uri = Subject.uri_lookup[Subject.LCSH]
        eq_([{'term': u'lcsh1', 'label': u'name2', rating_value: 2},
             {'term': u'lcsh2', 'label': u'name3', rating_value: 3}],
            sorted(category_tags[lcsh_uri]))

        genre_uri = Subject.uri_lookup[Subject.SIMPLIFIED_GENRE]
        eq_([dict(label='Fiction', term=Subject.SIMPLIFIED_GENRE+"Fiction")], category_tags[genre_uri])

    def test_appeals(self):
        work = self._work(with_open_access_download=True)
        work.appeal_language = 0.1
        work.appeal_character = 0.2
        work.appeal_story = 0.3
        work.appeal_setting = 0.4
        work.calculate_opds_entries(verbose=True)

        category_tags = VerboseAnnotator.categories(work)
        appeal_tags = category_tags[Work.APPEALS_URI]
        expect = [
            (Work.APPEALS_URI + Work.LANGUAGE_APPEAL, Work.LANGUAGE_APPEAL, 0.1),
            (Work.APPEALS_URI + Work.CHARACTER_APPEAL, Work.CHARACTER_APPEAL, 0.2),
            (Work.APPEALS_URI + Work.STORY_APPEAL, Work.STORY_APPEAL, 0.3),
            (Work.APPEALS_URI + Work.SETTING_APPEAL, Work.SETTING_APPEAL, 0.4)
        ]
        actual = [
            (x['term'], x['label'], x['{http://schema.org/}ratingValue'])
            for x in appeal_tags
        ]
        eq_(set(expect), set(actual))

    def test_detailed_author(self):
        c, ignore = self._contributor("Familyname, Givenname")
        c.display_name = "Givenname Familyname"
        c.family_name = "Familyname"
        c.wikipedia_name = "Givenname Familyname (Author)"
        c.viaf = "100"
        c.lc = "n100"

        author_tag = VerboseAnnotator.detailed_author(c)

        tag_string = etree.tostring(author_tag)
        assert "<name>Givenname Familyname</" in tag_string        
        assert "<simplified:sort_name>Familyname, Givenname</" in tag_string        
        assert "<simplified:wikipedia_name>Givenname Familyname (Author)</" in tag_string
        assert "<schema:sameas>http://viaf.org/viaf/100</" in tag_string
        assert "<schema:sameas>http://id.loc.gov/authorities/names/n100</"

        work = self._work(authors=[], with_license_pool=True)
        work.presentation_edition.add_contributor(c, Contributor.PRIMARY_AUTHOR_ROLE)

        [same_tag] = VerboseAnnotator.authors(
            work, work.license_pools[0], work.presentation_edition,
            work.presentation_edition.primary_identifier)
        eq_(tag_string, etree.tostring(same_tag))

    def test_duplicate_author_names_are_ignored(self):
        """Ignores duplicate author names"""
        work = self._work(with_license_pool=True)
        duplicate = self._contributor()[0]
        duplicate.sort_name = work.author

        edition = work.presentation_edition
        edition.add_contributor(duplicate, Contributor.AUTHOR_ROLE)

        eq_(1, len(Annotator.authors(
            work, work.license_pools[0], edition, edition.primary_identifier
        )))

    def test_all_annotators_mention_every_author(self):
        work = self._work(authors=[], with_license_pool=True)
        work.presentation_edition.add_contributor(
            self._contributor()[0], Contributor.PRIMARY_AUTHOR_ROLE)
        work.presentation_edition.add_contributor(
            self._contributor()[0], Contributor.AUTHOR_ROLE)
        work.presentation_edition.add_contributor(
            self._contributor()[0], "Illustrator")
        eq_(2, len(Annotator.authors(
            work, work.license_pools[0], work.presentation_edition,
            work.presentation_edition.primary_identifier)))
        eq_(2, len(VerboseAnnotator.authors(
            work, work.license_pools[0], work.presentation_edition,
            work.presentation_edition.primary_identifier)))

    def test_ratings(self):
        work = self._work(
            with_license_pool=True, with_open_access_download=True)
        work.quality = 1.0/3
        work.popularity = 0.25
        work.rating = 0.6
        work.calculate_opds_entries(verbose=True)
        feed = AcquisitionFeed(
            self._db, self._str, self._url, [work], VerboseAnnotator
        )
        url = self._url
        tag = feed.create_entry(work, None)

        nsmap = dict(schema='http://schema.org/')
        ratings = [(rating.get('{http://schema.org/}ratingValue'),
                    rating.get('{http://schema.org/}additionalType'))
                   for rating in tag.xpath("schema:Rating", namespaces=nsmap)]
        expected = [
            ('0.3333', Measurement.QUALITY),
            ('0.2500', Measurement.POPULARITY),
            ('0.6000', None)
        ]
        eq_(set(expected), set(ratings))

    def test_subtitle(self):
        work = self._work(with_license_pool=True, with_open_access_download=True)
        work.presentation_edition.subtitle = "Return of the Jedi"
        work.calculate_opds_entries()

        raw_feed = unicode(AcquisitionFeed(
            self._db, self._str, self._url, [work], Annotator
        ))
        assert "schema:alternativeHeadline" in raw_feed
        assert work.presentation_edition.subtitle in raw_feed

        feed = feedparser.parse(unicode(raw_feed))
        alternative_headline = feed['entries'][0]['schema_alternativeheadline']
        eq_(work.presentation_edition.subtitle, alternative_headline)

        # If there's no subtitle, the subtitle tag isn't included.
        work.presentation_edition.subtitle = None
        work.calculate_opds_entries()
        raw_feed = unicode(AcquisitionFeed(
            self._db, self._str, self._url, [work], Annotator
        ))

        assert "schema:alternativeHeadline" not in raw_feed
        assert "Return of the Jedi" not in raw_feed
        [entry] = feedparser.parse(unicode(raw_feed))['entries']
        assert 'schema_alternativeheadline' not in entry.items()

    def test_series(self):
        work = self._work(with_license_pool=True, with_open_access_download=True)
        work.presentation_edition.series = "Harry Otter and the Lifetime of Despair"
        work.presentation_edition.series_position = 4
        work.calculate_opds_entries()

        raw_feed = unicode(AcquisitionFeed(
            self._db, self._str, self._url, [work], Annotator
        ))
        assert "schema:Series" in raw_feed
        assert work.presentation_edition.series in raw_feed

        feed = feedparser.parse(unicode(raw_feed))
        schema_entry = feed['entries'][0]['schema_series']
        eq_(work.presentation_edition.series, schema_entry['name'])
        eq_(str(work.presentation_edition.series_position), schema_entry['schema:position'])

        # If there's no series title, the series tag isn't included.
        work.presentation_edition.series = None
        work.calculate_opds_entries()
        raw_feed = unicode(AcquisitionFeed(
            self._db, self._str, self._url, [work], Annotator
        ))

        assert "schema:Series" not in raw_feed
        assert "Lifetime of Despair" not in raw_feed
        [entry] = feedparser.parse(unicode(raw_feed))['entries']
        assert 'schema_series' not in entry.items()


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

    def setup(self):
        super(TestOPDS, self).setup()

        self.lanes = LaneList.from_description(
            self._db,
            None,
            [dict(full_name="Fiction",
                  fiction=True,
                  audiences=Classifier.AUDIENCE_ADULT,
                  genres=[],
                  sublanes=[Fantasy],
              ),
             History,
             dict(
                 full_name="Young Adult",
                 fiction=Lane.BOTH_FICTION_AND_NONFICTION,
                 audiences=Classifier.AUDIENCE_YOUNG_ADULT,
                 genres=[]),
             dict(full_name="Romance", fiction=True, genres=[],
                  sublanes=[
                      dict(full_name="Contemporary Romance")
                  ]
              ),
         ]
        )

        mock_top_level = Lane(
            self._db, '', display_name='', sublanes=self.lanes.lanes,
            include_all=False, invisible=True
        )

        class FakeConf(object):
            name = None
            display_name = None
            sublanes = self.lanes
            top_level_lane = mock_top_level

        self.conf = FakeConf()

    def test_acquisition_link(self):
        m = AcquisitionFeed.acquisition_link
        rel = AcquisitionFeed.BORROW_REL
        href = self._url

        # A doubly-indirect acquisition link.
        a = m(rel, href, ["text/html", "text/plain", "application/pdf"])
        eq_(etree.tostring(a), '<link href="%s" rel="http://opds-spec.org/acquisition/borrow" type="text/html"><ns0:indirectAcquisition xmlns:ns0="http://opds-spec.org/2010/catalog" type="text/plain"><ns0:indirectAcquisition type="application/pdf"/></ns0:indirectAcquisition></link>' % href)

        # A direct acquisition link.
        b = m(rel, href, ["application/epub"])    
        eq_(etree.tostring(b), '<link href="%s" rel="http://opds-spec.org/acquisition/borrow" type="application/epub"/>' % href)

    def test_group_uri(self):
        work = self._work(with_open_access_download=True, authors="Alice")
        [lp] = work.license_pools

        annotator = TestAnnotatorWithGroup()
        feed = AcquisitionFeed(self._db, "test", "http://the-url.com/",
                               [work], annotator)
        u = unicode(feed)
        parsed = feedparser.parse(u)
        [group_link] = parsed.entries[0]['links']
        expect_uri, expect_title = annotator.group_uri(
            work, lp, lp.identifier)
        eq_(OPDSFeed.GROUP_REL, group_link['rel'])
        eq_(expect_uri, group_link['href'])
        eq_(expect_title, group_link['title'])

    def test_acquisition_feed(self):
        work = self._work(with_open_access_download=True, authors="Alice")

        feed = AcquisitionFeed(self._db, "test", "http://the-url.com/",
                               [work])
        u = unicode(feed)
        assert '<entry schema:additionalType="http://schema.org/Book">' in u
        parsed = feedparser.parse(u)
        [with_author] = parsed['entries']
        eq_("Alice", with_author['authors'][0]['name'])

    def test_acquisition_feed_includes_license_source(self):
        work = self._work(with_open_access_download=True)
        feed = AcquisitionFeed(self._db, "test", "http://the-url.com/",
                               [work])
        parsed = feedparser.parse(unicode(feed))
        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        eq_(gutenberg.name, parsed.entries[0]['bibframe_distribution']['providername'])

    def test_acquisition_feed_includes_author_tag_even_when_no_author(self):
        work = self._work(with_open_access_download=True)
        feed = AcquisitionFeed(self._db, "test", "http://the-url.com/",
                               [work])
        u = unicode(feed)
        assert "<author>" in u

    def test_acquisition_feed_includes_permanent_work_id(self):
        work = self._work(with_open_access_download=True)
        feed = AcquisitionFeed(self._db, "test", "http://the-url.com/",
                               [work])
        u = unicode(feed)
        parsed = feedparser.parse(u)
        entry = parsed['entries'][0]
        eq_(work.presentation_edition.permanent_work_id, 
            entry['simplified_pwid'])

    def test_lane_feed_contains_facet_links(self):
        work = self._work(with_open_access_download=True)

        lane = Lane(self._db, "lane")
        facets = Facets.default()

        cached_feed = AcquisitionFeed.page(self._db, "title", "http://the-url.com/",
                                    lane, TestAnnotator, facets=facets)
        
        u = unicode(cached_feed.content)
        parsed = feedparser.parse(u)
        by_title = parsed['feed']

        [self_link] = self.links(by_title, 'self')
        eq_("http://the-url.com/", self_link['href'])
        facet_links = self.links(by_title, AcquisitionFeed.FACET_REL)
        
        order_facets = Configuration.enabled_facets(
            Facets.ORDER_FACET_GROUP_NAME
        )
        availability_facets = Configuration.enabled_facets(
            Facets.AVAILABILITY_FACET_GROUP_NAME
        )
        collection_facets = Configuration.enabled_facets(
            Facets.COLLECTION_FACET_GROUP_NAME
        )        

        def link_for_facets(facets):
            return [x for x in facet_links if facets.query_string in x['href']]

        facets = Facets(None, None, None)
        for i1, i2, new_facets, selected in facets.facet_groups:            
            links = link_for_facets(new_facets)
            if selected:
                # This facet set is already selected, so it should
                # show up three times--once for every facet group.
                eq_(3, len(links))
            else:
                # This facet set is not selected, so it should have one
                # transition link.
                eq_(1, len(links))

        # As we'll see below, the feed parser parses facetGroup as
        # facetgroup and activeFacet as activefacet. As we see here,
        # that's not a problem with the generator code.
        assert 'opds:facetgroup' not in u
        assert 'opds:facetGroup' in u
        assert 'opds:activefacet' not in u
        assert 'opds:activeFacet' in u

    def test_acquisition_feed_includes_available_and_issued_tag(self):
        today = datetime.date.today()
        today_s = today.strftime("%Y-%m-%d")
        the_past = today - datetime.timedelta(days=2)
        the_past_s = the_past.strftime("%Y-%m-%d")
        the_past_time = the_past.strftime(AtomFeed.TIME_FORMAT)
        the_distant_past = today - datetime.timedelta(days=100)
        the_distant_past_s = the_distant_past.strftime('%Y-%m-%dT%H:%M:%SZ')
        the_future = today + datetime.timedelta(days=2)

        # This work has both issued and published. issued will be used
        # for the dc:created tag.
        work1 = self._work(with_open_access_download=True)
        work1.presentation_edition.issued = today
        work1.presentation_edition.published = the_past
        work1.license_pools[0].availability_time = the_distant_past

        # This work only has published. published will be used for the
        # dc:created tag.
        work2 = self._work(with_open_access_download=True)
        work2.presentation_edition.published = the_past
        work2.license_pools[0].availability_time = the_distant_past

        # This work has neither published nor issued. There will be no
        # dc:issued tag.
        work3 = self._work(with_open_access_download=True)
        work3.license_pools[0].availability_time = None

        # This work is issued in the future. Since this makes no
        # sense, there will be no dc:issued tag.
        work4 = self._work(with_open_access_download=True)
        work4.presentation_edition.issued = the_future
        work4.presentation_edition.published = the_future
        work4.license_pools[0].availability_time = None

        for w in work1, work2, work3, work4:
            w.calculate_opds_entries(verbose=False)

        self._db.commit()
        works = self._db.query(Work)
        with_times = AcquisitionFeed(
            self._db, "test", "url", works, TestAnnotator)
        u = unicode(with_times)
        assert 'dcterms:created' in u
        with_times = feedparser.parse(u)
        e1, e2, e3, e4 = sorted(
            with_times['entries'], key = lambda x: int(x['title']))
        eq_(today_s, e1['created'])
        eq_(the_distant_past_s, e1['published'])

        eq_(the_past_s, e2['created'])
        eq_(the_distant_past_s, e2['published'])

        assert not 'created' in e3
        assert not 'published' in e3

        assert not 'created' in e4
        assert not 'published' in e4

    def test_acquisition_feed_includes_language_tag(self):
        work = self._work(with_open_access_download=True)
        work.presentation_edition.publisher = "The Publisher"
        work2 = self._work(with_open_access_download=True)
        work2.presentation_edition.publisher = None

        self._db.commit()
        for w in work, work2:
            w.calculate_opds_entries(verbose=False)

        works = self._db.query(Work)
        with_publisher = AcquisitionFeed(
            self._db, "test", "url", works, TestAnnotator)
        with_publisher = feedparser.parse(unicode(with_publisher))
        entries = sorted(with_publisher['entries'], key = lambda x: x['title'])
        eq_('The Publisher', entries[0]['dcterms_publisher'])
        assert 'publisher' not in entries[1]

    def test_acquisition_feed_includes_audience_as_category(self):
        work = self._work(with_open_access_download=True)
        work.audience = "Young Adult"
        work2 = self._work(with_open_access_download=True)
        work2.audience = "Children"
        work2.target_age = NumericRange(7,9)
        work3 = self._work(with_open_access_download=True)
        work3.audience = None

        self._db.commit()

        for w in work, work2, work3:
            w.calculate_opds_entries(verbose=False)

        works = self._db.query(Work)
        with_audience = AcquisitionFeed(self._db, "test", "url", works)
        u = unicode(with_audience)
        with_audience = feedparser.parse(u)
        entries = sorted(with_audience['entries'], key = lambda x: int(x['title']))
        scheme = "http://schema.org/audience"
        eq_(
            [('Young Adult', 'Young Adult')],
            [(x['term'], x['label']) for x in entries[0]['tags']
             if x['scheme'] == scheme]
        )

        eq_(
            [('Children', 'Children')],
            [(x['term'], x['label']) for x in entries[1]['tags']
             if x['scheme'] == scheme]
        )

        age_scheme = Subject.uri_lookup[Subject.AGE_RANGE]
        eq_(
            [('7-9', '7-9')],
            [(x['term'], x['label']) for x in entries[1]['tags']
             if x['scheme'] == age_scheme]
        )

        eq_([],
            [(x['term'], x['label']) for x in entries[2]['tags']
             if x['scheme'] == scheme])

    def test_acquisition_feed_includes_category_tags_for_appeals(self):
        work = self._work(with_open_access_download=True)
        work.appeal_language = 0.1
        work.appeal_character = 0.2
        work.appeal_story = 0.3
        work.appeal_setting = 0.4

        work2 = self._work(with_open_access_download=True)

        for w in work, work2:
            w.calculate_opds_entries(verbose=False)

        self._db.commit()
        works = self._db.query(Work)
        feed = AcquisitionFeed(self._db, "test", "url", works)
        feed = feedparser.parse(unicode(feed))
        entries = sorted(feed['entries'], key = lambda x: int(x['title']))

        tags = entries[0]['tags']
        matches = [(x['term'], x['label']) for x in tags if x['scheme'] == Work.APPEALS_URI]
        eq_([
            (Work.APPEALS_URI + 'Character', 'Character'),
            (Work.APPEALS_URI + 'Language', 'Language'),
            (Work.APPEALS_URI + 'Setting', 'Setting'),
            (Work.APPEALS_URI + 'Story', 'Story'),
        ],
            sorted(matches)
        )

        tags = entries[1]['tags']
        matches = [(x['term'], x['label']) for x in tags if x['scheme'] == Work.APPEALS_URI]
        eq_([], matches)

    def test_acquisition_feed_includes_category_tags_for_fiction_status(self):
        work = self._work(with_open_access_download=True)
        work.fiction = False

        work2 = self._work(with_open_access_download=True)
        work2.fiction = True

        for w in work, work2:
            w.calculate_opds_entries(verbose=False)

        self._db.commit()
        works = self._db.query(Work)
        feed = AcquisitionFeed(self._db, "test", "url", works)
        feed = feedparser.parse(unicode(feed))
        entries = sorted(feed['entries'], key = lambda x: int(x['title']))

        scheme = "http://librarysimplified.org/terms/fiction/"

        eq_([(scheme+'Nonfiction', 'Nonfiction')], 
            [(x['term'], x['label']) for x in entries[0]['tags']
             if x['scheme'] == scheme]
        )
        eq_([(scheme+'Fiction', 'Fiction')], 
            [(x['term'], x['label']) for x in entries[1]['tags']
             if x['scheme'] == scheme]
        )


    def test_acquisition_feed_includes_category_tags_for_genres(self):
        work = self._work(with_open_access_download=True)
        g1, ignore = Genre.lookup(self._db, "Science Fiction")
        g2, ignore = Genre.lookup(self._db, "Romance")
        work.genres = [g1, g2]

        work.calculate_opds_entries(verbose=False)

        self._db.commit()
        works = self._db.query(Work)
        feed = AcquisitionFeed(self._db, "test", "url", works)
        feed = feedparser.parse(unicode(feed))
        entries = sorted(feed['entries'], key = lambda x: int(x['title']))

        scheme = Subject.SIMPLIFIED_GENRE
        eq_(
            [(scheme+'Romance', 'Romance'),
             (scheme+'Science%20Fiction', 'Science Fiction')],
            sorted(
                [(x['term'], x['label']) for x in entries[0]['tags']
                 if x['scheme'] == scheme]
            )
        )

    def test_acquisition_feed_omits_works_with_no_active_license_pool(self):
        work = self._work(title="open access", with_open_access_download=True)
        no_license_pool = self._work(title="no license pool", with_license_pool=False)
        no_download = self._work(title="no download", with_license_pool=True)
        not_open_access = self._work("not open access", with_license_pool=True)
        not_open_access.license_pools[0].open_access = False
        self._db.commit()

        # We get a feed with two entries--the open-access book and
        # the non-open-access book--and two error messages--the book with
        # no license pool and the book but with no download.
        works = self._db.query(Work)
        by_title_feed = AcquisitionFeed(self._db, "test", "url", works)
        by_title_raw = unicode(by_title_feed)
        by_title = feedparser.parse(by_title_raw)

        # We have two entries...
        eq_(2, len(by_title['entries']))
        eq_(["not open access", "open access"], sorted(
            [x['title'] for x in by_title['entries']]))

        # ...and two messages.
        eq_(2,
            by_title_raw.count("I've heard about this work but have no active licenses for it.")
        )

    def test_acquisition_feed_includes_image_links(self):
        lane=self.lanes.by_languages['']['Fantasy']
        work = self._work(genre=Fantasy, language="eng",
                          with_open_access_download=True)
        work.presentation_edition.cover_thumbnail_url = "http://thumbnail/b"
        work.presentation_edition.cover_full_url = "http://full/a"

        with temp_config() as config:
            config['integrations'][Configuration.CDN_INTEGRATION] = {}
            work.calculate_opds_entries(verbose=False)
            feed = feedparser.parse(unicode(work.simple_opds_entry))
            links = sorted([x['href'] for x in feed['entries'][0]['links'] if 
                            'image' in x['rel']])
            eq_(['http://full/a', 'http://thumbnail/b'], links)

    def test_acquisition_feed_image_links_respect_cdn(self):
        work = self._work(genre=Fantasy, language="eng",
                          with_open_access_download=True)
        work.presentation_edition.cover_thumbnail_url = "http://thumbnail.com/b"
        work.presentation_edition.cover_full_url = "http://full.com/a"

        with temp_config() as config:
            config['integrations'][Configuration.CDN_INTEGRATION] = {}
            config['integrations'][Configuration.CDN_INTEGRATION]['thumbnail.com'] = "http://foo/"
            config['integrations'][Configuration.CDN_INTEGRATION]['full.com'] = "http://bar/"
            work.calculate_opds_entries(verbose=False)
            feed = feedparser.parse(work.simple_opds_entry)
            links = sorted([x['href'] for x in feed['entries'][0]['links'] if 
                            'image' in x['rel']])
            eq_(['http://bar/a', 'http://foo/b'], links)

    def test_messages(self):
        """Test the ability to include OPDSMessage objects for a given URN in
        lieu of a proper ODPS entry.
        """
        messages = [
            OPDSMessage("urn:foo", 400, _("msg1")),
            OPDSMessage("urn:bar", 500, _("msg2")),
        ]
        feed = AcquisitionFeed(self._db, "test", "http://the-url.com/",
                               [], precomposed_entries=messages)
        feed = unicode(feed)
        for m in messages:
            assert m.urn in feed
            assert str(m.status_code) in feed
            assert str(m.message) in feed

    def test_precomposed_entries(self):
        """Test the ability to include precomposed OPDS entries
        in a feed.
        """
        entry = AcquisitionFeed.E.entry()
        entry.text='foo'
        feed = AcquisitionFeed(self._db, "test", "http://the-url.com/",
                               works=[], precomposed_entries=[entry])
        feed = unicode(feed)
        assert '<entry>foo</entry>' in feed

    def test_page_feed(self):
        """Test the ability to create a paginated feed of works for a given
        lane.
        """       
        fantasy_lane = self.lanes.by_languages['']['Epic Fantasy']        
        work1 = self._work(genre=Epic_Fantasy, with_open_access_download=True)
        work2 = self._work(genre=Epic_Fantasy, with_open_access_download=True)

        facets = Facets.default()
        pagination = Pagination(size=1)

        def make_page(pagination):
            return AcquisitionFeed.page(
                self._db, "test", self._url, fantasy_lane, TestAnnotator, 
                pagination=pagination, use_materialized_works=False
            )
        cached_works = make_page(pagination)
        parsed = feedparser.parse(unicode(cached_works.content))
        eq_(work1.title, parsed['entries'][0]['title'])

        # Make sure the links are in place.
        [up_link] = self.links(parsed, 'up')
        eq_(TestAnnotator.groups_url(Fantasy), up_link['href'])
        eq_(fantasy_lane.parent.display_name, up_link['title'])

        [start] = self.links(parsed, 'start')
        eq_(TestAnnotator.groups_url(None), start['href'])
        eq_(TestAnnotator.top_level_title(), start['title'])

        [next_link] = self.links(parsed, 'next')
        eq_(TestAnnotator.feed_url(fantasy_lane, facets, pagination.next_page), next_link['href'])

        # This was the first page, so no previous link.
        eq_([], self.links(parsed, 'previous'))

        # Now get the second page and make sure it has a 'previous' link.
        cached_works = make_page(pagination.next_page)
        parsed = feedparser.parse(cached_works.content)
        [previous] = self.links(parsed, 'previous')
        eq_(TestAnnotator.feed_url(fantasy_lane, facets, pagination), previous['href'])
        eq_(work2.title, parsed['entries'][0]['title'])

        # The feed has breadcrumb links
        ancestors = fantasy_lane.visible_ancestors()
        root = ET.fromstring(cached_works.content)
        breadcrumbs = root.find("{%s}breadcrumbs" % AtomFeed.SIMPLIFIED_NS)
        links = breadcrumbs.getchildren()
        eq_(len(ancestors) + 1, len(links))
        eq_(TestAnnotator.top_level_title(), links[0].get("title"))
        eq_(TestAnnotator.default_lane_url(), links[0].get("href"))
        for i, lane in enumerate(reversed(ancestors)):
            eq_(lane.display_name, links[i+1].get("title"))
            eq_(TestAnnotator.lane_url(lane), links[i+1].get("href"))

        # When a feed is created without a cache_type of NO_CACHE,
        # CachedFeeds aren't used.
        old_cache_count = self._db.query(CachedFeed).count()
        raw_page = AcquisitionFeed.page(
            self._db, "test", self._url, fantasy_lane, TestAnnotator,
            pagination=pagination.next_page, cache_type=AcquisitionFeed.NO_CACHE,
            use_materialized_works=False
        )

        # Unicode is returned instead of a CachedFeed object.
        eq_(True, isinstance(raw_page, unicode))
        # No new CachedFeeds have been created.
        eq_(old_cache_count, self._db.query(CachedFeed).count())
        # The entries in the feed are the same as they were when
        # they were cached before.
        eq_(sorted(parsed.entries), sorted(feedparser.parse(raw_page).entries))

    def test_groups_feed(self):
        """Test the ability to create a grouped feed of recommended works for
        a given lane.
        """
        fantasy_lane = self.lanes.by_languages['']['Fantasy']
        fantasy_lane.include_all_feed = False
        work1 = self._work(genre=Epic_Fantasy, with_open_access_download=True)
        work1.quality = 0.75
        work2 = self._work(genre=Urban_Fantasy, with_open_access_download=True)
        work2.quality = 0.75
        with temp_config() as config:
            config['policies'] = {}
            config['policies'][Configuration.FEATURED_LANE_SIZE] = 2
            config['policies'][Configuration.GROUPS_MAX_AGE_POLICY] = Configuration.CACHE_FOREVER
            annotator = TestAnnotatorWithGroup()

            # By policy, group feeds are cached forever, which means
            # an attempt to generate them will fail. You'll get a
            # page-type feed as a consolation prize.

            feed = AcquisitionFeed.groups(
                self._db, "test", self._url, fantasy_lane, annotator, 
                force_refresh=False, use_materialized_works=False
            )
            eq_(CachedFeed.PAGE_TYPE, feed.type)
            cached_groups = AcquisitionFeed.groups(
                self._db, "test", self._url, fantasy_lane, annotator, 
                force_refresh=True, use_materialized_works=False
            )
            parsed = feedparser.parse(cached_groups.content)

            # There are two entries, one for each work.
            e1, e2 = parsed['entries']

            # Each entry has one and only one link.
            [l1], [l2] = e1['links'], e2['links']

            # Those links are 'collection' links that classify the
            # works under their subgenres.
            assert all([l['rel'] == 'collection' for l in (l1, l2)])

            eq_(l1['href'], 'http://group/Epic Fantasy')
            eq_(l1['title'], 'Group Title for Epic Fantasy!')
            eq_(l2['href'], 'http://group/Urban Fantasy')
            eq_(l2['title'], 'Group Title for Urban Fantasy!')

            # The feed itself has an 'up' link which points to the
            # groups for Fiction, and a 'start' link which points to
            # the top-level groups feed.
            [up_link] = self.links(parsed['feed'], 'up')
            eq_("http://groups/Fiction", up_link['href'])
            eq_("Fiction", up_link['title'])

            [start_link] = self.links(parsed['feed'], 'start')
            eq_("http://groups/", start_link['href'])
            eq_(annotator.top_level_title(), start_link['title'])

            # The feed has breadcrumb links
            ancestors = fantasy_lane.visible_ancestors()
            root = ET.fromstring(cached_groups.content)
            breadcrumbs = root.find("{%s}breadcrumbs" % AtomFeed.SIMPLIFIED_NS)
            links = breadcrumbs.getchildren()
            eq_(len(ancestors) + 1, len(links))
            eq_(annotator.top_level_title(), links[0].get("title"))
            eq_(annotator.default_lane_url(), links[0].get("href"))
            for i, lane in enumerate(reversed(ancestors)):
                eq_(lane.display_name, links[i+1].get("title"))
                eq_(annotator.lane_url(lane), links[i+1].get("href"))

            # When a feed is created without a cache_type of NO_CACHE,
            # CachedFeeds aren't used.
            old_cache_count = self._db.query(CachedFeed).count()
            raw_groups = AcquisitionFeed.groups(
                self._db, "test", self._url, fantasy_lane, annotator,
                cache_type=AcquisitionFeed.NO_CACHE, use_materialized_works=False
            )

            # Unicode is returned instead of a CachedFeed object.
            eq_(True, isinstance(raw_groups, unicode))
            # No new CachedFeeds have been created.
            eq_(old_cache_count, self._db.query(CachedFeed).count())
            # The entries in the feed are the same as they were when
            # they were cached before.
            eq_(sorted(parsed.entries), sorted(feedparser.parse(raw_groups).entries))

    def test_groups_feed_with_empty_sublanes_is_page_feed(self):
        """Test that a page feed is returned when the requested groups
        feed has no books in the groups.
        """
        
        test_lane = Lane(self._db, "Test Lane", genres=['Mystery'])

        work1 = self._work(genre=Mystery, with_open_access_download=True)
        work1.quality = 0.75
        work2 = self._work(genre=Mystery, with_open_access_download=True)
        work2.quality = 0.75

        with temp_config() as config:
            config['policies'] = {}
            config['policies'][Configuration.FEATURED_LANE_SIZE] = 2
            config['policies'][Configuration.GROUPS_MAX_AGE_POLICY] = Configuration.CACHE_FOREVER
            annotator = TestAnnotator()

            feed = AcquisitionFeed.groups(
                self._db, "test", self._url, test_lane, annotator,
                force_refresh=True, use_materialized_works=False
            )

            # The feed is filed as a groups feed, even though in
            # form it is a page feed.
            eq_(CachedFeed.GROUPS_TYPE, feed.type)

            parsed = feedparser.parse(feed.content)

            # There are two entries, one for each work.
            e1, e2 = parsed['entries']

            # The entries have no links (no collection links).
            assert all('links' not in entry for entry in [e1, e2])

    def test_search_feed(self):
        """Test the ability to create a paginated feed of works for a given
        search query.
        """
        fantasy_lane = self.lanes.by_languages['']['Epic Fantasy']
        fantasy_lane.searchable = True
        work1 = self._work(genre=Epic_Fantasy, with_open_access_download=True)
        work2 = self._work(genre=Epic_Fantasy, with_open_access_download=True)
        work1.set_presentation_ready()
        work2.set_presentation_ready()
        SessionManager.refresh_materialized_views(self._db)

        pagination = Pagination(size=1)
        search_client = DummyExternalSearchIndex()
        work1.update_external_index(search_client)
        work2.update_external_index(search_client)

        def make_page(pagination):
            return AcquisitionFeed.search(
                self._db, "test", self._url, fantasy_lane, search_client, 
                "fantasy",
                pagination=pagination,
                annotator=TestAnnotator,
            )
        feed = make_page(pagination)
        parsed = feedparser.parse(feed)
        eq_(work1.title, parsed['entries'][0]['title'])

        # Make sure the links are in place.
        [start] = self.links(parsed, 'start')
        eq_(TestAnnotator.groups_url(None), start['href'])
        eq_(TestAnnotator.top_level_title(), start['title'])

        [next_link] = self.links(parsed, 'next')
        eq_(TestAnnotator.search_url(fantasy_lane, "test", pagination.next_page), next_link['href'])

        # This was the first page, so no previous link.
        eq_([], self.links(parsed, 'previous'))

        # Make sure there's an "up" link to the lane that was searched
        [up_link] = self.links(parsed, 'up')
        uplink_url = TestAnnotator.lane_url(fantasy_lane)
        eq_(uplink_url, up_link['href'])
        eq_(fantasy_lane.display_name, up_link['title'])

        # Now get the second page and make sure it has a 'previous' link.
        feed = make_page(pagination.next_page)
        parsed = feedparser.parse(feed)
        [previous] = self.links(parsed, 'previous')
        eq_(TestAnnotator.search_url(fantasy_lane, "test", pagination), previous['href'])
        eq_(work2.title, parsed['entries'][0]['title'])

        # The feed has breadcrumb links
        ancestors = fantasy_lane.visible_ancestors()
        root = ET.fromstring(feed)
        breadcrumbs = root.find("{%s}breadcrumbs" % AtomFeed.SIMPLIFIED_NS)
        links = breadcrumbs.getchildren()
        eq_(len(ancestors) + 2, len(links))
        eq_(TestAnnotator.top_level_title(), links[0].get("title"))
        eq_(TestAnnotator.default_lane_url(), links[0].get("href"))
        for i, lane in enumerate(reversed(ancestors)):
            eq_(lane.display_name, links[i+1].get("title"))
            eq_(TestAnnotator.lane_url(lane), links[i+1].get("href"))
        eq_(fantasy_lane.display_name, links[-1].get("title"))
        eq_(TestAnnotator.lane_url(fantasy_lane), links[-1].get("href"))

    def test_cache(self):
        work1 = self._work(title="The Original Title",
                           genre=Epic_Fantasy, with_open_access_download=True)
        fantasy_lane = self.lanes.by_languages['']['Fantasy']

        def make_page():
            return AcquisitionFeed.page(
                self._db, "test", self._url, fantasy_lane, TestAnnotator, 
                pagination=Pagination.default(), use_materialized_works=False
            )

        with temp_config() as config:
            config['policies'] = {
                Configuration.PAGE_MAX_AGE_POLICY : 10
            }

            cached1 = make_page()
            assert work1.title in cached1.content
            old_timestamp = cached1.timestamp

            work2 = self._work(
                title="A Brand New Title", 
                genre=Epic_Fantasy, with_open_access_download=True
            )

            # The new work does not show up in the feed because 
            # we get the old cached version.
            cached2 = make_page()
            assert work2.title not in cached2.content
            assert cached2.timestamp == old_timestamp
            
            # Change the policy to disable caching, and we get
            # a brand new page with the new work.
            config['policies'][Configuration.PAGE_MAX_AGE_POLICY] = 0

            cached3 = make_page()
            assert cached3.timestamp > old_timestamp
            assert work2.title in cached3.content


class TestAcquisitionFeed(DatabaseTest):

    def test_single_entry(self):

        # Here's a Work with two LicensePools.
        work = self._work(with_open_access_download=True)
        original_pool = work.license_pools[0]
        edition, new_pool = self._edition(
            with_license_pool=True, with_open_access_download=True
        )
        work.license_pools.append(new_pool)

        # The presentation edition of the Work is associated with
        # the first LicensePool added to it.
        eq_(work.presentation_edition, original_pool.presentation_edition)

        # This is the edition used when we create an <entry> tag for
        # this Work.
        entry = AcquisitionFeed.single_entry(
            self._db, work, TestAnnotator
        )
        entry = etree.tostring(entry)
        assert original_pool.presentation_edition.title in entry
        assert new_pool.presentation_edition.title not in entry

    def test_entry_cache_adds_missing_drm_namespace(self):
        
        work = self._work(with_open_access_download=True)

        # This work's OPDS entry was created with a namespace map
        # that did not include the drm: namespace.
        work.simple_opds_entry = "<entry><foo>bar</foo></entry>"
        pool = work.license_pools[0]

        # But now the annotator is set up to insert a tag with that
        # namespace.
        class AddDRMTagAnnotator(TestAnnotator):
            @classmethod
            def annotate_work_entry(
                    cls, work, license_pool, edition, identifier, feed,
                    entry):
                drm_link = OPDSFeed.makeelement("{%s}licensor" % OPDSFeed.DRM_NS)
                entry.extend([drm_link])

        # The entry is retrieved from cache and the appropriate
        # namespace inserted.
        entry = AcquisitionFeed.single_entry(
            self._db, work, AddDRMTagAnnotator
        )
        eq_('<entry xmlns:drm="http://librarysimplified.org/terms/drm"><foo>bar</foo><drm:licensor/></entry>',
            etree.tostring(entry)
        )
        
    def test_error_when_work_has_no_identifier(self):
        """We cannot create an OPDS entry for a Work that cannot be associated
        with an Identifier.
        """
        work = self._work(title=u"Hello, World!", with_license_pool=True)
        work.license_pools[0].identifier = None
        work.presentation_edition.primary_identifier = None
        entry = AcquisitionFeed.single_entry(
            self._db, work, TestAnnotator
        )
        eq_(entry, None)

    def test_error_when_work_has_no_licensepool(self):
        work = self._work()
        feed = AcquisitionFeed(
            self._db, self._str, self._url, [], annotator=Annotator
        )
        entry = feed.create_entry(work)
        expect = AcquisitionFeed.error_message(
            work.presentation_edition.primary_identifier,
            403,
            "I've heard about this work but have no active licenses for it.",
        )
        eq_(expect, entry)

    def test_error_when_work_has_no_presentation_edition(self):
        """We cannot create an OPDS entry (or even an error message) for a
        Work that is disconnected from any Identifiers.
        """
        work = self._work(title=u"Hello, World!", with_license_pool=True)
        work.license_pools[0].presentation_edition = None
        work.presentation_edition = None
        feed = AcquisitionFeed(
            self._db, self._str, self._url, [], annotator=Annotator
        )
        entry = feed.create_entry(work)
        eq_(None, entry)
        
    def test_cache_usage(self):
        work = self._work(with_open_access_download=True)
        feed = AcquisitionFeed(
            self._db, self._str, self._url, [], annotator=Annotator
        )

        # Set the Work's cached OPDS entry to something that's clearly wrong.
        tiny_entry = '<feed>cached entry</feed>'
        work.simple_opds_entry = tiny_entry

        # If we pass in use_cache=True, the cached value is used.
        entry = feed.create_entry(work, use_cache=True)
        eq_(tiny_entry, work.simple_opds_entry)
        eq_(tiny_entry, etree.tostring(entry))

        # If we pass in use_cache=False, a new OPDS entry is created
        # from scratch, but the cache is not updated.
        entry = feed.create_entry(work, use_cache=False)
        assert etree.tostring(entry) != tiny_entry
        eq_(tiny_entry, work.simple_opds_entry)

        # If we pass in force_create, a new OPDS entry is created
        # and the cache is updated.
        entry = feed.create_entry(work, force_create=True)
        entry_string = etree.tostring(entry) 
        assert entry_string != tiny_entry
        eq_(entry_string, work.simple_opds_entry)

    def test_exception_during_entry_creation_is_not_reraised(self):
        # This feed will raise an exception whenever it's asked
        # to create an entry.
        class DoomedFeed(AcquisitionFeed):
            def _create_entry(self, *args, **kwargs):
                raise Exception("I'm doomed!")
        feed = DoomedFeed(
            self._db, self._str, self._url, [], annotator=Annotator
        )
        work = self._work(with_open_access_download=True)

        # But calling create_entry() doesn't raise an exception, it
        # just returns None.
        entry = feed.create_entry(work)
        eq_(entry, None)

    def test_unfilfullable_work(self):
        work = self._work(with_open_access_download=True)
        [pool] = work.license_pools
        entry = AcquisitionFeed.single_entry(
            self._db, work, TestUnfulfillableAnnotator
        )
        expect = AcquisitionFeed.error_message(
            pool.identifier, 403, 
            "I know about this work but can offer no way of fulfilling it."
        )
        eq_(expect, entry)

    def test_format_types(self):
        epub_no_drm, ignore = DeliveryMechanism.lookup(
            self._db, Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.NO_DRM)
        epub_adobe_drm, ignore = DeliveryMechanism.lookup(
            self._db, Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM)
        overdrive_streaming_text, ignore = DeliveryMechanism.lookup(
            self._db, DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE, DeliveryMechanism.OVERDRIVE_DRM)

        eq_([Representation.EPUB_MEDIA_TYPE],
            AcquisitionFeed.format_types(epub_no_drm))
        eq_([DeliveryMechanism.ADOBE_DRM, Representation.EPUB_MEDIA_TYPE],
            AcquisitionFeed.format_types(epub_adobe_drm))
        eq_([OPDSFeed.ENTRY_TYPE, Representation.TEXT_HTML_MEDIA_TYPE + DeliveryMechanism.STREAMING_PROFILE],
            AcquisitionFeed.format_types(overdrive_streaming_text))


class TestLookupAcquisitionFeed(DatabaseTest):

    def entry(self, identifier, work, annotator=VerboseAnnotator, **kwargs):
        """Helper method to create an entry."""
        feed = LookupAcquisitionFeed(
            self._db, u"Feed Title", "http://whatever.io", [],
            annotator=annotator, **kwargs
        )
        entry = feed.create_entry((identifier, work))
        if isinstance(entry, OPDSMessage):
            return feed, entry
        if entry:
            entry = etree.tostring(entry)
        return feed, entry

    def test_create_entry_uses_specified_identifier(self):

        # Here's a Work with two LicensePools.
        work = self._work(with_open_access_download=True)
        original_pool = work.license_pools[0]
        edition, new_pool = self._edition(
            with_license_pool=True, with_open_access_download=True
        )
        work.license_pools.append(new_pool)

        # We can generate two different OPDS feeds for a single work
        # depending on which identifier we look up.
        ignore, e1 = self.entry(original_pool.identifier, work)
        ignore, e2 = self.entry(new_pool.identifier, work)
        assert original_pool.identifier.urn in e1
        assert original_pool.presentation_edition.title in e1
        assert new_pool.identifier.urn not in e1
        assert new_pool.presentation_edition.title not in e1

        assert new_pool.identifier.urn in e2
        assert new_pool.presentation_edition.title in e2
        assert original_pool.identifier.urn not in e2
        assert original_pool.presentation_edition.title not in e2

    def test_error_on_mismatched_identifier(self):
        """We get an error if we try to make it look like an Identifier lookup
        retrieved a Work that's not actually associated with that Identifier.
        """
        work = self._work(with_open_access_download=True)

        # Here's an identifier not associated with any LicensePool.
        identifier = self._identifier()

        feed, entry = self.entry(identifier, work)

        # We were not successful at creating an <entry> for this
        # lookup. We got a OPDSMessage instead of an entry
        eq_(entry,
            OPDSMessage(identifier.urn, 404,
                              "Identifier not found in collection")
        )

        # We also get an error if we use an Identifier that is
        # associated with a LicensePool, but that LicensePool is not
        # associated with the Work.
        edition, lp = self._edition(with_license_pool=True)
        identifier = lp.identifier
        feed, entry = self.entry(identifier, work)
        eq_(entry,
            OPDSMessage(
                identifier.urn, 500,
                'I tried to generate an OPDS entry for the identifier "%s" using a Work not associated with that identifier.' % identifier.urn
            )
        )

    def test_error_when_work_has_no_licensepool(self):
        """Under most circumstances, a Work must have at least one
        LicensePool for a lookup to succeed.
        """

        # Here's a work with no LicensePools.
        work = self._work(title=u"Hello, World!", with_license_pool=False)
        identifier = work.presentation_edition.primary_identifier
        feed, entry = self.entry(identifier, work)

        # By default, a work is treated as 'not in the collection' if
        # there is no LicensePool for it.
        isinstance(entry, OPDSMessage)
        eq_(404, entry.status_code)
        eq_("Identifier not found in collection", entry.message)

    def test_unfilfullable_work(self):
        work = self._work(with_open_access_download=True)
        [pool] = work.license_pools
        feed, entry = self.entry(pool.identifier, work, 
                                 TestUnfulfillableAnnotator)
        expect = AcquisitionFeed.error_message(
            pool.identifier, 403, 
            "I know about this work but can offer no way of fulfilling it."
        )
        eq_(expect, entry)
