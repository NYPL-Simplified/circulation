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
    Genre,
    Measurement,
    Patron,
    SessionManager,
    Subject,
    Work,
    get_one_or_create,
)

from lane import (
    Facets,
    Pagination,
    Lane,
    LaneList,
)

from opds import (    
     AtomFeed,
     AcquisitionFeed,
     Annotator,
     LookupAcquisitionFeed,
     OPDSFeed,
     VerboseAnnotator,
     simplified_ns,
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

class TestAnnotator(Annotator):

    @classmethod
    def lane_url(cls, lane):
        if lane and lane.has_visible_sublane():
            return cls.groups_url(lane)
        elif lane:
            return cls.feed_url(lane)
        else:
            return ""

    @classmethod
    def feed_url(cls, lane, facets=None, pagination=None):
        base = "http://%s/" % lane.url_name
        sep = '?'
        if facets:
            base += sep + facets.query_string
            sep = '&'
        if pagination:
            base += sep + pagination.query_string
        return base

    @classmethod
    def search_url(cls, lane, query, pagination):
        base = "http://search/%s/" % lane.url_name
        sep = '?'
        if pagination:
            base += sep + pagination.query_string
        return base

    @classmethod
    def groups_url(cls, lane):
        if lane:
            name = lane.name
        else:
            name = ""
        return "http://groups/%s" % name

    @classmethod
    def default_lane_url(cls):
        return cls.groups_url(None)

    @classmethod
    def facet_url(cls, facets):
        return "http://facet/" + "&".join(
            ["%s=%s" % (k, v) for k, v in sorted(facets.items())]
        )

    @classmethod
    def top_level_title(cls):
        return "Test Top Level Title"


class TestAnnotatorWithGroup(TestAnnotator):

    def __init__(self):
        self.lanes_by_work = defaultdict(list)

    def group_uri(self, work, license_pool, identifier):
        lanes = self.lanes_by_work.get(work, None)
        if lanes:
            lane_name = lanes[0]['lane'].display_name
        else:
            lane_name = str(work.id)
        return ("http://group/%s" % lane_name,
                "Group Title for %s!" % lane_name)

    def group_uri_for_lane(self, lane):
        if lane:
            return ("http://groups/%s" % lane.display_name, 
                    "Groups of %s" % lane.display_name)
        else:
            return "http://groups/", "Top-level groups"

    def top_level_title(self):
        return "Test Top Level Title"


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

    def test_verbose_annotator_mentions_every_author(self):
        work = self._work(authors=[], with_license_pool=True)
        work.presentation_edition.add_contributor(
            self._contributor()[0], Contributor.PRIMARY_AUTHOR_ROLE)
        work.presentation_edition.add_contributor(
            self._contributor()[0], Contributor.AUTHOR_ROLE)
        work.presentation_edition.add_contributor(
            self._contributor()[0], "Illustrator")
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
        tag = feed.create_entry(work, url, None)

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

        # If there's no series title, the series tag isn't included.
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

        class FakeConf(object):
            name = None
            display_name = None
            sublanes = None
            pass

        self.conf = FakeConf()
        self.conf.sublanes = self.lanes

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

        # We get a feed with only one entry--the one with an open-access
        # license pool and an associated download.
        works = self._db.query(Work)
        by_title = AcquisitionFeed(self._db, "test", "url", works)
        by_title = feedparser.parse(unicode(by_title))
        eq_(2, len(by_title['entries']))
        eq_(["not open access", "open access"], sorted(
            [x['title'] for x in by_title['entries']]))

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
        work.presentation_edition.cover_thumbnail_url = "http://thumbnail/b"
        work.presentation_edition.cover_full_url = "http://full/a"

        with temp_config() as config:
            config['integrations'][Configuration.CDN_INTEGRATION] = {}
            config['integrations'][Configuration.CDN_INTEGRATION][Configuration.CDN_BOOK_COVERS] = "http://foo/"
            work.calculate_opds_entries(verbose=False)
            feed = feedparser.parse(work.simple_opds_entry)
            links = sorted([x['href'] for x in feed['entries'][0]['links'] if 
                            'image' in x['rel']])
            eq_(['http://foo/a', 'http://foo/b'], links)

    def test_messages(self):
        """Test the ability to include messages (with HTTP-style status code)
        for a given URI in lieu of a proper ODPS entry.
        """
        messages = { "urn:foo" : (400, "msg1"),
                     "urn:bar" : (500, "msg2")}
        feed = AcquisitionFeed(self._db, "test", "http://the-url.com/",
                               [], messages_by_urn=messages)
        parsed = feedparser.parse(unicode(feed))
        bar, foo = sorted(parsed['entries'], key = lambda x: x['id'])
        eq_("urn:foo", foo['id'])
        eq_("msg1", foo['simplified_message'])
        eq_("400", foo['simplified_status_code'])

        eq_("urn:bar", bar['id'])
        eq_("msg2", bar['simplified_message'])
        eq_("500", bar['simplified_status_code'])


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
        breadcrumbs = root.find("{%s}breadcrumbs" % simplified_ns)
        links = breadcrumbs.getchildren()
        eq_(len(ancestors) + 1, len(links))
        eq_(TestAnnotator.top_level_title(), links[0].get("title"))
        eq_(TestAnnotator.default_lane_url(), links[0].get("href"))
        for i, lane in enumerate(reversed(ancestors)):
            eq_(lane.display_name, links[i+1].get("title"))
            eq_(TestAnnotator.lane_url(lane), links[i+1].get("href"))

    def test_groups_feed(self):
        """Test the ability to create a grouped feed of recommended works for
        a given lane.
        """
        fantasy_lane = self.lanes.by_languages['']['Fantasy']
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
                False, False
            )
            eq_(CachedFeed.PAGE_TYPE, feed.type)

            cached_groups = AcquisitionFeed.groups(
                self._db, "test", self._url, fantasy_lane, annotator, 
                True, False
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
            breadcrumbs = root.find("{%s}breadcrumbs" % simplified_ns)
            links = breadcrumbs.getchildren()
            eq_(len(ancestors) + 1, len(links))
            eq_(annotator.top_level_title(), links[0].get("title"))
            eq_(annotator.default_lane_url(), links[0].get("href"))
            for i, lane in enumerate(reversed(ancestors)):
                eq_(lane.display_name, links[i+1].get("title"))
                eq_(annotator.lane_url(lane), links[i+1].get("href"))

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
                True, False
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
        breadcrumbs = root.find("{%s}breadcrumbs" % simplified_ns)
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


class TestLookupAcquisitionFeed(DatabaseTest):

    def entry(self, identifier, work, **kwargs):
        """Helper method to create an entry."""
        feed = LookupAcquisitionFeed(
            self._db, u"Feed Title", "http://whatever.io", [],
            annotator=VerboseAnnotator, **kwargs
        )
        entry = feed.create_entry((identifier, work), u"http://lane/")
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
        # lookup.
        expect_status = '<simplified:status_code>404'
        expect_message = '<simplified:message>Identifier not found in collection'
        assert expect_status in entry
        assert expect_message in entry

        # We also get an error if we use an Identifier that is
        # associated with a LicensePool, but that LicensePool is not
        # associated with the Work.
        edition, lp = self._edition(with_license_pool=True)
        feed, entry = self.entry(lp.identifier, work)
        expect_status = '<simplified:status_code>500'
        expect_message = '<simplified:message>I tried to generate an OPDS entry for the identifier "%s" using a Work not associated with that identifier.'
        assert expect_status in entry
        assert (expect_message % lp.identifier.urn) in entry

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
        eq_(True, feed.require_active_licensepool)
        assert "Identifier not found in collection" in entry
        assert work.title not in entry

        # But if the LookupAcquisitionFeed is set up to allow a lookup
        # even in the absense of a LicensePool (as might happen in the
        # metadata wrangler), the same lookup succeeds.
        feed, entry = self.entry(
            identifier, work, require_active_licensepool = False
        )
        assert 'simplified:status_code' not in entry
        assert work.title in entry
