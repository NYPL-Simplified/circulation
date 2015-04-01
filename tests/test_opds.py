import feedparser
import datetime
from lxml import etree
from nose.tools import (
    eq_,
    set_trace,
)

from . import (
    DatabaseTest,
)

from model import (
    Contributor,
    DataSource,
    Genre,
    Lane,
    LaneList,
    Measurement,
    Patron,
    Subject,
    Work,
    get_one_or_create,
)

from opds import (    
    AtomFeed,
    OPDSFeed,
    AcquisitionFeed,
    NavigationFeed,
    Annotator,
    VerboseAnnotator,
)

from classifier import (
    Classifier,
    Fantasy,
)

class TestAnnotator(Annotator):

    @classmethod
    def navigation_feed_url(cls, lane):
        url = "http://navigation-feed/"
        if lane and lane.name:
            url += lane.name
        return url

    @classmethod
    def featured_feed_url(cls, lane, order=None):
        url = "http://featured-feed/" + lane.name
        if order:
            url += "?order=%s" % order
        return url

    @classmethod
    def facet_url(cls, facet):
        return "http://facet/" + facet


class TestAnnotators(DatabaseTest):

    def test_all_subjects(self):
        work = self._work(genre="Fiction")
        edition = work.primary_edition
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
        eq_(['Fiction'], category_tags[genre_uri])

    def test_appeals(self):
        work = self._work(with_open_access_download=True)
        work.appeal_language = 0.1
        work.appeal_character = 0.2
        work.appeal_story = 0.3
        work.appeal_setting = 0.4

        category_tags = VerboseAnnotator.categories(work)
        appeal_tags = category_tags[Work.APPEALS_URI]
        expect = [
            (Work.LANGUAGE_APPEAL, 0.1),
            (Work.CHARACTER_APPEAL, 0.2),
            (Work.STORY_APPEAL, 0.3),
            (Work.SETTING_APPEAL, 0.4)
        ]
        actual = [
            (x['term'], x['{http://schema.org/}ratingValue'])
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

        work = self._work(authors=[])
        work.primary_edition.add_contributor(c, Contributor.PRIMARY_AUTHOR_ROLE)

        [same_tag] = VerboseAnnotator.authors(work)
        eq_(tag_string, etree.tostring(same_tag))

    def test_verbose_annotator_mentions_every_author(self):
        work = self._work(authors=[])
        work.primary_edition.add_contributor(
            self._contributor()[0], Contributor.PRIMARY_AUTHOR_ROLE)
        work.primary_edition.add_contributor(
            self._contributor()[0], Contributor.AUTHOR_ROLE)
        work.primary_edition.add_contributor(
            self._contributor()[0], "Illustrator")
        eq_(2, len(VerboseAnnotator.authors(work)))

    def test_ratings(self):
        work = self._work(
            with_license_pool=True, with_open_access_download=True)
        work.quality = 1.0/3
        work.popularity = 0.25
        work.rating = 0.6
        annotator = VerboseAnnotator
        feed = AcquisitionFeed(self._db, self._str, self._url, [work], annotator)
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

class TestOPDS(DatabaseTest):

    def setup(self):
        super(TestOPDS, self).setup()

        self.lanes = LaneList.from_description(
            self._db,
            None,
            [dict(full_name="Fiction",
                  fiction=True,
                  audience=Classifier.AUDIENCE_ADULT,
                  genres=[]),
             Fantasy,
             dict(
                 full_name="Young Adult",
                 fiction=Lane.BOTH_FICTION_AND_NONFICTION,
                 audience=Classifier.AUDIENCE_YOUNG_ADULT,
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
            sublanes = None
            pass

        self.conf = FakeConf()
        self.conf.sublanes = self.lanes
    
    def test_navigation_feed(self):
        original_feed = NavigationFeed.main_feed(self.conf, TestAnnotator)
        parsed = feedparser.parse(unicode(original_feed))
        feed = parsed['feed']

        # There's a self link.
        self_link, start_link = sorted(feed.links)
        eq_("http://navigation-feed/", self_link['href'])

        # There's a link to the top level, which is the same as the
        # self link.
        eq_("http://navigation-feed/", start_link['href'])
        eq_("start", start_link['rel'])
        eq_(NavigationFeed.NAVIGATION_FEED_TYPE, start_link['type'])

        # Every lane has an entry.
        eq_(4, len(parsed['entries']))
        tags = [x['title'] for x in parsed['entries']]
        eq_(['Fantasy', 'Fiction', 'Romance', 'Young Adult'], sorted(tags))

        # Let's look at one entry, Fiction, which has no sublanes.
        toplevel = [x for x in parsed['entries'] if x.title == 'Fiction'][0]
        eq_("http://featured-feed/Fiction", toplevel.id)

        # There are two links to acquisition feeds.
        featured, by_author = sorted(toplevel['links'])
        eq_('http://featured-feed/Fiction', featured['href'])
        eq_("Featured", featured['title'])
        eq_(NavigationFeed.FEATURED_REL, featured['rel'])
        eq_(NavigationFeed.ACQUISITION_FEED_TYPE, featured['type'])

        eq_('http://featured-feed/Fiction?order=author', by_author['href'])
        eq_("Look inside Fiction", by_author['title'])
        # eq_(None, by_author.get('rel'))
        eq_(NavigationFeed.ACQUISITION_FEED_TYPE, by_author['type'])

        # Now let's look at one entry, Romance, which has a sublane.
        toplevel = [x for x in parsed['entries'] if x.title == 'Romance'][0]
        eq_("http://featured-feed/Romance", toplevel.id)

        # Instead of an acquisition feed (by author), we have a navigation feed
        # (the sublanes of Romance).
        featured, sublanes = sorted(toplevel['links'])
        eq_('http://navigation-feed/Romance', sublanes['href'])
        eq_("Look inside Romance", sublanes['title'])
        eq_("subsection", sublanes['rel'])
        eq_(NavigationFeed.NAVIGATION_FEED_TYPE, sublanes['type'])

    def test_navigation_feed_for_sublane(self):
        original_feed = NavigationFeed.main_feed(
            self.conf.sublanes.by_name['Romance'], TestAnnotator)
        parsed = feedparser.parse(unicode(original_feed))
        feed = parsed['feed']

        start_link, up_link, self_link = sorted(feed.links)

        # There's a self link.
        eq_("http://navigation-feed/Romance", self_link['href'])
        eq_("self", self_link['rel'])

        # There's a link to the top level.
        eq_("http://navigation-feed/", start_link['href'])
        eq_("start", start_link['rel'])
        eq_(NavigationFeed.NAVIGATION_FEED_TYPE, start_link['type'])

        # There's a link to one level up.
        eq_("http://navigation-feed/", up_link['href'])
        eq_("up", up_link['rel'])
        eq_(NavigationFeed.NAVIGATION_FEED_TYPE, up_link['type'])


    def test_acquisition_feed(self):
        work = self._work(with_open_access_download=True, authors="Alice")

        feed = AcquisitionFeed(self._db, "test", "http://the-url.com/",
                               [work])
        u = unicode(feed)
        parsed = feedparser.parse(u)
        [with_author] = parsed['entries']
        eq_("Alice", with_author['authors'][0]['name'])

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
        eq_(work.primary_edition.permanent_work_id, 
            entry['simplified_pwid'])

    def test_acquisition_feed_contains_facet_links(self):
        work = self._work(with_open_access_download=True)


        works = self._db.query(Work)
        feed = AcquisitionFeed(self._db, "test", "http://the-url.com/",
                               works, TestAnnotator, "author")
        u = unicode(feed)
        parsed = feedparser.parse(u)
        by_title = parsed['feed']

        by_author, by_title, self_link = sorted(
            by_title['links'], key=lambda x: (x['rel'], x.get('title')))

        eq_("http://the-url.com/", self_link['href'])

        # As we'll see below, the feed parser parses facetGroup as
        # facetgroup and activeFacet as activefacet. As we see here,
        # that's not a problem with the generator code.
        assert 'opds:facetgroup' not in u
        assert 'opds:facetGroup' in u
        assert 'opds:activefacet' not in u
        assert 'opds:activeFacet' in u

        eq_('Sort by', by_author['opds:facetgroup'])
        eq_('http://opds-spec.org/facet', by_author['rel'])
        eq_('true', by_author['opds:activefacet'])
        eq_('Author', by_author['title'])
        eq_(TestAnnotator.facet_url("author"), by_author['href'])

        eq_('Sort by', by_title['opds:facetgroup'])
        eq_('http://opds-spec.org/facet', by_title['rel'])
        eq_('Title', by_title['title'])
        assert not 'opds:activefacet' in by_title
        eq_(TestAnnotator.facet_url("title"), by_title['href'])

    def test_acquisition_feed_includes_available_and_issued_tag(self):
        today = datetime.date.today()
        today_s = today.strftime("%Y-%m-%d")
        the_past = today - datetime.timedelta(days=2)
        the_past_s = the_past.strftime("%Y-%m-%d")
        the_past_time = the_past.strftime(AtomFeed.TIME_FORMAT)

        # This work has both issued and published. issued will be used
        # for the dc:dateCopyrighted tag.
        work1 = self._work(with_open_access_download=True)
        work1.primary_edition.issued = today
        work1.primary_edition.published = the_past
        work1.license_pools[0].availability_time = the_past

        # This work only has published. published will be used for the
        # dc:dateCopyrighted tag.
        work2 = self._work(with_open_access_download=True)
        work2.primary_edition.published = today
        work2.license_pools[0].availability_time = None

        # This work has neither published nor issued. There will be no
        # dc:dateCopyrighted tag.
        work3 = self._work(with_open_access_download=True)
        work3.license_pools[0].availability_time = None

        self._db.commit()
        works = self._db.query(Work)
        with_times = AcquisitionFeed(
            self._db, "test", "url", works, TestAnnotator)
        u = unicode(with_times)
        assert 'dcterms:dateCopyrighted' in u
        with_times = feedparser.parse(u)
        e1, e2, e3 = sorted(
            with_times['entries'], key = lambda x: int(x['title']))

        eq_(the_past_s, e1['dcterms_datecopyrighted'])
        eq_(the_past_time, e1['published'])

        eq_(today_s, e2['dcterms_datecopyrighted'])
        assert not 'published' in e2

        assert not 'dcterms_datecopyrighted' in e3
        assert not 'published' in e3

    def test_acquisition_feed_includes_language_tag(self):
        work = self._work(with_open_access_download=True)
        work.primary_edition.publisher = "The Publisher"
        work2 = self._work(with_open_access_download=True)
        work2.primary_edition.publisher = None

        self._db.commit()

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
        work3 = self._work(with_open_access_download=True)
        work3.audience = None

        self._db.commit()

        works = self._db.query(Work)
        with_audience = AcquisitionFeed(self._db, "test", "url", works)
        u = unicode(with_audience)
        with_audience = feedparser.parse(u)
        entries = sorted(with_audience['entries'], key = lambda x: int(x['title']))
        scheme = "http://schema.org/audience"
        eq_(['Young Adult'],
            [x['term'] for x in entries[0]['tags']
             if x['scheme'] == scheme])

        eq_(['Children'],
            [x['term'] for x in entries[1]['tags']
             if x['scheme'] == scheme])

        eq_([],
            [x['term'] for x in entries[2]['tags']
             if x['scheme'] == scheme])

    def test_acquisition_feed_includes_category_tags_for_appeals(self):
        work = self._work(with_open_access_download=True)
        work.appeal_language = 0.1
        work.appeal_character = 0.2
        work.appeal_story = 0.3
        work.appeal_setting = 0.4

        work2 = self._work(with_open_access_download=True)

        self._db.commit()
        works = self._db.query(Work)
        feed = AcquisitionFeed(self._db, "test", "url", works)
        feed = feedparser.parse(unicode(feed))
        entries = sorted(feed['entries'], key = lambda x: int(x['title']))

        tags = entries[0]['tags']
        matches = [x['term'] for x in tags if x['scheme'] == Work.APPEALS_URI]
        eq_(['Character', 'Language', 'Setting', 'Story'], sorted(matches))

        tags = entries[1]['tags']
        matches = [x['term'] for x in tags if x['scheme'] == Work.APPEALS_URI]
        eq_([], matches)

    def test_acquisition_feed_includes_category_tags_for_genres(self):
        work = self._work(with_open_access_download=True)
        g1, ignore = Genre.lookup(self._db, "Science Fiction")
        g2, ignore = Genre.lookup(self._db, "Romance")
        work.genres = [g1, g2]

        work2 = self._work(with_open_access_download=True)
        work2.genres = []
        work2.fiction = False

        work3 = self._work(with_open_access_download=True)
        work3.genres = []
        work3.fiction = True

        self._db.commit()
        works = self._db.query(Work)
        feed = AcquisitionFeed(self._db, "test", "url", works)
        feed = feedparser.parse(unicode(feed))
        entries = sorted(feed['entries'], key = lambda x: int(x['title']))

        scheme = Subject.SIMPLIFIED_GENRE
        eq_(['Romance', 'Science Fiction'], 
            sorted([x['term'] for x in entries[0]['tags']
                    if x['scheme'] == scheme]))
        eq_(['Nonfiction'], [x['term'] for x in entries[1]['tags']
                             if x['scheme'] == scheme])
        eq_(['Fiction'], [x['term'] for x in entries[2]['tags']
                          if x['scheme'] == scheme])

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

    def test_featured_feed_ignores_low_quality_works(self):
        lane=self.lanes.by_name['Fantasy']
        good = self._work(genre=Fantasy, language="eng",
                          with_open_access_download=True)
        good.quality = 100
        bad = self._work(genre=Fantasy, language="eng",
                         with_open_access_download=True)
        bad.quality = 0

        # We get the good one and omit the bad one.
        feed = AcquisitionFeed.featured("eng", lane, TestAnnotator)
        feed = feedparser.parse(unicode(feed))
        eq_([good.title], [x['title'] for x in feed['entries']])

    def test_acquisition_feed_includes_image_links(self):
        lane=self.lanes.by_name['Fantasy']
        work = self._work(genre=Fantasy, language="eng",
                          with_open_access_download=True)
        work.primary_edition.cover_thumbnail_url = "http://thumbnail/"
        work.primary_edition.cover_full_url = "http://full/"
        feed = AcquisitionFeed.featured("eng", lane, TestAnnotator)
        feed = feedparser.parse(unicode(feed))
        links = sorted([x['href'] for x in feed['entries'][0]['links'] if 
                     'image' in x['rel']])
        eq_(['http://full/', 'http://thumbnail/'], links)
        

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
