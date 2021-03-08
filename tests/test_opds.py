import datetime
import logging
import re
import xml.etree.ElementTree as ET
from StringIO import StringIO

import feedparser
from flask_babel import lazy_gettext as _
from lxml import etree
from psycopg2.extras import NumericRange

from . import (
    DatabaseTest,
)
from ..classifier import (
    Classifier,
    Contemporary_Romance,
    Epic_Fantasy,
    Fantasy,
    History,
)
from ..config import (
    Configuration,
    temp_config,
)
from ..entrypoint import (
    AudiobooksEntryPoint,
    EbooksEntryPoint,
    EntryPoint,
    EverythingEntryPoint,
)
from ..external_search import MockExternalSearchIndex
from ..facets import FacetConstants
from ..lane import (
    Facets,
    FeaturedFacets,
    Pagination,
    SearchFacets,
    WorkList,
)
from ..lcp.credential import LCPCredentialFactory
from ..model import (
    CachedFeed,
    Contributor,
    CustomList,
    CustomListEntry,
    DataSource,
    DeliveryMechanism,
    ExternalIntegration,
    Genre,
    Measurement,
    Representation,
    Subject,
    Work,
    get_one,
    create,
)
from ..opds import (
    AcquisitionFeed,
    Annotator,
    LookupAcquisitionFeed,
    NavigationFacets,
    NavigationFeed,
    VerboseAnnotator,
    TestAnnotator,
    TestAnnotatorWithGroup,
    TestUnfulfillableAnnotator
)
from ..opds_import import OPDSXMLParser
from ..util.flask_util import (
    OPDSEntryResponse,
    OPDSFeedResponse,
    Response,
)
from ..util.opds_writer import (
    AtomFeed,
    OPDSFeed,
    OPDSMessage,
)


class TestBaseAnnotator(DatabaseTest):

    def test_authors(self):
        # Create an Edition with an author and a narrator.
        edition = self._edition(authors=[])
        edition.add_contributor(
            "Steven King", Contributor.PRIMARY_AUTHOR_ROLE
        )
        edition.add_contributor(
            "Jonathan Frakes", Contributor.NARRATOR_ROLE
        )
        author, contributor = sorted(
            Annotator.authors(None, edition),
            key=lambda x: x.tag
        )


        # The <author> tag indicates a role of 'author', so there's no
        # need for an explicitly specified role property.
        assert 'author' == author.tag
        [name] = author.getchildren()
        assert "name" == name.tag
        assert "King, Steven" == name.text
        assert {} == author.attrib

        # The <contributor> tag includes an explicitly specified role
        # property to explain the nature of the contribution.
        assert 'contributor' == contributor.tag
        [name] = contributor.getchildren()
        assert "name" == name.tag
        assert "Frakes, Jonathan" == name.text
        role_attrib = '{%s}role' % AtomFeed.OPF_NS
        assert (Contributor.MARC_ROLE_CODES[Contributor.NARRATOR_ROLE] ==
            contributor.attrib[role_attrib])

    def test_annotate_work_entry_adds_tags(self):
        work = self._work(with_license_pool=True,
                          with_open_access_download=True)
        work.last_update_time = datetime.datetime(2018, 2, 5, 7, 39, 49, 580651)
        [pool] = work.license_pools
        pool.availability_time = datetime.datetime(2015, 1, 1)

        entry = []
        # This will create four extra tags which could not be
        # generated in the cached entry because they depend on the
        # active LicensePool or identifier: the Atom ID, the distributor,
        # the date published and the date updated.
        annotator = Annotator()
        annotator.annotate_work_entry(work, pool, None, None, None, entry)
        [id, distributor, published, updated] = entry

        id_tag = etree.tounicode(id)
        assert 'id' in id_tag
        assert pool.identifier.urn in id_tag

        assert 'ProviderName="Gutenberg"' in etree.tounicode(distributor)

        published_tag = etree.tounicode(published)
        assert 'published' in published_tag
        assert '2015-01-01' in published_tag

        updated_tag = etree.tounicode(updated)
        assert 'updated' in updated_tag
        assert '2018-02-05' in updated_tag

        entry = []
        # We can pass in a specific update time to override the one
        # found in work.last_update_time.
        annotator.annotate_work_entry(
            work, pool, None, None, None, entry,
            updated=datetime.datetime(2017, 1, 2, 3, 39, 49, 580651)
        )
        [id, distributor, published, updated] = entry
        assert 'updated' in etree.tounicode(updated)
        assert '2017-01-02' in etree.tounicode(updated)

class TestAnnotators(DatabaseTest):

    def test_all_subjects(self):
        self.work = self._work(genre="Fiction", with_open_access_download=True)
        edition = self.work.presentation_edition
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

        # Mock Work.all_identifier_ids (called by VerboseAnnotator.categories)
        # so we can track the value that was passed in for `cutoff`.
        def mock_all_identifier_ids(policy=None):
            self.work.called_with_policy = policy
            # Do the actual work so that categories() gets the
            # correct information.
            return self.work.original_all_identifier_ids(policy)
        self.work.original_all_identifier_ids = self.work.all_identifier_ids
        self.work.all_identifier_ids = mock_all_identifier_ids
        category_tags = VerboseAnnotator.categories(self.work)

        # When we are generating subjects as part of an OPDS feed, by
        # default we set a cutoff of 100 equivalent identifiers. This
        # gives us reasonable worst-case performance at the cost of
        # not showing every single random subject under which an
        # extremely popular book is filed.
        assert 100 == self.work.called_with_policy.equivalent_identifier_cutoff

        ddc_uri = Subject.uri_lookup[Subject.DDC]
        rating_value = '{http://schema.org/}ratingValue'
        assert ([{'term': u'300',
              rating_value: 1,
              'label': u'Social sciences, sociology & anthropology'}] ==
            category_tags[ddc_uri])

        fast_uri = Subject.uri_lookup[Subject.FAST]
        assert ([{'term': u'fast1', 'label': u'name1', rating_value: 1}] ==
            category_tags[fast_uri])

        lcsh_uri = Subject.uri_lookup[Subject.LCSH]
        assert ([{'term': u'lcsh1', 'label': u'name2', rating_value: 2},
             {'term': u'lcsh2', 'label': u'name3', rating_value: 3}] ==
            category_tags[lcsh_uri])

        genre_uri = Subject.uri_lookup[Subject.SIMPLIFIED_GENRE]
        assert [dict(label='Fiction', term=Subject.SIMPLIFIED_GENRE+"Fiction")] == category_tags[genre_uri]

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
        assert set(expect) == set(actual)

    def test_detailed_author(self):
        c, ignore = self._contributor("Familyname, Givenname")
        c.display_name = "Givenname Familyname"
        c.family_name = "Familyname"
        c.wikipedia_name = "Givenname Familyname (Author)"
        c.viaf = "100"
        c.lc = "n100"

        author_tag = VerboseAnnotator.detailed_author(c)

        tag_string = etree.tounicode(author_tag)
        assert "<name>Givenname Familyname</" in tag_string
        assert "<simplified:sort_name>Familyname, Givenname</" in tag_string
        assert "<simplified:wikipedia_name>Givenname Familyname (Author)</" in tag_string
        assert "<schema:sameas>http://viaf.org/viaf/100</" in tag_string
        assert "<schema:sameas>http://id.loc.gov/authorities/names/n100</"

        work = self._work(authors=[], with_license_pool=True)
        work.presentation_edition.add_contributor(c, Contributor.PRIMARY_AUTHOR_ROLE)

        [same_tag] = VerboseAnnotator.authors(work, work.presentation_edition)
        assert tag_string == etree.tounicode(same_tag)

    def test_duplicate_author_names_are_ignored(self):
        # Ignores duplicate author names
        work = self._work(with_license_pool=True)
        duplicate = self._contributor()[0]
        duplicate.sort_name = work.author

        edition = work.presentation_edition
        edition.add_contributor(duplicate, Contributor.AUTHOR_ROLE)

        assert 1 == len(Annotator.authors(work, edition))

    def test_all_annotators_mention_every_relevant_author(self):
        work = self._work(authors=[], with_license_pool=True)
        edition = work.presentation_edition

        primary_author, ignore = self._contributor()
        author, ignore = self._contributor()
        illustrator, ignore = self._contributor()
        barrel_washer, ignore = self._contributor()

        edition.add_contributor(
            primary_author, Contributor.PRIMARY_AUTHOR_ROLE
        )
        edition.add_contributor(author, Contributor.AUTHOR_ROLE)

        # This contributor is relevant because we have a MARC Role Code
        # for the role.
        edition.add_contributor(illustrator, Contributor.ILLUSTRATOR_ROLE)

        # This contributor is not relevant because we have no MARC
        # Role Code for the role.
        edition.add_contributor(barrel_washer, "Barrel Washer")

        role_attrib = '{%s}role' % AtomFeed.OPF_NS
        illustrator_code = Contributor.MARC_ROLE_CODES[
            Contributor.ILLUSTRATOR_ROLE
        ]

        for annotator in Annotator, VerboseAnnotator:
            tags = Annotator.authors(work, edition)
            # We made two <author> tags and one <contributor>
            # tag, for the illustrator.
            assert (['author', 'author', 'contributor'] ==
                [x.tag for x in tags])
            assert ([None, None, illustrator_code] ==
                [x.attrib.get(role_attrib) for x in tags])

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
        assert set(expected) == set(ratings)

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
        assert work.presentation_edition.subtitle == alternative_headline

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
        assert work.presentation_edition.series == schema_entry['name']
        assert str(work.presentation_edition.series_position) == schema_entry['schema:position']

        # The series position can be 0, for a prequel for example.
        work.presentation_edition.series_position = 0
        work.calculate_opds_entries()

        raw_feed = unicode(AcquisitionFeed(
            self._db, self._str, self._url, [work], Annotator
        ))
        assert "schema:Series" in raw_feed
        assert work.presentation_edition.series in raw_feed

        feed = feedparser.parse(unicode(raw_feed))
        schema_entry = feed['entries'][0]['schema_series']
        assert work.presentation_edition.series == schema_entry['name']
        assert str(work.presentation_edition.series_position) == schema_entry['schema:position']

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

    def setup_method(self):
        super(TestOPDS, self).setup_method()

        self.fiction = self._lane("Fiction")
        self.fiction.fiction = True
        self.fiction.audiences = [Classifier.AUDIENCE_ADULT]

        self.fantasy = self._lane(
            "Fantasy", parent=self.fiction, genres="Fantasy"
        )
        self.history = self._lane(
            "History", genres="History"
        )
        self.ya = self._lane("Young Adult")
        self.ya.history = None
        self.ya.audiences = [Classifier.AUDIENCE_YOUNG_ADULT]
        self.romance = self._lane("Romance", genres="Romance")
        self.romance.fiction = True
        self.contemporary_romance = self._lane(
            "Contemporary Romance", parent=self.romance,
            genres="Contemporary Romance"
        )

        self.conf = WorkList()
        self.conf.initialize(
            self._default_library,
            children=[self.fiction, self.fantasy, self.history, self.ya,
                      self.romance]
        )

    def _assert_xml_equal(self, a, b):
        # Compare xml is the same, we use etree to canonicalize the xml
        # then compare the canonical versions
        assert etree.tostring(a, method="c14n2") == \
            etree.tostring(etree.fromstring(b), method="c14n2")

    def test_acquisition_link(self):
        m = AcquisitionFeed.acquisition_link
        rel = AcquisitionFeed.BORROW_REL
        href = self._url

        # A doubly-indirect acquisition link.
        a = m(rel, href, ["text/html", "text/plain", "application/pdf"])
        self._assert_xml_equal(
            a,
            '<link href="%s" rel="http://opds-spec.org/acquisition/borrow" type="text/html"><ns0:indirectAcquisition '
            'xmlns:ns0="http://opds-spec.org/2010/catalog" type="text/plain"><ns0:indirectAcquisition '
            'type="application/pdf"/></ns0:indirectAcquisition></link>' % href
        )

        # A direct acquisition link.
        b = m(rel, href, ["application/epub"])
        self._assert_xml_equal(
            b,
            '<link href="%s" rel="http://opds-spec.org/acquisition/borrow" type="application/epub"/>' % href,
        )

        # A direct acquisition link to a document with embedded access restriction rules.
        c = m(rel, href, ['application/audiobook+json;profile=http://www.feedbooks.com/audiobooks/access-restriction'])
        self._assert_xml_equal(
            c,
            '<link href="%s" rel="http://opds-spec.org/acquisition/borrow" '
            'type="application/audiobook+json;profile=http://www.feedbooks.com/audiobooks/access-restriction"/>' % href
        )

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
        assert OPDSFeed.GROUP_REL == group_link['rel']
        assert expect_uri == group_link['href']
        assert expect_title == group_link['title']

    def test_acquisition_feed(self):
        work = self._work(with_open_access_download=True, authors="Alice")

        feed = AcquisitionFeed(self._db, "test", "http://the-url.com/",
                               [work])
        u = unicode(feed)
        assert '<entry schema:additionalType="http://schema.org/EBook">' in u
        parsed = feedparser.parse(u)
        [with_author] = parsed['entries']
        assert "Alice" == with_author['authors'][0]['name']

    def test_acquisition_feed_includes_license_source(self):
        work = self._work(with_open_access_download=True)
        feed = AcquisitionFeed(self._db, "test", "http://the-url.com/",
                               [work])
        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)

        # The <bibframe:distribution> tag containing the license
        # source should show up once and only once. (At one point a
        # bug caused it to be added to the generated OPDS twice.)
        expect = '<bibframe:distribution bibframe:ProviderName="%s"/>' % (
            gutenberg.name
        )
        assert 1 == unicode(feed).count(expect)

        # If the LicensePool is a stand-in produced for internal
        # processing purposes, it does not represent an actual license for
        # the book, and the <bibframe:distribution> tag is not
        # included.
        internal = DataSource.lookup(self._db, DataSource.INTERNAL_PROCESSING)
        work.license_pools[0].data_source = internal
        feed = AcquisitionFeed(self._db, "test", "http://the-url.com/",
                               [work])
        assert '<bibframe:distribution' not in unicode(feed)

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
        assert (work.presentation_edition.permanent_work_id ==
            entry['simplified_pwid'])

    def test_lcp_acquisition_link_contains_hashed_passphrase(self):
        # Arrange
        lcp_collection = self._collection(protocol=ExternalIntegration.LCP)
        data_source = DataSource.lookup(self._db, DataSource.LCP, autocreate=True)
        data_source_name = data_source.name
        license_pool = self._licensepool(
            edition=None, data_source_name=data_source_name, collection=lcp_collection)
        hashed_passphrase = '12345'
        patron = self._patron()
        lcp_credential_factory = LCPCredentialFactory()
        loan, _ = license_pool.loan_to(patron)
        rel = AcquisitionFeed.ACQUISITION_REL
        href = self._url
        types = [DeliveryMechanism.LCP_DRM, Representation.EPUB_MEDIA_TYPE]
        expected_result = (
            '<link href="{0}" rel="http://opds-spec.org/acquisition" '
            'type="application/vnd.readium.lcp.license.v1.0+json">'
            '<ns0:hashed_passphrase xmlns:ns0="http://readium.org/lcp-specs/ns">{1}</ns0:hashed_passphrase>'
            '<ns0:indirectAcquisition xmlns:ns0="http://opds-spec.org/2010/catalog" type="application/epub+zip"/>'
            '</link>').format(href, hashed_passphrase)

        # Act
        lcp_credential_factory.set_hashed_passphrase(self._db, patron, hashed_passphrase)
        acquisition_link = AcquisitionFeed.acquisition_link(rel, href, types, loan)

        # Assert
        self._assert_xml_equal(acquisition_link, expected_result)

    def test_lane_feed_contains_facet_links(self):
        work = self._work(with_open_access_download=True)

        lane = self._lane()
        facets = Facets.default(self._default_library)

        cached_feed = AcquisitionFeed.page(
            self._db, "title", "http://the-url.com/",
            lane, TestAnnotator, facets=facets
        )

        u = unicode(cached_feed)
        parsed = feedparser.parse(u)
        by_title = parsed['feed']

        [self_link] = self.links(by_title, 'self')
        assert "http://the-url.com/" == self_link['href']
        facet_links = self.links(by_title, AcquisitionFeed.FACET_REL)

        library = self._default_library
        order_facets = library.enabled_facets(
            Facets.ORDER_FACET_GROUP_NAME
        )
        availability_facets = library.enabled_facets(
            Facets.AVAILABILITY_FACET_GROUP_NAME
        )
        collection_facets = library.enabled_facets(
            Facets.COLLECTION_FACET_GROUP_NAME
        )

        def link_for_facets(facets):
            return [x for x in facet_links if facets.query_string in x['href']]

        facets = Facets(library, None, None, None)
        for i1, i2, new_facets, selected in facets.facet_groups:
            links = link_for_facets(new_facets)
            if selected:
                # This facet set is already selected, so it should
                # show up three times--once for every facet group.
                assert 3 == len(links)
            else:
                # This facet set is not selected, so it should have one
                # transition link.
                assert 1 == len(links)

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
        # for the dc:issued tag.
        work1 = self._work(with_open_access_download=True)
        work1.presentation_edition.issued = today
        work1.presentation_edition.published = the_past
        work1.license_pools[0].availability_time = the_distant_past

        # This work only has published. published will be used for the
        # dc:issued tag.
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
        assert 'dcterms:issued' in u

        with_times = etree.parse(StringIO(u))
        entries = OPDSXMLParser._xpath(with_times, '/atom:feed/atom:entry')
        parsed = []
        for entry in entries:
            title = OPDSXMLParser._xpath1(entry, 'atom:title').text
            issued = OPDSXMLParser._xpath1(entry, 'dcterms:issued')
            if issued != None:
                issued = issued.text
            published = OPDSXMLParser._xpath1(entry, 'atom:published')
            if published != None:
                published = published.text
            parsed.append(
                dict(
                    title=title,
                    issued=issued,
                    published=published,
                )
            )
        e1, e2, e3, e4 = sorted(
            parsed, key = lambda x: x['title']
        )
        assert today_s == e1['issued']
        assert the_distant_past_s == e1['published']

        assert the_past_s == e2['issued']
        assert the_distant_past_s == e2['published']

        assert None == e3['issued']
        assert None == e3['published']

        assert None == e4['issued']
        assert None == e4['published']

    def test_acquisition_feed_includes_publisher_and_imprint_tag(self):
        work = self._work(with_open_access_download=True)
        work.presentation_edition.publisher = "The Publisher"
        work.presentation_edition.imprint = "The Imprint"
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
        assert 'The Publisher' == entries[0]['dcterms_publisher']
        assert 'The Imprint' == entries[0]['bib_publisherimprint']
        assert 'publisher' not in entries[1]

    def test_acquisition_feed_includes_audience_as_category(self):
        work = self._work(with_open_access_download=True)
        work.audience = "Young Adult"
        work2 = self._work(with_open_access_download=True)
        work2.audience = "Children"
        work2.target_age = NumericRange(7, 9, '[]')
        work3 = self._work(with_open_access_download=True)
        work3.audience = None
        work4 = self._work(with_open_access_download=True)
        work4.audience = "Adult"
        work4.target_age = NumericRange(18)

        self._db.commit()

        for w in work, work2, work3, work4:
            w.calculate_opds_entries(verbose=False)

        works = self._db.query(Work)
        with_audience = AcquisitionFeed(self._db, "test", "url", works)
        u = unicode(with_audience)
        with_audience = feedparser.parse(u)
        ya, children, no_audience, adult = sorted(with_audience['entries'], key = lambda x: int(x['title']))
        scheme = "http://schema.org/audience"
        assert (
            [('Young Adult', 'Young Adult')] ==
            [(x['term'], x['label']) for x in ya['tags']
             if x['scheme'] == scheme])

        assert (
            [('Children', 'Children')] ==
            [(x['term'], x['label']) for x in children['tags']
             if x['scheme'] == scheme])

        age_scheme = Subject.uri_lookup[Subject.AGE_RANGE]
        assert (
            [('7-9', '7-9')] ==
            [(x['term'], x['label']) for x in children['tags']
             if x['scheme'] == age_scheme])

        assert ([] ==
            [(x['term'], x['label']) for x in no_audience['tags']
             if x['scheme'] == scheme])

        # Even though the 'Adult' book has a target age, the target
        # age is not shown, because target age is only a relevant
        # concept for children's and YA books.
        assert (
            [] ==
            [(x['term'], x['label']) for x in adult['tags']
             if x['scheme'] == age_scheme])

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
        assert ([
            (Work.APPEALS_URI + 'Character', 'Character'),
            (Work.APPEALS_URI + 'Language', 'Language'),
            (Work.APPEALS_URI + 'Setting', 'Setting'),
            (Work.APPEALS_URI + 'Story', 'Story'),
        ] ==
            sorted(matches))

        tags = entries[1]['tags']
        matches = [(x['term'], x['label']) for x in tags if x['scheme'] == Work.APPEALS_URI]
        assert [] == matches

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

        assert ([(scheme+'Nonfiction', 'Nonfiction')] ==
            [(x['term'], x['label']) for x in entries[0]['tags']
             if x['scheme'] == scheme])
        assert ([(scheme+'Fiction', 'Fiction')] ==
            [(x['term'], x['label']) for x in entries[1]['tags']
             if x['scheme'] == scheme])


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
        assert (
            [(scheme+'Romance', 'Romance'),
             (scheme+'Science%20Fiction', 'Science Fiction')] ==
            sorted(
                [(x['term'], x['label']) for x in entries[0]['tags']
                 if x['scheme'] == scheme]
            ))

    def test_acquisition_feed_omits_works_with_no_active_license_pool(self):
        work = self._work(title="open access", with_open_access_download=True)
        no_license_pool = self._work(title="no license pool", with_license_pool=False)
        no_download = self._work(title="no download", with_license_pool=True)
        no_download.license_pools[0].open_access = True
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
        assert 2 == len(by_title['entries'])
        assert ["not open access", "open access"] == sorted(
            [x['title'] for x in by_title['entries']])

        # ...and two messages.
        assert (2 ==
            by_title_raw.count("I've heard about this work but have no active licenses for it."))

    def test_acquisition_feed_includes_image_links(self):
        work = self._work(genre=Fantasy, with_open_access_download=True)
        work.presentation_edition.cover_thumbnail_url = "http://thumbnail/b"
        work.presentation_edition.cover_full_url = "http://full/a"
        work.calculate_opds_entries(verbose=False)

        feed = feedparser.parse(unicode(work.simple_opds_entry))
        links = sorted([x['href'] for x in feed['entries'][0]['links'] if
                        'image' in x['rel']])
        assert ['http://full/a', 'http://thumbnail/b'] == links

    def test_acquisition_feed_image_links_respect_cdn(self):
        work = self._work(genre=Fantasy, with_open_access_download=True)
        work.presentation_edition.cover_thumbnail_url = "http://thumbnail.com/b"
        work.presentation_edition.cover_full_url = "http://full.com/a"

        # Create some CDNS.
        with temp_config() as config:
            config[Configuration.INTEGRATIONS][ExternalIntegration.CDN] = {
                'thumbnail.com' : 'http://foo/',
                'full.com' : 'http://bar/'
            }
            config[Configuration.CDNS_LOADED_FROM_DATABASE] = True
            work.calculate_opds_entries(verbose=False)

        feed = feedparser.parse(work.simple_opds_entry)
        links = sorted([x['href'] for x in feed['entries'][0]['links'] if
                        'image' in x['rel']])
        assert ['http://bar/a', 'http://foo/b'] == links

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
        # Test the ability to include precomposed OPDS entries
        # in a feed.

        entry = AcquisitionFeed.E.entry()
        entry.text='foo'
        feed = AcquisitionFeed(self._db, "test", "http://the-url.com/",
                               works=[], precomposed_entries=[entry])
        feed = unicode(feed)
        assert '<entry>foo</entry>' in feed

    def test_page_feed(self):
        # Test the ability to create a paginated feed of works for a given
        # lane.
        lane = self.contemporary_romance
        work1 = self._work(genre=Contemporary_Romance, with_open_access_download=True)
        work2 = self._work(genre=Contemporary_Romance, with_open_access_download=True)

        search_engine = MockExternalSearchIndex()
        search_engine.bulk_update([work1, work2])

        facets = Facets.default(self._default_library)
        pagination = Pagination(size=1)

        def make_page(pagination):
            return AcquisitionFeed.page(
                self._db, "test", self._url, lane, TestAnnotator,
                pagination=pagination, search_engine=search_engine
            )
        cached_works = unicode(make_page(pagination))
        parsed = feedparser.parse(cached_works)
        assert work1.title == parsed['entries'][0]['title']

        # Make sure the links are in place.
        [up_link] = self.links(parsed, 'up')
        assert TestAnnotator.groups_url(lane.parent) == up_link['href']
        assert lane.parent.display_name == up_link['title']

        [start] = self.links(parsed, 'start')
        assert TestAnnotator.groups_url(None) == start['href']
        assert TestAnnotator.top_level_title() == start['title']

        [next_link] = self.links(parsed, 'next')
        assert TestAnnotator.feed_url(lane, facets, pagination.next_page) == next_link['href']

        # This was the first page, so no previous link.
        assert [] == self.links(parsed, 'previous')

        # Now get the second page and make sure it has a 'previous' link.
        cached_works = unicode(make_page(pagination.next_page))
        parsed = feedparser.parse(cached_works)
        [previous] = self.links(parsed, 'previous')
        assert TestAnnotator.feed_url(lane, facets, pagination) == previous['href']
        assert work2.title == parsed['entries'][0]['title']

        # The feed has breadcrumb links
        parentage = list(lane.parentage)
        root = ET.fromstring(cached_works)
        breadcrumbs = root.find("{%s}breadcrumbs" % AtomFeed.SIMPLIFIED_NS)
        links = breadcrumbs.getchildren()

        # There's one breadcrumb link for each parent Lane, plus one for
        # the top-level.
        assert len(parentage) + 1 == len(links)
        assert TestAnnotator.top_level_title() == links[0].get("title")
        assert TestAnnotator.default_lane_url() == links[0].get("href")
        for i, lane in enumerate(parentage):
            assert lane.display_name == links[i+1].get("title")
            assert TestAnnotator.lane_url(lane) == links[i+1].get("href")

    def test_page_feed_for_worklist(self):
        # Test the ability to create a paginated feed of works for a
        # WorkList instead of a Lane.
        lane = self.conf
        work1 = self._work(genre=Contemporary_Romance, with_open_access_download=True)
        work2 = self._work(genre=Contemporary_Romance, with_open_access_download=True)

        search_engine = MockExternalSearchIndex()
        search_engine.bulk_update([work1, work2])

        facets = Facets.default(self._default_library)
        pagination = Pagination(size=1)

        def make_page(pagination):
            return AcquisitionFeed.page(
                self._db, "test", self._url, lane, TestAnnotator,
                pagination=pagination, search_engine=search_engine
            )
        cached_works = make_page(pagination)
        parsed = feedparser.parse(unicode(cached_works))
        assert work1.title == parsed['entries'][0]['title']

        # Make sure the links are in place.
        # This is the top-level, so no up link.
        assert [] == self.links(parsed, 'up')

        [start] = self.links(parsed, 'start')
        assert TestAnnotator.groups_url(None) == start['href']
        assert TestAnnotator.top_level_title() == start['title']

        [next_link] = self.links(parsed, 'next')
        assert TestAnnotator.feed_url(lane, facets, pagination.next_page) == next_link['href']

        # This was the first page, so no previous link.
        assert [] == self.links(parsed, 'previous')

        # Now get the second page and make sure it has a 'previous' link.
        cached_works = unicode(make_page(pagination.next_page))
        parsed = feedparser.parse(cached_works)
        [previous] = self.links(parsed, 'previous')
        assert TestAnnotator.feed_url(lane, facets, pagination) == previous['href']
        assert work2.title == parsed['entries'][0]['title']

        # The feed has no parents, so no breadcrumbs.
        root = ET.fromstring(cached_works)
        breadcrumbs = root.find("{%s}breadcrumbs" % AtomFeed.SIMPLIFIED_NS)
        assert None == breadcrumbs

    def test_from_query(self):
        """Test creating a feed for a custom list from a query.
        """

        display_name = "custom_list"
        staff_data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        list, ignore = create(self._db, CustomList, name=self._str, library=self._default_library, data_source=staff_data_source)
        work = self._work(with_license_pool=True)
        work2 = self._work(with_license_pool=True)
        list.add_entry(work)
        list.add_entry(work2)

        # get all the entries from a custom list
        query = self._db.query(Work).join(Work.custom_list_entries).filter(CustomListEntry.list_id==list.id)

        pagination = Pagination(size=1)
        worklist = WorkList()
        worklist.initialize(self._default_library, customlists=[list], display_name=display_name)

        def url_for_custom_list(library, list):
            def url_fn(after):
                base = "http://%s/" % display_name
                if after:
                    base += "?after=%s&size=1" % after
                return base
            return url_fn

        url_fn = url_for_custom_list(self._default_library, list)
        def from_query(pagination):
            return AcquisitionFeed.from_query(
                query, self._db, list.name, "url",
                pagination, url_fn, TestAnnotator,
            )

        works = from_query(pagination)
        parsed = feedparser.parse(unicode(works))
        assert 1 == len(parsed['entries'])
        assert list.name == parsed['feed'].title

        [next_link] = self.links(parsed, 'next')
        assert TestAnnotator.feed_url(worklist, pagination=pagination.next_page) == next_link['href']

        # This was the first page, so no previous link.
        assert [] == self.links(parsed, 'previous')

        # Now get the second page and make sure it has a 'previous' link.
        works = from_query(pagination.next_page)
        parsed = feedparser.parse(unicode(works))
        [previous_link] = self.links(parsed, 'previous')
        assert TestAnnotator.feed_url(worklist, pagination=pagination.previous_page) == previous_link['href']
        assert 1 == len(parsed['entries'])
        assert [] == self.links(parsed, 'next')


    def test_groups_feed(self):
        # Test the ability to create a grouped feed of recommended works for
        # a given lane.

        # Every time it's invoked, the mock search index is going to
        # return everything in its index. That's fine -- we're only
        # concerned with _how_ it's invoked -- how many times and in
        # what context.
        #
        # So it's sufficient to create a single work, and the details
        # of the work don't matter. It just needs to have a LicensePool
        # so it'll show up in the OPDS feed.
        work = self._work(title="An epic tome", with_open_access_download=True)
        search_engine = MockExternalSearchIndex()
        search_engine.bulk_update([work])

        # The lane setup does matter a lot -- that's what controls
        # how many times the search functionality is invoked.
        epic_fantasy = self._lane(
            "Epic Fantasy", parent=self.fantasy, genres=["Epic Fantasy"]
        )
        urban_fantasy = self._lane(
            "Urban Fantasy", parent=self.fantasy, genres=["Urban Fantasy"]
        )

        annotator = TestAnnotatorWithGroup()
        private = object()
        cached_groups = AcquisitionFeed.groups(
            self._db, "test", self._url, self.fantasy, annotator,
            max_age=0, search_engine=search_engine,
            search_debug=True, private=private
        )

        # The result is an OPDSFeedResponse object. The 'private'
        # argument, unused by groups(), was passed along into the
        # constructor.
        assert isinstance(cached_groups, OPDSFeedResponse)
        assert private == cached_groups.private

        parsed = feedparser.parse(cached_groups.data)

        # There are three entries in three lanes.
        e1, e2, e3 = parsed['entries']

        # Each entry has one and only one link.
        [l1], [l2], [l3] = [x['links'] for x in parsed['entries']]

        # Those links are 'collection' links that classify the
        # works under their subgenres.
        assert all([l['rel'] == 'collection' for l in (l1, l2)])

        assert l1['href'] == 'http://group/Epic Fantasy'
        assert l1['title'] == 'Group Title for Epic Fantasy!'
        assert l2['href'] == 'http://group/Urban Fantasy'
        assert l2['title'] == 'Group Title for Urban Fantasy!'
        assert l3['href'] == 'http://group/Fantasy'
        assert l3['title'] == 'Group Title for Fantasy!'

        # The feed itself has an 'up' link which points to the
        # groups for Fiction, and a 'start' link which points to
        # the top-level groups feed.
        [up_link] = self.links(parsed['feed'], 'up')
        assert "http://groups/%s" % self.fiction.id == up_link['href']
        assert "Fiction" == up_link['title']

        [start_link] = self.links(parsed['feed'], 'start')
        assert "http://groups/" == start_link['href']
        assert annotator.top_level_title() == start_link['title']

        # The feed has breadcrumb links
        ancestors = list(self.fantasy.parentage)
        root = ET.fromstring(cached_groups.data)
        breadcrumbs = root.find("{%s}breadcrumbs" % AtomFeed.SIMPLIFIED_NS)
        links = breadcrumbs.getchildren()
        assert len(ancestors) + 1 == len(links)
        assert annotator.top_level_title() == links[0].get("title")
        assert annotator.default_lane_url() == links[0].get("href")
        for i, lane in enumerate(reversed(ancestors)):
            assert lane.display_name == links[i+1].get("title")
            assert annotator.lane_url(lane) == links[i+1].get("href")

    def test_empty_groups_feed(self):
        # Test the case where a grouped feed turns up nothing.

        # A Lane, and a Work not in the Lane.
        test_lane = self._lane("Test Lane", genres=['Mystery'])
        work1 = self._work(genre=History, with_open_access_download=True)

        # Mock search index and Annotator.
        search_engine = MockExternalSearchIndex()
        class Mock(TestAnnotator):
            def annotate_feed(self, feed, worklist):
                self.called = True
        annotator = Mock()

        # Build a grouped feed for the lane.
        feed = AcquisitionFeed.groups(
            self._db, "test", self._url, test_lane, annotator,
            max_age=0, search_engine=search_engine
        )

        # A grouped feed was cached for the lane, but there were no
        # relevant works found,.
        cached = get_one(self._db, CachedFeed, lane=test_lane)
        assert CachedFeed.GROUPS_TYPE == cached.type

        # So the feed contains no entries.
        parsed = feedparser.parse(feed)
        assert [] == parsed['entries']

        # but our mock Annotator got a chance to modify the feed in place.
        assert True == annotator.called

    def test_search_feed(self):
        # Test the ability to create a paginated feed of works for a given
        # search query.
        fantasy_lane = self.fantasy
        work1 = self._work(genre=Epic_Fantasy, with_open_access_download=True)
        work2 = self._work(genre=Epic_Fantasy, with_open_access_download=True)

        pagination = Pagination(size=1)
        search_client = MockExternalSearchIndex()
        search_client.bulk_update([work1, work2])
        facets = SearchFacets(order="author", min_score=10)

        private = object()

        def make_page(pagination):
            return AcquisitionFeed.search(
                self._db, "test", self._url, fantasy_lane, search_client,
                "fantasy",
                pagination=pagination,
                facets=facets,
                annotator=TestAnnotator,
                private=private
            )
        response = make_page(pagination)
        assert isinstance(response, OPDSFeedResponse)
        assert OPDSFeed.DEFAULT_MAX_AGE == response.max_age
        assert OPDSFeed.ACQUISITION_FEED_TYPE == response.content_type
        assert private == response.private

        parsed = feedparser.parse(response.data)
        assert work1.title == parsed['entries'][0]['title']

        # Make sure the links are in place.
        [start] = self.links(parsed, 'start')
        assert TestAnnotator.groups_url(None) == start['href']
        assert TestAnnotator.top_level_title() == start['title']

        [next_link] = self.links(parsed, 'next')
        expect = TestAnnotator.search_url(
            fantasy_lane, "test", pagination.next_page, facets=facets
        )
        assert expect == next_link['href']

        # This is tested elsewhere, but let's make sure
        # SearchFacets-specific fields like order and min_score are
        # propagated to the next-page URL.
        assert all(x in expect for x in ('order=author', 'min_score=10'))

        # This was the first page, so no previous link.
        assert [] == self.links(parsed, 'previous')

        # Make sure there's an "up" link to the lane that was searched
        [up_link] = self.links(parsed, 'up')
        uplink_url = TestAnnotator.lane_url(fantasy_lane)
        assert uplink_url == up_link['href']
        assert fantasy_lane.display_name == up_link['title']

        # Now get the second page and make sure it has a 'previous' link.
        feed = unicode(make_page(pagination.next_page))
        parsed = feedparser.parse(feed)
        [previous] = self.links(parsed, 'previous')
        expect = TestAnnotator.search_url(
            fantasy_lane, "test", pagination, facets=facets
        )
        assert expect == previous['href']
        assert all(x in expect for x in ('order=author', 'min_score=10'))

        assert work2.title == parsed['entries'][0]['title']

        # The feed has no breadcrumb links, since we're not
        # searching the lane -- just using some aspects of the lane
        # to guide the search.
        parentage = list(fantasy_lane.parentage)
        root = ET.fromstring(feed)
        breadcrumbs = root.find("{%s}breadcrumbs" % AtomFeed.SIMPLIFIED_NS)
        assert None == breadcrumbs

    def test_cache(self):
        work1 = self._work(title="The Original Title",
                           genre=Epic_Fantasy, with_open_access_download=True)
        fantasy_lane = self.fantasy

        search_engine = MockExternalSearchIndex()
        search_engine.bulk_update([work1])
        def make_page():
            return AcquisitionFeed.page(
                self._db, "test", self._url, fantasy_lane, TestAnnotator,
                pagination=Pagination.default(), search_engine=search_engine
            )

        response1 = make_page()
        assert work1.title in unicode(response1)
        cached = get_one(self._db, CachedFeed, lane=fantasy_lane)
        old_timestamp = cached.timestamp

        work2 = self._work(
            title="A Brand New Title",
            genre=Epic_Fantasy, with_open_access_download=True
        )
        search_engine.bulk_update([work2])

        # The new work does not show up in the feed because
        # we get the old cached version.
        response2 = make_page()
        assert work2.title not in unicode(response2)
        assert cached.timestamp == old_timestamp

        # Change the WorkList's MAX_CACHE_AGE to disable caching, and
        # we get a brand new page with the new work.
        fantasy_lane.MAX_CACHE_AGE = 0
        response3 = make_page()
        assert cached.timestamp > old_timestamp
        assert work2.title in unicode(response3)


class TestAcquisitionFeed(DatabaseTest):

    def test_page(self):
        # Verify that AcquisitionFeed.page() returns an appropriate OPDSFeedResponse

        wl = WorkList()
        wl.initialize(self._default_library)
        private = object()
        response = AcquisitionFeed.page(
            self._db, "feed title", "url", wl, TestAnnotator,
            max_age=10, private=private
        )

        # The result is an OPDSFeedResponse. The 'private' argument,
        # unused by page(), was passed along into the constructor.
        assert isinstance(response, OPDSFeedResponse)
        assert 10 == response.max_age
        assert private == response.private

        assert '<title>feed title</title>' in response.data

    def test_as_response(self):
        # Verify the ability to convert an AcquisitionFeed object to an
        # OPDSFeedResponse containing the feed.
        feed = AcquisitionFeed(self._db, "feed title", "http://url/", [], TestAnnotator)

        # Some other piece of code set expectations for how this feed should
        # be cached.
        response = feed.as_response(max_age=101, private=False)
        assert 200 == response.status_code

        # We get an OPDSFeedResponse containing the feed in its
        # entity-body.
        assert isinstance(response, OPDSFeedResponse)
        assert '<title>feed title</title>' in response.data

        # The caching expectations are respected.
        assert 101 == response.max_age
        assert False == response.private

    def test_as_error_response(self):
        # Verify the ability to convert an AcquisitionFeed object to an
        # OPDSFeedResponse that is to be treated as an error message.
        feed = AcquisitionFeed(self._db, "feed title", "http://url/", [], TestAnnotator)

        # Some other piece of code set expectations for how this feed should
        # be cached.
        kwargs = dict(max_age=101, private=False)

        # But we know that something has gone wrong and the feed is
        # being served as an error message.
        response = feed.as_error_response(**kwargs)
        assert isinstance(response, OPDSFeedResponse)

        # The content of the feed is unchanged.
        assert 200 == response.status_code
        assert '<title>feed title</title>' in response.data

        # But the max_age and private settings have been overridden.
        assert 0 == response.max_age
        assert True == response.private

    def test_add_entrypoint_links(self):
        """Verify that add_entrypoint_links calls _entrypoint_link
        on every EntryPoint passed in.
        """
        m = AcquisitionFeed.add_entrypoint_links

        old_entrypoint_link = AcquisitionFeed._entrypoint_link
        class Mock(object):
            attrs = dict(href="the response")

            def __init__(self):
                self.calls = []

            def __call__(self, *args):
                self.calls.append(args)
                return self.attrs

        mock = Mock()
        old_entrypoint_link = AcquisitionFeed._entrypoint_link
        AcquisitionFeed._entrypoint_link = mock

        xml = etree.fromstring("<feed/>")
        feed = OPDSFeed("title", "url")
        feed.feed = xml
        entrypoints = [AudiobooksEntryPoint, EbooksEntryPoint]
        url_generator = object()
        AcquisitionFeed.add_entrypoint_links(
            feed, url_generator, entrypoints, EbooksEntryPoint,
            "Some entry points"
        )

        # Two different calls were made to the mock method.
        c1, c2 = mock.calls

        # The first entry point is not selected.
        assert (c1 ==
            (url_generator, AudiobooksEntryPoint, EbooksEntryPoint, True, "Some entry points"))
        # The second one is selected.
        assert (c2 ==
            (url_generator, EbooksEntryPoint, EbooksEntryPoint, False, "Some entry points"))

        # Two identical <link> tags were added to the <feed> tag, one
        # for each call to the mock method.
        l1, l2 = list(xml.iterchildren())
        for l in l1, l2:
            assert "link" == l.tag
            assert mock.attrs == l.attrib
        AcquisitionFeed._entrypoint_link = old_entrypoint_link

        # If there is only one facet in the facet group, no links are
        # added.
        xml = etree.fromstring("<feed/>")
        feed.feed = xml
        mock.calls = []
        entrypoints = [EbooksEntryPoint]
        AcquisitionFeed.add_entrypoint_links(
            feed, url_generator, entrypoints, EbooksEntryPoint,
            "Some entry points"
        )
        assert [] == mock.calls

    def test_entrypoint_link(self):
        """Test the _entrypoint_link method's ability to create
        attributes for <link> tags.
        """
        m = AcquisitionFeed._entrypoint_link
        def g(entrypoint):
            """A mock URL generator."""
            return "%s" % (entrypoint.INTERNAL_NAME)

        # If the entry point is not registered, None is returned.
        assert None == m(g, object(), object(), True, "group")

        # Now make a real set of link attributes.
        l = m(g, AudiobooksEntryPoint, AudiobooksEntryPoint, False, "Grupe")

        # The link is identified as belonging to an entry point-type
        # facet group.
        assert l['rel'] == AcquisitionFeed.FACET_REL
        assert (l['{http://librarysimplified.org/terms/}facetGroupType'] ==
            FacetConstants.ENTRY_POINT_REL)
        assert 'Grupe' == l['{http://opds-spec.org/2010/catalog}facetGroup']

        # This facet is the active one in the group.
        assert 'true' == l['{http://opds-spec.org/2010/catalog}activeFacet']

        # The URL generator was invoked to create the href.
        assert l['href'] == g(AudiobooksEntryPoint)

        # The facet title identifies it as a way to look at audiobooks.
        assert EntryPoint.DISPLAY_TITLES[AudiobooksEntryPoint] == l['title']

        # Now try some variants.

        # Here, the entry point is the default one.
        l = m(g, AudiobooksEntryPoint, AudiobooksEntryPoint, True, "Grupe")

        # This may affect the URL generated for the facet link.
        assert l['href'] == g(AudiobooksEntryPoint)

        # Here, the entry point for which we're generating the link is
        # not the selected one -- EbooksEntryPoint is.
        l = m(g, AudiobooksEntryPoint, EbooksEntryPoint, True, "Grupe")

        # This means the 'activeFacet' attribute is not present.
        assert '{http://opds-spec.org/2010/catalog}activeFacet' not in l

    def test_license_tags_no_loan_or_hold(self):
        edition, pool = self._edition(with_license_pool=True)
        availability, holds, copies = AcquisitionFeed.license_tags(
            pool, None, None
        )
        assert dict(status='available') == availability.attrib
        assert dict(total='0') == holds.attrib
        assert dict(total='1', available='1') == copies.attrib

    def test_license_tags_hold_position(self):
        # When a book is placed on hold, it typically takes a while
        # for the LicensePool to be updated with the new number of
        # holds. This test verifies the normal and exceptional
        # behavior used to generate the opds:holds tag in different
        # scenarios.
        edition, pool = self._edition(with_license_pool=True)
        patron = self._patron()

        # If the patron's hold position is less than the total number
        # of holds+reserves, that total is used as opds:total.
        pool.patrons_in_hold_queue = 3
        hold, is_new = pool.on_hold_to(patron, position=1)

        availability, holds, copies = AcquisitionFeed.license_tags(
            pool, None, hold
        )
        assert '1' == holds.attrib['position']
        assert '3' == holds.attrib['total']

        # If the patron's hold position is missing, we assume they
        # are last in the list.
        hold.position = None
        availability, holds, copies = AcquisitionFeed.license_tags(
            pool, None, hold
        )
        assert '3' == holds.attrib['position']
        assert '3' == holds.attrib['total']

        # If the patron's current hold position is greater than the
        # total recorded number of holds+reserves, their position will
        # be used as the value of opds:total.
        hold.position = 5
        availability, holds, copies = AcquisitionFeed.license_tags(
            pool, None, hold
        )
        assert '5' == holds.attrib['position']
        assert '5' == holds.attrib['total']

        # A patron earlier in the holds queue may see a different
        # total number of holds, but that's fine -- it doesn't matter
        # very much to that person the precise number of people behind
        # them in the queue.
        hold.position = 4
        availability, holds, copies = AcquisitionFeed.license_tags(
            pool, None, hold
        )
        assert '4' == holds.attrib['position']
        assert '4' == holds.attrib['total']

        # If the patron's hold position is zero (because the book is
        # reserved to them), we do not represent them as having a hold
        # position (so no opds:position), but they still count towards
        # opds:total in the case where the LicensePool's information
        # is out of date.
        hold.position = 0
        pool.patrons_in_hold_queue = 0
        availability, holds, copies = AcquisitionFeed.license_tags(
            pool, None, hold
        )
        assert 'position' not in holds.attrib
        assert '1' == holds.attrib['total']

    def test_license_tags_show_unlimited_access_books(self):
        # Arrange
        edition, pool = self._edition(with_license_pool=True)
        pool.open_access = False
        pool.self_hosted = False
        pool.unlimited_access = True

        # Act
        tags = AcquisitionFeed.license_tags(
            pool, None, None
        )

        # Assert
        assert 1 == len(tags)

        [tag] = tags

        assert ('status' in tag.attrib) == True
        assert 'available' == tag.attrib['status']
        assert ('holds' in tag.attrib) == False
        assert ('copies' in tag.attrib) == False

    def test_license_tags_show_self_hosted_books(self):
        # Arrange
        edition, pool = self._edition(with_license_pool=True)
        pool.self_hosted = True
        pool.open_access = False
        pool.licenses_available = 0
        pool.licenses_owned = 0

        # Act
        tags = AcquisitionFeed.license_tags(
            pool, None, None
        )

        # Assert
        assert 1 == len(tags)
        assert 'status' in tags[0].attrib
        assert 'available' == tags[0].attrib['status']

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
        assert work.presentation_edition == original_pool.presentation_edition

        # This is the edition used when we create an <entry> tag for
        # this Work.
        private = object()
        entry = AcquisitionFeed.single_entry(
            self._db, work, TestAnnotator, private=private
        )
        assert isinstance(entry, OPDSEntryResponse)

        # We provided a value for private, which was used.  We didn't
        # provide value for max_age, and zero was used instead of the
        # ten-minute default typical for OPDS feeds.
        assert 0 == entry.max_age
        assert entry.private == private

        assert original_pool.presentation_edition.title in entry.data
        assert new_pool.presentation_edition.title not in entry.data

        # If the edition was issued before 1980, no datetime formatting error
        # is raised.
        work.simple_opds_entry = work.verbose_opds_entry = None
        five_hundred_years = datetime.timedelta(days=(500*365))
        work.presentation_edition.issued = (
            datetime.datetime.utcnow() - five_hundred_years
        )

        entry = AcquisitionFeed.single_entry(self._db, work, TestAnnotator)

        expected = str(work.presentation_edition.issued.date())
        assert expected in entry.data

    def test_single_entry_is_opds_message(self):
        # When single_entry has to deal with an 'OPDS entry' that
        # turns out to be an error message, caching rules are
        # overridden to treat the 'entry' as a private error message.
        work = self._work()

        # We plan on caching the OPDS entry as a public, long-lived
        # document.
        is_public = dict(max_age=200, private=False)

        # But something goes wrong in create_entry() and we get an
        # error instead.
        class MockAcquisitionFeed(AcquisitionFeed):
            def create_entry(*args, **kwargs):
                return OPDSMessage("urn", 500, "oops")

        response = MockAcquisitionFeed.single_entry(
            self._db, work, object(), **is_public
        )

        # We got an OPDS entry containing the message.
        assert isinstance(response, OPDSEntryResponse)
        assert 200 == response.status_code
        assert '500' in response.data
        assert 'oops' in response.data

        # Our caching preferences were overridden.
        assert True == response.private
        assert 0 == response.max_age

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
        assert ('<entry xmlns:drm="http://librarysimplified.org/terms/drm"><foo>bar</foo><drm:licensor/></entry>' ==
            unicode(entry))

    def test_error_when_work_has_no_identifier(self):
        # We cannot create an OPDS entry for a Work that cannot be associated
        # with an Identifier.
        work = self._work(title=u"Hello, World!", with_license_pool=True)
        work.license_pools[0].identifier = None
        work.presentation_edition.primary_identifier = None
        entry = AcquisitionFeed.single_entry(
            self._db, work, TestAnnotator
        )
        assert entry == None

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
        assert expect == entry

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
        assert None == entry

    def test_cache_usage(self):
        work = self._work(with_open_access_download=True)
        feed = AcquisitionFeed(
            self._db, self._str, self._url, [], annotator=Annotator
        )

        # Set the Work's cached OPDS entry to something that's clearly wrong.
        tiny_entry = '<feed>cached entry</feed>'
        work.simple_opds_entry = tiny_entry

        # If we pass in use_cache=True, the cached value is used as a basis
        # for the annotated entry.
        entry = feed.create_entry(work, use_cache=True)
        assert tiny_entry == work.simple_opds_entry

        # We know what the final value looks like -- it's the cached entry
        # run through `Annotator.annotate_work_entry`.
        [pool] = work.license_pools
        xml = etree.fromstring(work.simple_opds_entry)
        annotator = Annotator()
        annotator.annotate_work_entry(
            work, pool, pool.presentation_edition, pool.identifier, feed,
            xml
        )
        assert etree.tounicode(xml) == etree.tounicode(entry)

        # If we pass in use_cache=False, a new OPDS entry is created
        # from scratch, but the cache is not updated.
        entry = feed.create_entry(work, use_cache=False)
        assert etree.tounicode(entry) != tiny_entry
        assert tiny_entry == work.simple_opds_entry

        # If we pass in force_create, a new OPDS entry is created
        # and the cache is updated.
        entry = feed.create_entry(work, force_create=True)
        entry_string = etree.tounicode(entry)
        assert entry_string != tiny_entry
        assert work.simple_opds_entry != tiny_entry

        # Again, we got entry_string by running the (new) cached value
        # through `Annotator.annotate_work_entry`.
        full_entry = etree.fromstring(work.simple_opds_entry)
        annotator.annotate_work_entry(
            work, pool, pool.presentation_edition, pool.identifier, feed,
            full_entry
        )
        assert entry_string == etree.tounicode(full_entry)

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
        assert entry == None

    def test_unfilfullable_work(self):
        work = self._work(with_open_access_download=True)
        [pool] = work.license_pools
        response = AcquisitionFeed.single_entry(
            self._db, work, TestUnfulfillableAnnotator,
        )
        assert isinstance(response, Response)
        expect = AcquisitionFeed.error_message(
            pool.identifier, 403,
            "I know about this work but can offer no way of fulfilling it."
        )
        # The status code equivalent inside the OPDS message has not affected
        # the status code of the Response itself.
        assert 200 == response.status_code
        assert unicode(expect) == unicode(response)

    def test_format_types(self):
        m = AcquisitionFeed.format_types

        epub_no_drm, ignore = DeliveryMechanism.lookup(
            self._db, Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.NO_DRM)
        assert [Representation.EPUB_MEDIA_TYPE] == m(epub_no_drm)

        epub_adobe_drm, ignore = DeliveryMechanism.lookup(
            self._db, Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM)
        assert ([DeliveryMechanism.ADOBE_DRM, Representation.EPUB_MEDIA_TYPE] ==
            m(epub_adobe_drm))

        overdrive_streaming_text, ignore = DeliveryMechanism.lookup(
            self._db, DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
            DeliveryMechanism.OVERDRIVE_DRM
        )
        assert (
            [OPDSFeed.ENTRY_TYPE,
             Representation.TEXT_HTML_MEDIA_TYPE + DeliveryMechanism.STREAMING_PROFILE] ==
            m(overdrive_streaming_text))

        audiobook_drm, ignore = DeliveryMechanism.lookup(
            self._db, Representation.AUDIOBOOK_MANIFEST_MEDIA_TYPE,
            DeliveryMechanism.FEEDBOOKS_AUDIOBOOK_DRM
        )

        assert (
            [Representation.AUDIOBOOK_MANIFEST_MEDIA_TYPE + DeliveryMechanism.FEEDBOOKS_AUDIOBOOK_PROFILE] ==
            m(audiobook_drm))

        # Test a case where there is a DRM scheme but no underlying
        # content type.
        findaway_manifest, ignore = DeliveryMechanism.lookup(
            self._db, DeliveryMechanism.FINDAWAY_DRM, None
        )
        assert ([DeliveryMechanism.FINDAWAY_DRM] ==
            AcquisitionFeed.format_types(findaway_manifest))

    def test_add_breadcrumbs(self):
        _db = self._db

        def getElementChildren(feed):
            f = feed.feed[0]
            children = f.getchildren()
            return children

        class MockFeed(AcquisitionFeed):
            def __init__(self):
                super(MockFeed, self).__init__(
                    _db, "", "", [], annotator=TestAnnotator()
                )
                self.feed = []

        lane = self._lane(display_name="lane")
        sublane = self._lane(parent=lane, display_name="sublane")
        subsublane = self._lane(parent=sublane, display_name="subsublane")
        subsubsublane = self._lane(parent=subsublane,
                                   display_name="subsubsublane")

        top_level = object()
        ep = AudiobooksEntryPoint

        def assert_breadcrumbs(
            expect_breadcrumbs_for, lane, **add_breadcrumbs_kwargs
        ):
            # Create breadcrumbs leading up to `lane` and verify that
            # there is a breadcrumb for everything in
            # `expect_breadcrumbs_for` -- Lanes, EntryPoints, and the
            # top-level lane. Verify that the titles and URLs of the
            # breadcrumbs match what we expect.
            #
            # For easier reading, all assertions in this test are
            # written as calls to this function.
            feed = MockFeed()
            annotator = TestAnnotator()

            entrypoint = add_breadcrumbs_kwargs.get('entrypoint', None)
            include_lane = add_breadcrumbs_kwargs.get('include_lane', False)

            feed.add_breadcrumbs(lane, **add_breadcrumbs_kwargs)

            if not expect_breadcrumbs_for:
                # We are expecting no breadcrumbs at all;
                # nothing should have been added to the feed.
                assert [] == feed.feed
                return

            # At this point we expect at least one breadcrumb.
            crumbs = getElementChildren(feed)

            entrypoint_selected = False
            entrypoint_query = "?entrypoint="

            # First, compare the titles of the breadcrumbs to what was
            # passed in. This makes test writing much easier.
            def title(x):
                if x is top_level:
                    return annotator.top_level_title()
                elif x is ep:
                    return x.INTERNAL_NAME
                else:
                    return x.display_name

            expect_titles = [title(x) for x in expect_breadcrumbs_for]
            actual_titles = [x.attrib.get('title') for x in crumbs]
            assert expect_titles == actual_titles

            # Now, compare the URLs of the breadcrumbs. This is
            # trickier, mainly because the URLs change once an
            # entrypoint is selected.
            previous_breadcrumb_url = None
            actual_urls = []

            for i, crumb in enumerate(crumbs):
                expect = expect_breadcrumbs_for[i]
                actual_url = crumb.attrib.get("href")

                if expect is top_level:
                    # Breadcrumb for the library root.
                    expect_url = annotator.default_lane_url()
                elif expect is ep:
                    # Breadcrumb for the entrypoint selection.

                    # Beyond this point all URLs must propagate the
                    # selected entrypoint.
                    entrypoint_selected = True
                    entrypoint_query += expect.INTERNAL_NAME

                    # The URL for this breadcrumb is the URL for the
                    # previous breadcrumb with the addition of the
                    # entrypoint selection query.
                    expect_url = (
                        previous_breadcrumb_url + entrypoint_query
                    )
                else:
                    # Breadcrumb for a lane.

                    # The breadcrumb URL is determined by the
                    # Annotator.
                    lane_url = annotator.lane_url(expect)
                    if entrypoint_selected:
                        # All breadcrumbs after the entrypoint selection
                        # must propagate the entrypoint.
                        expect_url = lane_url + entrypoint_query
                    else:
                        expect_url = lane_url

                logging.debug(
                    "%s: expect=%s actual=%s", expect_titles[i],
                    expect_url, actual_url
                )
                assert expect_url == actual_url

                # Keep track of the URL just used, in case the next
                # breadcrumb is the same URL but with an entrypoint
                # selection appended.
                previous_breadcrumb_url = actual_url

        # That was a complicated method, but now our assertions
        # are very easy to write and understand.

        # At the top level, there are no breadcrumbs whatsoever.
        assert_breadcrumbs([], None)

        # It doesn't matter if an entrypoint is selected.
        assert_breadcrumbs([], None, entrypoint=ep)

        # A lane with no entrypoint -- note that the breadcrumbs stop
        # _before_ the lane in question.
        assert_breadcrumbs([top_level], lane)

        # If you pass include_lane=True into add_breadcrumbs, the lane
        # itself is included.
        assert_breadcrumbs([top_level, lane], lane, include_lane=True)

        # A lane with an entrypoint selected
        assert_breadcrumbs([top_level, ep], lane, entrypoint=ep)
        assert_breadcrumbs(
            [top_level, ep, lane],
            lane, entrypoint=ep, include_lane=True
        )

        # One lane level down.
        assert_breadcrumbs([top_level, lane], sublane)
        assert_breadcrumbs([top_level, ep, lane], sublane, entrypoint=ep)
        assert_breadcrumbs(
            [top_level, ep, lane, sublane],
            sublane, entrypoint=ep, include_lane=True
        )

        # Two lane levels down.
        assert_breadcrumbs([top_level, lane, sublane], subsublane)
        assert_breadcrumbs(
            [top_level, ep, lane, sublane],
            subsublane, entrypoint=ep
        )

        # Three lane levels down.
        assert_breadcrumbs(
            [top_level, lane, sublane, subsublane],
            subsubsublane,
        )

        assert_breadcrumbs(
            [top_level, ep, lane, sublane, subsublane],
            subsubsublane, entrypoint=ep
        )

        # Make the sublane a root lane for a certain patron type, and
        # the breadcrumbs will be start at that lane -- we won't see
        # the sublane's parent or the library root.
        sublane.root_for_patron_type = ["ya"]
        assert_breadcrumbs([], sublane)

        assert_breadcrumbs(
            [sublane, subsublane],
            subsubsublane
        )

        assert_breadcrumbs(
            [sublane, subsublane, subsubsublane],
            subsubsublane, include_lane=True
        )

        # However, if an entrypoint is selected we will see a
        # breadcrumb for it between the patron root lane and its
        # child.
        assert_breadcrumbs(
            [sublane, ep, subsublane],
            subsubsublane, entrypoint=ep
        )

        assert_breadcrumbs(
            [sublane, ep, subsublane, subsubsublane],
            subsubsublane, entrypoint=ep, include_lane=True
        )

    def test_add_breadcrumb_links(self):

        class MockFeed(AcquisitionFeed):
            add_link_calls = []
            add_breadcrumbs_call = None
            current_entrypoint = None
            def add_link_to_feed(self, **kwargs):
                self.add_link_calls.append(kwargs)

            def add_breadcrumbs(self, lane, entrypoint):
                self.add_breadcrumbs_call = (lane, entrypoint)

            def show_current_entrypoint(self, entrypoint):
                self.current_entrypoint = entrypoint

        annotator = TestAnnotator
        feed = MockFeed(self._db, "title", "url", [], annotator=annotator)

        lane = self._lane()
        sublane = self._lane(parent=lane)
        ep = AudiobooksEntryPoint
        feed.add_breadcrumb_links(sublane, ep)

        # add_link_to_feed was called twice, to create the 'start' and
        # 'up' links.
        start, up = feed.add_link_calls
        assert 'start' == start['rel']
        assert annotator.top_level_title() == start['title']

        assert 'up' == up['rel']
        assert lane.display_name == up['title']

        # The Lane and EntryPoint were passed into add_breadcrumbs.
        assert (sublane, ep) == feed.add_breadcrumbs_call

        # The EntryPoint was passed into show_current_entrypoint.
        assert ep == feed.current_entrypoint

    def test_show_current_entrypoint(self):
        """Calling AcquisitionFeed.show_current_entrypoint annotates
        the top-level <feed> tag with information about the currently
        selected entrypoint, if any.
        """
        feed = AcquisitionFeed(self._db, "title", "url", [], annotator=None)
        assert feed.CURRENT_ENTRYPOINT_ATTRIBUTE not in feed.feed.attrib

        # No entry point, no annotation.
        feed.show_current_entrypoint(None)

        ep = AudiobooksEntryPoint
        feed.show_current_entrypoint(ep)
        assert ep.URI == feed.feed.attrib[feed.CURRENT_ENTRYPOINT_ATTRIBUTE]

    def test_facet_links_unrecognized_facets(self):
        # AcquisitionFeed.facet_links does not produce links for any
        # facet groups or facets not known to the current version of
        # the system, because it doesn't know what the links should look
        # like.
        class MockAnnotator(object):
            def facet_url(self, new_facets):
                return "url: " + new_facets

        class MockFacets(object):
            @property
            def facet_groups(self):
                """Yield a facet group+facet 4-tuple that passes the test we're
                running (which will be turned into a link), and then a
                bunch that don't (which will be ignored).
                """

                # Real facet group, real facet
                yield (
                    Facets.COLLECTION_FACET_GROUP_NAME,
                    Facets.COLLECTION_FULL,
                    "try the featured collection instead",
                    True,
                )

                # Real facet group, nonexistent facet
                yield (
                    Facets.COLLECTION_FACET_GROUP_NAME,
                    "no such facet",
                    "this facet does not exist",
                    True,
                )

                # Nonexistent facet group, real facet
                yield (
                    "no such group",
                    Facets.COLLECTION_FULL,
                    "this facet exists but it's in a nonexistent group",
                    True,
                )

                # Nonexistent facet group, nonexistent facet
                yield (
                    "no such group",
                    "no such facet",
                    "i just don't know",
                    True,
                )

        class MockFeed(AcquisitionFeed):
            links = []
            @classmethod
            def facet_link(cls, url, facet_title, group_title, selected):
                # Return the passed-in objects as is.
                return (url, facet_title, group_title, selected)

        annotator = MockAnnotator()
        facets = MockFacets()

        # The only 4-tuple yielded by facet_groups was passed on to us.
        # The link was run through MockAnnotator.facet_url(),
        # and the human-readable titles were found using lookups.
        #
        # The other three 4-tuples were ignored since we don't know
        # how to generate human-readable titles for them.
        [[url, facet, group, selected]] = MockFeed.facet_links(
            annotator, facets
        )
        assert 'url: try the featured collection instead' == url
        assert Facets.FACET_DISPLAY_TITLES[Facets.COLLECTION_FULL] == facet
        assert (Facets.GROUP_DISPLAY_TITLES[Facets.COLLECTION_FACET_GROUP_NAME] ==
            group)
        assert True == selected


class TestLookupAcquisitionFeed(DatabaseTest):

    def feed(self, annotator=VerboseAnnotator, **kwargs):
        """Helper method to create a LookupAcquisitionFeed."""
        return LookupAcquisitionFeed(
            self._db, u"Feed Title", "http://whatever.io", [],
            annotator=annotator, **kwargs
        )

    def entry(self, identifier, work, annotator=VerboseAnnotator, **kwargs):
        """Helper method to create an entry."""
        feed = self.feed(annotator, **kwargs)
        entry = feed.create_entry((identifier, work))
        if isinstance(entry, OPDSMessage):
            return feed, entry
        if entry:
            entry = etree.tounicode(entry)
        return feed, entry

    def test_create_entry_uses_specified_identifier(self):

        # Here's a Work with two LicensePools.
        work = self._work(with_open_access_download=True)
        original_pool = work.license_pools[0]
        edition, new_pool = self._edition(
            with_license_pool=True, with_open_access_download=True
        )
        work.license_pools.append(new_pool)

        # We can generate two different OPDS entries for a single work
        # depending on which identifier we look up.
        ignore, e1 = self.entry(original_pool.identifier, work)
        assert original_pool.identifier.urn in e1
        assert original_pool.presentation_edition.title in e1
        assert new_pool.identifier.urn not in e1
        assert new_pool.presentation_edition.title not in e1

        # Passing in the other identifier gives an OPDS entry with the
        # same bibliographic data (taken from the original pool's
        # presentation edition) but with different identifier
        # information.
        i = new_pool.identifier
        ignore, e2 = self.entry(i, work)
        assert new_pool.identifier.urn in e2
        assert new_pool.presentation_edition.title not in e2
        assert original_pool.presentation_edition.title in e2
        assert original_pool.identifier.urn not in e2

    def test_error_on_mismatched_identifier(self):
        """We get an error if we try to make it look like an Identifier lookup
        retrieved a Work that's not actually associated with that Identifier.
        """
        work = self._work(with_open_access_download=True)

        # Here's an identifier not associated with any LicensePool or
        # Work.
        identifier = self._identifier()

        # It doesn't make sense to make an OPDS feed out of that
        # Identifier and a totally random Work.
        expect_error = 'I tried to generate an OPDS entry for the identifier "%s" using a Work not associated with that identifier.'
        feed, entry = self.entry(identifier, work)
        assert (
            entry ==OPDSMessage(
                identifier.urn, 500, expect_error  % identifier.urn
            ))

        # Even if the Identifier does have a Work, if the Works don't
        # match, we get the same error.
        edition, lp = self._edition(with_license_pool=True)
        work2 = lp.calculate_work()
        feed, entry = self.entry(lp.identifier, work)
        assert (entry ==
            OPDSMessage(
                lp.identifier.urn, 500, expect_error % lp.identifier.urn
            ))

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
        assert 404 == entry.status_code
        assert "Identifier not found in collection" == entry.message

    def test_unfilfullable_work(self):
        work = self._work(with_open_access_download=True)
        [pool] = work.license_pools
        feed, entry = self.entry(pool.identifier, work,
                                 TestUnfulfillableAnnotator)
        expect = AcquisitionFeed.error_message(
            pool.identifier, 403,
            "I know about this work but can offer no way of fulfilling it."
        )
        assert expect == entry

    def test_create_entry_uses_cache_for_all_licensepools_for_work(self):
        """A Work's cached OPDS entries can be reused by all LicensePools for
        that Work, even LicensePools associated with different
        identifiers.
        """
        class InstrumentableActiveLicensePool(VerboseAnnotator):
            """A mock class that lets us control the output of
            active_license_pool.
            """

            ACTIVE = None

            @classmethod
            def active_licensepool_for(cls, work):
                return cls.ACTIVE
        feed = self.feed(annotator=InstrumentableActiveLicensePool())

        # Here are two completely different LicensePools for the same work.
        work = self._work(with_license_pool=True)
        work.verbose_opds_entry = "<entry>Cached</entry>"
        [pool1] = work.license_pools
        identifier1 = pool1.identifier

        collection2 = self._collection()
        edition2 = self._edition()
        pool2 = self._licensepool(edition=edition2, collection=collection2)
        identifier2 = pool2.identifier
        work.license_pools.append(pool2)

        # Regardless of which LicensePool the annotator thinks is
        # 'active', passing in (identifier, work) will use the cache.
        m = feed.create_entry
        annotator = feed.annotator

        annotator.ACTIVE = pool1
        assert "Cached" == m((pool1.identifier, work)).text

        annotator.ACTIVE = pool2
        assert "Cached" == m((pool2.identifier, work)).text

        # If for some reason we pass in an identifier that is not
        # associated with the active license pool, we don't get
        # anything.
        work.license_pools = [pool1]
        result = m((identifier2, work))
        assert isinstance(result, OPDSMessage)
        assert (
            'using a Work not associated with that identifier.'
            in result.message
        )


class TestEntrypointLinkInsertion(DatabaseTest):
    """Verify that the three main types of OPDS feeds -- grouped,
    paginated, and search results -- will all include links to the same
    feed but through a different entry point.
    """

    def setup_method(self):
        super(TestEntrypointLinkInsertion, self).setup_method()

        # Mock for AcquisitionFeed.add_entrypoint_links
        class Mock(object):
            def add_entrypoint_links(self, *args):
                self.called_with = args
        self.mock = Mock()

        # A WorkList with no EntryPoints -- should not call the mock method.
        self.no_eps = WorkList()
        self.no_eps.initialize(
            library=self._default_library, display_name="no_eps"
        )

        # A WorkList with two EntryPoints -- may call the mock method
        # depending on circumstances.
        self.entrypoints = [AudiobooksEntryPoint, EbooksEntryPoint]
        self.wl = WorkList()
        # The WorkList must have at least one child, or we won't generate
        # a real groups feed for it.
        self.lane = self._lane()
        self.wl.initialize(library=self._default_library, display_name="wl",
        entrypoints=self.entrypoints, children=[self.lane])

        def works(_db, **kwargs):
            """Mock WorkList.works so we don't need any actual works
            to run the test.
            """
            return []
        self.no_eps.works = works
        self.wl.works = works

        self.annotator = TestAnnotator
        self.old_add_entrypoint_links = AcquisitionFeed.add_entrypoint_links
        AcquisitionFeed.add_entrypoint_links = self.mock.add_entrypoint_links

    def teardown_method(self):
        super(TestEntrypointLinkInsertion, self).teardown_method()
        AcquisitionFeed.add_entrypoint_links = self.old_add_entrypoint_links

    def test_groups(self):
        # When AcquisitionFeed.groups() generates a grouped
        # feed, it will link to different entry points into the feed,
        # assuming the WorkList has different entry points.
        def run(wl=None, facets=None):
            """Call groups() and see what add_entrypoint_links
            was called with.
            """
            self.mock.called_with = None
            AcquisitionFeed.groups(
                self._db, "title", "url", wl, self.annotator,
                max_age=0, facets=facets,
            )
            return self.mock.called_with

        # This WorkList has no entry points, so the mock method is not
        # even called.
        assert None == run(self.no_eps)

        # A WorkList with entry points does cause the mock method
        # to be called.
        facets = FeaturedFacets(
            minimum_featured_quality=self._default_library.minimum_featured_quality,
            entrypoint=EbooksEntryPoint
        )
        feed, make_link, entrypoints, selected = run(self.wl, facets)

        # add_entrypoint_links was passed both possible entry points
        # and the selected entry point.
        assert self.wl.entrypoints == entrypoints
        assert selected == EbooksEntryPoint

        # The make_link function that was passed in calls
        # TestAnnotator.groups_url() when passed an EntryPoint.
        assert "http://groups/?entrypoint=Book" == make_link(EbooksEntryPoint)

    def test_page(self):
        # When AcquisitionFeed.page() generates the first page of a paginated
        # list, it will link to different entry points into the list,
        # assuming the WorkList has different entry points.

        def run(wl=None, facets=None, pagination=None):
            """Call page() and see what add_entrypoint_links
            was called with.
            """
            self.mock.called_with = None
            private = object()
            AcquisitionFeed.page(
                self._db, "title", "url", wl, self.annotator,
                max_age=0, facets=facets,
                pagination=pagination, private=private
            )

            return self.mock.called_with

        # The WorkList has no entry points, so the mock method is not
        # even called.
        assert None == run(self.no_eps)

        # Let's give the WorkList two possible entry points, and choose one.
        facets = Facets.default(self._default_library).navigate(
            entrypoint=EbooksEntryPoint
        )
        feed, make_link, entrypoints, selected = run(self.wl, facets)

        # This time, add_entrypoint_links was called, and passed both
        # possible entry points and the selected entry point.
        assert self.wl.entrypoints == entrypoints
        assert selected == EbooksEntryPoint

        # The make_link function that was passed in calls
        # TestAnnotator.feed_url() when passed an EntryPoint. The
        # Facets object's other facet groups are propagated in this URL.
        first_page_url = "http://wl/?available=all&collection=full&entrypoint=Book&order=author"
        assert first_page_url == make_link(EbooksEntryPoint)

        # Pagination information is not propagated through entry point links
        # -- you always start at the beginning of the list.
        pagination = Pagination(offset=100)
        feed, make_link, entrypoints, selected = run(
            self.wl, facets, pagination
        )
        assert first_page_url == make_link(EbooksEntryPoint)

    def test_search(self):
        # When AcquisitionFeed.search() generates the first page of
        # search results, it will link to related searches for different
        # entry points, assuming the WorkList has different entry points.
        def run(wl=None, facets=None, pagination=None):
            """Call search() and see what add_entrypoint_links
            was called with.
            """
            self.mock.called_with = None
            AcquisitionFeed.search(
                self._db, "title", "url", wl, None, None,
                annotator=self.annotator, facets=facets,
                pagination=pagination
            )
            return self.mock.called_with

        # Mock search() so it never tries to return anything.
        def mock_search(self, *args, **kwargs):
            return []
        self.no_eps.search = mock_search
        self.wl.search = mock_search

        # This WorkList has no entry points, so the mock method is not
        # even called.
        assert None == run(self.no_eps)

        # The mock method is called for a WorkList that does have
        # entry points.
        facets = SearchFacets().navigate(entrypoint=EbooksEntryPoint)
        assert isinstance(facets, SearchFacets)
        feed, make_link, entrypoints, selected = run(self.wl, facets)

        # Since the SearchFacets has more than one entry point,
        # the EverythingEntryPoint is prepended to the list of possible
        # entry points.
        assert (
            [EverythingEntryPoint, AudiobooksEntryPoint, EbooksEntryPoint] ==
            entrypoints)

        # add_entrypoint_links was passed the three possible entry points
        # and the selected entry point.
        assert selected == EbooksEntryPoint

        # The make_link function that was passed in calls
        # TestAnnotator.search_url() when passed an EntryPoint.
        first_page_url = 'http://wl/?available=all&collection=full&entrypoint=Book&order=relevance'
        assert first_page_url == make_link(EbooksEntryPoint)

        # Pagination information is not propagated through entry point links
        # -- you always start at the beginning of the list.
        pagination = Pagination(offset=100)
        feed, make_link, entrypoints, selected = run(
            self.wl, facets, pagination
        )
        assert first_page_url == make_link(EbooksEntryPoint)


class TestNavigationFacets(object):

    def test_feed_type(self):
        # If a navigation feed is built via CachedFeed.fetch, it will be
        # filed as a navigation feed.
        assert CachedFeed.NAVIGATION_TYPE == NavigationFacets.CACHED_FEED_TYPE


class TestNavigationFeed(DatabaseTest):

    def setup_method(self):
        super(TestNavigationFeed, self).setup_method()
        self.fiction = self._lane("Fiction")
        self.fantasy = self._lane(
            "Fantasy", parent=self.fiction)
        self.romance = self._lane(
            "Romance", parent=self.fiction)
        self.contemporary_romance = self._lane(
            "Contemporary Romance", parent=self.romance)

    def test_add_entry(self):
        feed = NavigationFeed("title", "http://navigation")
        feed.add_entry("http://example.com", "Example", "text/html")
        parsed = feedparser.parse(unicode(feed))
        [entry] = parsed["entries"]
        assert "Example" == entry["title"]
        [link] = entry["links"]
        assert "http://example.com" == link["href"]
        assert "text/html" == link["type"]
        assert "subsection" == link["rel"]

    def test_navigation_with_sublanes(self):
        private = object()
        response = NavigationFeed.navigation(
            self._db, "Navigation", "http://navigation",
            self.fiction, TestAnnotator, max_age=42, private=private
        )

        # We got an OPDSFeedResponse back. The values we passed in for
        # max_age and private were propagated to the response
        # constructor.
        assert isinstance(response, OPDSFeedResponse)
        assert 42 == response.max_age
        assert private == response.private

        # The media type of this response is different than from the
        # typical OPDSFeedResponse.
        assert OPDSFeed.NAVIGATION_FEED_TYPE == response.content_type

        parsed = feedparser.parse(response.data)

        assert "Navigation" == parsed["feed"]["title"]
        [self_link] = parsed["feed"]["links"]
        assert "http://navigation" == self_link["href"]
        assert "self" == self_link["rel"]
        assert "http://navigation" == parsed["feed"]["id"]
        [fantasy, romance] = sorted(parsed["entries"], key=lambda x: x["title"])

        assert self.fantasy.display_name == fantasy["title"]
        assert "http://%s/" % self.fantasy.id == fantasy["id"]
        [fantasy_link] = fantasy["links"]
        assert "http://%s/" % self.fantasy.id == fantasy_link["href"]
        assert "subsection" == fantasy_link["rel"]
        assert NavigationFeed.ACQUISITION_FEED_TYPE == fantasy_link["type"]

        assert self.romance.display_name == romance["title"]
        assert "http://navigation/%s" % self.romance.id == romance["id"]
        [romance_link] = romance["links"]
        assert "http://navigation/%s" % self.romance.id == romance_link["href"]
        assert "subsection" == romance_link["rel"]
        assert NavigationFeed.NAVIGATION_FEED_TYPE == romance_link["type"]

        # The feed was cached.
        cached = get_one(self._db, CachedFeed)
        assert "http://%s/" % self.fantasy.id in cached.content

    def test_navigation_without_sublanes(self):
        feed = NavigationFeed.navigation(
            self._db, "Navigation", "http://navigation",
            self.fantasy, TestAnnotator)
        parsed = feedparser.parse(unicode(feed))
        assert "Navigation" == parsed["feed"]["title"]
        [self_link] = parsed["feed"]["links"]
        assert "http://navigation" == self_link["href"]
        assert "self" == self_link["rel"]
        assert "http://navigation" == parsed["feed"]["id"]
        [fantasy] = parsed["entries"]

        assert "All " + self.fantasy.display_name == fantasy["title"]
        assert "http://%s/" % self.fantasy.id == fantasy["id"]
        [fantasy_link] = fantasy["links"]
        assert "http://%s/" % self.fantasy.id == fantasy_link["href"]
        assert "subsection" == fantasy_link["rel"]
        assert NavigationFeed.ACQUISITION_FEED_TYPE == fantasy_link["type"]
