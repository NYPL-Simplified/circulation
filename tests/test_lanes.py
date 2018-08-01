# encoding: utf-8
from collections import Counter
from nose.tools import set_trace, eq_, assert_raises
import json
import datetime

from . import (
    DatabaseTest,
)

from core.classifier import Classifier
from core.lane import (
    Lane,
    WorkList,
)
from core.metadata_layer import Metadata
from core.lane import FacetsWithEntryPoint
from core.model import (
    create,
    Contribution,
    Contributor,
    Edition,
    SessionManager,
    DataSource,
    ExternalIntegration,
    Library,
    MaterializedWorkWithGenre,
)

from api.config import (
    Configuration,
    temp_config,
)
from api.lanes import (
    create_default_lanes,
    create_lanes_for_large_collection,
    create_lane_for_small_collection,
    create_lane_for_tiny_collection,
    create_world_languages_lane,
    _lane_configuration_from_collection_sizes,
    load_lanes,
    ContributorLane,
    CrawlableCollectionBasedLane,
    CrawlableFacets,
    CrawlableCustomListBasedLane,
    FeaturedSeriesFacets,
    RecommendationLane,
    RelatedBooksLane,
    SeriesLane,
    WorkBasedLane,
)
from api.novelist import MockNoveListAPI


class TestLaneCreation(DatabaseTest):

    def test_create_lanes_for_large_collection(self):
        languages = ['eng', 'spa']
        create_lanes_for_large_collection(self._db, self._default_library, languages)
        lanes = self._db.query(Lane).filter(Lane.parent_id==None).order_by(Lane.priority).all()

        # We have five top-level lanes.
        eq_(5, len(lanes))
        eq_(
            ['Fiction', 'Nonfiction', 'Young Adult Fiction', 
             'Young Adult Nonfiction', 'Children and Middle Grade'],
            [x.display_name for x in lanes]
        )
        for lane in lanes:
            eq_(self._default_library, lane.library)
            # They all are restricted to English and Spanish.
            eq_(x.languages, languages)

            # They have no restrictions on media type -- that's handled
            # with entry points.
            eq_(None, x.media)

        eq_(
            ['Fiction', 'Nonfiction', 'Young Adult Fiction',
             'Young Adult Nonfiction', 'Children and Middle Grade'],
            [x.display_name for x in lanes]
        )
        

        # The Adult Fiction and Adult Nonfiction lanes reproduce the
        # genre structure found in the genre definitions.
        fiction, nonfiction = lanes[0:2]
        [sf] = [x for x in fiction.sublanes if 'Science Fiction' in x.display_name]
        [periodicals] = [x for x in nonfiction.sublanes if 'Periodicals' in x.display_name]
        eq_(True, sf.fiction)
        eq_("Science Fiction", sf.display_name)
        assert 'Science Fiction' in [genre.name for genre in sf.genres]

        [nonfiction_humor] = [x for x in nonfiction.sublanes 
                              if 'Humor' in x.display_name]
        eq_(False, nonfiction_humor.fiction)
        
        [fiction_humor] = [x for x in fiction.sublanes 
                           if 'Humor' in x.display_name]
        eq_(True, fiction_humor.fiction)

        [space_opera] = [x for x in sf.sublanes if 'Space Opera' in x.display_name]
        eq_(True, sf.fiction)
        eq_("Space Opera", space_opera.display_name)
        eq_(["Space Opera"], [genre.name for genre in space_opera.genres])

        [history] = [x for x in nonfiction.sublanes if 'History' in x.display_name]
        eq_(False, history.fiction)
        eq_("History", history.display_name)
        assert 'History' in [genre.name for genre in history.genres]
        [european_history] = [x for x in history.sublanes if 'European History' in x.display_name]
        assert 'European History' in [genre.name for genre in european_history.genres]

        # Delete existing lanes.
        for lane in self._db.query(Lane).filter(Lane.library_id==self._default_library.id):
            self._db.delete(lane)

        # If there's an NYT Best Sellers integration and we create the lanes again...
        integration, ignore = create(
            self._db, ExternalIntegration, goal=ExternalIntegration.METADATA_GOAL,
            protocol=ExternalIntegration.NYT)

        create_lanes_for_large_collection(self._db, self._default_library, languages)
        lanes = self._db.query(Lane).filter(Lane.parent_id==None).order_by(Lane.priority).all()

        # Now we have six top-level lanes, with best sellers at the beginning.
        eq_(
            [u'Best Sellers', 'Fiction', 'Nonfiction', 'Young Adult Fiction', 
             'Young Adult Nonfiction', 'Children and Middle Grade'],
            [x.display_name for x in lanes]
        )

        # Each sublane other than best sellers also contains a best sellers lane.
        for sublane in lanes[1:]:
            best_sellers = sublane.visible_children[0]
            eq_("Best Sellers", best_sellers.display_name)

        # The best sellers lane has a data source.
        nyt_data_source = DataSource.lookup(self._db, DataSource.NYT)
        eq_(nyt_data_source, lanes[0].list_datasource)

    def test_create_world_languages_lane(self):
        # If there are no small or tiny collections, calling
        # create_world_languages_lane does not create any lanes or change
        # the priority.
        new_priority = create_world_languages_lane(
            self._db, self._default_library, [], [], priority=10
        )
        eq_(10, new_priority)
        eq_([], self._db.query(Lane).all())

        # If there are lanes to be created, create_world_languages_lane
        # creates them.
        new_priority = create_world_languages_lane(
            self._db, self._default_library,
            ["eng"], [["spa", "fre"]], priority=10
        )

        # priority has been incremented to make room for the newly
        # created lane.
        eq_(11, new_priority)

        # One new top-level lane has been created. It contains books
        # from all three languages mentioned in its children.
        top_level = self._db.query(Lane).filter(Lane.parent==None).one()
        eq_("World Languages", top_level.display_name)
        eq_(set(['spa', 'fre', 'eng']), top_level.languages)

        # It has two children -- one for the small English collection and
        # one for the tiny Spanish/French collection.,
        small, tiny = top_level.visible_children
        eq_(u'English', small.display_name)
        eq_([u'eng'], small.languages)

        eq_(u'espa\xf1ol/fran\xe7ais', tiny.display_name)
        eq_([u'spa', u'fre'], tiny.languages)

        # The tiny collection has no sublanes, but the small one has
        # three.  These lanes are tested in more detail in
        # test_create_lane_for_small_collection.
        fiction, nonfiction, children = small.sublanes
        eq_([], tiny.sublanes)
        eq_("Fiction", fiction.display_name)
        eq_("Nonfiction", nonfiction.display_name)
        eq_("Children & Young Adult", children.display_name)

    def test_create_lane_for_small_collection(self):
        languages = ['eng', 'spa', 'chi']
        create_lane_for_small_collection(
            self._db, self._default_library, None, languages
        )
        [lane] = self._db.query(Lane).filter(Lane.parent_id==None).all()

        eq_(u"English/español/Chinese", lane.display_name)
        sublanes = lane.visible_children
        eq_(
            ['Fiction', 'Nonfiction', 'Children & Young Adult'],
            [x.display_name for x in sublanes]
        )
        for x in sublanes:
            eq_(languages, x.languages)
            eq_([Edition.BOOK_MEDIUM], x.media)

        eq_(
            [set(['Adults Only', 'Adult']), 
             set(['Adults Only', 'Adult']), 
             set(['Young Adult', 'Children'])],
            [set(x.audiences) for x in sublanes]
        )
        eq_([True, False, None],
            [x.fiction for x in sublanes]
        )

    def test_lane_for_tiny_collection(self):
        parent = self._lane()
        new_priority = create_lane_for_tiny_collection(
            self._db, self._default_library, parent, 'ger',
            priority=3
        )
        eq_(4, new_priority)
        lane = self._db.query(Lane).filter(Lane.parent==parent).one()
        eq_([Edition.BOOK_MEDIUM], lane.media)
        eq_(parent, lane.parent)
        eq_(['ger'], lane.languages)
        eq_(u'Deutsch', lane.display_name)
        eq_([], lane.children)

    def test_create_default_lanes(self):
        library = self._default_library
        library.setting(
            Configuration.LARGE_COLLECTION_LANGUAGES
        ).value = json.dumps(
            ['eng']
        )

        library.setting(
            Configuration.SMALL_COLLECTION_LANGUAGES
        ).value = json.dumps(
            ['spa', 'chi']
        )

        library.setting(
            Configuration.TINY_COLLECTION_LANGUAGES
        ).value = json.dumps(
            ['ger','fre','ita']
        )

        create_default_lanes(self._db, self._default_library)
        lanes = self._db.query(Lane).filter(Lane.library==library).filter(Lane.parent_id==None).all()

        # We have five top-level lanes for the large collection,
        # a top-level lane for each small collection, and a lane
        # for everything left over.
        eq_(set(['Fiction', "Nonfiction", "Young Adult Fiction", "Young Adult Nonfiction",
                 "Children and Middle Grade", u'World Languages']),
            set([x.display_name for x in lanes])
        )

        [english_fiction_lane] = [x for x in lanes if x.display_name == 'Fiction']
        eq_(0, english_fiction_lane.priority)
        [world] = [x for x in lanes if x.display_name == 'World Languages']
        eq_(5, world.priority)

    def test_lane_configuration_from_collection_sizes(self):

        # If the library has no holdings, we assume it has a large English
        # collection.
        m = _lane_configuration_from_collection_sizes
        eq_(([u'eng'], [], []), m(None))
        eq_(([u'eng'], [], []), m(Counter()))

        # Otherwise, the language with the largest collection, and all
        # languages more than 10% as large, go into `large`.  All
        # languages with collections more than 1% as large as the
        # largest collection go into `small`. All languages with
        # smaller collections go into `tiny`.
        base = 10000
        holdings = Counter(large1=base, large2=base*0.1001,
                           small1=base*0.1, small2=base*0.01001,
                           tiny=base*0.01)
        large, small, tiny = m(holdings)
        eq_(set(['large1', 'large2']), set(large))
        eq_(set(['small1', 'small2']), set(small))
        eq_(['tiny'], tiny)

class TestWorkBasedLane(DatabaseTest):

    def test_initialization_sets_appropriate_audiences(self):
        work = self._work(with_license_pool=True)

        work.audience = Classifier.AUDIENCE_CHILDREN
        children_lane = WorkBasedLane(self._default_library, work, '')
        eq_([Classifier.AUDIENCE_CHILDREN], children_lane.audiences)

        work.audience = Classifier.AUDIENCE_YOUNG_ADULT
        ya_lane = WorkBasedLane(self._default_library, work, '')
        eq_(sorted(Classifier.AUDIENCES_JUVENILE), sorted(ya_lane.audiences))

        work.audience = Classifier.AUDIENCE_ADULT
        adult_lane = WorkBasedLane(self._default_library, work, '')
        eq_(sorted(Classifier.AUDIENCES), sorted(adult_lane.audiences))

        work.audience = Classifier.AUDIENCE_ADULTS_ONLY
        adults_only_lane = WorkBasedLane(self._default_library, work, '')
        eq_(sorted(Classifier.AUDIENCES), sorted(adults_only_lane.audiences))

    def test_default_children_list_not_reused(self):
        work = self._work()

        # By default, a WorkBasedLane has no children.
        lane1 = WorkBasedLane(self._default_library, work)
        eq_([], lane1.children)

        # Add a child...
        lane1.children.append(object)

        # Another lane for the same work gets a different, empty list
        # of children. It doesn't reuse the first lane's list.
        lane2 = WorkBasedLane(self._default_library, work)
        eq_([], lane2.children)


class TestRelatedBooksLane(DatabaseTest):

    def setup(self):
        super(TestRelatedBooksLane, self).setup()
        self.work = self._work(
            with_license_pool=True, audience=Classifier.AUDIENCE_YOUNG_ADULT
        )
        [self.lp] = self.work.license_pools
        self.edition = self.work.presentation_edition

    def test_initialization(self):
        """Asserts that a RelatedBooksLane won't be initialized for a work
        without related books
        """

        # A book without a series or a contributor on a circ manager without
        # NoveList recommendations raises an error.
        self._db.delete(self.edition.contributions[0])
        self._db.commit()

        assert_raises(
            ValueError, RelatedBooksLane, self._default_library, self.work, ""
        )

        # A book with a contributor initializes a RelatedBooksLane.
        luthor, i = self._contributor('Luthor, Lex')
        self.edition.add_contributor(luthor, [Contributor.EDITOR_ROLE])

        result = RelatedBooksLane(self._default_library, self.work, '')
        eq_(self.work, result.work)
        [sublane] = result.children
        eq_(True, isinstance(sublane, ContributorLane))
        eq_(sublane.contributors, [luthor])

        # As does a book in a series.
        self.edition.series = "All By Myself"
        result = RelatedBooksLane(self._default_library, self.work, "")
        eq_(2, len(result.children))
        [contributor, series] = result.children
        eq_(True, isinstance(series, SeriesLane))

        # When NoveList is configured and recommendations are available,
        # a RecommendationLane will be included.
        self._external_integration(
            ExternalIntegration.NOVELIST,
            goal=ExternalIntegration.METADATA_GOAL, username=u'library',
            password=u'sure', libraries=[self._default_library]
        )
        mock_api = MockNoveListAPI(self._db)
        response = Metadata(
            self.edition.data_source, recommendations=[self._identifier()]
        )
        mock_api.setup(response)
        result = RelatedBooksLane(self._default_library, self.work, "", novelist_api=mock_api)
        eq_(3, len(result.children))

        # The book's language and audience list is passed down to all sublanes.
        eq_(['eng'], result.languages)
        for sublane in result.children:
            eq_(result.languages, sublane.languages)
            if isinstance(sublane, SeriesLane):
                eq_([result.source_audience], sublane.audiences)
            else:
                eq_(sorted(list(result.audiences)), sorted(list(sublane.audiences)))

        contributor, recommendations, series = result.children
        eq_(True, isinstance(recommendations, RecommendationLane))
        eq_(True, isinstance(series, SeriesLane))
        eq_(True, isinstance(contributor, ContributorLane))

    def test_contributor_lane_generation(self):

        original = self.edition.contributions[0].contributor
        luthor, i = self._contributor('Luthor, Lex')
        self.edition.add_contributor(luthor, Contributor.EDITOR_ROLE)

        # Lex Luthor doesn't show up because he's only an editor,
        # and an author is listed.
        result = RelatedBooksLane(self._default_library, self.work, '')
        eq_(1, len(result.children))
        [sublane] = result.children
        eq_([original], sublane.contributors)

        # A book with multiple contributors results in multiple
        # ContributorLane sublanes.
        lane, i = self._contributor('Lane, Lois')
        self.edition.add_contributor(lane, Contributor.PRIMARY_AUTHOR_ROLE)
        result = RelatedBooksLane(self._default_library, self.work, '')
        eq_(2, len(result.children))
        sublane_contributors = list()
        [sublane_contributors.extend(c.contributors) for c in result.children]
        eq_(set([lane, original]), set(sublane_contributors))

        # When there are no AUTHOR_ROLES present, contributors in
        # displayable secondary roles appear.
        for contribution in self.edition.contributions:
            if contribution.role in Contributor.AUTHOR_ROLES:
                self._db.delete(contribution)
        self._db.commit()

        result = RelatedBooksLane(self._default_library, self.work, '')
        eq_(1, len(result.children))
        [sublane] = result.children
        eq_([luthor], sublane.contributors)

    def test_works_query(self):
        """RelatedBooksLane is an invisible, groups lane without works."""

        self.edition.series = "All By Myself"
        lane = RelatedBooksLane(self._default_library, self.work, "")
        eq_([], lane.works(self._db).all())


class LaneTest(DatabaseTest):

    def assert_works_queries(self, lane, expected):
        """Tests resulting Lane.works() and Lane.materialized_works() results"""

        materialized_expected = []
        if expected:
            materialized_expected = [work.id for work in expected]
        
        query = lane.works(self._db)
        materialized_results = [work.works_id for work in query.all()]
        
        eq_(sorted(materialized_expected), sorted(materialized_results))

    def sample_works_for_each_audience(self):
        """Create a work for each audience-type."""
        works = list()
        audiences = [
            Classifier.AUDIENCE_CHILDREN, Classifier.AUDIENCE_YOUNG_ADULT,
            Classifier.AUDIENCE_ADULT, Classifier.AUDIENCE_ADULTS_ONLY
        ]

        for audience in audiences:
            work = self._work(with_license_pool=True, audience=audience,
                              data_source_name=DataSource.OVERDRIVE)
            works.append(work)

        return works


class TestRecommendationLane(LaneTest):

    def setup(self):
        super(TestRecommendationLane, self).setup()
        self.work = self._work(with_license_pool=True)

    def generate_mock_api(self):
        """Prep an empty NoveList result."""
        source = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        metadata = Metadata(source)

        mock_api = MockNoveListAPI(self._db)
        mock_api.setup(metadata)
        return mock_api

    def test_works_query(self):
        # Prep an empty result.
        mock_api = self.generate_mock_api()

        # With an empty recommendation result, the lane is empty.
        lane = RecommendationLane(self._default_library, self.work, '', novelist_api=mock_api)
        eq_(None, lane.works(self._db))

        # Resulting recommendations are returned when available, though.
        # TODO: Setting a data source name is necessary because Gutenberg
        # books get filtered out when children or ya is one of the lane's
        # audiences.
        result = self._work(with_license_pool=True, data_source_name=DataSource.OVERDRIVE)
        lane.recommendations = [result.license_pools[0].identifier]
        SessionManager.refresh_materialized_views(self._db)
        self.assert_works_queries(lane, [result])

    def test_works_query_with_source_audience(self):

        # If the lane is created with a source audience, it filters the
        # recommendations appropriately.
        works = self.sample_works_for_each_audience()
        [children, ya, adult, adults_only] = works
        recommendations = list()
        for work in works:
            recommendations.append(work.license_pools[0].identifier)

        expected = {
            Classifier.AUDIENCE_CHILDREN : [children],
            Classifier.AUDIENCE_YOUNG_ADULT : [children, ya],
            Classifier.AUDIENCE_ADULTS_ONLY : works
        }

        for audience, results in expected.items():
            self.work.audience = audience
            SessionManager.refresh_materialized_views(self._db)

            mock_api = self.generate_mock_api()
            lane = RecommendationLane(
                self._default_library, self.work, '', novelist_api=mock_api
            )
            lane.recommendations = recommendations
            self.assert_works_queries(lane, results)

    def test_works_query_with_source_language(self):
        # Prepare a number of works with different languages.
        # TODO: Setting a data source name is necessary because
        # Gutenberg books get filtered out when children or ya
        # is one of the lane's audiences.
        eng = self._work(with_license_pool=True, language='eng', data_source_name=DataSource.OVERDRIVE)
        fre = self._work(with_license_pool=True, language='fre', data_source_name=DataSource.OVERDRIVE)
        spa = self._work(with_license_pool=True, language='spa', data_source_name=DataSource.OVERDRIVE)
        SessionManager.refresh_materialized_views(self._db)

        # They're all returned as recommendations from NoveList Select.
        recommendations = list()
        for work in [eng, fre, spa]:
            recommendations.append(work.license_pools[0].identifier)

        # But only the work that matches the source work is included.
        mock_api = self.generate_mock_api()
        lane = RecommendationLane(self._default_library, self.work, '', novelist_api=mock_api)
        lane.recommendations = recommendations
        self.assert_works_queries(lane, [eng])

        # It doesn't matter the language.
        self.work.presentation_edition.language = 'fre'
        SessionManager.refresh_materialized_views(self._db)
        mock_api = self.generate_mock_api()
        lane = RecommendationLane(self._default_library, self.work, '', novelist_api=mock_api)
        lane.recommendations = recommendations
        self.assert_works_queries(lane, [fre])


class TestSeriesLane(LaneTest):

    def test_initialization(self):
        # An error is raised if SeriesLane is created with an empty string.
        assert_raises(
            ValueError, SeriesLane, self._default_library, ''
        )
        assert_raises(
            ValueError, SeriesLane, self._default_library, None
        )

        lane = SeriesLane(self._default_library, 'Alrighty Then')
        eq_('Alrighty Then', lane.series)

    def test_works_query(self):
        # If there are no works with the series name, no works are returned.
        series_name = "Like As If Whatever Mysteries"
        lane = SeriesLane(self._default_library, series_name)
        self.assert_works_queries(lane, [])

        # Works in the series are returned as expected.
        w1 = self._work(with_license_pool=True)
        w1.presentation_edition.series = series_name
        self._db.commit()
        SessionManager.refresh_materialized_views(self._db)
        self.assert_works_queries(lane, [w1])

        # When there are two works without series_position, they're
        # returned in alphabetical order by title.
        w1.presentation_edition.title = "Zoology"
        w2 = self._work(with_license_pool=True)
        w2.presentation_edition.title = "Anthropology"
        w2.presentation_edition.series = series_name
        self._db.commit()
        SessionManager.refresh_materialized_views(self._db)
        self.assert_works_queries(lane, [w2, w1])

        # If a series_position is added, they're ordered in numerical order.
        w1.presentation_edition.series_position = 6
        w2.presentation_edition.series_position = 13
        self._db.commit()
        SessionManager.refresh_materialized_views(self._db)
        self.assert_works_queries(lane, [w1, w2])

        # If the lane is created with languages, works in other languages
        # aren't included.
        fre = self._work(with_license_pool=True, language='fre')
        spa = self._work(with_license_pool=True, language='spa')
        for work in [fre, spa]:
            work.presentation_edition.series = series_name
        self._db.commit()
        SessionManager.refresh_materialized_views(self._db)

        lane.languages = ['fre', 'spa']
        self.assert_works_queries(lane, [fre, spa])


    def test_childrens_series_with_same_name_as_adult_series(self):
        [children, ya, adult, adults_only] = self.sample_works_for_each_audience()

        # Give them all the same series name.
        series_name = "Monkey Business"
        for work in [children, adult, adults_only]:
            work.presentation_edition.series = series_name
        self._db.commit()
        SessionManager.refresh_materialized_views(self._db)

        # SeriesLane only returns works that match a given audience.
        children_lane = SeriesLane(
            self._default_library, series_name, audiences=[Classifier.AUDIENCE_CHILDREN]
        )
        self.assert_works_queries(children_lane, [children])

        # It's strict about this, in an attempt to increase series accuracy.
        # A request for adult material, only returns Adult material, not
        # Adults Only material.
        adult_lane = SeriesLane(
            self._default_library, series_name, audiences=[Classifier.AUDIENCE_ADULT]
        )
        self.assert_works_queries(adult_lane, [adult])

        adult_lane = SeriesLane(
            self._default_library, series_name, audiences=[Classifier.AUDIENCE_ADULTS_ONLY]
        )
        self.assert_works_queries(adult_lane, [adults_only])

    def test_facets_entry_point_propagated(self):
        """The facets passed in to SeriesLane.featured_works are converted
        to a FeaturedSeriesFacets object with the same entry point.
        """
        lane = SeriesLane(self._default_library, "A series")
        def mock_works(_db, facets, pagination):
            self.called_with = facets
            # It doesn't matter what the query we return matches; just
            # return some kind of query.
            return _db.query(MaterializedWorkWithGenre)
        lane.works = mock_works
        entrypoint = object()
        facets = FacetsWithEntryPoint(entrypoint=entrypoint)
        lane.featured_works(self._db, facets=facets)

        new_facets = self.called_with
        assert isinstance(new_facets, FeaturedSeriesFacets)
        eq_(entrypoint, new_facets.entrypoint)

        # Availability facets have been hard-coded rather than propagated.
        eq_(FeaturedSeriesFacets.COLLECTION_FULL, new_facets.collection)
        eq_(FeaturedSeriesFacets.AVAILABLE_ALL, new_facets.availability)


class TestContributorLane(LaneTest):

    def setup(self):
        super(TestContributorLane, self).setup()
        self.contributor, i = self._contributor(
            'Lane, Lois', **dict(viaf='7', display_name='Lois Lane')
        )

    def test_initialization(self):
        # An error is raised if ContributorLane is created without
        # at least a name.
        assert_raises(ValueError, ContributorLane, self._default_library, '')

    def test_works_query(self):
        # A work by someone else.
        w1 = self._work(with_license_pool=True)

        # A work by the contributor with the same name, without VIAF info.
        w2 = self._work(title="X is for Xylophone", with_license_pool=True)
        same_name = w2.presentation_edition.contributions[0].contributor
        same_name.display_name = 'Lois Lane'
        self._db.commit()
        SessionManager.refresh_materialized_views(self._db)

        # The work with a matching name is found in the contributor lane.
        lane = ContributorLane(self._default_library, 'Lois Lane')
        self.assert_works_queries(lane, [w2])

        # And when we add some additional works, like:
        # A work by the contributor.
        w3 = self._work(title="A is for Apple", with_license_pool=True)
        w3.presentation_edition.add_contributor(self.contributor, [Contributor.PRIMARY_AUTHOR_ROLE])

        # A work by the contributor with VIAF info, writing with a pseudonym.
        w4 = self._work(title="D is for Dinosaur", with_license_pool=True)
        same_viaf, i = self._contributor('Lane, L', **dict(viaf='7'))
        w4.presentation_edition.add_contributor(same_viaf, [Contributor.EDITOR_ROLE])
        self._db.commit()
        SessionManager.refresh_materialized_views(self._db)

        # Those works are also included in the lane, in alphabetical order.
        self.assert_works_queries(lane, [w3, w4, w2])

        # If the lane is created with languages, works in other languages
        # aren't included.
        fre = self._work(with_license_pool=True, language='fre')
        spa = self._work(with_license_pool=True, language='spa')
        for work in [fre, spa]:
            main_contribution = work.presentation_edition.contributions[0]
            main_contribution.contributor = self.contributor
        self._db.commit()
        SessionManager.refresh_materialized_views(self._db)

        lane = ContributorLane(self._default_library, 'Lois Lane', languages=['eng'])
        self.assert_works_queries(lane, [w3, w4, w2])

        lane.languages = ['fre', 'spa']
        self.assert_works_queries(lane, [fre, spa])

    def test_works_query_accounts_for_source_audience(self):
        works = self.sample_works_for_each_audience()
        [children, ya] = works[:2]

        # Give them all the same contributor.
        for work in works:
            work.presentation_edition.contributions[0].contributor = self.contributor
        self._db.commit()
        SessionManager.refresh_materialized_views(self._db)

        # Only childrens works are available in a ContributorLane with a
        # Children audience source
        children_lane = ContributorLane(
            self._default_library, 'Lois Lane', audiences=[Classifier.AUDIENCE_CHILDREN]
        )
        self.assert_works_queries(children_lane, [children])

        # When more than one audience is requested, all are included.
        ya_lane = ContributorLane(
            self._default_library, 'Lois Lane', audiences=list(Classifier.AUDIENCES_JUVENILE)
        )
        self.assert_works_queries(ya_lane, [children, ya])

class TestCrawlableFacets(DatabaseTest):

    def test_default(self):
        facets = CrawlableFacets.default(self._default_library)
        eq_(CrawlableFacets.COLLECTION_FULL, facets.collection)
        eq_(CrawlableFacets.AVAILABLE_ALL, facets.availability)
        eq_(CrawlableFacets.ORDER_LAST_UPDATE, facets.order)
        eq_(False, facets.order_ascending)
        enabled_facets = facets.facets_enabled_at_init
        # There's only one enabled facets for each facet group.
        for group in enabled_facets.itervalues():
            eq_(1, len(group))

    def test_last_update_order_facet(self):
        facets = CrawlableFacets.default(self._default_library)

        w1 = self._work(with_license_pool=True)
        w2 = self._work(with_license_pool=True)
        now = datetime.datetime.utcnow()
        w1.last_update_time = now - datetime.timedelta(days=4)
        w2.last_update_time = now - datetime.timedelta(days=3)
        self.add_to_materialized_view([w1, w2])

        from core.model import MaterializedWorkWithGenre as work_model
        qu = self._db.query(work_model)
        qu = facets.apply(self._db, qu)
        # w2 is first because it was updated more recently.
        eq_([w2.id, w1.id], [mw.works_id for mw in qu])

        list, ignore = self._customlist(num_entries=0)
        e2, ignore = list.add_entry(w2)
        e1, ignore = list.add_entry(w1)
        self._db.flush()
        SessionManager.refresh_materialized_views(self._db)
        qu = self._db.query(work_model)
        qu = facets.apply(self._db, qu)
        # w1 is first because it was added to the list more recently.
        eq_([w1.id, w2.id], [mw.works_id for mw in qu])

    def test_order_by(self):
        """Crawlable feeds are always ordered by time updated and then by 
        collection ID and work ID.
        """
        from core.model import MaterializedWorkWithGenre as mw
        order_by, distinct = CrawlableFacets.order_by()

        updated, collection_id, works_id = distinct
        expect_func = 'greatest(mv_works_for_lanes.availability_time, mv_works_for_lanes.first_appearance, mv_works_for_lanes.last_update_time)'
        eq_(expect_func, str(updated))
        eq_(mw.collection_id, collection_id)
        eq_(mw.works_id, works_id)

        updated_desc, collection_id, works_id = order_by
        eq_(expect_func + ' DESC', str(updated_desc))
        eq_(mw.collection_id, collection_id)
        eq_(mw.works_id, works_id)


class TestCrawlableCustomListBasedLane(DatabaseTest):

    def test_initialize(self):
        list, ignore = self._customlist()
        lane = CrawlableCustomListBasedLane()
        lane.initialize(self._default_library, list)
        eq_(self._default_library.id, lane.library_id)
        eq_([list], lane.customlists)
        eq_("Crawlable feed: %s" % list.name, lane.display_name)
        eq_(None, lane.audiences)
        eq_(None, lane.languages)
        eq_(None, lane.media)
        eq_([], lane.children)

    def test_bibliographic_filter_clause(self):
        w1 = self._work(with_license_pool=True)
        w2 = self._work(with_license_pool=True)

        # Only w2 is in the list.
        list, ignore = self._customlist(num_entries=0)
        e2, ignore = list.add_entry(w2)
        self.add_to_materialized_view([w1, w2])
        self._db.flush()
        SessionManager.refresh_materialized_views(self._db)

        lane = CrawlableCustomListBasedLane()
        lane.initialize(self._default_library, list)

        from core.model import MaterializedWorkWithGenre as work_model
        qu = self._db.query(work_model)
        qu, clause = lane.bibliographic_filter_clause(self._db, qu)

        qu = qu.filter(clause)

        eq_([w2.id], [mw.works_id for mw in qu])

    def test_url_arguments(self):
        list, ignore = self._customlist()
        lane = CrawlableCustomListBasedLane()
        lane.initialize(self._default_library, list)
        route, kwargs = lane.url_arguments
        eq_(CrawlableCustomListBasedLane.ROUTE, route)
        eq_(list.name, kwargs.get("list_name"))
        

class TestCrawlableCollectionBasedLane(DatabaseTest):

    def test_init(self):

        library = self._default_library
        default_collection = self._default_collection
        other_collection = self._collection()
        other_collection_2 = self._collection()

        # A lane for all the collections associated with a library.
        lane = CrawlableCollectionBasedLane(library)
        eq_("Crawlable feed: %s" % library.name, lane.display_name)
        eq_([x.id for x in library.collections], lane.collection_ids)

        # A lane for a collection not actually associated with a
        # library.
        lane = CrawlableCollectionBasedLane(
            None, [other_collection, other_collection_2]
        )
        eq_(
            "Crawlable feed: %s / %s" % tuple(
                sorted([other_collection.name, other_collection_2.name])
            ),
            lane.display_name
        )
        eq_(set([other_collection.id, other_collection_2.id]),
            set(lane.collection_ids))
        eq_(None, lane.get_library(self._db))

    def test_bibliographic_filter_clause(self):
        # Normally, if collection_ids is empty it means there are no
        # restrictions on collection. However, in this case if
        # collections_id is empty it means no titles should be
        # returned.
        collection = self._default_collection
        self._default_library.collections = []
        lane = CrawlableCollectionBasedLane(self._default_library)
        eq_([], lane.collection_ids)

        # This is managed by having bibliographic_filter_clause return None
        # to short-circuit a query in progress.
        eq_((None, None), lane.bibliographic_filter_clause(object(), object()))

        # If collection_ids is not empty, then
        # bibliographic_filter_clause passed through the query it's
        # given without changing it.
        lane.collection_ids = [self._default_collection.id]
        qu = self._db.query(MaterializedWorkWithGenre)
        qu2, clause = lane.bibliographic_filter_clause(self._db, qu)
        eq_(qu, qu2)
        eq_(None, clause)

    def test_url_arguments(self):
        library = self._default_library
        other_collection = self._collection()

        # A lane for all the collections associated with a library.
        lane = CrawlableCollectionBasedLane(library)
        route, kwargs = lane.url_arguments
        eq_(CrawlableCollectionBasedLane.LIBRARY_ROUTE, route)
        eq_(None, kwargs.get("collection_name"))

        # A lane for a collection not actually associated with a
        # library. (A Library is still necessary to provide a point of
        # reference for classes like Facets and CachedFeed.)
        lane = CrawlableCollectionBasedLane(
            library, [other_collection]
        )
        route, kwargs = lane.url_arguments
        eq_(CrawlableCollectionBasedLane.COLLECTION_ROUTE, route)
        eq_(other_collection.name, kwargs.get("collection_name"))
