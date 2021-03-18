# encoding: utf-8
from collections import Counter

import pytest
import json
import datetime
from mock import MagicMock

from core.testing import (
    DatabaseTest,
)

from core.classifier import Classifier
from core.entrypoint import AudiobooksEntryPoint
from core.external_search import Filter
from core.lane import (
    DatabaseBackedFacets,
    DefaultSortOrderFacets,
    Facets,
    FeaturedFacets,
    Lane,
    WorkList,
)
from core.metadata_layer import (
    ContributorData,
    Metadata,
)
from core.lane import FacetsWithEntryPoint
from core.model import (
    create,
    CachedFeed,
    Contribution,
    Contributor,
    Edition,
    SessionManager,
    DataSource,
    ExternalIntegration,
    Library,
)

from api.config import (
    Configuration,
    CannotLoadConfiguration,
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
    ContributorFacets,
    ContributorLane,
    CrawlableCollectionBasedLane,
    CrawlableFacets,
    CrawlableCustomListBasedLane,
    HasSeriesFacets,
    JackpotFacets,
    JackpotWorkList,
    KnownOverviewFacetsWorkList,
    RecommendationLane,
    RelatedBooksLane,
    SeriesFacets,
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
        assert 5 == len(lanes)
        assert (
            ['Fiction', 'Nonfiction', 'Young Adult Fiction',
             'Young Adult Nonfiction', 'Children and Middle Grade'] ==
            [x.display_name for x in lanes])
        for lane in lanes:
            assert self._default_library == lane.library
            # They all are restricted to English and Spanish.
            assert x.languages == languages

            # They have no restrictions on media type -- that's handled
            # with entry points.
            assert None == x.media

        assert (
            ['Fiction', 'Nonfiction', 'Young Adult Fiction',
             'Young Adult Nonfiction', 'Children and Middle Grade'] ==
            [x.display_name for x in lanes])


        # The Adult Fiction and Adult Nonfiction lanes reproduce the
        # genre structure found in the genre definitions.
        fiction, nonfiction = lanes[0:2]
        [sf] = [x for x in fiction.sublanes if 'Science Fiction' in x.display_name]
        [periodicals] = [x for x in nonfiction.sublanes if 'Periodicals' in x.display_name]
        assert True == sf.fiction
        assert "Science Fiction" == sf.display_name
        assert 'Science Fiction' in [genre.name for genre in sf.genres]

        [nonfiction_humor] = [x for x in nonfiction.sublanes
                              if 'Humor' in x.display_name]
        assert False == nonfiction_humor.fiction

        [fiction_humor] = [x for x in fiction.sublanes
                           if 'Humor' in x.display_name]
        assert True == fiction_humor.fiction

        [space_opera] = [x for x in sf.sublanes if 'Space Opera' in x.display_name]
        assert True == sf.fiction
        assert "Space Opera" == space_opera.display_name
        assert ["Space Opera"] == [genre.name for genre in space_opera.genres]

        [history] = [x for x in nonfiction.sublanes if 'History' in x.display_name]
        assert False == history.fiction
        assert "History" == history.display_name
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
        assert (
            ['Best Sellers', 'Fiction', 'Nonfiction', 'Young Adult Fiction',
             'Young Adult Nonfiction', 'Children and Middle Grade'] ==
            [x.display_name for x in lanes])

        # Each sublane other than best sellers also contains a best sellers lane.
        for sublane in lanes[1:]:
            best_sellers = sublane.visible_children[0]
            assert "Best Sellers" == best_sellers.display_name

        # The best sellers lane has a data source.
        nyt_data_source = DataSource.lookup(self._db, DataSource.NYT)
        assert nyt_data_source == lanes[0].list_datasource

    def test_create_world_languages_lane(self):
        # If there are no small or tiny collections, calling
        # create_world_languages_lane does not create any lanes or change
        # the priority.
        new_priority = create_world_languages_lane(
            self._db, self._default_library, [], [], priority=10
        )
        assert 10 == new_priority
        assert [] == self._db.query(Lane).all()

        # If there are lanes to be created, create_world_languages_lane
        # creates them.
        new_priority = create_world_languages_lane(
            self._db, self._default_library,
            ["eng"], [["spa", "fre"]], priority=10
        )

        # priority has been incremented to make room for the newly
        # created lane.
        assert 11 == new_priority

        # One new top-level lane has been created. It contains books
        # from all three languages mentioned in its children.
        top_level = self._db.query(Lane).filter(Lane.parent==None).one()
        assert "World Languages" == top_level.display_name
        assert set(['spa', 'fre', 'eng']) == top_level.languages

        # It has two children -- one for the small English collection and
        # one for the tiny Spanish/French collection.,
        small, tiny = top_level.visible_children
        assert 'English' == small.display_name
        assert ['eng'] == small.languages

        assert 'espa\xf1ol/fran\xe7ais' == tiny.display_name
        assert ['spa', 'fre'] == tiny.languages

        # The tiny collection has no sublanes, but the small one has
        # three.  These lanes are tested in more detail in
        # test_create_lane_for_small_collection.
        fiction, nonfiction, children = small.sublanes
        assert [] == tiny.sublanes
        assert "Fiction" == fiction.display_name
        assert "Nonfiction" == nonfiction.display_name
        assert "Children & Young Adult" == children.display_name

    def test_create_lane_for_small_collection(self):
        languages = ['eng', 'spa', 'chi']
        create_lane_for_small_collection(
            self._db, self._default_library, None, languages
        )
        [lane] = self._db.query(Lane).filter(Lane.parent_id==None).all()

        assert "English/español/Chinese" == lane.display_name
        sublanes = lane.visible_children
        assert (
            ['Fiction', 'Nonfiction', 'Children & Young Adult'] ==
            [x.display_name for x in sublanes])
        for x in sublanes:
            assert languages == x.languages
            assert [Edition.BOOK_MEDIUM] == x.media

        assert (
            [set(['All Ages', 'Adults Only', 'Adult']),
             set(['All Ages', 'Adults Only', 'Adult']),
             set(['Young Adult', 'Children'])] ==
            [set(x.audiences) for x in sublanes])
        assert ([True, False, None] ==
            [x.fiction for x in sublanes])

        # If a language name is not found, don't create any lanes.
        languages = ['eng', 'mul', 'chi']
        parent = self._lane()
        priority = create_lane_for_small_collection(
            self._db, self._default_library, parent, languages, priority=2
        )
        lane = self._db.query(Lane).filter(Lane.parent==parent)
        assert priority == 0
        assert lane.count() == 0

    def test_lane_for_tiny_collection(self):
        parent = self._lane()
        new_priority = create_lane_for_tiny_collection(
            self._db, self._default_library, parent, 'ger',
            priority=3
        )
        assert 4 == new_priority
        lane = self._db.query(Lane).filter(Lane.parent==parent).one()
        assert [Edition.BOOK_MEDIUM] == lane.media
        assert parent == lane.parent
        assert ['ger'] == lane.languages
        assert 'Deutsch' == lane.display_name
        assert [] == lane.children

        # No lane should be created when the language has no name.
        new_parent = self._lane()
        new_priority = create_lane_for_tiny_collection(
            self._db, self._default_library, new_parent, ['spa', 'gaa', 'eng'],
            priority=3
        )
        assert 0 == new_priority
        lane = self._db.query(Lane).filter(Lane.parent==new_parent)
        assert lane.count() == 0

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
        assert (set(['Fiction', "Nonfiction", "Young Adult Fiction", "Young Adult Nonfiction",
                 "Children and Middle Grade", 'World Languages']) ==
            set([x.display_name for x in lanes]))

        [english_fiction_lane] = [x for x in lanes if x.display_name == 'Fiction']
        assert 0 == english_fiction_lane.priority
        [world] = [x for x in lanes if x.display_name == 'World Languages']
        assert 5 == world.priority

    def test_lane_configuration_from_collection_sizes(self):

        # If the library has no holdings, we assume it has a large English
        # collection.
        m = _lane_configuration_from_collection_sizes
        assert (['eng'], [], []) == m(None)
        assert (['eng'], [], []) == m(Counter())

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
        assert set(['large1', 'large2']) == set(large)
        assert set(['small1', 'small2']) == set(small)
        assert ['tiny'] == tiny

class TestWorkBasedLane(DatabaseTest):

    def test_initialization_sets_appropriate_audiences(self):
        work = self._work(with_license_pool=True)

        work.audience = Classifier.AUDIENCE_CHILDREN
        children_lane = WorkBasedLane(self._default_library, work, '')
        assert [Classifier.AUDIENCE_CHILDREN] == children_lane.audiences

        work.audience = Classifier.AUDIENCE_YOUNG_ADULT
        ya_lane = WorkBasedLane(self._default_library, work, '')
        assert sorted(Classifier.AUDIENCES_JUVENILE) == sorted(ya_lane.audiences)

        work.audience = Classifier.AUDIENCE_ADULT
        adult_lane = WorkBasedLane(self._default_library, work, '')
        assert sorted(Classifier.AUDIENCES) == sorted(adult_lane.audiences)

        work.audience = Classifier.AUDIENCE_ADULTS_ONLY
        adults_only_lane = WorkBasedLane(self._default_library, work, '')
        assert sorted(Classifier.AUDIENCES) == sorted(adults_only_lane.audiences)

    def test_append_child(self):
        """When a WorkBasedLane gets a child, its language and audience
        restrictions are propagated to the child.
        """
        work = self._work(
            with_license_pool=True, audience=Classifier.AUDIENCE_CHILDREN,
            language='spa'
        )

        def make_child():
            # Set up a WorkList with settings that contradict the
            # settings of the work we'll be using as the basis for our
            # WorkBasedLane.
            child = WorkList()
            child.initialize(
                self._default_library, 'sublane', languages=['eng'],
                audiences=[Classifier.AUDIENCE_ADULT]
            )
            return child
        child1, child2 = [make_child() for i in range(2)]

        # The WorkBasedLane's restrictions are propagated to children
        # passed in to the constructor.
        lane = WorkBasedLane(self._default_library, work, 'parent lane',
                             children=[child1])

        assert ['spa'] == child1.languages
        assert [Classifier.AUDIENCE_CHILDREN] == child1.audiences

        # It also happens when .append_child is called after the
        # constructor.
        lane.append_child(child2)
        assert ['spa'] == child2.languages
        assert [Classifier.AUDIENCE_CHILDREN] == child2.audiences

    def test_default_children_list_not_reused(self):
        work = self._work()

        # By default, a WorkBasedLane has no children.
        lane1 = WorkBasedLane(self._default_library, work)
        assert [] == lane1.children

        # Add a child...
        lane1.children.append(object)

        # Another lane for the same work gets a different, empty list
        # of children. It doesn't reuse the first lane's list.
        lane2 = WorkBasedLane(self._default_library, work)
        assert [] == lane2.children

    def test_accessible_to(self):
        # A lane based on a Work is accessible to a patron only if
        # the Work is age-appropriate for the patron.
        work = self._work()
        patron = self._patron()
        lane = WorkBasedLane(self._default_library, work)

        work.age_appropriate_for_patron = MagicMock(return_value=False)
        assert False == lane.accessible_to(patron)
        work.age_appropriate_for_patron.assert_called_once_with(patron)

        # If for whatever reason Work is not set, we just we say the Lane is
        # accessible -- but things probably won't work.
        lane.work = None
        assert True == lane.accessible_to(patron)

        # age_appropriate_for_patron wasn't called, since there was no
        # work.
        work.age_appropriate_for_patron.assert_called_once_with(patron)

        lane.work = work
        work.age_appropriate_for_patron = MagicMock(return_value=True)
        lane = WorkBasedLane(self._default_library, work)
        assert True == lane.accessible_to(patron)
        work.age_appropriate_for_patron.assert_called_once_with(patron)

        # The WorkList rules are still enforced -- for instance, a
        # patron from library B can't access any kind of WorkList from
        # library A.
        other_library_patron = self._patron(library=self._library())
        assert False == lane.accessible_to(other_library_patron)

        # age_appropriate_for_patron was never called with the new
        # patron -- the WorkList rules answered the question before we
        # got to that point.
        work.age_appropriate_for_patron.assert_called_once_with(patron)


class TestRelatedBooksLane(DatabaseTest):

    def setup_method(self):
        super(TestRelatedBooksLane, self).setup_method()
        self.work = self._work(
            with_license_pool=True, audience=Classifier.AUDIENCE_YOUNG_ADULT
        )
        [self.lp] = self.work.license_pools
        self.edition = self.work.presentation_edition

    def test_feed_type(self):
        # All feeds from these lanes are cached as 'related works' feeds.
        assert CachedFeed.RELATED_TYPE == RelatedBooksLane.CACHED_FEED_TYPE

    def test_initialization(self):
        # Asserts that a RelatedBooksLane won't be initialized for a work
        # without related books

        # A book without a series or a contributor on a circ manager without
        # NoveList recommendations raises an error.
        self._db.delete(self.edition.contributions[0])
        self._db.commit()

        pytest.raises(
            ValueError, RelatedBooksLane, self._default_library, self.work, ""
        )

        # A book with a contributor initializes a RelatedBooksLane.
        luthor, i = self._contributor('Luthor, Lex')
        self.edition.add_contributor(luthor, [Contributor.EDITOR_ROLE])

        result = RelatedBooksLane(self._default_library, self.work, '')
        assert self.work == result.work
        [sublane] = result.children
        assert True == isinstance(sublane, ContributorLane)
        assert sublane.contributor == luthor

        # As does a book in a series.
        self.edition.series = "All By Myself"
        result = RelatedBooksLane(self._default_library, self.work, "")
        assert 2 == len(result.children)
        [contributor, series] = result.children
        assert True == isinstance(series, SeriesLane)

        # When NoveList is configured and recommendations are available,
        # a RecommendationLane will be included.
        self._external_integration(
            ExternalIntegration.NOVELIST,
            goal=ExternalIntegration.METADATA_GOAL, username='library',
            password='sure', libraries=[self._default_library]
        )
        mock_api = MockNoveListAPI(self._db)
        response = Metadata(
            self.edition.data_source, recommendations=[self._identifier()]
        )
        mock_api.setup_method(response)
        result = RelatedBooksLane(self._default_library, self.work, "", novelist_api=mock_api)
        assert 3 == len(result.children)

        [novelist_recommendations] = [
            x for x in result.children if isinstance(x, RecommendationLane)
        ]
        assert ("Similar titles recommended by NoveList" ==
            novelist_recommendations.display_name)

        # The book's language and audience list is passed down to all sublanes.
        assert ['eng'] == result.languages
        for sublane in result.children:
            assert result.languages == sublane.languages
            if isinstance(sublane, SeriesLane):
                assert [result.source_audience] == sublane.audiences
            else:
                assert sorted(list(result.audiences)) == sorted(list(sublane.audiences))

        contributor, recommendations, series = result.children
        assert True == isinstance(recommendations, RecommendationLane)
        assert True == isinstance(series, SeriesLane)
        assert True == isinstance(contributor, ContributorLane)

    def test_contributor_lane_generation(self):

        original = self.edition.contributions[0].contributor
        luthor, i = self._contributor('Luthor, Lex')
        self.edition.add_contributor(luthor, Contributor.EDITOR_ROLE)

        # Lex Luthor doesn't show up because he's only an editor,
        # and an author is listed.
        result = RelatedBooksLane(self._default_library, self.work, '')
        assert 1 == len(result.children)
        [sublane] = result.children
        assert original == sublane.contributor

        # A book with multiple contributors results in multiple
        # ContributorLane sublanes.
        lane, i = self._contributor('Lane, Lois')
        self.edition.add_contributor(lane, Contributor.PRIMARY_AUTHOR_ROLE)
        result = RelatedBooksLane(self._default_library, self.work, '')
        assert 2 == len(result.children)
        sublane_contributors = list()
        [sublane_contributors.append(c.contributor) for c in result.children]
        assert set([lane, original]) == set(sublane_contributors)

        # When there are no AUTHOR_ROLES present, contributors in
        # displayable secondary roles appear.
        for contribution in self.edition.contributions:
            if contribution.role in Contributor.AUTHOR_ROLES:
                self._db.delete(contribution)
        self._db.commit()

        result = RelatedBooksLane(self._default_library, self.work, '')
        assert 1 == len(result.children)
        [sublane] = result.children
        assert luthor == sublane.contributor

    def test_works_query(self):
        """RelatedBooksLane is an invisible, groups lane without works."""

        self.edition.series = "All By Myself"
        lane = RelatedBooksLane(self._default_library, self.work, "")
        assert [] == lane.works(self._db)


class LaneTest(DatabaseTest):

    def assert_works_from_database(self, lane, expected):
        """Tests resulting Lane.works_from_database() results"""

        if expected:
            expected = [work.id for work in expected]
        actual = [work.id for work in lane.works_from_database(self._db)]

        assert sorted(expected) == sorted(actual)

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

    def setup_method(self):
        super(TestRecommendationLane, self).setup_method()
        self.work = self._work(with_license_pool=True)

    def generate_mock_api(self):
        """Prep an empty NoveList result."""
        source = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        metadata = Metadata(source)

        mock_api = MockNoveListAPI(self._db)
        mock_api.setup_method(metadata)
        return mock_api

    def test_modify_search_filter_hook(self):
        # Prep an empty result.
        mock_api = self.generate_mock_api()

        # With an empty recommendation result, the Filter is set up
        # to return nothing.
        lane = RecommendationLane(self._default_library, self.work, '', novelist_api=mock_api)
        filter = Filter()
        assert False == filter.match_nothing
        modified = lane.modify_search_filter_hook(filter)
        assert modified == filter
        assert True == filter.match_nothing

        # When there are recommendations, the Filter is modified to
        # match only those ISBNs.
        i1 = self._identifier()
        i2 = self._identifier()
        lane.recommendations = [i1, i2]
        filter = Filter()
        assert [] == filter.identifiers
        modified = lane.modify_search_filter_hook(filter)
        assert modified == filter
        assert [i1, i2] == filter.identifiers
        assert False == filter.match_nothing

    def test_overview_facets(self):
        # A FeaturedFacets object is adapted to a Facets object with
        # specific settings.
        featured = FeaturedFacets(0.44, entrypoint=AudiobooksEntryPoint)
        lane = RecommendationLane(
            self._default_library, self.work, '',
            novelist_api=self.generate_mock_api()
        )
        overview = lane.overview_facets(self._db, featured)
        assert isinstance(overview, Facets)
        assert Facets.COLLECTION_FULL == overview.collection
        assert Facets.AVAILABLE_ALL == overview.availability
        assert Facets.ORDER_AUTHOR == overview.order

        # Entry point was preserved.
        assert AudiobooksEntryPoint == overview.entrypoint


class TestHasSeriesFacets(DatabaseTest):

    def test_modify_search_filter(self):
        facets = HasSeriesFacets.default(self._default_library)
        filter = Filter()
        assert None == filter.series
        facets.modify_search_filter(filter)
        assert True == filter.series


class TestSeriesFacets(DatabaseTest):

    def test_default_sort_order(self):
        assert Facets.ORDER_SERIES_POSITION == SeriesFacets.DEFAULT_SORT_ORDER
        facets = SeriesFacets.default(self._default_library)
        assert isinstance(facets, DefaultSortOrderFacets)
        assert Facets.ORDER_SERIES_POSITION == facets.order


class TestSeriesLane(LaneTest):

    def test_feed_type(self):
        # All feeds from these lanes are cached as series feeds.
        assert CachedFeed.SERIES_TYPE == SeriesLane.CACHED_FEED_TYPE

    def test_initialization(self):
        # An error is raised if SeriesLane is created with an empty string.
        pytest.raises(
            ValueError, SeriesLane, self._default_library, ''
        )
        pytest.raises(
            ValueError, SeriesLane, self._default_library, None
        )

        work = self._work(
            language='spa', audience=[Classifier.AUDIENCE_CHILDREN]
        )
        work_based_lane = WorkBasedLane(self._default_library, work)
        child = SeriesLane(self._default_library, "Alrighty Then",
                           parent=work_based_lane, languages=['eng'],
                           audiences=['another audience'])

        # The series provided in the constructor is stored as .series.
        assert "Alrighty Then" == child.series

        # The SeriesLane is added as a child of its parent
        # WorkBasedLane -- something that doesn't happen by default.
        assert [child] == work_based_lane.children

        # As a side effect of that, this lane's audiences and
        # languages were changed to values consistent with its parent.
        assert [work_based_lane.source_audience] == child.audiences
        assert work_based_lane.languages == child.languages

        # If for some reason there's no audience for the work used as
        # a basis for the parent lane, the parent lane's audience
        # filter is used as a basis for the child lane's audience filter.
        work_based_lane.source_audience = None
        child = SeriesLane(
            self._default_library, "No Audience", parent=work_based_lane
        )
        assert work_based_lane.audiences == child.audiences

    def test_modify_search_filter_hook(self):
        lane = SeriesLane(self._default_library, "So That Happened")
        filter = Filter()
        lane.modify_search_filter_hook(filter)
        assert "So That Happened" == filter.series

    def test_overview_facets(self):
        # A FeaturedFacets object is adapted to a SeriesFacets object.
        # This guarantees that a SeriesLane's contributions to a
        # grouped feed will be ordered correctly.
        featured = FeaturedFacets(0.44, entrypoint=AudiobooksEntryPoint)
        lane = SeriesLane(self._default_library, "Alrighty Then")
        overview = lane.overview_facets(self._db, featured)
        assert isinstance(overview, SeriesFacets)
        assert Facets.COLLECTION_FULL == overview.collection
        assert Facets.AVAILABLE_ALL == overview.availability
        assert Facets.ORDER_SERIES_POSITION == overview.order

        # Entry point was preserved.
        assert AudiobooksEntryPoint == overview.entrypoint


class TestContributorFacets(DatabaseTest):

    def test_default_sort_order(self):
        assert Facets.ORDER_TITLE == ContributorFacets.DEFAULT_SORT_ORDER
        facets = ContributorFacets.default(self._default_library)
        assert isinstance(facets, DefaultSortOrderFacets)
        assert Facets.ORDER_TITLE == facets.order


class TestContributorLane(LaneTest):

    def test_feed_type(self):
        # All feeds of this type are cached as contributor feeds.
        assert CachedFeed.CONTRIBUTOR_TYPE == ContributorLane.CACHED_FEED_TYPE

    def setup_method(self):
        super(TestContributorLane, self).setup_method()
        self.contributor, i = self._contributor(
            'Lane, Lois', **dict(viaf='7', display_name='Lois Lane')
        )

    def test_initialization(self):
        with pytest.raises(ValueError) as excinfo:
            ContributorLane(self._default_library, None)
        assert "ContributorLane can't be created without contributor" in str(excinfo.value)

        parent = WorkList()
        parent.initialize(self._default_library)

        lane = ContributorLane(
            self._default_library, self.contributor, parent,
            languages=['a'], audiences=['b'],
        )
        assert self.contributor == lane.contributor
        assert ['a'] == lane.languages
        assert ['b'] == lane.audiences
        assert [lane] == parent.children

        # The contributor_key will be used in links to other pages
        # of this Lane and so on.
        assert "Lois Lane" == lane.contributor_key

        # If the contributor used to create a ContributorLane has no
        # display name, their sort name is used as the
        # contributor_key.
        contributor = ContributorData(sort_name="Lane, Lois")
        lane = ContributorLane(self._default_library, contributor)
        assert contributor == lane.contributor
        assert "Lane, Lois" == lane.contributor_key

    def test_url_arguments(self):
        lane = ContributorLane(
            self._default_library, self.contributor,
            languages=['eng', 'spa'], audiences=['Adult', 'Children'],
        )
        route, kwargs = lane.url_arguments
        assert lane.ROUTE == route

        assert (
            dict(
                contributor_name=lane.contributor_key,
                languages='eng,spa',
                audiences='Adult,Children'
            ) ==
            kwargs)

    def test_modify_search_filter_hook(self):
        lane = ContributorLane(self._default_library, self.contributor)
        filter = Filter()
        lane.modify_search_filter_hook(filter)
        assert self.contributor == filter.author

    def test_overview_facets(self):
        # A FeaturedFacets object is adapted to a ContributorFacets object.
        # This guarantees that a ContributorLane's contributions to a
        # grouped feed will be ordered correctly.
        featured = FeaturedFacets(0.44, entrypoint=AudiobooksEntryPoint)
        lane = ContributorLane(self._default_library, self.contributor)
        overview = lane.overview_facets(self._db, featured)
        assert isinstance(overview, ContributorFacets)
        assert Facets.COLLECTION_FULL == overview.collection
        assert Facets.AVAILABLE_ALL == overview.availability
        assert Facets.ORDER_TITLE == overview.order

        # Entry point was preserved.
        assert AudiobooksEntryPoint == overview.entrypoint


class TestCrawlableFacets(DatabaseTest):

    def test_feed_type(self):
        # All crawlable feeds are cached as such, no matter what
        # WorkList they come from.
        assert CachedFeed.CRAWLABLE_TYPE == CrawlableFacets.CACHED_FEED_TYPE

    def test_default(self):
        facets = CrawlableFacets.default(self._default_library)
        assert CrawlableFacets.COLLECTION_FULL == facets.collection
        assert CrawlableFacets.AVAILABLE_ALL == facets.availability
        assert CrawlableFacets.ORDER_LAST_UPDATE == facets.order
        assert False == facets.order_ascending

        # There's only one enabled value for each facet group.
        for group in facets.enabled_facets:
            assert 1 == len(group)


class TestCrawlableCollectionBasedLane(DatabaseTest):

    def test_init(self):

        # Collection-based crawlable feeds are cached for 2 hours.
        assert 2 * 60 * 60 == CrawlableCollectionBasedLane.MAX_CACHE_AGE

        # This library has two collections.
        library = self._default_library
        default_collection = self._default_collection
        other_library_collection = self._collection()
        library.collections.append(other_library_collection)

        # This collection is not associated with any library.
        unused_collection = self._collection()

        # A lane for all the collections associated with a library.
        lane = CrawlableCollectionBasedLane()
        lane.initialize(library)
        assert "Crawlable feed: %s" % library.name == lane.display_name
        assert set([x.id for x in library.collections]) == set(lane.collection_ids)

        # A lane for specific collection, regardless of their library
        # affiliation.
        lane = CrawlableCollectionBasedLane()
        lane.initialize([unused_collection, other_library_collection])
        assert (
            "Crawlable feed: %s / %s" % tuple(
                sorted([unused_collection.name, other_library_collection.name])
            ) ==
            lane.display_name)
        assert (set([unused_collection.id, other_library_collection.id]) ==
            set(lane.collection_ids))

        # Unlike pretty much all other lanes in the system, this lane
        # has no affiliated library.
        assert None == lane.get_library(self._db)

    def test_url_arguments(self):
        library = self._default_library
        other_collection = self._collection()

        # A lane for all the collections associated with a library.
        lane = CrawlableCollectionBasedLane()
        lane.initialize(library)
        route, kwargs = lane.url_arguments
        assert CrawlableCollectionBasedLane.LIBRARY_ROUTE == route
        assert None == kwargs.get("collection_name")

        # A lane for a collection not actually associated with a
        # library.
        lane = CrawlableCollectionBasedLane()
        lane.initialize([other_collection])
        route, kwargs = lane.url_arguments
        assert CrawlableCollectionBasedLane.COLLECTION_ROUTE == route
        assert other_collection.name == kwargs.get("collection_name")


class TestCrawlableCustomListBasedLane(DatabaseTest):

    def test_initialize(self):
        # These feeds are cached for 12 hours.
        assert 12 * 60 * 60 == CrawlableCustomListBasedLane.MAX_CACHE_AGE

        customlist, ignore = self._customlist()
        lane = CrawlableCustomListBasedLane()
        lane.initialize(self._default_library, customlist)
        assert self._default_library.id == lane.library_id
        assert [customlist.id] == lane.customlist_ids
        assert customlist.name == lane.customlist_name
        assert "Crawlable feed: %s" % customlist.name == lane.display_name
        assert None == lane.audiences
        assert None == lane.languages
        assert None == lane.media
        assert [] == lane.children

    def test_url_arguments(self):
        customlist, ignore = self._customlist()
        lane = CrawlableCustomListBasedLane()
        lane.initialize(self._default_library, customlist)
        route, kwargs = lane.url_arguments
        assert CrawlableCustomListBasedLane.ROUTE == route
        assert customlist.name == kwargs.get("list_name")


class TestKnownOverviewFacetsWorkList(DatabaseTest):
    """Test of the KnownOverviewFacetsWorkList class.

    This is an unusual class which should be used when hard-coding the
    faceting object to use for a given WorkList when generating a
    grouped feed.
    """
    def test_overview_facets(self):
        # Show that we can hard-code the return value of overview_facets.
        #
        # core/tests/test_lanes.py#TestWorkList.test_groups_propagates_facets
        # verifies that WorkList.groups() calls
        # WorkList.overview_facets() and passes the return value
        # (which we hard-code here) into WorkList.works().

        # Pass in a known faceting object.
        known_facets = object()
        wl = KnownOverviewFacetsWorkList(known_facets)

        # That faceting object is always returned when we're
        # making a grouped feed.
        some_other_facets = object()
        assert known_facets == wl.overview_facets(self._db, some_other_facets)


class TestJackpotFacets(DatabaseTest):

    def test_default_facet(self):
        # A JackpotFacets object defaults to showing only books that
        # are currently available. Normal facet configuration is
        # ignored.
        m = JackpotFacets.default_facet

        default = m(None, JackpotFacets.AVAILABILITY_FACET_GROUP_NAME)
        assert Facets.AVAILABLE_NOW == default

        # For other facet groups, the class defers to the Facets
        # superclass. (But this doesn't matter because it's not relevant
        # to the creation of jackpot feeds.)
        for group in (Facets.COLLECTION_FACET_GROUP_NAME,
                      Facets.ORDER_FACET_GROUP_NAME):
            assert (m(self._default_library, group) ==
                Facets.default_facet(self._default_library, group))

    def test_available_facets(self):
        # A JackpotFacets object always has the same availability
        # facets. Normal facet configuration is ignored.

        m = JackpotFacets.available_facets
        available = m(None, JackpotFacets.AVAILABILITY_FACET_GROUP_NAME)
        assert ([Facets.AVAILABLE_NOW, Facets.AVAILABLE_NOT_NOW,
             Facets.AVAILABLE_ALL, Facets.AVAILABLE_OPEN_ACCESS] ==
             available)

        # For other facet groups, the class defers to the Facets
        # superclass. (But this doesn't matter because it's not relevant
        # to the creation of jackpot feeds.)
        for group in (Facets.COLLECTION_FACET_GROUP_NAME,
                      Facets.ORDER_FACET_GROUP_NAME):
            assert (m(self._default_library, group) ==
                Facets.available_facets(self._default_library, group))


class TestJackpotWorkList(DatabaseTest):
    """Test the 'jackpot' WorkList that always contains the information
    necessary to run a full suite of integration tests.
    """

    def test_constructor(self):
        # Add some stuff to the default library to make sure we
        # test everything.

        # The default library comes with a collection whose data
        # source is unspecified. Make another one whose data source _is_
        # specified.
        overdrive_collection = self._collection(
            "Test Overdrive Collection", protocol=ExternalIntegration.OVERDRIVE,
            data_source_name=DataSource.OVERDRIVE
        )
        self._default_library.collections.append(overdrive_collection)

        # Create another collection that is _not_ associated with this
        # library. It will not be used at all.
        ignored_collection = self._collection(
            "Ignored Collection", protocol=ExternalIntegration.BIBLIOTHECA,
            data_source_name=DataSource.BIBLIOTHECA
        )

        # Pass in a JackpotFacets object
        facets = JackpotFacets.default(self._default_library)

        # The JackpotWorkList has no works of its own -- only its children
        # have works.
        wl = JackpotWorkList(self._default_library, facets)
        assert [] == wl.works(self._db)

        # Let's take a look at the children.

        # NOTE: This test is structured to make it easy to add other
        # groups of children later on. However it's more likely we will
        # test other features with totally different feeds.
        children = list(wl.children)
        available_now = children[:4]
        children = children[4:]

        # This group contains four similar
        # KnownOverviewFacetsWorkLists. They only show works that are
        # currently available.
        for i in available_now:
            # Each lane is associated with the JackpotFacets we passed
            # in.
            assert isinstance(i, KnownOverviewFacetsWorkList)
            internal_facets = i.facets
            assert facets == internal_facets

        # These worklists show ebooks and audiobooks from the two
        # collections associated with the default library.
        [default_ebooks, default_audio, overdrive_ebooks, overdrive_audio] = (
            available_now
        )

        assert ("License source {[Unknown]} - Medium {Book} - Collection name {%s}" % self._default_collection.name ==
            default_ebooks.display_name)
        assert [self._default_collection.id] == default_ebooks.collection_ids
        assert [Edition.BOOK_MEDIUM] == default_ebooks.media

        assert ("License source {[Unknown]} - Medium {Audio} - Collection name {%s}" % self._default_collection.name ==
            default_audio.display_name)
        assert [self._default_collection.id] == default_audio.collection_ids
        assert [Edition.AUDIO_MEDIUM] == default_audio.media

        assert ("License source {Overdrive} - Medium {Book} - Collection name {Test Overdrive Collection}" ==
            overdrive_ebooks.display_name)
        assert [overdrive_collection.id] == overdrive_ebooks.collection_ids
        assert [Edition.BOOK_MEDIUM] == overdrive_ebooks.media


        assert ("License source {Overdrive} - Medium {Audio} - Collection name {Test Overdrive Collection}" ==
            overdrive_audio.display_name)
        assert [overdrive_collection.id] == overdrive_audio.collection_ids
        assert [Edition.AUDIO_MEDIUM] == overdrive_audio.media

        # At this point we've looked at all the children of the
        # JackpotWorkList
        assert [] == children
