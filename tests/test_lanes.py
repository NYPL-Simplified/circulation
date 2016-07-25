# encoding: utf-8
from nose.tools import set_trace, eq_, assert_raises

from . import (
    DatabaseTest,
)

from core.lane import (
    Lane,
    LaneList,
)
from core.metadata_layer import Metadata
from core.model import (
    Contributor,
    SessionManager,
    DataSource,
)

from api.config import (
    Configuration,
    temp_config,
)
from api.lanes import (
    make_lanes,
    make_lanes_default,
    lanes_for_large_collection,
    lane_for_small_collection,
    lane_for_other_languages,
    ContributorLane,
    RecommendationLane,
    RelatedBooksLane,
    SeriesLane,
)
from api.novelist import MockNoveListAPI


class TestLaneCreation(DatabaseTest):

    def test_lanes_for_large_collection(self):
        languages = ['eng', 'spa']
        lanes = lanes_for_large_collection(self._db, languages)
        [lane] = lanes

        # We have one top-level lane for English & Spanish
        eq_(u'English/español', lane.name)
        assert lane.invisible

        # The top-level lane has five sublanes.
        eq_(
            [u'English/español - Best Sellers', 'Adult Fiction', 'Adult Nonfiction', 'Young Adult Fiction', 
             'Young Adult Nonfiction', 'Children and Middle Grade'],
            [x.name for x in lane.sublanes]
        )

        # They all are restricted to English and Spanish.
        assert all(x.languages==languages for x in lane.sublanes)

        # The Adult Fiction and Adult Nonfiction lanes reproduce the
        # genre structure found in the genre definitions.
        fiction, nonfiction = lane.sublanes.lanes[1:3]
        [sf] = [x for x in fiction.sublanes.lanes if x.name=='Science Fiction']
        [periodicals] = [x for x in nonfiction.sublanes.lanes if x.name=='Periodicals']
        [humor] = [x for x in nonfiction.sublanes.lanes if x.name=='Humorous Nonfiction']
        eq_(True, sf.fiction)
        eq_("Science Fiction", sf.name)
        eq_("Humor", humor.display_name)
        assert 'Science Fiction' in sf.genre_names
        assert 'Cyberpunk' in sf.genre_names
        assert periodicals.invisible

        [space_opera] = [x for x in sf.sublanes.lanes if x.name=='Space Opera']
        eq_(True, sf.fiction)
        eq_("Space Opera", space_opera.name)
        eq_(["Space Opera"], space_opera.genre_names)

        [history] = [x for x in nonfiction.sublanes.lanes if x.name=='History']
        eq_(False, history.fiction)
        eq_("History", history.name)
        assert 'History' in history.genre_names
        assert 'European History' in history.genre_names

        # The Children lane is searchable; the others are not.
        ya_fiction, ya_nonfiction, children = lane.sublanes.lanes[3:6]
        eq_(False, nonfiction.searchable)
        eq_(False, ya_fiction.searchable)

        eq_("Children and Middle Grade", children.name)
        eq_(True, children.searchable)

    def test_lane_for_small_collection(self):
        lane = lane_for_small_collection(self._db, ['eng', 'spa', 'chi'])
        eq_(u"English/español/Chinese", lane.display_name)
        sublanes = lane.sublanes.lanes
        eq_(
            ['Adult Fiction', 'Adult Nonfiction', 'Children & Young Adult'],
            [x.name for x in sublanes]
        )
        eq_(
            [set(['Adults Only', 'Adult']), 
             set(['Adults Only', 'Adult']), 
             set(['Young Adult', 'Children'])],
            [x.audiences for x in sublanes]
        )
        eq_([True, False, Lane.BOTH_FICTION_AND_NONFICTION],
            [x.fiction for x in sublanes]
        )

    def test_lane_for_other_languages(self):

        with temp_config() as config:
            config[Configuration.POLICIES] = {
                Configuration.LANGUAGE_POLICY: {
                    Configuration.TINY_COLLECTION_LANGUAGES : 'ger,fre,ita'
                }
            }

            exclude = ['eng', 'spa']
            lane = lane_for_other_languages(self._db, exclude)
            eq_(None, lane.languages)
            eq_(exclude, lane.exclude_languages)
            eq_("Other Languages", lane.name)
            eq_(
                ['Deutsch', u'français', 'Italiano'],
                [x.name for x in lane.sublanes.lanes]
            )
            eq_([['ger'], ['fre'], ['ita']],
                [x.languages for x in lane.sublanes.lanes]
            )

        # If no tiny languages are configured, the other languages lane
        # doesn't show up.
        with temp_config() as config:
            config[Configuration.POLICIES] = {
                Configuration.LANGUAGE_POLICY: {
                    Configuration.LARGE_COLLECTION_LANGUAGES : 'eng'
                }
            }

            exclude = ['eng', 'spa']
            lane = lane_for_other_languages(self._db, exclude)
            eq_(None, lane)

    def test_make_lanes_default(self):
        with temp_config() as config:
            config[Configuration.POLICIES] = {
                Configuration.AUTHENTICATION_POLICY : "Millenium",
                Configuration.LANGUAGE_POLICY : {
                    Configuration.LARGE_COLLECTION_LANGUAGES : 'eng',
                    Configuration.SMALL_COLLECTION_LANGUAGES : 'spa,chi',
                    Configuration.TINY_COLLECTION_LANGUAGES : 'ger,fre,ita'
                }
            }
            lane_list = make_lanes_default(self._db)

            assert isinstance(lane_list, LaneList)
            lanes = lane_list.lanes

            # We have a top-level lane for the large collections,
            # a top-level lane for each small collection, and a lane
            # for everything left over.
            eq_(['English', u'español', 'Chinese', 'Other Languages'],
                [x.name for x in lane_list.lanes]
            )

            english_lane = lanes[0]
            eq_(['English - Best Sellers', 'Adult Fiction', 'Adult Nonfiction', 'Young Adult Fiction', 'Young Adult Nonfiction', 'Children and Middle Grade'],
                [x.name for x in english_lane.sublanes.lanes]
            )
            eq_(['Best Sellers', 'Fiction', 'Nonfiction', 'Young Adult Fiction', 'Young Adult Nonfiction', 'Children and Middle Grade'],
                [x.display_name for x in english_lane.sublanes.lanes]
            )


class TestRelatedBooksLane(DatabaseTest):

    def setup(self):
        super(TestRelatedBooksLane, self).setup()
        self.work = self._work(with_license_pool=True)
        [self.lp] = self.work.license_pools

    def test_initialization(self):
        """Asserts that a RelatedBooksLane won't be initialized for a work
        without related books
        """
        with temp_config() as config:
            # A book without a series on a circ manager without
            # NoveList recommendations raises an error.
            config['integrations'][Configuration.NOVELIST_INTEGRATION] = {}
            assert_raises(
                ValueError, RelatedBooksLane, self._db, self.lp, ""
            )

            # But a book from a series initializes a RelatedBooksLane just fine.
            self.lp.presentation_edition.series = "All By Myself"
            result = RelatedBooksLane(self._db, self.lp, "")
            eq_(self.lp, result.license_pool)
            [sublane] = result.sublanes
            eq_(True, isinstance(sublane, SeriesLane))

        with temp_config() as config:
            config['integrations'][Configuration.NOVELIST_INTEGRATION] = {
                Configuration.NOVELIST_PROFILE : 'library',
                Configuration.NOVELIST_PASSWORD : 'sure'
            }
            # When NoveList is configured and recommendations are available,
            # a RecommendationLane will be included.
            mock_api = MockNoveListAPI()
            response = Metadata(
                self.lp.data_source, recommendations=[self._identifier()]
            )
            mock_api.setup(response)
            result = RelatedBooksLane(self._db, self.lp, "", novelist_api=mock_api)
            eq_(2, len(result.sublanes))
            recommendations, series = result.sublanes
            eq_(True, isinstance(recommendations, RecommendationLane))
            eq_(True, isinstance(series, SeriesLane))

    def test_works_query(self):
        """RelatedBooksLane is an invisible, groups lane without works."""

        self.work.presentation_edition.series = "All By Myself"
        lane = RelatedBooksLane(self._db, self.lp, "")
        eq_(None, lane.works())
        eq_(None, lane.materialized_works())


class LaneTest(DatabaseTest):

    def assert_works_queries(self, lane, expected, ordered_result=True):
        """Tests resulting Lane.works() and Lane.materialized_works() results"""

        query = lane.works()
        if not ordered_result:
            eq_(set(expected), set(query.all()))
        else:
            eq_(expected, query.all())

        materialized_expected = expected
        if expected:
            materialized_expected = [work.id for work in expected]
        results = lane.materialized_works().all()
        materialized_results = [work.works_id for work in results]

        if expected and not ordered_result:
            eq_(set(materialized_expected), set(materialized_results))
        else:
            eq_(materialized_expected, materialized_results)


class TestRecommendationLane(LaneTest):

    def test_works_query(self):
        # Prep an empty result.
        source = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        metadata = Metadata(source)
        mock_api = MockNoveListAPI()
        mock_api.setup(metadata)

        # With an empty result, the lane is empty.
        work = self._work(with_license_pool=True)
        lp = work.license_pools[0]
        lane = RecommendationLane(self._db, lp, '', novelist_api=mock_api)
        eq_(None, lane.works())
        eq_(None, lane.materialized_works())

        result = self._work(with_license_pool=True)
        lane.recommendations = [result.license_pools[0].identifier]
        SessionManager.refresh_materialized_views(self._db)
        self.assert_works_queries(lane, [result])

class TestSeriesLane(LaneTest):

    def test_initialization(self):
        # An error is raised if SeriesLane is created with an empty string.
        assert_raises(
            ValueError, SeriesLane, self._db, ''
        )

        lane = SeriesLane(self._db, 'Alrighty Then')
        eq_('Alrighty Then', lane.series)

    def test_works_query(self):
        # If there are no works with the series name, no works are returned.
        series_name = "Like As If Whatever Mysteries"
        lane = SeriesLane(self._db, series_name)
        self.assert_works_queries(lane, [])

        # Works in the series are returned as expected.
        w1 = self._work(with_license_pool=True)
        w1.presentation_edition.series = series_name
        SessionManager.refresh_materialized_views(self._db)
        self.assert_works_queries(lane, [w1])

        # When there are two works without series_position, they're
        # returned in alphabetical order by title.
        w1.presentation_edition.title = "Zoology"
        w2 = self._work(with_license_pool=True)
        w2.presentation_edition.title = "Anthropology"
        w2.presentation_edition.series = series_name
        SessionManager.refresh_materialized_views(self._db)
        self.assert_works_queries(lane, [w2, w1])

        # If a series_position is added, they're ordered in numerical order.
        w1.presentation_edition.series_position = 6
        w2.presentation_edition.series_position = 13
        SessionManager.refresh_materialized_views(self._db)
        self.assert_works_queries(lane, [w1, w2])

class TestContributorLane(LaneTest):

    def setup(self):
        super(TestContributorLane, self).setup()
        self.contributor, i = self._contributor(
            'Lane, Lois', **dict(viaf='7', display_name='Lois Lane')
        )

    def test_initialization(self):
        # An error is raised if ContributorLane is created without
        # at least a name.
        assert_raises(ValueError, ContributorLane, self._db, '')

        # An error is raised if ContributorLane is created with a name and
        # ID that don't match.
        assert_raises(
            ValueError, ContributorLane,
            self._db, 'Clark Kent', contributor.id
        )

    def test_works_query(self):
        # A work by someone else.
        w1 = self._work(with_license_pool=True)

        # A work by the contributor with the same name, without VIAF info.
        w2 = self._work(with_license_pool=True)
        same_name, i = self._contributor('Lane, Lois')
        w2.presentation_edition.add_contributor(same_name, [Contributor.AUTHOR_ROLE])
        SessionManager.refresh_materialized_views(self._db)

        # The work with a matching name is found in the contributor lane.
        lane = ContributorLane(
            self._db, 'Lois Lane', contributor_id=self.contributor.id
        )
        self.assert_works_queries(lane, [w2])

        # And when we add some additional works, like:
        # A work by the contributor.
        w3 = self._work(with_license_pool=True)
        w3.presentation_edition.add_contributor(self.contributor, [Contributor.PRIMARY_AUTHOR_ROLE])

        # A work by the contributor with VIAF info, writing with a pseudonym.
        w4 = self._work(with_license_pool=True)
        same_viaf, i = self._contributor('Lane, L', **dict(viaf='7'))
        w4.presentation_edition.add_contributor(same_viaf, [Contributor.EDITOR_ROLE])
        SessionManager.refresh_materialized_views(self._db)

        # Those works are also included in the lane.
        lane = ContributorLane(
            self._db, 'Lois Lane', contributor_id=self.contributor.id
        )
        self.assert_works_queries(lane, [w3, w4, w2], ordered_result=False)
