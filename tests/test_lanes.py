# encoding: utf-8
from nose.tools import set_trace, eq_, assert_raises
import json

from . import (
    DatabaseTest,
)

from core.classifier import Classifier
from core.lane import (
    Lane,
    WorkList,
)
from core.metadata_layer import Metadata
from core.model import (
    create,
    Contribution,
    Contributor,
    SessionManager,
    DataSource,
    ExternalIntegration,
    Library,
)

from api.config import (
    Configuration,
    temp_config,
)
from api.lanes import (
    create_default_lanes,
    create_lanes_for_large_collection,
    create_lane_for_small_collection,
    create_lane_for_tiny_collections,
    load_lanes,
    ContributorLane,
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


    def test_create_lane_for_small_collection(self):
        languages = ['eng', 'spa', 'chi']
        create_lane_for_small_collection(
            self._db, self._default_library, languages
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
        eq_(
            [set(['Adults Only', 'Adult']), 
             set(['Adults Only', 'Adult']), 
             set(['Young Adult', 'Children'])],
            [set(x.audiences) for x in sublanes]
        )
        eq_([True, False, None],
            [x.fiction for x in sublanes]
        )

    def test_lane_for_other_languages(self):
        # If no tiny languages are configured, the other languages lane
        # doesn't show up.
        create_lane_for_tiny_collections(self._db, self._default_library, [])
        eq_(0, self._db.query(Lane).filter(Lane.parent_id==None).count())


        create_lane_for_tiny_collections(self._db, self._default_library, ['ger', 'fre', 'ita'])
        [lane] = self._db.query(Lane).filter(Lane.parent_id==None).all()
        eq_(['ger', 'fre', 'ita'], lane.languages)
        eq_("Other Languages", lane.display_name)
        eq_(
            ['Deutsch', u'français', 'Italiano'],
            [x.display_name for x in lane.visible_children]
        )
        eq_([['ger'], ['fre'], ['ita']],
            [x.languages for x in lane.visible_children]
        )


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
                 "Children and Middle Grade", u'español', 'Chinese', 'Other Languages']),
            set([x.display_name for x in lanes])
        )

        [english_fiction_lane] = [x for x in lanes if x.display_name == 'Fiction']
        eq_(0, english_fiction_lane.priority)
        [chinese_lane] = [x for x in lanes if x.display_name == 'Chinese']
        eq_(6, chinese_lane.priority)
        [other_lane] = [x for x in lanes if x.display_name == 'Other Languages']
        eq_(7, other_lane.priority)

    def test_load_lanes(self):
        # These two top-level lanes should be children of the WorkList.
        lane1 = self._lane(display_name="Top-level Lane 1")
        lane1.priority = 0
        lane2 = self._lane(display_name="Top-level Lane 2")
        lane2.priority = 1

        # This lane is invisible and will be filtered out.
        invisible_lane = self._lane(display_name="Invisible Lane")
        invisible_lane.visible = False

        # This lane has a parent and will be filtered out.
        sublane = self._lane(display_name="Sublane")
        lane1.sublanes.append(sublane)

        # This lane belongs to a different library.
        other_library = self._library(
            name="Other Library", short_name="Other"
        )
        other_library_lane = self._lane(
            display_name="Other Library Lane", library=other_library
        )

        # The default library gets a WorkList with the two top-level lanes as children.
        wl = load_lanes(self._db, self._default_library)
        eq_([lane1, lane2], wl.children)

        # The other library only has one top-level lane, so we use that lane.
        l = load_lanes(self._db, other_library)
        eq_(other_library_lane, l)

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

        # If the lane is created with languages, works in other languages
        # aren't included.
        fre = self._work(with_license_pool=True, language='fre')
        spa = self._work(with_license_pool=True, language='spa')
        for work in [fre, spa]:
            work.presentation_edition.series = series_name
        SessionManager.refresh_materialized_views(self._db)

        lane.languages = ['fre', 'spa']
        self.assert_works_queries(lane, [fre, spa])


    def test_childrens_series_with_same_name_as_adult_series(self):
        [children, ya, adult, adults_only] = self.sample_works_for_each_audience()

        # Give them all the same series name.
        series_name = "Monkey Business"
        for work in [children, adult, adults_only]:
            work.presentation_edition.series = series_name
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
