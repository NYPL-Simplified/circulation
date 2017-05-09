# encoding: utf-8
from nose.tools import set_trace, eq_, assert_raises

from . import (
    DatabaseTest,
)

from core.classifier import Classifier
from core.lane import (
    Lane,
    LaneList,
)
from core.metadata_layer import Metadata
from core.model import (
    Contribution,
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
    WorkBasedLane,
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


class TestWorkBasedLane(DatabaseTest):

    def test_initialization_sets_appropriate_audiences(self):
        work = self._work(with_license_pool=True)

        work.audience = Classifier.AUDIENCE_CHILDREN
        children_lane = WorkBasedLane(self._db, work, '')
        eq_(set([Classifier.AUDIENCE_CHILDREN]), children_lane.audiences)

        work.audience = Classifier.AUDIENCE_YOUNG_ADULT
        ya_lane = WorkBasedLane(self._db, work, '')
        eq_(sorted(Classifier.AUDIENCES_JUVENILE), sorted(ya_lane.audiences))

        work.audience = Classifier.AUDIENCE_ADULT
        adult_lane = WorkBasedLane(self._db, work, '')
        eq_(sorted(Classifier.AUDIENCES), sorted(adult_lane.audiences))

        work.audience = Classifier.AUDIENCE_ADULTS_ONLY
        adults_only_lane = WorkBasedLane(self._db, work, '')
        eq_(sorted(Classifier.AUDIENCES), sorted(adults_only_lane.audiences))


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

        with temp_config() as config:
            # A book without a series or a contributor on a circ manager without
            # NoveList recommendations raises an error.
            config['integrations'][Configuration.NOVELIST_INTEGRATION] = {}
            self._db.delete(self.edition.contributions[0])
            self._db.commit()

            assert_raises(
                ValueError, RelatedBooksLane, self._db, self.work, ""
            )

            # A book with a contributor initializes a RelatedBooksLane.
            luthor, i = self._contributor('Luthor, Lex')
            self.edition.add_contributor(luthor, [Contributor.EDITOR_ROLE])

            result = RelatedBooksLane(self._db, self.work, '')
            eq_(self.work, result.work)
            [sublane] = result.sublanes
            eq_(True, isinstance(sublane, ContributorLane))
            eq_(sublane.contributor, luthor)

            # As does a book in a series.
            self.edition.series = "All By Myself"
            result = RelatedBooksLane(self._db, self.work, "")
            eq_(2, len(result.sublanes))
            [contributor, series] = result.sublanes
            eq_(True, isinstance(series, SeriesLane))

        with temp_config() as config:
            config['integrations'][Configuration.NOVELIST_INTEGRATION] = {
                Configuration.NOVELIST_PROFILE : 'library',
                Configuration.NOVELIST_PASSWORD : 'sure'
            }
            # When NoveList is configured and recommendations are available,
            # a RecommendationLane will be included.
            mock_api = MockNoveListAPI()
            response = Metadata(
                self.edition.data_source, recommendations=[self._identifier()]
            )
            mock_api.setup(response)
            result = RelatedBooksLane(self._db, self.work, "", novelist_api=mock_api)
            eq_(3, len(result.sublanes))

            # The book's language and audience list is passed down to all sublanes.
            eq_(['eng'], result.languages)
            for sublane in result.sublanes:
                eq_(result.languages, sublane.languages)
                if isinstance(sublane, SeriesLane):
                    eq_(set([result.source_audience]), sublane.audiences)
                else:
                    eq_(sorted(list(result.audiences)), sorted(list(sublane.audiences)))

            contributor, recommendations, series = result.sublanes
            eq_(True, isinstance(recommendations, RecommendationLane))
            eq_(True, isinstance(series, SeriesLane))
            eq_(True, isinstance(contributor, ContributorLane))

    def test_contributor_lane_generation(self):

        with temp_config() as config:
            config['integrations'][Configuration.NOVELIST_INTEGRATION] = {}

            original = self.edition.contributions[0].contributor
            luthor, i = self._contributor('Luthor, Lex')
            self.edition.add_contributor(luthor, Contributor.EDITOR_ROLE)

            # Lex Luthor doesn't show up because he's only an editor,
            # and an author is listed.
            result = RelatedBooksLane(self._db, self.work, '')
            eq_(1, len(result.sublanes))
            [sublane] = result.sublanes
            eq_(original, sublane.contributor)

        # A book with multiple contributors results in multiple
        # ContributorLane sublanes.
        lane, i = self._contributor('Lane, Lois')
        self.edition.add_contributor(lane, Contributor.PRIMARY_AUTHOR_ROLE)
        result = RelatedBooksLane(self._db, self.work, '')
        eq_(2, len(result.sublanes))
        sublane_contributors = [c.contributor for c in result.sublanes]
        eq_(set([lane, original]), set(sublane_contributors))

        # When there are no AUTHOR_ROLES present, contributors in
        # displayable secondary roles appear.
        for contribution in self.edition.contributions:
            if contribution.role in Contributor.AUTHOR_ROLES:
                self._db.delete(contribution)
        self._db.commit()

        result = RelatedBooksLane(self._db, self.work, '')
        eq_(1, len(result.sublanes))
        [sublane] = result.sublanes
        eq_(luthor, sublane.contributor)

    def test_works_query(self):
        """RelatedBooksLane is an invisible, groups lane without works."""

        self.edition.series = "All By Myself"
        lane = RelatedBooksLane(self._db, self.work, "")
        eq_(None, lane.works())
        eq_(None, lane.materialized_works())


class LaneTest(DatabaseTest):

    def assert_works_queries(self, lane, expected):
        """Tests resulting Lane.works() and Lane.materialized_works() results"""

        query = lane.works()
        eq_(sorted(expected), sorted(query.all()))

        materialized_expected = expected
        if expected:
            materialized_expected = [work.id for work in expected]
        results = lane.materialized_works().all()
        materialized_results = [work.works_id for work in results]
        eq_(sorted(materialized_expected), sorted(materialized_results))

    def sample_works_for_each_audience(self):
        """Create a work for each audience-type."""
        works = list()
        audiences = [
            Classifier.AUDIENCE_CHILDREN, Classifier.AUDIENCE_YOUNG_ADULT,
            Classifier.AUDIENCE_ADULT, Classifier.AUDIENCE_ADULTS_ONLY
        ]

        for audience in audiences:
            work = self._work(with_license_pool=True, audience=audience)
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

        mock_api = MockNoveListAPI()
        mock_api.setup(metadata)
        return mock_api

    def test_works_query(self):
        # Prep an empty result.
        mock_api = self.generate_mock_api()

        # With an empty recommendation result, the lane is empty.
        lane = RecommendationLane(self._db, self.work, '', novelist_api=mock_api)
        eq_(None, lane.works())
        eq_(None, lane.materialized_works())

        # Resulting recommendations are returned when available, though.
        result = self._work(with_license_pool=True)
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
                self._db, self.work, '', novelist_api=mock_api
            )
            lane.recommendations = recommendations
            self.assert_works_queries(lane, results)

    def test_works_query_with_source_language(self):
        # Prepare a number of works with different languages.
        eng = self._work(with_license_pool=True, language='eng')
        fre = self._work(with_license_pool=True, language='fre')
        spa = self._work(with_license_pool=True, language='spa')
        SessionManager.refresh_materialized_views(self._db)

        # They're all returned as recommendations from NoveList Select.
        recommendations = list()
        for work in [eng, fre, spa]:
            recommendations.append(work.license_pools[0].identifier)

        # But only the work that matches the source work is included.
        mock_api = self.generate_mock_api()
        lane = RecommendationLane(self._db, self.work, '', novelist_api=mock_api)
        lane.recommendations = recommendations
        self.assert_works_queries(lane, [eng])

        # It doesn't matter the language.
        self.work.presentation_edition.language = 'fre'
        SessionManager.refresh_materialized_views(self._db)
        mock_api = self.generate_mock_api()
        lane = RecommendationLane(self._db, self.work, '', novelist_api=mock_api)
        lane.recommendations = recommendations
        self.assert_works_queries(lane, [fre])


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
            self._db, series_name, audiences=[Classifier.AUDIENCE_CHILDREN]
        )
        self.assert_works_queries(children_lane, [children])

        # It's strict about this, in an attempt to increase series accuracy.
        # A request for adult material, only returns Adult material, not
        # Adults Only material.
        adult_lane = SeriesLane(
            self._db, series_name, audiences=[Classifier.AUDIENCE_ADULT]
        )
        self.assert_works_queries(adult_lane, [adult])

        adult_lane = SeriesLane(
            self._db, series_name, audiences=[Classifier.AUDIENCE_ADULTS_ONLY]
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
        assert_raises(ValueError, ContributorLane, self._db, '')

        # An error is raised if ContributorLane is created with a name and
        # ID that don't match.
        assert_raises(
            ValueError, ContributorLane,
            self._db, 'Clark Kent', self.contributor.id
        )

    def test_works_query(self):
        # A work by someone else.
        w1 = self._work(with_license_pool=True)

        # A work by the contributor with the same name, without VIAF info.
        w2 = self._work(title="X is for Xylophone", with_license_pool=True)
        same_name = w2.presentation_edition.contributions[0].contributor
        same_name.display_name = 'Lois Lane'
        SessionManager.refresh_materialized_views(self._db)

        # The work with a matching name is found in the contributor lane.
        lane = ContributorLane(
            self._db, 'Lois Lane', contributor_id=self.contributor.id
        )
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

        # When the lane is created without a contributor_id, the query
        # only searches by name.
        lane = ContributorLane(self._db, 'Lois Lane')
        self.assert_works_queries(lane, [w3, w2])

        # If the lane is created with languages, works in other languages
        # aren't included.
        fre = self._work(with_license_pool=True, language='fre')
        spa = self._work(with_license_pool=True, language='spa')
        for work in [fre, spa]:
            main_contribution = work.presentation_edition.contributions[0]
            main_contribution.contributor = self.contributor
        SessionManager.refresh_materialized_views(self._db)

        lane = ContributorLane(self._db, 'Lois Lane', languages=['eng'])
        self.assert_works_queries(lane, [w3, w2])

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
            self._db, 'Lois Lane', audiences=[Classifier.AUDIENCE_CHILDREN]
        )
        self.assert_works_queries(children_lane, [children])

        # When more than one audience is requested, all are included.
        ya_lane = ContributorLane(
            self._db, 'Lois Lane', audiences=list(Classifier.AUDIENCES_JUVENILE)
        )
        self.assert_works_queries(ya_lane, [children, ya])
