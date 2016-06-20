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

    def test_initialization(self):
        """Asserts that a RelatedBooksLane won't be initialized for a work
        without related books
        """
        work = self._work(with_license_pool=True)
        [lp] = work.license_pools
        with temp_config() as config:
            # A book without a series on a circ manager without
            # NoveList recommendations raises an error.
            config['integrations'][Configuration.NOVELIST_INTEGRATION] = {}
            assert_raises(
                ValueError, RelatedBooksLane, self._db, lp, ""
            )

            # But a book from a series initializes a RelatedBooksLane just fine.
            lp.presentation_edition.series = "All By Myself"
            result = RelatedBooksLane(self._db, lp, "")
            eq_(lp, result.license_pool)
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
                lp.data_source, recommendations=[self._identifier()]
            )
            mock_api.setup(response)
            result = RelatedBooksLane(self._db, lp, "", novelist_api=mock_api)
            eq_(2, len(result.sublanes))
            recommendations, series = result.sublanes
            eq_(True, isinstance(recommendations, RecommendationLane))
            eq_(True, isinstance(series, SeriesLane))
