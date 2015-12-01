from nose.tools import set_trace, eq_

from . import (
    DatabaseTest,
)

from ..config import (
    Configuration,
    temp_config,
)

from ..core.lane import (
    LaneList,
)

from ..lanes import (
    make_lanes,
    make_lanes_default,
    lanes_for_large_collection,
    lane_for_small_collection,
    lane_for_other_languages,
)

from ..core.lane import Lane

class TestLaneCreation(DatabaseTest):

    def test_lanes_for_large_collection(self):
        languages = ['eng', 'spa']
        lanes = lanes_for_large_collection(self._db, languages)

        # We have five lanes.
        eq_(
            ['Adult Fiction', 'Adult Nonfiction', 'Young Adult Fiction', 
             'Young Adult Nonfiction', 'Children and Middle Grade'],
            [x.name for x in lanes]
        )

        # They all are restricted to English and Spanish.
        assert all(x.languages==languages for x in lanes)

        # The Adult Fiction and Adult Nonfiction lanes reproduce the
        # genre structure found in the genre definitions.
        fiction, nonfiction = lanes[0:2]
        [sf] = [x for x in fiction.sublanes.lanes if x.name=='Science Fiction']
        eq_(True, sf.fiction)
        eq_("Science Fiction", sf.name)
        assert 'Science Fiction' in [x.name for x in sf.genres]
        assert 'Cyberpunk' in [x.name for x in sf.genres]

        [space_opera] = [x for x in sf.sublanes.lanes if x.name=='Space Opera']
        eq_(True, sf.fiction)
        eq_("Space Opera", space_opera.name)
        eq_(["Space Opera"], [x.name for x in space_opera.genres])

        [history] = [x for x in nonfiction.sublanes.lanes if x.name=='History']
        eq_(False, history.fiction)
        eq_("History", history.name)
        assert 'History' in [x.name for x in history.genres]
        assert 'European History' in [x.name for x in history.genres]

    def test_lane_for_small_collection(self):
        lane = lane_for_small_collection(self._db, ['eng', 'spa', 'chi'])
        eq_("English, Spanish, & Chinese", lane.name)
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

        exclude = ['eng', 'spa']
        lane = lane_for_other_languages(self._db, exclude)
        eq_(None, lane.languages)
        eq_(exclude, lane.exclude_languages)
        eq_("Other Languages", lane.name)
        eq_(
            ['Adult Fiction', 'Adult Nonfiction', 'Children & Young Adult'],
            [x.name for x in lane.sublanes.lanes]
        )

    def test_make_lanes_default(self):
        with temp_config() as config:
            languages = Configuration.language_policy()
            languages[Configuration.LARGE_COLLECTION_LANGUAGES] = 'eng'
            languages[Configuration.SMALL_COLLECTION_LANGUAGES] = 'spa,chi'

            lane_list = make_lanes_default(self._db)

            assert isinstance(lane_list, LaneList)
            lanes = lane_list.lanes

            # We have a set of top-level lanes for the large collections,
            # a top-level lane for each small collection, and a lane
            # for everything left over.
            eq_(['Adult Fiction', 'Adult Nonfiction', 'Young Adult Fiction', 'Young Adult Nonfiction', 'Children and Middle Grade', 'Spanish', 'Chinese', 'Other Languages'],
                [x.name for x in lane_list.lanes]
            )
            
