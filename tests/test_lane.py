from nose.tools import (
    eq_,
    set_trace,
)

from psycopg2.extras import NumericRange

from . import (
    DatabaseTest,
)

import classifier
from classifier import (
    Classifier,
)

from lane import (
    Lane,
    LaneList,
)

from config import (
    Configuration, 
    temp_config,
)

from model import (
    Genre,
    Work,
    SessionManager,
)


class TestLanes(DatabaseTest):

    def test_setup(self):
        fantasy_genre, ig = Genre.lookup(self._db, classifier.Fantasy)
        epic_fantasy, ig = Genre.lookup(self._db, classifier.Epic_Fantasy)
        historical_fantasy, ig = Genre.lookup(
            self._db, classifier.Historical_Fantasy)
        urban_fantasy, ig = Genre.lookup(
            self._db, classifier.Urban_Fantasy)
        fantasy_subgenres = classifier.Fantasy.subgenres

        # Here's an 'adult fantasy' lane, in which the subgenres of Fantasy
        # have their own lanes.
        adult_fantasy_lane = Lane(
            self._db, fantasy_genre.name, 
            [fantasy_genre], Lane.IN_SAME_LANE,
            fiction=Lane.FICTION_DEFAULT_FOR_GENRE,
            audience=Classifier.AUDIENCE_ADULT,
            sublanes=fantasy_subgenres
        )

        fantasy_and_subgenres = set([
            fantasy_genre, urban_fantasy, epic_fantasy, historical_fantasy])

        # Although the subgenres have their own lanes, the parent lane
        # also incorporates books from the subgenres.
        eq_(fantasy_and_subgenres, set(adult_fantasy_lane.genres))
        eq_(Classifier.AUDIENCE_ADULT, adult_fantasy_lane.audience)
        eq_(Lane.FICTION_DEFAULT_FOR_GENRE, adult_fantasy_lane.fiction)

        # Here's a 'YA Fantasy' lane, which has no sublanes.
        ya_fantasy_lane = Lane(
            self._db, fantasy_genre.name, 
            [fantasy_genre], Lane.IN_SAME_LANE,
            fiction=Lane.FICTION_DEFAULT_FOR_GENRE,
            audience=Classifier.AUDIENCE_YOUNG_ADULT)

        # The parent lane also includes books from the subgenres.
        eq_(fantasy_and_subgenres, set(ya_fantasy_lane.genres))
        eq_(Classifier.AUDIENCE_YOUNG_ADULT, ya_fantasy_lane.audience)

        # Here's a 'YA Science Fiction' lane, which has no sublanes,
        # and which excludes Dystopian SF and Steampunk (which have their
        # own lanes on the same level as 'YA Science Fiction')
        ya_sf = Lane(
            self._db, full_name="YA Science Fiction",
            display_name="Science Fiction",
            genres=[classifier.Science_Fiction],
            subgenre_behavior=Lane.IN_SAME_LANE,
            exclude_genres=[
                classifier.Dystopian_SF, classifier.Steampunk],
            audience=Classifier.AUDIENCE_YOUNG_ADULT)
        eq_([], ya_sf.sublanes.lanes)
        eq_("YA Science Fiction", ya_sf.name)
        eq_("Science Fiction", ya_sf.display_name)
        included_subgenres = [x.name for x in ya_sf.genres]
        assert "Cyberpunk" in included_subgenres
        assert "Dystopian SF" not in included_subgenres
        assert "Steampunk" not in included_subgenres

    def test_materialized_works(self):
        from model import (
            MaterializedWork,
            MaterializedWorkWithGenre,
        )

        # Look up two genres.
        fantasy, ig = Genre.lookup(self._db, classifier.Fantasy)
        cooking, ig = Genre.lookup(self._db, classifier.Cooking)

        # Here's a fantasy book.
        w1 = self._work(genre=fantasy, with_license_pool=True)
        w1.simple_opds_entry = "foo"

        # Here's a cooking book.
        w2 = self._work(genre=cooking, fiction=False, with_license_pool=True)
        w2.simple_opds_entry = "bar"

        # Here's a fantasy book for children ages 7-9.
        w3 = self._work(genre=fantasy, fiction=True, with_license_pool=True,
                        audience=Classifier.AUDIENCE_CHILDREN)
        w3.target_age = NumericRange(7,9)
        w3.simple_opds_entry = "baz"
        
        # Refresh the materialized views so that all three books are
        # included in the views.
        SessionManager.refresh_materialized_views(self._db)

        # Let's get materialized works from the Fantasy genre.
        fantasy_lane = Lane(
            self._db, full_name="Fantasy", genres=[classifier.Fantasy])
        [materialized] = fantasy_lane.materialized_works(['eng']).all()

        # This materialized work corresponds to the adult fantasy book. We
        # did not get an entry for the cooking book or the children's book.
        assert isinstance(materialized, MaterializedWorkWithGenre)
        eq_(materialized.works_id, w1.id)

        # Let's get materialized works of nonfiction.
        nonfiction_lane = Lane(
            self._db, full_name="Nonfiction", genres=[], fiction=False)
        [materialized] = nonfiction_lane.materialized_works().all()

        # This materialized work corresponds to the cooking book. We
        # did not get an entry for the other books.
        assert isinstance(materialized, MaterializedWork)
        eq_(materialized.works_id, w2.id)

        # Let's get materialized works suitable for children age 8.
        age_8_lane = Lane(
            self._db, full_name="Age 8", genres=[], audience='Children',
            age_range=[8]
        )
        [materialized] = age_8_lane.materialized_works().all()
        assert isinstance(materialized, MaterializedWork)
        eq_(materialized.works_id, w3.id)

        # We get the same book by asking for works suitable for
        # children ages 7-10.
        age_7_10_lane = Lane(
            self._db, full_name="Ages 7-10", genres=[], audience='Children',
            age_range=[7,10]
        )
        [materialized] = age_7_10_lane.materialized_works().all()
        assert isinstance(materialized, MaterializedWork)
        eq_(materialized.works_id, w3.id)

        # Verify that the language restriction works.
        eq_([], fantasy_lane.materialized_works(['fre']).all())

    def test_availability_restriction(self):

        fantasy, ig = Genre.lookup(self._db, classifier.Fantasy)

        # Here's a fantasy book.
        w1 = self._work(genre=fantasy, with_license_pool=True)

        # The book is not available.
        w1.license_pools[0].licenses_available = 0
        w1.license_pools[0].open_access = False
        self._db.commit()

        fantasy_lane = Lane(
            self._db, full_name="Fantasy", genres=[fantasy])

        # So long as the hold behavior allows books to be put on hold,
        # the book will show up in lanes.
        with temp_config() as config:
            config['policies'][Configuration.HOLD_POLICY] = Configuration.HOLD_POLICY_ALLOW
            allow_on_hold_works = fantasy_lane.works(['eng']).all()
            eq_(1, len(allow_on_hold_works))

        # When the hold behavior is to hide unavailable books, the
        # book disappears.
        with temp_config() as config:
            config['policies'][Configuration.HOLD_POLICY] = Configuration.HOLD_POLICY_HIDE
            hide_on_hold_works = fantasy_lane.works(['eng']).all()
            eq_([], hide_on_hold_works)

            # When the book becomes available, it shows up in lanes again.
            w1.license_pools[0].licenses_available = 1
            hide_on_hold_works = fantasy_lane.works(['eng']).all()
            eq_(1, len(hide_on_hold_works))
        

class TestLaneList(DatabaseTest):
    
    def test_from_description(self):
        lanes = LaneList.from_description(
            self._db,
            None,
            [dict(full_name="Fiction",
                  fiction=True,
                  audience=Classifier.AUDIENCE_ADULT,
                  genres=[]),
             classifier.Fantasy,
             dict(
                 full_name="Young Adult",
                 fiction=Lane.BOTH_FICTION_AND_NONFICTION,
                 audience=Classifier.AUDIENCE_YOUNG_ADULT,
                 genres=[]),
         ]
        )

        fantasy_genre, ignore = Genre.lookup(self._db, classifier.Fantasy.name)

        fiction = lanes.by_name['Fiction']
        young_adult = lanes.by_name['Young Adult']
        fantasy = lanes.by_name['Fantasy'] 

        eq_(set([fantasy, fiction, young_adult]), set(lanes.lanes))

        eq_("Fiction", fiction.name)
        eq_(Classifier.AUDIENCE_ADULT, fiction.audience)
        eq_([], fiction.genres)
        eq_(True, fiction.fiction)

        eq_("Fantasy", fantasy.name)
        eq_(Classifier.AUDIENCES_ADULT, fantasy.audience)
        eq_([fantasy_genre], fantasy.genres)
        eq_(Lane.FICTION_DEFAULT_FOR_GENRE, fantasy.fiction)

        eq_("Young Adult", young_adult.name)
        eq_(Classifier.AUDIENCE_YOUNG_ADULT, young_adult.audience)
        eq_([], young_adult.genres)
        eq_(Lane.BOTH_FICTION_AND_NONFICTION, young_adult.fiction)
