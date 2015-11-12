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
    Facets,
    Pagination,
    Lane,
    LaneList,
)

from config import (
    Configuration, 
    temp_config,
)

from model import (
    get_one_or_create,
    DataSource,
    Genre,
    Work,
    Edition,
    SessionManager,
    WorkGenre,
)


class TestFacets(object):

    def test_order_facet_to_database_field(self):
        from model import (
            MaterializedWork as mw,
            MaterializedWorkWithGenre as mwg,
        )

        def fields(facet):
            return [
                Facets.order_facet_to_database_field(facet, w, e)
                for w, e in ((Work, Edition), (mw, mw), (mwg, mwg))
            ]

        # You can sort by title...
        eq_([Edition.sort_title, mw.sort_title, mwg.sort_title],
            fields(Facets.ORDER_TITLE))

        # ...by author...
        eq_([Edition.sort_author, mw.sort_author, mwg.sort_author],
            fields(Facets.ORDER_AUTHOR))

        # ...by work ID...
        eq_([Work.id, mw.works_id, mwg.works_id],
            fields(Facets.ORDER_WORK_ID))

        # ...by last update time...
        eq_([Work.last_update_time, mw.last_update_time, mwg.last_update_time],
            fields(Facets.ORDER_LAST_UPDATE))

        # ...or randomly.
        eq_([Work.random, mw.random, mwg.random],
            fields(Facets.ORDER_RANDOM))

    def test_order_by(self):
        from model import (
            MaterializedWork as mw,
            MaterializedWorkWithGenre as mwg,
        )

        def order(facet, work, edition, ascending=True):
            f = Facets(
                collection=Facets.COLLECTION_FULL, 
                availability=Facets.AVAILABLE_ALL,
                order=facet,
                order_ascending=ascending,
            )
            return f.order_by(work, edition)

        def compare(a, b):
            assert(len(a) == len(b))
            for i in range(0, len(a)):
                assert(a[i].compare(b[i]))

        expect = [Edition.sort_author.asc(), Edition.sort_title.asc(), Work.id.asc()]
        actual = order(Facets.ORDER_AUTHOR, Work, Edition, True)  
        compare(expect, actual)

        expect = [Edition.sort_author.desc(), Edition.sort_title.desc(), Work.id.desc()]
        actual = order(Facets.ORDER_AUTHOR, Work, Edition, False)  
        compare(expect, actual)

        expect = [mw.sort_title.asc(), mw.sort_author.asc(), mw.works_id.asc()]
        actual = order(Facets.ORDER_TITLE, mw, mw, True)
        compare(expect, actual)

        expect = [mwg.works_id.asc(), mwg.sort_title.asc(), mwg.sort_author.asc()]
        actual = order(Facets.ORDER_WORK_ID, mwg, mwg, True)
        compare(expect, actual)

        expect = [Work.last_update_time.asc(), Edition.sort_title.asc(), Edition.sort_author.asc(), Work.id.asc()]
        actual = order(Facets.ORDER_LAST_UPDATE, Work, Edition, True)
        compare(expect, actual)

        expect = [mw.random.asc(), mw.sort_title.asc(), mw.sort_author.asc(),
                  mw.works_id.asc()]
        actual = order(Facets.ORDER_RANDOM, mw, mw, True)
        compare(expect, actual)

class TestFacetsApply(DatabaseTest):

    def test_apply(self):
        # Set up works that are matched by different types of collections.

        # A high-quality open-access work.
        open_access_high = self._work(with_open_access_download=True)
        open_access_high.quality = 0.8
        open_access_high.random = 0.2
        
        # A low-quality open-access work.
        open_access_low = self._work(with_open_access_download=True)
        open_access_low.quality = 0.2
        open_access_low.random = 0.4

        # A high-quality licensed work which is not currently available.
        (licensed_e1, licensed_p1) = self._edition(
            data_source_name=DataSource.OVERDRIVE,
            with_license_pool=True)
        licensed_high = self._work(primary_edition=licensed_e1)
        licensed_high.license_pools.append(licensed_p1)
        licensed_high.quality = 0.8
        licensed_p1.open_access = False
        licensed_p1.licenses_owned = 1
        licensed_p1.licenses_available = 0
        licensed_high.random = 0.3

        # A low-quality licensed work which is currently available.
        (licensed_e2, licensed_p2) = self._edition(
            data_source_name=DataSource.OVERDRIVE,
            with_license_pool=True)
        licensed_p2.open_access = False
        licensed_low = self._work(primary_edition=licensed_e2)
        licensed_low.license_pools.append(licensed_p2)
        licensed_low.quality = 0.2
        licensed_p2.licenses_owned = 1
        licensed_p2.licenses_available = 1
        licensed_low.random = 0.1

        def facetify(collection=Facets.COLLECTION_FULL, 
                     available=Facets.AVAILABLE_ALL,
                     order=Facets.ORDER_TITLE
        ):
            f = Facets(collection, available, order)
            return f.apply(self._db, qu)

        # We start by finding all works.
        qu = self._db.query(Work).join(Work.primary_edition).join(
            Work.license_pools
        )
        eq_(4, qu.count())

        # If we restrict to books currently available we lose one book.
        available_now = facetify(available=Facets.AVAILABLE_NOW)
        eq_(3, available_now.count())
        assert licensed_high not in available_now

        # If we restrict to open-access books we lose two books.
        open_access = facetify(available=Facets.AVAILABLE_OPEN_ACCESS)
        eq_(2, open_access.count())
        assert licensed_high not in open_access
        assert licensed_low not in open_access

        # If we restrict to the main collection we lose the low-quality
        # open-access book.
        main_collection = facetify(collection=Facets.COLLECTION_MAIN)
        eq_(3, main_collection.count())
        assert open_access_low not in main_collection

        # If we restrict to the featured collection we lose both
        # low-quality books
        featured_collection = facetify(collection=Facets.COLLECTION_FEATURED)
        eq_(2, featured_collection.count())
        assert open_access_low not in featured_collection
        assert licensed_low not in featured_collection

        title_order = facetify(order=Facets.ORDER_TITLE)
        eq_([open_access_high, open_access_low, licensed_high, licensed_low],
            title_order.all())

        random_order = facetify(order=Facets.ORDER_RANDOM)
        eq_([licensed_low, open_access_high, licensed_high, open_access_low],
            random_order.all())

class TestLanes(DatabaseTest):

    def setup(self):
        super(TestLanes, self).setup()

        # Look up the Fantasy genre and some of its subgenres.
        self.fantasy, ig = Genre.lookup(self._db, classifier.Fantasy)
        self.epic_fantasy, ig = Genre.lookup(self._db, classifier.Epic_Fantasy)
        self.urban_fantasy, ig = Genre.lookup(
            self._db, classifier.Urban_Fantasy
        )

        # Look up the History genre and some of its subgenres.
        self.history, ig = Genre.lookup(self._db, classifier.History)
        self.african_history, ig = Genre.lookup(
            self._db, classifier.African_History
        )

        self.adult_works = {}
        self.ya_works = {}
        self.childrens_works = {}

        for genre in (self.fantasy, self.epic_fantasy, self.urban_fantasy,
                      self.history, self.african_history):
            fiction = True
            if genre in (self.history, self.african_history):
                fiction = False

            # Create a number of books for each genre.
            adult_work = self._work(
                title="%s Adult" % genre.name, 
                audience=Lane.AUDIENCE_ADULT,
                fiction=fiction,
                with_license_pool=True,
                genre=genre,
            )
            self.adult_works[genre] = adult_work
            adult_work.simple_opds_entry = '<entry>'

            # Childrens and YA books need to be attached to a data
            # source other than Gutenberg, or they'll get filtered
            # out.
            ya_edition = self._edition(
                data_source_name=DataSource.OVERDRIVE,
                with_license_pool=True
            )
            ya_work = self._work(
                title="%s YA" % genre.name, 
                audience=Lane.AUDIENCE_YOUNG_ADULT,
                fiction=fiction,
                with_license_pool=True,
                primary_edition=ya_edition,
                genre=genre,
            )
            self.ya_works[genre] = ya_work
            ya_work.simple_opds_entry = '<entry>'

            childrens_edition = self._edition(
                data_source_name=DataSource.OVERDRIVE, with_license_pool=True
            )
            childrens_work = self._work(
                title="%s Childrens" % genre.name,
                audience=Lane.AUDIENCE_CHILDREN,
                fiction=fiction,
                with_license_pool=True,
                primary_edition=childrens_edition,
                genre=genre,
            )
            if genre == self.epic_fantasy:
                childrens_work.target_age = NumericRange(7, 9, '[]')
            else:
                childrens_work.target_age = NumericRange(8, 10, '[]')
            self.childrens_works[genre] = childrens_work
            childrens_work.simple_opds_entry = '<entry>'

        # Create generic 'Adults Only' fiction and nonfiction books
        # that are not in any genre.
        self.nonfiction = self._work(
            title="Generic Nonfiction", fiction=False,
            audience=Lane.AUDIENCE_ADULTS_ONLY,
            with_license_pool=True
        )
        self.nonfiction.simple_opds_entry = '<entry>'
        self.fiction = self._work(
            title="Generic Fiction", fiction=True,
            audience=Lane.AUDIENCE_ADULTS_ONLY,
            with_license_pool=True
        )
        self.fiction.simple_opds_entry = '<entry>'

        # Create a work of music.
        self.music = self._work(
            title="Music", fiction=False,
            audience=Lane.AUDIENCE_ADULT,
            with_license_pool=True,
        )
        self.music.primary_edition.medium=Edition.MUSIC_MEDIUM
        self.music.simple_opds_entry = '<entry>'

        # Refresh the materialized views so that all these books are present
        # in the 
        SessionManager.refresh_materialized_views(self._db)

    def test_lanes(self):
        # I'm putting all these tests into one method because the
        # setup function is so very expensive.

        def test_expectations(lane, expected_count, predicate,
                              mw_predicate=None):
            """Ensure that a database query and a query of the
            materialized view give the same results.
            """
            mw_predicate = mw_predicate or predicate
            w = lane.works().all()
            mw = lane.materialized_works().all()
            eq_(len(w), expected_count)
            eq_(len(mw), expected_count)
            assert all([predicate(x) for x in w])
            assert all([mw_predicate(x) for x in mw])
            return w, mw

        # The 'everything' lane contains 18 works.
        lane = Lane.everything(self._db, media=None)
        w, mw = test_expectations(lane, 18, lambda x: True)

        # The 'music' lane contains 1 work of music
        lane = Lane.everything(self._db, media=Edition.MUSIC_MEDIUM)
        w, mw = test_expectations(
            lane, 1, 
            lambda x: x.primary_edition.medium==Edition.MUSIC_MEDIUM,
            lambda x: x.medium==Edition.MUSIC_MEDIUM
        )
        
        # The 'fiction' lane contains ten fiction books.
        lane = Lane.everything(self._db, fiction=True)
        w, mw = test_expectations(
            lane, 10, lambda x: x.fiction
        )

        # The 'nonfiction' lane contains seven nonfiction books.
        # It does not contain the music.
        lane = Lane.everything(self._db, fiction=False)
        w, mw = test_expectations(
            lane, 7, 
            lambda x: x.primary_edition.medium==Edition.BOOK_MEDIUM and not x.fiction,
            lambda x: x.medium==Edition.BOOK_MEDIUM and not x.fiction
        )

        # The 'adults' lane contains five books for adults.
        lane = Lane.everything(self._db, audience=Lane.AUDIENCE_ADULT)
        w, mw = test_expectations(
            lane, 5, lambda x: x.audience==Lane.AUDIENCE_ADULT
        )

        # This lane contains those five books plus two adults-only
        # books.
        audiences = [Lane.AUDIENCE_ADULT, Lane.AUDIENCE_ADULTS_ONLY]
        lane = Lane.everything(self._db, audience=audiences)
        w, mw = test_expectations(
            lane, 7, lambda x: x.audience in audiences
        )
        assert(2, len([x for x in w if x.audience==Lane.AUDIENCE_ADULTS_ONLY]))
        assert(2, len([x for x in mw if x.audience==Lane.AUDIENCE_ADULTS_ONLY]))

        # The 'Young Adults' lane contains five books.
        lane = Lane.everything(self._db, audience=Lane.AUDIENCE_YOUNG_ADULT)
        w, mw = test_expectations(
            lane, 5, lambda x: x.audience==Lane.AUDIENCE_YOUNG_ADULT
        )

        # There is one book suitable for seven-year-olds.
        lane = Lane.everything(
            self._db, audience=Lane.AUDIENCE_CHILDREN,
            age_range=7
        )
        w, mw = test_expectations(
            lane, 1, lambda x: x.audience==Lane.AUDIENCE_CHILDREN
        )

        # There are four books suitable for ages 10-12.
        lane = Lane.everything(
            self._db, audience=Lane.AUDIENCE_CHILDREN,
            age_range=(10,12)
        )
        w, mw = test_expectations(
            lane, 4, lambda x: x.audience==Lane.AUDIENCE_CHILDREN
        )
       
        #
        # Now let's start messing with genres.
        #

        # Here's an 'adult fantasy' lane, in which the subgenres of Fantasy
        # have their own lanes.
        lane = Lane(
            self._db, full_name="Adult Fantasy",
            genres=[self.fantasy], 
            subgenre_behavior=Lane.IN_SAME_LANE,
            fiction=Lane.FICTION_DEFAULT_FOR_GENRE,
            audience=Lane.AUDIENCE_ADULT,
        )

        # We get three books: Fantasy, Urban Fantasy, and Epic Fantasy.
        w, mw = test_expectations(
            lane, 3, lambda x: True
        )
        set_trace()        

#     def test_setup(self):
#         fantasy_genre, ig = 
#         epic_fantasy, ig = 
#         historical_fantasy, ig = Genre.lookup(
#             self._db, classifier.Historical_Fantasy)
#         urban_fantasy, ig = Genre.lookup(
#             self._db, classifier.Urban_Fantasy)
#         fantasy_subgenres = classifier.Fantasy.subgenres


#         fantasy_and_subgenres = set([
#             fantasy_genre, urban_fantasy, epic_fantasy, historical_fantasy])

#         # Although the subgenres have their own lanes, the parent lane
#         # also incorporates books from the subgenres.
#         eq_(fantasy_and_subgenres, set(adult_fantasy_lane.genres))
#         eq_(Classifier.AUDIENCE_ADULT, adult_fantasy_lane.audience)
#         eq_(Lane.FICTION_DEFAULT_FOR_GENRE, adult_fantasy_lane.fiction)

#         # Here's a 'YA Fantasy' lane, which has no sublanes.
#         ya_fantasy_lane = Lane(
#             self._db, fantasy_genre.name, 
#             [fantasy_genre], Lane.IN_SAME_LANE,
#             fiction=Lane.FICTION_DEFAULT_FOR_GENRE,
#             audience=Classifier.AUDIENCE_YOUNG_ADULT)

#         # The parent lane also includes books from the subgenres.
#         eq_(fantasy_and_subgenres, set(ya_fantasy_lane.genres))
#         eq_(Classifier.AUDIENCE_YOUNG_ADULT, ya_fantasy_lane.audience)

#         # Here's a 'YA Science Fiction' lane, which has no sublanes,
#         # and which excludes Dystopian SF and Steampunk (which have their
#         # own lanes on the same level as 'YA Science Fiction')
#         ya_sf = Lane(
#             self._db, full_name="YA Science Fiction",
#             display_name="Science Fiction",
#             genres=[classifier.Science_Fiction],
#             subgenre_behavior=Lane.IN_SAME_LANE,
#             exclude_genres=[
#                 classifier.Dystopian_SF, classifier.Steampunk],
#             audience=Classifier.AUDIENCE_YOUNG_ADULT)
#         eq_([], ya_sf.sublanes.lanes)
#         eq_("YA Science Fiction", ya_sf.name)
#         eq_("Science Fiction", ya_sf.display_name)
#         included_subgenres = [x.name for x in ya_sf.genres]
#         assert "Cyberpunk" in included_subgenres
#         assert "Dystopian SF" not in included_subgenres
#         assert "Steampunk" not in included_subgenres

#     def test_materialized_works(self):
#         from model import (
#             MaterializedWork,
#             MaterializedWorkWithGenre,
#         )

#         # Look up two genres.
#         fantasy, ig = Genre.lookup(self._db, classifier.Fantasy)
#         cooking, ig = Genre.lookup(self._db, classifier.Cooking)

#         # Here's a fantasy book.
#         w1 = self._work(genre=fantasy, with_license_pool=True)
#         w1.simple_opds_entry = "foo"

#         # Here's a cooking book.
#         w2 = self._work(genre=cooking, fiction=False, with_license_pool=True)
#         w2.simple_opds_entry = "bar"

#         # Here's a fantasy book for children ages 7-9.
#         w3 = self._work(genre=fantasy, fiction=True, with_license_pool=True,
#                         audience=Classifier.AUDIENCE_CHILDREN)
#         w3.target_age = NumericRange(7,9)
#         w3.simple_opds_entry = "baz"
        
#         # Refresh the materialized views so that all three books are
#         # included in the views.
#         SessionManager.refresh_materialized_views(self._db)

#         # Let's get materialized works from the Fantasy genre.
#         fantasy_lane = Lane(
#             self._db, full_name="Fantasy", genres=[classifier.Fantasy])
#         [materialized] = fantasy_lane.materialized_works(['eng']).all()

#         # This materialized work corresponds to the adult fantasy book. We
#         # did not get an entry for the cooking book or the children's book.
#         assert isinstance(materialized, MaterializedWorkWithGenre)
#         eq_(materialized.works_id, w1.id)

#         # Let's get materialized works of nonfiction.
#         nonfiction_lane = Lane(
#             self._db, full_name="Nonfiction", genres=[], fiction=False)
#         [materialized] = nonfiction_lane.materialized_works().all()

#         # This materialized work corresponds to the cooking book. We
#         # did not get an entry for the other books.
#         assert isinstance(materialized, MaterializedWork)
#         eq_(materialized.works_id, w2.id)

#         # Let's get materialized works suitable for children age 8.
#         age_8_lane = Lane(
#             self._db, full_name="Age 8", genres=[], audience='Children',
#             age_range=[8]
#         )
#         [materialized] = age_8_lane.materialized_works().all()
#         assert isinstance(materialized, MaterializedWork)
#         eq_(materialized.works_id, w3.id)

#         # We get the same book by asking for works suitable for
#         # children ages 7-10.
#         age_7_10_lane = Lane(
#             self._db, full_name="Ages 7-10", genres=[], audience='Children',
#             age_range=[7,10]
#         )
#         [materialized] = age_7_10_lane.materialized_works().all()
#         assert isinstance(materialized, MaterializedWork)
#         eq_(materialized.works_id, w3.id)

#         # Verify that the language restriction works.
#         eq_([], fantasy_lane.materialized_works(['fre']).all())

#     def test_availability_restriction(self):

#         fantasy, ig = Genre.lookup(self._db, classifier.Fantasy)

#         # Here's a fantasy book.
#         w1 = self._work(genre=fantasy, with_license_pool=True)

#         # The book is not available.
#         w1.license_pools[0].licenses_available = 0
#         w1.license_pools[0].open_access = False
#         self._db.commit()

#         fantasy_lane = Lane(
#             self._db, full_name="Fantasy", genres=[fantasy])

#         # So long as the hold behavior allows books to be put on hold,
#         # the book will show up in lanes.
#         with temp_config() as config:
#             config['policies'][Configuration.HOLD_POLICY] = Configuration.HOLD_POLICY_ALLOW
#             allow_on_hold_works = fantasy_lane.works(['eng']).all()
#             eq_(1, len(allow_on_hold_works))

#         # When the hold behavior is to hide unavailable books, the
#         # book disappears.
#         with temp_config() as config:
#             config['policies'][Configuration.HOLD_POLICY] = Configuration.HOLD_POLICY_HIDE
#             hide_on_hold_works = fantasy_lane.works(['eng']).all()
#             eq_([], hide_on_hold_works)

#             # When the book becomes available, it shows up in lanes again.
#             w1.license_pools[0].licenses_available = 1
#             hide_on_hold_works = fantasy_lane.works(['eng']).all()
#             eq_(1, len(hide_on_hold_works))
        

# class TestLaneList(DatabaseTest):
    
#     def test_from_description(self):
#         lanes = LaneList.from_description(
#             self._db,
#             None,
#             [dict(full_name="Fiction",
#                   fiction=True,
#                   audience=Classifier.AUDIENCE_ADULT,
#                   genres=[]),
#              classifier.Fantasy,
#              dict(
#                  full_name="Young Adult",
#                  fiction=Lane.BOTH_FICTION_AND_NONFICTION,
#                  audience=Classifier.AUDIENCE_YOUNG_ADULT,
#                  genres=[]),
#          ]
#         )

#         fantasy_genre, ignore = Genre.lookup(self._db, classifier.Fantasy.name)

#         fiction = lanes.by_name['Fiction']
#         young_adult = lanes.by_name['Young Adult']
#         fantasy = lanes.by_name['Fantasy'] 

#         eq_(set([fantasy, fiction, young_adult]), set(lanes.lanes))

#         eq_("Fiction", fiction.name)
#         eq_(Classifier.AUDIENCE_ADULT, fiction.audience)
#         eq_([], fiction.genres)
#         eq_(True, fiction.fiction)

#         eq_("Fantasy", fantasy.name)
#         eq_(Classifier.AUDIENCES_ADULT, fantasy.audience)
#         eq_([fantasy_genre], fantasy.genres)
#         eq_(Lane.FICTION_DEFAULT_FOR_GENRE, fantasy.fiction)

#         eq_("Young Adult", young_adult.name)
#         eq_(Classifier.AUDIENCE_YOUNG_ADULT, young_adult.audience)
#         eq_([], young_adult.genres)
#         eq_(Lane.BOTH_FICTION_AND_NONFICTION, young_adult.fiction)
