import json
from nose.tools import (
    eq_,
    set_trace,
    assert_raises,
)

from . import DatabaseTest

from lane import (
    Facets,
    Pagination,
    WorkList,
    Lane,
)

from model import (
    DataSource,
    Edition,
    Genre,
    Library,
    LicensePool,
    SessionManager,
    Work,
)

class TestFacets(DatabaseTest):

    def _configure_facets(self, library, enabled, default):
        """Set facet configuration for the given Library."""
        for key, values in enabled.items():
            library.enabled_facets_setting(key).value = json.dumps(values)
        for key, value in default.items():
            library.default_facet_setting(key).value = value
    
    def test_facet_groups(self):

        facets = Facets(
            self._default_library,
            Facets.COLLECTION_MAIN, Facets.AVAILABLE_ALL, Facets.ORDER_TITLE
        )
        all_groups = list(facets.facet_groups)

        # By default, there are a 9 facet transitions: three groups of three.
        eq_(9, len(all_groups))

        # available=all, collection=main, and order=title are the selected
        # facets.
        selected = sorted([x[:2] for x in all_groups if x[-1] == True])
        eq_(
            [('available', 'all'), ('collection', 'main'), ('order', 'title')],
            selected
        )

        test_enabled_facets = {
                Facets.ORDER_FACET_GROUP_NAME : [
                    Facets.ORDER_WORK_ID, Facets.ORDER_TITLE
                ],
                Facets.COLLECTION_FACET_GROUP_NAME : [Facets.COLLECTION_FULL],
                Facets.AVAILABILITY_FACET_GROUP_NAME : [Facets.AVAILABLE_ALL],
        }
        test_default_facets = {
            Facets.ORDER_FACET_GROUP_NAME : Facets.ORDER_TITLE,
            Facets.COLLECTION_FACET_GROUP_NAME : Facets.COLLECTION_FULL,
            Facets.AVAILABILITY_FACET_GROUP_NAME : Facets.AVAILABLE_ALL,
        }
        library = self._default_library
        self._configure_facets(
            library, test_enabled_facets, test_default_facets
        )
            
        facets = Facets(self._default_library,
                        None, None, Facets.ORDER_TITLE)
        all_groups = list(facets.facet_groups)
        # We have disabled almost all the facets, so the list of
        # facet transitions includes only two items.
        #
        # 'Sort by title' was selected, and it shows up as the selected
        # item in this facet group.
        expect = [['order', 'title', True], ['order', 'work_id', False]]
        eq_(expect, sorted([list(x[:2]) + [x[-1]] for x in all_groups]))

    def test_facets_can_be_enabled_at_initialization(self):
        enabled_facets = {
            Facets.ORDER_FACET_GROUP_NAME : [
                Facets.ORDER_TITLE, Facets.ORDER_AUTHOR,
            ],
            Facets.COLLECTION_FACET_GROUP_NAME : [Facets.COLLECTION_MAIN],
            Facets.AVAILABILITY_FACET_GROUP_NAME : [Facets.AVAILABLE_OPEN_ACCESS]
        }
        library = self._default_library
        self._configure_facets(library, enabled_facets, {})
        
        # Create a new Facets object with these facets enabled,
        # no matter the Configuration.
        facets = Facets(
            self._default_library,
            Facets.COLLECTION_MAIN, Facets.AVAILABLE_OPEN_ACCESS,
            Facets.ORDER_TITLE, enabled_facets=enabled_facets
        )
        all_groups = list(facets.facet_groups)
        expect = [['order', 'author', False], ['order', 'title', True]]
        eq_(expect, sorted([list(x[:2]) + [x[-1]] for x in all_groups]))

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

        # ...by most recently added...
        eq_([LicensePool.availability_time, mw.availability_time, mwg.availability_time],
            fields(Facets.ORDER_ADDED_TO_COLLECTION))

        # ...or randomly.
        eq_([Work.random, mw.random, mwg.random],
            fields(Facets.ORDER_RANDOM))

    def test_order_by(self):
        from model import (
            MaterializedWork as mw,
            MaterializedWorkWithGenre as mwg,
        )

        def order(facet, work, edition, ascending=None):
            f = Facets(
                self._default_library,
                collection=Facets.COLLECTION_FULL, 
                availability=Facets.AVAILABLE_ALL,
                order=facet,
                order_ascending=ascending,
            )
            return f.order_by(work, edition)[0]

        def compare(a, b):
            assert(len(a) == len(b))
            for i in range(0, len(a)):
                assert(a[i].compare(b[i]))

        expect = [Edition.sort_author.asc(), Edition.sort_title.asc(), Work.id.asc()]
        actual = order(Facets.ORDER_AUTHOR, Work, Edition, True)  
        compare(expect, actual)

        expect = [Edition.sort_author.desc(), Edition.sort_title.asc(), Work.id.asc()]
        actual = order(Facets.ORDER_AUTHOR, Work, Edition, False)  
        compare(expect, actual)

        expect = [mw.sort_title.asc(), mw.sort_author.asc(), mw.works_id.asc()]
        actual = order(Facets.ORDER_TITLE, mw, mw, True)
        compare(expect, actual)

        expect = [Work.last_update_time.asc(), Edition.sort_author.asc(), Edition.sort_title.asc(), Work.id.asc()]
        actual = order(Facets.ORDER_LAST_UPDATE, Work, Edition, True)
        compare(expect, actual)

        expect = [mw.random.asc(), mw.sort_author.asc(), mw.sort_title.asc(),
                  mw.works_id.asc()]
        actual = order(Facets.ORDER_RANDOM, mw, mw, True)
        compare(expect, actual)

        expect = [LicensePool.availability_time.desc(), Edition.sort_author.asc(), Edition.sort_title.asc(), Work.id.asc()]
        actual = order(Facets.ORDER_ADDED_TO_COLLECTION, Work, Edition, None)  
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
        licensed_high = self._work(presentation_edition=licensed_e1)
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
        licensed_low = self._work(presentation_edition=licensed_e2)
        licensed_low.license_pools.append(licensed_p2)
        licensed_low.quality = 0.2
        licensed_p2.licenses_owned = 1
        licensed_p2.licenses_available = 1
        licensed_low.random = 0.1

        qu = self._db.query(Work).join(Work.presentation_edition).join(
            Work.license_pools
        )
        def facetify(collection=Facets.COLLECTION_FULL, 
                     available=Facets.AVAILABLE_ALL,
                     order=Facets.ORDER_TITLE
        ):
            f = Facets(self._default_library, collection, available, order)
            return f.apply(self._db, qu)

        # When holds are allowed, we can find all works by asking
        # for everything.
        library = self._default_library
        library.setting(Library.ALLOW_HOLDS).value = "True"
        everything = facetify()
        eq_(4, everything.count())

        # If we disallow holds, we lose one book even when we ask for
        # everything.
        library.setting(Library.ALLOW_HOLDS).value = "False"
        everything = facetify()
        eq_(3, everything.count())
        assert licensed_high not in everything

        library.setting(Library.ALLOW_HOLDS).value = "True"
        # Even when holds are allowed, if we restrict to books
        # currently available we lose the unavailable book.
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
        # low-quality books.
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

class TestPagination(DatabaseTest):

    def test_has_next_page(self):
        query = self._db.query(Work)
        pagination = Pagination(size=2)

        # When the query is empty, pagination doesn't have a next page.
        pagination.apply(query)
        eq_(False, pagination.has_next_page)

        # When there are more results in the query, it does.
        for num in range(3):
            # Create three works.
            self._work()
        pagination.apply(query)
        eq_(True, pagination.has_next_page)

        # When we reach the end of results, there's no next page.
        pagination.offset = 1
        eq_(False, pagination.has_next_page)

        # When the database is updated, pagination knows.
        for num in range(3):
            self._work()
        pagination.apply(query)
        eq_(True, pagination.has_next_page)

        # Even when the query ends at the same size as a page, all is well.
        pagination.offset = 4
        eq_(False, pagination.has_next_page)


class MockFeaturedWorks(object):
    """A mock WorkList that mocks featured_works()."""

    def __init__(self):
        self._featured_works = []

    def queue_featured_works(self, works):
        """Set the next return value for featured_works()."""
        self._featured_works.append(works)

    def featured_works(self, *args, **kwargs):
        try:
            return self._featured_works.pop(0)
        except IndexError:
            return []

class MockWork(object):
    def __init__(self, id):
        self.id = id

class MockWorks(WorkList):
    """A WorkList that mocks works() but not featured_works()."""

    def __init__(self):
        self.reset()

    def reset(self):
        self._works = []
        self.works_calls = []

    def queue_works(self, works):
        """Set the next return value for works()."""
        self._works.append(works)

    def queue_featured_works(self, works):
        """Set the next return value for featured_works()."""
        self._featured_works.append(works)

    def works(self, _db, facets=None, pagination=None, featured=False):
        self.works_calls.append((facets, pagination, featured))
        try:
            return self._works.pop(0)
        except IndexError:
            return []

    def random_sample(self, query, target_size):
        # The 'query' is actually a list, and we're in a test
        # environment where randomness is not welcome. Just take
        # a sample from the front of the list.
        return query[:target_size]


class TestWorkList(DatabaseTest):

    def add_to_materialized_view(self, *works):
        """Make sure all the works show up in the materialized view.
        """
        for work in works:
            work.presentation_ready = True
            work.simple_opds_entry = "an entry"
        self._db.commit()
        SessionManager.refresh_materialized_views(self._db)

    def test_initialize(self):
        wl = WorkList()
        child = WorkList()
        sf, ignore = Genre.lookup(self._db, "Science Fiction")
        romance, ignore = Genre.lookup(self._db, "Romance")

        # Create a WorkList that's associated with a Library, two genres,
        # and a child WorkList.
        wl.initialize(self._default_library, children=[child],
                      genres=[sf, romance])

        # Access the Library.
        eq_(self._default_library, wl.library(self._db))

        # The Collections associated with the WorkList are those associated
        # with the Library.
        eq_(set(wl.collection_ids), 
            set([x.id for x in self._default_library.collections]))

        # The Genres associated with the WorkList are the ones passed
        # in on the constructor.
        eq_(set(wl.genre_ids),
            set([x.id for x in [sf, romance]]))

        # The WorkList's child is the WorkList passed in to the constructor.
        eq_([child], wl.visible_children)

    def test_audience_key(self):
        pass

    def test_groups(self):
        w1 = MockWork(1)
        w2 = MockWork(2)
        w3 = MockWork(3)

        # This WorkList has one featured work.
        child1 = MockFeaturedWorks()
        child1.queue_featured_works([w1])

        # This WorkList has two featured works.
        child2 = MockFeaturedWorks()
        child2.queue_featured_works([w2, w1])

        # This WorkList has two children -- the two WorkLists created
        # above.
        wl = WorkList()
        wl.initialize(self._default_library, children=[child1, child2])
        
        # Calling groups() on the parent WorkList returns three
        # 2-tuples; one for each work featured by one of its children
        # WorkLists. Note that the same work appears twice, through two
        # different children.
        [wwl1, wwl2, wwl3] = wl.groups(self._db)
        eq_((w1, child1), wwl1)
        eq_((w2, child2), wwl2)
        eq_((w1, child2), wwl3)

    def test_featured_works(self):
        wl = MockWorks()
        wl.initialize(library=self._default_library)

        # We're going to try to get 3 featured works.
        self._default_library.setting(Library.FEATURED_LANE_SIZE).value = 3

        # Here are four.
        w1 = MockWork(1)
        w2 = MockWork(2)
        w3 = MockWork(3)
        w4 = MockWork(4)

        # With a single work queued, we will call works() several
        # times -- once for every item in the
        # featured_collection_facets() generator -- but we will not be
        # able to get more than that one featured work.
        queue = wl.queue_works
        queue([w1])
        featured = wl.featured_works(self._db)
        eq_([w1], featured)

        # To verify that works() was called multiple times and that
        # the calls were driven by featured_collection_facets(),
        # compare the actual arguments passed into works() with what
        # featured_collection_facets() would dictate.
        actual_facets = [
            (facets.collection, facets.availability, featured)
            for [facets, pagination, featured] in wl.works_calls
        ]
        expect_facets = list(MockWorks.featured_collection_facets())
        eq_(actual_facets, expect_facets)

        # Here, we will get three sets of results before we have enough works.
        wl.reset()
        queue([w1, w1, w3])
        # Putting w2 at the end of the second set of results simulates
        # a situation where query results include a work that has not
        # been chosen, but the random sample chooses a bunch of works
        # that _have_ already been chosen instead. If the random
        # sample had turned out differently, we would have had
        # slightly better results and saved some time.
        queue([w3, w1, w1, w1, w2])
        queue([w4])
        featured = wl.featured_works(self._db)

        # Works are presented in the order they were received, to put
        # higher-quality works at the front. Duplicates are ignored.
        eq_([w1.id, w3.id, w4.id], [x.id for x in featured])

        # We only had to make three calls to works() before filling
        # our quota.
        eq_(3, len(wl.works_calls))

        # Here, we only have to try once.
        wl.reset()
        queue([w2, w3, w4, w1])
        featured = wl.featured_works(self._db)
        eq_([w2.id, w3.id, w4.id], [x.id for x in featured])        
        eq_(1, len(wl.works_calls))

        # Here, the WorkList thinks that calling works() is a bad idea,
        # and persistently returns None.
        wl.reset()
        for i in range(len(expect_facets)):
            queue(None)

        # featured_works() doesn't crash, but it doesn't return
        # any values either.
        eq_([], wl.featured_works(self._db))

        # And it keeps calling works() for every facet, rather than
        # giving up after the first None.
        eq_(len(expect_facets), len(wl.works_calls))

    def test_featured_collection_facets(self):
        """Test the specific values expected from the default
        featured_collection_facets() implementation.

        This encodes our belief about what aspects of a book make it
        "featurable". We like works that have high .quality scores
        and can be loaned out immediately.
        """
        expect = [(Facets.COLLECTION_FEATURED, Facets.AVAILABLE_NOW, False),
         (Facets.COLLECTION_FEATURED, Facets.AVAILABLE_ALL, False),
         (Facets.COLLECTION_MAIN, Facets.AVAILABLE_NOW, False),
         (Facets.COLLECTION_MAIN, Facets.AVAILABLE_ALL, False),
         (Facets.COLLECTION_FULL, Facets.AVAILABLE_ALL, False)
        ]
        actual = list(WorkList.featured_collection_facets())
        eq_(expect, actual)

    def test_works(self):
        """Verify that WorkList.works() correctly locates works
        that match the criteria specified by apply_filters().
        """

        # Create two books and add them to the materialized view.
        oliver_twist = self._work(title='Oliver Twist', with_license_pool=True)
        not_oliver_twist = self._work(
            title='Barnaby Rudge', with_license_pool=True
        )
        self.add_to_materialized_view(oliver_twist, not_oliver_twist)

        class OnlyOliverTwist(WorkList):
            """Mock WorkList that overrides apply_filters() so that it
            only finds copies of 'Oliver Twist'.
            """

            def apply_filters(self, _db, qu, work_model, *args, **kwargs):
                return qu.filter(work_model.sort_title=='Oliver Twist')

        # A normal WorkList will use the default apply_filters()
        # implementation and find both books.
        wl = WorkList()
        wl.initialize(self._default_library)
        eq_(2, wl.works(self._db).count())

        # But the mock WorkList will only find Oliver Twist.
        wl = OnlyOliverTwist()
        wl.initialize(self._default_library)
        eq_([oliver_twist.id], [x.works_id for x in wl.works(self._db)])

        # A WorkList will only find books licensed through one of its
        # collections.
        library2 = self._library()
        collection = self._collection()
        libary2.collections = [collection]
        library_2_worklist = WorkList()
        library_2_worklist.initialize(library2)
        eq_(0, library_2_worklist.works(self._db).count())


    def test_works_for_specific_ids(self):
        # Create two works and put them in the materialized view.
        w1 = self._work(with_license_pool=True)
        w2 = self._work(with_license_pool=True)
        self.add_to_materialized_view(w1, w2)
        wl = WorkList()
        wl.initialize(self._default_library)

        # We asked for w2 only, and we got (the materialized view's
        # version of) w2 only.
        [w2_mv] = wl.works_for_specific_ids(self._db, [w2.id])
        eq_(w2_mv.sort_title, w2.sort_title)

        # Works are returned in the order we ask for.
        for ordering in ([w1, w2], [w2, w1]):            
            ids = [x.id for x in ordering]
            mv_works = wl.works_for_specific_ids(self._db, ids)
            eq_(ids, [x.works_id for x in mv_works])

        # If we ask for a work ID that's not in the materialized view,
        # we don't get it.
        eq_([], wl.works_for_specific_ids(self._db, [-100]))

        # If we ask for a work that's not deliverable, we don't get it.
        for lpdm in w2.license_pools[0].delivery_mechanisms:
            self._db.delete(lpdm)
        eq_([], wl.works_for_specific_ids(self._db, [w2.id]))
