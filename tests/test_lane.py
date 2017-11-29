import datetime
import json
import random
from nose.tools import (
    eq_,
    set_trace,
    assert_raises,
    assert_raises_regexp,
)

from . import DatabaseTest

from classifier import Classifier

from external_search import (
    DummyExternalSearchIndex,
)

from lane import (
    Facets,
    FeaturedFacets,
    Pagination,
    WorkList,
    Lane,
)

from model import (
    tuple_to_numericrange,
    DataSource,
    Edition,
    Genre,
    Identifier,
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
                Facets.order_facet_to_database_field(facet, w)
                for w in (mw, mwg)
            ]

        # You can sort by title...
        eq_([mw.sort_title, mwg.sort_title],
            fields(Facets.ORDER_TITLE))

        # ...by author...
        eq_([mw.sort_author, mwg.sort_author],
            fields(Facets.ORDER_AUTHOR))

        # ...by work ID...
        eq_([mw.works_id, mwg.works_id],
            fields(Facets.ORDER_WORK_ID))

        # ...by last update time...
        eq_([mw.last_update_time, mwg.last_update_time],
            fields(Facets.ORDER_LAST_UPDATE))

        # ...by most recently added...
        eq_([mw.availability_time, mwg.availability_time],
            fields(Facets.ORDER_ADDED_TO_COLLECTION))

        # ...or randomly.
        eq_([mw.random, mwg.random],
            fields(Facets.ORDER_RANDOM))

    def test_order_by(self):
        from model import (
            MaterializedWork as mw,
            MaterializedWorkWithGenre as mwg,
        )

        def order(facet, work, ascending=None):
            f = Facets(
                self._default_library,
                collection=Facets.COLLECTION_FULL, 
                availability=Facets.AVAILABLE_ALL,
                order=facet,
                order_ascending=ascending,
            )
            return f.order_by(work)[0]

        def compare(a, b):
            assert(len(a) == len(b))
            for i in range(0, len(a)):
                assert(a[i].compare(b[i]))

        for m in mw, mwg:
            expect = [m.sort_author.asc(), m.sort_title.asc(), m.works_id.asc()]
            actual = order(Facets.ORDER_AUTHOR, m, True)  
            compare(expect, actual)

            expect = [m.sort_author.desc(), m.sort_title.asc(), m.works_id.asc()]
            actual = order(Facets.ORDER_AUTHOR, m, False)  
            compare(expect, actual)

            expect = [m.sort_title.asc(), m.sort_author.asc(), m.works_id.asc()]
            actual = order(Facets.ORDER_TITLE, m, True)
            compare(expect, actual)

            expect = [m.last_update_time.asc(), m.sort_author.asc(), m.sort_title.asc(), m.works_id.asc()]
            actual = order(Facets.ORDER_LAST_UPDATE, m, True)
            compare(expect, actual)

            expect = [m.random.asc(), m.sort_author.asc(), m.sort_title.asc(),
                      m.works_id.asc()]
            actual = order(Facets.ORDER_RANDOM, m, True)
            compare(expect, actual)

            expect = [m.availability_time.desc(), m.sort_author.asc(), m.sort_title.asc(), m.works_id.asc()]
            actual = order(Facets.ORDER_ADDED_TO_COLLECTION, m, None)  
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
        
        self.add_to_materialized_view([open_access_high, open_access_low,
                                       licensed_high, licensed_low])

        from model import MaterializedWork as mw
        qu = self._db.query(mw).join(
            LicensePool, mw.license_pool_id==LicensePool.id
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
        eq_([open_access_high.id, open_access_low.id, licensed_high.id, 
             licensed_low.id],
            [x.works_id for x in title_order])

        random_order = facetify(order=Facets.ORDER_RANDOM)
        eq_([licensed_low.id, open_access_high.id, licensed_high.id, 
             open_access_low.id],
            [x.works_id for x in random_order])


class TestFeaturedFacets(DatabaseTest):

    def test_quality_calculation(self):
        minimum_featured_quality = 0.6

        featurable = self._work(title="Featurable", with_license_pool=True)
        featurable.quality = minimum_featured_quality

        featurable_but_not_available = self._work(
            title="Featurable but not available",
            with_license_pool=True
        )
        featurable_but_not_available.quality = minimum_featured_quality
        featurable_but_not_available.license_pools[0].licenses_available = 0

        awful_but_licensed = self._work(
            title="Awful but licensed",
            with_license_pool=True
        )
        awful_but_licensed.quality = 0

        decent_open_access = self._work(
            title="Decent open access", with_license_pool=True,
            with_open_access_download=True
        )
        decent_open_access.quality = 0.3
    
        awful_open_access = self._work(
            title="Awful open access", with_license_pool=True,
            with_open_access_download=True
        )
        awful_open_access.quality = 0

        awful_but_featured_on_a_list = self._work(
            title="Awful but featured on a list", with_license_pool=True,
            with_open_access_download=True
        )
        awful_but_featured_on_a_list.license_pools[0].licenses_available = 0
        awful_but_featured_on_a_list.quality = 0

        facets = FeaturedFacets(minimum_featured_quality, True)
        quality_field = facets.quality_tier_field(Work).label("tier")
        self._db.commit()

        qu = self._db.query(Work, quality_field).join(
            Work.license_pools).outerjoin(Work.custom_list_entries)
        set_trace()


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
        self.visible = True
        self.priority = 0
        self.display_name = "name"

    def queue_featured_works(self, works):
        """Set the next return value for featured_works()."""
        self._featured_works.append(works)

    def featured_works(self, *args, **kwargs):
        try:
            return self._featured_works.pop(0)
        except IndexError:
            return []

class MockWork(object):
    """Acts as a Work or a MaterializedWork interchangeably."""
    def __init__(self, id):
        self.id = id
        self.works_id = id

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

    def test_initialize(self):
        wl = WorkList()
        child = WorkList()
        child.initialize(self._default_library)
        sf, ignore = Genre.lookup(self._db, "Science Fiction")
        romance, ignore = Genre.lookup(self._db, "Romance")

        # Create a WorkList that's associated with a Library, two genres,
        # and a child WorkList.
        wl.initialize(self._default_library, children=[child],
                      genres=[sf, romance])

        # Access the Library.
        eq_(self._default_library, wl.get_library(self._db))

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
        wl = WorkList()
        wl.initialize(library=self._default_library)

        # No audience.
        eq_(u'', wl.audience_key)

        # All audiences.
        wl.audiences = Classifier.AUDIENCES
        eq_(u'', wl.audience_key)

        # Specific audiences.
        wl.audiences = [Classifier.AUDIENCE_CHILDREN, 
                        Classifier.AUDIENCE_YOUNG_ADULT]
        eq_(u'Children,Young+Adult', wl.audience_key)

    def test_visible_children(self):
        """Invisible children don't show up in WorkList.visible_children."""
        wl = WorkList()
        visible = self._lane()
        invisible = self._lane()
        invisible.visible = False
        child_wl = WorkList()
        child_wl.initialize(self._default_library)
        wl.initialize(
            self._default_library, children=[visible, invisible, child_wl]
        )
        eq_(set([child_wl, visible]), set(wl.visible_children))

    def test_visible_children_sorted(self):
        """Visible children are sorted by priority and then by display name."""
        wl = WorkList()

        lane_child = self._lane()
        lane_child.display_name='ZZ'
        lane_child.priority = 0

        wl_child = WorkList()
        wl_child.priority = 1
        wl_child.display_name='AA'

        wl.initialize(
            self._default_library, children=[lane_child, wl_child]
        )

        # lane_child has a higher priority so it shows up first even
        # though its display name starts with a Z.
        eq_([lane_child, wl_child], wl.visible_children)

        # If the priorities are the same, wl_child shows up first,
        # because its display name starts with an A.
        wl_child.priority = 0
        eq_([wl_child, lane_child], wl.visible_children)


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

        # Here, we will get three sets of results before we have enough works.
        wl.reset()
        queue([w1, w3, w4, w2])
        featured = wl.featured_works(self._db)

        # Works are presented in the order they were received, to put
        # higher-quality works at the front. Duplicates are ignored.
        eq_([w1.id, w3.id, w4.id], [x.id for x in featured])

        # In a previous version, featured_works had to call works()
        # multiple times to meet its quota. Now it only has to call
        # works() once.
        eq_(1, len(wl.works_calls))

        # Here, the WorkList thinks that calling works() is a bad
        # idea, and returns None.
        wl.reset()

        # featured_works() doesn't crash, but it doesn't return
        # any values either.
        eq_([], wl.featured_works(self._db))

        # Still only one call.
        eq_(1, len(wl.works_calls))

    def test_works(self):
        """Verify that WorkList.works() correctly locates works
        that match the criteria specified by apply_filters().
        """

        # Create two books and add them to the materialized view.
        oliver_twist = self._work(title='Oliver Twist', with_license_pool=True)
        not_oliver_twist = self._work(
            title='Barnaby Rudge', with_license_pool=True
        )
        self.add_to_materialized_view([oliver_twist, not_oliver_twist])

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
        library2.collections = [collection]
        library_2_worklist = WorkList()
        library_2_worklist.initialize(library2)
        eq_(0, library_2_worklist.works(self._db).count())

        # If a WorkList has no collections, it has no books.
        self._default_library.collections = []
        wl.initialize(self._default_library)
        eq_(0, wl.works(self._db).count())

    def test_works_for_specific_ids(self):
        # Create two works and put them in the materialized view.
        w1 = self._work(with_license_pool=True)
        w2 = self._work(with_license_pool=True)
        self.add_to_materialized_view([w1, w2])
        wl = WorkList()
        wl.initialize(self._default_library)

        # Now we're going to ask for a WorkList that contains specific
        # Works, such as those returned from a search request.

        # If we ask for w2 only, we get (the materialized view's
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

    def test_apply_filters(self):

        called = dict()

        class MockWorkList(WorkList):
            """Mock WorkList that simply verifies that apply_filters()
            calls various hook methods.
            """

            def __init__(self, distinct=True):
                self.distinct = distinct
            
            def only_show_ready_deliverable_works(
                    self, _db, query, *args, **kwargs
            ):
                called['only_show_ready_deliverable_works'] = True
                return query

            def apply_bibliographic_filters(
                    self, _db, query, work_model, featured
            ):
                called['apply_bibliographic_filters'] = True
                called['apply_bibliographic_filters.featured'] = featured
                return query, self.distinct

        class MockFacets(object):
            def apply(self, _db, query, work_model, distinct):
                called['facets.apply'] = True
                called['facets.apply.distinct'] = distinct
                return query

        class MockPagination(object):
            def apply(self, query):
                called['pagination.apply'] = True
                return query

        from model import MaterializedWork
        original_qu = self._db.query(MaterializedWork)
        wl = MockWorkList()
        final_qu = wl.apply_filters(
            self._db, original_qu, MaterializedWork, MockFacets(), 
            MockPagination()
        )
        
        # The hook methods were called with the right arguments.
        eq_(called['only_show_ready_deliverable_works'], True)
        eq_(called['apply_bibliographic_filters'], True)
        eq_(called['facets.apply'], True)
        eq_(called['pagination.apply'], True)

        eq_(called['apply_bibliographic_filters.featured'], False)
        eq_(called['facets.apply.distinct'], True)

        # We mocked everything that might have changed the final query,
        # and the end result was the query wasn't modified.
        eq_(original_qu, final_qu)

        # Test that apply_filters() makes a query distinct if there is
        # no Facets object to do the job.
        called = dict()
        distinct_qu = wl.apply_filters(
            self._db, original_qu, MaterializedWork, None, None
        )
        eq_(str(original_qu.distinct()), str(distinct_qu))
        assert 'facets.apply' not in called
        assert 'pagination.apply' not in called


    def test_apply_bibliographic_filters_short_circuits_apply_filters(self):
        class MockWorkList(WorkList):
            """Mock WorkList whose apply_bibliographic_filters implementation
            believes the WorkList should not exist at all.
            """

            def apply_bibliographic_filters(
                    self, _db, query, work_model, featured
            ):
                return None, False

        wl = MockWorkList()
        wl.initialize(self._default_library)
        from model import MaterializedWork
        qu = self._db.query(MaterializedWork)
        eq_(None, wl.apply_filters(self._db, qu, MaterializedWork, None, None))

    def test_apply_bibliographic_filters(self):
        called = dict()

        class MockWorkList(WorkList):
            """Mock WorkList that simply verifies that apply_filters()
            calls various hook methods.
            """

            def __init__(self, languages=None, genre_ids=None, media=None):
                self.languages = languages
                self.genre_ids = genre_ids
                self.media = media

            def apply_audience_filter(self, _db, qu, work_model):
                called['apply_audience_filter'] = True
                return qu

            def apply_custom_filters(self, _db, qu, work_model, featured):
                called['apply_custom_filters'] = True
                called['apply_custom_filters.featured'] = featured
                return qu, featured

        wl = MockWorkList()
        from model import MaterializedWorkWithGenre as wg
        original_qu = self._db.query(wg)

        # If no languages or genre IDs are specified, and the hook
        # methods do nothing, then apply_bibliographic_filters() has
        # no effect.
        featured_object = object()
        final_qu, distinct = wl.apply_bibliographic_filters(
            self._db, original_qu, wg, featured_object
        )
        eq_(original_qu, final_qu)

        # But the hook methods were called with the correct arguments.
        eq_(True, called['apply_audience_filter'])

        # If languages, media, and genre IDs are specified, then they are
        # incorporated into the query.
        english_sf = self._work(language="eng", with_license_pool=True)
        english_sf.presentation_edition.medium = Edition.BOOK_MEDIUM
        sf, ignore = Genre.lookup(self._db, "Science Fiction")
        english_sf.genres.append(sf)
        self.add_to_materialized_view(english_sf)

        # Create a WorkList that will find the MaterializedWorkWithGenre
        # for the English SF book.
        english_sf_list = MockWorkList(languages=["eng"], genre_ids=[sf.id], media=[Edition.BOOK_MEDIUM])
        english_sf_qu, distinct = english_sf_list.apply_bibliographic_filters(
            self._db, original_qu, wg, False
        )

        # Here it is!
        eq_([english_sf.sort_title], [x.sort_title for x in english_sf_qu])

        # WorkLists that do not match by language, medium, or genre will not
        # find the English SF book.
        spanish_sf_list = MockWorkList(languages=["spa"], genre_ids=[sf.id])
        spanish_sf_qu, distinct = spanish_sf_list.apply_bibliographic_filters(
            self._db, original_qu, wg, False
        )
        eq_(0, spanish_sf_qu.count())
        
        romance, ignore = Genre.lookup(self._db, "Romance")
        english_romance_list = MockWorkList(
            languages=["eng"], genre_ids=[romance.id]
        )
        english_romance_qu, distinct = english_romance_list.apply_bibliographic_filters(
            self._db, original_qu, wg, False
        )
        eq_(0, english_romance_qu.count())

        audio_list = MockWorkList(
            languages=["eng"], genre_ids=[sf.id], media=[Edition.AUDIO_MEDIUM])
        audio_qu, distinct = audio_list.apply_bibliographic_filters(
            self._db, original_qu, wg, False
        )
        eq_(0, audio_qu.count())

    def test_apply_audience_filter(self):

        # Create two childrens' books (one from Gutenberg, one not)
        # and one book for adults.

        gutenberg_children = self._work(
            title="Beloved Treasury of Racist Nursery Rhymes",
            with_license_pool=True,
            with_open_access_download=True,
        )
        eq_(DataSource.GUTENBERG, 
            gutenberg_children.license_pools[0].data_source.name)

        # _work() will not create a test Gutenberg book for children
        # to avoid exactly the problem we're trying to test, so
        # we need to set it manually.
        gutenberg_children.audience=Classifier.AUDIENCE_CHILDREN

        gutenberg_adult = self._work(
            title="Diseases of the Horse",
            with_license_pool=True, with_open_access_download=True,
            audience=Classifier.AUDIENCE_ADULT
        )

        edition, lp = self._edition(
            title="Wholesome Nursery Rhymes For All Children",
            data_source_name=DataSource.OVERDRIVE,
            with_license_pool=True
        )
        non_gutenberg_children = self._work(
            presentation_edition=edition, audience=Classifier.AUDIENCE_CHILDREN
        )

        def for_audiences(*audiences):
            """Invoke WorkList.apply_audience_filter using the given 
            `audiences`, and return all the matching Work objects.
            """
            wl = WorkList()
            wl.audiences = audiences
            qu = self._db.query(Work).join(Work.license_pools)
            return wl.apply_audience_filter(self._db, qu, Work).all()

        eq_([gutenberg_adult], 
            for_audiences(Classifier.AUDIENCE_ADULT))

        # The Gutenberg "children's" book is filtered out because it we have
        # no guarantee it is actually suitable for children.
        eq_([non_gutenberg_children], 
            for_audiences(Classifier.AUDIENCE_CHILDREN))

        # This can sometimes lead to unexpected results, but the whole
        # thing is a hack and needs to be improved anyway.
        eq_([non_gutenberg_children], 
            for_audiences(Classifier.AUDIENCE_ADULT, 
                          Classifier.AUDIENCE_CHILDREN))

    def test_random_sample(self):
        # This lets me test which items are chosen in a random sample,
        # but for some reason the shuffled lists still come out in an
        # unpredictable order.
        random.seed(42)

        # It doesn't matter what type of model object the query
        # returns, so query something that's faster to create than
        # Works.
        i1 = self._identifier()
        i2 = self._identifier()
        i3 = self._identifier()
        i4 = self._identifier()
        i5 = self._identifier()
        qu = self._db.query(Identifier)

        # If the random sample is smaller than the population, a
        # randomly located slice is chosen, and the slice is
        # shuffled. (It's presumed that the query sorts items by some
        # randomly generated number such as Work.random, so that choosing
        # a slice gets you a random sample -- that's not the case here.)
        sample = WorkList.random_sample(qu, 2, quality_coefficient=1)
        eq_([i3, i4], sorted(sample, key=lambda x: x.id))

        # If the random sample is larger than the sample population,
        # the population is shuffled.
        sample = WorkList.random_sample(qu, 6)
        eq_([i1, i2, i3, i4, i5], sorted(sample, key=lambda x: x.id))

    def test_search_target(self):
        # A WorkList can be searched - it is its own search target.
        wl = WorkList()
        eq_(wl, wl.search_target)

    def test_search(self):
        work = self._work(with_license_pool=True)
        self.add_to_materialized_view(work)

        # Create a WorkList that has very specific requirements.
        wl = WorkList()
        sf, ignore = Genre.lookup(self._db, "Science Fiction")
        wl.initialize(
            self._default_library, "Work List",
            genres=[sf], audiences=[Classifier.AUDIENCE_CHILDREN],
            languages=["eng", "spa"], media=[Edition.BOOK_MEDIUM],
        )
        wl.fiction = True
        wl.target_age = tuple_to_numericrange((2,2))
        search_client = DummyExternalSearchIndex()
        search_client.bulk_update([work])

        # Do a search within the list.
        pagination = Pagination(offset=0, size=1)
        results = wl.search(
            self._db, work.title, search_client, pagination
        )

        # The List configuration was passed on to the search client
        # as parameters to use when creating the search query.
        [query] = search_client.queries
        [fixed, kw] = query
        eq_((), fixed)
        eq_(wl.fiction, kw['fiction'])
        eq_((2,2), kw['target_age'])
        eq_(wl.languages, kw['languages'])
        eq_(wl.media, kw['media'])
        eq_(wl.audiences, kw['audiences'])
        eq_(wl.genre_ids, kw['in_any_of_these_genres'])
        eq_(1, kw['size'])
        eq_(0, kw['offset'])
        
        # The single search result was converted to a MaterializedWork.
        [result] = results
        from model import MaterializedWork
        assert isinstance(result, MaterializedWork)
        eq_(work.id, result.works_id)


class TestLane(DatabaseTest):

    def test_get_library(self):
        lane = self._lane()
        eq_(self._default_library, lane.get_library(self._db))

    def test_visibility(self):
        parent = self._lane()
        visible_child = self._lane(parent=parent)
        invisible_child = self._lane(parent=parent)
        invisible_child.visible = False
        eq_([visible_child], list(parent.visible_children))

        grandchild = self._lane(parent=invisible_child)
        eq_(True, parent.visible)
        eq_(True, visible_child.visible)
        eq_(False, invisible_child.visible)

        # The grandchild lane is set to visible in the database, but
        # it is not visible because its parent is not visible.
        eq_(True, grandchild._visible)
        eq_(False, grandchild.visible)

    def test_parentage(self):
        worklist = WorkList()
        worklist.display_name = "A WorkList"
        lane = self._lane()
        child_lane = self._lane(parent=lane)
        unrelated = self._lane()
        worklist.sublanes = [child_lane]

        # A WorkList has no parentage.
        eq_([], list(worklist.parentage))
        eq_("A WorkList", worklist.full_identifier)

        # The WorkList has the Lane as a child, but the Lane doesn't know
        # this.
        eq_([], list(lane.parentage))
        eq_([lane], list(child_lane.parentage))
        eq_(lane.display_name, lane.full_identifier)

        eq_("%s / %s" % (lane.display_name, child_lane.display_name), 
            child_lane.full_identifier)

        # TODO: The error should be raised when we try to set the parent
        # to an illegal value, not afterwards.
        lane.parent = child_lane
        assert_raises_regexp(
            ValueError, "Lane parentage loop detected", list, lane.parentage
        )

    def test_depth(self):
        child = self._lane("sublane")
        parent = self._lane("parent")
        parent.sublanes.append(child)
        eq_(0, parent.depth)
        eq_(1, child.depth)

    def test_url_name(self):
        lane = self._lane("Fantasy / Science Fiction")
        eq_(lane.id, lane.url_name)

    def test_display_name_for_all(self):
        lane = self._lane("Fantasy / Science Fiction")
        eq_("All Fantasy / Science Fiction", lane.display_name_for_all)

    def test_setting_target_age_locks_audiences(self):
        lane = self._lane()
        lane.target_age = (16, 18)
        eq_(
            sorted([Classifier.AUDIENCE_YOUNG_ADULT, Classifier.AUDIENCE_ADULT]),
            sorted(lane.audiences)
        )
        lane.target_age = (0, 2)
        eq_([Classifier.AUDIENCE_CHILDREN], lane.audiences)
        lane.target_age = 14
        eq_([Classifier.AUDIENCE_YOUNG_ADULT], lane.audiences)

        # It's not possible to modify .audiences to a value that's
        # incompatible with .target_age.
        lane.audiences = lane.audiences
        def doomed():
            lane.audiences = [Classifier.AUDIENCE_CHILDREN]
        assert_raises_regexp(
            ValueError, 
            "Cannot modify Lane.audiences when Lane.target_age is set", doomed
        ) 

        # Setting target_age to None leaves preexisting .audiences in place.
        lane.target_age = None
        eq_([Classifier.AUDIENCE_YOUNG_ADULT], lane.audiences)

        # But now you can modify .audiences.
        lane.audiences = [Classifier.AUDIENCE_CHILDREN]

    def test_target_age_treats_all_adults_equally(self):
        """We don't distinguish between different age groups for adults.
        """
        lane = self._lane()
        lane.target_age = (35,40)
        eq_(tuple_to_numericrange((18, 18)), lane.target_age)

    def test_uses_customlists(self):
        lane = self._lane()
        eq_(False, lane.uses_customlists)

        customlist, ignore = self._customlist(num_entries=0)
        lane.customlists = [customlist]
        eq_(True, lane.uses_customlists)

        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        lane.list_datasource = gutenberg
        self._db.commit()
        eq_(True, lane.uses_customlists)

        # Note that the specific custom list was removed from this
        # Lane when it switched to using all lists from a certain data
        # source.
        eq_([], lane.customlists)

        # A Lane may use custom lists by virtue of inheriting
        # restrictions from its parent.
        child = self._lane(parent=lane)
        child.inherit_parent_restrictions = True
        eq_(True, child.uses_customlists)        

    def test_genre_ids(self):
        # By default, when you add a genre to a lane, you are saying
        # that Works classified under it and all its subgenres should 
        # show up in the lane.
        fantasy = self._lane()
        fantasy.add_genre("Fantasy")

        # At this point the lane picks up Fantasy and all of its
        # subgenres.
        expect = [
            Genre.lookup(self._db, genre)[0].id for genre in [
                "Fantasy", "Epic Fantasy","Historical Fantasy", 
                "Urban Fantasy"
            ]
        ]
        eq_(set(expect), fantasy.genre_ids)

        # Let's exclude one of the subgenres.
        fantasy.add_genre("Urban Fantasy", inclusive=False)
        urban_fantasy, ignore = Genre.lookup(self._db, "Urban Fantasy")
        # That genre's ID has disappeared from .genre_ids.
        assert urban_fantasy.id not in fantasy.genre_ids

        # Let's add Science Fiction, but not its subgenres.
        fantasy.add_genre("Science Fiction", recursive=False)
        science_fiction, ignore = Genre.lookup(self._db, "Science Fiction")
        space_opera, ignore = Genre.lookup(self._db, "Space Opera")
        assert science_fiction.id in fantasy.genre_ids
        assert space_opera.id not in fantasy.genre_ids

        # Let's add Space Opera, but exclude Science Fiction and its
        # subgenres.
        fantasy.lane_genres = []
        fantasy.add_genre("Space Opera")
        fantasy.add_genre("Science Fiction", inclusive=False, recursive=True)
        
        # That eliminates everything.
        eq_(set([]), fantasy.genre_ids)

        # NOTE: We don't have any doubly nested subgenres, so we can't
        # test the case where a genre is included recursively but one
        # of its subgenres is exclused recursively (in which case the
        # sub-subgenre would be excluded), but it should work.

        # We can exclude a genre even when no genres are explicitly included.
        # The lane will include all genres that aren't excluded.
        no_inclusive_genres = self._lane()
        no_inclusive_genres.add_genre("Science Fiction", inclusive=False)
        assert len(no_inclusive_genres.genre_ids) > 10
        assert science_fiction.id not in no_inclusive_genres.genre_ids

    def test_groups(self):
        w1 = MockWork(1)
        w2 = MockWork(2)
        w3 = MockWork(3)

        parent = self._lane()
        def mock_parent_featured_works(_db):
            return [w1, w2]
        parent.featured_works = mock_parent_featured_works

        child = self._lane()
        parent.sublanes = [child]
        def mock_child_featured_works(_db):
            return [w2]
        child.featured_works = mock_child_featured_works

        # Calling groups() on the parent Lane returns three
        # 2-tuples; one for a work featured in the sublane,
        # and then two for a work featured in the parent lane.
        [wwl1, wwl2, wwl3] = parent.groups(self._db)
        eq_((w2, child), wwl1)
        eq_((w1, parent), wwl2)
        eq_((w2, parent), wwl3)

        # If a lane's sublanes don't contribute any books, then
        # groups() returns an entirely empty list, indicating that no
        # groups feed should be displayed.
        def mock_child_featured_works(_db):
            return []
        child.featured_works = mock_child_featured_works
        eq_([], parent.groups(self._db))

    def test_search_target(self):

        # A Lane that is the root for a patron type can be
        # searched.
        root_lane = self._lane()
        root_lane.root_for_patron_type = ["A"]
        eq_(root_lane, root_lane.search_target)

        # A Lane that's the descendant of a root Lane for a
        # patron type will search that root Lane.
        child = self._lane(parent=root_lane)
        eq_(root_lane, child.search_target)

        grandchild = self._lane(parent=child)
        eq_(root_lane, grandchild.search_target)

        # Any Lane that does not descend from a root Lane will
        # get a WorkList as its search target, with some
        # restrictions from the Lane.
        lane = self._lane()

        lane.languages = ["eng", "ger"]
        target = lane.search_target
        eq_("English/Deutsch", target.display_name)
        eq_(["eng", "ger"], target.languages)
        eq_(None, target.audiences)
        eq_(None, target.media)

        # If there are too many languages, they're left out of the
        # display name (so the search description will be "Search").
        lane.languages = ["eng", "ger", "spa", "fre"]
        target = lane.search_target
        eq_("", target.display_name)
        eq_(["eng", "ger", "spa", "fre"], target.languages)
        eq_(None, target.audiences)
        eq_(None, target.media)

        lane.languages = ["eng"]
        target = lane.search_target
        eq_("English", target.display_name)
        eq_(["eng"], target.languages)
        eq_(None, target.audiences)
        eq_(None, target.media)

        target = lane.search_target
        eq_("English", target.display_name)
        eq_(["eng"], target.languages)
        eq_(None, target.audiences)
        eq_(None, target.media)

        # Media aren't included in the description, but they
        # are used in search.
        lane.media = [Edition.BOOK_MEDIUM]
        target = lane.search_target
        eq_("English", target.display_name)
        eq_(["eng"], target.languages)
        eq_(None, target.audiences)
        eq_([Edition.BOOK_MEDIUM], target.media)

        # Audiences are only used in search if one of the
        # audiences is young adult or children.
        lane.audiences = [Classifier.AUDIENCE_ADULTS_ONLY]
        target = lane.search_target
        eq_("English", target.display_name)
        eq_(["eng"], target.languages)
        eq_(None, target.audiences)
        eq_([Edition.BOOK_MEDIUM], target.media)

        lane.audiences = [Classifier.AUDIENCE_ADULT, Classifier.AUDIENCE_YOUNG_ADULT]
        target = lane.search_target
        eq_("English Adult and Young Adult", target.display_name)
        eq_(["eng"], target.languages)
        eq_([Classifier.AUDIENCE_ADULT, Classifier.AUDIENCE_YOUNG_ADULT], target.audiences)
        eq_([Edition.BOOK_MEDIUM], target.media)

        # If there are too many audiences, they're left
        # out of the display name.
        lane.audiences = [Classifier.AUDIENCE_ADULT, Classifier.AUDIENCE_YOUNG_ADULT, Classifier.AUDIENCE_CHILDREN]
        target = lane.search_target
        eq_("English", target.display_name)
        eq_(["eng"], target.languages)
        eq_([Classifier.AUDIENCE_ADULT, Classifier.AUDIENCE_YOUNG_ADULT, Classifier.AUDIENCE_CHILDREN], target.audiences)
        eq_([Edition.BOOK_MEDIUM], target.media)

    def test_search(self):
        # Searching a Lane searches its search_target.

        work = self._work(with_license_pool=True)
        self.add_to_materialized_view(work)

        lane = self._lane()
        search_client = DummyExternalSearchIndex()
        search_client.bulk_update([work])

        pagination = Pagination(offset=0, size=1)

        results = lane.search(
            self._db, work.title, search_client, pagination
        )
        target_results = lane.search_target.search(
            self._db, work.title, search_client, pagination
        )
        eq_(results, target_results)

        # The single search result was converted to a MaterializedWork.
        [result] = results
        from model import MaterializedWork
        assert isinstance(result, MaterializedWork)
        eq_(work.id, result.works_id)

        # This still works if the lane is its own search_target.
        lane.root_for_patron_type = ["A"]
        results = lane.search(
            self._db, work.title, search_client, pagination
        )
        target_results = lane.search_target.search(
            self._db, work.title, search_client, pagination
        )
        eq_(results, target_results)

    def test_apply_custom_filters(self):

        # Create some works that will or won't show up in various
        # lanes.
        childrens_fiction = self._work(
            fiction=True, with_license_pool=True, 
            audience=Classifier.AUDIENCE_CHILDREN
        )
        nonfiction = self._work(fiction=False, with_license_pool=True)
        childrens_fiction.target_age = tuple_to_numericrange((8,8))
        self.add_to_materialized_view([childrens_fiction, nonfiction])

        def match_works(lane, works, featured=False):
            """Verify that calling apply_bibliographic_filters to the given
            lane yields the given list of works.
            """
            from model import MaterializedWork
            base_query = self._db.query(MaterializedWork).join(
                LicensePool, MaterializedWork.license_pool_id==LicensePool.id
            )
            query, distinct = lane.apply_bibliographic_filters(
                self._db, base_query, MaterializedWork, featured
            )
            results = query.all()
            works = sorted([(x.id, x.sort_title) for x in works])
            materialized_works = sorted(
                [(x.works_id, x.sort_title) for x in results]
            )
            eq_(works, materialized_works)
            return distinct

        # A lane may show only titles that come from a specific license source.
        gutenberg_only = self._lane()
        gutenberg_only.license_datasource = DataSource.lookup(
            self._db, DataSource.GUTENBERG
        )

        distinct = match_works(gutenberg_only, [nonfiction])
        # No custom list is involved, so there's no need to make the query
        # distinct.
        eq_(False, distinct)

        # A lane may show fiction, nonfiction, or both.
        fiction_lane = self._lane()
        fiction_lane.fiction = True
        match_works(fiction_lane, [childrens_fiction])

        nonfiction_lane = self._lane()
        nonfiction_lane.fiction = False
        match_works(nonfiction_lane, [nonfiction])

        both_lane = self._lane()
        both_lane.fiction = None
        match_works(both_lane, [childrens_fiction, nonfiction])

        # A lane may include a target age range.
        children_lane = self._lane()
        children_lane.target_age = (0,2)
        match_works(children_lane, [])
        children_lane.target_age = (8,10)
        match_works(children_lane, [childrens_fiction])

        # A lane may restrict itself to works on certain CustomLists.
        best_sellers, ignore = self._customlist(num_entries=0)
        childrens_fiction_entry, ignore = best_sellers.add_entry(
            childrens_fiction
        )
        best_sellers_lane = self._lane()
        best_sellers_lane.customlists.append(best_sellers)
        distinct = match_works(
            best_sellers_lane, [childrens_fiction], featured=False
        )

        # Now that CustomLists are in play, the query needs to be made
        # distinct, because a single work can show up on more than one
        # list.
        eq_(True, distinct)

        # Also, the `featured` argument makes a difference now. The
        # work isn't featured on its list, so the lane appears empty
        # when featured=True.
        match_works(best_sellers_lane, [], featured=True)

        # If the work becomes featured, it starts showing up again.
        childrens_fiction_entry.featured = True
        match_works(best_sellers_lane, [childrens_fiction], featured=True)

        # A lane may inherit restrictions from its parent.
        all_time_classics, ignore = self._customlist(num_entries=0)
        all_time_classics.add_entry(childrens_fiction)
        all_time_classics.add_entry(nonfiction)

        # This lane takes its entries from a list, and is the child
        # of a lane that takes its entries from a second list.
        best_selling_classics = self._lane(parent=best_sellers_lane)
        best_selling_classics.customlists.append(all_time_classics)
        match_works(best_selling_classics, [childrens_fiction, nonfiction])

        # When it inherits its parent's restrictions, only the
        # works that are on _both_ lists show up in the lane,
        best_selling_classics.inherit_parent_restrictions = True
        match_works(best_selling_classics, [childrens_fiction])

        # Other restrictions are inherited as well. Here, a title must
        # show up on both lists _and_ be a nonfiction book. There are
        # no titles that meet all three criteria.
        best_sellers_lane.fiction = False
        match_works(best_selling_classics, [])

        best_sellers_lane.fiction = True
        match_works(best_selling_classics, [childrens_fiction])       

    def test_apply_custom_filters_medium_restriction(self):
        """We have to test the medium query specially in a kind of hacky way,
        since currently the materialized view only includes ebooks.
        """
        audiobook = self._work(fiction=False, with_license_pool=True)
        audiobook.presentation_edition.medium = Edition.AUDIO_MEDIUM
        lane = self._lane()

        # This lane only includes ebooks, and it's empty.
        lane.media = [Edition.BOOK_MEDIUM]
        qu = self._db.query(Work).join(Work.license_pools).join(Work.presentation_edition)
        qu, distinct = lane.apply_bibliographic_filters(
            self._db, qu, Edition, False
        )
        eq_([], qu.all())

        # This lane only includes audiobooks, and it contains one book.
        lane.media = [Edition.AUDIO_MEDIUM]
        qu = self._db.query(Work).join(Work.license_pools)
        qu, distinct = lane.apply_bibliographic_filters(
            self._db, qu, Edition, False
        )
        eq_([audiobook], qu.all())

    def test_apply_age_range_filter(self):
        """Standalone test of apply_age_range_filter.
        
        Some of this code is also tested by test_apply_custom_filters.
        """
        adult = self._work(audience=Classifier.AUDIENCE_ADULT)
        eq_(None, adult.target_age)
        fourteen_or_fifteen = self._work(
            audience=Classifier.AUDIENCE_YOUNG_ADULT
        )
        fourteen_or_fifteen.target_age = tuple_to_numericrange((14,15))

        qu = self._db.query(Work)

        # This lane contains the YA book because its age range overlaps
        # the age range of the book.
        younger_ya = self._lane()
        younger_ya.target_age = (12,14)
        younger_ya_q = younger_ya.apply_age_range_filter(self._db, qu, Work)
        eq_([fourteen_or_fifteen], younger_ya_q.all())

        # This lane contains no books because it skews too old for the YA
        # book, but books for adults are not allowed.
        older_ya = self._lane()
        older_ya.target_age = (16,17)
        older_ya_q = older_ya.apply_age_range_filter(self._db, qu, Work)
        eq_([], older_ya_q.all())

        # Expand it to include books for adults, and the adult book
        # shows up despite having no target age at all.
        older_ya.target_age = (16,18)
        older_ya_q = older_ya.apply_age_range_filter(self._db, qu, Work)
        eq_([adult], older_ya_q.all())

    def test_apply_customlist_filter(self):
        """Standalone test of apply_age_range_filter.
        
        Some of this code is also tested by test_apply_custom_filters.
        """
        qu = self._db.query(Work)

        # If the lane has nothing to do with CustomLists,
        # apply_customlist_filter does nothing.
        no_lists = self._lane()
        eq_((qu, False), no_lists.apply_customlist_filter(qu, Work))

        # Set up a Work and a CustomList that contains the work.
        work = self._work(with_license_pool=True)
        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        eq_(gutenberg, work.license_pools[0].data_source)
        gutenberg_list, ignore = self._customlist(num_entries=0)
        gutenberg_list.data_source = gutenberg
        gutenberg_list_entry, ignore = gutenberg_list.add_entry(work)

        # This lane gets every work on a specific list.
        gutenberg_list_lane = self._lane()
        gutenberg_list_lane.customlists.append(gutenberg_list)

        # This lane gets every work on every list associated with Project
        # Gutenberg.
        gutenberg_lists_lane = self._lane()
        gutenberg_lists_lane.list_datasource = gutenberg

        def results(lane=gutenberg_lists_lane, must_be_featured=False):
            modified, distinct = lane.apply_customlist_filter(
                qu, Work, must_be_featured=must_be_featured
            )
            # Whenver a CustomList is in play, the query needs to be made
            # distinct.
            eq_(distinct, True)
            return modified.all()

        # Both lanes contain the work.
        eq_([work], results(gutenberg_list_lane))
        eq_([work], results(gutenberg_lists_lane))

        # This lane gets every work on a list associated with Overdrive.
        # There are no such lists, so the lane is empty.
        overdrive = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        overdrive_lists_lane = self._lane()
        overdrive_lists_lane.list_datasource = overdrive
        modified, distinct = overdrive_lists_lane.apply_customlist_filter(
            qu, Work
        )
        eq_([], modified.all())

        # It's possible to restrict a lane so that only works that are
        # _featured_ on a list show up. The work isn't featured, so it
        # doesn't show up.
        eq_([], results(must_be_featured=True))

        # Now it's featured, and it shows up.
        gutenberg_list_entry.featured = True
        eq_([work], results(must_be_featured=True))

        # It's possible to restrict a lane to works that were seen on
        # a certain list in a given timeframe.
        now = datetime.datetime.utcnow()
        two_days_ago = now - datetime.timedelta(days=2)
        gutenberg_list_entry.most_recent_appearance = two_days_ago

        # The lane will only show works that were seen within the last
        # day. There are no such works.
        gutenberg_lists_lane.list_seen_in_previous_days = 1
        eq_([], results())

        # Now it's been loosened to three days, and the work shows up.
        gutenberg_lists_lane.list_seen_in_previous_days = 3
        eq_([work], results())
