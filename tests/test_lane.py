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
from sqlalchemy import (
    and_,
    func,
)

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
    CustomListEntry,
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

        # Create a number of works that fall into various quality tiers.
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

        custom_list, ignore = self._customlist(num_entries=0)
        entry, ignore = custom_list.add_entry(
            awful_but_featured_on_a_list, featured=True
        )

        self.add_to_materialized_view(
            [awful_but_featured_on_a_list, featurable,
             featurable_but_not_available, decent_open_access,
             awful_but_licensed, awful_open_access]
        )

        # This FeaturedFacets object will be able to assign a numeric
        # value to each work that places it in a quality tier.
        facets = FeaturedFacets(minimum_featured_quality, True)

        # This custom database query field will perform the calculation.
        from model import MaterializedWork
        quality_field = facets.quality_tier_field(
            MaterializedWork).label("tier")

        # Test it out by using it in a SELECT statement.
        qu = self._db.query(
            MaterializedWork, quality_field
        ).join(
            LicensePool, 
            LicensePool.id==MaterializedWork.license_pool_id
        ).outerjoin(
            CustomListEntry, CustomListEntry.work_id==MaterializedWork.works_id
        )
        from model import dump_query

        expect_scores = {
            # featured on list (11) + available (1)
            awful_but_featured_on_a_list.sort_title: 12,

            # featurable (5) + licensed (2) + available (1)
            featurable.sort_title : 8,

            # featurable (5) + licensed (2)
            featurable_but_not_available.sort_title : 7,

            # quality open access (2) + available (1)
            decent_open_access.sort_title : 3,

            # licensed (2) + available (1)
            awful_but_licensed.sort_title : 3,

            # available (1)
            awful_open_access.sort_title : 1,
        }

        def best_score_dict(qu):
            return dict((x.sort_title,y) for x, y in qu)

        actual_scores = best_score_dict(qu)
        eq_(expect_scores, actual_scores)

        # If custom lists are not being considered, the "awful but
        # featured on a list" work loses its cachet.
        no_list_facets = FeaturedFacets(minimum_featured_quality, False)
        quality_field = no_list_facets.quality_tier_field(MaterializedWork).label("tier")
        no_list_qu = self._db.query(MaterializedWork, quality_field).join(
            LicensePool, 
            LicensePool.id==MaterializedWork.license_pool_id
        )

        # 1 is the expected score for a work that has nothing going
        # for it except for being available right now.
        expect_scores[awful_but_featured_on_a_list.sort_title] = 1
        actual_scores = best_score_dict(no_list_qu)
        eq_(expect_scores, actual_scores)

        # A low-quality work achieves the same low score if lists are
        # considered but the work is not _featured_ on its list.
        entry.featured = False
        actual_scores = best_score_dict(qu)
        eq_(expect_scores, actual_scores)


    def test_apply(self):
        """apply() orders a query randomly within quality tiers."""
        high_quality_1 = self._work(
            title="High quality, high random", with_license_pool=True
        )
        high_quality_1.quality = 1
        high_quality_1.random = 1

        high_quality_2 = self._work(
            title="High quality, low random", with_license_pool=True
        )
        high_quality_2.quality = 0.7
        high_quality_2.random = 0
        
        low_quality = self._work(
            title="Low quality, high random", with_license_pool=True
        )
        low_quality.quality = 0
        low_quality.random = 1

        facets = FeaturedFacets(0.5, False)
        base_query = self._db.query(Work).join(Work.license_pools)

        # Higher-tier works show up before lower-tier works.
        #
        # Within a tier, works with a high random number show up
        # before works with a low random number. The exact quality
        # doesn't matter (high_quality_2 is slightly lower quality
        # than high_quality_1), only the quality tier.
        featured = facets.apply(self._db, base_query, Work, False)
        eq_([high_quality_2, high_quality_1, low_quality], featured.all())

        # Switch the random numbers, and the order of high-quality
        # works is switched, but the high-quality works still show up
        # first.
        high_quality_1.random = 0
        high_quality_2.random = 1
        eq_([high_quality_1, high_quality_2, low_quality], featured.all())

        # Passing in distinct=True makes the query distinct on
        # three different fields.
        eq_(False, base_query._distinct)
        distinct_query = facets.apply(self._db, base_query, Work, True)
        eq_(3, len(distinct_query._distinct))



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
        self.random_sample_calls = []

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
        self.random_sample_calls.append((query, target_size))
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

        w1 = MockWork(1)

        wl.queue_works([w1])
        featured = wl.featured_works(self._db)
        eq_([w1], featured)

        # We created a FeaturedFacets object and passed it in to works().
        [(facets, pagination, featured)] = wl.works_calls
        eq_(self._default_library.minimum_featured_quality, 
            facets.minimum_featured_quality)
        eq_(featured, facets.uses_customlists)

        # We then called random_sample() on the results.
        [(query, target_size)] = wl.random_sample_calls
        eq_([w1], query)
        eq_(self._default_library.featured_lane_size, target_size)

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

            def bibliographic_filter_clause(
                    self, _db, query, work_model, featured
            ):
                called['apply_bibliographic_filters'] = True
                called['apply_bibliographic_filters.featured'] = featured
                return query, None, self.distinct

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
            """Mock WorkList whose bibliographic_filter_clause implementation
            believes the WorkList should not exist at all.
            """

            def bibliographic_filter_clause(
                    self, _db, query, work_model, featured
            ):
                return None, None, False

        wl = MockWorkList()
        wl.initialize(self._default_library)
        from model import MaterializedWork
        qu = self._db.query(MaterializedWork)
        eq_(None, wl.apply_filters(self._db, qu, MaterializedWork, None, None))

    def test_bibliographic_filter_clause(self):
        called = dict()

        class MockWorkList(WorkList):
            """Mock WorkList that simply verifies that
            bibliographic_filter_clause() calls various hook methods.
            """

            def __init__(self, languages=None, genre_ids=None, media=None):
                self.languages = languages
                self.genre_ids = genre_ids
                self.media = media

            def audience_filter_clauses(self, _db, qu, work_model):
                called['apply_audience_filter'] = True
                return []

        wl = MockWorkList()
        from model import MaterializedWorkWithGenre as wg
        original_qu = self._db.query(wg)

        # If no languages or genre IDs are specified, and the hook
        # methods do nothing, then bibliographic_filter_clause() has
        # no effect.
        featured_object = object()
        final_qu, bibliographic_filter, distinct = wl.bibliographic_filter_clause(
            self._db, original_qu, wg, featured_object
        )
        eq_(original_qu, final_qu)
        eq_(None, bibliographic_filter)

        # But at least the hook methods were called with the correct
        # arguments.
        eq_(True, called['apply_audience_filter'])

        # If languages, media, and genre IDs are specified, then they are
        # incorporated into the query.
        #
        english_sf = self._work(language="eng", with_license_pool=True)
        english_sf.presentation_edition.medium = Edition.BOOK_MEDIUM
        sf, ignore = Genre.lookup(self._db, "Science Fiction")
        romance, ignore = Genre.lookup(self._db, "Romance")
        english_sf.genres.append(sf)
        self.add_to_materialized_view(english_sf)

        # Create a WorkList that will find the MaterializedWorkWithGenre
        # for the English SF book.
        def worklist_has_books(
                expect_books, **worklist_constructor_args
        ):
            """Apply bibliographic filters to a query and verify
            that it finds only the given books.
            """
            worklist = MockWorkList(**worklist_constructor_args)
            qu, clause, distinct = worklist.bibliographic_filter_clause(
                self._db, original_qu, wg, False
            )
            qu = qu.filter(clause)
            if distinct:
                qu = qu.distinct()
            expect_titles = sorted([x.sort_title for x in expect_books])
            actual_titles = sorted([x.sort_title for x in qu])
            eq_(expect_titles, actual_titles)

        worklist_has_books(
            [english_sf], 
            languages=["eng"], genre_ids=[sf.id], media=[Edition.BOOK_MEDIUM]
        )

        # WorkLists that do not match by language, medium, or genre will not
        # find the English SF book.
        worklist_has_books([], languages=["spa"], genre_ids=[sf.id])
        worklist_has_books([], languages=["eng"], genre_ids=[romance.id])
        worklist_has_books(
            [], 
            languages=["eng"], genre_ids=[sf.id], media=[Edition.AUDIO_MEDIUM]
        )

    def test_audience_filter_clauses(self):

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
            """Invoke WorkList.apply_audience_clauses using the given 
            `audiences`, and return all the matching Work objects.
            """
            wl = WorkList()
            wl.audiences = audiences
            qu = self._db.query(Work).join(Work.license_pools)
            clauses = wl.audience_filter_clauses(self._db, qu, Work)
            if clauses:
                qu = qu.filter(and_(*clauses))
            return qu.all()

        eq_([gutenberg_adult], for_audiences(Classifier.AUDIENCE_ADULT))

        # The Gutenberg "children's" book is filtered out because it we have
        # no guarantee it is actually suitable for children.
        eq_([non_gutenberg_children], 
            for_audiences(Classifier.AUDIENCE_CHILDREN))

        # This can sometimes lead to unexpected results, but the whole
        # thing is a hack and needs to be improved anyway.
        eq_([non_gutenberg_children], 
            for_audiences(Classifier.AUDIENCE_ADULT, 
                          Classifier.AUDIENCE_CHILDREN))

        # If no particular audiences are specified, no books are filtered.
        eq_(set([gutenberg_adult, gutenberg_children, non_gutenberg_children]), 
            set(for_audiences()))

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
        i6 = self._identifier()
        i7 = self._identifier()
        i8 = self._identifier()
        i9 = self._identifier()
        i10 = self._identifier()
        qu = self._db.query(Identifier).order_by(Identifier.id)

        # If the random sample is smaller than the population, a
        # randomly located slice is chosen, and the slice is
        # shuffled. (It's presumed that the query sorts items by some
        # randomly generated number such as Work.random, so that choosing
        # a slice gets you a random sample -- that's not the case here.)
        sample = WorkList.random_sample(qu, 2, quality_coefficient=1)
        eq_([i6, i7], sorted(sample, key=lambda x: x.id))

        # If the random sample is larger than the sample population,
        # the population is shuffled.
        sample = WorkList.random_sample(qu, 11)
        eq_(set([i1, i2, i3, i4, i5, i6, i7, i8, i9, i10]),
            set(sample))

        # We weight the random sample towards the front of the list.
        # By default we only choose from the first 10% of the list.
        #
        # This means if we sample one item from this ten-item
        # population, we will always get the first value.
        for i in range(0, 10):
            eq_([i1], WorkList.random_sample(qu, 1))

        # If we sample two items, we will always get the first and
        # second values.
        for i in range(0, 10):
            eq_(set([i1, i2]), set(WorkList.random_sample(qu, 2)))

        # If we set the quality coefficient to sample from the first
        # half of the list, we will never get an item from the second
        # half.
        samples = [WorkList.random_sample(qu, 2, 0.5) for x in range(5)]
        eq_(
            [set([i4, i3]), 
             set([i1, i2]), 
             set([i3, i2]), 
             set([i1, i2]), 
             set([i3, i4])],
            [set(x) for x in samples]
        )

        # This works even if the quality coefficient appears to limit
        # selection to a fractional number of works.
        sample = WorkList.random_sample(qu, 2, quality_coefficient=0.23109)
        eq_([i1, i2], sorted(sample, key=lambda x: x.id))


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
        eq_("%s / %s" % (lane.library.short_name, lane.display_name),
            lane.full_identifier)

        eq_(
            "%s / %s / %s" % (
                lane.library.short_name, lane.display_name, 
                child_lane.display_name
            ), 
            child_lane.full_identifier
        )

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

    def test_affected_by_customlist(self):

        # Two lists.
        l1, ignore = self._customlist(
            data_source_name=DataSource.GUTENBERG,
            num_entries=0
        )
        l2, ignore = self._customlist(
            data_source_name=DataSource.OVERDRIVE, num_entries=0
        )

        # A lane populated by specific lists.
        lane = self._lane()

        # Not affected by any lists.
        for l in [l1, l2]:
            eq_(0, Lane.affected_by_customlist(l1).count())

        # Add a lane to the list, and it becomes affected.
        lane.customlists.append(l1)
        eq_([lane], lane.affected_by_customlist(l1).all())
        eq_(0, lane.affected_by_customlist(l2).count())
        lane.customlists = []

        # A lane based on all lists with the GUTENBERG data source.
        lane2 = self._lane()
        lane2.list_datasource = l1.data_source

        # It's affected by the GUTENBERG list but not the OVERDRIVE
        # list.
        eq_([lane2], Lane.affected_by_customlist(l1).all())
        eq_(0, Lane.affected_by_customlist(l2).count())

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

    def test_bibliographic_filter_clause(self):

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
            new_query, bibliographic_clause, distinct = lane.bibliographic_filter_clause(
                self._db, base_query, MaterializedWork, featured
            )
            
            if lane.uses_customlists:
                # bibliographic_filter_clause modifies the query (by
                # calling customlist_filter_clauses).
                assert base_query != new_query
                if not lane.list_datasource and len(lane.customlists) < 2:
                    # This query does not need to be distinct -- either
                    # there are no custom lists involved, or there
                    # is known to be only a single list.
                    eq_(False, distinct)
                else:
                    # This query needs to be distinct, because a 
                    # single book might show up more than once.
                    eq_(True, distinct)
            else:
                # The input query is the same as the output query, and
                # it does not need to be distinct.
                eq_(base_query, new_query)
                eq_(False, distinct)

            final_query = new_query.filter(bibliographic_clause)
            results = final_query.all()
            works = sorted([(x.id, x.sort_title) for x in works])
            materialized_works = sorted(
                [(x.works_id, x.sort_title) for x in results]
            )
            eq_(works, materialized_works)

        # A lane may show only titles that come from a specific license source.
        gutenberg_only = self._lane()
        gutenberg_only.license_datasource = DataSource.lookup(
            self._db, DataSource.GUTENBERG
        )

        match_works(gutenberg_only, [nonfiction])

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
        match_works(
            best_sellers_lane, [childrens_fiction], featured=False
        )

        # Now that CustomLists are in play, the `featured` argument
        # makes a difference. The work isn't featured on its list, so
        # the lane appears empty when featured=True.
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

    def test_bibliographic_filter_clause_medium_restriction(self):
        """We have to test the medium query specially in a kind of hacky way,
        since currently the materialized view only includes ebooks.
        """
        audiobook = self._work(
            title="Audiobook", fiction=False, with_license_pool=True
        )
        audiobook.presentation_edition.medium = Edition.AUDIO_MEDIUM
        lane = self._lane()

        def matches(lane):
            qu = self._db.query(Work).join(Work.license_pools).join(Work.presentation_edition)
            new_qu, bib_filter, distinct = lane.bibliographic_filter_clause(
                self._db, qu, Edition, False
            )
            eq_(new_qu, qu)
            eq_(False, distinct)
            return new_qu.filter(bib_filter).all()

        # This lane only includes ebooks, and it's empty.
        lane.media = [Edition.BOOK_MEDIUM]
        eq_([], matches(lane))

        # This lane only includes audiobooks, and it contains one book.
        lane.media = [Edition.AUDIO_MEDIUM]
        eq_([audiobook], matches(lane))

    def test_age_range_filter_clauses(self):
        """Standalone test of age_range_filter_clauses().
        """
        def filtered(lane):
            """Build a query that applies the given lane's age filter to the 
            works table.
            """
            qu = self._db.query(Work)
            clauses = lane.age_range_filter_clauses(Work)
            if clauses:
                qu = qu.filter(and_(*clauses))
            return qu.all()

        adult = self._work(title="For adults", 
                           audience=Classifier.AUDIENCE_ADULT)
        eq_(None, adult.target_age)
        fourteen_or_fifteen = self._work(
            title="For teens",
            audience=Classifier.AUDIENCE_YOUNG_ADULT
        )
        fourteen_or_fifteen.target_age = tuple_to_numericrange((14,15))

        # This lane contains the YA book because its age range overlaps
        # the age range of the book.
        younger_ya = self._lane()
        younger_ya.target_age = (12,14)
        eq_([fourteen_or_fifteen], filtered(younger_ya))

        # This lane contains no books because it skews too old for the YA
        # book, but books for adults are not allowed.
        older_ya = self._lane()
        older_ya.target_age = (16,17)
        eq_([], filtered(older_ya))

        # Expand it to include books for adults, and the adult book
        # shows up despite having no target age at all.
        older_ya.target_age = (16,18)
        eq_([adult], filtered(older_ya))

    def test_customlist_filter_clauses(self):
        """Standalone test of apply_customlist_filter.
        
        Some of this code is also tested by test_apply_custom_filters.
        """

        # If a lane has nothing to do with CustomLists,
        # apply_customlist_filter does nothing.
        no_lists = self._lane()
        qu = self._db.query(Work)
        new_qu, clauses, distinct = no_lists.customlist_filter_clauses(qu, Work)
        eq_(qu, new_qu)
        eq_([], clauses)
        eq_(False, distinct)

        # Now set up a Work and a CustomList that contains the work.
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

        def results(lane=gutenberg_lists_lane, must_be_featured=False,
                    expect_distinct=False):
            qu = self._db.query(Work)
            new_qu, clauses, distinct = lane.customlist_filter_clauses(
                qu, Work, must_be_featured=must_be_featured
            )

            # The query comes out different than it goes in -- there's a
            # new join against CustomList.
            assert new_qu != qu

            eq_(expect_distinct, distinct)

            # Run the query and see what it matches.
            modified = new_qu.filter(and_(*clauses)).distinct()
            return modified.all()

        # Both lanes contain the work.
        eq_([work], results(gutenberg_list_lane, expect_distinct=False))
        eq_([work], results(gutenberg_lists_lane, expect_distinct=True))

        # If we add another list to the gutenberg_list_lane,
        # it becomes distinct, because there's now a possibility
        # that a single book might show up more than once.
        gutenberg_list_2, ignore = self._customlist(num_entries=0)
        gutenberg_list_lane.customlists.append(gutenberg_list)
        eq_([work], results(gutenberg_list_lane, expect_distinct=True))

        # This lane gets every work on a list associated with Overdrive.
        # There are no such lists, so the lane is empty.
        overdrive = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        overdrive_lists_lane = self._lane()
        overdrive_lists_lane.list_datasource = overdrive
        eq_([], results(overdrive_lists_lane, expect_distinct=True))

        # It's possible to restrict a lane so that only works that are
        # _featured_ on a list show up. The work isn't featured, so it
        # doesn't show up.
        eq_([], results(must_be_featured=True, expect_distinct=True))

        # Now it's featured, and it shows up.
        gutenberg_list_entry.featured = True
        eq_([work], results(must_be_featured=True, expect_distinct=True))

        # It's possible to restrict a lane to works that were seen on
        # a certain list in a given timeframe.
        now = datetime.datetime.utcnow()
        two_days_ago = now - datetime.timedelta(days=2)
        gutenberg_list_entry.most_recent_appearance = two_days_ago

        # The lane will only show works that were seen within the last
        # day. There are no such works.
        gutenberg_lists_lane.list_seen_in_previous_days = 1
        eq_([], results(expect_distinct=True))

        # Now it's been loosened to three days, and the work shows up.
        gutenberg_lists_lane.list_seen_in_previous_days = 3
        eq_([work], results(expect_distinct=True))
