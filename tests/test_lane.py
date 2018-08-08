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
from sqlalchemy.sql.elements import Case
from sqlalchemy import (
    and_,
    func,
)

from classifier import Classifier

from entrypoint import (
    AudiobooksEntryPoint,
    EbooksEntryPoint,
    EverythingEntryPoint,
    EntryPoint,
)

from external_search import (
    DummyExternalSearchIndex,
)

from lane import (
    Facets,
    FacetsWithEntryPoint,
    FeaturedFacets,
    Pagination,
    SearchFacets,
    WorkList,
    Lane,
)

from model import (
    dump_query,
    get_one_or_create,
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
    WorkGenre,
)
from problem_details import INVALID_INPUT


class TestFacetsWithEntryPoint(DatabaseTest):

    class MockFacetConfig(object):
        """Pass this in when you call FacetsWithEntryPoint.from_request
        but you don't care which EntryPoints are configured.
        """
        entrypoints = []

    def test_items(self):
        ep = AudiobooksEntryPoint
        f = FacetsWithEntryPoint(ep)
        expect_items = (f.ENTRY_POINT_FACET_GROUP_NAME, ep.INTERNAL_NAME)
        eq_([expect_items], list(f.items()))
        eq_("%s=%s" % expect_items, f.query_string)

    def test_apply(self):
        class MockEntryPoint(object):
            def apply(self, qu):
                self.called_with = qu

        ep = MockEntryPoint()
        f = FacetsWithEntryPoint(ep)
        _db = object()
        qu = object()
        f.apply(_db, qu)
        eq_(qu, ep.called_with)

    def test_navigate(self):
        old_entrypoint = object()
        kwargs = dict(extra_key="extra_value")
        facets = FacetsWithEntryPoint(old_entrypoint, **kwargs)
        new_entrypoint = object()
        new_facets = facets.navigate(new_entrypoint)

        # A new FacetsWithEntryPoint was created.
        assert isinstance(new_facets, FacetsWithEntryPoint)

        # It has the new entry point.
        eq_(new_entrypoint, new_facets.entrypoint)

        # The keyword arguments used to create the origina faceting
        # object were propagated to its constructor.
        eq_(kwargs, new_facets.constructor_kwargs)

    def test_from_request(self):
        """from_request just calls _from_request."""
        expect = object()
        class Mock(FacetsWithEntryPoint):
            @classmethod
            def _from_request(cls, *args, **kwargs):
                return expect
        eq_(expect, Mock.from_request(None, None, None, None))

    def test_from_request_propagates_extra_kwargs(self):
        """Any keyword arguments passed to from_request() are propagated
        through to the facet constructor.
        """
        class ExtraFacets(FacetsWithEntryPoint):
            def __init__(self, entrypoint=None, extra=None):
                self.extra = extra

        facets = ExtraFacets.from_request(
            None, self.MockFacetConfig, {}.get, None, extra="extra value"
        )
        assert isinstance(facets, ExtraFacets)
        eq_("extra value", facets.extra)

    def test__from_request(self):
        """_from_request calls load_entrypoint and instantiates the
        class with the result.
        """
        self.expect = object()
        @classmethod
        def mock_load_entrypoint(cls, entrypoint_name, entrypoints):
            self.called_with = (entrypoint_name, entrypoints)
            return self.expect
        old = FacetsWithEntryPoint.load_entrypoint
        FacetsWithEntryPoint.load_entrypoint = mock_load_entrypoint

        # The facet group name will be pulled out of the 'request'
        # and passed into mock_load_entrypoint.
        def get_argument(key, default):
            eq_(key, Facets.ENTRY_POINT_FACET_GROUP_NAME)
            return "name of the entrypoint"

        mock_worklist = object()
        config = self.MockFacetConfig
        facets = FacetsWithEntryPoint._from_request(
            config, get_argument, mock_worklist
        )
        assert isinstance(facets, FacetsWithEntryPoint)
        eq_(self.expect, facets.entrypoint)
        eq_(("name of the entrypoint", config.entrypoints), self.called_with)

        # If load_entrypoint returns a ProblemDetail, that object is
        # returned instead of the faceting class.
        self.expect = INVALID_INPUT
        eq_(
            self.expect,
            FacetsWithEntryPoint._from_request(
                config, get_argument, mock_worklist
            )
        )
        FacetsWithEntryPoint.load_entrypoint = old

    def test_load_entrypoint(self):
        audio = AudiobooksEntryPoint
        ebooks = EbooksEntryPoint

        # These are the allowable entrypoints for this site -- we'll
        # be passing this in to load_entrypoint every time.
        entrypoints = [audio, ebooks]

        worklist = object()
        m = FacetsWithEntryPoint.load_entrypoint

        # This request does not ask for any particular entrypoint,
        # so it gets the default.
        eq_(audio, m(None, entrypoints))

        # This request asks for an entrypoint and gets it.
        eq_(ebooks, m(ebooks.INTERNAL_NAME, entrypoints))

        # This request asks for an entrypoint that is not available,
        # and gets the default.
        eq_(audio, m("no such entrypoint", entrypoints))

        # If no EntryPoints are available, load_entrypoint returns
        # nothing.
        eq_(None, m(audio.INTERNAL_NAME, []))

    def test_selectable_entrypoints(self):
        """The default implementation of selectable_entrypoints just returns
        the worklist's entrypoints.
        """
        class MockWorkList(object):
            def __init__(self, entrypoints):
                self.entrypoints = entrypoints

        mock_entrypoints = object()
        worklist = MockWorkList(mock_entrypoints)

        m = FacetsWithEntryPoint.selectable_entrypoints
        eq_(mock_entrypoints, m(worklist))
        eq_([], m(None))


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

    def test_facets_dont_need_a_library(self):
        enabled_facets = {
            Facets.ORDER_FACET_GROUP_NAME : [
                Facets.ORDER_TITLE, Facets.ORDER_AUTHOR,
            ],
            Facets.COLLECTION_FACET_GROUP_NAME : [Facets.COLLECTION_MAIN],
            Facets.AVAILABILITY_FACET_GROUP_NAME : [Facets.AVAILABLE_OPEN_ACCESS]
        }

        facets = Facets(
            None,
            Facets.COLLECTION_MAIN, Facets.AVAILABLE_OPEN_ACCESS,
            Facets.ORDER_TITLE, enabled_facets=enabled_facets
        )
        all_groups = list(facets.facet_groups)
        expect = [['order', 'author', False], ['order', 'title', True]]
        eq_(expect, sorted([list(x[:2]) + [x[-1]] for x in all_groups]))

    def test_items(self):
        """Verify that Facets.items() returns all information necessary
        to recreate the Facets object.
        """
        facets = Facets(
            self._default_library,
            Facets.COLLECTION_MAIN, Facets.AVAILABLE_ALL, Facets.ORDER_TITLE,
            entrypoint=AudiobooksEntryPoint
        )
        eq_([
            ('available', Facets.AVAILABLE_ALL),
            ('collection', Facets.COLLECTION_MAIN),
            ('entrypoint', AudiobooksEntryPoint.INTERNAL_NAME),
            ('order', Facets.ORDER_TITLE)],
            sorted(facets.items())
        )

    def test_order_facet_to_database_field(self):
        from model import MaterializedWorkWithGenre as mwg
        def fields(facet):
            return [
                Facets.order_facet_to_database_field(facet)
            ]

        # You can sort by title...
        eq_([mwg.sort_title],
            fields(Facets.ORDER_TITLE))

        # ...by author...
        eq_([mwg.sort_author],
            fields(Facets.ORDER_AUTHOR))

        # ...by work ID...
        eq_([mwg.works_id],
            fields(Facets.ORDER_WORK_ID))

        # ...by last update time...
        eq_([mwg.last_update_time],
            fields(Facets.ORDER_LAST_UPDATE))

        # ...by most recently added...
        eq_([mwg.availability_time],
            fields(Facets.ORDER_ADDED_TO_COLLECTION))

        # ...or randomly.
        eq_([mwg.random],
            fields(Facets.ORDER_RANDOM))

    def test_order_by(self):
        from model import MaterializedWorkWithGenre as m

        def order(facet, ascending=None):
            f = Facets(
                self._default_library,
                collection=Facets.COLLECTION_FULL,
                availability=Facets.AVAILABLE_ALL,
                order=facet,
                order_ascending=ascending,
            )
            return f.order_by()[0]

        def compare(a, b):
            assert(len(a) == len(b))
            for i in range(0, len(a)):
                assert(a[i].compare(b[i]))

        expect = [m.sort_author.asc(), m.sort_title.asc(), m.works_id.asc()]
        actual = order(Facets.ORDER_AUTHOR, True)
        compare(expect, actual)

        expect = [m.sort_author.desc(), m.sort_title.asc(), m.works_id.asc()]
        actual = order(Facets.ORDER_AUTHOR, False)
        compare(expect, actual)

        expect = [m.sort_title.asc(), m.sort_author.asc(), m.works_id.asc()]
        actual = order(Facets.ORDER_TITLE, True)
        compare(expect, actual)

        expect = [m.last_update_time.asc(), m.sort_author.asc(), m.sort_title.asc(), m.works_id.asc()]
        actual = order(Facets.ORDER_LAST_UPDATE, True)
        compare(expect, actual)

        expect = [m.random.asc(), m.sort_author.asc(), m.sort_title.asc(),
                  m.works_id.asc()]
        actual = order(Facets.ORDER_RANDOM, True)
        compare(expect, actual)

        expect = [m.availability_time.desc(), m.sort_author.asc(), m.sort_title.asc(), m.works_id.asc()]
        actual = order(Facets.ORDER_ADDED_TO_COLLECTION, None)
        compare(expect, actual)

    def test_navigate(self):
        """Test the ability of navigate() to move between slight
        variations of a FeaturedFacets object.
        """
        F = Facets

        ebooks = EbooksEntryPoint
        f = Facets(self._default_library, F.COLLECTION_FULL, F.AVAILABLE_ALL,
                   F.ORDER_TITLE, entrypoint=ebooks)

        different_collection = f.navigate(collection=F.COLLECTION_FEATURED)
        eq_(F.COLLECTION_FEATURED, different_collection.collection)
        eq_(F.AVAILABLE_ALL, different_collection.availability)
        eq_(F.ORDER_TITLE, different_collection.order)
        eq_(ebooks, different_collection.entrypoint)

        different_availability = f.navigate(availability=F.AVAILABLE_NOW)
        eq_(F.COLLECTION_FULL, different_availability.collection)
        eq_(F.AVAILABLE_NOW, different_availability.availability)
        eq_(F.ORDER_TITLE, different_availability.order)
        eq_(ebooks, different_availability.entrypoint)

        different_order = f.navigate(order=F.ORDER_AUTHOR)
        eq_(F.COLLECTION_FULL, different_order.collection)
        eq_(F.AVAILABLE_ALL, different_order.availability)
        eq_(F.ORDER_AUTHOR, different_order.order)
        eq_(ebooks, different_order.entrypoint)

        audiobooks = AudiobooksEntryPoint
        different_entrypoint = f.navigate(entrypoint=audiobooks)
        eq_(F.COLLECTION_FULL, different_entrypoint.collection)
        eq_(F.AVAILABLE_ALL, different_entrypoint.availability)
        eq_(F.ORDER_TITLE, different_entrypoint.order)
        eq_(audiobooks, different_entrypoint.entrypoint)

    def test_from_request(self):
        library = self._default_library

        library.setting(EntryPoint.ENABLED_SETTING).value = json.dumps(
            [AudiobooksEntryPoint.INTERNAL_NAME, EbooksEntryPoint.INTERNAL_NAME]
        )

        config = library
        worklist = WorkList()
        worklist.initialize(library)

        m = Facets.from_request

        # Valid object using the default settings.
        default_order = config.default_facet(Facets.ORDER_FACET_GROUP_NAME)
        default_collection = config.default_facet(
            Facets.COLLECTION_FACET_GROUP_NAME
        )
        default_availability = config.default_facet(
            Facets.AVAILABILITY_FACET_GROUP_NAME
        )
        args = {}
        facets = m(library, library, args.get, worklist)
        eq_(default_order, facets.order)
        eq_(default_collection, facets.collection)
        eq_(default_availability, facets.availability)
        eq_(library, facets.library)
        eq_(AudiobooksEntryPoint, facets.entrypoint)

        # Valid object using non-default settings.
        args = dict(
            order=Facets.ORDER_TITLE,
            collection=Facets.COLLECTION_FULL,
            available=Facets.AVAILABLE_OPEN_ACCESS,
            entrypoint=EbooksEntryPoint.INTERNAL_NAME,
        )
        facets = m(library, library, args.get, worklist)
        eq_(Facets.ORDER_TITLE, facets.order)
        eq_(Facets.COLLECTION_FULL, facets.collection)
        eq_(Facets.AVAILABLE_OPEN_ACCESS, facets.availability)
        eq_(library, facets.library)
        eq_(EbooksEntryPoint, facets.entrypoint)

        # Invalid order
        args = dict(order="no such order")
        invalid_order = m(library, library, args.get, None)
        eq_(INVALID_INPUT.uri, invalid_order.uri)
        eq_("I don't know how to order a feed by 'no such order'",
            invalid_order.detail)

        # Invalid availability
        args = dict(available="no such availability")
        invalid_availability = m(library, library, args.get, None)
        eq_(INVALID_INPUT.uri, invalid_availability.uri)
        eq_("I don't understand the availability term 'no such availability'",
            invalid_availability.detail)

        # Invalid collection
        args = dict(collection="no such collection")
        invalid_collection = m(library, library, args.get, None)
        eq_(INVALID_INPUT.uri, invalid_collection.uri)
        eq_("I don't understand what 'no such collection' refers to.",
            invalid_collection.detail)

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

        from model import MaterializedWorkWithGenre as mwg
        qu = self._db.query(mwg).join(
            LicensePool, mwg.license_pool_id==LicensePool.id
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

    def test_navigate(self):
        """Test the ability of navigate() to move between slight
        variations of a FeaturedFacets object.
        """
        entrypoint = EbooksEntryPoint
        f = FeaturedFacets(1, True, entrypoint)

        different_entrypoint = f.navigate(entrypoint=AudiobooksEntryPoint)
        eq_(1, different_entrypoint.minimum_featured_quality)
        eq_(True, different_entrypoint.uses_customlists)
        eq_(AudiobooksEntryPoint, different_entrypoint.entrypoint)

        different_quality = f.navigate(minimum_featured_quality=2)
        eq_(2, different_quality.minimum_featured_quality)
        eq_(True, different_quality.uses_customlists)
        eq_(entrypoint, different_quality.entrypoint)

        not_a_list = f.navigate(uses_customlists=False)
        eq_(1, not_a_list.minimum_featured_quality)
        eq_(False, not_a_list.uses_customlists)
        eq_(entrypoint, not_a_list.entrypoint)


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
        quality_field = facets.quality_tier_field().label("tier")

        # Test it out by using it in a SELECT statement.
        from model import MaterializedWorkWithGenre as work_model
        qu = self._db.query(
            work_model, quality_field
        ).join(
            LicensePool,
            LicensePool.id==work_model.license_pool_id
        ).outerjoin(
            CustomListEntry, CustomListEntry.work_id==work_model.works_id
        )

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
        quality_field = no_list_facets.quality_tier_field().label("tier")
        no_list_qu = self._db.query(work_model, quality_field).join(
            LicensePool,
            LicensePool.id==work_model.license_pool_id
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
        high_quality_low_random = self._work(
            title="High quality, low random", with_license_pool=True
        )
        high_quality_low_random.quality = 1
        high_quality_low_random.random = 0

        high_quality_high_random = self._work(
            title="High quality, high random", with_license_pool=True
        )
        high_quality_high_random.quality = 0.7
        high_quality_high_random.random = 1

        low_quality = self._work(
            title="Low quality, high random", with_license_pool=True
        )
        low_quality.quality = 0
        low_quality.random = 1

        self.add_to_materialized_view(
            [high_quality_low_random, high_quality_high_random,
             low_quality]
        )

        facets = FeaturedFacets(0.5, False)
        from model import MaterializedWorkWithGenre as work_model
        base_query = self._db.query(work_model).join(work_model.license_pool)

        def expect(works, qu):
            expect_ids = [x.id for x in works]
            actual_ids = [x.works_id for x in qu]
            eq_(expect_ids, actual_ids)

        # Higher-tier works show up before lower-tier works.
        #
        # Within a tier, works with a high random number show up
        # before works with a low random number. The exact quality
        # doesn't matter (high_quality_2 is slightly lower quality
        # than high_quality_1), only the quality tier.
        featured = facets.apply(self._db, base_query)
        expect(
            [high_quality_high_random, high_quality_low_random, low_quality],
            featured
        )

        # Switch the random numbers, and the order of high-quality
        # works is switched, but the high-quality works still show up
        # first.
        high_quality_high_random.random = 0.12
        high_quality_low_random.random = 0.98
        self._db.commit()
        SessionManager.refresh_materialized_views(self._db)
        expect([high_quality_low_random, high_quality_high_random, low_quality], featured)

        # The query is distinct on works_id. (It's also distinct on the
        # fields used in the ORDER BY clause, but that's just to get the
        # query to work.)
        eq_(False, base_query._distinct)
        distinct_query = facets.apply(self._db, base_query)
        eq_(
            work_model.works_id, distinct_query._distinct[-1]
        )


class TestSearchFacets(DatabaseTest):

    def test_selectable_entrypoints(self):
        """If the WorkList has more than one facet, an 'everything' facet
        is added for search purposes.
        """
        class MockWorkList(object):
            def __init__(self):
                self.entrypoints = None

        ep1 = object()
        ep2 = object()
        worklist = MockWorkList()

        # No WorkList, no EntryPoints.
        m = SearchFacets.selectable_entrypoints
        eq_([], m(None))

        # If there is one EntryPoint, it is returned as-is.
        worklist.entrypoints = [ep1]
        eq_([ep1], m(worklist))

        # If there are multiple EntryPoints, EverythingEntryPoint
        # shows up at the beginning.
        worklist.entrypoints = [ep1, ep2]
        eq_([EverythingEntryPoint, ep1, ep2], m(worklist))

        # If EverythingEntryPoint is already in the list, it's not
        # added twice.
        worklist.entrypoints = [ep1, EverythingEntryPoint, ep2]
        eq_(worklist.entrypoints, m(worklist))

    def test_navigation(self):
        """Navigating from one SearchFacets to another
        gives a new SearchFacets object, even though SearchFacets doesn't
        define navigate().

        I.e. this is really a test of FacetsWithEntryPoint.navigate().
        """
        facets = SearchFacets(object())
        new_ep = object()
        new_facets = facets.navigate(new_ep)
        assert isinstance(new_facets, SearchFacets)
        eq_(new_ep, new_facets.entrypoint)

class TestPagination(DatabaseTest):

    def test_has_next_page_total_size(self):
        """Test the ability of Pagination.total_size to control whether there is a next page."""
        query = self._db.query(Work)
        pagination = Pagination(size=2)

        # When total_size is not set, Pagination assumes there is a
        # next page.
        pagination.apply(query)
        eq_(True, pagination.has_next_page)

        # Here, there is one more item on the next page.
        pagination.total_size = 3
        eq_(0, pagination.offset)
        eq_(True, pagination.has_next_page)

        # Here, the last item on this page is the last item in the dataset.
        pagination.offset = 1
        eq_(False, pagination.has_next_page)

        # If we somehow go over the end of the dataset, there is no next page.
        pagination.offset = 400
        eq_(False, pagination.has_next_page)

        # If both total_size and this_page_size are set, total_size
        # takes precedence.
        pagination.offset = 0
        pagination.total_size = 100
        pagination.this_page_size = 0
        eq_(True, pagination.has_next_page)

        pagination.total_size = 0
        pagination.this_page_size = 10
        eq_(False, pagination.has_next_page)

    def test_has_next_page_this_page_size(self):
        """Test the ability of Pagination.this_page_size to control whether there is a next page."""
        query = self._db.query(Work)
        pagination = Pagination(size=2)

        # When this_page_size is not set, Pagination assumes there is a
        # next page.
        pagination.apply(query)
        eq_(True, pagination.has_next_page)

        # Here, there is nothing on the current page. There is no next page.
        pagination.this_page_size = 0
        eq_(False, pagination.has_next_page)

        # If the page is full, we can be almost certain there is a next page.
        pagination.this_page_size = 400
        eq_(True, pagination.has_next_page)

        # Here, there is one item on the current page. Even though the
        # current page is not full (page size is 2), we assume for
        # safety's sake that there is a next page. The cost of getting
        # this wrong is low, compared to the cost of saying there is no
        # next page when there actually is.
        pagination.this_page_size = 1
        eq_(True, pagination.has_next_page)


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

    def groups(self, *args, **kwargs):
        try:
            for work in self._featured_works.pop(0):
                yield work, self
        except IndexError:
            return

class MockWork(object):
    """Acts as a Work or a MaterializedWorkWithGenre interchangeably."""
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
                      genres=[sf, romance], entrypoints=[1,2,3])

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

        # The Worklist's .entrypoints is whatever was passed in
        # to the constructor.
        eq_([1,2,3], wl.entrypoints)

    def test_initialize_without_library(self):
        wl = WorkList()
        sf, ignore = Genre.lookup(self._db, "Science Fiction")
        romance, ignore = Genre.lookup(self._db, "Romance")

        # Create a WorkList that's associated with two genres.
        wl.initialize(None, genres=[sf, romance])
        wl.collection_ids = [self._default_collection.id]

        # There is no Library.
        eq_(None, wl.get_library(self._db))

        # The Genres associated with the WorkList are the ones passed
        # in on the constructor.
        eq_(set(wl.genre_ids),
            set([x.id for x in [sf, romance]]))

    def test_top_level_for_library(self):
        """Test the ability to generate a top-level WorkList."""
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
        wl = WorkList.top_level_for_library(self._db, self._default_library)
        eq_([lane1, lane2], wl.children)
        eq_(Edition.FULFILLABLE_MEDIA, wl.media)

        # The other library only has one top-level lane, so we use that lane.
        l = WorkList.top_level_for_library(self._db, other_library)
        eq_(other_library_lane, l)

        # This library has no lanes configured at all.
        no_config_library = self._library(
            name="No configuration Library", short_name="No config"
        )
        wl = WorkList.top_level_for_library(self._db, no_config_library)
        eq_([], wl.children)
        eq_(Edition.FULFILLABLE_MEDIA, wl.media)


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

    def test_groups_propagates_facets(self):
        """Verify that the Facets object passed into groups() is
        propagated to the methods called by groups().
        """
        class MockWorkList(WorkList):

            def featured_works(self, _db, facets):
                self.featured_called_with = facets
                return []

            def _groups_for_lanes(self, _db, relevant_children, relevant_lanes, facets):
                self.groups_called_with = facets
                return []

        mock = MockWorkList()
        mock.initialize(library=self._default_library)
        facets = object()
        [x for x in mock.groups(self._db, facets=facets)]
        eq_(facets, mock.groups_called_with)

        [x for x in mock.groups(self._db, facets=facets, include_sublanes=False)]
        eq_(facets, mock.featured_called_with)

    def test_featured_works(self):
        wl = MockWorks()
        self._default_library.setting(Library.FEATURED_LANE_SIZE).value = "10"
        wl.initialize(library=self._default_library)

        w1 = MockWork(1)

        # Set the underlying 'query' to return the same work twice.
        # This can happen in real life. For instance, if a lane is
        # based on a number of CustomLists, and a single work is
        # featured on one CustomList but not featured on another, the
        # query will find the same work with two different quality
        # scores.
        wl.queue_works([w1, w1])

        # We asked for 10 works, the query returned two, but there was
        # a duplicate, so we ended up with one.
        featured = wl.featured_works(self._db)
        eq_([w1], featured)

        # We created a FeaturedFacets object and passed it in to works().
        [(facets, pagination, featured)] = wl.works_calls
        eq_(self._default_library.minimum_featured_quality,
            facets.minimum_featured_quality)
        eq_(False, facets.uses_customlists)

        # We then called random_sample() on the results.
        [(query, target_size)] = wl.random_sample_calls
        eq_([w1, w1], query)
        eq_(self._default_library.featured_lane_size, target_size)

    def test_methods_that_call_works_propagate_entrypoint(self):
        """Verify that the EntryPoint mentioned in the Facets object passed
        into featured_works() and works_in_window() is propagated when
        those methods call works().
        """
        class Mock(WorkList):
            def works(self, _db, *args, **kwargs):
                self.works_called_with = kwargs['facets']
                # This query won't work, but we need to return some
                # kind of query so works_in_window can complete.
                return _db.query(Work)

            def _restrict_query_to_window(self, query, target_size):
                return query

        wl = Mock()
        wl.initialize(library=self._default_library)
        audio = AudiobooksEntryPoint
        facets = FeaturedFacets(0, entrypoint=audio)

        # The Facets object passed in to works() is different from the
        # one we passed in -- it's got some settings for
        # minimum_featured_quality and uses_customlists which we
        # didn't bother to provide -- but the EntryPoint we did provide
        # is propagated.
        wl.featured_works(self._db, facets=facets)
        eq_(audio, wl.works_called_with.entrypoint)

        wl.works_called_with = None
        wl.works_in_window(self._db, facets, 10)
        eq_(audio, wl.works_called_with.entrypoint)

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
            def apply_filters(self, _db, qu, *args, **kwargs):
                from model import MaterializedWorkWithGenre as mwg
                return qu.filter(mwg.sort_title=='Oliver Twist')

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

        # A WorkList can also have a collection with no library.
        wl = WorkList()
        wl.initialize(None)
        wl.collection_ids = [self._default_collection.id]
        eq_(2, wl.works(self._db).count())

    def test_works_propagates_facets(self):
        """Verify that the Facets object passed into works() is
        propagated to the methods called by works().
        """
        class Mock(WorkList):
            def apply_filters(self, _db, qu, facets, pagination):
                self.apply_filters_called_with = facets
        wl = Mock()
        wl.initialize(self._default_library)
        facets = FacetsWithEntryPoint()
        wl.works(self._db, facets=facets)
        eq_(facets, wl.apply_filters_called_with)

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

            def only_show_ready_deliverable_works(
                    self, _db, query, *args, **kwargs
            ):
                called['only_show_ready_deliverable_works'] = True
                return query

            def bibliographic_filter_clause(
                    self, _db, query, featured
            ):
                called['apply_bibliographic_filters'] = True
                called['apply_bibliographic_filters.featured'] = featured
                return query, None

        class MockFacets(object):
            def apply(self, _db, query):
                called['facets.apply'] = True
                return query

        class MockPagination(object):
            def apply(self, query):
                called['pagination.apply'] = True
                return query

        from model import MaterializedWorkWithGenre as work_model
        original_qu = self._db.query(work_model)
        wl = MockWorkList()
        final_qu = wl.apply_filters(
            self._db, original_qu, MockFacets(),
            MockPagination()
        )

        # The hook methods were called with the right arguments.
        eq_(called['only_show_ready_deliverable_works'], True)
        eq_(called['apply_bibliographic_filters'], True)
        eq_(called['facets.apply'], True)
        eq_(called['pagination.apply'], True)

        eq_(called['apply_bibliographic_filters.featured'], False)

        # We mocked everything that might have changed the final query,
        # and the end result was the query wasn't modified.
        eq_(original_qu, final_qu)

        # Test that apply_filters() makes a query distinct if there is
        # no Facets object to do the job.
        called = dict()
        distinct_qu = wl.apply_filters(self._db, original_qu, None, None)
        eq_(str(original_qu.distinct(work_model.works_id)), str(distinct_qu))
        assert 'facets.apply' not in called
        assert 'pagination.apply' not in called

        # If a Facets is passed into apply_filters, the query
        # is passed into the Facets.apply() method.
        class MockFacets(object):
            def apply(self, _db, qu):
                self.called_with = qu
                return qu
        facets = MockFacets()
        wl.apply_filters(self._db, original_qu, facets, None)
        # The query was modified by the time it was passed in, so it's
        # not the same as original_qu, but all we need to check is that
        # _some_ query was passed in.
        assert isinstance(facets.called_with, type(original_qu))

    def test_apply_bibliographic_filters_short_circuits_apply_filters(self):
        class MockWorkList(WorkList):
            """Mock WorkList whose bibliographic_filter_clause implementation
            believes the WorkList should not exist at all.
            """

            def bibliographic_filter_clause(
                    self, _db, query, featured
            ):
                return None, None

        wl = MockWorkList()
        wl.initialize(self._default_library)
        from model import MaterializedWorkWithGenre as mwg
        qu = self._db.query(mwg)
        eq_(None, wl.apply_filters(self._db, qu, None, None))

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

            def audience_filter_clauses(self, _db, qu):
                called['apply_audience_filter'] = True
                return []

        wl = MockWorkList()
        from model import MaterializedWorkWithGenre as wg
        original_qu = self._db.query(wg)

        # If no languages or genre IDs are specified, and the hook
        # methods do nothing, then bibliographic_filter_clause() has
        # no effect.
        featured_object = object()
        final_qu, bibliographic_filter = wl.bibliographic_filter_clause(
            self._db, original_qu, featured_object
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
            qu, clause = worklist.bibliographic_filter_clause(
                self._db, original_qu, False
            )
            qu = qu.filter(clause)
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
        self.add_to_materialized_view(
            [gutenberg_children, non_gutenberg_children, gutenberg_adult]
        )

        def for_audiences(*audiences):
            """Invoke WorkList.apply_audience_clauses using the given
            `audiences`, and return all the matching Work objects.
            """
            wl = WorkList()
            wl.audiences = audiences
            from model import MaterializedWorkWithGenre as work_model
            qu = self._db.query(work_model).join(work_model.license_pool)
            clauses = wl.audience_filter_clauses(self._db, qu)
            if clauses:
                qu = qu.filter(and_(*clauses))
            return [x.works_id for x in qu.all()]

        eq_([gutenberg_adult.id], for_audiences(Classifier.AUDIENCE_ADULT))

        # The Gutenberg "children's" book is filtered out because it we have
        # no guarantee it is actually suitable for children.
        eq_([non_gutenberg_children.id],
            for_audiences(Classifier.AUDIENCE_CHILDREN))

        # This can sometimes lead to unexpected results, but the whole
        # thing is a hack and needs to be improved anyway.
        eq_([non_gutenberg_children.id],
            for_audiences(Classifier.AUDIENCE_ADULT,
                          Classifier.AUDIENCE_CHILDREN))

        # If no particular audiences are specified, no books are filtered.
        eq_(set([gutenberg_adult.id, gutenberg_children.id,
                 non_gutenberg_children.id]),
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

        class MockWorkList(WorkList):
            def customlist_ids(self):
                """WorkList.customlist_ids returns an empty list; we
                want to return something specific so we can make sure
                the results are passed into search().
                """
                return ["a customlist id"]

        # Create a WorkList that has very specific requirements.
        wl = MockWorkList()
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
            self._db, work.title, search_client, pagination=pagination,
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
        eq_(wl.customlist_ids, kw['on_any_of_these_lists'])
        eq_(1, kw['size'])
        eq_(0, kw['offset'])

        # The single search result was converted to a MaterializedWorkWithGenre.
        [result] = results
        from model import MaterializedWorkWithGenre as mwg
        assert isinstance(result, mwg)
        eq_(work.id, result.works_id)

        # Test that language and media are passed in
        languages = ["fre"]
        media = ["audiobook"]
        results = wl.search(
            self._db, work.title, search_client, media, pagination, languages
        )
        [query, second_query] = search_client.queries
        [fixed, kw] = second_query
        # lane languages should take preference over user entered languages
        eq_(["eng", "spa"], kw["languages"])
        eq_(media, kw["media"])

        # pass all media
        media = Edition.ALL_MEDIUM
        results = wl.search(
            self._db, work.title, search_client, media, pagination, languages
        )
        [query, second_query, third_query] = search_client.queries
        [fixed, kw] = third_query
        eq_(None, kw["media"])

        # If a Facets object is passed into search(), and the Facets
        # object has an EntryPoint set, a subset of search arguments
        # are passed into EntryPoint.modified_search_arguments().
        class MockEntryPoint(object):
            def modified_search_arguments(self, **kwargs):
                self.called_with = dict(kwargs)
                return kwargs
        entrypoint = MockEntryPoint()
        facets = SearchFacets(entrypoint=entrypoint)
        wl.search(self._db, work.title, search_client, facets=facets)

        # Arguments relevant to the EntryPoint's view of the
        # collection were passed in...
        for i in ['audiences', 'fiction', 'in_any_of_these_genres', 'languages', 'media', 'target_age']:
            assert i in entrypoint.called_with

        # Arguments pertaining to the search query or result
        # navigation were not.
        for i in ['size', 'query_string', 'offset']:
            assert i not in entrypoint.called_with


class TestLane(DatabaseTest):

    def test_get_library(self):
        lane = self._lane()
        eq_(self._default_library, lane.get_library(self._db))

    def test_set_audiences(self):
        """Setting Lane.audiences to a single value will
        auto-convert it into a list containing one value.
        """
        lane = self._lane()
        lane.audiences = Classifier.AUDIENCE_ADULT
        eq_([Classifier.AUDIENCE_ADULT], lane.audiences)

    def test_update_size(self):
        # One work in two subgenres of fiction.
        work = self._work(fiction=True, with_license_pool=True,
                          genre="Science Fiction")
        romance, ignore = Genre.lookup(self._db, "Romance")
        work.genres.append(romance)

        # The 'Fiction' lane has one book.
        fiction = self._lane(display_name="Fiction", fiction=True)

        # But the materialized view contains the book twice -- once under
        # Science Fiction and once under Romance.
        self.add_to_materialized_view([work])

        # update_size() sets the Lane's size to the correct number.
        fiction.size = 100
        fiction.update_size(self._db)
        eq_(1, fiction.size)

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
        grandchild_lane = self._lane(parent=child_lane)
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
            "%s / %s / %s / %s" % (
                lane.library.short_name, lane.display_name,
                child_lane.display_name, grandchild_lane.display_name
            ),
            grandchild_lane.full_identifier
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

    def test_entrypoints(self):
        """Currently a Lane can never have entrypoints."""
        eq_([], self._lane().entrypoints)

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

    def test_customlist_ids(self):
        # WorkLists always return None for customlist_ids.
        wl = WorkList()
        wl.initialize(self._default_library)
        eq_(None, wl.customlist_ids)

        # When you add a CustomList to a Lane, you are saying that works
        # from that CustomList can appear in the Lane.
        nyt1, ignore = self._customlist(
            num_entries=0, data_source_name=DataSource.NYT
        )
        nyt2, ignore = self._customlist(
            num_entries=0, data_source_name=DataSource.NYT
        )

        no_lists = self._lane()
        eq_(None, no_lists.customlist_ids)

        has_list = self._lane()
        has_list.customlists.append(nyt1)
        eq_([nyt1.id], has_list.customlist_ids)

        # When you set a Lane's list_datasource, you're saying that
        # works appear in the Lane if they are on _any_ CustomList from
        # that data source.
        has_list_source = self._lane()
        has_list_source.list_datasource = DataSource.lookup(
            self._db, DataSource.NYT
        )
        eq_(set([nyt1.id, nyt2.id]), set(has_list_source.customlist_ids))

        # If there are no CustomLists from that data source, an empty
        # list is returned.
        has_no_lists = self._lane()
        has_no_lists.list_datasource = DataSource.lookup(
            self._db, DataSource.OVERDRIVE
        )
        eq_([], has_no_lists.customlist_ids)

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
            self._db, work.title, search_client, pagination=pagination
        )
        target_results = lane.search_target.search(
            self._db, work.title, search_client, pagination=pagination
        )
        eq_(results, target_results)

        # The single search result was converted to a MaterializedWorkWithGenre.
        [result] = results
        from model import MaterializedWorkWithGenre as mwg
        assert isinstance(result, mwg)
        eq_(work.id, result.works_id)

        # This still works if the lane is its own search_target.
        lane.root_for_patron_type = ["A"]
        results = lane.search(
            self._db, work.title, search_client, pagination=pagination
        )
        target_results = lane.search_target.search(
            self._db, work.title, search_client, pagination=pagination
        )
        eq_(results, target_results)

    def test_search_propagates_facets(self):
        """Lane.search propagates facets when calling search() on
        its search target.
        """
        class Mock(object):
            def search(self, *args, **kwargs):
                self.called_with = kwargs['facets']
        mock = Mock()
        lane = self._lane()

        old_lane_search_target = Lane.search_target
        old_wl_search = WorkList.search
        Lane.search_target = mock
        facets = SearchFacets()
        lane.search(self._db, "query", None, facets=facets)
        eq_(facets, mock.called_with)

        # Now try the case where a lane is its own search target.  The
        # Facets object is propagated to the WorkList.search().
        mock.called_with = None
        Lane.search_target = lane
        WorkList.search = mock.search
        lane.search(self._db, "query", None, facets=facets)
        eq_(facets, mock.called_with)

        # Restore methods that were mocked.
        Lane.search_target = old_lane_search_target
        WorkList.search = old_wl_search

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

        def match_works(lane, works, featured=False,
                        expect_bibliographic_filter=True):
            """Verify that calling apply_bibliographic_filters to the given
            lane yields the given list of works.
            """
            from model import MaterializedWorkWithGenre as mwg
            base_query = self._db.query(mwg).join(
                LicensePool, mwg.license_pool_id==LicensePool.id
            )
            new_query, bibliographic_clause = lane.bibliographic_filter_clause(
                self._db, base_query, featured
            )

            if lane.uses_customlists:
                # bibliographic_filter_clause modifies the query (by
                # calling customlist_filter_clauses).
                assert base_query != new_query

            # The query will also be modified if a lane includes genre
            # restrictions and also inherits genre restrictions from
            # its parent, but we don't have a good way of seeing
            # whether that happened.

            if expect_bibliographic_filter:
                # There must be some kind of bibliographic filter.
                assert bibliographic_clause is not None
                final_query = new_query.filter(bibliographic_clause)
            else:
                # There must *not* be some kind of bibliographic filter.
                assert bibliographic_clause is None
                final_query = new_query
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
        match_works(both_lane, [childrens_fiction, nonfiction],
                    expect_bibliographic_filter=False)

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

        # The materialized view must be refreshed for the changes to
        # list membership to take effect.
        self.add_to_materialized_view([childrens_fiction, nonfiction])

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
        best_selling_classics.inherit_parent_restrictions = False

        SessionManager.refresh_materialized_views(self._db)
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

        # Parent restrictions based on genre can also be inherited.
        #

        # Here's a lane that finds only short stories.
        short_stories, ignore = Genre.lookup(self._db, "Short Stories")
        short_stories_lane = self._lane(genres=["Short Stories"])

        # Here's a child of that lane, which contains science fiction.
        sf, ignore = Genre.lookup(self._db, "Science Fiction")
        sf_lane = self._lane(genres=[sf], parent=short_stories_lane)

        # Without the parent restriction in place, all science fiction
        # shows up in sf_lane.
        sf_lane.inherit_parent_restrictions = False
        sf_short = self._work(with_license_pool=True)
        sf_short.genres.append(sf)
        self.add_to_materialized_view(sf_short)
        from model import MaterializedWorkWithGenre as work_model
        match_works(sf_lane, [sf_short])

        # With the parent restriction in place, a book must be classified
        # under both science fiction and short stories to show up.
        sf_lane.inherit_parent_restrictions = True
        match_works(sf_lane, [])
        sf_short.genres.append(short_stories)
        match_works(sf_lane, [sf_short])

    def test_bibliographic_filter_clause_no_restrictions(self):
        """A lane that matches every single book has no bibliographic
        filter clause.
        """
        lane = self._lane()
        from model import MaterializedWorkWithGenre as work_model
        qu = self._db.query(work_model)
        eq_(
            (qu, None),
            lane.bibliographic_filter_clause(self._db, qu, False, False)
        )

    def test_bibliographic_filter_clause_medium_restriction(self):
        book = self._work(fiction=False, with_license_pool=True)
        eq_(Edition.BOOK_MEDIUM, book.presentation_edition.medium)
        lane = self._lane()
        self.add_to_materialized_view([book])

        from model import MaterializedWorkWithGenre as work_model
        def matches(lane):
            qu = self._db.query(work_model)
            new_qu, bib_filter = lane.bibliographic_filter_clause(
                self._db, qu, False
            )
            eq_(new_qu, qu)
            return [x.works_id for x in new_qu.filter(bib_filter)]

        # This lane only includes ebooks, and it has one item.
        lane.media = [Edition.BOOK_MEDIUM]
        eq_([book.id], matches(lane))

        # This lane only includes audiobooks, and it's empty
        lane.media = [Edition.AUDIO_MEDIUM]
        eq_([], matches(lane))

    def test_age_range_filter_clauses(self):
        """Standalone test of age_range_filter_clauses().
        """
        def filtered(lane):
            """Build a query that applies the given lane's age filter to the
            works table.
            """
            from model import MaterializedWorkWithGenre as work_model
            qu = self._db.query(work_model)
            clauses = lane.age_range_filter_clauses()
            if clauses:
                qu = qu.filter(and_(*clauses))
            return [x.works_id for x in qu]

        adult = self._work(
            title="For adults",
            audience=Classifier.AUDIENCE_ADULT,
            with_license_pool=True,
        )
        eq_(None, adult.target_age)
        fourteen_or_fifteen = self._work(
            title="For teens",
            audience=Classifier.AUDIENCE_YOUNG_ADULT,
            with_license_pool=True,
        )
        fourteen_or_fifteen.target_age = tuple_to_numericrange((14,15))

        # This lane contains the YA book because its age range overlaps
        # the age range of the book.
        younger_ya = self._lane()
        younger_ya.target_age = (12,14)
        self.add_to_materialized_view([adult, younger_ya])
        eq_([fourteen_or_fifteen.id], filtered(younger_ya))

        # This lane contains no books because it skews too old for the YA
        # book, but books for adults are not allowed.
        older_ya = self._lane()
        older_ya.target_age = (16,17)
        self.add_to_materialized_view([older_ya])
        eq_([], filtered(older_ya))

        # Expand it to include books for adults, and the adult book
        # shows up despite having no target age at all.
        older_ya.target_age = (16,18)
        eq_([adult.id], filtered(older_ya))

    def test_customlist_filter_clauses(self):
        """Standalone test of apply_customlist_filter.

        Some of this code is also tested by test_apply_custom_filters.
        """

        # If a lane has nothing to do with CustomLists,
        # apply_customlist_filter does nothing.
        no_lists = self._lane()
        qu = self._db.query(Work)
        new_qu, clauses = no_lists.customlist_filter_clauses(qu)
        eq_(qu, new_qu)
        eq_([], clauses)

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
        self.add_to_materialized_view([work])

        from model import MaterializedWorkWithGenre as work_model
        def _run(qu, clauses):
            # Run a query with certain clauses and pick out the
            # work IDs returned.
            modified = qu.filter(and_(*clauses))
            return [x.works_id for x in modified]

        def results(lane=gutenberg_lists_lane, must_be_featured=False):
            qu = self._db.query(work_model)
            new_qu, clauses = lane.customlist_filter_clauses(
                qu, must_be_featured=must_be_featured
            )

            if must_be_featured or lane.list_seen_in_previous_days:
                # The query comes out different than it goes in -- there's a
                # new join against CustomListEntry.
                assert new_qu != qu
            return _run(new_qu, clauses)

        # Both lanes contain the work.
        eq_([work.id], results(gutenberg_list_lane))
        eq_([work.id], results(gutenberg_lists_lane))

        # If there's another list with the same work on it, the
        # work only shows up once.
        gutenberg_list_2, ignore = self._customlist(num_entries=0)
        gutenberg_list_2_entry, ignore = gutenberg_list_2.add_entry(work)
        gutenberg_list_lane.customlists.append(gutenberg_list)
        eq_([work.id], results(gutenberg_list_lane))

        # This lane gets every work on a list associated with Overdrive.
        # There are no such lists, so the lane is empty.
        overdrive = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        overdrive_lists_lane = self._lane()
        overdrive_lists_lane.list_datasource = overdrive
        eq_([], results(overdrive_lists_lane))

        # It's possible to restrict a lane so that only works that are
        # _featured_ on a list show up. The work isn't featured, so it
        # doesn't show up.
        eq_([], results(must_be_featured=True))

        # Now it's featured, and it shows up.
        gutenberg_list_entry.featured = True
        eq_([work.id], results(must_be_featured=True))

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
        eq_([work.id], results())

        # Now let's test what happens when we chain calls to this
        # method.
        gutenberg_list_2_lane = self._lane()
        gutenberg_list_2_lane.customlists.append(gutenberg_list_2)

        # These two lines aren't necessary for the test but they
        # illustrate how this would happen in a real scenario -- When
        # determining which works belong in the child lane,
        # customlist_filter_clauses() will be called on the parent
        # lane and then on the child. We only want books that are
        # on _both_ gutenberg_list and gutenberg_list_2.
        gutenberg_list_2_lane.parent = gutenberg_list_lane
        gutenberg_list_2_lane.inherit_parent_restrictions = True

        qu = self._db.query(work_model)
        list_1_qu, list_1_clauses = gutenberg_list_lane.customlist_filter_clauses(qu)

        # The query has been modified to indicate that we are filtering
        # on the materialized view's customlist_id field.
        eq_(True, list_1_qu.customlist_id_filtered)
        eq_([work.id], [x.works_id for x in list_1_qu])

        # Now call customlist_filter_clauses again so that the query
        # must only match books on _both_ lists. This simulates
        # what happens when the second lane is a child of the first,
        # and inherits its restrictions.
        both_lists_qu, list_2_clauses = gutenberg_list_2_lane.customlist_filter_clauses(
            list_1_qu,
        )
        both_lists_clauses = list_1_clauses + list_2_clauses

        # The combined query matches the work that shows up on
        # both lists.
        eq_([work.id], _run(both_lists_qu, both_lists_clauses))

        # If we remove `work` from either list, the combined query
        # matches nothing. This works even though the materialized
        # view has not been refreshed.
        for l in [gutenberg_list, gutenberg_list_2]:
            l.remove_entry(work)
            eq_([], _run(both_lists_qu, both_lists_clauses))
            l.add_entry(work)

    def test_explain(self):
        parent = self._lane(display_name="Parent")
        parent.priority = 1
        child = self._lane(parent=parent, display_name="Child")
        child.priority = 2
        data = parent.explain()
        eq_(['ID: %s' % parent.id,
             'Library: %s' % self._default_library.short_name,
             'Priority: 1',
             'Display name: Parent',
        ],
            data
        )

        data = child.explain()
        eq_(['ID: %s' % child.id,
             'Library: %s' % self._default_library.short_name,
             'Parent ID: %s (Parent)' % parent.id,
             'Priority: 2',
             'Display name: Child',
        ],
            data
        )

    def test_groups_propagates_facets(self):
        """Lane.groups propagates a received Facets object into
        _groups_for_lanes.
        """
        def mock(self, _db, relevant_lanes, queryable_lanes, facets):
            self.called_with = facets
            return []
        old_value = Lane._groups_for_lanes
        Lane._groups_for_lanes = mock
        lane = self._lane()
        facets = FeaturedFacets(0)
        lane.groups(self._db, facets=facets)
        eq_(facets, lane.called_with)
        Lane._groups_for_lanes = old_value


class TestWorkListGroups(DatabaseTest):
    """Tests of WorkList.groups() and the helper methods."""

    def setup(self):
        super(TestWorkListGroups, self).setup()

        # Make sure random selections and range generations go the
        # same way every time.
        random.seed(42)

    def test_groups(self):
        """A comprehensive test of WorkList.groups()"""
        def _w(**kwargs):
            """Helper method to create a work with license pool."""
            return self._work(with_license_pool=True, **kwargs)

        # In this library, the groups feed includes at most two books
        # for each lane.
        library = self._default_library
        library.setting(library.FEATURED_LANE_SIZE).value = "2"

        # Create eight works.
        hq_litfic = _w(title="HQ LitFic", fiction=True, genre='Literary Fiction')
        hq_litfic.quality = 0.8
        lq_litfic = _w(title="LQ LitFic", fiction=True, genre='Literary Fiction')
        lq_litfic.quality = 0
        hq_sf = _w(title="HQ SF", genre="Science Fiction", fiction=True)
        hq_sf.random = 0.25

        # Add a lot of irrelevant genres to one of the works. This
        # will clutter up the materialized view, but it won't affect
        # the results.
        for genre in ['Westerns', 'Horror', 'Erotica']:
            genre_obj, is_new = Genre.lookup(self._db, genre)
            get_one_or_create(self._db, WorkGenre, work=hq_sf, genre=genre_obj)

        hq_sf.quality = 0.8
        mq_sf = _w(title="MQ SF", genre="Science Fiction", fiction=True)
        mq_sf.quality = 0.6
        lq_sf = _w(title="LQ SF", genre="Science Fiction", fiction=True)
        lq_sf.quality = 0.1
        hq_ro = _w(title="HQ Romance", genre="Romance", fiction=True)
        hq_ro.quality = 0.8
        hq_ro.random = 0.75
        mq_ro = _w(title="MQ Romance", genre="Romance", fiction=True)
        mq_ro.quality = 0.6
        lq_ro = _w(title="LQ Romance", genre="Romance", fiction=True)
        lq_ro.quality = 0.1
        nonfiction = _w(title="Nonfiction", fiction=False)

        # One of these works (mq_sf) is a best-seller and also a staff
        # pick.
        best_seller_list, ignore = self._customlist(num_entries=0)
        best_seller_list.add_entry(mq_sf)

        staff_picks_list, ignore = self._customlist(num_entries=0)
        staff_picks_list.add_entry(mq_sf)

        # Create a 'Fiction' lane with five sublanes.
        fiction = self._lane("Fiction")
        fiction.fiction = True

        # "Best Sellers", which will contain one book.
        best_sellers = self._lane(
            "Best Sellers", parent=fiction
        )
        best_sellers.customlists.append(best_seller_list)

        # "Staff Picks", which will contain the same book.
        staff_picks = self._lane(
            "Staff Picks", parent=fiction
        )
        staff_picks.customlists.append(staff_picks_list)

        # "Science Fiction", which will contain two books (but
        # will not contain the best-seller).
        sf_lane = self._lane(
            "Science Fiction", parent=fiction, genres=["Science Fiction"]
        )

        # "Romance", which will contain two books.
        romance_lane = self._lane(
            "Romance", parent=fiction, genres=["Romance"]
        )

        # "Discredited Nonfiction", which contains a book that would
        # not normally appear in 'Fiction'.
        discredited_nonfiction = self._lane(
            "Discredited Nonfiction", fiction=False,
            parent=fiction
        )
        discredited_nonfiction.inherit_parent_restrictions = False

        self.add_to_materialized_view(
            [hq_sf, mq_sf, lq_sf, hq_ro, mq_ro, lq_ro, hq_litfic, lq_litfic,
             nonfiction]
        )

        def assert_contents(g, expect):
            """Assert that a generator yields the expected
            (MaterializedWorkWithGenre, lane) 2-tuples.
            """
            results = list(g)
            expect = [
                (x[0].sort_title, x[1].display_name) for x in expect
            ]
            actual = [
                (x[0].sort_title, x[1].display_name) for x in results
            ]
            for i, expect_item in enumerate(expect):
                if i >= len(actual):
                    actual_item = None
                else:
                    actual_item = actual[i]
                eq_(
                    expect_item, actual_item,
                    "Mismatch in position %d: Expected %r, got %r.\nOverall, expected:\n%r\nGot:\n%r:" %
                    (i, expect_item, actual_item,
                     expect, actual)
                )
            eq_(len(expect), len(actual),
               "Expect matches actual, but actual has extra members.\nOverall, expected:\n%r\nGot:\n%r:" %
               (expect, actual)
            )

        fiction.groups(self._db)
        assert_contents(
            fiction.groups(self._db),
            [
                # The lanes based on lists feature every title on the
                # list.  This isn't enough to pad out the lane to
                # FEATURED_LANE_SIZE, but nothing else belongs in the
                # lane.
                (mq_sf, best_sellers),

                # In fact, both lanes feature the same title -- this
                # generally won't happen but it can happen when
                # multiple lanes are based on lists that feature the
                # same title.
                (mq_sf, staff_picks),

                # The genre-based lanes contain FEATURED_LANE_SIZE
                # (two) titles each. The 'Science Fiction' lane
                # features a middle-quality work that was already
                # featured above in a list, even though there's a
                # low-quality work that could have been used
                # instead. Each lane query has its own LIMIT applied,
                # so we didn't even see the low-quality work.
                (hq_sf, sf_lane),
                (mq_sf, sf_lane),
                (hq_ro, romance_lane),
                (mq_ro, romance_lane),

                # The 'Discredited Nonfiction' lane contains a single
                # book. There just weren't enough matching books to fill
                # out the lane to FEATURED_LANE_SIZE.
                (nonfiction, discredited_nonfiction),

                # The 'Fiction' lane contains a title that fits in the
                # fiction lane but was not classified under any other
                # lane. It also contains a title that was previously
                # featured earlier. There's a low-quality litfic title
                # in the database, but we didn't see it because the
                # 'Fiction' query had a LIMIT applied to it.
                (hq_litfic, fiction),
                (hq_ro, fiction),
            ]
        )

        # If we ask only about 'Fiction', not including its sublanes,
        # we get the same books associated with that lane.
        #
        # hq_ro shows up before hq_litfic because its .random is a
        # larger number. In the previous example, hq_ro showed up
        # after hq_litfic because we knew we'd already shown hq_ro in
        # a previous lane.
        assert_contents(
            fiction.groups(self._db, include_sublanes=False),
            [(hq_ro, fiction), (hq_litfic, fiction)]
        )

        # If we exclude 'Fiction' from its own grouped feed, we get
        # all the other books/lane combinations except for the books
        # associated directly with 'Fiction'.
        fiction.include_self_in_grouped_feed = False
        assert_contents(
            fiction.groups(self._db),
            [
                (mq_sf, best_sellers),
                (mq_sf, staff_picks),
                (hq_sf, sf_lane),
                (mq_sf, sf_lane),
                (hq_ro, romance_lane),
                (mq_ro, romance_lane),
                (nonfiction, discredited_nonfiction),
            ]
        )
        fiction.include_self_in_grouped_feed = True

        # When a lane has no sublanes, its behavior is the same whether
        # it is called with include_sublanes true or false.
        for include_sublanes in (True, False):
            assert_contents(
                discredited_nonfiction.groups(
                    self._db, include_sublanes=include_sublanes
                ),
                [(nonfiction, discredited_nonfiction)]
            )

        # If we make the lanes thirstier for content, we see slightly
        # different behavior.
        library.setting(library.FEATURED_LANE_SIZE).value = "3"
        assert_contents(
            fiction.groups(self._db),
            [
                # The list-based lanes are the same as before.
                (mq_sf, best_sellers),
                (mq_sf, staff_picks),

                # After using every single science fiction work that
                # wasn't previously used, we reuse mq_sf to pad the
                # "Science Fiction" lane up to three items. It's
                # better to have lq_sf show up before mq_sf, even
                # though it's lower quality, because lq_sf hasn't been
                # used before.
                (hq_sf, sf_lane),
                (lq_sf, sf_lane),
                (mq_sf, sf_lane),

                # The 'Romance' lane now contains all three Romance
                # titles, with the higher-quality titles first.
                (hq_ro, romance_lane),
                (mq_ro, romance_lane),
                (lq_ro, romance_lane),

                # The 'Discredited Nonfiction' lane is the same as
                # before.
                (nonfiction, discredited_nonfiction),

                # After using every single fiction work that wasn't
                # previously used, we reuse high-quality works to pad
                # the "Fiction" lane to three items. The
                # lowest-quality Romance title doesn't show up here
                # anymore, because the 'Romance' lane claimed it. If
                # we have to reuse titles, we'll reuse the
                # high-quality ones.
                (hq_litfic, fiction),
                (hq_sf, fiction),
                (hq_ro, fiction),
            ]
        )

        # Let's see how entry points affect the feeds.
        #

        # There are no audiobooks in the system, so passing in a
        # FeaturedFacets scoped to the AudiobooksEntryPoint excludes everything.
        facets = FeaturedFacets(0, entrypoint=AudiobooksEntryPoint)
        _db = self._db
        eq_([], list(fiction.groups(self._db, facets=facets)))

        # Here's an entry point that ignores everything except one
        # specific book.
        class LQRomanceEntryPoint(object):
            @classmethod
            def apply(cls, qu):
                from model import MaterializedWorkWithGenre as mv
                return qu.filter(mv.sort_title=='LQ Romance')
        facets = FeaturedFacets(0, entrypoint=LQRomanceEntryPoint)
        assert_contents(
            fiction.groups(self._db, facets=facets),
            [
                # The single recognized book shows up in both lanes
                # that can show it.
                (lq_ro, romance_lane),
                (lq_ro, fiction),
            ]
        )

        # Now, instead of relying on the 'Fiction' lane, make a
        # WorkList containing two different lanes, and call groups() on
        # the WorkList.

        class MockWorkList(object):

            display_name = "Mock"
            visible = True
            priority = 2

            def groups(self, _db, include_sublanes, facets=None):
                yield lq_litfic, self

        mock = MockWorkList()

        wl = WorkList()
        wl.initialize(
            self._default_library, children=[best_sellers, staff_picks, mock]
        )

        # We get results from the two lanes and from the MockWorkList.
        # Since the MockWorkList wasn't a lane, its results were obtained
        # by calling groups() recursively.
        assert_contents(
            wl.groups(self._db),
            [
                (mq_sf, best_sellers),
                (mq_sf, staff_picks),
                (lq_litfic, mock),
            ]
        )

    def test_groups_for_lanes_propagates_facets(self):
        class Mock(WorkList):
            def _featured_works_with_lanes(self, *args, **kwargs):
                self.featured_called_with = kwargs['facets']
                return []

        wl = Mock()
        wl.initialize(library=self._default_library)
        facets = FeaturedFacets(0)
        groups = list(wl._groups_for_lanes(self._db, [], [], facets))
        eq_(facets, wl.featured_called_with)

    def test_featured_works_propagates_facets(self):
        """featured_works uses facets when it calls works().
        """
        class Mock(WorkList):
            def works(self, _db, facets):
                self.works_called_with = facets
                return []

        wl = Mock()
        wl.initialize(library=self._default_library)
        facets = FeaturedFacets(
            minimum_featured_quality = object(),
            uses_customlists = object(),
            entrypoint=AudiobooksEntryPoint
        )
        groups = list(wl.featured_works(self._db, facets))
        eq_(facets, wl.works_called_with)

        # If no FeaturedFacets object is specified, one is created
        # based on default library configuration.
        groups = list(wl.featured_works(self._db, None))
        facets2 = wl.works_called_with
        eq_(self._default_library.minimum_featured_quality,
            facets2.minimum_featured_quality)
        eq_(wl.uses_customlists, facets2.uses_customlists)

    def test_featured_works_with_lanes(self):
        """_featured_works_with_lanes calls works_in_window on every lane
        pass in to it.
        """
        class Mock(object):
            """A Mock of Lane.works_in_window."""

            def __init__(self, mock_works):
                self.mock_works = mock_works

            def works_in_window(self, _db, facets, target_size):
                self.called_with = [_db, facets, target_size]
                return [self.mock_works]

        mock1 = Mock(("mw1","quality1"))
        mock2 = Mock(("mw2","quality2"))

        lane = self._lane()
        facets = FeaturedFacets(0.1)
        results = lane._featured_works_with_lanes(
            self._db, [mock1, mock2], facets
        )

        # The results of works_in_window were annotated with the
        # 'lane' that produced the result.
        eq_([('mw1', 'quality1', mock1), ('mw2', 'quality2', mock2)],
            list(results))

        # Each Mock's works_in_window was called with the same
        # arguments.
        eq_(mock1.called_with, mock2.called_with)

        # The Facets object passed in to _featured_works_with_lanes()
        # is passed on into works_in_window().
        _db, called_with_facets, target_size = mock1.called_with
        eq_(self._db, _db)
        eq_(facets, called_with_facets)
        eq_(lane.library.featured_lane_size, target_size)

    def test_featured_window(self):
        lane = self._lane()

        # Unless the lane has more items than we are asking for, the
        # 'window' spans the entire range from zero to one.
        eq_((0,1), lane.featured_window(1))
        lane.size = 99
        eq_((0,1), lane.featured_window(99))

        # Otherwise, the 'window' is a smaller, randomly selected range
        # between zero and one.
        lane.size = 6094
        start, end = lane.featured_window(17)
        expect_start = 0.025
        eq_(expect_start, start)
        eq_(round(start+0.014,8), end)

        # Given a lane with 6094 works, selecting works with .random
        # between 0.630 and 0.644 should give us about 85 items, which
        # is what we need to make it likely that we get 17 items of
        # featurable quality.
        width = (end-start)
        estimated_items = lane.size * width
        eq_(85, int(estimated_items))

        # Given a lane with one billion works, you'd expect the range
        # to be incredibly small. But the resolution of Works.random
        # is only three decimal places, so there's a limit on how
        # small the range can get.
        lane.size = 10**9
        start, end = lane.featured_window(10)
        assert end == start + 0.001


    def test_fill_parent_lane(self):

        class Mock(object):
            def __init__(self, works_id):
                self.works_id = works_id

            def __repr__(self):
                return self.works_id

        a = Mock("a")
        b = Mock("b")
        c = Mock("c")
        d = Mock("d")
        e = Mock("e")
        f = Mock("f")

        def fill(lane, additional_needed, unused_by_tier, used_by_tier,
              used_works=[]):
            mws = []
            used_ids = set([x.works_id for x in used_works])
            for mw, yielded_lane in lane._fill_parent_lane(
                    additional_needed, unused_by_tier, used_by_tier,
                    used_ids
            ):
                # The lane should always be the lane on which
                # _fill_parent_lane was called.
                eq_(yielded_lane, lane)
                mws.append(mw)
            return mws

        unused = { 10 : [a], 1 : [b]}
        used = { 10 : [c] }
        lane = self._lane()

        # If we don't ask for any works, we don't get any.
        eq_([], fill(lane, 0, unused, used))

        # If we ask for three or more, we get all three, with unused
        # prioritized over used and high-quality prioritized over
        # low-quality.
        eq_([a,b,c], fill(lane, 3, unused, used))
        eq_([a,b,c], fill(lane, 100, unused, used))

        # If one of the items in 'unused' is actually used,
        # it will be ignored. (TODO: it would make more sense
        # to treat it like the other 'used' items, but it doesn't
        # matter much in real usage.)
        eq_([b,c], fill(lane, 3, unused, used, set([a, c])))

        # TODO: If a work shows up multiple times in the 'used'
        # dictionary it can be reused multiple times -- basically once
        # we go into the 'used' dictionary we don't care how often we
        # reuse things. I don't think this matters in real usage.

        # Within a quality tier, works are given up in random order.
        unused = { 10 : [a, b, c], 1 : [d, e, f]}
        eq_([c,a,b, e,f,d], fill(lane, 6, unused, used))

    def test_restrict_query_to_window(self):
        lane = self._lane()

        from model import MaterializedWorkWithGenre as work_model
        query = self._db.query(work_model).filter(work_model.fiction==True)
        target_size = 10

        # If the lane is so small that windowing is not safe,
        # _restrict_query_to_window does nothing.
        lane.size = 1
        eq_(
            query,
            lane._restrict_query_to_window(query, target_size)
        )

        # If the lane size is small enough to window, then
        # _restrict_query_to_window adds restrictions on the .random
        # field.
        lane.size = 960
        modified = lane._restrict_query_to_window(query, target_size)

        # Check the SQL.
        sql = dump_query(modified)

        expect_lower = 0.606
        expect_upper = 0.658
        args = dict(mv=work_model.__table__.name, lower=expect_lower,
                    upper=expect_upper)

        assert '%(mv)s.fiction =' % args in sql
        expect_upper_range = '%(mv)s.random <= %(upper)s' % args
        assert expect_upper_range in sql

        expect_lower_range = '%(mv)s.random >= %(lower)s' % args
        assert expect_lower_range in sql

        # Those values came from featured_window(). If we call that
        # method ourselves we will get a different window of
        # approximately the same width.
        width = expect_upper-expect_lower
        new_lower, new_upper = lane.featured_window(target_size)
        eq_(round(width, 3), round(new_upper-new_lower, 3))
