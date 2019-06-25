import datetime
import json
import random
from nose.tools import (
    eq_,
    set_trace,
    assert_raises,
    assert_raises_regexp,
)

from . import (
    DatabaseTest,
)

from sqlalchemy.sql.elements import Case
from sqlalchemy import (
    and_,
    func,
)

from elasticsearch_dsl.function import (
    ScriptScore,
    RandomScore,
)

from elasticsearch.exceptions import ElasticsearchException

from ..classifier import Classifier

from ..entrypoint import (
    AudiobooksEntryPoint,
    EbooksEntryPoint,
    EverythingEntryPoint,
    EntryPoint,
)

from ..external_search import (
    Filter,
    MockExternalSearchIndex,
    WorkSearchResult,
)

from ..lane import (
    DatabaseBackedFacets,
    DatabaseBackedWorkList,
    FacetConstants,
    Facets,
    FacetsWithEntryPoint,
    FeaturedFacets,
    Pagination,
    SearchFacets,
    WorkList,
    Lane,
)

from ..model import (
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
from ..problem_details import INVALID_INPUT
from ..testing import EndToEndSearchTest

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
        # navigate creates a new FacetsWithEntryPoint.

        old_entrypoint = object()
        kwargs = dict(extra_key="extra_value")
        facets = FacetsWithEntryPoint(
            old_entrypoint, entrypoint_is_default=True, **kwargs
        )
        new_entrypoint = object()
        new_facets = facets.navigate(new_entrypoint)

        # A new FacetsWithEntryPoint was created.
        assert isinstance(new_facets, FacetsWithEntryPoint)

        # It has the new entry point.
        eq_(new_entrypoint, new_facets.entrypoint)

        # Since navigating from one Facets object to another is a choice,
        # the new Facets object is not using a default EntryPoint.
        eq_(False, new_facets.entrypoint_is_default)

        # The keyword arguments used to create the original faceting
        # object were propagated to its constructor.
        eq_(kwargs, new_facets.constructor_kwargs)

    def test_from_request(self):
        # from_request just calls the _from_request class method
        expect = object()
        class Mock(FacetsWithEntryPoint):
            @classmethod
            def _from_request(cls, *args, **kwargs):
                cls.called_with = (args, kwargs)
                return expect
        result = Mock.from_request(
            "library", "facet config", "get_argument",
            "get_header", "worklist", "default entrypoint",
            extra="extra argument"
        )

        # The arguments given to from_request were propagated to _from_request.
        args, kwargs = Mock.called_with
        eq_(("facet config", "get_argument",
             "get_header", "worklist", "default entrypoint"), args)
        eq_(dict(extra="extra argument"), kwargs)

        # The return value of _from_request was propagated through
        # from_request.
        eq_(expect, result)

    def test__from_request(self):
        # _from_request calls load_entrypoint() and instantiates the
        # class with the result.

        # Mock load_entrypoint() to return whatever value we have set up
        # ahead of time.

        class MockFacetsWithEntryPoint(FacetsWithEntryPoint):

            @classmethod
            def selectable_entrypoints(cls, facet_config):
                cls.selectable_entrypoints_called_with = facet_config
                return ["Selectable entrypoints"]

            @classmethod
            def load_entrypoint(cls, entrypoint_name, entrypoints, default=None):
                cls.load_entrypoint_called_with = (entrypoint_name, entrypoints, default)
                return cls.expect

        # Mock the functions that pull information out of an HTTP
        # request.

        # EntryPoint.load_entrypoint pulls the facet group name out of
        # the 'request' and passes it into load_entrypoint().
        def get_argument(key, default):
            eq_(key, Facets.ENTRY_POINT_FACET_GROUP_NAME)
            return "name of the entrypoint"

        # FacetsWithEntryPoint.load_entrypoint does not use
        # get_header().
        def get_header(name):
            raise Exception("I'll never be called")

        config = self.MockFacetConfig
        mock_worklist = object()

        # First, test failure. If load_entrypoint() returns a
        # ProblemDetail, that object is returned instead of the
        # faceting class.
        MockFacetsWithEntryPoint.expect = INVALID_INPUT
        eq_(
            MockFacetsWithEntryPoint.expect,
            MockFacetsWithEntryPoint._from_request(
                config, get_argument, get_header, mock_worklist,
                extra="extra kwarg"
            )
        )

        # Now, test success. If load_entrypoint() returns an object,
        # that object is passed as 'entrypoint' into the
        # FacetsWithEntryPoint constructor.
        #
        # The object returned by load_entrypoint() does not need to be a
        # currently enabled entrypoint for the library.
        expect_entrypoint = object()
        expect_is_default = object()
        MockFacetsWithEntryPoint.expect = (expect_entrypoint, expect_is_default)
        config = self.MockFacetConfig
        default_entrypoint = object()
        facets = MockFacetsWithEntryPoint._from_request(
            config, get_argument, get_header, mock_worklist,
            default_entrypoint=default_entrypoint, extra="extra kwarg"
        )
        assert isinstance(facets, FacetsWithEntryPoint)
        eq_(expect_entrypoint, facets.entrypoint)
        eq_(expect_is_default, facets.entrypoint_is_default)
        eq_(
            ("name of the entrypoint", ["Selectable entrypoints"], default_entrypoint),
            MockFacetsWithEntryPoint.load_entrypoint_called_with
        )
        eq_(dict(extra="extra kwarg"), facets.constructor_kwargs)
        eq_(MockFacetsWithEntryPoint.selectable_entrypoints_called_with, config)

    def test_load_entrypoint(self):
        audio = AudiobooksEntryPoint
        ebooks = EbooksEntryPoint

        # These are the allowable entrypoints for this site -- we'll
        # be passing this in to load_entrypoint every time.
        entrypoints = [audio, ebooks]

        worklist = object()
        m = FacetsWithEntryPoint.load_entrypoint

        # This request does not ask for any particular entrypoint, and
        # it doesn't specify a default, so it gets the first available
        # entrypoint.
        audio_default, is_default = m(None, entrypoints)
        eq_(audio, audio_default)
        eq_(True, is_default)

        # This request does not ask for any particular entrypoint, so
        # it gets the specified default.
        default = object()
        eq_((default, True), m(None, entrypoints, default))

        # This request asks for an entrypoint and gets it.
        eq_((ebooks, False), m(ebooks.INTERNAL_NAME, entrypoints))

        # This request asks for an entrypoint that is not available,
        # and gets the default.
        eq_((audio, True), m("no such entrypoint", entrypoints))

        # If no EntryPoints are available, load_entrypoint returns
        # nothing.
        eq_((None, True), m(audio.INTERNAL_NAME, []))

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

    def test_modify_search_filter(self):

        # When an entry point is selected, search filters are modified so
        # that they only find works that fit that entry point.
        filter = Filter()
        facets = FacetsWithEntryPoint(AudiobooksEntryPoint)
        facets.modify_search_filter(filter)
        eq_([Edition.AUDIO_MEDIUM], filter.media)

        # If no entry point is selected, the filter is not modified.
        filter = Filter()
        facets = FacetsWithEntryPoint()
        facets.modify_search_filter(filter)
        eq_(None, filter.media)


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

    def test_default(self):
        # Calling Facets.default() is like calling the constructor with
        # no arguments except the library.
        class Mock(Facets):
            def __init__(self, library, **kwargs):
                self.library = library
                self.kwargs = kwargs
        facets = Mock.default(self._default_library)
        eq_(self._default_library, facets.library)
        eq_(dict(collection=None, availability=None, order=None),
            facets.kwargs)

    def test_default_availability(self):

        # Normally, the availability will be the library's default availability
        # facet.
        test_enabled_facets = {
            Facets.ORDER_FACET_GROUP_NAME : [Facets.ORDER_WORK_ID],
            Facets.COLLECTION_FACET_GROUP_NAME : [Facets.COLLECTION_FULL],
            Facets.AVAILABILITY_FACET_GROUP_NAME : [Facets.AVAILABLE_ALL, Facets.AVAILABLE_NOW],
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
        facets = Facets(library, None, None, None)
        eq_(Facets.AVAILABLE_ALL, facets.availability)

        # However, if the library does not allow holds, we only show
        # books that are currently available.
        library.setting(Library.ALLOW_HOLDS).value = False
        facets = Facets(library, None, None, None)
        eq_(Facets.AVAILABLE_NOW, facets.availability)

        # Unless 'now' is not one of the enabled facets - then we keep
        # using the library's default.
        test_enabled_facets[Facets.AVAILABILITY_FACET_GROUP_NAME] = [Facets.AVAILABLE_ALL]
        self._configure_facets(
            library, test_enabled_facets, test_default_facets
        )
        facets = Facets(library, None, None, None)
        eq_(Facets.AVAILABLE_ALL, facets.availability)

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

    def test_default_order_ascending(self):

        # Most fields are ordered ascending by default (A-Z).
        for order in (Facets.ORDER_TITLE, Facets.ORDER_RANDOM):
            f = Facets(
                self._default_library,
                collection=Facets.COLLECTION_FULL,
                availability=Facets.AVAILABLE_ALL,
                order=order
            )
            eq_(True, f.order_ascending)

        # But the time-based facets are ordered descending by default
        # (newest->oldest)
        eq_(set([Facets.ORDER_ADDED_TO_COLLECTION, Facets.ORDER_LAST_UPDATE]),
            set(Facets.ORDER_DESCENDING_BY_DEFAULT))
        for order in Facets.ORDER_DESCENDING_BY_DEFAULT:
            f = Facets(
                self._default_library,
                collection=Facets.COLLECTION_FULL,
                availability=Facets.AVAILABLE_ALL,
                order=order
            )
            eq_(False, f.order_ascending)

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
        headers = {}
        facets = m(library, library, args.get, headers.get, worklist)
        eq_(default_order, facets.order)
        eq_(default_collection, facets.collection)
        eq_(default_availability, facets.availability)
        eq_(library, facets.library)

        # The AudiobooksEntryPoint was selected as a default.
        eq_(AudiobooksEntryPoint, facets.entrypoint)
        eq_(True, facets.entrypoint_is_default)

        # Valid object using non-default settings.
        args = dict(
            order=Facets.ORDER_TITLE,
            collection=Facets.COLLECTION_FULL,
            available=Facets.AVAILABLE_OPEN_ACCESS,
            entrypoint=EbooksEntryPoint.INTERNAL_NAME,
        )
        facets = m(library, library, args.get, headers.get, worklist)
        eq_(Facets.ORDER_TITLE, facets.order)
        eq_(Facets.COLLECTION_FULL, facets.collection)
        eq_(Facets.AVAILABLE_OPEN_ACCESS, facets.availability)
        eq_(library, facets.library)
        eq_(EbooksEntryPoint, facets.entrypoint)

        # Invalid order
        args = dict(order="no such order")
        invalid_order = m(library, library, args.get, headers.get, None)
        eq_(INVALID_INPUT.uri, invalid_order.uri)
        eq_("I don't know how to order a feed by 'no such order'",
            invalid_order.detail)

        # Invalid availability
        args = dict(available="no such availability")
        invalid_availability = m(library, library, args.get, headers.get, None)
        eq_(INVALID_INPUT.uri, invalid_availability.uri)
        eq_("I don't understand the availability term 'no such availability'",
            invalid_availability.detail)

        # Invalid collection
        args = dict(collection="no such collection")
        invalid_collection = m(library, library, args.get, headers.get, None)
        eq_(INVALID_INPUT.uri, invalid_collection.uri)
        eq_("I don't understand what 'no such collection' refers to.",
            invalid_collection.detail)

    def test_from_request_gets_available_facets_through_hook_methods(self):
        # Available and default facets are determined by calling the
        # available_facets() and default_facets() methods. This gives
        # subclasses a chance to add extra facets or change defaults.
        class Mock(Facets):
            available_facets_calls = []
            default_facet_calls = []

            # For whatever reason, this faceting object allows only a
            # single setting for each facet group.
            mock_enabled = dict(order=[Facets.ORDER_TITLE],
                                available=[Facets.AVAILABLE_OPEN_ACCESS],
                                collection=[Facets.COLLECTION_MAIN])

            @classmethod
            def available_facets(cls, config, facet_group_name):
                cls.available_facets_calls.append((config, facet_group_name))
                return cls.mock_enabled[facet_group_name]

            @classmethod
            def default_facet(cls, config, facet_group_name):
                cls.default_facet_calls.append((config, facet_group_name))
                return cls.mock_enabled[facet_group_name][0]

        library = self._default_library
        result = Mock.from_request(library, library, {}.get, {}.get, None)

        order, available, collection = Mock.available_facets_calls
        # available_facets was called three times, to ask the Mock class what it thinks
        # the options for order, availability, and collection should be.
        eq_((library, "order"), order)
        eq_((library, "available"), available)
        eq_((library, "collection"), collection)

        # default_facet was called three times, to ask the Mock class what it thinks
        # the default order, availability, and collection should be.
        order_d, available_d, collection_d = Mock.default_facet_calls
        eq_((library, "order"), order_d)
        eq_((library, "available"), available_d)
        eq_((library, "collection"), collection_d)

        # Finally, verify that the return values from the mocked methods were actually used.

        # The facets enabled during initialization are the limited
        # subset established by available_facets().
        eq_(Mock.mock_enabled, result.facets_enabled_at_init)

        # The current values came from the defaults provided by default_facet().
        eq_(Facets.ORDER_TITLE, result.order)
        eq_(Facets.AVAILABLE_OPEN_ACCESS, result.availability)
        eq_(Facets.COLLECTION_MAIN, result.collection)

    def test_modify_search_filter(self):
        
        # Test superclass behavior -- filter is modified by entrypoint.
        facets = Facets(
            self._default_library, None, None, None,
            entrypoint=AudiobooksEntryPoint
        )
        filter = Filter()
        facets.modify_search_filter(filter)
        eq_([Edition.AUDIO_MEDIUM], filter.media)

        # Now test the subclass behavior.
        facets = Facets(
            self._default_library, "some collection", "some availability",
            order=Facets.ORDER_ADDED_TO_COLLECTION, order_ascending="yep"
        )
        facets.modify_search_filter(filter)

        # The library's minimum featured quality is passed in.
        eq_(self._default_library.minimum_featured_quality,
            filter.minimum_featured_quality)

        # Availability and collection are propagated with no 
        # validation.
        eq_("some availability", filter.availability)
        eq_("some collection", filter.subcollection)

        # The sort order constant is converted to the name of an
        # Elasticsearch field.
        expect = Facets.SORT_ORDER_TO_ELASTICSEARCH_FIELD_NAME[
            Facets.ORDER_ADDED_TO_COLLECTION
        ]
        eq_(expect, filter.order)
        eq_("yep", filter.order_ascending)

        # Specifying an invalid sort order doesn't cause a crash, but you
        # don't get a sort order.
        facets = Facets(self._default_library, None, None, "invalid order")
        filter = Filter()
        facets.modify_search_filter(filter)
        eq_(None, filter.order)
        

class TestDatabaseBackedFacets(DatabaseTest):

    def test_available_facets(self):
        # The only available sort orders are the ones that map
        # directly onto a database field.

        f1 = Facets
        f2 = DatabaseBackedFacets

        # The sort orders available to a DatabaseBackedFacets are a
        # subset of the ones available to a Facets under the same
        # configuration.
        f1_orders = f1.available_facets(
            self._default_library, FacetConstants.ORDER_FACET_GROUP_NAME
        )

        f2_orders = f2.available_facets(
            self._default_library, FacetConstants.ORDER_FACET_GROUP_NAME
        )
        assert len(f2_orders) < len(f1_orders)
        for order in f2_orders:
            assert (
                order in f1_orders and order in f2.ORDER_FACET_TO_DATABASE_FIELD
            )

        # The rules for collection and availability are the same.
        for group in (
            FacetConstants.COLLECTION_FACET_GROUP_NAME,
            FacetConstants.AVAILABILITY_FACET_GROUP_NAME,
        ):
            eq_(f1.available_facets(self._default_library, group),
                f2.available_facets(self._default_library, group))
            
    def test_default_facets(self):
        # If the configured default sort order is not available,
        # DatabaseBackedFacets chooses the first enabled sort order.
        f1 = Facets
        f2 = DatabaseBackedFacets

        # The rules for collection and availability are the same.
        for group in (
            FacetConstants.COLLECTION_FACET_GROUP_NAME,
            FacetConstants.AVAILABILITY_FACET_GROUP_NAME,
        ):
            eq_(f1.default_facet(self._default_library, group),
                f2.default_facet(self._default_library, group))

        # In this bizarre library, the default sort order is 'time
        # added to collection' -- an order not supported by
        # DatabaseBackedFacets.
        class Mock(object):
            enabled = [
                FacetConstants.ORDER_ADDED_TO_COLLECTION,
                FacetConstants.ORDER_TITLE, FacetConstants.ORDER_AUTHOR
            ]
            def enabled_facets(self, group_name):
                return self.enabled

            def default_facet(self, group_name):
                return FacetConstants.ORDER_ADDED_TO_COLLECTION

        # A Facets object uses the 'time added to collection' order by
        # default.
        config = Mock()
        eq_(f1.ORDER_ADDED_TO_COLLECTION, 
            f1.default_facet(config, f1.ORDER_FACET_GROUP_NAME))

        # A DatabaseBacked Facets can't do that. It finds the first
        # enabled sort order that it can support, and uses it instead.
        eq_(f2.ORDER_TITLE, 
            f2.default_facet(config, f2.ORDER_FACET_GROUP_NAME))

        # If no enabled sort orders are supported, it just sorts
        # by Work ID, so that there is always _some_ sort order.
        config.enabled = [FacetConstants.ORDER_ADDED_TO_COLLECTION]
        eq_(f2.ORDER_WORK_ID,
            f2.default_facet(config, f2.ORDER_FACET_GROUP_NAME))

    def test_order_by(self):
        E = Edition
        W = Work
        def order(facet, ascending=None):
            f = DatabaseBackedFacets(
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

        expect = [E.sort_author.asc(), E.sort_title.asc(), W.id.asc()]
        actual = order(Facets.ORDER_AUTHOR, True)
        compare(expect, actual)

        expect = [E.sort_author.desc(), E.sort_title.asc(), W.id.asc()]
        actual = order(Facets.ORDER_AUTHOR, False)
        compare(expect, actual)

        expect = [E.sort_title.asc(), E.sort_author.asc(), W.id.asc()]
        actual = order(Facets.ORDER_TITLE, True)
        compare(expect, actual)

        expect = [W.last_update_time.asc(), E.sort_author.asc(), 
                  E.sort_title.asc(), W.id.asc()]
        actual = order(Facets.ORDER_LAST_UPDATE, True)
        compare(expect, actual)

        expect = [W.random.asc(), E.sort_author.asc(), E.sort_title.asc(),
                  W.id.asc()]
        actual = order(Facets.ORDER_RANDOM, True)
        compare(expect, actual)

        # Unsupported sort order -> default (author, title, work ID)
        expect = [E.sort_author.asc(), E.sort_title.asc(), W.id.asc()]
        actual = order(Facets.ORDER_ADDED_TO_COLLECTION, True)
        compare(expect, actual)


    def test_modify_database_query(self):
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

        qu = self._db.query(Work).join(
            LicensePool, LicensePool.work_id==Work.id
        ).join(
            Edition, Work.presentation_edition_id==Edition.id
        )
        def facetify(collection=Facets.COLLECTION_FULL,
                     available=Facets.AVAILABLE_ALL,
                     order=Facets.ORDER_TITLE
        ):
            f = DatabaseBackedFacets(
                self._default_library, collection, available, order
            )
            return f.modify_database_query(self._db, qu)

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

        # Try some different orderings to verify that order_by()
        # is called and used properly.
        title_order = facetify(order=Facets.ORDER_TITLE)
        eq_([open_access_high.id, open_access_low.id, licensed_high.id,
             licensed_low.id],
            [x.id for x in title_order])
        eq_(
            ['sort_title', 'sort_author', 'id'],
            [x.name for x in title_order._distinct],
        )

        random_order = facetify(order=Facets.ORDER_RANDOM)
        eq_([licensed_low.id, open_access_high.id, licensed_high.id,
             open_access_low.id],
            [x.id for x in random_order])
        eq_(
            ['random', 'sort_author', 'sort_title', 'id'],
            [x.name for x in random_order._distinct],
        )

        # This sort order is not supported, so the default is used.
        unsupported_order = facetify(order=Facets.ORDER_ADDED_TO_COLLECTION)
        eq_([licensed_low.id, licensed_high.id, open_access_low.id,
             open_access_high.id],
            [x.id for x in unsupported_order])
        eq_(
            ['sort_author', 'sort_title', 'id'],
            [x.name for x in unsupported_order._distinct],
        )


class TestFeaturedFacets(DatabaseTest):

    def test_constructor(self):
        # Verify that constructor arguments are stored.
        entrypoint = object()
        facets = FeaturedFacets(1, entrypoint, entrypoint_is_default=True)
        eq_(1, facets.minimum_featured_quality)
        eq_(entrypoint, facets.entrypoint)
        eq_(True, facets.entrypoint_is_default)

    def test_navigate(self):
        # Test the ability of navigate() to move between slight
        # variations of a FeaturedFacets object.
        entrypoint = EbooksEntryPoint
        f = FeaturedFacets(1, entrypoint)

        different_entrypoint = f.navigate(entrypoint=AudiobooksEntryPoint)
        eq_(1, different_entrypoint.minimum_featured_quality)
        eq_(AudiobooksEntryPoint, different_entrypoint.entrypoint)

        different_quality = f.navigate(minimum_featured_quality=2)
        eq_(2, different_quality.minimum_featured_quality)
        eq_(entrypoint, different_quality.entrypoint)

    def test_scoring_functions(self):        
        # Verify that FeaturedFacets sets appropriate scoring functions
        # for ElasticSearch queries.
        f = FeaturedFacets(minimum_featured_quality=0.55, random_seed=42)
        from core.external_search import Filter
        filter = Filter()

        # In most cases, there are three things that can boost a work's score.
        [featurable, available_now, random] = f.scoring_functions(filter)

        # It can be high-quality enough to be featured.
        assert isinstance(featurable, ScriptScore)
        source = f.FEATURABLE_SCRIPT % dict(
            cutoff=f.minimum_featured_quality ** 2, exponent=2
        )
        eq_(source, featurable.script['source'])

        # It can be currently available.
        availability_filter = available_now['filter']
        eq_(
            dict(nested=dict(
                path='licensepools',
                query=dict(term={'licensepools.available': True})
            )),
            availability_filter.to_dict()
        )
        eq_(5, available_now['weight'])

        # It can get lucky.
        assert isinstance(random, RandomScore)
        eq_(42, random.seed)
        eq_(1.1, random.weight)

        # If the FeaturedFacets is set to be deterministic (which only happens
        # in tests), the RandomScore is removed.
        f.random_seed = f.DETERMINISTIC
        [featurable_2, available_now_2] = f.scoring_functions(filter)
        eq_(featurable_2, featurable)
        eq_(available_now_2, available_now)

        # If custom lists are in play, it can also be featured on one
        # of its custom lists.
        filter.customlist_restriction_sets = [[1,2], [3]]
        [featurable_2, available_now_2,
         featured_on_list] = f.scoring_functions(filter)
        eq_(featurable_2, featurable)
        eq_(available_now_2, available_now)

        # Any list will do -- the customlist restriction sets aren't
        # relevant here.
        featured_filter = featured_on_list['filter']
        eq_(dict(
            nested=dict(
                path='customlists',
                query=dict(bool=dict(
                    must=[{'term': {'customlists.featured': True}},
                          {'terms': {'customlists.list_id': [1, 2, 3]}}])))),
            featured_filter.to_dict()
        )
        eq_(11, featured_on_list['weight'])


class TestSearchFacets(DatabaseTest):

    def test_constructor(self):
        # The SearchFacets constructor allows you to specify
        # a medium and language (or a list of them) as well
        # as an entrypoint.

        m = SearchFacets

        # If you don't pass any information in, you get a SearchFacets
        # that does nothing.
        defaults = m()
        eq_(None, defaults.entrypoint)
        eq_(None, defaults.languages)
        eq_(None, defaults.media)

        mock_entrypoint = object()

        # If you pass in a single value for medium or language
        # they are turned into a list.
        with_single_value = m(mock_entrypoint, Edition.BOOK_MEDIUM, "eng")
        eq_(mock_entrypoint, with_single_value.entrypoint)
        eq_([Edition.BOOK_MEDIUM], with_single_value.media)
        eq_(["eng"], with_single_value.languages)

        # If you pass in a list of values, it's left alone.
        media = [Edition.BOOK_MEDIUM, Edition.AUDIO_MEDIUM]
        languages = ["eng", "spa"]
        with_multiple_values = m(None, media, languages)
        eq_(media, with_multiple_values.media)
        eq_(languages, with_multiple_values.languages)

        # The only exception is if you pass in Edition.ALL_MEDIUM
        # as 'medium' -- that's passed through as is.
        every_medium = m(None, Edition.ALL_MEDIUM)
        eq_(Edition.ALL_MEDIUM, every_medium.media)

    def test_from_request(self):
        # An HTTP client can customize which SearchFacets object
        # is created by sending different HTTP requests.

        # These variables mock the query string arguments and
        # HTTP headers of an HTTP request.
        arguments = dict(entrypoint=EbooksEntryPoint.INTERNAL_NAME,
                         media=Edition.AUDIO_MEDIUM)
        headers = {"Accept-Language" : "da, en-gb;q=0.8"}
        get_argument = arguments.get
        get_header = headers.get

        unused = object()

        library = self._default_library
        library.setting(EntryPoint.ENABLED_SETTING).value = json.dumps(
            [AudiobooksEntryPoint.INTERNAL_NAME, EbooksEntryPoint.INTERNAL_NAME]
        )

        def from_request(**extra):
            return SearchFacets.from_request(
                unused, self._default_library, get_argument, get_header,
                unused, **extra
            )

        facets = from_request(extra="value")
        eq_(dict(extra="value"), facets.constructor_kwargs)

        # The superclass's from_request implementation pulled the
        # requested EntryPoint out of the request.
        eq_(EbooksEntryPoint, facets.entrypoint)

        # The SearchFacets implementation pulled the 'media' query
        # string argument.
        #
        # The medium from the 'media' argument contradicts the medium
        # implied by the entry point, but that's not our problem.
        eq_([Edition.AUDIO_MEDIUM], facets.media)

        # The SearchFacets implementation turned the 'Accept-Language'
        # header into a set of language codes.
        eq_(['dan', 'eng'], facets.languages)

        # Try again with bogus media and languages.
        arguments['media'] = 'Unknown Media'
        headers['Accept-Language'] = "xx, ql"

        # None of the bogus information was used.
        facets = from_request()
        eq_(None, facets.media)
        eq_(None, facets.languages)

        # Reading the language query with acceptable Accept-Language header
        # but not passing that value through.
        arguments['language'] = 'all'
        headers['Accept-Language'] = "da, en-gb;q=0.8"

        facets = from_request()
        eq_(None, facets.languages)

        # Try again with no information.
        del arguments['media']
        del headers['Accept-Language']

        facets = from_request()
        eq_(None, facets.media)
        eq_(None, facets.languages)

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

    def test_items(self):
        facets = SearchFacets(
            entrypoint=EverythingEntryPoint,
            media=Edition.BOOK_MEDIUM, languages=['eng']
        )

        # When we call items(), e.g. to create a query string that
        # propagates the facet settings, both entrypoint and
        # media are propagated if present.
        #
        # language is not propagated, because it's set through
        # the Accept-Language header rather than through a query
        # string.
        eq_(
            [('entrypoint', EverythingEntryPoint.INTERNAL_NAME),
             ('media', Edition.BOOK_MEDIUM)],
            list(facets.items())
        )

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

    def test_modify_search_filter(self):

        # Test superclass behavior -- filter is modified by entrypoint.
        facets = SearchFacets(AudiobooksEntryPoint)
        filter = Filter()
        facets.modify_search_filter(filter)
        eq_([Edition.AUDIO_MEDIUM], filter.media)

        # The medium specified in the constructor overrides anything
        # already present in the filter.
        facets = SearchFacets(None, Edition.BOOK_MEDIUM)
        filter = Filter(media=Edition.AUDIO_MEDIUM)
        facets.modify_search_filter(filter)
        eq_([Edition.BOOK_MEDIUM], filter.media)

        # It also overrides anything specified by the EntryPoint.
        facets = SearchFacets(AudiobooksEntryPoint, Edition.BOOK_MEDIUM)
        filter = Filter()
        facets.modify_search_filter(filter)
        eq_([Edition.BOOK_MEDIUM], filter.media)

        # The language specified in the constructor does *not* override
        # anything already present in the filter.
        facets = SearchFacets(None, languages=["eng", "spa"])
        filter = Filter(languages="spa")
        facets.modify_search_filter(filter)
        eq_("spa", filter.languages)

        # It only takes effect if the filter doesn't have any languages
        # set.
        filter = Filter()
        facets.modify_search_filter(filter)
        eq_(["eng", "spa"], filter.languages)


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
        eq_(None, pagination.next_page)

        # If we somehow go over the end of the dataset, there is no next page.
        pagination.offset = 400
        eq_(False, pagination.has_next_page)
        eq_(None, pagination.next_page)

        # If both total_size and this_page_size are set, total_size
        # takes precedence.
        pagination.offset = 0
        pagination.total_size = 100
        pagination.this_page_size = 0
        eq_(True, pagination.has_next_page)

        pagination.total_size = 0
        pagination.this_page_size = 10
        eq_(False, pagination.has_next_page)
        eq_(None, pagination.next_page)

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

    def test_page_loaded(self):
        # Test page_loaded(), which lets the Pagination object see the
        # size of the current page.
        pagination = Pagination()
        eq_(None, pagination.this_page_size)
        eq_(False, pagination.page_has_loaded)
        pagination.page_loaded([1,2,3])
        eq_(3, pagination.this_page_size)
        eq_(True, pagination.page_has_loaded)

    def test_modify_search_query(self):
        # The default implementation of modify_search_query is a no-op.
        pagination = Pagination()
        o = object()
        eq_(o, pagination.modify_search_query(o))


class MockWork(object):
    """Acts as a Work or a MaterializedWorkWithGenre interchangeably."""
    def __init__(self, id):
        self.id = id
        self.works_id = id

class MockWorks(WorkList):
    """A WorkList that mocks works_from_database()."""

    def __init__(self):
        self.reset()

    def reset(self):
        self._works = []
        self.works_from_database_calls = []
        self.random_sample_calls = []

    def queue_works(self, works):
        """Set the next return value for works_from_database()."""
        self._works.append(works)

    def works_from_database(self, _db, facets=None, pagination=None, featured=False):
        self.works_from_database_calls.append((facets, pagination, featured))
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

    def test_initialize_with_customlists(self):

        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)

        customlist1, ignore = self._customlist(
            data_source_name=gutenberg.name, num_entries=0
        )
        customlist2, ignore = self._customlist(
            data_source_name=gutenberg.name, num_entries=0
        )
        customlist3, ignore = self._customlist(
            data_source_name=DataSource.OVERDRIVE, num_entries=0
        )

        # Make a WorkList based on specific CustomLists.
        worklist = WorkList()
        worklist.initialize(self._default_library,
                            customlists=[customlist1, customlist3])
        eq_([customlist1.id, customlist3.id], worklist.customlist_ids)
        eq_(None, worklist.list_datasource_id)

        # Make a WorkList based on a DataSource, as a shorthand for
        # 'all the CustomLists from that DataSource'.
        worklist = WorkList()
        worklist.initialize(self._default_library,
                            list_datasource=gutenberg)
        eq_([customlist1.id, customlist2.id], worklist.customlist_ids)
        eq_(gutenberg.id, worklist.list_datasource_id)

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

    def test_initialize_uses_append_child_hook_method(self):
        # When a WorkList is initialized with children, the children
        # are passed individually through the append_child() hook
        # method, not simply set to WorkList.children.
        class Mock(WorkList):
            append_child_calls = []
            def append_child(self, child):
                self.append_child_calls.append(child)
                return super(Mock, self).append_child(child)

        child = WorkList()
        parent = Mock()
        parent.initialize(self._default_library, children=[child])
        eq_([child], parent.append_child_calls)

        # They do end up in WorkList.children, since that's what the
        # default append_child() implementation does.
        eq_([child], parent.children)

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

    def test_parent(self):
        # A WorkList has no parent.
        eq_(None, WorkList().parent)

    def test_parentage(self):
        # A WorkList has no parentage, since it has no parent.
        eq_([], WorkList().parentage)

    def test_inherit_parent_restrictions(self):
        # A WorkList never inherits parent restrictions, because it
        # can't have a parent.
        eq_(False, WorkList().inherit_parent_restrictions)

    def test_hierarchy(self):
        # A WorkList's hierarchy includes only itself, because it
        # can't have a parent.
        wl = WorkList()
        eq_([wl], wl.hierarchy)

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

    def test_uses_customlists(self):
        """A WorkList is said to use CustomLists if either ._customlist_ids
        or .list_datasource_id is set.
        """
        wl = WorkList()
        wl.initialize(self._default_library)
        eq_(False, wl.uses_customlists)

        wl._customlist_ids = object()
        eq_(True, wl.uses_customlists)

        wl._customlist_ids = None
        wl.list_datasource_id = object()
        eq_(True, wl.uses_customlists)

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

            def works(self, _db, facets):
                self.featured_called_with = facets
                return []

            def _groups_for_lanes(
                self, _db, relevant_children, relevant_lanes, facets, search_engine=None, debug=False
            ):
                self.groups_called_with = facets
                return []

        mock = MockWorkList()
        mock.initialize(library=self._default_library)
        facets = object()
        [x for x in mock.groups(self._db, facets=facets)]
        eq_(facets, mock.groups_called_with)

        [x for x in mock.groups(self._db, facets=facets, include_sublanes=False)]
        eq_(facets, mock.featured_called_with)

    def test_works(self):
        """Test the method that uses the search index to fetch a list of
        results appropriate for a given WorkList.
        """

        class MockSearchClient(object):
            """Respond to search requests with some fake work IDs."""
            fake_work_ids = [1, 10, 100, 1000]
            def query_works(self, **kwargs):
                self.called_with = kwargs
                return self.fake_work_ids

        class MockWorkList(WorkList):
            """Mock the process of turning work IDs into WorkSearchResult
            objects."""
            fake_work_list = "a list of works"
            def works_for_hits(self, _db, work_ids):
                self.called_with = (_db, work_ids)
                return self.fake_work_list

        # Here's a WorkList.
        wl = MockWorkList()
        wl.initialize(self._default_library, languages=["eng"])
        facets = Facets(
            self._default_library, None, None, order=Facets.ORDER_TITLE
        )
        mock_pagination = object()
        mock_debug = object()
        search_client = MockSearchClient()

        # Ask the WorkList for a page of works, using the search index
        # to drive the query instead of the database.
        result = wl.works_from_search_index(
            self._db, facets, mock_pagination, search_client, mock_debug
        )

        # MockSearchClient.query_works was used to grab a list of work
        # IDs.
        query_works_kwargs = search_client.called_with

        # Our facets and the requirements of the WorkList were used to
        # make a Filter object, which was passed as the 'filter'
        # keyword argument.
        filter = query_works_kwargs.pop('filter')
        eq_(Filter.from_worklist(self._db, wl, facets).build(),
            filter.build())

        # The other arguments to query_works are either constants or
        # our mock objects.
        eq_(dict(query_string=None,
                 pagination=mock_pagination,
                 debug=mock_debug),
            query_works_kwargs
        )

        # The fake work IDs returned from query_works() were passed into
        # works_for_hits().
        eq_(
            (self._db, search_client.fake_work_ids, Work),
            wl.called_with
        )

        # And the fake return value of works_for_hits() was
        # used as the return value of works_from_search_index(), the
        # method we're testing.
        eq_(wl.fake_work_list, result)

    def test_works_for_hits(self):
        # Verify that WorkList.works_for_hits turns (mocked) Hit
        # objects into Work or WorkSearchResult objects.

        # Create two works.
        w1 = self._work(with_license_pool=True)
        w2 = self._work(with_license_pool=True)

        class MockHit(object):
            def __init__(self, work_id, has_last_update=False):
                if isinstance(work_id, Work):
                    self.work_id=work_id.id
                else:
                    self.work_id=work_id
                self.has_last_update = has_last_update

            def __contains__(self, k):
                # Pretend to have the 'last_update' script field,
                # if necessary.
                return (k == 'last_update' and self.has_last_update)

        hit1 = MockHit(w1)
        hit2 = MockHit(w2)

        wl = WorkList()
        wl.initialize(self._default_library)
        eq_([w2], wl.works_for_hits(self._db, [hit2]))

        # Works are returned in the order we ask for.
        for ordering in ([hit1, hit2], [hit2, hit1]):
            works = wl.works_for_hits(self._db, ordering)
            eq_([x.work_id for x in ordering], [x.id for x in works])

        # If we ask for a work ID that's not in the database,
        # we don't get it.
        eq_([], wl.works_for_hits(self._db, [MockHit(-100)]))

        # If we pass in Hit objects that have extra information in them,
        # we get WorkSearchResult objects
        hit1_extra = MockHit(w1, True)
        hit2_extra = MockHit(w2, True)

        results = wl.works_for_hits(self._db, [hit2_extra, hit1_extra])
        assert all(isinstance(x, WorkSearchResult) for x in results)
        r1, r2 = results

        # These WorkSearchResult objects wrap Work objects together
        # with the corresponding Hit objects.
        eq_(w2, r1._work)
        eq_(hit2_extra, r1._hit)

        eq_(w1, r2._work)
        eq_(hit1_extra, r2._hit)

        # Finally, test that undeliverable works are filtered out.
        for lpdm in w2.license_pools[0].delivery_mechanisms:
            self._db.delete(lpdm)
            eq_([], wl.works_for_hits(self._db, [hit2]))


    def test_search_target(self):
        # A WorkList can be searched - it is its own search target.
        wl = WorkList()
        eq_(wl, wl.search_target)

    def test_search(self):
        # Test the successful execution of WorkList.search()

        class MockWorkList(WorkList):
            def works_for_hits(self, _db, work_ids):
                self.works_for_hits_called_with = (_db, work_ids)
                return "A bunch of Works"

        wl = MockWorkList()
        wl.initialize(
            self._default_library, audiences=[Classifier.AUDIENCE_CHILDREN]
        )
        query = "a query"

        class MockSearchClient(object):
            def query_works(self, query, filter, pagination, debug):
                self.query_works_called_with = (
                    query, filter, pagination, debug
                )
                return "A bunch of work IDs"

        # Search with the default arguments.
        client = MockSearchClient()
        results = wl.search(self._db, query, client)

        # The results of query_works were passed into
        # MockWorkList.works_for_hits.
        eq_(
            (self._db, "A bunch of work IDs", Work),
            wl.works_for_hits_called_with
        )

        # The return value of MockWorkList.works_for_hits is
        # used as the return value of query_works().
        eq_("A bunch of Works", results)

        # From this point on we are only interested in the arguments
        # passed in to query_works, since MockSearchClient always
        # returns the same result.

        # First, let's see what the default arguments look like.
        qu, filter, pagination, debug = client.query_works_called_with

        # The query was passed through.
        eq_(query, qu)
        eq_(False, debug)

        # A Filter object was created to match only works that belong
        # in the MockWorkList.
        eq_([Classifier.AUDIENCE_CHILDREN], filter.audiences)

        # A default Pagination object was created.
        eq_(0, pagination.offset)
        eq_(Pagination.DEFAULT_SEARCH_SIZE, pagination.size)

        # Now let's try a search with specific Pagination and Facets
        # objects.
        facets = SearchFacets(None, languages=["chi"])
        pagination = object()
        results = wl.search(self._db, query, client, pagination, facets,
                            debug=True)

        qu, filter, pag, debug = client.query_works_called_with
        eq_(query, qu)
        eq_(pagination, pag)
        eq_(True, debug)

        # The Filter incorporates restrictions imposed by both the
        # MockWorkList and the Facets.
        eq_([Classifier.AUDIENCE_CHILDREN], filter.audiences)
        eq_(["chi"], filter.languages)

    def test_search_failures(self):
        # Test reasons why WorkList.search() might not work.
        wl = WorkList()
        wl.initialize(self._default_library)
        query = "a query"

        # If there is no SearchClient, there are no results.
        eq_([], wl.search(self._db, query, None))

        # If the SearchClient returns nothing, there are no results.
        class NoResults(object):
            def query_works(self, *args, **kwargs):
                return None
        eq_([], wl.search(self._db, query, NoResults()))

        # If there's an ElasticSearch exception during the query,
        # there are no results.
        class RaisesException(object):
            def query_works(self, *args, **kwargs):
                raise ElasticsearchException("oh no")
        eq_([], wl.search(self._db, query, RaisesException()))


class TestDatabaseBackedWorkList(DatabaseTest):

    def test_works(self):
        """Verify that the works() method calls the methods we expect,
        in the right order.
        """

        class MockQuery(object):
            # Simulates the behavior of a database Query object
            # without the need to pass around actual database clauses.
            #
            # This is a lot of instrumentation but it means we can
            # test what happened inside works() mainly by looking at a
            # string of method names in the result object.
            def __init__(self, clauses, distinct=False):
                self.clauses = clauses
                self._distinct = distinct

            def filter(self, clause):
                # Create a new MockQuery object with a new clause
                return MockQuery(self.clauses + [clause], self._distinct)

            def distinct(self, fields):
                return MockQuery(self.clauses, fields)

            def __repr__(self):
                return "<MockQuery %d clauses, most recent %s>" % (
                    len(self.clauses), self.clauses[-1]
                )

        class MockWorkList(DatabaseBackedWorkList):
            def __init__(self, _db):
                super(MockWorkList, self).__init__()
                self._db = _db # We'll be using this in assertions.
                self.stages = []

            def _stage(self, method_name, _db, qu, qu_is_previous_stage=True):
                # _db must always be self._db; check it here and then
                # ignore it.
                eq_(_db, self._db)

                if qu_is_previous_stage:
                    # qu must be the MockQuery returned from the
                    # previous call.
                    eq_(qu, self.stages[-1])

                # Create a new MockQuery with an additional filter,
                # named after the method that was called.
                new_filter = qu.filter(method_name)
                self.stages.append(new_filter)
                return new_filter

            def base_query(self, _db):
                # This kicks off the process -- most future calls will
                # use _stage().
                eq_(_db, self._db)
                query = MockQuery(['base_query'])
                self.stages.append(query)
                return query

            def only_show_ready_deliverable_works(self, _db, qu):
                return self._stage('only_show_ready_deliverable_works', _db, qu)

            def bibliographic_filter_clause(self, _db, qu):
                # This method is a little different, so we can't use
                # _stage().
                #
                # This implementation doesn't change anything; it will be
                # replaced with an implementation that does.
                eq_(_db, self._db)
                self.bibliographic_filter_clause_called_with = qu
                return qu, None

            def modify_database_query_hook(self, _db, qu):
                return self._stage('modify_database_query_hook', _db, qu)

            def active_bibliographic_filter_clause(self, _db, qu):
                # This alternate implementation of
                # bibliographic_filter_clause returns a brand new
                # MockQuery object and a string that should be used as
                # a filter on it.
                self.pre_bibliographic_filter = qu
                new_query = MockQuery(
                    ["new query made inside active_bibliographic_filter_clause"]
                )
                self.stages.append(new_query)
                return (
                    new_query,
                    "bibliographic filter returned by active_bibliographic_filter_clause"
                )

        # The simplest case: no facets or pagination,
        # and bibliographic_filter_clause does nothing.
        wl = MockWorkList(self._db)
        result = wl.works(self._db, extra_kwarg="ignored")

        # We got a MockQuery.
        assert isinstance(result, MockQuery)

        # During the course of the works() call, we verified that the
        # MockQuery is constructed by chaining method calls.  Now we
        # just need to verify that all the methods were called and in
        # the order we expect.
        eq_(['base_query', 'only_show_ready_deliverable_works',
             'modify_database_query_hook'],
            result.clauses
        )
        
        # bibliographic_filter_clause used a different mechanism, but
        # since it stored the MockQuery it was called with, we can see
        # when it was called -- just after
        # only_show_ready_deliverable_works.
        eq_(
            ['base_query', 'only_show_ready_deliverable_works'],
            wl.bibliographic_filter_clause_called_with.clauses
        )
        wl.bibliographic_filter_clause_called_with = None

        # Since nobody made the query distinct, it was set distinct on
        # Work.id.
        eq_(Work.id, result._distinct)

        # Now we're going to do a more complicated test, with
        # faceting, pagination, and a bibliographic_filter_clause that
        # actually does something.
        wl.bibliographic_filter_clause = wl.active_bibliographic_filter_clause

        class MockFacets(DatabaseBackedFacets):
            def __init__(self, wl):
                self.wl = wl

            def modify_database_query(self, _db, qu):
                # This is the only place we pass in False for
                # qu_is_preivous_stage. This is called right after
                # bibliographic_filter_clause, which caused a brand
                # new MockQuery object to be created.
                result = self.wl._stage(
                    "facets", _db, qu, qu_is_previous_stage=False
                )
                
                distinct = result.distinct("some other field")
                self.wl.stages.append(distinct)
                return distinct

        # MockFacets has to subclass DatabaseBasedFacets because we check
        # for this, in an attempt to avoid bugs caused by passing a normal
        # Facets into works().
        assert_raises_regexp(
            ValueError,
            "Incompatible faceting object for DatabaseBackedWorkList: 'bad facet'",
            wl.works, self._db, facets="bad facet"
        )

        class MockPagination(object):
            def __init__(self, wl):
                self.wl = wl

            def modify_database_query(self, _db, qu):
                return self.wl._stage("pagination", _db, qu)

        result = wl.works(
            self._db, facets=MockFacets(wl), pagination=MockPagination(wl)
        )

        # Here are the methods called before bibliographic_filter_clause.
        eq_(['base_query', 'only_show_ready_deliverable_works'],
            wl.pre_bibliographic_filter.clauses)

        # bibliographic_filter_clause created a brand new object,
        # which ended up as our result after some more methods were
        # called on it.
        eq_(['new query made inside active_bibliographic_filter_clause',
             'bibliographic filter returned by active_bibliographic_filter_clause',
             'facets',
             'modify_database_query_hook',
             'pagination',
             ],
            result.clauses
        )

        # The query was made distinct on some other field, so the
        # default behavior (making it distinct on Work.id) wasn't
        # triggered.
        eq_("some other field", result._distinct)

    def test_works_end_to_end(self):
        """Verify that works() correctly locates works that match the criteria
        specified by the DatabaseBackedWorkList, the faceting object, and
        the pagination object.

        This is a simple end-to-end test of functionality that's
        tested in more detail elsewhere.
        """

        # Create two books.
        oliver_twist = self._work(
            title='Oliver Twist', with_license_pool=True, language="eng"
        )
        barnaby_rudge = self._work(
            title='Baraby Rudge', with_license_pool=True, language="spa"
        )

        # A standard DatabaseBackedWorkList will find both books.
        wl = DatabaseBackedWorkList()
        wl.initialize(self._default_library)
        eq_(2, wl.works(self._db).count())

        # A work list with a language restriction will only find books
        # in that language.
        wl.initialize(self._default_library, languages=['eng'])
        eq_([oliver_twist], [x for x in wl.works(self._db)])

        # A DatabaseBackedWorkList will only find books licensed
        # through one of its collections.
        collection = self._collection()
        self._default_library.collections = [collection]
        wl.initialize(self._default_library)
        eq_(0, wl.works(self._db).count())

        # If a DatabaseBackedWorkList has no collections, it has no
        # books.
        self._default_library.collections = []
        wl.initialize(self._default_library)
        eq_(0, wl.works(self._db).count())

        # A DatabaseBackedWorkList can be set up with a collection
        # rather than a library. TODO: The syntax here could be improved.
        wl = DatabaseBackedWorkList()
        wl.initialize(None)
        wl.collection_ids = [self._default_collection.id]
        eq_(None, wl.get_library(self._db))
        eq_(2, wl.works(self._db).count())

        # Facets and pagination can affect which entries and how many
        # are returned.
        facets = DatabaseBackedFacets(
            self._default_library,
            collection=Facets.COLLECTION_FULL,
            availability=Facets.AVAILABLE_ALL,
            order=Facets.ORDER_TITLE
        )
        pagination = Pagination(offset=1, size=1)
        eq_([oliver_twist], wl.works(self._db, facets, pagination).all())

        facets.order_ascending = False
        eq_([barnaby_rudge], wl.works(self._db, facets, pagination).all())

    def test_base_query(self):
        # Verify that base_query makes the query we expect and then
        # calls some optimization methods (not tested).
        class Mock(DatabaseBackedWorkList):
            def _modify_loading(self, qu):
                return [qu, "_modify_loading"]

            def _defer_unused_fields(self, qu):
                return qu + ['_defer_unused_fields']

        result = Mock().base_query(self._db)

        [base_query, m, d] = result
        expect = self._db.query(Work).join(Work.license_pools).join(
            Work.presentation_edition
        )
        eq_(str(expect), str(base_query))
        eq_("_modify_loading", m)
        eq_("_defer_unused_fields", d)

    def test_bibliographic_filter_clause(self):
        called = dict()

        class MockWorkList(DatabaseBackedWorkList):
            """Verifies that bibliographic_filter_clause() calls various hook
            methods.

            The hook methods themselves are tested separately.
            """
            def audience_filter_clauses(self, _db, qu):
                called['apply_audience_filter'] = (_db, qu)
                return []

            def customlist_filter_clauses(self, *args, **kwargs):
                called['customlist_filter_clauses'] = (args, kwargs)
                return super(MockWorkList, self).customlist_filter_clauses(
                    *args, **kwargs
                )

        wl = MockWorkList()
        wl.initialize(self._default_library)
        original_qu = self._db.query(Work)

        # If no languages or genre IDs are specified, and the hook
        # methods do nothing, then bibliographic_filter_clause() has
        # no effect.
        final_qu, bibliographic_filter = wl.bibliographic_filter_clause(
            self._db, original_qu
        )
        eq_(original_qu, final_qu)
        eq_(None, bibliographic_filter)

        # But at least the apply_audience_filter was called with the correct
        # arguments.
        _db, qu = called['apply_audience_filter']
        eq_(self._db, _db)
        eq_(original_qu, qu)

        # customlist_filter_clauses was not called because the WorkList
        # doesn't do anything relating to custom lists.
        assert 'customlist_filter_clauses' not in called

        # If languages, media, and genre IDs are specified, then they are
        # incorporated into the query.
        #
        english_sf = self._work(language="eng", with_license_pool=True)
        english_sf.presentation_edition.medium = Edition.BOOK_MEDIUM
        sf, ignore = Genre.lookup(self._db, "Science Fiction")
        romance, ignore = Genre.lookup(self._db, "Romance")
        english_sf.genres.append(sf)

        # Create a WorkList that will find the English SF book.
        def worklist_has_books(expect_books, **initialize_kwargs):
            """Apply bibliographic filters to a query and verify
            that it finds only the given books.
            """
            worklist = MockWorkList()
            worklist.initialize(self._default_library, **initialize_kwargs)
            qu, clause = worklist.bibliographic_filter_clause(
                self._db, original_qu
            )
            qu = qu.filter(clause)
            expect_titles = sorted([x.sort_title for x in expect_books])
            actual_titles = sorted([x.sort_title for x in qu])
            eq_(expect_titles, actual_titles)

        worklist_has_books(
            [english_sf],
            languages=["eng"], genres=[sf], media=[Edition.BOOK_MEDIUM]
        )

        # WorkLists that do not match by language, medium, or genre will not
        # find the English SF book.
        worklist_has_books([], languages=["spa"], genres=[sf])
        worklist_has_books([], languages=["eng"], genres=[romance])
        worklist_has_books(
            [],
            languages=["eng"], genres=[sf], media=[Edition.AUDIO_MEDIUM]
        )

        # If the WorkList has custom list IDs, then works will only show up if
        # they're on one of the matching CustomLists.

        sf_list, ignore = self._customlist(num_entries=0)
        sf_list.add_entry(english_sf)
        empty_list, ignore = self._customlist(num_entries=0)
        self.add_to_materialized_view(english_sf)

        worklist_has_books([], customlists=[empty_list])
        # We ended up with no results, but customlist_filter_clauses
        # was called with a query that _would_ have returned results.
        [query], kwargs= called['customlist_filter_clauses']
        eq_([english_sf], query.all())
        eq_({}, kwargs)

        worklist_has_books([english_sf], customlists=[sf_list])

    def test_audience_filter_clauses(self):
        # Verify that audience_filter_clauses restricts a query to
        # reflect a DatabaseBackedWorkList's audience filter.

        # Create a children's book and a book for adults.
        adult = self._work(
            title="Diseases of the Horse",
            with_license_pool=True, with_open_access_download=True,
            audience=Classifier.AUDIENCE_ADULT
        )

        children = self._work(
            title="Wholesome Nursery Rhymes For All Children",
            with_license_pool=True, with_open_access_download=True,
            audience=Classifier.AUDIENCE_CHILDREN
        )

        def for_audiences(*audiences):
            """Invoke audience_filter_clauses using the given
            `audiences`, and return all the matching Work objects.
            """
            wl = DatabaseBackedWorkList()
            wl.audiences = audiences
            qu = wl.base_query(self._db)
            clauses = wl.audience_filter_clauses(self._db, qu)
            if clauses:
                qu = qu.filter(and_(*clauses))
            return qu.all()

        eq_([adult], for_audiences(Classifier.AUDIENCE_ADULT))
        eq_([children], for_audiences(Classifier.AUDIENCE_CHILDREN))

        # If no particular audiences are specified, no books are filtered.
        eq_(set([adult, children]), set(for_audiences()))

    def test_customlist_filter_clauses(self):
        # Standalone test of customlist_filter_clauses

        # If a lane has nothing to do with CustomLists,
        # apply_customlist_filter does nothing.
        no_lists = DatabaseBackedWorkList()
        no_lists.initialize(self._default_library)
        qu = no_lists.base_query(self._db)
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

        # This DatabaseBackedWorkList gets every work on a specific list.
        works_on_list = DatabaseBackedWorkList()
        works_on_list.initialize(
            self._default_library, customlists=[gutenberg_list]
        )

        # This lane gets every work on every list associated with Project
        # Gutenberg.
        works_on_gutenberg_lists = DatabaseBackedWorkList()
        works_on_gutenberg_lists.initialize(
            self._default_library, list_datasource=gutenberg
        )

        def _run(qu, clauses):
            # Run a query with certain clauses
            return qu.filter(and_(*clauses)).all()

        def results(wl=works_on_gutenberg_lists, must_be_featured=False):
            qu = wl.base_query(self._db)
            new_qu, clauses = wl.customlist_filter_clauses(qu)

            # The query comes out different than it goes in -- there's a
            # new join against CustomListEntry.
            assert new_qu != qu
            return _run(new_qu, clauses)

        # Both lanes contain the work.
        eq_([work], results(works_on_list))
        eq_([work], results(works_on_gutenberg_lists))

        # If there's another list with the same work on it, the
        # work only shows up once.
        gutenberg_list_2, ignore = self._customlist(num_entries=0)
        gutenberg_list_2_entry, ignore = gutenberg_list_2.add_entry(work)
        works_on_list._customlist_ids.append(gutenberg_list.id)
        eq_([work], results(works_on_list))

        # This WorkList gets every work on a list associated with Overdrive.
        # There are no such lists, so the lane is empty.
        overdrive = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        works_on_overdrive_lists = DatabaseBackedWorkList()
        works_on_overdrive_lists.initialize(
            self._default_library, list_datasource=overdrive
        )
        eq_([], results(works_on_overdrive_lists))

        # It's possible to restrict a WorkList to works that were seen on
        # a certain list recently.
        now = datetime.datetime.utcnow()
        two_days_ago = now - datetime.timedelta(days=2)
        gutenberg_list_entry.most_recent_appearance = two_days_ago

        # The lane will only show works that were seen within the last
        # day. There are no such works.
        works_on_gutenberg_lists.list_seen_in_previous_days = 1
        eq_([], results())

        # Now it's been loosened to three days, and the work shows up.
        works_on_gutenberg_lists.list_seen_in_previous_days = 3
        eq_([work], results())

        # Now let's test what happens when we chain calls to this
        # method.
        gutenberg_list_2_wl = DatabaseBackedWorkList()
        gutenberg_list_2_wl.initialize(
            self._default_library, customlists = [gutenberg_list_2]
        )

        # These two lines won't work, because these are
        # DatabaseBackedWorkLists, not Lanes, but they show the
        # scenario in which this would actually happen. When
        # determining which works belong in the child lane,
        # Lane.customlist_filter_clauses() will be called on the
        # parent lane and then on the child. In this case, only want
        # books that are on _both_ works_on_list and gutenberg_list_2.
        #
        # gutenberg_list_2_wl.parent = works_on_list
        # gutenberg_list_2_wl.inherit_parent_restrictions = True

        qu = works_on_list.base_query(self._db)
        list_1_qu, list_1_clauses = works_on_list.customlist_filter_clauses(qu)

        # The query has been modified -- we've added a join against
        # CustomListEntry.
        assert list_1_qu != qu
        eq_([work], list_1_qu.all())

        # Now call customlist_filter_clauses again so that the query
        # must only match books on _both_ lists. This simulates
        # what happens when the second lane is a child of the first,
        # and inherits its restrictions.
        both_lists_qu, list_2_clauses = gutenberg_list_2_wl.customlist_filter_clauses(
            list_1_qu,
        )
        # The query has been modified again -- we've added a second join
        # against CustomListEntry.
        assert both_lists_qu != list_1_qu
        both_lists_clauses = list_1_clauses + list_2_clauses

        # The combined query matches the work that shows up on
        # both lists.
        eq_([work], _run(both_lists_qu, both_lists_clauses))

        # If we remove `work` from either list, the combined query
        # matches nothing.
        for l in [gutenberg_list, gutenberg_list_2]:
            l.remove_entry(work)
            eq_([], _run(both_lists_qu, both_lists_clauses))
            l.add_entry(work)



class TestLane(DatabaseTest):

    def test_get_library(self):
        lane = self._lane()
        eq_(self._default_library, lane.get_library(self._db))

    def test_list_datasource(self):
        """Test setting and retrieving the DataSource object and
        the underlying ID.
        """
        lane = self._lane()

        # This lane is based on a specific CustomList.
        customlist1, ignore = self._customlist(num_entries=0)
        customlist2, ignore = self._customlist(num_entries=0)
        lane.customlists.append(customlist1)
        eq_(None, lane.list_datasource)
        eq_(None, lane.list_datasource_id)
        eq_([customlist1.id], lane.customlist_ids)

        # Now change it so it's based on all CustomLists from a given
        # DataSource.
        source = customlist1.data_source
        lane.list_datasource = source
        eq_(source, lane.list_datasource)
        eq_(source.id, lane.list_datasource_id)

        # The lane is now based on two CustomLists instead of one.
        eq_(set([customlist1.id, customlist2.id]), set(lane.customlist_ids))

    def test_set_audiences(self):
        """Setting Lane.audiences to a single value will
        auto-convert it into a list containing one value.
        """
        lane = self._lane()
        lane.audiences = Classifier.AUDIENCE_ADULT
        eq_([Classifier.AUDIENCE_ADULT], lane.audiences)

    def test_update_size(self):

        # Enable the 'ebooks' and 'audiobooks' entry points.
        self._default_library.setting(EntryPoint.ENABLED_SETTING).value = json.dumps(
            [AudiobooksEntryPoint.INTERNAL_NAME, EbooksEntryPoint.INTERNAL_NAME]
        )

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

        # update_size() sets the Lane's size and size_by_entrypoint to
        # the correct number.
        fiction.size = 100
        fiction.size_by_entrypoint = {"Nonexistent entrypoint": 200}
        fiction.update_size(self._db)

        # The total number of books in the lane, regardless of entrypoint,
        # is stored in .size.
        eq_(1, fiction.size)

        # The lane size is also calculated individually for every
        # enabled entry point. EverythingEntryPoint is used for the
        # total size of the lane.
        expect = {
            EverythingEntryPoint.URI: fiction.size,
            AudiobooksEntryPoint.URI: 0,
            EbooksEntryPoint.URI: fiction.size,
        }
        eq_(expect, fiction.size_by_entrypoint)

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

        eq_([lane, child_lane, grandchild_lane], grandchild_lane.hierarchy)

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

    def test_inherited_value(self):
        # Test WorkList.inherited_value.
        #
        # It's easier to test this in Lane because WorkLists can't have
        # parents.

        # This lane contains fiction.
        fiction_lane = self._lane(fiction=True)

        # This sublane contains nonfiction.
        nonfiction_sublane = self._lane(parent=fiction_lane, fiction=False)
        nonfiction_sublane.inherit_parent_restrictions = False

        # This sublane doesn't specify a value for .fiction.
        default_sublane = self._lane(parent=fiction_lane)
        default_sublane.inherit_parent_restrictions = False

        # When inherit_parent_restrictions is False,
        # inherited_value("fiction") returns whatever value is set for
        # .fiction.
        eq_(None, default_sublane.inherited_value("fiction"))
        eq_(False, nonfiction_sublane.inherited_value("fiction"))

        # When inherit_parent_restrictions is True,
        # inherited_value("fiction") returns False for the sublane
        # that sets no value for .fiction.
        default_sublane.inherit_parent_restrictions = True
        eq_(True, default_sublane.inherited_value("fiction"))

        # The sublane that sets its own value for .fiction is unaffected.
        nonfiction_sublane.inherit_parent_restrictions = True
        eq_(False, nonfiction_sublane.inherited_value("fiction"))

    def test_inherited_values(self):
        # Test WorkList.inherited_values.
        #
        # It's easier to test this in Lane because WorkLists can't have
        # parents.

        # This lane contains best-sellers.
        best_sellers_lane = self._lane()
        best_sellers, ignore = self._customlist(num_entries=0)
        best_sellers_lane.customlists.append(best_sellers)

        # This sublane contains staff picks.
        staff_picks_lane = self._lane(parent=best_sellers_lane)
        staff_picks, ignore = self._customlist(num_entries=0)
        staff_picks_lane.customlists.append(staff_picks)

        # What does it mean that the 'staff picks' lane is *inside*
        # the 'best sellers' lane?

        # If inherit_parent_restrictions is False, it doesn't mean
        # anything in particular. This lane contains books that
        # are on the staff picks list.
        staff_picks_lane.inherit_parent_restrictions = False
        eq_([[staff_picks]], staff_picks_lane.inherited_values('customlists'))

        # If inherit_parent_restrictions is True, then the lane
        # has *two* sets of restrictions: a book must be on both
        # the staff picks list *and* the best sellers list.
        staff_picks_lane.inherit_parent_restrictions = True
        x = staff_picks_lane.inherited_values('customlists')
        eq_(sorted([[staff_picks], [best_sellers]]),
            sorted(staff_picks_lane.inherited_values('customlists')))

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
        # Searching a Lane calls search() on its search_target.
        #
        # TODO: This test could be trimmed down quite a bit with
        # mocks.

        work = self._work(with_license_pool=True)
        self.add_to_materialized_view(work)

        lane = self._lane()
        search_client = MockExternalSearchIndex()
        search_client.bulk_update([work])

        pagination = Pagination(offset=0, size=1)

        results = lane.search(
            self._db, work.title, search_client, pagination=pagination
        )
        target_results = lane.search_target.search(
            self._db, work.title, search_client, pagination=pagination
        )
        eq_(results, target_results)

        # The single search result was returned as a Work.
        [result] = results
        eq_(work, result)

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
            base_query = self._db.query(Work).join(
                LicensePool, LicensePool.work_id==Work.id
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
        qu = self._db.query(Work)
        eq_(
            (qu, None),
            lane.bibliographic_filter_clause(self._db, qu, False, False)
        )

    def test_bibliographic_filter_clause_medium_restriction(self):
        book = self._work(fiction=False, with_license_pool=True)
        eq_(Edition.BOOK_MEDIUM, book.presentation_edition.medium)
        lane = self._lane()
        self.add_to_materialized_view([book])

        def matches(lane):
            qu = self._db.query(Work)
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
            qu = self._db.query(Work)
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
        # Lane.groups propagates a received Facets object into
        # _groups_for_lanes.
        def mock(self, _db, relevant_lanes, queryable_lanes, facets, *args, **kwargs):
            self.called_with = facets
            return []
        old_value = Lane._groups_for_lanes
        Lane._groups_for_lanes = mock
        lane = self._lane()
        facets = FeaturedFacets(0)
        lane.groups(self._db, facets=facets)
        eq_(facets, lane.called_with)
        Lane._groups_for_lanes = old_value


class TestWorkListGroupsEndToEnd(EndToEndSearchTest):
    # A comprehensive end-to-end test of WorkList.groups()
    # using a real Elasticsearch index.
    #
    # Helper methods are tested in a different class, TestWorkListGroups

    def populate_works(self):
        def _w(**kwargs):
            """Helper method to create a work with license pool."""
            return self._work(with_license_pool=True, **kwargs)

        # In this library, the groups feed includes at most two books
        # for each lane.
        library = self._default_library
        library.setting(library.FEATURED_LANE_SIZE).value = "2"

        # Create eight works.
        self.hq_litfic = _w(title="HQ LitFic", fiction=True, genre='Literary Fiction')
        self.hq_litfic.quality = 0.8
        self.lq_litfic = _w(title="LQ LitFic", fiction=True, genre='Literary Fiction')
        self.lq_litfic.quality = 0
        self.hq_sf = _w(title="HQ SF", genre="Science Fiction", fiction=True)

        # Add a lot of irrelevant genres to one of the works. This
        # will clutter up the materialized view, but it won't affect
        # the results.
        for genre in ['Westerns', 'Horror', 'Erotica']:
            genre_obj, is_new = Genre.lookup(self._db, genre)
            get_one_or_create(self._db, WorkGenre, work=self.hq_sf, genre=genre_obj)

        self.hq_sf.quality = 0.8
        self.mq_sf = _w(title="MQ SF", genre="Science Fiction", fiction=True)
        self.mq_sf.quality = 0.6
        self.lq_sf = _w(title="LQ SF", genre="Science Fiction", fiction=True)
        self.lq_sf.quality = 0.1
        self.hq_ro = _w(title="HQ Romance", genre="Romance", fiction=True)
        self.hq_ro.quality = 0.8
        self.mq_ro = _w(title="MQ Romance", genre="Romance", fiction=True)
        self.mq_ro.quality = 0.6
        # This work is in a different language -- necessary to run the
        # LQRomanceEntryPoint test below.
        self.lq_ro = _w(title="LQ Romance", genre="Romance", fiction=True, language='lan')
        self.lq_ro.quality = 0.1
        self.nonfiction = _w(title="Nonfiction", fiction=False)

        # One of these works (mq_sf) is a best-seller and also a staff
        # pick.
        self.best_seller_list, ignore = self._customlist(num_entries=0)
        self.best_seller_list.add_entry(self.mq_sf)

        self.staff_picks_list, ignore = self._customlist(num_entries=0)
        self.staff_picks_list.add_entry(self.mq_sf)

    def test_groups(self):
        if not self.search:
            return

        # Create a 'Fiction' lane with five sublanes.
        fiction = self._lane("Fiction")
        fiction.fiction = True

        # "Best Sellers", which will contain one book.
        best_sellers = self._lane(
            "Best Sellers", parent=fiction
        )
        best_sellers.customlists.append(self.best_seller_list)

        # "Staff Picks", which will contain the same book.
        staff_picks = self._lane(
            "Staff Picks", parent=fiction
        )
        staff_picks.customlists.append(self.staff_picks_list)

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

        def make_groups(lane, facets=None, **kwargs):
            # Run the `WorkList.groups` method in a way that's
            # instrumented for this unit test.

            # Most of the time, we want a simple deterministic query.
            facets = facets or FeaturedFacets(
                1, random_seed=FeaturedFacets.DETERMINISTIC
            )

            return lane.groups(
                self._db, facets=facets, search_engine=self.search, debug=True,
                **kwargs
            )

        assert_contents(
            make_groups(fiction),
            [
                # The lanes based on lists feature every title on the
                # list.  This isn't enough to pad out the lane to
                # FEATURED_LANE_SIZE, but nothing else belongs in the
                # lane.
                (self.mq_sf, best_sellers),

                # In fact, both lanes feature the same title -- this
                # generally won't happen but it can happen when
                # multiple lanes are based on lists that feature the
                # same title.
                (self.mq_sf, staff_picks),

                # The genre-based lanes contain FEATURED_LANE_SIZE
                # (two) titles each. The 'Science Fiction' lane
                # features a low-quality work because the
                # medium-quality work was already used above.
                (self.hq_sf, sf_lane),
                (self.lq_sf, sf_lane),
                (self.hq_ro, romance_lane),
                (self.mq_ro, romance_lane),

                # The 'Discredited Nonfiction' lane contains a single
                # book. There just weren't enough matching books to fill
                # out the lane to FEATURED_LANE_SIZE.
                (self.nonfiction, discredited_nonfiction),

                # The 'Fiction' lane contains a title that fits in the
                # fiction lane but was not classified under any other
                # lane. It also contains a title that was previously
                # featured earlier. The search index knows about a
                # title (lq_litfix) that was not previously featured,
                # but we didn't see it because the Elasticsearch query
                # didn't happen to fetch it.
                #
                # Each lane gets a separate query, and there were too
                # many high-quality works in 'fiction' for the
                # low-quality one to show up.
                (self.hq_litfic, fiction),
                (self.hq_sf, fiction),
            ]
        )

        # If we ask only about 'Fiction', not including its sublanes,
        # we get only the subset of the books previously returned for
        # 'fiction'.
        assert_contents(
            make_groups(fiction, include_sublanes=False),
            [(self.hq_litfic, fiction), (self.hq_sf, fiction)]
        )

        # If we exclude 'Fiction' from its own grouped feed, we get
        # all the other books/lane combinations *except for* the books
        # associated directly with 'Fiction'.
        fiction.include_self_in_grouped_feed = False
        assert_contents(
            make_groups(fiction),
            [
                (self.mq_sf, best_sellers),
                (self.mq_sf, staff_picks),
                (self.hq_sf, sf_lane),
                (self.lq_sf, sf_lane),
                (self.hq_ro, romance_lane),
                (self.mq_ro, romance_lane),
                (self.nonfiction, discredited_nonfiction),
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
                [(self.nonfiction, discredited_nonfiction)]
            )

        # If we make the lanes thirstier for content, we see slightly
        # different behavior.
        library = self._default_library
        library.setting(library.FEATURED_LANE_SIZE).value = "3"
        assert_contents(
            make_groups(fiction),
            [
                # The list-based lanes are the same as before.
                (self.mq_sf, best_sellers),
                (self.mq_sf, staff_picks),

                # After using every single science fiction work that
                # wasn't previously used, we reuse self.mq_sf to pad the
                # "Science Fiction" lane up to three items. It's
                # better to have self.lq_sf show up before self.mq_sf, even
                # though it's lower quality, because self.lq_sf hasn't been
                # used before.
                (self.hq_sf, sf_lane),
                (self.lq_sf, sf_lane),
                (self.mq_sf, sf_lane),

                # The 'Romance' lane now contains all three Romance
                # titles, with the higher-quality titles first.
                (self.hq_ro, romance_lane),
                (self.mq_ro, romance_lane),
                (self.lq_ro, romance_lane),

                # The 'Discredited Nonfiction' lane is the same as
                # before.
                (self.nonfiction, discredited_nonfiction),

                # After using every single fiction work that wasn't
                # previously used, we reuse high-quality works to pad
                # the "Fiction" lane to three items. The
                # lowest-quality Romance title doesn't show up here
                # anymore, because the 'Romance' lane claimed it. If
                # we have to reuse titles, we'll reuse the
                # high-quality ones.
                (self.hq_litfic, fiction),
                (self.hq_sf, fiction),
                (self.hq_ro, fiction),
            ]
        )

        # Let's see how entry points affect the feeds.
        #

        # There are no audiobooks in the system, so passing in a
        # FeaturedFacets scoped to the AudiobooksEntryPoint excludes everything.
        facets = FeaturedFacets(0, entrypoint=AudiobooksEntryPoint)
        _db = self._db
        eq_([], list(fiction.groups(self._db, facets=facets)))

        # Here's an entry point that applies a language filter
        # that only finds one book.
        class LQRomanceEntryPoint(object):
            URI = ""
            @classmethod
            def modify_search_filter(cls, filter):
                filter.languages = ['lan']
        facets = FeaturedFacets(
            1, entrypoint=LQRomanceEntryPoint,
            random_seed=FeaturedFacets.DETERMINISTIC
        )
        assert_contents(
            make_groups(fiction, facets=facets),
            [
                # The single recognized book shows up in both lanes
                # that can show it.
                (self.lq_ro, romance_lane),
                (self.lq_ro, fiction),
            ]
        )

        # Now, instead of relying on the 'Fiction' lane, make a
        # WorkList containing two different lanes, and call groups() on
        # the WorkList.

        class MockWorkList(object):

            display_name = "Mock"
            visible = True
            priority = 2

            def groups(slf, _db, include_sublanes, facets=None):
                yield self.lq_litfic, slf

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
                (self.mq_sf, best_sellers),
                (self.mq_sf, staff_picks),
                (self.lq_litfic, mock),
            ]
        )


class TestWorkListGroups(DatabaseTest):

    def setup(self):
        super(TestWorkListGroups, self).setup()

        # Make sure random selections and range generations go the
        # same way every time.
        random.seed(42)

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
        # featured_works uses facets when it calls works_from_database().
        class Mock(WorkList):
            def works_from_database(self, _db, facets):
                self.works_from_database_called_with = facets
                return []

        wl = Mock()
        wl.initialize(library=self._default_library)
        facets = FeaturedFacets(
            minimum_featured_quality = object(),
            uses_customlists = object(),
            entrypoint=AudiobooksEntryPoint
        )
        groups = list(wl.featured_works(self._db, facets))
        eq_(facets, wl.works_from_database_called_with)

        # If no FeaturedFacets object is specified, one is created
        # based on default library configuration.
        groups = list(wl.featured_works(self._db, None))
        facets2 = wl.works_from_database_called_with
        eq_(self._default_library.minimum_featured_quality,
            facets2.minimum_featured_quality)
        eq_(wl.uses_customlists, facets2.uses_customlists)

    def test_featured_works_with_lanes(self):
        # _featured_works_with_lanes calls works_from_search_index
        # on every lane we pass in to it.
        class Mock(object):
            """A Mock of Lane.works_from_search_index."""

            def __init__(self, mock_works):
                self.mock_works = mock_works

            def works(self, _db, facets, pagination, *args, **kwargs):
                self.called_with = [_db, facets, pagination]
                return [self.mock_works]

        mock1 = Mock(["work1", "work2"])
        mock2 = Mock(["workA", "workB"])

        lane = self._lane()
        facets = FeaturedFacets(0.1)
        pagination = object()
        results = lane._featured_works_with_lanes(
            self._db, [mock1, mock2], facets, pagination, search_engine=object()
        )

        # The results of works_in_window were annotated with the
        # 'lane' that produced the result.
        eq_([(['work1', 'work2'], mock1), (['workA', 'workB'], mock2)],
            list(results))

        # Each Mock's works_in_window was called with the same
        # arguments.
        eq_(mock1.called_with, mock2.called_with)

        # The Facets object passed in to _featured_works_with_lanes()
        # is passed on into works_from_search_index().
        _db, called_with_facets, pagination = mock1.called_with
        eq_(self._db, _db)
        eq_(facets, called_with_facets)
        eq_(pagination, pagination)

    def test__size_for_facets(self):

        lane = self._lane()
        m = lane._size_for_facets

        ebooks, audio, everything, nothing = [
            FeaturedFacets(minimum_featured_quality=0.5, entrypoint=x)
            for x in (
                EbooksEntryPoint, AudiobooksEntryPoint, EverythingEntryPoint,
                None
            )
        ]

        # When Lane.size_by_entrypoint is not set, Lane.size is used.
        # This should only happen immediately after a site is upgraded.
        lane.size = 100
        for facets in (ebooks, audio):
            eq_(100, lane._size_for_facets(facets))

        # Once Lane.size_by_entrypoint is set, it's used when possible.
        lane.size_by_entrypoint = {
            EverythingEntryPoint.URI : 99,
            EbooksEntryPoint.URI : 1,
            AudiobooksEntryPoint.URI : 2
        }
        eq_(99, m(None))
        eq_(99, m(nothing))
        eq_(99, m(everything))
        eq_(1, m(ebooks))
        eq_(2, m(audio))

        # If size_by_entrypoint contains no estimate for a given
        # EntryPoint URI, the overall lane size is used. This can
        # happen between the time an EntryPoint is enabled and the
        # materialized view refresh script is run.
        del lane.size_by_entrypoint[AudiobooksEntryPoint.URI]
        eq_(100, m(audio))
