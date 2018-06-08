from collections import defaultdict
from nose.tools import set_trace
import datetime
import logging
import random
import time
import urllib

from psycopg2.extras import NumericRange
from sqlalchemy.sql import select
from sqlalchemy.sql.expression import Select

from config import Configuration
from flask_babel import lazy_gettext as _

import classifier
from classifier import (
    Classifier,
    GenreData,
)

from sqlalchemy import (
    and_,
    case,
    or_,
    not_,
    Integer,
    Table,
    Unicode,
)
from sqlalchemy.ext.associationproxy import (
    association_proxy,
)
from sqlalchemy.ext.hybrid import (
    hybrid_property,
)
from sqlalchemy.orm import (
    aliased,
    backref,
    contains_eager,
    defer,
    joinedload,
    lazyload,
    relationship,
)
from sqlalchemy.sql.expression import literal

from entrypoint import (
    EntryPoint,
    EverythingEntryPoint,
)
from model import (
    directly_modified,
    get_one_or_create,
    numericrange_to_tuple,
    site_configuration_has_changed,
    tuple_to_numericrange,
    Base,
    CachedFeed,
    CustomList,
    CustomListEntry,
    DataSource,
    DeliveryMechanism,
    Edition,
    Genre,
    get_one,
    Library,
    LicensePool,
    LicensePoolDeliveryMechanism,
    Session,
    Work,
    WorkGenre,
)
from facets import FacetConstants
from problem_details import *
from util import (
    fast_query_count,
    LanguageCodes,
)
from util.problem_detail import ProblemDetail

import elasticsearch

from sqlalchemy import (
    event,
    Boolean,
    Column,
    ForeignKey,
    Integer,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import (
    ARRAY,
    INT4RANGE,
)

class FacetsWithEntryPoint(FacetConstants):
    """Basic Facets class that knows how to filter a query based on a
    selected EntryPoint.
    """
    def __init__(self, entrypoint=None, **kwargs):
        """Constructor.

        :param entrypoint: An EntryPoint (optional).
        :param kwargs: Other arguments may be supplied based on user
            input, but the default implementation is to ignore them.
        """
        self.entrypoint = entrypoint
        self.constructor_kwargs = kwargs

    def navigate(self, entrypoint):
        """Create a very similar FacetsWithEntryPoint that points to
        a different EntryPoint.
        """
        return self.__class__(
            entrypoint=entrypoint, **self.constructor_kwargs
        )

    @classmethod
    def from_request(
            cls, library, facet_config, get_argument, worklist, **extra_kwargs
    ):
        """Load a faceting object from an HTTP request.

        :param facet_config: A Library (or mock of one) that knows
           which subset of the available facets are configured.

        :param get_argument: A callable that takes one argument and
           retrieves (or pretends to retrieve) a query string
           parameter of that name from an incoming HTTP request.

        :param worklist: A WorkList associated with the current request,
           if any.

        :param extra_kwargs: A dictionary of keyword arguments to pass
           into the constructor when a faceting object is instantiated.

        :return: A FacetsWithEntryPoint, or a ProblemDetail if there's
            a problem with the input from the request.
        """
        return cls._from_request(
            facet_config, get_argument, worklist, **extra_kwargs
        )

    @classmethod
    def _from_request(
            cls, facet_config, get_argument, worklist, **extra_kwargs
    ):
        """Load a faceting object from an HTTP request.

        Subclasses of FacetsWithEntryPoint can override `from_request`,
        but call this method to load the EntryPoint and actually
        instantiate the faceting class.
        """
        entrypoint_name = get_argument(
            Facets.ENTRY_POINT_FACET_GROUP_NAME, None
        )
        entrypoint = cls.load_entrypoint(
            entrypoint_name, list(facet_config.entrypoints)
        )
        if isinstance(entrypoint, ProblemDetail):
            return entrypoint
        return cls(entrypoint=entrypoint, **extra_kwargs)

    @classmethod
    def selectable_entrypoints(cls, worklist):
        """Which EntryPoints can be selected for these facets on this
        WorkList?

        In most cases, there are no selectable EntryPoints; this generally
        happens only at the top level.

        By default, this is completely determined by the WorkList.
        See SearchFacets for an example that changes this.
        """
        if not worklist:
            return []
        return worklist.entrypoints

    @classmethod
    def load_entrypoint(cls, name, valid_entrypoints):
        """Look up an EntryPoint by name, assuming it's valid in the
        given WorkList.

        :param valid_entrypoints: The EntryPoints that might be
        valid. This is probably not the value of
        WorkList.selectable_entrypoints, because an EntryPoint
        selected in a WorkList remains valid (but not selectable) for
        all of its children.

        :return: An EntryPoint class. This will be the requested
        EntryPoint if possible. If a nonexistent or unusable
        EntryPoint is requested, the first valid EntryPoint will be
        returned. If there are no valid EntryPoints, None will be
        returned.
        """
        if not valid_entrypoints:
            return None
        default = valid_entrypoints[0]
        ep = EntryPoint.BY_INTERNAL_NAME.get(name)
        if not ep or ep not in valid_entrypoints:
            return default
        return ep

    def items(self):
        """Yields a 2-tuple for every active facet setting.

        In this class that just means the entrypoint.
        """
        if self.entrypoint:
            yield (self.ENTRY_POINT_FACET_GROUP_NAME,
                   self.entrypoint.INTERNAL_NAME)

    @property
    def query_string(self):
        """A query string fragment that propagates all active facet
        settings.
        """
        return "&".join("=".join(x) for x in sorted(self.items()))

    def apply(self, _db, qu):
        """Modify the given query based on the EntryPoint associated
        with this object.
        """
        if self.entrypoint:
            qu = self.entrypoint.apply(qu)
        return qu


class Facets(FacetsWithEntryPoint):
    """A full-fledged facet class that supports complex navigation between
    multiple facet groups.

    Despite the generic name, this is only used in 'page' type OPDS
    feeds that list all the works in some WorkList.
    """
    @classmethod
    def default(cls, library):
        return cls(
            library,
            collection=cls.COLLECTION_MAIN,
            availability=cls.AVAILABLE_ALL,
            order=cls.ORDER_AUTHOR
        )

    @classmethod
    def from_request(cls, library, config, get_argument, worklist, **extra):
        """Load a faceting object from an HTTP request."""
        g = Facets.ORDER_FACET_GROUP_NAME
        order = get_argument(g, config.default_facet(g))
        order_facets = config.enabled_facets(Facets.ORDER_FACET_GROUP_NAME)
        if order and not order in order_facets:
            return INVALID_INPUT.detailed(
                _("I don't know how to order a feed by '%(order)s'", order=order),
                400
            )
        extra['order'] = order

        g = Facets.AVAILABILITY_FACET_GROUP_NAME
        availability = get_argument(g, config.default_facet(g))
        availability_facets = config.enabled_facets(
            Facets.AVAILABILITY_FACET_GROUP_NAME
        )
        if availability and not availability in availability_facets:
            return INVALID_INPUT.detailed(
                _("I don't understand the availability term '%(availability)s'", availability=availability),
                400
            )
        extra['availability'] = availability

        g = Facets.COLLECTION_FACET_GROUP_NAME
        collection = get_argument(g, config.default_facet(g))
        collection_facets = config.enabled_facets(
            Facets.COLLECTION_FACET_GROUP_NAME
        )
        if collection and not collection in collection_facets:
            return INVALID_INPUT.detailed(
                _("I don't understand what '%(collection)s' refers to.", collection=collection),
                400
            )
        extra['collection'] = collection

        extra['enabled_facets'] = {
            Facets.ORDER_FACET_GROUP_NAME : order_facets,
            Facets.AVAILABILITY_FACET_GROUP_NAME : availability_facets,
            Facets.COLLECTION_FACET_GROUP_NAME : collection_facets,
        }
        extra['library'] = library

        return cls._from_request(config, get_argument, worklist, **extra)

    def __init__(self, library, collection, availability, order,
                 order_ascending=None, enabled_facets=None, entrypoint=None):
        """Constructor.

        :param collection: This is not a Collection object; it's a value for
        the 'collection' facet, e.g. 'main' or 'featured'.

        :param entrypoint: An EntryPoint class. The 'entry point'
        facet group is configured on a per-WorkList basis rather than
        a per-library basis.
        """
        super(Facets, self).__init__(entrypoint)
        if order_ascending is None:
            if order == self.ORDER_ADDED_TO_COLLECTION:
                order_ascending = self.ORDER_DESCENDING
            else:
                order_ascending = self.ORDER_ASCENDING

        collection = collection or library.default_facet(
            self.COLLECTION_FACET_GROUP_NAME
        )
        availability = availability or library.default_facet(
            self.AVAILABILITY_FACET_GROUP_NAME
        )
        order = order or library.default_facet(self.ORDER_FACET_GROUP_NAME)

        if (availability == self.AVAILABLE_ALL and (library and not library.allow_holds)):
            # Under normal circumstances we would show all works, but
            # library configuration says to hide books that aren't
            # available.
            availability = self.AVAILABLE_NOW

        self.library = library
        self.collection = collection
        self.availability = availability
        self.order = order
        if order_ascending == self.ORDER_ASCENDING:
            order_ascending = True
        elif order_ascending == self.ORDER_DESCENDING:
            order_ascending = False
        self.order_ascending = order_ascending
        self.facets_enabled_at_init = enabled_facets

    def navigate(self, collection=None, availability=None, order=None,
                 entrypoint=None):
        """Create a slightly different Facets object from this one."""
        return self.__class__(self.library,
                              collection or self.collection,
                              availability or self.availability,
                              order or self.order,
                              enabled_facets=self.facets_enabled_at_init,
                              entrypoint=(entrypoint or self.entrypoint)
        )

    def items(self):
        for k,v in super(Facets, self).items():
            yield k, v
        if self.order:
            yield (self.ORDER_FACET_GROUP_NAME, self.order)
        if self.availability:
            yield (self.AVAILABILITY_FACET_GROUP_NAME,  self.availability)
        if self.collection:
            yield (self.COLLECTION_FACET_GROUP_NAME, self.collection)

    @property
    def enabled_facets(self):
        """Yield a 3-tuple of lists (order, availability, collection)
        representing facet values enabled via initialization or Configuration

        The 'entry point' facet group is handled separately, since it
        is not always used.
        """
        if self.facets_enabled_at_init:
            # When this Facets object was initialized, a list of enabled
            # facets was passed. We'll only work with those facets.
            facet_types = [
                self.ORDER_FACET_GROUP_NAME,
                self.AVAILABILITY_FACET_GROUP_NAME,
                self.COLLECTION_FACET_GROUP_NAME
            ]
            for facet_type in facet_types:
                yield self.facets_enabled_at_init.get(facet_type, [])
        else:
            order_facets = self.library.enabled_facets(
                Facets.ORDER_FACET_GROUP_NAME
            )
            yield order_facets

            availability_facets = self.library.enabled_facets(
                Facets.AVAILABILITY_FACET_GROUP_NAME
            )
            yield availability_facets

            collection_facets = self.library.enabled_facets(
                Facets.COLLECTION_FACET_GROUP_NAME
            )
            yield collection_facets

    @property
    def facet_groups(self):
        """Yield a list of 4-tuples
        (facet group, facet value, new Facets object, selected)
        for use in building OPDS facets.

        This does not yield anything for the 'entry point' facet group,
        which must be handled separately.
        """

        order_facets, availability_facets, collection_facets = self.enabled_facets

        def dy(new_value):
            group = self.ORDER_FACET_GROUP_NAME
            current_value = self.order
            facets = self.navigate(order=new_value)
            return (group, new_value, facets, current_value==new_value)

        # First, the order facets.
        if len(order_facets) > 1:
            for facet in order_facets:
                yield dy(facet)

        # Next, the availability facets.
        def dy(new_value):
            group = self.AVAILABILITY_FACET_GROUP_NAME
            current_value = self.availability
            facets = self.navigate(availability=new_value)
            return (group, new_value, facets, new_value==current_value)

        if len(availability_facets) > 1:
            for facet in availability_facets:
                yield dy(facet)

        # Next, the collection facets.
        def dy(new_value):
            group = self.COLLECTION_FACET_GROUP_NAME
            current_value = self.collection
            facets = self.navigate(collection=new_value)
            return (group, new_value, facets, new_value==current_value)

        if len(collection_facets) > 1:
            for facet in collection_facets:
                yield dy(facet)

    @classmethod
    def order_facet_to_database_field(cls, order_facet):
        """Turn the name of an order facet into a materialized-view field
        for use in an ORDER BY clause.
        """
        from model import MaterializedWorkWithGenre as work_model
        order_facet_to_database_field = {
            cls.ORDER_ADDED_TO_COLLECTION: work_model.availability_time,
            cls.ORDER_WORK_ID : work_model.works_id,
            cls.ORDER_TITLE : work_model.sort_title,
            cls.ORDER_AUTHOR : work_model.sort_author,
            cls.ORDER_LAST_UPDATE : work_model.last_update_time,
            cls.ORDER_SERIES_POSITION : work_model.series_position,
            cls.ORDER_RANDOM : work_model.random,
        }
        return order_facet_to_database_field[order_facet]

    def apply(self, _db, qu):
        """Restrict a query against MaterializedWorkWithGenre so that it only
        matches works that fit the given facets, and the query is
        ordered appropriately.
        """
        qu = super(Facets, self).apply(_db, qu)
        from model import MaterializedWorkWithGenre as work_model
        if self.availability == self.AVAILABLE_NOW:
            availability_clause = or_(
                LicensePool.open_access==True,
                LicensePool.licenses_available > 0)
        elif self.availability == self.AVAILABLE_ALL:
            availability_clause = or_(
                LicensePool.open_access==True,
                LicensePool.licenses_owned > 0)
        elif self.availability == self.AVAILABLE_OPEN_ACCESS:
            availability_clause = LicensePool.open_access==True
        qu = qu.filter(availability_clause)

        if self.collection == self.COLLECTION_FULL:
            # Include everything.
            pass
        elif self.collection == self.COLLECTION_MAIN:
            # Exclude open-access books with a quality of less than
            # 0.3.
            or_clause = or_(
                LicensePool.open_access==False,
                work_model.quality >= 0.3
            )
            qu = qu.filter(or_clause)
        elif self.collection == self.COLLECTION_FEATURED:
            # Exclude books with a quality of less than the library's
            # minimum featured quality.
            qu = qu.filter(
                work_model.quality >= self.library.minimum_featured_quality
            )

        # Set the ORDER BY clause.
        order_by, order_distinct = self.order_by()
        qu = qu.order_by(*order_by)

        # We always mark the query as distinct because the materialized
        # view can contain the same title many times.
        qu = qu.distinct(*order_distinct)

        return qu

    def order_by(self):
        """Given these Facets, create a complete ORDER BY clause for queries
        against WorkModelWithGenre.
        """
        from model import MaterializedWorkWithGenre as work_model
        work_id = work_model.works_id
        default_sort_order = [
            work_model.sort_author, work_model.sort_title, work_id
        ]

        primary_order_by = self.order_facet_to_database_field(self.order)
        if primary_order_by is not None:
            # Promote the field designated by the sort facet to the top of
            # the order-by list.
            order_by = [primary_order_by]

            for i in default_sort_order:
                if i not in order_by:
                    order_by.append(i)
        else:
            # Use the default sort order
            order_by = default_order_by

        # order_ascending applies only to the first field in the sort order.
        # For now, everything else is ordered ascending.
        if self.order_ascending:
            order_by_sorted = [x.asc() for x in order_by]
        else:
            order_by_sorted = [order_by[0].desc()] + [x.asc() for x in order_by[1:]]
        return order_by_sorted, order_by


class FeaturedFacets(FacetsWithEntryPoint):

    """A simple faceting object that configures a query so that the 'most
    featurable' items are at the front.

    This is mainly a convenient thing to pass into
    AcquisitionFeed.groups().
    """

    def __init__(self, minimum_featured_quality, uses_customlists=False,
                 entrypoint=None, **kwargs):
        """Set up an object that finds featured books in a given
        WorkList.

        :param kwargs: Other arguments may be supplied based on user
            input, but the default implementation is to ignore them.
        """
        super(FeaturedFacets, self).__init__(entrypoint)
        self.minimum_featured_quality = minimum_featured_quality
        self.uses_customlists = uses_customlists

    def navigate(self, minimum_featured_quality=None, uses_customlists=None,
                 entrypoint=None):
        """Create a slightly different FeaturedFacets object based on this
        one.
        """
        minimum_featured_quality = minimum_featured_quality or self.minimum_featured_quality
        if uses_customlists is None:
            uses_customlists = self.uses_customlists
        entrypoint = entrypoint or self.entrypoint
        return self.__class__(
            minimum_featured_quality, uses_customlists, entrypoint
        )

    def apply(self, _db, qu):
        """Order a query by quality tier, and then randomly.

        This isn't usually necessary because works_in_window orders
        items by quality tier, then randomly, but if you want to call
        apply() on a query to get a featured subset of that query,
        this will work.
        """
        from model import MaterializedWorkWithGenre as work_model
        qu = super(FeaturedFacets, self).apply(_db, qu)
        quality = self.quality_tier_field()
        qu = qu.order_by(
            quality.desc(), work_model.random.desc(), work_model.works_id
        )
        qu = qu.distinct(quality, work_model.random, work_model.works_id)
        return qu

    def quality_tier_field(self):
        """A selectable field that summarizes the overall quality of a work
        from a materialized view as a single numeric value.

        Works of featurable quality will have a higher number than
        works not of featurable quality; works that are available now
        will have a higher number than works not currently available;
        and so on.

        The tiers correspond roughly to the selectable facets, but
        this is a historical quirk and could change in the future.

        Using this field in an ORDER BY statement ensures that
        higher-quality works show up at the beginning of the
        results. But if there aren't enough high-quality works,
        lower-quality works will show up later on in the results,
        eliminating the need to find lower-quality works with a second
        query.
        """
        if hasattr(self, '_quality_tier_field'):
            return self._quality_tier_field
        from model import MaterializedWorkWithGenre as mwg
        featurable_quality = self.minimum_featured_quality

        # Being of featureable quality is great.
        featurable_quality = case(
            [(mwg.quality >= featurable_quality, 5)],
            else_=0
        )

        # Being a licensed work or an open-access work of decent quality
        # is good.
        regular_collection = case(
            [(or_(LicensePool.open_access==False, mwg.quality >= 0.3), 2)],
            else_=0
        )

        # All else being equal, it's better if a book is available
        # now.
        available_now = case(
            [(or_(LicensePool.licenses_available > 0,
                  LicensePool.open_access==True), 1)],
            else_=0
        )

        tier = featurable_quality + regular_collection + available_now
        if self.uses_customlists:
            # Being explicitly featured in your CustomListEntry is the
            # best.
            featured_on_list = case(
                [(CustomListEntry.featured, 11)], else_=0
            )
            tier = tier + featured_on_list
        tier = tier.label("quality_tier")
        self._quality_tier_field = tier
        return self._quality_tier_field


class SearchFacets(FacetsWithEntryPoint):

    @classmethod
    def selectable_entrypoints(cls, worklist):
        """If the WorkList has more than one facet, an 'everything' facet
        is added for search purposes.
        """
        if not worklist:
            return []
        entrypoints = list(worklist.entrypoints)
        if len(entrypoints) < 2:
            return entrypoints
        if EverythingEntryPoint not in entrypoints:
            entrypoints.insert(0, EverythingEntryPoint)
        return entrypoints


class Pagination(object):

    DEFAULT_SIZE = 50
    DEFAULT_SEARCH_SIZE = 10
    DEFAULT_FEATURED_SIZE = 10

    @classmethod
    def default(cls):
        return Pagination(0, cls.DEFAULT_SIZE)

    def __init__(self, offset=0, size=DEFAULT_SIZE):
        """Constructor.

        :param offset: Start pulling entries from the query at this index.
        :param size: Pull no more than this number of entries from the query.
        """
        self.offset = offset
        self.size = size
        self.total_size = None
        self.this_page_size = None

    def items(self):
        yield("after", self.offset)
        yield("size", self.size)

    @property
    def query_string(self):
       return "&".join("=".join(map(str, x)) for x in self.items())

    @property
    def first_page(self):
        return Pagination(0, self.size)

    @property
    def next_page(self):
        return Pagination(self.offset+self.size, self.size)

    @property
    def previous_page(self):
        if self.offset <= 0:
            return None
        previous_offset = self.offset - self.size
        previous_offset = max(0, previous_offset)
        return Pagination(previous_offset, self.size)

    @property
    def has_next_page(self):
        """Returns boolean reporting whether pagination is done for a query

        Either `total_size` or `this_page_size` must be set for this
        method to be accurate.
        """
        if self.total_size is not None:
            # We know the total size of the result set, so we know
            # whether or not there are more results.
            return self.offset + self.size < self.total_size
        if self.this_page_size is not None:
            # We know the number of items on the current page. If this
            # page was empty, we can assume there is no next page; if
            # not, we can assume there is a next page. This is a little
            # more conservative than checking whether we have a 'full'
            # page.
            return self.this_page_size > 0

        # We don't know anything about this result set, so assume there is
        # a next page.
        return True

    def apply(self, qu):
        """Modify the given query with OFFSET and LIMIT."""
        return qu.offset(self.offset).limit(self.size)


class WorkList(object):
    """An object that can obtain a list of Work/MaterializedWorkWithGenre
    objects for use in generating an OPDS feed.
    """

    # Unless a sitewide setting intervenes, the set of Works in a
    # WorkList is cacheable for two weeks by default.
    MAX_CACHE_AGE = 14*24*60*60

    # By default, a WorkList is always visible.
    visible = True

    # By default, a WorkList does not draw from CustomLists
    uses_customlists = False

    @classmethod
    def top_level_for_library(self, _db, library):
        """Create a WorkList representing this library's collection
        as a whole.

        If no top-level visible lanes are configured, the WorkList
        will be configured to show every book in the collection.

        If a single top-level Lane is configured, it will returned as
        the WorkList.

        Otherwise, a WorkList containing the visible top-level lanes
        is returned.
        """
        # Load all of this Library's visible top-level Lane objects
        # from the database.
        top_level_lanes = _db.query(Lane).filter(
            Lane.library==library
        ).filter(
            Lane.parent==None
        ).filter(
            Lane._visible==True
        ).order_by(
            Lane.priority
        ).all()

        if len(top_level_lanes) == 1:
            # The site configuration includes a single top-level lane;
            # this can stand in for the library on its own.
            return top_level_lanes[0]

        # This WorkList contains every title available to this library
        # in one of the media supported by the default client.
        wl = WorkList()

        wl.initialize(
            library, display_name=library.name, children=top_level_lanes,
            media=Edition.FULFILLABLE_MEDIA, entrypoints=library.entrypoints
        )
        return wl

    def initialize(self, library, display_name=None, genres=None,
                   audiences=None, languages=None, media=None,
                   children=None, priority=None, entrypoints=None):
        """Initialize with basic data.

        This is not a constructor, to avoid conflicts with `Lane`, an
        ORM object that subclasses this object but does not use this
        initialization code.

        :param library: Only Works available in this Library will be
        included in lists.

        :param display_name: Name to display for this WorkList in the
        user interface.

        :param genres: Only Works classified under one of these Genres
        will be included in lists.

        :param audiences: Only Works classified under one of these audiences
        will be included in lists.

        :param languages: Only Works in one of these languages will be
        included in lists.

        :param media: Only Works in one of these media will be included
        in lists.

        :param children: This WorkList has children, which are also
        WorkLists.

        :param priority: A number indicating where this WorkList should
        show up in relation to its siblings when it is the child of
        some other WorkList.

        :param entrypoints: A list of EntryPoint classes representing
        different ways of slicing up this WorkList.
        """
        self.library_id = None
        self.collection_ids = []
        if library:
            self.library_id = library.id
            self.collection_ids = [
                collection.id for collection in library.all_collections
            ]
        self.display_name = display_name
        if genres:
            self.genre_ids = [x.id for x in genres]
        else:
            self.genre_ids = None
        self.audiences = audiences
        self.languages = languages
        self.media = media

        # By default, a WorkList doesn't have a fiction status or target age.
        # Set them to None so they can be ignored in search on a WorkList, but
        # used when calling search on a Lane.
        self.fiction = None
        self.target_age = None

        self.children = children or []
        self.priority = priority or 0

        if entrypoints:
            self.entrypoints = list(entrypoints)
        else:
            self.entrypoints = []

    def get_library(self, _db):
        """Find the Library object associated with this WorkList."""
        return Library.by_id(_db, self.library_id)

    @property
    def display_name_for_all(self):
        """The display name to use when referring to the set of all books in
        this WorkList, as opposed to the WorkList itself.
        """
        return _("All %(worklist)s", worklist=self.display_name)

    @property
    def visible_children(self):
        """A WorkList's children can be used to create a grouped acquisition
        feed for that WorkList.
        """
        return sorted(
            [x for x in self.children if x.visible],
            key = lambda x: (x.priority, x.display_name)
        )

    @property
    def has_visible_children(self):
        for lane in self.visible_children:
            if lane:
                return True
        return False

    @property
    def parentage(self):
        """WorkLists have no parentage. This method is defined for compatibility
        with Lane.
        """
        return []

    @property
    def customlist_ids(self):
        """WorkLists per se are not associated with custom lists, although
        Lanes might be.
        """
        return None

    @property
    def full_identifier(self):
        """A human-readable identifier for this WorkList that
        captures its position within the heirarchy.
        """
        lane_parentage = list(reversed(list(self.parentage))) + [self]
        full_parentage = [unicode(x.display_name) for x in lane_parentage]
        if getattr(self, 'library', None):
            # This WorkList is associated with a specific library.
            # incorporate the library's name to distinguish between it
            # and other lanes in the same position in another library.
            full_parentage.insert(0, self.library.short_name)
        return " / ".join(full_parentage)

    @property
    def language_key(self):
        """Return a string identifying the languages used in this WorkList.
        This will usually be in the form of 'eng,spa' (English and Spanish).
        """
        key = ""
        if self.languages:
            key += ",".join(sorted(self.languages))
        return key

    @property
    def audience_key(self):
        """Translates audiences list into url-safe string"""
        key = u''
        if (self.audiences and
            Classifier.AUDIENCES.difference(self.audiences)):
            # There are audiences and they're not the default
            # "any audience", so add them to the URL.
            audiences = [urllib.quote_plus(a) for a in sorted(self.audiences)]
            key += ','.join(audiences)
        return key

    def groups(self, _db, include_sublanes=True, facets=None):
        """Extract a list of samples from each child of this WorkList.  This
        can be used to create a grouped acquisition feed for the WorkList.

        :param facets: A FeaturedFacets object, presumably a FeaturedFacets,
        that may restrict the works on view.

        :yield: A sequence of (Work, WorkList) 2-tuples, with each
        WorkList representing the child WorkList in which the Work is
        found.
        """
        if not include_sublanes:
            # We only need to find featured works for this lane,
            # not this lane plus its sublanes.
            for work in self.featured_works(_db, facets=facets):
                yield work, self
            return

        # This is a list rather than a dict because we want to
        # preserve the ordering of the children.
        relevant_lanes = []
        relevant_children = []

        # We use an explicit check for Lane.visible here, instead of
        # iterating over self.visible_children, because Lane.visible only
        # works when the Lane is merged into a database session.
        for child in self.children:
            if isinstance(child, Lane):
                child = _db.merge(child)

            if not child.visible:
                continue

            if isinstance(child, Lane):
                # Children that turn out to be Lanes go into relevant_lanes.
                # Their Works will all be filled in with a single query.
                relevant_lanes.append(child)
            # Both Lanes and WorkLists go into relevant_children.
            # This controls the yield order for Works.
            relevant_children.append(child)

        # _groups_for_lanes will run a query to pull featured works
        # for any children that are Lanes, and call groups()
        # recursively for any children that are not.
        for work, worklist in self._groups_for_lanes(
                _db, relevant_children, relevant_lanes, facets=facets
        ):
            yield work, worklist

    def default_featured_facets(self, _db):
        """Helper method to create a FeaturedFacets object."""
        library = self.get_library(_db)
        return FeaturedFacets(
            minimum_featured_quality=library.minimum_featured_quality,
            uses_customlists=self.uses_customlists
        )

    def featured_works(self, _db, facets=None):
        """Find a random sample of featured books.

        Used when building a grouped OPDS feed for this WorkList's parent.

        :param facets: A FeaturedFacets object.

        :return: A list of MaterializedWorkWithGenre objects.  Under
        no circumstances will a single work show up multiple times in
        this list, even if that means the list contains fewer works
        than anticipated.
        """
        books = []
        book_ids = set()
        featured_subquery = None
        library = self.get_library(_db)
        target_size = library.featured_lane_size

        facets = facets or self.default_featured_facets(_db)
        query = self.works(_db, facets=facets)
        if not query:
            # works() may return None, indicating that the whole
            # thing is a bad idea and the query should not even be
            # run.
            return []

        work_ids = set()
        works = []
        for work in self.random_sample(query, target_size)[:target_size]:
            if isinstance(work, tuple):
                # This is a (work, score) 2-tuple.
                work = work[0]
            if work.works_id not in work_ids:
                works.append(work)
                work_ids.add(work.works_id)
        return works

    def works(self, _db, facets=None, pagination=None, include_quality_tier=False):
        """Create a query against a materialized view that finds Work-like
        objects corresponding to all the Works that belong in this
        WorkList.

        The apply_filters() implementation defines which Works qualify
        for membership in a WorkList of this type.

        :param _db: A database connection.
        :param facets: A Facets object which may put additional
           constraints on WorkList membership.
        :param pagination: A Pagination object indicating which part of
           the WorkList the caller is looking at.
        :return: A Query, or None if the WorkList is deemed to be a
           bad idea in the first place.
        """
        from model import (
            MaterializedWorkWithGenre,
        )
        mw = MaterializedWorkWithGenre
        # apply_filters() will apply the genre
        # restrictions.

        if isinstance(facets, FeaturedFacets):
            field = facets.quality_tier_field()
            qu = _db.query(mw, field)
            if include_quality_tier:
                qu = qu.add_columns(field)
        else:
            qu = _db.query(mw)

        # Apply some database optimizations.
        qu = self._lazy_load(qu)
        qu = self._defer_unused_fields(qu)

        # apply_filters() requires that the query include a join
        # against LicensePool. If nothing else, the `facets` may
        # restrict the query to currently available items.
        qu = qu.join(mw.license_pool)

        if self.collection_ids is not None:
            qu = qu.filter(
                LicensePool.collection_id.in_(self.collection_ids)
            )
            # Also apply the filter on the materialized view --
            # this doesn't seem to do anything, but it's possible that
            # applying the filter here might cause the database to use
            # an index it wouldn't have otherwise used.
            qu = qu.filter(
                mw.collection_id.in_(self.collection_ids)
            )
        qu = self.apply_filters(_db, qu, facets, pagination)
        if qu:
            qu = qu.options(
                contains_eager(mw.license_pool),
                # TODO: Strictly speaking, these joinedload calls are
                # only needed by the circulation manager. This code could
                # be moved to circulation and everyone else who uses this
                # would be a little faster. (But right now there is no one
                # else who uses this.)

                # These speed up the process of generating acquisition links.
                joinedload("license_pool", "delivery_mechanisms"),
                joinedload("license_pool", "delivery_mechanisms", "delivery_mechanism"),
                # These speed up the process of generating the open-access link
                # for open-access works.
                joinedload("license_pool", "delivery_mechanisms", "resource"),
                joinedload("license_pool", "delivery_mechanisms", "resource", "representation"),
            )
        return qu

    def works_for_specific_ids(self, _db, work_ids):
        """Create the appearance of having called works(),
        but return the specific MaterializedWorks identified by `work_ids`.
        """

        # Get a list of MaterializedWorkWithGenre objects as though we
        # had called works().
        from model import MaterializedWorkWithGenre as mw
        qu = _db.query(mw).join(
            LicensePool, mw.license_pool_id==LicensePool.id
        ).filter(
            mw.works_id.in_(work_ids),
            LicensePool.work_id.in_(work_ids),
        ).enable_eagerloads(False)
        qu = self._lazy_load(qu)
        qu = self._defer_unused_fields(qu)
        qu = self.only_show_ready_deliverable_works(_db, qu)
        qu = qu.distinct(mw.works_id)
        work_by_id = dict()
        a = time.time()
        works = qu.all()

        # Put the MaterializedWork objects in the same order as their
        # work_ids were.
        for mw in works:
            work_by_id[mw.works_id] = mw
        results = [work_by_id[x] for x in work_ids if x in work_by_id]

        b = time.time()
        logging.debug(
            "Obtained %d MaterializedWork objects in %.2fsec",
            len(results), b-a
        )
        return results

    def apply_filters(self, _db, qu, facets, pagination, featured=False):
        """Apply common WorkList filters to a query. Also apply any
        subclass-specific filters defined by
        bibliographic_filter_clause().
        """
        from model import MaterializedWorkWithGenre as work_model
        # In general, we only show books that are ready to be delivered
        # to patrons.
        qu = self.only_show_ready_deliverable_works(_db, qu)

        # This method applies whatever filters are necessary to implement
        # the rules of this particular WorkList.
        qu, bibliographic_clause = self.bibliographic_filter_clause(
            _db, qu, featured
        )
        if qu is None:
            # bibliographic_filter_clause() may return a null query to
            # indicate that the WorkList should not exist at all.
            return None
        if bibliographic_clause is not None:
            qu = qu.filter(bibliographic_clause)

        if facets:
            qu = facets.apply(_db, qu)
        else:
            # Ordinarily facets.apply() would take care of ordering
            # the query and making it distinct. In the absence
            # of any ordering information, we will make the query distinct
            # based on work ID.
            qu = qu.distinct(work_model.works_id)

        if pagination:
            qu = pagination.apply(qu)

        return qu

    def bibliographic_filter_clause(self, _db, qu, featured=False):
        """Create a SQLAlchemy filter that excludes books whose bibliographic
        metadata doesn't match what we're looking for.

        :return: A 2-tuple (query, clause).

        - query is either `qu`, or a new query that has been modified to
        join against additional tables.
        """
        # Audience and language restrictions are common to all
        # WorkLists. (So are genre and collection restrictions, bt those
        # were applied back in works().)

        from model import MaterializedWorkWithGenre as work_model
        clauses = self.audience_filter_clauses(_db, qu)
        if self.languages:
            clauses.append(work_model.language.in_(self.languages))
        if self.media:
            clauses.append(work_model.medium.in_(self.media))
        if self.genre_ids:
            already_filtered_genre_id_on_materialized_view = getattr(
                qu, 'genre_id_filtered', False
            )
            if already_filtered_genre_id_on_materialized_view:
                wg = aliased(WorkGenre)
                qu = qu.join(wg, wg.work_id==work_model.works_id)
                field = wg.genre_id
            else:
                qu.genre_id_filtered = True
                field = work_model.genre_id
            clauses.append(field.in_(self.genre_ids))
        if not clauses:
            clause = None
        else:
            clause = and_(*clauses)
        return qu, clause

    def audience_filter_clauses(self, _db, qu):
        """Create a SQLAlchemy filter that excludes books whose intended
        audience doesn't match what we're looking for.
        """
        if not self.audiences:
            return []
        from model import MaterializedWorkWithGenre as work_model
        clauses = [work_model.audience.in_(self.audiences)]
        if (Classifier.AUDIENCE_CHILDREN in self.audiences
            or Classifier.AUDIENCE_YOUNG_ADULT in self.audiences):
            # TODO: A huge hack to exclude Project Gutenberg
            # books (which were deemed appropriate for
            # pre-1923 children but are not necessarily so for
            # 21st-century children.)
            #
            # This hack should be removed in favor of a
            # whitelist system and some way of allowing adults
            # to see books aimed at pre-1923 children.
            gutenberg = DataSource.lookup(_db, DataSource.GUTENBERG)
            clauses.append(LicensePool.data_source_id != gutenberg.id)
        return clauses

    def only_show_ready_deliverable_works(
            self, _db, query, show_suppressed=False
    ):
        """Restrict a query to show only presentation-ready works present in
        an appropriate collection which the default client can
        fulfill.

        Note that this assumes the query has an active join against
        LicensePool.
        """
        from model import MaterializedWorkWithGenre as mwg, Collection
        return Collection.restrict_to_ready_deliverable_works(
            query, mwg, show_suppressed=show_suppressed,
            collection_ids=self.collection_ids
        )

    @classmethod
    def random_sample(self, query, target_size, quality_coefficient=0.1):
        """Take a random sample of high-quality items from a query.

        :param: A database query, assumed to cover every relevant
        item, with the higher-quality items grouped at the front and
        with items ordered randomly within each quality tier.

        :param quality_coefficient: What fraction of the query
        represents 'high-quality' works. Empirical measurement
        indicates that the top tier of titles -- very high quality
        items that are currently available -- represents about ten
        percent of a library's holdings at any given time.
        """
        if not query:
            return []
        if isinstance(query, list):
            # This is probably a unit test.
            total_size = len(query)
        else:
            total_size = fast_query_count(query)

        # Determine the highest offset we could choose and still
        # choose `target_size` items that fit entirely in the portion
        # of the query delimited by the quality coefficient.
        if quality_coefficient < 0:
            quality_coefficient = 0.1
        if quality_coefficient > 1:
            quality_coefficient = 1
        max_offset = int((total_size * quality_coefficient)-target_size)

        if max_offset > 0:
            # There are enough high-quality items that we can pick a
            # random entry point into the list, increasing the variety
            # of featured works shown to patrons.
            offset = random.randint(0, max_offset)
        else:
            offset = 0
        items = query.offset(offset).limit(target_size).all()
        random.shuffle(items)
        return items

    @classmethod
    def _lazy_load(cls, qu):
        """Avoid eager loading of objects that are contained in the
        materialized view.
        """
        from model import MaterializedWorkWithGenre as work_model
        return qu.options(
            lazyload(work_model.license_pool, LicensePool.data_source),
            lazyload(work_model.license_pool, LicensePool.identifier),
            lazyload(work_model.license_pool, LicensePool.presentation_edition),
        )

    @classmethod
    def _defer_unused_fields(cls, query):
        """Some applications use the simple OPDS entry and some
        applications use the verbose. Whichever one we don't need,
        we can stop from even being sent over from the
        database.
        """
        from model import MaterializedWorkWithGenre as work_model
        if Configuration.DEFAULT_OPDS_FORMAT == "simple_opds_entry":
            return query.options(defer(work_model.verbose_opds_entry))
        else:
            return query.options(defer(work_model.simple_opds_entry))

    @property
    def search_target(self):
        """By default, a WorkList is searchable."""
        return self

    def search(self, _db, query, search_client, media=None, pagination=None, languages=None, facets=None):
        """Find works in this WorkList that match a search query.

        :param facets: A faceting object, probably a SearchFacets.
        """
        if not pagination:
            pagination = Pagination(
                offset=0, size=Pagination.DEFAULT_SEARCH_SIZE
            )

        # Get the search results from Elasticsearch.
        results = None

        if not media:
            media = self.media
        elif media is Edition.ALL_MEDIUM:
            media = None
        if isinstance(media, basestring):
            media = [media]

        default_languages = languages
        if self.languages:
            default_languages = self.languages

        if self.target_age:
            target_age = numericrange_to_tuple(self.target_age)
        else:
            target_age = None

        if search_client:
            docs = None
            a = time.time()

            # These arguments to query_works might be modified by
            # the facets in play.
            kwargs = dict(
                media=media,
                languages=default_languages,
                fiction=self.fiction,
                audiences=self.audiences,
                target_age=target_age,
                in_any_of_these_genres=self.genre_ids,
                on_any_of_these_lists=self.customlist_ids,
            )
            if facets and facets.entrypoint:
                kwargs = facets.entrypoint.modified_search_arguments(**kwargs)

            # These arguments to query_works cannot be modified by
            # the facets in play.
            kwargs.update(
                dict(
                    library=self.get_library(_db),
                    query_string=query,
                    fields=["_id", "title", "author", "license_pool_id"],
                    size=pagination.size,
                    offset=pagination.offset,
                )
            )

            try:
                docs = search_client.query_works(**kwargs)
            except elasticsearch.exceptions.ConnectionError, e:
                logging.error(
                    "Could not connect to ElasticSearch. Returning empty list of search results."
                )
            b = time.time()
            logging.debug("Elasticsearch query completed in %.2fsec", b-a)
            results = []
            if docs:
                doc_ids = [
                    int(x['_id']) for x in docs['hits']['hits']
                ]
                if doc_ids:
                    results = self.works_for_specific_ids(_db, doc_ids)

        return results

    def _groups_for_lanes(self, _db, relevant_lanes, queryable_lanes, facets=None):
        library = self.get_library(_db)
        target_size = library.featured_lane_size

        if isinstance(self, Lane):
            parent_lane = self
        else:
            parent_lane = None

        queryable_lane_set = set(queryable_lanes)
        work_quality_tier_lane = list(
            self._featured_works_with_lanes(_db, queryable_lanes, facets=facets)
        )

        def _done_with_lane(lane):
            """Called when we're done with a Lane, either because
            the lane changes or we've reached the end of the list.
            """
            # Did we get enough items?
            num_missing = target_size-len(by_lane[lane])
            if num_missing > 0 and might_need_to_reuse:
                # No, we need to use some works we used in a
                # previous lane to fill out this lane. Stick
                # them at the end.
                by_lane[lane].extend(
                    might_need_to_reuse.values()[:num_missing]
                )

        used_works = set()
        by_lane = defaultdict(list)
        working_lane = None
        might_need_to_reuse = dict()
        for mw, quality_tier, lane in work_quality_tier_lane:
            if lane != working_lane:
                # Either we're done with the old lane, or we're just
                # starting and there was no old lane.
                if working_lane:
                    _done_with_lane(working_lane)
                working_lane = lane
                used_works_this_lane = set()
                might_need_to_reuse = dict()
            if len(by_lane[lane]) >= target_size:
                # We've already filled this lane.
                continue

            if mw.works_id in used_works:
                if mw.works_id not in used_works_this_lane:
                    # We already used this work in another lane, but we
                    # might need to use it again to fill out this lane.
                    might_need_to_reuse[mw.works_id] = mw
            else:
                by_lane[lane].append(mw)
                used_works.add(mw.works_id)
                used_works_this_lane.add(mw.works_id)

        # Close out the last lane encountered.
        _done_with_lane(working_lane)
        for lane in relevant_lanes:
            if lane in queryable_lane_set:
                # We found results for this lane through the main query.
                # Yield those results.
                for mw in by_lane.get(lane, []):
                    yield (mw, lane)
            else:
                # We didn't try to use the main query to find results
                # for this lane because we knew the results, if there
                # were any, wouldn't be representative. This is most
                # likely because this 'lane' is a WorkList and not a
                # Lane at all. Do a whole separate query and plug it
                # in at this point.
                for x in lane.groups(
                    _db, include_sublanes=False, facets=facets
                ):
                    yield x

    def _featured_works_with_lanes(self, _db, lanes, facets):
        """Find a sequence of works that can be used to
        populate this lane's grouped acquisition feed.

        :param lanes: Classify MaterializedWorkWithGenre objects
        as belonging to one of these lanes (presumably sublanes
        of `self`).

        :param facets: A faceting object, presumably a FeaturedFacets

        :yield: A sequence of (MaterializedWorkWithGenre,
        quality_tier, Lane) 3-tuples.
        """
        if not lanes:
            # We can't run this query at all.
            return

        library = self.get_library(_db)
        target_size = library.featured_lane_size

        facets = facets or self.default_featured_facets(_db)

        # Pull a window of works for every lane we were given.
        for lane in lanes:
            for mw, quality_tier in lane.works_in_window(
                    _db, facets, target_size
            ):
                yield mw, quality_tier, lane

    def works_in_window(self, _db, facets, target_size):
        """Find all MaterializedWorkWithGenre objects within a randomly
        selected window of values for the `random` field.

        :param facets: A `FeaturedFacets` object.

        :param target_size: Try to get approximately this many
        items. There may be more or less; this controls the size of
        the window and the LIMIT on the query.
        """
        from model import MaterializedWorkWithGenre
        work_model = MaterializedWorkWithGenre

        lane_query = self.works(_db, facets=facets)

        # Make sure this query finds a number of works proportinal
        # to the expected size of the lane.
        lane_query = self._restrict_query_to_window(lane_query, target_size)

        lane_query = lane_query.order_by(
            "quality_tier desc", work_model.random.desc()
        )

        # Allow some overage to reduce the risk that we'll have to
        # use a given book more than once in the overall feed. But
        # set an upper limit so that a weird random distribution
        # doesn't retrieve far more items than we need.
        lane_query = lane_query.limit(target_size*1.3)
        return lane_query

    def _restrict_query_to_window(self, query, target_size):
        """Restrict the given SQLAlchemy query so that it matches
        approximately `target_size` items.
        """
        from model import MaterializedWorkWithGenre as work_model
        if query is None:
            return query
        window_start, window_end = self.featured_window(target_size)
        if window_start > 0 and window_start < 1:
            query = query.filter(
                work_model.random <= window_end,
                work_model.random >= window_start
            )
        return query

    def _fill_parent_lane(self, additional_needed, unused_by_tier,
                          used_by_tier, previously_used):
        """Yield up to `additional_needed` randomly selected items from
        `unused_by_tier`, falling back to `used_by_tier` if necessary.

        NOTE: This method is currently unused.

        :param unused_by_tier: A dictionary mapping quality tiers to
        lists of unused MaterializedWorkWithGenre items. Because the
        same book may have shown up as multiple
        MaterializedWorkWithGenre items, it may show up as 'unused'
        here even if another occurance of it has been used.

        :param used_by_tier: A dictionary mapping quality tiers to lists
        of previously used MaterializedWorkWithGenre items. These will only
        be chosen once every item in unused_by_tier has been chosen.

        :param previously_used: A set of work IDs corresponding to
        previously selected MaterializedWorkWithGenre items. A work in
        `unused_by_tier` will be treated as actually having been used
        if its ID is in this set.

        """
        if not additional_needed:
            return
        additional_found = 0
        for by_tier in unused_by_tier, used_by_tier:
            # Go through each tier in decreasing quality order.
            for tier in sorted(by_tier.keys(), key=lambda x: -x):
                mws = by_tier[tier]
                random.shuffle(mws)
                for mw in mws:
                    if (by_tier is unused_by_tier
                        and mw.works_id in previously_used):
                        # We initially thought this work was unused,
                        # and put it in the 'unused' bucket, but then
                        # the work was used after that happened.
                        # Treat it as used and don't use it again.
                        continue
                    yield (mw, self)
                    previously_used.add(mw.works_id)
                    additional_found += 1
                    if additional_found >= additional_needed:
                        # We're all done.
                        return


class LaneGenre(Base):
    """Relationship object between Lane and Genre."""
    __tablename__ = 'lanes_genres'
    id = Column(Integer, primary_key=True)
    lane_id = Column(Integer, ForeignKey('lanes.id'), index=True,
                     nullable=False)
    genre_id = Column(Integer, ForeignKey('genres.id'), index=True,
                      nullable=False)

    # An inclusive relationship means that books classified under the
    # genre are included in the lane. An exclusive relationship means
    # that books classified under the genre are excluded, even if they
    # would otherwise be included.
    inclusive = Column(Boolean, default=True, nullable=False)

    # By default, this relationship applies not only to the genre
    # itself but to all of its subgenres. Setting recursive=false
    # means that only the genre itself is affected.
    recursive = Column(Boolean, default=True, nullable=False)

    __table_args__ = (
        UniqueConstraint('lane_id', 'genre_id'),
    )

    @classmethod
    def from_genre(cls, genre):
        """Used in the Lane.genres association proxy."""
        lg = LaneGenre()
        lg.genre = genre
        return lg

Genre.lane_genres = relationship(
    "LaneGenre", foreign_keys=LaneGenre.genre_id, backref="genre"
)


class Lane(Base, WorkList):
    """A WorkList that draws its search criteria from a row in a
    database table.

    A Lane corresponds roughly to a section in a branch library or
    bookstore. Lanes are the primary means by which patrons discover
    books.
    """

    # Unless a sitewide setting intervenes, the set of Works in a
    # Lane is cacheable for twenty minutes by default.
    MAX_CACHE_AGE = 20*60

    __tablename__ = 'lanes'
    id = Column(Integer, primary_key=True)
    library_id = Column(Integer, ForeignKey('libraries.id'), index=True,
                        nullable=False)
    parent_id = Column(Integer, ForeignKey('lanes.id'), index=True,
                       nullable=True)
    priority = Column(Integer, index=True, nullable=False, default=0)

    # How many titles are in this lane? This is periodically
    # calculated and cached.
    size = Column(Integer, nullable=False, default=0)

    # A lane may have one parent lane and many sublanes.
    sublanes = relationship(
        "Lane",
        backref=backref("parent", remote_side = [id]),
    )

    # A lane may have multiple associated LaneGenres. For most lanes,
    # this is how the contents of the lanes are defined.
    genres = association_proxy('lane_genres', 'genre',
                               creator=LaneGenre.from_genre)
    lane_genres = relationship(
        "LaneGenre", foreign_keys="LaneGenre.lane_id", backref="lane",
        cascade='all, delete-orphan'
    )

    # display_name is the name of the lane as shown to patrons.  It's
    # okay for this to be duplicated within a library, but it's not
    # okay to have two lanes with the same parent and the same display
    # name -- that would be confusing.
    display_name = Column(Unicode)

    # True = Fiction only
    # False = Nonfiction only
    # null = Both fiction and nonfiction
    #
    # This may interact with lane_genres, for genres such as Humor
    # which can apply to either fiction or nonfiction.
    fiction = Column(Boolean, index=True, nullable=True)

    # A lane may be restricted to works classified for specific audiences
    # (e.g. only Young Adult works).
    _audiences = Column(ARRAY(Unicode), name='audiences')

    # A lane may further be restricted to works classified as suitable
    # for a specific age range.
    _target_age = Column(INT4RANGE, name="target_age", index=True)

    # A lane may be restricted to works available in certain languages.
    languages = Column(ARRAY(Unicode))

    # A lane may be restricted to works in certain media (e.g. only
    # audiobooks).
    media = Column(ARRAY(Unicode))

    # TODO: At some point it may be possible to restrict a lane to certain
    # formats (e.g. only electronic materials or only codices).

    # Only books licensed through this DataSource will be shown.
    license_datasource_id = Column(
        Integer, ForeignKey('datasources.id'), index=True,
        nullable=True
    )

    # Only books on one or more CustomLists obtained from this
    # DataSource will be shown.
    _list_datasource_id = Column(
        Integer, ForeignKey('datasources.id'), index=True,
        nullable=True
    )

    # Only the books on these specific CustomLists will be shown.
    customlists = relationship(
        "CustomList", secondary=lambda: lanes_customlists,
        backref="lane"
    )

    # This has no effect unless list_datasource_id or
    # list_identifier_id is also set. If this is set, then a book will
    # only be shown if it has a CustomListEntry on an appropriate list
    # where `most_recent_appearance` is within this number of days. If
    # the number is zero, then the lane contains _every_ book with a
    # CustomListEntry associated with an appropriate list.
    list_seen_in_previous_days = Column(Integer, nullable=True)

    # If this is set to True, then a book will show up in a lane only
    # if it would _also_ show up in its parent lane.
    inherit_parent_restrictions = Column(Boolean, default=True, nullable=False)

    # Patrons whose external type is in this list will be sent to this
    # lane when they ask for the root lane.
    #
    # This is almost never necessary.
    root_for_patron_type = Column(ARRAY(Unicode), nullable=True)

    # A grouped feed for a Lane contains a swim lane from each
    # sublane, plus a swim lane at the bottom for the Lane itself. In
    # some cases that final swim lane should not be shown. This
    # generally happens because a) the sublanes are so varied that no
    # one would want to see a big list containing everything, and b)
    # the sublanes are exhaustive of the Lane's content, so there's
    # nothing new to be seen by going into that big list.
    include_self_in_grouped_feed = Column(
        Boolean, default=True, nullable=False
    )

    # Only a visible lane will show up in the user interface.  The
    # admin interface can see all the lanes, visible or not.
    _visible = Column(Boolean, default=True, nullable=False, name="visible")

    # A Lane may have many CachedFeeds.
    cachedfeeds = relationship(
        "CachedFeed", backref="lane",
        cascade="all, delete-orphan",
    )


    __table_args__ = (
        UniqueConstraint('parent_id', 'display_name'),
    )

    def get_library(self, _db):
        """For compatibility with WorkList.get_library()."""
        return self.library

    @property
    def collection_ids(self):
        return [x.id for x in self.library.collections]

    @property
    def children(self):
        return self.sublanes

    @property
    def visible_children(self):
        children = [lane for lane in self.sublanes if lane.visible]
        return sorted(children, key=lambda x: (x.priority, x.display_name))

    @property
    def parentage(self):
        """Yield the parent, grandparent, etc. of this Lane.

        The Lane may be inside one or more non-Lane WorkLists, but those
        WorkLists are not counted in the parentage.
        """
        if not self.parent:
            return
        yield self.parent
        seen = set([self, self.parent])
        for parent in self.parent.parentage:
            if parent in seen:
                raise ValueError("Lane parentage loop detected")
            seen.add(parent)
            yield parent

    @property
    def depth(self):
        """How deep is this lane in this site's hierarchy?
        i.e. how many times do we have to follow .parent before we get None?
        """
        return len(list(self.parentage))

    @property
    def entrypoints(self):
        """Lanes cannot currently have EntryPoints."""
        return []

    @hybrid_property
    def visible(self):
        return self._visible and (not self.parent or self.parent.visible)

    @visible.setter
    def visible(self, value):
        self._visible = value

    @property
    def url_name(self):
        """Return the name of this lane to be used in URLs.

        Since most aspects of the lane can change through administrative
        action, we use the internal database ID of the lane in URLs.
        """
        return self.id

    @hybrid_property
    def audiences(self):
        return self._audiences or []

    @audiences.setter
    def audiences(self, value):
        """The `audiences` field cannot be set to a value that
        contradicts the current value to the `target_age` field.
        """
        if self._audiences and self._target_age and value != self._audiences:
            raise ValueError("Cannot modify Lane.audiences when Lane.target_age is set!")
        if isinstance(value, basestring):
            value = [value]
        self._audiences = value

    @hybrid_property
    def target_age(self):
        return self._target_age

    @target_age.setter
    def target_age(self, value):
        """Setting .target_age will lock .audiences to appropriate values.

        If you set target_age to 16-18, you're saying that the audiences
        are [Young Adult, Adult].

        If you set target_age 12-15, you're saying that the audiences are
        [Young Adult, Children].

        If you set target age 0-2, you're saying that the audiences are
        [Children].

        In no case is the "Adults Only" audience allowed, since target
        age only makes sense in lanes intended for minors.
        """
        if value is None:
            self._target_age = None
            return
        audiences = []
        if isinstance(value, int):
            value = (value, value)
        if isinstance(value, tuple):
            value = tuple_to_numericrange(value)
        if value.lower >= Classifier.ADULT_AGE_CUTOFF:
            # Adults are adults and there's no point in tracking
            # precise age gradations for them.
            value = tuple_to_numericrange(
                (Classifier.ADULT_AGE_CUTOFF, value.upper)
            )
        if value.upper >= Classifier.ADULT_AGE_CUTOFF:
            value = tuple_to_numericrange(
                (value.lower, Classifier.ADULT_AGE_CUTOFF)
            )
        self._target_age = value

        if value.upper >= Classifier.ADULT_AGE_CUTOFF:
            audiences.append(Classifier.AUDIENCE_ADULT)
        if value.lower < Classifier.YOUNG_ADULT_AGE_CUTOFF:
            audiences.append(Classifier.AUDIENCE_CHILDREN)
        if value.upper >= Classifier.YOUNG_ADULT_AGE_CUTOFF:
            audiences.append(Classifier.AUDIENCE_YOUNG_ADULT)
        self._audiences = audiences

    @hybrid_property
    def list_datasource(self):
        return self._list_datasource

    @list_datasource.setter
    def list_datasource(self, value):
        """Setting .list_datasource to a non-null value wipes out any specific
        CustomLists previously associated with this Lane.
        """
        if value:
            self.customlists = []
            value = value.id
        self._list_datasource_id = value

    @property
    def uses_customlists(self):
        """Does the works() implementation for this Lane look for works on
        CustomLists?
        """
        if self.customlists or self.list_datasource:
            return True
        if (self.parent and self.inherit_parent_restrictions
            and self.parent.uses_customlists):
            return True
        return False

    def update_size(self, _db):
        """Update the stored estimate of the number of Works in this Lane."""
        query = self.works(_db).limit(None)
        from model import MaterializedWorkWithGenre as mw
        query = query.distinct(mw.works_id)
        self.size = fast_query_count(query)

    @property
    def genre_ids(self):
        """Find the database ID of every Genre such that a Work classified in
        that Genre should be in this Lane.

        :return: A list of genre IDs, or None if this Lane does not
        consider genres at all.
        """
        if not hasattr(self, '_genre_ids'):
            self._genre_ids = self._gather_genre_ids()
        return self._genre_ids

    def _gather_genre_ids(self):
        """Method that does the work of `genre_ids`."""
        if not self.lane_genres:
            return None

        included_ids = set()
        excluded_ids = set()
        for lanegenre in self.lane_genres:
            genre = lanegenre.genre
            if lanegenre.inclusive:
                bucket = included_ids
            else:
                bucket = excluded_ids
            if self.fiction != None and genre.default_fiction != None and self.fiction != genre.default_fiction:
                logging.error("Lane %s has a genre %s that does not match its fiction restriction.", (self.full_identifier, genre.name))
            bucket.add(genre.id)
            if lanegenre.recursive:
                for subgenre in genre.subgenres:
                    bucket.add(subgenre.id)
        if not included_ids:
            # No genres have been explicitly included, so this lane
            # includes all genres that aren't excluded.
            _db = Session.object_session(self)
            included_ids = set([genre.id for genre in _db.query(Genre)])
        genre_ids = included_ids - excluded_ids
        if not genre_ids:
            # This can happen if you create a lane where 'Epic
            # Fantasy' is included but 'Fantasy' and its subgenres are
            # excluded.
            logging.error(
                "Lane %s has a self-negating set of genre IDs.",
                self.full_identifier
            )
        return genre_ids

    @property
    def customlist_ids(self):
        """Find the database ID of every CustomList such that a Work filed
        in that List should be in this Lane.

        :return: A list of CustomList IDs, possibly empty.
        """
        if not hasattr(self, '_customlist_ids'):
            self._customlist_ids = self._gather_customlist_ids()
        return self._customlist_ids

    def _gather_customlist_ids(self):
        """Method that does the work of `customlist_ids`."""
        if self.list_datasource:
            # Find the ID of every CustomList from a certain
            # DataSource.
            _db = Session.object_session(self)
            query = select(
                [CustomList.id],
                CustomList.data_source_id==self.list_datasource.id
            )
            ids = [x[0] for x in _db.execute(query)]
        else:
            # Find the IDs of some specific CustomLists.
            ids = [x.id for x in self.customlists]
        if len(ids) == 0:
            if self.list_datasource:
                # We are restricted to all lists from a given data
                # source, and there are no such lists, so we want to
                # exclude everything.
                return []
            else:
                # There is no custom list restriction at all.
                return None
        return ids

    @classmethod
    def affected_by_customlist(self, customlist):
        """Find all Lanes whose membership is partially derived
        from the membership of the given CustomList.
        """
        _db = Session.object_session(customlist)

        # Either the data source must match, or there must be a specific link
        # between the Lane and the CustomList.
        data_source_matches = (
            Lane._list_datasource_id==customlist.data_source_id
        )
        specific_link = CustomList.id==customlist.id

        return _db.query(Lane).outerjoin(Lane.customlists).filter(
            or_(data_source_matches, specific_link)
        )

    def add_genre(self, genre, inclusive=True, recursive=True):
        """Create a new LaneGenre for the given genre and
        associate it with this Lane.

        Mainly used in tests.
        """
        _db = Session.object_session(self)
        if isinstance(genre, basestring):
            genre, ignore = Genre.lookup(_db, genre)
        lanegenre, is_new = get_one_or_create(
            _db, LaneGenre, lane=self, genre=genre
        )
        lanegenre.inclusive=inclusive
        lanegenre.recursive=recursive
        self._genre_ids = self._gather_genre_ids()
        return lanegenre, is_new

    @property
    def search_target(self):
        """Obtain the WorkList that should be searched when someone
        initiates a search from this Lane."""

        # See if this Lane is the root lane for a patron type, or has an
        # ancestor that's the root lane for a patron type. If so, search
        # that Lane.
        if self.root_for_patron_type:
            return self

        for parent in self.parentage:
            if parent.root_for_patron_type:
                return parent

        # Otherwise, we want to use the lane's languages, media, and
        # juvenile audiences in search.
        languages = self.languages
        media = self.media
        audiences = None
        if Classifier.AUDIENCE_YOUNG_ADULT in self.audiences or Classifier.AUDIENCE_CHILDREN in self.audiences:
            audiences = self.audiences

        # If there are too many languages or audiences, the description
        # could get too long to be useful, so we'll leave them out.
        # Media isn't part of the description yet.

        display_name_parts = []
        if languages and len(languages) <= 2:
            display_name_parts.append(LanguageCodes.name_for_languageset(languages))

        if audiences:
            if len(audiences) <= 2:
                display_name_parts.append(" and ".join(audiences))

        display_name = " ".join(display_name_parts)

        wl = WorkList()
        wl.initialize(self.library, display_name=display_name,
                      languages=languages, media=media, audiences=audiences)
        return wl

    def featured_window(self, target_size):
        """Randomly select an interval over `Work.random` that ought to
        contain approximately `target_size` high-quality works from
        this lane.

        :param: A 2-tuple (low value, high value), or None if the
        entire span should be considered.
        """
        if self.size < target_size:
            # Don't bother -- we're returning the whole lane.
            return 0,1
        width = target_size / (self.size * 0.2)
        width = min(1, width)

        maximum_offset = 1-width
        start = random.random() * maximum_offset
        end = start+width

        # TODO: The resolution of Work.random is only three decimal
        # places. It should be increased. Until then, we need to make
        # sure start and end are at least 0.001 apart, or in a very
        # large lane we'll pick up nothing.
        start = round(start, 3)
        end = round(end, 3)
        if start == end:
            end = start + 0.001
        return start, end

    def groups(self, _db, include_sublanes=True, facets=None):
        """Return a list of (MaterializedWorkWithGenre, Lane) 2-tuples
        describing a sequence of featured items for this lane and
        (optionally) its children.

        :param facets: A FeaturedFacets object.
        """
        clauses = []
        library = self.get_library(_db)
        target_size = library.featured_lane_size

        if self.include_self_in_grouped_feed:
            relevant_lanes = [self]
        else:
            relevant_lanes = []
        if include_sublanes:
            # The child lanes go first.
            relevant_lanes = list(self.visible_children) + relevant_lanes

        # We can use a single query to build the featured feeds for
        # this lane, as well as any of its sublanes that inherit this
        # lane's restrictions. Lanes that don't inherit this lane's
        # restrictions will need to be handled in a separate call to
        # groups().
        queryable_lanes = [x for x in relevant_lanes
                           if x == self or x.inherit_parent_restrictions]
        return self._groups_for_lanes(
            _db, relevant_lanes, queryable_lanes, facets=facets
        )

    def search(self, _db, query, search_client, media=None, pagination=None, languages=None, facets=None):
        """Find works in this lane that also match a search query.

        :param facets: A SearchFacets object.
        """
        target = self.search_target

        if target == self:
            return super(Lane, self).search(_db, query, search_client, media, pagination, languages, facets=facets)
        else:
            return target.search(_db, query, search_client, media, pagination, languages, facets=facets)

    def bibliographic_filter_clause(self, _db, qu, featured, outer_join=False):
        """Create an AND clause that restricts a query to find
        only works classified in this lane.

        :param qu: A Query object. The filter will not be applied to this
        Query, but the query may be extended with additional table joins.

        :return: A 2-tuple (query, statement).

        `query` is the same query as `qu`, possibly extended with
        additional table joins.

        `statement` is a SQLAlchemy statement suitable for passing
        into filter() or case().
        """
        from model import MaterializedWorkWithGenre as work_model
        qu, superclass_clause = super(
            Lane, self
        ).bibliographic_filter_clause(
            _db, qu, featured
        )
        clauses = []
        if superclass_clause is not None:
            clauses.append(superclass_clause)
        if self.parent and self.inherit_parent_restrictions:
            # In addition to the other restrictions imposed by this
            # Lane, books will show up here only if they would
            # also show up in the parent Lane.
            qu, clause = self.parent.bibliographic_filter_clause(
                _db, qu, featured
            )
            if clause is not None:
                clauses.append(clause)

        # If a license source is specified, only show books from that
        # source.
        if self.license_datasource:
            clauses.append(LicensePool.data_source==self.license_datasource)

        if self.fiction is not None:
            clauses.append(work_model.fiction==self.fiction)

        if self.media:
            clauses.append(work_model.medium.in_(self.media))

        clauses.extend(self.age_range_filter_clauses())
        qu, customlist_clauses = self.customlist_filter_clauses(
            qu, featured, outer_join
        )
        clauses.extend(customlist_clauses)

        if clauses:
            clause = and_(*clauses)
        else:
            clause = None
        return qu, clause

    def age_range_filter_clauses(self):
        """Create a clause that filters out all books not classified as
        suitable for this Lane's age range.
        """
        from model import MaterializedWorkWithGenre as work_model
        if self.target_age == None:
            return []

        if (Classifier.AUDIENCE_ADULT in self.audiences
            or Classifier.AUDIENCE_ADULTS_ONLY in self.audiences):
            # Books for adults don't have target ages. If we're including
            # books for adults, allow the target age to be empty.
            audience_has_no_target_age = work_model.target_age == None
        else:
            audience_has_no_target_age = False

        # The lane's target age is an inclusive NumericRange --
        # set_target_age makes sure of that. The work's target age
        # must overlap that of the lane.
        return [
            or_(
                work_model.target_age.overlaps(self.target_age),
                audience_has_no_target_age
            )
        ]

    def customlist_filter_clauses(
            self, qu, must_be_featured=False, outer_join=False
    ):
        """Create a filter clause that only books that are on one of the
        CustomLists allowed by Lane configuration.

        :param must_be_featured: It's not enough for the book to be on
        an appropriate list; it must be _featured_ on an appropriate list.

        :return: A 3-tuple (query, clauses).

        `query` is the same query as `qu`, possibly extended with
        additional table joins.

        `clauses` is a list of SQLAlchemy statements for use in a
        filter() or case() statement.
        """
        from model import MaterializedWorkWithGenre as work_model
        if not self.customlists and not self.list_datasource:
            # This lane does not require that books be on any particular
            # CustomList.
            return qu, []

        already_filtered_customlist_on_materialized_view = getattr(
            qu, 'customlist_id_filtered', False
        )

        # We will be joining against CustomListEntry at least once, to
        # run filters on fields like `featured` not found in the
        # materialized view. For a lane derived from the intersection
        # of two or more custom lists, we may be joining
        # CustomListEntry multiple times. To avoid confusion, we make
        # a new alias for the table every time except the first time.
        if already_filtered_customlist_on_materialized_view:
            a_entry = aliased(CustomListEntry)
        else:
            a_entry = CustomListEntry

        clause = a_entry.work_id==work_model.works_id
        if not already_filtered_customlist_on_materialized_view:
            # Since this is the first join, we're treating
            # work_model.list_id as a stand-in for CustomListEntry.list_id,
            # which means we should force them to be the same when joining
            # the view to the table.
            #
            # For subsequent joins, this won't apply -- we want to
            # match a _different_ list's entry for the same work.
            clause = and_(clause, a_entry.list_id==work_model.list_id)
        if outer_join:
            qu = qu.outerjoin(a_entry, clause)
        else:
            qu = qu.join(a_entry, clause)

        # Actually build the restriction clauses.
        clauses = []
        customlist_ids = None
        if self.list_datasource:
            # Use a subquery to obtain the CustomList IDs of all
            # CustomLists from this DataSource. This is significantly
            # simpler than adding a join against CustomList.
            customlist_ids = Select(
                [CustomList.id],
                CustomList.data_source_id==self.list_datasource.id
            )
        else:
            customlist_ids = self.customlist_ids
        if customlist_ids is not None:
            clauses.append(a_entry.list_id.in_(customlist_ids))
            if not already_filtered_customlist_on_materialized_view:
                clauses.append(work_model.list_id.in_(customlist_ids))
                # Now that we've put a restriction on the materialized
                # view's list_id, we need to signal that no future
                # call to this method should put a restriction on the
                # same field.
                #
                # Future calls will apply their restrictions
                # solely by restricting CustomListEntry.list_id,
                # as above.
                qu.customlist_id_filtered = True
        if must_be_featured:
            clauses.append(a_entry.featured==True)
        if self.list_seen_in_previous_days:
            cutoff = datetime.datetime.utcnow() - datetime.timedelta(
                self.list_seen_in_previous_days
            )
            clauses.append(a_entry.most_recent_appearance >=cutoff)

        return qu, clauses

    def explain(self):
        """Create a series of human-readable strings to explain a lane's settings."""
        lines = []
        lines.append("ID: %s" % self.id)
        lines.append("Library: %s" % self.library.short_name)
        if self.parent:
            lines.append("Parent ID: %s (%s)" % (self.parent.id, self.parent.display_name))
        lines.append("Priority: %s" % self.priority)
        lines.append("Display name: %s" % self.display_name)
        return lines

Library.lanes = relationship("Lane", backref="library", foreign_keys=Lane.library_id, cascade='all, delete-orphan')
DataSource.list_lanes = relationship("Lane", backref="_list_datasource", foreign_keys=Lane._list_datasource_id)
DataSource.license_lanes = relationship("Lane", backref="license_datasource", foreign_keys=Lane.license_datasource_id)


lanes_customlists = Table(
    'lanes_customlists', Base.metadata,
    Column(
        'lane_id', Integer, ForeignKey('lanes.id'),
        index=True, nullable=False
    ),
    Column(
        'customlist_id', Integer, ForeignKey('customlists.id'),
        index=True, nullable=False
    ),
    UniqueConstraint('lane_id', 'customlist_id'),
)

@event.listens_for(Lane, 'after_insert')
@event.listens_for(Lane, 'after_delete')
@event.listens_for(LaneGenre, 'after_insert')
@event.listens_for(LaneGenre, 'after_delete')
def configuration_relevant_lifecycle_event(mapper, connection, target):
    site_configuration_has_changed(target)


@event.listens_for(Lane, 'after_update')
@event.listens_for(LaneGenre, 'after_update')
def configuration_relevant_update(mapper, connection, target):
    if directly_modified(target):
        site_configuration_has_changed(target)
