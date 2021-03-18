# encoding: utf-8
from collections import defaultdict

import datetime
import logging
import time
import urllib

from psycopg2.extras import NumericRange
from sqlalchemy.sql import select
from sqlalchemy.sql.expression import Select
from sqlalchemy.dialects.postgresql import JSON

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
    text,
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
    Collection,
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
from model.constants import EditionConstants
from facets import FacetConstants
from problem_details import *
from util import (
    fast_query_count,
    LanguageCodes,
)
from util.problem_detail import ProblemDetail
from util.accept_language import parse_accept_language
from util.opds_writer import OPDSFeed

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

class BaseFacets(FacetConstants):
    """Basic faceting class that doesn't modify a search filter at all.

    This is intended solely for use as a base class.
    """

    # If the use of a certain faceting object has implications for the
    # type of feed (the way FeaturedFacets always implies a 'groups' feed),
    # set the type of feed here. This will override any CACHED_FEED_TYPE
    # associated with the WorkList.
    CACHED_FEED_TYPE = None

    # By default, faceting objects have no opinion on how long the feeds
    # generated using them should be cached.
    max_cache_age = None

    def items(self):
        """Yields a 2-tuple for every active facet setting.

        These tuples are used to generate URLs that can identify
        specific facet settings, and to distinguish between CachedFeed
        objects that represent the same feed with different facet
        settings.
        """
        return []

    @property
    def cached(self):
        """This faceting object's opinion on whether feeds should be cached.

        :return: A boolean, or None for 'no opinion'.
        """
        if self.max_cache_age is None:
            return None
        return (self.max_cache_age != 0)

    @property
    def query_string(self):
        """A query string fragment that propagates all active facet
        settings.
        """
        return "&".join("=".join(x) for x in sorted(self.items()))

    @property
    def facet_groups(self):
        """Yield a list of 4-tuples
        (facet group, facet value, new Facets object, selected)
        for use in building OPDS facets.

        This does not include the 'entry point' facet group,
        which must be handled separately.
        """
        return []

    @classmethod
    def selectable_entrypoints(cls, worklist):
        """Ignore all entry points, even if the WorkList supports them."""
        return []

    def modify_search_filter(self, filter):
        """Modify an external_search.Filter object to filter out works
        excluded by the business logic of this faceting class.
        """
        return filter

    def modify_database_query(cls, _db, qu):
        """If necessary, modify a database query so that resulting
        items conform the constraints of this faceting object.

        The default behavior is to not modify the query.
        """
        return qu

    def scoring_functions(self, filter):
        """Create a list of ScoringFunction objects that modify how
        works in the given WorkList should be ordered.

        Most subclasses will not use this because they order
        works using the 'order' feature.
        """
        return []


class FacetsWithEntryPoint(BaseFacets):
    """Basic Facets class that knows how to filter a query based on a
    selected EntryPoint.
    """
    def __init__(self, entrypoint=None, entrypoint_is_default=False,
                 max_cache_age=None, **kwargs):
        """Constructor.

        :param entrypoint: An EntryPoint (optional).
        :param entrypoint_is_default: If this is True, then `entrypoint`
            is a default value and was not determined by a user's
            explicit choice.
        :param max_cache_age: Any feeds generated by this faceting object
            will be cached for this amount of time. The default is to have
            no opinion and let the Worklist manage this.
        :param kwargs: Other arguments may be supplied based on user
            input, but the default implementation is to ignore them.
        """
        self.entrypoint = entrypoint
        self.entrypoint_is_default = entrypoint_is_default
        self.max_cache_age = max_cache_age
        self.constructor_kwargs = kwargs

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

    def navigate(self, entrypoint):
        """Create a very similar FacetsWithEntryPoint that points to
        a different EntryPoint.
        """
        return self.__class__(
            entrypoint=entrypoint, entrypoint_is_default=False,
            max_cache_age=self.max_cache_age,
            **self.constructor_kwargs
        )

    @classmethod
    def from_request(
            cls, library, facet_config, get_argument, get_header, worklist,
            default_entrypoint=None, **extra_kwargs
    ):
        """Load a faceting object from an HTTP request.

        :param facet_config: A Library (or mock of one) that knows
           which subset of the available facets are configured.

        :param get_argument: A callable that takes one argument and
           retrieves (or pretends to retrieve) a query string
           parameter of that name from an incoming HTTP request.

        :param get_header: A callable that takes one argument and
           retrieves (or pretends to retrieve) an HTTP header
           of that name from an incoming HTTP request.

        :param worklist: A WorkList associated with the current request,
           if any.

        :param default_entrypoint: Select this EntryPoint if the
           incoming request does not specify an enabled EntryPoint.
           If this is None, the first enabled EntryPoint will be used
           as the default.

        :param extra_kwargs: A dictionary of keyword arguments to pass
           into the constructor when a faceting object is instantiated.

        :return: A FacetsWithEntryPoint, or a ProblemDetail if there's
            a problem with the input from the request.
        """
        return cls._from_request(
            facet_config, get_argument, get_header, worklist,
            default_entrypoint, **extra_kwargs
        )

    @classmethod
    def _from_request(
            cls, facet_config, get_argument, get_header, worklist,
            default_entrypoint=None, **extra_kwargs
    ):
        """Load a faceting object from an HTTP request.

        Subclasses of FacetsWithEntryPoint can override `from_request`,
        but call this method to load the EntryPoint and actually
        instantiate the faceting class.
        """
        entrypoint_name = get_argument(
            Facets.ENTRY_POINT_FACET_GROUP_NAME, None
        )
        valid_entrypoints = list(cls.selectable_entrypoints(facet_config))
        entrypoint = cls.load_entrypoint(
            entrypoint_name, valid_entrypoints, default=default_entrypoint
        )
        if isinstance(entrypoint, ProblemDetail):
            return entrypoint
        entrypoint, is_default = entrypoint

        max_cache_age = get_argument(
            Facets.MAX_CACHE_AGE_NAME, None
        )
        max_cache_age = cls.load_max_cache_age(max_cache_age)
        if isinstance(max_cache_age, ProblemDetail):
            return max_cache_age

        return cls(
            entrypoint=entrypoint, entrypoint_is_default=is_default,
            max_cache_age=max_cache_age, **extra_kwargs
        )

    @classmethod
    def load_entrypoint(cls, name, valid_entrypoints, default=None):
        """Look up an EntryPoint by name, assuming it's valid in the
        given WorkList.

        :param valid_entrypoints: The EntryPoints that might be
            valid. This is probably not the value of
            WorkList.selectable_entrypoints, because an EntryPoint
            selected in a WorkList remains valid (but not selectable) for
            all of its children.

        :param default: A class to use as the default EntryPoint if
            none is specified. If no default is specified, the first
            enabled EntryPoint will be used.

        :return: A 2-tuple (EntryPoint class, is_default).
        """
        if not valid_entrypoints:
            return None, True
        if default is None:
            default = valid_entrypoints[0]
        ep = EntryPoint.BY_INTERNAL_NAME.get(name)
        if not ep or ep not in valid_entrypoints:
            return default, True
        return ep, False

    @classmethod
    def load_max_cache_age(cls, value):
        """Convert a value for the MAX_CACHE_AGE_NAME parameter to a value
        that CachedFeed will understand.

        :param value: A string.
        :return: For now, either CachedFeed.IGNORE_CACHE or None.
        """
        if value is None:
            return value

        try:
            value = int(value)
        except ValueError, e:
            value = None

        # At the moment, the only acceptable value that can be set
        # through the web is zero -- i.e. don't use the cache at
        # all. We can't give web clients fine-grained control over
        # the internal workings of our cache; the most we can do
        # is give them the opportunity to opt out.
        #
        # Thus, any nonzero value will be ignored.
        if value == 0:
            value = CachedFeed.IGNORE_CACHE
        else:
            value = None
        return value

    def items(self):
        """Yields a 2-tuple for every active facet setting.

        In this class that just means the entrypoint and any max_cache_age.
        """
        if self.entrypoint:
            yield (self.ENTRY_POINT_FACET_GROUP_NAME,
                   self.entrypoint.INTERNAL_NAME)
        if self.max_cache_age not in (None, CachedFeed.CACHE_FOREVER):
            if self.max_cache_age == CachedFeed.IGNORE_CACHE:
                value = 0
            else:
                value = self.max_cache_age
            yield (self.MAX_CACHE_AGE_NAME, unicode(value))

    def modify_search_filter(self, filter):
        """Modify the given external_search.Filter object
        so that it reflects this set of facets.
        """
        if self.entrypoint:
            self.entrypoint.modify_search_filter(filter)
        return filter

    def modify_database_query(self, _db, qu):
        """Modify the given database query so that it reflects this set of
        facets.
        """
        if self.entrypoint:
            qu = self.entrypoint.modify_database_query(_db, qu)
        return qu


class Facets(FacetsWithEntryPoint):
    """A full-fledged facet class that supports complex navigation between
    multiple facet groups.

    Despite the generic name, this is only used in 'page' type OPDS
    feeds that list all the works in some WorkList.
    """

    ORDER_BY_RELEVANCE = "relevance"

    @classmethod
    def default(cls, library, collection=None, availability=None, order=None,
                entrypoint=None):
        return cls(library, collection=collection, availability=availability,
                   order=order, entrypoint=entrypoint)

    @classmethod
    def available_facets(cls, config, facet_group_name):
        """Which facets are enabled for the given facet group?

        You can override this to forcible enable or disable facets
        that might not be enabled in library configuration, but you
        can't make up totally new facets.

        TODO: This sytem would make more sense if you _could_ make up
        totally new facets, maybe because each facet was represented
        as a policy object rather than a key to code implemented
        elsewhere in this class. Right now this method implies more
        flexibility than actually exists.
        """
        available = config.enabled_facets(facet_group_name)

        # "The default facet isn't available" makes no sense. If the
        # default facet is not in the available list for any reason,
        # add it to the beginning of the list. This makes other code
        # elsewhere easier to write.
        default = cls.default_facet(config, facet_group_name)
        if default not in available:
            available = [default] + available
        return available

    @classmethod
    def default_facet(cls, config, facet_group_name):
        """The default value for the given facet group.

        The default value must be one of the values returned by available_facets() above.
        """
        return config.default_facet(facet_group_name)

    @classmethod
    def _values_from_request(cls, config, get_argument, get_header):
        g = Facets.ORDER_FACET_GROUP_NAME
        order = get_argument(g, cls.default_facet(config, g))
        order_facets = cls.available_facets(config, g)
        if order and not order in order_facets:
            return INVALID_INPUT.detailed(
                _("I don't know how to order a feed by '%(order)s'", order=order),
                400
            )

        g = Facets.AVAILABILITY_FACET_GROUP_NAME
        availability = get_argument(g, cls.default_facet(config, g))
        availability_facets = cls.available_facets(config, g)
        if availability and not availability in availability_facets:
            return INVALID_INPUT.detailed(
                _("I don't understand the availability term '%(availability)s'", availability=availability),
                400
            )

        g = Facets.COLLECTION_FACET_GROUP_NAME
        collection = get_argument(g, cls.default_facet(config, g))
        collection_facets = cls.available_facets(config, g)
        if collection and not collection in collection_facets:
            return INVALID_INPUT.detailed(
                _("I don't understand what '%(collection)s' refers to.", collection=collection),
                400
            )

        enabled = {
            Facets.ORDER_FACET_GROUP_NAME : order_facets,
            Facets.AVAILABILITY_FACET_GROUP_NAME : availability_facets,
            Facets.COLLECTION_FACET_GROUP_NAME : collection_facets,
        }

        return dict(
            order=order, availability=availability, collection=collection,
            enabled_facets=enabled
        )

    @classmethod
    def from_request(cls, library, config, get_argument, get_header, worklist,
                     default_entrypoint=None, **extra):
        """Load a faceting object from an HTTP request."""

        values = cls._values_from_request(config, get_argument, get_header)
        if isinstance(values, ProblemDetail):
            return values
        extra.update(values)
        extra['library'] = library

        return cls._from_request(config, get_argument, get_header, worklist,
                                 default_entrypoint, **extra)

    def __init__(self, library, collection, availability, order,
                 order_ascending=None, enabled_facets=None, entrypoint=None,
                 entrypoint_is_default=False, **constructor_kwargs):
        """Constructor.

        :param collection: This is not a Collection object; it's a value for
        the 'collection' facet, e.g. 'main' or 'featured'.

        :param entrypoint: An EntryPoint class. The 'entry point'
        facet group is configured on a per-WorkList basis rather than
        a per-library basis.
        """
        super(Facets, self).__init__(
            entrypoint, entrypoint_is_default, **constructor_kwargs
        )
        collection = collection or self.default_facet(
            library, self.COLLECTION_FACET_GROUP_NAME
        )
        availability = availability or self.default_facet(
            library, self.AVAILABILITY_FACET_GROUP_NAME
        )
        order = order or self.default_facet(library, self.ORDER_FACET_GROUP_NAME)
        if order_ascending is None:
            if order in Facets.ORDER_DESCENDING_BY_DEFAULT:
                order_ascending = self.ORDER_DESCENDING
            else:
                order_ascending = self.ORDER_ASCENDING

        if (availability == self.AVAILABLE_ALL and (library and not library.allow_holds)
            and (self.AVAILABLE_NOW in self.available_facets(library, self.AVAILABILITY_FACET_GROUP_NAME))):
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
        return self.__class__(
            library=self.library,
            collection=collection or self.collection,
            availability=availability or self.availability,
            order=order or self.order,
            enabled_facets=self.facets_enabled_at_init,
            entrypoint=(entrypoint or self.entrypoint),
            entrypoint_is_default=False,
            max_cache_age=self.max_cache_age
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
        representing facet values enabled via initialization or configuration

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
            library = self.library
            for group_name in (
                Facets.ORDER_FACET_GROUP_NAME,
                Facets.AVAILABILITY_FACET_GROUP_NAME,
                Facets.COLLECTION_FACET_GROUP_NAME
            ):
                yield self.available_facets(self.library, group_name)

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

    def modify_search_filter(self, filter):
        """Modify the given external_search.Filter object
        so that it reflects the settings of this Facets object.

        This is the Elasticsearch equivalent of apply(). However, the
        Elasticsearch implementation of (e.g.) the meaning of the
        different availabilty statuses is kept in Filter.build().
        """
        super(Facets, self).modify_search_filter(filter)

        if self.library:
            filter.minimum_featured_quality = self.library.minimum_featured_quality

        filter.availability = self.availability
        filter.subcollection = self.collection

        # No order and relevance order both signify the default and,
        # thus, either should leave `filter.order` unset.
        if self.order and self.order != self.ORDER_BY_RELEVANCE:
            order = self.SORT_ORDER_TO_ELASTICSEARCH_FIELD_NAME.get(self.order)
            if order:
                filter.order = order
                filter.order_ascending = self.order_ascending
            else:
                logging.error("Unrecognized sort order: %s", self.order)

    def modify_database_query(self, _db, qu):
        """Restrict a query against Work+LicensePool+Edition so that it
        matches only works that fit the criteria of this Faceting object.

        Sort order facet cannot be handled in this method, but can be
        handled in subclasses that override this method.
        """

        # Apply any superclass criteria
        qu = super(Facets, self).modify_database_query(_db, qu)

        available_now = or_(
            LicensePool.open_access == True,
            LicensePool.self_hosted == True,
            LicensePool.unlimited_access,
            LicensePool.licenses_available > 0
        )

        if self.availability == self.AVAILABLE_NOW:
            availability_clause = available_now
        elif self.availability == self.AVAILABLE_ALL:
            availability_clause = or_(
                LicensePool.open_access == True,
                LicensePool.self_hosted == True,
                LicensePool.licenses_owned > 0,
                LicensePool.unlimited_access
            )
        elif self.availability == self.AVAILABLE_OPEN_ACCESS:
            # TODO: self-hosted content could be allowed here
            # depending on what exactly the wording is.
            availability_clause = LicensePool.open_access == True
        elif self.availability == self.AVAILABLE_NOT_NOW:
            # The book must be licensed but currently unavailable.
            availability_clause = and_(
                not_(available_now),
                LicensePool.licenses_owned > 0
            )

        qu = qu.filter(availability_clause)

        if self.collection == self.COLLECTION_FULL:
            # Include everything.
            pass
        elif self.collection == self.COLLECTION_FEATURED:
            # Exclude books with a quality of less than the library's
            # minimum featured quality.
            qu = qu.filter(
                Work.quality >= self.library.minimum_featured_quality
            )

        return qu


class DefaultSortOrderFacets(Facets):
    """A faceting object that changes the default sort order.

    Subclasses must set DEFAULT_SORT_ORDER
    """

    @classmethod
    def available_facets(cls, config, facet_group_name):
        """Make sure the default sort order is the first item
        in the list of available sort orders.
        """
        if facet_group_name != cls.ORDER_FACET_GROUP_NAME:
            return super(DefaultSortOrderFacets, cls).available_facets(
                config, facet_group_name
            )
        default = config.enabled_facets(facet_group_name)

        # Promote the default sort order to the front of the list,
        # adding it if necessary.
        order = cls.DEFAULT_SORT_ORDER
        if order in default:
            default = filter(lambda x: x!=order, default)
        return [order] + default

    @classmethod
    def default_facet(cls, config, facet_group_name):
        if facet_group_name == cls.ORDER_FACET_GROUP_NAME:
            return cls.DEFAULT_SORT_ORDER
        return super(DefaultSortOrderFacets, cls).default_facet(
            config, facet_group_name
        )


class DatabaseBackedFacets(Facets):
    """A generic faceting object designed for managing queries against the
    database. (Other faceting objects are designed for managing
    Elasticsearch searches.)
    """

    # Of the sort orders in Facets, these are the only available ones
    # -- they map directly onto a field of one of the tables we're
    # querying.
    ORDER_FACET_TO_DATABASE_FIELD = {
        FacetConstants.ORDER_WORK_ID : Work.id,
        FacetConstants.ORDER_TITLE : Edition.sort_title,
        FacetConstants.ORDER_AUTHOR : Edition.sort_author,
        FacetConstants.ORDER_LAST_UPDATE : Work.last_update_time,
    }

    @classmethod
    def available_facets(cls, config, facet_group_name):
        """Exclude search orders not available through database queries."""
        standard = config.enabled_facets(facet_group_name)
        if facet_group_name != cls.ORDER_FACET_GROUP_NAME:
            return standard
        return [order for order in standard
                if order in cls.ORDER_FACET_TO_DATABASE_FIELD]

    @classmethod
    def default_facet(cls, config, facet_group_name):
        """Exclude search orders not available through database queries."""
        standard_default = super(DatabaseBackedFacets, cls).default_facet(
            config, facet_group_name
        )
        if facet_group_name != cls.ORDER_FACET_GROUP_NAME:
            return standard_default
        if standard_default in cls.ORDER_FACET_TO_DATABASE_FIELD:
            # This default sort order is supported.
            return standard_default

        # The default sort order is not supported. Just pick the first
        # enabled sort order.
        enabled = config.enabled_facets(facet_group_name)
        for i in enabled:
            if i in cls.ORDER_FACET_TO_DATABASE_FIELD:
                return i

        # None of the enabled sort orders are usable. Order by work ID.
        return cls.ORDER_WORK_ID

    def order_by(self):
        """Given these Facets, create a complete ORDER BY clause for queries
        against WorkModelWithGenre.
        """
        default_sort_order = [
            Edition.sort_author, Edition.sort_title, Work.id
        ]

        primary_order_by = self.ORDER_FACET_TO_DATABASE_FIELD.get(self.order)
        if primary_order_by is not None:
            # Promote the field designated by the sort facet to the top of
            # the order-by list.
            order_by = [primary_order_by]

            for i in default_sort_order:
                if i not in order_by:
                    order_by.append(i)
        else:
            # Use the default sort order
            order_by = default_sort_order

        # order_ascending applies only to the first field in the sort order.
        # Everything else is ordered ascending.
        if self.order_ascending:
            order_by_sorted = [x.asc() for x in order_by]
        else:
            order_by_sorted = [order_by[0].desc()] + [x.asc() for x in order_by[1:]]
        return order_by_sorted, order_by

    def modify_database_query(self, _db, qu):
        """Restrict a query so that it matches only works
        that fit the criteria of this faceting object. Ensure
        query is appropriately ordered and made distinct.
        """

        # Filter by facet criteria
        qu = super(DatabaseBackedFacets, self).modify_database_query(_db, qu)

        # Set the ORDER BY clause.
        order_by, order_distinct = self.order_by()
        qu = qu.order_by(*order_by)
        qu = qu.distinct(*order_distinct)
        return qu


class FeaturedFacets(FacetsWithEntryPoint):

    """A simple faceting object that configures a query so that the 'most
    featurable' items are at the front.

    This is mainly a convenient thing to pass into
    AcquisitionFeed.groups().
    """

    # This Facets class is used exclusively for grouped feeds.
    CACHED_FEED_TYPE = CachedFeed.GROUPS_TYPE

    def __init__(self, minimum_featured_quality, entrypoint=None,
                 random_seed=None, **kwargs):
        """Set up an object that finds featured books in a given
        WorkList.

        :param kwargs: Other arguments may be supplied based on user
            input, but the default implementation is to ignore them.
        """
        super(FeaturedFacets, self).__init__(entrypoint=entrypoint, **kwargs)
        self.minimum_featured_quality = minimum_featured_quality
        self.random_seed=random_seed

    @classmethod
    def default(cls, lane, **kwargs):
        library = None
        if lane:
            if isinstance(lane, Library):
                library = lane
            else:
                library = lane.library

        if library:
            quality = library.minimum_featured_quality
        else:
            quality = Configuration.DEFAULT_MINIMUM_FEATURED_QUALITY
        return cls(quality, **kwargs)

    def navigate(self, minimum_featured_quality=None, entrypoint=None):
        """Create a slightly different FeaturedFacets object based on this
        one.
        """
        minimum_featured_quality = minimum_featured_quality or self.minimum_featured_quality
        entrypoint = entrypoint or self.entrypoint
        return self.__class__(minimum_featured_quality, entrypoint, max_cache_age=self.max_cache_age)

    def modify_search_filter(self, filter):
        super(FeaturedFacets, self).modify_search_filter(filter)
        filter.minimum_featured_quality = self.minimum_featured_quality

    def scoring_functions(self, filter):
        """Generate scoring functions that weight works randomly, but
        with 'more featurable' works tending to be at the top.
        """
        return filter.featurability_scoring_functions(self.random_seed)


class SearchFacets(Facets):
    """A Facets object designed to filter search results.

    Most search result filtering is handled by WorkList, but this
    allows someone to, e.g., search a multi-lingual WorkList in their
    preferred language.
    """

    # If search results are to be ordered by some field other than
    # score, we need a cutoff point so that marginal matches don't get
    # top billing just because they're first alphabetically. This is
    # the default cutoff point, determined empirically.
    DEFAULT_MIN_SCORE = 500

    def __init__(self, **kwargs):
        languages = kwargs.pop('languages', None)
        media = kwargs.pop('media', None)

        # Our default_facets implementation will fill in values for
        # the facet groups defined by the Facets class. This
        # eliminates the need to explicitly specify a library, since
        # the library is mainly used to determine these defaults --
        # SearchFacets itself doesn't need one. However, in real
        # usage, a Library will be provided via
        # SearchFacets.from_request.
        kwargs.setdefault('library', None)
        kwargs.setdefault('collection', None)
        kwargs.setdefault('availability', None)
        order = kwargs.setdefault('order', None)

        if order in (None, self.ORDER_BY_RELEVANCE):
            # Search results are ordered by score, so there is no
            # need for a score cutoff.
            default_min_score = None
        else:
            default_min_score = self.DEFAULT_MIN_SCORE
        self.min_score = kwargs.pop('min_score', default_min_score)

        super(SearchFacets, self).__init__(**kwargs)
        if media == Edition.ALL_MEDIUM:
            self.media = media
        else:
            self.media = self._ensure_list(media)
        self.media_argument = media

        self.languages = self._ensure_list(languages)

    @classmethod
    def default_facet(cls, ignore, group_name):
        """The default facet settings for SearchFacets are hard-coded.

        By default, we will search the full collection and all
        availabilities, and order by match quality rather than any
        bibliographic field.
        """
        if group_name == cls.COLLECTION_FACET_GROUP_NAME:
            return cls.COLLECTION_FULL

        if group_name == cls.AVAILABILITY_FACET_GROUP_NAME:
            return cls.AVAILABLE_ALL

        if group_name == cls.ORDER_FACET_GROUP_NAME:
            return cls.ORDER_BY_RELEVANCE
        return None

    def _ensure_list(self, x):
        """Make sure x is a list of values, if there is a value at all."""
        if x is None:
            return None
        if isinstance(x, list):
            return x
        return [x]

    @classmethod
    def from_request(cls, library, config, get_argument, get_header, worklist,
                     default_entrypoint=EverythingEntryPoint, **extra):

        values = cls._values_from_request(config, get_argument, get_header)
        if isinstance(values, ProblemDetail):
            return values
        extra.update(values)
        extra['library'] = library
        # Searches against a WorkList will use the union of the
        # languages allowed by the WorkList and the languages found in
        # the client's Accept-Language header.
        language_header = get_header("Accept-Language")
        languages = get_argument("language") or None
        if not languages:
            if language_header:
                languages = parse_accept_language(language_header)
                languages = [l[0] for l in languages]
                languages = map(LanguageCodes.iso_639_2_for_locale, languages)
                languages = [l for l in languages if l]
            languages = languages or None

        # The client can request a minimum score for search results.
        min_score = get_argument("min_score", None)
        if min_score is not None:
            try:
                min_score = int(min_score)
            except ValueError, e:
                min_score = None
        if min_score is not None:
            extra['min_score'] = min_score

        # The client can request an additional restriction on
        # the media types to be returned by searches.

        media = get_argument("media", None)
        if media not in EditionConstants.KNOWN_MEDIA:
            media = None
        extra['media'] = media
        languageQuery = get_argument("language", None)
        # Currently, the only value passed to the language query from the client is
        # `all`. This will remove the default browser's Accept-Language header value
        # in the search request.
        if languageQuery != "all" :
            extra['languages'] = languages

        return cls._from_request(
            config, get_argument, get_header, worklist, default_entrypoint,
            **extra
        )

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

    def modify_search_filter(self, filter):
        """Modify the given external_search.Filter object
        so that it reflects this SearchFacets object.
        """
        super(SearchFacets, self).modify_search_filter(filter)

        if filter.order is not None and filter.min_score is None:
            # The user wants search results to be ordered by one of
            # the data fields, not the match score; and no overriding
            # score cutoff has been provided yet. Use ours.
            filter.min_score = self.min_score

        # The incoming 'media' argument takes precedence over any
        # media restriction defined by the WorkList or the EntryPoint.
        if self.media == Edition.ALL_MEDIUM:
            # Clear any preexisting media restrictions.
            filter.media = None
        elif self.media:
            filter.media = self.media

        # The languages matched by the filter are the union of the
        # languages allowed by the WorkList (which were set to
        # filter.languages upon instantiation) and the languages
        # mentioned in the the user's Accept-Language header (which
        # were stuck into the SearchFacets object when _it_ was
        # instantiated).
        #
        # We don't rely solely on the WorkList languages because at
        # the moment it's hard for people who don't read the dominant
        # language of the circulation manager to find the right place
        # to search.
        #
        # We don't rely solely on the SearchFacets languages because a
        # lot of people read in languages other than the one they've
        # set for their device UI.
        all_languages = set()
        for language_list in (self.languages, filter.languages):
            for language in self._ensure_list(language_list) or []:
                all_languages.add(language)
        filter.languages = sorted(all_languages) or None

    def items(self):
        """Yields a 2-tuple for every active facet setting.

        This means the EntryPoint (handled by the superclass)
        as well as possible settings for 'media' and "min_score".
        """
        for k, v in super(SearchFacets, self).items():
            yield k, v
        if self.media_argument:
            yield ("media", self.media_argument)

        if self.min_score is not None:
            yield ('min_score', unicode(self.min_score))

    def navigate(self, **kwargs):
        min_score = kwargs.pop('min_score', self.min_score)
        new_facets = super(SearchFacets, self).navigate(**kwargs)
        new_facets.min_score = min_score
        return new_facets

class Pagination(object):

    DEFAULT_SIZE = 50
    DEFAULT_SEARCH_SIZE = 10
    DEFAULT_FEATURED_SIZE = 10
    DEFAULT_CRAWLABLE_SIZE = 100
    MAX_SIZE = 100

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
        self.page_has_loaded = False
        self.max_size = self.MAX_SIZE

    @classmethod
    def _int_from_request(cls, key, get_arg, make_detail, default):
        """Helper method to get and parse an integer value from
        a URL query argument in a Flask request.

        :param key: Name of the argument.
        :param get_arg: A function which when called with (key, default)
           returns the value of the query argument.
        :pass make_detail: A function, called with the value
           obtained from the request, which returns the detail
           information that should be included in a problem detail
           document if the input isn't convertable to an integer.
        :param default: Use this value if none is specified.
        """
        raw = get_arg(key, default)
        try:
            as_int = int(raw)
        except ValueError:
            return INVALID_INPUT.detailed(make_detail(raw))
        return as_int

    @classmethod
    def size_from_request(cls, get_arg, default):
        make_detail = lambda size: (
            _("Invalid page size: %(size)s", size=size)
        )
        size = cls._int_from_request(
            'size', get_arg, make_detail, default or cls.DEFAULT_SIZE
        )
        if isinstance(size, ProblemDetail):
            return size
        return min(size, cls.MAX_SIZE)

    @classmethod
    def from_request(cls, get_arg, default_size=None):
        """Instantiate a Pagination object from a Flask request."""
        default_size = default_size or cls.DEFAULT_SIZE
        size = cls.size_from_request(get_arg, default_size)
        if isinstance(size, ProblemDetail):
            return size
        offset = cls._int_from_request(
            'after', get_arg,
            lambda offset: _("Invalid offset: %(offset)s", offset=offset),
            0
        )
        if isinstance(offset, ProblemDetail):
            return offset
        return cls(offset, size)

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
        if not self.has_next_page:
            return None
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

    def modify_database_query(self, _db, qu):
        """Modify the given database query with OFFSET and LIMIT."""
        return qu.offset(self.offset).limit(self.size)

    def modify_search_query(self, search):
        """Modify a Search object so that it retrieves only a single 'page'
        of results.

        :return: A Search object.
        """
        return search[self.offset:self.offset+self.size]

    def page_loaded(self, page):
        """An actual page of results has been fetched. Keep any internal state
        that would be useful to know when reasoning about earlier or
        later pages.
        """
        self.this_page_size = len(page)
        self.page_has_loaded = True


class WorkList(object):
    """An object that can obtain a list of Work objects for use
    in generating an OPDS feed.

    By default, these Work objects come from a search index.
    """

    # The default maximum cache time of a feed derived from a WorkList
    # is the default cache time for any OPDS feed.
    MAX_CACHE_AGE = OPDSFeed.DEFAULT_MAX_AGE

    # If a certain type of Worklist should always have its OPDS feeds
    # cached under a specific type, define that type as
    # CACHED_FEED_TYPE.
    CACHED_FEED_TYPE = None

    # By default, a WorkList is always visible.
    visible = True

    # By default, a WorkList does not draw from CustomLists
    uses_customlists = False

    def max_cache_age(self, type):
        """Determine how long a feed for this WorkList should be cached
        internally.
        """
        return self.MAX_CACHE_AGE

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
        wl = TopLevelWorkList()

        wl.initialize(
            library, display_name=library.name, children=top_level_lanes,
            media=Edition.FULFILLABLE_MEDIA, entrypoints=library.entrypoints
        )
        return wl

    def initialize(self, library, display_name=None, genres=None,
                   audiences=None, languages=None, media=None,
                   customlists=None, list_datasource=None,
                   list_seen_in_previous_days=None,
                   children=None, priority=None, entrypoints=None,
                   fiction=None, license_datasource=None,
                   target_age=None,
    ):
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

        :param fiction: Only Works with this fiction status will be included
            in lists.

        :param target_age: Only Works targeted at readers in this age range
            will be included in lists.

        :param license_datasource: Only Works with a LicensePool from this
            DataSource will be included in lists.

        :param customlists: Only Works included on one of these CustomLists
            will be included in lists.

        :param list_datasource: Only Works included on a CustomList
            associated with this DataSource will be included in
            lists. This overrides any specific CustomLists provided in
            `customlists`.

        :param list_seen_in_previous_days: Only Works that were added
            to a matching CustomList within this number of days will be
            included in lists.

        :param children: This WorkList has children, which are also
            WorkLists.

        :param priority: A number indicating where this WorkList should
            show up in relation to its siblings when it is the child of
            some other WorkList.

        :param entrypoints: A list of EntryPoint classes representing
            different ways of slicing up this WorkList.

        """
        self.library_id = None
        self.collection_ids = None
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
        self.fiction = fiction

        if license_datasource:
            self.license_datasource_id = license_datasource.id
        else:
            self.license_datasource_id = None

        # If a specific set of CustomLists was passed in, store their IDs.
        #
        # If a custom list DataSource was passed in, gather the IDs for
        # every CustomList associated with that DataSource, and store
        # those IDs.
        #
        # Either way, WorkList starts out with a specific list of IDs,
        # which simplifies the WorkList code in a way that isn't
        # available to Lane.
        self._customlist_ids = None
        self.list_datasource_id = None
        if list_datasource:
            customlists = list_datasource.custom_lists

            # We do also store the CustomList ID, which is used as an
            # optimization in customlist_filter_clauses().
            self.list_datasource_id = list_datasource.id

        # The custom list IDs are stored in _customlist_ids, for
        # compatibility with Lane.
        if customlists:
            self._customlist_ids = [x.id for x in customlists]
        self.list_seen_in_previous_days = list_seen_in_previous_days

        self.fiction = fiction
        self.target_age = target_age

        self.children = []
        if children:
            for child in children:
                self.append_child(child)
        self.priority = priority or 0

        if entrypoints:
            self.entrypoints = list(entrypoints)
        else:
            self.entrypoints = []

    def append_child(self, child):
        """Add one child to the list of children in this WorkList.

        This hook method can be overridden to modify the child's
        configuration so as to make it fit with what the parent is
        offering.
        """
        self.children.append(child)

    @property
    def customlist_ids(self):
        """Return the custom list IDs."""
        return self._customlist_ids

    @property
    def uses_customlists(self):
        """Does the works() implementation for this WorkList look for works on
        CustomLists?
        """
        if self._customlist_ids or self.list_datasource_id:
            return True
        return False

    def get_library(self, _db):
        """Find the Library object associated with this WorkList."""
        return Library.by_id(_db, self.library_id)

    def get_customlists(self, _db):
        """Get customlists associated with the Worklist."""
        if hasattr(self, "_customlist_ids") and self._customlist_ids is not None:
            return _db.query(CustomList).filter(CustomList.id.in_(self._customlist_ids)).all()
        return []

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
            key = lambda x: (x.priority, x.display_name or "")
        )

    @property
    def has_visible_children(self):
        for lane in self.visible_children:
            if lane:
                return True
        return False

    @property
    def parent(self):
        """A WorkList has no parent. This method is defined for compatibility
        with Lane.
        """
        return None

    @property
    def parentage(self):
        """WorkLists have no parentage. This method is defined for compatibility
        with Lane.
        """
        return []

    def is_self_or_descendant(self, ancestor):
        """Is this WorkList the given WorkList or one of its descendants?

        :param ancestor: A WorkList.
        :return: A boolean.
        """
        for candidate in [self] + list(self.parentage):
            if candidate == ancestor:
                return True
        return False

    @property
    def inherit_parent_restrictions(self):
        """Since a WorkList has no parent, it cannot inherit any restrictions
        from its parent. This method is defined for compatibility
        with Lane.
        """
        return False

    @property
    def hierarchy(self):
        """The portion of the WorkList hierarchy that culminates in this
        WorkList.
        """
        return list(reversed(list(self.parentage))) + [self]

    def inherited_value(self, k):
        """Try to find this WorkList's value for the given key (e.g. 'fiction'
        or 'audiences').

        If it's not set, try to inherit a value from the WorkList's
        parent. This only works if this WorkList has a parent and is
        configured to inherit values from its parent.

        Note that inheritance works differently for genre_ids and
        customlist_ids -- use inherited_values() for that.
        """
        value = getattr(self, k)
        if value not in (None, []):
            return value
        else:
            if not self.parent or not self.inherit_parent_restrictions:
                return None
            parent = self.parent
            return parent.inherited_value(k)

    def inherited_values(self, k):
        """Find the values for the given key (e.g. 'genre_ids' or
        'customlist_ids') imposed by this WorkList and its parentage.

        This is for values like .genre_ids and .customlist_ids, where
        each member of the WorkList hierarchy can impose a restriction
        on query results, and the effects of the restrictions are
        additive.
        """
        values = []
        if not self.inherit_parent_restrictions:
            hierarchy = [self]
        else:
            hierarchy = self.hierarchy
        for wl in hierarchy:
            value = getattr(wl, k)
            if value not in (None, []):
                values.append(value)
        return values

    @property
    def full_identifier(self):
        """A human-readable identifier for this WorkList that
        captures its position within the heirarchy.
        """
        full_parentage = [unicode(x.display_name) for x in self.hierarchy]
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

    @property
    def unique_key(self):
        """A string key that uniquely describes this WorkList within
        its Library.

        This is used when caching feeds for this WorkList. For Lanes,
        the lane_id is used instead.
        """
        return "%s-%s-%s" % (
            self.display_name, self.language_key, self.audience_key
        )

    def accessible_to(self, patron):
        """As a matter of library policy, is the given `Patron` allowed
        to access this `WorkList`?
        """
        if not patron:
            # We have no lanes that are private, per se, so if there
            # is no active patron, every lane is accessible.
            return True

        _db = Session.object_session(patron)
        if patron.library != self.get_library(_db):
            # You can't access a WorkList from another library.
            return False

        if not patron.library.has_root_lanes:
            # The patron's library has no root lanes, so it's not necessary
            # to run the somewhat expensive check for a patron's root lane.
            # All lanes are accessible to all patrons.
            return True

        # Get the patron's root lane, if any.
        root = patron.root_lane
        if not root:
            # A patron with no root lane can access every one of the
            # library's WorkLists.
            return True

        # A WorkList is only accessible if the audiences and target age
        # of the WorkList are fully compatible with that of the
        # patron's root lane.
        if self.audiences:
            for work_audience in self.audiences:
                # work_audience represents a type of book that _might_
                # show up in this WorkList.
                if not patron.work_is_age_appropriate(work_audience, self.target_age):
                    # Books of this type would not be appropriate to show to
                    # this patron, so the lane itself is not accessible.
                    return False

        return True

    def overview_facets(self, _db, facets):
        """Convert a generic FeaturedFacets to some other faceting object,
        suitable for showing an overview of this WorkList in a grouped
        feed.
        """
        return facets

    def groups(self, _db, include_sublanes=True, pagination=None, facets=None,
               search_engine=None, debug=False):
        """Extract a list of samples from each child of this WorkList.  This
        can be used to create a grouped acquisition feed for the WorkList.

        :param pagination: A Pagination object which may affect how many
            works each child of this WorkList may contribute.
        :param facets: A FeaturedFacets object that may restrict the works on view.
        :param search_engine: An ExternalSearchIndex to use when
            asking for the featured works in a given WorkList.
        :param debug: A debug argument passed into `search_engine` when
            running the search.
        :yield: A sequence of (Work, WorkList) 2-tuples, with each
            WorkList representing the child WorkList in which the Work is
            found.
        """
        if not include_sublanes:
            # We only need to find featured works for this lane,
            # not this lane plus its sublanes.
            adapted = self.overview_facets(_db, facets)
            for work in self.works(_db, pagination=pagination, facets=adapted):
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
                # Children that turn out to be Lanes go into
                # relevant_lanes. Their Works will be obtained from
                # the search index.
                relevant_lanes.append(child)
            # Both Lanes and WorkLists go into relevant_children.
            # This controls the yield order for Works.
            relevant_children.append(child)

        # _groups_for_lanes will run a query to pull featured works
        # for any children that are Lanes, and call groups()
        # recursively for any children that are not.
        for work, worklist in self._groups_for_lanes(
            _db, relevant_children, relevant_lanes, pagination=pagination,
            facets=facets, search_engine=search_engine, debug=debug
        ):
            yield work, worklist

    def works(self, _db, facets=None, pagination=None, search_engine=None,
              debug=False, **kwargs):

        """Use a search engine to obtain Work or Work-like objects that belong
        in this WorkList.

        Compare DatabaseBackedWorkList.works_from_database, which uses
        a database query to obtain the same Work objects.

        :param _db: A database connection.
        :param facets: A Facets object which may put additional
           constraints on WorkList membership.
        :param pagination: A Pagination object indicating which part of
           the WorkList the caller is looking at, and/or a limit on the
           number of works to fetch.
        :param kwargs: Different implementations may fetch the
           list of works from different sources and may need different
           keyword arguments.
        :return: A list of Work or Work-like objects, or a database query
            that generates such a list when executed.

        """
        from external_search import (
            Filter,
            ExternalSearchIndex,
        )
        search_engine = search_engine or ExternalSearchIndex.load(_db)
        filter = self.filter(_db, facets)
        hits = search_engine.query_works(
            query_string=None, filter=filter, pagination=pagination,
            debug=debug
        )
        return self.works_for_hits(_db, hits, facets=facets)

    def filter(self, _db, facets):
        """Helper method to instantiate a Filter object for this WorkList.

        Using this ensures that modify_search_filter_hook() is always
        called.
        """
        from external_search import Filter
        filter = Filter.from_worklist(_db, self, facets)
        modified = self.modify_search_filter_hook(filter)
        if modified is None:
            # The Filter was modified in place, rather than a new
            # Filter being returned.
            modified = filter
        return modified

    def modify_search_filter_hook(self, filter):
        """A hook method allowing subclasses to modify a Filter
        object that's about to find all the works in this WorkList.

        This can avoid the need for complex subclasses of Facets.
        """
        return filter

    def works_for_hits(self, _db, hits, facets=None):
        """Convert a list of search results into Work objects.

        This works by calling works_for_resultsets() on a list
        containing a single list of search results.

        :param _db: A database connection
        :param hits: A list of Hit objects from ElasticSearch.
        :return: A list of Work or (if the search results include
            script fields), WorkSearchResult objects.
        """

        [results] = self.works_for_resultsets(_db, [hits], facets=facets)
        return results

    def works_for_resultsets(self, _db, resultsets, facets=None):
        """Convert a list of lists of Hit objects into a list
        of lists of Work objects.
        """
        from external_search import (
            Filter,
            WorkSearchResult,
        )

        has_script_fields = None
        work_ids = set()
        for resultset in resultsets:
            for result in resultset:
                work_ids.add(result.work_id)
                if has_script_fields is None:
                    # We don't know whether any script fields were
                    # included, and now we're in a position to find
                    # out.
                    has_script_fields = (
                        any(
                            x in result for x in Filter.KNOWN_SCRIPT_FIELDS
                        )
                    )

        if has_script_fields is None:
            # This can only happen when there are no results. The code
            # will work even if has_script_fields is None, but just to
            # be safe.
            has_script_fields = False

        # The simplest way to turn Hits into Works is to create a
        # DatabaseBackedWorkList that fetches those specific Works
        # while applying the general availability filters.
        #
        # If facets were passed in, then they are used to further
        # filter the list.
        #
        # TODO: There's a lot of room for improvement here, but
        # performance isn't a big concern -- it's just ugly.
        wl = SpecificWorkList(work_ids)
        wl.initialize(self.get_library(_db))
        qu = wl.works_from_database(_db, facets=facets)
        a = time.time()
        all_works = qu.all()

        # Create a list of lists with the same membership as the original
        # `resultsets`, but with Hit objects replaced with Work objects.
        work_by_id = dict()
        for w in all_works:
            work_by_id[w.id] = w

        work_lists = []
        for resultset in resultsets:
            works = []
            work_lists.append(works)
            for hit in resultset:
                if hit.work_id in work_by_id:
                    work = work_by_id[hit.work_id]
                    if has_script_fields:
                        # Wrap the Work objects in WorkSearchResult so the
                        # data from script fields isn't lost.
                        work = WorkSearchResult(work, hit)
                    works.append(work)

        b = time.time()
        logging.info(
            u"Obtained %sxWork in %.2fsec", len(all_works), b-a
        )
        return work_lists

    @property
    def search_target(self):
        """By default, a WorkList is searchable."""
        return self

    def search(self, _db, query, search_client, pagination=None, facets=None,
               debug=False):
        """Find works in this WorkList that match a search query.

        :param _db: A database connection.
        :param query: Search for this string.
        :param search_client: An ExternalSearchIndex object.
        :param pagination: A Pagination object.
        :param facets: A faceting object, probably a SearchFacets.
        :param debug: Pass in True to see a summary of results returned
            from the search index.
        """
        results = []
        hits = None
        if not search_client:
            # We have no way of actually doing a search. Return nothing.
            return results

        if not pagination:
            pagination = Pagination(
                offset=0, size=Pagination.DEFAULT_SEARCH_SIZE
            )

        filter = self.filter(_db, facets)
        try:
            hits = search_client.query_works(
                query, filter, pagination, debug
            )
        except elasticsearch.exceptions.ElasticsearchException, e:
            logging.error(
                "Problem communicating with ElasticSearch. Returning empty list of search results.",
                exc_info=e
            )
        if hits:
            results = self.works_for_hits(_db, hits)

        return results

    def _groups_for_lanes(
        self, _db, relevant_lanes, queryable_lanes, pagination, facets,
        search_engine=None, debug=False
    ):
        """Ask the search engine for groups of featurable works in the
        given lanes. Fill in gaps as necessary.

        :param pagination: An optional Pagination object which will be
           used to paginate each group individually. Note that this
           means Pagination.page_loaded() method will be called once
           for each group.
        :param facets: A FeaturedFacets object.

        :param search_engine: An ExternalSearchIndex to use when
           asking for the featured works in a given WorkList.
        :param debug: A debug argument passed into `search_engine` when
           running the search.
        :yield: A sequence of (Work, WorkList) 2-tuples, with each
            WorkList representing the child WorkList in which the Work is
            found.

        """
        library = self.get_library(_db)
        if pagination is None:
            # No pagination object was provided. Our target size is
            # the featured lane size, but we'll ask for a few extra
            # works for each lane, to reduce the risk that we end up
            # reusing a book in two different lanes.
            target_size = library.featured_lane_size

            # We ask for a few extra works for each lane, to reduce the
            # risk that we'll end up reusing a book in two different
            # lanes.
            ask_for_size = max(target_size+1, int(target_size * 1.10))
            pagination = Pagination(size=ask_for_size)
        else:
            target_size = pagination.size

        from external_search import ExternalSearchIndex
        search_engine = search_engine or ExternalSearchIndex.load(_db)

        if isinstance(self, Lane):
            parent_lane = self
        else:
            parent_lane = None

        queryable_lane_set = set(queryable_lanes)
        works_and_lanes = list(
            self._featured_works_with_lanes(
                _db, queryable_lanes, pagination=pagination,
                facets=facets, search_engine=search_engine, debug=debug
            )
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
        for work, lane in works_and_lanes:
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

            if work.id in used_works:
                if work.id not in used_works_this_lane:
                    # We already used this work in another lane, but we
                    # might need to use it again to fill out this lane.
                    might_need_to_reuse[work.id] = work
            else:
                by_lane[lane].append(work)
                used_works.add(work.id)
                used_works_this_lane.add(work.id)

        # Close out the last lane encountered.
        _done_with_lane(working_lane)
        for lane in relevant_lanes:
            if lane in queryable_lane_set:
                # We found results for this lane through the main query.
                # Yield those results.
                for work in by_lane.get(lane, []):
                    yield (work, lane)
            else:
                # We didn't try to use the main query to find results
                # for this lane because we knew the results, if there
                # were any, wouldn't be representative. This is most
                # likely because this 'lane' is a WorkList and not a
                # Lane at all. Do a whole separate query and plug it
                # in at this point.
                for x in lane.groups(
                    _db, include_sublanes=False,
                        pagination=pagination, facets=facets,
                ):
                    yield x

    def _featured_works_with_lanes(
        self, _db, lanes, pagination, facets, search_engine, debug=False
    ):
        """Find a sequence of works that can be used to
        populate this lane's grouped acquisition feed.

        :param lanes: Classify Work objects
            as belonging to one of these WorkLists (presumably sublanes
            of `self`).
        :param facets: A faceting object, presumably a FeaturedFacets
        :param pagination: A Pagination object explaining how many
            items to ask for. In most cases this should be slightly more than
            the number of items you actually want, so that you have some
            slack to remove duplicates across multiple lanes.
        :param search_engine: An ExternalSearchIndex to use when
           asking for the featured works in a given WorkList.
        :param debug: A debug argument passed into `search_engine` when
           running the search.

        :yield: A sequence of (Work, Lane) 2-tuples.
        """
        if not lanes:
            # We can't run this query at all.
            return

        # Ask the search engine for works from every lane we're given.

        # NOTE: At the moment, every WorkList in the system can be
        # generated using an Elasticsearch query. That is, there are
        # no subclasses of the DatabaseExclusiveWorkList class defined
        # in circulation/api/lanes.py. If that ever changes, we'll
        # need to change this code.
        #
        # The simplest change would probably be to return a dictionary
        # mapping WorkList to Works and let the caller figure out the
        # ordering. In fact, we could start doing that now.
        queries = []
        for lane in lanes:
            overview_facets = lane.overview_facets(_db, facets)
            from external_search import Filter
            filter = Filter.from_worklist(_db, lane, overview_facets)
            queries.append((None, filter, pagination))
        resultsets = list(search_engine.query_works_multi(queries))
        works = self.works_for_resultsets(_db, resultsets, facets=facets)

        for i, lane in enumerate(lanes):
            results = works[i]
            for work in results:
                yield work, lane


class HierarchyWorkList(WorkList):
    """A WorkList representing part of a hierarchical view of a a
    library's collection. (As opposed to a non-hierarchical view such
    as search results or "books by author X".)
    """

    def accessible_to(self, patron):
        """As a matter of library policy, is the given `Patron` allowed
        to access this `WorkList`?

        Most of the logic is inherited from `WorkList`, but there's also
        a restriction based on the site hierarchy.

        :param patron: A Patron
        :return: A boolean
        """

        # All the rules of WorkList apply.
        if not super(HierarchyWorkList, self).accessible_to(patron):
            return False

        if patron is None:
            return True

        root_lane = patron.root_lane
        if root_lane and not self.is_self_or_descendant(root_lane):
            # In addition, a HierarchyWorkList that's not in
            # scope of the patron's root lane is not accessible,
            # period. Even if all of the books in the WorkList are
            # age-appropriate, it's in a different part of the
            # navigational structure and navigating to it is not
            # allowed.
            return False

        return True


class TopLevelWorkList(HierarchyWorkList):
    """A special WorkList representing the top-level view of
    a library's collection.
    """
    pass


class DatabaseBackedWorkList(WorkList):
    """A WorkList that can get its works from the database in addition to
    (or possibly instead of) the search index.

    Even when works _are_ obtained through the search index, a
    DatabaseBackedWorkList is then created to look up the Work objects
    for use in an OPDS feed.
    """

    def works_from_database(self, _db, facets=None, pagination=None, **kwargs):
        """Create a query against the `works` table that finds Work objects
        corresponding to all the Works that belong in this WorkList.

        The apply_filters() implementation defines which Works qualify
        for membership in a WorkList of this type.

        This tends to be slower than WorkList.works, but not all
        lanes can be generated through search engine queries.

        :param _db: A database connection.
        :param facets: A faceting object, which may place additional
           constraints on WorkList membership.
        :param pagination: A Pagination object indicating which part of
           the WorkList the caller is looking at.
        :param kwargs: Ignored -- only included for compatibility with works().
        :return: A Query.
        """

        qu = self.base_query(_db)

        # In general, we only show books that are present in one of
        # the WorkList's collections and ready to be delivered to
        # patrons.
        qu = self.only_show_ready_deliverable_works(_db, qu)

        # Apply to the database the bibliographic restrictions with
        # which this WorkList was initialized -- genre, audience, and
        # whatnot.
        qu, bibliographic_clauses = self.bibliographic_filter_clauses(_db, qu)
        if bibliographic_clauses:
            bibliographic_clause = and_(*bibliographic_clauses)
            qu = qu.filter(bibliographic_clause)

        # Allow the faceting object to modify the database query.
        if facets is not None:
            qu = facets.modify_database_query(_db, qu)

        # Allow a subclass to modify the database query.
        qu = self.modify_database_query_hook(_db, qu)

        if qu._distinct is False:
            # This query must always be made distinct, since a Work
            # can have more than one LicensePool. If no one else has
            # taken the opportunity to make it distinct (e.g. the
            # faceting object, while setting sort order), we'll make
            # it distinct based on work ID.
            qu = qu.distinct(Work.id)

        # Allow the pagination object to modify the database query.
        if pagination is not None:
            qu = pagination.modify_database_query(_db, qu)

        return qu

    @classmethod
    def base_query(cls, _db):
        """Return a query that contains the joins set up as necessary to
        create OPDS feeds.
        """
        qu = _db.query(
            Work
        ).join(
            Work.license_pools
        ).join(
            Work.presentation_edition
        ).filter(
            LicensePool.superceded==False
        )

        # Apply optimizations.
        qu = cls._modify_loading(qu)
        qu = cls._defer_unused_fields(qu)
        return qu

    @classmethod
    def _modify_loading(cls, qu):
        """Optimize a query for use in generating OPDS feeds, by modifying
        which related objects get pulled from the database.
        """
        # Avoid eager loading of objects that are already being loaded.
        qu = qu.options(
            contains_eager(Work.presentation_edition),
            contains_eager(Work.license_pools),
        )
        license_pool_name = 'license_pools'

        # Load some objects that wouldn't normally be loaded, but
        # which are necessary when generating OPDS feeds.

        # TODO: Strictly speaking, these joinedload calls are
        # only needed by the circulation manager. This code could
        # be moved to circulation and everyone else who uses this
        # would be a little faster. (But right now there is no one
        # else who uses this.)
        qu = qu.options(
            # These speed up the process of generating acquisition links.
            joinedload(license_pool_name, "delivery_mechanisms"),
            joinedload(license_pool_name, "delivery_mechanisms", "delivery_mechanism"),

            joinedload(license_pool_name, "identifier"),

            # These speed up the process of generating the open-access link
            # for open-access works.
            joinedload(license_pool_name, "delivery_mechanisms", "resource"),
            joinedload(license_pool_name, "delivery_mechanisms", "resource", "representation"),
        )
        return qu

    def only_show_ready_deliverable_works(
        self, _db, query, show_suppressed=False
    ):
        """Restrict a query to show only presentation-ready works present in
        an appropriate collection which the default client can
        fulfill.

        Note that this assumes the query has an active join against
        LicensePool.
        """
        return Collection.restrict_to_ready_deliverable_works(
            query, show_suppressed=show_suppressed,
            collection_ids=self.collection_ids
        )

    @classmethod
    def _defer_unused_fields(cls, query):
        """Some applications use the simple OPDS entry and some
        applications use the verbose. Whichever one we don't need,
        we can stop from even being sent over from the
        database.
        """
        if Configuration.DEFAULT_OPDS_FORMAT == "simple_opds_entry":
            return query.options(defer(Work.verbose_opds_entry))
        else:
            return query.options(defer(Work.simple_opds_entry))

    def bibliographic_filter_clauses(self, _db, qu):
        """Create a SQLAlchemy filter that excludes books whose bibliographic
        metadata doesn't match what we're looking for.

        query is either `qu`, or a new query that has been modified to
        join against additional tables.

        :return: A 2-tuple (query, clauses).

        """
        # Audience language, and genre restrictions are allowed on all
        # WorkLists. (So are collection restrictions, but those are
        # applied by only_show_ready_deliverable_works().
        clauses = self.audience_filter_clauses(_db, qu)
        if self.languages:
            clauses.append(Edition.language.in_(self.languages))
        if self.media:
            clauses.append(Edition.medium.in_(self.media))
        if self.fiction is not None:
            clauses.append(Work.fiction==self.fiction)
        if self.license_datasource_id:
            clauses.append(
                LicensePool.data_source_id==self.license_datasource_id
            )

        if self.genre_ids:
            qu, clause = self.genre_filter_clause(qu)
            if clause is not None:
                clauses.append(clause)

        if self.customlist_ids:
            qu, customlist_clauses = self.customlist_filter_clauses(qu)
            clauses.extend(customlist_clauses)

        clauses.extend(self.age_range_filter_clauses())

        if self.parent and self.inherit_parent_restrictions:
            # In addition to the other any other restrictions, books
            # will show up here only if they would also show up in the
            # parent WorkList.
            qu, parent_clauses = self.parent.bibliographic_filter_clauses(
                _db, qu
            )
            if parent_clauses:
                clauses.extend(parent_clauses)

        return qu, clauses

    def audience_filter_clauses(self, _db, qu):
        """Create a SQLAlchemy filter that excludes books whose intended
        audience doesn't match what we're looking for.
        """
        if not self.audiences:
            return []
        return [Work.audience.in_(self.audiences)]

    def customlist_filter_clauses(self, qu):
        """Create a filter clause that only books that are on one of the
        CustomLists allowed by Lane configuration.

        :return: A 3-tuple (query, clauses).

        `query` is the same query as `qu`, possibly extended with
        additional table joins.

        `clauses` is a list of SQLAlchemy statements for use in a
        filter() or case() statement.
        """
        if not self.uses_customlists:
            # This lane does not require that books be on any particular
            # CustomList.
            return qu, []

        # We will be joining against CustomListEntry at least
        # once. For a lane derived from the intersection of two or
        # more custom lists, we may be joining CustomListEntry
        # multiple times. To avoid confusion, we make a new alias for
        # the table every time.
        a_entry = aliased(CustomListEntry)

        clause = a_entry.work_id==Work.id
        qu = qu.join(a_entry, clause)

        # Actually build the restriction clauses.
        clauses = []
        customlist_ids = None
        if self.list_datasource_id:
            # Use a subquery to obtain the CustomList IDs of all
            # CustomLists from this DataSource. This is significantly
            # simpler than adding a join against CustomList.
            customlist_ids = Select(
                [CustomList.id],
                CustomList.data_source_id==self.list_datasource_id
            )
        else:
            customlist_ids = self.customlist_ids
        if customlist_ids is not None:
            clauses.append(a_entry.list_id.in_(customlist_ids))
        if self.list_seen_in_previous_days:
            cutoff = datetime.datetime.utcnow() - datetime.timedelta(
                self.list_seen_in_previous_days
            )
            clauses.append(a_entry.most_recent_appearance >=cutoff)

        return qu, clauses

    def genre_filter_clause(self, qu):
        wg = aliased(WorkGenre)
        qu = qu.join(wg, wg.work_id==Work.id)
        return qu, wg.genre_id.in_(self.genre_ids)

    def age_range_filter_clauses(self):
        """Create a clause that filters out all books not classified as
        suitable for this DatabaseBackedWorkList's age range.
        """
        if self.target_age is None:
            return []

        # self.target_age will be a NumericRange for Lanes and a tuple for
        # most other WorkLists. Make sure it's always a NumericRange.
        target_age = self.target_age
        if isinstance(target_age, tuple):
            target_age = tuple_to_numericrange(target_age)

        audiences = self.audiences or []
        adult_audiences = [
            Classifier.AUDIENCE_ADULT, Classifier.AUDIENCE_ADULTS_ONLY
        ]
        if (target_age.upper >= 18 or (
                any(x in audiences for x in adult_audiences))
        ):
            # Books for adults don't have target ages. If we're
            # including books for adults, either due to the audience
            # setting or the target age setting, allow the target age
            # to be empty.
            audience_has_no_target_age = Work.target_age == None
        else:
            audience_has_no_target_age = False

        # The lane's target age is an inclusive NumericRange --
        # set_target_age makes sure of that. The work's target age
        # must overlap that of the lane.

        return [
            or_(
                Work.target_age.overlaps(target_age),
                audience_has_no_target_age
            )
        ]

    def modify_database_query_hook(self, _db, qu):
        """A hook method allowing subclasses to modify a database query
        that's about to find all the works in this WorkList.

        This can avoid the need for complex subclasses of
        DatabaseBackedFacets.
        """
        return qu


class SpecificWorkList(DatabaseBackedWorkList):
    """A WorkList that only finds specific works, identified by ID."""
    def __init__(self, work_ids):
        super(SpecificWorkList, self).__init__()
        self.work_ids = work_ids

    def modify_database_query_hook(self, _db, qu):
        qu = qu.filter(
            Work.id.in_(self.work_ids),
            LicensePool.work_id.in_(self.work_ids), # Query optimization
        )
        return qu


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


class Lane(Base, DatabaseBackedWorkList, HierarchyWorkList):
    """A WorkList that draws its search criteria from a row in a
    database table.

    A Lane corresponds roughly to a section in a branch library or
    bookstore. Lanes are the primary means by which patrons discover
    books.
    """

    # The set of Works in a standard Lane is cacheable for twenty
    # minutes. Note that this only applies to paginated feeds --
    # grouped feeds are cached indefinitely.
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

    # How many titles are in this lane when viewed through a specific
    # entry point? This is periodically calculated and cached.
    size_by_entrypoint = Column(JSON, nullable=True)

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

    # A Lane may have many CachedMARCFiles.
    cachedmarcfiles = relationship(
        "CachedMARCFile", backref="lane",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        UniqueConstraint('parent_id', 'display_name'),
    )

    def get_library(self, _db):
        """For compatibility with WorkList.get_library()."""
        return self.library

    @property
    def list_datasource_id(self):
        return self._list_datasource_id

    @property
    def collection_ids(self):
        return [x.id for x in self.library.collections]

    @property
    def children(self):
        return self.sublanes

    @property
    def visible_children(self):
        children = [lane for lane in self.sublanes if lane.visible]
        return sorted(children, key=lambda x: (x.priority, x.display_name or ""))

    @property
    def parentage(self):
        """Yield the parent, grandparent, etc. of this Lane.

        The Lane may be inside one or more non-Lane WorkLists, but those
        WorkLists are not counted in the parentage.
        """
        if not self.parent:
            return
        parent = self.parent
        if Session.object_session(parent) is None:
            # This lane's parent was disconnected from its database session,
            # probably when an app server started up.
            # Reattach it to the database session used by this lane.
            parent = Session.object_session(self).merge(parent)

        yield parent
        seen = set([self, parent])
        for grandparent in parent.parentage:
            if grandparent in seen:
                raise ValueError("Lane parentage loop detected")
            seen.add(grandparent)
            yield grandparent

    def is_self_or_descendant(self, ancestor):
        """Is this WorkList the given WorkList or one of its descendants?

        :param ancestor: A WorkList.
        :return: A boolean.
        """
        if super(Lane, self).is_self_or_descendant(ancestor):
            return True

        # A TopLevelWorkList won't show up in a Lane's parentage,
        # because it's not a Lane, but if they share the same library
        # it can be presumed to be the lane's ultimate ancestor.
        if isinstance(ancestor, TopLevelWorkList) and self.library_id==ancestor.library_id:
            return True
        return False

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
            if hasattr(self, '_customlist_ids'):
                # The next time someone asks for .customlist_ids,
                # the list will be refreshed.
                del self._customlist_ids

        # TODO: It's not clear to me why it's necessary to set these two
        # values separately.
        self._list_datasource = value
        self._list_datasource_id = value.id

    @property
    def list_datasource_id(self):
        if self._list_datasource_id:
            return self._list_datasource_id
        return None

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

    def max_cache_age(self, type):
        """Determine how long a feed for this WorkList should be cached
        internally.

        :param type: The type of feed.
        """
        if type == CachedFeed.GROUPS_TYPE:
            # Generating grouped feeds on the fly for Lanes is not incredibly
            # expensive, but it's slow enough that we prefer to regenerate
            # them in the background (using force_refresh=True) rather
            # than while someone is waiting for an HTTP response.
            return CachedFeed.CACHE_FOREVER

        # Other than that, we have no opinion -- use the default.
        return super(Lane, self).max_cache_age(type)

    def update_size(self, _db, search_engine=None):
        """Update the stored estimate of the number of Works in this Lane."""
        library = self.get_library(_db)
        from external_search import ExternalSearchIndex
        search_engine = search_engine or ExternalSearchIndex.load(_db)

        # Do the estimate for every known entry point.
        by_entrypoint = dict()
        for entrypoint in EntryPoint.ENTRY_POINTS:
            facets = DatabaseBackedFacets(
                library, FacetConstants.COLLECTION_FULL,
                FacetConstants.AVAILABLE_ALL,
                order=FacetConstants.ORDER_WORK_ID, entrypoint=entrypoint
            )
            filter = self.filter(_db, facets)
            by_entrypoint[entrypoint.URI] = search_engine.count_works(filter)
        self.size_by_entrypoint = by_entrypoint
        self.size = by_entrypoint[EverythingEntryPoint.URI]

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

    def _size_for_facets(self, facets):
        """How big is this lane under the given `Facets` object?

        :param facets: A Facets object.
        :return: An int.
        """
        # Default to the total size of the lane.
        size = self.size

        entrypoint_name = EverythingEntryPoint.URI
        if facets and facets.entrypoint:
            entrypoint_name = facets.entrypoint.URI

        if (self.size_by_entrypoint
            and entrypoint_name in self.size_by_entrypoint):
            size = self.size_by_entrypoint[entrypoint_name]
        return size

    def groups(self, _db, include_sublanes=True, pagination=None, facets=None,
               search_engine=None, debug=False):
        """Return a list of (Work, Lane) 2-tuples
        describing a sequence of featured items for this lane and
        (optionally) its children.

        :param pagination: A Pagination object which may affect how many
            works each child of this WorkList may contribute.
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
            _db, relevant_lanes, queryable_lanes, pagination=pagination,
            facets=facets, search_engine=search_engine, debug=debug
        )

    def search(self, _db, query_string, search_client, pagination=None,
               facets=None):
        """Find works in this lane that also match a search query.

        :param _db: A database connection.
        :param query_string: Search for this string.
        :param search_client: An ExternalSearchIndex object.
        :param pagination: A Pagination object.
        :param facets: A faceting object, probably a SearchFacets.
        """
        search_target = self.search_target

        if search_target == self:
            # The actual implementation happens in WorkList.
            m = super(Lane, self).search
        else:
            # Searches in this Lane actually go against some other WorkList.
            # Tell that object to run the search.
            m = search_target.search

        return m(_db, query_string, search_client, pagination,
                 facets=facets)

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

        # Some elements of Lane configuration are stored in the
        # corresponding Library objects for performance reasons.

        # Remove this information whenever the Lane configuration
        # changes. This will force it to be recalculated.
        Library._has_root_lane_cache.clear()
