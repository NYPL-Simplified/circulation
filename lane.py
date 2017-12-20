from collections import defaultdict
from nose.tools import set_trace
import datetime
import logging
import random
import time
import urllib

from psycopg2.extras import NumericRange

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
    Table,
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

from model import (
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
from util import (
    fast_query_count,
    LanguageCodes,
)

import elasticsearch

from sqlalchemy import (
    event,
    Boolean,
    Column,
    ForeignKey,
    Integer,
    Unicode,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import (
    ARRAY,
    INT4RANGE,
)

class Facets(FacetConstants):

    @classmethod
    def default(cls, library):
        return cls(
            library,
            collection=cls.COLLECTION_MAIN,
            availability=cls.AVAILABLE_ALL,
            order=cls.ORDER_AUTHOR
        )
    
    def __init__(self, library, collection, availability, order,
                 order_ascending=None, enabled_facets=None):
        """
        :param collection: This is not a Collection object; it's a value for
        the 'collection' facet, e.g. 'main' or 'featured'.
        """
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

        if (availability == self.AVAILABLE_ALL and not library.allow_holds):
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

    def navigate(self, collection=None, availability=None, order=None):
        """Create a slightly different Facets object from this one."""
        return Facets(self.library,
                      collection or self.collection, 
                      availability or self.availability, 
                      order or self.order,
                      enabled_facets=self.facets_enabled_at_init)

    def items(self):
        if self.order:
            yield (self.ORDER_FACET_GROUP_NAME, self.order)
        if self.availability:
            yield (self.AVAILABILITY_FACET_GROUP_NAME,  self.availability)        
        if self.collection:
            yield (self.COLLECTION_FACET_GROUP_NAME, self.collection)

    @property
    def query_string(self):
        return "&".join("=".join(x) for x in sorted(self.items()))

    @property
    def enabled_facets(self):
        """Yield a 3-tuple of lists (order, availability, collection)
        representing facet values enabled via initialization or Configuration
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
    def order_facet_to_database_field(cls, order_facet, work_model):
        """Turn the name of an order facet into a materialized-view field
        for use in an ORDER BY clause.

        :param work_model: Either MaterializedWork or
        MaterializedWorkWithGenre.
        """
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

    def apply(self, _db, qu, work_model=None, distinct=False):
        """Restrict a query so that it only matches works that fit
        the given facets, and the query is ordered appropriately.

        :param work_model: Either MaterializedWork or
        MaterializedWorkWithGenre.
        """
        if work_model is None:
            from model import MaterializedWork
            work_model = MaterializedWork
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
        order_by, order_distinct = self.order_by(work_model)
        qu = qu.order_by(*order_by)
        if distinct:
            qu = qu.distinct(*order_distinct)

        return qu

    def order_by(self, work_model):
        """Establish a complete ORDER BY clause for works.

        :param work_model: Either MaterializedWork or
        MaterializedWorkWithGenre.
        """
        if work_model == Work:
            work_id = Work.id
        else:
            work_id = work_model.works_id
        default_sort_order = [
            work_model.sort_author, work_model.sort_title, work_id
        ]
    
        primary_order_by = self.order_facet_to_database_field(
            self.order, work_model
        )
        if primary_order_by:
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


class FeaturedFacets(object):

    """A Facets-like object that configures a query so that the 'most
    featurable' items are at the front.

    The only method of Facets implemented is apply(), and there is no
    way to navigate to or from this Facets object. It's just a
    convenient thing to pass into Lane.works().
    """

    def __init__(self, minimum_featured_quality, uses_customlists):
        """Set up an object that finds featured books in a given
        WorkList.
        """
        self.minimum_featured_quality = minimum_featured_quality
        self.uses_customlists = uses_customlists

    def apply(self, _db, qu, work_model, distinct):

        quality = self.quality_tier_field(work_model)
        qu = qu.order_by(quality.desc(), work_model.random)
        if distinct:
            if work_model is Work:
                id_field = work_model.id
            else:
                id_field = work_model.works_id
            qu = qu.distinct(quality, work_model.random, id_field)
        return qu

    def quality_tier_field(self, mv):
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

        :param mv: Either MaterializedWork, MaterializedWorkWithGenre,
        or Work is acceptable here.
        """
        if hasattr(self, '_quality_tier_field'):
            return self._quality_tier_field
        featurable_quality = self.minimum_featured_quality

        # Being of featureable quality is great.
        featurable_quality = case(
            [(mv.quality >= featurable_quality, 5)],
            else_=0
        )

        # Being a licensed work or an open-access work of decent quality
        # is good.
        regular_collection = case(
            [(or_(LicensePool.open_access==False, mv.quality >= 0.3), 2)],
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
        self._quality_tier_field = tier
        return self._quality_tier_field


class Pagination(object):

    DEFAULT_SIZE = 50
    DEFAULT_SEARCH_SIZE = 10
    DEFAULT_FEATURED_SIZE = 10

    @classmethod
    def default(cls):
        return Pagination(0, cls.DEFAULT_SIZE)

    def __init__(self, offset=0, size=DEFAULT_SIZE):
        self.offset = offset
        self.size = size
        self.query_size = None

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

        This method only returns valid information _after_ self.apply
        has been run on a query.
        """
        if self.query_size is None:
            return True
        if self.query_size==0:
            return False
        return self.offset + self.size < self.query_size

    def apply(self, qu):
        """Modify the given query with OFFSET and LIMIT."""
        self.query_size = fast_query_count(qu)
        return qu.offset(self.offset).limit(self.size)


class WorkList(object):
    """An object that can obtain a list of
    Work/MaterializedWork/MaterializedWorkWithGenre objects
    for use in generating an OPDS feed.
    """

    # Unless a sitewide setting intervenes, the set of Works in a
    # WorkList is cacheable for two weeks by default.
    MAX_CACHE_AGE = 14*24*60*60

    # By default, a WorkList is always visible.
    visible = True

    # By default, a WorkList does not draw from CustomLists
    uses_customlists = False

    def initialize(self, library, display_name=None, genres=None, 
                   audiences=None, languages=None, media=None,
                   children=None, priority=None):
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
        """
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
    def full_identifier(self):
        """A human-readable identifier for this WorkList that
        captures its position within the heirarchy.
        """
        lane_parentage = list(self.parentage) + [self]
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

    def groups(self, _db):
        """Extract a list of samples from each child of this WorkList.  This
        can be used to create a grouped acquisition feed for the WorkList.

        :return: A list of (Work, WorkList) 2-tuples, with each WorkList
        representing the child WorkList in which the Work is found.
        """
        # This is a list rather than a dict because we want to
        # preserve the ordering of the children.
        works_and_worklists = []
        for child in self.visible_children:
            if isinstance(child, Lane):
                child = _db.merge(child)
            works = child.featured_works(_db)
            for work in works:
                works_and_worklists.append((work, child))
        return works_and_worklists

    def featured_works(self, _db):
        """Find a random sample of featured books.

        Used when building a grouped OPDS feed for this WorkList's parent.

        While it's semi-okay for this method to be slow for the Lanes
        that make up the bulk of a circulation manager's offerings,
        other WorkList implementations may need to do something
        simpler for performance reasons.

        :return: A list of MaterializedWork or MaterializedWorkWithGenre
        objects.
        """
        books = []
        book_ids = set()
        featured_subquery = None
        library = self.get_library(_db)
        target_size = library.featured_lane_size

        facets = FeaturedFacets(
            library.minimum_featured_quality,
            self.uses_customlists
        )
        query = self.works(_db, facets=facets)
        if not query:
            # works() may return None, indicating that the whole
            # thing is a bad idea and the query should not even be
            # run.
            return []

        works = []
        for work in self.random_sample(query, target_size)[:target_size]:
            if isinstance(work, tuple):
                # This is a (work, score) 2-tuple.
                works.append(work[0])
            else:
                # This is a regular work.
                works.append(work)
        return works

    def works(self, _db, facets=None, pagination=None):
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
            MaterializedWork,
            MaterializedWorkWithGenre,
        )
        if self.genre_ids:
            mw = MaterializedWorkWithGenre
            # apply_filters() will apply the genre
            # restrictions.
        else:
            mw = MaterializedWork

        if isinstance(facets, FeaturedFacets):
            qu = _db.query(mw, facets.quality_tier_field(mw))
        else:
            qu = _db.query(mw)

        # Apply some database optimizations.
        qu = self._lazy_load(qu, mw)
        qu = self._defer_unused_fields(qu, mw)

        # apply_filters() requires that the query include a join
        # against LicensePool. If nothing else, the `facets` may
        # restrict the query to currently available items.
        qu = qu.join(LicensePool, LicensePool.id==mw.license_pool_id)
        qu = qu.options(contains_eager(mw.license_pool))
        if self.collection_ids is not None:
            qu = qu.filter(
                LicensePool.collection_id.in_(self.collection_ids)
            )

        return self.apply_filters(_db, qu, mw, facets, pagination)

    def works_for_specific_ids(self, _db, work_ids):
        """Create the appearance of having called works(),
        but return the specific MaterializedWorks identified by `work_ids`.
        """

        # Get a list of MaterializedWorks as though we had called works().
        from model import MaterializedWork as mw
        qu = _db.query(mw).join(
            LicensePool, mw.license_pool_id==LicensePool.id
        ).filter(
            mw.works_id.in_(work_ids)
        )
        qu = self._lazy_load(qu, mw)
        qu = self._defer_unused_fields(qu, mw)
        qu = self.only_show_ready_deliverable_works(_db, qu, mw)
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

    def apply_filters(self, _db, qu, work_model, facets, pagination,
                      featured=False):
        """Apply common WorkList filters to a query. Also apply any
        subclass-specific filters defined by
        bibliographic_filter_clause().
        """
        # In general, we only show books that are ready to be delivered
        # to patrons.
        qu = self.only_show_ready_deliverable_works(_db, qu, work_model)

        # This method applies whatever filters are necessary to implement
        # the rules of this particular WorkList.
        qu, bibliographic_clause, distinct = self.bibliographic_filter_clause(
            _db, qu, work_model, featured
        )
        if not qu:
            # bibliographic_filter_clause() may return a null query to
            # indicate that the WorkList should not exist at all.
            return None
        if bibliographic_clause is not None:
            qu = qu.filter(bibliographic_clause)

        if facets:
            qu = facets.apply(_db, qu, work_model, distinct=distinct)
        elif distinct:
            # Something about the query makes it possible that the same
            # book might show up twice. We set the query as DISTINCT
            # to avoid this possibility.
            qu = qu.distinct()

        if pagination:
            qu = pagination.apply(qu)
        return qu

    def bibliographic_filter_clause(self, _db, qu, work_model, featured=False):
        """Create a SQLAlchemy filter that excludes books whose bibliographic
        metadata doesn't match what we're looking for.

        :return: A 3-tuple (query, clause, distinct).

        - query is either `qu`, or a new query that has been modified to
        join against additional tables.
        """
        # Audience and language restrictions are common to all
        # WorkLists. (So are genre and collection restrictions, but those
        # were applied back in works().)

        clauses = self.audience_filter_clauses(_db, qu, work_model)
        if self.languages:
            clauses.append(work_model.language.in_(self.languages))
        if self.media:
            clauses.append(work_model.medium.in_(self.media))
        if self.genre_ids:
            clauses.append(work_model.genre_id.in_(self.genre_ids))
        if not clauses:
            clause = None
        else:
            clause = and_(*clauses)
        return qu, clause, False

    def audience_filter_clauses(self, _db, qu, work_model):
        """Create a SQLAlchemy filter that excludes books whose intended
        audience doesn't match what we're looking for.
        """
        if not self.audiences:
            return []
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
            self, _db, query, work_model, show_suppressed=False
    ):
        """Restrict a query to show only presentation-ready works present in
        an appropriate collection which the default client can
        fulfill.

        Note that this assumes the query has an active join against
        LicensePool.
        """
        return self.get_library(_db).restrict_to_ready_deliverable_works(
            query, work_model, show_suppressed=show_suppressed,
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
    def _lazy_load(cls, qu, work_model):
        """Avoid eager loading of objects that are contained in the 
        materialized view.
        """
        return qu.options(
            lazyload(work_model.license_pool, LicensePool.data_source),
            lazyload(work_model.license_pool, LicensePool.identifier),
            lazyload(work_model.license_pool, LicensePool.presentation_edition),
        )

    @classmethod
    def _defer_unused_fields(cls, query, work_model):
        """Some applications use the simple OPDS entry and some
        applications use the verbose. Whichever one we don't need,
        we can stop from even being sent over from the
        database.
        """
        if Configuration.DEFAULT_OPDS_FORMAT == "simple_opds_entry":
            return query.options(defer(work_model.verbose_opds_entry))
        else:
            return query.options(defer(work_model.simple_opds_entry))

    @property
    def search_target(self):
        """By default, a WorkList is searchable."""
        return self

    def search(self, _db, query, search_client, pagination=None):
        """Find works in this WorkList that match a search query."""
        if not pagination:
            pagination = Pagination(
                offset=0, size=Pagination.DEFAULT_SEARCH_SIZE
            )

        # Get the search results from Elasticsearch.
        results = None

        if self.target_age:
            target_age = numericrange_to_tuple(self.target_age)
        else:
            target_age = None

        if search_client:
            docs = None
            a = time.time()
            try:
                docs = search_client.query_works(
                    library=self.get_library(_db),
                    query_string=query,
                    media=self.media,
                    languages=self.languages,
                    fiction=self.fiction,
                    audiences=self.audiences,
                    target_age=target_age,
                    in_any_of_these_genres=self.genre_ids,
                    fields=["_id", "title", "author", "license_pool_id"],
                    size=pagination.size,
                    offset=pagination.offset,
                )
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
    inherit_parent_restrictions = Column(Boolean, default=False, nullable=False)

    # Patrons whose external type is in this list will be sent to this
    # lane when they ask for the root lane.
    #
    # This is almost never necessary.
    root_for_patron_type = Column(ARRAY(Unicode), nullable=True)

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

    @hybrid_property
    def visible(self):
        return self._visible and (not self.parent or self.parent.visible)

    @visible.setter
    def set_visible(self, value):
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
    def set_audiences(self, value):
        """The `audiences` field cannot be set to a value that
        contradicts the current value to the `target_age` field.
        """
        if self._audiences and self._target_age and value != self._audiences:
            raise ValueError("Cannot modify Lane.audiences when Lane.target_age is set!")
        self._audiences = value

    @hybrid_property
    def target_age(self):
        return self._target_age

    @target_age.setter
    def set_target_age(self, value):
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
    def set_list_datasource(self, value):
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
        self.size = fast_query_count(self.works(_db).limit(None))

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

    def groups(self, _db):
        """Extract a list of samples from each child of this Lane, as well as
        from the lane itself. This can be used to create a grouped
        acquisition feed for the Lane.

        :return: A list of (Work, Lane) 2-tuples, with each Lane
        representing the Lane in which the Work can be found.
        """
        # This takes care of all of the children.
        works_and_lanes = super(Lane, self).groups(_db)

        if not works_and_lanes:
            # The children of this Lane did not contribute any works
            # to the groups feed. This means there should not be
            # a groups feed in the first place -- we should send a list
            # feed instead.
            return works_and_lanes

        # The children of this Lane contributed works to the groups
        # feed, which means we need an additional group in the feed
        # representing everything in the Lane (since the child lanes
        # are almost never exhaustive).
        lane = _db.merge(self)
        works = lane.featured_works(_db)
        for work in works:
            works_and_lanes.append((work, lane))
        return works_and_lanes
           
    def search(self, _db, query, search_client, pagination=None):
        """Find works in this lane that also match a search query.
        """
        target = self.search_target

        if target == self:
            return super(Lane, self).search(_db, query, search_client, pagination)
        else:
            return target.search(_db, query, search_client, pagination)

    def bibliographic_filter_clause(self, _db, qu, work_model, featured):
        """Create an AND clause that restricts a query to find
        only works classified in this lane.

        :param qu: A Query object. The filter will not be applied to this
        Query, but the query may be extended with additional table joins.

        :return: A 3-tuple (query, statement, distinct).

        `query` is the same query as `qu`, possibly extended with
        additional table joins.

        `statement` is a SQLAlchemy statement suitable for passing
        into filter() or case().

        `distinct` is whether or not the query needs to be set as
        DISTINCT.
        """
        qu, superclass_clause, superclass_distinct = super(
            Lane, self
        ).bibliographic_filter_clause(
            _db, qu, work_model, featured
        )
        clauses = []
        if superclass_clause is not None:
            clauses.append(superclass_clause)
        if self.parent and self.inherit_parent_restrictions:
            # In addition to the other restrictions imposed by this
            # Lane, books will show up here only if they would
            # also show up in the parent Lane.
            qu, clause, parent_distinct = self.parent.bibliographic_filter_clause(
                _db, qu, work_model, featured
            )
            if clause is not None:
                clauses.append(clause)
        else:
            parent_distinct = False

        # If a license source is specified, only show books from that
        # source.
        if self.license_datasource:
            clauses.append(LicensePool.data_source==self.license_datasource)

        if self.fiction is not None:
            clauses.append(work_model.fiction==self.fiction)

        if self.media:
            clauses.append(work_model.medium.in_(self.media))

        clauses.extend(self.age_range_filter_clauses(work_model))
        qu, customlist_clauses, customlist_distinct = self.customlist_filter_clauses(
            qu, work_model, featured
        )
        clauses.extend(customlist_clauses)
        
        return qu, and_(*clauses), (
            superclass_distinct or parent_distinct or customlist_distinct
        )

    def age_range_filter_clauses(self, work_model):
        """Create a clause that filters out all books not classified as
        suitable for this Lane's age range.
        """

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
            self, qu, work_model, must_be_featured=False
    ):
        """Create a filter clause that only books that are on one of the
        CustomLists allowed by Lane configuration.

        :param must_be_featured: It's not enough for the book to be on
        an appropriate list; it must be _featured_ on an appropriate list.

        :return: A 3-tuple (query, clauses, distinct).

        `query` is the same query as `qu`, possibly extended with
        additional table joins.

        `clauses` is a list of SQLAlchemy statements for use in a
        filter() or case() statement.

        `distinct` is whether or not the query needs to be set as
        DISTINCT.
        """
        if not self.customlists and not self.list_datasource:
            # This lane does not require that books be on any particular
            # CustomList.
            return qu, [], False

        # There may already be a join against CustomListEntry, in the case 
        # of a Lane that inherits its parent's restrictions. To avoid
        # confusion, create a different join every time.
        a_entry = aliased(CustomListEntry)
        if work_model == Work:
            clause = a_entry.work_id==work_model.id
        else:
            clause = a_entry.work_id==work_model.works_id
        qu = qu.join(a_entry, clause)
        a_list = aliased(CustomListEntry.customlist)
        qu = qu.join(a_list, a_entry.list_id==a_list.id)

        # Actually build the restriction clauses.
        clauses = []
        if self.list_datasource:
            clauses.append(a_list.data_source==self.list_datasource)
        customlist_ids = [x.id for x in self.customlists]

        # Now that custom list(s) are involved, we must (probably)
        # eventually set DISTINCT to True on the query.
        distinct = True
        if customlist_ids:
            clauses.append(a_list.id.in_(customlist_ids))
            if len(customlist_ids) == 1:
                # There's only one list, so no risk that a book
                # might show up more than once.
                distinct = False
        if must_be_featured:
            clauses.append(a_entry.featured==True)
        if self.list_seen_in_previous_days:
            cutoff = datetime.datetime.utcnow() - datetime.timedelta(
                self.list_seen_in_previous_days
            )
            clauses.append(a_entry.most_recent_appearance >=cutoff)
            
        return qu, clauses, distinct

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
@event.listens_for(Lane, 'after_update')
@event.listens_for(LaneGenre, 'after_insert')
@event.listens_for(LaneGenre, 'after_delete')
@event.listens_for(LaneGenre, 'after_update')
def configuration_relevant_lifecycle_event(mapper, connection, target):
    site_configuration_has_changed(target)
