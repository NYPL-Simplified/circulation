from collections import defaultdict
from nose.tools import set_trace
import datetime
import logging
import random
import time
import urllib

from psycopg2.extras import NumericRange

from config import Configuration

import classifier
from classifier import (
    Classifier,
    GenreData,
)

from sqlalchemy import (
    and_,
    or_,
    not_,
    Table,
)
from sqlalchemy.orm import (
    contains_eager,
    defer,
    joinedload,
    lazyload,
    relationship,
)

from model import (
    Base,
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
from util import fast_query_count
import elasticsearch

from sqlalchemy import (
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
    def order_facet_to_database_field(
            cls, order_facet, work_model, edition_model
    ):
        """Turn the name of an order facet into a database field
        for use in an ORDER BY clause.
        """
        if order_facet == cls.ORDER_WORK_ID:
            if work_model is Work:
                return work_model.id
            else:
                # This is a materialized view and the field name is
                # different.
                return work_model.works_id

        if order_facet == cls.ORDER_ADDED_TO_COLLECTION:
            if work_model is Work:
                # We must get this data from LicensePool.
                return LicensePool.availability_time
            else:
                # We can get this data from the materialized view.
                return work_model.availability_time

        # In all other cases the field names are the same whether
        # we are using Work/Edition or a materialized view.
        order_facet_to_database_field = {
            cls.ORDER_TITLE : edition_model.sort_title,
            cls.ORDER_AUTHOR : edition_model.sort_author,
            cls.ORDER_LAST_UPDATE : work_model.last_update_time,
            cls.ORDER_SERIES_POSITION : edition_model.series_position,
            cls.ORDER_RANDOM : work_model.random,
        }
        return order_facet_to_database_field[order_facet]

    def apply(self, _db, qu, work_model=Work, edition_model=Edition,
              distinct=False):
        """Restrict a query so that it only matches works that fit
        the given facets, and the query is ordered appropriately.
        """
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
        order_by, order_distinct = self.order_by(
            work_model, edition_model
        )
        qu = qu.order_by(*order_by)
        if distinct:
            qu = qu.distinct(*order_distinct)

        return q

    def order_by(self, work_model, edition_model):
        """Establish a complete ORDER BY clause for books."""
        if work_model == Work:
            work_id = Work.id
        else:
            work_id = work_model.works_id
        default_sort_order = [
            edition_model.sort_author, edition_model.sort_title, work_id
        ]
    
        primary_order_by = self.order_facet_to_database_field(
            self.order, work_model, edition_model
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

    def apply(self, q):
        """Modify the given query with OFFSET and LIMIT."""
        self.query_size = fast_query_count(q)
        return qu.offset(self.offset).limit(self.size)


class WorkList(object):
    """An object that can obtain a list of
    Work/MaterializedWork/MaterializedWorkWithGenre objects
    for use in generating an OPDS feed.
    """

    MINIMUM_SAMPLE_SIZE = 0

    def initialize(self, library, genres=None, audiences=None, languages=None):
        """Initialize with basic data.

        This is not a constructor, to avoid conflicts with `Lane`, an
        ORM object that subclasses this object but does not use this
        initialization code.
        
        :param library: Only Works available in this Library will be
        included in lists.

        :param genres: Only Works classified under one of these Genres
        will be included in lists.

        :param audiences: Only Works classified under one of these audiences
        will be included in lists.

        :param languages: Only Works in one of these languages will be
        included in lists.

        """
        self.library_id = library.id
        self.collection_ids = [
            collection.id for collection in library.all_collections
        ]
        if genres:
            self.genre_ids = [x.id for x in genres]
        else:
            self.genre_ids = None
        self.audiences = audiences
        self.languages = languages

    def library(self, _db):
        """Find the Library object associated with this WorkList."""
        return Library.by_id(_db, self.library_id)

    @property
    def visible_children(self):
        """A WorkList's children can be used to create a grouped acquisition
        feed for that WorkList.

        By default, a WorkList has no children.
        """
        return []

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
            works = child.featured_works(_db)
            for work in works:
                works_and_worklists.append((work, child))
        return works_and_worklists

    def featured_works(self, _db):
        """Extract a random sample of high-quality works from the WorkList.

        Used when building a grouped OPDS feed for this WorkList's parent.
        """
        # TODO: It seems like quality and ordering by Work.random need
        # to be involved here.

        # Build a query that would find all of the works.
        query = self.works(_db, featured=True)
        if not query:
            return []

        # Then take a random sample from that query.
        target_size = self.library(_db).featured_lane_size
        return self.random_sample(query, target_size=target_size)

    def works(self, _db, facets=None, pagination=None, featured=False):

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
        :param featured: If this is true, then Works that belong on a
           WorkList by virtue of belonging in a CustomList must be _featured_
           on that CustomList. If this is False, then all Works on an
           eligible CustomList are also on the WorkList. If the 
           WorkList does not consider CustomLists at all, then this value is
           irrelevant.
        :return: A Query, or None if the WorkList is deemed to be a
           bad idea in the first place.
        """
        from model import (
            MaterializedWork,
            MaterializedWorkWithGenre,
        )
        if self.genre_ids:
            mw = MaterializedWorkWithGenre
            qu = _db.query(mw)
            # apply_bibliographic_filters() will apply the genre
            # restrictions.
        else:
            mw = MaterializedWork
            qu = self._db.query(mw)

        # Apply some database optimizations.
        qu = self._lazy_load(qu, mw)
        qu = self._defer_unused_fields(qu, mw)

        # apply_filters() requires that the query include a join
        # against LicensePool. If nothing else, the `facets` may
        # restrict the query to currently available items.
        qu = qu.join(LicensePool, LicensePool.id==mw.license_pool_id)
        qu = qu.options(contains_eager(mw.license_pool))
        if self.collection_ids:
            qu = qu.filter(
                LicensePool.collection_id.in_(self.collection_ids)
            )

        return self.apply_filters(
            _db, qu, work_model, facets, pagination, featured
        )

    @classmethod
    def works_for_specific_ids(self, _db, work_ids):
        """Create the appearance of having called works() on a WorkList object,
        but return the specific Works identified by `work_ids`.
        """

        # Get a list of MaterializedWorks as though we had called works()
        from model import MaterializedWork as mw
        qu = _db.query(mw).join(
            LicensePool, mw.license_pool_id==LicensePool.id
        ).filter(
            mw.works_id.in_(doc_ids)
        )
        qu = self._lazy_load(qu, mw)
        qu = self._defer_unused_fields(qu, mw)
        qu = self.only_show_ready_deliverable_works(qu, mw)
        work_by_id = dict()
        a = time.time()
        works = qu.all()

        # Put the Work objects in the same order as their work_ids were.
        for mw in works:
            work_by_id[mw.works_id] = mw
        results = [work_by_id[x] for x in doc_ids if x in work_by_id]

        b = time.time()
        logging.debug(
            "Obtained %d MaterializedWork objects in %.2fsec",
            len(results), b-a
        )
        return results

    def apply_filters(self, _db, qu, work_model, facets, pagination,
                      featured=False):
        """Apply common WorkList filters to a query. Also apply
        subclass-specific filters by calling
        apply_bibliographic_filters(), which will call the
        apply_custom_filters() hook method.
        """
        # In general, we only show books that are ready to be delivered
        # to patrons.
        qu = self.only_show_ready_deliverable_works(qu, work_model)

        # This method applies whatever filters are necessary to implement
        # the rules of this particular WorkList.
        qu, distinct = self.apply_bibliographic_filters(
            _db, qu, work_model, featured
        )
        if not qu:
            # apply_bibliographic_filters() may return a null query to
            # indicate that the WorkList should not exist at all.
            return None

        if facets:
            qu = facets.apply(self._db, qu, work_model, distinct=distinct)
        elif distinct:
            # Something about the query makes it possible that the same
            # book might show up twice. We set the query as DISTINCT
            # to avoid this possibility.
            qu = qu.distinct()

        if pagination:
            qu = pagination.apply(qu)
        return qu

    def apply_bibliographic_filters(self, _db, qu, work_model, featured=False):
        """Filter out books whose bibliographic metadata doesn't match
        what we're looking for.
        """
        # Audience and language restrictions are common to all
        # WorkLists. (So are genre and collection restrictions, but those
        # were applied back in works().)
        qu = self.apply_audience_filter(_db, qu, work_model)
        if self.languages:
            qu = qu.filter(edition_model.language.in_(self.languages))
        if self.genre_ids:
            qu = qu.filter(mw.genre_id.in_(self.genre_ids))
        return self.apply_custom_filters(_db, qu, work_model, featured)

    def apply_custom_filters(self, _db, qu, work_model, featured=False):
        """Apply subclass-specific filters to a query in progress.

        :return: A 2-tuple (query, distinct). `distinct` controls whether
        the query should be made DISTINCT. We never want to show duplicate
        Works in a query, but adding DISTINCT slows things down, so you
        should only return it when it's reasonable that a book might show
        up more than once.
        """
        raise NotImplementedError()

    def apply_audience_filter(self, _db, qu, work_model):
        """Make sure that only Works classified under this lane's
        allowed audiences are returned.
        """
        if not self.audiences:
            return qu
        qu = qu.filter(work_model.audience.in_(self.audiences))
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
            qu = qu.filter(edition_model.data_source_id != gutenberg.id)
        return qu

    def only_show_ready_deliverable_works(
            self, query, work_model, show_suppressed=False
    ):
        """Restrict a query to show only presentation-ready works present in
        an appropriate collection which the default client can
        fulfill.

        Note that this assumes the query has an active join against
        LicensePool.
        """
        return self.library.restrict_to_ready_deliverable_works(
            query, work_model, show_suppressed=show_suppressed,
            collection_ids=self.collection_ids
        )

    def random_sample(self, query, target_size):
        """Find a random sample of items obtained from a query"""
        total_size = fast_query_count(query)

        if total_size > target_size:
            # We have enough results to randomly offset the selection.
            offset = random.randint(0, total_size-target_size)

        items = query.offset(offset).limit(target_size).all()
        random.shuffle(items)
        return works

    def _lazy_load(self, qu, work_model):
        """Avoid eager loading of objects that are contained in the 
        materialized view.
        """
        qu = qu.options(
            lazyload(work_model.license_pool, LicensePool.data_source),
            lazyload(work_model.license_pool, LicensePool.identifier),
            lazyload(work_model.license_pool, LicensePool.presentation_edition),
        )

    def _defer_unused_fields(self, query, work_model):
        """Some applications use the simple OPDS entry and some
        applications use the verbose. Whichever one we don't need,
        we can stop from even being sent over from the
        database.
        """
        if Configuration.DEFAULT_OPDS_FORMAT == "simple_opds_entry":
            return query.options(defer(work_model.verbose_opds_entry))
        else:
            return query.options(defer(work_model.simple_opds_entry))


class Lane(Base, WorkList):
    """A WorkList that draws its search criteria from a row in a
    database table.

    A Lane corresponds roughly to a section in a branch library or
    bookstore. Lanes are the primary means by which patrons discover
    books.
    """

    MAX_CACHE_AGE = 20*60      # 20 minutes

    # If a Lane has fewer than 5 titles, don't even bother showing it
    # in its parent's grouped feed.
    MINIMUM_SAMPLE_SIZE = 5

    __tablename__ = 'lanes'
    id = Column(Integer, primary_key=True)
    library_id = Column(Integer, ForeignKey('libraries.id'), index=True,
                        nullable=False)
    parent_id = Column(Integer, ForeignKey('lanes.id'), index=True,
                       nullable=True)

    # A lane may have one parent lane and many sublanes.
    parent = relationship("Lane", foreign_keys=parent_id, backref="sublanes")

    # A lane may have multiple associated LaneGenres. For most lanes,
    # this is how the contents of the lanes are defined.
    lane_genres = relationship(
        "LaneGenre", foreign_keys="lane_id", backref="lane"
    )

    # identifier is a name for this lane that is unique across the
    # library.  "Adult Fiction" is a good example. We can't have an
    # adult fiction lane called "Fiction" and a YA fiction lane called
    # "Fiction".
    identifier = Column(Unicode)

    # display_name is the name of the lane as shown to patrons.  It's
    # okay for this to be duplicated across the library, but it's not
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
    audiences = Column(ARRAY(Unicode))

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
    list_datasource_id = Column(
        Integer, ForeignKey('datasources.id'), index=True,
        nullable=True
    )

    # Only the books on these specific CustomLists will be shown.
    customlists = relationship(
        "CustomList", secondary=lambda: lanes_customlists,
        backref="lanes"
    )

    # This has no effect unless list_datasource_id or
    # list_identifier_id is also set. If this is set, then a book will
    # only be shown if it was seen on an appropriate list within this
    # number of days. If the number is zero, the book must be
    # _currently_ on an appropriate list.
    list_seen_in_previous_days = Column(Integer, nullable=True)

    # If this is set to True, then a book will show up in a lane only
    # if it would _also_ show up in its parent lane.
    #
    # Currently this has no effect unless list_datasource_id or
    # list_identifier_id is also set.
    inherit_parent_restrictions = Column(Boolean, default=False, nullable=False)

    # Patrons whose external type is in this list will be sent to this
    # lane when they ask for the root lane.
    #
    # This is almost never necessary.
    root_for_patron_type = Column(ARRAY(Unicode), nullable=True)

    # Only a visible lane will show up in the user interface.  The
    # admin interface can see all the lanes.
    visible = Column(Boolean, default=True, nullable=False)

    __table_args__ = (
        UniqueConstraint('library_id', 'identifier'),
        UniqueConstraint('parent_id', 'display_name'),
    )

    @property
    def library(self):
        _db = Session.object_session(self)
        return Library.by_id(_db, self.library_id)

    @property
    def visible_children(self):
        for lane in self.sublanes:
            if lane.visible:
                yield lane
    
    @property
    def url_name(self):
        """Return the name of this lane to be used in URLs.

        Basically, forward slash is changed to "__". This is necessary
        because Flask tries to route "feed/Suspense%2FThriller" to
        feed/Suspense/Thriller.
        """
        return self.identifier.replace("/", "__")

    @property
    def audiences(self):
        return self._audiences

    @audiences.setter
    def set_audiences(self, value):
        """The `audiences` field cannot be set to a value that
        contradicts the current value to the `target_age` field.
        """
        if self._audiences and self._target_age and value != self._audiences:
            raise ValueError("Cannot modify Lane.audiences when Lane.target_age is set!")
        self._audiences = value

    @property
    def target_age(self):
        return self._target_age

    @target_age.setter
    def set_target_age(self, value):
        """Setting .target_age will lock .audiences to appropriate values.

        If you set target_age to 16-18, you're saying that the audiences
        are [Young Adult, Adult].

        If you set target_age 12-15, you're saying that the audiences are
        [Adult, Children].

        If you set target age 0-2, you're saying that the audiences are
        [Children].

        In no case is the "Adults Only" audience allowed, since target
        age only makes sense in lanes intended for minors.
        """
        if not audiences:
            if self.parent:
                audiences = self.parent.audiences
            else:
                audiences = []
        if isinstance(audiences, basestring):
            audiences = [audiences]
        if isinstance(audiences, set):
            audiences = audiences
        else:
            audiences = set(audiences)
        if not age_range:
            return audiences

        if not isinstance(age_range, list):
            age_range = [age_range]

        if age_range[-1] >= 18:
            audiences.add(Classifier.AUDIENCE_ADULT)
        if age_range[0] < Classifier.YOUNG_ADULT_AGE_CUTOFF:
            audiences.add(Classifier.AUDIENCE_CHILDREN)
        if age_range[0] >= Classifier.YOUNG_ADULT_AGE_CUTOFF:
            audiences.add(Classifier.AUDIENCE_YOUNG_ADULT)
        self._target_age = age_range
        self._audiences = audiences

    # TODO: Setting list_data_source should clear custom_lists.

    @property
    def custom_lists(self):
        """Find the specific CustomLists that control which works
        are in this Lane.
        """
        if not self.custom_lists and not self.list_data_source:
            # This isn't that kind of lane.
            return None

        if self.custom_lists:
            # This Lane draws from a specific set of lists.
            return [self.custom_lists]

        # This lane draws from every list associated with a certain
        # data source.
        _db = Session.object_session(self)
        return _db.query(CustomList).filter(
            CustomList.data_source==self.list_data_source
        )

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
            bucket.add(genre.id)
            if lanegenre.recursive:
                for subgenre in genre.subgenres:
                    bucket.add(subgenre.id)
        genre_ids = included_ids - excluded_ids
        if not genre_ids:
            logging.error(
                "Lane %s has a self-negating set of genre IDs.", self.identifier
            )
        return genre_ids

    @property
    def search_target(self):
        """When someone in this lane wants to do a search, determine which
        Lane should actually be searched.
        """
        if self.parent is None:
            # We are at the top level. Search everything.
            return self

        if self.root_for_patron_type:
            # This lane acts as the "top level" for one or more patron
            # types.  Search it, even if the active patron is not of
            # that type. (This avoids panic reactions when an admin
            # searches the 'Early Grades' lane and finds books from
            # other lanes.)
            return self

        if not self.genres and not self.list_ids:
            # This lane is not restricted to any particular genres or
            # lists. It can be searched and any lane below it should
            # search this one.
            return self

        # Any other lane cannot be searched directly, but maybe its
        # parent can be searched.
        logging.debug(
            "Lane %s is not searchable; using parent %s" % (
                self.name, self.parent.name)
        )
        return self.parent.search_target

    def search(self, _db, query, search_client, pagination=None):
        """Find works in this lane that also match a search query.
        """        
           
        if not pagination:
            pagination = Pagination(
                offset=0, size=Pagination.DEFAULT_SEARCH_SIZE
            )

        search_lane = self.search_target
        if not search_lane:
            # This lane is not searchable, and neither are any of its
            # parents. There are no search results.
            return []

        if search_lane.fiction in (True, False):
            fiction = search_lane.fiction
        else:
            fiction = None

        # Get the search results from Elasticsearch.
        results = None
        if search_client:
            docs = None
            a = time.time()
            try:
                docs = search_client.query_works(
                    query, search_lane.media, search_lane.languages,
                    fiction, list(search_lane.audiences), 
                    search_lane.target_age,
                    search_lane.genre_ids,
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
                    results = WorkList.works_for_specific_ids(_db, doc_ids)

        return results

    def featured_works(self, _db):
        """Find a random sample of featured books.

        While it's semi-okay for this request to be slow for the
        genre-based lanes that make up the bulk of the site, subclass
        implementations such as LicensePoolBasedLane may require
        improved performance.

        :return: A list of MaterializedWork or MaterializedWorkWithGenre
        objects.
        """
        books = []
        book_ids = set()
        featured_subquery = None
        target_size = self.library.featured_lane_size

        # Prefer to feature available books in the featured
        # collection, but if that fails, gradually degrade to
        # featuring all books, no matter what the availability.

        # TODO: knowing whether the lane is list-based would be useful
        # here; we could try or avoid some variants based on toggling
        # featured_on_list.
        for (collection, availability, featured_on_list) in (
                (Facets.COLLECTION_FEATURED, Facets.AVAILABLE_NOW, True),
                (Facets.COLLECTION_FEATURED, Facets.AVAILABLE_ALL, True),
                (Facets.COLLECTION_MAIN, Facets.AVAILABLE_NOW, False),
                (Facets.COLLECTION_MAIN, Facets.AVAILABLE_ALL, False),
                (Facets.COLLECTION_FULL, Facets.AVAILABLE_ALL, False),
        ):
            facets = Facets(
                self.library, collection=collection, availability=availability,
                order=Facets.ORDER_RANDOM
            )
            query = self.works(facets=facets, featured=featured_on_list)
            if not query:
                # works() may return None, indicating that the whole
                # thing is a bad idea and the query should not even be
                # run. It will probably think the whole thing is a bad
                # idea no matter which arguments we pass in, but let's
                # keep trying.
                continue

            # Get a new list of books that meet our (possibly newly 
            # reduced) standards.
            new_books = self.random_sample(query, target_size)
            for book in new_books:
                if book.id not in book_ids:
                    books.append(book)
                    book_ids.add(book)
            if len(books) >= target_size:
                # We found enough books.
                break
        return books[:target_size]

    def apply_custom_filters(self, _db, qu, work_model, featured=False):
        """Apply filters to a base query against a materialized view,
        yielding a query that only finds books in this Lane.

        :param work_model: Either MaterializedWork or MaterializedWorkWithGenre
        """
        parent_distinct = False
        if self.parent and self.inherit_parent_restrictions:
            # In addition to the other restrictions imposed by this
            # Lane, books will show up here only if they would
            # also show up in the parent Lane.
            qu, parent_distinct = self.parent.apply_bibliographic_filters(
                _db, qu, work_model, featured
            )

        # If a license source is specified, only show books from that
        # source.
        if self.license_source:
            qu = qu.filter(LicensePool.data_source==self.license_source)

        if self.fiction == self.UNCLASSIFIED:
            qu = qu.filter(work_model.fiction==None)
        elif self.fiction != self.BOTH_FICTION_AND_NONFICTION:
            qu = qu.filter(work_model.fiction==self.fiction)

        if self.media:
            qu = qu.filter(edition_model.medium.in_(self.media))

        qu = self.apply_age_range_filter(_db, qu, work_model)
        qu, child_distinct = self.apply_customlist_filter(
            qu, work_model, featured
        )
        return qu, (parent_distinct or child_distinct)

    def apply_age_range_filter(self, _db, qu, work_model):
        """Filter out all books that are not classified as suitable for this
        Lane's age range.
        """
        if self.target_age == None:
            return qu
            
        if (Classifier.AUDIENCE_ADULT in self.audiences
            or Classifier.AUDIENCE_ADULTS_ONLY in self.audiences):
            # Books for adults don't have target ages. If we're including
            # books for adults, allow the target age to be empty.
            audience_has_no_target_age = work_model.target_age == None
        else:
            audience_has_no_target_age = False

        if len(self.target_age) == 1:
            # The target age must include this number.
            r = NumericRange(self.target_age[0], self.target_age[0], '[]')
            qu = qu.filter(
                or_(
                    work_model.target_age.contains(r),
                    audience_has_no_target_age
                )
            )
        else:
            # The target age range must overlap this age range
            r = NumericRange(self.target_age[0], self.target_age[-1], '[]')
            qu = qu.filter(
                or_(
                    work_model.target_age.overlaps(r),
                    audience_has_no_target_age
                )
            )

    def apply_customlist_filter(
            self, qu, work_model, must_be_featured=False
    ):
        """Change the given query so that it finds only books that are
        on one of the CustomLists allowed by Lane configuration.

        :param must_be_featured: It's not enough for the book to be on
        an appropriate list; it must be _featured_ on an appropriate list.
        """
        if not self.custom_lists and not self.list_data_source:
            # This lane does not require that books be on any particular
            # CustomList.
            return qu, False

        # There may already be a join against CustomListEntry, in the case 
        # of a Lane that inherits its parent's restrictions. To avoid
        # confusion, create a different join every time.
        a_entry = aliased(CustomListEntry)
        if work_model == Work:
            clause = CustomListEntry.work_id==work_model.id
        else:
            clause = CustomListEntry.work_id==work_model.works_id
        qu = qu.join(a_entry, clause)
        a_list = aliased(CustomListEntry.customlist)
        qu = qu.join(a_entry).join(a_list)

        # Actually apply the restriction.
        if self.list_data_source:
            qu = qu.filter(a_list.data_source==self.list_data_source)
        if self.custom_lists:
            qu = qu.filter(a_list.id.in_([x.id for x in self.custom_lists]))
        if must_be_featured:
            qu = qu.filter(a_entry.featured==True)
        if self.list_seen_in_previous_days:
            cutoff = datetime.datetime.utcnow() - datetime.timedelta(
                self.list_seen_in_previous_days
            )
            qu = qu.filter(a_entry.most_recent_appearance >=cutoff)
            
        # Now that a custom list is involved, we must eventually set
        # DISTINCT to True on the query.
        return qu, True

Library.lanes = relationship("Lane", backref="library")


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

Genre.lane_genres = relationship(
    "LaneGenre", foreign_keys="genre_id", backref="genre"
)


lanes_customlists = Table(
    'lanes_customlists', Base.metadata,
    Column(
        'lane_id', Integer, ForeignKey('lanes.id'),
        index=True, nullable=False
    ),
    Column(
        'customlist_id', Integer, ForeignKey('identifiers.id'),
        index=True, nullable=False
    ),
    UniqueConstraint('lane_id', 'customlist_id'),
)



class QueryGeneratedLane(WorkList):
    """A WorkList that takes its list of books from a database query
    rather than a Lane object.
    """

    MAX_CACHE_AGE = 14*24*60*60      # two weeks

    # When generating groups feeds, we want to return a sample
    # even if there's only a single result.
    MINIMUM_SAMPLE_SIZE = 1

    def query_hook(self, qu, work_model=Work):
        """Create the query specific to a subclass of  QueryGeneratedLane

        :return: query or None
        """
        raise NotImplementedError()
