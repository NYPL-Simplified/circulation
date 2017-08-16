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
)
from sqlalchemy.orm import (
    contains_eager,
    defer,
    joinedload,
    lazyload,
)

from model import (
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

    @classmethod
    def database_field_to_order_facet(cls, database_field):
        """The inverse of order_facet_to_database_field.

        TODO: This method may not be necessary.
        """
        from model import (
            MaterializedWork as mw,
            MaterializedWorkWithGenre as mwg,
        )

        if database_field in (Edition.sort_title, mw.sort_title, 
                              mwg.sort_title):
            return cls.ORDER_TITLE

        if database_field in (Edition.sort_author, mw.sort_author,
                              mwg.sort_author):
            return cls.ORDER_AUTHOR

        if database_field in (Work.last_update_time, mw.last_update_time, 
                              mwg.last_update_time):
            return cls.ORDER_LAST_UPDATE

        if database_field in (Edition.series_position, mw.series_position):
            return cls.ORDER_SERIES_POSITION

        if database_field in (Work.id, mw.works_id, mwg.works_id):
            return cls.ORDER_WORK_ID

        if database_field in (Work.random, mw.random, mwg.random):
            return cls.ORDER_RANDOM

        return None

    def apply(self, _db, q, work_model=Work, edition_model=Edition,
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
        q = q.filter(availability_clause)

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
            q = q.filter(or_clause)
        elif self.collection == self.COLLECTION_FEATURED:
            # Exclude books with a quality of less than the library's
            # minimum featured quality.
            q = q.filter(
                work_model.quality >= self.library.minimum_featured_quality
            )

        # Set the ORDER BY clause.
        order_by, order_distinct = self.order_by(
            work_model, edition_model
        )
        q = q.order_by(*order_by)
        if distinct:
            q = q.distinct(*order_distinct)

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
        return q.offset(self.offset).limit(self.size)


class UndefinedLane(Exception):
    """Cannot create a lane because its definition is contradictory
    or incomplete.
    """


class Lane(object):

    """A set of books that would go together in a display."""

    UNCLASSIFIED = u"unclassified"
    BOTH_FICTION_AND_NONFICTION = u"both"
    FICTION_DEFAULT_FOR_GENRE = u"fiction default for genre"

    # A book is considered a 'best seller' if it's been seen on
    # the best seller list sometime in the past two years.
    BEST_SELLER_LIST_DURATION = 730

    # Books classified in a subgenre of this lane's genre(s) will
    # be shown in separate lanes.
    IN_SUBLANES = u"separate"

    # Books classified in a subgenre of this lane's genre(s) will be
    # shown in this lane.
    IN_SAME_LANE = u"collapse"

    AUDIENCE_ADULT = Classifier.AUDIENCE_ADULT
    AUDIENCE_ADULTS_ONLY = Classifier.AUDIENCE_ADULTS_ONLY
    AUDIENCE_YOUNG_ADULT = Classifier.AUDIENCE_YOUNG_ADULT
    AUDIENCE_CHILDREN = Classifier.AUDIENCE_CHILDREN

    MINIMUM_SAMPLE_SIZE = None

    @property
    def library(self):
        return Library.by_id(self._db, self.library_id)
    
    @property
    def url_name(self):
        """Return the name of this lane to be used in URLs.

        Basically, forward slash is changed to "__". This is necessary
        because Flask tries to route "feed/Suspense%2FThriller" to
        feed/Suspense/Thriller.
        """
        return self.name.replace("/", "__")

    @property
    def language_key(self):
        """Return a string identifying the languages used in this lane.

        This will usually be in the form of 'eng,spa' (English and Spanish)
        or '!eng,!spa' (everything except English and Spanish)
        """
        key = ""
        if self.languages:
            key += ",".join(self.languages)
        if self.exclude_languages:
            key = ",".join("!" + l for l in self.exclude_languages)
        return key

    @property
    def depth(self):
        """How deep is this lane in this site's hierarchy?

        i.e. how many times do we have to follow .parent before we get None?
        """
        depth = 0
        tmp = self
        while tmp.parent:
            depth += 1
            tmp = tmp.parent
        return depth
    
    def __repr__(self):
        template = (
            "<Lane name=%(full_name)s, display=%(display_name)s, "
            "media=%(media)s, genre=%(genres)s, fiction=%(fiction)s, "
            "audience=%(audiences)s, age_range=%(age_range)r, "
            "language=%(language)s, sublanes=%(sublanes)d>")

        sublanes = getattr(self, 'sublanes', None)
        if sublanes:
            sublanes = sublanes.lanes
        else:
            sublanes = []

        vars = dict(
            full_name=self.name or "",
            display_name=self.display_name or "",
            genres = "+".join(self.genre_names or ["all"]),
            fiction=self.fiction,
            media=", ".join(self.media or ["all"]),
            audiences = "+".join(self.audiences or ["all"]),
            age_range = self.age_range or "all",
            language=self.language_key or "all",
            sublanes = len(sublanes)
        )

        if self.exclude_languages:
            exclude_language = "-(%s)" % "+".join(self.exclude_languages)

        output = template % vars
        return output.encode("ascii", "replace")

    def debug(self, level=0):
        """Output information about the lane layout."""
        print "%s%r" % ("-" * level, self)
        for lane in self.sublanes.lanes:
            lane.debug(level+1)

    def __init__(self, 
                 _db,
                 library,
                 full_name,
                 display_name=None,

                 parent=None,
                 sublanes=[],
                 include_best_sellers=False,
                 include_staff_picks=False,
                 include_all=True,

                 genres=[],
                 exclude_genres=None,
                 subgenre_behavior=None,

                 fiction=None,

                 audiences=None,
                 age_range=None,

                 appeals=None,

                 languages=None,
                 exclude_languages=None,
                 media=Edition.BOOK_MEDIUM,
                 formats=Edition.ELECTRONIC_FORMAT,

                 license_source=None,

                 list_data_source=None,
                 list_identifier=None,
                 list_seen_in_previous_days=None,

                 searchable=False,
                 invisible=False,
                 ):
        if not isinstance(library, Library):
            raise ValueError("Expected Library in Lane constructor, got %r" % library)
        self.name = full_name
        self.display_name = display_name or self.name
        self.parent = parent
        self._db = _db
        self.library_id = library.id
        self.collection_ids = [
            collection.id for collection in library.all_collections
        ]
        self.default_for_language = False
        self.searchable = searchable
        self.invisible = invisible
        self.license_source = license_source
        
        self.log = logging.getLogger("Lane %s" % self.name)

        # This controls which feeds to display when showing this lane
        # and its sublanes as a group.
        #
        # This is not a sublane--it's a group that's shown as part of the main
        # lane.
        #
        # e.g. "All Science Fiction"
        self.include_all_feed = include_all

        if isinstance(age_range, int):
            age_range = [age_range]
        if age_range is not None:
            age_range = sorted(age_range)
        self.set_from_parent('age_range', age_range)

        self.audiences = self.audience_list_for_age_range(audiences, age_range)


        def set_list(field_name, value, default=None):
            if isinstance(value, basestring):
                value = [value]
            self.set_from_parent(field_name, value, default)

        set_list('languages', languages)
        set_list('exclude_languages', exclude_languages)
        set_list('appeals', appeals)

        # The lane may be restricted to items in particular media
        # and/or formats.
        set_list('media', media, Edition.BOOK_MEDIUM)
        set_list('formats', formats)

        self.set_from_parent(
            'subgenre_behavior', subgenre_behavior, self.IN_SUBLANES)

        self.set_sublanes(
            self._db, sublanes, genres,
            exclude_genres=exclude_genres, fiction=fiction
        )

        if list_data_source or list_identifier:
            # CustomList information must be set after sublanes.
            # Otherwise, sublanes won't be restricted by the list.
            self.set_customlist_information(
                list_data_source, list_identifier, list_seen_in_previous_days
            )
        else:
            self.list_data_source_id = None
            self.list_ids = list()
            self.list_featured_works_query = None

        # Best-seller and staff pick lanes go at the top.
        base_args = dict(
            library=self.library, parent=self, include_all=False, genres=genres,
            exclude_genres=exclude_genres, fiction=fiction, 
            audiences=audiences, age_range=age_range,
            appeals=appeals, languages=languages, 
            exclude_languages=exclude_languages, media=media, 
            formats=formats
        )
        if include_staff_picks:
            self.include_staff_picks(**base_args)
        if include_best_sellers:
            self.include_best_sellers(**base_args)

        # Run some sanity checks.
        ch = Classifier.AUDIENCE_CHILDREN
        ya = Classifier.AUDIENCE_YOUNG_ADULT
        if ((include_best_sellers or include_staff_picks) and
            (self.list_data_source_id or self.list_ids)):
            raise UndefinedLane(
                "Cannot include best-seller or staff-picks in a lane "
                "based on lists."
            )

        if (self.age_range and
            not any(x in self.audiences for x in [ch, ya])):
            raise UndefinedLane(
                "Lane %s specifies age range but does not contain "
                "children's or young adult books." % self.name
            )

    def set_from_parent(self, field_name, value, default=None):
        if value is None:
            if self.parent:
                value = getattr(self.parent, field_name, default)
            else:
                value = default
        setattr(self, field_name, value)

    def set_customlist_information(self, list_data_source, list_identifier,
                                   list_seen_in_previous_days):
        """Sets any attributes relevant to lanes created from CustomLists"""

        # The lane may be restricted to books that are on a list
        # from a given data source.
        custom_list_details = self.custom_lists_for_identifier(
            list_data_source, list_identifier)
        (self.list_data_source_id,
         self.list_ids,
         self.list_featured_works_query) = custom_list_details

        # Or its parent may be restricted, in which case it should be,
        # too.
        if not self.list_data_source_id:
            self.set_from_parent('list_data_source_id', None)
        if not self.list_ids:
            self.set_from_parent('list_ids', None)
        if not self.list_featured_works_query:
            # The parent may have featured works from the list that
            # could be included here.
            self.set_from_parent('list_featured_works_query', None)

        for sublane in self.sublanes:
            # If the sublanes were set beforehand, they need to
            # inherit this information now.
            #
            # TODO: Find a different way to combine list restrictions
            # from parent lanes. This is a placeholder.
            sublane.set_customlist_information(None, None, None)

        self.set_from_parent(
            'list_seen_in_previous_days', list_seen_in_previous_days)

    def custom_lists_for_identifier(self, list_data_source, list_identifier):
        """Turn a data source and an identifier into a specific list
        of CustomLists.
        """
        if isinstance(list_data_source, basestring):
            list_data_source = DataSource.lookup(self._db, list_data_source)
        # The lane may be restricted to books that are on one or
        # more specific lists.
        if not list_identifier:
            lists = None
        elif isinstance(list_identifier, CustomList):
            lists = [list_identifier]
        elif (isinstance(list_identifier, list) and
              isinstance(list_identifier[0], CustomList)):
            lists = list_identifier
        else:
            if isinstance(list_identifier, basestring):
                list_identifiers = [list_identifier]
            q = self._db.query(CustomList).filter(
                CustomList.foreign_identifier.in_(list_identifiers))
            if list_data_source:
                q = q.filter(CustomList.data_source==list_data_source)
            lists = q.all()
            if not lists:
                raise UndefinedLane(
                    "Could not find any matching lists: %s, %r" %
                    (list_data_source, list_identifiers)
                )
        if list_data_source:
            list_data_source_id = list_data_source.id
        else:
            list_data_source_id = None

        list_featured_works_query = None
        if lists:
            list_ids = [x.id for x in lists]
            list_featured_works_query = self.extract_list_featured_works_query(lists)
        else:
            list_ids = None
        return list_data_source_id, list_ids, list_featured_works_query

    def extract_list_featured_works_query(self, lists):
        if not lists:
            return None
        if isinstance(lists[0], int):
            # We have CustomList ids instead of CustomList objects.
            lists = self._db.query(CustomList).filter(CustomList.id.in_(lists))
            lists = lists.all()

        lists = [custom_list for custom_list in lists if custom_list.featured_works]
        if not lists:
            return None

        work_ids = list()
        for custom_list in lists:
            work_ids += [work.id for work in custom_list.featured_works]

        works_query = self._db.query(Work).with_labels().\
            filter(Work.id.in_(work_ids))
        return works_query

    def set_sublanes(self, _db, sublanes, genres,
                     exclude_genres=None, fiction=None):
        """Transforms a list of genres or sublanes into a LaneList and sets
        that LaneList as the value of self.sublanes
        """
        # However the genres came in, turn them into database Genre
        # objects and the corresponding GenreData objects.
        genres = self.load_genres(self._db, genres)[0]

        # Create a complete list of genres to exclude.
        full_exclude_genres = set()
        if exclude_genres:
            # TODO: automatically extract exclude_genres from parent lanes.
            for genre in exclude_genres:
                genre, ignore = self.load_genre(self._db, genre)
                for l in genre.self_and_subgenres:
                    full_exclude_genres.add(l)

        if fiction is None:
            fiction = self.FICTION_DEFAULT_FOR_GENRE

        # Find all the genres that will go into this lane.
        genres, self.fiction = self.gather_matching_genres(
            _db, genres, fiction, full_exclude_genres
        )
        self.genre_ids = [x.id for x in genres]
        self.genre_names = [x.name for x in genres]

        if sublanes and not isinstance(sublanes, list):
            sublanes = [sublanes]
        subgenre_sublanes = []
        if self.subgenre_behavior == self.IN_SUBLANES:
            # All subgenres of the given genres that are not in
            # full_exclude_genres must get a constructed sublane.
            for genre in genres:
                for subgenre in genre.subgenres:
                    if subgenre in full_exclude_genres:
                        continue
                    sublane = Lane(
                            self._db, self.library, full_name=subgenre.name,
                            parent=self, genres=[subgenre],
                            subgenre_behavior=self.IN_SUBLANES
                    )
                    subgenre_sublanes.append(sublane)

        if sublanes and subgenre_sublanes:
            raise UndefinedLane(
                "Explicit list of sublanes was provided, but I'm also asked "\
                "to turn %s subgenres into sublanes!" % len(subgenre_sublanes)
            )

        if subgenre_sublanes:
            self.sublanes = LaneList(self)
            for sl in subgenre_sublanes:
                self.sublanes.add(sl)
        elif sublanes:
            self.sublanes = LaneList.from_description(
                self._db, self.library, self, sublanes
            )
        else:
            self.sublanes = LaneList.from_description(self._db, self.library, self, [])

    def include_staff_picks(self, **base_args):
        """Includes a Staff Picks sublane to the base/top of this lane."""

        full_name = "%s - Staff Picks" % self.name
        try:
            staff_picks_lane = Lane(
                self._db,
                full_name=full_name, display_name="Staff Picks",
                list_identifier="Staff Picks",
                searchable=False,
                **base_args
            )
        except UndefinedLane, e:
            # Not a big deal, just don't add the lane.
            staff_picks_lane = None
        if staff_picks_lane:
            self.sublanes.lanes.insert(0, staff_picks_lane)

    def include_best_sellers(self, **base_args):
        """Includes a NYT Best Sellers sublane to the base/top of this lane."""

        full_name = "%s - Best Sellers" % self.name
        try:
            best_seller_lane = Lane(
                self._db,
                full_name=full_name, display_name="Best Sellers",
                list_data_source=DataSource.NYT,
                list_seen_in_previous_days=365*2,
                searchable=False,
                **base_args
            )
        except UndefinedLane, e:
            # Not a big deal, just don't add the lane.
            best_seller_lane = None
        if best_seller_lane:
            self.sublanes.lanes.insert(0, best_seller_lane)

    def includes_language(self, language):
        """Would you expect to find books in the given language in
        this lane?
        """
        if self.exclude_languages:
            # We include all language except the ones on the exclude list.
            if language in self.exclude_languages:
                return False
            else:
                return True
        if self.languages and language not in self.languages:
            # We only include languages on the include list.
            return False
        # We include all languages.
        return True        

    def audience_list_for_age_range(self, audiences, age_range):
        """Normalize a value for Work.audience based on .age_range

        If you set audience to Young Adult but age_range to 16-18,
        you're saying that books for 18-year-olds (i.e. adults) are
        okay.

        If you set age_range to Young Adult but age_range to 12-15, you're
        saying that books for 12-year-olds (i.e. children) are
        okay.
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
        return audiences      

    @classmethod
    def from_description(cls, _db, library, parent, description):
        genre = None
        if not isinstance(library, Library):
            raise ValueError("Expected library, got %r" % library)
        if isinstance(description, Lane):
            # The lane has already been created.
            description.parent = parent
            return description
        elif isinstance(description, dict):
            if description.get('suppress_lane'):
                return None
            # Invoke the constructor
            return Lane(_db, library, parent=parent, **description)
        else:
            # This is a lane for a specific genre.
            genre, genredata = Lane.load_genre(_db, description)
            return Lane(_db, library, genre.name, parent=parent, genres=genre)

    @classmethod
    def load_genre(cls, _db, descriptor):
        """Turn some kind of genre descriptor into a (Genre, GenreData) 
        2-tuple.

        The descriptor might be a 2-tuple, a 3-tuple, a Genre object
        or a GenreData object.
        """
        if isinstance(descriptor, tuple):
            if len(descriptor) == 2:
                genre, subgenres = descriptor
            else:
                genre, subgenres, audience_restriction = descriptor
        else:
            genre = descriptor

        if isinstance(genre, GenreData):
            genredata = genre
        else:
            if isinstance(genre, Genre):
                genre_name = genre.name
            else:
                genre_name = genre
            # It's in the database--just make sure it's not an old entry
            # that shouldn't be in the database anymore.
            genredata = classifier.genres.get(genre_name)

        if not isinstance(genre, Genre):
            genre, ignore = Genre.lookup(_db, genre)
        return genre, genredata

    @classmethod
    def load_genres(cls, _db, genres):
        """Turn a list of genre-like things into a list of Genre objects
        and a list of GenreData objects.
        """
        genre_obj = []
        genre_data = []
        if genres is None:
            return genre_obj, genre_data
        if not any(isinstance(genres, x) for x in (list, tuple, set)):
            genres = [genres]
        for name in genres:
            genre, data = cls.load_genre(_db, name)
            genre_obj.append(genre)
            genre_data.append(data)
        return genre_obj, genre_data

    @classmethod
    def all_matching_genres(cls, _db, genres, exclude_genres=None, fiction=None):
        matches = set()
        exclude_genres = exclude_genres or []
        if exclude_genres:
            if not isinstance(exclude_genres, list):
                exclude_genres = list(exclude_genres)

            for excluded_genre in exclude_genres[:]:
                exclude_genres += excluded_genre.self_and_subgenres

            if not genres:
                # This is intended for lanes like "General Fiction"
                # when we want everything EXCEPT for some given lanes.
                genres = _db.query(Genre).all()
                if fiction is not None:
                    # Filter the genres according to their expected
                    # fiction status.
                    genres = filter(lambda g: g.default_fiction==fiction, genres)

        if genres:
            for genre in genres:
                matches = matches.union(genre.self_and_subgenres)
        return [x for x in matches if x not in exclude_genres]

    @classmethod
    def gather_matching_genres(cls, _db, genres, fiction, exclude_genres=[]):
        """Find all subgenres of the given genres which match the given fiction
        status.
        
        This may also turn into an additional restriction (or
        liberation) on the fiction status.

        It may also result in the need to create more sublanes.
        """
        fiction_default_by_genre = (fiction == cls.FICTION_DEFAULT_FOR_GENRE)

        if fiction_default_by_genre:
            # Unset `fiction`. We'll set it again when we find out
            # whether we've got fiction or nonfiction genres.
            fiction = None
        genres = cls.all_matching_genres(_db, genres, exclude_genres, fiction=fiction)
        for genre in genres:
            if fiction_default_by_genre:
                if fiction is None:
                    fiction = genre.default_fiction
                elif fiction != genre.default_fiction:
                    raise UndefinedLane(
                        "I was told to use the default fiction restriction, but the genres %s include contradictory fiction restrictions." % ", ".join([x.name for x in genres])
                    )
            else:
                if fiction is not None and fiction != genre.default_fiction:
                    # This is an impossible situation. Rather than
                    # eliminate all books from consideration, allow
                    # both fiction and nonfiction.
                    fiction = cls.BOTH_FICTION_AND_NONFICTION

        if fiction is None:
            fiction = cls.BOTH_FICTION_AND_NONFICTION
        return genres, fiction

    def works(self, facets=None, pagination=None):
        """Find Works that will go together in this Lane.

        Works will:

        * Be in one of the languages listed in `languages`,
          and not one of the languages listed in `exclude_languages`.

        * Be filed under of the genres listed in `self.genre_ids` (or, if
          `self.include_subgenres` is True, any of those genres'
          subgenres).

        * Have the same appeal as `self.appeal`, if `self.appeal` is present.

        * Are intended for the audience in `self.audience`.

        * Are fiction (if `self.fiction` is True), or nonfiction (if fiction
          is false), or of the default fiction status for the genre
          (if fiction==FICTION_DEFAULT_FOR_GENRE and all genres have
          the same default fiction status). If fiction==None, no fiction
          restriction is applied.

        * Have a delivery mechanism that can be rendered by the
          default client.

        * Have an unsuppressed license pool that belongs to one of the
          available collections.
        """

        q = self._db.query(Work).join(Work.presentation_edition)
        q = q.join(Work.license_pools).enable_eagerloads(False).\
            join(LicensePool.data_source).\
            join(LicensePool.identifier)
        q = q.options(
            joinedload(Work.license_pools),
            contains_eager(Work.presentation_edition),
            contains_eager(Work.license_pools, LicensePool.data_source),
            contains_eager(Work.license_pools, LicensePool.presentation_edition),
            contains_eager(Work.license_pools, LicensePool.identifier),
            defer(Work.presentation_edition, Edition.extra),
            defer(Work.license_pools, LicensePool.presentation_edition, Edition.extra),
        )
        q = self._defer_unused_opds_entry(q)

        if self.genre_ids:
            q = q.join(Work.work_genres)
            q = q.options(contains_eager(Work.work_genres))
            q = q.filter(WorkGenre.genre_id.in_(self.genre_ids))

        q = self.apply_filters(
            q,
            facets=facets, pagination=pagination,
            work_model=Work, edition_model=Edition
        )
        if not q:
            # apply_filters may return None in subclasses of Lane
            return None
        return q

    def materialized_works(self, facets=None, pagination=None):
        """Find MaterializedWorks that will go together in this Lane."""
        from model import (
            MaterializedWork,
            MaterializedWorkWithGenre,
        )
        if self.genre_ids:
            mw =MaterializedWorkWithGenre
            q = self._db.query(mw)
            q = q.filter(mw.genre_id.in_(self.genre_ids))
        else:
            mw = MaterializedWork
            q = self._db.query(mw)

        # Avoid eager loading of objects that are contained in the 
        # materialized view.
        q = q.options(
            lazyload(mw.license_pool, LicensePool.data_source),
            lazyload(mw.license_pool, LicensePool.identifier),
            lazyload(mw.license_pool, LicensePool.presentation_edition),
        )
        q = self._defer_unused_opds_entry(q, work_model=mw)

        q = q.join(LicensePool, LicensePool.id==mw.license_pool_id)
        q = q.options(contains_eager(mw.license_pool))
        q = self.apply_filters(
                q,
                facets=facets, pagination=pagination,
                work_model=mw, edition_model=mw
            )
        if not q:
            # apply_filters may return None in subclasses of Lane
            return None
        return q

    def apply_filters(self, q, facets=None, pagination=None, work_model=Work, edition_model=Edition):
        """Apply filters to a base query against Work or a materialized view.

        :param work_model: Either Work, MaterializedWork, or MaterializedWorkWithGenre
        :param edition_model: Either Edition, MaterializedWork, or MaterializedWorkWithGenre
        """
        if self.languages:
            q = q.filter(edition_model.language.in_(self.languages))

        if self.exclude_languages:
            q = q.filter(not_(edition_model.language.in_(self.exclude_languages)))

        if self.audiences:
            q = q.filter(work_model.audience.in_(self.audiences))
            if (Classifier.AUDIENCE_CHILDREN in self.audiences
                or Classifier.AUDIENCE_YOUNG_ADULT in self.audiences):
                    gutenberg = DataSource.lookup(
                        self._db, DataSource.GUTENBERG)
                    # TODO: A huge hack to exclude Project Gutenberg
                    # books (which were deemed appropriate for
                    # pre-1923 children but are not necessarily so for
                    # 21st-century children.)
                    #
                    # This hack should be removed in favor of a
                    # whitelist system and some way of allowing adults
                    # to see books aimed at pre-1923 children.
                    q = q.filter(edition_model.data_source_id != gutenberg.id)

        if self.appeals:
            q = q.filter(work_model.primary_appeal.in_(self.appeals))

        # If a license source is specified, only show books from that
        # source.
        if self.license_source:
            q = q.filter(
                LicensePool.data_source==self.license_source
            )

        if self.age_range != None:
            if (Classifier.AUDIENCE_ADULT in self.audiences
                or Classifier.AUDIENCE_ADULTS_ONLY in self.audiences):
                # Books for adults don't have target ages. If we're including
                # books for adults, allow the target age to be empty.
                audience_has_no_target_age = work_model.target_age == None
            else:
                audience_has_no_target_age = False

            if len(self.age_range) == 1:
                # The target age must include this number.
                r = NumericRange(self.age_range[0], self.age_range[0], '[]')
                q = q.filter(
                    or_(
                        work_model.target_age.contains(r),
                        audience_has_no_target_age
                    )
                )
            else:
                # The target age range must overlap this age range
                r = NumericRange(self.age_range[0], self.age_range[-1], '[]')
                q = q.filter(
                    or_(
                        work_model.target_age.overlaps(r),
                        audience_has_no_target_age
                    )
                )

        if self.fiction == self.UNCLASSIFIED:
            q = q.filter(work_model.fiction==None)
        elif self.fiction != self.BOTH_FICTION_AND_NONFICTION:
            q = q.filter(work_model.fiction==self.fiction)

        if self.media:
            q = q.filter(edition_model.medium.in_(self.media))

        # TODO: Also filter on formats.

        q = self.only_show_ready_deliverable_works(q, work_model)

        distinct = False
        if self.list_data_source_id or self.list_ids:
            # One book can show up on more than one list; we need to
            # add a DISTINCT clause.
            distinct = True

            if work_model == Work:
                clause = CustomListEntry.work_id==work_model.id
            else:
                clause = CustomListEntry.work_id==work_model.works_id
            q = q.join(CustomListEntry, clause)
            if self.list_data_source_id:
                q = q.join(CustomListEntry.customlist).filter(
                    CustomList.data_source_id==self.list_data_source_id)
            else:
                q = q.filter(
                    CustomListEntry.list_id.in_(self.list_ids)
                )
            if self.list_seen_in_previous_days:
                cutoff = datetime.datetime.utcnow() - datetime.timedelta(
                    self.list_seen_in_previous_days
                )
                q = q.filter(CustomListEntry.most_recent_appearance
                             >=cutoff)

        if facets:
            q = facets.apply(self._db, q, work_model, edition_model,
                             distinct=distinct)
        if pagination:
            q = pagination.apply(q)

        return q

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

    @property
    def search_target(self):
        """When performing a search in this lane, determine which lane
        should actually be searched.
        """
        if self.searchable:
            # This lane is searchable.
            return self
        if self.parent is None:
            # We're at the top level and still no searchable
            # lane. Give up.
            return None
        logging.debug(
            "Lane %s is not searchable; using parent %s" % (
                self.name, self.parent.name)
        )
        return self.parent.search_target

    def search(self, query, search_client, pagination=None):
        """Find works in this lane that match a search query.
        """        
           
        if not pagination:
            pagination = Pagination(offset=0, size=Pagination.DEFAULT_SEARCH_SIZE)

        search_lane = self.search_target
        if not search_lane:
            # This lane is not searchable, and neither are any of its
            # parents.
            return []

        if search_lane.fiction in (True, False):
            fiction = search_lane.fiction
        else:
            fiction = None

        results = None
        if search_client:
            docs = None
            a = time.time()
            try:
                docs = search_client.query_works(
                    query, search_lane.media, search_lane.languages, search_lane.exclude_languages,
                    fiction, list(search_lane.audiences), search_lane.age_range,
                    search_lane.genre_ids,
                    fields=["_id", "title", "author", "license_pool_id"],
                    size=pagination.size,
                    offset=pagination.offset,
                )
            except elasticsearch.exceptions.ConnectionError, e:
                logging.error(
                    "Could not connect to Elasticsearch; falling back to database search."
                )
            b = time.time()
            logging.debug("Elasticsearch query completed in %.2fsec", b-a)
            results = []
            if docs:
                doc_ids = [
                    int(x['_id']) for x in docs['hits']['hits']
                ]
                if doc_ids:
                    from model import MaterializedWork as mw
                    q = self._db.query(mw).join(
                        LicensePool, mw.license_pool_id==LicensePool.id
                    ).filter(
                        mw.works_id.in_(doc_ids)
                    )
                    q = q.options(
                        lazyload(mw.license_pool, LicensePool.data_source),
                        lazyload(mw.license_pool, LicensePool.identifier),
                        lazyload(mw.license_pool, LicensePool.presentation_edition),
                    )
                    q = self.only_show_ready_deliverable_works(q, mw)
                    q = self._defer_unused_opds_entry(q, work_model=mw)
                    work_by_id = dict()
                    a = time.time()
                    works = q.all()
                    for mw in works:
                        work_by_id[mw.works_id] = mw
                    results = [work_by_id[x] for x in doc_ids if x in work_by_id]
                    b = time.time()
                    logging.debug(
                        "Obtained %d MaterializedWork objects in %.2fsec",
                        len(results), b-a
                    )

        if not results:
            logging.debug("No elasticsearch results, falling back to database query")
            results = self._search_database(query).limit(pagination.size).offset(pagination.offset).all()
        return results

    def _search_database(self, query):
        """Do a really awful database search for a book using ILIKE.

        This is useful if an app server has no external search
        interface defined, or if the search interface isn't working
        for some reason.
        """
        k = "%" + query + "%"
        q = self.works().filter(
            or_(Edition.title.ilike(k),
                Edition.author.ilike(k)))
        #q = q.order_by(Work.quality.desc())
        return q

    def _defer_unused_opds_entry(self, query, work_model=Work):
        """Defer the appropriate opds entry
        """
        if Configuration.DEFAULT_OPDS_FORMAT == "simple_opds_entry":
            return query.options(defer(work_model.verbose_opds_entry))
        else:
            return query.options(defer(work_model.simple_opds_entry))

    def sublane_samples(self, use_materialized_works=True):
        """Generates a list of samples from each sublane for a groups feed"""

        # This is a list rather than a dict because we want to
        # preserve the ordering of the lanes.
        works_and_lanes = []
        for sublane in self.visible_sublanes:
            works = sublane.featured_works(
                use_materialized_works=use_materialized_works
            )
            for work in works:
                works_and_lanes.append((work, sublane))
        return works_and_lanes

    def featured_works(self, use_materialized_works=True):
        """Find a random sample of featured books.

        While it's semi-okay for this request to be slow for default Lanes,
        subclass implementations such as LicensePoolBasedLane may require
        improved performance.

        :return: A list of MaterializedWork or MaterializedWorkWithGenre
        objects.
        """
        books = []
        featured_subquery = None
        target_size = self.library.featured_lane_size
        # If this lane (or its ancestors) is a CustomList, look for any
        # featured works that were set on the list itself.
        list_books, work_id_column = self.list_featured_works(
            use_materialized_works=use_materialized_works
        )
        if list_books:
            target_size = target_size - len(list_books)
            if target_size <= 0:
                # We've found all the books we need from the
                # human-generated selections on the CustomList.
                return list_books

        # Prefer to feature available books in the featured
        # collection, but if that fails, gradually degrade to
        # featuring all books, no matter what the availability.
        for (collection, availability) in (
                (Facets.COLLECTION_FEATURED, Facets.AVAILABLE_NOW),
                (Facets.COLLECTION_FEATURED, Facets.AVAILABLE_ALL),
                (Facets.COLLECTION_MAIN, Facets.AVAILABLE_NOW),
                (Facets.COLLECTION_MAIN, Facets.AVAILABLE_ALL),
                (Facets.COLLECTION_FULL, Facets.AVAILABLE_ALL),
        ):
            facets = Facets(
                self.library, collection=collection, availability=availability,
                order=Facets.ORDER_RANDOM
            )
            if use_materialized_works:
                query = self.materialized_works(facets=facets)
            else:
                query = self.works(facets=facets)
            if not query:
                # apply_filters may return None in subclasses of Lane
                continue

            if list_books:
                # Remove any already-featured books, set by the
                # CustomList(s), from the database results.
                list_book_ids = [getattr(w, work_id_column.key) for w in list_books]
                query = query.filter(work_id_column.notin_(list_book_ids))

            # This is the end of the line, so we're desperate
            # to fill the lane, even if it's a little short.
            use_min_size = (collection==Facets.COLLECTION_FULL and
                            availability==Facets.AVAILABLE_ALL)

            # Get a random sample of books to be featured.
            books += self.randomized_sample_works(
                query, target_size=target_size, use_min_size=use_min_size)
            if books:
                break

        if list_books and books:
            # Combine any books from the CustomList with those that were
            # randomly generated.
            return list_books+books
        return books

    def list_featured_works(self, target_size=None, use_materialized_works=True):
        """Returns the featured books for a lane descended from CustomList(s)"""
        books = list()
        work_id_column = None
        target_size = target_size or self.library.featured_lane_size

        if self.list_featured_works_query:
            subquery = self.list_featured_works_query.with_labels().subquery()

            if use_materialized_works:
                query = self.materialized_works()

                # Extract the MaterializedView model from the query
                [work_model] = query._entities[0].entities
                work_id_column = work_model.works_id
            else:
                query = self.works()
                work_id_column = Work.id

            query = query.join(subquery, work_id_column==subquery.c.works_id)
            books += self.randomized_sample_works(
                query, target_size=target_size, use_min_size=True
            )

        return books, work_id_column

    def randomized_sample_works(self, query, target_size=None, use_min_size=False):
        """Find a random sample of works for a feed"""
        offset = 0
        smallest_sample_size = target_size

        if use_min_size:
            smallest_sample_size = self.MINIMUM_SAMPLE_SIZE or (target_size-5)
        total_size = fast_query_count(query)

        if total_size < smallest_sample_size:
            # There aren't enough works here. Ignore the lane.
            return []
        if total_size > target_size:
            # We have enough results to randomly offset the selection.
            offset = random.randint(0, total_size-target_size)

        works = query.offset(offset).limit(target_size).all()
        random.shuffle(works)
        return works

    @property
    def visible_sublanes(self):
        visible_sublanes = []
        for sublane in self.sublanes:
            if not sublane.invisible:
                visible_sublanes.append(sublane)
            else:
                visible_sublanes += sublane.visible_sublanes
        return visible_sublanes

    def visible_parent(self):
        if self.parent == None:
            return None
        elif not self.parent.invisible:
            return self.parent
        else:
            return self.parent.visible_parent()

    def visible_ancestors(self):
        """Returns a list of visible ancestors in ascending order."""
        visible_parent = self.visible_parent()
        if visible_parent == None:
            return []
        else:
            return [visible_parent] + visible_parent.visible_ancestors()

    def has_visible_sublane(self):
        return len([lane for lane in self.sublanes if not lane.invisible]) > 0


class LaneList(object):
    """A list of lanes such as you might see in an OPDS feed."""

    log = logging.getLogger("Lane list")

    def __repr__(self):
        parent = ""
        if self.parent:
            parent = "parent=%s, " % self.parent.name

        return "<LaneList: %slanes=[%s]>" % (
            parent,
            ", ".join([repr(x) for x in self.lanes])
        )       

    @classmethod
    def from_description(cls, _db, library, parent_lane, description):
        lanes = LaneList(parent_lane)
        description = description or []
        for lane_description in description:
            lane = Lane.from_description(_db, library, parent_lane, lane_description)

            def _add_recursively(l):
                lanes.add(l)
                sublanes = l.sublanes.lanes
                for sl in sublanes:
                    _add_recursively(sl)
            if lane:
                _add_recursively(lane)

        return lanes

    def __init__(self, parent=None):
        self.parent = parent
        self.lanes = []
        self.by_languages = defaultdict(dict)

    def __len__(self):
        return len(self.lanes)

    def __iter__(self):
        return self.lanes.__iter__()

    def add(self, lane):
        """A given set of languages may have only one lane with a given name."""
        if lane.parent == self.parent:
            self.lanes.append(lane)

        this_language = self.by_languages[lane.language_key]
        if lane.name in this_language and this_language[lane.name] is not lane:
            raise ValueError(
                "Duplicate lane for language key %s: %s" % (
                    lane.language_key, lane.name
                )
            )
        this_language[lane.name] = lane


class QueryGeneratedLane(Lane):
    """A lane dependent on a particular query, instead of a genre or search"""

    MAX_CACHE_AGE = 14*24*60*60      # two weeks
    # Inside of groups feeds, we want to return a sample
    # even if there's only a single result.
    MINIMUM_SAMPLE_SIZE = 1

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

    def apply_filters(self, qu, facets=None, pagination=None, work_model=Work,
                      edition_model=Edition):
        """Incorporates general filters that help determine which works can be
        usefully presented to users with lane-specific queries that select
        the works specific to the QueryGeneratedLane

        :return: query or None
        """
        # Only show works that can be borrowed or reserved.
        qu = self.only_show_ready_deliverable_works(qu, work_model)

        # Only show works for the proper audiences.
        if self.audiences:
            qu = qu.filter(work_model.audience.in_(self.audiences))

        # Only show works in the source language.
        if self.languages:
            qu = qu.filter(edition_model.language.in_(self.languages))

        # Add lane-specific details to query and return the result.
        qu = self.lane_query_hook(qu, work_model=work_model)
        if not qu:
            # The hook may return None.
            return None

        if facets:
            qu = facets.apply(self._db, qu, work_model, edition_model)

        if pagination:
            qu = pagination.apply(qu)

        return qu

    def featured_works(self, use_materialized_works=True):
        """Find a random sample of books for the feed"""

        # Lane.featured_works searches for books along a variety of facets.
        # Because WorkBasedLanes are created for individual works as
        # needed (instead of at app start), we need to avoid the relative
        # slowness of those queries.
        #
        # We'll just ignore facets and return whatever we find.
        if not use_materialized_works:
            query = self.works()
        else:
            query = self.materialized_works()
        if not query:
            return []

        target_size = self.library.featured_lane_size
        return self.randomized_sample_works(
            query, target_size=target_size, use_min_size=True
        )

    def lane_query_hook(self, qu, work_model=Work):
        """Create the query specific to a subclass of  QueryGeneratedLane

        :return: query or None
        """
        raise NotImplementedError()

def make_lanes(_db, library, definitions=None):

    definitions = definitions or Configuration.policy(
        Configuration.LANES_POLICY
    )
    if not definitions:
        # A lane arrangement is required for lane making.
        return None
    lanes = [Lane(_db, library, **definition) for definition in definitions]
    return LaneList.from_description(_db, library, None, lanes)
