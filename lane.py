from collections import defaultdict
from nose.tools import set_trace
import datetime
import random
import time
import logging

from psycopg2.extras import NumericRange

from config import Configuration

import classifier
from classifier import (
    Classifier,
    GenreData,
)

from sqlalchemy import (
    or_,
    not_,
)
from sqlalchemy.orm import (
    contains_eager,
    defer,
    lazyload,
)

from model import (
    CustomList,
    CustomListEntry,
    DataSource,
    DeliveryMechanism,
    Edition,
    Genre,
    LicensePool,
    Work,
    WorkGenre,
)
from facets import FacetConstants
from util import fast_query_count
import elasticsearch

class Facets(FacetConstants):

    @classmethod
    def default(cls):
        return cls(
            collection=cls.COLLECTION_MAIN,
            availability=cls.AVAILABLE_ALL,
            order=cls.ORDER_AUTHOR
        )


    def __init__(self, collection, availability, order,
                 order_ascending=None):
        
        if order_ascending is None:
            if order == self.ORDER_ADDED_TO_COLLECTION:
                order_ascending = self.ORDER_DESCENDING
            else:
                order_ascending = self.ORDER_ASCENDING

        collection = collection or Configuration.default_facet(
            self.COLLECTION_FACET_GROUP_NAME
        )
        availability = availability or Configuration.default_facet(
            self.AVAILABILITY_FACET_GROUP_NAME
        )
        order = order or Configuration.default_facet(
            self.ORDER_FACET_GROUP_NAME
        )

        hold_policy = Configuration.hold_policy()
        if (availability == self.AVAILABLE_ALL and 
            hold_policy == Configuration.HOLD_POLICY_HIDE):
            # Under normal circumstances we would show all works, but
            # site configuration says to hide books that aren't
            # available.
            availability = self.AVAILABLE_NOW

        self.collection = collection
        self.availability = availability
        self.order = order
        if order_ascending == self.ORDER_ASCENDING:
            order_ascending = True
        elif order_ascending == self.ORDER_DESCENDING:
            order_ascending = False
        self.order_ascending = order_ascending

    def navigate(self, collection=None, availability=None, order=None):
        """Create a slightly different Facets object from this one."""
        return Facets(collection or self.collection, 
                      availability or self.availability, 
                      order or self.order) 

    def items(self):
        if self.order:
            yield (self.ORDER_FACET_GROUP_NAME, self.order)
        if self.availability:
            yield (self.AVAILABILITY_FACET_GROUP_NAME,  self.availability)        
        if self.collection:
            yield (self.COLLECTION_FACET_GROUP_NAME, self.collection)

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

    @property
    def query_string(self):
        return "&".join("=".join(x) for x in sorted(self.items()))

    @property
    def facet_groups(self):
        """Yield a list of 4-tuples 
        (facet group, facet value, new Facets object, selected)
        for use in building OPDS facets.
        """

        def dy(new_value):
            group = self.ORDER_FACET_GROUP_NAME
            current_value = self.order
            facets = self.navigate(order=new_value)
            return (group, new_value, facets, current_value==new_value)

        # First, the order facets.
        order_facets = Configuration.enabled_facets(
            Facets.ORDER_FACET_GROUP_NAME
        )
        if len(order_facets) > 1:
            for facet in order_facets:
                yield dy(facet)

        # Next, the availability facets.
        def dy(new_value):
            group = self.AVAILABILITY_FACET_GROUP_NAME
            current_value = self.availability
            facets = self.navigate(availability=new_value)
            return (group, new_value, facets, new_value==current_value)
        availability_facets = Configuration.enabled_facets(
            Facets.AVAILABILITY_FACET_GROUP_NAME
        )
        if len(availability_facets) > 1:
            for facet in availability_facets:
                yield dy(facet)

        # Next, the collection facets.
        collection_facets = Configuration.enabled_facets(
            Facets.COLLECTION_FACET_GROUP_NAME
        )        
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
            # Exclude books with a quality of less than
            # MINIMUM_FEATURED_QUALITY.
            q = q.filter(
                work_model.quality >= Configuration.minimum_featured_quality()
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
        """Returns boolean reporting whether pagination is done for a query"""
        if self.query_size is None:
            return True
        if self.query_size==0:
            return False
        return (self.offset+1) * self.size < self.query_size

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

    def __repr__(self):
        template = "<Lane name=%(full_name)s, display=%(display_name)s, media=%(media)s, genre=%(genres)s, fiction=%(fiction)s, audience=%(audiences)s, age_range=%(age_range)r, language=%(language)s, sublanes=%(sublanes)d>"

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

        #self.log.debug("%s%r", "-" * level, self)
        #for lane in self.sublanes.lanes:
        #    lane.debug(level+1)

    def __init__(self, 
                 _db, 
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
        self.name = full_name
        self.display_name = display_name or self.name
        self.parent = parent
        self._db = _db
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

        def set_from_parent(field_name, value, default=None):
            if value is None:
                if self.parent:
                    value = getattr(self.parent, field_name, default)
                else:
                    value = default
            setattr(self, field_name, value)

        def set_list(field_name, value, default=None):
            if value is None:
                if self.parent:
                    value = getattr(self.parent, field_name, default)
                else:
                    value = default
            if isinstance(value, basestring):
                value = [value]
            set_from_parent(field_name, value, default)

        if isinstance(age_range, int):
            age_range = [age_range]
        if age_range is not None:
            age_range = sorted(age_range)
        set_from_parent('age_range', age_range)

        self.audiences = self.audience_list_for_age_range(audiences, age_range)

        set_list('languages', languages)
        set_list('exclude_languages', exclude_languages)
        set_list('appeals', appeals)

        # The lane may be restricted to items in particular media
        # and/or formats.
        set_list('media', media, Edition.BOOK_MEDIUM)
        set_list('formats', formats)

        # The lane may be restricted to books that are on a list
        # from a given data source.
        self.list_data_source_id, self.list_ids = self.custom_lists_for_identifier(
            list_data_source, list_identifier)
        set_from_parent(
            'list_seen_in_previous_days', list_seen_in_previous_days)

        set_from_parent(
            'subgenre_behavior', subgenre_behavior, self.IN_SUBLANES)

        if self.searchable and (self.list_data_source_id or self.list_ids):
            raise UndefinedLane("Lane with list data source cannot be searchable")

        # However the genres came in, turn them into database Genre
        # objects and the corresponding GenreData objects.
        genres, genredata = self.load_genres(self._db, genres)

        # Create a complete list of genres to exclude.
        full_exclude_genres = set()
        if exclude_genres:
            for genre in exclude_genres:
                genre, ignore = self.load_genre(self._db, genre)
                for l in genre.self_and_subgenres:
                    full_exclude_genres.add(l)

        if fiction is None:
            fiction = self.FICTION_DEFAULT_FOR_GENRE

        # Find all the genres that will go into this lane.
        genres, self.fiction = self.gather_matching_genres(
            genres, fiction, full_exclude_genres
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
                            self._db, full_name=subgenre.name,
                            parent=self, genres=[subgenre],
                            subgenre_behavior=self.IN_SUBLANES
                    )
                    subgenre_sublanes.append(sublane)

        if sublanes and subgenre_sublanes:
            raise UndefinedLane(
                "Explicit list of sublanes was provided, but I'm also asked to turn %s subgenres into sublanes!" % len(subgenre_sublanes)
            )

        if subgenre_sublanes:
            self.sublanes = LaneList(self)
            for sl in subgenre_sublanes:
                self.sublanes.add(sl)
        elif sublanes:
            self.sublanes = LaneList.from_description(
                _db, self, sublanes
            )
        else:
            self.sublanes = LaneList.from_description(_db, self, [])

        # Best-seller and staff pick lanes go at the top.
        base_args = dict(
            _db=self._db, parent=self, include_all=False, genres=genres,
            exclude_genres=exclude_genres, fiction=fiction, 
            audiences=audiences, age_range=age_range,
            appeals=appeals, languages=languages, 
            exclude_languages=exclude_languages, media=media, 
            formats=formats
        )
        if include_staff_picks:
            full_name = "%s - Staff Picks" % self.name
            try:
                staff_picks_lane = Lane(
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

        if include_best_sellers:
            full_name = "%s - Best Sellers" % self.name
            try:
                best_seller_lane = Lane(
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


        # Run some sanity checks.
        ch = Classifier.AUDIENCE_CHILDREN
        ya = Classifier.AUDIENCE_YOUNG_ADULT
        if ((include_best_sellers or include_staff_picks) and
            (self.list_data_source_id or self.list_ids)):
            raise UndefinedLane(
                "Cannot include best-seller or staff-picks in a lane based on lists."
            )

        if (
                self.age_range 
                and not any(x in self.audiences for x in [ch, ya])
        ):
            raise UndefinedLane(
                "Lane %s specifies age range but does not contain children's or young adult books." % self.name
            )

    def audience_list_for_age_range(self, audiences, age_range, default=[]):
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
        if lists:
            list_ids = [x.id for x in lists]
        else:
            list_ids = None
        return list_data_source_id, list_ids

    @classmethod
    def from_description(cls, _db, parent, description):
        genre = None
        if isinstance(description, Lane):
            # The lane has already been created.
            description.parent = parent
            return description
        elif isinstance(description, dict):
            if description.get('suppress_lane'):
                return None
            # Invoke the constructor
            return Lane(_db, parent=parent, **description)
        else:
            # This is a lane for a specific genre.
            genre, genredata = Lane.load_genre(_db, description)
            return Lane(_db, genre.name, parent=parent, genres=genre)

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
    def all_matching_genres(cls, genres, exclude_genres=None):
        matches = set()
        exclude_genres = exclude_genres or []
        if genres:
            for genre in genres:
                matches = matches.union(genre.self_and_subgenres)
        return [x for x in matches if x not in exclude_genres]

    @classmethod
    def gather_matching_genres(cls, genres, fiction, exclude_genres=[]):
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
        genres = cls.all_matching_genres(genres, exclude_genres)
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

        * Have an unsuppressed license pool.
        """

        q = self._db.query(Work).join(Work.presentation_edition)
        q = q.join(Work.license_pools).join(LicensePool.data_source).join(
            LicensePool.identifier
        )
        q = q.options(
            contains_eager(Work.license_pools),
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

        q = self.apply_filters(q, facets, pagination, Work, Edition)

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
        q = self.apply_filters(q, facets, pagination, mw, mw)

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

            q = q.join(LicensePool.custom_list_entries)
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

    @classmethod
    def only_show_ready_deliverable_works(
            cls, query, work_model, show_suppressed=False
    ):
        """Restrict a query to show only presentation-ready
        works which the default client can fulfill.

        Note that this assumes the query has an active join against
        LicensePool.
        """
        # Only find presentation-ready works.
        #
        # Such works are automatically filtered out of 
        # the materialized view.
        if work_model == Work:
            query = query.filter(
                work_model.presentation_ready == True,
            )

        # Only find books the default client can fulfill.
        query = query.filter(LicensePool.delivery_mechanisms.any(
            DeliveryMechanism.default_client_can_fulfill==True)
        )

        # Only find books with unsuppressed LicensePools.
        if not show_suppressed:
            query = query.filter(LicensePool.suppressed==False)

        # Only find books with available licenses.
        query = query.filter(
                or_(LicensePool.licenses_owned > 0, LicensePool.open_access)
        )

        # If we don't allow holds, hide any books with no available copies.
        hold_policy = Configuration.hold_policy()
        if hold_policy == Configuration.HOLD_POLICY_HIDE:
            query = query.filter(
                or_(LicensePool.licenses_available > 0, LicensePool.open_access)
            )

        return query

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
            pagination = Pagination.default()

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

    def featured_works(self, size, use_materialized_works=True):
        """Find a random sample of `size` featured books.

        It's semi-okay for this to be slow, since it will only be run to
        create cached feeds.

        :return: A list of MaterializedWork or MaterializedWorkWithGenre
        objects.
        """
        books = []
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
            facets = Facets(collection=collection, availability=availability,
                            order=Facets.ORDER_RANDOM)
            desperate = (collection==Facets.COLLECTION_FULL
                         and availability == Facets.AVAILABLE_ALL)
            books = self.featured_works_for_facets(facets, size, desperate,
                                                   use_materialized_works)
            if books:
                break
        return books

    def featured_works_for_facets(
            self, facets, size, desperate=False, use_materialized_works=True
    ):
        """Find a random sample of `size` featured books matching
        the given facets.
        """
        if use_materialized_works:
            query = self.materialized_works(facets)
        else:
            query = self.works(facets)
        total_size = query.count()
        if total_size >= size:
            # There are enough results that we can take a random
            # sample.
            offset = random.randint(0, total_size-size)
        else:
            if desperate:
                # There are not enough results that we can take a
                # random sample. But we're desperate. Use these books.
                offset = 0
            else:
                # We're not desperate. Just return nothing.
                return []
        works = query.offset(offset).limit(size).all()
        random.shuffle(works)
        return works

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
    def from_description(cls, _db, parent_lane, description):
        lanes = LaneList(parent_lane)
        description = description or []
        for lane_description in description:
            lane = Lane.from_description(_db, parent_lane, lane_description)

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
