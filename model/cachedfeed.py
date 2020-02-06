# encoding: utf-8
# CachedFeed, WillNotGenerateExpensiveFeed
from nose.tools import set_trace

from . import (
    Base,
    flush,
    get_one_or_create,
)

import collections
import datetime
import logging
from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Unicode,
)
from sqlalchemy.sql.expression import (
    and_,
)

class CachedFeed(Base):

    __tablename__ = 'cachedfeeds'
    id = Column(Integer, primary_key=True)

    # Every feed is associated with a lane. If null, this is a feed
    # for a WorkList. If work_id is also null, it's a feed for the
    # top-level.
    lane_id = Column(
        Integer, ForeignKey('lanes.id'),
        nullable=True, index=True)

    # Every feed has a timestamp reflecting when it was created.
    timestamp = Column(DateTime, nullable=True, index=True)

    # A feed is of a certain type--such as 'page' or 'groups'.
    type = Column(Unicode, nullable=False)

    # A feed associated with a WorkList can have a unique key.
    # This should be null if the feed is associated with a Lane.
    unique_key = Column(Unicode, nullable=True)

    # A 'page' feed is associated with a set of values for the facet
    # groups.
    facets = Column(Unicode, nullable=True)

    # A 'page' feed is associated with a set of values for pagination.
    pagination = Column(Unicode, nullable=False)

    # The content of the feed.
    content = Column(Unicode, nullable=True)

    # Every feed is associated with a Library.
    library_id = Column(
        Integer, ForeignKey('libraries.id'), index=True
    )

    # A feed may be associated with a Work.
    work_id = Column(Integer, ForeignKey('works.id'),
        nullable=True, index=True)

    # Distinct types of feeds that might be cached.
    GROUPS_TYPE = u'groups'
    PAGE_TYPE = u'page'
    NAVIGATION_TYPE = u'navigation'
    CRAWLABLE_TYPE = u'crawlable'
    RELATED_TYPE = u'related'
    RECOMMENDATIONS_TYPE = u'recommendations'
    SERIES_TYPE = u'series'
    CONTRIBUTOR_TYPE = u'contributor'

    # Special constants for cache durations.
    CACHE_FOREVER = object()

    log = logging.getLogger("CachedFeed")

    @classmethod
    def fetch(cls, _db, worklist, facets, pagination, refresher_method,
              cache_type=None, max_age=None
    ):
        """Retrieve a cached feed from the database if possible.

        Generate it from scratch and store it in the database if
        necessary.

        :param _db: A database connection.
        :param worklist: The WorkList associated with this feed.
        :param facets: A Facets object that distinguishes this feed from
            others (for instance, by its sort order).
        :param pagination: A Pagination object that explains which
            page of a larger feed is being cached.
        :param refresher_method: A function to call if it turns out
            the contents of the feed need to be regenerated. This
            function must take no arguments and return an object that
            implements __unicode__. (A Unicode string or an OPDSFeed is fine.)
        :param max_age: If a cached feed is older than this, it will
            be considered stale and regenerated. This may be either a
            number of seconds or a timedelta. If no value is
            specified, a default value will be calculated based on
            WorkList and Facets configuration. Setting this value to
            zero will force a refresh.

        :return: A CachedFeed containing up-to-date content.
        """
        # Gather the information necessary to uniquely identify this
        # page of this feed.
        keys = cls._prepare_keys(worklist, facets, pagination)

        # Calculate the maximum cache age, converting from timedelta
        # to seconds if necessary.
        max_age = cls.max_cache_age(worklist, keys.type, max_age)

        # These arguments will probably be passed into get_one, and
        # will be passed into get_one_or_create in the event of a cache
        # miss.
        constraint_clause = and_(cls.content!=None, cls.timestamp!=None)
        kwargs = dict(
            on_multiple='interchangeable',
            constraint=constraint_clause,
            type=keys.feed_type,
            library=keys.library,
            work=keys.work,
            lane_id=keys.lane_id,
            unique_key=keys.unique_key,
            facets=keys.facets_key,
            pagination=keys.pagination_key
        )

        if isinstance(max_age, int) and max_age <= 0:
            # Don't even bother checking for a CachedFeed: we're
            # just going to replace it.
            existing_item = None
        else:
            existing_item = get(_db, cls, **kwargs)

        if existing_item is None:
            # If we didn't find a CachedFeed (maybe because we didn't
            # bother looking), we must always refresh.
            should_refresh = True
        elif max_age == cls.CACHE_FOREVER:
            # If we found *anything*, and the cache time is CACHE_FOREVER,
            # we will never refresh.
            should_refresh = False
        elif (feed.timestamp
            and feed.timestamp >= (datetime.datetime.utcnow() - max_age)):
            # Otherwise, it comes down to a date comparison: how old
            # is the CachedFeed?
            should_refresh = True

        if not should_refresh:
            # This is a cache hit. We found a matching CachedFeed that
            # had fresh content.
            return feed

        # This is a cache miss. We need to generate a new feed.
        new_feed = unicode(refresh_method())

        # Since it can take a while to generate a feed, and we know
        # that the feed in the database is stale, it's possible that
        # another thread _also_ noticed that feed was stale, and
        # generated a similar feed while we were working.
        #
        # To avoid a database error, fetch the feed _again_ from the
        # database rather than assuming we have the up-to-date
        # object.
        feed_obj, is_new = get_one_or_create(_db, cls, **kwargs)
        feed_obj.content = new_feed
        return feed_obj

    @classmethod
    def feed_type(cls, worklist, facets):
        """Determine the 'type' of the feed.

        This may be defined either by `worklist` or by `facets`, with
        `facets` taking priority.

        :return: A string that can go into cachedfeeds.type.
        """
        type = CachedFeed.PAGE_TYPE
        if lane:
            type = worklist.CACHED_FEED_TYPE or implied_type
        if facets:
            type = facets.CACHED_FEED_TYPE or implied_type
        return type

    @classmethod
    def max_cache_age(cls, worklist, type, override):
        """Determine the number of seconds that a cached feed
        of a given type can remain fresh.

        :param worklist: A WorkList.
        :param type: The type of feed being generated.
        :param override: A specific value passed in by the user. This
            may either be a number of seconds or a timedelta.

        :return: A number of seconds, or CACHE_FOREVER.
        """
        value = None
        if override is not None:
            value = override
        elif lane:
            value = worklist.max_cache_age(type)

        if value == cls.CACHE_FOREVER:
            # This feed should be cached forever.
            return value

        if value is None:
            # Assume the feed should not be cached at all.
            value = 0

        if isinstance(value, timedelta):
            value = value.seconds()
        return value

    # This named tuple makes it easy to manage the return value of
    # _prepare_keys.
    CachedFeedKeys = namedtuple(
        'CachedFeedKeys',
        ['feed_type', 'library', 'work', 'lane_id', 'unique_key', 'facets_key',
         'pagination_key']
    )

    @classmethod
    def _prepare_keys(cls, worklist, facets, pagination):
        """Prepare various unique keys that will go into the database
        and be used to distinguish CachedFeeds from one another.

        This is kept in a helper method for ease of testing.

        :param worklist: A WorkList.
        :param facets: A Facets object.
        :param pagination: A Pagination object.

        :return: A CachedFeedKeys object.
        """
        if not worklist:
            raise ValueError(
                "Cannot prepare a CachedFeed without a WorkList."
            )

        feed_type = cls.feed_type(lane, facets)

        # The Library is the one associated with `worklist`.
        library = worklist.get_library(_db)

        # A feed may be associated with a specific Work,
        # e.g. recommendations for readers of that Work.
        work = getattr(worklist, 'work', None)

        # Either lane_id or unique_key must be set, but not both.
        from ..lane import Lane
        if isinstance(worklist, Lane):
            lane_id = worklist.id
            unique_key = None
        else:
            lane_id = None
            unique_key = worklist.unique_key

        facets_key = u""
        if facets is not None:
            facets_key = unicode(facets.query_string)

        pagination_key = u""
        if pagination is not None:
            pagination_key = unicode(pagination.query_string)

        return CachedFeedKeys(
            feed_type=feed_type, library=library, work=work, lane_id=lane_id,
            unique_key=unique_key, facets_key=facets_key,
            pagination_key=pagination_key
        )

    def update(self, _db, content):
        self.content = content
        self.timestamp = datetime.datetime.utcnow()
        flush(_db)

    def __repr__(self):
        if self.content:
            length = len(self.content)
        else:
            length = "No content"
        return "<CachedFeed #%s %s %s %s %s %s %s >" % (
            self.id, self.lane_id, self.type,
            self.facets, self.pagination,
            self.timestamp, length
        )


Index(
    "ix_cachedfeeds_library_id_lane_id_type_facets_pagination",
    CachedFeed.library_id, CachedFeed.lane_id, CachedFeed.type,
    CachedFeed.facets, CachedFeed.pagination
)


class WillNotGenerateExpensiveFeed(Exception):
    """This exception is raised when a feed is not cached, but it's too
    expensive to generate.
    """
    pass

class CachedMARCFile(Base):
    """A record that a MARC file has been created and cached for a particular lane."""

    __tablename__ = 'cachedmarcfiles'
    id = Column(Integer, primary_key=True)

    # Every MARC file is associated with a library and a lane. If the
    # lane is null, the file is for the top-level WorkList.
    library_id = Column(
        Integer, ForeignKey('libraries.id'),
        nullable=False, index=True)

    lane_id = Column(
        Integer, ForeignKey('lanes.id'),
        nullable=True, index=True)

    # The representation for this file stores the URL where it was mirrored.
    representation_id = Column(
        Integer, ForeignKey('representations.id'),
        nullable=False)

    start_time = Column(DateTime, nullable=True, index=True)
    end_time = Column(DateTime, nullable=True, index=True)
