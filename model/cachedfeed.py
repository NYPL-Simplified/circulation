# encoding: utf-8
# CachedFeed, WillNotGenerateExpensiveFeed


from . import (
    Base,
    flush,
    get_one,
    get_one_or_create,
)

from collections import namedtuple
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
from ..util.flask_util import OPDSFeedResponse

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
    timestamp = Column(DateTime(timezone=True), nullable=True, index=True)

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
    GROUPS_TYPE = 'groups'
    PAGE_TYPE = 'page'
    NAVIGATION_TYPE = 'navigation'
    CRAWLABLE_TYPE = 'crawlable'
    RELATED_TYPE = 'related'
    RECOMMENDATIONS_TYPE = 'recommendations'
    SERIES_TYPE = 'series'
    CONTRIBUTOR_TYPE = 'contributor'

    # Special constants for cache durations.
    CACHE_FOREVER = object()
    IGNORE_CACHE = object()

    log = logging.getLogger("CachedFeed")

    @classmethod
    def fetch(cls, _db, worklist, facets, pagination, refresher_method,
              max_age=None, raw=False, **response_kwargs
    ):
        """Retrieve a cached feed from the database if possible.

        Generate it from scratch and store it in the database if
        necessary.

        Return it in the most useful form to the caller.

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
        :param raw: If this is False (the default), a Response ready to be
            converted into a Flask Response object will be returned. If this
            is True, the CachedFeed object itself will be returned. In most
            non-test situations the default is better.

        :return: A Response or CachedFeed containing up-to-date content.
        """

        # Gather the information necessary to uniquely identify this
        # page of this feed.
        keys = cls._prepare_keys(_db, worklist, facets, pagination)

        # Calculate the maximum cache age, converting from timedelta
        # to seconds if necessary.
        max_age = cls.max_cache_age(worklist, keys.feed_type, facets, max_age)

        # These arguments will probably be passed into get_one, and
        # will be passed into get_one_or_create in the event of a cache
        # miss.

        # TODO: this constraint_clause might not be necessary anymore.
        # ISTR it was an attempt to avoid race conditions, and we do a
        # better job of that now.
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
        feed_data = None
        if (max_age is cls.IGNORE_CACHE or isinstance(max_age, int) and max_age <= 0):
            # Don't even bother checking for a CachedFeed: we're
            # just going to replace it.
            feed_obj = None
        else:
            feed_obj = get_one(_db, cls, **kwargs)

        should_refresh = cls._should_refresh(feed_obj, max_age)
        if should_refresh:
            # This is a cache miss. Either feed_obj is None or
            # it's no good. We need to generate a new feed.
            feed_data = str(refresher_method())
            generation_time = datetime.datetime.now(tz=datetime.timezone.utc)

            if max_age is not cls.IGNORE_CACHE:
                # Having gone through all the trouble of generating
                # the feed, we want to cache it in the database.

                # Since it can take a while to generate a feed, and we know
                # that the feed in the database is stale, it's possible that
                # another thread _also_ noticed that feed was stale, and
                # generated a similar feed while we were working.
                #
                # To avoid a database error, fetch the feed _again_ from the
                # database rather than assuming we have the up-to-date
                # object.
                feed_obj, is_new = get_one_or_create(_db, cls, **kwargs)
                if feed_obj.timestamp is None or feed_obj.timestamp < generation_time:
                    # Either there was no contention for this object, or there
                    # was contention but our feed is more up-to-date than
                    # the other thread(s). Our feed takes priority.
                    feed_obj.content = feed_data
                    feed_obj.timestamp = generation_time
        elif feed_obj:
            feed_data = feed_obj.content

        if raw and feed_obj:
            return feed_obj

        # We have the information necessary to create a useful
        # response-type object.
        #
        # Set some defaults in case the caller didn't pass them in.
        if isinstance(max_age, int):
            response_kwargs.setdefault('max_age', max_age)

        if max_age == cls.IGNORE_CACHE:
            # If we were asked to ignore our internal cache, we should
            # also tell the client not to store this document in _its_
            # internal cache.
            response_kwargs['max_age'] = 0

        return OPDSFeedResponse(
            response=feed_data,
            **response_kwargs
        )

    @classmethod
    def feed_type(cls, worklist, facets):
        """Determine the 'type' of the feed.

        This may be defined either by `worklist` or by `facets`, with
        `facets` taking priority.

        :return: A string that can go into cachedfeeds.type.
        """
        type = CachedFeed.PAGE_TYPE
        if worklist:
            type = worklist.CACHED_FEED_TYPE or type
        if facets:
            type = facets.CACHED_FEED_TYPE or type
        return type

    @classmethod
    def max_cache_age(cls, worklist, type, facets, override=None):
        """Determine the number of seconds that a cached feed
        of a given type can remain fresh.

        Order of precedence: `override`, `facets`, `worklist`.

        :param worklist: A WorkList which may have an opinion on this
           topic.
        :param type: The type of feed being generated.
        :param facets: A faceting object that may have an opinion on this
           topic.
        :param override: A specific value passed in by the caller. This
            may either be a number of seconds or a timedelta.

        :return: A number of seconds, or CACHE_FOREVER or IGNORE_CACHE
        """
        value = override
        if value is None and facets is not None:
            value = facets.max_cache_age
        if value is None and worklist is not None:
            value = worklist.max_cache_age(type)

        if value in (cls.CACHE_FOREVER, cls.IGNORE_CACHE):
            # Special caching rules apply.
            return value

        if value is None:
            # Assume the feed should not be cached at all.
            value = 0

        if isinstance(value, datetime.timedelta):
            value = value.total_seconds()
        return value

    @classmethod
    def _should_refresh(cls, feed_obj, max_age):
        """Should we try to get a new representation of this CachedFeed?

        :param feed_obj: A CachedFeed. This may be None, which is why
            this is a class method.

        :param max_age: Either a number of seconds, or one of the constants
            CACHE_FOREVER or IGNORE_CACHE.
        """
        should_refresh = False
        if feed_obj is None:
            # If we didn't find a CachedFeed (maybe because we didn't
            # bother looking), we must always refresh.
            should_refresh = True
        elif max_age == cls.IGNORE_CACHE:
            # If we are ignoring the cache, we must always refresh.
            should_refresh = True
        elif max_age == cls.CACHE_FOREVER:
            # If we found *anything*, and the cache time is CACHE_FOREVER,
            # we will never refresh.
            should_refresh = False
        elif (feed_obj.timestamp
              and feed_obj.timestamp + datetime.timedelta(seconds=max_age) <=
                  datetime.datetime.now(tz=datetime.timezone.utc)
        ):
            # Here it comes down to a date comparison: how old is the
            # CachedFeed?
            should_refresh = True
        return should_refresh

    # This named tuple makes it easy to manage the return value of
    # _prepare_keys.
    CachedFeedKeys = namedtuple(
        'CachedFeedKeys',
        ['feed_type', 'library', 'work', 'lane_id', 'unique_key', 'facets_key',
         'pagination_key']
    )

    @classmethod
    def _prepare_keys(cls, _db, worklist, facets, pagination):
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

        feed_type = cls.feed_type(worklist, facets)

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

        facets_key = ""
        if facets is not None:
            if isinstance(facets.query_string, bytes):
                facets_key = facets.query_string.decode("utf-8")
            else:
                facets_key = facets.query_string

        pagination_key = ""
        if pagination is not None:
            if isinstance(pagination.query_string, bytes):
                pagination_key = pagination.query_string.decode("utf-8")
            else:
                pagination_key = pagination.query_string

        return cls.CachedFeedKeys(
            feed_type=feed_type, library=library, work=work, lane_id=lane_id,
            unique_key=unique_key, facets_key=facets_key,
            pagination_key=pagination_key
        )

    def update(self, _db, content):
        self.content = content
        self.timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
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

    start_time = Column(DateTime(timezone=True), nullable=True, index=True)
    end_time = Column(DateTime(timezone=True), nullable=True, index=True)
