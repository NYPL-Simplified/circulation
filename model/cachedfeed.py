# encoding: utf-8
# CachedFeed, WillNotGenerateExpensiveFeed
from nose.tools import set_trace

from . import (
    Base,
    flush,
    get_one_or_create,
)

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

    GROUPS_TYPE = u'groups'
    PAGE_TYPE = u'page'
    NAVIGATION_TYPE = u'navigation'
    RECOMMENDATIONS_TYPE = u'recommendations'
    SERIES_TYPE = u'series'
    CONTRIBUTOR_TYPE = u'contributor'

    log = logging.getLogger("CachedFeed")

    @classmethod
    def fetch(cls, _db, lane, type, facets, pagination, annotator,
              force_refresh=False, max_age=None):
        from ..opds import AcquisitionFeed
        from ..lane import Lane, WorkList
        if max_age is None:
            if type == cls.GROUPS_TYPE:
                max_age = AcquisitionFeed.grouped_max_age(_db)
            elif type == cls.PAGE_TYPE:
                max_age = AcquisitionFeed.nongrouped_max_age(_db)
            elif hasattr(lane, 'MAX_CACHE_AGE'):
                max_age = lane.MAX_CACHE_AGE
            else:
                max_age = 0
        if isinstance(max_age, int):
            max_age = datetime.timedelta(seconds=max_age)

        unique_key = None
        if lane and isinstance(lane, Lane):
            lane_id = lane.id
        else:
            lane_id = None
            unique_key = "%s-%s-%s" % (lane.display_name, lane.language_key, lane.audience_key)
        work = None
        if lane:
            work = getattr(lane, 'work', None)
        library = None
        if lane and isinstance(lane, Lane):
            library = lane.library
        elif lane and isinstance(lane, WorkList):
            library = lane.get_library(_db)

        if facets:
            facets_key = unicode(facets.query_string)
        else:
            facets_key = u""

        if pagination:
            pagination_key = unicode(pagination.query_string)
        else:
            pagination_key = u""

        # Get a CachedFeed object. We will either return its .content,
        # or update its .content.
        constraint_clause = and_(cls.content!=None, cls.timestamp!=None)
        feed, is_new = get_one_or_create(
            _db, cls,
            on_multiple='interchangeable',
            constraint=constraint_clause,
            lane_id=lane_id,
            unique_key=unique_key,
            library=library,
            work=work,
            type=type,
            facets=facets_key,
            pagination=pagination_key)

        if force_refresh is True:
            # No matter what, we've been directed to treat this
            # cached feed as stale.
            return feed, False

        if max_age is AcquisitionFeed.CACHE_FOREVER:
            # This feed is so expensive to generate that it must be cached
            # forever (unless force_refresh is True).
            if not is_new and feed.content:
                # Cacheable!
                return feed, True
            else:
                # We're supposed to generate this feed, but as a group
                # feed, it's too expensive.
                #
                # Rather than generate an error (which will provide a
                # terrible user experience), fall back to generating a
                # default page-type feed, which should be cheap to fetch.
                identifier = None
                if isinstance(lane, Lane):
                    identifier = lane.id
                elif isinstance(lane, WorkList):
                    identifier = lane.display_name
                cls.log.warn(
                    "Could not generate a groups feed for %s, falling back to a page feed.",
                    identifier
                )
                return cls.fetch(
                    _db, lane, CachedFeed.PAGE_TYPE, facets, pagination,
                    annotator, force_refresh, max_age=None
                )
        else:
            # This feed is cheap enough to generate on the fly.
            cutoff = datetime.datetime.utcnow() - max_age
            fresh = False
            if feed.timestamp and feed.content:
                if feed.timestamp >= cutoff:
                    fresh = True
            return feed, fresh

        # Either there is no cached feed or it's time to update it.
        return feed, False

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
