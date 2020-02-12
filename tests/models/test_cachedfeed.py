# encoding: utf-8
from nose.tools import (
    eq_,
    set_trace,
)
import datetime
from .. import DatabaseTest
from ...classifier import Classifier
from ...lane import (
    Facets,
    Pagination,
    Lane,
    WorkList,
)
from ...model.cachedfeed import CachedFeed
from ...model.configuration import ConfigurationSetting
from ...opds import AcquisitionFeed


class TestCachedFeed(DatabaseTest):

    def test_fetch_page_feeds(self):
        """CachedFeed.fetch retrieves paginated feeds from the database if
        they exist, and prepares them for creation if not.
        """
        m = CachedFeed.fetch
        lane = self._lane()
        page = CachedFeed.PAGE_TYPE
        annotator = object()

        # A page feed for a lane with no facets or pagination.
        feed, fresh = m(self._db, lane, page, None, None, annotator)
        eq_(page, feed.type)

        # The feed is not usable as-is because there's no content.
        eq_(False, fresh)

        # If we set content, we can fetch the same feed and then it
        # becomes usable.
        feed.content = "some content"
        feed.timestamp = (
            datetime.datetime.utcnow() - datetime.timedelta(seconds=5)
        )
        feed2, fresh = m(self._db, lane, page, None, None, annotator)
        eq_(feed, feed2)
        eq_(True, fresh)

        # But a feed is not considered fresh if it's older than `max_age`
        # seconds.
        feed, fresh = m(
            self._db, lane, page, None, None, annotator, max_age=0
        )
        eq_(False, fresh)

        # This feed has no unique key because its lane ID and type
        # are enough to uniquely identify it.
        eq_(None, feed.unique_key)
        eq_("", feed.pagination)
        eq_("", feed.facets)

        # Now let's introduce some pagination and facet information.
        facets = Facets.default(self._default_library)
        pagination = Pagination.default()
        feed2, fresh = m(
            self._db, lane, page, facets, pagination, annotator
        )
        assert feed2 != feed
        eq_(pagination.query_string, feed2.pagination)
        eq_(facets.query_string, feed2.facets)

        # There's still no need for a unique key because pagination
        # and facets are taken into account when trying to uniquely
        # identify a feed.
        eq_(None, feed.unique_key)

        # However, a lane based on a WorkList has no lane ID, so a
        # unique key is necessary.
        worklist = WorkList()
        worklist.initialize(
            library=self._default_library, display_name="aworklist",
            languages=["eng", "spa"], audiences=[Classifier.AUDIENCE_CHILDREN]
        )
        feed, fresh = m(
            self._db, worklist, page, None, None, annotator
        )
        # The unique key incorporates the WorkList's display name,
        # its languages, and its audiences.
        eq_("aworklist-eng,spa-Children", feed.unique_key)

    def test_fetch_group_feeds(self):
        # Group feeds don't need to worry about facets or pagination,
        # but they have their own complications.

        m = CachedFeed.fetch
        lane = self._lane()
        groups = CachedFeed.GROUPS_TYPE
        annotator = object()

        # Ask for a groups feed for a lane.
        feed, usable = m(self._db, lane, groups, None, None, annotator)

        # The feed is not usable because there's no content.
        eq_(False, usable)

        # Group-type feeds are too expensive to generate, so when
        # asked to produce one we prepared a page-type feed instead.
        eq_(CachedFeed.PAGE_TYPE, feed.type)
        eq_(lane, feed.lane)
        eq_(None, feed.unique_key)
        eq_("", feed.facets)
        eq_("", feed.pagination)

        # But what if a group feed had been created ahead of time
        # through some other mechanism?
        feed.content = "some content"
        feed.type = groups
        feed.timestamp = datetime.datetime.utcnow()

        # Now fetch() finds the feed, but because there was content
        # and a recent timestamp, it's now usable and there's no need
        # to change the type.
        feed2, usable = m(self._db, lane, groups, None, None, annotator)
        eq_(feed, feed2)
        eq_(True, usable)
        eq_(groups, feed.type)
        eq_("some content", feed.content)

        # If we pass in force_refresh then the feed is always treated as
        # stale.
        feed, usable = m(self._db, lane, groups, None, None, annotator,
                         force_refresh=True)
        eq_(False, usable)

    def test_calculate_max_age(self):
        # Verify the rules for determining how long a feed should be cached,
        # assuming fetch() was called with no explicit time limit.

        # Create three WorkLists -- these are subject to different rules
        # for different types of feeds.
        lane = self._lane()
        default_max_cache_age = WorkList()
        class NoMaxCacheAge(WorkList):
            MAX_CACHE_AGE = None
        no_max_cache_age = NoMaxCacheAge()

        class HasMaxCacheAge(WorkList):
            MAX_CACHE_AGE = 42
        has_max_cache_age = HasMaxCacheAge()

        # These are the three feed types we'll be checking.
        groups = CachedFeed.GROUPS_TYPE
        page = CachedFeed.PAGE_TYPE
        other = "some other kind of feed"

        def time(lane, type, expect):
            actual = CachedFeed.calculate_max_age(self._db, lane, type)
            eq_(expect, actual)

        # When site-wide configuration settings are not set, grouped
        # feeds for lanes are cached forever and other feeds are
        # cached for Lane.MAX_CACHE_AGE.
        time(lane, groups, Lane.CACHE_FOREVER)
        time(lane, page, Lane.MAX_CACHE_AGE)
        time(lane, other, Lane.MAX_CACHE_AGE)

        # WorkLists with no explicit MAX_CACHE_AGE set always get
        # WorkList.MAX_CACHE_AGE -- two weeks.
        for type in (groups, page, other):
            time(default_max_cache_age, type, WorkList.MAX_CACHE_AGE)

        # WorkLists with MAX_CACHE_AGE set always get that value.
        for type in (groups, page, other):
            time(has_max_cache_age, type, HasMaxCacheAge.MAX_CACHE_AGE)

        # If MAX_CACHE_AGE is set to None, AcquisitionFeed assumes the feed
        # should not be cached at all.
        for type in (groups, page, other):
            time(no_max_cache_age, type, 0)

    def test_lifecycle(self):
        facets = Facets.default(self._default_library)
        pagination = Pagination.default()
        lane = self._lane(u"My Lane", languages=['eng','chi'])

        # Fetch a cached feed from the database--it's empty.
        args = (self._db, lane, facets, pagination, None)
        feed, fresh = CachedFeed.fetch(*args, max_age=0)

        eq_(False, fresh)
        eq_(None, feed.content)

        eq_(pagination.query_string, feed.pagination)
        eq_(facets.query_string, feed.facets)
        eq_(lane.id, feed.lane_id)

        # Update the content
        feed.update(self._db, u"The content")
        self._db.commit()

        # Fetch it again.
        feed, fresh = CachedFeed.fetch(*args, max_age=0)

        # Now it's cached! But not fresh, because max_age is zero
        eq_("The content", feed.content)
        eq_(False, fresh)

        # Lower our standards, and it's fresh!
        feed, fresh = CachedFeed.fetch(*args, max_age=1000)
        eq_("The content", feed.content)
        eq_(True, fresh)

    def test_lifecycle_with_worklist(self):
        facets = Facets.default(self._default_library)
        pagination = Pagination.default()
        lane = WorkList()
        lane.initialize(self._default_library)

        # Fetch a cached feed from the database--it's empty.
        args = (self._db, lane, facets, pagination, None)
        feed, fresh = CachedFeed.fetch(*args, max_age=0)

        eq_(False, fresh)
        eq_(None, feed.content)

        eq_(pagination.query_string, feed.pagination)
        eq_(facets.query_string, feed.facets)
        eq_(None, feed.lane_id)

        # Update the content
        feed.update(self._db, u"The content")
        self._db.commit()

        # Fetch it again.
        feed, fresh = CachedFeed.fetch(*args, max_age=0)

        # Now it's cached! But not fresh, because max_age is zero
        eq_("The content", feed.content)
        eq_(False, fresh)

        # Lower our standards, and it's fresh!
        feed, fresh = CachedFeed.fetch(*args, max_age=1000)
        eq_("The content", feed.content)
        eq_(True, fresh)

    def test_fetch_ignores_feeds_without_content(self):
        facets = Facets.default(self._default_library)
        pagination = Pagination.default()
        lane = self._lane(u"My Lane", languages=['eng', 'chi'])

        # Create a feed without content (i.e. don't update it)
        contentless_feed = get_one_or_create(
            self._db, CachedFeed,
            lane_id=lane.id,
            type=CachedFeed.PAGE_TYPE,
            facets=unicode(facets.query_string),
            pagination=unicode(pagination.query_string))[0]

        # It's not returned because it hasn't been updated.
        args = (self._db, lane, facets, pagination, None)
        feed, fresh = CachedFeed.fetch(*args)
        eq_(True, feed != contentless_feed)
        eq_(False, fresh)

        # But if the feed is updated, we get it back.
        feed.update(self._db, u"Just feedy things")
        result, fresh = CachedFeed.fetch(*args)
        eq_(True, fresh)
        eq_(feed, result)

    def test_refusal_to_create_expensive_feed(self):

        facets = Facets.default(self._default_library)
        pagination = Pagination.default()
        lane = self._lane(u"My Lane", languages=['eng', 'chi'])

        args = (self._db, lane, facets, pagination, None)

        # If we ask for a group feed that will be cached forever, and it's
        # not around, we'll get a page feed instead.
        feed, fresh = CachedFeed.fetch(
            *args, max_age=WorkList.CACHE_FOREVER
        )
        eq_(CachedFeed.PAGE_TYPE, feed.type)

        # If we ask for the same feed, but we don't say it must be cached
        # forever, it'll be created.
        feed, fresh = CachedFeed.fetch(*args, max_age=0)

        # Or if we explicitly demand that the feed be created, it will
        # be created.
        feed, fresh = CachedFeed.fetch(
            *args, force_refresh=True, max_age=WorkList.CACHE_FOREVER
        )
        feed.update(self._db, "Cache this forever!")

        # Once the feed has content associated with it, we can ask for
        # it in cached-forever mode and no longer get the exception.
        feed, fresh = CachedFeed.fetch(
            *args, max_age=WorkList.CACHE_FOREVER
        )
        eq_("Cache this forever!", feed.content)
