from nose.tools import (
    assert_raises,
    assert_raises_regexp,
    eq_,
    set_trace,
)

from config import (
    Configuration,
    temp_config,
)

from model import (
    get_one_or_create,
    CachedFeed,
    WillNotGenerateExpensiveFeed,
)

from lane import (
    Lane,
    Pagination,
    Facets,
    WorkList,
)

from opds import AcquisitionFeed

from . import (
    DatabaseTest
)

class TestCachedFeed(DatabaseTest):

    def test_lifecycle(self):
        facets = Facets.default(self._default_library)
        pagination = Pagination.default()
        lane = self._lane(u"My Lane", languages=['eng','chi'])

        # Fetch a cached feed from the database--it's empty.
        args = (self._db, lane, CachedFeed.PAGE_TYPE, facets, pagination, None)
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
        args = (self._db, lane, CachedFeed.PAGE_TYPE, facets, pagination, None)
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
        args = (self._db, lane, CachedFeed.PAGE_TYPE, facets,
                pagination, None)
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

        args = (self._db, lane, CachedFeed.PAGE_TYPE, facets,
                     pagination, None)

        # If we ask for a group feed that will be cached forever, and it's
        # not around, we'll get a page feed instead.
        feed, fresh = CachedFeed.fetch(
            *args, max_age=AcquisitionFeed.CACHE_FOREVER
        )
        eq_(CachedFeed.PAGE_TYPE, feed.type)

        # If we ask for the same feed, but we don't say it must be cached
        # forever, it'll be created.
        feed, fresh = CachedFeed.fetch(*args, max_age=0)

        # Or if we explicitly demand that the feed be created, it will
        # be created.
        feed, fresh = CachedFeed.fetch(
            *args, force_refresh=True, max_age=AcquisitionFeed.CACHE_FOREVER
        )
        feed.update(self._db, "Cache this forever!")

        # Once the feed has content associated with it, we can ask for
        # it in cached-forever mode and no longer get the exception.
        feed, fresh = CachedFeed.fetch(
            *args, max_age=AcquisitionFeed.CACHE_FOREVER
        )
        eq_("Cache this forever!", feed.content)
