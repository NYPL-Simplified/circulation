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
    CachedFeed,
    WillNotGenerateExpensiveFeed,
)

from lane import (
    Lane,
    Pagination,
    Facets,
)

from . import (
    DatabaseTest
)

class TestCachedFeed(DatabaseTest):

    def test_get_feed_or_create(self):
        lane = Lane(self._db, u'Fantasy', languages=['eng'])
        kwargs = dict(
            lane_name=u'Fantasy',
            languages=u'eng',
            facets=u'',
            pagination=u'',
            type=CachedFeed.PAGE_TYPE)

        result, is_new = CachedFeed.get_feed_or_create(self._db, **kwargs)
        eq_(True, isinstance(result, CachedFeed))
        eq_(True, is_new)

        # If a CachedFeed exists, but it hasn't been updated,
        # a new CachedFeed is returned.
        old_result = result
        new_result, is_new = CachedFeed.get_feed_or_create(self._db, **kwargs)
        eq_(True, old_result != new_result)
        eq_(True, is_new)
        # And the unusable CachedFeed is deleted from the db.
        eq_(True, old_result not in self._db)

        # But if we give the CachedFeed content, we'll get it back.
        new_result.update(self._db, u"l'elephante")
        old_result = new_result
        new_result, is_new = CachedFeed.get_feed_or_create(self._db, **kwargs)
        eq_(old_result, new_result)
        eq_(False, is_new)

    def test_lifecycle(self):
        facets = Facets.default()
        pagination = Pagination.default()
        lane = Lane(self._db, u"My Lane", languages=['eng', 'chi'])

        # Fetch a cached feed from the database--it's empty.
        args = (self._db, lane, CachedFeed.PAGE_TYPE, facets, pagination, None)
        feed, fresh = CachedFeed.fetch(*args, max_age=0)
            
        eq_(False, fresh)
        eq_(None, feed.content)

        eq_(pagination.query_string, feed.pagination)
        eq_(facets.query_string, feed.facets)
        eq_(lane.name, feed.lane_name)
        eq_('eng,chi', feed.languages)

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

    def test_refusal_to_create_expensive_feed(self):
        
        facets = Facets.default()
        pagination = Pagination.default()
        lane = Lane(self._db, u"My Lane", languages=['eng', 'chi'])

        args = (self._db, lane, CachedFeed.PAGE_TYPE, facets, 
                     pagination, None)
        
        # If we ask for a group feed that will be cached forever, and it's
        # not around, we'll get a page feed instead.
        feed, fresh = CachedFeed.fetch(
            *args, max_age=Configuration.CACHE_FOREVER
        )
        eq_(CachedFeed.PAGE_TYPE, feed.type)
      
        # If we ask for the same feed, but we don't say it must be cached
        # forever, it'll be created.
        feed, fresh = CachedFeed.fetch(*args, max_age=0)
        
        # Or if we explicitly demand that the feed be created, it will
        # be created.
        feed, fresh = CachedFeed.fetch(
            *args, force_refresh=True, max_age=Configuration.CACHE_FOREVER
        )
        feed.update(self._db, "Cache this forever!")

        # Once the feed has content associated with it, we can ask for
        # it in cached-forever mode and no longer get the exception.
        feed, fresh = CachedFeed.fetch(
            *args, max_age=Configuration.CACHE_FOREVER
        )
        eq_("Cache this forever!", feed.content)
