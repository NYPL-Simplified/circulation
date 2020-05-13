# encoding: utf-8
from nose.tools import (
    assert_raises_regexp,
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
from ...util.flask_util import OPDSFeedResponse
from ...util.opds_writer import OPDSFeed

class MockFeedGenerator(object):

    def __init__(self):
        self.calls = []

    def __call__(self):
        self.calls.append(object())
        return b"This is feed #%d" % len(self.calls)


class TestCachedFeed(DatabaseTest):

    def test_fetch(self):
        # Verify that CachedFeed.fetch looks in the database for a
        # matching CachedFeed
        #
        # If a new feed needs to be generated, this is done by calling
        # a hook function, and the result is stored in the database.

        work = self._work()
        lane = self._lane()

        class Mock(CachedFeed):
            # Mock all of the helper methods, which are tested
            # separately below.

            @classmethod
            def _prepare_keys(cls, *args):
                cls._prepare_keys_called_with = args
                return cls._keys
            # _prepare_keys always returns this named tuple. Manipulate its
            # members to test different bits of fetch().
            _keys = CachedFeed.CachedFeedKeys(
                feed_type="mock type",
                library=self._default_library,
                work=work,
                lane_id=lane.id,
                unique_key="unique key",
                facets_key=u'facets',
                pagination_key=u'pagination',
            )

            @classmethod
            def max_cache_age(cls, *args):
                cls.max_cache_age_called_with = args
                return cls.MAX_CACHE_AGE
            # max_cache_age always returns whatever value is stored here.
            MAX_CACHE_AGE = 42

            @classmethod
            def _should_refresh(cls, *args):
                cls._should_refresh_called_with = args
                return cls.SHOULD_REFRESH
            # _should_refresh always returns whatever value is stored here.
            SHOULD_REFRESH = True
        m = Mock.fetch

        def clear_helpers():
            "Clear out the records of calls made to helper methods."
            Mock._prepare_keys_called_with = None
            Mock.max_cache_age_called_with = None
            Mock._should_refresh_called_with = None
        clear_helpers()

        # Define the hook function that is called whenever
        # we need to generate a feed.
        refresher = MockFeedGenerator()

        # The first time we call fetch(), we end up with a CachedFeed.
        worklist = object()
        facets = object()
        pagination = object()
        max_age = object()
        result1 = m(
            self._db, worklist, facets, pagination, refresher, max_age,
            raw=True
        )
        now = datetime.datetime.utcnow()
        assert isinstance(result1, CachedFeed)

        # The content of the CachedFeed comes from refresher(). It was
        # converted to Unicode. (Verifying the unicode() call may seem
        # like a small thing, but it means a refresher method can
        # return an OPDSFeed object.)
        eq_(u"This is feed #1", result1.content)

        # The timestamp is recent.
        timestamp1 = result1.timestamp
        assert (now - timestamp1).total_seconds() < 2

        # Relevant information from the named tuple returned by
        # _prepare_keys made it into the CachedFeed.
        k = Mock._keys
        eq_(k.feed_type, result1.type)
        eq_(k.lane_id, result1.lane_id)
        eq_(k.unique_key, result1.unique_key)
        eq_(unicode(k.facets_key), result1.facets)
        eq_(unicode(k.pagination_key), result1.pagination)

        # Now let's verify that the helper methods were called with the
        # right arguments.

        # We called _prepare_keys with all the necessary information
        # to create a named tuple.
        eq_(
            (self._db, worklist, facets, pagination),
            Mock._prepare_keys_called_with,
        )

        # We then called max_cache_age on the WorkList, the page
        # type, and the max_age object passed in to fetch().
        eq_(
            (worklist, "mock type", facets, max_age),
            Mock.max_cache_age_called_with,
        )

        # Then we called _should_refresh with the feed retrieved from
        # the database (which was None), and the return value of
        # max_cache_age.
        eq_(
            (None, 42),
            Mock._should_refresh_called_with
        )

        # Since _should_refresh is hard-coded to return True, we then
        # called refresher() to generate a feed and created a new
        # CachedFeed in the database.

        # Now let's try the same thing again. This time, there's a
        # CachedFeed already in the database, but our mocked
        # _should_refresh() is hard-coded to always return True, so
        # refresher() will be called again.
        clear_helpers()
        result2 = m(
            self._db, worklist, facets, pagination, refresher, max_age,
            raw=True
        )

        # The CachedFeed from before was reused.
        eq_(result2, result1)

        # But its .content has been updated.
        eq_("This is feed #2", result2.content)
        timestamp2 = result2.timestamp
        assert timestamp2 > timestamp1

        # Since there was a matching CachedFeed in the database
        # already, that CachedFeed was passed into _should_refresh --
        # previously this value was None.
        eq_(
            (result1, 42),
            Mock._should_refresh_called_with
        )

        # Now try the scenario where the feed does not need to be refreshed.
        clear_helpers()
        Mock.SHOULD_REFRESH = False
        result3 = m(
            self._db, worklist, facets, pagination, refresher, max_age,
            raw=True
        )

        # Not only do we have the same CachedFeed as before, but its
        # timestamp and content are unchanged.
        eq_(result3, result2)
        eq_("This is feed #2", result3.content)
        eq_(timestamp2, result3.timestamp)

        # If max_age ends up zero, we don't check for the existence of a
        # cached feed before forging ahead.
        Mock.MAX_CACHE_AGE = 0
        clear_helpers()
        m(
            self._db, worklist, facets, pagination, refresher, max_age,
            raw=True
        )

        # A matching CachedFeed exists in the database, but we didn't
        # even look for it, because we knew we'd be looking it up
        # again after feed generation.
        eq_(
            (None, 0),
            Mock._should_refresh_called_with
        )

    def test_no_race_conditions(self):
        # Why do we look up a CachedFeed again after feed generation?
        # Well, let's see what happens if someone else messes around
        # with the CachedFeed object _while the refresher is running_.
        #
        # This is a race condition that happens in real life. Rather
        # than setting up a multi-threaded test, we can have the
        # refresher itself simulate a background modification by
        # messing around with the CachedFeed object we know will
        # eventually be returned.
        #
        # The most up-to-date feed always wins, so background
        # modifications will take effect only if they made the
        # CachedFeed look _newer_ than the foreground process does.
        facets = Facets.default(self._default_library)
        pagination = Pagination.default()
        wl = WorkList()
        wl.initialize(self._default_library)

        m = CachedFeed.fetch

        # In this case, two simulated threads try to create the same
        # CachedFeed at the same time. We end up with a single
        # CachedFeed containing the result of the last code that ran.
        def simultaneous_refresher():
            # This refresher method simulates another thread creating
            # a CachedFeed for this feed while this thread's
            # refresher is running.
            def other_thread_refresher():
                return "Another thread made a feed."
            m(
                self._db, wl, facets, pagination, other_thread_refresher, 0,
                raw=True
            )

            return "Then this thread made a feed."

        # This will call simultaneous_refresher(), which will call
        # CachedFeed.fetch() _again_, which will call
        # other_thread_refresher().
        result = m(
            self._db, wl, facets, pagination, simultaneous_refresher, 0,
            raw=True
        )

        # We ended up with a single CachedFeed containing the
        # latest information.
        eq_([result], self._db.query(CachedFeed).all())
        eq_("Then this thread made a feed.", result.content)

        # If two threads contend for an existing CachedFeed, the one that
        # sets CachedFeed.timestamp to the later value wins.
        #
        # Here, the other thread wins by setting .timestamp on the
        # existing CachedFeed to a date in the future.
        now = datetime.datetime.utcnow()
        tomorrow = now + datetime.timedelta(days=1)
        yesterday = now - datetime.timedelta(days=1)
        def tomorrow_vs_now():
            result.content = "Someone in the background set tomorrow's content."
            result.timestamp = tomorrow
            return "Today's content can't compete."
        tomorrow_result = m(
            self._db, wl, facets, pagination, tomorrow_vs_now, 0, raw=True
        )
        eq_(tomorrow_result, result)
        eq_("Someone in the background set tomorrow's content.",
            tomorrow_result.content)
        eq_(tomorrow_result.timestamp, tomorrow)

        # Here, the other thread sets .timestamp to a date in the past, and
        # it loses out to the (apparently) newer feed.
        def yesterday_vs_now():
            result.content = "Someone in the background set yesterday's content."
            result.timestamp = yesterday
            return "Today's content is fresher."
        now_result = m(
            self._db, wl, facets, pagination, yesterday_vs_now, 0, raw=True
        )

        # We got the same CachedFeed we've been getting this whole
        # time, but the outdated data set by the 'background thread'
        # has been fixed.
        eq_(result, now_result)
        eq_("Today's content is fresher.", result.content)
        assert result.timestamp > yesterday

        # This shouldn't happen, but if the CachedFeed's timestamp or
        # content are *cleared out* in the background, between the
        # time the CacheFeed is fetched and the time the refresher
        # finishes, then we don't know what's going on and we don't
        # take chances. We create a whole new CachedFeed object for
        # the updated version of the feed.

        # First, try the situation where .timestamp is cleared out in
        # the background.
        def timestamp_cleared_in_background():
            result.content = "Someone else sets content and clears timestamp."
            result.timestamp = None

            return "Non-weird content."
        result2 = m(
            self._db, wl, facets, pagination, timestamp_cleared_in_background,
            0, raw=True
        )
        now = datetime.datetime.utcnow()

        # result2 is a brand new CachedFeed.
        assert result2 != result
        eq_("Non-weird content.", result2.content)
        assert (now - result2.timestamp).total_seconds() < 2

        # We let the background process do whatever it wants to do
        # with the old one.
        eq_("Someone else sets content and clears timestamp.", result.content)
        eq_(None, result.timestamp)

        # Next, test the situation where .content is cleared out.
        def content_cleared_in_background():
            result2.content = None
            result2.timestamp = tomorrow

            return "Non-weird content."
        result3 = m(
            self._db, wl, facets, pagination, content_cleared_in_background, 0,
            raw=True
        )
        now = datetime.datetime.utcnow()

        # Again, a brand new CachedFeed.
        assert result3 != result2
        assert result3 != result
        eq_("Non-weird content.", result3.content)
        assert (now - result3.timestamp).total_seconds() < 2

        # Again, we let the background process have the old one for
        # whatever weird thing it wants to do.
        eq_(None, result2.content)
        eq_(tomorrow, result2.timestamp)

    def test_response_format(self):
        # Verify that fetch() can be told to return an appropriate
        # OPDSFeedResponse object. This is the default behavior, since
        # it preserves some useful information that would otherwise be
        # lost.
        facets = Facets.default(self._default_library)
        pagination = Pagination.default()
        wl = WorkList()
        wl.initialize(self._default_library)

        def refresh():
            return "Here's a feed."

        private=object()
        r = CachedFeed.fetch(
            self._db, wl, facets, pagination, refresh, max_age=102,
            private=private
        )
        assert isinstance(r, OPDSFeedResponse)
        eq_(200, r.status_code)
        eq_(OPDSFeed.ACQUISITION_FEED_TYPE, r.content_type)
        eq_(102, r.max_age)
        eq_("Here's a feed.", r.data)

        # The extra argument `private`, not used by CachedFeed.fetch, was
        # passed on to the OPDSFeedResponse constructor.
        eq_(private, r.private)

        # The CachedFeed was created; just not returned.
        cf = self._db.query(CachedFeed).one()
        eq_("Here's a feed.", cf.content)

        # Try it again as a cache hit.
        r = CachedFeed.fetch(
            self._db, wl, facets, pagination, refresh, max_age=102,
            private=private
        )
        assert isinstance(r, OPDSFeedResponse)
        eq_(200, r.status_code)
        eq_(OPDSFeed.ACQUISITION_FEED_TYPE, r.content_type)
        eq_(102, r.max_age)
        eq_("Here's a feed.", r.data)

        # If we tell CachedFeed to cache its feed 'forever', that only
        # applies to the _database_ cache. The client is told to cache
        # the feed for the default period.
        r = CachedFeed.fetch(
            self._db, wl, facets, pagination, refresh,
            max_age=CachedFeed.CACHE_FOREVER, private=private
        )
        assert isinstance(r, OPDSFeedResponse)
        eq_(OPDSFeed.DEFAULT_MAX_AGE, r.max_age)


    # Tests of helper methods.

    def test_feed_type(self):
        # Verify that a WorkList or a Facets object can determine the
        # value to be stored in CachedFeed.type, with Facets taking
        # priority.
        class DontCare(object):
            CACHED_FEED_TYPE = None

        class WorkList(object):
            CACHED_FEED_TYPE = "from worklist"

        class Facets(object):
            CACHED_FEED_TYPE = "from facets"

        m = CachedFeed.feed_type

        # The default type is PAGE_TYPE.
        eq_(CachedFeed.PAGE_TYPE, m(None, None))
        eq_(CachedFeed.PAGE_TYPE, m(DontCare, DontCare))

        # If `worklist` has an opinion and `facets` doesn't, we use that.
        eq_("from worklist", m(WorkList, None))
        eq_("from worklist", m(WorkList, DontCare))

        # If `facets` has an opinion`, it is always used.
        eq_("from facets", m(DontCare, Facets))
        eq_("from facets", m(None, Facets))
        eq_("from facets", m(WorkList, Facets))

    def test_max_cache_age(self):
        m = CachedFeed.max_cache_age

        # If override is provided, that value is always used.
        eq_(60, m(None, None, None, 60))
        eq_(60, m(None, None, None, datetime.timedelta(minutes=1)))

        # Otherwise, the faceting object gets a chance to weigh in.
        class MockFacets(object):
            max_cache_age = 22
        facets = MockFacets()
        eq_(22, m(None, "feed type", facets=facets))

        # If there is no override and the faceting object doesn't
        # care, CachedFeed.max_cache_age depends on
        # WorkList.max_cache_age. This method can return a few
        # different data types.
        class MockWorklist(object):
            def max_cache_age(self, type):
                return dict(
                    number=1,
                    timedelta=datetime.timedelta(seconds=2),
                    expensive=CachedFeed.CACHE_FOREVER,
                    dont_cache=None,
                )[type]

        # The result is always either a number of seconds or
        # CACHE_FOREVER.
        wl = MockWorklist()
        eq_(1, m(wl, "number", None))
        eq_(2, m(wl, "timedelta", None))
        eq_(0, m(wl, "dont_cache", None))
        eq_(CachedFeed.CACHE_FOREVER, m(wl, "expensive", None))

        # The faceting object still takes precedence, assuming it has
        # an opinion.
        facets.max_cache_age = None
        eq_(CachedFeed.CACHE_FOREVER, m(wl, "expensive", facets))

        facets.max_cache_age = 22
        eq_(22, m(wl, "expensive", facets))

        # And an override takes precedence over that.
        eq_(60, m(wl, "expensive", facets, 60))

    def test__prepare_keys(self):
        # Verify the method that turns WorkList, Facets, and Pagination
        # into a unique set of values for CachedFeed fields.

        # First, prepare some mock classes.
        class MockCachedFeed(CachedFeed):
            feed_type_called_with = None
            @classmethod
            def feed_type(cls, worklist, facets):
                cls.feed_type_called_with = (worklist, facets)
                return "mock type"

        class MockFacets(object):
            query_string = b"facets query string"

        class MockPagination(object):
            query_string = b"pagination query string"

        m = MockCachedFeed._prepare_keys
        # A WorkList of some kind is required.
        assert_raises_regexp(
            ValueError, "Cannot prepare a CachedFeed without a WorkList.",
            m, self._db, None, MockFacets, MockPagination
        )

        # Basic Lane case, no facets or pagination.
        lane = self._lane()

        # The response object is a named tuple. feed_type, library and
        # lane_id are the only members set.
        keys = m(self._db, lane, None, None)
        eq_("mock type", keys.feed_type)
        eq_(lane.library, keys.library)
        eq_(None, keys.work)
        eq_(lane.id, keys.lane_id)
        eq_(None, keys.unique_key)
        eq_(u'', keys.facets_key)
        eq_(u'', keys.pagination_key)

        # When pagination and/or facets are available, facets_key and
        # pagination_key are set appropriately.
        keys = m(self._db, lane, MockFacets, MockPagination)
        eq_(u"facets query string", keys.facets_key)
        eq_(u"pagination query string", keys.pagination_key)

        # Now we can check that feed_type was obtained by passing
        # `worklist` and `facets` into MockCachedFeed.feed_type.
        eq_("mock type", keys.feed_type)
        eq_((lane, MockFacets), MockCachedFeed.feed_type_called_with)

        # When a WorkList is used instead of a Lane, keys.lane_id is None
        # but keys.unique_key is set to worklist.unique_key.
        worklist = WorkList()
        worklist.initialize(
            library=self._default_library, display_name="wl",
            languages=["eng", "spa"], audiences=[Classifier.AUDIENCE_CHILDREN]
        )

        keys = m(self._db, worklist, None, None)
        eq_("mock type", keys.feed_type)
        eq_(worklist.get_library(self._db), keys.library)
        eq_(None, keys.work)
        eq_(None, keys.lane_id)
        eq_("wl-eng,spa-Children", keys.unique_key)
        eq_(keys.unique_key, worklist.unique_key)
        eq_(u'', keys.facets_key)
        eq_(u'', keys.pagination_key)

        # When a WorkList is associated with a specific .work,
        # that information is included as keys.work.
        work = object()
        worklist.work = work
        keys = m(self._db, worklist, None, None)
        eq_(work, keys.work)

    def test__should_refresh(self):
        # Test the algorithm that tells whether a CachedFeed is stale.
        m = CachedFeed._should_refresh

        # If there's no CachedFeed, we must always refresh.
        eq_(True, m(None, object()))

        class MockCachedFeed(object):
            def __init__(self, timestamp):
                self.timestamp = timestamp

        now = datetime.datetime.utcnow()

        # This feed was generated five minutes ago.
        five_minutes_old = MockCachedFeed(
            now - datetime.timedelta(minutes=5)
        )

        # This feed was generated a thousand years ago.
        ancient = MockCachedFeed(
            now - datetime.timedelta(days=1000*365)
        )

        # If we intend to cache forever, then even a thousand-year-old
        # feed shouldn't be refreshed.
        eq_(False, m(ancient, CachedFeed.CACHE_FOREVER))

        # Otherwise, it comes down to a date comparison.

        # If we're caching a feed for ten minutes, then the
        # five-minute-old feed should not be refreshed.
        eq_(False, m(five_minutes_old, 600))

        # If we're caching a feed for only a few seconds (or not at all),
        # then the five-minute-old feed should be refreshed.
        eq_(True, m(five_minutes_old, 0))
        eq_(True, m(five_minutes_old, 1))


    # Realistic end-to-end tests.

    def test_lifecycle_with_lane(self):
        facets = Facets.default(self._default_library)
        pagination = Pagination.default()
        lane = self._lane(u"My Lane", languages=['eng','chi'])

        # Fetch a cached feed from the database. It comes out updated.
        refresher = MockFeedGenerator()
        args = (self._db, lane, facets, pagination, refresher)
        feed = CachedFeed.fetch(*args, max_age=0, raw=True)
        eq_("This is feed #1", feed.content)

        eq_(pagination.query_string, feed.pagination)
        eq_(facets.query_string, feed.facets)
        eq_(lane.id, feed.lane_id)

        # Fetch it again, with a high max_age, and it's cached!
        feed = CachedFeed.fetch(*args, max_age=1000, raw=True)
        eq_("This is feed #1", feed.content)

        # Fetch it with a low max_age, and it gets updated again.
        feed = CachedFeed.fetch(*args, max_age=0, raw=True)
        eq_("This is feed #2", feed.content)

        # The special constant CACHE_FOREVER means it's always cached.
        feed = CachedFeed.fetch(*args, max_age=CachedFeed.CACHE_FOREVER, raw=True)
        eq_("This is feed #2", feed.content)

    def test_lifecycle_with_worklist(self):
        facets = Facets.default(self._default_library)
        pagination = Pagination.default()
        lane = WorkList()
        lane.initialize(self._default_library)

        # Fetch a cached feed from the database. It comes out updated.
        refresher = MockFeedGenerator()
        args = (self._db, lane, facets, pagination, refresher)
        feed = CachedFeed.fetch(*args, max_age=0, raw=True)
        eq_("This is feed #1", feed.content)

        eq_(pagination.query_string, feed.pagination)
        eq_(facets.query_string, feed.facets)
        eq_(None, feed.lane_id)
        eq_(lane.unique_key, feed.unique_key)

        # Fetch it again, with a high max_age, and it's cached!
        feed = CachedFeed.fetch(*args, max_age=1000, raw=True)
        eq_("This is feed #1", feed.content)

        # Fetch it with a low max_age, and it gets updated again.
        feed = CachedFeed.fetch(*args, max_age=0, raw=True)
        eq_("This is feed #2", feed.content)

        # The special constant CACHE_FOREVER means it's always cached.
        feed = CachedFeed.fetch(
            *args, max_age=CachedFeed.CACHE_FOREVER, raw=True
        )
        eq_("This is feed #2", feed.content)
