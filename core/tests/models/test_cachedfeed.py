# encoding: utf-8
import pytest
import datetime
from ...classifier import Classifier
from ...lane import (
    Facets,
    Pagination,
    WorkList,
)
from ...model.cachedfeed import CachedFeed
from ...util.flask_util import OPDSFeedResponse
from ...util.opds_writer import OPDSFeed
from ...util.datetime_helpers import utc_now

class MockFeedGenerator(object):

    def __init__(self):
        self.calls = []

    def __call__(self):
        self.calls.append(object())
        return "This is feed #%d" % len(self.calls)


class TestCachedFeed:

    def test_fetch(self, db_session, create_work, create_lane, create_library):
        """
        GIVEN: Data that needs to be stored in a CachedFeed
        WHEN:  Calling CachedFeed.fetch
        THEN:  Data is pulled from database
        """
        # Verify that CachedFeed.fetch looks in the database for a
        # matching CachedFeed
        #
        # If a new feed needs to be generated, this is done by calling
        # a hook function, and the result is stored in the database.

        lane = create_lane(db_session)
        library = create_library(db_session)
        work = create_work(db_session)

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
                library=library,
                work=work,
                lane_id=lane.id,
                unique_key="unique key",
                facets_key='facets',
                pagination_key='pagination',
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
            db_session, worklist, facets, pagination, refresher, max_age,
            raw=True
        )
        now = utc_now()
        assert isinstance(result1, CachedFeed)

        # The content of the CachedFeed comes from refresher(). It was
        # converted to Unicode. (Verifying the unicode() call may seem
        # like a small thing, but it means a refresher method can
        # return an OPDSFeed object.)
        assert "This is feed #1" == result1.content

        # The timestamp is recent.
        timestamp1 = result1.timestamp
        assert (now - timestamp1).total_seconds() < 2

        # Relevant information from the named tuple returned by
        # _prepare_keys made it into the CachedFeed.
        k = Mock._keys
        assert k.feed_type == result1.type
        assert k.lane_id == result1.lane_id
        assert k.unique_key == result1.unique_key
        assert str(k.facets_key) == result1.facets
        assert str(k.pagination_key) == result1.pagination

        # Now let's verify that the helper methods were called with the
        # right arguments.

        # We called _prepare_keys with all the necessary information
        # to create a named tuple.
        assert (
            (db_session, worklist, facets, pagination) ==
            Mock._prepare_keys_called_with)

        # We then called max_cache_age on the WorkList, the page
        # type, and the max_age object passed in to fetch().
        assert (
            (worklist, "mock type", facets, max_age) ==
            Mock.max_cache_age_called_with)

        # Then we called _should_refresh with the feed retrieved from
        # the database (which was None), and the return value of
        # max_cache_age.
        assert (
            (None, 42) ==
            Mock._should_refresh_called_with)

        # Since _should_refresh is hard-coded to return True, we then
        # called refresher() to generate a feed and created a new
        # CachedFeed in the database.

        # Now let's try the same thing again. This time, there's a
        # CachedFeed already in the database, but our mocked
        # _should_refresh() is hard-coded to always return True, so
        # refresher() will be called again.
        clear_helpers()
        result2 = m(
            db_session, worklist, facets, pagination, refresher, max_age,
            raw=True
        )

        # The CachedFeed from before was reused.
        assert result2 == result1

        # But its .content has been updated.
        assert "This is feed #2" == result2.content
        timestamp2 = result2.timestamp
        assert timestamp2 > timestamp1

        # Since there was a matching CachedFeed in the database
        # already, that CachedFeed was passed into _should_refresh --
        # previously this value was None.
        assert (
            (result1, 42) ==
            Mock._should_refresh_called_with)

        # Now try the scenario where the feed does not need to be refreshed.
        clear_helpers()
        Mock.SHOULD_REFRESH = False
        result3 = m(
            db_session, worklist, facets, pagination, refresher, max_age,
            raw=True
        )

        # Not only do we have the same CachedFeed as before, but its
        # timestamp and content are unchanged.
        assert result3 == result2
        assert "This is feed #2" == result3.content
        assert timestamp2 == result3.timestamp

        # If max_age ends up zero, we don't check for the existence of a
        # cached feed before forging ahead.
        Mock.MAX_CACHE_AGE = 0
        clear_helpers()
        m(
            db_session, worklist, facets, pagination, refresher, max_age,
            raw=True
        )

        # A matching CachedFeed exists in the database, but we didn't
        # even look for it, because we knew we'd be looking it up
        # again after feed generation.
        assert (
            (None, 0) ==
            Mock._should_refresh_called_with)

    def test_no_race_conditions(self, db_session, create_library):
        """
        GIVEN: A CachedFeed.fetch
        WHEN:  There is a race condition
        THEN:  The most up to date CachedFeed is returned
        """
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
        library = create_library(db_session)
        facets = Facets.default(library)
        pagination = Pagination.default()
        wl = WorkList()
        wl.initialize(library)

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
                db_session, wl, facets, pagination, other_thread_refresher, 0,
                raw=True
            )

            return "Then this thread made a feed."

        # This will call simultaneous_refresher(), which will call
        # CachedFeed.fetch() _again_, which will call
        # other_thread_refresher().
        result = m(
            db_session, wl, facets, pagination, simultaneous_refresher, 0,
            raw=True
        )

        # We ended up with a single CachedFeed containing the
        # latest information.
        assert [result] == db_session.query(CachedFeed).all()
        assert "Then this thread made a feed." == result.content

        # If two threads contend for an existing CachedFeed, the one that
        # sets CachedFeed.timestamp to the later value wins.
        #
        # Here, the other thread wins by setting .timestamp on the
        # existing CachedFeed to a date in the future.
        now = utc_now()
        tomorrow = now + datetime.timedelta(days=1)
        yesterday = now - datetime.timedelta(days=1)
        def tomorrow_vs_now():
            result.content = "Someone in the background set tomorrow's content."
            result.timestamp = tomorrow
            return "Today's content can't compete."
        tomorrow_result = m(
            db_session, wl, facets, pagination, tomorrow_vs_now, 0, raw=True
        )
        assert tomorrow_result == result
        assert ("Someone in the background set tomorrow's content." ==
            tomorrow_result.content)
        assert tomorrow_result.timestamp == tomorrow

        # Here, the other thread sets .timestamp to a date in the past, and
        # it loses out to the (apparently) newer feed.
        def yesterday_vs_now():
            result.content = "Someone in the background set yesterday's content."
            result.timestamp = yesterday
            return "Today's content is fresher."
        now_result = m(
            db_session, wl, facets, pagination, yesterday_vs_now, 0, raw=True
        )

        # We got the same CachedFeed we've been getting this whole
        # time, but the outdated data set by the 'background thread'
        # has been fixed.
        assert result == now_result
        assert "Today's content is fresher." == result.content
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
            db_session, wl, facets, pagination, timestamp_cleared_in_background,
            0, raw=True
        )
        now = utc_now()

        # result2 is a brand new CachedFeed.
        assert result2 != result
        assert "Non-weird content." == result2.content
        assert (now - result2.timestamp).total_seconds() < 2

        # We let the background process do whatever it wants to do
        # with the old one.
        assert "Someone else sets content and clears timestamp." == result.content
        assert None == result.timestamp

        # Next, test the situation where .content is cleared out.
        def content_cleared_in_background():
            result2.content = None
            result2.timestamp = tomorrow

            return "Non-weird content."
        result3 = m(
            db_session, wl, facets, pagination, content_cleared_in_background, 0,
            raw=True
        )
        now = utc_now()

        # Again, a brand new CachedFeed.
        assert result3 != result2
        assert result3 != result
        assert "Non-weird content." == result3.content
        assert (now - result3.timestamp).total_seconds() < 2

        # Again, we let the background process have the old one for
        # whatever weird thing it wants to do.
        assert None == result2.content
        assert tomorrow == result2.timestamp

    def test_response_format(self, db_session, create_library):
        """
        GIVEN: A CachedFeed
        WHEN:  Calling CachedFeed.fetch()
        THEN:  An OPDSFeedResponse is returned
        """
        # Verify that fetch() can be told to return an appropriate
        # OPDSFeedResponse object. This is the default behavior, since
        # it preserves some useful information that would otherwise be
        # lost.
        library = create_library(db_session)
        facets = Facets.default(library)
        pagination = Pagination.default()
        wl = WorkList()
        wl.initialize(library)

        def refresh():
            return "Here's a feed."

        private=object()
        r = CachedFeed.fetch(
            db_session, wl, facets, pagination, refresh, max_age=102,
            private=private
        )
        assert isinstance(r, OPDSFeedResponse)
        assert 200 == r.status_code
        assert OPDSFeed.ACQUISITION_FEED_TYPE == r.content_type
        assert "Here's a feed." == str(r)

        # The extra argument `private`, not used by CachedFeed.fetch, was
        # passed on to the OPDSFeedResponse constructor.
        assert private == r.private

        # The CachedFeed was created; just not returned.
        cf = db_session.query(CachedFeed).one()
        assert "Here's a feed." == cf.content

        # Try it again as a cache hit.
        r = CachedFeed.fetch(
            db_session, wl, facets, pagination, refresh, max_age=102,
            private=private
        )
        assert isinstance(r, OPDSFeedResponse)
        assert 200 == r.status_code
        assert OPDSFeed.ACQUISITION_FEED_TYPE == r.content_type
        assert "Here's a feed." == str(r)

        # If we tell CachedFeed to cache its feed 'forever', that only
        # applies to the _database_ cache. The client is told to cache
        # the feed for the default period.
        r = CachedFeed.fetch(
            db_session, wl, facets, pagination, refresh,
            max_age=CachedFeed.CACHE_FOREVER, private=private
        )
        assert isinstance(r, OPDSFeedResponse)

        # If the Library associated with the WorkList used in the feed
        # has root lanes, `private` is always set to True, even if we
        # asked for the opposite.
        from unittest.mock import PropertyMock, patch
        from ...model import Library
        Library._has_root_lane_cache[library.id] = True
        r = CachedFeed.fetch(
            db_session, wl, facets, pagination, refresh,
            private=False
        )
        assert isinstance(r, OPDSFeedResponse)
        assert True == r.private

    # Tests of helper methods.

    def test_feed_type(self):
        """
        GIVEN: A WorkList and Facets
        WHEN:  Calling CachedFeed.feed_type()
        THEN:  Facets has feed type precedence over WorkList
        """
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
        assert CachedFeed.PAGE_TYPE == m(None, None)
        assert CachedFeed.PAGE_TYPE == m(DontCare, DontCare)

        # If `worklist` has an opinion and `facets` doesn't, we use that.
        assert "from worklist" == m(WorkList, None)
        assert "from worklist" == m(WorkList, DontCare)

        # If `facets` has an opinion`, it is always used.
        assert "from facets" == m(DontCare, Facets)
        assert "from facets" == m(None, Facets)
        assert "from facets" == m(WorkList, Facets)

    def test_max_cache_age(self):
        """
        GIVEN: A CachedFeed
        WHEN:  Checking CachedFeed.max_cache_age
        THEN:  The correct duration is returned
        """
        m = CachedFeed.max_cache_age

        # If override is provided, that value is always used.
        assert 60 == m(None, None, None, 60)
        assert 60 == m(None, None, None, datetime.timedelta(minutes=1))

        # Otherwise, the faceting object gets a chance to weigh in.
        class MockFacets(object):
            max_cache_age = 22
        facets = MockFacets()
        assert 22 == m(None, "feed type", facets=facets)

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
        assert 1 == m(wl, "number", None)
        assert 2 == m(wl, "timedelta", None)
        assert 0 == m(wl, "dont_cache", None)
        assert CachedFeed.CACHE_FOREVER == m(wl, "expensive", None)

        # The faceting object still takes precedence, assuming it has
        # an opinion.
        facets.max_cache_age = None
        assert CachedFeed.CACHE_FOREVER == m(wl, "expensive", facets)

        facets.max_cache_age = 22
        assert 22 == m(wl, "expensive", facets)

        # And an override takes precedence over that.
        assert 60 == m(wl, "expensive", facets, 60)

    def test__prepare_keys(self, db_session, create_lane, create_library):
        """
        GIVEN: A WorkList, Facets, and Pagination
        WHEN:  Creating a CachedFeed
        THEN:  Unique values are created for CachedFeed fields
        """
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
        with pytest.raises(ValueError) as excinfo:
            m(db_session, None, MockFacets, MockPagination)
        assert "Cannot prepare a CachedFeed without a WorkList." in str(excinfo.value)

        # Basic Lane case, no facets or pagination.
        lane = create_lane(db_session)

        # The response object is a named tuple. feed_type, library and
        # lane_id are the only members set.
        keys = m(db_session, lane, None, None)
        assert "mock type" == keys.feed_type
        assert lane.library == keys.library
        assert None == keys.work
        assert lane.id == keys.lane_id
        assert None == keys.unique_key
        assert '' == keys.facets_key
        assert '' == keys.pagination_key

        # When pagination and/or facets are available, facets_key and
        # pagination_key are set appropriately.
        keys = m(db_session, lane, MockFacets, MockPagination)
        assert "facets query string" == keys.facets_key
        assert "pagination query string" == keys.pagination_key

        # Now we can check that feed_type was obtained by passing
        # `worklist` and `facets` into MockCachedFeed.feed_type.
        assert "mock type" == keys.feed_type
        assert (lane, MockFacets) == MockCachedFeed.feed_type_called_with

        # When a WorkList is used instead of a Lane, keys.lane_id is None
        # but keys.unique_key is set to worklist.unique_key.
        worklist = WorkList()
        worklist.initialize(
            library=create_library(db_session), display_name="wl",
            languages=["eng", "spa"], audiences=[Classifier.AUDIENCE_CHILDREN]
        )

        keys = m(db_session, worklist, None, None)
        assert "mock type" == keys.feed_type
        assert worklist.get_library(db_session) == keys.library
        assert None == keys.work
        assert None == keys.lane_id
        assert "wl-eng,spa-Children" == keys.unique_key
        assert keys.unique_key == worklist.unique_key
        assert '' == keys.facets_key
        assert '' == keys.pagination_key

        # When a WorkList is associated with a specific .work,
        # that information is included as keys.work.
        work = object()
        worklist.work = work
        keys = m(db_session, worklist, None, None)
        assert work == keys.work

    def test__should_refresh(self):
        """
        GIVEN: A CachedFeed
        WHEN:  Checking if the CachedFeed is stale
        THEN:  CachedFeed is refreshed if is not CACHED_FOREVER or less than the time delta
        """
        # Test the algorithm that tells whether a CachedFeed is stale.
        m = CachedFeed._should_refresh

        # If there's no CachedFeed, we must always refresh.
        assert True == m(None, object())

        class MockCachedFeed(object):
            def __init__(self, timestamp):
                self.timestamp = timestamp

        now = utc_now()

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
        assert False == m(ancient, CachedFeed.CACHE_FOREVER)

        # Otherwise, it comes down to a date comparison.

        # If we're caching a feed for ten minutes, then the
        # five-minute-old feed should not be refreshed.
        assert False == m(five_minutes_old, 600)

        # If we're caching a feed for only a few seconds (or not at all),
        # then the five-minute-old feed should be refreshed.
        assert True == m(five_minutes_old, 0)
        assert True == m(five_minutes_old, 1)


    # Realistic end-to-end tests.

    def test_lifecycle_with_lane(self, db_session, create_lane, create_library):
        """
        GIVEN: A CachedFeed with Lane
        WHEN:  Fetching the CachedFeed
        THEN:  CachedFeed is fetched from database or updated
        """
        library = create_library(db_session)
        facets = Facets.default(library)
        pagination = Pagination.default()
        lane = create_lane(db_session, "My Lane", languages=['eng','chi'])

        # Fetch a cached feed from the database. It comes out updated.
        refresher = MockFeedGenerator()
        args = (db_session, lane, facets, pagination, refresher)
        feed = CachedFeed.fetch(*args, max_age=0, raw=True)
        assert "This is feed #1" == feed.content

        assert pagination.query_string == feed.pagination
        assert facets.query_string == feed.facets
        assert lane.id == feed.lane_id

        # Fetch it again, with a high max_age, and it's cached!
        feed = CachedFeed.fetch(*args, max_age=1000, raw=True)
        assert "This is feed #1" == feed.content

        # Fetch it with a low max_age, and it gets updated again.
        feed = CachedFeed.fetch(*args, max_age=0, raw=True)
        assert "This is feed #2" == feed.content

        # The special constant CACHE_FOREVER means it's always cached.
        feed = CachedFeed.fetch(*args, max_age=CachedFeed.CACHE_FOREVER, raw=True)
        assert "This is feed #2" == feed.content

    def test_lifecycle_with_worklist(self, db_session, create_library):
        """
        GIVEN: A CachedFeed with WorkList
        WHEN:  Fetching the CachedFeed
        THEN:  CachedFeed is fetched from database or updated
        """
        library = create_library(db_session)
        facets = Facets.default(library)
        pagination = Pagination.default()
        lane = WorkList()
        lane.initialize(library)

        # Fetch a cached feed from the database. It comes out updated.
        refresher = MockFeedGenerator()
        args = (db_session, lane, facets, pagination, refresher)
        feed = CachedFeed.fetch(*args, max_age=0, raw=True)
        assert "This is feed #1" == feed.content

        assert pagination.query_string == feed.pagination
        assert facets.query_string == feed.facets
        assert None == feed.lane_id
        assert lane.unique_key == feed.unique_key

        # Fetch it again, with a high max_age, and it's cached!
        feed = CachedFeed.fetch(*args, max_age=1000, raw=True)
        assert "This is feed #1" == feed.content

        # Fetch it with a low max_age, and it gets updated again.
        feed = CachedFeed.fetch(*args, max_age=0, raw=True)
        assert "This is feed #2" == feed.content

        # The special constant CACHE_FOREVER means it's always cached.
        feed = CachedFeed.fetch(
            *args, max_age=CachedFeed.CACHE_FOREVER, raw=True
        )
        assert "This is feed #2" == feed.content
