# encoding: utf-8
"""Test functionality of util/flask_util.py."""

import datetime
import time
from flask import Response as FlaskResponse
from wsgiref.handlers import format_date_time
from ...util.flask_util import (
    OPDSEntryResponse,
    OPDSFeedResponse,
    Response,
)
from ...util.opds_writer import OPDSFeed

class TestResponse(object):

    def test_constructor(self):
        response = Response(
            "content", 401, dict(Header="value"), "mime/type",
            "content/type", True, 1002
        )
        assert 1002 == response.max_age
        assert isinstance(response, FlaskResponse)
        assert 401 == response.status_code
        assert "content" == response.data
        assert True == response.direct_passthrough

        # Response.headers is tested in more detail below.
        headers = response.headers
        assert "value" == headers['Header']
        assert 'Cache-Control' in headers
        assert 'Expires' in headers

    def test_headers(self):
        # First, test cases where the response should be private and
        # not cached. These are the kinds of settings used for error
        # messages.
        def assert_not_cached(max_age):
            headers = Response(max_age=max_age).headers
            assert "private, no-cache" == headers['Cache-Control']
            assert 'Expires' not in headers
        assert_not_cached(max_age=None)
        assert_not_cached(max_age=0)
        assert_not_cached(max_age="Not a number")

        # Test the case where the response is public but should not be cached.
        headers = Response(max_age=0, private=False).headers
        assert "public, no-cache" == headers['Cache-Control']

        # Test the case where the response _should_ be cached.
        max_age = 60*60*24*12
        obj = Response(max_age=max_age)

        headers = obj.headers
        cc = headers['Cache-Control']
        assert cc == 'public, no-transform, max-age=1036800, s-maxage=518400'

        # We expect the Expires header to look basically like this.
        expect_expires = (
            datetime.datetime.utcnow() + datetime.timedelta(seconds=max_age)
        )
        expect_expires_string = format_date_time(
            time.mktime(expect_expires.timetuple())
        )

        # We'll only check the date part of the Expires header, to
        # minimize the changes of spurious failures based on
        # unfortunate timing.
        expires = headers['Expires']
        assert expires[:17] == expect_expires_string[:17]

        # It's possible to have a response that is private but should
        # be cached. The feed of a patron's current loans is a good
        # example.
        response = Response(max_age=30, private=True)
        cache_control = response.headers['Cache-Control']
        assert 'private' in cache_control
        assert 'max-age=30' in cache_control

    def test_unicode(self):
        # You can easily convert a Response object to Unicode
        # for use in a test.
        obj = Response(u"some data")
        assert u"some data" == unicode(obj)


class TestOPDSFeedResponse(object):
    """Test the OPDS feed-specific specialization of Response."""
    def test_defaults(self):
        # OPDSFeedResponse provides reasonable defaults for
        # `mimetype` and `max_age`.
        c = OPDSFeedResponse

        use_defaults = c("a feed")
        assert OPDSFeed.ACQUISITION_FEED_TYPE == use_defaults.content_type
        assert OPDSFeed.DEFAULT_MAX_AGE == use_defaults.max_age

        # Flask Response.mimetype is the same as content_type but
        # with parameters removed.
        assert OPDSFeed.ATOM_TYPE == use_defaults.mimetype

        # These defaults can be overridden.
        override_defaults = c(
            "a feed", 200, dict(Header="value"), "mime/type",
            "content/type", True, 1002
        )
        assert 1002 == override_defaults.max_age

        # In Flask code, if mimetype and content_type conflict,
        # content_type takes precedence.
        assert "content/type" == override_defaults.content_type
        assert "content/type" == override_defaults.mimetype

        # A max_age of zero is retained, not replaced by the default.
        do_not_cache = c(max_age=0)
        assert 0 == do_not_cache.max_age

class TestOPDSEntryResponse(object):
    """Test the OPDS entry-specific specialization of Response."""
    def test_defaults(self):
        # OPDSEntryResponse provides a reasonable defaults for
        # `mimetype`.
        c = OPDSEntryResponse

        use_defaults = c("an entry")
        assert OPDSFeed.ENTRY_TYPE == use_defaults.content_type

        # Flask Response.mimetype is the same as content_type but
        # with parameters removed.
        assert OPDSFeed.ATOM_TYPE == use_defaults.mimetype

        # These defaults can be overridden.
        override_defaults = c("an entry", content_type= "content/type")
        assert "content/type" == override_defaults.content_type
        assert "content/type" == override_defaults.mimetype
