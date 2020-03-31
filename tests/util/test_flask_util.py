# encoding: utf-8
"""Test functionality of util/flask_util.py."""

from nose.tools import (
    eq_,
    set_trace,
)
import datetime
import time
from flask import Response as FlaskResponse
from wsgiref.handlers import format_date_time
from ...util.flask_util import (
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
        eq_(1002, response.max_age)
        assert isinstance(response, FlaskResponse)
        eq_(401, response.status_code)
        eq_("content", response.data)
        eq_(True, response.direct_passthrough)

        # Response.headers is tested in more detail below.
        headers = response.headers
        eq_("value", headers['Header'])
        assert 'Cache-Control' in headers
        assert 'Expires' in headers

    def test_headers(self):
        # First test cases where the response should not be cached at all
        def assert_not_cached(max_age):
            headers = Response(max_age=max_age).headers
            eq_("private, no-cache", headers['Cache-Control'])
            assert 'Expires' not in headers
        assert_not_cached(max_age=None)
        assert_not_cached(max_age=0)
        assert_not_cached(max_age="Not a number")

        # Test the case where the response is public but should not be cached.
        headers = Response(max_age=0, public=True).headers
        eq_("public, no-cache" % public, headers['Cache-Control'])

        # Test the case where the response _should_ be cached.
        max_age = 60*60*24*12
        obj = Response(max_age=max_age)

        headers = obj.headers
        cc = headers['Cache-Control']
        eq_(cc, 'public, no-transform, max-age=1036800, s-maxage=518400')

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
        eq_(expires[:17], expect_expires_string[:17])

    def test_unicode(self):
        # You can easily convert a Response object to Unicode
        # for use in a test.
        obj = Response(u"some data")
        eq_(u"some data", unicode(obj))


class TestOPDSFeedResponse(object):
    """Test the OPDS feed-specific specialization of Response."""
    def test_defaults(self):
        # OPDSFeedResponse provides reasonable defaults for
        # `mimetype` and `max_age`.
        c = OPDSFeedResponse

        use_defaults = c("a feed")
        eq_(OPDSFeed.ACQUISITION_FEED_TYPE, use_defaults.content_type)
        eq_(OPDSFeed.DEFAULT_MAX_AGE, use_defaults.max_age)

        # Flask Response.mimetype is the same as content_type but
        # with parameters removed.
        eq_(OPDSFeed.ATOM_TYPE, use_defaults.mimetype)

        # These defaults can be overridden.
        override_defaults = c(
            "a feed", 200, dict(Header="value"), "mime/type",
            "content/type", True, 1002
        )
        eq_(1002, override_defaults.max_age)

        # In Flask code, if mimetype and content_type conflict,
        # content_type takes precedence.
        eq_("content/type", override_defaults.content_type)
        eq_("content/type", override_defaults.mimetype)

        # A max_age of zero is retained, not replaced by the default.
        do_not_cache = c(max_age=0)
        eq_(0, do_not_cache.max_age)

