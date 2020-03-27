# encoding: utf-8
"""Test functionality of util/flask_util.py."""

from nose.tools import (
    eq_,
    set_trace,
)
import datetime
import time
from flask import Response
from wsgiref.handlers import format_date_time
from ...util.flask_util import Responselike

class TestResponselike(object):

    def test_response(self):
        obj = Responselike(
            "content", 401, dict(Header="value"), "mime/type",
            "content/type", True, 1002
        )
        response = obj.response
        eq_(1002, obj.max_age)
        assert isinstance(response, Response)
        eq_(401, response.status_code)
        eq_("content", response.data)
        eq_(True, response.direct_passthrough)

        # Responselike.headers is tested in more detail below.
        headers = response.headers
        eq_("value", headers['Header'])
        assert 'Cache-Control' in headers
        assert 'Expires' in headers

    def test_headers(self):
        # First test cases where the response should not be cached at all
        def assert_not_cached(max_age):
            headers = Responselike(max_age=max_age).headers
            eq_("private, no-cache", headers['Cache-Control'])
            assert 'Expires' not in headers
        assert_not_cached(max_age=None)
        assert_not_cached(max_age=0)
        assert_not_cached(max_age="Not a number")

        # Test the case where the response _should_ be cached.
        max_age = 60*60*24*12
        obj = Responselike(max_age=max_age)

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
        # You can easily convert a Responselike object to Unicode
        # for use in a test.
        obj = Responselike(u"some data")
        eq_(u"some data", unicode(obj))
