import requests
from util.http import HTTP, RequestTimedOut
from nose.tools import (
    assert_raises_regexp,
    eq_, 
    set_trace
)


class TestHTTP(object):

    def test_request_with_timeout_success(self):

        def succeed(*args, **kwargs):
            return True

        eq_(True, HTTP._request_with_timeout("the url", succeed, "a", "b"))

    def test_request_with_timeout_failure(self):

        def immediately_timeout(*args, **kwargs):
            raise requests.exceptions.Timeout("I give up")

        assert_raises_regexp(
            RequestTimedOut,
            "Timeout accessing http://url/: I give up",
            HTTP._request_with_timeout, "http://url/", immediately_timeout,
            "a", "b"
        )


class TestRequestTimedOut(object):

    def test_as_problem_detail_document(self):
        exception = RequestTimedOut("http://url/", "I give up")

        debug_detail = exception.as_problem_detail_document(debug=True)
        eq_("Timeout", debug_detail.title)
        eq_('Request timed out while accessing http://url/', debug_detail.detail)

        # If we're not in debug mode, we hide the URL we accessed and just
        # show the hostname.
        standard_detail = exception.as_problem_detail_document(debug=False)
        eq_("Request timed out while accessing url", standard_detail.detail)

        # The status code corresponding to an upstream timeout is 502.
        document, status_code, headers = standard_detail.response
        eq_(502, status_code)
