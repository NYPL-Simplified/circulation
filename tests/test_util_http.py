import requests
from util.http import (
    HTTP, 
    RemoteIntegrationException,
    RequestNetworkException,
    RequestTimedOut,
)
from nose.tools import (
    assert_raises_regexp,
    eq_, 
    set_trace
)
from testing import MockRequestsResponse

class TestHTTP(object):

    def test_request_with_timeout_success(self):

        def fake_200_response(*args, **kwargs):
            return MockRequestsResponse(200, content="Success!")

        response = HTTP._request_with_timeout(
            "the url", fake_200_response, "a", "b"
        )
        eq_(200, response.status_code)
        eq_("Success!", response.content)

    def test_request_with_timeout_failure(self):

        def immediately_timeout(*args, **kwargs):
            raise requests.exceptions.Timeout("I give up")

        assert_raises_regexp(
            RequestTimedOut,
            "Timeout accessing http://url/: I give up",
            HTTP._request_with_timeout, "http://url/", immediately_timeout,
            "a", "b"
        )

    def test_request_with_network_failure(self):

        def immediately_fail(*args, **kwargs):
            raise requests.exceptions.ConnectionError("a disaster")

        assert_raises_regexp(
            RequestNetworkException,
            "Network error accessing http://url/: a disaster",
            HTTP._request_with_timeout, "http://url/", immediately_fail,
            "a", "b"
        )

    def test_request_with_response_indicative_of_failure(self):

        def fake_500_response(*args, **kwargs):
            return MockRequestsResponse(500, content="Failure!")

        assert_raises_regexp(
            RemoteIntegrationException,
            "Network error accessing http://url/: Got status code 500 from external server.",
            HTTP._request_with_timeout, "http://url/", fake_500_response,
            "a", "b"
        )

class TestRequestTimedOut(object):

    def test_as_problem_detail_document(self):
        exception = RequestTimedOut("http://url/", "I give up")

        debug_detail = exception.as_problem_detail_document(debug=True)
        eq_("Timeout", debug_detail.title)
        eq_('The server made a request to http://url/, and that request timed out.', debug_detail.detail)

        # If we're not in debug mode, we hide the URL we accessed and just
        # show the hostname.
        standard_detail = exception.as_problem_detail_document(debug=False)
        eq_("The server made a request to url, and that request timed out.", standard_detail.detail)

        # The status code corresponding to an upstream timeout is 502.
        document, status_code, headers = standard_detail.response
        eq_(502, status_code)


class TestRequestNetworkException(object):

    def test_as_problem_detail_document(self):
        exception = RequestNetworkException("http://url/", "Colossal failure")

        debug_detail = exception.as_problem_detail_document(debug=True)
        eq_("Network failure contacting external service", debug_detail.title)
        eq_('The server experienced a network error while accessing http://url/.', debug_detail.detail)

        # If we're not in debug mode, we hide the URL we accessed and just
        # show the hostname.
        standard_detail = exception.as_problem_detail_document(debug=False)
        eq_("The server experienced a network error while accessing url.", standard_detail.detail)

        # The status code corresponding to an upstream timeout is 502.
        document, status_code, headers = standard_detail.response
        eq_(502, status_code)
