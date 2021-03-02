import requests
import json
from ...util.http import (
    HTTP,
    BadResponseException,
    RemoteIntegrationException,
    RequestNetworkException,
    RequestTimedOut,
    INTEGRATION_ERROR,
)
from nose.tools import (
    assert_raises_regexp,
    eq_,
    set_trace
)
from ...testing import MockRequestsResponse
from ...util.problem_detail import ProblemDetail
from ...problem_details import INVALID_INPUT

class TestHTTP(object):

    def test_series(self):
        m = HTTP.series
        eq_("2xx", m(201))
        eq_("3xx", m(399))
        eq_("5xx", m(500))


    def test_request_with_timeout_success(self):

        called_with = None
        def fake_200_response(*args, **kwargs):
            # The HTTP method and URL are passed in the order
            # requests.request would expect.
            eq_(("GET", "http://url/"), args)

            # Keyword arguments to _request_with_timeout are passed in
            # as-is.
            eq_("value", kwargs["kwarg"])

            # A default timeout is added.
            eq_(20, kwargs['timeout'])
            return MockRequestsResponse(200, content="Success!")

        response = HTTP._request_with_timeout(
            "http://url/", fake_200_response, "GET", kwarg="value"
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
            "Network error contacting http://url/: a disaster",
            HTTP._request_with_timeout, "http://url/", immediately_fail,
            "a", "b"
        )

    def test_request_with_response_indicative_of_failure(self):

        def fake_500_response(*args, **kwargs):
            return MockRequestsResponse(500, content="Failure!")

        assert_raises_regexp(
            BadResponseException,
            "Bad response from http://url/: Got status code 500 from external server.",
            HTTP._request_with_timeout, "http://url/", fake_500_response,
            "a", "b"
        )

    def test_allowed_response_codes(self):
        """Test our ability to raise BadResponseException when
        an HTTP-based integration does not behave as we'd expect.
        """

        def fake_401_response(*args, **kwargs):
            return MockRequestsResponse(401, content="Weird")

        def fake_200_response(*args, **kwargs):
            return MockRequestsResponse(200, content="Hurray")

        url = "http://url/"
        m = HTTP._request_with_timeout

        # By default, every code except for 5xx codes is allowed.
        response = m(url, fake_401_response)
        eq_(401, response.status_code)

        # You can say that certain codes are specifically allowed, and
        # all others are forbidden.
        assert_raises_regexp(
            BadResponseException,
            "Bad response.*Got status code 401 from external server, but can only continue on: 200, 201.",
            m, url, fake_401_response,
            allowed_response_codes=[201, 200]
        )

        response = m(url, fake_401_response, allowed_response_codes=[401])
        response = m(url, fake_401_response, allowed_response_codes=["4xx"])

        # In this way you can even raise an exception on a 200 response code.
        assert_raises_regexp(
            BadResponseException,
            "Bad response.*Got status code 200 from external server, but can only continue on: 401.",
            m, url, fake_200_response,
            allowed_response_codes=[401]
        )

        # You can say that certain codes are explicitly forbidden, and
        # all others are allowed.
        assert_raises_regexp(
            BadResponseException,
            "Bad response.*Got status code 401 from external server, cannot continue.",
            m, url, fake_401_response,
            disallowed_response_codes=[401]
        )

        assert_raises_regexp(
            BadResponseException,
            "Bad response.*Got status code 200 from external server, cannot continue.",
            m, url, fake_200_response,
            disallowed_response_codes=["2xx", 301]
        )

        response = m(url, fake_401_response,
                     disallowed_response_codes=["2xx"])
        eq_(401, response.status_code)

        # The exception can be turned into a useful problem detail document.
        exc = None
        try:
            m(url, fake_200_response,
              disallowed_response_codes=["2xx"])
        except Exception as e:
            exc = e
            pass
        assert exc is not None

        debug_doc = exc.as_problem_detail_document(debug=True)

        # 502 is the status code to be returned if this integration error
        # interrupts the processing of an incoming HTTP request, not the
        # status code that caused the problem.
        #
        eq_(502, debug_doc.status_code)
        eq_("Bad response", debug_doc.title)
        eq_('The server made a request to http://url/, and got an unexpected or invalid response.', debug_doc.detail)
        eq_('Bad response from http://url/: Got status code 200 from external server, cannot continue.\n\nResponse content: Hurray', debug_doc.debug_message)

        no_debug_doc = exc.as_problem_detail_document(debug=False)
        eq_("Bad response", no_debug_doc.title)
        eq_('The server made a request to url, and got an unexpected or invalid response.', no_debug_doc.detail)
        eq_(None, no_debug_doc.debug_message)

    def test_unicode_converted_to_utf8(self):
        """Any Unicode that sneaks into the URL, headers or body is
        converted to UTF-8.
        """
        class ResponseGenerator(object):
            def __init__(self):
                self.requests = []

            def response(self, *args, **kwargs):
                self.requests.append((args, kwargs))
                return MockRequestsResponse(200, content="Success!")

        generator = ResponseGenerator()
        url = "http://foo"
        response = HTTP._request_with_timeout(
            url, generator.response, "POST",
            headers = { "unicode header": "unicode value"},
            data="unicode data"
        )
        [(args, kwargs)] = generator.requests
        url, method = args
        headers = kwargs['headers']
        data = kwargs['data']

        # All the Unicode data was converted to bytes before being sent
        # "over the wire".
        for k,v in list(headers.items()):
            assert isinstance(k, bytes)
            assert isinstance(v, bytes)
        assert isinstance(data, bytes)

    def test_debuggable_request(self):
        class Mock(HTTP):
            @classmethod
            def _request_with_timeout(cls, *args, **kwargs):
                cls.called_with = (args, kwargs)
                return "response"
        def mock_request(*args, **kwargs):
            response = MockRequestsResponse(200, "Success!")
            return response

        Mock.debuggable_request(
            "method", "url", make_request_with=mock_request, key="value"
        )
        (args, kwargs) = Mock.called_with
        eq_(args, ("url", mock_request, "method"))
        eq_(kwargs["key"], "value")
        eq_(kwargs["process_response_with"], Mock.process_debuggable_response)

    def test_process_debuggable_response(self):
        """Test a method that gives more detailed information when a
        problem happens.
        """
        m = HTTP.process_debuggable_response
        success = MockRequestsResponse(200, content="Success!")
        eq_(success, m("url", success))

        success = MockRequestsResponse(302, content="Success!")
        eq_(success, m("url", success))

        # An error is turned into a detailed ProblemDetail
        error = MockRequestsResponse(500, content="Error!")
        problem = m("url", error)
        assert isinstance(problem, ProblemDetail)
        eq_(INTEGRATION_ERROR.uri, problem.uri)
        eq_("500 response from integration server: 'Error!'", problem.detail)

        content, status_code, headers = INVALID_INPUT.response
        error = MockRequestsResponse(status_code, headers, content)
        problem = m("url", error)
        assert isinstance(problem, ProblemDetail)
        eq_(INTEGRATION_ERROR.uri, problem.uri)
        eq_("Remote service returned a problem detail document: %r" % content,
            problem.detail)
        eq_(content, problem.debug_message)
        # You can force a response to be treated as successful by
        # passing in its response code as allowed_response_codes.
        eq_(error, m("url", error, allowed_response_codes=[400]))
        eq_(error, m("url", error, allowed_response_codes=["400"]))
        eq_(error, m("url", error, allowed_response_codes=['4xx']))

class TestRemoteIntegrationException(object):

    def test_with_service_name(self):
        """You don't have to provide a URL when creating a
        RemoteIntegrationException; you can just provide the service
        name.
        """
        exc = RemoteIntegrationException(
            "Unreliable Service",
            "I just can't handle your request right now."
        )

        # Since only the service name is provided, there are no details to
        # elide in the non-debug version of a problem detail document.
        debug_detail = exc.document_detail(debug=True)
        other_detail = exc.document_detail(debug=False)
        eq_(debug_detail, other_detail)

        eq_('The server tried to access Unreliable Service but the third-party service experienced an error.',
            debug_detail
        )

class TestBadResponseException(object):

    def test_helper_constructor(self):
        response = MockRequestsResponse(102, content="nonsense")
        exc = BadResponseException.from_response(
            "http://url/", "Terrible response, just terrible", response
        )

        # Turn the exception into a problem detail document, and it's full
        # of useful information.
        doc, status_code, headers = exc.as_problem_detail_document(debug=True).response
        doc = json.loads(doc)

        eq_('Bad response', doc['title'])
        eq_('The server made a request to http://url/, and got an unexpected or invalid response.', doc['detail'])
        eq_(
            'Bad response from http://url/: Terrible response, just terrible\n\nStatus code: 102\nContent: nonsense',
            doc['debug_message']
        )

        # Unless debug is turned off, in which case none of that
        # information is present.
        doc, status_code, headers = exc.as_problem_detail_document(debug=False).response
        assert 'debug_message' not in json.loads(doc)

    def test_bad_status_code_helper(object):
        response = MockRequestsResponse(500, content="Internal Server Error!")
        exc = BadResponseException.bad_status_code(
            "http://url/", response
        )
        doc, status_code, headers = exc.as_problem_detail_document(debug=True).response
        doc = json.loads(doc)

        assert doc['debug_message'].startswith("Bad response from http://url/: Got status code 500 from external server, cannot continue.")

    def test_as_problem_detail_document(self):
        exception = BadResponseException(
            "http://url/", "What even is this",
            debug_message="some debug info"
        )
        document = exception.as_problem_detail_document(debug=True)
        eq_(502, document.status_code)
        eq_("Bad response", document.title)
        eq_("The server made a request to http://url/, and got an unexpected or invalid response.",
            document.detail
        )
        eq_("Bad response from http://url/: What even is this\n\nsome debug info", document.debug_message)


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
        eq_("Network failure contacting third-party service", debug_detail.title)
        eq_('The server experienced a network error while contacting http://url/.', debug_detail.detail)

        # If we're not in debug mode, we hide the URL we accessed and just
        # show the hostname.
        standard_detail = exception.as_problem_detail_document(debug=False)
        eq_("The server experienced a network error while contacting url.", standard_detail.detail)

        # The status code corresponding to an upstream timeout is 502.
        document, status_code, headers = standard_detail.response
        eq_(502, status_code)
