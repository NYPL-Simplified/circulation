import logging

import requests
from urllib.parse import urlparse
from flask_babel import lazy_gettext as _
from .problem_detail import (
    ProblemDetail as pd,
    JSON_MEDIA_TYPE as PROBLEM_DETAIL_JSON_MEDIA_TYPE,
)

INTEGRATION_ERROR = pd(
      "http://librarysimplified.org/terms/problem/remote-integration-failed",
      502,
      _("Third-party service failed."),
      _("A third-party service has failed."),
)

class IntegrationException(Exception):
    """An exception that happens when the site's connection to a
    third-party service is broken.

    This may be because communication failed
    (RemoteIntegrationException), or because local configuration is
    missing or obviously wrong (CannotLoadConfiguration).
    """

    def __init__(self, message, debug_message=None):
        """Constructor.

        :param message: The normal message passed to any Exception
        constructor.

        :param debug_message: An extra human-readable explanation of the
        problem, shown to admins but not to patrons. This may include
        instructions on what bits of the integration configuration might need
        to be changed.

        For example, an API key might be wrong, or the API key might
        be correct but the API provider might not have granted that
        key enough permissions.
        """
        super(IntegrationException, self).__init__(message)
        self.debug_message = debug_message


class RemoteIntegrationException(IntegrationException):
    """An exception that happens when we try and fail to communicate
    with a third-party service over HTTP.
    """

    title = _("Failure contacting external service")
    detail = _("The server tried to access %(service)s but the third-party service experienced an error.")
    internal_message = "Error accessing %s: %s"

    def __init__(self, url_or_service, message, debug_message=None):
        """Indicate that a remote integration has failed.

        `param url_or_service` The name of the service that failed
           (e.g. "Overdrive"), or the specific URL that had the problem.
        """
        if (url_or_service and
            any(url_or_service.startswith(x) for x in ('http:', 'https:'))):
            self.url = url_or_service
            self.service = urlparse(url_or_service).netloc
        else:
            self.url = self.service = url_or_service
        if not debug_message:
            debug_message = self.internal_message % (self.url, message)
        super(RemoteIntegrationException, self).__init__(message, debug_message)

    def __str__(self):
        message = super(RemoteIntegrationException, self).__str__()
        return self.internal_message % (self.url, message)

    def document_detail(self, debug=True):
        if debug:
            return _(str(self.detail), service=self.url)
        return _(str(self.detail), service=self.service)

    def document_debug_message(self, debug=True):
        if debug:
            return _(str(self.detail), service=self.url)
        return None

    def as_problem_detail_document(self, debug):
        return INTEGRATION_ERROR.detailed(
            detail=self.document_detail(debug), title=self.title,
            debug_message=self.document_debug_message(debug)
        )

class BadResponseException(RemoteIntegrationException):
    """The request seemingly went okay, but we got a bad response."""
    title = _("Bad response")
    detail = _("The server made a request to %(service)s, and got an unexpected or invalid response.")
    internal_message = "Bad response from %s: %s"

    BAD_STATUS_CODE_MESSAGE = "Got status code %s from external server, cannot continue."

    def __init__(self, url_or_service, message, debug_message=None, status_code=None):
        """Indicate that a remote integration has failed.

        `param url_or_service` The name of the service that failed
           (e.g. "Overdrive"), or the specific URL that had the problem.
        """
        super(BadResponseException, self).__init__(url_or_service, message, debug_message)
        # to be set to 500, etc.
        self.status_code = status_code

    def document_debug_message(self, debug=True):
        if debug:
            msg = str(self)
            if self.debug_message:
                msg += "\n\n" + self.debug_message
            return msg
        return None

    @classmethod
    def from_response(cls, url, message, response):
        """Helper method to turn a `requests` Response object into
        a BadResponseException.
        """
        if isinstance(response, tuple):
            # The response has been unrolled into a (status_code,
            # headers, body) 3-tuple.
            status_code, headers, content = response
        else:
            status_code = response.status_code
            content = response.content
        if content and isinstance(content, bytes):
            content = content.decode("utf-8")
        return BadResponseException(
            url, message,
            status_code=status_code,
            debug_message="Status code: %s\nContent: %s" % (
                status_code,
                content,
            )
        )

    @classmethod
    def bad_status_code(cls, url, response):
        """The response is bad because the status code is wrong."""
        message = cls.BAD_STATUS_CODE_MESSAGE % response.status_code
        return cls.from_response(
            url,
            message,
            response,
        )


class RequestNetworkException(RemoteIntegrationException,
                              requests.exceptions.RequestException):
    """An exception from the requests module that can be represented as
    a problem detail document.
    """
    title = _("Network failure contacting third-party service")
    detail = _("The server experienced a network error while contacting %(service)s.")
    internal_message = "Network error contacting %s: %s"


class RequestTimedOut(RequestNetworkException, requests.exceptions.Timeout):
    """A timeout exception that can be represented as a problem
    detail document.
    """

    title = _("Timeout")
    detail = _("The server made a request to %(service)s, and that request timed out.")
    internal_message = "Timeout accessing %s: %s"


class HTTP(object):
    """A helper for the `requests` module."""

    @classmethod
    def get_with_timeout(cls, url, *args, **kwargs):
        """Make a GET request with timeout handling."""
        return cls.request_with_timeout("GET", url, *args, **kwargs)

    @classmethod
    def post_with_timeout(cls, url, payload, *args, **kwargs):
        """Make a POST request with timeout handling."""
        kwargs['data'] = payload
        return cls.request_with_timeout("POST", url, *args, **kwargs)

    @classmethod
    def put_with_timeout(cls, url, payload, *args, **kwargs):
        """Make a PUT request with timeout handling."""
        kwargs['data'] = payload
        return cls.request_with_timeout("PUT", url, *args, **kwargs)

    @classmethod
    def request_with_timeout(cls, http_method, url, *args, **kwargs):
        """Call requests.request and turn a timeout into a RequestTimedOut
        exception.
        """
        return cls._request_with_timeout(
            url, requests.request, http_method, *args, **kwargs
        )

    @classmethod
    def _request_with_timeout(cls, url, make_request_with, *args, **kwargs):
        """Call some kind of method and turn a timeout into a RequestTimedOut
        exception.

        The core of `request_with_timeout` made easy to test.

        :param url: Make the request to this URL.
        :param make_request_with: A function that actually makes the
            HTTP request.
        :param args: Positional arguments for the request function.
        :param kwargs: Keyword arguments for the request function.
        """
        process_response_with = kwargs.pop(
            'process_response_with', cls._process_response
        )
        allowed_response_codes = kwargs.pop('allowed_response_codes', [])
        disallowed_response_codes = kwargs.pop('disallowed_response_codes', [])
        verbose = kwargs.pop('verbose', False)

        if not 'timeout' in kwargs:
            kwargs['timeout'] = 20

        # Unicode data can't be sent over the wire. Convert it
        # to UTF-8.
        if 'data' in kwargs and isinstance(kwargs['data'], str):
            kwargs['data'] = kwargs.get('data').encode("utf8")
        if 'headers' in kwargs:
            headers = kwargs['headers']
            new_headers = {}
            for k, v in list(headers.items()):
                if isinstance(k, str):
                    k = k.encode("utf8")
                if isinstance(v, str):
                    v = v.encode("utf8")
                new_headers[k] = v
            kwargs['headers'] = new_headers

        try:
            if verbose:
                logging.info("Sending request to %s: args %r kwargs %r",
                             url, args, kwargs)
            if len(args) == 1:
                # requests.request takes two positional arguments,
                # an HTTP method and a URL. In most cases, the URL
                # gets added on here. But if you do pass in both
                # arguments, it will still work.
                args = args + (url,)
            response = make_request_with(*args, **kwargs)
            if verbose:
                logging.info(
                    "Response from %s: %s %r %r",
                    url, response.status_code, response.headers,
                    response.content
                )
        except requests.exceptions.Timeout as e:
            # Wrap the requests-specific Timeout exception
            # in a generic RequestTimedOut exception.
            raise RequestTimedOut(url, e)
        except requests.exceptions.RequestException as e:
            # Wrap all other requests-specific exceptions in
            # a generic RequestNetworkException.
            raise RequestNetworkException(url, e)

        return process_response_with(
            url, response, allowed_response_codes, disallowed_response_codes
        )

    @classmethod
    def _process_response(cls, url, response, allowed_response_codes=None,
                          disallowed_response_codes=None):
        """Raise a RequestNetworkException if the response code indicates a
        server-side failure, or behavior so unpredictable that we can't
        continue.

        :param allowed_response_codes If passed, then only the responses with
            http status codes in this list are processed.  The rest generate
            BadResponseExceptions.
        :param disallowed_response_codes The values passed are added to 5xx, as
            http status codes that would generate BadResponseExceptions.
        """
        if allowed_response_codes:
            allowed_response_codes = list(map(str, allowed_response_codes))
            status_code_not_in_allowed = "Got status code %%s from external server, but can only continue on: %s." % (
                ", ".join(sorted(allowed_response_codes)),
            )
        if disallowed_response_codes:
            disallowed_response_codes = list(map(str, disallowed_response_codes))
        else:
            disallowed_response_codes = []

        code = response.status_code
        series = cls.series(code)
        code = str(code)

        if allowed_response_codes and (
                code in allowed_response_codes
                or series in allowed_response_codes
        ):
            # The code or series has been explicitly allowed. Allow
            # the request to be processed.
            return response

        error_message = None
        if (series == '5xx' or code in disallowed_response_codes
            or series in disallowed_response_codes
        ):
            # Unless explicitly allowed, the 5xx series always results in
            # an exception.
            error_message = BadResponseException.BAD_STATUS_CODE_MESSAGE
        elif (allowed_response_codes and not (
                code in allowed_response_codes
                or series in allowed_response_codes
        )):
            error_message = status_code_not_in_allowed
        if error_message:
            if response.content and isinstance(response.content, bytes):
                response_content = response.content.decode("utf-8")
            else:
                response_content = response.content
            raise BadResponseException(
                url,
                error_message % code,
                status_code=code,
                debug_message="Response content: %s" % response_content
            )
        return response

    @classmethod
    def series(cls, status_code):
        """Return the HTTP series for the given status code."""
        return "%sxx" % (int(status_code) // 100)

    @classmethod
    def debuggable_get(cls, url, **kwargs):
        """Make a GET request that returns a detailed problem
        detail document on error.
        """
        return cls.debuggable_request("GET", url, **kwargs)

    @classmethod
    def debuggable_post(cls, url, payload, **kwargs):
        """Make a POST request that returns a detailed problem
        detail document on error.
        """
        kwargs['data'] = payload
        return cls.debuggable_request("POST", url, **kwargs)

    @classmethod
    def debuggable_request(cls, http_method, url, make_request_with=None,
                           **kwargs):
        """Make a request that returns a detailed problem detail document on
        error, rather than a generic "an integration error occured"
        message.

        :param http_method: HTTP method to use when making the request.
        :param url: Make the request to this URL.
        :param make_request_with: A function that actually makes the
            HTTP request.
        :param kwargs: Keyword arguments for the make_request_with
            function.
        """
        logging.info("Making debuggable %s request to %s: kwargs %r",
                     http_method, url, kwargs)
        make_request_with = make_request_with or requests.request
        return cls._request_with_timeout(
            url, make_request_with, http_method,
            process_response_with=cls.process_debuggable_response,
            **kwargs
        )

    @classmethod
    def process_debuggable_response(cls, url, response, disallowed_response_codes=None, allowed_response_codes=None):
        """If there was a problem with an integration request,
        return an appropriate ProblemDetail. Otherwise, return the
        response to the original request.

        :param response: A Response object from the requests library.
        """

        allowed_response_codes = allowed_response_codes or ['2xx', '3xx']
        allowed_response_codes = list(map(str, allowed_response_codes))
        code = response.status_code
        series = cls.series(code)
        if str(code) in allowed_response_codes or series in allowed_response_codes:
            # Whether or not it looks like there's been a problem,
            # we've been told to let this response code through.
            return response

        content_type = response.headers.get('Content-Type')
        response_content = response.content
        if isinstance(response_content, bytes):
            response_content = response_content.decode("utf-8")
        if content_type == PROBLEM_DETAIL_JSON_MEDIA_TYPE:
            # The server returned a problem detail document. Wrap it
            # in a new document that represents the integration
            # failure.
            problem = INTEGRATION_ERROR.detailed(
                _('Remote service returned a problem detail document: %r') % (
                    response_content
                )
            )
            problem.debug_message = response_content
            return problem
        # There's been a problem. Return the message we got from the
        # server, verbatim.
        return INTEGRATION_ERROR.detailed(
            _("%s response from integration server: %r") % (
                response.status_code,
                response_content,
            )
        )
