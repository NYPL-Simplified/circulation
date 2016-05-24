from nose.tools import set_trace
import requests
import urlparse
from problem_detail import ProblemDetail as pd

INTEGRATION_ERROR = pd(
      "http://librarysimplified.org/terms/problem/remote-integration-failed",
      502,
      "Third-party service failed.",
      "A third-party service has failed.",
)

class RemoteIntegrationException(Exception):

    """An exception that happens when communicating with a third-party
    service.
    """
    title = "Network failure contacting external service"
    detail = "The server experienced a network error while accessing %s."
    internal_message = "Network error accessing %s: %s"

    def __init__(self, url, message, debug_message=None):
        super(RemoteIntegrationException, self).__init__(message)
        self.url = url
        self.hostname = urlparse.urlparse(url).netloc
        self.debug_message = debug_message

    def __str__(self):
        return self.internal_message % (self.url, self.message)

    def as_problem_detail_document(self, debug):
        if debug:
            message = self.detail % self.url
            debug_message = self.debug_message
        else:
            message = self.detail % self.hostname
            debug_message = None
        return INTEGRATION_ERROR.detailed(
            detail=message, title=self.title, debug_message=debug_message
        )

class MalformedResponseException(RemoteIntegrationException):
    """The request seemingly went okay, but we got a malformed response."""
    title = "Bad response"
    detail = "The server made a request to %s, and got an improperly formatted response."
    internal_message = "Malformed response from %s: %s"

class RequestNetworkException(RemoteIntegrationException,
                              requests.exceptions.RequestException):
    """An exception from the requests module that can be represented as
    a problem detail document.
    """
    pass

class RequestTimedOut(RequestNetworkException, requests.exceptions.Timeout):
    """A timeout exception that can be represented as a problem
    detail document.
    """

    title = "Timeout"
    detail = "The server made a request to %s, and that request timed out."
    internal_message = "Timeout accessing %s: %s"


class HTTP(object):
    """A helper for the `requests` module."""

    @classmethod
    def get_with_timeout(cls, url, *args, **kwargs):
        """Make a GET request with timeout handling."""
        return cls.request_with_timeout("GET", url, *args, **kwargs)

    @classmethod
    def post_with_timeout(cls, url, *args, **kwargs):
        """Make a POST request with timeout handling."""
        return cls.request_with_timeout("POST", url, *args, **kwargs)

    @classmethod
    def request_with_timeout(cls, http_method, url, *args, **kwargs):
        """Call requests.request and turn a timeout into a RequestTimedOut
        exception.
        """
        return cls._request_with_timeout(
            url, requests.request, http_method, url, *args, **kwargs
        )

    @classmethod
    def _request_with_timeout(cls, url, m, *args, **kwargs):
        """Call some kind of method and turn a timeout into a RequestTimedOut
        exception.

        The core of `request_with_timeout` made easy to test.
        """
        if kwargs.get('require_200'):
            require_200 = True
            del kwargs['require_200']
        else:
            require_200 = False

        if not 'timeout' in kwargs:
            kwargs['timeout'] = 20
        try:
            response = m(*args, **kwargs)
        except requests.exceptions.Timeout, e:
            # Wrap the requests-specific Timeout exception 
            # in a generic RequestTimedOut exception.
            raise RequestTimedOut(url, e.message)
        except requests.exceptions.RequestException, e:
            # Wrap all other requests-specific exceptions in
            # a generic RequestNetworkException.
            raise RequestNetworkException(url, e.message)

        cls._process_response(response, require_200)

    def _process_response(cls, response, require_200=False):
        """Raise a RequestNetworkException if the response code indicates a
        server-side failure, or behavior so unpredictable that we can't
        continue.
        """
        if require_200:
            bad_status_code = "Got status code %s from external server, expected status code 200."
        else:
            bad_status_code = "Got status code %s from external server."

        if ((response.status_code / 100 == 5)
            or (require_200 and response.status_code != 200)):
            raise RemoteIntegrationException(
                url,
                bad_status_code % response.status_code, 
                debug_message=response.content
            )
        return response

