import requests
import urlparse

from problem_details import INTEGRATION_ERROR

class RequestTimedOut(requests.exceptions.Timeout):
    """A timeout exception that can be represented as a problem
    detail document.
    """

    def __init__(self, url, response):
        self.url = url
        self.hostname = urlparse.urlparse(url).netloc
        super(RequestTimedOut, self).__init__(response)

    def __str__(self):
        return "Timeout accessing %s: %s" % (self.url, self.message)

    def as_problem_detail_document(self, debug):
        template = "Request timed out while accessing %s"
        if debug:
            message = template % self.url
        else:
            message = template % self.hostname
        return INTEGRATION_ERROR.detailed(detail=message, title="Timeout")


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
        m = requests.request
        return cls._request_with_timeout(
            url, requests.request, http_method, url, *args, **kwargs
        )

    @classmethod
    def _request_with_timeout(cls, url, m, *args, **kwargs):
        """Call some kind of method and turn a timeout into a RequestTimedOut
        exception.

        The core of `request_with_timeout` made easy to test.
        """
        if not 'timeout' in kwargs:
            kwargs['timeout'] = 20
        try:
            response = m(*args, **kwargs)
        except requests.exceptions.Timeout, e:
            # Wrap the requests-specific Timeout exception 
            # in a generic RequestTimedOut exception.
            raise RequestTimedOut(url, e.message)
        return response

