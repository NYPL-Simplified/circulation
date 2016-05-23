import requests
import urlparse

from ..problem_details import INTEGRATION_ERROR

class RequestTimedOut(Exception):
    """A timeout exception that can be represented as a problem
    detail document.
    """

    def __init__(self, url, response):
        self.url = url
        self.hostname = urlparse.urlparse(url).netloc

    def as_problem_detail_document(self, debug):
        template = "Request timed out while accessing %s"
        if debug:
            message = template % self.url
        else:
            message = template % self.hostname
        if debug:
            instance = self.url
        else:
            instance = None
        return INTEGRATION_ERROR.detailed(
            detail=message, title="Timeout", instance=instance
        )


class HTTP(object):
    """A helper for the `requests` module."""

    @classmethod
    def request_with_timeout(cls, http_method, url, *args, **kwargs):
        """Call requests.request and turn a timeout into a RequestTimedOut
        exception.
        """
        if not 'timeout' in kwargs:
            kwargs['timeout'] = 20
        kwargs['timeout'] = 0.001
        try:
            response = requests.request(http_method, url, *args, **kwargs)
        except requests.exceptions.Timeout, e:
            # Wrap the requests-specific Timeout exception 
            # in a generic RequestTimedOut exception.
            raise RequestTimedOut(url, e.message)
        return response

    @classmethod
    def get_with_timeout(cls, url, *args, **kwargs):
        return cls.request_with_timeout("POST", url, *args, **kwargs)

    @classmethod
    def post_with_timeout(cls, url, *args, **kwargs):
        return cls.request_with_timeout("POST", url, *args, **kwargs)
