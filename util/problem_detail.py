"""Simple helper library for generating problem detail documents.

As per http://datatracker.ietf.org/doc/draft-ietf-appsawg-http-problem/
"""
import json as j

JSON_MEDIA_TYPE = "application/problem+json"


def json(type, title, status, detail=None, instance=None):
    d = dict(type=type, title=title, status=status)
    if detail:
        d['detail'] = detail
    if instance:
        d['instance'] = instance
    return j.dumps(d)

class ProblemDetail(object):

    """A common type of problem."""

    def __init__(self, uri, title=None, status_code=None, detail=None,
                 instance=None):
        self.uri = uri
        self.title = title
        self.status_code=status_code
        self.detail = detail
        self.instance = instance

    def response(self):
        """Create a Flask response."""
        return json(
            self.uri, self.title, self.status_code, self.detail, 
            instance
        )

    def detailed(self, detail, title=None, status_code=None, instance=None):
        """Create a ProblemDetail for a specific occurance of a problem."""
        return ProblemDetail(
            self.uri, title or self.title, status_code or self.status_code, 
            detail, instance)
