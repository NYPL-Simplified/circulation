"""Simple helper library for generating problem detail documents.

As per http://datatracker.ietf.org/doc/draft-ietf-appsawg-http-problem/
"""
import json as j

JSON_MEDIA_TYPE = "application/problem+json"


def json(type, status, title, detail=None, instance=None):
    d = dict(type=type, title=title, status=status)
    if detail:
        d['detail'] = detail
    if instance:
        d['instance'] = instance
    return j.dumps(d)

class ProblemDetail(object):

    """A common type of problem."""

    def __init__(self, uri, status_code=None, title=None, detail=None,
                 instance=None):
        self.uri = uri
        self.title = title
        self.status_code=status_code
        self.detail = detail
        self.instance = instance

    @property
    def response(self):
        """Create a Flask-style response."""
        return (
            json(
                self.uri, self.status_code, self.title, self.detail, 
                self.instance
            ),
            self.status_code,
            { "Content-Type": "application/api-problem+json"}
        )

    def detailed(self, detail, status_code=None, title=None, instance=None):
        """Create a ProblemDetail for a specific occurance of a problem."""
        return ProblemDetail(
            self.uri, status_code or self.status_code, title or self.title, 
            detail, instance)
