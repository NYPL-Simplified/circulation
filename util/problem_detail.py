"""Simple helper library for generating problem detail documents.

As per http://datatracker.ietf.org/doc/draft-ietf-appsawg-http-problem/
"""
import json as j

JSON_MEDIA_TYPE = "application/api-problem+json"


def json(type, status, title, detail=None, instance=None, debug_message=None):
    d = dict(type=type, title=title, status=status)
    if detail:
        d['detail'] = detail
    if instance:
        d['instance'] = instance
    if debug_message:
        d['debug_message'] = debug_message
    return j.dumps(d)

class ProblemDetail(object):

    """A common type of problem."""

    def __init__(self, uri, status_code=None, title=None, detail=None,
                 instance=None, debug_message=None):
        self.uri = uri
        self.title = title
        self.status_code=status_code
        self.detail = detail
        self.instance = instance
        self.debug_message = debug_message

    @property
    def response(self):
        """Create a Flask-style response."""
        return (
            json(
                self.uri, self.status_code, self.title, self.detail, 
                self.instance, self.debug_message
            ),
            self.status_code or 400,
            { "Content-Type": JSON_MEDIA_TYPE}
        )

    def detailed(self, detail, status_code=None, title=None, instance=None,
                 debug_message=None):
        """Create a ProblemDetail for a more specific occurance of an existing
        ProblemDetail.

        The detailed error message will be shown to patrons.
        """
        return ProblemDetail(
            self.uri, status_code or self.status_code, title or self.title, 
            detail, instance, debug_message
        )

    def with_debug(self, debug_message, detail=None, status_code=None, 
                   title=None, instance=None):
        """Insert debugging information into a ProblemDetail.

        The original ProblemDetail's error message will be shown to
        patrons, but a more specific error message will be visible to
        those who inspect the problem document.
        """
        return ProblemDetail(
            self.uri, status_code or self.status_code, title or self.title, 
            detail or self.detail, instance, debug_message
        )
