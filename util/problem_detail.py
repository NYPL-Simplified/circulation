"""Simple helper library for generating problem detail documents.

As per http://datatracker.ietf.org/doc/draft-ietf-appsawg-http-problem/
"""
import json as j
import logging

from flask_babel import LazyString
from nose.tools import set_trace

from ..exceptions import BaseError

JSON_MEDIA_TYPE = "application/api-problem+json"


def json(type, status, title, detail=None, instance=None, debug_message=None):
    d = dict(type=type, title=unicode(title), status=status)
    if detail:
        d['detail'] = unicode(detail)
    if instance:
        d['instance'] = instance
    if debug_message:
        d['debug_message'] = debug_message
    return j.dumps(d)


class ProblemDetail(object):

    """A common type of problem."""

    JSON_MEDIA_TYPE = JSON_MEDIA_TYPE

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

        # Title and detail must be LazyStrings from Flask-Babel that are
        # localized when they are first used as strings.
        if title and not isinstance(title, LazyString):
            logging.warn("\"%s\" has not been internationalized" % title)
        if detail and not isinstance(detail, LazyString):
            logging.warn("\"%s\" has not been internationalized" % detail)

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
            detail or self.detail, instance or self.instance, debug_message
        )


class ProblemError(BaseError):
    """Exception class allowing to raise and catch ProblemDetail objects."""

    def __init__(self, problem_detail):
        """Initialize a new instance of ProblemError class.

        :param problem_detail: ProblemDetail object
        :type problem_detail: ProblemDetail
        """
        if not isinstance(problem_detail, ProblemDetail):
            raise ValueError('Argument "problem_detail" must be an instance of ProblemDetail class')

        self._problem_detail = problem_detail

    @property
    def problem_detail(self):
        """Return the ProblemDetail object associated with this exception.

        :return: ProblemDetail object associated with this exception
        :rtype: ProblemDetail
        """
        return self._problem_detail
