"""Utilities for Flask applications."""
import flask
from flask import Response

from . import (
    problem_detail,
)

def problem_raw(type, status, title, detail=None, instance=None, headers={}):
    data = problem_detail.json(type, status, title, detail, instance)
    final_headers = { "Content-Type" : problem_detail.JSON_MEDIA_TYPE }
    final_headers.update(headers)
    return status, final_headers, data

def problem(type, status, title, detail=None, instance=None, headers={}):
    """Create a Response that includes a Problem Detail Document."""
    status, headers, data = problem_raw(
        type, status, title, detail, instance, headers)
    return Response(data, status, headers)


class Responselike(object):
    """An object similar to a Flask Response object, but with some improvements.

    The improvements focus around making it easy to calculating header values
    such as Cache-Control based on standard rules for this system.
    """

    def __init__(self, response=None, status=None, headers=None, mimetype=None,
                 content_type=None, direct_passthrough=False, max_age=None):
        """Constructor.

        All parameters are the same as for the Flask/Werkzeug Response class,
        with these additions:

        :param max_age: The number of seconds for which clients should
            cache this response. Used to set a value for the
            Cache-Control header.
        """
        self._response = response
        self.status = status
        self._headers = dict(headers) or {}
        self.mimetype = mimetype
        self.content_type = content_type
        self.direct_passthrough = direct_passthrough

        self.max_age = max_age

    def __unicode__(self):
        """This object can be treated as a string, e.g. in tests.

        :return: The entity-body portion of the response.
        """
        return self._response

    @property
    def response(self):
        """Convert to a real Flask response."""
        return Response(
            response=self.response,
            status=self.status,
            headers=self.headers,
            mimetype=self.mimetype,
            content_type=self.content_type,
            body=self.body,
            headers=self.headers
        )

    @property
    def headers(self):
        headers = dict(self._headers)
        # Set Cache-Control based on max-age.

        # Explicitly set Expires based on max-age; some clients need this.
        return headers
