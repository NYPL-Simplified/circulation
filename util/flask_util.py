"""Utilities for Flask applications."""
import datetime
import flask
from lxml import etree
from flask import Response
from wsgiref.handlers import format_date_time
import time

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
    """An object like a Flask Response object, but with some conveniences.

    The conveniences:

       * It's easy to calculate header values such as Cache-Control.
       * A Responselike can be easily converted into a string for use in
         tests.
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
        self._headers = headers or {}
        self.mimetype = mimetype
        self.content_type = content_type
        self.direct_passthrough = direct_passthrough

        self.max_age = max_age

    def __unicode__(self):
        """This object can be treated as a string, e.g. in tests.

        :return: The entity-body portion of the response.
        """
        return self.entity_body

    @property
    def entity_body(self):
        body = self._response
        if isinstance(body, etree._Element):
            body = etree.tostring(body)
        elif not isinstance(body, (bytes, unicode)):
            body = unicode(body)
        return body

    @property
    def response(self):
        """Convert to a real Flask response."""
        return Response(
            response=self.entity_body,
            status=self.status,
            headers=self.headers,
            mimetype=self.mimetype,
            content_type=self.content_type,
            direct_passthrough=self.direct_passthrough
        )

    @property
    def headers(self):
        """Build an appropriate set of HTTP response headers."""
        # Don't modify the underlying dictionary; it came from somewhere else.
        headers = dict(self._headers)

        # Set Cache-Control based on max-age.
        if self.max_age and isinstance(self.max_age, int):
            # A CDN should hold on to the cached representation only half
            # as long as the end-user.
            client_cache = self.max_age
            cdn_cache = self.max_age / 2
            cache_control = "public, no-transform, max-age=%d, s-maxage=%d" % (
                client_cache, cdn_cache
            )

            # Explicitly set Expires based on max-age; some clients need this.
            expires_at = datetime.datetime.utcnow() + datetime.timedelta(
                seconds=self.max_age
            )
            headers['Expires'] = format_date_time(
                time.mktime(expires_at.timetuple())
            )
        else:
            # Missing, invalid or zero max-age means don't cache at all.
            cache_control = "private, no-cache"
        headers['Cache-Control'] = cache_control

        return headers
