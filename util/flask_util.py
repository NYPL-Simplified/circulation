"""Utilities for Flask applications."""
import datetime
import flask
from lxml import etree
from flask import Response as FlaskResponse
from wsgiref.handlers import format_date_time
import time

from . import (
    problem_detail,
)
from .opds_writer import OPDSFeed
from .datetime_helpers import utc_now

def problem_raw(type, status, title, detail=None, instance=None, headers={}):
    data = problem_detail.json(type, status, title, detail, instance)
    final_headers = { "Content-Type" : problem_detail.JSON_MEDIA_TYPE }
    final_headers.update(headers)
    return status, final_headers, data

def problem(type, status, title, detail=None, instance=None, headers={}):
    """Create a Response that includes a Problem Detail Document."""
    status, headers, data = problem_raw(
        type, status, title, detail, instance, headers)
    return FlaskResponse(data, status, headers)


class Response(FlaskResponse):
    """A Flask Response object with some conveniences added.

    The conveniences:

       * It's easy to calculate header values such as Cache-Control.
       * A response can be easily converted into a string for use in
         tests.
    """

    def __init__(self, response=None, status=None, headers=None, mimetype=None,
                 content_type=None, direct_passthrough=False, max_age=0,
                 private=None):
        """Constructor.

        All parameters are the same as for the Flask/Werkzeug Response class,
        with these additions:

        :param max_age: The number of seconds for which clients should
            cache this response. Used to set a value for the
            Cache-Control header.
        :param private: If this is True, then the response contains
            information from an authenticated client and should not be stored
            in intermediate caches.
        """
        max_age = max_age or 0
        try:
            max_age = int(max_age)
        except ValueError:
            max_age = 0
        self.max_age = max_age
        if private is None:
            if self.max_age == 0:
                # The most common reason for max_age to be set to 0 is that a resource
                # is _also_ private.
                private = True
            else:
                private = False
        self.private = private

        body = response
        if isinstance(body, etree._Element):
            body = etree.tostring(body)
        elif not isinstance(body, (bytes, str)):
            body = str(body)

        super(Response, self).__init__(
            response=body,
            status=status,
            headers=self._headers(headers or {}),
            mimetype=mimetype,
            content_type=content_type,
            direct_passthrough=direct_passthrough
        )

    def __str__(self):
        """This object can be treated as a string, e.g. in tests.

        :return: The entity-body portion of the response.
        """
        return self.get_data(as_text=True)

    def _headers(self, headers={}):
        """Build an appropriate set of HTTP response headers."""
        # Don't modify the underlying dictionary; it came from somewhere else.
        headers = dict(headers)

        # Set headers based on privacy settings and maximum age.
        if self.private:
            private = "private"

            # A private resource should be re-requested, rather than
            # retrieved from cache, if the authorization credentials
            # change from those originally used to retrieve it.
            headers['Vary'] = 'Authorization'
        else:
            private = "public"
        if self.max_age and isinstance(self.max_age, int):
            client_cache = self.max_age
            if self.private:
                # A private resource should not be cached by
                # intermediaries at all.
                s_maxage = ""
            else:
                # A public resource can be cached by intermediaries
                # for half as long as the end-user can cache it.
                s_maxage = ", s-maxage=%d" % (self.max_age / 2)
            cache_control = "%s, no-transform, max-age=%d%s" % (
                private, client_cache, s_maxage
            )

            # Explicitly set Expires based on max-age; some clients need this.
            expires_at = utc_now() + datetime.timedelta(
                seconds=self.max_age
            )
            headers['Expires'] = format_date_time(
                time.mktime(expires_at.timetuple())
            )
        else:
            # Missing, invalid or zero max-age means don't cache at all.
            cache_control = "%s, no-cache" % private
        headers['Cache-Control'] = cache_control

        return headers


class OPDSFeedResponse(Response):
    """A convenience specialization of Response for typical OPDS feeds."""
    def __init__(self, response=None, status=None, headers=None, mimetype=None,
                 content_type=None, direct_passthrough=False, max_age=None,
                 private=None):

        mimetype = mimetype or OPDSFeed.ACQUISITION_FEED_TYPE
        status = status or 200
        if max_age is None:
            max_age = OPDSFeed.DEFAULT_MAX_AGE
        super(OPDSFeedResponse, self).__init__(
            response=response, status=status, headers=headers,
            mimetype=mimetype, content_type=content_type,
            direct_passthrough=direct_passthrough, max_age=max_age,
            private=private
        )


class OPDSEntryResponse(Response):
    """A convenience specialization of Response for typical OPDS entries."""
    def __init__(self, response=None, **kwargs):
        kwargs.setdefault('mimetype', OPDSFeed.ENTRY_TYPE)
        super(OPDSEntryResponse, self).__init__(response, **kwargs)
