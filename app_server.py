"""Implement logic common to more than one of the Simplified applications."""
from nose.tools import set_trace
from psycopg2 import DatabaseError
import flask
import json
import os
import sys
import subprocess
from lxml import etree
from functools import wraps
from flask import url_for, make_response
from flask_babel import lazy_gettext as _
from util.flask_util import problem
from util.problem_detail import ProblemDetail
import traceback
import logging
from entrypoint import EntryPoint
from opds import (
    AcquisitionFeed,
    LookupAcquisitionFeed,
)
from util.opds_writer import (
    OPDSFeed,
    OPDSMessage,
)
from sqlalchemy.orm.session import Session
from sqlalchemy.orm.exc import (
    NoResultFound,
)
from model import (
    get_one,
    Complaint,
    Identifier,
    Patron,
)
from cdn import cdnify
from classifier import Classifier
from config import Configuration
from lane import (
    Facets,
    Pagination,
)
from problem_details import *


def cdn_url_for(*args, **kwargs):
    base_url = url_for(*args, **kwargs)
    return cdnify(base_url)

def load_lending_policy(policy):
    if not policy:
        logging.info("No lending policy.")
        return {}
    if isinstance(policy, basestring):
        policy = json.loads(policy)
    for external_type, p in policy.items():
        if Patron.AUDIENCE_RESTRICTION_POLICY in p:
            for audience in p[Patron.AUDIENCE_RESTRICTION_POLICY]:
                if not audience in Classifier.AUDIENCES:
                    raise ValueError(
                        "Unrecognized audience in lending policy: %s" %
                        audience)
    return policy

def feed_response(feed, acquisition=True, cache_for=AcquisitionFeed.FEED_CACHE_TIME):
    if acquisition:
        content_type = OPDSFeed.ACQUISITION_FEED_TYPE
    else:
        content_type = OPDSFeed.NAVIGATION_FEED_TYPE
    return _make_response(feed, content_type, cache_for)

def entry_response(entry, cache_for=AcquisitionFeed.FEED_CACHE_TIME):
    content_type = OPDSFeed.ENTRY_TYPE
    return _make_response(entry, content_type, cache_for)

def _make_response(content, content_type, cache_for):
    if isinstance(content, etree._Element):
        content = etree.tostring(content)
    elif not isinstance(content, basestring):
        content = unicode(content)

    if isinstance(cache_for, int):
        # A CDN should hold on to the cached representation only half
        # as long as the end-user.
        client_cache = cache_for
        cdn_cache = cache_for / 2
        cache_control = "public, no-transform, max-age=%d, s-maxage=%d" % (
            client_cache, cdn_cache)
    else:
        cache_control = "private, no-cache"

    return make_response(content, 200, {"Content-Type": content_type,
                                        "Cache-Control": cache_control})

def load_facets_from_request(
        facet_config=None, worklist=None, base_class=Facets,
        base_class_constructor_kwargs=None
):
    """Figure out which faceting object this request is asking for.

    The active request must have the `library` member set to a Library
    object.

    :param worklist: The WorkList, if any, associated with the request.
    :param facet_config: An object containing the currently configured
        facet groups, if different from the request library.
    :param base_class: The faceting class to instantiate.
    :param base_class_constructor_kwargs: Keyword arguments to pass into
        the faceting class constructor, other than those obtained from
        the request.
    :return: A faceting object if possible; otherwise a ProblemDetail.
    """
    kwargs = base_class_constructor_kwargs or dict()
    get_arg = flask.request.args.get
    library = flask.request.library
    facet_config = facet_config or library
    return base_class.from_request(
        library, facet_config, get_arg, worklist, **kwargs
    )

def load_pagination_from_request(default_size=Pagination.DEFAULT_SIZE):
    """Figure out which Pagination object this request is asking for."""
    arg = flask.request.args.get
    size = arg('size', default_size)
    offset = arg('after', 0)
    return load_pagination(size, offset)

def load_pagination(size, offset):
    """Turn user input into a Pagination object."""
    try:
        size = int(size)
    except ValueError:
        return INVALID_INPUT.detailed(_("Invalid page size: %(size)s", size=size))
    size = min(size, 100)
    if offset:
        try:
            offset = int(offset)
        except ValueError:
            return INVALID_INPUT.detailed(_("Invalid offset: %(offset)s", offset=offset))
    return Pagination(offset, size)

def returns_problem_detail(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        v = f(*args, **kwargs)
        if isinstance(v, ProblemDetail):
            return v.response
        return v
    return decorated

class ErrorHandler(object):
    def __init__(self, app, debug):
        self.app = app
        self.debug = debug

    def handle(self, exception):
        if hasattr(self.app, 'manager') and hasattr(self.app.manager, '_db'):
            # There is an active database session. Roll it back.
            self.app.manager._db.rollback()
        tb = traceback.format_exc()

        if isinstance(exception, DatabaseError):
            # The database session may have become tainted. For now
            # the simplest thing to do is to kill the entire process
            # and let uwsgi restart it.
            logging.error(
                "Database error: %s Treating as fatal to avoid holding on to a tainted session!",
                exception, exc_info=exception
            )
            shutdown = flask.request.environ.get('werkzeug.server.shutdown')
            if shutdown:
                shutdown()
            else:
                sys.exit()

        # By default, the error will be logged at log level ERROR.
        log_method = logging.error

        # Okay, it's not a database error. Turn it into a useful HTTP error
        # response.
        if hasattr(exception, 'as_problem_detail_document'):
            # This exception can be turned directly into a problem
            # detail document.
            document = exception.as_problem_detail_document(self.debug)
            if not self.debug:
                document.debug_message = None
            else:
                if document.debug_message:
                    document.debug_message += "\n\n" + tb
                else:
                    document.debug_message = tb
            if document.status_code == 502:
                # This is an error in integrating with some upstream
                # service. It's a serious problem, but probably not
                # indicative of a bug in our software. Log it at log level
                # WARN.
                log_method = logging.warn
            response = make_response(document.response)
        else:
            # There's no way to turn this exception into a problem
            # document. This is probably indicative of a bug in our
            # software.
            if self.debug:
                body = tb
            else:
                body = _('An internal error occured')
            response = make_response(unicode(body), 500, {"Content-Type": "text/plain"})

        log_method("Exception in web app: %s", exception, exc_info=exception)
        return response


class HeartbeatController(object):

    HEALTH_CHECK_TYPE = 'application/vnd.health+json'
    VERSION_FILENAME = '.version'

    def heartbeat(self, conf_class=None):
        health_check_object = dict(status='pass')

        Conf = conf_class or Configuration
        app_version = Conf.app_version()
        if app_version and app_version != Conf.NO_APP_VERSION_FOUND:
            health_check_object['releaseID'] = app_version
            health_check_object['version'] = app_version.split('-')[0]

        data = json.dumps(health_check_object)
        return make_response(data, 200, {"Content-Type": self.HEALTH_CHECK_TYPE})


class URNLookupController(object):
    """A generic controller that takes URNs as input and looks up their
    OPDS entries.
    """

    UNRECOGNIZED_IDENTIFIER = "This work is not in the collection."
    WORK_NOT_PRESENTATION_READY = "Work created but not yet presentation-ready."
    WORK_NOT_CREATED = "Identifier resolved but work not yet created."

    def __init__(self, _db):
        self._db = _db
        self.works = []
        self.precomposed_entries = []
        self.unresolved_identifiers = []

    def work_lookup(self, annotator, route_name='lookup', **process_urn_kwargs):
        """Generate an OPDS feed describing works identified by identifier."""
        urns = flask.request.args.getlist('urn')

        this_url = cdn_url_for(route_name, _external=True, urn=urns)
        response = self.process_urns(urns, **process_urn_kwargs)
        self.post_lookup_hook()

        if response:
            # In a subclass, self.process_urns may return a ProblemDetail
            return response

        opds_feed = LookupAcquisitionFeed(
            self._db, "Lookup results", this_url, self.works, annotator,
            precomposed_entries=self.precomposed_entries,
        )
        return feed_response(opds_feed)

    def permalink(self, urn, annotator, route_name='work'):
        """Look up a single identifier and generate an OPDS feed."""
        this_url = cdn_url_for(route_name, _external=True, urn=urn)
        self.process_urns([urn])
        self.post_lookup_hook()

        # A LookupAcquisitionFeed's .works is a list of (identifier,
        # work) tuples, but an AcquisitionFeed's .works is just a
        # list of works.
        works = [work for (identifier, work) in self.works]
        opds_feed = AcquisitionFeed(
            self._db, urn, this_url, works, annotator,
            precomposed_entries=self.precomposed_entries
        )

        return feed_response(opds_feed)

    def process_urns(self, urns, **process_urn_kwargs):
        """Processes a list of URNs for a lookup request.

        :return: None or, to override default feed behavior, a ProblemDetail
        or Response
        """
        identifiers_by_urn, failures = Identifier.parse_urns(self._db, urns)
        self.add_urn_failure_messages(failures)

        for urn, identifier in identifiers_by_urn.items():
            self.process_identifier(identifier, urn, **process_urn_kwargs)

    def add_urn_failure_messages(self, failures):
        for urn in failures:
            self.add_message(urn, 400, INVALID_URN.detail)

    def process_identifier(self, identifier, urn, **kwargs):
        """Turn a URN into a Work suitable for use in an OPDS feed.
        """
        if not identifier.licensed_through:
            # The default URNLookupController cannot look up an
            # Identifier that has no associated LicensePool.
            return self.add_message(urn, 404, self.UNRECOGNIZED_IDENTIFIER)

        # If we get to this point, there is at least one LicensePool
        # for this identifier.
        work = identifier.work
        if not work:
            # There are LicensePools but no Work.
            return self.add_message(urn, 202, self.WORK_NOT_CREATED)
        if not work.presentation_ready:
            # There is a work but it's not presentation ready.
            return self.add_message(urn, 202, self.WORK_NOT_PRESENTATION_READY)

        # The work is ready for use in an OPDS feed!
        return self.add_work(identifier, work)

    def add_work(self, identifier, work):
        """An identifier lookup succeeded in finding a Work."""
        self.works.append((identifier, work))

    def add_entry(self, entry):
        """An identifier lookup succeeded in creating an OPDS entry."""
        self.precomposed_entries.append(entry)

    def add_message(self, urn, status_code, message):
        """An identifier lookup resulted in the creation of a message."""
        self.precomposed_entries.append(
            OPDSMessage(urn, status_code, message)
        )

    def post_lookup_hook(self):
        """Run after looking up a number of Identifiers.

        By default, does nothing.
        """
        pass


class ComplaintController(object):
    """A controller to register complaints against objects."""

    def register(self, license_pool, raw_data):

        if license_pool is None:
            return problem(None, 400, _("No license pool specified"))

        _db = Session.object_session(license_pool)
        try:
            data = json.loads(raw_data)
        except ValueError, e:
            return problem(None, 400, _("Invalid problem detail document"))

        type = data.get('type')
        source = data.get('source')
        detail = data.get('detail')
        if not type:
            return problem(None, 400, _("No problem type specified."))
        if type not in Complaint.VALID_TYPES:
            return problem(None, 400, _("Unrecognized problem type: %(type)s", type=type))

        complaint = None
        try:
            complaint = Complaint.register(license_pool, type, source, detail)
            _db.commit()
        except ValueError, e:
            return problem(
                None, 400, _("Error registering complaint: %(error)s", error=str(e))
            )

        return make_response(unicode(_("Success")), 201, {"Content-Type": "text/plain"})
