"""Implement logic common to more than one of the Simplified applications."""
from nose.tools import set_trace
from psycopg2 import DatabaseError
import flask
import gzip
import json
import os
import sys
import subprocess
from lxml import etree
from functools import wraps
from flask import url_for, make_response
from flask_babel import lazy_gettext as _
from io import BytesIO
from util.flask_util import problem
from util.problem_detail import ProblemDetail
import traceback
import logging
from entrypoint import EntryPoint
from opds import (
    AcquisitionFeed,
    LookupAcquisitionFeed,
)
from util.flask_util import OPDSFeedResponse
from util.opds_writer import (
    OPDSFeed,
    OPDSMessage,
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm.session import Session
from sqlalchemy.orm.exc import (
    NoResultFound,
)
from log import LogConfiguration
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
        return {}
    if isinstance(policy, basestring):
        logging.info("Lending policy: %s", policy)
        policy = json.loads(policy)
    for external_type, p in policy.items():
        if Patron.AUDIENCE_RESTRICTION_POLICY in p:
            for audience in p[Patron.AUDIENCE_RESTRICTION_POLICY]:
                if not audience in Classifier.AUDIENCES:
                    raise ValueError(
                        "Unrecognized audience in lending policy: %s" %
                        audience)
    return policy

def load_facets_from_request(
        facet_config=None, worklist=None, base_class=Facets,
        base_class_constructor_kwargs=None, default_entrypoint=None
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
    get_header = flask.request.headers.get
    library = flask.request.library
    facet_config = facet_config or library
    return base_class.from_request(
        library, facet_config, get_arg, get_header, worklist,
        default_entrypoint, **kwargs
    )


def load_pagination_from_request(
    base_class=Pagination, base_class_constructor_kwargs=None,
    default_size=None
):
    """Figure out which Pagination object this request is asking for.

    :param base_class: A subclass of Pagination to instantiate.
    :param base_class_constructor_kwargs: Extra keyword arguments to use
        when instantiating the Pagination subclass.
    :param default_size: The default page size.
    :return: An instance of `base_class`.
    """
    kwargs = base_class_constructor_kwargs or dict()

    get_arg = flask.request.args.get
    return base_class.from_request(get_arg, default_size, **kwargs)


def returns_problem_detail(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        v = f(*args, **kwargs)
        if isinstance(v, ProblemDetail):
            return v.response
        return v
    return decorated


def compressible(f):
    """Decorate a function to make it transparently handle whatever
    compression the client has announced it supports.

    Currently the only form of compression supported is
    representation-level gzip compression requested through the
    Accept-Encoding header.

    This code was modified from
    http://kb.sites.apiit.edu.my/knowledge-base/how-to-gzip-response-in-flask/,
    though I don't know if that's the original source; it shows up in
    a lot of places.
    """
    @wraps(f)
    def compressor(*args, **kwargs):
        @flask.after_this_request
        def compress(response):
            if (response.status_code < 200 or
                response.status_code >= 300 or
                'Content-Encoding' in response.headers):
                # Don't encode anything other than a 2xx response
                # code. Don't encode a response that's
                # already been encoded.
                return response

            accept_encoding = flask.request.headers.get('Accept-Encoding', '')
            if not 'gzip' in accept_encoding.lower():
                return response

            # At this point we know we're going to be changing the
            # outgoing response.

            # TODO: I understand what direct_passthrough does, but am
            # not sure what it has to do with this, and commenting it
            # out doesn't change the results or cause tests to
            # fail. This is pure copy-and-paste magic.
            response.direct_passthrough = False

            buffer = BytesIO()
            gzipped = gzip.GzipFile(mode='wb', fileobj=buffer)
            gzipped.write(response.data)
            gzipped.close()
            response.data = buffer.getvalue()

            response.headers['Content-Encoding'] = 'gzip'
            response.vary.add('Accept-Encoding')
            response.headers['Content-Length'] = len(response.data)

            return response

        return f(*args, **kwargs)
    return compressor


class ErrorHandler(object):
    def __init__(self, app, debug=False):
        """Constructor.

        :param app: A flask.app object.
        :param debug: Set this to True to give detailed debugging
           information on errors, even if the site is not configured
           to do so.
        """
        self.app = app
        self.debug = debug

    def handle(self, exception):
        """Something very bad has happened. Notify the client."""
        # By default, when reporting errors, err on the side of
        # terseness, to avoid leaking sensitive information.
        debug = self.app.config['DEBUG'] or self.debug

        if hasattr(self.app, 'manager') and hasattr(self.app.manager, '_db'):
            # There is an active database session.

            # Use it to determine whether we are in debug mode, in
            # which case we _should_ provide the client with a lot of
            # information about the problem, without worrying
            # whether it contains sensitive information.
            _db = self.app.manager._db
            try:
                LogConfiguration.from_configuration(_db)
                (log_level, database_log_level, handlers,
                 errors) = LogConfiguration.from_configuration(
                     self.app.manager._db
                 )
                debug = debug or (
                    LogConfiguration.DEBUG in (log_level, database_log_level)
                )
            except SQLAlchemyError, e:
                # The database session could not be used, possibly due to
                # the very error under consideration. Go with the
                # preexisting value for `debug`.
                pass

            # Then roll the session back.
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
            document = exception.as_problem_detail_document(debug)
            if not debug:
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
            if debug:
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
    """A controller for looking up OPDS entries for specific books,
    identified in terms of their Identifier URNs.
    """

    def __init__(self, _db):
        """Constructor.

        :param _db: A database connection.
        """
        self._db = _db

    def work_lookup(self, annotator, route_name='lookup', **process_urn_kwargs):
        """Generate an OPDS feed describing works identified by identifier."""
        urns = flask.request.args.getlist('urn')

        this_url = cdn_url_for(route_name, _external=True, urn=urns)
        handler = self.process_urns(urns, **process_urn_kwargs)

        if isinstance(handler, ProblemDetail):
            # In a subclass, self.process_urns may return a ProblemDetail
            return handler

        opds_feed = LookupAcquisitionFeed(
            self._db, "Lookup results", this_url, handler.works, annotator,
            precomposed_entries=handler.precomposed_entries,
        )
        return OPDSFeedResponse(opds_feed)

    def process_urns(self, urns, **process_urn_kwargs):
        """Process a number of URNs by instantiating a URNLookupHandler
        and having it do the work.

        The information gathered by the URNLookupHandler can be used
        by the caller to generate an OPDS feed.

        :return: A URNLookupHandler, or a ProblemDetail if
            there's a problem with the request.
        """
        handler = URNLookupHandler(self._db)
        handler.process_urns(urns, **process_urn_kwargs)
        return handler

    def permalink(self, urn, annotator, route_name='work'):
        """Look up a single identifier and generate an OPDS feed.

        TODO: This method is tested, but it seems unused and it
        should be possible to remove it.
        """
        handler = URNLookupHandler(self._db)
        this_url = cdn_url_for(route_name, _external=True, urn=urn)
        handler.process_urns([urn])

        # A LookupAcquisitionFeed's .works is a list of (identifier,
        # work) tuples, but an AcquisitionFeed's .works is just a
        # list of works.
        works = [work for (identifier, work) in handler.works]
        opds_feed = AcquisitionFeed(
            self._db, urn, this_url, works, annotator,
            precomposed_entries=handler.precomposed_entries
        )
        return OPDSFeedResponse(opds_feed)


class URNLookupHandler(object):
    """A helper for URNLookupController that takes URNs as input and looks
    up their OPDS entries.

    This is a separate class from URNLookupController because
    URNLookupController is designed to not keep state.
    """

    UNRECOGNIZED_IDENTIFIER = "This work is not in the collection."
    WORK_NOT_PRESENTATION_READY = "Work created but not yet presentation-ready."
    WORK_NOT_CREATED = "Identifier resolved but work not yet created."

    def __init__(self, _db):
        self._db = _db
        self.works = []
        self.precomposed_entries = []
        self.unresolved_identifiers = []

    def process_urns(self, urns, **process_urn_kwargs):
        """Processes a list of URNs for a lookup request.

        :return: None or, to override default feed behavior, a ProblemDetail
            or Response.

        """
        identifiers_by_urn, failures = Identifier.parse_urns(self._db, urns)
        self.add_urn_failure_messages(failures)

        for urn, identifier in identifiers_by_urn.items():
            self.process_identifier(identifier, urn, **process_urn_kwargs)
        self.post_lookup_hook()

    def add_urn_failure_messages(self, failures):
        for urn in failures:
            self.add_message(urn, 400, INVALID_URN.detail)

    def process_identifier(self, identifier, urn, **kwargs):
        """Turn a URN into a Work suitable for use in an OPDS feed.
        """
        if not identifier.licensed_through:
            # The default URNLookupHandler cannot look up an
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
                None, 400, _("Error registering complaint: %(error)s", error=unicode(e))
            )

        return make_response(unicode(_("Success")), 201, {"Content-Type": "text/plain"})
