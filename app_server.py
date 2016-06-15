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
from flask.ext.babel import lazy_gettext as _
from util.flask_util import problem
from util.problem_detail import ProblemDetail
import traceback
import logging
from opds import (
    AcquisitionFeed,
    LookupAcquisitionFeed,
)
from util.opds_writer import (    
    OPDSFeed,
)
from sqlalchemy.orm.session import Session
from sqlalchemy.orm.exc import (
    NoResultFound,
)
from model import (
    get_one,
    Complaint,
    CoverageRecord,
    DataSource,
    Edition,
    Identifier,
    Patron,
    UnresolvedIdentifier,
    Work,
)
from util.cdn import cdnify
from classifier import Classifier
from config import Configuration
from lane import (
    Facets,
    Pagination,
)
from problem_details import *

opds_cdn_host = Configuration.cdn_host(Configuration.CDN_OPDS_FEEDS)
def cdn_url_for(*args, **kwargs):
    base_url = url_for(*args, **kwargs)
    return cdnify(base_url, opds_cdn_host)

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
        cache_control = "public, no-transform, max-age: %d, s-maxage: %d" % (
            client_cache, cdn_cache)
    else:
        cache_control = "private, no-cache"

    return make_response(content, 200, {"Content-Type": content_type,
                                        "Cache-Control": cache_control})

def load_facets_from_request(config=Configuration):
    """Figure out which Facets object this request is asking for."""
    arg = flask.request.args.get

    g = Facets.ORDER_FACET_GROUP_NAME
    order = arg(g, config.default_facet(g))

    g = Facets.AVAILABILITY_FACET_GROUP_NAME
    availability = arg(g, config.default_facet(g))

    g = Facets.COLLECTION_FACET_GROUP_NAME
    collection = arg(g, config.default_facet(g))
    return load_facets(order, availability, collection, config)

def load_pagination_from_request():
    """Figure out which Facets object this request is asking for."""
    arg = flask.request.args.get
    size = arg('size', Pagination.DEFAULT_SIZE)
    offset = arg('after', 0)
    return load_pagination(size, offset)

def load_facets(order, availability, collection, config=Configuration):
    """Turn user input into a Facets object."""
    order_facets = config.enabled_facets(
        Facets.ORDER_FACET_GROUP_NAME
    )
    if order and not order in order_facets:
        return INVALID_INPUT.detailed(
            _("I don't know how to order a feed by '%(order)s'", order=order),
            400
        )
    availability_facets = config.enabled_facets(
        Facets.AVAILABILITY_FACET_GROUP_NAME
    )
    if availability and not availability in availability_facets:
        return INVALID_INPUT.detailed(
            _("I don't understand the availability term '%(availability)s'", availability=availability),
            400
        )

    collection_facets = config.enabled_facets(
        Facets.COLLECTION_FACET_GROUP_NAME
    )
    if collection and not collection in collection_facets:
        return INVALID_INPUT.detailed(
            _("I don't understand which collection '%(collection)s' refers to.", collection=collection),
            400
        )
    return Facets(
        collection=collection, availability=availability, order=order
    )

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

    def heartbeat(self):
        return make_response("", 200, {"Content-Type": "application/json"})


class URNLookupController(object):

    UNRECOGNIZED_IDENTIFIER = "I've never heard of this work."
    UNRESOLVABLE_URN = "I don't know how to get metadata for this kind of identifier."
    WORK_NOT_PRESENTATION_READY = "Work created but not yet presentation-ready."
    WORK_NOT_CREATED = "Identifier resolved but work not yet created."
    IDENTIFIER_REGISTERED = "You're the first one to ask about this identifier. I'll try to find out about it."
    WORKING_TO_RESOLVE_IDENTIFIER = "I'm working to locate a source for this identifier."

    def __init__(self, _db, can_resolve_identifiers=False):
        self._db = _db
        self.works = []
        self.messages_by_urn = dict()
        self.precomposed_entries = []
        self.unresolved_identifiers = []
        self.can_resolve_identifiers = can_resolve_identifiers
        self.content_cafe = DataSource.lookup(self._db, DataSource.CONTENT_CAFE)

    @classmethod
    def parse_urn(self, _db, urn, must_support_metadata=True):

        try:
            identifier, is_new = Identifier.parse_urn(
                _db, urn)
        except ValueError, e:
            return (INVALID_URN.status_code, INVALID_URN.detail)

        if not must_support_metadata:
            return identifier

        # We support any identifier that can support a metadata
        # lookup.
        if DataSource.metadata_sources_for(_db, identifier):
            return identifier

        # Failing that, we support any identifier that can support a
        # license pool.
        source = DataSource.license_sources_for(_db, identifier)
        if source.count() > 0:
            return identifier

        return (400, self.UNRESOLVABLE_URN)

    def process_urn(self, urn, collection=None):
        """Turn a URN into a Work suitable for use in an OPDS feed.

        :return: If a Work is found, the return value is None.
        Otherwise a 2-tuple (status, message) is returned explaining why
        no work was found.
        """
        identifier = self.parse_urn(self._db, urn, True)
        if not isinstance(identifier, Identifier):
            # Error.
            self.messages_by_urn[urn] = identifier
            return

        if collection:
            collection.catalog_identifier(self._db, identifier)

        if identifier.licensed_through:
            # There is a LicensePool for this identifier!
            work = identifier.licensed_through.work
            if work:
                # And there's a Work! Is it presentation ready?
                if work.presentation_ready:
                    # It's ready for use in an OPDS feed!
                    self.works.append((identifier, work))
                    return
                else:
                    self.messages_by_urn[urn] = (202, self.WORK_NOT_PRESENTATION_READY)
                    return
            else:
                # There is a LicensePool but no Work. 
                self.messages_by_urn[urn] = (202, self.WORK_NOT_CREATED)
                return

        # This identifier has yet to be resolved into a LicensePool. Or maybe
        # the best we can do is metadata lookups.
        if not self.can_resolve_identifiers:
            # This app can't resolve identifiers, so the best thing to
            # do is to treat this identifier as a 404 error.
            #
            # TODO: We should delete the original Identifier object as it
            # is not properly part of the dataset and never will be.
            self.messages_by_urn[urn] = (404, self.UNRECOGNIZED_IDENTIFIER)
            return

        license_sources = DataSource.license_sources_for(
            self._db, identifier)
        if identifier.type != Identifier.ISBN and license_sources.count():
            self.messages_by_urn[urn] = self.register_identifier_as_unresolved(identifier)
            return
        else:
            entry = self.make_opds_entry_from_metadata_lookups(identifier)
            if isinstance(entry, tuple):
                # Alleged 'entry' is actually a message
                self.messages_by_urn[urn] = entry
                return
            else:
                self.precomposed_entries.append(entry)
                return

    def register_identifier_as_unresolved(self, identifier):
        # This identifier could have a LicensePool associated with
        # it. If this application is capable of resolving identifiers,
        # then create or retrieve an UnresolvedIdentifier object for
        # it.
        unresolved_identifier, is_new = UnresolvedIdentifier.register(
            self._db, identifier)
        self.unresolved_identifiers.append(unresolved_identifier)
        if is_new:
            # We just found out about this identifier, or rather,
            # we just found out that someone expects it to be associated
            # with a LicensePool.
            return (201, self.IDENTIFIER_REGISTERED)
        else:
            # There is a pending attempt to resolve this identifier.
            message = (unresolved_identifier.exception 
                           or self.WORKING_TO_RESOLVE_IDENTIFIER)
            return (unresolved_identifier.status, message)

    def make_opds_entry_from_metadata_lookups(self, identifier):
        """This identifier cannot be resolved into a LicensePool,
        but maybe we can make an OPDS entry based on metadata
        lookups.
        """

        # We can only create an OPDS entry if all the lookups have
        # in fact been done.
        metadata_sources = DataSource.metadata_sources_for(
            self._db, identifier)
        q = self._db.query(
            CoverageRecord).filter(
                CoverageRecord.identifier==identifier).filter(
                    CoverageRecord.data_source_id.in_(
                        [x.id for x in metadata_sources]))
        coverage_records = q.all()
        unaccounted_for = set(metadata_sources)
        for r in coverage_records:
            if r.data_source in unaccounted_for:
                unaccounted_for.remove(r.data_source)

        if unaccounted_for:
            # At least one metadata lookup has not successfully
            # completed.
            names = [x.name for x in unaccounted_for]
            logging.info(
                "Cannot build metadata-based OPDS feed for %r: missing coverage records for %s",
                identifier,
                ", ".join(names)
            )
            unresolved_identifier, is_new = UnresolvedIdentifier.register(
                self._db, identifier)
            if is_new:
                # We just found out about this identifier, or rather,
                # we just found out that someone expects it to be associated
                # with a LicensePool.
                return (201, self.IDENTIFIER_REGISTERED)
            else:
                # There is a pending attempt to resolve this identifier.
                message = (unresolved_identifier.exception 
                           or self.WORKING_TO_RESOLVE_IDENTIFIER)
                return (unresolved_identifier.status, message)
        else:
            # All metadata lookups have completed. Create that OPDS
            # entry!
            entry = identifier.opds_entry()

        if entry is None:
            # This app can't do lookups on an identifier of this
            # type, so the best thing to do is to treat this
            # identifier as a 404 error.
            return (404, self.UNRECOGNIZED_IDENTIFIER)

        # We made it!
        return entry

    def work_lookup(self, annotator, route_name='lookup',
                    require_active_licensepool=True, collection=None):
        """Generate an OPDS feed describing works identified by identifier."""
        urns = flask.request.args.getlist('urn')

        this_url = cdn_url_for(route_name, _external=True, urn=urns)
        for urn in urns:
            self.process_urn(urn, collection=collection)

        # The commit is necessary because we may have registered new
        # Identifier or UnresolvedIdentifier objects.
        self._db.commit()

        opds_feed = LookupAcquisitionFeed(
            self._db, "Lookup results", this_url, self.works, annotator,
            messages_by_urn=self.messages_by_urn, 
            precomposed_entries=self.precomposed_entries,
            require_active_licensepool=require_active_licensepool
        )
        return feed_response(opds_feed)

    def permalink(self, urn, annotator):
        """Generate an OPDS feed for looking up a single work by identifier."""
        this_url = cdn_url_for('work', _external=True, urn=urn)
        self.process_urn(urn)

        # The commit is necessary because we may have registered new
        # Identifier or UnresolvedIdentifier objects.
        self._db.commit()

        opds_feed = AcquisitionFeed(
            self._db, urn, this_url, self.works, annotator,
            messages_by_urn=self.messages_by_urn)

        return feed_response(opds_feed)


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
