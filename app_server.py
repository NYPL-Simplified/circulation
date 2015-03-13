"""Implement logic common to more than one of the Simplified applications."""
from nose.tools import set_trace
import flask
from flask import url_for, make_response
from util.flask_util import problem
from opds import (
    AcquisitionFeed,
    LookupAcquisitionFeed,
    OPDSFeed,

)
from model import (
    Edition,
    Identifier,
    UnresolvedIdentifier,
    Work,
)

def feed_response(feed, acquisition=True):
    if not isinstance(feed, basestring):
        feed = unicode(feed)
    if acquisition:
        content_type = OPDSFeed.ACQUISITION_FEED_TYPE
    else:
        content_type = OPDSFeed.NAVIGATION_FEED_TYPE
    return make_response(feed, 200, {"Content-Type": content_type})

class HeartbeatController(object):

    def heartbeat(self):
        return make_response("", 200, {"Content-Type": "text/plain"})


class URNLookupController(object):

    INVALID_URN = "Could not parse identifier."
    UNRECOGNIZED_IDENTIFIER = "I've never heard of this work."
    UNRESOLVABLE_URN = "I don't know how to resolve an identifier of this type into a work."
    WORK_NOT_PRESENTATION_READY = "Work created but not yet presentation-ready."
    WORK_NOT_CREATED = "Identifier resolved but work not yet created."
    IDENTIFIER_REGISTERED = "You're the first one to ask about this identifier. I'll try to find out about it."
    WORKING_TO_RESOLVE_IDENTIFIER = "I'm working to locate a source for this identifier."

    COULD_NOT_PARSE_URN_TYPE = "http://librarysimplified.org/terms/problem/could-not-parse-urn"

    def __init__(self, _db, can_resolve_identifiers=False):
        self._db = _db
        self.works = []
        self.unresolved_identifiers = []
        self.can_resolve_identifiers = can_resolve_identifiers

    @classmethod
    def parse_urn(self, _db, urn, must_support_license_pools=True):
        try:
            identifier, is_new = Identifier.parse_urn(
                _db, urn,
                must_support_license_pools=must_support_license_pools)
        except ValueError, e:
            return (400, self.INVALID_URN)
        except Identifier.UnresolvableIdentifierException, e:
            return (400, self.UNRESOLVABLE_URN)
        return identifier

    def process_urn(self, urn):
        """Turn a URN into a Work suitable for use in an OPDS feed.

        :return: If a Work is found, the return value is None.
        Otherwise a 2-tuple (status, message) is returned explaining why
        no work was found.
        """
        identifier = self.parse_urn(self._db, urn, True)
        if not isinstance(identifier, Identifier):
            # Error.
            return identifier

        # We were able to parse the URN into an identifier, and it's
        # of a type that should in theory be resolvable into a
        # LicensePool.
        if identifier.licensed_through:
            # There is a LicensePool for this identifier!
            work = identifier.licensed_through.work
            if work:
                # And there's a Work! Is it presentation ready?
                if work.presentation_ready:
                    # It's ready for use in an OPDS feed!
                    self.works.append((identifier, work))
                    return None, None
                else:
                    return (202, self.WORK_NOT_PRESENTATION_READY)
            else:
                # There is a LicensePool but no Work. 
                return (202, self.WORK_NOT_CREATED)

        # This identifier has yet to be resolved into a LicensePool.
        # If this application is capable of resolving identifiers, then
        # create or retrieve an UnresolvedIdentifier object for it.
        if self.can_resolve_identifiers:
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
        else:
            # This app can't resolve identifiers, so the best thing to
            # do is to treat this identifier as a 404 error.
            return (404, self.UNRECOGNIZED_IDENTIFIER)

            # TODO: We should delete the original Identifier object as it
            # is not properly part of the dataset and never will be.

    def work_lookup(self, annotator, controller_name='lookup'):
        """Generate an OPDS feed describing works identified by identifier."""
        urns = flask.request.args.getlist('urn')

        messages_by_urn = dict()
        this_url = url_for(controller_name, _external=True, urn=urns)
        for urn in urns:
            code, message = self.process_urn(urn)
            if code:
                messages_by_urn[urn] = (code, message)

        # The commit is necessary because we may have registered new
        # Identifier or UnresolvedIdentifier objects.
        self._db.commit()

        opds_feed = LookupAcquisitionFeed(
            self._db, "Lookup results", this_url, self.works, annotator,
            messages_by_urn=messages_by_urn)

        return feed_response(opds_feed)

    def permalink(self, urn, annotator):
        """Generate an OPDS feed for looking up a single work by identifier."""
        this_url = url_for('work', _external=True, urn=urn)
        messages_by_urn = dict()
        code, message = self.process_urn(urn)
        if code:
            messages_by_urn[urn] = (code, message)

        # The commit is necessary because we may have registered new
        # Identifier or UnresolvedIdentifier objects.
        self._db.commit()

        opds_feed = AcquisitionFeed(
            self._db, urn, this_url, self.works, annotator,
            messages_by_urn=messages_by_urn)

        return feed_response(opds_feed)

    
