"""Implement logic common to more than one of the Simplified applications."""
import flask
from flask import url_for
from util.flask_util import problem
from opds import AcquisitionFeed
from model import (
    Edition,
    Identifier,
    Work,
)

INVALID_URN_PROBLEM = "%s is not a valid identifier."

def work_lookup(_db, annotator):
    """Generate an OPDS feed describing works identified by identifier."""
    urns = flask.request.args.getlist('urn')

    identifiers = []
    works = []

    data_sources = {}
    this_url = url_for('lookup', _external=True, urn=urns)
    new_identifiers = []

    for urn in urns:
        try:
            identifier, is_new = Identifier.parse_urn(_db, urn)
            if is_new:
                new_identifiers.append(identifier)
            else:
                identifiers.append(identifier.id)
        except ValueError, e:
            return problem(
                INVALID_URN_PROBLEM % identifier_urn,
                400
            )

    works = _db.query(Work).join(Work.editions).filter(
            Edition.primary_identifier_id.in_(identifiers))

    opds_feed = AcquisitionFeed(
        _db, "Lookup results", this_url, works, annotator)
    return unicode(opds_feed)
