import flask
from flask import Response
from core.util.problem_detail import ProblemDetail
from circulation_exceptions import *
from problem_details import *
from flask.ext.babel import lazy_gettext as _
from core.model import Library

class BaseCirculationManagerController(object):
    """Define minimal standards for a circulation manager controller,
    mainly around authentication.
    """
    
    def __init__(self, manager):
        """:param manager: A CirculationManager object."""
        self.manager = manager
        self._db = self.manager._db
        self.circulation = self.manager.circulation
        self.url_for = self.manager.url_for
        self.cdn_url_for = self.manager.cdn_url_for

    @property
    def library(self):
        """Set flask.request.library.

        TODO: This is a stopgap which will need to be modified once
        we actually support more than one library.
        """
        flask.request.library = Library.instance(self._db)
        return flask.request.library
        
    def authorization_header(self):
        """Get the authentication header."""

        # This is the basic auth header.
        header = flask.request.authorization

        # If we're using a token instead, flask doesn't extract it for us.
        if not header:
            if 'Authorization' in flask.request.headers:
                header = flask.request.headers['Authorization']

        return header

    def authenticated_patron_from_request(self):
        header = self.authorization_header()

        if not header:
            # No credentials were provided.
            return self.authenticate()

        try:
            patron = self.authenticated_patron(header)
        except RemoteInitiatedServerError,e:
            return REMOTE_INTEGRATION_FAILED.detailed(
                _("Error in authentication service")
            )
        if isinstance(patron, ProblemDetail):
            flask.request.patron = None
            return patron
        else:
            flask.request.patron = patron
            return patron

    def authenticated_patron(self, authorization_header):
        """Look up the patron authenticated by the given authorization header.

        The header could contain a barcode and pin or a token for an
        external service.

        If there's a problem, return a Problem Detail Document.

        If there's no problem, return a Patron object.
        """
        patron = self.manager.auth.authenticated_patron(
            self._db, authorization_header
        )
        if not patron:
            return INVALID_CREDENTIALS

        if isinstance(patron, ProblemDetail):
            return patron

        return patron

    def authenticate(self):
        """Sends a 401 response that demands authentication."""
        if not self.manager.opds_authentication_document:
            self.manager.opds_authentication_document = self.manager.auth.create_authentication_document()

        data = self.manager.opds_authentication_document
        headers = self.manager.auth.create_authentication_headers()
        return Response(data, 401, headers)
