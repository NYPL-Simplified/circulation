import flask
from flask import Response
from core.util.problem_detail import ProblemDetail
from circulation_exceptions import *
from problem_details import *
from flask.ext.babel import lazy_gettext as _
from core.model import Library, get_one

class BaseCirculationManagerController(object):
    """Define minimal standards for a circulation manager controller,
    mainly around authentication.
    """
    
    def __init__(self, manager):
        """:param manager: A CirculationManager object."""
        self.manager = manager
        self._db = self.manager._db
        self.url_for = self.manager.url_for
        self.cdn_url_for = self.manager.cdn_url_for

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
        library_short_name = flask.request.library.short_name
        if library_short_name not in self.manager.opds_authentication_documents:
            self.manager.opds_authentication_documents[library_short_name] = self.manager.auth.create_authentication_document()

        data = self.manager.opds_authentication_documents[library_short_name]
        headers = self.manager.auth.create_authentication_headers()
        return Response(data, 401, headers)

    def library_for_request(self, library_short_name):
        """Look up the library the user is trying to access.

        Since this is called on pretty much every request, it's also
        an appropriate time to check whether the site configuration
        has been changed and needs to be updated.
        """
        self.manager.reload_settings_if_changed()
        
        if library_short_name:
            library = get_one(self._db, Library, short_name=library_short_name)
        else:
            library = Library.default(self._db)
        
        if not library:
            return LIBRARY_NOT_FOUND
        flask.request.library = library
        return library
