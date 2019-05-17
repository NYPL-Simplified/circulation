from flask_babel import lazy_gettext as _
import logging
from authenticator import (
    BasicAuthenticationProvider,
    PatronData,
)
from config import CannotLoadConfiguration
from core.model import ExternalIntegration
from lxml import etree
from core.util.http import HTTP


class KansasAuthenticationAPI(BasicAuthenticationProvider):

    NAME = 'Kansas'

    DESCRIPTION = _("""
        An authentication service for the Kansas State Library.
        """)

    DISPLAY_NAME = NAME

    SETTINGS = [
        {
            "key": ExternalIntegration.URL,
            "format": "url",
            "label": _("URL"),
            "default": "https://ks-kansaslibrary3m.civicplus.com/api/UserDetails",
            "required": True
        },
    ] + BasicAuthenticationProvider.SETTINGS

    log = logging.getLogger("Kansas authentication API")

    def __init__(self, library_id, integration, analytics=None, base_url=None):
        super(KansasAuthenticationAPI, self).__init__(library_id, integration, analytics)
        if base_url is None:
            base_url = integration.url
        if not base_url:
            raise CannotLoadConfiguration(
                "Kansas server url not configured."
            )
        self.base_url = base_url

    # Begin implementation of BasicAuthenticationProvider abstract
    # methods.

    def remote_authenticate(self, username, password):
        # Create XML doc for request
        authorization_request = self.create_authorize_request(username, password)
        # Post request to the server
        response = self.post_request(authorization_request)
        # Parse response from server
        authorized, patron_name, library_identifier = self.parse_authorize_response(response.content)
        if not authorized:
            return False
        # Kansas auth gives very little data about the patron. Only name and a library identifier.
        return PatronData(
            permanent_id=username,
            authorization_identifier=username,
            personal_name=patron_name,
            library_identifier=library_identifier,
            complete=True
        )

    # End implementation of BasicAuthenticationProvider abstract methods.

    @staticmethod
    def create_authorize_request(barcode, pin):
        # Create the authentication document
        authorize_request = etree.Element("AuthorizeRequest")
        user_id = etree.Element("UserID")
        user_id.text = barcode
        password = etree.Element("Password")
        password.text = pin
        authorize_request.append(user_id)
        authorize_request.append(password)
        return etree.tostring(authorize_request, encoding='utf8')

    def parse_authorize_response(self, response):
        try:
            authorize_response = etree.fromstring(response)
        except etree.XMLSyntaxError:
            self.log.error("Unable to parse response from API. Deny Access. Response: \n%s", response)
            return False, None, None
        patron_names = []
        for tag in ["FirstName", "LastName"]:
            element = authorize_response.find(tag)
            if element is not None and element.text is not None:
                patron_names.append(element.text)
        patron_name = ' '.join(patron_names) if len(patron_names) != 0 else None
        element = authorize_response.find("LibraryID")
        library_identifier = element.text if element is not None else None
        element = authorize_response.find('Status')
        if element is None:
            self.log.info("Status element not found in response from server. Deny Access.")
        authorized = True if element is not None and element.text == "1" else False
        return authorized, patron_name, library_identifier

    def post_request(self, data):
        """Make an HTTP request.

        Defined solely so it can be overridden in the mock.
        """
        return HTTP.post_with_timeout(
            self.base_url,
            data,
            headers={"Content-Type": "application/xml"},
            allowed_response_codes=['2xx'],
        )


# Specify which of the classes defined in this module is the
# authentication provider.
AuthenticationProvider = KansasAuthenticationAPI
