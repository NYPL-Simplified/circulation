from nose.tools import set_trace
import datetime
from flask.ext.babel import lazy_gettext as _

from authenticator import (
    BasicAuthenticationProvider,
    PatronData,
)

from config import (
    CannotLoadConfiguration,
)

class SimpleAuthenticationProvider(BasicAuthenticationProvider):
    """An authentication provider that authenticates a single patron.

    This serves only one purpose: to set up a working circulation
    manager before connecting it to an ILS.
    """
    NAME = "Simple Authentication Provider"

    DESCRIPTION = _("""
        An internal authentication service that authenticates a single patron.
        This is useful for testing a circulation manager before connecting
        it to an ILS.""")

    def __init__(self, library, integration, analytics=None):
        super(SimpleAuthenticationProvider, self).__init__(
            library, integration, analytics
        )
        self.test_identifier = integration.setting(self.TEST_IDENTIFIER).value
        self.test_password = integration.setting(self.TEST_PASSWORD).value
        if not (self.test_identifier and self.test_password):
            raise CannotLoadConfiguration(
                "Test identifier and password not set."
            )
        
    def remote_authenticate(self, username, password):
        "Fake 'remote' authentication."
        if not username or not password:
            return None

        if not self.valid_patron(username, password):
            return None

        username = self.test_identifier
        patrondata = PatronData(
            authorization_identifier=username,
            permanent_id=username + "_id",
            username=username + "_username",
            authorization_expires = None,
            fines = None,
        )
        return patrondata

    # End implementation of BasicAuthenticationProvider abstract
    # methods.

    def valid_patron(self, username, password):
        """Is this patron associated with the given password in 
        the given dictionary?
        """
        return password==self.test_password and (
            username==self.test_identifier
            or username == self.test_identifier + '_username'
        )

AuthenticationProvider = SimpleAuthenticationProvider
