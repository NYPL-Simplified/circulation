from nose.tools import set_trace
import datetime
from flask_babel import lazy_gettext as _

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

    ADDITIONAL_TEST_IDENTIFIERS = 'additional_test_identifiers'

    SETTINGS = BasicAuthenticationProvider.SETTINGS + [
        { "key": ADDITIONAL_TEST_IDENTIFIERS,
          "label": _("Additional test identifiers"),
          "type": "list",
          "optional": True,
          "description": _("Identifiers for additional patrons to use in testing. The identifiers will all use the same test password as the first identifier."),
        }
    ]

    def __init__(self, library, integration, analytics=None):
        super(SimpleAuthenticationProvider, self).__init__(
            library, integration, analytics
        )

        self.test_password = integration.setting(self.TEST_PASSWORD).value
        test_identifier = integration.setting(self.TEST_IDENTIFIER).value
        if not (test_identifier and self.test_password):
            raise CannotLoadConfiguration(
                "Test identifier and password not set."
            )

        self.test_identifiers = [test_identifier, test_identifier + "_username"]
        additional_identifiers = integration.setting(self.ADDITIONAL_TEST_IDENTIFIERS).json_value
        if additional_identifiers:
            for identifier in additional_identifiers:
                self.test_identifiers += [identifier, identifier + "_username"]
        
    def remote_authenticate(self, username, password):
        "Fake 'remote' authentication."
        if not username or not password:
            return None

        if not self.valid_patron(username, password):
            return None

        if username.endswith("_username"):
            username = username
            identifier = username[:-9]
        else:
            identifier = username
            username = identifier + "_username"

        patrondata = PatronData(
            authorization_identifier=identifier,
            permanent_id=identifier + "_id",
            username=username,
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
            username in self.test_identifiers
        )

AuthenticationProvider = SimpleAuthenticationProvider
