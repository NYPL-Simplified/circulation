import datetime
from flask_babel import lazy_gettext as _

from .authenticator import (
    BasicAuthenticationProvider,
    PatronData,
)

from .config import (
    CannotLoadConfiguration,
)

from core.model import Patron

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

    TEST_NEIGHBORHOOD = 'neighborhood'

    basic_settings = list(BasicAuthenticationProvider.SETTINGS)
    for i, setting in enumerate(basic_settings):
        if setting['key'] == BasicAuthenticationProvider.TEST_IDENTIFIER:
            s = dict(**setting)
            s['description'] = BasicAuthenticationProvider.TEST_IDENTIFIER_DESCRIPTION_FOR_REQUIRED_PASSWORD
            basic_settings[i] = s
        elif setting['key'] == BasicAuthenticationProvider.TEST_PASSWORD:
            s = dict(**setting)
            s['required'] = True
            s['description'] = BasicAuthenticationProvider.TEST_PASSWORD_DESCRIPTION_REQUIRED
            basic_settings[i] = s

    SETTINGS = basic_settings + [
        { "key": ADDITIONAL_TEST_IDENTIFIERS,
          "label": _("Additional test identifiers"),
          "type": "list",
          "description": _("Identifiers for additional patrons to use in testing. The identifiers will all use the same test password as the first identifier."),
        },
        { "key": TEST_NEIGHBORHOOD,
          "label": _("Test neighborhood"),
          "description": _("For analytics purposes, all patrons will be 'from' this neighborhood."),
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

        self.test_neighborhood = integration.setting(self.TEST_NEIGHBORHOOD).value or None

    def remote_authenticate(self, username, password):
        "Fake 'remote' authentication."
        if not username or (self.collects_password and not password):
            return None

        if not self.valid_patron(username, password):
            return None

        return self.generate_patrondata(username, self.test_neighborhood)

    @classmethod
    def generate_patrondata(cls, authorization_identifier, neighborhood=None):

        if authorization_identifier.endswith("_username"):
            username = authorization_identifier
            identifier = authorization_identifier[:-9]
        else:
            identifier = authorization_identifier
            username = authorization_identifier + "_username"

        personal_name = "PersonalName" + identifier

        patrondata = PatronData(
            authorization_identifier=identifier,
            permanent_id=identifier + "_id",
            username=username,
            personal_name=personal_name,
            authorization_expires = None,
            fines = None,
            neighborhood=neighborhood,
        )
        return patrondata

    # End implementation of BasicAuthenticationProvider abstract
    # methods.

    def valid_patron(self, username, password):
        """Is this patron associated with the given password in
        the given dictionary?
        """
        if self.collects_password:
            password_match = (password==self.test_password)
        else:
            password_match = (password in (None, ''))
        return password_match and username in self.test_identifiers

    def _remote_patron_lookup(self, patron_or_patrondata):
        if not patron_or_patrondata:
            return None
        if ((isinstance(patron_or_patrondata, PatronData)
            or isinstance(patron_or_patrondata, Patron))
            and patron_or_patrondata.authorization_identifier in self.test_identifiers):
                return self.generate_patrondata(patron_or_patrondata.authorization_identifier)

AuthenticationProvider = SimpleAuthenticationProvider
