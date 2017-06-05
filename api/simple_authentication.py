from nose.tools import set_trace
import datetime

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

    def __init__(self, library_id, integration):
        super(SimpleAuthenticationProvider, self).__init__(
            library_id, integration,
        )
        self.test_identifier = integration.get(self.TEST_IDENTIFIER)
        self.test_password = integration.get(self.TEST_PASSWORD)
        if not (self.test_identifier and self.test_password):
            raise CannotLoadConfiguration(
                "Test identifier and password not set."
            )
        self.patron_expiration = None
        self.patron_fines = None
        
    def remote_authenticate(self, username, password):
        "Fake 'remote' authentication."
        if not username or not password:
            return None

        patrondata = PatronData(
            authorization_identifier=username,
            permanent_id=username + "_id",
            username=username + "_username"
        )
        now = datetime.datetime.utcnow()
        one_day = datetime.timedelta(days=1)
        if self.valid_patron(username, password):
            # The patron's authorization expires tomorrow.
            patrondata.authorization_expires = self.patron_expiration or (now + one_day)
            patrondata.fines = self.patron_fines or None
        else:
            patrondata = None
        return patrondata

    # End implementation of BasicAuthenticationProvider abstract
    # methods.

    def valid_patron(self, username, password):
        """Is this patron associated with the given password in 
        the given dictionary?
        """
        return username==self.test_identifier and password==self.test_password

AuthenticationProvider = SimpleAuthenticationProvider
