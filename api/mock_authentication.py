from nose.tools import set_trace
import datetime
import logging
import json
from decimal import Decimal

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
    NAME = "Mock Authentication Provider"

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

    def remote_authenticate(self, username, password):
        "Fake 'remote' authentication."
        if not username or not password:
            return None

        patrondata = PatronData(
            authorization_identifier=username,
            permanent_id=username + "_id",
            username=username + "_username"
        )
        if self.valid_patron(username, password, self.patrons):
            # The patron's authorization expires tomorrow.
            patrondata.authorization_expires = now + one_day
        else:
            patrondata = None
        return patrondata

    # End implementation of BasicAuthenticationProvider abstract
    # methods.

    def valid_patron(self, username, password, patrons):
        """Is this patron associated with the given password in 
        the given dictionary?
        """
        return username==self.username and password==self.password

    
class MockAuthenticationProvider(BasicAuthenticationProvider):
    """A subclass of SimpleAuthenticationProvider that offers easier
    setup for tests.

    This should not be used outside of tests -- use
    SimpleAuthenticationProvider instead.
    """

    NAME = "Mock Authentication Provider"

    VALID_PATRONS = 'patrons'
    EXPIRED_PATRONS = 'expired_patrons'
    PATRONS_WITH_FINES = 'patrons_with_fines'

    def __init__(self, library_id, patrons, expired_patrons={},
                 patrons_with_fines={}):
        """Constructor.

        This class can be instantiated by calling its constructor, but it 
        cannot be used as part of a LibraryAuthenticator.
        """
        self.library_id = library_id
        self.log = logging.getLogger(self.NAME)
        self.identifier_re = None
        self.password_re = None
        if not patrons:
            self.log.warn(
                "No valid patron configured for mock authentication provider."
            )
            patrons = {}            
        self.patrons = patrons
        self.expired_patrons = expired_patrons
        self.patrons_with_fines = patrons_with_fines
        
    # Begin implementation of BasicAuthenticationProvider abstract
    # methods.

    def remote_authenticate(self, username, password):
        now = datetime.datetime.utcnow()
        one_day = datetime.timedelta(days=1)
        
        patrondata = PatronData(
            authorization_identifier=username,
            permanent_id=username + "_id",
            username=username + "_username"
        )
        if self.valid_patron(username, password, self.patrons):
            # The patron's authorization expires tomorrow.
            patrondata.authorization_expires = now + one_day
        elif self.valid_patron(username, password, self.expired_patrons):
            # The patron's authorization expired yesterday.
            patrondata.authorization_expires = now - one_day
        elif self.valid_patron(username, password, self.patrons_with_fines):
            # The patron has racked up huge fines.
            patrondata.fines = Decimal(12345678.90)
        else:
            return None
        return patrondata

    def valid_patron(self, username, password, patrons):
        """Is this patron associated with the given password in 
        the given dictionary?
        """
        return username in patrons and patrons[username]==password

    
AuthenticationProvider = SimpleAuthenticationProvider
