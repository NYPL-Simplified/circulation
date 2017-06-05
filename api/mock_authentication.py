from nose.tools import set_trace
import datetime
import logging
from decimal import Decimal

from authenticator import (
    BasicAuthenticationProvider,
    PatronData,
)

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

    def __init__(self, library_id, integration=None,
                 patrons={ "unittestuser": "unittestpassword"},
                 expired_patrons={ "expired": "password"},
                 patrons_with_fines={"ihavefines": "password"}
    ):
        """Constructor.

        If this class is used as part of a LibraryAuthenticator, the
        `integration` passed in will be ignored. This class is designed
        to be configured by customizing arguments such as `patrons`.
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
    
AuthenticationProvider = MockAuthenticationProvider
