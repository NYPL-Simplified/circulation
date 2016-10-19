from nose.tools import set_trace
import datetime

from authenticator import (
    BasicAuthenticationProvider,
    PatronData,
)

from config import (
    CannotLoadConfiguration,
)


class MockAuthenticationProvider(BasicAuthenticationProvider):
    """An authentication provider that authenticates a predefined
    set of patrons.

    This is used primarily in tests, but it can also be used to set up
    a working circulation manager before connecting it to an ILS.
    """

    PATRONS = 'patrons'
    EXPIRED_PATRONS = 'expired_patrons'
    PATRONS_WITH_FINES = 'patrons_with_fines'
    
    @classmethod
    def config_values(cls):
        config, values = super(MockAuthenticationProvider, cls).config_values()
        if cls.PATRONS not in config:
            raise CannotLoadConfiguration(
                "No patrons configured for mock authentication provider."
            )

        values['patrons'] = config.get(cls.PATRONS)
        values['expired_patrons'] = config.get(cls.EXPIRED_PATRONS)
        values['patrons_with_fines'] = config.get(cls.PATRONS_WITH_FINES)
        return config, values
        
    def __init__(self, patrons=None, expired_patrons=None,
                 patrons_with_fines=None, *args, **kwargs):
        self.patrons = patrons
        self.expired_patrons = expired_patrons
        self.patrons_with_fines = patrons_with_fines
        super(MockAuthenticationProvider, self).__init__(*args, **kwargs)

    # Begin implementation of BasicAuthenticationProvider abstract
    # methods.

    def remote_authenticate(self, username, password):
        if not username or not password:
            return None

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
            patrondata.fines = "$12345678.90"
        else:
            return None
        return patrondata

    # End implementation of BasicAuthenticationProvider abstract
    # methods.

    def valid_patron(self, username, password, patrons):
        """Is this patron associated with the given password in 
        the given dictionary?
        """
        return username in patrons and patrons[username]==password
        
AuthenticationProvider = MockAuthenticationProvider
