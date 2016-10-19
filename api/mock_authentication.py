from nose.tools import set_trace

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
    
    @classmethod
    def config_values(cls):
        config, values = super(MockAuthenticationProvider, cls).config_values()
        if cls.PATRONS not in config:
            raise CannotLoadConfiguration(
                "No patrons configured for mock authentication provider."
            )

        values['patrons'] = config.get(cls.PATRONS)
        return config, values
        
    def __init__(self, patrons=None, *args, **kwargs):
        self.patrons = patrons
        super(MockAuthenticationProvider, self).__init__(*args, **kwargs)

    # Begin implementation of BasicAuthenticationProvider abstract
    # methods.
    def remote_authenticate(self, username, password):
        if not username or not password:
            return None
        # This is what success would look like.
        success = PatronData(authorization_identifier=username)
        if self.patrons:
            # We are authenticating patrons from a specific list.
            # This is the best solution when setting up a collection
            # without connecting it to an ILS.
            if username in self.patrons and self.patrons[username]==password:
                return success
            return None
        else:
            # We will authenticate any patron if their password is the
            # first four characters of their username. This is the
            # solution we use in tests.
            if len(password) == 4 and username.startswith(password):
                return success
            return None

AuthenticationProvider = MockAuthenticationProvider
