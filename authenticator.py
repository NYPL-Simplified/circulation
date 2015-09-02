from config import (
    Configuration,
    CannotLoadConfiguration,
)

class Authenticator(object):

    @classmethod
    def initialize(cls, _db, test=False):
        provider = Configuration.policy("authentication")
        if not provider:
            raise CannotLoadConfiguration(
                "No authentication policy given."
            )
        if provider == 'Millenium':
            from millenium_patron import (
                DummyMilleniumPatronAPI,
                MilleniumPatronAPI,
            )
            if test:
                return DummyMilleniumPatronAPI()
            else:
                return MilleniumPatronAPI.from_environment()
        elif provider == 'First Book':
            from firstbook import FirstBookAuthenticationAPI
            return FirstBookAuthenticationAPI.from_config()
        else:
            raise CannotLoadConfiguration(
                "Unrecognized authentication provider: %s" % provider
            )

    def authenticated_patron(self, _db, identifier, password):
        pass
