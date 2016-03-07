from config import (
    Configuration,
    CannotLoadConfiguration,
)

class Authenticator(object):

    @classmethod
    def initialize(cls, _db, test=False):
        if test:
            from millenium_patron import (
                DummyMilleniumPatronAPI,
            )
            return DummyMilleniumPatronAPI()

        provider = Configuration.policy("authentication")
        if not provider:
            raise CannotLoadConfiguration(
                "No authentication policy given."
            )
        if provider == 'Millenium':
            from millenium_patron import (
                MilleniumPatronAPI,
            )
            api = MilleniumPatronAPI.from_environment()
        elif provider == 'First Book':
            from firstbook import FirstBookAuthenticationAPI
            api = FirstBookAuthenticationAPI.from_config()
        else:
            raise CannotLoadConfiguration(
                "Unrecognized authentication provider: %s" % provider
            )
        return api

    def server_side_validation(self, identifier, password):
        if not hasattr(self, 'identifier_re'):
            self.identifier_re = Configuration.policy(
                Configuration.IDENTIFIER_REGULAR_EXPRESSION,
                default=Configuration.DEFAULT_IDENTIFIER_REGULAR_EXPRESSION)
        if not hasattr(self, 'password_re'):
            self.password_re = Configuration.policy(
                Configuration.PASSWORD_REGULAR_EXPRESSION,
                default=Configuration.DEFAULT_PASSWORD_REGULAR_EXPRESSION)

        valid = True
        if self.identifier_re:
            valid = valid and (self.identifier_re.match(identifier) is not None)
        if self.password_re:
            valid = valid and (self.password_re.match(password) is not None)
        return valid

    def authenticated_patron(self, _db, identifier, password):
        pass
