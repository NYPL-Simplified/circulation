from api.authenticator import BasicAuthenticationProvider
from core.util.http import RemoteIntegrationException


class MockExplodingAuthenticationProvider(BasicAuthenticationProvider):
    def __init__(self, library, integration, analytics=None, patron=None, patrondata=None, *args, **kwargs):
        raise RemoteIntegrationException("Mock", "Mock exploded.")

    def authenticate(self, _db, header):
        pass

    def remote_authenticate(self, username, password):
        pass

    def remote_patron_lookup(self, patrondata):
        pass


AuthenticationProvider = MockExplodingAuthenticationProvider
