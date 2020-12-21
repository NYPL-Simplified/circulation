from api.saml.provider import SAMLWebSSOAuthenticationProvider
from core.model import ExternalIntegration
from core.testing import DatabaseTest as BaseDatabaseTest


class DatabaseTest(BaseDatabaseTest):
    def __init__(self):
        self._integration = None
        self._authentication_provider = None

    def setup(self, mock_search=True):
        super(DatabaseTest, self).setup(mock_search)

        self._integration = self._external_integration(
            protocol=SAMLWebSSOAuthenticationProvider.NAME,
            goal=ExternalIntegration.PATRON_AUTH_GOAL,
        )
        self._authentication_provider = SAMLWebSSOAuthenticationProvider(
            self._default_library, self._integration
        )
