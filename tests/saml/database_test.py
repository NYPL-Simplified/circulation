from api.saml.provider import SAMLAuthenticationProvider
from core.model import ExternalIntegration
from core.testing import DatabaseTest as BaseDatabaseTest


class DatabaseTest(BaseDatabaseTest):
    def __init__(self):
        self._library = None
        self._integration = None
        self._authentication_provider = None

    def setup(self, mock_search=True):
        super(DatabaseTest, self).setup(mock_search)

        self._library = self.make_default_library(self._db)
        self._integration = self._external_integration(
            protocol=SAMLAuthenticationProvider.NAME,
            goal=ExternalIntegration.PATRON_AUTH_GOAL
        )
        self._authentication_provider = SAMLAuthenticationProvider(self._library, self._integration)
