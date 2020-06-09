from api.saml.provider import SAMLAuthenticationProvider
from core.model import ExternalIntegration
from tests.test_controller import ControllerTest as BaseControllerTest


class ControllerTest(BaseControllerTest):
    def __init__(self):
        self._library = None
        self._integration = None

    def setup(self, _db=None, set_up_circulation_manager=True):
        super(ControllerTest, self).setup(_db, set_up_circulation_manager)

        self._library = self.make_default_library(self._db)
        self._integration = self._external_integration(
            protocol=SAMLAuthenticationProvider.NAME,
            goal=ExternalIntegration.PATRON_AUTH_GOAL
        )
