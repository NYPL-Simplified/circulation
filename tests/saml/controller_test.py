from api.saml.provider import SAMLWebSSOAuthenticationProvider
from core.model import ExternalIntegration
from tests.test_controller import ControllerTest as BaseControllerTest


class ControllerTest(BaseControllerTest):
    def __init__(self):
        self._integration = None

    def setup(self, _db=None, set_up_circulation_manager=True):
        super(ControllerTest, self).setup(_db, set_up_circulation_manager)

        self._integration = self._external_integration(
            protocol=SAMLWebSSOAuthenticationProvider.NAME,
            goal=ExternalIntegration.PATRON_AUTH_GOAL,
        )
