from api.saml.provider import SAMLWebSSOAuthenticationProvider
from core.model import ExternalIntegration
from tests.test_controller import ControllerTest as BaseControllerTest


class ControllerTest(BaseControllerTest):

    def setup_method(self):
        self._integration = None
        super(ControllerTest, self).setup_method()

        self._integration = self._external_integration(
            protocol=SAMLWebSSOAuthenticationProvider.NAME,
            goal=ExternalIntegration.PATRON_AUTH_GOAL,
        )
