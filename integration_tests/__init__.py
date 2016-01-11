from nose.tools import set_trace

from ..config import Configuration

class CirculationIntegrationTest(object):

    def setup(self):
        Configuration.load()
        self.url = Configuration.integration_url(Configuration.CIRCULATION_MANAGER_INTEGRATION)

        millenium = Configuration.integration(Configuration.MILLENIUM_INTEGRATION)
        self.test_username = millenium.get(Configuration.AUTHENTICATION_TEST_USERNAME)
        self.test_password = millenium.get(Configuration.AUTHENTICATION_TEST_PASSWORD)

