from api.lcp.hash import HasherFactory
from api.lcp.server import LCPServer
from core.lcp.credential import LCPCredentialFactory
from core.model.configuration import ConfigurationStorage, ConfigurationFactory


class LCPServerFactory(object):
    """Creates a new instance of LCPServer"""

    def create(self, integration_association):
        """Creates a new instance of LCPServer

        :param integration_association: Association with an external integration
        :type integration_association: core.model.configuration.HasExternalIntegration

        :return: New instance of LCPServer
        :rtype: LCPServer
        """
        configuration_storage = ConfigurationStorage(integration_association)
        configuration_factory = ConfigurationFactory()
        hasher_factory = HasherFactory()
        credential_factory = LCPCredentialFactory()
        lcp_server = LCPServer(configuration_storage, configuration_factory, hasher_factory, credential_factory)

        return lcp_server
