"""Self-tests for metadata integrations."""
from nose.tools import set_trace
from flask_babel import lazy_gettext as _
from core.opds_import import MetadataWranglerOPDSLookup
from core.model import (
    ExternalIntegration
)
from api.nyt import NYTBestSellerAPI
from api.admin.controller.self_tests import SelfTestsController

class MetadataSelfTestsController(SelfTestsController):

    def __init__(self, manager):
        super(MetadataSelfTestsController, self).__init__(manager)
        self.type = _("metadata service")

        # Set up the information necessary to call run_self_tests
        # on metadata integrations with different protocols.
        self.test_providers = {
            ExternalIntegration.METADATA_WRANGLER : {
                "class": MetadataWranglerOPDSLookup,
                "args" : (MetadataWranglerOPDSLookup.from_config, self._db)
            },
            ExternalIntegration.NYT : {
                "class": NYTBestSellerAPI,
                "args" : (NYTBestSellerAPI.from_config, self._db)
            },
        }

    def process_metadata_self_tests(self, identifier):        
        return self._manage_self_tests(identifier)

    def look_up_by_id(self, id):
        return self.look_up_service_by_id(
            id, ExternalIntegration.METADATA_GOAL
        )

    def get_info(self, integration):
        protocol_class = self.test_providers[integration.protocol]['class']
        [protocol] = self._get_integration_protocols([protocol_class])
        return dict(
            id=integration.id,
            name=integration.name,
            protocol=protocol,
            settings=protocol.get("settings"),
            goal=integration.goal
        )

    def run_tests(self, integration):
        """Run the tests for a given ExternalIntegration."""
        # Look up the class that provides tests for the given protocol,
        # and the arguments that need to be passed into a run_self_tests()
        # call to instantiate an instance of that class.
        data = self.test_providers[integration.protocol]
        cls = data['class']
        args = data.get('args') or ()

        # Then call run_self_tests().
        value, results = cls.run_self_tests(self._db, *args)
        return value
