"""Self-tests for metadata integrations."""
from nose.tools import set_trace
from flask_babel import lazy_gettext as _
from core.opds_import import MetadataWranglerOPDSLookup
from core.model import (
    ExternalIntegration
)
from api.nyt import NYTBestSellerAPI
from api.admin.controller.self_tests import SelfTestsController

class MetadataServiceSelfTestsController(SelfTestsController):

    def __init__(self, manager):
        super(MetadataServiceSelfTestsController, self).__init__(manager)
        self.type = _("metadata service")

    def _find_protocol_class(self, integration):
        """Given an ExternalIntegration, find the class on which run_tests()
        or prior_test_results() should be called, and any extra
        arguments that should be passed into the call.
        """
        if integration.protocol == ExternalIntegration.METADATA_WRANGLER:
            return (
                MetadataWranglerOPDSLookup,
                (MetadataWranglerOPDSLookup.from_config, self._db)
            )
        elif integration.protocol == ExternalIntegration.NYT:
            return (
                NYTBestSellerAPI,
                (NYTBestSellerAPI.from_config, self._db)
            )
        raise NotImplementedError(
            "No metadata self-test class for protocol %s" % integration.protocol
        )

    def process_metadata_service_self_tests(self, identifier):
        return self._manage_self_tests(identifier)

    def look_up_by_id(self, id):
        return self.look_up_service_by_id(
            id, protocol=None, goal=ExternalIntegration.METADATA_GOAL
        )
