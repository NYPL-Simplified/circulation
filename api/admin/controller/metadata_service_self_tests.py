"""Self-tests for metadata integrations."""
from nose.tools import set_trace
from flask_babel import lazy_gettext as _
from core.opds_import import MetadataWranglerOPDSLookup
from core.model import (
    ExternalIntegration
)
from api.nyt import NYTBestSellerAPI
from api.admin.controller.self_tests import SelfTestsController
from api.admin.controller.metadata_services import MetadataServicesController

class MetadataServiceSelfTestsController(MetadataServicesController, SelfTestsController):

    def __init__(self, manager):
        super(MetadataServiceSelfTestsController, self).__init__(manager)
        self.type = _("metadata service")

    def process_metadata_service_self_tests(self, identifier):
        return self._manage_self_tests(identifier)

    def look_up_by_id(self, id):
        return self.look_up_service_by_id(
            id, protocol=None, goal=ExternalIntegration.METADATA_GOAL
        )
