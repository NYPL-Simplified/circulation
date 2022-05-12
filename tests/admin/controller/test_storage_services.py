
import flask
from api.admin.controller import SettingsController
from api.admin.controller.storage_services import StorageServicesController
from core.model import (
    ExternalIntegration,
)
from core.s3 import S3Uploader, MockS3Uploader
from test_controller import SettingsControllerTest

class TestStorageServices(SettingsControllerTest):
    def test_storage_service_management(self):

        class MockStorage(StorageServicesController):
            def _get_integration_protocols(self, apis, protocol_name_attr):
                self.manage_called_with = (apis, protocol_name_attr)

            def _delete_integration(self, *args):
                self.delete_called_with = args
        controller = MockStorage(self.manager)
        EI = ExternalIntegration
        with self.request_context_with_admin("/"):
            controller.process_services()
            (apis, procotol_name) = controller.manage_called_with

            assert S3Uploader in apis
            assert procotol_name == 'NAME'

        with self.request_context_with_admin("/"):
            id = object()
            controller.process_delete(id)
            assert (id, EI.STORAGE_GOAL) == controller.delete_called_with
