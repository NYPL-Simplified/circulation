from nose.tools import (
    set_trace,
    eq_,
    assert_raises
)
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
        """The configuration of search and logging collections is delegated to
        the _manage_sitewide_service and _delete_integration methods.

        Since search collections are more comprehensively tested in test_search_services,
        this provides test coverage for storage collections."""

        # Test storage services first.

        class MockStorage(StorageServicesController):
            def _manage_sitewide_service(self,*args):
                self.manage_called_with = args

            def _delete_integration(self, *args):
                self.delete_called_with = args
        controller = MockStorage(self.manager)
        EI = ExternalIntegration
        with self.request_context_with_admin("/"):
            controller.process_services()
            goal, apis, key_name, problem = controller.manage_called_with
            eq_(EI.STORAGE_GOAL, goal)
            assert S3Uploader in apis
            eq_('storage_services', key_name)
            assert 'new storage service' in problem

        with self.request_context_with_admin("/"):
            id = object()
            controller.process_delete(id)
            eq_((id, EI.STORAGE_GOAL), controller.delete_called_with)
