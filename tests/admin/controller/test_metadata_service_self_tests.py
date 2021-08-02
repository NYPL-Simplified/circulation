
from flask_babel import lazy_gettext as _
from core.selftest import (
    HasSelfTests,
    SelfTestResult,
)
from .test_controller import SettingsControllerTest
from core.model import (
    create,
    ExternalIntegration,
)
from core.opds_import import MetadataWranglerOPDSLookup

from api.admin.problem_details import *
from api.nyt import NYTBestSellerAPI

class TestMetadataServiceSelfTests(SettingsControllerTest):

    def test_metadata_service_self_tests_with_no_identifier(self):
        with self.request_context_with_admin("/"):
            response = self.manager.admin_metadata_service_self_tests_controller.process_metadata_service_self_tests(None)
            assert response.title == MISSING_IDENTIFIER.title
            assert response.detail == MISSING_IDENTIFIER.detail
            assert response.status_code == 400

    def test_metadata_service_self_tests_with_no_metadata_service_found(self):
        with self.request_context_with_admin("/"):
            response = self.manager.admin_metadata_service_self_tests_controller.process_metadata_service_self_tests(-1)
            assert response == MISSING_SERVICE
            assert response.status_code == 404

    def test_metadata_service_self_tests_test_get(self):
        old_prior_test_results = HasSelfTests.prior_test_results
        HasSelfTests.prior_test_results = self.mock_prior_test_results
        metadata_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.NYT,
            goal=ExternalIntegration.METADATA_GOAL,
        )
        # Make sure that HasSelfTest.prior_test_results() was called and that
        # it is in the response's self tests object.
        with self.request_context_with_admin("/"):
            response = self.manager.admin_metadata_service_self_tests_controller.process_metadata_service_self_tests(metadata_service.id)
            response_metadata_service = response.get("self_test_results")

            assert response_metadata_service.get("id") == metadata_service.id
            assert response_metadata_service.get("name") == metadata_service.name
            assert response_metadata_service.get("protocol").get("label") == NYTBestSellerAPI.NAME
            assert response_metadata_service.get("goal") == metadata_service.goal
            assert (
                response_metadata_service.get("self_test_results") ==
                HasSelfTests.prior_test_results())
        HasSelfTests.prior_test_results = old_prior_test_results

    def test_metadata_service_self_tests_post(self):
        old_run_self_tests = HasSelfTests.run_self_tests
        HasSelfTests.run_self_tests = self.mock_run_self_tests

        metadata_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.NYT,
            goal=ExternalIntegration.METADATA_GOAL
        )
        m = self.manager.admin_metadata_service_self_tests_controller.self_tests_process_post
        with self.request_context_with_admin("/", method="POST"):
            response = m(metadata_service.id)
            assert response._status == "200 OK"
            assert "Successfully ran new self tests" == response.get_data(as_text=True)

        positional, keyword = self.run_self_tests_called_with
        # run_self_tests was called with positional arguments:
        # * The database connection
        # * The method to call to instantiate a HasSelfTests implementation
        #   (NYTBestSellerAPI.from_config)
        # * The database connection again (to be passed into
        #   NYTBestSellerAPI.from_config).
        assert (
            (
                self._db,
                NYTBestSellerAPI.from_config,
                self._db
            ) ==
            positional)

        # run_self_tests was not called with any keyword arguments.
        assert {} == keyword

        # Undo the mock.
        HasSelfTests.run_self_tests = old_run_self_tests
