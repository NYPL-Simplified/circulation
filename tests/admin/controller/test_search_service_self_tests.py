
from flask_babel import lazy_gettext as _
from api.admin.problem_details import *
from api.axis import (Axis360API, MockAxis360API)
from core.opds_import import (OPDSImporter, OPDSImportMonitor)
from core.selftest import (
    HasSelfTests,
    SelfTestResult,
)
from .test_controller import SettingsControllerTest
from core.model import (
    create,
    ExternalIntegration,
)
from core.external_search import ExternalSearchIndex, MockExternalSearchIndex, MockSearchResult

class TestSearchServiceSelfTests(SettingsControllerTest):
    def test_search_service_self_tests_with_no_identifier(self):
        with self.request_context_with_admin("/"):
            response = self.manager.admin_search_service_self_tests_controller.process_search_service_self_tests(None)
            assert response.title == MISSING_IDENTIFIER.title
            assert response.detail == MISSING_IDENTIFIER.detail
            assert response.status_code == 400


    def test_search_service_self_tests_with_no_search_service_found(self):
        with self.request_context_with_admin("/"):
            response = self.manager.admin_search_service_self_tests_controller.process_search_service_self_tests(-1)
            assert response == MISSING_SERVICE
            assert response.status_code == 404

    def test_search_service_self_tests_test_get(self):
        old_prior_test_results = HasSelfTests.prior_test_results
        HasSelfTests.prior_test_results = self.mock_prior_test_results
        search_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.ELASTICSEARCH,
            goal=ExternalIntegration.SEARCH_GOAL,
        )
        # Make sure that HasSelfTest.prior_test_results() was called and that
        # it is in the response's self tests object.
        with self.request_context_with_admin("/"):
            response = self.manager.admin_search_service_self_tests_controller.process_search_service_self_tests(search_service.id)
            response_search_service = response.get("self_test_results")

            assert response_search_service.get("id") == search_service.id
            assert response_search_service.get("name") == search_service.name
            assert response_search_service.get("protocol").get("label") == search_service.protocol
            assert response_search_service.get("goal") == search_service.goal
            assert (
                response_search_service.get("self_test_results") ==
                HasSelfTests.prior_test_results())

        HasSelfTests.prior_test_results = old_prior_test_results

    def test_search_service_self_tests_post(self):
        old_run_self_tests = HasSelfTests.run_self_tests
        HasSelfTests.run_self_tests = self.mock_run_self_tests

        search_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.ELASTICSEARCH,
            goal=ExternalIntegration.SEARCH_GOAL
        )
        m = self.manager.admin_search_service_self_tests_controller.self_tests_process_post
        with self.request_context_with_admin("/", method="POST"):
            response = m(search_service.id)
            assert response._status == "200 OK"
            assert "Successfully ran new self tests" == response.get_data(as_text=True)

        positional, keyword = self.run_self_tests_called_with
        # run_self_tests was called with positional arguments:
        # * The database connection
        # * The method to call to instantiate a HasSelfTests implementation
        #   (None -- this means to use the default ExternalSearchIndex
        #   constructor.)
        # * The database connection again (to be passed into
        #   the ExternalSearchIndex constructor).
        assert (self._db, None, self._db) == positional

        # run_self_tests was not called with any keyword arguments.
        assert {} == keyword

        # Undo the mock.
        HasSelfTests.run_self_tests = old_run_self_tests
