from nose.tools import (
    set_trace,
    eq_,
    assert_raises
)
from flask_babel import lazy_gettext as _
from api.admin.problem_details import *
from api.axis import (Axis360API, MockAxis360API)
from core.opds_import import (OPDSImporter, OPDSImportMonitor)
from core.selftest import HasSelfTests
from test_controller import SettingsControllerTest
from core.model import (
    create,
    ExternalIntegration,
)
from core.external_search import ExternalSearchIndex, MockExternalSearchIndex, MockSearchResult

class TestSearchServiceSelfTests(SettingsControllerTest):
    def test_search_service_self_tests_with_no_identifier(self):
        with self.request_context_with_admin("/"):
            response = self.manager.admin_search_service_self_tests_controller.process_search_service_self_tests(None)
            eq_(response, MISSING_SEARCH_SERVICE_IDENTIFIER)
            eq_(response.status_code, 400)

    def test_search_service_self_tests_with_no_search_service_found(self):
        with self.request_context_with_admin("/"):
            response = self.manager.admin_search_service_self_tests_controller.process_search_service_self_tests(-1)
            eq_(response, MISSING_SERVICE)
            eq_(response.status_code, 404)

    def test_search_service_self_tests_test_get(self):
        old_prior_test_results = HasSelfTests.prior_test_results
        HasSelfTests.prior_test_results = self.mock_prior_test_results
        search_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.ELASTICSEARCH,
            goal=ExternalIntegration.SEARCH_GOAL,
        )
        # Make sure that HasSelfTest.prior_test_results() was called and that
        # it is in the response's collection object.
        with self.request_context_with_admin("/"):
            response = self.manager.admin_search_service_self_tests_controller.process_search_service_self_tests(search_service.id)
            responseSearchService = response.get("search_service")

            eq_(responseSearchService.get("id"), search_service.id)
            eq_(responseSearchService.get("name"), search_service.name)
            eq_(responseSearchService.get("protocol"), search_service.protocol)
            eq_(responseSearchService.get("goal"), search_service.goal)
            eq_(responseSearchService.get("self_test_results"), self.self_test_results)

        HasSelfTests.prior_test_results = old_prior_test_results

    def test_search_service_self_tests_failed_post(self):
        # This makes HasSelfTests.run_self_tests return no values
        old_run_self_tests = HasSelfTests.run_self_tests

        search_index = MockExternalSearchIndex()
        search_index._run_self_tests = self.mock_failed_run_search_service_self_tests

        search_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.ELASTICSEARCH,
            goal=ExternalIntegration.SEARCH_GOAL
        )
        search_service.setting("test_search_term").value = "testing"

        m = self.manager.admin_search_service_self_tests_controller.process_post
        # Failed to run self tests
        with self.request_context_with_admin("/", method="POST"):
            response = m(search_service.id, search_index)
            eq_(response, FAILED_TO_RUN_SEARCH_SELF_TESTS)
            eq_(response.status_code, 400)

        HasSelfTests.run_self_tests = old_run_self_tests

    def test_search_service_self_tests_no_results(self):
        old_run_self_tests = HasSelfTests.run_self_tests
        HasSelfTests.run_self_tests = self.mock_run_self_tests

        search_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.ELASTICSEARCH,
            goal=ExternalIntegration.SEARCH_GOAL
        )
        search_service.setting("test_search_term").value = "testing"

        m = self.manager.admin_search_service_self_tests_controller.process_post
        search_index = MockExternalSearchIndex()

        with self.request_context_with_admin("/", method="POST"):
            response = m(search_service.id, search_index)
            eq_(response, NO_SEARCH_RESULTS)
            eq_(response.status_code, 404)

        HasSelfTests.run_self_tests = old_run_self_tests

    def test_search_service_self_tests_post(self):
        old_run_self_tests = HasSelfTests.run_self_tests
        search_index = MockExternalSearchIndex()
        HasSelfTests.run_self_tests = self.mock_run_self_tests

        search_result = MockSearchResult("Sample Book Title", "author", {}, "id")
        search_index.index("1", "2", "3", search_result)

        search_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.ELASTICSEARCH,
            goal=ExternalIntegration.SEARCH_GOAL
        )
        search_service.setting("test_search_term").value = "testing"

        m = self.manager.admin_search_service_self_tests_controller.process_post

        with self.request_context_with_admin("/", method="POST"):
            response = m(search_service.id, search_index)
            eq_(response.response.get("result"), ["Sample Book Title"])
            eq_(response.response.get("name"), "Searching for the specified term")
            eq_(response._status, "200 OK")

        HasSelfTests.run_self_tests = old_run_self_tests
