from nose.tools import (
    set_trace,
    eq_,
    assert_raises
)
from flask_babel import lazy_gettext as _
from api.admin.problem_details import *
from core.selftest import (
    HasSelfTests,
    SelfTestResult,
)
from api.simple_authentication import SimpleAuthenticationProvider
from .test_controller import SettingsControllerTest
from core.model import (
    create,
    ExternalIntegration,
)

class TestPatronAuthSelfTests(SettingsControllerTest):

    def _auth_service(self, libraries=[]):
        auth_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=SimpleAuthenticationProvider.__module__,
            goal=ExternalIntegration.PATRON_AUTH_GOAL,
            name="name",
            libraries=libraries
        )
        return auth_service

    def test_patron_auth_self_tests_with_no_identifier(self):
        with self.request_context_with_admin("/"):
            response = self.manager.admin_patron_auth_service_self_tests_controller.process_patron_auth_service_self_tests(None)
            eq_(response.title, MISSING_IDENTIFIER.title)
            eq_(response.detail, MISSING_IDENTIFIER.detail)
            eq_(response.status_code, 400)

    def test_patron_auth_self_tests_with_no_auth_service_found(self):
        with self.request_context_with_admin("/"):
            response = self.manager.admin_patron_auth_service_self_tests_controller.process_patron_auth_service_self_tests(-1)
            eq_(response, MISSING_SERVICE)
            eq_(response.status_code, 404)

    def test_patron_auth_self_tests_get_with_no_libraries(self):
        auth_service = self._auth_service()
        with self.request_context_with_admin("/"):
            response = self.manager.admin_patron_auth_service_self_tests_controller.process_patron_auth_service_self_tests(auth_service.id)
            results = response.get("self_test_results").get("self_test_results")
            eq_(results.get("disabled"), True)
            eq_(results.get("exception"), "You must associate this service with at least one library before you can run self tests for it.")

    def test_patron_auth_self_tests_test_get(self):
        old_prior_test_results = HasSelfTests.prior_test_results
        HasSelfTests.prior_test_results = self.mock_prior_test_results
        auth_service = self._auth_service([self._library()])

        # Make sure that HasSelfTest.prior_test_results() was called and that
        # it is in the response's self tests object.
        with self.request_context_with_admin("/"):
            response = self.manager.admin_patron_auth_service_self_tests_controller.process_patron_auth_service_self_tests(auth_service.id)
            response_auth_service = response.get("self_test_results")

            eq_(response_auth_service.get("name"), auth_service.name)
            eq_(response_auth_service.get("protocol"), auth_service.protocol)
            eq_(response_auth_service.get("id"), auth_service.id)
            eq_(response_auth_service.get("goal"), auth_service.goal)
            eq_(response_auth_service.get("self_test_results"), self.self_test_results)

        HasSelfTests.prior_test_results = old_prior_test_results

    def test_patron_auth_self_tests_post_with_no_libraries(self):
        auth_service = self._auth_service()
        with self.request_context_with_admin("/", method="POST"):
            response = self.manager.admin_patron_auth_service_self_tests_controller.process_patron_auth_service_self_tests(auth_service.id)
            eq_(response.title, FAILED_TO_RUN_SELF_TESTS.title)
            eq_(response.detail, "Failed to run self tests for this patron authentication service.")
            eq_(response.status_code, 400)

    def test_patron_auth_self_tests_test_post(self):
        old_run_self_tests = HasSelfTests.run_self_tests
        HasSelfTests.run_self_tests = self.mock_run_self_tests
        auth_service = self._auth_service([self._library()])

        with self.request_context_with_admin("/", method="POST"):
            response = self.manager.admin_patron_auth_service_self_tests_controller.process_patron_auth_service_self_tests(auth_service.id)
            eq_(response._status, "200 OK")
            eq_("Successfully ran new self tests", response.data)

        # run_self_tests was called with the database twice (the
        # second time to be used in the ExternalSearchIntegration
        # constructor). There were no keyword arguments.
        eq_(((self._db, None, auth_service.libraries[0], auth_service), {}), self.run_self_tests_called_with)

        HasSelfTests.run_self_tests = old_run_self_tests
