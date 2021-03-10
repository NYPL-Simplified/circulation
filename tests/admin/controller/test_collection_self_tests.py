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
from .test_controller import SettingsControllerTest

class TestCollectionSelfTests(SettingsControllerTest):
    def test_collection_self_tests_with_no_identifier(self):
        with self.request_context_with_admin("/"):
            response = self.manager.admin_collection_self_tests_controller.process_collection_self_tests(None)
            eq_(response.title, MISSING_IDENTIFIER.title)
            eq_(response.detail, MISSING_IDENTIFIER.detail)
            eq_(response.status_code, 400)

    def test_collection_self_tests_with_no_collection_found(self):
        with self.request_context_with_admin("/"):
            response = self.manager.admin_collection_self_tests_controller.process_collection_self_tests(-1)
            eq_(response, NO_SUCH_COLLECTION)
            eq_(response.status_code, 404)

    def test_collection_self_tests_test_get(self):
        old_prior_test_results = HasSelfTests.prior_test_results
        HasSelfTests.prior_test_results = self.mock_prior_test_results
        collection = MockAxis360API.mock_collection(self._db)

        # Make sure that HasSelfTest.prior_test_results() was called and that
        # it is in the response's collection object.
        with self.request_context_with_admin("/"):
            response = self.manager.admin_collection_self_tests_controller.process_collection_self_tests(collection.id)

            responseCollection = response.get("self_test_results")

            eq_(responseCollection.get("id"), collection.id)
            eq_(responseCollection.get("name"), collection.name)
            eq_(responseCollection.get("protocol"), collection.protocol)
            eq_(responseCollection.get("self_test_results"), self.self_test_results)

        HasSelfTests.prior_test_results = old_prior_test_results

    def test_collection_self_tests_failed_post(self):
        # This makes HasSelfTests.run_self_tests return no values
        old_run_self_tests = HasSelfTests.run_self_tests
        HasSelfTests.run_self_tests = self.mock_failed_run_self_tests

        collection = MockAxis360API.mock_collection(self._db)

        # Failed to run self tests
        with self.request_context_with_admin("/", method="POST"):
            response = self.manager.admin_collection_self_tests_controller.process_collection_self_tests(collection.id)

            (run_self_tests_args, run_self_tests_kwargs) = self.failed_run_self_tests_called_with
            eq_(response.title, FAILED_TO_RUN_SELF_TESTS.title)
            eq_(response.detail, "Failed to run self tests for this collection.")
            eq_(response.status_code, 400)

        HasSelfTests.run_self_tests = old_run_self_tests

    def test_collection_self_tests_post(self):
        old_run_self_tests = HasSelfTests.run_self_tests
        HasSelfTests.run_self_tests = self.mock_run_self_tests

        collection = self._collection()
        # Successfully ran new self tests for the OPDSImportMonitor provider API
        with self.request_context_with_admin("/", method="POST"):
            response = self.manager.admin_collection_self_tests_controller.process_collection_self_tests(collection.id)

            (run_self_tests_args, run_self_tests_kwargs) = self.run_self_tests_called_with
            eq_(response.response, _("Successfully ran new self tests"))
            eq_(response._status, "200 OK")

            # The provider API class and the collection should be passed to
            # the run_self_tests method of the provider API class.
            eq_(run_self_tests_args[1], OPDSImportMonitor)
            eq_(run_self_tests_args[3], collection)


        collection = MockAxis360API.mock_collection(self._db)
        # Successfully ran new self tests
        with self.request_context_with_admin("/", method="POST"):
            response = self.manager.admin_collection_self_tests_controller.process_collection_self_tests(collection.id)

            (run_self_tests_args, run_self_tests_kwargs) = self.run_self_tests_called_with
            eq_(response.response, _("Successfully ran new self tests"))
            eq_(response._status, "200 OK")

            # The provider API class and the collection should be passed to
            # the run_self_tests method of the provider API class.
            eq_(run_self_tests_args[1], Axis360API)
            eq_(run_self_tests_args[3], collection)

        collection = MockAxis360API.mock_collection(self._db)
        collection.protocol = "Non existing protocol"
        # clearing out previous call to mocked run_self_tests
        self.run_self_tests_called_with = (None, None)

        # No protocol found so run_self_tests was not called
        with self.request_context_with_admin("/", method="POST"):
            response = self.manager.admin_collection_self_tests_controller.process_collection_self_tests(collection.id)

            (run_self_tests_args, run_self_tests_kwargs) = self.run_self_tests_called_with
            eq_(response.title, FAILED_TO_RUN_SELF_TESTS.title)
            eq_(response.detail, "Failed to run self tests for this collection.")
            eq_(response.status_code, 400)

            # The method returns None but it was not called
            eq_(run_self_tests_args, None)

        HasSelfTests.run_self_tests = old_run_self_tests
