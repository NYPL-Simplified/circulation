from nose.tools import set_trace
import flask
from flask import Response
from flask_babel import lazy_gettext as _
from api.admin.problem_details import *
from core.util.problem_detail import ProblemDetail
from . import SettingsController

class SelfTestsController(SettingsController):
    def _manage_self_tests(self, identifier):
        """Generic request-processing method."""
        if not identifier:
            return MISSING_IDENTIFIER
        if flask.request.method == "GET":
            return self.self_tests_process_get(identifier)
        else:
            return self.self_tests_process_post(identifier)

    def find_protocol_class(self, integration):
        """Given an ExternalIntegration, find the class on which run_tests()
        or prior_test_results() should be called, and any extra
        arguments that should be passed into the call.
        """
        if not hasattr(self, "_find_protocol_class"):
            raise NotImplementedError()
        protocol_class = self._find_protocol_class(integration)
        if isinstance(protocol_class, tuple):
            protocol_class, extra_arguments = protocol_class
        else:
            extra_arguments = ()
        return protocol_class, extra_arguments

    def get_info(self, integration):
        protocol_class, ignore = self.find_protocol_class(integration)
        [protocol] = self._get_integration_protocols([protocol_class])
        return dict(
            id=integration.id,
            name=integration.name,
            protocol=protocol,
            settings=protocol.get("settings"),
            goal=integration.goal
        )

    def run_tests(self, integration):
        protocol_class, extra_arguments = self.find_protocol_class(integration)
        value, results = protocol_class.run_self_tests(
            self._db, *extra_arguments
        )
        return value

    def self_tests_process_get(self, identifier):
        integration = self.look_up_by_id(identifier)
        if isinstance(integration, ProblemDetail):
            return integration
        info = self.get_info(integration)
        protocol_class, extra_arguments = self.find_protocol_class(integration)
        info["self_test_results"] = self._get_prior_test_results(
            integration, protocol_class, *extra_arguments
        )
        return dict(self_test_results=info)

    def self_tests_process_post(self, identifier):
        integration = self.look_up_by_id(identifier)
        if isinstance (integration, ProblemDetail):
            return integration
        value = self.run_tests(integration)
        if value and isinstance(value, ProblemDetail):
            return value
        elif value:
            return Response(_("Successfully ran new self tests"), 200)

        return FAILED_TO_RUN_SELF_TESTS.detailed(
            _("Failed to run self tests for this %(type)s.", type=self.type)
        )
