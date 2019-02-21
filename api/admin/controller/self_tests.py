from nose.tools import set_trace
import flask
from flask import Response
from flask_babel import lazy_gettext as _
from api.admin.problem_details import *
from core.util.problem_detail import ProblemDetail
from . import SettingsController

class SelfTestsController(SettingsController):
    def _manage_self_tests(self, identifier):
        if not identifier:
            return MISSING_IDENTIFIER
        if flask.request.method == "GET":
            return self.process_get(identifier)
        else:
            return self.process_post(identifier)

    def process_get(self, identifier):
        item = self.look_up_by_id(identifier)
        if isinstance(item, ProblemDetail):
            return item
        info = self.get_info(item)
        info["self_test_results"] = self._get_prior_test_results(item)
        return dict(self_test_results=info)

    def process_post(self, identifier):
        item = self.look_up_by_id(identifier)
        if isinstance (item, ProblemDetail):
            return item
        value = self.run_tests(item)
        if (value):
            return Response(_("Successfully ran new self tests"), 200)
        return FAILED_TO_RUN_SELF_TESTS
