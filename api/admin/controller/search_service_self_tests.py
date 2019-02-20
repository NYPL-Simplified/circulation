from nose.tools import set_trace
import flask
from flask import Response
from flask_babel import lazy_gettext as _
from api.admin.problem_details import *
from core.opds_import import (OPDSImporter, OPDSImportMonitor)
from core.model import (
    ExternalIntegration
)
from core.external_search import ExternalSearchIndex
from core.tests.test_external_search import ExternalSearchTest

from core.selftest import HasSelfTests
from core.util.problem_detail import ProblemDetail
from . import SettingsController

class SearchServiceSelfTestsController(SettingsController, ExternalSearchTest):

    def process_search_service_self_tests(self, identifier):
        if not identifier:
            return MISSING_SEARCH_SERVICE_IDENTIFIER
        if flask.request.method == "GET":
            return self.process_get(identifier)
        else:
            return self.process_post(identifier)

    def process_get(self, identifier):
        search_service = self.look_up_service_by_id(
            identifier,
            ExternalIntegration.ELASTICSEARCH,
            ExternalIntegration.SEARCH_GOAL
        )
        if isinstance(search_service, ProblemDetail):
            return search_service

        info = dict(
            id=search_service.id,
            name=search_service.name,
            protocol=search_service.protocol,
            settings=search_service.settings,
            goal=search_service.goal
        )
        search_index = ExternalSearchIndex(self._db)
        info["self_test_results"] = self._get_prior_search_test_results(search_service, search_index)
        return dict(search_service=info)

    def process_post(self, identifier, search_index_class=ExternalSearchIndex):
        search_service = self.look_up_service_by_id(
            identifier,
            flask.request.form.get("protocol"),
            ExternalIntegration.SEARCH_GOAL
        )
        if isinstance(search_service, ProblemDetail):
            return search_service
        search_term = search_service.setting(
            "test_search_term").value_or_default("test")
        [results_dict, results_list] = search_index_class.run_self_tests(
            self._db, self._db
        )
        return Response(results_dict, 200)
