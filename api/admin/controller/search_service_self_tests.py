from nose.tools import set_trace
import flask
from flask import Response
from flask_babel import lazy_gettext as _
from api.admin.problem_details import *
from core.model import (
    ExternalIntegration
)
from core.external_search import ExternalSearchIndex
from core.tests.test_external_search import ExternalSearchTest

from core.selftest import HasSelfTests
from core.util.problem_detail import ProblemDetail
from api.admin.controller.self_tests import SelfTestsController

class SearchServiceSelfTestsController(SelfTestsController, ExternalSearchTest):

    def __init__(self, manager):
        super(SearchServiceSelfTestsController, self).__init__(manager)

    def process_search_service_self_tests(self, identifier):
        return self._manage_self_tests(identifier)

    def look_up_by_id(self, identifier):
        return self.look_up_service_by_id(
            identifier,
            ExternalIntegration.ELASTICSEARCH,
            ExternalIntegration.SEARCH_GOAL
        )

    def get_info(self, search_service):
        [protocol] = self._get_integration_protocols([ExternalSearchIndex])
        return dict(
            id=search_service.id,
            name=search_service.name,
            protocol=protocol,
            settings=protocol.get("settings"),
            goal=search_service.goal
        )

    def run_tests(self, search_service):
        return ExternalSearchIndex.run_self_tests(
            self._db, None, self._db, None
        )
