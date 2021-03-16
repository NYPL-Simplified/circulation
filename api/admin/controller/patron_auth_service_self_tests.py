import flask
from flask import Response
from flask_babel import lazy_gettext as _
from api.admin.problem_details import *
from api.simple_authentication import SimpleAuthenticationProvider
from api.millenium_patron import MilleniumPatronAPI
from api.sip import SIP2AuthenticationProvider
from api.firstbook import FirstBookAuthenticationAPI as OldFirstBookAuthenticationAPI
from api.firstbook2 import FirstBookAuthenticationAPI
from api.clever import CleverAuthenticationAPI
from core.model import (
    get_one,
    ExternalIntegration,
)
from core.selftest import HasSelfTests
from core.util.problem_detail import ProblemDetail
from api.admin.controller.self_tests import SelfTestsController
from api.admin.controller.patron_auth_services import PatronAuthServicesController

class PatronAuthServiceSelfTestsController(SelfTestsController, PatronAuthServicesController):

    def process_patron_auth_service_self_tests(self, identifier):
        return self._manage_self_tests(identifier)

    def look_up_by_id(self, identifier):
        service = get_one(
            self._db,
            ExternalIntegration,
            id=identifier,
            goal=ExternalIntegration.PATRON_AUTH_GOAL
        )
        if not service:
            return MISSING_SERVICE
        return service

    def get_info(self, patron_auth_service):
        [protocol] = [p for p in self._get_integration_protocols(self.provider_apis) if p.get("name") == patron_auth_service.protocol]
        info = dict(
            id=patron_auth_service.id,
            name=patron_auth_service.name,
            protocol=patron_auth_service.protocol,
            goal=patron_auth_service.goal,
            settings=protocol.get("settings")
        )
        return info

    def run_tests(self, patron_auth_service):
        # If the auth service doesn't have at least one library associated with it,
        # then admins will not be able to access the button to run self tests for it, so
        # this code will never be reached; hence, no need to check here that :library exists.
        value = None
        if len(patron_auth_service.libraries):
            library = patron_auth_service.libraries[0]
            value = self._find_protocol_class(patron_auth_service).run_self_tests(
                self._db, None, library, patron_auth_service
            )
        return value
