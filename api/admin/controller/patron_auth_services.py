from nose.tools import set_trace
import flask
from flask import Response
from flask_babel import lazy_gettext as _
import re
from . import SettingsController
from api.authenticator import AuthenticationProvider
from api.simple_authentication import SimpleAuthenticationProvider
from api.millenium_patron import MilleniumPatronAPI
from api.sip import SIP2AuthenticationProvider
from api.firstbook import FirstBookAuthenticationAPI as OldFirstBookAuthenticationAPI
from api.firstbook2 import FirstBookAuthenticationAPI
from api.clever import CleverAuthenticationAPI
from core.model import (
    ConfigurationSetting,
    ExternalIntegration,
    get_one,
)
from api.admin.problem_details import *
from core.util.problem_detail import ProblemDetail

class PatronAuthServicesController(SettingsController):

    def __init__(self, manager):
        super(PatronAuthServicesController, self).__init__(manager)
        provider_apis = [SimpleAuthenticationProvider,
                         MilleniumPatronAPI,
                         SIP2AuthenticationProvider,
                         FirstBookAuthenticationAPI,
                         OldFirstBookAuthenticationAPI,
                         CleverAuthenticationAPI,
                        ]
        self.protocols = self._get_integration_protocols(provider_apis)

        self.basic_auth_protocols = [SimpleAuthenticationProvider.__module__,
                                MilleniumPatronAPI.__module__,
                                SIP2AuthenticationProvider.__module__,
                                FirstBookAuthenticationAPI.__module__,
                                OldFirstBookAuthenticationAPI.__module__,
                               ]

    def process_patron_auth_services(self):
        self.require_system_admin()

        if flask.request.method == 'GET':
            return self.process_get()
        else:
            return self.process_post()

    def process_get(self):
        services = self._get_integration_info(ExternalIntegration.PATRON_AUTH_GOAL, self.protocols)
        return dict(
            patron_auth_services=services,
            protocols=self.protocols,
        )

    def process_post(self):
        protocol = flask.request.form.get("protocol")
        is_new = False
        error = self.validate_form_fields(protocol)
        if error:
            return error

        id = flask.request.form.get("id")
        if id:
            # Find an existing service to edit
            auth_service = get_one(self._db, ExternalIntegration, id=id, goal=ExternalIntegration.PATRON_AUTH_GOAL)
            if not auth_service:
                return MISSING_SERVICE
            if protocol != auth_service.protocol:
                return CANNOT_CHANGE_PROTOCOL
        else:
            # Create a new service
            auth_service, is_new = self._create_integration(
                self.protocols, protocol, ExternalIntegration.PATRON_AUTH_GOAL
            )
            if isinstance(auth_service, ProblemDetail):
                return auth_service

        name = self.get_name(auth_service)
        if isinstance(name, ProblemDetail):
            self._db.rollback()
            return name
        elif name:
            auth_service.name = name

        [protocol] = [p for p in self.protocols if p.get("name") == protocol]
        result = self._set_integration_settings_and_libraries(auth_service, protocol)
        if isinstance(result, ProblemDetail):
            return result

        library_error = self.check_libraries(auth_service)
        if library_error:
            self._db.rollback()
            return library_error

        if is_new:
            return Response(unicode(auth_service.id), 201)
        else:
            return Response(unicode(auth_service.id), 200)

    def validate_form_fields(self, protocol):
        """Verify that the protocol which the user has selected is in the list
        of recognized protocol options."""

        if protocol and protocol not in [p.get("name") for p in self.protocols]:
            return UNKNOWN_PROTOCOL

    def get_name(self, auth_service):
        """Check that there isn't already an auth service with this name"""

        name = flask.request.form.get("name")
        if name:
            if auth_service.name != name:
                service_with_name = get_one(self._db, ExternalIntegration, name=name)
                if service_with_name:
                    return INTEGRATION_NAME_ALREADY_IN_USE
            return name

    def check_library_integrations(self, library):
        """Check that the library didn't end up with multiple basic auth services."""

        basic_auth_count = 0
        for integration in library.integrations:
            if integration.goal == ExternalIntegration.PATRON_AUTH_GOAL and integration.protocol in self.basic_auth_protocols:
                basic_auth_count += 1
                if basic_auth_count > 1:
                    return MULTIPLE_BASIC_AUTH_SERVICES.detailed(_(
                        "You tried to add a patron authentication service that uses basic auth to %(library)s, but it already has one.",
                        library=library.short_name,
                    ))

    def check_external_type(self, library, auth_service):
        """Check that the library's external type regular expression is valid, if it was set."""

        value = ConfigurationSetting.for_library_and_externalintegration(
            self._db, AuthenticationProvider.EXTERNAL_TYPE_REGULAR_EXPRESSION,
            library, auth_service).value
        if value:
            try:
                re.compile(value)
            except Exception, e:
                return INVALID_EXTERNAL_TYPE_REGULAR_EXPRESSION

    def check_identifier_restriction(self, library, auth_service):
        """Check whether the library's identifier restriction regular expression is set and
        is supposed to be a regular expression; if so, check that it's valid."""

        identifier_restriction_type = ConfigurationSetting.for_library_and_externalintegration(
            self._db, AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE,
            library, auth_service).value
        identifier_restriction = ConfigurationSetting.for_library_and_externalintegration(
            self._db, AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION,
            library, auth_service).value
        if identifier_restriction and identifier_restriction_type == AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_REGEX:
            try:
                re.compile(identifier_restriction)
            except Exception, e:
                return INVALID_LIBRARY_IDENTIFIER_RESTRICTION_REGULAR_EXPRESSION

    def check_libraries(self, auth_service):
        """Run the three library validation methods on each of the libraries for which the user is trying
        to configure this patron auth service."""

        for library in auth_service.libraries:
            error = self.check_library_integrations(library) or self.check_external_type(library, auth_service) or self.check_identifier_restriction(library, auth_service)
            if error:
                return error

    def process_delete(self, service_id):
        return self._delete_integration(
            service_id, ExternalIntegration.PATRON_AUTH_GOAL
        )
