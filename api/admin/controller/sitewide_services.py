import flask
from flask import Response
from flask_babel import lazy_gettext as _
from api.admin.problem_details import *
from core.external_search import ExternalSearchIndex
from core.model import (
    ExternalIntegration,
    get_one,
    get_one_or_create,
)
from core.log import (
    Loggly,
    SysLogger,
    CloudwatchLogs,
)
from core.util.problem_detail import ProblemDetail
from . import SettingsController

class SitewideServicesController(SettingsController):

    def _manage_sitewide_service(
            self, goal, provider_apis, service_key_name,
            multiple_sitewide_services_detail, protocol_name_attr='NAME'
    ):
        protocols = self._get_integration_protocols(provider_apis, protocol_name_attr=protocol_name_attr)

        self.require_system_admin()
        if flask.request.method == 'GET':
            return self.process_get(protocols, goal, service_key_name)
        else:
            return self.process_post(protocols, goal, multiple_sitewide_services_detail)

    def process_get(self, protocols, goal, service_key_name):
        services = self._get_integration_info(goal, protocols)
        return {
            service_key_name : services,
            'protocols' : protocols,
        }

    def process_post(self, protocols, goal, multiple_sitewide_services_detail):
        name = flask.request.form.get("name")
        protocol = flask.request.form.get("protocol")
        fields = {"name": name, "protocol": protocol}
        form_field_error = self.validate_form_fields(protocols, **fields)
        if form_field_error:
            return form_field_error

        settings = protocols[0].get("settings")
        wrong_format = self.validate_formats(settings)
        if wrong_format:
            return wrong_format

        is_new = False
        id = flask.request.form.get("id")

        if id:
            # Find an existing service in order to edit it
            service = self.look_up_service_by_id(id, protocol, goal)
        else:
            if protocol:
                service, is_new = get_one_or_create(
                    self._db, ExternalIntegration, protocol=protocol,
                    goal=goal
                )
                # There can only be one of each sitewide service.
                if not is_new:
                    self._db.rollback()
                    return MULTIPLE_SITEWIDE_SERVICES.detailed(
                        multiple_sitewide_services_detail
                    )
            else:
                return NO_PROTOCOL_FOR_NEW_SERVICE

        if isinstance(service, ProblemDetail):
            self._db.rollback()
            return service

        name_error = self.check_name_unique(service, name)
        if name_error:
            self._db.rollback()
            return name_error

        protocol_error = self.set_protocols(service, protocol, protocols)
        if protocol_error:
            self._db.rollback()
            return protocol_error

        service.name = name

        if is_new:
            return Response(unicode(service.id), 201)
        else:
            return Response(unicode(service.id), 200)

    def validate_form_fields(self, protocols, **fields):
        """The 'name' and 'protocol' fields cannot be blank, and the protocol must
        be selected from the list of recognized protocols."""

        name = fields.get("name")
        protocol = fields.get("protocol")

        if not name:
            return INCOMPLETE_CONFIGURATION
        if protocol and protocol not in [p.get("name") for p in protocols]:
            return UNKNOWN_PROTOCOL

class LoggingServicesController(SitewideServicesController):
    def process_services(self):
        detail = _("You tried to create a new logging service, but a logging service is already configured.")
        return self._manage_sitewide_service(
            ExternalIntegration.LOGGING_GOAL,
            [Loggly, SysLogger, CloudwatchLogs],
            'logging_services', detail
        )

    def process_delete(self, service_id):
        return self._delete_integration(
            service_id, ExternalIntegration.LOGGING_GOAL
        )

class SearchServicesController(SitewideServicesController):
    def __init__(self, manager):
        super(SearchServicesController, self).__init__(manager)
        self.type = _("search service")

    def process_services(self):
        detail = _("You tried to create a new search service, but a search service is already configured.")
        return self._manage_sitewide_service(
            ExternalIntegration.SEARCH_GOAL, [ExternalSearchIndex],
            'search_services', detail
        )

    def process_delete(self, service_id):
        return self._delete_integration(
            service_id, ExternalIntegration.SEARCH_GOAL
        )
