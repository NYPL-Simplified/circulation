from nose.tools import set_trace
import flask
from flask import Response
from api.admin.problem_details import *
from api.google_analytics_provider import GoogleAnalyticsProvider
from core.local_analytics_provider import LocalAnalyticsProvider
from core.model import (
    ExternalIntegration,
    get_one,
)
from core.util.problem_detail import ProblemDetail
from . import SettingsController

class AnalyticsServicesController(SettingsController):

    def __init__(self, manager):
        super(AnalyticsServicesController, self).__init__(manager)
        provider_apis = [GoogleAnalyticsProvider,
                         LocalAnalyticsProvider,
                        ]
        self.protocols = self._get_integration_protocols(provider_apis)
        self.goal = ExternalIntegration.ANALYTICS_GOAL

    def process_analytics_services(self):
        if flask.request.method == 'GET':
            return self.process_get()
        else:
            return self.process_post()

    def process_get(self):
        if flask.request.method == 'GET':
            services = self._get_integration_info(self.goal, self.protocols)
            # Librarians should be able to see, but not modify local analytics services.
            # Setting the level to 2 will communicate that to the front end.
            for x in services:
                if (x["protocol"] == 'core.local_analytics_provider'):
                    x["level"] = 2
            return dict(
                analytics_services=services,
                protocols=self.protocols,
            )

    def process_post(self):
        name = flask.request.form.get("name")
        protocol = flask.request.form.get("protocol")
        url = flask.request.form.get("url")
        fields = {"name": name, "protocol": protocol, "url": url}

        # Don't let librarians create local analytics services.
        if protocol == 'core.local_analytics_provider':
            self.require_higher_than_librarian()

        form_field_error = self.validate_form_fields(**fields)
        if form_field_error:
            return form_field_error

        is_new = False
        id = flask.request.form.get("id")

        if id:
            # Find an existing service in order to edit it
            service = self.look_up_service_by_id(id, protocol)
        else:
            service, is_new = self._create_integration(
                self.protocols, protocol, self.goal
            )

        if isinstance(service, ProblemDetail):
            self._db.rollback()
            return service

        name_error = self.check_name_unique(service, name)
        if name_error:
            self._db.rollback()
            return name_error

        protocol_error = self.set_protocols(service, protocol)
        if protocol_error:
            self._db.rollback()
            return protocol_error

        service.name = name
        if is_new:
            return Response(str(service.id), 201)
        else:
            return Response(str(service.id), 200)

    def validate_form_fields(self, **fields):
        """The 'name' and 'URL' fields cannot be blank, the URL must be valid,
        and the protocol must be selected from the list of recognized protocols."""

        name = fields.get("name")
        protocol = fields.get("protocol")
        url = fields.get("url")

        if not name:
            return MISSING_ANALYTICS_NAME
        if protocol:
            error = self.validate_protocol()
            if error:
                return error
            else:
                wrong_format = self.validate_formats()
                if wrong_format:
                    return wrong_format

        # The URL is only relevant, and required, if the user is creating a Google Analytics
        # integration; the local analytics form doesn't have a URL field.
        if "url" in list(flask.request.form.keys()) and not url:
            return INCOMPLETE_CONFIGURATION

    def process_delete(self, service_id):
        return self._delete_integration(
            service_id, self.goal
        )
