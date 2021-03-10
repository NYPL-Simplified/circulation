from nose.tools import set_trace
import flask
from flask import Response
from flask_babel import lazy_gettext as _
from api.admin.problem_details import *
from core.model import (
    Configuration,
    ExternalIntegration,
    get_one,
)
from core.util.problem_detail import ProblemDetail

from . import SettingsController

class CDNServicesController(SettingsController):

    def __init__(self, manager):
        super(CDNServicesController, self).__init__(manager)
        self.protocols = [
            {
                "name": ExternalIntegration.CDN,
                "sitewide": True,
                "settings": [
                    { "key": ExternalIntegration.URL, "label": _("CDN URL"), "required": True, "format": "url" },
                    { "key": Configuration.CDN_MIRRORED_DOMAIN_KEY, "label": _("Mirrored domain"), "required": True },
                ],
            }
        ]
        self.goal = ExternalIntegration.CDN_GOAL

    def process_cdn_services(self):
        self.require_system_admin()
        if flask.request.method == 'GET':
            return self.process_get()
        else:
            return self.process_post()


    def process_get(self):
        services = self._get_integration_info(self.goal, self.protocols)
        return dict(
            cdn_services=services,
            protocols=self.protocols,
        )

    def process_post(self):
        name = flask.request.form.get("name")
        protocol = flask.request.form.get("protocol")
        fields = {"name": name, "protocol": protocol}
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
        """The 'name' and 'protocol' fields cannot be blank, and the protocol must
        be selected from the list of recognized protocols.  The URL must be valid."""

        name = fields.get("name")
        protocol = fields.get("protocol")

        if not name:
            return INCOMPLETE_CONFIGURATION
        if protocol:
            error = self.validate_protocol()
            if error:
                return error
            else:
                wrong_format = self.validate_formats()
                if wrong_format:
                    return wrong_format

    def process_delete(self, service_id):
        return self._delete_integration(
            service_id, self.goal
        )
