import flask
from flask import Response
from flask_babel import lazy_gettext as _
from core.model import (
    ExternalIntegration,
    get_one,
    get_one_or_create,
)
from core.util.problem_detail import ProblemDetail
from . import SettingsController
from api.admin.google_oauth_admin_authentication_provider import GoogleOAuthAdminAuthenticationProvider
from api.admin.problem_details import *


class AdminAuthServicesController(SettingsController):

    def __init__(self, manager):
        super(AdminAuthServicesController, self).__init__(manager)
        provider_apis = [GoogleOAuthAdminAuthenticationProvider]
        self.protocols = self._get_integration_protocols(
            provider_apis, protocol_name_attr="NAME")

    def process_admin_auth_services(self):
        """Fetch, create, or update admin_auth_services

        Returns:
            dict: if Get request returns a dict of auth services and protocols
            Response: If POST request updates or creates auth services and protocols.
        """
        self.require_system_admin()
        if flask.request.method == 'GET':
            return self.process_get()
        else:
            return self.process_post()

    def process_get(self):
        """Return dict of auth services and protocols available to library

        Returns:
            dict: auth_services and protocols available
        """
        auth_services = self._get_integration_info(
            ExternalIntegration.ADMIN_AUTH_GOAL, self.protocols)
        return dict(
            admin_auth_services=auth_services,
            protocols=self.protocols,
        )

    def process_post(self):
        """Create new auth_service if none exists and set service and protocol

        form: 'protocol'
        form: 'id'
        form: 'name'

        Returns:
            Response: ProblemDetail or string of auth_service.protocol and 200, or 201 if newly created service
        """
        protocol = flask.request.form.get("protocol")
        id = flask.request.form.get("id")
        auth_service = ExternalIntegration.admin_authentication(self._db)
        fields = {"protocol": protocol, "id": id, "auth_service": auth_service}
        error = self.validate_form_fields(**fields)
        if error:
            return error

        is_new = False

        if not auth_service:
            if protocol:
                auth_service, is_new = get_one_or_create(
                    self._db, ExternalIntegration, protocol=protocol,
                    goal=ExternalIntegration.ADMIN_AUTH_GOAL
                )
            else:
                return NO_PROTOCOL_FOR_NEW_SERVICE

        name = flask.request.form.get("name")
        auth_service.name = name

        [protocol] = [p for p in self.protocols if p.get("name") == protocol]
        result = self._set_integration_settings_and_libraries(
            auth_service, protocol)
        if isinstance(result, ProblemDetail):
            return result

        if is_new:
            return Response(str(auth_service.protocol), 201)
        else:
            return Response(str(auth_service.protocol), 200)

    def validate_form_fields(self, **fields):
        """Check that 1) the user has selected a valid protocol, 2) the user has not
        left the required fields blank, and 3) the user is not attempting to
        change the protocol of an existing admin auth service."""

        protocol = fields.get("protocol")
        auth_service = fields.get("auth_service")
        id = fields.get("id")

        if protocol:
            if protocol not in ExternalIntegration.ADMIN_AUTH_PROTOCOLS:
                return UNKNOWN_PROTOCOL
            else:
                wrong_format = self.validate_formats()
                if wrong_format:
                    return wrong_format
        if auth_service:
            if id and int(id) != auth_service.id:
                return MISSING_SERVICE
            if protocol != auth_service.protocol:
                return CANNOT_CHANGE_PROTOCOL
        else:
            if id:
                return MISSING_SERVICE

    def process_delete(self, protocol):
        """Delete an auth service from the database

        Args:
            protocol (string): Name of protocol to search for the service to be deleted.

        Returns:
            Response: ('Deleted', 200)
        """
        self.require_system_admin()
        service = get_one(self._db, ExternalIntegration,
                          protocol=protocol, goal=ExternalIntegration.ADMIN_AUTH_GOAL)
        if not service:
            return MISSING_SERVICE
        self._db.delete(service)
        return Response(str(_("Deleted")), 200)
