from nose.tools import set_trace
import flask
from flask import Response
from flask_babel import lazy_gettext as _
from api.admin.problem_details import *
from api.registry import RemoteRegistry
from core.model import (
    ExternalIntegration,
    get_one,
    get_one_or_create,
)
from core.util.problem_detail import ProblemDetail
from . import SettingsController

class DiscoveryServicesController(SettingsController):

    def __init__(self, manager):
        super(DiscoveryServicesController, self).__init__(manager)
        self.opds_registration = ExternalIntegration.OPDS_REGISTRATION
        self.protocols = [
            {
                "name": self.opds_registration,
                "sitewide": True,
                "settings": [
                    { "key": ExternalIntegration.URL, "label": _("URL"), "required": True },
                ],
                "supports_registration": True,
                "supports_staging": True,
            }
        ]
        self.goal = ExternalIntegration.DISCOVERY_GOAL

    def process_discovery_services(self):
        self.require_system_admin()
        if flask.request.method == 'GET':
            return self.process_get()
        else:
            return self.process_post()

    def process_get(self):
        registries = list(
            RemoteRegistry.for_protocol_and_goal(
                self._db, self.opds_registration, self.goal
            )
        )
        if not registries:
            # There are no registries at all. Set up the default
            # library registry.
            self.set_up_default_registry()

        services = self._get_integration_info(self.goal, self.protocols)
        return dict(
            discovery_services=services,
            protocols=self.protocols,
        )

    def set_up_default_registry(self):
        """Set up the default library registry; no other registries exist yet."""

        service, is_new = get_one_or_create(
            self._db, ExternalIntegration, protocol=self.opds_registration,
            goal=self.goal
        )
        if is_new:
            service.url = (
                RemoteRegistry.DEFAULT_LIBRARY_REGISTRY_URL
            )

    def process_post(self):
        name = flask.request.form.get("name")
        protocol = flask.request.form.get("protocol")
        fields = {"name": name, "protocol": protocol}
        form_field_error = self.validate_form_fields(**fields)
        if form_field_error:
            return form_field_error

        id = flask.request.form.get("id")
        is_new = False
        if id:
            # Find an existing service in order to edit it
            service = self.look_up_service_by_id(protocol, id)
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
            return Response(unicode(service.id), 201)
        else:
            return Response(unicode(service.id), 200)

    def validate_form_fields(self, **fields):
        """The 'name' and 'protocol' fields cannot be blank, and the protocol must
        be selected from the list of recognized protocols."""

        name = fields.get("name")
        protocol = fields.get("protocol")
        if not name:
            return INCOMPLETE_CONFIGURATION
        if not protocol:
            return NO_PROTOCOL_FOR_NEW_SERVICE
        if protocol not in [p.get("name") for p in self.protocols]:
            return UNKNOWN_PROTOCOL

    def look_up_service_by_id(self, protocol, id):
        """Find an existing service, and make sure that the user is not trying to edit
        its protocol."""

        registry = RemoteRegistry.for_integration_id(self._db, id, self.goal)
        if not registry:
            return MISSING_SERVICE
        service = registry.integration
        if protocol != service.protocol:
            return CANNOT_CHANGE_PROTOCOL
        return service

    def check_name_unique(self, new_service, name):
        """A service cannot be created with, or edited to have, the same name
        as a service that already exists."""

        existing_service = get_one(self._db, ExternalIntegration, name=name)
        if existing_service and not existing_service.id == new_service.id:
            # Without checking that the IDs are different, you can't save
            # changes to an existing service unless you've also changed its name.
            return INTEGRATION_NAME_ALREADY_IN_USE

    def set_protocols(self, service, protocol):
        """Validate the protocol that the user has submitted; depending on whether
        the validations pass, either save it to this discovery service or
        return an error message."""

        [protocol] = [p for p in self.protocols if p.get("name") == protocol]
        result = self._set_integration_settings_and_libraries(service, protocol)
        if isinstance(result, ProblemDetail):
            return result

    def process_delete(self, service_id):
        return self._delete_integration(
            service_id, ExternalIntegration.DISCOVERY_GOAL
        )
