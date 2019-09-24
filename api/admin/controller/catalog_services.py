from nose.tools import set_trace
import flask
from flask import Response
from flask_babel import lazy_gettext as _

from . import SettingsController
from core.marc import MARCExporter
from core.model import (
    ExternalIntegration,
    Session,
    get_one,
)
from api.admin.problem_details import *
from core.util.problem_detail import ProblemDetail
from core.s3 import S3Uploader

class CatalogServicesController(SettingsController):

    def __init__(self, manager):
        super(CatalogServicesController, self).__init__(manager)
        service_apis = [MARCExporter]
        self.protocols = self._get_integration_protocols(service_apis, protocol_name_attr="NAME")
        mirror_integration_setting = self._mirror_integration_setting()
        if mirror_integration_setting:
            mirror_integration_setting["description"] = MARCExporter.SETTING_DESCRIPTION
            self.protocols[0]['settings'].append(mirror_integration_setting)

    def process_catalog_services(self):
        self.require_system_admin()

        if flask.request.method == "GET":
            return self.process_get()
        else:
            return self.process_post()

    def process_get(self):
        services = self._get_integration_info(ExternalIntegration.CATALOG_GOAL, self.protocols)
        return dict(
            catalog_services=services,
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
            service = get_one(self._db, ExternalIntegration, id=id, goal=ExternalIntegration.CATALOG_GOAL)
            if not service:
                return MISSING_SERVICE
            if protocol != service.protocol:
                return CANNOT_CHANGE_PROTOCOL
        else:
            # Create a new service
            service, is_new = self._create_integration(
                self.protocols, protocol, ExternalIntegration.CATALOG_GOAL,
            )
            if isinstance(service, ProblemDetail):
                return service

        name = self.get_name(service)
        if isinstance(name, ProblemDetail):
            self._db.rollback()
            return name
        elif name:
            service.name = name

        [protocol] = [p for p in self.protocols if p.get("name") == protocol]
        result = self._set_integration_settings_and_libraries(service, protocol)
        if isinstance(result, ProblemDetail):
            return result

        library_error = self.check_libraries(service)
        if library_error:
            self._db.rollback()
            return library_error

        storage_protocol_error = self.check_storage_protocol(service)
        if storage_protocol_error:
            self._db.rollback()
            return storage_protocol_error

        if is_new:
            return Response(unicode(service.id), 201)
        else:
            return Response(unicode(service.id), 200)

    def validate_form_fields(self, protocol):
        """Verify that the protocol which the user has selected is in the list
        of recognized protocol options."""

        if protocol and protocol not in [p.get("name") for p in self.protocols]:
            return UNKNOWN_PROTOCOL

    def get_name(self, service):
        """Check that there isn't already a service with this name"""

        name = flask.request.form.get("name")
        if name:
            if service.name != name:
                service_with_name = get_one(self._db, ExternalIntegration, name=name)
                if service_with_name:
                    return INTEGRATION_NAME_ALREADY_IN_USE
            return name

    def check_libraries(self, service):
        """Check that no library ended up with multiple MARC export integrations."""

        for library in service.libraries:
            marc_export_count = 0
            for integration in library.integrations:
                if integration.goal == ExternalIntegration.CATALOG_GOAL and integration.protocol == ExternalIntegration.MARC_EXPORT:
                    marc_export_count += 1
                    if marc_export_count > 1:
                        return MULTIPLE_SERVICES_FOR_LIBRARY.detailed(_(
                            "You tried to add a MARC export service to %(library)s, but it already has one.",
                            library=library.short_name,
                        ))

    def check_storage_protocol(self, service):
        """For MARC Export integrations, check that the storage protocol corresponds to an
        existing storage integration."""
        if service.protocol == MARCExporter.NAME:
            storage_protocol = service.setting(MARCExporter.STORAGE_PROTOCOL).value
            _db = Session.object_session(service)
            integration = ExternalIntegration.lookup(
                _db, storage_protocol, ExternalIntegration.STORAGE_GOAL)
            if not integration:
                return MISSING_SERVICE.detailed(_(
                    "You set the storage protocol to %(protocol)s, but no storage service with that protocol is configured.",
                    protocol=storage_protocol,
                ))
            if storage_protocol == ExternalIntegration.S3:
                # For S3, the storage service must also have a MARC file bucket.
                bucket = integration.setting(S3Uploader.MARC_BUCKET_KEY).value
                if not bucket:
                    return MISSING_SERVICE.detailed(_(
                        "You set the storage protocol to %(protocol)s, but the storage service with that protocol does not have a MARC file bucket configured.",
                        protocol=storage_protocol,
                    ))

    def process_delete(self, service_id):
        return self._delete_integration(
            service_id, ExternalIntegration.CATALOG_GOAL
        )
