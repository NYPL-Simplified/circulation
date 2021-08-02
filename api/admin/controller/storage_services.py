import flask
from flask import Response

from api.admin.problem_details import *
from core.mirror import MirrorUploader
from core.model import (
    ExternalIntegration,
    get_one
)
from core.util.problem_detail import ProblemDetail
from . import SettingsController

# NOTE: We need to import it explicitly to initialize MirrorUploader.IMPLEMENTATION_REGISTRY
from api.lcp import mirror


class StorageServicesController(SettingsController):

    def __init__(self, manager):
        super(StorageServicesController, self).__init__(manager)
        self.goal = ExternalIntegration.STORAGE_GOAL
        self.protocols = self._get_integration_protocols(
            list(MirrorUploader.IMPLEMENTATION_REGISTRY.values()),
            protocol_name_attr="NAME"
        )

    def process_services(self):
        if flask.request.method == 'GET':
            return self.process_get()
        else:
            return self.process_post()

    def process_get(self):
        services = self._get_integration_info(self.goal, self.protocols)
        return dict(
            storage_services=services,
            protocols=self.protocols
        )
    
    def process_post(self):
        protocol = flask.request.form.get("protocol")
        name = flask.request.form.get("name")
        is_new = False
        protocol_error = self.validate_protocol()
        if protocol_error:
            return protocol_error

        id = flask.request.form.get("id")
        if id:
            # Find an existing service to edit
            storage_service = get_one(self._db, ExternalIntegration, id=id, goal=self.goal)
            if not storage_service:
                return MISSING_SERVICE
            if protocol != storage_service.protocol:
                return CANNOT_CHANGE_PROTOCOL
        else:
            # Create a new service
            storage_service, is_new = self._create_integration(
                self.protocols, protocol, self.goal
            )
            if isinstance(storage_service, ProblemDetail):
                self._db.rollback()
                return storage_service

        protocol_error = self.set_protocols(storage_service, protocol, self.protocols)

        if protocol_error:
            self._db.rollback()
            return protocol_error
        storage_service.name = name

        if is_new:
            return Response(str(storage_service.id), 201)
        else:
            return Response(str(storage_service.id), 200)

    def process_delete(self, service_id):
        return self._delete_integration(
            service_id, ExternalIntegration.STORAGE_GOAL
        )
