from nose.tools import set_trace
import flask
from flask import Response
from api.admin.problem_details import *
from api.nyt import NYTBestSellerAPI
from api.novelist import NoveListAPI
from core.opds_import import MetadataWranglerOPDSLookup
from core.model import (
    ExternalIntegration,
    get_one,
)
from core.util.http import HTTP
from core.util.problem_detail import ProblemDetail

from sitewide_registration import SitewideRegistrationController

class MetadataServicesController(SitewideRegistrationController):

    def __init__(self, manager):
        super(MetadataServicesController, self).__init__(manager)
        provider_apis = [
                            NYTBestSellerAPI,
                            NoveListAPI,
                            MetadataWranglerOPDSLookup,
                        ]

        self.protocols = self._get_integration_protocols(provider_apis, protocol_name_attr="PROTOCOL")
        self.goal = ExternalIntegration.METADATA_GOAL

    def process_metadata_services(self):
        self.require_system_admin()
        if flask.request.method == 'GET':
            return self.process_get()
        else:
            return self.process_post()

    def process_get(self):
        metadata_services = self._get_integration_info(self.goal, self.protocols)
        return dict(
            metadata_services=metadata_services,
            protocols=self.protocols,
        )

    def process_post(self, do_get=HTTP.debuggable_get, do_post=HTTP.debuggable_post):
        name = flask.request.form.get("name")
        protocol = flask.request.form.get("protocol")
        url = flask.request.form.get("url")
        fields = {"name": name, "protocol": protocol, "url": url}
        form_field_error = self.validate_form_fields(**fields)
        if form_field_error:
            return form_field_error

        id = flask.request.form.get("id")
        is_new = False
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

        wrangler_error = self.register_with_metadata_wrangler(
            do_get, do_post, is_new, service
        )
        if wrangler_error:
            self._db.rollback()
            return wrangler_error

        service.name = name

        if is_new:
            return Response(unicode(service.id), 201)
        else:
            return Response(unicode(service.id), 200)

    def validate_form_fields(self, **fields):
        """The 'name' and 'protocol' fields cannot be blank, and the protocol must
        be selected from the list of recognized protocols.  The URL must be valid."""

        name = fields.get("name")
        protocol = fields.get("protocol")
        url = fields.get("url")

        if not name:
            return INCOMPLETE_CONFIGURATION
        if not protocol:
            return NO_PROTOCOL_FOR_NEW_SERVICE

        error = self.validate_protocol()
        if error:
            return error

        wrong_format = self.validate_formats()
        if wrong_format:
            return wrong_format

    def register_with_metadata_wrangler(self, do_get, do_post, is_new, service):
        """Register this site with the Metadata Wrangler."""

        if ((is_new or not service.password) and
            service.protocol == ExternalIntegration.METADATA_WRANGLER):

            problem_detail = self.process_sitewide_registration(
                service, do_get=do_get, do_post=do_post
            )
            if problem_detail:
                return problem_detail

    def process_delete(self, service_id):
        return self._delete_integration(
            service_id, self.goal
        )
