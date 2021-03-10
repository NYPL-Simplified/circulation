from nose.tools import set_trace
import flask
from flask import Response
from flask_babel import lazy_gettext as _
import json
from api.admin.problem_details import *
from api.registry import (
    RemoteRegistry,
    Registration,
)
from core.model import (
    ExternalIntegration,
    get_one,
    Library,
)
from core.util.http import HTTP
from core.util.problem_detail import ProblemDetail
from . import SettingsController

class DiscoveryServiceLibraryRegistrationsController(SettingsController):

    """List the libraries that have been registered with a specific
    RemoteRegistry, and allow the admin to register a library with
    a RemoteRegistry.

    :param registration_class: Mock class to use instead of Registration.
    """

    def __init__(self, manager):
        super(DiscoveryServiceLibraryRegistrationsController, self).__init__(manager)
        self.goal = ExternalIntegration.DISCOVERY_GOAL

    def process_discovery_service_library_registrations(self,
            registration_class=None,
            do_get=HTTP.debuggable_get,
            do_post=HTTP.debuggable_post
    ):
        registration_class = registration_class or Registration
        self.require_system_admin()
        if flask.request.method == 'GET':
            return self.process_get(do_get)
        else:
            return self.process_post(registration_class, do_get, do_post)

    def process_get(self, do_get=HTTP.debuggable_get):
        """Make a list of all discovery services, each with the
        list of libraries registered with that service and the
        status of the registration."""

        services = []
        for registry in RemoteRegistry.for_protocol_and_goal(
                self._db, ExternalIntegration.OPDS_REGISTRATION, self.goal
        ):
            result = (
                registry.fetch_registration_document(do_get=do_get)
            )
            if isinstance(result, ProblemDetail):
                # Unlike most cases like this, a ProblemDetail doesn't
                # mean the whole request is ruined -- just that one of
                # the discovery services isn't working. Turn the
                # ProblemDetail into a JSON object and return it for
                # handling on the client side.
                access_problem = json.loads(result.response[0])
                terms_of_service_link = terms_of_service_html = None
            else:
                access_problem = None
                terms_of_service_link, terms_of_service_html = result
            libraries = []
            for registration in registry.registrations:
                library_info = self.get_library_info(registration)
                if library_info:
                    libraries.append(library_info)

            services.append(
                dict(
                    id=registry.integration.id,
                    access_problem=access_problem,
                    terms_of_service_link=terms_of_service_link,
                    terms_of_service_html=terms_of_service_html,
                    libraries=libraries,
                )
            )

        return dict(library_registrations=services)

    def get_library_info(self, registration):
        """Find the relevant information about the library which the user
        is trying to register"""

        library = registration.library
        library_info = dict(short_name=library.short_name)
        status = registration.status_field.value
        stage_field = registration.stage_field.value
        if stage_field:
            library_info["stage"] = stage_field
        if status:
            library_info["status"] = status
            return library_info

    def look_up_registry(self, integration_id):
        """Find the RemoteRegistry that the user is trying to register the library with,
         and check that it actually exists."""

        registry = RemoteRegistry.for_integration_id(
            self._db, integration_id, self.goal
        )
        if not registry:
            return MISSING_SERVICE
        return registry

    def look_up_library(self, library_short_name):
        """Find the library the user is trying to register, and check that it actually exists."""

        library = get_one(self._db, Library, short_name=library_short_name)
        if not library:
            return NO_SUCH_LIBRARY
        return library

    def process_post(self, registration_class, do_get, do_post):
        """Attempt to register a library with a RemoteRegistry."""

        integration_id = flask.request.form.get("integration_id")
        library_short_name = flask.request.form.get("library_short_name")
        stage = flask.request.form.get("registration_stage") or Registration.TESTING_STAGE

        registry = self.look_up_registry(integration_id)
        if isinstance(registry, ProblemDetail):
            return registry

        library = self.look_up_library(library_short_name)
        if isinstance(library, ProblemDetail):
            return library

        registration = registration_class(registry, library)
        registered = registration.push(
            stage, self.url_for, do_get=do_get, do_post=do_post
        )
        if isinstance(registered, ProblemDetail):
            return registered

        return Response(str(_("Success")), 200)
