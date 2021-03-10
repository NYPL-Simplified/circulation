from nose.tools import set_trace
import flask
from flask import Response
from flask_babel import lazy_gettext as _
from api.admin.problem_details import *
from api.odl import SharedODLAPI
from api.registry import (
    Registration,
    RemoteRegistry,
)
from core.model import (
    Collection,
    ConfigurationSetting,
    get_one,
    Library,
)
from core.util.http import HTTP
from core.util.problem_detail import ProblemDetail
from . import SettingsController

class CollectionLibraryRegistrationsController(SettingsController):
    """Use the OPDS Directory Registration Protocol to register a
    Collection with its remote source of truth.

    :param registration_class: Mock class to use instead of Registration."""

    # TODO: This controller can share some code with DiscoveryServiceLibraryRegistrationsController.

    def __init__(self, manager):
        super(CollectionLibraryRegistrationsController, self).__init__(manager)
        self.shared_collection_provider_apis = [SharedODLAPI]

    def process_collection_library_registrations(self,
            do_get=HTTP.debuggable_get,
            do_post=HTTP.debuggable_post,
            key=None,
            registration_class=Registration):

        registration_class = registration_class or Registration
        self.require_system_admin()
        if flask.request.method == 'GET':
            return self.process_get()
        else:
            return self.process_post(registration_class, do_get, do_post)

    def get_library_info(self, library, collection):
        """Find the relevant information about the library which the user
        is trying to register"""

        library_info = dict(short_name=library.short_name)
        status = ConfigurationSetting.for_library_and_externalintegration(
            self._db, Registration.LIBRARY_REGISTRATION_STATUS, library, collection.external_integration,
        ).value
        if status:
            library_info["status"] = status
            return library_info

    def process_get(self):
        collections = []
        for collection in self._db.query(Collection):
            libraries = []
            for library in collection.libraries:
                library_info = self.get_library_info(library, collection)
                if library_info:
                    libraries.append(library_info)

            collections.append(
                dict(
                    id=collection.id,
                    libraries=libraries,
                )
            )
        return dict(library_registrations=collections)

    def process_post(self, registration_class, do_get, do_post):
        collection_id = flask.request.form.get("collection_id")
        library_short_name = flask.request.form.get("library_short_name")

        collection = self.look_up_collection(collection_id)
        if isinstance(collection, ProblemDetail):
            return collection

        library = self.look_up_library(library_short_name)
        if isinstance(library, ProblemDetail):
            return library

        registry = self.look_up_registry(collection.external_integration)
        if isinstance(registry, ProblemDetail):
            return registry

        registration = registration_class(registry, library)
        registered = registration.push(
            Registration.PRODUCTION_STAGE, self.url_for,
            catalog_url=collection.external_account_id,
            do_get=do_get, do_post=do_post
        )

        if isinstance(registered, ProblemDetail):
            return registered

        return Response(str(_("Success")), 200)

    def look_up_collection(self, collection_id):
        """Find the collection that the user is trying to register the library with,
        and check that it actually exists."""

        collection = get_one(self._db, Collection, id=collection_id)
        if not collection:
            return MISSING_COLLECTION
        if collection.protocol not in [api.NAME for api in self.shared_collection_provider_apis]:
            return COLLECTION_DOES_NOT_SUPPORT_REGISTRATION
        return collection

    def look_up_library(self, library_short_name):
        """Find the library the user is trying to register, and check that it actually exists."""

        library = get_one(self._db, Library, short_name=library_short_name)
        if not library:
            return NO_SUCH_LIBRARY
        return library

    def look_up_registry(self, external_integration):
        """Find the remote registry that the user is trying to register the collection with, and
        check that it is in the list of recognized protocols (currently just SharedODLAPI)"""

        registry = RemoteRegistry(external_integration)
        if not registry:
            return MISSING_SERVICE
        return registry
