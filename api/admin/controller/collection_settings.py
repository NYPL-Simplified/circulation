from nose.tools import set_trace
from . import SettingsController
import flask
from flask import Response
from flask_babel import lazy_gettext as _
import json
from core.opds_import import (OPDSImporter, OPDSImportMonitor)
from api.admin.problem_details import *
from api.feedbooks import FeedbooksOPDSImporter
from api.opds_for_distributors import OPDSForDistributorsAPI
from api.overdrive import OverdriveAPI
from api.odilo import OdiloAPI
from api.bibliotheca import BibliothecaAPI
from api.axis import Axis360API
from api.rbdigital import RBDigitalAPI
from api.enki import EnkiAPI
from api.odl import ODLWithConsolidatedCopiesAPI, SharedODLAPI
from core.model import (
    Collection,
    ConfigurationSetting,
    ExternalIntegration,
    get_one,
    get_one_or_create,
    Library,
)
from core.util.problem_detail import ProblemDetail

class CollectionSettingsController(SettingsController):

    # SET UP
    def process_collections(self):
        self.set_up_protocols()
        if flask.request.method == 'GET':
            return self.process_get()
        else:
            return self.process_post()

    def set_up_protocols(self):
        self.protocols = self._get_collection_protocols(self.PROVIDER_APIS)

        # If there are storage integrations, add a mirror integration
        # setting to every protocol's 'settings' block.
        mirror_integration_setting = self._mirror_integration_setting()
        if mirror_integration_setting:
            for protocol in self.protocols:
                protocol['settings'].append(mirror_integration_setting)

    # GET
    def process_get(self):
        user = flask.request.admin
        collections = []
        protocolClass = None
        for collection_object in self._db.query(Collection).order_by(Collection.name).all():
            if not user or not user.can_see_collection(collection_object):
                continue

            collection_dict = self.collection_to_dict(collection_object)

            if collection_object.protocol in [p.get("name") for p in self.protocols]:
                [protocol] = [p for p in self.protocols if p.get("name") == collection_object.protocol]
                self.load_libraries(collection_dict, collection_object, user, protocol)
                self.load_settings(protocol.get("settings"), collection_dict, collection_object)
                [protocolClass] = self.find_protocol_class(collection_object)

            collection_dict["self_test_results"] = self._get_prior_test_results(collection_object, protocolClass)
            collections.append(collection_dict)

        return dict(
            collections=collections,
            protocols=self.protocols,
        )

    def collection_to_dict(self, collection_object):
        return dict(
            id=collection_object.id,
            name=collection_object.name,
            protocol=collection_object.protocol,
            parent_id=collection_object.parent_id,
            settings=dict(external_account_id=collection_object.external_account_id),
        )

    def load_libraries(self, collection_dict, collection_object, user, protocol):
        libraries = []
        for library in collection_object.libraries:
            if not user or not user.is_librarian(library):
                continue
            libraries.append(self._get_integration_library_info(
                    collection_object.external_integration, library, protocol))

        collection_dict['libraries'] = libraries

    def load_settings(self, protocol_settings, collection_dict, collection_object):
        for protocol_setting in protocol_settings:
            key = protocol_setting.get("key")
            if key not in collection_dict.get("settings"):
                if key == 'mirror_integration_id':
                    value = collection_object.mirror_integration_id or self.NO_MIRROR_INTEGRATION
                elif protocol_setting.get("type") == "list":
                    value = collection_object.external_integration.setting(key).json_value
                else:
                    value = collection_object.external_integration.setting(key).value
                collection_dict["settings"][key] = value

    def find_protocol_class(self, collection_object):
        protocolClassFound = [p for p in self.PROVIDER_APIS if p.NAME == collection_object.protocol]
        if len(protocolClassFound) == 1:
            return protocolClassFound


    # POST
    def process_post(self):
        self.require_system_admin()
        is_new = False
        collection = None

        name = flask.request.form.get("name")
        protocol = flask.request.form.get("protocol")
        fields = {"name": name, "protocol": protocol}
        id = flask.request.form.get("id")
        if id:
            collection = get_one(self._db, Collection, id=id)
            fields["collection"] = collection

        error = self.validate_form_fields(is_new, **fields)
        if error:
            return error

        if protocol and not collection:
            collection, is_new = get_one_or_create(self._db, Collection, name=name)
            if not is_new:
                self._db.rollback()
                return COLLECTION_NAME_ALREADY_IN_USE
            collection.create_external_integration(fields.get("protocol"))

        collection.name = name
        [protocol] = [p for p in self.protocols if p.get("name") == protocol]

        settings = self.validate_parent(protocol, collection)
        if isinstance(settings, ProblemDetail):
            return settings
        settings_error = self.process_settings(settings, collection)
        if settings_error:
            return settings_error

        libraries_error = self.process_libraries(protocol, collection)
        if libraries_error:
            return libraries_error

        if is_new:
            return Response(unicode(collection.id), 201)
        else:
            return Response(unicode(collection.id), 200)

    def validate_form_fields(self, is_new, **fields):
        if not fields.get("name"):
            return MISSING_COLLECTION_NAME
        if fields.get("protocol") and fields.get("protocol") not in [p.get("name") for p in self.protocols]:
            return UNKNOWN_PROTOCOL
        if not fields.get("protocol"):
            return NO_PROTOCOL_FOR_NEW_SERVICE
        if "collection" in fields and not fields.get("collection"):
            return MISSING_COLLECTION
        if fields.get("collection"):
            return self.validate_collection(**fields)

    def validate_collection(self, **fields):
        if fields.get("protocol") != fields.get("collection").protocol:
            return CANNOT_CHANGE_PROTOCOL
        if fields.get("name") != fields.get("collection").name:
            collection_with_name = get_one(self._db, Collection, name=fields.get("name"))
            if collection_with_name:
                return COLLECTION_NAME_ALREADY_IN_USE

    def validate_parent(self, protocol, collection):
        parent_id = flask.request.form.get("parent_id")
        if parent_id and not protocol.get("child_settings"):
            self._db.rollback()
            return PROTOCOL_DOES_NOT_SUPPORT_PARENTS

        if parent_id:
            parent = get_one(self._db, Collection, id=parent_id)
            if not parent:
                self._db.rollback()
                return MISSING_PARENT
            collection.parent = parent
            settings = protocol.get("child_settings")
        else:
            collection.parent = None
            settings = protocol.get("settings")

        return settings

    def validate_external_account_id_setting(self, value, setting):
        if not value and not setting.get("optional"):
            # Roll back any changes to the collection that have already been made.
            self._db.rollback()
            return INCOMPLETE_CONFIGURATION.detailed(
                _("The collection configuration is missing a required setting: %(setting)s",
                  setting=setting.get("label")))

    def get_integration_id(self, value):
        if value == self.NO_MIRROR_INTEGRATION:
            integration_id = None
        else:
            integration = get_one(
                self._db, ExternalIntegration, id=value
            )
            if not integration:
                self._db.rollback()
                return MISSING_SERVICE
            if integration.goal != ExternalIntegration.STORAGE_GOAL:
                self._db.rollback()
                return INTEGRATION_GOAL_CONFLICT
            return integration.id

    def process_settings(self, settings, collection):
        for setting in settings:
            key = setting.get("key")
            value = flask.request.form.get(key)
            if key == "external_account_id":
                error = self.validate_external_account_id_setting(value, setting)
                if error:
                    return error
                collection.external_account_id = value
            elif key == 'mirror_integration_id':
                integration_id = self.get_integration_id(value)
                if isinstance(integration_id, ProblemDetail):
                    return integration_id
                collection.mirror_integration_id = integration_id
            else:
                result = self._set_integration_setting(collection.external_integration, setting)
                if isinstance(result, ProblemDetail):
                    return result

    def process_libraries(self, protocol, collection):
        libraries = []
        if flask.request.form.get("libraries"):
            libraries = json.loads(flask.request.form.get("libraries"))

        for library_info in libraries:
            library = get_one(self._db, Library, short_name=library_info.get("short_name"))
            if not library:
                return NO_SUCH_LIBRARY.detailed(_("You attempted to add the collection to %(library_short_name)s, but it does not exist.", library_short_name=library_info.get("short_name")))
            if collection not in library.collections:
                library.collections.append(collection)
            result = self._set_integration_library(collection.external_integration, library_info, protocol)
            if isinstance(result, ProblemDetail):
                return result
        for library in collection.libraries:
            if library.short_name not in [l.get("short_name") for l in libraries]:
                library.collections.remove(collection)
                for setting in protocol.get("library_settings", []):
                    ConfigurationSetting.for_library_and_externalintegration(
                        self._db, setting.get("key"), library, collection.external_integration,
                    ).value = None

    # DELETE
    def process_delete(self, collection_id):
        self.require_system_admin()
        collection = get_one(self._db, Collection, id=collection_id)
        if not collection:
            return MISSING_COLLECTION
        if len(collection.children) > 0:
            return CANNOT_DELETE_COLLECTION_WITH_CHILDREN
        self._db.delete(collection)
        return Response(unicode(_("Deleted")), 200)
