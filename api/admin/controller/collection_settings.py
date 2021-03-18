from . import SettingsController
import flask
from flask import Response
from flask_babel import lazy_gettext as _
import json
from api.admin.problem_details import *
from core.model import (
    Collection,
    ConfigurationSetting,
    ExternalIntegration,
    get_one,
    get_one_or_create,
    Library,
)
from core.util.problem_detail import ProblemDetail
from core.model.configuration import ExternalIntegrationLink

class CollectionSettingsController(SettingsController):
    def __init__(self, manager):
        super(CollectionSettingsController, self).__init__(manager)
        self.type = _("collection")

    def _get_collection_protocols(self):
        protocols = super(CollectionSettingsController, self)._get_collection_protocols(self.PROVIDER_APIS)
        # If there are storage integrations, add a mirror integration
        # setting to every protocol's 'settings' block.
        mirror_integration_settings = self._mirror_integration_settings()
        if mirror_integration_settings:
            for protocol in protocols:
                protocol['settings'] += mirror_integration_settings
        return protocols

    def process_collections(self):
        if flask.request.method == 'GET':
            return self.process_get()
        else:
            return self.process_post()

    # GET
    def process_get(self):
        protocols = self._get_collection_protocols()
        user = flask.request.admin
        collections = []
        protocolClass = None
        for collection_object in self._db.query(Collection).order_by(Collection.name).all():
            if not user or not user.can_see_collection(collection_object):
                continue

            collection_dict = self.collection_to_dict(collection_object)

            if collection_object.protocol in [p.get("name") for p in protocols]:
                [protocol] = [p for p in protocols if p.get("name") == collection_object.protocol]
                libraries = self.load_libraries(collection_object, user, protocol)
                collection_dict['libraries'] = libraries
                settings = self.load_settings(protocol.get("settings"), collection_object, collection_dict.get("settings"))
                collection_dict['settings'] = settings
                protocolClass = self.find_protocol_class(collection_object)

            collection_dict["self_test_results"] = self._get_prior_test_results(collection_object, protocolClass)
            collection_dict["marked_for_deletion"] = collection_object.marked_for_deletion

            collections.append(collection_dict)

        return dict(
            collections=collections,
            protocols=protocols,
        )

    def collection_to_dict(self, collection_object):
        return dict(
            id=collection_object.id,
            name=collection_object.name,
            protocol=collection_object.protocol,
            parent_id=collection_object.parent_id,
        )

    def load_libraries(self, collection_object, user, protocol):
        """Get a list of the libraries that 1) are associated with this collection
        and 2) the user is affiliated with"""

        libraries = []
        for library in collection_object.libraries:
            if not user or not user.is_librarian(library):
                continue
            libraries.append(self._get_integration_library_info(
                    collection_object.external_integration, library, protocol))

        return libraries

    def load_settings(self, protocol_settings, collection_object, collection_settings):
        """Compile the information about the collection that corresponds to the settings
        externally imposed by the collection's protocol."""

        settings = {}
        for protocol_setting in protocol_settings:
            if not protocol_setting:
                continue
            key = protocol_setting.get("key")
            if not collection_settings or key not in collection_settings:
                if key.endswith('mirror_integration_id'):
                    storage_integration = get_one(
                        self._db, ExternalIntegrationLink,
                        external_integration_id=collection_object.external_integration_id,
                        # either 'books_mirror' or 'covers_mirror'
                        purpose=key.rsplit('_', 2)[0]
                    )
                    if storage_integration:
                        value = str(storage_integration.other_integration_id)
                    else:
                        value = self.NO_MIRROR_INTEGRATION
                elif protocol_setting.get("type") in ("list", "menu"):
                    value = collection_object.external_integration.setting(key).json_value
                else:
                    value = collection_object.external_integration.setting(key).value
                settings[key] = value
        settings["external_account_id"] = collection_object.external_account_id
        return settings

    def find_protocol_class(self, collection_object):
        """Figure out which class this collection's protocol belongs to, from the list
        of possible protocols defined in PROVIDER_APIS (in SettingsController)"""

        protocolClassFound = [p for p in self.PROVIDER_APIS if p.NAME == collection_object.protocol]
        if len(protocolClassFound) == 1:
            return protocolClassFound[0]

    # POST
    def process_post(self):
        self.require_system_admin()
        protocols = self._get_collection_protocols()
        is_new = False
        collection = None

        name = flask.request.form.get("name")
        protocol_name = flask.request.form.get("protocol")
        fields = {"name": name, "protocol": protocol_name}
        id = flask.request.form.get("id")
        if id:
            collection = get_one(self._db, Collection, id=id)
            fields["collection"] = collection

        error = self.validate_form_fields(is_new, protocols, **fields)
        if error:
            return error

        if protocol_name and not collection:
            collection, is_new = get_one_or_create(self._db, Collection, name=name)
            if not is_new:
                self._db.rollback()
                return COLLECTION_NAME_ALREADY_IN_USE
            collection.create_external_integration(protocol_name)

        collection.name = name
        [protocol_dict] = [p for p in protocols if p.get("name") == protocol_name]

        settings = self.validate_parent(protocol_dict, collection)
        if isinstance(settings, ProblemDetail):
            self._db.rollback()
            return settings

        settings_error = self.process_settings(settings, collection)
        if settings_error:
            self._db.rollback()
            return settings_error

        libraries_error = self.process_libraries(protocol_dict, collection)
        if libraries_error:
            return libraries_error

        if is_new:
            return Response(str(collection.id), 201)
        else:
            return Response(str(collection.id), 200)

    def validate_form_fields(self, is_new, protocols, **fields):
        """Check that 1) the required fields aren't blank, 2) the protocol is on the
        list of recognized protocols, 3) the collection (if there is one) is valid, and
        4) the URL is valid"""
        if not fields.get("name"):
            return MISSING_COLLECTION_NAME
        if "collection" in fields:
            if fields.get("collection"):
                invalid_collection = self.validate_collection(**fields)
                if invalid_collection:
                    return invalid_collection
            else:
                return MISSING_COLLECTION
        if fields.get("protocol"):
            if fields.get("protocol") not in [p.get("name") for p in protocols]:
                return UNKNOWN_PROTOCOL
            else:
                [protocol] = [p for p in protocols if p.get("name") == fields.get("protocol")]
                wrong_format = self.validate_formats(protocol.get("settings"))
                if wrong_format:
                    return wrong_format
        else:
            return NO_PROTOCOL_FOR_NEW_SERVICE

    def validate_collection(self, **fields):
        """The protocol of an existing collection cannot be changed, and
        collections must have unique names."""
        if fields.get("protocol") != fields.get("collection").protocol:
            return CANNOT_CHANGE_PROTOCOL
        if fields.get("name") != fields.get("collection").name:
            collection_with_name = get_one(self._db, Collection, name=fields.get("name"))
            if collection_with_name:
                return COLLECTION_NAME_ALREADY_IN_USE

    def validate_parent(self, protocol, collection):
        """Verify that the parent collection is set properly, then determine
        the type of the settings that need to be validated: are they 1) settings for a
        regular collection (e.g. client key and client secret for an Overdrive collection),
        or 2) settings for a child collection (e.g. library ID for an Overdrive Advantage collection)?"""

        parent_id = flask.request.form.get("parent_id")
        if parent_id and not protocol.get("child_settings"):
            return PROTOCOL_DOES_NOT_SUPPORT_PARENTS
        if parent_id:
            parent = get_one(self._db, Collection, id=parent_id)
            if not parent:
                return MISSING_PARENT
            collection.parent = parent
            settings = protocol.get("child_settings")
        else:
            collection.parent = None
            settings = protocol.get("settings")

        return settings

    def validate_external_account_id_setting(self, value, setting):
        """Check that the user has submitted any required values for associating
        this collection with an external account."""
        if not value and not setting.get("optional"):
            # Roll back any changes to the collection that have already been made.
            return INCOMPLETE_CONFIGURATION.detailed(
                _("The collection configuration is missing a required setting: %(setting)s",
                  setting=setting.get("label")))

    def process_settings(self, settings, collection):
        """Go through the settings that the user has just submitted for this collection,
        and check that each setting is valid and that no required settings are missing.  If
        the setting passes all of the validations, go ahead and set it for this collection."""

        for setting in settings:
            key = setting.get("key")
            value = flask.request.form.get(key)
            if key == "external_account_id":
                error = self.validate_external_account_id_setting(value, setting)
                if error:
                    return error
                collection.external_account_id = value
            elif key.endswith('mirror_integration_id') and value:
                external_integration_link = self._set_external_integration_link(
                    self._db, key, value, collection,
                )

                if isinstance(external_integration_link, ProblemDetail):
                    return external_integration_link
            else:
                result = self._set_integration_setting(collection.external_integration, setting)
                if isinstance(result, ProblemDetail):
                    return result

    def _set_external_integration_link(
            self, _db, key, value, collection,
    ):
        """Find or create a ExternalIntegrationLink and either delete it
        or update the other external integration it links to.
        """

        collection_service = get_one(
            _db, ExternalIntegration,
            id=collection.external_integration_id
        )

        storage_service = None
        other_integration_id = None

        purpose = key.rsplit('_', 2)[0]
        external_integration_link, ignore = get_one_or_create(
            _db, ExternalIntegrationLink,
            library_id=None,
            external_integration_id=collection_service.id,
            purpose=purpose
        )
        if not external_integration_link:
            return MISSING_INTEGRATION

        if value == self.NO_MIRROR_INTEGRATION:
            _db.delete(external_integration_link)
        else:
            storage_service = get_one(
                _db, ExternalIntegration,
                id=value
            )
            if storage_service:
                if storage_service.goal != ExternalIntegration.STORAGE_GOAL:
                    return INTEGRATION_GOAL_CONFLICT
                other_integration_id = storage_service.id
            else:
                return MISSING_SERVICE

        external_integration_link.other_integration_id = other_integration_id

        return external_integration_link

    def process_libraries(self, protocol, collection):
        """Go through the libraries that the user is trying to associate with this collection;
        check that each library actually exists, and that the library-related configuration settings
        that the user has submitted are complete and valid.  If the library passes all of the validations,
        go ahead and associate it with this collection."""

        libraries = []
        if flask.request.form.get("libraries"):
            libraries = json.loads(flask.request.form.get("libraries"))

        for library_info in libraries:
            library = get_one(self._db, Library, short_name=library_info.get("short_name"))
            if not library:
                return NO_SUCH_LIBRARY.detailed(_("You attempted to add the collection to %(library_short_name)s, but the library does not exist.", library_short_name=library_info.get("short_name")))
            if collection not in library.collections:
                library.collections.append(collection)
            result = self._set_integration_library(collection.external_integration, library_info, protocol)
            if isinstance(result, ProblemDetail):
                return result
        for library in collection.libraries:
            if library.short_name not in [l.get("short_name") for l in libraries]:
                collection.disassociate_library(library)

    # DELETE
    def process_delete(self, collection_id):
        self.require_system_admin()
        collection = get_one(self._db, Collection, id=collection_id)
        if not collection:
            return MISSING_COLLECTION
        if len(collection.children) > 0:
            return CANNOT_DELETE_COLLECTION_WITH_CHILDREN

        # Flag the collection to be deleted by script in the background.
        collection.marked_for_deletion = True
        return Response(str(_("Deleted")), 200)
