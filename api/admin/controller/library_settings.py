import base64
import flask
from flask import Response
from flask_babel import lazy_gettext as _
import json
from StringIO import StringIO
import uuid
from . import AdminCirculationManagerController
from api.config import Configuration
from api.lanes import create_default_lanes
from core.model import (
    ConfigurationSetting,
    create,
    get_one,
    Library,
    Representation,
)
from PIL import Image
from api.admin.exceptions import *
from api.admin.problem_details import *
from nose.tools import set_trace

class LibrarySettingsController(AdminCirculationManagerController):

    def process_get(self):
        libraries = []
        for library in self._db.query(Library).order_by(Library.name):
            # Only include libraries this admin has librarian access to.
            if not flask.request.admin or not flask.request.admin.is_librarian(library):
                continue

            settings = dict()
            for setting in Configuration.LIBRARY_SETTINGS:
                if setting.get("type") == "list":
                    value = ConfigurationSetting.for_library(setting.get("key"), library).json_value
                else:
                    value = ConfigurationSetting.for_library(setting.get("key"), library).value
                if value:
                    settings[setting.get("key")] = value
            libraries += [dict(
                uuid=library.uuid,
                name=library.name,
                short_name=library.short_name,
                settings=settings,
            )]
        return dict(libraries=libraries, settings=Configuration.LIBRARY_SETTINGS)

    def process_post(self):
        library = None
        is_new = False

        error = self.validate_form_fields()
        if error:
            return error

        library_uuid = flask.request.form.get("uuid")
        name = flask.request.form.get("name")
        library = self.get_library_from_uuid(library_uuid)
        short_name = flask.request.form.get("short_name")
        short_name_not_unique = self.check_short_name_unique(library, short_name)
        if short_name_not_unique:
            return short_name_not_unique

        if not library:
            (library, is_new) = self.create_library(short_name, library_uuid)
        else:
            self.require_library_manager(library)

        if name:
            library.name = name
        if short_name:
            library.short_name = short_name

        self.library_configuration_settings(library)

        if is_new:
            # Now that the configuration settings are in place, create
            # a default set of lanes.
            create_default_lanes(self._db, library)
            return Response(unicode(library.uuid), 201)
        else:
            return Response(unicode(library.uuid), 200)

    def create_library(self, short_name, library_uuid):
        self.require_system_admin()
        library, is_new = create(
            self._db, Library, short_name=short_name,
            uuid=str(uuid.uuid4()))
        return library, is_new

    def process_delete(self, library_uuid):
        self.require_system_admin()
        library = self.get_library_from_uuid(library_uuid)
        self._db.delete(library)
        return Response(unicode(_("Deleted")), 200)

# Validation methods:

    def validate_form_fields(self):
        settings = Configuration.LIBRARY_SETTINGS
        return self.check_for_missing_fields(settings) or self.check_input_type(settings)

    def check_for_missing_fields(self, settings):
        MISSING_FIELD_MESSAGES = dict(
            short_name = MISSING_LIBRARY_SHORT_NAME,
        )

        for field in flask.request.form.keys():
            if MISSING_FIELD_MESSAGES.get(field) and not flask.request.form.get(field):
                return MISSING_FIELD_MESSAGES.get(field)

        self.check_for_missing_settings(settings)

    def check_for_missing_settings(self, settings):
        required = filter(lambda s: not s.get('optional') and not s.get('default'), Configuration.LIBRARY_SETTINGS)
        missing = filter(lambda s: not flask.request.form.get(s.get("key")), required)
        if missing:
            return INCOMPLETE_CONFIGURATION.detailed(
                _("The configuration is missing a required setting: %(setting)s",
                setting=missing[0].get("label"))
            )

    def check_input_type(self, settings):
        # Once there are validations for email address, URL, etc., they'll get called
        # from here; for now, it's just image type.
        for setting in settings:
            if setting.get("type") == "image":
                return self.check_image_type(setting)

    def get_library_from_uuid(self, library_uuid):
        if library_uuid:
            # Library UUID is required when editing an existing library
            # from the admin interface, and isn't present for new libraries.
            library = get_one(
                self._db, Library, uuid=library_uuid,
            )
            if library:
                return library
            else:
                return LIBRARY_NOT_FOUND.detailed(_("The specified library uuid does not exist."))

    def check_short_name_unique(self, library, short_name):
        if not library or short_name != library.short_name:
            # If you're adding a new short_name, either by editing an
            # existing library or creating a new library, it must be unique.
            library_with_short_name = get_one(self._db, Library, short_name=short_name)
            if library_with_short_name:
                return LIBRARY_SHORT_NAME_ALREADY_IN_USE

    def check_image_type(self, setting):
        allowed_types = [Representation.JPEG_MEDIA_TYPE, Representation.PNG_MEDIA_TYPE, Representation.GIF_MEDIA_TYPE]
        image_file = flask.request.files.get(setting.get("key"))
        if image_file:
            image_type = image_file.headers.get("Content-Type")
            if image_type not in allowed_types:
                return INVALID_CONFIGURATION_OPTION.detailed(_(
                    "Upload for %(setting)s must be in GIF, PNG, or JPG format. (Upload was %(format)s.)",
                    setting=setting.get("label"),
                    format=image_type))


# Configuration settings:

    def library_configuration_settings(self, library):
        for setting in Configuration.LIBRARY_SETTINGS:
            if setting.get("type") == "list":
                value = self.list_setting(setting) or self.current_value(setting, library)
            elif setting.get("type") == "image":
                value = self.image_setting(setting) or self.current_value(setting, library)
            else:
                default = setting.get('default')
                value = flask.request.form.get(setting['key'], default)

            ConfigurationSetting.for_library(setting['key'], library).value = value

    def current_value(self, setting, library):
        return ConfigurationSetting.for_library(setting['key'], library).value

    def list_setting(self, setting):
        if setting.get('options'):
            # Restrict to the values in 'options'.
            value = []
            for option in setting.get("options"):
                if setting["key"] + "_" + option["key"] in flask.request.form:
                    value += [option["key"]]
        else:
            # Allow any entered values.
            value = [item for item in flask.request.form.getlist(setting.get('key')) if item]

        return json.dumps(value)

    def image_setting(self, setting):
        image_file = flask.request.files.get(setting.get("key"))
        if image_file:
            image = Image.open(image_file)
            width, height = image.size
            if width > 135 or height > 135:
                image.thumbnail((135, 135), Image.ANTIALIAS)
            buffer = StringIO()
            image.save(buffer, format="PNG")
            b64 = base64.b64encode(buffer.getvalue())
            return "data:image/png;base64,%s" % b64
