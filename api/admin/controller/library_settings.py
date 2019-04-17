import base64
import flask
from flask import Response
from flask_babel import lazy_gettext as _
import json
from pypostalcode import PostalCodeDatabase
import re
from StringIO import StringIO
import urllib
import uszipcode
import uuid
import wcag_contrast_ratio

from . import SettingsController
from api.config import Configuration
from api.lanes import create_default_lanes
from core.model import (
    ConfigurationSetting,
    create,
    ExternalIntegration,
    get_one,
    Library,
    Representation,
)
from core.util.http import HTTP
from PIL import Image
from api.admin.exceptions import *
from api.admin.problem_details import *
from core.util.problem_detail import ProblemDetail
from core.util import LanguageCodes
from nose.tools import set_trace
from api.registry import RemoteRegistry

class LibrarySettingsController(SettingsController):

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
                    value = self.current_value(setting, library)

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
        self.require_system_admin()

        library = None
        is_new = False

        library_uuid = flask.request.form.get("uuid")
        library = self.get_library_from_uuid(library_uuid)
        if isinstance(library, ProblemDetail):
            return library

        short_name = flask.request.form.get("short_name")
        short_name_not_unique = self.check_short_name_unique(library, short_name)
        if short_name_not_unique:
            return short_name_not_unique

        error = self.validate_form_fields()
        if error:
            return error

        if not library:
            (library, is_new) = self.create_library(short_name, library_uuid)
        else:
            self.require_library_manager(library)

        name = flask.request.form.get("name")
        if name:
            library.name = name
        if short_name:
            library.short_name = short_name

        configuration_settings = self.library_configuration_settings(library)
        if isinstance(configuration_settings, ProblemDetail):
            return configuration_settings

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
        validations = [
            self.check_for_missing_fields,
            self.check_input_type,
            self.check_web_color_contrast,
            self.check_header_links,
            self.validate_formats
        ]
        for validation in validations:
            result = validation(settings)
            if isinstance(result, ProblemDetail):
                return result

    def check_for_missing_fields(self, settings):
        if not flask.request.form.get("short_name"):
            return MISSING_LIBRARY_SHORT_NAME

        error = self.check_for_missing_settings(settings)
        if error:
            return error

    def check_for_missing_settings(self, settings):
        required = filter(lambda s: s.get('required') and not s.get('default'), Configuration.LIBRARY_SETTINGS)
        missing = filter(lambda s: not flask.request.form.get(s.get("key")), required)
        if missing:
            return INCOMPLETE_CONFIGURATION.detailed(
                _("The configuration is missing a required setting: %(setting)s",
                setting=missing[0].get("label"))
            )
 
    def check_input_type(self, settings):
        for setting in settings:
            if setting.get("type") == "image":
                return self.check_image_type(setting)

    def check_web_color_contrast(self, settings):
        """Verify that the web background color and web foreground
        color go together.
        """
        background = flask.request.form.get(Configuration.WEB_BACKGROUND_COLOR, Configuration.DEFAULT_WEB_BACKGROUND_COLOR)
        foreground = flask.request.form.get(Configuration.WEB_FOREGROUND_COLOR, Configuration.DEFAULT_WEB_FOREGROUND_COLOR)
        def hex_to_rgb(hex):
            hex = hex.lstrip("#")
            return tuple(int(hex[i:i+2], 16)/255.0 for i in (0, 2 ,4))
        if not wcag_contrast_ratio.passes_AA(wcag_contrast_ratio.rgb(hex_to_rgb(background), hex_to_rgb(foreground))):
            contrast_check_url = "https://contrast-ratio.com/#%23" + foreground[1:] + "-on-%23" + background[1:]
            return INVALID_CONFIGURATION_OPTION.detailed(
                _("The web background and foreground colors don't have enough contrast to pass the WCAG 2.0 AA guidelines and will be difficult for some patrons to read. Check contrast <a href='%(contrast_check_url)s' target='_blank'>here</a>.",
                  contrast_check_url=contrast_check_url))

    def check_header_links(self, settings):
        """Verify that header links and labels are the same length."""
        header_links = flask.request.form.getlist(Configuration.WEB_HEADER_LINKS)
        header_labels = flask.request.form.getlist(Configuration.WEB_HEADER_LABELS)
        if len(header_links) != len(header_labels):
            return INVALID_CONFIGURATION_OPTION.detailed(
                _("There must be the same number of web header links and web header labels."))

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
            if setting.get("format") == "geographic":
                locations = self.validate_geographic_areas(self.list_setting(setting))
                if isinstance(locations, ProblemDetail):
                    return locations
                value = locations or self.current_value(setting, library)
            elif setting.get("type") == "list":
                value = self.list_setting(setting) or self.current_value(setting, library)
                if setting.get("format") == "language-code":
                    value = json.dumps([LanguageCodes.string_to_alpha_3(language) for language in json.loads(value)])
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
            value = []
            inputs = flask.request.form.getlist(setting.get("key"))
            for i in inputs:
                value.extend(i) if isinstance(i, list) else value.append(i)

        return json.dumps(filter(None, value))

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

    def validate_geographic_areas(self, values):
        # Note: the validator does not recognize data from US territories other than Puerto Rico.

        us_search = uszipcode.SearchEngine(simple_zipcode=True)
        ca_search = PostalCodeDatabase()
        CA_PROVINCES = ["AB", "BC", "MB", "NB", "NL", "NT", "NS", "NU", "ON", "PE", "QC", "SK", "YT"]

        locations = {"US": [], "CA": []}

        for value in json.loads(values):
            flagged = False
            if value == "everywhere":
                locations["US"].append(value)
            elif len(value) and isinstance(value, basestring):
                if len(value) == 2:
                    # Is it a US state or Canadian province abbreviation?
                    if value in CA_PROVINCES:
                        locations["CA"].append(value)
                    elif len(us_search.query(state=value)):
                        locations["US"].append(value)
                    else:
                        return UNKNOWN_LOCATION.detailed(_('"%(value)s" is not a valid U.S. state or Canadian province abbreviation.', value=value))
                elif len(value) == 3 and re.search("^[A-Za-z]\\d[A-Za-z]", value):
                    # Is it a Canadian zipcode?
                    try:
                        info = ca_search[value]
                        locations["CA"].append(self.format_place(value, info.city, info.province));
                    except:
                        return UNKNOWN_LOCATION.detailed(_('"%(value)s" is not a valid Canadian zipcode.', value=value))
                elif len(value.split(", ")) == 2:
                    # Is it in the format "[city], [state abbreviation]" or "[county], [state abbreviation]"?
                    city_or_county, state = value.split(", ")
                    if us_search.by_city_and_state(city_or_county, state):
                        locations["US"].append(value);
                    elif len([x for x in us_search.query(state=state, returns=None) if x.county == city_or_county]):
                        locations["US"].append(value);
                    else:
                        # Flag this as needing to be checked with the registry
                        flagged = True
                elif value.isdigit():
                    # Is it a US zipcode?
                    info = us_search.by_zipcode(value)
                    if not info:
                        return UNKNOWN_LOCATION.detailed(_('"%(value)s" is not a valid U.S. zipcode.', value=value))
                    locations["US"].append(self.format_place(value, info.major_city, info.state));
                else:
                    flagged = True

                if flagged:
                    registry_response = self.find_location_through_registry(value)
                    if registry_response and isinstance(registry_response, ProblemDetail):
                        return registry_response
                    elif registry_response:
                        locations[registry_response].append(value)
                    else:
                        return UNKNOWN_LOCATION.detailed(_('Unable to locate "%(value)s".', value=value))

        return json.dumps(locations)

    def format_place(self, zip, city, state_or_province):
        info = "%s, %s" % (city, state_or_province)
        return { zip: info }

    def find_location_through_registry(self, value):
        for nation in ["US", "CA"]:
            service_area_object = urllib.quote('{"%s": "%s"}' % (nation, value))
            registry_check = self.ask_registry(service_area_object)
            if registry_check and isinstance(registry_check, ProblemDetail):
                return registry_check
            elif registry_check:
                # If the registry has established that this is a US location, don't bother also trying to find it in Canada
                return nation

    def ask_registry(self, service_area_object, do_get=HTTP.debuggable_get):
        # If the circulation manager doesn't know about this location, check whether the Library Registry does.
        result = None
        for registry in RemoteRegistry.for_protocol_and_goal(
            self._db, ExternalIntegration.OPDS_REGISTRATION, ExternalIntegration.DISCOVERY_GOAL
        ):
            base_url = registry.integration.url + "/coverage?coverage="

            response = do_get(base_url + service_area_object)
            if not response.status_code == 200:
                result = REMOTE_INTEGRATION_FAILED.detailed(_("Unable to contact the registry at %(url)s.", url=registry.integration.url))

            if hasattr(response, "content"):
                content = json.loads(response.content)
                found_place = not (content.get("unknown") or content.get("ambiguous"))
                if found_place:
                    return True

        return result
