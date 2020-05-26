from nose.tools import (
    set_trace,
    eq_,
    assert_raises
)
import base64
import flask
import json
import urllib
from StringIO import StringIO
from werkzeug.datastructures import ImmutableMultiDict, MultiDict
from api.admin.exceptions import *
from api.config import Configuration
from api.registry import (
    Registration,
    RemoteRegistry,
)
from core.facets import FacetConstants
from core.model import (
    AdminRole,
    ConfigurationSetting,
    create,
    ExternalIntegration,
    get_one,
    get_one_or_create,
    Library,
)
from core.testing import MockRequestsResponse
from core.util.problem_detail import ProblemDetail
from api.admin.controller.library_settings import LibrarySettingsController
from api.admin.geographic_validator import GeographicValidator
from test_controller import SettingsControllerTest

class TestLibrarySettings(SettingsControllerTest):

    def library_form(self, library, fields={}):

        defaults = {
            "uuid": library.uuid,
            "name": "The New York Public Library",
            "short_name": library.short_name,
            Configuration.WEBSITE_URL: "https://library.library/",
            Configuration.HELP_EMAIL: "help@example.com",
            Configuration.DEFAULT_NOTIFICATION_EMAIL_ADDRESS: "email@example.com"
        }
        defaults.update(fields)
        form = MultiDict(defaults.items())
        return form

    def test_libraries_get_with_no_libraries(self):
        # Delete any existing library created by the controller test setup.
        library = get_one(self._db, Library)
        if library:
            self._db.delete(library)

        with self.app.test_request_context("/"):
            response = self.manager.admin_library_settings_controller.process_get()
            eq_(response.get("libraries"), [])

    def test_libraries_get_with_geographic_info(self):
        # Delete any existing library created by the controller test setup.
        library = get_one(self._db, Library)
        if library:
            self._db.delete(library)

        test_library = self._library("Library 1", "L1")
        ConfigurationSetting.for_library(
            Configuration.LIBRARY_FOCUS_AREA, test_library
        ).value = '{"CA": ["N3L"], "US": ["11235"]}'
        ConfigurationSetting.for_library(
            Configuration.LIBRARY_SERVICE_AREA, test_library
        ).value = '{"CA": ["J2S"], "US": ["31415"]}'

        with self.request_context_with_admin("/"):
            response = self.manager.admin_library_settings_controller.process_get()
            library_settings = response.get("libraries")[0].get("settings")
            eq_(library_settings.get("focus_area"), {u'CA': [{u'N3L': u'Paris, Ontario'}], u'US': [{u'11235': u'Brooklyn, NY'}]})
            eq_(library_settings.get("service_area"), {u'CA': [{u'J2S': u'Saint-Hyacinthe Southwest, Quebec'}], u'US': [{u'31415': u'Savannah, GA'}]})

    def test_libraries_get_with_multiple_libraries(self):
        # Delete any existing library created by the controller test setup.
        library = get_one(self._db, Library)
        if library:
            self._db.delete(library)

        l1 = self._library("Library 1", "L1")
        l2 = self._library("Library 2", "L2")
        l3 = self._library("Library 3", "L3")
        # L2 has some additional library-wide settings.
        ConfigurationSetting.for_library(Configuration.FEATURED_LANE_SIZE, l2).value = 5
        ConfigurationSetting.for_library(
            Configuration.DEFAULT_FACET_KEY_PREFIX + FacetConstants.ORDER_FACET_GROUP_NAME, l2
        ).value = FacetConstants.ORDER_RANDOM
        ConfigurationSetting.for_library(
            Configuration.ENABLED_FACETS_KEY_PREFIX + FacetConstants.ORDER_FACET_GROUP_NAME, l2
        ).value = json.dumps([FacetConstants.ORDER_TITLE, FacetConstants.ORDER_RANDOM])
        ConfigurationSetting.for_library(
            Configuration.LARGE_COLLECTION_LANGUAGES, l2
        ).value = json.dumps(["French"])
        # The admin only has access to L1 and L2.
        self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        self.admin.add_role(AdminRole.LIBRARIAN, l1)
        self.admin.add_role(AdminRole.LIBRARY_MANAGER, l2)

        with self.request_context_with_admin("/"):
            response = self.manager.admin_library_settings_controller.process_get()
            libraries = response.get("libraries")
            eq_(2, len(libraries))

            eq_(l1.uuid, libraries[0].get("uuid"))
            eq_(l2.uuid, libraries[1].get("uuid"))

            eq_(l1.name, libraries[0].get("name"))
            eq_(l2.name, libraries[1].get("name"))

            eq_(l1.short_name, libraries[0].get("short_name"))
            eq_(l2.short_name, libraries[1].get("short_name"))

            eq_({}, libraries[0].get("settings"))
            eq_(4, len(libraries[1].get("settings").keys()))
            settings = libraries[1].get("settings")
            eq_("5", settings.get(Configuration.FEATURED_LANE_SIZE))
            eq_(FacetConstants.ORDER_RANDOM,
                settings.get(Configuration.DEFAULT_FACET_KEY_PREFIX + FacetConstants.ORDER_FACET_GROUP_NAME))
            eq_([FacetConstants.ORDER_TITLE, FacetConstants.ORDER_RANDOM],
               settings.get(Configuration.ENABLED_FACETS_KEY_PREFIX + FacetConstants.ORDER_FACET_GROUP_NAME))
            eq_(["French"], settings.get(Configuration.LARGE_COLLECTION_LANGUAGES))

    def test_libraries_post_errors(self):
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Brooklyn Public Library"),
            ])
            response = self.manager.admin_library_settings_controller.process_post()
            eq_(response, MISSING_LIBRARY_SHORT_NAME)

        self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Brooklyn Public Library"),
                ("short_name", "bpl"),
            ])
            assert_raises(AdminNotAuthorized,
              self.manager.admin_library_settings_controller.process_post)

        library = self._library()
        self.admin.add_role(AdminRole.LIBRARIAN, library)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("uuid", library.uuid),
                ("name", "Brooklyn Public Library"),
                ("short_name", library.short_name),
            ])
            assert_raises(AdminNotAuthorized,
                self.manager.admin_library_settings_controller.process_post)

        self.admin.add_role(AdminRole.SYSTEM_ADMIN)
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = self.library_form(library, {"uuid": "1234"})
            response = self.manager.admin_library_settings_controller.process_post()
            eq_(response.uri, LIBRARY_NOT_FOUND.uri)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Brooklyn Public Library"),
                ("short_name", library.short_name),
            ])
            response = self.manager.admin_library_settings_controller.process_post()
            eq_(response, LIBRARY_SHORT_NAME_ALREADY_IN_USE)

        bpl, ignore = get_one_or_create(
            self._db, Library, short_name="bpl"
        )
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("uuid", bpl.uuid),
                ("name", "Brooklyn Public Library"),
                ("short_name", library.short_name),
            ])
            response = self.manager.admin_library_settings_controller.process_post()
            eq_(response, LIBRARY_SHORT_NAME_ALREADY_IN_USE)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("uuid", library.uuid),
                ("name", "The New York Public Library"),
                ("short_name", library.short_name),
            ])
            response = self.manager.admin_library_settings_controller.process_post()
            eq_(response.uri, INCOMPLETE_CONFIGURATION.uri)

        # Test a bad contrast ratio between the web foreground and
        # web background colors.
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = self.library_form(
                library, {Configuration.WEB_BACKGROUND_COLOR: "#000000",
                Configuration.WEB_FOREGROUND_COLOR: "#010101"}
            )
            response = self.manager.admin_library_settings_controller.process_post()
            eq_(response.uri, INVALID_CONFIGURATION_OPTION.uri)
            assert "contrast-ratio.com/#%23010101-on-%23000000" in response.detail

        # Test a list of web header links and a list of labels that
        # aren't the same length.
        library = self._library()
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("uuid", library.uuid),
                ("name", "The New York Public Library"),
                ("short_name", library.short_name),
                (Configuration.WEBSITE_URL, "https://library.library/"),
                (Configuration.DEFAULT_NOTIFICATION_EMAIL_ADDRESS, "email@example.com"),
                (Configuration.HELP_EMAIL, "help@example.com"),
                (Configuration.WEB_HEADER_LINKS, "http://library.com/1"),
                (Configuration.WEB_HEADER_LINKS, "http://library.com/2"),
                (Configuration.WEB_HEADER_LABELS, "One"),
            ])
            response = self.manager.admin_library_settings_controller.process_post()
            eq_(response.uri, INVALID_CONFIGURATION_OPTION.uri)


    def test_libraries_post_create(self):
        class TestFileUpload(StringIO):
            headers = { "Content-Type": "image/png" }
        image_data = '\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x01\x03\x00\x00\x00%\xdbV\xca\x00\x00\x00\x06PLTE\xffM\x00\x01\x01\x01\x8e\x1e\xe5\x1b\x00\x00\x00\x01tRNS\xcc\xd24V\xfd\x00\x00\x00\nIDATx\x9cc`\x00\x00\x00\x02\x00\x01H\xaf\xa4q\x00\x00\x00\x00IEND\xaeB`\x82'

        original_validate = GeographicValidator().validate_geographic_areas
        class MockValidator(GeographicValidator):
            def __init__(self):
                self.was_called = False
            def validate_geographic_areas(self, values, db):
                self.was_called = True
                return original_validate(values, db)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "The New York Public Library"),
                ("short_name", "nypl"),
                ("library_description", "Short description of library"),
                (Configuration.WEBSITE_URL, "https://library.library/"),
                (Configuration.TINY_COLLECTION_LANGUAGES, ['ger']),
                (Configuration.LIBRARY_SERVICE_AREA, ['06759', 'everywhere', 'MD', 'Boston, MA']),
                (Configuration.LIBRARY_FOCUS_AREA, ['Manitoba', 'Broward County, FL', 'QC']),
                (Configuration.DEFAULT_NOTIFICATION_EMAIL_ADDRESS, "email@example.com"),
                (Configuration.HELP_EMAIL, "help@example.com"),
                (Configuration.FEATURED_LANE_SIZE, "5"),
                (Configuration.DEFAULT_FACET_KEY_PREFIX + FacetConstants.ORDER_FACET_GROUP_NAME,
                 FacetConstants.ORDER_RANDOM),
                (Configuration.ENABLED_FACETS_KEY_PREFIX + FacetConstants.ORDER_FACET_GROUP_NAME + "_" + FacetConstants.ORDER_TITLE,
                 ''),
                (Configuration.ENABLED_FACETS_KEY_PREFIX + FacetConstants.ORDER_FACET_GROUP_NAME + "_" + FacetConstants.ORDER_RANDOM,
                 ''),
            ])
            flask.request.files = MultiDict([
                (Configuration.LOGO, TestFileUpload(image_data)),
            ])
            validator = MockValidator()
            validators = dict(geographic=validator)
            response = self.manager.admin_library_settings_controller.process_post(validators)
            eq_(response.status_code, 201)

        library = get_one(self._db, Library, short_name="nypl")
        eq_(library.uuid, response.response[0])
        eq_(library.name, "The New York Public Library")
        eq_(library.short_name, "nypl")
        eq_("5", ConfigurationSetting.for_library(Configuration.FEATURED_LANE_SIZE, library).value)
        eq_(FacetConstants.ORDER_RANDOM,
            ConfigurationSetting.for_library(
                Configuration.DEFAULT_FACET_KEY_PREFIX + FacetConstants.ORDER_FACET_GROUP_NAME,
                library).value)
        eq_(json.dumps([FacetConstants.ORDER_TITLE, FacetConstants.ORDER_RANDOM]),
            ConfigurationSetting.for_library(
                Configuration.ENABLED_FACETS_KEY_PREFIX + FacetConstants.ORDER_FACET_GROUP_NAME,
                library).value)
        eq_("data:image/png;base64,%s" % base64.b64encode(image_data),
            ConfigurationSetting.for_library(Configuration.LOGO, library).value)
        eq_(validator.was_called, True)
        eq_('{"CA": [], "US": ["06759", "everywhere", "MD", "Boston, MA"]}',
            ConfigurationSetting.for_library(Configuration.LIBRARY_SERVICE_AREA, library).value)
        eq_('{"CA": ["Manitoba", "Quebec"], "US": ["Broward County, FL"]}',
            ConfigurationSetting.for_library(Configuration.LIBRARY_FOCUS_AREA, library).value)

        # When the library was created, default lanes were also created
        # according to its language setup. This library has one tiny
        # collection (not a good choice for a real library), so only
        # two lanes were created: "Other Languages" and then "German"
        # underneath it.
        [german, other_languages] = sorted(
            library.lanes, key=lambda x: x.display_name
        )
        eq_(None, other_languages.parent)
        eq_(['ger'], other_languages.languages)
        eq_(other_languages, german.parent)
        eq_(['ger'], german.languages)

    def test_libraries_post_edit(self):
        # A library already exists.
        library = self._library("New York Public Library", "nypl")

        ConfigurationSetting.for_library(Configuration.FEATURED_LANE_SIZE, library).value = 5
        ConfigurationSetting.for_library(
            Configuration.DEFAULT_FACET_KEY_PREFIX + FacetConstants.ORDER_FACET_GROUP_NAME, library
        ).value = FacetConstants.ORDER_RANDOM
        ConfigurationSetting.for_library(
            Configuration.ENABLED_FACETS_KEY_PREFIX + FacetConstants.ORDER_FACET_GROUP_NAME, library
        ).value = json.dumps([FacetConstants.ORDER_TITLE, FacetConstants.ORDER_RANDOM])
        ConfigurationSetting.for_library(
            Configuration.LOGO, library
        ).value = "A tiny image"

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("uuid", library.uuid),
                ("name", "The New York Public Library"),
                ("short_name", "nypl"),
                (Configuration.FEATURED_LANE_SIZE, "20"),
                (Configuration.MINIMUM_FEATURED_QUALITY, "0.9"),
                (Configuration.WEBSITE_URL, "https://library.library/"),
                (Configuration.DEFAULT_NOTIFICATION_EMAIL_ADDRESS, "email@example.com"),
                (Configuration.HELP_EMAIL, "help@example.com"),
                (Configuration.DEFAULT_FACET_KEY_PREFIX + FacetConstants.ORDER_FACET_GROUP_NAME,
                 FacetConstants.ORDER_AUTHOR),
                (Configuration.ENABLED_FACETS_KEY_PREFIX + FacetConstants.ORDER_FACET_GROUP_NAME + "_" + FacetConstants.ORDER_AUTHOR,
                 ''),
                (Configuration.ENABLED_FACETS_KEY_PREFIX + FacetConstants.ORDER_FACET_GROUP_NAME + "_" + FacetConstants.ORDER_RANDOM,
                 ''),
            ])
            flask.request.files = MultiDict([])
            response = self.manager.admin_library_settings_controller.process_post()
            eq_(response.status_code, 200)

        library = get_one(self._db, Library, uuid=library.uuid)

        eq_(library.uuid, response.response[0])
        eq_(library.name, "The New York Public Library")
        eq_(library.short_name, "nypl")

        # The library-wide settings were updated.
        def val(x):
            return ConfigurationSetting.for_library(x, library).value
        eq_("https://library.library/", val(Configuration.WEBSITE_URL))
        eq_("email@example.com", val(Configuration.DEFAULT_NOTIFICATION_EMAIL_ADDRESS))
        eq_("help@example.com", val(Configuration.HELP_EMAIL))
        eq_("20", val(Configuration.FEATURED_LANE_SIZE))
        eq_("0.9", val(Configuration.MINIMUM_FEATURED_QUALITY))
        eq_(FacetConstants.ORDER_AUTHOR,
            val(Configuration.DEFAULT_FACET_KEY_PREFIX + FacetConstants.ORDER_FACET_GROUP_NAME)
        )
        eq_(json.dumps([FacetConstants.ORDER_AUTHOR, FacetConstants.ORDER_RANDOM]),
            val(Configuration.ENABLED_FACETS_KEY_PREFIX + FacetConstants.ORDER_FACET_GROUP_NAME)
        )

        # The library-wide logo was not updated and has been left alone.
        eq_("A tiny image",
            ConfigurationSetting.for_library(Configuration.LOGO, library).value
        )

    def test_library_delete(self):
        library = self._library()

        with self.request_context_with_admin("/", method="DELETE"):
            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_library_settings_controller.process_delete,
                          library.uuid)

            self.admin.add_role(AdminRole.SYSTEM_ADMIN)
            response = self.manager.admin_library_settings_controller.process_delete(library.uuid)
            eq_(response.status_code, 200)

        library = get_one(self._db, Library, uuid=library.uuid)
        eq_(None, library)

    def test_library_configuration_settings(self):
        # Verify that library_configuration_settings validates and updates every
        # setting for a library.
        settings = [
            dict(key="setting1", format="format1"),
            dict(key="setting2", format="format2"),
        ]

        # format1 has a custom validation class; format2 does not.
        validator1 = object()
        validators = dict(format1=validator1)

        class MockController(LibrarySettingsController):
            succeed = True
            _validate_setting_calls = []

            def _validate_setting(self, library, setting, validator):
                self._validate_setting_calls.append((library, setting, validator))
                if self.succeed:
                    return "validated %s" % setting['key']
                else:
                    return INVALID_INPUT.detailed("invalid!")

        # Run library_configuration_settings in a situation where all validations succeed.
        controller = MockController(self.manager)
        library = self._default_library
        result = controller.library_configuration_settings(library, validators, settings)

        # No problem detail was returned -- the 'request' can continue.
        eq_(None, result)

        # _validate_setting was called twice...
        [c1, c2] = controller._validate_setting_calls

        # ...once for each item in `settings`. One of the settings was
        # of a type with a known validator, so the validator was
        # passed in.
        eq_((library, settings[0], validator1), c1)
        eq_((library, settings[1], None), c2)

        # Each validated value was written to the database.
        for x in settings:
            setting = library.setting(x['key'])
            eq_("validated %s" % x['key'], setting.value)
            setting.value = None

        # Try again in a situation where there are validation failures.
        controller.succeed = False
        controller._validate_setting_calls = []
        result = controller.library_configuration_settings(
            self._default_library, validators, settings
        )

        # _validate_setting was only called once.
        eq_([(library, settings[0], validator1)],
            controller._validate_setting_calls)

        # When it returned a ProblemDetail, that ProblemDetail
        # was propagated outwards.
        assert isinstance(result, ProblemDetail)
        eq_("invalid!", result.detail)

        # No new values were written to the database.
        for x in settings:
            eq_(None, library.setting(x['key']).value)

    def test__validate_setting(self):
        # Verify the rules for validating different kinds of settings,
        # one simulated setting at a time.

        library = self._default_library
        class MockController(LibrarySettingsController):

            # Mock the functions that pull various values out of the
            # 'current request' or the 'database' so we don't need an
            # actual current request or actual database settings.
            def scalar_setting(self, setting):
                return self.scalar_form_values.get(setting['key'])

            def list_setting(self, setting, json_objects=False):
                value = self.list_form_values.get(setting['key'])
                if json_objects:
                    value = [json.loads(x) for x in value]
                return json.dumps(value)

            def image_setting(self, setting):
                return self.image_form_values.get(setting['key'])

            def current_value(self, setting, _library):
                # While we're here, make sure the right Library
                # object was passed in.
                eq_(_library, library)
                return self.current_values.get(setting['key'])

            # Now insert mock data into the 'form submission' and
            # the 'database'.

            # Simulate list values in a form submission. The geographic values
            # go in as normal strings; the announcements go in as strings that are
            # JSON-encoded data structures.
            announcement_list = [{"content" : "announcement1"}, {"content": "announcement2"}]
            list_form_values = dict(
                geographic_setting=["geographic values"],
                announcement_list=[
                    json.dumps(x) for x in announcement_list
                ],
                language_codes=["English", "fr"],
                list_value=["a list"],
            )

            # Simulate scalar values in a form submission.
            scalar_form_values = dict(
                string_value="a scalar value"
            )

            # Simulate uploaded images in a form submission.
            image_form_values = dict(
                image_setting="some image data"
            )

            # Simulate values present in the database but not present
            # in the form submission.
            current_values = dict(
                value_not_present_in_request = "a database value",
                previously_uploaded_image = "an old image",
            )

        # First test some simple cases: scalar values.
        controller = MockController(self.manager)
        m = controller._validate_setting

        # The incoming request has a value for this setting.
        eq_("a scalar value", m(library, dict(key="string_value")))

        # But not for this setting: we end up going to the database
        # instead.
        eq_("a database value", m(library, dict(key="value_not_present_in_request")))

        # And not for this setting either: there is no database value,
        # so we have to use the default associated with the setting configuration.
        eq_("a default value", m(library, dict(key="some_other_value",
                                               default="a default value")) )

        # An uploaded image is (from the perspective of this method) also simple.

        # Here, a new image was uploaded.
        eq_("some image data", m(library, dict(key="image_setting", type="image")))

        # Here, no image was uploaded so we use the currently stored database value.
        eq_("an old image", m(library, dict(key="previously_uploaded_image", type="image")))

        # There are some lists which are more complex, but a normal list is
        # simple: the return value is the JSON-encoded list.
        eq_(json.dumps(["a list"]), m(library, dict(key="list_value", type="list")))

        # Now let's look at the more complex lists.

        # A list of language codes.
        eq_(
            json.dumps(["eng", "fre"]),
            m(library, dict(key="language_codes", format="language-code", type="list"))
        )

        # A list of geographic places
        class MockGeographicValidator(object):
            value = "validated value"
            def validate_geographic_areas(self, value, _db):
                self.called_with = (value, _db)
                return self.value
        validator = MockGeographicValidator()

        # The validator was consulted and its response was used as the
        # value.
        eq_(
            'validated value',
            m(library, dict(key="geographic_setting", format="geographic"), validator)
        )
        eq_((json.dumps(["geographic values"]), self._db), validator.called_with)

        # Just to be explicit, let's also test the case where the 'response' sent from the
        # validator is a ProblemDetail.
        validator.value = INVALID_INPUT
        eq_(
            INVALID_INPUT,
            m(library, dict(key="geographic_setting", format="geographic"), validator)
        )

        # A list of announcements.
        class MockAnnouncementValidator(object):
            value = "validated value"
            def validate(self, value):
                self.called_with = value
                return self.value
        validator = MockAnnouncementValidator()

        eq_(
            'validated value',
            m(library, dict(key="announcement_list", format="announcements"), validator)
        )
        eq_(json.dumps(controller.announcement_list), validator.called_with)
