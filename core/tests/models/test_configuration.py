# encoding: utf-8
from enum import Enum

import pytest
import sqlalchemy
from flask_babel import lazy_gettext as _
from mock import MagicMock, create_autospec
from parameterized import parameterized
from sqlalchemy.exc import IntegrityError

from ...config import CannotLoadConfiguration, Configuration
from ...model import create, get_one
from ...model.collection import Collection
from ...model.configuration import (
    ConfigurationAttribute,
    ConfigurationAttributeType,
    ConfigurationGrouping,
    ConfigurationMetadata,
    ConfigurationOption,
    ConfigurationSetting,
    ConfigurationStorage,
    ExternalIntegration,
    ExternalIntegrationLink,
    HasExternalIntegration,
)
from ...model.datasource import DataSource
from ...testing import DatabaseTest


class TestConfigurationSetting(DatabaseTest):

    def test_is_secret(self):
        """Some configuration settings are considered secrets,
        and some are not.
        """
        m = ConfigurationSetting._is_secret
        assert True == m('secret')
        assert True == m('password')
        assert True == m('its_a_secret_to_everybody')
        assert True == m('the_password')
        assert True == m('password_for_the_account')
        assert False == m('public_information')

        assert (True ==
            ConfigurationSetting.sitewide(self._db, "secret_key").is_secret)
        assert (False ==
            ConfigurationSetting.sitewide(self._db, "public_key").is_secret)

    def test_value_or_default(self):
        integration, ignore = create(
            self._db, ExternalIntegration, goal=self._str, protocol=self._str
        )
        setting = integration.setting("key")
        assert None == setting.value

        # If the setting has no value, value_or_default sets the value to
        # the default, and returns the default.
        assert "default value" == setting.value_or_default("default value")
        assert "default value" == setting.value

        # Once the value is set, value_or_default returns the value.
        assert "default value" == setting.value_or_default("new default")

        # If the setting has any value at all, even the empty string,
        # it's returned instead of the default.
        setting.value = ""
        assert "" == setting.value_or_default("default")

    def test_value_inheritance(self):

        key = "SomeKey"

        # Here's a sitewide configuration setting.
        sitewide_conf = ConfigurationSetting.sitewide(self._db, key)

        # Its value is not set.
        assert None == sitewide_conf.value

        # Set it.
        sitewide_conf.value = "Sitewide value"
        assert "Sitewide value" == sitewide_conf.value

        # Here's an integration, let's say the SIP2 authentication mechanism
        sip, ignore = create(
            self._db, ExternalIntegration,
            goal=ExternalIntegration.PATRON_AUTH_GOAL, protocol="SIP2"
        )

        # It happens to a ConfigurationSetting for the same key used
        # in the sitewide configuration.
        sip_conf = ConfigurationSetting.for_externalintegration(key, sip)

        # But because the meaning of a configuration key differ so
        # widely across integrations, the SIP2 integration does not
        # inherit the sitewide value for the key.
        assert None == sip_conf.value
        sip_conf.value = "SIP2 value"

        # Here's a library which has a ConfigurationSetting for the same
        # key used in the sitewide configuration.
        library = self._default_library
        library_conf = ConfigurationSetting.for_library(key, library)

        # Since all libraries use a given ConfigurationSetting to mean
        # the same thing, a library _does_ inherit the sitewide value
        # for a configuration setting.
        assert "Sitewide value" == library_conf.value

        # Change the site-wide configuration, and the default also changes.
        sitewide_conf.value = "New site-wide value"
        assert "New site-wide value" == library_conf.value

        # The per-library value takes precedence over the site-wide
        # value.
        library_conf.value = "Per-library value"
        assert "Per-library value" == library_conf.value

        # Now let's consider a setting like the patron identifier
        # prefix.  This is set on the combination of a library and a
        # SIP2 integration.
        key = "patron_identifier_prefix"
        library_patron_prefix_conf = ConfigurationSetting.for_library_and_externalintegration(
            self._db, key, library, sip
        )
        assert None == library_patron_prefix_conf.value

        # If the SIP2 integration has a value set for this
        # ConfigurationSetting, that value is inherited for every
        # individual library that uses the integration.
        generic_patron_prefix_conf = ConfigurationSetting.for_externalintegration(
            key, sip
        )
        assert None == generic_patron_prefix_conf.value
        generic_patron_prefix_conf.value = "Integration-specific value"
        assert "Integration-specific value" == library_patron_prefix_conf.value

        # Change the value on the integration, and the default changes
        # for each individual library.
        generic_patron_prefix_conf.value = "New integration-specific value"
        assert "New integration-specific value" == library_patron_prefix_conf.value

        # The library+integration setting takes precedence over the
        # integration setting.
        library_patron_prefix_conf.value = "Library-specific value"
        assert "Library-specific value" == library_patron_prefix_conf.value

    def test_duplicate(self):
        """You can't have two ConfigurationSettings for the same key,
        library, and external integration.

        (test_relationships shows that you can have two settings for the same
        key as long as library or integration is different.)
        """
        key = self._str
        integration, ignore = create(
            self._db, ExternalIntegration, goal=self._str, protocol=self._str
        )
        library = self._default_library
        setting = ConfigurationSetting.for_library_and_externalintegration(
            self._db, key, library, integration
        )
        setting2 = ConfigurationSetting.for_library_and_externalintegration(
            self._db, key, library, integration
        )
        assert setting.id == setting2.id
        pytest.raises(
            IntegrityError,
            create, self._db, ConfigurationSetting,
            key=key,
            library=library, external_integration=integration
        )

    def test_relationships(self):
        integration, ignore = create(
            self._db, ExternalIntegration, goal=self._str, protocol=self._str
        )
        assert [] == integration.settings

        library = self._default_library
        assert [] == library.settings

        # Create four different ConfigurationSettings with the same key.
        cs = ConfigurationSetting
        key = self._str

        for_neither = cs.sitewide(self._db, key)
        assert None == for_neither.library
        assert None == for_neither.external_integration

        for_library = cs.for_library(key, library)
        assert library == for_library.library
        assert None == for_library.external_integration

        for_integration = cs.for_externalintegration(key, integration)
        assert None == for_integration.library
        assert integration == for_integration.external_integration

        for_both = cs.for_library_and_externalintegration(
            self._db, key, library, integration
        )
        assert library == for_both.library
        assert integration == for_both.external_integration

        # We got four distinct objects with the same key.
        objs = [for_neither, for_library, for_integration, for_both]
        assert 4 == len(set(objs))
        for o in objs:
            assert o.key == key

        assert [for_library, for_both] == library.settings
        assert [for_integration, for_both] == integration.settings
        assert library == for_both.library
        assert integration == for_both.external_integration

        # If we delete the integration, all configuration settings
        # associated with it are deleted, even the one that's also
        # associated with the library.
        self._db.delete(integration)
        self._db.commit()
        assert [for_library.id] == [x.id for x in library.settings]

    def test_no_orphan_delete_cascade(self):
        # Disconnecting a ConfigurationSetting from a Library or
        # ExternalIntegration doesn't delete it, because it's fine for
        # a ConfigurationSetting to have no associated Library or
        # ExternalIntegration.

        library = self._default_library
        for_library = ConfigurationSetting.for_library(self._str, library)

        integration = self._external_integration(self._str)
        for_integration = ConfigurationSetting.for_externalintegration(
            self._str, integration
        )

        # Remove library and external_integration.
        for_library.library = None
        for_integration.external_integration = None
        self._db.commit()

        # That was a weird thing to do, but the ConfigurationSettings
        # are still in the database.
        for cs in for_library, for_integration:
            assert (
                cs == get_one(self._db, ConfigurationSetting, id=cs.id))

    @parameterized.expand([
        ('no value', None, None),
        ('stringable value', 1, '1'),
        ('string value', 'snowman', 'snowman'),
        ('bytes value', '☃'.encode("utf8"), '☃'),
    ])
    def test_setter(self, _, set_to, expect):
        # Values are converted into Unicode strings on the way in to
        # the 'value' setter.
        setting = ConfigurationSetting.sitewide(self._db, "setting")
        setting.value = set_to
        assert setting.value == expect

    def test_int_value(self):
        number = ConfigurationSetting.sitewide(self._db, "number")
        assert None == number.int_value

        number.value = "1234"
        assert 1234 == number.int_value

        number.value = "tra la la"
        pytest.raises(ValueError, lambda: number.int_value)

    def test_float_value(self):
        number = ConfigurationSetting.sitewide(self._db, "number")
        assert None == number.int_value

        number.value = "1234.5"
        assert 1234.5 == number.float_value

        number.value = "tra la la"
        pytest.raises(ValueError, lambda: number.float_value)

    def test_json_value(self):
        jsondata = ConfigurationSetting.sitewide(self._db, "json")
        assert None == jsondata.int_value

        jsondata.value = "[1,2]"
        assert [1,2] == jsondata.json_value

        jsondata.value = "tra la la"
        pytest.raises(ValueError, lambda: jsondata.json_value)

    def test_excluded_audio_data_sources(self):
        # Get a handle on the underlying ConfigurationSetting
        setting = ConfigurationSetting.sitewide(
            self._db, Configuration.EXCLUDED_AUDIO_DATA_SOURCES
        )
        m = ConfigurationSetting.excluded_audio_data_sources
        # When no explicit value is set for the ConfigurationSetting,
        # the return value of the method is AUDIO_EXCLUSIONS -- whatever
        # the default is for the current version of the circulation manager.
        assert None == setting.value
        assert (ConfigurationSetting.EXCLUDED_AUDIO_DATA_SOURCES_DEFAULT ==
            m(self._db))
        # When an explicit value for the ConfigurationSetting, is set, that
        # value is interpreted as JSON and returned.
        setting.value = "[]"
        assert [] == m(self._db)

    def test_explain(self):
        """Test that ConfigurationSetting.explain gives information
        about all site-wide configuration settings.
        """
        ConfigurationSetting.sitewide(self._db, "a_secret").value = "1"
        ConfigurationSetting.sitewide(self._db, "nonsecret_setting").value = "2"

        integration = self._external_integration("a protocol", "a goal")

        actual = ConfigurationSetting.explain(self._db, include_secrets=True)
        expect = """Site-wide configuration settings:
---------------------------------
a_secret='1'
nonsecret_setting='2'"""
        assert expect == "\n".join(actual)

        without_secrets = "\n".join(ConfigurationSetting.explain(
            self._db, include_secrets=False
        ))
        assert 'a_secret' not in without_secrets
        assert 'nonsecret_setting' in without_secrets


class TestUniquenessConstraints(DatabaseTest):

    def test_duplicate_sitewide_setting(self):
        # You can't create two sitewide settings with the same key.
        c1 = ConfigurationSetting(key="key", value="value1")
        self._db.add(c1)
        self._db.flush()
        c2 = ConfigurationSetting(key="key", value="value2")
        self._db.add(c2)
        pytest.raises(IntegrityError, self._db.flush)

    def test_duplicate_library_setting(self):
        # A library can't have two settings with the same key.
        c1 = ConfigurationSetting(
            key="key", value="value1", library=self._default_library
        )
        self._db.add(c1)
        self._db.flush()
        c2 = ConfigurationSetting(
            key="key", value="value2", library=self._default_library
        )
        self._db.add(c2)
        pytest.raises(IntegrityError, self._db.flush)

    def test_duplicate_integration_setting(self):
        # An external integration can't have two settings with the
        # same key.
        integration = self._external_integration(self._str)
        c1 = ConfigurationSetting(
            key="key", value="value1", external_integration=integration
        )
        self._db.add(c1)
        self._db.flush()
        c2 = ConfigurationSetting(
            key="key", value="value1", external_integration=integration
        )
        self._db.add(c2)
        pytest.raises(IntegrityError, self._db.flush)

    def test_duplicate_library_integration_setting(self):
        # A library can't configure an external integration two
        # different ways for the same key.
        integration = self._external_integration(self._str)
        c1 = ConfigurationSetting(
            key="key", value="value1", library=self._default_library,
            external_integration=integration
        )
        self._db.add(c1)
        self._db.flush()
        c2 = ConfigurationSetting(
            key="key", value="value1", library=self._default_library,
            external_integration=integration
        )
        self._db.add(c2)
        pytest.raises(IntegrityError, self._db.flush)


class TestExternalIntegrationLink(DatabaseTest):
    def test_collection_mirror_settings(self):
        settings = ExternalIntegrationLink.COLLECTION_MIRROR_SETTINGS

        assert settings[0]["key"] == ExternalIntegrationLink.COVERS_KEY
        assert settings[0]["label"] == "Covers Mirror"
        assert (settings[0]["options"][0]['key'] ==
            ExternalIntegrationLink.NO_MIRROR_INTEGRATION)
        assert (settings[0]["options"][0]['label'] ==
            _("None - Do not mirror cover images"))

        assert settings[1]["key"] == ExternalIntegrationLink.OPEN_ACCESS_BOOKS_KEY
        assert settings[1]["label"] == "Open Access Books Mirror"
        assert (settings[1]["options"][0]['key'] ==
            ExternalIntegrationLink.NO_MIRROR_INTEGRATION)
        assert (settings[1]["options"][0]['label'] ==
            _("None - Do not mirror free books"))

        assert settings[2]["key"] == ExternalIntegrationLink.PROTECTED_ACCESS_BOOKS_KEY
        assert settings[2]["label"] == "Protected Access Books Mirror"
        assert (settings[2]["options"][0]['key'] ==
            ExternalIntegrationLink.NO_MIRROR_INTEGRATION)
        assert (settings[2]["options"][0]['label'] ==
            _("None - Do not mirror self-hosted, commercially licensed books"))
    
    def test_relationships(self):
        # Create a collection with two storage external integrations.
        collection = self._collection(
            name="Collection", protocol=ExternalIntegration.OVERDRIVE,
        )

        storage1 = self._external_integration(
            name="integration1",
            protocol=ExternalIntegration.S3,
        )
        storage2 = self._external_integration(
            name="integration2",
            protocol=ExternalIntegration.S3,
            goal=ExternalIntegration.STORAGE_GOAL,
            username="username", password="password",
        )

        # Two external integration links need to be created to associate
        # the collection's external integration with the two storage
        # external integrations.
        s1_external_integration_link = self._external_integration_link(
            integration=collection.external_integration,
            other_integration=storage1, purpose="covers_mirror"
        )
        s2_external_integration_link = self._external_integration_link(
            integration=collection.external_integration,
            other_integration=storage2, purpose="books_mirror"
        )

        qu = self._db.query(ExternalIntegrationLink
            ).order_by(ExternalIntegrationLink.other_integration_id)
        external_integration_links = qu.all()

        assert len(external_integration_links) == 2
        assert external_integration_links[0].other_integration_id == storage1.id
        assert external_integration_links[1].other_integration_id == storage2.id

        # When a storage integration is deleted, the related external
        # integration link row is deleted, and the relationship with the
        # collection is removed.
        self._db.delete(storage1)

        qu = self._db.query(ExternalIntegrationLink)
        external_integration_links = qu.all()

        assert len(external_integration_links) == 1
        assert external_integration_links[0].other_integration_id == storage2.id

class TestExternalIntegration(DatabaseTest):

    def setup_method(self):
        super(TestExternalIntegration, self).setup_method()
        self.external_integration, ignore = create(
            self._db, ExternalIntegration, goal=self._str, protocol=self._str
        )

    def test_for_library_and_goal(self):
        goal = self.external_integration.goal
        qu = ExternalIntegration.for_library_and_goal(
            self._db, self._default_library, goal
        )

        # This matches nothing because the ExternalIntegration is not
        # associated with the Library.
        assert [] == qu.all()
        get_one = ExternalIntegration.one_for_library_and_goal
        assert None == get_one(self._db, self._default_library, goal)

        # Associate the library with the ExternalIntegration and
        # the query starts matching it. one_for_library_and_goal
        # also starts returning it.
        self.external_integration.libraries.append(self._default_library)
        assert [self.external_integration] == qu.all()
        assert (self.external_integration ==
            get_one(self._db, self._default_library, goal))

        # Create another, similar ExternalIntegration. By itself, this
        # has no effect.
        integration2, ignore = create(
            self._db, ExternalIntegration, goal=goal, protocol=self._str
        )
        assert [self.external_integration] == qu.all()
        assert (self.external_integration ==
            get_one(self._db, self._default_library, goal))

        # Associate that ExternalIntegration with the library, and
        # the query starts picking it up, and one_for_library_and_goal
        # starts raising an exception.
        integration2.libraries.append(self._default_library)
        assert set([self.external_integration, integration2]) == set(qu.all())
        with pytest.raises(CannotLoadConfiguration) as excinfo:
            get_one(self._db, self._default_library, goal)
        assert "Library {} defines multiple integrations with goal {}".format(self._default_library.name, goal) \
            in str(excinfo.value)
    
    def test_for_collection_and_purpose(self):
        wrong_purpose = "isbn"
        collection = self._collection()

        with pytest.raises(CannotLoadConfiguration) as excinfo:
            ExternalIntegration.for_collection_and_purpose(self._db, collection, wrong_purpose)
        assert "No storage integration for collection '%s' and purpose '%s' is configured" \
            % (collection.name, wrong_purpose) \
            in str(excinfo.value)

        external_integration = self._external_integration("some protocol")
        collection.external_integration_id = external_integration.id
        purpose = "covers_mirror"
        self._external_integration_link(
            integration=external_integration, purpose=purpose
        )

        integration = ExternalIntegration.for_collection_and_purpose(
            self._db, collection=collection, purpose=purpose
        )
        assert isinstance(integration, ExternalIntegration)

    def test_with_setting_value(self):
        def results():
            # Run the query and return all results.
            return ExternalIntegration.with_setting_value(
                self._db, "protocol", "goal", "key", "value"
            ).all()

        # We start off with no results.
        assert [] == results()

        # This ExternalIntegration will not match the result,
        # even though protocol and goal match, because it
        # doesn't have the 'key' ConfigurationSetting set.
        integration = self._external_integration("protocol", "goal")
        assert [] == results()

        # Now 'key' is set, but set to the wrong value.
        setting = integration.setting("key")
        setting.value = "wrong"
        assert [] == results()

        # Now it's set to the right value, so we get a result.
        setting.value = "value"
        assert [integration] == results()

        # Create another, identical integration.
        integration2, is_new = create(
            self._db, ExternalIntegration, protocol="protocol", goal="goal"
        )
        assert integration2 != integration
        integration2.setting("key").value = "value"

        # Both integrations show up.
        assert set([integration, integration2]) == set(results())

        # If the integration's goal doesn't match, it doesn't show up.
        integration2.goal = "wrong"
        assert [integration] == results()

        # If the integration's protocol doesn't match, it doesn't show up.
        integration.protocol = "wrong"
        assert [] == results()

    def test_data_source(self):
        # For most collections, the protocol determines the
        # data source.
        collection = self._collection(protocol=ExternalIntegration.OVERDRIVE)
        assert DataSource.OVERDRIVE == collection.data_source.name

        # For OPDS Import collections, data source is a setting which
        # might not be present.
        assert None == self._default_collection.data_source

        # data source will be automatically created if necessary.
        self._default_collection.external_integration.setting(
            Collection.DATA_SOURCE_NAME_SETTING
        ).value = "New Data Source"
        assert "New Data Source" == self._default_collection.data_source.name

    def test_set_key_value_pair(self):
        """Test the ability to associate extra key-value pairs with
        an ExternalIntegration.
        """
        assert [] == self.external_integration.settings

        setting = self.external_integration.set_setting("website_id", "id1")
        assert "website_id" == setting.key
        assert "id1" == setting.value

        # Calling set() again updates the key-value pair.
        assert [setting.id] == [x.id for x in self.external_integration.settings]
        setting2 = self.external_integration.set_setting("website_id", "id2")
        assert setting.id == setting2.id
        assert "id2" == setting2.value

        assert setting2 == self.external_integration.setting("website_id")

    def test_explain(self):
        integration = self._external_integration(
            "protocol", "goal"
        )
        integration.name = "The Integration"
        integration.url = "http://url/"
        integration.username = "someuser"
        integration.password = "somepass"
        integration.setting("somesetting").value = "somevalue"

        # Two different libraries have slightly different
        # configurations for this integration.
        self._default_library.name = "First Library"
        self._default_library.integrations.append(integration)
        ConfigurationSetting.for_library_and_externalintegration(
            self._db, "library-specific", self._default_library, integration
        ).value = "value1"

        library2 = self._library()
        library2.name = "Second Library"
        library2.integrations.append(integration)
        ConfigurationSetting.for_library_and_externalintegration(
            self._db, "library-specific", library2, integration
        ).value = "value2"

        # If we decline to pass in a library, we get information about how
        # each library in the system configures this integration.

        expect = """ID: %s
Name: The Integration
Protocol/Goal: protocol/goal
library-specific='value1' (applies only to First Library)
library-specific='value2' (applies only to Second Library)
somesetting='somevalue'
url='http://url/'
username='someuser'""" % integration.id
        actual = integration.explain()
        assert expect == "\n".join(actual)

        # If we pass in a library, we only get information about
        # how that specific library configures the integration.
        for_library_2 = "\n".join(integration.explain(library=library2))
        assert "applies only to First Library" not in for_library_2
        assert "applies only to Second Library" in for_library_2

        # If we pass in True for include_secrets, we see the passwords.
        with_secrets = integration.explain(include_secrets=True)
        assert "password='somepass'" in with_secrets

    def test_custom_accept_header(self):
        integration = self._external_integration("protocol", "goal")
        # Must be empty if not set
        assert integration.custom_accept_header == None

        # Must be the same value if set
        integration.custom_accept_header = "custom header"
        assert integration.custom_accept_header == "custom header"

SETTING1_KEY = 'setting1'
SETTING1_LABEL = 'Setting 1\'s label'
SETTING1_DESCRIPTION = 'Setting 1\'s description'
SETTING1_TYPE = ConfigurationAttributeType.TEXT
SETTING1_REQUIRED = False
SETTING1_DEFAULT = '12345'
SETTING1_CATEGORY = 'Settings'

SETTING2_KEY = 'setting2'
SETTING2_LABEL = 'Setting 2\'s label'
SETTING2_DESCRIPTION = 'Setting 2\'s description'
SETTING2_TYPE = ConfigurationAttributeType.SELECT
SETTING2_REQUIRED = False
SETTING2_DEFAULT = 'value1'
SETTING2_OPTIONS = [
    ConfigurationOption('key1', 'value1'),
    ConfigurationOption('key2', 'value2'),
    ConfigurationOption('key3', 'value3')
]
SETTING2_CATEGORY = 'Settings'


class TestConfiguration(ConfigurationGrouping):
    setting1 = ConfigurationMetadata(
        key='setting1',
        label=SETTING1_LABEL,
        description=SETTING1_DESCRIPTION,
        type=SETTING1_TYPE,
        required=SETTING1_REQUIRED,
        default=SETTING1_DEFAULT,
        category=SETTING1_CATEGORY
    )

    setting2 = ConfigurationMetadata(
        key='setting2',
        label=SETTING2_LABEL,
        description=SETTING2_DESCRIPTION,
        type=SETTING2_TYPE,
        required=SETTING2_REQUIRED,
        default=SETTING2_DEFAULT,
        options=SETTING2_OPTIONS,
        category=SETTING2_CATEGORY
    )


class ConfigurationWithBooleanProperty(ConfigurationGrouping):
    boolean_setting = ConfigurationMetadata(
        key='boolean_setting',
        label='Boolean Setting',
        description='Boolean Setting',
        type=ConfigurationAttributeType.SELECT,
        required=True,
        default='true',
        options=[
            ConfigurationOption('true', 'True'),
            ConfigurationOption('false', 'False')
        ]
    )


class TestConfiguration2(ConfigurationGrouping):
    setting1 = ConfigurationMetadata(
        key='setting1',
        label=SETTING1_LABEL,
        description=SETTING1_DESCRIPTION,
        type=SETTING1_TYPE,
        required=SETTING1_REQUIRED,
        default=SETTING1_DEFAULT,
        category=SETTING1_CATEGORY,
        index=1
    )

    setting2 = ConfigurationMetadata(
        key='setting2',
        label=SETTING2_LABEL,
        description=SETTING2_DESCRIPTION,
        type=SETTING2_TYPE,
        required=SETTING2_REQUIRED,
        default=SETTING2_DEFAULT,
        options=SETTING2_OPTIONS,
        category=SETTING2_CATEGORY,
        index=0
    )


class TestConfigurationOption(object):
    def test_to_settings(self):
        # Arrange
        option = ConfigurationOption('key1', 'value1')
        expected_result = {
            'key': 'key1',
            'label': 'value1'
        }

        # Act
        result = option.to_settings()

        # Assert
        assert result == expected_result

    def test_from_enum(self):
        # Arrange
        class TestEnum(Enum):
            LABEL1 = 'KEY1'
            LABEL2 = 'KEY2'
        expected_result = [
            ConfigurationOption('KEY1', 'LABEL1'),
            ConfigurationOption('KEY2', 'LABEL2')
        ]

        # Act
        result = ConfigurationOption.from_enum(TestEnum)

        # Assert
        assert result == expected_result


class TestConfigurationGrouping(object):
    @parameterized.expand([
        ('setting1', 'setting1', 12345),
        ('setting2', 'setting2', '12345')
    ])
    def test_getters(self, _, setting_name, expected_value):
        # Arrange
        configuration_storage = create_autospec(spec=ConfigurationStorage)
        configuration_storage.load = MagicMock(return_value=expected_value)
        db = create_autospec(spec=sqlalchemy.orm.session.Session)
        configuration = TestConfiguration(configuration_storage, db)

        # Act
        setting_value = getattr(configuration, setting_name)

        # Assert
        assert setting_value == expected_value
        configuration_storage.load.assert_called_once_with(db, setting_name)

    @parameterized.expand([
        ('setting1', 'setting1', 12345),
        ('setting2', 'setting2', '12345')
    ])
    def test_setters(self, _, setting_name, expected_value):
        # Arrange
        configuration_storage = create_autospec(spec=ConfigurationStorage)
        configuration_storage.save = MagicMock(return_value=expected_value)
        db = create_autospec(spec=sqlalchemy.orm.session.Session)
        configuration = TestConfiguration(configuration_storage, db)

        # Act
        setattr(configuration, setting_name, expected_value)

        # Assert
        configuration_storage.save.assert_called_once_with(db, setting_name, expected_value)

    def test_to_settings_considers_default_indices(self):
        # Act
        settings = TestConfiguration.to_settings()

        # Assert
        assert len(settings) == 2

        assert settings[0][ConfigurationAttribute.KEY.value] == SETTING1_KEY
        assert settings[0][ConfigurationAttribute.LABEL.value] == SETTING1_LABEL
        assert settings[0][ConfigurationAttribute.DESCRIPTION.value] == SETTING1_DESCRIPTION
        assert settings[0][ConfigurationAttribute.TYPE.value] == None
        assert settings[0][ConfigurationAttribute.REQUIRED.value] == SETTING1_REQUIRED
        assert settings[0][ConfigurationAttribute.DEFAULT.value] == SETTING1_DEFAULT
        assert settings[0][ConfigurationAttribute.CATEGORY.value] == SETTING1_CATEGORY

        assert settings[1][ConfigurationAttribute.KEY.value] == SETTING2_KEY
        assert settings[1][ConfigurationAttribute.LABEL.value] == SETTING2_LABEL
        assert settings[1][ConfigurationAttribute.DESCRIPTION.value] == SETTING2_DESCRIPTION
        assert settings[1][ConfigurationAttribute.TYPE.value] == SETTING2_TYPE.value
        assert settings[1][ConfigurationAttribute.REQUIRED.value] == SETTING2_REQUIRED
        assert settings[1][ConfigurationAttribute.DEFAULT.value] == SETTING2_DEFAULT
        assert settings[1][ConfigurationAttribute.OPTIONS.value] == [option.to_settings() for option in SETTING2_OPTIONS]
        assert settings[1][ConfigurationAttribute.CATEGORY.value] == SETTING2_CATEGORY

    def test_to_settings_considers_explicit_indices(self):
        # Act
        settings = TestConfiguration2.to_settings()

        # Assert
        assert len(settings) == 2

        assert settings[0][ConfigurationAttribute.KEY.value] == SETTING2_KEY
        assert settings[0][ConfigurationAttribute.LABEL.value] == SETTING2_LABEL
        assert settings[0][ConfigurationAttribute.DESCRIPTION.value] == SETTING2_DESCRIPTION
        assert settings[0][ConfigurationAttribute.TYPE.value] == SETTING2_TYPE.value
        assert settings[0][ConfigurationAttribute.REQUIRED.value] == SETTING2_REQUIRED
        assert settings[0][ConfigurationAttribute.DEFAULT.value] == SETTING2_DEFAULT
        assert settings[0][ConfigurationAttribute.OPTIONS.value] == [option.to_settings() for option in SETTING2_OPTIONS]
        assert settings[0][ConfigurationAttribute.CATEGORY.value] == SETTING2_CATEGORY

        assert settings[1][ConfigurationAttribute.KEY.value] == SETTING1_KEY
        assert settings[1][ConfigurationAttribute.LABEL.value] == SETTING1_LABEL
        assert settings[1][ConfigurationAttribute.DESCRIPTION.value] == SETTING1_DESCRIPTION
        assert settings[1][ConfigurationAttribute.TYPE.value] == None
        assert settings[1][ConfigurationAttribute.REQUIRED.value] == SETTING1_REQUIRED
        assert settings[1][ConfigurationAttribute.DEFAULT.value] == SETTING1_DEFAULT
        assert settings[1][ConfigurationAttribute.CATEGORY.value] == SETTING1_CATEGORY


class TestBooleanConfigurationMetadata(DatabaseTest):
    @parameterized.expand([
        ('true', 'true', True),
        ('t', 't', True),
        ('yes', 'yes', True),
        ('y', 'y', True),
        (1, 1, False),
        ('false', 'false', False),
    ])
    def test_configuration_metadata_correctly_recognize_bool_values(self, _, value, expected_result):
        """Ensure that ConfigurationMetadata.to_bool correctly translates different values into boolean (True/False).

        :param _: Name of the test case
        :type _: str

        :param value: Configuration setting's value
        :type value: Any

        :param expected_result: Expected boolean result
        :type expected_result: bool
        """
        # Arrange
        external_integration = self._external_integration('test')

        external_integration_association = create_autospec(spec=HasExternalIntegration)
        external_integration_association.external_integration = MagicMock(return_value=external_integration)

        configuration_storage = ConfigurationStorage(external_integration_association)

        configuration = ConfigurationWithBooleanProperty(configuration_storage, self._db)

        # We set a new value using ConfigurationMetadata.__set__
        configuration.boolean_setting = value

        # Act
        # We read the existing value using ConfigurationMetadata.__get__
        result = ConfigurationMetadata.to_bool(configuration.boolean_setting)

        # Assert
        assert expected_result == result
