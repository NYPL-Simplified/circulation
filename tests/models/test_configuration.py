# encoding: utf-8
from enum import Enum

import pytest
import sqlalchemy
from flask_babel import lazy_gettext as _
from mock import MagicMock, create_autospec
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


class TestConfigurationSetting:

    @pytest.mark.parametrize(
        'secret,expected',
        [
            ('secret', True),
            ('password', True),
            ('its_a_secret_to_everybody', True),
            ('the_password', True),
            ('password_for_the_account', True),
            ('public_information', False)
        ]
    )
    def test_is_secret(self, secret, expected):
        """
        GIVEN: A ConfigurationSetting
        WHEN:  Checking if the setting is secret
        THEN:  The secret settings are treated as secret
        """
        assert ConfigurationSetting._is_secret(secret) is expected

    @pytest.mark.parametrize(
        'setting,expected',
        [
            ('secret_key', True),
            ('public_key', False)
        ]
    )
    def test_sitewide_is_secret(self, db_session, setting, expected):
        """
        GIVEN: A sitewide ConfigurationSetting
        WHEN:  Checking if the setting is secret
        THEN:  The secret settings are treated as secret
        """
        assert ConfigurationSetting.sitewide(db_session, setting).is_secret is expected

    def test_value_or_default(self, db_session):
        """
        GIVEN: An ExternalIntegration setting
        WHEN:  Fetching the setting value through value_or_default()
        THEN:  The setting value is returned or
               set if the value is None then returned
        """
        integration, _ = create(
            db_session, ExternalIntegration, goal="goal", protocol="protocol"
        )
        setting = integration.setting("key")
        assert setting.value is None

        # If the setting has no value, value_or_default sets the value to
        # the default, and returns the default.
        assert setting.value_or_default("default value") == "default value"
        assert setting.value == "default value"

        # Once the value is set, value_or_default returns the value.
        assert setting.value_or_default("new default") == "default value"

        # If the setting has any value at all, even the empty string,
        # it's returned instead of the default.
        setting.value = ""
        assert setting.value_or_default("default") == ""

    def test_value_inheritance(self, db_session, create_library):
        """
        GIVEN: A sitewide ConfigurationSetting,
               a ConfigurationSetting for an ExternalIntegration,
               a ConfigurationSetting for a Library, and
               a ConfigurationSetting for an ExternalIntegration and Library
        WHEN:  Updating a setting's value
        THEN:  Updated setting is reflected either through the ConfigurationSetting,
               or inherited from some other ConfigurationSetting through a Library,
               or Library and ExternalIntegration
        """
        key = "SomeKey"

        # Here's a sitewide configuration setting.
        sitewide_conf = ConfigurationSetting.sitewide(db_session, key)

        # Its value is not set.
        assert sitewide_conf.value is None

        # Set it.
        sitewide_conf.value = "Sitewide value"
        assert sitewide_conf.value == "Sitewide value"

        # Here's an integration, let's say the SIP2 authentication mechanism
        sip, _ = create(
            db_session, ExternalIntegration,
            goal=ExternalIntegration.PATRON_AUTH_GOAL, protocol="SIP2"
        )

        # It happens to a ConfigurationSetting for the same key used
        # in the sitewide configuration.
        sip_conf = ConfigurationSetting.for_externalintegration(key, sip)

        # But because the meaning of a configuration key differ so
        # widely across integrations, the SIP2 integration does not
        # inherit the sitewide value for the key.
        assert sip_conf.value is None
        sip_conf.value = "SIP2 value"

        # Here's a library which has a ConfigurationSetting for the same
        # key used in the sitewide configuration.
        library = create_library(db_session)
        library_conf = ConfigurationSetting.for_library(key, library)

        # Since all libraries use a given ConfigurationSetting to mean
        # the same thing, a library _does_ inherit the sitewide value
        # for a configuration setting.
        assert library_conf.value == "Sitewide value"

        # Change the site-wide configuration, and the default also changes.
        sitewide_conf.value = "New site-wide value"
        assert library_conf.value == "New site-wide value"

        # The per-library value takes precedence over the site-wide
        # value.
        library_conf.value = "Per-library value"
        assert library_conf.value == "Per-library value"

        # Now let's consider a setting like the patron identifier
        # prefix. This is set on the combination of a library and a
        # SIP2 integration.
        key = "patron_identifier_prefix"
        library_patron_prefix_conf = ConfigurationSetting.for_library_and_externalintegration(
            db_session, key, library, sip
        )
        assert library_patron_prefix_conf.value is None

        # If the SIP2 integration has a value set for this
        # ConfigurationSetting, that value is inherited for every
        # individual library that uses the integration.
        generic_patron_prefix_conf = ConfigurationSetting.for_externalintegration(
            key, sip
        )
        assert generic_patron_prefix_conf.value is None
        generic_patron_prefix_conf.value = "Integration-specific value"
        assert library_patron_prefix_conf.value == "Integration-specific value"

        # Change the value on the integration, and the default changes
        # for each individual library.
        generic_patron_prefix_conf.value = "New integration-specific value"
        assert library_patron_prefix_conf.value == "New integration-specific value"

        # The library+integration setting takes precedence over the
        # integration setting.
        library_patron_prefix_conf.value = "Library-specific value"
        assert library_patron_prefix_conf.value == "Library-specific value"

    def test_duplicate(self, db_session, create_library):
        """
        GIVEN: Two ConfigurationSettings for the same key,
               a Library, and an ExternalIntegration
        WHEN:  Creating a duplicate ConfigurationSetting for the Library and ExternalIntegration
               with the same key
        THEN:  An IntegrityError is raised
        """
        key = "SomeKey"
        integration, _ = create(
            db_session, ExternalIntegration, goal="goal", protocol="protocol"
        )
        library = create_library(db_session)
        setting = ConfigurationSetting.for_library_and_externalintegration(
            db_session, key, library, integration
        )
        setting2 = ConfigurationSetting.for_library_and_externalintegration(
            db_session, key, library, integration
        )
        assert setting.id == setting2.id
        pytest.raises(
            IntegrityError,
            create, db_session, ConfigurationSetting,
            key=key,
            library=library, external_integration=integration
        )

    def test_relationships(self, db_session, create_library):
        """
        GIVEN: 4 ConfigurationSettings with the same key for
               Sitewide, Library, ExternalIntegration, Library and ExternalIntegration
        WHEN:  Deleting the ExternalIntegration
        THEN:  All ConfigurationSettings assocaiated with the ExternalIntegration are deleted
        """
        integration, _ = create(
            db_session, ExternalIntegration, goal="goal", protocol="protocol"
        )
        assert [] == integration.settings

        library = create_library(db_session)
        assert [] == library.settings

        # Create four different ConfigurationSettings with the same key.
        cs = ConfigurationSetting
        key = "SomeKey"

        for_neither = cs.sitewide(db_session, key)
        assert for_neither.library is None
        assert for_neither.external_integration is None

        for_library = cs.for_library(key, library)
        assert for_library.library == library
        assert for_library.external_integration is None

        for_integration = cs.for_externalintegration(key, integration)
        assert for_integration.library is None
        assert for_integration.external_integration == integration

        for_both = cs.for_library_and_externalintegration(
            db_session, key, library, integration
        )
        assert for_both.library == library
        assert for_both.external_integration == integration

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
        db_session.delete(integration)
        db_session.commit()
        assert [for_library.id] == [setting.id for setting in library.settings]

    def test_no_orphan_delete_cascade(self, db_session, create_externalintegration, create_library):
        """
        GIVEN: A ConfigurationSetting for a Library and
               a ConfigurationSetting for an ExternalIntegration
        WHEN:  Disconnecting the Library and ExternalIntegration from their ConfigurationSetting
        THEN:  ConfigurationSettings are still found in the database
               because it's fine to have no associated Library or ExternalIntegration
        """
        library = create_library(db_session)
        for_library = ConfigurationSetting.for_library("one", library)

        integration = create_externalintegration(db_session, "two")
        for_integration = ConfigurationSetting.for_externalintegration("three", integration)

        # Remove library and external_integration.
        for_library.library = None
        for_integration.external_integration = None

        # That was a weird thing to do, but the ConfigurationSettings
        # are still in the database.
        for cs in for_library, for_integration:
            assert cs == get_one(db_session, ConfigurationSetting, id=cs.id)

    @pytest.mark.parametrize(
        'set_to,expect',
        [
            (None, None),
            (1, '1'),
            ('snowman', 'snowman'),
            ('☃'.encode("utf8"), '☃')
        ],
        ids=[
            'no value',
            'stringable value',
            'string value',
            'bytes value'
        ]
    )
    def test_setter(self, db_session, set_to, expect):
        """
        GIVEN: A value
        WHEN:  Setting the value for a sitewide ConfigurationSetting
        THEN:  The setting's value is correctly set
        """
        # Values are converted into Unicode strings on the way in to
        # the 'value' setter.
        setting = ConfigurationSetting.sitewide(db_session, "setting")
        setting.value = set_to
        assert setting.value == expect

    def test_int_value(self, db_session):
        """
        GIVEN: A sitewide ConfigurationSetting
        WHEN:  Getting the value as an integer
        THEN:  Integer value is returned if possible
               or None if there is no value
               otherwise a ValueError is raised
        """
        number = ConfigurationSetting.sitewide(db_session, "number")
        assert number.int_value is None

        number.value = "1234"
        assert number.int_value == 1234

        number.value = "tra la la"
        pytest.raises(ValueError, lambda: number.int_value)

    def test_float_value(self, db_session):
        """
        GIVEN: A sitewide ConfigurationSetting
        WHEN:  Getting the value as a float
        THEN:  Float value is returned if possible
               or None if there is no value
               otherwise a ValueError is raised
        """
        number = ConfigurationSetting.sitewide(db_session, "number")
        assert number.int_value is None

        number.value = "1234.5"
        assert number.float_value == 1234.5

        number.value = "tra la la"
        pytest.raises(ValueError, lambda: number.float_value)

    def test_json_value(self, db_session):
        """
        GIVEN: A sitewide ConfigurationSetting
        WHEN:  Getting the value as JSON
        THEN:  JSON is returned if possible
               or None if there is no value
               otherwise a ValueError is raised
        """
        jsondata = ConfigurationSetting.sitewide(db_session, "json")
        assert jsondata.int_value is None

        jsondata.value = "[1,2]"
        assert jsondata.json_value == [1, 2]

        jsondata.value = "tra la la"
        pytest.raises(ValueError, lambda: jsondata.json_value)

    def test_excluded_audio_data_sources(self, db_session):
        """
        GIVEN: A sitewide ConfigurationSetting for excluding audio data sources
        WHEN:  Listing the data sources whose audiobooks should not be published in feeds
        THEN:  The correct data sources are returned
        """
        # Get a handle on the underlying ConfigurationSetting
        setting = ConfigurationSetting.sitewide(
            db_session, Configuration.EXCLUDED_AUDIO_DATA_SOURCES
        )
        m = ConfigurationSetting.excluded_audio_data_sources
        # When no explicit value is set for the ConfigurationSetting,
        # the return value of the method is AUDIO_EXCLUSIONS -- whatever
        # the default is for the current version of the circulation manager.
        assert setting.value is None
        assert m(db_session) == ConfigurationSetting.EXCLUDED_AUDIO_DATA_SOURCES_DEFAULT

        # When an explicit value for the ConfigurationSetting, is set, that
        # value is interpreted as JSON and returned.
        setting.value = "[]"
        assert m(db_session) == []

    def test_explain(self, db_session, create_externalintegration):
        """
        GIVEN: ConfigurationSettings
        WHEN:  Explaining the ConfigurationSetting
        THEN:  A series human-readable strings are returned
        """
        """Test that ConfigurationSetting.explain gives information
        about all site-wide configuration settings.
        """
        ConfigurationSetting.sitewide(db_session, "a_secret").value = "1"
        ConfigurationSetting.sitewide(db_session, "nonsecret_setting").value = "2"

        create_externalintegration(db_session, "a protocol", "a goal")

        actual = ConfigurationSetting.explain(db_session, include_secrets=True)
        expect = """Site-wide configuration settings:
---------------------------------
a_secret='1'
nonsecret_setting='2'"""
        assert expect == "\n".join(actual)

        without_secrets = "\n".join(ConfigurationSetting.explain(
            db_session, include_secrets=False
        ))
        assert 'a_secret' not in without_secrets
        assert 'nonsecret_setting' in without_secrets


class TestUniquenessConstraints:

    def test_duplicate_sitewide_setting(self, db_session):
        """
        GIVEN: A ConfigurationSetting
        WHEN:  Creating a ConfigurationSetting with a duplicate key
        THEN:  An IntegrityError is raised
        """
        # You can't create two sitewide settings with the same key.
        c1 = ConfigurationSetting(key="key", value="value1")
        db_session.add(c1)
        db_session.flush()
        c2 = ConfigurationSetting(key="key", value="value2")
        db_session.add(c2)
        pytest.raises(IntegrityError, db_session.flush)

    def test_duplicate_library_setting(self, db_session, create_library):
        """
        GIVEN: A Library and ConfigurationSetting
        WHEN:  Creating a ConfigurationSetting for the Library with a duplicate key
        THEN:  An IntegrityError is raised
        """
        # A library can't have two settings with the same key.
        library = create_library(db_session)

        c1 = ConfigurationSetting(key="key", value="value1", library=library)
        db_session.add(c1)
        db_session.flush()

        c2 = ConfigurationSetting(key="key", value="value2", library=library)
        db_session.add(c2)
        pytest.raises(IntegrityError, db_session.flush)

    def test_duplicate_integration_setting(self, db_session, create_externalintegration):
        """
        GIVEN: An ExternalIntegration and ConfigurationSetting
        WHEN:  Creating a ConfigurationSetting for the ExternalIntegratino with a duplicate key
        THEN:  An IntegrityError is raised
        """
        integration = create_externalintegration(db_session, "protocol")
        c1 = ConfigurationSetting(key="key", value="value1", external_integration=integration)
        db_session.add(c1)
        db_session.flush()

        c2 = ConfigurationSetting(key="key", value="value1", external_integration=integration)
        db_session.add(c2)
        pytest.raises(IntegrityError, db_session.flush)

    def test_duplicate_library_integration_setting(self, db_session, create_externalintegration, create_library):
        """
        GIVEN: A Library, an ExternalIntegration, and a ConfigurationSetting
        WHEN:  Creating a ConfigurationSetting for a Library and ExternalIntegration with a duplicate key
        THEN:  An IntegrityError is raised
        """
        # A library can't configure an external integration two
        # different ways for the same key.
        integration = create_externalintegration(db_session, "protocol")
        library = create_library(db_session)
        c1 = ConfigurationSetting(
            key="key", value="value1", library=library,
            external_integration=integration
        )
        db_session.add(c1)
        db_session.flush()
        c2 = ConfigurationSetting(
            key="key", value="value1", library=library,
            external_integration=integration
        )
        db_session.add(c2)
        pytest.raises(IntegrityError, db_session.flush)


class TestExternalIntegrationLink:

    def test_collection_mirror_settings(self):
        """
        GIVEN: A dictionary of settings
        WHEN:  Checking the settings
        THEN:  The default settings are set
        """
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

    def test_relationships(self, db_session, create_collection,
                           create_externalintegration, create_external_integration_link):
        """
        GIVEN: A collection, two ExternalIntegrations, and two ExternalIntegrationLinks
        WHEN:  Deleting an ExternalIntegration
        THEN:  The related ExternalIntegrationLink is deleted
        """
        # Create a collection with two storage external integrations.
        collection = create_collection(
            db_session, name="Collection", protocol=ExternalIntegration.OVERDRIVE,
        )

        storage1 = create_externalintegration(
            db_session,
            name="integration1",
            protocol=ExternalIntegration.S3,
        )
        storage2 = create_externalintegration(
            db_session,
            name="integration2",
            protocol=ExternalIntegration.S3,
            goal=ExternalIntegration.STORAGE_GOAL,
            username="username", password="password",
        )

        # Two external integration links need to be created to associate
        # the collection's external integration with the two storage
        # external integrations.
        create_external_integration_link(
            db_session,
            integration=collection.external_integration,
            other_integration=storage1, purpose="covers_mirror"
        )
        create_external_integration_link(
            db_session,
            integration=collection.external_integration,
            other_integration=storage2, purpose="books_mirror"
        )

        qu = db_session.query(ExternalIntegrationLink).order_by(ExternalIntegrationLink.other_integration_id)
        external_integration_links = qu.all()

        assert len(external_integration_links) == 2
        assert external_integration_links[0].other_integration_id == storage1.id
        assert external_integration_links[1].other_integration_id == storage2.id

        # When a storage integration is deleted, the related external
        # integration link row is deleted, and the relationship with the
        # collection is removed.
        db_session.delete(storage1)

        qu = db_session.query(ExternalIntegrationLink)
        external_integration_links = qu.all()

        assert len(external_integration_links) == 1
        assert external_integration_links[0].other_integration_id == storage2.id


class TestExternalIntegration:

    def test_for_library_and_goal(self, db_session, create_externalintegration, create_library):
        """
        GIVEN: A Library and two ExternalIntegrations
        WHEN:  Finding ExternalIntegration associated with the Library and goal
        THEN:  Appropriate ExternalIntegration(s) are returned
        """
        external_integration = create_externalintegration(db_session, "protocol", goal="goal")
        library = create_library(db_session)
        goal = external_integration.goal
        qu = ExternalIntegration.for_library_and_goal(db_session, library, goal)
        get_one = ExternalIntegration.one_for_library_and_goal

        # This matches nothing because the ExternalIntegration is not
        # associated with the Library.
        assert qu.all() == []
        assert get_one(db_session, library, goal) is None

        # Associate the library with the ExternalIntegration and
        # the query starts matching it. one_for_library_and_goal
        # also starts returning it.
        external_integration.libraries.append(library)
        assert qu.all() == [external_integration]
        assert get_one(db_session, library, goal) == external_integration

        # Create another, similar ExternalIntegration. By itself, this
        # has no effect.
        integration2, _ = create(db_session, ExternalIntegration, goal=goal, protocol="protocol2")
        assert qu.all() == [external_integration]
        assert get_one(db_session, library, goal) == external_integration

        # Associate that ExternalIntegration with the library, and
        # the query starts picking it up, and one_for_library_and_goal
        # starts raising an exception.
        integration2.libraries.append(library)
        assert set([external_integration, integration2]) == set(qu.all())
        with pytest.raises(CannotLoadConfiguration) as excinfo:
            get_one(db_session, library, goal)
        assert "Library {} defines multiple integrations with goal {}".format(library.name, goal) \
            in str(excinfo.value)

    def test_for_collection_and_purpose_exception(self, db_session, create_collection):
        """
        GIVEN: A Collection and purpose
        WHEN:  The purpose is incorrect
        THEN:  A CannotLoadConfiguration is raised
        """
        wrong_purpose = "isbn"
        collection = create_collection(db_session)

        with pytest.raises(CannotLoadConfiguration) as excinfo:
            ExternalIntegration.for_collection_and_purpose(db_session, collection, wrong_purpose)
        assert "No storage integration for collection '%s' and purpose '%s' is configured" \
            % (collection.name, wrong_purpose) \
            in str(excinfo.value)

    def test_for_collection_and_purpose(
            self, db_session, create_collection, create_externalintegration, create_external_integration_link):
        """
        GIVEN: A Collection with an ExternalIntegration, a purpose,
               an ExternalIntegrationLink with the ExternalIntegration and purpose
        WHEN:  Creating an ExternalIntegration for the Collection and purpose
        THEN:  The ExternalIntegration is returned
        """
        collection = create_collection(db_session)
        external_integration = create_externalintegration(db_session, "some protocol")
        collection.external_integration_id = external_integration.id
        purpose = "covers_mirror"

        create_external_integration_link(
            db_session, integration=external_integration, purpose=purpose
        )

        integration = ExternalIntegration.for_collection_and_purpose(
            db_session, collection=collection, purpose=purpose
        )

        assert isinstance(integration, ExternalIntegration)

    def test_with_setting_value(self, db_session, create_externalintegration):
        """
        GIVEN: ExternalIntegration with protocol, goal, key, and value
        WHEN:  Finding an ExternalIntegration with these settings
        THEN:  The correct ExternalIntegration(s) are returned
        """
        def results():
            '''Run the query and return all results.'''
            return ExternalIntegration.with_setting_value(
                db_session, "protocol", "goal", "key", "value"
            ).all()

        # We start off with no results.
        assert [] == results()

        # This ExternalIntegration will not match the result,
        # even though protocol and goal match, because it
        # doesn't have the 'key' ConfigurationSetting set.
        integration = create_externalintegration(db_session, "protocol", "goal")
        assert [] == results()

        # Now 'key' is set, but set to the wrong value.
        setting = integration.setting("key")
        setting.value = "wrong"
        assert [] == results()

        # Now it's set to the right value, so we get a result.
        setting.value = "value"
        assert [integration] == results()

        # Create another, identical integration.
        integration2, _ = create(
            db_session, ExternalIntegration, protocol="protocol", goal="goal"
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

    def test_data_source(self, db_session, create_collection, default_library):
        """
        GIVEN: A Collection
        WHEN:  Setting the external integration data source
        THEN:  A new data source is created if necessary
        """
        collection = create_collection(db_session, protocol=ExternalIntegration.OVERDRIVE)
        [default_collection] = default_library.collections

        # For most collections, the protocol determines the
        # data source.
        assert collection.data_source.name == DataSource.OVERDRIVE

        # For OPDS Import collections, data source is a setting which
        # might not be present.
        assert default_collection.data_source is None

        # data source will be automatically created if necessary.
        default_collection.external_integration.setting(
            Collection.DATA_SOURCE_NAME_SETTING
        ).value = "New Data Source"
        assert default_collection.data_source.name == "New Data Source"

    def test_set_key_value_pair(self, db_session, create_externalintegration):
        """
        GIVEN: An ExternalIntegration
        WHEN:  Setting settings for the ExternalIntegration
        THEN:  Creates or updates the key-value setting for the ExternalIntegration
        """
        external_integration = create_externalintegration(db_session, "protocol")
        assert external_integration.settings == []

        setting = external_integration.set_setting("website_id", "id1")
        assert setting.key == "website_id"
        assert setting.value == "id1"

        # Calling set() again updates the key-value pair.
        assert [setting.id] == [x.id for x in external_integration.settings]
        setting2 = external_integration.set_setting("website_id", "id2")
        assert setting2.id == setting.id
        assert setting2.value == "id2"

        assert setting2 == external_integration.setting("website_id")

    def test_explain(self, db_session, create_externalintegration, create_library):
        """
        GIVEN: An ExternalIntegration associated with two Libraries
        WHEN:  Explaining the ExternalIntegration
        THEN:  Returns a series of human-readable strings to explain the ExternalIntegration settings
        """
        integration = create_externalintegration(db_session, "protocol", "goal")
        integration.name = "The Integration"
        integration.url = "http://url/"
        integration.username = "someuser"
        integration.password = "somepass"
        integration.setting("somesetting").value = "somevalue"

        # Two different libraries have slightly different
        # configurations for this integration.
        library1 = create_library(db_session, name="library1", short_name="library1")
        library1.name = "First Library"
        library1.integrations.append(integration)
        ConfigurationSetting.for_library_and_externalintegration(
            db_session, "library-specific", library1, integration
        ).value = "value1"

        library2 = create_library(db_session, name="library2", short_name="library2")
        library2.name = "Second Library"
        library2.integrations.append(integration)
        ConfigurationSetting.for_library_and_externalintegration(
            db_session, "library-specific", library2, integration
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

    def test_custom_accept_header(self, db_session, create_externalintegration):
        """
        GIVEN: An ExternalIntegration with a protocol and goal
        WHEN:  Setting a custom accept header
        THEN:  Custom accept header is set
        """
        integration = create_externalintegration(db_session, "protocol", "goal")
        # Must be empty if not set
        assert integration.custom_accept_header is None

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
        """
        GIVEN: A ConfigurationOption
        WHEN:  Calling .to_settings()
        THEN:  A dictionary containing option metadata
        """
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
        """
        GIVEN: An Enum with two variables
        WHEN:  Converting the Enum to a list of options
        THEN:  Returns a list of options
        """
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
    @pytest.mark.parametrize(
        'setting_name,expected_value',
        [
            ('setting1', 12345),
            ('setting2', '12345')
        ],
    )
    def test_getters(self, setting_name, expected_value):
        """
        GIVEN: A ConfigurationGrouping
        WHEN:  Getting attributes from the ConfigurationGrouping
        THEN:  Correct attribute is returned
        """
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

    @pytest.mark.parametrize(
        'setting_name,expected_value',
        [
            ('setting1', 12345),
            ('setting2', '12345')
        ],
    )
    def test_setters(self, setting_name, expected_value):
        """
        GIVEN: A ConfigurationGrouping
        WHEN:  Setting an attribute for the ConfigurationGrouping
        THEN:  Correct attribute is set
        """
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
        """
        GIVEN: A ConfigurationGrouping
        WHEN:  Getting a list of settings
        THEN:  List of settings is returned
        """
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
        """
        GIVEN: A ConfigurationGrouping
        WHEN:  Getting a list of settings
        THEN:  List of settings is returned
        """
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


class TestBooleanConfigurationMetadata:

    @pytest.mark.parametrize(
        'value,expected_result',
        [
            ('true', True),
            ('t', True),
            ('yes', True),
            ('y', True),
            (1, False),
            ('false', False)
        ],
    )
    def test_configuration_metadata_correctly_recognize_bool_values(
            self, db_session, create_externalintegration, value, expected_result):
        """
        GIVEN: A ConfigurationMetadata
        WHEN:  Setting a value that can be boolean
        THEN:  Value is correctly translated into boolean
        """
        # Arrange
        external_integration = create_externalintegration(db_session, 'test')

        external_integration_association = create_autospec(spec=HasExternalIntegration)
        external_integration_association.external_integration = MagicMock(return_value=external_integration)

        configuration_storage = ConfigurationStorage(external_integration_association)

        configuration = ConfigurationWithBooleanProperty(configuration_storage, db_session)

        # We set a new value using ConfigurationMetadata.__set__
        configuration.boolean_setting = value

        # Act
        # We read the existing value using ConfigurationMetadata.__get__
        result = ConfigurationMetadata.to_bool(configuration.boolean_setting)

        # Assert
        assert expected_result == result
