import contextlib
import datetime
from nose.tools import set_trace
import os
import json
import logging
import copy
from facets import FacetConstants as Facets
from util import LanguageCodes

class CannotLoadConfiguration(Exception):
    pass

@contextlib.contextmanager
def temp_config(new_config=None, replacement_classes=None):
    old_config = Configuration.instance
    replacement_classes = replacement_classes or [Configuration]
    if new_config is None:
        new_config = copy.deepcopy(old_config)
    try:
        for c in replacement_classes:
            c.instance = new_config
        yield new_config
    finally:
        for c in replacement_classes:
            c.instance = old_config

@contextlib.contextmanager
def empty_config(replacement_classes=None):
    with temp_config({}, replacement_classes) as i:
        yield i


class Configuration(object):

    log = logging.getLogger("Configuration file loader")

    instance = None

    # Logging stuff
    LOGGING_LEVEL = "level"
    LOGGING_FORMAT = "format"
    LOG_FORMAT_TEXT = "text"
    LOG_FORMAT_JSON = "json"

    # Logging
    LOGGING = "logging"
    LOG_LEVEL = "level"
    DATABASE_LOG_LEVEL = "database_level"
    LOG_OUTPUT_TYPE = "output"
    LOG_DATA_FORMAT = "format"

    DATA_DIRECTORY = "data_directory"

    # ConfigurationSetting key  for the base url of the app.
    BASE_URL_KEY = u'base_url'

    # Policies, mostly circulation specific
    POLICIES = "policies"

    # TODO: These policies should be converted to per-library
    # ConfigurationSettings. Since they affect how feeds are
    # generated, this can only happen once lane configuration happens
    # in the database.
    HOLD_POLICY = "holds"
    HOLD_POLICY_ALLOW = "allow"
    HOLD_POLICY_HIDE = "hide"
    
    LANES_POLICY = "lanes"

    # Facet policies
    FACET_POLICY = 'facets'
    ENABLED_FACETS_KEY = 'enabled'
    DEFAULT_FACET_KEY = 'default'

    DEFAULT_ENABLED_FACETS = {
        Facets.ORDER_FACET_GROUP_NAME : [
            Facets.ORDER_AUTHOR, Facets.ORDER_TITLE, Facets.ORDER_ADDED_TO_COLLECTION
        ],
        Facets.AVAILABILITY_FACET_GROUP_NAME : [
            Facets.AVAILABLE_ALL, Facets.AVAILABLE_NOW, Facets.AVAILABLE_OPEN_ACCESS
        ],
        Facets.COLLECTION_FACET_GROUP_NAME : [
            Facets.COLLECTION_FULL, Facets.COLLECTION_MAIN, Facets.COLLECTION_FEATURED
        ]
    }

    DEFAULT_FACET = {
        Facets.ORDER_FACET_GROUP_NAME : Facets.ORDER_AUTHOR,
        Facets.AVAILABILITY_FACET_GROUP_NAME : Facets.AVAILABLE_ALL,
        Facets.COLLECTION_FACET_GROUP_NAME : Facets.COLLECTION_MAIN,
    }

    # Lane policies
    DEFAULT_OPDS_FORMAT = "verbose_opds_entry"

    ANALYTICS_POLICY = "analytics"

    LOCALIZATION_LANGUAGES = "localization_languages"

    # Integrations
    URL = "url"
    NAME = "name"
    TYPE = "type"
    INTEGRATIONS = "integrations"
    DATABASE_INTEGRATION = u"Postgres"
    DATABASE_PRODUCTION_URL = "production_url"
    DATABASE_TEST_URL = "test_url"

    CONTENT_SERVER_INTEGRATION = u"Content Server"

    NYT_INTEGRATION = u"New York Times"
    NYT_BEST_SELLERS_API_KEY = u"best_sellers_api_key"

    NOVELIST_INTEGRATION = u"NoveList Select"
    NOVELIST_PROFILE = u"profile"
    NOVELIST_PASSWORD = u"password"

    AXIS_INTEGRATION = "Axis 360"
    ONECLICK_INTEGRATION = "OneClick"
    OVERDRIVE_INTEGRATION = "Overdrive"
    THREEM_INTEGRATION = "3M"

    MINIMUM_FEATURED_QUALITY = "minimum_featured_quality"
    FEATURED_LANE_SIZE = "featured_lane_size"

    BASE_OPDS_AUTHENTICATION_DOCUMENT = "base_opds_authentication_document"
    SHOW_STAFF_PICKS_ON_TOP_LEVEL = "show_staff_picks_on_top_level"

    # General getters

    @classmethod
    def get(cls, key, default=None):
        if not cls.instance:
            raise ValueError("No configuration file loaded!")
        return cls.instance.get(key, default)

    @classmethod
    def required(cls, key):
        if cls.instance:
            value = cls.get(key)
            if value is not None:
                return value
        raise ValueError(
            "Required configuration variable %s was not defined!" % key
        )

    @classmethod
    def integration(cls, name, required=False):
        """Find an integration configuration by name."""
        integrations = cls.get(cls.INTEGRATIONS, {})
        v = integrations.get(name, {})
        if not v and required:
            raise ValueError(
                "Required integration '%s' was not defined! I see: %r" % (
                    name, ", ".join(sorted(integrations.keys()))
                )
            )
        return v

    @classmethod
    def integration_url(cls, name, required=False):
        """Find the URL to an integration."""
        integration = cls.integration(name, required=required)
        v = integration.get(cls.URL, None)
        if not v and required:
            raise ValueError(
                "Integration '%s' did not define a required 'url'!" % name
            )
        return v

    @classmethod
    def policy(cls, name, default=None, required=False):
        """Find a policy configuration by name."""
        v = cls.get(cls.POLICIES, {}).get(name, default)
        if not v and required:
            raise ValueError(
                "Required policy %s was not defined!" % name
            )
        return v

    # More specific getters.

    @classmethod
    def database_url(cls, test=False):
        if test:
            key = cls.DATABASE_TEST_URL
        else:
            key = cls.DATABASE_PRODUCTION_URL
        return cls.integration(cls.DATABASE_INTEGRATION)[key]

    @classmethod
    def data_directory(cls):
        return cls.get(cls.DATA_DIRECTORY)

    # TODO: This needs to be turned into a per-Library
    # ConfigurationSetting, but it's not practical to do so until lane
    # configuration is moved into the database.
    @classmethod
    def hold_policy(cls):
        return cls.policy(cls.HOLD_POLICY, cls.HOLD_POLICY_ALLOW)

    @classmethod
    def enabled_facets(cls, group_name):
        """Look up the enabled facets for a given facet group."""
        policy = cls.policy(cls.FACET_POLICY)
        if not policy or not cls.ENABLED_FACETS_KEY in policy:
            return cls.DEFAULT_ENABLED_FACETS[group_name]
        return policy[cls.ENABLED_FACETS_KEY][group_name]

    @classmethod
    def default_facet(cls, group_name):
        """Look up the default facet for a given facet group."""
        policy = cls.policy(cls.FACET_POLICY)
        if not policy or not cls.DEFAULT_FACET_KEY in policy:
            return cls.DEFAULT_FACET[group_name]
        return policy[cls.DEFAULT_FACET_KEY][group_name]

    @classmethod
    def base_opds_authentication_document(cls):
        return cls.get(cls.BASE_OPDS_AUTHENTICATION_DOCUMENT, {})

    @classmethod
    def logging_policy(cls):
        default_logging = {}
        return cls.get(cls.LOGGING, default_logging)

    # TODO: Needs to be a per-library ConfigurationSetting once
    # lanes are configured in database.
    @classmethod
    def minimum_featured_quality(cls):
        return float(cls.policy(cls.MINIMUM_FEATURED_QUALITY, 0.65))

    # TODO: Needs to be a per-library ConfigurationSetting once
    # lanes are configured in database.
    @classmethod
    def featured_lane_size(cls):
        return int(cls.policy(cls.FEATURED_LANE_SIZE, 15))

    # TODO: Needs to be a per-library ConfigurationSetting once
    # lanes are configured in database.
    @classmethod
    def show_staff_picks_on_top_level(cls):
        return cls.policy(cls.SHOW_STAFF_PICKS_ON_TOP_LEVEL, default=True)

    @classmethod
    def localization_languages(cls):
        languages = cls.policy(cls.LOCALIZATION_LANGUAGES, default=["eng"])
        return [LanguageCodes.three_to_two[l] for l in languages]
    
    @classmethod
    def load(cls):
        cfv = 'SIMPLIFIED_CONFIGURATION_FILE'
        if not cfv in os.environ:
            raise CannotLoadConfiguration(
                "No configuration file defined in %s." % cfv)

        config_path = os.environ[cfv]
        try:
            cls.log.info("Loading configuration from %s", config_path)
            configuration = cls._load(open(config_path).read())
        except Exception, e:
            raise CannotLoadConfiguration(
                "Error loading configuration file %s: %s" % (
                    config_path, e)
            )
        cls.instance = configuration
        return configuration

    @classmethod
    def _load(cls, str):
        lines = [x for x in str.split("\n")
                 if not (x.strip().startswith("#") or x.strip().startswith("//"))]
        return json.loads("\n".join(lines))
