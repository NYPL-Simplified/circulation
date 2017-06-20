import contextlib
import datetime
from nose.tools import set_trace
import os
import json
import logging
import copy
from util import LanguageCodes
from flask.ext.babel import lazy_gettext as _

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

    # ConfigurationSetting key for the base url of the app.
    BASE_URL_KEY = u'base_url'

    # Policies, mostly circulation specific
    POLICIES = "policies"
   
    LANES_POLICY = "lanes"

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

    AXIS_INTEGRATION = "Axis 360"
    ONECLICK_INTEGRATION = "OneClick"
    OVERDRIVE_INTEGRATION = "Overdrive"
    THREEM_INTEGRATION = "3M"

    # ConfigurationSEtting key for a CDN's mirror domain
    CDN_MIRRORED_DOMAIN_KEY = u'mirrored_domain'

    UNINITIALIZED_CDNS = object()

    BASE_OPDS_AUTHENTICATION_DOCUMENT = "base_opds_authentication_document"


    # The names of the site-wide configuration settings that determine
    # feed cache time.
    NONGROUPED_MAX_AGE_POLICY = "default_nongrouped_feed_max_age" 
    GROUPED_MAX_AGE_POLICY = "default_grouped_feed_max_age" 

    # The name of the per-library configuration policy that controls whether
    # books may be put on hold.
    ALLOW_HOLDS = "allow_holds"

    # Each library may set a minimum quality for the books that show
    # up in the 'featured' lanes that show up on the front page.
    MINIMUM_FEATURED_QUALITY = "minimum_featured_quality"

    # Each library may configure the maximum number of books in the
    # 'featured' lanes.
    FEATURED_LANE_SIZE = "featured_lane_size"
    
    # The name of the per-library per-patron authentication integration
    # regular expression used to derive a patron's external_type from
    # their authorization_identifier.
    EXTERNAL_TYPE_REGULAR_EXPRESSION = 'external_type_regular_expression'

    WEBSITE_URL = u'website'
    
    SITEWIDE_SETTINGS = [
        {
            "key": NONGROUPED_MAX_AGE_POLICY,
            "label": _("Cache time for paginated OPDS feeds"),
        },
        {
            "key": GROUPED_MAX_AGE_POLICY,
            "label": _("Cache time for grouped OPDS feeds")
        },
    ]

    LIBRARY_SETTINGS = [
        {
            "key": WEBSITE_URL,
            "label": _("URL of the library's website"),
        },
        {
            "key": ALLOW_HOLDS,
            "label": _("Allow books to be put on hold"),
        },
        {
            "key": FEATURED_LANE_SIZE,
            "label": _("Maximum number of books in the 'featured' lanes"),
        },
        {
            "key": MINIMUM_FEATURED_QUALITY,
            "label": _("Minimum quality for books that show up in 'featured' lanes"),
        },
    ]

    # This is set once data is loaded from the database and inserted into
    # the Configuration object.
    LOADED_FROM_DATABASE = 'loaded_from_database'

    @classmethod
    def loaded_from_database(cls):
        """Has the site configuration been loaded from the database yet?"""
        return cls.instance and cls.instance.get(
            cls.LOADED_FROM_DATABASE, False
        )
    
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
    def cdns(cls):
        from model import ExternalIntegration
        cdns = cls.integration(ExternalIntegration.CDN)
        if cdns == cls.UNINITIALIZED_CDNS:
            raise CannotLoadConfiguration(
                'CDN configuration has not been loaded from the database'
            )
        return cdns

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

    @classmethod
    def load_cdns(cls, _db, config_instance=None):
        from model import ExternalIntegration as EI
        cdns = _db.query(EI).filter(EI.goal==EI.CDN_GOAL).all()
        cdn_integration = dict()
        for cdn in cdns:
            cdn_integration[cdn.setting(cls.CDN_MIRRORED_DOMAIN_KEY).value] = cdn.url

        config_instance = config_instance or cls.instance
        config_instance[EI.CDN] = cdn_integration

    @classmethod
    def base_opds_authentication_document(cls):
        return cls.get(cls.BASE_OPDS_AUTHENTICATION_DOCUMENT, {})

    @classmethod
    def logging_policy(cls):
        default_logging = {}
        return cls.get(cls.LOGGING, default_logging)

    @classmethod
    def localization_languages(cls):
        languages = cls.policy(cls.LOCALIZATION_LANGUAGES, default=["eng"])
        return [LanguageCodes.three_to_two[l] for l in languages]
    
    @classmethod
    def load(cls, _db=None):
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

        if _db:
            cls.load_cdns(_db)
            cls.instance[cls.LOADED_FROM_DATABASE] = True
        else:
            if not cls.integration('CDN'):
                cls.instance[cls.INTEGRATIONS]['CDN'] = cls.UNINITIALIZED_CDNS

        return configuration

    @classmethod
    def _load(cls, str):
        lines = [x for x in str.split("\n")
                 if not (x.strip().startswith("#") or x.strip().startswith("//"))]
        return json.loads("\n".join(lines))
