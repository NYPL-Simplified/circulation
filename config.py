import contextlib
import datetime

import os
import json
import logging
import copy
from sqlalchemy import create_engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.orm.session import Session
from flask_babel import lazy_gettext as _

from .facets import FacetConstants
from .entrypoint import EntryPoint

from sqlalchemy.exc import ArgumentError
from .util import LanguageCodes

# It's convenient for other modules import IntegrationException
# from this module, alongside CannotLoadConfiguration.
from .util.http import IntegrationException


class CannotLoadConfiguration(IntegrationException):
    """The current configuration of an external integration, or of the
    site as a whole, is in an incomplete or inconsistent state.

    This is more specific than a base IntegrationException because it
    assumes the problem is evident just by looking at the current
    configuration, with no need to actually talk to the foreign
    server.
    """
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


class ConfigurationConstants(object):

    # Each facet group has two associated per-library keys: one
    # configuring which facets are enabled for that facet group, and
    # one configuring which facet is the default.
    ENABLED_FACETS_KEY_PREFIX = "facets_enabled_"
    DEFAULT_FACET_KEY_PREFIX = "facets_default_"
    
    # The "level" property determines which admins will be able to modify the setting.  Level 1 settings can be modified by anyone.
    # Level 2 settings can be modified only by library managers and system admins (i.e. not by librarians).  Level 3 settings can be changed only by system admins.
    # If no level is specified, the setting will be treated as Level 1 by default.
    ALL_ACCESS = 1
    SYS_ADMIN_OR_MANAGER = 2
    SYS_ADMIN_ONLY = 3

class Configuration(ConfigurationConstants):

    log = logging.getLogger("Configuration file loader")

    # This is a dictionary containing information loaded from the
    # configuration file. It will be populated immediately after
    # this class is defined.
    instance = None


    # Environment variables that contain URLs to the database
    DATABASE_TEST_ENVIRONMENT_VARIABLE = 'SIMPLIFIED_TEST_DATABASE'
    DATABASE_PRODUCTION_ENVIRONMENT_VARIABLE = 'SIMPLIFIED_PRODUCTION_DATABASE'

    # The version of the app.
    APP_VERSION = 'app_version'
    VERSION_FILENAME = '.version'
    NO_APP_VERSION_FOUND = object()

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
    BASE_URL_KEY = 'base_url'

    # ConfigurationSetting to enable the MeasurementReaper script
    MEASUREMENT_REAPER = 'measurement_reaper_enabled'

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
    DATABASE_INTEGRATION = "Postgres"
    DATABASE_PRODUCTION_URL = "production_url"
    DATABASE_TEST_URL = "test_url"

    CONTENT_SERVER_INTEGRATION = "Content Server"

    AXIS_INTEGRATION = "Axis 360"
    RBDIGITAL_INTEGRATION = "RBDigital"
    OVERDRIVE_INTEGRATION = "Overdrive"
    THREEM_INTEGRATION = "3M"

    # ConfigurationSetting key for a CDN's mirror domain
    CDN_MIRRORED_DOMAIN_KEY = 'mirrored_domain'

    # The name of the per-library configuration policy that controls whether
    # books may be put on hold.
    ALLOW_HOLDS = "allow_holds"

    # Each library may set a minimum quality for the books that show
    # up in the 'featured' lanes that show up on the front page.
    MINIMUM_FEATURED_QUALITY = "minimum_featured_quality"
    DEFAULT_MINIMUM_FEATURED_QUALITY = 0.65

    # Each library may configure the maximum number of books in the
    # 'featured' lanes.
    FEATURED_LANE_SIZE = "featured_lane_size"

    # The name of the per-library per-patron authentication integration
    # regular expression used to derive a patron's external_type from
    # their authorization_identifier.
    EXTERNAL_TYPE_REGULAR_EXPRESSION = 'external_type_regular_expression'

    WEBSITE_URL = 'website'
    NAME = 'name'
    SHORT_NAME = 'short_name'

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"

    # The default value to put into the 'app' field of JSON-format logs,
    # unless LOG_APP_NAME overrides it.
    DEFAULT_APP_NAME = 'simplified'

    # Settings for the integration with protocol=INTERNAL_LOGGING
    LOG_LEVEL = 'log_level'
    LOG_APP_NAME = 'log_app'
    DATABASE_LOG_LEVEL = 'database_log_level'
    LOG_LEVEL_UI = [
        { "key": DEBUG, "label": _("Debug") },
        { "key": INFO, "label": _("Info") },
        { "key": WARN, "label": _("Warn") },
        { "key": ERROR, "label": _("Error") },
    ]

    EXCLUDED_AUDIO_DATA_SOURCES = 'excluded_audio_data_sources'

    SITEWIDE_SETTINGS = [
        {
            "key": BASE_URL_KEY,
            "label": _("Base url of the application"),
            "required": True,
            "format": "url",
        },
        {
            "key": LOG_LEVEL, "label": _("Log Level"), "type": "select",
            "options": LOG_LEVEL_UI, "default": INFO,
        },
        {
            "key": LOG_APP_NAME, "label": _("Application name"),
            "description": _("Log messages originating from this application will be tagged with this name. If you run multiple instances, giving each one a different application name will help you determine which instance is having problems."),
            "default": DEFAULT_APP_NAME,
            "required": True,
        },
        {
            "key": DATABASE_LOG_LEVEL, "label": _("Database Log Level"),
            "type": "select", "options": LOG_LEVEL_UI,
            "description": _("Database logs are extremely verbose, so unless you're diagnosing a database-related problem, it's a good idea to set a higher log level for database messages."),
            "default": WARN,
        },
        {
            "key": EXCLUDED_AUDIO_DATA_SOURCES,
            "label": _("Excluded audiobook sources"),
            "description": _("Audiobooks from these data sources will be hidden from the collection, even if they would otherwise show up as available."),
            "default": None,
            "required": True,
        },
        {
            "key": MEASUREMENT_REAPER,
            "label": _("Cleanup old measurement data"), "type": "select",
            "description": _("If this settings is 'true' old book measurement data will be cleaned out of the database. Some sites may want to keep this data for later analysis."),
            "options": { "true": "true", "false": "false" }, "default": "true",
        },
    ]

    LIBRARY_SETTINGS = [
        {
            "key": NAME,
            "label": _("Name"),
            "description": _("The human-readable name of this library."),
            "category": "Basic Information",
            "level": ConfigurationConstants.SYS_ADMIN_ONLY,
            "required": True
        },
        {
            "key": SHORT_NAME,
            "label": _("Short name"),
            "description": _("A short name of this library, to use when identifying it in scripts or URLs, e.g. 'NYPL'."),
            "category": "Basic Information",
            "level": ConfigurationConstants.SYS_ADMIN_ONLY,
            "required": True
        },
        {
            "key": WEBSITE_URL,
            "label": _("URL of the library's website"),
            "description": _("The library's main website, e.g. \"https://www.nypl.org/\" (not this Circulation Manager's URL)."),
            "required": True,
            "format": "url",
            "level": ConfigurationConstants.SYS_ADMIN_ONLY,
            "category": "Basic Information"
        },
        {
            "key": ALLOW_HOLDS,
            "label": _("Allow books to be put on hold"),
            "type": "select",
            "options": [
                { "key": "true", "label": _("Allow holds") },
                { "key": "false", "label": _("Disable holds") },
            ],
            "default": "true",
            "category": "Loans, Holds, & Fines",
            "level": ConfigurationConstants.SYS_ADMIN_ONLY
        },
        { "key": EntryPoint.ENABLED_SETTING,
          "label": _("Enabled entry points"),
          "description": _("Patrons will see the selected entry points at the top level and in search results. <p>Currently supported audiobook vendors: Bibliotheca, Axis 360"),
          "type": "list",
          "options": [
              { "key": entrypoint.INTERNAL_NAME,
                "label": EntryPoint.DISPLAY_TITLES.get(entrypoint) }
              for entrypoint in EntryPoint.ENTRY_POINTS
          ],
          "default": [x.INTERNAL_NAME for x in EntryPoint.DEFAULT_ENABLED],
          "category": "Lanes & Filters",
          # Renders a component with options that get narrowed down as the user makes selections.
          "format": "narrow",
          # Renders an input field that cannot be edited.
          "readOnly": True,
          "level": ConfigurationConstants.SYS_ADMIN_ONLY
        },
        {
            "key": FEATURED_LANE_SIZE,
            "label": _("Maximum number of books in the 'featured' lanes"),
            "type": "number",
            "default": 15,
            "category": "Lanes & Filters",
            "level": ConfigurationConstants.ALL_ACCESS

        },
        {
            "key": MINIMUM_FEATURED_QUALITY,
            "label": _("Minimum quality for books that show up in 'featured' lanes"),
            "description": _("Between 0 and 1."),
            "type": "number",
            "max": 1,
            "default": DEFAULT_MINIMUM_FEATURED_QUALITY,
            "category": "Lanes & Filters",
            "level": ConfigurationConstants.ALL_ACCESS
        },
    ] + [
        { "key": ConfigurationConstants.ENABLED_FACETS_KEY_PREFIX + group,
          "label": description,
          "type": "list",
          "options": [
              { "key": facet, "label": FacetConstants.FACET_DISPLAY_TITLES.get(facet) }
              for facet in FacetConstants.FACETS_BY_GROUP.get(group)
          ],
          "default": FacetConstants.FACETS_BY_GROUP.get(group),
          "category": "Lanes & Filters",
          # Tells the front end that each of these settings is related to the corresponding default setting.
          "paired": ConfigurationConstants.DEFAULT_FACET_KEY_PREFIX + group,
          "level": ConfigurationConstants.SYS_ADMIN_OR_MANAGER
        } for group, description in FacetConstants.GROUP_DESCRIPTIONS.items()
    ] + [
        { "key": ConfigurationConstants.DEFAULT_FACET_KEY_PREFIX + group,
          "label": _("Default %(group)s", group=display_name),
          "type": "select",
          "options": [
              { "key": facet, "label": FacetConstants.FACET_DISPLAY_TITLES.get(facet) }
              for facet in FacetConstants.FACETS_BY_GROUP.get(group)
          ],
          "default": FacetConstants.DEFAULT_FACET.get(group),
          "category": "Lanes & Filters",
          "skip": True
        } for group, display_name in FacetConstants.GROUP_DISPLAY_TITLES.items()
    ]

    # This is set once CDN data is loaded from the database and
    # inserted into the Configuration object.
    CDNS_LOADED_FROM_DATABASE = 'loaded_from_database'

    @classmethod
    def load(cls, _db=None):
        """Load configuration information from the filesystem, and
        (optionally) from the database.
        """
        cls.instance = cls.load_from_file()
        if _db:
            # Only do the database portion of the work if
            # a database connection was provided.
            cls.load_cdns(_db)
        cls.app_version()
        for parent in cls.__bases__:
            if parent.__name__.endswith('Configuration'):
                parent.load(_db)

    @classmethod
    def cdns_loaded_from_database(cls):
        """Has the site configuration been loaded from the database yet?"""
        return cls.instance and cls.instance.get(
            cls.CDNS_LOADED_FROM_DATABASE, False
        )

    # General getters

    @classmethod
    def get(cls, key, default=None):
        if cls.instance is None:
            raise ValueError("No configuration object loaded!")
        return cls.instance.get(key, default)

    @classmethod
    def required(cls, key):
        if cls.instance is not None:
            value = cls.get(key)
            if value is not None:
                return value

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
        """Get CDN configuration, loading it from the database
        if necessary.
        """
        if not cls.cdns_loaded_from_database():
            # The CDNs were never initialized from the database.
            # Create a new database connection and find that
            # information now.
            from .model import SessionManager
            url = cls.database_url()
            _db = SessionManager.session(url)
            cls.load_cdns(_db)

        from .model import ExternalIntegration
        return cls.integration(ExternalIntegration.CDN)

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
    def database_url(cls):
        """Find the database URL configured for this site.

        For compatibility with old configurations, we will look in the
        site configuration first.

        If it's not there, we will look in the appropriate environment
        variable.
        """

        # To avoid expensive mistakes, test and production databases
        # are always configured with separate keys. The TESTING variable
        # controls which database is used, and it's set by the
        # package_setup() function called in every component's
        # tests/__init__.py.
        test = os.environ.get('TESTING', False)
        if test:
            config_key = cls.DATABASE_TEST_URL
            environment_variable = cls.DATABASE_TEST_ENVIRONMENT_VARIABLE
        else:
            config_key = cls.DATABASE_PRODUCTION_URL
            environment_variable = cls.DATABASE_PRODUCTION_ENVIRONMENT_VARIABLE

        url = os.environ.get(environment_variable)
        if not url:
            raise CannotLoadConfiguration(
                "Database URL was not defined in environment variable (%s)." % environment_variable
            )

        url_obj = None
        try:
            url_obj = make_url(url)
        except ArgumentError as e:
            # Improve the error message by giving a guide as to what's
            # likely to work.
            raise ArgumentError(
                "Bad format for database URL (%s). Expected something like postgres://[username]:[password]@[hostname]:[port]/[database name]" %
                url
            )

        # Calling __to_string__ will hide the password.
        logging.info("Connecting to database: %s" % url_obj.__to_string__())
        return url

    @classmethod
    def app_version(cls):
        """Returns the git version of the app, if a .version file exists."""
        version = cls.get(cls.APP_VERSION, None)
        if version:
            # The version has been set in Configuration before.
            return version

        # Look in the parent directory, e.g. circulation/ or metadata/
        root_dir = os.path.join(os.path.split(__file__)[0], "..")
        version_file = os.path.join(root_dir, cls.VERSION_FILENAME)

        version = cls.NO_APP_VERSION_FOUND
        if os.path.exists(version_file):
            with open(version_file) as f:
                version = f.readline().strip() or version

        cls.instance[cls.APP_VERSION] = version
        return version

    @classmethod
    def data_directory(cls):
        return cls.get(cls.DATA_DIRECTORY)

    @classmethod
    def load_cdns(cls, _db, config_instance=None):
        from .model import ExternalIntegration as EI
        cdns = _db.query(EI).filter(EI.goal==EI.CDN_GOAL).all()
        cdn_integration = dict()
        for cdn in cdns:
            cdn_integration[cdn.setting(cls.CDN_MIRRORED_DOMAIN_KEY).value] = cdn.url

        config_instance = config_instance or cls.instance
        integrations = config_instance.setdefault(cls.INTEGRATIONS, {})
        integrations[EI.CDN] = cdn_integration
        config_instance[cls.CDNS_LOADED_FROM_DATABASE] = True

    @classmethod
    def localization_languages(cls):
        languages = cls.policy(cls.LOCALIZATION_LANGUAGES, default=["eng"])
        return [LanguageCodes.three_to_two[l] for l in languages]

    # The last time the database configuration is known to have changed.
    SITE_CONFIGURATION_LAST_UPDATE = "site_configuration_last_update"

    # The last time we *checked* whether the database configuration had
    # changed.
    LAST_CHECKED_FOR_SITE_CONFIGURATION_UPDATE = "last_checked_for_site_configuration_update"

    # A sitewide configuration setting controlling *how often* to check
    # whether the database configuration has changed.
    #
    # NOTE: This setting is currently not used; the most reliable
    # value seems to be zero. Assuming that's true, this whole
    # subsystem can be removed.
    SITE_CONFIGURATION_TIMEOUT = 'site_configuration_timeout'

    # The name of the service associated with a Timestamp that tracks
    # the last time the site's configuration changed in the database.
    SITE_CONFIGURATION_CHANGED = "Site Configuration Changed"

    @classmethod
    def last_checked_for_site_configuration_update(cls):
        """When was the last time we actually checked when the database
        was updated?
        """
        return cls.instance.get(
            cls.LAST_CHECKED_FOR_SITE_CONFIGURATION_UPDATE, None
        )

    @classmethod
    def site_configuration_last_update(cls, _db, known_value=None,
                                       timeout=0):
        """Check when the site configuration was last updated.

        Updates Configuration.instance[Configuration.SITE_CONFIGURATION_LAST_UPDATE].
        It's the application's responsibility to periodically check
        this value and reload the configuration if appropriate.

        :param known_value: We know when the site configuration was
            last updated--it's this timestamp. Use it instead of checking
            with the database.

        :param timeout: We will only call out to the database once in
            this number of seconds. If we are asked again before this
            number of seconds elapses, we will assume site
            configuration has not changed. By default, we call out to
            the database every time.

        :return: a datetime object.

        """

        now = datetime.datetime.utcnow()

        # NOTE: Currently we never check the database (because timeout is
        # never set to None). This code will hopefully be removed soon.
        if _db and timeout is None:
            from .model import ConfigurationSetting
            timeout = ConfigurationSetting.sitewide(
                _db, cls.SITE_CONFIGURATION_TIMEOUT
            ).int_value

        if timeout is None:
            # NOTE: this only happens if timeout is explicitly set to
            # None _and_ no database value is present. Right now that
            # never happens because timeout is never explicitly set to
            # None.
            timeout = 60

        last_check = cls.instance.get(
            cls.LAST_CHECKED_FOR_SITE_CONFIGURATION_UPDATE
        )

        if (not known_value
            and last_check and (now - last_check).total_seconds() < timeout):
            # We went to the database less than [timeout] seconds ago.
            # Assume there has been no change.
            return cls._site_configuration_last_update()

        # Ask the database when was the last time the site
        # configuration changed. Specifically, this is the last time
        # site_configuration_was_changed() (defined in model.py) was
        # called.
        if not known_value:
            from .model import Timestamp
            known_value = Timestamp.value(
                _db, cls.SITE_CONFIGURATION_CHANGED, service_type=None,
                collection=None
            )
        if not known_value:
            # The site configuration has never changed.
            last_update = None
        else:
            last_update = known_value

        # Update the Configuration object's record of the last update time.
        cls.instance[cls.SITE_CONFIGURATION_LAST_UPDATE] = last_update

        # Whether that record changed or not, the time at which we
        # _checked_ is going to be set to the current time.
        cls.instance[cls.LAST_CHECKED_FOR_SITE_CONFIGURATION_UPDATE] = now
        return last_update

    @classmethod
    def _site_configuration_last_update(cls):
        """Get the raw SITE_CONFIGURATION_LAST_UPDATE value,
        without any attempt to find a fresher value from the database.
        """
        return cls.instance.get(cls.SITE_CONFIGURATION_LAST_UPDATE, None)

    @classmethod
    def load_from_file(cls):
        """Load additional site configuration from a config file.

        This is being phased out in favor of taking all configuration from a
        database.
        """
        cfv = 'SIMPLIFIED_CONFIGURATION_FILE'
        config_path = os.environ.get(cfv)
        if config_path:
            try:
                cls.log.info("Loading configuration from %s", config_path)
                configuration = cls._load(open(config_path).read())
            except Exception as e:
                raise CannotLoadConfiguration(
                    "Error loading configuration file %s: %s" % (
                        config_path, e)
                )
        else:
            configuration = cls._load('{}')

        return configuration

    @classmethod
    def _load(cls, str):
        lines = [x for x in str.split("\n")
                 if not (x.strip().startswith("#") or x.strip().startswith("//"))]
        return json.loads("\n".join(lines))

# Immediately load the configuration file (if any).
Configuration.instance = Configuration.load_from_file()
