import contextlib
from nose.tools import set_trace
import os
import json
import logging

class CannotLoadConfiguration(Exception):
    pass

@contextlib.contextmanager
def temp_config(new_config=None, replacement_classes=None):
    old_config = Configuration.instance
    replacement_classes = replacement_classes or [Configuration]
    if new_config is None:
        new_config = dict(old_config)
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
    LOGGING = "logging"
    LOGGING_LEVEL = "level"
    LOGGING_FORMAT = "format"
    LOG_FORMAT_TEXT = "text"
    LOG_FORMAT_JSON = "json"

    # Links
    LINKS = "links"
    PRIVACY_POLICY = "privacy_policy"
    TERMS_OF_SERVICE = "terms_of_service"

    # Logging
    LOGGING = "logging"
    LOG_LEVEL = "level"
    DATABASE_LOG_LEVEL = "database_level"
    LOG_OUTPUT_TYPE = "output"
    LOG_DATA_FORMAT = "format"

    DATA_DIRECTORY = "data_directory"

    # Policies, mostly circulation specific
    POLICIES = "policies"

    HOLD_POLICY = "holds"
    HOLD_POLICY_ALLOW = "allow"
    HOLD_POLICY_HIDE = "hide"

    # Integrations
    URL = "url"
    NAME = "name"
    TYPE = "type"
    INTEGRATIONS = "integrations"
    DATABASE_INTEGRATION = "Postgres"
    DATABASE_PRODUCTION_URL = "production_url"
    DATABASE_TEST_URL = "test_url"

    ELASTICSEARCH_INTEGRATION = "Elasticsearch"
    ELASTICSEARCH_INDEX_KEY = "works_index"

    METADATA_WRANGLER_INTEGRATION = "Metadata Wrangler"
    CONTENT_SERVER_INTEGRATION = "Content Server"
    CIRCULATION_MANAGER_INTEGRATION = "Circulation Manager"

    NYT_INTEGRATION = "New York Times"
    NYT_BEST_SELLERS_API_KEY = "best_sellers_api_key"

    OVERDRIVE_INTEGRATION = "Overdrive"
    THREEM_INTEGRATION = "3M"
    AXIS_INTEGRATION = "Axis 360"

    S3_INTEGRATION = "S3"
    S3_ACCESS_KEY = "access_key"
    S3_SECRET_KEY = "secret_key"
    S3_OPEN_ACCESS_CONTENT_BUCKET = "open_access_content_bucket"
    S3_BOOK_COVERS_BUCKET = "book_covers_bucket"

    CDN_INTEGRATION = "CDN"
    CDN_BOOK_COVERS = "book_covers"
    CDN_OPDS_FEEDS = "opds"

    METADATA_WRANGLER_INTEGRATION = "Metadata Wrangler"
    CONTENT_SERVER_INTEGRATION = "Content Server"

    BASE_OPDS_AUTHENTICATION_DOCUMENT = "base_opds_authentication_document"

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
    def link(cls, name):
        """Find a link by name."""
        return cls.get(cls.LINKS, {}).get(name, None)

    @classmethod
    def integration(cls, name, required=False):
        """Find an integration configuration by name."""
        integrations = cls.get(cls.INTEGRATIONS, {})
        v = integrations.get(name, {})
        logging.info("Integration %s: %r", name, v)
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
    def cdn_host(cls, type):
        integration = cls.integration(cls.CDN_INTEGRATION)
        return integration.get(type)

    @classmethod
    def s3_bucket(cls, bucket_name):
        integration = cls.integration(cls.S3_INTEGRATION)
        return integration[bucket_name]

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
    def terms_of_service_url(cls):
        return cls.link(cls.TERMS_OF_SERVICE)

    @classmethod
    def privacy_policy_url(cls):
        return cls.link(cls.PRIVACY_POLICY)

    @classmethod
    def hold_policy(cls):
        return cls.policy(cls.HOLD_POLICY, cls.HOLD_POLICY_ALLOW)

    @classmethod
    def base_opds_authentication_document(cls):
        return cls.get(cls.BASE_OPDS_AUTHENTICATION_DOCUMENT, {})

    @classmethod
    def logging_policy(cls):
        default_logging = {}
        return cls.get(cls.LOGGING, default_logging)

    # Methods for loading the configuration from disk.

    @classmethod
    def load(cls):
        cfv = 'CONFIGURATION_FILE'
        if not cfv in os.environ:
            raise CannotLoadConfiguration(
                "No configuration file defined in %s." % cfv)

        config_path = os.environ[cfv]
        try:
            cls.log.info("Loading configuration from %s", config_path)
            configuration = cls._load(open(config_path))
        except Exception, e:
            raise CannotLoadConfiguration(
                "Error loading configuration file %s: %s" % (
                    config_path, e)
            )
        cls.instance = configuration
        return configuration

    @classmethod
    def _load(cls, fh):
        lines = [x for x in fh if not x.startswith("#")]
        return json.loads("".join(lines))
