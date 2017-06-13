import re
from nose.tools import set_trace
import contextlib
from copy import deepcopy
from core.config import (
    Configuration as CoreConfiguration,
    CannotLoadConfiguration,
    empty_config as core_empty_config,
    temp_config as core_temp_config,
)
from core.util import MoneyUtility
from core.lane import Facets
from core.model import ConfigurationSetting


class Configuration(CoreConfiguration):

    LENDING_POLICY = "lending"
    LANGUAGE_POLICY = "languages"
    LARGE_COLLECTION_LANGUAGES = "large_collections"
    SMALL_COLLECTION_LANGUAGES = "small_collections"
    TINY_COLLECTION_LANGUAGES = "tiny_collections"

    DEFAULT_OPDS_FORMAT = "simple_opds_entry"

    ROOT_LANE_POLICY = "root_lane"

    # The name of the sitewide url that points to the patron web catalog.
    PATRON_WEB_CLIENT_URL = u"Patron Web Client"

    # The name of the sitewide secret used to sign cookies for admin login.
    SECRET_KEY = u"secret_key"

    # The name of the per-library setting that sets the maximum amount
    # of fines a patron can have before losing lending privileges.
    MAX_OUTSTANDING_FINES = u"max_outstanding_fines"

    # The name of the per-library setting that sets the default email
    # address to use when notifying patrons of changes.
    DEFAULT_NOTIFICATION_EMAIL_ADDRESS = u"default_notification_email_address"
    
    @classmethod
    def lending_policy(cls):
        return cls.policy(cls.LENDING_POLICY)

    @classmethod
    def root_lane_policy(cls):
        return cls.policy(cls.ROOT_LANE_POLICY)

    @classmethod
    def language_policy(cls):
        return cls.policy(cls.LANGUAGE_POLICY, required=True)

    @classmethod
    def large_collection_languages(cls):
        value = cls.language_policy().get(cls.LARGE_COLLECTION_LANGUAGES, 'eng')
        if not value:
            return []
        if isinstance(value, list):
            return value
        return [[x] for x in value.split(',')]

    @classmethod
    def small_collection_languages(cls):
        import logging
        logging.info("In small_collection_languages.")
        value = cls.language_policy().get(cls.SMALL_COLLECTION_LANGUAGES, '')
        logging.info("Language policy: %r" % cls.language_policy())
        logging.info("Small collections: %r" % value)
        if not value:
            return []
        if isinstance(value, list):
            return value
        return [[x] for x in value.split(',')]

    @classmethod
    def tiny_collection_languages(cls):
        import logging
        logging.info("In tiny_collection_languages.")
        value = cls.language_policy().get(cls.TINY_COLLECTION_LANGUAGES, '')
        logging.info("Language policy: %r" % cls.language_policy())
        logging.info("Tiny collections: %r" % value)
        if not value:
            return []
        if isinstance(value, list):
            return value
        return [[x] for x in value.split(',')]

    @classmethod
    def max_outstanding_fines(cls, library):
        max_fines = ConfigurationSetting.for_library(
            cls.MAX_OUTSTANDING_FINES, library
        ).value
        return MoneyUtility.parse(max_fines)
    
    @classmethod
    def load(cls):
        CoreConfiguration.load()
        cls.instance = CoreConfiguration.instance

@contextlib.contextmanager
def empty_config():
    with core_empty_config({}, [CoreConfiguration, Configuration]) as i:
        yield i

@contextlib.contextmanager
def temp_config(new_config=None, replacement_classes=None):
    all_replacement_classes = [CoreConfiguration, Configuration]
    if replacement_classes:
        all_replacement_classes.extend(replacement_classes)
    with core_temp_config(new_config, all_replacement_classes) as i:
        yield i
