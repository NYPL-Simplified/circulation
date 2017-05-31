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

class Configuration(CoreConfiguration):

    INCLUDE_ADMIN_INTERFACE = "include_admin_interface"
    LENDING_POLICY = "lending"
    AUTHENTICATION_POLICY = "authentication"
    LANGUAGE_POLICY = "languages"
    LANGUAGE_FORCE = "force"
    LARGE_COLLECTION_LANGUAGES = "large_collections"
    SMALL_COLLECTION_LANGUAGES = "small_collections"
    TINY_COLLECTION_LANGUAGES = "tiny_collections"

    DEFAULT_OPDS_FORMAT = "simple_opds_entry"

    ROOT_LANE_POLICY = "root_lane"
    EXTERNAL_TYPE_REGULAR_EXPRESSION = "external_type_regular_expression"

    MAX_OUTSTANDING_FINES = "max_outstanding_fines"

    PRELOADED_CONTENT = "preloaded_content"

    ADOBE_VENDOR_ID_INTEGRATION = "Adobe Vendor ID"
    ADOBE_VENDOR_ID = "vendor_id"
    ADOBE_VENDOR_ID_NODE_VALUE = "node_value"

    AUTHENTICATION = "authentication"
    AUTHENTICATION_TEST_USERNAME = "test_username"
    AUTHENTICATION_TEST_PASSWORD = "test_password"

    OAUTH_CLIENT_ID = 'client_id'
    OAUTH_CLIENT_SECRET = 'client_secret'
    OAUTH_TOKEN_EXPIRATION_DAYS = 'token_expiration_days'
    SECRET_KEY = "secret_key"

    MILLENIUM_INTEGRATION = "Millenium"
    MILLENIUM_MODE = "auth_mode"
    AUTHORIZATION_IDENTIFIER_BLACKLIST = "authorization_identifier_blacklist"
    STAFF_PICKS_INTEGRATION = "Staff Picks"
    PATRON_WEB_CLIENT_INTEGRATION = "Patron Web Client"

    LIST_FIELDS = "fields"
   
    DEFAULT_NOTIFICATION_EMAIL_ADDRESS = "default_notification_email_address"

    IDENTIFIER_REGULAR_EXPRESSION = "identifier_regular_expression"
    PASSWORD_REGULAR_EXPRESSION = "password_regular_expression"

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
    def force_language(cls, language):
        """Override normal language settings to deliver a particular
        collection no matter what.
        """
        policy = cls.language_policy()
        return policy.get(cls.LANGUAGE_FORCE, language)

    @classmethod
    def default_notification_email_address(cls):
        return cls.required(cls.DEFAULT_NOTIFICATION_EMAIL_ADDRESS)

    @classmethod
    def max_outstanding_fines(cls):
        max_fines = Configuration.policy(
            Configuration.MAX_OUTSTANDING_FINES
        )
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

class FacetConfig(object):
    """A class that implements the facet-related methods of
    Configuration, and allows modifications to the enabled
    and default facets. For use when a controller needs to
    use a facet configuration different from the site-wide
    facets. 
    """
    @classmethod
    def from_config(cls):
        facet_policy = Configuration.policy(Configuration.FACET_POLICY, default=dict())
        enabled_facets = deepcopy(facet_policy.get(Configuration.ENABLED_FACETS_KEY,
                                               Configuration.DEFAULT_ENABLED_FACETS))
        default_facets = deepcopy(facet_policy.get(Configuration.DEFAULT_FACET_KEY,
                                               Configuration.DEFAULT_FACET))
        return FacetConfig(enabled_facets, default_facets)

    def __init__(self, enabled_facets, default_facets):
        self._enabled_facets = enabled_facets
        self._default_facets = default_facets

    def enabled_facets(self, group_name):
        return self._enabled_facets.get(group_name)

    def default_facet(self, group_name):
        return self._default_facets.get(group_name)

    def enable_facet(self, group_name, facet):
        self._enabled_facets.setdefault(group_name, [])
        if facet not in self._enabled_facets[group_name]:
            self._enabled_facets[group_name] += [facet]

    def set_default_facet(self, group_name, facet):
        self.enable_facet(group_name, facet)
        self._default_facets[group_name] = facet


