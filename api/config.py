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

    ADOBE_VENDOR_ID_INTEGRATION = u"Adobe Vendor ID"
    ADOBE_VENDOR_ID = u"vendor_id"
    ADOBE_VENDOR_ID_NODE_VALUE = u"node_value"

    PATRON_WEB_CLIENT_INTEGRATION = u"Patron Web Client"

    # The name of the sitewide secret used to sign cookies for admin login.
    SECRET_KEY = "secret_key"

    # The name of the per-library setting that sets the maximum amount
    # of fines a patron can have before losing lending privileges.
    MAX_OUTSTANDING_FINES = "max_outstanding_fines"

    # The name of the per-library setting that sets the default email
    # address to use when notifying patrons of changes.
    DEFAULT_NOTIFICATION_EMAIL_ADDRESS = "default_notification_email_address"
    
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

class FacetConfig(object):
    """A class that implements the facet-related methods of
    Library, and allows modifications to the enabled
    and default facets. For use when a controller needs to
    use a facet configuration different from the site-wide
    facets. 
    """
    @classmethod
    def from_library(cls, library):

        enabled_facets = dict()
        for group in Facets.DEFAULT_ENABLED_FACETS.keys():
            enabled_facets[group] = library.enabled_facets(group)

        default_facets = dict()
        for group in Facets.DEFAULT_FACET.keys():
            default_facets[group] = library.default_facet(group)
        
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
        """Add `facet` to the list of possible values for `group_name`, even
        if the library does not have that facet configured.
        """
        self.enable_facet(group_name, facet)
        self._default_facets[group_name] = facet


