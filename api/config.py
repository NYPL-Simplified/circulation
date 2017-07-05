import re
from nose.tools import set_trace
import contextlib
from copy import deepcopy
from flask.ext.babel import lazy_gettext as _
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

    # Name of the site-wide ConfigurationSetting containing the secret
    # used to sign bearer tokens.
    BEARER_TOKEN_SIGNING_SECRET = "bearer_token_signing_secret"

    # The client-side color scheme to use for this library.
    COLOR_SCHEME = "color_scheme"
    DEFAULT_COLOR_SCHEME = "blue"
    
    # Names of the library-wide link settings.
    TERMS_OF_SERVICE = 'terms-of-service'
    PRIVACY_POLICY = 'privacy-policy'
    COPYRIGHT = 'copyright'
    ABOUT = 'about'
    LICENSE = 'license'

    # We support three different ways of integrating help processes.
    # All three of these will be sent out as links with rel='help'
    HELP_EMAIL = 'help-email'
    HELP_WEB = 'help-web'
    HELP_URI = 'help-uri'
    HELP_LINKS = [HELP_EMAIL, HELP_WEB, HELP_URI]
    
    SITEWIDE_SETTINGS = CoreConfiguration.SITEWIDE_SETTINGS + [
        {
            "key": BEARER_TOKEN_SIGNING_SECRET,
            "label": _("Internal signing secret for OAuth bearer tokens"),
        },
        {
            "key": SECRET_KEY,
            "label": _("Internal secret key for admin interface cookies"),
        },
        {
            "key": PATRON_WEB_CLIENT_URL,
            "label": _("URL of the web catalog for patrons"),
        },
    ]

    LIBRARY_SETTINGS = CoreConfiguration.LIBRARY_SETTINGS + [
        {
            "key": COLOR_SCHEME,
            "label": _("Color scheme"),
            "options": [
                { "key": "blue", "label": _("Blue") },
                { "key": "red", "label": _("Red") },
                { "key": "gray", "label": _("Gray") },
                { "key": "gold", "label": _("Gold") },
                { "key": "green", "label": _("Green") },
                { "key": "teal", "label": _("Teal") },
                { "key": "purple", "label": _("Purple") },
            ],
        },
        {
            "key": MAX_OUTSTANDING_FINES,
            "label": _("Maximum amount of fines a patron can have before losing lending privileges"),
        },
        {
            "key": DEFAULT_NOTIFICATION_EMAIL_ADDRESS,
            "label": _("Default email address to use when notifying patrons of changes"),
        },
        {
            "key": TERMS_OF_SERVICE,
            "label": _("Terms of Service URL"),
        },
        {
            "key": PRIVACY_POLICY,
            "label": _("Privacy Policy URL"),
        },
        {
            "key": COPYRIGHT,
            "label": _("Copyright URL"),
        },
        {
            "key": ABOUT,
            "label": _("About URL"),
        },
        {
            "key": LICENSE,
            "label": _("License URL"),
        },
        {
            "key": HELP_EMAIL,
            "label": _("Patron support email address"),
        },
        {
            "key": HELP_WEB,
            "label": _("Patron support web site"),
        },
        {
            "key": HELP_URI,
            "label": _("Patron support custom integration URI")
        },
    ]
    
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
    def load(cls, _db=None):
        CoreConfiguration.load(_db)
        cls.instance = CoreConfiguration.instance

    @classmethod
    def help_uris(cls, library):
        """Find all the URIs that might help patrons get help from
        this library.

        :yield: A sequence of 2-tuples (media type, URL)
        """
        for name in self.HELP_LINKS:
            setting = ConfigurationSetting.for_library(name, self.library)
            value = setting.value
            if not value:
                continue
            type = None
            if name == self.HELP_EMAIL:
                value = 'mailto:' + value
            if name == self.HELP_WEB:
                type = 'text/html'
            yield type, value
            
        
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
