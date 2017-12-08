import json
import re
from nose.tools import set_trace
import contextlib
from copy import deepcopy
from flask_babel import lazy_gettext as _
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

    DEFAULT_OPDS_FORMAT = "simple_opds_entry"

    # The name of the sitewide url that points to the patron web catalog.
    PATRON_WEB_CLIENT_URL = u"Patron Web Client"

    # The name of the sitewide secret used to sign cookies for admin login.
    SECRET_KEY = u"secret_key"

    # The name of the setting that controls how long static files are cached.
    STATIC_FILE_CACHE_TIME = u"static_file_cache_time"

    # A short description of the library, used in its Authentication
    # for OPDS document.
    LIBRARY_DESCRIPTION = 'library_description'
    
    # The name of the per-library setting that sets the maximum amount
    # of fines a patron can have before losing lending privileges.
    MAX_OUTSTANDING_FINES = u"max_outstanding_fines"

    # The name of the per-library setting that sets the default email
    # address to use when notifying patrons of changes.
    DEFAULT_NOTIFICATION_EMAIL_ADDRESS = u"default_notification_email_address"

    # Name of the site-wide ConfigurationSetting containing the secret
    # used to sign bearer tokens.
    BEARER_TOKEN_SIGNING_SECRET = "bearer_token_signing_secret"

    # Names of per-library ConfigurationSettings that control
    # how detailed the lane configuration gets for various languages.
    LARGE_COLLECTION_LANGUAGES = "large_collections"
    SMALL_COLLECTION_LANGUAGES = "small_collections"
    TINY_COLLECTION_LANGUAGES = "tiny_collections"

    # The client-side color scheme to use for this library.
    COLOR_SCHEME = "color_scheme"
    DEFAULT_COLOR_SCHEME = "blue"

    # The library-wide logo setting.
    LOGO = "logo"
   
    # Names of the library-wide link settings.
    TERMS_OF_SERVICE = 'terms-of-service'
    PRIVACY_POLICY = 'privacy-policy'
    COPYRIGHT = 'copyright'
    ABOUT = 'about'
    LICENSE = 'license'
    REGISTER = 'register'

    # A library with this many titles in a given language will be given
    # a large, detailed lane configuration for that language.
    LARGE_COLLECTION_CUTOFF = 10000
    # A library with this many titles in a given language will be
    # given separate fiction and nonfiction lanes for that language.
    SMALL_COLLECTION_CUTOFF = 500
    # A library with fewer titles than that will be given a single
    # lane containing all books in that language.

    # These are link relations that are valid in Authentication for
    # OPDS documents but are not registered with IANA.
    AUTHENTICATION_FOR_OPDS_LINKS = ['register']
    
    # We support three different ways of integrating help processes.
    # All three of these will be sent out as links with rel='help'
    HELP_EMAIL = 'help-email'
    HELP_WEB = 'help-web'
    HELP_URI = 'help-uri'
    HELP_LINKS = [HELP_EMAIL, HELP_WEB, HELP_URI]

    # Features of an OPDS client which a library may want to enable or
    # disable.
    RESERVATIONS_FEATURE = "https://librarysimplified.org/rel/policy/reservations"

    # Name of the library-wide public key configuration setting for negotiating
    # a shared secret with a library registry. The setting is automatically generated
    # and not editable by admins.
    PUBLIC_KEY = "public-key"
    
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
        {
            "key": STATIC_FILE_CACHE_TIME,
            "label": _("Cache time for static JS and CSS files for the admin interface"),
        },
    ]

    LIBRARY_SETTINGS = CoreConfiguration.LIBRARY_SETTINGS + [
        {
            "key": LIBRARY_DESCRIPTION,
            "label": _("A short description of this library, shown to people who aren't sure they've chosen the right library."),
            "optional": True,
        },
        {
            "key": DEFAULT_NOTIFICATION_EMAIL_ADDRESS,
            "label": _("Default email address to use when notifying patrons of changes."),
            "description": _("This should be an address that the library controls, but no emails will (currently) be sent to this address. If this address is not specified, no holds can be placed on Overdrive, Bibliotheca, or Axis 360 titles, and no RBdigital titles can be put on loan.")
        },
        {
            "key": COLOR_SCHEME,
            "label": _("Color scheme"),
            "description": _("This tells clients what colors to use when rendering this library's OPDS feed."),
            "options": [
                { "key": "blue", "label": _("Blue") },
                { "key": "red", "label": _("Red") },
                { "key": "gray", "label": _("Gray") },
                { "key": "gold", "label": _("Gold") },
                { "key": "green", "label": _("Green") },
                { "key": "teal", "label": _("Teal") },
                { "key": "purple", "label": _("Purple") },
            ],
            "type": "select",
            "default": DEFAULT_COLOR_SCHEME,
        },
        {
            "key": LOGO,
            "label": _("Logo image"),
            "type": "image",
            "optional": True,
            "description": _("The image must be in GIF, PNG, or JPG format, approximately square, no larger than 135x135 pixels, and look good on a white background."),
        },
        {
            "key": MAX_OUTSTANDING_FINES,
            "label": _("Maximum amount of fines a patron can have before losing lending privileges"),
            "optional": True,
        },
        {
            "key": TERMS_OF_SERVICE,
            "label": _("Terms of Service URL"),
            "optional": True,
        },
        {
            "key": PRIVACY_POLICY,
            "label": _("Privacy Policy URL"),
            "optional": True,
        },
        {
            "key": COPYRIGHT,
            "label": _("Copyright URL"),
            "optional": True,
        },
        {
            "key": ABOUT,
            "label": _("About URL"),
            "optional": True,
        },
        {
            "key": LICENSE,
            "label": _("License URL"),
            "optional": True,
        },
        {
            "key": REGISTER,
            "label": _("Patron registration URL"),
            "description": _("A URL where someone who doesn't have a library card yet can sign up for one."),
            "optional": True,
        },
        {
            "key": HELP_EMAIL,
            "label": _("Patron support email address"),
            "description": _("An email address a patron can use if they need help, e.g. 'simplyehelp@nypl.org'."),
            "optional": True,
        },
        {
            "key": HELP_WEB,
            "label": _("Patron support web site"),
            "description": _("A URL for patrons to get help."),
            "optional": True,
        },
        {
            "key": HELP_URI,
            "label": _("Patron support custom integration URI"),
            "description": _("A custom help integration like Helpstack, e.g. 'helpstack:nypl.desk.com'."),
            "optional": True,
        },
        {
            "key": LARGE_COLLECTION_LANGUAGES,
            "label": _("The primary languages represented in this library's collection"),
            "type": "list",
        },
        {
            "key": SMALL_COLLECTION_LANGUAGES,
            "label": _("Other major languages represented in this library's collection"),
            "type": "list",
        },        
        {
            "key": TINY_COLLECTION_LANGUAGES,
            "label": _("Other languages in this library's collection"),
            "type": "list",
        },        
    ]
    
    @classmethod
    def lending_policy(cls):
        return cls.policy(cls.LENDING_POLICY)

    @classmethod
    def _collection_languages(cls, library, key):
        """Look up a list of languages in a library configuration.

        If the value is not set, estimate a value (and all related
        values) by looking at the library's collection.
        """
        setting = ConfigurationSetting.for_library(key, library)
        value = None
        try:
            value = setting.json_value
            if not isinstance(value, list):
                value = None
        except (TypeError, ValueError):
            pass

        if value is None:
            # We have no value or a bad value. Estimate a better value.
            cls.estimate_language_collections_for_library(library)
            value = setting.json_value
        return value
    
    @classmethod
    def large_collection_languages(cls, library):
        return cls._collection_languages(
            library, cls.LARGE_COLLECTION_LANGUAGES
        )

    @classmethod
    def small_collection_languages(cls, library):
        return cls._collection_languages(
            library, cls.SMALL_COLLECTION_LANGUAGES
        )

    @classmethod
    def tiny_collection_languages(cls, library):
        return cls._collection_languages(
            library, cls.TINY_COLLECTION_LANGUAGES
        )

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
        return cls.instance
        
    @classmethod
    def estimate_language_collections_for_library(cls, library):
        """Guess at appropriate values for the given library for
        LARGE_COLLECTION_LANGUAGES, SMALL_COLLECTION_LANGUAGES, and
        TINY_COLLECTION_LANGUAGES. Set configuration values
        appropriately, overriding any previous values.
        """
        holdings = library.estimated_holdings_by_language()
        large, small, tiny = cls.classify_holdings(holdings)
        for setting, value in (
                (cls.LARGE_COLLECTION_LANGUAGES, large),
                (cls.SMALL_COLLECTION_LANGUAGES, small),
                (cls.TINY_COLLECTION_LANGUAGES, tiny),
        ):
            ConfigurationSetting.for_library(
                setting, library).value = json.dumps(value)

    @classmethod
    def classify_holdings(cls, works_by_language):
        """Divide languages into 'large', 'small', and 'tiny' colletions based
        on the number of works available for each.

        :param works_by_language: A Counter mapping languages to the
        number of active works available for that language.  The
        output of `Library.estimated_holdings_by_language` is a good
        thing to pass in.

        :return: a 3-tuple of lists (large, small, tiny).
        """
        large = []
        small = []
        tiny = []
        result = [large, small, tiny]

        if not works_by_language:
            # In the absence of any information, assume we have an
            # English collection and nothing else.
            large.append('eng')
            return result
        
        # The single most common language always gets a large
        # collection.
        #
        # Otherwise, it depends on how many works are in the
        # collection.
        for language, num_works in works_by_language.most_common():
            if not large:
                bucket = large
            elif num_works >= cls.LARGE_COLLECTION_CUTOFF:
                bucket = large
            elif num_works >= cls.SMALL_COLLECTION_CUTOFF:
                bucket = small
            else:
                bucket = tiny
            bucket.append(language)
            
        return result        
        
    @classmethod
    def help_uris(cls, library):
        """Find all the URIs that might help patrons get help from
        this library.

        :yield: A sequence of 2-tuples (media type, URL)
        """
        for name in cls.HELP_LINKS:
            setting = ConfigurationSetting.for_library(name, library)
            value = setting.value
            if not value:
                continue
            type = None
            if name == cls.HELP_EMAIL:
                value = 'mailto:' + value
            if name == cls.HELP_WEB:
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
