import json
import re
from nose.tools import set_trace
import contextlib
from copy import deepcopy

from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP

from flask_babel import lazy_gettext as _

from core.config import (
    Configuration as CoreConfiguration,
    CannotLoadConfiguration,
    IntegrationException,
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

    # The name of the per-library settings that set the maximum amounts
    # of books a patron can have on loan or on hold at once.
    # (Note: depending on distributor settings, a patron may be able
    # to exceed the limits by checking out books directly from a distributor's
    # app. They may also get a limit exceeded error before they reach these
    # limits if a distributor has a smaller limit.)
    LOAN_LIMIT = u"loan_limit"
    HOLD_LIMIT = u"hold_limit"

    # The name of the per-library setting that sets the default email
    # address to use when notifying patrons of changes.
    DEFAULT_NOTIFICATION_EMAIL_ADDRESS = u"default_notification_email_address"

    # The name of the per-library setting that sets the email address
    # of the Designated Agent for copyright complaints
    COPYRIGHT_DESIGNATED_AGENT_EMAIL = u"copyright_designated_agent_email_address"

    # This is the link relation used to indicate
    COPYRIGHT_DESIGNATED_AGENT_REL = "http://librarysimplified.org/rel/designated-agent/copyright"

    # The name of the per-library setting that sets the contact address
    # for problems with the library configuration itself.
    CONFIGURATION_CONTACT_EMAIL = u"configuration_contact_email_address"

    # Name of the site-wide ConfigurationSetting containing the secret
    # used to sign bearer tokens.
    BEARER_TOKEN_SIGNING_SECRET = "bearer_token_signing_secret"

    # Names of per-library ConfigurationSettings that control
    # how detailed the lane configuration gets for various languages.
    LARGE_COLLECTION_LANGUAGES = "large_collections"
    SMALL_COLLECTION_LANGUAGES = "small_collections"
    TINY_COLLECTION_LANGUAGES = "tiny_collections"

    LANGUAGE_DESCRIPTION = _('Each value must be an <a href="https://www.loc.gov/standards/iso639-2/php/code_list.php" target="_blank">ISO-639-2</a> language code.')

    # The color scheme for native mobile applications to use for this library.
    COLOR_SCHEME = "color_scheme"
    DEFAULT_COLOR_SCHEME = "blue"

    # The color options for web applications to use for this library.
    WEB_BACKGROUND_COLOR = "web-background-color"
    WEB_FOREGROUND_COLOR = "web-foreground-color"
    DEFAULT_WEB_BACKGROUND_COLOR = "#000000"
    DEFAULT_WEB_FOREGROUND_COLOR = "#ffffff"

    # Header links and labels for web applications to display for this library.
    # TODO: It's very awkward to have these as separate settings, and separate
    # lists of inputs in the UI.
    WEB_HEADER_LINKS = "web-header-links"
    WEB_HEADER_LABELS = "web-header-labels"

    # The library-wide logo setting.
    LOGO = "logo"

    # Settings for geographic areas associated with the library.
    LIBRARY_FOCUS_AREA = "focus_area"
    LIBRARY_SERVICE_AREA = "service_area"

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

    # Name of the library-wide public key configuration setting for
    # negotiating a shared secret with a library registry. The setting
    # is automatically generated and not editable by admins.
    #
    KEY_PAIR = "key-pair"

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
            "label": _("Cache time for static images and JS and CSS files"),
        },
    ]

    LIBRARY_SETTINGS = CoreConfiguration.LIBRARY_SETTINGS + [
        {
            "key": LIBRARY_DESCRIPTION,
            "label": _("A short description of this library."),
            "description": _("This will be shown to people who aren't sure they've chosen the right library."),
            "optional": True,
        },
        {
            "key": HELP_EMAIL,
            "label": _("Patron support email address"),
            "description": _("An email address a patron can use if they need help, e.g. 'simplyehelp@yourlibrary.org'."),
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
            "key": COPYRIGHT_DESIGNATED_AGENT_EMAIL,
            "label": _("Copyright designated agent email"),
            "description": _("Patrons of this library should use this email address to send a DMCA notification (or other copyright complaint) to the library.<br/>If no value is specified here, the general patron support address will be used."),
            "optional": True,
        },
        {
            "key": CONFIGURATION_CONTACT_EMAIL,
            "label": _("A point of contact for the organization reponsible for configuring this library."),
            "description": _("This email address will be shared as part of integrations that you set up through this interface. It will not be shared with the general public. This gives the administrator of the remote integration a way to contact you about problems with this library's use of that integration.<br/>If no value is specified here, the general patron support address will be used."),
            "optional": True,
        },
        {
            "key": DEFAULT_NOTIFICATION_EMAIL_ADDRESS,
            "label": _("Default email address to use when sending vendor hold notifications"),
            "description": _('This should be an address controlled by the library which rejects or trashes all email sent to it. Vendor hold notifications contain sensitive patron information, but <a href="https://confluence.nypl.org/display/SIM/About+Hold+Notifications" target="_blank">cannot be forwarded to patrons</a> because they contain vendor-specific instructions.')
        },
        {
            "key": COLOR_SCHEME,
            "label": _("Mobile color scheme"),
            "description": _("This tells mobile applications what color scheme to use when rendering this library's OPDS feed."),
            "options": [
                dict(key="amber", label=_("Amber")),
                dict(key="black", label=_("Black")),
                dict(key="blue", label=_("Blue")),
                dict(key="bluegray", label=_("Blue Gray")),
                dict(key="brown", label=_("Brown")),
                dict(key="cyan", label=_("Cyan")),
                dict(key="darkorange", label=_("Dark Orange")),
                dict(key="darkpurple", label=_("Dark Purple")),
                dict(key="green", label=_("Green")),
                dict(key="gray", label=_("Gray")),
                dict(key="indigo", label=_("Indigo")),
                dict(key="lightblue", label=_("Light Blue")),
                dict(key="orange", label=_("Orange")),
                dict(key="pink", label=_("Pink")),
                dict(key="purple", label=_("Purple")),
                dict(key="red", label=_("Red")),
                dict(key="teal", label=_("Teal")),
            ],
            "type": "select",
            "default": DEFAULT_COLOR_SCHEME,
        },
        {
            "key": WEB_BACKGROUND_COLOR,
            "label": _("Web background color"),
            "description": _("This tells web applications what background color to use. Must have sufficient contrast with the foreground color."),
            "type": "color-picker",
            "default": DEFAULT_WEB_BACKGROUND_COLOR,
        },
        {
            "key": WEB_FOREGROUND_COLOR,
            "label": _("Web foreground color"),
            "description": _("This tells web applications what foreground color to use. Must have sufficient contrast with the background color."),
            "type": "color-picker",
            "default": DEFAULT_WEB_FOREGROUND_COLOR,
        },
        {
            "key": WEB_HEADER_LINKS,
            "label": _("Web header links"),
            "description": _("This gives web applications a list of links to display in the header. Specify labels for each link in the same order under 'Web header labels'."),
            "type": "list",
            "optional": True
        },
        {
            "key": WEB_HEADER_LABELS,
            "label": _("Web header labels"),
            "description": _("Labels for each link under 'Web header links'."),
            "type": "list",
            "optional": True
        },
        {
            "key": LOGO,
            "label": _("Logo image"),
            "type": "image",
            "optional": True,
            "description": _("The image must be in GIF, PNG, or JPG format, approximately square, no larger than 135x135 pixels, and look good on a white background."),
        },
        {
            "key": LIBRARY_FOCUS_AREA,
            "label": _("Focus area"),
            "type": "text",
            "optional": True,
            "description": _("The library focuses on serving patrons in this geographic area. In most cases this will be a city name like <code>Springfield, OR</code>."),
        },
        {
            "key": LIBRARY_SERVICE_AREA,
            "label": _("Service area"),
            "type": "text",
            "optional": True,
            "description": _("The full geographic area served by this library. In most cases this is the same as the focus area and can be left blank, but it may be a larger area such as a US state (which should be indicated by its abbreviation, like <code>OR</code>)."),
        },
        {
            "key": MAX_OUTSTANDING_FINES,
            "label": _("Maximum amount of fines a patron can have before losing lending privileges"),
            "optional": True,
        },
        {
            "key": LOAN_LIMIT,
            "label": _("Maximum number of books a patron can have on loan at once."),
            "description": _("(Note: depending on distributor settings, a patron may be able to exceed the limit by checking out books directly from a distributor's app. They may also get a limit exceeded error before they reach these limits if a distributor has a smaller limit.)"),
            "type": "number",
            "optional": True,
        },
        {
            "key": HOLD_LIMIT,
            "label": _("Maximum number of books a patron can have on hold at once."),
            "description": _("(Note: depending on distributor settings, a patron may be able to exceed the limit by checking out books directly from a distributor's app. They may also get a limit exceeded error before they reach these limits if a distributor has a smaller limit.)"),
            "type": "number",
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
            "key": LARGE_COLLECTION_LANGUAGES,
            "label": _("The primary languages represented in this library's collection"),
            "type": "list",
            "description": LANGUAGE_DESCRIPTION,
            "optional": True
        },
        {
            "key": SMALL_COLLECTION_LANGUAGES,
            "label": _("Other major languages represented in this library's collection"),
            "type": "list",
            "description": LANGUAGE_DESCRIPTION,
            "optional": True,
        },
        {
            "key": TINY_COLLECTION_LANGUAGES,
            "label": _("Other languages in this library's collection"),
            "type": "list",
            "description": LANGUAGE_DESCRIPTION,
            "optional": True,
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
    def _as_mailto(cls, value):
        """Turn an email address into a mailto: URI."""
        if not value:
            return value
        if value.startswith("mailto:"):
            return value
        return "mailto:%s" % value

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
                value = cls._as_mailto(value)
            if name == cls.HELP_WEB:
                type = 'text/html'
            yield type, value

    @classmethod
    def _email_uri_with_fallback(cls, library, key):
        """Try to find a certain email address configured for the given
        purpose. If not available, use the general patron support
        address.

        :param key: The specific email address to look for.
        """
        for setting in [key, Configuration.HELP_EMAIL]:
            value = ConfigurationSetting.for_library(setting, library).value
            if not value:
                continue
            return cls._as_mailto(value)

    @classmethod
    def copyright_designated_agent_uri(cls, library):
        return cls._email_uri_with_fallback(
            library, Configuration.COPYRIGHT_DESIGNATED_AGENT_EMAIL
        )

    @classmethod
    def configuration_contact_uri(cls, library):
        return cls._email_uri_with_fallback(
            library, Configuration.CONFIGURATION_CONTACT_EMAIL
        )

    @classmethod
    def key_pair(cls, setting):
        """Look up a public-private key pair in a ConfigurationSetting.

        If the value is missing or incorrect, a new key pair is
        created and stored.

        TODO: This could go into ConfigurationSetting or core Configuration.

        :param public_setting: A ConfigurationSetting for the public key.
        :param private_setting: A ConfigurationSetting for the private key.

        :return: A 2-tuple (public key, private key)
        """
        public = None
        private = None

        try:
            public, private = setting.json_value
        except Exception, e:
            pass

        if not public or not private:
            key = RSA.generate(2048)
            encryptor = PKCS1_OAEP.new(key)
            public = key.publickey().exportKey()
            private = key.exportKey()
            setting.value = json.dumps([public, private])
        return public, private

    @classmethod
    def cipher(cls, key):
        """Create a Cipher for a public or private key.

        This just wraps some hard-to-remember Crypto code.

        :param key: A string containing the key.

        :return: A Cipher object which will support either
        encrypt() (public key) or decrypt() (private key).
        """
        return PKCS1_OAEP.new(RSA.import_key(key))


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
