# encoding: utf-8
# ExternalIntegration, ExternalIntegrationLink, ConfigurationSetting
import inspect
import json
import logging
from abc import abstractmethod, ABCMeta
from contextlib import contextmanager

from enum import Enum
from flask_babel import lazy_gettext as _
from sqlalchemy import (
    Column,
    ForeignKey,
    Integer,
    Unicode,
    UniqueConstraint,
)
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship
from sqlalchemy.orm.session import Session

from constants import DataSourceConstants
from hasfulltablecache import HasFullTableCache
from library import Library
from . import (
    Base,
    get_one,
    get_one_or_create,
)
from ..config import (
    CannotLoadConfiguration,
    Configuration,
)
from ..mirror import MirrorUploader
from ..util.string_helpers import random_string


class ExternalIntegrationLink(Base, HasFullTableCache):

    __tablename__ = 'externalintegrationslinks'

    NO_MIRROR_INTEGRATION = u"NO_MIRROR"
    # Possible purposes that a storage external integration can be used for.
    # These string literals may be stored in the database, so changes to them
    # may need to be accompanied by a DB migration.
    COVERS = 'covers_mirror'
    COVERS_KEY = '{0}_integration_id'.format(COVERS)

    OPEN_ACCESS_BOOKS = 'books_mirror'
    OPEN_ACCESS_BOOKS_KEY = '{0}_integration_id'.format(OPEN_ACCESS_BOOKS)

    PROTECTED_ACCESS_BOOKS = 'protected_access_books_mirror'
    PROTECTED_ACCESS_BOOKS_KEY = '{0}_integration_id'.format(PROTECTED_ACCESS_BOOKS)

    MARC = "MARC_mirror"

    id = Column(Integer, primary_key=True)
    external_integration_id = Column(
        Integer, ForeignKey('externalintegrations.id'), index=True
    )
    library_id = Column(
        Integer, ForeignKey('libraries.id'), index=True
    )
    other_integration_id = Column(
        Integer, ForeignKey('externalintegrations.id'), index=True
    )
    purpose = Column(Unicode, index=True)

    mirror_settings = [
        {
            'key': COVERS_KEY,
            'type': COVERS,
            'description_type': 'cover images',
            'label': 'Covers Mirror'
        },
        {
            'key': OPEN_ACCESS_BOOKS_KEY,
            'type': OPEN_ACCESS_BOOKS,
            'description_type': 'free books',
            'label': 'Open Access Books Mirror'
        },
        {
            'key': PROTECTED_ACCESS_BOOKS_KEY,
            'type': PROTECTED_ACCESS_BOOKS,
            'description_type': 'self-hosted, commercially licensed books',
            'label': 'Protected Access Books Mirror'
        }
    ]
    settings = []

    for mirror_setting in mirror_settings:
        mirror_type = mirror_setting['type']
        mirror_description_type = mirror_setting['description_type']
        mirror_label = mirror_setting['label']

        settings.append({
            'key': '{0}_integration_id'.format(mirror_type.lower()),
            'label': _(mirror_label),
            "description": _('Any {0} encountered while importing content from this collection '
                             'can be mirrored to a server you control.'.format(mirror_description_type)),
            'type': 'select',
            'options': [
                {
                    'key': NO_MIRROR_INTEGRATION,
                    'label': _('None - Do not mirror {0}'.format(mirror_description_type))
                }
            ]
        })

    COLLECTION_MIRROR_SETTINGS = settings


class ExternalIntegration(Base, HasFullTableCache):

    """An external integration contains configuration for connecting
    to a third-party API.
    """

    # Possible goals of ExternalIntegrations.
    #
    # These integrations are associated with external services such as
    # Google Enterprise which authenticate library administrators.
    ADMIN_AUTH_GOAL = u'admin_auth'

    # These integrations are associated with external services such as
    # SIP2 which authenticate library patrons. Other constants related
    # to this are defined in the circulation manager.
    PATRON_AUTH_GOAL = u'patron_auth'

    # These integrations are associated with external services such
    # as Overdrive which provide access to books.
    LICENSE_GOAL = u'licenses'

    # These integrations are associated with external services such as
    # the metadata wrangler, which provide information about books,
    # but not the books themselves.
    METADATA_GOAL = u'metadata'

    # These integrations are associated with external services such as
    # S3 that provide access to book covers.
    STORAGE_GOAL = MirrorUploader.STORAGE_GOAL

    # These integrations are associated with external services like
    # Cloudfront or other CDNs that mirror and/or cache certain domains.
    CDN_GOAL = u'CDN'

    # These integrations are associated with external services such as
    # Elasticsearch that provide indexed search.
    SEARCH_GOAL = u'search'

    # These integrations are associated with external services such as
    # Google Analytics, which receive analytics events.
    ANALYTICS_GOAL = u'analytics'

    # These integrations are associated with external services such as
    # Adobe Vendor ID, which manage access to DRM-dependent content.
    DRM_GOAL = u'drm'

    # These integrations are associated with external services that
    # help patrons find libraries.
    DISCOVERY_GOAL = u'discovery'

    # These integrations are associated with external services that
    # collect logs of server-side events.
    LOGGING_GOAL = u'logging'

    # These integrations are associated with external services that
    # a library uses to manage its catalog.
    CATALOG_GOAL = u'ils_catalog'

    # Supported protocols for ExternalIntegrations with LICENSE_GOAL.
    OPDS_IMPORT = u'OPDS Import'
    OPDS2_IMPORT = u'OPDS 2.0 Import'
    OVERDRIVE = DataSourceConstants.OVERDRIVE
    ODILO = DataSourceConstants.ODILO
    BIBLIOTHECA = DataSourceConstants.BIBLIOTHECA
    AXIS_360 = DataSourceConstants.AXIS_360
    RB_DIGITAL = DataSourceConstants.RB_DIGITAL
    ONE_CLICK = RB_DIGITAL
    OPDS_FOR_DISTRIBUTORS = u'OPDS for Distributors'
    ENKI = DataSourceConstants.ENKI
    FEEDBOOKS = DataSourceConstants.FEEDBOOKS
    LCP = DataSourceConstants.LCP
    MANUAL = DataSourceConstants.MANUAL

    # These protocols were used on the Content Server when mirroring
    # content from a given directory or directly from Project
    # Gutenberg, respectively. DIRECTORY_IMPORT was replaced by
    # MANUAL.  GUTENBERG has yet to be replaced, but will eventually
    # be moved into LICENSE_PROTOCOLS.
    DIRECTORY_IMPORT = "Directory Import"
    GUTENBERG = DataSourceConstants.GUTENBERG

    LICENSE_PROTOCOLS = [
        OPDS_IMPORT, OVERDRIVE, ODILO, BIBLIOTHECA, AXIS_360, RB_DIGITAL,
        GUTENBERG, ENKI, MANUAL
    ]

    # Some integrations with LICENSE_GOAL imply that the data and
    # licenses come from a specific data source.
    DATA_SOURCE_FOR_LICENSE_PROTOCOL = {
        OVERDRIVE : DataSourceConstants.OVERDRIVE,
        ODILO : DataSourceConstants.ODILO,
        BIBLIOTHECA : DataSourceConstants.BIBLIOTHECA,
        AXIS_360 : DataSourceConstants.AXIS_360,
        RB_DIGITAL : DataSourceConstants.RB_DIGITAL,
        ENKI : DataSourceConstants.ENKI,
        FEEDBOOKS : DataSourceConstants.FEEDBOOKS,
    }

    # Integrations with METADATA_GOAL
    BIBBLIO = u'Bibblio'
    CONTENT_CAFE = u'Content Cafe'
    NOVELIST = u'NoveList Select'
    NYPL_SHADOWCAT = u'Shadowcat'
    NYT = u'New York Times'
    METADATA_WRANGLER = u'Metadata Wrangler'
    CONTENT_SERVER = u'Content Server'

    # Integrations with STORAGE_GOAL
    S3 = u'Amazon S3'
    MINIO = u'MinIO'
    LCP = u'LCP'

    # Integrations with CDN_GOAL
    CDN = u'CDN'

    # Integrations with SEARCH_GOAL
    ELASTICSEARCH = u'Elasticsearch'

    # Integrations with DRM_GOAL
    ADOBE_VENDOR_ID = u'Adobe Vendor ID'

    # Integrations with DISCOVERY_GOAL
    OPDS_REGISTRATION = u'OPDS Registration'

    # Integrations with ANALYTICS_GOAL
    GOOGLE_ANALYTICS = u'Google Analytics'

    # Integrations with ADMIN_AUTH_GOAL
    GOOGLE_OAUTH = u'Google OAuth'

    # List of such ADMIN_AUTH_GOAL integrations
    ADMIN_AUTH_PROTOCOLS = [GOOGLE_OAUTH]

    # Integrations with LOGGING_GOAL
    INTERNAL_LOGGING = u'Internal logging'
    LOGGLY = u"Loggly"
    CLOUDWATCH = u"AWS Cloudwatch Logs"

    # Integrations with CATALOG_GOAL
    MARC_EXPORT = u"MARC Export"

    # Keys for common configuration settings

    # If there is a special URL to use for access to this API,
    # put it here.
    URL = u"url"

    # If access requires authentication, these settings represent the
    # username/password or key/secret combination necessary to
    # authenticate. If there's a secret but no key, it's stored in
    # 'password'.
    USERNAME = u"username"
    PASSWORD = u"password"

    _cache = HasFullTableCache.RESET
    _id_cache = HasFullTableCache.RESET

    __tablename__ = 'externalintegrations'
    id = Column(Integer, primary_key=True)

    # Each integration should have a protocol (explaining what type of
    # code or network traffic we need to run to get things done) and a
    # goal (explaining the real-world goal of the integration).
    #
    # Basically, the protocol is the 'how' and the goal is the 'why'.
    protocol = Column(Unicode, nullable=False)
    goal = Column(Unicode, nullable=True)

    # A unique name for this ExternalIntegration. This is primarily
    # used to identify ExternalIntegrations from command-line scripts.
    name = Column(Unicode, nullable=True, unique=True)

    # Any additional configuration information goes into
    # ConfigurationSettings.
    settings = relationship(
        "ConfigurationSetting", backref="external_integration",
        lazy="joined", cascade="all, delete-orphan",
    )

    # Any number of Collections may designate an ExternalIntegration
    # as the source of their configuration
    collections = relationship(
        "Collection", backref="_external_integration",
        foreign_keys='Collection.external_integration_id',
    )

    links = relationship(
        "ExternalIntegrationLink",
        backref="other_integration",
        foreign_keys="ExternalIntegrationLink.other_integration_id",
        cascade="all, delete-orphan"
    )

    def __repr__(self):
        return u"<ExternalIntegration: protocol=%s goal='%s' settings=%d ID=%d>" % (
            self.protocol, self.goal, len(self.settings), self.id)

    def cache_key(self):
        # TODO: This is not ideal, but the lookup method isn't like
        # other HasFullTableCache lookup methods, so for now we use
        # the unique ID as the cache key. This means that
        # by_cache_key() and by_id() do the same thing.
        #
        # This is okay because we need by_id() quite a
        # bit and by_cache_key() not as much.
        return self.id

    @classmethod
    def for_goal(cls, _db, goal):
        """Return all external integrations by goal type.
        """
        integrations = _db.query(cls).filter(
            cls.goal==goal
        ).order_by(
            cls.name
        )

        return integrations

    @classmethod
    def for_collection_and_purpose(cls, _db, collection, purpose):
        """Find the ExternalIntegration for the collection.

        :param collection: Use the mirror configuration for this Collection.
        :param purpose: Use the purpose of the mirror configuration.
        """
        qu = _db.query(cls).join(
            ExternalIntegrationLink,
            ExternalIntegrationLink.other_integration_id==cls.id
        ).filter(
            ExternalIntegrationLink.external_integration_id==collection.external_integration_id,
            ExternalIntegrationLink.purpose==purpose
        )
        integrations = qu.all()
        if not integrations:
            raise CannotLoadConfiguration(
                "No storage integration for collection '%s' and purpose '%s' is configured." %
                (collection.name, purpose)
            )
        if len(integrations) > 1:
            raise CannotLoadConfiguration(
                "Multiple integrations found for collection '%s' and purpose '%s'" % (collection.name, purpose)
            )

        [integration] = integrations
        return integration

    @classmethod
    def lookup(cls, _db, protocol, goal, library=None):

        integrations = _db.query(cls).outerjoin(cls.libraries).filter(
            cls.protocol==protocol, cls.goal==goal
        )

        if library:
            integrations = integrations.filter(Library.id==library.id)

        integrations = integrations.all()
        if len(integrations) > 1:
            logging.warn("Multiple integrations found for '%s'/'%s'" % (protocol, goal))

        if filter(lambda i: i.libraries, integrations) and not library:
            raise ValueError(
                'This ExternalIntegration requires a library and none was provided.'
            )

        if not integrations:
            return None
        return integrations[0]

    @classmethod
    def with_setting_value(cls, _db, protocol, goal, key, value):
        """Find ExternalIntegrations with the given protocol, goal, and with a
        particular ConfigurationSetting key/value pair.
        This is useful in a scenario where an ExternalIntegration is
        made unique by a ConfigurationSetting, such as
        ExternalIntegration.URL, rather than by anything in the
        ExternalIntecation itself.

        :param protocol: ExternalIntegrations must have this protocol.
        :param goal: ExternalIntegrations must have this goal.
        :param key: Look only at ExternalIntegrations with
            a ConfigurationSetting for this key.
        :param value: Find ExternalIntegrations whose ConfigurationSetting
            has this value.
        :return: A Query object.
        """
        return _db.query(
            ExternalIntegration
        ).join(
            ExternalIntegration.settings
        ).filter(
            ExternalIntegration.goal==goal
        ).filter(
            ExternalIntegration.protocol==protocol
        ).filter(
            ConfigurationSetting.key==key
        ).filter(
            ConfigurationSetting.value==value
        )

    @classmethod
    def admin_authentication(cls, _db):
        admin_auth = get_one(_db, cls, goal=cls.ADMIN_AUTH_GOAL)
        return admin_auth

    @classmethod
    def for_library_and_goal(cls, _db, library, goal):
        """Find all ExternalIntegrations associated with the given
        Library and the given goal.
        :return: A Query.
        """
        return _db.query(ExternalIntegration).join(
            ExternalIntegration.libraries
        ).filter(
            ExternalIntegration.goal==goal
        ).filter(
            Library.id==library.id
        )

    @classmethod
    def one_for_library_and_goal(cls, _db, library, goal):
        """Find the ExternalIntegration associated with the given
        Library and the given goal.
        :return: An ExternalIntegration, or None.
        :raise: CannotLoadConfiguration
        """
        integrations = cls.for_library_and_goal(_db, library, goal).all()
        if len(integrations) == 0:
            return None
        if len(integrations) > 1:
            raise CannotLoadConfiguration(
                "Library %s defines multiple integrations with goal %s!" % (
                    library.name, goal
                )
            )
        return integrations[0]

    def set_setting(self, key, value):
        """Create or update a key-value setting for this ExternalIntegration."""
        setting = self.setting(key)
        setting.value = value
        return setting

    def setting(self, key):
        """Find or create a ConfigurationSetting on this ExternalIntegration.
        :param key: Name of the setting.
        :return: A ConfigurationSetting
        """
        return ConfigurationSetting.for_externalintegration(
            key, self
        )

    @hybrid_property
    def url(self):
        return self.setting(self.URL).value

    @url.setter
    def url(self, new_url):
        self.set_setting(self.URL, new_url)

    @hybrid_property
    def username(self):
        return self.setting(self.USERNAME).value

    @username.setter
    def username(self, new_username):
        self.set_setting(self.USERNAME, new_username)

    @hybrid_property
    def password(self):
        return self.setting(self.PASSWORD).value

    @password.setter
    def password(self, new_password):
        return self.set_setting(self.PASSWORD, new_password)

    def explain(self, library=None, include_secrets=False):
        """Create a series of human-readable strings to explain an
        ExternalIntegration's settings.

        :param library: Include additional settings imposed upon this
            ExternalIntegration by the given Library.
        :param include_secrets: For security reasons,
            sensitive settings such as passwords are not displayed by default.
        :return: A list of explanatory strings.
        """
        lines = []
        lines.append("ID: %s" % self.id)
        if self.name:
            lines.append("Name: %s" % self.name)
        lines.append("Protocol/Goal: %s/%s" % (self.protocol, self.goal))

        def key(setting):
            if setting.library:
                return setting.key, setting.library.name
            return (setting.key, None)
        for setting in sorted(self.settings, key=key):
            if library and setting.library and setting.library != library:
                # This is a different library's specialization of
                # this integration. Ignore it.
                continue
            if setting.value is None:
                # The setting has no value. Ignore it.
                continue
            explanation = "%s='%s'" % (setting.key, setting.value)
            if setting.library:
                explanation = "%s (applies only to %s)" % (
                    explanation, setting.library.name
                )
            if include_secrets or not setting.is_secret:
                lines.append(explanation)
        return lines

class ConfigurationSetting(Base, HasFullTableCache):
    """An extra piece of site configuration.
    A ConfigurationSetting may be associated with an
    ExternalIntegration, a Library, both, or neither.
    * The secret used by the circulation manager to sign OAuth bearer
    tokens is not associated with an ExternalIntegration or with a
    Library.
    * The link to a library's privacy policy is associated with the
    Library, but not with any particular ExternalIntegration.
    * The "website ID" for an Overdrive collection is associated with
    an ExternalIntegration (the Overdrive integration), but not with
    any particular Library (since multiple libraries might share an
    Overdrive collection).
    * The "identifier prefix" used to determine which library a patron
    is a patron of, is associated with both a Library and an
    ExternalIntegration.
    """
    __tablename__ = 'configurationsettings'
    id = Column(Integer, primary_key=True)
    external_integration_id = Column(
        Integer, ForeignKey('externalintegrations.id'), index=True
    )
    library_id = Column(
        Integer, ForeignKey('libraries.id'), index=True
    )
    key = Column(Unicode, index=True)
    _value = Column(Unicode, name="value")

    __table_args__ = (
        UniqueConstraint('external_integration_id', 'library_id', 'key'),
    )

    _cache = HasFullTableCache.RESET
    _id_cache = HasFullTableCache.RESET

    def __repr__(self):
        return u'<ConfigurationSetting: key=%s, ID=%d>' % (
            self.key, self.id)

    @classmethod
    def sitewide_secret(cls, _db, key):
        """Find or create a sitewide shared secret.
        The value of this setting doesn't matter, only that it's
        unique across the site and that it's always available.
        """
        secret = ConfigurationSetting.sitewide(_db, key)
        if not secret.value:
            secret.value = random_string(24)
            # Commit to get this in the database ASAP.
            _db.commit()
        return secret.value

    @classmethod
    def explain(cls, _db, include_secrets=False):
        """Explain all site-wide ConfigurationSettings."""
        lines = []
        site_wide_settings = []

        for setting in _db.query(ConfigurationSetting).filter(
                ConfigurationSetting.library==None).filter(
                    ConfigurationSetting.external_integration==None):
            if not include_secrets and setting.key.endswith("_secret"):
                continue
            site_wide_settings.append(setting)
        if site_wide_settings:
            lines.append("Site-wide configuration settings:")
            lines.append("---------------------------------")
        for setting in sorted(site_wide_settings, key=lambda s: s.key):
            if setting.value is None:
                continue
            lines.append("%s='%s'" % (setting.key, setting.value))
        return lines

    @classmethod
    def sitewide(cls, _db, key):
        """Find or create a sitewide ConfigurationSetting."""
        return cls.for_library_and_externalintegration(_db, key, None, None)

    @classmethod
    def for_library(cls, key, library):
        """Find or create a ConfigurationSetting for the given Library."""
        _db = Session.object_session(library)
        return cls.for_library_and_externalintegration(_db, key, library, None)

    @classmethod
    def for_externalintegration(cls, key, externalintegration):
        """Find or create a ConfigurationSetting for the given
        ExternalIntegration.
        """
        _db = Session.object_session(externalintegration)
        return cls.for_library_and_externalintegration(
            _db, key, None, externalintegration
        )

    @classmethod
    def _cache_key(cls, library, external_integration, key):
        if library:
            library_id = library.id
        else:
            library_id = None
        if external_integration:
            external_integration_id = external_integration.id
        else:
            external_integration_id = None
        return (library_id, external_integration_id, key)

    def cache_key(self):
        return self._cache_key(self.library, self.external_integration, self.key)

    @classmethod
    def for_library_and_externalintegration(
            cls, _db, key, library, external_integration
    ):
        """Find or create a ConfigurationSetting associated with a Library
        and an ExternalIntegration.
        """
        def create():
            """Function called when a ConfigurationSetting is not found in cache
            and must be created.
            """
            return get_one_or_create(
                _db, ConfigurationSetting,
                library=library, external_integration=external_integration,
                key=key
            )

        # ConfigurationSettings are stored in cache based on their library,
        # external integration, and the name of the setting.
        cache_key = cls._cache_key(library, external_integration, key)
        setting, ignore = cls.by_cache_key(_db, cache_key, create)
        return setting

    @hybrid_property
    def value(self):

        """What's the current value of this configuration setting?
        If not present, the value may be inherited from some other
        ConfigurationSetting.
        """
        if self._value:
            # An explicitly set value always takes precedence.
            return self._value
        elif self.library and self.external_integration:
            # This is a library-specific specialization of an
            # ExternalIntegration. Treat the value set on the
            # ExternalIntegration as a default.
            return self.for_externalintegration(
                self.key, self.external_integration).value
        elif self.library:
            # This is a library-specific setting. Treat the site-wide
            # value as a default.
            _db = Session.object_session(self)
            return self.sitewide(_db, self.key).value
        return self._value

    @value.setter
    def value(self, new_value):
        if new_value is not None:
            new_value = unicode(new_value)
        self._value = new_value

    @classmethod
    def _is_secret(self, key):
        """Should the value of the given key be treated as secret?
        This will have to do, in the absence of programmatic ways of
        saying that a specific setting should be treated as secret.
        """
        return any(
            key == x or
            key.startswith('%s_' % x) or
            key.endswith('_%s' % x) or
            ("_%s_" %x) in key
            for x in ('secret', 'password')
        )

    @property
    def is_secret(self):
        """Should the value of this key be treated as secret?"""
        return self._is_secret(self.key)

    def value_or_default(self, default):
        """Return the value of this setting. If the value is None,
        set it to `default` and return that instead.
        """
        if self.value is None:
            self.value = default
        return self.value

    MEANS_YES = set(['true', 't', 'yes', 'y'])
    @property
    def bool_value(self):
        """Turn the value into a boolean if possible.
        :return: A boolean, or None if there is no value.
        """
        if self.value:
            if self.value.lower() in self.MEANS_YES:
                return True
            return False
        return None

    @property
    def int_value(self):
        """Turn the value into an int if possible.
        :return: An integer, or None if there is no value.
        :raise ValueError: If the value cannot be converted to an int.
        """
        if self.value:
            return int(self.value)
        return None

    @property
    def float_value(self):
        """Turn the value into an float if possible.
        :return: A float, or None if there is no value.
        :raise ValueError: If the value cannot be converted to a float.
        """
        if self.value:
            return float(self.value)
        return None

    @property
    def json_value(self):
        """Interpret the value as JSON if possible.
        :return: An object, or None if there is no value.
        :raise ValueError: If the value cannot be parsed as JSON.
        """
        if self.value:
            return json.loads(self.value)
        return None

    # As of this release of the software, this is our best guess as to
    # which data sources should have their audiobooks excluded from
    # lanes.
    EXCLUDED_AUDIO_DATA_SOURCES_DEFAULT = []

    @classmethod
    def excluded_audio_data_sources(cls, _db):
        """List the data sources whose audiobooks should not be published in
        feeds, either because this server can't fulfill them or the
        expected client can't play them.
        Most methods like this go into Configuration, but this one needs
        to reference data model objects for its default value.
        """
        value = cls.sitewide(
            _db, Configuration.EXCLUDED_AUDIO_DATA_SOURCES
        ).json_value
        if value is None:
            value = cls.EXCLUDED_AUDIO_DATA_SOURCES_DEFAULT
        return value


class HasExternalIntegration(object):
    """Interface allowing to get access to an external integration"""

    __metaclass__ = ABCMeta

    @abstractmethod
    def external_integration(self, db):
        """Returns an external integration associated with this object

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :return: External integration associated with this object
        :rtype: core.model.configuration.ExternalIntegration
        """
        raise NotImplementedError()


class BaseConfigurationStorage(object):
    """Serializes and deserializes values as configuration settings"""

    __metaclass__ = ABCMeta

    @abstractmethod
    def save(self, db, setting_name, value):
        """Save the value as as a new configuration setting

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param setting_name: Name of the configuration setting
        :type setting_name: string

        :param value: Value to be saved
        :type value: Any
        """
        raise NotImplementedError()

    @abstractmethod
    def load(self, db, setting_name):
        """Loads and returns the library's configuration setting

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param setting_name: Name of the configuration setting
        :type setting_name: string

        :return: Any
        """
        raise NotImplementedError()


class ConfigurationStorage(BaseConfigurationStorage):
    """Serializes and deserializes values as configuration settings"""

    def __init__(self, integration_association):
        """Initializes a new instance of ConfigurationStorage class

        :param integration_association: Association with an external integration
        :type integration_association: HasExternalIntegration
        """
        self._integration_association = integration_association

    def save(self, db, setting_name, value):
        """Save the value as as a new configuration setting

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param setting_name: Name of the configuration setting
        :type setting_name: string

        :param value: Value to be saved
        :type value: Any
        """
        integration = self._integration_association.external_integration(db)
        ConfigurationSetting.for_externalintegration(
            setting_name,
            integration).value = value

    def load(self, db, setting_name):
        """Loads and returns the library's configuration setting

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param setting_name: Name of the library's configuration setting
        :type setting_name: string

        :return: Any
        """
        integration = self._integration_association.external_integration(db)
        value = ConfigurationSetting.for_externalintegration(
            setting_name,
            integration).value

        return value


class ConfigurationAttributeType(Enum):
    """Enumeration of configuration setting types"""

    TEXT = 'text'
    TEXTAREA = 'textarea'
    SELECT = 'select'
    NUMBER = 'number'

    def to_control_type(self):
        """Converts the value to a attribute type understandable by circulation-web

        :return: String representation of attribute's type
        :rtype: string
        """
        # NOTE: For some reason, circulation-web converts "text" into <text> so we have to turn it into None
        # In this case circulation-web will use <input>
        # TODO: To be fixed in https://jira.nypl.org/browse/SIMPLY-3008
        if self.value == self.TEXT.value:
            return None
        else:
            return self.value


class ConfigurationAttribute(Enum):
    """Enumeration of configuration setting attributes"""

    KEY = 'key'
    LABEL = 'label'
    DESCRIPTION = 'description'
    TYPE = 'type'
    REQUIRED = 'required'
    DEFAULT = 'default'
    OPTIONS = 'options'
    CATEGORY = 'category'


class ConfigurationOption(object):
    """Key-value pair containing information about configuration attribute option"""

    def __init__(self, key, label):
        """Initializes a new instance of ConfigurationOption class

        :param key: Key
        :type key: string

        :param label: Label
        :type label: string
        """
        self._key = key
        self._label = label

    def __eq__(self, other):
        """Compares two ConfigurationOption objects

        :param other: ConfigurationOption object
        :type other: ConfigurationOption

        :return: Boolean value indicating whether two items are equal
        :rtype: bool
        """
        if not isinstance(other, ConfigurationOption):
            return False

        return \
            self.key == other.key and \
            self.label == other.label

    @property
    def key(self):
        """Returns option's key

        :return: Option's key
        :rtype: string
        """
        return self._key

    @property
    def label(self):
        """Returns option's label

        :return: Option's label
        :rtype: string
        """
        return self._label

    def to_settings(self):
        """Returns a dictionary containing option metadata in the SETTINGS format

        :return: Dictionary containing option metadata in the SETTINGS format
        :rtype: Dict
        """
        return {
            'key': self.key,
            'label': self.label
        }

    @staticmethod
    def from_enum(cls):
        """Convers Enum to a list of options in the SETTINGS format

        :param cls: Enum type
        :type cls: type

        :return: List of options in the SETTINGS format
        :rtype: List[Dict]
        """
        if not issubclass(cls, Enum):
            raise ValueError('Class should be descendant of Enum')

        return [
            ConfigurationOption(element.value, element.name)
            for element in cls
        ]


class HasConfigurationSettings(object):
    """Interface representing class containing ConfigurationMetadata properties"""

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_setting_value(self, setting_name):
        """Returns a settings'value

        :param setting_name: Name of the setting
        :type setting_name: string

        :return: Setting's value
        :rtype: Any
        """
        raise NotImplementedError()

    @abstractmethod
    def set_setting_value(self, setting_name, setting_value):
        """Sets setting's value

        :param setting_name: Name of the setting
        :type setting_name: string

        :param setting_value: New value of the setting
        :type setting_value: Any
        """
        raise NotImplementedError()


class ConfigurationMetadata(object):
    """Contains configuration metadata"""

    _counter = 0

    def __init__(
            self,
            key,
            label,
            description,
            type,
            required=False,
            default=None,
            options=None,
            category=None,
            index=None):
        """Initializes a new instance of ConfigurationMetadata class

        :param key: Setting's key
        :type key: string

        :param label: Setting's label
        :type label: string

        :param description: Setting's description
        :type description: string

        :param type: Setting's type
        :type type: ConfigurationAttributeType

        :param required: Boolean value indicating whether the setting is required or not
        :type required: bool

        :param default: Setting's default value
        :type default: Any

        :param options: Setting's options (used in the case of select)
        :type options: List[ConfigurationSettingAttributeOption]

        :param category: Setting's category
        :type category: string
        """
        self._key = key
        self._label = label
        self._description = description
        self._type = type
        self._required = required
        self._default = default
        self._options = options
        self._category = category

        if index is not None:
            self._index = index
        else:
            ConfigurationMetadata._counter += 1
            self._index = ConfigurationMetadata._counter

    def __get__(self, owner_instance, owner_type):
        """Returns a value of the setting

        :param owner_instance: Instance of the owner, class having instance of ConfigurationMetadata as an attribute
        :type owner_instance: Optional[ConfigurationMetadataOwner]

        :param owner_type: Owner's class
        :type owner_type: Optional[Type]

        :return: ConfigurationMetadata instance (when called via a static method) or
            the setting's value (when called via an instance method)
        :rtype: Union[ConfigurationMetadata, Any]
        """
        # If owner_instance is empty, it means that this method was called
        # via a static method of ConfigurationMetadataOwner (for example, ConfigurationBucket.to_settings).
        # In this case we need to return the metadata instance itself
        if owner_instance is None:
            return self

        if not isinstance(owner_instance, HasConfigurationSettings):
            raise Exception('owner must be an instance of ConfigurationSettingsMetadataOwner type')

        return owner_instance.get_setting_value(self._key)

    def __set__(self, owner_instance, value):
        """Updates the setting's value

        :param owner_instance: Instance of the owner, class having instance of ConfigurationMetadata as an attribute
        :type owner_instance: Optional[ConfigurationMetadataOwner]

        :param value: New setting's value
        :type value: Any
        """
        if not isinstance(owner_instance, HasConfigurationSettings):
            raise Exception('owner must be an instance ConfigurationSettingsMetadataOwner type')

        return owner_instance.set_setting_value(self._key, value)

    @property
    def key(self):
        """Returns the setting's key

        :return: Setting's key
        :rtype: string
        """
        return self._key

    @property
    def label(self):
        """Returns the setting's label

        :return: Setting's label
        :rtype: string
        """
        return self._label

    @property
    def description(self):
        """Returns the setting's description

        :return: Setting's description
        :rtype: string
        """
        return self._description

    @property
    def type(self):
        """Returns the setting's type

        :return: Setting's type
        :rtype: string
        """
        return self._type

    @property
    def required(self):
        """Returns the boolean value indicating whether the setting is required or not

        :return: Boolean value indicating whether the setting is required or not
        :rtype: string
        """
        return self._required

    @property
    def default(self):
        """Returns the setting's default value

        :return: Setting's default value
        :rtype: string
        """
        return self._default

    @property
    def options(self):
        """Returns the setting's options (used in the case of select)

        :return: Setting's options (used in the case of select)
        :rtype: string
        """
        return self._options

    @property
    def category(self):
        """Returns the setting's category

        :return: Setting's category
        :rtype: string
        """
        return self._category

    @property
    def index(self):
        return self._index

    @staticmethod
    def get_configuration_metadata(cls):
        """Returns a list of 2-tuples containing information ConfigurationMetadata properties in the specified class

        :param cls: Class
        :type cls: type

        :return: List of 2-tuples containing information ConfigurationMetadata properties in the specified class
        :rtype: List[Tuple[string, ConfigurationMetadata]]
        """
        members = inspect.getmembers(cls)
        configuration_metadata = []

        for name, member in members:
            if isinstance(member, ConfigurationMetadata):
                configuration_metadata.append((name, member))

        configuration_metadata.sort(key=lambda pair: pair[1].index)

        return configuration_metadata

    def to_settings(self):
        return {
            ConfigurationAttribute.KEY.value: self.key,
            ConfigurationAttribute.LABEL.value: self.label,
            ConfigurationAttribute.DESCRIPTION.value: self.description,
            ConfigurationAttribute.TYPE.value: self.type.to_control_type(),
            ConfigurationAttribute.REQUIRED.value: self.required,
            ConfigurationAttribute.DEFAULT.value: self.default,
            ConfigurationAttribute.OPTIONS.value:
                [option.to_settings() for option in self.options]
                if self.options
                else None,
            ConfigurationAttribute.CATEGORY.value: self.category
        }


class ConfigurationGrouping(HasConfigurationSettings):
    """Base class for all classes containing configuration settings

    NOTE: Be aware that it's valid only while a database session is valid and must not be stored between requests
    """

    def __init__(self, configuration_storage, db):
        """Initializes a new instance of ConfigurationGrouping

        :param configuration_storage: ConfigurationStorage object
        :type configuration_storage: BaseConfigurationStorage

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session
        """
        self._logger = logging.getLogger()
        self._configuration_storage = configuration_storage
        self._db = db

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._db = None

    def get_setting_value(self, setting_name):
        """Returns a settings'value

        :param setting_name: Name of the setting
        :type setting_name: string

        :return: Setting's value
        :rtype: Any
        """
        return self._configuration_storage.load(self._db, setting_name)

    def set_setting_value(self, setting_name, setting_value):
        """Sets setting's value

        :param setting_name: Name of the setting
        :type setting_name: string

        :param setting_value: New value of the setting
        :type setting_value: Any
        """
        self._configuration_storage.save(self._db, setting_name, setting_value)

    @classmethod
    def to_settings(cls):
        settings = []

        for name, member in ConfigurationMetadata.get_configuration_metadata(cls):
            key_attribute = getattr(member, ConfigurationAttribute.KEY.value, None)
            label_attribute = getattr(member, ConfigurationAttribute.LABEL.value, None)
            description_attribute = getattr(member, ConfigurationAttribute.DESCRIPTION.value, None)
            type_attribute = getattr(member, ConfigurationAttribute.TYPE.value, None)
            required_attribute = getattr(member, ConfigurationAttribute.REQUIRED.value, None)
            default_attribute = getattr(member, ConfigurationAttribute.DEFAULT.value, None)
            options_attribute = getattr(member, ConfigurationAttribute.OPTIONS.value, None)
            category_attribute = getattr(member, ConfigurationAttribute.CATEGORY.value, None)

            settings.append({
                ConfigurationAttribute.KEY.value: key_attribute,
                ConfigurationAttribute.LABEL.value: label_attribute,
                ConfigurationAttribute.DESCRIPTION.value: description_attribute,
                ConfigurationAttribute.TYPE.value: type_attribute.to_control_type(),
                ConfigurationAttribute.REQUIRED.value: required_attribute,
                ConfigurationAttribute.DEFAULT.value: default_attribute,
                ConfigurationAttribute.OPTIONS.value:
                    [option.to_settings() for option in options_attribute]
                    if options_attribute
                    else None,
                ConfigurationAttribute.CATEGORY.value: category_attribute
            })

        return settings


class ConfigurationFactory(object):
    """Factory creating new instances of ConfigurationBucket class descendants"""

    @contextmanager
    def create(self, configuration_storage, db, configuration_bucket_class):
        """Creates a new instance of ConfigurationFactory

        :param configuration_storage: ConfigurationStorage object
        :type configuration_storage: ConfigurationStorage

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param configuration_bucket_class: Configuration bucket's class
        :type configuration_bucket_class: type

        :return: Configuration bucket instance
        :rtype: ConfigurationGrouping
        """
        with configuration_bucket_class(configuration_storage, db) as configuration_bucket:
            yield configuration_bucket
