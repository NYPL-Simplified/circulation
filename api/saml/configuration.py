from flask_babel import lazy_gettext as _
from onelogin.saml2.settings import OneLogin_Saml2_Settings

from api.saml.metadata import ServiceProviderMetadata, IdentityProviderMetadata
from core.exceptions import BaseError


class SAMLConfigurationError(BaseError):
    """Raised in the case of any configuration errors"""


class SAMLConfiguration(object):
    """Contains SP and IdP settings"""

    DEBUG = 'debug'
    STRICT = 'strict'

    SP_XML_METADATA = 'sp_xml_metadata'
    SP_PRIVATE_KEY = 'sp_private_key'

    IDP_XML_METADATA = 'idp_xml_metadata'

    IDP_DISPLAY_NAME_DEFAULT_TEMPLATE = 'Identity Provider #{0}'

    def __init__(self, configuration_storage, metadata_parser):
        """Initializes a new instance of SAMLConfiguration class

        :param configuration_storage: SAML configuration storage
        :type configuration_storage: ConfigurationStorage

        :param metadata_parser: SAML metadata parser
        :type metadata_parser: SAMLMetadataParser
        """
        self._configuration_storage = configuration_storage
        self._metadata_parser = metadata_parser

        self._debug = None
        self._strict = None

        self._identity_providers = None
        self._service_provider = None

    def _load_debug(self, db):
        """Returns a debug mode indicator

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :return: Debug mode indicator
        :rtype: bool
        """
        debug = bool(self._configuration_storage.load(db, self.DEBUG))

        return debug

    def _load_strict(self, db):
        """Returns a strict mode indicator

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :return: Strict mode indicator
        :rtype: bool
        """
        strict = bool(self._configuration_storage.load(db, self.STRICT))

        return strict

    def _load_identity_providers(self, db):
        """Loads IdP settings from the library's configuration settings

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :return: List of IdentityProviderMetadata objects
        :rtype: List[IdentityProviderMetadata]

        :raise: SAMLParsingError
        """
        idp_providers_metadata = self._configuration_storage.load(db, self.IDP_XML_METADATA)
        idp_providers = self._metadata_parser.parse(idp_providers_metadata)

        return idp_providers

    def _load_service_provider(self, db):
        """Loads SP settings from the library's configuration settings

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :return: ServiceProviderMetadata object
        :rtype: ServiceProviderMetadata

        :raise: SAMLParsingError
        """
        sp_provider_metadata = self._configuration_storage.load(db, self.SP_XML_METADATA)
        parsing_result = self._metadata_parser.parse(sp_provider_metadata)

        if not isinstance(parsing_result, list) or len(parsing_result) != 1:
            raise SAMLConfigurationError(_('SAML SP configuration is not correct'))

        sp_provider = parsing_result[0]

        if not isinstance(sp_provider, ServiceProviderMetadata):
            raise SAMLConfigurationError(_('SAML SP configuration is not correct'))

        sp_provider_private_key = self._configuration_storage.load(db, self.SP_PRIVATE_KEY)
        sp_provider.private_key = sp_provider_private_key

        return sp_provider

    def get_debug(self, db):
        """Returns a debug mode indicator

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :return: Debug mode indicator
        :rtype: bool
        """
        if self._debug is None:
            self._debug = self._load_debug(db)

        return self._debug

    def get_identity_providers(self, db):
        """Returns identity providers

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :return: List of IdentityProviderMetadata objects
        :rtype: List[IdentityProviderMetadata]

        :raise: ConfigurationError
        """
        if self._identity_providers is None:
            self._identity_providers = self._load_identity_providers(db)

        return self._identity_providers

    def get_service_provider(self, db):
        """Returns service provider

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :return: ServiceProviderMetadata object
        :rtype: ServiceProviderMetadata

        :raise: ConfigurationError
        """
        if self._service_provider is None:
            self._service_provider = self._load_service_provider(db)

        return self._service_provider

    def get_strict(self, db):
        """Returns strict mode indicator

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :return: Strict mode indicator
        :rtype: bool
        """
        if self._strict is None:
            self._strict = self._load_strict(db)

        return self._strict


class SAMLOneLoginConfiguration(object):
    """Converts metadata objects to the OneLogin's SAML Toolkit format"""

    DEBUG = 'debug'
    STRICT = 'strict'

    ENTITY_ID = 'entityId'
    URL = 'url'
    BINDING = 'binding'
    X509_CERT = 'x509cert'
    X509_CERT_MULTI = 'x509certMulti'
    SIGNING = 'signing'
    ENCRYPTION = 'encryption'

    IDP = 'idp'
    SINGLE_SIGN_ON_SERVICE = 'singleSignOnService'

    SP = 'sp'
    ASSERTION_CONSUMER_SERVICE = 'assertionConsumerService'
    NAME_ID_FORMAT = 'NameIDFormat'
    PRIVATE_KEY = 'privateKey'

    SECURITY = 'security'
    AUTHN_REQUESTS_SIGNED = 'authnRequestsSigned'

    def __init__(self, configuration):
        """Initializes a new instance of SAMLOneLoginConfiguration class

        :param configuration: Configuration object containing SAML metadata
        :type configuration: SAMLConfiguration
        """
        self._configuration = configuration
        self._service_provider = None
        self._identity_providers = {}

    def _get_identity_provider_settings(self, identity_provider):
        """Converts ServiceProviderMetadata object to the OneLogin's SAML Toolkit format

        :param identity_provider: IdentityProviderMetadata object
        :type identity_provider: IdentityProviderMetadata

        :return: Dictionary containing service provider's settings in the OneLogin's SAML Toolkit format
        :rtype: Dict
        """
        onelogin_identity_provider = {
            self.IDP: {
                self.ENTITY_ID: identity_provider.entity_id,
                self.SINGLE_SIGN_ON_SERVICE: {
                    self.URL: identity_provider.sso_service.url,
                    self.BINDING: identity_provider.sso_service.binding.value
                },
            },
            self.SECURITY: {
                self.AUTHN_REQUESTS_SIGNED: identity_provider.want_authn_requests_signed
            }
        }

        if len(identity_provider.signing_certificates) == 1 and \
                len(identity_provider.encryption_certificates) == 1 and \
                identity_provider.signing_certificates[0] == identity_provider.encryption_certificates[0]:
            onelogin_identity_provider[self.IDP][self.X509_CERT] = identity_provider.signing_certificates[0]
        else:
            if len(identity_provider.signing_certificates) > 0:
                if self.X509_CERT_MULTI not in onelogin_identity_provider[self.IDP]:
                    onelogin_identity_provider[self.IDP][self.X509_CERT_MULTI] = {}

                onelogin_identity_provider[self.IDP][self.X509_CERT_MULTI][self.SIGNING] = \
                    identity_provider.signing_certificates
            if len(identity_provider.encryption_certificates) > 0:
                if self.X509_CERT_MULTI not in onelogin_identity_provider[self.IDP]:
                    onelogin_identity_provider[self.IDP][self.X509_CERT_MULTI] = {}

                onelogin_identity_provider[self.IDP][self.X509_CERT_MULTI][self.ENCRYPTION] = \
                    identity_provider.encryption_certificates

        return onelogin_identity_provider

    def _get_service_provider_settings(self, service_provider):
        """Converts ServiceProviderMetadata object to the OneLogin's SAML Toolkit format

        :param service_provider: ServiceProviderMetadata object
        :type service_provider: ServiceProviderMetadata

        :return: Dictionary containing service provider's settings in the OneLogin's SAML Toolkit format
        :rtype: Dict
        """
        onelogin_service_provider = {
            self.SP: {
                self.ENTITY_ID: service_provider.entity_id,
                self.ASSERTION_CONSUMER_SERVICE: {
                    self.URL: service_provider.acs_service.url,
                    self.BINDING: service_provider.acs_service.binding.value
                },
                self.NAME_ID_FORMAT: service_provider.name_id_format,
                self.X509_CERT: service_provider.certificate if service_provider.certificate else '',
                self.PRIVATE_KEY: service_provider.private_key if service_provider.private_key else ''
            },
            self.SECURITY: {
                self.AUTHN_REQUESTS_SIGNED: service_provider.authn_requests_signed
            }
        }

        return onelogin_service_provider

    @property
    def configuration(self):
        """Returns original configuration

        :return: Original configuration
        :rtype: SAMLConfiguration
        """
        return self._configuration

    def get_identity_provider_settings(self, db, idp_entity_id):
        """Returns a dictionary containing identity provider's settings in a OneLogin's SAML Toolkit format

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session
        
        :param idp_entity_id: IdP's entity ID
        :type idp_entity_id: string

        :return: Dictionary containing identity provider's settings in a OneLogin's SAML Toolkit format
        :rtype: Dict
        """
        if idp_entity_id in self._identity_providers:
            return self._identity_providers[idp_entity_id]

        identity_providers = [idp for idp in self._configuration.get_identity_providers(db)
                              if idp.entity_id == idp_entity_id]

        if not identity_providers:
            raise SAMLConfigurationError(
                message=_('There is no identity provider with entityID = {0}'.format(idp_entity_id)))

        if len(identity_providers) > 1:
            raise SAMLConfigurationError(
                message=_('There are multiple identity providers with entityID = {0}'.format(idp_entity_id)))

        identity_provider = identity_providers[0]
        identity_provider = self._get_identity_provider_settings(identity_provider)

        self._identity_providers[idp_entity_id] = identity_provider

        return identity_provider

    def get_service_provider_settings(self, db):
        """Returns a dictionary containing service provider's settings in the OneLogin's SAML Toolkit format

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :return: Dictionary containing service provider's settings in the OneLogin's SAML Toolkit format
        :rtype: Dict
        """
        if self._service_provider is None:
            self._service_provider = self._get_service_provider_settings(self._configuration.get_service_provider(db))

        return self._service_provider

    def get_settings(self, db, idp_entity_id):
        """Returns a dictionary containing SP's and IdP's settings in the OneLogin's SAML Toolkit format

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param idp_entity_id: IdP's entity ID
        :type idp_entity_id: string

        :return: Dictionary containing SP's and IdP's settings in the OneLogin's SAML Toolkit format
        :rtype: Dict
        """
        onelogin_settings = {
            self.DEBUG: self._configuration.get_debug(db),
            self.STRICT: self._configuration.get_strict(db)
        }
        identity_provider_settings = self.get_identity_provider_settings(db, idp_entity_id)
        service_provider_settings = self.get_service_provider_settings(db)

        onelogin_settings.update(identity_provider_settings)
        onelogin_settings.update(service_provider_settings)

        # We need to use disjunction separately because dict.update just overwrites values
        onelogin_settings[self.SECURITY][self.AUTHN_REQUESTS_SIGNED] = \
            service_provider_settings[self.SECURITY][self.AUTHN_REQUESTS_SIGNED] or \
            service_provider_settings[self.SECURITY][self.AUTHN_REQUESTS_SIGNED]

        settings = OneLogin_Saml2_Settings(onelogin_settings)

        return {
            self.DEBUG: self._configuration.get_debug(db),
            self.STRICT: self._configuration.get_strict(db),
            self.IDP: settings.get_idp_data(),
            self.SP: settings.get_sp_data(),
            self.SECURITY: settings.get_security_data()
        }
