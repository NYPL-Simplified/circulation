import pickle

from onelogin.saml2.settings import OneLogin_Saml2_Settings

from api.saml.exceptions import SAMLError
from api.saml.metadata import ServiceProviderMetadata, IdentityProviderMetadata
from core.model import ConfigurationSetting
from flask_babel import lazy_gettext as _


class SAMLConfigurationSerializingError(SAMLError):
    """Raised in the case of any errors during configuration serializing"""


class SAMLConfigurationSerializer(object):
    """Serializes and deserializes values as library's configuration settings"""

    def __init__(self, integration):
        """Initializes a new instance of SAMLConfigurationSerializer class

        :param integration: External integration
        :type integration: ExternalIntegration
        """
        self._integration = integration

    def serialize(self, setting_name, value):
        """Serializes the value as a pickled library's configuration setting

        :param setting_name: Name of the library's configuration setting
        :type setting_name: string

        :param value: Value to be serialized
        :type value: Any
        """
        ConfigurationSetting.for_externalintegration(
            setting_name,
            self._integration).value = value

    def deserialize(self, setting_name):
        """Deserializes and returns the library's configuration setting

        :param setting_name: Name of the library's configuration setting
        :type setting_name: string

        :return: Any
        """
        value = ConfigurationSetting.for_externalintegration(
            setting_name,
            self._integration).value

        return value


class SAMLMetadataSerializer(SAMLConfigurationSerializer):
    """Serializes and deserializes values as pickled library's configuration settings"""

    def __init__(self, integration):
        """Initializes a new instance of SAMLMetadataSerializer class

        :param integration: External integration
        :type integration: ExternalIntegration
        """
        super(SAMLMetadataSerializer, self).__init__(integration)

    def serialize(self, setting_name, value):
        """Serializes the value as a pickled library's configuration setting

        :param setting_name: Name of the library's configuration setting
        :type setting_name: string

        :param value: Value to be serialized (should be picklable)
        :type value: Any
        """
        if not value:
            raise ValueError('Value must be non-empty')

        serialized_value = pickle.dumps(value)
        super(SAMLMetadataSerializer, self).serialize(setting_name, serialized_value)

    def deserialize(self, setting_name):
        """Deserializes and returns the library's configuration setting

        :param setting_name: Name of the library's configuration setting
        :type setting_name: string

        :return: Any
        """
        serialized_value = super(SAMLMetadataSerializer, self).deserialize(setting_name)

        if not serialized_value:
            raise SAMLConfigurationSerializingError(message=_('Serialized value is empty'))

        deserialized_value = pickle.loads(serialized_value)

        return deserialized_value


class SAMLConfigurationError(SAMLError):
    """Raised in the case of any configuration errors"""


class SAMLConfiguration(object):
    """Contains SP and IdP settings"""

    DEBUG = 'debug'
    STRICT = 'strict'

    SP_METADATA = 'sp_metadata'
    SP_XML_METADATA = 'sp_xml_metadata'
    SP_PRIVATE_KEY = 'sp_private_key'

    IDP_XML_METADATA = 'idp_xml_metadata'
    IDP_METADATA = 'idp_metadata'

    def __init__(self, configuration_serializer, metadata_serializer):
        """Initializes a new instance of SAMLConfiguration class

        :param configuration_serializer: SAML configuration serializer
        :type configuration_serializer: SAMLConfigurationSerializer

        :param metadata_serializer: SAML metadata configuration serializer
        :type metadata_serializer: SAMLMetadataSerializer
        """
        self._configuration_serializer = configuration_serializer
        self._metadata_serializer = metadata_serializer

        self._debug = None
        self._strict = None

        self._identity_providers = None
        self._service_provider = None

    def _load_debug(self):
        """Returns a debug mode indicator

        :return: Debug mode indicator
        :rtype: bool
        """
        debug = bool(self._configuration_serializer.deserialize(self.DEBUG))

        return debug

    def _load_strict(self):
        """Returns a strict mode indicator

        :return: Strict mode indicator
        :rtype: bool
        """
        strict = bool(self._configuration_serializer.deserialize(self.STRICT))

        return strict

    def _load_identity_providers(self):
        """Loads IdP settings from the library's configuration settings

        :return: List of IdentityProviderMetadata objects
        :rtype: List[IdentityProviderMetadata]

        :raise: ConfigurationError
        """
        idp_providers = self._metadata_serializer.deserialize(self.IDP_METADATA)

        if not isinstance(idp_providers, list):
            raise SAMLConfigurationError(message=_('Configuration settings is missing identity provider\'s metadata'))

        for idp_provider in idp_providers:
            if not isinstance(idp_provider, IdentityProviderMetadata):
                raise SAMLConfigurationError(message=_('Identity provider\'s metadata is not correct'))

        return idp_providers

    def _load_service_provider(self):
        """Loads SP settings from the library's configuration settings

        :return: ServiceProviderMetadata object
        :rtype: ServiceProviderMetadata

        :raise: ConfigurationError
        """
        sp_provider = self._metadata_serializer.deserialize(self.SP_METADATA)

        if not isinstance(sp_provider, ServiceProviderMetadata):
            raise SAMLConfigurationError(message=_('Configuration settings is missing service provider\'s metadata'))

        return sp_provider

    @property
    def debug(self):
        """Returns a debug mode indicator

        :return: Debug mode indicator
        :rtype: bool
        """
        if self._debug is None:
            self._debug = self._load_debug()

        return self._debug

    @property
    def identity_providers(self):
        """Returns identity providers

        :return: List of IdentityProviderMetadata objects
        :rtype: List[IdentityProviderMetadata]

        :raise: ConfigurationError
        """
        if self._identity_providers is None:
            self._identity_providers = self._load_identity_providers()

        return self._identity_providers

    @property
    def service_provider(self):
        """Returns service provider

        :return: ServiceProviderMetadata object
        :rtype: ServiceProviderMetadata

        :raise: ConfigurationError
        """
        if self._service_provider is None:
            self._service_provider = self._load_service_provider()

        return self._service_provider

    @property
    def strict(self):
        """Returns strict mode indicator

        :return: Strict mode indicator
        :rtype: bool
        """
        if self._strict is None:
            self._strict = self._load_strict()

        return self._strict


class SAMLOneLoginConfiguration(object):
    """Converts metadata objects to the OneLogin's SAML Toolkit format"""

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
            'idp': {
                'entityId': identity_provider.entity_id,
                'singleSignOnService': {
                    'url': identity_provider.sso_service.url,
                    'binding': identity_provider.sso_service.binding.value
                },
            },
            'security': {
                'authnRequestsSigned': identity_provider.want_authn_requests_signed
            }
        }

        if len(identity_provider.signing_certificates) == 1 and \
                len(identity_provider.encryption_certificates) == 1 and \
                identity_provider.signing_certificates[0] == identity_provider.encryption_certificates[0]:
            onelogin_identity_provider['idp']['x509cert'] = identity_provider.signing_certificates[0]
        else:
            if len(identity_provider.signing_certificates) > 0:
                if 'x509certMulti' not in onelogin_identity_provider['idp']:
                    onelogin_identity_provider['idp']['x509certMulti'] = {}

                onelogin_identity_provider['idp']['x509certMulti']['signing'] = \
                    identity_provider.signing_certificates
            if len(identity_provider.encryption_certificates) > 0:
                if 'x509certMulti' not in onelogin_identity_provider['idp']:
                    onelogin_identity_provider['idp']['x509certMulti'] = {}

                onelogin_identity_provider['idp']['x509certMulti']['encryption'] = \
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
            'sp': {
                'entityId': service_provider.entity_id,
                'assertionConsumerService': {
                    'url': service_provider.acs_service.url,
                    'binding': service_provider.acs_service.binding.value
                },
                'NameIDFormat': service_provider.name_id_format,
                'x509cert': service_provider.certificate if service_provider.certificate else '',
                'privateKey': service_provider.private_key if service_provider.private_key else ''
            }
        }

        return onelogin_service_provider

    def get_identity_provider_settings(self, entity_id):
        """Returns a dictionary containing identity provider's settings in a OneLogin's SAML Toolkit format

        :return: Dictionary containing identity provider's settings in a OneLogin's SAML Toolkit format
        :rtype: Dict
        """
        if entity_id in self._identity_providers:
            return self._identity_providers[entity_id]

        identity_providers = filter(lambda idp: idp.entity_id == entity_id, self._configuration.identity_providers)

        if not identity_providers:
            raise SAMLConfigurationError(
                message=_('There is no identity provider with entityID = {0}'.format(entity_id)))

        identity_provider = identity_providers[0]
        identity_provider = self._get_identity_provider_settings(identity_provider)

        self._identity_providers[entity_id] = identity_provider

        return identity_provider

    def get_service_provider_settings(self):
        """Returns a dictionary containing service provider's settings in the OneLogin's SAML Toolkit format

        :return: Dictionary containing service provider's settings in the OneLogin's SAML Toolkit format
        :rtype: Dict
        """
        if self._service_provider is None:
            self._service_provider = self._get_service_provider_settings(self._configuration.service_provider)

        return self._service_provider

    def get_settings(self, idp_entity_id):
        """Returns SP and IdP settings in a OneLogin format

        :param idp_entity_id:
        :return:
        """
        onelogin_settings = {
            'debug': self._configuration.debug,
            'strict': self._configuration.strict
        }
        identity_provider_settings = self.get_identity_provider_settings(idp_entity_id)
        service_provider_settings = self.get_service_provider_settings()

        onelogin_settings.update(service_provider_settings)
        onelogin_settings.update(identity_provider_settings)

        settings = OneLogin_Saml2_Settings(onelogin_settings)

        return {
            'debug': self._configuration.debug,
            'strict': self._configuration.strict,
            'idp': settings.get_idp_data(),
            'sp': settings.get_sp_data(),
            'security': settings.get_security_data()
        }
