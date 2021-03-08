import cgi
import json
from threading import Lock

from contextlib2 import contextmanager
from flask_babel import lazy_gettext as _
from onelogin.saml2.settings import OneLogin_Saml2_Settings

from api.saml.metadata.federations import incommon
from api.saml.metadata.federations.model import (
    SAMLFederatedIdentityProvider,
    SAMLFederation,
)
from api.saml.metadata.model import SAMLAttributeType, SAMLServiceProviderMetadata
from api.saml.metadata.parser import SAMLMetadataParser
from core.exceptions import BaseError
from core.model.configuration import (
    ConfigurationAttributeType,
    ConfigurationFactory,
    ConfigurationGrouping,
    ConfigurationMetadata,
    ConfigurationOption,
)


class SAMLConfigurationError(BaseError):
    """Raised in the case of any configuration errors."""


class SAMLConfiguration(ConfigurationGrouping):
    """Contains SP and IdP settings."""

    service_provider_xml_metadata = ConfigurationMetadata(
        key="sp_xml_metadata",
        label=_("Service Provider's XML Metadata"),
        description=_(
            "SAML metadata of the Circulation Manager's Service Provider in an XML format. "
            "MUST contain exactly one SPSSODescriptor tag with at least one "
            "AssertionConsumerService tag with Binding attribute set to "
            "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST."
        ),
        type=ConfigurationAttributeType.TEXTAREA,
        required=True,
    )

    service_provider_private_key = ConfigurationMetadata(
        key="sp_private_key",
        label=_("Service Provider's Private Key"),
        description=_("Private key used for encrypting SAML requests."),
        type=ConfigurationAttributeType.TEXTAREA,
        required=False,
    )

    federated_identity_provider_entity_ids = ConfigurationMetadata(
        key="saml_federated_idp_entity_ids",
        label=_("List of Federated IdPs"),
        description=_(
            "List of federated (for example, from InCommon Federation) IdPs supported by this authentication provider. "
            "Try to type the name of the IdP to find it in the list."
        ),
        type=ConfigurationAttributeType.MENU,
        required=False,
        options=[],
        default=[],
        format="narrow",
    )

    patron_id_use_name_id = ConfigurationMetadata(
        key="saml_patron_id_use_name_id",
        label=_("Patron ID: Use SAML NameID"),
        description=_(
            "Boolean value indicating whether SAML NameID should be searched for a unique patron ID: "
            "<br>"
            "- <b>0</b> means that NameID won't be used, "
            "<br>"
            "- <b>1</b> (default) means that Circulation Manager will scan NameID in search for a patron ID. "
            "If found, it will supersede any SAML attributes selected in the next section."
        ),
        type=ConfigurationAttributeType.NUMBER,
        required=False,
        default=1,
    )

    patron_id_attributes = ConfigurationMetadata(
        key="saml_patron_id_attributes",
        label=_("Patron ID: SAML Attributes"),
        description=_(
            "List of SAML attributes that MAY contain a unique patron ID. "
            "The attributes will be scanned sequentially in the order you chose them, "
            "and the first existing attribute will be used to extract a unique patron ID."
            "<br>"
            "NOTE: If a SAML attribute contains several values, only the first will be used."
        ),
        type=ConfigurationAttributeType.MENU,
        required=False,
        options=[
            ConfigurationOption(attribute.name, attribute.name)
            for attribute in SAMLAttributeType
        ],
        default=[
            SAMLAttributeType.eduPersonUniqueId.name,
            SAMLAttributeType.eduPersonTargetedID.name,
            SAMLAttributeType.eduPersonPrincipalName.name,
            SAMLAttributeType.uid.name,
        ],
        format="narrow",
    )

    patron_id_regular_expression = ConfigurationMetadata(
        key="saml_patron_id_regular_expression",
        label=_("Patron ID: Regular expression"),
        description=_(
            "Regular expression used to extract a unique patron ID from the attributes "
            "specified in <b>Patron ID: SAML Attributes</b> and/or NameID (if <b>Patron ID: Use SAML NameID</b> is 1). "
            "<br>"
            "The expression MUST contain a named group <b>patron_id</b> used to match the patron ID. "
            "For example:"
            "<br>"
            "<pre>"
            "{the_regex_pattern}"
            "</pre>"
            "The expression will extract the <b>patron_id</b> from the first SAML attribute that matches "
            "or NameID if it matches the expression."
        ).format(the_regex_pattern=cgi.escape(r"(?P<patron_id>.+)@university\.org")),
        type=ConfigurationAttributeType.TEXT,
        required=False,
    )

    non_federated_identity_provider_xml_metadata = ConfigurationMetadata(
        key="idp_xml_metadata",
        label=_("Identity Provider's XML metadata"),
        description=_(
            "SAML metadata of Identity Providers in an XML format. "
            "MAY contain multiple IDPSSODescriptor tags but each of them MUST contain "
            "at least one SingleSignOnService tag with Binding attribute set to "
            "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect."
        ),
        type=ConfigurationAttributeType.TEXTAREA,
        required=False,
    )

    session_lifetime = ConfigurationMetadata(
        key="saml_session_lifetime",
        label=_("Session Lifetime"),
        description=_(
            "This configuration setting determines how long "
            "a session created by the SAML authentication provider will live in days. "
            "By default it's empty meaning that the lifetime of the Circulation Manager's session "
            "is exactly the same as the lifetime of the IdP's session. "
            "Setting this value to a specific number will override this behaviour."
            "<br>"
            "NOTE: This setting affects the session's lifetime only Circulation Manager's side. "
            "Accessing content protected by SAML will still be governed by the IdP and patrons "
            "will have to reauthenticate each time the IdP's session expires."
        ),
        type=ConfigurationAttributeType.NUMBER,
        required=False,
        default=None,
    )

    filter_expression = ConfigurationMetadata(
        key="saml_filter_expression",
        label=_("Filter Expression"),
        description=_(
            "Python expression used for filtering out patrons by their SAML attributes."
            "<br>"
            "<br>"
            'For example, if you want to authenticate using SAML only patrons having "eresources" '
            'as the value of their "eduPersonEntitlement" then you need to use the following expression:'
            "<br>"
            "<pre>"
            """
"urn:mace:nyu.edu:entl:lib:eresources" == subject.attribute_statement.attributes["eduPersonEntitlement"].values[0]
"""
            "</pre>"
            "<br>"
            'If "eduPersonEntitlement" can have multiple values, you can use the following expression:'
            "<br>"
            "<pre>"
            """
"urn:mace:nyu.edu:entl:lib:eresources" in subject.attribute_statement.attributes["eduPersonEntitlement"].values
"""
            "</pre>"
        ),
        type=ConfigurationAttributeType.TEXTAREA,
        required=False,
    )

    service_provider_strict_mode = ConfigurationMetadata(
        key="strict",
        label=_("Service Provider's Strict Mode"),
        description=_(
            "If strict is 1, then the Python Toolkit will reject unsigned or unencrypted messages "
            "if it expects them to be signed or encrypted. Also, it will reject the messages "
            "if the SAML standard is not strictly followed."
        ),
        type=ConfigurationAttributeType.NUMBER,
        required=False,
        default=0,
    )

    service_provider_debug_mode = ConfigurationMetadata(
        key="debug",
        label=_("Service Provider's Debug Mode"),
        description=_("Enable debug mode (outputs errors)."),
        type=ConfigurationAttributeType.NUMBER,
        required=False,
        default=0,
    )

    IDP_DISPLAY_NAME_DEFAULT_TEMPLATE = "Identity Provider #{0}"

    def __init__(self, configuration_storage, db, metadata_parser):
        """Initializes a new instance of SAMLConfiguration class

        :param configuration_storage: SAML configuration storage
        :type configuration_storage: ConfigurationStorage

        :param metadata_parser: SAML metadata parser
        :type metadata_parser: SAMLMetadataParser
        """
        super(SAMLConfiguration, self).__init__(configuration_storage, db)

        self._metadata_parser = metadata_parser

        self._identity_providers = None
        self._service_provider = None

    def _get_federated_identity_providers(self, db):
        """Return a list of federated IdPs corresponding to the entity IDs selected by the admin.

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :return: List of SAMLFederatedIdP objects
        :rtype: List[api.saml.metadata.federations.model.SAMLFederatedIdP]
        """
        if not self.federated_identity_provider_entity_ids:
            return []

        federated_identity_provider_entity_ids = json.loads(
            self.federated_identity_provider_entity_ids
        )

        return (
            db.query(SAMLFederatedIdentityProvider)
            .filter(
                SAMLFederatedIdentityProvider.entity_id.in_(
                    federated_identity_provider_entity_ids
                )
            )
            .all()
        )

    def _load_identity_providers(self, db):
        """Loads IdP settings from the library's configuration settings

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :return: List of IdentityProviderMetadata objects
        :rtype: List[IdentityProviderMetadata]

        :raise: SAMLParsingError
        """
        identity_providers = []

        if self.non_federated_identity_provider_xml_metadata:
            parsing_results = self._metadata_parser.parse(
                self.non_federated_identity_provider_xml_metadata
            )
            identity_providers = [
                parsing_result.provider for parsing_result in parsing_results
            ]

        if self.federated_identity_provider_entity_ids:
            for identity_provider_metadata in self._get_federated_identity_providers(
                db
            ):
                parsing_results = self._metadata_parser.parse(
                    identity_provider_metadata.xml_metadata
                )

                for parsing_result in parsing_results:
                    identity_providers.append(parsing_result.provider)

        return identity_providers

    def _load_service_provider(self, db):
        """Loads SP settings from the library's configuration settings

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :return: SAMLServiceProviderMetadata object
        :rtype: SAMLServiceProviderMetadata

        :raise: SAMLParsingError
        """
        parsing_results = self._metadata_parser.parse(
            self.service_provider_xml_metadata
        )

        if not isinstance(parsing_results, list) or len(parsing_results) != 1:
            raise SAMLConfigurationError(
                _("SAML Service Provider's configuration is not correct")
            )

        parsing_result = parsing_results[0]
        service_provider = parsing_result.provider

        if not isinstance(service_provider, SAMLServiceProviderMetadata):
            raise SAMLConfigurationError(
                _("SAML Service Provider's configuration is not correct")
            )

        service_provider.private_key = (
            self.service_provider_private_key
            if self.service_provider_private_key
            else ""
        )

        return service_provider

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


class SAMLSettings(dict):
    """Converts SAMLConfiguration to SETTINGS-compatible dictionary.

    Once a database session becomes available,
    this class updates SAMLConfiguration with a list of available federated IdPs.
    """

    _mutex = Lock()

    def __get__(self, instance, owner):
        """Return a SETTINGS-compatible dictionary.

        :return: SETTINGS-compatible dictionary
        :rtype: Dict
        """
        with self._mutex:
            if not SAMLConfiguration.federated_identity_provider_entity_ids.options:
                try:
                    from api.app import app

                    # 1. Load all InCommon IdPs from the database
                    incommon_federated_identity_providers = (
                        app._db.query(
                            SAMLFederatedIdentityProvider.entity_id,
                            SAMLFederatedIdentityProvider.display_name,
                        )
                        .join(SAMLFederation)
                        .filter(SAMLFederation.type == incommon.FEDERATION_TYPE)
                        .order_by(SAMLFederatedIdentityProvider.display_name)
                    ).all()

                    # 2. Convert SAMLFederatedIdentityProvider objects to ConfigurationOption objects
                    configuration_options = []
                    for (
                        incommon_federated_identity_provider
                    ) in incommon_federated_identity_providers:
                        configuration_options.append(
                            ConfigurationOption(
                                key=incommon_federated_identity_provider[0],
                                label=incommon_federated_identity_provider[1],
                            )
                        )

                    # 3. Update SAMLConfiguration.federated_identity_provider_entity_ids.options
                    SAMLConfiguration.federated_identity_provider_entity_ids = ConfigurationMetadata(
                        SAMLConfiguration.federated_identity_provider_entity_ids.key,
                        SAMLConfiguration.federated_identity_provider_entity_ids.label,
                        SAMLConfiguration.federated_identity_provider_entity_ids.description,
                        SAMLConfiguration.federated_identity_provider_entity_ids.type,
                        SAMLConfiguration.federated_identity_provider_entity_ids.required,
                        SAMLConfiguration.federated_identity_provider_entity_ids.default,
                        configuration_options,
                        SAMLConfiguration.federated_identity_provider_entity_ids.category,
                        SAMLConfiguration.federated_identity_provider_entity_ids.format,
                        SAMLConfiguration.federated_identity_provider_entity_ids.index,
                    )
                except:
                    pass

            # 4. Return updated settings
            return SAMLConfiguration.to_settings()


class SAMLConfigurationFactory(ConfigurationFactory):
    """Factory creating new instances of SAMLConfiguration class."""

    def __init__(self, parser):
        """Initialize a new instance of SAMLConfigurationFactory class.

        :param parser: SAMLMetadataParser object
        :type parser: api.saml.metadata.parser.SAMLMetadataParser
        """
        if not isinstance(parser, SAMLMetadataParser):
            raise ValueError(
                "Argument 'parser' must be an instance of {0} class".format(
                    SAMLMetadataParser
                )
            )

        self._parser = parser

    @contextmanager
    def create(self, configuration_storage, db, configuration_grouping_class):
        """Create a new instance of SAMLConfiguration.

        :param configuration_storage: ConfigurationStorage object
        :type configuration_storage: ConfigurationStorage

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param configuration_grouping_class: Configuration bucket's class
        :type configuration_grouping_class: Type[ConfigurationGrouping]

        :return: SAMLConfiguration object
        :rtype: SAMLConfiguration
        """
        if not issubclass(configuration_grouping_class, SAMLConfiguration):
            raise ValueError(
                "Argument 'configuration_grouping_class' must be a subclass of {0} class".format(
                    SAMLConfiguration
                )
            )

        with configuration_grouping_class(
            configuration_storage, db, self._parser
        ) as configuration_bucket:
            yield configuration_bucket


class SAMLOneLoginConfiguration(object):
    """Converts metadata objects to the OneLogin's SAML Toolkit format"""

    DEBUG = "debug"
    STRICT = "strict"

    ENTITY_ID = "entityId"
    URL = "url"
    BINDING = "binding"
    X509_CERT = "x509cert"
    X509_CERT_MULTI = "x509certMulti"
    SIGNING = "signing"
    ENCRYPTION = "encryption"

    IDP = "idp"
    SINGLE_SIGN_ON_SERVICE = "singleSignOnService"

    SP = "sp"
    ASSERTION_CONSUMER_SERVICE = "assertionConsumerService"
    NAME_ID_FORMAT = "NameIDFormat"
    PRIVATE_KEY = "privateKey"

    SECURITY = "security"
    AUTHN_REQUESTS_SIGNED = "authnRequestsSigned"

    def __init__(self, configuration):
        """Initializes a new instance of SAMLOneLoginConfiguration class

        :param configuration: Configuration object containing SAML metadata
        :type configuration: api.saml.configuration.model.SAMLConfiguration
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
                    self.BINDING: identity_provider.sso_service.binding.value,
                },
            },
            self.SECURITY: {
                self.AUTHN_REQUESTS_SIGNED: identity_provider.want_authn_requests_signed
            },
        }

        if (
            len(identity_provider.signing_certificates) == 1
            and len(identity_provider.encryption_certificates) == 1
            and identity_provider.signing_certificates[0]
            == identity_provider.encryption_certificates[0]
        ):
            onelogin_identity_provider[self.IDP][
                self.X509_CERT
            ] = identity_provider.signing_certificates[0]
        else:
            if len(identity_provider.signing_certificates) > 0:
                if self.X509_CERT_MULTI not in onelogin_identity_provider[self.IDP]:
                    onelogin_identity_provider[self.IDP][self.X509_CERT_MULTI] = {}

                onelogin_identity_provider[self.IDP][self.X509_CERT_MULTI][
                    self.SIGNING
                ] = identity_provider.signing_certificates
            if len(identity_provider.encryption_certificates) > 0:
                if self.X509_CERT_MULTI not in onelogin_identity_provider[self.IDP]:
                    onelogin_identity_provider[self.IDP][self.X509_CERT_MULTI] = {}

                onelogin_identity_provider[self.IDP][self.X509_CERT_MULTI][
                    self.ENCRYPTION
                ] = identity_provider.encryption_certificates

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
                    self.BINDING: service_provider.acs_service.binding.value,
                },
                self.NAME_ID_FORMAT: service_provider.name_id_format,
                self.X509_CERT: service_provider.certificate
                if service_provider.certificate
                else "",
                self.PRIVATE_KEY: service_provider.private_key
                if service_provider.private_key
                else "",
            },
            self.SECURITY: {
                self.AUTHN_REQUESTS_SIGNED: service_provider.authn_requests_signed
            },
        }

        return onelogin_service_provider

    @property
    def configuration(self):
        """Returns original configuration

        :return: Original configuration
        :rtype: api.saml.configuration.model.SAMLConfiguration
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

        identity_providers = [
            idp
            for idp in self._configuration.get_identity_providers(db)
            if idp.entity_id == idp_entity_id
        ]

        if not identity_providers:
            raise SAMLConfigurationError(
                _(
                    "There is no identity provider with entityID = {0}".format(
                        idp_entity_id
                    )
                )
            )

        if len(identity_providers) > 1:
            raise SAMLConfigurationError(
                _(
                    "There are multiple identity providers with entityID = {0}".format(
                        idp_entity_id
                    )
                )
            )

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
            self._service_provider = self._get_service_provider_settings(
                self._configuration.get_service_provider(db)
            )

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
            self.DEBUG: self._configuration.service_provider_debug_mode,
            self.STRICT: self._configuration.service_provider_strict_mode,
        }
        identity_provider_settings = self.get_identity_provider_settings(
            db, idp_entity_id
        )
        service_provider_settings = self.get_service_provider_settings(db)

        onelogin_settings.update(identity_provider_settings)
        onelogin_settings.update(service_provider_settings)

        # We need to use disjunction separately because dict.update just overwrites values
        onelogin_settings[self.SECURITY][self.AUTHN_REQUESTS_SIGNED] = (
            service_provider_settings[self.SECURITY][self.AUTHN_REQUESTS_SIGNED]
            or service_provider_settings[self.SECURITY][self.AUTHN_REQUESTS_SIGNED]
        )

        settings = OneLogin_Saml2_Settings(onelogin_settings)

        return {
            self.DEBUG: self._configuration.service_provider_debug_mode,
            self.STRICT: self._configuration.service_provider_strict_mode,
            self.IDP: settings.get_idp_data(),
            self.SP: settings.get_sp_data(),
            self.SECURITY: settings.get_security_data(),
        }
