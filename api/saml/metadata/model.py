import datetime
from enum import Enum
from json import JSONDecoder, JSONEncoder
from json.decoder import WHITESPACE

from onelogin.saml2.constants import OneLogin_Saml2_Constants

from core.util.string_helpers import is_string


class SAMLLocalizedMetadataItem(object):
    """Represents md:localizedNameType."""

    def __init__(self, value, language=None):
        """Initialize a new instance of SAMLLocalizedMetadataItem class.

        :param value: String containing the actual value
        :type value: string

        :param language: String containing language of the actual value
        :type language: Optional[string]
        """
        if not value and not is_string(value):
            raise ValueError("Argument 'value' must be a non-empty string")
        self._value = value
        self._language = language

    def __eq__(self, other):
        """Compare two SAMLLocalizedMetadataItem objects.

        :param other: SAMLLocalizedMetadataItem object
        :type other: SAMLLocalizedMetadataItem

        :return: Boolean value indicating whether two items are equal
        :rtype: bool
        """
        if not isinstance(other, SAMLLocalizedMetadataItem):
            return False

        return self.value == other.value and self.language == other.language

    def __repr__(self):
        """Return a string representation.

        :return: String representation
        :rtype: str
        """
        return "<SAMLLocalizableMetadataItem(value={0}, language={1})>".format(
            self.value, self.language
        )

    @property
    def value(self):
        """Return the actual value.

        :return: Actual value
        :rtype: string
        """
        return self._value

    @property
    def language(self):
        """Return the value's language.

        :return: Value's language
        :rtype: string
        """
        return self._language


class SAMLOrganization(object):
    """Represents md:Organization and contains basic information about an organization
    responsible for a SAML entity or role.
    """

    def __init__(
        self,
        organization_names=None,
        organization_display_names=None,
        organization_urls=None,
    ):
        """Initialize a new instance of SAMLOrganization class.

        :param organization_names: (Optional) List of localized organization names that may or may not be
            suitable for human consumption
        :type organization_names: Optional[List[SAMLLocalizedMetadataItem]]

        :param organization_display_names: (Optional) List of localized organization names that
            suitable for human consumption
        :type organization_display_names: Optional[List[SAMLLocalizedMetadataItem]]

        :param organization_urls: (Optional) List of localized organization URIs that
            specify a location to which to direct a user for additional information
        :type organization_urls: Optional[List[SAMLLocalizedMetadataItem]]
        """
        if organization_names:
            for organization_name in organization_names:
                if not isinstance(organization_name, SAMLLocalizedMetadataItem):
                    raise ValueError(
                        'Argument "organization_name" must be an instance of {0} class'.format(
                            SAMLLocalizedMetadataItem
                        )
                    )

        if organization_display_names:
            for organization_display_name in organization_display_names:
                if not isinstance(organization_display_name, SAMLLocalizedMetadataItem):
                    raise ValueError(
                        'Argument "organization_display_name" must be an instance of {0} class'.format(
                            SAMLLocalizedMetadataItem
                        )
                    )

        if organization_urls:
            for organization_url in organization_urls:
                if not isinstance(organization_url, SAMLLocalizedMetadataItem):
                    raise ValueError(
                        'Argument "organization_url" must be an instance of {0} class'.format(
                            SAMLLocalizedMetadataItem
                        )
                    )

        self._organization_names = organization_names
        self._organization_display_names = organization_display_names
        self._organization_urls = organization_urls

    def __eq__(self, other):
        """Compare two SAMLOrganization objects.

        :param other: SAMLOrganization object
        :type other: SAMLOrganization

        :return: Boolean value indicating whether two items are equal
        :rtype: bool
        """
        if not isinstance(other, SAMLOrganization):
            return False

        return (
            self.organization_names == other.organization_names
            and self.organization_display_names == other.organization_display_names
            and self.organization_urls == other.organization_urls
        )

    @property
    def organization_names(self):
        """Return a list of localized organization names that may or may not be
        suitable for human consumption.

        :return: List of localized organization names that may or may not be
            suitable for human consumption
        :rtype: Optional[List[SAMLLocalizedMetadataItem]]
        """
        return self._organization_names

    @property
    def organization_display_names(self):
        """Return a list of localized organization names that suitable for human consumption.

        :return: List of localized organization names that suitable for human consumption
        :rtype: Optional[List[SAMLLocalizedMetadataItem]]
        """
        return self._organization_display_names

    @property
    def organization_urls(self):
        """Return a list of localized organization URIs that specify a location to which to direct a user for
        additional information.

        :return: List of localized organization URIs that
            specify a location to which to direct a user for additional information
        :rtype: Optional[List[SAMLLocalizedMetadataItem]]
        """
        return self._organization_urls


class SAMLUIInfo(object):
    """Represents mdui:UIInfoType and contains values that can be shown in the UI to describe IdPs/SPs."""

    def __init__(
        self,
        display_names=None,
        descriptions=None,
        information_urls=None,
        privacy_statement_urls=None,
        logo_urls=None,
    ):
        """Initialize a new instance of SAMLUIInfo class.

        :param display_names: (Optional) List of localized display names
        :type display_names: Optional[List[SAMLLocalizedMetadataItem]]

        :param descriptions: (Optional) List of localized descriptions
        :type descriptions: Optional[List[SAMLLocalizedMetadataItem]]

        :param information_urls: (Optional) List of localized information URLs
        :type information_urls: Optional[List[SAMLLocalizedMetadataItem]]

        :param privacy_statement_urls: (Optional) List of localized privacy statement URLs
        :type privacy_statement_urls: Optional[List[SAMLLocalizedMetadataItem]]

        :param logo_urls: (Optional) List of localized logo URLs
        :type logo_urls: Optional[List[SAMLLocalizedMetadataItem]]
        """
        if display_names:
            for display_name in display_names:
                if not isinstance(display_name, SAMLLocalizedMetadataItem):
                    raise ValueError(
                        'Argument "display_names" must be an instance of {0} class'.format(
                            SAMLLocalizedMetadataItem
                        )
                    )

        if descriptions:
            for description in descriptions:
                if not isinstance(description, SAMLLocalizedMetadataItem):
                    raise ValueError(
                        'Argument "description" must be an instance of {0} class'.format(
                            SAMLLocalizedMetadataItem
                        )
                    )

        if information_urls:
            for information_url in information_urls:
                if not isinstance(information_url, SAMLLocalizedMetadataItem):
                    raise ValueError(
                        'Argument "information_url" must be an instance of {0} class'.format(
                            SAMLLocalizedMetadataItem
                        )
                    )

        if privacy_statement_urls:
            for privacy_statement_url in privacy_statement_urls:
                if not isinstance(privacy_statement_url, SAMLLocalizedMetadataItem):
                    raise ValueError(
                        'Argument "privacy_statement_url" must be an instance of {0} class'.format(
                            SAMLLocalizedMetadataItem
                        )
                    )

        if logo_urls:
            for logo_url in logo_urls:
                if not isinstance(logo_url, SAMLLocalizedMetadataItem):
                    raise ValueError("logo_urls must have type LocalizableMetadataItem")

        self._display_names = display_names
        self._descriptions = descriptions
        self._information_urls = information_urls
        self._privacy_statement_urls = privacy_statement_urls
        self._logo_urls = logo_urls

    def __eq__(self, other):
        """Compares two UIInfo objects

        :param other: UIInfo object
        :type other: SAMLUIInfo

        :return: Boolean value indicating whether two items are equal
        :rtype: bool
        """
        if not isinstance(other, SAMLUIInfo):
            return False

        return (
            self.display_names == other.display_names
            and self.descriptions == other.descriptions
            and self.information_urls == other.information_urls
            and self.privacy_statement_urls == other.privacy_statement_urls
            and self.logo_urls == other.logo_urls
        )

    @property
    def display_names(self):
        """Returns a list of localizable display names

        :return: List of localizable display names
        :rtype: List[LocalizableMetadataItem]
        """
        return self._display_names

    @property
    def descriptions(self):
        """Returns a list of localizable descriptions

        :return: List of localizable descriptions
        :rtype: List[LocalizableMetadataItem]
        """
        return self._descriptions

    @property
    def information_urls(self):
        """Returns a list of localizable information URLs

        :return: List of localizable information URLs
        :rtype: List[LocalizableMetadataItem]
        """
        return self._information_urls

    @property
    def privacy_statement_urls(self):
        """Returns a list of localizable privacy statement URLs

        :return: List of localizable privacy statement URLs
        :rtype: List[LocalizableMetadataItem]
        """
        return self._privacy_statement_urls

    @property
    def logo_urls(self):
        """Returns a list of localizable logo URLs

        :return: List of localizable logo URLs
        :rtype: List[LocalizableMetadataItem]
        """
        return self._logo_urls


class SAMLBinding(Enum):
    """Enumeration of SAML bindings"""

    HTTP_POST = OneLogin_Saml2_Constants.BINDING_HTTP_POST
    HTTP_REDIRECT = OneLogin_Saml2_Constants.BINDING_HTTP_REDIRECT
    HTTP_ARTIFACT = OneLogin_Saml2_Constants.BINDING_HTTP_ARTIFACT
    SOAP = OneLogin_Saml2_Constants.BINDING_SOAP
    DEFLATE = OneLogin_Saml2_Constants.BINDING_DEFLATE


class SAMLNameIDFormat(Enum):
    """Enumeration of SAML name ID formats"""

    EMAIL_ADDRESS = "urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress"
    X509_SUBJECT_NAME = "urn:oasis:names:tc:SAML:1.1:nameid-format:X509SubjectName"
    WINDOWS_DOMAIN_QUALIFIED_NAME = (
        "urn:oasis:names:tc:SAML:1.1:nameid-format:WindowsDomainQualifiedName"
    )
    UNSPECIFIED = "urn:oasis:names:tc:SAML:1.1:nameid-format:unspecified"
    KERBEROS = "urn:oasis:names:tc:SAML:2.0:nameid-format:kerberos"
    ENTITY = "urn:oasis:names:tc:SAML:2.0:nameid-format:entity"
    TRANSIENT = "urn:oasis:names:tc:SAML:2.0:nameid-format:transient"
    PERSISTENT = "urn:oasis:names:tc:SAML:2.0:nameid-format:persistent"
    ENCRYPTED = "urn:oasis:names:tc:SAML:2.0:nameid-format:encrypted"


class SAMLService(object):
    """Represents a service: IdP's SingleSignOnService, SingleLogOutService, SP's AssertionConsumerService"""

    def __init__(self, url, binding):
        """Initializes a new instance of Service class

        :param url: Service's URL
        :type url: string

        :param binding: Service's binding
        :type binding: SAMLBinding
        """
        if not isinstance(url, str):
            raise ValueError("url must be a string")
        if not isinstance(binding, SAMLBinding):
            raise ValueError("binding must have type Binding")

        self._url = url
        self._binding = binding

    def __eq__(self, other):
        """Compares two Service objects

        :param other: Service object
        :type other: SAMLService

        :return: Boolean value indicating whether two items are equal
        :rtype: bool
        """
        if not isinstance(other, SAMLService):
            return False

        return self.url == other.url and self.binding == other.binding

    @property
    def url(self):
        """Returns the service's URL

        :return: Service's URL
        :rtype: string
        """
        return self._url

    @property
    def binding(self):
        """Returns the service's binding

        :return: Service's binding
        :rtype: SAMLBinding
        """
        return self._binding


class SAMLProviderMetadata(object):
    """Base class for IdentityProvider and ServiceProvider classes"""

    def __init__(
        self,
        entity_id,
        ui_info,
        organization,
        name_id_format=SAMLNameIDFormat.UNSPECIFIED,
    ):
        """Initializes a new instance of ProviderMetadata class

        :param entity_id: Provider's entityID
        :type entity_id: string

        :param ui_info: UIInfo object containing "UI" metadata of the provider
        :type ui_info: SAMLUIInfo

        :param organization: Organization object containing basic information about an organization
            responsible for a SAML entity or role
        :type organization: SAMLOrganization

        :param name_id_format: String defining the name identifier formats supported by the identity provider
        :type name_id_format: string
        """
        if not isinstance(ui_info, SAMLUIInfo):
            raise ValueError("ui_info must have type UIInfo")

        if not isinstance(organization, SAMLOrganization):
            raise ValueError("organization must have type UIInfo")

        if not isinstance(name_id_format, str):
            raise ValueError("name_id_format must be a string")

        self._entity_id = entity_id
        self._ui_info = ui_info
        self._organization = organization
        self._name_id_format = name_id_format

    def __eq__(self, other):
        """Compares two ProviderMetadata objects

        :param other: ProviderMetadata object
        :type other: SAMLProviderMetadata

        :return: Boolean value indicating whether two items are equal
        :rtype: bool
        """
        if not isinstance(other, SAMLProviderMetadata):
            return False

        return (
            self.entity_id == other.entity_id
            and self.ui_info == other.ui_info
            and self.organization == other.organization
            and self.name_id_format == other.name_id_format
        )

    @property
    def entity_id(self):
        """Returns the provider's entityID

        :return: Provider's entityID
        :rtype: string
        """
        return self._entity_id

    @entity_id.setter
    def entity_id(self, value):
        """Sets the provider's entityID

        :return: Provider's entityID
        :rtype: string
        """
        self._entity_id = value

    @property
    def ui_info(self):
        """Returns the provider's UIInfo object
        :return: Provider's UIInfo object
        :rtype: SAMLUIInfo
        """
        return self._ui_info

    @property
    def organization(self):
        """Returns the provider's Organization object
        :return: Provider's Organization object
        :rtype: SAMLOrganization
        """
        return self._organization

    @property
    def name_id_format(self):
        """Returns the name ID format

        :return: Name ID format
        :rtype: string
        """
        return self._name_id_format


class SAMLIdentityProviderMetadata(SAMLProviderMetadata):
    """Represents IdP metadata"""

    def __init__(
        self,
        entity_id,
        ui_info,
        organization,
        name_id_format,
        sso_service,
        slo_service=None,
        want_authn_requests_signed=False,
        signing_certificates=None,
        encryption_certificates=None,
    ):
        """Initializes a new instance of IdentityProviderMetadata

        :param entity_id: String containing this IdP's entityID
        :type entity_id: string

        :param ui_info: UIInfo object containing this IdP's description which can be shown the UI
        :type ui_info: SAMLUIInfo

        :param organization: Organization object containing basic information about an organization
            responsible for a SAML entity or role
        :type organization: SAMLOrganization

        :param name_id_format: String defining the name identifier formats supported by the identity provider
        :type name_id_format: string

        :param sso_service: Service object containing information about Single Sign-On (SSO) service
        :type sso_service: SAMLService

        :param slo_service: (Optional) Service object containing information about Single Log-Out (SLO) service
        :type slo_service: Optional[Service]

        :param want_authn_requests_signed: (Optional) Boolean value intended to indicate to service providers
            whether or not they can expect an unsigned <AuthnRequest> message to be accepted by the identity provider
        :type want_authn_requests_signed: Optional[bool]

        :param signing_certificates: (Optional) Certificate in X.509 format used for signing <AuthnResponse> messages
        :type signing_certificates: Optional[List[string]]

        :param encryption_certificates: (Optional) Certificate in X.509 format used for encrypting <AuthnResponse>
        :type encryption_certificates: Optional[List[string]]
        """
        super(SAMLIdentityProviderMetadata, self).__init__(
            entity_id, ui_info, organization, name_id_format
        )

        if not isinstance(sso_service, SAMLService):
            raise ValueError("sso_service must have type Service")

        if slo_service is not None and not isinstance(slo_service, SAMLService):
            raise ValueError("slo_service must have type Service")

        self._sso_service = sso_service
        self._slo_service = slo_service
        self._want_authn_requests_signed = bool(want_authn_requests_signed)
        self._signing_certificates = (
            signing_certificates if signing_certificates else []
        )
        self._encryption_certificates = (
            encryption_certificates if encryption_certificates else []
        )

    def __eq__(self, other):
        """Compares two IdentityProviderMetadata objects

        :param other: IdentityProviderMetadata object
        :type other: SAMLIdentityProviderMetadata

        :return: Boolean value indicating whether two items are equal
        :rtype: bool
        """
        if not super(SAMLIdentityProviderMetadata, self).__eq__(other):
            return False

        if not isinstance(other, SAMLIdentityProviderMetadata):
            return False

        return (
            self.sso_service == other.sso_service
            and self.slo_service == other.slo_service
            and self.want_authn_requests_signed == other.want_authn_requests_signed
            and self.signing_certificates == other.signing_certificates
            and self.encryption_certificates == other.encryption_certificates
        )

    @property
    def sso_service(self):
        """Returns Single Sign-On service's metadata

        :return: Single Sign-On service's metadata
        :rtype: SAMLService
        """
        return self._sso_service

    @property
    def slo_service(self):
        """Returns Single Log-Out service's metadata

        :return: Single Log-Out service's metadata
        :rtype: SAMLService
        """
        return self._slo_service

    @property
    def want_authn_requests_signed(self):
        """Returns the boolean value indicating to service providers
        whether or not they can expect an unsigned <AuthnRequest> message to be accepted by the identity provider

        :return: Boolean value indicating to service providers
            whether or not they can expect an unsigned <AuthnRequest> message to be accepted by the identity provider
        :rtype: bool
        """
        return self._want_authn_requests_signed

    @property
    def signing_certificates(self):
        """Returns a list of signing certificates

        :return: List of signing certificates
        :rtype: List[string]
        """
        return self._signing_certificates

    @property
    def encryption_certificates(self):
        """
        Returns a list of encryption certificates

        :return: List of encryption certificates
        :rtype: List[string]
        """
        return self._encryption_certificates


class SAMLServiceProviderMetadata(SAMLProviderMetadata):
    """Represents SP metadata"""

    def __init__(
        self,
        entity_id,
        ui_info,
        organization,
        name_id_format,
        acs_service,
        authn_requests_signed=False,
        want_assertions_signed=False,
        certificate=None,
        private_key=None,
    ):
        """Initializes a new instance of ServiceProviderMetadata class

        :param entity_id: String containing this IdP's entityID
        :type entity_id: string

        :param ui_info: UIInfo object containing this IdP's description which can be shown the UI
        :type ui_info: SAMLUIInfo

        :param organization: Organization object containing basic information about an organization
            responsible for a SAML entity or role
        :type organization: SAMLOrganization

        :param name_id_format: String defining the name identifier formats supported by the identity provider
        :type name_id_format: string

        :param acs_service: Service object describing AssertionConsumerService
        :type acs_service: SAMLService

        :param authn_requests_signed: (Optional) Attribute that indicates whether the <samlp:AuthnRequest> messages
            sent by this service provider will be signed. If omitted, the value is assumed to be false
        :type authn_requests_signed: bool

        :param want_assertions_signed: (Optional) Attribute that indicates a requirement for the <saml:Assertion>
            elements received by this service provider to be signed. If omitted, the value is assumed to be false
        :type want_assertions_signed: bool

        :param certificate: (Optional) Certificate in X.509 format containing a public key used
            for signing SAML requests
        :type certificate: string

        :param private_key: (Optional) Private key used for encrypting SAML requests
        :type private_key: string
        """
        super(SAMLServiceProviderMetadata, self).__init__(
            entity_id, ui_info, organization, name_id_format
        )

        if not isinstance(acs_service, SAMLService):
            raise ValueError("acs_service must have type Service")

        self._name_id_format = name_id_format
        self._acs_service = acs_service
        self._authn_requests_signed = authn_requests_signed
        self._want_assertions_signed = want_assertions_signed
        self._certificate = certificate if certificate else ""
        self._private_key = private_key if private_key else ""

    def __eq__(self, other):
        """Compares two ServiceProviderMetadata objects

        :param other: ServiceProviderMetadata object
        :type other: SAMLServiceProviderMetadata

        :return: Boolean value indicating whether two items are equal
        :rtype: bool
        """
        if not super(SAMLServiceProviderMetadata, self).__eq__(other):
            return False

        if not isinstance(other, SAMLServiceProviderMetadata):
            return False

        return (
            self.acs_service == other.acs_service
            and self.authn_requests_signed == other.authn_requests_signed
            and self.want_assertions_signed == other.want_assertions_signed
            and self.certificate == other.certificate
            and self.private_key == other.private_key
        )

    @property
    def acs_service(self):
        """Returns Assertion Consumer service's metadata

        :return: Assertion Consumer service's metadata
        :rtype: SAMLService
        """
        return self._acs_service

    @property
    def authn_requests_signed(self):
        """Returns the value that indicates whether the <samlp:AuthnRequest> messages
        sent by this service provider will be signed

        :return: Value that indicates whether the <samlp:AuthnRequest> messages
            sent by this service provider will be signed
        :rtype: bool
        """
        return self._authn_requests_signed

    @property
    def want_assertions_signed(self):
        """Returns the value that indicates a requirement for the <saml:Assertion>
        elements received by this service provider to be signed

        :return: Value that indicates a requirement for the <saml:Assertion>
            elements received by this service provider to be signed
        :rtype: bool
        """
        return self._want_assertions_signed

    @property
    def certificate(self):
        """Returns the certificate in X.509 format containing the public key used for signing SAML requests

        :return: Certificate in X.509 format containing the public key used for signing SAML requests
        :rtype: string
        """
        return self._certificate

    @property
    def private_key(self):
        """Returns the private key used for encrypting SAML requests

        :return: Private key used for encrypting SAML requests
        :rtype: string
        """
        return self._private_key

    @private_key.setter
    def private_key(self, value):
        """Returns the private key used for encrypting SAML requests

        :param value: New private key
        :type value: string

        :return: Private key used for encrypting SAML requests
        :rtype: string
        """
        self._private_key = value


class SAMLNameID(object):
    """Represents saml2:NameID"""

    def __init__(self, name_format, name_qualifier, sp_name_qualifier, name_id):
        """Initializes a new instance of NameID class

        :param name_format: Name ID's format
        :type name_format: string

        :param name_qualifier: The security or administrative domain that qualifies the name identifier of the subject.
            This attribute provides a means to federate names from disparate user stores without collision
        :type name_qualifier: string

        :param sp_name_qualifier: Further qualifies a federated name identifier with the name of the service provider
            or affiliation of providers which has federated the principal's identity
        :type sp_name_qualifier: string

        :param name_id: Name ID value
        :type name_id: string
        """
        self._name_format = name_format
        self._name_qualifier = name_qualifier
        self._sp_name_qualifier = sp_name_qualifier
        self._name_id = name_id

    def __eq__(self, other):
        """Compares two NameID objects

        :param other: NameID object
        :type other: SAMLNameID

        :return: Boolean value indicating whether two items are equal
        :rtype: bool
        """
        if not isinstance(other, SAMLNameID):
            return False

        return (
            self.name_format == other.name_format
            and self.name_qualifier == other.name_qualifier
            and self.sp_name_qualifier == other.sp_name_qualifier
            and self.name_id == other.name_id
        )

    def __repr__(self):
        """Return a string representation.

        :return: String representation
        :rtype: str
        """
        return "<NameID(name_format={0}, name_qualifier={1}, sp_name_qualifier={2}, name_id={3})>".format(
            self.name_format, self.name_qualifier, self.sp_name_qualifier, self.name_id
        )

    @property
    def name_format(self):
        """Returns name ID's format

        :return: Name ID's format
        :rtype: string
        """
        return self._name_format

    @property
    def name_qualifier(self):
        """Returns the security or administrative domain that qualifies the name identifier of the subject.
        This attribute provides a means to federate names from disparate user stores without collision

        :return: Security or administrative domain that qualifies the name identifier of the subject.
            This attribute provides a means to federate names from disparate user stores without collision
        :rtype: string
        """
        return self._name_qualifier

    @property
    def sp_name_qualifier(self):
        """Returns the attribute that further qualifies a federated name identifier with the name of the service provider
        or affiliation of providers which has federated the principal's identity

        :return: Attribute that further qualifies a federated name identifier with the name of the service provider
            or affiliation of providers which has federated the principal's identity
        :rtype: string
        """
        return self._sp_name_qualifier

    @property
    def name_id(self):
        """Returns name ID

        :return: Name ID
        :rtype: string
        """
        return self._name_id


class SAMLAttributeType(Enum):
    """Enumeration of different attributes supported by different SAML IdPs"""

    uid = "urn:oid:0.9.2342.19200300.100.1.1"

    givenName = "urn:oid:2.5.4.42"
    surname = "urn:oid:2.5.4.4"
    mail = "urn:oid:0.9.2342.19200300.100.1.3"
    displayName = "urn:oid:2.16.840.1.113730.3.1.241"

    eduPerson = "urn:oid:1.3.6.1.4.1.5923.1.1.2"
    eduPersonAffiliation = "urn:oid:1.3.6.1.4.1.5923.1.1.1.1"
    eduPersonNickname = "urn:oid:1.3.6.1.4.1.5923.1.1.1.2"
    eduPersonOrgDN = "urn:oid:1.3.6.1.4.1.5923.1.1.1.3"
    eduPersonOrgUnitDN = "urn:oid:1.3.6.1.4.1.5923.1.1.1.4"
    eduPersonPrimaryAffiliation = "urn:oid:1.3.6.1.4.1.5923.1.1.1.5"
    eduPersonPrincipalName = "urn:oid:1.3.6.1.4.1.5923.1.1.1.6"
    eduPersonEntitlement = "urn:oid:1.3.6.1.4.1.5923.1.1.1.7"
    eduPersonPrimaryOrgUnitDN = "urn:oid:1.3.6.1.4.1.5923.1.1.1.8"
    eduPersonScopedAffiliation = "urn:oid:1.3.6.1.4.1.5923.1.1.1.9"
    eduPersonTargetedID = "urn:oid:1.3.6.1.4.1.5923.1.1.1.10"
    eduPersonAssurance = "urn:oid:1.3.6.1.4.1.5923.1.1.1.11"
    eduPersonOrcid = "urn:oid:1.3.6.1.4.1.5923.1.1.1.12"
    eduPersonUniqueId = "urn:oid:1.3.6.1.4.1.5923.1.1.1.13"
    eduPersonPrincipalNamePrior = "urn:oid:1.3.6.1.4.1.5923.1.1.1.16"

    eduOrg = "urn:oid:1.3.6.1.4.1.5923.1.2.2"
    eduOrgHomePageURI = "urn:oid:1.3.6.1.4.1.5923.1.2.1.2"
    eduOrgIdentityAuthNPolicyURI = "urn:oid:1.3.6.1.4.1.5923.1.2.1.3"
    eduOrgLegalName = "urn:oid:1.3.6.1.4.1.5923.1.2.1.4"
    eduOrgSuperiorURI = "urn:oid:1.3.6.1.4.1.5923.1.2.1.5"
    eduOrgWhitePagesURI = "urn:oid:1.3.6.1.4.1.5923.1.2.1.6"


class SAMLAttribute(object):
    """Represents saml2:Attribute"""

    def __init__(self, name, values, friendly_name=None, name_format=None):
        """Initializes a new instance of Attribute class

        :param name: Attribute's name
        :type name: string

        :param values: List of values
        :type values: List

        :param friendly_name: Attribute's friendly name
        :type friendly_name: string

        :param name_format: Attribute's name format
        :type name_format: string
        """
        self._name = name
        self._values = values
        self._friendly_name = friendly_name
        self._name_format = name_format

    def __eq__(self, other):
        """Compares two Attribute objects

        :param other: Attribute object
        :type other: SAMLAttribute

        :return: Boolean value indicating whether two items are equal
        :rtype: bool
        """
        if not isinstance(other, SAMLAttribute):
            return False

        return (
            self.name == other.name
            and self.values == other.values
            and self.friendly_name == other.friendly_name
            and self.name_format == other.name_format
        )

    def __repr__(self):
        """Return a string representation.

        :return: String representation
        :rtype: str
        """
        return "<Attribute(name={0}, friendly_name={1}, name_format={2}, values={3})>".format(
            self.name, self.friendly_name, self.name_format, self.values
        )

    @property
    def friendly_name(self):
        """Returns the attribute's friendly name

        :return: Attribute's friendly name
        :rtype: string
        """
        return self._friendly_name

    @property
    def name(self):
        """Returns the attribute's name

        :return: Attribute's name
        :rtype: string
        """
        return self._name

    @property
    def name_format(self):
        """Returns the attribute's name format

        :return: Attribute's name format
        :rtype: string
        """
        return self._name_format

    @property
    def values(self):
        """Returns a list of the attribute's values

        :return: List of the attribute's values
        :rtype: List
        """
        return self._values


class SAMLAttributeStatement(object):
    """Represents saml2:AttributeStatement"""

    def __init__(self, attributes):
        """Initializes a new instance of AttributeStatement class

        :param attributes: Attributes in a form of a list of a dictionary
        :type attributes: List[Attribute]
        """
        self._attributes = {}

        for attribute in attributes:
            self._attributes[attribute.name] = attribute

    def __eq__(self, other):
        """Compares two AttributeStatement objects

        :param other: AttributeStatement object
        :type other: SAMLAttributeStatement

        :return: Boolean value indicating whether two items are equal
        :rtype: bool
        """
        if not isinstance(other, SAMLAttributeStatement):
            return False

        return self.attributes == other.attributes

    def __repr__(self):
        """Return a string representation.

        :return: String representation
        :rtype: str
        """
        return "<AttributeStatement(attributes={0})>".format(self.attributes)

    @property
    def attributes(self):
        """Returns an attributes dictionary

        :return: Dictionary containing attributes
        :rtype: Dict[string, Attribute]
        """
        return self._attributes


class SAMLSubject(object):
    """Contains a name ID and a attribute statement"""

    def __init__(self, name_id, attribute_statement, valid_till=None):
        """Initializes a new instance of Subject class

        :param name_id: Name ID
        :type name_id: SAMLNameID

        :param attribute_statement: Attribute statement
        :type attribute_statement: SAMLAttributeStatement

        :param valid_till: Time till which the subject is valid
            The default value is 30 minutes
            Please refer to the Shibboleth IdP documentation for more details:
            - https://wiki.shibboleth.net/confluence/display/IDP30/SessionConfiguration
        :type valid_till: Optional[Union[datetime.datetime, datetime.timedelta]]
        """
        self._name_id = name_id
        self._attribute_statement = attribute_statement
        self._valid_till = valid_till

        if valid_till is None:
            self._valid_till = datetime.timedelta(minutes=30)
        elif isinstance(valid_till, datetime.datetime):
            self._valid_till = valid_till - datetime.datetime.utcnow()
        elif isinstance(valid_till, int):
            self._valid_till = (
                datetime.datetime.utcfromtimestamp(valid_till)
                - datetime.datetime.utcnow()
            )
        elif isinstance(valid_till, datetime.timedelta):
            self._valid_till = valid_till
        else:
            raise ValueError("valid_till is not valid")

    def __eq__(self, other):
        """Compares two Subject objects

        :param other: Subject object
        :type other: SAMLSubject

        :return: Boolean value indicating whether two items are equal
        :rtype: bool
        """
        if not isinstance(other, SAMLSubject):
            return False

        return (
            self.name_id == other.name_id
            and self.attribute_statement == other.attribute_statement
            and self.valid_till == other.valid_till
        )

    def __repr__(self):
        """Return a string representation.

        :return: String representation
        :rtype: str
        """
        return "<SAMLSubject(name_id={0}, attribute_statement={1}, valid_till={2})>".format(
            self.name_id, self.attribute_statement, self.valid_till
        )

    @property
    def name_id(self):
        """Returns the name ID

        :return: Name ID
        :rtype: SAMLNameID
        """
        return self._name_id

    @name_id.setter
    def name_id(self, value):
        """Set the name ID.

        :param value: New name ID
        :type value: Optional[SAMLNameID]
        """
        if value and not isinstance(value, SAMLNameID):
            raise ValueError(
                "Argument 'value' must be either None or an instance of {0} class".format(
                    SAMLNameID
                )
            )

        self._name_id = value

    @property
    def attribute_statement(self):
        """Returns the attribute statement

        :return: Attribute statement
        :rtype: SAMLAttributeStatement
        """
        return self._attribute_statement

    @property
    def valid_till(self):
        """Returns the time till which the subject is valid.
        The default value is 30 minutes. Please refer to the Shibboleth IdP documentation for more details:
        - https://wiki.shibboleth.net/confluence/display/IDP30/SessionConfiguration

        :return: Time till which the subject is valid
        :rtype: datetime.timedelta
        """
        return self._valid_till


class SAMLSubjectJSONEncoder(JSONEncoder):
    """Subject's JSON encoder"""

    def default(self, subject):
        """Serializers a Subject object to JSON

        :param subject: Subject object
        :type subject: api.saml.metadata.Subject

        :return: String containing JSON representation of the Subject object
        :rtype: string
        """
        if not isinstance(subject, SAMLSubject):
            raise ValueError("subject must have type Subject")

        result = {}

        if subject.name_id:
            result["name_id"] = {
                "name_format": subject.name_id.name_format,
                "name_id": subject.name_id.name_id,
                "name_qualifier": subject.name_id.name_qualifier,
                "sp_name_qualifier": subject.name_id.sp_name_qualifier,
            }

        if subject.attribute_statement and subject.attribute_statement.attributes:
            result["attributes"] = {
                attribute.name: attribute.values
                for attribute in subject.attribute_statement.attributes.itervalues()
            }

        return result


class SAMLSubjectJSONDecoder(JSONDecoder):
    """Subject's JSON decoder."""

    def decode(self, raw_subject, _w=WHITESPACE.match):
        """Decode a JSON document into Subject object.

        :param raw_subject: String containing JSON document
        :type raw_subject: str

        :param _w: Regular expression used to match white spaces
        :type _w: RegEx

        :return: Subject object
        :rtype: api.saml.metadata.Subject
        """
        raw_subject = super(SAMLSubjectJSONDecoder, self).decode(raw_subject, _w)
        attribute_statement = None
        name_id = None

        if "name_id" in raw_subject:
            raw_name_id_dict = raw_subject["name_id"]
            raw_name_format = raw_name_id_dict["name_format"]
            raw_name_id = raw_name_id_dict["name_id"]
            raw_name_qualifier = raw_name_id_dict["name_qualifier"]
            raw_sp_name_qualifier = raw_name_id_dict["sp_name_qualifier"]
            name_id = SAMLNameID(
                raw_name_format, raw_name_qualifier, raw_sp_name_qualifier, raw_name_id
            )

        if "attributes" in raw_subject:
            raw_attributes = raw_subject["attributes"]
            attributes = []

            for raw_attribute_name in raw_attributes:
                raw_attribute_values = raw_attributes[raw_attribute_name]
                attribute = SAMLAttribute(raw_attribute_name, raw_attribute_values)

                attributes.append(attribute)

            attribute_statement = SAMLAttributeStatement(attributes)

        subject = SAMLSubject(name_id, attribute_statement)

        return subject


class SAMLSubjectUIDExtractor(object):
    """Implements an algorithm for extracting a subject's unique ID from its attributes"""

    def extract(self, subject):
        """Extracts a unique ID from the subject object

        :param subject: Subject object
        :type subject: api.saml.metadata.Subject

        :return: Unique ID
        :rtype: string

        Unfortunately, there is no single standard regarding what attributes can be treated as unique IDs.
        Different systems use different attributes and all of them have their pros and cons.
        This class implements an algorithm which tries different attributes in the following order
        and selects the first of them as the unique ID:

        1. eduPersonUniqueId
           (https://wiki.refeds.org/display/STAN/eduPerson+2020-01#eduPerson2020-01-eduPersonUniqueId)

           A long-lived, non re-assignable, omnidirectional identifier suitable for use as a principal identifier
           by authentication providers or as a unique external key by applications.

        2. eduPersonTargetedID
           (https://wiki.refeds.org/display/STAN/eduPerson+2020-01#eduPerson2020-01-eduPersonTargetedID)

           A persistent, non-reassigned, opaque identifier for a principal.
           eduPersonTargetedID is an abstracted version of the SAML V2.0 Name Identifier format of
           "urn:oasis:names:tc:SAML:2.0:nameid-format:persistent"
           (see http://www.oasis-open.org/committees/download.php/35711).

           NOTE: eduPersonTargetedID is DEPRECATED and will be marked as obsolete in a future version
           of this specification. Its equivalent definition in SAML 2.0 has been replaced by a new specification
           for standard Subject Identifier attributes
           [https://docs.oasis-open.org/security/saml-subject-id-attr/v1.0/saml-subject-id-attr-v1.0.html],
           one of which ("urn:oasis:names:tc:SAML:attribute:pairwise-id") is a direct replacement for this identifier
           with a simpler syntax and safer comparison rules.
           Existing use of this attribute in SAML 1.1 or SAML 2.0 should be phased out
           in favor of the new Subject Identifier attributes."

        3. uid
           http://oid-info.com/get/0.9.2342.19200300.100.1.1

           See IETF RFC 4519.
           IETF RFC 1274 uses the identifier "userid".

        4. Name ID
           The extractor fetches the first name ID it could find as a last resort which may no be correct.
           It might be better to fetch only persistent name IDs.

        Also, please note that eduPersonTargetedID attribute and name IDs should be phased out and replaced with
        the pairwise-id attribute from the OASIS SAML 2.0 SubjectID Attributes Profile.
        However, it's not yet supported by most of the IdPs
        """
        unique_id_attributes = [
            SAMLAttributeType.eduPersonUniqueId,
            SAMLAttributeType.eduPersonTargetedID,
            SAMLAttributeType.uid,
        ]

        if subject.attribute_statement:
            for unique_id_attribute in unique_id_attributes:
                if unique_id_attribute.name in subject.attribute_statement.attributes:
                    unique_id_attribute = subject.attribute_statement.attributes[
                        unique_id_attribute.name
                    ]

                    return unique_id_attribute.values[0]

        if subject.name_id and subject.name_id.name_id:
            return subject.name_id.name_id
