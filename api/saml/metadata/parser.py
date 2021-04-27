import logging

from defusedxml.lxml import fromstring
from flask_babel import lazy_gettext as _
from lxml.etree import XMLSyntaxError
from onelogin.saml2.constants import OneLogin_Saml2_Constants
from onelogin.saml2.utils import OneLogin_Saml2_XML

from api.saml.metadata.model import (
    SAMLAttribute,
    SAMLAttributeStatement,
    SAMLAttributeType,
    SAMLBinding,
    SAMLIdentityProviderMetadata,
    SAMLLocalizedMetadataItem,
    SAMLNameID,
    SAMLNameIDFormat,
    SAMLOrganization,
    SAMLService,
    SAMLServiceProviderMetadata,
    SAMLSubject,
    SAMLUIInfo,
)
from core.exceptions import BaseError


class SAMLMetadataParsingError(BaseError):
    """Raised in the case of any errors occurred during parsing of SAML metadata"""


class SAMLMetadataParsingResult(object):
    def __init__(self, provider, xml_node):
        """Initialize a new instance of SAMLMetadataParsingResult class.

        :param provider: Object containing either SP's or IdP's metadata
        :type provider: api.saml.metadata.model.SAMLProviderMetadata

        :param xml_node: XML node containing metadata
        :type xml_node: defusedxml.lxml.RestrictedElement
        """
        self._provider = provider
        self._xml_node = xml_node

    @property
    def provider(self):
        """Return the object containing either SP's or IdP's metadata.

        :return: Object containing either SP's or IdP's metadata
        :rtype: api.saml.metadata.model.SAMLProviderMetadata
        """
        return self._provider

    @property
    def xml_node(self):
        """Return the XML node containing metadata

        :return: XML node containing metadata
        :rtype: defusedxml.lxml.RestrictedElement
        """
        return self._xml_node


class SAMLMetadataParser(object):
    """Parses SAML metadata"""

    def __init__(self, skip_incorrect_providers=False):
        """Initialize a new instance of MetadataParser class.

        :param skip_incorrect_providers: Boolean value indicating whether the parse should skip
            incorrect SAML provider declarations instead of raising an exception
        :type skip_incorrect_providers: bool
        """
        self._skip_incorrect_providers = skip_incorrect_providers

        self._logger = logging.getLogger(__name__)

        # Add missing namespaces to be able to parse mdui:UIInfoType
        OneLogin_Saml2_Constants.NS_PREFIX_MDUI = "mdui"
        OneLogin_Saml2_Constants.NS_MDUI = "urn:oasis:names:tc:SAML:metadata:ui"
        OneLogin_Saml2_Constants.NSMAP[
            OneLogin_Saml2_Constants.NS_PREFIX_MDUI
        ] = OneLogin_Saml2_Constants.NS_MDUI

        OneLogin_Saml2_Constants.NS_PREFIX_ALG = "alg"
        OneLogin_Saml2_Constants.NS_ALG = "urn:oasis:names:tc:SAML:metadata:algsupport"
        OneLogin_Saml2_Constants.NSMAP[
            OneLogin_Saml2_Constants.NS_PREFIX_ALG
        ] = OneLogin_Saml2_Constants.NS_ALG

    def _convert_xml_string_to_dom(self, xml_metadata):
        """Converts an XML string containing SAML metadata into XML DOM

        :param xml_metadata: XML string containing SAML metadata
        :type xml_metadata: string

        :return: XML DOM tree containing SAML metadata
        :rtype: defusedxml.lxml.RestrictedElement

        :raise: MetadataParsingError
        """
        self._logger.debug(
            "Started converting XML string containing SAML metadata into XML DOM"
        )

        try:
            metadata_dom = fromstring(xml_metadata.encode("utf-8"), forbid_dtd=True)
        except (
            ValueError,
            XMLSyntaxError,
        ) as exception:
            self._logger.exception(
                "An unhandled exception occurred during converting XML string containing SAML metadata into XML DOM"
            )

            raise SAMLMetadataParsingError(inner_exception=exception)

        self._logger.debug(
            "Finished converting XML string containing SAML metadata into XML DOM"
        )

        return metadata_dom

    def _parse_certificates(self, certificate_nodes):
        """Parses XML nodes containing X.509 certificates into a list of strings

        :param certificate_nodes: List of XML nodes containing X.509 certificates
        :type certificate_nodes: List[defusedxml.lxml.RestrictedElement]

        :return: List of string containing X.509 certificates
        :rtype: List[string]

        :raise: MetadataParsingError
        """
        certificates = []

        self._logger.debug(
            "Started parsing {0} certificates".format(len(certificate_nodes))
        )

        try:
            for certificate_node in certificate_nodes:
                certificate = "".join(
                    OneLogin_Saml2_XML.element_text(certificate_node).split()
                )

                self._logger.debug(
                    "Found the following certificate: {0}".format(certificate)
                )

                certificates.append(certificate)
        except XMLSyntaxError as exception:
            raise SAMLMetadataParsingError(inner_exception=exception)

        self._logger.debug(
            "Finished parsing {0} certificates: {1}".format(
                len(certificate_nodes), certificates
            )
        )

        return certificates

    def _parse_providers(self, entity_descriptor_node, provider_nodes, parse_function):
        """Parses a list of IDPSSODescriptor/SPSSODescriptor nodes and translates them
        into IdentityProviderMetadata/ServiceProviderMetadata object

        :param entity_descriptor_node: Parent EntityDescriptor node
        :type entity_descriptor_node: defusedxml.lxml.RestrictedElement

        :param provider_nodes: List of IDPSSODescriptor/SPSSODescriptor nodes
        :type provider_nodes: List[defusedxml.lxml.RestrictedElement]

        :param parse_function: Function used to parse body of IDPSSODescriptor/SPSSODescriptor nodes
        and return corresponding IdentityProviderMetadata/ServiceProviderMetadata objects
        :type parse_function: Callable[[defusedxml.lxml.RestrictedElement, string, UIInfo], ProviderMetadata]

        :return: List of IdentityProviderMetadata/ServiceProviderMetadata objects containing SAML metadata from the XML
        :rtype: List[ProviderMetadata]

        :raise: MetadataParsingError
        """
        providers = []

        for provider_node in provider_nodes:
            entity_id = entity_descriptor_node.get("entityID", None)
            ui_info = self._parse_ui_info(provider_node)
            organization = self._parse_organization_metadata(entity_descriptor_node)

            try:
                provider = parse_function(
                    provider_node, entity_id, ui_info, organization
                )

                providers.append(provider)
            except SAMLMetadataParsingError:
                if not self._skip_incorrect_providers:
                    raise

        return providers

    def _parse_localizable_metadata_items(
        self, provider_descriptor_node, xpath, required=False
    ):
        """Parses IDPSSODescriptor/SPSSODescriptor's mdui:UIInfo child elements (for example, mdui:DisplayName)

        :param provider_descriptor_node: Parent IDPSSODescriptor/SPSSODescriptor XML node
        :type provider_descriptor_node: defusedxml.lxml.RestrictedElement

        :param xpath: XPath expression for a particular md:localizedNameType child element
            (for example, mdui:DisplayName)
        :type xpath: string

        :param required: Boolean value indicating whether particular md:localizedNameType child element
            is required or not
        :type required: bool

        :return: List of md:localizedNameType child elements
        :rtype: Optional[List[LocalizableMetadataItem]]

        :raise: MetadataParsingError
        """
        localizable_metadata_nodes = OneLogin_Saml2_XML.query(
            provider_descriptor_node, xpath
        )

        if not localizable_metadata_nodes and required:
            last_slash_index = xpath.rfind("/")
            localizable_metadata_tag_name = xpath[last_slash_index + 1 :]

            raise SAMLMetadataParsingError(
                _("{0} tag is missing".format(localizable_metadata_tag_name))
            )

        localizable_items = None

        if localizable_metadata_nodes:
            localizable_items = []

            for localizable_metadata_node in localizable_metadata_nodes:
                localizable_item_text = localizable_metadata_node.text
                localizable_item_language = localizable_metadata_node.get(
                    "{http://www.w3.org/XML/1998/namespace}lang", None
                )
                localizable_item = SAMLLocalizedMetadataItem(
                    localizable_item_text, localizable_item_language
                )

                localizable_items.append(localizable_item)

        return localizable_items

    def _parse_ui_info(self, provider_node):
        """Parses IDPSSODescriptor/SPSSODescriptor's mdui:UIInfo and translates it into UIInfo object

        :param provider_node: Parent IDPSSODescriptor/SPSSODescriptor node
        :type provider_node: defusedxml.lxml.RestrictedElement

        :return: UIInfo object
        :rtype: UIInfo

        :raise: MetadataParsingError
        """
        display_names = self._parse_localizable_metadata_items(
            provider_node, "./md:Extensions/mdui:UIInfo/mdui:DisplayName"
        )
        descriptions = self._parse_localizable_metadata_items(
            provider_node, "./md:Extensions/mdui:UIInfo/mdui:Description"
        )
        information_urls = self._parse_localizable_metadata_items(
            provider_node, "./md:Extensions/mdui:UIInfo/mdui:InformationURL"
        )
        privacy_statement_urls = self._parse_localizable_metadata_items(
            provider_node, "./md:Extensions/mdui:UIInfo/mdui:PrivacyStatementURL"
        )
        logos = self._parse_localizable_metadata_items(
            provider_node, "./md:Extensions/mdui:UIInfo/mdui:Logo"
        )

        ui_info = SAMLUIInfo(
            display_names, descriptions, information_urls, privacy_statement_urls, logos
        )

        return ui_info

    def _parse_organization_metadata(self, entity_descriptor_node):
        """Parses IDPSSODescriptor/SPSSODescriptor's mdui:Organization and translates it into Organization object

        :param entity_descriptor_node: Parent EntityDescriptor node
        :type entity_descriptor_node: defusedxml.lxml.RestrictedElement

        :return: Organization object
        :rtype: Organization

        :raise: MetadataParsingError
        """
        organization_names = self._parse_localizable_metadata_items(
            entity_descriptor_node, "./md:Organization/md:OrganizationName"
        )
        organization_display_names = self._parse_localizable_metadata_items(
            entity_descriptor_node, "./md:Organization/md:OrganizationDisplayName"
        )
        organization_urls = self._parse_localizable_metadata_items(
            entity_descriptor_node, "./md:Organization/md:OrganizationURL"
        )

        organization = SAMLOrganization(
            organization_names, organization_display_names, organization_urls
        )

        return organization

    def _parse_name_id_format(self, provider_node):
        """Parses a name ID format

        NOTE: OneLogin's python-saml library used for implementing SAML authentication support only one name ID format.
        If there are multiple name ID formats specified in the XML metadata, we select the first one.

        :param provider_node: Parent IDPSSODescriptor/SPSSODescriptor node
        :type provider_node: defusedxml.lxml.RestrictedElement

        :return: Name ID format
        :rtype: string
        """
        name_id_format = SAMLNameIDFormat.UNSPECIFIED.value
        name_id_format_nodes = OneLogin_Saml2_XML.query(
            provider_node, "./ md:NameIDFormat"
        )
        if len(name_id_format_nodes) > 0:
            # OneLogin's python-saml supports only one name ID format so we select the first one
            name_id_format = OneLogin_Saml2_XML.element_text(name_id_format_nodes[0])

        return name_id_format

    def _parse_idp_metadata(
        self,
        provider_node,
        entity_id,
        ui_info,
        organization,
        required_sso_binding=SAMLBinding.HTTP_REDIRECT,
        required_slo_binding=SAMLBinding.HTTP_REDIRECT,
    ):
        """Parses IDPSSODescriptor node and translates it into an IdentityProviderMetadata object

        :param provider_node: IDPSSODescriptor node containing IdP metadata
        :param provider_node: defusedxml.lxml.RestrictedElement

        :param entity_id: String containing IdP's entityID
        :type entity_id: string

        :param ui_info: UIInfo object containing IdP's description
        :type ui_info: UIInfo

        :param organization: Organization object containing basic information about an organization
            responsible for a SAML entity or role
        :type organization: Organization

        :param required_sso_binding: Required binding for Single Sign-On profile (HTTP-Redirect by default)
        :type required_sso_binding: Binding

        :param required_slo_binding: Required binding for Single Sing-Out profile (HTTP-Redirect by default)
        :type required_slo_binding: Binding

        :return: IdentityProviderMetadata containing IdP metadata
        :rtype: IdentityProviderMetadata

        :raise: MetadataParsingError
        """
        want_authn_requests_signed = provider_node.get("WantAuthnRequestsSigned", False)

        name_id_format = self._parse_name_id_format(provider_node)

        sso_service = None
        sso_nodes = OneLogin_Saml2_XML.query(
            provider_node,
            "./md:SingleSignOnService[@Binding='%s']" % required_sso_binding.value,
        )
        if len(sso_nodes) > 0:
            sso_node = self._select_default_or_first_indexed_element(sso_nodes)
            sso_url = sso_node.get("Location", None)
            sso_service = SAMLService(sso_url, required_sso_binding)
        else:
            raise SAMLMetadataParsingError(
                _(
                    "Missing {0} SingleSignOnService service declaration".format(
                        required_sso_binding.value
                    )
                )
            )

        slo_service = None
        slo_nodes = OneLogin_Saml2_XML.query(
            provider_node,
            "./md:SingleLogoutService[@Binding='%s']" % required_slo_binding.value,
        )
        if len(slo_nodes) > 0:
            slo_node = self._select_default_or_first_indexed_element(slo_nodes)
            slo_url = slo_node.get("Location", None)
            slo_service = SAMLService(slo_url, required_slo_binding)

        signing_certificate_nodes = OneLogin_Saml2_XML.query(
            provider_node,
            './md:KeyDescriptor[not(contains(@use, "encryption"))]/ds:KeyInfo/ds:X509Data/ds:X509Certificate',
        )
        signing_certificates = self._parse_certificates(signing_certificate_nodes)

        encryption_certificate_nodes = OneLogin_Saml2_XML.query(
            provider_node,
            './md:KeyDescriptor[not(contains(@use, "signing"))]/ds:KeyInfo/ds:X509Data/ds:X509Certificate',
        )
        encryption_certificates = self._parse_certificates(encryption_certificate_nodes)

        idp = SAMLIdentityProviderMetadata(
            entity_id,
            ui_info,
            organization,
            name_id_format,
            sso_service,
            slo_service,
            want_authn_requests_signed,
            signing_certificates,
            encryption_certificates,
        )

        return idp

    def _parse_sp_metadata(
        self,
        provider_node,
        entity_id,
        ui_info,
        organization,
        required_acs_binding=SAMLBinding.HTTP_POST,
    ):
        """Parses SPSSODescriptor node and translates it into a ServiceProvider object

        :param provider_node: SPSSODescriptor node containing SP metadata
        :param provider_node: defusedxml.lxml.RestrictedElement

        :param entity_id: String containing IdP's entityID
        :type entity_id: string

        :param ui_info: UIInfo object containing IdP's description
        :type ui_info: UIInfo

        :param organization: Organization object containing basic information about an organization
            responsible for a SAML entity or role
        :type organization: Organization

        :param required_acs_binding: Required binding for Assertion Consumer Service (HTTP-Redirect by default)
        :type required_acs_binding: Binding

        :return: ServiceProvider containing SP metadata
        :rtype: ServiceProvider

        :raise: MetadataParsingError
        """
        authn_requests_signed = provider_node.get("AuthnRequestsSigned", False)
        want_assertions_signed = provider_node.get("WantAssertionsSigned", False)

        name_id_format = self._parse_name_id_format(provider_node)

        acs_service = None
        acs_service_nodes = OneLogin_Saml2_XML.query(
            provider_node,
            "./md:AssertionConsumerService[@Binding='%s']" % required_acs_binding.value,
        )
        if len(acs_service_nodes) > 0:
            acs_service_node = self._select_default_or_first_indexed_element(
                acs_service_nodes
            )
            acs_url = acs_service_node.get("Location", None)
            acs_service = SAMLService(acs_url, required_acs_binding)
        else:
            raise SAMLMetadataParsingError(
                _(
                    "Missing {0} AssertionConsumerService".format(
                        required_acs_binding.value
                    )
                )
            )

        certificate_nodes = OneLogin_Saml2_XML.query(
            provider_node,
            "./md:KeyDescriptor/ds:KeyInfo/ds:X509Data/ds:X509Certificate",
        )
        certificates = self._parse_certificates(certificate_nodes)

        if len(certificates) > 1:
            raise SAMLMetadataParsingError(
                _(
                    "There are more than 1 SP certificates".format(
                        required_acs_binding.value
                    )
                )
            )

        certificate = next(iter(certificates)) if certificates else None

        sp = SAMLServiceProviderMetadata(
            entity_id,
            ui_info,
            organization,
            name_id_format,
            acs_service,
            authn_requests_signed,
            want_assertions_signed,
            certificate,
        )

        return sp

    def _select_default_element(self, nodes):
        """Selects a node with attribute "isDefault=true"

        :param nodes: List of XML nodes
        :type nodes: List[defusedxml.lxml.RestrictedElement]

        :return: "Default" node or None if there is no one
        :rtype: Optional[defusedxml.lxml.RestrictedElement]
        """
        default_nodes = [node for node in nodes if node.get("isDefault", False)]

        default_node = self._select_first_indexed_element(default_nodes)

        return default_node

    def _select_first_indexed_element(self, nodes):
        """Sorts a list of XML nodes by "index" attribute and selects the first node

        :param nodes: List of XML nodes
        :type nodes: List[defusedxml.lxml.RestrictedElement]

        :return: Node with the smallest index or None if there is no one
        :rtype: Optional[defusedxml.lxml.RestrictedElement]
        """
        if not nodes:
            return None

        nodes = sorted(nodes, key=lambda node: node.get("index", 0))

        return nodes[0]

    def _select_default_or_first_indexed_element(self, nodes):
        """Selects a node with attribute "isDefault=true" or a node with the smallest "index" attribute

        :param nodes: List of XML nodes
        :type nodes: List[defusedxml.lxml.RestrictedElement]

        :return: "Default" node or the node with the smallest "index" attribute or None if there is no one
        :rtype: Optional[defusedxml.lxml.RestrictedElement]
        """
        default_node = self._select_default_element(nodes)

        if default_node:
            return default_node

        return self._select_first_indexed_element(nodes)

    def parse(self, xml_metadata):
        """Parses an XML string containing SAML metadata and translates it into a list of
        IdentityProviderMetadata/ServiceProviderMetadata objects

        :param xml_metadata: XML string containing SAML metadata
        :type xml_metadata: string

        :return: List of SAMLMetadataParsingResult objects
        :rtype: List[SAMLMetadataParsingResult]

        :raise: MetadataParsingError
        """
        self._logger.info("Started parsing an XML string containing SAML metadata")

        metadata_dom = self._convert_xml_string_to_dom(xml_metadata)
        parsing_results = []

        try:
            entity_descriptor_nodes = OneLogin_Saml2_XML.query(
                metadata_dom, "//md:EntityDescriptor"
            )

            for entity_descriptor_node in entity_descriptor_nodes:
                idp_descriptor_nodes = OneLogin_Saml2_XML.query(
                    entity_descriptor_node, "./md:IDPSSODescriptor"
                )
                idps = self._parse_providers(
                    entity_descriptor_node,
                    idp_descriptor_nodes,
                    self._parse_idp_metadata,
                )

                for idp in idps:
                    parsing_result = SAMLMetadataParsingResult(
                        idp, entity_descriptor_node
                    )
                    parsing_results.append(parsing_result)

                sp_descriptor_nodes = OneLogin_Saml2_XML.query(
                    entity_descriptor_node, "./md:SPSSODescriptor"
                )
                sps = self._parse_providers(
                    entity_descriptor_node, sp_descriptor_nodes, self._parse_sp_metadata
                )

                for sp in sps:
                    parsing_result = SAMLMetadataParsingResult(
                        sp, entity_descriptor_node
                    )
                    parsing_results.append(parsing_result)
        except XMLSyntaxError as exception:
            self._logger.exception(
                "An unexpected error occurred during parsing an XML string containing SAML metadata"
            )

            raise SAMLMetadataParsingError(inner_exception=exception)

        self._logger.info("Finished parsing an XML string containing SAML metadata")

        return parsing_results


class SAMLSubjectParser(object):
    """Parses SAML response into Subject object"""

    def _parse_name_id(self, name_id_attributes):
        """Parses NameID attributes

        :param name_id_attributes: Dictionary containing NameID attributes
        :type name_id_attributes: Dict

        :return: NameID object
        :rtype: NameID
        """
        name_id = SAMLNameID(
            name_id_attributes["Format"],
            name_id_attributes["NameQualifier"],
            None,
            name_id_attributes["value"],
        )

        return name_id

    def _parse_attribute_values(self, attribute_values):
        """Parses SAML attribute values

        :param attribute_values: List containing SAML attribute values
        :type attribute_values: List[Union[str, Dict]]

        :return: 2-tuple containing an optional name ID from the attribute list and
            a list of parsed SAML attribute values
        :rtype: Tuple[Optional[NameID, List[str]]
        """
        name_id = None
        parsed_attribute_values = []

        for attribute_value in attribute_values:
            if isinstance(attribute_value, dict) and "NameID" in attribute_value:
                name_id = self._parse_name_id(attribute_value["NameID"])

                parsed_attribute_values.append(name_id.name_id)
            else:
                parsed_attribute_values.append(attribute_value)

        return name_id, parsed_attribute_values

    def _parse_attributes(self, attributes):
        """Parses SAML attributes

        :param attributes: Dictionary containing SAML attributes
        :type attributes: Dict

        :return: 2-tuple containing an optional name ID from the attribute list and
            an attribute statement object
        :rtype: Tuple[Optional[NameID, AttributeStatement]
        """
        name_id = None
        parsed_attributes = []
        attribute_names = {
            attribute.value: attribute for attribute in SAMLAttributeType
        }

        for name, attribute_values in list(attributes.items()):
            if name in attribute_names:
                name = attribute_names[name].name

            current_name_id, parsed_attribute_values = self._parse_attribute_values(
                attribute_values
            )

            if current_name_id:
                name_id = current_name_id

            attribute = SAMLAttribute(name=name, values=parsed_attribute_values)

            parsed_attributes.append(attribute)

        attribute_statement = SAMLAttributeStatement(parsed_attributes)

        return name_id, attribute_statement

    def parse(self, auth):
        """Parses OneLogin_Saml2_Auth object containing SAML response data into Subject

        :param auth: OneLogin_Saml2_Auth object containing SAML response
        :type auth: OneLogin_Saml2_Auth

        :return: Subject object containing SAML attributes and NameID
        :rtype: api.saml.metadata.Subject
        """
        name_id = SAMLNameID(
            auth.get_nameid_format(),
            auth.get_nameid_nq(),
            auth.get_nameid_spnq(),
            auth.get_nameid(),
        )
        raw_attributes = auth.get_attributes()
        attribute_name_id, attribute_statement = self._parse_attributes(raw_attributes)
        valid_till = auth.get_session_expiration()

        if attribute_name_id:
            name_id = attribute_name_id

        if not valid_till:
            valid_till = auth.get_last_assertion_not_on_or_after()

        subject = SAMLSubject(name_id, attribute_statement, valid_till)

        return subject
