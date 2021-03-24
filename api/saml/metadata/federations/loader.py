import logging

from defusedxml.lxml import tostring
from onelogin.saml2.idp_metadata_parser import OneLogin_Saml2_IdPMetadataParser

from api.saml.metadata.federations.model import (
    SAMLFederatedIdentityProvider,
    SAMLFederation,
)
from api.saml.metadata.federations.validator import SAMLFederatedMetadataValidator
from api.saml.metadata.parser import SAMLMetadataParser
from core.exceptions import BaseError
from core.util import first_or_default


class SAMLMetadataLoadingError(BaseError):
    """Raised in the case of any errors occurred during loading of SAML metadata from a remote source"""


class SAMLMetadataLoader(object):
    """Loads SAML metadata from a remote source (e.g. InCommon Metadata Service)"""

    def __init__(self):
        """Initializes a new instance of SAMLMetadataLoader"""

        self._logger = logging.getLogger(__name__)

    def load_idp_metadata(self, url=None):
        """Load IdP metadata in an XML format from the specified url.

        :param url: URL of a metadata service
        :type url: Optional[string]

        :return: XML string containing InCommon Metadata
        :rtype: string

        :raise: MetadataLoadError
        """
        self._logger.info("Started loading IdP XML metadata from {0}".format(url))

        try:
            xml_metadata = OneLogin_Saml2_IdPMetadataParser.get_metadata(url)
        except Exception as exception:
            raise SAMLMetadataLoadingError(inner_exception=exception)

        self._logger.info("Finished loading IdP XML metadata from {0}".format(url))

        return xml_metadata


class SAMLFederatedIdentityProviderLoader(object):
    """Loads metadata of federated IdPs from the specified metadata service."""

    ENGLISH_LANGUAGE_CODES = ("en", "eng")

    def __init__(self, loader, validator, parser):
        """Initialize a new instance of SAMLFederatedIdentityProviderLoader class.

        :param loader: SAML metadata loader
        :type loader: api.saml.metadata.federations.loader.SAMLMetadataLoader

        :param validator: SAML metadata validator
        :type validator: api.saml.metadata.federations.validator.SAMLFederatedMetadataValidator

        :param parser: SAML metadata parser
        :type parser: api.saml.metadata.parser.SAMLMetadataParser
        """
        if not isinstance(loader, SAMLMetadataLoader):
            raise ValueError(
                "Argument 'loader' must be an instance of {0} class".format(
                    SAMLMetadataLoader
                )
            )
        if not isinstance(validator, SAMLFederatedMetadataValidator):
            raise ValueError(
                "Argument 'validator' must be an instance of {0} class".format(
                    SAMLFederatedMetadataValidator
                )
            )
        if not isinstance(parser, SAMLMetadataParser):
            raise ValueError(
                "Argument 'parser' must be an instance of {0} class".format(
                    SAMLMetadataParser
                )
            )

        self._loader = loader
        self._validator = validator
        self._parser = parser

        self._logger = logging.getLogger(__name__)

    def _try_to_get_an_english_value(self, localized_values):
        """Try to fetch an English value from the list of localized values.

        :param localized_values: List of localized values
        :type localized_values: List[api.saml.metadata.LocalizedMetadataItem]

        :return: Localized value in English (if any, otherwise first value from the list)
        :rtype: Optional[str]
        """
        if not localized_values:
            return None

        for localized_value in localized_values:
            if localized_value.language in self.ENGLISH_LANGUAGE_CODES:
                return localized_value.value

        return first_or_default(localized_values).value

    def load(self, federation):
        """Loads metadata of federated IdPs from the specified metadata service.

        :param federation: SAML federation where loaded IdPs belong to
        :type federation: api.saml.metadata.federations.model.SAMLFederation

        :return: List of SAMLFederatedIdP objects
        :rtype: Iterable[api.saml.configuration.SAMLFederatedIdentityProvider]
        """
        if not isinstance(federation, SAMLFederation):
            raise ValueError(
                "Argument 'federation' must be an instance of {0} class".format(
                    SAMLFederation
                )
            )

        self._logger.info("Started loading federated IdP's for {0}".format(federation))

        federated_idps = []
        metadata = self._loader.load_idp_metadata(federation.idp_metadata_service_url)

        self._validator.validate(federation, metadata)

        parsing_results = self._parser.parse(metadata)

        for parsing_result in parsing_results:
            idp = parsing_result.provider

            if idp.ui_info.display_names:
                display_name = self._try_to_get_an_english_value(
                    idp.ui_info.display_names
                )
            elif idp.organization.organization_display_names:
                display_name = self._try_to_get_an_english_value(
                    idp.organization.organization_display_names
                )
            elif idp.organization.organization_names:
                display_name = self._try_to_get_an_english_value(
                    idp.organization.organization_names
                )
            else:
                display_name = idp.entity_id

            xml_metadata = tostring(parsing_result.xml_node).decode("utf-8")
            federated_idp = SAMLFederatedIdentityProvider(
                federation, idp.entity_id.strip(), display_name.strip(), xml_metadata
            )

            federated_idps.append(federated_idp)

        self._logger.info(
            "Finished loading {0} federated IdP's for {1}".format(
                len(federated_idps), federation
            )
        )

        return federated_idps
