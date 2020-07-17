import logging

from onelogin.saml2.idp_metadata_parser import OneLogin_Saml2_IdPMetadataParser

from api.saml.exceptions import SAMLError


class SAMLMetadataLoadingError(SAMLError):
    """Raised in the case of any errors occurred during loading of SAML metadata from a remote source"""


class SAMLMetadataLoader(object):
    """Loads SAML metadata from a remote source (e.g. InCommon Metadata Service)"""

    # InCommon Metadata Service URL serving only IdP metadata
    # (https://spaces.at.internet2.edu/display/federation/Download+InCommon+metadata)
    IN_COMMON_METADATA_SERVICE_URL = 'http://md.incommon.org/InCommon/InCommon-metadata-idp-only.xml'

    def __init__(self):
        """Initializes a new instance of SAMLMetadataLoader"""

        self._logger = logging.getLogger(__name__)

    def load_idp_metadata(self, url=None):
        """Loads IdP metadata in an XML format from the specified url (by default InCommon IdP Metadata Service)

        :param url: URL of a metadata service (by default InCommon IdP Metadata Service)
        :type url: Optional[string]

        :return: XML string containing InCommon Metadata
        :rtype: string

        :raise: MetadataLoadError
        """
        url = url if url else self.IN_COMMON_METADATA_SERVICE_URL

        self._logger.info('Started loading IdP XML metadata from {0}'.format(url))

        try:
            # TODO: Add metadata validation
            # Metadata validation is described in section Validate downloaded metadata
            # on https://spaces.at.internet2.edu/display/federation/Consume+InCommon+metadata)
            xml_metadata = OneLogin_Saml2_IdPMetadataParser.get_metadata(url)
        except Exception as exception:
            raise SAMLMetadataLoadingError(inner_exception=exception)

        self._logger.info('Finished loading IdP XML metadata from {0}'.format(url))

        return xml_metadata
