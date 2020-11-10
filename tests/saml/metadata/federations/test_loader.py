from mock import MagicMock, create_autospec, patch
from nose.tools import eq_, raises

from api.saml.metadata.federations import incommon
from api.saml.metadata.federations.loader import (
    SAMLFederatedIdentityProviderLoader,
    SAMLMetadataLoader,
    SAMLMetadataLoadingError,
)
from api.saml.metadata.federations.model import SAMLFederation
from api.saml.metadata.federations.validator import SAMLFederatedMetadataValidator
from api.saml.metadata.parser import SAMLMetadataParser
from tests.saml import fixtures


class TestSAMLMetadataLoader(object):
    @patch("urllib2.urlopen")
    @raises(SAMLMetadataLoadingError)
    def test_load_idp_metadata_raises_error_when_xml_is_incorrect(self, urlopen_mock):
        # Arrange
        url = "http://md.incommon.org/InCommon/metadata.xml"
        incorrect_xml = fixtures.INCORRECT_XML
        urlopen_response_mock = MagicMock()
        urlopen_response_mock.read = MagicMock(return_value=incorrect_xml)
        urlopen_mock.return_value = urlopen_response_mock
        metadata_loader = SAMLMetadataLoader()

        # Act
        metadata_loader.load_idp_metadata(url)

    @patch("urllib2.urlopen")
    def test_load_idp_metadata_correctly_loads_one_descriptor(self, urlopen_mock):
        # Arrange
        url = "http://md.incommon.org/InCommon/metadata.xml"
        incorrect_xml = fixtures.CORRECT_XML_WITH_IDP_1
        urlopen_response_mock = MagicMock()
        urlopen_response_mock.read = MagicMock(return_value=incorrect_xml)
        urlopen_mock.return_value = urlopen_response_mock
        metadata_loader = SAMLMetadataLoader()

        # Act
        xml_metadata = metadata_loader.load_idp_metadata(url)

        # Assert
        urlopen_mock.assert_called_with(url)
        eq_(fixtures.CORRECT_XML_WITH_IDP_1, xml_metadata)

    @patch("urllib2.urlopen")
    def test_load_idp_metadata_correctly_loads_multiple_descriptors(self, urlopen_mock):
        # Arrange
        url = "http://md.incommon.org/InCommon/metadata.xml"
        incorrect_xml = fixtures.CORRECT_XML_WITH_MULTIPLE_IDPS
        urlopen_response_mock = MagicMock()
        urlopen_response_mock.read = MagicMock(return_value=incorrect_xml)
        urlopen_mock.return_value = urlopen_response_mock
        metadata_loader = SAMLMetadataLoader()

        # Act
        xml_metadata = metadata_loader.load_idp_metadata(url)

        # Assert
        urlopen_mock.assert_called_with(url)
        eq_(fixtures.CORRECT_XML_WITH_MULTIPLE_IDPS, xml_metadata)


class TestSAMLFederatedIdentityProviderLoader(object):
    def test_load(self):
        # Arrange
        federation_type = incommon.FEDERATION_TYPE
        federation_idp_metadata_service_url = incommon.IDP_METADATA_SERVICE_URL
        xml_metadata = fixtures.CORRECT_XML_WITH_MULTIPLE_IDPS

        metadata_loader = create_autospec(spec=SAMLMetadataLoader)
        metadata_validator = create_autospec(spec=SAMLFederatedMetadataValidator)
        metadata_parser = SAMLMetadataParser()
        idp_loader = SAMLFederatedIdentityProviderLoader(
            metadata_loader, metadata_validator, metadata_parser
        )
        saml_federation = SAMLFederation(
            federation_type, federation_idp_metadata_service_url
        )

        metadata_loader.load_idp_metadata = MagicMock(return_value=xml_metadata)
        metadata_parser.parse = MagicMock(side_effect=metadata_parser.parse)

        # Act
        idps = idp_loader.load(saml_federation)

        # Assert
        eq_(2, len(idps))

        eq_(fixtures.IDP_1_ENTITY_ID, idps[0].entity_id)
        eq_(fixtures.IDP_1_UI_INFO_EN_DISPLAY_NAME, idps[0].display_name)
        eq_(saml_federation, idps[0].federation)

        eq_(fixtures.IDP_2_ENTITY_ID, idps[1].entity_id)
        eq_(fixtures.IDP_2_UI_INFO_EN_DISPLAY_NAME, idps[1].display_name)
        eq_(saml_federation, idps[1].federation)

        metadata_loader.load_idp_metadata.assert_called_once_with(
            federation_idp_metadata_service_url
        )
        metadata_parser.parse.assert_called_once_with(xml_metadata)
