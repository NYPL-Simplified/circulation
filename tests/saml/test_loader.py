from mock import patch, MagicMock
from nose.tools import raises

from api.saml.loader import SAMLMetadataLoadingError, SAMLMetadataLoader
from tests.saml import fixtures


class TestSAMLMetadataLoader(object):
    @patch('urllib2.urlopen')
    @raises(SAMLMetadataLoadingError)
    def test_load_idp_metadata_raises_error_when_xml_is_incorrect(self, urlopen_mock):
        # Arrange
        url = 'http://md.incommon.org/InCommon/metadata.xml'
        incorrect_xml = fixtures.INCORRECT_XML
        urlopen_response_mock = MagicMock()
        urlopen_response_mock.read = MagicMock(return_value=incorrect_xml)
        urlopen_mock.return_value = urlopen_response_mock
        metadata_loader = SAMLMetadataLoader()

        # Act
        metadata_loader.load_idp_metadata(url)

    @patch('urllib2.urlopen')
    def test_load_idp_metadata_uses_incommon_metadata_service_by_default(self, urlopen_mock):
        # Arrange
        url = None
        incorrect_xml = fixtures.CORRECT_ONE_IDP_METADATA
        urlopen_response_mock = MagicMock()
        urlopen_response_mock.read = MagicMock(return_value=incorrect_xml)
        urlopen_mock.return_value = urlopen_response_mock
        metadata_loader = SAMLMetadataLoader()

        # Act
        xml_metadata = metadata_loader.load_idp_metadata(url)

        # Assert
        urlopen_mock.assert_called_with(SAMLMetadataLoader.IN_COMMON_METADATA_SERVICE_URL)
        assert xml_metadata is not None
        assert xml_metadata == fixtures.CORRECT_ONE_IDP_METADATA

    @patch('urllib2.urlopen')
    def test_load_idp_metadata_correctly_loads_one_descriptor(self, urlopen_mock):
        # Arrange
        url = 'http://md.incommon.org/InCommon/metadata.xml'
        incorrect_xml = fixtures.CORRECT_ONE_IDP_METADATA
        urlopen_response_mock = MagicMock()
        urlopen_response_mock.read = MagicMock(return_value=incorrect_xml)
        urlopen_mock.return_value = urlopen_response_mock
        metadata_loader = SAMLMetadataLoader()

        # Act
        xml_metadata = metadata_loader.load_idp_metadata(url)

        # Assert
        urlopen_mock.assert_called_with(url)
        assert xml_metadata is not None
        assert xml_metadata == fixtures.CORRECT_ONE_IDP_METADATA

    @patch('urllib2.urlopen')
    def test_load_idp_metadata_correctly_loads_multiple_descriptors(self, urlopen_mock):
        # Arrange
        url = 'http://md.incommon.org/InCommon/metadata.xml'
        incorrect_xml = fixtures.CORRECT_MULTIPLE_IDPS_METADATA
        urlopen_response_mock = MagicMock()
        urlopen_response_mock.read = MagicMock(return_value=incorrect_xml)
        urlopen_mock.return_value = urlopen_response_mock
        metadata_loader = SAMLMetadataLoader()

        # Act
        xml_metadata = metadata_loader.load_idp_metadata(url)

        # Assert
        urlopen_mock.assert_called_with(url)
        assert xml_metadata is not None
        assert xml_metadata == fixtures.CORRECT_MULTIPLE_IDPS_METADATA
