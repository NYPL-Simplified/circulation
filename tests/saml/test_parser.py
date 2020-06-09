from nose.tools import raises, eq_

from api.saml.metadata import IdentityProviderMetadata, UIInfo, LocalizableMetadataItem, Service, \
    ServiceProviderMetadata, NameIDFormat
from api.saml.parser import SAMLMetadataParsingError, SAMLMetadataParser
from tests.saml import fixtures


class SAMLMetadataParserTest(object):
    def _strip_certificate(self, certificate):
        """
        Converts certificate to a one-line format

        :param certificate: Certificate in a multi-line format
        :type certificate: string

        :return: Certificate in a one-line format
        :rtype: string
        """

        return certificate.replace('\n', '')

    def _check_idp_metadata(
            self,
            idp_metadata,
            entity_id,
            ui_info,
            name_id_format,
            sso_service,
            want_authn_requests_signed,
            signing_certificates,
            encryption_certificates):
        assert isinstance(idp_metadata, IdentityProviderMetadata)
        assert idp_metadata.entity_id == entity_id

        assert isinstance(idp_metadata.ui_info, UIInfo)
        assert idp_metadata.ui_info == ui_info

        assert idp_metadata.name_id_format == name_id_format

        assert idp_metadata.sso_service == sso_service

        assert idp_metadata.want_authn_requests_signed == want_authn_requests_signed

        assert idp_metadata.signing_certificates == signing_certificates
        assert idp_metadata.encryption_certificates == encryption_certificates

    @raises(SAMLMetadataParsingError)
    def test_parse_raises_exception_when_xml_metadata_has_incorrect_format(self):
        # Arrange
        metadata_parser = SAMLMetadataParser()

        # Act
        metadata_parser.parse(fixtures.INCORRECT_XML)

    @raises(SAMLMetadataParsingError)
    def test_parse_raises_exception_when_idp_metadata_does_not_contain_sso_service(self):
        # Arrange
        metadata_parser = SAMLMetadataParser()

        # Act
        metadata_parser.parse(fixtures.INCORRECT_ONE_IDP_METADATA_WITHOUT_SSO_SERVICE)

    @raises(SAMLMetadataParsingError)
    def test_parse_raises_exception_when_idp_metadata_contains_sso_service_with_wrong_binding(self):
        # Arrange
        metadata_parser = SAMLMetadataParser()

        # Act
        metadata_parser.parse(fixtures.INCORRECT_ONE_IDP_METADATA_WITH_SSO_SERVICE_WITH_WRONG_BINDING)

    def test_parse_correctly_parses_one_idp_metadata(self):
        # Arrange
        metadata_parser = SAMLMetadataParser()

        # Act
        result = metadata_parser.parse(fixtures.CORRECT_ONE_IDP_METADATA)

        # Assert
        assert isinstance(result, list)
        eq_(len(result), 1)

        eq_(
            result[0],
            IdentityProviderMetadata(
                entity_id=fixtures.IDP_1_ENTITY_ID,
                ui_info=UIInfo(
                    [
                        LocalizableMetadataItem(fixtures.IDP_1_UI_INFO_EN_DISPLAY_NAME, 'en'),
                        LocalizableMetadataItem(fixtures.IDP_1_UI_INFO_ES_DISPLAY_NAME, 'es')
                    ],
                    [
                        LocalizableMetadataItem(fixtures.IDP_1_UI_INFO_DESCRIPTION, 'en')
                    ],
                    [
                        LocalizableMetadataItem(fixtures.IDP_1_UI_INFO_INFORMATION_URL, 'en')
                    ],
                    [
                        LocalizableMetadataItem(fixtures.IDP_1_UI_INFO_PRIVACY_STATEMENT_URL, 'en')
                    ],
                    [
                        LocalizableMetadataItem(fixtures.IDP_1_UI_INFO_LOGO_URL)
                    ]
                ),
                name_id_format=fixtures.NAME_ID_FORMAT_1,
                sso_service=Service(
                    fixtures.IDP_1_SSO_URL,
                    fixtures.IDP_1_SSO_BINDING
                ),
                want_authn_requests_signed=False,
                signing_certificates=[self._strip_certificate(fixtures.SIGNING_CERTIFICATE)],
                encryption_certificates=[self._strip_certificate(fixtures.ENCRYPTION_CERTIFICATE)]
            )
        )

    def test_parse_correctly_parses_idp_metadata_without_name_id_format(self):
        # Arrange
        metadata_parser = SAMLMetadataParser()

        # Act
        result = metadata_parser.parse(fixtures.CORRECT_ONE_IDP_METADATA_WITHOUT_NAME_ID_FORMAT)

        # Assert
        assert isinstance(result, list)
        eq_(len(result), 1)

        eq_(
            result[0],
            IdentityProviderMetadata(
                entity_id=fixtures.IDP_1_ENTITY_ID,
                ui_info=UIInfo(
                    [
                        LocalizableMetadataItem(fixtures.IDP_1_UI_INFO_EN_DISPLAY_NAME, 'en'),
                        LocalizableMetadataItem(fixtures.IDP_1_UI_INFO_ES_DISPLAY_NAME, 'es')
                    ],
                    [
                        LocalizableMetadataItem(fixtures.IDP_1_UI_INFO_DESCRIPTION, 'en')
                    ],
                    [
                        LocalizableMetadataItem(fixtures.IDP_1_UI_INFO_INFORMATION_URL, 'en')
                    ],
                    [
                        LocalizableMetadataItem(fixtures.IDP_1_UI_INFO_PRIVACY_STATEMENT_URL, 'en')
                    ],
                    [
                        LocalizableMetadataItem(fixtures.IDP_1_UI_INFO_LOGO_URL, 'en')
                    ]
                ),
                name_id_format=NameIDFormat.UNSPECIFIED.value,
                sso_service=Service(
                    fixtures.IDP_1_SSO_URL,
                    fixtures.IDP_1_SSO_BINDING
                ),
                want_authn_requests_signed=False,
                signing_certificates=[self._strip_certificate(fixtures.SIGNING_CERTIFICATE)],
                encryption_certificates=[self._strip_certificate(fixtures.ENCRYPTION_CERTIFICATE)]
            )
        )

    def test_parse_correctly_parses_idp_metadata_with_one_certificate(self):
        # Arrange
        metadata_parser = SAMLMetadataParser()

        # Act
        result = metadata_parser.parse(fixtures.CORRECT_ONE_IDP_METADATA_WITH_ONE_CERTIFICATE)

        # Assert
        eq_(
            result[0],
            IdentityProviderMetadata(
                entity_id=fixtures.IDP_1_ENTITY_ID,
                ui_info=UIInfo(
                    [
                        LocalizableMetadataItem(fixtures.IDP_1_UI_INFO_EN_DISPLAY_NAME, 'en'),
                        LocalizableMetadataItem(fixtures.IDP_1_UI_INFO_ES_DISPLAY_NAME, 'es')
                    ],
                    [
                        LocalizableMetadataItem(fixtures.IDP_1_UI_INFO_DESCRIPTION, 'en')
                    ],
                    [
                        LocalizableMetadataItem(fixtures.IDP_1_UI_INFO_INFORMATION_URL, 'en')
                    ],
                    [
                        LocalizableMetadataItem(fixtures.IDP_1_UI_INFO_PRIVACY_STATEMENT_URL, 'en')
                    ],
                    [
                        LocalizableMetadataItem(fixtures.IDP_1_UI_INFO_LOGO_URL, 'en')
                    ]
                ),
                name_id_format=fixtures.NAME_ID_FORMAT_1,
                sso_service=Service(
                    fixtures.IDP_1_SSO_URL,
                    fixtures.IDP_1_SSO_BINDING
                ),
                want_authn_requests_signed=False,
                signing_certificates=[self._strip_certificate(fixtures.SIGNING_CERTIFICATE)],
                encryption_certificates=[self._strip_certificate(fixtures.SIGNING_CERTIFICATE)]
            )
        )

    def test_parse_correctly_parses_metadata_with_multiple_descriptors_(self):
        # Arrange
        metadata_parser = SAMLMetadataParser()

        # Act
        result = metadata_parser.parse(fixtures.CORRECT_MULTIPLE_IDPS_METADATA)

        # Assert
        assert isinstance(result, list)
        assert len(result) == 2

        eq_(
            result[0],
            IdentityProviderMetadata(
                entity_id=fixtures.IDP_1_ENTITY_ID,
                ui_info=UIInfo(
                    [
                        LocalizableMetadataItem(fixtures.IDP_1_UI_INFO_EN_DISPLAY_NAME, 'en'),
                        LocalizableMetadataItem(fixtures.IDP_1_UI_INFO_ES_DISPLAY_NAME, 'es')
                    ]
                ),
                name_id_format=fixtures.NAME_ID_FORMAT_1,
                sso_service=Service(
                    fixtures.IDP_1_SSO_URL,
                    fixtures.IDP_1_SSO_BINDING
                ),
                want_authn_requests_signed=False,
                signing_certificates=[self._strip_certificate(fixtures.SIGNING_CERTIFICATE)],
                encryption_certificates=[self._strip_certificate(fixtures.ENCRYPTION_CERTIFICATE)]
            )
        )

        eq_(
            result[1],
            IdentityProviderMetadata(
                entity_id=fixtures.IDP_2_ENTITY_ID,
                ui_info=UIInfo(
                    [
                        LocalizableMetadataItem(fixtures.IDP_2_UI_INFO_EN_DISPLAY_NAME, 'en'),
                        LocalizableMetadataItem(fixtures.IDP_2_UI_INFO_ES_DISPLAY_NAME, 'es')
                    ]
                ),
                name_id_format=fixtures.NAME_ID_FORMAT_1,
                sso_service=Service(
                    fixtures.IDP_2_SSO_URL,
                    fixtures.IDP_2_SSO_BINDING
                ),
                want_authn_requests_signed=False,
                signing_certificates=[self._strip_certificate(fixtures.SIGNING_CERTIFICATE)],
                encryption_certificates=[self._strip_certificate(fixtures.ENCRYPTION_CERTIFICATE)]
            )
        )

    @raises(SAMLMetadataParsingError)
    def test_parse_raises_exception_when_sp_metadata_does_not_contain_acs_service(self):
        # Arrange
        metadata_parser = SAMLMetadataParser()

        # Act
        metadata_parser.parse(fixtures.INCORRECT_ONE_SP_METADATA_WITHOUT_ACS_SERVICE)

    def test_parse_correctly_parses_one_sp_metadata(self):
        # Arrange
        metadata_parser = SAMLMetadataParser()

        # Act
        result = metadata_parser.parse(fixtures.CORRECT_ONE_SP_METADATA)

        # Assert
        assert isinstance(result, list)
        eq_(len(result), 1)

        eq_(
            result[0],
            ServiceProviderMetadata(
                entity_id=fixtures.SP_ENTITY_ID,
                ui_info=UIInfo(),
                name_id_format=NameIDFormat.UNSPECIFIED.value,
                acs_service=Service(
                    fixtures.SP_ACS_URL,
                    fixtures.SP_ACS_BINDING
                ),
                authn_requests_signed=False,
                want_assertions_signed=False,
                certificate=[self._strip_certificate(fixtures.SIGNING_CERTIFICATE)]
            )
        )
