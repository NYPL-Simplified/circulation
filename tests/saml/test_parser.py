from mock import create_autospec, MagicMock
from nose.tools import raises, eq_
from onelogin.saml2.auth import OneLogin_Saml2_Auth
from parameterized import parameterized

from api.saml.metadata import IdentityProviderMetadata, UIInfo, LocalizableMetadataItem, Service, \
    ServiceProviderMetadata, NameIDFormat, Organization, Subject, NameID, Attribute, SAMLAttributes, AttributeStatement
from api.saml.parser import SAMLMetadataParsingError, SAMLMetadataParser, SAMLSubjectParser
from tests.saml import fixtures
from tests.saml.fixtures import strip_certificate


class SAMLMetadataParserTest(object):
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

    def test_parse_does_not_raise_exception_when_xml_metadata_does_not_have_display_names(self):
        # Arrange
        metadata_parser = SAMLMetadataParser()

        # Act
        result = metadata_parser.parse(fixtures.CORRECT_ONE_IDP_METADATA_WITHOUT_DISPLAY_NAMES)

        # Assert
        assert isinstance(result, list)
        eq_(len(result), 1)

        [result] = result

        eq_(
            result,
            IdentityProviderMetadata(
                entity_id=fixtures.IDP_1_ENTITY_ID,
                ui_info=UIInfo(),
                organization=Organization(),
                name_id_format=fixtures.NAME_ID_FORMAT_1,
                sso_service=Service(
                    fixtures.IDP_1_SSO_URL,
                    fixtures.IDP_1_SSO_BINDING
                ),
                want_authn_requests_signed=False,
                signing_certificates=[strip_certificate(fixtures.SIGNING_CERTIFICATE)],
                encryption_certificates=[strip_certificate(fixtures.ENCRYPTION_CERTIFICATE)]
            )
        )

    def test_parse_correctly_parses_one_idp_metadata(self):
        # Arrange
        metadata_parser = SAMLMetadataParser()

        # Act
        result = metadata_parser.parse(fixtures.CORRECT_ONE_IDP_METADATA)

        # Assert
        assert isinstance(result, list)
        eq_(len(result), 1)

        [result] = result

        eq_(
            result,
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
                organization=Organization(
                    [
                        LocalizableMetadataItem(fixtures.IDP_1_ORGANIZATION_EN_ORGANIZATION_NAME, 'en'),
                        LocalizableMetadataItem(fixtures.IDP_1_ORGANIZATION_ES_ORGANIZATION_NAME, 'es')
                    ],
                    [
                        LocalizableMetadataItem(fixtures.IDP_1_ORGANIZATION_EN_ORGANIZATION_DISPLAY_NAME, 'en'),
                        LocalizableMetadataItem(fixtures.IDP_1_ORGANIZATION_ES_ORGANIZATION_DISPLAY_NAME, 'es')
                    ],
                    [
                        LocalizableMetadataItem(fixtures.IDP_1_ORGANIZATION_EN_ORGANIZATION_URL, 'en'),
                        LocalizableMetadataItem(fixtures.IDP_1_ORGANIZATION_ES_ORGANIZATION_URL, 'es')
                    ],
                ),
                name_id_format=fixtures.NAME_ID_FORMAT_1,
                sso_service=Service(
                    fixtures.IDP_1_SSO_URL,
                    fixtures.IDP_1_SSO_BINDING
                ),
                want_authn_requests_signed=False,
                signing_certificates=[strip_certificate(fixtures.SIGNING_CERTIFICATE)],
                encryption_certificates=[strip_certificate(fixtures.ENCRYPTION_CERTIFICATE)]
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
                organization=Organization(
                    [
                        LocalizableMetadataItem(fixtures.IDP_1_ORGANIZATION_EN_ORGANIZATION_NAME, 'en'),
                        LocalizableMetadataItem(fixtures.IDP_1_ORGANIZATION_ES_ORGANIZATION_NAME, 'es')
                    ],
                    [
                        LocalizableMetadataItem(fixtures.IDP_1_ORGANIZATION_EN_ORGANIZATION_DISPLAY_NAME, 'en'),
                        LocalizableMetadataItem(fixtures.IDP_1_ORGANIZATION_ES_ORGANIZATION_DISPLAY_NAME, 'es')
                    ],
                    [
                        LocalizableMetadataItem(fixtures.IDP_1_ORGANIZATION_EN_ORGANIZATION_URL, 'en'),
                        LocalizableMetadataItem(fixtures.IDP_1_ORGANIZATION_ES_ORGANIZATION_URL, 'es')
                    ],
                ),
                name_id_format=NameIDFormat.UNSPECIFIED.value,
                sso_service=Service(
                    fixtures.IDP_1_SSO_URL,
                    fixtures.IDP_1_SSO_BINDING
                ),
                want_authn_requests_signed=False,
                signing_certificates=[strip_certificate(fixtures.SIGNING_CERTIFICATE)],
                encryption_certificates=[strip_certificate(fixtures.ENCRYPTION_CERTIFICATE)]
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
                organization=Organization(
                    [
                        LocalizableMetadataItem(fixtures.IDP_1_ORGANIZATION_EN_ORGANIZATION_NAME, 'en'),
                        LocalizableMetadataItem(fixtures.IDP_1_ORGANIZATION_ES_ORGANIZATION_NAME, 'es')
                    ],
                    [
                        LocalizableMetadataItem(fixtures.IDP_1_ORGANIZATION_EN_ORGANIZATION_DISPLAY_NAME, 'en'),
                        LocalizableMetadataItem(fixtures.IDP_1_ORGANIZATION_ES_ORGANIZATION_DISPLAY_NAME, 'es')
                    ],
                    [
                        LocalizableMetadataItem(fixtures.IDP_1_ORGANIZATION_EN_ORGANIZATION_URL, 'en'),
                        LocalizableMetadataItem(fixtures.IDP_1_ORGANIZATION_ES_ORGANIZATION_URL, 'es')
                    ],
                ),
                name_id_format=fixtures.NAME_ID_FORMAT_1,
                sso_service=Service(
                    fixtures.IDP_1_SSO_URL,
                    fixtures.IDP_1_SSO_BINDING
                ),
                want_authn_requests_signed=False,
                signing_certificates=[strip_certificate(fixtures.SIGNING_CERTIFICATE)],
                encryption_certificates=[strip_certificate(fixtures.SIGNING_CERTIFICATE)]
            )
        )

    def test_parse_correctly_parses_metadata_with_multiple_descriptors(self):
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
                organization=Organization(
                    [
                        LocalizableMetadataItem(fixtures.IDP_1_ORGANIZATION_EN_ORGANIZATION_NAME, 'en'),
                        LocalizableMetadataItem(fixtures.IDP_1_ORGANIZATION_ES_ORGANIZATION_NAME, 'es')
                    ],
                    [
                        LocalizableMetadataItem(fixtures.IDP_1_ORGANIZATION_EN_ORGANIZATION_DISPLAY_NAME, 'en'),
                        LocalizableMetadataItem(fixtures.IDP_1_ORGANIZATION_ES_ORGANIZATION_DISPLAY_NAME, 'es')
                    ],
                    [
                        LocalizableMetadataItem(fixtures.IDP_1_ORGANIZATION_EN_ORGANIZATION_URL, 'en'),
                        LocalizableMetadataItem(fixtures.IDP_1_ORGANIZATION_ES_ORGANIZATION_URL, 'es')
                    ],
                ),
                name_id_format=fixtures.NAME_ID_FORMAT_1,
                sso_service=Service(
                    fixtures.IDP_1_SSO_URL,
                    fixtures.IDP_1_SSO_BINDING
                ),
                want_authn_requests_signed=False,
                signing_certificates=[strip_certificate(fixtures.SIGNING_CERTIFICATE)],
                encryption_certificates=[strip_certificate(fixtures.ENCRYPTION_CERTIFICATE)]
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
                organization=Organization(
                    [
                        LocalizableMetadataItem(fixtures.IDP_2_ORGANIZATION_EN_ORGANIZATION_NAME, 'en'),
                        LocalizableMetadataItem(fixtures.IDP_2_ORGANIZATION_ES_ORGANIZATION_NAME, 'es')
                    ],
                    [
                        LocalizableMetadataItem(fixtures.IDP_2_ORGANIZATION_EN_ORGANIZATION_DISPLAY_NAME, 'en'),
                        LocalizableMetadataItem(fixtures.IDP_2_ORGANIZATION_ES_ORGANIZATION_DISPLAY_NAME, 'es')
                    ],
                    [
                        LocalizableMetadataItem(fixtures.IDP_2_ORGANIZATION_EN_ORGANIZATION_URL, 'en'),
                        LocalizableMetadataItem(fixtures.IDP_2_ORGANIZATION_ES_ORGANIZATION_URL, 'es')
                    ],
                ),
                name_id_format=fixtures.NAME_ID_FORMAT_1,
                sso_service=Service(
                    fixtures.IDP_2_SSO_URL,
                    fixtures.IDP_2_SSO_BINDING
                ),
                want_authn_requests_signed=False,
                signing_certificates=[strip_certificate(fixtures.SIGNING_CERTIFICATE)],
                encryption_certificates=[strip_certificate(fixtures.ENCRYPTION_CERTIFICATE)]
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

        [result] = result

        eq_(
            result,
            ServiceProviderMetadata(
                entity_id=fixtures.SP_ENTITY_ID,
                ui_info=UIInfo(
                    [
                        LocalizableMetadataItem(fixtures.SP_UI_INFO_EN_DISPLAY_NAME, 'en'),
                        LocalizableMetadataItem(fixtures.SP_UI_INFO_ES_DISPLAY_NAME, 'es')
                    ],
                    [
                        LocalizableMetadataItem(fixtures.SP_UI_INFO_DESCRIPTION, 'en')
                    ],
                    [
                        LocalizableMetadataItem(fixtures.SP_UI_INFO_INFORMATION_URL, 'en')
                    ],
                    [
                        LocalizableMetadataItem(fixtures.SP_UI_INFO_PRIVACY_STATEMENT_URL, 'en')
                    ],
                    [
                        LocalizableMetadataItem(fixtures.SP_UI_INFO_LOGO_URL)
                    ]
                ),
                organization=Organization(
                    [
                        LocalizableMetadataItem(fixtures.SP_ORGANIZATION_EN_ORGANIZATION_NAME, 'en'),
                        LocalizableMetadataItem(fixtures.SP_ORGANIZATION_ES_ORGANIZATION_NAME, 'es')
                    ],
                    [
                        LocalizableMetadataItem(fixtures.SP_ORGANIZATION_EN_ORGANIZATION_DISPLAY_NAME, 'en'),
                        LocalizableMetadataItem(fixtures.SP_ORGANIZATION_ES_ORGANIZATION_DISPLAY_NAME, 'es')
                    ],
                    [
                        LocalizableMetadataItem(fixtures.SP_ORGANIZATION_EN_ORGANIZATION_URL, 'en'),
                        LocalizableMetadataItem(fixtures.SP_ORGANIZATION_ES_ORGANIZATION_URL, 'es')
                    ],
                ),
                name_id_format=NameIDFormat.UNSPECIFIED.value,
                acs_service=Service(
                    fixtures.SP_ACS_URL,
                    fixtures.SP_ACS_BINDING
                ),
                authn_requests_signed=False,
                want_assertions_signed=False,
                certificate=strip_certificate(fixtures.SIGNING_CERTIFICATE)
            )
        )


class SAMLSubjectParserTest(object):
    @parameterized.expand([
        (
            'name_id_and_attributes',
            NameIDFormat.TRANSIENT.value, fixtures.IDP_1_ENTITY_ID, fixtures.SP_ENTITY_ID, '12345',
            {
                SAMLAttributes.eduPersonUniqueId.value: ['12345']
            },
            Subject(
                NameID(
                    NameIDFormat.TRANSIENT.value,
                    fixtures.IDP_1_ENTITY_ID,
                    fixtures.SP_ENTITY_ID,
                    '12345'
                ),
                AttributeStatement(
                    [
                        Attribute(SAMLAttributes.eduPersonUniqueId.name, ['12345'])
                    ]
                )
            )
        ),
        (
            'edu_person_targeted_id_as_name_id',
            None, None, None, None,
            {
                SAMLAttributes.eduPersonTargetedID.value: [
                    {
                        'NameID': {
                            'Format': NameIDFormat.PERSISTENT.value,
                            'NameQualifier': fixtures.IDP_1_ENTITY_ID,
                            'value': '12345'
                        }
                    }
                ]
            },
            Subject(
                NameID(
                    NameIDFormat.PERSISTENT.value,
                    fixtures.IDP_1_ENTITY_ID,
                    None,
                    '12345'
                ),
                AttributeStatement(
                    [
                        Attribute(SAMLAttributes.eduPersonTargetedID.name, ['12345'])
                    ]
                )
            )
        ),
        (
            'edu_person_targeted_id_as_name_id_and_other_attributes',
            None, None, None, None,
            {
                SAMLAttributes.eduPersonTargetedID.value: [
                    {
                        'NameID': {
                            'Format': NameIDFormat.PERSISTENT.value,
                            'NameQualifier': fixtures.IDP_1_ENTITY_ID,
                            'value': '12345'
                        }
                    }
                ],
                SAMLAttributes.eduPersonPrincipalName.value: [
                    '12345'
                ]
            },
            Subject(
                NameID(
                    NameIDFormat.PERSISTENT.value,
                    fixtures.IDP_1_ENTITY_ID,
                    None,
                    '12345'
                ),
                AttributeStatement(
                    [
                        Attribute(SAMLAttributes.eduPersonTargetedID.name, ['12345']),
                        Attribute(SAMLAttributes.eduPersonPrincipalName.name, ['12345'])
                    ]
                )
            )
        ),
        (
            'edu_person_principal_name_as_name_id',
            None, None, None, None,
            {
                SAMLAttributes.eduPersonPrincipalName.value: [
                    {
                        'NameID': {
                            'Format': NameIDFormat.PERSISTENT.value,
                            'NameQualifier': fixtures.IDP_1_ENTITY_ID,
                            'value': '12345'
                        }
                    }
                ]
            },
            Subject(
                NameID(
                    NameIDFormat.PERSISTENT.value,
                    fixtures.IDP_1_ENTITY_ID,
                    None,
                    '12345'
                ),
                AttributeStatement(
                    [
                        Attribute(SAMLAttributes.eduPersonPrincipalName.name, ['12345'])
                    ]
                )
            )
        )
    ])
    def test_parse(self, name, name_id_format, name_id_nq, name_id_spnq, name_id, attributes, expected_result):
        # Arrange
        parser = SAMLSubjectParser()
        auth = create_autospec(spec=OneLogin_Saml2_Auth)
        auth.get_nameid_format = MagicMock(return_value=name_id_format)
        auth.get_nameid_nq = MagicMock(return_value=name_id_nq)
        auth.get_nameid_spnq = MagicMock(return_value=name_id_spnq)
        auth.get_nameid = MagicMock(return_value=name_id)
        auth.get_attributes = MagicMock(return_value=attributes)
        auth.get_session_expiration = MagicMock(return_value=None)
        auth.get_last_assertion_not_on_or_after = MagicMock(return_value=None)

        # Act
        result = parser.parse(auth)

        # Arrange
        eq_(result, expected_result)
