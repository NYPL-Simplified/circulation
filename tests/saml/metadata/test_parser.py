import defusedxml
import pytest
from mock import MagicMock, create_autospec
from onelogin.saml2.auth import OneLogin_Saml2_Auth
from parameterized import parameterized

from api.saml.metadata.model import (
    SAMLAttribute,
    SAMLAttributeStatement,
    SAMLAttributeType,
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
from api.saml.metadata.parser import (
    SAMLMetadataParser,
    SAMLMetadataParsingError,
    SAMLMetadataParsingResult,
    SAMLSubjectParser,
)
from tests.saml import fixtures


class TestSAMLMetadataParser(object):
    def test_parse_raises_exception_when_xml_metadata_has_incorrect_format(self):
        # Arrange
        metadata_parser = SAMLMetadataParser()

        # Act
        with pytest.raises(SAMLMetadataParsingError):
            metadata_parser.parse(fixtures.INCORRECT_XML)

    def test_parse_raises_exception_when_idp_metadata_does_not_contain_sso_service(
        self,
    ):
        # Arrange
        metadata_parser = SAMLMetadataParser()

        # Act
        with pytest.raises(SAMLMetadataParsingError):
            metadata_parser.parse(
                fixtures.INCORRECT_XML_WITH_ONE_IDP_METADATA_WITHOUT_SSO_SERVICE
            )

    def test_parse_raises_exception_when_idp_metadata_contains_sso_service_with_wrong_binding(
        self,
    ):
        # Arrange
        metadata_parser = SAMLMetadataParser()

        # Act
        with pytest.raises(SAMLMetadataParsingError):
            metadata_parser.parse(
                fixtures.INCORRECT_XML_WITH_ONE_IDP_METADATA_WITH_SSO_SERVICE_WITH_WRONG_BINDING
            )

    def test_parse_does_not_raise_exception_when_xml_metadata_does_not_have_display_names(
        self,
    ):
        # Arrange
        metadata_parser = SAMLMetadataParser()

        # Act
        parsing_results = metadata_parser.parse(
            fixtures.CORRECT_XML_WITH_ONE_IDP_METADATA_WITHOUT_DISPLAY_NAMES
        )

        # Assert
        assert 1 == len(parsing_results)

        [parsing_result] = parsing_results
        assert True == isinstance(parsing_result, SAMLMetadataParsingResult)
        assert True == isinstance(parsing_result.provider, SAMLIdentityProviderMetadata)
        assert (
            True == isinstance(parsing_result.xml_node, defusedxml.lxml.RestrictedElement))
        assert (
            SAMLIdentityProviderMetadata(
                entity_id=fixtures.IDP_1_ENTITY_ID,
                ui_info=SAMLUIInfo(),
                organization=SAMLOrganization(),
                name_id_format=fixtures.NAME_ID_FORMAT_1,
                sso_service=SAMLService(
                    fixtures.IDP_1_SSO_URL, fixtures.IDP_1_SSO_BINDING
                ),
                want_authn_requests_signed=False,
                signing_certificates=[
                    fixtures.strip_certificate(fixtures.SIGNING_CERTIFICATE)
                ],
                encryption_certificates=[
                    fixtures.strip_certificate(fixtures.ENCRYPTION_CERTIFICATE)
                ],
            ) ==
            parsing_result.provider)

    def test_parse_correctly_parses_one_idp_metadata(self):
        # Arrange
        metadata_parser = SAMLMetadataParser()

        # Act
        parsing_results = metadata_parser.parse(fixtures.CORRECT_XML_WITH_IDP_1)

        # Assert
        assert 1 == len(parsing_results)

        [parsing_result] = parsing_results
        assert True == isinstance(parsing_result, SAMLMetadataParsingResult)
        assert True == isinstance(parsing_result.provider, SAMLIdentityProviderMetadata)
        assert (
            True == isinstance(parsing_result.xml_node, defusedxml.lxml.RestrictedElement))
        assert (
            SAMLIdentityProviderMetadata(
                entity_id=fixtures.IDP_1_ENTITY_ID,
                ui_info=SAMLUIInfo(
                    [
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_1_UI_INFO_EN_DISPLAY_NAME, "en"
                        ),
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_1_UI_INFO_ES_DISPLAY_NAME, "es"
                        ),
                    ],
                    [
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_1_UI_INFO_DESCRIPTION, "en"
                        )
                    ],
                    [
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_1_UI_INFO_INFORMATION_URL, "en"
                        )
                    ],
                    [
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_1_UI_INFO_PRIVACY_STATEMENT_URL, "en"
                        )
                    ],
                    [SAMLLocalizedMetadataItem(fixtures.IDP_1_UI_INFO_LOGO_URL, "en")],
                ),
                organization=SAMLOrganization(
                    [
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_1_ORGANIZATION_EN_ORGANIZATION_NAME, "en"
                        ),
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_1_ORGANIZATION_ES_ORGANIZATION_NAME, "es"
                        ),
                    ],
                    [
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_1_ORGANIZATION_EN_ORGANIZATION_DISPLAY_NAME,
                            "en",
                        ),
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_1_ORGANIZATION_ES_ORGANIZATION_DISPLAY_NAME,
                            "es",
                        ),
                    ],
                    [
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_1_ORGANIZATION_EN_ORGANIZATION_URL, "en"
                        ),
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_1_ORGANIZATION_ES_ORGANIZATION_URL, "es"
                        ),
                    ],
                ),
                name_id_format=fixtures.NAME_ID_FORMAT_1,
                sso_service=SAMLService(
                    fixtures.IDP_1_SSO_URL, fixtures.IDP_1_SSO_BINDING
                ),
                want_authn_requests_signed=False,
                signing_certificates=[
                    fixtures.strip_certificate(fixtures.SIGNING_CERTIFICATE)
                ],
                encryption_certificates=[
                    fixtures.strip_certificate(fixtures.ENCRYPTION_CERTIFICATE)
                ],
            ) ==
            parsing_result.provider)

    def test_parse_correctly_parses_idp_metadata_without_name_id_format(self):
        # Arrange
        metadata_parser = SAMLMetadataParser()

        # Act
        parsing_results = metadata_parser.parse(fixtures.CORRECT_XML_WITH_IDP_1)

        # Assert
        assert 1 == len(parsing_results)

        [parsing_result] = parsing_results
        assert True == isinstance(parsing_result, SAMLMetadataParsingResult)
        assert True == isinstance(parsing_result.provider, SAMLIdentityProviderMetadata)
        assert (
            True == isinstance(parsing_result.xml_node, defusedxml.lxml.RestrictedElement))
        assert (
            SAMLIdentityProviderMetadata(
                entity_id=fixtures.IDP_1_ENTITY_ID,
                ui_info=SAMLUIInfo(
                    [
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_1_UI_INFO_EN_DISPLAY_NAME, "en"
                        ),
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_1_UI_INFO_ES_DISPLAY_NAME, "es"
                        ),
                    ],
                    [
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_1_UI_INFO_DESCRIPTION, "en"
                        )
                    ],
                    [
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_1_UI_INFO_INFORMATION_URL, "en"
                        )
                    ],
                    [
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_1_UI_INFO_PRIVACY_STATEMENT_URL, "en"
                        )
                    ],
                    [SAMLLocalizedMetadataItem(fixtures.IDP_1_UI_INFO_LOGO_URL, "en")],
                ),
                organization=SAMLOrganization(
                    [
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_1_ORGANIZATION_EN_ORGANIZATION_NAME, "en"
                        ),
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_1_ORGANIZATION_ES_ORGANIZATION_NAME, "es"
                        ),
                    ],
                    [
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_1_ORGANIZATION_EN_ORGANIZATION_DISPLAY_NAME,
                            "en",
                        ),
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_1_ORGANIZATION_ES_ORGANIZATION_DISPLAY_NAME,
                            "es",
                        ),
                    ],
                    [
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_1_ORGANIZATION_EN_ORGANIZATION_URL, "en"
                        ),
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_1_ORGANIZATION_ES_ORGANIZATION_URL, "es"
                        ),
                    ],
                ),
                name_id_format=fixtures.NAME_ID_FORMAT_1,
                sso_service=SAMLService(
                    fixtures.IDP_1_SSO_URL, fixtures.IDP_1_SSO_BINDING
                ),
                want_authn_requests_signed=False,
                signing_certificates=[
                    fixtures.strip_certificate(fixtures.SIGNING_CERTIFICATE)
                ],
                encryption_certificates=[
                    fixtures.strip_certificate(fixtures.ENCRYPTION_CERTIFICATE)
                ],
            ) ==
            parsing_result.provider)

    def test_parse_correctly_parses_idp_metadata_with_one_certificate(self):
        # Arrange
        metadata_parser = SAMLMetadataParser()

        # Act
        parsing_results = metadata_parser.parse(
            fixtures.CORRECT_XML_WITH_ONE_IDP_METADATA_WITH_ONE_CERTIFICATE
        )

        # Assert
        assert 1 == len(parsing_results)
        [parsing_result] = parsing_results

        assert True == isinstance(parsing_result, SAMLMetadataParsingResult)
        assert True == isinstance(parsing_result.provider, SAMLIdentityProviderMetadata)
        assert (
            True == isinstance(parsing_result.xml_node, defusedxml.lxml.RestrictedElement))
        assert (
            SAMLIdentityProviderMetadata(
                entity_id=fixtures.IDP_1_ENTITY_ID,
                ui_info=SAMLUIInfo(
                    [
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_1_UI_INFO_EN_DISPLAY_NAME, "en"
                        ),
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_1_UI_INFO_ES_DISPLAY_NAME, "es"
                        ),
                    ],
                    [
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_1_UI_INFO_DESCRIPTION, "en"
                        )
                    ],
                    [
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_1_UI_INFO_INFORMATION_URL, "en"
                        )
                    ],
                    [
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_1_UI_INFO_PRIVACY_STATEMENT_URL, "en"
                        )
                    ],
                    [SAMLLocalizedMetadataItem(fixtures.IDP_1_UI_INFO_LOGO_URL, "en")],
                ),
                organization=SAMLOrganization(
                    [
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_1_ORGANIZATION_EN_ORGANIZATION_NAME, "en"
                        ),
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_1_ORGANIZATION_ES_ORGANIZATION_NAME, "es"
                        ),
                    ],
                    [
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_1_ORGANIZATION_EN_ORGANIZATION_DISPLAY_NAME,
                            "en",
                        ),
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_1_ORGANIZATION_ES_ORGANIZATION_DISPLAY_NAME,
                            "es",
                        ),
                    ],
                    [
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_1_ORGANIZATION_EN_ORGANIZATION_URL, "en"
                        ),
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_1_ORGANIZATION_ES_ORGANIZATION_URL, "es"
                        ),
                    ],
                ),
                name_id_format=fixtures.NAME_ID_FORMAT_1,
                sso_service=SAMLService(
                    fixtures.IDP_1_SSO_URL, fixtures.IDP_1_SSO_BINDING
                ),
                want_authn_requests_signed=False,
                signing_certificates=[
                    fixtures.strip_certificate(fixtures.SIGNING_CERTIFICATE)
                ],
                encryption_certificates=[
                    fixtures.strip_certificate(fixtures.SIGNING_CERTIFICATE)
                ],
            ) ==
            parsing_result.provider)

    def test_parse_correctly_parses_metadata_with_multiple_descriptors(self):
        # Arrange
        metadata_parser = SAMLMetadataParser()

        # Act
        parsing_results = metadata_parser.parse(fixtures.CORRECT_XML_WITH_MULTIPLE_IDPS)

        # Assert
        assert 2 == len(parsing_results)
        assert True == isinstance(parsing_results[0], SAMLMetadataParsingResult)
        assert True == isinstance(parsing_results[0].provider, SAMLIdentityProviderMetadata)
        assert (
            True ==
            isinstance(parsing_results[0].xml_node, defusedxml.lxml.RestrictedElement))
        assert (
            SAMLIdentityProviderMetadata(
                entity_id=fixtures.IDP_1_ENTITY_ID,
                ui_info=SAMLUIInfo(
                    [
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_1_UI_INFO_EN_DISPLAY_NAME, "en"
                        ),
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_1_UI_INFO_ES_DISPLAY_NAME, "es"
                        ),
                    ]
                ),
                organization=SAMLOrganization(
                    [
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_1_ORGANIZATION_EN_ORGANIZATION_NAME, "en"
                        ),
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_1_ORGANIZATION_ES_ORGANIZATION_NAME, "es"
                        ),
                    ],
                    [
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_1_ORGANIZATION_EN_ORGANIZATION_DISPLAY_NAME,
                            "en",
                        ),
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_1_ORGANIZATION_ES_ORGANIZATION_DISPLAY_NAME,
                            "es",
                        ),
                    ],
                    [
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_1_ORGANIZATION_EN_ORGANIZATION_URL, "en"
                        ),
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_1_ORGANIZATION_ES_ORGANIZATION_URL, "es"
                        ),
                    ],
                ),
                name_id_format=fixtures.NAME_ID_FORMAT_1,
                sso_service=SAMLService(
                    fixtures.IDP_1_SSO_URL, fixtures.IDP_1_SSO_BINDING
                ),
                want_authn_requests_signed=False,
                signing_certificates=[
                    fixtures.strip_certificate(fixtures.SIGNING_CERTIFICATE)
                ],
                encryption_certificates=[
                    fixtures.strip_certificate(fixtures.ENCRYPTION_CERTIFICATE)
                ],
            ) ==
            parsing_results[0].provider)

        assert True == isinstance(parsing_results[1], SAMLMetadataParsingResult)
        assert True == isinstance(parsing_results[1].provider, SAMLIdentityProviderMetadata)
        assert (
            True ==
            isinstance(parsing_results[1].xml_node, defusedxml.lxml.RestrictedElement))
        assert (
            SAMLIdentityProviderMetadata(
                entity_id=fixtures.IDP_2_ENTITY_ID,
                ui_info=SAMLUIInfo(
                    [
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_2_UI_INFO_EN_DISPLAY_NAME, "en"
                        ),
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_2_UI_INFO_ES_DISPLAY_NAME, "es"
                        ),
                    ]
                ),
                organization=SAMLOrganization(
                    [
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_2_ORGANIZATION_EN_ORGANIZATION_NAME, "en"
                        ),
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_2_ORGANIZATION_ES_ORGANIZATION_NAME, "es"
                        ),
                    ],
                    [
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_2_ORGANIZATION_EN_ORGANIZATION_DISPLAY_NAME,
                            "en",
                        ),
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_2_ORGANIZATION_ES_ORGANIZATION_DISPLAY_NAME,
                            "es",
                        ),
                    ],
                    [
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_2_ORGANIZATION_EN_ORGANIZATION_URL, "en"
                        ),
                        SAMLLocalizedMetadataItem(
                            fixtures.IDP_2_ORGANIZATION_ES_ORGANIZATION_URL, "es"
                        ),
                    ],
                ),
                name_id_format=fixtures.NAME_ID_FORMAT_1,
                sso_service=SAMLService(
                    fixtures.IDP_2_SSO_URL, fixtures.IDP_2_SSO_BINDING
                ),
                want_authn_requests_signed=False,
                signing_certificates=[
                    fixtures.strip_certificate(fixtures.SIGNING_CERTIFICATE)
                ],
                encryption_certificates=[
                    fixtures.strip_certificate(fixtures.ENCRYPTION_CERTIFICATE)
                ],
            ) ==
            parsing_results[1].provider)

    def test_parse_raises_exception_when_sp_metadata_does_not_contain_acs_service(self):
        # Arrange
        metadata_parser = SAMLMetadataParser()

        # Act
        with pytest.raises(SAMLMetadataParsingError):
            metadata_parser.parse(
                fixtures.INCORRECT_XML_WITH_ONE_SP_METADATA_WITHOUT_ACS_SERVICE
            )

    def test_parse_correctly_parses_one_sp_metadata(self):
        # Arrange
        metadata_parser = SAMLMetadataParser()

        # Act
        parsing_results = metadata_parser.parse(fixtures.CORRECT_XML_WITH_ONE_SP)

        # Assert
        assert 1 == len(parsing_results)

        [parsing_result] = parsing_results
        assert True == isinstance(parsing_result, SAMLMetadataParsingResult)
        assert True == isinstance(parsing_result.provider, SAMLServiceProviderMetadata)
        assert (
            True == isinstance(parsing_result.xml_node, defusedxml.lxml.RestrictedElement))

        assert (
            SAMLServiceProviderMetadata(
                entity_id=fixtures.SP_ENTITY_ID,
                ui_info=SAMLUIInfo(
                    [
                        SAMLLocalizedMetadataItem(
                            fixtures.SP_UI_INFO_EN_DISPLAY_NAME, "en"
                        ),
                        SAMLLocalizedMetadataItem(
                            fixtures.SP_UI_INFO_ES_DISPLAY_NAME, "es"
                        ),
                    ],
                    [SAMLLocalizedMetadataItem(fixtures.SP_UI_INFO_DESCRIPTION, "en")],
                    [
                        SAMLLocalizedMetadataItem(
                            fixtures.SP_UI_INFO_INFORMATION_URL, "en"
                        )
                    ],
                    [
                        SAMLLocalizedMetadataItem(
                            fixtures.SP_UI_INFO_PRIVACY_STATEMENT_URL, "en"
                        )
                    ],
                    [SAMLLocalizedMetadataItem(fixtures.SP_UI_INFO_LOGO_URL)],
                ),
                organization=SAMLOrganization(
                    [
                        SAMLLocalizedMetadataItem(
                            fixtures.SP_ORGANIZATION_EN_ORGANIZATION_NAME, "en"
                        ),
                        SAMLLocalizedMetadataItem(
                            fixtures.SP_ORGANIZATION_ES_ORGANIZATION_NAME, "es"
                        ),
                    ],
                    [
                        SAMLLocalizedMetadataItem(
                            fixtures.SP_ORGANIZATION_EN_ORGANIZATION_DISPLAY_NAME, "en"
                        ),
                        SAMLLocalizedMetadataItem(
                            fixtures.SP_ORGANIZATION_ES_ORGANIZATION_DISPLAY_NAME, "es"
                        ),
                    ],
                    [
                        SAMLLocalizedMetadataItem(
                            fixtures.SP_ORGANIZATION_EN_ORGANIZATION_URL, "en"
                        ),
                        SAMLLocalizedMetadataItem(
                            fixtures.SP_ORGANIZATION_ES_ORGANIZATION_URL, "es"
                        ),
                    ],
                ),
                name_id_format=SAMLNameIDFormat.UNSPECIFIED.value,
                acs_service=SAMLService(fixtures.SP_ACS_URL, fixtures.SP_ACS_BINDING),
                authn_requests_signed=False,
                want_assertions_signed=False,
                certificate=fixtures.strip_certificate(fixtures.SIGNING_CERTIFICATE),
            ) ==
            parsing_result.provider)


class TestSAMLSubjectParser(object):
    @parameterized.expand(
        [
            (
                "name_id_and_attributes",
                SAMLNameIDFormat.TRANSIENT.value,
                fixtures.IDP_1_ENTITY_ID,
                fixtures.SP_ENTITY_ID,
                "12345",
                {SAMLAttributeType.eduPersonUniqueId.value: ["12345"]},
                SAMLSubject(
                    SAMLNameID(
                        SAMLNameIDFormat.TRANSIENT.value,
                        fixtures.IDP_1_ENTITY_ID,
                        fixtures.SP_ENTITY_ID,
                        "12345",
                    ),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                SAMLAttributeType.eduPersonUniqueId.name, ["12345"]
                            )
                        ]
                    ),
                ),
            ),
            (
                "edu_person_targeted_id_as_name_id",
                None,
                None,
                None,
                None,
                {
                    SAMLAttributeType.eduPersonTargetedID.value: [
                        {
                            "NameID": {
                                "Format": SAMLNameIDFormat.PERSISTENT.value,
                                "NameQualifier": fixtures.IDP_1_ENTITY_ID,
                                "value": "12345",
                            }
                        }
                    ]
                },
                SAMLSubject(
                    SAMLNameID(
                        SAMLNameIDFormat.PERSISTENT.value,
                        fixtures.IDP_1_ENTITY_ID,
                        None,
                        "12345",
                    ),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                SAMLAttributeType.eduPersonTargetedID.name, ["12345"]
                            )
                        ]
                    ),
                ),
            ),
            (
                "edu_person_targeted_id_as_name_id_and_other_attributes",
                None,
                None,
                None,
                None,
                {
                    SAMLAttributeType.eduPersonTargetedID.value: [
                        {
                            "NameID": {
                                "Format": SAMLNameIDFormat.PERSISTENT.value,
                                "NameQualifier": fixtures.IDP_1_ENTITY_ID,
                                "value": "12345",
                            }
                        }
                    ],
                    SAMLAttributeType.eduPersonPrincipalName.value: ["12345"],
                },
                SAMLSubject(
                    SAMLNameID(
                        SAMLNameIDFormat.PERSISTENT.value,
                        fixtures.IDP_1_ENTITY_ID,
                        None,
                        "12345",
                    ),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                SAMLAttributeType.eduPersonTargetedID.name, ["12345"]
                            ),
                            SAMLAttribute(
                                SAMLAttributeType.eduPersonPrincipalName.name, ["12345"]
                            ),
                        ]
                    ),
                ),
            ),
            (
                "edu_person_principal_name_as_name_id",
                None,
                None,
                None,
                None,
                {
                    SAMLAttributeType.eduPersonPrincipalName.value: [
                        {
                            "NameID": {
                                "Format": SAMLNameIDFormat.PERSISTENT.value,
                                "NameQualifier": fixtures.IDP_1_ENTITY_ID,
                                "value": "12345",
                            }
                        }
                    ]
                },
                SAMLSubject(
                    SAMLNameID(
                        SAMLNameIDFormat.PERSISTENT.value,
                        fixtures.IDP_1_ENTITY_ID,
                        None,
                        "12345",
                    ),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                SAMLAttributeType.eduPersonPrincipalName.name, ["12345"]
                            )
                        ]
                    ),
                ),
            ),
        ]
    )
    def test_parse(
        self,
        _,
        name_id_format,
        name_id_nq,
        name_id_spnq,
        name_id,
        attributes,
        expected_result,
    ):
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
        assert result == expected_result
