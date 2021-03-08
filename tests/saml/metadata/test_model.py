# coding=utf-8
from parameterized import parameterized

from api.saml.metadata.model import (
    SAMLAttribute,
    SAMLAttributeStatement,
    SAMLAttributeType,
    SAMLNameID,
    SAMLNameIDFormat,
    SAMLSubject,
    SAMLSubjectPatronIDExtractor,
)
from tests.saml import fixtures


class TestAttributeStatement(object):
    def test_init_accepts_list_of_attributes(self):
        # Arrange
        attributes = [
            SAMLAttribute(SAMLAttributeType.uid.name, [12345]),
            SAMLAttribute(SAMLAttributeType.eduPersonTargetedID.name, [12345]),
        ]

        # Act
        attribute_statement = SAMLAttributeStatement(attributes)

        # Assert
        assert True == (SAMLAttributeType.uid.name in attribute_statement.attributes)
        assert (
            attributes[0].values ==
            attribute_statement.attributes[SAMLAttributeType.uid.name].values)

        assert (
            True ==
            (SAMLAttributeType.eduPersonTargetedID.name
            in attribute_statement.attributes))
        assert (
            attributes[1].values ==
            attribute_statement.attributes[
                SAMLAttributeType.eduPersonTargetedID.name
            ].values)


class TestSAMLSubjectPatronIDExtractor(object):
    @parameterized.expand(
        [
            ("subject_without_patron_id", SAMLSubject(None, None), None),
            (
                "subject_with_eduPersonTargetedID_attribute",
                SAMLSubject(
                    SAMLNameID(SAMLNameIDFormat.UNSPECIFIED, "", "", "1"),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonTargetedID.name,
                                values=["2"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonUniqueId.name,
                                values=["3"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.uid.name, values=["4"]
                            ),
                        ]
                    ),
                ),
                "3",
            ),
            (
                "subject_with_eduPersonUniqueId_attribute",
                SAMLSubject(
                    SAMLNameID(SAMLNameIDFormat.UNSPECIFIED, "", "", "1"),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonUniqueId.name,
                                values=["2"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.uid.name, values=["3"]
                            ),
                        ]
                    ),
                ),
                "2",
            ),
            (
                "subject_with_uid_attribute",
                SAMLSubject(
                    SAMLNameID(SAMLNameIDFormat.UNSPECIFIED, "", "", "1"),
                    SAMLAttributeStatement(
                        [SAMLAttribute(name=SAMLAttributeType.uid.name, values=["2"])]
                    ),
                ),
                "2",
            ),
            (
                "subject_with_name_id",
                SAMLSubject(
                    SAMLNameID(SAMLNameIDFormat.UNSPECIFIED, "", "", "1"),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonPrincipalName.name,
                                values=["2"],
                            )
                        ]
                    ),
                ),
                "1",
            ),
            (
                "subject_with_switched_off_use_of_name_id",
                SAMLSubject(
                    SAMLNameID(SAMLNameIDFormat.UNSPECIFIED, "", "", "1"),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonPrincipalName.name,
                                values=["2"],
                            )
                        ]
                    ),
                ),
                None,
                False,
            ),
            (
                "patron_id_attributes_matching_attributes_in_subject",
                SAMLSubject(
                    SAMLNameID(SAMLNameIDFormat.UNSPECIFIED, "", "", "1"),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonTargetedID.name,
                                values=["2"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonUniqueId.name,
                                values=["3"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.uid.name, values=["4"]
                            ),
                        ]
                    ),
                ),
                "4",
                False,
                [SAMLAttributeType.uid.name],
            ),
            (
                "patron_id_attributes_matching_second_saml_attribute",
                SAMLSubject(
                    SAMLNameID(SAMLNameIDFormat.UNSPECIFIED, "", "", "1"),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonTargetedID.name,
                                values=[None],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonUniqueId.name,
                                values=["3"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.uid.name, values=["4"]
                            ),
                        ]
                    ),
                ),
                "4",
                True,
                [
                    SAMLAttributeType.eduPersonTargetedID.name,
                    SAMLAttributeType.uid.name,
                ],
            ),
            (
                "patron_id_attributes_not_matching_attributes_in_subject",
                SAMLSubject(
                    SAMLNameID(SAMLNameIDFormat.UNSPECIFIED, "", "", "1"),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonTargetedID.name,
                                values=["2"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonUniqueId.name,
                                values=["3"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.uid.name, values=["4"]
                            ),
                        ]
                    ),
                ),
                None,
                False,
                [SAMLAttributeType.givenName.name],
            ),
            (
                "patron_id_attributes_not_matching_attributes_in_subject_and_using_name_id_instead",
                SAMLSubject(
                    SAMLNameID(SAMLNameIDFormat.UNSPECIFIED, "", "", "1"),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonTargetedID.name,
                                values=["2"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonUniqueId.name,
                                values=["3"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.uid.name, values=["4"]
                            ),
                        ]
                    ),
                ),
                "1",
                True,
                [SAMLAttributeType.givenName.name],
            ),
            (
                "patron_id_regular_expression_matching_saml_subject",
                SAMLSubject(
                    SAMLNameID(SAMLNameIDFormat.UNSPECIFIED, "", "", "1"),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonPrincipalName.name,
                                values=["patron@university.org"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonUniqueId.name,
                                values=["3"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.uid.name, values=["4"]
                            ),
                        ]
                    ),
                ),
                "patron",
                False,
                [
                    SAMLAttributeType.eduPersonPrincipalName.name,
                    SAMLAttributeType.mail.name,
                ],
                fixtures.PATRON_ID_REGULAR_EXPRESSION_ORG,
            ),
            (
                "patron_id_regular_expression_matching_second_saml_attribute",
                SAMLSubject(
                    SAMLNameID(SAMLNameIDFormat.UNSPECIFIED, "", "", "1"),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonPrincipalName.name,
                                values=["patron@university.org"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonUniqueId.name,
                                values=["3"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.uid.name, values=["4"]
                            ),
                        ]
                    ),
                ),
                "patron",
                False,
                [
                    SAMLAttributeType.eduPersonUniqueId.name,
                    SAMLAttributeType.eduPersonPrincipalName.name,
                    SAMLAttributeType.mail.name,
                ],
                fixtures.PATRON_ID_REGULAR_EXPRESSION_ORG,
            ),
            (
                "unicode_patron_id_regular_expression_matching_saml_subject",
                SAMLSubject(
                    SAMLNameID(SAMLNameIDFormat.UNSPECIFIED, "", "", "1"),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonPrincipalName.name,
                                values=[u"pątron@university.org"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonUniqueId.name,
                                values=["3"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.uid.name, values=["4"]
                            ),
                        ]
                    ),
                ),
                u"pątron",
                False,
                [
                    SAMLAttributeType.eduPersonPrincipalName.name,
                    SAMLAttributeType.mail.name,
                ],
                fixtures.PATRON_ID_REGULAR_EXPRESSION_ORG,
            ),
            (
                "patron_id_regular_expression_not_matching_saml_subject",
                SAMLSubject(
                    SAMLNameID(SAMLNameIDFormat.UNSPECIFIED, "", "", "1"),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonPrincipalName.name,
                                values=["patron@university.org"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonUniqueId.name,
                                values=["3"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.uid.name, values=["4"]
                            ),
                        ]
                    ),
                ),
                None,
                False,
                [
                    SAMLAttributeType.eduPersonPrincipalName.name,
                    SAMLAttributeType.mail.name,
                ],
                fixtures.PATRON_ID_REGULAR_EXPRESSION_COM,
            ),
            (
                "patron_id_regular_expression_not_matching_saml_attributes_but_matching_name_id",
                SAMLSubject(
                    SAMLNameID(
                        SAMLNameIDFormat.UNSPECIFIED, "", "", "patron@university.com"
                    ),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonPrincipalName.name,
                                values=["patron@university.org"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonUniqueId.name,
                                values=["3"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.uid.name, values=["4"]
                            ),
                        ]
                    ),
                ),
                "patron",
                True,
                [
                    SAMLAttributeType.eduPersonPrincipalName.name,
                    SAMLAttributeType.mail.name,
                ],
                fixtures.PATRON_ID_REGULAR_EXPRESSION_COM,
            ),
        ]
    )
    def test(
        self,
        _,
        subject,
        expected_patron_id,
        use_name_id=True,
        patron_id_attributes=None,
        patron_id_regular_expression=None,
    ):
        """Make sure that SAMLSubjectUIDExtractor correctly extracts a unique patron ID from the SAML subject.

        :param _: Name of the test case
        :type _: str

        :param expected_patron_id: Expected patron ID
        :type expected_patron_id: str

        :param use_name_id: Boolean value indicating whether SAMLSubjectUIDExtractor
            is allowed to search for patron IDs in NameID
        :type use_name_id: bool

        :param patron_id_attributes: List of SAML attributes used by SAMLSubjectUIDExtractor to search for a patron ID
        :type patron_id_attributes: List[SAMLAttributeType]

        :param patron_id_regular_expression: Regular expression used to extract a patron ID from SAML attributes
        :type patron_id_regular_expression: str
        """
        # Arrange
        extractor = SAMLSubjectPatronIDExtractor(
            use_name_id, patron_id_attributes, patron_id_regular_expression
        )

        # Act
        patron_id = extractor.extract(subject)

        # Assert
        assert expected_patron_id == patron_id
