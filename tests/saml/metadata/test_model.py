from nose.tools import eq_
from parameterized import parameterized

from api.saml.metadata.model import (
    SAMLAttribute,
    SAMLAttributeStatement,
    SAMLAttributeType,
    SAMLNameID,
    SAMLNameIDFormat,
    SAMLSubject,
    SAMLSubjectUIDExtractor,
)


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
        eq_(True, SAMLAttributeType.uid.name in attribute_statement.attributes)
        eq_(
            attributes[0].values,
            attribute_statement.attributes[SAMLAttributeType.uid.name].values,
        )

        eq_(
            True,
            SAMLAttributeType.eduPersonTargetedID.name
            in attribute_statement.attributes,
        )
        eq_(
            attributes[1].values,
            attribute_statement.attributes[
                SAMLAttributeType.eduPersonTargetedID.name
            ].values,
        )


class TestSubjectUIDExtractor(object):
    @parameterized.expand(
        [
            ("subject_without_unique_id", SAMLSubject(None, None), None),
            (
                "subject_with_eduPersonTargetedID_attribute",
                SAMLSubject(
                    SAMLNameID(SAMLNameIDFormat.UNSPECIFIED, "", "", "12345"),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonTargetedID.name,
                                values=["12345"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonUniqueId.name,
                                values=["12345"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.uid.name, values=["12345"]
                            ),
                        ]
                    ),
                ),
                "12345",
            ),
            (
                "subject_with_eduPersonUniqueId_attribute",
                SAMLSubject(
                    SAMLNameID(SAMLNameIDFormat.UNSPECIFIED, "", "", "12345"),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonUniqueId.name,
                                values=["12345"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.uid.name, values=["12345"]
                            ),
                        ]
                    ),
                ),
                "12345",
            ),
            (
                "subject_with_uid_attribute",
                SAMLSubject(
                    SAMLNameID(SAMLNameIDFormat.UNSPECIFIED, "", "", "12345"),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.uid.name, values=["12345"]
                            )
                        ]
                    ),
                ),
                "12345",
            ),
            (
                "subject_with_name_id",
                SAMLSubject(
                    SAMLNameID(SAMLNameIDFormat.UNSPECIFIED, "", "", "12345"),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonPrincipalName.name,
                                values=["12345"],
                            )
                        ]
                    ),
                ),
                "12345",
            ),
        ]
    )
    def test(self, _, subject, expected_unique_id):
        # Arrange
        extractor = SAMLSubjectUIDExtractor()

        # Act
        unique_id = extractor.extract(subject)

        # Assert
        eq_(expected_unique_id, unique_id)
