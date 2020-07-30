from nose.tools import eq_
from parameterized import parameterized

from api.saml.metadata import Attribute, SAMLAttributes, AttributeStatement, SubjectUIDExtractor, Subject, NameID, \
    NameIDFormat


class AttributeStatementTest(object):
    def test_init_accepts_list_of_attributes(self):
        # Arrange
        attributes = [
            Attribute(SAMLAttributes.uid.name, [12345]),
            Attribute(SAMLAttributes.eduPersonTargetedID.name, [12345])
        ]

        # Act
        attribute_statement = AttributeStatement(attributes)

        # Assert
        assert SAMLAttributes.uid.name in attribute_statement.attributes
        eq_(attribute_statement.attributes[SAMLAttributes.uid.name].values, attributes[0].values)

        assert SAMLAttributes.eduPersonTargetedID.name in attribute_statement.attributes
        eq_(attribute_statement.attributes[SAMLAttributes.eduPersonTargetedID.name].values, attributes[1].values)


class SubjectUIDExtractorTest(object):
    @parameterized.expand([
        (
            'subject_without_unique_id',
            Subject(None, None),
            None
        ),
        (
            'subject_with_eduPersonTargetedID_attribute',
            Subject(
                NameID(NameIDFormat.UNSPECIFIED, '', '', '12345'),
                AttributeStatement([
                    Attribute(name=SAMLAttributes.eduPersonTargetedID.name, values=['12345']),
                    Attribute(name=SAMLAttributes.eduPersonUniqueId.name, values=['12345']),
                    Attribute(name=SAMLAttributes.uid.name, values=['12345'])
                ])
            ),
            '12345'
        ),
        (
            'subject_with_eduPersonUniqueId_attribute',
            Subject(
                NameID(NameIDFormat.UNSPECIFIED, '', '', '12345'),
                AttributeStatement([
                    Attribute(name=SAMLAttributes.eduPersonUniqueId.name, values=['12345']),
                    Attribute(name=SAMLAttributes.uid.name, values=['12345'])
                ])
            ),
            '12345'
        ),
        (
            'subject_with_uid_attribute',
            Subject(
                NameID(NameIDFormat.UNSPECIFIED, '', '', '12345'),
                AttributeStatement([
                    Attribute(name=SAMLAttributes.uid.name, values=['12345'])
                ])
            ),
            '12345'
        ),
        (
            'subject_with_name_id',
            Subject(
                NameID(NameIDFormat.UNSPECIFIED, '', '', '12345'),
                AttributeStatement([
                    Attribute(name=SAMLAttributes.eduPersonOrgUnitDN.name, values=['12345'])
                ])
            ),
            '12345'
        ),
    ])
    def test(self, name, subject, expected_unique_id):
        # Arrange
        extractor = SubjectUIDExtractor()

        # Act
        unique_id = extractor.extract(subject)

        # Assert
        eq_(unique_id, expected_unique_id)
