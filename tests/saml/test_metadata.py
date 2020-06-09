from nose.tools import eq_
from parameterized import parameterized

from api.saml.metadata import Attribute, SAMLAttributes, AttributeStatement, SubjectUIDExtractor, Subject, NameID, \
    NameIDFormat


class AttributeStatementTest(object):
    def test_init_accepts_list_of_attributes(self):
        # Arrange
        attributes = [
            Attribute(SAMLAttributes.uid.value, [12345]),
            Attribute(SAMLAttributes.eduPersonTargetedID.value, [12345])
        ]

        # Act
        attribute_statement = AttributeStatement(attributes)

        # Assert
        assert SAMLAttributes.uid.name in attribute_statement.attributes
        eq_(attribute_statement.attributes[SAMLAttributes.uid.name].values, attributes[0].values)

        assert SAMLAttributes.eduPersonTargetedID.name in attribute_statement.attributes
        eq_(attribute_statement.attributes[SAMLAttributes.eduPersonTargetedID.name].values, attributes[1].values)

    def test_init_accepts_dictionary_of_attributes(self):
        # Arrange
        attributes = {
            SAMLAttributes.uid.value: [12345],
            SAMLAttributes.eduPersonTargetedID.value: [12345]
        }

        # Act
        attribute_statement = AttributeStatement(attributes)

        # Assert
        assert SAMLAttributes.uid.name in attribute_statement.attributes
        eq_(
            attribute_statement.attributes[SAMLAttributes.uid.name].values,
            attributes[SAMLAttributes.uid.value])

        assert SAMLAttributes.eduPersonTargetedID.name in attribute_statement.attributes
        eq_(
            attribute_statement.attributes[SAMLAttributes.eduPersonTargetedID.name].values,
            attributes[SAMLAttributes.eduPersonTargetedID.value])


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
                AttributeStatement({
                    SAMLAttributes.eduPersonTargetedID.name: ['12345'],
                    SAMLAttributes.eduPersonUniqueId.name: ['12345'],
                    SAMLAttributes.uid.name: ['12345']
                })
            ),
            '12345'
        ),
        (
            'subject_with_eduPersonUniqueId_attribute',
            Subject(
                NameID(NameIDFormat.UNSPECIFIED, '', '', '12345'),
                AttributeStatement({
                    SAMLAttributes.eduPersonUniqueId.name: ['12345'],
                    SAMLAttributes.uid.name: ['12345']
                })
            ),
            '12345'
        ),
        (
            'subject_with_uid_attribute',
            Subject(
                NameID(NameIDFormat.UNSPECIFIED, '', '', '12345'),
                AttributeStatement({
                    SAMLAttributes.uid.name: ['12345']
                })
            ),
            '12345'
        ),
        (
            'subject_with_name_id',
            Subject(
                NameID(NameIDFormat.UNSPECIFIED, '', '', '12345'),
                AttributeStatement({
                    SAMLAttributes.eduPersonOrgUnitDN.name: ['12345']
                })
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
