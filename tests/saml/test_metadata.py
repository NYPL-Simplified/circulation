import json

from nose.tools import eq_
from parameterized import parameterized

from api.saml.metadata import Attribute, SAMLAttributes, AttributeStatement, SubjectUIDExtractor, Subject, NameID, \
    NameIDFormat, SubjectJSONDecoder, SubjectJSONEncoder
from tests.saml import fixtures
from tests.saml.fixtures import strip_json


class TestAttributeStatement(object):
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


class TestSubjectUIDExtractor(object):
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
                        Attribute(name=SAMLAttributes.eduPersonPrincipalName.name, values=['12345'])
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


class TestSubjectJSONEncoder(object):
    def test_encode(self):
        # Arrange
        subject = Subject(
            NameID(
                fixtures.NAME_FORMAT,
                fixtures.NAME_QUALIFIER,
                fixtures.SP_NAME_QUALIFIER,
                fixtures.NAME_ID
            ),
            AttributeStatement(
                [
                    Attribute(SAMLAttributes.mail.name, [fixtures.MAIL]),
                    Attribute(SAMLAttributes.givenName.name, [fixtures.GIVEN_NAME]),
                    Attribute(SAMLAttributes.surname.name, [fixtures.SURNAME]),
                    Attribute(SAMLAttributes.uid.name, [fixtures.UID]),
                    Attribute(SAMLAttributes.eduPersonPrincipalName.name, [fixtures.EDU_PERSON_PRINCIPAL_NAME])
                ]
            )
        )

        # Act
        subject_json = json.dumps(subject, cls=SubjectJSONEncoder)

        # Assert
        eq_(strip_json(fixtures.JSON_DOCUMENT_WITH_SAML_SUBJECT), subject_json)


class TestSubjectJSONDecoder(object):
    def test_decode(self):
        # Arrange
        expected_subject = Subject(
            NameID(
                fixtures.NAME_FORMAT,
                fixtures.NAME_QUALIFIER,
                fixtures.SP_NAME_QUALIFIER,
                fixtures.NAME_ID
            ),
            AttributeStatement(
                [
                    Attribute(SAMLAttributes.mail.name, [fixtures.MAIL]),
                    Attribute(SAMLAttributes.givenName.name, [fixtures.GIVEN_NAME]),
                    Attribute(SAMLAttributes.surname.name, [fixtures.SURNAME]),
                    Attribute(SAMLAttributes.uid.name, [fixtures.UID]),
                    Attribute(SAMLAttributes.eduPersonPrincipalName.name, [fixtures.EDU_PERSON_PRINCIPAL_NAME])
                ]
            )
        )

        # Act
        subject = json.loads(fixtures.JSON_DOCUMENT_WITH_SAML_SUBJECT, cls=SubjectJSONDecoder)

        # Assert
        eq_(expected_subject, subject)
