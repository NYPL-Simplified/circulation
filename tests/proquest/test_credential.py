import datetime
import json

from nose.tools import eq_
from parameterized import parameterized

from api.authenticator import BaseSAMLAuthenticationProvider
from api.proquest.credential import ProQuestCredentialManager, ProQuestCredentialType
from api.saml.metadata import (
    Attribute,
    AttributeStatement,
    SAMLAttributes,
    Subject,
    SubjectJSONEncoder,
)
from core.model import Credential, DataSource
from core.testing import DatabaseTest
from tests.saml import fixtures


class TestProQuestCredentialManager(DatabaseTest):
    def setup(self, mock_search=True):
        super(TestProQuestCredentialManager, self).setup(mock_search)

        self._data_source = DataSource.lookup(
            self._db, DataSource.PROQUEST, autocreate=True
        )

    def test_lookup_proquest_token_returns_none_if_token_missing(self):
        # Arrange
        credential_manager = ProQuestCredentialManager()
        patron = self._patron()

        # Act
        token = credential_manager.lookup_proquest_token(self._db, patron)

        # Assert
        eq_(None, token)

    def test_lookup_proquest_token_returns_token(self):
        # Arrange
        credential_manager = ProQuestCredentialManager()
        patron = self._patron()
        expected_token = "12345"

        Credential.temporary_token_create(
            self._db,
            self._data_source,
            ProQuestCredentialType.PROQUEST_JWT_TOKEN.value,
            patron,
            datetime.timedelta(hours=1),
            expected_token,
        )

        # Act
        token = credential_manager.lookup_proquest_token(self._db, patron)

        # Assert
        eq_(expected_token, token)

    def test_save_proquest_token_saves_token(self):
        # Arrange
        credential_manager = ProQuestCredentialManager()
        patron = self._patron()
        expected_token = "12345"

        # Act
        credential_manager.save_proquest_token(
            self._db, patron, datetime.timedelta(hours=1), expected_token
        )
        token = Credential.lookup_by_patron(
            self._db,
            self._data_source.name,
            ProQuestCredentialType.PROQUEST_JWT_TOKEN.value,
            patron,
        )

        # Assert
        eq_(expected_token, token.credential)

    @parameterized.expand(
        [
            ("when_there_is_no_token", None, None),
            (
                "when_subject_does_not_contain_affiliation_id_attributes",
                Subject(
                    None,
                    AttributeStatement(
                        [Attribute(SAMLAttributes.mail.name, [fixtures.MAIL])]
                    ),
                ),
                None,
            ),
            (
                "when_subject_contains_affiliation_id_attribute",
                Subject(
                    None,
                    AttributeStatement(
                        [
                            Attribute(
                                SAMLAttributes.eduPersonPrincipalName.name,
                                [fixtures.EDU_PERSON_PRINCIPAL_NAME],
                            ),
                        ]
                    ),
                ),
                fixtures.EDU_PERSON_PRINCIPAL_NAME,
            ),
            (
                "when_subject_contains_multiple_affiliation_id_attributes",
                Subject(
                    None,
                    AttributeStatement(
                        [
                            Attribute(
                                SAMLAttributes.eduPersonPrincipalName.name,
                                ["12345", fixtures.EDU_PERSON_PRINCIPAL_NAME],
                            ),
                        ]
                    ),
                ),
                "12345",
            ),
            (
                "with_custom_affiliation_attributes",
                Subject(
                    None,
                    AttributeStatement(
                        [
                            Attribute(
                                SAMLAttributes.mail.name,
                                ["12345@example.com"],
                            ),
                        ]
                    ),
                ),
                "12345@example.com",
                (SAMLAttributes.mail.name,),
            ),
        ]
    )
    def test_lookup_patron_affiliation_id(
        self, _, subject, expected_affiliation_id, affiliation_attributes=None
    ):
        # Arrange
        credential_manager = ProQuestCredentialManager()
        patron = self._patron()

        if subject:
            expected_token = json.dumps(subject, cls=SubjectJSONEncoder)

            data_source = DataSource.lookup(
                self._db,
                BaseSAMLAuthenticationProvider.TOKEN_DATA_SOURCE_NAME,
                autocreate=True,
            )
            Credential.temporary_token_create(
                self._db,
                data_source,
                BaseSAMLAuthenticationProvider.TOKEN_TYPE,
                patron,
                datetime.timedelta(hours=1),
                expected_token,
            )

        # Act
        if affiliation_attributes:
            token = credential_manager.lookup_patron_affiliation_id(
                self._db, patron, affiliation_attributes
            )
        else:
            token = credential_manager.lookup_patron_affiliation_id(self._db, patron)

        # Assert
        eq_(expected_affiliation_id, token)
