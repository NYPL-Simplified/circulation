from mock import patch
from nose.tools import eq_, assert_raises
from parameterized import parameterized

from ...testing import DatabaseTest
from ...lcp.credential import LCPCredentialFactory, LCPCredentialType
from ...lcp.exceptions import LCPError
from ...model import Credential, DataSource


class TestCredentialFactory(DatabaseTest):
    def setup_method(self, mock_search=True):
        super(TestCredentialFactory, self).setup_method(mock_search)

        self._factory = LCPCredentialFactory()
        self._patron = self._patron()
        self._data_source = DataSource.lookup(self._db, DataSource.INTERNAL_PROCESSING, autocreate=True)

    @parameterized.expand([
        (
                'get_patron_id',
                LCPCredentialType.PATRON_ID.value,
                'get_patron_id',
                '52a190d1-cd69-4794-9d7a-1ec50392697f'
        ),
        (
                'get_patron_passphrase',
                LCPCredentialType.LCP_PASSPHRASE.value,
                'get_patron_passphrase',
                '52a190d1-cd69-4794-9d7a-1ec50392697f'
        )
    ])
    def test_getter(self, _, credential_type, method_name, expected_result):
        # Arrange
        credential = Credential(credential=expected_result)

        with patch.object(Credential, 'persistent_token_create') as persistent_token_create_mock:
            persistent_token_create_mock.return_value = (credential, True)

            method = getattr(self._factory, method_name)

            # Act
            result = method(self._db, self._patron)

            # Assert
            eq_(result, expected_result)
            persistent_token_create_mock.assert_called_once_with(
                self._db, self._data_source, credential_type, self._patron, None)

    def test_get_hashed_passphrase_raises_exception_when_there_is_no_passphrase(self):
        # Act, assert
        with assert_raises(LCPError):
            self._factory.get_hashed_passphrase(self._db, self._patron)

    def test_get_hashed_passphrase_returns_existing_hashed_passphrase(self):
        # Arrange
        expected_result = '12345'

        # Act
        self._factory.set_hashed_passphrase(self._db, self._patron, expected_result)
        result = self._factory.get_hashed_passphrase(self._db, self._patron)

        # Assert
        eq_(result, expected_result)
