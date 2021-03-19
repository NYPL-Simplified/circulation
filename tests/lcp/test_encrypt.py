import pytest
from mock import patch, create_autospec, MagicMock
from parameterized import parameterized
from pyfakefs.fake_filesystem_unittest import Patcher

from api.lcp.encrypt import LCPEncryptor, LCPEncryptionException, LCPEncryptionConfiguration, LCPEncryptionResult
from core.model import Identifier
from core.model.configuration import HasExternalIntegration, ConfigurationStorage, ConfigurationFactory
from tests.lcp import fixtures
from tests.lcp.database_test import DatabaseTest


class TestLCPEncryptor(DatabaseTest):
    @parameterized.expand([
        (
            'non_existing_directory',
            fixtures.NOT_EXISTING_BOOK_FILE_PATH,
            fixtures.LCPENCRYPT_NOT_EXISTING_DIRECTORY_RESULT,
            None,
            LCPEncryptionException(fixtures.LCPENCRYPT_NOT_EXISTING_DIRECTORY_RESULT.strip()),
            False
        ),
        (
            'failed_encryption',
            fixtures.NOT_EXISTING_BOOK_FILE_PATH,
            fixtures.LCPENCRYPT_FAILED_ENCRYPTION_RESULT,
            None,
            LCPEncryptionException('Encryption failed')
        ),
        (
            'successful_encryption',
            fixtures.EXISTING_BOOK_FILE_PATH,
            fixtures.LCPENCRYPT_SUCCESSFUL_ENCRYPTION_RESULT,
            LCPEncryptionResult(
                content_id=fixtures.BOOK_IDENTIFIER,
                content_encryption_key=fixtures.CONTENT_ENCRYPTION_KEY,
                protected_content_location=fixtures.PROTECTED_CONTENT_LOCATION,
                protected_content_disposition=fixtures.PROTECTED_CONTENT_DISPOSITION,
                protected_content_type=fixtures.PROTECTED_CONTENT_TYPE,
                protected_content_length=fixtures.PROTECTED_CONTENT_LENGTH,
                protected_content_sha256=fixtures.PROTECTED_CONTENT_SHA256
            )
        ),
        (
            'failed_lcp_server_notification',
            fixtures.EXISTING_BOOK_FILE_PATH,
            fixtures.LCPENCRYPT_FAILED_LCPSERVER_NOTIFICATION,
            None,
            LCPEncryptionException(fixtures.LCPENCRYPT_FAILED_LCPSERVER_NOTIFICATION.strip())
        ),
        (
            'successful_lcp_server_notification',
            fixtures.EXISTING_BOOK_FILE_PATH,
            fixtures.LCPENCRYPT_SUCCESSFUL_NOTIFICATION_RESULT,
            LCPEncryptionResult(
                content_id=fixtures.BOOK_IDENTIFIER,
                content_encryption_key=fixtures.CONTENT_ENCRYPTION_KEY,
                protected_content_location=fixtures.PROTECTED_CONTENT_LOCATION,
                protected_content_disposition=fixtures.PROTECTED_CONTENT_DISPOSITION,
                protected_content_type=fixtures.PROTECTED_CONTENT_TYPE,
                protected_content_length=fixtures.PROTECTED_CONTENT_LENGTH,
                protected_content_sha256=fixtures.PROTECTED_CONTENT_SHA256
            )
        ),
    ])
    def test_local_lcpencrypt(
            self,
            _,
            file_path,
            lcpencrypt_output,
            expected_result,
            expected_exception=None,
            create_file=True):
        # Arrange
        integration_owner = create_autospec(spec=HasExternalIntegration)
        integration_owner.external_integration = MagicMock(return_value=self._integration)
        configuration_storage = ConfigurationStorage(integration_owner)
        configuration_factory = ConfigurationFactory()
        encryptor = LCPEncryptor(configuration_storage, configuration_factory)
        identifier = Identifier(identifier=fixtures.BOOK_IDENTIFIER)

        with configuration_factory.create(configuration_storage, self._db, LCPEncryptionConfiguration) as configuration:
            configuration.lcpencrypt_location = LCPEncryptionConfiguration.DEFAULT_LCPENCRYPT_LOCATION

            with Patcher() as patcher:
                patcher.fs.create_file(LCPEncryptionConfiguration.DEFAULT_LCPENCRYPT_LOCATION)

                if create_file:
                    patcher.fs.create_file(file_path)

                with patch('subprocess.check_output') as subprocess_check_output_mock:
                    subprocess_check_output_mock.return_value = lcpencrypt_output

                    if expected_exception:
                        with pytest.raises(expected_exception.__class__) as exception_metadata:
                            encryptor.encrypt(self._db, file_path, identifier.identifier)

                        # Assert
                        assert exception_metadata.value == expected_exception
                    else:
                        # Assert
                        result = encryptor.encrypt(self._db, file_path, identifier.identifier)
                        assert result == expected_result
