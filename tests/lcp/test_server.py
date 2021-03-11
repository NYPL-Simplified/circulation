import datetime
import json
import os
import urlparse

import requests_mock
from mock import create_autospec, MagicMock
from parameterized import parameterized

from api.lcp import utils
from api.lcp.encrypt import LCPEncryptionResult
from api.lcp.hash import HasherFactory
from api.lcp.server import LCPServer, LCPServerConfiguration
from core.lcp.credential import LCPCredentialFactory
from core.model.configuration import HasExternalIntegration, ConfigurationStorage, ConfigurationFactory, \
    ExternalIntegration
from tests.lcp import fixtures
from tests.lcp.database_test import DatabaseTest


class TestLCPServer(DatabaseTest):
    def setup_method(self):
        super(TestLCPServer, self).setup_method()

        self._lcp_collection = self._collection(protocol=ExternalIntegration.LCP)
        self._integration = self._lcp_collection.external_integration
        integration_owner = create_autospec(spec=HasExternalIntegration)
        integration_owner.external_integration = MagicMock(return_value=self._integration)
        self._configuration_storage = ConfigurationStorage(integration_owner)
        self._configuration_factory = ConfigurationFactory()
        self._hasher_factory = HasherFactory()
        self._credential_factory = LCPCredentialFactory()
        self._lcp_server = LCPServer(
            self._configuration_storage, self._configuration_factory, self._hasher_factory, self._credential_factory
        )

    @parameterized.expand(
        [
            ('empty_input_directory', ''),
            ('non_empty_input_directory', '/tmp/encrypted_books')
        ]
    )
    def test_add_content(self, _, input_directory):
        # Arrange
        lcp_server = LCPServer(
            self._configuration_storage, self._configuration_factory, self._hasher_factory, self._credential_factory)
        encrypted_content = LCPEncryptionResult(
            content_id=fixtures.CONTENT_ID,
            content_encryption_key='12345',
            protected_content_location='/opt/readium/files/encrypted',
            protected_content_disposition='encrypted_book',
            protected_content_type='application/epub+zip',
            protected_content_length=12345,
            protected_content_sha256='12345'
        )
        expected_protected_content_disposition = os.path.join(
            input_directory, encrypted_content.protected_content_disposition)

        with self._configuration_factory.create(
                self._configuration_storage, self._db, LCPServerConfiguration) as configuration:
            configuration.lcpserver_url = fixtures.LCPSERVER_URL
            configuration.lcpserver_user = fixtures.LCPSERVER_USER
            configuration.lcpserver_password = fixtures.LCPSERVER_PASSWORD
            configuration.lcpserver_input_directory = input_directory
            configuration.provider_name = fixtures.PROVIDER_NAME
            configuration.passphrase_hint = fixtures.TEXT_HINT
            configuration.encryption_algorithm = LCPServerConfiguration.DEFAULT_ENCRYPTION_ALGORITHM

            with requests_mock.Mocker() as request_mock:
                url = urlparse.urljoin(fixtures.LCPSERVER_URL, '/contents/{0}'.format(fixtures.CONTENT_ID))
                request_mock.put(url)

                # Act
                lcp_server.add_content(self._db, encrypted_content)

                # Assert
                assert request_mock.called == True

                json_request = json.loads(request_mock.last_request.text)
                assert json_request['content-id'] == encrypted_content.content_id
                assert json_request['content-encryption-key'] == encrypted_content.content_encryption_key
                assert json_request['protected-content-location'] == expected_protected_content_disposition
                assert json_request['protected-content-disposition'] == encrypted_content.protected_content_disposition
                assert json_request['protected-content-type'] == encrypted_content.protected_content_type
                assert json_request['protected-content-length'] == encrypted_content.protected_content_length
                assert json_request['protected-content-sha256'] == encrypted_content.protected_content_sha256

    @parameterized.expand([
        ('none_rights', None, None, None, None),
        (
                'license_start',
                datetime.datetime(2020, 01, 01, 00, 00, 00),
                None,
                None,
                None
        ),
        (
                'license_end',
                None,
                datetime.datetime(2020, 12, 31, 23, 59, 59),
                None,
                None
        ),
        (
                'max_printable_pages',
                None,
                None,
                10,
                None
        ),
        (
                'max_printable_pages_empty_max_copiable_pages',
                None,
                None,
                10,
                ''
        ),
        (
                'empty_max_printable_pages',
                None,
                None,
                '',
                None
        ),
        (
                'max_copiable_pages',
                None,
                None,
                None,
                1024
        ),
        (
                'empty_max_printable_pages_max_copiable_pages',
                None,
                None,
                '',
                1024
        ),
        (
                'empty_max_copiable_pages',
                None,
                None,
                None,
                ''
        ),
        (
                'dates',
                datetime.datetime(2020, 01, 01, 00, 00, 00),
                datetime.datetime(2020, 12, 31, 23, 59, 59),
                None,
                None
        ),
        (
                'full_rights',
                datetime.datetime(2020, 01, 01, 00, 00, 00),
                datetime.datetime(2020, 12, 31, 23, 59, 59),
                10,
                1024
        ),
    ])
    def test_generate_license(self, _, license_start, license_end, max_printable_pages, max_copiable_pages):
        # Arrange
        patron = self._patron()
        expected_patron_id = '52a190d1-cd69-4794-9d7a-1ec50392697f'
        expected_patron_passphrase = '52a190d1-cd69-4794-9d7a-1ec50392697a'
        expected_patron_key = self._hasher_factory \
            .create(LCPServerConfiguration.DEFAULT_ENCRYPTION_ALGORITHM) \
            .hash(expected_patron_passphrase)

        with self._configuration_factory.create(
                self._configuration_storage, self._db, LCPServerConfiguration) as configuration:
            configuration.lcpserver_url = fixtures.LCPSERVER_URL
            configuration.lcpserver_user = fixtures.LCPSERVER_USER
            configuration.lcpserver_password = fixtures.LCPSERVER_PASSWORD
            configuration.provider_name = fixtures.PROVIDER_NAME
            configuration.passphrase_hint = fixtures.TEXT_HINT
            configuration.encryption_algorithm = LCPServerConfiguration.DEFAULT_ENCRYPTION_ALGORITHM
            configuration.max_printable_pages = max_printable_pages
            configuration.max_copiable_pages = max_copiable_pages

            self._credential_factory.get_patron_id = MagicMock(return_value=expected_patron_id)
            self._credential_factory.get_patron_passphrase = MagicMock(return_value=expected_patron_passphrase)

            with requests_mock.Mocker() as request_mock:
                url = urlparse.urljoin(fixtures.LCPSERVER_URL, '/contents/{0}/license'.format(fixtures.CONTENT_ID))
                request_mock.post(url, json=fixtures.LCPSERVER_LICENSE)

                # Act
                license = self._lcp_server.generate_license(
                    self._db, fixtures.CONTENT_ID, patron, license_start, license_end)

                # Assert
                assert request_mock.called == True
                assert license == fixtures.LCPSERVER_LICENSE

                json_request = json.loads(request_mock.last_request.text)
                assert json_request['provider'] == fixtures.PROVIDER_NAME
                assert json_request['user']['id'] == expected_patron_id
                assert json_request['encryption']['user_key']['text_hint'] == fixtures.TEXT_HINT
                assert json_request['encryption']['user_key']['hex_value'] == expected_patron_key

                if license_start is not None:
                    assert json_request['rights']['start'] == utils.format_datetime(license_start)
                if license_end is not None:
                    assert json_request['rights']['end'] == utils.format_datetime(license_end)
                if max_printable_pages is not None and max_printable_pages != '':
                    assert json_request['rights']['print'] == max_printable_pages
                if max_copiable_pages is not None and max_copiable_pages != '':
                    assert json_request['rights']['copy'] == max_copiable_pages

                all_rights_fields_are_empty = all(
                    map(
                        lambda rights_field: rights_field is None or rights_field == '',
                        [license_start, license_end, max_printable_pages, max_copiable_pages]
                    )
                )
                if all_rights_fields_are_empty:
                    assert ('rights' in json_request) == False

                self._credential_factory.get_patron_id.assert_called_once_with(self._db, patron)
                self._credential_factory.get_patron_passphrase.assert_called_once_with(self._db, patron)
