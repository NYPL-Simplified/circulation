import datetime
import json

from freezegun import freeze_time
from mock import create_autospec, MagicMock, patch
from nose.tools import eq_

from api.lcp.collection import LCPAPI, LCPFulfilmentInfo
from api.lcp.encrypt import LCPEncryptionConfiguration
from api.lcp.server import LCPServerConfiguration, LCPServer
from core.model import ExternalIntegration, DataSource
from core.model.configuration import HasExternalIntegration, ConfigurationStorage, ConfigurationAttribute, \
    ConfigurationFactory
from tests.lcp import fixtures
from tests.lcp.database_test import DatabaseTest


class TestLCPAPI(DatabaseTest):
    def setup(self, mock_search=True):
        super(TestLCPAPI, self).setup()

        self._lcp_collection = self._collection(protocol=ExternalIntegration.LCP)
        self._integration = self._lcp_collection.external_integration
        integration_association = create_autospec(spec=HasExternalIntegration)
        integration_association.external_integration = MagicMock(return_value=self._integration)
        self._configuration_storage = ConfigurationStorage(integration_association)
        self._configuration_factory = ConfigurationFactory()

    def test_settings(self):
        # Assert
        eq_(len(LCPAPI.SETTINGS), 12)

        # lcpserver_url
        eq_(
            LCPAPI.SETTINGS[0][ConfigurationAttribute.KEY.value],
            LCPServerConfiguration.lcpserver_url.key
        )
        eq_(
            LCPAPI.SETTINGS[0][ConfigurationAttribute.LABEL.value],
            LCPServerConfiguration.lcpserver_url.label
        )
        eq_(
            LCPAPI.SETTINGS[0][ConfigurationAttribute.DESCRIPTION.value],
            LCPServerConfiguration.lcpserver_url.description
        )
        eq_(
            LCPAPI.SETTINGS[0][ConfigurationAttribute.TYPE.value],
            None
        )
        eq_(
            LCPAPI.SETTINGS[0][ConfigurationAttribute.REQUIRED.value],
            LCPServerConfiguration.lcpserver_url.required
        )
        eq_(
            LCPAPI.SETTINGS[0][ConfigurationAttribute.DEFAULT.value],
            LCPServerConfiguration.lcpserver_url.default
        )
        eq_(
            LCPAPI.SETTINGS[0][ConfigurationAttribute.CATEGORY.value],
            LCPServerConfiguration.lcpserver_url.category
        )

        # lcpserver_user
        eq_(
            LCPAPI.SETTINGS[1][ConfigurationAttribute.KEY.value],
            LCPServerConfiguration.lcpserver_user.key
        )
        eq_(
            LCPAPI.SETTINGS[1][ConfigurationAttribute.LABEL.value],
            LCPServerConfiguration.lcpserver_user.label
        )
        eq_(
            LCPAPI.SETTINGS[1][ConfigurationAttribute.DESCRIPTION.value],
            LCPServerConfiguration.lcpserver_user.description
        )
        eq_(
            LCPAPI.SETTINGS[1][ConfigurationAttribute.TYPE.value],
            None
        )
        eq_(
            LCPAPI.SETTINGS[1][ConfigurationAttribute.REQUIRED.value],
            LCPServerConfiguration.lcpserver_user.required
        )
        eq_(
            LCPAPI.SETTINGS[1][ConfigurationAttribute.DEFAULT.value],
            LCPServerConfiguration.lcpserver_user.default
        )
        eq_(
            LCPAPI.SETTINGS[1][ConfigurationAttribute.CATEGORY.value],
            LCPServerConfiguration.lcpserver_user.category
        )

        # lcpserver_password
        eq_(
            LCPAPI.SETTINGS[2][ConfigurationAttribute.KEY.value],
            LCPServerConfiguration.lcpserver_password.key
        )
        eq_(
            LCPAPI.SETTINGS[2][ConfigurationAttribute.LABEL.value],
            LCPServerConfiguration.lcpserver_password.label
        )
        eq_(
            LCPAPI.SETTINGS[2][ConfigurationAttribute.DESCRIPTION.value],
            LCPServerConfiguration.lcpserver_password.description
        )
        eq_(
            LCPAPI.SETTINGS[2][ConfigurationAttribute.TYPE.value],
            None
        )
        eq_(
            LCPAPI.SETTINGS[2][ConfigurationAttribute.REQUIRED.value],
            LCPServerConfiguration.lcpserver_password.required
        )
        eq_(
            LCPAPI.SETTINGS[2][ConfigurationAttribute.DEFAULT.value],
            LCPServerConfiguration.lcpserver_password.default
        )
        eq_(
            LCPAPI.SETTINGS[2][ConfigurationAttribute.CATEGORY.value],
            LCPServerConfiguration.lcpserver_password.category
        )

        # lcpserver_input_directory
        eq_(
            LCPAPI.SETTINGS[3][ConfigurationAttribute.KEY.value],
            LCPServerConfiguration.lcpserver_input_directory.key
        )
        eq_(
            LCPAPI.SETTINGS[3][ConfigurationAttribute.LABEL.value],
            LCPServerConfiguration.lcpserver_input_directory.label
        )
        eq_(
            LCPAPI.SETTINGS[3][ConfigurationAttribute.DESCRIPTION.value],
            LCPServerConfiguration.lcpserver_input_directory.description
        )
        eq_(
            LCPAPI.SETTINGS[3][ConfigurationAttribute.TYPE.value],
            None
        )
        eq_(
            LCPAPI.SETTINGS[3][ConfigurationAttribute.REQUIRED.value],
            LCPServerConfiguration.lcpserver_input_directory.required
        )
        eq_(
            LCPAPI.SETTINGS[3][ConfigurationAttribute.DEFAULT.value],
            LCPServerConfiguration.lcpserver_input_directory.default
        )
        eq_(
            LCPAPI.SETTINGS[3][ConfigurationAttribute.CATEGORY.value],
            LCPServerConfiguration.lcpserver_input_directory.category
        )

        # lcpserver_page_size
        eq_(
            LCPAPI.SETTINGS[4][ConfigurationAttribute.KEY.value],
            LCPServerConfiguration.lcpserver_page_size.key
        )
        eq_(
            LCPAPI.SETTINGS[4][ConfigurationAttribute.LABEL.value],
            LCPServerConfiguration.lcpserver_page_size.label
        )
        eq_(
            LCPAPI.SETTINGS[4][ConfigurationAttribute.DESCRIPTION.value],
            LCPServerConfiguration.lcpserver_page_size.description
        )
        eq_(
            LCPAPI.SETTINGS[4][ConfigurationAttribute.TYPE.value],
            'number'
        )
        eq_(
            LCPAPI.SETTINGS[4][ConfigurationAttribute.REQUIRED.value],
            LCPServerConfiguration.lcpserver_page_size.required
        )
        eq_(
            LCPAPI.SETTINGS[4][ConfigurationAttribute.DEFAULT.value],
            LCPServerConfiguration.lcpserver_page_size.default
        )
        eq_(
            LCPAPI.SETTINGS[4][ConfigurationAttribute.CATEGORY.value],
            LCPServerConfiguration.lcpserver_page_size.category
        )

        # provider_name
        eq_(
            LCPAPI.SETTINGS[5][ConfigurationAttribute.KEY.value],
            LCPServerConfiguration.provider_name.key
        )
        eq_(
            LCPAPI.SETTINGS[5][ConfigurationAttribute.LABEL.value],
            LCPServerConfiguration.provider_name.label
        )
        eq_(
            LCPAPI.SETTINGS[5][ConfigurationAttribute.DESCRIPTION.value],
            LCPServerConfiguration.provider_name.description
        )
        eq_(
            LCPAPI.SETTINGS[5][ConfigurationAttribute.TYPE.value],
            None
        )
        eq_(
            LCPAPI.SETTINGS[5][ConfigurationAttribute.REQUIRED.value],
            LCPServerConfiguration.provider_name.required
        )
        eq_(
            LCPAPI.SETTINGS[5][ConfigurationAttribute.DEFAULT.value],
            LCPServerConfiguration.provider_name.default
        )
        eq_(
            LCPAPI.SETTINGS[5][ConfigurationAttribute.CATEGORY.value],
            LCPServerConfiguration.provider_name.category
        )

        # passphrase_hint
        eq_(
            LCPAPI.SETTINGS[6][ConfigurationAttribute.KEY.value],
            LCPServerConfiguration.passphrase_hint.key
        )
        eq_(
            LCPAPI.SETTINGS[6][ConfigurationAttribute.LABEL.value],
            LCPServerConfiguration.passphrase_hint.label
        )
        eq_(
            LCPAPI.SETTINGS[6][ConfigurationAttribute.DESCRIPTION.value],
            LCPServerConfiguration.passphrase_hint.description
        )
        eq_(
            LCPAPI.SETTINGS[6][ConfigurationAttribute.TYPE.value],
            None
        )
        eq_(
            LCPAPI.SETTINGS[6][ConfigurationAttribute.REQUIRED.value],
            LCPServerConfiguration.passphrase_hint.required
        )
        eq_(
            LCPAPI.SETTINGS[6][ConfigurationAttribute.DEFAULT.value],
            LCPServerConfiguration.passphrase_hint.default
        )
        eq_(
            LCPAPI.SETTINGS[6][ConfigurationAttribute.CATEGORY.value],
            LCPServerConfiguration.passphrase_hint.category
        )

        # encryption_algorithm
        eq_(
            LCPAPI.SETTINGS[7][ConfigurationAttribute.KEY.value],
            LCPServerConfiguration.encryption_algorithm.key
        )
        eq_(
            LCPAPI.SETTINGS[7][ConfigurationAttribute.LABEL.value],
            LCPServerConfiguration.encryption_algorithm.label
        )
        eq_(
            LCPAPI.SETTINGS[7][ConfigurationAttribute.DESCRIPTION.value],
            LCPServerConfiguration.encryption_algorithm.description
        )
        eq_(
            LCPAPI.SETTINGS[7][ConfigurationAttribute.TYPE.value],
            LCPServerConfiguration.encryption_algorithm.type.value
        )
        eq_(
            LCPAPI.SETTINGS[7][ConfigurationAttribute.REQUIRED.value],
            LCPServerConfiguration.encryption_algorithm.required
        )
        eq_(
            LCPAPI.SETTINGS[7][ConfigurationAttribute.DEFAULT.value],
            LCPServerConfiguration.encryption_algorithm.default
        )
        eq_(
            LCPAPI.SETTINGS[7][ConfigurationAttribute.CATEGORY.value],
            LCPServerConfiguration.encryption_algorithm.category
        )

        # max_printable_pages
        eq_(
            LCPAPI.SETTINGS[8][ConfigurationAttribute.KEY.value],
            LCPServerConfiguration.max_printable_pages.key
        )
        eq_(
            LCPAPI.SETTINGS[8][ConfigurationAttribute.LABEL.value],
            LCPServerConfiguration.max_printable_pages.label
        )
        eq_(
            LCPAPI.SETTINGS[8][ConfigurationAttribute.DESCRIPTION.value],
            LCPServerConfiguration.max_printable_pages.description
        )
        eq_(
            LCPAPI.SETTINGS[8][ConfigurationAttribute.TYPE.value],
            'number'
        )
        eq_(
            LCPAPI.SETTINGS[8][ConfigurationAttribute.REQUIRED.value],
            LCPServerConfiguration.max_printable_pages.required
        )
        eq_(
            LCPAPI.SETTINGS[8][ConfigurationAttribute.DEFAULT.value],
            LCPServerConfiguration.max_printable_pages.default
        )
        eq_(
            LCPAPI.SETTINGS[8][ConfigurationAttribute.CATEGORY.value],
            LCPServerConfiguration.max_printable_pages.category
        )

        # max_copiable_pages
        eq_(
            LCPAPI.SETTINGS[9][ConfigurationAttribute.KEY.value],
            LCPServerConfiguration.max_copiable_pages.key
        )
        eq_(
            LCPAPI.SETTINGS[9][ConfigurationAttribute.LABEL.value],
            LCPServerConfiguration.max_copiable_pages.label
        )
        eq_(
            LCPAPI.SETTINGS[9][ConfigurationAttribute.DESCRIPTION.value],
            LCPServerConfiguration.max_copiable_pages.description
        )
        eq_(
            LCPAPI.SETTINGS[9][ConfigurationAttribute.TYPE.value],
            'number'
        )
        eq_(
            LCPAPI.SETTINGS[9][ConfigurationAttribute.REQUIRED.value],
            LCPServerConfiguration.max_copiable_pages.required
        )
        eq_(
            LCPAPI.SETTINGS[9][ConfigurationAttribute.DEFAULT.value],
            LCPServerConfiguration.max_copiable_pages.default
        )
        eq_(
            LCPAPI.SETTINGS[9][ConfigurationAttribute.CATEGORY.value],
            LCPServerConfiguration.max_copiable_pages.category
        )

        # lcpencrypt_location
        eq_(
            LCPAPI.SETTINGS[10][ConfigurationAttribute.KEY.value],
            LCPEncryptionConfiguration.lcpencrypt_location.key
        )
        eq_(
            LCPAPI.SETTINGS[10][ConfigurationAttribute.LABEL.value],
            LCPEncryptionConfiguration.lcpencrypt_location.label
        )
        eq_(
            LCPAPI.SETTINGS[10][ConfigurationAttribute.DESCRIPTION.value],
            LCPEncryptionConfiguration.lcpencrypt_location.description
        )
        eq_(
            LCPAPI.SETTINGS[10][ConfigurationAttribute.TYPE.value],
            None
        )
        eq_(
            LCPAPI.SETTINGS[10][ConfigurationAttribute.REQUIRED.value],
            LCPEncryptionConfiguration.lcpencrypt_location.required
        )
        eq_(
            LCPAPI.SETTINGS[10][ConfigurationAttribute.DEFAULT.value],
            LCPEncryptionConfiguration.lcpencrypt_location.default
        )
        eq_(
            LCPAPI.SETTINGS[10][ConfigurationAttribute.CATEGORY.value],
            LCPEncryptionConfiguration.lcpencrypt_location.category
        )

        # lcpencrypt_output_directory
        eq_(
            LCPAPI.SETTINGS[11][ConfigurationAttribute.KEY.value],
            LCPEncryptionConfiguration.lcpencrypt_output_directory.key
        )
        eq_(
            LCPAPI.SETTINGS[11][ConfigurationAttribute.LABEL.value],
            LCPEncryptionConfiguration.lcpencrypt_output_directory.label
        )
        eq_(
            LCPAPI.SETTINGS[11][ConfigurationAttribute.DESCRIPTION.value],
            LCPEncryptionConfiguration.lcpencrypt_output_directory.description
        )
        eq_(
            LCPAPI.SETTINGS[11][ConfigurationAttribute.TYPE.value],
            None
        )
        eq_(
            LCPAPI.SETTINGS[11][ConfigurationAttribute.REQUIRED.value],
            LCPEncryptionConfiguration.lcpencrypt_output_directory.required
        )
        eq_(
            LCPAPI.SETTINGS[11][ConfigurationAttribute.DEFAULT.value],
            LCPEncryptionConfiguration.lcpencrypt_output_directory.default
        )
        eq_(
            LCPAPI.SETTINGS[11][ConfigurationAttribute.CATEGORY.value],
            LCPEncryptionConfiguration.lcpencrypt_output_directory.category
        )

    @freeze_time("2020-01-01 00:00:00")
    def test_checkout_without_existing_loan(self):
        # Arrange
        lcp_api = LCPAPI(self._db, self._lcp_collection)
        patron = self._patron()
        days = self._lcp_collection.default_loan_period(patron.library)
        start_date = datetime.datetime.utcnow()
        end_date = start_date + datetime.timedelta(days=days)
        data_source = DataSource.lookup(self._db, DataSource.LCP, autocreate=True)
        data_source_name = data_source.name
        edition = self._edition(data_source_name=data_source_name, identifier_id=fixtures.CONTENT_ID)
        license_pool = self._licensepool(
            edition=edition, data_source_name=data_source_name, collection=self._lcp_collection)
        lcp_license = json.loads(fixtures.LCPSERVER_LICENSE)
        lcp_server_mock = create_autospec(spec=LCPServer)
        lcp_server_mock.generate_license = MagicMock(return_value=lcp_license)

        with self._configuration_factory.create(
                self._configuration_storage, self._db, LCPServerConfiguration) as configuration:

            with patch('api.lcp.collection.LCPServer') as lcp_server_constructor:
                lcp_server_constructor.return_value = lcp_server_mock

                configuration.lcpserver_url = fixtures.LCPSERVER_URL
                configuration.lcpserver_user = fixtures.LCPSERVER_USER
                configuration.lcpserver_password = fixtures.LCPSERVER_PASSWORD
                configuration.lcpserver_input_directory = fixtures.LCPSERVER_INPUT_DIRECTORY
                configuration.provider_name = fixtures.PROVIDER_NAME
                configuration.passphrase_hint = fixtures.TEXT_HINT
                configuration.encryption_algorithm = LCPServerConfiguration.DEFAULT_ENCRYPTION_ALGORITHM

                # Act
                loan = lcp_api.checkout(patron, 'pin', license_pool, 'internal format')

                # Assert
                eq_(loan.collection_id, self._lcp_collection.id)
                eq_(loan.collection(self._db), self._lcp_collection)
                eq_(loan.license_pool(self._db), license_pool)
                eq_(loan.data_source_name, data_source_name)
                eq_(loan.identifier_type, license_pool.identifier.type)
                eq_(loan.external_identifier, lcp_license['id'])
                eq_(loan.start_date, start_date)
                eq_(loan.end_date, end_date)

                lcp_server_mock.generate_license.assert_called_once_with(
                    self._db, fixtures.CONTENT_ID, patron, start_date, end_date)

    @freeze_time("2020-01-01 00:00:00")
    def test_checkout_with_existing_loan(self):
        # Arrange
        lcp_api = LCPAPI(self._db, self._lcp_collection)
        patron = self._patron()
        days = self._lcp_collection.default_loan_period(patron.library)
        start_date = datetime.datetime.utcnow()
        end_date = start_date + datetime.timedelta(days=days)
        data_source = DataSource.lookup(self._db, DataSource.LCP, autocreate=True)
        data_source_name = data_source.name
        edition = self._edition(data_source_name=data_source_name, identifier_id=fixtures.CONTENT_ID)
        license_pool = self._licensepool(
            edition=edition, data_source_name=data_source_name, collection=self._lcp_collection)
        lcp_license = json.loads(fixtures.LCPSERVER_LICENSE)
        lcp_server_mock = create_autospec(spec=LCPServer)
        lcp_server_mock.get_license = MagicMock(return_value=lcp_license)
        loan_identifier = 'e99be177-4902-426a-9b96-0872ae877e2f'

        license_pool.loan_to(patron, external_identifier=loan_identifier)

        with self._configuration_factory.create(
                self._configuration_storage, self._db, LCPServerConfiguration) as configuration:
            with patch('api.lcp.collection.LCPServer') as lcp_server_constructor:
                lcp_server_constructor.return_value = lcp_server_mock

                configuration.lcpserver_url = fixtures.LCPSERVER_URL
                configuration.lcpserver_user = fixtures.LCPSERVER_USER
                configuration.lcpserver_password = fixtures.LCPSERVER_PASSWORD
                configuration.lcpserver_input_directory = fixtures.LCPSERVER_INPUT_DIRECTORY
                configuration.provider_name = fixtures.PROVIDER_NAME
                configuration.passphrase_hint = fixtures.TEXT_HINT
                configuration.encryption_algorithm = LCPServerConfiguration.DEFAULT_ENCRYPTION_ALGORITHM

                # Act
                loan = lcp_api.checkout(patron, 'pin', license_pool, 'internal format')

                # Assert
                eq_(loan.collection_id, self._lcp_collection.id)
                eq_(loan.collection(self._db), self._lcp_collection)
                eq_(loan.license_pool(self._db), license_pool)
                eq_(loan.data_source_name, data_source_name)
                eq_(loan.identifier_type, license_pool.identifier.type)
                eq_(loan.external_identifier, loan_identifier)
                eq_(loan.start_date, start_date)
                eq_(loan.end_date, end_date)

                lcp_server_mock.get_license.assert_called_once_with(
                    self._db, loan_identifier, patron)

    @freeze_time("2020-01-01 00:00:00")
    def test_fulfil(self):
        # Arrange
        lcp_api = LCPAPI(self._db, self._lcp_collection)
        patron = self._patron()
        days = self._lcp_collection.default_loan_period(patron.library)
        today = datetime.datetime.utcnow()
        expires = today + datetime.timedelta(days=days)
        data_source = DataSource.lookup(self._db, DataSource.LCP, autocreate=True)
        data_source_name = data_source.name
        license_pool = self._licensepool(
            edition=None, data_source_name=data_source_name, collection=self._lcp_collection)
        lcp_license = json.loads(fixtures.LCPSERVER_LICENSE)
        lcp_server_mock = create_autospec(spec=LCPServer)
        lcp_server_mock.get_license = MagicMock(return_value=lcp_license)

        with self._configuration_factory.create(
                self._configuration_storage, self._db, LCPServerConfiguration) as configuration:
            with patch('api.lcp.collection.LCPServer') as lcp_server_constructor:
                lcp_server_constructor.return_value = lcp_server_mock

                configuration.lcpserver_url = fixtures.LCPSERVER_URL
                configuration.lcpserver_user = fixtures.LCPSERVER_USER
                configuration.lcpserver_password = fixtures.LCPSERVER_PASSWORD
                configuration.lcpserver_input_directory = fixtures.LCPSERVER_INPUT_DIRECTORY

                configuration.provider_name = fixtures.PROVIDER_NAME
                configuration.passphrase_hint = fixtures.TEXT_HINT
                configuration.encryption_algorithm = LCPServerConfiguration.DEFAULT_ENCRYPTION_ALGORITHM

                # Act
                license_pool.loan_to(patron, start=today, end=expires, external_identifier=lcp_license['id'])
                fulfilment_info = lcp_api.fulfill(patron, 'pin', license_pool, 'internal format')

                # Assert
                eq_(isinstance(fulfilment_info, LCPFulfilmentInfo), True)
                eq_(fulfilment_info.collection_id, self._lcp_collection.id)
                eq_(fulfilment_info.collection(self._db), self._lcp_collection)
                eq_(fulfilment_info.license_pool(self._db), license_pool)
                eq_(fulfilment_info.data_source_name, data_source_name)
                eq_(fulfilment_info.identifier_type, license_pool.identifier.type)

                lcp_server_mock.get_license.assert_called_once_with(
                    self._db, lcp_license['id'], patron)

    def test_patron_activity_returns_correct_result(self):
        # Arrange
        lcp_api = LCPAPI(self._db, self._lcp_collection)

        # 1. Correct loan
        patron = self._patron()
        days = self._lcp_collection.default_loan_period(patron.library)
        today = datetime.datetime.utcnow()
        expires = today + datetime.timedelta(days=days)
        data_source = DataSource.lookup(self._db, DataSource.LCP, autocreate=True)
        data_source_name = data_source.name
        external_identifier = '1'
        license_pool = self._licensepool(
            edition=None, data_source_name=data_source_name, collection=self._lcp_collection)
        license_pool.loan_to(patron, start=today, end=expires, external_identifier=external_identifier)

        # 2. Loan from a different collection
        other_collection = self._collection(protocol=ExternalIntegration.MANUAL)
        other_external_identifier = '2'
        other_license_pool = self._licensepool(
            edition=None, data_source_name=data_source_name, collection=other_collection)
        other_license_pool.loan_to(patron, start=today, end=expires, external_identifier=other_external_identifier)

        # 3. Other patron's loan
        other_patron = self._patron()
        other_license_pool = self._licensepool(
            edition=None, data_source_name=data_source_name, collection=other_collection)
        other_license_pool.loan_to(other_patron, start=today, end=expires)

        # 4. Expired loan
        other_license_pool = self._licensepool(
            edition=None, data_source_name=data_source_name, collection=self._lcp_collection)
        other_license_pool.loan_to(patron, start=today, end=today - datetime.timedelta(days=1))

        # 5. Not started loan
        other_license_pool = self._licensepool(
            edition=None, data_source_name=data_source_name, collection=self._lcp_collection)
        other_license_pool.loan_to(
            patron, start=today + datetime.timedelta(days=1), end=today + datetime.timedelta(days=2))

        # Act
        loans = lcp_api.patron_activity(patron, 'pin')

        # Assert
        eq_(len(loans), 1)

        loan = loans[0]
        eq_(loan.collection_id, self._lcp_collection.id)
        eq_(loan.collection(self._db), self._lcp_collection)
        eq_(loan.license_pool(self._db), license_pool)
        eq_(loan.data_source_name, data_source_name)
        eq_(loan.identifier_type, license_pool.identifier.type)
        eq_(loan.external_identifier, external_identifier)
        eq_(loan.start_date, today)
        eq_(loan.end_date, expires)
