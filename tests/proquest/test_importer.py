import datetime
import json

from api.authenticator import BaseSAMLAuthenticationProvider
from api.circulation_exceptions import CannotFulfill, CannotLoan
from api.proquest.client import (
    ProQuestAPIClient,
    ProQuestAPIClientFactory,
    ProQuestBook,
)
from api.proquest.credential import ProQuestCredentialManager
from api.proquest.identifier import ProQuestIdentifierParser
from api.proquest.importer import (
    ProQuestOPDS2Importer,
    ProQuestOPDS2ImporterConfiguration,
    ProQuestOPDS2ImportMonitor,
)
from api.saml.metadata.model import (
    SAMLAttribute,
    SAMLAttributeStatement,
    SAMLAttributeType,
    SAMLSubject,
    SAMLSubjectJSONEncoder,
)
from core.metadata_layer import LinkData
from core.model import (
    Collection,
    CoverageRecord,
    Credential,
    DataSource,
    DeliveryMechanism,
    ExternalIntegration,
    Hyperlink,
    Identifier,
)
from core.model.configuration import (
    ConfigurationFactory,
    ConfigurationStorage,
    HasExternalIntegration,
)
from core.testing import DatabaseTest
from flask import Response
from freezegun import freeze_time
from mock import MagicMock, call, create_autospec, patch
from nose.tools import assert_raises, eq_
from parameterized import parameterized
from requests import HTTPError
from tests.proquest import fixtures


class TestProQuestOPDS2Importer(DatabaseTest):
    def setup(self, mock_search=True):
        super(TestProQuestOPDS2Importer, self).setup()

        self._proquest_data_source = DataSource.lookup(
            self._db, DataSource.PROQUEST, autocreate=True
        )
        self._proquest_collection = self._collection(
            protocol=ExternalIntegration.PROQUEST
        )
        self._proquest_collection.external_integration.set_setting(
            Collection.DATA_SOURCE_NAME_SETTING, DataSource.PROQUEST
        )

        self._proquest_patron = self._patron()
        self._loan_start_date = datetime.datetime(2020, 1, 1)
        self._loan_end_date = None
        self._proquest_document_id = "12345"
        self._proquest_edition = self._edition(
            data_source_name=self._proquest_data_source.name,
            identifier_type=Identifier.PROQUEST_ID,
            identifier_id=self._proquest_document_id,
        )
        self._proquest_license_pool = self._licensepool(
            edition=self._proquest_edition,
            data_source_name=self._proquest_data_source.name,
            collection=self._proquest_collection,
        )
        self._proquest_delivery_mechanism = self._add_generic_delivery_mechanism(
            self._proquest_license_pool
        )

        self._integration = self._proquest_collection.external_integration
        integration_owner = create_autospec(spec=HasExternalIntegration)
        integration_owner.external_integration = MagicMock(
            return_value=self._integration
        )
        self._configuration_storage = ConfigurationStorage(integration_owner)
        self._configuration_factory = ConfigurationFactory()

    @freeze_time("2020-01-01 00:00:00")
    def test_checkout_lookups_for_existing_token(self):
        # We want to test that checkout operation always is always preceded by
        # checking for a ProQuest JWT bearer token. # Without a valid JWT token, checkout operation will fail.

        # Arrange
        proquest_token = "1234567890"
        credential_manager_mock = create_autospec(spec=ProQuestCredentialManager)
        credential_manager_mock.lookup_proquest_token = MagicMock(
            return_value=proquest_token
        )

        with patch(
            "api.proquest.importer.ProQuestCredentialManager"
        ) as credential_manager_constructor_mock:
            credential_manager_constructor_mock.return_value = credential_manager_mock

            # Act
            importer = ProQuestOPDS2Importer(self._db, self._proquest_collection)
            loan = importer.checkout(
                self._proquest_patron,
                "pin",
                self._proquest_license_pool,
                self._proquest_delivery_mechanism,
            )

            # Assert
            eq_(self._proquest_collection.id, loan.collection_id)
            eq_(self._proquest_collection, loan.collection(self._db))
            eq_(self._proquest_license_pool, loan.license_pool(self._db))
            eq_(self._proquest_data_source.name, loan.data_source_name)
            eq_(self._proquest_license_pool.identifier.type, loan.identifier_type)
            eq_(
                None,
                loan.external_identifier,
            )
            eq_(self._loan_start_date, loan.start_date)
            eq_(self._loan_end_date, loan.end_date)

            # Assert that ProQuestCredentialManager.lookup_proquest_token
            # was called when CM tried to fetch an existing token.
            credential_manager_mock.lookup_proquest_token.assert_called_once_with(
                self._db, self._proquest_patron
            )

    @freeze_time("2020-01-01 00:00:00")
    def test_checkout_creates_new_token_if_there_is_none(self):
        # We want to test that checkout operation without an existing ProQuest JWT bearer token leads to the following:
        # 1. Circulation Manager (CM) lookups for an existing token and doesn't find any.
        # 2. CM looks for an existing SAML affiliation ID.
        # 3. CM creates a new ProQuest JWT bearer token using the SAML affiliation ID from the previous step.
        # 4. CM saves the new token.

        # Arrange
        affiliation_id = "12345"
        proquest_token = "1234567890"

        api_client_mock = create_autospec(spec=ProQuestAPIClient)
        api_client_mock.create_token = MagicMock(return_value=proquest_token)

        api_client_factory_mock = create_autospec(spec=ProQuestAPIClientFactory)
        api_client_factory_mock.create = MagicMock(return_value=api_client_mock)

        credential_manager_mock = create_autospec(spec=ProQuestCredentialManager)
        credential_manager_mock.lookup_patron_affiliation_id = MagicMock(
            return_value=affiliation_id
        )
        credential_manager_mock.lookup_proquest_token = MagicMock(
            side_effect=[None, proquest_token]
        )

        with patch(
            "api.proquest.importer.ProQuestAPIClientFactory"
        ) as api_client_factory_constructor_mock, patch(
            "api.proquest.importer.ProQuestCredentialManager"
        ) as credential_manager_constructor_mock:
            api_client_factory_constructor_mock.return_value = api_client_factory_mock
            credential_manager_constructor_mock.return_value = credential_manager_mock

            # Act
            importer = ProQuestOPDS2Importer(self._db, self._proquest_collection)
            loan = importer.checkout(
                self._proquest_patron,
                "pin",
                self._proquest_license_pool,
                self._proquest_delivery_mechanism,
            )

            # Assert
            eq_(self._proquest_collection.id, loan.collection_id)
            eq_(self._proquest_collection, loan.collection(self._db))
            eq_(self._proquest_license_pool, loan.license_pool(self._db))
            eq_(self._proquest_data_source.name, loan.data_source_name)
            eq_(self._proquest_license_pool.identifier.type, loan.identifier_type)
            eq_(
                None,
                loan.external_identifier,
            )
            eq_(self._loan_start_date, loan.start_date)
            eq_(self._loan_end_date, loan.end_date)

            # 1. Assert that ProQuestCredentialManager.lookup_proquest_token
            # was called when CM tried to fetch a non-existent token.
            credential_manager_mock.lookup_proquest_token.assert_called_once_with(
                self._db, self._proquest_patron
            )

            # 2. Assert that ProQuestCredentialManager.lookup_patron_affiliation_id
            # was called when CM tried to fetch an existing SAML affiliation ID.
            credential_manager_mock.lookup_patron_affiliation_id.assert_called_once_with(
                self._db,
                self._proquest_patron,
                ProQuestOPDS2ImporterConfiguration.DEFAULT_AFFILIATION_ATTRIBUTES,
            )

            # 3. Assert that ProQuest.create_token was called when CM tried to create a new ProQuest JWT bearer token
            # using the SAML affiliation ID from step 2.
            api_client_mock.create_token.assert_called_once_with(
                self._db, affiliation_id
            )

            # 4. Assert that ProQuestCredentialManager.save_proquest_token
            # was called when CM tried to save the token created in step 3.
            credential_manager_mock.save_proquest_token.assert_called_once_with(
                self._db,
                self._proquest_patron,
                datetime.timedelta(hours=1),
                proquest_token,
            )

    @parameterized.expand(
        [
            (
                "tuple",
                (
                    SAMLAttributeType.mail.name,
                    SAMLAttributeType.uid.name,
                ),
            ),
            (
                "list",
                [
                    SAMLAttributeType.mail.name,
                    SAMLAttributeType.uid.name,
                ],
            ),
            (
                "tuple_string",
                "({0}, {1})".format(
                    SAMLAttributeType.mail.name,
                    SAMLAttributeType.uid.name,
                ),
            ),
            (
                "list_string",
                json.dumps(
                    [
                        SAMLAttributeType.mail.name,
                        SAMLAttributeType.uid.name,
                    ]
                ),
            ),
        ]
    )
    @freeze_time("2020-01-01 00:00:00")
    def test_checkout_creates_new_token_using_affiliation_id_from_custom_saml_attribute(
        self, _, custom_affiliation_attributes
    ):
        # We want to test that checkout operation without an existing ProQuest JWT bearer token leads to the following:
        # 1. Circulation Manager (CM) lookups for an existing token and doesn't find any.
        # 2. CM looks for an existing SAML affiliation ID in the list of SAML attributes specified in the settings.
        # 3. CM creates a new ProQuest JWT bearer token using the SAML affiliation ID from the previous step.
        # 4. CM saves the new token.

        # Arrange
        affiliation_id = "12345"
        proquest_token = "1234567890"

        expected_affiliation_attributes = (
            SAMLAttributeType.mail.name,
            SAMLAttributeType.uid.name,
        )

        saml_subject = SAMLSubject(
            None,
            SAMLAttributeStatement(
                [SAMLAttribute(SAMLAttributeType.uid.name, [affiliation_id])]
            ),
        )
        saml_token = json.dumps(saml_subject, cls=SAMLSubjectJSONEncoder)
        saml_datasource = DataSource.lookup(
            self._db,
            BaseSAMLAuthenticationProvider.TOKEN_DATA_SOURCE_NAME,
            autocreate=True,
        )
        Credential.temporary_token_create(
            self._db,
            saml_datasource,
            BaseSAMLAuthenticationProvider.TOKEN_TYPE,
            self._proquest_patron,
            datetime.timedelta(hours=1),
            saml_token,
        )

        api_client_mock = create_autospec(spec=ProQuestAPIClient)
        api_client_mock.create_token = MagicMock(return_value=proquest_token)

        api_client_factory_mock = create_autospec(spec=ProQuestAPIClientFactory)
        api_client_factory_mock.create = MagicMock(return_value=api_client_mock)

        credential_manager_mock = create_autospec(spec=ProQuestCredentialManager)
        credential_manager_mock.lookup_patron_affiliation_id = MagicMock(
            return_value=affiliation_id
        )
        credential_manager_mock.lookup_proquest_token = MagicMock(
            side_effect=[None, proquest_token]
        )

        with self._configuration_factory.create(
            self._configuration_storage, self._db, ProQuestOPDS2ImporterConfiguration
        ) as configuration:
            configuration.affiliation_attributes = custom_affiliation_attributes

            with patch(
                "api.proquest.importer.ProQuestAPIClientFactory"
            ) as api_client_factory_constructor_mock, patch(
                "api.proquest.importer.ProQuestCredentialManager"
            ) as credential_manager_constructor_mock:
                api_client_factory_constructor_mock.return_value = (
                    api_client_factory_mock
                )
                credential_manager_constructor_mock.return_value = (
                    credential_manager_mock
                )

                # Act
                importer = ProQuestOPDS2Importer(self._db, self._proquest_collection)
                loan = importer.checkout(
                    self._proquest_patron,
                    "pin",
                    self._proquest_license_pool,
                    self._proquest_delivery_mechanism,
                )

                # Assert
                eq_(self._proquest_collection.id, loan.collection_id)
                eq_(self._proquest_collection, loan.collection(self._db))
                eq_(self._proquest_license_pool, loan.license_pool(self._db))
                eq_(self._proquest_data_source.name, loan.data_source_name)
                eq_(self._proquest_license_pool.identifier.type, loan.identifier_type)
                eq_(
                    None,
                    loan.external_identifier,
                )
                eq_(self._loan_start_date, loan.start_date)
                eq_(self._loan_end_date, loan.end_date)

                # 1. Assert that ProQuestCredentialManager.lookup_proquest_token
                # was called when CM tried to fetch a non-existent token.
                credential_manager_mock.lookup_proquest_token.assert_called_once_with(
                    self._db, self._proquest_patron
                )

                # 2. Assert that ProQuestCredentialManager.lookup_patron_affiliation_id
                # was called when CM tried to fetch an existing SAML affiliation ID.
                credential_manager_mock.lookup_patron_affiliation_id.assert_called_once_with(
                    self._db,
                    self._proquest_patron,
                    expected_affiliation_attributes,
                )

                # 3. Assert that ProQuest.create_token was called when CM tried to create
                # a new ProQuest JWT bearer token using the SAML affiliation ID from step 2.
                api_client_mock.create_token.assert_called_once_with(
                    self._db, affiliation_id
                )

                # 4. Assert that ProQuestCredentialManager.save_proquest_token
                # was called when CM tried to save the token created in step 3.
                credential_manager_mock.save_proquest_token.assert_called_once_with(
                    self._db,
                    self._proquest_patron,
                    datetime.timedelta(hours=1),
                    proquest_token,
                )

    @freeze_time("2020-01-01 00:00:00")
    def test_checkout_raises_cannot_loan_error_if_it_cannot_get_affiliation_id(self):
        # We want to test that checkout operation returns api.proquest.importer.MISSING_AFFILIATION_ID
        # when it cannot get the patron's affiliation ID.

        # Arrange
        credential_manager_mock = create_autospec(spec=ProQuestCredentialManager)
        credential_manager_mock.lookup_proquest_token = MagicMock(return_value=None)
        credential_manager_mock.lookup_patron_affiliation_id = MagicMock(
            return_value=None
        )

        with patch(
            "api.proquest.importer.ProQuestCredentialManager"
        ) as credential_manager_constructor_mock:
            credential_manager_constructor_mock.return_value = credential_manager_mock

            # Act
            importer = ProQuestOPDS2Importer(self._db, self._proquest_collection)

            with assert_raises(CannotLoan):
                importer.checkout(
                    self._proquest_patron,
                    "pin",
                    self._proquest_license_pool,
                    self._proquest_delivery_mechanism,
                )

            # Assert
            # 1. Assert that ProQuestCredentialManager.lookup_proquest_token
            # was called when CM tried to fetch a non-existent token.
            credential_manager_mock.lookup_proquest_token.assert_called_once_with(
                self._db, self._proquest_patron
            )

            # 2. Assert that ProQuestCredentialManager.lookup_patron_affiliation_id
            # was called when CM tried to fetch an affiliation ID.
            # This operation failed leading to raising CannotLoan.
            credential_manager_mock.lookup_proquest_token.lookup_patron_affiliation_id(
                self._db, self._proquest_patron
            )

    @freeze_time("2020-01-01 00:00:00")
    def test_checkout_raises_cannot_loan_error_if_it_cannot_create_proquest_token(self):
        # We want to test that checkout operation returns api.proquest.importer.CANNOT_CREATE_PROQUEST_TOKEN
        # when it cannot create a ProQuest JWT bearer token using ProQuest API.

        # Arrange
        affiliation_id = "1"

        api_client_mock = create_autospec(spec=ProQuestAPIClient)
        api_client_mock.create_token = MagicMock(side_effect=HTTPError)

        api_client_factory_mock = create_autospec(spec=ProQuestAPIClientFactory)
        api_client_factory_mock.create = MagicMock(return_value=api_client_mock)

        credential_manager_mock = create_autospec(spec=ProQuestCredentialManager)
        credential_manager_mock.lookup_proquest_token = MagicMock(return_value=None)
        credential_manager_mock.lookup_patron_affiliation_id = MagicMock(
            return_value=affiliation_id
        )

        with patch(
            "api.proquest.importer.ProQuestAPIClientFactory"
        ) as api_client_factory_constructor_mock, patch(
            "api.proquest.importer.ProQuestCredentialManager"
        ) as credential_manager_constructor_mock:
            api_client_factory_constructor_mock.return_value = api_client_factory_mock
            credential_manager_constructor_mock.return_value = credential_manager_mock

            # Act
            importer = ProQuestOPDS2Importer(self._db, self._proquest_collection)
            with assert_raises(CannotLoan):
                importer.checkout(
                    self._proquest_patron,
                    "pin",
                    self._proquest_license_pool,
                    self._proquest_delivery_mechanism,
                )

            # Assert
            # Assert than ProQuestOPDS2Importer correctly created an instance of ProQuestAPIClient.
            api_client_factory_mock.create.assert_called_once_with(importer)

            # 1. Assert that ProQuestCredentialManager.lookup_proquest_token
            # was called when CM tried to fetch a non-existent token.
            credential_manager_mock.lookup_proquest_token.assert_called_once_with(
                self._db, self._proquest_patron
            )

            # 2. Assert that ProQuestCredentialManager.lookup_patron_affiliation_id
            # was called when CM tried to fetch an existing affiliation ID.
            credential_manager_mock.lookup_patron_affiliation_id.assert_called_once_with(
                self._db,
                self._proquest_patron,
                ProQuestOPDS2ImporterConfiguration.DEFAULT_AFFILIATION_ATTRIBUTES,
            )

            # 3. Assert that ProQuestAPIClient.create_token was called when CM tried to create a new JWT bearer token.
            # This operation failed resulting in raising CannotFulfill error.
            api_client_mock.create_token.assert_called_once_with(
                self._db, affiliation_id
            )

    @freeze_time("2020-01-01 00:00:00")
    def test_fulfil_lookups_for_existing_token(self):
        # We want to test that fulfil operation always is always preceded by
        # checking for a ProQuest JWT bearer token.
        # Without a valid JWT token, fulfil operation will fail.
        # Additionally, we want to test that Circulation Manager handles downloading of DRM-free books.

        # Arrange
        proquest_token = "1234567890"
        proquest_token_expires_in = datetime.datetime.utcnow() + datetime.timedelta(
            hours=1
        )
        proquest_credential = Credential(
            credential=proquest_token, expires=proquest_token_expires_in
        )
        drm_free_book = ProQuestBook(link="https://proquest.com/books/books.epub")

        api_client_mock = create_autospec(spec=ProQuestAPIClient)
        api_client_mock.get_book = MagicMock(return_value=drm_free_book)

        api_client_factory_mock = create_autospec(spec=ProQuestAPIClientFactory)
        api_client_factory_mock.create = MagicMock(return_value=api_client_mock)

        credential_manager_mock = create_autospec(spec=ProQuestCredentialManager)
        credential_manager_mock.lookup_proquest_token = MagicMock(
            return_value=proquest_credential
        )

        with patch(
            "api.proquest.importer.ProQuestAPIClientFactory"
        ) as api_client_factory_constructor_mock, patch(
            "api.proquest.importer.ProQuestCredentialManager"
        ) as credential_manager_constructor_mock:
            api_client_factory_constructor_mock.return_value = api_client_factory_mock
            credential_manager_constructor_mock.return_value = credential_manager_mock

            # Act
            importer = ProQuestOPDS2Importer(self._db, self._proquest_collection)
            fulfilment_info = importer.fulfill(
                self._proquest_patron,
                "pin",
                self._proquest_license_pool,
                self._proquest_delivery_mechanism,
            )

            # Assert
            eq_(self._proquest_collection.id, fulfilment_info.collection_id)
            eq_(self._proquest_collection, fulfilment_info.collection(self._db))
            eq_(self._proquest_license_pool, fulfilment_info.license_pool(self._db))
            eq_(self._proquest_data_source.name, fulfilment_info.data_source_name)
            eq_(
                self._proquest_license_pool.identifier.type,
                fulfilment_info.identifier_type,
            )

            # Make sure that the fulfilment info doesn't contain a link but instead contains a JSON document
            # which is used to pass the book's link and the ProQuest token to the client app.
            eq_(None, fulfilment_info.content_link)
            eq_(DeliveryMechanism.BEARER_TOKEN, fulfilment_info.content_type)
            eq_(True, fulfilment_info.content is not None)

            token_document = json.loads(fulfilment_info.content)
            eq_("Bearer", token_document["token_type"])
            eq_(proquest_token, token_document["access_token"])
            eq_(
                (
                    proquest_token_expires_in - datetime.datetime.utcnow()
                ).total_seconds(),
                token_document["expires_in"],
            )
            eq_(drm_free_book.link, token_document["location"])
            eq_(
                DeliveryMechanism.BEARER_TOKEN,
                fulfilment_info.content_type,
            )
            eq_(proquest_token_expires_in, fulfilment_info.content_expires)

            # Assert than ProQuestOPDS2Importer correctly created an instance of ProQuestAPIClient.
            api_client_factory_mock.create.assert_called_once_with(importer)

            # 1. Assert that ProQuestCredentialManager.lookup_proquest_token
            # was called when CM tried to fetch an existing token.
            credential_manager_mock.lookup_proquest_token.assert_called_with(
                self._db, self._proquest_patron
            )

            # 2. Assert that ProQuestAPIClient.get_book
            # was called when CM tried to get the book.
            api_client_mock.get_book.assert_called_once_with(
                self._db,
                proquest_token,
                self._proquest_license_pool.identifier.identifier,
            )

    @freeze_time("2020-01-01 00:00:00")
    def test_fulfil_creates_new_token_if_there_is_none(self):
        # We want to test that fulfil operation without an existing ProQuest JWT bearer token leads to the following:
        # 1. Circulation Manager (CM) lookups for an existing token and doesn't find any.
        # 2. CM looks for an existing SAML affiliation ID.
        # 3. CM creates a new ProQuest JWT bearer token using the SAML affiliation ID from the previous step.
        # 4. CM saves the new token.

        # Arrange
        affiliation_id = "12345"
        proquest_token = "1234567890"
        proquest_credential = Credential(credential=proquest_token)
        book = ProQuestBook(content=bytes("Book"))

        api_client_mock = create_autospec(spec=ProQuestAPIClient)
        api_client_mock.create_token = MagicMock(return_value=proquest_token)
        api_client_mock.get_book = MagicMock(return_value=book)

        api_client_factory_mock = create_autospec(spec=ProQuestAPIClientFactory)
        api_client_factory_mock.create = MagicMock(return_value=api_client_mock)

        credential_manager_mock = create_autospec(spec=ProQuestCredentialManager)
        credential_manager_mock.lookup_patron_affiliation_id = MagicMock(
            return_value=affiliation_id
        )
        credential_manager_mock.lookup_proquest_token = MagicMock(
            side_effect=[None, proquest_credential]
        )

        with patch(
            "api.proquest.importer.ProQuestAPIClientFactory"
        ) as api_client_factory_constructor_mock, patch(
            "api.proquest.importer.ProQuestCredentialManager"
        ) as credential_manager_constructor_mock:
            api_client_factory_constructor_mock.return_value = api_client_factory_mock
            credential_manager_constructor_mock.return_value = credential_manager_mock

            # Act
            importer = ProQuestOPDS2Importer(self._db, self._proquest_collection)
            fulfilment_info = importer.fulfill(
                self._proquest_patron,
                "pin",
                self._proquest_license_pool,
                self._proquest_delivery_mechanism,
            )

            # Assert
            eq_(self._proquest_collection.id, fulfilment_info.collection_id)
            eq_(self._proquest_collection, fulfilment_info.collection(self._db))
            eq_(self._proquest_license_pool, fulfilment_info.license_pool(self._db))
            eq_(self._proquest_data_source.name, fulfilment_info.data_source_name)
            eq_(
                self._proquest_license_pool.identifier.type,
                fulfilment_info.identifier_type,
            )
            eq_(None, fulfilment_info.content_link)
            eq_(
                self._proquest_delivery_mechanism.delivery_mechanism.media_type,
                fulfilment_info.content_type,
            )
            eq_(book.content, fulfilment_info.content)
            eq_(None, fulfilment_info.content_expires)

            # Assert than ProQuestOPDS2Importer correctly created an instance of ProQuestAPIClient.
            api_client_factory_mock.create.assert_called_once_with(importer)

            # 1. Assert that ProQuestCredentialManager.lookup_proquest_token
            # was called when CM tried to fetch an nonexistent token.
            credential_manager_mock.lookup_proquest_token.assert_called_with(
                self._db, self._proquest_patron
            )

            # 2. Assert that ProQuestCredentialManager.lookup_patron_affiliation_id
            # was called when CM tried to fetch an existing SAML affiliation ID.
            credential_manager_mock.lookup_patron_affiliation_id.assert_called_once_with(
                self._db,
                self._proquest_patron,
                ProQuestOPDS2ImporterConfiguration.DEFAULT_AFFILIATION_ATTRIBUTES,
            )

            # 3. Assert that ProQuest.create_token was called when CM tried to create a new ProQuest JWT bearer token
            # using the SAML affiliation ID from step 2.
            api_client_mock.create_token.assert_called_once_with(
                self._db, affiliation_id
            )

            # 4. Assert that ProQuestCredentialManager.save_proquest_token
            # was called when CM tried to save the token created in step 3.
            credential_manager_mock.save_proquest_token.assert_called_once_with(
                self._db,
                self._proquest_patron,
                datetime.timedelta(hours=1),
                proquest_token,
            )

            # 5. Assert that ProQuestAPIClient.get_book
            # was called when CM tried to get the book.
            api_client_mock.get_book.assert_called_once_with(
                self._db,
                proquest_token,
                self._proquest_license_pool.identifier.identifier,
            )

    @freeze_time("2020-01-01 00:00:00")
    def test_fulfil_raises_cannot_fulfil_error_if_it_cannot_get_affiliation_id(self):
        # We want to test that fulfil operation returns api.proquest.importer.MISSING_AFFILIATION_ID
        # when it cannot get the patron's affiliation ID.

        # Arrange
        credential_manager_mock = create_autospec(spec=ProQuestCredentialManager)
        credential_manager_mock.lookup_proquest_token = MagicMock(return_value=None)
        credential_manager_mock.lookup_patron_affiliation_id = MagicMock(
            return_value=None
        )

        with patch(
            "api.proquest.importer.ProQuestCredentialManager"
        ) as credential_manager_constructor_mock:
            credential_manager_constructor_mock.return_value = credential_manager_mock

            # Act
            importer = ProQuestOPDS2Importer(self._db, self._proquest_collection)

            with assert_raises(CannotFulfill):
                importer.fulfill(
                    self._proquest_patron,
                    "pin",
                    self._proquest_license_pool,
                    self._proquest_delivery_mechanism,
                )

            # Assert
            # 1. Assert that ProQuestCredentialManager.lookup_proquest_token
            # was called when CM tried to fetch an nonexistent token.
            credential_manager_mock.lookup_proquest_token.assert_called_once_with(
                self._db, self._proquest_patron
            )

            # 2. Assert that ProQuestCredentialManager.lookup_patron_affiliation_id
            # was called when CM tried to fetch an existing SAML affiliation ID.
            credential_manager_mock.lookup_patron_affiliation_id.assert_called_once_with(
                self._db,
                self._proquest_patron,
                ProQuestOPDS2ImporterConfiguration.DEFAULT_AFFILIATION_ATTRIBUTES,
            )

    @freeze_time("2020-01-01 00:00:00")
    def test_fulfil_raises_cannot_fulfil_error_if_it_cannot_create_proquest_token(self):
        # We want to test that fulfil operation returns api.proquest.importer.CANNOT_CREATE_PROQUEST_TOKEN
        # when it cannot create a ProQuest JWT bearer token using ProQuest API.

        # Arrange
        affiliation_id = "1"

        api_client_mock = create_autospec(spec=ProQuestAPIClient)
        api_client_mock.create_token = MagicMock(side_effect=HTTPError)

        api_client_factory_mock = create_autospec(spec=ProQuestAPIClientFactory)
        api_client_factory_mock.create = MagicMock(return_value=api_client_mock)

        credential_manager_mock = create_autospec(spec=ProQuestCredentialManager)
        credential_manager_mock.lookup_proquest_token = MagicMock(return_value=None)
        credential_manager_mock.lookup_patron_affiliation_id = MagicMock(
            return_value=affiliation_id
        )

        with patch(
            "api.proquest.importer.ProQuestAPIClientFactory"
        ) as api_client_factory_constructor_mock, patch(
            "api.proquest.importer.ProQuestCredentialManager"
        ) as credential_manager_constructor_mock:
            api_client_factory_constructor_mock.return_value = api_client_factory_mock
            credential_manager_constructor_mock.return_value = credential_manager_mock

            # Act
            importer = ProQuestOPDS2Importer(self._db, self._proquest_collection)

            with assert_raises(CannotFulfill):
                importer.fulfill(
                    self._proquest_patron,
                    "pin",
                    self._proquest_license_pool,
                    self._proquest_delivery_mechanism,
                )

            # Assert
            # 1. Assert that ProQuestCredentialManager.lookup_proquest_token
            # was called when CM tried to fetch an nonexistent token.
            credential_manager_mock.lookup_proquest_token.assert_called_once_with(
                self._db, self._proquest_patron
            )

            # 2. Assert that ProQuestCredentialManager.lookup_patron_affiliation_id
            # was called when CM tried to fetch an existing SAML affiliation ID.
            credential_manager_mock.lookup_patron_affiliation_id.assert_called_once_with(
                self._db,
                self._proquest_patron,
                ProQuestOPDS2ImporterConfiguration.DEFAULT_AFFILIATION_ATTRIBUTES,
            )

            # 3. Assert that ProQuestAPIClient.create_token was called when CM tried to create a new JWT bearer token.
            # This operation failed resulting in raising CannotFulfill error.
            api_client_mock.create_token.assert_called_once_with(
                self._db, affiliation_id
            )

    @freeze_time("2020-01-01 00:00:00")
    def test_fulfil_refreshes_expired_token(self):
        # By default ProQuest JWT bearer tokens should be valid for 1 hour but
        # since they are controlled by ProQuest we cannot be sure that they will not change this setting.
        # We want to test that fulfil operation automatically refreshes an expired token:
        # 1. CM fetches a token from the storage.
        # 2. CM tries to download the book using the token but ProQuest API returns 401 status code.
        # 3. CM generates a new token.
        # 4. CM tries to generate a book using the new token.
        # Additionally, we want to test that Circulation Manager handles downloading of ACSM files.

        # Arrange
        affiliation_id = "12345"
        expired_proquest_token = "1234567890"
        expired_proquest_token_expired_in = (
            datetime.datetime.utcnow() - datetime.timedelta(minutes=1)
        )
        expired_proquest_token_credential = Credential(
            credential=expired_proquest_token, expires=expired_proquest_token_expired_in
        )
        new_proquest_token = "1234567890_"
        new_proquest_token_expires_in = datetime.datetime.utcnow() + datetime.timedelta(
            hours=1
        )
        new_proquest_token_credential = Credential(
            credential=new_proquest_token, expires=new_proquest_token_expires_in
        )
        adobe_drm_protected_book = ProQuestBook(
            content=bytes("ACSM file"), content_type=DeliveryMechanism.ADOBE_DRM
        )

        api_client_mock = create_autospec(spec=ProQuestAPIClient)
        api_client_mock.create_token = MagicMock(return_value=new_proquest_token)
        api_client_mock.get_book = MagicMock(
            side_effect=[
                HTTPError(response=Response(status=401)),
                adobe_drm_protected_book,
            ]
        )

        api_client_factory_mock = create_autospec(spec=ProQuestAPIClientFactory)
        api_client_factory_mock.create = MagicMock(return_value=api_client_mock)

        credential_manager_mock = create_autospec(spec=ProQuestCredentialManager)
        credential_manager_mock.lookup_patron_affiliation_id = MagicMock(
            return_value=affiliation_id
        )
        credential_manager_mock.lookup_proquest_token = MagicMock(
            return_value=expired_proquest_token_credential
        )
        credential_manager_mock.save_proquest_token = MagicMock(
            return_value=new_proquest_token_credential
        )

        with patch(
            "api.proquest.importer.ProQuestAPIClientFactory"
        ) as api_client_factory_constructor_mock, patch(
            "api.proquest.importer.ProQuestCredentialManager"
        ) as credential_manager_constructor_mock:
            api_client_factory_constructor_mock.return_value = api_client_factory_mock
            credential_manager_constructor_mock.return_value = credential_manager_mock

            # Act
            importer = ProQuestOPDS2Importer(self._db, self._proquest_collection)
            fulfilment_info = importer.fulfill(
                self._proquest_patron,
                "pin",
                self._proquest_license_pool,
                self._proquest_delivery_mechanism,
            )

            # Assert
            eq_(self._proquest_collection.id, fulfilment_info.collection_id)
            eq_(self._proquest_collection, fulfilment_info.collection(self._db))
            eq_(self._proquest_license_pool, fulfilment_info.license_pool(self._db))
            eq_(self._proquest_data_source.name, fulfilment_info.data_source_name)
            eq_(
                self._proquest_license_pool.identifier.type,
                fulfilment_info.identifier_type,
            )

            # Make sure that fulfilment info contains content of the ACSM file not a link.
            eq_(None, fulfilment_info.content_link)
            eq_(
                adobe_drm_protected_book.content_type,
                fulfilment_info.content_type,
            )
            eq_(adobe_drm_protected_book.content, fulfilment_info.content)
            eq_(None, fulfilment_info.content_expires)

            # Assert than ProQuestOPDS2Importer correctly created an instance of ProQuestAPIClient.
            api_client_factory_mock.create.assert_called_once_with(importer)

            # 1. Assert that ProQuestCredentialManager.lookup_proquest_token
            # was called when CM tried to fetch a existing token.
            credential_manager_mock.lookup_proquest_token.assert_called_with(
                self._db, self._proquest_patron
            )

            # 2. Assert that ProQuestAPIClient.get_book
            # was called when CM tried to get the book.
            api_client_mock.get_book.assert_any_call(
                self._db,
                expired_proquest_token,
                self._proquest_license_pool.identifier.identifier,
            )

            # 3. Assert that ProQuestCredentialManager.lookup_patron_affiliation_id
            # was called when CM tried to fetch an existing SAML affiliation ID.
            credential_manager_mock.lookup_patron_affiliation_id.assert_called_once_with(
                self._db,
                self._proquest_patron,
                ProQuestOPDS2ImporterConfiguration.DEFAULT_AFFILIATION_ATTRIBUTES,
            )

            # 4. Assert that ProQuest.create_token was called when CM tried to create a new ProQuest JWT bearer token
            # using the SAML affiliation ID from step 2.
            api_client_mock.create_token.assert_called_once_with(
                self._db, affiliation_id
            )

            # 5. Assert that ProQuestCredentialManager.save_proquest_token
            # was called when CM tried to save the token created in step 3.
            credential_manager_mock.save_proquest_token.assert_called_once_with(
                self._db,
                self._proquest_patron,
                datetime.timedelta(hours=1),
                new_proquest_token,
            )

            # 6. Assert that ProQuestAPIClient.get_book
            # was called when CM tried to get the book.
            api_client_mock.get_book.assert_any_call(
                self._db,
                new_proquest_token,
                self._proquest_license_pool.identifier.identifier,
            )
            eq_(2, api_client_mock.get_book.call_count)

    def test_correctly_imports_covers(self):
        # We want to make sure that ProQuestOPDS2Importer
        # correctly processes cover links in the ProQuest feed
        # and generates LinkData for both, the full cover and thumbnail.

        # Act
        importer = ProQuestOPDS2Importer(self._db, self._proquest_collection)

        result = importer.extract_feed_data(fixtures.PROQUEST_RAW_FEED)

        # Assert
        eq_(2, len(result))
        publication_metadata_dictionary = result[0]

        eq_(
            True,
            fixtures.PROQUEST_RAW_PUBLICATION_ID in publication_metadata_dictionary,
        )
        publication_metadata = publication_metadata_dictionary[
            fixtures.PROQUEST_RAW_PUBLICATION_ID
        ]

        eq_(1, len(publication_metadata.links))

        [full_cover_link] = publication_metadata.links
        eq_(True, isinstance(full_cover_link, LinkData))
        eq_(fixtures.PROQUEST_RAW_PUBLICATION_COVER_HREF, full_cover_link.href)
        eq_(Hyperlink.IMAGE, full_cover_link.rel)

        thumbnail_cover_link = full_cover_link.thumbnail
        eq_(True, isinstance(thumbnail_cover_link, LinkData))
        eq_(fixtures.PROQUEST_RAW_PUBLICATION_COVER_HREF, thumbnail_cover_link.href)
        eq_(Hyperlink.THUMBNAIL_IMAGE, thumbnail_cover_link.rel)


class TestProQuestOPDS2ImportMonitor(DatabaseTest):
    def setup(self, mock_search=True):
        super(TestProQuestOPDS2ImportMonitor, self).setup()

        self._proquest_data_source = DataSource.lookup(
            self._db, DataSource.PROQUEST, autocreate=True
        )
        self._proquest_collection = self._collection(
            protocol=ExternalIntegration.PROQUEST
        )
        self._proquest_collection.external_integration.set_setting(
            Collection.DATA_SOURCE_NAME_SETTING, DataSource.PROQUEST
        )

    @parameterized.expand(
        [
            ("no_pages", [], []),
            (
                "one_page",
                [fixtures.PROQUEST_FEED_PAGE_1],
                [call(fixtures.PROQUEST_FEED_PAGE_1)],
            ),
            (
                "two_pages",
                [fixtures.PROQUEST_FEED_PAGE_1, fixtures.PROQUEST_FEED_PAGE_2],
                [
                    call(fixtures.PROQUEST_FEED_PAGE_1),
                    call(fixtures.PROQUEST_FEED_PAGE_2),
                ],
            ),
        ]
    )
    def test_monitor_correctly_processes_pages(self, _, feeds, expected_calls):
        """This test makes sure that ProQuestOPDS2ImportMonitor correctly processes
        any response returned by ProQuestAPIClient.download_all_feed_pages without having any prior CoverageRecords.

        :param feeds: List of ProQuest OPDS 2.0 paged feeds
        :type feeds: List[webpub_manifest_parser.opds2.ast.OPDS2Feed]

        :param expected_calls: List of expected ProQuestOPDS2ImportMonitor.import_one_feed calls
        :type expected_calls: List[call]
        """
        # Arrange
        client = create_autospec(spec=ProQuestAPIClient)
        client.download_all_feed_pages = MagicMock(return_value=feeds)

        client_factory = create_autospec(spec=ProQuestAPIClientFactory)
        client_factory.create = MagicMock(return_value=client)

        monitor = ProQuestOPDS2ImportMonitor(
            client_factory, self._db, self._proquest_collection, ProQuestOPDS2Importer
        )
        monitor.import_one_feed = MagicMock(return_value=([], []))

        # Act
        monitor.run_once(False)

        # Assert
        # Make sure that ProQuestOPDS2ImportMonitor.import_one_feed was called for each paged feed (if any)
        monitor.import_one_feed.assert_has_calls(expected_calls)

    def test_monitor_correctly_does_not_process_already_processed_pages(self):
        """This test makes sure that the monitor has a short circuit breaker
        which allows to not process already processed feeds.

        The feed contains two pages:
        - page # 1: publication # 1 and publication # 2
        - page # 2: publication # 3 and publication # 4

        Publication # 2, 3, and 4 were already processed and have coverage records.
        Publication # 1 is a new one and doesn't have a coverage record.
        It means the monitor must process the whole page # 1.
        """
        # Arrange
        # There are two pages: page # 1 and page # 2
        feeds = [fixtures.PROQUEST_FEED_PAGE_1, fixtures.PROQUEST_FEED_PAGE_2]
        # But only the page # 1 will be processed
        expected_calls = [call(fixtures.PROQUEST_FEED_PAGE_1)]

        identifier_parser = ProQuestIdentifierParser()

        # Create Identifiers for publications # 2, 3, and 4
        publication_2_identifier, _ = identifier, _ = Identifier.parse(
            self._db,
            fixtures.PROQUEST_PUBLICATION_2.metadata.identifier,
            identifier_parser,
        )
        publication_3_identifier, _ = identifier, _ = Identifier.parse(
            self._db,
            fixtures.PROQUEST_PUBLICATION_3.metadata.identifier,
            identifier_parser,
        )
        publication_4_identifier, _ = identifier, _ = Identifier.parse(
            self._db,
            fixtures.PROQUEST_PUBLICATION_4.metadata.identifier,
            identifier_parser,
        )

        # Make sure that all the publications # 2, 3, and 4 were already processed
        max_modified_date = max(
            fixtures.PROQUEST_PUBLICATION_2.metadata.modified,
            fixtures.PROQUEST_PUBLICATION_3.metadata.modified,
            fixtures.PROQUEST_PUBLICATION_4.metadata.modified,
        )
        coverage_date = max_modified_date + datetime.timedelta(days=1)

        # Create coverage records for publications # 2, 3, and 4
        CoverageRecord.add_for(
            publication_2_identifier,
            self._proquest_data_source,
            operation=CoverageRecord.IMPORT_OPERATION,
            timestamp=coverage_date,
        )
        CoverageRecord.add_for(
            publication_3_identifier,
            self._proquest_data_source,
            operation=CoverageRecord.IMPORT_OPERATION,
            timestamp=coverage_date,
        )
        CoverageRecord.add_for(
            publication_4_identifier,
            self._proquest_data_source,
            operation=CoverageRecord.IMPORT_OPERATION,
            timestamp=coverage_date,
        )

        client = create_autospec(spec=ProQuestAPIClient)
        client.download_all_feed_pages = MagicMock(return_value=feeds)

        client_factory = create_autospec(spec=ProQuestAPIClientFactory)
        client_factory.create = MagicMock(return_value=client)

        monitor = ProQuestOPDS2ImportMonitor(
            client_factory, self._db, self._proquest_collection, ProQuestOPDS2Importer
        )
        monitor.import_one_feed = MagicMock(return_value=([], []))

        # Act
        monitor.run_once(False)

        # Assert
        # Make sure that ProQuestOPDS2ImportMonitor.import_one_feed was called only for the page # 1
        monitor.import_one_feed.assert_has_calls(expected_calls)
