import json

import requests_mock
from mock import MagicMock, create_autospec
from nose.tools import assert_raises, eq_
from parameterized import parameterized
from requests import HTTPError

from api.proquest.client import (
    Book,
    ProQuestAPIClient,
    ProQuestAPIClientConfiguration,
    ProQuestAPIInvalidJSONResponseError,
    ProQuestAPIMissingJSONPropertyError,
)
from api.util.url import URLUtility
from core.model import ExternalIntegration
from core.model.configuration import (
    ConfigurationFactory,
    ConfigurationStorage,
    HasExternalIntegration,
)
from core.testing import DatabaseTest

BOOKS_CATALOG_SERVICE_URL = "https://proquest.com/lib/nyulibrary-ebooks/BooksCatalog"
PARTNER_AUTH_TOKEN_SERVICE_URL = (
    "https://proquest.com/lib/nyulibrary-ebooks/PartnerAuthToken"
)
DOWNLOAD_LINK_SERVICE_URL = "https://proquest.com/lib/nyulibrary-ebooks/DownloadLink"


class TestProQuestAPIClient(DatabaseTest):
    def setup(self, mock_search=True):
        super(TestProQuestAPIClient, self).setup()

        self._proquest_collection = self._collection(
            protocol=ExternalIntegration.PROQUEST
        )
        self._integration = self._proquest_collection.external_integration
        integration_owner = create_autospec(spec=HasExternalIntegration)
        integration_owner.external_integration = MagicMock(
            return_value=self._integration
        )
        self._configuration_storage = ConfigurationStorage(integration_owner)
        self._configuration_factory = ConfigurationFactory()
        self._client = ProQuestAPIClient(
            self._configuration_storage, self._configuration_factory
        )

    @parameterized.expand(
        [
            ("in_the_case_of_http_error_status_code", {"status_code": 401}, HTTPError),
            (
                "in_the_case_of_non_json_response",
                {"text": "garbage"},
                ProQuestAPIInvalidJSONResponseError,
            ),
            (
                "when_json_document_does_not_contain_status_code",
                {"json": {"feed": ""}},
                ProQuestAPIMissingJSONPropertyError,
            ),
            (
                "json_document_contains_error_status_code",
                {"json": {ProQuestAPIClient.RESPONSE_STATUS_CODE_FIELD: 401}},
                HTTPError,
            ),
            (
                "json_document_does_not_contain_opds_feed",
                {"json": {ProQuestAPIClient.RESPONSE_STATUS_CODE_FIELD: 200}},
                ProQuestAPIMissingJSONPropertyError,
            ),
        ]
    )
    def test_download_feed_page_correctly_fails(
        self, _, response_arguments, expected_exception_class
    ):
        # Arrange
        page = 1
        hits_per_page = 10
        books_catalog_service_url = URLUtility.build_url(
            BOOKS_CATALOG_SERVICE_URL, {"page": page, "hitsPerPage": hits_per_page}
        )

        with self._configuration_factory.create(
            self._configuration_storage, self._db, ProQuestAPIClientConfiguration
        ) as configuration:
            configuration.books_catalog_service_url = BOOKS_CATALOG_SERVICE_URL

        with requests_mock.Mocker() as request_mock:
            request_mock.get(books_catalog_service_url, **response_arguments)

            # Act
            with assert_raises(expected_exception_class):
                self._client.download_feed_page(self._db, page, hits_per_page)

    def test_download_feed_page_successfully_extracts_feed_from_correct_response(self):
        # Arrange
        page = 1
        hits_per_page = 10
        books_catalog_service_url = URLUtility.build_url(
            BOOKS_CATALOG_SERVICE_URL, {"page": page, "hitsPerPage": hits_per_page}
        )
        expected_feed = json.dumps({})
        response = {
            ProQuestAPIClient.RESPONSE_STATUS_CODE_FIELD: 200,
            ProQuestAPIClient.RESPONSE_OPDS_FEED_FIELD: expected_feed,
        }

        with self._configuration_factory.create(
            self._configuration_storage, self._db, ProQuestAPIClientConfiguration
        ) as configuration:
            configuration.books_catalog_service_url = BOOKS_CATALOG_SERVICE_URL

        with requests_mock.Mocker() as request_mock:
            request_mock.get(books_catalog_service_url, json=response)

            # Act
            feed = self._client.download_feed_page(self._db, page, hits_per_page)

            # Assert
            eq_(expected_feed, feed)

    @parameterized.expand(
        [
            (
                "in_the_case_of_http_error_status_code",
                {"status_code": 401},
            ),
            (
                "in_the_case_of_non_json_response",
                {"text": "garbage"},
            ),
            (
                "when_json_document_does_not_contain_status_code",
                {"json": {"feed": ""}},
            ),
            (
                "json_document_contains_error_status_code",
                {"json": {ProQuestAPIClient.RESPONSE_STATUS_CODE_FIELD: 401}},
            ),
            (
                "json_document_does_not_contain_opds_feed",
                {"json": {ProQuestAPIClient.RESPONSE_STATUS_CODE_FIELD: 200}},
            ),
        ]
    )
    def test_download_all_feed_pages_correctly_stops(
        self, _, last_page_response_arguments
    ):
        # Arrange
        page_size = ProQuestAPIClient.MAX_PAGE_SIZE
        books_catalog_service_url_1 = URLUtility.build_url(
            BOOKS_CATALOG_SERVICE_URL, {"page": 1, "hitsPerPage": page_size}
        )
        books_catalog_service_url_2 = URLUtility.build_url(
            BOOKS_CATALOG_SERVICE_URL, {"page": 2, "hitsPerPage": page_size}
        )
        books_catalog_service_url_3 = URLUtility.build_url(
            BOOKS_CATALOG_SERVICE_URL, {"page": 3, "hitsPerPage": page_size}
        )
        expected_feed_1 = {"metadata": {"title": "Page 1"}}
        expected_feed_2 = {"metadata": {"title": "Page 2"}}
        expected_response_1 = {
            ProQuestAPIClient.RESPONSE_STATUS_CODE_FIELD: 200,
            ProQuestAPIClient.RESPONSE_OPDS_FEED_FIELD: expected_feed_1,
        }
        expected_response_2 = {
            ProQuestAPIClient.RESPONSE_STATUS_CODE_FIELD: 200,
            ProQuestAPIClient.RESPONSE_OPDS_FEED_FIELD: expected_feed_2,
        }

        with self._configuration_factory.create(
            self._configuration_storage, self._db, ProQuestAPIClientConfiguration
        ) as configuration:
            configuration.books_catalog_service_url = BOOKS_CATALOG_SERVICE_URL
            configuration.page_size = page_size

        with requests_mock.Mocker() as request_mock:
            request_mock.get(books_catalog_service_url_1, json=expected_response_1)
            request_mock.get(books_catalog_service_url_2, json=expected_response_2)
            request_mock.get(
                books_catalog_service_url_3, **last_page_response_arguments
            )

            # Act
            feeds = self._client.download_all_feed_pages(self._db)

            # Assert
            eq_([expected_feed_1, expected_feed_2], list(feeds))

    @parameterized.expand(
        [
            ("in_the_case_of_http_error_status_code", {"status_code": 401}, HTTPError),
            (
                "in_the_case_of_non_json_response",
                {"text": "garbage"},
                ProQuestAPIInvalidJSONResponseError,
            ),
            (
                "when_json_document_does_not_contain_status_code",
                {"json": {"dummy": ""}},
                ProQuestAPIMissingJSONPropertyError,
            ),
            (
                "json_document_contains_error_status_code",
                {"json": {ProQuestAPIClient.RESPONSE_STATUS_CODE_FIELD: 401}},
                HTTPError,
            ),
            (
                "json_document_does_not_contain_token",
                {"json": {ProQuestAPIClient.RESPONSE_STATUS_CODE_FIELD: 200}},
                ProQuestAPIMissingJSONPropertyError,
            ),
        ]
    )
    def test_create_token_correctly_fails(
        self, _, response_arguments, expected_exception_class
    ):
        # Arrange
        affiliation_id = "1"
        partner_auth_token_service_url = URLUtility.build_url(
            PARTNER_AUTH_TOKEN_SERVICE_URL, {"userName": affiliation_id}
        )

        with self._configuration_factory.create(
            self._configuration_storage, self._db, ProQuestAPIClientConfiguration
        ) as configuration:
            configuration.partner_auth_token_service_url = (
                PARTNER_AUTH_TOKEN_SERVICE_URL
            )

        with requests_mock.Mocker() as request_mock:
            request_mock.get(partner_auth_token_service_url, **response_arguments)

            # Act
            with assert_raises(expected_exception_class):
                self._client.create_token(self._db, affiliation_id)

    def test_create_token_correctly_extracts_token(self):
        # Arrange
        affiliation_id = "1"
        partner_auth_token_service_url = URLUtility.build_url(
            PARTNER_AUTH_TOKEN_SERVICE_URL, {"userName": affiliation_id}
        )
        expected_token = "12345"
        response = {
            ProQuestAPIClient.RESPONSE_STATUS_CODE_FIELD: 200,
            ProQuestAPIClient.TOKEN_FIELD: expected_token,
        }

        with self._configuration_factory.create(
            self._configuration_storage, self._db, ProQuestAPIClientConfiguration
        ) as configuration:
            configuration.partner_auth_token_service_url = (
                PARTNER_AUTH_TOKEN_SERVICE_URL
            )

        with requests_mock.Mocker() as request_mock:
            request_mock.get(partner_auth_token_service_url, json=response)

            # Act
            token = self._client.create_token(self._db, affiliation_id)

            # Assert
            eq_(expected_token, token)

    @parameterized.expand(
        [
            ("in_the_case_of_http_error_status_code", {"status_code": 401}, HTTPError),
            (
                "when_json_document_does_not_contain_status_code",
                {"json": {"dummy": ""}},
                ProQuestAPIMissingJSONPropertyError,
            ),
            (
                "json_document_contains_error_status_code",
                {"json": {ProQuestAPIClient.RESPONSE_STATUS_CODE_FIELD: 401}},
                HTTPError,
            ),
            (
                "json_document_does_not_contain_download_link",
                {"json": {ProQuestAPIClient.RESPONSE_STATUS_CODE_FIELD: 200}},
                ProQuestAPIMissingJSONPropertyError,
            ),
        ]
    )
    def test_get_book_correctly_fails(
        self, _, response_arguments, expected_exception_class
    ):
        # Arrange
        token = "12345"
        document_id = "12345"
        download_link_service_url = URLUtility.build_url(
            DOWNLOAD_LINK_SERVICE_URL, {"docID": document_id}
        )

        with self._configuration_factory.create(
            self._configuration_storage, self._db, ProQuestAPIClientConfiguration
        ) as configuration:
            configuration.download_link_service_url = DOWNLOAD_LINK_SERVICE_URL

        with requests_mock.Mocker() as request_mock:
            request_mock.get(download_link_service_url, **response_arguments)

            # Act
            with assert_raises(expected_exception_class):
                self._client.get_book(self._db, token, document_id)

    def test_get_book_correctly_extracts_open_access_books(self):
        # Arrange
        book_content = "PDF Book12345"
        response_arguments = {"content": book_content}
        expected_open_access_book = Book(content=bytes(book_content))

        token = "12345"
        document_id = "12345"
        download_link_service_url = URLUtility.build_url(
            DOWNLOAD_LINK_SERVICE_URL, {"docID": document_id}
        )

        with self._configuration_factory.create(
            self._configuration_storage, self._db, ProQuestAPIClientConfiguration
        ) as configuration:
            configuration.download_link_service_url = DOWNLOAD_LINK_SERVICE_URL

        with requests_mock.Mocker() as request_mock:
            request_mock.get(download_link_service_url, **response_arguments)

            # Act
            book = self._client.get_book(self._db, token, document_id)

            # Assert
            eq_(expected_open_access_book, book)
            eq_(type(expected_open_access_book), type(book))

    def test_get_book_correctly_extracts_acsm_books(
        self,
    ):
        # Arrange
        acsm_file_content = """<fulfillmentToken fulfillmentType="loan" auth="user" xmlns="http://ns.adobe.com/adept">
                <distributor>urn:uuid:9cb786e8-586a-4950-8901-fff8d2ee6025</distributor>
            </fulfillmentToken
"""
        download_link = "https://proquest.com/fulfill?documentID=12345"
        expected_acsm_book = Book(content=bytes(acsm_file_content))

        first_response_arguments = {
            "json": {
                ProQuestAPIClient.RESPONSE_STATUS_CODE_FIELD: 200,
                ProQuestAPIClient.DOWNLOAD_LINK_FIELD: download_link,
            }
        }
        second_response_arguments = {"content": acsm_file_content}

        token = "12345"
        document_id = "12345"
        download_link_service_url = URLUtility.build_url(
            DOWNLOAD_LINK_SERVICE_URL, {"docID": document_id}
        )

        with self._configuration_factory.create(
            self._configuration_storage, self._db, ProQuestAPIClientConfiguration
        ) as configuration:
            configuration.download_link_service_url = DOWNLOAD_LINK_SERVICE_URL

        with requests_mock.Mocker() as request_mock:
            request_mock.get(download_link_service_url, **first_response_arguments)
            request_mock.get(download_link, **second_response_arguments)

            # Act
            book = self._client.get_book(self._db, token, document_id)

            # Assert
            eq_(expected_acsm_book, book)
            eq_(type(expected_acsm_book), type(book))
