import logging
from contextlib import contextmanager

import requests
from flask_babel import lazy_gettext as _
from requests import HTTPError, Request

from core.exceptions import BaseError
from core.model import DeliveryMechanism
from core.model.configuration import (
    ConfigurationAttributeType,
    ConfigurationFactory,
    ConfigurationGrouping,
    ConfigurationMetadata,
    ConfigurationStorage,
)
from core.util import is_session


class ProQuestAPIClientConfiguration(ConfigurationGrouping):
    """Contains configuration settings of ProQuest API client."""

    DEFAULT_PAGE_SIZE = 5000

    books_catalog_service_url = ConfigurationMetadata(
        key="books_catalog_service_url",
        label=_("BooksCatalog service's URL"),
        description=_("URL of the BooksCatalog service endpoint"),
        type=ConfigurationAttributeType.TEXT,
        required=True,
    )

    page_size = ConfigurationMetadata(
        key="page_size",
        label=_("Feed page's size"),
        description=_(
            "This value determines how many publications "
            "will be on a single page fetched from the BooksCatalog service."
        ),
        type=ConfigurationAttributeType.NUMBER,
        required=False,
        default=DEFAULT_PAGE_SIZE,
    )

    partner_auth_token_service_url = ConfigurationMetadata(
        key="partner_auth_token_service_url",
        label=_("PartnerAuthToken service's URL"),
        description=_("URL of the PartnerAuthToken service endpoint."),
        type=ConfigurationAttributeType.TEXT,
        required=True,
    )

    download_link_service_url = ConfigurationMetadata(
        key="download_link_service_url",
        label=_("DownloadLink service's URL"),
        description=_("URL of the DownloadLink service endpoint."),
        type=ConfigurationAttributeType.TEXT,
        required=True,
    )

    http_proxy_url = ConfigurationMetadata(
        key="http_proxy_url",
        label=_("HTTP proxy's URL"),
        description=_("URL of the proxy handling HTTP traffic."),
        type=ConfigurationAttributeType.TEXT,
        required=False,
    )

    https_proxy_url = ConfigurationMetadata(
        key="https_proxy_url",
        label=_("HTTPS proxy's URL"),
        description=_("URL of the proxy handling HTTPS traffic."),
        type=ConfigurationAttributeType.TEXT,
        required=False,
    )


class ProQuestAPIInvalidJSONResponseError(BaseError):
    """Raised when the client receives from ProQuest API a response with incorrect JSON document."""

    def __init__(self, response):
        """Initialize a new instance of ProQuestAPIIncorrectResponseError class.

        :param response: Response object
        :type response: requests.models.Response
        """
        super(ProQuestAPIInvalidJSONResponseError, self).__init__(
            "Response body does not contain a valid JSON document"
        )

        self._response = response

    @property
    def response(self):
        """Return the response associated with this error.

        :return: Response associated with this error
        :rtype: requests.models.Response
        """
        return self._response


class ProQuestAPIMissingJSONPropertyError(ProQuestAPIInvalidJSONResponseError):
    """Raised when the client receives from ProQuest API a response with incorrect JSON document."""

    def __init__(self, response, missing_property):
        """Initialize a new instance of ProQuestAPIMissingJSONPropertyError class.

        :param response: Response object
        :type response: requests.models.Response

        :param missing_property: Name of the missing property
        :type missing_property: str
        """
        super(ProQuestAPIInvalidJSONResponseError, self).__init__(
            "JSON document does not contain required property '{0}'".format(
                missing_property
            ),
            response,
        )

        self._missing_property = missing_property

    @property
    def missing_property(self):
        """Return the name of the missing property.

        :return: Name of the missing property
        :rtype: str
        """
        return self._missing_property


class ProQuestBook(object):
    """POCO class containing information about a ProQuest book."""

    def __init__(self, link=None, content=None, content_type=None):
        """Initialize a new instance of ProQuestBook class.

        :param link: Book's link
        :type link: Optional[str]

        :param content: Book's content
        :type content: Optional[bytes]

        :param content_type: Content type
        :type content_type: Optional[str]
        """
        if link is not None and not isinstance(link, str):
            raise ValueError("Argument 'link' must be a string")
        if content is not None and not isinstance(content, bytes):
            raise ValueError("Argument 'content' must be a bytes string")
        if content_type is not None and not isinstance(content_type, str):
            raise ValueError("Argument 'content_type' must be a string")
        if link is not None and content is not None:
            raise ValueError(
                "'link' and 'content' cannot be both set up at the same time"
            )

        self._link = link
        self._content = content
        self._content_type = content_type

    def __eq__(self, other):
        """Compare self and other other book.

        :param other: Other book instance
        :type other: Any

        :return: Boolean value indicating whether self and other are equal to each to other
        :rtype: bool
        """
        if not isinstance(other, ProQuestBook):
            return False

        return (
            self.link == other.link
            and self.content == other.content
            and self.content_type == other.content_type
        )

    @property
    def link(self):
        """Return the book's link.

        :return: Book's link
        :rtype: Optional[str]
        """
        return self._link

    @property
    def content(self):
        """Return the book's content.

        :return: Book's content
        :rtype: Optional[Union[str, bytes]]
        """
        return self._content

    @property
    def content_type(self):
        """Return the content type.

        :return: Content type
        :rtype: Optional[str]
        """
        return self._content_type


class ProQuestAPIClient(object):
    """ProQuest API client."""

    MAX_PAGE_INDEX = 32766
    MAX_PAGE_SIZE = 32766

    RESPONSE_STATUS_CODE_FIELD = "statusCode"
    RESPONSE_OPDS_FEED_FIELD = "opdsFeed"
    TOKEN_FIELD = "token"
    DOWNLOAD_LINK_FIELD = "downloadLink"
    DRM_FREE_DOWNLOAD_LINK_KEYWORD = "getDrmFreeFile"

    SUCCESS_STATUS_CODE = 200

    def __init__(self, configuration_storage, configuration_factory):
        """Initialize a new instance of ProQuestAPIClient class.

        :param configuration_storage: ConfigurationStorage object
        :type configuration_storage: core.model.configuration.ConfigurationStorage

        :param configuration_factory: Factory creating ProQuestAPIClientConfiguration instance
        :type configuration_factory: core.model.configuration.ConfigurationFactory
        """
        self._configuration_storage = configuration_storage
        self._configuration_factory = configuration_factory

        self._logger = logging.getLogger(__name__)

    @contextmanager
    def _get_configuration(self, db):
        """Return the configuration object.

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :return: Configuration object
        :rtype: ProQuestAPIClientConfiguration
        """
        with self._configuration_factory.create(
            self._configuration_storage, db, ProQuestAPIClientConfiguration
        ) as configuration:
            yield configuration

    def _get_request_headers(self, token):
        headers = {"Content-Type": "application/json"}

        if token:
            headers["Authorization"] = "Bearer {0}".format(token)

        self._logger.debug("Headers: {0}".format(headers))

        return headers

    def _get_request_proxies(self, configuration):
        proxies = {}
        if configuration.http_proxy_url:
            proxies["http"] = configuration.http_proxy_url
        if configuration.https_proxy_url:
            proxies["https"] = configuration.https_proxy_url

        self._logger.debug("Proxies: {0}".format(proxies))

        return proxies

    def _create_request(self, method, url, query_parameters, token=None):
        """Create a new HTTP request.

        :param method: HTTP method
        :type method: str

        :param url: Target URL
        :type url: str

        :param query_parameters: Dictionary containing query parameters
        :type query_parameters: Dict

        :param token: Optional JWT token to be put in the Authorization header
        :type token: Optional[str]

        :return: Response object
        :rtype: requests.models.Response
        """
        self._logger.debug("Started creating a new request")

        headers = self._get_request_headers(token)
        request = Request(method, url, params=query_parameters, headers=headers)

        self._logger.debug(
            "Finished creating a new request: {0} ({1})".format(request, request.url)
        )

        return request

    @staticmethod
    def _try_to_extract_json_from_response(response):
        """Try to extract a JSON document from the response.

        NOTE: DownloadLink service doesn't always return a JSON document.
        For open-access books it returns the book content.

        :param response: Response object
        :type response: requests.models.Response

        :return: JSON document containing in the response (if any)
        :rtype: Optional[Dict]
        """
        try:
            response_json = response.json()

            return response_json
        except ValueError:
            return None

    def _parse_response(self, response, must_be_json=False):
        """Parse the response and return a JSON document containing in it.

        :param response: Response object
        :type response: requests.models.Response

        :param must_be_json: Boolean value specifying whether the response must contain a valid JSON document
        :type must_be_json: bool

        :return: 2-tuple containing the response and the JSON document containing in it (if any)
        :rtype: Tuple[requests.models.Response, Optional[Dict]]
        """
        response_json = self._try_to_extract_json_from_response(response)

        if response.status_code != requests.codes.ok and response_json:
            self._logger.error("Request failed: {0}".format(response_json))

        response.raise_for_status()

        if not response_json:
            if must_be_json:
                raise ProQuestAPIInvalidJSONResponseError(response)

            return response, None

        if self.RESPONSE_STATUS_CODE_FIELD not in response_json:
            raise ProQuestAPIMissingJSONPropertyError(
                response, self.RESPONSE_STATUS_CODE_FIELD
            )

        status_code = response_json[self.RESPONSE_STATUS_CODE_FIELD]

        if status_code != requests.codes.ok:
            raise HTTPError(
                "Request failed with {0} code".format(status_code), response=response
            )

        return response, response_json

    def _send_request(
        self,
        configuration,
        method,
        url,
        query_parameters,
        token=None,
        response_must_be_json=False,
    ):
        """Send an HTTP requests, check the result code and return the response.

        :param configuration: Configuration object
        :type configuration: ProQuestAPIClientConfiguration

        :param method: HTTP method
        :type method: str

        :param url: Target URL
        :type url: str

        :param query_parameters: Dictionary containing query parameters
        :type query_parameters: Dict

        :param token: Optional JWT token to be put in the Authorization header
        :type token: Optional[str]

        :param response_must_be_json: Boolean value specifying whether the response must contain a valid JSON document
        :type response_must_be_json: bool

        :return: 2-tuple containing the response and the JSON document containing in it (if any)
        :rtype: Tuple[requests.models.Response, Optional[Dict]]
        """
        self._logger.debug(
            "Started sending {0} HTTP request to {1} with the following parameters: {2}".format(
                method, url, query_parameters
            )
        )

        request = self._create_request(method, url, query_parameters, token)
        proxies = self._get_request_proxies(configuration)

        with requests.sessions.Session() as session:
            request = session.prepare_request(request)
            response = session.send(request, proxies=proxies)

        self._logger.debug("Received the following response: {0}".format(response))

        response, response_json = self._parse_response(response, response_must_be_json)

        self._logger.debug(
            "Finished sending {0} HTTP request to {1} with the following parameters: {2}".format(
                method, url, query_parameters
            )
        )

        return response, response_json

    def _download_feed_page(self, configuration, page, hits_per_page):
        """Download a single page of a paginated OPDS 2.0 feed.

        :param configuration: Configuration object
        :type configuration: ProQuestAPIClientConfiguration

        :param page: Page index (max = 32,767)
        :type page: int

        :param hits_per_page: Number of publications on a single page (max = 32,767)
        :type hits_per_page: int

        :return: Python dictionary object containing the feed's page
        :rtype: dict
        """
        self._logger.info(
            "Started downloading page # {0} ({1} hits) of a paginated OPDS 2.0 feed from {2}".format(
                page, hits_per_page, configuration.books_catalog_service_url
            )
        )

        parameters = {"page": page, "hitsPerPage": hits_per_page}
        response, response_json = self._send_request(
            configuration,
            "get",
            configuration.books_catalog_service_url,
            parameters,
            response_must_be_json=True,
        )

        self._logger.info(
            "Finished downloading page # {0} ({1} hits) of a paginated OPDS 2.0 feed from {2}".format(
                page, hits_per_page, configuration.books_catalog_service_url
            )
        )

        if self.RESPONSE_OPDS_FEED_FIELD not in response_json:
            raise ProQuestAPIMissingJSONPropertyError(
                response, self.RESPONSE_OPDS_FEED_FIELD
            )

        return response_json[self.RESPONSE_OPDS_FEED_FIELD]

    def download_feed_page(self, db, page, hits_per_page):
        """Download a single page of a paginated OPDS 2.0 feed.

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param page: Page index (max = 32,766)
        :type page: int

        :param hits_per_page: Number of publications on a single page (max = 32,766)
        :type hits_per_page: int

        :return: Python dictionary object containing the feed's page
        :rtype: dict
        """
        if not is_session(db):
            raise ValueError('"db" argument must be a valid SQLAlchemy session')
        if not isinstance(page, int):
            raise ValueError('"page" argument must be an integer')
        if page < 0 or page > self.MAX_PAGE_INDEX:
            raise ValueError(
                "Page argument must a non-negative number less than {0}".format(
                    self.MAX_PAGE_INDEX
                )
            )
        if not isinstance(hits_per_page, int):
            raise ValueError('"hits_per_page" argument must be an integer')
        if hits_per_page < 0 or hits_per_page > self.MAX_PAGE_SIZE:
            raise ValueError(
                "Hits per page argument must a non-negative number less than {0}".format(
                    self.MAX_PAGE_SIZE
                )
            )

        self._logger.info(
            "Started downloading page # {0} ({1} hits) of a paginated OPDS 2.0 feed ".format(
                page, hits_per_page
            )
        )

        with self._get_configuration(db) as configuration:
            feed = self._download_feed_page(configuration, page, hits_per_page)

            self._logger.info(
                "Finished downloading page # {0} ({1} hits) of a paginated OPDS 2.0 feed".format(
                    page, hits_per_page
                )
            )

            return feed

    def download_all_feed_pages(self, db):
        """Download all available feed pages.

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :return: Iterable list of feed pages in a form of Python dictionaries
        :rtype: Iterable[dict]
        """
        if not is_session(db):
            raise ValueError('"db" argument must be a valid SQLAlchemy session')

        self._logger.info(
            "Started downloading all of the pages of a paginated OPDS 2.0 feed"
        )

        with self._get_configuration(db) as configuration:
            page = 1

            while True:
                try:
                    feed = self._download_feed_page(
                        configuration, page, configuration.page_size
                    )
                    page += 1

                    yield feed
                except HTTPError as error:
                    self._logger.debug(
                        "Got an HTTP error {0}, assuming we reached the end of the feed".format(
                            error
                        )
                    )
                    break
                except ProQuestAPIInvalidJSONResponseError:
                    self._logger.exception(
                        "Got unexpected ProQuestAPIIncorrectResponseError, assuming we reached the end of the feed"
                    )
                    break

        self._logger.info(
            "Finished downloading all of the pages of a paginated OPDS 2.0 feed"
        )

    def create_token(self, db, affiliation_id):
        """Create a new JWT bearer token.

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param affiliation_id: SAML affiliation ID used as a patron's unique identifier by ProQuest
        :type affiliation_id: str

        :return: New JWT bearer token
        :rtype: str
        """
        if not is_session(db):
            raise ValueError('"db" argument must be a valid SQLAlchemy session')
        if not affiliation_id or not isinstance(affiliation_id, str):
            raise ValueError('"affiliation_id" argument must be a non-empty string')

        self._logger.info(
            "Started creating a new JWT bearer token for affiliation ID {0}".format(
                affiliation_id
            )
        )

        with self._get_configuration(db) as configuration:
            parameters = {"userName": affiliation_id}
            response, response_json = self._send_request(
                configuration,
                "get",
                configuration.partner_auth_token_service_url,
                parameters,
                response_must_be_json=True,
            )

            self._logger.info(
                "Finished creating a new JWT bearer token for affiliation ID {0}: {1}".format(
                    affiliation_id, response_json
                )
            )

            if self.TOKEN_FIELD not in response_json:
                raise ProQuestAPIMissingJSONPropertyError(response, self.TOKEN_FIELD)

            return response_json[self.TOKEN_FIELD]

    def get_book(self, db, token, document_id):
        """Get a book by it's ProQuest Doc ID.

        NOTE: There are two different cases to consider:
        - Open-access books: in this case ProQuest API returns the book content.
        - Adobe DRM copy protected books: in this case ProQuest API returns an ACSM file containing
        information about downloading a digital publication.

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param token: JWT bearer token created using `ProQuestAPIClient.create_token` method
        :type token: str

        :param document_id: ProQuest Doc ID
        :type document_id: str

        :return: Book instance containing either an ACS link to the book or the book content
        :rtype: ProQuestBook
        """
        if not is_session(db):
            raise ValueError('"db" argument must be a valid SQLAlchemy session')
        if not token or not isinstance(token, str):
            raise ValueError('"token" argument must be a non-empty string')
        if not document_id or not isinstance(document_id, str):
            raise ValueError('"document_id" must be a non-empty string')

        self._logger.info(
            "Started fetching a book link for Doc ID {0} using JWT token {1}".format(
                document_id, token
            )
        )

        with self._get_configuration(db) as configuration:
            parameters = {"docID": document_id}
            response, response_json = self._send_request(
                configuration,
                "get",
                configuration.download_link_service_url,
                parameters,
                token,
            )

            if response_json:
                self._logger.info(
                    "Finished fetching a download link for Doc ID {0} using JWT token {1}: {2}".format(
                        document_id, token, response_json
                    )
                )

                if self.DOWNLOAD_LINK_FIELD not in response_json:
                    raise ProQuestAPIMissingJSONPropertyError(
                        response, self.DOWNLOAD_LINK_FIELD
                    )

                # The API returns another link leading to either a DRM-free book or ACSM file:
                # - DRM-free books are publicly accessible, meaning that their download links
                #   are not protected by IP whitelisting and we shall pass the link to the client
                #   to avoid proxying the content through Circulation Manager.
                # - DRM-protected download links are protected by IP whitelisting
                #   and can be called only from Circulation Manager,
                #   meaning that Circulation Manager has to download an ACSM file
                #   and proxy it to the client.
                #   However, it shouldn't incur any bad consequences because
                #   ACSM files are usually relatively small.
                link = response_json[self.DOWNLOAD_LINK_FIELD]

                # In the case of DRM-free books we return a link immediately
                # and we'll pass it to the client app.
                if self.DRM_FREE_DOWNLOAD_LINK_KEYWORD in link:
                    return ProQuestBook(link=link)

                # In the case of Adobe DRM-protected books we have to download an ACSM file
                # and pass its content to the client app.
                response, _ = self._send_request(
                    configuration, "get", link, {}, token, response_must_be_json=False
                )

                self._logger.info(
                    "Finished fetching an ACSM file for Doc ID {0} using JWT token {1}".format(
                        document_id, token
                    )
                )

                return ProQuestBook(
                    content=bytes(response.content),
                    content_type=DeliveryMechanism.ADOBE_DRM,
                )
            else:
                self._logger.info(
                    "Finished fetching an open-access book for Doc ID {0} using JWT token {1}".format(
                        document_id, token
                    )
                )

                return ProQuestBook(content=bytes(response.content))


class ProQuestAPIClientFactory(object):
    """Factory used for creating ProQuestAPIClient instances."""

    def create(self, integration_association):
        """Create a new instance of ProQuestAPIClientFactory.

        :param integration_association: Association with an external integration
        :type integration_association: core.model.configuration.HasExternalIntegration

        :return: New instance of ProQuestAPIClient
        :rtype: ProQuestAPIClient
        """
        configuration_storage = ConfigurationStorage(integration_association)
        configuration_factory = ConfigurationFactory()
        client = ProQuestAPIClient(configuration_storage, configuration_factory)

        return client
