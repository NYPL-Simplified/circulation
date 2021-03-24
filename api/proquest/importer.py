import codecs
import datetime
import json
import logging
import shutil
import tempfile
from contextlib import contextmanager
from pdb import set_trace
import webpub_manifest_parser.opds2.ast as opds2_ast
from flask_babel import lazy_gettext as _
from requests import HTTPError
from sqlalchemy import or_
from webpub_manifest_parser.utils import encode

from api.circulation import BaseCirculationAPI, FulfillmentInfo, LoanInfo
from api.circulation_exceptions import CannotFulfill, CannotLoan
from api.proquest.client import ProQuestAPIClientConfiguration, ProQuestAPIClientFactory
from api.proquest.credential import ProQuestCredentialManager
from api.proquest.identifier import ProQuestIdentifierParser
from api.saml.metadata.model import SAMLAttributeType
from core.classifier import Classifier
from core.exceptions import BaseError
from core.metadata_layer import TimestampData
from core.model import (
    Collection,
    DeliveryMechanism,
    Hyperlink,
    Identifier,
    LicensePool,
    Loan,
    MediaTypes,
    get_one,
)
from core.model.configuration import (
    ConfigurationAttributeType,
    ConfigurationFactory,
    ConfigurationGrouping,
    ConfigurationMetadata,
    ConfigurationOption,
    ConfigurationStorage,
    ExternalIntegration,
    HasExternalIntegration,
)
from core.opds2_import import OPDS2Importer, OPDS2ImportMonitor, parse_feed
from core.opds_import import OPDSImporter

MISSING_AFFILIATION_ID = BaseError(
    _(
        "Patron does not have a SAML affiliation ID. "
        "It can mean either incorrect configuration of a SAML authentication provider or "
        "that patron has not yet been authenticated using the SAML authentication provider."
    )
)


def parse_identifier(db, identifier):
    """Parse the identifier and return an Identifier object representing it.

    :param db: Database session
    :type db: sqlalchemy.orm.session.Session

    :param identifier: String containing the identifier
    :type identifier: str

    :return: Identifier object
    :rtype: core.model.identifier.Identifier
    """
    identifier, _ = Identifier.parse(db, identifier, ProQuestIdentifierParser())

    return identifier


class CannotCreateProQuestTokenError(BaseError):
    def __init__(self, inner_exception):
        message = "{0}: {1}".format(
            _("Can not create a ProQuest JWT bearer token"),
            str(inner_exception),
        )

        super(CannotCreateProQuestTokenError, self).__init__(message, inner_exception)


class ProQuestOPDS2ImporterConfiguration(ConfigurationGrouping):
    """Contains configuration settings of ProQuestOPDS2Importer."""

    DEFAULT_TOKEN_EXPIRATION_TIMEOUT_SECONDS = 60 * 60
    TEST_AFFILIATION_ID = 1
    DEFAULT_AFFILIATION_ATTRIBUTES = (
        SAMLAttributeType.eduPersonPrincipalName.name,
        SAMLAttributeType.eduPersonScopedAffiliation.name,
    )

    data_source_name = ConfigurationMetadata(
        key=Collection.DATA_SOURCE_NAME_SETTING,
        label=_("Data source name"),
        description=_("Name of the data source associated with this collection."),
        type=ConfigurationAttributeType.TEXT,
        required=True,
        default="ProQuest",
    )

    token_expiration_timeout = ConfigurationMetadata(
        key="token_expiration_timeout",
        label=_("ProQuest JWT token's expiration timeout"),
        description=_(
            "Determines how long in seconds can a ProQuest JWT token be valid."
        ),
        type=ConfigurationAttributeType.NUMBER,
        required=False,
        default=DEFAULT_TOKEN_EXPIRATION_TIMEOUT_SECONDS,
    )

    affiliation_attributes = ConfigurationMetadata(
        key="affiliation_attributes",
        label=_("List of SAML attributes containing an affiliation ID"),
        description=_(
            "ProQuest integration assumes that the SAML provider is used for authentication. "
            "ProQuest JWT bearer tokens required by the most ProQuest API services "
            "are created based on the affiliation ID - SAML attribute uniquely identifying the patron."
            "This setting determines what attributes the ProQuest integration will use to look for affiliation IDs. "
            "The ProQuest integration will investigate the specified attributes sequentially "
            "and will take the first non-empty value."
        ),
        type=ConfigurationAttributeType.MENU,
        required=False,
        default=list(DEFAULT_AFFILIATION_ATTRIBUTES),
        options=[
            ConfigurationOption(attribute.name, attribute.name)
            for attribute in SAMLAttributeType
        ],
        format="narrow",
    )

    test_affiliation_id = ConfigurationMetadata(
        key="test_affiliation_id",
        label=_("Test SAML affiliation ID"),
        description=_(
            "Test SAML affiliation ID used for testing ProQuest API. "
            "Please contact ProQuest before using it."
        ),
        type=ConfigurationAttributeType.TEXT,
        required=False,
    )

    default_audience = ConfigurationMetadata(
        key=Collection.DEFAULT_AUDIENCE_KEY,
        label=_("Default audience"),
        description=_(
            "If ProQuest does not specify the target audience for their books, "
            "assume the books have this target audience."
        ),
        type=ConfigurationAttributeType.SELECT,
        required=False,
        default=OPDSImporter.NO_DEFAULT_AUDIENCE,
        options=[
            ConfigurationOption(
                key=OPDSImporter.NO_DEFAULT_AUDIENCE, label=_("No default audience")
            )
        ]
        + [
            ConfigurationOption(key=audience, label=audience)
            for audience in sorted(Classifier.AUDIENCES)
        ],
        format="narrow",
    )


class ProQuestOPDS2Importer(OPDS2Importer, BaseCirculationAPI, HasExternalIntegration):
    """Allows to import ProQuest OPDS 2.0 feeds into Circulation Manager."""

    NAME = ExternalIntegration.PROQUEST
    DESCRIPTION = _("Import books from a ProQuest OPDS 2.0 feed.")
    SETTINGS = (
        ProQuestOPDS2ImporterConfiguration.to_settings()
        + ProQuestAPIClientConfiguration.to_settings()
    )
    LIBRARY_SETTINGS = BaseCirculationAPI.LIBRARY_SETTINGS + [
        BaseCirculationAPI.DEFAULT_LOAN_DURATION_SETTING
    ]

    def __init__(
        self,
        db,
        collection,
        data_source_name=None,
        identifier_mapping=None,
        http_get=None,
        metadata_client=None,
        content_modifier=None,
        map_from_collection=None,
        mirrors=None,
    ):
        """Initialize a new instance of ProQuestOPDS2Importer class.

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param collection: Circulation Manager's collection.
            LicensePools created by this OPDS2Import class will be associated with the given Collection.
            If this is None, no LicensePools will be created -- only Editions.
        :type collection: Collection

        :param data_source_name: Name of the source of this OPDS feed.
            All Editions created by this import will be associated with this DataSource.
            If there is no DataSource with this name, one will be created.
            NOTE: If `collection` is provided, its .data_source will take precedence over any value provided here.
            This is only for use when you are importing OPDS metadata without any particular Collection in mind.
        :type data_source_name: str

        :param identifier_mapping: Dictionary used for mapping external identifiers into a set of internal ones
        :type identifier_mapping: Dict

        :param metadata_client: A SimplifiedOPDSLookup object that is used to fill in missing metadata
        :type metadata_client: SimplifiedOPDSLookup

        :param content_modifier: A function that may modify-in-place representations (such as images and EPUB documents)
            as they come in from the network.
        :type content_modifier: Callable

        :param map_from_collection: Identifier mapping
        :type map_from_collection: Dict

        :param mirrors: A dictionary of different MirrorUploader objects for different purposes
        :type mirrors: Dict[MirrorUploader]
        """
        super(ProQuestOPDS2Importer, self).__init__(
            db,
            collection,
            data_source_name,
            identifier_mapping,
            http_get,
            metadata_client,
            content_modifier,
            map_from_collection,
            mirrors,
        )

        self._logger = logging.getLogger(__name__)

        self._configuration_storage = ConfigurationStorage(self)
        self._configuration_factory = ConfigurationFactory()

        factory = ProQuestAPIClientFactory()
        self._api_client = factory.create(self)
        self._credential_manager = ProQuestCredentialManager()

    @contextmanager
    def _get_configuration(self, db):
        """Return the configuration object.

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :return: Configuration object
        :rtype: ProQuestOPDS2ImporterConfiguration
        """
        with self._configuration_factory.create(
            self._configuration_storage, db, ProQuestOPDS2ImporterConfiguration
        ) as configuration:
            yield configuration

    @staticmethod
    def _get_affiliation_attributes(configuration):
        """Return a configured list of SAML attributes which can contain affiliation ID.

        :param configuration: Configuration object
        :type configuration: ProQuestOPDS2ImporterConfiguration

        :return: Configured list of SAML attributes which can contain affiliation ID
        :rtype: List[str]
        """
        affiliation_attributes = (
            ProQuestOPDS2ImporterConfiguration.DEFAULT_AFFILIATION_ATTRIBUTES
        )

        if configuration.affiliation_attributes:
            if isinstance(configuration.affiliation_attributes, list):
                affiliation_attributes = configuration.affiliation_attributes
            elif isinstance(configuration.affiliation_attributes, str):
                affiliation_attributes = tuple(
                    map(
                        str.strip,
                        str(configuration.affiliation_attributes)
                        .replace("[", "")
                        .replace("]", "")
                        .replace("(", "")
                        .replace(")", "")
                        .replace("'", "")
                        .replace('"', "")
                        .split(","),
                    )
                )
            else:
                raise ValueError(
                    "Configuration setting 'affiliation_attributes' has an incorrect format"
                )

        return affiliation_attributes

    def _get_patron_affiliation_id(self, patron, configuration):
        """Get a patron's affiliation ID.

        :param patron: Patron object
        :type patron: core.model.patron.Patron

        :param configuration: Configuration object
        :type configuration: ProQuestOPDS2ImporterConfiguration

        :return: Patron's affiliation ID
        :rtype: Optional[str]
        """
        affiliation_attributes = self._get_affiliation_attributes(configuration)
        affiliation_id = self._credential_manager.lookup_patron_affiliation_id(
            self._db, patron, affiliation_attributes
        )

        self._logger.info(
            "Patron {0} has the following SAML affiliation ID: {1}".format(
                patron, affiliation_id
            )
        )

        if not affiliation_id:
            affiliation_id = configuration.test_affiliation_id

            if not affiliation_id:
                self._logger.error(
                    "Patron {0} does not have neither real affiliation ID "
                    "nor test affiliation ID set up as a configuration setting".format(
                        patron
                    )
                )
                raise MISSING_AFFILIATION_ID

            self._logger.info(
                "Since patron doesn't have an affiliation ID we set it to the test one: {1}".format(
                    patron, affiliation_id
                )
            )

        return affiliation_id

    def _create_proquest_token(self, patron, configuration):
        """Create a new ProQuest JWT bearer token.

        :param patron: Patron object
        :type patron: core.model.patron.Patron

        :param configuration: Configuration object
        :type configuration: ProQuestOPDS2ImporterConfiguration

        :return: ProQuest JWT bearer token
        :rtype: core.model.credential.Credential
        """
        affiliation_id = self._get_patron_affiliation_id(patron, configuration)

        try:
            token = self._api_client.create_token(self._db, affiliation_id)
            token_expiration_timeout = (
                int(configuration.token_expiration_timeout)
                if configuration.token_expiration_timeout
                else ProQuestOPDS2ImporterConfiguration.DEFAULT_TOKEN_EXPIRATION_TIMEOUT_SECONDS
            )
            token = self._credential_manager.save_proquest_token(
                self._db,
                patron,
                datetime.timedelta(seconds=token_expiration_timeout),
                token,
            )

            return token
        except Exception as exception:
            self._logger.exception("Cannot create a ProQuest JWT bearer token")

            raise CannotCreateProQuestTokenError(str(exception))

    def _get_or_create_proquest_token(self, patron, configuration):
        """Get an existing or create a new ProQuest JWT bearer token.

        :param patron: Patron object
        :type patron: core.model.patron.Patron

        :param configuration: Configuration object
        :type configuration: ProQuestOPDS2ImporterConfiguration

        :return: ProQuest JWT bearer token
        :rtype: core.model.credential.Credential
        """
        token = self._credential_manager.lookup_proquest_token(self._db, patron)

        self._logger.info(
            "Patron {0} has the following token: {1}".format(patron, token)
        )

        if not token:
            token = self._create_proquest_token(patron, configuration)

        return token

    def _get_book(self, patron, configuration, document_id):
        """Get a book's content (in the case of open-access books) or a book's link otherwise.

        :param patron: Patron object
        :type patron: core.model.patron.Patron

        :param configuration: Configuration object
        :type configuration: ProQuestOPDS2ImporterConfiguration

        :param document_id: ProQuest Doc ID
        :type document_id: str

        :return: Either an ACS link to the book or the book content
        :rtype: api.proquest.client.ProQuestBook
        """
        token = self._get_or_create_proquest_token(patron, configuration)
        iterations = 0

        while True:
            try:
                book = self._api_client.get_book(
                    self._db, token.credential, document_id
                )

                return book
            except HTTPError as exception:
                if exception.response.status_code != 401 or iterations >= 1:
                    raise
                else:
                    token = self._create_proquest_token(patron, configuration)

            iterations += 1

    def _extract_image_links(self, publication, feed_self_url):
        """Extracts a list of LinkData objects containing information about artwork.

        :param publication: Publication object
        :type publication: ast_core.Publication

        :param feed_self_url: Feed's self URL
        :type feed_self_url: str

        :return: List of links metadata
        :rtype: List[LinkData]
        """
        self._logger.debug(
            "Started extracting image links from {0}".format(
                encode(publication.images)
            )
        )

        image_links = []

        for image_link in publication.images.links:
            thumbnail_link = self._extract_link(
                image_link,
                feed_self_url,
                default_link_rel=Hyperlink.THUMBNAIL_IMAGE,
            )
            thumbnail_link.rel = Hyperlink.THUMBNAIL_IMAGE

            cover_link = self._extract_link(
                image_link,
                feed_self_url,
                default_link_rel=Hyperlink.IMAGE,
            )
            cover_link.rel = Hyperlink.IMAGE
            cover_link.thumbnail = thumbnail_link
            image_links.append(cover_link)

        self._logger.debug(
            "Finished extracting image links from {0}: {1}".format(
                encode(publication.images), encode(image_links)
            )
        )

        return image_links

    def _extract_media_types_and_drm_scheme_from_link(self, link):
        """Extract information about content's media type and used DRM schema from the link.

        We consider viable the following two options:
        1. DRM-free books
        {
            "rel": "http://opds-spec.org/acquisition",
            "href": "http://distributor.com/bookID",
            "type": "application/epub+zip"
        }

        2. DRM-protected books
        {
            "rel": "http://opds-spec.org/acquisition",
            "href": "http://distributor.com/bookID",
            "type": "application/vnd.adobe.adept+xml",
            "properties": {
                "indirectAcquisition": [
                    {
                        "type": "application/epub+zip"
                    }
                ]
            }
        }

        :param link: Link object
        :type link: ast_core.Link

        :return: 2-tuple containing information about the content's media type and its DRM schema
        :rtype: List[Tuple[str, str]]
        """
        self._logger.debug(
            "Started extracting media types and a DRM scheme from {0}".format(
                encode(link)
            )
        )

        media_types_and_drm_scheme = []

        if link.properties:
            if (
                not link.properties.availability
                or link.properties.availability.state
                == opds2_ast.OPDS2AvailabilityType.AVAILABLE.value
            ):
                drm_scheme = (
                    link.type
                    if link.type in DeliveryMechanism.KNOWN_DRM_TYPES
                    else DeliveryMechanism.NO_DRM
                )

                for acquisition_object in link.properties.indirect_acquisition:
                    media_types_and_drm_scheme.append(
                        (acquisition_object.type, drm_scheme)
                    )
        else:
            if (
                link.type in MediaTypes.BOOK_MEDIA_TYPES
                or link.type in MediaTypes.AUDIOBOOK_MEDIA_TYPES
            ):
                # Despite the fact that the book is DRM-free, we set its DRM type as DeliveryMechanism.BEARER_TOKEN.
                # We need it to allow the book to be downloaded by a client app.
                media_types_and_drm_scheme.append(
                    (link.type, DeliveryMechanism.BEARER_TOKEN)
                )

        self._logger.debug(
            "Finished extracting media types and a DRM scheme from {0}: {1}".format(
                encode(link), encode(media_types_and_drm_scheme)
            )
        )

        return media_types_and_drm_scheme

    def _parse_identifier(self, identifier):
        """Parse the identifier and return an Identifier object representing it.

        :param identifier: String containing the identifier
        :type identifier: str

        :return: Identifier object
        :rtype: Identifier
        """
        return parse_identifier(self._db, identifier)

    def extract_next_links(self, feed):
        """Extract "next" links from the feed.

        :param feed: OPDS 2.0 feed
        :type feed: Union[str, opds2_ast.OPDS2Feed]

        :return: List of "next" links
        :rtype: List[str]
        """
        return []

    def patron_activity(self, patron, pin):
        """Return patron's loans.

        TODO This and code from ODLAPI should be refactored into a generic set of rules
        for any situation where the CM, not the remote API, is responsible for managing loans and holds.

        :param patron: A Patron object for the patron who wants to check out the book
        :type patron: Patron

        :param pin: The patron's alleged password
        :type pin: string

        :return: List of patron's loans
        :rtype: List[LoanInfo]
        """
        now = datetime.datetime.utcnow()
        loans = (
            self._db.query(Loan)
            .join(LicensePool)
            .join(Collection)
            .filter(
                Collection.id == self._collection_id,
                Loan.patron == patron,
                or_(Loan.start == None, Loan.start <= now),
                or_(Loan.end == None, Loan.end > now),
            )
        ).all()

        loan_info_objects = []

        for loan in loans:
            licensepool = get_one(self._db, LicensePool, id=loan.license_pool_id)

            loan_info_objects.append(
                LoanInfo(
                    collection=self.collection,
                    data_source_name=licensepool.data_source.name,
                    identifier_type=licensepool.identifier.type,
                    identifier=licensepool.identifier.identifier,
                    start_date=loan.start,
                    end_date=loan.end,
                    fulfillment_info=None,
                    external_identifier=None,
                )
            )

        return loan_info_objects

    def place_hold(self, patron, pin, licensepool, notification_email_address):
        pass

    def release_hold(self, patron, pin, licensepool):
        pass

    def internal_format(self, delivery_mechanism):
        """Look up the internal format for this delivery mechanism or raise an exception.

        :param delivery_mechanism: A LicensePoolDeliveryMechanism
        :type delivery_mechanism: LicensePoolDeliveryMechanism
        """
        return delivery_mechanism

    def checkout(self, patron, pin, licensepool, internal_format):
        """Checkout the book.

        NOTE: This method requires the patron to have either:
        - an active ProQuest JWT bearer token
        - or a SAML affiliation ID which will be used to create a new ProQuest JWT bearer token.
        """
        self._logger.info(
            "Started checking out '{0}' for patron {1}".format(internal_format, patron)
        )

        try:
            with self._get_configuration(self._db) as configuration:
                self._get_or_create_proquest_token(patron, configuration)

                loan_period = self.collection.default_loan_period(patron.library)
                start_time = datetime.datetime.utcnow()
                end_time = start_time + datetime.timedelta(days=loan_period)
                loan = LoanInfo(
                    licensepool.collection,
                    licensepool.data_source.name,
                    identifier_type=licensepool.identifier.type,
                    identifier=licensepool.identifier.identifier,
                    start_date=start_time,
                    end_date=end_time,
                    fulfillment_info=None,
                    external_identifier=None,
                )

                self._logger.info(
                    "Finished checking out {0} for patron {1}: {2}".format(
                        internal_format, patron, loan
                    )
                )

                return loan
        except BaseError as exception:
            self._logger.exception("Failed to check out {0} for patron {1}")
            raise CannotLoan(str(exception))

    def fulfill(
        self,
        patron,
        pin,
        licensepool,
        internal_format=None,
        part=None,
        fulfill_part_url=None,
    ):
        """Fulfill the loan.

        NOTE: This method requires the patron to have either:
        - an active ProQuest JWT bearer token
        - or a SAML affiliation ID which will be used to create a new ProQuest JWT bearer token.
        """
        self._logger.info(
            "Started fulfilling '{0}' for patron {1}".format(internal_format, patron)
        )

        try:
            with self._get_configuration(self._db) as configuration:
                token = self._get_or_create_proquest_token(patron, configuration)
                book = self._get_book(
                    patron, configuration, licensepool.identifier.identifier
                )

                if book.content is not None:
                    fulfillment_info = FulfillmentInfo(
                        licensepool.collection,
                        licensepool.data_source.name,
                        licensepool.identifier.type,
                        licensepool.identifier.identifier,
                        content_link=None,
                        content_type=book.content_type
                        if book.content_type
                        else internal_format.delivery_mechanism.content_type,
                        content=book.content,
                        content_expires=None,
                    )
                else:
                    now = datetime.datetime.utcnow()
                    expires_in = (token.expires - now).total_seconds()
                    token_document = dict(
                        token_type="Bearer",
                        access_token=token.credential,
                        expires_in=expires_in,
                        location=book.link,
                    )

                    return FulfillmentInfo(
                        licensepool.collection,
                        licensepool.data_source.name,
                        licensepool.identifier.type,
                        licensepool.identifier.identifier,
                        content_link=None,
                        content_type=DeliveryMechanism.BEARER_TOKEN,
                        content=json.dumps(token_document),
                        content_expires=token.expires,
                    )

                self._logger.info(
                    "Finished fulfilling {0} for patron {1}: {2}".format(
                        internal_format, patron, fulfillment_info
                    )
                )

                return fulfillment_info
        except BaseError as exception:
            self._logger.exception("Failed to fulfill out {0} for patron {1}")

            raise CannotFulfill(str(exception))

    def external_integration(self, db):
        """Return an external integration associated with this object.

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :return: External integration associated with this object
        :rtype: core.model.configuration.ExternalIntegration
        """
        return self.collection.external_integration


class ProQuestOPDS2ImportMonitor(OPDS2ImportMonitor, HasExternalIntegration):
    PROTOCOL = ExternalIntegration.PROQUEST

    def __init__(
        self,
        client_factory,
        db,
        collection,
        import_class,
        force_reimport=False,
        process_removals=False,
        **import_class_kwargs
    ):
        """Initialize a new instance of ProQuestOPDS2ImportMonitor class.

        :param client_factory: ProQuest API client
        :type client_factory: api.proquest.client.ProQuestAPIClientFactory

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param collection: Collection instance
        :type collection: core.model.collection.Collection

        :param import_class: Class containing the import logic
        :type import_class: Type

        :param force_reimport: Boolean value indicating whether the import process must be started from scratch
        :type force_reimport: bool

        :param process_removals: Boolean value indicating whether
            the monitor must process removals and clean items
            that are no longer present in the ProQuest feed from the CM's catalog
        :type process_removals: bool
        """
        super(ProQuestOPDS2ImportMonitor, self).__init__(
            db, collection, import_class, force_reimport, **import_class_kwargs
        )

        self._client_factory = client_factory
        self._feeds = None
        self._client = self._client_factory.create(self)
        self._process_removals = process_removals

        self._logger = logging.getLogger(__name__)

    def _parse_identifier(self, identifier):
        """Extract the publication's identifier from its metadata.

        :param identifier: String containing the identifier
        :type identifier: str

        :return: Identifier object
        :rtype: Identifier
        """
        return parse_identifier(self._db, identifier)

    @staticmethod
    def _get_publications(feed):
        """Return all the publications in the feed.

        :param feed: OPDS 2.0 feed
        :type feed: opds2_ast.OPDS2Feed

        :return: An iterable list of publications containing in the feed
        :rtype: Iterable[opds2_ast.OPDS2Publication]
        """
        if feed.publications:
            for publication in feed.publications:
                yield publication

        if feed.groups:
            for group in feed.groups:
                if group.publications:
                    for publication in group.publications:
                        yield publication

    def _download_feed_pages(self, feed_pages_directory):
        """Download all the pages of the ProQuest OPDS feed.

        Downloaded feed pages are dumped to the local disk instead of storing them in memory
        in order to decrease the memory footprint.

        We have to wait until all the pages are downloaded because
        the ProQuest feed structure doesn't allow us to use short-circuit logic
        (there is no update date and ordering sometimes is incorrect),
        and we always have to start from scratch.
        Hence, we need to have all the pages locally before starting the import process.
        Otherwise, the import won't succeed.

        :param feed_pages_directory: Absolute path to the directory containing temporary files with feed pages
        :type feed_pages_directory: str

        :return: List of absolute file names containing feed pages
        :rtype: List[str]
        """
        self._logger.info("Started downloading feed pages")

        feed_page_files = []

        for feed in self._client.download_all_feed_pages(self._db):
            feed_page_file = tempfile.NamedTemporaryFile(
                mode="r+", dir=feed_pages_directory, delete=False
            )

            try:
                feed_page_content = json.dumps(
                    feed, default=str, ensure_ascii=True,
                )
                feed_page_file.write(feed_page_content)
                feed_page_file.flush()

                feed_page_files.append(feed_page_file.name)
            except:
                self._logger.exception(
                    "An unexpected error occurred during saving a feed page to a temporary file"
                )
                raise

        self._logger.info(
            "Finished downloading {0} feed pages".format(len(feed_page_files))
        )

        return feed_page_files

    def _parse_feed_page(self, page, feed_page_file):
        """Parse the ProQuest feed page residing in a temporary file on a local disk.

        :param page: Index of the page
        :type page: int

        :param feed_page_file: Absolute path to a temporary file containing the feed page
        :type feed_page_file: str

        :return: Parsed OPDS feed page
        :rtype: opds2_ast.OPDS2Feed
        """
        self._logger.info("Page # {0}. Started parsing the feed".format(page))

        with codecs.open(
            feed_page_file, "r", encoding="utf-8"
        ) as feed_page_file_handle:
            feed = feed_page_file_handle.read()

        feed = parse_feed(feed, silent=False)

        self._logger.info("Page # {0}. Finished parsing the feed".format(page))

        return feed

    def _collect_feed_identifiers(self, feed, feed_identifiers):
        """Keep track of all identifiers in the ProQuest feed and save them in a list.

        :param feed: ProQuest OPDS 2.0 feed
        :type feed: opds2_ast.OPDS2Feed

        :param feed_identifiers: List of identifiers in the ProQuest feed
        :type feed_identifiers: List[str]
        """
        for publication in self._get_publications(feed):
            identifier = parse_identifier(self._db, publication.metadata.identifier)

            feed_identifiers.append(identifier.identifier)

    def _clean_removed_items(self, feed_identifiers):
        """Make items that are no longer present in the ProQuest feed to be invisible in the CM's catalog.

        :param feed_identifiers: List of identifiers present in the ProQuest feed
        :type feed_identifiers: List[str]
        """
        self._logger.info(
            "Started removing identifiers that are no longer present in the ProQuest feed"
        )

        items_to_remove = (
            self._db.query(LicensePool)
            .join(Collection)
            .join(Identifier)
            .filter(Collection.id == self.collection_id)
            .filter(~Identifier.identifier.in_(feed_identifiers))
        )

        for item in items_to_remove:
            item.unlimited_access = False

        self._logger.info(
            "Finished removing identifiers that are no longer present in the ProQuest feed"
        )

    def _get_feeds(self):
        """Return a generator object traversing through a list of the ProQuest OPDS 2.0 feed pages.

        :return: Generator object traversing through a list of the ProQuest OPDS 2.0 feed pages
        :rtype: Iterable[opds2_ast.OPDS2Feed]
        """
        self._logger.info("Started fetching ProQuest paged OPDS 2.0 feeds")

        feed_pages_directory = tempfile.mkdtemp()

        try:
            feed_page_files = self._download_feed_pages(feed_pages_directory)

            page = 1
            processed_number_of_items = 0
            total_number_of_items = None

            self._logger.info("Started processing feed pages")

            for feed_page_file in feed_page_files:
                feed = self._parse_feed_page(page, feed_page_file)

                # FIXME: We cannot short-circuit the feed import process
                #  because ProQuest feed is not ordered by the publication's modified date.
                #  This issue will be addressed in https://jira.nypl.org/browse/SIMPLY-3343
                # if not self.feed_contains_new_data(feed):
                #     break

                if total_number_of_items is None:
                    total_number_of_items = (
                        feed.metadata.number_of_items
                        if feed.metadata.number_of_items
                        else 0
                    )

                processed_number_of_items += (
                    feed.metadata.items_per_page if feed.metadata.items_per_page else 0
                )

                self._logger.info(
                    "Page # {0}. Processed {1} items out of {2} ({3:.2f}%)".format(
                        page,
                        processed_number_of_items,
                        total_number_of_items,
                        processed_number_of_items / total_number_of_items * 100.0,
                    )
                )

                page += 1

                yield None, feed

            self._logger.info(
                "Finished processing {0} feed pages".format(len(feed_page_files))
            )
        except Exception:
            self._logger.exception(
                "An unexpected exception occurred during fetching ProQuest paged OPDS 2.0 feeds"
            )
        finally:
            shutil.rmtree(feed_pages_directory)

        self._logger.info("Finished fetching ProQuest paged OPDS 2.0 feeds")

    def run_once(self, progress_ignore):
        # This list is used to keep track of all identifiers in the ProQuest feed.
        feed_identifiers = []

        feeds = self._get_feeds()
        total_imported = 0
        total_failures = 0

        for link, feed in feeds:
            if self._process_removals:
                self._collect_feed_identifiers(feed, feed_identifiers)

            self.log.info("Importing next feed: %s", link)
            imported_editions, failures = self.import_one_feed(feed)
            total_imported += len(imported_editions)
            total_failures += len(failures)
            self._db.commit()

        achievements = "Items imported: %d. Failures: %d." % (
            total_imported,
            total_failures,
        )

        if self._process_removals:
            self._clean_removed_items(feed_identifiers)

        return TimestampData(achievements=achievements)
