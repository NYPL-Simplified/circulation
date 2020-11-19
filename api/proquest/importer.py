import datetime
import logging
from contextlib import contextmanager

import six
import webpub_manifest_parser.opds2.ast as opds2_ast
from flask_babel import lazy_gettext as _
from requests import HTTPError
from six import StringIO
from sqlalchemy import or_
from webpub_manifest_parser.opds2.parsers import OPDS2DocumentParserFactory

from api.circulation import BaseCirculationAPI, FulfillmentInfo, LoanInfo
from api.circulation_exceptions import CannotFulfill, CannotLoan
from api.proquest.client import ProQuestAPIClientConfiguration, ProQuestAPIClientFactory
from api.proquest.credential import ProQuestCredentialManager
from api.proquest.identifier import ProQuestIdentifierParser
from api.saml.metadata import SAMLAttributes
from core.classifier import Classifier
from core.exceptions import BaseError
from core.model import Collection, Identifier, LicensePool, Loan, get_one
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
from core.opds2_import import OPDS2Importer, OPDS2ImportMonitor
from core.opds_import import OPDSImporter
from core.util.string_helpers import is_string

MISSING_AFFILIATION_ID = BaseError(
    _(
        "Patron does not have a SAML affiliation ID. "
        "It can mean either incorrect configuration of a SAML authentication provider or "
        "that patron has not yet been authenticated using the SAML authentication provider."
    )
)


class CannotCreateProQuestTokenError(BaseError):
    def __init__(self, inner_exception):
        message = "{0}: {1}".format(
            _("Can not create a ProQuest JWT bearer token"),
            six.ensure_text(str(inner_exception)),
        )

        super(CannotCreateProQuestTokenError, self).__init__(message, inner_exception)


class ProQuestOPDS2ImporterConfiguration(ConfigurationGrouping):
    """Contains configuration settings of ProQuestOPDS2Importer."""

    DEFAULT_TOKEN_EXPIRATION_TIMEOUT_SECONDS = 60 * 60
    TEST_AFFILIATION_ID = 1
    DEFAULT_AFFILIATION_ATTRIBUTES = (
        SAMLAttributes.eduPersonPrincipalName.name,
        SAMLAttributes.eduPersonScopedAffiliation.name,
    )

    data_source_name = ConfigurationMetadata(
        key=Collection.DATA_SOURCE_NAME_SETTING,
        label=_("Data source name"),
        description=_("Name of the data source associated with this collection."),
        type=ConfigurationAttributeType.TEXT,
        required=True,
        default="ProQuest",
    )

    default_audience = ConfigurationMetadata(
        key=Collection.DEFAULT_AUDIENCE_KEY,
        label=_("Default audience"),
        description=_(
            "Useful in the case if Circulation Manager cannot derive an audience from a book's classifications"
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

    token_expiration_timeout = ConfigurationMetadata(
        key="token_expiration_timeout",
        label=_("ProQuest JWT token's expiration timeout"),
        description=_("Determines how long in seconds can a ProQuest JWT token be valid."),
        type=ConfigurationAttributeType.NUMBER,
        required=False,
        default=DEFAULT_TOKEN_EXPIRATION_TIMEOUT_SECONDS,
    )

    affiliation_attributes = ConfigurationMetadata(
        key="affiliation_attributes",
        label=_("List of SAML attributes containing an affiliation ID"),
        description=_(""),
        type=ConfigurationAttributeType.MENU,
        required=False,
        default=list(DEFAULT_AFFILIATION_ATTRIBUTES),
        options=[
            ConfigurationOption(attribute.name, attribute.name)
            for attribute in SAMLAttributes
        ],
        format="narrow",
    )

    test_affiliation_id = ConfigurationMetadata(
        key="test_affiliation_id",
        label=_("Test SAML affiliation ID"),
        description=_("Test SAML affiliation ID used for testing ProQuest API."),
        type=ConfigurationAttributeType.TEXT,
        required=False,
    )


class ProQuestOPDS2Importer(OPDS2Importer, BaseCirculationAPI, HasExternalIntegration):
    """Allows to import ProQuest OPDS 2.0 feeds into Circulation Manager."""

    NAME = ExternalIntegration.PROQUEST
    DESCRIPTION = _(u"Import books from a ProQuest OPDS 2.0 feed.")
    SETTINGS = (
        ProQuestOPDS2ImporterConfiguration.to_settings()
        + ProQuestAPIClientConfiguration.to_settings()
    )

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

    def _extract_identifier(self, publication):
        """Extract the publication's identifier from its metadata.

        :param publication: Publication object
        :type publication: opds2_core.OPDS2Publication

        :return: Identifier object
        :rtype: Identifier
        """
        identifier, _ = Identifier.parse(
            self._db, publication.metadata.identifier, ProQuestIdentifierParser()
        )

        return identifier

    @staticmethod
    def _get_affiliation_attributes(configuration):
        """Return a configured list of SAML attributes which can contain affiliation ID.

        :param configuration: Configuration object
        :type configuration: ProQuestOPDS2ImporterConfiguration

        :return: Configured list of SAML attributes which can contain affiliation ID
        :rtype: List[str]
        """
        if configuration.affiliation_attributes:
            return configuration.affiliation_attributes
        else:
            return ProQuestOPDS2ImporterConfiguration.DEFAULT_AFFILIATION_ATTRIBUTES

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
        :rtype: str
        """
        affiliation_id = self._get_patron_affiliation_id(patron, configuration)

        try:
            token = self._api_client.create_token(self._db, affiliation_id)
            token_expiration_timeout = (
                int(configuration.token_expiration_timeout)
                if configuration.token_expiration_timeout
                else ProQuestOPDS2ImporterConfiguration.DEFAULT_TOKEN_EXPIRATION_TIMEOUT_SECONDS
            )

            self._credential_manager.save_proquest_token(
                self._db,
                patron,
                datetime.timedelta(seconds=token_expiration_timeout),
                token,
            )

            return token
        except Exception as exception:
            self._logger.exception("Cannot create a ProQuest JWT bearer token")

            raise CannotCreateProQuestTokenError(exception)

    def _get_or_create_proquest_token(self, patron, configuration):
        """Get an existing or create a new ProQuest JWT bearer token.

        :param patron: Patron object
        :type patron: core.model.patron.Patron

        :param configuration: Configuration object
        :type configuration: ProQuestOPDS2ImporterConfiguration

        :return: ProQuest JWT bearer token
        :rtype: str
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
                book = self._api_client.get_book(self._db, token, document_id)

                return book
            except HTTPError as exception:
                if exception.response.status_code != 401 or iterations >= 1:
                    raise
                else:
                    token = self._create_proquest_token(patron, configuration)

            iterations += 1

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
                or_(Loan.start is None, Loan.start <= now),
                or_(Loan.end is None, Loan.end > now),
            )
        )

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

                today = datetime.datetime.utcnow()

                loan = LoanInfo(
                    licensepool.collection,
                    licensepool.data_source.name,
                    identifier_type=licensepool.identifier.type,
                    identifier=licensepool.identifier.identifier,
                    start_date=today,
                    end_date=None,
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

            raise CannotLoan(six.ensure_text(str(exception)))

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
                        else internal_format.delivery_mechanism.media_type,
                        content=book.content,
                        content_expires=None,
                    )
                else:
                    fulfillment_info = FulfillmentInfo(
                        licensepool.collection,
                        licensepool.data_source.name,
                        licensepool.identifier.type,
                        licensepool.identifier.identifier,
                        content_link=book.link,
                        content_type=internal_format.delivery_mechanism.media_type,
                        content=None,
                        content_expires=None,
                    )

                self._logger.info(
                    "Finished fulfilling {0} for patron {1}: {2}".format(
                        internal_format, patron, fulfillment_info
                    )
                )

                return fulfillment_info
        except BaseError as exception:
            self._logger.exception("Failed to fulfill out {0} for patron {1}")

            raise CannotFulfill(six.ensure_text(str(exception)))

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
        **import_class_kwargs
    ):
        """Initialize a new instance of ProQuestOPDS2ImportMonitor class.

        :param client_factory: ProQuest API client
        :type client_factory: api.proquest.client.ProQuestAPIClientFactory
        """
        super(ProQuestOPDS2ImportMonitor, self).__init__(
            db, collection, import_class, force_reimport, **import_class_kwargs
        )

        self._client_factory = client_factory
        self._feeds = None
        self._client = self._client_factory.create(self)

        self._logger = logging.getLogger(__name__)

    def _parse_feed(self, feed, silent=True):
        """Parses the feed into OPDS2Feed object.

        :param feed: OPDS 2.0 feed
        :type feed: Union[str, opds2_ast.OPDS2Feed]

        :param silent: Boolean value indicating whether to raise
        :type silent: bool

        :return: Parsed OPDS 2.0 feed
        :rtype: opds2_ast.OPDS2Feed
        """
        parsed_feed = None

        if is_string(feed):
            try:
                input_stream = StringIO(feed)
                parser_factory = OPDS2DocumentParserFactory()
                parser = parser_factory.create()

                parsed_feed = parser.parse_stream(input_stream)
            except BaseError:
                self._logger.exception("Failed to parse the OPDS 2.0 feed")

                if not silent:
                    raise
        elif isinstance(feed, dict):
            try:
                parser_factory = OPDS2DocumentParserFactory()
                parser = parser_factory.create()

                parsed_feed = parser.parse_json(feed)
            except BaseError:
                self._logger.exception("Failed to parse the OPDS 2.0 feed")

                if not silent:
                    raise
        elif isinstance(feed, opds2_ast.OPDS2Feed):
            parsed_feed = feed
        else:
            raise ValueError(
                "Feed argument must be either string or OPDS2Feed instance"
            )

        return parsed_feed

    def _get_feeds(self):
        self._logger.info("Started fetching ProQuest paged OPDS 2.0 feeds")

        processed_number_of_items = 0
        total_number_of_items = None

        for feed in self._client.download_all_feed_pages(self._db):
            feed = self._parse_feed(feed, silent=False)

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
                "Page # {0}. Processed {0} items out of {1} ({2:.2f}%)".format(
                    feed.metadata.current_page,
                    processed_number_of_items,
                    total_number_of_items,
                    processed_number_of_items / total_number_of_items * 100.0,
                )
            )

            yield None, feed

        self._logger.info("Finished fetching ProQuest paged OPDS 2.0 feeds")
