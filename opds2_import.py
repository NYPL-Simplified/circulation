import logging

import webpub_manifest_parser.opds2.ast as opds2_ast
from flask_babel import lazy_gettext as _
from io import StringIO
from urllib.parse import urljoin, urlparse
from webpub_manifest_parser.errors import BaseError
from webpub_manifest_parser.opds2.parsers import OPDS2DocumentParserFactory
from webpub_manifest_parser.opds2.registry import (
    OPDS2LinkRelationsRegistry,
    OPDS2MediaTypesRegistry,
)
from webpub_manifest_parser.utils import encode, first_or_default

from .metadata_layer import (
    CirculationData,
    ContributorData,
    FormatData,
    IdentifierData,
    LinkData,
    Metadata,
    SubjectData,
)
from .model import (
    Contributor,
    DeliveryMechanism,
    Edition,
    ExternalIntegration,
    Hyperlink,
    Identifier,
    LicensePool,
    LinkRelations,
    MediaTypes,
    Representation,
    RightsStatus,
    Subject,
)
from .opds_import import OPDSImporter, OPDSImportMonitor
from .util.http import BadResponseException
from .util.opds_writer import OPDSFeed


def parse_feed(feed, silent=True):
    """Parses the feed into OPDS2Feed object.

    :param feed: OPDS 2.0 feed
    :type feed: Union[str, opds2_ast.OPDS2Feed]

    :param silent: Boolean value indicating whether to raise
    :type silent: bool

    :return: Parsed OPDS 2.0 feed
    :rtype: opds2_ast.OPDS2Feed
    """
    parsed_feed = None

    if isinstance(feed, str):
        try:
            input_stream = StringIO(feed)
            parser_factory = OPDS2DocumentParserFactory()
            parser = parser_factory.create()

            parsed_feed = parser.parse_stream(input_stream)
        except BaseError:
            logging.exception("Failed to parse the OPDS 2.0 feed")

            if not silent:
                raise
    elif isinstance(feed, dict):
        try:
            parser_factory = OPDS2DocumentParserFactory()
            parser = parser_factory.create()

            parsed_feed = parser.parse_json(feed)
        except BaseError:
            logging.exception("Failed to parse the OPDS 2.0 feed")

            if not silent:
                raise
    elif isinstance(feed, opds2_ast.OPDS2Feed):
        parsed_feed = feed
    else:
        raise ValueError(
            "Argument 'feed' must be either a string or instance of {0} class".format(opds2_ast.OPDS2Feed)
        )

    return parsed_feed


class OPDS2Importer(OPDSImporter):
    """Imports editions and license pools from an OPDS 2.0 feed."""

    NAME = ExternalIntegration.OPDS2_IMPORT
    DESCRIPTION = _(u"Import books from a publicly-accessible OPDS 2.0 feed.")
    NEXT_LINK_RELATION = u"next"

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
        """Initialize a new instance of OPDS2Importer class.

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
        super(OPDS2Importer, self).__init__(
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

    def _extract_subjects(self, subjects):
        """Extract a list of SubjectData objects from the webpub-manifest-parser's subject.

        :param subjects: Parsed subject object
        :type subjects: List[core_ast.Subject]

        :return: List of subjects metadata
        :rtype: List[SubjectMetadata]
        """
        self._logger.debug(u"Started extracting subjects metadata")

        subject_metadata_list = []

        for subject in subjects:
            self._logger.debug(
                u"Started extracting subject metadata from {0}".format(encode(subject))
            )

            scheme = subject.scheme

            subject_type = Subject.by_uri.get(scheme)
            if not subject_type:
                # We can't represent this subject because we don't
                # know its scheme. Just treat it as a tag.
                subject_type = Subject.TAG

            subject_metadata = SubjectData(
                type=subject_type, identifier=subject.code, name=subject.name, weight=1
            )

            subject_metadata_list.append(subject_metadata)

            self._logger.debug(
                u"Finished extracting subject metadata from {0}: {1}".format(
                    encode(subject), encode(subject_metadata)
                )
            )

        self._logger.debug(
            u"Finished extracting subjects metadata: {0}".format(
                encode(subject_metadata_list)
            )
        )

        return subject_metadata_list

    def _extract_contributors(self, contributors, default_role=Contributor.AUTHOR_ROLE):
        """Extract a list of ContributorData objects from the webpub-manifest-parser's contributor.

        :param contributors: Parsed contributor object
        :type contributors: List[core_ast.Contributor]

        :param default_role: Default role
        :type default_role: Optional[str]

        :return: List of contributors metadata
        :rtype: List[ContributorData]
        """
        self._logger.debug(u"Started extracting contributors metadata")

        contributor_metadata_list = []

        for contributor in contributors:
            self._logger.debug(
                u"Started extracting contributor metadata from {0}".format(
                    encode(contributor)
                )
            )

            contributor_metadata = ContributorData(
                sort_name=contributor.sort_as,
                display_name=contributor.name,
                family_name=None,
                wikipedia_name=None,
                roles=contributor.roles if contributor.roles else default_role,
            )

            self._logger.debug(
                u"Finished extracting contributor metadata from {0}: {1}".format(
                    encode(contributor), encode(contributor_metadata)
                )
            )

            contributor_metadata_list.append(contributor_metadata)

        self._logger.debug(
            u"Finished extracting contributors metadata: {0}".format(
                encode(contributor_metadata_list)
            )
        )

        return contributor_metadata_list

    def _extract_link(self, link, feed_self_url, default_link_rel=None):
        """Extract a LinkData object from webpub-manifest-parser's link.

        :param link: webpub-manifest-parser's link
        :type link: ast_core.Link

        :param feed_self_url: Feed's self URL
        :type feed_self_url: str

        :param default_link_rel: Default link's relation
        :type default_link_rel: Optional[str]

        :return: Link metadata
        :rtype: LinkData
        """
        self._logger.debug(
            u"Started extracting link metadata from {0}".format(encode(link))
        )

        # FIXME: It seems that OPDS 2.0 spec doesn't contain information about rights so we use the default one.
        rights_uri = RightsStatus.rights_uri_from_string("")
        rel = first_or_default(link.rels, default_link_rel)
        media_type = link.type
        href = link.href

        if feed_self_url and not urlparse(href).netloc:
            # This link is relative, so we need to get the absolute url
            href = urljoin(feed_self_url, href)

        link_metadata = LinkData(
            rel=rel,
            href=href,
            media_type=media_type,
            rights_uri=rights_uri,
            content=None,
        )

        self._logger.debug(
            u"Finished extracting link metadata from {0}: {1}".format(
                encode(link), encode(link_metadata)
            )
        )

        return link_metadata

    def _extract_description_link(self, publication):
        """Extract description from the publication object and create a Hyperlink.DESCRIPTION link containing it.

        :param publication: Publication object
        :type publication: opds2_ast.Publication

        :return: LinkData object containing publication's description
        :rtype: LinkData
        """
        self._logger.debug(
            u"Started extracting a description link from {0}".format(
                encode(publication.metadata.description)
            )
        )

        description_link = None

        if publication.metadata.description:
            description_link = LinkData(
                rel=Hyperlink.DESCRIPTION,
                media_type=MediaTypes.TEXT_PLAIN,
                content=publication.metadata.description,
            )

        self._logger.debug(
            u"Finished extracting a description link from {0}: {1}".format(
                encode(publication.metadata.description), encode(description_link)
            )
        )

        return description_link

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
            u"Started extracting image links from {0}".format(
                encode(publication.images)
            )
        )

        # FIXME: This code most likely will not work in general.
        # There's no guarantee that these images have the same media type,
        # or that the second-largest image isn't far too large to use as a thumbnail.
        # Instead of using the second-largest image as a thumbnail,
        # find the image that would make the best thumbnail
        # because of its dimensions, media type, and aspect ratio:
        #       IDEAL_COVER_ASPECT_RATIO = 2.0/3
        #       IDEAL_IMAGE_HEIGHT = 240
        #       IDEAL_IMAGE_WIDTH = 160

        sorted_raw_image_links = list(
            reversed(
                sorted(
                    publication.images.links, key=lambda link: (link.width or 0, link.height or 0)
                )
            )
        )
        image_links = []

        if len(sorted_raw_image_links) > 0:
            cover_link = self._extract_link(
                sorted_raw_image_links[0],
                feed_self_url,
                default_link_rel=Hyperlink.IMAGE,
            )
            image_links.append(cover_link)

        if len(sorted_raw_image_links) > 1:
            cover_link = self._extract_link(
                sorted_raw_image_links[1],
                feed_self_url,
                default_link_rel=Hyperlink.THUMBNAIL_IMAGE,
            )
            image_links.append(cover_link)

        self._logger.debug(
            u"Finished extracting image links from {0}: {1}".format(
                encode(publication.images), encode(image_links)
            )
        )

        return image_links

    def _extract_links(self, publication, feed_self_url):
        """Extract a list of LinkData objects from a list of webpub-manifest-parser links.

        :param publication: Publication object
        :type publication: ast_core.Publication

        :param feed_self_url: Feed's self URL
        :type feed_self_url: str

        :return: List of links metadata
        :rtype: List[LinkData]
        """
        self._logger.debug(
            u"Started extracting links from {0}".format(encode(publication.links))
        )

        links = []

        for link in publication.links:
            link_metadata = self._extract_link(link, feed_self_url)
            links.append(link_metadata)

        description_link = self._extract_description_link(publication)
        if description_link:
            links.append(description_link)

        image_links = self._extract_image_links(publication, feed_self_url)
        if image_links:
            links.extend(image_links)

        self._logger.debug(
            u"Finished extracting links from {0}: {1}".format(
                encode(publication.links), encode(links)
            )
        )

        return links

    def _extract_media_types_and_drm_scheme_from_link(self, link):
        """Extract information about content's media type and used DRM schema from the link.

        :param link: Link object
        :type link: ast_core.Link

        :return: 2-tuple containing information about the content's media type and its DRM schema
        :rtype: List[Tuple[str, str]]
        """
        self._logger.debug(
            u"Started extracting media types and a DRM scheme from {0}".format(
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
                for acquisition_object in link.properties.indirect_acquisition:
                    nested_acquisition_object = acquisition_object

                    while nested_acquisition_object.child:
                        nested_acquisition_object = first_or_default(
                            acquisition_object.child
                        )

                    drm_scheme = (
                        acquisition_object.type
                        if acquisition_object.type in DeliveryMechanism.KNOWN_DRM_TYPES
                        else DeliveryMechanism.NO_DRM
                    )

                    media_types_and_drm_scheme.append(
                        (nested_acquisition_object.type, drm_scheme)
                    )
        else:
            if (
                link.type in MediaTypes.BOOK_MEDIA_TYPES
                or link.type in MediaTypes.AUDIOBOOK_MEDIA_TYPES
            ):
                media_types_and_drm_scheme.append((link.type, DeliveryMechanism.NO_DRM))

        self._logger.debug(
            u"Finished extracting media types and a DRM scheme from {0}: {1}".format(
                encode(link), encode(media_types_and_drm_scheme)
            )
        )

        return media_types_and_drm_scheme

    def _extract_medium_from_links(self, links):
        """Extract the publication's medium from its links.

        :param links: List of links
        :type links: ast_core.LinkList

        :return: Publication's medium
        :rtype: Optional[str]
        """
        derived = None

        for link in links:
            if not link.rels or not link.type or not self._is_acquisition_link(link):
                continue

            link_media_type, _ = first_or_default(
                self._extract_media_types_and_drm_scheme_from_link(link),
                default=(None, None),
            )
            derived = Edition.medium_from_media_type(link_media_type)

            if derived:
                break

        return derived

    @staticmethod
    def _extract_medium(publication, default_medium=Edition.BOOK_MEDIUM):
        """Extract the publication's medium from its metadata.

        :param publication: Publication object
        :type publication: opds2_core.OPDS2Publication

        :return: Publication's medium
        :rtype: str
        """
        medium = default_medium

        if publication.metadata.type:
            medium = Edition.additional_type_to_medium.get(
                publication.metadata.type, default_medium
            )

        return medium

    def _extract_identifier(self, publication):
        """Extract the publication's identifier from its metadata.

        :param publication: Publication object
        :type publication: opds2_core.OPDS2Publication

        :return: Identifier object
        :rtype: Identifier
        """
        return self._parse_identifier(publication.metadata.identifier)

    def _extract_publication_metadata(self, feed, publication, data_source_name):
        """Extract a Metadata object from webpub-manifest-parser's publication.

        :param publication: Feed object
        :type publication: opds2_ast.OPDS2Feed

        :param publication: Publication object
        :type publication: opds2_ast.OPDS2Publication

        :param data_source_name: Data source's name
        :type data_source_name: str

        :return: Publication's metadata
        :rtype: Metadata
        """
        self._logger.debug(
            u"Started extracting metadata from publication {0}".format(
                encode(publication)
            )
        )

        title = publication.metadata.title

        if title == OPDSFeed.NO_TITLE:
            title = None

        subtitle = publication.metadata.subtitle

        languages = first_or_default(publication.metadata.languages)
        derived_medium = self._extract_medium_from_links(publication.links)
        medium = self._extract_medium(publication, derived_medium)

        publisher = first_or_default(publication.metadata.publishers)
        if publisher:
            publisher = publisher.name

        imprint = first_or_default(publication.metadata.imprints)
        if imprint:
            imprint = imprint.name

        published = publication.metadata.published
        subjects = self._extract_subjects(publication.metadata.subjects)
        contributors = (
            self._extract_contributors(
                publication.metadata.authors, Contributor.AUTHOR_ROLE
            )
            + self._extract_contributors(
                publication.metadata.translators, Contributor.TRANSLATOR_ROLE
            )
            + self._extract_contributors(
                publication.metadata.editors, Contributor.EDITOR_ROLE
            )
            + self._extract_contributors(
                publication.metadata.artists, Contributor.ARTIST_ROLE
            )
            + self._extract_contributors(
                publication.metadata.illustrators, Contributor.ILLUSTRATOR_ROLE
            )
            + self._extract_contributors(
                publication.metadata.letterers, Contributor.LETTERER_ROLE
            )
            + self._extract_contributors(
                publication.metadata.pencilers, Contributor.PENCILER_ROLE
            )
            + self._extract_contributors(
                publication.metadata.colorists, Contributor.COLORIST_ROLE
            )
            + self._extract_contributors(
                publication.metadata.inkers, Contributor.INKER_ROLE
            )
            + self._extract_contributors(
                publication.metadata.narrators, Contributor.NARRATOR_ROLE
            )
            + self._extract_contributors(
                publication.metadata.contributors, Contributor.CONTRIBUTOR_ROLE
            )
        )

        feed_self_url = first_or_default(
            feed.links.get_by_rel(OPDS2LinkRelationsRegistry.SELF.key)
        ).href
        links = self._extract_links(publication, feed_self_url)

        last_opds_update = publication.metadata.modified

        identifier = self._extract_identifier(publication)
        identifier_data = IdentifierData(
            type=identifier.type, identifier=identifier.identifier
        )

        # FIXME: There are no measurements in OPDS 2.0
        measurements = []

        # FIXME: There is no series information in OPDS 2.0
        series = None
        series_position = None

        # FIXME: It seems that OPDS 2.0 spec doesn't contain information about rights so we use the default one
        rights_uri = RightsStatus.rights_uri_from_string("")

        circulation_data = CirculationData(
            default_rights_uri=rights_uri,
            data_source=data_source_name,
            primary_identifier=identifier_data,
            links=links,
            licenses_owned=LicensePool.UNLIMITED_ACCESS,
            licenses_available=LicensePool.UNLIMITED_ACCESS,
            licenses_reserved=0,
            patrons_in_hold_queue=0,
            formats=[],
        )

        formats = self._find_formats_in_non_open_access_acquisition_links(
            publication.links, links, rights_uri, circulation_data
        )
        circulation_data.formats.extend(formats)

        metadata = Metadata(
            data_source=data_source_name,
            title=title,
            subtitle=subtitle,
            language=languages,
            medium=medium,
            publisher=publisher,
            published=published,
            imprint=imprint,
            primary_identifier=identifier_data,
            subjects=subjects,
            contributors=contributors,
            measurements=measurements,
            series=series,
            series_position=series_position,
            links=links,
            data_source_last_updated=last_opds_update,
            circulation=circulation_data,
        )

        self._logger.debug(
            u"Finished extracting metadata from publication {0}: {1}".format(
                encode(publication), encode(metadata)
            )
        )

        return metadata

    def _find_formats_in_non_open_access_acquisition_links(
        self, ast_link_list, link_data_list, rights_uri, circulation_data
    ):
        """Find circulation formats in non open-access acquisition links.

        :param ast_link_list: List of Link objects
        :type ast_link_list: List[ast_core.Link]

        :param link_data_list: List of LinkData objects
        :type link_data_list: List[LinkData]

        :param rights_uri: Rights URI
        :type rights_uri: str

        :param circulation_data: Circulation data
        :type circulation_data: CirculationData

        :return: List of additional circulation formats found in non-open access links
        :rtype: List[FormatData]
        """
        formats = []

        for ast_link, parsed_link in zip(ast_link_list, link_data_list):
            if not self._is_acquisition_link(ast_link):
                continue
            if self._is_open_access_link_(parsed_link, circulation_data):
                continue

            for (
                content_type,
                drm_scheme,
            ) in self._extract_media_types_and_drm_scheme_from_link(ast_link):
                formats.append(
                    FormatData(
                        content_type=content_type,
                        drm_scheme=drm_scheme,
                        link=parsed_link,
                        rights_uri=rights_uri,
                    )
                )

        return formats

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

    @staticmethod
    def _is_acquisition_link(link):
        """Return a boolean value indicating whether a link can be considered an acquisition link.

        :param link: Link object
        :type link: ast_core.Link

        :return: Boolean value indicating whether a link can be considered an acquisition link
        :rtype: bool
        """
        return any(
            [rel for rel in link.rels if rel in LinkRelations.CIRCULATION_ALLOWED]
        )

    @staticmethod
    def _is_open_access_link_(link_data, circulation_data):
        """Return a boolean value indicating whether the specified LinkData object describes an open-access link.

        :param link_data: LinkData object
        :type link_data: LinkData

        :param circulation_data: CirculationData object
        :type circulation_data: CirculationData
        """
        open_access_link = (
            link_data.rel == Hyperlink.OPEN_ACCESS_DOWNLOAD and link_data.href
        )

        if open_access_link:
            return True

        # Try to deduce if the ast_link is open-access, even if it doesn't explicitly say it is
        rights_uri = link_data.rights_uri or circulation_data.default_rights_uri
        open_access_rights_link = (
            link_data.media_type in Representation.BOOK_MEDIA_TYPES
            and link_data.href
            and rights_uri in RightsStatus.OPEN_ACCESS
        )

        return open_access_rights_link

    def extract_next_links(self, feed):
        """Extracts "next" links from the feed.

        :param feed: OPDS 2.0 feed
        :type feed: Union[str, opds2_ast.OPDS2Feed]

        :return: List of "next" links
        :rtype: List[str]
        """
        parsed_feed = parse_feed(feed)

        if not parsed_feed:
            return []

        next_links = parsed_feed.links.get_by_rel(self.NEXT_LINK_RELATION)
        next_links = [next_link.href for next_link in next_links]

        return next_links

    def extract_last_update_dates(self, feed):
        """Extract last update date of the feed.

        :param feed: OPDS 2.0 feed
        :type feed: Union[str, opds2_ast.OPDS2Feed]

        :return: A list of 2-tuples containing publication's identifiers and their last modified dates
        :rtype: List[Tuple[str, datetime.datetime]]
        """
        parsed_feed = parse_feed(feed)

        if not parsed_feed:
            return []

        dates = [
            (publication.metadata.identifier, publication.metadata.modified)
            for publication in self._get_publications(parsed_feed)
            if publication.metadata.modified
        ]

        return dates

    def extract_feed_data(self, feed, feed_url=None):
        """Turn an OPDS 2.0 feed into lists of Metadata and CirculationData objects.

        :param feed: OPDS 2.0 feed
        :type feed: Union[str, opds2_ast.OPDS2Feed]

        :param feed_url: Feed URL used to resolve relative links
        :type feed_url: Optional[str]f
        """
        feed = parse_feed(feed, silent=False)
        publication_metadata_dictionary = {}
        failures = {}

        for publication in self._get_publications(feed):
            publication_metadata = self._extract_publication_metadata(
                feed, publication, self.data_source_name
            )

            publication_metadata_dictionary[
                publication_metadata.primary_identifier.identifier
            ] = publication_metadata

        return publication_metadata_dictionary, failures


class OPDS2ImportMonitor(OPDSImportMonitor):
    PROTOCOL = ExternalIntegration.OPDS2_IMPORT
    MEDIA_TYPE = OPDS2MediaTypesRegistry.OPDS_FEED.key, "application/json"

    def _verify_media_type(self, url, status_code, headers, feed):
        # Make sure we got an OPDS feed, and not an error page that was
        # sent with a 200 status code.
        media_type = headers.get("content-type")
        if not media_type or not any(x in media_type for x in self.MEDIA_TYPE):
            message = "Expected {0} OPDS 2.0 feed, got {1}".format(
                self.MEDIA_TYPE, media_type
            )

            raise BadResponseException(
                url, message=message, debug_message=feed, status_code=status_code
            )

    def _get_accept_header(self):
        return "{0}, {1};q=0.9, */*;q=0.1".format(
            OPDS2MediaTypesRegistry.OPDS_FEED.key, "application/json"
        )
