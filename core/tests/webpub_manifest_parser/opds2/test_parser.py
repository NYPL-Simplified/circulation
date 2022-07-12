import datetime
import os
from unittest import TestCase

from core.util.webpub_manifest_parser.core.ast import (
    CompactCollection,
    Contributor,
    LinkList,
    PresentationMetadata,
)
from core.util.webpub_manifest_parser.opds2.ast import (
    OPDS2AcquisitionObject,
    OPDS2AvailabilityType,
    OPDS2FeedMetadata,
    OPDS2LinkProperties,
)
from core.util.webpub_manifest_parser.opds2.parsers import OPDS2DocumentParserFactory
from core.util.webpub_manifest_parser.opds2.registry import (
    OPDS2LinkRelationsRegistry,
    OPDS2MediaTypesRegistry,
)
from core.util.webpub_manifest_parser.utils import first_or_default


class OPDS2Parser(TestCase):
    def test(self):
        # Arrange
        parser_factory = OPDS2DocumentParserFactory()
        parser = parser_factory.create()
        input_file_path = os.path.join(
            os.path.dirname(__file__), "../../files/opds2/feed.json"
        )

        # Act
        feed = parser.parse_file(input_file_path)

        # Assert
        self.assertIsInstance(feed.metadata, OPDS2FeedMetadata)
        self.assertEqual("Example listing publications", feed.metadata.title)

        self.assertIsInstance(feed.links, list)
        self.assertEqual(1, len(feed.links))
        [manifest_link] = feed.links
        self.assertEqual(OPDS2LinkRelationsRegistry.SELF.key, manifest_link.rels[0])
        self.assertEqual("http://example.com/new", manifest_link.href)
        self.assertEqual(OPDS2MediaTypesRegistry.OPDS_FEED.key, manifest_link.type)

        self.assertIsInstance(feed.publications, list)
        self.assertEqual(2, len(feed.publications))
        publication = feed.publications[0]

        self.assertIsInstance(publication.metadata, PresentationMetadata)
        self.assertEqual("http://schema.org/Book", publication.metadata.type)
        self.assertEqual("Moby-Dick", publication.metadata.title)
        self.assertEqual(
            [Contributor(name="Herman Melville", roles=[], links=LinkList())],
            publication.metadata.authors
        )
        self.assertEqual("urn:isbn:978-3-16-148410-0", publication.metadata.identifier)
        self.assertEqual(["en"], publication.metadata.languages)
        self.assertEqual(
            datetime.datetime(2015, 9, 29, 17, 0, 0), publication.metadata.modified
        )

        self.assertIsInstance(publication.links, list)
        self.assertEqual(len(publication.links), 2)

        publication_self_link = first_or_default(
            publication.links.get_by_rel(OPDS2LinkRelationsRegistry.SELF.key)
        )
        self.assertEqual(
            OPDS2LinkRelationsRegistry.SELF.key, publication_self_link.rels[0]
        )
        self.assertEqual(
            "http://example.org/publication.json", publication_self_link.href
        )
        self.assertEqual(
            OPDS2MediaTypesRegistry.OPDS_PUBLICATION.key, publication_self_link.type
        )

        publication_acquisition_link = first_or_default(
            publication.links.get_by_rel(OPDS2LinkRelationsRegistry.OPEN_ACCESS.key)
        )
        self.assertEqual(
            OPDS2LinkRelationsRegistry.OPEN_ACCESS.key,
            publication_acquisition_link.rels[0],
        )
        self.assertEqual(
            "http://example.org/moby-dick.epub", publication_acquisition_link.href
        )
        self.assertEqual(
            OPDS2MediaTypesRegistry.EPUB_PUBLICATION_PACKAGE.key,
            publication_acquisition_link.type,
        )

        self.assertIsInstance(publication.images, CompactCollection)
        self.assertIsInstance(publication.images.links, list)
        self.assertEqual(3, len(publication.images.links))

        jpeg_cover_link = first_or_default(
            publication.images.links.get_by_href("http://example.org/cover.jpg")
        )
        self.assertEqual([], jpeg_cover_link.rels)
        self.assertEqual("http://example.org/cover.jpg", jpeg_cover_link.href)
        self.assertEqual(OPDS2MediaTypesRegistry.JPEG.key, jpeg_cover_link.type)
        self.assertEqual(1400, jpeg_cover_link.height)
        self.assertEqual(800, jpeg_cover_link.width)

        small_jpeg_cover_link = first_or_default(
            publication.images.links.get_by_href("http://example.org/cover-small.jpg")
        )
        self.assertEqual(
            "http://example.org/cover-small.jpg", small_jpeg_cover_link.href
        )
        self.assertEqual(OPDS2MediaTypesRegistry.JPEG.key, small_jpeg_cover_link.type)
        self.assertEqual(700, small_jpeg_cover_link.height)
        self.assertEqual(400, small_jpeg_cover_link.width)

        svg_cover_link = first_or_default(
            publication.images.links.get_by_href("http://example.org/cover.svg")
        )
        self.assertEqual(svg_cover_link.href, "http://example.org/cover.svg")
        self.assertEqual(svg_cover_link.type, OPDS2MediaTypesRegistry.SVG_XML.key)

        publication = feed.publications[1]
        self.assertIsInstance(publication.metadata, PresentationMetadata)
        self.assertEqual("http://schema.org/Book", publication.metadata.type)
        self.assertEqual("Adventures of Huckleberry Finn", publication.metadata.title)
        self.assertEqual(
            [
                Contributor(name="Mark Twain", roles=[], links=LinkList()),
                Contributor(
                    name="Samuel Langhorne Clemens", roles=[], links=LinkList()
                ),
            ],
            publication.metadata.authors,
        )
        self.assertEqual("urn:isbn:9781234567897", publication.metadata.identifier)
        self.assertEqual(["eng"], publication.metadata.languages)
        self.assertEqual(
            datetime.datetime(2014, 9, 28, 0, 0), publication.metadata.published
        )
        self.assertEqual(
            datetime.datetime(2015, 9, 29, 17, 0, 0), publication.metadata.modified
        )

        self.assertIsInstance(publication.links, list)

        publication_acquisition_link = first_or_default(
            publication.links.get_by_rel(OPDS2LinkRelationsRegistry.BORROW.key)
        )
        self.assertEqual(
            OPDS2LinkRelationsRegistry.BORROW.key, publication_acquisition_link.rels[0]
        )
        self.assertEqual(
            OPDS2MediaTypesRegistry.OPDS_PUBLICATION.key,
            publication_acquisition_link.type,
        )

        link_properties = publication_acquisition_link.properties
        self.assertIsInstance(link_properties, OPDS2LinkProperties)

        self.assertEqual(
            OPDS2AvailabilityType.AVAILABLE.value, link_properties.availability.state
        )

        self.assertEqual(2, len(link_properties.indirect_acquisition))

        indirect_acquisition_object = link_properties.indirect_acquisition[0]
        self.assertEqual(
            "application/vnd.adobe.adept+xml", indirect_acquisition_object.type
        )
        self.assertEqual(1, len(indirect_acquisition_object.child))
        self.assertIsInstance(
            indirect_acquisition_object.child[0], OPDS2AcquisitionObject
        )
        self.assertEqual(
            "application/epub+zip", indirect_acquisition_object.child[0].type
        )

        indirect_acquisition_object = link_properties.indirect_acquisition[1]
        self.assertEqual(
            "application/vnd.readium.lcp.license.v1.0+json",
            indirect_acquisition_object.type,
        )
        self.assertEqual(1, len(indirect_acquisition_object.child))
        self.assertIsInstance(
            indirect_acquisition_object.child[0], OPDS2AcquisitionObject
        )
        self.assertEqual(
            "application/epub+zip", indirect_acquisition_object.child[0].type
        )
