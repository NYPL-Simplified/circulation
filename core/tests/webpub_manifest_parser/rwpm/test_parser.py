import datetime
import os
from unittest import TestCase

from core.util.webpub_manifest_parser.core.ast import (
    CompactCollection,
    Contributor,
    LinkList,
    Metadata,
)
from core.util.webpub_manifest_parser.rwpm.parser import RWPMDocumentParserFactory
from core.util.webpub_manifest_parser.rwpm.registry import (
    RWPMLinkRelationsRegistry,
    RWPMMediaTypesRegistry,
)
from core.util.webpub_manifest_parser.utils import first_or_default


class RWPMParserTest(TestCase):
    def test(self):
        # Arrange
        parser_factory = RWPMDocumentParserFactory()
        parser = parser_factory.create()
        input_file_path = os.path.join(
            os.path.dirname(__file__), "../../files/rwpm/spec_example.json"
        )

        # Act
        manifest = parser.parse_file(input_file_path)

        # Assert
        self.assertIsInstance(manifest.context, list)
        self.assertEqual(1, len(manifest.context))
        [context] = manifest.context
        self.assertEqual(context, "https://readium.org/webpub-manifest/context.jsonld")

        self.assertIsInstance(manifest.metadata, Metadata)
        self.assertEqual("http://schema.org/Book", manifest.metadata.type)
        self.assertEqual("Moby-Dick", manifest.metadata.title)
        self.assertEqual(
            [Contributor(name="Herman Melville", roles=[], links=LinkList())],
            manifest.metadata.authors,
        )
        self.assertEqual("urn:isbn:978031600000X", manifest.metadata.identifier)
        self.assertEqual(["en"], manifest.metadata.languages)
        self.assertEqual(
            datetime.datetime(2015, 9, 29, 17, 0, 0), manifest.metadata.modified
        )

        self.assertIsInstance(manifest.links, list)
        self.assertEqual(3, len(manifest.links))

        self_link = first_or_default(
            manifest.links.get_by_rel(RWPMLinkRelationsRegistry.SELF.key)
        )
        self.assertIsNotNone(self_link)
        self.assertIn(RWPMLinkRelationsRegistry.SELF.key, self_link.rels)
        self.assertEqual("https://example.com/manifest.json", self_link.href)
        self.assertEqual(RWPMMediaTypesRegistry.MANIFEST.key, self_link.type)

        alternate_link = first_or_default(
            manifest.links.get_by_rel(RWPMLinkRelationsRegistry.ALTERNATE.key)
        )
        self.assertIsNotNone(alternate_link)
        self.assertIn(RWPMLinkRelationsRegistry.ALTERNATE.key, alternate_link.rels)
        self.assertEqual("https://example.com/publication.epub", alternate_link.href)
        self.assertEqual(
            RWPMMediaTypesRegistry.EPUB_PUBLICATION_PACKAGE.key, alternate_link.type
        )

        search_link = first_or_default(
            manifest.links.get_by_rel(RWPMLinkRelationsRegistry.SEARCH.key)
        )
        self.assertIsNotNone(search_link)
        self.assertIn(RWPMLinkRelationsRegistry.SEARCH.key, search_link.rels)
        self.assertEqual("https://example.com/search{?query}", search_link.href)
        self.assertEqual(RWPMMediaTypesRegistry.HTML.key, search_link.type)

        self.assertIsInstance(manifest.reading_order, CompactCollection)
        self.assertIsInstance(manifest.reading_order.links, list)
        self.assertEqual(2, len(manifest.reading_order.links))

        reading_order_link = manifest.reading_order.links[0]
        self.assertEqual("https://example.com/c001.html", reading_order_link.href)
        self.assertEqual(RWPMMediaTypesRegistry.HTML.key, reading_order_link.type)
        self.assertEqual("Chapter 1", reading_order_link.title)

        reading_order_link = manifest.reading_order.links[1]
        self.assertEqual("https://example.com/c002.html", reading_order_link.href)
        self.assertEqual(RWPMMediaTypesRegistry.HTML.key, reading_order_link.type)
        self.assertEqual("Chapter 2", reading_order_link.title)

        resources_sub_collection = manifest.resources
        self.assertEqual(5, len(resources_sub_collection.links))
        self.assertEqual(
            [RWPMLinkRelationsRegistry.COVER.key],
            resources_sub_collection.links[0].rels,
        )
        self.assertEqual(
            "https://example.com/cover.jpg", resources_sub_collection.links[0].href
        )
        self.assertEqual(
            RWPMMediaTypesRegistry.JPEG.key, resources_sub_collection.links[0].type
        )
        self.assertEqual(600, resources_sub_collection.links[0].height)
        self.assertEqual(400, resources_sub_collection.links[0].width)

        self.assertEqual(
            "https://example.com/style.css", resources_sub_collection.links[1].href
        )
        self.assertEqual(
            RWPMMediaTypesRegistry.CSS.key, resources_sub_collection.links[1].type
        )

        self.assertEqual(
            "https://example.com/whale.jpg", resources_sub_collection.links[2].href
        )
        self.assertEqual(
            RWPMMediaTypesRegistry.JPEG.key, resources_sub_collection.links[2].type
        )

        self.assertEqual(
            "https://example.com/boat.svg", resources_sub_collection.links[3].href
        )
        self.assertEqual(
            RWPMMediaTypesRegistry.SVG_XML.key, resources_sub_collection.links[3].type
        )

        self.assertEqual(
            "https://example.com/notes.html", resources_sub_collection.links[4].href
        )
        self.assertEqual(
            RWPMMediaTypesRegistry.HTML.key, resources_sub_collection.links[4].type
        )
