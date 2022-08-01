from unittest import TestCase

from parameterized import parameterized

from core.util.webpub_manifest_parser.core.ast import (
    CompactCollection,
    Link,
    LinkList,
    PresentationMetadata,
)
from core.util.webpub_manifest_parser.core.registry import Registry
from core.util.webpub_manifest_parser.core.semantic import (
    MISSING_MANIFEST_LINK_REL_PROPERTY_ERROR,
    MISSING_SELF_LINK_ERROR,
    WRONG_SELF_LINK_HREF_FORMAT,
    SemanticAnalyzer,
)
from core.util.webpub_manifest_parser.rwpm.ast import RWPMManifest
from core.util.webpub_manifest_parser.rwpm.registry import RWPMLinkRelationsRegistry


class SemanticAnalyzerTest(TestCase):
    @parameterized.expand(
        [
            (
                "when_manifest_link_rel_property_is_missing",
                RWPMManifest(
                    metadata=PresentationMetadata("test"),
                    links=LinkList([Link(href="http://example.com")]),
                    reading_order=CompactCollection(),
                ),
                MISSING_MANIFEST_LINK_REL_PROPERTY_ERROR,
            ),
            (
                "when_manifest_self_link_is_missing",
                RWPMManifest(
                    metadata=PresentationMetadata("test"),
                    links=LinkList(
                        [
                            Link(
                                href="http://example.com",
                                rels=[RWPMLinkRelationsRegistry.SEARCH.key],
                            )
                        ]
                    ),
                    reading_order=CompactCollection(),
                ),
                MISSING_SELF_LINK_ERROR,
            ),
            (
                "when_manifest_self_link_has_wrong_href",
                RWPMManifest(
                    metadata=PresentationMetadata("test"),
                    links=LinkList(
                        [
                            Link(
                                href="example.com",
                                rels=[RWPMLinkRelationsRegistry.SELF.key],
                            )
                        ]
                    ),
                    reading_order=CompactCollection(),
                ),
                WRONG_SELF_LINK_HREF_FORMAT,
            ),
        ]
    )
    def test_semantic_analyzer_raises_error(self, _, manifest, expected_error):
        # Arrange
        media_types_registry = Registry()
        link_relations_registry = Registry()
        collection_roles_registry = Registry()
        semantic_analyzer = SemanticAnalyzer(
            media_types_registry, link_relations_registry, collection_roles_registry
        )

        # Act
        with self.assertRaises(expected_error.__class__) as assert_raises_context:
            semantic_analyzer.visit(manifest)

        # Assert
        self.assertEqual(str(expected_error), str(assert_raises_context.exception))
