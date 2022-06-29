import pytest
from unittest import TestCase

from mock import MagicMock, call
from parameterized import parameterized

from core.util.webpub_manifest_parser.core.ast import (
    CollectionList,
    Link,
    LinkList,
    PresentationMetadata,
)
from core.util.webpub_manifest_parser.opds2.ast import (
    OPDS2Feed,
    OPDS2FeedMetadata,
    OPDS2Group,
    OPDS2Navigation,
    OPDS2Publication,
)
from core.util.webpub_manifest_parser.opds2.registry import (
    OPDS2CollectionRolesRegistry,
    OPDS2LinkRelationsRegistry,
    OPDS2MediaTypesRegistry,
)
from core.util.webpub_manifest_parser.opds2.semantic import (
    MISSING_ACQUISITION_LINK,
    MISSING_NAVIGATION_LINK_TITLE_ERROR,
    MISSING_REQUIRED_FEED_SUB_COLLECTIONS,
    WRONG_GROUP_STRUCTURE,
    OPDS2SemanticAnalyzer,
)
from core.util.webpub_manifest_parser.rwpm.registry import RWPMLinkRelationsRegistry


class SemanticAnalyzerTest(TestCase):
    @parameterized.expand(
        [
            (
                "when_feed_does_not_contain_neither_publications_nor_navigation_nor_groups",
                OPDS2Feed(
                    metadata=OPDS2FeedMetadata(title="test"),
                    links=LinkList(
                        [
                            Link(
                                href="http://example.com",
                                rels=[RWPMLinkRelationsRegistry.SELF.key],
                            )
                        ]
                    ),
                ),
                MISSING_REQUIRED_FEED_SUB_COLLECTIONS,
            ),
            (
                "when_navigation_link_does_not_contain_title",
                OPDS2Feed(
                    metadata=OPDS2FeedMetadata(title="test"),
                    links=LinkList(
                        [
                            Link(
                                href="http://example.com",
                                rels=[RWPMLinkRelationsRegistry.SELF.key],
                            )
                        ]
                    ),
                    navigation=OPDS2Navigation(
                        links=LinkList([Link(href="http://example.com")])
                    ),
                ),
                MISSING_NAVIGATION_LINK_TITLE_ERROR,
            ),
            (
                "when_publication_does_not_contain_acquisition_link",
                OPDS2Feed(
                    metadata=OPDS2FeedMetadata(title="test"),
                    links=LinkList(
                        [
                            Link(
                                href="http://example.com",
                                rels=[RWPMLinkRelationsRegistry.SELF.key],
                            )
                        ]
                    ),
                    publications=CollectionList(
                        [
                            OPDS2Publication(
                                metadata=PresentationMetadata(title="Publication 1"),
                                links=LinkList([Link(href="http://example.com")]),
                            )
                        ]
                    ),
                ),
                MISSING_ACQUISITION_LINK,
            ),
            (
                "when_navigation_contains_both_links_and_publications",
                OPDS2Feed(
                    metadata=OPDS2FeedMetadata(title="test"),
                    links=LinkList(
                        [
                            Link(
                                href="http://example.com",
                                rels=[RWPMLinkRelationsRegistry.SELF.key],
                            )
                        ]
                    ),
                    groups=CollectionList(
                        [
                            OPDS2Group(
                                navigation=OPDS2Navigation(
                                    links=LinkList(
                                        [
                                            Link(
                                                href="http://example.com",
                                                rels=["current"],
                                            )
                                        ]
                                    )
                                ),
                                publications=CollectionList(
                                    [
                                        OPDS2Publication(
                                            metadata=PresentationMetadata(
                                                title="Publication 1"
                                            ),
                                            links=LinkList(
                                                [Link(href="http://example.com")]
                                            ),
                                        )
                                    ]
                                ),
                            )
                        ]
                    ),
                ),
                WRONG_GROUP_STRUCTURE,
            ),
        ]
    )
    def test_semantic_analyzer_raises_error(self, _, manifest, expected_error):
        # Arrange
        media_types_registry = OPDS2MediaTypesRegistry()
        link_relations_registry = OPDS2LinkRelationsRegistry()
        collection_roles_registry = OPDS2CollectionRolesRegistry()
        semantic_analyzer = OPDS2SemanticAnalyzer(
            media_types_registry, link_relations_registry, collection_roles_registry
        )

        # Act
        with pytest.raises(expected_error.__class__) as assert_raises_context:
            semantic_analyzer.visit(manifest)

        # Assert
        assert str(assert_raises_context.value) == str(expected_error)

    def test_semantic_analyzer_does_correctly_processes_valid_ast(self):
        # Arrange
        feed = OPDS2Feed(
            metadata=OPDS2FeedMetadata(title="test"),
            links=LinkList(
                [
                    Link(
                        href="http://example.com",
                        rels=[RWPMLinkRelationsRegistry.SELF.key],
                    )
                ]
            ),
            publications=CollectionList(
                [
                    OPDS2Publication(
                        metadata=PresentationMetadata(title="Publication 1"),
                        links=LinkList(
                            [
                                Link(
                                    href="http://example.com",
                                    rels=[OPDS2LinkRelationsRegistry.ACQUISITION.key],
                                )
                            ]
                        ),
                    )
                ]
            ),
            navigation=OPDS2Navigation(
                links=LinkList(
                    [
                        Link(
                            href="/new",
                            title="New Publications",
                            _type=OPDS2MediaTypesRegistry.OPDS_FEED,
                            rels=["current"],
                        )
                    ]
                )
            ),
            groups=CollectionList(
                [
                    OPDS2Group(
                        metadata=OPDS2FeedMetadata(title="Group 1"),
                        publications=CollectionList(
                            [
                                OPDS2Publication(
                                    metadata=PresentationMetadata(
                                        title="Publication 1.1"
                                    ),
                                    links=LinkList(
                                        [
                                            Link(
                                                href="http://example.com",
                                                rels=[
                                                    OPDS2LinkRelationsRegistry.ACQUISITION.key
                                                ],
                                            )
                                        ]
                                    ),
                                )
                            ]
                        ),
                    )
                ]
            ),
        )
        media_types_registry = OPDS2MediaTypesRegistry()
        link_relations_registry = OPDS2LinkRelationsRegistry()
        collection_roles_registry = OPDS2CollectionRolesRegistry()
        semantic_analyzer = OPDS2SemanticAnalyzer(
            media_types_registry, link_relations_registry, collection_roles_registry
        )

        semantic_analyzer.visit = MagicMock(side_effect=semantic_analyzer.visit)

        # Act
        semantic_analyzer.visit(feed)

        # Assert
        semantic_analyzer.visit.assert_has_calls(
            calls=[
                call(feed),
                call(feed.metadata),
                call(feed.links),
                call(feed.links[0]),
                call(feed.sub_collections),
                call(feed.publications),
                call(feed.publications[0]),
                call(feed.publications[0].metadata),
                call(feed.publications[0].links),
                call(feed.publications[0].links[0]),
                call(feed.publications[0].sub_collections),
                call(feed.navigation),
                call(feed.navigation),
                call(feed.navigation.links),
                call(feed.navigation.links[0]),
                call(feed.groups),
                call(feed.groups[0]),
                call(feed.groups[0].metadata),
                call(feed.groups[0].publications),
                call(feed.groups[0].publications[0]),
                call(feed.groups[0].publications[0].metadata),
                call(feed.groups[0].publications[0].links),
                call(feed.groups[0].publications[0].links[0]),
            ],
            any_order=False,
        )
