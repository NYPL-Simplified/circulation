from unittest import TestCase

from parameterized import parameterized

from core.util.webpub_manifest_parser.core.ast import (
    Collection,
    CompactCollection,
    Link,
    LinkList,
)
from core.util.webpub_manifest_parser.opds2.ast import OPDS2Navigation
from core.util.webpub_manifest_parser.utils import cast, first_or_default


class UtilsTest(TestCase):
    @parameterized.expand(
        [
            ("returns_None_in_the_case_of_empty_collection", [], None, None),
            (
                "returns_default_value_in_the_case_of_empty_collection",
                [],
                "default",
                "default",
            ),
            ("returns_first_non_empty_value", [1, 2, 3], 1),
        ]
    )
    def test_first_or_default(self, _, collection, expected_result, default_value=None):
        result = first_or_default(collection, default_value)

        assert result == expected_result

    def test_cast(self):
        # Arrange
        navigation_links = LinkList([Link(href="http://example.com")])
        navigation = OPDS2Navigation(links=navigation_links)

        # Act
        casted_navigation = cast(navigation, CompactCollection)

        # Assert
        self.assertIsInstance(casted_navigation, CompactCollection)
        self.assertEqual(navigation_links, casted_navigation.links)
