import datetime
from unittest import TestCase

import six
from parameterized import parameterized
from pyfakefs.fake_filesystem_unittest import Patcher

from core.util.webpub_manifest_parser.core.ast import (
    CompactCollection,
    Contributor,
    Link,
    LinkList,
    Metadata,
    PresentationMetadata,
    Subject,
)
from core.util.webpub_manifest_parser.core.parsers import DocumentParser, ValueParsingError
from core.util.webpub_manifest_parser.core.syntax import MissingPropertyError
from core.util.webpub_manifest_parser.rwpm.ast import RWPMManifest
from core.util.webpub_manifest_parser.rwpm.registry import (
    RWPMLinkRelationsRegistry,
    RWPMMediaTypesRegistry,
)
from core.util.webpub_manifest_parser.rwpm.syntax import RWPMSyntaxAnalyzer

RWPM_MANIFEST_WITHOUT_METADATA = """
{
    "links": [
        {"rel": "self", "href": "https://example.com/manifest.json", "type": "application/webpub+json"}
    ],
    
    "readingOrder": [
        {"href": "https://example.com/c001.html", "type": "text/html", "title": "Chapter 1"}
    ]
}
"""

RWPM_MANIFEST_WITH_MISSING_METADATA_TITLE_PROPERTY = """
{
    "metadata": {
        "@type": "http://schema.org/Book",
        "author": "Herman Melville",
        "identifier": "urn:isbn:978031600000X",
        "language": "en",
        "modified": "2015-09-29T17:00:00Z"
    },
    
    "links": [
        {"rel": "self", "href": "https://example.com/manifest.json", "type": "application/webpub+json"}
    ],
    
    "readingOrder": [
        {"href": "https://example.com/c001.html", "type": "text/html", "title": "Chapter 1"}
    ]
}
"""

RWPM_MANIFEST_WITH_MISSING_CONTRIBUTOR_NAME_PROPERTY = """
{
    "metadata": {
        "@type": "http://schema.org/Book",
        "author": {
            "identifier": "urn:123:456"
        },
        "identifier": "urn:isbn:978031600000X",
        "language": "en",
        "modified": "2015-09-29T17:00:00Z"
    },

    "links": [
        {"rel": "self", "href": "https://example.com/manifest.json", "type": "application/webpub+json"}
    ],

    "readingOrder": [
        {"href": "https://example.com/c001.html", "type": "text/html", "title": "Chapter 1"}
    ]
}
"""

RWPM_MANIFEST_WITH_MISSING_SUBJECT_NAME_PROPERTY = """
{
    "metadata": {
        "@type": "http://schema.org/Book",
        "subject": {
            "identifier": "urn:123:456"
        },
        "identifier": "urn:isbn:978031600000X",
        "language": "en",
        "modified": "2015-09-29T17:00:00Z"
    },

    "links": [
        {"rel": "self", "href": "https://example.com/manifest.json", "type": "application/webpub+json"}
    ],

    "readingOrder": [
        {"href": "https://example.com/c001.html", "type": "text/html", "title": "Chapter 1"}
    ]
}
"""

RWPM_MANIFEST_WITHOUT_LINKS = """
{
    "metadata": {
        "@type": "http://schema.org/Book",
        "title": "Moby-Dick",
        "author": "Herman Melville",
        "identifier": "urn:isbn:978031600000X",
        "language": "en",
        "modified": "2015-09-29T17:00:00Z"
    },
    
    "readingOrder": [
        {"href": "https://example.com/c001.html", "type": "text/html", "title": "Chapter 1"}
    ]
}
"""

RWPM_MANIFEST_WITH_MISSING_LINK_HREF_PROPERTY = """
{
    "@context": "https://readium.org/webpub-manifest/context.jsonld",

    "metadata": {
        "@type": "http://schema.org/Book",
        "title": "Moby-Dick",
        "author": "Herman Melville",
        "identifier": "urn:isbn:978031600000X",
        "language": "en",
        "modified": "2015-09-29T17:00:00Z"
    },

    "links": [
        {"rel": "self", "type": "application/webpub+json"}
    ],
    
    "readingOrder": [
        {"href": "https://example.com/c001.html", "type": "text/html", "title": "Chapter 1"}
    ]
}
"""

RWPM_MANIFEST_WITH_MISSING_READING_ORDER = """
{
    "@context": "https://readium.org/webpub-manifest/context.jsonld",

    "metadata": {
        "@type": "http://schema.org/Book",
        "title": "Moby-Dick",
        "author": "Herman Melville",
        "identifier": "urn:isbn:978031600000X",
        "language": "en",
        "modified": "2015-09-29T17:00:00Z"
    },

    "links": [
        {"rel": "self", "href": "https://example.com/manifest.json", "type": "application/webpub+json"}
    ]
}
"""

RWPM_MANIFEST_WINT_INCORRECT_CONTEXT = """
{
    "@context": [
        "context1",
        "context1"
    ],

    "metadata": {
        "@type": "http://schema.org/Book",
        "title": "Moby-Dick",
        "author": "Herman Melville",
        "identifier": "urn:isbn:978031600000X",
        "language": "en",
        "modified": "2015-09-29T17:00:00Z"
    },

    "links": [
        {"rel": "self", "href": "https://example.com/manifest.json", "type": "application/webpub+json"}
    ],

    "readingOrder": [
        {"href": "https://example.com/c001.html", "type": "text/html", "title": "Chapter 1"}
    ]
}
"""

RWPM_MANIFEST_WITH_INCORRECT_METADATA_MODIFIED_PROPERTY = """
{
    "@context": "https://readium.org/webpub-manifest/context.jsonld",

    "metadata": {
        "@type": "http://schema.org/Book",
        "title": "Moby-Dick",
        "author": "Herman Melville",
        "identifier": "urn:isbn:978031600000X",
        "language": "en",
        "modified": "2015-09-29T17:00:00Z---"
    },

    "links": [
        {"rel": "self", "href": "https://example.com/manifest.json", "type": "application/webpub+json"}
    ],

    "readingOrder": [
        {"href": "https://example.com/c001.html", "type": "text/html", "title": "Chapter 1"}
    ]
}
"""

RWPM_MANIFEST_WITH_INCORRECT_METADATA_LANGUAGE_PROPERTY = """
{
    "@context": "https://readium.org/webpub-manifest/context.jsonld",

    "metadata": {
        "@type": "http://schema.org/Book",
        "title": "Moby-Dick",
        "author": "Herman Melville",
        "identifier": "urn:isbn:978031600000X",
        "language": "eng;dat",
        "modified": "2015-09-29T17:00:00Z"
    },

    "links": [
        {"rel": "self", "href": "https://example.com/manifest.json", "type": "application/webpub+json"}
    ],

    "readingOrder": [
        {"href": "https://example.com/c001.html", "type": "text/html", "title": "Chapter 1"}
    ]
}
"""

RWPM_MANIFEST_WITH_INCORRECT_CONTRIBUTOR_IDENTIFIER_PROPERTY = """
{
    "@context": "https://readium.org/webpub-manifest/context.jsonld",

    "metadata": {
        "@type": "http://schema.org/Book",
        "title": "Moby-Dick",
        "author": "Herman Melville",
        "identifier": "x",
        "language": [
            "eng",
            "dat"
        ],
        "modified": "2015-09-29T17:00:00Z"
    },

    "links": [
        {"rel": "self", "href": "https://example.com/manifest.json", "type": "application/webpub+json"}
    ],

    "readingOrder": [
        {"href": "https://example.com/c001.html", "type": "text/html", "title": "Chapter 1"}
    ]
}
"""

RWPM_MANIFEST_WITH_INCORRECT_LINK_HEIGHT_PROPERTY = """
{
    "@context": "https://readium.org/webpub-manifest/context.jsonld",

    "metadata": {
        "@type": "http://schema.org/Book",
        "title": "Moby-Dick",
        "author": "Herman Melville",
        "identifier": "urn:isbn:978031600000X",
        "language": [
            "eng",
            "dat"
        ],
        "modified": "2015-09-29T17:00:00Z"
    },

    "links": [
        {"rel": "self", "href": "https://example.com/manifest.json", "type": "application/webpub+json", "height": -10}
    ],

    "readingOrder": [
        {"href": "https://example.com/c001.html", "type": "text/html", "title": "Chapter 1"}
    ]
}
"""

RWPM_MANIFEST_WITH_AUTHOR_ENCODED_AS_STRING = """
{
    "@context": "https://readium.org/webpub-manifest/context.jsonld",

    "metadata": {
        "@type": "http://schema.org/Book",
        "title": "Moby-Dick",
        "author": "Herman Melville",
        "identifier": "urn:isbn:978031600000X",
        "language": "en",
        "modified": "2015-09-29T17:00:00Z"
    },

    "links": [
        {"rel": "self", "href": "https://example.com/manifest.json", "type": "application/webpub+json"}
    ],

    "readingOrder": [
        {"href": "https://example.com/c001.html", "type": "text/html", "title": "Chapter 1"}
    ]
}
"""

RWPM_MANIFEST_WITH_AUTHORS_ENCODED_AS_ARRAY_OF_STRINGS = """
{
    "@context": "https://readium.org/webpub-manifest/context.jsonld",

    "metadata": {
        "@type": "http://schema.org/Book",
        "title": "Moby-Dick",
        "author": [
            "Herman Melville 1",
            "Herman Melville 2"
        ],
        "identifier": "urn:isbn:978031600000X",
        "language": "en",
        "modified": "2015-09-29T17:00:00Z"
    },

    "links": [
        {"rel": "self", "href": "https://example.com/manifest.json", "type": "application/webpub+json"}
    ],

    "readingOrder": [
        {"href": "https://example.com/c001.html", "type": "text/html", "title": "Chapter 1"}
    ]
}
"""

RWPM_MANIFEST_WITH_AUTHOR_ENCODED_AS_OBJECT = """
{
    "@context": "https://readium.org/webpub-manifest/context.jsonld",

    "metadata": {
        "@type": "http://schema.org/Book",
        "title": "Moby-Dick",
        "author": {
            "name": "Herman Melville"
        },
        "identifier": "urn:isbn:978031600000X",
        "language": "en",
        "modified": "2015-09-29T17:00:00Z"
    },

    "links": [
        {"rel": "self", "href": "https://example.com/manifest.json", "type": "application/webpub+json"}
    ],

    "readingOrder": [
        {"href": "https://example.com/c001.html", "type": "text/html", "title": "Chapter 1"}
    ]
}
"""

RWPM_MANIFEST_WITH_AUTHORS_ENCODED_AS_ARRAY_OF_OBJECTS = """
{
    "@context": "https://readium.org/webpub-manifest/context.jsonld",

    "metadata": {
        "@type": "http://schema.org/Book",
        "title": "Moby-Dick",
        "author": [
            {
                "name": "Herman Melville 1"
            },
            {
                "name": "Herman Melville 2"
            }
        ],
        "identifier": "urn:isbn:978031600000X",
        "language": "en",
        "modified": "2015-09-29T17:00:00Z"
    },

    "links": [
        {"rel": "self", "href": "https://example.com/manifest.json", "type": "application/webpub+json"}
    ],

    "readingOrder": [
        {"href": "https://example.com/c001.html", "type": "text/html", "title": "Chapter 1"}
    ]
}
"""

RWPM_MANIFEST_WITH_AUTHORS_ENCODED_AS_MIXED_ARRAY_OF_STRINGS_AND_OBJECTS = """
{
    "@context": "https://readium.org/webpub-manifest/context.jsonld",

    "metadata": {
        "@type": "http://schema.org/Book",
        "title": "Moby-Dick",
        "author": [
            "Herman Melville 1",
            {
                "name": "Herman Melville 2"
            }
        ],
        "identifier": "urn:isbn:978031600000X",
        "language": "en",
        "modified": "2015-09-29T17:00:00Z"
    },

    "links": [
        {"rel": "self", "href": "https://example.com/manifest.json", "type": "application/webpub+json"}
    ],

    "readingOrder": [
        {"href": "https://example.com/c001.html", "type": "text/html", "title": "Chapter 1"}
    ]
}
"""

RWPM_MANIFEST = """
{
    "@context": "https://readium.org/webpub-manifest/context.jsonld",

    "metadata": {
        "@type": "http://schema.org/Book",
        "title": "Moby-Dick",
        "author": "Herman Melville",
        "identifier": "urn:isbn:978031600000X",
        "language": "en",
        "modified": "2015-09-29T17:00:00Z"
    },

    "links": [
        {"rel": "self", "href": "https://example.com/manifest.json", "type": "application/webpub+json"}
    ],

    "readingOrder": [
        {"href": "https://example.com/c001.html", "type": "text/html", "title": "Chapter 1"}
    ]
}
"""


class RWPMSyntaxAnalyzerTest(TestCase):
    @parameterized.expand(
        [
            (
                "when_manifest_does_not_contain_metadata",
                RWPM_MANIFEST_WITHOUT_METADATA,
                RWPMManifest,
                RWPMManifest.metadata.key,
            ),
            (
                "when_manifest_metadata_does_not_contain_title",
                RWPM_MANIFEST_WITH_MISSING_METADATA_TITLE_PROPERTY,
                PresentationMetadata,
                PresentationMetadata.title.key,
            ),
            (
                "when_metadata_contributor_does_not_contain_name",
                RWPM_MANIFEST_WITH_MISSING_CONTRIBUTOR_NAME_PROPERTY,
                Contributor,
                Contributor.name.key,
            ),
            (
                "when_metadata_subject_does_not_contain_name",
                RWPM_MANIFEST_WITH_MISSING_SUBJECT_NAME_PROPERTY,
                Subject,
                Subject.name.key,
            ),
            (
                "when_manifest_does_not_contain_links",
                RWPM_MANIFEST_WITHOUT_LINKS,
                RWPMManifest,
                RWPMManifest.links.key,
            ),
            (
                "when_manifest_link_does_not_contain_href",
                RWPM_MANIFEST_WITH_MISSING_LINK_HREF_PROPERTY,
                Link,
                Link.href.key,
            ),
            (
                "when_manifest_link_does_not_contain_reading_order_sub_collection",
                RWPM_MANIFEST_WITH_MISSING_READING_ORDER,
                RWPMManifest,
                RWPMManifest.reading_order.key,
            ),
        ]
    )
    def test_syntax_analyzer_raises_missing_property_error_correctly(
        self,
        _,
        rwpm_manifest_content,
        expected_class_with_missing_property,
        expected_missing_property,
    ):
        # Arrange
        syntax_analyzer = RWPMSyntaxAnalyzer()
        input_steam = six.StringIO(rwpm_manifest_content)
        manifest_json = DocumentParser.get_manifest_json(input_steam)

        # Act
        with self.assertRaises(MissingPropertyError) as assert_raises_context:
            syntax_analyzer.analyze(manifest_json)

        # Assert
        self.assertEqual(
            expected_class_with_missing_property,
            assert_raises_context.exception.cls,
        )
        self.assertEqual(
            expected_missing_property,
            assert_raises_context.exception.object_property.key,
        )

    @parameterized.expand(
        [
            (
                "when_manifest_context_has_duplicated_items",
                RWPM_MANIFEST_WINT_INCORRECT_CONTEXT,
                "Item 'context1' is not unique",
            ),
            (
                "when_metadata_modified_property_has_incorrect_format",
                RWPM_MANIFEST_WITH_INCORRECT_METADATA_MODIFIED_PROPERTY,
                "'2015-09-29T17:00:00Z---' is not a 'date-time'",
            ),
            (
                "when_metadata_modified_language_has_incorrect_format",
                RWPM_MANIFEST_WITH_INCORRECT_METADATA_LANGUAGE_PROPERTY,
                "String value 'eng;dat' does not match regular expression ^((?P<grandfathered>(en-GB-oed|i-ami|i-bnn|i-default|i-enochian|i-hak|i-klingon|i-lux|i-mingo|i-navajo|i-pwn|i-tao|i-tay|i-tsu|sgn-BE-FR|sgn-BE-NL|sgn-CH-DE)|(art-lojban|cel-gaulish|no-bok|no-nyn|zh-guoyu|zh-hakka|zh-min|zh-min-nan|zh-xiang))|((?P<language>([A-Za-z]{2,3}(-(?P<extlang>[A-Za-z]{3}(-[A-Za-z]{3}){0,2}))?)|[A-Za-z]{4}|[A-Za-z]{5,8})(-(?P<script>[A-Za-z]{4}))?(-(?P<region>[A-Za-z]{2}|[0-9]{3}))?(-(?P<variant>[A-Za-z0-9]{5,8}|[0-9][A-Za-z0-9]{3}))*(-(?P<extension>[0-9A-WY-Za-wy-z](-[A-Za-z0-9]{2,8})+))*(-(?P<privateUse>x(-[A-Za-z0-9]{1,8})+))?)|(?P<privateUse2>x(-[A-Za-z0-9]{1,8})+))$",
            ),
            (
                "when_contributor_identifier_has_incorrect_format",
                RWPM_MANIFEST_WITH_INCORRECT_CONTRIBUTOR_IDENTIFIER_PROPERTY,
                "'x' is not a 'uri'",
            ),
            (
                "when_link_height_less_or_equal_than_the_exclusive_minimum",
                RWPM_MANIFEST_WITH_INCORRECT_LINK_HEIGHT_PROPERTY,
                "Value -10 is less or equal than the exclusive minimum (0)",
            ),
        ]
    )
    def test_syntax_analyzer_raises_value_parsing_error_when_property_has_incorrect_value(
        self, _, rwpm_manifest_content, expected_error_message
    ):
        # Arrange
        syntax_analyzer = RWPMSyntaxAnalyzer()
        input_steam = six.StringIO(rwpm_manifest_content)
        manifest_json = DocumentParser.get_manifest_json(input_steam)

        # Act
        with self.assertRaises(ValueParsingError) as assert_raises_context:
            syntax_analyzer.analyze(manifest_json)

        # Assert
        self.assertEqual(
            expected_error_message,
            six.text_type(assert_raises_context.exception).strip("u"),
        )

    @parameterized.expand(
        [
            (
                "when_manifest_has_author_encoded_as_string",
                RWPM_MANIFEST_WITH_AUTHOR_ENCODED_AS_STRING,
                [Contributor(name="Herman Melville", roles=[], links=LinkList())],
            ),
            (
                "when_manifest_has_authors_encoded_as_array_of_strings",
                RWPM_MANIFEST_WITH_AUTHORS_ENCODED_AS_ARRAY_OF_STRINGS,
                [
                    Contributor(name="Herman Melville 1", roles=[], links=LinkList()),
                    Contributor(name="Herman Melville 2", roles=[], links=LinkList()),
                ],
            ),
            (
                "when_manifest_has_author_encoded_as_object",
                RWPM_MANIFEST_WITH_AUTHOR_ENCODED_AS_OBJECT,
                [Contributor(name="Herman Melville", roles=[], links=LinkList())],
            ),
            (
                "when_manifest_has_authors_encoded_as_array_of_objects",
                RWPM_MANIFEST_WITH_AUTHORS_ENCODED_AS_ARRAY_OF_OBJECTS,
                [
                    Contributor(name="Herman Melville 1", roles=[], links=LinkList()),
                    Contributor(name="Herman Melville 2", roles=[], links=LinkList()),
                ],
            ),
            (
                "when_manifest_has_authors_encoded_as_mixed_array_of_strings_and_objects",
                RWPM_MANIFEST_WITH_AUTHORS_ENCODED_AS_MIXED_ARRAY_OF_STRINGS_AND_OBJECTS,
                [
                    Contributor(name="Herman Melville 1", roles=[], links=LinkList()),
                    Contributor(name="Herman Melville 2", roles=[], links=LinkList()),
                ],
            ),
        ]
    )
    def test_syntax_analyzer_correctly_parses_contributor_metadata_as_contributor_objects(
        self, _, rwpm_manifest_content, expected_authors
    ):
        # Arrange
        syntax_analyzer = RWPMSyntaxAnalyzer()
        input_steam = six.StringIO(rwpm_manifest_content)
        manifest_json = DocumentParser.get_manifest_json(input_steam)

        # Act
        manifest = syntax_analyzer.analyze(manifest_json)

        # Assert
        self.assertEqual(expected_authors, manifest.metadata.authors)

    def test_syntax_analyzer_returns_ast(self):
        # Arrange
        syntax_analyzer = RWPMSyntaxAnalyzer()
        input_steam = six.StringIO(RWPM_MANIFEST)
        manifest_json = DocumentParser.get_manifest_json(input_steam)

        # Act
        manifest = syntax_analyzer.analyze(manifest_json)

        # Assert
        self.assertIsInstance(manifest, RWPMManifest)

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
        self.assertEqual(1, len(manifest.links))
        [link] = manifest.links

        self.assertIsInstance(link.rels, list)
        self.assertEqual(1, len(link.rels))
        self.assertEqual(RWPMLinkRelationsRegistry.SELF.key, link.rels[0])
        self.assertEqual("https://example.com/manifest.json", link.href)
        self.assertEqual(RWPMMediaTypesRegistry.MANIFEST.key, link.type)

        self.assertIsInstance(manifest.reading_order, CompactCollection)
        self.assertIsInstance(manifest.reading_order.links, list)
        self.assertEqual(1, len(manifest.reading_order.links))
        [reading_order_link] = manifest.reading_order.links
        self.assertEqual("https://example.com/c001.html", reading_order_link.href)
        self.assertEqual(RWPMMediaTypesRegistry.HTML.key, reading_order_link.type)
        self.assertEqual("Chapter 1", reading_order_link.title)
