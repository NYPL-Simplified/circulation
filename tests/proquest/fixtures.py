# coding=utf-8

import datetime

from webpub_manifest_parser.core.ast import (
    CollectionList,
    CompactCollection,
    Link,
    LinkList,
    Node,
    PresentationMetadata,
)
from webpub_manifest_parser.core.parsers import TypeParser
from webpub_manifest_parser.core.properties import BaseArrayProperty, PropertiesGrouping
from webpub_manifest_parser.core.registry import RegistryItem
from webpub_manifest_parser.opds2.ast import (
    OPDS2Feed,
    OPDS2FeedMetadata,
    OPDS2Group,
    OPDS2Publication,
)
from webpub_manifest_parser.opds2.registry import OPDS2LinkRelationsRegistry
from webpub_manifest_parser.rwpm.registry import RWPMLinkRelationsRegistry


def serialize(rwpm_item):
    """Serialize RWPM AST node into a Python dictionary.

    :param rwpm_item: RWPM AST node
    :type rwpm_item: Node

    :return: Dictionary containing properties of the serialized RWPM AST node
    :rtype: dict
    """
    if isinstance(rwpm_item, list):
        result = []

        for i in rwpm_item:
            result.append(serialize(i))

        return result

    result = {}

    if isinstance(rwpm_item, Node):

        required_properties = PropertiesGrouping.get_class_properties(
            rwpm_item.__class__
        )

        for (property_name, property_object) in required_properties:
            property_value = getattr(rwpm_item, property_name, None)

            if property_value is None and property_object.required:
                if property_object.default_value:
                    property_value = property_object.default_value
                elif isinstance(property_object, BaseArrayProperty) or (
                    isinstance(property_object.parser, TypeParser)
                    and issubclass(property_object.parser.type, CompactCollection)
                ):
                    property_value = []

            if isinstance(property_value, Node):
                property_value = serialize(property_value)
            elif isinstance(property_value, list):
                property_value = serialize(property_value)
            elif isinstance(property_value, datetime.datetime):
                property_value = property_value.isoformat() + "Z"
            if isinstance(rwpm_item, list):
                result.append(property_value)
            else:
                result[property_object.key] = property_value
    elif isinstance(rwpm_item, RegistryItem):
        result = rwpm_item.key

    return result


PROQUEST_PUBLICATION_1 = OPDS2Publication(
    metadata=PresentationMetadata(
        identifier="urn:proquest.com/document-id/1",
        title="Publićation # 1",
        modified=datetime.datetime(2020, 1, 31, 0, 0, 0),
    ),
    links=LinkList(
        [
            Link(
                href="https://feed.org/document-id/1",
                rels=[OPDS2LinkRelationsRegistry.ACQUISITION],
            )
        ]
    ),
)

PROQUEST_PUBLICATION_2 = OPDS2Publication(
    metadata=PresentationMetadata(
        identifier="urn:proquest.com/document-id/2",
        title="Publication # 2",
        modified=datetime.datetime(2020, 1, 30, 0, 0, 0),
    ),
    links=LinkList(
        [
            Link(
                href="https://feed.org/document-id/2",
                rels=[OPDS2LinkRelationsRegistry.ACQUISITION],
            )
        ]
    ),
)

PROQUEST_PUBLICATION_3 = OPDS2Publication(
    metadata=PresentationMetadata(
        identifier="urn:proquest.com/document-id/3",
        title="Publication # 3",
        modified=datetime.datetime(2020, 1, 29, 0, 0, 0),
    ),
    links=LinkList(
        [
            Link(
                href="https://feed.org/document-id/3",
                rels=[OPDS2LinkRelationsRegistry.ACQUISITION],
            )
        ]
    ),
)

PROQUEST_PUBLICATION_4 = OPDS2Publication(
    metadata=PresentationMetadata(
        identifier="urn:proquest.com/document-id/4",
        title="Publication # 4",
        modified=datetime.datetime(2020, 1, 28, 0, 0, 0),
    ),
    links=LinkList(
        [
            Link(
                href="https://feed.org/document-id/4",
                rels=[OPDS2LinkRelationsRegistry.ACQUISITION],
            )
        ]
    ),
)

PROQUEST_FEED_PAGE_1 = OPDS2Feed(
    metadata=OPDS2FeedMetadata(
        title="Page # 1", current_page=1, items_per_page=10, number_of_items=20
    ),
    groups=CollectionList(
        [
            OPDS2Group(
                publications=CollectionList(
                    [PROQUEST_PUBLICATION_1, PROQUEST_PUBLICATION_2]
                )
            )
        ]
    ),
    links=LinkList(
        [Link(href="https://feed.org/pages/1", rels=[RWPMLinkRelationsRegistry.SELF])]
    ),
)

PROQUEST_FEED_PAGE_2 = OPDS2Feed(
    metadata=OPDS2FeedMetadata(
        title="Page # 2", current_page=2, items_per_page=10, number_of_items=20
    ),
    groups=CollectionList(
        [
            OPDS2Group(
                publications=CollectionList(
                    [PROQUEST_PUBLICATION_3, PROQUEST_PUBLICATION_4]
                )
            )
        ]
    ),
    links=LinkList(
        [Link(href="https://feed.org/pages/2", rels=[RWPMLinkRelationsRegistry.SELF])]
    ),
)

PROQUEST_RAW_PUBLICATION_1_ID = "12345"
PROQUEST_RAW_PUBLICATION_1_COVER_HREF = "http://proquest.com/covers/12345-m.jpg"

PROQUEST_RAW_PUBLICATION_2_ID = "12346"
PROQUEST_RAW_PUBLICATION_2_COVER_HREF = "http://proquest.com/covers/12346-m.jpg"

PROQUEST_RAW_FEED = """{{
  "metadata": {{
    "title": "Test Feed",
    "itemsPerPage": 1,
    "numberOfItems": 1
  }},
  "links": [{{
    "href": "https://drafts.opds.io/schema/feed.schema.json",
    "type": "application/opds+json",
    "rel": "self",
    "alternate": [],
    "children": []
  }}],
  "publications": [],
  "navigation": [{{
    "href": "https://drafts.opds.io/schema/feed.schema.json",
    "type": "application/opds+json",
    "title": "Test",
    "rel": "self",
    "alternate": [],
    "children": []
  }}],
  "facets": [],
  "groups": [{{
    "metadata": {{
      "title": "Test Group"
    }},
    "links": [{{
      "href": "https://drafts.opds.io/schema/feed.schema.json",
      "type": "application/opds+json",
      "rel": "self",
      "alternate": [],
      "children": []
    }}],
    "publications": [{{
      "metadata": {{
        "identifier": "urn:proquest.com/document-id/{0}",
        "@type": "http://schema.org/Book",
        "title": "Test Book 1",
        "modified": "2020-11-19T08:00:00.000Z",
        "published": "2020-01-15T08:06:00.000Z",
        "language": [
          "eng"
        ],
        "author": [{{
          "name": "Test, Author",
          "links": [{{
            "href": "https://catalog.feedbooks.com/catalog/index.json",
            "type": "application/opds+json",
            "alternate": [],
            "children": []
          }}]
        }}],
        "publisher": {{
          "name": "Test Publisher",
          "links": []
        }},
        "subject": [],
        "readingProgression": "ltr"
      }},
      "links": [{{
        "href": "https://proquest.com/lib/detail.action?docID={0}",
        "type": "application/vnd.adobe.adept+xml",
        "rel": "http://opds-spec.org/acquisition",
        "properties": {{
          "indirectAcquisition": [{{
            "type": "application/epub+zip",
            "alternate": [],
            "children": []
          }}]
        }},
        "language": [
          "eng"
        ],
        "alternate": [],
        "children": []
      }}],
      "images": [{{
        "href": "{1}",
        "type": "image/jpeg",
        "language": [
          "eng"
        ],
        "alternate": [],
        "children": []
      }}]
    }},
    {{
      "metadata": {{
        "identifier": "urn:proquest.com/document-id/{2}",
        "@type": "http://schema.org/Book",
        "title": "Test Book 2",
        "modified": "2020-11-19T08:00:00.000Z",
        "published": "2020-01-15T08:06:00.000Z",
        "language": [
          "eng"
        ],
        "author": [{{
          "name": "Test, Author",
          "links": [{{
            "href": "https://catalog.feedbooks.com/catalog/index.json",
            "type": "application/opds+json",
            "alternate": [],
            "children": []
          }}]
        }}],
        "publisher": {{
          "name": "Test Publisher",
          "links": []
        }},
        "subject": [],
        "readingProgression": "ltr"
      }},
      "links": [{{
        "href": "https://proquest.com/lib/detail.action?docID={2}",
        "type": "application/vnd.adobe.adept+xml",
        "rel": "http://opds-spec.org/acquisition",
        "properties": {{
          "indirectAcquisition": [{{
            "type": "application/epub+zip",
            "alternate": [],
            "children": []
          }}]
        }},
        "language": [
          "eng"
        ],
        "alternate": [],
        "children": []
      }}],
      "images": [{{
        "href": "{3}",
        "type": "image/jpeg",
        "language": [
          "eng"
        ],
        "alternate": [],
        "children": []
      }}]
    }}]
  }}]
}}
""".format(
    PROQUEST_RAW_PUBLICATION_1_ID,
    PROQUEST_RAW_PUBLICATION_1_COVER_HREF,
    PROQUEST_RAW_PUBLICATION_2_ID,
    PROQUEST_RAW_PUBLICATION_2_COVER_HREF,
)

PROQUEST_RAW_PUBLICATION_3_ID = "12347"
PROQUEST_RAW_PUBLICATION_3_COVER_HREF = "http://proquest.com/covers/12347-m.jpg"

PROQUEST_RAW_FEED_WITH_A_REMOVED_PUBLICATION = """{{
  "metadata": {{
    "title": "Test Feed",
    "itemsPerPage": 1,
    "numberOfItems": 1
  }},
  "links": [{{
    "href": "https://drafts.opds.io/schema/feed.schema.json",
    "type": "application/opds+json",
    "rel": "self",
    "alternate": [],
    "children": []
  }}],
  "publications": [],
  "navigation": [{{
    "href": "https://drafts.opds.io/schema/feed.schema.json",
    "type": "application/opds+json",
    "title": "Test",
    "rel": "self",
    "alternate": [],
    "children": []
  }}],
  "facets": [],
  "groups": [{{
    "metadata": {{
      "title": "Test Group"
    }},
    "links": [{{
      "href": "https://drafts.opds.io/schema/feed.schema.json",
      "type": "application/opds+json",
      "rel": "self",
      "alternate": [],
      "children": []
    }}],
    "publications": [{{
      "metadata": {{
        "identifier": "urn:proquest.com/document-id/{0}",
        "@type": "http://schema.org/Book",
        "title": "Test Book 1",
        "modified": "2020-11-19T08:00:00.000Z",
        "published": "2020-01-15T08:06:00.000Z",
        "language": [
          "eng"
        ],
        "author": [{{
          "name": "Test, Author",
          "links": [{{
            "href": "https://catalog.feedbooks.com/catalog/index.json",
            "type": "application/opds+json",
            "alternate": [],
            "children": []
          }}]
        }}],
        "publisher": {{
          "name": "Test Publisher",
          "links": []
        }},
        "subject": [],
        "readingProgression": "ltr"
      }},
      "links": [{{
        "href": "https://proquest.com/lib/detail.action?docID={0}",
        "type": "application/vnd.adobe.adept+xml",
        "rel": "http://opds-spec.org/acquisition",
        "properties": {{
          "indirectAcquisition": [{{
            "type": "application/epub+zip",
            "alternate": [],
            "children": []
          }}]
        }},
        "language": [
          "eng"
        ],
        "alternate": [],
        "children": []
      }}],
      "images": [{{
        "href": "{1}",
        "type": "image/jpeg",
        "language": [
          "eng"
        ],
        "alternate": [],
        "children": []
      }}]
    }},
    {{
      "metadata": {{
        "identifier": "urn:proquest.com/document-id/{2}",
        "@type": "http://schema.org/Book",
        "title": "Test Book 3",
        "modified": "2020-11-19T08:00:00.000Z",
        "published": "2020-01-15T08:06:00.000Z",
        "language": [
          "eng"
        ],
        "author": [{{
          "name": "Test, Author",
          "links": [{{
            "href": "https://catalog.feedbooks.com/catalog/index.json",
            "type": "application/opds+json",
            "alternate": [],
            "children": []
          }}]
        }}],
        "publisher": {{
          "name": "Test Publisher",
          "links": []
        }},
        "subject": [],
        "readingProgression": "ltr"
      }},
      "links": [{{
        "href": "https://proquest.com/lib/detail.action?docID={2}",
        "type": "application/vnd.adobe.adept+xml",
        "rel": "http://opds-spec.org/acquisition",
        "properties": {{
          "indirectAcquisition": [{{
            "type": "application/epub+zip",
            "alternate": [],
            "children": []
          }}]
        }},
        "language": [
          "eng"
        ],
        "alternate": [],
        "children": []
      }}],
      "images": [{{
        "href": "{3}",
        "type": "image/jpeg",
        "language": [
          "eng"
        ],
        "alternate": [],
        "children": []
      }}]
    }}]
  }}]
}}
""".format(
    PROQUEST_RAW_PUBLICATION_1_ID,
    PROQUEST_RAW_PUBLICATION_1_COVER_HREF,
    PROQUEST_RAW_PUBLICATION_3_ID,
    PROQUEST_RAW_PUBLICATION_3_COVER_HREF,
)
