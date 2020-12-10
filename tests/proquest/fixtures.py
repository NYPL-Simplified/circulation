import datetime

from webpub_manifest_parser.core.ast import CollectionList, PresentationMetadata
from webpub_manifest_parser.opds2.ast import (
    OPDS2Feed,
    OPDS2FeedMetadata,
    OPDS2Group,
    OPDS2Publication,
)

PROQUEST_PUBLICATION_1 = OPDS2Publication(
    metadata=PresentationMetadata(
        identifier="urn:proquest.com/document-id/1",
        modified=datetime.datetime(2020, 1, 31, 0, 0, 0),
    )
)

PROQUEST_PUBLICATION_2 = OPDS2Publication(
    metadata=PresentationMetadata(
        identifier="urn:proquest.com/document-id/2",
        modified=datetime.datetime(2020, 1, 30, 0, 0, 0),
    )
)

PROQUEST_PUBLICATION_3 = OPDS2Publication(
    metadata=PresentationMetadata(
        identifier="urn:proquest.com/document-id/3",
        modified=datetime.datetime(2020, 1, 29, 0, 0, 0),
    )
)

PROQUEST_PUBLICATION_4 = OPDS2Publication(
    metadata=PresentationMetadata(
        identifier="urn:proquest.com/document-id/4",
        modified=datetime.datetime(2020, 1, 28, 0, 0, 0),
    )
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
)

PROQUEST_RAW_PUBLICATION_ID = "12345"
PROQUEST_RAW_PUBLICATION_COVER_HREF = "http://proquest.com/covers/12345-m.jpg"

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
        "title": "Test Book",
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
    }}]
  }}]
}}
""".format(
    PROQUEST_RAW_PUBLICATION_ID, PROQUEST_RAW_PUBLICATION_COVER_HREF
)
