from core.util.webpub_manifest_parser.core.registry import (
    CollectionRole,
    LinkRelation,
    MediaType,
    Registry,
)


class RWPMCollectionRolesRegistry(Registry):
    """Registry containing collection roles defined in the RWPM spec."""

    READING_ORDER = CollectionRole(key="readingOrder", compact=True, required=True)
    RESOURCES = CollectionRole(key="resources", compact=True, required=False)
    TOC = CollectionRole(key="toc", compact=True, required=False)

    GUIDED = CollectionRole(key="guided", compact=True, required=False)
    LANDMARKS = CollectionRole(key="landmarks", compact=True, required=False)
    LOA = CollectionRole(key="loa", compact=True, required=False)
    LOI = CollectionRole(key="loi", compact=True, required=False)
    LOT = CollectionRole(key="lot", compact=True, required=False)
    LOV = CollectionRole(key="lov", compact=True, required=False)
    PAGE_LIST = CollectionRole(key="pageList", compact=True, required=False)

    CORE_ROLES = [READING_ORDER, RESOURCES, TOC]

    EXTENSIONS = [GUIDED, LANDMARKS, LOA, LOI, LOT, LOV, PAGE_LIST]

    def __init__(self):
        """Initialize a new instance of RWPMCollectionRolesRegistry class."""
        super(RWPMCollectionRolesRegistry, self).__init__(
            self.CORE_ROLES + self.EXTENSIONS
        )


class RWPMLinkRelationsRegistry(Registry):
    """Registry containing link relations mentioned in the RWPM spec."""

    ALTERNATE = LinkRelation(key="alternate")
    CONTENTS = LinkRelation(key="contents")
    COVER = LinkRelation(key="cover")
    MANIFEST = LinkRelation(key="manifest")
    SEARCH = LinkRelation(key="search")
    SELF = LinkRelation(key="self")

    CORE_LINK_RELATIONS = [ALTERNATE, CONTENTS, COVER, MANIFEST, SEARCH, SELF]

    def __init__(self):
        """Initialize a new instance of RWPMLinkRelationsRegistry class."""
        super(RWPMLinkRelationsRegistry, self).__init__(self.CORE_LINK_RELATIONS)


class RWPMMediaTypesRegistry(Registry):
    """Registry containing media types mentioned in the RWPM spec."""

    # https://github.com/readium/webpub-manifest#4-media-type
    MANIFEST = MediaType(key="application/webpub+json")

    # https://github.com/readium/webpub-manifest#6-table-of-contents
    HTML = MediaType(key="text/html")
    CSS = MediaType(key="text/css")

    # https://github.com/readium/webpub-manifest#7-cover
    JPEG = MediaType(key="image/jpeg")
    PNG = MediaType(key="image/png")
    GIF = MediaType(key="image/gif")
    WEBP = MediaType(key="image/webp")
    SVG = MediaType(key="image/svg")
    SVG_XML = MediaType(key="image/svg+xml")

    # https://github.com/readium/webpub-manifest#9-package
    WEB_PUBLICATION_PACKAGE = MediaType("application/webpub+zip")
    EPUB_PUBLICATION_PACKAGE = MediaType("application/epub+zip")

    CORE_TYPES = [
        MANIFEST,
        HTML,
        CSS,
        JPEG,
        PNG,
        GIF,
        WEBP,
        SVG,
        SVG_XML,
        WEB_PUBLICATION_PACKAGE,
        EPUB_PUBLICATION_PACKAGE,
    ]

    def __init__(self):
        """Initialize a new instance of RWPMMediaTypesRegistry class."""
        super(RWPMMediaTypesRegistry, self).__init__(self.CORE_TYPES)
