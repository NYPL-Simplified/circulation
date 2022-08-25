from core.util.webpub_manifest_parser.core.registry import CollectionRole, LinkRelation, MediaType
from core.util.webpub_manifest_parser.rwpm.registry import (
    RWPMCollectionRolesRegistry,
    RWPMLinkRelationsRegistry,
    RWPMMediaTypesRegistry,
)


class OPDS2MediaTypesRegistry(RWPMMediaTypesRegistry):
    """Registry containing OPDS 2.0 media types."""

    # https://drafts.opds.io/opds-2.0.html#overview
    OPDS_FEED = MediaType(key="application/opds+json")

    # https://drafts.opds.io/opds-2.0.html#41-opds-publication
    OPDS_PUBLICATION = MediaType(key="application/opds-publication+json")

    OPDS_MEDIA_TYPES = [OPDS_FEED, OPDS_PUBLICATION]

    def __init__(self):
        """Initialize a new instance of OPDS2MediaTypesRegistry class."""
        super(OPDS2MediaTypesRegistry, self).__init__()

        self._add_items(self.OPDS_MEDIA_TYPES)


class OPDS2LinkRelationsRegistry(RWPMLinkRelationsRegistry):
    """Registry containing OPDS 2.0 link relations."""

    ACQUISITION = LinkRelation(key="http://opds-spec.org/acquisition")
    OPEN_ACCESS = LinkRelation(key="http://opds-spec.org/acquisition/open-access")
    BORROW = LinkRelation(key="http://opds-spec.org/acquisition/borrow")
    BUY = LinkRelation(key="http://opds-spec.org/acquisition/buy")
    SAMPLE = LinkRelation(key="http://opds-spec.org/acquisition/sample")
    PREVIEW = LinkRelation(key="preview")
    SUBSCRIBE = LinkRelation(key="http://opds-spec.org/acquisition/subscribe")

    CORE_LINK_RELATIONS = [
        ACQUISITION,
        OPEN_ACCESS,
        BORROW,
        BUY,
        SAMPLE,
        PREVIEW,
        SUBSCRIBE,
    ]

    def __init__(self):
        """Initialize a new instance of OPDS2LinkRelationsRegistry class."""
        super(OPDS2LinkRelationsRegistry, self).__init__()

        self._add_items(self.CORE_LINK_RELATIONS)


class OPDS2CollectionRolesRegistry(RWPMCollectionRolesRegistry):
    """Registry containing ODPS 2.0 collection roles."""

    NAVIGATION = CollectionRole(key="navigation", compact=True, required=False)
    PUBLICATIONS = CollectionRole(key="publications", compact=False, required=False)
    IMAGES = CollectionRole(key="images", compact=True, required=False)
    FACETS = CollectionRole(key="facets", compact=False, required=False)
    GROUPS = CollectionRole(key="groups", compact=False, required=False)

    OPDS_2_0_ROLES = [NAVIGATION, PUBLICATIONS, IMAGES, FACETS, GROUPS]

    def __init__(self):
        """Initialize a new instance of OPDS2CollectionRolesRegistry class."""
        super(OPDS2CollectionRolesRegistry, self).__init__()

        self._add_items(self.OPDS_2_0_ROLES)
