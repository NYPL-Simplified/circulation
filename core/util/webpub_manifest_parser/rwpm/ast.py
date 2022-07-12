from core.util.webpub_manifest_parser.core.ast import CompactCollectionProperty, Manifestlike
from core.util.webpub_manifest_parser.core.properties import ArrayOfStringsProperty
from core.util.webpub_manifest_parser.rwpm.registry import RWPMCollectionRolesRegistry


class RWPMManifest(Manifestlike):
    """Readium Web Publication Manifest."""

    # https://github.com/readium/webpub-manifest#22-metadata
    DEFAULT_CONTEXT = "https://readium.org/webpub-manifest/context.jsonld"

    def __init__(  # pylint: disable=R0913
        self,
        metadata=None,
        links=None,
        reading_order=None,
        context=None,
        resources=None,
        toc=None,
    ):
        """Initialize a new instance of RWPMManifest class.

        :param metadata: RWPM metadata
        :type metadata: core.util.webpub_manifest_parser.core.ast.Metadata

        :param links: Links
        :type links: core.util.webpub_manifest_parser.core.ast.LinkList

        :param reading_order: Reading order sub-collection
        :type reading_order: core.util.webpub_manifest_parser.core.ast.CompactCollection

        :param resources: (Optional) Resources sub-collection
        :type resources: core.util.webpub_manifest_parser.core.ast.CompactCollection

        :param context: (Optional) RWPM manifest's context
        :type context: list

        :param toc: (Optional) TOC sub-collection
        :type toc: core.util.webpub_manifest_parser.core.ast.CompactCollection
        """
        super(RWPMManifest, self).__init__()

        self.metadata = metadata
        self.links = links
        self.reading_order = reading_order
        self.context = context
        self.resources = resources
        self.toc = toc

    context = ArrayOfStringsProperty(
        "@context", required=False, unique_items=True, default_value=DEFAULT_CONTEXT
    )
    reading_order = CompactCollectionProperty(
        "readingOrder", required=True, role=RWPMCollectionRolesRegistry.READING_ORDER
    )
    resources = CompactCollectionProperty(
        "resources", required=False, role=RWPMCollectionRolesRegistry.RESOURCES
    )
    toc = CompactCollectionProperty(
        "toc", required=False, role=RWPMCollectionRolesRegistry.TOC
    )
