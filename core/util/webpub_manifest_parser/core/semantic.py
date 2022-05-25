import logging

from multipledispatch import dispatch

from core.util.webpub_manifest_parser.core.ast import (
    Collection,
    CollectionList,
    CompactCollection,
    Link,
    LinkList,
    Manifestlike,
    Metadata,
    Visitor,
)
from core.util.webpub_manifest_parser.core.errors import BaseSemanticError
from core.util.webpub_manifest_parser.core.parsers import (
    URIParser,
    URIReferenceParser,
    ValueParsingError,
)
from core.util.webpub_manifest_parser.rwpm.registry import RWPMLinkRelationsRegistry
from core.util.webpub_manifest_parser.utils import encode, first_or_default

MISSING_MANIFEST_LINK_REL_PROPERTY_ERROR = BaseSemanticError(
    "Manifest link's rel property is missing"
)

MISSING_SELF_LINK_ERROR = BaseSemanticError("Required self link is missing")

WRONG_SELF_LINK_HREF_FORMAT = BaseSemanticError(
    "Self link's href must be an absolute URI to the canonical location of the manifest"
)


class CollectionWrongFormatError(BaseSemanticError):
    """Exception raised in the case when collection's format (compact, full) doesn't not conform with its role."""

    def __init__(self, collection, inner_exception=None):
        """Initialize a new instance of CollectionWrongFormat class.

        :param collection: Collection with a wrong format
        :type collection: python_rwpm_parser.ast.Collection

        :param inner_exception: (Optional) inner exception
        :type inner_exception: Optional[Exception]
        """
        message = "Collection {0} must be {1} but it is not".format(
            collection.role.key, "compact" if collection.role.compact else "full"
        )

        super(CollectionWrongFormatError, self).__init__(message, inner_exception)

        self._collection = collection

    @property
    def collection(self):
        """Return a collection with a wrong format.

        :return: Collection with a wrong format
        :rtype: python_rwpm_parser.ast.Collection
        """
        return self._collection


class SemanticAnalyzer(Visitor):
    """Visitor performing semantic analysis of the RWPM-compatible documents."""

    def __init__(
        self, media_types_registry, link_relations_registry, collection_roles_registry
    ):
        """Initialize a new instance of SemanticAnalyzer.

        :param media_types_registry: Media types registry
        :type media_types_registry: python_rwpm_parser.registry.Registry

        :param link_relations_registry: Link relations registry
        :type link_relations_registry: python_rwpm_parser.registry.Registry

        :param collection_roles_registry: Collections roles registry
        :type collection_roles_registry: python_rwpm_parser.registry.Registry
        """
        self._media_types_registry = media_types_registry
        self._link_relations_registry = link_relations_registry
        self._collection_roles_registry = collection_roles_registry
        self._logger = logging.getLogger(__name__)

    @dispatch(Manifestlike)
    def visit(self, node):
        """Perform semantic analysis of the manifest node.

        :param node: Manifest-like node
        :type node: Manifestlike
        """
        self._logger.debug(u"Started processing {0}".format(encode(node)))

        node.metadata.accept(self)
        node.links.accept(self)

        for link in node.links:
            if not link.rels:
                raise MISSING_MANIFEST_LINK_REL_PROPERTY_ERROR

        self_link = first_or_default(
            node.links.get_by_rel(RWPMLinkRelationsRegistry.SELF.key)
        )

        if self_link is None:
            raise MISSING_SELF_LINK_ERROR

        parser = URIParser()

        try:
            parser.parse(self_link.href)
        except ValueParsingError:
            raise WRONG_SELF_LINK_HREF_FORMAT

        node.sub_collections.accept(self)

        self._logger.debug(u"Finished processing {0}".format(encode(node)))

    @dispatch(Metadata)  # noqa: F811
    def visit(self, node):  # pylint: disable=E0102
        """Perform semantic analysis of the manifest's metadata.

        :param node: Manifest's metadata
        :type node: Metadata
        """
        self._logger.debug(u"Started processing {0}".format(encode(node)))

        self._logger.debug(u"Finished processing {0}".format(encode(node)))

    @dispatch(LinkList)  # noqa: F811
    def visit(self, node):  # pylint: disable=E0102
        """Perform semantic analysis of the list of links.

        :param node: Manifest's metadata
        :type node: LinkList
        """
        self._logger.debug(u"Started processing {0}".format(encode(node)))

        for link in node:
            link.accept(self)

        self._logger.debug(u"Finished processing {0}".format(encode(node)))

    @dispatch(Link)  # noqa: F811
    def visit(self, node):  # pylint: disable=E0102
        """Perform semantic analysis of the link node.

        :param node: Link node
        :type node: Link
        """
        self._logger.debug(u"Started processing {0}".format(encode(node)))

        if not node.templated:
            parser = URIReferenceParser()
            parser.parse(node.href)

        self._logger.debug(u"Finished processing {0}".format(encode(node)))

    @dispatch(CollectionList)  # noqa: F811
    def visit(self, node):  # pylint: disable=E0102
        """Perform semantic analysis of the list of sub-collections.

        :param node: CollectionList node
        :type node: CollectionList
        """
        self._logger.debug(u"Started processing {0}".format(encode(node)))

        for collection in node:
            collection.accept(self)

        self._logger.debug(u"Finished processing {0}".format(encode(node)))

    @dispatch(CompactCollection)  # noqa: F811
    def visit(self, node):  # pylint: disable=E0102
        """Perform semantic analysis of the compact collection node.

        :param node: Collection node
        :type node: CompactCollection
        """
        self._logger.debug(u"Started processing {0}".format(encode(node)))

        node.links.accept(self)

        self._logger.debug(u"Finished processing {0}".format(encode(node)))

    @dispatch(Collection)  # noqa: F811
    def visit(self, node):  # pylint: disable=E0102
        """Perform semantic analysis of the collection node.

        :param node: Collection node
        :type node: Collection
        """
        self._logger.debug(u"Started processing {0}".format(encode(node)))

        node.metadata.accept(self)
        node.links.accept(self)
        node.sub_collections.accept(self)

        self._logger.debug(u"Finished processing {0}".format(encode(node)))
