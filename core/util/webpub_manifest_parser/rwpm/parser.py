from core.util.webpub_manifest_parser.core.parsers import DocumentParser, DocumentParserFactory
from core.util.webpub_manifest_parser.rwpm.registry import (
    RWPMCollectionRolesRegistry,
    RWPMLinkRelationsRegistry,
    RWPMMediaTypesRegistry,
)
from core.util.webpub_manifest_parser.rwpm.semantic import RWPMSemanticAnalyzer
from core.util.webpub_manifest_parser.rwpm.syntax import RWPMSyntaxAnalyzer


class RWPMDocumentParserFactory(DocumentParserFactory):
    """Factory creating RWPM parsers."""

    def create(self):
        """Create a new RWPMParser.

        :return: RWPM parser instance
        :rtype: Parser
        """
        media_types_registry = RWPMMediaTypesRegistry()
        link_relations_registry = RWPMLinkRelationsRegistry()
        collection_roles_registry = RWPMCollectionRolesRegistry()
        syntax_analyzer = RWPMSyntaxAnalyzer()
        semantic_analyzer = RWPMSemanticAnalyzer(
            media_types_registry, link_relations_registry, collection_roles_registry
        )
        parser = DocumentParser(syntax_analyzer, semantic_analyzer)

        return parser
