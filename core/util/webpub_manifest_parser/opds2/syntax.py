from core.util.webpub_manifest_parser.core.syntax import SyntaxAnalyzer
from core.util.webpub_manifest_parser.opds2.ast import OPDS2Feed


class OPDS2SyntaxAnalyzer(SyntaxAnalyzer):
    """OPDS 2.0 syntax analyzer."""

    def _create_manifest(self):
        """Create a new OPDS 2.0 manifest.

        :return: OPDS 2.0 manifest
        :rtype: Manifestlike
        """
        return OPDS2Feed()
