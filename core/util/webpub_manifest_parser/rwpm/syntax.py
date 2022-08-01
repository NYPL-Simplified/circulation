from core.util.webpub_manifest_parser.core.syntax import SyntaxAnalyzer
from core.util.webpub_manifest_parser.rwpm.ast import RWPMManifest


class RWPMSyntaxAnalyzer(SyntaxAnalyzer):
    """Syntax analyzer for RWPM grammar."""

    def _create_manifest(self):
        """Create a new RWPM manifest.

        :return: RWPM manifest
        :rtype: RWPMManifest
        """
        return RWPMManifest()
