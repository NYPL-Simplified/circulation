import logging
import re

from core.model.identifier import Identifier, IdentifierParser


class ProQuestIdentifierParser(IdentifierParser):
    """Parser for ProQuest Doc IDs."""

    PROQUEST_ID_REGEX = re.compile(r"urn:proquest.com/document-id/(\d+)")

    def __init__(self):
        self._logger = logging.getLogger(__name__)

    def parse(self, identifier_string):
        """Parse a string containing an identifier, extract it and determine its type.

        :param identifier_string: String containing an identifier
        :type identifier_string: str

        :return: 2-tuple containing the identifier's type and identifier itself or None
            if the string contains an incorrect identifier
        :rtype: Optional[Tuple[str, str]]
        """
        self._logger.debug(
            'Started parsing identifier string "{0}"'.format(identifier_string)
        )

        match = self.PROQUEST_ID_REGEX.match(identifier_string)

        if match:
            document_id = match.groups()[0]
            result = Identifier.PROQUEST_ID, document_id

            self._logger.debug(
                'Finished parsing identifier string "{0}". Result: {1}'.format(
                    document_id, result
                )
            )

            return result

        self._logger.debug(
            'Finished parsing identifier string "{0}". It does not contain a ProQuest Doc ID'.format(
                identifier_string
            )
        )

        return None
