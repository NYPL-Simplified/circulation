from api.proquest.identifier import ProQuestIdentifierParser
from core.model import Identifier
from nose.tools import eq_
from parameterized import parameterized


class TestProQuestIdentifierParser(object):
    @parameterized.expand(
        [
            (
                "incorrect_identifier",
                "urn:librarysimplified.org/terms/id/Overdrive%20ID/adfcc11a-cc5b-4c82-8048-e005e4a90222",
                None,
            ),
            (
                "correct_identifier",
                "urn:proquest.com/document-id/12345",
                (Identifier.PROQUEST_ID, "12345"),
            ),
        ]
    )
    def test_parse(self, _, identifier_string, expected_result):
        parser = ProQuestIdentifierParser()

        result = parser.parse(identifier_string)

        eq_(expected_result, result)
