from nose.tools import assert_raises, eq_
from parameterized import parameterized

from ...python_expression_dsl.parser import DSLParseError, DSLParser


class TestDSLParser(object):
    @parameterized.expand(
        [
            ("incorrect_expression", "?", "Unexpected symbol '?' at position 0"),
            ("incorrect_expression_2", "(+", "Unexpected symbol '+' at position 1"),
            ("incorrect_expression_2", "(1 +", "Unexpected symbol '+' at position 3"),
        ]
    )
    def test_parse_generates_correct_error_message(
        self, _, expression, expected_error_message
    ):
        # Arrange
        parser = DSLParser()

        # Act
        with assert_raises(DSLParseError) as exception_context:
            parser.parse(expression)

        # Assert
        eq_(expected_error_message, str(exception_context.exception))
