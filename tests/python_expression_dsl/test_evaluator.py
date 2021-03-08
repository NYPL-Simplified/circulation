from nose.tools import assert_raises, eq_
from parameterized import parameterized

from ...python_expression_dsl.evaluator import (
    DSLEvaluationError,
    DSLEvaluationVisitor,
    DSLEvaluator,
)
from ...python_expression_dsl.parser import DSLParseError, DSLParser

class Subject(object):
    """Dummy object designed for testing DSLEvaluator."""

    def __init__(self, attributes):
        """Initialize a new instance of Subject.

        :param attributes: List of attributes
        :type attributes: List[str]
        """
        self._attributes = attributes

    @property
    def attributes(self):
        """Return the list of attributes.

        :return: List of attributes
        :rtype: List
        """
        return self._attributes

    def get_attribute_value(self, index):
        """Dummy method to test method invocation.

        :param index: Attribute's index
        :type index: int

        :return: Attribute's value
        :rtype: Any
        """
        return self._attributes[index]


class TestDSLEvaluator(object):
    @parameterized.expand(
        [
            ("incorrect_expression", "?", None, None, None, DSLParseError),

            ("numeric_literal", "9", 9),
            ("numeric_float_literal", "9.5", 9.5),

            ("unknown_identifier", "foo", None, None, None, DSLEvaluationError),
            ("known_identifier", "foo", 9, {"foo": 9}),
            (
                "unknown_nested_identifier",
                "foo.bar",
                None,
                {"foo": 9},
                None,
                DSLEvaluationError,
            ),
            ("known_nested_identifier", "foo.bar", 9, {"foo": {"bar": 9}}),
            ("known_nested_identifier", "foo.bar.baz", 9, {"foo": {"bar": {"baz": 9}}}),
            ("known_nested_identifier", "foo.bar[0].baz", 9, {"foo": {"bar": [{"baz": 9}]}}),
            (
                "identifier_pointing_to_the_object",
                "'eresources' in subject.attributes",
                True,
                {"subject": Subject(["eresources"])},
            ),

            ("simple_negation", "-9", -9),
            ("simple_parenthesized_expression_negation", "-(9)", -(9)),
            ("parenthesized_expression_negation", "-(9 + 3)", -(9 + 3)),
            ("slice_expression_negation", "-(arr[1])", -12, {"arr": [1, 12, 3]}),

            ("addition_with_two_operands", "9 + 3", 9 + 3),
            ("addition_with_three_operands", "9 + 3 + 3", 9 + 3 + 3),
            ("addition_with_four_operands", "9 + 3 + 3 + 3", 9 + 3 + 3 + 3),

            ("subtraction_with_two_operands", "9 - 3", 9 - 3),

            ("multiplication_with_two_operands", "9 * 3", 9 * 3),

            ("division_with_two_operands", "9 / 3", 9 / 3),
            ("division_with_two_operands_and_remainder", "9 / 4", 9.0 / 4.0),

            ("exponentiation_with_two_operands", "9 ** 3", 9 ** 3),
            ("exponentiation_with_three_operands", "2 ** 3 ** 3", 2 ** 3 ** 3),

            (
                "associative_law_for_addition",
                "(a + b) + c == a + (b + c)",
                True,
                {"a": 9, "b": 3, "c": 3},
            ),
            (
                "associative_law_for_multiplication",
                "(a * b) * c == a * (b * c)",
                True,
                {"a": 9, "b": 3, "c": 3},
            ),

            (
                "commutative_law_for_addition",
                "a + b == b + a",
                True,
                {"a": 9, "b": 3}
            ),
            (
                "commutative_law_for_multiplication",
                "a * b == b * a",
                True,
                {"a": 9, "b": 3},
            ),

            (
                "distributive_law",
                "a * (b + c) == a * b + a * c",
                True,
                {"a": 9, "b": 3, "c": 3},
            ),

            ("less_comparison", "9 < 3", 9 < 3),
            ("less_or_equal_comparison", "3 <= 3", 3 <= 3),
            ("greater_comparison", "9 > 3", 9 > 3),
            ("greater_or_equal_comparison", "3 >= 2", 3 >= 2),

            ("in_operator", "3 in list", True, {"list": [1, 2, 3]}),

            ("inversion", "not 9 < 3", not 9 < 3),
            ("double_inversion", "not not 9 < 3", not not 9 < 3),
            ("triple_inversion", "not not not 9 < 3", not not not 9 < 3),

            ("conjunction", "9 == 9 and 3 == 3", 9 == 9 and 3 == 3),
            ("disjunction", "9 == 3 or 3 == 3", 9 == 3 or 3 == 3),

            ("simple_parenthesized_expression", "(9 + 3)", (9 + 3)),
            ("arithmetic_parenthesized_expression", "2 * (9 + 3) * 2", 2 * (9 + 3) * 2),

            ("slice_expression", "arr[1] == 12", True, {"arr": [1, 12, 3]}),
            ("complex_slice_expression", "arr[1] + arr[2]", 15, {"arr": [1, 12, 3]}),

            ("method_call", "string.upper()", "HELLO WORLD", {"string": "Hello World"}),
            ("builtin_function_call", "min(1, 2)", min(1, 2)),
            (
                "unsafe_class_method_call",
                "subject.get_attribute_value(0)",
                "eresources",
                {"subject": Subject(["eresources"])},
                None,
                DSLEvaluationError,
            ),
            (
                "safe_class_method_call",
                "subject.get_attribute_value(0)",
                "eresources",
                {"subject": Subject(["eresources"])},
                [Subject],
            ),
            (
                "safe_class_method_call_with_direct_context",
                "get_attribute_value(0)",
                "eresources",
                Subject(["eresources"]),
                [Subject],
            ),
        ]
    )
    def test(
        self,
        _,
        expression,
        expected_result,
        context=None,
        safe_classes=None,
        expected_exception=None,
    ):
        # Arrange
        parser = DSLParser()
        visitor = DSLEvaluationVisitor()
        evaluator = DSLEvaluator(parser, visitor)

        if safe_classes is None:
            safe_classes = []

        # Act
        if expected_exception:
            with assert_raises(expected_exception):
                evaluator.evaluate(expression, context, safe_classes)
        else:
            result = evaluator.evaluate(expression, context, safe_classes)

            # Assert
            eq_(expected_result, result)
