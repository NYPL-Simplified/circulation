import re

from pyparsing import (
    Forward,
    Group,
    Literal,
    ParseException,
    QuotedString,
    Regex,
    Suppress,
    Word,
    ZeroOrMore,
    alphanums,
    alphas,
)

from ..exceptions import BaseError
from ..util import chunks
from .ast import (
    BinaryArithmeticExpression,
    BinaryBooleanExpression,
    ComparisonExpression,
    DotExpression,
    FunctionCallExpression,
    Identifier,
    Number,
    Operator,
    SliceExpression,
    String,
    UnaryArithmeticExpression,
    UnaryBooleanExpression,
)


class DSLParseError(BaseError):
    """Raised when expression has an incorrect format."""


class DSLParser(object):
    """Parses expressions into AST objects."""

    PARSE_ERROR_MESSAGE_REGEX = re.compile(
        r"Expected\s+(\{.+\}),\s+found\s+('.+')\s+\(at\s+char\s+(\d+)\)"
    )
    DEFAULT_ERROR_MESSAGE = "Could not parse the expression"

    @staticmethod
    def _parse_identifier(tokens):
        """Transform the token into an Identifier node.

        :param tokens: ParseResults objects
        :type tokens: pyparsing.ParseResults

        :return: Identifier node
        :rtype: core.python_expression_dsl.ast.Identifier
        """
        return Identifier(tokens[0])

    @staticmethod
    def _parse_string(tokens):
        """Transform the token into a String node.

        :param tokens: ParseResults objects
        :type tokens: pyparsing.ParseResults

        :return: Identifier node
        :rtype: core.python_expression_dsl.ast.String
        """
        return String(tokens[0])

    @staticmethod
    def _parse_number(tokens):
        """Transform the token into a Number node.

        :param tokens: ParseResults objects
        :type tokens: pyparsing.ParseResults

        :return: Number node
        :rtype: core.python_expression_dsl.ast.Number
        """
        return Number(tokens[0])

    @staticmethod
    def _parse_unary_expression(expression_type, tokens):
        """Transform the token into an unary expression.

        :param tokens: ParseResults objects
        :type tokens: pyparsing.ParseResults

        :return: UnaryExpression node
        :rtype: core.python_expression_dsl.ast.UnaryExpression
        """
        if len(tokens) >= 2:
            tokens = list(reversed(tokens))
            argument = tokens[0]
            operator_type = tokens[1]
            expression = expression_type(operator_type, argument)

            for tokens_chunk in chunks(tokens, 1, 2):
                operator_type = tokens_chunk[0]
                expression = expression_type(operator_type, expression)

            return expression

    @staticmethod
    def _parse_unary_arithmetic_expression(tokens):
        """Transform the token into an UnaryArithmeticExpression node.

        :param tokens: ParseResults objects
        :type tokens: pyparsing.ParseResults

        :return: UnaryArithmeticExpression node
        :rtype: core.python_expression_dsl.ast.UnaryArithmeticExpression
        """
        return DSLParser._parse_unary_expression(UnaryArithmeticExpression, tokens)

    @staticmethod
    def _parse_unary_boolean_expression(tokens):
        """Transform the token into an UnaryBooleanExpression node.

        :param tokens: ParseResults objects
        :type tokens: pyparsing.ParseResults

        :return: UnaryBooleanExpression node
        :rtype: core.python_expression_dsl.ast.UnaryBooleanExpression
        """
        return DSLParser._parse_unary_expression(UnaryBooleanExpression, tokens)

    @staticmethod
    def _parse_binary_expression(expression_type, tokens):
        """Transform the token into a BinaryExpression node.

        :param tokens: ParseResults objects
        :type tokens: pyparsing.ParseResults

        :return: BinaryExpression node
        :rtype: core.python_expression_dsl.ast.BinaryExpression
        """
        if len(tokens) >= 3:
            left_argument = tokens[0]
            operator_type = tokens[1]
            right_argument = tokens[2]
            expression = expression_type(operator_type, left_argument, right_argument)

            for tokens_chunk in chunks(tokens, 2, 3):
                operator_type = tokens_chunk[0]
                right_argument = tokens_chunk[1]
                expression = expression_type(operator_type, expression, right_argument)

            return expression

    @staticmethod
    def _parse_binary_arithmetic_expression(tokens):
        """Transform the token into a BinaryArithmeticExpression node.

        :param tokens: ParseResults objects
        :type tokens: pyparsing.ParseResults

        :return: BinaryArithmeticExpression node
        :rtype: core.python_expression_dsl.ast.BinaryArithmeticExpression
        """
        return DSLParser._parse_binary_expression(BinaryArithmeticExpression, tokens)

    @staticmethod
    def _parse_binary_boolean_expression(tokens):
        """Transform the token into a BinaryBooleanExpression node.

        :param tokens: ParseResults objects
        :type tokens: pyparsing.ParseResults

        :return: BinaryBooleanExpression node
        :rtype: core.python_expression_dsl.ast.BinaryBooleanExpression
        """
        return DSLParser._parse_binary_expression(BinaryBooleanExpression, tokens)

    @staticmethod
    def _parse_comparison_expression(tokens):
        """Transform the token into a ComparisonExpression node.

        :param tokens: ParseResults objects
        :type tokens: pyparsing.ParseResults

        :return: ComparisonExpression node
        :rtype: core.python_expression_dsl.ast.ComparisonExpression
        """
        return DSLParser._parse_binary_expression(ComparisonExpression, tokens)

    @staticmethod
    def _parse_dot_expression(tokens):
        """Transform the token into a DotExpression node.

        :param tokens: ParseResults objects
        :type tokens: pyparsing.ParseResults

        :return: ComparisonExpression node
        :rtype: core.python_expression_dsl.ast.DotExpression
        """
        return DotExpression(list(tokens[0]))

    @staticmethod
    def _parse_parenthesized_expression(tokens):
        """Transform the token into a Expression node.

        :param tokens: ParseResults objects
        :type tokens: pyparsing.ParseResults

        :return: ComparisonExpression node
        :rtype: core.python_expression_dsl.ast.Expression
        """
        return tokens[0]

    @staticmethod
    def _parse_function_call_expression(tokens):
        """Transform the token into a FunctionCallExpression node.

        :param tokens: ParseResults objects
        :type tokens: pyparsing.ParseResults

        :return: ComparisonExpression node
        :rtype: core.python_expression_dsl.ast.FunctionCallExpression
        """
        function_identifier = tokens[0][0]
        arguments = tokens[0][1:]

        return FunctionCallExpression(function_identifier, arguments)

    @staticmethod
    def _parse_slice_operation(tokens):
        """Transform the token into a SliceExpression node.

        :param tokens: ParseResults objects
        :type tokens: pyparsing.ParseResults

        :return: SliceExpression node
        :rtype: core.python_expression_dsl.ast.SliceExpression
        """
        array_expression = tokens[0][0]
        slice_expression = tokens[0][1]

        return SliceExpression(array_expression, slice_expression)

    # Auxiliary tokens
    LEFT_PAREN, RIGHT_PAREN = map(Suppress, "()")
    LEFT_BRACKET, RIGHT_BRACKET = map(Suppress, "[]")
    COMMA = Suppress(",")
    FULL_STOP = Suppress(".")

    # Unary arithmetic operators
    NEGATION_OPERATOR = Literal("-").setParseAction(lambda _: Operator.NEGATION)

    # Binary additive arithmetic operators
    ADDITION_OPERATOR = Literal("+").setParseAction(lambda _: Operator.ADDITION)
    SUBTRACTION_OPERATOR = Literal("-").setParseAction(lambda _: Operator.SUBTRACTION)
    ADDITIVE_OPERATOR = ADDITION_OPERATOR | SUBTRACTION_OPERATOR

    # Binary multiplicative arithmetic operators
    MULTIPLICATION_OPERATOR = Literal("*").setParseAction(
        lambda _: Operator.MULTIPLICATION
    )
    DIVISION_OPERATOR = Literal("/").setParseAction(lambda _: Operator.DIVISION)
    MULTIPLICATIVE_OPERATOR = MULTIPLICATION_OPERATOR | DIVISION_OPERATOR

    # Power operator
    POWER_OPERATOR = Literal("**").setParseAction(lambda _: Operator.EXPONENTIATION)

    # Comparison operators
    EQUAL_OPERATOR = Literal("==").setParseAction(lambda _: Operator.EQUAL)
    NOT_EQUAL_OPERATOR = Literal("!=").setParseAction(lambda _: Operator.NOT_EQUAL)
    GREATER_OPERATOR = Literal(">").setParseAction(lambda _: Operator.GREATER)
    GREATER_OR_EQUAL_OPERATOR = Literal(">=").setParseAction(
        lambda _: Operator.GREATER_OR_EQUAL
    )
    LESS_OPERATOR = Literal("<").setParseAction(lambda _: Operator.LESS)
    LESS_OR_EQUAL_OPERATOR = Literal("<=").setParseAction(
        lambda _: Operator.LESS_OR_EQUAL
    )
    IN_OPERATOR = Literal("in").setParseAction(lambda _: Operator.IN)
    COMPARISON_OPERATOR = (
        EQUAL_OPERATOR
        | NOT_EQUAL_OPERATOR
        | GREATER_OR_EQUAL_OPERATOR
        | GREATER_OPERATOR
        | LESS_OR_EQUAL_OPERATOR
        | LESS_OPERATOR
        | IN_OPERATOR
    )

    NUMBER = Regex(r"[+-]?\d+(?:\.\d*)?(?:[eE][+-]?\d+)?").setParseAction(
        _parse_number.__func__
    )
    IDENTIFIER = Word(alphas, alphanums + "_$").setParseAction(
        _parse_identifier.__func__
    )
    STRING = (QuotedString("'") | QuotedString('"')).setParseAction(
        _parse_string.__func__
    )

    # Unary boolean operator
    INVERSION_OPERATOR = Literal("not").setParseAction(lambda _: Operator.INVERSION)

    # Binary boolean operators
    CONJUNCTION_OPERATOR = Literal("and").setParseAction(lambda _: Operator.CONJUNCTION)
    DISJUNCTION_OPERATOR = Literal("or").setParseAction(lambda _: Operator.DISJUNCTION)

    arithmetic_expression = Forward()

    comparison_expression = (
        arithmetic_expression + ZeroOrMore(COMPARISON_OPERATOR + arithmetic_expression)
    ).setParseAction(_parse_comparison_expression.__func__)

    inversion_expression = (
        ZeroOrMore(INVERSION_OPERATOR) + comparison_expression
    ).setParseAction(_parse_unary_boolean_expression.__func__)
    conjunction_expression = (
        inversion_expression + ZeroOrMore(CONJUNCTION_OPERATOR + inversion_expression)
    ).setParseAction(_parse_binary_boolean_expression.__func__)
    disjunction_expression = (
        conjunction_expression
        + ZeroOrMore(DISJUNCTION_OPERATOR + conjunction_expression)
    ).setParseAction(_parse_binary_boolean_expression.__func__)

    expression = disjunction_expression

    dot_expression = Group(
        IDENTIFIER + ZeroOrMore(FULL_STOP + expression)
    ).setParseAction(_parse_dot_expression.__func__)

    parenthesized_expression = Group(
        LEFT_PAREN + expression + RIGHT_PAREN
    ).setParseAction(_parse_parenthesized_expression.__func__)

    slice = expression
    slice_expression = Group(
        IDENTIFIER + LEFT_BRACKET + slice + RIGHT_BRACKET
    ).setParseAction(_parse_slice_operation.__func__)

    function_call_arguments = ZeroOrMore(expression + ZeroOrMore(COMMA + expression))
    function_call_expression = Group(
        IDENTIFIER + LEFT_PAREN + function_call_arguments + RIGHT_PAREN
    ).setParseAction(_parse_function_call_expression.__func__)

    atom = (
        ZeroOrMore(NEGATION_OPERATOR)
        + (
            NUMBER
            | STRING
            | slice_expression
            | parenthesized_expression
            | function_call_expression
            | dot_expression
            | IDENTIFIER
        )
    ).setParseAction(_parse_unary_arithmetic_expression.__func__)

    factor = Forward()
    factor << (atom + ZeroOrMore(POWER_OPERATOR + factor)).setParseAction(
        _parse_binary_arithmetic_expression.__func__
    )
    term = (factor + ZeroOrMore(MULTIPLICATIVE_OPERATOR + factor)).setParseAction(
        _parse_binary_arithmetic_expression.__func__
    )
    arithmetic_expression << (
        term + ZeroOrMore(ADDITIVE_OPERATOR + term)
    ).setParseAction(_parse_binary_arithmetic_expression.__func__)

    def _parse_error_message(self, parse_exception):
        """Transform the standard error description into a readable concise message.

        :param parse_exception: Exception thrown by pyparsing
        :type parse_exception: ParseException

        :return: Error message
        :rtype: str
        """
        error_message = str(parse_exception)
        match = self.PARSE_ERROR_MESSAGE_REGEX.match(error_message)

        if not match:
            return self.DEFAULT_ERROR_MESSAGE

        found = match.group(2).strip("'")
        position = match.group(3)

        return "Unexpected symbol '{0}' at position {1}".format(found, position)

    def parse(self, expression):
        """Parse the expression and transform it into AST.

        :param expression: String containing the expression
        :type expression: str

        :return: AST node
        :rtype: core.python_expression_dsl.ast.Node
        """
        try:
            results = self.expression.parseString(expression, parseAll=True)

            return results[0]
        except ParseException as exception:
            error_message = self._parse_error_message(exception)

            raise DSLParseError(error_message, inner_exception=exception)
