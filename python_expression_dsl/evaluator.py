import operator
from copy import copy, deepcopy

from multipledispatch import dispatch

from ..exceptions import BaseError
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
    Visitor,
)
from .parser import DSLParser
import types

class DSLEvaluationError(BaseError):
    """Raised when evaluation of a DSL expression fails."""


class DSLEvaluationVisitor(Visitor):
    """Visitor traversing expression's AST and evaluating it."""

    ARITHMETIC_OPERATORS = {
        Operator.NEGATION: operator.neg,
        Operator.ADDITION: operator.add,
        Operator.SUBTRACTION: operator.sub,
        Operator.MULTIPLICATION: operator.mul,
        Operator.DIVISION: operator.truediv,
        Operator.EXPONENTIATION: operator.pow,
    }

    BOOLEAN_OPERATORS = {
        Operator.INVERSION: operator.not_,
        Operator.CONJUNCTION: operator.and_,
        Operator.DISJUNCTION: operator.or_,
    }

    COMPARISON_OPERATORS = {
        Operator.EQUAL: operator.eq,
        Operator.NOT_EQUAL: operator.ne,
        Operator.GREATER: operator.gt,
        Operator.GREATER_OR_EQUAL: operator.ge,
        Operator.LESS: operator.lt,
        Operator.LESS_OR_EQUAL: operator.le,
        Operator.IN: lambda a, b: operator.contains(b, a),
    }

    BUILTIN_FUNCTIONS = {
        "abs": abs,
        "all": all,
        "any": any,
        "len": len,
        "max": max,
        "min": min,
        "int": int,
        "float": float,
        "str": str,
    }

    BUILTIN_CLASSES = [float, int, str, types.ModuleType]

    def __init__(self, context=None, safe_classes=None):
        """Initialize a new instance of DSLEvaluationVisitor class.

        :param context: Optional evaluation context
        :type context: Optional[Union[Dict, object]]

        :param safe_classes: Optional list of classes which methods can be called.
            By default it contains only built-in classes: float, int, str
        :type safe_classes: Optional[List[type]]
        """
        self._context = {}
        self._safe_classes = []
        self._current_scope = None
        self._root_dot_node = None

        if safe_classes is None:
            safe_classes = []

        self.context = context
        self.safe_classes = safe_classes

    @staticmethod
    def _get_attribute_value(obj, attribute):
        """Return the attribute's value by its name.

        :param obj: Object or a dictionary containing the attribute
        :type obj: Union[Dict, object]

        :param attribute: Attribute's name
        :type attribute: str

        :return: Attribute's value
        :rtype: Any
        """
        if isinstance(obj, dict):
            if attribute not in obj:
                raise DSLEvaluationError(
                    "Cannot find attribute '{0}' in {1}".format(attribute, obj)
                )

            return obj[attribute]
        else:
            if not hasattr(obj, attribute):
                raise DSLEvaluationError(
                    "Cannot find attribute '{0}' in {1}".format(attribute, obj)
                )

            return getattr(obj, attribute)

    def _evaluate_unary_expression(self, unary_expression, available_operators):
        """Evaluate the unary expression.

        :param unary_expression: Unary expression
        :type unary_expression: core.dsl.ast.UnaryExpression

        :param available_operators: Dictionary containing available operators
        :type available_operators: Dict[core.dsl.ast.Operator, operator]

        :return: Evaluation result
        :rtype: Any
        """
        argument = unary_expression.argument.accept(self)

        if unary_expression.operator not in available_operators:
            raise DSLEvaluationError(
                "Wrong operator {0}. Was expecting one of {1}".format(
                    unary_expression.operator, list(available_operators.keys())
                )
            )

        expression_operator = available_operators[unary_expression.operator]
        result = expression_operator(argument)

        return result

    def _evaluate_binary_expression(self, binary_expression, available_operators):
        """Evaluate the binary expression.

        :param binary_expression: Binary expression
        :type binary_expression: core.dsl.ast.BinaryExpression

        :param available_operators: Dictionary containing available operators
        :type available_operators: Dict[core.dsl.ast.Operator, operator]

        :return: Evaluation result
        :rtype: Any
        """
        left_argument = binary_expression.left_argument.accept(self)
        right_argument = binary_expression.right_argument.accept(self)

        if binary_expression.operator not in available_operators:
            raise DSLEvaluationError(
                "Wrong operator {0}. Was expecting one of {1}".format(
                    binary_expression.operator, list(available_operators.keys())
                )
            )

        expression_operator = available_operators[binary_expression.operator]
        result = expression_operator(left_argument, right_argument)

        return result

    @property
    def context(self):
        """Return the evaluation context.

        :return: Evaluation context
        :rtype: Union[Dict, object]
        """
        return self._context

    @context.setter
    def context(self, value):
        """Set the evaluation context.

        :param value: New evaluation context
        :type value: Union[Dict, object]
        """
        if not isinstance(value, (dict, object)):
            raise ValueError(
                "Argument 'value' must be an either a dictionary or object"
            )

        new_context = {}

        if value is not None:
            if isinstance(value, dict):
                for key, item in value.items():
                    new_context[key] = deepcopy(item)
            else:
                new_context = deepcopy(value)

        self._context = new_context

    @property
    def safe_classes(self):
        """Return a list of classes which methods can be called.

        :return: List of safe classes which methods can be called
        :rtype: List[type]
        """
        return self._safe_classes

    @safe_classes.setter
    def safe_classes(self, value):
        """Set safe classes which methods can be called.

        :param value: List of safe classes which methods be called
        :type value: List[type]
        """
        if not isinstance(value, list):
            raise ValueError("Argument 'value' must be a list")

        new_safe_classes = copy(value)
        new_safe_classes.extend(self.BUILTIN_CLASSES)
        new_safe_classes = list(set(new_safe_classes))

        self._safe_classes = new_safe_classes

    @dispatch(Identifier)
    def visit(self, node):
        """Process the Identifier node.

        :param node: Identifier node
        :type node: Identifier
        """
        if self._current_scope is None and node.value in self.BUILTIN_FUNCTIONS:
            value = self.BUILTIN_FUNCTIONS[node.value]
        else:
            value = self._get_attribute_value(
                self._current_scope
                if self._current_scope is not None
                else self._context,
                node.value,
            )

        return value

    @dispatch(String)
    def visit(self, node):
        """Process the String node.

        :param node: String node
        :type node: String
        """
        return str(node.value)

    @dispatch(Number)
    def visit(self, node):
        """Process the Number node.

        :param node: Number node
        :type node: Number
        """
        try:
            return int(node.value)
        except:
            return float(node.value)

    @dispatch(DotExpression)
    def visit(self, node):
        """Process the DotExpression node.

        :param node: DotExpression node
        :type node: DotExpression
        """
        if self._root_dot_node is None:
            self._root_dot_node = node

        value = None

        for expression in node.expressions:
            value = expression.accept(self)

            self._current_scope = value

        if self._root_dot_node == node:
            self._root_dot_node = None
            self._current_scope = None

        return value

    @dispatch(UnaryArithmeticExpression)
    def visit(self, node):
        """Process the UnaryArithmeticExpression node.

        :param node: UnaryArithmeticExpression node
        :type node: UnaryArithmeticExpression
        """
        return self._evaluate_unary_expression(node, self.ARITHMETIC_OPERATORS)

    @dispatch(BinaryArithmeticExpression)
    def visit(self, node):
        """Process the BinaryArithmeticExpression node.

        :param node: BinaryArithmeticExpression node
        :type node: BinaryArithmeticExpression
        """
        return self._evaluate_binary_expression(node, self.ARITHMETIC_OPERATORS)

    @dispatch(UnaryBooleanExpression)
    def visit(self, node):
        """Process the UnaryBooleanExpression node.

        :param node: UnaryBooleanExpression node
        :type node: UnaryBooleanExpression
        """
        return self._evaluate_unary_expression(node, self.BOOLEAN_OPERATORS)

    @dispatch(BinaryBooleanExpression)
    def visit(self, node):
        """Process the BinaryBooleanExpression node.

        :param node: BinaryBooleanExpression node
        :type node: BinaryBooleanExpression
        """
        return self._evaluate_binary_expression(node, self.BOOLEAN_OPERATORS)

    @dispatch(ComparisonExpression)
    def visit(self, node):
        """Process the ComparisonExpression node.

        :param node: ComparisonExpression node
        :type node: ComparisonExpression
        """
        return self._evaluate_binary_expression(node, self.COMPARISON_OPERATORS)

    @dispatch(SliceExpression)
    def visit(self, node):
        """Process the SliceExpression node.

        :param node: SliceExpression node
        :type node: SliceExpression
        """
        array = node.array.accept(self)
        index = node.slice.accept(self)

        return operator.getitem(array, index)

    @dispatch(FunctionCallExpression)
    def visit(self, node):
        """Process the FunctionCallExpression node.

        :param node: FunctionCallExpression node
        :type node: FunctionCallExpression
        """
        function = node.function.accept(self)
        arguments = []

        if node.arguments:
            for argument in node.arguments:
                argument = argument.accept(self)

                arguments.append(argument)

        function_class = getattr(function.__self__, "__class__", None)

        if function_class and function_class not in self.safe_classes:
            raise DSLEvaluationError(
                "Function {0} defined in a not-safe class {1} and cannot be called".format(
                    function, function_class
                )
            )

        result = function(*arguments)

        return result


class DSLEvaluator(object):
    """Evaluates the expression."""

    def __init__(self, parser, visitor):
        """Initialize a new instance of DSLEvaluator class.

        :param parser: DSL parser transforming the expression string into an AST object
        :type parser: DSLParser

        :param visitor: Visitor used for evaluating the expression's AST
        :type visitor: DSLEvaluationVisitor
        """
        if not isinstance(parser, DSLParser):
            raise ValueError(
                "Argument 'parser' must be an instance of {0} class".format(DSLParser)
            )
        if not isinstance(visitor, DSLEvaluationVisitor):
            raise ValueError(
                "Argument 'visitor' must be an instance of {0} class".format(
                    DSLEvaluationVisitor
                )
            )

        self._parser = parser
        self._visitor = visitor

    @property
    def parser(self):
        """Return the parser used by this evaluator.

        :return: Parser used by this evaluator
        :rtype: DSLParser
        """
        return self._parser

    def evaluate(self, expression, context=None, safe_classes=None):
        """Evaluate the expression and return the resulting value.

        :param expression: String containing the expression
        :type expression: str

        :param context: Evaluation context
        :type context: Union[Dict, object]

        :param safe_classes: List of classes which methods can be called
        :type safe_classes: List[type]

        :return: Evaluation result
        :rtype: Any
        """
        node = self._parser.parse(expression)

        old_context = self._visitor.context
        old_safe_classes = self._visitor.safe_classes

        self._visitor.context = context
        self._visitor.safe_classes = safe_classes

        try:
            result = self._visitor.visit(node)

            return result
        finally:
            self._visitor.context = old_context
            self._visitor.safe_classes = old_safe_classes
