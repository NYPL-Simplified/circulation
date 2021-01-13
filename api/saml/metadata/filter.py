import logging

from api.saml.metadata.model import (
    SAMLAttribute,
    SAMLAttributeStatement,
    SAMLNameID,
    SAMLSubject,
)
from core.exceptions import BaseError
from core.python_expression_dsl.evaluator import DSLEvaluator
from core.util.string_helpers import is_string


class SAMLSubjectFilterError(BaseError):
    """Raised in the case of any errors during execution of a filter expression."""

    def __init__(self, inner_exception):
        """Initialize a new instance of SAMLSubjectFilterError class.

        :param inner_exception: Inner exception
        :type inner_exception: Exception
        """
        message = "Incorrect filter expression: {0}".format(str(inner_exception))

        super(SAMLSubjectFilterError, self).__init__(message, inner_exception)


class SAMLSubjectFilter(object):
    """Executes filter expressions."""

    def __init__(self, dsl_evaluator):
        """Initialize a new instance of SAMLSubjectFilter class.

        :param dsl_evaluator: DSL evaluator
        :type dsl_evaluator: core.python_expression_dsl.evaluator.DSLEvaluator
        """
        if not isinstance(dsl_evaluator, DSLEvaluator):
            raise ValueError("Argument 'dsl_evaluator' must be an instance of {0} class".format(DSLEvaluator))

        self._dsl_evaluator = dsl_evaluator
        self._logger = logging.getLogger(__name__)

    def execute(self, expression, subject):
        """Apply the expression to the subject and return a boolean value indicating whether it's a valid subject.

        :param expression: String containing the filter expression
        :type expression: str

        :param subject: SAML subject
        :type subject: api.saml.metadata.model.SAMLSubject

        :return: Boolean value indicating whether it's a valid subject
        :rtype: bool

        :raise SAMLSubjectFilterError: in the case of any errors occurred during expression evaluation
        """
        if not expression or not is_string(expression):
            raise ValueError("Argument 'expression' must be a non-empty string")
        if not isinstance(subject, SAMLSubject):
            raise ValueError("Argument 'subject' must an instance of Subject class")

        self._logger.info(
            "Started applying expression '{0}' to {1}".format(expression, subject)
        )

        try:
            result = self._dsl_evaluator.evaluate(
                expression,
                context={"subject": subject},
                safe_classes=[
                    SAMLSubject,
                    SAMLNameID,
                    SAMLAttributeStatement,
                    SAMLAttribute,
                ],
            )
        except Exception as exception:
            raise SAMLSubjectFilterError(exception)

        self._logger.info(
            "Finished applying expression '{0}' to {1}: {2}".format(
                expression, subject, result
            )
        )

        result = bool(result)

        return result

    def validate(self, expression):
        """Validate the filter expression.

        Try to apply the expression to a dummy Subject object containing all the known SAML attributes.

        :param expression: String containing the filter expression
        :type expression: str

        :raise: SAMLSubjectFilterError
        """
        if not expression or not is_string(expression):
            raise ValueError("Argument 'expression' must be a non-empty string")

        try:
            self._dsl_evaluator.parser.parse(expression)
        except Exception as exception:
            raise SAMLSubjectFilterError(exception)
