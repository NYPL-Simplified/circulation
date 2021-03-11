from itertools import tee

from parameterized import parameterized
from werkzeug.datastructures import MultiDict

from api.admin.problem_details import INCOMPLETE_CONFIGURATION
from api.admin.validator import PatronAuthenticationValidatorFactory
from api.app import initialize_database
from api.saml.configuration.model import SAMLConfiguration
from api.saml.configuration.validator import (
    SAML_INCORRECT_METADATA,
    SAMLSettingsValidator,
)
from api.saml.metadata.filter import SAMLSubjectFilter
from api.saml.metadata.parser import SAMLMetadataParser
from api.saml.provider import SAMLWebSSOAuthenticationProvider
from core.python_expression_dsl.evaluator import DSLEvaluationVisitor, DSLEvaluator
from core.python_expression_dsl.parser import DSLParser
from core.util.problem_detail import ProblemDetail
from tests.saml import fixtures
from tests.saml.controller_test import ControllerTest


class TestSAMLSettingsValidator(ControllerTest):
    @classmethod
    def setup_class(cls):
        super(TestSAMLSettingsValidator, cls).setup_class()

        initialize_database(autoinitialize=False)

    @parameterized.expand(
        [
            (
                "missing_sp_metadata_and_missing_idp_metadata",
                None,
                None,
                INCOMPLETE_CONFIGURATION.detailed(
                    "Required field 'Service Provider's XML Metadata' is missing"
                ),
            ),
            (
                "empty_sp_metadata_and_empty_idp_metadata",
                fixtures.INCORRECT_XML,
                fixtures.INCORRECT_XML,
                INCOMPLETE_CONFIGURATION.detailed(
                    "Required field 'Service Provider's XML Metadata' is missing"
                ),
            ),
            (
                "incorrect_sp_metadata_and_incorrect_idp_metadata",
                fixtures.INCORRECT_XML_WITH_ONE_SP_METADATA_WITHOUT_ACS_SERVICE,
                fixtures.INCORRECT_XML_WITH_ONE_IDP_METADATA_WITHOUT_SSO_SERVICE,
                SAML_INCORRECT_METADATA.detailed(
                    "Service Provider's metadata has incorrect format: "
                    "Missing urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST AssertionConsumerService"
                ),
            ),
            (
                "correct_sp_metadata_and_incorrect_idp_metadata",
                fixtures.CORRECT_XML_WITH_ONE_SP,
                fixtures.INCORRECT_XML_WITH_ONE_IDP_METADATA_WITHOUT_SSO_SERVICE,
                SAML_INCORRECT_METADATA.detailed(
                    "Identity Provider's metadata has incorrect format: "
                    "Missing urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect SingleSignOnService "
                    "service declaration"
                ),
            ),
            (
                "correct_sp_and_idp_metadata",
                fixtures.CORRECT_XML_WITH_ONE_SP,
                fixtures.CORRECT_XML_WITH_IDP_1,
                None,
            ),
        ]
    )
    def test_validate(
        self, _, sp_xml_metadata, idp_xml_metadata, expected_validation_result
    ):
        # Arrange
        submitted_form_data = MultiDict()

        if sp_xml_metadata is not None:
            submitted_form_data.add(
                SAMLConfiguration.service_provider_xml_metadata.key, sp_xml_metadata
            )
        if idp_xml_metadata is not None:
            submitted_form_data.add(
                SAMLConfiguration.non_federated_identity_provider_xml_metadata.key,
                idp_xml_metadata,
            )

        submitted_form = {"form": submitted_form_data}
        metadata_parser = SAMLMetadataParser()
        parser = DSLParser()
        visitor = DSLEvaluationVisitor()
        evaluator = DSLEvaluator(parser, visitor)
        subject_filter = SAMLSubjectFilter(evaluator)
        validator = SAMLSettingsValidator(metadata_parser, subject_filter)

        # Act
        settings = list(SAMLWebSSOAuthenticationProvider.SETTINGS)
        result = validator.validate(settings, submitted_form)

        # Assert
        if isinstance(result, ProblemDetail):
            assert expected_validation_result.response == result.response
        else:
            assert expected_validation_result == result


class TestSAMLSettingsValidatorFactory(object):
    @parameterized.expand([("validator_using_factory_method", "api.saml.provider")])
    def test_create_can_create(self, _, protocol):
        # Arrange
        factory = PatronAuthenticationValidatorFactory()

        # Act
        result = factory.create(protocol)

        # Assert
        assert True == isinstance(result, SAMLSettingsValidator)
