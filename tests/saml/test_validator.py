from nose.tools import eq_
from parameterized import parameterized
from werkzeug.datastructures import MultiDict

from api.admin.problem_details import INCOMPLETE_CONFIGURATION
from api.admin.validator import PatronAuthenticationValidatorFactory
from api.saml import configuration
from api.saml.parser import SAMLMetadataParser
from api.saml.provider import SAMLWebSSOAuthenticationProvider
from api.saml.validator import SAMLSettingsValidator, SAML_INCORRECT_METADATA
from core.util.problem_detail import ProblemDetail
from tests.saml import fixtures
from tests.saml.database_test import DatabaseTest


class SAMLSettingsValidatorTest(DatabaseTest):
    @parameterized.expand([
        (
            'missing_sp_metadata_and_missing_idp_metadata',
            None,
            None,
            INCOMPLETE_CONFIGURATION.detailed('Required field sp_xml_metadata is missing')
        ),
        (
            'empty_sp_metadata_and_empty_idp_metadata',
            fixtures.INCORRECT_XML,
            fixtures.INCORRECT_XML,
            INCOMPLETE_CONFIGURATION.detailed('Required field sp_xml_metadata is missing')
        ),
        (
            'incorrect_sp_metadata_and_incorrect_idp_metadata',
            fixtures.INCORRECT_ONE_SP_METADATA_WITHOUT_ACS_SERVICE,
            fixtures.INCORRECT_ONE_IDP_METADATA_WITHOUT_SSO_SERVICE,
            SAML_INCORRECT_METADATA.detailed(
                'Missing urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST AssertionConsumerService')
        ),
        (
            'correct_sp_metadata_and_incorrect_idp_metadata',
            fixtures.CORRECT_ONE_SP_METADATA,
            fixtures.INCORRECT_ONE_IDP_METADATA_WITHOUT_SSO_SERVICE,
            SAML_INCORRECT_METADATA.detailed(
                'Missing urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect SingleSignOnService service declaration')
        ),
        (
            'correct_sp_and_idp_metadata',
            fixtures.CORRECT_ONE_SP_METADATA,
            fixtures.CORRECT_ONE_IDP_METADATA,
            None
        )
    ])
    def test_validate(
            self,
            name,
            sp_xml_metadata,
            idp_xml_metadata,
            expected_validation_result):
        # Arrange
        submitted_form_data = MultiDict()

        if sp_xml_metadata is not None:
            submitted_form_data.add(configuration.SAMLConfiguration.SP_XML_METADATA, sp_xml_metadata)
        if idp_xml_metadata is not None:
            submitted_form_data.add(configuration.SAMLConfiguration.IDP_XML_METADATA, idp_xml_metadata)

        submitted_form = {'form': submitted_form_data}
        settings = SAMLWebSSOAuthenticationProvider.SETTINGS
        metadata_parser = SAMLMetadataParser()
        validator = SAMLSettingsValidator(metadata_parser)

        # Act
        result = validator.validate(settings, submitted_form)

        # Assert
        if isinstance(result, ProblemDetail):
            eq_(result.response, expected_validation_result.response)
        else:
            eq_(result, expected_validation_result)


class SAMLSettingsValidatorFactoryTest(object):
    @parameterized.expand([
        ('validator_using_factory_method', 'api.saml.provider')
    ])
    def test_create_can_create(self, name, protocol):
        # Arrange
        factory = PatronAuthenticationValidatorFactory()

        # Act
        result = factory.create(protocol)

        # Assert
        assert isinstance(result, SAMLSettingsValidator)
