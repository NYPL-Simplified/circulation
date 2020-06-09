# FIXME: Required to get rid of the circular import error
import api.app

import pickle

from nose.tools import eq_
from parameterized import parameterized
from werkzeug.datastructures import MultiDict

from api.admin.problem_details import INCOMPLETE_CONFIGURATION

from api.saml import configuration
from api.saml.configuration import SAMLMetadataSerializer
from api.saml.parser import SAMLMetadataParsingError, SAMLMetadataParser
from api.saml.provider import SAMLAuthenticationProvider
from api.saml.validator import SAMLSettingsValidator, INCORRECT_METADATA
from core.model import ConfigurationSetting
from core.util.problem_detail import ProblemDetail
from tests.saml import fixtures
from tests.saml.database_test import DatabaseTest


class SAMLSettingsValidatorTest(DatabaseTest):
    def _get_result_providers(self, setting_name):
        """
        Fetches provider definitions from the settings

        :param setting_name: Name of the setting
        :type setting_name: string

        :return: ServiceProviderMetadata instance of a list of IdentityProviderMetadata
        :rtype: Union[ServiceProviderMetadata, List[IdentityProviderMetadata]]
        """

        raw_providers = ConfigurationSetting.for_library_and_externalintegration(
            self._db,
            setting_name,
            self._authentication_provider.library(self._db),
            self._authentication_provider.external_integration(self._db)).value
        providers = pickle.loads(raw_providers)

        return providers

    def _get_result_sp_provider(self):
        """
        Fetches SP provider definition from the settings

        :return: ServiceProviderMetadata
        :rtype: ServiceProviderMetadata
        """

        return self._get_result_providers(configuration.SAMLConfiguration.SP_METADATA)

    def _get_result_idp_providers(self):
        """
        Fetches IdP provider definitions from the settings

        :return: List of IdentityProviderMetadata
        :rtype: List[IdentityProviderMetadata]
        """

        return self._get_result_providers(configuration.SAMLConfiguration.IDP_METADATA)

    def _get_expected_providers(self, xml_metadata):
        """
        Returns expected provider definitions

        :param xml_metadata: Providers' SAML XML metadata
        :param xml_metadata: string

        :return: List IdentityProviderMetadata or ServiceProviderMetadata objects
        :rtype: List[ProviderMetadata]
        """

        metadata_parser = SAMLMetadataParser()

        try:
            providers = metadata_parser.parse(xml_metadata)

            return providers
        except SAMLMetadataParsingError:
            return None

    def _get_expected_sp_provider(self, xml_metadata):
        """
        Returns expected SP definition

        :param xml_metadata: SP provider's SAML XML metadata
        :param xml_metadata: string

        :return: ServiceProviderMetadata object
        :rtype: ServiceProviderMetadata
        """

        sp_providers = self._get_expected_providers(xml_metadata)

        if sp_providers:
            return sp_providers[0]
        else:
            return None

    def _get_expected_idp_providers(self, xml_metadata):
        """
        Returns a list of expected IdP provider definitions

        :param xml_metadata: SP provider's SAML XML metadata
        :param xml_metadata: string

        :return: List of IdentityProviderMetadata objects
        :rtype: List[IdentityProviderMetadata]
        """

        idp_providers = self._get_expected_providers(xml_metadata)

        return idp_providers

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
            INCORRECT_METADATA.detailed(
                'Missing urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST AssertionConsumerService')
        ),
        (
            'correct_sp_metadata_and_incorrect_idp_metadata',
            fixtures.CORRECT_ONE_SP_METADATA,
            fixtures.INCORRECT_ONE_IDP_METADATA_WITHOUT_SSO_SERVICE,
            INCORRECT_METADATA.detailed(
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
        settings = SAMLAuthenticationProvider.SETTINGS
        metadata_parser = SAMLMetadataParser()
        metadata_serializer = SAMLMetadataSerializer(self._integration)
        validator = SAMLSettingsValidator(
            metadata_parser,
            metadata_serializer)

        # Act
        result = validator.validate(settings, submitted_form)

        # Assert
        eq_(result, expected_validation_result)

        if not isinstance(result, ProblemDetail):
            result_sp = self._get_result_sp_provider()
            expected_sp = self._get_expected_sp_provider(sp_xml_metadata)
            eq_(result_sp, expected_sp)

            result_idps = self._get_result_idp_providers()
            expected_idps = self._get_expected_idp_providers(idp_xml_metadata)
            eq_(result_idps, expected_idps)
