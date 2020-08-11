import logging

from flask_babel import lazy_gettext as _

from api.admin.problem_details import INCOMPLETE_CONFIGURATION
from api.admin.validator import Validator
from api.saml import configuration
from api.saml.parser import SAMLMetadataParsingError
from core.problem_details import *
from core.util.problem_detail import ProblemDetail

SAML_INCORRECT_METADATA = pd(
    'http://librarysimplified.org/terms/problem/saml/metadata-incorrect-format',
    status_code=400,
    title=_('SAML metadata has incorrect format.'),
    detail=_('SAML metadata has incorrect format.')
)

SAML_GENERIC_PARSING_ERROR = pd(
    'http://librarysimplified.org/terms/problem/saml/generic-parsing-error',
    status_code=500,
    title=_('Unexpected error.'),
    detail=_('An unexpected error occurred during validation of SAML authentication settings.')
)


class SAMLSettingsValidator(Validator):
    """Validates SAMLAuthenticationProvider's settings submitted by a user"""

    def __init__(self, metadata_parser):
        """Initializes a new instance of SAMLAuthenticationProviderSettingsValidator class

        :param metadata_parser: SAML metadata parser
        :type metadata_parser: MetadataParser
        """
        self._logger = logging.getLogger(__name__)
        self._metadata_parser = metadata_parser

    def _get_setting_value(self, settings, content, setting_name):
        """Selects a setting's value from the form submitted by a user

        :param settings: Dictionary containing provider's settings (SAMLAuthenticationProvider.SETTINGS)
        :type: Dict

        :param content: Dictionary containing submitted form's metadata
        :type content: werkzeug.datastructures.MultiDict

        :param setting_name: Name of the setting
        :param setting_name: string

        :return: Setting's value set by the user or a ProblemDetail instance in the case of any error
        :rtype: Union[string, ProblemDetail]
        """
        submitted_form = content.get('form')
        setting_values = self._extract_inputs(settings, setting_name, submitted_form, 'key')

        if setting_values is None or not isinstance(setting_values, list) or len(setting_values) < 1:
            return INCOMPLETE_CONFIGURATION.detailed('Required field {0} is missing'.format(setting_name))

        return setting_values[0]

    def _parse_metadata(self, xml_metadata):
        """Parses SAML XML metadata

        :param xml_metadata: SAML XML metadata
        :type xml_metadata: string

        :return: List of IdentityProviderMetadata/ServiceProvider instances or a ProblemDetail instance
        in the case of any errors
        :rtype: Union[List[ProviderMetadata], ProblemDetail]
        """
        try:
            result = self._metadata_parser.parse(xml_metadata)

            return result
        except SAMLMetadataParsingError as exception:
            self._logger.exception('An unexpected exception occurred duing parsing SAML metadata')

            return SAML_INCORRECT_METADATA.detailed(exception.message)
        except Exception as exception:
            self._logger.exception('An unexpected exception occurred duing parsing SAML metadata')

            return SAML_GENERIC_PARSING_ERROR.detailed(exception.message)

    def _get_providers(self, settings, content, setting_name):
        """Fetches provider definition from the SAML metadata submitted by the user

        :param settings: Dictionary containing provider's settings (SAMLAuthenticationProvider.SETTINGS)
        :type: Dict

        :param content: Dictionary containing submitted form's metadata
        :type content: werkzeug.datastructures.MultiDict

        :param setting_name: Name of the setting
        :param setting_name: string

        :return: List of IdentityProviderMetadata/ServiceProvider instances or a ProblemDetail instance
        in the case of any errors
        :rtype: Union[List[ProviderMetadata], ProblemDetail]
        """
        provider_xml_metadata = self._get_setting_value(settings, content, setting_name)

        if isinstance(provider_xml_metadata, ProblemDetail):
            return provider_xml_metadata

        providers = self._parse_metadata(provider_xml_metadata)

        return providers

    def _process_sp_providers(self, settings, content, setting_name):
        """Fetches SP provider definition from the SAML metadata submitted by the user

        :param settings: Dictionary containing provider's settings (SAMLAuthenticationProvider.SETTINGS)
        :type: Dict

        :param content: Dictionary containing submitted form's metadata
        :type content: werkzeug.datastructures.MultiDict

        :param setting_name: Name of the setting
        :param setting_name: string

        :return: SP provider definition or a ProblemDetail instance in the case of any errors
        :rtype: Union[ServiceProviderMetadata, ProblemDetail]
        """
        sp_providers = self._get_providers(settings, content, setting_name)

        if isinstance(sp_providers, ProblemDetail):
            return sp_providers

        if len(sp_providers) != 1:
            return SAML_INCORRECT_METADATA.detailed(
                'Service Provider\'s XML metadata must contain exactly one declaration of SPSSODescriptor')

        return sp_providers[0]

    def _process_idp_providers(self, settings, content, setting_name):
        """Fetches IdP provider definitions from the SAML metadata submitted by the user

        :param settings: Dictionary containing provider's settings (SAMLAuthenticationProvider.SETTINGS)
        :type: Dict

        :param content: Dictionary containing submitted form's metadata
        :type content: werkzeug.datastructures.MultiDict

        :param setting_name: Name of the setting
        :param setting_name: string

        :return: List of IdP provider definitions or a ProblemDetail instance in the case of any errors
        :rtype: Union[List[IdentityProviderMetadata], ProblemDetail]
        """
        idp_providers = self._get_providers(settings, content, setting_name)

        if isinstance(idp_providers, ProblemDetail):
            return idp_providers

        if len(idp_providers) < 0:
            return SAML_INCORRECT_METADATA.detailed(
                'Identity Provider\'s XML metadata must contain at least one declaration of IDPSSODescriptor')

        return idp_providers

    def validate(self, settings, content):
        """Validates provider's setting values submitted by the user

        :param settings: Dictionary containing provider's settings (SAMLAuthenticationProvider.SETTINGS)
        :type settings: Optional[ProblemDetail]

        :param content: Dictionary containing submitted form's metadata
        :type content: werkzeug.datastructures.MultiDict

        :return: ProblemDetail in the case of any errors, None if validation succeeded
        :rtype: Optional[ProblemDetail]
        """
        validation_result = super(SAMLSettingsValidator, self).validate(settings, content)

        if isinstance(validation_result, ProblemDetail):
            return validation_result

        sp_providers = self._process_sp_providers(settings, content, configuration.SAMLConfiguration.SP_XML_METADATA)

        if isinstance(sp_providers, ProblemDetail):
            return sp_providers

        idp_providers = self._process_idp_providers(settings, content, configuration.SAMLConfiguration.IDP_XML_METADATA)

        if isinstance(idp_providers, ProblemDetail):
            return idp_providers

        return None
