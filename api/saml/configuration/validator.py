import logging
import re
from enum import Enum
import six
from flask_babel import lazy_gettext as _

from api.admin.problem_details import INCOMPLETE_CONFIGURATION
from api.admin.validator import Validator
from api.saml.configuration.model import SAMLConfiguration
from api.saml.metadata.filter import SAMLSubjectFilter, SAMLSubjectFilterError
from api.saml.metadata.model import SAMLSubjectPatronIDExtractor
from api.saml.metadata.parser import SAMLMetadataParser, SAMLMetadataParsingError
from core.problem_details import *
from core.util.problem_detail import ProblemDetail

SAML_INCORRECT_METADATA = pd(
    "http://librarysimplified.org/terms/problem/saml/incorrect-metadata-format",
    status_code=400,
    title=_("SAML metadata has an incorrect format."),
    detail=_("SAML metadata has an incorrect format."),
)

SAML_GENERIC_PARSING_ERROR = pd(
    "http://librarysimplified.org/terms/problem/saml/generic-parsing-error",
    status_code=500,
    title=_("Unexpected error."),
    detail=_(
        "An unexpected error occurred during validation of SAML authentication settings."
    ),
)

SAML_INCORRECT_FILTRATION_EXPRESSION = pd(
    "http://librarysimplified.org/terms/problem/saml/incorrect-filtration-expression-format",
    status_code=400,
    title=_("SAML filtration expression has an incorrect format."),
    detail=_("SAML filtration expression has an incorrect format."),
)

SAML_INCORRECT_PATRON_ID_REGULAR_EXPRESSION = pd(
    "http://librarysimplified.org/terms/problem/saml/incorrect-patron-id-regex",
    status_code=400,
    title=_("SAML patron ID regular expression has an incorrect format."),
    detail=_("SAML patron ID regular expression has an incorrect format."),
)


class ProviderType(Enum):
    ServiceProvider = "SP"
    IdentityProvider = "IdP"


class SAMLSettingsValidator(Validator):
    """Validates SAMLAuthenticationProvider's settings submitted by a user"""

    def __init__(self, metadata_parser, subject_filter):
        """Initializes a new instance of SAMLAuthenticationProviderSettingsValidator class

        :param metadata_parser: SAML metadata parser
        :type metadata_parser: api.saml.metadata.parser.SAMLMetadataParser

        :param subject_filter: SAML subject filter
        :type subject_filter: api.saml.metadata.filter.SAMLSubjectFilter
        """
        if not isinstance(metadata_parser, SAMLMetadataParser):
            raise ValueError(
                "Argument 'metadata_parser' must be an instance of {0} class".format(
                    SAMLMetadataParser
                )
            )
        if not isinstance(subject_filter, SAMLSubjectFilter):
            raise ValueError(
                "Argument 'subject_filter' must be an instance of {0} class".format(
                    SAMLSubjectFilter
                )
            )

        self._metadata_parser = metadata_parser
        self._subject_filter = subject_filter

        self._logger = logging.getLogger(__name__)

    def _get_setting_value(
        self, settings, content, setting_key, setting_name, required
    ):
        """Selects a setting's value from the form submitted by a user

        :param settings: Dictionary containing provider's settings (SAMLAuthenticationProvider.SETTINGS)
        :type: Dict

        :param content: Dictionary containing submitted form's metadata
        :type content: werkzeug.datastructures.MultiDict

        :param setting_key: Setting's key
        :param setting_key: str

        :param setting_name: Setting's name
        :param setting_name: str

        :param required: Boolean value indicating whether the setting is required
        :param required: bool

        :return: Setting's value set by the user or a ProblemDetail instance in the case of any error
        :rtype: Union[str, core.util.problem_detail.ProblemDetail]
        """
        submitted_form = content.get("form")
        setting_values = self._extract_inputs(
            settings, setting_key, submitted_form, "key"
        )

        if required and not setting_values:
            return INCOMPLETE_CONFIGURATION.detailed(
                _("Required field '{0}' is missing".format(setting_name))
            )

        return setting_values[0] if setting_values else None

    def _parse_metadata(self, xml_metadata, provider_type):
        """Parses SAML XML metadata

        :param xml_metadata: SAML XML metadata
        :type xml_metadata: string

        :param provider_type: Type of the metadata: SP or IdP
        :type provider_type: ProviderType

        :return: List of IdentityProviderMetadata/ServiceProvider instances or a ProblemDetail instance
            in the case of any errors
        :rtype: Union[List[api.saml.metadata.model.SAMLProviderMetadata], core.util.problem_detail.ProblemDetail]
        """
        try:
            result = self._metadata_parser.parse(xml_metadata)

            return result
        except SAMLMetadataParsingError as exception:
            self._logger.exception(
                "An unexpected exception occurred during parsing of SAML metadata"
            )

            if provider_type == ProviderType.ServiceProvider:
                message = (
                    "Service Provider's metadata has incorrect format: {0}".format(
                        six.ensure_text(str(exception))
                    )
                )
            else:
                message = (
                    "Identity Provider's metadata has incorrect format: {0}".format(
                        six.ensure_text(str(exception))
                    )
                )

            return SAML_INCORRECT_METADATA.detailed(message)
        except Exception as exception:
            self._logger.exception(
                "An unexpected exception occurred duing parsing SAML metadata"
            )

            return SAML_GENERIC_PARSING_ERROR.detailed(str(exception))

    def _get_providers(
        self, settings, content, setting_key, setting_name, provider_type
    ):
        """Fetches provider definition from the SAML metadata submitted by the user

        :param settings: Dictionary containing provider's settings (SAMLAuthenticationProvider.SETTINGS)
        :type: Dict

        :param content: Dictionary containing submitted form's metadata
        :type content: werkzeug.datastructures.MultiDict

        :param setting_key: Setting's key
        :param setting_key: str

        :param setting_name: Setting's name
        :param setting_name: str

        :param provider_type: Type of the metadata: SP or IdP
        :type provider_type: ProviderType

        :return: List of IdentityProviderMetadata/ServiceProvider instances or a ProblemDetail instance
        in the case of any errors
        :rtype: Union[List[api.saml.metadata.model.SAMLProviderMetadata], core.util.problem_detail.ProblemDetail]
        """
        provider_xml_metadata = self._get_setting_value(
            settings, content, setting_key, setting_name, True
        )

        if isinstance(provider_xml_metadata, ProblemDetail):
            return provider_xml_metadata

        providers = self._parse_metadata(provider_xml_metadata, provider_type)

        return providers

    def _process_sp_providers(self, settings, content, setting_key, setting_name):
        """Fetches SP provider definition from the SAML metadata submitted by the user

        :param settings: Dictionary containing provider's settings (SAMLAuthenticationProvider.SETTINGS)
        :type: Dict

        :param content: Dictionary containing submitted form's metadata
        :type content: werkzeug.datastructures.MultiDict

        :param setting_key: Setting's key
        :param setting_key: str

        :param setting_name: Setting's name
        :param setting_name: str

        :return: SP provider definition or a ProblemDetail instance in the case of any errors
        :rtype: Union[api.saml.metadata.model.SAMLServiceProviderMetadata, core.util.problem_detail.ProblemDetail]
        """
        sp_providers = self._get_providers(
            settings, content, setting_key, setting_name, ProviderType.ServiceProvider
        )

        if isinstance(sp_providers, ProblemDetail):
            return sp_providers

        if len(sp_providers) != 1:
            return SAML_INCORRECT_METADATA.detailed(
                "Service Provider's XML metadata must contain exactly one declaration of SPSSODescriptor"
            )

        return sp_providers[0]

    def _process_idp_providers(self, settings, content, setting_key, setting_name):
        """Fetches IdP provider definitions from the SAML metadata submitted by the user

        :param settings: Dictionary containing provider's settings (SAMLAuthenticationProvider.SETTINGS)
        :type: Dict

        :param content: Dictionary containing submitted form's metadata
        :type content: werkzeug.datastructures.MultiDict

        :param setting_key: Setting's key
        :param setting_key: str

        :param setting_name: Setting's name
        :param setting_name: str

        :return: List of IdP provider definitions or a ProblemDetail instance in the case of any errors
        :rtype: Union[
            List[api.saml.metadata.model.SAMLIdentityProviderMetadata],
            core.util.problem_detail.ProblemDetail
        ]
        """
        idp_providers = self._get_providers(
            settings, content, setting_key, setting_name, ProviderType.IdentityProvider
        )

        if isinstance(idp_providers, ProblemDetail):
            return idp_providers

        if len(idp_providers) < 0:
            return SAML_INCORRECT_METADATA.detailed(
                "Identity Provider's XML metadata must contain at least one declaration of IDPSSODescriptor"
            )

        return idp_providers

    def _process_filtration_expression(
        self, settings, content, setting_key, setting_name
    ):
        filtration_expression = self._get_setting_value(
            settings, content, setting_key, setting_name, False
        )

        if filtration_expression:
            try:
                self._subject_filter.validate(filtration_expression)
            except SAMLSubjectFilterError as exception:
                self._logger.exception("Validation of the filtration expression failed")

                return SAML_INCORRECT_FILTRATION_EXPRESSION.detailed(
                    _(
                        "SAML filtration expression has an incorrect format: {0}".format(
                            six.ensure_text(str(exception))
                        )
                    )
                )

    def _validate_patron_id_regular_expression(
        self, settings, content, setting_key, setting_name
    ):
        """Validate a regular expression used to extract a unique patron ID from SAML attributes.

        :param settings: Dictionary containing provider's settings (SAMLAuthenticationProvider.SETTINGS)
        :type: Dict

        :param content: Dictionary containing submitted form's metadata
        :type content: werkzeug.datastructures.MultiDict

        :param setting_key: Setting's key
        :param setting_key: str

        :param setting_name: Setting's name
        :param setting_name: str

        :return: ProblemDetail object if the regular expression is invalid
        :rtype: Optional[ProblemDetail]
        """
        patron_id_regular_expression = self._get_setting_value(
            settings, content, setting_key, setting_name, False
        )

        if patron_id_regular_expression:
            try:
                regex = re.compile(patron_id_regular_expression)

                if (
                    SAMLSubjectPatronIDExtractor.PATRON_ID_REGULAR_EXPRESSION_NAMED_GROUP
                    not in regex.groupindex
                ):
                    return SAML_INCORRECT_PATRON_ID_REGULAR_EXPRESSION.detailed(
                        _(
                            "SAML patron ID regular expression '{0}' does not have mandatory named group '{1}'".format(
                                six.ensure_text(patron_id_regular_expression),
                                six.ensure_text(
                                    SAMLSubjectPatronIDExtractor.PATRON_ID_REGULAR_EXPRESSION_NAMED_GROUP
                                ),
                            )
                        )
                    )
            except re.error as exception:
                error_message = "SAML patron ID regular expression '{0}' has an incorrect format: {1}".format(
                    six.ensure_text(patron_id_regular_expression), exception
                )

                self._logger.exception(error_message)

                return SAML_INCORRECT_PATRON_ID_REGULAR_EXPRESSION.detailed(
                    _(error_message)
                )

        return None

    def validate(self, settings, content):
        """Validates provider's setting values submitted by the user

        :param settings: Dictionary containing provider's settings (SAMLAuthenticationProvider.SETTINGS)
        :type settings: Optional[ProblemDetail]

        :param content: Dictionary containing submitted form's metadata
        :type content: werkzeug.datastructures.MultiDict

        :return: ProblemDetail in the case of any errors, None if validation succeeded
        :rtype: Optional[core.util.problem_detail.ProblemDetail]
        """
        validation_result = super(SAMLSettingsValidator, self).validate(
            settings, content
        )

        if isinstance(validation_result, ProblemDetail):
            return validation_result

        validation_result = self._process_sp_providers(
            settings,
            content,
            SAMLConfiguration.service_provider_xml_metadata.key,
            SAMLConfiguration.service_provider_xml_metadata.label,
        )

        if isinstance(validation_result, ProblemDetail):
            return validation_result

        validation_result = self._get_setting_value(
            settings,
            content,
            SAMLConfiguration.non_federated_identity_provider_xml_metadata.key,
            SAMLConfiguration.non_federated_identity_provider_xml_metadata.label,
            False,
        )

        if validation_result and not isinstance(validation_result, ProblemDetail):
            validation_result = self._process_idp_providers(
                settings,
                content,
                SAMLConfiguration.non_federated_identity_provider_xml_metadata.key,
                SAMLConfiguration.non_federated_identity_provider_xml_metadata.label,
            )

            if isinstance(validation_result, ProblemDetail):
                return validation_result

        validation_result = self._process_filtration_expression(
            settings,
            content,
            SAMLConfiguration.filter_expression.key,
            SAMLConfiguration.filter_expression.label,
        )

        if isinstance(validation_result, ProblemDetail):
            return validation_result

        validation_result = self._validate_patron_id_regular_expression(
            settings,
            content,
            SAMLConfiguration.patron_id_regular_expression.key,
            SAMLConfiguration.patron_id_regular_expression.label,
        )

        return validation_result
