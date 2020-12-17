import datetime
import json

from freezegun import freeze_time
from mock import MagicMock, create_autospec, patch
from nose.tools import eq_
from parameterized import parameterized

from api.authenticator import PatronData
from api.saml.auth import SAMLAuthenticationManager, SAMLAuthenticationManagerFactory
from api.saml.configuration.model import (
    SAMLConfiguration,
    SAMLConfigurationFactory,
    SAMLOneLoginConfiguration,
)
from api.saml.metadata.filter import SAMLSubjectFilter
from api.saml.metadata.model import (
    SAMLAttribute,
    SAMLAttributeStatement,
    SAMLAttributeType,
    SAMLIdentityProviderMetadata,
    SAMLLocalizedMetadataItem,
    SAMLNameIDFormat,
    SAMLOrganization,
    SAMLService,
    SAMLServiceProviderMetadata,
    SAMLSubject,
    SAMLSubjectJSONEncoder,
    SAMLUIInfo,
    SAMLNameID,
)
from api.saml.metadata.parser import SAMLSubjectParser, SAMLMetadataParser
from api.saml.provider import SAML_INVALID_SUBJECT, SAMLWebSSOAuthenticationProvider
from core.model.configuration import HasExternalIntegration, ConfigurationStorage
from core.python_expression_dsl.evaluator import DSLEvaluationVisitor, DSLEvaluator
from core.python_expression_dsl.parser import DSLParser
from core.util.problem_detail import ProblemDetail
from tests.saml import fixtures
from tests.saml.controller_test import ControllerTest

SERVICE_PROVIDER = SAMLServiceProviderMetadata(
    fixtures.SP_ENTITY_ID,
    SAMLUIInfo(),
    SAMLOrganization(),
    SAMLNameIDFormat.UNSPECIFIED.value,
    SAMLService(fixtures.SP_ACS_URL, fixtures.SP_ACS_BINDING),
)

IDENTITY_PROVIDER_WITH_DISPLAY_NAME = SAMLIdentityProviderMetadata(
    fixtures.IDP_2_ENTITY_ID,
    SAMLUIInfo(
        display_names=[
            SAMLLocalizedMetadataItem(fixtures.IDP_1_UI_INFO_EN_DISPLAY_NAME, "en"),
            SAMLLocalizedMetadataItem(fixtures.IDP_1_UI_INFO_ES_DISPLAY_NAME, "es"),
        ],
        descriptions=[
            SAMLLocalizedMetadataItem(fixtures.IDP_1_UI_INFO_DESCRIPTION, "en"),
            SAMLLocalizedMetadataItem(fixtures.IDP_1_UI_INFO_DESCRIPTION, "es"),
        ],
        information_urls=[
            SAMLLocalizedMetadataItem(fixtures.IDP_1_UI_INFO_INFORMATION_URL, "en"),
            SAMLLocalizedMetadataItem(fixtures.IDP_1_UI_INFO_INFORMATION_URL, "es"),
        ],
        privacy_statement_urls=[
            SAMLLocalizedMetadataItem(
                fixtures.IDP_1_UI_INFO_PRIVACY_STATEMENT_URL, "en"
            ),
            SAMLLocalizedMetadataItem(
                fixtures.IDP_1_UI_INFO_PRIVACY_STATEMENT_URL, "es"
            ),
        ],
        logo_urls=[
            SAMLLocalizedMetadataItem(fixtures.IDP_1_UI_INFO_LOGO_URL, "en"),
            SAMLLocalizedMetadataItem(fixtures.IDP_1_UI_INFO_LOGO_URL, "es"),
        ],
    ),
    SAMLOrganization(),
    SAMLNameIDFormat.UNSPECIFIED.value,
    SAMLService(fixtures.IDP_2_SSO_URL, fixtures.IDP_2_SSO_BINDING),
)

IDENTITY_PROVIDER_WITH_ORGANIZATION_DISPLAY_NAME = SAMLIdentityProviderMetadata(
    fixtures.IDP_2_ENTITY_ID,
    SAMLUIInfo(),
    SAMLOrganization(
        organization_display_names=[
            SAMLLocalizedMetadataItem(
                fixtures.IDP_1_ORGANIZATION_EN_ORGANIZATION_DISPLAY_NAME, "en"
            ),
            SAMLLocalizedMetadataItem(
                fixtures.IDP_1_ORGANIZATION_ES_ORGANIZATION_DISPLAY_NAME, "es"
            ),
        ]
    ),
    SAMLNameIDFormat.UNSPECIFIED.value,
    SAMLService(fixtures.IDP_2_SSO_URL, fixtures.IDP_2_SSO_BINDING),
)

IDENTITY_PROVIDER_WITHOUT_DISPLAY_NAMES = SAMLIdentityProviderMetadata(
    fixtures.IDP_1_ENTITY_ID,
    SAMLUIInfo(),
    SAMLOrganization(),
    SAMLNameIDFormat.UNSPECIFIED.value,
    SAMLService(fixtures.IDP_1_SSO_URL, fixtures.IDP_1_SSO_BINDING),
)


class TestSAMLWebSSOAuthenticationProvider(ControllerTest):
    def setup(self, _db=None, set_up_circulation_manager=True):
        super(TestSAMLWebSSOAuthenticationProvider, self).setup(
            _db, set_up_circulation_manager
        )

        metadata_parser = SAMLMetadataParser()

        self._external_integration_association = create_autospec(
            spec=HasExternalIntegration
        )
        self._external_integration_association.external_integration = MagicMock(
            return_value=self._integration
        )

        self._configuration_storage = ConfigurationStorage(
            self._external_integration_association
        )
        self._configuration_factory = SAMLConfigurationFactory(metadata_parser)

    @parameterized.expand(
        [
            (
                "identity_provider_with_display_name",
                [IDENTITY_PROVIDER_WITH_DISPLAY_NAME],
                {
                    "type": SAMLWebSSOAuthenticationProvider.FLOW_TYPE,
                    "description": SAMLWebSSOAuthenticationProvider.NAME,
                    "links": [
                        {
                            "rel": "authenticate",
                            "href": "http://localhost/default/saml_authenticate?idp_entity_id=http%3A%2F%2Fidp2.hilbertteam.net%2Fidp%2Fshibboleth&provider=SAML+2.0+Web+SSO",
                            "display_names": [
                                {
                                    "value": fixtures.IDP_1_UI_INFO_EN_DISPLAY_NAME,
                                    "language": "en",
                                },
                                {
                                    "value": fixtures.IDP_1_UI_INFO_ES_DISPLAY_NAME,
                                    "language": "es",
                                },
                            ],
                            "descriptions": [
                                {
                                    "value": fixtures.IDP_1_UI_INFO_DESCRIPTION,
                                    "language": "en",
                                },
                                {
                                    "value": fixtures.IDP_1_UI_INFO_DESCRIPTION,
                                    "language": "es",
                                },
                            ],
                            "information_urls": [
                                {
                                    "value": fixtures.IDP_1_UI_INFO_INFORMATION_URL,
                                    "language": "en",
                                },
                                {
                                    "value": fixtures.IDP_1_UI_INFO_INFORMATION_URL,
                                    "language": "es",
                                },
                            ],
                            "privacy_statement_urls": [
                                {
                                    "value": fixtures.IDP_1_UI_INFO_PRIVACY_STATEMENT_URL,
                                    "language": "en",
                                },
                                {
                                    "value": fixtures.IDP_1_UI_INFO_PRIVACY_STATEMENT_URL,
                                    "language": "es",
                                },
                            ],
                            "logo_urls": [
                                {
                                    "value": fixtures.IDP_1_UI_INFO_LOGO_URL,
                                    "language": "en",
                                },
                                {
                                    "value": fixtures.IDP_1_UI_INFO_LOGO_URL,
                                    "language": "es",
                                },
                            ],
                        }
                    ],
                },
            ),
            (
                "identity_provider_with_organization_display_name",
                [IDENTITY_PROVIDER_WITH_ORGANIZATION_DISPLAY_NAME],
                {
                    "type": SAMLWebSSOAuthenticationProvider.FLOW_TYPE,
                    "description": SAMLWebSSOAuthenticationProvider.NAME,
                    "links": [
                        {
                            "rel": "authenticate",
                            "href": "http://localhost/default/saml_authenticate?idp_entity_id=http%3A%2F%2Fidp2.hilbertteam.net%2Fidp%2Fshibboleth&provider=SAML+2.0+Web+SSO",
                            "display_names": [
                                {
                                    "value": fixtures.IDP_1_ORGANIZATION_EN_ORGANIZATION_DISPLAY_NAME,
                                    "language": "en",
                                },
                                {
                                    "value": fixtures.IDP_1_ORGANIZATION_ES_ORGANIZATION_DISPLAY_NAME,
                                    "language": "es",
                                },
                            ],
                            "descriptions": [],
                            "information_urls": [],
                            "privacy_statement_urls": [],
                            "logo_urls": [],
                        }
                    ],
                },
            ),
            (
                "identity_provider_without_display_names_and_default_template",
                [
                    IDENTITY_PROVIDER_WITHOUT_DISPLAY_NAMES,
                    IDENTITY_PROVIDER_WITHOUT_DISPLAY_NAMES,
                ],
                {
                    "type": SAMLWebSSOAuthenticationProvider.FLOW_TYPE,
                    "description": SAMLWebSSOAuthenticationProvider.NAME,
                    "links": [
                        {
                            "rel": "authenticate",
                            "href": "http://localhost/default/saml_authenticate?idp_entity_id=http%3A%2F%2Fidp1.hilbertteam.net%2Fidp%2Fshibboleth&provider=SAML+2.0+Web+SSO",
                            "display_names": [
                                {
                                    "value": SAMLConfiguration.IDP_DISPLAY_NAME_DEFAULT_TEMPLATE.format(
                                        1
                                    ),
                                    "language": "en",
                                }
                            ],
                            "descriptions": [],
                            "information_urls": [],
                            "privacy_statement_urls": [],
                            "logo_urls": [],
                        },
                        {
                            "rel": "authenticate",
                            "href": "http://localhost/default/saml_authenticate?idp_entity_id=http%3A%2F%2Fidp1.hilbertteam.net%2Fidp%2Fshibboleth&provider=SAML+2.0+Web+SSO",
                            "display_names": [
                                {
                                    "value": SAMLConfiguration.IDP_DISPLAY_NAME_DEFAULT_TEMPLATE.format(
                                        2
                                    ),
                                    "language": "en",
                                }
                            ],
                            "descriptions": [],
                            "information_urls": [],
                            "privacy_statement_urls": [],
                            "logo_urls": [],
                        },
                    ],
                },
            ),
        ]
    )
    def test_authentication_document(self, _, identity_providers, expected_result):
        # Arrange
        configuration = create_autospec(spec=SAMLConfiguration)
        configuration.get_service_provider = MagicMock(return_value=SERVICE_PROVIDER)
        configuration.get_identity_providers = MagicMock(
            return_value=identity_providers
        )

        configuration_factory = create_autospec(spec=SAMLConfigurationFactory)
        configuration_factory.create = MagicMock(return_value=configuration)

        onelogin_configuration = SAMLOneLoginConfiguration(configuration)
        subject_parser = SAMLSubjectParser()
        parser = DSLParser()
        visitor = DSLEvaluationVisitor()
        evaluator = DSLEvaluator(parser, visitor)
        subject_filter = SAMLSubjectFilter(evaluator)
        authentication_manager = SAMLAuthenticationManager(
            onelogin_configuration, subject_parser, subject_filter
        )

        authentication_manager_factory = create_autospec(
            spec=SAMLAuthenticationManagerFactory
        )
        authentication_manager_factory.create = MagicMock(
            return_value=authentication_manager
        )

        with patch(
            "api.saml.provider.SAMLAuthenticationManagerFactory"
        ) as authentication_manager_factory_constructor_mock, patch(
            "api.saml.provider.SAMLConfigurationFactory"
        ) as configuration_factory_constructor_mock:
            authentication_manager_factory_constructor_mock.return_value = (
                authentication_manager_factory
            )
            configuration_factory_constructor_mock.return_value = configuration_factory

            # Act
            provider = SAMLWebSSOAuthenticationProvider(
                self._default_library, self._integration
            )

            self.app.config["SERVER_NAME"] = "localhost"

            with self.app.test_request_context("/"):
                result = provider.authentication_flow_document(self._db)

            # Assert
            eq_(expected_result, result)

    @parameterized.expand(
        [
            ("empty_subject", None, SAML_INVALID_SUBJECT.detailed("Subject is empty")),
            (
                "subject_is_patron_data",
                PatronData(permanent_id=12345),
                PatronData(permanent_id=12345),
            ),
            (
                "subject_does_not_have_unique_id",
                SAMLSubject(None, None),
                SAML_INVALID_SUBJECT.detailed("Subject does not have a unique ID"),
            ),
            (
                "subject_has_unique_id",
                SAMLSubject(
                    None,
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonUniqueId.name,
                                values=["12345"],
                            )
                        ]
                    ),
                ),
                PatronData(
                    permanent_id="12345",
                    authorization_identifier="12345",
                    external_type="A",
                    complete=True,
                ),
            ),
        ]
    )
    def test_remote_patron_lookup(self, _, subject, expected_result):
        # Arrange
        provider = SAMLWebSSOAuthenticationProvider(
            self._default_library, self._integration
        )

        # Act
        result = provider.remote_patron_lookup(subject)

        # Assert
        if isinstance(result, ProblemDetail):
            eq_(result.response, expected_result.response)
        else:
            eq_(result, expected_result)

    @parameterized.expand(
        [
            ("empty_subject", None, SAML_INVALID_SUBJECT.detailed("Subject is empty")),
            (
                "subject_does_not_have_unique_id",
                SAMLSubject(None, None),
                SAML_INVALID_SUBJECT.detailed("Subject does not have a unique ID"),
            ),
            (
                "subject_has_unique_id",
                SAMLSubject(
                    None,
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonUniqueId.name,
                                values=["12345"],
                            )
                        ]
                    ),
                ),
                PatronData(
                    permanent_id="12345",
                    authorization_identifier="12345",
                    external_type="A",
                    complete=True,
                ),
            ),
            (
                "subject_has_unique_id_and_persistent_name_id",
                SAMLSubject(
                    SAMLNameID(
                        SAMLNameIDFormat.PERSISTENT.value,
                        "name-qualifier",
                        "sp-name-qualifier",
                        "12345",
                    ),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonUniqueId.name,
                                values=["12345"],
                            )
                        ]
                    ),
                ),
                PatronData(
                    permanent_id="12345",
                    authorization_identifier="12345",
                    external_type="A",
                    complete=True,
                ),
                None,
            ),
            (
                "subject_has_unique_id_and_transient_name_id",
                SAMLSubject(
                    SAMLNameID(
                        SAMLNameIDFormat.TRANSIENT.value,
                        "name-qualifier",
                        "sp-name-qualifier",
                        "12345",
                    ),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonUniqueId.name,
                                values=["12345"],
                            )
                        ]
                    ),
                ),
                PatronData(
                    permanent_id="12345",
                    authorization_identifier="12345",
                    external_type="A",
                    complete=True,
                ),
                '{"attributes": {"eduPersonUniqueId": ["12345"]}}',
            ),
            (
                "subject_has_unique_id_and_custom_session_lifetime",
                SAMLSubject(
                    None,
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonUniqueId.name,
                                values=["12345"],
                            )
                        ]
                    ),
                ),
                PatronData(
                    permanent_id="12345",
                    authorization_identifier="12345",
                    external_type="A",
                    complete=True,
                ),
                None,
                datetime.datetime(2020, 1, 1) + datetime.timedelta(days=42),
                42,
            ),
            (
                "subject_has_unique_id_and_empty_session_lifetime",
                SAMLSubject(
                    None,
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonUniqueId.name,
                                values=["12345"],
                            )
                        ]
                    ),
                ),
                PatronData(
                    permanent_id="12345",
                    authorization_identifier="12345",
                    external_type="A",
                    complete=True,
                ),
                None,
                None,
                "",
            ),
            (
                "subject_has_unique_id_and_non_default_expiration_timeout",
                SAMLSubject(
                    None,
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonUniqueId.name,
                                values=["12345"],
                            )
                        ]
                    ),
                    valid_till=datetime.timedelta(days=1),
                ),
                PatronData(
                    permanent_id="12345",
                    authorization_identifier="12345",
                    external_type="A",
                    complete=True,
                ),
            ),
            (
                "subject_has_unique_id_non_default_expiration_timeout_and_custom_session_lifetime",
                SAMLSubject(
                    None,
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonUniqueId.name,
                                values=["12345"],
                            )
                        ]
                    ),
                    valid_till=datetime.timedelta(days=1),
                ),
                PatronData(
                    permanent_id="12345",
                    authorization_identifier="12345",
                    external_type="A",
                    complete=True,
                ),
                None,
                datetime.datetime(2020, 1, 1) + datetime.timedelta(days=42),
                42,
            ),
            (
                "subject_has_unique_id_non_default_expiration_timeout_and_empty_session_lifetime",
                SAMLSubject(
                    None,
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonUniqueId.name,
                                values=["12345"],
                            )
                        ]
                    ),
                    valid_till=datetime.timedelta(days=1),
                ),
                PatronData(
                    permanent_id="12345",
                    authorization_identifier="12345",
                    external_type="A",
                    complete=True,
                ),
                None,
                None,
                "",
            ),
        ]
    )
    @freeze_time("2020-01-01 00:00:00")
    def test_saml_callback(
        self,
        _,
        subject,
        expected_patron_data,
        expected_credential=None,
        expected_expiration_time=None,
        cm_session_lifetime=None,
    ):
        # This test makes sure that SAMLWebSSOAuthenticationProvider.saml_callback
        # correctly processes a SAML subject and returns right PatronData.

        # Arrange
        provider = SAMLWebSSOAuthenticationProvider(
            self._default_library, self._integration
        )

        if expected_credential is None:
            expected_credential = json.dumps(subject, cls=SAMLSubjectJSONEncoder)

        if expected_expiration_time is None and subject is not None:
            expected_expiration_time = datetime.datetime.utcnow() + subject.valid_till

        if cm_session_lifetime is not None:
            with self._configuration_factory.create(
                self._configuration_storage, self._db, SAMLConfiguration
            ) as configuration:
                configuration.session_lifetime = cm_session_lifetime

        # Act
        result = provider.saml_callback(self._db, subject)

        # Assert
        if isinstance(result, ProblemDetail):
            eq_(result.response, expected_patron_data.response)
        else:
            credential, patron, patron_data = result

            eq_(expected_credential, credential.credential)
            eq_(expected_patron_data.permanent_id, patron.external_identifier)
            eq_(expected_patron_data, patron_data)
            eq_(expected_expiration_time, credential.expires)
