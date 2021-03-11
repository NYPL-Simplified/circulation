import pytest
from nose.tools import (
    set_trace,
    eq_,
    assert_raises
)
import flask
from flask_babel import lazy_gettext as _
import json
from werkzeug.datastructures import MultiDict
from api.admin.controller.patron_auth_services import PatronAuthServicesController
from api.admin.exceptions import *
from api.authenticator import (
    AuthenticationProvider,
    BasicAuthenticationProvider,
)
from api.clever import CleverAuthenticationAPI
from api.firstbook import FirstBookAuthenticationAPI
from api.millenium_patron import MilleniumPatronAPI
from api.problem_details import *
from api.saml.provider import SAMLWebSSOAuthenticationProvider
from api.simple_authentication import SimpleAuthenticationProvider
from api.sip import SIP2AuthenticationProvider
from core.model import (
    AdminRole,
    create,
    ConfigurationSetting,
    ExternalIntegration,
    get_one,
    Library,
)
from test_controller import SettingsControllerTest

class TestPatronAuth(SettingsControllerTest):

    def test_patron_auth_services_get_with_no_services(self):
        with self.request_context_with_admin("/"):
            response = self.manager.admin_patron_auth_services_controller.process_patron_auth_services()
            assert response.get("patron_auth_services") == []
            protocols = response.get("protocols")
            assert 8 == len(protocols)
            assert SimpleAuthenticationProvider.__module__ == protocols[0].get("name")
            assert "settings" in protocols[0]
            assert "library_settings" in protocols[0]

            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            self._db.flush()
            pytest.raises(
                AdminNotAuthorized,
                self.manager.admin_patron_auth_services_controller.process_patron_auth_services
            )

    def test_patron_auth_services_get_with_simple_auth_service(self):
        auth_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=SimpleAuthenticationProvider.__module__,
            goal=ExternalIntegration.PATRON_AUTH_GOAL,
            name="name",
        )
        auth_service.setting(BasicAuthenticationProvider.TEST_IDENTIFIER).value = "user"
        auth_service.setting(BasicAuthenticationProvider.TEST_PASSWORD).value = "pass"

        with self.request_context_with_admin("/"):
            response = self.manager.admin_patron_auth_services_controller.process_patron_auth_services()
            [service] = response.get("patron_auth_services")

            assert auth_service.id == service.get("id")
            assert auth_service.name == service.get("name")
            assert SimpleAuthenticationProvider.__module__ == service.get("protocol")
            assert "user" == service.get("settings").get(BasicAuthenticationProvider.TEST_IDENTIFIER)
            assert "pass" == service.get("settings").get(BasicAuthenticationProvider.TEST_PASSWORD)
            assert [] == service.get("libraries")

        auth_service.libraries += [self._default_library]
        with self.request_context_with_admin("/"):
            response = self.manager.admin_patron_auth_services_controller.process_patron_auth_services()
            [service] = response.get("patron_auth_services")

            assert "user" == service.get("settings").get(BasicAuthenticationProvider.TEST_IDENTIFIER)
            [library] = service.get("libraries")
            assert self._default_library.short_name == library.get("short_name")
            assert None == library.get(AuthenticationProvider.EXTERNAL_TYPE_REGULAR_EXPRESSION)

        ConfigurationSetting.for_library_and_externalintegration(
            self._db, AuthenticationProvider.EXTERNAL_TYPE_REGULAR_EXPRESSION,
            self._default_library, auth_service,
        ).value = "^(u)"
        with self.request_context_with_admin("/"):
            response = self.manager.admin_patron_auth_services_controller.process_patron_auth_services()
            [service] = response.get("patron_auth_services")

            [library] = service.get("libraries")
            assert self._default_library.short_name == library.get("short_name")
            assert "^(u)" == library.get(AuthenticationProvider.EXTERNAL_TYPE_REGULAR_EXPRESSION)

    def test_patron_auth_services_get_with_millenium_auth_service(self):
        auth_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=MilleniumPatronAPI.__module__,
            goal=ExternalIntegration.PATRON_AUTH_GOAL
        )
        auth_service.setting(BasicAuthenticationProvider.TEST_IDENTIFIER).value = "user"
        auth_service.setting(BasicAuthenticationProvider.TEST_PASSWORD).value = "pass"
        auth_service.setting(BasicAuthenticationProvider.IDENTIFIER_REGULAR_EXPRESSION).value = "u*"
        auth_service.setting(BasicAuthenticationProvider.PASSWORD_REGULAR_EXPRESSION).value = "p*"
        auth_service.libraries += [self._default_library]
        ConfigurationSetting.for_library_and_externalintegration(
            self._db, AuthenticationProvider.EXTERNAL_TYPE_REGULAR_EXPRESSION,
            self._default_library, auth_service,
        ).value = "^(u)"

        with self.request_context_with_admin("/"):
            response = self.manager.admin_patron_auth_services_controller.process_patron_auth_services()
            [service] = response.get("patron_auth_services")

            assert auth_service.id == service.get("id")
            assert MilleniumPatronAPI.__module__ == service.get("protocol")
            assert "user" == service.get("settings").get(BasicAuthenticationProvider.TEST_IDENTIFIER)
            assert "pass" == service.get("settings").get(BasicAuthenticationProvider.TEST_PASSWORD)
            assert "u*" == service.get("settings").get(BasicAuthenticationProvider.IDENTIFIER_REGULAR_EXPRESSION)
            assert "p*" == service.get("settings").get(BasicAuthenticationProvider.PASSWORD_REGULAR_EXPRESSION)
            [library] = service.get("libraries")
            assert self._default_library.short_name == library.get("short_name")
            assert "^(u)" == library.get(AuthenticationProvider.EXTERNAL_TYPE_REGULAR_EXPRESSION)


    def test_patron_auth_services_get_with_sip2_auth_service(self):
        auth_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=SIP2AuthenticationProvider.__module__,
            goal=ExternalIntegration.PATRON_AUTH_GOAL
        )
        auth_service.url = "url"
        auth_service.setting(SIP2AuthenticationProvider.PORT).value = "1234"
        auth_service.username = "user"
        auth_service.password = "pass"
        auth_service.setting(SIP2AuthenticationProvider.LOCATION_CODE).value = "5"
        auth_service.setting(SIP2AuthenticationProvider.FIELD_SEPARATOR).value = ","

        auth_service.libraries += [self._default_library]
        ConfigurationSetting.for_library_and_externalintegration(
            self._db, AuthenticationProvider.EXTERNAL_TYPE_REGULAR_EXPRESSION,
            self._default_library, auth_service,
        ).value = "^(u)"

        with self.request_context_with_admin("/"):
            response = self.manager.admin_patron_auth_services_controller.process_patron_auth_services()
            [service] = response.get("patron_auth_services")

            assert auth_service.id == service.get("id")
            assert SIP2AuthenticationProvider.__module__ == service.get("protocol")
            assert "url" == service.get("settings").get(ExternalIntegration.URL)
            assert "1234" == service.get("settings").get(SIP2AuthenticationProvider.PORT)
            assert "user" == service.get("settings").get(ExternalIntegration.USERNAME)
            assert "pass" == service.get("settings").get(ExternalIntegration.PASSWORD)
            assert "5" == service.get("settings").get(SIP2AuthenticationProvider.LOCATION_CODE)
            assert "," == service.get("settings").get(SIP2AuthenticationProvider.FIELD_SEPARATOR)
            [library] = service.get("libraries")
            assert self._default_library.short_name == library.get("short_name")
            assert "^(u)" == library.get(AuthenticationProvider.EXTERNAL_TYPE_REGULAR_EXPRESSION)

    def test_patron_auth_services_get_with_firstbook_auth_service(self):
        auth_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=FirstBookAuthenticationAPI.__module__,
            goal=ExternalIntegration.PATRON_AUTH_GOAL
        )
        auth_service.url = "url"
        auth_service.password = "pass"
        auth_service.libraries += [self._default_library]
        ConfigurationSetting.for_library_and_externalintegration(
            self._db, AuthenticationProvider.EXTERNAL_TYPE_REGULAR_EXPRESSION,
            self._default_library, auth_service,
        ).value = "^(u)"

        with self.request_context_with_admin("/"):
            response = self.manager.admin_patron_auth_services_controller.process_patron_auth_services()
            [service] = response.get("patron_auth_services")

            assert auth_service.id == service.get("id")
            assert FirstBookAuthenticationAPI.__module__ == service.get("protocol")
            assert "url" == service.get("settings").get(ExternalIntegration.URL)
            assert "pass" == service.get("settings").get(ExternalIntegration.PASSWORD)
            [library] = service.get("libraries")
            assert self._default_library.short_name == library.get("short_name")
            assert "^(u)" == library.get(AuthenticationProvider.EXTERNAL_TYPE_REGULAR_EXPRESSION)

    def test_patron_auth_services_get_with_clever_auth_service(self):
        auth_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=CleverAuthenticationAPI.__module__,
            goal=ExternalIntegration.PATRON_AUTH_GOAL
        )
        auth_service.username = "user"
        auth_service.password = "pass"
        auth_service.libraries += [self._default_library]

        with self.request_context_with_admin("/"):
            response = self.manager.admin_patron_auth_services_controller.process_patron_auth_services()
            [service] = response.get("patron_auth_services")

            assert auth_service.id == service.get("id")
            assert CleverAuthenticationAPI.__module__ == service.get("protocol")
            assert "user" == service.get("settings").get(ExternalIntegration.USERNAME)
            assert "pass" == service.get("settings").get(ExternalIntegration.PASSWORD)
            [library] = service.get("libraries")
            assert self._default_library.short_name == library.get("short_name")

    def test_patron_auth_services_get_with_saml_auth_service(self):
        auth_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=SAMLWebSSOAuthenticationProvider.__module__,
            goal=ExternalIntegration.PATRON_AUTH_GOAL
        )
        auth_service.libraries += [self._default_library]

        with self.request_context_with_admin("/"):
            response = self.manager.admin_patron_auth_services_controller.process_patron_auth_services()
            [service] = response.get("patron_auth_services")

            assert auth_service.id == service.get("id")
            assert SAMLWebSSOAuthenticationProvider.__module__ == service.get("protocol")
            [library] = service.get("libraries")
            assert self._default_library.short_name == library.get("short_name")

    def _common_basic_auth_arguments(self):
        """We're not really testing these arguments, but a value for them
        is required for all Basic Auth type integrations.
        """
        B = BasicAuthenticationProvider
        return [
            (B.TEST_IDENTIFIER, "user"),
            (B.TEST_PASSWORD, "pass"),
            (B.IDENTIFIER_KEYBOARD, B.DEFAULT_KEYBOARD),
            (B.PASSWORD_KEYBOARD, B.DEFAULT_KEYBOARD),
            (B.IDENTIFIER_BARCODE_FORMAT, B.BARCODE_FORMAT_CODABAR),
        ]

    def test_patron_auth_services_post_errors(self):
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", "Unknown"),
            ])
            response = self.manager.admin_patron_auth_services_controller.process_patron_auth_services()
            assert response == UNKNOWN_PROTOCOL

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([])
            response = self.manager.admin_patron_auth_services_controller.process_patron_auth_services()
            assert response == NO_PROTOCOL_FOR_NEW_SERVICE

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", "123"),
            ])
            response = self.manager.admin_patron_auth_services_controller.process_patron_auth_services()
            assert response == MISSING_SERVICE

        auth_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=SimpleAuthenticationProvider.__module__,
            goal=ExternalIntegration.PATRON_AUTH_GOAL,
            name="name",
        )

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", auth_service.id),
                ("protocol", SIP2AuthenticationProvider.__module__),
            ])
            response = self.manager.admin_patron_auth_services_controller.process_patron_auth_services()
            assert response == CANNOT_CHANGE_PROTOCOL

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", auth_service.name),
                ("protocol", SIP2AuthenticationProvider.__module__),
            ])
            response = self.manager.admin_patron_auth_services_controller.process_patron_auth_services()
            assert response == INTEGRATION_NAME_ALREADY_IN_USE

        auth_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=MilleniumPatronAPI.__module__,
            goal=ExternalIntegration.PATRON_AUTH_GOAL
        )

        common_args = self._common_basic_auth_arguments()
        with self.request_context_with_admin("/", method="POST"):
            M = MilleniumPatronAPI
            flask.request.form = MultiDict([
                ("name", "some auth name"),
                ("id", auth_service.id),
                ("protocol", MilleniumPatronAPI.__module__),
                (ExternalIntegration.URL, "http://url"),
                (M.AUTHENTICATION_MODE, "Invalid mode"),
                (M.VERIFY_CERTIFICATE, "true"),
            ] + common_args)
            response = self.manager.admin_patron_auth_services_controller.process_patron_auth_services()
            assert response.uri == INVALID_CONFIGURATION_OPTION.uri

        auth_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=SimpleAuthenticationProvider.__module__,
            goal=ExternalIntegration.PATRON_AUTH_GOAL
        )

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", auth_service.id),
                ("protocol", SimpleAuthenticationProvider.__module__),
            ])
            response = self.manager.admin_patron_auth_services_controller.process_patron_auth_services()
            assert response.uri == INCOMPLETE_CONFIGURATION.uri

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", SimpleAuthenticationProvider.__module__),
                ("libraries", json.dumps([{ "short_name": "not-a-library" }])),
            ] + common_args)
            response = self.manager.admin_patron_auth_services_controller.process_patron_auth_services()
            assert response.uri == NO_SUCH_LIBRARY.uri

        library, ignore = create(
            self._db, Library, name="Library", short_name="L",
        )
        auth_service.libraries += [library]

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", SimpleAuthenticationProvider.__module__),
                ("libraries", json.dumps([{
                    "short_name": library.short_name,
                    AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE: AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_NONE,
                    AuthenticationProvider.LIBRARY_IDENTIFIER_FIELD: AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_BARCODE,
                }])),
            ] + common_args)
            response = self.manager.admin_patron_auth_services_controller.process_patron_auth_services()
            assert response.uri == MULTIPLE_BASIC_AUTH_SERVICES.uri

        self._db.delete(auth_service)
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", SimpleAuthenticationProvider.__module__),
                ("libraries", json.dumps([{
                    "short_name": library.short_name,
                    AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE: AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_NONE,
                    AuthenticationProvider.LIBRARY_IDENTIFIER_FIELD: AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_BARCODE,
                    AuthenticationProvider.EXTERNAL_TYPE_REGULAR_EXPRESSION: "(invalid re",
                }])),
            ] + common_args)
            response = self.manager.admin_patron_auth_services_controller.process_patron_auth_services()
            assert response == INVALID_EXTERNAL_TYPE_REGULAR_EXPRESSION

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", SimpleAuthenticationProvider.__module__),
                ("libraries", json.dumps([{
                    "short_name": library.short_name,
                    AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE: AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_REGEX,
                    AuthenticationProvider.LIBRARY_IDENTIFIER_FIELD: AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_BARCODE,
                    AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION: "(invalid re",
                }])),
            ] + common_args)
            response = self.manager.admin_patron_auth_services_controller.process_patron_auth_services()
            assert response == INVALID_LIBRARY_IDENTIFIER_RESTRICTION_REGULAR_EXPRESSION

        self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        self._db.flush()
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", SimpleAuthenticationProvider.__module__),
            ] + self._common_basic_auth_arguments())
            pytest.raises(AdminNotAuthorized,
                          self.manager.admin_patron_auth_services_controller.process_patron_auth_services)

    def _get_mock(self):
        manager = self.manager

        class Mock(PatronAuthServicesController):
            def __init__(self, manager):
                self.validate_formats_call_count = 0
                super(Mock, self).__init__(manager)

            def validate_formats(self, settings=None, validator=None):
                self.validate_formats_call_count += 1
                super(Mock, self).validate_formats()

        self.manager.admin_patron_auth_services_controller = Mock(manager)
        return self.manager.admin_patron_auth_services_controller

    def test_patron_auth_services_post_create(self):
        mock_controller = self._get_mock()

        library, ignore = create(
            self._db, Library, name="Library", short_name="L",
        )

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", SimpleAuthenticationProvider.__module__),
                ("libraries", json.dumps([{
                    "short_name": library.short_name,
                    AuthenticationProvider.EXTERNAL_TYPE_REGULAR_EXPRESSION: "^(.)",
                    AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE: AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_REGEX,
                    AuthenticationProvider.LIBRARY_IDENTIFIER_FIELD: AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_BARCODE,
                    AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION: "^1234",
                }])),
            ] + self._common_basic_auth_arguments())

            response = mock_controller.process_patron_auth_services()
            assert response.status_code == 201
            assert mock_controller.validate_formats_call_count == 1

        auth_service = get_one(self._db, ExternalIntegration, goal=ExternalIntegration.PATRON_AUTH_GOAL)
        assert auth_service.id == int(response.response[0])
        assert SimpleAuthenticationProvider.__module__ == auth_service.protocol
        assert "user" == auth_service.setting(BasicAuthenticationProvider.TEST_IDENTIFIER).value
        assert "pass" == auth_service.setting(BasicAuthenticationProvider.TEST_PASSWORD).value
        assert [library] == auth_service.libraries
        assert "^(.)" == ConfigurationSetting.for_library_and_externalintegration(
                self._db, AuthenticationProvider.EXTERNAL_TYPE_REGULAR_EXPRESSION,
                library, auth_service).value
        common_args = self._common_basic_auth_arguments()
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", MilleniumPatronAPI.__module__),
                (ExternalIntegration.URL, "url"),
                (MilleniumPatronAPI.VERIFY_CERTIFICATE, "true"),
                (MilleniumPatronAPI.AUTHENTICATION_MODE, MilleniumPatronAPI.PIN_AUTHENTICATION_MODE),
            ] + common_args)
            response = mock_controller.process_patron_auth_services()
            assert response.status_code == 201
            assert mock_controller.validate_formats_call_count == 2

        auth_service2 = get_one(self._db, ExternalIntegration,
                               goal=ExternalIntegration.PATRON_AUTH_GOAL,
                               protocol=MilleniumPatronAPI.__module__)
        assert auth_service2 != auth_service
        assert auth_service2.id == int(response.response[0])
        assert "url" == auth_service2.url
        assert "user" == auth_service2.setting(BasicAuthenticationProvider.TEST_IDENTIFIER).value
        assert "pass" == auth_service2.setting(BasicAuthenticationProvider.TEST_PASSWORD).value
        assert ("true" ==
            auth_service2.setting(MilleniumPatronAPI.VERIFY_CERTIFICATE).value)
        assert (MilleniumPatronAPI.PIN_AUTHENTICATION_MODE ==
            auth_service2.setting(MilleniumPatronAPI.AUTHENTICATION_MODE).value)
        assert None == auth_service2.setting(MilleniumPatronAPI.BLOCK_TYPES).value
        assert [] == auth_service2.libraries

    def test_patron_auth_services_post_edit(self):
        mock_controller = self._get_mock()

        l1, ignore = create(
            self._db, Library, name="Library 1", short_name="L1",
        )
        l2, ignore = create(
            self._db, Library, name="Library 2", short_name="L2",
        )

        auth_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=SimpleAuthenticationProvider.__module__,
            goal=ExternalIntegration.PATRON_AUTH_GOAL
        )
        auth_service.setting(BasicAuthenticationProvider.TEST_IDENTIFIER).value = "old_user"
        auth_service.setting(BasicAuthenticationProvider.TEST_PASSWORD).value = "old_password"
        auth_service.libraries = [l1]

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", auth_service.id),
                ("protocol", SimpleAuthenticationProvider.__module__),
                ("libraries", json.dumps([{
                    "short_name": l2.short_name,
                    AuthenticationProvider.EXTERNAL_TYPE_REGULAR_EXPRESSION: "^(.)",
                    AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE: AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_NONE,
                    AuthenticationProvider.LIBRARY_IDENTIFIER_FIELD: AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_BARCODE,
                }])),
            ] + self._common_basic_auth_arguments())
            response = self.manager.admin_patron_auth_services_controller.process_patron_auth_services()
            assert response.status_code == 200
            assert mock_controller.validate_formats_call_count == 1

        assert auth_service.id == int(response.response[0])
        assert SimpleAuthenticationProvider.__module__ == auth_service.protocol
        assert "user" == auth_service.setting(BasicAuthenticationProvider.TEST_IDENTIFIER).value
        assert "pass" == auth_service.setting(BasicAuthenticationProvider.TEST_PASSWORD).value
        assert [l2] == auth_service.libraries
        assert "^(.)" == ConfigurationSetting.for_library_and_externalintegration(
                self._db, AuthenticationProvider.EXTERNAL_TYPE_REGULAR_EXPRESSION,
                l2, auth_service).value

    def test_patron_auth_service_delete(self):
        l1, ignore = create(
            self._db, Library, name="Library 1", short_name="L1",
        )
        auth_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=SimpleAuthenticationProvider.__module__,
            goal=ExternalIntegration.PATRON_AUTH_GOAL
        )
        auth_service.setting(BasicAuthenticationProvider.TEST_IDENTIFIER).value = "old_user"
        auth_service.setting(BasicAuthenticationProvider.TEST_PASSWORD).value = "old_password"
        auth_service.libraries = [l1]

        with self.request_context_with_admin("/", method="DELETE"):
            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            pytest.raises(AdminNotAuthorized,
                          self.manager.admin_patron_auth_services_controller.process_delete,
                          auth_service.id)

            self.admin.add_role(AdminRole.SYSTEM_ADMIN)
            response = self.manager.admin_patron_auth_services_controller.process_delete(auth_service.id)
            assert response.status_code == 200

        service = get_one(self._db, ExternalIntegration, id=auth_service.id)
        assert None == service
