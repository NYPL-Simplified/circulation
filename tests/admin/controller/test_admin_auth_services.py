import pytest

import flask
import json
from werkzeug.datastructures import MultiDict
from api.admin.exceptions import *
from api.app import initialize_database
from core.model import (
    AdminRole,
    ConfigurationSetting,
    create,
    ExternalIntegration,
    get_one,
)
from .test_controller import SettingsControllerTest


class TestAdminAuthServices(SettingsControllerTest):
    @classmethod
    def setup_class(cls):
        super(TestAdminAuthServices, cls).setup_class()

        initialize_database(autoinitialize=False)

    def test_admin_auth_services_get_with_no_services(self):
        with self.request_context_with_admin("/"):
            response = self.manager.admin_auth_services_controller.process_admin_auth_services()
            assert response.get("admin_auth_services") == []

            # All the protocols in ExternalIntegration.ADMIN_AUTH_PROTOCOLS
            # are supported by the admin interface.
            assert (sorted([p.get("name") for p in response.get("protocols")]) ==
                sorted(ExternalIntegration.ADMIN_AUTH_PROTOCOLS))

            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            self._db.flush()
            pytest.raises(AdminNotAuthorized,
                          self.manager.admin_auth_services_controller.process_admin_auth_services)

    def test_admin_auth_services_get_with_google_oauth_service(self):
        auth_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.GOOGLE_OAUTH,
            goal=ExternalIntegration.ADMIN_AUTH_GOAL
        )
        auth_service.url = "http://oauth.test"
        auth_service.username = "user"
        auth_service.password = "pass"
        auth_service.libraries += [self._default_library]
        ConfigurationSetting.for_library_and_externalintegration(
            self._db, "domains", self._default_library, auth_service
        ).value = json.dumps(["nypl.org"])

        with self.request_context_with_admin("/"):
            response = self.manager.admin_auth_services_controller.process_admin_auth_services()
            [service] = response.get("admin_auth_services")

            assert auth_service.id == service.get("id")
            assert auth_service.name == service.get("name")
            assert auth_service.protocol == service.get("protocol")
            assert auth_service.url == service.get("settings").get("url")
            assert auth_service.username == service.get("settings").get("username")
            assert auth_service.password == service.get("settings").get("password")
            [library_info] = service.get("libraries")
            assert self._default_library.short_name == library_info.get("short_name")
            assert ["nypl.org"] == library_info.get("domains")

    def test_admin_auth_services_post_errors(self):
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", "Unknown"),
            ])
            response = self.manager.admin_auth_services_controller.process_admin_auth_services()
            assert response == UNKNOWN_PROTOCOL

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([])
            response = self.manager.admin_auth_services_controller.process_admin_auth_services()
            assert response == NO_PROTOCOL_FOR_NEW_SERVICE

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", "1234"),
            ])
            response = self.manager.admin_auth_services_controller.process_admin_auth_services()
            assert response == MISSING_SERVICE

        auth_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.GOOGLE_OAUTH,
            goal=ExternalIntegration.ADMIN_AUTH_GOAL
        )

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", str(auth_service.id)),
            ])
            response = self.manager.admin_auth_services_controller.process_admin_auth_services()
            assert response == CANNOT_CHANGE_PROTOCOL

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", "Google OAuth"),
            ])
            response = self.manager.admin_auth_services_controller.process_admin_auth_services()
            assert response.uri == INCOMPLETE_CONFIGURATION.uri

        self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        self._db.flush()
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "oauth"),
                ("protocol", "Google OAuth"),
                ("url", "url"),
                ("username", "username"),
                ("password", "password"),
                ("domains", "nypl.org"),
            ])
            pytest.raises(AdminNotAuthorized,
                          self.manager.admin_auth_services_controller.process_admin_auth_services)

    def test_admin_auth_services_post_create(self):
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "oauth"),
                ("protocol", "Google OAuth"),
                ("url", "http://url2"),
                ("username", "username"),
                ("password", "password"),
                ("libraries", json.dumps([{ "short_name": self._default_library.short_name,
                                            "domains": ["nypl.org", "gmail.com"] }])),
            ])
            response = self.manager.admin_auth_services_controller.process_admin_auth_services()
            assert response.status_code == 201

        # The auth service was created and configured properly.
        auth_service = ExternalIntegration.admin_authentication(self._db)
        assert auth_service.protocol == response.get_data(as_text=True)
        assert "oauth" == auth_service.name
        assert "http://url2" == auth_service.url
        assert "username" == auth_service.username
        assert "password" == auth_service.password

        assert [self._default_library] == auth_service.libraries
        setting = ConfigurationSetting.for_library_and_externalintegration(
            self._db, "domains", self._default_library, auth_service
        )
        assert "domains" == setting.key
        assert ["nypl.org", "gmail.com"] == json.loads(setting.value)

    def test_admin_auth_services_post_google_oauth_edit(self):
        # The auth service exists.
        auth_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.GOOGLE_OAUTH,
            goal=ExternalIntegration.ADMIN_AUTH_GOAL
        )
        auth_service.url = "url"
        auth_service.username = "user"
        auth_service.password = "pass"
        auth_service.libraries += [self._default_library]
        setting = ConfigurationSetting.for_library_and_externalintegration(
            self._db, "domains", self._default_library, auth_service)
        setting.value = json.dumps(["library1.org"])

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "oauth"),
                ("protocol", "Google OAuth"),
                ("url", "http://url2"),
                ("username", "user2"),
                ("password", "pass2"),
                ("libraries", json.dumps([{ "short_name": self._default_library.short_name,
                                            "domains": ["library2.org"] }])),
            ])
            response = self.manager.admin_auth_services_controller.process_admin_auth_services()
            assert response.status_code == 200

        assert auth_service.protocol == response.get_data(as_text=True)
        assert "oauth" == auth_service.name
        assert "http://url2" == auth_service.url
        assert "user2" == auth_service.username
        assert "domains" == setting.key
        assert ["library2.org"] == json.loads(setting.value)

    def test_admin_auth_service_delete(self):
        auth_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.GOOGLE_OAUTH,
            goal=ExternalIntegration.ADMIN_AUTH_GOAL
        )
        auth_service.url = "url"
        auth_service.username = "user"
        auth_service.password = "pass"
        auth_service.set_setting("domains", json.dumps(["library1.org"]))

        with self.request_context_with_admin("/", method="DELETE"):
            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            pytest.raises(AdminNotAuthorized,
                          self.manager.admin_auth_services_controller.process_delete,
                          auth_service.protocol)

            self.admin.add_role(AdminRole.SYSTEM_ADMIN)
            response = self.manager.admin_auth_services_controller.process_delete(auth_service.protocol)
            assert response.status_code == 200

        service = get_one(self._db, ExternalIntegration, id=auth_service.id)
        assert None == service
