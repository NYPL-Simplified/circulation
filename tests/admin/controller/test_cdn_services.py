from nose.tools import (
    set_trace,
    eq_,
    assert_raises
)
import flask
from werkzeug.datastructures import MultiDict
from api.admin.exceptions import *
from core.model import (
    AdminRole,
    Configuration,
    create,
    ExternalIntegration,
    get_one,
)
from test_controller import SettingsControllerTest

class TestCDNServices(SettingsControllerTest):
    def test_cdn_services_get_with_no_services(self):
        with self.request_context_with_admin("/"):
            response = self.manager.admin_cdn_services_controller.process_cdn_services()
            assert response.get("cdn_services") == []
            protocols = response.get("protocols")
            assert ExternalIntegration.CDN in [p.get("name") for p in protocols]
            assert "settings" in protocols[0]

            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            self._db.flush()
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_cdn_services_controller.process_cdn_services)

    def test_cdn_services_get_with_one_service(self):
        cdn_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.CDN,
            goal=ExternalIntegration.CDN_GOAL,
        )
        cdn_service.url = "cdn url"
        cdn_service.setting(Configuration.CDN_MIRRORED_DOMAIN_KEY).value = "mirrored domain"

        with self.request_context_with_admin("/"):
            response = self.manager.admin_cdn_services_controller.process_cdn_services()
            [service] = response.get("cdn_services")

            assert cdn_service.id == service.get("id")
            assert cdn_service.protocol == service.get("protocol")
            assert "cdn url" == service.get("settings").get(ExternalIntegration.URL)
            assert "mirrored domain" == service.get("settings").get(Configuration.CDN_MIRRORED_DOMAIN_KEY)

    def test_cdn_services_post_errors(self):
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([])
            response = self.manager.admin_cdn_services_controller.process_cdn_services()
            assert response == INCOMPLETE_CONFIGURATION

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Name"),
                ("protocol", "Unknown"),
            ])
            response = self.manager.admin_cdn_services_controller.process_cdn_services()
            assert response == UNKNOWN_PROTOCOL

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Name"),
            ])
            response = self.manager.admin_cdn_services_controller.process_cdn_services()
            assert response == NO_PROTOCOL_FOR_NEW_SERVICE

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Name"),
                ("id", "123"),
            ])
            response = self.manager.admin_cdn_services_controller.process_cdn_services()
            assert response == MISSING_SERVICE

        service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.CDN,
            goal=ExternalIntegration.CDN_GOAL,
            name="name",
        )

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", service.name),
                ("protocol", ExternalIntegration.CDN),
            ])
            response = self.manager.admin_cdn_services_controller.process_cdn_services()
            assert response == INTEGRATION_NAME_ALREADY_IN_USE

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Name"),
                ("id", service.id),
                ("protocol", ExternalIntegration.CDN),
            ])
            response = self.manager.admin_cdn_services_controller.process_cdn_services()
            assert response.uri == INCOMPLETE_CONFIGURATION.uri

        self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Name"),
                ("protocol", ExternalIntegration.CDN),
                (ExternalIntegration.URL, "cdn url"),
                (Configuration.CDN_MIRRORED_DOMAIN_KEY, "mirrored domain"),
            ])
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_cdn_services_controller.process_cdn_services)

    def test_cdn_services_post_create(self):
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Name"),
                ("protocol", ExternalIntegration.CDN),
                (ExternalIntegration.URL, "http://cdn_url"),
                (Configuration.CDN_MIRRORED_DOMAIN_KEY, "mirrored domain"),
            ])
            response = self.manager.admin_cdn_services_controller.process_cdn_services()
            assert response.status_code == 201

        service = get_one(self._db, ExternalIntegration, goal=ExternalIntegration.CDN_GOAL)
        assert service.id == int(response.response[0])
        assert ExternalIntegration.CDN == service.protocol
        assert "http://cdn_url" == service.url
        assert "mirrored domain" == service.setting(Configuration.CDN_MIRRORED_DOMAIN_KEY).value

    def test_cdn_services_post_edit(self):
        cdn_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.CDN,
            goal=ExternalIntegration.CDN_GOAL,
        )
        cdn_service.url = "cdn url"
        cdn_service.setting(Configuration.CDN_MIRRORED_DOMAIN_KEY).value = "mirrored domain"

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Name"),
                ("id", cdn_service.id),
                ("protocol", ExternalIntegration.CDN),
                (ExternalIntegration.URL, "http://new_cdn_url"),
                (Configuration.CDN_MIRRORED_DOMAIN_KEY, "new mirrored domain")
            ])
            response = self.manager.admin_cdn_services_controller.process_cdn_services()
            assert response.status_code == 200

        assert cdn_service.id == int(response.response[0])
        assert ExternalIntegration.CDN == cdn_service.protocol
        assert "http://new_cdn_url" == cdn_service.url
        assert "new mirrored domain" == cdn_service.setting(Configuration.CDN_MIRRORED_DOMAIN_KEY).value

    def test_check_name_unique(self):
       kwargs = dict(protocol=ExternalIntegration.CDN,
                    goal=ExternalIntegration.CDN_GOAL)

       existing_service, ignore = create(self._db, ExternalIntegration, name="existing service", **kwargs)
       new_service, ignore = create(self._db, ExternalIntegration, name="new service", **kwargs)

       m = self.manager.admin_cdn_services_controller.check_name_unique

       # Try to change new service so that it has the same name as existing service
       # -- this is not allowed.
       result = m(new_service, existing_service.name)
       assert result == INTEGRATION_NAME_ALREADY_IN_USE

       # Try to edit existing service without changing its name -- this is fine.
       assert (
           None ==
           m(existing_service, existing_service.name))

       # Changing the existing service's name is also fine.
       assert (
            None ==
            m(existing_service, "new name"))

    def test_cdn_service_delete(self):
        cdn_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.CDN,
            goal=ExternalIntegration.CDN_GOAL,
        )
        cdn_service.url = "cdn url"
        cdn_service.setting(Configuration.CDN_MIRRORED_DOMAIN_KEY).value = "mirrored domain"

        with self.request_context_with_admin("/", method="DELETE"):
            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_cdn_services_controller.process_delete,
                          cdn_service.id)

            self.admin.add_role(AdminRole.SYSTEM_ADMIN)
            response = self.manager.admin_cdn_services_controller.process_delete(cdn_service.id)
            assert response.status_code == 200

        service = get_one(self._db, ExternalIntegration, id=cdn_service.id)
        assert None == service
