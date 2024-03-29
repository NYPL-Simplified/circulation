import pytest

import flask
from werkzeug.datastructures import MultiDict
from api.admin.exceptions import *
# from api.admin.problem_details import *
from core.model import (
    AdminRole,
    create,
    ExternalIntegration,
    get_one,
)
from .test_controller import SettingsControllerTest

class TestDiscoveryServices(SettingsControllerTest):

    """Test the controller functions that list and create new discovery
    services.
    """

    def test_discovery_services_get_with_no_services_creates_default(self):
        with self.request_context_with_admin("/"):
            response = self.manager.admin_discovery_services_controller.process_discovery_services()
            [service] = response.get("discovery_services")
            protocols = response.get("protocols")
            assert ExternalIntegration.OPDS_REGISTRATION in [p.get("name") for p in protocols]
            assert "settings" in protocols[0]
            assert ExternalIntegration.OPDS_REGISTRATION == service.get("protocol")
            assert "https://libraryregistry.librarysimplified.org/" == service.get("settings").get(ExternalIntegration.URL)

            # Only system admins can see the discovery services.
            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            self._db.flush()
            pytest.raises(AdminNotAuthorized,
                          self.manager.admin_discovery_services_controller.process_discovery_services)

    def test_discovery_services_get_with_one_service(self):
        discovery_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.OPDS_REGISTRATION,
            goal=ExternalIntegration.DISCOVERY_GOAL,
        )
        discovery_service.url = self._str

        controller = self.manager.admin_discovery_services_controller

        with self.request_context_with_admin("/"):
            response = controller.process_discovery_services()
            [service] = response.get("discovery_services")

            assert discovery_service.id == service.get("id")
            assert discovery_service.protocol == service.get("protocol")
            assert discovery_service.url == service.get("settings").get(ExternalIntegration.URL)

    def test_discovery_services_post_errors(self):
        controller = self.manager.admin_discovery_services_controller
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Name"),
                ("protocol", "Unknown"),
            ])
            response = controller.process_discovery_services()
            assert response == UNKNOWN_PROTOCOL

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Name"),
            ])
            response = controller.process_discovery_services()
            assert response == NO_PROTOCOL_FOR_NEW_SERVICE

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Name"),
                ("id", "123"),
                ("protocol", ExternalIntegration.OPDS_REGISTRATION),
            ])
            response = controller.process_discovery_services()
            assert response == MISSING_SERVICE

        service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.OPDS_REGISTRATION,
            goal=ExternalIntegration.DISCOVERY_GOAL,
            name="name",
        )

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", service.name),
                ("protocol", ExternalIntegration.OPDS_REGISTRATION),
            ])
            response = controller.process_discovery_services()
            assert response == INTEGRATION_NAME_ALREADY_IN_USE

        existing_integration = self._external_integration(
            ExternalIntegration.OPDS_REGISTRATION,
            ExternalIntegration.DISCOVERY_GOAL,
            url=self._url
        )
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "new name"),
                ("protocol", existing_integration.protocol),
                ("url", existing_integration.url)
            ])
            response = controller.process_discovery_services()
            assert response == INTEGRATION_URL_ALREADY_IN_USE

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", service.id),
                ("protocol", ExternalIntegration.OPDS_REGISTRATION),
            ])
            response = controller.process_discovery_services()
            assert response.uri == INCOMPLETE_CONFIGURATION.uri

        self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", ExternalIntegration.OPDS_REGISTRATION),
                (ExternalIntegration.URL, "registry url"),
            ])
            pytest.raises(AdminNotAuthorized,
                          controller.process_discovery_services)

    def test_discovery_services_post_create(self):
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Name"),
                ("protocol", ExternalIntegration.OPDS_REGISTRATION),
                (ExternalIntegration.URL, "http://registry_url"),
            ])
            response = self.manager.admin_discovery_services_controller.process_discovery_services()
            assert response.status_code == 201

        service = get_one(self._db, ExternalIntegration, goal=ExternalIntegration.DISCOVERY_GOAL)
        assert service.id == int(response.response[0])
        assert ExternalIntegration.OPDS_REGISTRATION == service.protocol
        assert "http://registry_url" == service.url

    def test_discovery_services_post_edit(self):
        discovery_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.OPDS_REGISTRATION,
            goal=ExternalIntegration.DISCOVERY_GOAL,
        )
        discovery_service.url = "registry url"

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Name"),
                ("id", discovery_service.id),
                ("protocol", ExternalIntegration.OPDS_REGISTRATION),
                (ExternalIntegration.URL, "http://new_registry_url"),
            ])
            response = self.manager.admin_discovery_services_controller.process_discovery_services()
            assert response.status_code == 200

        assert discovery_service.id == int(response.response[0])
        assert ExternalIntegration.OPDS_REGISTRATION == discovery_service.protocol
        assert "http://new_registry_url" == discovery_service.url

    def test_check_name_unique(self):
       kwargs = dict(protocol=ExternalIntegration.OPDS_REGISTRATION,
                     goal=ExternalIntegration.DISCOVERY_GOAL,)

       existing_service, ignore = create(self._db, ExternalIntegration, name="existing service", **kwargs)
       new_service, ignore = create(self._db, ExternalIntegration, name="new service", **kwargs)

       m = self.manager.admin_discovery_services_controller.check_name_unique

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

    def test_discovery_service_delete(self):
        discovery_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.OPDS_REGISTRATION,
            goal=ExternalIntegration.DISCOVERY_GOAL,
        )
        discovery_service.url = "registry url"

        with self.request_context_with_admin("/", method="DELETE"):
            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            pytest.raises(AdminNotAuthorized,
                          self.manager.admin_discovery_services_controller.process_delete,
                          discovery_service.id)

            self.admin.add_role(AdminRole.SYSTEM_ADMIN)
            response = self.manager.admin_discovery_services_controller.process_delete(discovery_service.id)
            assert response.status_code == 200

        service = get_one(self._db, ExternalIntegration, id=discovery_service.id)
        assert None == service
