import pytest

import flask
from werkzeug.datastructures import MultiDict
from api.admin.exceptions import *
from core.external_search import ExternalSearchIndex
from core.model import (
    AdminRole,
    create,
    get_one,
    ExternalIntegration,
)
from .test_controller import SettingsControllerTest

class TestSearchServices(SettingsControllerTest):
    def test_search_services_get_with_no_services(self):
        with self.request_context_with_admin("/"):
            response = self.manager.admin_search_services_controller.process_services()
            assert response.get("search_services") == []
            protocols = response.get("protocols")
            assert ExternalIntegration.ELASTICSEARCH in [p.get("name") for p in protocols]
            assert "settings" in protocols[0]

            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            self._db.flush()
            pytest.raises(AdminNotAuthorized,
                          self.manager.admin_search_services_controller.process_services)

    def test_search_services_get_with_one_service(self):
        search_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.ELASTICSEARCH,
            goal=ExternalIntegration.SEARCH_GOAL,
        )
        search_service.url = "search url"
        search_service.setting(ExternalSearchIndex.WORKS_INDEX_PREFIX_KEY).value = "works-index-prefix"
        search_service.setting(ExternalSearchIndex.TEST_SEARCH_TERM_KEY).value = "search-term-for-self-tests"

        with self.request_context_with_admin("/"):
            response = self.manager.admin_search_services_controller.process_services()
            [service] = response.get("search_services")

            assert search_service.id == service.get("id")
            assert search_service.protocol == service.get("protocol")
            assert "search url" == service.get("settings").get(ExternalIntegration.URL)
            assert "works-index-prefix" == service.get("settings").get(ExternalSearchIndex.WORKS_INDEX_PREFIX_KEY)
            assert "search-term-for-self-tests" == service.get("settings").get(ExternalSearchIndex.TEST_SEARCH_TERM_KEY)

    def test_search_services_post_errors(self):
        controller = self.manager.admin_search_services_controller

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Name"),
                ("protocol", "Unknown"),
            ])
            response = controller.process_services()
            assert response == UNKNOWN_PROTOCOL

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([("name", "Name")])
            response = controller.process_services()
            assert response == NO_PROTOCOL_FOR_NEW_SERVICE

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Name"),
                ("id", "123"),
            ])
            response = controller.process_services()
            assert response == MISSING_SERVICE

        service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.ELASTICSEARCH,
            goal=ExternalIntegration.SEARCH_GOAL,
        )

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Name"),
                ("protocol", ExternalIntegration.ELASTICSEARCH),
            ])
            response = controller.process_services()
            assert response.uri == MULTIPLE_SITEWIDE_SERVICES.uri

        self._db.delete(service)
        service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.CDN,
            goal=ExternalIntegration.CDN_GOAL,
            name="name",
        )

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", service.name),
                ("protocol", ExternalIntegration.ELASTICSEARCH),
            ])
            response = controller.process_services()
            assert response == INTEGRATION_NAME_ALREADY_IN_USE

        service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.ELASTICSEARCH,
            goal=ExternalIntegration.SEARCH_GOAL,
        )

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Name"),
                ("id", service.id),
                ("protocol", ExternalIntegration.ELASTICSEARCH),
            ])
            response = controller.process_services()
            assert response.uri == INCOMPLETE_CONFIGURATION.uri

        self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", ExternalIntegration.ELASTICSEARCH),
                (ExternalIntegration.URL, "search url"),
                (ExternalSearchIndex.WORKS_INDEX_PREFIX_KEY, "works-index-prefix"),
            ])
            pytest.raises(AdminNotAuthorized,
                         controller.process_services)

    def test_search_services_post_create(self):
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Name"),
                ("protocol", ExternalIntegration.ELASTICSEARCH),
                (ExternalIntegration.URL, "http://search_url"),
                (ExternalSearchIndex.WORKS_INDEX_PREFIX_KEY, "works-index-prefix"),
                (ExternalSearchIndex.TEST_SEARCH_TERM_KEY, "sample-search-term")
            ])
            response = self.manager.admin_search_services_controller.process_services()
            assert response.status_code == 201

        service = get_one(self._db, ExternalIntegration, goal=ExternalIntegration.SEARCH_GOAL)
        assert service.id == int(response.response[0])
        assert ExternalIntegration.ELASTICSEARCH == service.protocol
        assert "http://search_url" == service.url
        assert "works-index-prefix" == service.setting(ExternalSearchIndex.WORKS_INDEX_PREFIX_KEY).value
        assert "sample-search-term" == service.setting(ExternalSearchIndex.TEST_SEARCH_TERM_KEY).value

    def test_search_services_post_edit(self):
        search_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.ELASTICSEARCH,
            goal=ExternalIntegration.SEARCH_GOAL,
        )
        search_service.url = "search url"
        search_service.setting(ExternalSearchIndex.WORKS_INDEX_PREFIX_KEY).value = "works-index-prefix"
        search_service.setting(ExternalSearchIndex.TEST_SEARCH_TERM_KEY).value = "sample-search-term"

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Name"),
                ("id", search_service.id),
                ("protocol", ExternalIntegration.ELASTICSEARCH),
                (ExternalIntegration.URL, "http://new_search_url"),
                (ExternalSearchIndex.WORKS_INDEX_PREFIX_KEY, "new-works-index-prefix"),
                (ExternalSearchIndex.TEST_SEARCH_TERM_KEY, "new-sample-search-term")
            ])
            response = self.manager.admin_search_services_controller.process_services()
            assert response.status_code == 200

        assert search_service.id == int(response.response[0])
        assert ExternalIntegration.ELASTICSEARCH == search_service.protocol
        assert "http://new_search_url" == search_service.url
        assert "new-works-index-prefix" == search_service.setting(ExternalSearchIndex.WORKS_INDEX_PREFIX_KEY).value
        assert "new-sample-search-term" == search_service.setting(ExternalSearchIndex.TEST_SEARCH_TERM_KEY).value

    def test_search_service_delete(self):
        search_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.ELASTICSEARCH,
            goal=ExternalIntegration.SEARCH_GOAL,
        )
        search_service.url = "search url"
        search_service.setting(ExternalSearchIndex.WORKS_INDEX_PREFIX_KEY).value = "works-index-prefix"

        with self.request_context_with_admin("/", method="DELETE"):
            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            pytest.raises(AdminNotAuthorized,
                          self.manager.admin_search_services_controller.process_delete,
                          search_service.id)

            self.admin.add_role(AdminRole.SYSTEM_ADMIN)
            response = self.manager.admin_search_services_controller.process_delete(search_service.id)
            assert response.status_code == 200

        service = get_one(self._db, ExternalIntegration, id=search_service.id)
        assert None == service
