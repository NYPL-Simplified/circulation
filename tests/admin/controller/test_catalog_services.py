import json

import flask
import pytest
from werkzeug.datastructures import MultiDict

from api.admin.exceptions import *
from core.marc import MARCExporter
from core.model import (
    AdminRole,
    ConfigurationSetting,
    ExternalIntegration,
    create,
    get_one,
)
from core.model.configuration import ExternalIntegrationLink
from core.s3 import S3Uploader, S3UploaderConfiguration
from test_controller import SettingsControllerTest


class TestCatalogServicesController(SettingsControllerTest):

    def test_catalog_services_get_with_no_services(self):
        with self.request_context_with_admin("/"):
            response = self.manager.admin_catalog_services_controller.process_catalog_services()
            assert response.get("catalog_services") == []
            protocols = response.get("protocols")
            assert 1 == len(protocols)
            assert MARCExporter.NAME == protocols[0].get("name")
            assert "settings" in protocols[0]
            assert "library_settings" in protocols[0]

            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            self._db.flush()
            pytest.raises(AdminNotAuthorized,
                          self.manager.admin_catalog_services_controller.process_catalog_services)

        def test_catalog_services_get_with_marc_exporter(self):
            integration, ignore = create(
                self._db, ExternalIntegration,
                protocol=ExternalIntegration.MARC_EXPORT,
                goal=ExternalIntegration.CATALOG_GOAL,
                name="name",
            )
            integration.libraries += [self._default_library]
            ConfigurationSetting.for_library_and_externalintegration(
                self._db, MARCExporter.MARC_ORGANIZATION_CODE,
                self._default_library, integration).value = "US-MaBoDPL"
            ConfigurationSetting.for_library_and_externalintegration(
                self._db, MARCExporter.INCLUDE_SUMMARY,
                self._default_library, integration).value = "false"
            ConfigurationSetting.for_library_and_externalintegration(
                self._db, MARCExporter.INCLUDE_SIMPLIFIED_GENRES,
                self._default_library, integration).value = "true"

            with self.request_context_with_admin("/"):
                response = self.manager.admin_catalog_services_controller.process_catalog_services()
                [service] = response.get("catalog_services")
                assert integration.id == service.get("id")
                assert integration.name == service.get("name")
                assert integration.protocol == service.get("protocol")
                [library] = service.get("libraries")
                assert self._default_library.short_name == library.get("short_name")
                assert "US-MaBoDPL" == library.get(MARCExporter.MARC_ORGANIZATION_CODE)
                assert "false" == library.get(MARCExporter.INCLUDE_SUMMARY)
                assert "true" == library.get(MARCExporter.INCLUDE_SIMPLIFIED_GENRES)


    def test_catalog_services_post_errors(self):
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", "Unknown"),
            ])
            response = self.manager.admin_catalog_services_controller.process_catalog_services()
            assert response == UNKNOWN_PROTOCOL

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", "123"),
            ])
            response = self.manager.admin_catalog_services_controller.process_catalog_services()
            assert response == MISSING_SERVICE

        service, ignore = create(
            self._db, ExternalIntegration,
            protocol="fake protocol",
            goal=ExternalIntegration.CATALOG_GOAL,
            name="name",
        )

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", service.id),
                ("protocol", ExternalIntegration.MARC_EXPORT),
            ])
            response = self.manager.admin_catalog_services_controller.process_catalog_services()
            assert response == CANNOT_CHANGE_PROTOCOL

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", service.name),
                ("protocol", ExternalIntegration.MARC_EXPORT),
            ])
            response = self.manager.admin_catalog_services_controller.process_catalog_services()
            assert response == INTEGRATION_NAME_ALREADY_IN_USE


        service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.MARC_EXPORT,
            goal=ExternalIntegration.CATALOG_GOAL,
        )

        # Attempt to set an S3 mirror external integration but it does not exist!
        with self.request_context_with_admin("/", method="POST"):
            ME = MARCExporter
            flask.request.form = MultiDict([
                ("name", "exporter name"),
                ("id", service.id),
                ("protocol", ME.NAME),
                ("mirror_integration_id", "1234")
            ])
            response = self.manager.admin_catalog_services_controller.process_catalog_services()
            assert response.uri == MISSING_INTEGRATION.uri

        s3, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.S3,
            goal=ExternalIntegration.STORAGE_GOAL,
        )

        # Now an S3 integration exists, but it has no MARC bucket configured.
        with self.request_context_with_admin("/", method="POST"):
            ME = MARCExporter
            flask.request.form = MultiDict([
                ("name", "exporter name"),
                ("id", service.id),
                ("protocol", ME.NAME),
                ("mirror_integration_id", s3.id)
            ])
            response = self.manager.admin_catalog_services_controller.process_catalog_services()
            assert response.uri == MISSING_INTEGRATION.uri

        self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        self._db.flush()
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "new name"),
                ("protocol", ME.NAME),
                ("mirror_integration_id", s3.id),
            ])
            pytest.raises(AdminNotAuthorized,
                          self.manager.admin_catalog_services_controller.process_catalog_services)

        # This should be the last test to check since rolling back database
        # changes in the test can cause it to crash.
        s3.setting(S3UploaderConfiguration.MARC_BUCKET_KEY).value = "marc-files"
        service.libraries += [self._default_library]
        self.admin.add_role(AdminRole.SYSTEM_ADMIN)

        with self.request_context_with_admin("/", method="POST"):
            ME = MARCExporter
            flask.request.form = MultiDict([
                ("name", "new name"),
                ("protocol", ME.NAME),
                ("mirror_integration_id", s3.id),
                ("libraries", json.dumps([{
                    "short_name": self._default_library.short_name,
                    ME.INCLUDE_SUMMARY: "false",
                    ME.INCLUDE_SIMPLIFIED_GENRES: "true",
                }])),
            ])
            response = self.manager.admin_catalog_services_controller.process_catalog_services()
            assert response.uri == MULTIPLE_SERVICES_FOR_LIBRARY.uri

    def test_catalog_services_post_create(self):
        ME = MARCExporter

        s3, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.S3,
            goal=ExternalIntegration.STORAGE_GOAL,
        )
        s3.setting(S3UploaderConfiguration.MARC_BUCKET_KEY).value = "marc-files"

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "exporter name"),
                ("protocol", ME.NAME),
                ("mirror_integration_id", s3.id),
                ("libraries", json.dumps([{
                    "short_name": self._default_library.short_name,
                    ME.INCLUDE_SUMMARY: "false",
                    ME.INCLUDE_SIMPLIFIED_GENRES: "true",
                }])),
            ])
            response = self.manager.admin_catalog_services_controller.process_catalog_services()
            assert response.status_code == 201

        service = get_one(self._db, ExternalIntegration, goal=ExternalIntegration.CATALOG_GOAL)
        # There was one S3 integration and it was selected. The service has an
        # External Integration Link to the storage integration that is created
        # in a POST with purpose of ExternalIntegrationLink.MARC.
        integration_link = get_one(
            self._db, ExternalIntegrationLink, external_integration_id=service.id, purpose=ExternalIntegrationLink.MARC
        )

        assert service.id == int(response.response[0])
        assert ME.NAME == service.protocol
        assert "exporter name" == service.name
        assert [self._default_library] == service.libraries
        # We expect the Catalog external integration to have a link to the
        # S3 storage external integration
        assert s3.id == integration_link.other_integration_id
        assert "false" == ConfigurationSetting.for_library_and_externalintegration(
                self._db, ME.INCLUDE_SUMMARY, self._default_library, service).value
        assert "true" == ConfigurationSetting.for_library_and_externalintegration(
                self._db, ME.INCLUDE_SIMPLIFIED_GENRES, self._default_library, service).value

    def test_catalog_services_post_edit(self):
        ME = MARCExporter

        s3, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.S3,
            goal=ExternalIntegration.STORAGE_GOAL,
        )
        s3.setting(S3UploaderConfiguration.MARC_BUCKET_KEY).value = "marc-files"

        service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ME.NAME, goal=ExternalIntegration.CATALOG_GOAL,
            name="name"
        )

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "exporter name"),
                ("id", service.id),
                ("protocol", ME.NAME),
                ("mirror_integration_id", s3.id),
                ("libraries", json.dumps([{
                    "short_name": self._default_library.short_name,
                    ME.INCLUDE_SUMMARY: "false",
                    ME.INCLUDE_SIMPLIFIED_GENRES: "true",
                }])),
            ])
            response = self.manager.admin_catalog_services_controller.process_catalog_services()
            assert response.status_code == 200

        integration_link = get_one(
            self._db, ExternalIntegrationLink, external_integration_id=service.id, purpose=ExternalIntegrationLink.MARC
        )
        assert service.id == int(response.response[0])
        assert ME.NAME == service.protocol
        assert "exporter name" == service.name
        assert s3.id == integration_link.other_integration_id
        assert [self._default_library] == service.libraries
        assert "false" == ConfigurationSetting.for_library_and_externalintegration(
                self._db, ME.INCLUDE_SUMMARY, self._default_library, service).value
        assert "true" == ConfigurationSetting.for_library_and_externalintegration(
                self._db, ME.INCLUDE_SIMPLIFIED_GENRES, self._default_library, service).value

    def test_catalog_services_delete(self):
        ME = MARCExporter
        service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ME.NAME, goal=ExternalIntegration.CATALOG_GOAL,
            name="name"
        )

        with self.request_context_with_admin("/", method="DELETE"):
            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            pytest.raises(AdminNotAuthorized,
                          self.manager.admin_catalog_services_controller.process_delete,
                          service.id)

            self.admin.add_role(AdminRole.SYSTEM_ADMIN)
            response = self.manager.admin_catalog_services_controller.process_delete(service.id)
            assert response.status_code == 200

        service = get_one(self._db, ExternalIntegration, id=service.id)
        assert None == service
