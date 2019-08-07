from nose.tools import (
    set_trace,
    eq_,
    assert_raises,
)
import flask
from werkzeug import MultiDict
import json

from api.admin.exceptions import *
from core.marc import MARCExporter
from core.model import (
    AdminRole,
    ConfigurationSetting,
    ExternalIntegration,
    create,
    get_one,
)
from .test_controller import SettingsControllerTest
from core.s3 import S3Uploader

class TestCatalogServicesController(SettingsControllerTest):

    def test_catalog_services_get_with_no_services(self):
        with self.request_context_with_admin("/"):
            response = self.manager.admin_catalog_services_controller.process_catalog_services()
            eq_(response.get("catalog_services"), [])
            protocols = response.get("protocols")
            eq_(1, len(protocols))
            eq_(MARCExporter.NAME, protocols[0].get("name"))
            assert "settings" in protocols[0]
            assert "library_settings" in protocols[0]

            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            self._db.flush()
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_catalog_services_controller.process_catalog_services)

        def test_catalog_services_get_with_marc_exporter(self):
            integration, ignore = create(
                self._db, ExternalIntegration,
                protocol=ExternalIntegration.MARC_EXPORT,
                goal=ExternalIntegration.CATALOG_GOAL,
                name="name",
            )
            integration.setting(MARCExporter.STORAGE_PROTOCOL).value = ExternalIntegration.S3
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
                eq_(integration.id, service.get("id"))
                eq_(integration.name, service.get("name"))
                eq_(integration.protocol, service.get("protocol"))
                eq_(ExternalIntegration.S3, service.get("settings").get(MARCExporter.STORAGE_PROTOCOL))
                [library] = service.get("libraries")
                eq_(self._default_library.short_name, library.get("short_name"))
                eq_("US-MaBoDPL", library.get(MARCExporter.MARC_ORGANIZATION_CODE))
                eq_("false", library.get(MARCExporter.INCLUDE_SUMMARY))
                eq_("true", library.get(MARCExporter.INCLUDE_SIMPLIFIED_GENRES))


    def test_catalog_services_post_errors(self):
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", "Unknown"),
            ])
            response = self.manager.admin_catalog_services_controller.process_catalog_services()
            eq_(response, UNKNOWN_PROTOCOL)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", "123"),
            ])
            response = self.manager.admin_catalog_services_controller.process_catalog_services()
            eq_(response, MISSING_SERVICE)

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
            eq_(response, CANNOT_CHANGE_PROTOCOL)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", service.name),
                ("protocol", ExternalIntegration.MARC_EXPORT),
            ])
            response = self.manager.admin_catalog_services_controller.process_catalog_services()
            eq_(response, INTEGRATION_NAME_ALREADY_IN_USE)

        service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.MARC_EXPORT,
            goal=ExternalIntegration.CATALOG_GOAL,
        )

        with self.request_context_with_admin("/", method="POST"):
            ME = MARCExporter
            flask.request.form = MultiDict([
                ("name", "exporter name"),
                ("id", service.id),
                ("protocol", ME.NAME),
                (ME.STORAGE_PROTOCOL, "not-a-protocol"),
            ])
            response = self.manager.admin_catalog_services_controller.process_catalog_services()
            eq_(response.uri, INVALID_CONFIGURATION_OPTION.uri)

        # Try to configure S3, but no S3 integration exists.
        with self.request_context_with_admin("/", method="POST"):
            ME = MARCExporter
            flask.request.form = MultiDict([
                ("name", "exporter name"),
                ("id", service.id),
                ("protocol", ME.NAME),
                (ME.STORAGE_PROTOCOL, ExternalIntegration.S3),
            ])
            response = self.manager.admin_catalog_services_controller.process_catalog_services()
            eq_(response.uri, MISSING_SERVICE.uri)

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
                (ME.STORAGE_PROTOCOL, ExternalIntegration.S3),
            ])
            response = self.manager.admin_catalog_services_controller.process_catalog_services()
            eq_(response.uri, MISSING_SERVICE.uri)

        s3.setting(S3Uploader.MARC_BUCKET_KEY).value = "marc-files"
        service.libraries += [self._default_library]

        with self.request_context_with_admin("/", method="POST"):
            ME = MARCExporter
            flask.request.form = MultiDict([
                ("name", "new name"),
                ("protocol", ME.NAME),
                (ME.STORAGE_PROTOCOL, ExternalIntegration.S3),
                ("libraries", json.dumps([{
                    "short_name": self._default_library.short_name,
                    ME.INCLUDE_SUMMARY: "false",
                    ME.INCLUDE_SIMPLIFIED_GENRES: "true",
                }])),
            ])
            response = self.manager.admin_catalog_services_controller.process_catalog_services()
            eq_(response.uri, MULTIPLE_SERVICES_FOR_LIBRARY.uri)

        self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        self._db.flush()
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "new name"),
                ("protocol", ME.NAME),
                (ME.STORAGE_PROTOCOL, ExternalIntegration.S3),
            ])
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_catalog_services_controller.process_catalog_services)


    def test_catalog_services_post_create(self):
        ME = MARCExporter

        s3, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.S3,
            goal=ExternalIntegration.STORAGE_GOAL,
        )
        s3.setting(S3Uploader.MARC_BUCKET_KEY).value = "marc-files"

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "exporter name"),
                ("protocol", ME.NAME),
                (ME.STORAGE_PROTOCOL, ExternalIntegration.S3),
                ("libraries", json.dumps([{
                    "short_name": self._default_library.short_name,
                    ME.INCLUDE_SUMMARY: "false",
                    ME.INCLUDE_SIMPLIFIED_GENRES: "true",
                }])),
            ])
            response = self.manager.admin_catalog_services_controller.process_catalog_services()
            eq_(response.status_code, 201)

        service = get_one(self._db, ExternalIntegration, goal=ExternalIntegration.CATALOG_GOAL)
        eq_(service.id, int(response.response[0]))
        eq_(ME.NAME, service.protocol)
        eq_("exporter name", service.name)
        eq_(ExternalIntegration.S3, service.setting(ME.STORAGE_PROTOCOL).value)
        eq_([self._default_library], service.libraries)
        eq_("false", ConfigurationSetting.for_library_and_externalintegration(
                self._db, ME.INCLUDE_SUMMARY, self._default_library, service).value)
        eq_("true", ConfigurationSetting.for_library_and_externalintegration(
                self._db, ME.INCLUDE_SIMPLIFIED_GENRES, self._default_library, service).value)

    def test_catalog_services_post_edit(self):
        ME = MARCExporter

        s3, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.S3,
            goal=ExternalIntegration.STORAGE_GOAL,
        )
        s3.setting(S3Uploader.MARC_BUCKET_KEY).value = "marc-files"

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
                (ME.STORAGE_PROTOCOL, ExternalIntegration.S3),
                ("libraries", json.dumps([{
                    "short_name": self._default_library.short_name,
                    ME.INCLUDE_SUMMARY: "false",
                    ME.INCLUDE_SIMPLIFIED_GENRES: "true",
                }])),
            ])
            response = self.manager.admin_catalog_services_controller.process_catalog_services()
            eq_(response.status_code, 200)

        eq_(service.id, int(response.response[0]))
        eq_(ME.NAME, service.protocol)
        eq_("exporter name", service.name)
        eq_(ExternalIntegration.S3, service.setting(ME.STORAGE_PROTOCOL).value)
        eq_([self._default_library], service.libraries)
        eq_("false", ConfigurationSetting.for_library_and_externalintegration(
                self._db, ME.INCLUDE_SUMMARY, self._default_library, service).value)
        eq_("true", ConfigurationSetting.for_library_and_externalintegration(
                self._db, ME.INCLUDE_SIMPLIFIED_GENRES, self._default_library, service).value)

    def test_catalog_services_delete(self):
        ME = MARCExporter
        service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ME.NAME, goal=ExternalIntegration.CATALOG_GOAL,
            name="name"
        )

        with self.request_context_with_admin("/", method="DELETE"):
            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_catalog_services_controller.process_delete,
                          service.id)

            self.admin.add_role(AdminRole.SYSTEM_ADMIN)
            response = self.manager.admin_catalog_services_controller.process_delete(service.id)
            eq_(response.status_code, 200)

        service = get_one(self._db, ExternalIntegration, id=service.id)
        eq_(None, service)
