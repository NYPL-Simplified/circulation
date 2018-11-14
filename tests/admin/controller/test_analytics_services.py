from nose.tools import (
    set_trace,
    eq_,
    assert_raises
)
import flask
import json
from werkzeug import MultiDict
from api.admin.exceptions import *
from api.google_analytics_provider import GoogleAnalyticsProvider
from core.local_analytics_provider import LocalAnalyticsProvider
from core.model import (
    AdminRole,
    ConfigurationSetting,
    create,
    ExternalIntegration,
    get_one,
    Library,
)
from test_controller import SettingsControllerTest

class TestAnalyticsServices(SettingsControllerTest):

    def test_analytics_services_get_with_no_services(self):
        with self.request_context_with_admin("/"):
            response = self.manager.admin_analytics_services_controller.process_analytics_services()
            eq_(response.get("analytics_services"), [])
            protocols = response.get("protocols")
            assert GoogleAnalyticsProvider.NAME in [p.get("label") for p in protocols]
            assert "settings" in protocols[0]

            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            self._db.flush()
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_analytics_services_controller.process_analytics_services)

    def test_analytics_services_get_with_one_service(self):
        ga_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=GoogleAnalyticsProvider.__module__,
            goal=ExternalIntegration.ANALYTICS_GOAL,
        )
        ga_service.url = self._str

        with self.request_context_with_admin("/"):
            response = self.manager.admin_analytics_services_controller.process_analytics_services()
            [service] = response.get("analytics_services")

            eq_(ga_service.id, service.get("id"))
            eq_(ga_service.protocol, service.get("protocol"))
            eq_(ga_service.url, service.get("settings").get(ExternalIntegration.URL))

        ga_service.libraries += [self._default_library]
        ConfigurationSetting.for_library_and_externalintegration(
            self._db, GoogleAnalyticsProvider.TRACKING_ID, self._default_library, ga_service
        ).value = "trackingid"
        with self.request_context_with_admin("/"):
            response = self.manager.admin_analytics_services_controller.process_analytics_services()
            [service] = response.get("analytics_services")

            [library] = service.get("libraries")
            eq_(self._default_library.short_name, library.get("short_name"))
            eq_("trackingid", library.get(GoogleAnalyticsProvider.TRACKING_ID))

        self._db.delete(ga_service)

        local_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=LocalAnalyticsProvider.__module__,
            goal=ExternalIntegration.ANALYTICS_GOAL,
        )

        local_service.libraries += [self._default_library]
        with self.request_context_with_admin("/"):
            response = self.manager.admin_analytics_services_controller.process_analytics_services()
            [service] = response.get("analytics_services")

            eq_(local_service.id, service.get("id"))
            eq_(local_service.protocol, service.get("protocol"))
            [library] = service.get("libraries")
            eq_(self._default_library.short_name, library.get("short_name"))

    def test_analytics_services_post_errors(self):
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([])
            response = self.manager.admin_analytics_services_controller.process_analytics_services()
            eq_(response, MISSING_ANALYTICS_NAME)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Name"),
                ("protocol", "Unknown"),
                ("url", "http://test"),
            ])
            response = self.manager.admin_analytics_services_controller.process_analytics_services()
            eq_(response, UNKNOWN_PROTOCOL)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Name"),
                ("url", "http://test"),
            ])
            response = self.manager.admin_analytics_services_controller.process_analytics_services()
            eq_(response, NO_PROTOCOL_FOR_NEW_SERVICE)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Name"),
                ("id", "123"),
                ("url", "http://test"),
            ])
            response = self.manager.admin_analytics_services_controller.process_analytics_services()
            eq_(response.uri, MISSING_SERVICE.uri)

        service, ignore = create(
            self._db, ExternalIntegration,
            protocol=GoogleAnalyticsProvider.__module__,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            name="name",
        )

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", service.name),
                ("protocol", GoogleAnalyticsProvider.__module__),
                ("url", "http://test"),
            ])
            response = self.manager.admin_analytics_services_controller.process_analytics_services()
            eq_(response, INTEGRATION_NAME_ALREADY_IN_USE)

        service, ignore = create(
            self._db, ExternalIntegration,
            protocol=GoogleAnalyticsProvider.__module__,
            goal=ExternalIntegration.ANALYTICS_GOAL,
        )

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Name"),
                ("id", service.id),
                ("protocol", "core.local_analytics_provider"),
                ("url", "http://test"),
            ])
            response = self.manager.admin_analytics_services_controller.process_analytics_services()
            eq_(response, CANNOT_CHANGE_PROTOCOL)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", service.id),
                ("name", "analytics name"),
                ("protocol", GoogleAnalyticsProvider.__module__),
                ("url", None),
            ])
            response = self.manager.admin_analytics_services_controller.process_analytics_services()
            eq_(response.uri, INCOMPLETE_CONFIGURATION.uri)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", service.id),
                ("name", "analytics name"),
                ("protocol", GoogleAnalyticsProvider.__module__),
                ("url", "bad_url")
            ])
            response = self.manager.admin_analytics_services_controller.process_analytics_services()
            eq_(response.uri, INVALID_URL.uri)
            assert "bad_url" in response.detail

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", service.id),
                ("protocol", GoogleAnalyticsProvider.__module__),
                ("name", "some other analytics name"),
                (ExternalIntegration.URL, "http://test"),
                ("libraries", json.dumps([{"short_name": "not-a-library"}])),
            ])
            response = self.manager.admin_analytics_services_controller.process_analytics_services()
            eq_(response.uri, NO_SUCH_LIBRARY.uri)

        library, ignore = create(
            self._db, Library, name="Library", short_name="L",
        )

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", service.id),
                ("protocol", GoogleAnalyticsProvider.__module__),
                ("name", "some other name"),
                (ExternalIntegration.URL, ""),
                ("libraries", json.dumps([{"short_name": library.short_name}])),
            ])
            response = self.manager.admin_analytics_services_controller.process_analytics_services()
            eq_(response.uri, INCOMPLETE_CONFIGURATION.uri)

        self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", GoogleAnalyticsProvider.__module__),
                (ExternalIntegration.URL, "url"),
                ("libraries", json.dumps([])),
            ])
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_analytics_services_controller.process_analytics_services)

    def test_analytics_services_post_create(self):
        library, ignore = create(
            self._db, Library, name="Library", short_name="L",
        )
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Google analytics name"),
                ("protocol", GoogleAnalyticsProvider.__module__),
                (ExternalIntegration.URL, "http://test"),
                ("libraries", json.dumps([{"short_name": "L", "tracking_id": "trackingid"}])),
            ])
            response = self.manager.admin_analytics_services_controller.process_analytics_services()
            eq_(response.status_code, 201)

        service = get_one(self._db, ExternalIntegration, goal=ExternalIntegration.ANALYTICS_GOAL)
        eq_(service.id, int(response.response[0]))
        eq_(GoogleAnalyticsProvider.__module__, service.protocol)
        eq_("http://test", service.url)
        eq_([library], service.libraries)
        eq_("trackingid", ConfigurationSetting.for_library_and_externalintegration(
                self._db, GoogleAnalyticsProvider.TRACKING_ID, library, service).value)

        # Creating a local analytics service doesn't require a URL.
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "local analytics name"),
                ("protocol", LocalAnalyticsProvider.__module__),
                ("libraries", json.dumps([{"short_name": "L", "tracking_id": "trackingid"}])),
            ])
            response = self.manager.admin_analytics_services_controller.process_analytics_services()
            eq_(response.status_code, 201)

    def test_analytics_services_post_edit(self):
        l1, ignore = create(
            self._db, Library, name="Library 1", short_name="L1",
        )
        l2, ignore = create(
            self._db, Library, name="Library 2", short_name="L2",
        )

        ga_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=GoogleAnalyticsProvider.__module__,
            goal=ExternalIntegration.ANALYTICS_GOAL,
        )
        ga_service.url = "oldurl"
        ga_service.libraries = [l1]

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", ga_service.id),
                ("name", "some other analytics name"),
                ("protocol", GoogleAnalyticsProvider.__module__),
                (ExternalIntegration.URL, "http://test"),
                ("libraries", json.dumps([{"short_name": "L2", "tracking_id": "l2id"}])),
            ])
            response = self.manager.admin_analytics_services_controller.process_analytics_services()
            eq_(response.status_code, 200)

        eq_(ga_service.id, int(response.response[0]))
        eq_(GoogleAnalyticsProvider.__module__, ga_service.protocol)
        eq_("http://test", ga_service.url)
        eq_([l2], ga_service.libraries)
        eq_("l2id", ConfigurationSetting.for_library_and_externalintegration(
                self._db, GoogleAnalyticsProvider.TRACKING_ID, l2, ga_service).value)

    def test_check_name_unique(self):
       kwargs = dict(protocol=GoogleAnalyticsProvider.__module__,
                      goal=ExternalIntegration.ANALYTICS_GOAL)
       existing_service, ignore = create(self._db, ExternalIntegration, name="existing service", **kwargs)
       new_service, ignore = create(self._db, ExternalIntegration, name="new service", **kwargs)

       m = self.manager.admin_analytics_services_controller.check_name_unique

       # Try to change new service so that it has the same name as existing service
       # -- this is not allowed.
       result = m(new_service, existing_service.name)
       eq_(result, INTEGRATION_NAME_ALREADY_IN_USE)

       # Try to edit existing service without changing its name -- this is fine.
       eq_(
           None,
           m(existing_service, existing_service.name)
       )

       # Changing the existing service's name is also fine.
       eq_(
            None,
            m(existing_service, "new name")
       )

    def test_analytics_service_delete(self):
        l1, ignore = create(
            self._db, Library, name="Library 1", short_name="L1",
        )
        ga_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=GoogleAnalyticsProvider.__module__,
            goal=ExternalIntegration.ANALYTICS_GOAL,
        )
        ga_service.url = "oldurl"
        ga_service.libraries = [l1]

        with self.request_context_with_admin("/", method="DELETE"):
            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_analytics_services_controller.process_delete,
                          ga_service.id)

            self.admin.add_role(AdminRole.SYSTEM_ADMIN)
            response = self.manager.admin_analytics_services_controller.process_delete(ga_service.id)
            eq_(response.status_code, 200)

        service = get_one(self._db, ExternalIntegration, id=ga_service.id)
        eq_(None, service)
