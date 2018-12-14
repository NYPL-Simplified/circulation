from nose.tools import (
    set_trace,
    eq_,
    assert_raises
)
import flask
import json
from werkzeug import MultiDict
from api.admin.exceptions import *
from api.admin.problem_details import INVALID_URL
from api.novelist import NoveListAPI
from api.admin.controller.metadata_services import MetadataServicesController
from core.model import (
    AdminRole,
    create,
    ExternalIntegration,
    get_one,
    Library,
)
from test_controller import SettingsControllerTest

class TestMetadataServices(SettingsControllerTest):

    def test_process_metadata_services_dispatches_by_request_method(self):
        class Mock(MetadataServicesController):
            def process_get():
                self.called = "GET"
            def process_post():
                self.called = "POST"
                
        self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        self._db.flush()
        
        # This is also where permissions are checked.
        assert_raises(
            AdminNotAuthorized,
            controller.process_metadata_services()
        )


    def test_metadata_services_get_with_no_services(self):
        with self.request_context_with_admin("/"):
            response = self.manager.admin_metadata_services_controller.process_metadata_services()
            eq_(response.get("metadata_services"), [])
            protocols = response.get("protocols")
            assert NoveListAPI.NAME in [p.get("label") for p in protocols]
            assert "settings" in protocols[0]


    def test_metadata_services_get_with_one_service(self):
        novelist_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.NOVELIST,
            goal=ExternalIntegration.METADATA_GOAL,
        )
        novelist_service.username = "user"
        novelist_service.password = "pass"

        with self.request_context_with_admin("/"):
            response = self.manager.admin_metadata_services_controller.process_metadata_services()
            [service] = response.get("metadata_services")

            eq_(novelist_service.id, service.get("id"))
            eq_(ExternalIntegration.NOVELIST, service.get("protocol"))
            eq_("user", service.get("settings").get(ExternalIntegration.USERNAME))
            eq_("pass", service.get("settings").get(ExternalIntegration.PASSWORD))

        novelist_service.libraries += [self._default_library]
        with self.request_context_with_admin("/"):
            response = self.manager.admin_metadata_services_controller.process_metadata_services()
            [service] = response.get("metadata_services")

            eq_("user", service.get("settings").get(ExternalIntegration.USERNAME))
            [library] = service.get("libraries")
            eq_(self._default_library.short_name, library.get("short_name"))

    def test_metadata_services_post_errors(self):
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Name"),
                ("protocol", "Unknown"),
            ])
            response = self.manager.admin_metadata_services_controller.process_metadata_services()
            eq_(response, UNKNOWN_PROTOCOL)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([])
            response = self.manager.admin_metadata_services_controller.process_metadata_services()
            eq_(response, INCOMPLETE_CONFIGURATION)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Name"),
            ])
            response = self.manager.admin_metadata_services_controller.process_metadata_services()
            eq_(response, NO_PROTOCOL_FOR_NEW_SERVICE)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Name"),
                ("id", "123"),
                ("protocol", ExternalIntegration.NYT),
            ])
            response = self.manager.admin_metadata_services_controller.process_metadata_services()
            eq_(response, MISSING_SERVICE)

        service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.NOVELIST,
            goal=ExternalIntegration.METADATA_GOAL,
            name="name",
        )

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", service.name),
                ("protocol", ExternalIntegration.NYT),
            ])
            response = self.manager.admin_metadata_services_controller.process_metadata_services()
            eq_(response, INTEGRATION_NAME_ALREADY_IN_USE)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Name"),
                ("id", service.id),
                ("protocol", ExternalIntegration.NYT),
            ])
            response = self.manager.admin_metadata_services_controller.process_metadata_services()
            eq_(response, CANNOT_CHANGE_PROTOCOL)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", service.id),
                ("protocol", ExternalIntegration.NOVELIST),
            ])
            response = self.manager.admin_metadata_services_controller.process_metadata_services()
            eq_(response.uri, INCOMPLETE_CONFIGURATION.uri)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Name"),
                ("id", service.id),
                ("protocol", ExternalIntegration.NOVELIST),
                (ExternalIntegration.USERNAME, "user"),
                (ExternalIntegration.PASSWORD, "pass"),
                ("libraries", json.dumps([{"short_name": "not-a-library"}])),
            ])
            response = self.manager.admin_metadata_services_controller.process_metadata_services()
            eq_(response.uri, NO_SUCH_LIBRARY.uri)

        self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Name"),
                ("protocol", ExternalIntegration.NOVELIST),
                (ExternalIntegration.USERNAME, "user"),
                (ExternalIntegration.PASSWORD, "pass"),
                ("libraries", json.dumps([])),
            ])
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_metadata_services_controller.process_metadata_services)

    def test_metadata_services_post_create(self):

        library, ignore = create(
            self._db, Library, name="Library", short_name="L",
        )
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Name"),
                ("protocol", ExternalIntegration.NOVELIST),
                (ExternalIntegration.USERNAME, "user"),
                (ExternalIntegration.PASSWORD, "pass"),
                ("libraries", json.dumps([{"short_name": "L"}])),
            ])
            response = self.manager.admin_metadata_services_controller.process_metadata_services()
            eq_(response.status_code, 201)

        service = get_one(self._db, ExternalIntegration, goal=ExternalIntegration.METADATA_GOAL)
        eq_(service.id, int(response.response[0]))
        eq_(ExternalIntegration.NOVELIST, service.protocol)
        eq_("user", service.username)
        eq_("pass", service.password)
        eq_([library], service.libraries)

    def test_metadata_services_post_edit(self):
        l1, ignore = create(
            self._db, Library, name="Library 1", short_name="L1",
        )
        l2, ignore = create(
            self._db, Library, name="Library 2", short_name="L2",
        )

        novelist_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.NOVELIST,
            goal=ExternalIntegration.METADATA_GOAL,
        )
        novelist_service.username = "olduser"
        novelist_service.password = "oldpass"
        novelist_service.libraries = [l1]

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Name"),
                ("id", novelist_service.id),
                ("protocol", ExternalIntegration.NOVELIST),
                (ExternalIntegration.USERNAME, "user"),
                (ExternalIntegration.PASSWORD, "pass"),
                ("libraries", json.dumps([{"short_name": "L2"}])),
            ])
            response = self.manager.admin_metadata_services_controller.process_metadata_services()
            eq_(response.status_code, 200)

        eq_(novelist_service.id, int(response.response[0]))
        eq_(ExternalIntegration.NOVELIST, novelist_service.protocol)
        eq_("user", novelist_service.username)
        eq_("pass", novelist_service.password)
        eq_([l2], novelist_service.libraries)

    def test_process_post_calls_register_with_metadata_wrangler(self):
        class Mock(MetadataServicesController):
            RETURN_VALUE = INVALID_URL
            def register_with_metadata_wrangler(
                self, do_get, do_post, is_new, service
            ):
                self.called_with = (do_get, do_post, is_new, service)
                return self.RETURN_VALUE

        

    def test_register_with_metadata_wrangler(self):
        """Verify that register_with_metadata wrangler calls
        process_sitewide_registration appropriately.
        """
        class Mock(MetadataServicesController):
            called_with = None
            def process_sitewide_registration(
                    self, integration, do_get, do_post
            ):
                self.called_with = (integration, do_get, do_post)

        controller = Mock(self.manager)
        m = controller.register_with_metadata_wrangler
        do_get = object()
        do_post = object()

        # If register_with_metadata_wrangler is called on an ExternalIntegration
        # with some other service, nothing happens.
        integration = self._external_integration(
            protocol=ExternalIntegration.NOVELIST
        )
        m(do_get, do_post, True, integration)
        eq_(None, controller.called_with)

        # If it's called on an existing metadata wrangler integration
        # that that already has a password set, nothing happens.
        integration = self._external_integration(
            protocol=ExternalIntegration.METADATA_WRANGLER
        )
        integration.password = 'already done'
        m(do_get, do_post, False, integration)
        eq_(None, controller.called_with)

        # If it's called on a new metadata wrangler integration,
        # register_with_metadata_wrangler is called.
        m(do_get, do_post, True, integration)
        eq_((integration, do_get, do_post), controller.called_with)

        # Same if it's called on an old integration that's missing its
        # password.
        controller.called_with = None
        integration.password = None
        result = m(do_get, do_post, False, integration)

    def test_check_name_unique(self):
       kwargs = dict(protocol=ExternalIntegration.NYT,
                    goal=ExternalIntegration.METADATA_GOAL)

       existing_service, ignore = create(self._db, ExternalIntegration, name="existing service", **kwargs)
       new_service, ignore = create(self._db, ExternalIntegration, name="new service", **kwargs)

       m = self.manager.admin_metadata_services_controller.check_name_unique

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

    def test_metadata_service_delete(self):
        l1, ignore = create(
            self._db, Library, name="Library 1", short_name="L1",
        )
        novelist_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.NOVELIST,
            goal=ExternalIntegration.METADATA_GOAL,
        )
        novelist_service.username = "olduser"
        novelist_service.password = "oldpass"
        novelist_service.libraries = [l1]

        with self.request_context_with_admin("/", method="DELETE"):
            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_metadata_services_controller.process_delete,
                          novelist_service.id)

            self.admin.add_role(AdminRole.SYSTEM_ADMIN)
            response = self.manager.admin_metadata_services_controller.process_delete(novelist_service.id)
            eq_(response.status_code, 200)

        service = get_one(self._db, ExternalIntegration, id=novelist_service.id)
        eq_(None, service)
