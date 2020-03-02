from nose.tools import (
    set_trace,
    eq_,
    assert_raises
)
import flask
import json
from werkzeug.datastructures import MultiDict
from core.util.http import HTTP
from api.nyt import NYTBestSellerAPI
from api.admin.exceptions import *
from api.admin.problem_details import INVALID_URL
from api.novelist import NoveListAPI
from api.admin.controller.metadata_services import MetadataServicesController
from core.opds_import import MetadataWranglerOPDSLookup
from core.model import (
    AdminRole,
    create,
    ExternalIntegration,
    get_one,
    Library,
)
from test_controller import SettingsControllerTest

class TestMetadataServices(SettingsControllerTest):
    def create_service(self, name):
        return create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.__dict__.get(name) or "fake",
            goal=ExternalIntegration.METADATA_GOAL
        )[0]

    def test_process_metadata_services_dispatches_by_request_method(self):
        class Mock(MetadataServicesController):
            def process_get(self):
                return "GET"

            def process_post(self):
                return "POST"

        controller = Mock(self.manager)
        with self.request_context_with_admin("/"):
            eq_("GET", controller.process_metadata_services())

        with self.request_context_with_admin("/", method="POST"):
            eq_("POST", controller.process_metadata_services())

        # This is also where permissions are checked.
        self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        self._db.flush()

        with self.request_context_with_admin("/"):
            assert_raises(
                AdminNotAuthorized,
                controller.process_metadata_services
            )

    def test_process_get_with_no_services(self):
        with self.request_context_with_admin("/"):
            response = self.manager.admin_metadata_services_controller.process_get()
            eq_(response.get("metadata_services"), [])
            protocols = response.get("protocols")
            assert NoveListAPI.NAME in [p.get("label") for p in protocols]
            assert "settings" in protocols[0]

    def test_process_get_with_one_service(self):
        novelist_service = self.create_service("NOVELIST")
        novelist_service.username = "user"
        novelist_service.password = "pass"

        controller = self.manager.admin_metadata_services_controller

        with self.request_context_with_admin("/"):
            response = controller.process_get()
            [service] = response.get("metadata_services")

            eq_(novelist_service.id, service.get("id"))
            eq_(ExternalIntegration.NOVELIST, service.get("protocol"))
            eq_("user", service.get("settings").get(ExternalIntegration.USERNAME))
            eq_("pass", service.get("settings").get(ExternalIntegration.PASSWORD))

        novelist_service.libraries += [self._default_library]
        with self.request_context_with_admin("/"):
            response = controller.process_get()
            [service] = response.get("metadata_services")

            eq_("user", service.get("settings").get(ExternalIntegration.USERNAME))
            [library] = service.get("libraries")
            eq_(self._default_library.short_name, library.get("short_name"))

    def test_process_get_with_self_tests(self):
        metadata_service = self.create_service("METADATA_WRANGLER")
        metadata_service.name = "Test"
        controller = self.manager.admin_metadata_services_controller

        with self.request_context_with_admin("/"):
            response = controller.process_get()
            [service] = response.get("metadata_services")
            eq_(metadata_service.id, service.get("id"))
            eq_(ExternalIntegration.METADATA_WRANGLER, service.get("protocol"))
            eq_(service.has_key("self_test_results"), True)
            # The exception is because there isn't a library registered with the metadata service.
            # But we just need to make sure that the response has a self_test_results attribute--for this test,
            # it doesn't matter what it is--so that's fine.
            eq_(
                service.get("self_test_results").get("exception"),
                "Exception getting self-test results for metadata service Test: Metadata Wrangler improperly configured."
            )

    def test_find_protocol_class(self):
        [wrangler, nyt, novelist, fake] = [self.create_service(x) for x in ["METADATA_WRANGLER", "NYT", "NOVELIST", "FAKE"]]
        m = self.manager.admin_metadata_services_controller.find_protocol_class

        eq_(m(wrangler)[0], MetadataWranglerOPDSLookup)
        eq_(m(nyt)[0], NYTBestSellerAPI)
        eq_(m(novelist)[0], NoveListAPI)
        assert_raises(NotImplementedError, m, fake)

    def test_metadata_services_post_errors(self):
        controller = self.manager.admin_metadata_services_controller
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Name"),
                ("protocol", "Unknown"),
            ])
            response = controller.process_post()
            eq_(response, UNKNOWN_PROTOCOL)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([])
            response = controller.process_post()
            eq_(response, INCOMPLETE_CONFIGURATION)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Name"),
            ])
            response = controller.process_post()
            eq_(response, NO_PROTOCOL_FOR_NEW_SERVICE)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Name"),
                ("id", "123"),
                ("protocol", ExternalIntegration.NYT),
            ])
            response = controller.process_post()
            eq_(response, MISSING_SERVICE)

        service = self.create_service("NOVELIST")
        service.name = "name"

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", service.name),
                ("protocol", ExternalIntegration.NYT),
            ])
            response = controller.process_post()
            eq_(response, INTEGRATION_NAME_ALREADY_IN_USE)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Name"),
                ("id", service.id),
                ("protocol", ExternalIntegration.NYT),
            ])
            response = controller.process_post()
            eq_(response, CANNOT_CHANGE_PROTOCOL)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", service.id),
                ("protocol", ExternalIntegration.NOVELIST),
            ])
            response = controller.process_post()
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
            response = controller.process_post()
            eq_(response.uri, NO_SUCH_LIBRARY.uri)

    def test_metadata_services_post_create(self):
        controller = self.manager.admin_metadata_services_controller
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
            response = controller.process_post()
            eq_(response.status_code, 201)

        # A new ExternalIntegration has been created based on the submitted
        # information.
        service = get_one(
            self._db, ExternalIntegration,
            goal=ExternalIntegration.METADATA_GOAL
        )
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
        novelist_service = self.create_service("NOVELIST")
        novelist_service.username = "olduser"
        novelist_service.password = "oldpass"
        novelist_service.libraries = [l1]

        controller = self.manager.admin_metadata_services_controller
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Name"),
                ("id", novelist_service.id),
                ("protocol", ExternalIntegration.NOVELIST),
                (ExternalIntegration.USERNAME, "user"),
                (ExternalIntegration.PASSWORD, "pass"),
                ("libraries", json.dumps([{"short_name": "L2"}])),
            ])
            response = controller.process_post()
            eq_(response.status_code, 200)

    def test_metadata_services_post_calls_register_with_metadata_wrangler(self):
        """Verify that process_post() calls register_with_metadata_wrangler
        if the rest of the request is handled successfully.
        """
        class Mock(MetadataServicesController):
            RETURN_VALUE = INVALID_URL
            called_with = None
            def register_with_metadata_wrangler(
                self, do_get, do_post, is_new, service
            ):
                self.called_with = (do_get, do_post, is_new, service)
                return self.RETURN_VALUE

        controller = Mock(self.manager)
        library, ignore = create(
            self._db, Library, name="Library", short_name="L",
        )
        do_get = object()
        do_post = object()
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([])
            controller.process_post(do_get, do_post)

            # Since there was an error condition,
            # register_with_metadata_wrangler was not called.
            eq_(None, controller.called_with)

        form = MultiDict([
            ("name", "Name"),
            ("protocol", ExternalIntegration.NOVELIST),
            (ExternalIntegration.USERNAME, "user"),
            (ExternalIntegration.PASSWORD, "pass"),
        ])

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = form
            response = controller.process_post(do_get=do_get, do_post=do_post)

            # register_with_metadata_wrangler was called, but it
            # returned a ProblemDetail, so the overall request
            # failed.
            eq_((do_get, do_post, True), controller.called_with[:-1])
            eq_(INVALID_URL, response)

            # We ended up not creating an ExternalIntegration.
            eq_(None, get_one(self._db, ExternalIntegration,
                              goal=ExternalIntegration.METADATA_GOAL))

            # But the ExternalIntegration we _would_ have created was
            # passed in to register_with_metadata_wrangler.
            bad_integration = controller.called_with[-1]

            # We can tell it's bad because it was disconnected from
            # our database session.
            eq_(None, bad_integration._sa_instance_state.session)

        # Now try the same scenario, except that
        # register_with_metadata_wrangler does _not_ return a
        # ProblemDetail.
        Mock.RETURN_VALUE = "It's all good"
        Mock.called_with = None
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = form
            response = controller.process_post(do_get=do_get, do_post=do_post)

            # This time we successfully created an ExternalIntegration.
            integration = get_one(
                self._db, ExternalIntegration,
                goal=ExternalIntegration.METADATA_GOAL
            )
            assert integration != None

            # It was passed in to register_with_metadata_wrangler
            # along with the rest of the arguments we expect.
            eq_((do_get, do_post, True, integration), controller.called_with)
            eq_(integration, controller.called_with[-1])
            eq_(self._db, integration._sa_instance_state.session)

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
        novelist_service = self.create_service("NOVELIST")
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
