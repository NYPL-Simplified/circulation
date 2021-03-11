from nose.tools import (
    set_trace,
    eq_,
    assert_raises
)
import flask
import json
from werkzeug.datastructures import MultiDict
from api.admin.exceptions import *
from api.registry import (
    RemoteRegistry,
    Registration,
)
from core.model import (
    AdminRole,
    ConfigurationSetting,
    create,
    ExternalIntegration,
    Library,
)
from core.testing import (
    DummyHTTPClient,
    MockRequestsResponse,
)
from core.util.http import HTTP
from test_controller import SettingsControllerTest

class TestLibraryRegistration(SettingsControllerTest):
    """Test the process of registering a library with a RemoteRegistry."""

    def test_discovery_service_library_registrations_get(self):
        # Here's a discovery service.
        discovery_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.OPDS_REGISTRATION,
            goal=ExternalIntegration.DISCOVERY_GOAL,
        )

        # We'll be making a mock request to this URL later.
        discovery_service.setting(ExternalIntegration.URL).value = (
            "http://service-url/"
        )

        # We successfully registered this library with the service.
        succeeded, ignore = create(
            self._db, Library, name="Library 1", short_name="L1",
        )
        config = ConfigurationSetting.for_library_and_externalintegration
        config(
            self._db, "library-registration-status", succeeded,
            discovery_service
        ).value = "success"

        # We tried to register this library with the service but were
        # unsuccessful.
        config(
            self._db, "library-registration-stage", succeeded,
            discovery_service
        ).value = "production"
        failed, ignore = create(
            self._db, Library, name="Library 2", short_name="L2",
        )
        config(
            self._db, "library-registration-status", failed, discovery_service,
        ).value = "failure"
        config(
            self._db, "library-registration-stage", failed, discovery_service,
        ).value = "testing"

        # We've never tried to register this library with the service.
        unregistered, ignore = create(
            self._db, Library, name="Library 3", short_name="L3",
        )
        discovery_service.libraries = [succeeded, failed]

        # When a client sends a GET request to the controller, the
        # controller is going to call
        # RemoteRegistry.fetch_registration_document() to try and find
        # the discovery services' terms of service. That's going to
        # make one or two HTTP requests.

        # First, let's try the scenario where the discovery serivce is
        # working and has a terms-of-service.
        client = DummyHTTPClient()

        # In this case we'll make two requests. The first request will
        # ask for the root catalog, where we'll look for a
        # registration link.
        root_catalog = dict(
            links=[dict(href="http://register-here/", rel="register")]
        )
        client.queue_requests_response(
            200, RemoteRegistry.OPDS_2_TYPE,
            content=json.dumps(root_catalog)
        )

        # The second request will fetch that registration link -- then
        # we'll look for TOS data inside.
        registration_document = dict(
            links=[
                dict(
                    rel="terms-of-service", type="text/html",
                    href="http://tos/"
                ),
                dict(
                    rel="terms-of-service", type="text/html",
                    href="data:text/html;charset=utf-8;base64,PHA+SG93IGFib3V0IHRoYXQgVE9TPC9wPg=="
                )
            ]
        )
        client.queue_requests_response(
            200, RemoteRegistry.OPDS_2_TYPE,
            content=json.dumps(registration_document)
        )

        controller = self.manager.admin_discovery_service_library_registrations_controller
        m = controller.process_discovery_service_library_registrations
        with self.request_context_with_admin("/", method="GET"):
            response = m(do_get=client.do_get)
            # The document we get back from the controller is a
            # dictionary with useful information on all known
            # discovery integrations -- just one, in this case.
            [service] = response["library_registrations"]
            assert discovery_service.id == service["id"]

            # The two mock HTTP requests we predicted actually
            # happened.  The target of the first request is the URL to
            # the discovery service's main catalog. The second request
            # is to the "register" link found in that catalog.
            assert (["http://service-url/", "http://register-here/"] ==
                client.requests)

            # The TOS link and TOS HTML snippet were recovered from
            # the registration document served in response to the
            # second HTTP request, and included in the dictionary.
            assert "http://tos/" == service['terms_of_service_link']
            assert "<p>How about that TOS</p>" == service['terms_of_service_html']
            assert None == service['access_problem']

            # The dictionary includes a 'libraries' object, a list of
            # dictionaries with information about the relationships
            # between this discovery integration and every library
            # that's tried to register with it.
            info1, info2 = service["libraries"]

            # Here's the library that successfully registered.
            assert (
                info1 ==
                dict(short_name=succeeded.short_name, status="success",
                     stage="production"))

            # And here's the library that tried to register but
            # failed.
            assert (
                info2 ==
                dict(short_name=failed.short_name, status="failure",
                     stage="testing"))

            # Note that `unregistered`, the library that never tried
            # to register with this discover service, is not included.

            # Now let's try the controller method again, except this
            # time the discovery service's web server is down. The
            # first request will return a ProblemDetail document, and
            # there will be no second request.
            client.requests = []
            client.queue_requests_response(
                502, content=REMOTE_INTEGRATION_FAILED,
            )
            response = m(do_get=client.do_get)

            # Everything looks good, except that there's no TOS data
            # available.
            [service] = response["library_registrations"]
            assert discovery_service.id == service["id"]
            assert 2 == len(service['libraries'])
            assert None == service['terms_of_service_link']
            assert None == service['terms_of_service_html']

            # The problem detail document that prevented the TOS data
            # from showing up has been converted to a dictionary and
            # included in the dictionary of information for this
            # discovery service.
            assert (REMOTE_INTEGRATION_FAILED.uri ==
                service['access_problem']['type'])

            # When the user lacks the SYSTEM_ADMIN role, the
            # controller won't even start processing their GET
            # request.
            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            self._db.flush()
            assert_raises(AdminNotAuthorized, m)

    def test_discovery_service_library_registrations_post(self):
        """Test what might happen when you POST to
        discovery_service_library_registrations.
        """

        controller = self.manager.admin_discovery_service_library_registrations_controller
        m = controller.process_discovery_service_library_registrations

        # Here, the user doesn't have permission to start the
        # registration process.
        self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        with self.request_context_with_admin("/", method="POST"):
            assert_raises(AdminNotAuthorized, m,
                          do_get=self.do_request, do_post=self.do_request)
        self.admin.add_role(AdminRole.SYSTEM_ADMIN)

        # The integration ID might not correspond to a valid
        # ExternalIntegration.
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("integration_id", "1234"),
            ])
            response = m()
            assert MISSING_SERVICE == response

        # Create an ExternalIntegration to avoid that problem in future
        # tests.
        discovery_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.OPDS_REGISTRATION,
            goal=ExternalIntegration.DISCOVERY_GOAL,
        )
        discovery_service.url = "registry url"

        # The library name might not correspond to a real library.
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("integration_id", discovery_service.id),
                ("library_short_name", "not-a-library"),
            ])
            response = m()
            assert NO_SUCH_LIBRARY == response

        # Take care of that problem.
        library = self._default_library
        form = MultiDict([
            ("integration_id", discovery_service.id),
            ("library_short_name", library.short_name),
            ("registration_stage", Registration.TESTING_STAGE),
        ])

        # Registration.push might return a ProblemDetail for whatever
        # reason.
        class Mock(Registration):
            # We reproduce the signature, even though it's not
            # necessary for what we're testing, so that if the push()
            # signature changes this test will fail.
            def push(self, stage, url_for, catalog_url=None, do_get=None,
                     do_post=None):
                return REMOTE_INTEGRATION_FAILED

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = form
            response = m(registration_class=Mock)
            assert REMOTE_INTEGRATION_FAILED == response

        # But if that doesn't happen, success!
        class Mock(Registration):
            """When asked to push a registration, do nothing and say it
            worked.
            """
            called_with = None
            def push(self, *args, **kwargs):
                Mock.called_with = (args, kwargs)
                return True

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = form
            response = controller.process_discovery_service_library_registrations(
                registration_class=Mock
            )
            assert 200 == response.status_code

            # push() was called with the arguments we would expect.
            args, kwargs = Mock.called_with
            assert (Registration.TESTING_STAGE, self.manager.url_for) == args

            # We would have made real HTTP requests.
            assert HTTP.debuggable_post == kwargs.pop('do_post')
            assert HTTP.debuggable_get == kwargs.pop('do_get')

            # No other keyword arguments were passed in.
            assert {} == kwargs
