from nose.tools import (
    set_trace,
    eq_,
    assert_raises
)
import flask
from werkzeug import MultiDict
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
from core.util.http import HTTP
from .test_controller import SettingsControllerTest

class TestLibraryRegistration(SettingsControllerTest):
    """Test the process of registering a library with a RemoteRegistry."""

    def test_discovery_service_library_registrations_get(self):
        discovery_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.OPDS_REGISTRATION,
            goal=ExternalIntegration.DISCOVERY_GOAL,
        )
        succeeded, ignore = create(
            self._db, Library, name="Library 1", short_name="L1",
        )
        ConfigurationSetting.for_library_and_externalintegration(
            self._db, "library-registration-status", succeeded, discovery_service,
            ).value = "success"
        ConfigurationSetting.for_library_and_externalintegration(
            self._db, "library-registration-stage", succeeded, discovery_service,
            ).value = "production"
        failed, ignore = create(
            self._db, Library, name="Library 2", short_name="L2",
        )
        ConfigurationSetting.for_library_and_externalintegration(
            self._db, "library-registration-status", failed, discovery_service,
            ).value = "failure"
        ConfigurationSetting.for_library_and_externalintegration(
            self._db, "library-registration-stage", failed, discovery_service,
            ).value = "testing"
        unregistered, ignore = create(
            self._db, Library, name="Library 3", short_name="L3",
        )
        discovery_service.libraries = [succeeded, failed]

        controller = self.manager.admin_discovery_service_library_registrations_controller
        with self.request_context_with_admin("/", method="GET"):
            response = controller.process_discovery_service_library_registrations()

            serviceInfo = response.get("library_registrations")
            eq_(1, len(serviceInfo))
            eq_(discovery_service.id, serviceInfo[0].get("id"))

            libraryInfo = serviceInfo[0].get("libraries")
            expected = [
                dict(short_name=succeeded.short_name, status="success", stage="production"),
                dict(short_name=failed.short_name, status="failure", stage="testing"),
            ]
            eq_(expected, libraryInfo)

            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            self._db.flush()
            assert_raises(AdminNotAuthorized,
                         controller.process_discovery_service_library_registrations)

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
            eq_(MISSING_SERVICE, response)

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
            eq_(NO_SUCH_LIBRARY, response)

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
            eq_(REMOTE_INTEGRATION_FAILED, response)

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
            eq_(200, response.status_code)

            # push() was called with the arguments we would expect.
            args, kwargs = Mock.called_with
            eq_((Registration.TESTING_STAGE, self.manager.url_for), args)

            # We would have made real HTTP requests.
            eq_(HTTP.debuggable_post, kwargs.pop('do_post'))
            eq_(HTTP.debuggable_get, kwargs.pop('do_get'))

            # No other keyword arguments were passed in.
            eq_({}, kwargs)
