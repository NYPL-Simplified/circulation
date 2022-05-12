import json
from oauth2client import client as GoogleClient

from core.testing import DatabaseTest
from core.util.problem_detail import ProblemDetail

from api.admin.google_oauth_admin_authentication_provider import (
    GoogleOAuthAdminAuthenticationProvider,
    DummyGoogleClient,
)
from api.admin.problem_details import INVALID_ADMIN_CREDENTIALS
from core.model import (
    Admin,
    AdminRole,
    ConfigurationSetting,
    ExternalIntegration,
    create,
)

class TestGoogleOAuthAdminAuthenticationProvider(DatabaseTest):

    def test_callback(self):
        super(TestGoogleOAuthAdminAuthenticationProvider, self).setup_method()
        auth_integration, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.GOOGLE_OAUTH,
            goal=ExternalIntegration.ADMIN_AUTH_GOAL
        )
        self.google = GoogleOAuthAdminAuthenticationProvider(auth_integration, "", test_mode=True)
        auth_integration.libraries += [self._default_library]
        ConfigurationSetting.for_library_and_externalintegration(
            self._db, "domains", self._default_library, auth_integration
        ).value = json.dumps(["nypl.org"])

        # Returns a problem detail when Google returns an error.
        error_response, redirect = self.google.callback(self._db, {'error' : 'access_denied'})
        assert True == isinstance(error_response, ProblemDetail)
        assert 400 == error_response.status_code
        assert True == error_response.detail.endswith('access_denied')
        assert None == redirect

        # Successful case creates a dict of admin details
        success, redirect = self.google.callback(self._db, {'code' : 'abc'})
        assert 'example@nypl.org' == success['email']
        default_credentials = json.dumps({"id_token": {"email": "example@nypl.org", "hd": "nypl.org"}})
        assert default_credentials == success['credentials']
        assert GoogleOAuthAdminAuthenticationProvider.NAME == success["type"]
        [role] = success.get("roles")
        assert AdminRole.LIBRARIAN == role.get("role")
        assert self._default_library.short_name == role.get("library")

        # If domains are set, the admin's domain must match one of the domains.
        setting = ConfigurationSetting.for_library_and_externalintegration(
            self._db, "domains", self._default_library, auth_integration)
        setting.value = json.dumps(["otherlibrary.org"])
        failure, ignore = self.google.callback(self._db, {'code' : 'abc'})
        assert INVALID_ADMIN_CREDENTIALS == failure
        setting.value = json.dumps(["nypl.org"])

        # Returns a problem detail when the oauth client library
        # raises an exception.
        class ExceptionRaisingClient(DummyGoogleClient):
            def step2_exchange(self, auth_code):
                raise GoogleClient.FlowExchangeError("mock error")
        self.google.dummy_client = ExceptionRaisingClient()
        error_response, redirect = self.google.callback(self._db, {'code' : 'abc'})
        assert True == isinstance(error_response, ProblemDetail)
        assert 400 == error_response.status_code
        assert True == error_response.detail.endswith('mock error')
        assert None == redirect

    def test_domains(self):
        super(TestGoogleOAuthAdminAuthenticationProvider, self).setup_method()
        auth_integration, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.GOOGLE_OAUTH,
            goal=ExternalIntegration.ADMIN_AUTH_GOAL
        )
        auth_integration.libraries += [self._default_library]
        ConfigurationSetting.for_library_and_externalintegration(
            self._db, "domains", self._default_library, auth_integration
        ).value = json.dumps(["nypl.org"])

        google = GoogleOAuthAdminAuthenticationProvider(auth_integration, "", test_mode=True)

        assert ["nypl.org"] == google.domains.keys()
        assert [self._default_library] == google.domains["nypl.org"]

        l2 = self._library()
        auth_integration.libraries += [l2]
        ConfigurationSetting.for_library_and_externalintegration(
            self._db, "domains", l2, auth_integration
        ).value = json.dumps(["nypl.org", "l2.org"])

        assert set([self._default_library, l2]) == set(google.domains["nypl.org"])
        assert [l2] == google.domains["l2.org"]

    def test_staff_email(self):
        super(TestGoogleOAuthAdminAuthenticationProvider, self).setup_method()
        auth_integration, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.GOOGLE_OAUTH,
            goal=ExternalIntegration.ADMIN_AUTH_GOAL
        )

        nypl_admin = create(self._db, Admin, email="admin@nypl.org")
        bpl_admin = create(self._db, Admin, email="admin@bklynlibrary.org")

        # If no domains are set, the admin must already exist in the db
        # to be considered library staff.
        google = GoogleOAuthAdminAuthenticationProvider(auth_integration, "", test_mode=True)

        assert True == google.staff_email(self._db, "admin@nypl.org")
        assert True == google.staff_email(self._db, "admin@bklynlibrary.org")
        assert False == google.staff_email(self._db, "someone@nypl.org")

        # If domains are set, the admin's domain can match one of the domains
        # if the admin doesn't exist yet.
        auth_integration.libraries += [self._default_library]
        setting = ConfigurationSetting.for_library_and_externalintegration(
            self._db, "domains", self._default_library, auth_integration)
        setting.value = json.dumps(["nypl.org"])
        assert True == google.staff_email(self._db, "admin@nypl.org")
        assert True == google.staff_email(self._db, "admin@bklynlibrary.org")
        assert True == google.staff_email(self._db, "someone@nypl.org")
        assert False == google.staff_email(self._db, "someone@bklynlibrary.org")

        setting.value = json.dumps(["nypl.org", "bklynlibrary.org"])
        assert True == google.staff_email(self._db, "admin@nypl.org")
        assert True == google.staff_email(self._db, "admin@bklynlibrary.org")
        assert True == google.staff_email(self._db, "someone@nypl.org")
        assert True == google.staff_email(self._db, "someone@bklynlibrary.org")

