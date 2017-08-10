import json
from nose.tools import (
    eq_,
    set_trace,
)

from .. import DatabaseTest
from core.util.problem_detail import ProblemDetail

from api.admin.google_oauth_admin_authentication_provider import (
    GoogleOAuthAdminAuthenticationProvider,
    DummyGoogleClient,
)
from core.model import (
    ExternalIntegration,
    create,
)

class TestGoogleOAuthAdminAuthenticationProvider(DatabaseTest):

    def test_callback(self):
        super(TestGoogleOAuthAdminAuthenticationProvider, self).setup()
        auth_integration, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.GOOGLE_OAUTH,
            goal=ExternalIntegration.ADMIN_AUTH_GOAL
        )
        self.google = GoogleOAuthAdminAuthenticationProvider(auth_integration, "", test_mode=True)

        # Returns a problem detail when Google returns an error.
        error_response, redirect = self.google.callback({'error' : 'access_denied'})
        eq_(True, isinstance(error_response, ProblemDetail))
        eq_(400, error_response.status_code)
        eq_(True, error_response.detail.endswith('access_denied'))
        eq_(None, redirect)

        # Successful case creates a dict of admin details
        success, redirect = self.google.callback({'code' : 'abc'})
        eq_('example@nypl.org', success['email'])
        default_credentials = json.dumps({"id_token": {"email": "example@nypl.org", "hd": "nypl.org"}})
        eq_(default_credentials, success['credentials'])

    def test_domains(self):
        super(TestGoogleOAuthAdminAuthenticationProvider, self).setup()
        auth_integration, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.GOOGLE_OAUTH,
            goal=ExternalIntegration.ADMIN_AUTH_GOAL
        )
        auth_integration.set_setting("domains", json.dumps(["nypl.org"]))
        
        google = GoogleOAuthAdminAuthenticationProvider(auth_integration, "", test_mode=True)

        eq_(["nypl.org"], google.domains)
