import json
from nose.tools import (
    eq_,
    set_trace,
)

from . import DatabaseTest
from ..core.model import Admin
from ..core.util.problem_detail import ProblemDetail

from ..oauth import GoogleAuthService


class DummyGoogleClient(object):
    """Mock Google Oauth client for testing"""

    class Credentials(object):
        """Mock Oauth2Credentials object for testing"""

        def __init__(self, email):
            domain = email[email.index('@')+1:]
            self.id_token = {"hd" : domain, "email" : email}

        def to_json(self):
            return json.loads('{"id_token" : %s }' % json.dumps(self.id_token))

    def __init__(self, email='example@nypl.org'):
        self.credentials = self.Credentials(email=email)
        self.Oauth2Credentials = self.credentials

    def flow_from_client_secrets(self, config, scope=None, redirect_uri=None):
        return self

    def step2_exchange(self, auth_code):
        return self.credentials

class TestGoogleAuthService(DatabaseTest):
    def setup(self):
        super(TestGoogleAuthService, self).setup()
        self.google = GoogleAuthService(self._db, "", test_mode=True)
        self.google.client = DummyGoogleClient()

    def test_signin(self):
        # There are no admins or credentials in the database.
        admins = self._db.query(Admin)
        eq_(0, len(admins.all()))

        # Returns a problem detail when an email doesn't have authorization.
        self.google.client = DummyGoogleClient(email='broken@example.com')
        invalid_response = self.google.signin({'code' : 'abc'})
        eq_(0, len(admins.all()))
        eq_(True, isinstance(invalid_response, ProblemDetail))
        eq_(401, invalid_response.status_code)
        eq_("Invalid administrative credentials", invalid_response.title)

        # Returns a problem detail when Google returns an error.
        self.google.client = DummyGoogleClient()
        error_response = self.google.signin({'error' : 'access_denied'})
        eq_(0, len(admins.all()))
        eq_(True, isinstance(error_response, ProblemDetail))
        eq_(400, error_response.status_code)
        eq_(True, error_response.detail.endswith('access_denied'))

        # Successful case creates and admin with credentials
        success_response = self.google.signin({'code' : 'abc'})
        eq_(1, len(admins.all()))
        assert "example@nypl.org" in admins.all()[0].credential
        assert "<authorizationIdentifier>" in success_response
        assert "example@nypl.org" in success_response
