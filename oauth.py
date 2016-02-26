import re
import json
import datetime
from flask import url_for, redirect
from nose.tools import set_trace

# from core.config import Configuration
from core.model import (
    get_one,
    get_one_or_create,
    Admin,
)
from core.util.problem_detail import ProblemDetail as pd
from oauth2client import client as GoogleClient

class GoogleAuthService(object):

    def __init__(self, _db, redirect_uri, test_mode=False):
        self._db = _db
        if test_mode:
            self.client = DummyGoogleClient()
        else:
            self.client = GoogleClient.flow_from_clientsecrets(
                '../nypl_config/client_secret.json',
                scope='https://www.googleapis.com/auth/userinfo.email',
                redirect_uri=redirect_uri
            )

    @property
    def auth_uri(self):
        return self.client.step1_get_authorize_url()

    def callback(self, request={}):
        """Google Oauth sign-in flow"""

        # The Google Oauth client sometimes hits the callback with an error.
        # These will be returned as a problem detail.
        error = request.get('error')
        if error:
            return self.google_error_problem_detail(error)
        auth_code = request.get('code')
        if auth_code:
            credentials = self.client.step2_exchange(auth_code)
            return dict(
                email_domain=credentials.id_token.get('hd'),
                email=credentials.id_token.get('email'),
                access_token=credentials.get_access_token()[0],
                credentials=json.dumps(credentials.to_json()),
            )

    def google_error_problem_detail(self, error):
        detail = "There was an error connecting with Google Oauth: %s" % error
        return pd(
            "http://librarysimplified.org/terms/problem/google-oauth-error",
            400,
            "Google Oauth Error",
            detail,
        )

    def active_credentials(self, admin):
        """Check that existing credentials aren't expired"""

        if admin.credential:
            credentials = json.loads(admin.credential)
            oauth_credentials = self.client.Oauth2Credentials.from_json(credentials)
            return not oauth_credentials.access_token_expired
        return False


class DummyGoogleClient(object):
    """Mock Google Oauth client for testing"""

    expired = False

    class Credentials(object):
        """Mock Oauth2Credentials object for testing"""

        access_token_expired = False

        def __init__(self, email):
            domain = email[email.index('@')+1:]
            self.id_token = {"hd" : domain, "email" : email}

        def to_json(self):
            return json.loads('{"id_token" : %s }' % json.dumps(self.id_token))

        def from_json(self, credentials):
            return self

    def __init__(self, email='example@nypl.org'):
        self.credentials = self.Credentials(email=email)
        self.Oauth2Credentials = self.credentials

    def flow_from_client_secrets(self, config, scope=None, redirect_uri=None):
        return self

    def step2_exchange(self, auth_code):
        return self.credentials
