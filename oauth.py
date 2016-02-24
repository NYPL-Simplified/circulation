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
from problem_details import INVALID_ADMIN_CREDENTIALS
from oauth2client import client as GoogleClient

class GoogleAuthService(object):

    AUTHENTICATION_RESPONSE_TEMPLATE = """
    <oauthResponse xmlns="http://librarysimplified.org/terms">
        <authorizationIdentifier>%(auth_id)s</authorizationIdentifier>
    </oauthResponse>
    """

    def __init__(self, _db, redirect_uri, test_mode=False):
        self._db = _db
        if not test_mode:
            self.client = GoogleClient.flow_from_clientsecrets(
                '../nypl_config/client_secret.json',
                scope='https://www.googleapis.com/auth/userinfo.email',
                redirect_uri=redirect_uri
            )

    def signin(self, request):
        """Google Oauth sign-in flow"""

        # The Google Oauth client sometimes hits the callback with an error.
        # These will be returned as a problem detail.
        error = request.get('error')
        if error:
            return pd(
                "http://librarysimplified.org/terms/problem/google-oauth-error",
                400,
                "Google Oauth Error",
                "There was an error connecting with Google Oauth: %s" % error,
            )

        # If the client sends an authorization id to us, we can check to
        # confirm we already have details for that user.
        email = request.get('auth_id')
        if email:
            if self.existing_credentials(email):
                return self.AUTHENTICATION_RESPONSE_TEMPLATE % dict(
                    auth_id = email
                )

        auth_code = request.get('code')
        if auth_code:
            credentials = self.client.step2_exchange(auth_code)
            email_domain = credentials.id_token.get('hd')
            if email_domain and email_domain == "nypl.org":
                admin = self.create_admin(credentials)
                return self.AUTHENTICATION_RESPONSE_TEMPLATE % dict(
                    auth_id = admin.authorization_identifier
                )
            else:
                return INVALID_ADMIN_CREDENTIALS

        return redirect(self.client.step1_get_authorize_url())

    def existing_credentials(self, email):
        """Check for existing credentials"""

        admin = get_one(self._db, Admin, authorization_identifier=email)
        if admin and admin.credentials:
            # Use the credentials if they're not expired.
            oauth_credentials = self._build_oauth_credentials(admin.credentials)
            return oauth_credentials.access_token_expired == False
        return False

    def _build_oauth_credentials(self, credentials):
        """Builds our saved credentials into a Oauth2Credentials object"""
        credentials = json.loads(credentials.as_json)
        return self.client.Oauth2Credentials.from_json(credentials)

    def create_admin(self, credentials):
        credentials_str = json.dumps(credentials.to_json())
        admin, ignore = get_one_or_create(
            self._db, Admin,
            authorization_identifier=credentials.id_token.get('email'),
            credential=credentials_str,
        )
        return admin

