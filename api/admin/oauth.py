import json
from nose.tools import set_trace

from problem_details import GOOGLE_OAUTH_FAILURE
from oauth2client import client as GoogleClient
from flask.ext.babel import lazy_gettext as _

class GoogleAuthService(object):

    def __init__(self, integration, redirect_uri, test_mode=False):
        self.integration = integration
        self.redirect_uri = redirect_uri
        self.test_mode = test_mode

    @property
    def client(self):
        if self.test_mode:
            return DummyGoogleClient()

        config = dict()
        config["auth_uri"] = self.integration.url
        config["client_id"] = self.integration.username
        config["client_secret"] = self.integration.password
        config['redirect_uri'] = self.redirect_uri
        config['scope'] = "https://www.googleapis.com/auth/userinfo.email"
        return GoogleClient.OAuth2WebServerFlow(**config)

    @property
    def domains(self):
        if self.integration and self.integration.setting("domains").value:
            return json.loads(self.integration.setting("domains").value)
        return []

    def auth_uri(self, redirect_url):
        return self.client.step1_get_authorize_url(state=redirect_url)

    def callback(self, request={}):
        """Google OAuth sign-in flow"""

        # The Google OAuth client sometimes hits the callback with an error.
        # These will be returned as a problem detail.
        error = request.get('error')
        if error:
            return self.google_error_problem_detail(error)
        auth_code = request.get('code')
        if auth_code:
            redirect_url = request.get("state")
            credentials = self.client.step2_exchange(auth_code)
            return dict(
                email=credentials.id_token.get('email'),
                access_token=credentials.get_access_token()[0],
                credentials=credentials.to_json(),
            ), redirect_url

    def google_error_problem_detail(self, error):
        error_detail = _("Error: %(error)s", error=error)

        # ProblemDetail.detailed requires the detail to be an internationalized
        # string, so pass the combined string through _ as well even though the
        # components were translated already. Space is a variable so it doesn't
        # end up in the translation template.
        space = " "
        error_detail = _(unicode(GOOGLE_OAUTH_FAILURE.detail) + space + unicode(error_detail))

        return GOOGLE_OAUTH_FAILURE.detailed(error_detail)

    def active_credentials(self, admin):
        """Check that existing credentials aren't expired"""

        if admin.credential:
            oauth_credentials = GoogleClient.OAuth2Credentials.from_json(admin.credential)
            return not oauth_credentials.access_token_expired
        return False


class DummyGoogleClient(object):
    """Mock Google OAuth client for testing"""

    expired = False

    class Credentials(object):
        """Mock OAuth2Credentials object for testing"""

        access_token_expired = False

        def __init__(self, email):
            domain = email[email.index('@')+1:]
            self.id_token = {"hd" : domain, "email" : email}

        def to_json(self):
            return json.loads('{"id_token" : %s }' % json.dumps(self.id_token))

        def from_json(self, credentials):
            return self

        def get_access_token(self):
            return ["opensesame"]

    def __init__(self, email='example@nypl.org'):
        self.credentials = self.Credentials(email=email)
        self.OAuth2Credentials = self.credentials

    def flow_from_client_secrets(self, config, scope=None, redirect_uri=None):
        return self

    def step2_exchange(self, auth_code):
        return self.credentials

    def step1_get_authorize_url(self, state):
        return "GOOGLE REDIRECT"
