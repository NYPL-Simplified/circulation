import json
from collections import defaultdict

from api.admin.template_styles import *
from .admin_authentication_provider import AdminAuthenticationProvider
from .problem_details import GOOGLE_OAUTH_FAILURE, INVALID_ADMIN_CREDENTIALS
from oauth2client import client as GoogleClient
from flask_babel import lazy_gettext as _
from core.model import (
    Admin,
    AdminRole,
    ConfigurationSetting,
    ExternalIntegration,
    Session,
    get_one,
)

class GoogleOAuthAdminAuthenticationProvider(AdminAuthenticationProvider):

    NAME = ExternalIntegration.GOOGLE_OAUTH
    DESCRIPTION = _("How to Configure a Google OAuth Integration")
    DOMAINS = "domains"

    INSTRUCTIONS = _("<p>Configuring a Google OAuth integration in the Circulation Manager " +
                    "will allow admins to sign into the Admin interface with their Google/GMail credentials.</p>" +
                    "<p>Configure the Google OAuth Service: </p>" +
                    "<ol><li>To use this integration, visit the " +
                    "<a href='https://console.developers.google.com/apis/dashboard?pli=1' rel='noopener' " +
                    "rel='noreferer' target='_blank'>Google developer console.</a> " +
                    "Create a project, click 'Create Credentials' in the left sidebar, and select 'OAuth client ID'. " +
                    "If you get a warning about the consent screen, click 'Configure consent screen' and enter your " +
                    "library name as the product name. Save the consent screen information.</li>" +
                    "<li>Choose 'Web Application' as the application type.</li>" +
                    "<li>Leave 'Authorized JavaScript origins' blank, but under 'Authorized redirect URIs', add the url " +
                    "of your circulation manager followed by '/admin/GoogleAuth/callback', e.g. " +
                    "'http://mycircmanager.org/admin/GoogleAuth/callback'.</li>"
                    "<li>Click create, and you'll get a popup with your new client ID and secret. " +
                    "Copy these values and enter them in the form below.</li></ol>")


    SETTINGS = [
        {
          "key": ExternalIntegration.URL,
          "label": _("Authentication URI"),
          "default": "https://accounts.google.com/o/oauth2/auth",
          "required": True,
          "format": "url",
        },
        { "key": ExternalIntegration.USERNAME, "label": _("Client ID"), "required": True },
        { "key": ExternalIntegration.PASSWORD, "label": _("Client Secret"), "required": True },
    ]

    LIBRARY_SETTINGS = [
        { "key": DOMAINS,
          "label": _("Allowed Domains"),
          "description": _("Anyone who logs in with an email address from one of these domains will automatically have librarian-level access to this library. Library manager roles must still be granted individually by other admins. If you want to set up admins individually but still allow them to log in with Google, you can create the admin authentication service without adding any libraries."),
          "type": "list" },
    ]
    SITEWIDE = True

    TEMPLATE = """
        <a style='{}' href=%(auth_uri)s>Sign in with Google</a>
    """.format(link_style)

    def __init__(self, integration, redirect_uri, test_mode=False):
        super(GoogleOAuthAdminAuthenticationProvider, self).__init__(integration)
        self.redirect_uri = redirect_uri
        self.test_mode = test_mode
        if self.test_mode:
            self.dummy_client = DummyGoogleClient()

    @property
    def client(self):
        if self.test_mode:
            return self.dummy_client

        config = dict()
        config["auth_uri"] = self.integration.url
        config["client_id"] = self.integration.username
        config["client_secret"] = self.integration.password
        config['redirect_uri'] = self.redirect_uri
        config['scope'] = "https://www.googleapis.com/auth/userinfo.email"
        return GoogleClient.OAuth2WebServerFlow(**config)

    @property
    def domains(self):
        domains = defaultdict(list)
        if self.integration:
            _db = Session.object_session(self.integration)
            for library in self.integration.libraries:
                setting = ConfigurationSetting.for_library_and_externalintegration(
                    _db, self.DOMAINS, library, self.integration)
                if setting.json_value:
                    for domain in setting.json_value:
                        domains[domain.lower()].append(library)
        return domains

    def sign_in_template(self, redirect_url):
        return self.TEMPLATE % dict(auth_uri = self.auth_uri(redirect_url))

    def auth_uri(self, redirect_url):
        return self.client.step1_get_authorize_url(state=redirect_url)

    def callback(self, _db, request={}):
        """Google OAuth sign-in flow"""

        # The Google OAuth client sometimes hits the callback with an error.
        # These will be returned as a problem detail.
        error = request.get('error')
        if error:
            return self.google_error_problem_detail(error), None
        auth_code = request.get('code')
        if auth_code:
            redirect_url = request.get("state")
            try:
                credentials = self.client.step2_exchange(auth_code)
            except GoogleClient.FlowExchangeError as e:
                return self.google_error_problem_detail(str(e)), None
            email = credentials.id_token.get('email')
            if not self.staff_email(_db, email):
                return INVALID_ADMIN_CREDENTIALS, None
            domain = email[email.index('@')+1:].lower()
            roles = []
            for library in self.domains[domain]:
                roles.append({ "role": AdminRole.LIBRARIAN, "library": library.short_name })
            return dict(
                email=email,
                credentials=credentials.to_json(),
                type=self.NAME,
                roles=roles,
            ), redirect_url

    def google_error_problem_detail(self, error):
        error_detail = _("Error: %(error)s", error=error)

        # ProblemDetail.detailed requires the detail to be an internationalized
        # string, so pass the combined string through _ as well even though the
        # components were translated already. Space is a variable so it doesn't
        # end up in the translation template.
        space = " "
        error_detail = _(str(GOOGLE_OAUTH_FAILURE.detail) + space + str(error_detail))

        return GOOGLE_OAUTH_FAILURE.detailed(error_detail)

    def active_credentials(self, admin):
        """Check that existing credentials aren't expired"""

        if admin.credential:
            oauth_credentials = GoogleClient.OAuth2Credentials.from_json(admin.credential)
            return not oauth_credentials.access_token_expired
        return False

    def staff_email(self, _db, email):
        # If the admin already exists in the database, they can log in regardless of
        # whether their domain has been whitelisted for a library.
        admin = get_one(_db, Admin, email=email)
        if admin:
            return True

        # Otherwise, their email must match one of the configured domains.
        staff_domains = list(self.domains.keys())
        domain = email[email.index('@')+1:]
        return domain.lower() in [staff_domain.lower() for staff_domain in staff_domains]

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
            return json.dumps(dict(id_token=self.id_token))

        def from_json(self, credentials):
            return self

    def __init__(self, email='example@nypl.org'):
        self.credentials = self.Credentials(email=email)
        self.OAuth2Credentials = self.credentials

    def flow_from_client_secrets(self, config, scope=None, redirect_uri=None):
        return self

    def step2_exchange(self, auth_code):
        return self.credentials

    def step1_get_authorize_url(self, state):
        return "GOOGLE REDIRECT"
