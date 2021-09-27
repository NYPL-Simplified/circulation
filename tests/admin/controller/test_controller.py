import csv
import datetime
import json
import re
from io import StringIO
from contextlib import contextmanager
from datetime import timedelta

import feedparser
import flask
import pytest
from werkzeug.datastructures import MultiDict
from werkzeug.http import dump_cookie

from api.admin.controller import (
    setup_admin_controllers,
    AdminAnnotator,
    SettingsController,
    PatronController
)
from api.admin.exceptions import (
    ADMIN_AUTH_MECHANISM_NOT_CONFIGURED,
    ADMIN_AUTH_NOT_CONFIGURED,
    CANNOT_CHANGE_LIBRARY_FOR_CUSTOM_LIST,
    CANNOT_EDIT_DEFAULT_LANE,
    CANNOT_SHOW_LANE_WITH_HIDDEN_PARENT,
    COLLECTION_NOT_ASSOCIATED_WITH_LIBRARY,
    CUSTOM_LIST_NAME_ALREADY_IN_USE,
    INVALID_ADMIN_CREDENTIALS,
    LANE_WITH_PARENT_AND_DISPLAY_NAME_ALREADY_EXISTS,
    MISSING_COLLECTION,
    MISSING_CUSTOM_LIST,
    MISSING_LANE,
    NO_CUSTOM_LISTS_FOR_LANE,
    NO_DISPLAY_NAME_FOR_LANE,
    NO_PROTOCOL_FOR_NEW_SERVICE,
    NO_SUCH_PATRON,
    UNKNOWN_PROTOCOL,
    AdminNotAuthorized,
)
from api.admin.google_oauth_admin_authentication_provider import GoogleOAuthAdminAuthenticationProvider
from api.admin.password_admin_authentication_provider import PasswordAdminAuthenticationProvider
from api.admin.problem_details import (
    DUPLICATE_INTEGRATION,
    INTEGRATION_URL_ALREADY_IN_USE,
    INVALID_EMAIL,
)
from api.admin.routes import setup_admin
from api.admin.validator import Validator
from api.adobe_vendor_id import (
    AdobeVendorIDModel
)
from api.adobe_vendor_id import AuthdataUtility
from api.authenticator import (
    PatronData,
)
from api.axis import (Axis360API, MockAxis360API)
from api.config import (
    Configuration,
)
from core.classifier import (
    genres
)
from core.lane import Lane
from core.model import (
    Admin,
    AdminRole,
    CirculationEvent,
    Complaint,
    ConfigurationSetting,
    CustomList,
    CustomListEntry,
    create,
    DataSource,
    Edition,
    ExternalIntegration,
    Genre,
    get_one,
    get_one_or_create,
    Library,
    Timestamp,
    WorkGenre
)
from core.opds_import import (OPDSImporter, OPDSImportMonitor)
from core.s3 import S3UploaderConfiguration
from core.selftest import HasSelfTests
from core.util.datetime_helpers import utc_now
from core.util.http import HTTP
from tests.test_controller import CirculationControllerTest


class AdminControllerTest(CirculationControllerTest):

    # Automatically creating books before the test wastes time -- we don't need them.
    BOOKS = []

    def setup_method(self):
        super(AdminControllerTest, self).setup_method()
        ConfigurationSetting.sitewide(self._db, Configuration.SECRET_KEY).value = "a secret"
        setup_admin(self._db)
        setup_admin_controllers(self.manager)
        (self.admin, _) = create(self._db, Admin, email='example@nypl.org')
        self.admin.password = "password"

    @contextmanager
    def request_context_with_admin(self, route, *args, **kwargs):
        admin = self.admin
        if 'admin' in kwargs:
            admin = kwargs.pop('admin')
        with self.app.test_request_context(route, *args, **kwargs) as c:
            flask.request.form = {}
            flask.request.files = {}
            self._db.begin_nested()
            flask.request.admin = admin
            yield c
            self._db.commit()

    @contextmanager
    def request_context_with_library_and_admin(self, route, *args, **kwargs):
        admin = self.admin
        if 'admin' in kwargs:
            admin = kwargs.pop('admin')
        with self.request_context_with_library(route, *args, **kwargs) as c:
            flask.request.form = {}
            flask.request.files = {}
            self._db.begin_nested()
            flask.request.admin = admin
            yield c
            self._db.commit()


class TestViewController(AdminControllerTest):

    def test_setting_up(self):
        # Test that the view is in setting-up mode if there's no auth service
        # and no admin with a password.
        self.admin.password_hashed = None

        with self.app.test_request_context('/admin'):
            response = self.manager.admin_view_controller(None, None)
            assert 200 == response.status_code
            html = response.get_data(as_text=True)
            assert 'settingUp: true' in html

    def test_not_setting_up(self):
        with self.app.test_request_context('/admin'):
            flask.session['admin_email'] = self.admin.email
            flask.session['auth_type'] = PasswordAdminAuthenticationProvider.NAME
            response = self.manager.admin_view_controller("collection", "book")
            assert 200 == response.status_code
            html = response.get_data(as_text=True)
            assert 'settingUp: false' in html

    def test_redirect_to_sign_in(self):
        with self.app.test_request_context('/admin/web/collection/a/(b)/book/c/(d)'):
            response = self.manager.admin_view_controller("a/(b)", "c/(d)")
            assert 302 == response.status_code
            location = response.headers.get("Location")
            assert "sign_in" in location
            assert "admin%2Fweb" in location
            assert "collection%2Fa%252F%2528b%2529" in location
            assert "book%2Fc%252F%2528d%2529" in location

    def test_redirect_to_library(self):
        # If the admin doesn't have access to any libraries, they get a message
        # instead of a redirect.
        with self.app.test_request_context('/admin'):
            flask.session['admin_email'] = self.admin.email
            flask.session['auth_type'] = PasswordAdminAuthenticationProvider.NAME
            response = self.manager.admin_view_controller(None, None)
            assert 200 == response.status_code
            assert "Your admin account doesn't have access to any libraries" in response.get_data(as_text=True)

        # Unless there aren't any libraries yet. In that case, an admin needs to
        # get in to create one.
        for library in self._db.query(Library):
            self._db.delete(library)
        with self.app.test_request_context('/admin'):
            flask.session['admin_email'] = self.admin.email
            flask.session['auth_type'] = PasswordAdminAuthenticationProvider.NAME
            response = self.manager.admin_view_controller(None, None)
            assert 200 == response.status_code
            assert "<body>" in response.get_data(as_text=True)

        l1 = self._library(short_name="L1")
        l2 = self._library(short_name="L2")     # noqa: F841
        l3 = self._library(short_name="L3")
        self.admin.add_role(AdminRole.LIBRARIAN, l1)
        self.admin.add_role(AdminRole.LIBRARY_MANAGER, l3)
        # An admin with roles gets redirected to the oldest library they have access to.
        with self.app.test_request_context('/admin'):
            flask.session['admin_email'] = self.admin.email
            flask.session['auth_type'] = PasswordAdminAuthenticationProvider.NAME
            response = self.manager.admin_view_controller(None, None)
            assert 302 == response.status_code
            location = response.headers.get("Location")
            assert "admin/web/collection/%s" % l1.short_name in location

        # Only the root url redirects - a non-library specific page with another
        # path won't.
        with self.app.test_request_context('/admin/web/config'):
            flask.session['admin_email'] = self.admin.email
            flask.session['auth_type'] = PasswordAdminAuthenticationProvider.NAME
            response = self.manager.admin_view_controller(None, None, "config")
            assert 200 == response.status_code

    def test_csrf_token(self):
        self.admin.password_hashed = None
        with self.app.test_request_context('/admin'):
            response = self.manager.admin_view_controller(None, None)
            assert 200 == response.status_code
            html = response.get_data(as_text=True)

            # The CSRF token value is random, but the cookie and the html have the same value.
            html_csrf_re = re.compile('csrfToken: \"([^\"]*)\"')
            match = html_csrf_re.search(html)
            assert match is not None
            csrf = match.groups(0)[0]
            assert csrf in response.headers.get('Set-Cookie')
            assert 'HttpOnly' in response.headers.get("Set-Cookie")

        self.admin.password = "password"
        # If there's a CSRF token in the request cookie, the response
        # should keep that same token.
        token = self._str
        cookie = dump_cookie("csrf_token", token)
        with self.app.test_request_context('/admin', environ_base={'HTTP_COOKIE': cookie}):
            flask.session['admin_email'] = self.admin.email
            flask.session['auth_type'] = PasswordAdminAuthenticationProvider.NAME
            response = self.manager.admin_view_controller("collection", "book")
            assert 200 == response.status_code
            html = response.get_data(as_text=True)
            assert 'csrfToken: "%s"' % token in html
            assert token in response.headers.get('Set-Cookie')

    def test_tos_link(self):

        def assert_tos(expect_href, expect_text):
            with self.app.test_request_context('/admin'):
                flask.session['admin_email'] = self.admin.email
                flask.session['auth_type'] = PasswordAdminAuthenticationProvider.NAME
                response = self.manager.admin_view_controller("collection", "book")
                assert 200 == response.status_code
                html = response.get_data(as_text=True)

                assert ('tos_link_href: "%s",' % expect_href) in html
                assert ('tos_link_text: "%s",' % expect_text) in html

        # First, verify the default values, which very few circulation
        # managers will have any need to change.
        #
        # The default value has an apostrophe in it, which gets
        # escaped when the HTML is generated.
        assert_tos(
            Configuration.DEFAULT_TOS_HREF,
            Configuration.DEFAULT_TOS_TEXT.replace("'", "&#39;"),
        )

        # Now set some custom values.
        sitewide = ConfigurationSetting.sitewide
        sitewide(self._db, Configuration.CUSTOM_TOS_HREF).value = "http://tos/"
        sitewide(self._db, Configuration.CUSTOM_TOS_TEXT).value = "a tos"

        # Verify that those values are picked up and used to build the page.
        assert_tos("http://tos/", "a tos")

    def test_show_circ_events_download(self):
        # The local analytics provider will be configured by default if
        # there isn't one.
        with self.app.test_request_context('/admin'):
            flask.session['admin_email'] = self.admin.email
            flask.session['auth_type'] = PasswordAdminAuthenticationProvider.NAME
            response = self.manager.admin_view_controller("collection", "book")
            assert 200 == response.status_code
            html = response.get_data(as_text=True)
            assert 'showCircEventsDownload: true' in html

    def test_roles(self):
        self.admin.add_role(AdminRole.SITEWIDE_LIBRARIAN)
        self.admin.add_role(AdminRole.LIBRARY_MANAGER, self._default_library)
        with self.app.test_request_context('/admin'):
            flask.session['admin_email'] = self.admin.email
            flask.session['auth_type'] = PasswordAdminAuthenticationProvider.NAME
            response = self.manager.admin_view_controller("collection", "book")
            assert 200 == response.status_code
            html = response.get_data(as_text=True)
            assert "\"role\": \"librarian-all\"" in html
            assert "\"role\": \"manager\", \"library\": \"%s\"" % self._default_library.short_name in html


class TestAdminCirculationManagerController(AdminControllerTest):
    def test_require_system_admin(self):
        with self.request_context_with_admin('/admin'):
            pytest.raises(AdminNotAuthorized,
                          self.manager.admin_work_controller.require_system_admin)

            self.admin.add_role(AdminRole.SYSTEM_ADMIN)
            self.manager.admin_work_controller.require_system_admin()

    def test_require_sitewide_library_manager(self):
        with self.request_context_with_admin('/admin'):
            pytest.raises(AdminNotAuthorized,
                          self.manager.admin_work_controller.require_sitewide_library_manager)

            self.admin.add_role(AdminRole.SITEWIDE_LIBRARY_MANAGER)
            self.manager.admin_work_controller.require_sitewide_library_manager()

    def test_require_library_manager(self):
        with self.request_context_with_admin('/admin'):
            pytest.raises(AdminNotAuthorized,
                          self.manager.admin_work_controller.require_library_manager,
                          self._default_library)

            self.admin.add_role(AdminRole.LIBRARY_MANAGER, self._default_library)
            self.manager.admin_work_controller.require_library_manager(self._default_library)

    def test_require_librarian(self):
        with self.request_context_with_admin('/admin'):
            pytest.raises(AdminNotAuthorized,
                          self.manager.admin_work_controller.require_librarian,
                          self._default_library)

            self.admin.add_role(AdminRole.LIBRARIAN, self._default_library)
            self.manager.admin_work_controller.require_librarian(self._default_library)


class TestSignInController(AdminControllerTest):

    def setup_method(self):
        super(TestSignInController, self).setup_method()
        self.admin.credential = json.dumps({
            'access_token': 'abc123',
            'client_id': '', 'client_secret': '',
            'refresh_token': '', 'token_expiry': '', 'token_uri': '',
            'user_agent': '', 'invalid': ''
        })
        self.admin.password_hashed = None

    def test_admin_auth_providers(self):
        with self.app.test_request_context('/admin'):
            ctrl = self.manager.admin_sign_in_controller

            # An admin exists, but they have no password and there's
            # no auth service set up.
            assert [] == ctrl.admin_auth_providers

            # The auth service exists.
            create(
                self._db, ExternalIntegration,
                protocol=ExternalIntegration.GOOGLE_OAUTH,
                goal=ExternalIntegration.ADMIN_AUTH_GOAL
            )
            assert 1 == len(ctrl.admin_auth_providers)
            assert GoogleOAuthAdminAuthenticationProvider.NAME == ctrl.admin_auth_providers[0].NAME

            # Here's another admin with a password.
            (pw_admin, _) = create(self._db, Admin, email="pw@nypl.org")
            pw_admin.password = "password"
            assert 2 == len(ctrl.admin_auth_providers)
            assert (
                set([GoogleOAuthAdminAuthenticationProvider.NAME, PasswordAdminAuthenticationProvider.NAME]) ==
                set([provider.NAME for provider in ctrl.admin_auth_providers])
            )

            # Only an admin with a password.
            self._db.delete(self.admin)
            assert 2 == len(ctrl.admin_auth_providers)
            assert (
                set([GoogleOAuthAdminAuthenticationProvider.NAME, PasswordAdminAuthenticationProvider.NAME]) ==
                set([provider.NAME for provider in ctrl.admin_auth_providers])
            )

            # No admins. Someone new could still log in with google if domains are
            # configured.
            self._db.delete(pw_admin)
            assert 1 == len(ctrl.admin_auth_providers)
            assert GoogleOAuthAdminAuthenticationProvider.NAME == ctrl.admin_auth_providers[0].NAME

    def test_admin_auth_provider(self):
        with self.app.test_request_context('/admin'):
            ctrl = self.manager.admin_sign_in_controller

            create(
                self._db, ExternalIntegration,
                protocol=ExternalIntegration.GOOGLE_OAUTH,
                goal=ExternalIntegration.ADMIN_AUTH_GOAL
            )

            # We can find a google auth provider.
            auth = ctrl.admin_auth_provider(GoogleOAuthAdminAuthenticationProvider.NAME)
            assert isinstance(auth, GoogleOAuthAdminAuthenticationProvider)

            # But not a password auth provider, since no admin has a password.
            auth = ctrl.admin_auth_provider(PasswordAdminAuthenticationProvider.NAME)
            assert auth is None

            # Here's another admin with a password.
            (pw_admin, _) = create(self._db, Admin, email="pw@nypl.org")
            pw_admin.password = "password"

            # Now we can find both auth providers.
            auth = ctrl.admin_auth_provider(GoogleOAuthAdminAuthenticationProvider.NAME)
            assert isinstance(auth, GoogleOAuthAdminAuthenticationProvider)
            auth = ctrl.admin_auth_provider(PasswordAdminAuthenticationProvider.NAME)
            assert isinstance(auth, PasswordAdminAuthenticationProvider)

    def test_authenticated_admin_from_request(self):
        # Returns an error if there's no admin auth service.
        with self.app.test_request_context('/admin'):
            flask.session['admin_email'] = self.admin.email
            flask.session['auth_type'] = GoogleOAuthAdminAuthenticationProvider.NAME
            response = self.manager.admin_sign_in_controller.authenticated_admin_from_request()
            assert ADMIN_AUTH_NOT_CONFIGURED == response

        # Works once the admin auth service exists.
        create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.GOOGLE_OAUTH,
            goal=ExternalIntegration.ADMIN_AUTH_GOAL
        )
        with self.app.test_request_context('/admin'):
            flask.session['admin_email'] = self.admin.email
            flask.session['auth_type'] = GoogleOAuthAdminAuthenticationProvider.NAME
            response = self.manager.admin_sign_in_controller.authenticated_admin_from_request()
            assert self.admin == response

        # Returns an error if you aren't authenticated.
        with self.app.test_request_context('/admin'):
            # You get back a problem detail when you're not authenticated.
            response = self.manager.admin_sign_in_controller.authenticated_admin_from_request()
            assert 401 == response.status_code
            assert INVALID_ADMIN_CREDENTIALS.detail == response.detail

        # Returns an error if the admin email or auth type is missing from the session.
        with self.app.test_request_context('/admin'):
            flask.session['auth_type'] = GoogleOAuthAdminAuthenticationProvider.NAME
            response = self.manager.admin_sign_in_controller.authenticated_admin_from_request()
            assert 401 == response.status_code
            assert INVALID_ADMIN_CREDENTIALS.detail == response.detail

        with self.app.test_request_context('/admin'):
            flask.session['admin_email'] = self.admin.email
            response = self.manager.admin_sign_in_controller.authenticated_admin_from_request()
            assert 401 == response.status_code
            assert INVALID_ADMIN_CREDENTIALS.detail == response.detail

        # Returns an error if the admin authentication type isn't configured.
        with self.app.test_request_context('/admin'):
            flask.session['admin_email'] = self.admin.email
            flask.session['auth_type'] = PasswordAdminAuthenticationProvider.NAME
            response = self.manager.admin_sign_in_controller.authenticated_admin_from_request()
            assert 400 == response.status_code
            assert ADMIN_AUTH_MECHANISM_NOT_CONFIGURED.detail == response.detail

    def test_authenticated_admin(self):

        # Unset the base URL -- it will be set automatically when we
        # successfully authenticate as an admin.
        base_url = ConfigurationSetting.sitewide(
            self._db, Configuration.BASE_URL_KEY
        )
        base_url.value = None
        assert base_url.value is None

        # Creates a new admin with fresh details.
        new_admin_details = {
            'email': 'admin@nypl.org',
            'credentials': 'gnarly',
            'type': GoogleOAuthAdminAuthenticationProvider.NAME,
            'roles': [{"role": AdminRole.LIBRARY_MANAGER, "library": self._default_library.short_name}],
        }
        with self.app.test_request_context('/admin/sign_in?redirect=foo'):
            flask.request.url = "http://chosen-hostname/admin/sign_in?redirect=foo"
            admin = self.manager.admin_sign_in_controller.authenticated_admin(new_admin_details)
            assert 'admin@nypl.org' == admin.email
            assert 'gnarly' == admin.credential
            [role] = admin.roles
            assert AdminRole.LIBRARY_MANAGER == role.role
            assert self._default_library == role.library

            # Also sets up the admin's flask session.
            assert "admin@nypl.org" == flask.session["admin_email"]
            assert GoogleOAuthAdminAuthenticationProvider.NAME == flask.session["auth_type"]
            assert flask.session.permanent is True

        # The first successfully authenticated admin user automatically
        # sets the site's base URL.
        assert "http://chosen-hostname/" == base_url.value

        # Or overwrites credentials for an existing admin.
        existing_admin_details = {
            'email': 'example@nypl.org',
            'credentials': 'b-a-n-a-n-a-s',
            'type': GoogleOAuthAdminAuthenticationProvider.NAME,
            'roles': [{"role": AdminRole.LIBRARY_MANAGER, "library": self._default_library.short_name}],
        }
        with self.app.test_request_context('/admin/sign_in?redirect=foo'):
            flask.request.url = "http://a-different-hostname/"
            admin = self.manager.admin_sign_in_controller.authenticated_admin(existing_admin_details)
            assert self.admin.id == admin.id
            assert 'b-a-n-a-n-a-s' == self.admin.credential
            # No roles were created since the admin already existed.
            assert [] == admin.roles

        # We already set the site's base URL, and it doesn't get set
        # to a different value just because someone authenticated
        # through a different hostname.
        assert "http://chosen-hostname/" == base_url.value

    def test_admin_signin(self):
        # Returns an error if there's no admin auth service.
        with self.app.test_request_context('/admin/sign_in?redirect=foo'):
            response = self.manager.admin_sign_in_controller.sign_in()
            assert ADMIN_AUTH_NOT_CONFIGURED == response

        create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.GOOGLE_OAUTH,
            goal=ExternalIntegration.ADMIN_AUTH_GOAL
        )

        # Shows the login page if there's an auth service
        # but no signed in admin.
        with self.app.test_request_context('/admin/sign_in?redirect=foo'):
            response = self.manager.admin_sign_in_controller.sign_in()
            assert 200 == response.status_code
            response_data = response.get_data(as_text=True)
            assert "GOOGLE REDIRECT" in response_data
            assert "Sign in with Google" in response_data
            assert "Email" not in response_data
            assert "Password" not in response_data

        # If there are multiple auth providers, the login page
        # shows them all.
        self.admin.password = "password"
        with self.app.test_request_context('/admin/sign_in?redirect=foo'):
            response = self.manager.admin_sign_in_controller.sign_in()
            assert 200 == response.status_code
            response_data = response.get_data(as_text=True)
            assert "GOOGLE REDIRECT" in response_data
            assert "Sign in with Google" in response_data
            assert "Email" in response_data
            assert "Password" in response_data

        # Redirects to the redirect parameter if an admin is signed in.
        with self.app.test_request_context('/admin/sign_in?redirect=foo'):
            flask.session['admin_email'] = self.admin.email
            flask.session['auth_type'] = PasswordAdminAuthenticationProvider.NAME
            response = self.manager.admin_sign_in_controller.sign_in()
            assert 302 == response.status_code
            assert "foo" == response.headers["Location"]

    def test_redirect_after_google_sign_in(self):
        self._db.delete(self.admin)

        # Returns an error if there's no admin auth service.
        with self.app.test_request_context('/admin/GoogleOAuth/callback'):
            response = self.manager.admin_sign_in_controller.redirect_after_google_sign_in()
            assert ADMIN_AUTH_NOT_CONFIGURED == response

        # Returns an error if the admin auth service isn't google.
        (admin, _) = create(self._db, Admin, email="admin@nypl.org")
        admin.password = "password"
        with self.app.test_request_context('/admin/GoogleOAuth/callback'):
            response = self.manager.admin_sign_in_controller.redirect_after_google_sign_in()
            assert ADMIN_AUTH_MECHANISM_NOT_CONFIGURED == response

        self._db.delete(admin)
        (auth_integration, _) = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.GOOGLE_OAUTH,
            goal=ExternalIntegration.ADMIN_AUTH_GOAL
        )
        auth_integration.libraries += [self._default_library]
        setting = ConfigurationSetting.for_library_and_externalintegration(
            self._db, "domains", self._default_library, auth_integration)

        # Returns an error if google oauth fails..
        with self.app.test_request_context('/admin/GoogleOAuth/callback?error=foo'):
            response = self.manager.admin_sign_in_controller.redirect_after_google_sign_in()
            assert 400 == response.status_code

        # Returns an error if the admin email isn't a staff email.
        setting.value = json.dumps(["alibrary.org"])
        with self.app.test_request_context('/admin/GoogleOAuth/callback?code=1234&state=foo'):
            response = self.manager.admin_sign_in_controller.redirect_after_google_sign_in()
            assert 401 == response.status_code

        # Redirects to the state parameter if the admin email is valid.
        setting.value = json.dumps(["nypl.org"])
        with self.app.test_request_context('/admin/GoogleOAuth/callback?code=1234&state=foo'):
            response = self.manager.admin_sign_in_controller.redirect_after_google_sign_in()
            assert 302 == response.status_code
            assert "foo" == response.headers["Location"]

    def test_password_sign_in(self):
        # Returns an error if there's no admin auth service and no admins.
        with self.app.test_request_context('/admin/sign_in_with_password'):
            response = self.manager.admin_sign_in_controller.password_sign_in()
            assert ADMIN_AUTH_NOT_CONFIGURED == response

        # Returns an error if the admin auth service isn't password auth.
        (auth_integration, _) = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.GOOGLE_OAUTH,
            goal=ExternalIntegration.ADMIN_AUTH_GOAL
        )
        with self.app.test_request_context('/admin/sign_in_with_password'):
            response = self.manager.admin_sign_in_controller.password_sign_in()
            assert ADMIN_AUTH_MECHANISM_NOT_CONFIGURED == response

        self._db.delete(auth_integration)
        (admin, _) = create(self._db, Admin, email="admin@nypl.org")
        admin.password = "password"

        # Returns an error if there's no admin with the provided email.
        with self.app.test_request_context('/admin/sign_in_with_password', method='POST'):
            flask.request.form = MultiDict([
                ("email", "notanadmin@nypl.org"),
                ("password", "password"),
                ("redirect", "foo")
            ])
            response = self.manager.admin_sign_in_controller.password_sign_in()
            assert 401 == response.status_code

        # Returns an error if the password doesn't match.
        self.admin.password = "password"
        with self.app.test_request_context('/admin/sign_in_with_password', method='POST'):
            flask.request.form = MultiDict([
                ("email", self.admin.email),
                ("password", "notthepassword"),
                ("redirect", "foo")
            ])
            response = self.manager.admin_sign_in_controller.password_sign_in()
            assert 401 == response.status_code

        # Redirects if the admin email/password combination is valid.
        with self.app.test_request_context('/admin/sign_in_with_password', method='POST'):
            flask.request.form = MultiDict([
                ("email", self.admin.email),
                ("password", "password"),
                ("redirect", "foo")
            ])
            response = self.manager.admin_sign_in_controller.password_sign_in()
            assert 302 == response.status_code
            assert "foo" == response.headers["Location"]

    def test_change_password(self):
        (admin, _) = create(self._db, Admin, email=self._str)
        admin.password = "old"
        with self.request_context_with_admin('/admin/change_password', admin=admin):
            flask.request.form = MultiDict([
                ("password", "new"),
            ])
            response = self.manager.admin_sign_in_controller.change_password()
            assert 200 == response.status_code
            assert admin == Admin.authenticate(self._db, admin.email, "new")
            assert Admin.authenticate(self._db, admin.email, "old") is None

    def test_sign_out(self):
        (admin, _) = create(self._db, Admin, email=self._str)
        admin.password = "pass"
        with self.app.test_request_context('/admin/sign_out'):
            flask.session["admin_email"] = admin.email
            flask.session["auth_type"] = PasswordAdminAuthenticationProvider.NAME
            response = self.manager.admin_sign_in_controller.sign_out()
            assert 302 == response.status_code

            # The admin's credentials have been removed from the session.
            assert flask.session.get("admin_email") is None
            assert flask.session.get("auth_type") is None


class TestPatronController(AdminControllerTest):
    def setup_method(self):
        super(TestPatronController, self).setup_method()
        self.admin.add_role(AdminRole.LIBRARIAN, self._default_library)

    def test__load_patrondata(self):
        """Test the _load_patrondata helper method."""
        class MockAuthenticator(object):
            def __init__(self, providers):
                self.providers = providers

        class MockAuthenticationProvider(object):
            def __init__(self, patron_dict):
                self.patron_dict = patron_dict

            def remote_patron_lookup(self, patrondata):
                return self.patron_dict.get(patrondata.authorization_identifier)

        authenticator = MockAuthenticator([])
        auth_provider = MockAuthenticationProvider({})
        identifier = "Patron"

        form = MultiDict([("identifier", identifier)])
        m = self.manager.admin_patron_controller._load_patrondata

        # User doesn't have admin permission
        with self.request_context_with_library("/"):
            pytest.raises(AdminNotAuthorized, m, authenticator)

        # No form data specified
        with self.request_context_with_library_and_admin("/"):
            response = m(authenticator)
            assert 404 == response.status_code
            assert NO_SUCH_PATRON.uri == response.uri
            assert "Please enter a patron identifier" == response.detail

        # AuthenticationProvider has no Authenticators.
        with self.request_context_with_library_and_admin("/"):
            flask.request.form = form
            response = m(authenticator)

            assert 404 == response.status_code
            assert NO_SUCH_PATRON.uri == response.uri
            assert response.detail == "This library has no authentication providers, so it has no patrons."

        # Authenticator can't find patron with this identifier
        authenticator.providers.append(auth_provider)
        with self.request_context_with_library_and_admin("/"):
            flask.request.form = form
            response = m(authenticator)

            assert 404 == response.status_code
            assert NO_SUCH_PATRON.uri == response.uri
            assert response.detail == "No patron with identifier %s was found at your library" % identifier

    def test_lookup_patron(self):

        # Here's a patron.
        patron = self._patron()
        patron.authorization_identifier = self._str

        # This PatronController will always return information about that
        # patron, no matter what it's asked for.
        class MockPatronController(PatronController):
            def _load_patrondata(self, authenticator):
                self.called_with = authenticator
                return PatronData(
                    authorization_identifier="An Identifier",
                    personal_name="A Patron",
                )

        controller = MockPatronController(self.manager)

        authenticator = object()
        with self.request_context_with_library_and_admin("/"):
            flask.request.form = MultiDict([("identifier", object())])
            response = controller.lookup_patron(authenticator)
            # The authenticator was passed into _load_patrondata()
            assert authenticator == controller.called_with

            # _load_patrondata() returned a PatronData object. We
            # converted it to a dictionary, which will be dumped to
            # JSON on the way out.
            assert "An Identifier" == response['authorization_identifier']
            assert "A Patron" == response['personal_name']

    def test_reset_adobe_id(self):
        # Here's a patron with two Adobe-relevant credentials.
        patron = self._patron()
        patron.authorization_identifier = self._str

        self._credential(
            patron=patron, type=AdobeVendorIDModel.VENDOR_ID_UUID_TOKEN_TYPE
        )
        self._credential(
            patron=patron, type=AuthdataUtility.ADOBE_ACCOUNT_ID_PATRON_IDENTIFIER
        )

        # This PatronController will always return a specific
        # PatronData object, no matter what is asked for.
        class MockPatronController(PatronController):
            mock_patrondata = None

            def _load_patrondata(self, authenticator):
                self.called_with = authenticator
                return self.mock_patrondata

        controller = MockPatronController(self.manager)
        controller.mock_patrondata = PatronData(
            authorization_identifier=patron.authorization_identifier
        )

        # We reset their Adobe ID.
        authenticator = object()
        with self.request_context_with_library_and_admin("/"):
            form = MultiDict([("identifier", patron.authorization_identifier)])
            flask.request.form = form

            response = controller.reset_adobe_id(authenticator)
            assert 200 == response.status_code

            # _load_patrondata was called and gave us information about
            # which Patron to modify.
            controller.called_with = authenticator

        # Both of the Patron's credentials are gone.
        assert patron.credentials == []

        # Here, the AuthenticationProvider finds a PatronData, but the
        # controller can't turn it into a Patron because it's too vague.
        controller.mock_patrondata = PatronData()
        with self.request_context_with_library_and_admin("/"):
            flask.request.form = form
            response = controller.reset_adobe_id(authenticator)

            assert 404 == response.status_code
            assert NO_SUCH_PATRON.uri == response.uri
            assert "Could not create local patron object" in response.detail


class TestTimestampsController(AdminControllerTest):

    def setup_method(self):
        super(TestTimestampsController, self).setup_method()
        for timestamp in self._db.query(Timestamp):
            self._db.delete(timestamp)

        self.collection = self._default_collection
        self.start = utc_now()
        self.finish = utc_now()

        (cp, _) = create(
            self._db, Timestamp,
            service_type="coverage_provider",
            service="test_cp",
            start=self.start,
            finish=self.finish,
            collection=self.collection
        )

        (monitor, _) = create(
            self._db, Timestamp,
            service_type="monitor",
            service="test_monitor",
            start=self.start,
            finish=self.finish,
            collection=self.collection,
            exception="stack trace string"
        )

        (script, _) = create(
            self._db, Timestamp,
            achievements="ran a script",
            service_type="script",
            service="test_script",
            start=self.start,
            finish=self.finish,
        )

        (other, _) = create(
            self._db, Timestamp,
            service="test_other",
            start=self.start,
            finish=self.finish,
        )

    def test_diagnostics_admin_not_authorized(self):
        with self.request_context_with_admin("/"):
            pytest.raises(AdminNotAuthorized, self.manager.timestamps_controller.diagnostics)

    def test_diagnostics(self):
        duration = (self.finish - self.start).total_seconds()

        with self.request_context_with_admin("/"):
            self.admin.add_role(AdminRole.SYSTEM_ADMIN)
            response = self.manager.timestamps_controller.diagnostics()

        assert set(response.keys()) == set(["coverage_provider", "monitor", "script", "other"])

        cp_service = response["coverage_provider"]
        cp_name, cp_collection = list(cp_service.items())[0]
        assert cp_name == "test_cp"
        cp_collection_name, [cp_timestamp] = list(cp_collection.items())[0]
        assert cp_collection_name == self.collection.name
        assert cp_timestamp.get("exception") is None
        assert cp_timestamp.get("start") == self.start
        assert cp_timestamp.get("duration") == duration
        assert cp_timestamp.get("achievements") is None

        monitor_service = response["monitor"]
        monitor_name, monitor_collection = list(monitor_service.items())[0]
        assert monitor_name == "test_monitor"
        monitor_collection_name, [monitor_timestamp] = list(monitor_collection.items())[0]
        assert monitor_collection_name == self.collection.name
        assert monitor_timestamp.get("exception") == "stack trace string"
        assert monitor_timestamp.get("start") == self.start
        assert monitor_timestamp.get("duration") == duration
        assert monitor_timestamp.get("achievements") is None

        script_service = response["script"]
        script_name, script_collection = list(script_service.items())[0]
        assert script_name == "test_script"
        script_collection_name, [script_timestamp] = list(script_collection.items())[0]
        assert script_collection_name == "No associated collection"
        assert script_timestamp.get("exception") is None
        assert script_timestamp.get("duration") == duration
        assert script_timestamp.get("start") == self.start
        assert script_timestamp.get("achievements") == "ran a script"

        other_service = response["other"]
        other_name, other_collection = list(other_service.items())[0]
        assert other_name == "test_other"
        other_collection_name, [other_timestamp] = list(other_collection.items())[0]
        assert other_collection_name == "No associated collection"
        assert other_timestamp.get("exception") is None
        assert other_timestamp.get("duration") == duration
        assert other_timestamp.get("start") == self.start
        assert other_timestamp.get("achievements") is None


class TestFeedController(AdminControllerTest):

    def setup_method(self):
        super(TestFeedController, self).setup_method()
        self.admin.add_role(AdminRole.LIBRARIAN, self._default_library)

    def test_complaints(self):
        type = iter(Complaint.VALID_TYPES)
        type1 = next(type)
        type2 = next(type)

        work1 = self._work("fiction work with complaint 1", language="eng", fiction=True,
                           with_open_access_download=True)
        complaint1 = self._complaint(work1.license_pools[0], type1, "complaint source 1", "complaint detail 1")  # noqa: F841,E501
        complaint2 = self._complaint(work1.license_pools[0], type2, "complaint source 2", "complaint detail 2")  # noqa: F841,E501
        work2 = self._work("nonfiction work with complaint", language="eng", fiction=False,
                           with_open_access_download=True)
        complaint3 = self._complaint(work2.license_pools[0], type1, "complaint source 3", "complaint detail 3")  # noqa: F841,E501

        with self.request_context_with_library_and_admin("/"):
            response = self.manager.admin_feed_controller.complaints()
            feed = feedparser.parse(response.get_data(as_text=True))
            entries = feed['entries']

            assert len(entries) == 2

        self.admin.remove_role(AdminRole.LIBRARIAN, self._default_library)
        with self.request_context_with_library_and_admin("/"):
            pytest.raises(AdminNotAuthorized,
                          self.manager.admin_feed_controller.complaints)

    def test_suppressed(self):
        suppressed_work = self._work(with_open_access_download=True)
        suppressed_work.license_pools[0].suppressed = True

        unsuppressed_work = self._work()        # noqa: F841

        with self.request_context_with_library_and_admin("/"):
            response = self.manager.admin_feed_controller.suppressed()
            feed = feedparser.parse(response.get_data(as_text=True))
            entries = feed['entries']
            assert 1 == len(entries)
            assert suppressed_work.title == entries[0]['title']

        self.admin.remove_role(AdminRole.LIBRARIAN, self._default_library)
        with self.request_context_with_library_and_admin("/"):
            pytest.raises(AdminNotAuthorized,
                          self.manager.admin_feed_controller.suppressed)

    def test_genres(self):
        with self.app.test_request_context("/"):
            response = self.manager.admin_feed_controller.genres()

            for name in genres:
                top = "Fiction" if genres[name].is_fiction else "Nonfiction"
                assert response[top][name] == dict({
                    "name": name,
                    "parents": [parent.name for parent in genres[name].parents],
                    "subgenres": [subgenre.name for subgenre in genres[name].subgenres]
                })


class TestCustomListsController(AdminControllerTest):
    def setup_method(self):
        super(TestCustomListsController, self).setup_method()
        self.admin.add_role(AdminRole.LIBRARIAN, self._default_library)

    def test_custom_lists_get(self):
        # This list has no associated Library and should not be included.
        (no_library, _) = create(self._db, CustomList, name=self._str)

        (one_entry, _) = create(self._db, CustomList, name=self._str, library=self._default_library)
        edition = self._edition()
        one_entry.add_entry(edition)
        collection = self._collection()
        collection.customlists = [one_entry]

        (no_entries, _) = create(self._db, CustomList, name=self._str, library=self._default_library)

        with self.request_context_with_library_and_admin("/"):
            response = self.manager.admin_custom_lists_controller.custom_lists()
            assert 2 == len(response.get("custom_lists"))
            lists = response.get("custom_lists")
            [l1, l2] = sorted(lists, key=lambda l: l.get("id"))

            assert one_entry.id == l1.get("id")
            assert one_entry.name == l1.get("name")
            assert 1 == l1.get("entry_count")
            assert 1 == len(l1.get("collections"))
            [c] = l1.get("collections")
            assert collection.name == c.get("name")
            assert collection.id == c.get("id")
            assert collection.protocol == c.get("protocol")

            assert no_entries.id == l2.get("id")
            assert no_entries.name == l2.get("name")
            assert 0 == l2.get("entry_count")
            assert 0 == len(l2.get("collections"))

        self.admin.remove_role(AdminRole.LIBRARIAN, self._default_library)
        with self.request_context_with_library_and_admin("/"):
            pytest.raises(AdminNotAuthorized,
                          self.manager.admin_custom_lists_controller.custom_lists)

    def test_custom_lists_post_errors(self):
        with self.request_context_with_library_and_admin("/", method='POST'):
            flask.request.form = MultiDict([
                ("id", "4"),
                ("name", "name"),
            ])
            response = self.manager.admin_custom_lists_controller.custom_lists()
            assert MISSING_CUSTOM_LIST == response

        library = self._library()
        data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        (list, _) = create(self._db, CustomList, name=self._str, data_source=data_source)
        list.library = library
        with self.request_context_with_library_and_admin("/", method='POST'):
            flask.request.form = MultiDict([
                ("id", list.id),
                ("name", list.name),
            ])
            response = self.manager.admin_custom_lists_controller.custom_lists()
            assert CANNOT_CHANGE_LIBRARY_FOR_CUSTOM_LIST == response

        (list, _) = create(self._db, CustomList, name=self._str, data_source=data_source, library=self._default_library)
        with self.request_context_with_library_and_admin("/", method='POST'):
            flask.request.form = MultiDict([
                ("name", list.name),
            ])
            response = self.manager.admin_custom_lists_controller.custom_lists()
            assert CUSTOM_LIST_NAME_ALREADY_IN_USE == response

        (l1, _) = create(self._db, CustomList, name=self._str, data_source=data_source, library=self._default_library)
        (l2, _) = create(self._db, CustomList, name=self._str, data_source=data_source, library=self._default_library)
        with self.request_context_with_library_and_admin("/", method='POST'):
            flask.request.form = MultiDict([
                ("id", l2.id),
                ("name", l1.name),
            ])
            response = self.manager.admin_custom_lists_controller.custom_lists()
            assert CUSTOM_LIST_NAME_ALREADY_IN_USE == response

        with self.request_context_with_library_and_admin("/", method='POST'):
            flask.request.form = MultiDict([
                ("name", "name"),
                ("collections", json.dumps([12345])),
            ])
            response = self.manager.admin_custom_lists_controller.custom_lists()
            assert MISSING_COLLECTION == response

        (admin, _) = create(self._db, Admin, email="test@nypl.org")
        library = self._library()
        with self.request_context_with_admin("/", method="POST", admin=admin):
            flask.request.library = library
            flask.request.form = MultiDict([
                ("name", "name"),
                ("collections", json.dumps([])),
            ])
            pytest.raises(AdminNotAuthorized,
                          self.manager.admin_custom_lists_controller.custom_lists)

    def test_custom_lists_post_collection_with_wrong_library(self):
        # This collection is not associated with any libraries.
        collection = self._collection()
        with self.request_context_with_library_and_admin("/", method='POST'):
            flask.request.form = MultiDict([
                ("name", "name"),
                ("collections", json.dumps([collection.id])),
            ])
            response = self.manager.admin_custom_lists_controller.custom_lists()
            assert COLLECTION_NOT_ASSOCIATED_WITH_LIBRARY == response

    def test_custom_lists_create(self):
        work = self._work(with_open_access_download=True)
        collection = self._collection()
        collection.libraries = [self._default_library]

        with self.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "List"),
                ("entries", json.dumps([dict(id=work.presentation_edition.primary_identifier.urn)])),
                ("collections", json.dumps([collection.id])),
            ])

            response = self.manager.admin_custom_lists_controller.custom_lists()
            assert 201 == response.status_code

            [list] = self._db.query(CustomList).all()
            assert list.id == int(response.get_data(as_text=True))
            assert self._default_library == list.library
            assert "List" == list.name
            assert 1 == len(list.entries)
            assert work == list.entries[0].work
            assert work.presentation_edition == list.entries[0].edition
            assert list.entries[0].featured is True
            assert [collection] == list.collections

    def test_custom_list_get(self):
        data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        (list, _) = create(self._db, CustomList, name=self._str, library=self._default_library, data_source=data_source)

        work1 = self._work(with_license_pool=True)
        work2 = self._work(with_license_pool=True)
        list.add_entry(work1)
        list.add_entry(work2)

        with self.request_context_with_library_and_admin("/"):
            response = self.manager.admin_custom_lists_controller.custom_list(list.id)
            feed = feedparser.parse(response.get_data())

            assert list.name == feed.feed.title
            assert 2 == len(feed.entries)

            [self_custom_list_link] = [x['href'] for x in feed.feed['links']
                                       if x['rel'] == "self"]
            assert self_custom_list_link == feed.feed.id

            [entry1, entry2] = feed.entries
            assert work1.title == entry1.get("title")
            assert work2.title == entry2.get("title")

            assert work1.presentation_edition.author == entry1.get("author")
            assert work2.presentation_edition.author == entry2.get("author")

    def test_custom_list_get_errors(self):
        with self.request_context_with_library_and_admin("/"):
            response = self.manager.admin_custom_lists_controller.custom_list(123)
            assert MISSING_CUSTOM_LIST == response

        data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        (list, _) = create(self._db, CustomList, name=self._str, library=self._default_library, data_source=data_source)

        self.admin.remove_role(AdminRole.LIBRARIAN, self._default_library)
        with self.request_context_with_library_and_admin("/"):
            pytest.raises(AdminNotAuthorized,
                          self.manager.admin_custom_lists_controller.custom_list,
                          list.id)

    def test_custom_list_edit(self):
        data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        (list, _) = create(self._db, CustomList, name=self._str, data_source=data_source)
        list.library = self._default_library

        # Create a Lane that depends on this CustomList for its membership.
        lane = self._lane()
        lane.customlists.append(list)
        lane.size = 350

        self.controller.search_engine.docs = {}

        w1 = self._work(title="Alpha", with_license_pool=True, language="eng")
        w2 = self._work(title="Bravo", with_license_pool=True, language="fre")
        w3 = self._work(title="Charlie", with_license_pool=True)
        w2.presentation_edition.medium = Edition.AUDIO_MEDIUM
        w3.presentation_edition.permanent_work_id = w2.presentation_edition.permanent_work_id
        w3.presentation_edition.medium = Edition.BOOK_MEDIUM

        list.add_entry(w1)
        list.add_entry(w2)
        self.controller.search_engine.bulk_update([w1, w2, w3])

        # All three works should be indexed, but only w1 and w2 should be related to the list
        assert len(self.controller.search_engine.docs) == 3
        currently_indexed_on_list = [v['title'] for (k, v)
                                     in self.controller.search_engine.docs.items()
                                     if v['customlists'] is not None]
        assert sorted(currently_indexed_on_list) == ['Alpha', 'Bravo']

        new_entries = [dict(id=work.presentation_edition.primary_identifier.urn,
                            medium=Edition.medium_to_additional_type[work.presentation_edition.medium])
                       for work in [w2, w3]]
        deletedEntries = [dict(id=work.presentation_edition.primary_identifier.urn,
                               medium=Edition.medium_to_additional_type[work.presentation_edition.medium])
                          for work in [w1]]

        c1 = self._collection()
        c1.libraries = [self._default_library]
        c2 = self._collection()
        c2.libraries = [self._default_library]
        list.collections = [c1]
        new_collections = [c2]

        with self.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", str(list.id)),
                ("name", "new name"),
                ("entries", json.dumps(new_entries)),
                ("deletedEntries", json.dumps(deletedEntries)),
                ("collections", json.dumps([c.id for c in new_collections])),
            ])

            response = self.manager.admin_custom_lists_controller.custom_list(list.id)

        # The works associated with the list in ES should have changed, though the total
        # number of documents in the index should be the same.
        assert len(self.controller.search_engine.docs) == 3
        currently_indexed_on_list = [v['title'] for (k, v)
                                     in self.controller.search_engine.docs.items()
                                     if v['customlists'] is not None]
        assert sorted(currently_indexed_on_list) == ['Bravo', 'Charlie']

        assert 200 == response.status_code
        assert list.id == int(response.get_data(as_text=True))

        assert "new name" == list.name
        assert (set([w2, w3]) == set([entry.work for entry in list.entries]))
        assert new_collections == list.collections

        # This change caused an immediate update to lane.size
        # based on information from the mocked search index.
        assert 2 == lane.size

        self.admin.remove_role(AdminRole.LIBRARIAN, self._default_library)
        with self.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", str(list.id)),
                ("name", "another new name"),
                ("entries", json.dumps(new_entries)),
                ("collections", json.dumps([c.id for c in new_collections])),
            ])
            pytest.raises(AdminNotAuthorized,
                          self.manager.admin_custom_lists_controller.custom_list,
                          list.id)

    def test_custom_list_delete_success(self):
        self.admin.add_role(AdminRole.LIBRARY_MANAGER, self._default_library)

        # Create a CustomList with two Works on it.
        library_staff = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        (list, _) = create(
            self._db, CustomList, name=self._str, data_source=library_staff
        )
        list.library = self._default_library

        w1 = self._work(with_license_pool=True)
        w2 = self._work(with_license_pool=True)
        list.add_entry(w1)
        list.add_entry(w2)

        # Whenever the mocked search engine is asked how many
        # works are in a Lane, it will say there are two.
        self.controller.search_engine.docs = dict(id1="doc1", id2="doc2")

        # Create a second CustomList, from another data source,
        # containing a single work.
        nyt = DataSource.lookup(self._db, DataSource.NYT)
        (list2, _) = create(
            self._db, CustomList, name=self._str, data_source=nyt
        )
        list2.library = self._default_library
        list2.add_entry(w2)

        # Create a Lane which takes all of its contents from that
        # CustomList. When the CustomList is deleted, the Lane will
        # have no reason to exist, and it will be automatically
        # deleted as well.
        lane = self._lane(display_name="to be automatically removed")
        lane.customlists.append(list)

        # This Lane is based on two different CustomLists. Its size
        # will be updated when the CustomList is deleted, but the Lane
        # itself will not be deleted, since it's still based on
        # something.
        lane2 = self._lane(display_name="to have size updated")
        lane2.customlists.append(list)
        lane2.customlists.append(list2)
        lane2.size = 100

        # This lane is based on _all_ lists from a given data source.
        # It will also not be deleted when the CustomList is deleted,
        # because other lists from that data source might show up in
        # the future.
        lane3 = self._lane(display_name="All library staff lists")
        lane3.list_datasource = list.data_source
        lane3.size = 150

        with self.request_context_with_library_and_admin("/", method="DELETE"):
            response = self.manager.admin_custom_lists_controller.custom_list(list.id)
            assert 200 == response.status_code

        # The first CustomList and all of its entries have been removed.
        # Only the second one remains.
        assert [list2] == self._db.query(CustomList).all()
        assert list2.entries == self._db.query(CustomListEntry).all()

        # The first lane was automatically removed when it became
        # based on an empty set of CustomLists.
        assert get_one(self._db, Lane, id=lane.id) is None

        # The second and third lanes were not removed, because they
        # weren't based solely on this specific list. But their .size
        # attributes were updated to reflect the removal of the list from
        # the lane.
        #
        # In the context of this test, this means that
        # MockExternalSearchIndex.count_works() was called, and we set
        # it up to always return 2.
        assert 2 == lane2.size
        assert 2 == lane3.size

    def test_custom_list_delete_errors(self):
        data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        (list, _) = create(self._db, CustomList, name=self._str, data_source=data_source)
        with self.request_context_with_library_and_admin("/", method="DELETE"):
            pytest.raises(AdminNotAuthorized,
                          self.manager.admin_custom_lists_controller.custom_list,
                          list.id)

        self.admin.add_role(AdminRole.LIBRARY_MANAGER, self._default_library)
        with self.request_context_with_library_and_admin("/", method="DELETE"):
            response = self.manager.admin_custom_lists_controller.custom_list(123)
            assert MISSING_CUSTOM_LIST == response


class TestLanesController(AdminControllerTest):
    def setup_method(self):
        super(TestLanesController, self).setup_method()
        self.admin.add_role(AdminRole.LIBRARY_MANAGER, self._default_library)

    def test_lanes_get(self):
        library = self._library()
        collection = self._collection()
        library.collections += [collection]

        english = self._lane("English", library=library, languages=["eng"])
        english.priority = 0
        english.size = 44
        english_fiction = self._lane("Fiction", library=library, parent=english, fiction=True)
        english_fiction.visible = False
        english_fiction.size = 33
        english_sf = self._lane("Science Fiction", library=library, parent=english_fiction)
        english_sf.add_genre("Science Fiction")
        english_sf.inherit_parent_restrictions = True
        english_sf.size = 22
        spanish = self._lane("Spanish", library=library, languages=["spa"])
        spanish.priority = 1
        spanish.size = 11

        w1 = self._work(with_license_pool=True, language="eng", genre="Science Fiction", collection=collection)  # noqa: F841,E501
        w2 = self._work(with_license_pool=True, language="eng", fiction=False, collection=collection)            # noqa: F841,E501

        (list, _) = self._customlist(data_source_name=DataSource.LIBRARY_STAFF, num_entries=0)
        list.library = library
        lane_for_list = self._lane("List Lane", library=library)
        lane_for_list.customlists += [list]
        lane_for_list.priority = 2
        lane_for_list.size = 1

        with self.request_context_with_library_and_admin("/"):
            flask.request.library = library
            # The admin is not a librarian for this library.
            pytest.raises(AdminNotAuthorized, self.manager.admin_lanes_controller.lanes)
            self.admin.add_role(AdminRole.LIBRARIAN, library)
            response = self.manager.admin_lanes_controller.lanes()

            assert 3 == len(response.get("lanes"))
            [english_info, spanish_info, list_info] = response.get("lanes")

            assert english.id == english_info.get("id")
            assert english.display_name == english_info.get("display_name")
            assert english.visible == english_info.get("visible")
            assert 44 == english_info.get("count")
            assert [] == english_info.get("custom_list_ids")
            assert english_info.get("inherit_parent_restrictions") is True

            [fiction_info] = english_info.get("sublanes")
            assert english_fiction.id == fiction_info.get("id")
            assert english_fiction.display_name == fiction_info.get("display_name")
            assert english_fiction.visible == fiction_info.get("visible")
            assert 33 == fiction_info.get("count")
            assert [] == fiction_info.get("custom_list_ids")
            assert fiction_info.get("inherit_parent_restrictions") is True

            [sf_info] = fiction_info.get("sublanes")
            assert english_sf.id == sf_info.get("id")
            assert english_sf.display_name == sf_info.get("display_name")
            assert english_sf.visible == sf_info.get("visible")
            assert 22 == sf_info.get("count")
            assert [] == sf_info.get("custom_list_ids")
            assert sf_info.get("inherit_parent_restrictions") is True

            assert spanish.id == spanish_info.get("id")
            assert spanish.display_name == spanish_info.get("display_name")
            assert spanish.visible == spanish_info.get("visible")
            assert 11 == spanish_info.get("count")
            assert [] == spanish_info.get("custom_list_ids")
            assert spanish_info.get("inherit_parent_restrictions") is True

            assert lane_for_list.id == list_info.get("id")
            assert lane_for_list.display_name == list_info.get("display_name")
            assert lane_for_list.visible == list_info.get("visible")
            assert 1 == list_info.get("count")
            assert [list.id] == list_info.get("custom_list_ids")
            assert list_info.get("inherit_parent_restrictions") is True

    def test_lanes_post_errors(self):
        with self.request_context_with_library_and_admin("/", method='POST'):
            flask.request.form = MultiDict([
            ])
            response = self.manager.admin_lanes_controller.lanes()
            assert NO_DISPLAY_NAME_FOR_LANE == response

        with self.request_context_with_library_and_admin("/", method='POST'):
            flask.request.form = MultiDict([
                ("display_name", "lane"),
            ])
            response = self.manager.admin_lanes_controller.lanes()
            assert NO_CUSTOM_LISTS_FOR_LANE == response

        (list, _) = self._customlist(data_source_name=DataSource.LIBRARY_STAFF, num_entries=0)
        list.library = self._default_library

        with self.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", "12345"),
                ("display_name", "lane"),
                ("custom_list_ids", json.dumps([list.id])),
            ])
            response = self.manager.admin_lanes_controller.lanes()
            assert MISSING_LANE == response

        library = self._library()
        with self.request_context_with_library_and_admin("/", method='POST'):
            flask.request.library = library
            flask.request.form = MultiDict([
                ("display_name", "lane"),
                ("custom_list_ids", json.dumps([list.id])),
            ])
            pytest.raises(AdminNotAuthorized,
                          self.manager.admin_lanes_controller.lanes)

        lane1 = self._lane("lane1")
        lane2 = self._lane("lane2")     # noqa: F841

        with self.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", lane1.id),
                ("display_name", "lane1"),
                ("custom_list_ids", json.dumps([list.id])),
            ])
            response = self.manager.admin_lanes_controller.lanes()
            assert CANNOT_EDIT_DEFAULT_LANE == response

        lane1.customlists += [list]

        with self.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", lane1.id),
                ("display_name", "lane2"),
                ("custom_list_ids", json.dumps([list.id])),
            ])
            response = self.manager.admin_lanes_controller.lanes()
            assert LANE_WITH_PARENT_AND_DISPLAY_NAME_ALREADY_EXISTS == response

        with self.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("display_name", "lane2"),
                ("custom_list_ids", json.dumps([list.id])),
            ])
            response = self.manager.admin_lanes_controller.lanes()
            assert LANE_WITH_PARENT_AND_DISPLAY_NAME_ALREADY_EXISTS == response

        with self.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("parent_id", "12345"),
                ("display_name", "lane"),
                ("custom_list_ids", json.dumps([list.id])),
            ])
            response = self.manager.admin_lanes_controller.lanes()
            assert MISSING_LANE.uri == response.uri

        with self.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("parent_id", lane1.id),
                ("display_name", "lane"),
                ("custom_list_ids", json.dumps(["12345"])),
            ])
            response = self.manager.admin_lanes_controller.lanes()
            assert MISSING_CUSTOM_LIST.uri == response.uri

    def test_lanes_create(self):
        (list, _) = self._customlist(data_source_name=DataSource.LIBRARY_STAFF, num_entries=0)
        list.library = self._default_library

        # The new lane's parent has a sublane already.
        parent = self._lane("parent")
        sibling = self._lane("sibling", parent=parent)

        with self.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("parent_id", parent.id),
                ("display_name", "lane"),
                ("custom_list_ids", json.dumps([list.id])),
                ("inherit_parent_restrictions", "false"),
            ])
            response = self.manager.admin_lanes_controller.lanes()
            assert 201 == response.status_code

            [lane] = self._db.query(Lane).filter(Lane.display_name == "lane")
            assert lane.id == int(response.get_data(as_text=True))
            assert self._default_library == lane.library
            assert "lane" == lane.display_name
            assert parent == lane.parent
            assert lane.media is None
            assert 1 == len(lane.customlists)
            assert list == lane.customlists[0]
            assert lane.inherit_parent_restrictions is False
            assert 0 == lane.priority

            # The sibling's priority has been shifted down to put the new lane at the top.
            assert 1 == sibling.priority

    def test_lanes_edit(self):

        work = self._work(with_license_pool=True)

        (list1, _) = self._customlist(data_source_name=DataSource.LIBRARY_STAFF, num_entries=0)
        list1.library = self._default_library
        (list2, _) = self._customlist(data_source_name=DataSource.LIBRARY_STAFF, num_entries=0)
        list2.library = self._default_library
        list2.add_entry(work)

        lane = self._lane("old name")
        lane.customlists += [list1]

        # When we add a list to the lane, the controller will ask the
        # search engine to update lane.size, and it will think there
        # are two works in the lane.
        assert 0 == lane.size
        self.controller.search_engine.docs = dict(id1="value1", id2="value2")

        with self.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", str(lane.id)),
                ("display_name", "new name"),
                ("custom_list_ids", json.dumps([list2.id])),
                ("inherit_parent_restrictions", "true"),
            ])

            response = self.manager.admin_lanes_controller.lanes()
            assert 200 == response.status_code
            assert lane.id == int(response.get_data(as_text=True))

            assert "new name" == lane.display_name
            assert [list2] == lane.customlists
            assert lane.inherit_parent_restrictions is True
            assert lane.media is None
            assert 2 == lane.size

    def test_lane_delete_success(self):
        library = self._library()
        self.admin.add_role(AdminRole.LIBRARY_MANAGER, library)
        lane = self._lane("lane", library=library)
        (list, _) = self._customlist(data_source_name=DataSource.LIBRARY_STAFF, num_entries=0)
        list.library = library
        lane.customlists += [list]
        assert 1 == self._db.query(Lane).filter(Lane.library == library).count()

        with self.request_context_with_library_and_admin("/", method="DELETE"):
            flask.request.library = library
            response = self.manager.admin_lanes_controller.lane(lane.id)
            assert 200 == response.status_code

            # The lane has been deleted.
            assert 0 == self._db.query(Lane).filter(Lane.library == library).count()

            # The custom list still exists though.
            assert 1 == self._db.query(CustomList).filter(CustomList.library == library).count()

        lane = self._lane("lane", library=library)
        lane.customlists += [list]
        child = self._lane("child", parent=lane, library=library)
        child.customlists += [list]
        grandchild = self._lane("grandchild", parent=child, library=library)
        grandchild.customlists += [list]
        assert 3 == self._db.query(Lane).filter(Lane.library == library).count()

        with self.request_context_with_library_and_admin("/", method="DELETE"):
            flask.request.library = library
            response = self.manager.admin_lanes_controller.lane(lane.id)
            assert 200 == response.status_code

            # The lanes have all been deleted.
            assert 0 == self._db.query(Lane).filter(Lane.library == library).count()

            # The custom list still exists though.
            assert 1 == self._db.query(CustomList).filter(CustomList.library == library).count()

    def test_lane_delete_errors(self):
        with self.request_context_with_library_and_admin("/", method="DELETE"):
            response = self.manager.admin_lanes_controller.lane(123)
            assert MISSING_LANE == response

        lane = self._lane("lane")
        library = self._library()
        with self.request_context_with_library_and_admin("/", method="DELETE"):
            flask.request.library = library
            pytest.raises(AdminNotAuthorized,
                          self.manager.admin_lanes_controller.lane,
                          lane.id)

        with self.request_context_with_library_and_admin("/", method="DELETE"):
            response = self.manager.admin_lanes_controller.lane(lane.id)
            assert CANNOT_EDIT_DEFAULT_LANE == response

    def test_show_lane_success(self):
        lane = self._lane("lane")
        lane.visible = False
        with self.request_context_with_library_and_admin("/"):
            response = self.manager.admin_lanes_controller.show_lane(lane.id)
            assert 200 == response.status_code
            assert lane.visible is True

    def test_show_lane_errors(self):
        with self.request_context_with_library_and_admin("/"):
            response = self.manager.admin_lanes_controller.show_lane(123)
            assert MISSING_LANE == response

        parent = self._lane("parent")
        parent.visible = False
        child = self._lane("lane")
        child.visible = False
        child.parent = parent
        with self.request_context_with_library_and_admin("/"):
            response = self.manager.admin_lanes_controller.show_lane(child.id)
            assert CANNOT_SHOW_LANE_WITH_HIDDEN_PARENT == response

        self.admin.remove_role(AdminRole.LIBRARY_MANAGER, self._default_library)
        with self.request_context_with_library_and_admin("/"):
            pytest.raises(AdminNotAuthorized,
                          self.manager.admin_lanes_controller.show_lane,
                          parent.id)

    def test_hide_lane_success(self):
        lane = self._lane("lane")
        lane.visible = True
        with self.request_context_with_library_and_admin("/"):
            response = self.manager.admin_lanes_controller.hide_lane(lane.id)
            assert 200 == response.status_code
            assert lane.visible is False

    def test_hide_lane_errors(self):
        with self.request_context_with_library_and_admin("/"):
            response = self.manager.admin_lanes_controller.hide_lane(123456789)
            assert MISSING_LANE == response

        lane = self._lane()
        self.admin.remove_role(AdminRole.LIBRARY_MANAGER, self._default_library)
        with self.request_context_with_library_and_admin("/"):
            pytest.raises(AdminNotAuthorized,
                          self.manager.admin_lanes_controller.show_lane,
                          lane.id)

    def test_reset(self):
        library = self._library()
        old_lane = self._lane("old lane", library=library)

        with self.request_context_with_library_and_admin("/"):
            flask.request.library = library
            pytest.raises(AdminNotAuthorized, self.manager.admin_lanes_controller.reset)

            self.admin.add_role(AdminRole.LIBRARY_MANAGER, library)
            response = self.manager.admin_lanes_controller.reset()
            assert 200 == response.status_code

            # The old lane is gone.
            assert 0 == self._db.query(Lane).filter(Lane.library == library).filter(Lane.id == old_lane.id).count()
            # tests/test_lanes.py tests the default lane creation, but make sure some
            # lanes were created.
            assert 0 < self._db.query(Lane).filter(Lane.library == library).count()

    def test_change_order(self):
        library = self._library()
        parent1 = self._lane("parent1", library=library)
        parent2 = self._lane("parent2", library=library)
        child1 = self._lane("child1", parent=parent2)
        child2 = self._lane("child2", parent=parent2)
        parent1.priority = 0
        parent2.priority = 1
        child1.priority = 0
        child2.priority = 1

        new_order = [{"id": parent2.id, "sublanes": [{"id": child2.id}, {"id": child1.id}]}, {"id": parent1.id}]

        with self.request_context_with_library_and_admin("/"):
            flask.request.library = library
            flask.request.data = json.dumps(new_order)

            pytest.raises(AdminNotAuthorized, self.manager.admin_lanes_controller.change_order)

            self.admin.add_role(AdminRole.LIBRARY_MANAGER, library)
            response = self.manager.admin_lanes_controller.change_order()
            assert 200 == response.status_code

            assert 0 == parent2.priority
            assert 1 == parent1.priority
            assert 0 == child2.priority
            assert 1 == child1.priority


class TestDashboardController(AdminControllerTest):

    # Unlike most of these controllers, we do want to have a book
    # automatically created as part of setup.
    BOOKS = CirculationControllerTest.BOOKS

    def test_circulation_events(self):
        [lp] = self.english_1.license_pools
        types = [
            CirculationEvent.DISTRIBUTOR_CHECKIN,
            CirculationEvent.DISTRIBUTOR_CHECKOUT,
            CirculationEvent.DISTRIBUTOR_HOLD_PLACE,
            CirculationEvent.DISTRIBUTOR_HOLD_RELEASE,
            CirculationEvent.DISTRIBUTOR_TITLE_ADD
        ]
        time = utc_now() - timedelta(minutes=len(types))
        for type in types:
            get_one_or_create(
                self._db, CirculationEvent,
                license_pool=lp, type=type, start=time, end=time,
            )
            time += timedelta(minutes=1)

        with self.request_context_with_library_and_admin("/"):
            response = self.manager.admin_dashboard_controller.circulation_events()
            url = AdminAnnotator(
                self.manager.d_circulation,
                self._default_library
            ).permalink_for(self.english_1, lp, lp.identifier)

        events = response['circulation_events']
        assert types[::-1] == [event['type'] for event in events]
        assert [self.english_1.title]*len(types) == [event['book']['title'] for event in events]
        assert [url]*len(types) == [event['book']['url'] for event in events]

        # request fewer events
        with self.request_context_with_library_and_admin("/?num=2"):
            response = self.manager.admin_dashboard_controller.circulation_events()
            url = AdminAnnotator(
                self.manager.d_circulation,
                self._default_library
            ).permalink_for(self.english_1, lp, lp.identifier)

        assert 2 == len(response['circulation_events'])

    def test_bulk_circulation_events(self):
        [lp] = self.english_1.license_pools
        edition = self.english_1.presentation_edition
        identifier = self.english_1.presentation_edition.primary_identifier
        genres = self._db.query(Genre).all()
        get_one_or_create(self._db, WorkGenre, work=self.english_1, genre=genres[0], affinity=0.2)

        time = utc_now() - timedelta(minutes=1)
        (event, _) = get_one_or_create(
            self._db, CirculationEvent,
            license_pool=lp, type=CirculationEvent.DISTRIBUTOR_CHECKOUT,
            start=time, end=time
        )
        time += timedelta(minutes=1)

        # Try an end-to-end test, getting all circulation events for
        # the current day.
        with self.app.test_request_context("/"):
            (response, requested_date, date_end, library_short_name) = \
                self.manager.admin_dashboard_controller.bulk_circulation_events()

        reader = csv.reader(
            [row for row in response.split("\r\n") if row],
            dialect=csv.excel
        )
        rows = [row for row in reader][1::]  # skip header row
        assert 1 == len(rows)
        [row] = rows
        assert CirculationEvent.DISTRIBUTOR_CHECKOUT == row[1]
        assert identifier.identifier == row[2]
        assert identifier.type == row[3]
        assert edition.title == row[4]
        assert genres[0].name == row[12]

        # Now verify that this works by passing incoming query
        # parameters into a LocalAnalyticsExporter object.
        class MockLocalAnalyticsExporter(object):
            def export(self, _db, date_start, date_end, locations, library):
                self.called_with = (
                    _db, date_start, date_end, locations, library
                )
                return "A CSV file"

        exporter = MockLocalAnalyticsExporter()
        with self.request_context_with_library("/?date=2018-01-01&dateEnd=2018-01-04&locations=loc1,loc2"):
            (response, requested_date, date_end, library_short_name) = \
                self.manager.admin_dashboard_controller.bulk_circulation_events(analytics_exporter=exporter)

            # export() was called with the arguments we expect.
            #
            args = list(exporter.called_with)
            assert self._db == args.pop(0)
            assert datetime.date(2018, 1, 1) == args.pop(0)
            # This is the start of the day _after_ the dateEnd we
            # specified -- we want all events that happened _before_
            # 2018-01-05.
            assert datetime.date(2018, 1, 5) == args.pop(0)
            assert "loc1,loc2" == args.pop(0)
            assert self._default_library == args.pop(0)
            assert [] == args

            # The data returned is whatever export() returned.
            assert "A CSV file" == response

            # The other data is necessary to build a filename for the
            # "CSV file".
            assert "2018-01-01" == requested_date

            # Note that the date_end is the date we requested --
            # 2018-01-04 -- not the cutoff time passed in to export(),
            # which is the start of the subsequent day.
            assert "2018-01-04" == date_end
            assert self._default_library.short_name == library_short_name

    def test_stats_patrons(self):
        with self.request_context_with_admin("/"):
            self.admin.add_role(AdminRole.SYSTEM_ADMIN)

            # At first, there's one patron in the database.
            response = self.manager.admin_dashboard_controller.stats()
            library_data = response.get(self._default_library.short_name)
            total_data = response.get("total")
            for data in [library_data, total_data]:
                patron_data = data.get('patrons')
                assert 1 == patron_data.get('total')
                assert 0 == patron_data.get('with_active_loans')
                assert 0 == patron_data.get('with_active_loans_or_holds')
                assert 0 == patron_data.get('loans')
                assert 0 == patron_data.get('holds')

            edition, pool = self._edition(with_license_pool=True, with_open_access_download=False)
            edition2, open_access_pool = self._edition(with_open_access_download=True)

            # patron1 has a loan.
            patron1 = self._patron()
            pool.loan_to(patron1, end=utc_now() + timedelta(days=5))

            # patron2 has a hold.
            patron2 = self._patron()
            pool.on_hold_to(patron2)

            # patron3 has an open access loan with no end date, but it doesn't count
            # because we don't know if it is still active.
            patron3 = self._patron()
            open_access_pool.loan_to(patron3)

            response = self.manager.admin_dashboard_controller.stats()
            library_data = response.get(self._default_library.short_name)
            total_data = response.get("total")
            for data in [library_data, total_data]:
                patron_data = data.get('patrons')
                assert 4 == patron_data.get('total')
                assert 1 == patron_data.get('with_active_loans')
                assert 2 == patron_data.get('with_active_loans_or_holds')
                assert 1 == patron_data.get('loans')
                assert 1 == patron_data.get('holds')

            # These patrons are in a different library..
            l2 = self._library()
            patron4 = self._patron(library=l2)
            pool.loan_to(patron4, end=utc_now() + timedelta(days=5))
            patron5 = self._patron(library=l2)
            pool.on_hold_to(patron5)

            response = self.manager.admin_dashboard_controller.stats()
            library_data = response.get(self._default_library.short_name)
            total_data = response.get("total")
            assert 4 == library_data.get('patrons').get('total')
            assert 1 == library_data.get('patrons').get('with_active_loans')
            assert 2 == library_data.get('patrons').get('with_active_loans_or_holds')
            assert 1 == library_data.get('patrons').get('loans')
            assert 1 == library_data.get('patrons').get('holds')
            assert 6 == total_data.get('patrons').get('total')
            assert 2 == total_data.get('patrons').get('with_active_loans')
            assert 4 == total_data.get('patrons').get('with_active_loans_or_holds')
            assert 2 == total_data.get('patrons').get('loans')
            assert 2 == total_data.get('patrons').get('holds')

            # If the admin only has access to some libraries, only those will be counted
            # in the total stats.
            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            self.admin.add_role(AdminRole.LIBRARIAN, self._default_library)

            response = self.manager.admin_dashboard_controller.stats()
            library_data = response.get(self._default_library.short_name)
            total_data = response.get("total")
            assert 4 == library_data.get('patrons').get('total')
            assert 1 == library_data.get('patrons').get('with_active_loans')
            assert 2 == library_data.get('patrons').get('with_active_loans_or_holds')
            assert 1 == library_data.get('patrons').get('loans')
            assert 1 == library_data.get('patrons').get('holds')
            assert 4 == total_data.get('patrons').get('total')
            assert 1 == total_data.get('patrons').get('with_active_loans')
            assert 2 == total_data.get('patrons').get('with_active_loans_or_holds')
            assert 1 == total_data.get('patrons').get('loans')
            assert 1 == total_data.get('patrons').get('holds')

    def test_stats_inventory(self):
        with self.request_context_with_admin("/"):
            self.admin.add_role(AdminRole.SYSTEM_ADMIN)

            # At first, there is 1 open access title in the database,
            # created in CirculationControllerTest.setup.
            response = self.manager.admin_dashboard_controller.stats()
            library_data = response.get(self._default_library.short_name)
            total_data = response.get("total")
            for data in [library_data, total_data]:
                inventory_data = data.get('inventory')
                assert 1 == inventory_data.get('titles')
                assert 0 == inventory_data.get('licenses')
                assert 0 == inventory_data.get('available_licenses')

            # This edition has no licenses owned and isn't counted in the inventory.
            edition1, pool1 = self._edition(with_license_pool=True, with_open_access_download=False)
            pool1.open_access = False
            pool1.licenses_owned = 0
            pool1.licenses_available = 0

            edition2, pool2 = self._edition(with_license_pool=True, with_open_access_download=False)
            pool2.open_access = False
            pool2.licenses_owned = 10
            pool2.licenses_available = 0

            edition3, pool3 = self._edition(with_license_pool=True, with_open_access_download=False)
            pool3.open_access = False
            pool3.licenses_owned = 5
            pool3.licenses_available = 4

            response = self.manager.admin_dashboard_controller.stats()
            library_data = response.get(self._default_library.short_name)
            total_data = response.get("total")
            for data in [library_data, total_data]:
                inventory_data = data.get('inventory')
                assert 3 == inventory_data.get('titles')
                assert 15 == inventory_data.get('licenses')
                assert 4 == inventory_data.get('available_licenses')

            # This edition is in a different collection.
            c2 = self._collection()
            edition4, pool4 = self._edition(with_license_pool=True, with_open_access_download=False, collection=c2)
            pool4.licenses_owned = 2
            pool4.licenses_available = 2

            response = self.manager.admin_dashboard_controller.stats()
            library_data = response.get(self._default_library.short_name)
            total_data = response.get("total")
            assert 3 == library_data.get('inventory').get('titles')
            assert 4 == total_data.get('inventory').get('titles')
            assert 15 == library_data.get('inventory').get('licenses')
            assert 17 == total_data.get('inventory').get('licenses')
            assert 4 == library_data.get('inventory').get('available_licenses')
            assert 6 == total_data.get('inventory').get('available_licenses')

            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            self.admin.add_role(AdminRole.LIBRARIAN, self._default_library)

            # The admin can no longer see the other collection, so it's not
            # counted in the totals.
            response = self.manager.admin_dashboard_controller.stats()
            library_data = response.get(self._default_library.short_name)
            total_data = response.get("total")
            for data in [library_data, total_data]:
                inventory_data = data.get('inventory')
                assert 3 == inventory_data.get('titles')
                assert 15 == inventory_data.get('licenses')
                assert 4 == inventory_data.get('available_licenses')

    def test_stats_collections(self):
        with self.request_context_with_admin("/"):
            self.admin.add_role(AdminRole.SYSTEM_ADMIN)

            # At first, there is 1 open access title in the database,
            # created in CirculationControllerTest.setup.
            response = self.manager.admin_dashboard_controller.stats()
            library_data = response.get(self._default_library.short_name)
            total_data = response.get("total")
            for data in [library_data, total_data]:
                collections_data = data.get('collections')
                assert 1 == len(collections_data)
                collection_data = collections_data.get(self._default_collection.name)
                assert 0 == collection_data.get('licensed_titles')
                assert 1 == collection_data.get('open_access_titles')
                assert 0 == collection_data.get('licenses')
                assert 0 == collection_data.get('available_licenses')

            c2 = self._collection()
            c3 = self._collection()
            c3.libraries += [self._default_library]

            edition1, pool1 = self._edition(with_license_pool=True,
                                            with_open_access_download=False,
                                            data_source_name=DataSource.OVERDRIVE,
                                            collection=c2)
            pool1.open_access = False
            pool1.licenses_owned = 10
            pool1.licenses_available = 5

            edition2, pool2 = self._edition(with_license_pool=True,
                                            with_open_access_download=False,
                                            data_source_name=DataSource.OVERDRIVE,
                                            collection=c3)
            pool2.open_access = False
            pool2.licenses_owned = 0
            pool2.licenses_available = 0

            edition3, pool3 = self._edition(with_license_pool=True,
                                            with_open_access_download=False,
                                            data_source_name=DataSource.BIBLIOTHECA)
            pool3.open_access = False
            pool3.licenses_owned = 3
            pool3.licenses_available = 0

            edition4, pool4 = self._edition(with_license_pool=True,
                                            with_open_access_download=False,
                                            data_source_name=DataSource.AXIS_360,
                                            collection=c2)
            pool4.open_access = False
            pool4.licenses_owned = 5
            pool4.licenses_available = 5

            response = self.manager.admin_dashboard_controller.stats()
            library_data = response.get(self._default_library.short_name)
            total_data = response.get("total")
            library_collections_data = library_data.get('collections')
            total_collections_data = total_data.get('collections')
            assert 2 == len(library_collections_data)
            assert 3 == len(total_collections_data)
            for data in [library_collections_data, total_collections_data]:
                c1_data = data.get(self._default_collection.name)
                assert 1 == c1_data.get('licensed_titles')
                assert 1 == c1_data.get('open_access_titles')
                assert 3 == c1_data.get('licenses')
                assert 0 == c1_data.get('available_licenses')

                c3_data = data.get(c3.name)
                assert 0 == c3_data.get('licensed_titles')
                assert 0 == c3_data.get('open_access_titles')
                assert 0 == c3_data.get('licenses')
                assert 0 == c3_data.get('available_licenses')

            assert library_collections_data.get(c2.name) is None
            c2_data = total_collections_data.get(c2.name)
            assert 2 == c2_data.get('licensed_titles')
            assert 0 == c2_data.get('open_access_titles')
            assert 15 == c2_data.get('licenses')
            assert 10 == c2_data.get('available_licenses')

            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            self.admin.add_role(AdminRole.LIBRARY_MANAGER, self._default_library)

            # c2 is no longer included in the totals since the admin's library does
            # not use it.
            response = self.manager.admin_dashboard_controller.stats()
            library_data = response.get(self._default_library.short_name)
            total_data = response.get("total")
            for data in [library_data, total_data]:
                collections_data = data.get("collections")
                assert 2 == len(collections_data)
                assert collections_data.get(c2.name) is None

                c1_data = collections_data.get(self._default_collection.name)
                assert 1 == c1_data.get('licensed_titles')
                assert 1 == c1_data.get('open_access_titles')
                assert 3 == c1_data.get('licenses')
                assert 0 == c1_data.get('available_licenses')

                c3_data = collections_data.get(c3.name)
                assert 0 == c3_data.get('licensed_titles')
                assert 0 == c3_data.get('open_access_titles')
                assert 0 == c3_data.get('licenses')
                assert 0 == c3_data.get('available_licenses')


class SettingsControllerTest(AdminControllerTest):
    """Test some part of the settings controller."""

    def setup_method(self):
        super(SettingsControllerTest, self).setup_method()
        # Delete any existing patron auth services created by controller test setup.
        for auth_service in self._db.query(ExternalIntegration).filter(
            ExternalIntegration.goal == ExternalIntegration.PATRON_AUTH_GOAL
         ):
            self._db.delete(auth_service)

        # Delete any existing sitewide ConfigurationSettings.
        for setting in self._db.query(ConfigurationSetting).filter(
            ConfigurationSetting.library_id == None).filter(            # noqa: E711
            ConfigurationSetting.external_integration_id == None        # noqa: E711
        ):
            self._db.delete(setting)

        self.responses = []
        self.requests = []

        # Make the admin a system admin so they can do everything by default.
        self.admin.add_role(AdminRole.SYSTEM_ADMIN)

    def do_request(self, url, *args, **kwargs):
        """Mock HTTP get/post method to replace HTTP.get_with_timeout or post_with_timeout."""
        self.requests.append((url, args, kwargs))
        response = self.responses.pop()
        return HTTP.process_debuggable_response(url, response)

    def mock_prior_test_results(self, *args, **kwargs):
        self.prior_test_results_called_with = (args, kwargs)
        self_test_results = dict(
            duration=0.9,
            start="2018-08-08T16:04:05Z",
            end="2018-08-08T16:05:05Z",
            results=[]
        )
        self.self_test_results = self_test_results

        return self_test_results

    def mock_run_self_tests(self, *args, **kwargs):
        # This mocks the entire HasSelfTests.run_self_tests
        # process. In general, controllers don't care what's returned
        # from this method, because they only display the test results
        # as they were stored alongside the ExternalIntegration
        # as a side effect of run_self_tests running.
        self.run_self_tests_called_with = (args, kwargs)
        return ("value", "results")

    def mock_failed_run_self_tests(self, *args, **kwargs):
        self.failed_run_self_tests_called_with = (args, kwargs)
        return (None, None)

    def test_get_prior_test_results(self):
        controller = SettingsController(self.manager)
        old_prior_test_results = HasSelfTests.prior_test_results
        HasSelfTests.prior_test_results = self.mock_prior_test_results

        collectionNoProtocol = self._collection()
        collectionNoProtocol.protocol = ""
        controller.type = "collection"

        # No collection or collection with protocol passed
        self_test_results = controller._get_prior_test_results({}, {})
        assert self_test_results is None
        self_test_results = controller._get_prior_test_results(collectionNoProtocol, {})
        assert self_test_results is None

        collection = MockAxis360API.mock_collection(self._db)
        # Test that a collection's protocol calls HasSelfTests.prior_test_results
        self_test_results = controller._get_prior_test_results(collection, Axis360API)
        args = self.prior_test_results_called_with[0]
        assert args[1] == Axis360API
        assert args[3] == collection

        OPDSCollection = self._collection()
        # If a collection's protocol is OPDSImporter, make sure that
        # OPDSImportMonitor.prior_test_results is called
        self_test_results = controller._get_prior_test_results(OPDSCollection, OPDSImporter)
        args = self.prior_test_results_called_with[0]
        assert args[1] == OPDSImportMonitor
        assert args[3] == OPDSCollection

        # We don't crash if there's a problem getting the prior test
        # results -- _get_prior_test_results just returns None.
        @classmethod
        def oops(cls, *args, **kwargs):
            raise Exception("Test result disaster!")
        HasSelfTests.prior_test_results = oops
        self_test_results = controller._get_prior_test_results(
            OPDSCollection, OPDSImporter
        )
        assert (
            "Exception getting self-test results for collection %s: Test result disaster!" % (
                OPDSCollection.name
            ) ==
            self_test_results["exception"])

        HasSelfTests.prior_test_results = old_prior_test_results


class TestSettingsController(SettingsControllerTest):

    def test_get_integration_protocols(self):
        """Test the _get_integration_protocols helper method."""
        class Protocol(object):
            __module__ = 'my name'
            NAME = 'my label'
            DESCRIPTION = 'my description'
            SITEWIDE = True
            SETTINGS = [1, 2, 3]
            CHILD_SETTINGS = [4, 5]
            LIBRARY_SETTINGS = [6]
            CARDINALITY = 1

        [result] = SettingsController._get_integration_protocols([Protocol])
        expect = dict(
            sitewide=True, description='my description',
            settings=[1, 2, 3], library_settings=[6],
            child_settings=[4, 5], label='my label',
            cardinality=1, name='my name'
        )
        assert expect == result

        # Remove the CARDINALITY setting
        del Protocol.CARDINALITY

        # And look in a different place for the name.
        [result] = SettingsController._get_integration_protocols(
            [Protocol], protocol_name_attr='NAME'
        )

        assert 'my label' == result['name']
        assert 'cardinality' not in result

    def test_get_integration_info(self):
        """Test the _get_integration_info helper method."""
        m = self.manager.admin_settings_controller._get_integration_info

        # Test the case where there are integrations in the database
        # with the given goal, but none of them match the
        # configuration.
        goal = self._str
        integration = self._external_integration(protocol="a protocol", goal=goal)  # noqa: F841
        assert [] == m(goal, [dict(name="some other protocol")])

    def test_create_integration(self):
        """Test the _create_integration helper method."""

        m = self.manager.admin_settings_controller._create_integration

        protocol_definitions = [
            dict(name="allow many"),
            dict(name="allow one", cardinality=1),
        ]
        goal = "some goal"

        # You get an error if you don't pass in a protocol.
        assert (
            (NO_PROTOCOL_FOR_NEW_SERVICE, False) ==
            m(protocol_definitions, None, goal))

        # You get an error if you do provide a protocol but no definition
        # for it can be found.
        assert (
            (UNKNOWN_PROTOCOL, False) ==
            m(protocol_definitions, "no definition", goal))

        # If the protocol has multiple cardinality you can create as many
        # integrations using that protocol as you want.
        i1, is_new1 = m(protocol_definitions, "allow many", goal)
        assert is_new1 is True

        i2, is_new2 = m(protocol_definitions, "allow many", goal)
        assert is_new2 is True

        assert i1 != i2
        for i in [i1, i2]:
            assert "allow many" == i.protocol
            assert goal == i.goal

        # If the protocol has single cardinality, you can only create one
        # integration using that protocol before you start getting errors.
        i1, is_new1 = m(protocol_definitions, "allow one", goal)
        assert is_new1 is True

        i2, is_new2 = m(protocol_definitions, "allow one", goal)
        assert is_new2 is False
        assert DUPLICATE_INTEGRATION == i2

    def test_validate_formats(self):
        class MockValidator(Validator):
            def __init__(self):
                self.was_called = False
                self.args = []

            def validate(self, settings, content):
                self.was_called = True
                self.args.append(settings)
                self.args.append(content)

            def validate_error(self, settings, content):
                return INVALID_EMAIL

        validator = MockValidator()

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "The New York Public Library"),
                ("short_name", "nypl"),
                (Configuration.WEBSITE_URL, "https://library.library/"),
                (Configuration.DEFAULT_NOTIFICATION_EMAIL_ADDRESS, "email@example.com"),
                (Configuration.HELP_EMAIL, "help@example.com")
            ])
            flask.request.files = MultiDict([
                (Configuration.LOGO, StringIO())
            ])
            response = self.manager.admin_settings_controller.validate_formats(
                Configuration.LIBRARY_SETTINGS, validator)
            assert response is None
            assert validator.was_called is True
            assert validator.args[0] == Configuration.LIBRARY_SETTINGS
            assert validator.args[1] == {"files": flask.request.files, "form": flask.request.form}

            validator.validate = validator.validate_error
            # If the validator returns an problem detail, validate_formats returns it.
            response = self.manager.admin_settings_controller.validate_formats(
                Configuration.LIBRARY_SETTINGS, validator)
            assert response == INVALID_EMAIL

    def test__mirror_integration_settings(self):
        # If no storage integrations are available, return none
        mirror_integration_settings = self.manager.admin_settings_controller._mirror_integration_settings

        assert mirror_integration_settings() is None

        # Storages created will appear for settings of any purpose
        storage1 = self._external_integration(
            "protocol1", ExternalIntegration.STORAGE_GOAL, name="storage1",
            settings={
                S3UploaderConfiguration.BOOK_COVERS_BUCKET_KEY: 'covers',
                S3UploaderConfiguration.OA_CONTENT_BUCKET_KEY: 'open-access-books',
                S3UploaderConfiguration.PROTECTED_CONTENT_BUCKET_KEY: 'protected-access-books'
            }
        )

        settings = mirror_integration_settings()

        assert settings[0]["key"] == "covers_mirror_integration_id"
        assert settings[0]["label"] == "Covers Mirror"
        assert (settings[0]["options"][0]['key'] == self.manager.admin_settings_controller.NO_MIRROR_INTEGRATION)
        assert (settings[0]["options"][1]['key'] == str(storage1.id))
        assert settings[1]["key"] == "books_mirror_integration_id"
        assert settings[1]["label"] == "Open Access Books Mirror"
        assert (settings[1]["options"][0]['key'] == self.manager.admin_settings_controller.NO_MIRROR_INTEGRATION)
        assert (settings[1]["options"][1]['key'] == str(storage1.id))
        assert settings[2]["label"] == "Protected Access Books Mirror"
        assert (settings[2]["options"][0]['key'] == self.manager.admin_settings_controller.NO_MIRROR_INTEGRATION)
        assert (settings[2]["options"][1]['key'] == str(storage1.id))

        storage2 = self._external_integration(
            "protocol2", ExternalIntegration.STORAGE_GOAL, name="storage2",
            settings={
                S3UploaderConfiguration.BOOK_COVERS_BUCKET_KEY: 'covers',
                S3UploaderConfiguration.OA_CONTENT_BUCKET_KEY: 'open-access-books',
                S3UploaderConfiguration.PROTECTED_CONTENT_BUCKET_KEY: 'protected-access-books'
            }
        )
        settings = mirror_integration_settings()

        assert settings[0]["key"] == "covers_mirror_integration_id"
        assert settings[0]["label"] == "Covers Mirror"
        assert (settings[0]["options"][0]['key'] == self.manager.admin_settings_controller.NO_MIRROR_INTEGRATION)
        assert (settings[0]["options"][1]['key'] == str(storage1.id))
        assert (settings[0]["options"][2]['key'] == str(storage2.id))
        assert settings[1]["key"] == "books_mirror_integration_id"
        assert settings[1]["label"] == "Open Access Books Mirror"
        assert (settings[1]["options"][0]['key'] == self.manager.admin_settings_controller.NO_MIRROR_INTEGRATION)
        assert (settings[1]["options"][1]['key'] == str(storage1.id))
        assert (settings[1]["options"][2]['key'] == str(storage2.id))
        assert settings[2]["label"] == "Protected Access Books Mirror"
        assert (settings[2]["options"][0]['key'] == self.manager.admin_settings_controller.NO_MIRROR_INTEGRATION)
        assert (settings[2]["options"][1]['key'] == str(storage1.id))

    def test_check_url_unique(self):
        # Verify our ability to catch duplicate integrations for a
        # given URL.
        m = self.manager.admin_settings_controller.check_url_unique

        # Here's an ExternalIntegration.
        protocol = "a protocol"
        goal = "a goal"
        original = self._external_integration(
            url="http://service/", protocol=protocol, goal=goal
        )
        protocol = original.protocol
        goal = original.goal

        # Here's another ExternalIntegration that might or might not
        # be about to become a duplicate of the original.
        new = self._external_integration(
            protocol=protocol, goal="new goal"
        )
        new.goal = original.goal
        assert new != original

        # We're going to call this helper function multiple times to check if
        # different scenarios trip the "duplicate" logic.
        def is_dupe(url, protocol, goal):
            result = m(new, url, protocol, goal)
            if result is None:
                return False
            elif result is INTEGRATION_URL_ALREADY_IN_USE:
                return True
            else:
                raise Exception(
                    "check_url_unique must return either the problem detail or None"
                )

        # The original ExternalIntegration is not a duplicate of itself.
        assert m(original, original.url, protocol, goal) is None

        # However, any other ExternalIntegration with the same URL,
        # protocol, and goal is considered a duplicate.
        assert is_dupe(original.url, protocol, goal) is True

        # Minor URL differences are ignored when considering duplicates
        # -- this is with help from url_variants().
        assert is_dupe("https://service/", protocol, goal) is True
        assert is_dupe("https://service", protocol, goal) is True

        # Not all variants are handled in this way
        assert is_dupe("https://service/#fragment", protocol, goal) is False

        # If any of URL, protocol, and goal are different, then the
        # integration is not considered a duplicate.
        assert is_dupe("different url", protocol, goal) is False
        assert is_dupe(original.url, "different protocol", goal) is False
        assert is_dupe(original.url, protocol, "different goal") is False

        # If you're not considering a URL at all, we assume no duplicate.
        assert is_dupe(None, protocol, goal) is False

    def test_url_variants(self):
        # Test the helper method that generates slight variants of
        # any given URL.
        def m(url):
            return list(SettingsController.url_variants(url))

        # No URL, no variants.
        assert [] == m(None)
        assert [] == m("not a url")

        # Variants of an HTTP URL with a trailing slash.
        assert (
            ['http://url/', 'http://url', 'https://url/', 'https://url'] ==
            m("http://url/"))

        # Variants of an HTTPS URL with a trailing slash.
        assert (
            ['https://url/', 'https://url', 'http://url/', 'http://url'] ==
            m("https://url/"))

        # Variants of a URL with no trailing slash.
        assert (
            ['https://url', 'https://url/', 'http://url', 'http://url/'] ==
            m("https://url"))
