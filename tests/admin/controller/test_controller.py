from nose.tools import (
    set_trace,
    eq_,
    assert_raises
)
import flask
import json
import os
import re
import feedparser
from werkzeug import ImmutableMultiDict, MultiDict
from werkzeug.http import dump_cookie
from StringIO import StringIO
import base64
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP
from contextlib import contextmanager
from PIL import Image
import math
import operator
from flask_babel import lazy_gettext as _

from tests import sample_data
from tests.test_controller import CirculationControllerTest
from api.adobe_vendor_id import (
    AdobeVendorIDModel,
    AuthdataUtility
)

from api.admin.controller import (
    setup_admin_controllers,
    AdminAnnotator,
    SettingsController,
    PatronController,
    TimestampsController
)
from api.admin.problem_details import *
from api.admin.exceptions import *
from api.admin.routes import setup_admin
from api.config import (
    Configuration,
    temp_config,
)
from api.admin.password_admin_authentication_provider import PasswordAdminAuthenticationProvider
from api.admin.google_oauth_admin_authentication_provider import GoogleOAuthAdminAuthenticationProvider
from core.model import (
    Admin,
    AdminRole,
    CirculationEvent,
    Classification,
    Collection,
    Complaint,
    ConfigurationSetting,
    Contributor,
    CoverageRecord,
    CustomList,
    CustomListEntry,
    create,
    DataSource,
    Edition,
    ExternalIntegration,
    Genre,
    get_one,
    get_one_or_create,
    Hyperlink,
    Identifier,
    Library,
    Representation,
    ResourceTransformation,
    RightsStatus,
    SessionManager,
    Subject,
    Timestamp,
    WorkGenre
)
from core.lane import Lane
from core.s3 import MockS3Uploader
from core.testing import (
    AlwaysSuccessfulCoverageProvider,
    NeverSuccessfulCoverageProvider,
    MockRequestsResponse,
)
from core.util.http import HTTP
from core.util.problem_detail import ProblemDetail
from core.classifier import (
    genres,
    SimplifiedGenreClassifier
)
from datetime import date, datetime, timedelta

from api.authenticator import (
    AuthenticationProvider,
    PatronData,
)

from core.local_analytics_provider import LocalAnalyticsProvider

from api.adobe_vendor_id import AuthdataUtility

from api.axis import (Axis360API, MockAxis360API)
from core.selftest import HasSelfTests
from core.opds_import import (OPDSImporter, OPDSImportMonitor)

class AdminControllerTest(CirculationControllerTest):

    # Automatically creating books before the test wastes time -- we
    # don't need them.
    BOOKS = []

    def setup(self):
        super(AdminControllerTest, self).setup()
        ConfigurationSetting.sitewide(self._db, Configuration.SECRET_KEY).value = "a secret"
        setup_admin(self._db)
        setup_admin_controllers(self.manager)
        self.admin, ignore = create(
            self._db, Admin, email=u'example@nypl.org',
        )
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
            eq_(200, response.status_code)
            html = response.response[0]
            assert 'settingUp: true' in html

    def test_not_setting_up(self):
        with self.app.test_request_context('/admin'):
            flask.session['admin_email'] = self.admin.email
            flask.session['auth_type'] = PasswordAdminAuthenticationProvider.NAME
            response = self.manager.admin_view_controller("collection", "book")
            eq_(200, response.status_code)
            html = response.response[0]
            assert 'settingUp: false' in html

    def test_redirect_to_sign_in(self):
        with self.app.test_request_context('/admin/web/collection/a/(b)/book/c/(d)'):
            response = self.manager.admin_view_controller("a/(b)", "c/(d)")
            eq_(302, response.status_code)
            location = response.headers.get("Location")
            assert "sign_in" in location
            assert "admin%2Fweb" in location
            assert "collection%2Fa%2F%28b%29" in location
            assert "book%2Fc%2F%28d%29" in location

    def test_redirect_to_library(self):
        # If the admin doesn't have access to any libraries, they get a message
        # instead of a redirect.
        with self.app.test_request_context('/admin'):
            flask.session['admin_email'] = self.admin.email
            flask.session['auth_type'] = PasswordAdminAuthenticationProvider.NAME
            response = self.manager.admin_view_controller(None, None)
            eq_(200, response.status_code)
            assert "Your admin account doesn't have access to any libraries" in response.data

        # Unless there aren't any libraries yet. In that case, an admin needs to
        # get in to create one.
        for library in self._db.query(Library):
            self._db.delete(library)
        with self.app.test_request_context('/admin'):
            flask.session['admin_email'] = self.admin.email
            flask.session['auth_type'] = PasswordAdminAuthenticationProvider.NAME
            response = self.manager.admin_view_controller(None, None)
            eq_(200, response.status_code)
            assert "<body>" in response.data

        l1 = self._library(short_name="L1")
        l2 = self._library(short_name="L2")
        l3 = self._library(short_name="L3")
        self.admin.add_role(AdminRole.LIBRARIAN, l1)
        self.admin.add_role(AdminRole.LIBRARY_MANAGER, l3)
        # An admin with roles gets redirected to the oldest library they have access to.
        with self.app.test_request_context('/admin'):
            flask.session['admin_email'] = self.admin.email
            flask.session['auth_type'] = PasswordAdminAuthenticationProvider.NAME
            response = self.manager.admin_view_controller(None, None)
            eq_(302, response.status_code)
            location = response.headers.get("Location")
            assert "admin/web/collection/%s" % l1.short_name in location

        # Only the root url redirects - a non-library specific page with another
        # path won't.
        with self.app.test_request_context('/admin/web/config'):
            flask.session['admin_email'] = self.admin.email
            flask.session['auth_type'] = PasswordAdminAuthenticationProvider.NAME
            response = self.manager.admin_view_controller(None, None, "config")
            eq_(200, response.status_code)

    def test_csrf_token(self):
        self.admin.password_hashed = None
        with self.app.test_request_context('/admin'):
            response = self.manager.admin_view_controller(None, None)
            eq_(200, response.status_code)
            html = response.response[0]

            # The CSRF token value is random, but the cookie and the html have the same value.
            html_csrf_re = re.compile('csrfToken: \"([^\"]*)\"')
            match = html_csrf_re.search(html)
            assert match != None
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
            eq_(200, response.status_code)
            html = response.response[0]
            assert 'csrfToken: "%s"' % token in html
            assert token in response.headers.get('Set-Cookie')

    def test_show_circ_events_download(self):
        # The local analytics provider isn't configured yet.
        with self.app.test_request_context('/admin'):
            flask.session['admin_email'] = self.admin.email
            flask.session['auth_type'] = PasswordAdminAuthenticationProvider.NAME
            response = self.manager.admin_view_controller("collection", "book")
            eq_(200, response.status_code)
            html = response.response[0]
            assert 'showCircEventsDownload: false' in html

        # Create the local analytics integration.
        local_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=LocalAnalyticsProvider.__module__,
            goal=ExternalIntegration.ANALYTICS_GOAL,
        )
        with self.app.test_request_context('/admin'):
            flask.session['admin_email'] = self.admin.email
            flask.session['auth_type'] = PasswordAdminAuthenticationProvider.NAME
            response = self.manager.admin_view_controller("collection", "book")
            eq_(200, response.status_code)
            html = response.response[0]
            assert 'showCircEventsDownload: true' in html

    def test_roles(self):
        self.admin.add_role(AdminRole.SITEWIDE_LIBRARIAN)
        self.admin.add_role(AdminRole.LIBRARY_MANAGER, self._default_library)
        with self.app.test_request_context('/admin'):
            flask.session['admin_email'] = self.admin.email
            flask.session['auth_type'] = PasswordAdminAuthenticationProvider.NAME
            response = self.manager.admin_view_controller("collection", "book")
            eq_(200, response.status_code)
            html = response.response[0]
            assert "\"role\": \"librarian-all\"" in html
            assert "\"role\": \"manager\", \"library\": \"%s\"" % self._default_library.short_name in html

class TestAdminCirculationManagerController(AdminControllerTest):
    def test_require_system_admin(self):
        with self.request_context_with_admin('/admin'):
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_work_controller.require_system_admin)

            self.admin.add_role(AdminRole.SYSTEM_ADMIN)
            self.manager.admin_work_controller.require_system_admin()

    def test_require_sitewide_library_manager(self):
        with self.request_context_with_admin('/admin'):
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_work_controller.require_sitewide_library_manager)

            self.admin.add_role(AdminRole.SITEWIDE_LIBRARY_MANAGER)
            self.manager.admin_work_controller.require_sitewide_library_manager()

    def test_require_library_manager(self):
        with self.request_context_with_admin('/admin'):
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_work_controller.require_library_manager,
                          self._default_library)

            self.admin.add_role(AdminRole.LIBRARY_MANAGER, self._default_library)
            self.manager.admin_work_controller.require_library_manager(self._default_library)

    def test_require_librarian(self):
        with self.request_context_with_admin('/admin'):
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_work_controller.require_librarian,
                          self._default_library)

            self.admin.add_role(AdminRole.LIBRARIAN, self._default_library)
            self.manager.admin_work_controller.require_librarian(self._default_library)

class TestWorkController(AdminControllerTest):

    # Unlike most of these controllers, we do want to have a book
    # automatically created as part of setup.
    BOOKS = CirculationControllerTest.BOOKS

    def setup(self):
        super(TestWorkController, self).setup()
        self.admin.add_role(AdminRole.LIBRARIAN, self._default_library)

    def test_details(self):
        [lp] = self.english_1.license_pools

        lp.suppressed = False
        with self.request_context_with_library_and_admin("/"):
            response = self.manager.admin_work_controller.details(
                lp.identifier.type, lp.identifier.identifier
            )
            eq_(200, response.status_code)
            feed = feedparser.parse(response.get_data())
            [entry] = feed['entries']
            suppress_links = [x['href'] for x in entry['links']
                              if x['rel'] == "http://librarysimplified.org/terms/rel/hide"]
            unsuppress_links = [x['href'] for x in entry['links']
                                if x['rel'] == "http://librarysimplified.org/terms/rel/restore"]
            eq_(0, len(unsuppress_links))
            eq_(1, len(suppress_links))
            assert lp.identifier.identifier in suppress_links[0]

        lp.suppressed = True
        with self.request_context_with_library_and_admin("/"):
            response = self.manager.admin_work_controller.details(
                lp.identifier.type, lp.identifier.identifier
            )
            eq_(200, response.status_code)
            feed = feedparser.parse(response.get_data())
            [entry] = feed['entries']
            suppress_links = [x['href'] for x in entry['links']
                              if x['rel'] == "http://librarysimplified.org/terms/rel/hide"]
            unsuppress_links = [x['href'] for x in entry['links']
                                if x['rel'] == "http://librarysimplified.org/terms/rel/restore"]
            eq_(0, len(suppress_links))
            eq_(1, len(unsuppress_links))
            assert lp.identifier.identifier in unsuppress_links[0]

        self.admin.remove_role(AdminRole.LIBRARIAN, self._default_library)
        with self.request_context_with_library_and_admin("/"):
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_work_controller.details,
                          lp.identifier.type, lp.identifier.identifier)

    def test_roles(self):
        roles = self.manager.admin_work_controller.roles()
        assert Contributor.ILLUSTRATOR_ROLE in roles.values()
        assert Contributor.NARRATOR_ROLE in roles.values()
        eq_(Contributor.ILLUSTRATOR_ROLE,
            roles[Contributor.MARC_ROLE_CODES[Contributor.ILLUSTRATOR_ROLE]])
        eq_(Contributor.NARRATOR_ROLE,
            roles[Contributor.MARC_ROLE_CODES[Contributor.NARRATOR_ROLE]])

    def test_languages(self):
        languages = self.manager.admin_work_controller.languages()
        assert 'en' in languages.keys()
        assert 'fre' in languages.keys()
        names = [name for sublist in languages.values() for name in sublist]
        assert 'English' in names
        assert 'French' in names

    def test_media(self):
        media = self.manager.admin_work_controller.media()
        assert Edition.BOOK_MEDIUM in media.values()
        assert Edition.medium_to_additional_type[Edition.BOOK_MEDIUM] in media.keys()

    def test_rights_status(self):
        rights_status = self.manager.admin_work_controller.rights_status()

        public_domain = rights_status.get(RightsStatus.PUBLIC_DOMAIN_USA)
        eq_(RightsStatus.NAMES.get(RightsStatus.PUBLIC_DOMAIN_USA), public_domain.get("name"))
        eq_(True, public_domain.get("open_access"))
        eq_(True, public_domain.get("allows_derivatives"))

        cc_by = rights_status.get(RightsStatus.CC_BY)
        eq_(RightsStatus.NAMES.get(RightsStatus.CC_BY), cc_by.get("name"))
        eq_(True, cc_by.get("open_access"))
        eq_(True, cc_by.get("allows_derivatives"))

        cc_by_nd = rights_status.get(RightsStatus.CC_BY_ND)
        eq_(RightsStatus.NAMES.get(RightsStatus.CC_BY_ND), cc_by_nd.get("name"))
        eq_(True, cc_by_nd.get("open_access"))
        eq_(False, cc_by_nd.get("allows_derivatives"))

        copyright = rights_status.get(RightsStatus.IN_COPYRIGHT)
        eq_(RightsStatus.NAMES.get(RightsStatus.IN_COPYRIGHT), copyright.get("name"))
        eq_(False, copyright.get("open_access"))
        eq_(False, copyright.get("allows_derivatives"))

    def _make_test_edit_request(self, data):
        [lp] = self.english_1.license_pools
        with self.request_context_with_library_and_admin("/"):
            flask.request.form = ImmutableMultiDict(data)
            return self.manager.admin_work_controller.edit(
                lp.identifier.type, lp.identifier.identifier
            )

    def test_edit_unknown_role(self):
        response = self._make_test_edit_request(
            [('contributor-role', self._str),
             ('contributor-name', self._str)])
        eq_(400, response.status_code)
        eq_(UNKNOWN_ROLE.uri, response.uri)

    def test_edit_invalid_series_position(self):
        response = self._make_test_edit_request(
            [('series', self._str),
             ('series_position', 'five')])
        eq_(400, response.status_code)
        eq_(INVALID_SERIES_POSITION.uri, response.uri)

    def test_edit_unknown_medium(self):
        response = self._make_test_edit_request(
            [('medium', self._str)])
        eq_(400, response.status_code)
        eq_(UNKNOWN_MEDIUM.uri, response.uri)

    def test_edit_unknown_language(self):
        response = self._make_test_edit_request(
            [('language', self._str)])
        eq_(400, response.status_code)
        eq_(UNKNOWN_LANGUAGE.uri, response.uri)

    def test_edit_invalid_date_format(self):
        response = self._make_test_edit_request(
            [('issued', self._str)])
        eq_(400, response.status_code)
        eq_(INVALID_DATE_FORMAT.uri, response.uri)

    def test_edit_invalid_rating_not_number(self):
        response = self._make_test_edit_request(
            [('rating', 'abc')])
        eq_(400, response.status_code)
        eq_(INVALID_RATING.uri, response.uri)

    def test_edit_invalid_rating_above_scale(self):
        response = self._make_test_edit_request(
            [('rating', 9999)])
        eq_(400, response.status_code)
        eq_(INVALID_RATING.uri, response.uri)

    def test_edit_invalid_rating_below_scale(self):
        response = self._make_test_edit_request(
            [('rating', -3)])
        eq_(400, response.status_code)
        eq_(INVALID_RATING.uri, response.uri)

    def test_edit(self):
        [lp] = self.english_1.license_pools

        staff_data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        def staff_edition_count():
            return self._db.query(Edition) \
                .filter(
                    Edition.data_source == staff_data_source,
                    Edition.primary_identifier_id == self.english_1.presentation_edition.primary_identifier.id
                ) \
                .count()

        with self.request_context_with_library_and_admin("/"):
            flask.request.form = ImmutableMultiDict([
                ("title", "New title"),
                ("subtitle", "New subtitle"),
                ("contributor-role", "Author"),
                ("contributor-name", "New Author"),
                ("contributor-role", "Narrator"),
                ("contributor-name", "New Narrator"),
                ("series", "New series"),
                ("series_position", "144"),
                ("medium", "Audio"),
                ("language", "French"),
                ("publisher", "New Publisher"),
                ("imprint", "New Imprint"),
                ("issued", "2017-11-05"),
                ("rating", "2"),
                ("summary", "<p>New summary</p>")
            ])
            response = self.manager.admin_work_controller.edit(
                lp.identifier.type, lp.identifier.identifier
            )
            eq_(200, response.status_code)
            eq_("New title", self.english_1.title)
            assert "New title" in self.english_1.simple_opds_entry
            eq_("New subtitle", self.english_1.subtitle)
            assert "New subtitle" in self.english_1.simple_opds_entry
            eq_("New Author", self.english_1.author)
            assert "New Author" in self.english_1.simple_opds_entry
            [author, narrator] = sorted(
                self.english_1.presentation_edition.contributions,
                key=lambda x: x.contributor.display_name)
            eq_("New Author", author.contributor.display_name)
            eq_("Author, New", author.contributor.sort_name)
            eq_("Primary Author", author.role)
            eq_("New Narrator", narrator.contributor.display_name)
            eq_("Narrator, New", narrator.contributor.sort_name)
            eq_("Narrator", narrator.role)
            eq_("New series", self.english_1.series)
            assert "New series" in self.english_1.simple_opds_entry
            eq_(144, self.english_1.series_position)
            assert "144" in self.english_1.simple_opds_entry
            eq_("Audio", self.english_1.presentation_edition.medium)
            eq_("fre", self.english_1.presentation_edition.language)
            eq_("New Publisher", self.english_1.publisher)
            eq_("New Imprint", self.english_1.presentation_edition.imprint)
            eq_(datetime(2017, 11, 5), self.english_1.presentation_edition.issued)
            eq_(0.25, self.english_1.quality)
            eq_("<p>New summary</p>", self.english_1.summary_text)
            assert "&lt;p&gt;New summary&lt;/p&gt;" in self.english_1.simple_opds_entry
            eq_(1, staff_edition_count())

        with self.request_context_with_library_and_admin("/"):
            # Change the summary again and add an author.
            flask.request.form = ImmutableMultiDict([
                ("title", "New title"),
                ("subtitle", "New subtitle"),
                ("contributor-role", "Author"),
                ("contributor-name", "New Author"),
                ("contributor-role", "Narrator"),
                ("contributor-name", "New Narrator"),
                ("contributor-role", "Author"),
                ("contributor-name", "Second Author"),
                ("series", "New series"),
                ("series_position", "144"),
                ("medium", "Audio"),
                ("language", "French"),
                ("publisher", "New Publisher"),
                ("imprint", "New Imprint"),
                ("issued", "2017-11-05"),
                ("rating", "2"),
                ("summary", "abcd")
            ])
            response = self.manager.admin_work_controller.edit(
                lp.identifier.type, lp.identifier.identifier
            )
            eq_(200, response.status_code)
            eq_("abcd", self.english_1.summary_text)
            assert 'New summary' not in self.english_1.simple_opds_entry
            [author, narrator, author2] = sorted(
                self.english_1.presentation_edition.contributions,
                key=lambda x: x.contributor.display_name)
            eq_("New Author", author.contributor.display_name)
            eq_("Author, New", author.contributor.sort_name)
            eq_("Primary Author", author.role)
            eq_("New Narrator", narrator.contributor.display_name)
            eq_("Narrator, New", narrator.contributor.sort_name)
            eq_("Narrator", narrator.role)
            eq_("Second Author", author2.contributor.display_name)
            eq_("Author", author2.role)
            eq_(1, staff_edition_count())

        with self.request_context_with_library_and_admin("/"):
            # Now delete the subtitle, narrator, series, and summary entirely
            flask.request.form = ImmutableMultiDict([
                ("title", "New title"),
                ("contributor-role", "Author"),
                ("contributor-name", "New Author"),
                ("subtitle", ""),
                ("series", ""),
                ("series_position", ""),
                ("medium", "Audio"),
                ("language", "French"),
                ("publisher", "New Publisher"),
                ("imprint", "New Imprint"),
                ("issued", "2017-11-05"),
                ("rating", "2"),
                ("summary", "")
            ])
            response = self.manager.admin_work_controller.edit(
                lp.identifier.type, lp.identifier.identifier
            )
            eq_(200, response.status_code)
            eq_(None, self.english_1.subtitle)
            [author] = self.english_1.presentation_edition.contributions
            eq_("New Author", author.contributor.display_name)
            eq_(None, self.english_1.series)
            eq_(None, self.english_1.series_position)
            eq_("", self.english_1.summary_text)
            assert 'New subtitle' not in self.english_1.simple_opds_entry
            assert "Narrator" not in self.english_1.simple_opds_entry
            assert 'New series' not in self.english_1.simple_opds_entry
            assert '144' not in self.english_1.simple_opds_entry
            assert 'abcd' not in self.english_1.simple_opds_entry
            eq_(1, staff_edition_count())

        with self.request_context_with_library_and_admin("/"):
            # Set the fields one more time
            flask.request.form = ImmutableMultiDict([
                ("title", "New title"),
                ("subtitle", "Final subtitle"),
                ("series", "Final series"),
                ("series_position", "169"),
                ("summary", "<p>Final summary</p>")
            ])
            response = self.manager.admin_work_controller.edit(
                lp.identifier.type, lp.identifier.identifier
            )
            eq_(200, response.status_code)
            eq_("Final subtitle", self.english_1.subtitle)
            eq_("Final series", self.english_1.series)
            eq_(169, self.english_1.series_position)
            eq_("<p>Final summary</p>", self.english_1.summary_text)
            assert 'Final subtitle' in self.english_1.simple_opds_entry
            assert 'Final series' in self.english_1.simple_opds_entry
            assert '169' in self.english_1.simple_opds_entry
            assert "&lt;p&gt;Final summary&lt;/p&gt;" in self.english_1.simple_opds_entry
            eq_(1, staff_edition_count())

        # Make sure a non-librarian of this library can't edit.
        self.admin.remove_role(AdminRole.LIBRARIAN, self._default_library)
        with self.request_context_with_library_and_admin("/"):
            flask.request.form = ImmutableMultiDict([
                ("title", "Another new title"),
            ])
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_work_controller.edit,
                          lp.identifier.type, lp.identifier.identifier)

    def test_edit_classifications(self):
        # start with a couple genres based on BISAC classifications from Axis 360
        work = self.english_1
        [lp] = work.license_pools
        primary_identifier = work.presentation_edition.primary_identifier
        work.audience = "Adult"
        work.fiction = True
        axis_360 = DataSource.lookup(self._db, DataSource.AXIS_360)
        classification1 = primary_identifier.classify(
            data_source=axis_360,
            subject_type=Subject.BISAC,
            subject_identifier="FICTION / Horror",
            weight=1
        )
        classification2 = primary_identifier.classify(
            data_source=axis_360,
            subject_type=Subject.BISAC,
            subject_identifier="FICTION / Science Fiction / Time Travel",
            weight=1
        )
        genre1, ignore = Genre.lookup(self._db, "Horror")
        genre2, ignore = Genre.lookup(self._db, "Science Fiction")
        work.genres = [genre1, genre2]

        # make no changes
        with self.request_context_with_library_and_admin("/"):
            flask.request.form = MultiDict([
                ("audience", "Adult"),
                ("fiction", "fiction"),
                ("genres", "Horror"),
                ("genres", "Science Fiction")
            ])
            requested_genres = flask.request.form.getlist("genres")
            response = self.manager.admin_work_controller.edit_classifications(
                lp.identifier.type, lp.identifier.identifier
            )
            eq_(response.status_code, 200)

        staff_data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        genre_classifications = self._db \
            .query(Classification) \
            .join(Subject) \
            .filter(
                Classification.identifier == primary_identifier,
                Classification.data_source == staff_data_source,
                Subject.genre_id != None
            )
        staff_genres = [
            c.subject.genre.name
            for c in genre_classifications
            if c.subject.genre
        ]
        eq_(staff_genres, [])
        eq_("Adult", work.audience)
        eq_(18, work.target_age.lower)
        eq_(None, work.target_age.upper)
        eq_(True, work.fiction)

        # remove all genres
        with self.request_context_with_library_and_admin("/"):
            flask.request.form = MultiDict([
                ("audience", "Adult"),
                ("fiction", "fiction")
            ])
            response = self.manager.admin_work_controller.edit_classifications(
                lp.identifier.type, lp.identifier.identifier
            )
            eq_(response.status_code, 200)

        primary_identifier = work.presentation_edition.primary_identifier
        staff_data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        none_classification_count = self._db \
            .query(Classification) \
            .join(Subject) \
            .filter(
                Classification.identifier == primary_identifier,
                Classification.data_source == staff_data_source,
                Subject.identifier == SimplifiedGenreClassifier.NONE
            ) \
            .all()
        eq_(1, len(none_classification_count))
        eq_("Adult", work.audience)
        eq_(18, work.target_age.lower)
        eq_(None, work.target_age.upper)
        eq_(True, work.fiction)

        # completely change genres
        with self.request_context_with_library_and_admin("/"):
            flask.request.form = MultiDict([
                ("audience", "Adult"),
                ("fiction", "fiction"),
                ("genres", "Drama"),
                ("genres", "Urban Fantasy"),
                ("genres", "Women's Fiction")
            ])
            requested_genres = flask.request.form.getlist("genres")
            response = self.manager.admin_work_controller.edit_classifications(
                lp.identifier.type, lp.identifier.identifier
            )
            eq_(response.status_code, 200)

        new_genre_names = [work_genre.genre.name for work_genre in work.work_genres]

        eq_(sorted(new_genre_names), sorted(requested_genres))
        eq_("Adult", work.audience)
        eq_(18, work.target_age.lower)
        eq_(None, work.target_age.upper)
        eq_(True, work.fiction)

        # remove some genres and change audience and target age
        with self.request_context_with_library_and_admin("/"):
            flask.request.form = MultiDict([
                ("audience", "Young Adult"),
                ("target_age_min", 16),
                ("target_age_max", 18),
                ("fiction", "fiction"),
                ("genres", "Urban Fantasy")
            ])
            requested_genres = flask.request.form.getlist("genres")
            response = self.manager.admin_work_controller.edit_classifications(
                lp.identifier.type, lp.identifier.identifier
            )
            eq_(response.status_code, 200)

        # new_genre_names = self._db.query(WorkGenre).filter(WorkGenre.work_id == work.id).all()
        new_genre_names = [work_genre.genre.name for work_genre in work.work_genres]
        eq_(sorted(new_genre_names), sorted(requested_genres))
        eq_("Young Adult", work.audience)
        eq_(16, work.target_age.lower)
        eq_(19, work.target_age.upper)
        eq_(True, work.fiction)

        previous_genres = new_genre_names

        # try to add a nonfiction genre
        with self.request_context_with_library_and_admin("/"):
            flask.request.form = MultiDict([
                ("audience", "Young Adult"),
                ("target_age_min", 16),
                ("target_age_max", 18),
                ("fiction", "fiction"),
                ("genres", "Cooking"),
                ("genres", "Urban Fantasy")
            ])
            response = self.manager.admin_work_controller.edit_classifications(
                lp.identifier.type, lp.identifier.identifier
            )

        eq_(response, INCOMPATIBLE_GENRE)
        new_genre_names = [work_genre.genre.name for work_genre in work.work_genres]
        eq_(sorted(new_genre_names), sorted(previous_genres))
        eq_("Young Adult", work.audience)
        eq_(16, work.target_age.lower)
        eq_(19, work.target_age.upper)
        eq_(True, work.fiction)

        # try to add Erotica
        with self.request_context_with_library_and_admin("/"):
            flask.request.form = MultiDict([
                ("audience", "Young Adult"),
                ("target_age_min", 16),
                ("target_age_max", 18),
                ("fiction", "fiction"),
                ("genres", "Erotica"),
                ("genres", "Urban Fantasy")
            ])
            response = self.manager.admin_work_controller.edit_classifications(
                lp.identifier.type, lp.identifier.identifier
            )
            eq_(response, EROTICA_FOR_ADULTS_ONLY)

        new_genre_names = [work_genre.genre.name for work_genre in work.work_genres]
        eq_(sorted(new_genre_names), sorted(previous_genres))
        eq_("Young Adult", work.audience)
        eq_(16, work.target_age.lower)
        eq_(19, work.target_age.upper)
        eq_(True, work.fiction)

        # try to set min target age greater than max target age
        # othe edits should not go through
        with self.request_context_with_library_and_admin("/"):
            flask.request.form = MultiDict([
                ("audience", "Young Adult"),
                ("target_age_min", 16),
                ("target_age_max", 14),
                ("fiction", "nonfiction"),
                ("genres", "Cooking")
            ])
            response = self.manager.admin_work_controller.edit_classifications(
                lp.identifier.type, lp.identifier.identifier
            )
            eq_(400, response.status_code)
            eq_(INVALID_EDIT.uri, response.uri)

        new_genre_names = [work_genre.genre.name for work_genre in work.work_genres]
        eq_(sorted(new_genre_names), sorted(previous_genres))
        eq_(True, work.fiction)

        # change to nonfiction with nonfiction genres and new target age
        with self.request_context_with_library_and_admin("/"):
            flask.request.form = MultiDict([
                ("audience", "Young Adult"),
                ("target_age_min", 15),
                ("target_age_max", 17),
                ("fiction", "nonfiction"),
                ("genres", "Cooking")
            ])
            requested_genres = flask.request.form.getlist("genres")
            response = self.manager.admin_work_controller.edit_classifications(
                lp.identifier.type, lp.identifier.identifier
            )

        new_genre_names = [work_genre.genre.name for work_genre in lp.work.work_genres]
        eq_(sorted(new_genre_names), sorted(requested_genres))
        eq_("Young Adult", work.audience)
        eq_(15, work.target_age.lower)
        eq_(18, work.target_age.upper)
        eq_(False, work.fiction)

        # set to Adult and make sure that target ages is set automatically
        with self.request_context_with_library_and_admin("/"):
            flask.request.form = MultiDict([
                ("audience", "Adult"),
                ("fiction", "nonfiction"),
                ("genres", "Cooking")
            ])
            requested_genres = flask.request.form.getlist("genres")
            response = self.manager.admin_work_controller.edit_classifications(
                lp.identifier.type, lp.identifier.identifier
            )

        eq_("Adult", work.audience)
        eq_(18, work.target_age.lower)
        eq_(None, work.target_age.upper)

        # Make sure a non-librarian of this library can't edit.
        self.admin.remove_role(AdminRole.LIBRARIAN, self._default_library)
        with self.request_context_with_library_and_admin("/"):
            flask.request.form = MultiDict([
                ("audience", "Children"),
                ("fiction", "nonfiction"),
                ("genres", "Biography")
            ])
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_work_controller.edit_classifications,
                          lp.identifier.type, lp.identifier.identifier)

    def test_suppress(self):
        [lp] = self.english_1.license_pools

        with self.request_context_with_library_and_admin("/"):
            response = self.manager.admin_work_controller.suppress(
                lp.identifier.type, lp.identifier.identifier
            )
            eq_(200, response.status_code)
            eq_(True, lp.suppressed)

        lp.suppressed = False
        self.admin.remove_role(AdminRole.LIBRARIAN, self._default_library)
        with self.request_context_with_library_and_admin("/"):
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_work_controller.suppress,
                          lp.identifier.type, lp.identifier.identifier)

    def test_unsuppress(self):
        [lp] = self.english_1.license_pools
        lp.suppressed = True

        broken_lp = self._licensepool(
            self.english_1.presentation_edition,
            data_source_name=DataSource.OVERDRIVE
        )
        broken_lp.work = self.english_1
        broken_lp.suppressed = True

        # The broken LicensePool doesn't render properly.
        Complaint.register(
            broken_lp,
            "http://librarysimplified.org/terms/problem/cannot-render",
            "blah", "blah"
        )

        with self.request_context_with_library_and_admin("/"):
            response = self.manager.admin_work_controller.unsuppress(
                lp.identifier.type, lp.identifier.identifier
            )

            # Both LicensePools are unsuppressed, even though one of them
            # has a LicensePool-specific complaint.
            eq_(200, response.status_code)
            eq_(False, lp.suppressed)
            eq_(False, broken_lp.suppressed)

        lp.suppressed = True
        self.admin.remove_role(AdminRole.LIBRARIAN, self._default_library)
        with self.request_context_with_library_and_admin("/"):
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_work_controller.unsuppress,
                          lp.identifier.type, lp.identifier.identifier)

    def test_refresh_metadata(self):
        wrangler = DataSource.lookup(self._db, DataSource.METADATA_WRANGLER)

        class AlwaysSuccessfulMetadataProvider(AlwaysSuccessfulCoverageProvider):
            DATA_SOURCE_NAME = wrangler.name
        success_provider = AlwaysSuccessfulMetadataProvider(self._db)

        class NeverSuccessfulMetadataProvider(NeverSuccessfulCoverageProvider):
            DATA_SOURCE_NAME = wrangler.name
        failure_provider = NeverSuccessfulMetadataProvider(self._db)

        with self.request_context_with_library_and_admin('/'):
            [lp] = self.english_1.license_pools
            response = self.manager.admin_work_controller.refresh_metadata(
                lp.identifier.type, lp.identifier.identifier, provider=success_provider
            )
            eq_(200, response.status_code)
            # Also, the work has a coverage record now for the wrangler.
            assert CoverageRecord.lookup(lp.identifier, wrangler)

            response = self.manager.admin_work_controller.refresh_metadata(
                lp.identifier.type, lp.identifier.identifier, provider=failure_provider
            )
            eq_(METADATA_REFRESH_FAILURE.status_code, response.status_code)
            eq_(METADATA_REFRESH_FAILURE.detail, response.detail)

        self.admin.remove_role(AdminRole.LIBRARIAN, self._default_library)
        with self.request_context_with_library_and_admin("/"):
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_work_controller.refresh_metadata,
                          lp.identifier.type, lp.identifier.identifier, provider=success_provider)

    def test_complaints(self):
        type = iter(Complaint.VALID_TYPES)
        type1 = next(type)
        type2 = next(type)

        work = self._work(
            "fiction work with complaint",
            language="eng",
            fiction=True,
            with_open_access_download=True)
        complaint1 = self._complaint(
            work.license_pools[0],
            type1,
            "complaint1 source",
            "complaint1 detail")
        complaint2 = self._complaint(
            work.license_pools[0],
            type1,
            "complaint2 source",
            "complaint2 detail")
        complaint3 = self._complaint(
            work.license_pools[0],
            type2,
            "complaint3 source",
            "complaint3 detail")

        SessionManager.refresh_materialized_views(self._db)
        [lp] = work.license_pools

        with self.request_context_with_library_and_admin("/"):
            response = self.manager.admin_work_controller.complaints(
                lp.identifier.type, lp.identifier.identifier
            )
            eq_(response['book']['identifier_type'], lp.identifier.type)
            eq_(response['book']['identifier'], lp.identifier.identifier)
            eq_(response['complaints'][type1], 2)
            eq_(response['complaints'][type2], 1)

        self.admin.remove_role(AdminRole.LIBRARIAN, self._default_library)
        with self.request_context_with_library_and_admin("/"):
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_work_controller.complaints,
                          lp.identifier.type, lp.identifier.identifier)

    def test_resolve_complaints(self):
        type = iter(Complaint.VALID_TYPES)
        type1 = next(type)
        type2 = next(type)

        work = self._work(
            "fiction work with complaint",
            language="eng",
            fiction=True,
            with_open_access_download=True)
        complaint1 = self._complaint(
            work.license_pools[0],
            type1,
            "complaint1 source",
            "complaint1 detail")
        complaint2 = self._complaint(
            work.license_pools[0],
            type1,
            "complaint2 source",
            "complaint2 detail")

        SessionManager.refresh_materialized_views(self._db)
        [lp] = work.license_pools

        # first attempt to resolve complaints of the wrong type
        with self.request_context_with_library_and_admin("/"):
            flask.request.form = ImmutableMultiDict([("type", type2)])
            response = self.manager.admin_work_controller.resolve_complaints(
                lp.identifier.type, lp.identifier.identifier
            )
            unresolved_complaints = [complaint for complaint in lp.complaints if complaint.resolved == None]
            eq_(response.status_code, 404)
            eq_(len(unresolved_complaints), 2)

        # then attempt to resolve complaints of the correct type
        with self.request_context_with_library_and_admin("/"):
            flask.request.form = ImmutableMultiDict([("type", type1)])
            response = self.manager.admin_work_controller.resolve_complaints(
                lp.identifier.type, lp.identifier.identifier
            )
            unresolved_complaints = [complaint for complaint in lp.complaints
                                               if complaint.resolved == None]
            eq_(response.status_code, 200)
            eq_(len(unresolved_complaints), 0)

        # then attempt to resolve the already-resolved complaints of the correct type
        with self.request_context_with_library_and_admin("/"):
            flask.request.form = ImmutableMultiDict([("type", type1)])
            response = self.manager.admin_work_controller.resolve_complaints(
                lp.identifier.type, lp.identifier.identifier
            )
            eq_(response.status_code, 409)

        self.admin.remove_role(AdminRole.LIBRARIAN, self._default_library)
        with self.request_context_with_library_and_admin("/"):
            flask.request.form = ImmutableMultiDict([("type", type1)])
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_work_controller.resolve_complaints,
                          lp.identifier.type, lp.identifier.identifier)

    def test_classifications(self):
        e, pool = self._edition(with_license_pool=True)
        work = self._work(presentation_edition=e)
        identifier = work.presentation_edition.primary_identifier
        genres = self._db.query(Genre).all()
        subject1 = self._subject(type="type1", identifier="subject1")
        subject1.genre = genres[0]
        subject2 = self._subject(type="type2", identifier="subject2")
        subject2.genre = genres[1]
        subject3 = self._subject(type="type2", identifier="subject3")
        subject3.genre = None
        source = DataSource.lookup(self._db, DataSource.AXIS_360)
        classification1 = self._classification(
            identifier=identifier, subject=subject1,
            data_source=source, weight=1)
        classification2 = self._classification(
            identifier=identifier, subject=subject2,
            data_source=source, weight=3)
        classification3 = self._classification(
            identifier=identifier, subject=subject3,
            data_source=source, weight=2)

        SessionManager.refresh_materialized_views(self._db)
        [lp] = work.license_pools

        with self.request_context_with_library_and_admin("/"):
            response = self.manager.admin_work_controller.classifications(
                lp.identifier.type, lp.identifier.identifier)
            eq_(response['book']['identifier_type'], lp.identifier.type)
            eq_(response['book']['identifier'], lp.identifier.identifier)

            expected_results = [classification2, classification3, classification1]
            eq_(len(response['classifications']), len(expected_results))
            for i, classification in enumerate(expected_results):
                subject = classification.subject
                source = classification.data_source
                eq_(response['classifications'][i]['name'], subject.identifier)
                eq_(response['classifications'][i]['type'], subject.type)
                eq_(response['classifications'][i]['source'], source.name)
                eq_(response['classifications'][i]['weight'], classification.weight)

        self.admin.remove_role(AdminRole.LIBRARIAN, self._default_library)
        with self.request_context_with_library_and_admin("/"):
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_work_controller.classifications,
                          lp.identifier.type, lp.identifier.identifier)

    def test_validate_cover_image(self):
        base_path = os.path.split(__file__)[0]
        folder = os.path.dirname(base_path)
        resource_path = os.path.join(folder, "..", "files", "images")

        path = os.path.join(resource_path, "blue_small.jpg")
        too_small = Image.open(path)

        result = self.manager.admin_work_controller._validate_cover_image(too_small)
        eq_(INVALID_IMAGE.uri, result.uri)
        eq_("Cover image must be at least 600px in width and 900px in height.", result.detail)

        path = os.path.join(resource_path, "blue.jpg")
        valid = Image.open(path)
        result = self.manager.admin_work_controller._validate_cover_image(valid)
        eq_(True, result)

    def test_process_cover_image(self):
        work = self._work(with_license_pool=True, title="Title", authors="Authpr")

        base_path = os.path.split(__file__)[0]
        folder = os.path.dirname(base_path)
        resource_path = os.path.join(folder, "..", "files", "images")
        path = os.path.join(resource_path, "blue.jpg")
        original = Image.open(path)
        processed = Image.open(path)

        # Without a title position, the image won't be changed.
        processed = self.manager.admin_work_controller._process_cover_image(work, processed, "none")

        image_histogram = original.histogram()
        expected_histogram = processed.histogram()

        root_mean_square = math.sqrt(reduce(operator.add,
                                            map(lambda a,b: (a-b)**2, image_histogram, expected_histogram))/len(image_histogram))
        assert root_mean_square < 10

        # Here the title and author are added in the center. Compare the result
        # with a pre-generated version.
        processed = Image.open(path)
        processed = self.manager.admin_work_controller._process_cover_image(work, processed, "center")

        path = os.path.join(resource_path, "blue_with_title_author.png")
        expected_image = Image.open(path)

        image_histogram = processed.histogram()
        expected_histogram = expected_image.histogram()

        root_mean_square = math.sqrt(reduce(operator.add,
                                            map(lambda a,b: (a-b)**2, image_histogram, expected_histogram))/len(image_histogram))
        assert root_mean_square < 10

    def test_preview_book_cover(self):
        work = self._work(with_license_pool=True)
        identifier = work.license_pools[0].identifier

        with self.request_context_with_library_and_admin("/"):
            response = self.manager.admin_work_controller.preview_book_cover(identifier.type, identifier.identifier)
            eq_(INVALID_IMAGE.uri, response.uri)
            eq_("Image file or image URL is required.", response.detail)

        class TestFileUpload(StringIO):
            headers = { "Content-Type": "image/png" }
        base_path = os.path.split(__file__)[0]
        folder = os.path.dirname(base_path)
        resource_path = os.path.join(folder, "..", "files", "images")
        path = os.path.join(resource_path, "blue.jpg")
        original = Image.open(path)
        buffer = StringIO()
        original.save(buffer, format="PNG")
        image_data = buffer.getvalue()

        with self.request_context_with_library_and_admin("/"):
            flask.request.form = MultiDict([
                ("title_position", "none")
            ])
            flask.request.files = MultiDict([
                ("cover_file", TestFileUpload(image_data)),
            ])
            response = self.manager.admin_work_controller.preview_book_cover(identifier.type, identifier.identifier)
            eq_(200, response.status_code)
            eq_("data:image/png;base64,%s" % base64.b64encode(image_data), response.data)

        self.admin.remove_role(AdminRole.LIBRARIAN, self._default_library)
        with self.request_context_with_library_and_admin("/"):
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_work_controller.preview_book_cover,
                          identifier.type, identifier.identifier)


    def test_change_book_cover(self):
        # Mock image processing which has been tested in other methods.
        process_called_with = []
        def mock_process(work, image, position):
            # Modify the image to ensure it gets a different generic URI.
            image.thumbnail((500, 500))
            process_called_with.append((work, image, position))
            return image
        old_process = self.manager.admin_work_controller._process_cover_image
        self.manager.admin_work_controller._process_cover_image = mock_process

        work = self._work(with_license_pool=True)
        identifier = work.license_pools[0].identifier

        with self.request_context_with_library_and_admin("/"):
            response = self.manager.admin_work_controller.change_book_cover(identifier.type, identifier.identifier)
            eq_(INVALID_IMAGE.uri, response.uri)
            eq_("Image file or image URL is required.", response.detail)

        with self.request_context_with_library_and_admin("/"):
            flask.request.form = MultiDict([
                ("cover_url", "http://example.com"),
                ("title_position", "none"),
            ])
            flask.request.files = MultiDict([])
            response = self.manager.admin_work_controller.change_book_cover(identifier.type, identifier.identifier)
            eq_(INVALID_IMAGE.uri, response.uri)
            eq_("You must specify the image's license.", response.detail)

        with self.request_context_with_library_and_admin("/"):
            flask.request.form = MultiDict([
                ("cover_url", "http://example.com"),
                ("title_position", "none"),
                ("rights_status", RightsStatus.CC_BY),
                ("rights_explanation", "explanation"),
            ])
            flask.request.files = MultiDict([])
            response = self.manager.admin_work_controller.change_book_cover(identifier.type, identifier.identifier)
            eq_(INVALID_CONFIGURATION_OPTION.uri, response.uri)
            assert "Could not find a storage integration" in response.detail

        mirror = MockS3Uploader()

        class TestFileUpload(StringIO):
            headers = { "Content-Type": "image/png" }
        base_path = os.path.split(__file__)[0]
        folder = os.path.dirname(base_path)
        resource_path = os.path.join(folder, "..", "files", "images")
        path = os.path.join(resource_path, "blue.jpg")
        original = Image.open(path)
        buffer = StringIO()
        original.save(buffer, format="PNG")
        image_data = buffer.getvalue()

        staff_data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)

        # Upload a new cover image but don't modify it.
        with self.request_context_with_library_and_admin("/"):
            flask.request.form = MultiDict([
                ("title_position", "none"),
                ("rights_status", RightsStatus.CC_BY),
                ("rights_explanation", "explanation"),
            ])
            flask.request.files = MultiDict([
                ("cover_file", TestFileUpload(image_data)),
            ])
            response = self.manager.admin_work_controller.change_book_cover(identifier.type, identifier.identifier, mirror)
            eq_(200, response.status_code)

            [link] = identifier.links
            eq_(Hyperlink.IMAGE, link.rel)
            eq_(staff_data_source, link.data_source)

            resource = link.resource
            assert identifier.urn in resource.url
            eq_(staff_data_source, resource.data_source)
            eq_(RightsStatus.CC_BY, resource.rights_status.uri)
            eq_("explanation", resource.rights_explanation)

            representation = resource.representation
            [thumbnail] = resource.representation.thumbnails

            eq_(resource.url, representation.url)
            eq_(Representation.PNG_MEDIA_TYPE, representation.media_type)
            eq_(Representation.PNG_MEDIA_TYPE, thumbnail.media_type)
            eq_(image_data, representation.content)
            assert identifier.identifier in representation.mirror_url
            assert identifier.identifier in thumbnail.mirror_url

            eq_([], process_called_with)
            eq_([representation, thumbnail], mirror.uploaded)
            eq_([representation.mirror_url, thumbnail.mirror_url], mirror.destinations)

        work = self._work(with_license_pool=True)
        identifier = work.license_pools[0].identifier

        # Upload a new cover image and add the title and author to it.
        # Both the original image and the generated image will become resources.
        with self.request_context_with_library_and_admin("/"):
            flask.request.form = MultiDict([
                ("title_position", "center"),
                ("rights_status", RightsStatus.CC_BY),
                ("rights_explanation", "explanation"),
            ])
            flask.request.files = MultiDict([
                ("cover_file", TestFileUpload(image_data)),
            ])
            response = self.manager.admin_work_controller.change_book_cover(identifier.type, identifier.identifier, mirror)
            eq_(200, response.status_code)

            [link] = identifier.links
            eq_(Hyperlink.IMAGE, link.rel)
            eq_(staff_data_source, link.data_source)

            resource = link.resource
            assert identifier.urn in resource.url
            eq_(staff_data_source, resource.data_source)
            eq_(RightsStatus.CC_BY, resource.rights_status.uri)
            eq_("The original image license allows derivatives.", resource.rights_explanation)

            transformation = self._db.query(ResourceTransformation).filter(ResourceTransformation.derivative_id==resource.id).one()
            original_resource = transformation.original
            assert resource != original_resource
            assert identifier.urn in original_resource.url
            eq_(staff_data_source, original_resource.data_source)
            eq_(RightsStatus.CC_BY, original_resource.rights_status.uri)
            eq_("explanation", original_resource.rights_explanation)
            eq_(image_data, original_resource.representation.content)
            eq_(None, original_resource.representation.mirror_url)
            eq_("center", transformation.settings.get("title_position"))
            assert resource.representation.content != original_resource.representation.content
            assert image_data != resource.representation.content

            eq_(work, process_called_with[0][0])
            eq_("center", process_called_with[0][2])

            eq_([], original_resource.representation.thumbnails)
            [thumbnail] = resource.representation.thumbnails
            eq_(Representation.PNG_MEDIA_TYPE, thumbnail.media_type)
            assert image_data != thumbnail.content
            assert resource.representation.content != thumbnail.content
            assert identifier.identifier in resource.representation.mirror_url
            assert identifier.identifier in thumbnail.mirror_url

            eq_([resource.representation, thumbnail], mirror.uploaded[2:])
            eq_([resource.representation.mirror_url, thumbnail.mirror_url], mirror.destinations[2:])

        self.admin.remove_role(AdminRole.LIBRARIAN, self._default_library)
        with self.request_context_with_library_and_admin("/"):
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_work_controller.preview_book_cover,
                          identifier.type, identifier.identifier)

        self.manager.admin_work_controller._process_cover_image = old_process

    def test_custom_lists_get(self):
        staff_data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        list, ignore = create(self._db, CustomList, name=self._str, library=self._default_library, data_source=staff_data_source)
        work = self._work(with_license_pool=True)
        list.add_entry(work)
        identifier = work.presentation_edition.primary_identifier

        with self.request_context_with_library_and_admin("/"):
            response = self.manager.admin_work_controller.custom_lists(identifier.type, identifier.identifier)
            lists = response.get('custom_lists')
            eq_(1, len(lists))
            eq_(list.id, lists[0].get("id"))
            eq_(list.name, lists[0].get("name"))

        self.admin.remove_role(AdminRole.LIBRARIAN, self._default_library)
        with self.request_context_with_library_and_admin("/"):
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_work_controller.custom_lists,
                          identifier.type, identifier.identifier)

    def test_custom_lists_edit_with_missing_list(self):
        work = self._work(with_license_pool=True)
        identifier = work.presentation_edition.primary_identifier

        with self.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", "4"),
                ("name", "name"),
            ])
            response = self.manager.admin_custom_lists_controller.custom_lists()
            eq_(MISSING_CUSTOM_LIST, response)

    def test_custom_lists_edit_success(self):
        staff_data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        list, ignore = create(self._db, CustomList, name=self._str, library=self._default_library, data_source=staff_data_source)
        work = self._work(with_license_pool=True)
        self.add_to_materialized_view([work])
        identifier = work.presentation_edition.primary_identifier

        # Create a Lane that depends on this CustomList for its membership.
        lane = self._lane()
        lane.customlists.append(list)
        eq_(0, lane.size)

        # Add the list to the work.
        with self.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("lists", json.dumps([{ "id": str(list.id), "name": list.name }]))
            ])
            response = self.manager.admin_work_controller.custom_lists(identifier.type, identifier.identifier)
            eq_(200, response.status_code)
            eq_(1, len(work.custom_list_entries))
            eq_(1, len(list.entries))
            eq_(list, work.custom_list_entries[0].customlist)
            eq_(True, work.custom_list_entries[0].featured)

            # The lane's size will be updated once the materialized
            # views are refreshed.
            SessionManager.refresh_materialized_views(self._db)
            eq_(1, lane.size)

        # Now remove the list.
        with self.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("lists", json.dumps([])),
            ])
            response = self.manager.admin_work_controller.custom_lists(identifier.type, identifier.identifier)
        eq_(200, response.status_code)
        eq_(0, len(work.custom_list_entries))
        eq_(0, len(list.entries))
        eq_(0, lane.size)

        # Add a list that didn't exist before.
        with self.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("lists", json.dumps([{ "name": "new list" }]))
            ])
            response = self.manager.admin_work_controller.custom_lists(identifier.type, identifier.identifier)
        eq_(200, response.status_code)
        eq_(1, len(work.custom_list_entries))
        new_list = CustomList.find(self._db, "new list", staff_data_source, self._default_library)
        eq_(new_list, work.custom_list_entries[0].customlist)
        eq_(True, work.custom_list_entries[0].featured)

        self.admin.remove_role(AdminRole.LIBRARIAN, self._default_library)
        with self.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("lists", json.dumps([{ "name": "another new list" }]))
            ])
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_work_controller.custom_lists,
                          identifier.type, identifier.identifier)

class TestSignInController(AdminControllerTest):

    def setup(self):
        super(TestSignInController, self).setup()
        self.admin.credential = json.dumps({
            u'access_token': u'abc123',
            u'client_id': u'', u'client_secret': u'',
            u'refresh_token': u'', u'token_expiry': u'', u'token_uri': u'',
            u'user_agent': u'', u'invalid': u''
        })
        self.admin.password_hashed = None

    def test_admin_auth_providers(self):
        with self.app.test_request_context('/admin'):
            ctrl = self.manager.admin_sign_in_controller

            # An admin exists, but they have no password and there's
            # no auth service set up.
            eq_([], ctrl.admin_auth_providers)

            # The auth service exists.
            create(
                self._db, ExternalIntegration,
                protocol=ExternalIntegration.GOOGLE_OAUTH,
                goal=ExternalIntegration.ADMIN_AUTH_GOAL
            )
            eq_(1, len(ctrl.admin_auth_providers))
            eq_(GoogleOAuthAdminAuthenticationProvider.NAME, ctrl.admin_auth_providers[0].NAME)

            # Here's another admin with a password.
            pw_admin, ignore = create(self._db, Admin, email="pw@nypl.org")
            pw_admin.password = "password"
            eq_(2, len(ctrl.admin_auth_providers))
            eq_(set([GoogleOAuthAdminAuthenticationProvider.NAME, PasswordAdminAuthenticationProvider.NAME]),
                set([provider.NAME for provider in ctrl.admin_auth_providers]))

            # Only an admin with a password.
            self._db.delete(self.admin)
            eq_(2, len(ctrl.admin_auth_providers))
            eq_(set([GoogleOAuthAdminAuthenticationProvider.NAME, PasswordAdminAuthenticationProvider.NAME]),
                set([provider.NAME for provider in ctrl.admin_auth_providers]))

            # No admins. Someone new could still log in with google if domains are
            # configured.
            self._db.delete(pw_admin)
            eq_(1, len(ctrl.admin_auth_providers))
            eq_(GoogleOAuthAdminAuthenticationProvider.NAME, ctrl.admin_auth_providers[0].NAME)

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
            eq_(None, auth)

            # Here's another admin with a password.
            pw_admin, ignore = create(self._db, Admin, email="pw@nypl.org")
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
            eq_(ADMIN_AUTH_NOT_CONFIGURED, response)

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
            eq_(self.admin, response)

        # Returns an error if you aren't authenticated.
        with self.app.test_request_context('/admin'):
            # You get back a problem detail when you're not authenticated.
            response = self.manager.admin_sign_in_controller.authenticated_admin_from_request()
            eq_(401, response.status_code)
            eq_(INVALID_ADMIN_CREDENTIALS.detail, response.detail)

        # Returns an error if the admin email or auth type is missing from the session.
        with self.app.test_request_context('/admin'):
            flask.session['auth_type'] = GoogleOAuthAdminAuthenticationProvider.NAME
            response = self.manager.admin_sign_in_controller.authenticated_admin_from_request()
            eq_(401, response.status_code)
            eq_(INVALID_ADMIN_CREDENTIALS.detail, response.detail)

        with self.app.test_request_context('/admin'):
            flask.session['admin_email'] = self.admin.email
            response = self.manager.admin_sign_in_controller.authenticated_admin_from_request()
            eq_(401, response.status_code)
            eq_(INVALID_ADMIN_CREDENTIALS.detail, response.detail)

        # Returns an error if the admin authentication type isn't configured.
        with self.app.test_request_context('/admin'):
            flask.session['admin_email'] = self.admin.email
            flask.session['auth_type'] = PasswordAdminAuthenticationProvider.NAME
            response = self.manager.admin_sign_in_controller.authenticated_admin_from_request()
            eq_(400, response.status_code)
            eq_(ADMIN_AUTH_MECHANISM_NOT_CONFIGURED.detail, response.detail)

    def test_authenticated_admin(self):

        # Unset the base URL -- it will be set automatically when we
        # successfully authenticate as an admin.
        base_url = ConfigurationSetting.sitewide(
            self._db, Configuration.BASE_URL_KEY
        )
        base_url.value = None
        eq_(None, base_url.value)


        # Creates a new admin with fresh details.
        new_admin_details = {
            'email' : u'admin@nypl.org',
            'credentials' : u'gnarly',
            'type': GoogleOAuthAdminAuthenticationProvider.NAME,
            'roles': [{ "role": AdminRole.LIBRARY_MANAGER, "library": self._default_library.short_name }],
        }
        with self.app.test_request_context('/admin/sign_in?redirect=foo'):
            flask.request.url = "http://chosen-hostname/admin/sign_in?redirect=foo"
            admin = self.manager.admin_sign_in_controller.authenticated_admin(new_admin_details)
            eq_('admin@nypl.org', admin.email)
            eq_('gnarly', admin.credential)
            [role] = admin.roles
            eq_(AdminRole.LIBRARY_MANAGER, role.role)
            eq_(self._default_library, role.library)

            # Also sets up the admin's flask session.
            eq_("admin@nypl.org", flask.session["admin_email"])
            eq_(GoogleOAuthAdminAuthenticationProvider.NAME, flask.session["auth_type"])
            eq_(True, flask.session.permanent)

        # The first successfully authenticated admin user automatically
        # sets the site's base URL.
        eq_("http://chosen-hostname/", base_url.value)

        # Or overwrites credentials for an existing admin.
        existing_admin_details = {
            'email' : u'example@nypl.org',
            'credentials' : u'b-a-n-a-n-a-s',
            'type': GoogleOAuthAdminAuthenticationProvider.NAME,
            'roles': [{ "role": AdminRole.LIBRARY_MANAGER, "library": self._default_library.short_name }],
        }
        with self.app.test_request_context('/admin/sign_in?redirect=foo'):
            flask.request.url = "http://a-different-hostname/"
            admin = self.manager.admin_sign_in_controller.authenticated_admin(existing_admin_details)
            eq_(self.admin.id, admin.id)
            eq_('b-a-n-a-n-a-s', self.admin.credential)
            # No roles were created since the admin already existed.
            eq_([], admin.roles)

        # We already set the site's base URL, and it doesn't get set
        # to a different value just because someone authenticated
        # through a different hostname.
        eq_("http://chosen-hostname/", base_url.value)

    def test_admin_signin(self):
        # Returns an error if there's no admin auth service.
        with self.app.test_request_context('/admin/sign_in?redirect=foo'):
            response = self.manager.admin_sign_in_controller.sign_in()
            eq_(ADMIN_AUTH_NOT_CONFIGURED, response)

        create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.GOOGLE_OAUTH,
            goal=ExternalIntegration.ADMIN_AUTH_GOAL
        )

        # Shows the login page if there's an auth service
        # but no signed in admin.
        with self.app.test_request_context('/admin/sign_in?redirect=foo'):
            response = self.manager.admin_sign_in_controller.sign_in()
            eq_(200, response.status_code)
            assert "GOOGLE REDIRECT" in response.data
            assert "Sign in with Google" in response.data
            assert "Email" not in response.data
            assert "Password" not in response.data

        # If there are multiple auth providers, the login page
        # shows them all.
        self.admin.password = "password"
        with self.app.test_request_context('/admin/sign_in?redirect=foo'):
            response = self.manager.admin_sign_in_controller.sign_in()
            eq_(200, response.status_code)
            assert "GOOGLE REDIRECT" in response.data
            assert "Sign in with Google" in response.data
            assert "Email" in response.data
            assert "Password" in response.data

        # Redirects to the redirect parameter if an admin is signed in.
        with self.app.test_request_context('/admin/sign_in?redirect=foo'):
            flask.session['admin_email'] = self.admin.email
            flask.session['auth_type'] = PasswordAdminAuthenticationProvider.NAME
            response = self.manager.admin_sign_in_controller.sign_in()
            eq_(302, response.status_code)
            eq_("foo", response.headers["Location"])

    def test_redirect_after_google_sign_in(self):
        self._db.delete(self.admin)

        # Returns an error if there's no admin auth service.
        with self.app.test_request_context('/admin/GoogleOAuth/callback'):
            response = self.manager.admin_sign_in_controller.redirect_after_google_sign_in()
            eq_(ADMIN_AUTH_NOT_CONFIGURED, response)

        # Returns an error if the admin auth service isn't google.
        admin, ignore = create(self._db, Admin, email="admin@nypl.org")
        admin.password = "password"
        with self.app.test_request_context('/admin/GoogleOAuth/callback'):
            response = self.manager.admin_sign_in_controller.redirect_after_google_sign_in()
            eq_(ADMIN_AUTH_MECHANISM_NOT_CONFIGURED, response)

        self._db.delete(admin)
        auth_integration, ignore = create(
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
            eq_(400, response.status_code)

        # Returns an error if the admin email isn't a staff email.
        setting.value = json.dumps(["alibrary.org"])
        with self.app.test_request_context('/admin/GoogleOAuth/callback?code=1234&state=foo'):
            response = self.manager.admin_sign_in_controller.redirect_after_google_sign_in()
            eq_(401, response.status_code)

        # Redirects to the state parameter if the admin email is valid.
        setting.value = json.dumps(["nypl.org"])
        with self.app.test_request_context('/admin/GoogleOAuth/callback?code=1234&state=foo'):
            response = self.manager.admin_sign_in_controller.redirect_after_google_sign_in()
            eq_(302, response.status_code)
            eq_("foo", response.headers["Location"])

    def test_password_sign_in(self):
        # Returns an error if there's no admin auth service and no admins.
        with self.app.test_request_context('/admin/sign_in_with_password'):
            response = self.manager.admin_sign_in_controller.password_sign_in()
            eq_(ADMIN_AUTH_NOT_CONFIGURED, response)

        # Returns an error if the admin auth service isn't password auth.
        auth_integration, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.GOOGLE_OAUTH,
            goal=ExternalIntegration.ADMIN_AUTH_GOAL
        )
        with self.app.test_request_context('/admin/sign_in_with_password'):
            response = self.manager.admin_sign_in_controller.password_sign_in()
            eq_(ADMIN_AUTH_MECHANISM_NOT_CONFIGURED, response)

        self._db.delete(auth_integration)
        admin, ignore = create(self._db, Admin, email="admin@nypl.org")
        admin.password = "password"

        # Returns an error if there's no admin with the provided email.
        with self.app.test_request_context('/admin/sign_in_with_password', method='POST'):
            flask.request.form = MultiDict([
                ("email", "notanadmin@nypl.org"),
                ("password", "password"),
                ("redirect", "foo")
            ])
            response = self.manager.admin_sign_in_controller.password_sign_in()
            eq_(401, response.status_code)

        # Returns an error if the password doesn't match.
        self.admin.password = "password"
        with self.app.test_request_context('/admin/sign_in_with_password', method='POST'):
            flask.request.form = MultiDict([
                ("email", self.admin.email),
                ("password", "notthepassword"),
                ("redirect", "foo")
            ])
            response = self.manager.admin_sign_in_controller.password_sign_in()
            eq_(401, response.status_code)

        # Redirects if the admin email/password combination is valid.
        with self.app.test_request_context('/admin/sign_in_with_password', method='POST'):
            flask.request.form = MultiDict([
                ("email", self.admin.email),
                ("password", "password"),
                ("redirect", "foo")
            ])
            response = self.manager.admin_sign_in_controller.password_sign_in()
            eq_(302, response.status_code)
            eq_("foo", response.headers["Location"])

    def test_change_password(self):
        admin, ignore = create(self._db, Admin, email=self._str)
        admin.password = "old"
        with self.request_context_with_admin('/admin/change_password', admin=admin):
            flask.request.form = MultiDict([
                ("password", "new"),
            ])
            response = self.manager.admin_sign_in_controller.change_password()
            eq_(200, response.status_code)
            eq_(admin, Admin.authenticate(self._db, admin.email, "new"))
            eq_(None, Admin.authenticate(self._db, admin.email, "old"))

    def test_sign_out(self):
        admin, ignore = create(self._db, Admin, email=self._str)
        admin.password = "pass"
        with self.app.test_request_context('/admin/sign_out'):
            flask.session["admin_email"] = admin.email
            flask.session["auth_type"] = PasswordAdminAuthenticationProvider.NAME
            response = self.manager.admin_sign_in_controller.sign_out()
            eq_(302, response.status_code)

            # The admin's credentials have been removed from the session.
            eq_(None, flask.session.get("admin_email"))
            eq_(None, flask.session.get("auth_type"))


class TestPatronController(AdminControllerTest):
    def setup(self):
        super(TestPatronController, self).setup()
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
            assert_raises(AdminNotAuthorized, m, authenticator)

        # No form data specified
        with self.request_context_with_library_and_admin("/"):
            response = m(authenticator)
            eq_(404, response.status_code)
            eq_(NO_SUCH_PATRON.uri, response.uri)
            eq_("Please enter a patron identifier", response.detail)

        # AuthenticationProvider has no Authenticators.
        with self.request_context_with_library_and_admin("/"):
            flask.request.form = form
            response = m(authenticator)

            eq_(404, response.status_code)
            eq_(NO_SUCH_PATRON.uri, response.uri)
            eq_("This library has no authentication providers, so it has no patrons.",
                response.detail
            )

        # Authenticator can't find patron with this identifier
        authenticator.providers.append(auth_provider)
        with self.request_context_with_library_and_admin("/"):
            flask.request.form = form
            response = m(authenticator)

            eq_(404, response.status_code)
            eq_(NO_SUCH_PATRON.uri, response.uri)
            eq_("No patron with identifier %s was found at your library" % identifier,
            response.detail)

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
            eq_(authenticator, controller.called_with)

            # _load_patrondata() returned a PatronData object. We
            # converted it to a dictionary, which will be dumped to
            # JSON on the way out.
            eq_("An Identifier", response['authorization_identifier'])
            eq_("A Patron", response['personal_name'])

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
            eq_(200, response.status_code)

            # _load_patrondata was called and gave us information about
            # which Patron to modify.
            controller.called_with = authenticator

        # Both of the Patron's credentials are gone.
        eq_(patron.credentials, [])

        # Here, the AuthenticationProvider finds a PatronData, but the
        # controller can't turn it into a Patron because it's too vague.
        controller.mock_patrondata = PatronData()
        with self.request_context_with_library_and_admin("/"):
            flask.request.form = form
            response = controller.reset_adobe_id(authenticator)

            eq_(404, response.status_code)
            eq_(NO_SUCH_PATRON.uri, response.uri)
            assert "Could not create local patron object" in response.detail

class TestTimestampsController(AdminControllerTest):

    def setup(self):
        super(TestTimestampsController, self).setup()
        for timestamp in self._db.query(Timestamp):
            self._db.delete(timestamp)

        self.collection = self._default_collection
        self.start = datetime.now()
        self.finish = datetime.now()

        cp, ignore = create(
            self._db, Timestamp,
            service_type="coverage_provider",
            service="test_cp",
            start=self.start,
            finish=self.finish,
            collection=self.collection
        )

        monitor, ignore = create(
            self._db, Timestamp,
            service_type="monitor",
            service="test_monitor",
            start=self.start,
            finish=self.finish,
            collection=self.collection,
            exception="stack trace string"
        )

        script, ignore = create(
            self._db, Timestamp,
            achievements="ran a script",
            service_type="script",
            service="test_script",
            start=self.start,
            finish=self.finish,
        )

        other, ignore = create(
            self._db, Timestamp,
            service="test_other",
            start=self.start,
            finish=self.finish,
        )

    def test_diagnostics_admin_not_authorized(self):
        with self.request_context_with_admin("/"):
            assert_raises(AdminNotAuthorized, self.manager.timestamps_controller.diagnostics)

    def test_diagnostics(self):
        duration = (self.finish - self.start).total_seconds()

        with self.request_context_with_admin("/"):
            self.admin.add_role(AdminRole.SYSTEM_ADMIN)
            response = self.manager.timestamps_controller.diagnostics()

        eq_(set(response.keys()), set(["coverage_provider", "monitor", "script", "other"]))

        cp_service = response["coverage_provider"]
        cp_name, cp_collection = cp_service.items()[0]
        eq_(cp_name, "test_cp")
        cp_collection_name, [cp_timestamp] = cp_collection.items()[0]
        eq_(cp_collection_name, self.collection.name)
        eq_(cp_timestamp.get("exception"), None)
        eq_(cp_timestamp.get("start"), self.start)
        eq_(cp_timestamp.get("duration"), duration)
        eq_(cp_timestamp.get("achievements"), None)

        monitor_service = response["monitor"]
        monitor_name, monitor_collection = monitor_service.items()[0]
        eq_(monitor_name, "test_monitor")
        monitor_collection_name, [monitor_timestamp] = monitor_collection.items()[0]
        eq_(monitor_collection_name, self.collection.name)
        eq_(monitor_timestamp.get("exception"), "stack trace string")
        eq_(monitor_timestamp.get("start"), self.start)
        eq_(monitor_timestamp.get("duration"), duration)
        eq_(monitor_timestamp.get("achievements"), None)

        script_service = response["script"]
        script_name, script_collection = script_service.items()[0]
        eq_(script_name, "test_script")
        script_collection_name, [script_timestamp] = script_collection.items()[0]
        eq_(script_collection_name, "No associated collection")
        eq_(script_timestamp.get("exception"), None)
        eq_(script_timestamp.get("duration"), duration)
        eq_(script_timestamp.get("start"), self.start)
        eq_(script_timestamp.get("achievements"), "ran a script")

        other_service = response["other"]
        other_name, other_collection = other_service.items()[0]
        eq_(other_name, "test_other")
        other_collection_name, [other_timestamp] = other_collection.items()[0]
        eq_(other_collection_name, "No associated collection")
        eq_(other_timestamp.get("exception"), None)
        eq_(other_timestamp.get("duration"), duration)
        eq_(other_timestamp.get("start"), self.start)
        eq_(other_timestamp.get("achievements"), None)

class TestFeedController(AdminControllerTest):

    def setup(self):
        super(TestFeedController, self).setup()
        self.admin.add_role(AdminRole.LIBRARIAN, self._default_library)

    def test_complaints(self):
        type = iter(Complaint.VALID_TYPES)
        type1 = next(type)
        type2 = next(type)

        work1 = self._work(
            "fiction work with complaint 1",
            language="eng",
            fiction=True,
            with_open_access_download=True)
        complaint1 = self._complaint(
            work1.license_pools[0],
            type1,
            "complaint source 1",
            "complaint detail 1")
        complaint2 = self._complaint(
            work1.license_pools[0],
            type2,
            "complaint source 2",
            "complaint detail 2")
        work2 = self._work(
            "nonfiction work with complaint",
            language="eng",
            fiction=False,
            with_open_access_download=True)
        complaint3 = self._complaint(
            work2.license_pools[0],
            type1,
            "complaint source 3",
            "complaint detail 3")

        SessionManager.refresh_materialized_views(self._db)
        with self.request_context_with_library_and_admin("/"):
            response = self.manager.admin_feed_controller.complaints()
            feed = feedparser.parse(response.data)
            entries = feed['entries']

            eq_(len(entries), 2)

        self.admin.remove_role(AdminRole.LIBRARIAN, self._default_library)
        with self.request_context_with_library_and_admin("/"):
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_feed_controller.complaints)

    def test_suppressed(self):
        suppressed_work = self._work(with_open_access_download=True)
        suppressed_work.license_pools[0].suppressed = True

        unsuppressed_work = self._work()

        SessionManager.refresh_materialized_views(self._db)
        with self.request_context_with_library_and_admin("/"):
            response = self.manager.admin_feed_controller.suppressed()
            feed = feedparser.parse(response.data)
            entries = feed['entries']
            eq_(1, len(entries))
            eq_(suppressed_work.title, entries[0]['title'])

        self.admin.remove_role(AdminRole.LIBRARIAN, self._default_library)
        with self.request_context_with_library_and_admin("/"):
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_feed_controller.suppressed)

    def test_genres(self):
        with self.app.test_request_context("/"):
            response = self.manager.admin_feed_controller.genres()

            for name in genres:
                top = "Fiction" if genres[name].is_fiction else "Nonfiction"
                eq_(response[top][name], dict({
                    "name": name,
                    "parents": [parent.name for parent in genres[name].parents],
                    "subgenres": [subgenre.name for subgenre in genres[name].subgenres]
                }))

class TestCustomListsController(AdminControllerTest):
    def setup(self):
        super(TestCustomListsController, self).setup()
        self.admin.add_role(AdminRole.LIBRARIAN, self._default_library)

    def test_custom_lists_get(self):
        # This list has no associated Library and should not be included.
        no_library, ignore = create(self._db, CustomList, name=self._str)

        one_entry, ignore = create(self._db, CustomList, name=self._str, library=self._default_library)
        edition = self._edition()
        one_entry.add_entry(edition)
        collection = self._collection()
        collection.customlists = [one_entry]

        no_entries, ignore = create(self._db, CustomList, name=self._str, library=self._default_library)

        with self.request_context_with_library_and_admin("/"):
            response = self.manager.admin_custom_lists_controller.custom_lists()
            eq_(2, len(response.get("custom_lists")))
            lists = response.get("custom_lists")
            [l1, l2] = sorted(lists, key=lambda l: l.get("id"))

            eq_(one_entry.id, l1.get("id"))
            eq_(one_entry.name, l1.get("name"))
            eq_(1, l1.get("entry_count"))
            eq_(1, len(l1.get("collections")))
            [c] = l1.get("collections")
            eq_(collection.name, c.get("name"))
            eq_(collection.id, c.get("id"))
            eq_(collection.protocol, c.get("protocol"))

            eq_(no_entries.id, l2.get("id"))
            eq_(no_entries.name, l2.get("name"))
            eq_(0, l2.get("entry_count"))
            eq_(0, len(l2.get("collections")))

        self.admin.remove_role(AdminRole.LIBRARIAN, self._default_library)
        with self.request_context_with_library_and_admin("/"):
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_custom_lists_controller.custom_lists)

    def test_custom_lists_post_errors(self):
        with self.request_context_with_library_and_admin("/", method='POST'):
            flask.request.form = MultiDict([
                ("id", "4"),
                ("name", "name"),
            ])
            response = self.manager.admin_custom_lists_controller.custom_lists()
            eq_(MISSING_CUSTOM_LIST, response)

        library = self._library()
        data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        list, ignore = create(self._db, CustomList, name=self._str, data_source=data_source)
        list.library = library
        with self.request_context_with_library_and_admin("/", method='POST'):
            flask.request.form = MultiDict([
                ("id", list.id),
                ("name", list.name),
            ])
            response = self.manager.admin_custom_lists_controller.custom_lists()
            eq_(CANNOT_CHANGE_LIBRARY_FOR_CUSTOM_LIST, response)

        list, ignore = create(self._db, CustomList, name=self._str, data_source=data_source, library=self._default_library)
        with self.request_context_with_library_and_admin("/", method='POST'):
            flask.request.form = MultiDict([
                ("name", list.name),
            ])
            response = self.manager.admin_custom_lists_controller.custom_lists()
            eq_(CUSTOM_LIST_NAME_ALREADY_IN_USE, response)

        l1, ignore = create(self._db, CustomList, name=self._str, data_source=data_source, library=self._default_library)
        l2, ignore = create(self._db, CustomList, name=self._str, data_source=data_source, library=self._default_library)
        with self.request_context_with_library_and_admin("/", method='POST'):
            flask.request.form = MultiDict([
                ("id", l2.id),
                ("name", l1.name),
            ])
            response = self.manager.admin_custom_lists_controller.custom_lists()
            eq_(CUSTOM_LIST_NAME_ALREADY_IN_USE, response)

        with self.request_context_with_library_and_admin("/", method='POST'):
            flask.request.form = MultiDict([
                ("name", "name"),
                ("collections", json.dumps([12345])),
            ])
            response = self.manager.admin_custom_lists_controller.custom_lists()
            eq_(MISSING_COLLECTION, response)

        admin, ignore = create(self._db, Admin, email="test@nypl.org")
        library = self._library()
        with self.request_context_with_admin("/", method="POST", admin=admin):
            flask.request.library = library
            flask.request.form = MultiDict([
                ("name", "name"),
                ("collections", json.dumps([])),
            ])
            assert_raises(AdminNotAuthorized,
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
            eq_(COLLECTION_NOT_ASSOCIATED_WITH_LIBRARY, response)

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
            eq_(201, response.status_code)

            [list] = self._db.query(CustomList).all()
            eq_(list.id, int(response.response[0]))
            eq_(self._default_library, list.library)
            eq_("List", list.name)
            eq_(1, len(list.entries))
            eq_(work, list.entries[0].work)
            eq_(work.presentation_edition, list.entries[0].edition)
            eq_(True, list.entries[0].featured)
            eq_([collection], list.collections)

    def test_custom_list_get(self):
        data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        list, ignore = create(self._db, CustomList, name=self._str, library=self._default_library, data_source=data_source)

        work1 = self._work(with_license_pool=True)
        work2 = self._work(with_license_pool=True)
        list.add_entry(work1)
        list.add_entry(work2)

        with self.request_context_with_library_and_admin("/"):
            response = self.manager.admin_custom_lists_controller.custom_list(list.id)
            feed = feedparser.parse(response.get_data())

            eq_(list.name, feed.feed.title)
            eq_(2, len(feed.entries))

            [self_custom_list_link] = [x['href'] for x in feed.feed['links']
                              if x['rel'] == "self"]
            eq_(self_custom_list_link, feed.feed.id)

            [entry1, entry2] = feed.entries
            eq_(work1.title, entry1.get("title"))
            eq_(work2.title, entry2.get("title"))

            eq_(work1.presentation_edition.author, entry1.get("author"))
            eq_(work2.presentation_edition.author, entry2.get("author"))

    def test_custom_list_get_errors(self):
        with self.request_context_with_library_and_admin("/"):
            response = self.manager.admin_custom_lists_controller.custom_list(123)
            eq_(MISSING_CUSTOM_LIST, response)

        data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        list, ignore = create(self._db, CustomList, name=self._str, library=self._default_library, data_source=data_source)

        self.admin.remove_role(AdminRole.LIBRARIAN, self._default_library)
        with self.request_context_with_library_and_admin("/"):
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_custom_lists_controller.custom_list,
                          list.id)

    def test_custom_list_edit(self):
        data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        list, ignore = create(self._db, CustomList, name=self._str, data_source=data_source)
        list.library = self._default_library

        # Create a Lane that depends on this CustomList for its membership.
        lane = self._lane()
        lane.customlists.append(list)
        eq_(0, lane.size)

        w1 = self._work(with_license_pool=True, language="eng")
        w2 = self._work(with_license_pool=True, language="fre")
        w3 = self._work(with_license_pool=True)
        w2.presentation_edition.medium = Edition.AUDIO_MEDIUM
        w3.presentation_edition.permanent_work_id = w2.presentation_edition.permanent_work_id
        w3.presentation_edition.medium = Edition.BOOK_MEDIUM

        list.add_entry(w1)
        list.add_entry(w2)
        self.add_to_materialized_view([w1, w2, w3])

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
        eq_(200, response.status_code)
        eq_(list.id, int(response.response[0]))

        eq_("new name", list.name)
        eq_(set([w2, w3]),
            set([entry.work for entry in list.entries]))
        eq_(new_collections, list.collections)

        # The lane's estimated size will be updated once the materialized
        # views are refreshed.
        SessionManager.refresh_materialized_views(self._db)
        eq_(2, lane.size)

        self.admin.remove_role(AdminRole.LIBRARIAN, self._default_library)
        with self.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", str(list.id)),
                ("name", "another new name"),
                ("entries", json.dumps(new_entries)),
                ("collections", json.dumps([c.id for c in new_collections])),
            ])
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_custom_lists_controller.custom_list,
                          list.id)

    def test_custom_list_delete_success(self):
        self.admin.add_role(AdminRole.LIBRARY_MANAGER, self._default_library)
        data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        list, ignore = create(self._db, CustomList, name=self._str, data_source=data_source)
        list.library = self._default_library

        w1 = self._work(with_license_pool=True)
        w2 = self._work(with_license_pool=True)
        list.add_entry(w1)
        list.add_entry(w2)

        # This lane depends heavily on lists from this data source.
        lane = self._lane()
        lane.list_datasource = list.data_source
        lane.size = 100

        with self.request_context_with_library_and_admin("/", method="DELETE"):
            response = self.manager.admin_custom_lists_controller.custom_list(list.id)
            eq_(200, response.status_code)

            eq_(0, self._db.query(CustomList).count())
            eq_(0, self._db.query(CustomListEntry).count())

        # The lane's estimate has been updated to reflect the removal
        # of a list from its data source.
        eq_(0, lane.size)

    def test_custom_list_delete_errors(self):
        data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        list, ignore = create(self._db, CustomList, name=self._str, data_source=data_source)
        with self.request_context_with_library_and_admin("/", method="DELETE"):
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_custom_lists_controller.custom_list,
                          list.id)

        self.admin.add_role(AdminRole.LIBRARY_MANAGER, self._default_library)
        with self.request_context_with_library_and_admin("/", method="DELETE"):
            response = self.manager.admin_custom_lists_controller.custom_list(123)
            eq_(MISSING_CUSTOM_LIST, response)


class TestLanesController(AdminControllerTest):
    def setup(self):
        super(TestLanesController, self).setup()
        self.admin.add_role(AdminRole.LIBRARY_MANAGER, self._default_library)

    def test_lanes_get(self):
        library = self._library()
        collection = self._collection()
        library.collections += [collection]

        english = self._lane("English", library=library, languages=["eng"])
        english.priority = 0
        english_fiction = self._lane("Fiction", library=library, parent=english, fiction=True)
        english_fiction.visible = False
        english_sf = self._lane("Science Fiction", library=library, parent=english_fiction)
        english_sf.add_genre("Science Fiction")
        english_sf.inherit_parent_restrictions = True
        spanish = self._lane("Spanish", library=library, languages=["spa"])
        spanish.priority = 1

        w1 = self._work(with_license_pool=True, language="eng", genre="Science Fiction", collection=collection)
        w2 = self._work(with_license_pool=True, language="eng", fiction=False, collection=collection)

        list, ignore = self._customlist(data_source_name=DataSource.LIBRARY_STAFF, num_entries=0)
        list.library = library
        lane_for_list = self._lane("List Lane", library=library)
        lane_for_list.customlists += [list]
        lane_for_list.priority = 2

        SessionManager.refresh_materialized_views(self._db)

        with self.request_context_with_library_and_admin("/"):
            flask.request.library = library
            # The admin is not a librarian for this library.
            assert_raises(AdminNotAuthorized, self.manager.admin_lanes_controller.lanes)
            self.admin.add_role(AdminRole.LIBRARIAN, library)
            response = self.manager.admin_lanes_controller.lanes()

            eq_(3, len(response.get("lanes")))
            [english_info, spanish_info, list_info] = response.get("lanes")

            eq_(english.id, english_info.get("id"))
            eq_(english.display_name, english_info.get("display_name"))
            eq_(english.visible, english_info.get("visible"))
            eq_(2, english_info.get("count"))
            eq_([], english_info.get("custom_list_ids"))
            eq_(True, english_info.get("inherit_parent_restrictions"))

            [fiction_info] = english_info.get("sublanes")
            eq_(english_fiction.id, fiction_info.get("id"))
            eq_(english_fiction.display_name, fiction_info.get("display_name"))
            eq_(english_fiction.visible, fiction_info.get("visible"))
            eq_(1, fiction_info.get("count"))
            eq_([], fiction_info.get("custom_list_ids"))
            eq_(True, fiction_info.get("inherit_parent_restrictions"))

            [sf_info] = fiction_info.get("sublanes")
            eq_(english_sf.id, sf_info.get("id"))
            eq_(english_sf.display_name, sf_info.get("display_name"))
            eq_(english_sf.visible, sf_info.get("visible"))
            eq_(1, sf_info.get("count"))
            eq_([], sf_info.get("custom_list_ids"))
            eq_(True, sf_info.get("inherit_parent_restrictions"))

            eq_(spanish.id, spanish_info.get("id"))
            eq_(spanish.display_name, spanish_info.get("display_name"))
            eq_(spanish.visible, spanish_info.get("visible"))
            eq_(0, spanish_info.get("count"))
            eq_([], spanish_info.get("custom_list_ids"))
            eq_(True, spanish_info.get("inherit_parent_restrictions"))

            eq_(lane_for_list.id, list_info.get("id"))
            eq_(lane_for_list.display_name, list_info.get("display_name"))
            eq_(lane_for_list.visible, list_info.get("visible"))
            eq_(0, list_info.get("count"))
            eq_([list.id], list_info.get("custom_list_ids"))
            eq_(True, list_info.get("inherit_parent_restrictions"))

    def test_lanes_post_errors(self):
        with self.request_context_with_library_and_admin("/", method='POST'):
            flask.request.form = MultiDict([
            ])
            response = self.manager.admin_lanes_controller.lanes()
            eq_(NO_DISPLAY_NAME_FOR_LANE, response)

        with self.request_context_with_library_and_admin("/", method='POST'):
            flask.request.form = MultiDict([
                ("display_name", "lane"),
            ])
            response = self.manager.admin_lanes_controller.lanes()
            eq_(NO_CUSTOM_LISTS_FOR_LANE, response)

        list, ignore = self._customlist(data_source_name=DataSource.LIBRARY_STAFF, num_entries=0)
        list.library = self._default_library

        with self.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", "12345"),
                ("display_name", "lane"),
                ("custom_list_ids", json.dumps([list.id])),
            ])
            response = self.manager.admin_lanes_controller.lanes()
            eq_(MISSING_LANE, response)

        library = self._library()
        with self.request_context_with_library_and_admin("/", method='POST'):
            flask.request.library = library
            flask.request.form = MultiDict([
                ("display_name", "lane"),
                ("custom_list_ids", json.dumps([list.id])),
            ])
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_lanes_controller.lanes)

        lane1 = self._lane("lane1")
        lane2 = self._lane("lane2")

        with self.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", lane1.id),
                ("display_name", "lane1"),
                ("custom_list_ids", json.dumps([list.id])),
            ])
            response = self.manager.admin_lanes_controller.lanes()
            eq_(CANNOT_EDIT_DEFAULT_LANE, response)

        lane1.customlists += [list]

        with self.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", lane1.id),
                ("display_name", "lane2"),
                ("custom_list_ids", json.dumps([list.id])),
            ])
            response = self.manager.admin_lanes_controller.lanes()
            eq_(LANE_WITH_PARENT_AND_DISPLAY_NAME_ALREADY_EXISTS, response)

        with self.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("display_name", "lane2"),
                ("custom_list_ids", json.dumps([list.id])),
            ])
            response = self.manager.admin_lanes_controller.lanes()
            eq_(LANE_WITH_PARENT_AND_DISPLAY_NAME_ALREADY_EXISTS, response)

        with self.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("parent_id", "12345"),
                ("display_name", "lane"),
                ("custom_list_ids", json.dumps([list.id])),
            ])
            response = self.manager.admin_lanes_controller.lanes()
            eq_(MISSING_LANE.uri, response.uri)

        with self.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("parent_id", lane1.id),
                ("display_name", "lane"),
                ("custom_list_ids", json.dumps(["12345"])),
            ])
            response = self.manager.admin_lanes_controller.lanes()
            eq_(MISSING_CUSTOM_LIST.uri, response.uri)

    def test_lanes_create(self):
        list, ignore = self._customlist(data_source_name=DataSource.LIBRARY_STAFF, num_entries=0)
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
            eq_(201, response.status_code)

            [lane] = self._db.query(Lane).filter(Lane.display_name=="lane")
            eq_(lane.id, int(response.response[0]))
            eq_(self._default_library, lane.library)
            eq_("lane", lane.display_name)
            eq_(parent, lane.parent)
            eq_([Edition.BOOK_MEDIUM], lane.media)
            eq_(1, len(lane.customlists))
            eq_(list, lane.customlists[0])
            eq_(False, lane.inherit_parent_restrictions)
            eq_(0, lane.priority)

            # The sibling's priority has been shifted down to put the new lane at the top.
            eq_(1, sibling.priority)

    def test_lanes_edit(self):

        work = self._work(with_license_pool=True)

        list1, ignore = self._customlist(data_source_name=DataSource.LIBRARY_STAFF, num_entries=0)
        list1.library = self._default_library
        list2, ignore = self._customlist(data_source_name=DataSource.LIBRARY_STAFF, num_entries=0)
        list2.library = self._default_library
        list2.add_entry(work)
        self.add_to_materialized_view([work])

        lane = self._lane("old name")
        lane.customlists += [list1]

        with self.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", str(lane.id)),
                ("display_name", "new name"),
                ("custom_list_ids", json.dumps([list2.id])),
                ("inherit_parent_restrictions", "true"),
            ])

            response = self.manager.admin_lanes_controller.lanes()
            eq_(200, response.status_code)
            eq_(lane.id, int(response.response[0]))

            eq_("new name", lane.display_name)
            eq_([list2], lane.customlists)
            eq_(True, lane.inherit_parent_restrictions)
            eq_(1, lane.size)

    def test_lane_delete_success(self):
        library = self._library()
        self.admin.add_role(AdminRole.LIBRARY_MANAGER, library)
        lane = self._lane("lane", library=library)
        list, ignore = self._customlist(data_source_name=DataSource.LIBRARY_STAFF, num_entries=0)
        list.library = library
        lane.customlists += [list]
        eq_(1, self._db.query(Lane).filter(Lane.library==library).count())

        with self.request_context_with_library_and_admin("/", method="DELETE"):
            flask.request.library = library
            response = self.manager.admin_lanes_controller.lane(lane.id)
            eq_(200, response.status_code)

            # The lane has been deleted.
            eq_(0, self._db.query(Lane).filter(Lane.library==library).count())

            # The custom list still exists though.
            eq_(1, self._db.query(CustomList).filter(CustomList.library==library).count())

        lane = self._lane("lane", library=library)
        lane.customlists += [list]
        child = self._lane("child", parent=lane, library=library)
        child.customlists += [list]
        grandchild = self._lane("grandchild", parent=child, library=library)
        grandchild.customlists += [list]
        eq_(3, self._db.query(Lane).filter(Lane.library==library).count())

        with self.request_context_with_library_and_admin("/", method="DELETE"):
            flask.request.library = library
            response = self.manager.admin_lanes_controller.lane(lane.id)
            eq_(200, response.status_code)

            # The lanes have all been deleted.
            eq_(0, self._db.query(Lane).filter(Lane.library==library).count())

            # The custom list still exists though.
            eq_(1, self._db.query(CustomList).filter(CustomList.library==library).count())

    def test_lane_delete_errors(self):
        with self.request_context_with_library_and_admin("/", method="DELETE"):
            response = self.manager.admin_lanes_controller.lane(123)
            eq_(MISSING_LANE, response)

        lane = self._lane("lane")
        library = self._library()
        with self.request_context_with_library_and_admin("/", method="DELETE"):
            flask.request.library = library
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_lanes_controller.lane,
                          lane.id)

        with self.request_context_with_library_and_admin("/", method="DELETE"):
            response = self.manager.admin_lanes_controller.lane(lane.id)
            eq_(CANNOT_EDIT_DEFAULT_LANE, response)

    def test_show_lane_success(self):
        lane = self._lane("lane")
        lane.visible = False
        with self.request_context_with_library_and_admin("/"):
            response = self.manager.admin_lanes_controller.show_lane(lane.id)
            eq_(200, response.status_code)
            eq_(True, lane.visible)

    def test_show_lane_errors(self):
        with self.request_context_with_library_and_admin("/"):
            response = self.manager.admin_lanes_controller.show_lane(123)
            eq_(MISSING_LANE, response)

        parent = self._lane("parent")
        parent.visible = False
        child = self._lane("lane")
        child.visible = False
        child.parent = parent
        with self.request_context_with_library_and_admin("/"):
            response = self.manager.admin_lanes_controller.show_lane(child.id)
            eq_(CANNOT_SHOW_LANE_WITH_HIDDEN_PARENT, response)

        self.admin.remove_role(AdminRole.LIBRARY_MANAGER, self._default_library)
        with self.request_context_with_library_and_admin("/"):
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_lanes_controller.show_lane,
                          parent.id)

    def test_hide_lane_success(self):
        lane = self._lane("lane")
        lane.visible = True
        with self.request_context_with_library_and_admin("/"):
            response = self.manager.admin_lanes_controller.hide_lane(lane.id)
            eq_(200, response.status_code)
            eq_(False, lane.visible)

    def test_hide_lane_errors(self):
        with self.request_context_with_library_and_admin("/"):
            response = self.manager.admin_lanes_controller.hide_lane(123456789)
            eq_(MISSING_LANE, response)

        lane = self._lane()
        self.admin.remove_role(AdminRole.LIBRARY_MANAGER, self._default_library)
        with self.request_context_with_library_and_admin("/"):
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_lanes_controller.show_lane,
                          lane.id)

    def test_reset(self):
        library = self._library()
        old_lane = self._lane("old lane", library=library)

        with self.request_context_with_library_and_admin("/"):
            flask.request.library = library
            assert_raises(AdminNotAuthorized, self.manager.admin_lanes_controller.reset)

            self.admin.add_role(AdminRole.LIBRARY_MANAGER, library)
            response = self.manager.admin_lanes_controller.reset()
            eq_(200, response.status_code)

            # The old lane is gone.
            eq_(0, self._db.query(Lane).filter(Lane.library==library).filter(Lane.id==old_lane.id).count())
            # tests/test_lanes.py tests the default lane creation, but make sure some
            # lanes were created.
            assert 0 < self._db.query(Lane).filter(Lane.library==library).count()

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

        new_order = [{ "id": parent2.id, "sublanes": [{ "id": child2.id }, { "id": child1.id }] },
                     { "id": parent1.id }]

        with self.request_context_with_library_and_admin("/"):
            flask.request.library = library
            flask.request.data = json.dumps(new_order)

            assert_raises(AdminNotAuthorized, self.manager.admin_lanes_controller.change_order)

            self.admin.add_role(AdminRole.LIBRARY_MANAGER, library)
            response = self.manager.admin_lanes_controller.change_order()
            eq_(200, response.status_code)

            eq_(0, parent2.priority)
            eq_(1, parent1.priority)
            eq_(0, child2.priority)
            eq_(1, child1.priority)

class TestDashboardController(AdminControllerTest):

    # Unlike most of these controllers, we do want to have a book
    # automatically created as part of setup.
    BOOKS = CirculationControllerTest.BOOKS

    def test_circulation_events(self):
        [lp] = self.english_1.license_pools
        patron_id = "patronid"
        types = [
            CirculationEvent.DISTRIBUTOR_CHECKIN,
            CirculationEvent.DISTRIBUTOR_CHECKOUT,
            CirculationEvent.DISTRIBUTOR_HOLD_PLACE,
            CirculationEvent.DISTRIBUTOR_HOLD_RELEASE,
            CirculationEvent.DISTRIBUTOR_TITLE_ADD
        ]
        time = datetime.now() - timedelta(minutes=len(types))
        for type in types:
            get_one_or_create(
                self._db, CirculationEvent,
                license_pool=lp, type=type, start=time, end=time,
                foreign_patron_id=patron_id)
            time += timedelta(minutes=1)

        with self.request_context_with_library_and_admin("/"):
            response = self.manager.admin_dashboard_controller.circulation_events()
            url = AdminAnnotator(self.manager.d_circulation, self._default_library).permalink_for(self.english_1, lp, lp.identifier)

        events = response['circulation_events']
        eq_(types[::-1], [event['type'] for event in events])
        eq_([self.english_1.title]*len(types), [event['book']['title'] for event in events])
        eq_([url]*len(types), [event['book']['url'] for event in events])
        eq_([patron_id]*len(types), [event['patron_id'] for event in events])

        # request fewer events
        with self.request_context_with_library_and_admin("/?num=2"):
            response = self.manager.admin_dashboard_controller.circulation_events()
            url = AdminAnnotator(self.manager.d_circulation, self._default_library).permalink_for(self.english_1, lp, lp.identifier)

        eq_(2, len(response['circulation_events']))

    def test_bulk_circulation_events(self):
        [lp] = self.english_1.license_pools
        edition = self.english_1.presentation_edition
        identifier = self.english_1.presentation_edition.primary_identifier
        genres = self._db.query(Genre).all()
        get_one_or_create(self._db, WorkGenre, work=self.english_1, genre=genres[0], affinity=0.2)
        get_one_or_create(self._db, WorkGenre, work=self.english_1, genre=genres[1], affinity=0.3)
        get_one_or_create(self._db, WorkGenre, work=self.english_1, genre=genres[2], affinity=0.5)
        ordered_genre_string = ",".join([genres[2].name, genres[1].name, genres[0].name])
        types = [
            CirculationEvent.DISTRIBUTOR_CHECKIN,
            CirculationEvent.DISTRIBUTOR_CHECKOUT,
            CirculationEvent.DISTRIBUTOR_HOLD_PLACE,
            CirculationEvent.DISTRIBUTOR_HOLD_RELEASE,
            CirculationEvent.DISTRIBUTOR_TITLE_ADD
        ]
        num = len(types)
        time = datetime.now() - timedelta(minutes=len(types))
        for type in types:
            get_one_or_create(
                self._db, CirculationEvent,
                license_pool=lp, type=type, start=time, end=time)
            time += timedelta(minutes=1)

        with self.app.test_request_context("/"):
            response, requested_date = self.manager.admin_dashboard_controller.bulk_circulation_events()
        rows = response[1::] # skip header row
        eq_(num, len(rows))
        eq_(types, [row[1] for row in rows])
        eq_([identifier.identifier]*num, [row[2] for row in rows])
        eq_([identifier.type]*num, [row[3] for row in rows])
        eq_([edition.title]*num, [row[4] for row in rows])
        eq_([edition.author]*num, [row[5] for row in rows])
        eq_(["fiction"]*num, [row[6] for row in rows])
        eq_([self.english_1.audience]*num, [row[7] for row in rows])
        eq_([edition.publisher]*num, [row[8] for row in rows])
        eq_([edition.language]*num, [row[9] for row in rows])
        eq_([self.english_1.target_age_string]*num, [row[10] for row in rows])
        eq_([ordered_genre_string]*num, [row[11] for row in rows])

        # use date
        today = date.strftime(date.today() - timedelta(days=1), "%Y-%m-%d")
        with self.app.test_request_context("/?date=%s" % today):
            response, requested_date = self.manager.admin_dashboard_controller.bulk_circulation_events()
        rows = response[1::] # skip header row
        eq_(0, len(rows))

    def test_stats_patrons(self):
        with self.request_context_with_admin("/"):
            self.admin.add_role(AdminRole.SYSTEM_ADMIN)

            # At first, there's one patron in the database.
            response = self.manager.admin_dashboard_controller.stats()
            library_data = response.get(self._default_library.short_name)
            total_data = response.get("total")
            for data in [library_data, total_data]:
                patron_data = data.get('patrons')
                eq_(1, patron_data.get('total'))
                eq_(0, patron_data.get('with_active_loans'))
                eq_(0, patron_data.get('with_active_loans_or_holds'))
                eq_(0, patron_data.get('loans'))
                eq_(0, patron_data.get('holds'))

            edition, pool = self._edition(with_license_pool=True, with_open_access_download=False)
            edition2, open_access_pool = self._edition(with_open_access_download=True)

            # patron1 has a loan.
            patron1 = self._patron()
            pool.loan_to(patron1, end=datetime.now() + timedelta(days=5))

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
                eq_(4, patron_data.get('total'))
                eq_(1, patron_data.get('with_active_loans'))
                eq_(2, patron_data.get('with_active_loans_or_holds'))
                eq_(1, patron_data.get('loans'))
                eq_(1, patron_data.get('holds'))

            # These patrons are in a different library..
            l2 = self._library()
            patron4 = self._patron(library=l2)
            pool.loan_to(patron4, end=datetime.now() + timedelta(days=5))
            patron5 = self._patron(library=l2)
            pool.on_hold_to(patron5)

            response = self.manager.admin_dashboard_controller.stats()
            library_data = response.get(self._default_library.short_name)
            total_data = response.get("total")
            eq_(4, library_data.get('patrons').get('total'))
            eq_(1, library_data.get('patrons').get('with_active_loans'))
            eq_(2, library_data.get('patrons').get('with_active_loans_or_holds'))
            eq_(1, library_data.get('patrons').get('loans'))
            eq_(1, library_data.get('patrons').get('holds'))
            eq_(6, total_data.get('patrons').get('total'))
            eq_(2, total_data.get('patrons').get('with_active_loans'))
            eq_(4, total_data.get('patrons').get('with_active_loans_or_holds'))
            eq_(2, total_data.get('patrons').get('loans'))
            eq_(2, total_data.get('patrons').get('holds'))

            # If the admin only has access to some libraries, only those will be counted
            # in the total stats.
            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            self.admin.add_role(AdminRole.LIBRARIAN, self._default_library)

            response = self.manager.admin_dashboard_controller.stats()
            library_data = response.get(self._default_library.short_name)
            total_data = response.get("total")
            eq_(4, library_data.get('patrons').get('total'))
            eq_(1, library_data.get('patrons').get('with_active_loans'))
            eq_(2, library_data.get('patrons').get('with_active_loans_or_holds'))
            eq_(1, library_data.get('patrons').get('loans'))
            eq_(1, library_data.get('patrons').get('holds'))
            eq_(4, total_data.get('patrons').get('total'))
            eq_(1, total_data.get('patrons').get('with_active_loans'))
            eq_(2, total_data.get('patrons').get('with_active_loans_or_holds'))
            eq_(1, total_data.get('patrons').get('loans'))
            eq_(1, total_data.get('patrons').get('holds'))

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
                eq_(1, inventory_data.get('titles'))
                eq_(0, inventory_data.get('licenses'))
                eq_(0, inventory_data.get('available_licenses'))

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
                eq_(3, inventory_data.get('titles'))
                eq_(15, inventory_data.get('licenses'))
                eq_(4, inventory_data.get('available_licenses'))

            # This edition is in a different collection.
            c2 = self._collection()
            edition4, pool4 = self._edition(with_license_pool=True, with_open_access_download=False, collection=c2)
            pool4.licenses_owned = 2
            pool4.licenses_available = 2

            response = self.manager.admin_dashboard_controller.stats()
            library_data = response.get(self._default_library.short_name)
            total_data = response.get("total")
            eq_(3, library_data.get('inventory').get('titles'))
            eq_(4, total_data.get('inventory').get('titles'))
            eq_(15, library_data.get('inventory').get('licenses'))
            eq_(17, total_data.get('inventory').get('licenses'))
            eq_(4, library_data.get('inventory').get('available_licenses'))
            eq_(6, total_data.get('inventory').get('available_licenses'))

            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            self.admin.add_role(AdminRole.LIBRARIAN, self._default_library)

            # The admin can no longer see the other collection, so it's not
            # counted in the totals.
            response = self.manager.admin_dashboard_controller.stats()
            library_data = response.get(self._default_library.short_name)
            total_data = response.get("total")
            for data in [library_data, total_data]:
                inventory_data = data.get('inventory')
                eq_(3, inventory_data.get('titles'))
                eq_(15, inventory_data.get('licenses'))
                eq_(4, inventory_data.get('available_licenses'))

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
                eq_(1, len(collections_data))
                collection_data = collections_data.get(self._default_collection.name)
                eq_(0, collection_data.get('licensed_titles'))
                eq_(1, collection_data.get('open_access_titles'))
                eq_(0, collection_data.get('licenses'))
                eq_(0, collection_data.get('available_licenses'))

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
            eq_(2, len(library_collections_data))
            eq_(3, len(total_collections_data))
            for data in [library_collections_data, total_collections_data]:
                c1_data = data.get(self._default_collection.name)
                eq_(1, c1_data.get('licensed_titles'))
                eq_(1, c1_data.get('open_access_titles'))
                eq_(3, c1_data.get('licenses'))
                eq_(0, c1_data.get('available_licenses'))

                c3_data = data.get(c3.name)
                eq_(0, c3_data.get('licensed_titles'))
                eq_(0, c3_data.get('open_access_titles'))
                eq_(0, c3_data.get('licenses'))
                eq_(0, c3_data.get('available_licenses'))

            eq_(None, library_collections_data.get(c2.name))
            c2_data = total_collections_data.get(c2.name)
            eq_(2, c2_data.get('licensed_titles'))
            eq_(0, c2_data.get('open_access_titles'))
            eq_(15, c2_data.get('licenses'))
            eq_(10, c2_data.get('available_licenses'))

            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            self.admin.add_role(AdminRole.LIBRARY_MANAGER, self._default_library)

            # c2 is no longer included in the totals since the admin's library does
            # not use it.
            response = self.manager.admin_dashboard_controller.stats()
            library_data = response.get(self._default_library.short_name)
            total_data = response.get("total")
            for data in [library_data, total_data]:
                collections_data = data.get("collections")
                eq_(2, len(collections_data))
                eq_(None, collections_data.get(c2.name))

                c1_data = collections_data.get(self._default_collection.name)
                eq_(1, c1_data.get('licensed_titles'))
                eq_(1, c1_data.get('open_access_titles'))
                eq_(3, c1_data.get('licenses'))
                eq_(0, c1_data.get('available_licenses'))

                c3_data = collections_data.get(c3.name)
                eq_(0, c3_data.get('licensed_titles'))
                eq_(0, c3_data.get('open_access_titles'))
                eq_(0, c3_data.get('licenses'))
                eq_(0, c3_data.get('available_licenses'))


class SettingsControllerTest(AdminControllerTest):
    """Test some part of the settings controller."""

    def setup(self):
        super(SettingsControllerTest, self).setup()
        # Delete any existing patron auth services created by controller test setup.
        for auth_service in self._db.query(ExternalIntegration).filter(
            ExternalIntegration.goal==ExternalIntegration.PATRON_AUTH_GOAL
         ):
            self._db.delete(auth_service)

        # Delete any existing sitewide ConfigurationSettings.
        for setting in self._db.query(ConfigurationSetting).filter(
            ConfigurationSetting.library_id==None).filter(
            ConfigurationSetting.external_integration_id==None):
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

        # No collection or collection with protocol passed
        self_test_results = controller._get_prior_test_results({}, {})
        eq_(None, self_test_results)
        self_test_results = controller._get_prior_test_results(collectionNoProtocol, {})
        eq_(None, self_test_results)

        collection = MockAxis360API.mock_collection(self._db)
        # Test that a collection's protocol calls HasSelfTests.prior_test_results
        self_test_results = controller._get_prior_test_results(collection, Axis360API)
        args = self.prior_test_results_called_with[0]
        eq_(args[1], Axis360API)
        eq_(args[3], collection)

        OPDSCollection = self._collection()
        # If a collection's protocol is OPDSImporter, make sure that
        # OPDSImportMonitor.prior_test_results is called
        self_test_results = controller._get_prior_test_results(OPDSCollection, OPDSImporter)
        args = self.prior_test_results_called_with[0]
        eq_(args[1], OPDSImportMonitor)
        eq_(args[3], OPDSCollection)

        # We don't crash if there's a problem getting the prior test
        # results -- _get_prior_test_results just returns None.
        @classmethod
        def oops(cls, *args, **kwargs):
            raise Exception("Test result disaster!")
        HasSelfTests.prior_test_results = oops
        self_test_results = controller._get_prior_test_results(
            OPDSCollection, OPDSImporter
        )
        eq_(
            "Exception getting self-test results for collection %s: Test result disaster!" % (
                OPDSCollection.name
            ),
            self_test_results["exception"]
        )

        HasSelfTests.prior_test_results = old_prior_test_results


class TestSettingsController(SettingsControllerTest):

    def test_get_integration_protocols(self):
        """Test the _get_integration_protocols helper method."""
        class Protocol(object):
            __module__ = 'my name'
            NAME = 'my label'
            DESCRIPTION = 'my description'
            SITEWIDE = True
            SETTINGS = [1,2,3]
            CHILD_SETTINGS = [4,5]
            LIBRARY_SETTINGS = [6]
            CARDINALITY = 1

        [result] = SettingsController._get_integration_protocols([Protocol])
        expect = dict(
            sitewide=True, description='my description',
            settings=[1, 2, 3], library_settings=[6],
            child_settings=[4, 5], label='my label',
            cardinality=1, name='my name'
        )
        eq_(expect, result)

        # Remove the CARDINALITY setting
        del Protocol.CARDINALITY

        # And look in a different place for the name.
        [result] = SettingsController._get_integration_protocols(
            [Protocol], protocol_name_attr='NAME'
        )

        eq_('my label', result['name'])
        assert 'cardinality' not in result

    def test_get_integration_info(self):
        """Test the _get_integration_info helper method."""
        m = self.manager.admin_settings_controller._get_integration_info

        # Test the case where there are integrations in the database
        # with the given goal, but none of them match the
        # configuration.
        goal = self._str
        integration = self._external_integration(
            protocol="a protocol", goal=goal
        )
        eq_([], m(goal, [dict(name="some other protocol")]))

    def test_create_integration(self):
        """Test the _create_integration helper method."""

        m = self.manager.admin_settings_controller._create_integration

        protocol_definitions = [
            dict(name="allow many"),
            dict(name="allow one", cardinality=1),
        ]
        goal = "some goal"

        # You get an error if you don't pass in a protocol.
        eq_(
            (NO_PROTOCOL_FOR_NEW_SERVICE, False),
            m(protocol_definitions, None, goal)
        )

        # You get an error if you do provide a protocol but no definition
        # for it can be found.
        eq_(
            (UNKNOWN_PROTOCOL, False),
            m(protocol_definitions, "no definition", goal)
        )

        # If the protocol has multiple cardinality you can create as many
        # integrations using that protocol as you want.
        i1, is_new1 = m(protocol_definitions, "allow many", goal)
        eq_(True, is_new1)

        i2, is_new2 = m(protocol_definitions, "allow many", goal)
        eq_(True, is_new2)

        assert i1 != i2
        for i in [i1, i2]:
            eq_("allow many", i.protocol)
            eq_(goal, i.goal)

        # If the protocol has single cardinality, you can only create one
        # integration using that protocol before you start getting errors.
        i1, is_new1 = m(protocol_definitions, "allow one", goal)
        eq_(True, is_new1)

        i2, is_new2 = m(protocol_definitions, "allow one", goal)
        eq_(False, is_new2)
        eq_(DUPLICATE_INTEGRATION, i2)
