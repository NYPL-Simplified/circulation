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
import jwt
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

from .. import sample_data
from ..test_controller import CirculationControllerTest
from api.admin.controller import (
    setup_admin_controllers,
    AdminAnnotator,
    SettingsController,
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
    WorkGenre
)
from core.lane import Lane
from core.s3 import S3Uploader, MockS3Uploader
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
from core.opds import AcquisitionFeed
from core.facets import FacetConstants
from datetime import date, datetime, timedelta

from api.authenticator import AuthenticationProvider, BasicAuthenticationProvider
from api.simple_authentication import SimpleAuthenticationProvider
from api.millenium_patron import MilleniumPatronAPI
from api.sip import SIP2AuthenticationProvider
from api.firstbook import FirstBookAuthenticationAPI
from api.clever import CleverAuthenticationAPI

from api.novelist import NoveListAPI

from api.odl import SharedODLAPI

from api.google_analytics_provider import GoogleAnalyticsProvider
from core.local_analytics_provider import LocalAnalyticsProvider

from api.adobe_vendor_id import AuthdataUtility

from core.external_search import ExternalSearchIndex

class AdminControllerTest(CirculationControllerTest):

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
        resource_path = os.path.join(base_path, "..", "files", "images")

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
        resource_path = os.path.join(base_path, "..", "files", "images")
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
        resource_path = os.path.join(base_path, "..", "files", "images")
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
        resource_path = os.path.join(base_path, "..", "files", "images")
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
            assert "Sign In With Google" in response.data
            assert "Email" not in response.data
            assert "Password" not in response.data

        # If there are multiple auth providers, the login page
        # shows them all.
        self.admin.password = "password"
        with self.app.test_request_context('/admin/sign_in?redirect=foo'):
            response = self.manager.admin_sign_in_controller.sign_in()
            eq_(200, response.status_code)
            assert "GOOGLE REDIRECT" in response.data
            assert "Sign In With Google" in response.data
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
                ("entries", json.dumps([dict(identifier_urn=work.presentation_edition.primary_identifier.urn)])),
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
        edition = self._edition()

        [c1] = edition.author_contributors
        c1.display_name = self._str
        c2, ignore = self._contributor()
        c2.display_name = self._str
        edition.add_contributor(c2, Contributor.AUTHOR_ROLE)
        list.add_entry(edition)
        collection = self._collection()
        collection.customlists = [list]
        with self.request_context_with_library_and_admin("/"):
            response = self.manager.admin_custom_lists_controller.custom_list(list.id)
            eq_(list.id, response.get("id"))
            eq_(list.name, response.get("name"))
            eq_(1, response.get("entry_count"))
            eq_(1, len(response.get("entries")))
            [entry] = response.get("entries")
            eq_(edition.primary_identifier.urn, entry.get("identifier_urn"))
            eq_(edition.title, entry.get("title"))
            eq_(2, len(entry.get("authors")))
            eq_(Edition.medium_to_additional_type[Edition.BOOK_MEDIUM], entry.get("medium"))
            eq_(edition.language, entry.get("language"))
            eq_(set([c1.display_name, c2.display_name]),
                set(entry.get("authors")))
            eq_(1, len(response.get("collections")))
            [c] = response.get("collections")
            eq_(collection.name, c.get("name"))
            eq_(collection.id, c.get("id"))
            eq_(collection.protocol, c.get("protocol"))

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

        new_entries = [dict(identifier_urn=work.presentation_edition.primary_identifier.urn,
                            medium=Edition.medium_to_additional_type[work.presentation_edition.medium])
                       for work in [w2, w3]]

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

class TestDashboardController(AdminControllerTest):

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
        with self.app.test_request_context("/"):

            # At first, there's one patron in the database.
            response = self.manager.admin_dashboard_controller.stats()
            patron_data = response.get('patrons')
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
            patron_data = response.get('patrons')
            eq_(4, patron_data.get('total'))
            eq_(1, patron_data.get('with_active_loans'))
            eq_(2, patron_data.get('with_active_loans_or_holds'))
            eq_(1, patron_data.get('loans'))
            eq_(1, patron_data.get('holds'))

    def test_stats_inventory(self):
        with self.app.test_request_context("/"):

            # At first, there is 1 open access title in the database,
            # created in CirculationControllerTest.setup.
            response = self.manager.admin_dashboard_controller.stats()
            inventory_data = response.get('inventory')
            eq_(1, inventory_data.get('titles'))
            eq_(0, inventory_data.get('licenses'))
            eq_(0, inventory_data.get('available_licenses'))

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
            inventory_data = response.get('inventory')
            eq_(4, inventory_data.get('titles'))
            eq_(15, inventory_data.get('licenses'))
            eq_(4, inventory_data.get('available_licenses'))

    def test_stats_vendors(self):
        with self.app.test_request_context("/"):

            # At first, there is 1 open access title in the database,
            # created in CirculationControllerTest.setup.
            response = self.manager.admin_dashboard_controller.stats()
            vendor_data = response.get('vendors')
            eq_(1, vendor_data.get('open_access'))
            eq_(None, vendor_data.get('overdrive'))
            eq_(None, vendor_data.get('bibliotheca'))
            eq_(None, vendor_data.get('axis360'))

            edition1, pool1 = self._edition(with_license_pool=True,
                                            with_open_access_download=False,
                                            data_source_name=DataSource.OVERDRIVE)
            pool1.open_access = False
            pool1.licenses_owned = 10

            edition2, pool2 = self._edition(with_license_pool=True,
                                            with_open_access_download=False,
                                            data_source_name=DataSource.OVERDRIVE)
            pool2.open_access = False
            pool2.licenses_owned = 0

            edition3, pool3 = self._edition(with_license_pool=True,
                                            with_open_access_download=False,
                                            data_source_name=DataSource.BIBLIOTHECA)
            pool3.open_access = False
            pool3.licenses_owned = 3

            edition4, pool4 = self._edition(with_license_pool=True,
                                            with_open_access_download=False,
                                            data_source_name=DataSource.AXIS_360)
            pool4.open_access = False
            pool4.licenses_owned = 5

            response = self.manager.admin_dashboard_controller.stats()
            vendor_data = response.get('vendors')
            eq_(1, vendor_data.get('open_access'))
            eq_(1, vendor_data.get('overdrive'))
            eq_(1, vendor_data.get('bibliotheca'))
            eq_(1, vendor_data.get('axis360'))

class TestSettingsController(AdminControllerTest):

    def setup(self):
        super(TestSettingsController, self).setup()
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
        return HTTP.process_debuggable_response(response)

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

    def test_libraries_get_with_no_libraries(self):
        # Delete any existing library created by the controller test setup.
        library = get_one(self._db, Library)
        if library:
            self._db.delete(library)

        with self.app.test_request_context("/"):
            response = self.manager.admin_settings_controller.libraries()
            eq_(response.get("libraries"), [])

    def test_libraries_get_with_multiple_libraries(self):
        # Delete any existing library created by the controller test setup.
        library = get_one(self._db, Library)
        if library:
            self._db.delete(library)

        l1 = self._library("Library 1", "L1")
        l2 = self._library("Library 2", "L2")
        l3 = self._library("Library 3", "L3")
        # L2 has some additional library-wide settings.
        ConfigurationSetting.for_library(Configuration.FEATURED_LANE_SIZE, l2).value = 5
        ConfigurationSetting.for_library(
            Configuration.DEFAULT_FACET_KEY_PREFIX + FacetConstants.ORDER_FACET_GROUP_NAME, l2
        ).value = FacetConstants.ORDER_RANDOM
        ConfigurationSetting.for_library(
            Configuration.ENABLED_FACETS_KEY_PREFIX + FacetConstants.ORDER_FACET_GROUP_NAME, l2
        ).value = json.dumps([FacetConstants.ORDER_TITLE, FacetConstants.ORDER_RANDOM])
        # The admin only has access to L1 and L2.
        self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        self.admin.add_role(AdminRole.LIBRARIAN, l1)
        self.admin.add_role(AdminRole.LIBRARY_MANAGER, l2)

        with self.request_context_with_admin("/"):
            response = self.manager.admin_settings_controller.libraries()
            libraries = response.get("libraries")
            eq_(2, len(libraries))

            eq_(l1.uuid, libraries[0].get("uuid"))
            eq_(l2.uuid, libraries[1].get("uuid"))

            eq_(l1.name, libraries[0].get("name"))
            eq_(l2.name, libraries[1].get("name"))

            eq_(l1.short_name, libraries[0].get("short_name"))
            eq_(l2.short_name, libraries[1].get("short_name"))

            eq_({}, libraries[0].get("settings"))
            eq_(3, len(libraries[1].get("settings").keys()))
            settings = libraries[1].get("settings")
            eq_("5", settings.get(Configuration.FEATURED_LANE_SIZE))
            eq_(FacetConstants.ORDER_RANDOM,
                settings.get(Configuration.DEFAULT_FACET_KEY_PREFIX + FacetConstants.ORDER_FACET_GROUP_NAME))
            eq_([FacetConstants.ORDER_TITLE, FacetConstants.ORDER_RANDOM],
               settings.get(Configuration.ENABLED_FACETS_KEY_PREFIX + FacetConstants.ORDER_FACET_GROUP_NAME))

    def test_libraries_post_errors(self):
        self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Brooklyn Public Library"),
                ("short_name", "bpl"),
            ])
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_settings_controller.libraries)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Brooklyn Public Library"),
            ])
            response = self.manager.admin_settings_controller.libraries()
            eq_(response, MISSING_LIBRARY_SHORT_NAME)

        library = self._library()
        self.admin.add_role(AdminRole.LIBRARIAN, library)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("uuid", library.uuid),
                ("name", "Brooklyn Public Library"),
                ("short_name", library.short_name),
            ])
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_settings_controller.libraries)

        self.admin.add_role(AdminRole.SYSTEM_ADMIN)
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("uuid", "1234"),
                ("name", "Brooklyn Public Library"),
                ("short_name", "bpl"),
            ])
            response = self.manager.admin_settings_controller.libraries()
            eq_(response.uri, LIBRARY_NOT_FOUND.uri)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Brooklyn Public Library"),
                ("short_name", library.short_name),
            ])
            response = self.manager.admin_settings_controller.libraries()
            eq_(response, LIBRARY_SHORT_NAME_ALREADY_IN_USE)

        bpl, ignore = get_one_or_create(
            self._db, Library, short_name="bpl"
        )
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("uuid", bpl.uuid),
                ("name", "Brooklyn Public Library"),
                ("short_name", library.short_name),
            ])
            response = self.manager.admin_settings_controller.libraries()
            eq_(response, LIBRARY_SHORT_NAME_ALREADY_IN_USE)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("uuid", library.uuid),
                ("name", "The New York Public Library"),
                ("short_name", library.short_name),
            ])
            response = self.manager.admin_settings_controller.libraries()
            eq_(response.uri, INCOMPLETE_CONFIGURATION.uri)

    def test_libraries_post_create(self):
        class TestFileUpload(StringIO):
            headers = { "Content-Type": "image/png" }
        image_data = '\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x01\x03\x00\x00\x00%\xdbV\xca\x00\x00\x00\x06PLTE\xffM\x00\x01\x01\x01\x8e\x1e\xe5\x1b\x00\x00\x00\x01tRNS\xcc\xd24V\xfd\x00\x00\x00\nIDATx\x9cc`\x00\x00\x00\x02\x00\x01H\xaf\xa4q\x00\x00\x00\x00IEND\xaeB`\x82'

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "The New York Public Library"),
                ("short_name", "nypl"),
                (Configuration.WEBSITE_URL, "https://library.library/"),
                (Configuration.TINY_COLLECTION_LANGUAGES, 'ger'),
                (Configuration.DEFAULT_NOTIFICATION_EMAIL_ADDRESS, "email@example.com"),
                (Configuration.FEATURED_LANE_SIZE, "5"),
                (Configuration.DEFAULT_FACET_KEY_PREFIX + FacetConstants.ORDER_FACET_GROUP_NAME,
                 FacetConstants.ORDER_RANDOM),
                (Configuration.ENABLED_FACETS_KEY_PREFIX + FacetConstants.ORDER_FACET_GROUP_NAME + "_" + FacetConstants.ORDER_TITLE,
                 ''),
                (Configuration.ENABLED_FACETS_KEY_PREFIX + FacetConstants.ORDER_FACET_GROUP_NAME + "_" + FacetConstants.ORDER_RANDOM,
                 ''),
            ])
            flask.request.files = MultiDict([
                (Configuration.LOGO, TestFileUpload(image_data)),
            ])
            response = self.manager.admin_settings_controller.libraries()
            eq_(response.status_code, 201)

        library = get_one(self._db, Library, short_name="nypl")

        eq_(library.uuid, response.response[0])
        eq_(library.name, "The New York Public Library")
        eq_(library.short_name, "nypl")
        eq_("5", ConfigurationSetting.for_library(Configuration.FEATURED_LANE_SIZE, library).value)
        eq_(FacetConstants.ORDER_RANDOM,
            ConfigurationSetting.for_library(
                Configuration.DEFAULT_FACET_KEY_PREFIX + FacetConstants.ORDER_FACET_GROUP_NAME,
                library).value)
        eq_(json.dumps([FacetConstants.ORDER_TITLE, FacetConstants.ORDER_RANDOM]),
            ConfigurationSetting.for_library(
                Configuration.ENABLED_FACETS_KEY_PREFIX + FacetConstants.ORDER_FACET_GROUP_NAME,
                library).value)
        eq_("data:image/png;base64,%s" % base64.b64encode(image_data),
            ConfigurationSetting.for_library(Configuration.LOGO, library).value)

        # When the library was created, default lanes were also created
        # according to its language setup. This library has one tiny
        # collection (not a good choice for a real library), so only
        # two lanes were created: "Other Languages" and then "German"
        # underneath it.
        [german, other_languages] = sorted(
            library.lanes, key=lambda x: x.display_name
        )
        eq_(None, other_languages.parent)
        eq_(['ger'], other_languages.languages)
        eq_(other_languages, german.parent)
        eq_(['ger'], german.languages)

    def test_libraries_post_edit(self):
        # A library already exists.
        library = self._library("New York Public Library", "nypl")

        ConfigurationSetting.for_library(Configuration.FEATURED_LANE_SIZE, library).value = 5
        ConfigurationSetting.for_library(
            Configuration.DEFAULT_FACET_KEY_PREFIX + FacetConstants.ORDER_FACET_GROUP_NAME, library
        ).value = FacetConstants.ORDER_RANDOM
        ConfigurationSetting.for_library(
            Configuration.ENABLED_FACETS_KEY_PREFIX + FacetConstants.ORDER_FACET_GROUP_NAME, library
        ).value = json.dumps([FacetConstants.ORDER_TITLE, FacetConstants.ORDER_RANDOM])
        ConfigurationSetting.for_library(
            Configuration.LOGO, library
        ).value = "A tiny image"

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("uuid", library.uuid),
                ("name", "The New York Public Library"),
                ("short_name", "nypl"),
                (Configuration.FEATURED_LANE_SIZE, "20"),
                (Configuration.MINIMUM_FEATURED_QUALITY, "0.9"),
                (Configuration.WEBSITE_URL, "https://library.library/"),
                (Configuration.DEFAULT_NOTIFICATION_EMAIL_ADDRESS, "email@example.com"),
                (Configuration.DEFAULT_FACET_KEY_PREFIX + FacetConstants.ORDER_FACET_GROUP_NAME,
                 FacetConstants.ORDER_AUTHOR),
                (Configuration.ENABLED_FACETS_KEY_PREFIX + FacetConstants.ORDER_FACET_GROUP_NAME + "_" + FacetConstants.ORDER_AUTHOR,
                 ''),
                (Configuration.ENABLED_FACETS_KEY_PREFIX + FacetConstants.ORDER_FACET_GROUP_NAME + "_" + FacetConstants.ORDER_RANDOM,
                 ''),
            ])
            flask.request.files = MultiDict([])
            response = self.manager.admin_settings_controller.libraries()
            eq_(response.status_code, 200)

        library = get_one(self._db, Library, uuid=library.uuid)

        eq_(library.uuid, response.response[0])
        eq_(library.name, "The New York Public Library")
        eq_(library.short_name, "nypl")

        # The library-wide settings were updated.
        def val(x):
            return ConfigurationSetting.for_library(x, library).value
        eq_("https://library.library/", val(Configuration.WEBSITE_URL))
        eq_("email@example.com", val(Configuration.DEFAULT_NOTIFICATION_EMAIL_ADDRESS))
        eq_("20", val(Configuration.FEATURED_LANE_SIZE))
        eq_("0.9", val(Configuration.MINIMUM_FEATURED_QUALITY))
        eq_(FacetConstants.ORDER_AUTHOR,
            val(Configuration.DEFAULT_FACET_KEY_PREFIX + FacetConstants.ORDER_FACET_GROUP_NAME)
        )
        eq_(json.dumps([FacetConstants.ORDER_AUTHOR, FacetConstants.ORDER_RANDOM]),
            val(Configuration.ENABLED_FACETS_KEY_PREFIX + FacetConstants.ORDER_FACET_GROUP_NAME)
        )

        # The library-wide logo was not updated and has been left alone.
        eq_("A tiny image",
            ConfigurationSetting.for_library(Configuration.LOGO, library).value
        )

    def test_library_delete(self):
        library = self._library()

        with self.request_context_with_admin("/", method="DELETE"):
            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_settings_controller.library,
                          library.uuid)

            self.admin.add_role(AdminRole.SYSTEM_ADMIN)
            response = self.manager.admin_settings_controller.library(library.uuid)
            eq_(response.status_code, 200)

        library = get_one(self._db, Library, uuid=library.uuid)
        eq_(None, library)

    def test_collections_get_with_no_collections(self):
        # Delete any existing collections created by the test setup.
        for collection in self._db.query(Collection):
            self._db.delete(collection)

        with self.app.test_request_context("/"):
            response = self.manager.admin_settings_controller.collections()
            eq_(response.get("collections"), [])


            names = [p.get("name") for p in response.get("protocols")]
            assert ExternalIntegration.OVERDRIVE in names
            assert ExternalIntegration.OPDS_IMPORT in names

    def test_collections_get_protocols(self):

        [c1] = self._default_library.collections

        # When there is no storage integration configured,
        # the protocols will not offer a 'mirror_integration_id'
        # setting.
        with self.request_context_with_admin("/"):
            response = self.manager.admin_settings_controller.collections()
            protocols = response.get('protocols')
            for protocol in protocols:
                assert all([s.get('key') != 'mirror_integration_id'
                            for s in protocol['settings']])

        # When storage integrations are configured, each protocol will
        # offer a 'mirror_integration_id' setting.
        storage1 = self._external_integration(
            name="integration 1",
            protocol=ExternalIntegration.S3,
            goal=ExternalIntegration.STORAGE_GOAL
        )
        storage2 = self._external_integration(
            name="integration 2",
            protocol="Some other protocol",
            goal=ExternalIntegration.STORAGE_GOAL
        )

        with self.request_context_with_admin("/"):
            controller = self.manager.admin_settings_controller
            response = controller.collections()
            protocols = response.get('protocols')
            for protocol in protocols:
                [setting] = [x for x in protocol['settings']
                             if x.get('key') == 'mirror_integration_id']
                eq_("Mirror", setting['label'])
                options = setting['options']

                # The first option is to disable mirroring on this
                # collection altogether.
                no_mirror = options[0]
                eq_(controller.NO_MIRROR_INTEGRATION, no_mirror['key'])

                # The other options are to use one of the storage
                # integrations to do the mirroring.
                use_mirrors = [(x['key'], x['label'])
                               for x in options[1:]]
                expect = [(integration.id, integration.name)
                          for integration in (storage1, storage2)]
                eq_(expect, use_mirrors)

    def test_collections_get_collections_with_multiple_collections(self):

        [c1] = self._default_library.collections

        c2 = self._collection(
            name="Collection 2", protocol=ExternalIntegration.OVERDRIVE,
        )
        c2_storage = self._external_integration(
            protocol=ExternalIntegration.S3,
            goal=ExternalIntegration.STORAGE_GOAL
        )
        c2.external_account_id = "1234"
        c2.external_integration.password = "b"
        c2.mirror_integration_id=c2_storage.id

        c3 = self._collection(
            name="Collection 3", protocol=ExternalIntegration.OVERDRIVE,
        )
        c3.external_account_id = "5678"
        c3.parent = c2

        l1 = self._library(short_name="L1")
        c3.libraries += [l1, self._default_library]
        c3.external_integration.libraries += [l1]
        ConfigurationSetting.for_library_and_externalintegration(
            self._db, "ebook_loan_duration", l1, c3.external_integration).value = "14"

        l1_librarian, ignore = create(self._db, Admin, email="admin@l1.org")
        l1_librarian.add_role(AdminRole.LIBRARIAN, l1)

        with self.request_context_with_admin("/"):
            controller = self.manager.admin_settings_controller
            response = controller.collections()
            # The system admin can see all collections.
            coll2, coll3, coll1 = sorted(
                response.get("collections"), key = lambda c: c.get('name')
            )
            eq_(c1.id, coll1.get("id"))
            eq_(c2.id, coll2.get("id"))
            eq_(c3.id, coll3.get("id"))

            eq_(c1.name, coll1.get("name"))
            eq_(c2.name, coll2.get("name"))
            eq_(c3.name, coll3.get("name"))

            eq_(c1.protocol, coll1.get("protocol"))
            eq_(c2.protocol, coll2.get("protocol"))
            eq_(c3.protocol, coll3.get("protocol"))

            settings1 = coll1.get("settings", {})
            settings2 = coll2.get("settings", {})
            settings3 = coll3.get("settings", {})

            eq_(controller.NO_MIRROR_INTEGRATION,
                settings1.get("mirror_integration_id"))
            eq_(c2_storage.id, settings2.get("mirror_integration_id"))
            eq_(controller.NO_MIRROR_INTEGRATION,
                settings3.get("mirror_integration_id"))

            eq_(c1.external_account_id, settings1.get("external_account_id"))
            eq_(c2.external_account_id, settings2.get("external_account_id"))
            eq_(c3.external_account_id, settings3.get("external_account_id"))

            eq_(c1.external_integration.password, settings1.get("password"))
            eq_(c2.external_integration.password, settings2.get("password"))

            eq_(c2.id, coll3.get("parent_id"))

            coll3_libraries = coll3.get("libraries")
            eq_(2, len(coll3_libraries))
            coll3_l1, coll3_default = sorted(coll3_libraries, key=lambda x: x.get("short_name"))
            eq_("L1", coll3_l1.get("short_name"))
            eq_("14", coll3_l1.get("ebook_loan_duration"))
            eq_(self._default_library.short_name, coll3_default.get("short_name"))

        with self.request_context_with_admin("/", admin=l1_librarian):
            # A librarian only sees collections associated with their library.
            response = controller.collections()
            [coll3] = response.get("collections")
            eq_(c3.id, coll3.get("id"))

            coll3_libraries = coll3.get("libraries")
            eq_(1, len(coll3_libraries))
            eq_("L1", coll3_libraries[0].get("short_name"))
            eq_("14", coll3_libraries[0].get("ebook_loan_duration"))

    def test_collections_post_errors(self):
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", "Overdrive"),
            ])
            response = self.manager.admin_settings_controller.collections()
            eq_(response, MISSING_COLLECTION_NAME)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "collection"),
            ])
            response = self.manager.admin_settings_controller.collections()
            eq_(response, NO_PROTOCOL_FOR_NEW_SERVICE)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "collection"),
                ("protocol", "Unknown"),
            ])
            response = self.manager.admin_settings_controller.collections()
            eq_(response, UNKNOWN_PROTOCOL)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", "123456789"),
                ("name", "collection"),
                ("protocol", "Bibliotheca"),
            ])
            response = self.manager.admin_settings_controller.collections()
            eq_(response, MISSING_COLLECTION)

        collection = self._collection(
            name="Collection 1",
            protocol=ExternalIntegration.OVERDRIVE
        )

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Collection 1"),
                ("protocol", "Bibliotheca"),
            ])
            response = self.manager.admin_settings_controller.collections()
            eq_(response, COLLECTION_NAME_ALREADY_IN_USE)

        self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", collection.id),
                ("name", "Collection 1"),
                ("protocol", "Overdrive"),
            ])
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_settings_controller.collections)

        self.admin.add_role(AdminRole.SYSTEM_ADMIN)
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", collection.id),
                ("name", "Collection 1"),
                ("protocol", "Bibliotheca"),
            ])
            response = self.manager.admin_settings_controller.collections()
            eq_(response, CANNOT_CHANGE_PROTOCOL)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Collection 2"),
                ("protocol", "Bibliotheca"),
                ("parent_id", "1234"),
            ])
            response = self.manager.admin_settings_controller.collections()
            eq_(response, PROTOCOL_DOES_NOT_SUPPORT_PARENTS)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Collection 2"),
                ("protocol", "Overdrive"),
                ("parent_id", "1234"),
            ])
            response = self.manager.admin_settings_controller.collections()
            eq_(response, MISSING_PARENT)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "collection"),
                ("protocol", "OPDS Import"),
                ("external_account_id", "test.com"),
                ("data_source", "test"),
                ("libraries", json.dumps([{"short_name": "nosuchlibrary"}])),
            ])
            response = self.manager.admin_settings_controller.collections()
            eq_(response.uri, NO_SUCH_LIBRARY.uri)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "collection1"),
                ("protocol", "OPDS Import"),
            ])
            response = self.manager.admin_settings_controller.collections()
            eq_(response.uri, INCOMPLETE_CONFIGURATION.uri)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "collection1"),
                ("protocol", "Overdrive"),
                ("external_account_id", "1234"),
                ("username", "user"),
                ("password", "password"),
            ])
            response = self.manager.admin_settings_controller.collections()
            eq_(response.uri, INCOMPLETE_CONFIGURATION.uri)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "collection1"),
                ("protocol", "Bibliotheca"),
                ("external_account_id", "1234"),
                ("password", "password"),
            ])
            response = self.manager.admin_settings_controller.collections()
            eq_(response.uri, INCOMPLETE_CONFIGURATION.uri)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "collection1"),
                ("protocol", "Axis 360"),
                ("username", "user"),
                ("password", "password"),
            ])
            response = self.manager.admin_settings_controller.collections()
            eq_(response.uri, INCOMPLETE_CONFIGURATION.uri)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "collection1"),
                ("protocol", ExternalIntegration.RB_DIGITAL),
                ("username", "user"),
                ("password", "password"),
            ])
            response = self.manager.admin_settings_controller.collections()
            eq_(response.uri, INCOMPLETE_CONFIGURATION.uri)

    def test_collections_post_create(self):
        l1, ignore = create(
            self._db, Library, name="Library 1", short_name="L1",
        )
        l2, ignore = create(
            self._db, Library, name="Library 2", short_name="L2",
        )
        l3, ignore = create(
            self._db, Library, name="Library 3", short_name="L3",
        )

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "New Collection"),
                ("protocol", "Overdrive"),
                ("libraries", json.dumps([
                    {"short_name": "L1", "ils_name": "l1_ils"},
                    {"short_name":"L2", "ils_name": "l2_ils"}
                ])),
                ("external_account_id", "acctid"),
                ("username", "username"),
                ("password", "password"),
                ("website_id", "1234"),
            ])
            response = self.manager.admin_settings_controller.collections()
            eq_(response.status_code, 201)

        # The collection was created and configured properly.
        collection = get_one(self._db, Collection, name="New Collection")
        eq_(collection.id, int(response.response[0]))
        eq_("New Collection", collection.name)
        eq_("acctid", collection.external_account_id)
        eq_("username", collection.external_integration.username)
        eq_("password", collection.external_integration.password)

        # Two libraries now have access to the collection.
        eq_([collection], l1.collections)
        eq_([collection], l2.collections)
        eq_([], l3.collections)

        # Additional settings were set on the collection.
        setting = collection.external_integration.setting("website_id")
        eq_("website_id", setting.key)
        eq_("1234", setting.value)

        eq_("l1_ils", ConfigurationSetting.for_library_and_externalintegration(
                self._db, "ils_name", l1, collection.external_integration).value)
        eq_("l2_ils", ConfigurationSetting.for_library_and_externalintegration(
                self._db, "ils_name", l2, collection.external_integration).value)

        # This collection will be a child of the first collection.
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Child Collection"),
                ("protocol", "Overdrive"),
                ("parent_id", collection.id),
                ("libraries", json.dumps([{"short_name": "L3", "ils_name": "l3_ils"}])),
                ("external_account_id", "child-acctid"),
            ])
            response = self.manager.admin_settings_controller.collections()
            eq_(response.status_code, 201)

        # The collection was created and configured properly.
        child = get_one(self._db, Collection, name="Child Collection")
        eq_(child.id, int(response.response[0]))
        eq_("Child Collection", child.name)
        eq_("child-acctid", child.external_account_id)

        # The settings that are inherited from the parent weren't set.
        eq_(None, child.external_integration.username)
        eq_(None, child.external_integration.password)
        setting = child.external_integration.setting("website_id")
        eq_(None, setting.value)

        # One library has access to the collection.
        eq_([child], l3.collections)

        eq_("l3_ils", ConfigurationSetting.for_library_and_externalintegration(
                self._db, "ils_name", l3, child.external_integration).value)

    def test_collections_post_edit(self):
        # The collection exists.
        collection = self._collection(
            name="Collection 1",
            protocol=ExternalIntegration.OVERDRIVE
        )

        l1, ignore = create(
            self._db, Library, name="Library 1", short_name="L1",
        )

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", collection.id),
                ("name", "Collection 1"),
                ("protocol", ExternalIntegration.OVERDRIVE),
                ("external_account_id", "1234"),
                ("username", "user2"),
                ("password", "password"),
                ("website_id", "1234"),
                ("libraries", json.dumps([{"short_name": "L1", "ils_name": "the_ils"}])),
            ])
            response = self.manager.admin_settings_controller.collections()
            eq_(response.status_code, 200)

        eq_(collection.id, int(response.response[0]))

        # The collection has been changed.
        eq_("user2", collection.external_integration.username)

        # A library now has access to the collection.
        eq_([collection], l1.collections)

        # Additional settings were set on the collection.
        setting = collection.external_integration.setting("website_id")
        eq_("website_id", setting.key)
        eq_("1234", setting.value)

        eq_("the_ils", ConfigurationSetting.for_library_and_externalintegration(
                self._db, "ils_name", l1, collection.external_integration).value)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", collection.id),
                ("name", "Collection 1"),
                ("protocol", ExternalIntegration.OVERDRIVE),
                ("external_account_id", "1234"),
                ("username", "user2"),
                ("password", "password"),
                ("website_id", "1234"),
                ("libraries", json.dumps([])),
            ])
            response = self.manager.admin_settings_controller.collections()
            eq_(response.status_code, 200)

        eq_(collection.id, int(response.response[0]))

        # The collection is the same.
        eq_("user2", collection.external_integration.username)
        eq_(ExternalIntegration.OVERDRIVE, collection.protocol)

        # But the library has been removed.
        eq_([], l1.collections)

        eq_(None, ConfigurationSetting.for_library_and_externalintegration(
                self._db, "ils_name", l1, collection.external_integration).value)

        parent = self._collection(
            name="Parent",
            protocol=ExternalIntegration.OVERDRIVE
        )

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", collection.id),
                ("name", "Collection 1"),
                ("protocol", ExternalIntegration.OVERDRIVE),
                ("parent_id", parent.id),
                ("external_account_id", "1234"),
                ("libraries", json.dumps([])),
            ])
            response = self.manager.admin_settings_controller.collections()
            eq_(response.status_code, 200)

        eq_(collection.id, int(response.response[0]))

        # The collection now has a parent.
        eq_(parent, collection.parent)

    def _base_collections_post_request(self, collection):
        """A template for POST requests to the collections controller."""
        return [
            ("id", collection.id),
            ("name", "Collection 1"),
            ("protocol", ExternalIntegration.RB_DIGITAL),
            ("external_account_id", "1234"),
            ("username", "user2"),
            ("password", "password"),
            ("url", "http://rb/"),
        ]

    def test_collections_post_edit_mirror_integration(self):
        # The collection exists.
        collection = self._collection(
            name="Collection 1",
            protocol=ExternalIntegration.RB_DIGITAL
        )

        # There is a storage integration not associated with the collection.
        storage = self._external_integration(
            protocol=ExternalIntegration.S3,
            goal=ExternalIntegration.STORAGE_GOAL
        )
        eq_(None, collection.mirror_integration_id)

        # It's possible to associate the storage integration with the
        # collection.
        base_request = self._base_collections_post_request(collection)
        with self.request_context_with_admin("/", method="POST"):
            request = MultiDict(
                base_request + [("mirror_integration_id", storage.id)]
            )
            flask.request.form = request
            response = self.manager.admin_settings_controller.collections()
            eq_(response.status_code, 200)
            eq_(storage.id, collection.mirror_integration_id)

        # It's possible to unset the mirror integration ID.
        controller = self.manager.admin_settings_controller
        with self.request_context_with_admin("/", method="POST"):
            request = MultiDict(
                base_request + [("mirror_integration_id",
                                 str(controller.NO_MIRROR_INTEGRATION))]
            )
            flask.request.form = request
            response = controller.collections()
            eq_(response.status_code, 200)
            eq_(None, collection.mirror_integration_id)

        # Providing a nonexistent integration ID gives an error.
        with self.request_context_with_admin("/", method="POST"):
            request = MultiDict(
                base_request + [("mirror_integration_id", -200)]
            )
            flask.request.form = request
            response = self.manager.admin_settings_controller.collections()
            eq_(response, MISSING_SERVICE)

    def test_cannot_set_non_storage_integration_as_mirror_integration(self):
        # The collection exists.
        collection = self._collection(
            name="Collection 1",
            protocol=ExternalIntegration.RB_DIGITAL
        )

        # There is a storage integration not associated with the collection,
        # which makes it possible to associate storage integrations
        # with collections through the collections controller.
        storage = self._external_integration(
            protocol=ExternalIntegration.S3,
            goal=ExternalIntegration.STORAGE_GOAL
        )

        # Trying to set a non-storage integration (such as the
        # integration associated with the collection's licenses) as
        # the collection's mirror integration gives an error.
        base_request = self._base_collections_post_request(collection)
        with self.request_context_with_admin("/", method="POST"):
            request = MultiDict(
                base_request + [
                    ("mirror_integration_id", collection.external_integration.id)
                ]
            )
            flask.request.form = request
            response = self.manager.admin_settings_controller.collections()
            eq_(response, INTEGRATION_GOAL_CONFLICT)

    def test_collections_post_edit_library_specific_configuration(self):
        # The collection exists.
        collection = self._collection(
            name="Collection 1",
            protocol=ExternalIntegration.RB_DIGITAL
        )

        l1, ignore = create(
            self._db, Library, name="Library 1", short_name="L1",
        )

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", collection.id),
                ("name", "Collection 1"),
                ("protocol", ExternalIntegration.RB_DIGITAL),
                ("external_account_id", "1234"),
                ("username", "user2"),
                ("password", "password"),
                ("url", "http://rb/"),
                ("libraries", json.dumps([
                    {
                        "short_name": "L1",
                        "ebook_loan_duration": "14",
                        "audio_loan_duration": "12"
                    }
                ])
                ),
            ])
            response = self.manager.admin_settings_controller.collections()
            eq_(response.status_code, 200)

        # Additional settings were set on the collection+library.
        eq_("14", ConfigurationSetting.for_library_and_externalintegration(
                self._db, "ebook_loan_duration", l1, collection.external_integration).value)
        eq_("12", ConfigurationSetting.for_library_and_externalintegration(
                self._db, "audio_loan_duration", l1, collection.external_integration).value)

        # Remove the connection between collection and library.
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", collection.id),
                ("name", "Collection 1"),
                ("protocol", ExternalIntegration.RB_DIGITAL),
                ("external_account_id", "1234"),
                ("username", "user2"),
                ("password", "password"),
                ("url", "http://rb/"),
                ("libraries", json.dumps([])),
            ])
            response = self.manager.admin_settings_controller.collections()
            eq_(response.status_code, 200)

        eq_(collection.id, int(response.response[0]))

        # The settings associated with the collection+library were removed
        # when the connection between collection and library was deleted.
        eq_(None, ConfigurationSetting.for_library_and_externalintegration(
                self._db, "ebook_loan_duration", l1, collection.external_integration).value)
        eq_(None, ConfigurationSetting.for_library_and_externalintegration(
                self._db, "audio_loan_duration", l1, collection.external_integration).value)
        eq_([], collection.libraries)

    def test_collection_delete(self):
        collection = self._collection()

        with self.request_context_with_admin("/", method="DELETE"):
            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_settings_controller.collection,
                          collection.id)

            self.admin.add_role(AdminRole.SYSTEM_ADMIN)
            response = self.manager.admin_settings_controller.collection(collection.id)
            eq_(response.status_code, 200)

        collection = get_one(self._db, Collection, id=collection.id)
        eq_(None, collection)

    def test_collection_delete_cant_delete_parent(self):
        parent = self._collection(protocol=ExternalIntegration.OVERDRIVE)
        child = self._collection(protocol=ExternalIntegration.OVERDRIVE)
        child.parent = parent

        with self.request_context_with_admin("/", method="DELETE"):
            response = self.manager.admin_settings_controller.collection(parent.id)
            eq_(CANNOT_DELETE_COLLECTION_WITH_CHILDREN, response)

    def test_admin_auth_services_get_with_no_services(self):
        with self.request_context_with_admin("/"):
            response = self.manager.admin_settings_controller.admin_auth_services()
            eq_(response.get("admin_auth_services"), [])

            # All the protocols in ExternalIntegration.ADMIN_AUTH_PROTOCOLS
            # are supported by the admin interface.
            eq_(sorted([p.get("name") for p in response.get("protocols")]),
                sorted(ExternalIntegration.ADMIN_AUTH_PROTOCOLS))

            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            self._db.flush()
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_settings_controller.admin_auth_services)

    def test_admin_auth_services_get_with_google_oauth_service(self):
        auth_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.GOOGLE_OAUTH,
            goal=ExternalIntegration.ADMIN_AUTH_GOAL
        )
        auth_service.url = "http://oauth.test"
        auth_service.username = "user"
        auth_service.password = "pass"
        auth_service.libraries += [self._default_library]
        ConfigurationSetting.for_library_and_externalintegration(
            self._db, "domains", self._default_library, auth_service
        ).value = json.dumps(["nypl.org"])

        with self.request_context_with_admin("/"):
            response = self.manager.admin_settings_controller.admin_auth_services()
            [service] = response.get("admin_auth_services")

            eq_(auth_service.id, service.get("id"))
            eq_(auth_service.name, service.get("name"))
            eq_(auth_service.protocol, service.get("protocol"))
            eq_(auth_service.url, service.get("settings").get("url"))
            eq_(auth_service.username, service.get("settings").get("username"))
            eq_(auth_service.password, service.get("settings").get("password"))
            [library_info] = service.get("libraries")
            eq_(self._default_library.short_name, library_info.get("short_name"))
            eq_(["nypl.org"], library_info.get("domains"))

    def test_admin_auth_services_post_errors(self):
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", "Unknown"),
            ])
            response = self.manager.admin_settings_controller.admin_auth_services()
            eq_(response, UNKNOWN_PROTOCOL)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([])
            response = self.manager.admin_settings_controller.admin_auth_services()
            eq_(response, NO_PROTOCOL_FOR_NEW_SERVICE)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", "1234"),
            ])
            response = self.manager.admin_settings_controller.admin_auth_services()
            eq_(response, MISSING_SERVICE)

        auth_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.GOOGLE_OAUTH,
            goal=ExternalIntegration.ADMIN_AUTH_GOAL
        )

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", str(auth_service.id)),
            ])
            response = self.manager.admin_settings_controller.admin_auth_services()
            eq_(response, CANNOT_CHANGE_PROTOCOL)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", "Google OAuth"),
            ])
            response = self.manager.admin_settings_controller.admin_auth_services()
            eq_(response.uri, INCOMPLETE_CONFIGURATION.uri)

        self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        self._db.flush()
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "oauth"),
                ("protocol", "Google OAuth"),
                ("url", "url"),
                ("username", "username"),
                ("password", "password"),
                ("domains", "nypl.org"),
            ])
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_settings_controller.admin_auth_services)

    def test_admin_auth_services_post_create(self):
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "oauth"),
                ("protocol", "Google OAuth"),
                ("url", "url"),
                ("username", "username"),
                ("password", "password"),
                ("libraries", json.dumps([{ "short_name": self._default_library.short_name,
                                            "domains": ["nypl.org", "gmail.com"] }])),
            ])
            response = self.manager.admin_settings_controller.admin_auth_services()
            eq_(response.status_code, 201)

        # The auth service was created and configured properly.
        auth_service = ExternalIntegration.admin_authentication(self._db)
        eq_(auth_service.protocol, response.response[0])
        eq_("oauth", auth_service.name)
        eq_("url", auth_service.url)
        eq_("username", auth_service.username)
        eq_("password", auth_service.password)

        eq_([self._default_library], auth_service.libraries)
        setting = ConfigurationSetting.for_library_and_externalintegration(
            self._db, "domains", self._default_library, auth_service
        )
        eq_("domains", setting.key)
        eq_(["nypl.org", "gmail.com"], json.loads(setting.value))

    def test_admin_auth_services_post_google_oauth_edit(self):
        # The auth service exists.
        auth_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.GOOGLE_OAUTH,
            goal=ExternalIntegration.ADMIN_AUTH_GOAL
        )
        auth_service.url = "url"
        auth_service.username = "user"
        auth_service.password = "pass"
        auth_service.libraries += [self._default_library]
        setting = ConfigurationSetting.for_library_and_externalintegration(
            self._db, "domains", self._default_library, auth_service)
        setting.value = json.dumps(["library1.org"])

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "oauth"),
                ("protocol", "Google OAuth"),
                ("url", "url2"),
                ("username", "user2"),
                ("password", "pass2"),
                ("libraries", json.dumps([{ "short_name": self._default_library.short_name,
                                            "domains": ["library2.org"] }])),
            ])
            response = self.manager.admin_settings_controller.admin_auth_services()
            eq_(response.status_code, 200)

        eq_(auth_service.protocol, response.response[0])
        eq_("oauth", auth_service.name)
        eq_("url2", auth_service.url)
        eq_("user2", auth_service.username)
        eq_("domains", setting.key)
        eq_(["library2.org"], json.loads(setting.value))

    def test_admin_auth_service_delete(self):
        auth_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.GOOGLE_OAUTH,
            goal=ExternalIntegration.ADMIN_AUTH_GOAL
        )
        auth_service.url = "url"
        auth_service.username = "user"
        auth_service.password = "pass"
        auth_service.set_setting("domains", json.dumps(["library1.org"]))

        with self.request_context_with_admin("/", method="DELETE"):
            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_settings_controller.admin_auth_service,
                          auth_service.protocol)

            self.admin.add_role(AdminRole.SYSTEM_ADMIN)
            response = self.manager.admin_settings_controller.admin_auth_service(auth_service.protocol)
            eq_(response.status_code, 200)

        service = get_one(self._db, ExternalIntegration, id=auth_service.id)
        eq_(None, service)

    def test_individual_admins_get(self):
        for admin in self._db.query(Admin):
            self._db.delete(admin)

        # There are two admins that can sign in with passwords, with different roles.
        admin1, ignore = create(self._db, Admin, email="admin1@nypl.org")
        admin1.password = "pass1"
        admin1.add_role(AdminRole.SYSTEM_ADMIN)
        admin2, ignore = create(self._db, Admin, email="admin2@nypl.org")
        admin2.password = "pass2"
        admin2.add_role(AdminRole.LIBRARY_MANAGER, self._default_library)
        admin2.add_role(AdminRole.SITEWIDE_LIBRARIAN)

        # These admins don't have passwords.
        admin3, ignore = create(self._db, Admin, email="admin3@nypl.org")
        admin3.add_role(AdminRole.LIBRARIAN, self._default_library)
        library2 = self._library()
        admin4, ignore = create(self._db, Admin, email="admin4@l2.org")
        admin4.add_role(AdminRole.LIBRARY_MANAGER, library2)
        admin5, ignore = create(self._db, Admin, email="admin5@l2.org")
        admin5.add_role(AdminRole.LIBRARIAN, library2)

        with self.request_context_with_admin("/", admin=admin1):
            # A system admin can see all other admins' roles.
            response = self.manager.admin_settings_controller.individual_admins()
            admins = response.get("individualAdmins")
            eq_([{"email": "admin1@nypl.org", "roles": [{ "role": AdminRole.SYSTEM_ADMIN }]},
                 {"email": "admin2@nypl.org", "roles": [{ "role": AdminRole.LIBRARY_MANAGER, "library": self._default_library.short_name }, { "role": AdminRole.SITEWIDE_LIBRARIAN }]},
                 {"email": "admin3@nypl.org", "roles": [{ "role": AdminRole.LIBRARIAN, "library": self._default_library.short_name }]},
                 {"email": "admin4@l2.org", "roles": [{ "role": AdminRole.LIBRARY_MANAGER, "library": library2.short_name }]},
                 {"email": "admin5@l2.org", "roles": [{ "role": AdminRole.LIBRARIAN, "library": library2.short_name }]}],
                admins)

        with self.request_context_with_admin("/", admin=admin2):
            # A sitewide librarian or library manager can also see all admins' roles.
            response = self.manager.admin_settings_controller.individual_admins()
            admins = response.get("individualAdmins")
            eq_([{"email": "admin1@nypl.org", "roles": [{ "role": AdminRole.SYSTEM_ADMIN }]},
                 {"email": "admin2@nypl.org", "roles": [{ "role": AdminRole.LIBRARY_MANAGER, "library": self._default_library.short_name }, { "role": AdminRole.SITEWIDE_LIBRARIAN }]},
                 {"email": "admin3@nypl.org", "roles": [{ "role": AdminRole.LIBRARIAN, "library": self._default_library.short_name }]},
                 {"email": "admin4@l2.org", "roles": [{ "role": AdminRole.LIBRARY_MANAGER, "library": library2.short_name }]},
                 {"email": "admin5@l2.org", "roles": [{ "role": AdminRole.LIBRARIAN, "library": library2.short_name }]}],
                admins)

        with self.request_context_with_admin("/", admin=admin3):
            # A librarian or library manager of a specific library can see all admins, but only
            # roles that affect their libraries.
            response = self.manager.admin_settings_controller.individual_admins()
            admins = response.get("individualAdmins")
            eq_([{"email": "admin1@nypl.org", "roles": [{ "role": AdminRole.SYSTEM_ADMIN }]},
                 {"email": "admin2@nypl.org", "roles": [{ "role": AdminRole.LIBRARY_MANAGER, "library": self._default_library.short_name }, { "role": AdminRole.SITEWIDE_LIBRARIAN }]},
                 {"email": "admin3@nypl.org", "roles": [{ "role": AdminRole.LIBRARIAN, "library": self._default_library.short_name }]},
                 {"email": "admin4@l2.org", "roles": []},
                 {"email": "admin5@l2.org", "roles": []}],
                admins)

        with self.request_context_with_admin("/", admin=admin4):
            response = self.manager.admin_settings_controller.individual_admins()
            admins = response.get("individualAdmins")
            eq_([{"email": "admin1@nypl.org", "roles": [{ "role": AdminRole.SYSTEM_ADMIN }]},
                 {"email": "admin2@nypl.org", "roles": [{ "role": AdminRole.SITEWIDE_LIBRARIAN }]},
                 {"email": "admin3@nypl.org", "roles": []},
                 {"email": "admin4@l2.org", "roles": [{ "role": AdminRole.LIBRARY_MANAGER, "library": library2.short_name }]},
                 {"email": "admin5@l2.org", "roles": [{ "role": AdminRole.LIBRARIAN, "library": library2.short_name }]}],
                admins)

        with self.request_context_with_admin("/", admin=admin5):
            response = self.manager.admin_settings_controller.individual_admins()
            admins = response.get("individualAdmins")
            eq_([{"email": "admin1@nypl.org", "roles": [{ "role": AdminRole.SYSTEM_ADMIN }]},
                 {"email": "admin2@nypl.org", "roles": [{ "role": AdminRole.SITEWIDE_LIBRARIAN }]},
                 {"email": "admin3@nypl.org", "roles": []},
                 {"email": "admin4@l2.org", "roles": [{ "role": AdminRole.LIBRARY_MANAGER, "library": library2.short_name }]},
                 {"email": "admin5@l2.org", "roles": [{ "role": AdminRole.LIBRARIAN, "library": library2.short_name }]}],
                admins)

    def test_individual_admins_post_errors(self):
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([])
            response = self.manager.admin_settings_controller.individual_admins()
            eq_(response.uri, INCOMPLETE_CONFIGURATION.uri)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
               ("email", "test@library.org"),
               ("roles", json.dumps([{ "role": AdminRole.LIBRARIAN, "library": "notalibrary" }])),
            ])
            response = self.manager.admin_settings_controller.individual_admins()
            eq_(response.uri, LIBRARY_NOT_FOUND.uri)

        library = self._library()
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
               ("email", "test@library.org"),
               ("roles", json.dumps([{ "role": "notarole", "library": library.short_name }])),
            ])
            response = self.manager.admin_settings_controller.individual_admins()
            eq_(response.uri, UNKNOWN_ROLE.uri)

    def test_individual_admins_post_permissions(self):
        l1 = self._library()
        l2 = self._library()
        system, ignore = create(self._db, Admin, email=self._str)
        system.add_role(AdminRole.SYSTEM_ADMIN)
        sitewide_manager, ignore = create(self._db, Admin, email=self._str)
        sitewide_manager.add_role(AdminRole.SITEWIDE_LIBRARY_MANAGER)
        sitewide_librarian, ignore = create(self._db, Admin, email=self._str)
        sitewide_librarian.add_role(AdminRole.SITEWIDE_LIBRARIAN)
        manager1, ignore = create(self._db, Admin, email=self._str)
        manager1.add_role(AdminRole.LIBRARY_MANAGER, l1)
        librarian1, ignore = create(self._db, Admin, email=self._str)
        librarian1.add_role(AdminRole.LIBRARIAN, l1)
        l2 = self._library()
        manager2, ignore = create(self._db, Admin, email=self._str)
        manager2.add_role(AdminRole.LIBRARY_MANAGER, l2)
        librarian2, ignore = create(self._db, Admin, email=self._str)
        librarian2.add_role(AdminRole.LIBRARIAN, l2)

        def test_changing_roles(admin_making_request, target_admin, roles=None, allowed=False):
            with self.request_context_with_admin("/", method="POST", admin=admin_making_request):
                flask.request.form = MultiDict([
                    ("email", target_admin.email),
                    ("roles", json.dumps(roles or [])),
                ])
                if allowed:
                    self.manager.admin_settings_controller.individual_admins()
                    self._db.rollback()
                else:
                    assert_raises(AdminNotAuthorized,
                                  self.manager.admin_settings_controller.individual_admins)

        test_changing_roles(system, system, allowed=True)
        for admin in [sitewide_manager, sitewide_librarian, manager1, librarian1, manager2, librarian2]:
            test_changing_roles(admin, system)

        for admin in [system, sitewide_manager]:
            test_changing_roles(admin, sitewide_manager, allowed=True)
        for admin in [sitewide_librarian, manager1, librarian1, manager2, librarian2]:
            test_changing_roles(admin, sitewide_manager)

        for admin in [system, sitewide_manager]:
            test_changing_roles(admin, sitewide_librarian, allowed=True)
        for admin in [sitewide_librarian, manager1, librarian1, manager2, librarian2]:
            test_changing_roles(admin, sitewide_librarian)

        test_changing_roles(manager1, manager1, allowed=True)
        test_changing_roles(manager1, sitewide_librarian,
                            roles=[{ "role": AdminRole.SITEWIDE_LIBRARIAN },
                                   { "role": AdminRole.LIBRARY_MANAGER, "library": l1.short_name }],
                            allowed=True)
        test_changing_roles(manager1, librarian1, allowed=True)
        test_changing_roles(manager2, librarian2,
                            roles=[{ "role": AdminRole.LIBRARIAN, "library": l1.short_name }])
        test_changing_roles(manager2, librarian1,
                            roles=[{ "role": AdminRole.LIBRARY_MANAGER, "library": l1.short_name }])

        test_changing_roles(sitewide_librarian, librarian1)
        test_changing_roles(sitewide_manager, sitewide_manager,
                            roles=[{ "role": AdminRole.SYSTEM_ADMIN }])
        test_changing_roles(sitewide_librarian, manager1,
                            roles=[{ "role": AdminRole.SITEWIDE_LIBRARY_MANAGER }])

        def test_changing_password(admin_making_request, target_admin, allowed=False):
            with self.request_context_with_admin("/", method="POST", admin=admin_making_request):
                flask.request.form = MultiDict([
                    ("email", target_admin.email),
                    ("password", "new password"),
                    ("roles", json.dumps([role.to_dict() for role in target_admin.roles])),
                ])
                if allowed:
                    self.manager.admin_settings_controller.individual_admins()
                    self._db.rollback()
                else:
                    assert_raises(AdminNotAuthorized,
                                  self.manager.admin_settings_controller.individual_admins)

        test_changing_password(system, system, allowed=True)
        for admin in [sitewide_manager, sitewide_librarian, manager1, librarian1, manager2, librarian2]:
            test_changing_password(admin, system)

        for admin in [system, sitewide_manager]:
            test_changing_password(admin, sitewide_manager, allowed=True)
        for admin in [sitewide_librarian, manager1, librarian1, manager2, librarian2]:
            test_changing_password(admin, sitewide_manager)

        for admin in [system, sitewide_manager, manager1, manager2]:
            test_changing_password(admin, sitewide_librarian, allowed=True)
        for admin in [sitewide_librarian, librarian1, librarian2]:
            test_changing_password(admin, sitewide_librarian)

        for admin in [system, sitewide_manager, manager1]:
            test_changing_password(admin, manager1, allowed=True)
            test_changing_password(admin, librarian1, allowed=True)
        for admin in [sitewide_librarian, manager2, librarian2]:
            test_changing_password(admin, manager1)
            test_changing_password(admin, librarian1)

        for admin in [system, sitewide_manager, manager2]:
            test_changing_password(admin, manager2, allowed=True)
            test_changing_password(admin, librarian2, allowed=True)
        for admin in [sitewide_librarian, manager1, librarian1]:
            test_changing_password(admin, manager2)
            test_changing_password(admin, librarian2)

    def test_individual_admins_post_create(self):
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("email", "admin@nypl.org"),
                ("password", "pass"),
                ("roles", json.dumps([{ "role": AdminRole.LIBRARY_MANAGER, "library": self._default_library.short_name }])),
            ])
            response = self.manager.admin_settings_controller.individual_admins()
            eq_(response.status_code, 201)

        # The admin was created.
        admin_match = Admin.authenticate(self._db, "admin@nypl.org", "pass")
        eq_(admin_match.email, response.response[0])
        assert admin_match
        assert admin_match.has_password("pass")

        [role] = admin_match.roles
        eq_(AdminRole.LIBRARY_MANAGER, role.role)
        eq_(self._default_library, role.library)

        # The new admin is a library manager, so they can create librarians.
        with self.request_context_with_admin("/", method="POST", admin=admin_match):
            flask.request.form = MultiDict([
                ("email", "admin2@nypl.org"),
                ("password", "pass"),
                ("roles", json.dumps([{ "role": AdminRole.LIBRARIAN, "library": self._default_library.short_name }])),
            ])
            response = self.manager.admin_settings_controller.individual_admins()
            eq_(response.status_code, 201)

        admin_match = Admin.authenticate(self._db, "admin2@nypl.org", "pass")
        eq_(admin_match.email, response.response[0])
        assert admin_match
        assert admin_match.has_password("pass")

        [role] = admin_match.roles
        eq_(AdminRole.LIBRARIAN, role.role)
        eq_(self._default_library, role.library)

    def test_individual_admins_post_edit(self):
        # An admin exists.
        admin, ignore = create(
            self._db, Admin, email="admin@nypl.org",
        )
        admin.password = "password"
        admin.add_role(AdminRole.SYSTEM_ADMIN)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("email", "admin@nypl.org"),
                ("password", "new password"),
                ("roles", json.dumps([{"role": AdminRole.SITEWIDE_LIBRARIAN},
                                      {"role": AdminRole.LIBRARY_MANAGER, "library": self._default_library.short_name}])),
            ])
            response = self.manager.admin_settings_controller.individual_admins()
            eq_(response.status_code, 200)

        eq_(admin.email, response.response[0])

        # The password was changed.
        old_password_match = Admin.authenticate(self._db, "admin@nypl.org", "password")
        eq_(None, old_password_match)

        new_password_match = Admin.authenticate(self._db, "admin@nypl.org", "new password")
        eq_(admin, new_password_match)

        # The roles were changed.
        eq_(False, admin.is_system_admin())
        [librarian_all, manager] = sorted(admin.roles, key=lambda x: x.role)
        eq_(AdminRole.SITEWIDE_LIBRARIAN, librarian_all.role)
        eq_(None, librarian_all.library)
        eq_(AdminRole.LIBRARY_MANAGER, manager.role)
        eq_(self._default_library, manager.library)

    def test_individual_admin_delete(self):
        librarian, ignore = create(
            self._db, Admin, email=self._str)
        librarian.password = "password"
        librarian.add_role(AdminRole.LIBRARIAN, self._default_library)

        sitewide_manager, ignore = create(
            self._db, Admin, email=self._str)
        sitewide_manager.add_role(AdminRole.SITEWIDE_LIBRARY_MANAGER)

        system_admin, ignore = create(
            self._db, Admin, email=self._str)
        system_admin.add_role(AdminRole.SYSTEM_ADMIN)

        with self.request_context_with_admin("/", method="DELETE", admin=librarian):
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_settings_controller.individual_admin,
                          librarian.email)

        with self.request_context_with_admin("/", method="DELETE", admin=sitewide_manager):
            response = self.manager.admin_settings_controller.individual_admin(librarian.email)
            eq_(response.status_code, 200)

            assert_raises(AdminNotAuthorized,
                          self.manager.admin_settings_controller.individual_admin,
                          system_admin.email)

        with self.request_context_with_admin("/", method="DELETE", admin=system_admin):
            response = self.manager.admin_settings_controller.individual_admin(system_admin.email)
            eq_(response.status_code, 200)

        admin = get_one(self._db, Admin, id=librarian.id)
        eq_(None, admin)

        admin = get_one(self._db, Admin, id=system_admin.id)
        eq_(None, admin)

    def test_individual_admins_post_create_on_setup(self):
        for admin in self._db.query(Admin):
            self._db.delete(admin)
        self.admin = None

        # Creating an admin that's not a system admin will fail.
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("email", "admin@nypl.org"),
                ("password", "pass"),
                ("roles", json.dumps([{ "role": AdminRole.LIBRARY_MANAGER, "library": self._default_library.short_name }])),
            ])
            assert_raises(AdminNotAuthorized, self.manager.admin_settings_controller.individual_admins)
            self._db.rollback()

        # But creating a system admin works.
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("email", "admin@nypl.org"),
                ("password", "pass"),
                ("roles", json.dumps([{ "role": AdminRole.SYSTEM_ADMIN }])),
            ])
            response = self.manager.admin_settings_controller.individual_admins()
            eq_(201, response.status_code)

        # The admin was created.
        admin_match = Admin.authenticate(self._db, "admin@nypl.org", "pass")
        eq_(admin_match.email, response.response[0])
        assert admin_match
        assert admin_match.has_password("pass")

        [role] = admin_match.roles
        eq_(AdminRole.SYSTEM_ADMIN, role.role)

    def test_patron_auth_services_get_with_no_services(self):
        with self.request_context_with_admin("/"):
            response = self.manager.admin_settings_controller.patron_auth_services()
            eq_(response.get("patron_auth_services"), [])
            protocols = response.get("protocols")
            eq_(5, len(protocols))
            eq_(SimpleAuthenticationProvider.__module__, protocols[0].get("name"))
            assert "settings" in protocols[0]
            assert "library_settings" in protocols[0]

            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            self._db.flush()
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_settings_controller.patron_auth_services)

    def test_patron_auth_services_get_with_simple_auth_service(self):
        auth_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=SimpleAuthenticationProvider.__module__,
            goal=ExternalIntegration.PATRON_AUTH_GOAL,
            name="name",
        )
        auth_service.setting(BasicAuthenticationProvider.TEST_IDENTIFIER).value = "user"
        auth_service.setting(BasicAuthenticationProvider.TEST_PASSWORD).value = "pass"

        with self.request_context_with_admin("/"):
            response = self.manager.admin_settings_controller.patron_auth_services()
            [service] = response.get("patron_auth_services")

            eq_(auth_service.id, service.get("id"))
            eq_(auth_service.name, service.get("name"))
            eq_(SimpleAuthenticationProvider.__module__, service.get("protocol"))
            eq_("user", service.get("settings").get(BasicAuthenticationProvider.TEST_IDENTIFIER))
            eq_("pass", service.get("settings").get(BasicAuthenticationProvider.TEST_PASSWORD))
            eq_([], service.get("libraries"))

        auth_service.libraries += [self._default_library]
        with self.request_context_with_admin("/"):
            response = self.manager.admin_settings_controller.patron_auth_services()
            [service] = response.get("patron_auth_services")

            eq_("user", service.get("settings").get(BasicAuthenticationProvider.TEST_IDENTIFIER))
            [library] = service.get("libraries")
            eq_(self._default_library.short_name, library.get("short_name"))
            eq_(None, library.get(AuthenticationProvider.EXTERNAL_TYPE_REGULAR_EXPRESSION))

        ConfigurationSetting.for_library_and_externalintegration(
            self._db, AuthenticationProvider.EXTERNAL_TYPE_REGULAR_EXPRESSION,
            self._default_library, auth_service,
        ).value = "^(u)"
        with self.request_context_with_admin("/"):
            response = self.manager.admin_settings_controller.patron_auth_services()
            [service] = response.get("patron_auth_services")

            [library] = service.get("libraries")
            eq_(self._default_library.short_name, library.get("short_name"))
            eq_("^(u)", library.get(AuthenticationProvider.EXTERNAL_TYPE_REGULAR_EXPRESSION))

    def test_patron_auth_services_get_with_millenium_auth_service(self):
        auth_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=MilleniumPatronAPI.__module__,
            goal=ExternalIntegration.PATRON_AUTH_GOAL
        )
        auth_service.setting(BasicAuthenticationProvider.TEST_IDENTIFIER).value = "user"
        auth_service.setting(BasicAuthenticationProvider.TEST_PASSWORD).value = "pass"
        auth_service.setting(BasicAuthenticationProvider.IDENTIFIER_REGULAR_EXPRESSION).value = "u*"
        auth_service.setting(BasicAuthenticationProvider.PASSWORD_REGULAR_EXPRESSION).value = "p*"
        auth_service.libraries += [self._default_library]
        ConfigurationSetting.for_library_and_externalintegration(
            self._db, AuthenticationProvider.EXTERNAL_TYPE_REGULAR_EXPRESSION,
            self._default_library, auth_service,
        ).value = "^(u)"

        with self.request_context_with_admin("/"):
            response = self.manager.admin_settings_controller.patron_auth_services()
            [service] = response.get("patron_auth_services")

            eq_(auth_service.id, service.get("id"))
            eq_(MilleniumPatronAPI.__module__, service.get("protocol"))
            eq_("user", service.get("settings").get(BasicAuthenticationProvider.TEST_IDENTIFIER))
            eq_("pass", service.get("settings").get(BasicAuthenticationProvider.TEST_PASSWORD))
            eq_("u*", service.get("settings").get(BasicAuthenticationProvider.IDENTIFIER_REGULAR_EXPRESSION))
            eq_("p*", service.get("settings").get(BasicAuthenticationProvider.PASSWORD_REGULAR_EXPRESSION))
            [library] = service.get("libraries")
            eq_(self._default_library.short_name, library.get("short_name"))
            eq_("^(u)", library.get(AuthenticationProvider.EXTERNAL_TYPE_REGULAR_EXPRESSION))

    def test_patron_auth_services_get_with_sip2_auth_service(self):
        auth_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=SIP2AuthenticationProvider.__module__,
            goal=ExternalIntegration.PATRON_AUTH_GOAL
        )
        auth_service.url = "url"
        auth_service.setting(SIP2AuthenticationProvider.PORT).value = "1234"
        auth_service.username = "user"
        auth_service.password = "pass"
        auth_service.setting(SIP2AuthenticationProvider.LOCATION_CODE).value = "5"
        auth_service.setting(SIP2AuthenticationProvider.FIELD_SEPARATOR).value = ","

        auth_service.libraries += [self._default_library]
        ConfigurationSetting.for_library_and_externalintegration(
            self._db, AuthenticationProvider.EXTERNAL_TYPE_REGULAR_EXPRESSION,
            self._default_library, auth_service,
        ).value = "^(u)"

        with self.request_context_with_admin("/"):
            response = self.manager.admin_settings_controller.patron_auth_services()
            [service] = response.get("patron_auth_services")

            eq_(auth_service.id, service.get("id"))
            eq_(SIP2AuthenticationProvider.__module__, service.get("protocol"))
            eq_("url", service.get("settings").get(ExternalIntegration.URL))
            eq_("1234", service.get("settings").get(SIP2AuthenticationProvider.PORT))
            eq_("user", service.get("settings").get(ExternalIntegration.USERNAME))
            eq_("pass", service.get("settings").get(ExternalIntegration.PASSWORD))
            eq_("5", service.get("settings").get(SIP2AuthenticationProvider.LOCATION_CODE))
            eq_(",", service.get("settings").get(SIP2AuthenticationProvider.FIELD_SEPARATOR))
            [library] = service.get("libraries")
            eq_(self._default_library.short_name, library.get("short_name"))
            eq_("^(u)", library.get(AuthenticationProvider.EXTERNAL_TYPE_REGULAR_EXPRESSION))

    def test_patron_auth_services_get_with_firstbook_auth_service(self):
        auth_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=FirstBookAuthenticationAPI.__module__,
            goal=ExternalIntegration.PATRON_AUTH_GOAL
        )
        auth_service.url = "url"
        auth_service.password = "pass"
        auth_service.libraries += [self._default_library]
        ConfigurationSetting.for_library_and_externalintegration(
            self._db, AuthenticationProvider.EXTERNAL_TYPE_REGULAR_EXPRESSION,
            self._default_library, auth_service,
        ).value = "^(u)"

        with self.request_context_with_admin("/"):
            response = self.manager.admin_settings_controller.patron_auth_services()
            [service] = response.get("patron_auth_services")

            eq_(auth_service.id, service.get("id"))
            eq_(FirstBookAuthenticationAPI.__module__, service.get("protocol"))
            eq_("url", service.get("settings").get(ExternalIntegration.URL))
            eq_("pass", service.get("settings").get(ExternalIntegration.PASSWORD))
            [library] = service.get("libraries")
            eq_(self._default_library.short_name, library.get("short_name"))
            eq_("^(u)", library.get(AuthenticationProvider.EXTERNAL_TYPE_REGULAR_EXPRESSION))

    def test_patron_auth_services_get_with_clever_auth_service(self):
        auth_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=CleverAuthenticationAPI.__module__,
            goal=ExternalIntegration.PATRON_AUTH_GOAL
        )
        auth_service.username = "user"
        auth_service.password = "pass"
        auth_service.libraries += [self._default_library]

        with self.request_context_with_admin("/"):
            response = self.manager.admin_settings_controller.patron_auth_services()
            [service] = response.get("patron_auth_services")

            eq_(auth_service.id, service.get("id"))
            eq_(CleverAuthenticationAPI.__module__, service.get("protocol"))
            eq_("user", service.get("settings").get(ExternalIntegration.USERNAME))
            eq_("pass", service.get("settings").get(ExternalIntegration.PASSWORD))
            [library] = service.get("libraries")
            eq_(self._default_library.short_name, library.get("short_name"))

    def _common_basic_auth_arguments(self):
        """We're not really testing these arguments, but a value for them
        is required for all Basic Auth type integrations.
        """
        B = BasicAuthenticationProvider
        return [
            (B.TEST_IDENTIFIER, "user"),
            (B.TEST_PASSWORD, "pass"),
            (B.IDENTIFIER_KEYBOARD, B.DEFAULT_KEYBOARD),
            (B.PASSWORD_KEYBOARD, B.DEFAULT_KEYBOARD),
        ]

    def test_patron_auth_services_post_errors(self):
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", "Unknown"),
            ])
            response = self.manager.admin_settings_controller.patron_auth_services()
            eq_(response, UNKNOWN_PROTOCOL)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([])
            response = self.manager.admin_settings_controller.patron_auth_services()
            eq_(response, NO_PROTOCOL_FOR_NEW_SERVICE)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", "123"),
            ])
            response = self.manager.admin_settings_controller.patron_auth_services()
            eq_(response, MISSING_SERVICE)

        auth_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=SimpleAuthenticationProvider.__module__,
            goal=ExternalIntegration.PATRON_AUTH_GOAL,
            name="name",
        )

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", auth_service.id),
                ("protocol", SIP2AuthenticationProvider.__module__),
            ])
            response = self.manager.admin_settings_controller.patron_auth_services()
            eq_(response, CANNOT_CHANGE_PROTOCOL)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", auth_service.name),
                ("protocol", SIP2AuthenticationProvider.__module__),
            ])
            response = self.manager.admin_settings_controller.patron_auth_services()
            eq_(response, INTEGRATION_NAME_ALREADY_IN_USE)

        auth_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=MilleniumPatronAPI.__module__,
            goal=ExternalIntegration.PATRON_AUTH_GOAL
        )

        common_args = self._common_basic_auth_arguments()
        with self.request_context_with_admin("/", method="POST"):
            M = MilleniumPatronAPI
            flask.request.form = MultiDict([
                ("id", auth_service.id),
                ("protocol", MilleniumPatronAPI.__module__),
                (ExternalIntegration.URL, "url"),
                (M.AUTHENTICATION_MODE, "Invalid mode"),
                (M.VERIFY_CERTIFICATE, "true"),
            ] + common_args)
            response = self.manager.admin_settings_controller.patron_auth_services()
            eq_(response.uri, INVALID_CONFIGURATION_OPTION.uri)

        auth_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=SimpleAuthenticationProvider.__module__,
            goal=ExternalIntegration.PATRON_AUTH_GOAL
        )

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", auth_service.id),
                ("protocol", SimpleAuthenticationProvider.__module__),
            ])
            response = self.manager.admin_settings_controller.patron_auth_services()
            eq_(response.uri, INCOMPLETE_CONFIGURATION.uri)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", SimpleAuthenticationProvider.__module__),
                ("libraries", json.dumps([{ "short_name": "not-a-library" }])),
            ] + common_args)
            response = self.manager.admin_settings_controller.patron_auth_services()
            eq_(response.uri, NO_SUCH_LIBRARY.uri)

        library, ignore = create(
            self._db, Library, name="Library", short_name="L",
        )
        auth_service.libraries += [library]

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", SimpleAuthenticationProvider.__module__),
                ("libraries", json.dumps([{
                    "short_name": library.short_name,
                    AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE: AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_NONE,
                    AuthenticationProvider.LIBRARY_IDENTIFIER_FIELD: AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_BARCODE,
                }])),
            ] + common_args)
            response = self.manager.admin_settings_controller.patron_auth_services()
            eq_(response.uri, MULTIPLE_BASIC_AUTH_SERVICES.uri)

        self._db.delete(auth_service)
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", SimpleAuthenticationProvider.__module__),
                ("libraries", json.dumps([{
                    "short_name": library.short_name,
                    AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE: AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_NONE,
                    AuthenticationProvider.LIBRARY_IDENTIFIER_FIELD: AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_BARCODE,
                    AuthenticationProvider.EXTERNAL_TYPE_REGULAR_EXPRESSION: "(invalid re",
                }])),
            ] + common_args)
            response = self.manager.admin_settings_controller.patron_auth_services()
            eq_(response, INVALID_EXTERNAL_TYPE_REGULAR_EXPRESSION)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", SimpleAuthenticationProvider.__module__),
                ("libraries", json.dumps([{
                    "short_name": library.short_name,
                    AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE: AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_REGEX,
                    AuthenticationProvider.LIBRARY_IDENTIFIER_FIELD: AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_BARCODE,
                    AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION: "(invalid re",
                }])),
            ] + common_args)
            response = self.manager.admin_settings_controller.patron_auth_services()
            eq_(response, INVALID_LIBRARY_IDENTIFIER_RESTRICTION_REGULAR_EXPRESSION)

        self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        self._db.flush()
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", SimpleAuthenticationProvider.__module__),
            ] + self._common_basic_auth_arguments())
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_settings_controller.patron_auth_services)

    def test_patron_auth_services_post_create(self):
        library, ignore = create(
            self._db, Library, name="Library", short_name="L",
        )
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", SimpleAuthenticationProvider.__module__),
                ("libraries", json.dumps([{
                    "short_name": library.short_name,
                    AuthenticationProvider.EXTERNAL_TYPE_REGULAR_EXPRESSION: "^(.)",
                    AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE: AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_REGEX,
                    AuthenticationProvider.LIBRARY_IDENTIFIER_FIELD: AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_BARCODE,
                    AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION: "^1234",
                }])),
            ] + self._common_basic_auth_arguments())
            response = self.manager.admin_settings_controller.patron_auth_services()
            eq_(response.status_code, 201)

        auth_service = get_one(self._db, ExternalIntegration, goal=ExternalIntegration.PATRON_AUTH_GOAL)
        eq_(auth_service.id, int(response.response[0]))
        eq_(SimpleAuthenticationProvider.__module__, auth_service.protocol)
        eq_("user", auth_service.setting(BasicAuthenticationProvider.TEST_IDENTIFIER).value)
        eq_("pass", auth_service.setting(BasicAuthenticationProvider.TEST_PASSWORD).value)
        eq_([library], auth_service.libraries)
        eq_("^(.)", ConfigurationSetting.for_library_and_externalintegration(
                self._db, AuthenticationProvider.EXTERNAL_TYPE_REGULAR_EXPRESSION,
                library, auth_service).value)
        common_args = self._common_basic_auth_arguments()
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", MilleniumPatronAPI.__module__),
                (ExternalIntegration.URL, "url"),
                (MilleniumPatronAPI.VERIFY_CERTIFICATE, "true"),
                (MilleniumPatronAPI.AUTHENTICATION_MODE, MilleniumPatronAPI.PIN_AUTHENTICATION_MODE),
            ] + common_args)
            response = self.manager.admin_settings_controller.patron_auth_services()
            eq_(response.status_code, 201)

        auth_service2 = get_one(self._db, ExternalIntegration,
                               goal=ExternalIntegration.PATRON_AUTH_GOAL,
                               protocol=MilleniumPatronAPI.__module__)
        assert auth_service2 != auth_service
        eq_(auth_service2.id, int(response.response[0]))
        eq_("url", auth_service2.url)
        eq_("user", auth_service2.setting(BasicAuthenticationProvider.TEST_IDENTIFIER).value)
        eq_("pass", auth_service2.setting(BasicAuthenticationProvider.TEST_PASSWORD).value)
        eq_("true",
            auth_service2.setting(MilleniumPatronAPI.VERIFY_CERTIFICATE).value)
        eq_(MilleniumPatronAPI.PIN_AUTHENTICATION_MODE,
            auth_service2.setting(MilleniumPatronAPI.AUTHENTICATION_MODE).value)
        eq_(None, auth_service2.setting(MilleniumPatronAPI.BLOCK_TYPES).value)
        eq_([], auth_service2.libraries)

    def test_patron_auth_services_post_edit(self):
        l1, ignore = create(
            self._db, Library, name="Library 1", short_name="L1",
        )
        l2, ignore = create(
            self._db, Library, name="Library 2", short_name="L2",
        )

        auth_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=SimpleAuthenticationProvider.__module__,
            goal=ExternalIntegration.PATRON_AUTH_GOAL
        )
        auth_service.setting(BasicAuthenticationProvider.TEST_IDENTIFIER).value = "old_user"
        auth_service.setting(BasicAuthenticationProvider.TEST_PASSWORD).value = "old_password"
        auth_service.libraries = [l1]

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", auth_service.id),
                ("protocol", SimpleAuthenticationProvider.__module__),
                ("libraries", json.dumps([{
                    "short_name": l2.short_name,
                    AuthenticationProvider.EXTERNAL_TYPE_REGULAR_EXPRESSION: "^(.)",
                    AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE: AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_NONE,
                    AuthenticationProvider.LIBRARY_IDENTIFIER_FIELD: AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_BARCODE,
                }])),
            ] + self._common_basic_auth_arguments())
            response = self.manager.admin_settings_controller.patron_auth_services()
            eq_(response.status_code, 200)

        eq_(auth_service.id, int(response.response[0]))
        eq_(SimpleAuthenticationProvider.__module__, auth_service.protocol)
        eq_("user", auth_service.setting(BasicAuthenticationProvider.TEST_IDENTIFIER).value)
        eq_("pass", auth_service.setting(BasicAuthenticationProvider.TEST_PASSWORD).value)
        eq_([l2], auth_service.libraries)
        eq_("^(.)", ConfigurationSetting.for_library_and_externalintegration(
                self._db, AuthenticationProvider.EXTERNAL_TYPE_REGULAR_EXPRESSION,
                l2, auth_service).value)

    def test_patron_auth_service_delete(self):
        l1, ignore = create(
            self._db, Library, name="Library 1", short_name="L1",
        )
        auth_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=SimpleAuthenticationProvider.__module__,
            goal=ExternalIntegration.PATRON_AUTH_GOAL
        )
        auth_service.setting(BasicAuthenticationProvider.TEST_IDENTIFIER).value = "old_user"
        auth_service.setting(BasicAuthenticationProvider.TEST_PASSWORD).value = "old_password"
        auth_service.libraries = [l1]

        with self.request_context_with_admin("/", method="DELETE"):
            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_settings_controller.patron_auth_service,
                          auth_service.id)

            self.admin.add_role(AdminRole.SYSTEM_ADMIN)
            response = self.manager.admin_settings_controller.patron_auth_service(auth_service.id)
            eq_(response.status_code, 200)

        service = get_one(self._db, ExternalIntegration, id=auth_service.id)
        eq_(None, service)

    def test_sitewide_settings_get(self):
        with self.request_context_with_admin("/"):
            response = self.manager.admin_settings_controller.sitewide_settings()
            settings = response.get("settings")
            all_settings = response.get("all_settings")

            eq_([], settings)
            keys = [s.get("key") for s in all_settings]
            assert AcquisitionFeed.GROUPED_MAX_AGE_POLICY in keys
            assert AcquisitionFeed.NONGROUPED_MAX_AGE_POLICY in keys
            assert Configuration.SECRET_KEY in keys

        ConfigurationSetting.sitewide(self._db, AcquisitionFeed.GROUPED_MAX_AGE_POLICY).value = 0
        ConfigurationSetting.sitewide(self._db, Configuration.SECRET_KEY).value = "secret"
        self._db.flush()

        with self.request_context_with_admin("/"):
            response = self.manager.admin_settings_controller.sitewide_settings()
            settings = response.get("settings")
            all_settings = response.get("all_settings")

            eq_(2, len(settings))
            settings_by_key = { s.get("key") : s.get("value") for s in settings }
            eq_("0", settings_by_key.get(AcquisitionFeed.GROUPED_MAX_AGE_POLICY))
            eq_("secret", settings_by_key.get(Configuration.SECRET_KEY))
            keys = [s.get("key") for s in all_settings]
            assert AcquisitionFeed.GROUPED_MAX_AGE_POLICY in keys
            assert AcquisitionFeed.NONGROUPED_MAX_AGE_POLICY in keys
            assert Configuration.SECRET_KEY in keys

            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            self._db.flush()
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_settings_controller.sitewide_settings)

    def test_sitewide_settings_post_errors(self):
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([])
            response = self.manager.admin_settings_controller.sitewide_settings()
            eq_(response, MISSING_SITEWIDE_SETTING_KEY)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("key", Configuration.SECRET_KEY),
            ])
            response = self.manager.admin_settings_controller.sitewide_settings()
            eq_(response, MISSING_SITEWIDE_SETTING_VALUE)

        self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("key", Configuration.SECRET_KEY),
                ("value", "secret"),
            ])
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_settings_controller.sitewide_settings)

    def test_sitewide_settings_post_create(self):
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("key", AcquisitionFeed.GROUPED_MAX_AGE_POLICY),
                ("value", "10"),
            ])
            response = self.manager.admin_settings_controller.sitewide_settings()
            eq_(response.status_code, 200)

        # The setting was created.
        setting = ConfigurationSetting.sitewide(self._db, AcquisitionFeed.GROUPED_MAX_AGE_POLICY)
        eq_(setting.key, response.response[0])
        eq_("10", setting.value)

    def test_sitewide_settings_post_edit(self):
        setting = ConfigurationSetting.sitewide(self._db, AcquisitionFeed.GROUPED_MAX_AGE_POLICY)
        setting.value = "10"

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("key", AcquisitionFeed.GROUPED_MAX_AGE_POLICY),
                ("value", "20"),
            ])
            response = self.manager.admin_settings_controller.sitewide_settings()
            eq_(response.status_code, 200)

        # The setting was changed.
        eq_(setting.key, response.response[0])
        eq_("20", setting.value)

    def test_sitewide_setting_delete(self):
        setting = ConfigurationSetting.sitewide(self._db, AcquisitionFeed.GROUPED_MAX_AGE_POLICY)
        setting.value = "10"

        with self.request_context_with_admin("/", method="DELETE"):
            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_settings_controller.sitewide_setting,
                          setting.key)

            self.admin.add_role(AdminRole.SYSTEM_ADMIN)
            response = self.manager.admin_settings_controller.sitewide_setting(setting.key)
            eq_(response.status_code, 200)

        eq_(None, setting.value)

    def test_metadata_services_get_with_no_services(self):
        with self.request_context_with_admin("/"):
            response = self.manager.admin_settings_controller.metadata_services()
            eq_(response.get("metadata_services"), [])
            protocols = response.get("protocols")
            assert NoveListAPI.NAME in [p.get("label") for p in protocols]
            assert "settings" in protocols[0]

            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            self._db.flush()
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_settings_controller.metadata_services)

    def test_metadata_services_get_with_one_service(self):
        novelist_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.NOVELIST,
            goal=ExternalIntegration.METADATA_GOAL,
        )
        novelist_service.username = "user"
        novelist_service.password = "pass"

        with self.request_context_with_admin("/"):
            response = self.manager.admin_settings_controller.metadata_services()
            [service] = response.get("metadata_services")

            eq_(novelist_service.id, service.get("id"))
            eq_(ExternalIntegration.NOVELIST, service.get("protocol"))
            eq_("user", service.get("settings").get(ExternalIntegration.USERNAME))
            eq_("pass", service.get("settings").get(ExternalIntegration.PASSWORD))

        novelist_service.libraries += [self._default_library]
        with self.request_context_with_admin("/"):
            response = self.manager.admin_settings_controller.metadata_services()
            [service] = response.get("metadata_services")

            eq_("user", service.get("settings").get(ExternalIntegration.USERNAME))
            [library] = service.get("libraries")
            eq_(self._default_library.short_name, library.get("short_name"))

    def test_metadata_services_post_errors(self):
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", "Unknown"),
            ])
            response = self.manager.admin_settings_controller.metadata_services()
            eq_(response, UNKNOWN_PROTOCOL)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([])
            response = self.manager.admin_settings_controller.metadata_services()
            eq_(response, NO_PROTOCOL_FOR_NEW_SERVICE)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", "123"),
            ])
            response = self.manager.admin_settings_controller.metadata_services()
            eq_(response, MISSING_SERVICE)

        service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.NOVELIST,
            goal=ExternalIntegration.METADATA_GOAL,
            name="name",
        )

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", service.name),
                ("protocol", ExternalIntegration.NYT),
            ])
            response = self.manager.admin_settings_controller.metadata_services()
            eq_(response, INTEGRATION_NAME_ALREADY_IN_USE)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", service.id),
                ("protocol", ExternalIntegration.NYT),
            ])
            response = self.manager.admin_settings_controller.metadata_services()
            eq_(response, CANNOT_CHANGE_PROTOCOL)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", service.id),
                ("protocol", ExternalIntegration.NOVELIST),
            ])
            response = self.manager.admin_settings_controller.metadata_services()
            eq_(response.uri, INCOMPLETE_CONFIGURATION.uri)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", service.id),
                ("protocol", ExternalIntegration.NOVELIST),
                (ExternalIntegration.USERNAME, "user"),
                (ExternalIntegration.PASSWORD, "pass"),
                ("libraries", json.dumps([{"short_name": "not-a-library"}])),
            ])
            response = self.manager.admin_settings_controller.metadata_services()
            eq_(response.uri, NO_SUCH_LIBRARY.uri)

        self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", ExternalIntegration.NOVELIST),
                (ExternalIntegration.USERNAME, "user"),
                (ExternalIntegration.PASSWORD, "pass"),
                ("libraries", json.dumps([])),
            ])
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_settings_controller.metadata_services)

    def test_metadata_services_post_create(self):
        library, ignore = create(
            self._db, Library, name="Library", short_name="L",
        )
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", ExternalIntegration.NOVELIST),
                (ExternalIntegration.USERNAME, "user"),
                (ExternalIntegration.PASSWORD, "pass"),
                ("libraries", json.dumps([{"short_name": "L"}])),
            ])
            response = self.manager.admin_settings_controller.metadata_services()
            eq_(response.status_code, 201)

        service = get_one(self._db, ExternalIntegration, goal=ExternalIntegration.METADATA_GOAL)
        eq_(service.id, int(response.response[0]))
        eq_(ExternalIntegration.NOVELIST, service.protocol)
        eq_("user", service.username)
        eq_("pass", service.password)
        eq_([library], service.libraries)

    def test_metadata_services_post_edit(self):
        l1, ignore = create(
            self._db, Library, name="Library 1", short_name="L1",
        )
        l2, ignore = create(
            self._db, Library, name="Library 2", short_name="L2",
        )

        novelist_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.NOVELIST,
            goal=ExternalIntegration.METADATA_GOAL,
        )
        novelist_service.username = "olduser"
        novelist_service.password = "oldpass"
        novelist_service.libraries = [l1]

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", novelist_service.id),
                ("protocol", ExternalIntegration.NOVELIST),
                (ExternalIntegration.USERNAME, "user"),
                (ExternalIntegration.PASSWORD, "pass"),
                ("libraries", json.dumps([{"short_name": "L2"}])),
            ])
            response = self.manager.admin_settings_controller.metadata_services()
            eq_(response.status_code, 200)

        eq_(novelist_service.id, int(response.response[0]))
        eq_(ExternalIntegration.NOVELIST, novelist_service.protocol)
        eq_("user", novelist_service.username)
        eq_("pass", novelist_service.password)
        eq_([l2], novelist_service.libraries)

    def test_metadata_service_delete(self):
        l1, ignore = create(
            self._db, Library, name="Library 1", short_name="L1",
        )
        novelist_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.NOVELIST,
            goal=ExternalIntegration.METADATA_GOAL,
        )
        novelist_service.username = "olduser"
        novelist_service.password = "oldpass"
        novelist_service.libraries = [l1]

        with self.request_context_with_admin("/", method="DELETE"):
            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_settings_controller.metadata_service,
                          novelist_service.id)

            self.admin.add_role(AdminRole.SYSTEM_ADMIN)
            response = self.manager.admin_settings_controller.metadata_service(novelist_service.id)
            eq_(response.status_code, 200)

        service = get_one(self._db, ExternalIntegration, id=novelist_service.id)
        eq_(None, service)

    def test_analytics_services_get_with_no_services(self):
        with self.request_context_with_admin("/"):
            response = self.manager.admin_settings_controller.analytics_services()
            eq_(response.get("analytics_services"), [])
            protocols = response.get("protocols")
            assert GoogleAnalyticsProvider.NAME in [p.get("label") for p in protocols]
            assert "settings" in protocols[0]

            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            self._db.flush()
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_settings_controller.analytics_services)

    def test_analytics_services_get_with_one_service(self):
        ga_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=GoogleAnalyticsProvider.__module__,
            goal=ExternalIntegration.ANALYTICS_GOAL,
        )
        ga_service.url = self._str

        with self.request_context_with_admin("/"):
            response = self.manager.admin_settings_controller.analytics_services()
            [service] = response.get("analytics_services")

            eq_(ga_service.id, service.get("id"))
            eq_(ga_service.protocol, service.get("protocol"))
            eq_(ga_service.url, service.get("settings").get(ExternalIntegration.URL))

        ga_service.libraries += [self._default_library]
        ConfigurationSetting.for_library_and_externalintegration(
            self._db, GoogleAnalyticsProvider.TRACKING_ID, self._default_library, ga_service
        ).value = "trackingid"
        with self.request_context_with_admin("/"):
            response = self.manager.admin_settings_controller.analytics_services()
            [service] = response.get("analytics_services")

            [library] = service.get("libraries")
            eq_(self._default_library.short_name, library.get("short_name"))
            eq_("trackingid", library.get(GoogleAnalyticsProvider.TRACKING_ID))

        self._db.delete(ga_service)

        local_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=LocalAnalyticsProvider.__module__,
            goal=ExternalIntegration.ANALYTICS_GOAL,
        )

        local_service.libraries += [self._default_library]
        with self.request_context_with_admin("/"):
            response = self.manager.admin_settings_controller.analytics_services()
            [service] = response.get("analytics_services")

            eq_(local_service.id, service.get("id"))
            eq_(local_service.protocol, service.get("protocol"))
            [library] = service.get("libraries")
            eq_(self._default_library.short_name, library.get("short_name"))

    def test_analytics_services_post_errors(self):
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", "Unknown"),
            ])
            response = self.manager.admin_settings_controller.analytics_services()
            eq_(response, UNKNOWN_PROTOCOL)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([])
            response = self.manager.admin_settings_controller.analytics_services()
            eq_(response, NO_PROTOCOL_FOR_NEW_SERVICE)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", "123"),
            ])
            response = self.manager.admin_settings_controller.analytics_services()
            eq_(response, MISSING_SERVICE)

        service, ignore = create(
            self._db, ExternalIntegration,
            protocol=GoogleAnalyticsProvider.__module__,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            name="name",
        )

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", service.name),
                ("protocol", GoogleAnalyticsProvider.__module__),
            ])
            response = self.manager.admin_settings_controller.analytics_services()
            eq_(response, INTEGRATION_NAME_ALREADY_IN_USE)

        service, ignore = create(
            self._db, ExternalIntegration,
            protocol=GoogleAnalyticsProvider.__module__,
            goal=ExternalIntegration.ANALYTICS_GOAL,
        )

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", service.id),
                ("protocol", "core.local_analytics_provider"),
            ])
            response = self.manager.admin_settings_controller.analytics_services()
            eq_(response, CANNOT_CHANGE_PROTOCOL)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", service.id),
                ("protocol", GoogleAnalyticsProvider.__module__),
            ])
            response = self.manager.admin_settings_controller.analytics_services()
            eq_(response.uri, INCOMPLETE_CONFIGURATION.uri)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", service.id),
                ("protocol", GoogleAnalyticsProvider.__module__),
                (ExternalIntegration.URL, "url"),
                ("libraries", json.dumps([{"short_name": "not-a-library"}])),
            ])
            response = self.manager.admin_settings_controller.analytics_services()
            eq_(response.uri, NO_SUCH_LIBRARY.uri)

        library, ignore = create(
            self._db, Library, name="Library", short_name="L",
        )

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", service.id),
                ("protocol", GoogleAnalyticsProvider.__module__),
                (ExternalIntegration.URL, "url"),
                ("libraries", json.dumps([{"short_name": library.short_name}])),
            ])
            response = self.manager.admin_settings_controller.analytics_services()
            eq_(response.uri, INCOMPLETE_CONFIGURATION.uri)

        self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", GoogleAnalyticsProvider.__module__),
                (ExternalIntegration.URL, "url"),
                ("libraries", json.dumps([])),
            ])
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_settings_controller.analytics_services)

    def test_analytics_services_post_create(self):
        library, ignore = create(
            self._db, Library, name="Library", short_name="L",
        )
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", GoogleAnalyticsProvider.__module__),
                (ExternalIntegration.URL, "url"),
                ("libraries", json.dumps([{"short_name": "L", "tracking_id": "trackingid"}])),
            ])
            response = self.manager.admin_settings_controller.analytics_services()
            eq_(response.status_code, 201)

        service = get_one(self._db, ExternalIntegration, goal=ExternalIntegration.ANALYTICS_GOAL)
        eq_(service.id, int(response.response[0]))
        eq_(GoogleAnalyticsProvider.__module__, service.protocol)
        eq_("url", service.url)
        eq_([library], service.libraries)
        eq_("trackingid", ConfigurationSetting.for_library_and_externalintegration(
                self._db, GoogleAnalyticsProvider.TRACKING_ID, library, service).value)


    def test_analytics_services_post_edit(self):
        l1, ignore = create(
            self._db, Library, name="Library 1", short_name="L1",
        )
        l2, ignore = create(
            self._db, Library, name="Library 2", short_name="L2",
        )

        ga_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=GoogleAnalyticsProvider.__module__,
            goal=ExternalIntegration.ANALYTICS_GOAL,
        )
        ga_service.url = "oldurl"
        ga_service.libraries = [l1]

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", ga_service.id),
                ("protocol", GoogleAnalyticsProvider.__module__),
                (ExternalIntegration.URL, "url"),
                ("libraries", json.dumps([{"short_name": "L2", "tracking_id": "l2id"}])),
            ])
            response = self.manager.admin_settings_controller.analytics_services()
            eq_(response.status_code, 200)

        eq_(ga_service.id, int(response.response[0]))
        eq_(GoogleAnalyticsProvider.__module__, ga_service.protocol)
        eq_("url", ga_service.url)
        eq_([l2], ga_service.libraries)
        eq_("l2id", ConfigurationSetting.for_library_and_externalintegration(
                self._db, GoogleAnalyticsProvider.TRACKING_ID, l2, ga_service).value)

    def test_analytics_service_delete(self):
        l1, ignore = create(
            self._db, Library, name="Library 1", short_name="L1",
        )
        ga_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=GoogleAnalyticsProvider.__module__,
            goal=ExternalIntegration.ANALYTICS_GOAL,
        )
        ga_service.url = "oldurl"
        ga_service.libraries = [l1]

        with self.request_context_with_admin("/", method="DELETE"):
            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_settings_controller.analytics_service,
                          ga_service.id)

            self.admin.add_role(AdminRole.SYSTEM_ADMIN)
            response = self.manager.admin_settings_controller.analytics_service(ga_service.id)
            eq_(response.status_code, 200)

        service = get_one(self._db, ExternalIntegration, id=ga_service.id)
        eq_(None, service)

    def test_cdn_services_get_with_no_services(self):
        with self.request_context_with_admin("/"):
            response = self.manager.admin_settings_controller.cdn_services()
            eq_(response.get("cdn_services"), [])
            protocols = response.get("protocols")
            assert ExternalIntegration.CDN in [p.get("name") for p in protocols]
            assert "settings" in protocols[0]

            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            self._db.flush()
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_settings_controller.cdn_services)

    def test_cdn_services_get_with_one_service(self):
        cdn_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.CDN,
            goal=ExternalIntegration.CDN_GOAL,
        )
        cdn_service.url = "cdn url"
        cdn_service.setting(Configuration.CDN_MIRRORED_DOMAIN_KEY).value = "mirrored domain"

        with self.request_context_with_admin("/"):
            response = self.manager.admin_settings_controller.cdn_services()
            [service] = response.get("cdn_services")

            eq_(cdn_service.id, service.get("id"))
            eq_(cdn_service.protocol, service.get("protocol"))
            eq_("cdn url", service.get("settings").get(ExternalIntegration.URL))
            eq_("mirrored domain", service.get("settings").get(Configuration.CDN_MIRRORED_DOMAIN_KEY))

    def test_cdn_services_post_errors(self):
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", "Unknown"),
            ])
            response = self.manager.admin_settings_controller.cdn_services()
            eq_(response, UNKNOWN_PROTOCOL)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([])
            response = self.manager.admin_settings_controller.cdn_services()
            eq_(response, NO_PROTOCOL_FOR_NEW_SERVICE)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", "123"),
            ])
            response = self.manager.admin_settings_controller.cdn_services()
            eq_(response, MISSING_SERVICE)

        service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.CDN,
            goal=ExternalIntegration.CDN_GOAL,
            name="name",
        )

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", service.name),
                ("protocol", ExternalIntegration.CDN),
            ])
            response = self.manager.admin_settings_controller.cdn_services()
            eq_(response, INTEGRATION_NAME_ALREADY_IN_USE)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", service.id),
                ("protocol", ExternalIntegration.CDN),
            ])
            response = self.manager.admin_settings_controller.cdn_services()
            eq_(response.uri, INCOMPLETE_CONFIGURATION.uri)

        self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", ExternalIntegration.CDN),
                (ExternalIntegration.URL, "cdn url"),
                (Configuration.CDN_MIRRORED_DOMAIN_KEY, "mirrored domain"),
            ])
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_settings_controller.cdn_services)

    def test_cdn_services_post_create(self):
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", ExternalIntegration.CDN),
                (ExternalIntegration.URL, "cdn url"),
                (Configuration.CDN_MIRRORED_DOMAIN_KEY, "mirrored domain"),
            ])
            response = self.manager.admin_settings_controller.cdn_services()
            eq_(response.status_code, 201)

        service = get_one(self._db, ExternalIntegration, goal=ExternalIntegration.CDN_GOAL)
        eq_(service.id, int(response.response[0]))
        eq_(ExternalIntegration.CDN, service.protocol)
        eq_("cdn url", service.url)
        eq_("mirrored domain", service.setting(Configuration.CDN_MIRRORED_DOMAIN_KEY).value)

    def test_cdn_services_post_edit(self):
        cdn_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.CDN,
            goal=ExternalIntegration.CDN_GOAL,
        )
        cdn_service.url = "cdn url"
        cdn_service.setting(Configuration.CDN_MIRRORED_DOMAIN_KEY).value = "mirrored domain"

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", cdn_service.id),
                ("protocol", ExternalIntegration.CDN),
                (ExternalIntegration.URL, "new cdn url"),
                (Configuration.CDN_MIRRORED_DOMAIN_KEY, "new mirrored domain")
            ])
            response = self.manager.admin_settings_controller.cdn_services()
            eq_(response.status_code, 200)

        eq_(cdn_service.id, int(response.response[0]))
        eq_(ExternalIntegration.CDN, cdn_service.protocol)
        eq_("new cdn url", cdn_service.url)
        eq_("new mirrored domain", cdn_service.setting(Configuration.CDN_MIRRORED_DOMAIN_KEY).value)

    def test_cdn_service_delete(self):
        cdn_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.CDN,
            goal=ExternalIntegration.CDN_GOAL,
        )
        cdn_service.url = "cdn url"
        cdn_service.setting(Configuration.CDN_MIRRORED_DOMAIN_KEY).value = "mirrored domain"

        with self.request_context_with_admin("/", method="DELETE"):
            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_settings_controller.cdn_service,
                          cdn_service.id)

            self.admin.add_role(AdminRole.SYSTEM_ADMIN)
            response = self.manager.admin_settings_controller.cdn_service(cdn_service.id)
            eq_(response.status_code, 200)

        service = get_one(self._db, ExternalIntegration, id=cdn_service.id)
        eq_(None, service)

    def test_search_services_get_with_no_services(self):
        with self.request_context_with_admin("/"):
            response = self.manager.admin_settings_controller.search_services()
            eq_(response.get("search_services"), [])
            protocols = response.get("protocols")
            assert ExternalIntegration.ELASTICSEARCH in [p.get("name") for p in protocols]
            assert "settings" in protocols[0]

            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            self._db.flush()
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_settings_controller.search_services)

    def test_search_services_get_with_one_service(self):
        search_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.ELASTICSEARCH,
            goal=ExternalIntegration.SEARCH_GOAL,
        )
        search_service.url = "search url"
        search_service.setting(ExternalSearchIndex.WORKS_INDEX_PREFIX_KEY).value = "works-index-prefix"

        with self.request_context_with_admin("/"):
            response = self.manager.admin_settings_controller.search_services()
            [service] = response.get("search_services")

            eq_(search_service.id, service.get("id"))
            eq_(search_service.protocol, service.get("protocol"))
            eq_("search url", service.get("settings").get(ExternalIntegration.URL))
            eq_("works-index-prefix", service.get("settings").get(ExternalSearchIndex.WORKS_INDEX_PREFIX_KEY))

    def test_search_services_post_errors(self):
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", "Unknown"),
            ])
            response = self.manager.admin_settings_controller.search_services()
            eq_(response, UNKNOWN_PROTOCOL)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([])
            response = self.manager.admin_settings_controller.search_services()
            eq_(response, NO_PROTOCOL_FOR_NEW_SERVICE)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", "123"),
            ])
            response = self.manager.admin_settings_controller.search_services()
            eq_(response, MISSING_SERVICE)

        service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.ELASTICSEARCH,
            goal=ExternalIntegration.SEARCH_GOAL,
        )

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", ExternalIntegration.ELASTICSEARCH),
            ])
            response = self.manager.admin_settings_controller.search_services()
            eq_(response.uri, MULTIPLE_SITEWIDE_SERVICES.uri)

        self._db.delete(service)
        service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.CDN,
            goal=ExternalIntegration.CDN_GOAL,
            name="name",
        )

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", service.name),
                ("protocol", ExternalIntegration.ELASTICSEARCH),
            ])
            response = self.manager.admin_settings_controller.search_services()
            eq_(response, INTEGRATION_NAME_ALREADY_IN_USE)

        service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.ELASTICSEARCH,
            goal=ExternalIntegration.SEARCH_GOAL,
        )

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", service.id),
                ("protocol", ExternalIntegration.ELASTICSEARCH),
            ])
            response = self.manager.admin_settings_controller.search_services()
            eq_(response.uri, INCOMPLETE_CONFIGURATION.uri)

        self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", ExternalIntegration.ELASTICSEARCH),
                (ExternalIntegration.URL, "search url"),
                (ExternalSearchIndex.WORKS_INDEX_PREFIX_KEY, "works-index-prefix"),
            ])
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_settings_controller.search_services)

    def test_search_services_post_create(self):
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", ExternalIntegration.ELASTICSEARCH),
                (ExternalIntegration.URL, "search url"),
                (ExternalSearchIndex.WORKS_INDEX_PREFIX_KEY, "works-index-prefix"),
            ])
            response = self.manager.admin_settings_controller.search_services()
            eq_(response.status_code, 201)

        service = get_one(self._db, ExternalIntegration, goal=ExternalIntegration.SEARCH_GOAL)
        eq_(service.id, int(response.response[0]))
        eq_(ExternalIntegration.ELASTICSEARCH, service.protocol)
        eq_("search url", service.url)
        eq_("works-index-prefix", service.setting(ExternalSearchIndex.WORKS_INDEX_PREFIX_KEY).value)

    def test_search_services_post_edit(self):
        search_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.ELASTICSEARCH,
            goal=ExternalIntegration.SEARCH_GOAL,
        )
        search_service.url = "search url"
        search_service.setting(ExternalSearchIndex.WORKS_INDEX_PREFIX_KEY).value = "works-index-prefix"

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", search_service.id),
                ("protocol", ExternalIntegration.ELASTICSEARCH),
                (ExternalIntegration.URL, "new search url"),
                (ExternalSearchIndex.WORKS_INDEX_PREFIX_KEY, "new-works-index-prefix")
            ])
            response = self.manager.admin_settings_controller.search_services()
            eq_(response.status_code, 200)

        eq_(search_service.id, int(response.response[0]))
        eq_(ExternalIntegration.ELASTICSEARCH, search_service.protocol)
        eq_("new search url", search_service.url)
        eq_("new-works-index-prefix", search_service.setting(ExternalSearchIndex.WORKS_INDEX_PREFIX_KEY).value)

    def test_search_service_delete(self):
        search_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.ELASTICSEARCH,
            goal=ExternalIntegration.SEARCH_GOAL,
        )
        search_service.url = "search url"
        search_service.setting(ExternalSearchIndex.WORKS_INDEX_PREFIX_KEY).value = "works-index-prefix"

        with self.request_context_with_admin("/", method="DELETE"):
            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_settings_controller.search_service,
                          search_service.id)

            self.admin.add_role(AdminRole.SYSTEM_ADMIN)
            response = self.manager.admin_settings_controller.search_service(search_service.id)
            eq_(response.status_code, 200)

        service = get_one(self._db, ExternalIntegration, id=search_service.id)
        eq_(None, service)

    def test_sitewide_service_management(self):
        """The configuration of storage and search collections is delegated to
        the _manage_sitewide_service and _delete_integration methods.

        Since search collections are tested above, this provides
        test coverage for storage collections.
        """
        class Mock(SettingsController):
            def _manage_sitewide_service(self,*args):
                self.manage_called_with = args

            def _delete_integration(self, *args):
                self.delete_called_with = args

        controller = Mock(self.manager)

        # Test storage services first.
        EI = ExternalIntegration
        with self.request_context_with_admin("/"):
            controller.storage_services()
            goal, apis, key_name, problem = controller.manage_called_with
            eq_(EI.STORAGE_GOAL, goal)
            assert S3Uploader in apis
            eq_('storage_services', key_name)
            assert 'new storage service' in problem

        with self.request_context_with_admin("/"):
            id = object()
            controller.storage_service(id)
            eq_((id, EI.STORAGE_GOAL), controller.delete_called_with)

        # Search services work the same way but pass in different
        # arguments.
        with self.request_context_with_admin("/"):
            controller.search_services()
            goal, apis, key_name, problem = controller.manage_called_with
            eq_(EI.SEARCH_GOAL, goal)
            assert ExternalSearchIndex in apis
            eq_('search_services', key_name)
            assert 'new search service' in problem

        with self.request_context_with_admin("/"):
            id = object()
            controller.search_service(id)
            eq_((id, EI.SEARCH_GOAL),
                controller.delete_called_with)

    def test_discovery_services_get_with_no_services_creates_default(self):
        with self.request_context_with_admin("/"):
            response = self.manager.admin_settings_controller.discovery_services()
            [service] = response.get("discovery_services")
            protocols = response.get("protocols")
            assert ExternalIntegration.OPDS_REGISTRATION in [p.get("name") for p in protocols]
            assert "settings" in protocols[0]
            eq_(ExternalIntegration.OPDS_REGISTRATION, service.get("protocol"))
            eq_("https://libraryregistry.librarysimplified.org", service.get("settings").get(ExternalIntegration.URL))

            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            self._db.flush()
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_settings_controller.discovery_services)

    def test_discovery_services_get_with_one_service(self):
        discovery_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.OPDS_REGISTRATION,
            goal=ExternalIntegration.DISCOVERY_GOAL,
        )
        discovery_service.url = self._str

        with self.request_context_with_admin("/"):
            response = self.manager.admin_settings_controller.discovery_services()
            [service] = response.get("discovery_services")

            eq_(discovery_service.id, service.get("id"))
            eq_(discovery_service.protocol, service.get("protocol"))
            eq_(discovery_service.url, service.get("settings").get(ExternalIntegration.URL))

    def test_discovery_services_post_errors(self):
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", "Unknown"),
            ])
            response = self.manager.admin_settings_controller.discovery_services()
            eq_(response, UNKNOWN_PROTOCOL)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([])
            response = self.manager.admin_settings_controller.discovery_services()
            eq_(response, NO_PROTOCOL_FOR_NEW_SERVICE)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", "123"),
            ])
            response = self.manager.admin_settings_controller.discovery_services()
            eq_(response, MISSING_SERVICE)

        service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.OPDS_REGISTRATION,
            goal=ExternalIntegration.DISCOVERY_GOAL,
            name="name",
        )

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", service.name),
                ("protocol", ExternalIntegration.OPDS_REGISTRATION),
            ])
            response = self.manager.admin_settings_controller.discovery_services()
            eq_(response, INTEGRATION_NAME_ALREADY_IN_USE)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", service.id),
                ("protocol", ExternalIntegration.OPDS_REGISTRATION),
            ])
            response = self.manager.admin_settings_controller.discovery_services()
            eq_(response.uri, INCOMPLETE_CONFIGURATION.uri)

        self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", ExternalIntegration.OPDS_REGISTRATION),
                (ExternalIntegration.URL, "registry url"),
            ])
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_settings_controller.discovery_services)

    def test_discovery_services_post_create(self):
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", ExternalIntegration.OPDS_REGISTRATION),
                (ExternalIntegration.URL, "registry url"),
            ])
            response = self.manager.admin_settings_controller.discovery_services()
            eq_(response.status_code, 201)

        service = get_one(self._db, ExternalIntegration, goal=ExternalIntegration.DISCOVERY_GOAL)
        eq_(service.id, int(response.response[0]))
        eq_(ExternalIntegration.OPDS_REGISTRATION, service.protocol)
        eq_("registry url", service.url)

    def test_discovery_services_post_edit(self):
        discovery_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.OPDS_REGISTRATION,
            goal=ExternalIntegration.DISCOVERY_GOAL,
        )
        discovery_service.url = "registry url"

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", discovery_service.id),
                ("protocol", ExternalIntegration.OPDS_REGISTRATION),
                (ExternalIntegration.URL, "new registry url"),
            ])
            response = self.manager.admin_settings_controller.discovery_services()
            eq_(response.status_code, 200)

        eq_(discovery_service.id, int(response.response[0]))
        eq_(ExternalIntegration.OPDS_REGISTRATION, discovery_service.protocol)
        eq_("new registry url", discovery_service.url)

    def test_discovery_service_delete(self):
        discovery_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.OPDS_REGISTRATION,
            goal=ExternalIntegration.DISCOVERY_GOAL,
        )
        discovery_service.url = "registry url"

        with self.request_context_with_admin("/", method="DELETE"):
            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_settings_controller.discovery_service,
                          discovery_service.id)

            self.admin.add_role(AdminRole.SYSTEM_ADMIN)
            response = self.manager.admin_settings_controller.discovery_service(discovery_service.id)
            eq_(response.status_code, 200)

        service = get_one(self._db, ExternalIntegration, id=discovery_service.id)
        eq_(None, service)

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
        failed, ignore = create(
            self._db, Library, name="Library 2", short_name="L2",
        )
        ConfigurationSetting.for_library_and_externalintegration(
            self._db, "library-registration-status", failed, discovery_service,
            ).value = "failure"
        unregistered, ignore = create(
            self._db, Library, name="Library 3", short_name="L3",
        )
        discovery_service.libraries = [succeeded, failed, unregistered]

        with self.request_context_with_admin("/", method="GET"):
            response = self.manager.admin_settings_controller.discovery_service_library_registrations()

            serviceInfo = response.get("library_registrations")
            eq_(1, len(serviceInfo))
            eq_(discovery_service.id, serviceInfo[0].get("id"))

            libraryInfo = serviceInfo[0].get("libraries")
            expected = [
                dict(short_name=succeeded.short_name, status="success"),
                dict(short_name=failed.short_name, status="failure"),
            ]
            eq_(expected, libraryInfo)

            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            self._db.flush()
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_settings_controller.discovery_service_library_registrations)

    def test_discovery_service_library_registrations_post_errors(self):
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("integration_id", "1234"),
            ])
            response = self.manager.admin_settings_controller.discovery_service_library_registrations()
            eq_(MISSING_SERVICE, response)

        discovery_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.OPDS_REGISTRATION,
            goal=ExternalIntegration.DISCOVERY_GOAL,
        )
        discovery_service.url = "registry url"

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("integration_id", discovery_service.id),
                ("library_short_name", "not-a-library"),
            ])
            response = self.manager.admin_settings_controller.discovery_service_library_registrations()
            eq_(NO_SUCH_LIBRARY, response)

        library = self._default_library

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("integration_id", discovery_service.id),
                ("library_short_name", library.short_name),
            ])
            feed = '<feed></feed>'
            self.responses.append(MockRequestsResponse(200, content=feed))

            response = self.manager.admin_settings_controller.discovery_service_library_registrations(do_get=self.do_request, do_post=self.do_request)
            eq_(REMOTE_INTEGRATION_FAILED.uri, response.uri)
            eq_("The service at registry url did not return OPDS.", response.detail)
            eq_([discovery_service.url], [x[0] for x in self.requests])
            eq_("failure", ConfigurationSetting.for_library_and_externalintegration(
                    self._db, "library-registration-status", library, discovery_service).value)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("integration_id", discovery_service.id),
                ("library_short_name", library.short_name),
            ])
            feed = '<feed></feed>'
            headers = { 'Content-Type': 'application/atom+xml;profile=opds-catalog;kind=navigation' }
            self.responses.append(MockRequestsResponse(200, content=feed, headers=headers))

            response = self.manager.admin_settings_controller.discovery_service_library_registrations(do_get=self.do_request, do_post=self.do_request)
            eq_(REMOTE_INTEGRATION_FAILED.uri, response.uri)
            eq_("The service at registry url did not provide a register link.", response.detail)
            eq_([discovery_service.url], [x[0] for x in self.requests[1:]])
            eq_("failure", ConfigurationSetting.for_library_and_externalintegration(
                    self._db, "library-registration-status", library, discovery_service).value)

        self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("integration_id", discovery_service.id),
                ("library_short_name", library.short_name),
            ])
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_settings_controller.discovery_service_library_registrations,
                          do_get=self.do_request, do_post=self.do_request)

    def test_discovery_service_library_registrations_post_success(self):
        discovery_service, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.OPDS_REGISTRATION,
            goal=ExternalIntegration.DISCOVERY_GOAL,
        )
        discovery_service.url = "registry url"

        library = self._default_library

        ConfigurationSetting.for_library(
            Configuration.CONFIGURATION_CONTACT_EMAIL, library
        ).value = "configproblems@library.org"

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("integration_id", discovery_service.id),
                ("library_short_name", library.short_name),
            ])
            self.responses.append(MockRequestsResponse(200, content='{}'))
            feed = '<feed><link rel="register" href="register url"/></feed>'
            headers = { 'Content-Type': 'application/atom+xml;profile=opds-catalog;kind=navigation' }
            self.responses.append(MockRequestsResponse(200, content=feed, headers=headers))

            response = self.manager.admin_settings_controller.discovery_service_library_registrations(do_get=self.do_request, do_post=self.do_request)

            eq_(200, response.status_code)
            [req1, req2] = self.requests
            eq_("registry url", req1[0])

            url, [body], kwargs = req2
            eq_("register url", url)

            # When we sent the location of our authentication document to
            # the 'registry', we provided an email address the registry can use
            # to contact us if there are problems.
            eq_("mailto:configproblems@library.org", body['contact'])

            # This registry doesn't support short client tokens and doesn't have a vendor id,
            # so no settings were added to it.
            eq_(None, discovery_service.setting(AuthdataUtility.VENDOR_ID_KEY).value)
            eq_(None, ConfigurationSetting.for_library_and_externalintegration(
                    self._db, ExternalIntegration.USERNAME, library, discovery_service).value)
            eq_(None, ConfigurationSetting.for_library_and_externalintegration(
                    self._db, ExternalIntegration.PASSWORD, library, discovery_service).value)

            # The registration status was recorded.
            eq_("success", ConfigurationSetting.for_library_and_externalintegration(
                    self._db, "library-registration-status", library, discovery_service).value)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("integration_id", discovery_service.id),
                ("library_short_name", library.short_name),
            ])
            # Generate a key in advance so we can mock the registry's encrypted response.
            key = RSA.generate(1024)
            encryptor = PKCS1_OAEP.new(key)
            encrypted_secret = encryptor.encrypt("secret")

            # This registry support short client tokens, and has a vendor id.
            metadata = dict(short_name="SHORT", shared_secret=base64.b64encode(encrypted_secret))
            catalog = dict(metadata=metadata)
            self.responses.append(MockRequestsResponse(200, content=json.dumps(catalog)))
            link = { 'rel': 'register', 'href': 'register url' }
            metadata = { 'adobe_vendor_id': 'vendorid' }
            feed = json.dumps(dict(links=[link], metadata=metadata))
            headers = { 'Content-Type': 'application/opds+json' }
            self.responses.append(MockRequestsResponse(200, content=feed, headers=headers))

            response = self.manager.admin_settings_controller.discovery_service_library_registrations(do_get=self.do_request, do_post=self.do_request, key=key)

            eq_(200, response.status_code)
            eq_(["registry url", "register url"], [x[0] for x in self.requests[2:]])

            # The vendor id and short client token settings were stored.
            eq_("vendorid", discovery_service.setting(AuthdataUtility.VENDOR_ID_KEY).value)
            eq_("SHORT", ConfigurationSetting.for_library_and_externalintegration(
                    self._db, ExternalIntegration.USERNAME, library, discovery_service).value)
            eq_("secret", ConfigurationSetting.for_library_and_externalintegration(
                    self._db, ExternalIntegration.PASSWORD, library, discovery_service).value)

            # The registration status is the same.
            eq_("success", ConfigurationSetting.for_library_and_externalintegration(
                    self._db, "library-registration-status", library, discovery_service).value)

    def test_collection_library_registrations_get(self):
        collection = self._default_collection
        succeeded, ignore = create(
            self._db, Library, name="Library 1", short_name="L1",
        )
        ConfigurationSetting.for_library_and_externalintegration(
            self._db, "library-registration-status", succeeded, collection.external_integration,
            ).value = "success"
        failed, ignore = create(
            self._db, Library, name="Library 2", short_name="L2",
        )
        ConfigurationSetting.for_library_and_externalintegration(
            self._db, "library-registration-status", failed, collection.external_integration,
            ).value = "failure"
        unregistered, ignore = create(
            self._db, Library, name="Library 3", short_name="L3",
        )
        collection.libraries = [succeeded, failed, unregistered]

        with self.request_context_with_admin("/", method="GET"):
            response = self.manager.admin_settings_controller.collection_library_registrations()

            serviceInfo = response.get("library_registrations")
            eq_(1, len(serviceInfo))
            eq_(collection.id, serviceInfo[0].get("id"))

            libraryInfo = serviceInfo[0].get("libraries")
            expected = [
                dict(short_name=succeeded.short_name, status="success"),
                dict(short_name=failed.short_name, status="failure"),
            ]
            eq_(expected, libraryInfo)

            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            self._db.flush()
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_settings_controller.collection_library_registrations)

    def test_collection_library_registrations_post_errors(self):
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("collection_id", "1234"),
            ])
            response = self.manager.admin_settings_controller.collection_library_registrations()
            eq_(MISSING_COLLECTION, response)

        collection = self._collection()
        collection.external_account_id = "collection url"

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("collection_id", collection.id),
                ("library_short_name", "not-a-library"),
            ])
            response = self.manager.admin_settings_controller.collection_library_registrations()
            eq_(COLLECTION_DOES_NOT_SUPPORT_REGISTRATION, response)

        collection.protocol = SharedODLAPI.NAME

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("collection_id", collection.id),
                ("library_short_name", "not-a-library"),
            ])
            response = self.manager.admin_settings_controller.collection_library_registrations()
            eq_(NO_SUCH_LIBRARY, response)

        library = self._default_library

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("collection_id", collection.id),
                ("library_short_name", library.short_name),
            ])
            feed = '<feed></feed>'
            self.responses.append(MockRequestsResponse(200, content=feed))

            response = self.manager.admin_settings_controller.collection_library_registrations(do_get=self.do_request, do_post=self.do_request)
            eq_(REMOTE_INTEGRATION_FAILED.uri, response.uri)
            eq_("The service at collection url did not return OPDS.", response.detail)
            eq_([collection.external_account_id], [x[0] for x in self.requests])
            eq_("failure", ConfigurationSetting.for_library_and_externalintegration(
                    self._db, "library-registration-status", library, collection.external_integration).value)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("collection_id", collection.id),
                ("library_short_name", library.short_name),
            ])
            feed = '<feed></feed>'
            headers = { 'Content-Type': 'application/atom+xml;profile=opds-catalog;kind=navigation' }
            self.responses.append(MockRequestsResponse(200, content=feed, headers=headers))

            response = self.manager.admin_settings_controller.collection_library_registrations(do_get=self.do_request, do_post=self.do_request)
            eq_(REMOTE_INTEGRATION_FAILED.uri, response.uri)
            eq_("The service at collection url did not provide a register link.", response.detail)
            eq_([collection.external_account_id], [x[0] for x in self.requests[1:]])
            eq_("failure", ConfigurationSetting.for_library_and_externalintegration(
                    self._db, "library-registration-status", library, collection.external_integration).value)

        self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("collection_id", collection.id),
                ("library_short_name", self._default_library.short_name),
            ])
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_settings_controller.collection_library_registrations,
                          do_get=self.do_request, do_post=self.do_request)

    def test_collection_library_registrations_post_success(self):
        collection = self._collection(protocol=SharedODLAPI.NAME)
        collection.external_account_id = "collection url"

        library = self._default_library

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("collection_id", collection.id),
                ("library_short_name", library.short_name),
            ])
            # Generate a key in advance so we can mock the collection's encrypted response.
            key = RSA.generate(2048)
            public_key = key.publickey().exportKey()
            encryptor = PKCS1_OAEP.new(key)

            secret = base64.b64encode(encryptor.encrypt("secret"))
            registration = json.dumps(dict(metadata=dict(shared_secret=secret)))
            self.responses.append(MockRequestsResponse(200, content=registration))
            feed = '<feed><link rel="register" href="register url"/></feed>'
            headers = { 'Content-Type': 'application/atom+xml;profile=opds-catalog;kind=navigation' }
            self.responses.append(MockRequestsResponse(200, content=feed, headers=headers))

            response = self.manager.admin_settings_controller.collection_library_registrations(do_get=self.do_request, do_post=self.do_request, key=key)

            eq_(200, response.status_code)
            eq_(["collection url", "register url"], [x[0] for x in self.requests])

            # The shared secret was saved.
            eq_("secret", ConfigurationSetting.for_library_and_externalintegration(
                    self._db, ExternalIntegration.PASSWORD, library, collection.external_integration).value)

            # The registration status was recorded.
            eq_("success", ConfigurationSetting.for_library_and_externalintegration(
                    self._db, "library-registration-status", library, collection.external_integration).value)

    def test_sitewide_registration_post_errors(self):
        def assert_remote_integration_error(response, message=None):
            eq_(REMOTE_INTEGRATION_FAILED.uri, response.uri)
            eq_(REMOTE_INTEGRATION_FAILED.title, response.title)
            if message:
                assert message in response.detail

        metadata_wrangler_service = self._external_integration(
            ExternalIntegration.METADATA_WRANGLER,
            goal=ExternalIntegration.METADATA_GOAL, url=self._url
        )
        default_form = None

        # If ExternalIntegration is given, a ProblemDetail is returned.
        with self.request_context_with_admin("/"):
            response = self.manager.admin_settings_controller.sitewide_registration(
                None, do_get=self.do_request
            )
            eq_(MISSING_SERVICE, response)

        # If an error is raised during registration, a ProblemDetail is returned.
        def error_get(*args, **kwargs):
            raise RuntimeError('Mock error during request')

        with self.request_context_with_admin("/"):
            response = self.manager.admin_settings_controller.sitewide_registration(
                metadata_wrangler_service, do_get=error_get
            )
            assert_remote_integration_error(response)

        # If the response has the wrong media type, a ProblemDetail is returned.
        self.responses.append(
            MockRequestsResponse(200, headers={'Content-Type' : 'text/plain'})
        )

        with self.request_context_with_admin("/"):
            response = self.manager.admin_settings_controller.sitewide_registration(
                metadata_wrangler_service, do_get=self.do_request
            )
            assert_remote_integration_error(
                response, 'The service did not provide a valid catalog.'
            )

        # If the response returns a ProblemDetail, its contents are wrapped
        # in another ProblemDetail.
        status_code, content, headers = MULTIPLE_BASIC_AUTH_SERVICES.response
        self.responses.append(
            MockRequestsResponse(content, headers, status_code)
        )
        with self.request_context_with_admin("/"):
            response = self.manager.admin_settings_controller.sitewide_registration(
                metadata_wrangler_service, do_get=self.do_request
            )
            assert isinstance(response, ProblemDetail)
            assert response.detail.startswith(
                "Remote service returned a problem detail document:"
            )
            assert unicode(MULTIPLE_BASIC_AUTH_SERVICES.detail) in response.detail

        # If no registration link is available, a ProblemDetail is returned
        catalog = dict(id=self._url, links=[])
        headers = { 'Content-Type' : 'application/opds+json' }
        self.responses.append(
            MockRequestsResponse(200, content=json.dumps(catalog), headers=headers)
        )

        with self.request_context_with_admin("/"):
            response = self.manager.admin_settings_controller.sitewide_registration(
                metadata_wrangler_service, do_get=self.do_request
            )
            assert_remote_integration_error(
                response, 'The service did not provide a register link.'
            )

        # If no registration details are given, a ProblemDetail is returned
        link_type = self.manager.admin_settings_controller.METADATA_SERVICE_URI_TYPE
        catalog['links'] = [dict(rel='register', href=self._url, type=link_type)]
        registration = dict(id=self._url, metadata={})
        self.responses.extend([
            MockRequestsResponse(200, content=json.dumps(registration), headers=headers),
            MockRequestsResponse(200, content=json.dumps(catalog), headers=headers)
        ])

        with self.request_context_with_admin('/', method='POST'):
            response = self.manager.admin_settings_controller.sitewide_registration(
                metadata_wrangler_service, do_get=self.do_request, do_post=self.do_request
            )
            assert_remote_integration_error(
                response, 'The service did not provide registration information.'
            )

        # If we get all the way to the registration POST, but that
        # request results in a ProblemDetail, that ProblemDetail is
        # passed along.
        self.responses.extend([
            MockRequestsResponse(200, content=json.dumps(registration), headers=headers),
            MockRequestsResponse(200, content=json.dumps(catalog), headers=headers)
        ])

        def bad_do_post(self, *args, **kwargs):
            return MULTIPLE_BASIC_AUTH_SERVICES
        with self.request_context_with_admin('/', method='POST'):
            flask.request.form = MultiDict([
                ('integration_id', metadata_wrangler_service.id),
            ])
            response = self.manager.admin_settings_controller.sitewide_registration(
                metadata_wrangler_service, do_get=self.do_request, do_post=bad_do_post
            )
        eq_(MULTIPLE_BASIC_AUTH_SERVICES, response)


    def test__decrypt_shared_secret(self):
        key = RSA.generate(2048)
        encryptor = PKCS1_OAEP.new(key)

        key2 = RSA.generate(2048)
        encryptor2 = PKCS1_OAEP.new(key2)

        shared_secret = os.urandom(24).encode('hex')
        encrypted_secret = base64.b64encode(encryptor.encrypt(shared_secret))

        # Success.
        m = self.manager.admin_settings_controller._decrypt_shared_secret
        eq_(shared_secret, m(encryptor, encrypted_secret))

        # If we try to decrypt using the wrong key, a ProblemDetail is
        # returned explaining the problem.
        problem = m(encryptor2, encrypted_secret)
        assert isinstance(problem, ProblemDetail)
        eq_(SHARED_SECRET_DECRYPTION_ERROR.uri, problem.uri)
        assert encrypted_secret in problem.detail

    def test_sitewide_registration_post_success(self):

        # A service to register with
        metadata_wrangler_service = self._external_integration(
            ExternalIntegration.METADATA_WRANGLER,
            goal=ExternalIntegration.METADATA_GOAL, url=self._url
        )

        # An RSA key for testing purposes
        key = RSA.generate(2048)
        encryptor = PKCS1_OAEP.new(key)

        # A catalog with registration url
        register_link_type = self.manager.admin_settings_controller.METADATA_SERVICE_URI_TYPE
        registration_url = self._url
        catalog = dict(
            id = metadata_wrangler_service.url,
            links = [
                dict(rel='collection-add', href=self._url, type='collection'),
                dict(rel='register', href=registration_url, type=register_link_type),
                dict(rel='collection-remove', href=self._url, type='collection'),
            ]
        )
        headers = { 'Content-Type' : 'application/opds+json' }
        self.responses.append(
            MockRequestsResponse(200, content=json.dumps(catalog), headers=headers)
        )

        # A registration document with secrets
        shared_secret = os.urandom(24).encode('hex')
        encrypted_secret = base64.b64encode(encryptor.encrypt(shared_secret))
        registration = dict(
            id = metadata_wrangler_service.url,
            metadata = dict(shared_secret=encrypted_secret)
        )
        self.responses.insert(0, MockRequestsResponse(200, content=json.dumps(registration)))

        with self.request_context_with_admin('/', method='POST'):
            flask.request.form = MultiDict([
                ('integration_id', metadata_wrangler_service.id),
            ])
            response = self.manager.admin_settings_controller.sitewide_registration(
                metadata_wrangler_service, do_get=self.do_request,
                do_post=self.do_request, key=key
            )
        eq_(None, response)

        # We made two requests: a GET to get the service document from
        # the metadata wrangler, and a POST to the registration
        # service, with the entity-body containing a callback URL and
        # a JWT.
        metadata_wrangler_service_request, registration_request = self.requests
        url, i1, i2 = metadata_wrangler_service_request
        eq_(metadata_wrangler_service.url, url)

        url, [document], ignore = registration_request
        eq_(url, registration_url)
        for k in 'url', 'jwt':
            assert k in document

        # The end result is that our ExternalIntegration for the metadata
        # wrangler has been updated with a shared secret.
        eq_(shared_secret, metadata_wrangler_service.password)

    def test_sitewide_registration_document(self):
        """Test the document sent along to sitewide registration."""
        key = RSA.generate(2048)
        controller = self.manager.admin_settings_controller
        with self.request_context_with_admin('/'):
            doc = controller.sitewide_registration_document(
                key.exportKey()
            )

            # The registrar knows where to go to get our public key.
            eq_(doc['url'], controller.url_for('public_key_document'))

            # The JWT proves that we control the public/private key pair.
            public_key = key.publickey().exportKey()
            parsed = jwt.decode(
                doc['jwt'], public_key, algorithm='RS256'
            )

            # The JWT must be valid or jwt.decode() would have raised
            # an exception. This simply verifies that the JWT includes
            # an expiration date and doesn't last forever.
            assert 'exp' in parsed
