from nose.tools import (
    set_trace,
    eq_,
)
import flask
import json
import feedparser
from werkzeug import ImmutableMultiDict, MultiDict

from ..test_controller import CirculationControllerTest
from api.admin.controller import setup_admin_controllers, AdminAnnotator
from api.admin.problem_details import *
from api.config import (
    Configuration,
    temp_config,
)
from core.model import (
    Admin,
    AdminAuthenticationService,
    CirculationEvent,
    Classification,
    Collection,
    Complaint,
    CoverageRecord,
    create,
    DataSource,
    Edition,
    Genre,
    get_one,
    get_one_or_create,
    Identifier,
    Library,
    SessionManager,
    Subject,
    WorkGenre
)
from core.testing import (
    AlwaysSuccessfulCoverageProvider,
    NeverSuccessfulCoverageProvider,
)
from core.classifier import (
    genres,
    SimplifiedGenreClassifier
)
from datetime import date, datetime, timedelta


class AdminControllerTest(CirculationControllerTest):

    def setup(self):
        with temp_config() as config:
            config[Configuration.INCLUDE_ADMIN_INTERFACE] = True
            config[Configuration.SECRET_KEY] = "a secret"

            super(AdminControllerTest, self).setup()

            setup_admin_controllers(self.manager)

class TestWorkController(AdminControllerTest):

    def test_details(self):
        [lp] = self.english_1.license_pools

        lp.suppressed = False
        with self.app.test_request_context("/"):
            response = self.manager.admin_work_controller.details(lp.data_source.name, lp.identifier.type, lp.identifier.identifier)
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
        with self.app.test_request_context("/"):
            response = self.manager.admin_work_controller.details(lp.data_source.name, lp.identifier.type, lp.identifier.identifier)
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

        with self.app.test_request_context("/"):
            flask.request.form = ImmutableMultiDict([
                ("title", "New title"),
                ("subtitle", "New subtitle"),
                ("series", "New series"),
                ("series_position", "144"),
                ("summary", "<p>New summary</p>")
            ])
            response = self.manager.admin_work_controller.edit(lp.data_source.name, lp.identifier.type, lp.identifier.identifier)
            eq_(200, response.status_code)
            eq_("New title", self.english_1.title)
            assert "New title" in self.english_1.simple_opds_entry
            eq_("New subtitle", self.english_1.subtitle)
            assert "New subtitle" in self.english_1.simple_opds_entry
            eq_("New series", self.english_1.series)
            assert "New series" in self.english_1.simple_opds_entry
            eq_(144, self.english_1.series_position)
            assert "144" in self.english_1.simple_opds_entry
            eq_("<p>New summary</p>", self.english_1.summary_text)
            assert "&lt;p&gt;New summary&lt;/p&gt;" in self.english_1.simple_opds_entry
            eq_(1, staff_edition_count())

        with self.app.test_request_context("/"):
            # Change the summary again
            flask.request.form = ImmutableMultiDict([
                ("title", "New title"),
                ("subtitle", "New subtitle"),
                ("series", "New series"),
                ("series_position", "144"),
                ("summary", "abcd")
            ])
            response = self.manager.admin_work_controller.edit(lp.data_source.name, lp.identifier.type, lp.identifier.identifier)
            eq_(200, response.status_code)
            eq_("abcd", self.english_1.summary_text)
            assert 'New summary' not in self.english_1.simple_opds_entry
            eq_(1, staff_edition_count())

        with self.app.test_request_context("/"):
            # Now delete the subtitle and series and summary entirely
            flask.request.form = ImmutableMultiDict([
                ("title", "New title"),
                ("subtitle", ""),
                ("series", ""),
                ("series_position", ""),
                ("summary", "")
            ])
            response = self.manager.admin_work_controller.edit(lp.data_source.name, lp.identifier.type, lp.identifier.identifier)
            eq_(200, response.status_code)
            eq_(None, self.english_1.subtitle)
            eq_(None, self.english_1.series)
            eq_(None, self.english_1.series_position)
            eq_("", self.english_1.summary_text)
            assert 'New subtitle' not in self.english_1.simple_opds_entry
            assert 'New series' not in self.english_1.simple_opds_entry
            assert '144' not in self.english_1.simple_opds_entry
            assert 'abcd' not in self.english_1.simple_opds_entry
            eq_(1, staff_edition_count())

        with self.app.test_request_context("/"):
            # Set the fields one more time
            flask.request.form = ImmutableMultiDict([
                ("title", "New title"),
                ("subtitle", "Final subtitle"),
                ("series", "Final series"),
                ("series_position", "169"),
                ("summary", "<p>Final summary</p>")
            ])
            response = self.manager.admin_work_controller.edit(lp.data_source.name, lp.identifier.type, lp.identifier.identifier)
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

        with self.app.test_request_context("/"):
            # Set the series position to a non-numerical value
            flask.request.form = ImmutableMultiDict([
                ("title", "New title"),
                ("subtitle", "Final subtitle"),
                ("series", "Final series"),
                ("series_position", "abc"),
                ("summary", "<p>Final summary</p>")
            ])
            response = self.manager.admin_work_controller.edit(lp.data_source.name, lp.identifier.type, lp.identifier.identifier)
            eq_(400, response.status_code)
            eq_(169, self.english_1.series_position)

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
        with self.app.test_request_context("/"):
            flask.request.form = MultiDict([
                ("audience", "Adult"),
                ("fiction", "fiction"),
                ("genres", "Horror"),
                ("genres", "Science Fiction")
            ])
            requested_genres = flask.request.form.getlist("genres")
            response = self.manager.admin_work_controller.edit_classifications(lp.data_source.name, lp.identifier.type, lp.identifier.identifier)
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
        with self.app.test_request_context("/"):
            flask.request.form = MultiDict([
                ("audience", "Adult"),
                ("fiction", "fiction")
            ])
            response = self.manager.admin_work_controller.edit_classifications(lp.data_source.name, lp.identifier.type, lp.identifier.identifier)
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
        with self.app.test_request_context("/"):
            flask.request.form = MultiDict([
                ("audience", "Adult"),
                ("fiction", "fiction"),
                ("genres", "Drama"),
                ("genres", "Urban Fantasy"),
                ("genres", "Women's Fiction")
            ])
            requested_genres = flask.request.form.getlist("genres")
            response = self.manager.admin_work_controller.edit_classifications(lp.data_source.name, lp.identifier.type, lp.identifier.identifier)
            eq_(response.status_code, 200)
            
        new_genre_names = [work_genre.genre.name for work_genre in work.work_genres]
        eq_(sorted(new_genre_names), sorted(requested_genres))
        eq_("Adult", work.audience)
        eq_(18, work.target_age.lower)
        eq_(None, work.target_age.upper)
        eq_(True, work.fiction)

        # remove some genres and change audience and target age
        with self.app.test_request_context("/"):
            flask.request.form = MultiDict([
                ("audience", "Young Adult"),
                ("target_age_min", 16),
                ("target_age_max", 18),
                ("fiction", "fiction"),
                ("genres", "Urban Fantasy")
            ])
            requested_genres = flask.request.form.getlist("genres")
            response = self.manager.admin_work_controller.edit_classifications(lp.data_source.name, lp.identifier.type, lp.identifier.identifier)
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
        with self.app.test_request_context("/"):
            flask.request.form = MultiDict([
                ("audience", "Young Adult"),
                ("target_age_min", 16),
                ("target_age_max", 18),
                ("fiction", "fiction"),
                ("genres", "Cooking"),
                ("genres", "Urban Fantasy")
            ])
            response = self.manager.admin_work_controller.edit_classifications(lp.data_source.name, lp.identifier.type, lp.identifier.identifier)

        eq_(response, INCOMPATIBLE_GENRE)
        new_genre_names = [work_genre.genre.name for work_genre in work.work_genres]
        eq_(sorted(new_genre_names), sorted(previous_genres))
        eq_("Young Adult", work.audience)
        eq_(16, work.target_age.lower)
        eq_(19, work.target_age.upper)
        eq_(True, work.fiction)

        # try to add Erotica
        with self.app.test_request_context("/"):
            flask.request.form = MultiDict([
                ("audience", "Young Adult"),
                ("target_age_min", 16),
                ("target_age_max", 18),
                ("fiction", "fiction"),
                ("genres", "Erotica"),
                ("genres", "Urban Fantasy")
            ])
            response = self.manager.admin_work_controller.edit_classifications(lp.data_source.name, lp.identifier.type, lp.identifier.identifier)
            eq_(response, EROTICA_FOR_ADULTS_ONLY)

        new_genre_names = [work_genre.genre.name for work_genre in work.work_genres]
        eq_(sorted(new_genre_names), sorted(previous_genres))
        eq_("Young Adult", work.audience)
        eq_(16, work.target_age.lower)
        eq_(19, work.target_age.upper)
        eq_(True, work.fiction)

        # try to set min target age greater than max target age
        # othe edits should not go through
        with self.app.test_request_context("/"):
            flask.request.form = MultiDict([
                ("audience", "Young Adult"),
                ("target_age_min", 16),
                ("target_age_max", 14),
                ("fiction", "nonfiction"),
                ("genres", "Cooking")
            ])
            response = self.manager.admin_work_controller.edit_classifications(lp.data_source.name, lp.identifier.type, lp.identifier.identifier)        
            eq_(400, response.status_code)
            eq_(INVALID_EDIT.uri, response.uri)

        new_genre_names = [work_genre.genre.name for work_genre in work.work_genres]
        eq_(sorted(new_genre_names), sorted(previous_genres))
        eq_(True, work.fiction)        

        # change to nonfiction with nonfiction genres and new target age
        with self.app.test_request_context("/"):
            flask.request.form = MultiDict([
                ("audience", "Young Adult"),
                ("target_age_min", 15),
                ("target_age_max", 17),
                ("fiction", "nonfiction"),
                ("genres", "Cooking")
            ])
            requested_genres = flask.request.form.getlist("genres")
            response = self.manager.admin_work_controller.edit_classifications(lp.data_source.name, lp.identifier.type, lp.identifier.identifier)

        new_genre_names = [work_genre.genre.name for work_genre in lp.work.work_genres]
        eq_(sorted(new_genre_names), sorted(requested_genres))
        eq_("Young Adult", work.audience)
        eq_(15, work.target_age.lower)
        eq_(18, work.target_age.upper)
        eq_(False, work.fiction)

        # set to Adult and make sure that target ages is set automatically
        with self.app.test_request_context("/"):
            flask.request.form = MultiDict([
                ("audience", "Adult"),
                ("fiction", "nonfiction"),
                ("genres", "Cooking")
            ])
            requested_genres = flask.request.form.getlist("genres")
            response = self.manager.admin_work_controller.edit_classifications(lp.data_source.name, lp.identifier.type, lp.identifier.identifier)

        eq_("Adult", work.audience)
        eq_(18, work.target_age.lower)
        eq_(None, work.target_age.upper)

    def test_suppress(self):
        [lp] = self.english_1.license_pools

        with self.app.test_request_context("/"):
            response = self.manager.admin_work_controller.suppress(lp.data_source.name, lp.identifier.type, lp.identifier.identifier)
            eq_(200, response.status_code)
            eq_(True, lp.suppressed)

    def test_unsuppress(self):
        [lp] = self.english_1.license_pools
        lp.suppressed = True

        with self.app.test_request_context("/"):
            response = self.manager.admin_work_controller.unsuppress(lp.data_source.name, lp.identifier.type, lp.identifier.identifier)
            eq_(200, response.status_code)
            eq_(False, lp.suppressed)

    def test_refresh_metadata(self):
        wrangler = DataSource.lookup(self._db, DataSource.METADATA_WRANGLER)
        success_provider = AlwaysSuccessfulCoverageProvider(
            "Always successful", [Identifier.GUTENBERG_ID], wrangler
        )
        failure_provider = NeverSuccessfulCoverageProvider(
            "Never successful", [Identifier.GUTENBERG_ID], wrangler
        )

        with self.app.test_request_context('/'):
            [lp] = self.english_1.license_pools
            response = self.manager.admin_work_controller.refresh_metadata(
                lp.data_source.name, lp.identifier.type, lp.identifier.identifier, provider=success_provider
            )
            eq_(200, response.status_code)
            # Also, the work has a coverage record now for the wrangler.
            assert CoverageRecord.lookup(lp.identifier, wrangler)

            response = self.manager.admin_work_controller.refresh_metadata(
                lp.data_source.name, lp.identifier.type, lp.identifier.identifier, provider=failure_provider
            )
            eq_(METADATA_REFRESH_FAILURE.status_code, response.status_code)
            eq_(METADATA_REFRESH_FAILURE.detail, response.detail)

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

        with self.app.test_request_context("/"):
            response = self.manager.admin_work_controller.complaints(lp.data_source.name, lp.identifier.type, lp.identifier.identifier)
            eq_(response['book']['data_source'], lp.data_source.name)
            eq_(response['book']['identifier_type'], lp.identifier.type)
            eq_(response['book']['identifier'], lp.identifier.identifier)
            eq_(response['complaints'][type1], 2)
            eq_(response['complaints'][type2], 1)

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
        with self.app.test_request_context("/"):
            flask.request.form = ImmutableMultiDict([("type", type2)])
            response = self.manager.admin_work_controller.resolve_complaints(lp.data_source.name, lp.identifier.type, lp.identifier.identifier)
            unresolved_complaints = [complaint for complaint in lp.complaints if complaint.resolved == None]
            eq_(response.status_code, 404)
            eq_(len(unresolved_complaints), 2)

        # then attempt to resolve complaints of the correct type
        with self.app.test_request_context("/"):
            flask.request.form = ImmutableMultiDict([("type", type1)])
            response = self.manager.admin_work_controller.resolve_complaints(lp.data_source.name, lp.identifier.type, lp.identifier.identifier)
            unresolved_complaints = [complaint for complaint in lp.complaints if complaint.resolved == None]
            eq_(response.status_code, 200)
            eq_(len(unresolved_complaints), 0)

        # then attempt to resolve the already-resolved complaints of the correct type
        with self.app.test_request_context("/"):
            flask.request.form = ImmutableMultiDict([("type", type1)])
            response = self.manager.admin_work_controller.resolve_complaints(lp.data_source.name, lp.identifier.type, lp.identifier.identifier)
            eq_(response.status_code, 409)

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

        with self.app.test_request_context("/"):
            response = self.manager.admin_work_controller.classifications(
                lp.data_source.name, lp.identifier.type, lp.identifier.identifier)
            eq_(response['book']['data_source'], lp.data_source.name)
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


class TestSignInController(AdminControllerTest):

    def setup(self):
        super(TestSignInController, self).setup()
        self.admin, ignore = create(
            self._db, Admin, email=u'example@nypl.org',
            credential=json.dumps({
                u'access_token': u'abc123',
                u'client_id': u'', u'client_secret': u'',
                u'refresh_token': u'', u'token_expiry': u'', u'token_uri': u'',
                u'user_agent': u'', u'invalid': u''
            })
        )

    def test_authenticated_admin_from_request(self):
        # Returns an error if there's no admin auth service.
        with self.app.test_request_context('/admin'):
            flask.session['admin_email'] = self.admin.email
            response = self.manager.admin_sign_in_controller.authenticated_admin_from_request()
            eq_(ADMIN_AUTH_NOT_CONFIGURED, response)

        # Works once the admin auth service exists.
        create(
            self._db, AdminAuthenticationService,
            name="Google OAuth", provider=AdminAuthenticationService.GOOGLE_OAUTH,
        )
        with self.app.test_request_context('/admin'):
            flask.session['admin_email'] = self.admin.email
            response = self.manager.admin_sign_in_controller.authenticated_admin_from_request()
            eq_(self.admin, response)

        # Returns an error if you aren't authenticated.
        with self.app.test_request_context('/admin'):
            # You get back a problem detail when you're not authenticated.
            response = self.manager.admin_sign_in_controller.authenticated_admin_from_request()
            eq_(401, response.status_code)
            eq_(INVALID_ADMIN_CREDENTIALS.detail, response.detail)

    def test_authenticated_admin(self):
        # Creates a new admin with fresh details.
        new_admin_details = {
            'email' : u'admin@nypl.org',
            'credentials' : u'gnarly',
        }
        with self.app.test_request_context('/admin/sign_in?redirect=foo'):
            admin = self.manager.admin_sign_in_controller.authenticated_admin(new_admin_details)
            eq_('admin@nypl.org', admin.email)
            eq_('gnarly', admin.credential)

            # Also sets up the admin's flask session.
            eq_("admin@nypl.org", flask.session["admin_email"])
            eq_(True, flask.session.permanent)

        # Or overwrites credentials for an existing admin.
        existing_admin_details = {
            'email' : u'example@nypl.org',
            'credentials' : u'b-a-n-a-n-a-s',
        }
        with self.app.test_request_context('/admin/sign_in?redirect=foo'):
            admin = self.manager.admin_sign_in_controller.authenticated_admin(existing_admin_details)
            eq_(self.admin.id, admin.id)
            eq_('b-a-n-a-n-a-s', self.admin.credential)

    def test_admin_signin(self):
        # Returns an error if there's no admin auth service.
        with self.app.test_request_context('/admin/sign_in?redirect=foo'):
            response = self.manager.admin_sign_in_controller.sign_in()
            eq_(ADMIN_AUTH_NOT_CONFIGURED, response)

        create(
            self._db, AdminAuthenticationService,
            name="Google OAuth", provider=AdminAuthenticationService.GOOGLE_OAUTH,
        )

        # Redirects to the auth service's login page if there's an auth service
        # but no signed in admin.
        with self.app.test_request_context('/admin/sign_in?redirect=foo'):
            response = self.manager.admin_sign_in_controller.sign_in()
            eq_(302, response.status_code)
            eq_("GOOGLE REDIRECT", response.headers["Location"])

        # Redirects to the redirect parameter if an admin is signed in.
        with self.app.test_request_context('/admin/sign_in?redirect=foo'):
            flask.session['admin_email'] = self.admin.email
            response = self.manager.admin_sign_in_controller.sign_in()
            eq_(302, response.status_code)
            eq_("foo", response.headers["Location"])

    def test_redirect_after_google_sign_in(self):
        # Returns an error if there's no admin auth service.
        with self.app.test_request_context('/admin/GoogleOAuth/callback'):
            response = self.manager.admin_sign_in_controller.redirect_after_google_sign_in()
            eq_(ADMIN_AUTH_NOT_CONFIGURED, response)

        # Returns an error if the admin auth service isn't google.
        auth_service, ignore = create(
            self._db, AdminAuthenticationService,
            name="Local Password", provider=AdminAuthenticationService.LOCAL_PASSWORD,
        )
        with self.app.test_request_context('/admin/GoogleOAuth/callback'):
            response = self.manager.admin_sign_in_controller.redirect_after_google_sign_in()
            eq_(ADMIN_AUTH_MECHANISM_NOT_CONFIGURED, response)

        self._db.delete(auth_service)
        auth_service, ignore = create(
            self._db, AdminAuthenticationService,
            name="Google OAuth", provider=AdminAuthenticationService.GOOGLE_OAUTH,
        )

        # Returns an error if google oauth fails..
        with self.app.test_request_context('/admin/GoogleOAuth/callback?error=foo'):
            response = self.manager.admin_sign_in_controller.redirect_after_google_sign_in()
            eq_(400, response.status_code)

        # Returns an error if the admin email isn't a staff email.
        auth_service.external_integration.set_setting("domains", json.dumps(["alibrary.org"]))
        with self.app.test_request_context('/admin/GoogleOAuth/callback?code=1234&state=foo'):
            response = self.manager.admin_sign_in_controller.redirect_after_google_sign_in()
            eq_(401, response.status_code)
        
        # Redirects to the state parameter if the admin email is valid.
        auth_service.external_integration.set_setting("domains", json.dumps(["nypl.org"]))
        with self.app.test_request_context('/admin/GoogleOAuth/callback?code=1234&state=foo'):
            response = self.manager.admin_sign_in_controller.redirect_after_google_sign_in()
            eq_(302, response.status_code)
            eq_("foo", response.headers["Location"])

    def test_staff_email(self):
        # Returns false if there's no admin auth service.
        with self.app.test_request_context('/admin/sign_in'):
            result = self.manager.admin_sign_in_controller.staff_email("working@alibrary.org")
            eq_(False, result)

        auth_service, ignore = create(
            self._db, AdminAuthenticationService,
            name="Google OAuth", provider=AdminAuthenticationService.GOOGLE_OAUTH,
        )
        auth_service.external_integration.set_setting("domains", json.dumps(["alibrary.org"]))

        with self.app.test_request_context('/admin/sign_in'):
            staff_email = self.manager.admin_sign_in_controller.staff_email("working@alibrary.org")
            interloper_email = self.manager.admin_sign_in_controller.staff_email("rando@gmail.com")
            eq_(True, staff_email)
            eq_(False, interloper_email)

    def test_password_sign_in(self):
        # Returns an error if there's no admin auth service.
        with self.app.test_request_context('/admin/sign_in_with_password'):
            response = self.manager.admin_sign_in_controller.password_sign_in()
            eq_(ADMIN_AUTH_NOT_CONFIGURED, response)

        # Returns an error if the admin auth service isn't password auth.
        auth_service, ignore = create(
            self._db, AdminAuthenticationService,
            name="Google OAuth", provider=AdminAuthenticationService.GOOGLE_OAUTH,
        )
        with self.app.test_request_context('/admin/sign_in_with_password'):
            response = self.manager.admin_sign_in_controller.password_sign_in()
            eq_(ADMIN_AUTH_MECHANISM_NOT_CONFIGURED, response)

        self._db.delete(auth_service)
        auth_service, ignore = create(
            self._db, AdminAuthenticationService,
            name="Local Password", provider=AdminAuthenticationService.LOCAL_PASSWORD,
        )

        # Returns a sign in page in response to a GET.
        with self.app.test_request_context('/admin/sign_in_with_password'):
            response = self.manager.admin_sign_in_controller.password_sign_in()
            eq_(200, response.status_code)
            assert "Email" in response.get_data()
            assert "Password" in response.get_data()

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


class TestFeedController(AdminControllerTest):

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
        with self.app.test_request_context("/"):
            response = self.manager.admin_feed_controller.complaints()
            feed = feedparser.parse(response.data)
            entries = feed['entries']

            eq_(len(entries), 2)

    def test_suppressed(self):
        suppressed_work = self._work(with_open_access_download=True)
        suppressed_work.license_pools[0].suppressed = True

        unsuppressed_work = self._work()

        SessionManager.refresh_materialized_views(self._db)
        with self.app.test_request_context("/"):
            response = self.manager.admin_feed_controller.suppressed()
            feed = feedparser.parse(response.data)
            entries = feed['entries']
            eq_(1, len(entries))
            eq_(suppressed_work.title, entries[0]['title'])

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

        with self.app.test_request_context("/"):
            response = self.manager.admin_dashboard_controller.circulation_events()
            url = AdminAnnotator(self.manager.circulation).permalink_for(self.english_1, lp, lp.identifier)

        events = response['circulation_events']
        eq_(types[::-1], [event['type'] for event in events])
        eq_([self.english_1.title]*len(types), [event['book']['title'] for event in events])
        eq_([url]*len(types), [event['book']['url'] for event in events])
        eq_([patron_id]*len(types), [event['patron_id'] for event in events])

        # request fewer events
        with self.app.test_request_context("/?num=2"):
            response = self.manager.admin_dashboard_controller.circulation_events()
            url = AdminAnnotator(self.manager.circulation).permalink_for(self.english_1, lp, lp.identifier)

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

            # At first, there are 3 open access titles in the database,
            # created in CirculationControllerTest.setup.
            response = self.manager.admin_dashboard_controller.stats()
            inventory_data = response.get('inventory')
            eq_(3, inventory_data.get('titles'))
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
            eq_(6, inventory_data.get('titles'))
            eq_(15, inventory_data.get('licenses'))
            eq_(4, inventory_data.get('available_licenses'))

    def test_stats_vendors(self):
        with self.app.test_request_context("/"):

            # At first, there are 3 open access titles in the database,
            # created in CirculationControllerTest.setup.
            response = self.manager.admin_dashboard_controller.stats()
            vendor_data = response.get('vendors')
            eq_(3, vendor_data.get('open_access'))
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
            eq_(3, vendor_data.get('open_access'))
            eq_(1, vendor_data.get('overdrive'))
            eq_(1, vendor_data.get('bibliotheca'))
            eq_(1, vendor_data.get('axis360'))

class TestSettingsController(AdminControllerTest):

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

        l1, ignore = create(
            self._db, Library, name="Library 1", short_name="L1",
        )
        l1.library_registry_short_name="L1"
        l1.library_registry_shared_secret="a"
        l2, ignore = create(
            self._db, Library, name="Library 2", short_name="L2",
        )

        with self.app.test_request_context("/"):
            response = self.manager.admin_settings_controller.libraries()
            libraries = response.get("libraries")
            eq_(2, len(libraries))

            eq_(l1.uuid, libraries[0].get("uuid"))
            eq_(l2.uuid, libraries[1].get("uuid"))

            eq_(l1.name, libraries[0].get("name"))
            eq_(l2.name, libraries[1].get("name"))

            eq_(l1.short_name, libraries[0].get("short_name"))
            eq_(l2.short_name, libraries[1].get("short_name"))

            eq_(l1.library_registry_short_name, libraries[0].get("library_registry_short_name"))
            eq_(l2.library_registry_short_name, libraries[1].get("library_registry_short_name"))

            eq_(l1.library_registry_shared_secret, libraries[0].get("library_registry_shared_secret"))
            eq_(l2.library_registry_shared_secret, libraries[1].get("library_registry_shared_secret"))

    def test_libraries_post_errors(self):
        library, ignore = get_one_or_create(
            self._db, Library
        )
        library.short_name = "nypl"
        library.library_registry_shared_secret = "secret"

        with self.app.test_request_context("/", method="POST"):
            flask.request.form = MultiDict([
                ("uuid", "1234"),
                ("name", "Brooklyn Public Library"),
                ("short_name", "bpl"),
            ])
            response = self.manager.admin_settings_controller.libraries()
            eq_(response, LIBRARY_NOT_FOUND)
        
        with self.app.test_request_context("/", method="POST"):
            flask.request.form = MultiDict([
                ("uuid", library.uuid),
                ("short_name", "nypl"),
                ("library_registry_shared_secret", "secret"),
                ("random_library_registry_shared_secret", ""),
            ])
            response = self.manager.admin_settings_controller.libraries()
            eq_(response, CANNOT_SET_BOTH_RANDOM_AND_SPECIFIC_SECRET)

        with self.app.test_request_context("/", method="POST"):
            flask.request.form = MultiDict([
                ("uuid", library.uuid),
                ("short_name", library.short_name),
                ("random_library_registry_shared_secret", ""),
            ])
            response = self.manager.admin_settings_controller.libraries()
            eq_(response, CANNOT_REPLACE_EXISTING_SECRET_WITH_RANDOM_SECRET)

    def test_libraries_post_create(self):
        # Delete any existing library created by the controller test setup.
        library = get_one(self._db, Library)
        if library:
            self._db.delete(library)

        with self.app.test_request_context("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "The New York Public Library"),
                ("short_name", "nypl"),
                ("library_registry_short_name", "NYPL"),
                ("library_registry_shared_secret", "secret"),
            ])
            response = self.manager.admin_settings_controller.libraries()
            eq_(response.status_code, 201)

        library = get_one(self._db, Library)

        eq_(library.name, "The New York Public Library")
        eq_(library.short_name, "nypl")
        eq_(library.library_registry_short_name, "NYPL")
        eq_(library.library_registry_shared_secret, "secret")

    def test_libraries_post_edit(self):
        # A library already exists.
        library, ignore = get_one_or_create(self._db, Library)

        library.name = "Nwe York Public Libary"
        library.short_name = "nypl"
        library.library_registry_short_name = None
        library.library_registry_shared_secret = None

        with self.app.test_request_context("/", method="POST"):
            flask.request.form = MultiDict([
                ("uuid", library.uuid),
                ("name", "The New York Public Library"),
                ("short_name", "nypl"),
                ("library_registry_short_name", "NYPL"),
                ("random_library_registry_shared_secret", ""),
            ])
            response = self.manager.admin_settings_controller.libraries()
            eq_(response.status_code, 200)

        library = get_one(self._db, Library)

        eq_(library.name, "The New York Public Library")
        eq_(library.short_name, "nypl")
        eq_(library.library_registry_short_name, "NYPL")

        # The shared secret was randomly generated, so we can't test
        # its exact value, but we do know it's a string that can be
        # converted into a hexadecimal number.
        assert library.library_registry_shared_secret != None
        int(library.library_registry_shared_secret, 16)
        
    def test_collections_get_with_no_collections(self):
        with self.app.test_request_context("/"):
            response = self.manager.admin_settings_controller.collections()
            eq_(response.get("collections"), [])

            # All the protocols in Collection.PROTOCOLS are supported by the admin interface.
            eq_(sorted([p.get("name") for p in response.get("protocols")]),
                sorted(Collection.PROTOCOLS))

    def test_collections_get_with_multiple_collections(self):
        c1, ignore = create(
            self._db, Collection, name="Collection 1", protocol=Collection.OVERDRIVE,
        )
        c1.external_account_id = "1234"
        c1.external_integration.password = "a"
        c2, ignore = create(
            self._db, Collection, name="Collection 2", protocol=Collection.BIBLIOTHECA,
        )
        c2.external_integration.password = "b"

        with self.app.test_request_context("/"):
            response = self.manager.admin_settings_controller.collections()
            collections = response.get("collections")
            eq_(2, len(collections))

            eq_(c1.name, collections[0].get("name"))
            eq_(c2.name, collections[1].get("name"))

            eq_(c1.protocol, collections[0].get("protocol"))
            eq_(c2.protocol, collections[1].get("protocol"))

            eq_(c1.external_account_id, collections[0].get("external_account_id"))
            eq_(c2.external_account_id, collections[1].get("external_account_id"))

            eq_(c1.external_integration.password, collections[0].get("password"))
            eq_(c2.external_integration.password, collections[1].get("password"))

    def test_collections_post_errors(self):
        with self.app.test_request_context("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", "Overdrive"),
            ])
            response = self.manager.admin_settings_controller.collections()
            eq_(response, MISSING_COLLECTION_NAME)

        with self.app.test_request_context("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "collection"),
            ])
            response = self.manager.admin_settings_controller.collections()
            eq_(response, NO_PROTOCOL_FOR_NEW_COLLECTION)

        with self.app.test_request_context("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "collection"),
                ("protocol", "Unknown"),
            ])
            response = self.manager.admin_settings_controller.collections()
            eq_(response, UNKNOWN_COLLECTION_PROTOCOL)

        collection, ignore = create(
            self._db, Collection, name="Collection 1",
            protocol=Collection.OVERDRIVE
        )

        with self.app.test_request_context("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Collection 1"),
                ("protocol", "Bibliotheca"),
            ])
            response = self.manager.admin_settings_controller.collections()
            eq_(response, CANNOT_CHANGE_COLLECTION_PROTOCOL)


        with self.app.test_request_context("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "collection"),
                ("protocol", "OPDS Import"),
                ("external_account_id", "test.com"),
                ("libraries", json.dumps(["nosuchlibrary"])),
            ])
            response = self.manager.admin_settings_controller.collections()
            eq_(response.uri, NO_SUCH_LIBRARY.uri)

        with self.app.test_request_context("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "collection1"),
                ("protocol", "OPDS Import"),
            ])
            response = self.manager.admin_settings_controller.collections()
            eq_(response.uri, INCOMPLETE_COLLECTION_CONFIGURATION.uri)

        with self.app.test_request_context("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Collection 1"),
                ("protocol", "Overdrive"),
                ("external_account_id", "1234"),
                ("username", "user"),
                ("password", "password"),
            ])
            response = self.manager.admin_settings_controller.collections()
            eq_(response.uri, INCOMPLETE_COLLECTION_CONFIGURATION.uri)

        with self.app.test_request_context("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "collection1"),
                ("protocol", "Bibliotheca"),
                ("external_account_id", "1234"),
                ("password", "password"),
            ])
            response = self.manager.admin_settings_controller.collections()
            eq_(response.uri, INCOMPLETE_COLLECTION_CONFIGURATION.uri)

        with self.app.test_request_context("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "collection1"),
                ("protocol", "Axis 360"),
                ("username", "user"),
                ("password", "password"),
            ])
            response = self.manager.admin_settings_controller.collections()
            eq_(response.uri, INCOMPLETE_COLLECTION_CONFIGURATION.uri)

        with self.app.test_request_context("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "collection1"),
                ("protocol", "OneClick"),
                ("username", "user"),
                ("password", "password"),
            ])
            response = self.manager.admin_settings_controller.collections()
            eq_(response.uri, INCOMPLETE_COLLECTION_CONFIGURATION.uri)

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

        with self.app.test_request_context("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "New Collection"),
                ("protocol", "Overdrive"),
                ("libraries", json.dumps(["L1", "L2"])),
                ("external_account_id", "acctid"),
                ("username", "username"),
                ("password", "password"),
                ("website_id", "1234"),
            ])
            response = self.manager.admin_settings_controller.collections()
            eq_(response.status_code, 201)

        # The collection was created and configured properly.
        collection = get_one(self._db, Collection)
        eq_("New Collection", collection.name)
        eq_("acctid", collection.external_account_id)
        eq_("username", collection.external_integration.username)
        eq_("password", collection.external_integration.password)

        # Two libraries now have access to the collection.
        eq_([collection], l1.collections)
        eq_([collection], l2.collections)
        eq_([], l3.collections)

        # One CollectionSetting was set on the collection.
        [setting] = collection.external_integration.settings
        eq_("website_id", setting.key)
        eq_("1234", setting.value)

    def test_collections_post_edit(self):
        # The collection exists.
        collection, ignore = create(
            self._db, Collection, name="Collection 1",
            protocol=Collection.OVERDRIVE
        )

        l1, ignore = create(
            self._db, Library, name="Library 1", short_name="L1",
        )

        with self.app.test_request_context("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Collection 1"),
                ("protocol", Collection.OVERDRIVE),
                ("external_account_id", "1234"),
                ("username", "user2"),
                ("password", "password"),
                ("website_id", "1234"),
                ("libraries", json.dumps(["L1"])),
            ])
            response = self.manager.admin_settings_controller.collections()
            eq_(response.status_code, 200)

        # The collection has been changed.
        eq_("user2", collection.external_integration.username)

        # A library now has access to the collection.
        eq_([collection], l1.collections)

        # One CollectionSetting was set on the collection.
        [setting] = collection.external_integration.settings
        eq_("website_id", setting.key)
        eq_("1234", setting.value)

        with self.app.test_request_context("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Collection 1"),
                ("protocol", Collection.OVERDRIVE),
                ("external_account_id", "1234"),
                ("username", "user2"),
                ("password", "password"),
                ("website_id", "1234"),
                ("libraries", json.dumps([])),
            ])
            response = self.manager.admin_settings_controller.collections()
            eq_(response.status_code, 200)

        # The collection is the same.
        eq_("user2", collection.external_integration.username)
        eq_(Collection.OVERDRIVE, collection.protocol)

        # But the library has been removed.
        eq_([], l1.collections)

    def test_admin_auth_services_get_with_no_services(self):
        with self.app.test_request_context("/"):
            response = self.manager.admin_settings_controller.admin_auth_services()
            eq_(response.get("admin_auth_services"), [])

            # All the providers in AdminAuthenticationService.PROVIDERS are supported by the admin interface.
            eq_(sorted([p for p in response.get("providers")]),
                sorted(AdminAuthenticationService.PROVIDERS))
        
    def test_admin_auth_services_get_with_google_oauth_service(self):
        auth_service, ignore = create(
            self._db, AdminAuthenticationService,
            name="Google OAuth", provider=AdminAuthenticationService.GOOGLE_OAUTH,
        )
        auth_service.external_integration.url = "http://oauth.test"
        auth_service.external_integration.username = "user"
        auth_service.external_integration.password = "pass"
        auth_service.external_integration.set_setting("domains", json.dumps(["nypl.org"]))

        with self.app.test_request_context("/"):
            response = self.manager.admin_settings_controller.admin_auth_services()
            [service] = response.get("admin_auth_services")

            eq_(auth_service.name, service.get("name"))
            eq_(auth_service.provider, service.get("provider"))
            eq_(auth_service.external_integration.url, service.get("url"))
            eq_(auth_service.external_integration.username, service.get("username"))
            eq_(auth_service.external_integration.password, service.get("password"))
            eq_(["nypl.org"], service.get("domains"))

    def test_admin_auth_services_get_with_local_password_service(self):
        auth_service, ignore = create(
            self._db, AdminAuthenticationService,
            name="Local Password", provider=AdminAuthenticationService.LOCAL_PASSWORD,
        )
        # There are two admins that can sign in with passwords.
        admin1, ignore = create(self._db, Admin, email="admin1@nypl.org")
        admin1.password = "pass1"
        admin2, ignore = create(self._db, Admin, email="admin2@nypl.org")
        admin2.password = "pass2"

        # This admin doesn't have a password, and won't be included.
        admin3, ignore = create(self._db, Admin, email="admin3@nypl.org")

        with self.app.test_request_context("/"):
            response = self.manager.admin_settings_controller.admin_auth_services()
            [service] = response.get("admin_auth_services")

            eq_(auth_service.name, service.get("name"))
            eq_(auth_service.provider, service.get("provider"))
            eq_([{"email": "admin1@nypl.org"}, {"email": "admin2@nypl.org"}], service.get("admins"))

    def test_admin_auth_services_post_errors(self):
        with self.app.test_request_context("/", method="POST"):
            flask.request.form = MultiDict([
            ])
            response = self.manager.admin_settings_controller.admin_auth_services()
            eq_(response, MISSING_ADMIN_AUTH_SERVICE_NAME)

        with self.app.test_request_context("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "auth service"),
                ("provider", "Unknown"),
            ])
            response = self.manager.admin_settings_controller.admin_auth_services()
            eq_(response, UNKNOWN_ADMIN_AUTH_SERVICE_PROVIDER)

        with self.app.test_request_context("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "auth service"),
            ])
            response = self.manager.admin_settings_controller.admin_auth_services()
            eq_(response, NO_PROVIDER_FOR_NEW_ADMIN_AUTH_SERVICE)


    def test_admin_auth_services_post_errors_google_oauth(self):
        auth_service, ignore = create(
            self._db, AdminAuthenticationService, name="auth service",
            provider=AdminAuthenticationService.GOOGLE_OAUTH,
        )

        with self.app.test_request_context("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "other auth service"),
            ])
            response = self.manager.admin_settings_controller.admin_auth_services()
            eq_(response, ADMIN_AUTH_SERVICE_NOT_FOUND)
        
        with self.app.test_request_context("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "auth service"),
                ("provider", "Local Password"),
            ])
            response = self.manager.admin_settings_controller.admin_auth_services()
            eq_(response, CANNOT_CHANGE_ADMIN_AUTH_SERVICE_PROVIDER)

        with self.app.test_request_context("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "auth service"),
                ("provider", "Google OAuth"),
            ])
            response = self.manager.admin_settings_controller.admin_auth_services()
            eq_(response.uri, INCOMPLETE_ADMIN_AUTH_SERVICE_CONFIGURATION.uri)

        with self.app.test_request_context("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "auth service"),
                ("provider", "Google OAuth"),
                ("url", "url"),
                ("username", "username"),
                ("password", "password"),
                ("domains", "not json"),
            ])
            response = self.manager.admin_settings_controller.admin_auth_services()
            eq_(response, INVALID_ADMIN_AUTH_DOMAIN_LIST)

    def test_admin_auth_services_post_errors_local_password(self):
        auth_service, ignore = create(
            self._db, AdminAuthenticationService, name="auth service",
            provider=AdminAuthenticationService.LOCAL_PASSWORD,
        )

        with self.app.test_request_context("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "auth service"),
                ("provider", "Local Password"),
            ])
            response = self.manager.admin_settings_controller.admin_auth_services()
            eq_(response.uri, INCOMPLETE_ADMIN_AUTH_SERVICE_CONFIGURATION.uri)

        with self.app.test_request_context("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "auth service"),
                ("provider", "Local Password"),
                ("admins", "not json"),
            ])
            response = self.manager.admin_settings_controller.admin_auth_services()
            eq_(response, INVALID_ADMIN_AUTH_ADMINS_LIST)
        
    def test_admin_auth_services_post_google_oauth_create(self):
        with self.app.test_request_context("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "new auth service"),
                ("provider", "Google OAuth"),
                ("url", "url"),
                ("username", "username"),
                ("password", "password"),
                ("domains", json.dumps(["nypl.org", "gmail.com"])),
            ])
            response = self.manager.admin_settings_controller.admin_auth_services()
            eq_(response.status_code, 201)

        # The auth service was created and configured properly.
        auth_service = get_one(self._db, AdminAuthenticationService)
        eq_("new auth service", auth_service.name)
        eq_("url", auth_service.external_integration.url)
        eq_("username", auth_service.external_integration.username)
        eq_("password", auth_service.external_integration.password)

        [setting] = auth_service.external_integration.settings
        eq_("domains", setting.key)
        eq_(["nypl.org", "gmail.com"], json.loads(setting.value))

    def test_admin_auth_services_post_google_oauth_edit(self):
        # The auth service exists.
        auth_service, ignore = create(
            self._db, AdminAuthenticationService, name="auth service",
            provider=AdminAuthenticationService.GOOGLE_OAUTH,
        )
        auth_service.external_integration.url = "url"
        auth_service.external_integration.username = "user"
        auth_service.external_integration.password = "pass"
        auth_service.external_integration.set_setting("domains", json.dumps(["library1.org"]))

        with self.app.test_request_context("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "auth service"),
                ("provider", "Google OAuth"),
                ("url", "url2"),
                ("username", "user2"),
                ("password", "pass2"),
                ("domains", json.dumps(["library2.org"])),
            ])
            response = self.manager.admin_settings_controller.admin_auth_services()
            eq_(response.status_code, 200)

        eq_("url2", auth_service.external_integration.url)
        eq_("user2", auth_service.external_integration.username)
        [setting] = auth_service.external_integration.settings
        eq_("domains", setting.key)
        eq_(["library2.org"], json.loads(setting.value))

    def test_admin_auth_services_post_local_password_create(self):
        with self.app.test_request_context("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "new auth service"),
                ("provider", "Local Password"),
                ("admins", json.dumps([{"email": "admin1@nypl.org", "password": "pass1"},
                                       {"email": "admin2@nypl.org", "password": "pass2"}])),
            ])
            response = self.manager.admin_settings_controller.admin_auth_services()
            eq_(response.status_code, 201)

        # The auth service was created, and two admins were created..
        auth_service = get_one(self._db, AdminAuthenticationService)
        eq_("new auth service", auth_service.name)

        admin1_matches = self._db.query(Admin).filter(Admin.email=="admin1@nypl.org").filter(Admin.password=="pass1").all()
        eq_(1, len(admin1_matches))

        admin2_matches = self._db.query(Admin).filter(Admin.email=="admin2@nypl.org").filter(Admin.password=="pass2").all()
        eq_(1, len(admin2_matches))

    def test_admin_auth_services_post_local_password_edit(self):
        # The auth service exists, with one admin.
        auth_service, ignore = create(
            self._db, AdminAuthenticationService, name="auth service",
            provider=AdminAuthenticationService.LOCAL_PASSWORD,
        )
        old_admin, ignore = create(
            self._db, Admin, email="oldadmin@nypl.org",
        )
        old_admin.password = "password"

        with self.app.test_request_context("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "auth service"),
                ("provider", "Local Password"),
                ("admins", json.dumps([{"email": "admin1@nypl.org", "password": "pass1"},
                                       {"email": "admin2@nypl.org", "password": "pass2"}])),
            ])
            response = self.manager.admin_settings_controller.admin_auth_services()
            eq_(response.status_code, 200)

        # The old admin was deleted, and the new admins were created.
        admins = self._db.query(Admin).all()
        eq_(2, len(admins))

        admin1_matches = self._db.query(Admin).filter(Admin.email=="admin1@nypl.org").filter(Admin.password=="pass1").all()
        eq_(1, len(admin1_matches))

        admin2_matches = self._db.query(Admin).filter(Admin.email=="admin2@nypl.org").filter(Admin.password=="pass2").all()
        eq_(1, len(admin2_matches))
