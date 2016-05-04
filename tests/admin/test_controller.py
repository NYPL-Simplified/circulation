from nose.tools import (
    set_trace,
    eq_,
)
import flask
import json
import feedparser
from werkzeug import ImmutableMultiDict, MultiDict

from ..test_controller import CirculationControllerTest
from api.admin.controller import setup_admin_controllers
from api.problem_details import *
from api.admin.config import (
    Configuration,
    temp_config,
)
from core.model import (
    Admin,
    Classification,
    Complaint,
    CoverageRecord,
    create,
    DataSource,
    Genre,
    Identifier,
    SessionManager,
    Subject
)
from core.testing import (
    AlwaysSuccessfulCoverageProvider,
    NeverSuccessfulCoverageProvider,
)
from core.classifier import genres

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
            response = self.manager.admin_work_controller.details(lp.data_source.name, lp.identifier.identifier)
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
            response = self.manager.admin_work_controller.details(lp.data_source.name, lp.identifier.identifier)
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
        with self.app.test_request_context("/"):
            flask.request.form = ImmutableMultiDict([
                ("title", "New title"),
                ("audience", "Adults Only"),
                ("summary", "<p>New summary</p>"),
                ("fiction", "nonfiction"),
            ])
            response = self.manager.admin_work_controller.edit(lp.data_source.name, lp.identifier.identifier)

            eq_(200, response.status_code)
            eq_("New title", self.english_1.title)
            assert "New title" in self.english_1.simple_opds_entry
            eq_("Adults Only", self.english_1.audience)
            assert 'Adults Only' in self.english_1.simple_opds_entry
            eq_("<p>New summary</p>", self.english_1.summary_text)
            assert "&lt;p&gt;New summary&lt;/p&gt;" in self.english_1.simple_opds_entry
            eq_(False, self.english_1.fiction)
            assert "Nonfiction" in self.english_1.simple_opds_entry

        with self.app.test_request_context("/"):
            # Change the audience and fiction status again, and add a target age
            flask.request.form = ImmutableMultiDict([
                ("title", "New title"),
                ("audience", "Young Adult"),
                ("summary", "<p>New summary</p>"),
                ("fiction", "fiction"),
                ("target_age_min", 13),
                ("target_age_max", 15),
            ])
            response = self.manager.admin_work_controller.edit(lp.data_source.name, lp.identifier.identifier)
            eq_(200, response.status_code)
            eq_("Young Adult", self.english_1.audience)
            assert 'Young Adult' in self.english_1.simple_opds_entry
            assert 'Adults Only' not in self.english_1.simple_opds_entry
            eq_(True, self.english_1.fiction)
            assert "Fiction" in self.english_1.simple_opds_entry
            assert "Nonfiction" not in self.english_1.simple_opds_entry
            eq_(13, self.english_1.target_age.lower)
            eq_(15, self.english_1.target_age.upper)
            assert "13-15" in self.english_1.simple_opds_entry

        with self.app.test_request_context("/"):
            # Change the summary again
            flask.request.form = ImmutableMultiDict([
                ("title", "New title"),
                ("audience", "Young Adult"),
                ("summary", "abcd"),
                ("fiction", "fiction"),
                ("target_age_min", 13),
                ("target_age_max", 15),
            ])
            response = self.manager.admin_work_controller.edit(lp.data_source.name, lp.identifier.identifier)
            eq_(200, response.status_code)
            eq_("abcd", self.english_1.summary_text)
            assert 'New summary' not in self.english_1.simple_opds_entry

        with self.app.test_request_context("/"):
            # Now delete the summary entirely and change the target age again
            flask.request.form = ImmutableMultiDict([
                ("title", "New title"),
                ("audience", "Young Adult"),
                ("summary", ""),
                ("fiction", "fiction"),
                ("target_age_min", 11),
                ("target_age_max", 14),
            ])
            response = self.manager.admin_work_controller.edit(lp.data_source.name, lp.identifier.identifier)
            eq_(200, response.status_code)
            eq_("", self.english_1.summary_text)
            assert 'abcd' not in self.english_1.simple_opds_entry
            eq_(11, self.english_1.target_age.lower)
            eq_(14, self.english_1.target_age.upper)
            assert "11-14" in self.english_1.simple_opds_entry
            assert "13-15" not in self.english_1.simple_opds_entry

        with self.app.test_request_context("/"):
            # Change audience and remove target age, so computed target age is based on audience
            flask.request.form = ImmutableMultiDict([
                ("title", "New title"),
                ("audience", "Adult"),
                ("summary", ""),
                ("fiction", "fiction"),
            ])
            response = self.manager.admin_work_controller.edit(lp.data_source.name, lp.identifier.identifier)
            eq_(200, response.status_code)
            eq_("Adult", self.english_1.audience)
            assert 'Adult' in self.english_1.simple_opds_entry
            assert 'Young Adult' not in self.english_1.simple_opds_entry
            eq_(18, self.english_1.target_age.lower)
            eq_(None, self.english_1.target_age.upper)
            assert "11-14" not in self.english_1.simple_opds_entry
            assert "18" in self.english_1.simple_opds_entry

    def test_edit_invalid_input(self):
        [lp] = self.english_1.license_pools
        with self.app.test_request_context("/"):
            # target age min greater than target age max
            flask.request.form = ImmutableMultiDict([
                ("target_age_min", 10),
                ("target_age_max", 5),
            ])
            response = self.manager.admin_work_controller.edit(lp.data_source.name, lp.identifier.identifier)
            eq_(400, response.status_code)
            eq_(INVALID_EDIT.uri, response.uri)

    def test_update_genres(self):
        # start with no genres
        [lp] = self.english_1.license_pools
    
        # add first few fiction genres
        with self.app.test_request_context("/"):
            requested_genres = ["Drama", "Urban Fantasy", "Women's Fiction"]
            form = MultiDict()
            for genre in requested_genres:
                form.add("genres", genre)
            flask.request.form = form
            response = self.manager.admin_work_controller.update_genres(lp.data_source.name, lp.identifier.identifier)
            new_genre_names = [work_genre.genre.name for work_genre in lp.work.work_genres]

            eq_(len(new_genre_names), len(requested_genres))
            for genre in requested_genres:
                eq_(True, genre in new_genre_names)

        # remove a genre
        with self.app.test_request_context("/"):
            requested_genres = ["Drama", "Women's Fiction"]
            form = MultiDict()
            for genre in requested_genres:
                form.add("genres", genre)
            flask.request.form = form
            response = self.manager.admin_work_controller.update_genres(lp.data_source.name, lp.identifier.identifier)
            new_genre_names = [work_genre.genre.name for work_genre in lp.work.work_genres]

            eq_(len(new_genre_names), len(requested_genres))
            for genre in requested_genres:
                eq_(True, genre in new_genre_names)

        previous_genres = requested_genres

        # try to add a nonfiction genre
        with self.app.test_request_context("/"):
            requested_genres = ["Drama", "Women's Fiction", "Cooking"]
            form = MultiDict()
            for genre in requested_genres:
                form.add("genres", genre)
            flask.request.form = form
            response = self.manager.admin_work_controller.update_genres(lp.data_source.name, lp.identifier.identifier)
            new_genre_names = [work_genre.genre.name for work_genre in lp.work.work_genres]

            eq_(len(new_genre_names), len(previous_genres))
            for genre in previous_genres:
                eq_(True, genre in new_genre_names)

        # try to add a nonexistent genre
        with self.app.test_request_context("/"):
            requested_genres = ["Drama", "Women's Fiction", "Epic Military Memoirs"]
            form = MultiDict()
            for genre in requested_genres:
                form.add("genres", genre)
            flask.request.form = form
            response = self.manager.admin_work_controller.update_genres(lp.data_source.name, lp.identifier.identifier)
            new_genre_names = [work_genre.genre.name for work_genre in lp.work.work_genres]

            eq_(len(new_genre_names), len(previous_genres))
            for genre in previous_genres:
                eq_(True, genre in new_genre_names)

    def test_suppress(self):
        [lp] = self.english_1.license_pools

        with self.app.test_request_context("/"):
            response = self.manager.admin_work_controller.suppress(lp.data_source.name, lp.identifier.identifier)
            eq_(200, response.status_code)
            eq_(True, lp.suppressed)

    def test_unsuppress(self):
        [lp] = self.english_1.license_pools
        lp.suppressed = True

        with self.app.test_request_context("/"):
            response = self.manager.admin_work_controller.unsuppress(lp.data_source.name, lp.identifier.identifier)
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
                lp.data_source.name, lp.identifier.identifier, provider=success_provider
            )
            eq_(200, response.status_code)
            # Also, the work has a coverage record now for the wrangler.
            assert CoverageRecord.lookup(lp.identifier, wrangler)

            response = self.manager.admin_work_controller.refresh_metadata(
                lp.data_source.name, lp.identifier.identifier, provider=failure_provider
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
            response = self.manager.admin_work_controller.complaints(lp.data_source.name, lp.identifier.identifier)
            eq_(response['book']['data_source'], lp.data_source.name)
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
            response = self.manager.admin_work_controller.resolve_complaints(lp.data_source.name, lp.identifier.identifier)
            unresolved_complaints = [complaint for complaint in lp.complaints if complaint.resolved == None]
            eq_(response.status_code, 404)
            eq_(len(unresolved_complaints), 2)

        # then attempt to resolve complaints of the correct type
        with self.app.test_request_context("/"):
            flask.request.form = ImmutableMultiDict([("type", type1)])
            response = self.manager.admin_work_controller.resolve_complaints(lp.data_source.name, lp.identifier.identifier)
            unresolved_complaints = [complaint for complaint in lp.complaints if complaint.resolved == None]
            eq_(response.status_code, 200)
            eq_(len(unresolved_complaints), 0)

        # then attempt to resolve the already-resolved complaints of the correct type
        with self.app.test_request_context("/"):
            flask.request.form = ImmutableMultiDict([("type", type1)])
            response = self.manager.admin_work_controller.resolve_complaints(lp.data_source.name, lp.identifier.identifier)
            eq_(response.status_code, 409)

    def test_classifications(self):
        e, pool = self._edition(with_license_pool=True)
        work = self._work(primary_edition=e)
        identifier = work.primary_edition.primary_identifier
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
            data_source=source, weight=2)
        classification3 = self._classification(
            identifier=identifier, subject=subject3, 
            data_source=source, weight=1.5)

        SessionManager.refresh_materialized_views(self._db)
        [lp] = work.license_pools

        # first attempt to resolve complaints of the wrong type
        with self.app.test_request_context("/"):
            response = self.manager.admin_work_controller.classifications(
                lp.data_source.name, lp.identifier.identifier)
            
            eq_(response['book']['data_source'], lp.data_source.name)
            eq_(response['book']['identifier'], lp.identifier.identifier)
            eq_(len(response['subjects']), 2)
            eq_(response['subjects'][0]['name'], subject2.identifier)
            eq_(response['subjects'][0]['type'], subject2.type)
            eq_(response['subjects'][0]['source'], source.name)
            eq_(response['subjects'][0]['weight'], classification2.weight)
            eq_(response['subjects'][1]['name'], subject1.identifier)
            eq_(response['subjects'][1]['type'], subject1.type)
            eq_(response['subjects'][1]['source'], source.name)
            eq_(response['subjects'][1]['weight'], classification1.weight)


class TestSignInController(AdminControllerTest):

    def setup(self):
        super(TestSignInController, self).setup()
        self.admin, ignore = create(
            self._db, Admin, email=u'example@nypl.org', access_token=u'abc123',
            credential=json.dumps({
                u'access_token': u'abc123',
                u'client_id': u'', u'client_secret': u'',
                u'refresh_token': u'', u'token_expiry': u'', u'token_uri': u'',
                u'user_agent': u'', u'invalid': u''
            })
        )

    def test_authenticated_admin_from_request(self):
        with self.app.test_request_context('/admin'):
            flask.session['admin_access_token'] = self.admin.access_token
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
            'access_token' : u'tubular',
            'credentials' : u'gnarly',
        }
        admin = self.manager.admin_sign_in_controller.authenticated_admin(new_admin_details)
        eq_('admin@nypl.org', admin.email)
        eq_('tubular', admin.access_token)
        eq_('gnarly', admin.credential)

        # Or overwrites credentials for an existing admin.
        existing_admin_details = {
            'email' : u'example@nypl.org',
            'access_token' : u'bananas',
            'credentials' : u'b-a-n-a-n-a-s',
        }
        admin = self.manager.admin_sign_in_controller.authenticated_admin(existing_admin_details)
        eq_(self.admin.id, admin.id)
        eq_('bananas', self.admin.access_token)
        eq_('b-a-n-a-n-a-s', self.admin.credential)

    def test_admin_signin(self):
        with self.app.test_request_context('/admin/sign_in?redirect=foo'):
            flask.session['admin_access_token'] = self.admin.access_token
            response = self.manager.admin_sign_in_controller.sign_in()
            eq_(302, response.status_code)
            eq_("foo", response.headers["Location"])

    def test_staff_email(self):
        with temp_config() as config:
            config[Configuration.POLICIES] = {
                Configuration.ADMIN_AUTH_DOMAIN : "alibrary.org"
            }
            with self.app.test_request_context('/admin/sign_in'):
                staff_email = self.manager.admin_sign_in_controller.staff_email("working@alibrary.org")
                interloper_email = self.manager.admin_sign_in_controller.staff_email("rando@gmail.com")
                eq_(True, staff_email)
                eq_(False, interloper_email)


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
