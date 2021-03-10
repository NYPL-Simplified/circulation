from nose.tools import (
    set_trace,
    eq_,
    assert_raises
)
from api.admin.exceptions import *
from api.admin.problem_details import *
import feedparser
from werkzeug.datastructures import ImmutableMultiDict, MultiDict
import base64
import flask
import json
import math
import operator
import os
from PIL import Image
from io import StringIO
from tests.admin.controller.test_controller import AdminControllerTest
from tests.test_controller import CirculationControllerTest
from core.classifier import (
    genres,
    SimplifiedGenreClassifier
)
from core.model import (
    AdminRole,
    Classification,
    Contributor,
    Complaint,
    CoverageRecord,
    create,
    CustomList,
    DataSource,
    Edition,
    Genre,
    Hyperlink,
    Measurement,
    Representation,
    ResourceTransformation,
    RightsStatus,
    SessionManager,
    Subject,
)
from core.model.configuration import ExternalIntegrationLink
from core.s3 import MockS3Uploader
from core.testing import (
    AlwaysSuccessfulCoverageProvider,
    NeverSuccessfulCoverageProvider,
    MockRequestsResponse,
)
from datetime import date, datetime, timedelta
from functools import reduce

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
        assert Contributor.ILLUSTRATOR_ROLE in list(roles.values())
        assert Contributor.NARRATOR_ROLE in list(roles.values())
        eq_(Contributor.ILLUSTRATOR_ROLE,
            roles[Contributor.MARC_ROLE_CODES[Contributor.ILLUSTRATOR_ROLE]])
        eq_(Contributor.NARRATOR_ROLE,
            roles[Contributor.MARC_ROLE_CODES[Contributor.NARRATOR_ROLE]])

    def test_languages(self):
        languages = self.manager.admin_work_controller.languages()
        assert 'en' in list(languages.keys())
        assert 'fre' in list(languages.keys())
        names = [name for sublist in list(languages.values()) for name in sublist]
        assert 'English' in names
        assert 'French' in names

    def test_media(self):
        media = self.manager.admin_work_controller.media()
        assert Edition.BOOK_MEDIUM in list(media.values())
        assert Edition.medium_to_additional_type[Edition.BOOK_MEDIUM] in list(media.keys())

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

            # If we don't pass in a provider, it will also fail because there
            # isn't one connfigured.
            response = self.manager.admin_work_controller.refresh_metadata(
                lp.identifier.type, lp.identifier.identifier
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
                                            list(map(lambda a,b: (a-b)**2, image_histogram, expected_histogram)))/len(image_histogram))
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
                                            list(map(lambda a,b: (a-b)**2, image_histogram, expected_histogram)))/len(image_histogram))
        assert root_mean_square < 10

    def test_preview_book_cover(self):
        work = self._work(with_license_pool=True)
        identifier = work.license_pools[0].identifier

        with self.request_context_with_library_and_admin("/"):
            response = self.manager.admin_work_controller.preview_book_cover(identifier.type, identifier.identifier)
            eq_(INVALID_IMAGE.uri, response.uri)
            eq_("Image file or image URL is required.", response.detail)

        with self.request_context_with_library_and_admin("/"):
            flask.request.form = MultiDict([
                ("cover_url", "bad_url"),
            ])
            response = self.manager.admin_work_controller.preview_book_cover(identifier.type, identifier.identifier)
            eq_(INVALID_URL.uri, response.uri)
            eq_('"bad_url" is not a valid URL.', response.detail)

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
        mirror_type = ExternalIntegrationLink.COVERS
        mirrors = dict(covers_mirror=MockS3Uploader(),books_mirror=None)

        with self.request_context_with_library_and_admin("/"):
            flask.request.form = MultiDict([
                ("rights_status", RightsStatus.CC_BY),
                ("rights_explanation", "explanation"),
            ])
            response = self.manager.admin_work_controller.change_book_cover(identifier.type, identifier.identifier, mirrors)
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
                ("cover_url", "bad_url"),
                ("title_position", "none"),
                ("rights_status", RightsStatus.CC_BY),
            ])
            response = self.manager.admin_work_controller.change_book_cover(identifier.type, identifier.identifier, mirrors)
            eq_(INVALID_URL.uri, response.uri)
            eq_('"bad_url" is not a valid URL.', response.detail)

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
            response = self.manager.admin_work_controller.change_book_cover(identifier.type, identifier.identifier, mirrors)
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
            eq_([representation, thumbnail], mirrors[mirror_type].uploaded)
            eq_([representation.mirror_url, thumbnail.mirror_url], mirrors[mirror_type].destinations)

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
            response = self.manager.admin_work_controller.change_book_cover(identifier.type, identifier.identifier, mirrors)
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

            eq_([resource.representation, thumbnail], mirrors[mirror_type].uploaded[2:])
            eq_([resource.representation.mirror_url, thumbnail.mirror_url], mirrors[mirror_type].destinations[2:])

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
        identifier = work.presentation_edition.primary_identifier

        # Whenever the mocked search engine is asked how many
        # works are in a Lane, it will say there are two.
        self.controller.search_engine.docs = dict(id1="doc1", id2="doc2")

        # Create a Lane that depends on this CustomList for its membership.
        lane = self._lane()
        lane.customlists.append(list)
        lane.size = 300

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

            # Lane.size will not be updated until the work is
            # reindexed with its new list memebership and lane sizes
            # are recalculated.
            eq_(2, lane.size)

        # Now remove the work from the list.
        self.controller.search_engine.docs = dict(id1="doc1")
        with self.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("lists", json.dumps([])),
            ])
            response = self.manager.admin_work_controller.custom_lists(identifier.type, identifier.identifier)
        eq_(200, response.status_code)
        eq_(0, len(work.custom_list_entries))
        eq_(0, len(list.entries))

        # The lane size was recalculated once again.
        eq_(1, lane.size)

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
