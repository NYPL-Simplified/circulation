# encoding: utf-8
"""Test the Flask app for the circulation server."""

import base64
import feedparser
import json
import os
from ..millenium_patron import DummyMilleniumPatronAPI

from nose.tools import (
    eq_,
    set_trace,
)

from . import (
    DatabaseTest,
)

from ..core.model import (
    DataSource,
    LaneList,
    Loan,
    Resource,
    Edition,
)

from flask import url_for
os.environ['TESTING'] = "True"
from .. import circulation_manager as circulation
del os.environ['TESTING']

class AuthenticationTest(DatabaseTest):

    def setup(self):
        super(AuthenticationTest, self).setup()
        circulation.old_auth = circulation.auth
        circulation.auth = DummyMilleniumPatronAPI()

    def teardown(self):
        super(AuthenticationTest, self).teardown()
        circulation.auth = circulation.old_auth
        circulation.old_auth = None

    def test_valid_barcode(self):
        patron = circulation.authenticated_patron("1", "1111")
        eq_("1", patron.authorization_identifier)

    def test_invalid_barcode(self):
        uri, title = circulation.authenticated_patron("1", "1112")
        eq_(circulation.INVALID_CREDENTIALS_PROBLEM, uri)
        eq_(circulation.INVALID_CREDENTIALS_TITLE, title)

    def test_no_such_patron(self):
        uri, title = circulation.authenticated_patron("404111", "4444")
        eq_(circulation.INVALID_CREDENTIALS_PROBLEM, uri)
        eq_(circulation.INVALID_CREDENTIALS_TITLE, title)

    def test_expired_barcode(self):
        uri, title = circulation.authenticated_patron("410111", "4444")
        eq_(circulation.EXPIRED_CREDENTIALS_PROBLEM, uri)
        eq_(circulation.EXPIRED_CREDENTIALS_TITLE, title)


class CirculationTest(DatabaseTest):
    # TODO: The language-based tests assumes that the default sitewide
    # language is English.

    def setup(self):
        super(CirculationTest, self).setup()
        circulation.app.config['TESTING'] = True

        self.lanes = LaneList.from_description(
            self._db,
            None,
            [dict(name="Fiction", fiction=True, genres=[]),
             dict(name="Nonfiction", fiction=False, genres=[]),

             dict(name="Romance", fiction=True, genres=[],
                  subgenres=[dict(name="Contemporary Romance")])])

        circulation.Conf.initialize(self._db, self.lanes)
        self.circulation = circulation
        self.app = circulation.app
        self.client = circulation.app.test_client()

        # Create two English books and a French book.
        self.english_1 = self._work(
            "Quite British", "John Bull", language="eng", fiction=True,
            with_open_access_download=True
        )

        self.english_2 = self._work(
            "Totally American", "Uncle Sam", language="eng", fiction=False,
            with_open_access_download=True
        )
        self.french_1 = self._work(
            u"Très Français", "Marianne", language="fre", fiction=False,
            with_open_access_download=True
        )

        self.valid_auth = 'Basic ' + base64.b64encode('200:2222')
        self.invalid_auth = 'Basic ' + base64.b64encode('200:2221')

class TestNavigationFeed(CirculationTest):

    def test_root_redirects_to_navigation_feed(self):
        response = self.client.get('/')
        eq_(302, response.status_code)
        assert response.headers['Location'].endswith('/lanes/')

    def test_presence_of_extra_links(self):
        with self.app.test_request_context("/"):
            response = circulation.navigation_feed(None)
            feed = feedparser.parse(response)
            links = feed['feed']['links']
            for expect_rel, expect_href_end in (
                    ('search', '/search/'), 
                    ('http://opds-spec.org/shelf', '/loans/')):
                link = [x for x in links if x['rel'] == expect_rel][0]
                assert link['href'].endswith(expect_href_end)

    def test_faceted_links(self):
        # Create some more books to force pagination.
        self.english_2 = self._work(
            "Quite British 2: British Harder", "John Bull", language="eng",
            fiction=True, with_open_access_download=True
        )
        self.english_3 = self._work(
            "Quite British 3: Live Free or Die British", "John Bull", 
            language="eng", fiction=True, with_open_access_download=True
        )

        with self.app.test_request_context(
                "/", query_string=dict(size=1, order="author")):
            response = circulation.feed('Fiction')
            parsed = feedparser.parse(unicode(response))
            [author_facet, title_facet, next_link, search] = sorted(
                [(x['rel'], x['href'])
                 for x in parsed['feed']['links']
                 if x['rel'] not in ('alternate', 'self')
             ]
            )

            eq_("http://opds-spec.org/facet", author_facet[0])
            assert author_facet[1].endswith("/Fiction?order=author")

            eq_("http://opds-spec.org/facet", title_facet[0])
            assert title_facet[1].endswith("/Fiction?order=title")

            eq_("next", next_link[0])
            assert "?after=" in next_link[1]
            assert next_link[1].endswith("order=author")

            eq_("search", search[0])
            assert search[1].endswith('/search/Fiction')

    def test_lane_without_language_preference_uses_default_language(self):
        with self.app.test_request_context("/"):
            response = circulation.feed('Nonfiction')
            assert "Totally American" in response
            assert "Quite British" not in response # Wrong lane
            assert u"Tr&#232;s Fran&#231;ais" not in response # Wrong language

        # Now change the default language.
        old_default = os.environ.get('DEFAULT_LANGUAGES', 'eng')
        
        os.environ['DEFAULT_LANGUAGES'] = "fre"
        with self.app.test_request_context("/"):
            response = circulation.feed('Nonfiction')
            assert "Totally American" not in response
            assert u"Tr&#232;s Fran&#231;ais" in response
        os.environ['DEFAULT_LANGUAGES'] = old_default

    def test_lane_with_language_preference(self):
        
        with self.app.test_request_context(
                "/", headers={"Accept-Language": "fr"}):
            response = circulation.feed('Nonfiction')
            assert "Totally American" not in response
            assert "Tr&#232;s Fran&#231;ais" in response

        with self.app.test_request_context(
                "/", headers={"Accept-Language": "fr,en-us"}):
            response = circulation.feed('Nonfiction')
            assert "Totally American" in response
            assert "Tr&#232;s Fran&#231;ais" in response


class TestAcquisitionFeed(CirculationTest):

    def test_active_loan_feed(self):
        # No loans.
        with self.app.test_request_context(
                "/", headers=dict(Authorization=self.valid_auth)):
            response = circulation.active_loans()
            assert not "<entry>" in response

        # One loan.
        self.english_1.license_pools[0].loan_to(self.default_patron)
        with self.app.test_request_context(
                "/", headers=dict(Authorization=self.valid_auth)):
            response = circulation.active_loans()
            assert self.english_1.title in response
            assert ">loan<" in response


class TestCheckout(CirculationTest):

    def setup(self):
        super(TestCheckout, self).setup()
        self.pool = self.english_1.license_pools[0]
        self.edition = self.pool.edition()
        self.data_source = self.edition.data_source
        self.identifier = self.edition.primary_identifier
    
    def test_checkout_requires_authentication(self):
        with self.app.test_request_context(
                "/", headers=dict(Authorization=self.invalid_auth)):
            response = circulation.checkout(
                self.data_source.name, self.identifier.identifier)
            eq_(401, response.status_code)
            detail = json.loads(response.data)
            eq_(circulation.INVALID_CREDENTIALS_PROBLEM, detail['type'])

    def test_checkout_with_bad_authentication_fails(self):
        with self.app.test_request_context(
                "/", headers=dict(Authorization=self.invalid_auth)):
            response = circulation.checkout(
                self.data_source.name, self.identifier.identifier)
        eq_(401, response.status_code)
        detail = json.loads(response.data)
        eq_(circulation.INVALID_CREDENTIALS_PROBLEM, detail['type'])
        
    def test_checkout_success(self):
        with self.app.test_request_context(
                "/", headers=dict(Authorization=self.valid_auth)):
            response = circulation.checkout(
                self.data_source.name, self.identifier.identifier)

            # We've been redirected to the download link.
            eq_(302, response.status_code)
            assert response.headers['Location'].startswith("http://foo.com/")

            # A loan has been created for this license pool.
            eq_(1, self._db.query(Loan).filter(Loan.license_pool==self.pool).count())

    # TODO: We have disabled this functionality so that we can see what
    # Overdrive books look like in the catalog.

    # def test_checkout_fails_when_no_available_licenses(self):
    #     pool = self.english_2.license_pools[0]
    #     pool.open_access = False
    #     edition = pool.edition()
    #     data_source = edition.data_source
    #     identifier = edition.primary_identifier

    #     with self.app.test_request_context(
    #             "/", headers=dict(Authorization=self.valid_auth)):
    #         response = circulation.checkout(
    #             data_source.name, identifier.identifier)
    #         eq_(404, response.status_code)
    #         assert "Sorry, couldn't find an available license." in response.data
    #     pool.open_access = True
