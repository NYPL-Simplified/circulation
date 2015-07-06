# encoding: utf-8
"""Test the Flask app for the circulation server."""

import base64
import feedparser
import json
import os
from ..millenium_patron import DummyMilleniumPatronAPI
from flask import url_for

from nose.tools import (
    eq_,
    set_trace,
)

from . import (
    DatabaseTest,
)

from ..core.model import (
    get_one,
    DataSource,
    LaneList,
    Loan,
    Patron,
    Resource,
    Edition,
)

from ..core.opds import (
    OPDSFeed,
)
from ..core.util.opds_authentication_document import (
    OPDSAuthenticationDocument
)


class CirculationTest(DatabaseTest):

    def setup(self):
        os.environ['TESTING'] = "True"
        from .. import app as circulation
        del os.environ['TESTING']

        super(CirculationTest, self).setup()
        self.lanes = LaneList.from_description(
            self._db,
            None,
            [dict(full_name="Fiction", fiction=True, genres=[]),
             dict(full_name="Nonfiction", fiction=False, genres=[]),

             dict(full_name="Romance", fiction=True, genres=[],
                  subgenres=[dict(full_name="Contemporary Romance")])])

        circulation.Conf.initialize(self._db, self.lanes)
        self.circulation = circulation
        self.app = circulation.app
        self.client = circulation.app.test_client()

class AuthenticationTest(CirculationTest):

    def test_valid_barcode(self):
        patron = self.circulation.authenticated_patron("1", "1111")
        eq_("1", patron.authorization_identifier)

    def test_invalid_barcode(self):
        uri, title = self.circulation.authenticated_patron("1", "1112")
        eq_(circulation.INVALID_CREDENTIALS_PROBLEM, uri)
        eq_(circulation.INVALID_CREDENTIALS_TITLE, title)

    def test_no_such_patron(self):
        uri, title = self.circulation.authenticated_patron("404111", "4444")
        eq_(circulation.INVALID_CREDENTIALS_PROBLEM, uri)
        eq_(circulation.INVALID_CREDENTIALS_TITLE, title)

    def test_expired_barcode(self):
        uri, title = self.circulation.authenticated_patron("410111", "4444")
        eq_(circulation.EXPIRED_CREDENTIALS_PROBLEM, uri)
        eq_(circulation.EXPIRED_CREDENTIALS_TITLE, title)


class CirculationAppTest(CirculationTest):
    # TODO: The language-based tests assumes that the default sitewide
    # language is English.

    def setup(self):
        super(CirculationAppTest, self).setup()

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

class TestNavigationFeed(CirculationAppTest):

    def test_root_redirects_to_groups_feed(self):
        response = self.client.get('/')
        eq_(302, response.status_code)
        assert response.headers['Location'].endswith('/groups/')

    def test_presence_of_extra_links(self):
        with self.app.test_request_context("/"):
            response = self.circulation.navigation_feed(None)
            cache_control = response.headers['Cache-Control']
            eq_('public, no-transform, max-age: 7200, s-maxage: 3600', cache_control)
            feed = feedparser.parse(response.data)
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
            response = self.circulation.feed('Fiction')
            assert response.headers['Cache-Control'].startswith('public,')
            parsed = feedparser.parse(unicode(response.data))
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
            assert "&order=author" in next_link[1]

            eq_("search", search[0])
            assert search[1].endswith('/search/Fiction')

    def test_lane_without_language_preference_uses_default_language(self):
        with self.app.test_request_context("/"):
            response = self.circulation.feed('Nonfiction')
            assert "Totally American" in response.data
            assert "Quite British" not in response.data # Wrong lane
            assert u"Tr&#232;s Fran&#231;ais" not in response.data # Wrong language

        # Now change the default language.
        old_default = os.environ.get('DEFAULT_LANGUAGES', 'eng')
        
        os.environ['DEFAULT_LANGUAGES'] = "fre"
        with self.app.test_request_context("/"):
            response = self.circulation.feed('Nonfiction')
            assert "Totally American" not in response.data
            assert u"Tr&#232;s Fran&#231;ais" in response.data
        os.environ['DEFAULT_LANGUAGES'] = old_default

    def test_lane_with_language_preference(self):
        
        with self.app.test_request_context(
                "/", headers={"Accept-Language": "fr"}):
            response = self.circulation.feed('Nonfiction')
            assert "Totally American" not in response.data
            assert "Tr&#232;s Fran&#231;ais" in response.data

        with self.app.test_request_context(
                "/", headers={"Accept-Language": "fr,en-us"}):
            response = self.circulation.feed('Nonfiction')
            assert "Totally American" in response.data
            assert "Tr&#232;s Fran&#231;ais" in response.data


class TestAcquisitionFeed(CirculationAppTest):

    def test_active_loan_feed(self):
        # No loans.

        overdrive = self.circulation.Conf.overdrive
        threem = self.circulation.Conf.threem
        from test_overdrive import TestOverdriveAPI as overdrive_data
        from test_threem import TestThreeMAPI as threem_data
        overdrive.queue_response(
            content=overdrive_data.sample_data("empty_checkouts_list.json"))
        threem.queue_response(
            content=threem_data.sample_data("empty_checkouts.xml"))

        with self.app.test_request_context(
                "/", headers=dict(Authorization=self.valid_auth)):
            response = self.circulation.active_loans()
            assert not "<entry>" in response.data
            assert response.headers['Cache-Control'].startswith('private,')

        # A number of loans and holds.
        overdrive.queue_response(
            content=overdrive_data.sample_data("holds.json"))
        overdrive.queue_response(
            content=overdrive_data.sample_data("checkouts_list.json"))
        threem.queue_response(
            content=threem_data.sample_data("checkouts.xml"))

        patron = get_one(self._db, Patron,
            authorization_identifier="200")

        # Sync the bookshelf so we can create works for the loans.
        pin = 'foo'
        overdrive_loans = overdrive.get_patron_checkouts(patron, pin)
        overdrive_holds = overdrive.get_patron_holds(patron, pin)

        threem_loans, threem_holds, threem_reserves = threem.get_patron_checkouts(
            patron)

        overdrive.sync_bookshelf(patron, overdrive_loans, overdrive_holds)
        threem.sync_bookshelf(
            patron, threem_loans, threem_holds, threem_reserves)

        # Super hacky--make sure the loans and holds have works that
        # will show up in the feed.
        for l in [patron.loans, patron.holds]:
            for loan in l:
                pool = loan.license_pool
                work = self._work()
                work.license_pools = [pool]
                work.editions[0].primary_identifier = pool.identifier
                work.editions[0].data_source = pool.data_source
        self._db.commit()

        # Queue the same loan and hold lists from last time,
        # so we can actually generate the feed.
        overdrive.queue_response(
            content=overdrive_data.sample_data("holds.json"))
        overdrive.queue_response(
            content=overdrive_data.sample_data("checkouts_list.json"))
        threem.queue_response(
            content=threem_data.sample_data("checkouts.xml"))

        with self.app.test_request_context(
                "/", headers=dict(Authorization=self.valid_auth)):
            response = self.circulation.active_loans()
            assert ">loan<" in response.data
            for loan in patron.loans:
                expect_title = loan.license_pool.work.title
                assert "title>%s</title" % expect_title in response.data

            assert ">hold<" in response.data
            for hold in patron.holds:
                expect_title = hold.license_pool.work.title
                assert "title>%s</title" % expect_title in response.data

            # Each entry must have a 'revoke' link.
            feed = feedparser.parse(response.data)
            for entry in feed['entries']:
                revoke_link = [x for x in entry['links']
                               if x['rel'] == OPDSFeed.REVOKE_LOAN_REL]
                assert revoke_link != []

class TestCheckout(CirculationAppTest):

    def setup(self):
        super(TestCheckout, self).setup()
        self.pool = self.english_1.license_pools[0]
        self.edition = self.pool.edition
        self.data_source = self.edition.data_source
        self.identifier = self.edition.primary_identifier
    
    def test_checkout_requires_authentication(self):
        with self.app.test_request_context(
                "/", headers=dict(Authorization=self.invalid_auth)):
            response = self.circulation.borrow(
                self.data_source.name, self.identifier.identifier)
            eq_(401, response.status_code)
            eq_(OPDSAuthenticationDocument.MEDIA_TYPE, 
                response.headers['Content-Type'])
            detail = json.loads(response.data)
            assert 'id' in detail
            assert 'labels' in detail

    def test_checkout_with_bad_authentication_fails(self):
        with self.app.test_request_context(
                "/", headers=dict(Authorization=self.invalid_auth)):
            response = self.circulation.borrow(
                self.data_source.name, self.identifier.identifier)
        eq_(401, response.status_code)
        eq_(OPDSAuthenticationDocument.MEDIA_TYPE, 
            response.headers['Content-Type'])
        detail = json.loads(response.data)
        assert 'id' in detail
        assert 'labels' in detail
        
    def test_checkout_success(self):
        with self.app.test_request_context(
                "/", headers=dict(Authorization=self.valid_auth)):
            response = self.circulation.borrow(
                self.data_source.name, self.identifier.identifier)

            # A loan has been created for this license pool.
            eq_(1, self._db.query(Loan).filter(Loan.license_pool==self.pool).count())

            # # We've been given an OPDS feed with one entry, which tells us how 
            # # to fulfill the license.
            # eq_(201, response.status_code)
            # feed = feedparser.parse(response.get_data())
            # [entry] = feed['entries']
            # fulfillment_link = [x for x in entry['links']
            #                    if x['rel'] == OPDSFeed.ACQUISITION_REL][0]
            # expect = url_for('fulfill', data_source=self.data_source.name,
            #                  identifier=self.identifier.identifier, _external=True)
            # eq_(expect, fulfillment_link['href'])

            # # Now let's try to fulfill the license.
            # response = self.circulation.fulfill(
            #     self.data_source.name, self.identifier.identifier)

            # We've been redirected to the download link.
            eq_(302, response.status_code)
            assert response.headers['Location'].startswith("http://foo.com/")


    # TODO: We have disabled this functionality so that we can see what
    # Overdrive books look like in the catalog.

    # def test_checkout_fails_when_no_available_licenses(self):
    #     pool = self.english_2.license_pools[0]
    #     pool.open_access = False
    #     edition = pool.edition
    #     data_source = edition.data_source
    #     identifier = edition.primary_identifier

    #     with self.app.test_request_context(
    #             "/", headers=dict(Authorization=self.valid_auth)):
    #         response = self.circulation.checkout(
    #             data_source.name, identifier.identifier)
    #         eq_(404, response.status_code)
    #         assert "Sorry, couldn't find an available license." in response.data
    #     pool.open_access = True
