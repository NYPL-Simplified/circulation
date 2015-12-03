# encoding: utf-8
"""Test the Flask app for the circulation server."""

import re
import base64
import feedparser
import random
import json
import os
import urllib
from lxml import etree
from ..millenium_patron import DummyMilleniumPatronAPI
from flask import url_for

from nose.tools import (
    eq_,
    set_trace,
)

from . import (
    DatabaseTest,
)

from ..circulation import (
    CirculationAPI,
)

from ..config import (
    temp_config,
    Configuration,
)
from ..core.model import (
    get_one,
    Complaint,
    CustomList,
    DataSource,
    DeliveryMechanism,
    Loan,
    Patron,
    Representation,
    Resource,
    Edition,
    SessionManager,
)

from ..core.lane import (
    LaneList,
)

from ..core.opds import (
    AcquisitionFeed,
    OPDSFeed,
)
from ..core.util.opds_authentication_document import (
    OPDSAuthenticationDocument
)

from ..opds import (
    CirculationManagerAnnotator,
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
                  sublanes=["Contemporary Romance"])
         ]
        )

        circulation.Conf.initialize(self._db, self.lanes)
        self.circulation = circulation
        circulation.Conf.configuration = Configuration
        self.app = circulation.app
        self.client = circulation.app.test_client()

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

        circulation = CirculationAPI(
            self._db, overdrive=overdrive, threem=threem)

        # Sync the bookshelf so we can create works for the loans.
        circulation.sync_bookshelf(patron, "dummy pin")

        # Super hacky--make sure the loans and holds have works that
        # will show up in the feed.
        for l in [patron.loans, patron.holds]:
            for loan in l:
                pool = loan.license_pool
                pool.set_delivery_mechanism(Representation.EPUB_MEDIA_TYPE,
                                            DeliveryMechanism.ADOBE_DRM, None)
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
            a = re.compile('<opds:availability[^>]+status="available"', re.S)
            assert a.search(response.data)
            for loan in patron.loans:
                expect_title = loan.license_pool.work.title
                assert "title>%s</title" % expect_title in response.data

            a = re.compile('<opds:availability[^>]+status="reserved"', re.S)
            assert a.search(response.data)
            for hold in patron.holds:
                expect_title = hold.license_pool.work.title
                assert "title>%s</title" % expect_title in response.data

            a = re.compile('<opds:availability[^>]+status="ready"', re.S)
            assert a.search(response.data)

            # Each entry must have a 'revoke' link, except for the 3M
            # ready book, which does not.
            feed = feedparser.parse(response.data)
            for entry in feed['entries']:
                revoke_link = [x for x in entry['links']
                               if x['rel'] == OPDSFeed.REVOKE_LOAN_REL]
                if revoke_link == []:
                    eq_(entry['opds_availability']['status'], 'ready')
                    assert "3M" in entry['id']
                else:
                    assert revoke_link
