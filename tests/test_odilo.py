# encoding: utf-8
from nose.tools import (
    eq_, ok_
)

import os
import json

from odilo import (
    MockOdiloAPI,
    OdiloRepresentationExtractor,
    OdiloBibliographicCoverageProvider
)

from model import (
    Contributor,
    DeliveryMechanism,
    Edition,
    Identifier,
    Representation,
    Subject,
    Hyperlink,
)

from testing import DatabaseTest


class OdiloTest(DatabaseTest):
    def setup(self):
        super(OdiloTest, self).setup()
        self.collection = MockOdiloAPI.mock_collection(self._db)
        base_path = os.path.split(__file__)[0]
        self.resource_path = os.path.join(base_path, "files", "odilo")

    def sample_json(self, filename):
        path = os.path.join(self.resource_path, filename)
        data = open(path).read()
        return data, json.loads(data)


class OdiloTestWithAPI(OdiloTest):
    """Automatically create a MockOdiloAPI class during setup.
    """

    def setup(self):
        super(OdiloTestWithAPI, self).setup()
        self.api = MockOdiloAPI(self._db, self.collection)


class TestOdiloBibliographicCoverageProvider(OdiloTest):
    def setup(self):
        super(TestOdiloBibliographicCoverageProvider, self).setup()
        self.provider = OdiloBibliographicCoverageProvider(
            self.collection, api_class=MockOdiloAPI
        )
        self.api = self.provider.api

    def test_process_item(self):
        print 'Testing process item...'
        record_metadata, record_metadata_json = self.sample_json("odilo_metadata.json")
        self.api.queue_response(200, content=record_metadata_json)
        availability, availability_json = self.sample_json("odilo_availability.json")
        self.api.queue_response(200, content=availability)

        identifier, made_new = self.provider.process_item('00010982')
        ok_(identifier, msg="Problem testing process item !!!")
        print 'Testing process finished ok !!'


class TestOdiloRepresentationExtractor(OdiloTestWithAPI):
    def test_book_info_with_metadata(self):
        # Tests that can convert an odilo json block into a Metadata object.

        raw, book_json = self.sample_json("odilo_metadata.json")
        raw, availability = self.sample_json("odilo_availability.json")
        metadata, active = OdiloRepresentationExtractor.record_info_to_metadata(book_json, availability)

        eq_("Busy Brownies", metadata.title)
        eq_(" (The Classic Fantasy Literature of Elves for Children)", metadata.subtitle)
        eq_("eng", metadata.language)
        eq_(Edition.BOOK_MEDIUM, metadata.medium)
        eq_("The Classic Fantasy Literature for Children written in 1896 retold for Elves adventure.", metadata.series)
        eq_("1", metadata.series_position)
        eq_("ANBOCO", metadata.publisher)
        eq_(2013, metadata.published.year)
        eq_(02, metadata.published.month)
        eq_(02, metadata.published.day)
        eq_(2017, metadata.data_source_last_updated.year)
        eq_(03, metadata.data_source_last_updated.month)
        eq_(10, metadata.data_source_last_updated.day)
        # Related IDs.
        eq_((Identifier.ODILO_ID, '00010982'),
            (metadata.primary_identifier.type, metadata.primary_identifier.identifier))
        ids = [(x.type, x.identifier) for x in metadata.identifiers]
        eq_(
            [
                (Identifier.ISBN, '9783736418837'),
                (Identifier.ODILO_ID, '00010982')
            ],
            sorted(ids)
        )

        subjects = sorted(metadata.subjects, key=lambda x: x.identifier)
        eq_([('Children', Subject.TAG, 100),
             ('Classics', Subject.TAG, 100),
             ('Fantasy', Subject.TAG, 100),
             ('K-12', Subject.GRADE_LEVEL, 10),
             ],
            [(x.identifier, x.type, x.weight) for x in subjects]
            )

        [author] = metadata.contributors
        eq_("E. Veale", author.sort_name)
        eq_("E. Veale", author.display_name)
        eq_([Contributor.AUTHOR_ROLE], author.roles)

        # Available formats.
        [acsm, ebook_streaming] = sorted(metadata.circulation.formats, key=lambda x: x.content_type)
        eq_(Representation.PDF_MEDIA_TYPE, acsm.content_type)
        eq_(DeliveryMechanism.ADOBE_DRM, acsm.drm_scheme)

        eq_(Representation.TEXT_HTML_MEDIA_TYPE, ebook_streaming.content_type)
        eq_(DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE, ebook_streaming.drm_scheme)

        # Links to various resources.
        image, description = sorted(metadata.links, key=lambda x: x.rel)

        eq_(Hyperlink.IMAGE, image.rel)
        eq_(
            'http://pruebasotk.odilotk.es/public/OdiloPlace_eduDistUS/pg54159_225x318.jpg',
            image.href)

        eq_(Hyperlink.DESCRIPTION, description.rel)
        assert description.content.startswith("All the Brownies had promised to help, and when a Brownie undertakes")

        circulation = metadata.circulation
        eq_(2, circulation.licenses_owned)
        eq_(1, circulation.licenses_available)
        eq_(2, circulation.patrons_in_hold_queue)
        eq_(1, circulation.licenses_reserved)
