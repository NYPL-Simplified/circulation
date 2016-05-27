# encoding: utf-8
from nose.tools import (
    eq_,
    set_trace,
)
import os
import json
import pkgutil

from overdrive import (
    OverdriveAPI,
    OverdriveRepresentationExtractor,
)

from model import (
    Contributor,
    DeliveryMechanism,
    Edition,
    Identifier,
    Representation,
    Subject,
    Measurement,
    Hyperlink,
)

class TestOverdriveAPI(object):

    def test_make_link_safe(self):
        eq_("http://foo.com?q=%2B%3A%7B%7D",
            OverdriveAPI.make_link_safe("http://foo.com?q=+:{}"))

class TestOverdriveRepresentationExtractor(object):

    def setup(self):
        base_path = os.path.split(__file__)[0]
        self.resource_path = os.path.join(base_path, "files", "overdrive")

    def sample_json(self, filename):
        path = os.path.join(self.resource_path, filename)
        data = open(path).read()
        return data, json.loads(data)

    def test_availability_info(self):
        data, raw = self.sample_json("overdrive_book_list.json")
        availability = OverdriveRepresentationExtractor.availability_link_list(
            raw)
        for item in availability:
            for key in 'availability_link', 'id', 'title':
                assert key in item

    def test_link(self):
        data, raw = self.sample_json("overdrive_book_list.json")
        expect = OverdriveAPI.make_link_safe("http://api.overdrive.com/v1/collections/collection-id/products?limit=300&offset=0&lastupdatetime=2014-04-28%2009:25:09&sort=popularity:desc&formats=ebook-epub-open,ebook-epub-adobe,ebook-pdf-adobe,ebook-pdf-open")
        eq_(expect, OverdriveRepresentationExtractor.link(raw, "first"))


    def test_book_info_with_circulationdata(self):
        # Tests that can convert an overdrive json block into a CirculationData object.

        raw, info = self.sample_json("overdrive_availability_information.json")
        circulationdata = OverdriveRepresentationExtractor.book_info_to_circulation(info)

        # Related IDs.
        eq_((Identifier.OVERDRIVE_ID, '2a005d55-a417-4053-b90d-7a38ca6d2065'),
            (circulationdata.primary_identifier.type, circulationdata.primary_identifier.identifier))



    def test_book_info_with_metadata(self):
        # Tests that can convert an overdrive json block into a Metadata object.

        raw, info = self.sample_json("overdrive_metadata.json")
        metadata = OverdriveRepresentationExtractor.book_info_to_metadata(info)

        eq_("Agile Documentation", metadata.title)
        eq_("Agile Documentation A Pattern Guide to Producing Lightweight Documents for Software Projects", metadata.sort_title)
        eq_("A Pattern Guide to Producing Lightweight Documents for Software Projects", metadata.subtitle)
        eq_(Edition.BOOK_MEDIUM, metadata.medium)
        eq_("Wiley Software Patterns", metadata.series)
        eq_("eng", metadata.language)
        eq_("Wiley", metadata.publisher)
        eq_("John Wiley & Sons, Inc.", metadata.imprint)
        eq_(2005, metadata.published.year)
        eq_(1, metadata.published.month)
        eq_(31, metadata.published.day)

        [author] = metadata.contributors
        eq_(u"Rüping, Andreas", author.sort_name)
        eq_("Andreas R&#252;ping", author.display_name)
        eq_([Contributor.AUTHOR_ROLE], author.roles)

        subjects = sorted(metadata.subjects, key=lambda x: x.identifier)

        eq_([("Computer Technology", Subject.OVERDRIVE, 100),
             ("Nonfiction", Subject.OVERDRIVE, 100),
             ('Object Technologies - Miscellaneous', 'tag', 1),
         ],
            [(x.identifier, x.type, x.weight) for x in subjects]
        )

        # Related IDs.
        eq_((Identifier.OVERDRIVE_ID, '3896665d-9d81-4cac-bd43-ffc5066de1f5'),
            (metadata.primary_identifier.type, metadata.primary_identifier.identifier))

        ids = [(x.type, x.identifier) for x in metadata.identifiers]

        # The original data contains a blank ASIN in addition to the
        # actual ASIN, but it doesn't show up here.
        eq_(
            [
                (Identifier.ASIN, "B000VI88N2"), 
                (Identifier.ISBN, "9780470856246"),
                (Identifier.OVERDRIVE_ID, '3896665d-9d81-4cac-bd43-ffc5066de1f5'),
            ],
            sorted(ids)
        )

        # Links to various resources.
        shortd, image, longd = sorted(
            metadata.links, key=lambda x:x.rel
        )

        eq_(Hyperlink.DESCRIPTION, longd.rel)
        assert longd.content.startswith("<p>Software documentation")

        eq_(Hyperlink.SHORT_DESCRIPTION, shortd.rel)
        assert shortd.content.startswith("<p>Software documentation")
        assert len(shortd.content) < len(longd.content)

        eq_(Hyperlink.IMAGE, image.rel)
        eq_('http://images.contentreserve.com/ImageType-100/0128-1/%7B3896665D-9D81-4CAC-BD43-FFC5066DE1F5%7DImg100.jpg', image.href)

        thumbnail = image.thumbnail

        eq_(Hyperlink.THUMBNAIL_IMAGE, thumbnail.rel)
        eq_('http://images.contentreserve.com/ImageType-200/0128-1/%7B3896665D-9D81-4CAC-BD43-FFC5066DE1F5%7DImg200.jpg', thumbnail.href)

        # Measurements associated with the book.

        measurements = metadata.measurements
        popularity = [x for x in measurements
                      if x.quantity_measured==Measurement.POPULARITY][0]
        eq_(2, popularity.value)

        rating = [x for x in measurements
                  if x.quantity_measured==Measurement.RATING][0]
        eq_(1, rating.value)


    def test_book_info_with_sample(self):
        raw, info = self.sample_json("has_sample.json")
        metadata = OverdriveRepresentationExtractor.book_info_to_metadata(info)
        [sample] = [x for x in metadata.links if x.rel == Hyperlink.SAMPLE]
        eq_("http://excerpts.contentreserve.com/FormatType-410/1071-1/9BD/24F/82/BridesofConvenienceBundle9781426803697.epub", sample.href)

    def test_book_info_with_grade_levels(self):
        raw, info = self.sample_json("has_grade_levels.json")
        metadata = OverdriveRepresentationExtractor.book_info_to_metadata(info)

        grade_levels = sorted(
            [x.identifier for x in metadata.subjects 
             if x.type==Subject.GRADE_LEVEL]
        )
        eq_([u'Grade 4', u'Grade 5', u'Grade 6', u'Grade 7', u'Grade 8'],
            grade_levels)

    def test_book_info_with_awards(self):
        raw, info = self.sample_json("has_awards.json")
        metadata = OverdriveRepresentationExtractor.book_info_to_metadata(info)

        [awards] = [x for x in metadata.measurements 
                    if Measurement.AWARDS == x.quantity_measured
        ]
        eq_(1, awards.value)
        eq_(1, awards.weight)
