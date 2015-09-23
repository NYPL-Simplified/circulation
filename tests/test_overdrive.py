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


    def test_book_info_with_metadata(self):

        raw, info = self.sample_json("overdrive_metadata.json")
        metadata = OverdriveRepresentationExtractor.book_info_to_metadata(info)
        set_trace()

        eq_("Agile Documentation", metadata.title)
        eq_("A Pattern Guide to Producing Lightweight Documents for Software Projects", metadata.subtitle)
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

        eq_(set(["Computer Technology", "Nonfiction"]),
            set([c.identifier for c in metadata.subjects])
        )
        eq_(["Overdrive", "Overdrive"],
            [c.type for c in metadata.subjects]
        )
        eq_([100, 100],
            [c.weight for c in metadata.subjects]
        )

        # Related IDs.
        eq_((Identifier.OVERDRIVE_ID, '3896665d-9d81-4cac-bd43-ffc5066de1f5'),
            (metadata.primary_identifier.type, metadata.primary_identifier.identifier))

        ids = [(x.type, x.identifier) for x in metadata.identifiers]
        eq_([("ASIN", "B000VI88N2"), ("ISBN", "9780470856246")],
            sorted(ids))

        # Associated resources.
        links = wr.primary_identifier.links
        eq_(3, len(links))
        long_description = [
            x.resource.representation for x in links
            if x.rel==Hyperlink.DESCRIPTION
        ][0]
        assert long_description.content.startswith("<p>Software documentation")

        short_description = [
            x.resource.representation for x in links
            if x.rel==Hyperlink.SHORT_DESCRIPTION
        ][0]
        assert short_description.content.startswith("<p>Software documentation")
        assert len(short_description.content) < len(long_description.content)

        image = [x.resource for x in links if x.rel==Hyperlink.IMAGE][0]
        eq_('http://images.contentreserve.com/ImageType-100/0128-1/%7B3896665D-9D81-4CAC-BD43-FFC5066DE1F5%7DImg100.jpg', image.url)

        measurements = wr.primary_identifier.measurements
        popularity = [x for x in measurements
                      if x.quantity_measured==Measurement.POPULARITY][0]
        eq_(2, popularity.value)

        rating = [x for x in measurements
                  if x.quantity_measured==Measurement.RATING][0]
        eq_(1, rating.value)

        # Un-schematized metadata.

        eq_(Edition.BOOK_MEDIUM, wr.medium)
        eq_("Agile Documentation A Pattern Guide to Producing Lightweight Documents for Software Projects", wr.sort_title)


    def test_book_info_with_sample(self):
        raw, info = self.sample_json("has_sample.json")
        metadata = OverdriveRepresentationExtractor.book_info_to_metadata(info)
        [sample] = [x for x in i.links if x.rel == Hyperlink.SAMPLE]
        eq_("http://excerpts.contentreserve.com/FormatType-410/1071-1/9BD/24F/82/BridesofConvenienceBundle9781426803697.epub", sample.resource.url)

    def test_book_info_with_awards(self):
        raw, info = self.sample_json("has_awards.json")
        metadata = OverdriveRepresentationExtractor.book_info_to_metadata(info)

        [awards] = [x for x in metadata.measurements 
                    if Measurement.AWARDS == x.quantity_measured
        ]
        eq(1, awards.value)
        eq(1, awards.weight)
