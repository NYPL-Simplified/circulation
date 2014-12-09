# encoding: utf-8
from nose.tools import (
    eq_,
)
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

    def test_availability_info(self):
        data = pkgutil.get_data(
            "tests",
            "files/overdrive/overdrive_book_list.json")
        raw = json.loads(data)
        availability = OverdriveRepresentationExtractor.availability_link_list(
            raw)
        for item in availability:
            for key in 'availability_link', 'id', 'title':
                assert key in item

    def test_link(self):
        data = pkgutil.get_data(
            "tests",
            "files/overdrive/overdrive_book_list.json")
        raw = json.loads(data)
        expect = OverdriveAPI.make_link_safe("http://api.overdrive.com/v1/collections/collection-id/products?limit=300&offset=0&lastupdatetime=2014-04-28%2009:25:09&sort=popularity:desc&formats=ebook-epub-open,ebook-epub-adobe,ebook-pdf-adobe,ebook-pdf-open")
        eq_(expect, OverdriveRepresentationExtractor.link(raw, "first"))
