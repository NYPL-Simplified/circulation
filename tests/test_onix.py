from nose.tools import (
    eq_,
    set_trace,
)
from StringIO import StringIO
import os

from . import sample_data

from api.onix import ONIXExtractor
from core.model import (
    Edition,
    Identifier,
)
from core.classifier import Classifier

class TestONIXExtractor(object):
    
    def sample_data(self, filename):
        return sample_data(filename, "onix")

    def test_parser(self):
        """Parse an ONIX file into Metadata objects."""

        file = self.sample_data("onix_example.xml")
        metadata_records = ONIXExtractor().parse(StringIO(file), "MIT Press")

        eq_(1, len(metadata_records))

        record = metadata_records[0]
        eq_("Safe Spaces, Brave Spaces", record.title)
        eq_("Diversity and Free Expression in Education", record.subtitle)
        eq_("Palfrey, John", record.contributors[0].sort_name)
        eq_("John Palfrey", record.contributors[0].display_name)
        eq_("Palfrey", record.contributors[0].family_name)
        assert "Head of School at Phillips Academy" in record.contributors[0].biography
        eq_("The MIT Press", record.publisher)
        eq_(None, record.imprint)
        eq_("9780262343664", record.primary_identifier.identifier)
        eq_(Identifier.ISBN, record.primary_identifier.type)
        eq_("eng", record.language)
        subjects = record.subjects
        eq_(7, len(subjects))
        eq_("EDU015000", subjects[0].identifier)
        eq_(Classifier.AUDIENCE_ADULT, subjects[-1].identifier)
        eq_(Classifier.BISAC, subjects[0].type)
        eq_(Edition.BOOK_MEDIUM, record.medium)
        eq_(2017, record.issued.year)

        eq_(1, len(record.links))
        assert "the essential democratic values of diversity and free expression" in record.links[0].content
