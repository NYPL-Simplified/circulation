from nose.tools import (
    eq_,
    set_trace,
)
import StringIO
import os

from . import sample_data

from api.marc import MARCExtractor
from core.model import (
    Edition,
    Identifier,
)

class TestMARCExtractor(object):
    
    def sample_data(self, filename):
        return sample_data(filename, "marc")

    def test_parser(self):
        """Parse a MARC file into Metadata objects."""

        file = self.sample_data("ils_plympton_01.mrc")
        metadata_records = MARCExtractor().parse(file, "Plympton")

        eq_(36, len(metadata_records))

        record = metadata_records[1]
        eq_("Strange Case of Dr Jekyll and Mr Hyde", record.title)
        assert "Stevenson" in record.contributors[0].sort_name
        assert "Recovering the Classics" in record.publisher
        eq_("9781682280041", record.primary_identifier.identifier)
        eq_(Identifier.ISBN, record.primary_identifier.type)
        subjects = record.subjects
        eq_(2, len(subjects))
        assert "Canon" in subjects[0].identifier
        eq_(Edition.BOOK_MEDIUM, record.medium)
        eq_(2015, record.issued.year)
        eq_('eng', record.language)

        eq_(1, len(record.links))
        assert "Utterson and Enfield are worried about their friend" in record.links[0].content
