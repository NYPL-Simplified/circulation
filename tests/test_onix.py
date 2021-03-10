from io import StringIO

from nose.tools import (
    eq_,
)
from parameterized import parameterized

from api.onix import ONIXExtractor
from core.classifier import Classifier
from core.metadata_layer import CirculationData
from core.model import (
    Classification,
    Edition,
    Identifier,
    LicensePool)
from . import sample_data


class TestONIXExtractor(object):

    def sample_data(self, filename):
        return sample_data(filename, "onix")

    def test_parser(self):
        """Parse an ONIX file into Metadata objects."""

        file = self.sample_data("onix_example.xml")
        metadata_records = ONIXExtractor().parse(StringIO(file), "MIT Press")

        eq_(2, len(metadata_records))

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
        eq_(Classification.TRUSTED_DISTRIBUTOR_WEIGHT, subjects[0].weight)
        eq_(Edition.BOOK_MEDIUM, record.medium)
        eq_(2017, record.issued.year)

        eq_(1, len(record.links))
        assert "the essential democratic values of diversity and free expression" in record.links[0].content

        record = metadata_records[1]
        eq_(Edition.AUDIO_MEDIUM, record.medium)

    @parameterized.expand([
        (
                'limited_usage_status',
                'onix_3_usage_constraints_example.xml',
                20
        ),
        (
                'unlimited_usage_status',
                'onix_3_usage_constraints_with_unlimited_usage_status.xml',
                LicensePool.UNLIMITED_ACCESS
        ),
        (
                'wrong_usage_unit',
                'onix_3_usage_constraints_example_with_day_usage_unit.xml',
                LicensePool.UNLIMITED_ACCESS
        )
    ])
    def test_parse_parses_correctly_onix_3_usage_constraints(self, _, file_name, licenses_number):
        # Arrange
        file = self.sample_data(file_name)

        # Act
        metadata_records = ONIXExtractor().parse(StringIO(file), 'ONIX 3 Usage Constraints Example')

        # Assert
        eq_(len(metadata_records), 1)

        [metadata_record] = metadata_records

        eq_(metadata_record.circulation is not None, True)
        eq_(isinstance(metadata_record.circulation, CirculationData), True)
        eq_(isinstance(metadata_record.circulation, CirculationData), True)
        eq_(metadata_record.circulation.licenses_owned, licenses_number)
        eq_(metadata_record.circulation.licenses_available, licenses_number)
