from io import BytesIO

from parameterized import parameterized

from api.onix import ONIXExtractor
from core.classifier import Classifier
from core.metadata_layer import CirculationData
from core.model import (
    Classification,
    Edition,
    Identifier,
    LicensePool)
from core.util.datetime_helpers import datetime_utc
from . import sample_data


class TestONIXExtractor(object):

    def sample_data(self, filename):
        return sample_data(filename, "onix")

    def test_parser(self):
        """Parse an ONIX file into Metadata objects."""

        file = self.sample_data("onix_example.xml")
        metadata_records = ONIXExtractor().parse(BytesIO(file), "MIT Press")

        assert 2 == len(metadata_records)

        record = metadata_records[0]
        assert "Safe Spaces, Brave Spaces" == record.title
        assert "Diversity and Free Expression in Education" == record.subtitle
        assert "Palfrey, John" == record.contributors[0].sort_name
        assert "John Palfrey" == record.contributors[0].display_name
        assert "Palfrey" == record.contributors[0].family_name
        assert "Head of School at Phillips Academy" in record.contributors[0].biography
        assert "The MIT Press" == record.publisher
        assert None == record.imprint
        assert "9780262343664" == record.primary_identifier.identifier
        assert Identifier.ISBN == record.primary_identifier.type
        assert "eng" == record.language
        assert datetime_utc(2017, 10, 6) == record.issued
        subjects = record.subjects
        assert 7 == len(subjects)
        assert "EDU015000" == subjects[0].identifier
        assert Classifier.AUDIENCE_ADULT == subjects[-1].identifier
        assert Classifier.BISAC == subjects[0].type
        assert Classification.TRUSTED_DISTRIBUTOR_WEIGHT == subjects[0].weight
        assert Edition.BOOK_MEDIUM == record.medium
        assert 2017 == record.issued.year

        assert 1 == len(record.links)
        assert "the essential democratic values of diversity and free expression" in record.links[0].content

        record = metadata_records[1]
        assert Edition.AUDIO_MEDIUM == record.medium

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
        metadata_records = ONIXExtractor().parse(BytesIO(file), 'ONIX 3 Usage Constraints Example')

        # Assert
        assert len(metadata_records) == 1

        [metadata_record] = metadata_records

        assert (metadata_record.circulation is not None) == True
        assert isinstance(metadata_record.circulation, CirculationData) == True
        assert isinstance(metadata_record.circulation, CirculationData) == True
        assert metadata_record.circulation.licenses_owned == licenses_number
        assert metadata_record.circulation.licenses_available == licenses_number
