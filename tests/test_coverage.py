import datetime
from nose.tools import (
    set_trace,
    eq_,
)
from . import (
    DatabaseTest
)
from testing import (
    AlwaysSuccessfulCoverageProvider,
    NeverSuccessfulCoverageProvider,
    TransientFailureCoverageProvider,
)
from model import (
    Edition,
    Identifier,
    Contributor,
    Subject,
    CoverageRecord,
    DataSource,
    Timestamp,
)
from metadata_layer import (
    Metadata,
    IdentifierData,
    ContributorData,
    SubjectData,
)
from coverage import (
    BibliographicCoverageProvider,
    CoverageFailure,
)

class TestCoverageProvider(DatabaseTest):

    def setup(self):
        super(TestCoverageProvider, self).setup()
        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        self.input_identifier_types = gutenberg.primary_identifier_type
        self.output_source = DataSource.lookup(self._db, DataSource.OCLC)
        self.edition = self._edition(gutenberg.name)
        self.identifier = self.edition.primary_identifier

    def test_ensure_coverage(self):

        provider = AlwaysSuccessfulCoverageProvider(
            "Always successful", self.input_identifier_types, self.output_source
        )
        result = provider.ensure_coverage(self.edition)

        # There is now one CoverageRecord -- the one returned by
        # ensure_coverage().
        [record] = self._db.query(CoverageRecord).all()
        eq_(self.edition.primary_identifier, record.identifier)
        eq_(self.output_source, self.output_source)
        eq_(None, record.exception)
        eq_(record, result)

        # The coverage provider's timestamp was not updated, because
        # we're using ensure_coverage.
        eq_([], self._db.query(Timestamp).all())

    def test_ensure_coverage_persistent_coverage_failure(self):

        provider = NeverSuccessfulCoverageProvider(
            "Never successful", self.input_identifier_types, self.output_source
        )
        record = provider.ensure_coverage(self.edition)

        assert isinstance(record, CoverageRecord)
        eq_(self.edition.primary_identifier, record.identifier)
        eq_("What did you expect?", record.exception)

        # The coverage provider's timestamp was not updated, because
        # we're using ensure_coverage.
        eq_([], self._db.query(Timestamp).all())

    def test_ensure_coverage_transient_coverage_failure(self):

        provider = TransientFailureCoverageProvider(
            "Transient failure", self.input_identifier_types, self.output_source
        )
        result = provider.ensure_coverage(self.edition)
        eq_(None, result)

        # Because the error is transient we have no coverage record.
        eq_([], self._db.query(CoverageRecord).all())

        # Timestamp was not updated.
        eq_([], self._db.query(Timestamp).all())

    def test_always_successful(self):

        # We start with no CoverageRecords and no Timestamp.
        eq_([], self._db.query(CoverageRecord).all())
        eq_([], self._db.query(Timestamp).all())

        provider = AlwaysSuccessfulCoverageProvider(
            "Always successful", self.input_identifier_types, self.output_source
        )
        provider.run()

        # There is now one CoverageRecord
        [record] = self._db.query(CoverageRecord).all()
        eq_(self.edition.primary_identifier, record.identifier)
        eq_(self.output_source, self.output_source)

        # The timestamp is now set.
        [timestamp] = self._db.query(Timestamp).all()
        eq_("Always successful", timestamp.service)


    def test_never_successful(self):

        # We start with no CoverageRecords and no Timestamp.
        eq_([], self._db.query(CoverageRecord).all())
        eq_([], self._db.query(Timestamp).all())

        provider = NeverSuccessfulCoverageProvider(
            "Never successful", self.input_identifier_types, self.output_source
        )
        provider.run()

        # We have a CoverageRecord that signifies failure.
        [record] = self._db.query(CoverageRecord).all()
        eq_(self.edition.primary_identifier, record.identifier)
        eq_(self.output_source, self.output_source)
        eq_("What did you expect?", record.exception)

        # But the coverage provider did run, and the timestamp is now set.
        [timestamp] = self._db.query(Timestamp).all()
        eq_("Never successful", timestamp.service)

    def test_transient_failure(self):

        # We start with no CoverageRecords and no Timestamp.
        eq_([], self._db.query(CoverageRecord).all())
        eq_([], self._db.query(Timestamp).all())

        provider = TransientFailureCoverageProvider(
            "Transient failure", self.input_identifier_types, self.output_source
        )
        provider.run()

        # We have no CoverageRecord, since the error was transient.
        eq_([], self._db.query(CoverageRecord).all())

        # But the coverage provider did run, and the timestamp is now set.
        [timestamp] = self._db.query(Timestamp).all()
        eq_("Transient failure", timestamp.service)


class TestBibliographicCoverageProvider(DatabaseTest):

    BIBLIOGRAPHIC_DATA = Metadata(
        DataSource.OVERDRIVE,
        publisher=u'Perfection Learning',
        language='eng',
        title=u'A Girl Named Disaster',
        published=datetime.datetime(1998, 3, 1, 0, 0),
        identifiers = [
            IdentifierData(
                    type=Identifier.OVERDRIVE_ID,
                    identifier=u'ba9b3419-b0bd-4ca7-a24f-26c4246b6b44'
                ),
            IdentifierData(type=Identifier.ISBN, identifier=u'9781402550805')
        ],
        contributors = [
            ContributorData(sort_name=u"Nancy Farmer",
                            roles=[Contributor.PRIMARY_AUTHOR_ROLE])
        ],
        subjects = [
            SubjectData(type=Subject.TOPIC,
                        identifier=u'Action & Adventure'),
            SubjectData(type=Subject.FREEFORM_AUDIENCE,
                        identifier=u'Young Adult'),
            SubjectData(type=Subject.PLACE, identifier=u'Africa')
        ],
    )

    def test_edition(self):
        provider = BibliographicCoverageProvider(self._db, None,
                DataSource.OVERDRIVE)
        identifier = self._identifier()
        test_metadata = self.BIBLIOGRAPHIC_DATA

        # Returns a CoverageFailure if the identifier doesn't have a
        # license pool.
        result = provider.work(identifier)
        assert isinstance(result, CoverageFailure)
        eq_("No license pool available", result.exception)

        # Returns an Edition otherwise, creating it if necessary.
        edition, lp = self._edition(with_license_pool=True)
        identifier = edition.primary_identifier
        eq_(edition, provider.edition(identifier))

        # The Edition will be created if necessary.
        lp.identifier.primarily_identifies = []
        e2 = provider.edition(identifier)
        assert edition != e2
        assert isinstance(e2, Edition)

    def test_work(self):
        provider = BibliographicCoverageProvider(self._db, None,
                DataSource.OVERDRIVE)
        identifier = self._identifier()
        test_metadata = self.BIBLIOGRAPHIC_DATA

        # Returns a CoverageFailure if the identifier doesn't have a
        # license pool.
        result = provider.work(identifier)
        assert isinstance(result, CoverageFailure)
        eq_("No license pool available", result.exception)

        # Returns a CoverageFailure if there's no work available.
        edition, lp = self._edition(with_license_pool=True)
        # Remove edition so that the work won't be calculated
        lp.identifier.primarily_identifies = []
        result = provider.work(lp.identifier)
        assert isinstance(result, CoverageFailure)
        eq_("Work could not be calculated", result.exception)

        # Returns the work if it can be created or found.
        ed, lp = self._edition(with_license_pool=True)
        result = provider.work(lp.identifier)
        eq_(result, lp.work)

    def test_set_metadata(self):
        provider = BibliographicCoverageProvider(self._db, None,
                DataSource.OVERDRIVE)
        identifier = self._identifier()
        test_metadata = self.BIBLIOGRAPHIC_DATA

        # If the work can't be found, a CoverageRecord results.
        result = provider.work(identifier)
        assert isinstance(result, CoverageFailure)
        eq_("No license pool available", result.exception)

        edition, lp = self._edition(with_license_pool=True)

        # If no metadata is passed in, a CoverageRecord results.
        result = provider.set_metadata(edition.primary_identifier, None)
        assert isinstance(result, CoverageFailure)
        eq_("Did not receive metadata from input source", result.exception)

        # If no work can be created (in this case, because there's no title),
        # a CoverageFailure results.
        edition.title = None
        old_title = test_metadata.title
        test_metadata.title = None
        result = provider.set_metadata(edition.primary_identifier, test_metadata)
        assert isinstance(result, CoverageFailure)
        eq_("Work could not be calculated", result.exception)
        test_metadata.title = old_title        

        # Test success
        result = provider.set_metadata(edition.primary_identifier, test_metadata)
        eq_(result, edition.primary_identifier)

        # If there's an exception setting the metadata, a
        # CoverageRecord results. This call raises a ValueError
        # because the primary identifier & the edition's primary
        # identifier don't match.
        test_metadata.primary_identifier = self._identifier()
        result = provider.set_metadata(lp.identifier, test_metadata)
        assert isinstance(result, CoverageFailure)
        assert "ValueError" in result.exception

    def test_set_presentation_ready(self):
        provider = BibliographicCoverageProvider(self._db, None,
                DataSource.OVERDRIVE)
        identifier = self._identifier()
        test_metadata = self.BIBLIOGRAPHIC_DATA

        # If the work can't be found, it can't be made presentation ready.
        result = provider.set_presentation_ready(identifier)
        assert isinstance(result, CoverageFailure)
        eq_("No license pool available", result.exception)

        # Test success.
        ed, lp = self._edition(with_license_pool=True)
        result = provider.set_presentation_ready(ed.primary_identifier)
        eq_(result, ed.primary_identifier)

