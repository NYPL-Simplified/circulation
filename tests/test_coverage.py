from nose.tools import (
    set_trace,
    eq_,
)
from testing import (
    DatabaseTest,
    AlwaysSuccessfulCoverageProvider,
    NeverSuccessfulCoverageProvider,
)
from model import (
    CoverageRecord,
    DataSource,
    Timestamp,
)

class TestCoverageProvider(DatabaseTest):

    def setup(self):
        super(TestCoverageProvider, self).setup()
        self.input_source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        self.output_source = DataSource.lookup(self._db, DataSource.OCLC)
        self.edition = self._edition(self.input_source.name)

    def test_ensure_coverage(self):

        provider = AlwaysSuccessfulCoverageProvider(
            "Always successful", self.input_source, self.output_source)
        result = provider.ensure_coverage(self.edition)

        # There is now one CoverageRecord -- the one returned by
        # ensure_coverage().
        [record] = self._db.query(CoverageRecord).all()
        eq_(self.edition.primary_identifier, record.identifier)
        eq_(self.output_source, self.output_source)
        eq_(record, result)

        # Timestamp was not updated.
        eq_([], self._db.query(Timestamp).all())

    def test_ensure_coverage_failure_returns_none(self):

        provider = NeverSuccessfulCoverageProvider(
            "Never successful", self.input_source, self.output_source)
        result = provider.ensure_coverage(self.edition)
        eq_(None, result)

        # Still no CoverageRecords.
        eq_([], self._db.query(CoverageRecord).all())

        # Timestamp was not updated.
        eq_([], self._db.query(Timestamp).all())

    def test_always_successful(self):

        # We start with no CoverageRecords and no Timestamp.
        eq_([], self._db.query(CoverageRecord).all())
        eq_([], self._db.query(Timestamp).all())

        provider = AlwaysSuccessfulCoverageProvider(
            "Always successful", self.input_source, self.output_source)
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
            "Never successful", self.input_source, self.output_source)
        provider.run()

        # There is still no CoverageRecord
        eq_([], self._db.query(CoverageRecord).all())

        # But the coverage provider did run, and the timestamp is now set.
        [timestamp] = self._db.query(Timestamp).all()
        eq_("Never successful", timestamp.service)
