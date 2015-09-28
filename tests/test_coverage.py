import contextlib
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
)
from config import (
    Configuration,
    temp_config as core_temp_config
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

# TODO: This really should be in its own file but there's a problem
# with the correct syntax for importing DatabaseTest which I can't fix
# right now.

import os
from s3 import S3Uploader

class TestS3URLGeneration(DatabaseTest):
    
    @contextlib.contextmanager
    def temp_config(self):
        with core_temp_config() as tmp:
            i = tmp['integrations']
            S3 = Configuration.S3_INTEGRATION
            i[S3] = {
                Configuration.S3_OPEN_ACCESS_CONTENT_BUCKET : 'test-open-access-s3-bucket',
                Configuration.S3_BOOK_COVERS_BUCKET : 'test-book-covers-s3-bucket'
            }
            yield tmp

    def test_content_root(self):
        with self.temp_config():
            eq_("http://s3.amazonaws.com/test-open-access-s3-bucket/",
                S3Uploader.content_root())

    def test_cover_image_root(self):
        with self.temp_config():
            gutenberg_illustrated = DataSource.lookup(
                self._db, DataSource.GUTENBERG_COVER_GENERATOR)
            overdrive = DataSource.lookup(
                self._db, DataSource.OVERDRIVE)
            eq_("http://s3.amazonaws.com/test-book-covers-s3-bucket/Gutenberg%20Illustrated/",
                S3Uploader.cover_image_root(gutenberg_illustrated))
            eq_("http://s3.amazonaws.com/test-book-covers-s3-bucket/Overdrive/",
                S3Uploader.cover_image_root(overdrive))
            eq_("http://s3.amazonaws.com/test-book-covers-s3-bucket/scaled/300/Overdrive/", 
                S3Uploader.cover_image_root(overdrive, 300))
