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

    def setup(self):
        super(TestS3URLGeneration, self).setup()
        self.c = "test-book-covers-s3-bucket"
        self.o = "test-open-access-s3-bucket"
        self.old_book_covers = os.environ.get('BOOK_COVERS_S3_BUCKET')
        self.old_open_access = os.environ.get('OPEN_ACCESS_CONTENT_S3_BUCKET')
        os.environ['BOOK_COVERS_S3_BUCKET'] = self.c
        os.environ['OPEN_ACCESS_CONTENT_S3_BUCKET'] = self.o

    def teardown(self):
        if self.old_book_covers:
            os.environ['BOOK_COVERS_S3_BUCKET'] = self.old_book_covers
        else:
            del os.environ['BOOK_COVERS_S3_BUCKET']
        if self.old_open_access:
            os.environ['OPEN_ACCESS_CONTENT_S3_BUCKET'] = self.old_open_access
        else:
            del os.environ['OPEN_ACCESS_CONTENT_S3_BUCKET']

    def test_content_root(self):
        eq_("http://s3.amazonaws.com/test-open-access-s3-bucket/",
            S3Uploader.content_root())

    def test_cover_image_root(self):
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
