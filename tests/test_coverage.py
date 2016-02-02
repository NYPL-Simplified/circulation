import contextlib
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
from config import (
    Configuration,
    temp_config as core_temp_config
)
from model import (
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
        eq_(None, record.exception)
        eq_(record, result)

        # The coverage provider's timestamp was not updated, because
        # we're using ensure_coverage.
        eq_([], self._db.query(Timestamp).all())

    def test_ensure_coverage_persistent_coverage_failure(self):

        provider = NeverSuccessfulCoverageProvider(
            "Never successful", self.input_source, self.output_source)
        record = provider.ensure_coverage(self.edition)

        assert isinstance(record, CoverageRecord)
        eq_(self.edition.primary_identifier, record.identifier)
        eq_("What did you expect?", record.exception)

        # The coverage provider's timestamp was not updated, because
        # we're using ensure_coverage.
        eq_([], self._db.query(Timestamp).all())

    def test_ensure_coverage_transient_coverage_failure(self):

        provider = TransientFailureCoverageProvider(
            "Transient failure", self.input_source, self.output_source)
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
            "Transient failure", self.input_source, self.output_source)
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

    def test_set_presentation_ready(self):
        provider = BibliographicCoverageProvider(self._db, None,
                DataSource.OVERDRIVE)
        identifier = self._identifier()
        test_metadata = self.BIBLIOGRAPHIC_DATA

        # Returns a CoverageFailure without metadata. (This is important
        # because the API call happens outside of the method, but isn't
        # necessarily checked.
        result = provider.set_presentation_ready(identifier, None)
        assert isinstance(result, CoverageFailure)
        eq_("Did not receive metadata from Overdrive", result.exception)

        # Returns a CoverageFailure if the identifier doesn't have a
        # license pool.
        result = provider.set_presentation_ready(identifier, test_metadata)
        assert isinstance(result, CoverageFailure)
        eq_("No license pool available", result.exception)

        # Returns a CoverageFailure if there's no work available.
        edition, lp = self._edition(with_license_pool=True)
        # Remove edition so that the work won't be calculated
        lp.identifier.primarily_identifies = []
        result = provider.set_presentation_ready(lp.identifier, test_metadata)
        assert isinstance(result, CoverageFailure)
        eq_("Work could not be calculated", result.exception)

        # Returns the identifier itself if all is well.
        ed, lp = self._edition(with_license_pool=True)
        result = provider.set_presentation_ready(lp.identifier, test_metadata)
        eq_(lp.identifier, result)

        # Catches other errors during the process and returns them as (the
        # ever-superior) CoverageFailures.
        edition, lp = self._edition(with_license_pool=True)
        # This call raises a ValueError because the primary identifier &
        # the edition's primary identifier don't match.
        test_metadata.primary_identifier = identifier
        result = provider.set_presentation_ready(lp.identifier, test_metadata)
        assert isinstance(result, CoverageFailure)
        assert "ValueError" in result.exception

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
