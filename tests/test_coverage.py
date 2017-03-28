import datetime
from nose.tools import (
    assert_raises,
    assert_raises_regexp,
    set_trace,
    eq_,
)
from . import (
    DatabaseTest
)
from testing import (
    AlwaysSuccessfulCoverageProvider,
    AlwaysSuccessfulWorkCoverageProvider,
    DummyHTTPClient,
    TaskIgnoringCoverageProvider,
    NeverSuccessfulWorkCoverageProvider,
    NeverSuccessfulCoverageProvider,
    TransientFailureCoverageProvider,
    TransientFailureWorkCoverageProvider,
)
from model import (
    Collection,
    CollectionMissing,
    Contributor,
    CoverageRecord,
    DataSource,
    DeliveryMechanism,
    Edition,
    Hyperlink,
    Identifier,
    PresentationCalculationPolicy,
    Representation,
    RightsStatus,
    Subject,
    Timestamp,
    Work,
    WorkCoverageRecord,
)
from metadata_layer import (
    Metadata,
    CirculationData,
    FormatData,
    IdentifierData,
    ContributorData,
    LinkData,
    ReplacementPolicy,
    SubjectData,
)
from s3 import DummyS3Uploader
from coverage import (
    BaseCoverageProvider,
    BibliographicCoverageProvider,
    CollectionCoverageProvider,
    CoverageFailure,
    IdentifierCoverageProvider,
)

class TestCoverageFailure(DatabaseTest):
    """Test the CoverageFailure class."""
    
    def test_to_coverage_record(self):
        source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        identifier = self._identifier()

        transient_failure = CoverageFailure(
            identifier, "Bah!", data_source=source, transient=True
        )
        rec = transient_failure.to_coverage_record(operation="the_operation")
        assert isinstance(rec, CoverageRecord)
        eq_(identifier, rec.identifier)
        eq_(source, rec.data_source)
        eq_("the_operation", rec.operation)
        eq_(CoverageRecord.TRANSIENT_FAILURE, rec.status)
        eq_("Bah!", rec.exception)

        persistent_failure = CoverageFailure(
            identifier, "Bah forever!", data_source=source, transient=False
        )
        rec = persistent_failure.to_coverage_record(operation="the_operation")
        eq_(CoverageRecord.PERSISTENT_FAILURE, rec.status)
        eq_("Bah forever!", rec.exception)        

    def test_to_work_coverage_record(self):
        work = self._work()

        transient_failure = CoverageFailure(
            work, "Bah!", transient=True
        )
        rec = transient_failure.to_work_coverage_record("the_operation")
        assert isinstance(rec, WorkCoverageRecord)
        eq_(work, rec.work)
        eq_("the_operation", rec.operation)
        eq_(CoverageRecord.TRANSIENT_FAILURE, rec.status)
        eq_("Bah!", rec.exception)

        persistent_failure = CoverageFailure(
            work, "Bah forever!", transient=False
        )
        rec = persistent_failure.to_work_coverage_record(
            operation="the_operation"
        )
        eq_(CoverageRecord.PERSISTENT_FAILURE, rec.status)
        eq_("Bah forever!", rec.exception)        


class CoverageProviderTest(DatabaseTest):
    BIBLIOGRAPHIC_DATA = Metadata(
        DataSource.OVERDRIVE,
        publisher=u'Perfection Learning',
        language='eng',
        title=u'A Girl Named Disaster',
        published=datetime.datetime(1998, 3, 1, 0, 0),
        primary_identifier=IdentifierData(
            type=Identifier.OVERDRIVE_ID,
            identifier=u'ba9b3419-b0bd-4ca7-a24f-26c4246b6b44'
        ),
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


class TestBaseCoverageProvider(CoverageProviderTest):

    def test_instantiation(self):
        """Verify variable initialization."""

        class ValidMock(BaseCoverageProvider):
            SERVICE_NAME = "A Service"
            OPERATION = "An Operation"
            DEFAULT_BATCH_SIZE = 50

        now = cutoff_time=datetime.datetime.utcnow()
        provider = ValidMock(self._db, cutoff_time=now)
    
        # Class variables defined in subclasses become appropriate
        # instance variables.
        eq_("A Service (An Operation)", provider.service_name)
        eq_("An Operation", provider.operation)
        eq_(50, provider.batch_size)
        eq_(now, provider.cutoff_time)
        
        # If you pass in an invalid value for batch_size, you get the default.
        provider = ValidMock(self._db, batch_size=-10)
        eq_(50, provider.batch_size)

    def test_subclass_must_define_service_name(self):
        class NoServiceName(BaseCoverageProvider):
            pass

        assert_raises_regexp(
            ValueError, "NoServiceName must define SERVICE_NAME",
            NoServiceName, self._db
        )

    def test_run(self):
        """Verify that run() calls run_once_and_update_timestamp()."""
        class MockCoverageProvider(BaseCoverageProvider):
            SERVICE_NAME = "I do nothing"
            was_run = False

            def run_once_and_update_timestamp(self):
                self.was_run = True

        provider = MockCoverageProvider(self._db)
        provider.run()
        eq_(True, provider.was_run)
        
    def test_run_once_and_update_timestamp(self):
        """Test that run_once_and_update_timestamp calls run_once twice and
        then updates a Timestamp.
        """
        class MockCoverageProvider(BaseCoverageProvider):
            SERVICE_NAME = "I do nothing"
            run_once_calls = []
            
            def run_once(self, offset, count_as_covered=None):
                self.run_once_calls.append(count_as_covered)
            
        # We start with no timestamps.
        eq_([], self._db.query(Timestamp).all())
        
        # Instantiate the Provider, and call
        # run_once_and_update_timestamp.
        provider = MockCoverageProvider(self._db)
        provider.run_once_and_update_timestamp()

        # The timestamp is now set.
        [timestamp] = self._db.query(Timestamp).all()
        eq_("I do nothing", timestamp.service)

        # run_once was called twice: once to exclude items that have
        # any coverage record whatsoever (ALL_STATUSES), and again to
        # exclude only items that have coverage records that indicate
        # success or persistent failure (DEFAULT_COUNT_AS_COVERED).
        eq_([CoverageRecord.ALL_STATUSES,
             CoverageRecord.DEFAULT_COUNT_AS_COVERED], provider.run_once_calls)
        
    def test_run_once(self):
        """Test run_once, showing how it covers items with different types of
        CoverageRecord.

        TODO: This could use a bit more work to show what the return
        value of run_once() means.
        """
        
        # We start with no CoverageRecords.
        eq_([], self._db.query(CoverageRecord).all())
        
        data_source = DataSource.lookup(
            self._db, AlwaysSuccessfulCoverageProvider.DATA_SOURCE_NAME
        )

        # Four identifiers.
        transient = self._identifier()
        persistent = self._identifier()
        uncovered = self._identifier()
        covered = self._identifier()
        
        # We previously tried to cover one of them, but got a
        # transient failure.
        self._coverage_record(
            transient, data_source,
            status=CoverageRecord.TRANSIENT_FAILURE
        )

        # Another of the four has a persistent failure.
        self._coverage_record(
            persistent, data_source, 
            status=CoverageRecord.PERSISTENT_FAILURE
        )        

        # The third one has no coverage record at all.

        # And the fourth one has been successfully covered.
        self._coverage_record(
            covered, data_source, status=CoverageRecord.SUCCESS
        )
        
        # Now let's run the coverage provider. Every Identifier
        # that's covered will succeed, so the question is which ones
        # get covered.
        provider = AlwaysSuccessfulCoverageProvider(self._db)
        provider.run_once(0)
        
        # By default, run_once() finds Identifiers that have no coverage
        # or which have transient failures.
        
        [transient_failure_has_gone] = transient.coverage_records
        eq_(CoverageRecord.SUCCESS, transient_failure_has_gone.status)

        [now_has_coverage] = uncovered.coverage_records
        eq_(CoverageRecord.SUCCESS, now_has_coverage.status)

        assert transient in provider.attempts
        assert uncovered in provider.attempts
        
        # Nothing happened to the identifier that had a persistent
        # failure or the identifier that was successfully covered.

        eq_([CoverageRecord.PERSISTENT_FAILURE],
            [x.status for x in persistent.coverage_records])
        eq_([CoverageRecord.SUCCESS],
            [x.status for x in covered.coverage_records])
        
        assert persistent not in provider.attempts
        assert covered not in provider.attempts
        
        
class TestCoverageProvider(CoverageProviderTest):

    def setup(self):
        super(TestCoverageProvider, self).setup()
        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        self.input_identifier_types = gutenberg.primary_identifier_type
        self.output_source = DataSource.lookup(self._db, DataSource.OCLC)
        self.edition = self._edition(gutenberg.name)
        self.identifier = self.edition.primary_identifier
        
    def test_ensure_coverage(self):

        provider = AlwaysSuccessfulCoverageProvider(
            "Always successful",
            output_source=self.output_source,
            input_identifier_types=self.input_identifier_types, 
        )
        record = provider.ensure_coverage(self.edition)
        assert isinstance(record, CoverageRecord)
        eq_(self.identifier, record.identifier)
        eq_(self.output_source, record.data_source)
        eq_(None, record.exception)

        # There is now one CoverageRecord -- the one returned by
        # ensure_coverage().
        [record2] = self._db.query(CoverageRecord).all()
        eq_(record2, record)

        # The coverage provider's timestamp was not updated, because
        # we're using ensure_coverage.
        eq_([], self._db.query(Timestamp).all())

    def test_ensure_coverage_respects_operation(self):
        # Two providers with the same output source but different operations.
        provider1 = AlwaysSuccessfulCoverageProvider(
            "Always successful",
            output_source=self.output_source,
            input_identifier_types=self.input_identifier_types, 
            operation="foo"
        )
        provider2 = AlwaysSuccessfulCoverageProvider(
            "Always successful",
            output_source=self.output_source,
            input_identifier_types=self.input_identifier_types, 
            operation="bar"
        )

        # Ensure coverage of both providers.
        coverage1 = provider1.ensure_coverage(self.edition)
        eq_("foo", coverage1.operation)
        old_timestamp = coverage1.timestamp

        coverage2  = provider2.ensure_coverage(self.edition)
        eq_("bar", coverage2.operation)

        # There are now two CoverageRecords, one for each operation.
        eq_(set([coverage1, coverage2]), set(self._db.query(CoverageRecord)))

        # If we try to ensure coverage again, no work is done and we
        # get the old coverage record back.
        new_coverage = provider1.ensure_coverage(self.edition)
        eq_(new_coverage, coverage1)
        new_coverage.timestamp = old_timestamp

    def test_ensure_coverage_persistent_coverage_failure(self):

        provider = NeverSuccessfulCoverageProvider(
            "Never successful",
            output_source=self.output_source,
            input_identifier_types=self.input_identifier_types, 
        )
        failure = provider.ensure_coverage(self.edition)

        # A CoverageRecord has been created to memorialize the
        # persistent failure.
        assert isinstance(failure, CoverageRecord)
        eq_("What did you expect?", failure.exception)

        # Here it is in the database.
        [record] = self._db.query(CoverageRecord).all()
        eq_(record, failure)

        # The coverage provider's timestamp was not updated, because
        # we're using ensure_coverage.
        eq_([], self._db.query(Timestamp).all())

    def test_items_that_need_coverage(self):
        cutoff_time = datetime.datetime(2016, 1, 1)
        record = CoverageRecord.add_for(
            self.edition, self.output_source, timestamp=cutoff_time
        )

        provider = AlwaysSuccessfulCoverageProvider(
            "Always successful",
            output_source=self.output_source,
            input_identifier_types=self.input_identifier_types, 
            cutoff_time=cutoff_time
        )
        eq_([], provider.items_that_need_coverage().all())

        one_second_after = cutoff_time + datetime.timedelta(seconds=1)
        provider = AlwaysSuccessfulCoverageProvider(
            "Always successful",
            output_source=self.output_source,
            input_identifier_types=self.input_identifier_types, 
            cutoff_time=one_second_after
        )
        eq_([self.identifier], 
            provider.items_that_need_coverage().all())

        provider = AlwaysSuccessfulCoverageProvider(
            "Always successful",
            output_source=self.output_source,
            input_identifier_types=self.input_identifier_types, 
        )
        eq_([], provider.items_that_need_coverage().all())

    def test_items_that_need_coverage_respects_operation(self):

        record1 = CoverageRecord.add_for(
            self.identifier, self.output_source
        )

        # Here's a provider that carries out the 'foo' operation.
        provider = AlwaysSuccessfulCoverageProvider(
            "Always successful",
            output_source=self.output_source,
            input_identifier_types=self.input_identifier_types, 
            operation='foo'
        )

        # It is missing coverage for self.identifier, because the
        # CoverageRecord we created at the start of this test has no
        # operation.
        eq_([self.identifier], provider.items_that_need_coverage().all())

        # Here's a provider that has no operation set.
        provider = AlwaysSuccessfulCoverageProvider(
            "Always successful",
            output_source=self.output_source,
            input_identifier_types=self.input_identifier_types, 
        )

        # It is not missing coverage for self.identifier, because the
        # CoverageRecord we created at the start of the test takes
        # care of it.
        eq_([], provider.items_that_need_coverage().all())

    def test_should_update(self):
        cutoff = datetime.datetime(2016, 1, 1)
        provider = AlwaysSuccessfulCoverageProvider(
            "Always successful",
            output_source=self.output_source,
            input_identifier_types=self.input_identifier_types, 
            cutoff_time = cutoff
        )

        # If coverage is missing, we should update.
        eq_(True, provider.should_update(None))

        # If coverage is outdated, we should update.
        record, ignore = CoverageRecord.add_for(
            self.identifier, self.output_source
        )
        record.timestamp = datetime.datetime(2015, 1, 1)
        eq_(True, provider.should_update(record))

        # If coverage is up-to-date, we should not update.
        record.timestamp = cutoff
        eq_(False, provider.should_update(record))

    def test_ensure_coverage_transient_coverage_failure(self):

        provider = TransientFailureCoverageProvider(
            "Transient failure",
            output_source=self.output_source,
            input_identifier_types=self.input_identifier_types,
        )
        failure = provider.ensure_coverage(self.identifier)
        eq_([failure], self.identifier.coverage_records)
        eq_(CoverageRecord.TRANSIENT_FAILURE, failure.status)
        eq_("Oops!", failure.exception)

        # Timestamp was not updated.
        eq_([], self._db.query(Timestamp).all())

    def test_run_on_specific_identifiers(self):
        provider = AlwaysSuccessfulCoverageProvider(
            "Always successful",
            output_source=self.output_source,
            input_identifier_types=self.input_identifier_types, 
        )
        provider.workset_size = 3
        to_be_tested = [self._identifier() for i in range(6)]
        not_to_be_tested = [self._identifier() for i in range(6)]
        counts, records = provider.run_on_specific_identifiers(to_be_tested)

        # Six identifiers were covered in two batches.
        eq_((6,0,0), counts)
        eq_(6, len(records))

        # Only the identifiers in to_be_tested were covered.
        assert all(isinstance(x, CoverageRecord) for x in records)
        eq_(set(to_be_tested), set([x.identifier for x in records]))
        for i in to_be_tested:
            assert i in provider.attempts
        for i in not_to_be_tested:
            assert i not in provider.attempts

    def test_run_on_specific_identifiers_respects_cutoff_time(self):

        last_run = datetime.datetime(2016, 1, 1)

        # Once upon a time we successfully added coverage for
        # self.identifier.
        record, ignore = CoverageRecord.add_for(
            self.identifier, self.output_source
        )
        record.timestamp = last_run

        # But now something has gone wrong, and if we ever run the
        # coverage provider again we will get a persistent failure.
        provider = NeverSuccessfulCoverageProvider(
            "Persistent failure",
            output_source=self.output_source,
            input_identifier_types=self.input_identifier_types,
            cutoff_time=last_run
        )

        # You might think this would result in a persistent failure...
        (success, transient_failure, persistent_failure), records = (
            provider.run_on_specific_identifiers([self.identifier])
        )

        # ...but we get an automatic success. We didn't even try to 
        # run the coverage provider on self.identifier because the
        # coverage record was up-to-date.
        eq_(1, success)
        eq_(0, persistent_failure)
        eq_([], records)

        # But if we move the cutoff time forward, the provider will run
        # on self.identifier and fail.
        provider.cutoff_time = datetime.datetime(2016, 2, 1)
        (success, transient_failure, persistent_failure), records = (
            provider.run_on_specific_identifiers([self.identifier])
        )
        eq_(0, success)
        eq_(1, persistent_failure)

        # The formerly successful CoverageRecord will be updated to
        # reflect the failure.
        eq_(records[0], record)
        eq_("What did you expect?", record.exception)

    def test_never_successful(self):

        # We start with no CoverageRecords and no Timestamp.
        eq_([], self._db.query(CoverageRecord).all())
        eq_([], self._db.query(Timestamp).all())

        provider = NeverSuccessfulCoverageProvider(
            "Never successful",
            output_source=self.output_source,
            input_identifier_types=self.input_identifier_types, 
        )
        provider.run()

        # We have a CoverageRecord that signifies failure.
        [record] = self._db.query(CoverageRecord).all()
        eq_(self.identifier, record.identifier)
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
            "Transient failure",
            output_source=self.output_source,
            input_identifier_types=self.input_identifier_types, 
        )
        provider.run()

        # We have a CoverageRecord representing the transient failure.
        [failure] = self.identifier.coverage_records
        eq_(CoverageRecord.TRANSIENT_FAILURE, failure.status)

        # The timestamp was set.
        [timestamp] = self._db.query(Timestamp).all()
        eq_("Transient failure", timestamp.service)

    def test_set_metadata(self):
        """Test that set_metadata can create and populate an
        appropriate Edition.
        """
        # Here's a provider that is not associated with any particular
        # Collection.
        provider = MockCoverageProvider(self._db)
        eq_(None, provider.collection)
        
        # It can't set circulation data, because it's not a
        # CollectionCoverageProvider.
        assert not hasattr(provider, 'set_metadata_and_circulationdata')

        # But it can set metadata.        
        test_metadata = self.BIBLIOGRAPHIC_DATA
        identifier = self._identifier(
            identifier_type=Identifier.OVERDRIVE_ID, 
            foreign_id=self.BIBLIOGRAPHIC_DATA.primary_identifier.identifier, 
        )
        eq_([], identifier.primarily_identifies)
        result = provider.set_metadata(identifier, test_metadata)

        # Here's the proof.
        edition = provider.edition(identifier)
        eq_("A Girl Named Disaster", edition.title)

        # If no metadata is passed in, a CoverageFailure results.
        result = provider.set_metadata(identifier, None)
        assert isinstance(result, CoverageFailure)
        eq_("Did not receive metadata from input source", result.exception)

        # If there's an exception setting the metadata, a
        # CoverageFailure results. This call raises a ValueError
        # because the primary identifier & the edition's primary
        # identifier don't match.
        old_identifier = test_metadata.primary_identifier
        test_metadata.primary_identifier = IdentifierData(
            type=Identifier.OVERDRIVE_ID, identifier="abcd"
        )
        result = provider.set_metadata(identifier, test_metadata)
        assert isinstance(result, CoverageFailure)
        assert "ValueError" in result.exception
        test_metadata.primary_identifier = old_identifier
        
    def test_set_metadata_incorporates_replacement_policy(self):
        """Make sure that if a ReplacementPolicy is passed in to
        set_metadata(), the policy's settings (and those of its
        .presentation_calculation_policy) are respected.
        """

        edition, pool = self._edition(with_license_pool=True)
        identifier = edition.primary_identifier

        # All images and open-access content will be fetched through this
        # 'HTTP client'...
        http = DummyHTTPClient()
        http.queue_response(
            200, content='I am an epub.',
            media_type=Representation.EPUB_MEDIA_TYPE,
        )
        
        # ..and will then be uploaded to this 'mirror'.
        mirror = DummyS3Uploader()

        class Tripwire(PresentationCalculationPolicy):
            # This class sets a variable if one of its properties is
            # accessed.
            def __init__(self, *args, **kwargs):
                self.tripped = False

            def __getattr__(self, name):
                self.tripped = True
                return True

        presentation_calculation_policy = Tripwire()

        metadata_replacement_policy = ReplacementPolicy(
            mirror=mirror,
            http_get=http.do_get,
            presentation_calculation_policy=presentation_calculation_policy
        )

        circulationdata_replacement_policy = ReplacementPolicy(
            mirror=mirror,
            http_get=http.do_get,
        )

        output_source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        provider = CollectionCoverageProvider(
            "service",
            collection=self._default_collection,
            data_source=output_source,
            input_identifier_types=[identifier.type],
        )

        metadata = Metadata(output_source)
        # We've got a CirculationData object that includes an open-access download.
        link = LinkData(rel=Hyperlink.OPEN_ACCESS_DOWNLOAD, href="http://foo.com/")
        circulationdata = CirculationData(output_source, 
            primary_identifier=metadata.primary_identifier, 
            links=[link])

        provider.set_metadata_and_circulation_data(
            identifier, metadata, circulationdata, 
            metadata_replacement_policy=metadata_replacement_policy, 
            circulationdata_replacement_policy=circulationdata_replacement_policy, 
        )

        # The open-access download was 'downloaded' and 'mirrored'.
        [mirrored] = mirror.uploaded
        eq_("http://foo.com/", mirrored.url)
        assert mirrored.mirror_url.endswith(
            "/%s/%s.epub" % (identifier.identifier, edition.title)
        )
        
        # The book content was removed from the db after it was
        # mirrored successfully.
        eq_(None, mirrored.content)

        # Our custom PresentationCalculationPolicy was used when
        # determining whether to recalculate the work's
        # presentation. We know this because the tripwire was
        # triggered.
        eq_(True, presentation_calculation_policy.tripped)


    def test_ensure_coverage_changes_status(self):
        """Verify that processing an item that has a preexisting 
        CoverageRecord can change the status of that CoverageRecord.
        """
        always = AlwaysSuccessfulCoverageProvider(
            "Always successful",
            output_source=self.output_source,
            input_identifier_types=self.input_identifier_types,
        )
        persistent = NeverSuccessfulCoverageProvider(
            "Persistent failures",
            output_source=self.output_source,
            input_identifier_types=self.input_identifier_types,
        )
        transient = TransientFailureCoverageProvider(
            "Persistent failures",
            output_source=self.output_source,
            input_identifier_types=self.input_identifier_types,
        )

        # Cover the same identifier multiple times, simulating all
        # possible states of a CoverageRecord. The same CoverageRecord
        # is used every time and the status is changed appropriately
        # after every run.
        c1 = persistent.ensure_coverage(self.identifier, force=True)
        eq_(CoverageRecord.PERSISTENT_FAILURE, c1.status)

        c2 = transient.ensure_coverage(self.identifier, force=True)
        eq_(c2, c1)
        eq_(CoverageRecord.TRANSIENT_FAILURE, c1.status)

        c3 = always.ensure_coverage(self.identifier, force=True)
        eq_(c3, c1)
        eq_(CoverageRecord.SUCCESS, c1.status)

        c4 = persistent.ensure_coverage(self.identifier, force=True)
        eq_(c4, c1)
        eq_(CoverageRecord.PERSISTENT_FAILURE, c1.status)

    def test_operation_included_in_records(self):
        provider = AlwaysSuccessfulCoverageProvider(
            "Always successful",
            output_source=self.output_source,
            input_identifier_types=self.input_identifier_types,
            operation=CoverageRecord.SYNC_OPERATION
        )
        result = provider.ensure_coverage(self.edition)

        # The provider's operation is added to the record on success
        [record] = self._db.query(CoverageRecord).all()
        eq_(record.operation, CoverageRecord.SYNC_OPERATION)
        self._db.delete(record)

        provider = NeverSuccessfulCoverageProvider(
            "Never successful",
            output_source=self.output_source,
            input_identifier_types=self.input_identifier_types,
            operation=CoverageRecord.REAP_OPERATION
        )
        result = provider.ensure_coverage(self.edition)

        # The provider's operation is added to the record on failure
        [record] = self._db.query(CoverageRecord).all()
        eq_(record.operation, CoverageRecord.REAP_OPERATION)

    def test_process_batch_and_handle_results(self):

        e1, p1 = self._edition(with_license_pool=True)
        i1 = e1.primary_identifier

        e2, p2 = self._edition(with_license_pool=True)
        i2 = e2.primary_identifier

        success_provider = AlwaysSuccessfulCoverageProvider(
            "Success",
            output_source=self.output_source,
            input_identifier_types=self.input_identifier_types,
            operation="i succeed"
        )

        batch = [i1, i2]
        counts, successes = success_provider.process_batch_and_handle_results(batch)

        # Two successes.
        eq_((2, 0, 0), counts)
        
        # Each represented with a CoverageRecord with status='success'
        assert all(isinstance(x, CoverageRecord) for x in successes)
        eq_([CoverageRecord.SUCCESS] * 2, [x.status for x in successes])

        # Each associated with one of the identifiers...
        eq_(set([i1, i2]), set([x.identifier for x in successes]))

        # ...and with the coverage provider's operation.
        eq_(['i succeed'] * 2, [x.operation for x in successes])

        # Now try a different CoverageProvider which creates transient
        # failures.
        transient_failure_provider = TransientFailureCoverageProvider(
            "Transient failure",
            output_source=self.output_source,
            input_identifier_types=self.input_identifier_types,
            operation="i fail transiently"
        )
        counts, failures = transient_failure_provider.process_batch_and_handle_results(batch)
        # Two transient failures.
        eq_((0, 2, 0), counts)

        # New coverage records were added to track the transient
        # failures.
        eq_([CoverageRecord.TRANSIENT_FAILURE] * 2,
            [x.status for x in failures])
        eq_(["i fail transiently"] * 2, [x.operation for x in failures])

        # Another way of getting transient failures is to just ignore every
        # item you're told to process.
        task_ignoring_provider = TaskIgnoringCoverageProvider(
            "Ignores all tasks",
            output_source=self.output_source,
            input_identifier_types=self.input_identifier_types,
            operation="i ignore"
        )
        counts, records = task_ignoring_provider.process_batch_and_handle_results(batch)

        eq_((0, 2, 0), counts)
        eq_([CoverageRecord.TRANSIENT_FAILURE] * 2,
            [x.status for x in records])
        eq_(["i ignore"] * 2, [x.operation for x in records])

        # If a transient failure becomes a success, the it won't have
        # an exception anymore.
        eq_(['Was ignored by CoverageProvider.'] * 2, [x.exception for x in records])
        records = success_provider.process_batch_and_handle_results(batch)[1]
        eq_([None, None], [x.exception for x in records])

        # Or you can go really bad and have persistent failures.
        persistent_failure_provider = NeverSuccessfulCoverageProvider(
            "Persistent failure",
            output_source=self.output_source,
            input_identifier_types=self.input_identifier_types,
            operation="i will always fail"
        )
        counts, results = persistent_failure_provider.process_batch_and_handle_results(batch)

        # Two persistent failures.
        eq_((0, 0, 2), counts)
        assert all([isinstance(x, CoverageRecord) for x in results])
        eq_(["What did you expect?", "What did you expect?"],
            [x.exception for x in results])
        eq_([CoverageRecord.PERSISTENT_FAILURE] * 2,
            [x.status for x in results])
        eq_(["i will always fail"] * 2, [x.operation for x in results])

    def test_no_input_identifier_types(self):
        # It's okay to pass input_identifier_types=None to the
        # constructor--it means you are looking for all identifier
        # types.
        provider = AlwaysSuccessfulCoverageProvider(
            "Always successful",
            output_source=self.output_source,
            input_identifier_types=None
        )
        eq_(None, provider.input_identifier_types)

    def test_failure_for_ignored_item(self):
        provider = NeverSuccessfulCoverageProvider(
            self._db,
            output_source=self.output_source,
            input_identifier_types=self.input_identifier_types,
            operation="i will ignore you"
            )
        result = provider.failure_for_ignored_item(self.identifier)
        assert isinstance(result, CoverageFailure)
        eq_(True, result.transient)
        eq_("Was ignored by CoverageProvider.", result.exception)
        eq_(self.identifier, result.obj)
        eq_(self.output_source, result.data_source)

    def test_edition(self):
        """Verify that CoverageProvider.edition() returns an appropriate
        Edition, even when there is no associated Collection.
        """
        # This CoverageProvider fetches bibliographic information
        # from Overdrive. It is not capable of creating LicensePools
        # because it has no Collection.
        provider = MockCoverageProvider(self._db)

        # Here's an Identifier, with no Editions.
        identifier = self._identifier(identifier_type=Identifier.OVERDRIVE_ID)
        eq_([], identifier.primarily_identifies)
        
        # Calling CoverageProvider.edition() on the Identifier gives
        # us a container for that Overdrive bibliographic information.
        # It doesn't matter that there's no Collection, because the
        # book's bibliographic information is the same across
        # Collections.
        edition = provider.edition(identifier)
        eq_(DataSource.OVERDRIVE, edition.data_source.name)
        eq_([edition], identifier.primarily_identifies)

        # Calling edition() again gives us the same Edition as before.
        edition2 = provider.edition(identifier)
        eq_(edition, edition2)


class MockCollectionCoverageProvider(CollectionCoverageProvider):

    PROTOCOL = Collection.OPDS_IMPORT
    SERVICE_NAME = "A Service"
    OPERATION = "An Operation"
    DATA_SOURCE_NAME = DataSource.OA_CONTENT_SERVER

class TestCollectionCoverageProvider(CoverageProviderTest):

    def test_class_variables(self):
        """Verify that class variables become appropriate instance
        variables.
        """
        collection = self._collection(protocol=Collection.OPDS_IMPORT)
        provider = MockCollectionCoverageProvider(collection)
        eq_(DataSource.OA_CONTENT_SERVER, provider.data_source.name)
    
    def test_all(self):
        """Verify that all() gives a sequence of CollectionCoverageProvider
        objects, one for each Collection that implements the
        appropriate protocol.
        """
        opds1 = self._collection(protocol=Collection.OPDS_IMPORT)
        opds2 = self._collection(protocol=Collection.OPDS_IMPORT)
        overdrive = self._collection(protocol=Collection.OVERDRIVE)
        providers = list(
            MockCollectionCoverageProvider.all(self._db, batch_size=34)
        )

        # The providers were returned in a random order, but there's one
        # for each collection that supports the 'OPDS Import' protocol.
        eq_(2, len(providers))
        collections = set([x.collection for x in providers])
        eq_(set([opds1, opds2]), collections)

        # The providers are of the appropriate type and the keyword arguments
        # passed into all() were propagated to the constructor.
        for provider in providers:
            assert isinstance(MockCollectionCoverageProvider, provider)
            eq_(34, provider.batch_size)

    CIRCULATION_DATA = CirculationData(
        DataSource.OVERDRIVE,
        primary_identifier=CoverageProviderTest.BIBLIOGRAPHIC_DATA.primary_identifier,
        formats = [
            FormatData(
                content_type=Representation.EPUB_MEDIA_TYPE,
                drm_scheme=DeliveryMechanism.NO_DRM,
                rights_uri=RightsStatus.IN_COPYRIGHT,
            )
        ]
    )

    def test_work(self):
        """Verify that a CollectionCoverageProvider can create a Work."""
        # Here's an Overdrive ID.
        identifier = self._identifier(identifier_type=Identifier.OVERDRIVE_ID)

        # Here's a BibliographicCoverageProvider that _is_ associated
        # with a Collection.
        provider = MockBibliographicCoverageProvider(
            self._db, collection=self._default_collection,
        )

        # This CoverageProvider cannot create a Work for the given
        # Identifier, because that would require creating a
        # LicensePool, and work() won't create a LicensePool if one
        # doesn't already exist.
        result = provider.work(identifier)
        assert isinstance(result, CoverageFailure)
        eq_("Cannot locate LicensePool", result.exception)
        
        # The CoverageProvider _can_ automatically create a
        # LicensePool, but since there is no Edition associated with
        # the Identifier, a Work still can't be created.
        pool = provider.license_pool(identifier)
        result = provider.work(identifier)
        assert isinstance(result, CoverageFailure)
        eq_("Work could not be calculated", result.exception)

        # So let's use the CoverageProvider to create an Edition
        # with minimal bibliographic information.
        edition = provider.edition(identifier)
        edition.title = u"A title"
        
        # Now we can create a Work.
        work = provider.work(identifier)
        assert isinstance(work, Work)
        eq_(u"A title", work.title)

        # Now that there's a Work associated with the Identifier, even
        # a CoverageProvider that's not associated with a Collection
        # can discover it.
        no_collection_provider = MockBibliographicCoverageProvider(
            self._db, collection=None, data_source=DataSource.OVERDRIVE
        )
        
        eq_(work, no_collection_provider.work(identifier))
        
    def test_set_metadata_and_circulationdata(self):
        """Verify that a CollectionCoverageProvider can set both
        metadata (on an Edition) and circulation data (on a LicensePool).
        """
        test_metadata = self.BIBLIOGRAPHIC_DATA
        test_circulationdata = self.CIRCULATION_DATA

        identifier = self._identifier(
            identifier_type=Identifier.OVERDRIVE_ID,
            foreign_id=self.BIBLIOGRAPHIC_DATA.primary_identifier.identifier, 
        )
        
        # Here's a BibliographicCoverageProvider that, for whatever reason,
        # is not associated with a collection.
        provider = MockBibliographicCoverageProvider(self._db, collection=None)

        # If it were a normal CoverageProvider, it would have no
        # mechanism to set circulation data. Since it's a
        # BibliographicCoverageProvider, the mechanism does exist, but
        # attempting to set circulation data will result in a
        # CoverageFailure.
        result = provider.set_metadata_and_circulation_data(
            identifier, None, test_circulationdata
        )
        assert isinstance(result, CoverageFailure)
        eq_(
            "Could not create a LicensePool for this identifier because the CoverageProvider has no associated Collection.",
            result.exception
        )
        
        # Let's associate a Collection with the
        # BibliographicCoverageProvider and try again.
        provider.collection = self._default_collection

        # We get a CoverageFailure if we don't pass in any data at all.
        result = provider.set_metadata_and_circulation_data(
            identifier, None, None
        )
        assert isinstance(result, CoverageFailure)
        eq_(
            "Received neither metadata nor circulation data from input source", 
            result.exception
        )

        # We get a CoverageFailure if no work can be created. In this
        # case, that happens because the metadata doesn't provide a
        # title.
        old_title = test_metadata.title
        test_metadata.title = None
        result = provider.set_metadata_and_circulation_data(
            identifier, test_metadata, test_circulationdata
        )
        assert isinstance(result, CoverageFailure)
        eq_("Work could not be calculated", result.exception)

        # Restore the title and try again. This time it will work.
        test_metadata.title = old_title        
        result = provider.set_metadata_and_circulation_data(
            identifier, test_metadata, test_circulationdata
        )
        eq_(result, identifier)

        # An Edition was created to hold the metadata, a LicensePool
        # was created to hold the circulation data, and a Work
        # was created to bind everything together.
        [edition] = identifier.primarily_identifies
        eq_("A Girl Named Disaster", edition.title)
        [pool] = identifier.licensed_through
        work = identifier.work
        eq_(work, pool.work)
        
        # BibliographicCoverageProviders typically don't have
        # circulation information in the sense of 'how many copies are
        # in this Collection?', but sometimes they do have circulation
        # information in the sense of 'what formats are available?'
        [lpdm] = pool.delivery_mechanisms
        mechanism = lpdm.delivery_mechanism
        eq_("application/epub+zip (DRM-free)", mechanism.name)

        # If there's an exception setting the metadata, a
        # CoverageFailure results. This call raises a ValueError
        # because the identifier we're trying to cover doesn't match
        # the identifier found in the Metadata object.
        old_identifier = test_metadata.primary_identifier
        test_metadata.primary_identifier = IdentifierData(
            type=Identifier.OVERDRIVE_ID, identifier="abcd"
        )
        result = provider.set_metadata_and_circulation_data(
            identifier, test_metadata, test_circulationdata
        )
        assert isinstance(result, CoverageFailure)
        assert "ValueError" in result.exception
        test_metadata.primary_identifier = old_identifier
        
    def test_autocreate_licensepool(self):

        # A coverage provider that does not provide a Collection cannot
        # create a LicensePool for an Identifier, because the LicensePool
        # would not belong to any particular Collection.
        no_collection_provider = MockBibliographicCoverageProvider(
            self._db, collection=None
        )
        identifier = self._identifier(identifier_type=Identifier.OVERDRIVE_ID)
        result = no_collection_provider.license_pool(identifier)
        assert isinstance(result, CoverageFailure)
        eq_(
            "Could not create a LicensePool for this identifier because the CoverageProvider has no associated Collection.",
            result.exception
        )

        # If a Collection is provided, the coverage provider can
        # create a LicensePool for an Identifier.
        with_collection_provider = MockBibliographicCoverageProvider(
            self._db, collection=self._default_collection
        )
        pool = with_collection_provider.license_pool(identifier)
        eq_(pool.data_source, with_collection_provider.output_source)
        eq_(pool.identifier, identifier)
        eq_(pool.collection, with_collection_provider.collection)
       
    def test_set_presentation_ready(self):
        """Test that a CollectionCoverageProvider can set a Work
        as presentation-ready.
        """
        provider = MockBibliographicCoverageProvider(
            self._db, collection=self._default_collection
        )
        identifier = self._identifier(identifier_type=Identifier.OVERDRIVE_ID)
        test_metadata = self.BIBLIOGRAPHIC_DATA

        # If there is no LicensePool for the Identifier,
        # set_presentation_ready will not try to create one,
        # and so no Work will be created.
        result = provider.set_presentation_ready(identifier)
        assert isinstance(result, CoverageFailure)
        eq_("Cannot locate LicensePool", result.exception)

        # Once a LicensePool and a suitable Edition exist,
        # set_presentation_ready will create a Work for the item and
        # mark it presentation ready.
        pool = provider.license_pool(identifier)
        edition = provider.edition(identifier)
        edition.title = u'A title'
        result = provider.set_presentation_ready(identifier)
        eq_(result, identifier)
        eq_(True, pool.work.presentation_ready)

    def test_process_batch_sets_work_presentation_ready(self):

        work = self._work(with_license_pool=True, 
                          with_open_access_download=True)
        pool = work.license_pools[0]
        identifier = pool.identifier
        work.presentation_ready = False
        provider = MockBibliographicCoverageProvider(
            self._db, pool.collection, data_source=DataSource.GUTENBERG
        )
        [result] = provider.process_batch([identifier])
        eq_(result, identifier)
        eq_(True, work.presentation_ready)

        # ensure_coverage does the same thing.
        work.presentation_ready = False
        result = provider.ensure_coverage(identifier)
        assert isinstance(result, CoverageRecord)
        eq_(result.identifier, identifier)
        eq_(True, work.presentation_ready)

    def test_failure_does_not_set_work_presentation_ready(self):
        work = self._work(with_license_pool=True, 
                          with_open_access_download=True)
        identifier = work.license_pools[0].identifier
        work.presentation_ready = False
        [pool] = work.license_pools
        provider = MockFailureBibliographicCoverageProvider(
            self._db, pool.collection
        )
        [result] = provider.process_batch([identifier])
        assert isinstance(result, CoverageFailure)
        eq_(False, work.presentation_ready)

        
class MockGenericAPI(object):
    """Mock only the features of an API that BibliographicCoverageProvider
    expects.
    """
    
    def __init__(self, collection):
        self.collection = collection


class MockCoverageProvider(IdentifierCoverageProvider):
    """Simulates a CoverageProvider that's always successful."""

    def __init__(self, _db, *args, **kwargs):
        if not 'service_name' in kwargs:
            kwargs['service_name'] = 'Generic provider'
        if not 'output_source' in kwargs:
            kwargs['output_source'] = DataSource.lookup(
                _db, DataSource.OVERDRIVE
            )
        super(MockCoverageProvider, self).__init__(
            *args, **kwargs
        )

    def process_item(self, identifier):
        return identifier

    
class MockBibliographicCoverageProvider(BibliographicCoverageProvider):
    """Simulates a BibliographicCoverageProvider that's always successful."""

    def __init__(self, _db, collection, **kwargs):
        if not 'api' in kwargs:
            kwargs['api'] = MockGenericAPI(collection)
        if not 'data_source' in kwargs:
            kwargs['data_source'] = DataSource.OVERDRIVE
        super(MockBibliographicCoverageProvider, self).__init__(
            _db, **kwargs
        )

    def process_item(self, identifier):
        return identifier

class TestWorkCoverageProvider(DatabaseTest):

    def setup(self):
        super(TestWorkCoverageProvider, self).setup()
        self.work = self._work()
        self.operation = 'the_operation'

    def test_success(self):
        qu = self._db.query(WorkCoverageRecord).filter(
            WorkCoverageRecord.operation==self.operation
        )
        # We start with no relevant WorkCoverageRecord and no Timestamp.
        eq_([], qu.all())

        eq_([], self._db.query(Timestamp).all())

        provider = AlwaysSuccessfulWorkCoverageProvider(
            self._db, "Always successful", operation=self.operation
        )
        provider.run()

        # There is now one relevant WorkCoverageRecord.
        [record] = qu.all()
        eq_(self.work, record.work)
        eq_(self.operation, record.operation)

        # The timestamp is now set.
        [timestamp] = self._db.query(Timestamp).all()
        eq_("Always successful", timestamp.service)

    def test_transient_failure(self):
        # We start with no relevant WorkCoverageRecords.
        qu = self._db.query(WorkCoverageRecord).filter(
            WorkCoverageRecord.operation==self.operation
        )
        eq_([], qu.all())

        provider = TransientFailureWorkCoverageProvider(
            self._db, "Transient failure", operation=self.operation
        )
        provider.run()

        # We have a CoverageRecord for the transient failure.
        [failure] = [x for x in self.work.coverage_records if 
                     x.operation==self.operation]
        eq_(CoverageRecord.TRANSIENT_FAILURE, failure.status)

        # The timestamp is now set.
        [timestamp] = self._db.query(Timestamp).all()
        eq_("Transient failure", timestamp.service)

    def test_persistent_failure(self):
        # We start with no relevant WorkCoverageRecords.
        qu = self._db.query(WorkCoverageRecord).filter(
            WorkCoverageRecord.operation==self.operation
        )
        eq_([], qu.all())

        provider = NeverSuccessfulWorkCoverageProvider(
            self._db, "Persistent failure", operation=self.operation
        )
        provider.run()

        # We have a WorkCoverageRecord, since the error was persistent.
        [record] = qu.all()
        eq_(self.work, record.work)
        eq_("What did you expect?", record.exception)

        # The timestamp is now set.
        [timestamp] = self._db.query(Timestamp).all()
        eq_("Persistent failure", timestamp.service)


    def test_items_that_need_coverage(self):
        # Here are three works,
        w1 = self.work
        w2 = self._work(with_license_pool=True)
        w3 = self._work(with_license_pool=True)
        
        # w2 has coverage, the other two do not.
        record = self._work_coverage_record(w2, self.operation)

        # Here's a WorkCoverageProvider.
        provider = AlwaysSuccessfulWorkCoverageProvider(
            self._db, "Success", operation=self.operation,
        )

        # By default, items_that_need_coverage returns the two
        # works that don't have coverage.
        eq_(set([w1, w3]), set(provider.items_that_need_coverage().all()))

        # If we pass in a list of Identifiers we further restrict
        # items_that_need_coverage to Works whose LicensePools have an
        # Identifier in that list.
        i2 = w2.license_pools[0].identifier
        i3 = w3.license_pools[0].identifier
        eq_([w3], provider.items_that_need_coverage([i2, i3]).all())

        # If we set a cutoff_time which is after the time the
        # WorkCoverageRecord was created, then that work starts
        # showing up again as needing coverage.
        provider.cutoff_time = record.timestamp + datetime.timedelta(seconds=1)
        eq_(set([w2, w3]),
            set(provider.items_that_need_coverage([i2, i3]).all())
        )

    def test_failure_for_ignored_item(self):
        provider = NeverSuccessfulWorkCoverageProvider(
            self._db, "I'll just ignore you", operation=self.operation
        )
        result = provider.failure_for_ignored_item(self.work)
        assert isinstance(result, CoverageFailure)
        eq_(True, result.transient)
        eq_("Was ignored by WorkCoverageProvider.", result.exception)
        eq_(self.work, result.obj)
        

class MockFailureBibliographicCoverageProvider(MockBibliographicCoverageProvider):
    """Simulates a BibliographicCoverageProvider that's never successful."""

    def process_item(self, identifier):
        return CoverageFailure(
            self, identifier, "Bitter failure", transient=True
        )
