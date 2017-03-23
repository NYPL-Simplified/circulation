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
    AlwaysSuccessfulWorkCoverageProvider,
    DummyHTTPClient,
    TaskIgnoringCoverageProvider,
    NeverSuccessfulWorkCoverageProvider,
    NeverSuccessfulCoverageProvider,
    TransientFailureCoverageProvider,
    TransientFailureWorkCoverageProvider,
)
from model import (
    Contributor,
    CoverageRecord,
    DataSource,
    Edition,
    Hyperlink,
    Identifier,
    PresentationCalculationPolicy,
    Representation,
    Subject,
    Timestamp,
    WorkCoverageRecord,
)
from metadata_layer import (
    Metadata,
    CirculationData,
    IdentifierData,
    ContributorData,
    LinkData,
    ReplacementPolicy,
    SubjectData,
)
from s3 import DummyS3Uploader
from coverage import (
    BibliographicCoverageProvider,
    CollectionCoverageProvider,
    CoverageProvider,
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

    def test_run_once_and_update_timestamp(self):
        """Test run_once_and_update_timestamp. It should cover items that have
        no CoverageRecord at all, then items whose previous coverage
        attempt resulted in a transient failure.

        This doubles as a test of AlwaysSuccessfulCoverageProvider's
        ability to always create a CoverageRecord.
        """

        # We start with no Timestamp.
        eq_([], self._db.query(CoverageRecord).all())

        # We previously tried to coverage the identifier we normally
        # use in these tests, but got a transient failure.
        self._coverage_record(
            self.identifier, self.output_source, 
            status=CoverageRecord.TRANSIENT_FAILURE
        )

        # Here's a new identifier that has no coverage at all.
        no_coverage = self._identifier()

        # And here's an identifier that has a persistent failure.
        persistent_failure = self._identifier()
        self._coverage_record(
            persistent_failure, self.output_source, 
            status=CoverageRecord.PERSISTENT_FAILURE
        )        

        eq_([], self._db.query(Timestamp).all())

        provider = AlwaysSuccessfulCoverageProvider(
            "Always successful",
            output_source=self.output_source,
            input_identifier_types=self.input_identifier_types, 
        )
        provider.run()

        # The timestamp is now set.
        [timestamp] = self._db.query(Timestamp).all()
        eq_("Always successful", timestamp.service)

        # The identifier with no coverage and the identifier with a
        # transient failure now have coverage records that indicate
        # success.

        [transient_failure_has_gone] = self.identifier.coverage_records
        eq_(CoverageRecord.SUCCESS, transient_failure_has_gone.status)

        [now_has_coverage] = no_coverage.coverage_records
        eq_(CoverageRecord.SUCCESS, now_has_coverage.status)

        # The identifier that had the transient failure was processed
        # second, even though it was created first in the
        # database. That's because we do the work in two passes: first
        # we process identifiers where coverage has never been
        # attempted, then we process identifiers with transient failures.
        eq_([no_coverage, self.identifier], provider.attempts)

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

class MockGenericAPI(object):
    """Mock only the features of an API that BibliographicCoverageProvider
    expects.
    """
    
    def __init__(self, collection):
        self.collection = collection


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


class TestBibliographicCoverageProvider(DatabaseTest):

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

    CIRCULATION_DATA = CirculationData(
        DataSource.OVERDRIVE,
        primary_identifier=BIBLIOGRAPHIC_DATA.primary_identifier,
    )

    def test_edition(self):
        provider = MockBibliographicCoverageProvider(
            self._db, collection=self._default_collection
        )
        provider.CAN_CREATE_LICENSE_POOLS = False
        identifier = self._identifier(identifier_type=Identifier.OVERDRIVE_ID)
        test_metadata = self.BIBLIOGRAPHIC_DATA

        # Returns a CoverageFailure if the identifier doesn't have a
        # license pool and none can be created.
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
        provider = MockBibliographicCoverageProvider(
            self._db, collection=self._default_collection,
        )
        identifier = self._identifier(identifier_type=Identifier.OVERDRIVE_ID)
        test_metadata = self.BIBLIOGRAPHIC_DATA
        provider.CAN_CREATE_LICENSE_POOLS = False

        # Returns a CoverageFailure if the identifier doesn't have a
        # license pool.
        result = provider.work(identifier)
        assert isinstance(result, CoverageFailure)
        eq_("No license pool available", result.exception)

        # Returns a CoverageFailure if there's no work available.
        edition, lp = self._edition(
            with_license_pool=True, data_source_name=DataSource.OVERDRIVE,
            identifier_type=Identifier.OVERDRIVE_ID
        )

        # Remove edition so that the work won't be calculated
        lp.identifier.primarily_identifies = []
        result = provider.work(lp.identifier)
        assert isinstance(result, CoverageFailure)
        eq_("Work could not be calculated", result.exception)

        # Returns the work if it can be created or found.
        ed, lp = self._edition(
            with_license_pool=True, data_source_name=DataSource.OVERDRIVE,
            identifier_type=Identifier.OVERDRIVE_ID
        )
        result = provider.work(lp.identifier)
        eq_(result, lp.work)

    def test_set_metadata(self):
        provider = MockBibliographicCoverageProvider(
            self._db, collection=self._default_collection
        )
        provider.CAN_CREATE_LICENSE_POOLS = False
        identifier = self._identifier(identifier_type=Identifier.OVERDRIVE_ID)
        test_metadata = self.BIBLIOGRAPHIC_DATA
        test_circulationdata = self.CIRCULATION_DATA

        # If there is no LicensePool and it can't be autocreated, a
        # CoverageRecord results.
        result = provider.work(identifier)
        assert isinstance(result, CoverageFailure)
        eq_("No license pool available", result.exception)

        edition, lp = self._edition(data_source_name=DataSource.OVERDRIVE, 
            identifier_type=Identifier.OVERDRIVE_ID, 
            identifier_id=self.BIBLIOGRAPHIC_DATA.primary_identifier.identifier, 
            with_license_pool=True)

        # If no metadata is passed in, a CoverageFailure results.
        result = provider.set_metadata_and_circulation_data(edition.primary_identifier, None, None)

        assert isinstance(result, CoverageFailure)
        eq_("Received neither metadata nor circulation data from input source", result.exception)

        # If no work can be created (in this case, because there's no title),
        # a CoverageFailure results.
        edition.title = None
        old_title = test_metadata.title
        test_metadata.title = None
        result = provider.set_metadata_and_circulation_data(edition.primary_identifier, test_metadata, test_circulationdata)
        assert isinstance(result, CoverageFailure)
        eq_("Work could not be calculated", result.exception)
        test_metadata.title = old_title        

        # Test success
        result = provider.set_metadata_and_circulation_data(edition.primary_identifier, test_metadata, test_circulationdata)
        eq_(result, edition.primary_identifier)

        # If there's an exception setting the metadata, a
        # CoverageRecord results. This call raises a ValueError
        # because the primary identifier & the edition's primary
        # identifier don't match.
        test_metadata.primary_identifier = self._identifier(
            identifier_type=Identifier.OVERDRIVE_ID
        )
        result = provider.set_metadata_and_circulation_data(lp.identifier, test_metadata, test_circulationdata)
        assert isinstance(result, CoverageFailure)
        assert "ValueError" in result.exception


    def test_autocreate_licensepool(self):
        provider = MockBibliographicCoverageProvider(
            self._db, collection=self._default_collection
        )
        identifier = self._identifier(identifier_type=Identifier.OVERDRIVE_ID)

        # If this constant is set to False, the coverage provider cannot
        # autocreate LicensePools for identifiers.
        provider.CAN_CREATE_LICENSE_POOLS = False
        eq_(None, provider.license_pool(identifier))

        # If it's set to True, the coverage provider can autocreate
        # LicensePools for identifiers.
        provider.CAN_CREATE_LICENSE_POOLS = True
        pool = provider.license_pool(identifier)
        eq_(pool.data_source, provider.output_source)
        eq_(pool.identifier, identifier)
       
    def test_set_presentation_ready(self):
        provider = MockBibliographicCoverageProvider(
            self._db, collection=self._default_collection
        )
        identifier = self._identifier(identifier_type=Identifier.OVERDRIVE_ID)
        test_metadata = self.BIBLIOGRAPHIC_DATA

        # If the work can't be found, it can't be made presentation ready.
        provider.CAN_CREATE_LICENSE_POOLS = False
        result = provider.set_presentation_ready(identifier)
        assert isinstance(result, CoverageFailure)
        eq_("No license pool available", result.exception)

        # Test success.
        ed, lp = self._edition(with_license_pool=True)
        result = provider.set_presentation_ready(ed.primary_identifier)
        eq_(result, ed.primary_identifier)

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


class TestCoverageFailure(DatabaseTest):

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
