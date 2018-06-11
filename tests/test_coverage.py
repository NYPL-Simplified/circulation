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
    AlwaysSuccessfulBibliographicCoverageProvider,
    AlwaysSuccessfulCollectionCoverageProvider,
    AlwaysSuccessfulCoverageProvider,
    AlwaysSuccessfulWorkCoverageProvider,
    DummyHTTPClient,
    TaskIgnoringCoverageProvider,
    NeverSuccessfulBibliographicCoverageProvider,
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
    ExternalIntegration,
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
from s3 import MockS3Uploader
from coverage import (
    BaseCoverageProvider,
    BibliographicCoverageProvider,
    CatalogCoverageProvider,
    CollectionCoverageProvider,
    CoverageFailure,
    IdentifierCoverageProvider,
    OPDSEntryWorkCoverageProvider,
    PresentationReadyWorkCoverageProvider,
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
        class MockProvider(BaseCoverageProvider):
            SERVICE_NAME = "I do nothing"
            was_run = False

            def run_once_and_update_timestamp(self):
                self.was_run = True

        provider = MockProvider(self._db)
        provider.run()
        eq_(True, provider.was_run)
        
    def test_run_once_and_update_timestamp(self):
        """Test that run_once_and_update_timestamp calls run_once twice and
        then updates a Timestamp.
        """
        class MockProvider(BaseCoverageProvider):
            SERVICE_NAME = "I do nothing"
            run_once_calls = []
            
            def run_once(self, offset, count_as_covered=None):
                self.run_once_calls.append(count_as_covered)
            
        # We start with no timestamp value.
        service_name = "I do nothing"
        eq_(None, Timestamp.value(self._db, service_name, collection=None))
        
        # Instantiate the Provider, and call
        # run_once_and_update_timestamp.
        provider = MockProvider(self._db)
        provider.run_once_and_update_timestamp()

        # The timestamp is now set to a recent value.
        value = Timestamp.value(self._db, service_name, collection=None)
        assert (datetime.datetime.utcnow() - value).total_seconds() < 1

        # run_once was called twice: once to exclude items that have
        # any coverage record whatsoever (PREVIOUSLY_ATTEMPTED), and again to
        # exclude only items that have coverage records that indicate
        # success or persistent failure (DEFAULT_COUNT_AS_COVERED).
        eq_([CoverageRecord.PREVIOUSLY_ATTEMPTED,
             CoverageRecord.DEFAULT_COUNT_AS_COVERED], provider.run_once_calls)
        
    def test_run_once(self):
        """Test run_once, showing how it covers items with different types of
        CoverageRecord.

        TODO: This could use a bit more work to show what the return
        value of run_once() means.
        """
        
        # We start with no CoverageRecords.
        eq_([], self._db.query(CoverageRecord).all())
        
        # Four identifiers.
        transient = self._identifier()
        persistent = self._identifier()
        uncovered = self._identifier()
        covered = self._identifier()

        # This provider will try to cover them.
        provider = AlwaysSuccessfulCoverageProvider(self._db)
        data_source = provider.data_source
        
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

        # We can change which identifiers get processed by changing
        # what counts as 'coverage'.
        provider.run_once(0, count_as_covered=[CoverageRecord.SUCCESS])

        # That processed the persistent failure, but not the success.
        assert persistent in provider.attempts
        assert covered not in provider.attempts

        # Let's call it again and say that we are covering everything
        # _except_ persistent failures.
        provider.run_once(0, count_as_covered=[CoverageRecord.PERSISTENT_FAILURE])

        # That got us to cover the identifier that had already been
        # successfully covered.
        assert covered in provider.attempts

    def test_process_batch_and_handle_results(self):
        """Test that process_batch_and_handle_results passes the identifiers
        its given into the appropriate BaseCoverageProvider, and deals
        correctly with the successes and failures it might return.
        """
        e1, p1 = self._edition(with_license_pool=True)
        i1 = e1.primary_identifier

        e2, p2 = self._edition(with_license_pool=True)
        i2 = e2.primary_identifier

        class MockProvider(AlwaysSuccessfulCoverageProvider):
            OPERATION = 'i succeed'

            def finalize_batch(self):
                self.finalized = True
            
        success_provider = MockProvider(self._db)

        batch = [i1, i2]
        counts, successes = success_provider.process_batch_and_handle_results(batch)

        # Two successes.
        eq_((2, 0, 0), counts)

        # finalize_batch() was called.
        eq_(True, success_provider.finalized)
        
        # Each represented with a CoverageRecord with status='success'
        assert all(isinstance(x, CoverageRecord) for x in successes)
        eq_([CoverageRecord.SUCCESS] * 2, [x.status for x in successes])

        # Each associated with one of the identifiers...
        eq_(set([i1, i2]), set([x.identifier for x in successes]))

        # ...and with the coverage provider's operation.
        eq_(['i succeed'] * 2, [x.operation for x in successes])

        # Now try a different CoverageProvider which creates transient
        # failures.
        class MockProvider(TransientFailureCoverageProvider):
            OPERATION = "i fail transiently"
            
        transient_failure_provider = MockProvider(self._db)
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
        class MockProvider(TaskIgnoringCoverageProvider):
            OPERATION = "i ignore"
        task_ignoring_provider = MockProvider(self._db)
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
        class MockProvider(NeverSuccessfulCoverageProvider):
            OPERATION = "i will always fail"
        persistent_failure_provider = MockProvider(self._db)
        counts, results = persistent_failure_provider.process_batch_and_handle_results(batch)

        # Two persistent failures.
        eq_((0, 0, 2), counts)
        assert all([isinstance(x, CoverageRecord) for x in results])
        eq_(["What did you expect?", "What did you expect?"],
            [x.exception for x in results])
        eq_([CoverageRecord.PERSISTENT_FAILURE] * 2,
            [x.status for x in results])
        eq_(["i will always fail"] * 2, [x.operation for x in results])

    def test_process_batch(self):
        """TODO: We're missing this test coverage.

        Among other things, verify that handle_success is called.
        """

    def test_should_update(self):
        """Verify that should_update gives the correct answer when we
        ask if a CoverageRecord needs to be updated.
        """
        cutoff = datetime.datetime(2016, 1, 1)
        provider = AlwaysSuccessfulCoverageProvider(
            self._db, cutoff_time = cutoff
        )
        identifier = self._identifier()
        
        # If coverage is missing, we should update.
        eq_(True, provider.should_update(None))

        # If coverage is outdated, we should update.
        record, ignore = CoverageRecord.add_for(
            identifier, provider.data_source
        )
        record.timestamp = datetime.datetime(2015, 1, 1)
        eq_(True, provider.should_update(record))

        # If coverage is up-to-date, we should not update.
        record.timestamp = cutoff
        eq_(False, provider.should_update(record))

        # If coverage is only 'registered', we should update.
        record.status = CoverageRecord.REGISTERED
        eq_(True, provider.should_update(record))


class TestIdentifierCoverageProvider(CoverageProviderTest):

    def setup(self):
        super(TestIdentifierCoverageProvider, self).setup()
        self.identifier = self._identifier()

    def test_input_identifier_types(self):
        """Test various acceptable and unacceptable values for the class
        variable INPUT_IDENTIFIER_TYPES.
        """
        # It's okay to set INPUT_IDENTIFIER_TYPES to None it means you
        # will cover any and all identifier types.
        class Base(IdentifierCoverageProvider):
            SERVICE_NAME = "Test provider"
            DATA_SOURCE_NAME = DataSource.GUTENBERG
            
        class MockProvider(Base):
            INPUT_IDENTIFIER_TYPES = None
        provider = MockProvider(self._db)
        eq_(None, provider.input_identifier_types)

        # It's okay to set a single value.
        class MockProvider(Base):
            INPUT_IDENTIFIER_TYPES = Identifier.ISBN
        provider = MockProvider(self._db)
        eq_([Identifier.ISBN], provider.input_identifier_types)

        # It's okay to set a list of values.
        class MockProvider(Base):
            INPUT_IDENTIFIER_TYPES = [Identifier.ISBN, Identifier.OVERDRIVE_ID]
        provider = MockProvider(self._db)
        eq_([Identifier.ISBN, Identifier.OVERDRIVE_ID],
            provider.input_identifier_types)

        # It's not okay to do nothing.
        class MockProvider(Base):
            pass
        assert_raises_regexp(
            ValueError,
            "MockProvider must define INPUT_IDENTIFIER_TYPES, even if the value is None.",
            MockProvider,
            self._db
        )

    def test_can_cover(self):
        """Verify that can_cover gives the correct answer when
        asked if an IdentifierCoverageProvider can handle a given Identifier.
        """
        provider = AlwaysSuccessfulCoverageProvider(self._db)
        identifier = self._identifier(identifier_type=Identifier.ISBN)
        m = provider.can_cover

        # This provider handles all identifier types.
        provider.input_identifier_types = None
        eq_(True, m(identifier))

        # This provider handles ISBNs.
        provider.input_identifier_types = [
            Identifier.OVERDRIVE_ID, Identifier.ISBN
        ]
        eq_(True, m(identifier))

        # This provider doesn't.
        provider.input_identifier_types = [Identifier.OVERDRIVE_ID]
        eq_(False, m(identifier))

    def test_replacement_policy(self):
        """Unless a different replacement policy is passed in, the
        default is ReplacementPolicy.from_metadata_source().
        """
        provider = AlwaysSuccessfulCoverageProvider(self._db)
        eq_(True, provider.replacement_policy.identifiers)
        eq_(False, provider.replacement_policy.formats)
        
        policy = ReplacementPolicy.from_license_source(self._db)
        provider = AlwaysSuccessfulCoverageProvider(
            self._db, replacement_policy=policy
        )
        eq_(policy, provider.replacement_policy)

    def test_register(self):
        # The identifier has no coverage.
        eq_(0, len(self.identifier.coverage_records))

        provider = AlwaysSuccessfulCoverageProvider
        
        # If a CoverageRecord doesn't exist for the provider,
        # a 'registered' record is created.
        new_record, was_registered = provider.register(self.identifier)

        eq_(self.identifier.coverage_records, [new_record])
        eq_(provider.DATA_SOURCE_NAME, new_record.data_source.name)
        eq_(CoverageRecord.REGISTERED, new_record.status)
        eq_(None, new_record.exception)

        # If a CoverageRecord exists already, it's returned.
        existing = new_record
        existing.status = CoverageRecord.SUCCESS

        new_record, was_registered = provider.register(self.identifier)
        eq_(existing, new_record)
        eq_(False, was_registered)
        # Its details haven't been changed in any way.
        eq_(CoverageRecord.SUCCESS, new_record.status)
        eq_(None, new_record.exception)

    def test_bulk_register(self):
        provider = AlwaysSuccessfulCoverageProvider
        source = DataSource.lookup(self._db, provider.DATA_SOURCE_NAME)

        i1 = self._identifier()
        covered = self._identifier()
        existing = self._coverage_record(
            covered, source, operation=provider.OPERATION
        )

        new_records, ignored_identifiers = provider.bulk_register([i1, covered])

        eq_(i1.coverage_records, new_records)
        [new_record] = new_records
        eq_(provider.DATA_SOURCE_NAME, new_record.data_source.name)
        eq_(provider.OPERATION, new_record.operation)
        eq_(CoverageRecord.REGISTERED, new_record.status)

        eq_([covered], ignored_identifiers)
        # The existing CoverageRecord hasn't been changed.
        eq_(CoverageRecord.SUCCESS, existing.status)

    def test_bulk_register_can_overwrite_existing_record_status(self):
        provider = AlwaysSuccessfulCoverageProvider

        # Create an existing record, and give it a SUCCESS status.
        provider.bulk_register([self.identifier])
        [existing] = self.identifier.coverage_records
        existing.status = CoverageRecord.SUCCESS
        self._db.commit()

        # If registration is forced, an existing record is updated.
        records, ignored = provider.bulk_register([self.identifier], force=True)
        eq_([existing], records)
        eq_(CoverageRecord.REGISTERED, existing.status)

    def test_bulk_register_with_collection(self):
        provider = AlwaysSuccessfulCoverageProvider
        collection = self._collection(data_source_name=DataSource.AXIS_360)

        try:
            # If a DataSource or data source name is provided and
            # autocreate is set True, the record is created with that source.
            provider.bulk_register(
                [self.identifier], data_source=collection.name,
                collection=collection, autocreate=True
            )
            [record] = self.identifier.coverage_records

            # A DataSource with the given name has been created.
            collection_source = DataSource.lookup(self._db, collection.name)
            assert collection_source
            assert provider.DATA_SOURCE_NAME != record.data_source.name
            eq_(collection_source, record.data_source)

            # Even though a collection was given, the record's collection isn't
            # set.
            eq_(None, record.collection)

            # However, when coverage is collection-specific the
            # CoverageRecord is related to the given collection.
            provider.COVERAGE_COUNTS_FOR_EVERY_COLLECTION = False

            provider.bulk_register(
                [self.identifier], collection_source, collection=collection
            )
            records = self.identifier.coverage_records
            eq_(2, len(records))
            assert [r for r in records if r.collection==collection]
        finally:
            # Return the mock class to its original state for other tests.
            provider.COVERAGE_COUNTS_FOR_EVERY_COLLECTION = True

    def test_ensure_coverage(self):
        """Verify that ensure_coverage creates a CoverageRecord for an
        Identifier, assuming that the CoverageProvider succeeds.
        """
        provider = AlwaysSuccessfulCollectionCoverageProvider(
            self._default_collection
        )
        provider.OPERATION = self._str
        record = provider.ensure_coverage(self.identifier)
        assert isinstance(record, CoverageRecord)
        eq_(self.identifier, record.identifier)
        eq_(provider.data_source, record.data_source)
        eq_(provider.OPERATION, record.operation)
        eq_(None, record.exception)

        # There is now one CoverageRecord -- the one returned by
        # ensure_coverage().
        [record2] = self._db.query(CoverageRecord).all()
        eq_(record2, record)

        # Because this provider counts coverage in one Collection as
        # coverage for all Collections, the coverage record was not
        # associated with any particular collection.
        eq_(True, provider.COVERAGE_COUNTS_FOR_EVERY_COLLECTION)
        eq_(None, record2.collection)

        # The coverage provider's timestamp was not updated, because
        # we're using ensure_coverage on a single record.
        eq_(None,
            Timestamp.value(self._db, provider.service_name, collection=None)
        )

        # Now let's try a CollectionCoverageProvider that needs to
        # grant coverage separately for every collection.
        provider.COVERAGE_COUNTS_FOR_EVERY_COLLECTION = False
        record3 = provider.ensure_coverage(self.identifier)

        # This creates a new CoverageRecord associated with the
        # provider's collection.
        assert record3 != record2
        eq_(provider.collection, record3.collection)

    def test_ensure_coverage_works_on_edition(self):
        """Verify that ensure_coverage() works on an Edition by covering
        its primary identifier.
        """        
        edition = self._edition()
        provider = AlwaysSuccessfulCoverageProvider(self._db)
        record = provider.ensure_coverage(edition)
        assert isinstance(record, CoverageRecord)
        eq_(edition.primary_identifier, record.identifier)
        
    def test_ensure_coverage_respects_operation(self):
        # Two providers with the same output source but different operations.
        class Mock1(AlwaysSuccessfulCoverageProvider):
            OPERATION = "foo"
        provider1 = Mock1(self._db)
        
        class Mock2(NeverSuccessfulCoverageProvider):
            OPERATION = "bar"
        provider2 = Mock2(self._db)

        # Ensure coverage from both providers.
        coverage1 = provider1.ensure_coverage(self.identifier)
        eq_("foo", coverage1.operation)
        old_timestamp = coverage1.timestamp

        coverage2  = provider2.ensure_coverage(self.identifier)
        eq_("bar", coverage2.operation)

        # There are now two CoverageRecords, one for each operation.
        eq_(set([coverage1, coverage2]), set(self._db.query(CoverageRecord)))

        # If we try to ensure coverage again, no work is done and we
        # get the old coverage record back.
        new_coverage = provider1.ensure_coverage(self.identifier)
        eq_(new_coverage, coverage1)
        new_coverage.timestamp = old_timestamp
        
    def test_ensure_coverage_persistent_coverage_failure(self):

        provider = NeverSuccessfulCoverageProvider(self._db)
        failure = provider.ensure_coverage(self.identifier)

        # A CoverageRecord has been created to memorialize the
        # persistent failure.
        assert isinstance(failure, CoverageRecord)
        eq_("What did you expect?", failure.exception)

        # Here it is in the database.
        [record] = self._db.query(CoverageRecord).all()
        eq_(record, failure)

        # The coverage provider's timestamp was not updated, because
        # we're using ensure_coverage.
        # The coverage provider's timestamp was not updated, because
        # we're using ensure_coverage on a single record.
        eq_(None,
            Timestamp.value(self._db, provider.service_name, collection=None)
        )

    def test_ensure_coverage_transient_coverage_failure(self):

        provider = TransientFailureCoverageProvider(self._db)
        failure = provider.ensure_coverage(self.identifier)
        eq_([failure], self.identifier.coverage_records)
        eq_(CoverageRecord.TRANSIENT_FAILURE, failure.status)
        eq_("Oops!", failure.exception)

        # Timestamp was not updated.
        eq_(None,
            Timestamp.value(self._db, provider.service_name, collection=None)
        )
        
    def test_ensure_coverage_changes_status(self):
        """Verify that processing an item that has a preexisting 
        CoverageRecord can change the status of that CoverageRecord.
        """
        always = AlwaysSuccessfulCoverageProvider(self._db)
        persistent = NeverSuccessfulCoverageProvider(self._db)
        transient = TransientFailureCoverageProvider(self._db)

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

    def test_edition(self):
        """Verify that CoverageProvider.edition() returns an appropriate
        Edition, even when there is no associated Collection.
        """
        # This CoverageProvider fetches bibliographic information
        # from Overdrive. It is not capable of creating LicensePools
        # because it has no Collection.
        provider = AlwaysSuccessfulCoverageProvider(self._db)
        eq_(None, provider.collection)
        
        # Here's an Identifier, with no Editions.
        identifier = self._identifier(identifier_type=Identifier.OVERDRIVE_ID)
        eq_([], identifier.primarily_identifies)
        
        # Calling CoverageProvider.edition() on the Identifier gives
        # us a container for the provider's bibliographic information,
        # as given to us by the provider's data source.
        #
        # It doesn't matter that there's no Collection, because the
        # book's bibliographic information is the same across
        # Collections.
        edition = provider.edition(identifier)
        eq_(provider.data_source, edition.data_source)
        eq_([edition], identifier.primarily_identifies)

        # Calling edition() again gives us the same Edition as before.
        edition2 = provider.edition(identifier)
        eq_(edition, edition2)

    def test_set_metadata(self):
        """Test that set_metadata can create and populate an
        appropriate Edition.

        set_metadata is tested in more detail in
        TestCollectionCoverageProvider.
        """
        # Here's a provider that is not associated with any particular
        # Collection.
        provider = AlwaysSuccessfulCoverageProvider(self._db)
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

    def test_items_that_need_coverage_respects_registration_reqs(self):
        provider = AlwaysSuccessfulCoverageProvider(
            self._db, registered_only=True
        )

        items = provider.items_that_need_coverage()
        assert self.identifier not in items

        # Once the identifier is registered, it shows up.
        provider.register(self.identifier)
        assert self.identifier in items

        # With a failing CoverageRecord, the item shows up.
        [record] = self.identifier.coverage_records
        record.status = CoverageRecord.TRANSIENT_FAILURE
        record.exception = 'Oh no!'
        assert self.identifier in items

    def test_items_that_need_coverage_respects_operation(self):

        # Here's a provider that carries out the 'foo' operation.
        class Mock1(AlwaysSuccessfulCoverageProvider):
            OPERATION = 'foo'
        provider = Mock1(self._db)

        # Here's a generic CoverageRecord for an identifier.
        record1 = CoverageRecord.add_for(self.identifier, provider.data_source)
        
        # That record doesn't count for purposes of
        # items_that_need_coverage, because the CoverageRecord doesn't
        # have an operation, and the CoverageProvider does.
        eq_([self.identifier], provider.items_that_need_coverage().all())

        # Here's a provider that has no operation set.
        provider = AlwaysSuccessfulCoverageProvider(self._db)
        eq_(None, provider.OPERATION)

        # For purposes of items_that_need_coverage, the identifier is
        # considered covered, because the operations match.
        eq_([], provider.items_that_need_coverage().all())

    def test_run_on_specific_identifiers(self):
        provider = AlwaysSuccessfulCoverageProvider(self._db)
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
        # self.identifier. But now something has gone wrong, and if we
        # ever run the coverage provider again we will get a
        # persistent failure.
        provider = NeverSuccessfulCoverageProvider(self._db)
        record, ignore = CoverageRecord.add_for(
            self.identifier, provider.data_source
        )
        record.timestamp = last_run

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

    def test_run_never_successful(self):
        """Verify that NeverSuccessfulCoverageProvider works the
        way we'd expect.
        """

        provider = NeverSuccessfulCoverageProvider(self._db)
        
        # We start with no CoverageRecords and no Timestamp.
        eq_([], self._db.query(CoverageRecord).all())
        eq_(None,
            Timestamp.value(self._db, provider.service_name, collection=None)
        )

        provider.run()

        # We have a CoverageRecord that signifies failure.
        [record] = self._db.query(CoverageRecord).all()
        eq_(self.identifier, record.identifier)
        eq_(record.data_source, provider.data_source)
        eq_("What did you expect?", record.exception)

        # But the coverage provider did run, and the timestamp is now set to
        # a recent value.
        value = Timestamp.value(self._db, provider.service_name, collection=None)
        assert (datetime.datetime.utcnow() - value).total_seconds() < 1

    def test_run_transient_failure(self):
        """Verify that TransientFailureCoverageProvider works the
        way we'd expect.
        """

        provider = TransientFailureCoverageProvider(self._db)

        # We start with no CoverageRecords and no Timestamp.
        eq_([], self._db.query(CoverageRecord).all())
        eq_(None,
            Timestamp.value(
                self._db, provider.service_name, collection=None
            )
        )

        now = datetime.datetime.utcnow()
        provider.run()

        # We have a CoverageRecord representing the transient failure.
        [failure] = self.identifier.coverage_records
        eq_(CoverageRecord.TRANSIENT_FAILURE, failure.status)

        # The timestamp was set.
        timestamp = Timestamp.value(
            self._db, provider.service_name, collection=None
        )
        assert (timestamp-now).total_seconds() < 1

    def test_add_coverage_record_for(self):
        """Calling CollectionCoverageProvider.add_coverage_record is the same
        as calling CoverageRecord.add_for with the relevant
        information.
        """
        provider = AlwaysSuccessfulCollectionCoverageProvider(
            self._default_collection
        )
        identifier = self._identifier()
        record = provider.add_coverage_record_for(identifier)

        # This is the same as calling CoverageRecord.add_for with
        # appropriate arguments.
        record2, is_new = CoverageRecord.add_for(
            identifier, data_source=provider.data_source, 
            operation=provider.operation,
            collection=provider.collection_or_not
        )
        eq_(False, is_new)
        eq_(record, record2)

        # By default, the CoverageRecord is not associated with any
        # particular collection.
        eq_(None, record.collection)
        
        # Setting COVERAGE_COUNTS_FOR_EVERY_COLLECTION to False will
        # change that -- a CoverageRecord will only count for the
        # collection associated with the CoverageProvider.
        provider.COVERAGE_COUNTS_FOR_EVERY_COLLECTION = False
        record = provider.add_coverage_record_for(identifier)
        eq_(self._default_collection, record.collection)

        record2, is_new = CoverageRecord.add_for(
            identifier, data_source=provider.data_source, 
            operation=provider.operation,
            collection=provider.collection_or_not
        )
        eq_(False, is_new)
        eq_(record, record2)

        
    def test_record_failure_as_coverage_record(self):
        """TODO: We need test coverage here."""

    def test_failure(self):
        provider = AlwaysSuccessfulCollectionCoverageProvider(
            self._default_collection
        )
        identifier = self._identifier()
        failure = provider.failure(
            identifier, error="an error", transient=False
        )
        eq_(provider.data_source, failure.data_source)
        eq_("an error", failure.exception)
        eq_(False, failure.transient)

        # By default, the failure is not associated with any
        # particular collection.
        eq_(None, failure.collection)
        
        # Setting COVERAGE_COUNTS_FOR_EVERY_COLLECTION to False
        # will change that -- a failure will only count for the
        # collection associated with the CoverageProvider.
        provider.COVERAGE_COUNTS_FOR_EVERY_COLLECTION = False
        failure = provider.failure(
            identifier, error="an error", transient=False
        )
        eq_(self._default_collection, failure.collection)

    def test_failure_for_ignored_item(self):
        """Test that failure_for_ignored_item creates an appropriate
        CoverageFailure.
        """
        provider = NeverSuccessfulCoverageProvider(self._db)
        result = provider.failure_for_ignored_item(self.identifier)
        assert isinstance(result, CoverageFailure)
        eq_(True, result.transient)
        eq_("Was ignored by CoverageProvider.", result.exception)
        eq_(self.identifier, result.obj)
        eq_(provider.data_source, result.data_source)


class TestCollectionCoverageProvider(CoverageProviderTest):

    # This data is used to test the insertion of circulation data
    # into a Collection.
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
    
    def test_class_variables(self):
        """Verify that class variables become appropriate instance
        variables.
        """
        collection = self._collection(protocol=ExternalIntegration.OPDS_IMPORT)
        provider = AlwaysSuccessfulCollectionCoverageProvider(collection)
        eq_(provider.DATA_SOURCE_NAME, provider.data_source.name)

    def test_must_have_collection(self):
        assert_raises_regexp(
            CollectionMissing,
            "AlwaysSuccessfulCollectionCoverageProvider must be instantiated with a Collection.",
            AlwaysSuccessfulCollectionCoverageProvider,
            None
        )

    def test_collection_protocol_must_match_class_protocol(self):
        collection = self._collection(protocol=ExternalIntegration.OVERDRIVE)
        assert_raises_regexp(
            ValueError,
            "Collection protocol \(Overdrive\) does not match CoverageProvider protocol \(OPDS Import\)",
            AlwaysSuccessfulCollectionCoverageProvider,
            collection
        )

    def test_items_that_need_coverage_ignores_collection_when_collection_is_irrelevant(self):

        # Two providers that do the same work, but one is associated
        # with a collection and the other is not.
        collection_provider = AlwaysSuccessfulCollectionCoverageProvider(
            self._default_collection
        )
        no_collection_provider = AlwaysSuccessfulCoverageProvider(
            self._db
        )

        # This distinction is irrelevant because they both consider an
        # Identifier covered when it has a CoverageRecord not
        # associated with any particular collection.
        eq_(True, collection_provider.COVERAGE_COUNTS_FOR_EVERY_COLLECTION)
        eq_(True, no_collection_provider.COVERAGE_COUNTS_FOR_EVERY_COLLECTION)

        eq_(collection_provider.data_source, 
            no_collection_provider.data_source)
        data_source = collection_provider.data_source

        # Create a license pool belonging to the default collection.
        pool = self._licensepool(None, collection=self._default_collection)
        identifier = pool.identifier

        def needs():
            """Returns all items that need coverage from both test
            CoverageProviders.
            """
            return tuple(
                p.items_that_need_coverage().all() for p in
                (collection_provider, no_collection_provider)
            )

        # We start out in the state where the identifier appears to need
        # coverage from both CoverageProviders.
        eq_(([identifier], [identifier]), needs())

        # Add coverage for the default collection, and both
        # CoverageProviders still consider the identifier
        # uncovered. (This shouldn't happen, but if it does, we don't
        # count it.)
        self._coverage_record(
            identifier, data_source, collection=self._default_collection
        )
        eq_(([identifier], [identifier]), needs())

        # Add coverage not associated with any collection, and both
        # CoverageProviders consider it covered.
        self._coverage_record(
            identifier, data_source, collection=None
        )
        eq_(([], []), needs())

    def test_items_that_need_coverage_respects_collection_when_collection_is_relevant(self):

        # Two providers that do the same work, but are associated
        # with different collections.
        collection_1_provider = AlwaysSuccessfulCollectionCoverageProvider(
            self._default_collection
        )
        collection_2 = self._collection()
        collection_2_provider = AlwaysSuccessfulCollectionCoverageProvider(
            collection_2
        )

        # And one that does the same work but is not associated with
        # any collection.
        no_collection_provider = AlwaysSuccessfulCoverageProvider(self._db)

        # The 'collection' distinction is relevant, because these
        # CoverageProviders consider an identifier covered only when
        # it has a CoverageRecord for _their_ collection.
        collection_1_provider.COVERAGE_COUNTS_FOR_EVERY_COLLECTION = False
        collection_2_provider.COVERAGE_COUNTS_FOR_EVERY_COLLECTION = False
        no_collection_provider.COVERAGE_COUNTS_FOR_EVERY_COLLECTION = False

        eq_(collection_1_provider.data_source, 
            collection_2_provider.data_source)
        data_source = collection_1_provider.data_source

        # Create a license pool belonging to the default collection so
        # that its Identifier will show up as needing coverage by the
        # CoverageProvider that manages that collection.
        pool = self._licensepool(None, collection=self._default_collection)
        identifier = pool.identifier

        def needs():
            """Returns all items that need coverage from both test
            CoverageProviders.
            """
            return tuple(
                p.items_that_need_coverage().all() for p in
                (collection_1_provider, no_collection_provider)
            )

        # We start out in the state where the identifier needs
        # coverage from the CoverageProvider not associated with
        # any Collection, and the CoverageProvider associated with
        # the Collection where the LicensePool lives.
        #
        eq_(([identifier], [identifier]), needs())

        # The CoverageProvider associated with a different Collection
        # doesn't care about this Identifier, because its Collection
        # doesn't include that Identiifer.
        eq_([], collection_2_provider.items_that_need_coverage().all())

        # Add coverage for an irrelevant collection, and nothing happens.
        self._coverage_record(
            identifier, data_source, collection=self._collection()
        )
        eq_(([identifier], [identifier]), needs())

        # Add coverage for a relevant collection, and it's treated as
        # covered by the provider that uses that collection.
        self._coverage_record(
            identifier, data_source, collection=self._default_collection
        )
        eq_(([], [identifier]), needs())

        # Add coverage not associated with a collection, and it's
        # treated as covered by the provider not associated with
        # any collection.
        self._coverage_record(identifier, data_source, collection=None)
        eq_(([], []), needs())

    def test_replacement_policy(self):
        """Unless a different replacement policy is passed in, the
        replacement policy is ReplacementPolicy.from_license_source().
        """
        provider = AlwaysSuccessfulCollectionCoverageProvider(
            self._default_collection
        )
        eq_(True, provider.replacement_policy.identifiers)
        eq_(True, provider.replacement_policy.formats)
        
        policy = ReplacementPolicy.from_metadata_source()
        provider = AlwaysSuccessfulCollectionCoverageProvider(
            self._default_collection, replacement_policy=policy
        )
        eq_(policy, provider.replacement_policy)
        
    def test_all(self):
        """Verify that all() gives a sequence of CollectionCoverageProvider
        objects, one for each Collection that implements the
        appropriate protocol.
        """
        opds1 = self._collection(protocol=ExternalIntegration.OPDS_IMPORT)
        opds2 = self._collection(protocol=ExternalIntegration.OPDS_IMPORT)
        overdrive = self._collection(protocol=ExternalIntegration.OVERDRIVE)
        providers = list(
            AlwaysSuccessfulCollectionCoverageProvider.all(self._db, batch_size=34)
        )

        # The providers were returned in a random order, but there's one
        # for each collection that supports the 'OPDS Import' protocol.
        eq_(2, len(providers))
        collections = set([x.collection for x in providers])
        eq_(set([opds1, opds2]), collections)

        # The providers are of the appropriate type and the keyword arguments
        # passed into all() were propagated to the constructor.
        for provider in providers:
            assert isinstance(provider, AlwaysSuccessfulCollectionCoverageProvider)
            eq_(34, provider.batch_size)

    def test_set_circulationdata_errors(self):
        """Verify that errors when setting circulation data
        are turned into CoverageFailure objects.
        """
        provider = AlwaysSuccessfulCollectionCoverageProvider(
            self._default_collection
        )
        identifier = self._identifier()

        # No data.
        failure = provider._set_circulationdata(identifier, None)
        eq_("Did not receive circulationdata from input source",
            failure.exception)

        # No identifier in CirculationData.
        empty = CirculationData(provider.data_source, primary_identifier=None)
        failure = provider._set_circulationdata(identifier, empty)
        eq_("Identifier did not match CirculationData's primary identifier.",
            failure.exception)

        # Mismatched identifier in CirculationData.
        wrong = CirculationData(provider.data_source,
                                primary_identifier=self._identifier())
        failure = provider._set_circulationdata(identifier, empty)
        eq_("Identifier did not match CirculationData's primary identifier.",
            failure.exception)

        # Here, the data is okay, but the ReplacementPolicy is
        # going to cause an error the first time we try to use it.
        correct = CirculationData(provider.data_source,
                                  identifier)
        provider.replacement_policy = object()
        failure = provider._set_circulationdata(identifier, correct)
        assert isinstance(failure, CoverageFailure)

        # Verify that the general error handling works whether or not
        # the provider is associated with a Collection.
        provider.collection_id = None
        failure = provider._set_circulationdata(identifier, correct)
        assert isinstance(failure, CoverageFailure)
        
    def test_set_metadata_incorporates_replacement_policy(self):
        """Make sure that if a ReplacementPolicy is passed in to
        set_metadata(), the policy's settings (and those of its
        .presentation_calculation_policy) are respected.

        This is tested in this class rather than in
        TestIdentifierCoverageProvider because with a collection in
        place we can test a lot more aspects of the ReplacementPolicy.
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
        mirror = MockS3Uploader()

        class Tripwire(PresentationCalculationPolicy):
            # This class sets a variable if one of its properties is
            # accessed.
            def __init__(self, *args, **kwargs):
                self.tripped = False

            def __getattr__(self, name):
                self.tripped = True
                return True

        presentation_calculation_policy = Tripwire()
        replacement_policy = ReplacementPolicy(
            mirror=mirror,
            http_get=http.do_get,
            presentation_calculation_policy=presentation_calculation_policy
        )

        provider = AlwaysSuccessfulCollectionCoverageProvider(
            self._default_collection, replacement_policy=replacement_policy
        )

        metadata = Metadata(provider.data_source, primary_identifier=identifier)
        # We've got a CirculationData object that includes an open-access download.
        link = LinkData(rel=Hyperlink.OPEN_ACCESS_DOWNLOAD, href="http://foo.com/")

        # We get an error if the CirculationData's identifier is
        # doesn't match what we pass in.
        circulationdata = CirculationData(
            provider.data_source, 
            primary_identifier=self._identifier(),
            links=[link]
        )
        failure = provider.set_metadata_and_circulation_data(
            identifier, metadata, circulationdata
        )
        eq_("Identifier did not match CirculationData's primary identifier.",
            failure.exception)

        # Otherwise, the data is applied.
        circulationdata = CirculationData(
            provider.data_source, 
            primary_identifier=metadata.primary_identifier, 
            links=[link]
        )

        provider.set_metadata_and_circulation_data(
            identifier, metadata, circulationdata
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

    def test_items_that_need_coverage(self):
        # Here's an Identifier that was covered on 01/01/2016.
        identifier = self._identifier()
        cutoff_time = datetime.datetime(2016, 1, 1)
        provider = AlwaysSuccessfulCoverageProvider(self._db)
        record, is_new = CoverageRecord.add_for(
            identifier, provider.data_source, timestamp=cutoff_time
        )

        # Since the Identifier was covered, it doesn't show up in
        # items_that_need_coverage.
        eq_([], provider.items_that_need_coverage().all())

        # If we set the CoverageProvider's cutoff_time to the time of
        # coverage, the Identifier is still treated as covered.
        provider = AlwaysSuccessfulCoverageProvider(
            self._db, cutoff_time=cutoff_time
        )
        eq_([], provider.items_that_need_coverage().all())
        
        # But if we set the cutoff time to immediately after the time
        # the Identifier was covered...
        one_second_after = cutoff_time + datetime.timedelta(seconds=1)
        provider = AlwaysSuccessfulCoverageProvider(
            self._db, cutoff_time=one_second_after
        )

        # The identifier is treated as lacking coverage.
        eq_([identifier], 
            provider.items_that_need_coverage().all())
    
    def test_work(self):
        """Verify that a CollectionCoverageProvider can create a Work."""
        # Here's an Gutenberg ID.
        identifier = self._identifier(identifier_type=Identifier.GUTENBERG_ID)

        # Here's a CollectionCoverageProvider that is associated
        # with an OPDS import-style Collection.
        provider = AlwaysSuccessfulCollectionCoverageProvider(
            self._default_collection
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

        # If necessary, we can tell work() to use a specific
        # LicensePool when calculating the Work. This is an extreme
        # example in which the LicensePool to use has a different
        # Identifier (identifier2) than the Identifier we're
        # processing (identifier1).
        #
        # In a real case, this would be used by a CoverageProvider
        # that just had to create a LicensePool using an
        # INTERNAL_PROCESSING DataSource rather than the DataSource
        # associated with the CoverageProvider.
        identifier2 = self._identifier()
        identifier.licensed_through = []
        collection2 = self._collection()
        edition2 = self._edition(identifier_type=identifier2.type,
                                 identifier_id=identifier2.identifier)
        pool2 = self._licensepool(edition=edition2, collection=collection2)
        work2 = provider.work(identifier, pool2)
        assert work2 != work
        eq_([pool2], work2.license_pools)

        # Once an identifier has a work associated with it,
        # that's always the one that's used, and the value of license_pool
        # is ignored.
        work3 = provider.work(identifier2, object())
        eq_(work2, work3)

        # Any keyword arguments passed into work() are propagated to
        # calculate_work(). This lets use (e.g.) create a Work even
        # when there is no title.
        edition, pool = self._edition(with_license_pool=True)
        edition.title = None
        work = provider.work(pool.identifier, pool, even_if_no_title=True)
        assert isinstance(work, Work)
        eq_(None, work.title)

    def test_set_metadata_and_circulationdata(self):
        """Verify that a CollectionCoverageProvider can set both
        metadata (on an Edition) and circulation data (on a LicensePool).
        """
        test_metadata = self.BIBLIOGRAPHIC_DATA
        test_circulationdata = self.CIRCULATION_DATA

        # Here's an Overdrive Identifier to work with.
        identifier = self._identifier(
            identifier_type=Identifier.OVERDRIVE_ID,
            foreign_id=self.BIBLIOGRAPHIC_DATA.primary_identifier.identifier, 
        )

        # Here's a CollectionCoverageProvider that is associated with
        # an Overdrive-type Collection. (We have to subclass and talk
        # about Overdrive because BIBLIOGRAPHIC_DATA and
        # CIRCULATION_DATA are data for an Overdrive book.)
        class OverdriveProvider(AlwaysSuccessfulCollectionCoverageProvider):
            DATA_SOURCE_NAME = DataSource.OVERDRIVE
            PROTOCOL = ExternalIntegration.OVERDRIVE
            IDENTIFIER_TYPES = Identifier.OVERDRIVE_ID
        collection = self._collection(protocol=ExternalIntegration.OVERDRIVE)
        provider = OverdriveProvider(collection)

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
        
        # CoverageProviders that offer bibliographic information
        # typically don't have circulation information in the sense of
        # 'how many copies are in this Collection?', but sometimes
        # they do have circulation information in the sense of 'what
        # formats are available?'
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
        """A CollectionCoverageProvider can locate (or, if necessary, create)
        a LicensePool for an identifier.
        """
        identifier = self._identifier()
        eq_([], identifier.licensed_through)
        provider = AlwaysSuccessfulCollectionCoverageProvider(
            self._default_collection
        )
        pool = provider.license_pool(identifier)
        eq_([pool], identifier.licensed_through)
        eq_(pool.data_source, provider.data_source)
        eq_(pool.identifier, identifier)
        eq_(pool.collection, provider.collection)

        # Calling license_pool again finds the same LicensePool
        # as before.
        pool2 = provider.license_pool(identifier)
        eq_(pool, pool2)

        # It's possible for a CollectionCoverageProvider to create a
        # LicensePool for a different DataSource than the one
        # associated with the Collection. Only the metadata wrangler
        # needs to do this -- it's so a CoverageProvider for a
        # third-party DataSource can create an 'Internal Processing'
        # LicensePool when some other part of the metadata wrangler
        # failed to do this earlier.

        # If a working pool already exists, it's returned and no new
        # pool is created.
        same_pool = provider.license_pool(
            identifier, DataSource.INTERNAL_PROCESSING
        )
        eq_(same_pool, pool2)
        eq_(provider.data_source, same_pool.data_source)

        # A new pool is only created if no working pool can be found.
        identifier2 = self._identifier()
        new_pool = provider.license_pool(
            identifier2, DataSource.INTERNAL_PROCESSING
        )
        eq_(new_pool.data_source.name, DataSource.INTERNAL_PROCESSING)
        eq_(new_pool.identifier, identifier2)
        eq_(new_pool.collection, provider.collection)

    def test_set_presentation_ready(self):
        """Test that a CollectionCoverageProvider can set a Work
        as presentation-ready.
        """
        identifier = self._identifier()
        provider = AlwaysSuccessfulCollectionCoverageProvider(
            self._default_collection
        )

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


class TestCatalogCoverageProvider(CoverageProviderTest):

    def test_items_that_need_coverage(self):

        c1 = self._collection()
        c2 = self._collection()

        i1 = self._identifier()
        c1.catalog_identifier(i1)

        i2 = self._identifier()
        c2.catalog_identifier(i2)

        i3 = self._identifier()

        # This Identifier is licensed through the Collection c1, but
        # it's not in the catalog--catalogs are used for different
        # things.
        edition, lp = self._edition(with_license_pool=True,
                                    collection=c1)
        
        # We have four identifiers, but only i1 shows up, because
        # it's the only one in c1's catalog.
        class Provider(CatalogCoverageProvider):
            SERVICE_NAME = "test"
            DATA_SOURCE_NAME = DataSource.OVERDRIVE
            pass

        provider = Provider(c1)
        eq_([i1], provider.items_that_need_coverage().all())


class TestBibliographicCoverageProvider(CoverageProviderTest):
    """Test the features specific to BibliographicCoverageProvider."""

    def setup(self):
        super(TestBibliographicCoverageProvider, self).setup()
        self.work = self._work(
            with_license_pool=True, with_open_access_download=True
        )
        self.work.presentation_ready = False
        [self.pool] = self.work.license_pools
        self.identifier = self.pool.identifier
        
    def test_work_set_presentation_ready_on_success(self):
        """When a Work is successfully run through a
        BibliographicCoverageProvider, it's set as presentation-ready.
        """
        provider = AlwaysSuccessfulBibliographicCoverageProvider(
            self.pool.collection
        )
        [result] = provider.process_batch([self.identifier])
        eq_(result, self.identifier)
        eq_(True, self.work.presentation_ready)

        # ensure_coverage does the same thing.
        self.work.presentation_ready = False
        result = provider.ensure_coverage(self.identifier)
        assert isinstance(result, CoverageRecord)
        eq_(result.identifier, self.identifier)
        eq_(True, self.work.presentation_ready)

    def test_failure_does_not_set_work_presentation_ready(self):
        """A Work is not set as presentation-ready except on success.
        """

        provider = NeverSuccessfulBibliographicCoverageProvider(
            self.pool.collection
        )
        result = provider.ensure_coverage(self.identifier)
        eq_(CoverageRecord.TRANSIENT_FAILURE, result.status)
        eq_(False, self.work.presentation_ready)


class TestWorkCoverageProvider(DatabaseTest):

    def setup(self):
        super(TestWorkCoverageProvider, self).setup()
        self.work = self._work()

    def test_success(self):
        class MockProvider(AlwaysSuccessfulWorkCoverageProvider):
            OPERATION = "the_operation"
        
        qu = self._db.query(WorkCoverageRecord).filter(
            WorkCoverageRecord.operation==MockProvider.OPERATION
        )
        provider = MockProvider(self._db)
        
        # We start with no relevant WorkCoverageRecord and no Timestamp.
        eq_([], qu.all())
        eq_(None, Timestamp.value(
            self._db, provider.service_name, collection=None)
        )

        now = datetime.datetime.utcnow()
        provider.run()

        # There is now one relevant WorkCoverageRecord, for our single work.
        [record] = qu.all()
        eq_(self.work, record.work)
        eq_(provider.operation, record.operation)

        # The timestamp is now set.
        timestamp = Timestamp.value(self._db, provider.service_name, collection=None)
        assert (timestamp-now).total_seconds() < 1

    def test_transient_failure(self):
        class MockProvider(TransientFailureWorkCoverageProvider):
            OPERATION = "the_operation"
        provider = MockProvider(self._db)
            
        # We start with no relevant WorkCoverageRecords.
        qu = self._db.query(WorkCoverageRecord).filter(
            WorkCoverageRecord.operation==provider.operation
        )
        eq_([], qu.all())

        provider.run()

        # We now have a CoverageRecord for the transient failure.
        [failure] = [x for x in self.work.coverage_records if 
                     x.operation==provider.operation]
        eq_(CoverageRecord.TRANSIENT_FAILURE, failure.status)

        # The timestamp is now set to a recent value.
        service_name = "Never successful (transient, works) (the_operation)"
        value = Timestamp.value(self._db, service_name, collection=None)
        assert (datetime.datetime.now()-value).total_seconds() < 2

    def test_persistent_failure(self):
        class MockProvider(NeverSuccessfulWorkCoverageProvider):
            OPERATION = "the_operation"
        provider = MockProvider(self._db)

        # We start with no relevant WorkCoverageRecords.
        qu = self._db.query(WorkCoverageRecord).filter(
            WorkCoverageRecord.operation==provider.operation
        )
        eq_([], qu.all())

        provider.run()

        # We have a WorkCoverageRecord, since the error was persistent.
        [record] = qu.all()
        eq_(self.work, record.work)
        eq_("What did you expect?", record.exception)

        # The timestamp is now set to a recent value.
        service_name = "Never successful (works) (the_operation)"
        value = Timestamp.value(self._db, service_name, collection=None)
        assert (datetime.datetime.now()-value).total_seconds() < 2

    def test_items_that_need_coverage(self):
        # Here's a WorkCoverageProvider.
        provider = AlwaysSuccessfulWorkCoverageProvider(self._db)

        # Here are three works,
        w1 = self.work
        w2 = self._work(with_license_pool=True)
        w3 = self._work(with_license_pool=True)
        
        # w2 has coverage, the other two do not.
        record = self._work_coverage_record(w2, provider.operation)

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
        class MockProvider(NeverSuccessfulWorkCoverageProvider):
            OPERATION = "the_operation"

        provider = NeverSuccessfulWorkCoverageProvider(self._db)
        result = provider.failure_for_ignored_item(self.work)
        assert isinstance(result, CoverageFailure)
        eq_(True, result.transient)
        eq_("Was ignored by WorkCoverageProvider.", result.exception)
        eq_(self.work, result.obj)
        
    def test_add_coverage_record_for(self):
        """TODO: We have coverage of code that calls this method,
        but not the method itself.
        """

    def test_record_failure_as_coverage_record(self):
        """TODO: We have coverage of code that calls this method,
        but not the method itself.
        """


class TestPresentationReadyWorkCoverageProvider(DatabaseTest):

    def test_items_that_need_coverage(self):

        class Mock(PresentationReadyWorkCoverageProvider):
            SERVICE_NAME = 'mock'

        provider = Mock(self._db)
        work = self._work()

        # The work is not presentation ready and so is not ready for
        # coverage.
        eq_(False, work.presentation_ready)
        eq_([], provider.items_that_need_coverage().all())

        # Make it presentation ready, and it needs coverage.
        work.presentation_ready = True
        eq_([work], provider.items_that_need_coverage().all())


class TestOPDSEntryWorkCoverageProvider(DatabaseTest):

    def test_run(self):

        provider = OPDSEntryWorkCoverageProvider(self._db)
        work = self._work()
        work.simple_opds_entry = 'old junk'
        work.verbose_opds_entry = 'old long junk'

        # The work is not presentation-ready, so nothing happens.
        provider.run()
        eq_('old junk', work.simple_opds_entry)
        eq_('old long junk', work.verbose_opds_entry)

        # The work is presentation-ready, so its OPDS entries are
        # regenerated.
        work.presentation_ready = True
        provider.run()
        assert work.simple_opds_entry.startswith('<entry')
        assert work.verbose_opds_entry.startswith('<entry')

