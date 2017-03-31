from nose.tools import (
    eq_, 
    set_trace,
    assert_raises,
    assert_raises_regexp,
)
import datetime

from . import DatabaseTest

from testing import (
    AlwaysSuccessfulCoverageProvider,
    NeverSuccessfulCoverageProvider,
    BrokenCoverageProvider,
)

from model import (
    Collection,
    CollectionMissing,
    DataSource,
    Identifier,
    Subject,
    Timestamp,
)

from monitor import (
    CollectionMonitor,
    CustomListEntrySweepMonitor,
    CustomListEntryWorkUpdateMonitor,
    EditionSweepMonitor,
    IdentifierSweepMonitor,
    Monitor,
    OPDSEntryCacheMonitor,
    PermanentWorkIDRefreshMonitor,
    PresentationReadyMonitor,
    PresentationReadyWorkSweepMonitor,
    SubjectAssignmentMonitor,
    SubjectSweepMonitor,
    SweepMonitor,
    WorkRandomnessUpdateMonitor,
    WorkSweepMonitor,
)

class MockMonitor(Monitor):

    SERVICE_NAME = "Dummy monitor for test"
    
    def __init__(self, _db, collection=None):
        super(MockMonitor, self).__init__(_db, collection)
        self.run_records = []
        self.cleanup_records = []

    def run_once(self, start, cutoff):
        self.original_timestamp = start
        self.run_records.append(True)

    def cleanup(self):
        self.cleanup_records.append(True)


class TestMonitor(DatabaseTest):

    def test_must_define_service_name(self):

        class NoServiceName(MockMonitor):
            SERVICE_NAME = None

        assert_raises_regexp(
            ValueError,
            "NoServiceName must define SERVICE_NAME.",
            NoServiceName,
            self._db
        )
    
    def test_monitor_lifecycle(self):
        monitor = MockMonitor(self._db, self._default_collection)
        eq_(self._default_collection, monitor.collection)
        
        # There is no timestamp for this monitor.
        eq_([], self._db.query(Timestamp).filter(
            Timestamp.service==monitor.service_name).all())

        # Run the monitor.
        monitor.run()

        # The monitor ran once and then stopped.
        eq_([True], monitor.run_records)

        # cleanup() was called once.
        eq_([True], monitor.cleanup_records)

        # A timestamp was put into the database when we ran the
        # monitor.
        timestamp = self._db.query(Timestamp).filter(
            Timestamp.service==monitor.service_name).one()

        # The current value of the timestamp later than the
        # original value, because it was updated after run_once() was
        # called.
        assert timestamp.timestamp > monitor.original_timestamp

    def test_same_monitor_different_collections(self):
        """A single Monitor has different Timestamps when run against
        different Collections.
        """
        c1 = self._collection()
        c2 = self._collection()
        m1 = MockMonitor(self._db, c1)
        m2 = MockMonitor(self._db, c2)

        # The two Monitors have the same service name but are operating
        # on different Collections.
        eq_(m1.service_name, m2.service_name)
        eq_(c1, m1.collection)
        eq_(c2, m2.collection)
        
        eq_([], c1.timestamps)
        eq_([], c2.timestamps)
        
        # Run the first Monitor.
        m1.run()
        [t1] = c1.timestamps
        eq_(m1.service_name, t1.service)
        eq_(m1.collection, t1.collection)
        old_m1_timestamp = m1.timestamp

        # Running the first Monitor did not create a timestamp for the
        # second Monitor.
        eq_([], c2.timestamps)
        
        # Run the second monitor.
        m2.run()

        # The timestamp for the first monitor was not updated when
        # we ran the second monitor.
        eq_(old_m1_timestamp, m1.timestamp)

        # But the second Monitor now has its own timestamp.
        [t2] = c2.timestamps
        assert t2.timestamp > t1.timestamp


class TestCollectionMonitor(DatabaseTest):
    """Test the special features of CollectionMonitor."""

    def test_protocol_enforcement(self):
        """A CollectionMonitor can require that it be instantiated
        with a Collection that implements a certain protocol.
        """
        class NoProtocolMonitor(CollectionMonitor):
            SERVICE_NAME = "Test Monitor 1"
            PROTOCOL = None

        class OverdriveMonitor(CollectionMonitor):
            SERVICE_NAME = "Test Monitor 2"
            PROTOCOL = Collection.OVERDRIVE

        # Two collections.
        c1 = self._collection(protocol=Collection.OVERDRIVE)
        c2 = self._collection(protocol=Collection.BIBLIOTHECA)

        # The NoProtocolMonitor can be instantiated with either one,
        # or with no Collection at all.
        NoProtocolMonitor(self._db, c1)
        NoProtocolMonitor(self._db, c2)
        NoProtocolMonitor(self._db, None)

        # The OverdriveMonitor can only be instantiated with the first one.
        OverdriveMonitor(self._db, c1)
        assert_raises_regexp(
            ValueError,
            "Collection protocol \(Bibliotheca\) does not match Monitor protocol \(Overdrive\)",
            OverdriveMonitor, self._db, c2
        )
        assert_raises(
            CollectionMissing,
            OverdriveMonitor, self._db, None
        )
        
    def test_all(self):
        """Test that we can create a list of Monitors using all()."""
        class OPDSCollectionMonitor(CollectionMonitor):
            SERVICE_NAME = "Test Monitor"
            PROTOCOL = Collection.OPDS_IMPORT
        
        # Here we have three OPDS import Collections...
        o1 = self._collection()
        o2 = self._collection()
        o3 = self._collection()

        # ...and a Bibliotheca collection.
        b1 = self._collection(protocol=Collection.BIBLIOTHECA)

        # o1 just had its Monitor run.
        Timestamp.stamp(self._db, OPDSCollectionMonitor.SERVICE_NAME, o1)

        # o2 and b1 have never had their Monitor run.

        # o3 had its Monitor run an hour ago.
        now = datetime.datetime.utcnow()
        an_hour_ago = now - datetime.timedelta(seconds=3600)
        Timestamp.stamp(self._db, OPDSCollectionMonitor.SERVICE_NAME,
                        o3, an_hour_ago)
        
        monitors = list(OPDSCollectionMonitor.all(self._db))

        # Three OPDSCollectionMonitors were returned, one for each
        # appropriate collection. The monitor that needs to be run the
        # worst was returned first in the list. The monitor that was
        # run most recently is returned last. There is no
        # OPDSCollectionMonitor for the Bibliotheca collection.
        eq_([o2, o3, o1], [x.collection for x in monitors])


class MockSweepMonitor(SweepMonitor):
    """A SweepMonitor that does nothing."""
    MODEL_CLASS = Identifier
    SERVICE_NAME = "Sweep Monitor"
    DEFAULT_BATCH_SIZE = 2
    
    def __init__(self, _db, **kwargs):
        super(MockSweepMonitor, self).__init__(_db, **kwargs)
        self.cleanup_called = []
        self.batches = []
        self.processed = []
    
    def scope_to_collection(self, qu, collection):
        return qu

    def process_batch(self, batch):
        self.batches.append(batch)
        return super(MockSweepMonitor, self).process_batch(batch)
            
    def process_item(self, item):
        self.processed.append(item)

    def cleanup(self):
        self.cleanup_called.append(True)


class TestSweepMonitor(DatabaseTest):

    def setup(self):
        super(TestSweepMonitor, self).setup()
        self.monitor = MockSweepMonitor(self._db)

    def test_model_class_is_required(self):
        class NoModelClass(SweepMonitor):
            MODEL_CLASS = None
        assert_raises_regexp(
            ValueError,
            "NoModelClass must define MODEL_CLASS",
            NoModelClass, self._db
        )

    def test_batch_size(self):
        eq_(MockSweepMonitor.DEFAULT_BATCH_SIZE, self.monitor.batch_size)
        
        monitor = MockSweepMonitor(self._db, batch_size=29)
        eq_(29, monitor.batch_size)

        # If you pass in an invalid value you get the default.
        monitor = MockSweepMonitor(self._db, batch_size=-1)
        eq_(MockSweepMonitor.DEFAULT_BATCH_SIZE, monitor.batch_size)

    def test_run_sweeps_entire_table(self):
        # Three Identifiers -- the batch size is 2.
        i1, i2, i3 = [self._identifier() for i in range(3)]
        eq_(2, self.monitor.batch_size)
        
        # Run the monitor.
        self.monitor.run()

        # All three Identifiers, and no other items, were processed.
        eq_([i1, i2, i3], self.monitor.processed)

        # We ran process_batch() three times: once starting at zero,
        # once starting at the ID that ended the first batch, and
        # again starting at the ID that ended the second batch.
        eq_([0, i2.id, i3.id], self.monitor.batches)

        # The cleanup method was called once.
        eq_([True], self.monitor.cleanup_called)

    def test_run_starts_at_previous_counter(self):
        # Two Identifiers.
        i1, i2 = [self._identifier() for i in range(2)]
        
        # The monitor was just run, but it was not able to proceed past
        # i1.
        timestamp = Timestamp.stamp(
            self._db, self.monitor.service_name, self.monitor.collection
        )
        timestamp.counter = i1.id
        
        # Run the monitor.
        self.monitor.run()

        # The last item in the table was processed. i1 was not
        # processed, because it was processed in a previous run.
        eq_([i2], self.monitor.processed)

        # The monitor's counter has been reset.
        eq_(0, timestamp.counter)

    def test_exception_interrupts_run(self):

        # Four Identifiers.
        i1, i2, i3, i4 = [self._identifier() for i in range(4)]

        # This monitor will never be able to process the fourth one.
        class IHateI4(MockSweepMonitor):
            def process_item(self, item):
                if item is i4:
                    raise Exception("HOW DARE YOU")
                super(IHateI4, self).process_item(item)

        monitor = IHateI4(self._db)
        monitor.run()

        # The monitor's counter was updated to the ID of the final
        # item in the last batch it was able to process. In this case,
        # this is I2.
        timestamp = monitor.timestamp()
        eq_(i2.id, timestamp.counter)

        # I3 was processed, but the batch did not complete, so any
        # changes wouldn't have been written to the database.
        eq_([i1, i2, i3], monitor.processed)
                
        # Running the monitor again will process I3 again, but the same error
        # will happen on i4 and the counter will not be updated.
        monitor.run()
        eq_([i1, i2, i3, i3], monitor.processed)
        eq_(i2.id, timestamp.counter)

        # cleanup() is only called when the sweep completes successfully.
        eq_([], monitor.cleanup_called)


class TestIdentifierSweepMonitor(DatabaseTest):

    def test_scope_to_collection(self):
        # Two Collections, each with a LicensePool.
        c1 = self._collection()
        c2 = self._collection()
        e1, p1 = self._edition(with_license_pool=True, collection=c1)
        e2, p2 = self._edition(with_license_pool=True, collection=c2)

        # A Random Identifier not associated with any Collection.
        i3 = self._identifier()

        class Mock(IdentifierSweepMonitor):
            SERVICE_NAME = "Mock"

        # With a Collection, we only process items that are licensed through
        # that collection.
        monitor = Mock(self._db, c1)
        eq_([p1.identifier], monitor.item_query().all())
            
        # With no Collection, we process all items.
        monitor = Mock(self._db, None)
        eq_([p1.identifier, p2.identifier, i3], monitor.item_query().all())


class TestSubjectSweepMonitor(DatabaseTest):

    def test_item_query(self):

        s1, ignore = Subject.lookup(self._db, Subject.DDC, "100", None)
        s2, ignore = Subject.lookup(
            self._db, Subject.TAG, None, "100 Years of Solitude"
        )

        # By default, SubjectSweepMonitor handles every Subject
        # in the database, whether or not a collection is provided.
        everything = SubjectSweepMonitor(self._db, collection=None)
        eq_([s1, s2], everything.item_query().all())
        everything = SubjectSweepMonitor(
            self._db, collection=self._default_collection
        )
        eq_([s1, s2], everything.item_query().all())

        # But you can tell SubjectSweepMonitor to handle only Subjects
        # of a certain type.
        dewey_monitor = SubjectSweepMonitor(
            self._db, collection=None, subject_type=Subject.DDC
        )
        eq_([s1], dewey_monitor.item_query().all())

        # You can also SubjectSweepMonitor to handle only Subjects
        # whose names or identifiers match a certain string.
        one_hundred_monitor = SubjectSweepMonitor(
            self._db, collection=None, filter_string="100"
        )
        eq_([s1, s2], one_hundred_monitor.item_query().all())

        specific_tag_monitor = SubjectSweepMonitor(
            self._db, collection=None, subject_type=Subject.TAG,
            filter_string="Years"
        )
        eq_([s2], specific_tag_monitor.item_query().all())


class TestPresentationReadyMonitor(DatabaseTest):

    def setup(self):
        super(TestPresentationReadyMonitor, self).setup()
        self.gutenberg_id = Identifier.GUTENBERG_ID
        self.oclc = DataSource.lookup(self._db, DataSource.OCLC)
        self.overdrive = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        self.edition, self.edition_license_pool = self._edition(DataSource.GUTENBERG, with_license_pool=True)
        self.work = self._work(
            DataSource.GUTENBERG, with_license_pool=True)
        # Don't fake that the work is presentation ready, as we usually do,
        # because presentation readiness is what we're trying to test.
        self.work.presentation_ready = False

    def test_make_batch_presentation_ready_sets_presentation_ready_on_success(self):
        class MockProvider(AlwaysSuccessfulCoverageProvider):
            SERVICE_NAME = "Provider 1"
            INPUT_IDENTIFIER_TYPES = Identifier.GUTENBERG_ID,
            DATA_SOURCE_NAME = DataSource.OCLC
        provider = MockProvider(self._db)
        monitor = PresentationReadyMonitor(self._db, [])
        monitor.process_batch([self.work])
        eq_(None, self.work.presentation_ready_exception)
        eq_(True, self.work.presentation_ready)

    def test_make_batch_presentation_ready_sets_exception_on_failure(self):
        class MockProvider1(AlwaysSuccessfulCoverageProvider):
            SERVICE_NAME = "Provider 1"
            INPUT_IDENTIFIER_TYPES = Identifier.GUTENBERG_ID
            DATA_SOURCE_NAME = DataSource.OCLC
        success = MockProvider1(self._db)

        class MockProvider2(NeverSuccessfulCoverageProvider):
            SERVICE_NAME = "Provider 2"
            INPUT_IDENTIFIER_TYPES = Identifier.GUTENBERG_ID
            DATA_SOURCE_NAME = DataSource.OVERDRIVE
        failure = MockProvider2(self._db)

        monitor = PresentationReadyMonitor(self._db, [success, failure])
        monitor.process_batch([self.work])
        eq_(False, self.work.presentation_ready)
        eq_(
            "Provider(s) failed: Provider 2",
            self.work.presentation_ready_exception)
        
    def test_prepare_returns_failing_providers(self):
        class MockProvider1(AlwaysSuccessfulCoverageProvider):
            SERVICE_NAME = "Provider 1"
            INPUT_IDENTIFIER_TYPES = Identifier.GUTENBERG_ID
            DATA_SOURCE_NAME = DataSource.OCLC
        success = MockProvider1(self._db)

        class MockProvider2(NeverSuccessfulCoverageProvider):
            SERVICE_NAME = "Provider 2"
            INPUT_IDENTIFIER_TYPES = Identifier.GUTENBERG_ID
            DATA_SOURCE_NAME = DataSource.OVERDRIVE
        failure = MockProvider2(self._db)
        monitor = PresentationReadyMonitor(self._db, [success, failure])
        result = monitor.prepare(self.work)
        eq_([failure], result)
        
    def test_irrelevant_provider_is_not_called(self):

        class GutenbergProvider(AlwaysSuccessfulCoverageProvider):
            SERVICE_NAME = "Gutenberg monitor"
            DATA_SOURCE_NAME = DataSource.OCLC
            INPUT_IDENTIFIER_TYPES = Identifier.GUTENBERG_ID
        gutenberg_monitor = GutenbergProvider(self._db)
            
        class OCLCProvider(NeverSuccessfulCoverageProvider):
            SERVICE_NAME = "OCLC monitor"
            DATA_SOURCE_NAME = DataSource.OCLC
            INPUT_IDENTIFIER_TYPES = Identifier.OCLC_NUMBER
        oclc_monitor = OCLCProvider(self._db)
            
        monitor = PresentationReadyMonitor(
            self._db, [gutenberg_monitor, oclc_monitor]
        )
        result = monitor.prepare(self.work)

        # There were no failures.
        eq_([], result)

        # The monitor that takes Gutenberg identifiers as input ran.
        eq_([self.work.presentation_edition.primary_identifier],
            gutenberg_monitor.attempts)

        # The monitor that takes OCLC editions as input did not.
        # (If it had, it would have failed.)
        eq_([], oclc_monitor.attempts)

        # The work has not been set to presentation ready--that's
        # handled elsewhere.
        eq_(False, self.work.presentation_ready)
