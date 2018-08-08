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
    CachedFeed,
    Collection,
    CollectionMissing,
    Credential,
    DataSource,
    ExternalIntegration,
    Identifier,
    Subject,
    Timestamp,
    Work,
    WorkCoverageRecord,
)

from monitor import (
    CachedFeedReaper,
    CollectionMonitor,
    CoverageProvidersFailed,
    CredentialReaper,
    CustomListEntrySweepMonitor,
    CustomListEntryWorkUpdateMonitor,
    EditionSweepMonitor,
    IdentifierSweepMonitor,
    MakePresentationReadyMonitor,
    Monitor,
    NotPresentationReadyWorkSweepMonitor,
    OPDSEntryCacheMonitor,
    PermanentWorkIDRefreshMonitor,
    PresentationReadyWorkSweepMonitor,
    ReaperMonitor,
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

    def test_collection(self):
        monitor = MockMonitor(self._db, self._default_collection)
        eq_(self._default_collection, monitor.collection)
        monitor.collection_id = None
        eq_(None, monitor.collection)

    def test_monitor_lifecycle(self):
        monitor = MockMonitor(self._db, self._default_collection)

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

    def test_initial_timestamp(self):
        class NeverRunMonitor(MockMonitor):
            SERVICE_NAME = "Never run"
            DEFAULT_START_TIME = MockMonitor.NEVER

        # The Timestamp object is created, but its .timestamp is None.
        m = NeverRunMonitor(self._db, self._default_collection)
        eq_(None, m.timestamp().timestamp)

        class RunLongAgoMonitor(MockMonitor):
            SERVICE_NAME = "Run long ago"
            DEFAULT_START_TIME = MockMonitor.ONE_YEAR_AGO
        # The Timestamp object is created, and its .timestamp is long ago.
        m = RunLongAgoMonitor(self._db, self._default_collection)
        timestamp = m.timestamp().timestamp
        now = datetime.datetime.utcnow()
        assert timestamp < now

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
            PROTOCOL = ExternalIntegration.OVERDRIVE

        # Two collections.
        c1 = self._collection(protocol=ExternalIntegration.OVERDRIVE)
        c2 = self._collection(protocol=ExternalIntegration.BIBLIOTHECA)

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
            PROTOCOL = ExternalIntegration.OPDS_IMPORT

        # Here we have three OPDS import Collections...
        o1 = self._collection()
        o2 = self._collection()
        o3 = self._collection()

        # ...and a Bibliotheca collection.
        b1 = self._collection(protocol=ExternalIntegration.BIBLIOTHECA)

        # o1 just had its Monitor run.
        Timestamp.stamp(self._db, OPDSCollectionMonitor.SERVICE_NAME, o1)

        # o2 and b1 have never had their Monitor run, but o2 has had some other Monitor run.
        Timestamp.stamp(self._db, "A Different Service", o2)

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

        class Mock(SubjectSweepMonitor):
            SERVICE_NAME = "Mock"

        s1, ignore = Subject.lookup(self._db, Subject.DDC, "100", None)
        s2, ignore = Subject.lookup(
            self._db, Subject.TAG, None, "100 Years of Solitude"
        )

        # By default, SubjectSweepMonitor handles every Subject
        # in the database.
        everything = Mock(self._db)
        eq_([s1, s2], everything.item_query().all())

        # But you can tell SubjectSweepMonitor to handle only Subjects
        # of a certain type.
        dewey_monitor = Mock(self._db, subject_type=Subject.DDC)
        eq_([s1], dewey_monitor.item_query().all())

        # You can also SubjectSweepMonitor to handle only Subjects
        # whose names or identifiers match a certain string.
        one_hundred_monitor = Mock(self._db, filter_string="100")
        eq_([s1, s2], one_hundred_monitor.item_query().all())

        specific_tag_monitor = Mock(
            self._db, subject_type=Subject.TAG, filter_string="Years"
        )
        eq_([s2], specific_tag_monitor.item_query().all())


class TestCustomListEntrySweepMonitor(DatabaseTest):

    def test_item_query(self):
        class Mock(CustomListEntrySweepMonitor):
            SERVICE_NAME = "Mock"

        # Three CustomLists, each containing one book.
        list1, [edition1] = self._customlist(num_entries=1)
        list2, [edition2] = self._customlist(num_entries=1)
        list3, [edition3] = self._customlist(num_entries=1)

        [entry1] = list1.entries
        [entry2] = list2.entries
        [entry3] = list3.entries

        # Two Collections, each with one book from one of the lists.
        c1 = self._collection()
        c1.licensepools.extend(edition1.license_pools)

        c2 = self._collection()
        c2.licensepools.extend(edition2.license_pools)

        # If we don't pass in a Collection to
        # CustomListEntrySweepMonitor, we get all three
        # CustomListEntries, in their order of creation.
        monitor = Mock(self._db)
        eq_([entry1, entry2, entry3], monitor.item_query().all())

        # If we pass in a Collection to CustomListEntrySweepMonitor,
        # we get only the CustomListEntry whose work is licensed
        # to that collection.
        monitor = Mock(self._db, collection=c2)
        eq_([entry2], monitor.item_query().all())


class TestEditionSweepMonitor(DatabaseTest):

    def test_item_query(self):
        class Mock(EditionSweepMonitor):
            SERVICE_NAME = "Mock"

        # Three Editions, two of which have LicensePools.
        e1, p1 = self._edition(with_license_pool=True)
        e2, p2 = self._edition(with_license_pool=True)
        e3 = self._edition(with_license_pool=False)

        # Two Collections, each with one book.
        c1 = self._collection()
        c1.licensepools.extend(e1.license_pools)

        c2 = self._collection()
        c2.licensepools.extend(e2.license_pools)

        # If we don't pass in a Collection to EditionSweepMonitor, we
        # get all three Editions, in their order of creation.
        monitor = Mock(self._db)
        eq_([e1, e2, e3], monitor.item_query().all())

        # If we pass in a Collection to EditionSweepMonitor, we get
        # only the Edition whose work is licensed to that collection.
        monitor = Mock(self._db, collection=c2)
        eq_([e2], monitor.item_query().all())


class TestWorkSweepMonitors(DatabaseTest):
    """To reduce setup costs, this class tests WorkSweepMonitor,
    PresentationReadyWorkSweepMonitor, and
    NotPresentationReadyWorkSweepMonitor at once.
    """

    def test_item_query(self):
        class Mock(WorkSweepMonitor):
            SERVICE_NAME = "Mock"

        # Three Works with LicensePools. Only one is presentation
        # ready.
        w1, w2, w3 = [self._work(with_license_pool=True) for i in range(3)]

        # Another Work that's presentation ready but has no
        # LicensePool.
        w4 = self._work()
        w4.presentation_ready = True

        w2.presentation_ready = False
        w3.presentation_ready = None

        # Two Collections, each with one book.
        c1 = self._collection()
        c1.licensepools.append(w1.license_pools[0])

        c2 = self._collection()
        c2.licensepools.append(w2.license_pools[0])

        # If we don't pass in a Collection to WorkSweepMonitor, we
        # get all four Works, in their order of creation.
        monitor = Mock(self._db)
        eq_([w1, w2, w3, w4], monitor.item_query().all())

        # If we pass in a Collection to EditionSweepMonitor, we get
        # only the Work licensed to that collection.
        monitor = Mock(self._db, collection=c2)
        eq_([w2], monitor.item_query().all())

        # PresentationReadyWorkSweepMonitor is the same, but it excludes
        # works that are not presentation ready.
        class Mock(PresentationReadyWorkSweepMonitor):
            SERVICE_NAME = "Mock"
        eq_([w1, w4], Mock(self._db).item_query().all())
        eq_([w1], Mock(self._db, collection=c1).item_query().all())
        eq_([], Mock(self._db, collection=c2).item_query().all())

        # NotPresentationReadyWorkSweepMonitor is the same, but it _only_
        # includes works that are not presentation ready.
        class Mock(NotPresentationReadyWorkSweepMonitor):
            SERVICE_NAME = "Mock"
        eq_([w2, w3], Mock(self._db).item_query().all())
        eq_([], Mock(self._db, collection=c1).item_query().all())
        eq_([w2], Mock(self._db, collection=c2).item_query().all())



class TestOPDSEntryCacheMonitor(DatabaseTest):

    def test_process_item(self):
        """This Monitor calculates OPDS entries for works."""
        class Mock(OPDSEntryCacheMonitor):
            SERVICE_NAME = "Mock"
        monitor = Mock(self._db)
        work = self._work()
        eq_(None, work.simple_opds_entry)
        eq_(None, work.verbose_opds_entry)

        monitor.process_item(work)
        assert work.simple_opds_entry != None
        assert work.verbose_opds_entry != None


class TestPermanentWorkIDRefresh(DatabaseTest):

    def test_process_item(self):
        """This Monitor calculates an Editions' permanent work ID."""
        class Mock(PermanentWorkIDRefreshMonitor):
            SERVICE_NAME = "Mock"
        edition = self._edition()
        eq_(None, edition.permanent_work_id)
        Mock(self._db).process_item(edition)
        assert edition.permanent_work_id != None


class TestMakePresentationReadyMonitor(DatabaseTest):

    def setup(self):
        super(TestMakePresentationReadyMonitor, self).setup()

        # This CoverageProvider will always succeed.
        class MockProvider1(AlwaysSuccessfulCoverageProvider):
            SERVICE_NAME = "Provider 1"
            INPUT_IDENTIFIER_TYPES = Identifier.GUTENBERG_ID
            DATA_SOURCE_NAME = DataSource.OCLC

        # This CoverageProvider will always fail.
        class MockProvider2(NeverSuccessfulCoverageProvider):
            SERVICE_NAME = "Provider 2"
            INPUT_IDENTIFIER_TYPES = Identifier.GUTENBERG_ID
            DATA_SOURCE_NAME = DataSource.OVERDRIVE

        self.success = MockProvider1(self._db)
        self.failure = MockProvider2(self._db)

        self.work = self._work(
            DataSource.GUTENBERG, with_license_pool=True)
        # Don't fake that the work is presentation ready, as we usually do,
        # because presentation readiness is what we're trying to test.
        self.work.presentation_ready = False

    def test_process_item_sets_presentation_ready_on_success(self):
        # Create a monitor that doesn't need to do anything.
        monitor = MakePresentationReadyMonitor(self._db, [])
        monitor.process_item(self.work)

        # When it's done doing nothing, it sets the work as
        # presentation-ready.
        eq_(None, self.work.presentation_ready_exception)
        eq_(True, self.work.presentation_ready)

    def test_process_item_sets_exception_on_failure(self):
        monitor = MakePresentationReadyMonitor(
            self._db, [self.success, self.failure]
        )
        monitor.process_item(self.work)
        eq_(
            "Provider(s) failed: %s" % self.failure.SERVICE_NAME,
            self.work.presentation_ready_exception
        )
        eq_(False, self.work.presentation_ready)

    def test_prepare_raises_exception_with_failing_providers(self):
        monitor = MakePresentationReadyMonitor(
            self._db, [self.success, self.failure]
        )
        assert_raises_regexp(
            CoverageProvidersFailed,
            self.failure.service_name,
            monitor.prepare, self.work
        )

    def test_prepare_does_not_call_irrelevant_provider(self):

        monitor = MakePresentationReadyMonitor(self._db, [self.success])
        result = monitor.prepare(self.work)

        # There were no failures.
        eq_([], result)

        # The 'success' monitor ran.
        eq_([self.work.presentation_edition.primary_identifier],
            self.success.attempts)

        # The 'failure' monitor did not. (If it had, it would have
        # failed.)
        eq_([], self.failure.attempts)

        # The work has not been set to presentation ready--that's
        # handled in process_item().
        eq_(False, self.work.presentation_ready)


class TestWorkRandomnessUpdateMonitor(DatabaseTest):

    def test_process_batch(self):
        """This Monitor sets Work.random to a random value.
        """
        work = self._work()
        old_random = work.random
        monitor = WorkRandomnessUpdateMonitor(self._db)
        value = monitor.process_batch(work.id)
        # Since there's only one work, a single batch finishes the job.
        eq_(0, value)

        # This is normally called by run().
        self._db.commit()

        # This could fail once, spuriously but the odds are much
        # higher that the code has broken and it's failing reliably.
        assert work.random != old_random


class TestCustomListEntryWorkUpdateMonitor(DatabaseTest):

    def test_set_item(self):

        # Create a CustomListEntry.
        list1, [edition1] = self._customlist(num_entries=1)
        [entry] = list1.entries

        # Pretend that its CustomListEntry's work was never set.
        old_work = entry.work
        entry.work = None

        # Running process_item resets it to the same value.
        monitor = CustomListEntryWorkUpdateMonitor(self._db)
        monitor.process_item(entry)
        eq_(old_work, entry.work)


class MockReaperMonitor(ReaperMonitor):
    MODEL_CLASS = Timestamp
    TIMESTAMP_FIELD = 'timestamp'


class TestReaperMonitor(DatabaseTest):

    def test_cutoff(self):
        """Test that cutoff behaves correctly when given different values for
        ReaperMonitor.MAX_AGE.
        """
        m = MockReaperMonitor(self._db)

        # A number here means a number of days.
        for value in [1, 1.5, -1]:
            m.MAX_AGE = value
            expect = datetime.datetime.utcnow() - datetime.timedelta(
                days=value
            )
            assert (m.cutoff - expect).total_seconds() < 2

        # But you can pass in a timedelta instead.
        m.MAX_AGE = datetime.timedelta(seconds=99)
        expect = datetime.datetime.utcnow() - m.MAX_AGE
        assert (m.cutoff - expect).total_seconds() < 2

    def test_specific_reapers(self):
        eq_(CachedFeed.timestamp, CachedFeedReaper(self._db).timestamp_field)
        eq_(30, CachedFeedReaper.MAX_AGE)
        eq_(Credential.expires, CredentialReaper(self._db).timestamp_field)
        eq_(1, CredentialReaper.MAX_AGE)

    def test_where_clause(self):
        m = CachedFeedReaper(self._db)
        eq_("cachedfeeds.timestamp < :timestamp_1", str(m.where_clause))

    def test_run_once(self):
        # Create three Credentials.
        expired = self._credential()
        now = datetime.datetime.utcnow()
        expired.expires = now - datetime.timedelta(
            days=CredentialReaper.MAX_AGE + 1
        )

        active = self._credential()
        active.expires = now - datetime.timedelta(
            days=CredentialReaper.MAX_AGE - 1
        )

        eternal = self._credential()

        m = CredentialReaper(self._db)
        m.run_once()

        # The expired credential has been reaped; the others
        # are still in the database.
        remaining = set(self._db.query(Credential).all())
        eq_(set([active, eternal]), remaining)
