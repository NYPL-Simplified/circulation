import datetime

from nose.tools import (
    assert_raises,
    assert_raises_regexp,
    eq_,
    ok_,
)

from . import DatabaseTest
from ..config import Configuration
from ..metadata_layer import TimestampData
from ..model import (
    CachedFeed,
    CirculationEvent,
    Collection,
    CollectionMissing,
    ConfigurationSetting,
    Credential,
    DataSource,
    Edition,
    ExternalIntegration,
    Genre,
    Identifier,
    Measurement,
    Patron,
    Subject,
    Timestamp,
    Work,
    WorkCoverageRecord,
    create,
    get_one,
    get_one_or_create,
)
from ..monitor import (
    CachedFeedReaper,
    CirculationEventLocationScrubber,
    CollectionMonitor,
    CollectionReaper,
    CoverageProvidersFailed,
    CredentialReaper,
    CustomListEntrySweepMonitor,
    CustomListEntryWorkUpdateMonitor,
    EditionSweepMonitor,
    IdentifierSweepMonitor,
    MakePresentationReadyMonitor,
    MeasurementReaper,
    Monitor,
    NotPresentationReadyWorkSweepMonitor,
    OPDSEntryCacheMonitor,
    PatronNeighborhoodScrubber,
    PatronRecordReaper,
    PermanentWorkIDRefreshMonitor,
    PresentationReadyWorkSweepMonitor,
    ReaperMonitor,
    SubjectSweepMonitor,
    SweepMonitor,
    TimelineMonitor,
    WorkReaper,
    WorkSweepMonitor,
)
from ..testing import (
    AlwaysSuccessfulCoverageProvider,
    NeverSuccessfulCoverageProvider,
)


class MockMonitor(Monitor):

    SERVICE_NAME = "Dummy monitor for test"

    def __init__(self, _db, collection=None):
        super(MockMonitor, self).__init__(_db, collection)
        self.run_records = []
        self.cleanup_records = []

    def run_once(self, progress):
        # Record the TimestampData object passed in.
        self.run_records.append(progress)

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

    def test_initial_start_time(self):
        monitor = MockMonitor(self._db, self._default_collection)

        # Setting the default start time to NEVER explicitly says to use
        # None as the initial time.
        monitor.default_start_time = monitor.NEVER
        eq_(None, monitor.initial_start_time)

        # Setting the value to None means "use the current time".
        monitor.default_start_time = None
        self.time_eq(datetime.datetime.utcnow(), monitor.initial_start_time)

        # Any other value is returned as-is.
        default = object()
        monitor.default_start_time = default
        eq_(default, monitor.initial_start_time)

    def test_monitor_lifecycle(self):
        monitor = MockMonitor(self._db, self._default_collection)
        monitor.default_start_time = datetime.datetime(2010, 1, 1)

        # There is no timestamp for this monitor.
        def get_timestamp():
            return get_one(self._db, Timestamp, service=monitor.service_name)
        eq_(None, get_timestamp())

        # Run the monitor.
        monitor.run()

        # The monitor ran once and then stopped.
        [progress] = monitor.run_records

        # The TimestampData passed in to run_once() had the
        # Monitor's default start time as its .start, and an empty
        # time for .finish.
        eq_(monitor.default_start_time, progress.start)
        eq_(None, progress.finish)

        # But the Monitor's underlying timestamp has been updated with
        # the time that the monitor actually took to run.
        timestamp = get_timestamp()
        assert timestamp.start > monitor.default_start_time
        assert timestamp.finish > timestamp.start
        self.time_eq(datetime.datetime.utcnow(), timestamp.start)

        # cleanup() was called once.
        eq_([True], monitor.cleanup_records)

    def test_initial_timestamp(self):
        class NeverRunMonitor(MockMonitor):
            SERVICE_NAME = "Never run"
            DEFAULT_START_TIME = MockMonitor.NEVER

        # The Timestamp object is created, but its .start is None,
        # indicating that it has never run to completion.
        m = NeverRunMonitor(self._db, self._default_collection)
        eq_(None, m.timestamp().start)

        class RunLongAgoMonitor(MockMonitor):
            SERVICE_NAME = "Run long ago"
            DEFAULT_START_TIME = MockMonitor.ONE_YEAR_AGO
        # The Timestamp object is created, and its .timestamp is long ago.
        m = RunLongAgoMonitor(self._db, self._default_collection)
        timestamp = m.timestamp()
        now = datetime.datetime.utcnow()
        assert timestamp.start < now

        # Timestamp.finish is set to None, on the assumption that the
        # first run is still in progress.
        eq_(timestamp.finish, None)

    def test_run_once_returning_timestampdata(self):
        # If a Monitor's run_once implementation returns a TimestampData,
        # that's the data used to set the Monitor's Timestamp, even if
        # the data doesn't make sense by the standards used by the main
        # Monitor class.
        start = datetime.datetime(2011, 1, 1)
        finish = datetime.datetime(2012, 1, 1)

        class Mock(MockMonitor):
            def run_once(self, progress):
                return TimestampData(start=start, finish=finish, counter=-100)
        monitor = Mock(self._db, self._default_collection)
        monitor.run()

        timestamp = monitor.timestamp()
        eq_(start, timestamp.start)
        eq_(finish, timestamp.finish)
        eq_(-100, timestamp.counter)

    def test_run_once_with_exception(self):
        # If an exception happens during a Monitor's run_once
        # implementation, a traceback for that exception is recorded
        # in the appropriate Timestamp, but the timestamp itself is
        # not updated.

        # This test function shows the behavior we expect from a
        # Monitor.
        def assert_run_sets_exception(monitor, check_for):
            timestamp = monitor.timestamp()
            old_start = timestamp.start
            old_finish = timestamp.finish
            eq_(None, timestamp.exception)

            monitor.run()

            # The timestamp has been updated, but the times have not.
            assert check_for in timestamp.exception
            eq_(old_start, timestamp.start)
            eq_(old_finish, timestamp.finish)

        # Try a monitor that raises an unhandled exception.
        class DoomedMonitor(MockMonitor):
            SERVICE_NAME = "Doomed"
            def run_once(self, *args, **kwargs):
                raise Exception("I'm doomed")
        m = DoomedMonitor(self._db, self._default_collection)
        assert_run_sets_exception(m, "Exception: I'm doomed")

        # Try a monitor that sets .exception on the TimestampData it
        # returns.
        class AlsoDoomed(MockMonitor):
            SERVICE_NAME = "Doomed, but in a different way."
            def run_once(self, progress):
                return TimestampData(exception="I'm also doomed")
        m = AlsoDoomed(self._db, self._default_collection)
        assert_run_sets_exception(m, "I'm also doomed")

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
        assert t2.start > t1.start


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
        o1 = self._collection("o1")
        o2 = self._collection("o2")
        o3 = self._collection("o3")

        # ...and a Bibliotheca collection.
        b1 = self._collection(protocol=ExternalIntegration.BIBLIOTHECA)

        # o1 just had its Monitor run.
        Timestamp.stamp(
            self._db, OPDSCollectionMonitor.SERVICE_NAME,
            Timestamp.MONITOR_TYPE, o1
        )

        # o2 and b1 have never had their Monitor run, but o2 has had some other Monitor run.
        Timestamp.stamp(
            self._db, "A Different Service", Timestamp.MONITOR_TYPE,
            o2
        )

        # o3 had its Monitor run an hour ago.
        now = datetime.datetime.utcnow()
        an_hour_ago = now - datetime.timedelta(seconds=3600)
        Timestamp.stamp(
            self._db, OPDSCollectionMonitor.SERVICE_NAME,
            Timestamp.MONITOR_TYPE, o3, start=an_hour_ago,
            finish=an_hour_ago
        )

        monitors = list(OPDSCollectionMonitor.all(self._db))

        # Three OPDSCollectionMonitors were returned, one for each
        # appropriate collection. The monitor that needs to be run the
        # worst was returned first in the list. The monitor that was
        # run most recently is returned last. There is no
        # OPDSCollectionMonitor for the Bibliotheca collection.
        eq_([o2, o3, o1], [x.collection for x in monitors])


class TestTimelineMonitor(DatabaseTest):

    def test_run_once(self):
        class Mock(TimelineMonitor):
            SERVICE_NAME = "Just a timeline"
            catchups = []
            def catch_up_from(self, start, cutoff, progress):
                self.catchups.append((start, cutoff, progress))

        m = Mock(self._db)
        progress = m.timestamp().to_data()
        m.run_once(progress)
        now = datetime.datetime.utcnow()

        # catch_up_from() was called once.
        (start, cutoff, progress) = m.catchups.pop()
        eq_(m.initial_start_time, start)
        self.time_eq(cutoff, now)

        # progress contains a record of the timespan now covered
        # by this Monitor.
        eq_(start, progress.start)
        eq_(cutoff, progress.finish)

    def test_subclass_cannot_modify_dates(self):
        """The subclass can modify some fields of the TimestampData
        passed in to it, but it can't modify the start or end dates.

        If you want that, you shouldn't subclass TimelineMonitor.
        """
        class Mock(TimelineMonitor):
            DEFAULT_START_TIME = Monitor.NEVER
            SERVICE_NAME = "I aim to misbehave"
            def catch_up_from(self, start, cutoff, progress):
                progress.start = 1
                progress.finish = 2
                progress.counter = 3
                progress.achievements = 4

        m = Mock(self._db)
        progress = m.timestamp().to_data()
        m.run_once(progress)
        now = datetime.datetime.utcnow()

        # The timestamp values have been set to appropriate values for
        # the portion of the timeline covered, overriding our values.
        eq_(None, progress.start)
        self.time_eq(now, progress.finish)

        # The non-timestamp values have been left alone.
        eq_(3, progress.counter)
        eq_(4, progress.achievements)

    def test_timestamp_not_updated_on_exception(self):
        """If the subclass sets .exception on the TimestampData
        passed into it, the dates aren't modified.
        """
        class Mock(TimelineMonitor):
            DEFAULT_START_TIME = datetime.datetime(2011, 1, 1)
            SERVICE_NAME = "doomed"
            def catch_up_from(self, start, cutoff, progress):
                self.started_at = start
                progress.exception = "oops"

        m = Mock(self._db)
        progress = m.timestamp().to_data()
        m.run_once(progress)

        # The timestamp value is set to a value indicating that the
        # initial run never completed.
        eq_(m.DEFAULT_START_TIME, progress.start)
        eq_(None, progress.finish)

    def test_slice_timespan(self):
        # Test the slice_timespan utility method.

        # Slicing up the time between 121 minutes ago and now in increments
        # of one hour will yield three slices:
        #
        # 121 minutes ago -> 61 minutes ago
        # 61 minutes ago -> 1 minute ago
        # 1 minute ago -> now
        now = datetime.datetime.utcnow()
        one_hour = datetime.timedelta(minutes=60)
        ago_1 = now - datetime.timedelta(minutes=1)
        ago_61 = ago_1 - one_hour
        ago_121 = ago_61 - one_hour

        slice1, slice2, slice3 = list(
            TimelineMonitor.slice_timespan(ago_121, now, one_hour)
        )
        eq_(slice1, (ago_121, ago_61, True))
        eq_(slice2, (ago_61, ago_1, True))
        eq_(slice3, (ago_1, now, False))

        # The True/True/False indicates that the first two slices are
        # complete -- they cover a span of an entire hour. The final
        # slice is incomplete -- it covers only one minute.


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

    def test_run_against_empty_table(self):
        # If there's nothing in the table to be swept, a SweepMonitor runs
        # to completion and accomplishes nothing.
        self.monitor.run()
        timestamp = self.monitor.timestamp()
        eq_("Records processed: 0.", timestamp.achievements)
        eq_(None, timestamp.exception)

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

        # The number of records processed reflects what happened over
        # the entire run, not just the final batch.
        eq_("Records processed: 3.", self.monitor.timestamp().achievements)

    def test_run_starts_at_previous_counter(self):
        # Two Identifiers.
        i1, i2 = [self._identifier() for i in range(2)]

        # The monitor was just run, but it was not able to proceed past
        # i1.
        timestamp = Timestamp.stamp(
            self._db, self.monitor.service_name,
            Timestamp.MONITOR_TYPE,
            self.monitor.collection
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

        timestamp = monitor.timestamp()
        original_start = timestamp.start
        monitor.run()

        # The monitor's counter was updated to the ID of the final
        # item in the last batch it was able to process. In this case,
        # this is I2.
        eq_(i2.id, timestamp.counter)

        # The exception that stopped the run was recorded.
        assert "Exception: HOW DARE YOU" in timestamp.exception

        # Even though the run didn't complete, the dates and
        # achievements of the timestamp were updated to reflect the
        # work that _was_ done.
        now = datetime.datetime.utcnow()
        assert timestamp.start > original_start
        self.time_eq(now, timestamp.start)
        self.time_eq(now, timestamp.finish)
        assert timestamp.start < timestamp.finish

        eq_("Records processed: 2.", timestamp.achievements)

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
            self.time_eq(m.cutoff, expect)

        # But you can pass in a timedelta instead.
        m.MAX_AGE = datetime.timedelta(seconds=99)
        self.time_eq(m.cutoff, datetime.datetime.utcnow() - m.MAX_AGE)

    def test_specific_reapers(self):
        eq_(CachedFeed.timestamp, CachedFeedReaper(self._db).timestamp_field)
        eq_(30, CachedFeedReaper.MAX_AGE)
        eq_(Credential.expires, CredentialReaper(self._db).timestamp_field)
        eq_(1, CredentialReaper.MAX_AGE)
        eq_(Patron.authorization_expires, PatronRecordReaper(self._db).timestamp_field)
        eq_(60, PatronRecordReaper.MAX_AGE)

    def test_where_clause(self):
        m = CachedFeedReaper(self._db)
        eq_("cachedfeeds.timestamp < :timestamp_1", str(m.where_clause))

    def test_run_once(self):
        # Create four Credentials: two expired, two valid.
        expired1 = self._credential()
        expired2 = self._credential()
        now = datetime.datetime.utcnow()
        expiration_date = now - datetime.timedelta(
            days=CredentialReaper.MAX_AGE + 1
        )
        for e in [expired1, expired2]:
            e.expires = expiration_date

        active = self._credential()
        active.expires = now - datetime.timedelta(
            days=CredentialReaper.MAX_AGE - 1
        )

        eternal = self._credential()

        m = CredentialReaper(self._db)

        # Set the batch size to 1 to make sure this works even
        # when there are multiple batches.
        m.BATCH_SIZE = 1

        eq_("Reaper for Credential.expires", m.SERVICE_NAME)
        result = m.run_once()
        eq_("Items deleted: 2", result.achievements)

        # The expired credentials have been reaped; the others
        # are still in the database.
        remaining = set(self._db.query(Credential).all())
        eq_(set([active, eternal]), remaining)

    def test_reap_patrons(self):
        m = PatronRecordReaper(self._db)
        expired = self._patron()
        credential = self._credential(patron=expired)
        now = datetime.datetime.utcnow()
        expired.authorization_expires = now - datetime.timedelta(
            days=PatronRecordReaper.MAX_AGE + 1
        )
        active = self._patron()
        active.expires = now - datetime.timedelta(
            days=PatronRecordReaper.MAX_AGE - 1
        )
        result = m.run_once()
        eq_("Items deleted: 1", result.achievements)
        remaining = self._db.query(Patron).all()
        eq_([active], remaining)

        eq_([], self._db.query(Credential).all())


class TestWorkReaper(DatabaseTest):

    def test_end_to_end(self):
        # Search mock
        class MockSearchIndex():
            removed = []

            def remove_work(self, work):
                self.removed.append(work)

        # First, create three works.

        # This work has a license pool.
        has_license_pool = self._work(with_license_pool=True)

        # This work had a license pool and then lost it.
        had_license_pool = self._work(with_license_pool=True)
        self._db.delete(had_license_pool.license_pools[0])

        # This work never had a license pool.
        never_had_license_pool = self._work(with_license_pool=False)

        # Each work has a presentation edition -- keep track of these
        # for later.
        works = self._db.query(Work)
        presentation_editions = [x.presentation_edition for x in works]

        # If and when Work gets database-level cascading deletes, this
        # is where they will all be triggered, with no chance that an
        # ORM-level delete is doing the work. So let's verify that all
        # of the cascades work.

        # First, set up some related items for each Work.

        # Each work is assigned to a genre.
        genre, ignore = Genre.lookup(self._db, "Science Fiction")
        for work in works:
            work.genres = [genre]

        # Each work is on the same CustomList.
        l, ignore = self._customlist("a list", num_entries=0)
        for work in works:
            l.add_entry(work)

        # Each work has a WorkCoverageRecord.
        for work in works:
            WorkCoverageRecord.add_for(work, operation="some operation")

        # Each work has a CachedFeed.
        for work in works:
            feed = CachedFeed(
                work=work, type='page', content="content",
                pagination="", facets=""
            )
            self._db.add(feed)

        # Also create a CachedFeed that has no associated Work.
        workless_feed = CachedFeed(
            work=None, type='page', content="content",
            pagination="", facets=""
        )
        self._db.add(workless_feed)

        self._db.commit()

        # Run the reaper.
        s = MockSearchIndex()
        m = WorkReaper(self._db, search_index_client=s)
        print(m.search_index_client)
        m.run_once()

        # Search index was updated
        eq_(2, len(s.removed))
        ok_(has_license_pool not in s.removed)
        ok_(had_license_pool in s.removed)
        ok_(never_had_license_pool in s.removed)

        # Only the work with a license pool remains.
        eq_([has_license_pool], [x for x in works])

        # The presentation editions are still around, since they might
        # theoretically be used by other parts of the system.
        all_editions = self._db.query(Edition).all()
        for e in presentation_editions:
            assert e in all_editions

        # The surviving work is still assigned to the Genre, and still
        # has WorkCoverageRecords.
        eq_([has_license_pool], genre.works)
        surviving_records = self._db.query(WorkCoverageRecord)
        assert surviving_records.count() > 0
        assert all(x.work==has_license_pool for x in surviving_records)

        # The CustomListEntries still exist, but two of them have lost
        # their work.
        eq_(2, len([x for x in l.entries if not x.work]))
        eq_([has_license_pool], [x.work for x in l.entries if x.work])

        # The CachedFeeds associated with the reaped Works have been
        # deleted. The surviving Work still has one, and the
        # CachedFeed that didn't have a work in the first place is
        # unaffected.
        feeds = self._db.query(CachedFeed).all()
        eq_([workless_feed], [x for x in feeds if not x.work])
        eq_([has_license_pool], [x.work for x in feeds if x.work])


class TestCollectionReaper(DatabaseTest):

    def test_query(self):
        # This reaper is looking for collections that are marked for
        # deletion.
        collection = self._default_collection
        reaper = CollectionReaper(self._db)
        eq_([], reaper.query().all())

        collection.marked_for_deletion = True
        eq_([collection], reaper.query().all())

    def test_reaper_delete_calls_collection_delete(self):
        # Unlike most ReaperMonitors, CollectionReaper.delete()
        # is overridden to call delete() on the object it was passed,
        # rather than just doing a database delete.
        class MockCollection(object):
            def delete(self):
                self.was_called = True
        collection = MockCollection()
        reaper = CollectionReaper(self._db)
        reaper.delete(collection)
        eq_(True, collection.was_called)

    def test_run_once(self):
        # End-to-end test
        c1 = self._default_collection
        c2 = self._collection()
        c2.marked_for_deletion = True
        reaper = CollectionReaper(self._db)
        result = reaper.run_once()

        # The Collection marked for deletion has been deleted; the other
        # one is unaffected.
        eq_([c1], self._db.query(Collection).all())
        eq_("Items deleted: 1", result.achievements)


class TestMeasurementReaper(DatabaseTest):

    def test_query(self):
        # This reaper is looking for measurements that are not current.
        measurement, created = get_one_or_create(
            self._db, Measurement,
            is_most_recent=True)
        reaper = MeasurementReaper(self._db)
        eq_([], reaper.query().all())
        measurement.is_most_recent = False
        eq_([measurement], reaper.query().all())

    def test_run_once(self):
        # End-to-end test
        measurement1, created = get_one_or_create(
            self._db, Measurement,
            quantity_measured="answer",
            value=12,
            is_most_recent=True)
        measurement2, created = get_one_or_create(
            self._db, Measurement,
            quantity_measured="answer",
            value=42,
            is_most_recent=False)
        reaper = MeasurementReaper(self._db)
        result = reaper.run_once()
        eq_([measurement1], self._db.query(Measurement).all())
        eq_("Items deleted: 1", result.achievements)

    def test_disable(self):
        # This reaper can be disabled with a configuration setting
        enabled = ConfigurationSetting.sitewide(self._db, Configuration.MEASUREMENT_REAPER)
        enabled.value = False
        measurement1, created = get_one_or_create(
            self._db, Measurement,
            quantity_measured="answer",
            value=12,
            is_most_recent=True)
        measurement2, created = get_one_or_create(
            self._db, Measurement,
            quantity_measured="answer",
            value=42,
            is_most_recent=False)
        reaper = MeasurementReaper(self._db)
        reaper.run()
        eq_([measurement1, measurement2], self._db.query(Measurement).all())
        enabled.value = True
        reaper.run()
        eq_([measurement1], self._db.query(Measurement).all())


class TestScrubberMonitor(DatabaseTest):

    def test_run_once(self):
        # ScrubberMonitor is basically an abstract class, with
        # subclasses doing nothing but define missing constants. This
        # is an end-to-end test using a specific subclass,
        # CirculationEventLocationScrubber.

        m = CirculationEventLocationScrubber(self._db)
        eq_("Scrubber for CirculationEvent.location", m.SERVICE_NAME)

        # CirculationEvents are only scrubbed if they have a location
        # *and* are older than MAX_AGE.
        now = datetime.datetime.utcnow()
        not_long_ago = (
            m.cutoff + datetime.timedelta(days=1)
        )
        long_ago = (
            m.cutoff - datetime.timedelta(days=1)
        )

        new, ignore = create(
            self._db, CirculationEvent, start=now, location="loc"
        )
        recent, ignore = create(
            self._db, CirculationEvent, start=not_long_ago, location="loc"
        )
        old, ignore = create(
            self._db, CirculationEvent, start=long_ago, location="loc"
        )
        already_scrubbed, ignore = create(
            self._db, CirculationEvent, start=long_ago, location=None
        )

        # Only the old unscrubbed CirculationEvent is eligible
        # to be scrubbed.
        eq_([old], m.query().all())

        # Other reapers say items were 'deleted'; we say they were
        # 'scrubbed'.
        timestamp = m.run_once()
        eq_("Items scrubbed: 1", timestamp.achievements)

        # Only the old unscrubbed CirculationEvent has been scrubbed.
        eq_(None, old.location)
        for untouched in (new, recent):
            eq_("loc", untouched.location)

    def test_specific_scrubbers(self):
        # Check that all specific ScrubberMonitors are set up
        # correctly.
        circ = CirculationEventLocationScrubber(self._db)
        eq_(CirculationEvent.start, circ.timestamp_field)
        eq_(CirculationEvent.location, circ.scrub_field)
        eq_(365, circ.MAX_AGE)

        patron = PatronNeighborhoodScrubber(self._db)
        eq_(Patron.last_external_sync, patron.timestamp_field)
        eq_(Patron.cached_neighborhood, patron.scrub_field)
        eq_(Patron.MAX_SYNC_TIME, patron.MAX_AGE)
