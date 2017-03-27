from nose.tools import (
    eq_, 
    set_trace,
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
    DataSource,
    Identifier,
    Subject,
    Timestamp,
)

from monitor import (
    Monitor,
    PresentationReadyMonitor,
    SubjectSweepMonitor,
)

class DummyMonitor(Monitor):

    def __init__(self, _db, collection=None):
        super(DummyMonitor, self).__init__(
            _db, "Dummy monitor for test", collection, 0.1
        )
        self.run_records = []
        self.cleanup_records = []

    def run_once(self, start, cutoff):
        self.original_timestamp = start
        self.run_records.append(True)
        self.stop_running = True

    def cleanup(self):
        self.cleanup_records.append(True)

class TestMonitor(DatabaseTest):

    def test_monitor_lifecycle(self):
        monitor = DummyMonitor(self._db)

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
        m1 = DummyMonitor(self._db, c1)
        m2 = DummyMonitor(self._db, c2)

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
        success = AlwaysSuccessfulCoverageProvider(
            service_name="Provider 1",
            input_identifier_types=self.gutenberg_id,
            output_source=self.oclc
        )
        monitor = PresentationReadyMonitor(self._db, [success])
        monitor.process_batch([self.work])
        eq_(None, self.work.presentation_ready_exception)
        eq_(True, self.work.presentation_ready)

    def test_make_batch_presentation_ready_sets_exception_on_failure(self):
        success = AlwaysSuccessfulCoverageProvider(
            service_name="Provider 1",
            input_identifier_types=self.gutenberg_id,
            output_source=self.oclc
        )
        failure = NeverSuccessfulCoverageProvider(
            service_name="Provider 2",
            input_identifier_types=self.gutenberg_id,
            output_source=self.overdrive
        )
        monitor = PresentationReadyMonitor(self._db, [success, failure])
        monitor.process_batch([self.work])
        eq_(False, self.work.presentation_ready)
        eq_(
            "Provider(s) failed: Provider 2",
            self.work.presentation_ready_exception)

    def test_prepare_returns_failing_providers(self):

        success = AlwaysSuccessfulCoverageProvider(
            service_name="Monitor 1",
            output_source=self.oclc,
            input_identifier_types=self.gutenberg_id,
        )
        failure = NeverSuccessfulCoverageProvider(
            service_name="Monitor 2",
            output_source=self.overdrive,
            input_identifier_types=self.gutenberg_id,
        )
        monitor = PresentationReadyMonitor(self._db, [success, failure])
        result = monitor.prepare(self.work)
        eq_([failure], result)

    def test_irrelevant_provider_is_not_called(self):

        gutenberg_monitor = AlwaysSuccessfulCoverageProvider(
            service_name="Gutenberg monitor",
            output_source=self.oclc,
            input_identifier_types=self.gutenberg_id
        )
        oclc_monitor = NeverSuccessfulCoverageProvider(
            service_name="OCLC monitor",
            output_source=self.oclc,
            input_identifier_types=Identifier.OCLC_NUMBER
        )
        monitor = PresentationReadyMonitor(
            self._db, [gutenberg_monitor, oclc_monitor])
        result = monitor.prepare(self.work)

        # There were no failures.
        eq_([], result)

        # The monitor that takes Gutenberg identifiers as input ran.
        eq_([self.work.presentation_edition.primary_identifier], gutenberg_monitor.attempts)

        # The monitor that takes OCLC editions as input did not.
        # (If it had, it would have failed.)
        eq_([], oclc_monitor.attempts)

        # The work has not been set to presentation ready--that's
        # handled elsewhere.
        eq_(False, self.work.presentation_ready)



class TestSubjectSweepMonitor(DatabaseTest):

    def test_subject_query(self):
        s1, ignore = Subject.lookup(self._db, Subject.DDC, "100", None)
        s2, ignore = Subject.lookup(
            self._db, Subject.TAG, None, "100 Years of Solitude"
        )

        dewey_monitor = SubjectSweepMonitor(
            self._db, "Test Monitor", Subject.DDC
        )
        eq_([s1], dewey_monitor.subject_query().all())

        one_hundred_monitor = SubjectSweepMonitor(
            self._db, "Test Monitor", None, "100"
        )
        eq_([s1, s2], one_hundred_monitor.subject_query().all())

        specific_tag_monitor = SubjectSweepMonitor(
            self._db, "Test Monitor", Subject.TAG, "Years"
        )
        eq_([s2], specific_tag_monitor.subject_query().all())
        
