from nose.tools import eq_, set_trace
import datetime

from . import DatabaseTest

from testing import (
    AlwaysSuccessfulCoverageProvider,
    NeverSuccessfulCoverageProvider,
)

from model import (
    DataSource,
    Timestamp,
)

from monitor import (
    Monitor,
    PresentationReadyMonitor,
)

class DummyMonitor(Monitor):

    def __init__(self, _db):
        super(DummyMonitor, self).__init__(_db, "Dummy monitor for test", 0.1)
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

class TestPresentationReadyMonitor(DatabaseTest):

    def setup(self):
        super(TestPresentationReadyMonitor, self).setup()
        self.gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        self.oclc = DataSource.lookup(self._db, DataSource.OCLC)
        self.overdrive = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        self.edition, self.edition_license_pool = self._edition(self.gutenberg.name, with_license_pool=True)
        self.work = self._work(
            self.gutenberg.name, with_license_pool=True)
        # Don't fake that the work is presentation ready, as we usually do,
        # because presentation readiness is what we're trying to test.
        self.work.presentation_ready = False

    def test_make_batch_presentation_ready_sets_presentation_ready_on_success(self):
        success = AlwaysSuccessfulCoverageProvider(
            "Provider 1", self.gutenberg, self.oclc)
        monitor = PresentationReadyMonitor(self._db, [success])
        monitor.process_batch([self.work])
        eq_(None, self.work.presentation_ready_exception)
        eq_(True, self.work.presentation_ready)

    def test_make_batch_presentation_ready_sets_exception_on_failure(self):
        success = AlwaysSuccessfulCoverageProvider(
            "Provider 1", self.gutenberg, self.oclc)
        failure = NeverSuccessfulCoverageProvider(
            "Provider 2", self.gutenberg, self.overdrive)
        monitor = PresentationReadyMonitor(self._db, [success, failure])
        monitor.process_batch([self.work])
        eq_(False, self.work.presentation_ready)
        eq_(
            "Provider(s) failed: Provider 2",
            self.work.presentation_ready_exception)

    def test_prepare_returns_failing_providers(self):

        success = AlwaysSuccessfulCoverageProvider(
            "Monitor 1", self.gutenberg, self.oclc)
        failure = NeverSuccessfulCoverageProvider(
            "Monitor 2", self.gutenberg, self.overdrive)
        monitor = PresentationReadyMonitor(self._db, [success, failure])
        result = monitor.prepare(self.work)
        eq_([failure], result)

    def test_irrelevant_provider_is_not_called(self):

        gutenberg_monitor = AlwaysSuccessfulCoverageProvider(
            "Gutenberg monitor", self.gutenberg, self.oclc)
        oclc_monitor = NeverSuccessfulCoverageProvider(
            "OCLC monitor", self.oclc, self.overdrive)
        monitor = PresentationReadyMonitor(
            self._db, [gutenberg_monitor, oclc_monitor])
        result = monitor.prepare(self.work)

        # There were no failures.
        eq_([], result)

        # The monitor that takes Gutenberg editions as input ran.
        eq_([self.work.primary_edition], gutenberg_monitor.attempts)

        # The monitor that takes OCLC editions as input did not.
        # (If it had, it would have failed.)
        eq_([], oclc_monitor.attempts)

        # The work has not been set to presentation ready--that's
        # handled elsewhere.
        eq_(False, self.work.presentation_ready)
