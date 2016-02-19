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
    get_one,
    CoverageRecord,
    DataSource,
    Identifier,
    Timestamp,
    UnresolvedIdentifier,
)

from monitor import (
    Monitor,
    PresentationReadyMonitor,
    IdentifierResolutionMonitor,
    ResolutionFailed,
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
        self.gutenberg = Identifier.GUTENBERG_ID
        # DataSource.lookup(self._db, DataSource.GUTENBERG)
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

        # The monitor that takes Gutenberg identifiers as input ran.
        eq_([self.work.primary_edition.primary_identifier], gutenberg_monitor.attempts)

        # The monitor that takes OCLC editions as input did not.
        # (If it had, it would have failed.)
        eq_([], oclc_monitor.attempts)

        # The work has not been set to presentation ready--that's
        # handled elsewhere.
        eq_(False, self.work.presentation_ready)


class TestIdentifierResolutionMonitor(DatabaseTest):

    def setup(self):
        super(TestIdentifierResolutionMonitor, self).setup()
        self.ui, ignore = self._unresolved_identifier()
        self.identifier = self.ui.identifier
        idtype = self.identifier.type
        source = DataSource.lookup(self._db, DataSource.GUTENBERG)

        self.always_successful = AlwaysSuccessfulCoverageProvider(
            "Always", [idtype], source
        )
        self.never_successful = NeverSuccessfulCoverageProvider(
            "Never", [idtype], source
        )
        self.broken = BrokenCoverageProvider("Broken", [idtype], source)


    def test_run_once_resolves_and_deletes_unresolvedidentifiers(self):
        m = IdentifierResolutionMonitor(
            self._db, "Will succeed",
            required_coverage_providers=[self.always_successful]
        )

        m.run_once(None)

        # The coverage provider succeeded and an appropriate coverage
        # record was created to mark the success.
        r = get_one(self._db, CoverageRecord, identifier=self.identifier)
        eq_(None, r.exception)

        # The outstanding UnresolvedIdentifier has been deleted.
        eq_(None, get_one(self._db, UnresolvedIdentifier))

    def test_resolve_success(self):
        m = IdentifierResolutionMonitor(
            self._db, "Will succeed",
            required_coverage_providers=[self.always_successful]
        )

        success = m.resolve(self.ui)

        # The coverage provider succeeded and an appropriate coverage
        # record was created to mark the success.
        r = get_one(self._db, CoverageRecord, identifier=self.identifier)
        eq_(None, r.exception)

        # resolve() returned True to indicate success.
        eq_(True, success)

    def test_resolve_fails_when_required_provider_returns_coveragefailed(self):
        m = IdentifierResolutionMonitor(
            self._db, "Will fail",
            required_coverage_providers=[self.never_successful]
        )

        assert_raises_regexp(
            ResolutionFailed, 
            "500: What did you expect?",
            m.resolve, self.ui
        )

        # The coverage provider failed and an appropriate coverage
        # record was created to mark the failure.
        r = get_one(self._db, CoverageRecord, identifier=self.identifier)
        eq_("What did you expect?", r.exception)

    def test_run_once_fails_when_required_provider_raises_exception(self):
        m = IdentifierResolutionMonitor(
            self._db, "Will raise exception",
            required_coverage_providers=[self.broken]
        )

        m.run_once()

        # The exception was recorded in the UnresolvedIdentifier object.
        assert "I'm too broken to even return a CoverageFailure." in self.ui.exception


    def test_run_once_fails_when_finalize_raises_exception(self):
        class FinalizeAlwaysFails(IdentifierResolutionMonitor):
            def finalize(self, unresolved_identifier):
                raise Exception("Oh no!")

        m = FinalizeAlwaysFails(self._db, "Always fails")
        ui, ignore = self._unresolved_identifier()
        m.run_once(ui)
        eq_(500, ui.status)
        assert "Oh no!" in ui.exception

    def test_resolve_succeeds_when_optional_provider_fails(self):
        ui, ignore = self._unresolved_identifier()
        identifier = self.ui.identifier
        source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        p = NeverSuccessfulCoverageProvider(
            "Never", [identifier.type], source
        )
        m = IdentifierResolutionMonitor(
            self._db, "Will fail but it's OK",
            optional_coverage_providers=[p]
        )

        success = m.resolve(self.ui)

        # The coverage provider failed and an appropriate coverage record
        # was created to mark the failure.
        r = get_one(self._db, CoverageRecord, identifier=self.identifier)
        eq_("What did you expect?", r.exception)

        # But because it was an optional CoverageProvider that failed,
        # no exception was raised and resolve() returned True to indicate
        # success.
        eq_(success, True)


