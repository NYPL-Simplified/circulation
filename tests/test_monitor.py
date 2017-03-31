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
    Collection,
    DataSource,
    Identifier,
    Subject,
    Timestamp,
)

from monitor import (
    CollectionMonitor,
    Monitor,
    PresentationReadyMonitor,
    SubjectSweepMonitor,
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

        class NoProtocolMonitor(CollectionMonitor):
            SERVICE_NAME = "Test Monitor 1"
            PROTOCOL = None

        class OverdriveMonitor(CollectionMonitor):
            SERVICE_NAME = "Test Monitor 2"
            PROTOCOL = Collection.OVERDRIVE

        # Two collections.
        c1 = self._collection(protocol=Collection.OVERDRIVE)
        c2 = self._collection(protocol=Collection.BIBLIOTHECA)

        # The NoProtocolMonitor can be instantiated with either one.
        NoProtocolMonitor(self._db, c1)
        NoProtocolMonitor(self._db, c2)

        # The OverdriveMonitor can only be instantiated with the first one.
        OverdriveMonitor(self._db, c1)
        assert_raises_regexp(
            ValueError,
            "Collection protocol \(Bibliotheca\) does not match Monitor protocol \(Overdrive\)",
            OverdriveMonitor, self._db, c2
        )
        
    def test_all(self):
        """Test that we can create a list of Monitors"""
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



class TestSubjectSweepMonitor(DatabaseTest):

    def test_subject_query(self):
        s1, ignore = Subject.lookup(self._db, Subject.DDC, "100", None)
        s2, ignore = Subject.lookup(
            self._db, Subject.TAG, None, "100 Years of Solitude"
        )

        dewey_monitor = SubjectSweepMonitor(self._db, Subject.DDC)
        eq_([s1], dewey_monitor.subject_query().all())

        one_hundred_monitor = SubjectSweepMonitor(self._db, None, "100")
        eq_([s1, s2], one_hundred_monitor.subject_query().all())

        specific_tag_monitor = SubjectSweepMonitor(
            self._db, Subject.TAG, "Years"
        )
        eq_([s2], specific_tag_monitor.subject_query().all())
        
