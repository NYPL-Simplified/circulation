import datetime
import os
import random
import shutil
import stat
import tempfile
from StringIO import StringIO

from nose.tools import (
    assert_raises,
    assert_raises_regexp,
    eq_,
)
from parameterized import parameterized

from core.s3 import S3Uploader, MinIOUploader
from . import (
    DatabaseTest,
)
from ..classifier import Classifier
from ..config import (
    CannotLoadConfiguration,
)
from ..external_search import MockExternalSearchIndex
from ..lane import (
    Lane,
    WorkList,
)
from ..metadata_layer import (
    LinkData,
    TimestampData,
)
from ..mirror import MirrorUploader
from ..model import (
    create,
    dump_query,
    get_one,
    CachedFeed,
    Collection,
    Complaint,
    ConfigurationSetting,
    Contributor,
    CoverageRecord,
    DataSource,
    ExternalIntegration,
    Hyperlink,
    Identifier,
    Library,
    RightsStatus,
    Timestamp,
    Work,
    WorkCoverageRecord,
)
from ..model.configuration import ExternalIntegrationLink
from ..monitor import (
    Monitor,
    CollectionMonitor,
    ReaperMonitor,
)
from ..scripts import (
    AddClassificationScript,
    CheckContributorNamesInDB,
    CollectionInputScript,
    ConfigureCollectionScript,
    ConfigureIntegrationScript,
    ConfigureLaneScript,
    ConfigureLibraryScript,
    ConfigureSiteScript,
    DatabaseMigrationInitializationScript,
    DatabaseMigrationScript,
    Explain,
    IdentifierInputScript,
    LaneSweeperScript,
    LibraryInputScript,
    ListCollectionMetadataIdentifiersScript,
    MirrorResourcesScript,
    MockStdin,
    OPDSImportScript,
    PatronInputScript,
    RebuildSearchIndexScript,
    ReclassifyWorksForUncheckedSubjectsScript,
    RunCollectionMonitorScript,
    RunCoverageProviderScript,
    RunMonitorScript,
    RunMultipleMonitorsScript,
    RunReaperMonitorsScript,
    RunThreadedCollectionCoverageProviderScript,
    RunWorkCoverageProviderScript,
    Script,
    SearchIndexCoverageRemover,
    ShowCollectionsScript,
    ShowIntegrationsScript,
    ShowLanesScript,
    ShowLibrariesScript,
    TimestampScript,
    UpdateCustomListSizeScript,
    UpdateLaneSizeScript,
    WhereAreMyBooksScript,
    WorkClassificationScript,
    WorkProcessingScript,
    CollectionType)
from ..testing import (
    AlwaysSuccessfulCollectionCoverageProvider,
    AlwaysSuccessfulWorkCoverageProvider,
)
from ..util.worker_pools import (
    DatabasePool,
)


class TestScript(DatabaseTest):

    def test_parse_time(self):
        reference_date = datetime.datetime(2016, 1, 1)

        eq_(Script.parse_time("2016-01-01"), reference_date)

        eq_(Script.parse_time("2016-1-1"), reference_date)

        eq_(Script.parse_time("1/1/2016"), reference_date)

        eq_(Script.parse_time("20160101"), reference_date)

        assert_raises(ValueError, Script.parse_time, "201601-01")


    def test_script_name(self):

        class Sample(Script):
            pass

        # If a script does not define .name, its class name
        # is treated as the script name.
        script = Sample(self._db)
        eq_("Sample", script.script_name)


        # If a script does define .name, that's used instead.
        script.name = "I'm a script"
        eq_(script.name, script.script_name)


class TestTimestampScript(DatabaseTest):

    def _ts(self, script):
        """Convenience method to look up the Timestamp for a script.

        We don't use Timestamp.stamp() because we want to make sure
        that Timestamps are being created by the actual code, not test
        code.
        """
        return get_one(self._db, Timestamp, service=script.script_name)

    def test_update_timestamp(self):
        # Test the Script subclass that sets a timestamp after a
        # script is run.
        class Noisy(TimestampScript):
            def do_run(self):
                pass
        script = Noisy(self._db)
        script.run()

        timestamp = self._ts(script)

        # The start and end points of do_run() have become
        # Timestamp.start and Timestamp.finish.
        now = datetime.datetime.utcnow()
        assert (now - timestamp.start).total_seconds() < 5
        assert (now - timestamp.finish).total_seconds() < 5
        assert timestamp.start < timestamp.finish
        eq_(None, timestamp.collection)

    def test_update_timestamp_with_collection(self):
        # A script can indicate that it is operating on a specific
        # collection.
        class MyCollection(TimestampScript):
            def do_run(self):
                pass

        script = MyCollection(self._db)
        script.timestamp_collection = self._default_collection
        script.run()
        timestamp = self._ts(script)
        eq_(self._default_collection, timestamp.collection)

    def test_update_timestamp_on_failure(self):
        # A TimestampScript that fails to complete still has its
        # Timestamp set -- the timestamp just records the time that
        # the script stopped running.
        #
        # This is different from Monitors, where the timestamp
        # is only updated when the Monitor runs to completion.
        # The difference is that Monitors are frequently responsible for
        # keeping track of everything that happened since a certain
        # time, and Scripts generally aren't.
        class Broken(TimestampScript):
            def do_run(self):
                raise Exception("i'm broken")

        script = Broken(self._db)
        assert_raises_regexp(Exception, "i'm broken", script.run)
        timestamp = self._ts(script)

        now = datetime.datetime.utcnow()
        assert (now - timestamp.finish).total_seconds() < 5

        # A stack trace for the exception has been recorded in the
        # Timestamp object.
        assert "Exception: i'm broken" in timestamp.exception

    def test_normal_script_has_no_timestamp(self):
        # Running a normal script does _not_ set a Timestamp.
        class Silent(Script):
            def do_run(self):
                pass
        script = Silent(self._db)
        script.run()
        eq_(None, self._ts(script))


class TestCheckContributorNamesInDB(DatabaseTest):
    def test_process_contribution_local(self):
        stdin = MockStdin()
        cmd_args = []

        edition_alice, pool_alice = self._edition(
            data_source_name=DataSource.GUTENBERG,
            identifier_type=Identifier.GUTENBERG_ID,
            identifier_id="1",
            with_open_access_download=True,
            title="Alice Writes Books")

        alice, new = self._contributor(sort_name="Alice Alrighty")
        alice._sort_name = "Alice Alrighty"
        alice.display_name="Alice Alrighty"

        edition_alice.add_contributor(
            alice, [Contributor.PRIMARY_AUTHOR_ROLE]
        )
        edition_alice.sort_author="Alice Rocks"

        # everything is set up as we expect
        eq_("Alice Alrighty", alice.sort_name)
        eq_("Alice Alrighty", alice.display_name)
        eq_("Alice Rocks", edition_alice.sort_author)

        edition_bob, pool_bob = self._edition(
            data_source_name=DataSource.GUTENBERG,
            identifier_type=Identifier.GUTENBERG_ID,
            identifier_id="2",
            with_open_access_download=True,
            title="Bob Writes Books")

        bob, new = self._contributor(sort_name="Bob")
        bob.display_name="Bob Bitshifter"

        edition_bob.add_contributor(
            bob, [Contributor.PRIMARY_AUTHOR_ROLE]
        )
        edition_bob.sort_author="Bob Rocks"

        eq_("Bob", bob.sort_name)
        eq_("Bob Bitshifter", bob.display_name)
        eq_("Bob Rocks", edition_bob.sort_author)

        contributor_fixer = CheckContributorNamesInDB(
            _db=self._db, cmd_args=cmd_args, stdin=stdin
        )
        contributor_fixer.do_run()

        # Alice got fixed up.
        eq_("Alrighty, Alice", alice.sort_name)
        eq_("Alice Alrighty", alice.display_name)
        eq_("Alrighty, Alice", edition_alice.sort_author)

        # Bob's repairs were too extensive to make.
        eq_("Bob", bob.sort_name)
        eq_("Bob Bitshifter", bob.display_name)
        eq_("Bob Rocks", edition_bob.sort_author)

        # and we lodged a proper complaint
        q = self._db.query(Complaint).filter(Complaint.source==CheckContributorNamesInDB.COMPLAINT_SOURCE)
        q = q.filter(Complaint.type==CheckContributorNamesInDB.COMPLAINT_TYPE).filter(Complaint.license_pool==pool_bob)
        complaints = q.all()
        eq_(1, len(complaints))
        eq_(None, complaints[0].resolved)



class TestIdentifierInputScript(DatabaseTest):

    def test_parse_list_as_identifiers(self):

        i1 = self._identifier()
        i2 = self._identifier()
        args = [i1.identifier, 'no-such-identifier', i2.identifier]
        identifiers = IdentifierInputScript.parse_identifier_list(
            self._db, i1.type, None, args
        )
        eq_([i1, i2], identifiers)

        eq_([], IdentifierInputScript.parse_identifier_list(
            self._db, i1.type, None, [])
        )

    def test_parse_list_as_identifiers_with_autocreate(self):

        type = Identifier.OVERDRIVE_ID
        args = ['brand-new-identifier']
        [i] = IdentifierInputScript.parse_identifier_list(
            self._db, type, None, args, autocreate=True
        )
        eq_(type, i.type)
        eq_('brand-new-identifier', i.identifier)

    def test_parse_list_as_identifiers_with_data_source(self):
        lp1 = self._licensepool(None, data_source_name=DataSource.UNGLUE_IT)
        lp2 = self._licensepool(None, data_source_name=DataSource.FEEDBOOKS)
        lp3 = self._licensepool(None, data_source_name=DataSource.FEEDBOOKS)

        i1, i2, i3 = [lp.identifier for lp in [lp1, lp2, lp3]]
        i1.type = i2.type = Identifier.URI
        source = DataSource.lookup(self._db, DataSource.FEEDBOOKS)

        # Only URIs with a FeedBooks LicensePool are selected.
        identifiers = IdentifierInputScript.parse_identifier_list(
            self._db, Identifier.URI, source, [])
        eq_([i2], identifiers)

    def test_parse_list_as_identifiers_by_database_id(self):
        id1 = self._identifier()
        id2 = self._identifier()

        # Make a list containing two Identifier database IDs,
        # as well as two strings which are not existing Identifier database
        # IDs.
        ids = [id1.id, "10000000", "abcde", id2.id]

        identifiers = IdentifierInputScript.parse_identifier_list(
            self._db, IdentifierInputScript.DATABASE_ID, None, ids)
        eq_([id1, id2], identifiers)

    def test_parse_command_line(self):
        i1 = self._identifier()
        i2 = self._identifier()
        # We pass in one identifier on the command line...
        cmd_args = ["--identifier-type",
                    i1.type, i1.identifier]
        # ...and another one into standard input.
        stdin = MockStdin(i2.identifier)
        parsed = IdentifierInputScript.parse_command_line(
            self._db, cmd_args, stdin
        )
        eq_([i1, i2], parsed.identifiers)
        eq_(i1.type, parsed.identifier_type)

    def test_parse_command_line_no_identifiers(self):
        cmd_args = [
            "--identifier-type", Identifier.OVERDRIVE_ID,
            "--identifier-data-source", DataSource.STANDARD_EBOOKS
        ]
        parsed = IdentifierInputScript.parse_command_line(
            self._db, cmd_args, MockStdin()
        )
        eq_([], parsed.identifiers)
        eq_(Identifier.OVERDRIVE_ID, parsed.identifier_type)
        eq_(DataSource.STANDARD_EBOOKS, parsed.identifier_data_source)


class SuccessMonitor(Monitor):
    """A simple Monitor that alway succeeds."""
    SERVICE_NAME = "Success"
    def run(self):
        self.ran = True


class OPDSCollectionMonitor(CollectionMonitor):
    """Mock Monitor for use in tests of Run*MonitorScript."""
    SERVICE_NAME = "Test Monitor"
    PROTOCOL = ExternalIntegration.OPDS_IMPORT

    def __init__(self, _db, test_argument=None, **kwargs):
        self.test_argument = test_argument
        super(OPDSCollectionMonitor, self).__init__(_db, **kwargs)

    def run_once(self, progress):
        self.collection.ran_with_argument = self.test_argument


class DoomedCollectionMonitor(CollectionMonitor):
    """Mock CollectionMonitor that always raises an exception."""
    SERVICE_NAME = "Doomed Monitor"
    PROTOCOL = ExternalIntegration.OPDS_IMPORT
    def run(self, *args, **kwargs):
        self.ran = True
        self.collection.doomed = True
        raise Exception("Doomed!")


class TestRunMonitorScript(DatabaseTest):

    def test_run_with_collection_monitor(self):
        """It's not ideal, but you can run a CollectionMonitor script from
        RunMonitorScript. This will run the monitor on every
        appropriate Collection.
        """
        c1 = self._collection()
        c2 = self._collection()
        script = RunMonitorScript(
            OPDSCollectionMonitor, self._db, test_argument="test value"
        )
        script.run()
        for c in [c1, c2]:
            eq_("test value", c.ran_with_argument)


class TestRunMultipleMonitorsScript(DatabaseTest):

    def test_do_run(self):
        m1 = SuccessMonitor(self._db)
        m2 = DoomedCollectionMonitor(self._db, self._default_collection)
        m3 = SuccessMonitor(self._db)

        class MockScript(RunMultipleMonitorsScript):
            name = "Run three monitors"
            def monitors(self, **kwargs):
                self.kwargs = kwargs
                return [m1, m2, m3]

        # Run the script.
        script = MockScript(self._db, kwarg="value")
        script.do_run()

        # The kwarg we passed in to the MockScript constructor was
        # propagated into the monitors() method.
        eq_(dict(kwarg="value"), script.kwargs)

        # All three monitors were run, even though the
        # second one raised an exception.
        eq_(True, m1.ran)
        eq_(True, m2.ran)
        eq_(True, m3.ran)

        # The exception that crashed the second monitor was stored as
        # .exception, in case we want to look at it.
        eq_("Doomed!", unicode(m2.exception))
        eq_(None, getattr(m1, 'exception', None))


class TestRunCollectionMonitorScript(DatabaseTest):


    def test_monitors(self):
        # Here we have three OPDS import Collections...
        o1 = self._collection()
        o2 = self._collection()
        o3 = self._collection()

        # ...and a Bibliotheca collection.
        b1 = self._collection(protocol=ExternalIntegration.BIBLIOTHECA)

        script = RunCollectionMonitorScript(OPDSCollectionMonitor, self._db)

        # Calling monitors() instantiates an OPDSCollectionMonitor
        # for every OPDS import collection. The Bibliotheca collection
        # is unaffected.
        monitors = script.monitors()
        collections = [x.collection for x in monitors]
        eq_(set(collections), set([o1, o2, o3]))
        for i in monitors:
            assert isinstance(monitor, OPDSCollectionMonitor)


class TestRunReaperMonitorsScript(DatabaseTest):

    def test_monitors(self):
        """This script instantiates a Monitor for every class in
        ReaperMonitor.REGISTRY.
        """
        old_registry = ReaperMonitor.REGISTRY
        ReaperMonitor.REGISTRY = [SuccessMonitor]
        script = RunReaperMonitorsScript(self._db)
        [monitor] = script.monitors()
        assert isinstance(monitor, SuccessMonitor)
        ReaperMonitor.REGISTRY = old_registry


class TestPatronInputScript(DatabaseTest):

    def test_parse_patron_list(self):
        """Test that patrons can be identified with any unique identifier."""
        l1 = self._library()
        l2 = self._library()
        p1 = self._patron()
        p1.authorization_identifier = self._str
        p1.library_id = l1.id
        p2 = self._patron()
        p2.username = self._str
        p2.library_id = l1.id
        p3 = self._patron()
        p3.external_identifier = self._str
        p3.library_id = l1.id
        p4 = self._patron()
        p4.external_identifier = self._str
        p4.library_id = l2.id
        args = [p1.authorization_identifier, 'no-such-patron',
                '', p2.username, p3.external_identifier]
        patrons = PatronInputScript.parse_patron_list(
            self._db, l1, args
        )
        eq_([p1, p2, p3], patrons)
        eq_([], PatronInputScript.parse_patron_list(self._db, l1, []))
        eq_([p1], PatronInputScript.parse_patron_list(self._db, l1, [p1.external_identifier, p4.external_identifier]))
        eq_([p4], PatronInputScript.parse_patron_list(self._db, l2, [p1.external_identifier, p4.external_identifier]))

    def test_parse_command_line(self):
        l1 = self._library()
        p1 = self._patron()
        p2 = self._patron()
        p1.authorization_identifier = self._str
        p2.authorization_identifier = self._str
        p1.library_id = l1.id
        p2.library_id = l1.id
        # We pass in one patron identifier on the command line...
        cmd_args = [l1.short_name, p1.authorization_identifier]
        # ...and another one into standard input.
        stdin = MockStdin(p2.authorization_identifier)
        parsed = PatronInputScript.parse_command_line(
            self._db, cmd_args, stdin
        )
        eq_([p1, p2], parsed.patrons)

    def test_patron_different_library(self):
        l1 = self._library()
        l2 = self._library()
        p1 = self._patron()
        p2 = self._patron()
        p1.authorization_identifier = self._str
        p2.authorization_identifier = p1.authorization_identifier
        p1.library_id = l1.id
        p2.library_id = l2.id
        cmd_args = [l1.short_name, p1.authorization_identifier]
        parsed = PatronInputScript.parse_command_line(self._db, cmd_args, None)
        eq_([p1], parsed.patrons)
        cmd_args = [l2.short_name, p2.authorization_identifier]
        parsed = PatronInputScript.parse_command_line(self._db, cmd_args, None)
        eq_([p2], parsed.patrons)

    def test_do_run(self):
        """Test that PatronInputScript.do_run() calls process_patron()
        for every patron designated by the command-line arguments.
        """
        class MockPatronInputScript(PatronInputScript):
            def process_patron(self, patron):
                patron.processed = True
        l1 = self._library()
        p1 = self._patron()
        p2 = self._patron()
        p3 = self._patron()
        p1.library_id = l1.id
        p2.library_id = l1.id
        p3.library_id = l1.id
        p1.processed = False
        p2.processed = False
        p3.processed = False
        p1.authorization_identifier = self._str
        p2.authorization_identifier = self._str
        cmd_args = [l1.short_name, p1.authorization_identifier]
        stdin = MockStdin(p2.authorization_identifier)
        script = MockPatronInputScript(self._db)
        script.do_run(cmd_args=cmd_args, stdin=stdin)
        eq_(True, p1.processed)
        eq_(True, p2.processed)
        eq_(False, p3.processed)


class TestLibraryInputScript(DatabaseTest):

    def test_parse_library_list(self):
        """Test that libraries can be identified with their full name or short name."""
        l1 = self._library()
        l2 = self._library()
        args = [l1.name, 'no-such-library', '', l2.short_name]
        libraries = LibraryInputScript.parse_library_list(
            self._db, args
        )
        eq_([l1, l2], libraries)

        eq_([], LibraryInputScript.parse_library_list(self._db, []))

    def test_parse_command_line(self):
        l1 = self._library()
        # We pass in one library identifier on the command line...
        cmd_args = [l1.name]
        parsed = LibraryInputScript.parse_command_line(self._db, cmd_args)

        # And here it is.
        eq_([l1], parsed.libraries)

    def test_parse_command_line_no_identifiers(self):
        """If you don't specify any libraries on the command
        line, we will process all libraries in the system.
        """
        parsed =LibraryInputScript.parse_command_line(
            self._db, []
        )
        eq_(self._db.query(Library).all(), parsed.libraries)


    def test_do_run(self):
        """Test that LibraryInputScript.do_run() calls process_library()
        for every library designated by the command-line arguments.
        """
        class MockLibraryInputScript(LibraryInputScript):
            def process_library(self, library):
                library.processed = True
        l1 = self._library()
        l2 = self._library()
        l2.processed = False
        cmd_args = [l1.name]
        script = MockLibraryInputScript(self._db)
        script.do_run(cmd_args=cmd_args)
        eq_(True, l1.processed)
        eq_(False, l2.processed)


class TestLaneSweeperScript(DatabaseTest):

    def test_process_library(self):

        class Mock(LaneSweeperScript):
            def __init__(self, _db):
                super(Mock, self).__init__(_db)
                self.considered = []
                self.processed = []

            def should_process_lane(self, lane):
                self.considered.append(lane)
                return lane.display_name == 'process me'

            def process_lane(self, lane):
                self.processed.append(lane)

        good = self._lane(display_name="process me")
        bad = self._lane(display_name="don't process me")
        good_child = self._lane(display_name="process me", parent=bad)

        script = Mock(self._db)
        script.do_run(cmd_args=[])

        # The first item considered for processing was an ad hoc
        # WorkList representing the library's entire collection.
        worklist = script.considered.pop(0)
        eq_(self._default_library, worklist.get_library(self._db))
        eq_(self._default_library.name, worklist.display_name)
        eq_(set([good, bad]), set(worklist.children))

        # After that, every lane was considered for processing, with
        # top-level lanes considered first.
        eq_(set([good, bad, good_child]), set(script.considered))

        # But a lane was processed only if should_process_lane
        # returned True.
        eq_(set([good, good_child]), set(script.processed))


class TestRunCoverageProviderScript(DatabaseTest):

    def test_parse_command_line(self):
        identifier = self._identifier()
        cmd_args = ["--cutoff-time", "2016-05-01", "--identifier-type",
                    identifier.type, identifier.identifier]
        parsed = RunCoverageProviderScript.parse_command_line(
            self._db, cmd_args, MockStdin()
        )
        eq_(datetime.datetime(2016, 5, 1), parsed.cutoff_time)
        eq_([identifier], parsed.identifiers)
        eq_(identifier.type, parsed.identifier_type)


class TestRunThreadedCollectionCoverageProviderScript(DatabaseTest):

    def test_run(self):
        provider = AlwaysSuccessfulCollectionCoverageProvider
        script = RunThreadedCollectionCoverageProviderScript(
            provider, worker_size=2, _db=self._db
        )

        # If there are no collections for the provider, run does nothing.
        # Pass a mock pool that will raise an error if it's used.
        pool = object()
        collection = self._collection(protocol=ExternalIntegration.ENKI)

        # Run exits without a problem because the pool is never touched.
        script.run(pool=pool)

        # Create some identifiers that need coverage.
        collection = self._collection()
        ed1, lp1 = self._edition(collection=collection, with_license_pool=True)
        ed2, lp2 = self._edition(collection=collection, with_license_pool=True)
        ed3 = self._edition()

        [id1, id2, id3] = [e.primary_identifier for e in (ed1, ed2, ed3)]

        # Set a timestamp for the provider.
        timestamp = Timestamp.stamp(
            self._db, provider.SERVICE_NAME, Timestamp.COVERAGE_PROVIDER_TYPE,
            collection=collection
        )
        original_timestamp = timestamp.finish
        self._db.commit()

        pool = DatabasePool(2, script.session_factory)
        script.run(pool=pool)
        self._db.commit()

        # The expected number of workers and jobs have been created.
        eq_(2, len(pool.workers))
        eq_(1, pool.job_total)

        # All relevant identifiers have been given coverage.
        source = DataSource.lookup(self._db, provider.DATA_SOURCE_NAME)
        identifiers_missing_coverage = Identifier.missing_coverage_from(
            self._db, provider.INPUT_IDENTIFIER_TYPES, source,
        )
        eq_([id3], identifiers_missing_coverage.all())

        record1, was_registered1 = provider.register(id1)
        record2, was_registered2 = provider.register(id2)
        eq_(CoverageRecord.SUCCESS, record1.status)
        eq_(CoverageRecord.SUCCESS, record2.status)
        eq_((False, False), (was_registered1, was_registered2))


        # The timestamp for the provider has been updated.
        new_timestamp = Timestamp.value(
            self._db, provider.SERVICE_NAME, Timestamp.COVERAGE_PROVIDER_TYPE,
            collection
        )
        assert new_timestamp != original_timestamp
        assert new_timestamp > original_timestamp


class TestRunWorkCoverageProviderScript(DatabaseTest):

    def test_constructor(self):
        script = RunWorkCoverageProviderScript(
            AlwaysSuccessfulWorkCoverageProvider, _db=self._db,
            batch_size=123
        )
        [provider] = script.providers
        assert isinstance(provider, AlwaysSuccessfulWorkCoverageProvider)
        eq_(123, provider.batch_size)


class TestWorkProcessingScript(DatabaseTest):

    def test_make_query(self):
        # Create two Gutenberg works and one Overdrive work
        g1 = self._work(with_license_pool=True, with_open_access_download=True)
        g2 = self._work(with_license_pool=True, with_open_access_download=True)

        overdrive_edition = self._edition(
            data_source_name=DataSource.OVERDRIVE,
            identifier_type=Identifier.OVERDRIVE_ID,
            with_license_pool=True
        )[0]
        overdrive_work = self._work(presentation_edition=overdrive_edition)

        ugi_edition = self._edition(
            data_source_name=DataSource.UNGLUE_IT,
            identifier_type=Identifier.URI,
            with_license_pool=True
        )[0]
        unglue_it = self._work(presentation_edition=ugi_edition)

        se_edition = self._edition(
            data_source_name=DataSource.STANDARD_EBOOKS,
            identifier_type=Identifier.URI,
            with_license_pool=True
        )[0]
        standard_ebooks = self._work(presentation_edition=se_edition)

        everything = WorkProcessingScript.make_query(self._db, None, None, None)
        eq_(set([g1, g2, overdrive_work, unglue_it, standard_ebooks]),
            set(everything.all()))

        all_gutenberg = WorkProcessingScript.make_query(
            self._db, Identifier.GUTENBERG_ID, [], None
        )
        eq_(set([g1, g2]), set(all_gutenberg.all()))

        one_gutenberg = WorkProcessingScript.make_query(
            self._db, Identifier.GUTENBERG_ID, [g1.license_pools[0].identifier], None
        )
        eq_([g1], one_gutenberg.all())

        one_standard_ebook = WorkProcessingScript.make_query(
            self._db, Identifier.URI, [], DataSource.STANDARD_EBOOKS
        )
        eq_([standard_ebooks], one_standard_ebook.all())


class TestTimestampInfo(DatabaseTest):

    TimestampInfo = DatabaseMigrationScript.TimestampInfo

    def test_find(self):
        # If there isn't a timestamp for the given service,
        # nothing is returned.
        result = self.TimestampInfo.find(self, 'test')
        eq_(None, result)

        # But an empty Timestamp has been placed into the database.
        timestamp = self._db.query(Timestamp).filter(Timestamp.service=='test').one()
        eq_(None, timestamp.start)
        eq_(None, timestamp.finish)
        eq_(None, timestamp.counter)

        # A repeat search for the empty Timestamp also results in None.
        script = DatabaseMigrationScript(self._db)
        eq_(None, self.TimestampInfo.find(script, 'test'))

        # If the Timestamp is stamped, it is returned.
        timestamp.finish = datetime.datetime.utcnow()
        timestamp.counter = 1
        self._db.flush()

        result = self.TimestampInfo.find(script, 'test')
        eq_(timestamp.finish, result.finish)
        eq_(1, result.counter)

    def test_update(self):
        # Create a Timestamp to be updated.
        past = datetime.datetime.strptime('19980101', '%Y%m%d')
        stamp = Timestamp.stamp(
            self._db, 'test', Timestamp.SCRIPT_TYPE, None, start=past,
            finish=past
        )
        script = DatabaseMigrationScript(self._db)
        timestamp_info = self.TimestampInfo.find(script, 'test')

        now = datetime.datetime.utcnow()
        timestamp_info.update(self._db, now, 2)

        # When we refresh the Timestamp object, it's been updated.
        self._db.refresh(stamp)
        eq_(now, stamp.start)
        eq_(now, stamp.finish)
        eq_(2, stamp.counter)

    def save(self):
        # The Timestamp doesn't exist.
        timestamp_qu = self._db.query(Timestamp).filter(Timestamp.service=='test')
        eq_(False, timestamp_qu.exists())

        now = datetime.datetime.utcnow()
        timestamp_info = self.TimestampInfo('test', now, 47)
        timestamp_info.save(self._db)

        # The Timestamp exists now.
        timestamp = timestamp_qu.one()
        eq_(now, timestamp.finish)
        eq_(47, timestamp.counter)


class MockDatabaseMigrationScript(DatabaseMigrationScript):

    @property
    def directories_by_priority(self):
        """Uses test migration directories for """
        real_migration_directories = super(
            MockDatabaseMigrationScript, self
        ).directories_by_priority

        test_directories = [
            os.path.join(os.path.split(d)[0], 'test_migration')
            for d in real_migration_directories
        ]

        return test_directories


class DatabaseMigrationScriptTest(DatabaseTest):

    def create_mock_script(self, cls, _db):
        """Creates a mock version of a DatabaseMigrationScript"""

        class MockDatabaseMigrationScript(cls):

            @property
            def directories_by_priority(self):
                """Uses test migration directories to find migration files."""
                real_migration_directories = super(
                    MockDatabaseMigrationScript, self
                ).directories_by_priority

                test_directories = [
                    os.path.join(os.path.split(d)[0], 'test_migration')
                    for d in real_migration_directories
                ]
                return test_directories

        return MockDatabaseMigrationScript(_db=_db)

    def _create_test_migration_file(self, directory, unique_string,
                                    migration_type, migration_date=None):
        suffix = '.'+migration_type

        if migration_type=='sql':
            # Create unique, innocuous content for a SQL file.
            # This SQL inserts a timestamp into the test database.
            service = "Test Database Migration Script - %s" % unique_string
            content = (("insert into timestamps(service, finish)"
                        " values ('%s', '%s');") % (service, '1970-01-01'))
        elif migration_type=='py':
            # Create unique, innocuous content for a Python file.
            # This python creates a temporary .py file in core/tests.
            core = os.path.split(self.core_migration_dir)[0]
            target_dir = os.path.join(core, 'tests')
            content = (
                "#!/usr/bin/env python\n\n"+
                "import tempfile\nimport os\n\n"+
                "file_info = tempfile.mkstemp(prefix='"+
                unique_string+"-', suffix='.py', dir='"+target_dir+"')\n\n"+
                "# Close file descriptor\n"+
                "os.close(file_info[0])\n"
            )

        if not migration_date:
            # Default date is just after self.timestamp.
            migration_date = '20260811'
        prefix = migration_date + '-'

        migration_file_info = tempfile.mkstemp(
            prefix=prefix, suffix=suffix, dir=directory
        )
        # Hold onto the filename for deletion in teardown().
        fd, migration_file = migration_file_info
        self.migration_files.append(migration_file)

        with open(migration_file, 'w') as migration:
            # Write content to the file.
            migration.write(content)

        # If it's a python migration, make it executable.
        if migration_file.endswith('py'):
            original_mode = os.stat(migration_file).st_mode
            mode = original_mode | (stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
            os.chmod(migration_file, mode)

        # Close the file descriptor.
        os.close(fd)

    def setup(self):
        super(DatabaseMigrationScriptTest, self).setup()

        # This list holds any temporary files created during tests
        # so they can be deleted during teardown().
        self.migration_files = []

        # Create temporary migration directories where
        # DatabaseMigrationScript expects them.
        script = self.create_mock_script(DatabaseMigrationScript, self._db)
        self.directories = script.directories_by_priority
        [self.core_migration_dir, self.parent_migration_dir] = self.directories
        for migration_dir in self.directories:
            if not os.path.isdir(migration_dir):
                temp_migration_dir = tempfile.mkdtemp()
                shutil.move(temp_migration_dir, migration_dir)

    def teardown(self):
        """Delete any files and directories created during testing."""
        for fpath in self.migration_files:
            os.remove(fpath)

        if self.migration_files:
            for directory in self.directories:
                os.rmdir(directory)

        test_dir = os.path.split(__file__)[0]
        all_files = os.listdir(test_dir)
        test_generated_files = sorted(
            [f for f in all_files if f.startswith(('CORE', 'SERVER'))]
        )
        for filename in test_generated_files:
            os.remove(os.path.join(test_dir, filename))

        timestamps = self._db.query(Timestamp).filter(
            Timestamp.service.like('%Database Migration%')
        ).delete(synchronize_session=False)

        super(DatabaseMigrationScriptTest, self).teardown()


class TestDatabaseMigrationScript(DatabaseMigrationScriptTest):

    def _create_test_migrations(self):
        """Sets up migrations in the expected locations"""
        # Put a file of each migratable type in each temporary migration
        # directory.
        self._create_test_migration_file(self.core_migration_dir, 'CORE', 'sql')
        self._create_test_migration_file(self.core_migration_dir, 'CORE', 'py')
        self._create_test_migration_file(self.parent_migration_dir, 'SERVER', 'sql')
        self._create_test_migration_file(self.parent_migration_dir, 'SERVER', 'py')

    def setup(self):
        super(TestDatabaseMigrationScript, self).setup()
        self.script = self.create_mock_script(DatabaseMigrationScript, self._db)
        self._create_test_migrations()

        stamp = datetime.datetime.strptime('20260810', '%Y%m%d')
        self.timestamp = Timestamp(
            service=self.script.name, start=stamp, finish=stamp
        )
        self.python_timestamp = Timestamp(
            service=self.script.PY_TIMESTAMP_SERVICE_NAME, start=stamp,
            finish=stamp
        )
        self._db.add_all([self.timestamp, self.python_timestamp])
        self._db.flush()

        self.timestamp_info = self.script.TimestampInfo(
            self.timestamp.service, self.timestamp.start
        )

    def test_name(self):
        """DatabaseMigrationScript.name returns an appropriate timestamp service
        name, depending on whether it is running only Python migrations or not.
        """

        # The default script returns the default timestamp name.
        eq_("Database Migration", self.script.name)

        # A python-only script returns a Python-specific timestamp name.
        self.script.python_only=True
        eq_("Database Migration - Python", self.script.name)

    def test_timestamp_properties(self):
        """DatabaseMigrationScript provides the appropriate TimestampInfo
        objects as properties.
        """
        # If there aren't any Database Migrations in the database, no
        # timestamps are returned.
        timestamps = self._db.query(Timestamp).filter(
            Timestamp.service.like('Database Migration%')
        )
        for timestamp in timestamps:
            self._db.delete(timestamp)
        self._db.commit()

        self.script._session = self._db
        eq_(None, self.script.python_timestamp)
        eq_(None, self.script.overall_timestamp)

        # If the Timestamps exist in the database, but they don't have
        # a timestamp, nothing is returned. Timestamps must be initialized.
        overall = self._db.query(Timestamp).filter(
            Timestamp.service==self.script.SERVICE_NAME
        ).one()
        python = self._db.query(Timestamp).filter(
            Timestamp.service==self.script.PY_TIMESTAMP_SERVICE_NAME
        ).one()

        # Neither Timestamp object has a timestamp.
        eq_((None, None), (python.finish, overall.finish))
        # So neither timestamp is returned as a property.
        eq_(None, self.script.python_timestamp)
        eq_(None, self.script.overall_timestamp)

        # If you give the Timestamps data, suddenly they show up.
        overall.finish = self.script.parse_time('1998-08-25')
        python.finish = self.script.parse_time('1993-06-11')
        python.counter = 2
        self._db.flush()

        overall_timestamp_info = self.script.overall_timestamp
        assert isinstance(overall_timestamp_info, self.script.TimestampInfo)
        eq_(overall.finish, overall_timestamp_info.finish)

        python_timestamp_info = self.script.python_timestamp
        assert isinstance(python_timestamp_info, self.script.TimestampInfo)
        eq_(python.finish, python_timestamp_info.finish)
        eq_(2, self.script.python_timestamp.counter)

    def test_directories_by_priority(self):
        core = os.path.split(os.path.split(__file__)[0])[0]
        parent = os.path.split(core)[0]
        expected_core = os.path.join(core, 'migration')
        expected_parent = os.path.join(parent, 'migration')

        # This is the only place we're testing the real script.
        # Everywhere else should use the mock.
        script = DatabaseMigrationScript()
        eq_(
            [expected_core, expected_parent],
            script.directories_by_priority
        )

    def test_fetch_migration_files(self):
        result = self.script.fetch_migration_files()
        result_migrations, result_migrations_by_dir = result

        for migration_file in self.migration_files:
            assert os.path.split(migration_file)[1] in result_migrations

        def extract_filenames(core=True, extensions=['.py', '.sql']):
            extensions = tuple(extensions)
            if core:
                pathnames = filter(lambda p: 'core' in p, self.migration_files)
            else:
                pathnames = filter(lambda p: 'core' not in p, self.migration_files)

            return [os.path.split(p)[1] for p in pathnames if p.endswith(extensions)]

        # Ensure that all the expected migrations from CORE are included in
        # the 'core' directory array in migrations_by_directory.
        core_migration_files = extract_filenames()
        eq_(2, len(core_migration_files))
        for filename in core_migration_files:
            assert filename in result_migrations_by_dir[self.core_migration_dir]

        # Ensure that all the expected migrations from the parent server
        # are included in the appropriate array in migrations_by_directory.
        parent_migration_files = extract_filenames(core=False)
        eq_(2, len(parent_migration_files))
        for filename in parent_migration_files:
            assert filename in result_migrations_by_dir[self.parent_migration_dir]

        # When the script is python_only, only python migrations are returned.
        self.script.python_only = True
        result_migrations, result_migrations_by_dir = self.script.fetch_migration_files()

        py_migration_files = [m for m in self.migration_files if m.endswith('.py')]
        py_migration_filenames = [os.path.split(f)[1] for f in py_migration_files]
        eq_(sorted(py_migration_filenames), sorted(result_migrations))

        core_migration_files = [m for m in extract_filenames() if m.endswith('.py')]
        eq_(1, len(core_migration_files))
        eq_(result_migrations_by_dir[self.core_migration_dir], core_migration_files)

        parent_migration_files = [m for m in extract_filenames(False) if m.endswith('.py')]
        eq_(1, len(parent_migration_files))
        eq_(result_migrations_by_dir[self.parent_migration_dir], parent_migration_files)

    def test_migratable_files(self):
        """Returns migrations that end with particular extensions."""

        migrations = [
            '.gitkeep', '20250521-make-bananas.sql', '20260810-do-a-thing.py',
            '20260802-did-a-thing.pyc', 'why-am-i-here.rb'
        ]

        result = self.script.migratable_files(migrations, ['.sql', '.py'])
        eq_(2, len(result))
        eq_(['20250521-make-bananas.sql', '20260810-do-a-thing.py'], result)

        result = self.script.migratable_files(migrations, ['.rb'])
        eq_(1, len(result))
        eq_(['why-am-i-here.rb'], result)

        result = self.script.migratable_files(migrations, ['banana'])
        eq_([], result)

    def test_get_new_migrations(self):
        """Filters out migrations that were run on or before a given timestamp"""

        migrations = [
            '20271204-far-future-migration-funtime.sql',
            '20271202-future-migration-funtime.sql',
            '20271203-do-another-thing.py',
            '20250521-make-bananas.sql',
            '20260810-last-timestamp',
            '20260811-do-a-thing.py',
            '20260809-already-done.sql',
        ]

        result = self.script.get_new_migrations(self.timestamp_info, migrations)
        # Expected migrations will be sorted by timestamp. Python migrations
        # will be sorted after SQL migrations.
        expected = [
            '20271202-future-migration-funtime.sql',
            '20271204-far-future-migration-funtime.sql',
            '20260811-do-a-thing.py',
            '20271203-do-another-thing.py',
        ]

        eq_(4, len(result))
        eq_(expected, result)

        # If the timestamp has a counter, the filter only finds new migrations
        # past the counter.
        migrations = [
            '20260810-last-timestamp.sql',
            '20260810-1-do-a-thing.sql',
            '20271202-future-migration-funtime.sql',
            '20260810-2-do-all-the-things.sql',
            '20260809-already-done.sql'
        ]
        self.timestamp_info.counter = 1
        result = self.script.get_new_migrations(self.timestamp_info, migrations)
        expected = [
            '20260810-2-do-all-the-things.sql',
            '20271202-future-migration-funtime.sql',
        ]

        eq_(2, len(result))
        eq_(expected, result)

        # If the timestamp has a (unlikely) mix of counter and non-counter
        # migrations with the same datetime, migrations with counters are
        # sorted after migrations without them.
        migrations = [
            '20260810-do-a-thing.sql',
            '20271202-1-more-future-migration-funtime.sql',
            '20260810-1-do-all-the-things.sql',
            '20260809-already-done.sql',
            '20271202-future-migration-funtime.sql',
        ]
        self.timestamp_info.counter = None

        result = self.script.get_new_migrations(self.timestamp_info, migrations)
        expected = [
            '20260810-1-do-all-the-things.sql',
            '20271202-future-migration-funtime.sql',
            '20271202-1-more-future-migration-funtime.sql'
        ]
        eq_(3, len(result))
        eq_(expected, result)

    def test_update_timestamps(self):
        """Resets a timestamp according to the date of a migration file"""

        migration = '20271202-future-migration-funtime.sql'
        py_last_run_time = self.python_timestamp.finish

        def assert_unchanged_python_timestamp():
            eq_(py_last_run_time, self.python_timestamp.finish)

        def assert_timestamp_matches_migration(timestamp, migration, counter=None):
            self._db.refresh(timestamp)
            timestamp_str = timestamp.finish.strftime('%Y%m%d')
            eq_(migration[0:8], timestamp_str)
            eq_(counter, timestamp.counter)

        assert self.timestamp_info.finish.strftime('%Y%m%d') != migration[0:8]
        self.script.update_timestamps(migration)
        assert_timestamp_matches_migration(self.timestamp, migration)
        assert_unchanged_python_timestamp()

        # It also takes care of counter digits when multiple migrations
        # exist for the same date.
        migration = '20280810-2-do-all-the-things.sql'
        self.script.update_timestamps(migration)
        assert_timestamp_matches_migration(self.timestamp, migration, counter=2)
        assert_unchanged_python_timestamp()

        # And removes those counter digits when the timestamp is updated.
        migration = '20280901-what-it-do.sql'
        self.script.update_timestamps(migration)
        assert_timestamp_matches_migration(self.timestamp, migration)
        assert_unchanged_python_timestamp()

        # If the migration is earlier than the existing timestamp,
        # the timestamp is not updated.
        migration = '20280801-before-the-existing-timestamp.sql'
        self.script.update_timestamps(migration)
        eq_(self.timestamp.finish.strftime('%Y%m%d'), '20280901')

        # Python migrations update both timestamps.
        migration = '20281001-new-task.py'
        self.script.update_timestamps(migration)
        assert_timestamp_matches_migration(self.timestamp, migration)
        assert_timestamp_matches_migration(self.python_timestamp, migration)

    def test_running_a_migration_updates_the_timestamps(self):
        future_time = datetime.datetime.strptime('20261030', '%Y%m%d')
        self.timestamp_info.finish = future_time

        # Create a test migration after that point and grab relevant info
        # about it.
        self._create_test_migration_file(
            self.core_migration_dir, 'SINGLE', 'sql',
            migration_date='20261202'
        )

        # Pop the last migration filepath off and run the migration with
        # the relevant information.
        migration_filepath = self.migration_files[-1]
        migration_filename = os.path.split(migration_filepath)[1]
        migrations_by_dir = {
            self.core_migration_dir : [migration_filename],
            self.parent_migration_dir : []
        }

        # Running the migration updates the timestamps
        self.script.run_migrations(
            [migration_filename], migrations_by_dir, self.timestamp_info
        )
        eq_(self.timestamp.finish.strftime('%Y%m%d'), '20261202')

        # Even when there are counters.
        self._create_test_migration_file(
            self.core_migration_dir, 'COUNTER', 'sql',
            migration_date='20261203-3'
        )
        migration_filename = os.path.split(self.migration_files[-1])[1]
        migrations_by_dir[self.core_migration_dir] = [migration_filename]
        self.script.run_migrations(
            [migration_filename], migrations_by_dir, self.timestamp_info
        )
        eq_(self.timestamp.finish.strftime('%Y%m%d'), '20261203')
        eq_(self.timestamp.counter, 3)

    def test_all_migration_files_are_run(self):
        self.script.run(
            test_db=self._db, test=True,
            cmd_args=["--last-run-date", "2010-01-01"]
        )

        # There are two test timestamps in the database, confirming that
        # the test SQL files created by self._create_test_migration_files()
        # have been run.
        timestamps = self._db.query(Timestamp).filter(
            Timestamp.service.like('Test Database Migration Script - %')
        ).order_by(Timestamp.service).all()
        eq_(2, len(timestamps))

        # A timestamp has been generated from each migration directory.
        eq_(True, timestamps[0].service.endswith('CORE'))
        eq_(True, timestamps[1].service.endswith('SERVER'))

        for timestamp in timestamps:
            self._db.delete(timestamp)

        # There are two temporary files created in core/tests,
        # confirming that the test Python files created by
        # self._create_test_migration_files() have been run.
        test_dir = os.path.split(__file__)[0]
        all_files = os.listdir(test_dir)
        test_generated_files = sorted([f for f in all_files
                                       if f.startswith(('CORE', 'SERVER'))])
        eq_(2, len(test_generated_files))

        # A file has been generated from each migration directory.
        assert 'CORE' in test_generated_files[0]
        assert 'SERVER' in test_generated_files[1]

    def test_python_migration_files_can_be_run_independently(self):
        self.script.run(
            test_db=self._db, test=True,
            cmd_args=["--last-run-date", "2010-01-01", "--python-only"]
        )

        # There are no test timestamps in the database, confirming that
        # no test SQL files created by self._create_test_migration_files()
        # have been run.
        timestamps = self._db.query(Timestamp).filter(
            Timestamp.service.like('Test Database Migration Script - %')
        ).order_by(Timestamp.service).all()
        eq_([], timestamps)

        # There are two temporary files in core/tests, confirming that the test
        # Python files created by self._create_test_migration_files() were run.
        test_dir = os.path.split(__file__)[0]
        all_files = os.listdir(test_dir)
        test_generated_files = sorted([f for f in all_files
                                       if f.startswith(('CORE', 'SERVER'))])

        eq_(2, len(test_generated_files))

        # A file has been generated from each migration directory.
        assert 'CORE' in test_generated_files[0]
        assert 'SERVER' in test_generated_files[1]


class TestDatabaseMigrationInitializationScript(DatabaseMigrationScriptTest):

    def setup(self):
        super(TestDatabaseMigrationInitializationScript, self).setup()
        self.script = DatabaseMigrationInitializationScript(self._db)

    def assert_matches_latest_python_migration(self, timestamp, script=None):
        script = script or self.script
        migrations = script.fetch_migration_files()[0]
        migrations_sorted = script.sort_migrations(migrations)
        last_migration_date = [x for x in migrations_sorted if x.endswith('.py')][-1][0:8]
        self.assert_matches_timestamp(timestamp, last_migration_date)

    def assert_matches_latest_migration(self, timestamp, script=None):
        script = script or self.script
        migrations = script.fetch_migration_files()[0]
        migrations_sorted = script.sort_migrations(migrations)
        py_migration = [x for x in migrations_sorted if x.endswith('.py')][-1][0:8]
        sql_migration = [x for x in migrations_sorted if x.endswith('.sql')][-1][0:8]
        last_migration_date = py_migration if int(py_migration) > int(sql_migration) else sql_migration
        self.assert_matches_timestamp(timestamp, last_migration_date)

    def assert_matches_timestamp(self, timestamp, migration_date):
        eq_(timestamp.finish.strftime('%Y%m%d'), migration_date)

    def test_accurate_timestamps_created(self):
        eq_(
            None,
            Timestamp.value(
                self._db, self.script.name, Timestamp.SCRIPT_TYPE,
                collection=None
            )
        )
        self.script.run()
        self.assert_matches_latest_migration(self.script.overall_timestamp)
        self.assert_matches_latest_python_migration(self.script.python_timestamp)

    def test_accurate_python_timestamp_created_python_later(self):
        script = self.create_mock_script(DatabaseMigrationInitializationScript, self._db)
        eq_(None, Timestamp.value(self._db, script.name, Timestamp.SCRIPT_TYPE, collection=None))

        # If the last python migration and the last SQL migration have
        # different timestamps, they're set accordingly.
        self._create_test_migration_file(self.core_migration_dir, 'CORE', 'sql', '20310101')
        self._create_test_migration_file(self.parent_migration_dir, 'SERVER', 'py', '20300101')

        script.run()
        self.assert_matches_timestamp(script.overall_timestamp, '20310101')
        self.assert_matches_timestamp(script.python_timestamp, '20300101')

    def test_accurate_python_timestamp_created_python_earlier(self):
        script = self.create_mock_script(DatabaseMigrationInitializationScript, self._db)
        eq_(None, Timestamp.value(self._db, script.name, Timestamp.SCRIPT_TYPE, collection=None))

        # If the last python migration and the last SQL migration have
        # different timestamps, they're set accordingly.
        self._create_test_migration_file(self.core_migration_dir, 'CORE', 'sql', '20310101')
        self._create_test_migration_file(self.parent_migration_dir, 'SERVER', 'py', '20350101')

        script.run()
        self.assert_matches_timestamp(script.overall_timestamp, '20350101')
        self.assert_matches_timestamp(script.python_timestamp, '20350101')

    def test_error_raised_when_timestamp_exists(self):
        Timestamp.stamp(self._db, self.script.name, Timestamp.SCRIPT_TYPE, None)
        assert_raises(RuntimeError, self.script.run)

    def test_error_not_raised_when_timestamp_forced(self):
        past = self.script.parse_time('19951127')
        Timestamp.stamp(self._db, self.script.name, Timestamp.SCRIPT_TYPE, None, finish=past)
        self.script.run(['-f'])
        self.assert_matches_latest_migration(self.script.overall_timestamp)
        self.assert_matches_latest_python_migration(self.script.python_timestamp)

    def test_accepts_last_run_date(self):
        # A timestamp can be passed via the command line.
        self.script.run(['--last-run-date', '20101010'])
        expected_stamp = datetime.datetime.strptime('20101010', '%Y%m%d')
        eq_(expected_stamp, self.script.overall_timestamp.finish)

        # It will override an existing timestamp if forced.
        self.script.run(['--last-run-date', '20111111', '--force'])
        expected_stamp = datetime.datetime.strptime('20111111', '%Y%m%d')
        eq_(expected_stamp, self.script.overall_timestamp.finish)
        eq_(expected_stamp, self.script.python_timestamp.finish)

    def test_accepts_last_run_counter(self):
        # If a counter is passed without a date, an error is raised.
        assert_raises(ValueError, self.script.run, ['--last-run-counter', '7'])

        # With a date, the counter can be set.
        self.script.run(['--last-run-date', '20101010', '--last-run-counter', '7'])
        expected_stamp = datetime.datetime.strptime('20101010', '%Y%m%d')
        eq_(expected_stamp, self.script.overall_timestamp.finish)
        eq_(7, self.script.overall_timestamp.counter)

        # When forced, the counter can be reset on an existing timestamp.
        previous_timestamp = self.script.overall_timestamp.finish
        self.script.run(['--last-run-date', '20121212', '--last-run-counter', '2', '-f'])
        expected_stamp = datetime.datetime.strptime('20121212', '%Y%m%d')
        eq_(expected_stamp, self.script.overall_timestamp.finish)
        eq_(expected_stamp, self.script.python_timestamp.finish)
        eq_(2, self.script.overall_timestamp.counter)
        eq_(2, self.script.python_timestamp.counter)


class TestAddClassificationScript(DatabaseTest):

    def test_end_to_end(self):
        work = self._work(with_license_pool=True)
        identifier = work.license_pools[0].identifier
        stdin = MockStdin(identifier.identifier)
        eq_(Classifier.AUDIENCE_ADULT, work.audience)

        cmd_args = [
            "--identifier-type", identifier.type,
            "--subject-type", Classifier.FREEFORM_AUDIENCE,
            "--subject-identifier", Classifier.AUDIENCE_CHILDREN,
            "--weight", "42", '--create-subject'
        ]
        script = AddClassificationScript(
            _db=self._db, cmd_args=cmd_args, stdin=stdin
        )
        script.do_run()

        # The identifier has been classified under 'children'.
        [classification] = identifier.classifications
        eq_(42, classification.weight)
        subject = classification.subject
        eq_(Classifier.FREEFORM_AUDIENCE, subject.type)
        eq_(Classifier.AUDIENCE_CHILDREN, subject.identifier)

        # The work has been reclassified and is now known as a
        # children's book.
        eq_(Classifier.AUDIENCE_CHILDREN, work.audience)

    def test_autocreate(self):
        work = self._work(with_license_pool=True)
        identifier = work.license_pools[0].identifier
        stdin = MockStdin(identifier.identifier)
        eq_(Classifier.AUDIENCE_ADULT, work.audience)

        cmd_args = [
            "--identifier-type", identifier.type,
            "--subject-type", Classifier.TAG,
            "--subject-identifier", "some random tag"
        ]
        script = AddClassificationScript(
            _db=self._db, cmd_args=cmd_args, stdin=stdin
        )
        script.do_run()

        # Nothing has happened. There was no Subject with that
        # identifier, so we assumed there was a typo and did nothing.
        eq_([], identifier.classifications)

        # If we stick the 'create-subject' onto the end of the
        # command-line arguments, the Subject is created and the
        # classification happens.
        stdin = MockStdin(identifier.identifier)
        cmd_args.append('--create-subject')
        script = AddClassificationScript(
            _db=self._db, cmd_args=cmd_args, stdin=stdin
        )
        script.do_run()

        [classification] = identifier.classifications
        subject = classification.subject
        eq_("some random tag", subject.identifier)


class TestShowLibrariesScript(DatabaseTest):

    def test_with_no_libraries(self):
        output = StringIO()
        ShowLibrariesScript().do_run(self._db, output=output)
        eq_("No libraries found.\n", output.getvalue())

    def test_with_multiple_libraries(self):
        l1, ignore = create(
            self._db, Library, name="Library 1", short_name="L1",
        )
        l1.library_registry_shared_secret="a"
        l2, ignore = create(
            self._db, Library, name="Library 2", short_name="L2",
        )
        l2.library_registry_shared_secret="b"

        # The output of this script is the result of running explain()
        # on both libraries.
        output = StringIO()
        ShowLibrariesScript().do_run(self._db, output=output)
        expect_1 = "\n".join(l1.explain(include_secrets=False))
        expect_2 = "\n".join(l2.explain(include_secrets=False))

        eq_(expect_1 + "\n" + expect_2 + "\n", output.getvalue())


        # We can tell the script to only list a single library.
        output = StringIO()
        ShowLibrariesScript().do_run(
            self._db,
            cmd_args=["--short-name=L2"],
            output=output
        )
        eq_(expect_2 + "\n", output.getvalue())

        # We can tell the script to include the library registry
        # shared secret.
        output = StringIO()
        ShowLibrariesScript().do_run(
            self._db,
            cmd_args=["--show-secrets"],
            output=output
        )
        expect_1 = "\n".join(l1.explain(include_secrets=True))
        expect_2 = "\n".join(l2.explain(include_secrets=True))
        eq_(expect_1 + "\n" + expect_2 + "\n", output.getvalue())


class TestConfigureSiteScript(DatabaseTest):

    def test_unknown_setting(self):
        script = ConfigureSiteScript()
        assert_raises_regexp(
            ValueError,
            "'setting1' is not a known site-wide setting. Use --force to set it anyway.",
            script.do_run, self._db, [
                "--setting=setting1=value1"
            ]
        )

        eq_(None, ConfigurationSetting.sitewide(self._db, "setting1").value)

        # Running with --force sets the setting.
        script.do_run(
            self._db, [
                "--setting=setting1=value1",
                "--force",
            ]
        )

        eq_("value1", ConfigurationSetting.sitewide(self._db, "setting1").value)

    def test_settings(self):
        class TestConfig(object):
            SITEWIDE_SETTINGS = [
                { "key": "setting1" },
                { "key": "setting2" },
                { "key": "setting_secret" },
            ]

        script = ConfigureSiteScript(config=TestConfig)
        output = StringIO()
        script.do_run(
            self._db, [
                "--setting=setting1=value1",
                "--setting=setting2=[1,2,\"3\"]",
                "--setting=setting_secret=secretvalue",
            ],
            output
        )
        # The secret was set, but is not shown.
        expect = "\n".join(
            ConfigurationSetting.explain(self._db, include_secrets=False)
        )
        eq_(expect, output.getvalue())
        assert 'setting_secret' not in expect
        eq_("value1", ConfigurationSetting.sitewide(self._db, "setting1").value)
        eq_('[1,2,"3"]', ConfigurationSetting.sitewide(self._db, "setting2").value)
        eq_("secretvalue", ConfigurationSetting.sitewide(self._db, "setting_secret").value)

        # If we run again with --show-secrets, the secret is shown.
        output = StringIO()
        script.do_run(self._db, ["--show-secrets"], output)
        expect = "\n".join(
            ConfigurationSetting.explain(self._db, include_secrets=True)
        )
        eq_(expect, output.getvalue())
        assert 'setting_secret' in expect

class TestConfigureLibraryScript(DatabaseTest):

    def test_bad_arguments(self):
        script = ConfigureLibraryScript()
        library, ignore = create(
            self._db, Library, name="Library 1", short_name="L1",
        )
        library.library_registry_shared_secret='secret'
        self._db.commit()
        assert_raises_regexp(
            ValueError,
            "You must identify the library by its short name.",
            script.do_run, self._db, []
        )

        assert_raises_regexp(
            ValueError,
            "Could not locate library 'foo'",
            script.do_run, self._db, ["--short-name=foo"]
        )

    def test_create_library(self):
        # There is no library.
        eq_([], self._db.query(Library).all())

        script = ConfigureLibraryScript()
        output = StringIO()
        script.do_run(
            self._db, [
                "--short-name=L1",
                "--name=Library 1",
                '--setting=customkey=value',
            ],
            output
        )

        # Now there is one library.
        [library] = self._db.query(Library).all()
        eq_("Library 1", library.name)
        eq_("L1", library.short_name)
        eq_("value", library.setting("customkey").value)
        expect_output = "Configuration settings stored.\n" + "\n".join(library.explain()) + "\n"
        eq_(expect_output, output.getvalue())

    def test_reconfigure_library(self):
        # The library exists.
        library, ignore = create(
            self._db, Library, name="Library 1", short_name="L1",
        )
        script = ConfigureLibraryScript()
        output = StringIO()

        # We're going to change one value and add a setting.
        script.do_run(
            self._db, [
                "--short-name=L1",
                "--name=Library 1 New Name",
                '--setting=customkey=value',
            ],
            output
        )

        eq_("Library 1 New Name", library.name)
        eq_("value", library.setting("customkey").value)

        expect_output = "Configuration settings stored.\n" + "\n".join(library.explain()) + "\n"
        eq_(expect_output, output.getvalue())


class TestShowCollectionsScript(DatabaseTest):

    def test_with_no_collections(self):
        output = StringIO()
        ShowCollectionsScript().do_run(self._db, output=output)
        eq_("No collections found.\n", output.getvalue())

    def test_with_multiple_collections(self):
        c1 = self._collection(name="Collection 1",
                              protocol=ExternalIntegration.OVERDRIVE)
        c1.collection_password="a"
        c2 = self._collection(name="Collection 2",
                              protocol=ExternalIntegration.BIBLIOTHECA)
        c2.collection_password="b"

        # The output of this script is the result of running explain()
        # on both collections.
        output = StringIO()
        ShowCollectionsScript().do_run(self._db, output=output)
        expect_1 = "\n".join(c1.explain(include_secrets=False))
        expect_2 = "\n".join(c2.explain(include_secrets=False))

        eq_(expect_1 + "\n" + expect_2 + "\n", output.getvalue())


        # We can tell the script to only list a single collection.
        output = StringIO()
        ShowCollectionsScript().do_run(
            self._db,
            cmd_args=["--name=Collection 2"],
            output=output
        )
        eq_(expect_2 + "\n", output.getvalue())

        # We can tell the script to include the collection password
        output = StringIO()
        ShowCollectionsScript().do_run(
            self._db,
            cmd_args=["--show-secrets"],
            output=output
        )
        expect_1 = "\n".join(c1.explain(include_secrets=True))
        expect_2 = "\n".join(c2.explain(include_secrets=True))
        eq_(expect_1 + "\n" + expect_2 + "\n", output.getvalue())


class TestConfigureCollectionScript(DatabaseTest):

    def test_bad_arguments(self):
        script = ConfigureCollectionScript()
        library, ignore = create(
            self._db, Library, name="Library 1", short_name="L1",
        )
        self._db.commit()

        # Reference to a nonexistent collection without the information
        # necessary to create it.
        assert_raises_regexp(
            ValueError,
            'No collection called "collection". You can create it, but you must specify a protocol.',
            script.do_run, self._db, ["--name=collection"]
        )

        # Incorrect format for the 'setting' argument.
        assert_raises_regexp(
            ValueError,
            'Incorrect format for setting: "key". Should be "key=value"',
            script.do_run, self._db, [
                "--name=collection", "--protocol=Overdrive",
                "--setting=key"
            ]
        )

        # Try to add the collection to a nonexistent library.
        assert_raises_regexp(
            ValueError,
            'No such library: "nosuchlibrary". I only know about: "L1"',
            script.do_run, self._db, [
                "--name=collection", "--protocol=Overdrive",
                "--library=nosuchlibrary"
            ]
        )


    def test_success(self):

        script = ConfigureCollectionScript()
        l1, ignore = create(
            self._db, Library, name="Library 1", short_name="L1",
        )
        l2, ignore = create(
            self._db, Library, name="Library 2", short_name="L2",
        )
        l3, ignore = create(
            self._db, Library, name="Library 3", short_name="L3",
        )
        self._db.commit()

        # Create a collection, set all its attributes, set a custom
        # setting, and associate it with two libraries.
        output = StringIO()
        script.do_run(
            self._db, ["--name=New Collection", "--protocol=Overdrive",
                       "--library=L2", "--library=L1",
                       "--setting=library_id=1234",
                       "--external-account-id=acctid",
                       "--url=url",
                       "--username=username",
                       "--password=password",
            ], output
        )

        # The collection was created and configured properly.
        collection = get_one(self._db, Collection)
        eq_("New Collection", collection.name)
        eq_("url", collection.external_integration.url)
        eq_("acctid", collection.external_account_id)
        eq_("username", collection.external_integration.username)
        eq_("password", collection.external_integration.password)

        # Two libraries now have access to the collection.
        eq_([collection], l1.collections)
        eq_([collection], l2.collections)
        eq_([], l3.collections)

        # One CollectionSetting was set on the collection, in addition
        # to url, username, and password.
        setting = collection.external_integration.setting("library_id")
        eq_("library_id", setting.key)
        eq_("1234", setting.value)

        # The output explains the collection settings.
        expect = ("Configuration settings stored.\n"
                  + "\n".join(collection.explain()) + "\n")
        eq_(expect, output.getvalue())

    def test_reconfigure_collection(self):
        # The collection exists.
        collection = self._collection(
            name="Collection 1",
            protocol=ExternalIntegration.OVERDRIVE
        )
        script = ConfigureCollectionScript()
        output = StringIO()

        # We're going to change one value and add a new one.
        script.do_run(
            self._db, [
                "--name=Collection 1",
                "--url=foo",
                "--protocol=%s" % ExternalIntegration.BIBLIOTHECA
            ],
            output
        )

        # The collection has been changed.
        eq_("foo", collection.external_integration.url)
        eq_(ExternalIntegration.BIBLIOTHECA, collection.protocol)

        expect = ("Configuration settings stored.\n"
                  + "\n".join(collection.explain()) + "\n")

        eq_(expect, output.getvalue())


class TestShowIntegrationsScript(DatabaseTest):

    def test_with_no_integrations(self):
        output = StringIO()
        ShowIntegrationsScript().do_run(self._db, output=output)
        eq_("No integrations found.\n", output.getvalue())

    def test_with_multiple_integrations(self):
        i1 = self._external_integration(
            name="Integration 1",
            goal="Goal",
            protocol=ExternalIntegration.OVERDRIVE
        )
        i1.password="a"
        i2 = self._external_integration(
            name="Integration 2",
            goal="Goal",
            protocol=ExternalIntegration.BIBLIOTHECA
        )
        i2.password="b"

        # The output of this script is the result of running explain()
        # on both integrations.
        output = StringIO()
        ShowIntegrationsScript().do_run(self._db, output=output)
        expect_1 = "\n".join(i1.explain(include_secrets=False))
        expect_2 = "\n".join(i2.explain(include_secrets=False))

        eq_(expect_1 + "\n" + expect_2 + "\n", output.getvalue())


        # We can tell the script to only list a single integration.
        output = StringIO()
        ShowIntegrationsScript().do_run(
            self._db,
            cmd_args=["--name=Integration 2"],
            output=output
        )
        eq_(expect_2 + "\n", output.getvalue())

        # We can tell the script to include the integration secrets
        output = StringIO()
        ShowIntegrationsScript().do_run(
            self._db,
            cmd_args=["--show-secrets"],
            output=output
        )
        expect_1 = "\n".join(i1.explain(include_secrets=True))
        expect_2 = "\n".join(i2.explain(include_secrets=True))
        eq_(expect_1 + "\n" + expect_2 + "\n", output.getvalue())


class TestConfigureIntegrationScript(DatabaseTest):

    def test_load_integration(self):
        m = ConfigureIntegrationScript._integration

        assert_raises_regexp(
            ValueError,
            "An integration must by identified by either ID, name, or the combination of protocol and goal.",
            m, self._db, None, None, "protocol", None
        )

        assert_raises_regexp(
            ValueError,
            "No integration with ID notanid.",
            m, self._db, "notanid", None, None, None
        )

        assert_raises_regexp(
            ValueError,
            'No integration with name "Unknown integration". To create it, you must also provide protocol and goal.',
            m, self._db, None, "Unknown integration", None, None
        )

        integration = self._external_integration(
            protocol="Protocol", goal="Goal"
        )
        integration.name = "An integration"
        eq_(integration,
            m(self._db, integration.id, None, None, None)
        )

        eq_(integration,
            m(self._db, None, integration.name, None, None)
        )

        eq_(integration,
            m(self._db, None, None, "Protocol", "Goal")
        )

        # An integration may be created given a protocol and goal.
        integration2 = m(self._db, None, "I exist now", "Protocol", "Goal2")
        assert integration2 != integration
        eq_("Protocol", integration2.protocol)
        eq_("Goal2", integration2.goal)
        eq_("I exist now", integration2.name)

    def test_add_settings(self):
        script = ConfigureIntegrationScript()
        output = StringIO()

        script.do_run(
            self._db, [
                "--protocol=aprotocol",
                "--goal=agoal",
                "--setting=akey=avalue",
            ],
            output
        )

        # An ExternalIntegration was created and configured.
        integration = get_one(self._db, ExternalIntegration,
                              protocol="aprotocol", goal="agoal")

        expect_output = "Configuration settings stored.\n" + "\n".join(integration.explain()) + "\n"
        eq_(expect_output, output.getvalue())

class TestShowLanesScript(DatabaseTest):

    def test_with_no_lanes(self):
        output = StringIO()
        ShowLanesScript().do_run(self._db, output=output)
        eq_("No lanes found.\n", output.getvalue())

    def test_with_multiple_lanes(self):
        l1 = self._lane()
        l2 = self._lane()

        # The output of this script is the result of running explain()
        # on both lanes.
        output = StringIO()
        ShowLanesScript().do_run(self._db, output=output)
        expect_1 = "\n".join(l1.explain())
        expect_2 = "\n".join(l2.explain())

        eq_(expect_1 + "\n\n" + expect_2 + "\n\n", output.getvalue())

        # We can tell the script to only list a single lane.
        output = StringIO()
        ShowLanesScript().do_run(
            self._db,
            cmd_args=["--id=%s" % l2.id],
            output=output
        )
        eq_(expect_2 + "\n\n", output.getvalue())

class TestConfigureLaneScript(DatabaseTest):

    def test_bad_arguments(self):
        script = ConfigureLaneScript()

        # No lane id but no library short name for creating it either.
        assert_raises_regexp(
            ValueError,
            'Library short name is required to create a new lane',
            script.do_run, self._db, []
        )

        # Try to create a lane for a nonexistent library.
        assert_raises_regexp(
            ValueError,
            'No such library: "nosuchlibrary".',
            script.do_run, self._db, [
                "--library-short-name=nosuchlibrary"
            ]
        )


    def test_create_lane(self):
        script = ConfigureLaneScript()
        parent = self._lane()

        # Create a lane and set its attributes.
        output = StringIO()
        script.do_run(
            self._db, ["--library-short-name=%s" % self._default_library.short_name,
                       "--parent-id=%s" % parent.id,
                       "--priority=3",
                       "--display-name=NewLane",
            ], output
        )

        # The lane was created and configured properly.
        lane = get_one(self._db, Lane, display_name="NewLane")
        eq_(self._default_library, lane.library)
        eq_(parent, lane.parent)
        eq_(3, lane.priority)

        # The output explains the lane settings.
        expect = ("Lane settings stored.\n"
                  + "\n".join(lane.explain()) + "\n")
        eq_(expect, output.getvalue())

    def test_reconfigure_lane(self):
        # The lane exists.
        lane = self._lane(display_name="Name")
        lane.priority = 3

        parent = self._lane()

        script = ConfigureLaneScript()
        output = StringIO()

        script.do_run(
            self._db, [
                "--id=%s" % lane.id,
                "--priority=1",
                "--parent-id=%s" % parent.id,
            ],
            output
        )

        # The lane has been changed.
        eq_(1, lane.priority)
        eq_(parent, lane.parent)
        expect = ("Lane settings stored.\n"
                  + "\n".join(lane.explain()) + "\n")

        eq_(expect, output.getvalue())


class TestCollectionInputScript(DatabaseTest):
    """Test the ability to name collections on the command line."""

    def test_parse_command_line(self):

        def collections(cmd_args):
            parsed = CollectionInputScript.parse_command_line(
                self._db, cmd_args
            )
            return parsed.collections

        # No collections named on command line -> no collections
        eq_([], collections([]))

        # Nonexistent collection -> ValueError
        assert_raises_regexp(
            ValueError,
            'Unknown collection: "no such collection"',
            collections, ['--collection="no such collection"']
        )

        # Collections are presented in the order they were encountered
        # on the command line.
        c2 = self._collection()
        expect = [c2, self._default_collection]
        args = ["--collection=" + c.name for c in expect]
        actual = collections(args)
        eq_(expect, actual)


# Mock classes used by TestOPDSImportScript
class MockOPDSImportMonitor(object):
    """Pretend to monitor an OPDS feed for new titles."""
    INSTANCES = []

    def __init__(self, _db, collection, *args, **kwargs):
        self.collection = collection
        self.args = args
        self.kwargs = kwargs
        self.INSTANCES.append(self)
        self.was_run = False

    def run(self):
        self.was_run = True

class MockOPDSImporter(object):
    """Pretend to import titles from an OPDS feed."""
    pass

class MockOPDSImportScript(OPDSImportScript):
    """Actually instantiate a monitor that will pretend to do something."""
    MONITOR_CLASS = MockOPDSImportMonitor
    IMPORTER_CLASS = MockOPDSImporter


class TestOPDSImportScript(DatabaseTest):

    def test_do_run(self):
        self._default_collection.external_integration.setting(Collection.DATA_SOURCE_NAME_SETTING).value = (
            DataSource.OA_CONTENT_SERVER
        )

        script = MockOPDSImportScript(self._db)
        script.do_run([])

        # Since we provided no collection, a MockOPDSImportMonitor
        # was instantiated for each OPDS Import collection in the database.
        monitor = MockOPDSImportMonitor.INSTANCES.pop()
        eq_(self._default_collection, monitor.collection)

        args = ['--collection=%s' % self._default_collection.name]
        script.do_run(args)

        # If we provide the collection name, a MockOPDSImportMonitor is
        # also instantiated.
        monitor = MockOPDSImportMonitor.INSTANCES.pop()
        eq_(self._default_collection, monitor.collection)
        eq_(True, monitor.was_run)

        # Our replacement OPDS importer class was passed in to the
        # monitor constructor. If this had been a real monitor, that's the
        # code we would have used to import OPDS feeds.
        eq_(MockOPDSImporter, monitor.kwargs['import_class'])
        eq_(False, monitor.kwargs['force_reimport'])

        # Setting --force changes the 'force_reimport' argument
        # passed to the monitor constructor.
        args.append('--force')
        script.do_run(args)
        monitor = MockOPDSImportMonitor.INSTANCES.pop()
        eq_(self._default_collection, monitor.collection)
        eq_(True, monitor.kwargs['force_reimport'])


class MockWhereAreMyBooks(WhereAreMyBooksScript):
    """A mock script that keeps track of its output in an easy-to-test
    form, so we don't have to mess around with StringIO.
    """
    def __init__(self, _db=None, output=None, search=None):
        # In most cases a list will do fine for `output`.
        output = output or []

        # In most tests an empty mock will do for `search`.
        search = search or MockExternalSearchIndex()

        super(MockWhereAreMyBooks, self).__init__(_db, output, search)
        self.output = []

    def out(self, s, *args):
        if args:
            self.output.append((s, list(args)))
        else:
            self.output.append(s)


class TestWhereAreMyBooksScript(DatabaseTest):

    def test_no_search_integration(self):
        # We can't even get started without a working search integration.

        # We'll also test the out() method by mocking the script's
        # standard output and using the normal out() implementation.
        # In other tests, which have more complicated output, we mock
        # out(), so this verifies that output actually gets written
        # out.
        output = StringIO()
        assert_raises(CannotLoadConfiguration, WhereAreMyBooksScript,
                      self._db, output=output)
        eq_(
            "Here's your problem: the search integration is missing or misconfigured.\n",
            output.getvalue()
        )

    def test_overall_structure(self):
        # Verify that run() calls the methods we expect.

        class Mock(MockWhereAreMyBooks):
            """Used to verify that the correct methods are called."""
            def __init__(self, *args, **kwargs):
                super(Mock, self).__init__(*args, **kwargs)
                self.delete_cached_feeds_called = False
                self.checked_libraries = []
                self.explained_collections = []

            def check_library(self, library):
                self.checked_libraries.append(library)

            def delete_cached_feeds(self):
                self.delete_cached_feeds_called = True

            def explain_collection(self, collection):
                self.explained_collections.append(collection)

        # If there are no libraries in the system, that's a big problem.
        script = Mock(self._db)
        script.run()
        eq_(["There are no libraries in the system -- that's a problem.", "\n"],
            script.output)

        # We still run the other checks, though.
        eq_(True, script.delete_cached_feeds_called)

        # Make some libraries and some collections, and try again.
        library1 = self._default_library
        library2 = self._library()

        collection1 = self._default_collection
        collection2 = self._collection()

        script = Mock(self._db)
        script.run()

        # Every library in the collection was checked.
        eq_(set([library1, library2]), set(script.checked_libraries))

        # delete_cached_feeds() was called.
        eq_(True, script.delete_cached_feeds_called)

        # Every collection in the database was explained.
        eq_(set([collection1, collection2]),
            set(script.explained_collections))

        # There only output were the newlines after the five method
        # calls. All other output happened inside the methods we
        # mocked.
        eq_(["\n"] * 5, script.output)

        # Finally, verify the ability to use the command line to limit
        # the check to specific collections. (This isn't terribly useful
        # since checks now run very quickly.)
        script = Mock(self._db)
        script.run(cmd_args=["--collection=%s" % collection2.name])
        eq_([collection2], script.explained_collections)

    def test_check_library(self):
        # Give the default library a collection and a lane.
        library = self._default_library
        collection = self._default_collection
        lane = self._lane(library=library)

        script = MockWhereAreMyBooks(self._db)
        script.check_library(library)

        checking, has_collection, has_lanes = script.output
        eq_(('Checking library %s', [library.name]), checking)
        eq_((' Associated with collection %s.', [collection.name]),
            has_collection)
        eq_((' Associated with %s lanes.', [1]), has_lanes)

        # This library has no collections and no lanes.
        library2 = self._library()
        script.output = []
        script.check_library(library2)
        checking, no_collection, no_lanes = script.output
        eq_(('Checking library %s', [library2.name]), checking)
        eq_(" This library has no collections -- that's a problem.",
            no_collection)
        eq_(" This library has no lanes -- that's a problem.",
            no_lanes)

    def test_delete_cached_feeds(self):
        groups = CachedFeed(type=CachedFeed.GROUPS_TYPE, pagination="")
        self._db.add(groups)
        not_groups = CachedFeed(type=CachedFeed.PAGE_TYPE, pagination="")
        self._db.add(not_groups)

        eq_(2, self._db.query(CachedFeed).count())

        script = MockWhereAreMyBooks(self._db)
        script.delete_cached_feeds()
        how_many, theyre_gone = script.output
        eq_(('%d feeds in cachedfeeds table, not counting grouped feeds.', [1]),
            how_many)
        eq_(" Deleting them all.", theyre_gone)

        # Call it again, and we don't see "Deleting them all". There aren't
        # any to delete.
        script.output = []
        script.delete_cached_feeds()
        [how_many] = script.output
        eq_(('%d feeds in cachedfeeds table, not counting grouped feeds.', [0]),
            how_many)

    def check_explanation(
        self, presentation_ready=1, not_presentation_ready=0,
        no_delivery_mechanisms=0, suppressed=0, not_owned=0,
        in_search_index=0, **kwargs
    ):
        """Runs explain_collection() and verifies expected output."""
        script = MockWhereAreMyBooks(self._db, **kwargs)
        script.explain_collection(self._default_collection)
        out = script.output

        # This always happens.
        eq_(('Examining collection "%s"', [self._default_collection.name]),
            out.pop(0))
        eq_((' %d presentation-ready works.', [presentation_ready]),
            out.pop(0))
        eq_((' %d works not presentation-ready.', [not_presentation_ready]),
            out.pop(0))

        # These totals are only given if the numbers are nonzero.
        #
        if no_delivery_mechanisms:
            eq_(
                (" %d works are missing delivery mechanisms and won't show up.", [no_delivery_mechanisms]),
                out.pop(0)
            )

        if suppressed:
            eq_(
                (" %d works have suppressed LicensePools and won't show up.",
                 [suppressed]),
                out.pop(0)
            )

        if not_owned:
            eq_(
                (" %d non-open-access works have no owned licenses and won't show up.",
                 [not_owned]
                ),
                out.pop(0)
            )

        # Search engine statistics are always shown.
        eq_(
            (" %d works in the search index, expected around %d.",
             [in_search_index, presentation_ready]),
            out.pop(0)
        )

    def test_no_presentation_ready_works(self):
        # This work is not presentation-ready.
        work = self._work(with_license_pool=True)
        work.presentation_ready=False
        script = MockWhereAreMyBooks(self._db)
        self.check_explanation(presentation_ready=0, not_presentation_ready=1)

    def test_no_delivery_mechanisms(self):
        # This work has a license pool, but no delivery mechanisms.
        work = self._work(with_license_pool=True)
        for lpdm in work.license_pools[0].delivery_mechanisms:
            self._db.delete(lpdm)
        self.check_explanation(no_delivery_mechanisms=1)

    def test_suppressed_pool(self):
        # This work has a license pool, but it's suppressed.
        work = self._work(with_license_pool=True)
        work.license_pools[0].suppressed = True
        self.check_explanation(suppressed=1)

    def test_no_licenses(self):
        # This work has a license pool, but no licenses owned.
        work = self._work(with_license_pool=True)
        work.license_pools[0].licenses_owned = 0
        self.check_explanation(not_owned=1)

    def test_search_engine(self):
        output = StringIO()
        search = MockExternalSearchIndex()
        work = self._work(with_license_pool=True)
        work.presentation_ready = True
        search.bulk_update([work])

        # This MockExternalSearchIndex will always claim there is one
        # result.
        self.check_explanation(search=search, in_search_index=1)


class TestExplain(DatabaseTest):

    def test_explain(self):
        """Make sure the Explain script runs without crashing."""
        work = self._work(with_license_pool=True, genre="Science Fiction")
        [pool] = work.license_pools
        edition = work.presentation_edition
        identifier = pool.identifier
        source = DataSource.lookup(self._db, DataSource.OCLC_LINKED_DATA)
        CoverageRecord.add_for(identifier, source, "an operation")
        input = StringIO()
        output = StringIO()
        args = ["--identifier-type", "Database ID", str(identifier.id)]
        Explain(self._db).do_run(cmd_args=args, stdin=input, stdout=output)
        output = output.getvalue()

        # The script ran. Spot-check that it provided various
        # information about the work, without testing the exact
        # output.
        assert pool.collection.name in output
        assert "Available to libraries: default" in output
        assert work.title in output
        assert "Science Fiction" in output
        for contributor in edition.contributors:
            assert contributor.sort_name in output

        # CoverageRecords associated with the primary identifier were
        # printed out.
        assert 'OCLC Linked Data | an operation | success' in output

        # WorkCoverageRecords associated with the work were
        # printed out.
        assert 'generate-opds | success' in output

        # There is an active LicensePool that is fulfillable and has
        # copies owned.
        assert "%s owned" % pool.licenses_owned in output
        assert "Fulfillable" in output
        assert "ACTIVE" in output

class TestReclassifyWorksForUncheckedSubjectsScript(DatabaseTest):

    def test_constructor(self):
        """Make sure that we're only going to classify works
        with unchecked subjects.
        """
        script = ReclassifyWorksForUncheckedSubjectsScript(self._db)
        eq_(WorkClassificationScript.policy,
            ReclassifyWorksForUncheckedSubjectsScript.policy)
        eq_(100, script.batch_size)
        eq_(dump_query(Work.for_unchecked_subjects(self._db)),
            dump_query(script.query))


class TestListCollectionMetadataIdentifiersScript(DatabaseTest):

    def test_do_run(self):
        output = StringIO()
        script = ListCollectionMetadataIdentifiersScript(
            _db=self._db, output=output
        )

        # Create two collections.
        c1 = self._collection(external_account_id=self._url)
        c2 = self._collection(
            name='Local Over', protocol=ExternalIntegration.OVERDRIVE,
            external_account_id='banana'
        )

        script.do_run()

        def expected(c):
            return '(%s) %s/%s => %s\n' % (
                unicode(c.id), c.name, c.protocol, c.metadata_identifier
            )

        # In the output, there's a header, a line describing the format,
        # metdata identifiers for each collection, and a count of the
        # collections found.
        output = output.getvalue()
        assert 'COLLECTIONS' in output
        assert '(id) name/protocol => metadata_identifier\n' in output
        assert expected(c1) in output
        assert expected(c2) in output
        assert '2 collections found.\n' in output


class TestMirrorResourcesScript(DatabaseTest):
    def test_do_run(self):

        has_uploader = self._collection()
        mock_uploader = object()

        class Mock(MirrorResourcesScript):

            processed = []

            def collections_with_uploader(self, collections, collection_type):
                # Pretend that `has_uploader` is the only Collection
                # with an uploader.
                for collection in collections:
                    if collection == has_uploader:
                        yield collection, mock_uploader

            def process_collection(self, collection, policy):
                self.processed.append((collection, policy))

        script = Mock(self._db)

        # If there are no command-line arguments, process_collection
        # is called on every Collection in the system that is okayed
        # by collections_with_uploader.
        script.do_run(cmd_args=[])
        processed = script.processed.pop()
        eq_((has_uploader, mock_uploader), processed)
        eq_([], script.processed)

        # If a Collection is named on the command line,
        # process_collection is called on that Collection _if_ it has
        # an uploader.
        args = ["--collection=%s" % self._default_collection.name]
        script.do_run(cmd_args=args)
        eq_([], script.processed)

        script.do_run(cmd_args=["--collection=%s" % has_uploader.name])
        processed = script.processed.pop()
        eq_((has_uploader, mock_uploader), processed)

    @parameterized.expand([
        (
            'containing_open_access_books_with_s3_uploader',
            CollectionType.OPEN_ACCESS,
            ExternalIntegrationLink.OPEN_ACCESS_BOOKS,
            ExternalIntegration.S3,
            S3Uploader
        ),
        (
            'containing_protected_access_books_with_s3_uploader',
            CollectionType.PROTECTED_ACCESS,
            ExternalIntegrationLink.PROTECTED_ACCESS_BOOKS,
            ExternalIntegration.S3,
            S3Uploader
        ),
        (
            'containing_open_access_books_with_minio_uploader',
            CollectionType.OPEN_ACCESS,
            ExternalIntegrationLink.OPEN_ACCESS_BOOKS,
            ExternalIntegration.MINIO,
            MinIOUploader,
            {MinIOUploader.ENDPOINT_URL: 'http://localhost'}
        ),
        (
            'containing_protected_access_books_with_minio_uploader',
            CollectionType.PROTECTED_ACCESS,
            ExternalIntegrationLink.PROTECTED_ACCESS_BOOKS,
            ExternalIntegration.MINIO,
            MinIOUploader,
            {MinIOUploader.ENDPOINT_URL: 'http://localhost'}
        )
    ])
    def test_collections(self, name, collection_type, book_mirror_type, protocol, uploader_class, settings=None):
        class Mock(MirrorResourcesScript):

            mock_policy = object()

            @classmethod
            def replacement_policy(cls, uploader):
                cls.replacement_policy_called_with = uploader
                return cls.mock_policy

        script = Mock()

        # The default collection does not have an uploader.
        # This new collection does.
        has_uploader = self._collection()
        mirror = self._external_integration(
            protocol, ExternalIntegration.STORAGE_GOAL
        )

        if settings:
            for key, value in settings.iteritems():
                mirror.setting(key).value = value

        integration_link = self._external_integration_link(
            integration=has_uploader._external_integration,
            other_integration=mirror,
            purpose=ExternalIntegrationLink.COVERS
        )

        # Calling collections_with_uploader will do nothing for collections
        # that don't have an uploader. It will make a MirrorUploader for
        # the other collection, pass it into replacement_policy,
        # and yield the result.
        result = script.collections_with_uploader(
            [self._default_collection, has_uploader, self._default_collection],
            collection_type
        )

        [(collection, policy)] = result
        eq_(has_uploader, collection)
        eq_(Mock.mock_policy, policy)
        # The mirror uploader was associated with a purpose of "covers", so we only
        # expect to have one MirrorUploader.
        eq_(Mock.replacement_policy_called_with[book_mirror_type], None)
        assert isinstance(
            Mock.replacement_policy_called_with[ExternalIntegrationLink.COVERS], MirrorUploader
        )

        # Add another storage for books.
        another_mirror = self._external_integration(
            protocol, ExternalIntegration.STORAGE_GOAL
        )

        integration_link = self._external_integration_link(
            integration=has_uploader._external_integration,
            other_integration=another_mirror,
            purpose=book_mirror_type
        )

        result = script.collections_with_uploader(
            [self._default_collection, has_uploader, self._default_collection],
            collection_type
        )

        [(collection, policy)] = result
        eq_(has_uploader, collection)
        eq_(Mock.mock_policy, policy)
        # There should be two MirrorUploaders, one for each purpose.
        assert isinstance(Mock.replacement_policy_called_with[ExternalIntegrationLink.COVERS], uploader_class)
        assert isinstance(
            Mock.replacement_policy_called_with[book_mirror_type], uploader_class)

    def test_replacement_policy(self):
        uploader = object()
        p = MirrorResourcesScript.replacement_policy(uploader)
        eq_(uploader, p.mirrors)
        eq_(True, p.link_content)
        eq_(True, p.even_if_not_apparently_updated)
        eq_(False, p.rights)

    def test_process_collection(self):

        class MockScript(MirrorResourcesScript):
            process_item_called_with = []
            def process_item(self, collection, link, policy):
                self.process_item_called_with.append((collection, link, policy))

        # Mock the Hyperlink.unmirrored method
        link1 = object()
        link2 = object()
        def unmirrored(collection):
            eq_(collection, self._default_collection)
            yield link1
            yield link2

        script = MockScript(self._db)
        policy = object()
        script.process_collection(self._default_collection, policy, unmirrored)

        # Process_collection called unmirrored() and then called process_item
        # on every item yielded by unmirrored()
        call1, call2 = script.process_item_called_with
        eq_((self._default_collection, link1, policy), call1)
        eq_((self._default_collection, link2, policy), call2)

    def test_derive_rights_status(self):
        """Test our ability to determine the rights status of a Resource,
        in the absence of immediate information from the server.
        """
        m = MirrorResourcesScript.derive_rights_status
        work = self._work(with_open_access_download=True)
        [pool] = work.license_pools
        [lpdm] = pool.delivery_mechanisms
        resource = lpdm.resource

        expect = lpdm.rights_status.uri

        # Given the LicensePool, we can figure out the Resource's
        # rights status based on what was previously recovered. This lets
        # us know whether it's okay to mirror that Resource.
        eq_(expect, m(pool, resource))

        # In theory, a Resource can be associated with several
        # LicensePoolDeliveryMechanisms. That's why a LicensePool is
        # necessary -- to see which LicensePoolDeliveryMechanism we're
        # looking at.
        eq_(None, m(None, resource))

        # If there's no Resource-specific information, but a
        # LicensePool has only one rights URI among all of its
        # LicensePoolDeliveryMechanisms, then we can assume all Resources
        # for that LicensePool use that same set of rights.
        w2 = self._work(with_license_pool=True)
        [pool2] = w2.license_pools
        eq_(pool2.delivery_mechanisms[0].rights_status.uri, m(pool2, None))

        # If there's more than one possibility, or the LicensePool has
        # no LicensePoolDeliveryMechanisms at all, then we just don't
        # know.
        pool2.set_delivery_mechanism(
            content_type="text/plain", drm_scheme=None,
            rights_uri=RightsStatus.CC_BY_ND
        )
        eq_(None, m(pool2, None))

        pool2.delivery_mechanisms = []
        eq_(None, m(pool2, None))

    def test_process_item(self):
        """Test the code that actually sets up the mirror operation."""

        # Every time process_item() is called, it's either going to ask
        # this thing to mirror the item, or it's going to decide not to.
        class MockMirrorUtility(object):
            def __init__(self):
                self.mirrored = []

            def mirror_link(self, **kwargs):
                self.mirrored.append(kwargs)
        mirror = MockMirrorUtility()

        class MockScript(MirrorResourcesScript):
            MIRROR_UTILITY = mirror
            RIGHTS_STATUS = None

            def derive_rights_status(self, license_pool, resource):
                """Always return the same rights status information.
                To start out, act like no rights information is available.
                """
                self.derive_rights_status_called_with = (license_pool, resource)
                return self.RIGHTS_STATUS

        # Resource and Hyperlink are a pain to use for real, so here
        # are some cheap mocks.
        class MockResource(object):
            def __init__(self, url):
                self.url = url

        class MockLink(object):
            def __init__(self, rel, href, identifier):
                self.rel = rel
                self.resource = MockResource(href)
                self.identifier = identifier

        script = MockScript(self._db)
        m = script.process_item

        # If we can't tie the Hyperlink to a LicensePool in the given
        # Collection, no upload happens. (This shouldn't happen
        # because Hyperlink.unmirrored only finds Hyperlinks
        # associated with Identifiers licensed through a Collection.)
        identifier = self._identifier()
        policy = object()
        download_link = MockLink(
            Hyperlink.OPEN_ACCESS_DOWNLOAD, self._url, identifier
        )
        self._default_collection.data_source = DataSource.GUTENBERG
        m(self._default_collection, download_link, policy)
        eq_([], mirror.mirrored)

        # This HyperLink does match a LicensePool, but it's not
        # in the collection we're mirroring, so mirroring it might not be
        # appropriate.
        work = self._work(
            with_open_access_download=True, collection=self._default_collection
        )
        pool = work.license_pools[0]
        download_link.identifier = pool.identifier
        wrong_collection = self._collection()
        wrong_collection.data_source = DataSource.GUTENBERG
        m(wrong_collection, download_link, policy)
        eq_([], mirror.mirrored)

        # For "open-access" downloads of actual books, if we can't
        # determine the actual rights status of the book, then we
        # don't do anything.
        m(self._default_collection, download_link, policy)
        eq_([], mirror.mirrored)
        eq_((pool, download_link.resource),
            script.derive_rights_status_called_with)

        # If we _can_ determine the rights status, a mirror attempt is made.
        script.RIGHTS_STATUS = object()
        m(self._default_collection, download_link, policy)
        attempt = mirror.mirrored.pop()
        eq_(policy, attempt['policy'])
        eq_(pool.data_source, attempt['data_source'])
        eq_(pool, attempt['model_object'])
        eq_(download_link, attempt['link_obj'])

        link = attempt['link']
        assert isinstance(link, LinkData)
        eq_(download_link.resource.url, link.href)

        # For other types of links, we rely on fair use, so the "rights
        # status" doesn't matter.
        script.RIGHTS_STATUS = None
        thumb_link = MockLink(Hyperlink.THUMBNAIL_IMAGE, self._url,
                              pool.identifier)
        m(self._default_collection, thumb_link, policy)
        attempt = mirror.mirrored.pop()
        eq_(thumb_link.resource.url, attempt['link'].href)


class TestRebuildSearchIndexScript(DatabaseTest):

    def test_do_run(self):
        class MockSearchIndex(object):
            def setup_index(self):
                # This is where the search index is deleted and recreated.
                self.setup_index_called = True

            def bulk_update(self, works):
                self.bulk_update_called_with = works
                return works, []

        index = MockSearchIndex()
        work = self._work(with_license_pool=True)
        work2 = self._work(with_license_pool=True)
        wcr = WorkCoverageRecord
        decoys = [wcr.QUALITY_OPERATION, wcr.GENERATE_MARC_OPERATION]

        # Set up some coverage records.
        for operation in decoys + [wcr.UPDATE_SEARCH_INDEX_OPERATION]:
            for w in (work, work2):
                wcr.add_for(
                    w, operation, status=random.choice(wcr.ALL_STATUSES)
                )

        coverage_qu = self._db.query(wcr).filter(
            wcr.operation==wcr.UPDATE_SEARCH_INDEX_OPERATION
        )
        original_coverage = [x.id for x in coverage_qu]

        # Run the script.
        script = RebuildSearchIndexScript(self._db, search_index_client=index)
        [progress] = script.do_run()

        # The mock methods were called with the values we expect.
        eq_(True, index.setup_index_called)
        eq_(set([work, work2]), set(index.bulk_update_called_with))

        # The script returned a list containing a single
        # CoverageProviderProgress object containing accurate
        # information about what happened (from the CoverageProvider's
        # point of view).
        eq_(
            'Items processed: 2. Successes: 2, transient failures: 0, persistent failures: 0',
            progress.achievements
        )

        # The old WorkCoverageRecords for the works were deleted. Then
        # the CoverageProvider did its job and new ones were added.
        new_coverage = [x.id for x in coverage_qu]
        eq_(2, len(new_coverage))
        assert set(new_coverage) != set(original_coverage)


class TestSearchIndexCoverageRemover(DatabaseTest):

    SERVICE_NAME = "Search Index Coverage Remover"

    def test_do_run(self):
        work = self._work()
        work2 = self._work()
        wcr = WorkCoverageRecord
        decoys = [wcr.QUALITY_OPERATION, wcr.GENERATE_MARC_OPERATION]

        # Set up some coverage records.
        for operation in decoys + [wcr.UPDATE_SEARCH_INDEX_OPERATION]:
            for w in (work, work2):
                wcr.add_for(
                    w, operation, status=random.choice(wcr.ALL_STATUSES)
                )

        # Run the script.
        script = SearchIndexCoverageRemover(self._db)
        result = script.do_run()
        assert isinstance(result, TimestampData)
        eq_("Coverage records deleted: 2", result.achievements)

        # UPDATE_SEARCH_INDEX_OPERATION records have been removed.
        # No other records are affected.
        for w in (work, work2):
            remaining = [x.operation for x in w.coverage_records]
            eq_(sorted(remaining), sorted(decoys))


class TestUpdateLaneSizeScript(DatabaseTest):

    def test_do_run(self):
        lane = self._lane()
        lane.size = 100
        UpdateLaneSizeScript(self._db).do_run(cmd_args=[])
        eq_(0, lane.size)

    def test_should_process_lane(self):
        """Only Lane objects can have their size updated."""
        lane = self._lane()
        script = UpdateLaneSizeScript(self._db)
        eq_(True, script.should_process_lane(lane))

        worklist = WorkList()
        eq_(False, script.should_process_lane(worklist))


class TestUpdateCustomListSizeScript(DatabaseTest):

    def test_do_run(self):
        customlist, ignore = self._customlist(num_entries=1)
        customlist.library = self._default_library
        customlist.size = 100
        UpdateCustomListSizeScript(self._db).do_run(cmd_args=[])
        eq_(1, customlist.size)


class TestWorkConsolidationScript(object):
    """TODO"""
    pass


class TestWorkPresentationScript(object):
    """TODO"""
    pass


class TestWorkClassificationScript(object):
    """TODO"""
    pass


class TestWorkOPDSScript(object):
    """TODO"""
    pass


class TestCustomListManagementScript(object):
    """TODO"""
    pass


class TestNYTBestSellerListsScript(object):
    """TODO"""
    pass
