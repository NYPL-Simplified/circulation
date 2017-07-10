import datetime
import json
import os
import tempfile
from StringIO import StringIO

from nose.tools import (
    assert_raises,
    assert_raises_regexp,
    eq_,
    set_trace,
)

from . import (
    DatabaseTest,
)
from classifier import Classifier

from config import (
    Configuration, 
    temp_config,
)
from external_search import DummyExternalSearchIndex

from model import (
    create,
    get_one,
    CachedFeed,
    Collection,
    Complaint, 
    ConfigurationSetting,
    Contributor, 
    CustomList,
    DataSource,
    Edition,
    ExternalIntegration,
    Identifier,
    Library,
    LicensePool,
    Timestamp, 
    Work,
)
from oneclick import MockOneClickAPI

from scripts import (
    AddClassificationScript,
    CheckContributorNamesInDB, 
    CollectionInputScript,
    ConfigureCollectionScript,
    ConfigureIntegrationScript,
    ConfigureLibraryScript,
    ConfigureSiteScript,
    CustomListManagementScript,
    DatabaseMigrationInitializationScript,
    DatabaseMigrationScript,
    IdentifierInputScript,
    FixInvisibleWorksScript,
    LibraryInputScript,
    MockStdin,
    OneClickDeltaScript,
    OneClickImportScript, 
    OPDSImportScript,
    PatronInputScript,
    RunCollectionMonitorScript,
    RunCoverageProviderScript,
    RunMonitorScript,
    Script,
    ShowCollectionsScript,
    ShowIntegrationsScript,
    ShowLibrariesScript,
    WorkProcessingScript,
)
from monitor import (
    CollectionMonitor,
)
from util.opds_writer import (
    OPDSFeed,
)


class TestScript(DatabaseTest):

    def test_parse_time(self): 
        reference_date = datetime.datetime(2016, 1, 1)

        eq_(Script.parse_time("2016-01-01"), reference_date)

        eq_(Script.parse_time("2016-1-1"), reference_date)

        eq_(Script.parse_time("1/1/2016"), reference_date)

        eq_(Script.parse_time("20160101"), reference_date)

        assert_raises(ValueError, Script.parse_time, "201601-01")


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

        contributor_fixer = CheckContributorNamesInDB(self._db, cmd_args)
        contributor_fixer.run()

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


class OPDSCollectionMonitor(CollectionMonitor):
    """Mock Monitor for use in tests of Run*MonitorScript."""
    SERVICE_NAME = "Test Monitor"
    PROTOCOL = ExternalIntegration.OPDS_IMPORT

    def __init__(self, _db, test_argument=None, **kwargs):
        self.test_argument = test_argument
        super(OPDSCollectionMonitor, self).__init__(_db, **kwargs)

    def run_once(self, start, cutoff):
        self.collection.ran_with_argument = self.test_argument


class DoomedCollectionMonitor(CollectionMonitor):
    """Mock CollectionMonitor that always raises an exception."""
    SERVICE_NAME = "Doomed Monitor"
    PROTOCOL = ExternalIntegration.OPDS_IMPORT
    def run_once(self, *args, **kwargs):
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
        
        
class TestRunCollectionMonitorScript(DatabaseTest):

    def test_all(self):
        # Here we have three OPDS import Collections...
        o1 = self._collection()
        o2 = self._collection()
        o3 = self._collection()

        # ...and a Bibliotheca collection.
        b1 = self._collection(protocol=ExternalIntegration.BIBLIOTHECA)

        script = RunCollectionMonitorScript(
            OPDSCollectionMonitor, self._db, test_argument="test value"
        )
        script.run()

        # Running the script instantiates an OPDSCollectionMonitor for
        # every Collection and calls run_once() on each one. This
        # propagates a value sent into the script constructor to the
        # Collection object.
        for i in [o1, o2, o3]:
            eq_("test value", i.ran_with_argument)

        # Nothing happened to the Bibliotheca collection.
        assert not hasattr(b1, 'ran_with_argument')

    def test_keep_going_on_failure(self):
        # Here we have two Collections that are going to be run
        # through a CollectionMonitor that always fails.
        o1 = self._collection()
        o2 = self._collection()
        script = RunCollectionMonitorScript(
            DoomedCollectionMonitor, self._db
        )
        script.run()

        # Even though run_once() raised an exception, it didn't stop
        # the script from calling run_once() again for the second
        # collection.
        assert(True, o1.doomed)
        assert(True, o2.doomed)
        

class TestPatronInputScript(DatabaseTest):

    def test_parse_patron_list(self):
        """Test that patrons can be identified with any unique identifier."""
        p1 = self._patron()
        p1.authorization_identifier = self._str
        p2 = self._patron()
        p2.username = self._str
        p3 = self._patron()
        p3.external_identifier = self._str
        args = [p1.authorization_identifier, 'no-such-patron',
                '', p2.username, p3.external_identifier]
        patrons = PatronInputScript.parse_patron_list(
            self._db, args
        )
        eq_([p1, p2, p3], patrons)

        eq_([], PatronInputScript.parse_patron_list(self._db, []))

    def test_parse_command_line(self):
        p1 = self._patron()
        p2 = self._patron()
        p1.authorization_identifier = self._str
        p2.authorization_identifier = self._str
        # We pass in one patron identifier on the command line...
        cmd_args = [p1.authorization_identifier]
        # ...and another one into standard input.
        stdin = MockStdin(p2.authorization_identifier)
        parsed = PatronInputScript.parse_command_line(
            self._db, cmd_args, stdin
        )
        eq_([p1, p2], parsed.patrons)

    def test_parse_command_line_no_identifiers(self):
        parsed = PatronInputScript.parse_command_line(
            self._db, [], MockStdin()
        )
        eq_([], parsed.patrons)


    def test_do_run(self):
        """Test that PatronInputScript.do_run() calls process_patron()
        for every patron designated by the command-line arguments.
        """
        class MockPatronInputScript(PatronInputScript):
            def process_patron(self, patron):
                patron.processed = True
        p1 = self._patron()
        p2 = self._patron()
        p3 = self._patron()
        p3.processed = False
        p1.authorization_identifier = self._str
        p2.authorization_identifier = self._str
        cmd_args = [p1.authorization_identifier]
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


class TestDatabaseMigrationScript(DatabaseTest):

    def _create_test_migrations(self):
        """Sets up migrations in the expected locations"""

        directories = self.script.directories_by_priority
        [self.core_migration_dir, self.parent_migration_dir] = directories

        # Create temporary migration directories where
        # DatabaseMigrationScript expects them.
        for migration_dir in directories:
            if not os.path.isdir(migration_dir):
                temp_migration_dir = tempfile.mkdtemp()
                os.rename(temp_migration_dir, migration_dir)

        # Put a file of each migratable type in both directories.
        self._create_test_migration_file(self.core_migration_dir, 'CORE', 'sql')
        self._create_test_migration_file(self.core_migration_dir, 'CORE', 'py')
        self._create_test_migration_file(self.parent_migration_dir, 'SERVER', 'sql')
        self._create_test_migration_file(self.parent_migration_dir, 'SERVER', 'py')

    def _create_test_migration_file(self, directory, unique_string,
                                    migration_type, migration_date=None):
        suffix = '.'+migration_type

        if migration_type=='sql':
            # Create unique, innocuous content for a SQL file.
            # This SQL inserts a timestamp into the test database.
            service = "Test Database Migration Script - %s" % unique_string
            content = (("insert into timestamps(service, timestamp)"
                        " values ('%s', '%s');") % (service, '1970-01-01'))
        elif migration_type=='py':
            # Create unique, innocuous content for a Python file.
            # This python creates a temporary .py file in core/tests.
            core = os.path.split(self.core_migration_dir)[0]
            target_dir = os.path.join(core, 'tests')
            content = (
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
        # Hold onto details about the file for deletion in teardown().
        self.migration_files.append(migration_file_info)

        with open(migration_file_info[1], 'w') as migration:
            # Write content to the file.
            migration.write(content)

    def setup(self):
        super(TestDatabaseMigrationScript, self).setup()
        self.script = MockDatabaseMigrationScript(_db=self._db)

        # This list holds any temporary files created during tests
        # so they can be deleted during teardown().
        self.migration_files = []
        self._create_test_migrations()

        stamp = datetime.datetime.strptime('20260810', '%Y%m%d')
        self.timestamp = Timestamp(service=self.script.name, timestamp=stamp)
        self._db.add(self.timestamp)

    def teardown(self):
        """Delete any files and directories created during testing."""
        for fd, fpath in self.migration_files:
            os.close(fd)
            os.remove(fpath)
            if fpath.endswith('.py'):
                # Remove compiled files.
                try:
                    os.remove(fpath+'c')
                except OSError:
                    pass

        for directory in self.script.directories_by_priority:
            os.rmdir(directory)

        test_dir = os.path.split(__file__)[0]
        all_files = os.listdir(test_dir)
        test_generated_files = sorted([f for f in all_files
                                       if f.startswith(('CORE', 'SERVER'))])
        for filename in test_generated_files:
            os.remove(os.path.join(test_dir, filename))

        super(TestDatabaseMigrationScript, self).teardown()

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

        for desc, migration_file in self.migration_files:
            assert os.path.split(migration_file)[1] in result_migrations

        def extract_filenames(core=True):
            pathnames = [pathname for desc, pathname in self.migration_files]
            if core:
                pathnames = [p for p in pathnames if 'core' in p]
            else:
                pathnames = [p for p in pathnames if 'core' not in p]

            return [os.path.split(p)[1] for p in pathnames]

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

    def test_migratable_files(self):
        """Removes migration files that aren't python or SQL from a list."""

        migrations = [
            '.gitkeep', '20250521-make-bananas.sql', '20260810-do-a-thing.py',
            '20260802-did-a-thing.pyc', 'why-am-i-here.rb'
        ]

        result = self.script.migratable_files(migrations)
        eq_(2, len(result))
        eq_(['20250521-make-bananas.sql', '20260810-do-a-thing.py'], result)

    def test_get_new_migrations(self):
        """Filters out migrations that were run on or before a given timestamp"""

        migrations = [
            '20271202-future-migration-funtime.sql',
            '20250521-make-bananas.sql',
            '20260810-last-timestamp',
            '20260811-do-a-thing.py',
            '20260809-already-done.sql'
        ]

        result = self.script.get_new_migrations(self.timestamp, migrations)
        # Expected migrations will be sorted by timestamp.
        expected = [
            '20260811-do-a-thing.py', '20271202-future-migration-funtime.sql'
        ]

        eq_(2, len(result))
        eq_(expected, result)

        # If the timestamp has a counter, the filter only finds new migrations
        # past the counter.
        migrations = [
            '20271202-future-migration-funtime.sql',
            '20260810-last-timestamp.sql',
            '20260810-1-do-a-thing.sql',
            '20260810-2-do-all-the-things.sql',
            '20260809-already-done.sql'
        ]
        self.timestamp.counter = 1
        result = self.script.get_new_migrations(self.timestamp, migrations)
        expected = [
            '20260810-2-do-all-the-things.sql',
            '20271202-future-migration-funtime.sql'
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
        self.timestamp.counter = None

        result = self.script.get_new_migrations(self.timestamp, migrations)
        expected = [
            '20260810-1-do-all-the-things.sql',
            '20271202-future-migration-funtime.sql',
            '20271202-1-more-future-migration-funtime.sql'
        ]
        eq_(3, len(result))
        eq_(expected, result)

    def test_update_timestamp(self):
        """Resets a timestamp according to the date of a migration file"""

        migration = '20271202-future-migration-funtime.sql'

        assert self.timestamp.timestamp.strftime('%Y%m%d') != migration[0:8]
        self.script.update_timestamp(self.timestamp, migration)
        eq_(self.timestamp.timestamp.strftime('%Y%m%d'), migration[0:8])

        # It also takes care of counter digits when multiple migrations
        # exist for the same date.
        migration = '20260810-2-do-all-the-things.sql'
        self.script.update_timestamp(self.timestamp, migration)
        eq_(self.timestamp.timestamp.strftime('%Y%m%d'), migration[0:8])
        eq_(str(self.timestamp.counter), migration[9])

        # And removes those counter digits when the timestamp is updated.
        migration = '20260101-what-it-do.sql'
        self.script.update_timestamp(self.timestamp, migration)
        eq_(self.timestamp.timestamp.strftime('%Y%m%d'), migration[0:8])
        eq_(self.timestamp.counter, None)

    def test_running_a_migration_updates_the_timestamp(self):
        future_time = datetime.datetime.strptime('20261030', '%Y%m%d')
        self.timestamp.timestamp = future_time

        # Create a test migration after that point and grab relevant info
        # about it.
        self._create_test_migration_file(
            self.core_migration_dir, 'SINGLE', 'sql',
            migration_date='20261202'
        )

        # Pop the last migration filepath off and run the migration with
        # the relevant information.
        migration_filepath = self.migration_files[-1][1]
        migration_filename = os.path.split(migration_filepath)[1]
        migrations_by_dir = {
            self.core_migration_dir : [migration_filename],
            self.parent_migration_dir : []
        }

        # Running the migration updates the timestamp
        self.script.run_migrations(
            [migration_filename], migrations_by_dir, self.timestamp
        )
        eq_(self.timestamp.timestamp.strftime('%Y%m%d'), '20261202')

        # Even when there are counters.
        self._create_test_migration_file(
            self.core_migration_dir, 'COUNTER', 'sql',
            migration_date='20261203-3'
        )
        migration_filename = os.path.split(self.migration_files[-1][1])[1]
        migrations_by_dir[self.core_migration_dir] = [migration_filename]
        self.script.run_migrations(
            [migration_filename], migrations_by_dir, self.timestamp
        )
        eq_(self.timestamp.timestamp.strftime('%Y%m%d'), '20261203')
        eq_(self.timestamp.counter, 3)

    def test_all_migration_files_are_run(self):
        self.script.do_run()

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


class TestDatabaseMigrationInitializationScript(DatabaseTest):

    def setup(self):
        super(TestDatabaseMigrationInitializationScript, self).setup()
        self.script = DatabaseMigrationInitializationScript(_db=self._db)

    @property
    def timestamp(self):
        return self._db.query(Timestamp).\
            filter(Timestamp.service==self.script.name).one()

    def assert_matches_latest_migration(self):
        migrations = self.script.fetch_migration_files()[0]
        last_migration_date = self.script.sort_migrations(migrations)[-1][:8]
        eq_(self.timestamp.timestamp.strftime('%Y%m%d'), last_migration_date)

    def test_accurate_timestamp_created(self):
        eq_(None, Timestamp.value(self._db, self.script.name, collection=None))

        self.script.do_run()
        self.assert_matches_latest_migration()

    def test_error_raised_when_timestamp_exists(self):
        Timestamp.stamp(self._db, self.script.name, None)
        assert_raises(RuntimeError, self.script.do_run)

    def test_error_not_raised_when_timestamp_forced(self):
        Timestamp.stamp(self._db, self.script.name, None)
        self.script.do_run(['-f'])
        self.assert_matches_latest_migration()

    def test_accepts_last_run_date(self):
        # A timestamp can be passed via the command line.
        self.script.do_run(['--last-run-date', '20101010'])
        expected_stamp = datetime.datetime.strptime('20101010', '%Y%m%d')
        eq_(expected_stamp, self.timestamp.timestamp)

        # It will override an existing timestamp if forced.
        previous_timestamp = self.timestamp
        self.script.do_run(['--last-run-date', '20111111', '--force'])
        expected_stamp = datetime.datetime.strptime('20111111', '%Y%m%d')
        eq_(previous_timestamp, self.timestamp)
        eq_(expected_stamp, self.timestamp.timestamp)

    def test_accepts_last_run_counter(self):
        # If a counter is passed without a date, an error is raised.
        assert_raises(ValueError, self.script.do_run, ['--last-run-counter', '7'])

        # With a date, the counter can be set.
        self.script.do_run(['--last-run-date', '20101010', '--last-run-counter', '7'])
        expected_stamp = datetime.datetime.strptime('20101010', '%Y%m%d')
        eq_(expected_stamp, self.timestamp.timestamp)
        eq_(7, self.timestamp.counter)

        # When forced, the counter can be reset on an existing timestamp.
        previous_timestamp = self.timestamp
        self.script.do_run(['--last-run-date', '20121212', '--last-run-counter', '2', '-f'])
        expected_stamp = datetime.datetime.strptime('20121212', '%Y%m%d')
        eq_(previous_timestamp, self.timestamp)
        eq_(expected_stamp, self.timestamp.timestamp)
        eq_(2, self.timestamp.counter)


class TestAddClassificationScript(DatabaseTest):

    def test_end_to_end(self):
        work = self._work(with_license_pool=True)
        identifier = work.license_pools[0].identifier
        eq_(Classifier.AUDIENCE_ADULT, work.audience)
        
        cmd_args = [
            "--identifier-type", identifier.type,
            "--subject-type", Classifier.FREEFORM_AUDIENCE,
            "--subject-identifier", Classifier.AUDIENCE_CHILDREN,
            "--weight", "42", '--create-subject',
            identifier.identifier
        ]
        script = AddClassificationScript(self._db, cmd_args)
        script.run()

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
        eq_(Classifier.AUDIENCE_ADULT, work.audience)
        
        cmd_args = [
            "--identifier-type", identifier.type,
            "--subject-type", Classifier.TAG,
            "--subject-identifier", "some random tag",
            identifier.identifier
        ]
        script = AddClassificationScript(self._db, cmd_args)
        script.run()

        # Nothing has happened. There was no Subject with that
        # identifier, so we assumed there was a typo and did nothing.
        eq_([], identifier.classifications)

        # If we stick the 'create-subject' onto the end of the
        # command-line arguments, the Subject is created and the
        # classification happens.
        cmd_args.append('--create-subject')
        script = AddClassificationScript(self._db, cmd_args)
        script.run()

        [classification] = identifier.classifications
        subject = classification.subject
        eq_("some random tag", subject.identifier)



class TestOneClickImportScript(DatabaseTest):

    def get_data(self, filename):
        base_path = os.path.split(__file__)[0]
        self.resource_path = os.path.join(base_path, "files", "oneclick")

        # returns contents of sample file as string and as dict
        path = os.path.join(self.resource_path, filename)
        data = open(path).read()
        return data, json.loads(data)

    def test_import(self):
        base_path = os.path.split(__file__)[0]
        collection = MockOneClickAPI.mock_collection(self._db)
        importer = OneClickImportScript(
            collection, api_class=MockOneClickAPI, base_path=base_path
        )
        datastr, datadict = self.get_data("response_catalog_all_sample.json")
        importer.api.queue_response(status_code=200, content=datastr)
        importer.run()

        # verify that we created Works, Editions, LicensePools
        works = self._db.query(Work).all()
        work_titles = [work.title for work in works]
        expected_titles = ["Tricks", "Emperor Mage: The Immortals", 
            "In-Flight Russian", "Road, The", "Private Patient, The", 
            "Year of Magical Thinking, The", "Junkyard Bot: Robots Rule, Book 1, The", 
            "Challenger Deep"]
        eq_(set(expected_titles), set(work_titles))

        # make sure we created some Editions
        edition = Edition.for_foreign_id(self._db, DataSource.ONECLICK, Identifier.ONECLICK_ID, "9780062231727", create_if_not_exists=False)
        assert(edition is not None)
        edition = Edition.for_foreign_id(self._db, DataSource.ONECLICK, Identifier.ONECLICK_ID, "9781615730186", create_if_not_exists=False)
        assert(edition is not None)

        # make sure we created some LicensePools
        pool, made_new = LicensePool.for_foreign_id(
            self._db, DataSource.ONECLICK, Identifier.ONECLICK_ID,
            "9780062231727", collection=collection
        )
        eq_(False, made_new)
        pool, made_new = LicensePool.for_foreign_id(
            self._db, DataSource.ONECLICK, Identifier.ONECLICK_ID,
            "9781615730186", collection=collection
        )
        eq_(False, made_new)

        # make sure there are 8 LicensePools
        pools = self._db.query(LicensePool).all()
        eq_(8, len(pools))


class TestOneClickDeltaScript(DatabaseTest):

    def get_data(self, filename):
        base_path = os.path.split(__file__)[0]
        self.resource_path = os.path.join(base_path, "files", "oneclick")

        # returns contents of sample file as string and as dict
        path = os.path.join(self.resource_path, filename)
        data = open(path).read()
        return data, json.loads(data)


    def test_delta(self):
        # First, load a collection.
        base_path = os.path.split(__file__)[0]
        collection = MockOneClickAPI.mock_collection(self._db)
        importer = OneClickImportScript(
            collection, api_class=MockOneClickAPI, base_path=base_path
        )
        datastr, datadict = self.get_data("response_catalog_all_sample.json")
        importer.api.queue_response(status_code=200, content=datastr)
        importer.run()

        # set license numbers on test pool
        pool, made_new = LicensePool.for_foreign_id(
            self._db, DataSource.ONECLICK, Identifier.ONECLICK_ID,
            "9781615730186", collection=collection
        )
        eq_(False, made_new)
        pool.licenses_owned = 10
        pool.licenses_available = 9
        pool.licenses_reserved = 2
        pool.patrons_in_hold_queue = 1

        # now update that library with a sample delta            
        delta_runner = OneClickDeltaScript(
            collection, api_class=MockOneClickAPI, base_path=base_path
        )
        datastr, datadict = self.get_data("response_catalog_delta.json")
        delta_runner.api.queue_response(status_code=200, content=datastr)
        delta_runner.run()

        # "Tricks" did not get deleted, but did get its pools set to "nope".
        # "Emperor Mage: The Immortals" got new metadata.
        works = self._db.query(Work).all()
        work_titles = [work.title for work in works]
        expected_titles = ["Tricks", "Emperor Mage: The Immortals", 
            "In-Flight Russian", "Road, The", "Private Patient, The", 
            "Year of Magical Thinking, The", "Junkyard Bot: Robots Rule, Book 1, The", 
            "Challenger Deep"]
        eq_(set(expected_titles), set(work_titles))

        eq_("Tricks", pool.presentation_edition.title)
        eq_(0, pool.licenses_owned)
        eq_(0, pool.licenses_available)
        eq_(0, pool.licenses_reserved)
        eq_(0, pool.patrons_in_hold_queue)
        assert (datetime.datetime.utcnow() - pool.last_checked) < datetime.timedelta(seconds=20)

        # make sure we updated fields
        edition = Edition.for_foreign_id(self._db, DataSource.ONECLICK, Identifier.ONECLICK_ID, "9781934180723", create_if_not_exists=False)
        eq_("Recorded Books, Inc.", edition.publisher)

        # make sure there are still 8 LicensePools
        pools = self._db.query(LicensePool).all()
        eq_(8, len(pools))


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
                { "key": "secret_setting" },
            ]

        script = ConfigureSiteScript(config=TestConfig)
        output = StringIO()
        script.do_run(
            self._db, [
                "--setting=setting1=value1",
                "--setting=setting2=[1,2,\"3\"]",
                "--setting=secret_setting=secretvalue",
            ],
            output
        )
        # The secret was set, but is not shown.
        eq_("""Current site-wide settings:
setting1='value1'
setting2='[1,2,"3"]'
""",
            output.getvalue()
        )
        eq_("value1", ConfigurationSetting.sitewide(self._db, "setting1").value)
        eq_('[1,2,"3"]', ConfigurationSetting.sitewide(self._db, "setting2").value)
        eq_("secretvalue", ConfigurationSetting.sitewide(self._db, "secret_setting").value)

        # If we run again with --show-secrets, the secret is shown.
        output = StringIO()
        script.do_run(self._db, ["--show-secrets"], output)
        eq_("""Current site-wide settings:
secret_setting='secretvalue'
setting1='value1'
setting2='[1,2,"3"]'
""",
            output.getvalue()
        )


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

        # Since we provided no collection, no MockOPDSImportMonitor
        # was instantiated.
        eq_([], MockOPDSImportMonitor.INSTANCES)

        args = ['--collection=%s' % self._default_collection.name]
        script.do_run(args)

        # Now a monitor has been instantiated and run.
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


class TestFixInvisibleWorksScript(DatabaseTest):

    def test_no_presentation_ready_works(self):
        output = StringIO()
        search = DummyExternalSearchIndex()

        FixInvisibleWorksScript(self._db, output, search=search).do_run()
        eq_("""0 presentation-ready works.
0 works not presentation-ready.
Here's your problem: there are no presentation-ready works.
""", output.getvalue())

    def test_no_materialized_view(self):
        output = StringIO()
        search = DummyExternalSearchIndex()

        # This work is marked as presentation-ready, but it has no
        # LicensePools, and will not show up in the materialized view.
        work = self._work(with_license_pool=False)
        work.presentation_ready=True
        FixInvisibleWorksScript(self._db, output, search=search).do_run()
        eq_("""1 presentation-ready works.
0 works not presentation-ready.
0 works in materialized view.
Refreshing the materialized views.
0 works in materialized view after refresh.
Here's your problem: your presentation-ready works are not making it into the materialized view.
""", output.getvalue())

    def test_no_delivery_mechanism(self):
        output = StringIO()
        search = DummyExternalSearchIndex()

        # This work has a license pool, but no delivery mechanisms.
        work = self._work(with_license_pool=True)
        work.presentation_ready=True
        for lpdm in work.license_pools[0].delivery_mechanisms:
            self._db.delete(lpdm)

        FixInvisibleWorksScript(self._db, output, search=search).do_run()
        eq_("""1 presentation-ready works.
0 works not presentation-ready.
0 works in materialized view.
Refreshing the materialized views.
1 works in materialized view after refresh.
Here's your problem: your works don't have delivery mechanisms.
""", output.getvalue())

    def test_suppressed_pool(self):
        output = StringIO()
        search = DummyExternalSearchIndex()

        # This work has a license pool, but it's suppressed.
        work = self._work(with_license_pool=True)
        work.presentation_ready=True
        work.license_pools[0].suppressed = True

        FixInvisibleWorksScript(self._db, output, search=search).do_run()
        eq_("""1 presentation-ready works.
0 works not presentation-ready.
0 works in materialized view.
Refreshing the materialized views.
1 works in materialized view after refresh.
Here's your problem: your works' license pools are suppressed.
""", output.getvalue())

    def test_no_licenses(self):
        output = StringIO()
        search = DummyExternalSearchIndex()

        # This work has a license pool, but no licenses owned.
        work = self._work(with_license_pool=True)
        work.presentation_ready=True
        work.license_pools[0].licenses_owned = 0

        FixInvisibleWorksScript(self._db, output, search=search).do_run()
        eq_("""1 presentation-ready works.
0 works not presentation-ready.
0 works in materialized view.
Refreshing the materialized views.
1 works in materialized view after refresh.
Here's your problem: your works aren't open access and have no licenses owned.
""", output.getvalue())

    def test_success(self):
        output = StringIO()
        search = DummyExternalSearchIndex()

        # Let's add a work that's not presentation-ready for a stupid
        # reason.
        work = self._work(with_license_pool=True)
        work.presentation_ready = False

        # It's not in the materialized view.
        from model import MaterializedWork
        mw_query = self._db.query(MaterializedWork)
        eq_(0, mw_query.count())
        
        # Let's also add a CachedFeed which might be clogging things up.
        feed = create(self._db, CachedFeed, type=CachedFeed.PAGE_TYPE,
                      pagination="")
        
        FixInvisibleWorksScript(self._db, output, search=search).do_run()
        eq_("""0 presentation-ready works.
1 works not presentation-ready.
Attempting to make 1 works presentation-ready based on their metadata.
1 works are now presentation-ready.
0 works in materialized view.
Refreshing the materialized views.
1 works in materialized view after refresh.
1 page-type feeds in cachedfeeds table.
Deleting them all.
I would now expect you to be able to find 1 works.
""", output.getvalue())

        # The Work was made presentation-ready
        eq_(True, work.presentation_ready)

        # The CachedFeed was deleted.
        eq_(0, self._db.query(CachedFeed).count())

        # The materialized view was refreshed.
        eq_(1, mw_query.count())

    def test_with_collections(self):
        search = DummyExternalSearchIndex()

        c1 = self._collection()
        c2 = self._collection()

        # One collection has a work that's not presentation-ready.
        work = self._work(with_license_pool=True, collection=c2)
        work.presentation_ready = False

        # It's not in the materialized view.
        from model import MaterializedWork
        mw_query = self._db.query(MaterializedWork)
        eq_(0, mw_query.count())

        output = StringIO()

        # Running the script on a different collection won't help.
        FixInvisibleWorksScript(self._db, output, search=search).do_run(collections=[c1])
        eq_("""0 presentation-ready works.
0 works not presentation-ready.
Here's your problem: there are no presentation-ready works.
""", output.getvalue())

        # The Work is still not presentation-ready
        eq_(False, work.presentation_ready)

        # It's still not in the materialized view.
        eq_(0, mw_query.count())


        output = StringIO()

        # But running it with the right collection fixes the work.
        FixInvisibleWorksScript(self._db, output, search=search).do_run(collections=[c2])
        eq_("""0 presentation-ready works.
1 works not presentation-ready.
Attempting to make 1 works presentation-ready based on their metadata.
1 works are now presentation-ready.
0 works in materialized view.
Refreshing the materialized views.
1 works in materialized view after refresh.
0 page-type feeds in cachedfeeds table.
I would now expect you to be able to find 1 works.
""", output.getvalue())

        # The Work was made presentation-ready
        eq_(True, work.presentation_ready)

        # The materialized view was refreshed.
        eq_(1, mw_query.count())
        

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


class TestSubjectAssignmentScript(object):
    """TODO"""
    pass


class TestBibliographicRefreshScript(object):
    """TODO"""
    pass

        
class TestNYTBestSellerListsScript(object):
    """TODO"""
    pass


class TestRefreshMaterializedViewsScript(object):
    """TODO"""
    pass


class TestExplain(object):
    """TODO"""
    pass
