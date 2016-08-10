import datetime

from nose.tools import (
    assert_raises,
    eq_,
    set_trace,
)

from . import (
    DatabaseTest,
)
from model import (
    get_one,
    CustomList,
    DataSource,
    Identifier,
    Timestamp
)
from scripts import (
    Script,
    CustomListManagementScript,
    DatabaseMigrationScript,
    IdentifierInputScript,
    RunCoverageProviderScript,
    WorkProcessingScript,
    MockStdin,
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


class TestIdentifierInputScript(DatabaseTest):

    def test_parse_list_as_identifiers(self):

        i1 = self._identifier()
        i2 = self._identifier()
        args = [i1.identifier, 'no-such-identifier', i2.identifier]
        identifiers = IdentifierInputScript.parse_identifier_list(
            self._db, i1.type, args
        )
        eq_([i1, i2], identifiers)

        eq_([], IdentifierInputScript.parse_identifier_list(
            self._db, i1.type, [])
        )

    def test_parse_list_as_identifiers_with_autocreate(self):

        type = Identifier.OVERDRIVE_ID
        args = ['brand-new-identifier']
        [i] = IdentifierInputScript.parse_identifier_list(
            self._db, type, args, autocreate=True
        )
        eq_(type, i.type)
        eq_('brand-new-identifier', i.identifier)

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
        cmd_args = ["--identifier-type", Identifier.OVERDRIVE_ID]
        parsed = IdentifierInputScript.parse_command_line(
            self._db, cmd_args, MockStdin()
        )
        eq_([], parsed.identifiers)
        eq_(Identifier.OVERDRIVE_ID, parsed.identifier_type)


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

        overdrive_edition, overdrive_pool = self._edition(
            data_source_name=DataSource.OVERDRIVE, 
            identifier_type=Identifier.OVERDRIVE_ID,
            with_license_pool=True
        )
        overdrive_work = self._work(presentation_edition=overdrive_edition)

        everything = WorkProcessingScript.make_query(self._db, None, None)
        eq_(set([g1, g2, overdrive_work]), set(everything.all()))

        all_gutenberg = WorkProcessingScript.make_query(
            self._db, Identifier.GUTENBERG_ID, []
        )
        eq_(set([g1, g2]), set(all_gutenberg.all()))

        one_gutenberg = WorkProcessingScript.make_query(
            self._db, Identifier.GUTENBERG_ID, [g1.license_pools[0].identifier]
        )
        eq_([g1], one_gutenberg.all())


class TestDatabaseMigrationScript(DatabaseTest):

    def setup(self):
        super(TestDatabaseMigrationScript, self).setup()
        self.script = DatabaseMigrationScript(_db=self._db)
        stamp = datetime.datetime.strptime('20161028', '%Y%m%d')
        self.timestamp = Timestamp(service=self.script.name, timestamp=stamp)

    def test_migration_files(self):
        """Removes migration files that aren't python or SQL from a list."""
        migrations = [
            '.gitkeep', '20150521-make-bananas.sql', '20161028-do-a-thing.py',
            '20161020-did-a-thing.pyc', 'why-am-i-here.rb'
        ]

        result = self.script.migration_files(migrations)
        eq_(2, len(result))
        eq_(['20150521-make-bananas.sql', '20161028-do-a-thing.py'], result)

    def test_new_migrations(self):
        """Filters out migrations that were run before a given timestamp"""

        migrations = [
            '20171202-future-migration-funtime.sql', '20150521-make-bananas.sql',
            '20161028-do-a-thing.py', '20161027-already-done.sql'
        ]

        result = self.script.get_new_migrations(self.timestamp, migrations)
        expected = ['20161028-do-a-thing.py', '20171202-future-migration-funtime.sql']

        eq_(2, len(result))
        eq_(expected, result)

    def test_update_timestamp(self):
        """Resets a timestamp according to the date of a migration file"""

        migration = '20171202-future-migration-funtime.sql'

        assert self.timestamp.timestamp.strftime('%Y%m%d') != migration[0:8]
        self.script.update_timestamp(self.timestamp, migration)
        eq_(self.timestamp.timestamp.strftime('%Y%m%d'), migration[0:8])
