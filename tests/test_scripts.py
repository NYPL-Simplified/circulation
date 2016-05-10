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
)
from scripts import (
    Script,
    CustomListManagementScript,
    IdentifierInputScript,
    RunCoverageProviderScript,
)

class TestScript(DatabaseTest):

    def test_parse_time(self): 
        reference_date = datetime.datetime(2016, 01, 01)

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
        cmd_args = ["--identifier-type",
                    i1.type, i1.identifier, i2.identifier]
        parsed = IdentifierInputScript.parse_command_line(
            self._db, cmd_args
        )
        eq_([i1, i2], parsed.identifiers)
        eq_(i1.type, parsed.identifier_type)

    def test_parse_command_line_no_identifiers(self):
        cmd_args = ["--identifier-type", Identifier.OVERDRIVE_ID]
        parsed = IdentifierInputScript.parse_command_line(
            self._db, cmd_args
        )
        eq_([], parsed.identifiers)
        eq_(Identifier.OVERDRIVE_ID, parsed.identifier_type)


class TestRunCoverageProviderScript(DatabaseTest):

    def test_parse_command_line(self):
        identifier = self._identifier()
        cmd_args = ["--cutoff-time", "2016-05-01", "--identifier-type", 
                    identifier.type, identifier.identifier]
        parsed = RunCoverageProviderScript.parse_command_line(
            self._db, cmd_args
        )
        eq_(datetime.datetime(2016, 5, 1), parsed.cutoff_time)
        eq_([identifier], parsed.identifiers)
        eq_(identifier.type, parsed.identifier_type)

        
