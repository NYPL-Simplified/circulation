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
)

class TestScript(DatabaseTest):

    def test_parse_list_as_identifiers(self):

        i1 = self._identifier()
        i2 = self._identifier()
        args = [i1.type, i1.identifier, 'no-such-identifier', i2.identifier]
        identifiers = Script.parse_identifier_list(self._db, args)
        eq_([i1, i2], identifiers)
        eq_([], Script.parse_identifier_list(self._db, []))

    def test_parse_list_as_identifiers_with_autocreate(self):

        args = [Identifier.OVERDRIVE_ID, 'brand-new-identifier']
        [i] = Script.parse_identifier_list(self._db, args, autocreate=True)
        eq_(Identifier.OVERDRIVE_ID, i.type)
        eq_('brand-new-identifier', i.identifier)

    def test_parse_list_as_identifiers_or_data_source(self):

        i1 = self._identifier()
        i2 = self._identifier()
        args = [i1.type, i1.identifier, 'no-such-identifier', i2.identifier]
        identifiers = Script.parse_identifier_list_or_data_source(
            self._db, args
        )
        eq_([i1, i2], identifiers)

        args = [DataSource.OVERDRIVE]
        data_source = Script.parse_identifier_list_or_data_source(self._db, args)
        overdrive = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        eq_(overdrive, data_source)

        eq_([], Script.parse_identifier_list(self._db, []))


    def test_parse_time(self): 
        reference_date = datetime.datetime(2016, 01, 01)

        eq_(Script.parse_time("2016-01-01"), reference_date)

        eq_(Script.parse_time("2016-1-1"), reference_date)

        eq_(Script.parse_time("1/1/2016"), reference_date)

        eq_(Script.parse_time("20160101"), reference_date)

        assert_raises(ValueError, Script.parse_time, "201601-01")


