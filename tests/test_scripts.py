from nose.tools import (
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
        identifiers = list(Script.parse_identifier_list(self._db, args))
        eq_([i1, i2], identifiers)
