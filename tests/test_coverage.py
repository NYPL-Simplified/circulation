from nose.tools import (
    set_trace,
    eq_,
)

from . import (
    DatabaseTest,
)

from core.model import (
    DataSource,
    Identifier,
)

from core.opds_import import(
    StatusMessage
)

from core.coverage import(
    CoverageFailure,
)

from api.coverage import (
    OPDSImportCoverageProvider,
)

class TestOPDSImportCoverageProvider(DatabaseTest):

    def test_handle_import_messages(self):
        data_source = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        provider = OPDSImportCoverageProvider("name", [], data_source)

        message = StatusMessage(201, "try again later")
        message2 = StatusMessage(404, "we're doomed")
        message3 = StatusMessage(200, "everything's fine")

        identifier = self._identifier()
        identifier2 = self._identifier()
        identifier3 = self._identifier()

        messages_by_id = { identifier.urn: message,
                           identifier2.urn: message2,
                           identifier3.urn: message3,
        }

        [f1, f2] = sorted(list(provider.handle_import_messages(messages_by_id)),
                          key=lambda x: x.exception)
        eq_(identifier, f1.obj)
        eq_("201", f1.exception)
        eq_(True, f1.transient)

        eq_(identifier2, f2.obj)
        eq_("404", f2.exception)
        eq_(False, f2.transient)
