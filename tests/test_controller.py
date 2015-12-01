from nose.tools import set_trace
from . import DatabaseTest
from ..controller import (
    CirculationManager,
    CirculationManagerController,
)
from ..core.model import (
    Patron
)
from ..problem_details import (
    INVALID_CREDENTIALS,
    EXPIRED_CREDENTIALS,
)

class TestBaseController(DatabaseTest):

    def setup(self):
        super(TestBaseController, self).setup()
        self.manager = CirculationManager(self._db, testing=True)
        self.controller = CirculationManagerController(self.manager)

    def test_authenticated_patron_invalid_credentials(self):
        value = self.controller.authenticated_patron("5", "1234")
        eq_(value, INVALID_CREDENTIALS)

    def test_authenticated_patron_expired_credentials(self):
        value = self.controller.authenticated_patron("0", "0000")
        eq_(value, EXPIRED_CREDENTIALS)

    def test_authenticated_patron_correct_credentials(self):
        value = self.controller.authenticated_patron("5", "5555")
        assert isinstance(value, Patron)
