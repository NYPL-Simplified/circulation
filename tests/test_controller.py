from nose.tools import set_trace
from . import DatabaseTest
from ..controller import (
    CirculationManager,
    CirculationManagerController,
)

class TestBaseController(DatabaseTest):

    def setup(self):
        super(TestBaseController, self).setup()
        self.manager = CirculationManager(self._db, testing=True)
        self.controller = CirculationManagerController(self.manager)

    def test_authenticated_patron(self):
        set_trace()
