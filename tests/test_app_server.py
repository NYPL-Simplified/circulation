from nose.tools import (
    assert_raises,
    assert_raises_regexp,
    eq_,
    set_trace,
)

from testing import (
    DatabaseTest,
)

from app_server import URNLookupController

class TestURNLookupController(DatabaseTest):

    def setup(self):
        super(TestURNLookupController, self).setup()
        self.controller = URNLookupController(self._db, True)

    def test_process_urn_invalid_urn(self):
        code, message = self.controller.process_urn("not even a URN")
        eq_(400, code)
        eq_(URNLookupController.INVALID_URN, message)

    def test_process_urn_unresolvable_urn(self):
        pass

    def test_process_urn_work_is_presentation_ready(self):
        pass

    def test_process_urn_work_is_not_presentation_ready(self):
        pass

    def test_process_urn_work_not_created_yet(self):
        pass

    def test_process_urn_initial_registration(self):
        pass

    def test_process_urn_pending_resolve_attempt(self):
        pass

    def test_process_urn_unrecognized_identifier(self):
        # Create a controller that just doesn't resolve identifiers.
        controller = URNLookupController(self._db, True)
        pass
