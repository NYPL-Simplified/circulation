from nose.tools import (
    assert_raises,
    assert_raises_regexp,
    eq_,
    set_trace,
)

from testing import (
    DatabaseTest,
)

from model import (
    Identifier,
    UnresolvedIdentifier,
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
        code, message = self.controller.process_urn("urn:isbn:9781449358068")
        eq_(400, code)
        eq_(URNLookupController.UNRESOLVABLE_URN, message)

    def test_process_urn_initial_registration(self):
        identifier = self._identifier(Identifier.GUTENBERG_ID)
        code, message = self.controller.process_urn(identifier.urn)
        eq_(201, code)
        eq_(URNLookupController.IDENTIFIER_REGISTERED, message)
        [unresolved] = self.controller.unresolved_identifiers
        eq_(identifier, unresolved.identifier)
        eq_(202, unresolved.status)

    def test_process_urn_pending_resolve_attempt(self):
        identifier = self._identifier(Identifier.GUTENBERG_ID)
        unresolved, is_new = UnresolvedIdentifier.register(self._db, identifier)
        code, message = self.controller.process_urn(identifier.urn)
        eq_(202, code)
        eq_(URNLookupController.WORKING_TO_RESOLVE_IDENTIFIER, message)

    def test_process_urn_exception_during_resolve_attempt(self):
        identifier = self._identifier(Identifier.GUTENBERG_ID)
        unresolved, is_new = UnresolvedIdentifier.register(self._db, identifier)
        unresolved.status = 500
        unresolved.exception = "foo"
        code, message = self.controller.process_urn(identifier.urn)
        eq_(500, code)
        eq_("foo", message)

    def test_process_urn_work_is_presentation_ready(self):
        work = self._work(with_license_pool=True)
        identifier = work.license_pools[0].identifier
        code, message = self.controller.process_urn(identifier.urn)
        eq_(None, code)
        eq_(None, message)
        eq_([work], self.controller.works)

    def test_process_urn_work_is_not_presentation_ready(self):
        work = self._work(with_license_pool=True)
        work.presentation_ready = False
        identifier = work.license_pools[0].identifier
        code, message = self.controller.process_urn(identifier.urn)
        eq_(202, code)
        eq_(self.controller.WORK_NOT_PRESENTATION_READY, message)
        eq_([], self.controller.works)

    def test_process_urn_work_not_created_yet(self):
        edition, pool = self._edition(with_license_pool=True)
        identifier = edition.primary_identifier
        code, message = self.controller.process_urn(identifier.urn)
        eq_(202, code)
        eq_(self.controller.WORK_NOT_CREATED, message)
        eq_([], self.controller.works)        

    def test_process_urn_unrecognized_identifier(self):
        # Create a controller that just doesn't resolve identifiers.
        controller = URNLookupController(self._db, False)

        # Give it an identifier it doesn't recognize.
        code, message = controller.process_urn(
            Identifier.URN_SCHEME_PREFIX + 'Gutenberg%20ID/30000000')

        # Instead of creating a resolution task, it simply rejects the
        # input.
        eq_(404, code)
        eq_(controller.UNRECOGNIZED_IDENTIFIER, message)
