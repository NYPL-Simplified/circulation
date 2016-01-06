import json
from flask import Flask
from nose.tools import (
    assert_raises,
    assert_raises_regexp,
    eq_,
    set_trace,
)

from . import (
    DatabaseTest,
)

from model import (
    Identifier,
    UnresolvedIdentifier,
)

from lane import (
    Facets,
    Pagination,
)

from app_server import (
    URNLookupController,
    ComplaintController,
    load_facets_from_request,
    load_pagination_from_request,
)

from problem_details import INVALID_INPUT

class TestURNLookupController(DatabaseTest):

    def setup(self):
        super(TestURNLookupController, self).setup()
        self.controller = URNLookupController(self._db, True)

    def test_process_urn_invalid_urn(self):
        code, message = self.controller.process_urn("not even a URN")
        eq_(400, code)
        eq_(URNLookupController.INVALID_URN, message)

    # We can't run this test at the moment because we actually can
    # get info from an ISBN.
    #def test_process_urn_unresolvable_urn(self):
    #    code, message = self.controller.process_urn("urn:isbn:9781449358068")
    #    eq_(400, code)
    #    eq_(URNLookupController.UNRESOLVABLE_URN, message)

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
        eq_([(work.primary_edition.primary_identifier, work)], self.controller.works)

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


class TestComplaintController(DatabaseTest):
    
    def setup(self):
        super(TestComplaintController, self).setup()
        self.controller = ComplaintController()
        self.edition, self.pool = self._edition(with_license_pool=True)
        self.app = Flask(__name__)

    def test_no_license_pool(self):
        with self.app.test_request_context("/"):
            response = self.controller.register(None, "{}")
        assert response.status.startswith('400')
        body = json.loads(response.data)
        eq_("No license pool specified", body['title'])

    def test_invalid_document(self):
        with self.app.test_request_context("/"):
            response = self.controller.register(self.pool, "not {a} valid document")
        assert response.status.startswith('400')
        body = json.loads(response.data)
        eq_("Invalid problem detail document", body['title'])

    def test_invalid_type(self):
        data = json.dumps({"type": "http://not-a-recognized-type/"})
        with self.app.test_request_context("/"):
            response = self.controller.register(self.pool, data)
        assert response.status.startswith('400')
        body = json.loads(response.data)
        eq_("Unrecognized problem type: http://not-a-recognized-type/",
            body['title']
        )

    def test_success(self):
        data = json.dumps(
            {
                "type": "http://librarysimplified.org/terms/problem/wrong-genre",
                "source": "foo",
                "detail": "bar",
            }
        )
        with self.app.test_request_context("/"):
            response = self.controller.register(self.pool, data)
        assert response.status.startswith('201')
        [complaint] = self.pool.complaints
        eq_("foo", complaint.source)
        eq_("bar", complaint.detail)

class TestLoadMethods(object):

    def setup(self):
        self.app = Flask(__name__)


    def test_load_facets_from_request(self):
        with self.app.test_request_context('/?order=%s' % Facets.ORDER_TITLE):
            facets = load_facets_from_request()
            eq_(Facets.ORDER_TITLE, facets.order)

        with self.app.test_request_context('/?order=bad_facet'):
            problemdetail = load_facets_from_request()
            eq_(INVALID_INPUT.uri, problemdetail.uri)

    def test_load_pagination_from_request(self):
        with self.app.test_request_context('/?size=50&after=10'):
            pagination = load_pagination_from_request()
            eq_(50, pagination.size)
            eq_(10, pagination.offset)

        with self.app.test_request_context('/'):
            pagination = load_pagination_from_request()
            eq_(Pagination.DEFAULT_SIZE, pagination.size)
            eq_(0, pagination.offset)

        with self.app.test_request_context('/?size=string'):
            pagination = load_pagination_from_request()
            eq_(INVALID_INPUT.uri, pagination.uri)
            eq_("Invalid size: string", pagination.detail)

        with self.app.test_request_context('/?after=string'):
            pagination = load_pagination_from_request()
            eq_(INVALID_INPUT.uri, pagination.uri)
            eq_("Invalid offset: string", pagination.detail)

        with self.app.test_request_context('/?size=5000'):
            pagination = load_pagination_from_request()
            eq_(100, pagination.size)

