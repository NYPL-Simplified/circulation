import json
from flask import Flask
from flask.ext.babel import (
    Babel,
    lazy_gettext as _
)
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
    ErrorHandler,
    ComplaintController,
    load_facets_from_request,
    load_pagination_from_request,
)

from problem_details import (
    INVALID_INPUT,
    INVALID_URN,
)

class TestURNLookupController(DatabaseTest):

    def setup(self):
        super(TestURNLookupController, self).setup()
        self.controller = URNLookupController(self._db)

    def test_process_urn_invalid_urn(self):
        urn = "not even a URN"
        self.controller.process_urn(urn)
        eq_(1, len(self.controller.messages_by_urn.keys()))
        code, message = self.controller.messages_by_urn[urn]
        eq_(400, code)
        eq_(INVALID_URN.detail, message)

    def test_process_urn_work_is_presentation_ready(self):
        work = self._work(with_license_pool=True)
        identifier = work.license_pools[0].identifier
        self.controller.process_urn(identifier.urn)
        eq_(0, len(self.controller.messages_by_urn.keys()))
        eq_([(work.presentation_edition.primary_identifier, work)], self.controller.works)

    def test_process_urn_work_is_not_presentation_ready(self):
        work = self._work(with_license_pool=True)
        work.presentation_ready = False
        identifier = work.license_pools[0].identifier
        self.controller.process_urn(identifier.urn)
        eq_(1, len(self.controller.messages_by_urn.keys()))
        code, message = self.controller.messages_by_urn[identifier.urn]
        eq_(202, code)
        eq_(self.controller.WORK_NOT_PRESENTATION_READY, message)
        eq_([], self.controller.works)

    def test_process_urn_work_not_created_yet(self):
        edition, pool = self._edition(with_license_pool=True)
        identifier = edition.primary_identifier
        self.controller.process_urn(identifier.urn)
        eq_(1, len(self.controller.messages_by_urn.keys()))
        code, message = self.controller.messages_by_urn[identifier.urn]
        eq_(202, code)
        eq_(self.controller.WORK_NOT_CREATED, message)
        eq_([], self.controller.works)        

    def test_process_urn_unrecognized_identifier(self):
        # Give the controller a URN that, although valid, doesn't
        # correspond to any Identifier in the database.
        urn = Identifier.URN_SCHEME_PREFIX + 'Gutenberg%20ID/30000000'
        self.controller.process_urn(urn)
        eq_(1, len(controller.messages_by_urn.keys()))

        # The result is a 404 message.
        code, message = controller.messages_by_urn[urn]
        eq_(404, code)
        eq_(self.controller.UNRECOGNIZED_IDENTIFIER, message)


class TestComplaintController(DatabaseTest):
    
    def setup(self):
        super(TestComplaintController, self).setup()
        self.controller = ComplaintController()
        self.edition, self.pool = self._edition(with_license_pool=True)
        self.app = Flask(__name__)
        Babel(self.app)

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
        Babel(self.app)


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
            eq_("Invalid page size: string", str(pagination.detail))

        with self.app.test_request_context('/?after=string'):
            pagination = load_pagination_from_request()
            eq_(INVALID_INPUT.uri, pagination.uri)
            eq_("Invalid offset: string", str(pagination.detail))

        with self.app.test_request_context('/?size=5000'):
            pagination = load_pagination_from_request()
            eq_(100, pagination.size)

    def test_load_pagination_from_request_default_size(self):
        with self.app.test_request_context('/?size=50&after=10'):
            pagination = load_pagination_from_request(default_size=10)
            eq_(50, pagination.size)
            eq_(10, pagination.offset)

        with self.app.test_request_context('/'):
            pagination = load_pagination_from_request(default_size=10)
            eq_(10, pagination.size)
            eq_(0, pagination.offset)


class TestErrorHandler(object):

    def setup(self):
        self.app = Flask(__name__)
        Babel(self.app)

    def raise_exception(self, cls=Exception):
        """Simulate an exception that happens deep within the stack."""
        raise cls()

    def test_unhandled_error(self):
        handler = ErrorHandler(self.app, debug=False)
        with self.app.test_request_context('/'):
            response = None
            try:
                self.raise_exception()
            except Exception, exception:
                response = handler.handle(exception)
            eq_(500, response.status_code)
            eq_("An internal error occured", response.data)

        # Try it again with debug=True to get a stack trace instead of
        # a generic error message.
        handler = ErrorHandler(self.app, debug=True)
        with self.app.test_request_context('/'):
            response = None
            try:
                self.raise_exception()
            except Exception, exception:
                response = handler.handle(exception)
            eq_(500, response.status_code)
            assert response.data.startswith('Traceback (most recent call last)')


    def test_handle_error_as_problem_detail_document(self):
        class CanBeProblemDetailDocument(Exception):

            def as_problem_detail_document(self, debug):
                return INVALID_URN.detailed(
                    _("detail info"),
                    debug_message="A debug_message which should only appear in debug mode."
                )

        handler = ErrorHandler(self.app, debug=False)
        with self.app.test_request_context('/'):
            try:
                self.raise_exception(CanBeProblemDetailDocument)
            except Exception, exception:
                response = handler.handle(exception)

            eq_(400, response.status_code)
            data = json.loads(response.data)
            eq_(INVALID_URN.title, data['title'])

            # Since we are not in debug mode, the debug_message is
            # destroyed.
            assert 'debug_message' not in data

        # Now try it with debug=True and see that the debug_message is
        # preserved and a stack trace is append it to it.
        handler = ErrorHandler(self.app, debug=True)
        with self.app.test_request_context('/'):
            try:
                self.raise_exception(CanBeProblemDetailDocument)
            except Exception, exception:
                response = handler.handle(exception)

            eq_(400, response.status_code)
            data = json.loads(response.data)
            eq_(INVALID_URN.title, data['title'])
            assert data['debug_message'].startswith(
                u"A debug_message which should only appear in debug mode.\n\n"
                u'Traceback (most recent call last)'
            )
