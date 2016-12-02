from nose.tools import (
    eq_,
    set_trace,
)
from api.circulation_exceptions import *
from api.problem_details import *

class TestCirculationExceptions(object):
    def test_as_problem_detail_document(self):
        """Verify that circulation exceptions can be turned into ProblemDetail
        documents.
        """

        e = RemoteInitiatedServerError("message", "some service")
        doc = e.as_problem_detail_document()
        eq_("Integration error communicating with some service", doc.detail)

        e = AuthorizationExpired()
        eq_(EXPIRED_CREDENTIALS, e.as_problem_detail_document())

        e = AuthorizationBlocked()
        eq_(BLOCKED_CREDENTIALS, e.as_problem_detail_document())

        e = PatronHoldLimitReached()
        eq_(HOLD_LIMIT_REACHED, e.as_problem_detail_document())

        e = NoLicenses()
        eq_(NO_LICENSES, e.as_problem_detail_document())
