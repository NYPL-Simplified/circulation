# encoding: utf-8
import json

from nose.tools import (
    assert_raises,
    eq_,
    set_trace,
)

from util.problem_detail import (
    ProblemDetail,
)

class TestProblemDetail(object):

    def test_with_debug(self):
        detail = ProblemDetail("http://uri/", title="Title", detail="Detail")
        with_debug = detail.with_debug("Debug Message")
        eq_("Detail", with_debug.detail)
        eq_("Debug Message", with_debug.debug_message)
        eq_("Title", with_debug.title)
        json_data, status, headers = with_debug.response
        data = json.loads(json_data)
        eq_("Debug Message", data['debug_message'])
        eq_("Detail", data['detail'])
