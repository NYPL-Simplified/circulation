# encoding: utf-8
import json

from ..util.problem_detail import (
    ProblemDetail,
)

class TestProblemDetail(object):

    def test_with_debug(self):
        detail = ProblemDetail("http://uri/", title="Title", detail="Detail")
        with_debug = detail.with_debug("Debug Message")
        assert "Detail" == with_debug.detail
        assert "Debug Message" == with_debug.debug_message
        assert "Title" == with_debug.title
        json_data, status, headers = with_debug.response
        data = json.loads(json_data)
        assert "Debug Message" == data['debug_message']
        assert "Detail" == data['detail']
