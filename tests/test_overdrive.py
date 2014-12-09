# encoding: utf-8
from nose.tools import (
    eq_,
)
from integration.overdrive import (
    OverdriveAPI,
)

class TestOverdriveAPI(object):

    def test_make_link_safe(self):
        eq_("http://foo.com?q=%2B%3A%7B%7D",
            OverdriveAPI.make_link_safe("http://foo.com?q=+:{}"))
