from nose.tools import (
    eq_,
    set_trace,
)

from ..firstbook import DummyFirstBookAuthentationAPI

class TestFirstBook(object):
    
    def setup(self):
        self.api = DummyFirstBookAuthentationAPI(dict(abcd="1234"))

    def test_authentication_success(self):
        eq_(True, self.api.pintest("abcd", "1234"))

    def test_authentication_failure(self):
        eq_(False, self.api.pintest("abcd", "9999"))
        eq_(False, self.api.pintest("nosuchkey", "9999"))

    def test_dump(self):
        eq_({}, self.api.dump("abcd"))
