from nose.tools import (
    eq_,
    set_trace,
)

from api.firstbook import DummyFirstBookAuthentationAPI

class TestFirstBook(object):
    
    def setup(self):
        self.api = DummyFirstBookAuthentationAPI(dict(abcd="1234"))

    def test_authentication_success(self):
        eq_(True, self.api.pintest("abcd", "1234"))

    def test_authentication_failure(self):
        eq_(False, self.api.pintest("abcd", "9999"))
        eq_(False, self.api.pintest("nosuchkey", "9999"))

    def test_server_side_validation(self):
        eq_(False, self.api.server_side_validation("foo' or 1=1 --;", "1234"))
        eq_(False, self.api.server_side_validation("foo", "12 34"))
        eq_(True, self.api.server_side_validation("foo", "1234"))
        eq_(True, self.api.server_side_validation("foo@bar", "1234"))

    def test_dump(self):
        eq_({}, self.api.dump("abcd"))

    def test_patron_info(self):
        eq_("1234", self.api.patron_info("1234").get('barcode'))
    
