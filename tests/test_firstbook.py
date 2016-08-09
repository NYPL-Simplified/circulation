from nose.tools import (
    assert_raises_regexp,
    eq_,
    set_trace,
)

from api.firstbook import (
    DummyFirstBookAuthentationAPI,
)

from api.circulation_exceptions import (
    RemoteInitiatedServerError
)

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

    def test_broken_service_pintest(self):
        api = DummyFirstBookAuthentationAPI(failure_status_code=502)
        assert_raises_regexp(
            RemoteInitiatedServerError, 
            "Got unexpected response code 502. Content: Error 502",
            api.pintest, "key", "pin"
        )
    
    def test_bad_connection_pintest(self):
        api = DummyFirstBookAuthentationAPI(bad_connection=True)
        assert_raises_regexp(
            RemoteInitiatedServerError, 
            "Could not connect!",
            api.pintest, "key", "pin"
        )
    
