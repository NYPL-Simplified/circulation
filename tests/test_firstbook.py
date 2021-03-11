from nose.tools import (
    assert_raises_regexp,
    eq_,
    set_trace,
)
import os

from api.authenticator import (
    PatronData,
)

from api.config import (
    Configuration,
    temp_config,
)

from api.firstbook import (
    FirstBookAuthenticationAPI,
    MockFirstBookAuthenticationAPI,
)

from api.circulation_exceptions import (
    RemoteInitiatedServerError
)

from core.testing import DatabaseTest
from core.model import ExternalIntegration


class TestFirstBook(DatabaseTest):

    def setup_method(self):
        super(TestFirstBook, self).setup_method()
        self.integration = self._external_integration(
            ExternalIntegration.PATRON_AUTH_GOAL)
        self.api = self.mock_api(dict(ABCD="1234"))

    def mock_api(self, *args, **kwargs):
        "Create a MockFirstBookAuthenticationAPI."
        return MockFirstBookAuthenticationAPI(
            self._default_library, self.integration,
            *args, **kwargs
        )

    def test_from_config(self):
        api = None
        integration = self._external_integration(self._str)
        integration.url = "http://example.com/"
        integration.password = "the_key"
        api = FirstBookAuthenticationAPI(self._default_library, integration)

        # Verify that the configuration details were stored properly.
        assert 'http://example.com/?key=the_key' == api.root

        # Test the default server-side authentication regular expressions.
        assert False == api.server_side_validation("foo' or 1=1 --;", "1234")
        assert False == api.server_side_validation("foo", "12 34")
        assert True == api.server_side_validation("foo", "1234")
        assert True == api.server_side_validation("foo@bar", "1234")

        # Try another case where the root URL has multiple arguments.
        integration.url = "http://example.com/?foo=bar"
        api = FirstBookAuthenticationAPI(self._default_library, integration)
        assert 'http://example.com/?foo=bar&key=the_key' == api.root

    def test_authentication_success(self):
        assert True == self.api.remote_pin_test("ABCD", "1234")

    def test_authentication_failure(self):
        assert False == self.api.remote_pin_test("ABCD", "9999")
        assert False == self.api.remote_pin_test("nosuchkey", "9999")

        # credentials are uppercased in remote_authenticate;
        # remote_pin_test just passes on whatever it's sent.
        assert False == self.api.remote_pin_test("abcd", "9999")

    def test_remote_authenticate(self):
        patrondata = self.api.remote_authenticate("abcd", "1234")
        assert "ABCD" == patrondata.permanent_id
        assert "ABCD" == patrondata.authorization_identifier
        assert None == patrondata.username

        patrondata = self.api.remote_authenticate("ABCD", "1234")
        assert "ABCD" == patrondata.permanent_id
        assert "ABCD" == patrondata.authorization_identifier
        assert None == patrondata.username


    def test_broken_service_remote_pin_test(self):
        api = self.mock_api(failure_status_code=502)
        assert_raises_regexp(
            RemoteInitiatedServerError,
            "Got unexpected response code 502. Content: Error 502",
            api.remote_pin_test, "key", "pin"
        )

    def test_bad_connection_remote_pin_test(self):
        api = self.mock_api(bad_connection=True)
        assert_raises_regexp(
            RemoteInitiatedServerError,
            "Could not connect!",
            api.remote_pin_test, "key", "pin"
        )

    def test_authentication_flow_document(self):
        # We're about to call url_for, so we must create an
        # application context.
        os.environ['AUTOINITIALIZE'] = "False"
        from api.app import app
        self.app = app
        del os.environ['AUTOINITIALIZE']
        with self.app.test_request_context("/"):
            doc = self.api.authentication_flow_document(self._db)
            assert self.api.DISPLAY_NAME == doc['description']
            assert self.api.FLOW_TYPE == doc['type']
