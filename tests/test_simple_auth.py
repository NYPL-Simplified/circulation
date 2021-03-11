import pytest
from nose.tools import (
    eq_,
    assert_raises_regexp,
    set_trace,
)
import json

from api.authenticator import PatronData

from api.simple_authentication import (
    SimpleAuthenticationProvider,
)

from api.config import (
    CannotLoadConfiguration,
)

from core.testing import DatabaseTest

class TestSimpleAuth(DatabaseTest):

    def test_simple(self):
        p = SimpleAuthenticationProvider
        integration = self._external_integration(self._str)

        with pytest.raises(CannotLoadConfiguration) as excinfo:
            p(self._default_library, integration)
        assert "Test identifier and password not set." in str(excinfo.value)

        integration.setting(p.TEST_IDENTIFIER).value = "barcode"
        integration.setting(p.TEST_PASSWORD).value = "pass"
        provider = p(self._default_library, integration)

        assert None == provider.remote_authenticate("user", "wrongpass")
        assert None == provider.remote_authenticate("user", None)
        assert None == provider.remote_authenticate(None, "pass")
        user = provider.remote_authenticate("barcode", "pass")
        assert isinstance(user, PatronData)
        assert "barcode" == user.authorization_identifier
        assert "barcode_id" == user.permanent_id
        assert "barcode_username" == user.username
        assert None == user.neighborhood

        # For the next test, set the test neighborhood.
        integration.setting(p.TEST_NEIGHBORHOOD).value = "neighborhood"
        provider = p(self._default_library, integration)

        # User can also authenticate by their 'username'
        user2 = provider.remote_authenticate("barcode_username", "pass")
        assert "barcode" == user2.authorization_identifier
        assert "neighborhood" == user2.neighborhood

    def test_no_password_authentication(self):
        """The SimpleAuthenticationProvider can be made even
        simpler by having it authenticate solely based on username.
        """
        p = SimpleAuthenticationProvider
        integration = self._external_integration(self._str)
        integration.setting(p.TEST_IDENTIFIER).value = "barcode"
        integration.setting(p.TEST_PASSWORD).value = "pass"
        integration.setting(p.PASSWORD_KEYBOARD).value = p.NULL_KEYBOARD
        provider = p(self._default_library, integration)

        # If you don't provide a password, you're in.
        user = provider.remote_authenticate("barcode", None)
        assert isinstance(user, PatronData)

        user2 =  provider.remote_authenticate("barcode", '')
        assert user2.authorization_identifier == user.authorization_identifier

        # If you provide any password, you're out.
        assert None == provider.remote_authenticate("barcode", "pass")

    def test_additional_identifiers(self):
        p = SimpleAuthenticationProvider
        integration = self._external_integration(self._str)

        with pytest.raises(CannotLoadConfiguration) as excinfo:
            p(self._default_library, integration)
        assert "Test identifier and password not set." in str(excinfo.value)

        integration.setting(p.TEST_IDENTIFIER).value = "barcode"
        integration.setting(p.TEST_PASSWORD).value = "pass"
        integration.setting(p.ADDITIONAL_TEST_IDENTIFIERS).value = json.dumps(["a", "b", "c"])
        provider = p(self._default_library, integration)

        assert None == provider.remote_authenticate("a", None)
        assert None == provider.remote_authenticate(None, "pass")

        user = provider.remote_authenticate("a", "pass")
        assert isinstance(user, PatronData)
        assert "a" == user.authorization_identifier
        assert "a_id" == user.permanent_id
        assert "a_username" == user.username

        user2 = provider.remote_authenticate("b", "pass")
        assert isinstance(user, PatronData)
        assert "b" == user2.authorization_identifier
        assert "b_id" == user2.permanent_id
        assert "b_username" == user2.username

        # Users can also authenticate by their 'username'
        user3 = provider.remote_authenticate("a_username", "pass")
        assert "a" == user3.authorization_identifier

        user4 = provider.remote_authenticate("b_username", "pass")
        assert "b" == user4.authorization_identifier

        # The main user can still authenticate too.
        user5 = provider.remote_authenticate("barcode", "pass")
        assert "barcode" == user5.authorization_identifier

    def test_generate_patrondata(self):

        m = SimpleAuthenticationProvider.generate_patrondata

        #Pass in numeric barcode as identifier
        result = m("1234")
        assert result.permanent_id == "1234_id"
        assert result.authorization_identifier == '1234'
        assert result.personal_name == "PersonalName1234"
        assert result.username == '1234_username'
        assert result.neighborhood == None

        #Pass in username as identifier
        result = m("1234_username")
        assert result.permanent_id == "1234_id"
        assert result.authorization_identifier == '1234'
        assert result.personal_name == "PersonalName1234"
        assert result.username == '1234_username'
        assert result.neighborhood == None

        # Pass in a neighborhood.
        result = m("1234", "Echo Park")
        assert result.neighborhood == "Echo Park"

    def test__remote_patron_lookup(self):
        p = SimpleAuthenticationProvider
        integration = self._external_integration(self._str)
        integration.setting(p.TEST_IDENTIFIER).value = "barcode"
        integration.setting(p.TEST_PASSWORD).value = "pass"
        integration.setting(p.PASSWORD_KEYBOARD).value = p.NULL_KEYBOARD
        provider = p(self._default_library, integration)
        patron_data = PatronData(authorization_identifier="barcode")
        patron = self._patron()
        patron.authorization_identifier = "barcode"

        #Returns None if nothing is passed in
        assert provider._remote_patron_lookup(None) == None

        #Returns a patron if a patron is passed in and something is found
        result = provider._remote_patron_lookup(patron)
        assert result.permanent_id == "barcode_id"

        #Returns None if no patron is found
        patron.authorization_identifier = "wrong barcode"
        result = provider._remote_patron_lookup(patron)
        assert result == None

        #Returns a patron if a PatronData object is passed in and something is found
        result = provider._remote_patron_lookup(patron_data)
        assert result.permanent_id == "barcode_id"
