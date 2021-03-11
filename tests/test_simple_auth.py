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

        assert_raises_regexp(
            CannotLoadConfiguration,
            "Test identifier and password not set.",
            p, self._default_library, integration
        )

        integration.setting(p.TEST_IDENTIFIER).value = "barcode"
        integration.setting(p.TEST_PASSWORD).value = "pass"
        provider = p(self._default_library, integration)

        eq_(None, provider.remote_authenticate("user", "wrongpass"))
        eq_(None, provider.remote_authenticate("user", None))
        eq_(None, provider.remote_authenticate(None, "pass"))
        user = provider.remote_authenticate("barcode", "pass")
        assert isinstance(user, PatronData)
        eq_("barcode", user.authorization_identifier)
        eq_("barcode_id", user.permanent_id)
        eq_("barcode_username", user.username)
        eq_(None, user.neighborhood)

        # For the next test, set the test neighborhood.
        integration.setting(p.TEST_NEIGHBORHOOD).value = "neighborhood"
        provider = p(self._default_library, integration)

        # User can also authenticate by their 'username'
        user2 = provider.remote_authenticate("barcode_username", "pass")
        eq_("barcode", user2.authorization_identifier)
        eq_("neighborhood", user2.neighborhood)

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
        eq_(user2.authorization_identifier, user.authorization_identifier)

        # If you provide any password, you're out.
        eq_(None, provider.remote_authenticate("barcode", "pass"))

    def test_additional_identifiers(self):
        p = SimpleAuthenticationProvider
        integration = self._external_integration(self._str)

        assert_raises_regexp(
            CannotLoadConfiguration,
            "Test identifier and password not set.",
            p, self._default_library, integration
        )

        integration.setting(p.TEST_IDENTIFIER).value = "barcode"
        integration.setting(p.TEST_PASSWORD).value = "pass"
        integration.setting(p.ADDITIONAL_TEST_IDENTIFIERS).value = json.dumps(["a", "b", "c"])
        provider = p(self._default_library, integration)

        eq_(None, provider.remote_authenticate("a", None))
        eq_(None, provider.remote_authenticate(None, "pass"))

        user = provider.remote_authenticate("a", "pass")
        assert isinstance(user, PatronData)
        eq_("a", user.authorization_identifier)
        eq_("a_id", user.permanent_id)
        eq_("a_username", user.username)

        user2 = provider.remote_authenticate("b", "pass")
        assert isinstance(user, PatronData)
        eq_("b", user2.authorization_identifier)
        eq_("b_id", user2.permanent_id)
        eq_("b_username", user2.username)

        # Users can also authenticate by their 'username'
        user3 = provider.remote_authenticate("a_username", "pass")
        eq_("a", user3.authorization_identifier)

        user4 = provider.remote_authenticate("b_username", "pass")
        eq_("b", user4.authorization_identifier)

        # The main user can still authenticate too.
        user5 = provider.remote_authenticate("barcode", "pass")
        eq_("barcode", user5.authorization_identifier)

    def test_generate_patrondata(self):

        m = SimpleAuthenticationProvider.generate_patrondata

        #Pass in numeric barcode as identifier
        result = m("1234")
        eq_(result.permanent_id, "1234_id")
        eq_(result.authorization_identifier, '1234')
        eq_(result.personal_name, "PersonalName1234")
        eq_(result.username, '1234_username')
        eq_(result.neighborhood, None)

        #Pass in username as identifier
        result = m("1234_username")
        eq_(result.permanent_id, "1234_id")
        eq_(result.authorization_identifier, '1234')
        eq_(result.personal_name, "PersonalName1234")
        eq_(result.username, '1234_username')
        eq_(result.neighborhood, None)

        # Pass in a neighborhood.
        result = m("1234", "Echo Park")
        eq_(result.neighborhood, "Echo Park")

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
        eq_(provider._remote_patron_lookup(None), None)

        #Returns a patron if a patron is passed in and something is found
        result = provider._remote_patron_lookup(patron)
        eq_(result.permanent_id, "barcode_id")

        #Returns None if no patron is found
        patron.authorization_identifier = "wrong barcode"
        result = provider._remote_patron_lookup(patron)
        eq_(result, None)

        #Returns a patron if a PatronData object is passed in and something is found
        result = provider._remote_patron_lookup(patron_data)
        eq_(result.permanent_id, "barcode_id")
