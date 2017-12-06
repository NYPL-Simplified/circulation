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

from . import DatabaseTest

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

        # User can also authenticate by their 'username'
        user2 = provider.remote_authenticate("barcode_username", "pass")
        eq_("barcode", user2.authorization_identifier)

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
