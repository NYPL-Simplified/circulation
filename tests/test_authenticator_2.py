"""Test the base authentication framework: that is, the classes that
don't interact with any particular source of truth.
"""

from nose.tools import (
    assert_raises_regexp,
    eq_,
    set_trace,
)

import datetime

from core.model import (
    Patron
)

from api.authenticator import (
    Authenticator,
    AuthenticationProvider,
    BasicAuthenticationProvider,
    OAuthController,
    OAuthAuthenticationProvider,
    PatronData,
)

from api.config import (
    CannotLoadConfiguration,
    Configuration,
    temp_config,
)

from . import DatabaseTest

class MockAuthenticationProvider(object):
    """An AuthenticationProvider that always authenticates requests for
    the given Patron and always returns the given PatronData when
    asked to look up data.
    """
    def __init__(self, patron, patrondata):
        self.patron = patron
        self.patrondata = patrondata

    def authenticate(self, header):
        return self.patron

    def remote_patron_lookup(self, patrondata):
        return self.patrondata


class TestPatronData(DatabaseTest):

    def setup(self):
        super(TestPatronData, self).setup()
        self.data = PatronData(
            permanent_id="1",
            authorization_identifier="2",
            username="3",
            personal_name="4",
            email_address="5",
            authorization_expires=datetime.datetime.utcnow(),
            fines="6",
            blocked=False,
        )
        
    
    def test_apply(self):
        patron = self._patron()

        self.data.apply(patron)
        eq_(self.data.permanent_id, patron.external_identifier)
        eq_(self.data.authorization_identifier, patron.authorization_identifier)
        eq_(self.data.username, patron.username)
        eq_(self.data.authorization_expires, patron.authorization_expires)
        eq_(self.data.fines, patron.fines)

        # TODO: blocked is not stored but should be.
        eq_(False, self.data.blocked)

        # This data is stored in PatronData but not applied to Patron.
        eq_("4", self.data.personal_name)
        eq_(False, hasattr(patron, 'personal_name'))
        eq_("5", self.data.email_address)
        eq_(False, hasattr(patron, 'email_address'))

    def test_to_response_parameters(self):

        params = self.data.to_response_parameters
        eq_(dict(name="4"), params)


class TestAuthenticator(DatabaseTest):

    def test_from_config(self):
        """TODO: Since registration happens by loading modules, do this
        after porting over some authorization providers.
        """

    def test_config_fails_when_no_providers_specified(self):
        with temp_config() as config:
            config[Configuration.POLICIES] = {
                Configuration.AUTHENTICATION_POLICY: []
            }
            assert_raises_regexp(
                CannotLoadConfiguration, "No authentication policy given."
            )
        
    def test_register_basic_auth_provider(self):
        """TODO: Since registration happens by loading a module, do this
        after porting over (say) FirstBook authorization provider.
        """

    def test_register_oauth_provider(self):
        """TODO: Since registration happens by loading a module, do this
        after porting over (say) Clever authorization provider.
        """
            
