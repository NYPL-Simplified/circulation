"""Test the base authentication framework: that is, the classes that
don't interact with any particular source of truth.
"""

from nose.tools import (
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

from . import DatabaseTest

class TestPatronData(DatabaseTest):

    def test_apply(self):
        patron = self._patron()
        now = datetime.datetime.utcnow()
        data = PatronData(
            permanent_id="1",
            authorization_identifier="2",
            username="3",
            personal_name="4",
            email_address="5",
            authorization_expires=now,
            fines="6",
            blocked=False,
        )

        data.apply(patron)
        eq_(data.permanent_id, patron.external_identifier)
        eq_(data.authorization_identifier, patron.authorization_identifier)
        eq_(data.username, patron.username)
        eq_(data.authorization_expires, patron.authorization_expires)
        eq_(data.fines, patron.fines)

        # TODO: blocked is not stored but should be.

        eq_(False, data.blocked)

        # This data is stored in PatronData but not applied to Patron.
        eq_("4", data.personal_name)
        eq_(False, hasattr(patron, 'personal_name'))
        eq_("5", data.email_address)
        eq_(False, hasattr(patron, 'email_address'))
