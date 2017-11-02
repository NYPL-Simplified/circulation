from datetime import datetime
from nose.tools import (
    assert_raises_regexp,
    set_trace,
    eq_,
)
from api.sip.client import MockSIPClient
from api.sip import SIP2AuthenticationProvider
from core.util.http import RemoteIntegrationException
from api.authenticator import PatronData

from .. import DatabaseTest

class TestSIP2AuthenticationProvider(DatabaseTest):

    # We feed sample data into the MockSIPClient, even though it adds
    # an extra step of indirection, because it lets us use as a
    # starting point the actual (albeit redacted) SIP2 messages we
    # receive from servers.
    
    sierra_valid_login = "64              000201610210000142637000000000000000000000000AOnypl |AA12345|AESHELDON, ALICE|BZ0030|CA0050|CB0050|BLY|CQY|BV0|CC15.00|BEfoo@example.com|AY1AZD1B7"
    sierra_excessive_fines = "64              000201610210000142637000000000000000000000000AOnypl |AA12345|AESHELDON, ALICE|BZ0030|CA0050|CB0050|BLY|CQY|BV20.00|CC15.00|BEfoo@example.com|AY1AZD1B7"
    sierra_invalid_login = "64Y  YYYYYYYYYYY000201610210000142725000000000000000000000000AOnypl |AA12345|AESHELDON, ALICE|BZ0030|CA0050|CB0050|BLY|CQN|BV0|CC15.00|BEfoo@example.com|AFInvalid PIN entered.  Please try again or see a staff member for assistance.|AFThere are unresolved issues with your account.  Please see a staff member for assistance.|AY1AZ91A8"

    evergreen_active_user = "64  Y           00020161021    142851000000000000000000000000AA12345|AEBooth Active Test|BHUSD|BDAdult Circ Desk 1 Newtown, CT USA 06470|AQNEWTWN|BLY|PA20191004|PCAdult|PIAllowed|XI863715|AOBiblioTest|AY2AZ0000"
    evergreen_expired_card = "64YYYY          00020161021    142937000000000000000000000000AA12345|AEBooth Expired Test|BHUSD|BDAdult Circ Desk #2 Newtown, CT USA 06470|AQNEWTWN|BLY|PA20080907|PCAdult|PIAllowed|XI863716|AFblocked|AOBiblioTest|AY2AZ0000"
    evergreen_excessive_fines = "64  Y           00020161021    143002000000000000000100000000AA12345|AEBooth Excessive Fines Test|BHUSD|BV100.00|BDChildrens Circ Desk 1 Newtown, CT USA 06470|AQNEWTWN|BLY|PA20191004|PCAdult|PIAllowed|XI863718|AOBiblioTest|AY2AZ0000"
    evergreen_hold_privileges_denied = "64   Y          00020161021    143002000000000000000100000000AA12345|AEBooth Excessive Fines Test|BHUSD|BV100.00|BDChildrens Circ Desk 1 Newtown, CT USA 06470|AQNEWTWN|BLY|PA20191004|PCAdult|PIAllowed|XI863718|AOBiblioTest|AY2AZ0000"
    evergreen_card_reported_lost = "64    Y        00020161021    143002000000000000000100000000AA12345|AEBooth Excessive Fines Test|BHUSD|BV100.00|BDChildrens Circ Desk 1 Newtown, CT USA 06470|AQNEWTWN|BLY|PA20191004|PCAdult|PIAllowed|XI863718|AOBiblioTest|AY2AZ0000"
    evergreen_inactive_account = "64YYYY          00020161021    143028000000000000000000000000AE|AA12345|BLN|AOBiblioTest|AY2AZ0000"

    polaris_valid_pin = "64              00120161121    143327000000000000000000000000AO3|AA25891000331441|AEFalk, Jen|BZ0050|CA0075|CB0075|BLY|CQY|BHUSD|BV9.25|CC9.99|BD123 Charlotte Hall, MD 20622|BEfoo@bar.com|BF501-555-1212|BC19710101    000000|PA1|PEHALL|PSSt. Mary's|U1|U2|U3|U4|U5|PZ20622|PX20180609    235959|PYN|FA0.00|AFPatron status is ok.|AGPatron status is ok.|AY2AZ94F3"
        
    polaris_wrong_pin = "64YYYY          00120161121    143157000000000000000000000000AO3|AA25891000331441|AEFalk, Jen|BZ0050|CA0075|CB0075|BLY|CQN|BHUSD|BV9.25|CC9.99|BD123 Charlotte Hall, MD 20622|BEfoo@bar.com|BF501-555-1212|BC19710101    000000|PA1|PEHALL|PSSt. Mary's|U1|U2|U3|U4|U5|PZ20622|PX20180609    235959|PYN|FA0.00|AFInvalid patron password. Passwords do not match.|AGInvalid patron password.|AY2AZ87B4"

    polaris_expired_card = "64YYYY          00120161121    143430000000000000000000000000AO3|AA25891000224613|AETester, Tess|BZ0050|CA0075|CB0075|BLY|CQY|BHUSD|BV0.00|CC9.99|BD|BEfoo@bar.com|BF|BC19710101    000000|PA1|PELEON|PSSt. Mary's|U1|U2|U3|U4|U5|PZ|PX20161025    235959|PYY|FA0.00|AFPatron has blocks.|AGPatron has blocks.|AY2AZA4F8"

    polaris_excess_fines = "64YYYY      Y   00120161121    144438000000000000000000000000AO3|AA25891000115879|AEFalk, Jen|BZ0050|CA0075|CB0075|BLY|CQY|BHUSD|BV11.50|CC9.99|BD123, Charlotte Hall, MD 20622|BE|BF501-555-1212|BC20140610    000000|PA1|PEHALL|PS|U1No|U2|U3|U4|U5|PZ20622|PX20170424    235959|PYN|FA0.00|AFPatron has blocks.|AGPatron has blocks.|AY2AZA27B"

    polaris_no_such_patron = "64YYYY          00120161121    143126000000000000000000000000AO3|AA1112|AE, |BZ0000|CA0000|CB0000|BLN|CQN|BHUSD|BV0.00|CC0.00|BD|BE|BF|BC|PA0|PE|PS|U1|U2|U3|U4|U5|PZ|PX|PYN|FA0.00|AFPatron does not exist.|AGPatron does not exist.|AY2AZBCF2"

    tlc_no_such_patron = "64YYYY          00020171031    092000000000000000000000000000AOhq|AA2642|AE|BLN|AF#Unknown borrower barcode - please refer to the circulation desk.|AY1AZD46E"

    def test_initialize_from_integration(self):
        p = SIP2AuthenticationProvider
        integration = self._external_integration(self._str)
        integration.url = "server.com"
        integration.username = "user1"
        integration.password = "pass1"
        integration.setting(p.FIELD_SEPARATOR).value = "\t"

        provider = p(self._default_library, integration, connect=False)

        # A SIPClient was initialized based on the integration values.
        client = provider.client
        eq_("user1", client.login_user_id)
        eq_("pass1", client.login_password)
        eq_("\t", client.separator)
        eq_("server.com", client.target_server)
        
        # Default port is 6001.
        eq_(6001, client.target_port)

        # Try again, specifying a port.
        integration.setting(p.PORT).value = "1234"
        provider = p(self._default_library, integration, connect=False)
        eq_(1234, provider.client.target_port)
        
    def test_remote_authenticate(self):
        integration = self._external_integration(self._str)
        client = MockSIPClient()
        auth = SIP2AuthenticationProvider(
            self._default_library, integration, client=client
        )

        # Some examples taken from a Sierra SIP API.
        client.queue_response(self.sierra_valid_login)
        patrondata = auth.remote_authenticate("user", "pass")
        eq_("12345", patrondata.authorization_identifier)
        eq_("foo@example.com", patrondata.email_address)
        eq_("SHELDON, ALICE", patrondata.personal_name)
        eq_(0, patrondata.fines)
        eq_(None, patrondata.authorization_expires)
        eq_(None, patrondata.external_type)
        eq_(PatronData.NO_VALUE, patrondata.block_reason)
        
        client.queue_response(self.sierra_invalid_login)
        eq_(None, auth.remote_authenticate("user", "pass"))

        # Since Sierra provides both the patron's fine amount and the
        # maximum allowable amount, we can determine just by looking
        # at the SIP message that this patron is blocked for excessive
        # fines.
        client.queue_response(self.sierra_excessive_fines)
        patrondata = auth.remote_authenticate("user", "pass")
        eq_(PatronData.EXCESSIVE_FINES, patrondata.block_reason)
        
        # Some examples taken from an Evergreen instance that doesn't
        # use passwords.
        client.queue_response(self.evergreen_active_user)
        patrondata = auth.remote_authenticate("user", "pass")
        eq_("12345", patrondata.authorization_identifier)
        eq_("863715", patrondata.permanent_id)
        eq_("Booth Active Test", patrondata.personal_name)
        eq_(0, patrondata.fines)
        eq_(datetime(2019, 10, 4), patrondata.authorization_expires)
        eq_("Adult", patrondata.external_type)

        # A patron with an expired card.
        client.queue_response(self.evergreen_expired_card)
        patrondata = auth.remote_authenticate("user", "pass")
        eq_("12345", patrondata.authorization_identifier)
        # SIP extension field XI becomes sipserver_internal_id which
        # becomes PatronData.permanent_id.
        eq_("863716", patrondata.permanent_id)
        eq_("Booth Expired Test", patrondata.personal_name)
        eq_(0, patrondata.fines)
        eq_(datetime(2008, 9, 7), patrondata.authorization_expires)
        eq_(PatronData.NO_BORROWING_PRIVILEGES, patrondata.block_reason)

        # A patron with excessive fines
        client.queue_response(self.evergreen_excessive_fines)
        patrondata = auth.remote_authenticate("user", "pass")
        eq_("12345", patrondata.authorization_identifier)
        eq_("863718", patrondata.permanent_id)
        eq_("Booth Excessive Fines Test", patrondata.personal_name)
        eq_(100, patrondata.fines)
        eq_(datetime(2019, 10, 04), patrondata.authorization_expires)

        # We happen to know that this patron can't borrow books due to
        # excessive fines, but that information doesn't show up as a 
        # block, because Evergreen doesn't also provide the
        # fine limit. This isn't a big deal -- we'll pick it up later
        # when we apply the site policy.
        #
        # This patron also has "Recall privileges denied" set, but
        # that's not a reason to block them.
        eq_(PatronData.NO_VALUE, patrondata.block_reason)

        # "Hold privileges denied" is not a block because you can
        # still borrow books.
        client.queue_response(self.evergreen_hold_privileges_denied)
        patrondata = auth.remote_authenticate("user", "pass")
        eq_(PatronData.NO_VALUE, patrondata.block_reason)

        client.queue_response(self.evergreen_card_reported_lost)
        patrondata = auth.remote_authenticate("user", "pass")
        eq_(PatronData.CARD_REPORTED_LOST, patrondata.block_reason)
        
        # Some examples taken from a Polaris instance.
        client.queue_response(self.polaris_valid_pin)
        patrondata = auth.remote_authenticate("user", "pass")
        eq_("25891000331441", patrondata.authorization_identifier)
        eq_("foo@bar.com", patrondata.email_address)
        eq_(9.25, patrondata.fines)
        eq_("Falk, Jen", patrondata.personal_name)
        eq_(datetime(2018, 6, 9, 23, 59, 59),
            patrondata.authorization_expires)

        client.queue_response(self.polaris_wrong_pin)
        patrondata = auth.remote_authenticate("user", "pass")
        eq_(None, patrondata)
        
        client.queue_response(self.polaris_expired_card)
        patrondata = auth.remote_authenticate("user", "pass")
        eq_(datetime(2016, 10, 25, 23, 59, 59),
            patrondata.authorization_expires)
        
        client.queue_response(self.polaris_excess_fines)
        patrondata = auth.remote_authenticate("user", "pass")
        eq_(11.50, patrondata.fines)

        # Two cases where the patron's authorization identifier was
        # just not recognized. One on an ILS that sets
        # valid_patron_password='N' when that happens.
        client.queue_response(self.polaris_no_such_patron)
        patrondata = auth.remote_authenticate("user", "pass")
        eq_(None, patrondata)

        # And once on an ILS that leaves valid_patron_password blank
        # when that happens.
        client.queue_response(self.tlc_no_such_patron)
        patrondata = auth.remote_authenticate("user", "pass")
        eq_(None, patrondata)


    def test_ioerror_during_connect_becomes_remoteintegrationexception(self):
        """If the IP of the circulation manager has not been whitelisted,
        we generally can't even connect to the server.
        """
        class CannotConnect(MockSIPClient):
            def connect(self):
                raise IOError("Doom!")


        integration = self._external_integration(self._str)
        assert_raises_regexp(
            RemoteIntegrationException,
            "Error accessing unknown server: Doom!",
            SIP2AuthenticationProvider,
            self._default_library, integration, client=CannotConnect
        )

    def test_ioerror_during_send_becomes_remoteintegrationexception(self):
        """If there's an IOError communicating with the server,
        it becomes a RemoteIntegrationException.
        """
        class CannotSend(MockSIPClient):
            def do_send(self, data):
                raise IOError("Doom!")
        client = CannotSend()

        integration = self._external_integration(self._str)
        provider = SIP2AuthenticationProvider(
            self._default_library, integration, client=client
        )
        provider.client.target_server = 'server.local'
        assert_raises_regexp(
            RemoteIntegrationException,
            "Error accessing server.local: Doom!",
            provider.remote_authenticate,
            "username", "password",
        )
        
    def test_parse_date(self):
        parse = SIP2AuthenticationProvider.parse_date
        eq_(datetime(2011, 1, 2), parse("20110102"))
        eq_(datetime(2011, 1, 2, 10, 20, 30), parse("20110102    102030"))
        eq_(datetime(2011, 1, 2, 10, 20, 30), parse("20110102UTC102030"))
