from datetime import datetime
from nose.tools import (
    set_trace,
    eq_,
)
from api.sip.client import MockSIPClient
from api.sip import SIP2AuthenticationProvider

class TestSIP2AuthenticationProvider(object):

    # We feed sample data into the MockSIPClient, even though it adds
    # an extra step of indirection, because it lets us use as a
    # starting point the actual (albeit redacted) SIP2 messages we
    # receive from servers.
    
    sierra_valid_login = "64              000201610210000142637000000000000000000000000AOnypl |AA12345|AESHELDON, ALICE|BZ0030|CA0050|CB0050|BLY|CQY|BV0|CC15.00|BEfoo@example.com|AY1AZD1B7"
    sierra_invalid_login = "64Y  YYYYYYYYYYY000201610210000142725000000000000000000000000AOnypl |AA12345|AESHELDON, ALICE|BZ0030|CA0050|CB0050|BLY|CQN|BV0|CC15.00|BEfoo@example.com|AFInvalid PIN entered.  Please try again or see a staff member for assistance.|AFThere are unresolved issues with your account.  Please see a staff member for assistance.|AY1AZ91A8"

    evergreen_active_user = "64  Y           00020161021    142851000000000000000000000000AA12345|AEBooth Active Test|BHUSD|BDAdult Circ Desk 1 Newtown, CT USA 06470|AQNEWTWN|BLY|PA20191004|PCAdult|PIAllowed|XI863715|AOBiblioTest|AY2AZ0000"
    evergreen_expired_card = "64YYYY          00020161021    142937000000000000000000000000AA12345|AEBooth Expired Test|BHUSD|BDAdult Circ Desk #2 Newtown, CT USA 06470|AQNEWTWN|BLY|PA20080907|PCAdult|PIAllowed|XI863716|AFblocked|AOBiblioTest|AY2AZ0000"
    evergreen_excessive_fines = "64  Y           00020161021    143002000000000000000100000000AA12345|AEBooth Excessive Fines Test|BHUSD|BV100.00|BDChildrens Circ Desk 1 Newtown, CT USA 06470|AQNEWTWN|BLY|PA20191004|PCAdult|PIAllowed|XI863718|AOBiblioTest|AY2AZ0000"
    evergreen_inactive_account = "64YYYY          00020161021    143028000000000000000000000000AE|AA12345|BLN|AOBiblioTest|AY2AZ0000"

    polaris_nonexistent_patron = "64YYYY          00120161021    145210000000000000000000000000AO3|AA12345|AE, |BZ0000|CA0000|CB0000|BLN|CQN|BHUSD|BV0.00|CC0.00|BD|BE|BF|BC|PA0|PE|PS|U1|U2|U3|U4|U5|PZ|PX|PYN|FA0.00|AFPatron does not exist.|AGPatron does not exist.|AY2AZBAE9"

    def test_remote_authenticate(self):
        client = MockSIPClient()
        auth = SIP2AuthenticationProvider(
            None, None, None, None, None, None, client=client
        )

        client.queue_response(self.sierra_valid_login)
        patrondata = auth.remote_authenticate("user", "pass")
        eq_("12345", patrondata.authorization_identifier)
        eq_("foo@example.com", patrondata.email_address)
        eq_("SHELDON, ALICE", patrondata.personal_name)
        eq_("0", patrondata.fines)

        client.queue_response(self.sierra_invalid_login)
        eq_(None, auth.remote_authenticate("user", "pass"))


        # Some examples taken from an Evergreen instance that doesn't
        # use passwords.
        client.queue_response(self.evergreen_active_user)
        patrondata = auth.remote_authenticate("user", "pass")
        eq_("12345", patrondata.authorization_identifier)
        eq_("863715", patrondata.permanent_id)
        eq_("Booth Active Test", patrondata.personal_name)
        eq_(None, patrondata.fines)
        eq_(datetime(2019, 10, 4), patrondata.authorization_expires)
        
        client.queue_response(self.evergreen_expired_card)
        patrondata = auth.remote_authenticate("user", "pass")
        eq_("12345", patrondata.authorization_identifier)
        eq_("863716", patrondata.permanent_id)
        eq_("Booth Expired Test", patrondata.personal_name)
        eq_(None, patrondata.fines)
        eq_(datetime(2008, 9, 7), patrondata.authorization_expires)

        client.queue_response(self.evergreen_excessive_fines)
        patrondata = auth.remote_authenticate("user", "pass")
        eq_("12345", patrondata.authorization_identifier)
        eq_("863716", patrondata.permanent_id)
        eq_("Booth Expired Test", patrondata.personal_name)
        eq_(None, patrondata.fines)
        eq_(datetime(2008, 9, 7), patrondata.authorization_expires)
