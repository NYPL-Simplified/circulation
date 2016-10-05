"""Standalone tests of the SIP2 client."""

from nose.tools import (
    eq_,
    set_trace,
    assert_raises,
)
from api.sip import (
    MockSIPClient,
)

class TestLogin(object):

    def test_login_message(self):
        sip = MockSIPClient()
        message = sip.login_message('user_id', 'password')
        eq_('9300CNuser_id|COpassword', message)
        
    def test_login_success(self):
        sip = MockSIPClient()
        sip.queue_response('941')
        response = sip.login('user_id', 'password')
        eq_('1', response['login_ok'])

    def test_login_failure(self):
        sip = MockSIPClient()
        sip.queue_response('940')
        response = sip.login('user_id', 'password')
        eq_('0', response['login_ok'])

    def test_login_happens_implicitly_when_user_id_and_password_specified(self):
        sip = MockSIPClient('user_id', 'password')
        sip.queue_response('941')
        sip.queue_response('64Y                201610050000114734                        AOnypl |AA12345|AENo Name|BLN|AFYour library card number cannot be located.  Please see a staff member for assistance.|AY1AZC9DE')
        response = sip.patron_information('patron_identifier')

        # Two requests were made.
        eq_(2, len(sip.requests))

        # We ended up with the right data.
        eq_('12345', response['patron_identifier'])

    def test_login_failure_interrupts_other_request(self):
        sip = MockSIPClient('user_id', 'password')
        sip.queue_response('940')

        # We don't even get a chance to make the patron information request
        # because our login attempt fails.
        assert_raises(IOError,  sip.patron_information, 'patron_identifier')
        
    def test_login_does_not_happen_implicitly_when_user_id_and_password_not_specified(self):
        sip = MockSIPClient()
        sip.queue_response('64Y                201610050000114734                        AOnypl |AA12345|AENo Name|BLN|AFYour library card number cannot be located.  Please see a staff member for assistance.|AY1AZC9DE')
        response = sip.patron_information('patron_identifier')

        # One request were made.
        eq_(1, len(sip.requests))

        # We ended up with the right data.
        eq_('12345', response['patron_identifier'])
        
        
class TestPatronResponse(object):

    def setup(self):
        self.sip = MockSIPClient()
    
    def test_incorrect_card_number(self):
        data = "64Y                201610050000114734                        AOnypl |AA24014027290454|AENo Name|BLN|AFYour library card number cannot be located.  Please see a staff member for assistance.|AY1AZC9DE"

    def test_hold_items(self):
        data = "64              000201610050000114837000300020002000000000000AOnypl |AA23333086712393|AESCHOR, STEPHEN TODD|BZ0030|CA0050|CB0050|BLY|CQY|BV0|CC15.00|AS123|AS456|AS789|BEFOO@BAR.COM|AY1AZC848"

    def test_multiple_screen_messages(self):
        data = "64Y  YYYYYYYYYYY000201610050000115040000000000000000000000000AOnypl |AA23333078284203|AESHELDON, ALICE|BZ0030|CA0050|CB0050|BLY|CQN|BV0|CC15.00|AFInvalid PIN entered.  Please try again or see a staff member for assistance.|AFThere are unresolved issues with your account.  Please see a staff member for assistance.|AY2AZ9B64"

    def test_expired_card(self):
        data = "64Y             000201610050000115547000000000000000000000000AOnypl |AA23333078284203|AESHELDON, ALICE|BZ0030|CA0050|CB0050|BLY|CQY|BV0|CC15.00|AFThere are unresolved issues with your account.  Please see a staff member for assistance.|AY2AZB98D"

    def test_intervening_extension_fields(self):
        """This SIP2 message includes an extension field with the code XI.
        """
        data = "64  Y           00020161005    122942000000000000000000000000AA24014027290454|AEBooth Active Test|BHUSD|BDAdult Circ Desk 1 Newtown, CT USA 06470|AQNEWTWN|BLY|CQN|PA20191004|PCAdult|PIAllowed|XI863715|AOBiblioTest|AY2AZ0000"

    def test_embedded_pipe(self):
        data = '64              000201610050000134405000000000000000000000000AOnypl |AA12345|AERICHARDSON, LEONARD|BZ0030|CA0050|CB0050|BLY|CQY|BV0|CC15.00|BEleona|rdr@|bar.com|AY1AZD1BB\r'

