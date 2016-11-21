"""Standalone tests of the SIP2 client."""

from nose.tools import (
    eq_,
    set_trace,
    assert_raises,
)
from api.sipclient import (
    MockSIPClient,
)

class TestBasicProtocol(object):

    def test_login_message(self):
        sip = MockSIPClient()
        message = sip.login_message('user_id', 'password')
        eq_('9300CNuser_id|COpassword', message)

    def test_append_checksum(self):
        sip = MockSIPClient()
        sip.sequence_number=7
        data = "some data"
        new_data = sip.append_checksum(data)
        eq_("some data|AY7AZFAAA", new_data)

    def test_sequence_number_increment(self):
        sip = MockSIPClient()
        sip.sequence_number=0
        sip.queue_response('941')
        response = sip.login('user_id', 'password')
        eq_(1, sip.sequence_number)

        # Test wraparound from 9 to 0
        sip.sequence_number=9
        sip.queue_response('941')
        response = sip.login('user_id', 'password')
        eq_(0, sip.sequence_number)

    def test_resend(self):
        sip = MockSIPClient()
        # The first response will be a request to resend the original message.
        sip.queue_response('96')
        # The second response will indicate a successful login.
        sip.queue_response('941')

        response = sip.login('user_id', 'password')

        # We made two requests for a single login command.
        req1, req2 = sip.requests
        # The first request includes a sequence ID field, "AY1".
        eq_('9300CNuser_id|COpassword|AY1AZF555\r', req1)

        # The second request does not include a sequence ID field. As
        # a consequence its checksum is different.
        eq_('9300CNuser_id|COpassword|AZF620\r', req2)

        # The login request eventually succeeded.
        eq_({'login_ok': '1', '_status': '94'}, response)


class TestLogin(object):
       
    def test_login_success(self):
        sip = MockSIPClient()
        sip.queue_response('941')
        response = sip.login('user_id', 'password')
        eq_({'login_ok': '1', '_status': '94'}, response)

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
        self.sip.queue_response("64Y                201610050000114734                        AOnypl |AA240|AENo Name|BLN|AFYour library card number cannot be located.|AY1AZC9DE")
        response = self.sip.patron_information('identifier')

        # Test some of the basic fields.
        response['institution_id'] = 'nypl '
        response['peronal_name'] = 'No Name'
        response['screen_message'] = ['Your library card number cannot be located.']
        response['valid_patron'] = 'N'
        response['patron_status'] = 'Y             '
        
    def test_hold_items(self):
        "A patron has multiple items on hold."
        self.sip.queue_response("64              000201610050000114837000300020002000000000000AOnypl |AA233|AEBAR, FOO|BZ0030|CA0050|CB0050|BLY|CQY|BV0|CC15.00|AS123|AS456|AS789|BEFOO@BAR.COM|AY1AZC848")
        response = self.sip.patron_information('identifier')        
        eq_('0003', response['hold_items_count'])
        eq_(['123', '456', '789'], response['hold_items'])

    def test_multiple_screen_messages(self):
        self.sip.queue_response("64Y  YYYYYYYYYYY000201610050000115040000000000000000000000000AOnypl |AA233|AESHELDON, ALICE|BZ0030|CA0050|CB0050|BLY|CQN|BV0|CC15.00|AFInvalid PIN entered.  Please try again or see a staff member for assistance.|AFThere are unresolved issues with your account.  Please see a staff member for assistance.|AY2AZ9B64")
        response = self.sip.patron_information('identifier')
        eq_(2, len(response['screen_message']))

    def test_extension_field_captured(self):
        """This SIP2 message includes an extension field with the code XI.
        """
        self.sip.queue_response("64  Y           00020161005    122942000000000000000000000000AA240|AEBooth Active Test|BHUSD|BDAdult Circ Desk 1 Newtown, CT USA 06470|AQNEWTWN|BLY|CQN|PA20191004|PCAdult|PIAllowed|XI86371|AOBiblioTest|ZZfoo|AY2AZ0000")
        response = self.sip.patron_information('identifier')

        # The Evergreen XI field is a known extension and is picked up
        # as internal_id.
        eq_("86371", response['internal_id'])

        # The ZZ field is an unknown extension and is captured under
        # its SIP code.
        eq_(["foo"], response['ZZ'])
       
    def test_embedded_pipe(self):
        """In most cases we can handle data even if it contains embedded
        instances of the separator character.
        """
        self.sip.queue_response('64              000201610050000134405000000000000000000000000AOnypl |AA12345|AERICHARDSON, LEONARD|BZ0030|CA0050|CB0050|BLY|CQY|BV0|CC15.00|BEleona|rdr@|bar.com|AY1AZD1BB\r')
        response = self.sip.patron_information('identifier')
        eq_("leona|rdr@|bar.com", response['email_address'])

    def test_different_separator(self):
        """When you create the SIPClient you get to specify which character
        to use as the field separator.
        """
        sip = MockSIPClient(separator='^')
        sip.queue_response("64Y                201610050000114734                        AOnypl ^AA240^AENo Name^BLN^AFYour library card number cannot be located.^AY1AZC9DE")
        response = sip.patron_information('identifier')
        eq_('240', response['patron_identifier'])

    def test_polaris_extensions(self):
        """Real-world tests of responses sent by Polaris ILS."""

        valid_pin = "64              00120161121    143327000000000000000000000000AO3|AA25891000331441|AEFalkowski, JenJen|BZ0050|CA0075|CB0075|BLY|CQY|BHUSD|BV9.25|CC9.99|BD123 Charlotte Hall, MD 20622|BEfoo@bar.com|BF501-555-1212|BC19710101    000000|PA1|PEHALL|PSSt. Mary's|U1|U2|U3|U4|U5|PZ20622|PX20180609    235959|PYN|FA0.00|AFPatron status is ok.|AGPatron status is ok.|AY2AZ94F3"
        
        wrong_pin = "64YYYY          00120161121    143157000000000000000000000000AO3|AA25891000331441|AEFalkowski, JenJen|BZ0050|CA0075|CB0075|BLY|CQN|BHUSD|BV9.25|CC9.99|BD123 Charlotte Hall, MD 20622|BEfoo@bar.com|BF501-555-1212|BC19710101    000000|PA1|PEHALL|PSSt. Mary's|U1|U2|U3|U4|U5|PZ20622|PX20180609    235959|PYN|FA0.00|AFInvalid patron password. Passwords do not match.|AGInvalid patron password.|AY2AZ87B4"

        expired_card = "64YYYY          00120161121    143430000000000000000000000000AO3|AA25891000224613|AETester, Tess|BZ0050|CA0075|CB0075|BLY|CQY|BHUSD|BV0.00|CC9.99|BD|BEfoo@bar.com|BF|BC19710101    000000|PA1|PELEON|PSSt. Mary's|U1|U2|U3|U4|U5|PZ|PX20161025    235959|PYY|FA0.00|AFPatron has blocks.|AGPatron has blocks.|AY2AZA4F8"

        excess_fines = "64YYYY      Y   00120161121    144438000000000000000000000000AO3|AA25891000115879|AEFalkowski, Jen|BZ0050|CA0075|CB0075|BLY|CQY|BHUSD|BV11.50|CC9.99|BD123, Charlotte Hall, MD 20622|BE|BF501-555-1212|BC20140610    000000|PA1|PEHALL|PS|U1No|U2|U3|U4|U5|PZ20622|PX20170424    235959|PYN|FA0.00|AFPatron has blocks.|AGPatron has blocks.|AY2AZA27B

        no_such_patron = "64YYYY          00120161121    143126000000000000000000000000AO3|AA1112|AE, |BZ0000|CA0000|CB0000|BLN|CQN|BHUSD|BV0.00|CC0.00|BD|BE|BF|BC|PA0|PE|PS|U1|U2|U3|U4|U5|PZ|PX|PYN|FA0.00|AFPatron does not exist.|AGPatron does not exist.|AY2AZBCF2"
