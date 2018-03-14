"""Standalone tests of the SIP2 client."""

from nose.tools import (
    eq_,
    set_trace,
    assert_raises,
)
import socket
from api.sip.client import (
    CannotReceiveMockSIPClient,
    CannotSendMockSIPClient,
    MockSIPClient,
    SIPClient,
)

class MockSocket(object):
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.timeout = None
        self.connected_to = None

    def connect(self, server_and_port):
        self.connected_to = server_and_port

    def settimeout(self, value):
        self.timeout = value


class TestSIPClient(object):
    """Test the real SIPClient class without allowing it to make
    network connections.
    """

    def test_connect(self):
        target_server = object()
        sip = SIPClient(target_server, 999, connect=False)

        old_socket = socket.socket

        # Mock the socket.socket function.
        socket.socket = MockSocket

        # Call connect() and make sure timeout is set properly.
        try:
            connection = sip.connect()
            eq_(12, connection.timeout)
        finally:
            # Un-mock the socket.socket function
            socket.socket = old_socket


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
        # The first request includes a sequence ID field, "AY", with
        # the value "0".
        eq_('9300CNuser_id|COpassword|AY0AZF556\r', req1)

        # The second request does not include a sequence ID field. As
        # a consequence its checksum is different.
        eq_('9300CNuser_id|COpassword|AZF620\r', req2)

        # The login request eventually succeeded.
        eq_({'login_ok': '1', '_status': '94'}, response)


class TestNetworkError(object):

    def test_retry_on_initial_ioerror(self):
        sip = CannotSendMockSIPClient()

        # When we try to send any data through the client, we get an
        # IOError.
        assert_raises(IOError, sip.login, 'username', 'password', 'location')
        
        # But after the initial failure we created a new socket
        # connection and sent the data again, so at least we tried.
        expect = ['Creating new socket connection.',
                  "I was unable to send data."] * 2
        eq_(expect, sip.status)
        
    def test_retry_on_initial_timeout(self):
        sip = CannotReceiveMockSIPClient()

        # When we try to send any data through the client, we get an
        # exception.
        assert_raises(
            socket.timeout, sip.login, 'username', 'password', 'location'
        )

        # But after the initial failure we created a new socket
        # connection and sent the data again, so at least we tried.
        expect = ['Creating new socket connection.',
                  "I was unable to read data."] * 2
        eq_(expect, sip.status)


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
        # We're not logged in, and we must log in before sending a real
        # message.
        eq_(False, sip.logged_in)
        eq_(True, sip.must_log_in)
        
        sip.queue_response('941')
        sip.queue_response('64Y                201610050000114734                        AOnypl |AA12345|AENo Name|BLN|AFYour library card number cannot be located.  Please see a staff member for assistance.|AY1AZC9DE')
        response = sip.patron_information('patron_identifier')

        # Two requests were made.
        eq_(2, len(sip.requests))
        eq_(2, sip.sequence_number)

        # We're logged in.
        eq_(True, sip.logged_in)
        
        # We ended up with the right data.
        eq_('12345', response['patron_identifier'])

        # If we reset the connection, we stop being logged in.
        sip.connect()
        eq_(False, sip.logged_in)
        eq_(0, sip.sequence_number)
        
    def test_login_failure_interrupts_other_request(self):
        sip = MockSIPClient('user_id', 'password')
        sip.queue_response('940')

        # We don't even get a chance to make the patron information request
        # because our login attempt fails.
        assert_raises(IOError,  sip.patron_information, 'patron_identifier')
        
    def test_login_does_not_happen_implicitly_when_user_id_and_password_not_specified(self):
        sip = MockSIPClient()

        # We're implicitly logged in.
        eq_(False, sip.must_log_in)
        eq_(True, sip.logged_in)

        sip.queue_response('64Y                201610050000114734                        AOnypl |AA12345|AENo Name|BLN|AFYour library card number cannot be located.  Please see a staff member for assistance.|AY1AZC9DE')
        response = sip.patron_information('patron_identifier')

        # One request was made.
        eq_(1, len(sip.requests))
        eq_(1, sip.sequence_number)
        
        # We ended up with the right data.
        eq_('12345', response['patron_identifier'])


class TestPatronResponse(object):

    def setup(self):
        self.sip = MockSIPClient()
    
    def test_incorrect_card_number(self):
        self.sip.queue_response("64Y                201610050000114734                        AOnypl |AA240|AENo Name|BLN|AFYour library card number cannot be located.|AY1AZC9DE")
        response = self.sip.patron_information('identifier')

        # Test some of the basic fields.
        eq_(response['institution_id'], 'nypl ')
        eq_(response['personal_name'], 'No Name')
        eq_(response['screen_message'], ['Your library card number cannot be located.'])
        eq_(response['valid_patron'], 'N')
        eq_(response['patron_status'], 'Y             ')
        parsed = response['patron_status_parsed']
        eq_(True, parsed['charge privileges denied'])
        eq_(False, parsed['too many items charged'])
        
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
        # as sipserver_internal_id.
        eq_("86371", response['sipserver_internal_id'])

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

    def test_location_code_is_optional(self):
        """You can specify a location_code when logging in, or not."""
        without_code = self.sip.login_message(
            "login_id", "login_password"
        )
        assert without_code.endswith("COlogin_password")
        with_code = self.sip.login_message(
            "login_id", "login_password", "location_code"
        )
        assert with_code.endswith("COlogin_password|CPlocation_code")
        
    def test_patron_password_is_optional(self):
        without_password = self.sip.patron_information_request(
            "patron_identifier"
        )
        assert without_password.endswith('AApatron_identifier|AC')
        with_password = self.sip.patron_information_request(
            "patron_identifier", "patron_password"
        )
        assert with_password.endswith(
            'AApatron_identifier|AC|ADpatron_password'
        )

    def test_parse_patron_status(self):
        m = MockSIPClient.parse_patron_status
        assert_raises(ValueError, m, None)
        assert_raises(ValueError, m, "")
        assert_raises(ValueError, m, " " * 20)
        parsed = m("Y Y Y Y Y Y Y ")
        for yes in [
                'charge privileges denied',
                #'renewal privileges denied',
                'recall privileges denied',
                #'hold privileges denied',
                'card reported lost',
                #'too many items charged',
                'too many items overdue',
                #'too many renewals',
                'too many claims of items returned',
                #'too many items lost',
                'excessive outstanding fines',
                #'excessive outstanding fees',
                'recall overdue',
                #'too many items billed',
        ]:
            eq_(parsed[yes], True)

        for no in [
                #'charge privileges denied',
                'renewal privileges denied',
                #'recall privileges denied',
                'hold privileges denied',
                #'card reported lost',
                'too many items charged',
                #'too many items overdue',
                'too many renewals',
                #'too many claims of items returned',
                'too many items lost',
                #'excessive outstanding fines',
                'excessive outstanding fees',
                #'recall overdue',
                'too many items billed',
        ]:
            eq_(parsed[no], False)
