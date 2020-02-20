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
import json
from core.config import CannotLoadConfiguration

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

    end_session_response = "36Y201610210000142637AO3|AA25891000331441|AF|AG"

    def test_initialize_from_integration(self):
        p = SIP2AuthenticationProvider
        integration = self._external_integration(self._str)
        integration.url = "server.com"
        integration.username = "user1"
        integration.password = "pass1"
        integration.setting(p.FIELD_SEPARATOR).value = "\t"
        integration.setting(p.INSTITUTION_ID).value = "MAIN"
        provider = p(self._default_library, integration)

        # A SIPClient was initialized based on the integration values.
        eq_("user1", provider.login_user_id)
        eq_("pass1", provider.login_password)
        eq_("\t", provider.field_separator)
        eq_("MAIN", provider.institution_id)
        eq_("server.com", provider.server)

        # Default port is 6001.
        eq_(None, provider.port)

        # Try again, specifying a port.
        integration.setting(p.PORT).value = "1234"
        provider = p(self._default_library, integration)
        eq_(1234, provider.port)

    def test_remote_authenticate(self):
        integration = self._external_integration(self._str)
        client = MockSIPClient()
        auth = SIP2AuthenticationProvider(
            self._default_library, integration, client=client
        )

        # Some examples taken from a Sierra SIP API.
        client.queue_response(self.sierra_valid_login)
        client.queue_response(self.end_session_response)
        patrondata = auth.remote_authenticate("user", "pass")
        eq_("12345", patrondata.authorization_identifier)
        eq_("foo@example.com", patrondata.email_address)
        eq_("SHELDON, ALICE", patrondata.personal_name)
        eq_(0, patrondata.fines)
        eq_(None, patrondata.authorization_expires)
        eq_(None, patrondata.external_type)
        eq_(PatronData.NO_VALUE, patrondata.block_reason)

        client.queue_response(self.sierra_invalid_login)
        client.queue_response(self.end_session_response)
        eq_(None, auth.remote_authenticate("user", "pass"))

        # Since Sierra provides both the patron's fine amount and the
        # maximum allowable amount, we can determine just by looking
        # at the SIP message that this patron is blocked for excessive
        # fines.
        client.queue_response(self.sierra_excessive_fines)
        client.queue_response(self.end_session_response)
        patrondata = auth.remote_authenticate("user", "pass")
        eq_(PatronData.EXCESSIVE_FINES, patrondata.block_reason)

        # A patron with an expired card.
        client.queue_response(self.evergreen_expired_card)
        client.queue_response(self.end_session_response)
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
        client.queue_response(self.end_session_response)
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
        client.queue_response(self.end_session_response)
        patrondata = auth.remote_authenticate("user", "pass")
        eq_(PatronData.NO_VALUE, patrondata.block_reason)

        client.queue_response(self.evergreen_card_reported_lost)
        client.queue_response(self.end_session_response)
        patrondata = auth.remote_authenticate("user", "pass")
        eq_(PatronData.CARD_REPORTED_LOST, patrondata.block_reason)

        # Some examples taken from a Polaris instance.
        client.queue_response(self.polaris_valid_pin)
        client.queue_response(self.end_session_response)
        patrondata = auth.remote_authenticate("user", "pass")
        eq_("25891000331441", patrondata.authorization_identifier)
        eq_("foo@bar.com", patrondata.email_address)
        eq_(9.25, patrondata.fines)
        eq_("Falk, Jen", patrondata.personal_name)
        eq_(datetime(2018, 6, 9, 23, 59, 59),
            patrondata.authorization_expires)

        client.queue_response(self.polaris_wrong_pin)
        client.queue_response(self.end_session_response)
        patrondata = auth.remote_authenticate("user", "pass")
        eq_(None, patrondata)

        client.queue_response(self.polaris_expired_card)
        client.queue_response(self.end_session_response)
        patrondata = auth.remote_authenticate("user", "pass")
        eq_(datetime(2016, 10, 25, 23, 59, 59),
            patrondata.authorization_expires)

        client.queue_response(self.polaris_excess_fines)
        client.queue_response(self.end_session_response)
        patrondata = auth.remote_authenticate("user", "pass")
        eq_(11.50, patrondata.fines)

        # Two cases where the patron's authorization identifier was
        # just not recognized. One on an ILS that sets
        # valid_patron_password='N' when that happens.
        client.queue_response(self.polaris_no_such_patron)
        client.queue_response(self.end_session_response)
        patrondata = auth.remote_authenticate("user", "pass")
        eq_(None, patrondata)

        # And once on an ILS that leaves valid_patron_password blank
        # when that happens.
        client.queue_response(self.tlc_no_such_patron)
        client.queue_response(self.end_session_response)
        patrondata = auth.remote_authenticate("user", "pass")
        eq_(None, patrondata)

    def test_remote_authenticate_no_password(self):

        integration = self._external_integration(self._str)
        p = SIP2AuthenticationProvider
        integration.setting(p.PASSWORD_KEYBOARD).value = p.NULL_KEYBOARD
        client = MockSIPClient()
        auth = SIP2AuthenticationProvider(
            self._default_library, integration, client=client
        )

        # This Evergreen instance doesn't use passwords.
        client.queue_response(self.evergreen_active_user)
        client.queue_response(self.end_session_response)
        patrondata = auth.remote_authenticate("user", None)
        eq_("12345", patrondata.authorization_identifier)
        eq_("863715", patrondata.permanent_id)
        eq_("Booth Active Test", patrondata.personal_name)
        eq_(0, patrondata.fines)
        eq_(datetime(2019, 10, 4), patrondata.authorization_expires)
        eq_("Adult", patrondata.external_type)

        # If a password is specified, it is not sent over the wire.
        client.queue_response(self.evergreen_active_user)
        client.queue_response(self.end_session_response)
        patrondata = auth.remote_authenticate("user2", "some password")
        eq_("12345", patrondata.authorization_identifier)
        request = client.requests[-1]
        assert 'user2' in request
        assert 'some password' not in request

    def test_ioerror_during_connect_becomes_remoteintegrationexception(self):
        """If the IP of the circulation manager has not been whitelisted,
        we generally can't even connect to the server.
        """
        class CannotConnect(MockSIPClient):
            def connect(self):
                raise IOError("Doom!")

        client = CannotConnect()
        integration = self._external_integration(self._str)
        provider = SIP2AuthenticationProvider(self._default_library, integration, client=client)

        assert_raises_regexp(
            RemoteIntegrationException,
            "Error accessing unknown server: Doom!",
            provider.remote_authenticate,
            "username", "password",
        )

    def test_ioerror_during_send_becomes_remoteintegrationexception(self):
        """If there's an IOError communicating with the server,
        it becomes a RemoteIntegrationException.
        """
        class CannotSend(MockSIPClient):
            def do_send(self, data):
                raise IOError("Doom!")

        integration = self._external_integration(self._str)
        integration.url = 'server.local'
        client = CannotSend()
        provider = SIP2AuthenticationProvider(
            self._default_library, integration, client=client
        )
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

    def test_remote_patron_lookup(self):
        #When the SIP authentication provider needs to look up a patron,
        #it calls patron_information on its SIP client and passes in None
        #for the password.
        patron = self._patron()
        patron.authorization_identifier = "1234"
        integration = self._external_integration(self._str)
        class Mock(MockSIPClient):
            def patron_information(self, identifier, password):
                self.patron_information = identifier
                self.password = password
                return self.patron_information_parser(TestSIP2AuthenticationProvider.polaris_wrong_pin)

        client = Mock()
        client.queue_response(self.end_session_response)
        auth = SIP2AuthenticationProvider(
            self._default_library, integration, client=client
        )
        patron = auth._remote_patron_lookup(patron)
        eq_(patron.__class__, PatronData)
        eq_("25891000331441", patron.authorization_identifier)
        eq_("foo@bar.com", patron.email_address)
        eq_(9.25, patron.fines)
        eq_("Falk, Jen", patron.personal_name)
        eq_(datetime(2018, 6, 9, 23, 59, 59), patron.authorization_expires)
        eq_(client.patron_information, "1234")
        eq_(client.password, None)

    def test_info_to_patrondata_validate_password(self):
        integration = self._external_integration(self._str)
        integration.url = 'server.local'
        client = MockSIPClient()
        provider = SIP2AuthenticationProvider(
            self._default_library, integration, client=client
        )

        # Test with valid login, should return PatronData
        info = client.patron_information_parser(TestSIP2AuthenticationProvider.sierra_valid_login)
        patron = provider.info_to_patrondata(info)
        eq_(patron.__class__, PatronData)
        eq_("12345", patron.authorization_identifier)
        eq_("foo@example.com", patron.email_address)
        eq_("SHELDON, ALICE", patron.personal_name)
        eq_(0, patron.fines)
        eq_(None, patron.authorization_expires)
        eq_(None, patron.external_type)
        eq_(PatronData.NO_VALUE, patron.block_reason)

        # Test with invalid login, should return None
        info = client.patron_information_parser(TestSIP2AuthenticationProvider.sierra_invalid_login)
        patron = provider.info_to_patrondata(info)
        eq_(None, patron)

    def test_info_to_patrondata_no_validate_password(self):
        integration = self._external_integration(self._str)
        integration.url = 'server.local'
        client = MockSIPClient()
        provider = SIP2AuthenticationProvider(
            self._default_library, integration, client=client
        )

        # Test with valid login, should return PatronData
        info = client.patron_information_parser(TestSIP2AuthenticationProvider.sierra_valid_login)
        patron = provider.info_to_patrondata(info, validate_password=False)
        eq_(patron.__class__, PatronData)
        eq_("12345", patron.authorization_identifier)
        eq_("foo@example.com", patron.email_address)
        eq_("SHELDON, ALICE", patron.personal_name)
        eq_(0, patron.fines)
        eq_(None, patron.authorization_expires)
        eq_(None, patron.external_type)
        eq_(PatronData.NO_VALUE, patron.block_reason)

        # Test with invalid login, should return PatronData
        info = client.patron_information_parser(TestSIP2AuthenticationProvider.sierra_invalid_login)
        patron = provider.info_to_patrondata(info, validate_password=False)
        eq_(patron.__class__, PatronData)
        eq_("12345", patron.authorization_identifier)
        eq_("foo@example.com", patron.email_address)
        eq_("SHELDON, ALICE", patron.personal_name)
        eq_(0, patron.fines)
        eq_(None, patron.authorization_expires)
        eq_(None, patron.external_type)
        eq_('no borrowing privileges', patron.block_reason)

    def test_patron_block_setting(self):
        integration_block = self._external_integration(self._str, settings={SIP2AuthenticationProvider.PATRON_STATUS_BLOCK: "true"})
        integration_noblock = self._external_integration(self._str, settings={SIP2AuthenticationProvider.PATRON_STATUS_BLOCK: "false"})
        client = MockSIPClient()

        # Test with blocked patron, block should be set
        p = SIP2AuthenticationProvider(self._default_library, integration_block, client=client)
        info = client.patron_information_parser(TestSIP2AuthenticationProvider.evergreen_expired_card)
        patron = p.info_to_patrondata(info)
        eq_(patron.__class__, PatronData)
        eq_("12345", patron.authorization_identifier)
        eq_("863716", patron.permanent_id)
        eq_("Booth Expired Test", patron.personal_name)
        eq_(0, patron.fines)
        eq_(datetime(2008, 9, 7), patron.authorization_expires)
        eq_(PatronData.NO_BORROWING_PRIVILEGES, patron.block_reason)

        # Test with blocked patron, block should not be set
        p = SIP2AuthenticationProvider(self._default_library, integration_noblock, client=client)
        info = client.patron_information_parser(TestSIP2AuthenticationProvider.evergreen_expired_card)
        patron = p.info_to_patrondata(info)
        eq_(patron.__class__, PatronData)
        eq_("12345", patron.authorization_identifier)
        eq_("863716", patron.permanent_id)
        eq_("Booth Expired Test", patron.personal_name)
        eq_(0, patron.fines)
        eq_(datetime(2008, 9, 7), patron.authorization_expires)
        eq_(PatronData.NO_VALUE, patron.block_reason)

    def test_run_self_tests(self):
        integration = self._external_integration(self._str)
        integration.url = "server.com"

        class MockBadConnection(MockSIPClient):
            def connect(self):
                # probably a timeout if the server or port values are not valid
                raise IOError("Could not connect")

        class MockSIPLogin(MockSIPClient):
            def now(self):
                return datetime(2019, 1, 1).strftime("%Y%m%d0000%H%M%S")
            def login(self):
                if not self.login_user_id and not self.login_password:
                    raise IOError("Error logging in")
            def patron_information(self, username, password):
                return self.patron_information_parser(TestSIP2AuthenticationProvider.sierra_valid_login)

        badConnectionClient = MockBadConnection()
        auth = SIP2AuthenticationProvider(
            self._default_library, integration, client=badConnectionClient
        )
        results = [r for r in auth._run_self_tests(self._db)]

        # If the connection doesn't work then don't bother running the other tests
        eq_(len(results), 1)
        eq_(results[0].name, "Test Connection")
        eq_(results[0].success, False)
        assert(results[0].exception, IOError("Could not connect"))

        badLoginClient = MockSIPLogin()
        auth = SIP2AuthenticationProvider(
            self._default_library, integration, client=badLoginClient
        )
        results = [x for x in auth._run_self_tests(self._db)]

        eq_(len(results), 2)
        eq_(results[0].name, "Test Connection")
        eq_(results[0].success, True)

        eq_(results[1].name, "Test Login with username 'None' and password 'None'")
        eq_(results[1].success, False)
        assert(results[1].exception, IOError("Error logging in"))

        # Set the log in username and password
        integration.username = "user1"
        integration.password = "pass1"
        goodLoginClient = MockSIPLogin(login_user_id="user1", login_password="pass1")
        auth = SIP2AuthenticationProvider(
            self._default_library, integration, client=goodLoginClient
        )
        results = [x for x in auth._run_self_tests(self._db)]

        eq_(len(results), 3)
        eq_(results[0].name, "Test Connection")
        eq_(results[0].success, True)

        eq_(results[1].name, "Test Login with username 'user1' and password 'pass1'")
        eq_(results[1].success, True)

        eq_(results[2].name, "Authenticating test patron")
        eq_(results[2].success, False)
        assert(results[2].exception, CannotLoadConfiguration("No test patron identifier is configured."))


        # Now add the test patron credentials into the mocked client and SIP2 authenticator provider
        patronDataClient = MockSIPLogin(login_user_id="user1", login_password="pass1")
        valid_login_patron = patronDataClient.patron_information_parser(TestSIP2AuthenticationProvider.sierra_valid_login)
        class MockSIP2PatronInformation(SIP2AuthenticationProvider):
            def patron_information(self, username, password):
                return valid_login_patron

        auth = MockSIP2PatronInformation(
            self._default_library, integration, client=patronDataClient
        )
        # The actual test patron credentials
        auth.test_username = "usertest1"
        auth.test_password = "userpassword1"
        results = [x for x in auth._run_self_tests(self._db)]

        eq_(len(results), 6)
        eq_(results[0].name, "Test Connection")
        eq_(results[0].success, True)

        eq_(results[1].name, "Test Login with username 'user1' and password 'pass1'")
        eq_(results[1].success, True)

        eq_(results[2].name, "Authenticating test patron")
        eq_(results[2].success, True)

        # Since test patron authentication is true, we can now see self
        # test results for syncing metadata and the raw data from `patron_information`
        eq_(results[3].name, "Syncing patron metadata")
        eq_(results[3].success, True)

        eq_(results[4].name, "Patron information request")
        eq_(results[4].success, True)
        eq_(results[4].result, patronDataClient.patron_information_request("usertest1", "userpassword1"))

        eq_(results[5].name, "Raw test patron information")
        eq_(results[5].success, True)
        eq_(results[5].result, json.dumps(valid_login_patron, indent=1))

