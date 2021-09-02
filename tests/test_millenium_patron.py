import pkgutil
from datetime import date, timedelta
from decimal import Decimal
import json
from urllib import parse

import pytest
from api.config import (
    CannotLoadConfiguration,
    Configuration,
)
from core.model import ConfigurationSetting
from api.authenticator import PatronData
from api.millenium_patron import MilleniumPatronAPI
from core.testing import DatabaseTest
from core.util.datetime_helpers import (
    utc_now
)
from . import sample_data

class MockResponse(object):
    def __init__(self, content):
        self.status_code = 200
        self.content = content

class MockAPI(MilleniumPatronAPI):

    def __init__(self, library_id, integration):
        super(MockAPI, self).__init__(library_id, integration)
        self.queue = []
        self.requests_made = []

    def sample_data(self, filename):
        return sample_data(filename, 'millenium_patron')

    def enqueue(self, filename):
        data = self.sample_data(filename)
        self.queue.append(data)

    def request(self, *args, **kwargs):
        self.requests_made.append((args, kwargs))
        response = self.queue[0]
        self.queue = self.queue[1:]
        return MockResponse(response)


class TestMilleniumPatronAPI(DatabaseTest):

    def mock_api(self, url="http://url/", blacklist=[], auth_mode=None, verify_certificate=True,
                 block_types=None, password_keyboard=None, library_identifier_field=None,
                 neighborhood_mode=None
    ):
        integration = self._external_integration(self._str)
        integration.url = url
        integration.setting(MilleniumPatronAPI.IDENTIFIER_BLACKLIST).value = json.dumps(blacklist)
        integration.setting(MilleniumPatronAPI.VERIFY_CERTIFICATE).value = json.dumps(verify_certificate)
        if block_types:
            integration.setting(MilleniumPatronAPI.BLOCK_TYPES).value = block_types

        if auth_mode:
            integration.setting(MilleniumPatronAPI.AUTHENTICATION_MODE).value = auth_mode
        if neighborhood_mode:
            integration.setting(MilleniumPatronAPI.NEIGHBORHOOD_MODE).value = neighborhood_mode
        if password_keyboard:
            integration.setting(MilleniumPatronAPI.PASSWORD_KEYBOARD).value = password_keyboard

        if library_identifier_field:
            ConfigurationSetting.for_library_and_externalintegration(
                self._db, MilleniumPatronAPI.LIBRARY_IDENTIFIER_FIELD,
                self._default_library, integration
            ).value = library_identifier_field

        return MockAPI(self._default_library, integration)

    def setup_method(self):
        super(TestMilleniumPatronAPI, self).setup_method()
        self.api = self.mock_api("http://url/")

    def test_constructor(self):
        api = self.mock_api("http://example.com/", ["a", "b"])
        assert "http://example.com/" == api.root
        assert ["a", "b"] == [x.pattern for x in api.blacklist]

        with pytest.raises(CannotLoadConfiguration) as excinfo:
            self.mock_api(neighborhood_mode="nope")
        assert "Unrecognized Millenium Patron API neighborhood mode: nope." in str(excinfo.value)

    def test__remote_patron_lookup_no_such_patron(self):
        self.api.enqueue("dump.no such barcode.html")
        patrondata = PatronData(authorization_identifier="bad barcode")
        assert None == self.api._remote_patron_lookup(patrondata)

    def test__remote_patron_lookup_success(self):
        self.api.enqueue("dump.success.html")
        patrondata = PatronData(authorization_identifier="good barcode")
        patrondata = self.api._remote_patron_lookup(patrondata)

        # Although "good barcode" was successful in lookup this patron
        # up, it didn't show up in their patron dump as a barcode, so
        # the authorization_identifier from the patron dump took
        # precedence.
        assert "6666666" == patrondata.permanent_id
        assert "44444444444447" == patrondata.authorization_identifier
        assert "alice" == patrondata.username
        assert Decimal(0) == patrondata.fines
        assert date(2059, 4, 1) == patrondata.authorization_expires
        assert "SHELDON, ALICE" == patrondata.personal_name
        assert "alice@sheldon.com" == patrondata.email_address
        assert PatronData.NO_VALUE == patrondata.block_reason

    def test__remote_patron_lookup_barcode_spaces(self):
        self.api.enqueue("dump.success_barcode_spaces.html")
        patrondata = PatronData(authorization_identifier="44444444444447")
        patrondata = self.api._remote_patron_lookup(patrondata)
        assert "44444444444447" == patrondata.authorization_identifier
        assert ["44444444444447", "4 444 4444 44444 7"] == patrondata.authorization_identifiers

    def test__remote_patron_lookup_block_rules(self):
        """This patron has a value of "m" in MBLOCK[56], which generally
        means they are blocked.
        """
        # Default behavior -- anything other than '-' means blocked.
        self.api.enqueue("dump.blocked.html")
        patrondata = PatronData(authorization_identifier="good barcode")
        patrondata = self.api._remote_patron_lookup(patrondata)
        assert PatronData.UNKNOWN_BLOCK == patrondata.block_reason

        # If we set custom block types that say 'm' doesn't really
        # mean the patron is blocked, they're not blocked.
        api = self.mock_api(block_types='abcde')
        api.enqueue("dump.blocked.html")
        patrondata = PatronData(authorization_identifier="good barcode")
        patrondata = api._remote_patron_lookup(patrondata)
        assert PatronData.NO_VALUE == patrondata.block_reason

        # If we set custom block types that include 'm', the patron
        # is blocked.
        api = self.mock_api(block_types='lmn')
        api.enqueue("dump.blocked.html")
        patrondata = PatronData(authorization_identifier="good barcode")
        patrondata = api._remote_patron_lookup(patrondata)
        assert PatronData.UNKNOWN_BLOCK == patrondata.block_reason

    def test_parse_poorly_behaved_dump(self):
        """The HTML parser is able to handle HTML embedded in
        field values.
        """
        self.api.enqueue("dump.embedded_html.html")
        patrondata = PatronData(authorization_identifier="good barcode")
        patrondata = self.api._remote_patron_lookup(patrondata)
        assert "abcd" == patrondata.authorization_identifier

    def test_incoming_authorization_identifier_retained(self):
        # This patron has two barcodes.
        dump = self.api.sample_data("dump.two_barcodes.html")

        # Let's say they authenticate with the first one.
        patrondata = self.api.patron_dump_to_patrondata("FIRST-barcode", dump)
        # Their Patron record will use their first barcode as authorization
        # identifier, because that's what they typed in.
        assert "FIRST-barcode" == patrondata.authorization_identifier

        # Let's say they authenticate with the second barcode.
        patrondata = self.api.patron_dump_to_patrondata("SECOND-barcode", dump)
        # Their Patron record will use their second barcode as authorization
        # identifier, because that's what they typed in.
        assert "SECOND-barcode" == patrondata.authorization_identifier

        # Let's say they authenticate with a username.
        patrondata = self.api.patron_dump_to_patrondata(
            "username", dump
        )
        # Their Patron record will suggest the second barcode as
        # authorization identifier, because it's likely to be the most
        # recently added one.
        assert "SECOND-barcode" == patrondata.authorization_identifier

    def test_remote_authenticate_no_such_barcode(self):
        self.api.enqueue("pintest.no such barcode.html")
        assert False == self.api.remote_authenticate("wrong barcode", "pin")

    def test_remote_authenticate_wrong_pin(self):
        self.api.enqueue("pintest.bad.html")
        assert False == self.api.remote_authenticate("barcode", "wrong pin")

    def test_remote_authenticate_correct_pin(self):
        self.api.enqueue("pintest.good.html")
        barcode = "barcode1234567!"
        pin = "!correct pin<>@"
        patrondata = self.api.remote_authenticate(barcode, pin)
        # The return value includes everything we know about the
        # authenticated patron, which isn't much.
        assert "barcode1234567!" == patrondata.authorization_identifier

        # The PIN went out URL-encoded. The barcode did not.
        [args, kwargs] = self.api.requests_made.pop()
        [url] = args
        assert kwargs == {}
        assert url == 'http://url/%s/%s/pintest' % (barcode, parse.quote(pin))

    def test_authentication_updates_patron_authorization_identifier(self):
        """Verify that Patron.authorization_identifier is updated when
        necessary and left alone when not necessary.

        This is an end-to-end test. Its components are tested in
        test_authenticator.py (especially TestPatronData) and
        elsewhere in this file. In theory, this test can be removed,
        but it has exposed bugs before.
        """
        p = self._patron()
        p.external_identifier = "6666666"

        # If the patron is new, and logged in with a username, we'll
        # use the last barcode in the list as their authorization
        # identifier.
        p.authorization_identifier = None
        p.last_external_sync = None
        self.api.enqueue("pintest.good.html")
        self.api.enqueue("dump.two_barcodes.html")
        p2 = self.api.authenticated_patron(self._db, dict(username="alice"))
        assert p2 == p
        assert "SECOND-barcode" == p.authorization_identifier

        # If the patron is new, and logged in with a barcode, their
        # authorization identifier will be the barcode they used.
        p.authorization_identifier = None
        self.api.enqueue("pintest.good.html")
        self.api.enqueue("dump.two_barcodes.html")
        self.api.authenticated_patron(self._db, dict(username="FIRST-barcode"))
        assert "FIRST-barcode" == p.authorization_identifier

        p.authorization_identifier = None
        self.api.enqueue("pintest.good.html")
        self.api.enqueue("dump.two_barcodes.html")
        self.api.authenticated_patron(self._db, dict(username="SECOND-barcode"))
        assert "SECOND-barcode" == p.authorization_identifier

        # If the patron authorizes with their username, we will leave
        # their authorization identifier alone.
        p.authorization_identifier = "abcd"
        self.api.enqueue("pintest.good.html")
        self.api.authenticated_patron(self._db, dict(username="alice"))
        assert "abcd" == p.authorization_identifier
        assert "alice" == p.username

        # If the patron authorizes with an unrecognized identifier
        # that is not their username, we will immediately sync their
        # metadata with the server. This can correct a case like the
        # one where the patron's authorization identifier is
        # incorrectly set to their username.
        p.authorization_identifier = "alice"
        self.api.enqueue("pintest.good.html")
        self.api.enqueue("dump.two_barcodes.html")
        self.api.authenticated_patron(self._db, dict(username="FIRST-barcode"))
        assert "FIRST-barcode" == p.authorization_identifier

        # Or to the case where the patron's authorization identifier is
        # simply not used anymore.
        p.authorization_identifier = "OLD-barcode"
        self.api.enqueue("pintest.good.html")
        self.api.enqueue("dump.two_barcodes.html")
        self.api.authenticated_patron(self._db, dict(username="SECOND-barcode"))
        assert "SECOND-barcode" == p.authorization_identifier

        # If the patron has an authorization identifier, and it _is_
        # one of their barcodes, we'll keep it.
        p.authorization_identifier = "FIRST-barcode"
        self.api.enqueue("pintest.good.html")
        self.api.enqueue("dump.two_barcodes.html")
        self.api.authenticated_patron(self._db, dict(username="alice"))
        assert "FIRST-barcode" == p.authorization_identifier

        # We'll keep the patron's authorization identifier constant
        # even if the patron has started authenticating with some
        # other identifier.  Third-party services may be tracking the
        # patron with this authorization identifier, and changing it
        # could cause them to lose books.
        #
        # TODO: Keeping a separate field for 'identifier we send to
        # third-party services that don't check the ILS', and using
        # the permanent ID in there, would alleviate this problem for
        # new patrons.
        p.authorization_identifier = "SECOND-barcode"
        self.api.enqueue("pintest.good.html")
        self.api.enqueue("dump.two_barcodes.html")
        self.api.authenticated_patron(self._db, dict(username="FIRST-barcode"))
        assert "SECOND-barcode" == p.authorization_identifier

    def test_authenticated_patron_success(self):
        """This test can probably be removed -- it mostly tests functionality
        from BasicAuthAuthenticator.
        """
        # Patron is valid, but not in our database yet
        self.api.enqueue("pintest.good.html")
        self.api.enqueue("dump.success.html")
        alice = self.api.authenticate(
            self._db, dict(username="alice", password="4444")
        )
        assert "44444444444447" == alice.authorization_identifier
        assert "alice" == alice.username

        # Create another patron who has a different barcode and username,
        # to verify that our authentication mechanism chooses the right patron
        # and doesn't look up whoever happens to be in the database.
        p = self._patron()
        p.username = 'notalice'
        p.authorization_identifier='111111111111'
        self._db.commit()

        # Patron is in the db, now authenticate with barcode
        self.api.enqueue("pintest.good.html")
        alice = self.api.authenticated_patron(self._db, dict(username="44444444444447", password="4444"))
        assert "44444444444447" == alice.authorization_identifier
        assert "alice" == alice.username

        # Authenticate with username again
        self.api.enqueue("pintest.good.html")
        alice = self.api.authenticated_patron(self._db, dict(username="alice", password="4444"))
        assert "44444444444447" == alice.authorization_identifier
        assert "alice" == alice.username

    def test_authenticated_patron_renewed_card(self):
        """This test can be removed -- authenticated_patron is
        tested in test_authenticator.py.
        """
        now = utc_now()
        one_hour_ago = now - timedelta(seconds=3600)
        one_week_ago = now - timedelta(days=7)

        # Patron is in the database.
        p = self._patron()
        p.authorization_identifier = "44444444444447"

        # We checked them against the ILS one hour ago.
        p.last_external_sync = one_hour_ago

        # Normally, calling authenticated_patron only performs a sync
        # and updates last_external_sync if the last sync was twelve
        # hours ago.
        self.api.enqueue("pintest.good.html")
        auth = dict(username="44444444444447", password="4444")
        p2 = self.api.authenticated_patron(self._db, auth)
        assert p2 == p
        assert p2.last_external_sync == one_hour_ago

        # However, if the card has expired, a sync is performed every
        # few seconds.
        ten_seconds_ago = now - timedelta(seconds=10)
        p.authorization_expires = one_week_ago
        p.last_external_sync = ten_seconds_ago
        self.api.enqueue("pintest.good.html")
        self.api.enqueue("dump.success.html")
        p2 = self.api.authenticated_patron(self._db, auth)
        assert p2 == p

        # Since the sync was performed, last_external_sync was updated.
        assert p2.last_external_sync > one_hour_ago

        # And the patron's card is no longer expired.
        expiration = date(2059, 4, 1)
        assert expiration == p.authorization_expires

    def test_authentication_patron_invalid_expiration_date(self):
        p = self._patron()
        p.authorization_identifier = "44444444444447"
        self.api.enqueue("pintest.good.html")
        self.api.enqueue("dump.invalid_expiration.html")
        auth = dict(username="44444444444447", password="4444")
        p2 = self.api.authenticated_patron(self._db, auth)
        assert p2 == p
        assert None == p.authorization_expires

    def test_authentication_patron_invalid_fine_amount(self):
        p = self._patron()
        p.authorization_identifier = "44444444444447"
        self.api.enqueue("pintest.good.html")
        self.api.enqueue("dump.invalid_fines.html")
        auth = dict(username="44444444444447", password="4444")
        p2 = self.api.authenticated_patron(self._db, auth)
        assert p2 == p
        assert 0 == p.fines

    def test_patron_dump_to_patrondata(self):
        content = self.api.sample_data("dump.success.html")
        patrondata = self.api.patron_dump_to_patrondata('alice', content)
        assert "44444444444447" == patrondata.authorization_identifier
        assert "alice" == patrondata.username
        assert None == patrondata.library_identifier

    def test_patron_dump_to_patrondata_restriction_field(self):
        api = self.mock_api(library_identifier_field="HOME LIBR[p53]")
        content = api.sample_data("dump.success.html")
        patrondata = api.patron_dump_to_patrondata('alice', content)
        assert "mm" == patrondata.library_identifier
        api = self.mock_api(library_identifier_field="P TYPE[p47]")
        content = api.sample_data("dump.success.html")
        patrondata = api.patron_dump_to_patrondata('alice', content)
        assert "10" == patrondata.library_identifier

    def test_neighborhood(self):
        # The value of PatronData.neighborhood depends on the 'neighborhood mode' setting.

        # Default behavior is not to gather neighborhood information at all.
        api = self.mock_api()
        content = api.sample_data("dump.success.html")
        patrondata = api.patron_dump_to_patrondata('alice', content)
        assert PatronData.NO_VALUE == patrondata.neighborhood

        # Patron neighborhood may be the identifier of their home library branch.
        api = self.mock_api(neighborhood_mode=MilleniumPatronAPI.HOME_BRANCH_NEIGHBORHOOD_MODE)
        content = api.sample_data("dump.success.html")
        patrondata = api.patron_dump_to_patrondata('alice', content)
        assert "mm" == patrondata.neighborhood

        # Or it may be the ZIP code of their home address.
        api = self.mock_api(neighborhood_mode=MilleniumPatronAPI.POSTAL_CODE_NEIGHBORHOOD_MODE)
        patrondata = api.patron_dump_to_patrondata('alice', content)
        assert "10001" == patrondata.neighborhood


    def test_authorization_identifier_blacklist(self):
        """A patron has two authorization identifiers. Ordinarily the second
        one (which would normally be preferred), but it contains a
        blacklisted string, so the first takes precedence.
        """
        content = self.api.sample_data("dump.two_barcodes.html")
        patrondata = self.api.patron_dump_to_patrondata('alice', content)
        assert "SECOND-barcode" == patrondata.authorization_identifier

        api = self.mock_api(blacklist=["second"])
        patrondata = api.patron_dump_to_patrondata('alice', content)
        assert "FIRST-barcode" == patrondata.authorization_identifier

    def test_blacklist_may_remove_every_authorization_identifier(self):
        """A patron may end up with no authorization identifier whatsoever
        because they're all blacklisted.
        """
        api = self.mock_api(blacklist=["barcode"])
        content = api.sample_data("dump.two_barcodes.html")
        patrondata = api.patron_dump_to_patrondata('alice', content)
        assert patrondata.NO_VALUE == patrondata.authorization_identifier
        assert [] == patrondata.authorization_identifiers

    def test_verify_certificate(self):
        """Test the ability to bypass verification of the Millenium Patron API
        server's SSL certificate.
        """
        # By default, verify_certificate is True.
        assert True == self.api.verify_certificate

        api = self.mock_api(verify_certificate=False)
        assert False == api.verify_certificate

        # Test that the value of verify_certificate becomes the
        # 'verify' argument when _modify_request_kwargs() is called.
        kwargs = dict(verify=False)
        api = self.mock_api(verify_certificate = "yes please")
        api._update_request_kwargs(kwargs)
        assert "yes please" == kwargs['verify']

        # NOTE: We can't automatically test that request() actually
        # calls _modify_request_kwargs() because request() is the
        # method we override for mock purposes.

    def test_patron_block_reason(self):
        m = MilleniumPatronAPI._patron_block_reason
        blocked = PatronData.UNKNOWN_BLOCK
        unblocked = PatronData.NO_VALUE

        # Our default behavior.
        assert blocked == m(None, "a")
        assert unblocked == m(None, None)
        assert unblocked == m(None, "-")
        assert unblocked == m(None, " ")

        # Behavior with custom block values.
        assert blocked == m("abcd", "b")
        assert unblocked == m("abcd", "e")
        assert unblocked == m("", "-")

        # This is unwise but allowed.
        assert blocked == m("ab-c", "-")

    def test_family_name_match(self):
        m = MilleniumPatronAPI.family_name_match
        assert False == m(None, None)
        assert False == m(None, "")
        assert False == m("", None)
        assert True == m("", "")
        assert True == m("cher", "cher")
        assert False == m("chert", "cher")
        assert False == m("cher", "chert")
        assert True == m("cherryh, c.j.", "cherryh")
        assert True == m("c.j. cherryh", "cherryh")
        assert True == m("caroline janice cherryh", "cherryh")

    def test_misconfigured_authentication_mode(self):
        with pytest.raises(CannotLoadConfiguration) as excinfo:
            self.mock_api(auth_mode = 'nosuchauthmode')
        assert "Unrecognized Millenium Patron API authentication mode: nosuchauthmode." in str(excinfo.value)

    def test_authorization_without_password(self):
        """Test authorization when no password is required, only
        patron identifier.
        """
        self.api = self.mock_api(
            password_keyboard=MilleniumPatronAPI.NULL_KEYBOARD
        )
        assert False == self.api.collects_password
        # If the patron lookup succeeds, the user is authenticated
        # as that patron.
        self.api.enqueue("dump.success.html")
        patrondata = self.api.remote_authenticate(
            "44444444444447", None
        )
        assert "44444444444447" == patrondata.authorization_identifier

        # If it fails, the user is not authenticated.
        self.api.enqueue("dump.no such barcode.html")
        patrondata = self.api.remote_authenticate("44444444444447", None)
        assert False == patrondata

    def test_authorization_family_name_success(self):
        """Test authenticating against the patron's family name, given the
        correct name (case insensitive)
        """
        self.api = self.mock_api(auth_mode = "family_name")
        self.api.enqueue("dump.success.html")
        patrondata = self.api.remote_authenticate(
            "44444444444447", "Sheldon"
        )
        assert "44444444444447" == patrondata.authorization_identifier

        # Since we got a full patron dump, the PatronData we get back
        # is complete.
        assert True == patrondata.complete

    def test_authorization_family_name_failure(self):
        """Test authenticating against the patron's family name, given the
        incorrect name
        """
        self.api = self.mock_api(auth_mode = "family_name")
        self.api.enqueue("dump.success.html")
        assert False == self.api.remote_authenticate("44444444444447", "wrong name")

    def test_authorization_family_name_no_such_patron(self):
        """If no patron is found, authorization based on family name cannot
        proceed.
        """
        self.api = self.mock_api(auth_mode = "family_name")
        self.api.enqueue("dump.no such barcode.html")
        assert False == self.api.remote_authenticate("44444444444447", "somebody")

    def test_extract_postal_code(self):
        # Test our heuristics for extracting postal codes from address fields.
        m = MilleniumPatronAPI.extract_postal_code
        assert "93203" == m("1 Main Street$Arvin CA 93203")
        assert "93203" == m("1 Main Street\nArvin CA 93203")
        assert "93203" == m("10145 Main Street$Arvin CA 93203")
        assert "93203" == m("10145 Main Street$Arvin CA$93203")
        assert "93203" == m("10145-6789 Main Street$Arvin CA 93203-1234")
        assert "93203" == m("10145-6789 Main Street$Arvin CA 93203-1234 (old address)")
        assert "93203" == m("10145-6789 Main Street$Arvin CA 93203 (old address)")
        assert "93203" == m("10145-6789 Main Street Apartment #12345$Arvin CA 93203 (old address)")

        assert None == m("10145 Main Street Apartment 123456$Arvin CA")
        assert None == m("10145 Main Street$Arvin CA")
        assert None == m("123 Main Street")

        # Some cases where we incorrectly detect a ZIP code where there is none.
        assert '12345' == m("10145 Main Street, Apartment #12345$Arvin CA")
