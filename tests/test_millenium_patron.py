import pkgutil
from datetime import date, datetime, timedelta
from nose.tools import (
    eq_,
    set_trace,
)

from api.config import (
    Configuration,
    temp_config,
)

from api.authenticator import PatronData
from api.millenium_patron import MilleniumPatronAPI
from . import DatabaseTest, sample_data

class MockResponse(object):
    def __init__(self, content):
        self.status_code = 200
        self.content = content

class MockAPI(MilleniumPatronAPI):

    def __init__(self, root="", *args, **kwargs):
        super(MockAPI, self).__init__(root, *args, **kwargs)
        self.queue = []

    def sample_data(self, filename):
        return sample_data(filename, 'millenium_patron')

    def enqueue(self, filename):
        data = self.sample_data(filename)
        self.queue.append(data)

    def request(self, *args, **kwargs):
        response = self.queue[0]
        self.queue = self.queue[1:]
        return MockResponse(response)


class TestMilleniumPatronAPI(DatabaseTest):

    def setup(self):
        super(TestMilleniumPatronAPI, self).setup()
        self.api = MockAPI()

    def test_from_config(self):
        api = None
        with temp_config() as config:
            data = {
                Configuration.URL : "http://example.com",
                Configuration.AUTHORIZATION_IDENTIFIER_BLACKLIST : ["a", "b"]
            }
            config[Configuration.INTEGRATIONS] = {
                MilleniumPatronAPI.CONFIGURATION_NAME : data
            }
            api = MilleniumPatronAPI.from_config()
        eq_("http://example.com/", api.root)
        eq_(["a", "b"], [x.pattern for x in api.blacklist])
            
    def test_remote_patron_lookup_no_such_patron(self):
        self.api.enqueue("dump.no such barcode.html")
        patrondata = PatronData(authorization_identifier="bad barcode")
        eq_(None, self.api.remote_patron_lookup(patrondata))

    def test_remote_patron_lookup_success(self):
        self.api.enqueue("dump.success.html")
        patrondata = PatronData(authorization_identifier="good barcode")
        patrondata = self.api.remote_patron_lookup(patrondata)

        # Although "good barcode" was successful in lookup this patron
        # up, it didn't show up in their patron dump as a barcode, so
        # the authorization_identifier from the patron dump took
        # precedence.
        eq_("6666666", patrondata.permanent_id)
        eq_("44444444444447", patrondata.authorization_identifier)
        eq_("alice", patrondata.username)
        eq_("$0.00", patrondata.fines)
        eq_(date(2059, 4, 1), patrondata.authorization_expires)
        eq_("SHELDON, ALICE", patrondata.personal_name)
        eq_("alice@sheldon.com", patrondata.email_address)
        
    def test_parse_poorly_behaved_dump(self):
        """The HTML parser is able to handle HTML embedded in
        field values.
        """
        self.api.enqueue("dump.embedded_html.html")
        patrondata = PatronData(authorization_identifier="good barcode")
        patrondata = self.api.remote_patron_lookup(patrondata)
        eq_("abcd", patrondata.authorization_identifier)

    def test_incoming_authorization_identifier_retained(self):
        # This patron has two barcodes.
        dump = self.api.sample_data("dump.two_barcodes.html")

        # Let's say they authenticate with the first one.
        patrondata = self.api.patron_dump_to_patrondata("FIRST_barcode", dump)
        # Their Patron record will use their first barcode as authorization
        # identifier, because that's what they typed in.
        eq_("FIRST_barcode", patrondata.authorization_identifier)

        # Let's say they authenticate with the second barcode.
        patrondata = self.api.patron_dump_to_patrondata("SECOND_barcode", dump)
        # Their Patron record will use their second barcode as authorization
        # identifier, because that's what they typed in.
        eq_("SECOND_barcode", patrondata.authorization_identifier)

        # Let's say they authenticate with a username.
        patrondata = self.api.patron_dump_to_patrondata(
            "username", dump
        )
        # Their Patron record will suggest the second barcode as
        # authorization identifier, because it's likely to be the most
        # recently added one.
        eq_("SECOND_barcode", patrondata.authorization_identifier)
        
    def test_remote_authenticate_no_such_barcode(self):
        self.api.enqueue("pintest.no such barcode.html")
        eq_(False, self.api.remote_authenticate("wrong barcode", "pin"))

    def test_remote_authenticate_wrong_pin(self):
        self.api.enqueue("pintest.bad.html")
        eq_(False, self.api.remote_authenticate("barcode", "wrong pin"))

    def test_remote_authenticate_correct_pin(self):
        self.api.enqueue("pintest.good.html")
        patrondata = self.api.remote_authenticate(
            "barcode1234567", "correct pin"
        )
        # The return value includes everything we know about the
        # authenticated patron, which isn't much.
        eq_("barcode1234567", patrondata.authorization_identifier)
        
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
        eq_(p2, p)
        eq_("SECOND_barcode", p.authorization_identifier)

        # If the patron is new, and logged in with a barcode, their
        # authorization identifier will be the barcode they used.
        p.authorization_identifier = None
        self.api.enqueue("pintest.good.html")
        self.api.enqueue("dump.two_barcodes.html")
        self.api.authenticated_patron(self._db, dict(username="FIRST_barcode"))
        eq_("FIRST_barcode", p.authorization_identifier)

        p.authorization_identifier = None
        self.api.enqueue("pintest.good.html")
        self.api.enqueue("dump.two_barcodes.html")
        self.api.authenticated_patron(self._db, dict(username="SECOND_barcode"))
        eq_("SECOND_barcode", p.authorization_identifier)

        # If the patron has an authorization identifier, but it's not
        # one of the barcodes, we'll replace it the same way we would
        # determine the authorization identifier for a new patron.
        p.authorization_identifier = "abcd"
        self.api.enqueue("pintest.good.html")
        self.api.enqueue("dump.two_barcodes.html")
        self.api.authenticated_patron(self._db, dict(username="alice"))
        eq_("SECOND_barcode", p.authorization_identifier)

        p.authorization_identifier = "abcd"
        self.api.enqueue("pintest.good.html")
        self.api.enqueue("dump.two_barcodes.html")
        self.api.authenticated_patron(self._db, dict(username="FIRST_barcode"))
        eq_("FIRST_barcode", p.authorization_identifier)

        p.authorization_identifier = "abcd"
        self.api.enqueue("pintest.good.html")
        self.api.enqueue("dump.two_barcodes.html")
        self.api.authenticated_patron(self._db, dict(username="SECOND_barcode"))
        eq_("SECOND_barcode", p.authorization_identifier)

        # If the patron has an authorization identifier, and it _is_
        # one of their barcodes, we'll keep it.
        p.authorization_identifier = "FIRST_barcode"
        self.api.enqueue("pintest.good.html")
        self.api.enqueue("dump.two_barcodes.html")
        self.api.authenticated_patron(self._db, dict(username="alice"))
        eq_("FIRST_barcode", p.authorization_identifier)

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
        p.authorization_identifier = "SECOND_barcode"
        self.api.enqueue("pintest.good.html")
        self.api.enqueue("dump.two_barcodes.html")
        self.api.authenticated_patron(self._db, dict(username="FIRST_barcode"))
        eq_("SECOND_barcode", p.authorization_identifier)

        # If somehow the patron ended up with their username as an
        # authorization identifier, we'll replace it.
        p.authorization_identifier = "alice"
        self.api.enqueue("pintest.good.html")
        self.api.enqueue("dump.two_barcodes.html")
        self.api.authenticated_patron(self._db, dict(username="alice"))
        eq_("SECOND_barcode", p.authorization_identifier)

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
        eq_("44444444444447", alice.authorization_identifier)
        eq_("alice", alice.username)

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
        eq_("44444444444447", alice.authorization_identifier)
        eq_("alice", alice.username)

        # Authenticate with username again
        self.api.enqueue("pintest.good.html")
        alice = self.api.authenticated_patron(self._db, dict(username="alice", password="4444"))
        eq_("44444444444447", alice.authorization_identifier)
        eq_("alice", alice.username)

    def test_authenticated_patron_renewed_card(self):
        now = datetime.utcnow()
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
        eq_(p2, p)
        eq_(p2.last_external_sync, one_hour_ago)

        # However, if the card has expired, a sync is performed every
        # time.
        p.authorization_expires = one_week_ago
        self.api.enqueue("dump.success.html")
        self.api.enqueue("pintest.good.html")
        p2 = self.api.authenticated_patron(self._db, auth)
        eq_(p2, p)

        # Since the sync was performed, last_external_sync was updated.
        assert p2.last_external_sync > one_hour_ago

        # And the patron's card is no longer expired.
        expiration = date(2059, 4, 1)
        eq_(expiration, p.authorization_expires)

    def test_authentication_patron_invalid_expiration_date(self):
        p = self._patron()
        p.authorization_identifier = "44444444444447"
        self.api.enqueue("dump.invalid_expiration.html")
        self.api.enqueue("pintest.good.html")
        auth = dict(username="44444444444447", password="4444")
        p2 = self.api.authenticated_patron(self._db, auth)
        eq_(p2, p)

    def test_patron_dump_to_patrondata(self):
        api = MockAPI()
        content = api.sample_data("dump.success.html")
        patrondata = api.patron_dump_to_patrondata('alice', content)
        eq_("44444444444447", patrondata.authorization_identifier)
        eq_("alice", patrondata.username)
        
    def test_authorization_identifier_blacklist(self):
        """A patron has two authorization identifiers. Ordinarily the second
        one (which would normally be preferred), but it contains a
        blacklisted string, so the first takes precedence.
        """
        api = MockAPI()
        content = api.sample_data("dump.two_barcodes.html")
        patrondata = api.patron_dump_to_patrondata('alice', content)
        eq_("SECOND_barcode", patrondata.authorization_identifier)

        api = MockAPI(authorization_blacklist=["second"])
        patrondata = api.patron_dump_to_patrondata('alice', content)
        eq_("FIRST_barcode", patrondata.authorization_identifier)
        
    def test_blacklist_may_remove_every_authorization_identifier(self):
        """A patron may end up with no authorization identifier whatsoever
        because they're all blacklisted.
        """
        api = MockAPI(authorization_blacklist=["barcode"])
        content = api.sample_data("dump.two_barcodes.html")
        patrondata = api.patron_dump_to_patrondata('alice', content)
        eq_(patrondata.NO_VALUE, patrondata.authorization_identifier)
        eq_([], patrondata.authorization_identifiers)

