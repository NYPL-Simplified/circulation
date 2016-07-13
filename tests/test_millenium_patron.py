import pkgutil
from datetime import date
from nose.tools import (
    eq_,
    set_trace,
)

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
        return MockResponse(self.queue.pop())


class TestMilleniumPatronAPI(DatabaseTest):

    def setup(self):
        super(TestMilleniumPatronAPI, self).setup()
        self.api = MockAPI()
        
    def test_dump_no_such_barcode(self):
        self.api.enqueue("dump.no such barcode.html")
        eq_(dict(ERRNUM='1', ERRMSG="Requested record not found"),
                 self.api.dump("bad barcode"))

    def test_dump_success(self):
        self.api.enqueue("dump.success.html")
        response = self.api.dump("good barcode")
        eq_('SHELDON, ALICE', response['PATRN NAME[pn]'])

        # The 'note' field has a list of values, not just one.
        eq_(2, len(response['NOTE[px]']))

    def test_pintest_no_such_barcode(self):
        self.api.enqueue("pintest.no such barcode.html")
        eq_(False, self.api.pintest("wrong barcode", "pin"))

    def test_pintest_wrong_pin(self):
        self.api.enqueue("pintest.bad.html")
        eq_(False, self.api.pintest("barcode", "wrong pin"))

    def test_pintest_correct_pin(self):
        self.api.enqueue("pintest.good.html")
        eq_(True, self.api.pintest("barcode1234567", "correct pin"))

    def test_update_patron(self):
        # Patron with a username
        self.api.enqueue("dump.success.html")
        p = self._patron()
        self.api.update_patron(p, "12345678901234")
        eq_("10", p.external_type)
        eq_("44444444444447", p.authorization_identifier)
        eq_("alice", p.username)
        expiration = date(1999, 4, 1)
        eq_(expiration, p.authorization_expires)

        # Patron with no username
        self.api.enqueue("dump.success_no_username.html")
        p = self._patron()
        self.api.update_patron(p, "12345678901234")
        eq_("10", p.external_type)
        eq_("44444444444448", p.authorization_identifier)
        eq_(None, p.username)
        expiration = date(1999, 4, 1)
        eq_(expiration, p.authorization_expires)

    def test_update_patron_authorization_identifiers(self):
        p = self._patron()

        # If the patron is new, and logged in with a username, we'll use
        # one of their barcodes as their authorization identifier.

        p.authorization_identifier = None
        self.api.enqueue("dump.two_barcodes.html")
        self.api.update_patron(p, "alice")
        eq_("SECOND_barcode", p.authorization_identifier)

        # If the patron is new, and logged in with a barcode, their
        # authorization identifier will be the barcode they used.

        p.authorization_identifier = None
        self.api.enqueue("dump.two_barcodes.html")
        self.api.update_patron(p, "FIRST_barcode")
        eq_("FIRST_barcode", p.authorization_identifier)

        p.authorization_identifier = None
        self.api.enqueue("dump.two_barcodes.html")
        self.api.update_patron(p, "SECOND_barcode")
        eq_("SECOND_barcode", p.authorization_identifier)

        # If the patron has an authorization identifier, but it's not one of the
        # barcodes, we'll replace it the same way we would determine the
        # authorization identifier for a new patron.

        p.authorization_identifier = "abcd"
        self.api.enqueue("dump.two_barcodes.html")
        self.api.update_patron(p, "alice")
        eq_("SECOND_barcode", p.authorization_identifier)

        p.authorization_identifier = "abcd"
        self.api.enqueue("dump.two_barcodes.html")
        self.api.update_patron(p, "FIRST_barcode")
        eq_("FIRST_barcode", p.authorization_identifier)

        p.authorization_identifier = "abcd"
        self.api.enqueue("dump.two_barcodes.html")
        self.api.update_patron(p, "SECOND_barcode")
        eq_("SECOND_barcode", p.authorization_identifier)

        # If the patron has an authorization identifier, and it _is_ one of
        # the barcodes, we'll keep it.

        p.authorization_identifier = "FIRST_barcode"
        self.api.enqueue("dump.two_barcodes.html")
        self.api.update_patron(p, "alice")
        eq_("FIRST_barcode", p.authorization_identifier)

        p.authorization_identifier = "SECOND_barcode"
        self.api.enqueue("dump.two_barcodes.html")
        self.api.update_patron(p, "FIRST_barcode")
        eq_("SECOND_barcode", p.authorization_identifier)

        # If somehow they ended up with their username as an authorization
        # identifier, we'll replace it.

        p.authorization_identifier = "alice"
        self.api.enqueue("dump.two_barcodes.html")
        self.api.update_patron(p, "alice")
        eq_("SECOND_barcode", p.authorization_identifier)


    def test_authenticated_patron_success(self):
        # Patron is valid, but not in our database yet
        self.api.enqueue("dump.success.html")
        self.api.enqueue("pintest.good.html")
        alice = self.api.authenticated_patron(self._db, dict(username="alice", password="4444"))
        eq_("44444444444447", alice.authorization_identifier)
        eq_("alice", alice.username)

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

    def test_patron_info(self):
        self.api.enqueue("dump.success.html")
        patron_info = self.api.patron_info("alice")
        eq_("44444444444447", patron_info.get('barcode'))
        eq_("alice", patron_info.get('username'))

    def test_first_value_takes_precedence(self):
        """This patron has two authorization identifiers.
        The second one takes precedence.
        """
        self.api.enqueue("dump.two_barcodes.html")
        patron_info = self.api.patron_info("alice")
        eq_("SECOND_barcode", patron_info.get('barcode'))
        
    def test_authorization_identifier_blacklist(self):
        """This patron has two authorization identifiers, but the second one
        contains a blacklisted string. The first takes precedence.
        """
        api = MockAPI(authorization_blacklist=["second"])
        api.enqueue("dump.two_barcodes.html")
        patron_info = api.patron_info("alice")
        eq_("FIRST_barcode", patron_info.get('barcode'))

    def test_blacklist_may_remove_every_authorization_identifier(self):
        """A patron may end up with no authorization identifier whatsoever
        because they're all blacklisted.
        """
        api = MockAPI(authorization_blacklist=["barcode"])
        api.enqueue("dump.two_barcodes.html")
        patron_info = api.patron_info("alice")
        eq_(None, patron_info.get('barcode'))

