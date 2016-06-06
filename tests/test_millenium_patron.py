import pkgutil
from datetime import date
from nose.tools import (
    eq_,
    set_trace,
)

from api.millenium_patron import MilleniumPatronAPI
from . import DatabaseTest, sample_data

class DummyResponse(object):
    def __init__(self, content):
        self.status_code = 200
        self.content = content

class DummyAPI(MilleniumPatronAPI):

    def __init__(self):
        super(DummyAPI, self).__init__("")
        self.queue = []

    def sample_data(self, filename):
        return sample_data(filename, 'millenium_patron')

    def enqueue(self, filename):
        data = self.sample_data(filename)
        self.queue.append(data)

    def request(self, *args, **kwargs):
        return DummyResponse(self.queue.pop())


class TestMilleniumPatronAPI(DatabaseTest):

    def setup(self):
        super(TestMilleniumPatronAPI, self).setup()
        self.api = DummyAPI()
        
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

        
