from nose.tools import (
    eq_,
    set_trace,
)
import datetime
from api.clever import (
    CleverAuthenticationAPI,
    UNSUPPORTED_CLEVER_USER_TYPE,
    CLEVER_NOT_ELIGIBLE,
)
from core.model import (
    Credential,
    DataSource,
    Patron,
    get_one,
    get_one_or_create,
)
from .. import DatabaseTest

class MockAPI(CleverAuthenticationAPI):
    def __init__(self, *args, **kwargs):
        super(MockAPI, self).__init__(*args, **kwargs)
        self.queue = []

    def queue_response(self, response):
        self.queue.append(response)

    def _get_token(self, payload, headers):
        return self.queue.pop()

    def _get(self, url, headers):
        return self.queue.pop()

    def _server_redirect_uri(self):
        return ""

    def _internal_authenticate_url(self):
        return ""

class TestCleverAuthenticationAPI(DatabaseTest):

    def setup(self):
        super(TestCleverAuthenticationAPI, self).setup()
        self.api = MockAPI('fake_client_id', 'fake_client_secret', 2)

    def test_authenticated_patron(self):
        eq_(None, self.api.authenticated_patron(self._db, "not a valid token"))

        # This patron has a valid clever token.
        patron = self._patron()
        credential, is_new = self.api.create_token(self._db, patron, "test")
        eq_(patron, self.api.authenticated_patron(self._db, "test"))

        # If the token is expired, the patron has to log in again.
        credential.expires = datetime.datetime.now() - datetime.timedelta(days=1)
        eq_(None, self.api.authenticated_patron(self._db, "test"))

    def test_remote_patron_lookup_unsupported_user_type(self):
        self.api.queue_response(dict(type='district_admin', data=dict(id='1234')))
        token = self.api.remote_patron_lookup("token")
        eq_(UNSUPPORTED_CLEVER_USER_TYPE, token)

    def test_remote_patron_lookup_ineligible(self):
        self.api.queue_response(dict(data=dict(nces_id='I am not Title I')))
        self.api.queue_response(dict(data=dict(school='1234', district='1234')))
        self.api.queue_response(dict(type='student', data=dict(id='1234'), links=[dict(rel='canonical', uri='test')]))

        token = self.api.remote_patron_lookup("")
        eq_(CLEVER_NOT_ELIGIBLE, token)

    def test_remote_patron_lookup_title_i(self):
        self.api.queue_response(dict(data=dict(nces_id='44270647')))
        self.api.queue_response(dict(data=dict(school='1234', district='1234', name='Abcd')))
        self.api.queue_response(dict(type='student', data=dict(id='5678'), links=[dict(rel='canonical', uri='test')]))

        patrondata = self.api.remote_patron_lookup("token")
        eq_('Abcd', patrondata.personal_name)
        eq_("5678", patrondata.permanent_id)
        eq_("5678", patrondata.authorization_identifier)

    def test_remote_patron_lookup_free_lunch_status(self):
        pass

    def test_remote_patron_lookup_external_type(self):
        # Teachers have an external type of 'A' indicating all access.
        self.api.queue_response(dict(data=dict(nces_id='44270647')))
        self.api.queue_response(dict(data=dict(school='1234', district='1234', name='Abcd')))
        self.api.queue_response(dict(type='teacher', data=dict(id='1'), links=[dict(rel='canonical', uri='test')]))

        patrondata = self.api.remote_patron_lookup("teacher token")
        eq_("A", patrondata.external_type)

        # Student type is based on grade
        def queue_student(grade):
            self.api.queue_response(dict(data=dict(nces_id='44270647')))
            self.api.queue_response(dict(data=dict(school='1234', district='1234', name='Abcd', grade=grade)))
            self.api.queue_response(dict(type='student', data=dict(id='2'), links=[dict(rel='canonical', uri='test')]))

        queue_student(grade="1")
        patrondata = self.api.remote_patron_lookup("token")
        eq_("E", patrondata.external_type)

        queue_student(grade="6")
        patrondata = self.api.remote_patron_lookup("token")
        eq_("M", patrondata.external_type)

        queue_student(grade="9")
        patrondata = self.api.remote_patron_lookup("token")
        eq_("H", patrondata.external_type)
