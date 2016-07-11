from nose.tools import (
    eq_,
    set_trace,
)
from api.clever import (
    CleverAuthenticationAPI,
    UNSUPPORTED_CLEVER_USER_TYPE,
    CLEVER_NOT_ELIGIBLE,
)
from core.model import (
    Patron,
    get_one,
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

    def _redirect_uri(self):
        return ""

class TestCleverAuthenticationAPI(DatabaseTest):

    def setup(self):
        super(TestCleverAuthenticationAPI, self).setup()
        self.api = MockAPI('fake_client_id', 'fake_client_secret')

    def test_oauth_callback_unsupported_user_type(self):
        self.api.queue_response(dict(type='district_admin', data=dict(id='1234')))
        self.api.queue_response(dict(access_token='token'))

        token, patron_info = self.api.oauth_callback(self._db, {})
        eq_(UNSUPPORTED_CLEVER_USER_TYPE, token)
        eq_(None, patron_info)

    def test_oauth_callback_ineligible(self):
        self.api.queue_response(dict(data=dict(name='I am not Title I')))
        self.api.queue_response(dict(data=dict(location=dict(state='TN'))))
        self.api.queue_response(dict(data=dict(school='1234', district='1234')))
        self.api.queue_response(dict(type='student', data=dict(id='1234'), links=[dict(rel='canonical', uri='test')]))
        self.api.queue_response(dict(access_token='token'))

        token, patron_info = self.api.oauth_callback(self._db, {})
        eq_(CLEVER_NOT_ELIGIBLE, token)
        eq_(None, patron_info)

    def test_oauth_callback_title_i(self):
        self.api.queue_response(dict(data=dict(name='#DEMO OEI Sandbox District')))
        self.api.queue_response(dict(data=dict(location=dict(state='TN'))))
        self.api.queue_response(dict(data=dict(school='1234', district='1234', name='Abcd')))
        self.api.queue_response(dict(type='student', data=dict(id='1234'), links=[dict(rel='canonical', uri='test')]))
        self.api.queue_response(dict(access_token='token'))

        token, patron_info = self.api.oauth_callback(self._db, {})
        eq_('token', token)
        eq_('Abcd', patron_info.get('name'))

        # A patron was created
        patron = get_one(self._db, Patron, authorization_identifier='1234')
        assert patron != None

    def test_oauth_callback_free_lunch_status(self):
        pass

    def test_oauth_callback_external_type(self):
        # Teacher is all-access
        self.api.queue_response(dict(data=dict(name='#DEMO OEI Sandbox District')))
        self.api.queue_response(dict(data=dict(location=dict(state='TN'))))
        self.api.queue_response(dict(data=dict(school='1234', district='1234', name='Abcd')))
        self.api.queue_response(dict(type='teacher', data=dict(id='1'), links=[dict(rel='canonical', uri='test')]))
        self.api.queue_response(dict(access_token='token'))

        self.api.oauth_callback(self._db, {})

        patron = get_one(self._db, Patron, authorization_identifier='1')
        eq_('A', patron.external_type)

        # Student type is based on grade

        self.api.queue_response(dict(data=dict(name='#DEMO OEI Sandbox District')))
        self.api.queue_response(dict(data=dict(location=dict(state='TN'))))
        self.api.queue_response(dict(data=dict(school='1234', district='1234', name='Abcd', grade='1')))
        self.api.queue_response(dict(type='student', data=dict(id='2'), links=[dict(rel='canonical', uri='test')]))
        self.api.queue_response(dict(access_token='token'))

        self.api.oauth_callback(self._db, {})

        patron = get_one(self._db, Patron, authorization_identifier='2')
        eq_('E', patron.external_type)

        self.api.queue_response(dict(data=dict(name='#DEMO OEI Sandbox District')))
        self.api.queue_response(dict(data=dict(location=dict(state='TN'))))
        self.api.queue_response(dict(data=dict(school='1234', district='1234', name='Abcd', grade='6')))
        self.api.queue_response(dict(type='student', data=dict(id='3'), links=[dict(rel='canonical', uri='test')]))
        self.api.queue_response(dict(access_token='token'))

        self.api.oauth_callback(self._db, {})

        patron = get_one(self._db, Patron, authorization_identifier='3')
        eq_('M', patron.external_type)

        self.api.queue_response(dict(data=dict(name='#DEMO OEI Sandbox District')))
        self.api.queue_response(dict(data=dict(location=dict(state='TN'))))
        self.api.queue_response(dict(data=dict(school='1234', district='1234', name='Abcd', grade='9')))
        self.api.queue_response(dict(type='student', data=dict(id='4'), links=[dict(rel='canonical', uri='test')]))
        self.api.queue_response(dict(access_token='token'))

        self.api.oauth_callback(self._db, {})

        patron = get_one(self._db, Patron, authorization_identifier='4')
        eq_('H', patron.external_type)

