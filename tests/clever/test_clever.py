import os
from nose.tools import (
    eq_,
    set_trace,
)
import datetime
import flask
from flask import url_for
from api.clever import (
    CleverAuthenticationAPI,
    UNSUPPORTED_CLEVER_USER_TYPE,
    CLEVER_NOT_ELIGIBLE,
    CLEVER_UNKNOWN_SCHOOL,
)
from api.problem_details import *
from core.model import (
    Credential,
    DataSource,
    ExternalIntegration,
    Patron,
    get_one,
    get_one_or_create,
)
from core.util.problem_detail import ProblemDetail
from core.testing import DatabaseTest

class MockAPI(CleverAuthenticationAPI):
    def __init__(self, *args, **kwargs):
        super(MockAPI, self).__init__(*args, **kwargs)
        self.queue = []

    def queue_response(self, response):
        self.queue.insert(0, response)

    def _get_token(self, payload, headers):
        return self.queue.pop()

    def _get(self, url, headers):
        return self.queue.pop()

    def _server_redirect_uri(self):
        return ""

    def _internal_authenticate_url(self):
        return ""

class TestCleverAuthenticationAPI(DatabaseTest):


    def setup_method(self):
        super(TestCleverAuthenticationAPI, self).setup_method()

        self.api = MockAPI(self._default_library, self.mock_integration)
        os.environ['AUTOINITIALIZE'] = "False"
        from api.app import app
        del os.environ['AUTOINITIALIZE']
        self.app = app

    @property
    def mock_integration(self):
        """Make a fake ExternalIntegration that can be used to configure
        a CleverAuthenticationAPI.
        """
        integration = self._external_integration(
            protocol="OAuth",
            goal=ExternalIntegration.PATRON_AUTH_GOAL,
            username="fake_client_id",
            password="fake_client_secret"
        )
        integration.setting(MockAPI.OAUTH_TOKEN_EXPIRATION_DAYS).value = 20
        return integration

    def test_authenticated_patron(self):
        """An end-to-end test of authenticated_patron()."""
        eq_(None, self.api.authenticated_patron(self._db, "not a valid token"))

        # This patron has a valid clever token.
        patron = self._patron()
        credential, is_new = self.api.create_token(self._db, patron, "test")
        eq_(patron, self.api.authenticated_patron(self._db, "test"))

        # If the token is expired, the patron has to log in again.
        credential.expires = datetime.datetime.now() - datetime.timedelta(days=1)
        eq_(None, self.api.authenticated_patron(self._db, "test"))

    def test_remote_exchange_code_for_bearer_token(self):
        # Test success.
        self.api.queue_response(dict(access_token="a token"))
        with self.app.test_request_context("/"):
            eq_("a token",
                self.api.remote_exchange_code_for_bearer_token(self._db, "code")
            )

        # Test failure.
        self.api.queue_response(None)
        with self.app.test_request_context("/"):
            problem = self.api.remote_exchange_code_for_bearer_token(self._db, "code")
        eq_(INVALID_CREDENTIALS.uri, problem.uri)

        self.api.queue_response(dict(something_else="not a token"))
        with self.app.test_request_context("/"):
            problem = self.api.remote_exchange_code_for_bearer_token(self._db, "code")
        eq_(INVALID_CREDENTIALS.uri, problem.uri)

    def test_remote_exchange_payload(self):
        """Test the content of the document sent to Clever when
        exchanging tokens.
        """
        with self.app.test_request_context("/"):
            payload = self.api._remote_exchange_payload(self._db, "a code")

            expect_uri = url_for("oauth_callback",
                                 library_short_name=self._default_library.name,
                                 _external=True)
            eq_('authorization_code', payload['grant_type'])
            eq_(expect_uri, payload['redirect_uri'])
            eq_('a code', payload['code'])

    def test_remote_patron_lookup_unsupported_user_type(self):
        self.api.queue_response(dict(type='district_admin', data=dict(id='1234')))
        token = self.api.remote_patron_lookup("token")
        eq_(UNSUPPORTED_CLEVER_USER_TYPE, token)

    def test_remote_patron_lookup_ineligible(self):
        self.api.queue_response(dict(type='student', data=dict(id='1234'), links=[dict(rel='canonical', uri='test')]))
        self.api.queue_response(dict(data=dict(school='1234', district='1234')))
        self.api.queue_response(dict(data=dict(nces_id='I am not Title I')))

        token = self.api.remote_patron_lookup("")
        eq_(CLEVER_NOT_ELIGIBLE, token)

    def test_remote_patron_lookup_missing_nces_id(self):
        self.api.queue_response(dict(type='student', data=dict(id='1234'), links=[dict(rel='canonical', uri='test')]))
        self.api.queue_response(dict(data=dict(school='1234', district='1234')))
        self.api.queue_response(dict(data=dict()))

        token = self.api.remote_patron_lookup("")
        eq_(CLEVER_UNKNOWN_SCHOOL, token)

    def test_remote_patron_lookup_title_i(self):
        self.api.queue_response(dict(type='student', data=dict(id='5678'), links=[dict(rel='canonical', uri='test')]))
        self.api.queue_response(dict(data=dict(school='1234', district='1234', name='Abcd')))
        self.api.queue_response(dict(data=dict(nces_id='44270647')))

        patrondata = self.api.remote_patron_lookup("token")
        eq_(None, patrondata.personal_name)
        eq_("5678", patrondata.permanent_id)
        eq_("5678", patrondata.authorization_identifier)

    def test_remote_patron_lookup_free_lunch_status(self):
        pass

    def test_remote_patron_lookup_external_type(self):
        # Teachers have an external type of 'A' indicating all access.
        self.api.queue_response(dict(type='teacher', data=dict(id='1'), links=[dict(rel='canonical', uri='test')]))
        self.api.queue_response(dict(data=dict(school='1234', district='1234', name='Abcd')))
        self.api.queue_response(dict(data=dict(nces_id='44270647')))

        patrondata = self.api.remote_patron_lookup("teacher token")
        eq_("A", patrondata.external_type)

        # Student type is based on grade
        def queue_student(grade):
            self.api.queue_response(dict(type='student', data=dict(id='2'), links=[dict(rel='canonical', uri='test')]))
            self.api.queue_response(dict(data=dict(school='1234', district='1234', name='Abcd', grade=grade)))
            self.api.queue_response(dict(data=dict(nces_id='44270647')))


        queue_student(grade="1")
        patrondata = self.api.remote_patron_lookup("token")
        eq_("E", patrondata.external_type)

        queue_student(grade="6")
        patrondata = self.api.remote_patron_lookup("token")
        eq_("M", patrondata.external_type)

        queue_student(grade="9")
        patrondata = self.api.remote_patron_lookup("token")
        eq_("H", patrondata.external_type)

    def test_oauth_callback_creates_patron(self):
        """Test a successful run of oauth_callback."""
        self.api.queue_response(dict(access_token="bearer token"))
        self.api.queue_response(dict(type='teacher', data=dict(id='1'), links=[dict(rel='canonical', uri='test')]))
        self.api.queue_response(dict(data=dict(school='1234', district='1234', name='Abcd')))
        self.api.queue_response(dict(data=dict(nces_id='44270647')))

        with self.app.test_request_context("/"):
            response = self.api.oauth_callback(self._db, dict(code="teacher code"))
            credential, patron, patrondata = response

        # The bearer token was turned into a Credential.
        expect_credential, ignore = self.api.create_token(
            self._db, patron, "bearer token"
        )
        eq_(credential, expect_credential)

        # Since the patron is a teacher, their external_type
        # was set to 'A'.
        eq_("A", patron.external_type)

        # Clever provided personal name information, but we don't
        # include it in the PatronData.
        eq_(None, patrondata.personal_name)

    def test_oauth_callback_problem_detail_if_bad_token(self):
        self.api.queue_response(dict(something_else="not a token"))
        with self.app.test_request_context("/"):
            response = self.api.oauth_callback(self._db, dict(code="teacher code"))
        assert isinstance(response, ProblemDetail)
        eq_(INVALID_CREDENTIALS.uri, response.uri)

    def test_oauth_callback_problem_detail_if_remote_patron_lookup_fails(self):
        self.api.queue_response(dict(access_token="token"))
        self.api.queue_response(dict())
        with self.app.test_request_context("/"):
            response = self.api.oauth_callback(self._db, dict(code="teacher code"))
        assert isinstance(response, ProblemDetail)
        eq_(INVALID_CREDENTIALS.uri, response.uri)

    def test_external_authenticate_url(self):
        """Verify that external_authenticate_url is generated properly.
        """
        # We're about to call url_for, so we must create an
        # application context.
        my_api = CleverAuthenticationAPI(
            self._default_library, self.mock_integration
        )
        with self.app.test_request_context("/"):
            flask.request.library = self._default_library
            params = my_api.external_authenticate_url("state", self._db)
            expected_redirect_uri = url_for("oauth_callback", library_short_name=self._default_library.short_name, _external=True)
            eq_('https://clever.com/oauth/authorize?response_type=code&client_id=fake_client_id&redirect_uri=%s&state=state' % expected_redirect_uri, params)

