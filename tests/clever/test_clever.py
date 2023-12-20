import os
import datetime

from flask import request, url_for

from api.clever import (
    CleverAuthenticationAPI,
    UNSUPPORTED_CLEVER_USER_TYPE,
    CLEVER_NOT_ELIGIBLE,
    CLEVER_UNKNOWN_SCHOOL,
    external_type_from_clever_grade,
)
from api.problem_details import INVALID_CREDENTIALS
from core.model import ExternalIntegration
from core.util.datetime_helpers import utc_now
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


class TestClever:
    def test_external_type_from_clever_grade(self):
        """
        GIVEN: A string representing a student grade level supplied by the Clever API
        WHEN:  That string is present in api.clever.CLEVER_GRADE_TO_EXTERNAL_TYPE_MAP
        THEN:  The matching external_type value should be returned, or None if the match fails
        """
        for e_grade in [
            "InfantToddler", "Preschool", "PreKindergarten", "TransitionalKindergarten",
            "Kindergarten", "1", "2", "3", "4", "5"
        ]:
            assert external_type_from_clever_grade(e_grade) == "E"

        for m_grade in ["6", "7", "8"]:
            assert external_type_from_clever_grade(m_grade) == "M"

        for h_grade in ["9", "10", "11", "12", "13", "PostGraduate"]:
            assert external_type_from_clever_grade(h_grade) == "H"

        for none_grade in ["Other", "Ungraded", None, "NOT A VALID GRADE STRING"]:
            assert external_type_from_clever_grade(none_grade) is None


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
        """Make a fake ExternalIntegration that can be used to configure a CleverAuthenticationAPI"""
        integration = self._external_integration(protocol="OAuth", goal=ExternalIntegration.PATRON_AUTH_GOAL,
                                                 username="fake_client_id", password="fake_client_secret")
        integration.setting(MockAPI.OAUTH_TOKEN_EXPIRATION_DAYS).value = 20
        return integration

    def test_authenticated_patron(self):
        """An end-to-end test of authenticated_patron()."""
        assert self.api.authenticated_patron(
            self._db, "not a valid token") is None

        # This patron has a valid clever token.
        patron = self._patron()
        (credential, _) = self.api.create_token(self._db, patron, "test")
        assert patron == self.api.authenticated_patron(self._db, "test")

        # If the token is expired, the patron has to log in again.
        credential.expires = utc_now() - datetime.timedelta(days=1)
        assert self.api.authenticated_patron(self._db, "test") is None

    def test_remote_exchange_code_for_bearer_token(self):
        # Test success.
        self.api.queue_response(dict(access_token="a token"))
        with self.app.test_request_context("/"):
            assert self.api.remote_exchange_code_for_bearer_token(
                self._db, "code") == "a token"

        # Test failure.
        self.api.queue_response(None)
        with self.app.test_request_context("/"):
            problem = self.api.remote_exchange_code_for_bearer_token(
                self._db, "code")
        assert INVALID_CREDENTIALS.uri == problem.uri

        self.api.queue_response(dict(something_else="not a token"))
        with self.app.test_request_context("/"):
            problem = self.api.remote_exchange_code_for_bearer_token(
                self._db, "code")
        assert INVALID_CREDENTIALS.uri == problem.uri

    def test_remote_exchange_payload(self):
        """Test the content of the document sent to Clever when exchanging tokens"""
        with self.app.test_request_context("/"):
            payload = self.api._remote_exchange_payload(self._db, "a code")

            expect_uri = url_for("oauth_callback",
                                 library_short_name=self._default_library.name,
                                 _external=True, _scheme='https')
            assert 'authorization_code' == payload['grant_type']
            assert expect_uri == payload['redirect_uri']
            assert 'a code' == payload['code']

    def test_remote_patron_lookup_unsupported_user_type(self):
        self.api.queue_response(dict(type='user', data=dict(
            id='1234'), links=[dict(rel='canonical', uri='test'), dict(rel='district', uri='test')]))
        self.api.queue_response(
            dict(data=dict(roles=dict(district_admin=None), data=dict(id='1234'))))
        token = self.api.remote_patron_lookup("token")
        assert UNSUPPORTED_CLEVER_USER_TYPE == token

    def test_remote_patron_lookup_ineligible(self):
        self.api.queue_response(dict(type='user', data=dict(
            id='1234'), links=[dict(rel='canonical', uri='test'), dict(rel='district', uri='test')]))
        self.api.queue_response(
            dict(data=dict(roles=dict(student=dict(
                school='1234', district='1234')))))
        self.api.queue_response(dict(data=dict(nces_id='I am not Title I')))

        token = self.api.remote_patron_lookup("")
        assert CLEVER_NOT_ELIGIBLE == token

    def test_remote_patron_lookup_missing_nces_id(self):
        # Missing nces_id should return UNKNOWN_SCHOOL
        self.api.queue_response(dict(type='user', data=dict(
            id='1234'), links=[dict(rel='canonical', uri='test'), dict(rel='district', uri='test')]))
        self.api.queue_response(
            dict(data=dict(roles=dict(student=dict(
                school='1234', district='1234')))))
        self.api.queue_response(dict(data=dict(nces_id=None)))
        self.api.queue_response(dict(data=dict(name='non-demo')))

        token = self.api.remote_patron_lookup("")
        assert CLEVER_UNKNOWN_SCHOOL == token

    def test_remote_patron_lookup_missing_sandbox_nces_id(self):
        #Sandbox does not have nces id and should pass with blank string
        self.api.queue_response(dict(type='user', data=dict(
            id='1234'), links=[dict(rel='canonical', uri='test'), dict(rel='district', uri='test')]))
        self.api.queue_response(
            dict(data=dict(roles=dict(student=dict(
                school='1234', district='1234')))))
        self.api.queue_response(dict(data=dict(nces_id='')))
        self.api.queue_response(dict(data=dict(name='#DEMO')))

        patron_data = self.api.remote_patron_lookup("")
        assert "1234" == patron_data.permanent_id
        assert "1234" == patron_data.authorization_identifier

    def test_remote_patron_unknown_student_grade(self):
        self.api.queue_response(dict(type='user', data=dict(
            id='1234'), links=[dict(rel='canonical', uri='test'), dict(rel='district', uri='test')]))
        self.api.queue_response(dict(data=dict(roles=dict(
            student=dict(
                school='1234',
                grade=""
            )
        ))))
        self.api.queue_response(dict(data=dict(nces_id='44270647')))

        patrondata = self.api.remote_patron_lookup("token")
        assert patrondata.external_type is None

    def test_remote_patron_lookup_title_i(self):
        self.api.queue_response(dict(type='user', data=dict(
            id='1234'), links=[dict(rel='canonical', uri='test'), dict(rel='district', uri='test')]))
        self.api.queue_response(dict(data=dict(roles=dict(
            student=dict(
                school='1234',
                grade="10"
            )
        ))))
        self.api.queue_response(dict(data=dict(nces_id='44270647')))

        patrondata = self.api.remote_patron_lookup("token")
        assert patrondata.personal_name is None
        assert "1234" == patrondata.permanent_id
        assert "1234" == patrondata.authorization_identifier

    def test_remote_patron_lookup_free_lunch_status(self):
        pass

    def test_remote_patron_lookup_external_type(self):
        # Teachers have an external type of 'A' indicating all access.
        self.api.queue_response(dict(type='user', data=dict(
            id='1234'), links=[dict(rel='canonical', uri='test'), dict(rel='district', uri='test')]))
        self.api.queue_response(dict(data=dict(roles=dict(
            teacher=dict(
                school='1234'
            )
        ))))
        self.api.queue_response(dict(data=dict(nces_id='44270647')))

        patrondata = self.api.remote_patron_lookup("teacher token")
        assert "A" == patrondata.external_type

        # Student type is based on grade
        def queue_student(grade):
            self.api.queue_response(dict(type='user', data=dict(
                id='1234'), links=[dict(rel='canonical', uri='test'), dict(rel='district', uri='test')]))
            self.api.queue_response(dict(data=dict(roles=dict(
                student=dict(
                    school='1234',
                    grade=grade
                )
            ))))
            self.api.queue_response(dict(data=dict(nces_id='44270647')))

        queue_student(grade="1")
        patrondata = self.api.remote_patron_lookup("token")
        assert "E" == patrondata.external_type

        queue_student(grade="6")
        patrondata = self.api.remote_patron_lookup("token")
        assert "M" == patrondata.external_type

        queue_student(grade="9")
        patrondata = self.api.remote_patron_lookup("token")
        assert "H" == patrondata.external_type

    def test_oauth_callback_creates_patron(self):
        """Test a successful run of oauth_callback."""
        self.api.queue_response(dict(access_token="bearer token"))
        self.api.queue_response(dict(type='user', data=dict(
            id='1234'), links=[dict(rel='canonical', uri='test'), dict(rel='district', uri='test')]))
        self.api.queue_response(dict(data=dict(roles=dict(
            teacher=dict(
                school='1234'
            )
        ))))
        self.api.queue_response(dict(data=dict(nces_id='44270647')))

        with self.app.test_request_context("/"):
            response = self.api.oauth_callback(
                self._db, dict(code="teacher code"))
            credential, patron, patrondata = response

        assert patrondata.is_new is True

        # The bearer token was turned into a Credential.
        expect_credential, ignore = self.api.create_token(
            self._db, patron, "bearer token")
        assert credential == expect_credential

        # Since the patron is a teacher, their external_type was set to 'A'.
        assert "A" == patron.external_type

        # Clever provided personal name information, but we don't include it in the PatronData.
        assert patrondata.personal_name is None

    def test_oauth_callback_problem_detail_if_bad_token(self):
        self.api.queue_response(dict(something_else="not a token"))
        with self.app.test_request_context("/"):
            response = self.api.oauth_callback(
                self._db, dict(code="teacher code"))
        assert isinstance(response, ProblemDetail)
        assert INVALID_CREDENTIALS.uri == response.uri

    def test_oauth_callback_problem_detail_if_remote_patron_lookup_fails(self):
        self.api.queue_response(dict(access_token="token"))
        self.api.queue_response(dict())

        with self.app.test_request_context("/"):
            response = self.api.oauth_callback(
                self._db, dict(code="teacher code"))

        assert isinstance(response, ProblemDetail)
        assert INVALID_CREDENTIALS.uri == response.uri

    def test_external_authenticate_url(self):
        """Verify that external_authenticate_url is generated properly"""
        # We're about to call url_for, so we must create an application context.
        my_api = CleverAuthenticationAPI(
            self._default_library, self.mock_integration)

        with self.app.test_request_context("/"):
            request.library = self._default_library
            params = my_api.external_authenticate_url("state", self._db)
            expected_redirect_uri = url_for("oauth_callback", library_short_name=self._default_library.short_name,
                                            _external=True, _scheme='https')
            expected = (
                'https://clever.com/oauth/authorize'
                '?response_type=code&client_id=fake_client_id&redirect_uri=%s&state=state'
            ) % expected_redirect_uri
            assert params == expected
