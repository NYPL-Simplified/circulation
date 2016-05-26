from nose.tools import set_trace
import logging
import base64
import json
import requests
from flask import url_for
from authenticator import Authenticator
from config import Configuration
from core.model import (
    get_one,
    get_one_or_create,
    Patron,
)
from problem_details import *

class CleverAuthenticationAPI(Authenticator):

    NAME = 'Clever'

    CLEVER_OAUTH_URL = "https://clever.com/oauth/authorize?response_type=code&client_id=%s&redirect_uri=%s&state=Clever"
    CLEVER_TOKEN_URL = "https://clever.com/oauth/tokens"
    CLEVER_API_BASE_URL = "https://api.clever.com"

    log = logging.getLogger('Clever authentication API')

    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret

    @classmethod
    def from_config(cls):
        config = Configuration.integration(cls.NAME, required=True)
        client_id = config.get(Configuration.OAUTH_CLIENT_ID)
        client_secret = config.get(Configuration.OAUTH_CLIENT_SECRET)
        return cls(client_id, client_secret)

    def authenticate_url(self):
        redirect_uri = url_for('oauth_callback', _external=True)
        return self.CLEVER_OAUTH_URL % (self.client_id, redirect_uri)

    def authenticated_patron(self, _db, token):
        bearer_headers = {
            'Authorization': 'Bearer %s' % token
        }

        result = requests.get(self.CLEVER_API_BASE_URL + '/me', headers=bearer_headers).json()
        data = result['data']

        identifier = data['id']

        patron = get_one(_db, Patron, authorization_identifier=identifier)
        return patron


    def oauth_callback(self, _db, params):
        code = params.get('code')
        payload = dict(
            code=code,
            grant_type='authorization_code',
            redirect_uri=url_for('oauth_callback', _external=True),
        )
        headers = {
            'Authorization': 'Basic %s' % base64.b64encode(self.client_id + ":" + self.client_secret),
            'Content-Type': 'application/json',
        }

        response = requests.post(self.CLEVER_TOKEN_URL, data=json.dumps(payload), headers=headers).json()
        token = response['access_token']

        bearer_headers = {
            'Authorization': 'Bearer %s' % token
        }
        result = requests.get(self.CLEVER_API_BASE_URL + '/me', headers=bearer_headers).json()
        data = result['data']

        # TODO: which teachers and admins should have access?
        if result['type'] != 'student':
            return INVALID_CREDENTIALS

        identifier = data['id']
        student = requests.get(self.CLEVER_API_BASE_URL + '/v1.1/students/%s' % identifier, headers=bearer_headers).json()

        # TODO: check student free and reduced lunch status and/or school's NCES ID

        student_data = student['data']
        school_id = student_data['school']
        school = requests.get(self.CLEVER_API_BASE_URL + '/v1.1/schools/%s' % school_id, headers=bearer_headers).json()

        grade = student_data.get('grade')
        external_type = None
        if grade in ["Kindergarten", "1", "2", "3"]:
            external_type = "E"
        elif grade in ["4", "5", "6", "7", "8"]:
            external_type = "M"
        elif grade in ["9", "10", "11", "12"]:
            external_type = "H"

        patron, is_new = get_one_or_create(
            _db, Patron, external_identifier=identifier,
            authorization_identifier=identifier,
        )
        patron._external_type = external_type

        return token

    def patron_info(self, identifier):
        return {}
