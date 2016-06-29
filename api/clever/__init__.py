from nose.tools import set_trace
import logging
import base64
import json
import requests
import os
from flask import url_for
from flask.ext.babel import lazy_gettext as _

from api.authenticator import Authenticator
from api.config import Configuration
from core.model import (
    get_one,
    get_one_or_create,
    Patron,
)
from api.problem_details import *


UNSUPPORTED_CLEVER_USER_TYPE = pd(
    "http://librarysimplified.org/terms/problem/unsupported-clever-user-type",
    401,
    _("Your Clever user type is not supported."),
    _("Your Clever user type is not supported. You can request a code from First Book instead"),
)

CLEVER_NOT_ELIGIBLE = pd(
    "http://librarysimplified.org/terms/problem/clever-not-eligible",
    401,
    _("Your Clever account is not eligible to access this application."),
    _("Your Clever account is not eligible to access this application."),
)


# Load Title I district name data from json.
TITLE_I_DISTRICT_NAMES_BY_STATE = None
clever_dir = os.path.split(__file__)[0]

with open('%s/title_i.json' % clever_dir) as f:
    json_data = f.read()
    TITLE_I_DISTRICT_NAMES_BY_STATE = json.loads(json_data)


class CleverAuthenticationAPI(Authenticator):

    TYPE = Authenticator.OAUTH
    NAME = 'Clever'
    URI = "http://librarysimplified.org/terms/auth/clever"
    AUTHENTICATION_HEADER = 'Clever'

    CLEVER_OAUTH_URL = "https://clever.com/oauth/authorize?response_type=code&client_id=%s&redirect_uri=%s&state=Clever"
    CLEVER_TOKEN_URL = "https://clever.com/oauth/tokens"
    CLEVER_API_BASE_URL = "https://api.clever.com"

    # To check Title I status we need state, which is associated with
    # a school in Clever's API. Any users at the district-level will
    # need to get a code from First Book instead.
    SUPPORTED_USER_TYPES = ['student', 'teacher']

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

    def _redirect_uri(self):
        return url_for('oauth_callback', _external=True)

    def authenticate_url(self):
        """URL to direct patrons to for authentication with the provider."""
        return self.CLEVER_OAUTH_URL % (self.client_id, self._redirect_uri())

    def authenticated_patron(self, _db, token):
        bearer_headers = {
            'Authorization': 'Bearer %s' % token
        }

        result = requests.get(self.CLEVER_API_BASE_URL + '/me', headers=bearer_headers).json()
        data = result['data']

        identifier = data['id']

        patron = get_one(_db, Patron, authorization_identifier=identifier)
        return patron

    def _get_token(self, payload, headers):
        return requests.post(self.CLEVER_TOKEN_URL, data=json.dumps(payload), headers=headers).json()

    def _get(self, url, headers):
        return requests.get(url, headers=headers).json()

    def oauth_callback(self, _db, params):
        code = params.get('code')
        payload = dict(
            code=code,
            grant_type='authorization_code',
            redirect_uri=self._redirect_uri(),
        )
        headers = {
            'Authorization': 'Basic %s' % base64.b64encode(self.client_id + ":" + self.client_secret),
            'Content-Type': 'application/json',
        }

        response = self._get_token(payload, headers)
        token = response.get('access_token', None)

        if not token:
            return INVALID_CREDENTIALS.detailed(_("A valid Clever login is required.")), None

        bearer_headers = {
            'Authorization': 'Bearer %s' % token
        }
        result = self._get(self.CLEVER_API_BASE_URL + '/me', bearer_headers)
        data = result.get('data', {})

        identifier = data.get('id', None)

        if not identifier:
            return INVALID_CREDENTIALS.detailed(_("A valid Clever login is required.")), None            

        if result.get('type') not in self.SUPPORTED_USER_TYPES:
            return UNSUPPORTED_CLEVER_USER_TYPE, None

        links = result['links']

        user_link = [l for l in links if l['rel'] == 'canonical'][0]['uri']
        user = self._get(self.CLEVER_API_BASE_URL + user_link, bearer_headers)
        
        user_data = user['data']
        school_id = user_data['school']
        school = self._get(self.CLEVER_API_BASE_URL + '/v1.1/schools/%s' % school_id, bearer_headers)

        district_id = user_data['district']
        district = self._get(self.CLEVER_API_BASE_URL + '/v1.1/districts/%s' % district_id, bearer_headers)

        state_code = school['data']['location']['state']
        district_name = district['data']['name']

        # TODO: check student free and reduced lunch status as well

        if district_name not in TITLE_I_DISTRICT_NAMES_BY_STATE.get(state_code, []):
            self.log.info("%s in %s didn't match a Title I district name" % (district_name, state_code))
            return CLEVER_NOT_ELIGIBLE, None

        if result['type'] == 'student':
            grade = user_data.get('grade')
            external_type = None
            if grade in ["Kindergarten", "1", "2", "3"]:
                external_type = "E"
            elif grade in ["4", "5", "6", "7", "8"]:
                external_type = "M"
            elif grade in ["9", "10", "11", "12"]:
                external_type = "H"
        else:
            external_type = "A"

        patron, is_new = get_one_or_create(
            _db, Patron, external_identifier=identifier,
            authorization_identifier=identifier,
        )
        patron._external_type = external_type

        return token, dict(name=user_data.get('name'))

    def patron_info(self, identifier):
        return {}

AuthenticationAPI = CleverAuthenticationAPI
