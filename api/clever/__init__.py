from nose.tools import set_trace
import logging
import base64
import json
import os
import datetime
from flask import url_for
from flask.ext.babel import lazy_gettext as _

from api.authenticator import OAuthAuthenticator
from api.config import Configuration
from core.model import (
    get_one,
    get_one_or_create,
    Credential,
    DataSource,
    Patron,
)
from core.util.http import HTTP
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


# Load Title I NCES ID data from json.
TITLE_I_NCES_IDS = None
clever_dir = os.path.split(__file__)[0]

with open('%s/title_i.json' % clever_dir) as f:
    json_data = f.read()
    TITLE_I_NCES_IDS = json.loads(json_data)


class CleverAuthenticationAPI(OAuthAuthenticator):

    NAME = 'Clever'
    URI = "http://librarysimplified.org/terms/auth/clever"
    METHOD = "http://librarysimplified.org/authtype/Clever"

    CLEVER_OAUTH_URL = "https://clever.com/oauth/authorize?response_type=code&client_id=%s&redirect_uri=%s&state=%s"
    CLEVER_TOKEN_URL = "https://clever.com/oauth/tokens"
    CLEVER_API_BASE_URL = "https://api.clever.com"

    # To check Title I status we need state, which is associated with
    # a school in Clever's API. Any users at the district-level will
    # need to get a code from First Book instead.
    SUPPORTED_USER_TYPES = ['student', 'teacher']

    TOKEN_TYPE = "Clever token"

    log = logging.getLogger('Clever authentication API')

    def _data_source(self, _db):
        data_source, is_new = get_one_or_create(
            _db, DataSource, name="Clever",
            offers_licenses=False,
            primary_identifier_type=None
        )
        return data_source

    def _server_redirect_uri(self):
        return url_for('oauth_callback', _external=True)

    def external_authenticate_url(self, state):
        """URL to direct patrons to for authentication with the provider."""
        return self.CLEVER_OAUTH_URL % (self.client_id, self._server_redirect_uri(), state)

    def authenticated_patron(self, _db, token):
        credential = Credential.lookup_by_token(
            _db, self._data_source(_db), self.TOKEN_TYPE, token)

        if credential:
            return credential.patron

        # This token wasn't in our database, or was expired. The patron will have
        # to log in through clever again to get a new token.
        return None

    def _get_token(self, payload, headers):
        return HTTP.post_with_timeout(self.CLEVER_TOKEN_URL, json.dumps(payload), headers=headers).json()

    def _get(self, url, headers):
        return HTTP.get_with_timeout(url, headers=headers).json()

    def oauth_callback(self, _db, params):
        code = params.get('code')
        payload = dict(
            code=code,
            grant_type='authorization_code',
            redirect_uri=self._server_redirect_uri(),
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

        school_nces_id = school['data'].get('nces_id')

        # TODO: check student free and reduced lunch status as well

        if school_nces_id not in TITLE_I_NCES_IDS:
            self.log.info("%s didn't match a Title I NCES ID" % school_nces_id)
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

        credential, is_new = get_one_or_create(
            _db, Credential, data_source=self._data_source(_db),
            type=self.TOKEN_TYPE, patron=patron,
        )
        credential.credential = token
        credential.expires = datetime.datetime.utcnow() + datetime.timedelta(days=self.token_expiration_days)

        return token, dict(name=user_data.get('name'))

AuthenticationAPI = CleverAuthenticationAPI
