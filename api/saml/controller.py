import json
import urllib
import urlparse

from flask import request, redirect
from flask_babel import lazy_gettext as _

from api.problem_details import *
from api.saml.auth import SAMLAuthenticationManager
from core.util.problem_detail import ProblemDetail
from core.util.problem_detail import (
    json as pd_json,
)

SAML_INVALID_RESPONSE = pd(
    'http://librarysimplified.org/terms/problem/saml/invalid-saml-response',
    status_code=401,
    title=_('SAML invalid response.'),
    detail=_('SAML invalid response.')
)


class SAMLController(object):
    """Controller used for handing SAML 2.0 authentication requests"""

    ERROR = 'error'
    REDIRECT_URI = 'redirect_uri'
    PROVIDER_NAME = 'provider'
    IDP_ENTITY_ID = 'idp_entity_id'
    LIBRARY_SHORT_NAME = 'library_short_name'
    RELAY_STATE = 'RelayState'
    ACCESS_TOKEN = 'access_token'
    PATRON_INFO = 'patron_info'

    def __init__(self, circulation_manager, authenticator, authentication_manager_factory):
        """Initializes a new instance of SAMLController class

        :param circulation_manager: Circulation Manager
        :type circulation_manager: CirculationManager

        :param authenticator: Authenticator object used to route requests to the appropriate LibraryAuthenticator
        :type authenticator: Authenticator

        :param authentication_manager_factory: SAML authentication manager factory
        :type authentication_manager_factory: SAMLAuthenticationManagerFactory
        """
        self._circulation_manager = circulation_manager
        self._authenticator = authenticator
        self._authentication_manager_factory = authentication_manager_factory

    def _get_authentication_manager(self, db, authentication_provider):
        """Returns an instance of SAML authentication manager

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param authentication_provider: SAML authentication provider
        :type authentication_provider: SAMLAuthenticationProvider

        :return: Authentication manager
        :rtype: SAMLAuthenticationManager
        """
        return self._authentication_manager_factory.create(authentication_provider.external_integration(db))

    def _add_params_to_url(self, url, params):
        """Adds parameters as a query part of the URL

        :param url: URL
        :type url: string

        :param params: Dictionary containing parameters
        :type params: Dict

        :return: URL with parameters formatted as a query string
        :rtype: string
        """
        query = urllib.urlencode(params)

        if '?' in url:
            url += '&' + query
        else:
            url += '?' + query

        return url

    def _error_uri(self, redirect_uri, problem_detail):
        """Encodes the given ProblemDetail into the fragment identifier of the given URI

        :param redirect_uri: Redirection URL
        :type redirect_uri: string

        :param problem_detail: ProblemDetail object describing the error
        :type problem_detail: ProblemDetail

        :return: Redirection URL
        :rtype: string
        """
        problem_detail_json = pd_json(
            problem_detail.uri,
            problem_detail.status_code,
            problem_detail.title,
            problem_detail.detail,
            problem_detail.instance,
            problem_detail.debug_message
        )
        params = {
            self.ERROR: problem_detail_json
        }
        redirect_uri = self._add_params_to_url(redirect_uri, params)

        return redirect_uri

    def _get_redirect_uri(self, relay_state):
        """Returns a redirection URL from the relay state

        :param relay_state: SAML response's relay state
        :type relay_state: string

        :return: Redirection URL
        :rtype: string
        """
        relay_state_parse_result = urlparse.urlparse(relay_state)
        relay_state_parameters = urlparse.parse_qs(relay_state_parse_result.query)

        del relay_state_parameters[self.PROVIDER_NAME]
        del relay_state_parameters[self.IDP_ENTITY_ID]
        del relay_state_parameters[self.LIBRARY_SHORT_NAME]

        redirect_uri = urlparse.urlunparse(
            (
                relay_state_parse_result.scheme,
                relay_state_parse_result.netloc,
                relay_state_parse_result.path,
                relay_state_parse_result.params,
                urllib.urlencode(relay_state_parameters),
                relay_state_parse_result.fragment,
            )
        )

        return redirect_uri

    def _redirect_with_error(self, redirect_uri, problem_detail):
        """Redirects the patron to the given URL, with the given ProblemDetail encoded into the fragment identifier

        :param redirect_uri: Redirection URL
        :type redirect_uri: string

        :param problem_detail: ProblemDetail object describing the error
        :type problem_detail: ProblemDetail

        :return: Redirection response
        :rtype: Response
        """
        return redirect(self._error_uri(redirect_uri, problem_detail))

    def saml_authentication_redirect(self, params, db):
        """Redirects an unauthenticated patron to the authentication URL of the
        appropriate SAML IdP.
        Over on that other site, the patron will authenticate and be
        redirected back to the circulation manager, ending up in
        saml_authentication_callback.

        :param params: Query parameters
        :type params: Dict

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :return: Redirection response
        :rtype: Response
        """
        provider_name = params.get(self.PROVIDER_NAME)
        idp_entity_id = params.get(self.IDP_ENTITY_ID)
        redirect_uri = params.get(self.REDIRECT_URI, request.path)

        provider = self._authenticator.saml_provider_lookup(provider_name)
        if isinstance(provider, ProblemDetail):
            return self._redirect_with_error(redirect_uri, provider)

        authentication_manager = self._get_authentication_manager(db, provider)
        relay_state = self._add_params_to_url(
            redirect_uri,
            {
                self.LIBRARY_SHORT_NAME: provider.library(db).short_name,
                self.PROVIDER_NAME: provider_name,
                self.IDP_ENTITY_ID: idp_entity_id
            }
        )
        redirect_uri = authentication_manager.start_authentication(idp_entity_id, relay_state)

        return redirect(redirect_uri)

    def saml_authentication_callback(self, request, db):
        """Creates a Patron object and a bearer token for a patron who has just
        authenticated with one of our SAML IdPs

        :param request: Flask request
        :type request: Request

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :return: Redirection response or a ProblemDetail if the response is not correct
        :rtype: Union[Response, ProblemDetail]
        """
        if self.RELAY_STATE not in request.form:
            return SAML_INVALID_RESPONSE.detailed('{0} is empty'.format(self.RELAY_STATE))

        relay_state = request.form[self.RELAY_STATE]
        relay_state_parse_result = urlparse.urlparse(relay_state)
        relay_state_parameters = urlparse.parse_qs(relay_state_parse_result.query)
        library_short_name = relay_state_parameters[self.LIBRARY_SHORT_NAME][0]
        provider_name = relay_state_parameters[self.PROVIDER_NAME][0]
        idp_entity_id = relay_state_parameters[self.IDP_ENTITY_ID][0]
        redirect_uri = self._get_redirect_uri(relay_state)

        library = self._circulation_manager.index_controller.library_for_request(library_short_name)
        if isinstance(library, ProblemDetail):
            return self._redirect_with_error(redirect_uri, library)

        provider = self._authenticator.saml_provider_lookup(provider_name)
        if isinstance(provider, ProblemDetail):
            return self._redirect_with_error(redirect_uri, provider)

        subject = self._get_authentication_manager(db, provider).finish_authentication(idp_entity_id)
        if isinstance(subject, ProblemDetail):
            return self._redirect_with_error(redirect_uri, subject)

        response = provider.saml_callback(db, subject)
        if isinstance(response, ProblemDetail):
            return self._redirect_with_error(redirect_uri, response)

        provider_token, patron, patron_data = response

        # Turn the provider token into a bearer token we can give to
        # the patron
        simplified_token = self._authenticator.create_bearer_token(
            provider.NAME, provider_token.credential
        )

        patron_info = json.dumps(patron_data.to_response_parameters)
        params = {
            'access_token': simplified_token,
            'patron_info': patron_info
        }

        redirect_uri = self._add_params_to_url(redirect_uri, params)

        return redirect(redirect_uri)
