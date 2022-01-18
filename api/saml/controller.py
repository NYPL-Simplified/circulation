import json
import logging

from flask import redirect
from flask_babel import lazy_gettext as _
from urllib.parse import (
    SplitResult,
    parse_qs,
    urlencode,
    urlparse,
    urlsplit,
    urlunparse,
)

from api.problem_details import *
from api.saml.auth import SAMLAuthenticationManager
from api.saml.configuration.model import SAMLConfigurationFactory
from api.saml.metadata.parser import SAMLMetadataParser
from core.util.problem_detail import ProblemDetail
from core.util.problem_detail import json as pd_json

SAML_INVALID_REQUEST = pd(
    "http://librarysimplified.org/terms/problem/saml/invalid-saml-request",
    status_code=401,
    title=_("SAML invalid request."),
    detail=_("SAML invalid request."),
)

SAML_INVALID_RESPONSE = pd(
    "http://librarysimplified.org/terms/problem/saml/invalid-saml-response",
    status_code=401,
    title=_("SAML invalid response."),
    detail=_("SAML invalid response."),
)


class SAMLController(object):
    """Controller used for handing SAML 2.0 authentication requests"""

    ERROR = "error"
    REDIRECT_URI = "redirect_uri"
    PROVIDER_NAME = "provider"
    IDP_ENTITY_ID = "idp_entity_id"
    LIBRARY_SHORT_NAME = "library_short_name"
    RELAY_STATE = "RelayState"
    ACCESS_TOKEN = "access_token"
    PATRON_INFO = "patron_info"

    def __init__(self, circulation_manager, authenticator):
        """Initializes a new instance of SAMLController class

        :param circulation_manager: Circulation Manager
        :type circulation_manager: CirculationManager

        :param authenticator: Authenticator object used to route requests to the appropriate LibraryAuthenticator
        :type authenticator: Authenticator
        """
        self._circulation_manager = circulation_manager
        self._authenticator = authenticator

        self._logger = logging.getLogger(__name__)
        self._configuration_factory = SAMLConfigurationFactory(SAMLMetadataParser())

    @staticmethod
    def _get_authentication_manager(db, authentication_provider):
        """Returns an instance of SAML authentication manager

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param authentication_provider: SAML authentication provider
        :type authentication_provider: api.saml.provider.SAMLWebSSOAuthenticationProvider

        :return: Authentication manager
        :rtype: SAMLAuthenticationManager
        """
        with authentication_provider.get_configuration(db) as configuration:
            return authentication_provider.get_authentication_manager(configuration)

    @staticmethod
    def _add_params_to_url(url, params):
        """Adds parameters as a query part of the URL

        :param url: URL
        :type url: string

        :param params: Dictionary containing parameters
        :type params: Dict

        :return: URL with parameters formatted as a query string
        :rtype: string
        """
        url_parts = urlsplit(url)

        # Extract the existing parameters specified in the redirection URI
        existing_params = parse_qs(url_parts.query)

        # Enrich our custom parameters with the existing ones
        params.update(existing_params)

        new_query = urlencode(params, True)
        url_parts = SplitResult(
            url_parts.scheme,
            url_parts.netloc,
            url_parts.path,
            new_query,
            url_parts.fragment,
        )
        url = url_parts.geturl()

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
            problem_detail.debug_message,
        )
        params = {self.ERROR: problem_detail_json}
        redirect_uri = self._add_params_to_url(redirect_uri, params)

        return redirect_uri

    def _get_redirect_uri(self, relay_state):
        """Returns a redirection URL from the relay state

        :param relay_state: SAML response's relay state
        :type relay_state: string

        :return: Redirection URL
        :rtype: string
        """
        relay_state_parse_result = urlparse(relay_state)
        relay_state_parameters = parse_qs(relay_state_parse_result.query)

        if self.LIBRARY_SHORT_NAME in relay_state_parameters:
            del relay_state_parameters[self.LIBRARY_SHORT_NAME]

        if self.PROVIDER_NAME in relay_state_parameters:
            del relay_state_parameters[self.PROVIDER_NAME]

        if self.IDP_ENTITY_ID in relay_state_parameters:
            del relay_state_parameters[self.IDP_ENTITY_ID]

        redirect_uri = urlunparse(
            (
                relay_state_parse_result.scheme,
                relay_state_parse_result.netloc,
                relay_state_parse_result.path,
                relay_state_parse_result.params,
                urlencode(relay_state_parameters, True),
                relay_state_parse_result.fragment,
            )
        )

        return redirect_uri

    @staticmethod
    def _get_request_parameter(params, name, default_value=None):
        """Returns a parameter containing in the incoming request

        :param params: Request's parameters
        :type params: Dict

        :param default_value: Optional default value
        :type params: Optional[Any]

        :return: Parameter's value or ProblemDetail instance if the parameter is missing
        :rtype: Union[string, ProblemDetail]
        """
        parameter = params.get(name, default_value)

        if not parameter:
            return SAML_INVALID_REQUEST.detailed(
                _("Required parameter {0} is missing".format(name))
            )

        return parameter

    @staticmethod
    def _get_relay_state_parameter(relay_parameters, name):
        """Returns a parameter containing in the query string of the relay state returned by the IdP

        :param relay_parameters: Dictionary containing a list of parameters
        :type relay_parameters: Dict

        :param name: Name of the parameter
        :type name: string

        :return: Parameter's value or ProblemDetail if the parameter is missing
        :rtype: Union[string, ProblemDetail]
        """
        if name not in relay_parameters:
            return SAML_INVALID_RESPONSE.detailed(
                _("Required parameter {0} is missing from RelayState".format(name))
            )

        return relay_parameters[name][0]

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
        provider_name = self._get_request_parameter(params, self.PROVIDER_NAME)
        if isinstance(provider_name, ProblemDetail):
            return provider_name

        idp_entity_id = self._get_request_parameter(params, self.IDP_ENTITY_ID)
        if isinstance(idp_entity_id, ProblemDetail):
            return idp_entity_id

        redirect_uri = self._get_request_parameter(params, self.REDIRECT_URI)
        if isinstance(redirect_uri, ProblemDetail):
            return redirect_uri

        provider = self._authenticator.bearer_token_provider_lookup(provider_name)
        if isinstance(provider, ProblemDetail):
            return self._redirect_with_error(redirect_uri, provider)

        with provider.get_configuration(db) as configuration:
            authentication_manager = provider.get_authentication_manager(configuration)

            # In general relay state should contain only a redirect URL.
            # However, we need to pass additional parameters which will be required in saml_authentication_callback.
            # There is no other way to pass them back to the Circulation Manager from the IdP.
            # We have to add them to the query part of the relay state and then remove them
            # before redirecting to the URL containing in the relay state.
            # The required parameters are:
            # - library's name
            # - SAML provider's name
            # - IdP's entity ID
            relay_state = self._add_params_to_url(
                redirect_uri,
                {
                    # NOTE: we cannot use @has_library decorator and append a library's name
                    # to SAMLController.saml_callback route (e.g. https://cm.org/LIBRARY_NAME/saml_callback).
                    # The URL of the SP's assertion consumer service (SAMLController.saml_callback) should be constant:
                    # SP's metadata is registered in the IdP and cannot change.
                    # If we try to append a library's name to the ACS's URL sent as a part of the SAML request,
                    # the IdP will fail this request because the URL mentioned in the request and
                    # the URL saved in the SP's metadata configured in this IdP will differ.
                    # Library's name is passed as a part of the relay state and
                    # processed in SAMLController.saml_authentication_callback
                    self.LIBRARY_SHORT_NAME: provider.library(db).short_name,
                    self.PROVIDER_NAME: provider_name,
                    self.IDP_ENTITY_ID: idp_entity_id,
                },
            )
            redirect_uri = authentication_manager.start_authentication(
                db, idp_entity_id, relay_state
            )
            if isinstance(redirect_uri, ProblemDetail):
                return redirect_uri

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
            return SAML_INVALID_RESPONSE.detailed(
                _(
                    "Required parameter {0} is missing from the response body".format(
                        self.RELAY_STATE
                    )
                )
            )

        relay_state = request.form[self.RELAY_STATE]
        relay_state_parse_result = urlparse(relay_state)
        relay_state_parameters = parse_qs(relay_state_parse_result.query)

        library_short_name = self._get_relay_state_parameter(
            relay_state_parameters, self.LIBRARY_SHORT_NAME
        )
        if isinstance(library_short_name, ProblemDetail):
            return library_short_name

        provider_name = self._get_relay_state_parameter(
            relay_state_parameters, self.PROVIDER_NAME
        )
        if isinstance(provider_name, ProblemDetail):
            return provider_name

        idp_entity_id = self._get_relay_state_parameter(
            relay_state_parameters, self.IDP_ENTITY_ID
        )
        if isinstance(idp_entity_id, ProblemDetail):
            return idp_entity_id

        redirect_uri = self._get_redirect_uri(relay_state)

        library = self._circulation_manager.index_controller.library_for_request(
            library_short_name
        )
        if isinstance(library, ProblemDetail):
            return self._redirect_with_error(redirect_uri, library)

        provider = self._authenticator.bearer_token_provider_lookup(provider_name)
        if isinstance(provider, ProblemDetail):
            return self._redirect_with_error(redirect_uri, provider)

        with provider.get_configuration(db) as configuration:
            authentication_manager = provider.get_authentication_manager(configuration)

            subject = authentication_manager.finish_authentication(db, idp_entity_id)
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
            params = {"access_token": simplified_token, "patron_info": patron_info}

            redirect_uri = self._add_params_to_url(redirect_uri, params)

            return redirect(redirect_uri)
