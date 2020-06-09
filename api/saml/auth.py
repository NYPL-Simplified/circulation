import logging
from urlparse import urlparse

from flask import request
from flask_babel import lazy_gettext as _
from onelogin.saml2.auth import OneLogin_Saml2_Auth

from api.saml.configuration import SAMLConfigurationSerializer, SAMLMetadataSerializer, SAMLConfiguration, \
    SAMLOneLoginConfiguration
from api.saml.metadata import NameID, AttributeStatement, Subject
from core.problem_details import *

SAML_INCORRECT_RESPONSE = pd(
    'http://librarysimplified.org/terms/problem/saml/incorrect-response',
    status_code=400,
    title=_('SAML incorrect response.'),
    detail=_('SAML incorrect response.')
)


SAML_AUTHENTICATION_ERROR = pd(
    'http://librarysimplified.org/terms/problem/saml/authentication-error',
    status_code=401,
    title=_('SAML authentication error.'),
    detail=_('SAML authentication error.')
)


class SAMLAuthenticationManager(object):
    """
    Implements SAML authentication process
    """

    def __init__(self, configuration):
        """
        Initializes a new instance of SAMLAuthenticationManager

        :param configuration: OneLoginConfiguration object
        :type configuration: SAMLOneLoginConfiguration
        """

        self._logger = logging.getLogger(__name__)

        self._configuration = configuration
        self._auth_objects = {}

    @staticmethod
    def _get_request_data():
        """
        Maps Flask request to what the SAML toolkit expects

        :return: Dictionary containing information about the request in the format SAML toolkit expects
        """
        # If server is behind proxys or balancers use the HTTP_X_FORWARDED fields
        url_data = urlparse(request.url)

        return {
            'https': 'on' if request.scheme == 'https' else 'off',
            'http_host': request.host,
            'server_port': url_data.port,
            'script_name': request.path,
            'get_data': request.args.copy(),
            # Uncomment if using ADFS as IdP, https://github.com/onelogin/python-saml/pull/144
            # 'lowercase_urlencoding': True,
            'post_data': request.form.copy(),
        }

    def _create_auth_object(self, idp_entity_id):
        """
        Creates and initializes an OneLogin_Saml2_Auth object

        :param idp_entity_id: IdP's entityID
        :type idp_entity_id: string

        :return: OneLogin_Saml2_Auth object
        :rtype: OneLogin_Saml2_Auth
        """

        request_data = self._get_request_data()
        settings = self._configuration.get_settings(idp_entity_id)
        auth = OneLogin_Saml2_Auth(request_data, old_settings=settings)

        return auth

    def _get_auth_object(self, idp_entity_id):
        """
        Returns a cached OneLogin_Saml2_Auth object

        :param idp_entity_id: IdP's entityID
        :type idp_entity_id: string

        :return: Cached OneLogin_Saml2_Auth object
        :rtype: OneLogin_Saml2_Auth
        """

        if idp_entity_id not in self._auth_objects:
            self._auth_objects[idp_entity_id] = self._create_auth_object(idp_entity_id)

        return self._auth_objects[idp_entity_id]

    def start_authentication(self, idp_entity_id, return_to_url):
        """
        Starts the SAML authentication workflow by sending a AuthnRequest to the IdP

        :param idp_entity_id: IdP's entityID
        :type idp_entity_id: string

        :param return_to_url: URL which will the user agent will be redirected to after authentication
        :type return_to_url: string

        :return: Redirection URL
        :rtype: string
        """

        auth = self._get_auth_object(idp_entity_id)

        return auth.login(return_to_url)

    def finish_authentication(self, idp_entity_id):
        """
        Finishes the SAML authentication workflow by validating AuthnResponse and extracting a SAML assertion from it

        :param idp_entity_id: IdP's entityID
        :type idp_entity_id: string

        :return:
        """

        request_data = self._get_request_data()

        if 'post_data' not in request_data or 'SAMLResponse' not in request_data['post_data']:
            return SAML_INCORRECT_RESPONSE.detailed('There is no SAMLResponse in the body of the response')

        auth = self._get_auth_object(idp_entity_id)
        auth.process_response()

        authenticated = auth.is_authenticated()

        if authenticated:
            name_id = NameID(
                auth.get_nameid_format(),
                auth.get_nameid_nq(),
                auth.get_nameid_spnq(),
                auth.get_nameid()
            )
            attributes = auth.get_attributes()
            attribute_statement = AttributeStatement(attributes)
            subject = Subject(name_id, attribute_statement)

            return subject
        else:
            self._logger.error(auth.get_last_error_reason())

            return SAML_AUTHENTICATION_ERROR.detailed(auth.get_last_error_reason())


class SAMLAuthenticationManagerFactory(object):
    """
    Responsible for creating SAMLAuthenticationManager instances
    """

    def create(self, integration):
        """
        Creates a new instance of SAMLAuthenticationManager class

        :param integration: External integration
        :type integration: ExternalIntegration

        :return: SAML authentication manager
        :rtype: SAMLAuthenticationManager
        """

        configuration_serializer = SAMLConfigurationSerializer(integration)
        metadata_serializer = SAMLMetadataSerializer(integration)
        configuration = SAMLConfiguration(configuration_serializer, metadata_serializer)
        onelogin_configuration = SAMLOneLoginConfiguration(configuration)
        authentication_manager = SAMLAuthenticationManager(onelogin_configuration)

        return authentication_manager
