import logging
from urlparse import urlparse

from flask import request
from flask_babel import lazy_gettext as _
from onelogin.saml2.auth import OneLogin_Saml2_Auth
from onelogin.saml2.errors import OneLogin_Saml2_Error

from api.saml.configuration import SAMLConfiguration, \
    SAMLOneLoginConfiguration
from api.saml.parser import SAMLMetadataParser, SAMLSubjectParser
from core.model.configuration import ConfigurationStorage
from core.problem_details import *

SAML_GENERIC_ERROR = pd(
    'http://librarysimplified.org/terms/problem/saml/generic-error',
    status_code=500,
    title=_('SAML error.'),
    detail=_('SAML error.')
)

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
    """Implements SAML authentication process"""

    def __init__(self, configuration, subject_parser):
        """Initializes a new instance of SAMLAuthenticationManager

        :param configuration: OneLoginConfiguration object
        :type configuration: SAMLOneLoginConfiguration

        :param subject_parser: Subject parser
        :type subject_parser: SAMLSubjectParser
        """
        self._logger = logging.getLogger(__name__)

        self._configuration = configuration
        self._subject_parser = subject_parser

    @staticmethod
    def _get_request_data():
        """Maps Flask request to what the SAML toolkit expects

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

    def _create_auth_object(self, db, idp_entity_id):
        """Creates and initializes an OneLogin_Saml2_Auth object

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param idp_entity_id: IdP's entityID
        :type idp_entity_id: string

        :return: OneLogin_Saml2_Auth object
        :rtype: OneLogin_Saml2_Auth
        """
        request_data = self._get_request_data()
        settings = self._configuration.get_settings(db, idp_entity_id)
        auth = OneLogin_Saml2_Auth(request_data, old_settings=settings)

        return auth

    def _get_auth_object(self, db, idp_entity_id):
        """Returns a cached OneLogin_Saml2_Auth object

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param idp_entity_id: IdP's entityID
        :type idp_entity_id: string

        :return: OneLogin_Saml2_Auth object
        :rtype: OneLogin_Saml2_Auth
        """
        auth_object = self._create_auth_object(db, idp_entity_id)

        return auth_object

    @property
    def configuration(self):
        """Returns manager's configuration

        :return: Manager's configuration
        :rtype: SAMLOneLoginConfiguration
        """
        return self._configuration

    def start_authentication(self, db, idp_entity_id, return_to_url):
        """Starts the SAML authentication workflow by sending a AuthnRequest to the IdP

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param idp_entity_id: IdP's entityID
        :type idp_entity_id: string

        :param return_to_url: URL which will the user agent will be redirected to after authentication
        :type return_to_url: string

        :return: Redirection URL
        :rtype: string
        """
        try:
            auth = self._get_auth_object(db, idp_entity_id)
            redirect_url = auth.login(return_to_url)

            if self._logger.isEnabledFor(logging.DEBUG):
                self._logger.debug('SAML request: {0}'.format(auth.get_last_request_xml()))

            return redirect_url
        except OneLogin_Saml2_Error as exception:
            self._logger.exception('Unexpected exception occurred while initiating a SAML flow')

            return SAML_GENERIC_ERROR.detailed(exception.message)

    def finish_authentication(self, db, idp_entity_id):
        """Finishes the SAML authentication workflow by validating AuthnResponse and extracting a SAML assertion from it

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param idp_entity_id: IdP's entityID
        :type idp_entity_id: string

        :return: Subject object containing name ID and attributes in the case of a successful authentication
            or ProblemDetail object otherwise
        :rtype: Union[api.saml.metadata.Subject, ProblemDetail]
        """
        request_data = self._get_request_data()

        if 'post_data' not in request_data or 'SAMLResponse' not in request_data['post_data']:
            return SAML_INCORRECT_RESPONSE.detailed('There is no SAMLResponse in the body of the response')

        auth = self._get_auth_object(db, idp_entity_id)
        auth.process_response()

        if self._logger.isEnabledFor(logging.DEBUG):
            self._logger.debug('SAML response: {0}'.format(auth.get_last_response_xml()))

        authenticated = auth.is_authenticated()

        if authenticated:
            subject = self._subject_parser.parse(auth)

            return subject
        else:
            self._logger.error(auth.get_last_error_reason())

            return SAML_AUTHENTICATION_ERROR.detailed(auth.get_last_error_reason())


class SAMLAuthenticationManagerFactory(object):
    """Responsible for creating SAMLAuthenticationManager instances"""

    def create(self, integration_owner):
        """
        Creates a new instance of SAMLAuthenticationManager class

        :param integration_owner: External integration owner
        :type integration_owner: api.saml.configuration.ExternalIntegrationOwner

        :return: SAML authentication manager
        :rtype: SAMLAuthenticationManager
        """
        configuration_storage = ConfigurationStorage(integration_owner)
        configuration = SAMLConfiguration(configuration_storage, SAMLMetadataParser())
        onelogin_configuration = SAMLOneLoginConfiguration(configuration)
        subject_parser = SAMLSubjectParser()
        authentication_manager = SAMLAuthenticationManager(onelogin_configuration, subject_parser)

        return authentication_manager
