#!/usr/bin/env python
"""Move authentication integration details from the Configuration file
into the database as ExternalIntegrations
"""
import os
import sys
import json
import logging

bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from core.model import (
    ConfigurationSetting,
    ExternalIntegration,
    get_one_or_create,
    production_session,
    Library,
    create,
)

from api.config import Configuration
from api.millenium_patron import MilleniumPatronAPI
from api.sip import SIP2AuthenticationProvider
from api.authenticator import (
    BasicAuthenticationProvider,
    OAuthAuthenticationProvider,
)

log = logging.getLogger(name="Circulation manager authentication configuration import")

def log_import(service_name):
    log.info("Importing configuration for %s" % service_name)

def make_patron_auth_integration(_db, provider):
    integration, ignore = get_one_or_create(
        _db, ExternalIntegration, protocol=provider.get('module'),
        goal=ExternalIntegration.PATRON_AUTH_GOAL
    )

    # If any of the common Basic Auth-type settings were provided, set them
    # as ConfigurationSettings on the ExternalIntegration.
    test_identifier = provider.get('test_username')
    test_password = provider.get('test_password')
    if test_identifier:
        integration.setting(BasicAuthenticationProvider.TEST_IDENTIFIER).value = test_identifier
    if test_password:
        integration.setting(BasicAuthenticationProvider.TEST_PASSWORD).value = test_password
    identifier_re = provider.get('identifier_regular_expression')
    password_re = provider.get('password_regular_expression')
    if identifier_re:
        integration.setting(BasicAuthenticationProvider.IDENTIFIER_REGULAR_EXPRESSION).value = identifier_re
    if password_re:
        integration.setting(BasicAuthenticationProvider.PASSWORD_REGULAR_EXPRESSION).value = password_re

    return integration

def convert_millenium(_db, integration, provider):

    # Cross-check MilleniumPatronAPI.__init__ to see how these values
    # are pulled from the ExternalIntegration.
    integration.url = provider.get('url')
    auth_mode = provider.get('auth_mode')
    blacklist = provider.get('authorization_identifier_blacklist')
    if blacklist:
        integration.setting(MilleniumPatronAPI.IDENTIFIER_BLACKLIST
        ).value = json.dumps(blacklist)
    if auth_mode:
        integration.setting(MilleniumPatronAPI.AUTHENTICATION_MODE
        ).value = auth_mode

def convert_sip(_db, integration, provider):
    # Cross-check SIP2AuthenticationProvider.__init__ to see how these values
    # are pulled from the ExternalIntegration.
    integration.url = provider.get('server')
    integration.username = provider.get('login_user_id')
    integration.password = provider.get('login_password')
    SAP = SIP2AuthenticationProvider
    port = provider.get('port')
    if port:
        integration.setting(SAP.PORT).value = port
    location_code = provider.get('location_code')
    if location_code:
        integration.setting(SAP.LOCATION_CODE).value = location_code
    field_separator = provider.get('field_separator')
    if field_separator:
        integration.setting(SAP.FIELD_SEPARATOR).value = field_separator

def convert_firstbook(_db, integration, provider):
    # Cross-check FirstBookAuthenticationAPI.__init__ to see how these values
    # are pulled from the ExternalIntegration.
    integration.url = provider.get('url')
    integration.password = provider.get('key')

def convert_clever(_db, integration, provider):
    # Cross-check OAuthAuthenticationProvider.from_config to see how
    # these values are pulled from the ExternalIntegration.
    integration.username = provider.get('client_id')
    integration.password = provider.get('client_secret')
    expiration_days = provider.get('token_expiration_days')
    if expiration_days:
        integration.setting(OAuthAuthenticationProvider.TOKEN_EXPIRATION_DAYS
        ).value = expiration_days

Configuration.load()
if not Configuration.instance:
    # No need to import configuration if there isn't any.
    sys.exit()

_db = production_session()
try:
    integrations = []
    auth_conf = Configuration.policy('authentication')
    if not auth_conf:
        sys.exit()

    bearer_token_signing_secret = auth_conf.get('bearer_token_signing_secret')
    secret_setting = ConfigurationSetting.sitewide(
        _db, OAuthAuthenticationProvider.BEARER_TOKEN_SIGNING_SECRET
    )
    if bearer_token_signing_secret:
        secret_setting.value = bearer_token_signing_secret

    for provider in auth_conf.get('providers'):
        integration = make_patron_auth_integration(_db, provider)
        module = provider.get('module')
        if module == 'api.millenium_patron':
            convert_millenium(_db, integration, provider)
        elif module == 'api.firstbook':
            convert_firstbook(_db, integration, provider)
        elif module == 'api.clever':
            convert_clever(_db, integration, provider)
        elif module == 'api.sip':
            convert_sip(_db, integration, provider)
        else:
            log.warn("I don't know how to convert a provider of type %s. Conversion is probably incomplete." % module)
        integrations.append(integration)

    # Add each integration to each library.
    library = Library.default(_db)
    for library in _db.query(Library):
        for integration in integrations:
            if integration not in library.integrations:
                library.integrations.append(integration)

    print "Sitewide bearer token signing secret: %s" % secret_setting.value
    for library in _db.query(Library):
        print "\n".join(library.explain(include_secrets=True))
finally:
    _db.commit()
    _db.close()
