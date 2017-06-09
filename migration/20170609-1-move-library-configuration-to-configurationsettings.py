#!/usr/bin/env python
"""Move per-library settings from the Configuration file
into the database as ConfigurationSettings.
"""
import os
import sys
import json
import logging
from nose.tools import set_trace

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

log = logging.getLogger(name="Circulation manager library configuration import")

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
    
try:
    Configuration.load()
    _db = production_session()
    integrations = []

    # If the secret key is set, make it a sitewide setting.
    secret_key = Configuration.get('secret_key')
    if secret_key:
        secret_setting = ConfigurationSetting.sitewide(
            _db, Configuration.SECRET_KEY
        )
        secret_setting.value = secret_key
    
    library = Library.instance(_db)
    libraries = _db.query(Library).all()
    
    # Copy default email address into each library.
    key = 'default_notification_email_address'
    value = Configuration.get(key)
    if value:
        for library in libraries:
            ConfigurationSetting.for_library(key, library).value = value

    # Copy maximum fines into each library.
    key = 'max_outstanding_fines'
    value = Configuration.policy(key)
    if value:
        for library in libraries:
            ConfigurationSetting.for_library(key, library).value = value

    # Copy external type regular expression into each collection for each
    # library.
    key = 'external_type_regular_expression'
    value = Configuration.policy(key)
    if value:
        for library in libraries:
            for collection in library.collections:
                integration = collection.external_integration
                if not integration:
                    continue
                ConfigurationSetting.for_library_and_externalintegration(
                    _db, key, library, integration).value = value
                
finally:
    _db.commit()
    _db.close()
