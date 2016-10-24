from datetime import datetime
from nose.tools import set_trace
from api.authenticator import (
    BasicAuthenticationProvider,
    PatronData,
)
from api.sip.client import SIPClient

class SIP2AuthenticationProvider(BasicAuthenticationProvider):

    NAME = "SIP2"
    SERVER = "server"
    PORT = "port"
    LOGIN_USER_ID = "login_user_id"
    LOGIN_PASSWORD = "login_password"
    LOCATION_CODE = "location_code"
    FIELD_SEPARATOR = "field_separator"

    DATE_FORMATS = ["%Y%m%d"]
    
    @classmethod
    def config_values(cls, configuration_name=None, required=False):
        """Retrieve constructor values from site configuration."""
        
        config, args = super(SIP2AuthenticationProvider, cls).config_values()
        args['server'] = config[cls.SERVER]
        args['port'] = config[cls.PORT]
        args['login_user_id'] = config.get(cls.LOGIN_USER_ID)
        args['login_password'] = config.get(cls.LOGIN_PASSWORD)
        args['location_code'] = config.get(cls.LOCATION_CODE)
        args['separator'] = config.get(cls.FIELD_SEPARATOR)        
        return config, args
   
    def __init__(self, server, port, login_user_id,
                 login_password, location_code, separator, client=None,
                 **kwargs):
        super(SIP2AuthenticationProvider, self).__init__(**kwargs)
        if client:
            self.client = client
        else:
            self.client = SIPClient(
                target_server=server, target_port=port,
                login_user_id=login_user_id, login_password=login_password,
                location_code=location_code, separator=separator
            )
            
    def remote_authenticate(self, username, password):
        info = self.client.patron_information(username, password)
        return self.info_to_patrondata(info)

    @classmethod
    def info_to_patrondata(cls, info):
        if info.get('valid_patron_password') == 'N':
            # The patron did not authenticate correctly. Don't
            # return any data.
            return None

            # TODO: I'm not 100% convinced that a missing CQ field
            # always means "we don't have passwords so you're
            # authenticated," rather than "you didn't provide a
            # password so we didn't check."
        patrondata = PatronData()
        if 'internal_id' in info:
            patrondata.permanent_id = info['internal_id']
        if 'patron_identifier' in info:
            patrondata.authorization_identifier = info['patron_identifier']
        if 'email_address' in info:
            patrondata.email_address = info['email_address']
        if 'personal_name' in info:
            patrondata.personal_name = info['personal_name']
        if 'fee_amount' in info:
            patrondata.fines = info['fee_amount']
        if 'patron_class' in info:
            patrondata.external_type = info['patron_class']
        if 'patron_expire' in info:
            expires = info['patron_expire']
            expires_date = None
            for format in cls.DATE_FORMATS:
                try:
                    expires_date = datetime.strptime(expires, format)
                except ValueError, e:
                    continue
            if expires_date:
                patrondata.authorization_expires = expires_date

        return patrondata 
        
    # It's not necessary to implement remote_patron_lookup because
    # authentication gets patron data as a side effect.

