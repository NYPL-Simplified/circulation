from datetime import datetime
from nose.tools import set_trace
from api.authenticator import (
    BasicAuthenticationProvider,
    PatronData,
)
from api.sip.client import SIPClient

class SIP2AuthenticationProvider(BasicAuthenticationProvider):

    NAME = "SIP2"

    DATE_FORMATS = ["%Y%m%d", "%Y%m%d%Z%H%M%S", "%Y%m%d    %H%M%S"]

    def __init__(self, server, port, login_user_id,
                 login_password, location_code, field_separator,
                 client=None,
                 **kwargs):
        super(SIP2AuthenticationProvider, self).__init__(**kwargs)
        if client:
            self.client = client
        else:
            self.client = SIPClient(
                target_server=server, target_port=port,
                login_user_id=login_user_id, login_password=login_password,
                location_code=location_code, separator=field_separator
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
        if 'sipserver_internal_id' in info:
            patrondata.permanent_id = info['sipserver_internal_id']
        if 'patron_identifier' in info:
            patrondata.authorization_identifier = info['patron_identifier']
        if 'email_address' in info:
            patrondata.email_address = info['email_address']
        if 'personal_name' in info:
            patrondata.personal_name = info['personal_name']
        if 'fee_amount' in info:
            patrondata.fines = info['fee_amount']
        if 'sipserver_patron_class' in info:
            patrondata.external_type = info['sipserver_patron_class']
        for expire_field in ['sipserver_patron_expiration', 'polaris_patron_expiration']:
            if expire_field in info:
                value = info.get(expire_field)
                value = cls.parse_date(value)
                if value:
                    patrondata.authorization_expires = value
                    break
        return patrondata

    @classmethod
    def parse_date(cls, value):
        """Try to parse `value` using any of several common date formats."""
        date_value = None
        for format in cls.DATE_FORMATS:
            try:
                date_value = datetime.strptime(value, format)
                break
            except ValueError, e:
                continue
        return date_value
        
    # NOTE: It's not necessary to implement remote_patron_lookup
    # because authentication gets patron data as a side effect.
