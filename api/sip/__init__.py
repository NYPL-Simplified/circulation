from datetime import datetime
from nose.tools import set_trace
from flask_babel import lazy_gettext as _
from api.authenticator import (
    BasicAuthenticationProvider,
    PatronData,
)
from api.sip.client import SIPClient
from core.util.http import RemoteIntegrationException
from core.util import MoneyUtility
from core.model import ExternalIntegration

class SIP2AuthenticationProvider(BasicAuthenticationProvider):

    NAME = "SIP2"

    DATE_FORMATS = ["%Y%m%d", "%Y%m%d%Z%H%M%S", "%Y%m%d    %H%M%S"]

    # Constants for integration configuration settings.
    PORT = "port"
    LOCATION_CODE = "location code"
    FIELD_SEPARATOR = "field separator"

    SETTINGS = [
        { "key": ExternalIntegration.URL, "label": _("Server") },
        { "key": PORT, "label": _("Port") },
        { "key": ExternalIntegration.USERNAME, "label": _("Login User ID"), "optional": True },
        { "key": ExternalIntegration.PASSWORD, "label": _("Login Password"), "optional": True },
        { "key": LOCATION_CODE, "label": _("Location Code"), "optional": True },
        { "key": FIELD_SEPARATOR, "label": _("Field Separator"),
          "default": "|",
        },
    ] + BasicAuthenticationProvider.SETTINGS

    # Map the reasons why SIP2 might report a patron is blocked to the
    # protocol-independent block reason used by PatronData.
    SPECIFIC_BLOCK_REASONS = {
        SIPClient.CARD_REPORTED_LOST : PatronData.CARD_REPORTED_LOST,
        SIPClient.EXCESSIVE_FINES : PatronData.EXCESSIVE_FINES,
        SIPClient.EXCESSIVE_FEES : PatronData.EXCESSIVE_FEES,
        SIPClient.TOO_MANY_ITEMS_BILLED : PatronData.TOO_MANY_ITEMS_BILLED,
        SIPClient.CHARGE_PRIVILEGES_DENIED : PatronData.NO_BORROWING_PRIVILEGES,
        SIPClient.TOO_MANY_ITEMS_CHARGED : PatronData.TOO_MANY_LOANS,
        SIPClient.TOO_MANY_ITEMS_OVERDUE : PatronData.TOO_MANY_OVERDUE,
        SIPClient.TOO_MANY_RENEWALS : PatronData.TOO_MANY_RENEWALS,
        SIPClient.TOO_MANY_LOST : PatronData.TOO_MANY_LOST,
        SIPClient.RECALL_OVERDUE : PatronData.RECALL_OVERDUE,
    }

    def __init__(self, library, integration, analytics=None, client=None, connect=True):
        """An object capable of communicating with a SIP server.

        :param server: Hostname of the SIP server.
        :param port: The port number to connect to on the SIP server.

        :param login_user_id: SIP field CN; the user ID to use when
         initiating a SIP session, if necessary. This is _not_ a
         patron identifier (SIP field AA); it identifies the SC
         creating the SIP session. SIP2 defines SC as "...any library
         automation device dealing with patrons or library materials."

        :param login_password: Sip field CO; the password to use when
         initiating a SIP session, if necessary.

        :param location_code: SIP field CP; the location code to use
         when initiating a SIP session. A location code supposedly
         refers to the physical location of a self-checkout machine
         within a library system. Some libraries require a special
         location code to be provided when authenticating patrons;
         others may require the circulation manager to be treated as
         its own special 'location'.

        :param field_separator: The field delimiter (see
        "Variable-length fields" in the SIP2 spec). If no value is
        specified, the default (the pipe character) will be used.

        :param client: A drop-in replacement for the SIPClient
        object. Only intended for use during testing.

        :param connect: If this is false, the generated SIPClient will
        not attempt to connect to the server. Only intended for use
        during testing.
        """
        super(SIP2AuthenticationProvider, self).__init__(
            library, integration, analytics
        )

        self.server = integration.url
        self.port = integration.setting(self.PORT).int_value
        self.login_user_id = integration.username
        self.login_password = integration.password
        self.location_code = integration.setting(self.LOCATION_CODE).value
        self.field_separator = integration.setting(self.FIELD_SEPARATOR).value or '|'
        self.client = client

    def patron_information(self, username, password):
        try:
            if self.client:
                sip = self.client
            else:
                sip = SIPClient(
                    target_server=self.server, target_port=self.port,
                    login_user_id=self.login_user_id, login_password=self.login_password,
                    location_code=self.location_code, separator=self.field_separator
                )
            sip.connect()
            sip.login()
            info = sip.patron_information(username, password)
            sip.end_session(username, password)
            sip.disconnect()
            return info

        except IOError, e:
            raise RemoteIntegrationException(
                self.server or 'unknown server', e.message
            )

    def _remote_patron_lookup(self, patron_or_patrondata):
        info = self.patron_information(
            patron_or_patrondata.authorization_identifier, None
        )
        return self.info_to_patrondata(info, False)

    def remote_authenticate(self, username, password):
        """Authenticate a patron with the SIP2 server.

        :param username: The patron's username/barcode/card
            number/authorization identifier.
        :param password: The patron's password/pin/access code.
        """
        if not self.collects_password:
            # Even if we were somehow given a password, we won't be
            # passing it on.
            password = None
        info = self.patron_information(username, password)
        return self.info_to_patrondata(info)

    @classmethod
    def info_to_patrondata(cls, info, validate_password=True):

        """Convert the SIP-specific dictionary obtained from
        SIPClient.patron_information() to an abstract,
        authenticator-independent PatronData object.
        """
        if info.get('valid_patron', 'N') == 'N':
            # The patron could not be identified as a patron of this
            # library. Don't return any data.
            return None

        if info.get('valid_patron_password') == 'N' and validate_password:
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
            fines = info['fee_amount']
        else:
            fines = '0'
        patrondata.fines = MoneyUtility.parse(fines)
        if 'sipserver_patron_class' in info:
            patrondata.external_type = info['sipserver_patron_class']
        for expire_field in ['sipserver_patron_expiration', 'polaris_patron_expiration']:
            if expire_field in info:
                value = info.get(expire_field)
                value = cls.parse_date(value)
                if value:
                    patrondata.authorization_expires = value
                    break

        # A True value in most (but not all) subfields of the
        # patron_status field will prohibit the patron from borrowing
        # books.
        status = info['patron_status_parsed']
        block_reason = PatronData.NO_VALUE
        for field in SIPClient.PATRON_STATUS_FIELDS_THAT_DENY_BORROWING_PRIVILEGES:
            if status.get(field) is True:
                block_reason = cls.SPECIFIC_BLOCK_REASONS.get(
                    field, PatronData.UNKNOWN_BLOCK
                )
                if block_reason not in (PatronData.NO_VALUE,
                                        PatronData.UNKNOWN_BLOCK):
                    # Even if there are multiple problems with this
                    # patron's account, we can now present a specific
                    # error message. There's no need to look through
                    # more fields.
                    break
        patrondata.block_reason = block_reason

        # If we can tell by looking at the SIP2 message that the
        # patron has excessive fines, we can use that as the reason
        # they're blocked.
        if 'fee_limit' in info:
            fee_limit = MoneyUtility.parse(info['fee_limit']).amount
            if fee_limit and patrondata.fines > fee_limit:
                patrondata.block_reason = PatronData.EXCESSIVE_FINES

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

AuthenticationProvider = SIP2AuthenticationProvider
