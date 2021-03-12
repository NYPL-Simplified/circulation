#!/usr/bin/env python
"""A simple SIP2 client.

Implementation is guided by the SIP2 specification:
 http://multimedia.3m.com/mws/media/355361O/sip2-protocol.pdf

This client implements a very small part of SIP2 but is easily extensible.

This client is based on sip2talk.py. Here is the original licensing
information for sip2talk.py:

Copyright [2010] [Eli Fulkerson]

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

import datetime
import logging
import os
import re
import socket
import ssl
import tempfile
from api.sip.dialect import GenericILS
from nose.tools import set_trace

# SIP2 defines a large number of fields which are used in request and
# response messages. This library focuses on defining the response
# fields in a way that makes it easy to reliably parse response
# documents.

class fixed(object):
    """A fixed-width field in a SIP2 response."""

    def __init__(self, internal_name, length):
        self.internal_name = internal_name
        self.length = length

    def consume(self, data, in_progress):
        """Remove the value of this field from the beginning of the
        input string, and store it in the given dictionary.

        :param in_progress: A dictionary mapping field names to
            values. The value of this field will be stored in this
            dictionary.

        :return: The original input string, after the value of this
            field has been removed.
        """
        value = data[:self.length]
        in_progress[self.internal_name] = value
        return data[self.length:]

    @classmethod
    def _add(cls, internal_name, *args, **kwargs):
        obj = cls(internal_name, *args, **kwargs)
        setattr(cls, internal_name, obj)

fixed._add('patron_status', 14)
fixed._add('language', 3)
fixed._add('transaction_date', 18)
fixed._add('hold_items_count', 4)
fixed._add('overdue_items_count', 4)
fixed._add('charged_items_count', 4)
fixed._add('fine_items_count', 4)
fixed._add('recall_items_count', 4)
fixed._add('unavailable_holds_count', 4)
fixed._add('login_ok', 1)
fixed._add('end_session', 1)

class named(object):
    """A variable-length field in a SIP2 response."""
    def __init__(self, internal_name, sip_code, required=False,
                 length=None, allow_multiple=False):
        self.sip_code = sip_code
        self.internal_name = internal_name
        self.req=required
        self.length = length
        self.allow_multiple = allow_multiple

    @property
    def required(self):
        """Create a variant of this field which is required.

        Most variable-length fields are not required, but certain
        fields may be required in the responses to specific types of
        requests.

        To check whether a specific field actually is required, check
        `field.req`.
        """
        return named(self.internal_name, self.sip_code, True,
                     self.length, self.allow_multiple)

    def consume(self, value, in_progress):
        """Process the given value for this field.

        Unlike fixed.consume, this does not modify the value -- it's
        assumed that this particular field value has already been
        isolated from the response string.

        :param in_progress: A dictionary mapping field names to
            values. The value of this field will be stored in this
            dictionary.
        """
        if self.length and len(value) != self.length:
            self.log.warn(
                "Expected string of length %d for field %s, but got %r",
                self.length, self.sip_code, value
            )
        if self.allow_multiple:
            in_progress.setdefault(self.internal_name,[]).append(value)
        else:
            in_progress[self.internal_name] = value

    @classmethod
    def _add(cls, internal_name, *args, **kwargs):
        obj = cls(internal_name, *args, **kwargs)
        setattr(cls, internal_name, obj)

named._add("institution_id", "AO")
named._add("patron_identifier", "AA")
named._add("personal_name", "AE")
named._add("hold_items_limit", "BZ", length=4)
named._add("overdue_items_limit", "CA", length=4)
named._add("charged_items_limit", "CB", length=4)
named._add("valid_patron", "BL", length=1)
named._add("valid_patron_password", "CQ", length=1)
named._add("currency_type", "BH", length=3)
named._add("fee_amount", "BV")
named._add("fee_limit", "CC")
named._add("hold_items", "AS", allow_multiple=True)
named._add("overdue_items", "AT", allow_multiple=True)
named._add("charged_items", "AU", allow_multiple=True)
named._add("fine_items", "AV", allow_multiple=True)
named._add("recall_items", "BU", allow_multiple=True)
named._add("unavailable_hold_items", "CD", allow_multiple=True)
named._add("home_address", "BD")
named._add("email_address", "BE")
named._add("phone_number", "BF")
named._add("sequence_number", "AY")

# The spec doesn't say there can be more than one screen message,
# but I have seen it happen.
named._add("screen_message", "AF", allow_multiple=True)
named._add("print_line", "AG")

# SIP extensions defined by Georgia Public Library Service's SIP
# server, used by Evergreen and Koha.
named._add('sipserver_patron_expiration', 'PA')
named._add('sipserver_patron_class', 'PC')
named._add('sipserver_internet_privileges', 'PI')
named._add('sipserver_internal_id', 'XI')

# SIP extensions defined by Polaris.
named._add('polaris_patron_birthdate', 'BC')
named._add('polaris_postal_code', 'PZ')
named._add('polaris_patron_expiration', 'PX')
named._add('polaris_patron_expired', 'PY')

# A potential problem: Polaris defines PA to refer to something else.

class RequestResend(IOError):
    """There was an error transmitting a message and the server has requested
    that it be resent.
    """

class Constants(object):
    UNKNOWN_LANGUAGE = "000"
    ENGLISH = "001"


class SIPClient(Constants):

    log = logging.getLogger("SIPClient")

    # Maximum retries of a SIP message before failing.
    MAXIMUM_RETRIES = 5

    # These are the subfield names associated with the 'patron status'
    # field as specified in the SIP2 spec.
    CHARGE_PRIVILEGES_DENIED = 'charge privileges denied'
    RENEWAL_PRIVILEGES_DENIED = 'renewal privileges denied'
    RECALL_PRIVILEGES_DENIED = 'recall privileges denied'
    HOLD_PRIVILEGES_DENIED = 'hold privileges denied'
    CARD_REPORTED_LOST = 'card reported lost'
    TOO_MANY_ITEMS_CHARGED = 'too many items charged'
    TOO_MANY_ITEMS_OVERDUE = 'too many items overdue'
    TOO_MANY_RENEWALS = 'too many renewals'
    TOO_MANY_RETURN_CLAIMS = 'too many claims of items returned'
    TOO_MANY_LOST= 'too many items lost'
    EXCESSIVE_FINES = 'excessive outstanding fines'
    EXCESSIVE_FEES = 'excessive outstanding fees'
    RECALL_OVERDUE = 'recall overdue'
    TOO_MANY_ITEMS_BILLED = 'too many items billed'

    # All the flags, in the order they're used in the 'patron status'
    # field.
    PATRON_STATUS_FIELDS = [
        CHARGE_PRIVILEGES_DENIED,
        RENEWAL_PRIVILEGES_DENIED,
        RECALL_PRIVILEGES_DENIED,
        HOLD_PRIVILEGES_DENIED,
        CARD_REPORTED_LOST,
        TOO_MANY_ITEMS_CHARGED,
        TOO_MANY_ITEMS_OVERDUE,
        TOO_MANY_RENEWALS,
        TOO_MANY_RETURN_CLAIMS,
        TOO_MANY_LOST,
        EXCESSIVE_FINES,
        EXCESSIVE_FEES,
        RECALL_OVERDUE,
        TOO_MANY_ITEMS_BILLED
    ]

    # Some, but not all, of these fields, imply that a patron has lost
    # borrowing privileges.
    PATRON_STATUS_FIELDS_THAT_DENY_BORROWING_PRIVILEGES = [
        CHARGE_PRIVILEGES_DENIED,
        CARD_REPORTED_LOST,
        TOO_MANY_ITEMS_CHARGED,
        TOO_MANY_ITEMS_OVERDUE,
        TOO_MANY_LOST,
        EXCESSIVE_FINES,
        EXCESSIVE_FEES,
        RECALL_OVERDUE,
        TOO_MANY_ITEMS_BILLED
    ]

    def __init__(self, target_server, target_port, login_user_id=None,
                 login_password=None, location_code=None, institution_id='', separator=None,
                 use_ssl=False, ssl_cert=None, ssl_key=None, dialect=GenericILS
    ):
        """Initialize a client for (but do not connect to) a SIP2 server.

        :param use_ssl: If this is True, all socket connections to the SIP2
            server will be wrapped with SSL.
        :param ssl_cert: A string containing an SSL certificate to use when
            connecting to the SIP server.
        :param ssl_key: A string containing an SSL certificate to use when
            connecting to the SIP server.
        """
        self.target_server = target_server
        if not target_port:
            target_port = 6001
        if target_port:
            self.target_port = int(target_port)
        self.location_code = location_code
        self.institution_id = institution_id
        self.separator = separator or '|'

        self.use_ssl = use_ssl or ssl_cert or ssl_key
        self.ssl_cert = ssl_cert
        self.ssl_key = ssl_key

        # Turn the separator string into a regular expression that splits
        # field name/field value pairs on the separator string.
        if self.separator in '|.^$*+?{}()[]\\':
            escaped = '\\' + self.separator
        else:
            escaped = self.separator
        self.separator_re = re.compile(escaped + "([A-Z][A-Z])")

        self.sequence_number = 0
        self.connection = None
        self.login_user_id = login_user_id
        if login_user_id:
            if not login_password:
                login_password = ''
            # We need to log in before using this server.
            self.must_log_in = True
        else:
            # We're implicitly logged in.
            self.must_log_in = False
        self.login_password = login_password
        self.dialect = dialect

    def login(self):
        """Log in to the SIP server if required."""
        if self.must_log_in:
            response = self.make_request(
                self.login_message, self.login_response_parser,
                self.login_user_id, self.login_password, self.location_code
            )
            if response['login_ok'] != '1':
                raise IOError("Error logging in: %r" % response)
            return response

    def patron_information(self, *args, **kwargs):
        """Get information about a patron.
        """
        return self.make_request(
            self.patron_information_request, self.patron_information_parser,
            *args, **kwargs
        )

    def end_session(self, *args, **kwargs):
        """Send end session message."""
        if self.dialect.sendEndSession:
            return self.make_request(
                self.end_session_message, self.end_session_response_parser,
                *args, **kwargs
            )
        else:
            return None

    def connect(self):
        """Create a socket connection to a SIP server."""
        try:
            if self.connection:
                # If we are still connected then disconnect.
                self.disconnect()
            if self.use_ssl:
                self.connection = self.make_secure_connection()
            else:
                self.connection = self.make_insecure_connection()
            
            self.connection.settimeout(12)
            self.connection.connect((self.target_server, self.target_port))
        except socket.error as message:
            raise IOError(
                "Could not connect to %s:%s - %s" % (
                    self.target_server, self.target_port, message
                )
            )
        
        # Since this is a new socket connection, reset the message count
        self.reset_connection_state()

    def make_insecure_connection(self):
        """Actually set up a socket connection."""
        return socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def make_secure_connection(self):
        """Create an SSL-enabled socket connection."""

        # If a certificate and/or key were provided, write them to
        # temporary files so OpenSSL can find them.
        #
        # Unfortunately there's no way to get OpenSSL to read a
        # certificate or key from a string. Alternatives suggested
        # online include M2Crypt, a pure Python SSL implementation.
        # M2Crypt seems like it will work, but I couldn't find the
        # documentation I needed, so for the time being... temporary
        # files.
        tmp_ssl_cert_path = None
        tmp_ssl_key_path = None
        if self.ssl_cert:
            fd, tmp_ssl_cert_path = tempfile.mkstemp()
            os.write(fd, self.ssl_cert.encode("utf-8"))
            os.close(fd)
        if self.ssl_key:
            fd, tmp_ssl_key_path = tempfile.mkstemp()
            os.write(fd, self.ssl_key.encode("utf-8"))
            os.close(fd)
        connection = self.make_insecure_connection()
        connection = ssl.wrap_socket(
            connection, certfile=tmp_ssl_cert_path,
            keyfile=tmp_ssl_key_path
        )

        # Now that the connection has been established, the temporary
        # files are no longer needed. Remove them.
        for path in tmp_ssl_cert_path, tmp_ssl_key_path:
            if path and os.path.exists(path):
                os.remove(path)

        return connection

    def reset_connection_state(self):
        """Reset connection-specific state.
        Specifically, the sequence number.
        """
        self.sequence_number = 0

    def disconnect(self):
        """Close the connection to the SIP server."""
        self.connection.close()
        self.connection = None

    def make_request(self, message_creator, parser, *args, **kwargs):
        """Send a request to a SIP server and parse the response.

        :param connection: Socket to send data over.
        :param message_creator: A function that creates the message to send.
        :param parser: A function that parses the response message.
        """
        original_message = message_creator(*args, **kwargs)
        message_with_checksum = self.append_checksum(original_message)
        parsed = None
        retries = 0
        while not parsed:
            if retries >= self.MAXIMUM_RETRIES:
                # Only retry MAXIMUM_RETRIES times in case we we are sending
                # a message the ILS doesn't like, so we don't retry forever
                raise IOError('Maximum SIP retries reached')
            self.send(message_with_checksum)
            response = self.read_message()
            try:
                parsed = parser(response)
            except RequestResend as e:
                # Instead of a response, we got a request to resend the data.
                # Generate a new checksum but do not include or increment
                # the sequence number.
                message_with_checksum = self.append_checksum(
                    original_message, include_sequence_number=False
                )
            retries += 1
        return parsed

    def login_message(self, login_user_id, login_password, location_code="",
                      uid_algorithm="0",
                      pwd_algorithm="0"):
        """Generate a message for logging in to a SIP server."""
        message = ("93" + uid_algorithm + pwd_algorithm
                   + "CN" + login_user_id + self.separator
                   + "CO" + login_password
        )
        if location_code:
            message = message + self.separator + "CP" + location_code
        return message

    def login_response_parser(self, message):
        """Parse the response from a login message."""
        return self.parse_response(
            message,
            94,
            fixed.login_ok
        )

    def end_session_message(
            self, patron_identifier, patron_password="",
            terminal_password="",
    ):
        """
        This message will be sent when a patron has completed all of their
        transactions. The ACS may, upon receipt of this command, close any
        open files or deallocate data structures pertaining to that patron.
        The ACS should respond with an End Session Response message.

        Format of message to send to ILS:
        35<transaction date><institution id><patron identifier>
        <terminal password><patron password>
        transaction date: 18-char, YYYYMMDDZZZZHHMMSS, required
        institution id: AO, variable length, required
        patron identifier: AA, variable length, required
        terminal password: AC, variable length, optional
        patron password: AD, variable length, optional
        """
        code = "35"
        timestamp = self.now()

        message = (code + timestamp +
                   "AO" + self.institution_id + self.separator +
                   "AA" + patron_identifier + self.separator +
                   "AC" + terminal_password
        )
        if patron_password:
            message += self.separator + "AD" + patron_password
        return message

    def end_session_response_parser(self, message):
        """Parse the response from a end session message."""
        return self.parse_response(
            message,
            36,
            fixed.end_session,
            fixed.transaction_date,
            named.institution_id.required,
            named.patron_identifier.required,
            named.screen_message,
            named.print_line
        )

    def patron_information_request(
            self, patron_identifier, patron_password="",
            terminal_password="",
            language=None, summary=None
    ):
        """
        A superset of patron status request.

        Format of message to send to ILS:
        63<language><transaction date><summary><institution id><patron identifier>
        <terminal password><patron password><start item><end item>
        language: 3-char, required
        transaction date: 18-char, YYYYMMDDZZZZHHMMSS, required
        summary: 10-char, required
        institution id: AO, variable length, required
        patron identifier: AA, variable length, required
        terminal password: AC, variable length, optional
        patron password: AD, variable length, optional
        start item: BP, variable length, optional
        end item: BQ, variable length, optional
        """
        code = "63"
        language = language or self.UNKNOWN_LANGUAGE
        timestamp = self.now()
        summary = summary or self.summary()

        message = (code + language + timestamp + summary
                   + "AO" + self.institution_id + self.separator +
                   "AA" + patron_identifier + self.separator +
                   "AC" + terminal_password
        )
        if patron_password:
            message += self.separator + "AD" + patron_password
        return message

    def patron_information_parser(self, data):
        """
        Parse the message sent in response to a patron information request.

        Format of message expected from ILS:
        64<patron status><language><transaction date><hold items count><overdue items count>
        <charged items count><fine items count><recall items count><unavailable holds count>
        <institution id><patron identifier><personal name><hold items limit><overdue items limit>
        <charged items limit><valid patron><valid patron password><currency type><fee amount>
        <fee limit><items><home address><e-mail address><home phone number><screen message><print line>

        patron status: 14-char, required
        language: 3-char, req
        transaction date: 18-char, YYYYMMDDZZZZHHMMSS, required
        hold items count: 4-char, required
        overdue items count: 4-char, required
        charged items count: 4-char, required
        fine items count: 4-char, required
        recall items count: 4-char, required
        unavailable holds count: 4-char, required
        institution id: AO, variable-length, required
        patron identifier: AA, var-length, req
        personal name: AE, var-length, req
        hold items limit: BZ, 4-char, optional
        overdue items limit: CA, 4-char, optional
        charged items limit: CB, 4-char, optional
        valid patron: BL, 1-char, Y/N, optional
        valid patron password: CQ, 1-char, Y/N, optional
        currency type: BH, 3-char, optional
        fee amount: BV, var-length.  The amount of fees owed by this patron.
        fee limit: CC, variable-length, optional
        items: 0 or more instances of one of the following, based on "summary" field of patron information message
        hold items: AS, var-length opt (should be sent for each hold item)
        overdue items: AT, var-length opt (should be sent for each overdue item)
        charged items: AU, var-length opt (should be sent for each charged item)
        fine items: AV, var-length opt (should be sent for each fine item)
        recall items: BU, var-length opt (should be sent for each recall item)
        unavailable hold items: CD, var-length opt (should be sent for each unavailable hold item)
        home address: BD, variable-length, optional
        email address: VE, variable-length, optional
        home phone number: BF, variable-length optional

        screen message: AF, var-length, optional
        print line: AG, var-length, optional
        """
        response = self.parse_response(
            data,
            64,
            fixed.patron_status,
            fixed.language,
            fixed.transaction_date,
            fixed.hold_items_count,
            fixed.overdue_items_count,
            fixed.charged_items_count,
            fixed.fine_items_count,
            fixed.recall_items_count,
            fixed.unavailable_holds_count,
            named.institution_id.required,
            named.patron_identifier.required,
            named.personal_name.required,
            named.hold_items_limit,
            named.overdue_items_limit,
            named.charged_items_limit,
            named.valid_patron,
            named.valid_patron_password,
            named.currency_type,
            named.fee_amount,
            named.fee_limit,
            named.hold_items,
            named.overdue_items,
            named.charged_items,
            named.fine_items,
            named.recall_items,
            named.unavailable_hold_items,
            named.home_address,
            named.email_address,
            named.phone_number,
            named.screen_message,
            named.print_line,

            # Add common extension fields.
            named.sipserver_patron_expiration,
            named.polaris_patron_expiration,
            named.sipserver_patron_class,
            named.sipserver_internet_privileges,
            named.sipserver_internal_id
        )

        # As a convenience, parse the patron_status field from a
        # 14-character string into a dictionary of booleans.
        try:
            parsed = self.parse_patron_status(response.get('patron_status'))
        except ValueError as e:
            parsed = {}
        response['patron_status_parsed'] = parsed
        return response

    def parse_response(self, data, expect_status_code, *fields):
        """Verify that the given response string starts with the expected
        status code. Then extract the values of both fixed-width and
        named fields.

        :param return: A dictionary containing the parsed-out information.
        """
        parsed = {}
        data = self.consume_status_code(data, str(expect_status_code), parsed)

        fields_by_sip_code = dict()

        required_fields_not_seen = set()

        # We've been given a list of unnamed fixed-width fields (which
        # must appear at the front) followed by a list of named
        # fields. Named fields must appear after the fixed-width
        # fields but otherwise may appear in any order, some of them
        # multiple times. Some named fields may themselves have a
        # fixed-width requirement.
        #
        # Go through the list once, consume all the unnamed
        # fixed-width fields, and build a dictionary of named fields
        # to use later.
        for field in fields:
            if isinstance(field, fixed):
                data = field.consume(data, parsed)
            else:
                fields_by_sip_code[field.sip_code] = field
                if field.req:
                    required_fields_not_seen.add(field)

        # We now have a list of named fields separated by
        # self.separator.  Use separator_re to split the data in a way
        # that minimizes the chances that embedded separators (which
        # shouldn't happen, but do) don't ruin the data.
        split = self.separator_re.split(data)

        # We now have alternating field name/value pairs, except for
        # the first field, which wasn't split because it didn't start with
        # the separator. Fix that.
        first_field = split[0]
        first_field = [first_field[:2], first_field[2:]]
        split = first_field + split[1:]
        i = 0

        # Now go through each name/value pair, find the corresponding
        # field object, and process it.
        while i < len(split):
            sip_code = split[i]
            value = split[i+1]
            if sip_code == named.sequence_number.sip_code:
                # Sequence number is special in two ways. First, it
                # indicates the end of the message. Second, it doesn't
                # have to be explicitly mentioned in the list of
                # fields -- we always expect it.
                named.sequence_number.consume(value, parsed)
                break
            else:
                field = fields_by_sip_code.get(sip_code)
                if sip_code and not field:
                    # This is an extension field. Do the best we can.
                    # This basically means storing it in the dictionary
                    # under its SIP code.
                    field = named(sip_code, sip_code, allow_multiple=True)

                if field:
                    field.consume(value, parsed)
                    if field.required and field in required_fields_not_seen:
                        required_fields_not_seen.remove(field)
            i += 2

        # If a named field is required and never showed up, sound the alarm.
        for field in required_fields_not_seen:
            self.log.error(
                "Expected required field %s but did not find it.",
                field.sip_code
            )
        return parsed

    def consume_status_code(self, data, expected, in_progress):
        """Pull the status code (the first two characters) off the
        given response string, and verify that it's as expected.
        """
        status_code = data[:2]
        in_progress['_status'] = status_code
        if status_code != expected:
            if status_code == '96': # Request SC Resend
                raise RequestResend()
            else:
                raise IOError(
                    "Unexpected status code %s: %s" % (status_code, data)
                )
        return data[2:]

    @classmethod
    def parse_patron_status(cls, status_string):
        """Parse the raw 14-character patron_status string.

        :return: A 14-element dictionary mapping flag names to boolean values.
        """
        if (not isinstance(status_string, (bytes, str))
            or len(status_string) != 14):
            raise ValueError(
                "Patron status must be a 14-character string."
            )
        status = {}
        for i, field in enumerate(cls.PATRON_STATUS_FIELDS):
            # ' ' means false, 'Y' means true.
            value = status_string[i] != ' '
            status[field] = value
        return status

    def now(self):
        """Return the current time, formatted as SIP expects it."""
        now = datetime.datetime.utcnow()
        return datetime.datetime.strftime(now, "%Y%m%d0000%H%M%S")

    def summary(self, hold_items=False, overdue_items=False,
                charged_items=False, fine_items=False, recall_items=False,
                unavailable_holds=False):
        """Generate the SIP summary field: a 10-character query string for
        requesting detailed information about a patron's relationship
        with items.
        """
        summary = ""
        for item in (
                hold_items, overdue_items,
                charged_items, fine_items, recall_items,
                unavailable_holds
        ):
            if item:
                summary += "Y"
            else:
                summary += " "
        # The last four spaces are always empty.
        summary += '    '
        if summary.count('Y') > 1:
            # This violates the spec but in my tests it seemed to
            # work, so we'll allow it.
            self.log.warn(
                'Summary requested too many kinds of detailed information: %s' %
                summary
            )
        return summary

    def send(self, data):
        """Send a message over the socket and update the sequence index."""
        data = data + '\r'
        return self.do_send(data)

    def do_send(self, data):
        """Actually send data over the socket.

        This method exists only to be subclassed by MockSIPClient.
        """
        self.connection.send(data)

    def read_message(self, max_size=1024*1024):
        """Read a SIP2 message from the socket connection.

        A SIP2 message ends with a \\r character.
        """
        done = False
        data = ""
        while not done:
            tmp = self.connection.recv(4096)
            data = data + tmp
            if not tmp:
                raise IOError("No data read from socket.")
            if ord(data[-1]) == 13 or ord(data[-1]) == 10:
                done = True
            if len(data) > max_size:
                raise IOError("SIP2 response too large.")
        return data

    def append_checksum(self, text, include_sequence_number=True):
        """Calculates checksum for passed-in message, and returns the message
        with the checksum appended.

        :param include_sequence_number: If this is true, include the
            current sequence number in the message, just before the
            checksum, and increment the sequence number. If this is false,
            do not include or increment the sequence number.

        When error checking is enabled between the ACS and the SC,
        each SC->ACS message is labeled with a sequence number (0, 1, 2, ...).
        When responding, the ACS tells the SC which sequence message it's
        responding to.
        """

        text += self.separator

        if include_sequence_number:
            text += "AY" + str(self.sequence_number)
            # Sequence numbers range from 0-9 and wrap around.
            self.sequence_number += 1
            if self.sequence_number > 9:
                self.sequence_number = 0

        # Finally, add the checksum.
        text += "AZ"

        check = 0
        for each in text:
            check = check + ord(each)
        check = check + ord('\0')
        check = (check ^ 0xFFFF) + 1

        checksum = "%4.4X" % (check)

        # Note that the checksum doesn't have the pipe character
        # before its AZ tag.  This is as should be.
        text += checksum

        return text


class MockSIPClient(SIPClient):
    """A SIP client that relies on canned responses rather than a socket
    connection.
    """

    def __init__(self, login_user_id=None, login_password=None, separator="|",
                 target_server=None, target_port=None, location_code=None, institution_id=''):
        super(MockSIPClient, self).__init__(
            None, None, login_user_id=login_user_id,
            login_password=login_password, separator=separator, institution_id=institution_id
        )

        self.read_count = 0
        self.write_count = 0
        self.requests = []
        self.responses = []
        self.status = []

    def queue_response(self, response):
        self.responses.append(response)

    def connect(self):
        # Since there is no socket, do nothing but reset the local
        # connection-specific variables.
        self.status.append("Creating new socket connection.")
        self.reset_connection_state()
        return None

    def do_send(self, data):
        self.write_count += 1
        self.requests.append(data)

    def read_message(self, max_size=1024*1024):
        """Read a response message off the queue."""
        self.read_count += 1
        response = self.responses[0]
        self.responses = self.responses[1:]
        return response

    def disconnect(self):
        pass
