#!/usr/bin/env python

"""A simple SIP2 client.

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
from nose.tools import set_trace
import socket
import sys
import time

class fixed(object):
    """A fixed-width field in a SIP2 response."""

    def __init__(self, internal_name, length):
        self.internal_name = internal_name
        self.length = length

    def consume(self, data, in_progress, separator):
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
        return named(self.internal_name, self.sip_code, True,
                     self.length, self.allow_multiple)

    def consume(self, data, in_progress, separator):
        keep_going = True
        if self.req and not data.startswith(self.sip_code):
            raise ValueError (
                "Unexpected field code in response (expected %s): %s" % (
                    self.sip_code, data
                )                
            )
        while data.startswith(self.sip_code):
            data = data[len(self.sip_code):]
            if self.length:
                end = self.length
            else:
                end = data.index(separator)
            value = data[:end]
            data = data[end+1:]
            if self.allow_multiple:
                in_progress.setdefault(self.internal_name,[]).append(value)
            else:
                in_progress[self.internal_name] = value
                break
        return data

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

# The spec doesn't say there can be more than one screen message,
# but I have seen it happen.
named._add("screen_message", "AF", allow_multiple=True)
named._add("print_line", "AG")

class Constants:
    UNKNOWN_LANGUAGE = "000"
    ENGLISH = "001"

       
class SIPClient(Constants):

    log = logging.getLogger("SIPClient")

    def __init__(self, target_server, target_port, login_user_id=None,
                 login_password=None, separator='|'):
        self.target_server = target_server
        self.target_port = int(target_port)
        # keeps count of messages sent to ACS
        self.sequence_index = 1
        self.socket = self.connect()
        self.separator = separator

        if login_user_id and login_password:
            # The first thing we need to do is log in.
            response = self.login(login_user_id, login_password)
            if response['login_ok'] != '1':
                raise IOError("Error logging in: %r" % response)
        
    def connect(self):
        """Create a socket connection to a SIP server."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        except socket.error, msg:
            self.log.warn("Error initializing socket: %s", msg[1])

        try:
            sock.connect((self.target_server, self.target_port))
        except socket.error, msg:
            self.log.warn("Error connecting to SIP server: %s", msg)
        return sock

    def now(self):
        """Return the current time, formatted as SIP expects it."""
        now = datetime.datetime.utcnow()
        return datetime.datetime.strftime(now, "%Y%m%d0000%H%M%S")

    def login_request(self, login_user_id, login_password, uid_algorithm="0",
                      pwd_algorithm="0"):
        message = ("93" + uid_algorithm + pwd_algorithm
                   + "CN" + login_user_id + self.separator
                   + "CO" + login_password)
        return message      

    def login_response_parser(self, message):
        return self.parse_response(
            message,
            94,
            fixed.login_ok
        )
    
    def summary(self, hold_items=False, overdue_items=False,
                charged_items=False, fine_items=False, recall_items=False,
                unavailable_holds=False):
        """Generate a summary: a 10-character query string for requesting
        detailed information about a patron's relationship with items.
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

    def patron_information_request(
            self, patron_identifier, patron_password="", institution_id="",
            terminal_password="",
            language=None, summary=None, start_item="", end_item=""
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
                   + "AO" + institution_id + self.separator +
                   "AA" + patron_identifier + self.separator +
                   "AC" + terminal_password + self.separator +
                   "AD" + patron_password
        )
        return message

    def patron_information_parser(self, data):
        """
        Sent in response to the Patron Information Request, and a superset of the Patron Status message.
        
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
        return self.parse_response(
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
        )

    def parse_response(self, data, expect_status_code, *fields):
        parsed = {}
        print data
        data = self.consume_status_code(data, str(expect_status_code), parsed)
        for field in fields:
            data = field.consume(data, parsed, self.separator)
        return parsed
    
    def consume_status_code(self, data, expected, in_progress):
        status_code = data[:2]
        in_progress['_status'] = status_code
        if status_code != expected:
            raise ValueError(
                "Unexpected status code %s: %s" % (status_code, data)
            )
        return data[2:]

    def login(self, *args, **kwargs):
        return self.make_request(
            self.login_request, self.login_response_parser,
            *args, **kwargs
        )
    
    def patron_information(self, *args, **kwargs):
        return self.make_request(
            self.patron_information_request, self.patron_information_parser,
            *args, **kwargs
        )

    def make_request(self, message_creator, parser, *args, **kwargs):
        message = message_creator(*args, **kwargs)
        message = self.append_checksum(message)
        print "Sending message: %r" % message
        self.send(message)
        response = self.read_message()
        print "Response: %r" % response
        return parser(response)

    def send(self, data, reset_sequence=False):
        """Send a message over the socket and update the sequence index."""
        if (reset_sequence):
            self.sequence_index = 0

        self.socket.send(data + '\r')
        self.sequence_index += 1
        if self.sequence_index > 9:
            self.sequence_index = 0
        
    def read_message(self, max_size=1024*1024):
        """Read a SIP2 message from the socket connection.

        A SIP2 message ends with a \r character.
        """
        done = False
        data = ""
        tmp = ""
        while not done:
            tmp = self.socket.recv(4096)
            data = data + tmp
            if not data:
                raise IOError("No data read from socket.")
            if ord(data[-1]) == 13:
                done = True
            if len(data) > max_size:
                raise IOError("SIP2 response too large.")

        return data
  
    def append_checksum(self, text, skip_index=False):
        """Calculates checksum for passed-in message, and returns the message
        with the checksum appended.  If we're using sequence_indexes,
        then appends the current index to the message.  Some
        communications (s.a. "repeat last communication" requests)
        should not have sequence indices.

        When error checking is enabled between the ACS and the SC, 
        each SC->ACS message is labeled with a sequence index (1, 2, 3, ...).
        When responding, the ACS tells the SC which sequence message it's 
        responding to.
        """
        if not skip_index:
            text = text + self.separator + "AY" + str(self.sequence_index) + "AZ"

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
