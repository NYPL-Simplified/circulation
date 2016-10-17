import logging
from nose.tools import set_trace
from lxml import etree
from urlparse import urljoin
from urllib import urlencode
import datetime
import requests

from core.util.xmlparser import XMLParser
from authenticator import (
    BasicAuthenticationProvider,
    PatronData,
)
from config import Configuration
import os
import re
from core.model import (
    get_one,
    get_one_or_create,
    Patron,
)

class MilleniumPatronAPI(BasicAuthenticationProvider, XMLParser):

    CONFIGURATION_NAME = "Millenium"

    RECORD_NUMBER_FIELD = 'RECORD #[p81]'
    PATRON_TYPE_FIELD = 'P TYPE[p47]'
    EXPIRATION_FIELD = 'EXP DATE[p43]'
    BARCODE_FIELD = 'P BARCODE[pb]'
    USERNAME_FIELD = 'ALT ID[pu]'
    FINES_FIELD = 'MONEY OWED[p96]'
    ERROR_MESSAGE_FIELD = 'ERRMSG'
    EXPIRATION_DATE_FORMAT = '%m-%d-%y'
    
    MULTIVALUE_FIELDS = set(['NOTE[px]', BARCODE_FIELD])

    REPORTED_LOST = re.compile("^CARD([0-9]{14})REPORTEDLOST")

    @classmethod
    def config_values(cls):
        config, values = super(MilleniumPatronAPI, cls).config_values()
        host = config.get(Configuration.URL)
        if not host:
            raise CannotLoadConfiguration(
                "Millenium Patron API server not configured."
            )
        values['host'] = host
        blacklist_strings = config.get(
            Configuration.AUTHORIZATION_IDENTIFIER_BLACKLIST, []
        )
        values['authorization_blacklist'] = blacklist_strings
        return config, values
    
    def __init__(self, host, authorization_blacklist=[], **kwargs):
        super(MilleniumPatronAPI, self).__init__(**kwargs)
        if not host.endswith('/'):
            host = host + "/"
        self.root = host
        self.parser = etree.HTMLParser()
        self.blacklist = [re.compile(x, re.I) for x in authorization_blacklist]

    # Begin implementation of BasicAuthenticationProvider abstract
    # methods.

    def remote_authenticate(self, username, password):
        """Does the Millenium Patron API approve of these credentials?

        :return: False if the credentials are invalid. If they are
        valid, a PatronData that serves only to indicate which
        authorization identifier the patron prefers.
        """
        path = "%(barcode)s/%(pin)s/pintest" % dict(
            barcode=username, pin=password
        )
        url = self.root + path
        response = self.request(url)
        data = dict(self._extract_text_nodes(response.content))
        if data.get('RETCOD') == '0':
            return PatronData(authorization_identifier=username)
        return False

    def remote_patron_lookup(self, patron_or_patrondata):
        """Ask the remote for detailed information about a patron's account.
        """
        current_identifier = patron_or_patrondata.authorization_identifier
        path = "%(barcode)s/dump" % dict(barcode=current_identifier)
        url = self.root + path
        response = self.request(url)
        return self.patron_dump_to_patrondata(
            current_identifier, response.content
        )

    # End implementation of BasicAuthenticationProvider abstract
    # methods.
    
    def request(self, url, *args, **kwargs):
        """Actually make an HTTP request. This method exists only so the mock
        can override it.
        """
        return HTTP.request_with_timeout("GET", url, *args, **kwargs)

    def patron_dump_to_patrondata(self, current_identifier, content):
        """Convert an HTML patron dump to a PatronData object.
        
        :param current_identifier: Either the authorization identifier
        the patron just logged in with, or the one currently
        associated with their Patron record. Keeping track of this
        ensures we don't change a patron's preferred authorization
        identifier out from under them.

        :param content: The HTML document containing the patron dump.
        """
        data = PatronData()
        
        # If we don't see these fields, erase any previous value
        # rather than leaving the old value in place. This shouldn't
        # happen (unless the expiration date changes to an invalid
        # date), but just to be safe.
        data.username = data.NO_VALUE
        data.authorization_expires = data.NO_VALUE
        data.fines = data.NO_VALUE
        data.external_type = data.NO_VALUE
        
        potential_identifiers = []
        for k, v in self._extract_text_nodes(content):
            if k == self.BARCODE_FIELD:
                if any(x.search(v) for x in self.blacklist):
                    # This barcode contains a blacklisted
                    # string. Ignore it, even if this means the patron
                    # ends up with no barcode whatsoever.
                    continue
                # We'll figure out which barcode is the 'right' one
                # later.
                potential_identifiers.append(v)
            elif k == self.RECORD_NUMBER_FIELD:
                data.permanent_id = v
            elif k == self.USERNAME_FIELD:
                data.username = v
            elif k == self.FINES_FIELD:
                data.fines = v
            elif k == self.EXPIRATION_FIELD:
                try:
                    expires = datetime.datetime.strptime(
                        v, self.EXPIRATION_DATE_FORMAT).date()
                except ValueError:
                    self.log.warn(
                        'Malformed expiration date for patron: "%s". Treating as unexpirable.',
                        expires
                    )
                data.authorization_expires = expires
            elif k == self.PATRON_TYPE_FIELD:
                data.external_type = v
            elif k == self.ERROR_MESSAGE_FIELD:
                # An error has occured. Most likely the patron lookup
                # failed.
                return None
        # Now we need to figure out which of the potential_identifiers
        # is the one to use.
        if current_identifier in potential_identifiers:
            # Don't rock the boat. The patron is used to using this
            # identifier and there's no need to change it.
            data.authorization_identifier=current_identifier
        elif potential_identifiers:
            # The identifier the patron is used to using has
            # disappeared. We need to choose a new one for them.
            data.authorization_identifier = potential_identifiers[-1]
        else:
            data.authorization_identifier = data.NO_VALUE
        return data
        
    def _extract_text_nodes(self, content):
        """Parse the HTML representations sent by the Millenium Patron API."""
        for line in content.split("\n"):
            if line.startswith('<HTML><BODY>'):
                line = line[12:]
            if not line.endswith('<BR>'):
                continue
            kv = line[:-4]
            if not '=' in kv:
                # This shouldn't happen, but there's no need to crash.
                self.log.warn("Unexpected line in patron dump: %s", line)
                continue
            yield kv.split('=', 1)


class MockMilleniumPatronAPI(MilleniumPatronAPI):

    """This mocks the API on a higher level than the HTTP level.

    It is not used in the tests of the MilleniumPatronAPI class.  It
    is used in the Adobe Vendor ID tests but maybe it shouldn't.
    """

    # This user's card has expired.
    user1 = PatronData(
        permanent_id="12345",
        authorization_identifier="0",
        username="alice",
        authorization_expires = datetime.datetime(2015, 4, 1)
    )
    
    # This user's card still has ten days on it.
    the_future = datetime.datetime.utcnow() + datetime.timedelta(days=10)
    user2 = PatronData(
        permanent_id="67890",
        authorization_identifier="5",
        username="bob",
        authorization_expires = the_future,
    )

    users = [user1, user2]

    def __init__(self):
        pass

    def remote_authenticate(self, barcode, pin):
        """A barcode that's 14 digits long is treated as valid,
        no matter which PIN is used.

        That's so real barcode/PIN combos can be passed through to
        third parties.

        Otherwise, valid test PIN is the first character of the barcode
        repeated four times.

        """
        u = self.dump(barcode)
        if 'ERRNUM' in u:
            return False
        return len(barcode) == 14 or pin == barcode[0] * 4

    def remote_patron_lookup(self, patron_or_patrondata):
        # We have a couple custom barcodes.
        look_for = patron_or_patrondata.authorization_identifier
        for u in self.users:
            if u.authorization_identifier == look_for:
                return u
        return None
            
AuthenticationProvider = MilleniumPatronAPI
