import logging
from lxml import etree
from urllib.parse import urljoin
from urllib.parse import urlencode
import datetime
import requests
from money import Money
from flask_babel import lazy_gettext as _

from core.util.xmlparser import XMLParser
from .authenticator import (
    BasicAuthenticationProvider,
    PatronData,
)
from .config import (
    Configuration,
    CannotLoadConfiguration,
)
import os
import re
from core.model import (
    get_one,
    get_one_or_create,
    ExternalIntegration,
    Patron,
)
from core.util.http import HTTP
from core.util import MoneyUtility

class MilleniumPatronAPI(BasicAuthenticationProvider, XMLParser):

    NAME = "Millenium"

    RECORD_NUMBER_FIELD = 'RECORD #[p81]'
    PATRON_TYPE_FIELD = 'P TYPE[p47]'
    EXPIRATION_FIELD = 'EXP DATE[p43]'
    HOME_BRANCH_FIELD = 'HOME LIBR[p53]'
    ADDRESS_FIELD = 'ADDRESS[pa]'
    BARCODE_FIELD = 'P BARCODE[pb]'
    USERNAME_FIELD = 'ALT ID[pu]'
    FINES_FIELD = 'MONEY OWED[p96]'
    BLOCK_FIELD = 'MBLOCK[p56]'
    ERROR_MESSAGE_FIELD = 'ERRMSG'
    PERSONAL_NAME_FIELD = 'PATRN NAME[pn]'
    EMAIL_ADDRESS_FIELD = 'EMAIL ADDR[pz]'
    EXPIRATION_DATE_FORMAT = '%m-%d-%y'

    MULTIVALUE_FIELDS = set(['NOTE[px]', BARCODE_FIELD])

    DEFAULT_CURRENCY = "USD"

    # Identifiers that contain any of these strings are ignored when
    # finding the "correct" identifier in a patron's record, even if
    # it means they end up with no identifier at all.
    IDENTIFIER_BLACKLIST = 'identifier_blacklist'

    # A configuration value for whether or not to validate the SSL certificate
    # of the Millenium Patron API server.
    VERIFY_CERTIFICATE = "verify_certificate"

    # The field to use when validating a patron's credential.
    AUTHENTICATION_MODE = 'auth_mode'
    PIN_AUTHENTICATION_MODE = 'pin'
    FAMILY_NAME_AUTHENTICATION_MODE = 'family_name'

    NEIGHBORHOOD_MODE = 'neighborhood_mode'
    NO_NEIGHBORHOOD_MODE = 'disabled'
    HOME_BRANCH_NEIGHBORHOOD_MODE = 'home_branch'
    POSTAL_CODE_NEIGHBORHOOD_MODE = 'postal_code'
    NEIGHBORHOOD_MODES = set(
        [NO_NEIGHBORHOOD_MODE, HOME_BRANCH_NEIGHBORHOOD_MODE, POSTAL_CODE_NEIGHBORHOOD_MODE]
    )

    # The field to use when seeing which values of MBLOCK[p56] mean a patron
    # is blocked. By default, any value other than '-' indicates a block.
    BLOCK_TYPES = 'block_types'

    AUTHENTICATION_MODES = [
        PIN_AUTHENTICATION_MODE, FAMILY_NAME_AUTHENTICATION_MODE
    ]

    SETTINGS = [
        { "key": ExternalIntegration.URL, "format": "url", "label": _("URL"), "required": True },
        { "key": VERIFY_CERTIFICATE, "label": _("Certificate Verification"),
          "type": "select", "options": [
              { "key": "true", "label": _("Verify Certificate Normally (Required for production)") },
              { "key": "false", "label": _("Ignore Certificate Problems (For temporary testing only)") },
          ],
          "default": "true"
        },
        { "key": BLOCK_TYPES, "label": _("Block types"),
          "description": _("Values of MBLOCK[p56] which mean a patron is blocked. By default, any value other than '-' indicates a block."),
        },
        { "key": IDENTIFIER_BLACKLIST, "label": _("Identifier Blacklist"),
          "type": "list",
          "description": _("Identifiers containing any of these strings are ignored when finding the 'correct' " +
                           "identifier for a patron's record, even if it means they end up with no identifier at all. " +
                           "If librarians invalidate library cards by adding strings like \"EXPIRED\" or \"INVALID\" " +
                           "on to the beginning of the card number, put those strings here so the Circulation Manager " +
                           "knows they do not represent real card numbers."),
        },
        { "key": AUTHENTICATION_MODE, "label": _("Authentication Mode"),
          "type": "select",
          "options": [
              { "key": PIN_AUTHENTICATION_MODE, "label": _("PIN") },
              { "key": FAMILY_NAME_AUTHENTICATION_MODE, "label": _("Family Name") },
          ],
          "default": PIN_AUTHENTICATION_MODE
        },
        {
            "key": NEIGHBORHOOD_MODE,
            "label": _("Patron neighborhood field"),
            "description": _("It's sometimes possible to guess a patron's neighborhood from their ILS record. You can use this when analyzing circulation activity by neighborhood. If you don't need to do this, it's better for patron privacy to disable this feature."),
            "type": "select",
            "options": [
                { "key": NO_NEIGHBORHOOD_MODE, "label": _("Disable this feature") },
                { "key": HOME_BRANCH_NEIGHBORHOOD_MODE, "label": _("Patron's home library branch is their neighborhood.") },
                { "key": POSTAL_CODE_NEIGHBORHOOD_MODE, "label": _("Patron's postal code is their neighborhood.") },
            ],
            "default": NO_NEIGHBORHOOD_MODE,
        },
    ] + BasicAuthenticationProvider.SETTINGS

    # Replace library settings to allow text in identifier field.
    LIBRARY_SETTINGS = []
    for setting in BasicAuthenticationProvider.LIBRARY_SETTINGS:
        if setting['key'] == BasicAuthenticationProvider.LIBRARY_IDENTIFIER_FIELD:
            LIBRARY_SETTINGS.append({
                "key": BasicAuthenticationProvider.LIBRARY_IDENTIFIER_FIELD,
                "label": _("Library Identifier Field"),
                "description": _("This is the field on the patron record that the <em>Library Identifier Restriction " +
                                 "Type</em> is applied to. The option 'barcode' matches the users barcode, other " +
                                 "values are pulled directly from the patron record for example: 'P TYPE[p47]'. " +
                                 "This value is not used if <em>Library Identifier Restriction Type</em> " +
                                 "is set to 'No restriction'."),
            })
        else:
            LIBRARY_SETTINGS.append(setting)

    def __init__(self, library, integration, analytics=None):
        super(MilleniumPatronAPI, self).__init__(library, integration, analytics)
        url = integration.url
        if not url:
            raise CannotLoadConfiguration(
                "Millenium Patron API server not configured."
            )

        if not url.endswith('/'):
            url = url + "/"
        self.root = url
        self.verify_certificate = integration.setting(
            self.VERIFY_CERTIFICATE).json_value
        if self.verify_certificate is None:
            self.verify_certificate = True
        self.parser = etree.HTMLParser()

        # In a Sierra ILS, a patron may have a large number of
        # identifiers, some of which are not real library cards. A
        # blacklist allows us to exclude certain types of identifiers
        # from being considered as library cards.
        authorization_identifier_blacklist = integration.setting(
            self.IDENTIFIER_BLACKLIST).json_value or []
        self.blacklist = [re.compile(x, re.I)
                          for x in authorization_identifier_blacklist]

        auth_mode = integration.setting(
            self.AUTHENTICATION_MODE).value or self.PIN_AUTHENTICATION_MODE

        if auth_mode not in self.AUTHENTICATION_MODES:
            raise CannotLoadConfiguration(
                "Unrecognized Millenium Patron API authentication mode: %s." % auth_mode
            )
        self.auth_mode = auth_mode

        self.block_types = integration.setting(self.BLOCK_TYPES).value or None

        neighborhood_mode = integration.setting(
            self.NEIGHBORHOOD_MODE
        ).value or self.NO_NEIGHBORHOOD_MODE
        if neighborhood_mode not in self.NEIGHBORHOOD_MODES:
            raise CannotLoadConfiguration(
                "Unrecognized Millenium Patron API neighborhood mode: %s." % neighborhood_mode
            )
        self.neighborhood_mode = neighborhood_mode


    # Begin implementation of BasicAuthenticationProvider abstract
    # methods.

    def _request(self, path):
        """Make an HTTP request and parse the response."""

    def remote_authenticate(self, username, password):
        """Does the Millenium Patron API approve of these credentials?

        :return: False if the credentials are invalid. If they are
            valid, a PatronData that serves only to indicate which
            authorization identifier the patron prefers.
        """
        if not self.collects_password:
            # We don't even look at the password. If the patron exists, they
            # are authenticated.
            patrondata = self._remote_patron_lookup(username)
            if not patrondata:
                return False
            return patrondata

        if self.auth_mode == self.PIN_AUTHENTICATION_MODE:
            # Patrons are authenticated with a secret PIN.
            path = "%(barcode)s/%(pin)s/pintest" % dict(
                barcode=username, pin=password
            )
            url = self.root + path
            response = self.request(url)
            data = dict(self._extract_text_nodes(response.content))
            if data.get('RETCOD') == '0':
                return PatronData(authorization_identifier=username, complete=False)
            return False
        elif self.auth_mode == self.FAMILY_NAME_AUTHENTICATION_MODE:
            # Patrons are authenticated by their family name.
            patrondata = self._remote_patron_lookup(username)
            if not patrondata:
                # The patron doesn't even exist.
                return False

            # The patron exists; but do the last names match?
            if self.family_name_match(patrondata.personal_name, password):
                # Since this is a complete PatronData, we'll be able
                # to update their account without making a separate
                # call to /dump.
                return patrondata
        return False

    @classmethod
    def family_name_match(self, actual_name, supposed_family_name):
        """Does `supposed_family_name` match `actual_name`?"""
        if actual_name is None or supposed_family_name is None:
            return False
        if actual_name.find(',') != -1:
            actual_family_name = actual_name.split(',')[0]
        else:
            actual_name_split = actual_name.split(' ')
            actual_family_name = actual_name_split[-1]
        if actual_family_name.upper() == supposed_family_name.upper():
            return True
        return False

    def _remote_patron_lookup(self, patron_or_patrondata_or_identifier):
        if isinstance(patron_or_patrondata_or_identifier, str):
            identifier = patron_or_patrondata_or_identifier
        else:
            identifier = patron_or_patrondata_or_identifier.authorization_identifier
        """Look up patron information for the given identifier."""
        path = "%(barcode)s/dump" % dict(barcode=identifier)
        url = self.root + path
        response = self.request(url)
        return self.patron_dump_to_patrondata(identifier, response.content)


    # End implementation of BasicAuthenticationProvider abstract
    # methods.

    def request(self, url, *args, **kwargs):
        """Actually make an HTTP request. This method exists only so the mock
        can override it.
        """
        self._update_request_kwargs(kwargs)
        return HTTP.request_with_timeout("GET", url, *args, **kwargs)

    def _update_request_kwargs(self, kwargs):
        """Modify the kwargs to HTTP.request_with_timeout to reflect the API
        configuration, in a testable way.
        """
        kwargs['verify'] = self.verify_certificate

    @classmethod
    def _patron_block_reason(cls, block_types, mblock_value):
        """Turn a value of the MBLOCK[56] field into a block type."""

        if block_types and mblock_value in block_types:
            # We are looking for a specific value, and we found it
            return PatronData.UNKNOWN_BLOCK

        if not block_types:
            # Apply the default rules.
            if not mblock_value or mblock_value.strip() in ('', '-'):
                # This patron is not blocked at all.
                return PatronData.NO_VALUE
            else:
                # This patron is blocked for an unknown reason.
                return PatronData.UNKNOWN_BLOCK

        # We have specific types that mean the patron is blocked.
        if mblock_value in block_types:
            # The patron has one of those types. They are blocked.
            return PatronData.UNKNOWN_BLOCK

        # The patron does not have one of those types, so is not blocked.
        return PatronData.NO_VALUE

    def patron_dump_to_patrondata(self, current_identifier, content):
        """Convert an HTML patron dump to a PatronData object.

        :param current_identifier: Either the authorization identifier
            the patron just logged in with, or the one currently
            associated with their Patron record. Keeping track of this
            ensures we don't change a patron's preferred authorization
            identifier out from under them.

        :param content: The HTML document containing the patron dump.
        """
        # If we don't see these fields, erase any previous value
        # rather than leaving the old value in place. This shouldn't
        # happen (unless the expiration date changes to an invalid
        # date), but just to be safe.
        permanent_id = PatronData.NO_VALUE
        username = authorization_expires = personal_name = PatronData.NO_VALUE
        email_address = fines = external_type = PatronData.NO_VALUE
        block_reason = PatronData.NO_VALUE
        neighborhood = PatronData.NO_VALUE

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
                # The millenium API doesn't care about spaces, so we add
                # a version of the barcode without spaces to our identifers
                # list as well.
                if " " in v:
                    potential_identifiers.append(v.replace(" ", ""))
            elif k == self.RECORD_NUMBER_FIELD:
                permanent_id = v
            elif k == self.USERNAME_FIELD:
                username = v
            elif k == self.PERSONAL_NAME_FIELD:
                personal_name = v
            elif k == self.EMAIL_ADDRESS_FIELD:
                email_address = v
            elif k == self.FINES_FIELD:
                try:
                    fines = MoneyUtility.parse(v)
                except ValueError:
                    self.log.warn(
                        'Malformed fine amount for patron: "%s". Treating as no fines.'
                    )
                    fines = Money("0", "USD")
            elif k == self.BLOCK_FIELD:
                block_reason = self._patron_block_reason(self.block_types, v)
            elif k == self.EXPIRATION_FIELD:
                try:
                    expires = datetime.datetime.strptime(
                        v, self.EXPIRATION_DATE_FORMAT).date()
                    authorization_expires = expires
                except ValueError:
                    self.log.warn(
                        'Malformed expiration date for patron: "%s". Treating as unexpirable.',
                        v
                    )
            elif k == self.PATRON_TYPE_FIELD:
                external_type = v
            elif (k == self.HOME_BRANCH_FIELD
                  and self.neighborhood_mode == self.HOME_BRANCH_NEIGHBORHOOD_MODE):
                neighborhood = v.strip()
            elif (k == self.ADDRESS_FIELD
                  and self.neighborhood_mode == self.POSTAL_CODE_NEIGHBORHOOD_MODE):
                neighborhood = self.extract_postal_code(v)
            elif k == self.ERROR_MESSAGE_FIELD:
                # An error has occured. Most likely the patron lookup
                # failed.
                return None

        # Set the library identifier field
        library_identifier = None
        for k, v in self._extract_text_nodes(content):
            if k == self.library_identifier_field:
                library_identifier = v.strip()

        # We may now have multiple authorization
        # identifiers. PatronData expects the best authorization
        # identifier to show up first in the list.
        #
        # The last identifier in the list is probably the most recently
        # added one. In the absence of any other information, it's the
        # one we should choose.
        potential_identifiers.reverse()

        authorization_identifiers = potential_identifiers
        if not authorization_identifiers:
            authorization_identifiers = PatronData.NO_VALUE
        elif current_identifier in authorization_identifiers:
            # Don't rock the boat. The patron is used to using this
            # identifier and there's no need to change it. Move the
            # currently used identifier to the front of the list.
            authorization_identifiers.remove(current_identifier)
            authorization_identifiers.insert(0, current_identifier)

        data = PatronData(
            permanent_id=permanent_id,
            authorization_identifier=authorization_identifiers,
            username=username,
            personal_name=personal_name,
            email_address=email_address,
            authorization_expires=authorization_expires,
            external_type=external_type,
            fines=fines,
            block_reason=block_reason,
            library_identifier=library_identifier,
            neighborhood=neighborhood,
            # We must cache neighborhood information in the patron's
            # database record because syncing with the ILS is so
            # expensive.
            cached_neighborhood=neighborhood,
            complete=True
        )
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

    # A number of regular expressions for finding postal codes in
    # freeform addresses, with more reliable techniques at the front.
    POSTAL_CODE_RES = [
        re.compile(x) for x in [
            "[^0-9]([0-9]{5})-[0-9]{4}$", # ZIP+4 at end
            "[^0-9]([0-9]{5})$", # ZIP at end
            ".*[^0-9]([0-9]{5})-[0-9]{4}[^0-9]", # ZIP+4 as close to end as possible without being at the end
            ".*[^0-9]([0-9]{5})[^0-9]", # ZIP as close to end as possible without being at the end
        ]
    ]

    @classmethod
    def extract_postal_code(cls, address):
        """Try to extract a postal code from an address."""
        for r in cls.POSTAL_CODE_RES:
            match = r.search(address)
            if match:
                return match.groups()[0]
        return None


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
