import logging
from nose.tools import set_trace
#from lxml import etree
#from urlparse import urljoin
#from urllib import urlencode
#import datetime
import requests

#from core.util.xmlparser import XMLParser
from authenticator import BasicAuthAuthenticator
#from config import Configuration
#import os
#import re

from circulation import (
    #LoanInfo,
    #FulfillmentInfo,
    #HoldInfo,
    BaseCirculationAPI
)
from circulation_exceptions import *

from core.oneclick import (
    OneClickAPI as BaseOneClickAPI,
    MockOneClickAPI as BaseMockOneClickAPI,
    #Axis360Parser,
    #BibliographicParser,
    OneClickBibliographicCoverageProvider
)

from core.model import (
#    get_one,
#    get_one_or_create,
    Patron,
)



class OneClickAPI(BaseOneClickAPI, BasicAuthAuthenticator, BaseCirculationAPI):

    NAME = "OneClick"
    
    '''
    RECORD_NUMBER_FIELD = 'RECORD #[p81]'
    PATRON_TYPE_FIELD = 'P TYPE[p47]'
    EXPIRATION_FIELD = 'EXP DATE[p43]'
    BARCODE_FIELD = 'P BARCODE[pb]'
    USERNAME_FIELD = 'ALT ID[pu]'
    FINES_FIELD = 'MONEY OWED[p96]'
    '''
    EXPIRATION_DATE_FORMAT = '%m-%d-%y'

    #MULTIVALUE_FIELDS = set(['NOTE[px]', BARCODE_FIELD])

    #REPORTED_LOST = re.compile("^CARD([0-9]{14})REPORTEDLOST")

    # How long we should go before syncing our internal Patron record
    # with Millenium.
    #MAX_STALE_TIME = datetime.timedelta(hours=12)

    log = logging.getLogger("OneClick Patron API")


    def __init__(self, *args, **kwargs):
        super(OneClickAPI, self).__init__(*args, **kwargs)
        self.bibliographic_coverage_provider = (
            OneClickBibliographicCoverageProvider(
                self._db, oneclick_api=self
            )
        )


    def checkout_item(self, patron_id, item_id, return_item=False):
        """
        :param patron_id OneClick internal id
        :param item_id isbn
        :return information on the transaction or error status and message
        """
        url = "%s/libraries/%s/patrons/%s/checkouts/%s" % (self.base_url, str(self.library_id), patron_id, item_id)

        method = "post"
        if return_item:
            method = "delete"

        try:
            response = self.request(url=url, method=method)
        except Exception, e:
            self.log.error("Item checkout/return failed: %r", e, exc_info=e)
            raise ValueError(e.message)

        resp_dict = {}
        message = None
        if response.text:
            resp_dict = response.json()
            message = resp_dict.get('message', None)

        error_response = self.check_and_log_errors(response=response, message=message, message_root="Checkout ")
        if error_response:
            return error_response

        return resp_dict


    def return_item(self, patron_id, item_id):
        """
        :param patron_id OneClick internal id
        :param item_id isbn
        """
        resp_dict = self.checkout_item(patron_id, item_id, return_item=True)
        if resp_dict == {}:
            resp_dict = {'output':'SUCCESS'}
        return resp_dict


    def create_patron(self, patron_email=None, patron_cardno=None):
        #http://api.oneclickdigital.us/v1/libraries/1931/patrons
        '''
        {
        "libraryId":"1931",
        "userName":"mickeymouse",
        "password":"disney",
        "email":"mickey@mouse.com",
        "firstName":"Mic",
        "lastName":"Mouse",
        "libraryCardNumber":"13057226"
        }
        '''

    def check_and_log_errors(self, response, message, message_root=""):
        """ OneClick tries to communicate statuses and errors through http codes.
        Malformed url requests will throw a 500, non-existent ids will get a 404, 
        trying an action like checkout on a patron/item combo that's blocked 
        (like if the item is already checked out, for example) will get a 409, etc..
        Further details are usually elaborated on in the "message" field of the response.

        :return True if non-fatal errors found, false otherwise (throws exceptions on bad errors).
        """
        if response.status_code != 200:
            if not message:
                message = response.text
            self.log.warning("%s call failed: %s ", message_root, message)
            return {"error_code":response.status_code, "message":message}
        elif message:
            # http code was OK, but info wasn't sucessfully read from db
            # not exception-level fatal.  
            self.log.warning("%s not retrieved: %s ", message_root, message)
            return {"error_code":0, "message":message}

        return None


    def get_patron_internal_id(self, patron_email=None, patron_cardno=None):
        """ Uses either an email address or a library card to identify a patron by.

        :param patron_email 
        :param patron_cardno
        """
        if patron_cardno: 
            patron_identifier = patron_cardno
        elif patron_email:
            patron_identifier = patron_email
        else:
            # consider raising an exception, since we should call methods with nice arguments
            return None

        url = "%s/rpc/libraries/%s/patrons/%s" % (self.base_url, str(self.library_id), patron_identifier)

        try:
            response = self.request(url)
        except Exception, e:
            self.log.error("Patron id call failed: %r", e, exc_info=e)
            raise ValueError(e.message)

        resp_dict = response.json()
        message = resp_dict.get('message', None)
        error_response = self.check_and_log_errors(response, message, message_root="Patron info")
        if error_response:
            # we've logged the errors.  the cleanest thing to do now is return none
            return None

        internal_patron_id = resp_dict['patronId']
        return internal_patron_id


    def get_patron_information(self, patron_id):
        """
        Retrieves patron's name, email, library card number from OneClick.

        :param patron_id OneClick's internal id for the patron.
        """
        url = "%s/libraries/%s/patrons/%s" % (self.base_url, str(self.library_id), patron_id)

        try:
            response = self.request(url)
        except Exception, e:
            self.log.error("Patron info call failed: %r", e, exc_info=e)
            raise ValueError(e.message)

        resp_dict = response.json()
        message = resp_dict.get('message', None)
        error_response = self.check_and_log_errors(response, message, message_root="Patron info")
        if error_response:
            return error_response

        return resp_dict




class MockOneClickAPI(BaseMockOneClickAPI, OneClickAPI):
    pass



