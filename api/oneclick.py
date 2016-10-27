import logging
from nose.tools import set_trace
#from lxml import etree
#from urlparse import urljoin
#from urllib import urlencode
import datetime
import requests

from authenticator import BasicAuthAuthenticator
#from config import Configuration
#import os
#import re

from circulation import (
    LoanInfo,
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

from core.monitor import (
    Monitor,
    IdentifierSweepMonitor,
)


class OneClickAPI(BaseOneClickAPI, BaseCirculationAPI):

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
    EXPIRATION_DEFAULT = datetime.timedelta(days=21)

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


    def checkout(self, patron, pin, licensepool, internal_format):
        """
        Associate an ebook or audio with a patron.

        :param patron: a Patron object for the patron who wants to check out the book.
        :param pin: The patron's password (not used).
        :param licensepool: The Identifier of the book to be checked out is 
        attached to this licensepool.
        :param internal_format: Represents the patron's desired book format.

        :return LoanInfo on success, None on failure
        """
        patron_cardno = patron.authorization_identifier
        if not patron_cardno:
            return None

        patron_oneclick_id = self.get_patron_internal_id(patron_cardno=patron_cardno)
        if not patron_oneclick_id:
            return None

        identifier = licensepool.identifier
        item_oneclick_id=identifier.identifier
        if not item_oneclick_id:
            return None

        resp_dict = self.circulate_item(patron_id=patron_oneclick_id, item_id=item_oneclick_id, return_item=False)

        if not resp_dict or ('error_code' in resp_dict):
            return None

        log.debug("Patron %s/%s checked out item %s with transaction id %s.", patron.authorization_identifier, 
            patron_oneclick_id, item_oneclick_id, resp_dict['transactionId'])

        today = datetime.datetime.now()
        expires = today + EXPIRATION_DEFAULT

        # Create the loan info. We don't know the expiration 
        loan = LoanInfo(
            identifier.type,
            item_oneclick_id,
            today,
            expires,
            None,
        )
        return loan


    def checkin(self, patron, pin, licensepool):
        """
        Allow a patron to return an ebook or audio before its due date.

        :param patron: a Patron object for the patron who wants to return the book.
        :param pin: The patron's password (not used).
        :param licensepool: The Identifier of the book to be checked out is 
        attached to this licensepool.
        """
        patron_cardno = patron.authorization_identifier
        if not patron_cardno:
            return None

        patron_oneclick_id = self.get_patron_internal_id(patron_cardno=patron_cardno)
        if not patron_oneclick_id:
            return None

        identifier = licensepool.identifier
        item_oneclick_id=identifier.identifier
        if not item_oneclick_id:
            return None

        resp_dict = self.circulate_item(patron_id=patron_oneclick_id, item_id=item_oneclick_id, return_item=True)

        if resp_dict == {}:
            resp_dict = {'output':'SUCCESS'}
        else:
            return None

        log.debug("Patron %s/%s returned item %s.", patron.authorization_identifier, 
            patron_oneclick_id, item_oneclick_id)

        return resp_dict


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


    def circulate_item(self, patron_id, item_id, return_item=False):
        """
        Borrow or return a catalog item.
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
            set_trace()
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


    def fulfill(self, patron, pin, licensepool, internal_format):
        """ Get the actual resource file to the patron.
        :return a FulfillmentInfo object.
        """
        raise NotImplementedException


    def patron_activity(self, patron, pin):
        """ Return a patron's current checkouts and holds.

        :param patron: a Patron object for the patron who wants to return the book.
        :param pin: The patron's password (not used).
        """
        patron_cardno = patron.authorization_identifier
        if not patron_cardno:
            return None

        patron_oneclick_id = self.get_patron_internal_id(patron_cardno=patron_cardno)
        if not patron_oneclick_id:
            return None

        url = "%s/libraries/%s/patrons/%s/checkouts/" % (self.base_url, str(self.library_id), patron_id)

        try:
            response = self.request(url=url)
        except Exception, e:
            self.log.error("Patron info failed: %r", e, exc_info=e)
            raise ValueError(e.message)

        resp_dict = {}
        message = None
        if response.text:
            resp_dict = response.json()
            message = resp_dict.get('message', None)

        error_response = self.check_and_log_errors(response=response, message=message, message_root="Checkout ")
        if error_response:
            # TODO: or throw exception?
            return None

        # TODO: go through patron's checkouts and holds and 
        # generate LoanInfo and HoldInfo objects.


    def place_hold(self, patron, pin, licensepool, notification_email_address):
        """Place a book on hold.

        :return: A HoldInfo object
        """
        raise NotImplementedException


    def release_hold(self, patron, pin, licensepool):
        """Release a patron's hold on a book.

        :raises CannotReleaseHold: If there is an error communicating
        with Overdrive, or Overdrive refuses to release the hold for
        any reason.
        """
        raise NotImplementedException



class MockOneClickAPI(BaseMockOneClickAPI, OneClickAPI):
    pass



class OneClickCirculationMonitor(Monitor):
    """Maintain LicensePools for OneClick titles.

    Bibliographic data isn't inserted into new LicensePools until
    we hear from the metadata wrangler.
    """
    def __init__(self, _db, name="OneClick Circulation Monitor",
                 interval_seconds=500,
                 maximum_consecutive_unchanged_books=None):
        super(OneClickCirculationMonitor, self).__init__(
            _db, name, interval_seconds=interval_seconds)
        self.maximum_consecutive_unchanged_books = (
            maximum_consecutive_unchanged_books)


    def recently_changed_ids(self, start, cutoff):
        return self.api.get_delta(start, cutoff)


    def run(self):
        self.api = OneClickAPI(self._db)
        super(OneClickCirculationMonitor, self).run()

    # ------
    def run_once(self, start, cutoff):
        _db = self._db
        added_books = 0
        overdrive_data_source = DataSource.lookup(
            _db, DataSource.OVERDRIVE)

        total_books = 0
        consecutive_unchanged_books = 0
        for i, book in enumerate(self.recently_changed_ids(start, cutoff)):
            total_books += 1
            if not total_books % 100:
                self.log.info("%s books processed", total_books)
            if not book:
                continue
            license_pool, is_new, is_changed = self.api.update_licensepool(book)
            # Log a circulation event for this work.
            if is_new:
                Analytics.collect_event(
                    _db, license_pool, CirculationEvent.TITLE_ADD, license_pool.last_checked)

            _db.commit()

            if is_changed:
                consecutive_unchanged_books = 0
            else:
                consecutive_unchanged_books += 1
                if (self.maximum_consecutive_unchanged_books
                    and consecutive_unchanged_books >= 
                    self.maximum_consecutive_unchanged_books):
                    # We're supposed to stop this run after finding a
                    # run of books that have not changed, and we have
                    # in fact seen that many consecutive unchanged
                    # books.
                    self.log.info("Stopping at %d unchanged books.",
                                  consecutive_unchanged_books)
                    break

        if total_books:
            self.log.info("Processed %d books total.", total_books)



class OneClickCollectionMonitor(OneClickCirculationMonitor):
    """Monitor recently changed books in the OneClick collection."""

    def __init__(self, _db, interval_seconds=60,
                 maximum_consecutive_unchanged_books=100):
        super(OneClickCollectionMonitor, self).__init__(
            _db, "Reverse Chronological Overdrive Collection Monitor",
            interval_seconds, maximum_consecutive_unchanged_books)



