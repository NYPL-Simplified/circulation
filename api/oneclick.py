import logging
from nose.tools import set_trace

import datetime
import json;
import os
import requests
import uuid
from flask.ext.babel import lazy_gettext as _

from circulation import (
    BaseCirculationAPI, 
    FulfillmentInfo,
    HoldInfo,
    LoanInfo,
)
from circulation_exceptions import *

from config import Configuration

from core.analytics import Analytics

from core.oneclick import (
    OneClickAPI as BaseOneClickAPI,
    MockOneClickAPI as BaseMockOneClickAPI,
    OneClickBibliographicCoverageProvider
)

from core.metadata_layer import (
    CirculationData, 
    ReplacementPolicy,
)

from core.model import (
    CirculationEvent,
    Collection,
    DataSource,
    Edition,
    ExternalIntegration,
    Identifier, 
    LicensePool,
    Patron,
    Representation,
    Session,
)

from core.monitor import (
    CollectionMonitor,
)

from core.util.http import (
    BadResponseException,
)


class OneClickAPI(BaseOneClickAPI, BaseCirculationAPI):

    NAME = ExternalIntegration.ONE_CLICK
    SETTINGS = [
        { "key": ExternalIntegration.PASSWORD, "label": _("Basic Token") },
        { "key": Collection.EXTERNAL_ACCOUNT_ID_KEY, "label": _("Library ID") },
        { "key": ExternalIntegration.URL, "label": _("URL") },
    ] + BaseCirculationAPI.SETTINGS
    
    EXPIRATION_DATE_FORMAT = '%Y-%m-%d'

    log = logging.getLogger("OneClick Patron API")


    def __init__(self, *args, **kwargs):
        super(OneClickAPI, self).__init__(*args, **kwargs)
        self.bibliographic_coverage_provider = (
            OneClickBibliographicCoverageProvider(
                self.collection, api_class=self
            )
        )

        # TODO: We need a general system for tracking default loan
        # durations for different media types. However it doesn't
        # matter much because the license sources generally tell us
        # when specific loans expire.
        self.ebook_expiration_default = datetime.timedelta(
            self.collection.default_reservation_period
        )
        self.eaudio_expiration_default = self.ebook_expiration_default


    def checkin(self, patron, pin, licensepool):
        """
        Allow a patron to return an ebook or audio before its due date.

        :param patron: a Patron object for the patron who wants to return the book.
        :param pin: The patron's password (not used).
        :param licensepool: The Identifier of the book to be checked out is 
        attached to this licensepool.

        :return True on success, raises circulation exceptions on failure.
        """
        patron_oneclick_id = self.validate_patron(patron)
        (item_oneclick_id, item_media) = self.validate_item(licensepool)

        resp_dict = self.circulate_item(patron_id=patron_oneclick_id, item_id=item_oneclick_id, return_item=True)

        if resp_dict == {}:
            self.log.debug("Patron %s/%s returned item %s.", patron.authorization_identifier, 
                patron_oneclick_id, item_oneclick_id)
            return True

        # should never happen
        raise CirculationException("Unknown error %s/%s checking in %s.", patron.authorization_identifier, 
            patron_oneclick_id, item_oneclick_id)


    def checkout(self, patron, pin, licensepool, internal_format):
        """
        Associate an ebook or audio with a patron.

        :param patron: a Patron object for the patron who wants to check out the book.
        :param pin: The patron's password (not used).
        :param licensepool: The Identifier of the book to be checked out is 
        attached to this licensepool.
        :param internal_format: Represents the patron's desired book format.  Ignored for now.

        :return LoanInfo on success, None on failure.
        """
        patron_oneclick_id = self.validate_patron(patron)
        (item_oneclick_id, item_media) = self.validate_item(licensepool)

        resp_dict = self.circulate_item(patron_id=patron_oneclick_id, item_id=item_oneclick_id, return_item=False)

        if not resp_dict or ('error_code' in resp_dict):
            return None

        self.log.debug("Patron %s/%s checked out item %s with transaction id %s.", patron.authorization_identifier, 
            patron_oneclick_id, item_oneclick_id, resp_dict['transactionId'])

        today = datetime.datetime.now()
        if item_media == Edition.AUDIO_MEDIUM:
            expires = today + self.eaudio_expiration_default
        else:
            expires = today + self.ebook_expiration_default

        # Create the loan info. We don't know the expiration for sure, 
        # but we know the library default.  We do have the option of 
        # getting expiration by checking patron's activity, but that 
        # would mean another http call and is not currently merited.
        loan = LoanInfo(
            self.collection,
            DataSource.ONECLICK,
            identifier_type=licensepool.identifier.type,
            identifier=item_oneclick_id,
            start_date=today,
            end_date=expires,
            fulfillment_info=None,
        )
        return loan


    def circulate_item(self, patron_id, item_id, hold=False, return_item=False):
        """
        Borrow or return a catalog item.
        :param patron_id OneClick internal id
        :param item_id isbn
        :return A dictionary of information on the transaction or error status and message
            Calling methods are expected to use this dictionary to create XxxInfo objects.
        """
        endpoint = "checkouts"
        if hold:
            endpoint = "holds"
        url = "%s/libraries/%s/patrons/%s/%s/%s" % (self.base_url, str(self.library_id), patron_id, endpoint, item_id)

        method = "post"
        action = "checkout"
        if not hold and return_item:
            method = "delete"
            action = "checkin"
        elif hold and not return_item:
            action = "place_hold"
        elif hold and return_item:
            method = "delete"
            action = "release_hold"

        resp_obj = {}
        message = None
        try:
            response = self.request(url=url, method=method)

            if response.text:
                resp_obj = response.json()

                # checkout responses are dictionaries, hold responses are strings
                if isinstance(resp_obj, dict):
                    message = resp_obj.get('message', None)

        except Exception, e:
            self.log.error("Item circulation request failed: %r", e, exc_info=e)
            raise RemoteInitiatedServerError(e.message, action)

        self.validate_response(response=response, message=message, action=action)

        return resp_obj


    def fulfill(self, patron, pin, licensepool, internal_format):
        """ Get the actual resource file to the patron.
        :return a FulfillmentInfo object.
        """

        patron_oneclick_id = self.validate_patron(patron)
        (item_oneclick_id, item_media) = self.validate_item(licensepool)

        # find patron's checkouts
        checkouts_list = self.get_patron_checkouts(patron_id=patron_oneclick_id)
        if not checkouts_list:
            raise NoActiveLoan("Cannot fulfill %s - patron %s/%s has no checkouts.", item_oneclick_id, 
                patron.authorization_identifier, patron_oneclick_id)

        # find this licensepool in patron's checkouts
        found_checkout = None
        for checkout in checkouts_list:
            if checkout.identifier == item_oneclick_id:
                found_checkout = checkout
                break
        if not found_checkout:
            raise NoActiveLoan("Cannot fulfill %s - patron %s/%s has no such checkout.", item_oneclick_id, 
                patron.authorization_identifier, patron_oneclick_id)
        
        return found_checkout.fulfillment_info



    def place_hold(self, patron, pin, licensepool, notification_email_address):
        """Place a book on hold.

        Note: If the requested book is available for checkout, OneClick will respond 
        with a "success" to the hold request.  Then, at the next database clean-up sweep, 
        OneClick will automatically convert the hold record to a checkout record. 

        :param patron: a Patron object for the patron who wants to check out the book.
        :param pin: The patron's password (not used).
        :param licensepool: The Identifier of the book to be checked out is 
        attached to this licensepool.
        :param internal_format: Represents the patron's desired book format.  Ignored for now.

        :return: A HoldInfo object on success, None on failure
        """
        patron_oneclick_id = self.validate_patron(patron)
        (item_oneclick_id, item_media) = self.validate_item(licensepool)

        resp_obj = self.circulate_item(patron_id=patron_oneclick_id, item_id=item_oneclick_id, hold=True, return_item=False)

        # successful holds return a numeric transaction id
        try:
            transaction_id = int(resp_obj)
        except Exception, e:
            self.log.error("Item hold request failed: %r", e, exc_info=e)
            raise CannotHold(e.message)

        self.log.debug("Patron %s/%s reserved item %s with transaction id %s.", patron.authorization_identifier, 
            patron_oneclick_id, item_oneclick_id, resp_obj)

        today = datetime.datetime.now()

        hold = HoldInfo(
            self.collection,
            DataSource.ONECLICK,
            identifier_type=licensepool.identifier.type,
            identifier=item_oneclick_id,
            start_date=today,
            # OneClick sets hold expirations to 2050-12-31, as a "forever"
            end_date=None,
            hold_position=None,
        )

        return hold


    def release_hold(self, patron, pin, licensepool):
        """Release a patron's hold on a book.

        :param patron: a Patron object for the patron who wants to return the book.
        :param pin: The patron's password (not used).
        :param licensepool: The Identifier of the book to be checked out is 
        attached to this licensepool.

        :return True on success, raises circulation exceptions on failure.
        """
        patron_oneclick_id = self.validate_patron(patron)
        (item_oneclick_id, item_media) = self.validate_item(licensepool)

        resp_dict = self.circulate_item(patron_id=patron_oneclick_id, item_id=item_oneclick_id, hold=True, return_item=True)

        if resp_dict == {}:
            self.log.debug("Patron %s/%s released hold %s.", patron.authorization_identifier, 
                patron_oneclick_id, item_oneclick_id)
            return True

        # should never happen
        raise CirculationException("Unknown error %s/%s releasing %s.", patron.authorization_identifier, 
            patron_oneclick_id, item_oneclick_id)


    def update_licensepool_for_identifier(self, isbn, availability):
        """Update availability information for a single book.

        If the book has never been seen before, a new LicensePool
        will be created for the book.

        The book's LicensePool will be updated with current approximate 
        circulation information (we can tell if it's available, but 
        not how many copies). 
        Bibliographic coverage will be ensured for the OneClick Identifier. 
        Work will be created for the LicensePool and set as presentation-ready.

        :param isbn the identifier OneClick uses
        :param availability boolean denoting if book can be lent to patrons 
        """

        # find a license pool to match the isbn, and see if it'll need a metadata update later
        license_pool, is_new_pool = LicensePool.for_foreign_id(
            self._db, DataSource.ONECLICK, Identifier.ONECLICK_ID, isbn,
            collection=self.collection
        )
        if is_new_pool:
            # This is the first time we've seen this book. Make sure its
            # identifier has bibliographic coverage.
            self.bibliographic_coverage_provider.ensure_coverage(
                license_pool.identifier
            )

        # now tell the licensepool if it's lendable
        policy = ReplacementPolicy(
            identifiers=False,
            subjects=True,
            contributions=True,
            formats=True,
            analytics=Analytics(self._db),
        )

        # licenses_available can be 0 or 999, depending on whether the book is 
        # lendable or not.   
        licenses_available = 999
        if not availability:
            licenses_available = 0

        circulation_data = CirculationData(data_source=DataSource.ONECLICK, 
            primary_identifier=license_pool.identifier, 
            licenses_available=licenses_available)

        license_pool, circulation_changed = circulation_data.apply(
            self._db,
            self.collection,
            replace=policy,
        )

        return license_pool, is_new_pool, circulation_changed
        

    def update_availability(self, licensepool):
        """Update the availability information for a single LicensePool.
        Part of the CirculationAPI interface.
        Inactive for now, because we'd have to request and go through all availabilities 
        from OneClick just to pick the one licensepool we want.
        """
        pass


    ''' -------------------------- Patron Account Handling -------------------------- '''
    def create_patron(self, patron):
        """ Ask OneClick to create a new patron record.

        :param patron: a Patron object which contains the permanent id to give to 
        OneClick as the patron's library card number.
        :return OneClick's internal patron id.
        """

        # TODO: will not work in all libraries, find a better solution
        patron_cardno = patron.authorization_identifier
        if not patron_cardno:
            raise InvalidInputException("Patron %r has no card number.", patron)

        url = "%s/libraries/%s/patrons/" % (self.base_url, str(self.library_id))
        action="create_patron"
        
        post_args = dict()
        post_args['libraryId'] = self.library_id
        post_args['libraryCardNumber'] = str(patron.authorization_identifier)
        # generate random values for the account fields the patron has not supplied us with
        patron_uuid = str(uuid.uuid1())
        post_args['userName'] = 'username_' + patron_uuid
        post_args['email'] = 'patron_' + patron_uuid + '@librarysimplified.org'
        post_args['firstName'] = 'Patron'
        post_args['lastName'] = 'Reader'
        # will not be used in our system, so just needs to be set to a securely randomized value
        post_args['password'] = os.urandom(8).encode('hex')


        resp_dict = {}
        message = None
        try:
            response = self.request(url=url, data=json.dumps(post_args), method="post")
            if response.text:
                resp_dict = response.json()
                message = resp_dict.get('message', None)
        except Exception, e:
            self.log.error("Patron create failed: %r", e, exc_info=e)
            raise RemoteInitiatedServerError(e.message, action)

        # general validation
        self.validate_response(response=response, message=message, action=action)

        # double-make sure specifically
        if response.status_code != 201 or 'patronId' not in resp_dict:
            raise RemotePatronCreationFailedException(action + 
                ": http=" + str(response.status_code) + ", response=" + response.text)

        patron_oneclick_id = resp_dict['patronId']

        return patron_oneclick_id


    def get_patron_internal_id(self, patron_email=None, patron_cardno=None):
        """ Uses either an email address or a library card to identify a patron by.
        :param patron_email 
        :param patron_cardno
        :return OneClick's internal id for the patron
        """
        if patron_cardno: 
            patron_identifier = patron_cardno
        elif patron_email:
            patron_identifier = patron_email
        else:
            # consider raising an exception, since we should call methods with nice arguments
            raise InvalidInputException("Need patron email or card number.")

        action="patron_id"
        url = "%s/rpc/libraries/%s/patrons/%s" % (self.base_url, str(self.library_id), patron_identifier)

        try:
            response = self.request(url)
        except Exception, e:
            self.log.error("Patron id call failed: %r", e, exc_info=e)
            raise RemoteInitiatedServerError(e.message, action)

        resp_dict = response.json()
        message = resp_dict.get('message', None)
        try:
            self.validate_response(response, message, action=action)
        except PatronNotFoundOnRemote, e:
            # this should not be fatal at this point
            return None

        internal_patron_id = resp_dict.get('patronId', None)
        return internal_patron_id


    def get_patron_checkouts(self, patron_id):
        """
        Gets the books and audio the patron currently has checked out.
        Obtains fulfillment info for each item -- the way to fulfill a book 
        is to get this list of possibilities first, and then call individual 
        fulfillment endpoints on the individual items.

        :param patron_id OneClick internal id for the patron.
        """
        url = "%s/libraries/%s/patrons/%s/checkouts/" % (self.base_url, str(self.library_id), patron_id)
        action="patron_checkouts"
        loans = []

        resp_obj = []
        message = None
        try:
            response = self.request(url=url)

            if response.text:
                resp_obj = response.json()
                # if we succeeded, then we got back a list of checkouts
                # if we failed, then we got back a dictionary with an error message
                if isinstance(resp_obj, dict):
                    message = resp_obj.get('message', None)
        except Exception, e:
            self.log.error("Patron checkouts failed: %r", e, exc_info=e)
            raise RemoteInitiatedServerError(e.message, action)

        self.validate_response(response=response, message=message, action=action)

        # by now we can assume response is either empty or a list
        for item in resp_obj:
            # go through patron's checkouts and generate LoanInfo objects, 
            # with FulfillmentInfo objects included
            media_type = item.get('mediaType', 'eBook')
            isbn = item.get('isbn', None)
            can_renew = item.get('canRenew', None)
            title = item.get('title', None)
            authors = item.get('authors', None)
            # refers to checkout expiration date, not the downloadUrl's
            expires = item.get('expiration', None)
            if expires:
                expires = datetime.datetime.strptime(expires, self.EXPIRATION_DATE_FORMAT).date()

            identifier, made_new = Identifier.for_foreign_id(self._db, 
                    foreign_identifier_type=Identifier.ONECLICK_ID, 
                    foreign_id=isbn, autocreate=False)

            # Note: if OneClick knows about a patron's checked-out item that wasn't
            # checked out through us, we ignore it
            if not identifier:
                continue

            files = item.get('files', None)
            for file in files:
                filename = file.get('filename', None)
                # assume fileFormat is same for all files associated with this checkout
                # and use the last one mentioned.  Ex: "fileFormat": "EPUB".
                # note: audio books don't list fileFormat field, just the filename, and the mediaType.
                file_format = file.get('fileFormat', None)
                if file_format == 'EPUB':
                    file_format = Representation.EPUB_MEDIA_TYPE
                else:
                    # slightly risky assumption here
                    file_format = Representation.MP3_MEDIA_TYPE

                # Note: download urls expire 15 minutes after being handed out
                # in the checkouts call
                download_url = file.get('downloadUrl', None)
                # is included in the downloadUrl, actually
                acs_resource_id = file.get('acsResourceId', None)

            # TODO: For audio books, the downloads are done by parts, and there are 
            # multiple download urls.  Need to have a mechanism for putting lists of 
            # parts into fulfillment objects.
            fulfillment_info = FulfillmentInfo(
                self.collection,
                DataSource.ONECLICK,
                Identifier.ONECLICK_ID, 
                identifier, 
                content_link = download_url, 
                content_type = file_format, 
                content = None, 
                content_expires = None
            )

            loan = LoanInfo(
                self.collection,
                DataSource.ONECLICK,
                Identifier.ONECLICK_ID,
                isbn,
                start_date=None,
                end_date=expires,
                fulfillment_info=fulfillment_info,
            )

            loans.append(loan)

        return loans


    def get_patron_holds(self, patron_id):
        """
        :param patron_id OneClick internal id for the patron.
        """
        url = "%s/libraries/%s/patrons/%s/holds/" % (self.base_url, str(self.library_id), patron_id)
        action="patron_holds"
        holds = []

        resp_obj = []
        message = None
        try:
            response = self.request(url=url)

            if response.text:
                resp_obj = response.json()
                # if we succeeded, then we got back a list of holds
                # if we failed, then we got back a dictionary with an error message
                if isinstance(resp_obj, dict):
                    message = resp_obj.get('message', None)
        except Exception, e:
            self.log.error("Patron holds failed: %r", e, exc_info=e)
            raise RemoteInitiatedServerError(e.message, action)

        self.validate_response(response=response, message=message, action=action)

        # by now we can assume response is either empty or a list
        for item in resp_obj:
            # go through patron's holds and HoldInfo objects.
            media_type = item.get('mediaType', 'eBook')
            isbn = item.get('isbn', None)
            title = item.get('title', None)
            authors = item.get('authors', None)
            expires = item.get('expiration', None)
            if expires:
                expires = datetime.datetime.strptime(expires, self.EXPIRATION_DATE_FORMAT).date()

            identifier = Identifier.from_asin(self._db, isbn, autocreate=False)
            # Note: if OneClick knows about a patron's checked-out item that wasn't
            # checked out through us, we ignore it
            if not identifier:
                continue

            hold = HoldInfo(
                self.collection,
                DataSource.ONECLICK,
                Identifier.ONECLICK_ID,
                isbn,
                start_date=None,
                end_date=expires,
                hold_position=None
            )

            holds.append(hold)

        return holds


    def get_patron_information(self, patron_id):
        """
        Retrieves patron's name, email, library card number from OneClick.

        :param patron_id OneClick's internal id for the patron.
        """
        if not patron_id:
            raise InvalidInputException("Need patron OneClick id.")

        url = "%s/libraries/%s/patrons/%s" % (self.base_url, str(self.library_id), patron_id)
        action="patron_info"

        try:
            response = self.request(url)
        except Exception, e:
            self.log.error("Patron info call failed: %r", e, exc_info=e)
            raise RemoteInitiatedServerError(e.message, action)

        resp_dict = response.json()
        message = resp_dict.get('message', None)
        self.validate_response(response, message, action=action)

        # If needed, will put info into PatronData subclass.  For now, OK to return a dictionary.
        return resp_dict


    def patron_activity(self, patron, pin):
        """ Get a patron's current checkouts and holds.

        :param patron: a Patron object for the patron who wants to return the book.
        :param pin: The patron's password (not used).
        """
        patron_oneclick_id = self.validate_patron(patron)

        patron_checkouts = self.get_patron_checkouts(patron_oneclick_id)
        patron_holds = self.get_patron_holds(patron_oneclick_id)

        return (patron_checkouts, patron_holds)


    ''' -------------------------- Validation Handling -------------------------- '''
    def validate_item(self, licensepool):
        """ Are we performing operations on a book that exists and can be 
        uniquely identified? 
        """
        item_oneclick_id = None
        media = None

        identifier = licensepool.identifier
        item_oneclick_id=identifier.identifier
        if not item_oneclick_id:
            raise InvalidInputException("Licensepool %r doesn't know its ISBN.", licensepool)

        if licensepool.work and licensepool.work.presentation_edition:
            media = licensepool.work.presentation_edition.media

        return item_oneclick_id, media


    def validate_patron(self, patron, create=False):
        """ Does the patron have what we need to identify them to OneClick?
        Does OneClick have this patron's record?
        Do we need to tell OneClick to create the patron record? 

        :param patron Our db patron-representing object
        :create if OneClick doesn't know this person, should we create them?
        :return OneClick's unique patron id for our patron
        """
        patron_cardno = patron.authorization_identifier
        if not patron_cardno:
            raise InvalidInputException("Patron %r has no card number.", patron)

        patron_oneclick_id = self.get_patron_internal_id(patron_cardno=patron_cardno)
        if not patron_oneclick_id:
            if not create:
                # OneClick doesn't recognize this patron's permanent identifier, and we 
                # were told not to ask OneClick to create a new record
                raise PatronAuthorizationFailedException("OneClick doesn't recognize patron card number %s.", patron_cardno)

            patron_oneclick_id = self.create_patron(patron)

        return patron_oneclick_id


    def validate_response(self, response, message, action=""):
        """ OneClick tries to communicate statuses and errors through http codes.
        Malformed url requests will throw a 500, non-existent ids will get a 404, 
        trying an action like checkout on a patron/item combo that's blocked 
        (like if the item is already checked out, for example) will get a 409, etc..
        Further details are usually elaborated on in the "message" field of the response.

        :param response http response object
        :message OneClick puts error explanation into 'message' field in response dictionary
        """
        if response.status_code not in [200, 201]:
            if not message:
                message = response.text
            self.log.warning("%s call failed: %s ", action, message)

            if response.status_code == 500:
                # yes, it could be a server error, but it can also be a malformed value in the request
                # sometimes those cause nice sql stack traces, which end up in 500s.
                if message.startswith("eXtensible Framework encountered a SqlException"):
                    raise InvalidInputException(action + ": " + message)
                elif message == "A patron account with the specified username, email address, or card number already exists for this library.":
                    raise RemotePatronCreationFailedException(action + ": " + message)
                else:
                    raise RemoteInitiatedServerError(message, action)

            # a 409 conflict code can mean many things
            if response.status_code == 409 and action == 'checkout':
                if message == "Maximum checkout count reached.":
                    raise PatronLoanLimitReached(action + ": " + message)
                elif message == "Checkout item already exists":
                    # we tried to borrow something the patron already has
                    raise AlreadyCheckedOut(action + ": " + message)
                else:
                    raise CannotLoan(action + ": " + message)

            if response.status_code == 409 and action == 'checkin':
                if message == "Checkout does not exists or it is already terminated or expired.":
                    # we tried to return something the patron doesn't own
                    raise NotCheckedOut(action + ": " + message)
                else:
                    raise CannotReturn(action + ": " + message)

            if response.status_code == 404:
                raise NotFoundOnRemote(action + ": " + message)

            if response.status_code == 400:
                raise InvalidInputException(action + ": " + message)

        elif message:
            # http code was OK, but info wasn't sucessfully read from db
            if message.startswith("eXtensible Framework was unable to locate the resource for RB.API.OneClick.UserPatron.Get"):
                raise PatronNotFoundOnRemote(action + ": " + message)
            else:
                self.log.warning("%s not retrieved: %s ", action, message)
                raise CirculationException(action + ": " + message)


    def queue_response(self, status_code, headers={}, content=None):
        """ Allows smoother faster creation of unit tests by letting 
        us live-test as we write. """
        pass



class MockOneClickAPI(BaseMockOneClickAPI, OneClickAPI):
    pass



class OneClickCirculationMonitor(CollectionMonitor):
    """Maintain LicensePools for OneClick titles.

    Bibliographic data isn't inserted into new LicensePools until
    we hear from the metadata wrangler.
    """
    SERVICE_NAME = "OneClick CirculationMonitor"
    DEFAULT_START_TIME = datetime.datetime(1970, 1, 1)
    INTERVAL_SECONDS = 1200
    DEFAULT_BATCH_SIZE = 50
    
    def __init__(self, collection, batch_size=None, api_class=OneClickAPI,
                 api_class_kwargs={}):
        _db = Session.object_session(collection)
        super(OneClickCirculationMonitor, self).__init__(_db, collection)
        self.batch_size = batch_size or self.DEFAULT_BATCH_SIZE

        self.api = api_class(_db, self.collection, **api_class_kwargs)
        self.bibliographic_coverage_provider = (
            OneClickBibliographicCoverageProvider(
                collection=self.collection, api_class=self.api,
            )
        )
        self.analytics = Analytics(self._db)

    def process_availability(self, media_type='ebook'):
        # get list of all titles, with availability info
        availability_list = self.api.get_ebook_availability_info(media_type=media_type)
        item_count = 0
        for availability in availability_list:
            isbn = availability['isbn']
            # boolean True/False value, not number of licenses
            available = availability['availability']

            license_pool, is_new, is_changed = self.api.update_licensepool_for_identifier(isbn, available)
            # Log a circulation event for this work.
            if is_new:
                for library in self.collection.libraries:
                    self.analytics.collect_event(
                        library, license_pool, CirculationEvent.DISTRIBUTOR_TITLE_ADD, license_pool.last_checked)

            item_count += 1
            if item_count % self.batch_size == 0:
                self._db.commit()

        return item_count


    def run(self):
        super(OneClickCirculationMonitor, self).run()


    def run_once(self, start, cutoff):
        ebook_count = self.process_availability(media_type='ebook')
        eaudio_count = self.process_availability(media_type='eaudio')

        self.log.info("Processed %d ebooks and %d audiobooks.", ebook_count, eaudio_count)






