import logging
from nose.tools import set_trace

import datetime
import json;
import os
import requests
import uuid
from flask_babel import lazy_gettext as _

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
    OneClickBibliographicCoverageProvider,
)

from core.metadata_layer import (
    CirculationData, 
    FormatData,
    ReplacementPolicy,
)

from core.model import (
    CirculationEvent,
    Collection,
    ConfigurationSetting,
    Credential,
    DataSource,
    DeliveryMechanism,
    Edition,
    ExternalIntegration,
    Hyperlink,
    Identifier, 
    Library,
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

from core.util.web_publication_manifest import (
    AudiobookManifest as CoreAudiobookManifest
)


class OneClickAPI(BaseOneClickAPI, BaseCirculationAPI):

    NAME = ExternalIntegration.RB_DIGITAL

    # With this API we don't need to guess the default loan period -- we
    # know which loan period we will ask for in which situations.
    BASE_SETTINGS = [x for x in BaseCirculationAPI.SETTINGS
                     if x['key'] != BaseCirculationAPI.DEFAULT_LOAN_PERIOD]

    SETTINGS = [
        { "key": ExternalIntegration.PASSWORD, "label": _("Basic Token") },
        { "key": Collection.EXTERNAL_ACCOUNT_ID_KEY, "label": _("Library ID") },
        { "key": ExternalIntegration.URL, "label": _("URL"), "default": BaseOneClickAPI.PRODUCTION_BASE_URL },
    ] + BASE_SETTINGS
    
    # The loan duration must be specified when connecting a library to an
    # RBdigital account, but if it's not specified, try one week.
    DEFAULT_LOAN_DURATION = 7

    my_audiobook_setting = dict(
        BaseCirculationAPI.AUDIOBOOK_LOAN_DURATION_SETTING
    )
    my_audiobook_setting.update(default=DEFAULT_LOAN_DURATION)
    my_ebook_setting = dict(
        BaseCirculationAPI.EBOOK_LOAN_DURATION_SETTING
    )
    my_ebook_setting.update(default=DEFAULT_LOAN_DURATION)
    LIBRARY_SETTINGS = BaseCirculationAPI.LIBRARY_SETTINGS + [
        my_audiobook_setting, 
        my_ebook_setting
    ]

    EXPIRATION_DATE_FORMAT = '%Y-%m-%d'

    log = logging.getLogger("OneClick Patron API")
   
    def __init__(self, *args, **kwargs):
        super(OneClickAPI, self).__init__(*args, **kwargs)
        self.bibliographic_coverage_provider = (
            OneClickBibliographicCoverageProvider(
                self.collection, api_class=self
            )
        )

    def remote_email_address(self, patron):
        """The fake email address to send to RBdigital when
        signing up this patron.
        """
        default = self.default_notification_email_address(patron, None)
        if not default:
            raise RemotePatronCreationFailedException(
                _("Cannot create remote account for patron because library's default notification address is not set.")
            )
        patron_identifier = patron.identifier_to_remote_service(
            DataSource.RB_DIGITAL
        )
        return default.replace('@', '+rbdigital-%s@' % patron_identifier, 1)

    def checkin(self, patron, pin, licensepool):
        """
        Allow a patron to return an ebook or audio before its due date.

        :param patron: a Patron object for the patron who wants to return the book.
        :param pin: The patron's password (not used).
        :param licensepool: The Identifier of the book to be checked out is 
        attached to this licensepool.

        :return True on success, raises circulation exceptions on failure.
        """
        patron_oneclick_id = self.patron_remote_identifier(patron)
        (item_oneclick_id, item_media) = self.validate_item(licensepool)

        resp_dict = self.circulate_item(patron_id=patron_oneclick_id, item_id=item_oneclick_id, return_item=True)

        if resp_dict.get('message') == 'success':
            self.log.debug("Patron %s/%s returned item %s.", patron.authorization_identifier, 
                patron_oneclick_id, item_oneclick_id)
            return True

        # should never happen
        raise CirculationException("Unknown error %s/%s checking in %s.", patron.authorization_identifier, 
            patron_oneclick_id, item_oneclick_id)


    def checkout(self, patron, pin, licensepool, internal_format):
        """
        Associate an eBook or eAudio with a patron.

        :param patron: a Patron object for the patron who wants to check out the book.
        :param pin: The patron's password (not used).
        :param licensepool: The Identifier of the book to be checked out is 
        attached to this licensepool.
        :param internal_format: Represents the patron's desired book format.  Ignored for now.

        :return LoanInfo on success, None on failure.
        """
        patron_oneclick_id = self.patron_remote_identifier(patron)
        (item_oneclick_id, item_media) = self.validate_item(licensepool)

        today = datetime.datetime.utcnow()

        library = patron.library

        if item_media == Edition.AUDIO_MEDIUM:
            key = Collection.AUDIOBOOK_LOAN_DURATION_KEY
            _db = Session.object_session(patron)
            days = (
                ConfigurationSetting.for_library_and_externalintegration(
                    _db, key, library, self.collection.external_integration
                ).int_value or Collection.STANDARD_DEFAULT_LOAN_PERIOD
            )
        else:
            days = self.collection.default_loan_period(library)

        resp_dict = self.circulate_item(patron_id=patron_oneclick_id, item_id=item_oneclick_id, return_item=False, days=days)

        if not resp_dict or ('error_code' in resp_dict):
            return None

        self.log.debug("Patron %s/%s checked out item %s with transaction id %s.", patron.authorization_identifier, 
            patron_oneclick_id, item_oneclick_id, resp_dict['transactionId'])

        expires = today + datetime.timedelta(days=days)
        loan = LoanInfo(
            self.collection,
            DataSource.RB_DIGITAL,
            identifier_type=licensepool.identifier.type,
            identifier=item_oneclick_id,
            start_date=today,
            end_date=expires,
            fulfillment_info=None,
        )
        return loan


    def circulate_item(self, patron_id, item_id, hold=False, return_item=False, days=None):
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

        if not hold and not return_item and days:
            url += "?days=%s" % days

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

        patron_oneclick_id = self.patron_remote_identifier(patron)
        (item_oneclick_id, item_media) = self.validate_item(licensepool)

        checkouts_list = self.get_patron_checkouts(patron_id=patron_oneclick_id)

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
        patron_oneclick_id = self.patron_remote_identifier(patron)
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
            DataSource.RB_DIGITAL,
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
        patron_oneclick_id = self.patron_remote_identifier(patron)
        (item_oneclick_id, item_media) = self.validate_item(licensepool)

        resp_dict = self.circulate_item(patron_id=patron_oneclick_id, item_id=item_oneclick_id, hold=True, return_item=True)

        if resp_dict.get('message') == 'success':
            self.log.debug("Patron %s/%s released hold %s.", patron.authorization_identifier, 
                patron_oneclick_id, item_oneclick_id)
            return True

        # should never happen
        raise CirculationException("Unknown error %s/%s releasing %s.", patron.authorization_identifier, 
            patron_oneclick_id, item_oneclick_id)

    @property
    def default_circulation_replacement_policy(self):
        return ReplacementPolicy(
            identifiers=False,
            subjects=True,
            contributions=True,
            formats=True,
            analytics=Analytics(self._db),
        )

    def update_licensepool_for_identifier(
            self, isbn, availability, medium, policy=None
    ):
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
        :param medium: The name OneClick uses for the book's medium.
        """

        # find a license pool to match the isbn, and see if it'll need a metadata update later
        license_pool, is_new_pool = LicensePool.for_foreign_id(
            self._db, DataSource.RB_DIGITAL, Identifier.RB_DIGITAL_ID, isbn,
            collection=self.collection
        )
        if is_new_pool:
            # This is the first time we've seen this book. Make sure its
            # identifier has bibliographic coverage.
            self.bibliographic_coverage_provider.ensure_coverage(
                license_pool.identifier
            )

        # now tell the licensepool if it's lendable

        # We don't know exactly how many licenses are available, but
        # we know that it's either zero (book is not lendable) or greater
        # than zero (book is lendable)
        licenses_available = 1
        if not availability:
            licenses_available = 0

        # Because the book showed up in availability, we know we own
        # at least one license to it.
        licenses_owned = 1

        if (not is_new_pool and 
            license_pool.licenses_owned == licenses_owned and 
            license_pool.licenses_available == licenses_available):
            # Optimization: Nothing has changed, so don't even bother
            # calling CirculationData.apply()
            return license_pool, is_new_pool, False

        # If possible, create a FormatData object representing
        # how the book is available.
        formats = []

        # Note that these strings are different from the similar strings
        # found in "fileFormat" when looking at a patron's loans.
        # "ebook" (a medium) versus "EPUB" (a format). Unfortunately we
        # don't get the file format when checking the book's
        # availability before a patron has checked it out.
        delivery_type = None
        drm_scheme = None
        medium = medium.lower()
        if medium == 'ebook':
            delivery_type = Representation.EPUB_MEDIA_TYPE
            # OneClick doesn't tell us the DRM scheme at this
            # point, but some of their EPUBs do have Adobe DRM.
            # Also, their DRM usage may change in the future.
            drm_scheme = DeliveryMechanism.ADOBE_DRM
        elif medium == 'eaudio':
            # TODO: we can't deliver on this promise yet, but this is
            # how we will be delivering audiobook manifests.
            delivery_type = Representation.AUDIOBOOK_MANIFEST_MEDIA_TYPE

        if delivery_type:
            formats.append(FormatData(delivery_type, drm_scheme))
        
        circulation_data = CirculationData(
            data_source=DataSource.RB_DIGITAL, 
            primary_identifier=license_pool.identifier, 
            licenses_owned=licenses_owned,
            licenses_available=licenses_available,
            formats=formats,
        )

        policy = policy or self.default_circulation_replacement_policy        
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


    def internal_format(self, delivery_mechanism):
        """We don't need to do any mapping between delivery mechanisms and
        internal formats, because each title is only available in one
        format.
        """
        return delivery_mechanism
    
### Patron account handling

    def patron_remote_identifier(self, patron):
        """Locate the identifier for the given Patron's account on the
        RBdigital side, creating a new RBdigital account if necessary.

        The identifier is cached in a persistent Credential object.

        :return: The remote identifier for this patron, taken from
        the corresponding Credential.
        """
        def refresher(credential):
            remote_identifier = self.patron_remote_identifier_lookup(patron)
            if not remote_identifier:
                remote_identifier = self.create_patron(patron)
            credential.credential = remote_identifier
            credential.expires = None

        _db = Session.object_session(patron)
        credential = Credential.lookup(
            _db, DataSource.RB_DIGITAL,
            Credential.IDENTIFIER_FROM_REMOTE_SERVICE,
            patron, refresher_method=refresher,
            allow_persistent_token=True
        )
        if not credential.credential:
            refresher(credential)
        return credential.credential

    def create_patron(self, patron):
        """Ask RBdigital to create a new patron record.

        :param patron: the Patron that needs a new RBdigital account.

        :return The internal RBdigital identifier for this patron.
        """

        url = "%s/libraries/%s/patrons/" % (self.base_url, str(self.library_id))
        action="create_patron"

        patron_identifier = patron.identifier_to_remote_service(
            DataSource.RB_DIGITAL
        )

        post_args = dict()
        post_args['libraryId'] = self.library_id
        post_args['libraryCardNumber'] = patron_identifier

        # Generate meaningless values for account fields that are not
        # relevant to our usage of the API.
        post_args['userName'] = 'username_' + patron_identifier
        post_args['email'] = self.remote_email_address(patron)
        post_args['firstName'] = 'Patron'
        post_args['lastName'] = 'Reader'

        # The patron will not be logging in to this RBdigital account,
        # so set their password to a secure value and forget it.
        post_args['password'] = os.urandom(8).encode('hex')

        resp_dict = {}
        message = None
        response = self.request(
            url=url, data=json.dumps(post_args), method="post"
        )
        if response.text:
            resp_dict = response.json()
            message = resp_dict.get('message', None)

        # general validation
        self.validate_response(
            response=response, message=message, action=action
        )

        # double-make sure specifically
        if response.status_code != 201 or 'patronId' not in resp_dict:
            raise RemotePatronCreationFailedException(action + 
                ": http=" + str(response.status_code) + ", response=" + response.text)

        patron_oneclick_id = resp_dict['patronId']

        return patron_oneclick_id

    def patron_remote_identifier_lookup(self, patron):
        """Look up a patron's RBdigital account based on a unique ID
        assigned to them for this purpose.

        :return: The RBdigital patron ID for the patron, or None
        if the patron currently has no RBdigital account.
        """
        patron_identifier = patron.identifier_to_remote_service(
            DataSource.RB_DIGITAL
        )

        action="patron_id"
        url = "%s/rpc/libraries/%s/patrons/%s" % (
            self.base_url, self.library_id, patron_identifier
        )

        response = self.request(url)

        resp_dict = response.json()
        message = resp_dict.get('message', None)
        try:
            self.validate_response(response, message, action=action)
        except (PatronNotFoundOnRemote, NotFoundOnRemote), e:
            # That's okay.
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
            loan_info = self._make_loan_info(item)
            if loan_info:
                loans.append(loan_info)
        return loans

    def _make_loan_info(self, item, fulfill=False):
        """Convert one of the items returned by a request to /checkouts into a
        LoanInfo with an RBFulfillmentInfo.
        """

        media_type = item.get('mediaType', 'eBook')
        isbn = item.get('isbn', None)

        # 'expiration' here refers to the expiration date of the loan, not
        # of the fulfillment URL.
        expires = item.get('expiration', None)
        if expires:
            expires = datetime.datetime.strptime(
                expires, self.EXPIRATION_DATE_FORMAT
            ).date()

        identifier, made_new = Identifier.for_foreign_id(
            self._db, foreign_identifier_type=Identifier.RB_DIGITAL_ID, 
            foreign_id=isbn, autocreate=False
        )
        if not identifier:
            # We have never heard of this book, which means the patron
            # didn't borrow it through us.
            return None

        fulfillment_info = RBFulfillmentInfo(
            self,
            DataSource.RB_DIGITAL,
            identifier, 
            item,
        )

        return LoanInfo(
            self.collection,
            DataSource.RB_DIGITAL,
            Identifier.RB_DIGITAL_ID,
            isbn,
            start_date=None,
            end_date=expires,
            fulfillment_info=fulfillment_info,
        )

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
                DataSource.RB_DIGITAL,
                Identifier.RB_DIGITAL_ID,
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
        patron_oneclick_id = self.patron_remote_identifier(patron)

        patron_checkouts = self.get_patron_checkouts(patron_oneclick_id)
        patron_holds = self.get_patron_holds(patron_oneclick_id)

        return patron_checkouts + patron_holds


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
            media = licensepool.work.presentation_edition.medium

        return item_oneclick_id, media

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
                elif message == "Title is not available for checkout":
                    # This will put the book on hold, and if it ever
                    # shows up again it'll be checked out
                    # automatically. If it doesn't show up again...
                    # best not to think about that.
                    raise NoAvailableCopies(message)
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
            if message == 'success':
                # There is no additional information to be had.
                return
            elif message.startswith("eXtensible Framework was unable to locate the resource for RB.API.OneClick.UserPatron.Get"):
                # http code was OK, but info wasn't sucessfully read from db
                raise PatronNotFoundOnRemote(action + ": " + message)
            else:
                self.log.warning("%s not retrieved: %s ", action, message)
                raise CirculationException(action + ": " + message)


    def queue_response(self, status_code, headers={}, content=None):
        """ Allows smoother faster creation of unit tests by letting 
        us live-test as we write. """
        pass


class RBFulfillmentInfo(object):
    """An RBdigital-specific FulfillmentInfo implementation.

    We use these instead of real FulfillmentInfo objects because
    generating a FulfillmentInfo object may require an extra HTTP request,
    and there's often no need to make that request.
    """

    def __init__(self, api, data_source_name, identifier,
                 raw_data):
        self.api = api
        self.collection = api.collection
        self.data_source_name = data_source_name
        self._identifier = identifier
        self.identifier_type = identifier.type
        self.identifier = identifier.identifier
        self.raw_data = raw_data

        self._fetched = False
        self._content_link = None
        self._content_type = None
        self._content = None
        self._content_expires = None
        
    @property
    def content_link(self):
        self.fetch()
        return self._content_link

    @property
    def content_type(self):
        self.fetch()
        return self._content_type

    @property
    def content(self):
        self.fetch()
        return self._content

    @property
    def content_expires(self):
        self.fetch()
        return self._content_expires

    def fetch(self):
        if self._fetched:
            return

        # Get a list of files associated with this loan.
        files = self.raw_data.get('files', [])

        # Determine if we're fulfilling an audiobook (which means sending a
        # manifest) or an ebook (which means sending a download link).
        individual_download_url = None
        representation_format = None
        if files:
            # If we have an ebook, there should only be one file in
            # the list. If we have an audiobook, the first file should
            # be representative of the whole.
            file = files[0]
            file_format = file.get('fileFormat', None)
            if file_format == 'EPUB':
                file_format = Representation.EPUB_MEDIA_TYPE
            else:
                # Audio books don't list a fileFormat at all. TODO:
                # they do list a mediaType, which could be useful.
                file_format = Representation.AUDIOBOOK_MANIFEST_MEDIA_TYPE
            self._content_type = file_format
            individual_download_url = file.get('downloadUrl', None)
            
        if self._content_type == Representation.AUDIOBOOK_MANIFEST_MEDIA_TYPE:
            # We have an audiobook.
            self._content = self.process_audiobook_manifest(self.raw_data)
        else:
            # We have some other kind of file. Follow the download
            # link, which will return a JSON-based access document
            # pointing to the 'real' download link.
            #
            # We don't send our normal RBdigital credentials with this
            # request because it's going to a different, publicly
            # accessible server.
            access_document = self.api._make_request(
                individual_download_url, 'GET', {}
            )
            self._content_type, self._content_link, self._content_expires = self.process_access_document(
                access_document
            )
        self._fetched = True

    @classmethod
    def process_audiobook_manifest(self, rb_data):
        """Convert RBdigital's proprietary manifest format
        into a standard Audiobook Manifest document.
        """
        return unicode(AudiobookManifest(rb_data))

    @classmethod
    def process_access_document(self, access_document):
        """Process the intermediary document served by RBdigital to tell
        you how to actually download a file.
        """
        data = json.loads(access_document.content)
        content_link = data.get('url')
        content_type = data.get('type')
        if content_type == 'application/vnd.adobe':
            # The manifest spells the media type wrong. Fix it.
            content_type = DeliveryMechanism.ADOBE_DRM

        # Now that we've found the download URL, the client has 15
        # minutes to use it. Set it to expire in 14 minutes to be
        # conservative.
        expires = datetime.datetime.utcnow() + datetime.timedelta(minutes=14)
        return content_type, content_link, expires


class MockOneClickAPI(BaseMockOneClickAPI, OneClickAPI):

    @classmethod
    def mock_collection(cls, _db):
        collection = BaseMockOneClickAPI.mock_collection(_db)
        for library in _db.query(Library):
            for key, value in (
                    (Collection.AUDIOBOOK_LOAN_DURATION_KEY, 1),
                    (Collection.EBOOK_LOAN_DURATION_KEY, 2)
            ):
                ConfigurationSetting.for_library_and_externalintegration(
                    _db, key, library, 
                    collection.external_integration
                ).value = value
        return collection


class OneClickCirculationMonitor(CollectionMonitor):
    """Maintain LicensePools for OneClick titles.

    Bibliographic data isn't inserted into new LicensePools until
    we hear from the metadata wrangler.
    """
    SERVICE_NAME = "OneClick CirculationMonitor"
    DEFAULT_START_TIME = datetime.datetime(1970, 1, 1)
    INTERVAL_SECONDS = 1200
    DEFAULT_BATCH_SIZE = 50

    PROTOCOL = ExternalIntegration.RB_DIGITAL
    
    def __init__(self, _db, collection, batch_size=None, api_class=OneClickAPI,
                 api_class_kwargs={}):
        super(OneClickCirculationMonitor, self).__init__(_db, collection)
        self.batch_size = batch_size or self.DEFAULT_BATCH_SIZE

        self.api = api_class(_db, self.collection, **api_class_kwargs)
        self.bibliographic_coverage_provider = (
            OneClickBibliographicCoverageProvider(
                collection=self.collection, api_class=self.api,
            )
        )
        self.analytics = Analytics(self._db)

    def process_availability(self, media_type='eBook'):
        # get list of all titles, with availability info
        policy = self.api.default_circulation_replacement_policy
        availability_list = self.api.get_ebook_availability_info(media_type=media_type)
        item_count = 0
        for availability in availability_list:
            isbn = availability['isbn']
            # boolean True/False value, not number of licenses
            available = availability['availability']

            medium = availability.get('mediaType')
            license_pool, is_new, is_changed = self.api.update_licensepool_for_identifier(
                isbn, available, medium, policy
            )
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
        ebook_count = self.process_availability(media_type='eBook')
        eaudio_count = self.process_availability(media_type='eAudio')
        
        self.log.info("Processed %d ebooks and %d audiobooks.", ebook_count, eaudio_count)


class AudiobookManifest(CoreAudiobookManifest):
    """A standard AudiobookManifest derived from an RBdigital audiobook
    manifest.
    """

    # Information not used because it's redundant or not useful.
    # "bookmarks": [],
    # "hasBookmark": false,
    # "mediaType": "eAudio",
    # "dateAdded": "2011-03-28",

    # Information not used because it's loan-specific
    # "expiration": "2017-11-15",
    # "canRenew": true,
    # "transactionId": 101,
    # "patronId": 111,
    # "libraryId": 222

    def __init__(self, content_dict, **kwargs):
        super(AudiobookManifest, self).__init__(**kwargs)
        self.raw = content_dict

        # Metadata values that map directly onto the core spec.
        self.import_metadata('title')
        self.import_metadata('publisher')
        self.import_metadata('description')
        self.import_metadata('isbn', 'identifier')
        self.import_metadata('authors', 'author')
        self.import_metadata('narrators', 'narrator')
        self.import_metadata('minutes', 'duration', lambda x: x*60)

        # Metadata values that have no equivalent in the core spec,
        # but are potentially useful.
        self.import_metadata('size', 'schema:contentSize')
        self.import_metadata('titleid', 'rbdigital:id', str)
        self.import_metadata('hasDrm', 'rbdigital:hasDrm')
        self.import_metadata('encryptionKey', 'rbdigital:encryptionKey')

        # Spine items.
        for file_data in self.raw.get('files', []):
            self.import_spine(file_data)

        # Links.
        download_url = self.raw.get('downloadUrl')
        if download_url:
            self.add_link(
                download_url, 'alternate', 
                type=Representation.guess_media_type(download_url)
            )

        cover = self.best_cover(self.raw.get('images', []))
        if cover:
            self.add_link(
                cover, "cover", type=Representation.guess_media_type(cover)
            )

    @classmethod
    def best_cover(self, images=[]):
        if not images:
            return None
        # Find the largest image that's large enough to use as a
        # cover.
        sizes = ['xx-large', 'x-large', 'large']
        images_by_size = {}
        for image in images:
            size = image.get('name')
            href = image.get('url')
            if href and size in sizes:
                images_by_size[size] = href

        for size in sizes:
            if size in images_by_size:
                return images_by_size[size]

    def import_metadata(
            self, rbdigital_field, standard_field=None, transform=None
    ):
        """Map a field in an RBdigital manifest to the corresponding
        standard manifest field.
        """
        standard_field = standard_field or rbdigital_field
        value = self.raw.get(rbdigital_field)
        if value is None:
            return
        if transform:
            value = transform(value)
        self.metadata[standard_field] = value

    def import_spine(self, file_data):
        """Import an RBdigital spine item as a Web Publication Manifest
        spine item.
        """
        href = file_data.get('downloadUrl')
        duration = file_data.get('minutes') * 60
        title = file_data.get('display')

        id = file_data.get('id')
        size = file_data.get('size')
        filename = file_data.get('filename')
        type = Representation.guess_media_type(filename)

        extra = {}
        for k, v, transform in (
                ('id', 'rbdigital:id', str),
                ('size', 'schema:contentSize', lambda x: x),
                ('minutes', 'duration', lambda x: x*60),
        ):
            if k in file_data:
                extra[v] = transform(file_data[k])
        self.add_spine(href, type, title, **extra)
