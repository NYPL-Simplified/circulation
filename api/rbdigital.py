import base64
from collections import defaultdict
import datetime
from dateutil.relativedelta import relativedelta
from flask_babel import lazy_gettext as _
import json
import logging
from nose.tools import set_trace
import os
import re
import requests
from sqlalchemy.orm.session import Session
import uuid

from circulation import (
    APIAwareFulfillmentInfo,
    BaseCirculationAPI,
    FulfillmentInfo,
    HoldInfo,
    LoanInfo,
)
from circulation_exceptions import *

from config import Configuration

from core.analytics import Analytics

from core.config import (
    CannotLoadConfiguration,
    Configuration,
    temp_config,
)

from core.coverage import BibliographicCoverageProvider, CoverageFailure

from core.metadata_layer import (
    CirculationData,
    ContributorData,
    FormatData,
    IdentifierData,
    LinkData,
    Metadata,
    ReplacementPolicy,
    SubjectData,
)

from core.model import (
    CirculationEvent,
    Collection,
    ConfigurationSetting,
    Contributor,
    Credential,
    DataSource,
    DeliveryMechanism,
    Edition,
    ExternalIntegration,
    get_one,
    get_one_or_create,
    Hyperlink,
    Identifier,
    Library,
    LicensePool,
    Patron,
    Representation,
    Session,
    Subject,
    Work,
)

from core.monitor import (
    CollectionMonitor,
)

from core.testing import DatabaseTest
from core.util import LanguageCodes

from core.util.http import (
    BadResponseException,
    HTTP,
)

from core.util.personal_names import (
    name_tidy,
    sort_name_to_display_name
)

from core.util.web_publication_manifest import (
    AudiobookManifest as CoreAudiobookManifest
)

from selftest import (
    HasSelfTests,
    SelfTestResult,
)

class RBDigitalAPI(BaseCirculationAPI, HasSelfTests):

    NAME = ExternalIntegration.RB_DIGITAL

    # The loan duration must be specified when connecting a library to an
    # RBdigital account, but if it's not specified, try one week.

    DEFAULT_LOAN_DURATION = 7
    API_VERSION = "v1"
    PRODUCTION_BASE_URL = "https://api.rbdigital.com/"
    QA_BASE_URL = "http://api.rbdigitalstage.com/"
    SERVER_NICKNAMES = {
        "production" : PRODUCTION_BASE_URL,
        "qa" : QA_BASE_URL,
    }

    BASE_SETTINGS = [x for x in BaseCirculationAPI.SETTINGS
                     if x['key'] != BaseCirculationAPI.DEFAULT_LOAN_PERIOD]

    SETTINGS = [
        { "key": ExternalIntegration.PASSWORD, "label": _("Basic Token") },
        { "key": Collection.EXTERNAL_ACCOUNT_ID_KEY, "label": _("Library ID") },
        { "key": ExternalIntegration.URL, "label": _("URL"), "default": PRODUCTION_BASE_URL },
    ] + BASE_SETTINGS

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

    DATE_FORMAT = "%Y-%m-%d" #ex: 2013-12-27

    # a complete response returns the json structure with more data fields than a basic response does
    RESPONSE_VERBOSITY = {0:'basic', 1:'compact', 2:'complete', 3:'extended', 4:'hypermedia'}

    log = logging.getLogger("RBDigital Patron API")

    def __init__(self, _db, collection):
        if collection.protocol != ExternalIntegration.RB_DIGITAL:
            raise ValueError(
                "Collection protocol is %s, but passed into RBDigitalAPI!" %
                collection.protocol
            )
        self._db = _db
        self.collection_id = collection.id
        self.library_id = collection.external_account_id
        self.token = collection.external_integration.password

        if not (self.library_id and self.token):
            raise CannotLoadConfiguration(
                "RBDigital configuration is incomplete."
            )

        # Use utf8 instead of unicode encoding
        self.library_id = self.library_id.encode('utf8')
        self.token = self.token.encode('utf8')

        # Convert the nickname for a server into an actual URL.
        base_url = collection.external_integration.url or self.PRODUCTION_BASE_URL
        if base_url in self.SERVER_NICKNAMES:
            base_url = self.SERVER_NICKNAMES[base_url]
        self.base_url = (base_url + self.API_VERSION).encode("utf8")
        self.bibliographic_coverage_provider = (
            RBDigitalBibliographicCoverageProvider(
                self.collection, api_class=self
            )
        )

    def external_integration(self, _db):
        return self.collection.external_integration

    def _run_self_tests(self, _db):
        def count(media_type):
            # Call get_ebook_availability_info and count how many titles
            # are available/unavailable. If our credentials are bad,
            # we'll get an error message.
            result = self.get_ebook_availability_info(media_type)

            available = 0
            unavailable = 0
            if isinstance(result, dict):
                # This is most likely an error condition.
                message = result.get(
                    'message', 'Unexpected response from server'
                )
                raise IntegrationException(message, repr(result))

            for i in result:
                if i.get('availability', False):
                    available += 1
                else:
                    unavailable += 1
            msg = "Total items: %d (%d currently loanable, %d currently not loanable)"
            return msg % (len(result), available, unavailable)

        response = self.run_test(
            "Counting ebooks in collection",
            count, 'eBook'
        )
        yield response
        if not response.success:
            # If we can't even see the collection properly, something is
            # wrong and we should not continue.
            return

        yield self.run_test(
            "Counting audiobooks in collection",
            count, 'eAudio'
        )

        for result in self.default_patrons(self.collection):
            if isinstance(result, SelfTestResult):
                yield result
                continue
            library, patron, pin = result
            task = "Checking patron activity, using test patron for library %s" % library.name
            def count_loans_and_holds(patron, pin):
                activity = self.patron_activity(patron, pin)
                return "Total loans and holds: %s" % len(activity)
            yield self.run_test(
                task, count_loans_and_holds, patron, pin
            )

    @classmethod
    def create_identifier_strings(cls, identifiers):
        identifier_strings = []
        for i in identifiers:
            if isinstance(i, Identifier):
                value = i.identifier
            else:
                value = i
            identifier_strings.append(value)

        return identifier_strings

    @property
    def source(self):
        return DataSource.lookup(self._db, DataSource.RB_DIGITAL)

    @property
    def collection(self):
        return Collection.by_id(self._db, id=self.collection_id)

    @property
    def authorization_headers(self):
        # the token given us by RBDigital is already utf/base64-encoded
        authorization = self.token
        return dict(Authorization="Basic " + authorization)

    def _make_request(self, url, method, headers, data=None, params=None, **kwargs):
        """Actually make an HTTP request."""
        return HTTP.request_with_timeout(
            method, url, headers=headers, data=data,
            params=params, **kwargs
        )

    def request(self, url, method='get', extra_headers={}, data=None,
                params=None, verbosity='complete'):
        """Make an HTTP request.
        """
        if verbosity not in self.RESPONSE_VERBOSITY.values():
            verbosity = self.RESPONSE_VERBOSITY[2]

        headers = dict(extra_headers)
        headers['Content-Type'] = 'application/json'
        headers['Accept-Media'] = verbosity
        headers.update(self.authorization_headers)

        # prevent the code throwing a BadResponseException when RBDigital
        # responds with a 500, because RBDigital uses 500s to indicate bad input,
        # rather than server error.
        # must list all 9 possibilities to use
        allowed_response_codes = ['1xx', '2xx', '3xx', '4xx', '5xx', '6xx', '7xx', '8xx', '9xx']
        # for now, do nothing with disallowed error codes, but in the future might have
        # some that will warrant repeating the request.
        disallowed_response_codes = []

        response = self._make_request(
            url=url, method=method, headers=headers,
            data=data, params=params,
            allowed_response_codes=allowed_response_codes,
            disallowed_response_codes=disallowed_response_codes
        )

        if (response.content
            and 'Invalid Basic Token or permission denied' in response.content):
            raise BadResponseException(
                url, "Permission denied. This may be a temporary rate-limiting issue, or the credentials for this collection may be wrong.",
                debug_message=response.content,
                status_code=502
            )

        return response

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
        patron_rbdigital_id = self.patron_remote_identifier(patron)
        (item_rbdigital_id, item_media) = self.validate_item(licensepool)

        resp_dict = self.circulate_item(patron_id=patron_rbdigital_id, item_id=item_rbdigital_id, return_item=True)

        if resp_dict.get('message') == 'success':
            self.log.debug("Patron %s/%s returned item %s.", patron.authorization_identifier,
                patron_rbdigital_id, item_rbdigital_id)
            return True

        # should never happen
        raise CirculationException(
            "Unknown error %s/%s checking in %s." % (
                patron.authorization_identifier, patron_rbdigital_id,
                item_rbdigital_id
            )
        )

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
        patron_rbdigital_id = self.patron_remote_identifier(patron)
        (item_rbdigital_id, item_media) = self.validate_item(licensepool)

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

        resp_dict = self.circulate_item(patron_id=patron_rbdigital_id, item_id=item_rbdigital_id, return_item=False, days=days)

        if not resp_dict or ('error_code' in resp_dict):
            return None

        self.log.debug("Patron %s/%s checked out item %s with transaction id %s.", patron.authorization_identifier,
            patron_rbdigital_id, item_rbdigital_id, resp_dict['transactionId'])

        expires = today + datetime.timedelta(days=days)
        loan = LoanInfo(
            self.collection,
            DataSource.RB_DIGITAL,
            identifier_type=licensepool.identifier.type,
            identifier=item_rbdigital_id,
            start_date=today,
            end_date=expires,
            fulfillment_info=None,
        )
        return loan

    def circulate_item(self, patron_id, item_id, hold=False, return_item=False, days=None):
        """
        Borrow or return a catalog item.
        :param patron_id RBDigital internal id
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

        patron_rbdigital_id = self.patron_remote_identifier(patron)
        (item_rbdigital_id, item_media) = self.validate_item(licensepool)

        checkouts_list = self.get_patron_checkouts(patron_id=patron_rbdigital_id)

        # find this licensepool in patron's checkouts
        found_checkout = None
        for checkout in checkouts_list:
            if checkout.identifier == item_rbdigital_id:
                found_checkout = checkout
                break
        if not found_checkout:
            raise NoActiveLoan(
                "Cannot fulfill %s - patron %s/%s has no such checkout." % (
                    item_rbdigital_id, patron.authorization_identifier,
                    patron_rbdigital_id
                )
            )

        return found_checkout.fulfillment_info

    def place_hold(self, patron, pin, licensepool, notification_email_address):
        """Place a book on hold.

        Note: If the requested book is available for checkout, RBDigital will respond
        with a "success" to the hold request.  Then, at the next database clean-up sweep,
        RBDigital will automatically convert the hold record to a checkout record.

        :param patron: a Patron object for the patron who wants to check out the book.
        :param pin: The patron's password (not used).
        :param licensepool: The Identifier of the book to be checked out is
        attached to this licensepool.
        :param internal_format: Represents the patron's desired book format.  Ignored for now.

        :return: A HoldInfo object on success, None on failure
        """
        patron_rbdigital_id = self.patron_remote_identifier(patron)
        (item_rbdigital_id, item_media) = self.validate_item(licensepool)

        resp_obj = self.circulate_item(patron_id=patron_rbdigital_id, item_id=item_rbdigital_id, hold=True, return_item=False)

        # successful holds return a numeric transaction id
        try:
            transaction_id = int(resp_obj)
        except Exception, e:
            self.log.error("Item hold request failed: %r", e, exc_info=e)
            raise CannotHold(e.message)

        self.log.debug("Patron %s/%s reserved item %s with transaction id %s.", patron.authorization_identifier,
            patron_rbdigital_id, item_rbdigital_id, resp_obj)

        today = datetime.datetime.now()

        hold = HoldInfo(
            self.collection,
            DataSource.RB_DIGITAL,
            identifier_type=licensepool.identifier.type,
            identifier=item_rbdigital_id,
            start_date=today,
            # RBDigital sets hold expirations to 2050-12-31, as a "forever"
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
        patron_rbdigital_id = self.patron_remote_identifier(patron)
        (item_rbdigital_id, item_media) = self.validate_item(licensepool)

        resp_dict = self.circulate_item(patron_id=patron_rbdigital_id, item_id=item_rbdigital_id, hold=True, return_item=True)

        if resp_dict.get('message') == 'success':
            self.log.debug("Patron %s/%s released hold %s.", patron.authorization_identifier,
                patron_rbdigital_id, item_rbdigital_id)
            return True

        # should never happen
        raise CirculationException(
            "Unknown error %s/%s releasing %s." % (
                patron.authorization_identifier, patron_rbdigital_id,
                item_rbdigital_id
            )
        )

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
        Bibliographic coverage will be ensured for the RBDigital Identifier.
        Work will be created for the LicensePool and set as presentation-ready.

        :param isbn the identifier RBDigital uses
        :param availability boolean denoting if book can be lent to patrons
        :param medium: The name RBDigital uses for the book's medium.
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
            # RBDigital doesn't tell us the DRM scheme at this
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
        from RBDigital just to pick the one licensepool we want.
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
        post_args['userName'] = 'username' + patron_identifier.replace("-", "")
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

        # Extract the patron's RBDigital ID from the response document.
        patron_rbdigital_id = None
        if response.status_code == 201:
            patron_info = resp_dict.get('patron')
            if patron_info:
                patron_rbdigital_id = patron_info.get('patronId')

        if not patron_rbdigital_id:
            raise RemotePatronCreationFailedException(action +
                ": http=" + str(response.status_code) + ", response=" + response.text)
        return patron_rbdigital_id

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

        :param patron_id RBDigital internal id for the patron.
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
            identifier.type,
            identifier.identifier,
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
        :param patron_id RBDigital internal id for the patron.
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
            # Note: if RBDigital knows about a patron's checked-out item that wasn't
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
        Retrieves patron's name, email, library card number from RBDigital.

        :param patron_id RBDigital's internal id for the patron.
        """
        if not patron_id:
            raise InvalidInputException("Need patron RBDigital id.")

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
        patron_rbdigital_id = self.patron_remote_identifier(patron)

        patron_checkouts = self.get_patron_checkouts(patron_rbdigital_id)
        patron_holds = self.get_patron_holds(patron_rbdigital_id)

        return patron_checkouts + patron_holds


    ''' -------------------------- Validation Handling -------------------------- '''
    def validate_item(self, licensepool):
        """ Are we performing operations on a book that exists and can be
        uniquely identified?
        """
        item_rbdigital_id = None
        media = None

        identifier = licensepool.identifier
        item_rbdigital_id=identifier.identifier
        if not item_rbdigital_id:
            raise InvalidInputException("Licensepool %r doesn't know its ISBN.", licensepool)

        if licensepool.work and licensepool.work.presentation_edition:
            media = licensepool.work.presentation_edition.medium

        return item_rbdigital_id, media

    def validate_response(self, response, message, action=""):
        """ RBDigital tries to communicate statuses and errors through http codes.
        Malformed url requests will throw a 500, non-existent ids will get a 404,
        trying an action like checkout on a patron/item combo that's blocked
        (like if the item is already checked out, for example) will get a 409, etc..
        Further details are usually elaborated on in the "message" field of the response.

        :param response http response object
        :message RBDigital puts error explanation into 'message' field in response dictionary
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

    ''' --------------------- Getters and Setters -------------------------- '''

    def get_all_available_through_search(self):
        """
        Gets a list of ebook and eaudio items this library has access to, that are currently
        available to lend.  Uses the "availability" facet of the search function.
        An alternative to self.get_availability_info().
        Calls paged search until done.
        Uses minimal verbosity for result set.

        Note:  Some libraries can see other libraries' catalogs, even if the patron
        cannot checkout the items.  The library ownership information is in the "interest"
        fields of the response.

        :return A dictionary representation of the response, containing catalog count and ebook item - interest pairs.
        """
        page = 0;
        response = self.search(availability='available', verbosity=self.RESPONSE_VERBOSITY[0])

        try:
            respdict = response.json()
        except Exception, e:
            raise BadResponseException("availability_search", "RBDigital availability response not parseable.")

        if not respdict:
            raise BadResponseException("availability_search", "RBDigital availability response not parseable - has no structure.")

        if not ('pageIndex' in respdict and 'pageCount' in respdict):
            raise BadResponseException("availability_search", "RBDigital availability response not parseable - has no page counts.")

        page_index = respdict['pageIndex']
        page_count = respdict['pageCount']

        while (page_count > (page_index+1)):
            page_index += 1
            response = self.search(availability='available', verbosity=self.RESPONSE_VERBOSITY[0], page_index=page_index)
            tempdict = response.json()
            if not ('items' in tempdict):
                raise BadResponseException("availability_search", "RBDigital availability response not parseable - has no next dict.")
            item_interest_pairs = tempdict['items']
            respdict['items'].extend(item_interest_pairs)

        return respdict


    def get_all_catalog(self):
        """
        Gets the entire RBDigital catalog for a particular library.

        Note:  This call taxes RBDigital's servers, and is to be performed sparingly.
        The results are returned unpaged.

        Also, the endpoint returns about as much metadata per item as the media/{isbn} endpoint does.
        If want more metadata, perform a search.

        :return A list of dictionaries representation of the response.
        """
        url = "%s/libraries/%s/media/all" % (self.base_url, str(self.library_id))

        response = self.request(url)

        try:
            resplist = response.json()
        except Exception, e:
            raise BadResponseException(url, "RBDigital all catalog response not parseable.")

        return response.json()

    def get_delta(self, from_date=None, to_date=None, verbosity=None):
        """
        Gets the changes to the library's catalog.

        Note:  As of now, RBDigital saves deltas for past 6 months, and can display them
        in max 2-month increments.

        :return A dictionary listing items added/removed/modified in the collection.
        """
        url = "%s/libraries/%s/media/delta" % (self.base_url, str(self.library_id))

        today = datetime.datetime.now()
        two_months = datetime.timedelta(days=60)
        six_months = datetime.timedelta(days=180)

        # from_date must be real, and less than 6 months ago
        if from_date and isinstance(from_date, basestring):
            from_date = datetime.datetime.strptime(from_date[:10], self.DATE_FORMAT)
            if (from_date > today) or ((today-from_date) > six_months):
                raise ValueError("from_date %s must be real, in the past, and less than 6 months ago." % from_date)

        # to_date must be real, and not in the future or too far in the past
        if to_date and isinstance(to_date, basestring):
            to_date = datetime.datetime.strptime(to_date[:10], self.DATE_FORMAT)
            if (to_date > today) or ((today - to_date) > six_months):
                raise ValueError("to_date %s must be real, and neither in the future nor too far in the past." % to_date)

        # can't reverse time direction
        if from_date and to_date and (from_date > to_date):
            raise ValueError("from_date %s cannot be after to_date %s." % (from_date, to_date))

        # can request no more that two month date range for catalog delta
        if from_date and to_date and ((to_date - from_date) > two_months):
            raise ValueError("from_date %s - to_date %s asks for too-wide date range." % (from_date, to_date))

        if from_date and not to_date:
            to_date = from_date + two_months
            if to_date > today:
                to_date = today

        if to_date and not from_date:
            from_date = to_date - two_months
            if from_date < today - six_months:
                from_date = today - six_months

        if not from_date and not to_date:
            from_date = today - two_months
            to_date = today

        args = dict()
        args['begin'] = from_date
        args['end'] = to_date

        response = self.request(url, params=args, verbosity=verbosity)
        return response.json()

    def get_ebook_availability_info(self, media_type='ebook'):
        """
        Gets a list of ebook items this library has access to, through the "availability" endpoint.
        The response at this endpoint is laconic -- just enough fields per item to
        identify the item and declare it either available to lend or not.

        :param media_type 'eBook'/'eAudio'

        :return A list of dictionary items, each item giving "yes/no" answer on a book's current availability to lend.
        Example of returned item format:
            "timeStamp": "2016-10-07T16:11:52.5887333Z"
            "isbn": "9781420128567"
            "mediaType": "eBook"
            "availability": false
            "titleId": 39764
        """
        url = "%s/libraries/%s/media/%s/availability" % (self.base_url, str(self.library_id), media_type)

        response = self.request(url)

        try:
            resplist = response.json()
        except Exception, e:
            raise BadResponseException(url, "RBDigital availability response not parseable.")
        return resplist

    def get_metadata_by_isbn(self, identifier):
        """
        Gets metadata, s.a. publisher, date published, genres, etc for the
        eBook or eAudio item passed, using isbn to search on.
        If isbn is not found, the response we get from RBDigital is an error message,
        and we throw an error.

        :return the json dictionary of the response object
        """
        if not identifier:
            raise ValueError("Need valid identifier to get metadata.")

        identifier_string = self.create_identifier_strings([identifier])[0]
        url = "%s/libraries/%s/media/%s" % (self.base_url, str(self.library_id), identifier_string)

        response = self.request(url)

        try:
            respdict = response.json()
        except Exception, e:
            raise BadResponseException(url, "RBDigital isbn search response not parseable.")

        if not respdict:
            # should never happen
            raise BadResponseException(url, "RBDigital isbn search response not parseable - has no respdict.")

        if "message" in respdict:
            message = respdict['message']
            if (message.startswith("Invalid 'MediaType', 'TitleId' or 'ISBN' token value supplied: ") or
                message.startswith("eXtensible Framework was unable to locate the resource")):
                # we searched for item that's not in library's catalog -- a mistake, but not an exception
                return None
            else:
                # something more serious went wrong
                error_message = "get_metadata_by_isbn(%s) in library #%s catalog ran into problems: %s" % (identifier_string, str(self.library_id), error_message)
                raise BadResponseException(url, message)

        return respdict

    def populate_all_catalog(self):
        """ Call get_all_catalog to get all of library's book info from RBDigital.
        Create Work, Edition, LicensePool objects in our database.
        """
        catalog_list = self.get_all_catalog()
        items_transmitted = len(catalog_list)
        items_created = 0

        # the default policy doesn't update delivery mechanisms, which we do want to do
        metadata_replacement_policy = ReplacementPolicy.from_metadata_source()
        metadata_replacement_policy.formats = True

        coverage_provider = RBDigitalBibliographicCoverageProvider(
            self.collection, api_class=self,
            replacement_policy=metadata_replacement_policy
        )

        for catalog_item in catalog_list:
            result = coverage_provider.update_metadata(
                catalog_item=catalog_item
            )
            if not isinstance(result, CoverageFailure):
                items_created += 1

                if isinstance(result, Identifier):
                    # calls work.set_presentation_ready() for us
                    coverage_provider.handle_success(result)

                    # We're populating the catalog, so we can assume the list RBDigital
                    # sent us is of books we own licenses to.
                    # NOTE:  TODO later:  For the 4 out of 2000 libraries that chose to display
                    # books they don't own, we'd need to call the search endpoint to get
                    # the interest field, and then deal with licenses_owned.
                    for lp in result.licensed_through:
                        if lp.collection == self.collection:
                            lp.licenses_owned = 1

                            # Start off by assuming the book is available.
                            # If it's not, we'll hear differently the
                            # next time we use the collection delta API.
                            lp.licenses_available = 1
            if not items_created % 100:
                # Periodically commit the work done so that if there's
                # a failure, the subsequent run through this code will
                # take less time.
                self._db.commit()
        # stay data, stay!
        self._db.commit()

        return items_transmitted, items_created

    def populate_delta(self, months=1):
        """ Call get_delta for the last month to get all of the library's book info changes
        from RBDigital.  Update Work, Edition, LicensePool objects in our database.
        """
        today = datetime.datetime.utcnow()
        time_ago = relativedelta(months=months)

        delta = self.get_delta(from_date=(today - time_ago), to_date=today)
        if not delta or len(delta) < 1:
            return None, None

        items_added = delta[0].get("addedTitles", 0)
        items_removed = delta[0].get("removedTitles", 0)
        items_transmitted = len(items_added) + len(items_removed)
        items_updated = 0
        coverage_provider = RBDigitalBibliographicCoverageProvider(
            collection=self.collection, api_class=self
        )
        for catalog_item in items_added:
            result = coverage_provider.update_metadata(catalog_item)
            if not isinstance(result, CoverageFailure):
                items_updated += 1

                if isinstance(result, Identifier):
                    # calls work.set_presentation_ready() for us
                    coverage_provider.handle_success(result)

        for catalog_item in items_removed:
            metadata = RBDigitalRepresentationExtractor.isbn_info_to_metadata(catalog_item)

            if not metadata:
                # generate a CoverageFailure to let the system know to revisit this book
                # TODO:  if did not create a Work, but have a CoverageFailure for the isbn,
                # check that re-processing that coverage would generate the work.
                e = "Could not extract metadata from RBDigital data: %r" % catalog_item
                make_note = CoverageFailure(identifier, e, data_source=self.data_source, transient=True)

            # convert IdentifierData into Identifier, if can
            identifier, made_new = metadata.primary_identifier.load(_db=self._db)
            if identifier and not made_new:
                # Don't delete works from the database.  Set them to "not ours anymore".
                # TODO: This was broken but it didn't cause any test failures,
                # which means it needs a test.
                for pool in identifier.licensed_through:
                    if pool.licenses_owned > 0:
                        if pool.presentation_edition:
                            self.log.warn("Removing %s (%s) from circulation",
                                          pool.presentation_edition.title, pool.presentation_edition.author)
                        else:
                            self.log.warn(
                                "Removing unknown work %s from circulation.",
                                identifier.identifier
                            )
                    pool.licenses_owned = 0
                    pool.licenses_available = 0
                    pool.licenses_reserved = 0
                    pool.patrons_in_hold_queue = 0
                    pool.last_checked = today

                items_updated += 1

        # stay data, stay!
        self._db.commit()

        return items_transmitted, items_updated

    def search(self, mediatype='ebook', genres=[], audience=None, availability=None, author=None, title=None,
        page_size=100, page_index=None, verbosity=None):
        """
        Form a rest-ful search query, send to RBDigital, and obtain the results.

        :param mediatype Facet to limit results by media type.  Options are: "eAudio", "eBook".
        :param genres The books found lie at intersection of genres passed.
        :audience Facet to limit results by target age group.  Options include (there may be more): "adult",
            "beginning-reader", "childrens", "young-adult".
        :param availability Facet to limit results by copies left.  Options are "available", "unavailable", or None
        :param author Full name to search on.
        :param author Book title to search on.
        :param page_index Used for paginated result sets.  Zero-based.
        :param verbosity "basic" returns smaller number of response json lines than "complete", etc..

        :return the response object
        """
        url = "%s/libraries/%s/search" % (self.base_url, str(self.library_id))

        # make sure availability is in allowed format
        if availability not in ("available", "unavailable"):
            availability = None

        args = dict()
        if mediatype:
            args['media-type'] = mediatype
        if genres:
            args['genre'] = genres
        if audience:
            args['audience'] = audience
        if availability:
            args['availability'] = availability
        if author:
            args['author'] = author
        if title:
            args['title'] = title
        if page_size != 100:
            args['page-size'] = page_size
        if page_index:
            args['page-index'] = page_index

        response = self.request(url, params=args, verbosity=verbosity)
        return response

class RBFulfillmentInfo(APIAwareFulfillmentInfo):
    """An RBdigital-specific FulfillmentInfo implementation.

    We use these instead of real FulfillmentInfo objects because
    generating a FulfillmentInfo object may require an extra HTTP request,
    and there's often no need to make that request.
    """

    def do_fetch(self):
        # Get a list of files associated with this loan.
        files = self.key.get('files', [])

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
            self._content = self.process_audiobook_manifest(self.key)
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

class MockRBDigitalAPI(RBDigitalAPI):

    @classmethod
    def mock_collection(self, _db):
        library = DatabaseTest.make_default_library(_db)
        collection, ignore = get_one_or_create(
            _db, Collection,
            name="Test RBDigital Collection",
            create_method_kwargs=dict(
                external_account_id=u'library_id_123',
            )
        )
        integration = collection.create_external_integration(
            protocol=ExternalIntegration.RB_DIGITAL
        )
        integration.password = u'abcdef123hijklm'
        library.collections.append(collection)
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

    def __init__(self, _db, collection, base_path=None, **kwargs):
        self._collection = collection
        self.responses = []
        self.requests = []
        base_path = base_path or os.path.split(__file__)[0]
        self.resource_path = os.path.join(base_path, "files", "rbdigital")
        return super(MockRBDigitalAPI, self).__init__(_db, collection, **kwargs)

    @property
    def collection(self):
        """We can store the actual Collection object with a mock API,
        so there's no need to store the ID and do lookups.
        """
        return self._collection

    def queue_response(self, status_code, headers={}, content=None):
        from core.testing import MockRequestsResponse
        self.responses.insert(
            0, MockRequestsResponse(status_code, headers, content)
        )

    def _make_request(self, url, *args, **kwargs):
        self.requests.append([url, args, kwargs])
        response = self.responses.pop()
        return HTTP._process_response(
            url, response, kwargs.get('allowed_response_codes'),
            kwargs.get('disallowed_response_codes')
        )

    def get_data(self, filename):
        # returns contents of sample file as string and as dict
        path = os.path.join(self.resource_path, filename)
        data = open(path).read()
        return data, json.loads(data)

    def populate_all_catalog(self):
        """
        Set up to use the smaller test catalog file, and then call the real
        populate_all_catalog.  Used to test import on non-test permanent database.
        """
        datastr, datadict = self.get_data("response_catalog_all_sample.json")
        self.queue_response(status_code=200, content=datastr)
        items_transmitted, items_created = super(MockRBDigitalAPI, self).populate_all_catalog()

        return items_transmitted, items_created

class RBDigitalRepresentationExtractor(object):
    """ Extract useful information from RBDigital's JSON representations. """
    DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ" #ex: 2013-12-27T00:00:00Z
    DATE_FORMAT = "%Y-%m-%d" #ex: 2013-12-27

    log = logging.getLogger("RBDigital representation extractor")

    rbdigital_medium_to_simplified_medium = {
        "eBook" : Edition.BOOK_MEDIUM,
        "eAudio" : Edition.AUDIO_MEDIUM,
    }

    @classmethod
    def image_link_to_linkdata(cls, link_url, rel):
        if not link_url or (link_url.find("http") < 0):
            return None

        media_type = None
        if link_url.endswith(".jpg"):
            media_type = "image/jpeg"

        return LinkData(rel=rel, href=link_url, media_type=media_type)

    @classmethod
    def isbn_info_to_metadata(cls, book, include_bibliographic=True, include_formats=True):
        """Turn RBDigital's JSON representation of a book into a Metadata object.
        Assumes the JSON is in the format that comes from the media/{isbn} endpoint.

        TODO:  Use the seriesTotal field.

        :param book a json response-derived dictionary of book attributes
        """
        if not 'isbn' in book:
            return None
        rbdigital_id = book['isbn']
        primary_identifier = IdentifierData(
            Identifier.RB_DIGITAL_ID, rbdigital_id
        )

        # medium is both bibliographic and format information.

        # options are: "eBook", "eAudio"
        rbdigital_medium = book.get('mediaType', None)
        if rbdigital_medium and rbdigital_medium not in cls.rbdigital_medium_to_simplified_medium:
            cls.log.error(
                "Could not process medium %s for %s", rbdigital_medium, rbdigital_id)

        medium = cls.rbdigital_medium_to_simplified_medium.get(
            rbdigital_medium, Edition.BOOK_MEDIUM
        )

        metadata = Metadata(
            data_source=DataSource.RB_DIGITAL,
            primary_identifier=primary_identifier,
            medium=medium,
        )

        if include_bibliographic:
            title = book.get('title', None)
            # NOTE: An item that's part of a series, will have the seriesName field, and
            # will have its seriesPosition and seriesTotal fields set to >0.
            # An item not part of a series will have the seriesPosition and seriesTotal fields
            # set to 0, and will not have a seriesName at all.
            # Sometimes, series position and total == 0, for many series items (ex: "seriesName": "EngLits").
            # Sometimes, seriesName is set to "Default Blank", meaning "not actually a series".
            series_name = book.get('seriesName', None)
            series_position = None
            if series_name == 'Default Blank':
                # This is not actually a series.
                series_name = None
            else:
                series_position = book.get('seriesPosition', None)
                if series_position:
                    try:
                        series_position = int(series_position)
                    except ValueError:
                        # not big enough deal to stop the whole process
                        series_position = None

            # ignored for now
            series_total = book.get('seriesTotal', None)
            # ignored for now
            has_digital_rights = book.get('hasDigitalRights', None)

            publisher = book.get('publisher', None)
            if 'publicationDate' in book:
                published = datetime.datetime.strptime(
                    book['publicationDate'][:10], cls.DATE_FORMAT)
            else:
                published = None

            if 'language' in book:
                language = LanguageCodes.string_to_alpha_3(book['language'])
            else:
                language = 'eng'

            contributors = []
            if 'authors' in book:
                authors = book['authors']
                for author in authors.split(";"):
                    sort_name = author.strip()
                    if sort_name:
                        sort_name = name_tidy(sort_name)
                        display_name = sort_name_to_display_name(sort_name)
                        roles = [Contributor.AUTHOR_ROLE]
                        contributor = ContributorData(sort_name=sort_name, display_name=display_name, roles=roles)
                        contributors.append(contributor)

            if 'narrators' in book:
                narrators = book['narrators']
                for narrator in narrators.split(";"):
                    sort_name = narrator.strip()
                    if sort_name:
                        sort_name = name_tidy(sort_name)
                        display_name = sort_name_to_display_name(sort_name)
                        roles = [Contributor.NARRATOR_ROLE]
                        contributor = ContributorData(sort_name=sort_name, display_name=display_name, roles=roles)
                        contributors.append(contributor)

            subjects = []
            if 'genres' in book:
                # example: "FICTION / Humorous / General"
                genres = book['genres']
                subject = SubjectData(
                    type=Subject.BISAC, identifier=None, name=genres,
                    weight=100
                )
                subjects.append(subject)

            if 'primaryGenre' in book:
                # example: "humorous-fiction,mystery,womens-fiction"
                genres = book['primaryGenre']
                for genre in genres.split(","):
                    subject = SubjectData(
                        type=Subject.RBDIGITAL, identifier=genre.strip(),
                        weight=200
                    )
                    subjects.append(subject)

            # audience options are: adult, beginning-reader, childrens, young-adult
            # NOTE: In RBDigital metadata, audience can be set to "Adult" while publisher is "HarperTeen".
            audience = book.get('audience', None)
            if audience:
                subject = SubjectData(
                    type=Subject.RBDIGITAL_AUDIENCE,
                    identifier=audience.strip().lower(),
                    weight=500
                )
                subjects.append(subject)

            # passed to metadata.apply, the isbn_identifier will create an equivalency
            # between the RBDigital-labeled and the ISBN-labeled identifier rows, which
            # will in turn allow us to ask the MetadataWrangler for more info about the book.
            isbn_identifier = IdentifierData(Identifier.ISBN, rbdigital_id)

            identifiers = [primary_identifier, isbn_identifier]

            links = []
            # A cover and its thumbnail become a single LinkData.
            # images come in small (ex: 71x108px), medium (ex: 95x140px),
            # and large (ex: 128x192px) sizes
            if 'images' in book:
                images = book['images']
                for image in images:
                    if image['name'] == "large":
                        image_data = cls.image_link_to_linkdata(image['url'], Hyperlink.IMAGE)
                    if image['name'] == "medium":
                        thumbnail_data = cls.image_link_to_linkdata(image['url'], Hyperlink.THUMBNAIL_IMAGE)
                    if image['name'] == "small":
                        thumbnail_data_backup = cls.image_link_to_linkdata(image['url'], Hyperlink.THUMBNAIL_IMAGE)

                if not thumbnail_data and thumbnail_data_backup:
                    thumbnail_data = thumbnail_data_backup

                if image_data:
                    if thumbnail_data:
                        image_data.thumbnail = thumbnail_data
                    links.append(image_data)


            # Descriptions become links.
            description = book.get('description', None)
            if description:
                links.append(
                    LinkData(
                        # there can be fuller descriptions in the search endpoint output
                        rel=Hyperlink.SHORT_DESCRIPTION,
                        content=description,
                        media_type="text/html",
                    )
                )

            metadata.title = title
            metadata.language = language
            metadata.series = series_name
            metadata.series_position = series_position
            metadata.publisher = publisher
            metadata.published = published
            metadata.identifiers = identifiers
            metadata.subjects = subjects
            metadata.contributors = contributors
            metadata.links = links

        if include_formats:
            formats = []
            if metadata.medium == Edition.BOOK_MEDIUM:
                content_type = Representation.EPUB_MEDIA_TYPE
                drm_scheme = DeliveryMechanism.ADOBE_DRM
                formats.append(FormatData(content_type, drm_scheme))
            elif metadata.medium == Edition.AUDIO_MEDIUM:
                content_type = Representation.AUDIOBOOK_MANIFEST_MEDIA_TYPE,
                drm_scheme = DeliveryMechanism.NO_DRM
                formats.append(FormatData(content_type, drm_scheme))
            else:
                cls.log.warn("Unfamiliar format: %s", metadata.medium)

            # Make a CirculationData so we can write the formats,
            circulationdata = CirculationData(
                data_source=DataSource.RB_DIGITAL,
                primary_identifier=primary_identifier,
                formats=formats,
            )

            metadata.circulation = circulationdata

        return metadata

class RBDigitalBibliographicCoverageProvider(BibliographicCoverageProvider):
    """Fill in bibliographic metadata for RBDigital records."""

    SERVICE_NAME = "RBDigital Bibliographic Coverage Provider"
    DATA_SOURCE_NAME = DataSource.RB_DIGITAL
    PROTOCOL = ExternalIntegration.RB_DIGITAL
    INPUT_IDENTIFIER_TYPES = Identifier.RB_DIGITAL_ID
    DEFAULT_BATCH_SIZE = 25

    def __init__(self, collection, api_class=RBDigitalAPI, api_class_kwargs={},
                 **kwargs):
        """Constructor.

        :param collection: Provide bibliographic coverage to all
            RBDigital books in the given Collection.
        :param api_class: Instantiate this class with the given Collection,
            rather than instantiating RBDigitalAPI.
        """
        super(RBDigitalBibliographicCoverageProvider, self).__init__(
            collection, **kwargs
        )
        if isinstance(api_class, RBDigitalAPI):
            # We were passed in a specific API object. This is not
            # generally the done thing, but it is necessary when a
            # RBDigitalAPI object itself wants a
            # RBDigitalBibliographicCoverageProvider.
            if api_class.collection_id != collection.id:
                raise ValueError(
                    "Coverage provider and its API are scoped to different collections! (%s vs. %s)" % (
                        api_class.collection_id, collection.id
                    )
                )
            else:
                self.api = api_class
        else:
            # A web application should not use this option because it
            # will put a non-scoped session in the mix.
            _db = Session.object_session(collection)
            self.api = api_class(_db, collection, **api_class_kwargs)

    def process_item(self, identifier):
        """ RBDigital availability information is served separately from
        the book's metadata.  Furthermore, the metadata returned by the
        "book by isbn" request is less comprehensive than the data returned
        by the "search titles/genres/etc." endpoint.

        This method hits the "by isbn" endpoint and updates the bibliographic
        metadata returned by it.
        """
        try:
            response_dictionary = self.api.get_metadata_by_isbn(identifier)
        except BadResponseException as error:
            return self.failure(identifier, error.message)
        except IOError as error:
            return self.failure(identifier, error.message)

        if not response_dictionary:
            message = "Cannot find RBDigital metadata for %r" % identifier
            return self.failure(identifier, message)

        result = self.update_metadata(response_dictionary, identifier)

        if isinstance(result, Identifier):
            # calls work.set_presentation_ready() for us
            self.handle_success(result)

        return result

    def update_metadata(self, catalog_item, identifier=None):
        """
        Creates db objects corresponding to the book info passed in.

        Note: It is expected that CoverageProvider.handle_success, which is responsible for
        setting the work to be presentation-ready is handled in the calling code.

        :catalog_item - JSON representation of the book's metadata, coming from RBDigital.
        :return CoverageFailure or a database object (Work, Identifier, etc.)
        """
        metadata = RBDigitalRepresentationExtractor.isbn_info_to_metadata(catalog_item)

        if not metadata:
            # generate a CoverageFailure to let the system know to revisit this book
            # TODO:  if did not create a Work, but have a CoverageFailure for the isbn,
            # check that re-processing that coverage would generate the work.
            e = "Could not extract metadata from RBDigital data: %r" % catalog_item
            return self.failure(identifier, e)

        # convert IdentifierData into Identifier, if can
        if not identifier:
            identifier, made_new = metadata.primary_identifier.load(_db=self._db)

        if not identifier:
            e = "Could not create identifier for RBDigital data: %r" % catalog_item
            return self.failure(identifier, e)

        return self.set_metadata(identifier, metadata)

class RBDigitalSyncMonitor(CollectionMonitor):

    PROTOCOL = ExternalIntegration.RB_DIGITAL

    def __init__(self, _db, collection, api_class=RBDigitalAPI,
                 api_class_kwargs={}):
        """Constructor."""
        super(RBDigitalSyncMonitor, self).__init__(_db, collection)
        self.api = api_class(_db, collection, **api_class_kwargs)

    def run_once(self, start, cutoff):
        items_transmitted, items_created = self.invoke()
        self._db.commit()
        result_string = "%s items transmitted, %s items saved to DB" % (items_transmitted, items_created)
        self.log.info(result_string)

    def invoke(self):
        raise NotImplementedError()

class RBDigitalImportMonitor(RBDigitalSyncMonitor):

    SERVICE_NAME = "RBDigital Full Import"

    def invoke(self):
        timestamp = self.timestamp()
        if timestamp.counter and timestamp.counter > 0:
            self.log.debug(
                "Collection %s has already had its initial import; doing nothing.",
                self.collection.name or self.collection.id
            )
            return 0, 0
        result = self.api.populate_all_catalog()

        # Record the work was done so it's not done again.
        if not timestamp.counter:
            timestamp.counter = 1
        else:
            timestamp.counter += 1
        return result

class RBDigitalDeltaMonitor(RBDigitalSyncMonitor):

    SERVICE_NAME = "RBDigital Delta Sync"

    def invoke(self):
        return self.api.populate_delta()

class RBDigitalCirculationMonitor(CollectionMonitor):
    """Maintain LicensePools for RBDigital titles.

    Bibliographic data isn't inserted into new LicensePools until
    we hear from the metadata wrangler.
    """
    SERVICE_NAME = "RBDigital CirculationMonitor"
    DEFAULT_START_TIME = datetime.datetime(1970, 1, 1)
    INTERVAL_SECONDS = 1200
    DEFAULT_BATCH_SIZE = 50

    PROTOCOL = ExternalIntegration.RB_DIGITAL

    def __init__(self, _db, collection, batch_size=None, api_class=RBDigitalAPI,
                 api_class_kwargs={}):
        super(RBDigitalCirculationMonitor, self).__init__(_db, collection)
        self.batch_size = batch_size or self.DEFAULT_BATCH_SIZE

        self.api = api_class(_db, self.collection, **api_class_kwargs)
        self.bibliographic_coverage_provider = (
            RBDigitalBibliographicCoverageProvider(
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
        super(RBDigitalCirculationMonitor, self).run()

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
        self.add_reading_order(href, type, title, **extra)
