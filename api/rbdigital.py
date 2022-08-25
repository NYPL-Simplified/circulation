from collections import defaultdict
import datetime
from dateutil.relativedelta import relativedelta
from flask import Response
from flask_babel import lazy_gettext as _
import json
import logging
import os
import random
import re
import requests
from sqlalchemy.orm.session import Session
import string
import urllib.parse
import uuid

from .circulation import (
    APIAwareFulfillmentInfo,
    BaseCirculationAPI,
    FulfillmentInfo,
    HoldInfo,
    LoanInfo,
)
from .circulation_exceptions import *

from .config import Configuration

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
    TimestampData,
)

from core.model import (
    CirculationEvent,
    Classification,
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
from core.util.datetime_helpers import (
    datetime_utc,
    strptime_utc,
    utc_now,
)

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
from core.util.string_helpers import random_string

from .selftest import (
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
        { "key": ExternalIntegration.PASSWORD, "label": _("Basic Token"), "required": True },
        { "key": Collection.EXTERNAL_ACCOUNT_ID_KEY, "label": _("Library ID (numeric)"), "required": True, "type": "number"},
        { "key": ExternalIntegration.URL, "label": _("URL"), "default": PRODUCTION_BASE_URL, "required": True, "format": "url" },
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

    CACHED_IDENTIFIER_PROPERTY = 'patronId'
    BEARER_TOKEN_PROPERTY = 'bearer'

    # Parameterize credentials.
    # - The `label` property maps to Credential `type`.
    # - The `lifetime` is used to calculate Credential `expires`
    #   and is specified in seconds. If it is None, then the
    #   Credential does not expire.
    CREDENTIAL_TYPES = {
        CACHED_IDENTIFIER_PROPERTY: dict(
            label=Credential.IDENTIFIER_FROM_REMOTE_SERVICE,
            lifetime=None
        ),
        BEARER_TOKEN_PROPERTY: dict(
            label="Patron Bearer Token",
            # RBdigital advertises a 24 hour lifetime, but we'll
            # cache it for only 23.5 hours, just in case.
            lifetime=((24 * 60) - 30) * 60
        ),
    }
    # Because we don't allow proxied requests to refresh the bearer
    # token, we need to ensure that there is enough time to complete
    # those requests before the token expires. If there's not then
    # we'll refresh it before returning the proxied URLs. This
    # property specifies (in seconds) the length of time we allocate
    # to complete those requests. It must be shorter than the Patron
    # Bearer Token lifetime and is currently set to 30 minutes.
    PROXY_BEARER_GRACE_PERIOD = 30 * 60

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
        if verbosity not in list(self.RESPONSE_VERBOSITY.values()):
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
            and 'Invalid Basic Token or permission denied' in response.content.decode("utf-8")):
            raise BadResponseException(
                url, "Permission denied. This may be a temporary rate-limiting issue, or the credentials for this collection may be wrong.",
                debug_message=response.content,
                status_code=502
            )

        return response

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

        today = utc_now()

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

        except Exception as e:
            self.log.error("Item circulation request failed: %r", e, exc_info=e)
            raise RemoteInitiatedServerError(str(e), action)

        self.validate_response(response=response, message=message, action=action)

        return resp_obj

    def patron_fulfillment_request(self, patron, url, reauthorize=True):
        """Make a fulfillment request on behalf of a patron, using the
        a bearer token either previously cached or newly retrieved on
        behalf of the patron.

        If the `reauthorize` parameter is set to True, then if the request
        fails with status code 401 (invalid bearer token), then we will
        attempt to obtain a new bearer token for the patron and repeat
        the request.

        :param patron: A Patron.
        :param url: URL for a resource.
        :param reauthorize: (Optional) Boolean indicating whether to
            reauthorize the patron bearer token if we receive status code 401.
        :return: The request response.
        """
        content_type = 'application/json;charset=UTF-8'

        def perform_request(reauthorize=False):
            bearer_token = self.patron_bearer_token(patron)
            headers = {"Authorization": 'Bearer {}'.format(bearer_token),
                       "Content-Type": content_type}
            response = self._make_request(url, 'GET', headers)
            if response.status_code == 401 and reauthorize:
                self.reauthorize_patron_bearer_token(patron)
                response = perform_request(reauthorize=False)
            return response

        response = perform_request(reauthorize=reauthorize)

        return response

    def fulfill(
        self, patron, pin, licensepool, internal_format, part=None,
        fulfill_part_url=None
    ):
        """Get an actual resource file to the patron. This may
        represent the entire book or only one part of it.

        :param part: When the patron wants to fulfill a specific part
            of the book, rather than the title as a whole, this will be
            set to a string representation of the numeric position of the
            desired part.

        :param fulfill_part_url: When the book can be fulfilled in
            parts, this function will take a part number and generate the
            URL to fulfill that specific part.

        :return a FulfillmentInfo object.
        """

        patron_rbdigital_id = self.patron_remote_identifier(patron)
        (item_rbdigital_id, item_media) = self.validate_item(licensepool)

        # If we are going to return a manifest to the client, then its
        # links should proxy through this CM. If we're going to fulfill
        # an access document for a part, we'll need the true RBdigital
        # access document URL, so that we can fetch and return the real
        # fulfillment link to the client.
        fulfillment_proxy = RBDigitalFulfillmentProxy(patron, api=self, for_part=part)
        checkouts_list = self.get_patron_checkouts(patron_id=patron_rbdigital_id,
                                                   fulfill_part_url=fulfill_part_url,
                                                   request_fulfillment=fulfillment_proxy.make_request,
                                                   fulfillment_proxy=fulfillment_proxy)

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

        fulfillment = found_checkout.fulfillment_info
        if part is None:
            # They want the whole thing.
            return fulfillment

        # They want only one part of the book.
        return fulfillment.fulfill_part(part)

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
        except Exception as e:
            self.log.error("Item hold request failed: %r", e, exc_info=e)
            raise CannotHold(str(e))

        self.log.debug("Patron %s/%s reserved item %s with transaction id %s.", patron.authorization_identifier,
            patron_rbdigital_id, item_rbdigital_id, resp_obj)

        now = utc_now()

        hold = HoldInfo(
            self.collection,
            DataSource.RB_DIGITAL,
            identifier_type=licensepool.identifier.type,
            identifier=item_rbdigital_id,
            start_date=now,
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

    def patron_credential(self, kind, patron, value=None):
        """Provide the credential of the given type for the given Patron,
        either from the cache or by retrieving it from the remote service.

        The behavior is as follows:
            - If a value is specified, we'll cache it.
            - If no value is specified and no cached credential is present
              and unexpired, then we'll retrieve a value from the remote
              service and cache it.
            - The cached value will be returned.

        :param patron: A Patron.
        :param kind: The type of credential.
        :param value: An optional value for the credential, which, if
            provided, will replace replace the value in the cache.
        :return: The credential value for type `type` for the patron.
        """
        credential = self._patron_credential(kind, patron, value=value)
        value = credential.credential if credential else None
        return value

    def _patron_credential(self, kind, patron, value=None):
        """Provide the credential of the given type for the given Patron,
        either from the cache or by retrieving it from the remote service.

        The behavior is as follows:
            - If a value is specified, we'll cache it.
            - If no value is specified and no cached credential is present
              and unexpired, then we'll retrieve a value from the remote
              service and cache it.
            - The cached value will be returned.

        :param patron: A Patron.
        :param kind: The type of credential.
        :param value: An optional value for the credential, which, if
            provided, will replace replace the value in the cache.
        :return: The `Credential` of type `type` for the patron.
        """

        credential_type = self.CREDENTIAL_TYPES[kind].get('label', None)
        lifetime = self.CREDENTIAL_TYPES[kind].get('lifetime', None)
        is_persistent = (lifetime is None)

        # Force refresh if we've specified a value for the credential. That
        # ensures that both the expiration date and value are updated.
        force_refresh = (value is not None)

        # Credential.lookup() expects to pass a Credential to this refresh method
        def refresh_credential(credential):
            if lifetime is not None:
                credential.expires = (utc_now() + datetime.timedelta(seconds=lifetime))
            else:
                credential.expires = None

            if value:
                value_ = value
            else:
                # retrieve the credential from the remote service
                if kind == self.CACHED_IDENTIFIER_PROPERTY:
                    # value_ = self.fetch_patron_identifier(patron)
                    # From self.patron_remote_identifier
                    try:
                        value_ = self._find_or_create_remote_account(
                            patron
                        )

                    except CirculationException:
                        # If an exception was thrown by _find_or_create_remote_account
                        # delete the credential so we don't create a credential with
                        # None stored in credential.credential, then continue to raise
                        # the exception.
                        _db = Session.object_session(credential)
                        _db.delete(credential)
                        raise
                elif kind == self.BEARER_TOKEN_PROPERTY:
                    value_ = self.fetch_patron_bearer_token(patron)
                else:
                    raise NotImplementedError("No RBDigital credential of type '%s'" % kind)

            credential.credential = value_
            return credential

        _db = Session.object_session(patron)
        collection = Collection.by_id(_db, id=self.collection_id)
        credential = Credential.lookup(
            _db, DataSource.RB_DIGITAL, credential_type, patron,
            refresh_credential, force_refresh=force_refresh,
            collection=collection, allow_persistent_token=is_persistent
        )
        return credential

    @staticmethod
    def get_credential_by_token(_db, data_source, credential_type, token):
        return Credential.lookup_by_token(_db, data_source, credential_type, token)

    def fetch_patron_bearer_token(self, patron):
        """Obtain a patron bearer token for an RBdigital Patron.

        A patron bearer token for an account within an RBdigital collection
        can be obtained with the patron's RBdigital `userId` for that
        collection. (An initial bearer token also can also be captured
        when an RBdigital account is first created, but that is not
        applicable here.)

        We don't cache `userId's locally, but can retrieve them with the
        account's `username`. (This usually has the same value as the
        patron's barcode/authorization_identifier; but, because of the
        `Barcode+6` technique used to create accounts for patrons who don't
        have a registered email address, this is not always the case, so we
        cannot rely on it.) So, we obtain the username by looking it up
        using the `patronId`, a property that we cache locally.

        So, the process, in summary:
            - Get `patronId` from cache or RBdigital,
            - Fetch `username` using `patronId`.
            - Fetch `userId` using `username`.
            - Obtain `bearer` token using `userId`.

        :param patron: A Patron.
        :return: A bearer token associated with the patron.
        """

        def request_helper(url, method='get', data=None, action='(unspecified action)',
                           allowed_response_codes=None, transform=None,):
            if transform is None:
                transform = lambda body: body
            if allowed_response_codes is None:
                allowed_response_codes = [200, 201]
            message = None
            result = None

            response = self.request(url, method=method, data=data)
            if response.text:
                result = response.json()
                message = result.get('message', None)
            self.validate_response(response=response, message=message, action=action)
            if result and response.status_code in allowed_response_codes:
                result = transform(result)
            if result is None:
                raise PatronAuthorizationFailedException(action +
                ": http=" + str(response.status_code) + ", response=" + response.text)
            return result

        # start with a patron_id
        patron_id = self.patron_credential(self.CACHED_IDENTIFIER_PROPERTY, patron)

        username = request_helper(
            "%s/libraries/%s/patrons/%s" % (self.base_url, self.library_id, patron_id),
            action="lookup username",
            transform=lambda body: body['userName'],
        )

        user_id = request_helper(
            "%s/rpc/libraries/%s/patrons/%s" % (self.base_url, self.library_id, username),
            action="lookup userId",
            transform=lambda body: body['userId'],
        )

        bearer_token = request_helper(
            "%s/libraries/%s/tokens" % (self.base_url, self.library_id),
            method='post', data=json.dumps({'userId': user_id}),
            action="obtain patron bearer token",
            transform=lambda body: body['bearer'],
        )

        return bearer_token

    def cache_patron_bearer_token(self, patron, value):
        self.patron_credential(self.BEARER_TOKEN_PROPERTY, patron, value=value)

    def patron_bearer_token(self, patron):
        return self.patron_credential(self.BEARER_TOKEN_PROPERTY, patron)

    def reauthorize_patron_bearer_token(self, patron):
        return self.cache_patron_bearer_token(
            patron, value=self.fetch_patron_bearer_token(patron)
        )

    def patron_remote_identifier(self, patron):
        """Locate the identifier for the given Patron's account on the
        RBdigital side, creating a new RBdigital account if necessary.

        The identifier is cached in a persistent Credential object.

        The logic is complicated and spread out over multiple methods,
        so here it is all in one place:

        If an already-cached identifier is present, we use it.

        Otherwise, we look up the patron's barcode on RBdigital to try
        to find their existing RBdigital account.

        If we find an existing RBdigital account, we cache the
        identifier associated with that account.

        Otherwise, we need to create an RBdigital account for this patron:

        If the ILS provides access to the patron's email address, we
        create an account using the patron's actual barcode and email
        address. This will let them use the 'recover password' feature
        if they want to use the RBdigital web site.

        If the ILS does not provide access to the patron's email
        address, we create an account using the patron's actual
        barcode but with six random characters appended. This will let
        the patron create a new RBdigital account using their actual
        barcode, if they want to use the web site.

        :param patron: A Patron.
        :return: The identifier associated with the patron's (possibly
            newly created) RBdigital account. This is an
            RBdigital-internal identifier with no connection to any
            identifier used by the patron, the circulation manager,
            and the ILS.
        """

        return self.patron_credential(self.CACHED_IDENTIFIER_PROPERTY, patron)

    def _find_or_create_remote_account(self, patron):
        """Look up a patron on RBdigital, creating an account if necessary.

        :param patron: A Patron.
        :return: The identifier associated with the (possibly newly
            created) RBdigital account. This is an RBdigital-internal
            patron ID and has no connection to any identifier used
            by the patron, the circulation manager, and the ILS.
        """

        # Try the easy case -- the patron already set up an RBdigital
        # account using their authorization identifier.
        remote_identifier = self.patron_remote_identifier_lookup(
            patron.authorization_identifier
        )
        if remote_identifier:
            return remote_identifier

        # There is no RBdigital account associated with the patron's
        # authorization identifier. And there is no preexisting
        # Credential representing a dummy account, or this method
        # wouldn't have been called. We must create a new account.
        try:
            return self.create_patron(
                patron.library, patron.authorization_identifier,
                self.patron_email_address(patron),
                bearer_token_handler=lambda token: self.cache_patron_bearer_token(patron, token)
            )
        except RemotePatronCreationFailedException:
            # Its possible to get a RemotePatronCreationFailedException if an account
            # was already created for this patron, but never put in the DB due to an
            # error. Here we try to recover that account using its email address.
            remote_identifier = self.patron_remote_identifier_lookup(
                self.patron_email_address(patron)
            )
            if remote_identifier:
                return remote_identifier
            else:
                raise


    def create_patron(self, library, authorization_identifier, email_address,
                      bearer_token_handler=None):
        """Ask RBdigital to create a new patron record.

        :param library: Library for the patron that needs a new RBdigital
            account. This has no necessary connection to the 'library_id'
            associated with the RBDigitalAPI, since multiple circulation
            manager libraries may share an RBdigital account.
        :param authorization_identifier: The identifier the patron uses
            to authenticate with their library.
        :param email_address: The email address, if any, which the patron
            has shared with their library.

        :return The internal RBdigital identifier for this patron.
        """

        url = "%s/libraries/%s/patrons/" % (self.base_url, self.library_id)
        action="create_patron"

        post_args = self._create_patron_body(
            library, authorization_identifier, email_address
        )

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
            if bearer_token_handler and 'bearer' in resp_dict:
                bearer_token_handler(resp_dict['bearer'])

        if not patron_rbdigital_id:
            raise RemotePatronCreationFailedException(action +
                ": http=" + str(response.status_code) + ", response=" + response.text)
        return patron_rbdigital_id

    def _create_patron_body(self, library, authorization_identifier,
                            email_address):
        """Make the entity-body for a patron creation request.

        :param library: Library for the patron that needs a new RBdigital
            account.
        :param authorization_identifier: The identifier the patron uses
            to authenticate with their library.
        :param email_address: The email address, if any, which the patron
            has shared with their library.

        :return: A dictionary of key-value pairs to go along with an
        HTTP POST request.
        """
        if email_address:
            # We know the patron's email address. We can create an
            # account they can also use in other contexts.
            patron_identifier = authorization_identifier
            email_address = email_address
        else:
            # We don't know the patron's email address. We will create
            # a dummy account to avoid locking them out of the ability
            # to use an RBdigital account in other contexts.
            patron_identifier = self.dummy_patron_identifier(
                authorization_identifier
            )
            email_address = self.dummy_email_address(
                library, authorization_identifier
            )

        # If we are using the patron's actual authorization identifier,
        # then our best guess at a username is that same identifier.
        #
        # If we're making up a dummy authorization identifier, then
        # using that as the username will minimize the risk of taking
        # someone's username.
        #
        # Either way:
        username = patron_identifier

        post_args = dict()
        post_args['libraryId'] = self.library_id
        post_args['libraryCard'] = patron_identifier
        post_args['userName'] = username
        post_args['email'] = email_address
        post_args['firstName'] = 'Library'
        post_args['lastName'] = 'Simplified'
        post_args['postalCode'] = '11111'

        # We have no way of communicating the password to this patron.
        # Set it to a random value and forget it. If we're creating an
        # account with the patron's email address, they'll be able to
        # recover their password. If not, at least we didn't claim
        # their barcode, and they can make a new account if they want.
        post_args['password'] = random_string(8)
        return post_args

    def dummy_patron_identifier(self, authorization_identifier):
        """Add six random alphanumeric characters to the end of
        the given `authorization_identifier`.

        :return: A random identifier based on the input identifier.
        """
        alphabet = string.digits + string.ascii_uppercase
        addendum = "".join(random.choice(alphabet) for x in range(6))
        return authorization_identifier + addendum

    def dummy_email_address(self, library, authorization_identifier):
        """The fake email address to send to RBdigital when
        creating an account for the given patron.

        :param library: A Library.
        :param authorization_identifier: A patron's authorization identifier.
        :return: An email address unique to this patron which will
            bounce or reject all mail sent to it.
        """
        default = self.default_notification_email_address(library, None)
        if not default:
            raise RemotePatronCreationFailedException(
                _("Cannot create remote account for patron because library's default notification address is not set.")
            )
        # notifications@library.org
        #   =>
        # notifications+rbdigital-1234567890@library.org
        replace_with = '+rbdigital-%s@' % authorization_identifier
        return default.replace('@', replace_with, 1)

    def patron_remote_identifier_lookup(self, remote_identifier):
        """Look up a patron's RBdigital account based on an identifier
        associated with their circulation manager account.

        :param remote_identifier: Depending on the context, this may
            be the patron's actual barcode, or a random string _based_ on
            their barcode.

        :return: The internal RBdigital patron ID for the given
            identifier, or None if there is no corresponding RBdigital
            account.

        """
        action="patron_id"
        url = "%s/rpc/libraries/%s/patrons/%s" % (
            self.base_url, self.library_id, remote_identifier
        )

        response = self.request(url)

        resp_dict = response.json()
        message = resp_dict.get('message', None)
        try:
            self.validate_response(response, message, action=action)
        except (PatronNotFoundOnRemote, NotFoundOnRemote) as e:
            # That's okay.
            return None

        internal_patron_id = resp_dict.get('patronId', None)
        return internal_patron_id

    def get_patron_checkouts(self, patron_id, fulfill_part_url=None,
                             request_fulfillment=None, fulfillment_proxy=None):
        """
        Gets the books and audio the patron currently has checked out.
        Obtains fulfillment info for each item -- the way to fulfill a book
        is to get this list of possibilities first, and then call individual
        fulfillment endpoints on the individual items.

        :param patron_id RBDigital internal id for the patron.

        :param fulfill_part_url: A function that generates circulation
           manager fulfillment URLs for individual parts of a book.
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
        except Exception as e:
            self.log.error("Patron checkouts failed: %r", str(e), exc_info=e)
            raise RemoteInitiatedServerError(str(e), action)

        self.validate_response(response=response, message=message, action=action)

        # by now we can assume response is either empty or a list
        for item in resp_obj:
            loan_info = self._make_loan_info(
                item, fulfill_part_url=fulfill_part_url,
                request_fulfillment=request_fulfillment,
                fulfillment_proxy=fulfillment_proxy,
            )
            if loan_info:
                loans.append(loan_info)
        return loans



    def _make_loan_info(self, item, fulfill_part_url=None,
                        request_fulfillment=None, fulfillment_proxy=False):
        """Convert one of the items returned by a request to /checkouts into a
        LoanInfo with an RBFulfillmentInfo.

        :param fulfill_part_url: A function that generates circulation
           manager fulfillment URLs for individual parts of a book.
        """

        media_type = item.get('mediaType', 'eBook')
        isbn = item.get('isbn', None)

        # 'expiration' here refers to the expiration date of the loan, not
        # of the fulfillment URL.
        expires = item.get('expiration', None)
        if expires:
            expires = strptime_utc(expires, self.EXPIRATION_DATE_FORMAT).date()

        identifier, made_new = Identifier.for_foreign_id(
            self._db, foreign_identifier_type=Identifier.RB_DIGITAL_ID,
            foreign_id=isbn, autocreate=False
        )
        if not identifier:
            # We have never heard of this book, which means the patron
            # didn't borrow it through us.
            return None

        fulfillment_info = RBFulfillmentInfo(
            fulfill_part_url,
            request_fulfillment,
            self,
            DataSource.RB_DIGITAL,
            identifier.type,
            identifier.identifier,
            item,
            fulfillment_proxy=fulfillment_proxy,
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
        except Exception as e:
            self.log.error("Patron holds failed: %r", str(e), exc_info=e)
            raise RemoteInitiatedServerError(str(e), action)

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
                expires = strptime_utc(
                    expires, self.EXPIRATION_DATE_FORMAT
                ).date()

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
            self.log.info("%s call failed: %s ", action, message)

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
        except Exception as e:
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
        except Exception as e:
            raise BadResponseException(url, "RBDigital all catalog response not parseable.")

        return response.json()

    def get_delta(self, from_date=None, to_date=None, verbosity=None):
        """
        Gets the changes to the library's catalog.

        :return A dictionary listing items added/removed/modified in the collection.
        """
        url = "%s/libraries/%s/book-holdings/delta" % (self.base_url, str(self.library_id))

        # can't reverse time direction
        if from_date and to_date and (from_date > to_date):
            raise ValueError("from_date %s cannot be after to_date %s." % (from_date, to_date))

        from_date, to_date = self.align_dates_to_available_snapshots(from_date, to_date)
        if from_date == to_date:
            # This can happen because from_date and to_date from the call were the same,
            # but can also occur for the following reasons:
            # - only a single snapshot is available
            # - both dates are less than the date of the first snapshot
            # - both dates are greater than the date of the last snapshot
            raise ValueError("The effective begin and end RBDigital catalog snapshot dates cannot be the same.")

        args = dict()
        args['begin'] = from_date
        args['end'] = to_date

        response = self.request(url, params=args, verbosity=verbosity)
        return response.json()

    class _FuzzyBinarySearcher(object):
        """
        A fuzzy binary searcher sorts an array by its key, and then must either:
          - find an exact match, if one exists; or
          - return an "adjacent" index and the direction in which a match
            would have been found, had one existed.
        """
        INDEXED_GREATER_THAN_MATCH = -1
        INDEXED_EQUALS_MATCH = 0
        INDEXED_LESS_THAN_MATCH = 1

        def __init__(self, array, key=None):
            """
            Initialize the object with a sorted array.

            :param array: An array
            :param key: A function of one argument that is used to extract
                a comparison key from each element in array and by which
                the array is sorted. The default value is None (compare
                value to complete array element).
            """

            self.key = key or (lambda e: e)
            if not callable(self.key):
                raise TypeError("'key' must be 'None' or a callable.")
            self.sorted_list = sorted(array, key=self.key, )
            self._count = len(self.sorted_list)

        def __call__(self, value):
            """
            Search for value in array, returning a matching or "adjacent" index.
            Return the selected index and the relative direction to a match.
            (0 => match, -1 => value < match's value, 1 => value > match's value).
            An empty array returns None for both offset and direction.

            :param value: the value to find
            :return: offset (index), direction
            """
            if self._count == 0:
                return None, None

            start, stop = 0, self._count
            index = None
            direction = None
            while start < stop:
                index = start + stop >> 1
                current = self.key(self.sorted_list[index])
                if current < value:
                    start = index + 1
                    direction = self.INDEXED_LESS_THAN_MATCH
                elif current > value:
                    stop = index
                    direction = self.INDEXED_GREATER_THAN_MATCH
                else:
                    return index, self.INDEXED_EQUALS_MATCH
            return index, direction

    def align_dates_to_available_snapshots(self, from_date=None, to_date=None):
        """
        Given specified begin and end dates for a delta, return the best dates from those available.

        Note: It might be useful to raise an exception or log a message if either of the
        "best" dates is too distant from the associated specified date.

        The endpoint utilized returns a JSON array of "snapshot" objects (nb: tenantId is the library ID):
            Example snapshot format:
                "tenantId": 525,
                "asOf": "2020-04-07",
                "eBookCount": 1630,
                "eAudioCount": 13414,
                "totalCount": 15044

        :return Best available begin and end dates.
        """
        SNAPSHOT_DATE_FORMAT = "%Y-%m-%d"

        url = "%s/libraries/%s/book-holdings/delta/available-dates" % (self.base_url, str(self.library_id))

        response = self.request(url)
        try:
            snapshots = response.json()
        except ValueError as e:
            raise BadResponseException(url, "RBDigital available-dates response not parsable.")

        if len(snapshots) < 1:
            raise BadResponseException(url, "RBDigital available-dates response contains no snapshots.")

        def get_snapshot_date(snapshot):
            return snapshot["asOf"]

        fuzzy_snapshot_search = self._FuzzyBinarySearcher(snapshots, key=get_snapshot_date)
        snapshots = fuzzy_snapshot_search.sorted_list

        # need date strings here
        if from_date and isinstance(from_date, datetime.datetime):
            from_date = from_date.strftime(SNAPSHOT_DATE_FORMAT)
        if to_date and isinstance(to_date, datetime.datetime):
            to_date = to_date.strftime((SNAPSHOT_DATE_FORMAT))

        # Find the best snapshot for the begin date and for the end date.
        # The approach here is to widen the net when there is not an exact
        # match, such that begin date would be adjusted back and end date
        # forward. A missing begin date will be assigned the date of the
        # earliest snapshot; a missing end date, gets the date of the latest.
        if from_date is None:
            begin_date = get_snapshot_date(snapshots[0])
        else:
            index, relative = fuzzy_snapshot_search(from_date)
            if relative == fuzzy_snapshot_search.INDEXED_GREATER_THAN_MATCH and index > 0:
                index -= 1
            begin_date = get_snapshot_date(snapshots[index])

        if to_date is None:
            end_date = get_snapshot_date(snapshots[-1])
        else:
            index, relative = fuzzy_snapshot_search(to_date)
            if relative == fuzzy_snapshot_search.INDEXED_LESS_THAN_MATCH and index < len(snapshots) - 1:
                index += 1
            end_date = get_snapshot_date(snapshots[index])

        return begin_date, end_date

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
        except Exception as e:
            raise BadResponseException(url, "RBDigital availability response not parsable.")
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
        except Exception as e:
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
                error_message = "get_metadata_by_isbn(%s) in library #%s catalog ran into problems: %s" % (identifier_string, str(self.library_id), message)
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

    def populate_delta(self, months=1, today=None):
        """ Call get_delta for the last month to get all of the library's book info changes
        from RBDigital.  Update Work, Edition, LicensePool objects in our database.

        :param today: A date to use instead of the current date, for use in tests.
        """
        today = today or utc_now()
        time_ago = relativedelta(months=months)

        delta = self.get_delta(from_date=(today - time_ago), to_date=today)
        if not delta or len(delta) < 1:
            return None, None
        items_added = delta.get("addedBooks", [])
        items_removed = delta.get("removedBooks", [])
        items_transmitted = len(items_added) + len(items_removed)
        items_updated = 0
        coverage_provider = RBDigitalBibliographicCoverageProvider(
            collection=self.collection, api_class=self
        )
        for item in items_added:
            isbn = item["isbn"]
            catalog_item = self.get_metadata_by_isbn(isbn)
            result = coverage_provider.update_metadata(catalog_item)
            if not isinstance(result, CoverageFailure):
                items_updated += 1

                # NOTE: To be consistent with populate_all_catalog, we
                # should start off assuming that this title is owned
                # and lendable. In practice, this isn't a big deal,
                # because process_availability() will give us the
                # right answer soon enough.
                if isinstance(result, Identifier):
                    # calls work.set_presentation_ready() for us
                    coverage_provider.handle_success(result)

        for catalog_item in items_removed:
            metadata = RBDigitalRepresentationExtractor.isbn_info_to_metadata(catalog_item)

            if not metadata:
                # generate a CoverageFailure to let the system know to revisit this book
                # TODO:  if did not create a Work, but have a CoverageFailure for the isbn,
                # check that re-processing that coverage would generate the work.
                # e = "Could not extract metadata from RBDigital data: %r" % catalog_item
                # make_note = CoverageFailure(identifier, e, data_source=self.data_source, transient=True)
                continue

            # convert IdentifierData into Identifier, if can
            identifier, made_new = metadata.primary_identifier.load(_db=self._db)
            if identifier and not made_new:
                # Don't delete works from the database.  Set them to "not ours anymore".
                # TODO: This was broken but it didn't cause any test failures,
                # which means it needs a test.
                for pool in identifier.licensed_through:
                    if pool.licenses_owned > 0:
                        if pool.presentation_edition:
                            self.log.warning("Removing %s (%s) from circulation",
                                          pool.presentation_edition.title, pool.presentation_edition.author)
                        else:
                            self.log.warning(
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

        :param mediatype: Facet to limit results by media type.  Options are: "eAudio", "eBook".
        :param genres: The books found lie at intersection of genres passed.
        :param audience: Facet to limit results by target age group.  Options include (there may be more): "adult",
            "beginning-reader", "childrens", "young-adult".
        :param availability: Facet to limit results by copies left.  Options are "available", "unavailable", or None
        :param author: Full name to search on.
        :param title: Book title to search on.
        :param page_index: Used for paginated result sets.  Zero-based.
        :param verbosity: "basic" returns smaller number of response json lines than "complete", etc..

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

    def __init__(self, fulfill_part_url, request_fulfillment, *args, **kwargs):
        # Grab properties used to support proxy fulfillment.
        self.fulfillment_proxy = kwargs.pop('fulfillment_proxy', None)

        super(RBFulfillmentInfo, self).__init__(*args, **kwargs)
        self.fulfill_part_url = fulfill_part_url
        self.request_fulfillment = request_fulfillment

    def fulfill_part(self, part):
        """Fulfill a specific part of this book.

        This will navigate the access document and find a link to
        the actual MP3 file so that a client doesn't know how to
        parse access documents.

        :return: A FulfillmentInfo if the part could be fulfilled;
            a ProblemDetail otherwise.
        """

        if self.content_type != Representation.AUDIOBOOK_MANIFEST_MEDIA_TYPE:
            raise CannotPartiallyFulfill(
                _("This work does not support partial fulfillment.")
            )

        try:
            part = int(part)
        except ValueError as e:
            raise CannotPartiallyFulfill(
                _('"%(part)s" is not a valid part number', part=part),
            )

        order = self.manifest.readingOrder
        if part < 0 or len(order) <= part:
            raise CannotPartiallyFulfill(
                _("Could not locate part number %(part)s", part=part),
            )
        part_url = order[part]['href']
        content_type, content_link, content_expires = (
            self.fetch_access_document(part_url)
        )
        return FulfillmentInfo(
            self.collection_id, self.data_source_name,
            self.identifier_type, self.identifier, content_link,
            content_type, None, content_expires
        )

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
            # We have an audiobook. Convert it from the
            # proprietary format to the standard format.
            self.manifest = AudiobookManifest(
                self.key, self.fulfill_part_url
            )
            # An upstream caller knows whether we need a proxied manifest
            # and, if so, how to structure its URLs, so we'll defer to
            # them when instructed.
            fulfillment_proxy = self.fulfillment_proxy
            if self.fulfillment_proxy and fulfillment_proxy.use_proxy_links:
                self._content = fulfillment_proxy.proxied_manifest(self.manifest)
            else:
                self._content = str(self.manifest)
        else:
            # We have some other kind of file. The download link
            # points to an access document for that file.
            self._content_type, self._content_link, self._content_expires = (
                self.fetch_access_document(individual_download_url)
            )

    def fetch_access_document(self, url):
        """Retrieve an access document from RBdigital and process it.

        An access document is a small JSON document containing a link
        to the URL we actually want to deliver.
        """
        access_document = self.request_fulfillment(url)
        return self.process_access_document(access_document)

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
        expires = utc_now() + datetime.timedelta(minutes=14)
        return content_type, content_link, expires

class MockRBDigitalAPI(RBDigitalAPI):

    @classmethod
    def mock_collection(self, _db):
        library = DatabaseTest.make_default_library(_db)
        collection, ignore = get_one_or_create(
            _db, Collection,
            name="Test RBDigital Collection",
            create_method_kwargs=dict(
                external_account_id='library_id_123',
            )
        )
        integration = collection.create_external_integration(
            protocol=ExternalIntegration.RB_DIGITAL
        )
        integration.password = 'abcdef123hijklm'
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
                published = strptime_utc(
                    book['publicationDate'][:10], cls.DATE_FORMAT
                )
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

            trusted_weight = Classification.TRUSTED_DISTRIBUTOR_WEIGHT
            subjects = []
            if 'genres' in book:
                # example: "FICTION / Humorous / General"
                genres = book['genres']
                subject = SubjectData(
                    type=Subject.BISAC, identifier=None, name=genres,
                    weight=trusted_weight,
                )
                subjects.append(subject)

            if 'primaryGenre' in book:
                # example: "humorous-fiction,mystery,womens-fiction"
                genres = book['primaryGenre']
                for genre in genres.split(","):
                    subject = SubjectData(
                        type=Subject.RBDIGITAL, identifier=genre.strip(),
                        weight=trusted_weight,
                    )
                    subjects.append(subject)

            # audience options are: adult, beginning-reader, childrens, young-adult
            # NOTE: In RBDigital metadata, audience can be set to "Adult" while publisher is "HarperTeen".
            audience = book.get('audience', None)
            if audience:
                subject = SubjectData(
                    type=Subject.RBDIGITAL_AUDIENCE,
                    identifier=audience.strip().lower(),
                    weight=trusted_weight
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
                cls.log.warning("Unfamiliar format: %s", metadata.medium)

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
            return self.failure(identifier, str(error))
        except IOError as error:
            return self.failure(identifier, str(error))

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
        if not isinstance(api_class, RBDigitalAPI):
            api_class = api_class(_db, collection, **api_class_kwargs)
        self.api = api_class

    def run_once(self, progress):
        """Find books in the RBdigital collection that changed recently.

        :param progress: A TimestampData, ignored.
        :return: A TimestampData describing what was accomplished.
        """
        items_transmitted, items_created = self.invoke()
        self._db.commit()
        achievements = (
            "Records received from vendor: %d. Records written to database: %d" % (
                items_transmitted, items_created
            )
        )
        return TimestampData(achievements=achievements)

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
    DEFAULT_START_TIME = datetime_utc(1970, 1, 1)
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

    def run_once(self, progress):
        """Update the availability information of all titles in the
        RBdigital collection.

        :param progress: A TimestampData, ignored.
        :return: A TimestampData describing what was accomplished.
        """
        ebook_count = self.process_availability(media_type='eBook')
        eaudio_count = self.process_availability(media_type='eAudio')

        message = "Ebooks processed: %d. Audiobooks processed: %d." % (
            ebook_count, eaudio_count
        )
        return TimestampData(achievements=message)

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

    # RBdigital audiobook manifests contain links to JSON documents
    # that contain links to MP3 files. This is a media type we
    # invented for these hypermedia documents, so that a client
    # examining a manifest can distinguish them from random JSON
    # files.
    #
    # Internally to the circulation manager, these documents can be processed
    # with RBFulfillmentInfo.process_access_document.
    INTERMEDIATE_LINK_MEDIA_TYPE = "vnd.librarysimplified/rbdigital-access-document+json"

    def __init__(self, content_dict, fulfill_part_url, **kwargs):
        """Create an audiobook manifest from the information provided
        by RBdigital.

        :param content_dict: A dictionary of data from RBdigital.
        :param fulfill_part_url: A function that takes a part number
            and returns a URL for fulfilling that part number on this
            circulation manager.
        """

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
        for part_number, file_data in enumerate(self.raw.get('files', [])):
            proxy_url = fulfill_part_url(part_number)
            self.import_spine(file_data, proxy_url)

        # Links.
        download_url = self.raw.get('downloadUrl')
        if download_url:
            self.add_link(
                download_url, 'alternate',
                type=Representation.guess_url_media_type_from_path(download_url)
            )

        cover = self.best_cover(self.raw.get('images', []))
        if cover:
            self.add_link(
                cover, "cover", type=Representation.guess_url_media_type_from_path(cover)
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

    def import_spine(self, file_data, proxy_url):
        """Import an RBdigital spine item as a Web Publication Manifest
        spine item.

        :param file_data: A dictionary of information about this spine
            item, obtained from RBdigital.

        :param proxy_url: A URL generated by the circulation manager
            (as opposed to being generated by RBdigital) for fulfilling this
            spine item as an audio file (as opposed to a JSON document that
            links to an audio file).
        """
        href = file_data.get('downloadUrl')
        title = file_data.get('display')

        filename = file_data.get('filename')
        type = self.INTERMEDIATE_LINK_MEDIA_TYPE

        extra = {}
        extra['proxy_link'] = dict(
            href=proxy_url,
        )

        for k, v, transform in (
                ('id', 'rbdigital:id', str),
                ('size', 'schema:contentSize', lambda x: x),
                ('minutes', 'duration', lambda x: x*60),
        ):
            if k in file_data:
                extra[v] = transform(file_data[k])
        self.add_reading_order(href, type, title, **extra)


class RBDProxyException(Exception):
    pass


class RBDigitalFulfillmentProxy(object):

    def __init__(self, patron, api, for_part=None):
        self.api = api
        self.patron = patron
        self.part = for_part

    @property
    def use_proxy_links(self):
        # If no `part` was specified, then we're returning a full manifest
        # and should rewrite the links to use the proxy service.
        return self.part is None

    @classmethod
    def proxy(cls, _db, bearer, url, api_class=None):
        # This method supports retrieval of resources that (a) require
        # a patron bearer token for fulfillment and (b) cannot be
        # fulfilled in a request authenticated by the usual patron
        # credentials.
        #
        # The overall flow is as follows, given a URL and a bearer token:
        # - Look up the `Credential` for the bearer token. If it does not
        #   exist or is expired, then return 403 Forbidden.
        # - We use the credential's `Collection` to create an instance of
        #   `RBDigitalAPI`, which we use to fulfill the request.

        api_class = api_class or RBDigitalAPI

        if not url:
            raise RBDProxyException(dict(status=400, message="No proxy URL was supplied."))

        # If we the bearer token is cached and unexpired, then we'll allow it.
        credential_type = api_class.CREDENTIAL_TYPES[api_class.BEARER_TOKEN_PROPERTY]['label']
        data_source = DataSource.lookup(_db, DataSource.RB_DIGITAL)
        credential = api_class.get_credential_by_token(_db, data_source, credential_type, bearer)
        if not credential:
            raise RBDProxyException(dict(status=403, message="Token not found or expired."))

        api = api_class(_db, credential.collection)
        # We don't want someone who sniffed this bearer token to be able
        # to generate another one, which could cause DOS to patron.
        endpoint = cls._add_api_base_url(api, url)
        response = api.patron_fulfillment_request(credential.patron, endpoint, reauthorize=False)
        return Response(
            response=response.content,
            status=response.status_code,
            headers=list(response.headers.items())
        )

    # The `_remove_api_base_url` and `_add_api_base_url` methods are used
    # in the construction and fulfillment of proxy URLs, respectively. They
    # are used to increase security in two ways:
    # - There is not enough context in the proxy URLs to fullfill content
    #   outside of this system.
    # - If an arbitrary URL is submitted to the service, it will have the
    #   API base URL prepended to it, rendering it useless, in practice.
    @staticmethod
    def _remove_api_base_url(api, url):
        # Strip off the API's base URL, if present. Otherwise, do nothing.
        prefix = api.PRODUCTION_BASE_URL
        prefix_matches = url.startswith(prefix)
        if prefix_matches:
            url = url[len(prefix):]
        return url, prefix_matches

    @staticmethod
    def _add_api_base_url(api, url):
        # Add the API's base URL to this one, no matter what.
        return "{}{}".format(api.PRODUCTION_BASE_URL, url)

    def make_request(self, url):
        return self.api.patron_fulfillment_request(self.patron, url)

    @staticmethod
    def _make_proxy_url(url, token):
        # Transform a fulfillment URL to its proxy form
        url_components = urllib.parse.urlsplit(url)
        new_path = '{}/rbdproxy/{}'.format(url_components.path, token)
        url = urllib.parse.urlunparse((
            url_components.scheme,
            url_components.netloc,
            new_path,
            '',
            url_components.query,
            url_components.fragment,
        ))
        return url

    def _rewrite_manifest(self, manifest, token):
        # Replace each part's base properties with those
        # from its own `proxy_link` dictionary.

        req = requests.models.PreparedRequest()

        def use_proxy(part):
            # We'll only do the replacement if the true download URL is
            # served by RBdigital and we have a proxy url.
            downloadUrl, is_api_link = self._remove_api_base_url(self.api, part['href'])
            if is_api_link and 'proxy_link' in part:
                proxy_link = part.pop('proxy_link')
                if 'href' in proxy_link:
                    proxy_url = self._make_proxy_url(proxy_link['href'], token)
                    params = {'url': downloadUrl}
                    req.prepare_url(proxy_url, params)
                    proxy_link['href'] = req.url
                part.update(proxy_link)
            return part

        data = manifest.as_dict
        data['readingOrder'] = [use_proxy(part) for part in data['readingOrder']]
        return json.dumps(data)

    def proxied_manifest(self, manifest):
        # Ensure that we have a token with enough time to allow
        # upcoming proxy requests to be completed.
        proxy_expires = (utc_now() +
                         datetime.timedelta(seconds=self.api.PROXY_BEARER_GRACE_PERIOD))
        credential = self.api._patron_credential(self.api.BEARER_TOKEN_PROPERTY, self.patron)
        token = credential.credential if credential else None
        if not token or credential.expires < proxy_expires:
            token = self.api.reauthorize_patron_bearer_token(self.patron)
            if not token:
                raise CirculationException("Unable to refresh patron bearer token.")

        # Transform manifest links for proxying
        manifest = self._rewrite_manifest(manifest, token)
        return manifest
