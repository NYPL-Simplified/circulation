from nose.tools import set_trace
import datetime
import dateutil
import json
import pytz
import re
import requests
import flask
import urlparse
from flask_babel import lazy_gettext as _

from sqlalchemy.orm import contains_eager

from circulation import (
    DeliveryMechanismInfo,
    LoanInfo,
    HoldInfo,
    FulfillmentInfo,
    BaseCirculationAPI,
)
from selftest import (
    HasSelfTests,
    SelfTestResult,
)
from core.overdrive import (
    OverdriveAPI as BaseOverdriveAPI,
    OverdriveRepresentationExtractor,
    OverdriveBibliographicCoverageProvider,
    MockOverdriveAPI as BaseMockOverdriveAPI,
)

from core.model import (
    CirculationEvent,
    Collection,
    Credential,
    DataSource,
    DeliveryMechanism,
    Edition,
    ExternalIntegration,
    Hold,
    Identifier,
    LicensePool,
    Loan,
    MediaTypes,
    Representation,
    Session,
)

from core.monitor import (
    CollectionMonitor,
    IdentifierSweepMonitor,
    TimelineMonitor,
)
from core.util.http import HTTP
from core.metadata_layer import ReplacementPolicy
from core.scripts import Script

from circulation_exceptions import *
from core.analytics import Analytics

class OverdriveAPI(BaseOverdriveAPI, BaseCirculationAPI, HasSelfTests):

    NAME = ExternalIntegration.OVERDRIVE
    DESCRIPTION = _("Integrate an Overdrive collection. For an Overdrive Advantage collection, select the consortium's Overdrive collection as the parent.")
    SETTINGS = [
        { "key": Collection.EXTERNAL_ACCOUNT_ID_KEY, "label": _("Library ID"), "required": True },
        { "key": BaseOverdriveAPI.WEBSITE_ID, "label": _("Website ID"), "required": True },
        { "key": ExternalIntegration.USERNAME, "label": _("Client Key"), "required": True },
        { "key": ExternalIntegration.PASSWORD, "label": _("Client Secret"), "required": True },
        {
            "key": BaseOverdriveAPI.SERVER_NICKNAME,
            "label": _("Server family"),
            "description": _("Unless you hear otherwise from Overdrive, your integration should use their production servers."),
            "type": "select",
            "options": [
                dict(
                    label=_("Production"),
                    key=BaseOverdriveAPI.PRODUCTION_SERVERS
                ),
                dict(
                    label=_("Testing"),
                    key=BaseOverdriveAPI.TESTING_SERVERS,
                )
            ],
            "default": BaseOverdriveAPI.PRODUCTION_SERVERS,
        },
    ] + BaseCirculationAPI.SETTINGS

    LIBRARY_SETTINGS = BaseCirculationAPI.LIBRARY_SETTINGS + [
        { "key": BaseOverdriveAPI.ILS_NAME_KEY, "label": _("ILS Name"),
          "default": BaseOverdriveAPI.ILS_NAME_DEFAULT,
          "description": _("When multiple libraries share an Overdrive account, Overdrive uses a setting called 'ILS Name' to determine which ILS to check when validating a given patron."),
        },
        BaseCirculationAPI.DEFAULT_LOAN_DURATION_SETTING
    ]

    # An Overdrive Advantage collection inherits everything except the library id
    # from its parent.
    CHILD_SETTINGS = [
        { "key": Collection.EXTERNAL_ACCOUNT_ID_KEY, "label": _("Library ID"), "required": True },
    ]

    SET_DELIVERY_MECHANISM_AT = BaseCirculationAPI.FULFILL_STEP

    # Create a lookup table between common DeliveryMechanism identifiers
    # and Overdrive format types.
    epub = Representation.EPUB_MEDIA_TYPE
    pdf = Representation.PDF_MEDIA_TYPE
    adobe_drm = DeliveryMechanism.ADOBE_DRM
    no_drm = DeliveryMechanism.NO_DRM
    streaming_drm = DeliveryMechanism.STREAMING_DRM
    streaming_text = DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE
    streaming_audio = DeliveryMechanism.STREAMING_AUDIO_CONTENT_TYPE
    overdrive_audiobook_manifest = MediaTypes.OVERDRIVE_AUDIOBOOK_MANIFEST_MEDIA_TYPE
    libby_drm = DeliveryMechanism.LIBBY_DRM

    # These are not real Overdrive formats; we use them internally so
    # we can distinguish between (e.g.) using "audiobook-overdrive"
    # to get into Overdrive Read, and using it to get a link to a
    # manifest file.
    MANIFEST_INTERNAL_FORMATS = set(
        ['audiobook-overdrive-manifest', 'ebook-overdrive-manifest']
    )

    # When a request comes in for a given DeliveryMechanism, what
    # do we tell Overdrive?
    delivery_mechanism_to_internal_format = {
        (epub, no_drm): 'ebook-epub-open',
        (epub, adobe_drm): 'ebook-epub-adobe',
        (pdf, no_drm): 'ebook-pdf-open',
        (pdf, adobe_drm): 'ebook-pdf-adobe',
        (streaming_text, streaming_drm): 'ebook-overdrive',
        (streaming_audio, streaming_drm): 'audiobook-overdrive',
        (overdrive_audiobook_manifest, libby_drm): 'audiobook-overdrive-manifest'
    }

    # These formats can be delivered either as manifest files or as
    # links to websites that stream the content.
    STREAMING_FORMATS = [
        'ebook-overdrive',
        'audiobook-overdrive',
    ]

    # Once you choose a non-streaming format you're locked into it and can't
    # use other formats.
    LOCK_IN_FORMATS = [
        x for x in BaseOverdriveAPI.FORMATS
        if x not in STREAMING_FORMATS
        and x not in MANIFEST_INTERNAL_FORMATS
    ]

    # TODO: This is a terrible choice but this URL should never be
    # displayed to a patron, so it doesn't matter much.
    DEFAULT_ERROR_URL = "http://librarysimplified.org/"

    # Map Overdrive's error messages to standard circulation manager
    # exceptions.
    ERROR_MESSAGE_TO_EXCEPTION = {
        "PatronHasExceededCheckoutLimit": PatronLoanLimitReached,
        "PatronHasExceededCheckoutLimit_ForCPC": PatronLoanLimitReached,
    }

    def __init__(self, _db, collection):
        super(OverdriveAPI, self).__init__(_db, collection)
        self.overdrive_bibliographic_coverage_provider = (
            OverdriveBibliographicCoverageProvider(
                collection, api_class=self
            )
        )

    def external_integration(self, _db):
        return self.collection.external_integration

    def _run_self_tests(self, _db):
        result = self.run_test(
            "Checking global Client Authentication privileges",
            self.check_creds, force_refresh=True
        )
        yield result
        if not result.success:
            # There is no point in running the other tests if we
            # can't even get a token.
            return

        def _count_advantage():
            """Count the Overdrive Advantage accounts"""
            accounts = list(self.get_advantage_accounts())
            return "Found %d Overdrive Advantage account(s)." % len(accounts)
        yield self.run_test(
            "Looking up Overdrive Advantage accounts",
            _count_advantage
        )

        def _count_books():
            """Count the titles in the collection."""
            url = self._all_products_link
            status, headers, body = self.get(url, {})
            body = json.loads(body)
            return "%d item(s) in collection" % body['totalItems']
        yield self.run_test(
            "Counting size of collection", _count_books
        )

        default_patrons = []
        for result in self.default_patrons(self.collection):
            if isinstance(result, SelfTestResult):
                yield result
                continue
            library, patron, pin = result
            task = "Checking Patron Authentication privileges, using test patron for library %s" % library.name
            yield self.run_test(
                task, self.get_patron_credential, patron, pin
            )

    def patron_request(self, patron, pin, url, extra_headers={}, data=None,
                       exception_on_401=False, method=None):
        """Make an HTTP request on behalf of a patron.

        The results are never cached.
        """
        patron_credential = self.get_patron_credential(patron, pin)
        headers = dict(Authorization="Bearer %s" % patron_credential.credential)
        headers.update(extra_headers)
        if method and method.lower() in ('get', 'post', 'put', 'delete'):
            method = method.lower()
        else:
            if data:
                method = 'post'
            else:
                method = 'get'
        url = self.endpoint(url)
        response = HTTP.request_with_timeout(
            method, url, headers=headers, data=data
        )
        if response.status_code == 401:
            if exception_on_401:
                # This is our second try. Give up.
                raise IntegrationException(
                    "Something's wrong with the patron OAuth Bearer Token!"
                )
            else:
                # Refresh the token and try again.
                self.refresh_patron_access_token(
                    patron_credential, patron, pin)
                return self.patron_request(
                    patron, pin, url, extra_headers, data, True)
        else:
            # This is commented out because it may expose patron
            # information.
            #
            # self.log.debug("%s: %s", url, response.status_code)
            return response

    def get_patron_credential(self, patron, pin):
        """Create an OAuth token for the given patron."""
        def refresh(credential):
            return self.refresh_patron_access_token(
                credential, patron, pin)
        return Credential.lookup(
            self._db, DataSource.OVERDRIVE, "OAuth Token", patron, refresh,
            collection=self.collection
        )

    def scope_string(self, library):
        """Create the Overdrive scope string for the given library.

        This is used when setting up Patron Authentication, and when
        generating the X-Overdrive-Scope header used by SimplyE to set up
        its own Patron Authentication.
        """
        return "websiteid:%s authorizationname:%s" % (
            self.website_id, self.ils_name(library)
        )

    def refresh_patron_access_token(self, credential, patron, pin):
        """Request an OAuth bearer token that allows us to act on
        behalf of a specific patron.

        Documentation: https://developer.overdrive.com/apis/patron-auth
        """
        payload = dict(
            grant_type="password",
            username=patron.authorization_identifier,
            scope=self.scope_string(patron.library)
        )
        if pin:
            # A PIN was provided.
            payload['password'] = pin
        else:
            # No PIN was provided. Depending on the library,
            # this might be fine. If it's not fine, Overdrive will
            # refuse to issue a token.
            payload['password_required'] = 'false'
            payload['password'] = '[ignore]'
        response = self.token_post(self.PATRON_TOKEN_ENDPOINT, payload)
        if response.status_code == 200:
            self._update_credential(credential, response.json())
        elif response.status_code == 400:
            response = response.json()
            message = response['error']
            error = response.get('error_description')
            if error:
                message += '/' + error
            diagnostic = None
            debug = message
            if error == 'Requested record not found':
                debug = "The patron failed Overdrive's cross-check against the library's ILS."
            raise PatronAuthorizationFailedException(message, debug)
        return credential

    def checkout(self, patron, pin, licensepool, internal_format):
        """Check out a book on behalf of a patron.

        :param patron: a Patron object for the patron who wants
            to check out the book.

        :param pin: The patron's alleged password.

        :param licensepool: Identifier of the book to be checked out is
            attached to this licensepool.

        :param internal_format: Represents the patron's desired book format.

        :return: a LoanInfo object.
        """

        identifier = licensepool.identifier
        overdrive_id=identifier.identifier
        headers = {"Content-Type": "application/json"}
        payload = dict(fields=[dict(name="reserveId", value=overdrive_id)])
        payload = json.dumps(payload)

        response = self.patron_request(
            patron, pin, self.CHECKOUTS_ENDPOINT, extra_headers=headers,
            data=payload
        )
        data = response.json()
        if response.status_code == 400:
            return self._process_checkout_error(patron, pin, licensepool, data)
        else:
            # Try to extract the expiration date from the response.
            expires = self.extract_expiration_date(data)

        # Create the loan info.
        loan = LoanInfo(
            licensepool.collection,
            licensepool.data_source.name,
            licensepool.identifier.type,
            licensepool.identifier.identifier,
            None,
            expires,
            None,
        )
        return loan

    def _process_checkout_error(self, patron, pin, licensepool, error):
        """Handle an error received by the API checkout endpoint.

        :param patron: The Patron who tried to check out the book.
        :param pin: The Patron's PIN; used in case follow-up
            API requests are necessary.
        :param licensepool: LicensePool for the book that was to be borrowed.
        :param error: A dictionary representing the error response, parsed as JSON.
        """
        code = "Unknown Error"
        identifier = licensepool.identifier
        if isinstance(error, dict):
            code = error.get('errorCode', code)
        if code == 'NoCopiesAvailable':
            # Clearly our info is out of date.
            self.update_licensepool(identifier.identifier)
            raise NoAvailableCopies()

        if code == 'TitleAlreadyCheckedOut':
            # Client should have used a fulfill link instead, but
            # we can handle it.
            #
            # NOTE: It's very unlikely this will happen, but it could
            # happen if the patron borrows a book through Libby and
            # then immediately borrows the same book through SimplyE.
            loan = self.get_loan(patron, pin, identifier.identifier)
            expires = self.extract_expiration_date(loan)
            return LoanInfo(
                licensepool.collection,
                licensepool.data_source.name,
                identifier.type,
                identifier.identifier,
                None,
                expires,
                None
            )

        if code in self.ERROR_MESSAGE_TO_EXCEPTION:
            exc_class = self.ERROR_MESSAGE_TO_EXCEPTION[code]
            raise exc_class()

        # All-purpose fallback
        raise CannotLoan(code)

    def checkin(self, patron, pin, licensepool):

        # Get the loan for this patron to see whether or not they
        # have a delivery mechanism recorded.
        loan = None
        loans = [l for l in patron.loans if l.license_pool == licensepool]
        if loans:
            loan = loans[0]
        if (loan and loan.fulfillment
            and loan.fulfillment.delivery_mechanism
            and loan.fulfillment.delivery_mechanism.drm_scheme
            == DeliveryMechanism.NO_DRM):
            # This patron fulfilled this loan without DRM. That means we
            # should be able to find a loanEarlyReturnURL and hit it.
            if self.perform_early_return(patron, pin, loan):
                # No need for the fallback strategy.
                return

        # Our fallback strategy is to DELETE the checkout endpoint.
        # We do this if no loan can be found, no delivery mechanism is
        # recorded, the delivery mechanism uses DRM, we are unable to
        # locate the return URL, or we encounter a problem using the
        # return URL.
        #
        # The only case where this is likely to work is when the
        # loan exists but has not been locked to a delivery mechanism.
        overdrive_id = licensepool.identifier.identifier
        url = self.endpoint(self.CHECKOUT_ENDPOINT, overdrive_id=overdrive_id)
        return self.patron_request(patron, pin, url, method='DELETE')

    def perform_early_return(self, patron, pin, loan, http_get=None):
        """Ask Overdrive for a loanEarlyReturnURL for the given loan
        and try to hit that URL.

        :param patron: A Patron
        :param pin: Authorization PIN for the patron
        :param loan: A Loan object corresponding to the title on loan.
        :param http_get: You may pass in a mock of HTTP.get_with_timeout
            for use in tests.
        """
        mechanism = loan.fulfillment.delivery_mechanism
        internal_format = self.delivery_mechanism_to_internal_format.get(
            (mechanism.content_type, mechanism.drm_scheme)
        )
        if not internal_format:
            # Something's wrong in general, but in particular we don't know
            # which fulfillment link to ask for. Bail out.
            return False

        # Ask Overdrive for a link that can be used to fulfill the book
        # (but which may also contain an early return URL).
        url, media_type = self.get_fulfillment_link(
            patron, pin, loan.license_pool.identifier.identifier,
            internal_format
        )
        # The URL comes from Overdrive, so it probably doesn't need
        # interpolation, but just in case.
        url = self.endpoint(url)

        # Make a regular, non-authenticated request to the fulfillment link.
        http_get = http_get or HTTP.get_with_timeout
        response = http_get(url, allow_redirects=False)
        location = response.headers.get('location')

        # Try to find an early return URL in the Location header
        # sent from the fulfillment request.
        early_return_url = self._extract_early_return_url(location)
        if early_return_url:
            response = http_get(early_return_url)
            if response.status_code == 200:
                return True
        return False

    @classmethod
    def _extract_early_return_url(cls, location):
        """Extract an early return URL from the URL Overdrive sends to
        fulfill a non-DRMed book.

        :param location: A URL found in a Location header.
        """
        if not location:
            return None
        parsed = urlparse.urlparse(location)
        query = urlparse.parse_qs(parsed.query)
        urls = query.get('loanEarlyReturnUrl')
        if urls:
            return urls[0]

    def fill_out_form(self, **values):
        fields = []
        for k, v in values.items():
            fields.append(dict(name=k, value=v))
        headers = {"Content-Type": "application/json; charset=utf-8"}
        return headers, json.dumps(dict(fields=fields))

    error_to_exception = {
        "TitleNotCheckedOut" : NoActiveLoan,
    }

    def raise_exception_on_error(self, data, custom_error_to_exception={}):
        if not 'errorCode' in data:
            return
        error = data['errorCode']
        message = data.get('message') or ''
        for d in custom_error_to_exception, self.error_to_exception:
            if error in d:
                raise d[error](message)

    def get_loan(self, patron, pin, overdrive_id):
        url = self.CHECKOUTS_ENDPOINT + "/" + overdrive_id.upper()
        data = self.patron_request(patron, pin, url).json()
        self.raise_exception_on_error(data)
        return data

    def get_hold(self, patron, pin, overdrive_id):
        url = self.endpoint(
            self.HOLD_ENDPOINT, product_id=overdrive_id.upper()
        )
        data = self.patron_request(patron, pin, url).json()
        self.raise_exception_on_error(data)
        return data

    def get_loans(self, patron, pin):
        """Get a JSON structure describing all of a patron's outstanding
        loans."""
        data = self.patron_request(patron, pin, self.CHECKOUTS_ENDPOINT).json()
        self.raise_exception_on_error(data)
        return data

    def fulfill(self, patron, pin, licensepool, internal_format, **kwargs):
        """Get the actual resource file to the patron.

        :param kwargs: A container for arguments to fulfill()
           which are not relevant to this vendor.

        :return: a FulfillmentInfo object.
        """
        try:
            result = self.get_fulfillment_link(
                patron, pin, licensepool.identifier.identifier, internal_format
            )
            if isinstance(result, FulfillmentInfo):
                # The fulfillment process was short-circuited, probably
                # by the creation of an OverdriveManifestFulfillmentInfo.
                return result

            url, media_type = result
            if internal_format in self.STREAMING_FORMATS:
                media_type += DeliveryMechanism.STREAMING_PROFILE
        except FormatNotAvailable, e:

            # It's possible the available formats for this book have changed and we
            # have an inaccurate delivery mechanism. Try to update the formats, but
            # reraise the error regardless.
            self.log.info("Overdrive id %s was not available as %s, getting updated formats" % (licensepool.identifier.identifier, internal_format))

            try:
                self.update_formats(licensepool)
            except Exception, e2:
                self.log.error("Could not update formats for Overdrive ID %s" % licensepool.identifier.identifier)

            raise e

        return FulfillmentInfo(
            licensepool.collection,
            licensepool.data_source.name,
            licensepool.identifier.type,
            licensepool.identifier.identifier,
            content_link=url,
            content_type=media_type,
            content=None,
            content_expires=None
        )


    def get_fulfillment_link(self, patron, pin, overdrive_id, format_type):
        """Get the link to the ACSM or manifest for an existing loan.
        """
        loan = self.get_loan(patron, pin, overdrive_id)
        if not loan:
            raise NoActiveLoan("Could not find active loan for %s" % overdrive_id)
        download_link = None
        if (not loan.get('isFormatLockedIn')
            and format_type in self.LOCK_IN_FORMATS):
            # The format is not locked in. Lock it in.
            # This will happen the first time someone tries to fulfill
            # a loan with a lock-in format (basically Adobe-gated formats)
            response = self.lock_in_format(
                patron, pin, overdrive_id, format_type)
            if response.status_code not in (201, 200):
                if response.status_code == 400:
                    message = response.json().get("message")
                    if message == "The selected format may not be available for this title.":
                        raise FormatNotAvailable("This book is not available in the format you requested.")
                else:
                    raise CannotFulfill("Could not lock in format %s" % format_type)
            response = response.json()
            try:
                download_link = self.extract_download_link(
                    response, self.DEFAULT_ERROR_URL)
            except IOError, e:
                # Get the loan fresh and see if that solves the problem.
                loan = self.get_loan(patron, pin, overdrive_id)

        # TODO: Verify that the asked-for format type is the same as the
        # one in the loan.

        if format_type and not download_link:
            download_link = self.get_download_link(
                loan, format_type, self.DEFAULT_ERROR_URL
            )
            if not download_link:
                raise CannotFulfill(
                    "No download link for %s, format %s" % (
                        overdrive_id, format_type))

        if download_link:
            if format_type in self.MANIFEST_INTERNAL_FORMATS:
                # The client must authenticate using its own
                # credentials to fulfill this URL; we can't do it.
                scope_string = self.scope_string(patron.library)
                return OverdriveManifestFulfillmentInfo(
                    self.collection, download_link,
                    overdrive_id, scope_string
                )

            return self.get_fulfillment_link_from_download_link(
                patron, pin, download_link)

        raise CannotFulfill("Cannot obtain a download link for patron[%r], overdrive_id[%s], format_type[%s].", patron, overdrive_id, format_type)

    def get_fulfillment_link_from_download_link(self, patron, pin, download_link, fulfill_url=None):
        # If this for Overdrive's streaming reader, and the link expires,
        # the patron can go back to the circulation manager fulfill url
        # again to get a new one.
        if not fulfill_url and flask.request:
            fulfill_url = flask.request.url
        else:
            fulfill_url=""
        download_link = download_link.replace("{odreadauthurl}", fulfill_url)
        download_response = self.patron_request(patron, pin, download_link)
        return self.extract_content_link(download_response.json())

    def extract_content_link(self, content_link_gateway_json):
        link = content_link_gateway_json['links']['contentlink']
        return link['href'], link['type']

    def lock_in_format(self, patron, pin, overdrive_id, format_type):

        overdrive_id = overdrive_id.upper()
        headers, document = self.fill_out_form(
            reserveId=overdrive_id, formatType=format_type)
        url = self.endpoint(
            self.FORMATS_ENDPOINT, overdrive_id=overdrive_id
        )
        return self.patron_request(patron, pin, url, headers, document)

    @classmethod
    def extract_data_from_checkout_response(cls, checkout_response_json,
                                            format_type, error_url):

        expires = cls.extract_expiration_date(checkout_response_json)
        return expires, cls.get_download_link(
            checkout_response_json, format_type, error_url)

    @classmethod
    def extract_data_from_hold_response(cls, hold_response_json):
        position = hold_response_json['holdListPosition']
        placed = cls._extract_date(hold_response_json, 'holdPlacedDate')
        return position, placed

    @classmethod
    def extract_expiration_date(cls, data):
        return cls._extract_date(data, 'expires')

    @classmethod
    def _extract_date(cls, data, field_name):
        if not isinstance(data, dict):
            return None
        if not field_name in data:
            return None
        try:
            return datetime.datetime.strptime(
                data[field_name], cls.TIME_FORMAT
            )
        except ValueError, e:
            # Wrong format
            return None

    def get_patron_information(self, patron, pin):
        data = self.patron_request(patron, pin, self.ME_ENDPOINT).json()
        self.raise_exception_on_error(data)
        return data

    def get_patron_checkouts(self, patron, pin):
        data = self.patron_request(patron, pin, self.CHECKOUTS_ENDPOINT).json()
        self.raise_exception_on_error(data)
        return data

    def get_patron_holds(self, patron, pin):
        data = self.patron_request(patron, pin, self.HOLDS_ENDPOINT).json()
        self.raise_exception_on_error(data)
        return data

    @classmethod
    def _pd(cls, d):
        """Stupid method to parse a date."""
        if not d:
            return d
        return datetime.datetime.strptime(d, cls.TIME_FORMAT)

    def patron_activity(self, patron, pin):
        try:
            loans = self.get_patron_checkouts(patron, pin)
            holds = self.get_patron_holds(patron, pin)
        except PatronAuthorizationFailedException, e:
            # This frequently happens because Overdrive performs
            # checks for blocked or expired accounts upon initial
            # authorization, where the circulation manager would let
            # the 'authorization' part succeed and block the patron's
            # access afterwards.
            #
            # It's common enough that it's hardly worth mentioning, but it
            # could theoretically be the sign of a larger problem.
            self.log.info(
                "Overdrive authentication failed, assuming no loans.",
                exc_info=e
            )
            loans = {}
            holds = {}

        for checkout in loans.get('checkouts', []):
            loan_info = self.process_checkout_data(checkout, self.collection)
            yield loan_info

        for hold in holds.get('holds', []):
            overdrive_identifier = hold['reserveId'].lower()
            start = self._pd(hold.get('holdPlacedDate'))
            end = self._pd(hold.get('holdExpires'))
            position = hold.get('holdListPosition')
            if position is not None:
                position = int(position)
            if 'checkout' in hold.get('actions', {}):
                # This patron needs to decide whether to check the
                # book out. By our reckoning, the patron's position is
                # 0, not whatever position Overdrive had for them.
                position = 0
            yield HoldInfo(
                self.collection,
                DataSource.OVERDRIVE,
                Identifier.OVERDRIVE_ID,
                overdrive_identifier,
                start_date=start,
                end_date=end,
                hold_position=position
            )

    @classmethod
    def process_checkout_data(cls, checkout, collection):
        """Convert one checkout from Overdrive's list of checkouts
        into a LoanInfo object.

        :return: A LoanInfo object if the book can be fulfilled
            by the default Library Simplified client, and None otherwise.
        """
        overdrive_identifier = checkout['reserveId'].lower()
        start = cls._pd(checkout.get('checkoutDate'))
        end = cls._pd(checkout.get('expires'))

        usable_formats = []

        # If a format is already locked in, it will be in formats.
        for format in checkout.get('formats', []):
            format_type = format.get('formatType')
            if format_type in cls.FORMATS:
                usable_formats.append(format_type)


        # If a format hasn't been selected yet, available formats are in actions.
        actions = checkout.get('actions', {})
        format_action = actions.get('format', {})
        format_fields = format_action.get('fields', [])
        for field in format_fields:
            if field.get('name', "") == "formatType":
                format_options = field.get("options", [])
                for format_type in format_options:
                    if format_type in cls.FORMATS:
                        usable_formats.append(format_type)

        if not usable_formats:
            # Either this book is not available in any format readable
            # by the default client, or the patron previously chose to
            # fulfill it in a format not readable by the default
            # client. Either way, we cannot fulfill this loan and we
            # shouldn't show it in the list.
            return None

        locked_to = None
        if len(usable_formats) == 1:
            # Either the book has been locked into a specific format,
            # or only one usable format is available. We don't know
            # which case we're looking at, but for our purposes the
            # book is locked -- unless, of course, what Overdrive
            # considers "one format" corresponds to more than one
            # format on our side.
            [overdrive_format] = usable_formats

            internal_formats = list(
                OverdriveRepresentationExtractor.internal_formats(
                    overdrive_format
                )
            )

            if len(internal_formats) == 1:
                [(media_type, drm_scheme)] = internal_formats
                # Make it clear that Overdrive will only deliver the content
                # in one specific media type.
                locked_to = DeliveryMechanismInfo(
                    content_type=media_type,
                    drm_scheme=drm_scheme
                )

        return LoanInfo(
            collection,
            DataSource.OVERDRIVE,
            Identifier.OVERDRIVE_ID,
            overdrive_identifier,
            start_date=start,
            end_date=end,
            locked_to=locked_to
        )

    def default_notification_email_address(self, patron, pin):
        """Find the email address this patron wants to use for hold
        notifications.

        :return: The email address Overdrive has on record for
           this patron's hold notifications, or None if there is
           no such address.
        """

        # We're calling the superclass implementation, but we have no
        # intention of actually using the result. This is a
        # per-library default that trashes all of its input, and
        # Overdrive has a better solution.
        trash_everything_address = super(
            OverdriveAPI, self
        ).default_notification_email_address(patron, pin)

        # Instead, we will ask _Overdrive_ if this patron has a
        # preferred email address for notifications.
        address = None
        response = self.patron_request(
            patron, pin, self.PATRON_INFORMATION_ENDPOINT
        )
        if response.status_code == 200:
            data = response.json()
            address = data.get('lastHoldEmail')

            # Great! Except, it's possible that this address is the
            # 'trash everything' address, because we _used_ to send
            # that address to Overdrive. If so, ignore it.
            if address == trash_everything_address:
                address = None
        else:
            self.log.error(
                "Unable to get patron information for %s: %s",
                patron.authorization_identifier,
                response.content
            )
        return address

    def place_hold(self, patron, pin, licensepool, notification_email_address):
        """Place a book on hold.

        :return: A HoldData object, if a hold was successfully placed,
            or the book was already on hold.
        :raise: A CirculationException explaining why no hold
            could be placed.
        """
        if not notification_email_address:
            notification_email_address = self.default_notification_email_address(
                patron, pin
            )
        overdrive_id = licensepool.identifier.identifier
        form_fields = dict(reserveId=overdrive_id)
        if notification_email_address:
            form_fields['emailAddress'] = notification_email_address
        else:
            form_fields['ignoreHoldEmail'] = True

        headers, document = self.fill_out_form(**form_fields)
        response = self.patron_request(
            patron, pin, self.HOLDS_ENDPOINT, headers,
            document
        )
        return self.process_place_hold_response(
            response, patron, pin, licensepool
        )

    def process_place_hold_response(self, response, patron, pin, licensepool):
        """Process the response to a HOLDS_ENDPOINT request.

        :return: A HoldData object, if a hold was successfully placed,
            or the book was already on hold.
        :raise: A CirculationException explaining why no hold
            could be placed.
        """
        def make_holdinfo(hold_response):
            # Create a HoldInfo object by combining data passed into
            # the enclosing method with the data from a hold response
            # (either creating a new hold or fetching an existing
            # one).
            position, start_date = self.extract_data_from_hold_response(
                hold_response
            )
            return HoldInfo(
                licensepool.collection,
                licensepool.data_source.name,
                licensepool.identifier.type,
                licensepool.identifier.identifier,
                start_date=start_date,
                end_date=None,
                hold_position=position
            )

        family = response.status_code // 100

        if family == 4:
            error = response.json()
            if not error or not 'errorCode' in error:
                raise CannotHold()
            code = error['errorCode']
            if code == 'AlreadyOnWaitList':
                # The book is already on hold, so this isn't an exceptional
                # condition. Refresh the queue info and act as though the
                # request was successful.
                hold = self.get_hold(
                    patron, pin, licensepool.identifier.identifier
                )
                return make_holdinfo(hold)
            elif code == 'NotWithinRenewalWindow':
                # The patron has this book checked out and cannot yet
                # renew their loan.
                raise CannotRenew()
            elif code == 'PatronExceededHoldLimit':
                raise PatronHoldLimitReached()
            else:
                raise CannotHold(code)
        elif family == 2:
            # The book was successfuly placed on hold. Return an
            # appropriate HoldInfo.
            data = response.json()
            return make_holdinfo(data)
        else:
            # Some other problem happened -- we don't know what.  It's
            # not a 5xx error because the HTTP client would have been
            # turned that into a RemoteIntegrationException.
            raise CannotHold()

    def release_hold(self, patron, pin, licensepool):
        """Release a patron's hold on a book.

        :raises CannotReleaseHold: If there is an error communicating
            with Overdrive, or Overdrive refuses to release the hold for
            any reason.
        """
        url = self.endpoint(
            self.HOLD_ENDPOINT,
            product_id=licensepool.identifier.identifier
        )
        response = self.patron_request(patron, pin, url, method='DELETE')
        if response.status_code // 100 == 2 or response.status_code == 404:
            return True
        if not response.content:
            raise CannotReleaseHold()
        data = response.json()
        if not 'errorCode' in data:
            raise CannotReleaseHold()
        if data['errorCode'] == 'PatronDoesntHaveTitleOnHold':
            # There was never a hold to begin with, so we're fine.
            return True
        raise CannotReleaseHold(response.content)

    def circulation_lookup(self, book):
        if isinstance(book, basestring):
            book_id = book
            circulation_link = self.endpoint(
                self.AVAILABILITY_ENDPOINT,
                collection_token=self.collection_token,
                product_id=book_id
            )
            book = dict(id=book_id)
        else:
            book_id = book['id']
            circulation_link = book['availability_link']
        return book, self.get(circulation_link, {})

    def update_formats(self, licensepool):
        """Update the format information for a single book.

        Incidentally updates the metadata, just in case Overdrive has
        changed it.
        """
        info = self.metadata_lookup(licensepool.identifier)

        metadata = OverdriveRepresentationExtractor.book_info_to_metadata(
            info, include_bibliographic=True, include_formats=True)
        if not metadata:
            # No work to be done.
            return

        edition, ignore = self._edition(licensepool)

        replace = ReplacementPolicy.from_license_source(self._db)
        metadata.apply(edition, self.collection, replace=replace)

    def update_licensepool(self, book_id):
        """Update availability information for a single book.

        If the book has never been seen before, a new LicensePool
        will be created for the book.

        The book's LicensePool will be updated with current
        circulation information. Bibliographic coverage will be
        ensured for the Overdrive Identifier, and a Work will be
        created for the LicensePool and set as presentation-ready.
        """
        # Retrieve current circulation information about this book
        try:
            book, (status_code, headers, content) = self.circulation_lookup(
                book_id
            )
        except Exception, e:
            status_code = None
            self.log.error(
                "HTTP exception communicating with Overdrive",
                exc_info=e
            )

        # TODO: If you ask for a book that you know about, and
        # Overdrive says the book doesn't exist in the collection,
        # then it's appropriate to update an existing
        # LicensePool. However we shouldn't be creating a *brand new*
        # LicensePool for a book Overdrive says isn't in the
        # collection.
        if status_code not in (200, 404):
            self.log.error(
                "Could not get availability for %s: status code %s",
                book_id, status_code
            )
            return None, None, False
        if isinstance(content, basestring):
            content = json.loads(content)
        book.update(content)

        # Update book_id now that we know we have new data.
        book_id = book['id']
        license_pool, is_new = LicensePool.for_foreign_id(
            self._db, DataSource.OVERDRIVE, Identifier.OVERDRIVE_ID, book_id,
            collection=self.collection
        )
        if is_new or not license_pool.work:
            # Either this is the first time we've seen this book or its doesn't
            # have an associated work. Make sure its identifier has bibliographic coverage.
            self.overdrive_bibliographic_coverage_provider.ensure_coverage(
                license_pool.identifier,
                force=True
            )

        return self.update_licensepool_with_book_info(
            book, license_pool, is_new
        )

    # Alias for the CirculationAPI interface
    def update_availability(self, licensepool):
        return self.update_licensepool(licensepool.identifier.identifier)

    def _edition(self, licensepool):
        """Find or create the Edition that would be used to contain
        Overdrive metadata for the given LicensePool.
        """
        return Edition.for_foreign_id(
            self._db, self.source, licensepool.identifier.type,
            licensepool.identifier.identifier
        )

    def update_licensepool_with_book_info(self, book, license_pool, is_new_pool):
        """Update a book's LicensePool with information from a JSON
        representation of its circulation info.

        Then, create an Edition and make sure it has bibliographic
        coverage. If the new Edition is the only candidate for the
        pool's presentation_edition, promote it to presentation
        status.
        """
        circulation = OverdriveRepresentationExtractor.book_info_to_circulation(
            book
        )
        license_pool, circulation_changed = circulation.apply(
            self._db, license_pool.collection
        )

        edition, is_new_edition = self._edition(license_pool)

        if is_new_pool:
            license_pool.open_access = False
            self.log.info("New Overdrive book discovered: %r", edition)
        return license_pool, is_new_pool, circulation_changed


    @classmethod
    def get_download_link(self, checkout_response, format_type, error_url):
        """Extract a download link from the given response.

        :param checkout_response: A JSON document describing a checkout-type
           response from the Overdrive API.
        :param format_type: The internal (Overdrive-facing) format type
           that should be retrieved. 'x-manifest' format types are treated
           as a variant of the 'x' format type -- Overdrive doesn't recognise
           'x-manifest' and uses 'x' for delivery of both streaming content
           and manifests.
        :param error_url: Value to interpolate for the {errorpageurl}
           URI template value. This is ignored if you're fetching a manifest;
           instead, the 'errorpageurl' variable is removed entirely.
        """
        link = None
        format = None
        available_formats = []
        if format_type in self.MANIFEST_INTERNAL_FORMATS:
            use_format_type = format_type.replace("-manifest", "")
            fetch_manifest = True
        else:
            use_format_type = format_type
            fetch_manifest = False
        for f in checkout_response.get('formats', []):
            this_type = f['formatType']
            available_formats.append(this_type)
            if this_type == use_format_type:
                format = f
                break
        if not format:
            if any(x in set(available_formats) for x in self.INCOMPATIBLE_PLATFORM_FORMATS):
                # The most likely explanation is that the patron
                # already had this book delivered to their Kindle.
                raise FulfilledOnIncompatiblePlatform(
                    "It looks like this loan was already fulfilled on another platform, most likely Amazon Kindle. We're not allowed to also send it to you as an EPUB."
                )
            else:
                # We don't know what happened -- most likely our
                # format data is bad.
                format_list = ", ".join(available_formats)
                msg = "Could not find specified format %s. Available formats: %s"
                raise NoAcceptableFormat(
                    msg % (use_format_type, ", ".join(available_formats))
                )

        return self.extract_download_link(format, error_url, fetch_manifest)

    @classmethod
    def extract_download_link(cls, format, error_url, fetch_manifest=False):
        """Extract a download link from the given format descriptor.

        :param format: A JSON document describing a specific format
           in which Overdrive makes a book available.
        :param error_url: Value to interpolate for the {errorpageurl}
           URI template value. This is ignored if you're fetching a manifest;
           instead, the 'errorpageurl' variable is removed entirely.
        :param fetch_manifest: If this is true, the download link will be
           modified to a URL that an authorized mobile client can use to fetch
           a manifest file.
        """

        format_type = format.get('formatType', '(unknown)')
        if not 'linkTemplates' in format:
            raise IOError("No linkTemplates for format %s" % format_type)
        templates = format['linkTemplates']
        if not 'downloadLink' in templates:
            raise IOError("No downloadLink for format %s" % format_type)
        download_link_data = templates['downloadLink']
        if not 'href' in download_link_data:
            raise IOError("No downloadLink href for format %s" % format_type)
        download_link = download_link_data['href']
        if download_link:
            if fetch_manifest:
                download_link = cls.make_direct_download_link(download_link)
            else:
                download_link = download_link.replace("{errorpageurl}", error_url)
            return download_link
        else:
            return None

    @classmethod
    def make_direct_download_link(cls, link):
        """Convert an Overdrive Read or Overdrive Listen link template to a
        direct-download link for the manifest.

        This means removing any templated arguments for Overdrive Read
        authentication URL and error URL; and adding a value for the
        'contentfile' argument.

        :param link: An Overdrive Read or Overdrive Listen template
            link.
        """
        # Remove any Overdrive Read authentication URL and error URL.
        for argument_name in ('odreadauthurl', 'errorpageurl'):
            argument_re = re.compile("%s={%s}&?" % (argument_name, argument_name))
            link = argument_re.sub("", link)

        # Add the contentfile=true argument.
        if '?' not in link:
            link += '?contentfile=true'
        elif link.endswith('&') or link.endswith('?'):
            link += 'contentfile=true'
        else:
            link += '&contentfile=true'
        return link

class MockOverdriveResponse(object):
    def __init__(self, status_code, headers, content):
        self.status_code = status_code
        self.headers = headers
        self.content = content

    def json(self):
        return json.loads(self.content)


class MockOverdriveAPI(BaseMockOverdriveAPI, OverdriveAPI):

    library_data = '{"id":1810,"name":"My Public Library (MA)","type":"Library","collectionToken":"1a09d9203","links":{"self":{"href":"http://api.overdrive.com/v1/libraries/1810","type":"application/vnd.overdrive.api+json"},"products":{"href":"http://api.overdrive.com/v1/collections/1a09d9203/products","type":"application/vnd.overdrive.api+json"},"dlrHomepage":{"href":"http://ebooks.nypl.org","type":"text/html"}},"formats":[{"id":"audiobook-wma","name":"OverDrive WMA Audiobook"},{"id":"ebook-pdf-adobe","name":"Adobe PDF eBook"},{"id":"ebook-mediado","name":"MediaDo eBook"},{"id":"ebook-epub-adobe","name":"Adobe EPUB eBook"},{"id":"ebook-kindle","name":"Kindle Book"},{"id":"audiobook-mp3","name":"OverDrive MP3 Audiobook"},{"id":"ebook-pdf-open","name":"Open PDF eBook"},{"id":"ebook-overdrive","name":"OverDrive Read"},{"id":"video-streaming","name":"Streaming Video"},{"id":"ebook-epub-open","name":"Open EPUB eBook"}]}'

    token_data = '{"access_token":"foo","token_type":"bearer","expires_in":3600,"scope":"LIB META AVAIL SRCH"}'

    collection_token = 'fake token'

    def patron_request(self, patron, pin, *args, **kwargs):
        response = self._make_request(*args, **kwargs)

        # Modify the record of the request to include the patron information.
        original_data = self.requests[-1]

        # The last item in the record of the request is keyword arguments.
        # Stick this information in there to minimize confusion.
        original_data[-1]['_patron'] = patron
        original_data[-1]['_pin'] = patron
        return response


class OverdriveCirculationMonitor(CollectionMonitor, TimelineMonitor):
    """Maintain LicensePools for recently changed Overdrive titles. Create
    basic Editions for any new LicensePools that show up.
    """
    SERVICE_NAME = "Overdrive Circulation Monitor"
    PROTOCOL = ExternalIntegration.OVERDRIVE
    OVERLAP = datetime.timedelta(minutes=1)

    def __init__(self, _db, collection, api_class=OverdriveAPI, analytics_class=Analytics):
        """Constructor."""
        super(OverdriveCirculationMonitor, self).__init__(_db, collection)
        self.api = api_class(_db, collection)
        self.analytics = analytics_class(_db)

    def recently_changed_ids(self, start, cutoff):
        return self.api.recently_changed_ids(start, cutoff)

    def catch_up_from(self, start, cutoff, progress):
        """Find Overdrive books that changed recently.

        :progress: A TimestampData representing the time previously
            covered by this Monitor.
        """
        overdrive_data_source = DataSource.lookup(
            self._db, DataSource.OVERDRIVE
        )

        # Ask for changes between the last time covered by the Monitor
        # and the current time.
        total_books = 0
        for book in self.recently_changed_ids(start, cutoff):
            total_books += 1
            if not total_books % 100:
                self.log.info("%s books processed", total_books)
            if not book:
                continue
            license_pool, is_new, is_changed = self.api.update_licensepool(book)
            # Log a circulation event for this work.
            if is_new:
                for library in self.collection.libraries:
                    self.analytics.collect_event(
                        library, license_pool, CirculationEvent.DISTRIBUTOR_TITLE_ADD, license_pool.last_checked
                    )

            self._db.commit()
            if self.should_stop(start, book, is_changed):
                break

        progress.achievements = "Books processed: %d." % total_books


class NewTitlesOverdriveCollectionMonitor(OverdriveCirculationMonitor):
    """Monitor the Overdrive collection for newly added titles.

    This catches any new titles that slipped through the cracks of the
    RecentOverdriveCollectionMonitor.
    """
    SERVICE_NAME = "Overdrive New Title Monitor"
    OVERLAP = datetime.timedelta(days=7)
    DEFAULT_START_TIME = OverdriveCirculationMonitor.NEVER

    def recently_changed_ids(self, start, cutoff):
        """Ignore the dates and return all IDs."""
        return self.api.all_ids()

    def should_stop(self, start, api_description, is_changed):
        if not start or start is self.NEVER:
            # This monitor has never run before. It should ask about
            # every single book.
            return False

        # We should stop if this book was added before our start time.
        date_added = api_description.get('date_added')
        if not date_added:
            # We don't know when this book was added -- shouldn't happen.
            return False

        try:
            date_added = dateutil.parser.parse(date_added)
        except ValueError, e:
            # The date format is unparseable -- shouldn't happen.
            self.log.error("Got invalid date: %s", date_added)
            return False

        # The time stored in the database is UTC, but it's stored
        # without any time zone information. Add that information so
        # we can compare it against the date we got from Overdrive.
        start = pytz.utc.localize(start)
        self.log.info(
            "Date added: %s, start time: %s, result %s",
            date_added, start, date_added < start
        )
        return date_added < start


class OverdriveCollectionReaper(IdentifierSweepMonitor):
    """Check for books that are in the local collection but have left our
    Overdrive collection.
    """
    SERVICE_NAME = "Overdrive Collection Reaper"
    PROTOCOL = ExternalIntegration.OVERDRIVE

    def __init__(self, _db, collection, api_class=OverdriveAPI):
        super(OverdriveCollectionReaper, self).__init__(_db, collection)
        self.api = api_class(_db, collection)

    def process_item(self, identifier):
        self.api.update_licensepool(identifier.identifier)


class RecentOverdriveCollectionMonitor(OverdriveCirculationMonitor):
    """Monitor recently changed books in the Overdrive collection."""

    SERVICE_NAME = "Reverse Chronological Overdrive Collection Monitor"

    # Report successful completion upon finding this number of
    # consecutive books in the Overdrive results whose LicensePools
    # haven't changed since last time. Overdrive results are not in
    # strict chronological order, but if you see 100 consecutive books
    # that haven't changed, you're probably done.
    MAXIMUM_CONSECUTIVE_UNCHANGED_BOOKS=100

    def __init__(self, *args, **kwargs):
        super(RecentOverdriveCollectionMonitor, self).__init__(*args, **kwargs)
        self.consecutive_unchanged_books = 0

    def should_stop(self, start, api_description, is_changed):
        if is_changed:
            self.consecutive_unchanged_books = 0
        else:
            self.consecutive_unchanged_books += 1
            if (self.consecutive_unchanged_books >=
                self.MAXIMUM_CONSECUTIVE_UNCHANGED_BOOKS):
                # We're supposed to stop this run after finding a
                # run of books that have not changed, and we have
                # in fact seen that many consecutive unchanged
                # books.
                self.log.info("Stopping at %d unchanged books.",
                              self.consecutive_unchanged_books)
                return True
        return False


class OverdriveFormatSweep(IdentifierSweepMonitor):
    """Check the current formats of every Overdrive book
    in our collection.
    """
    SERVICE_NAME = "Overdrive Format Sweep"
    DEFAULT_BATCH_SIZE = 25
    PROTOCOL = ExternalIntegration.OVERDRIVE

    def __init__(self, _db, collection, api_class=OverdriveAPI):
        super(OverdriveFormatSweep, self).__init__(_db, collection)
        self.api = api_class(_db, collection)

    def process_item(self, identifier):
        pools = identifier.licensed_through
        for pool in pools:
            self.api.update_formats(pool)
            # if there are multiple pools they should all have the same formats
            # so we break after processing the first one
            break


class OverdriveAdvantageAccountListScript(Script):

    def run(self):
        """Explain every Overdrive collection and, for each one, all of its
        Advantage collections.
        """
        collections = Collection.by_protocol(
            self._db, ExternalIntegration.OVERDRIVE
        )
        for collection in collections:
            self.explain_main_collection(collection)
            print

    def explain_main_collection(self, collection):
        """Explain an Overdrive collection and all of its Advantage
        collections.
        """
        api = OverdriveAPI(self._db, collection)
        print "Main Overdrive collection: %s" % collection.name
        print "\n".join(collection.explain())
        print "A few of the titles in the main collection:"
        for i, book in enumerate(api.all_ids()):
            print "", book['title']
            if i > 10:
                break
        advantage_accounts = list(api.get_advantage_accounts())
        print "%d associated Overdrive Advantage account(s)." % len(
            advantage_accounts
        )
        for advantage_collection in advantage_accounts:
            self.explain_advantage_collection(advantage_collection)
            print

    def explain_advantage_collection(self, collection):
        """Explain a single Overdrive Advantage collection."""
        parent_collection, child = collection.to_collection(self._db)
        print " Overdrive Advantage collection: %s" % child.name
        print " " + ("\n ".join(child.explain()))
        print " A few of the titles in this Advantage collection:"
        child_api = OverdriveAPI(self._db, child)
        for i, book in enumerate(child_api.all_ids()):
            print " ", book['title']
            if i > 10:
                break


class OverdriveManifestFulfillmentInfo(FulfillmentInfo):

    def __init__(self, collection, content_link, overdrive_identifier,
                 scope_string):
        """Constructor.

        Most of the arguments to the superconstructor can be assumed,
        and none of them matter all that much, since this class
        overrides the normal process by which a FulfillmentInfo becomes
        a Flask response.
        """
        super(OverdriveManifestFulfillmentInfo, self).__init__(
            collection=collection,
            data_source_name=DataSource.OVERDRIVE,
            identifier_type=Identifier.OVERDRIVE_ID,
            identifier=overdrive_identifier,
            content_link=content_link,
            content_type=None,
            content=None,
            content_expires=None,
        )
        self.scope_string = scope_string

    @property
    def as_response(self):
        headers = {
            "Location": self.content_link,
            "X-Overdrive-Scope": self.scope_string,
            "Content-Type": self.content_type or 'text/plain',
        }
        return flask.Response("", 302, headers)
