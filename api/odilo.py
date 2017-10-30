# coding=utf-8
import datetime
import json
from flask.ext.babel import lazy_gettext as _

from circulation import (
    LoanInfo,
    HoldInfo,
    FulfillmentInfo,
    BaseCirculationAPI,
)
from core.odilo import (
    OdiloAPI as BaseOdiloAPI,
    OdiloRepresentationExtractor,
    OdiloBibliographicCoverageProvider,
    MockOdiloAPI as BaseMockOdiloAPI,
)

from core.model import (
    Credential,
    DataSource,
    DeliveryMechanism,
    ExternalIntegration,
    Identifier,
    Representation,
)

from core.monitor import (
    CollectionMonitor,
)
from core.util.http import HTTP

from circulation_exceptions import *


class OdiloAPI(BaseOdiloAPI, BaseCirculationAPI):
    NAME = ExternalIntegration.ODILO
    DESCRIPTION = _("Integrate an Odilo library collection.")
    SETTINGS = [
                   {"key": BaseOdiloAPI.LIBRARY_API_BASE_URL, "label": _("Library API base URL")},
                   {"key": ExternalIntegration.USERNAME, "label": _("Client Key")},
                   {"key": ExternalIntegration.PASSWORD, "label": _("Client Secret")},
               ] + BaseCirculationAPI.SETTINGS

    SET_DELIVERY_MECHANISM_AT = BaseCirculationAPI.FULFILL_STEP

    # Create a lookup table between common DeliveryMechanism identifiers
    # and Odilo format types.
    epub = Representation.EPUB_MEDIA_TYPE
    pdf = Representation.PDF_MEDIA_TYPE
    mp3 = Representation.MP3_MEDIA_TYPE
    mp4 = Representation.MP4_MEDIA_TYPE
    wmv = Representation.WMV_MEDIA_TYPE
    jpg = Representation.JPEG_MEDIA_TYPE
    scorm = Representation.ZIP_MEDIA_TYPE

    adobe_drm = DeliveryMechanism.ADOBE_DRM
    no_drm = DeliveryMechanism.NO_DRM
    streaming_text = DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE

    # maps a 2-tuple (media_type, drm_mechanism) to the internal string used in Odilo API to describe that setup.
    delivery_mechanism_to_internal_format = {
        (pdf, adobe_drm): 'ACSM',
        (pdf, streaming_text): 'PDF',
        (epub, streaming_text): 'EPUB',
        (mp3, no_drm): 'MP3',
        (mp4, streaming_text): 'MP4',
        (wmv, streaming_text): 'WMV',
        (jpg, no_drm): 'JPG',
        (scorm, no_drm): 'SCORM',
    }

    error_to_exception = {
        "TitleNotCheckedOut": NoActiveLoan,
    }

    def __init__(self, _db, collection):
        super(OdiloAPI, self).__init__(_db, collection)
        self.odilo_bibliographic_coverage_provider = (
            OdiloBibliographicCoverageProvider(
                collection, api_class=self
            )
        )

    def patron_request(self, patron, pin, url, extra_headers={}, data=None, exception_on_401=False, method=None):
        """Make an HTTP request on behalf of a patron.

        The results are never cached.
        """
        headers = dict(Authorization="Bearer %s" % self.token)
        headers['Content-Type'] = 'application/json'
        headers.update(extra_headers)

        if method and method.lower() in ('get', 'post', 'put', 'delete'):
            method = method.lower()
        else:
            if data:
                method = 'post'
            else:
                method = 'get'

        response = HTTP.request_with_timeout(method, url, headers=headers, data=data, timeout=60)
        if response.status_code == 401:
            if exception_on_401:
                # This is our second try. Give up.
                raise Exception("Something's wrong with the patron OAuth Bearer Token!")
            else:
                # Refresh the token and try again.
                self.get_patron_credential(patron, pin)
                return self.patron_request(patron, pin, url, extra_headers, data, True)
        else:
            return response

    def get_patron_credential(self, patron, pin):
        """Create an OAuth token for the given patron."""

        def refresh(credential):
            return self.get_patron_access_token(credential, patron, pin)

        return Credential.lookup(self._db, DataSource.ODILO, "OAuth Token", patron, refresh)

    def get_patron_access_token(self, credential, patron, pin):
        """Request an OAuth bearer token that allows us to act on
        behalf of a specific patron.
        """

        self.client_key = patron
        self.client_secret = pin
        message = self.refresh_creds(credential)

        if 'OK' == message:
            return credential
        else:
            raise PatronAuthorizationFailedException(message)

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
        record_id = licensepool.identifier.identifier

        # Data just as 'x-www-form-urlencoded', no JSON
        payload = dict(patronId=patron, format=internal_format)

        response = self.patron_request(
            patron, pin, self.CHECKOUT_ENDPOINT.replace("{recordId}", record_id),
            extra_headers={'Content-Type': 'application/x-www-form-urlencoded'},
            data=payload)

        response_json = response.json()
        if response.status_code == 404:
            if response_json and response_json['errors'] and len(response_json['errors']) > 0:
                error = response_json['errors'][0]
                if error['id'] == 'ERROR_DATA_NOT_FOUND':
                    raise NotFoundOnRemote('Record %s' % record_id)

                elif error['id'] == 'NoCopiesAvailable':
                    # Clearly our info is out of date.
                    # self.update_licensepool(identifier.identifier)
                    raise NoAvailableCopies()
                elif error['id'] == 'PatronHasExceededCheckoutLimit':
                    raise PatronLoanLimitReached()
                else:
                    raise CannotLoan(error['id'])
        else:
            return self.loan_info_from_odilo_checkout(licensepool.collection, response_json)

    def loan_info_from_odilo_checkout(self, collection, checkout):
        start_date = self.extract_date(checkout, 'startTime')
        end_date = self.extract_date(checkout, 'endTime')

        return LoanInfo(
            collection,
            DataSource.ODILO,
            Identifier.ODILO_ID,
            checkout['id'],
            start_date,
            end_date,
            checkout['downloadUrl']
        )

    def checkin(self, patron, pin, licensepool):
        checkout_id = licensepool.identifier.identifier
        url = self.CHECKIN_ENDPOINT.replace("{checkoutId}", checkout_id) + "?patronId=" + patron

        response = self.patron_request(patron, pin, url, method='POST')
        if response.status_code == 404:
            data = response.json()
            if data and data['errors'] and len(response.json()['errors']) > 0:
                error = data['errors'][0]
                if error['id'] == 'CHECKOUT_NOT_FOUND':
                    raise NotCheckedOut(error['description'])
                else:
                    raise CannotReturn(error['id'] + ': ' + error['description'])
        else:
            return response

    @classmethod
    def extract_date(cls, data, field_name):
        if field_name not in data:
            d = None
        else:
            # OdiloAPI dates are timestamps in milliseconds
            d = datetime.datetime.fromtimestamp(float(data[field_name]) / 1000.0)
        return d

    def raise_exception_on_error(self, data, custom_error_to_exception={}):
        if not data or 'errors' not in data or len(data['errors']) <= 0:
            return

        error = data['errors'][0]
        error_code = error['id']
        message = ('description' in error and error['description']) or ''
        for d in custom_error_to_exception, self.error_to_exception:
            if error in d:
                raise d[error_code](message)

    def get_loan(self, patron, pin, checkout_id):
        url = self.CHECKOUT_GET.replace("{checkoutId}", checkout_id) + '?patronId=' + patron
        response = self.patron_request(patron, pin, url)
        if response.status_code == 200 and response.content:
            data = response.json()
            self.raise_exception_on_error(data)
            return data
        else:
            return None

    def get_hold(self, patron, pin, hold_id):
        url = self.HOLD_GET.replace("{holdId}", hold_id)
        data = self.patron_request(patron, pin, url).json()
        self.raise_exception_on_error(data)
        return data

    def fulfill(self, patron, pin, licensepool, internal_format):
        checkout_id = licensepool.identifier.identifier
        content_link, content, content_type = self.get_fulfillment_link(patron, pin, checkout_id, internal_format)

        if not content_link and not content:
            self.log.info("Odilo record_id %s was not available as %s" % (checkout_id, internal_format))
        else:
            return FulfillmentInfo(
                licensepool.collection,
                DataSource.ODILO,
                Identifier.ODILO_ID,
                checkout_id,
                content_link=content_link,
                content=content,
                content_type=content_type,
                content_expires=None
            )

    def get_fulfillment_link(self, patron, pin, checkout_id, format_type):
        """Get the link corresponding to an existing loan.
        """
        # Retrieve loan with its download_ulr. It is necessary to generate a download token in our API
        loan = self.get_loan(patron, pin, checkout_id)
        if not loan:
            raise NoActiveLoan("Could not find active loan for %s" % checkout_id)

        if format_type and loan['format'] and format_type == loan['format']:
            if 'downloadUrl' in loan and loan['downloadUrl']:
                content_link = loan['downloadUrl']
                content = None
                content_type = OdiloRepresentationExtractor.format_data_for_odilo_format[format_type]

                # Get also .acsm file
                if 'ACSM' == format_type:
                    response = self.patron_request(patron, pin, content_link)
                    if response.status_code == 200:
                        content = response.content
                    elif response.status_code == 404 and response.content:
                        data = response.json()
                        if data and 'errors' in data and len(data['errors']) > 0:
                            error = data['errors'][0]
                            raise CannotFulfill(error['id'] + ': ' + error['description'])

                return content_link, content, content_type

        raise CannotFulfill("Cannot obtain a download link for patron[%r], checkout_id[%s], format_type[%s].", patron,
                            checkout_id, format_type)

    def get_patron_checkouts(self, patron, pin):
        data = self.patron_request(patron, pin, self.PATRON_CHECKOUTS_ENDPOINT.replace("{patronId}", patron)).json()
        self.raise_exception_on_error(data)
        return data

    def get_patron_holds(self, patron, pin):
        data = self.patron_request(patron, pin, self.PATRON_HOLDS_ENDPOINT.replace("{patronId}", patron)).json()
        self.raise_exception_on_error(data)
        return data

    def patron_activity(self, patron, pin):
        odilo_checkouts = self.get_patron_checkouts(patron, pin)
        odilo_holds = self.get_patron_holds(patron, pin)

        loans_info = []
        holds_info = []

        collection = self.collection

        for checkout in odilo_checkouts:
            loan_info = self.loan_info_from_odilo_checkout(collection, checkout)
            loans_info.append(loan_info)

        for hold in odilo_holds:
            hold_info = self.hold_from_odilo_hold(collection, hold)
            holds_info.append(hold_info)

        return loans_info + holds_info

    def hold_from_odilo_hold(self, collection, hold):
        start = self.extract_date(hold, 'startTime')
        # end_date: The estimated date the title will be available for the patron to borrow.
        end = self.extract_date(hold, 'notifiedTime')
        position = hold.get('holdQueuePosition')

        if position is not None:
            position = int(position)

        # Patron already notified to borrow the title
        if 'A' == hold['status']:
            position = 0

        return HoldInfo(
            collection,
            DataSource.ODILO,
            Identifier.ODILO_ID,
            hold['id'],
            start_date=start,
            end_date=end,
            hold_position=position
        )

    def place_hold(self, patron, pin, licensepool, notification_email_address):
        """Place a book on hold.

        :return: A HoldInfo object
        """

        record_id = licensepool.identifier.identifier

        # Data just as 'x-www-form-urlencoded', no JSON
        payload = dict(patronId=patron)

        response = self.patron_request(
            patron, pin, self.PLACE_HOLD_ENDPOINT.replace("{recordId}", record_id),
            extra_headers={'Content-Type': 'application/x-www-form-urlencoded'},
            data=payload)

        data = response.json()
        if response.status_code == 200:
            return self.hold_from_odilo_hold(licensepool.collection, data)

        if response.status_code in (403, 404):
            if not data or 'errors' not in data or len(data['errors']) <= 0:
                raise CannotHold()

            error = data['errors'][0]
            code = error['id']
            description = ''
            if 'description' in error:
                description = error['description']

            if code == 'LOAN_ALREADY_RESERVED':
                raise AlreadyOnHold(description)
            elif code == 'ERROR_DATA_NOT_FOUND':
                raise NotFoundOnRemote(code + ', record: ' + record_id)
            elif code == 'PatronExceededHoldLimit':
                raise PatronHoldLimitReached(description)

        raise CannotHold()

    def release_hold(self, patron, pin, licensepool):
        """Release a patron's hold on a book.
        """
        hold_id = licensepool.identifier.identifier
        url = self.RELEASE_HOLD_ENDPOINT.replace("{holdId}", hold_id)
        payload = json.dumps(dict(patronId=patron))

        response = self.patron_request(patron, pin, url, extra_headers={}, data=payload, method='POST')

        if response.status_code == 200:
            return True

        if not response.content:
            raise CannotReleaseHold()

        data = response.json()
        if 'errors' not in data or len(data['errors']) <= 0:
            raise CannotReleaseHold()

        error = data['errors'][0]
        code = error['id']
        description = error['description']

        if code == 'HOLD_NOT_FOUND':
            return True

        raise CannotReleaseHold(description)


class OdiloCirculationMonitor(CollectionMonitor):
    """Maintain LicensePools for recently changed Odilo titles
    """
    SERVICE_NAME = "Odilo Circulation Monitor"
    INTERVAL_SECONDS = 500
    PROTOCOL = ExternalIntegration.ODILO

    def __init__(self, _db, collection, api_class=OdiloAPI):
        """Constructor."""
        super(OdiloCirculationMonitor, self).__init__(_db, collection)
        self.api = api_class(_db, collection)

    def run_once(self, start, cutoff):
        self.log.info("Starting recently_changed_ids, start: " + str(start) + ", cutoff: " + str(cutoff))

        start_time = datetime.datetime.now()
        self.recently_changed_ids(start)
        finish_time = datetime.datetime.now()

        time_elapsed = finish_time - start_time
        self.log.info("recently_changed_ids finished in: " + str(time_elapsed))

    def recently_changed_ids(self, start):
        modification_date = None
        if start:
            if isinstance(start, datetime.date):
                modification_date = start.strftime('%Y-%m-%d')  # Format YYYY-MM-DD
            elif isinstance(start, basestring):
                modification_date = start

        self.all_ids(modification_date)

    def all_ids(self, modication_date=None):
        """Get IDs for every book in the system, from modification date if any
        """

        retrieved = 0
        parsed = 0
        new = 0
        offset = 0
        limit = self.api.PAGE_SIZE_LIMIT

        url = "%s?limit=%i&offset=%i" % (self.api.ALL_PRODUCTS_ENDPOINT, limit, offset)
        if modication_date:
            url = "%s&modificationDate=%s" % (url, modication_date)

        # Retrieve first group of records
        status_code, headers, content = self.api.get(url)
        content = json.loads(content)

        # Retrieve Odilo record in groups
        while status_code == 200 and len(content) > 0:
            offset += limit
            retrieved += len(content)
            self.log.info('Retrieved %i records' % retrieved)

            # Process a bunch of records retrieved
            for record in content:
                record_id = record['id']
                self.log.info('Processing record %i/%i: %s' % (parsed, retrieved, record_id))
                identifier, is_new = self.api.odilo_bibliographic_coverage_provider.process_item(
                    record_id, record
                )

                if is_new:
                    new += 1

                parsed += 1

            # Persist each bunch of retrieved records
            self._db.commit()

            # Retrieve next group of records
            url = "%s?limit=%i&offset=%i" % (self.api.ALL_PRODUCTS_ENDPOINT, limit, offset)
            if modication_date:
                url = "%s&modificationDate=%s" % (url, modication_date)

            status_code, headers, content = self.api.get(url)
            content = json.loads(content)

        if status_code >= 400:
            self.log.error('ERROR: Fail while retrieving data from remote source: HTTP ' + status_code)
            if content:
                self.log.error('ERROR response content: ' + str(content))
        else:
            self.log.info('Retrieving all ids finished ok. Retrieved %i records!!' % retrieved)
            self.log.info('New records: %i' % new)


class FullOdiloCollectionMonitor(OdiloCirculationMonitor):
    """Monitor every single book in the Odilo collection.

    This tells us about books added to the Odilo collection that
    are not found in our collection.
    """
    SERVICE_NAME = "Odilo Full Collection Overview"
    INTERVAL_SECONDS = 3600 * 4

    def run_once(self, start=None, cutoff=None):
        """Ignore the dates and return all IDs."""
        self.log.info("Starting recently_changed_ids, start: " + str(start) + ", cutoff: " + str(cutoff))

        start_time = datetime.datetime.now()
        self.recently_changed_ids(None)
        finish_time = datetime.datetime.now()

        time_elapsed = finish_time - start_time
        self.log.info("recently_changed_ids finished in: " + str(time_elapsed))


class RecentOdiloCollectionMonitor(OdiloCirculationMonitor):
    """Monitor recently changed books in the Odilo collection."""

    SERVICE_NAME = "Odilo Collection Recent Monitor"
    INTERVAL_SECONDS = 60


class MockOdiloAPI(BaseMockOdiloAPI, OdiloAPI):
    def patron_request(self, patron, pin, *args, **kwargs):
        pass
