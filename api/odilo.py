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
    ExternalIntegration,
    Identifier
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

    # maps a 2-tuple (media_type, drm_mechanism) to the internal string used in Odilo API to describe that setup.
    delivery_mechanism_to_internal_format = {
        v: k for k, v in OdiloRepresentationExtractor.format_data_for_odilo_format.iteritems()
        }

    error_to_exception = {
        "TitleNotCheckedOut": NoActiveLoan,
        "patronNotFound": PatronNotFoundOnRemote,
        "ERROR_DATA_NOT_FOUND": NotFoundOnRemote,
        "LOAN_ALREADY_RESERVED": AlreadyOnHold,
        "CHECKOUT_NOT_FOUND": NotCheckedOut,
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

        response = HTTP.request_with_timeout(method, self.library_api_base_url + url, headers=headers, data=data,
                                             timeout=60)
        if response.status_code == 401:
            if exception_on_401:
                # This is our second try. Give up.
                raise Exception("Something's wrong with the patron OAuth Bearer Token!")
            else:
                # Refresh the token and try again.
                self.check_creds(True)
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
        self.refresh_creds(credential)

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
        record_id = licensepool.identifier.identifier

        # Data just as 'x-www-form-urlencoded', no JSON
        payload = dict(patronId=patron, format=internal_format)

        response = self.patron_request(
            patron, pin, self.CHECKOUT_ENDPOINT.format(recordId=record_id),
            extra_headers={'Content-Type': 'application/x-www-form-urlencoded'},
            data=payload)

        if response.content:
            response_json = response.json()
            if response.status_code == 404:
                self.raise_exception_on_error(response_json, default_exception_class=CannotLoan)
            else:
                return self.loan_info_from_odilo_checkout(licensepool.collection, response_json)

        # TODO: we need to improve this at the API and use an error code
        elif response.status_code == 400:
            raise NoAcceptableFormat('record_id: %s, format: %s' % (record_id, internal_format))

        raise CannotLoan('patron: %s, record_id: %s, format: %s' % (patron, record_id, internal_format))

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
        record_id = licensepool.identifier.identifier
        loan = self.get_checkout(patron, pin, record_id)
        url = self.CHECKIN_ENDPOINT.format(checkoutId=loan['id'], patronId=patron)

        response = self.patron_request(patron, pin, url, method='POST')
        if response.status_code == 200:
            return response

        self.raise_exception_on_error(response.json(), default_exception_class=CannotReturn)

    @classmethod
    def extract_date(cls, data, field_name):
        if field_name not in data or not data[field_name]:
            d = None
        else:
            # OdiloAPI dates are timestamps in milliseconds
            d = datetime.datetime.utcfromtimestamp(float(data[field_name]) / 1000.0)
        return d

    @classmethod
    def raise_exception_on_error(cls, data, default_exception_class=None, ignore_exception_codes=None):
        if not data or 'errors' not in data or len(data['errors']) <= 0:
            return '', ''

        error = data['errors'][0]
        error_code = error['id']
        message = ('description' in error and error['description']) or ''

        if not ignore_exception_codes or error_code not in ignore_exception_codes:
            if error_code in cls.error_to_exception:
                raise cls.error_to_exception[error_code](message)
            elif default_exception_class:
                raise default_exception_class(message)

    def get_checkout(self, patron, pin, record_id):
        patron_checkouts = self.get_patron_checkouts(patron, pin)
        for checkout in patron_checkouts:
            if checkout['recordId'] == record_id:
                return checkout

        raise NotFoundOnRemote("Could not find active loan for patron %s, record %s" % (patron, record_id))

    def get_hold(self, patron, pin, record_id):
        patron_holds = self.get_patron_holds(patron, pin)
        for hold in patron_holds:
            if hold['recordId'] == record_id and hold['status'] in ('informed', 'waiting'):
                return hold

        raise NotFoundOnRemote("Could not find active hold for patron %s, record %s" % (patron, record_id))

    def fulfill(self, patron, pin, licensepool, internal_format):
        record_id = licensepool.identifier.identifier
        content_link, content, content_type = self.get_fulfillment_link(patron, pin, record_id, internal_format)

        if not content_link and not content:
            self.log.info("Odilo record_id %s was not available as %s" % (record_id, internal_format))
        else:
            return FulfillmentInfo(
                licensepool.collection,
                DataSource.ODILO,
                Identifier.ODILO_ID,
                record_id,
                content_link=content_link,
                content=content,
                content_type=content_type,
                content_expires=None
            )

    def get_fulfillment_link(self, patron, pin, record_id, format_type):
        """Get the link corresponding to an existing checkout.
        """
        # Retrieve checkout with its download_ulr. It is necessary to generate a download token in our API
        checkout = self.get_checkout(patron, pin, record_id)
        loan_format = checkout['format']
        if format_type and loan_format and (
                        format_type == loan_format or
                        (loan_format == self.ACSM and format_type in (self.ACSM_EPUB, self.ACSM_PDF))
        ):
            if 'downloadUrl' in checkout and checkout['downloadUrl']:
                content_link = checkout['downloadUrl']
                content = None
                content_type = OdiloRepresentationExtractor.format_data_for_odilo_format[format_type]

                # Get also .acsm file
                if format_type in (self.ACSM_EPUB, self.ACSM_PDF):
                    response = self.patron_request(patron, pin, content_link)
                    if response.status_code == 200:
                        content = response.content
                    elif response.status_code == 404 and response.content:
                        self.raise_exception_on_error(response.json(), CannotFulfill)

                return content_link, content, content_type

        raise CannotFulfill("Cannot obtain a download link for patron[%r], record_id[%s], format_type[%s].", patron,
                            record_id, format_type)

    def get_patron_checkouts(self, patron, pin):
        data = self.patron_request(patron, pin, self.PATRON_CHECKOUTS_ENDPOINT.format(patronId=patron)).json()
        self.raise_exception_on_error(data)
        return data

    def get_patron_holds(self, patron, pin):
        data = self.patron_request(patron, pin, self.PATRON_HOLDS_ENDPOINT.format(patronId=patron)).json()
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
        if 'informed' == hold['status']:
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
            patron, pin, self.PLACE_HOLD_ENDPOINT.format(recordId=record_id),
            extra_headers={'Content-Type': 'application/x-www-form-urlencoded'},
            data=payload)

        data = response.json()
        if response.status_code == 200:
            return self.hold_from_odilo_hold(licensepool.collection, data)

        self.raise_exception_on_error(data, CannotHold)

    def release_hold(self, patron, pin, licensepool):
        """Release a patron's hold on a book.
        """

        record_id = licensepool.identifier.identifier
        hold = self.get_hold(patron, pin, record_id)
        url = self.RELEASE_HOLD_ENDPOINT.format(holdId=hold['id'])
        payload = json.dumps(dict(patronId=patron))

        response = self.patron_request(patron, pin, url, extra_headers={}, data=payload, method='POST')
        if response.status_code == 200:
            return True

        self.raise_exception_on_error(response.json(), default_exception_class=CannotReleaseHold,
                                      ignore_exception_codes=['HOLD_NOT_FOUND'])
        return True


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
        self.all_ids(start)
        finish_time = datetime.datetime.now()

        time_elapsed = finish_time - start_time
        self.log.info("recently_changed_ids finished in: " + str(time_elapsed))

    def all_ids(self, modification_date=None):
        """Get IDs for every book in the system, from modification date if any
        """

        retrieved = 0
        parsed = 0
        new = 0
        offset = 0
        limit = self.api.PAGE_SIZE_LIMIT

        if modification_date and isinstance(modification_date, datetime.date):
            modification_date = modification_date.strftime('%Y-%m-%d')  # Format YYYY-MM-DD

        # Retrieve first group of records
        url = self.get_url(limit, modification_date, offset)
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
            url = self.get_url(limit, modification_date, offset)
            status_code, headers, content = self.api.get(url)
            content = json.loads(content)

        if status_code >= 400:
            self.log.error('ERROR: Fail while retrieving data from remote source: HTTP ' + status_code)
            if content:
                self.log.error('ERROR response content: ' + str(content))
        else:
            self.log.info('Retrieving all ids finished ok. Retrieved %i records. New records: %i!!' % (retrieved, new))

    def get_url(self, limit, modification_date, offset):
        url = "%s?limit=%i&offset=%i" % (self.api.ALL_PRODUCTS_ENDPOINT, limit, offset)
        if modification_date:
            url = "%s&modificationDate=%s" % (url, modification_date)

        return url


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
        self.all_ids(None)
        finish_time = datetime.datetime.now()

        time_elapsed = finish_time - start_time
        self.log.info("recently_changed_ids finished in: " + str(time_elapsed))


class RecentOdiloCollectionMonitor(OdiloCirculationMonitor):
    """Monitor recently changed books in the Odilo collection."""

    SERVICE_NAME = "Odilo Collection Recent Monitor"
    INTERVAL_SECONDS = 60


class MockOdiloAPI(BaseMockOdiloAPI, OdiloAPI):
    def patron_request(self, patron, pin, *args, **kwargs):
        response = self._make_request(*args, **kwargs)

        # Modify the record of the request to include the patron information.
        original_data = self.requests[-1]

        # The last item in the record of the request is keyword arguments.
        # Stick this information in there to minimize confusion.
        original_data[-1]['_patron'] = patron
        original_data[-1]['_pin'] = patron
        return response
