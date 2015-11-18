from nose.tools import set_trace
import datetime
import json
import requests

from sqlalchemy.orm import contains_eager

from circulation import (
    LoanInfo,
    HoldInfo,
    FulfillmentInfo,
    BaseCirculationAPI,
)
from core.overdrive import (
    OverdriveAPI as BaseOverdriveAPI,
    OverdriveRepresentationExtractor,
)

from core.model import (
    CirculationEvent,
    Credential,
    DataSource,
    DeliveryMechanism,
    Edition,
    Hold,
    Identifier,
    LicensePool,
    Loan,
    Representation,
    Session,
)

from core.monitor import (
    Monitor,
    IdentifierSweepMonitor,
)

from circulation_exceptions import *

class OverdriveAPI(BaseOverdriveAPI, BaseCirculationAPI):

    SET_DELIVERY_MECHANISM_AT = BaseCirculationAPI.FULFILL_STEP

    # Create a lookup table between common DeliveryMechanism identifiers
    # and Overdrive format types.
    epub = Representation.EPUB_MEDIA_TYPE
    pdf = Representation.PDF_MEDIA_TYPE
    adobe_drm = DeliveryMechanism.ADOBE_DRM
    no_drm = DeliveryMechanism.NO_DRM

    delivery_mechanism_to_internal_format = {
        (epub, no_drm): 'ebook-epub-open',
        (epub, adobe_drm): 'ebook-epub-adobe',
        (pdf, no_drm): 'ebook-pdf-open',
        (pdf, adobe_drm): 'ebook-pdf-adobe',
    }

    # TODO: This is a terrible choice but this URL should never be
    # displayed to a patron, so it doesn't matter much.
    DEFAULT_ERROR_URL = "http://librarysimplified.org/"

    def patron_request(self, patron, pin, url, extra_headers={}, data=None,
                       exception_on_401=False, method=None):
        """Make an HTTP request on behalf of a patron.

        The results are never cached.
        """
        patron_credential = self.get_patron_credential(patron, pin)
        headers = dict(Authorization="Bearer %s" % patron_credential.credential)
        headers.update(extra_headers)
        if method and method.lower() in ('get', 'post', 'put', 'delete'):
            method = getattr(requests, method.lower())
        else:
            if data:
                method = requests.post
            else:
                method = requests.get
        response = method(url, headers=headers, data=data)
        if response.status_code == 401:
            if exception_on_401:
                # This is our second try. Give up.
                raise Exception("Something's wrong with the patron OAuth Bearer Token!")
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
            self._db, DataSource.OVERDRIVE, "OAuth Token", patron, refresh)

    def refresh_patron_access_token(self, credential, patron, pin):
        payload = dict(
            grant_type="password",
            username=patron.authorization_identifier,
            password=pin,
            scope="websiteid:%s authorizationname:%s" % (
                self.website_id, "default")
        )
        response = self.token_post(self.PATRON_TOKEN_ENDPOINT, payload)
        if response.status_code == 200:
            self._update_credential(credential, response.json())
        elif response.status_code == 400:
            response = response.json()
            raise PatronAuthorizationFailedException(
                response['error'] + "/" + response['error_description'])
        return credential

    def checkout(self, patron, pin, licensepool, internal_format):
        """Check out a book on behalf of a patron.

        :param patron_obj: a Patron object for the patron who wants
        to check out the book.

        :param patron_password: The patron's alleged password.

        :param identifier: Identifier of the book to be checked out.

        :param format_type: The patron's desired book format.

        :return: a LoanInfo object.
        """
        
        identifier = licensepool.identifier
        overdrive_id=identifier.identifier
        headers = {"Content-Type": "application/json"}
        payload = dict(fields=[dict(name="reserveId", value=overdrive_id)])
        payload = json.dumps(payload)

        response = self.patron_request(
            patron, pin, self.CHECKOUTS_ENDPOINT, extra_headers=headers,
            data=payload)

        if response.status_code == 400:
            error = response.json()
            code = error['errorCode']
            if code == 'NoCopiesAvailable':
                # Clearly our info is out of date.
                self.update_licensepool(identifier.identifier)
                raise NoAvailableCopies()
            elif code == 'TitleAlreadyCheckedOut':
                # Client should have used a fulfill link instead, but
                # we can handle it.
                loan = self.get_loan(patron, pin, identifier.identifier)
                expires = self.extract_expiration_date(loan)
                return LoanInfo(
                    licensepool.identifier.type,
                    licensepool.identifier.identifier,
                    None,
                    expires,
                    None
                )
            else:
                raise CannotLoan(code)
        else:
            # Try to extract the expiration date from the response.
            expires = self.extract_expiration_date(response.json())

        # Create the loan info. We don't know the expiration 
        loan = LoanInfo(
            licensepool.identifier.type,
            licensepool.identifier.identifier,
            None,
            expires,
            None,
        )
        return loan

    def checkin(self, patron, pin, licensepool):
        overdrive_id = licensepool.identifier.identifier
        url = self.CHECKOUT_ENDPOINT % dict(
            overdrive_id=overdrive_id)
        return self.patron_request(patron, pin, url, method='DELETE')

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
        url = self.HOLD_ENDPOINT % dict(product_id=overdrive_id.upper())
        data = self.patron_request(patron, pin, url).json()
        self.raise_exception_on_error(data)
        return data

    def get_loans(self, patron, pin):
        """Get a JSON structure describing all of a patron's outstanding
        loans."""
        data = self.patron_request(patron, pin, self.CHECKOUTS_ENDPOINT).json()
        self.raise_exception_on_error(data)
        return data

    def fulfill(self, patron, pin, licensepool, internal_format):
        url, media_type = self.get_fulfillment_link(
            patron, pin, licensepool.identifier.identifier, internal_format)
        return FulfillmentInfo(
            licensepool.identifier.type,
            licensepool.identifier.identifier,
            content_link=url,
            content_type=media_type, 
            content=None, 
            content_expires=None
        )

    def get_fulfillment_link(self, patron, pin, overdrive_id, format_type):
        """Get the link to the ACSM file corresponding to an existing loan.
        """
        loan = self.get_loan(patron, pin, overdrive_id)
        if not loan:
            raise NoActiveLoan("Could not find active loan for %s" % overdrive_id)
        if not 'formats' in loan:
            raise CannotFulfill("Loan for %s has no formats" % overdrive_id)

        if not loan['isFormatLockedIn']:
            # The format is not locked in. Lock it in.
            # This will happen the first time someone tries to fulfill
            # a loan.
            response = self.lock_in_format(
                patron, pin, overdrive_id, format_type)
            if response.status_code not in (201, 200):
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
                loan, format_type, self.DEFAULT_ERROR_URL)
            if not download_link:
                raise CannotFulfill(
                    "No download link for %s, format %s" % (
                        overdrive_id, format_type))

        if download_link:
            return self.get_fulfillment_link_from_download_link(
                patron, pin, download_link)
        else:
            return response

    def get_fulfillment_link_from_download_link(self, patron, pin, download_link):
        download_response = self.patron_request(patron, pin, download_link)
        return self.extract_content_link(download_response.json())
        
    def extract_content_link(self, content_link_gateway_json):
        link = content_link_gateway_json['links']['contentlink']
        return link['href'], link['type']

    def lock_in_format(self, patron, pin, overdrive_id, format_type):

        overdrive_id = overdrive_id.upper()
        headers, document = self.fill_out_form(
            reserveId=overdrive_id, formatType=format_type)
        url = self.FORMATS_ENDPOINT % dict(overdrive_id=overdrive_id)
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
        if not field_name in data:
            d = None
        else:
            d = datetime.datetime.strptime(
                data[field_name], cls.TIME_FORMAT)
        return d

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

    def patron_activity(self, patron, pin):

        def pd(d):
            """Stupid method to parse a date.""" 
            if not d:
                return d
            return datetime.datetime.strptime(d, self.TIME_FORMAT)

        try:
            loans = self.get_patron_checkouts(patron, pin)
            holds = self.get_patron_holds(patron, pin)
        except PatronAuthorizationFailedException, e:
            # TODO: This allows us to do account syncing
            # even when running against the test ILS, which
            # does not use barcodes recognized by Overdrive.
            self.log.error(
                "Overdrive authentication failed, assuming no loans.",
                exc_info=e
            )
            loans = {}
            holds = {}

        for checkout in loans.get('checkouts', []):
            overdrive_identifier = checkout['reserveId'].lower()
            start = pd(checkout.get('checkoutDate'))
            end = pd(checkout.get('expires'))
            yield LoanInfo(
                Identifier.OVERDRIVE_ID,
                overdrive_identifier,
                start_date=start,
                end_date=end,
                fulfillment_info=None
            )

        for hold in holds.get('holds', []):
            overdrive_identifier = hold['reserveId'].lower()
            start = pd(hold.get('holdPlacedDate'))
            end = pd(hold.get('holdExpires'))
            position = hold.get('holdListPosition')
            if position is not None:
                position = int(position)
            if position == 1 and 'checkout' in hold.get('actions', {}):
                # This patron needs to decide whether to check the
                # book out. By our reckoning, their position is 0, not 1.
                position = 0
            yield HoldInfo(
                Identifier.OVERDRIVE_ID,
                overdrive_identifier,
                start_date=start,
                end_date=end,
                hold_position=position
            )

    def place_hold(self, patron, pin, licensepool, notification_email_address):
        """Place a book on hold.

        :return: A HoldInfo object
        """
        if not notification_email_address:
            raise CannotHold("No notification email address provided.")
        overdrive_id = licensepool.identifier.identifier
        headers, document = self.fill_out_form(
            reserveId=overdrive_id, emailAddress=notification_email_address)
        response = self.patron_request(
            patron, pin, self.HOLDS_ENDPOINT, headers, 
            document)
        if response.status_code == 400:
            error = response.json()
            if not error or not 'errorCode' in error:
                raise CannotHold()
            code = error['errorCode']
            if code == 'AlreadyOnWaitList':
                # There's nothing we can do but refresh the queue info.
                hold = self.get_hold(patron, pin, overdrive_id)
                position, start_date = self.extract_data_from_hold_response(
                    hold)
                return HoldInfo(
                    licensepool.identifier.type,
                    licensepool.identifier.identifier,
                    start_date=start_date, 
                    end_date=None,
                    hold_position=position
                )
            elif code == 'NotWithinRenewalWindow':
                # The patron has this book checked out and cannot yet
                # renew their loan.
                raise CannotRenew()
            else:
                raise CannotHold(code)
        else:
            # The book was placed on hold.
            data = response.json()
            position, start_date = self.extract_data_from_hold_response(
                data)
            return HoldInfo(
                licensepool.identifier.type,
                licensepool.identifier.identifier,
                start_date=start_date,
                end_date=None,
                hold_position=position
            )

    def release_hold(self, patron, pin, licensepool):
        """Release a patron's hold on a book.

        :raises CannotReleaseHold: If there is an error communicating
        with Overdrive, or Overdrive refuses to release the hold for
        any reason.
        """
        url = self.HOLD_ENDPOINT % dict(
            product_id=licensepool.identifier.identifier)
        response = self.patron_request(patron, pin, url, method='DELETE')
        if response.status_code / 100 == 2 or response.status_code == 404:
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
       
    def update_licensepool(self, book):
        """Update availability information for a single book.

        If the book has never been seen before, a new LicensePool
        will be created for the book.

        The book's LicensePool will be updated with current
        circulation information.
        """
        # Retrieve current circulation information about this book
        # print "Update for %s" % book
        orig_book = book
        book_id = None
        if isinstance(book, basestring):
            book_id = book
            circulation_link = self.AVAILABILITY_ENDPOINT % dict(
                collection_name=self.collection_name,
                product_id=book_id
            )
            book = dict(id=book_id)
        else:
            book_id = book['id']
            circulation_link = book['availability_link']
        try:
            status_code, headers, content = self.get(circulation_link, {})
        except Exception, e:
            status_code = None
            self.log.error(
                "HTTP exception communicating with Overdrive",
                exc_info=e
            )

        if status_code != 200:
            self.log.error(
                "Could not get availability for %s: status code %s",
                book['id'], status_code
            )
            return None, None, False

        book.update(json.loads(content))
        license_pool, is_new = LicensePool.for_foreign_id(
            self._db, DataSource.OVERDRIVE, Identifier.OVERDRIVE_ID, book_id)
        return self.update_licensepool_with_book_info(
            book, license_pool, is_new
        )

    def update_licensepool_with_book_info(self, book, license_pool, is_new):
        """Update a book's LicensePool with information from a JSON
        representation of its circulation info.

        Also creates an Edition and gives it very basic bibliographic
        information (the title), if possible.
        """
        circulation = OverdriveRepresentationExtractor.book_info_to_circulation(
            book
        )
        circulation_changed = circulation.update(license_pool, is_new)

        edition, ignore = Edition.for_foreign_id(
            self._db, self.source, license_pool.identifier.type,
            license_pool.identifier.identifier)
        edition.title = edition.title or book.get('title')
        if is_new:
            license_pool.open_access = False
            self.log.info("New Overdrive book discovered: %r", edition)
        return license_pool, is_new, circulation_changed

    def _get_book_list_page(self, link, rel_to_follow='next'):
        """Process a page of inventory whose circulation we need to check.

        Returns a list of (title, id, availability_link) 3-tuples,
        plus a link to the next page of results.
        """
        # We don't cache this because it changes constantly.
        status_code, headers, content = self.get(link, {})
        try:
            data = json.loads(content)
        except Exception, e:
            self.log.error(
                "Error getting book list page: %r %r %r", 
                status_code, headers, content,
                exc_info=e
            )
            return [], None

        # Find the link to the next page of results, if any.
        next_link = OverdriveRepresentationExtractor.link(data, rel_to_follow)

        # Prepare to get availability information for all the books on
        # this page.
        availability_queue = (
            OverdriveRepresentationExtractor.availability_link_list(data))
        return availability_queue, next_link

    @classmethod
    def get_download_link(self, checkout_response, format_type, error_url):
        link = None
        format = None
        available_formats = []
        for f in checkout_response.get('formats', []):
            this_type = f['formatType']
            available_formats.append(this_type)
            if this_type == format_type:
                format = f
                break
        if not format:
            msg = "Could not find specified format %s. Available formats: %s"
            raise IOError(msg % (format_type, ", ".join(available_formats)))

        download_link = self.extract_download_link(format, error_url)

    def extract_download_link(self, format, error_url):
        format_type = format.get('formatType', '(unknown)')
        if not 'linkTemplates' in format:
            raise IOError("No linkTemplates for format %s" % format_type)
        templates = format['linkTemplates']
        if not 'downloadLink' in templates:
            raise IOError("No downloadLink for format %s" % format_type)
        download_link = templates['downloadLink']['href']
        if download_link:
            return download_link.replace("{errorpageurl}", error_url)
        else:
            return None


    def recently_changed_ids(self, start, cutoff):
        """Get IDs of books whose status has changed between the start time
        and now.
        """
        # `cutoff` is not supported by Overdrive, so we ignore it. All
        # we can do is get events between the start time and now.

        last_update_time = start-self.EVENT_DELAY
        self.log.info(
            "Asking for circulation changes since %s",
            last_update_time
        )
        params = dict(lastupdatetime=last_update_time,
                      sort="popularity:desc",
                      limit=self.PAGE_SIZE_LIMIT,
                      collection_name=self.collection_name)
        next_link = self.make_link_safe(self.EVENTS_ENDPOINT % params)
        while next_link:
            page_inventory, next_link = self._get_book_list_page(next_link)
            # We won't be sending out any events for these books yet,
            # because we don't know if anything changed, but we will
            # be putting them on the list of inventory items to
            # refresh. At that point we will send out events.
            for i in page_inventory:
                yield i


class DummyOverdriveResponse(object):
    def __init__(self, status_code, headers, content):
        self.status_code = status_code
        self.headers = headers
        self.content = content

    def json(self):
        return json.loads(self.content)

class DummyOverdriveAPI(OverdriveAPI):

    library_data = '{"id":1810,"name":"My Public Library (MA)","type":"Library","collectionToken":"1a09d9203","links":{"self":{"href":"http://api.overdrive.com/v1/libraries/1810","type":"application/vnd.overdrive.api+json"},"products":{"href":"http://api.overdrive.com/v1/collections/1a09d9203/products","type":"application/vnd.overdrive.api+json"},"dlrHomepage":{"href":"http://ebooks.nypl.org","type":"text/html"}},"formats":[{"id":"audiobook-wma","name":"OverDrive WMA Audiobook"},{"id":"ebook-pdf-adobe","name":"Adobe PDF eBook"},{"id":"ebook-mediado","name":"MediaDo eBook"},{"id":"ebook-epub-adobe","name":"Adobe EPUB eBook"},{"id":"ebook-kindle","name":"Kindle Book"},{"id":"audiobook-mp3","name":"OverDrive MP3 Audiobook"},{"id":"ebook-pdf-open","name":"Open PDF eBook"},{"id":"ebook-overdrive","name":"OverDrive Read"},{"id":"video-streaming","name":"Streaming Video"},{"id":"ebook-epub-open","name":"Open EPUB eBook"}]}'

    token_data = '{"access_token":"foo","token_type":"bearer","expires_in":3600,"scope":"LIB META AVAIL SRCH"}'

    def __init__(self, *args, **kwargs):
        super(DummyOverdriveAPI, self).__init__(*args, **kwargs)
        self.responses = []

    def queue_response(self, response_code=200, media_type="application/json",
                       other_headers=None, content=''):
        headers = {"content-type": media_type}
        if other_headers:
            for k, v in other_headers.items():
                headers[k.lower()] = v
        self.responses.append((response_code, headers, content))

    # Give canned answers to the most basic requests -- for access tokens
    # and basic library information.
    def token_post(self, *args, **kwargs):
        return DummyOverdriveResponse(200, {}, self.token_data)

    def get_library(self):
        return json.loads(self.library_data)

    def get(self, url, extra_headers, exception_on_401=False):
        return self.responses.pop()

    def patron_request(self, *args, **kwargs):
        value = self.responses.pop()
        return DummyOverdriveResponse(*value)


class OverdriveCirculationMonitor(Monitor):
    """Maintain LicensePools for Overdrive titles.

    Bibliographic data isn't inserted into new LicensePools until
    we hear from the metadata wrangler.
    """
    def __init__(self, _db, name="Overdrive Circulation Monitor",
                 interval_seconds=500,
                 maximum_consecutive_unchanged_books=None):
        super(OverdriveCirculationMonitor, self).__init__(
            _db, name, interval_seconds=interval_seconds)
        self.maximum_consecutive_unchanged_books = (
            maximum_consecutive_unchanged_books)

    def recently_changed_ids(self, start, cutoff):
        return self.api.recently_changed_ids(start, cutoff)

    def run(self):
        self.api = OverdriveAPI(self._db)
        super(OverdriveCirculationMonitor, self).run()

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
                CirculationEvent.log(
                    _db, license_pool, CirculationEvent.TITLE_ADD,
                    None, None, start=license_pool.last_checked)
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

class FullOverdriveCollectionMonitor(OverdriveCirculationMonitor):
    """Monitor every single book in the Overdrive collection.

    This tells us about books added to the Overdrive collection that
    are not found in our collection.
    """

    def __init__(self, _db, interval_seconds=3600*4):
        super(FullOverdriveCollectionMonitor, self).__init__(
            _db, "Overdrive Collection Overview", interval_seconds)

    def recently_changed_ids(self, start, cutoff):
        """Ignore the dates and return all IDs."""
        return self.api.all_ids()

class OverdriveCollectionReaper(IdentifierSweepMonitor):
    """Check for books that are in the local collection but have left our
    Overdrive collection.
    """

    def __init__(self, _db, interval_seconds=3600*4):
        super(OverdriveCollectionReaper, self).__init__(
            _db, "Overdrive Collection Reaper", interval_seconds)

    def run(self):
        self.api = OverdriveAPI(self._db)
        super(OverdriveCollectionReaper, self).run()

    def identifier_query(self):
        return self._db.query(Identifier).join(
            Identifier.licensed_through).filter(
                Identifier.type==Identifier.OVERDRIVE_ID).options(
                    contains_eager(Identifier.licensed_through))

    def process_batch(self, identifiers):
        for i in identifiers:
            self.api.update_licensepool(i.identifier)

class RecentOverdriveCollectionMonitor(FullOverdriveCollectionMonitor):
    """Monitor recently changed books in the Overdrive collection."""

    def __init__(self, _db, interval_seconds=60,
                 maximum_consecutive_unchanged_books=100):
        super(FullOverdriveCollectionMonitor, self).__init__(
            _db, "Reverse Chronological Overdrive Collection Monitor",
            interval_seconds, maximum_consecutive_unchanged_books)

