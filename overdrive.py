from nose.tools import set_trace
import datetime
import json

from core.overdrive import (
    OverdriveAPI as BaseOverdriveAPI,
    OverdriveRepresentationExtractor,
)

from core.model import (
    CirculationEvent,
    Credential,
    DataSource,
    Edition,
    Identifier,
    LicensePool,
    Loan,
    Session,
)

from core.monitor import Monitor

from circulation_exceptions import (
    NoAvailableCopies,
)

class OverdriveAPI(BaseOverdriveAPI):

    def patron_request(self, patron, pin, url, extra_headers={}, data=None,
                       exception_on_401=False):
        """Make an HTTP request on behalf of a patron.

        The results are never cached.
        """
        patron_credential = self.get_patron_credential(patron, pin)
        headers = dict(Authorization="Bearer %s" % patron_credential.credential)
        headers.update(extra_headers)
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
            return response

    def get_patron_credential(self, patron, pin):
        """Create an OAuth token for the given patron."""
        def refresh(credential):
            return self.refresh_patron_access_token(
                credential, patron, pin)
        return Credential.lookup(
            self._db, DataSource.OVERDRIVE, patron, refresh)

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
            raise IOError(response['error'] + "/" + response['error_description'])
        return credential

    def checkout(self, patron, pin, overdrive_id, 
                 format_type='ebook-epub-adobe'):
        
        headers = {"Content-Type": "application/json"}
        payload = dict(fields=[dict(name="reserveId", value=overdrive_id)])
        if format_type:
            # The only reason to specify format_type==None is to test
            # checkouts on the live site without actually claiming the book.
            field = dict(name="formatType", value=format_type)
            payload['fields'].append(field)
        payload = json.dumps(payload)

        response = self.patron_request(
            patron, pin, self.CHECKOUTS_ENDPOINT, extra_headers=headers,
            data=payload)

        if response.status_code == 400:
            error = response.json()
            code = error['errorCode']
            if code == 'NoCopiesAvailable':
                raise NoAvailableCopies()

        # TODO: We need a better error URL here, not that it matters.
        expires, content_link_gateway = self.extract_data_from_checkout_response(
            response.json(), format_type, "http://library-simplified.com/")

        # Now GET the content_link_gateway, which will point us to the
        # ACSM file or equivalent.
        final_response = self.patron_request(patron, pin, content_link_gateway)
        content_link, content_type = self.extract_content_link(final_response.json())
        return content_link, content_type, expires
        
    def extract_content_link(self, content_link_gateway_json):
        link = content_link_gateway_json['links']['contentlink']
        return link['href'], link['type']

    @classmethod
    def extract_data_from_checkout_response(cls, checkout_response_json,
                                            format_type, error_url):

        if not 'expires' in checkout_response_json:
            expires = None
        else:
            expires = datetime.datetime.strptime(
                checkout_response_json['expires'], cls.TIME_FORMAT)
        return expires, cls.get_download_link(
            checkout_response_json, format_type, error_url)

    def get_patron_information(self, patron, pin):
        return self.patron_request(patron, pin, self.ME_ENDPOINT).json()

    def get_patron_checkouts(self, patron, pin):
        return self.patron_request(patron, pin, self.CHECKOUTS_ENDPOINT).json()

    @classmethod
    def sync_bookshelf(cls, patron, remote_view):
        """Synchronize Overdrive's view of the patron's bookshelf with our view.
        """

        _db = Session.object_session(patron)
        overdrive_source = DataSource.lookup(_db, DataSource.OVERDRIVE)
        active_loans = []

        loans = _db.query(Loan).join(Loan.license_pool).filter(
            LicensePool.data_source==overdrive_source)
        loans_by_identifier = dict()
        for loan in loans:
            loans_by_identifier[loan.license_pool.identifier.identifier] = loan

        to_add = []
        for checkout in remote_view['checkouts']:
            start = datetime.datetime.strptime(
                checkout['checkoutDate'], cls.TIME_FORMAT)
            end = datetime.datetime.strptime(
                checkout['expires'], cls.TIME_FORMAT)
            overdrive_identifier = checkout['reserveId'].lower()
            identifier, new = Identifier.for_foreign_id(
                _db, Identifier.OVERDRIVE_ID, overdrive_identifier)
            if identifier.identifier in loans_by_identifier:
                # We have a corresponding local loan. Just make sure the
                # data matches up.
                loan = loans_by_identifier[identifier.identifier]
                loan.start = start
                loan.end = end
                active_loans.append(loan)

                # Remove the loan from the list so that we don't
                # delete it later.
                del loans_by_identifier[identifier.identifier]
            else:
                # We never heard of this loan. Create it locally.
                pool, new = LicensePool.for_foreign_id(
                    _db, overdrive_source, identifier.type,
                    identifier.identifier)
                loan, new = pool.loan_to(patron, start, end)
                active_loans.append(loan)

        # Every loan remaining in loans_by_identifier is a loan that
        # Overdrive doesn't know about, which means it's expired and
        # we should get rid of it.
        for loan in loans_by_identifier.values():
            _db.delete(loan)
        return active_loans
       
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
        if isinstance(book, basestring):
            book_id = book
            circulation_link = self.AVAILABILITY_ENDPOINT % dict(
                collection_name=self.collection_name,
                product_id=book_id
            )
            book = dict(id=book_id)
        else:
            circulation_link = book['availability_link']
        status_code, headers, content = self.get(circulation_link, {})
        if status_code != 200:
            print "ERROR: Could not get availability for %s: %s" % (
                book['id'], status_code)
            return None, None, False

        book.update(json.loads(content))
        return self.update_licensepool_with_book_info(book)

    def update_licensepool_with_book_info(self, book):
        """Update a book's LicensePool with information from a JSON
        representation of its circulation info.

        Also adds very basic bibliographic information to the Edition.
        """
        overdrive_id = book['id']
        pool, was_new = LicensePool.for_foreign_id(
            self._db, self.source, Identifier.OVERDRIVE_ID, overdrive_id)
        if was_new:
            pool.open_access = False
            wr, wr_new = Edition.for_foreign_id(
                self._db, self.source, Identifier.OVERDRIVE_ID, overdrive_id)
            if 'title' in book:
                wr.title = book['title']
            print "New book: %r" % wr

        new_licenses_owned = []
        new_licenses_available = []
        new_number_of_holds = []
        if 'collections' in book:
            for collection in book['collections']:
                if 'copiesOwned' in collection:
                    new_licenses_owned.append(collection['copiesOwned'])
                if 'copiesAvailable' in collection:
                    new_licenses_available.append(collection['copiesAvailable'])
                if 'numberOfHolds' in collection:
                    new_number_of_holds.append(collection['numberOfHolds'])

        if new_licenses_owned:
            new_licenses_owned = sum(new_licenses_owned)
        else:
            new_licenses_owned = pool.licenses_owned

        if new_licenses_available:
            new_licenses_available = sum(new_licenses_available)
        else:
            new_licenses_available = pool.licenses_available

        if new_number_of_holds:
            new_number_of_holds = sum(new_number_of_holds)
        else:
            new_number_of_holds = pool.patrons_in_hold_queue

        # Overdrive doesn't do 'reserved'.
        licenses_reserved = 0

        edition, ignore = Edition.for_foreign_id(
            self._db, self.source, pool.identifier.type,
            pool.identifier.identifier)

        changed = (pool.licenses_owned != new_licenses_owned
                   or pool.licenses_available != new_licenses_available
                   or pool.licenses_reserved != licenses_reserved
                   or pool.patrons_in_hold_queue != new_number_of_holds)
            
        if changed:
            print '%s "%s" %s' % (edition.medium, edition.title, edition.author)
        #print " Owned: %s => %s" % (pool.licenses_owned, new_licenses_owned)
        #print " Available: %s => %s" % (pool.licenses_available, new_licenses_available)
        #print " Holds: %s => %s" % (pool.patrons_in_hold_queue, new_number_of_holds)

        pool.update_availability(new_licenses_owned, new_licenses_available,
                                 licenses_reserved, new_number_of_holds)
        return pool, was_new, changed

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
            print "ERROR: %r %r %r %r" % (status_code, headers, content, e)
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
        for f in checkout_response['formats']:
            if f['formatType'] == format_type:
                format = f
                break
        if not format:
            raise IOError("Could not find specified format %s" % format_type)

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
        print "Now: %s Asking for: %s" % (start, last_update_time)
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
                print " %s processed" % total_books
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
                    print "Found %d unchanged books." % consecutive_unchanged_books
                    break

        if total_books:
            print "Processed %d books total." % total_books

class FullOverdriveCollectionMonitor(OverdriveCirculationMonitor):
    """Monitor every single book in the Overdrive collection."""

    def __init__(self, interval_seconds=3600*4):
        super(FullOverdriveCollectionMonitor, self).__init__(
            "Overdrive Collection Overview", interval_seconds)

    def recently_changed_ids(self, start, cutoff):
        """Ignore the dates and return all IDs."""
        return self.api.all_ids()


class RecentOverdriveCollectionMonitor(FullOverdriveCollectionMonitor):
    """Monitor recently changed books in the Overdrive collection."""

    def __init__(self, _db, interval_seconds=60,
                 maximum_consecutive_unchanged_books=100):
        super(FullOverdriveCollectionMonitor, self).__init__(
            _db, "Reverse Chronological Overdrive Collection Monitor",
            interval_seconds, maximum_consecutive_unchanged_books)

