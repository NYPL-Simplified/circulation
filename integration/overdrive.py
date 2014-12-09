import base64
import datetime
import os
import json
import isbnlib
import requests
import time
import urlparse
import urllib
import logging
import sys
from PIL import Image
from nose.tools import set_trace
from StringIO import StringIO

from sqlalchemy.orm.session import Session

from core.model import (
    get_one_or_create,
    CirculationEvent,
    CoverageProvider,
    Credential,
    DataSource,
    LicensePool,
    Loan,
    Measurement,
    Representation,
    Resource,
    Subject,
    Identifier,
    Edition,
)

from integration import (
    FilesystemCache,
    CoverImageMirror,
    NoAvailableCopies,
)
from monitor import Monitor
from util import LanguageCodes

class OverdriveAPI(object):

    TOKEN_ENDPOINT = "https://oauth.overdrive.com/token"
    PATRON_TOKEN_ENDPOINT = "https://oauth-patron.overdrive.com/patrontoken"

    LIBRARY_ENDPOINT = "http://api.overdrive.com/v1/libraries/%(library_id)s"
    ALL_PRODUCTS_ENDPOINT = "http://api.overdrive.com/v1/collections/%(collection_token)s/products"
    METADATA_ENDPOINT = "http://api.overdrive.com/v1/collections/%(collection_token)s/products/%(item_id)s/metadata"
    EVENTS_ENDPOINT = "http://api.overdrive.com/v1/collections/%(collection_name)s/products?lastupdatetime=%(lastupdatetime)s&sort=%(sort)s&limit=%(limit)s"
    AVAILABILITY_ENDPOINT = "http://api.overdrive.com/v1/collections/%(collection_name)s/products/%(product_id)s/availability"

    CHECKOUTS_ENDPOINT = "http://patron.api.overdrive.com/v1/patrons/me/checkouts"
    ME_ENDPOINT = "http://patron.api.overdrive.com/v1/patrons/me"

    MAX_CREDENTIAL_AGE = 50 * 60

    PAGE_SIZE_LIMIT = 300
    EVENT_SOURCE = "Overdrive"

    EVENT_DELAY = datetime.timedelta(minutes=120)
    #EVENT_DELAY = datetime.timedelta(minutes=0)

    # The ebook formats we care about.
    FORMATS = "ebook-epub-open,ebook-epub-adobe,ebook-pdf-adobe,ebook-pdf-open"

    TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
    
    def __init__(self, _db):
        self._db = _db
        self.source = DataSource.lookup(_db, DataSource.OVERDRIVE)

        # Set some stuff from environment variables
        self.client_key = os.environ['OVERDRIVE_CLIENT_KEY']
        self.client_secret = os.environ['OVERDRIVE_CLIENT_SECRET']
        self.website_id = os.environ['OVERDRIVE_WEBSITE_ID']
        self.library_id = os.environ['OVERDRIVE_LIBRARY_ID']
        self.collection_name = os.environ['OVERDRIVE_COLLECTION_NAME']

        # Get set up with up-to-date credentials from the API.
        self.check_creds()
        self.collection_token = self.get_library()['collectionToken']

    def check_creds(self, force_refresh=False):
        """If the Bearer Token has expired, update it."""
        if force_refresh:
            refresh_on_lookup = lambda x: x
        else:
            refresh_on_lookup = self.refresh_creds

        credential = Credential.lookup(
            self._db, DataSource.OVERDRIVE, None, refresh_on_lookup)
        if force_refresh:
            self.refresh_creds(credential)
        self.token = credential.credential

    def refresh_creds(self, credential):
        """Fetch a new Bearer Token and update the given Credential object."""
        response = self.token_post(
            self.TOKEN_ENDPOINT,
            dict(grant_type="client_credentials"))
        data = response.json()
        self._update_credential(credential, data)
        self.token = credential.credential

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

    def get(self, url, extra_headers, exception_on_401=False):
        """Make an HTTP GET request using the active Bearer Token."""
        headers = dict(Authorization="Bearer %s" % self.token)
        headers.update(extra_headers)
        status_code, headers, content = Representation.simple_http_get(
            url, headers)
        if status_code == 401:
            if exception_on_401:
                # This is our second try. Give up.
                raise Exception("Something's wrong with the OAuth Bearer Token!")
            else:
                # Refresh the token and try again.
                return self.get(url, extra_headers, True)
        else:
            return status_code, headers, content

    def token_post(self, url, payload, headers={}):
        """Make an HTTP POST request for purposes of getting an OAuth token."""
        s = "%s:%s" % (self.client_key, self.client_secret)
        auth = base64.encodestring(s).strip()
        headers = dict(headers)
        headers['Authorization'] = "Basic %s" % auth
        return requests.post(url, payload, headers=headers)

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

    def _update_credential(self, credential, overdrive_data):
        """Copy Overdrive OAuth data into a Credential object."""
        credential.credential = overdrive_data['access_token']
        expires_in = (overdrive_data['expires_in'] * 0.9)
        credential.expires = datetime.datetime.utcnow() + datetime.timedelta(
            seconds=expires_in)
        self._db.commit()

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

    @classmethod
    def extract_data_from_checkout_response(cls, checkout_response_json,
                                            format_type, error_url):

        if not 'expires' in checkout_response_json:
            set_trace()
            expires = None
        else:
            expires = datetime.datetime.strptime(
                checkout_response_json['expires'], cls.TIME_FORMAT)
        return expires, cls.get_download_link(
            checkout_response_json, format_type, error_url)


    def extract_content_link(self, content_link_gateway_json):
        link = content_link_gateway_json['links']['contentlink']
        return link['href'], link['type']

    def get_library(self):
        url = self.LIBRARY_ENDPOINT % dict(library_id=self.library_id)
        representation, cached = Representation.get(
            self._db, url, self.get, data_source=self.source)
        return json.loads(representation.content)

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
       

    def all_ids(self):
        """Get IDs for every book in the system, with (more or less) the most
        recent ones at the front.
        """
        params = dict(collection_token=self.collection_token)
        starting_link = self.make_link_safe(
            self.ALL_PRODUCTS_ENDPOINT % params)

        # Get the first page so we can find the 'last' link.
        status_code, headers, content = self.get(starting_link, {})
        try:
            data = json.loads(content)
        except Exception, e:
            print "ERROR: %r %r %r" % (status_code, headers, content)
            return
        previous_link = OverdriveRepresentationExtractor.link(data, 'last')

        while previous_link:
            try:
                page_inventory, previous_link = self._get_book_list_page(
                    previous_link, 'prev')
                for i in page_inventory:
                    yield i
            except Exception, e:
                print e
                sys.exit()


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

    def metadata_lookup(self, identifier):
        """Look up metadata for an Overdrive identifier.
        """
        url = self.METADATA_ENDPOINT % dict(
            collection_token=self.collection_token,
            item_id=identifier.identifier
        )
        representation, cached = Representation.get(
            self._db, url, self.get, data_source=self.source,
            identifier=identifier)
        return json.loads(representation.content)

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
            print "ERROR: %r %r %r" % (status_code, headers, content)
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

    @classmethod
    def make_link_safe(self, url):
        """Turn a server-provided link into a link the server will accept!

        This is completely obnoxious and I have complained about it to
        Overdrive.
        """
        parts = list(urlparse.urlsplit(url))
        parts[2] = urllib.quote(parts[2])
        query_string = parts[3]
        query_string = query_string.replace("+", "%2B")
        query_string = query_string.replace(":", "%3A")
        query_string = query_string.replace("{", "%7B")
        query_string = query_string.replace("}", "%7D")
        parts[3] = query_string
        return urlparse.urlunsplit(tuple(parts))
            

class OverdriveRepresentationExtractor(object):

    """Extract useful information from Overdrive's JSON representations."""

    @classmethod
    def availability_link_list(self, book_list):
        """:return: A list of dictionaries with keys `id`, `title`,
        `availability_link`.
        """
        l = []
        if not 'products' in book_list:
            return []

        products = book_list['products']
        for product in products:
            data = dict(id=product['id'],
                        title=product['title'],
                        author_name=None)
            
            if 'primaryCreator' in product:
                creator = product['primaryCreator']
                if creator.get('role') == 'Author':
                    data['author_name'] = creator.get('name')
            links = product.get('links', [])
            if 'availability' in links:
                link = links['availability']['href']
                data['availability_link'] = OverdriveAPI.make_link_safe(link)
            else:
                log.warn("No availability link for %s" % book_id)
            l.append(data)
        return l

    @classmethod
    def link(self, page, rel):
        if 'links' in page and rel in page['links']:
            raw_link = page['links'][rel]['href']
            link = OverdriveAPI.make_link_safe(raw_link)
        else:
            link = None
        return link


class OverdriveCirculationMonitor(Monitor):
    """Maintain license pool for Overdrive titles.

    This is where new books are given their LicensePools.  But the
    bibliographic data isn't inserted into those LicensePools until
    the OverdriveCoverageProvider runs.
    """
    def __init__(self, _db, name="Overdrive Circulation Monitor",
                 interval_seconds=500):
        super(OverdriveCirculationMonitor, self).__init__(
            name, interval_seconds=interval_seconds)
        self._db = _db
        self.api = OverdriveAPI(self._db)
        self.maximum_consecutive_unchanged_books = None

    def recently_changed_ids(self, start, cutoff):
        return self.api.recently_changed_ids(start, cutoff)

    def run_once(self, _db, start, cutoff):
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
                if self.maximum_consecutive_unchanged_books and consecutive_unchanged_books >= self.maximum_consecutive_unchanged_books:
                    # We're supposed to stop this run after finding a
                    # number of consecutive books that have not
                    # changed, and we have in fact seen this number of 
                    # consecutive unchanged books.
                    print "Found %d unchanged books." % consecutive_unchanged_books
                    break

        if total_books:
            print "Processed %d books total." % total_books

class FullOverdriveCollectionMonitor(OverdriveCirculationMonitor):
    """Monitor every single book in the Overdrive collection."""

    def __init__(self, _db, interval_seconds=3600*4):
        super(FullOverdriveCollectionMonitor, self).__init__(
            _db, "Overdrive Collection Overview", interval_seconds)

    def recently_changed_ids(self, start, cutoff):
        """Ignore the dates and return all IDs."""
        return self.api.all_ids()


class RecentOverdriveCollectionMonitor(FullOverdriveCollectionMonitor):
    """Monitor recently changed books in the Overdrive collection."""

    def __init__(self, _db, interval_seconds=60):
        super(FullOverdriveCollectionMonitor, self).__init__(
            _db, "Reverse Chronological Overdrive Collection Monitor",
            interval_seconds)
        self.maximum_consecutive_unchanged_books = 100


class OverdriveBibliographicMonitor(CoverageProvider):
    """Fill in bibliographic metadata for Overdrive records."""

    def __init__(self, _db):
        self._db = _db
        self.overdrive = OverdriveAPI(self._db)
        self.input_source = DataSource.lookup(_db, DataSource.OVERDRIVE)
        self.output_source = DataSource.lookup(_db, DataSource.OVERDRIVE)
        super(OverdriveBibliographicMonitor, self).__init__(
            "Overdrive Bibliographic Monitor",
            self.input_source, self.output_source)

    @classmethod
    def _add_value_as_resource(cls, input_source, identifier, pool, rel, value,
                               media_type="text/plain", url=None):
        if isinstance(value, str):
            value = value.decode("utf8")
        elif isinstance(value, unicode):
            pass
        else:
            value = str(value)
        identifier.add_resource(
            rel, url, input_source, pool, media_type, value)

    @classmethod
    def _add_value_as_measurement(
            cls, input_source, identifier, quantity_measured, value):
        if isinstance(value, str):
            value = value.decode("utf8")
        elif isinstance(value, unicode):
            pass

        value = float(value)
        identifier.add_measurement(
            input_source, quantity_measured, value)

    DATE_FORMAT = "%Y-%m-%d"

    def process_edition(self, wr):
        identifier = wr.primary_identifier
        info = self.overdrive.metadata_lookup(identifier)
        return self.annotate_edition_with_bibliographic_information(
            self._db, wr, info, self.input_source
        )

    media_type_for_overdrive_type = {
        "ebook-pdf-adobe" : "application/pdf",
        "ebook-pdf-open" : "application/pdf",
        "ebook-epub-adobe" : "application/epub+zip",
        "ebook-epub-open" : "application/epub+zip",
    }
        
    @classmethod
    def annotate_edition_with_bibliographic_information(
            cls, _db, wr, info, input_source):

        identifier = wr.primary_identifier
        license_pool = wr.license_pool

        # First get the easy stuff.
        wr.title = info['title']
        wr.subtitle = info.get('subtitle', None)
        wr.series = info.get('series', None)
        wr.publisher = info.get('publisher', None)
        wr.imprint = info.get('imprint', None)

        if 'publishDate' in info:
            wr.published = datetime.datetime.strptime(
                info['publishDate'][:10], cls.DATE_FORMAT)

        languages = [
            LanguageCodes.two_to_three.get(l['code'], l['code'])
            for l in info.get('languages', [])
        ]
        if 'eng' in languages or not languages:
            wr.language = 'eng'
        else:
            wr.language = sorted(languages)[0]

        # TODO: Is there a Gutenberg book with this title and the same
        # author names? If so, they're the same. Merge the work and
        # reuse the Contributor objects.
        #
        # Or, later might be the time to do that stuff.

        for creator in info.get('creators', []):
            name = creator['fileAs']
            display_name = creator['name']
            role = creator['role']
            contributor = wr.add_contributor(name, role)
            contributor.display_name = display_name
            if 'bioText' in creator:
                contributor.extra = dict(description=creator['bioText'])

        for i in info.get('subjects', []):
            c = identifier.classify(input_source, Subject.OVERDRIVE, i['value'])

        wr.sort_title = info.get('sortTitle')
        extra = dict()
        for inkey, outkey in (
                ('gradeLevels', 'grade_levels'),
                ('mediaType', 'medium'),
                ('awards', 'awards'),
        ):
            if inkey in info:
                extra[outkey] = info.get(inkey)
        wr.extra = extra

        # Associate the Overdrive Edition with other identifiers
        # such as ISBN.
        medium = Edition.BOOK_MEDIUM
        for format in info.get('formats', []):
            if format['id'].startswith('audiobook-'):
                medium = Edition.AUDIO_MEDIUM
            elif format['id'].startswith('video-'):
                medium = Edition.VIDEO_MEDIUM
            elif format['id'].startswith('ebook-'):
                medium = Edition.BOOK_MEDIUM
            elif format['id'].startswith('music-'):
                medium = Edition.MUSIC_MEDIUM
            else:
                print format['id']
                set_trace()
            for new_id in format.get('identifiers', []):
                t = new_id['type']
                v = new_id['value']
                type_key = None
                if t == 'ASIN':
                    type_key = Identifier.ASIN
                elif t == 'ISBN':
                    type_key = Identifier.ISBN
                    if len(v) == 10:
                        v = isbnlib.to_isbn13(v)
                elif t == 'DOI':
                    type_key = Identifier.DOI
                elif t == 'UPC':
                    type_key = Identifier.UPC
                elif t == 'PublisherCatalogNumber':
                    continue
                if type_key:
                    new_identifier, ignore = Identifier.for_foreign_id(
                        _db, type_key, v)
                    identifier.equivalent_to(
                        input_source, new_identifier, 1)

            # Samples become resources.
            if 'samples' in format:
                if format['id'] == 'ebook-overdrive':
                    # Useless to us.
                    continue
                media_type = cls.media_type_for_overdrive_type.get(
                    format['id'])
                for sample_info in format['samples']:
                    href = sample_info['url']
                    resource, new = identifier.add_resource(
                        Resource.SAMPLE, href, input_source,
                        license_pool, media_type)
                    resource.file_size = format['fileSize']

        # Add resources: cover and descriptions

        wr.medium = medium
        if medium == Edition.BOOK_MEDIUM:
            print medium, wr.title, wr.author
        if 'images' in info and 'cover' in info['images']:
            link = info['images']['cover']
            href = OverdriveAPI.make_link_safe(link['href'])
            media_type = link['type']
            identifier.add_resource(Resource.IMAGE, href, input_source,
                                    license_pool, media_type)

        short = info.get('shortDescription')
        full = info.get('fullDescription')

        if full:
            cls._add_value_as_resource(
                input_source, identifier, license_pool, Resource.DESCRIPTION, full,
                "text/html", "tag:full")

        if short and short != full and (not full or not full.startswith(short)):
            cls._add_value_as_resource(
                input_source, identifier, license_pool, Resource.DESCRIPTION, short,
                "text/html", "tag:short")

        # Add measurements: rating and popularity
        if info.get('starRating') is not None and info['starRating'] > 0:
            cls._add_value_as_measurement(
                input_source, identifier, Measurement.RATING,
                info['starRating'])

        if info['popularity']:
            cls._add_value_as_measurement(
                input_source, identifier, Measurement.POPULARITY,
                info['popularity'])

        return True

class OverdriveCoverImageMirror(CoverImageMirror):
    """Downloads images from Overdrive and writes them to disk."""

    ORIGINAL_PATH_VARIABLE = "original_overdrive_covers_mirror"
    SCALED_PATH_VARIABLE = "scaled_overdrive_covers_mirror"
    DATA_SOURCE = DataSource.OVERDRIVE
