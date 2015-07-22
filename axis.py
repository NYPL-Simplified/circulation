from nose.tools import set_trace
import datetime
import base64
import requests
import os
import json

from util import LanguageCodes
from util.xmlparser import XMLParser
from model import (
    Contributor,
    DataSource,
    LicensePool,
    Edition,
    Identifier,
    Subject,
)

class Axis360API(object):

    DEFAULT_BASE_URL = "https://axis360api.baker-taylor.com/Services/VendorAPI/"
    
    DATE_FORMAT = "%m-%d-%Y %H:%M:%S"

    access_token_endpoint = 'accesstoken'
    availability_endpoint = 'availability/v2'

    def __init__(self, _db, username=None, library_id=None, password=None,
                 base_url=DEFAULT_BASE_URL):
        self._db = _db
        self.library_id = library_id or os.environ['AXIS_360_LIBRARY_ID']
        self.username = username or os.environ['AXIS_360_USERNAME']
        self.password = password or os.environ['AXIS_360_PASSWORD']
        self.base_url = base_url
        self.token = None
        self.source = DataSource.lookup(self._db, DataSource.AXIS_360)

    @property
    def authorization_headers(self):
        authorization = u":".join([self.username, self.password, self.library_id])
        authorization = authorization.encode("utf_16_le")
        print authorization
        authorization = base64.b64encode(authorization)
        return dict(Authorization="Basic " + authorization)

    def refresh_bearer_token(self):
        url = self.base_url + self.access_token_endpoint
        headers = self.authorization_headers
        response = self._make_request(url, 'post', headers)
        if response.status_code != 200:
            raise Exception(
                "Could not acquire bearer token: %s, %s" % (
                    response.status_code, response.content))
        return self.parse_token(response.content)

    def request(self, url, method='get', extra_headers={}, data=None,
                params=None, exception_on_401=False):
        """Make an HTTP request, acquiring/refreshing a bearer token
        if necessary.
        """
        if not self.token:
            self.token = self.refresh_bearer_token()

        headers = dict(extra_headers)
        headers['Authorization'] = "Bearer " + self.token
        headers['Library'] = self.library_id
        response = self._make_request(url, method, headers, data, params)
        if response.status_code == 401:
            if exception_on_401:
                # This is our second try. Give up.
                raise Exception(
                    "Something's wrong with the OAuth Bearer Token!")
            else:
                # The token has expired. Get a new token and try again.
                self.token = None
                return self.request(method, url, extra_headers, data, True)
        else:
            return response

    def availability(self, patron_id=None, since=None):
        url = self.base_url + self.availability_endpoint
        args = dict()
        if since:
            since = since.strftime(self.DATE_FORMAT)
            args['updatedDate'] = since
        if patron_id:
            args['patronId'] = patron_id
        return self.request(url, params=args)

    @classmethod
    def parse_token(cls, token):
        data = json.loads(token)
        return data['access_token']

    def _make_request(self, url, method, headers, data=None, params=None):
        """Actually make an HTTP request."""
        print url, headers, params
        return requests.request(
            url=url, method=method, headers=headers, data=data,
            params=params)

class Axis360Parser(XMLParser):

    NS = {"axis": "http://axis360api.baker-taylor.com/vendorAPI"}

    SHORT_DATE_FORMAT = "%m/%d/%Y"
    FULL_DATE_FORMAT_IMPLICIT_UTC = "%m/%d/%Y %H:%M:%S %p"
    FULL_DATE_FORMAT = "%m/%d/%Y %H:%M:%S %p +00:00"

    def _xpath1_boolean(self, e, target, ns, default=False):
        text = self.text_of_optional_subtag(e, target, ns)
        if text is None:
            return default
        if text == 'true':
            return True
        else:
            return False

    def _xpath1_date(self, e, target, ns):
        value = self.text_of_optional_subtag(e, target, ns)
        if value is None:
            return value
        try:
            attempt = datetime.datetime.strptime(
                value, self.FULL_DATE_FORMAT_IMPLICIT_UTC)
            value += ' +00:00'
        except ValueError:
            pass
        return datetime.datetime.strptime(value, self.FULL_DATE_FORMAT)

class BibliographicParser(Axis360Parser):

    @classmethod
    def parse_list(self, l):
        """Turn strings like this into lists:

        FICTION / Thrillers; FICTION / Suspense; FICTION / General
        Ursu, Anne ; Fortune, Eric (ILT)
        """
        return [x.strip() for x in l.split(";")]

    def __init__(self, include_availability=True, include_bibliographic=True):
        self.include_availability = include_availability
        self.include_bibliographic = include_bibliographic

    def process_all(self, string):
        for i in super(BibliographicParser, self).process_all(
                string, "//axis:title", self.NS):
            yield i

    def extract_availability(self, element, ns):
        # TODO: There are also empty tags:
        #  Checkouts
        #  Holds
        # Presumably these contain information about active loans and holds.

        availability = self._xpath1(element, 'axis:availability', ns)
        total_copies = self.int_of_subtag(availability, 'axis:totalCopies', ns)
        available_copies = self.int_of_subtag(
            availability, 'axis:availableCopies', ns)
        size_of_hold_queue = self.int_of_subtag(
            availability, 'axis:holdsQueueSize', ns)

        availability_updated = self.text_of_optional_subtag(
            availability, 'axis:updateDate', ns)
        if availability_updated:
            try:
                attempt = datetime.datetime.strptime(
                    availability_updated, self.FULL_DATE_FORMAT_IMPLICIT_UTC)
                availability_updated += ' +00:00'
            except ValueError:
                pass
            availability_updated = datetime.datetime.strptime(
                    availability_updated, self.FULL_DATE_FORMAT)
        return {
            LicensePool.licenses_owned : total_copies,
            LicensePool.licenses_available : available_copies,
            LicensePool.patrons_in_hold_queue : size_of_hold_queue,
            LicensePool.last_checked : availability_updated,
        }

    def extract_bibliographic(self, element, ns):

        # TODO: These are consistently empty (some are clearly for
        # audiobooks) so I don't know what they do and/or what format
        # they're in.
        #
        # annotation
        # edition
        # narrator
        # runtime

        identifier = self.text_of_subtag(element, 'axis:titleId', ns)
        isbn = self.text_of_optional_subtag(element, 'axis:isbn', ns)

        title = self.text_of_subtag(element, 'axis:productTitle', ns)

        contributor = self.text_of_optional_subtag(
            element, 'axis:contributor', ns)
        if contributor:
            contributors = self.parse_list(contributor)
            primary_author = [contributors[0]]
            other_authors = contributors[1:]
        else:
            primary_author = []
            other_authors = []

        subject = self.text_of_optional_subtag(element, 'axis:subject', ns)
        subjects = []
        if subject:
            for subject_identifier in self.parse_list(subject):
                subjects.append( { Subject.type : Subject.BISAC,
                                   Subject.identifier: subject_identifier } )

        publication_date = self.text_of_optional_subtag(
            element, 'axis:publicationDate', ns)
        if publication_date:
            publication_date = datetime.datetime.strptime(
                publication_date, self.SHORT_DATE_FORMAT)

        series = self.text_of_optional_subtag(element, 'axis:series', ns)
        publisher = self.text_of_optional_subtag(element, 'axis:publisher', ns)
        imprint = self.text_of_optional_subtag(element, 'axis:imprint', ns)

        audience = self.text_of_optional_subtag(element, 'axis:audience', ns)
        if audience:
            subjects.append({ Subject.type : Subject.AXIS_360_AUDIENCE,
                              Subject.identifier: audience })

        language = self.text_of_subtag(element, 'axis:language', ns)
        language = language.lower()
        language = LanguageCodes.english_names_to_three.get(language, None)

        # We don't use this for anything.
        # file_size = self.int_of_optional_subtag(element, 'axis:fileSize', ns)

        identifiers = { 
            Identifier.AXIS_360_ID : [ { Identifier.identifier : identifier } ]
        }
        if isbn:
            identifiers[Identifier.ISBN] = [ {Identifier.identifier : isbn } ]

        data = {
            Edition.title : title,
            Edition.published : publication_date,
            Edition.series : series,
            Edition.publisher : publisher,
            Edition.imprint : imprint,
            Edition.language : language,
            Identifier : identifiers,
            Subject : subjects,
            Contributor : { 
                Contributor.PRIMARY_AUTHOR_ROLE : primary_author,
                Contributor.AUTHOR_ROLE : other_authors,
            }
        }
        return data

    def process_one(self, element, ns):
        if self.include_bibliographic:
            bibliographic = self.extract_bibliographic(element, ns)
        else:
            bibliographic = None
        if self.include_availability:
            availability = self.extract_availability(element, ns)
        else:
            availability = None
        return bibliographic, availability
