"""Interface to the New York Times APIs."""
import isbnlib
from nose.tools import set_trace
from datetime import datetime, timedelta
from collections import Counter
import os
import json
import logging
from sqlalchemy.orm.session import Session
from sqlalchemy.orm.exc import (
    NoResultFound,
)
from opds_import import SimplifiedOPDSLookup

from config import Configuration
from model import (
    get_one_or_create,
    CustomList,
    CustomListEntry,
    Contributor,
    DataSource,
    Edition,
    Hyperlink,
    Identifier,
    Representation,
    Resource,
)
from external_list import TitleFromExternalList

class NYTAPI(object):

    DATE_FORMAT = "%Y-%m-%d"

    @classmethod
    def parse_date(self, d):
        return datetime.strptime(d, self.DATE_FORMAT)

    @classmethod
    def date_string(self, d):
        return d.strftime(self.DATE_FORMAT)


class NYTBestSellerAPI(NYTAPI):
    
    BASE_URL = "http://api.nytimes.com/svc/books/v3/lists"

    LIST_NAMES_URL = BASE_URL + "/names.json"
    LIST_URL = BASE_URL + ".json?list=%s"
    
    LIST_OF_LISTS_MAX_AGE = timedelta(days=1)
    LIST_MAX_AGE = timedelta(days=1)
    HISTORICAL_LIST_MAX_AGE = timedelta(days=365)

    def __init__(self, _db, api_key=None, do_get=None, metadata_client=None):
        self._db = _db
        integration = Configuration.integration(Configuration.NYT_INTEGRATION)
        self.api_key = api_key or integration[
            Configuration.NYT_BEST_SELLERS_API_KEY
        ]
        self.do_get = do_get or Representation.simple_http_get
        self.source = DataSource.lookup(_db, DataSource.NYT)
        if not metadata_client:
            metadata_url = Configuration.integration_url(
                Configuration.METADATA_WRANGLER_INTEGRATION)
            metadata_client = SimplifiedOPDSLookup(metadata_url)
        self.metadata_client = metadata_client

    def request(self, path, identifier=None, max_age=LIST_MAX_AGE):
        if not path.startswith(self.BASE_URL):
            if not path.startswith("/"):
                path = "/" + path
            url = self.BASE_URL + path
        else:
            url = path
        joiner = '?'
        if '?' in url:
            joiner = '&'
        url += joiner + "api-key=" + self.api_key
        representation, cached = Representation.get(
            self._db, url, do_get=self.do_get, max_age=max_age, debug=True,
            pause_before=0.1)
        content = json.loads(representation.content)
        return content

    def list_of_lists(self, max_age=LIST_OF_LISTS_MAX_AGE):
        return self.request(self.LIST_NAMES_URL, max_age=max_age)

    def list_info(self, list_name):
        list_of_lists = self.list_of_lists()
        list_info = [x for x in list_of_lists['results']
                     if x['list_name_encoded'] == list_name]
        if not list_info:
            raise ValueError("No such list: %s" % list_name)
        return list_info[0]

    def best_seller_list(self, list_info, date=None):
        """Create (but don't update) a NYTBestSellerList object."""
        if isinstance(list_info, basestring):
            list_info = self.list_info(list_info)
        return NYTBestSellerList(list_info, self.metadata_client)

    def update(self, list, date=None, max_age=LIST_MAX_AGE):
        """Update the given list with data from the given date."""
        name = list.foreign_identifier
        url = self.LIST_URL % name
        if date:
            url += "&published-date=%s" % self.date_string(date)

        data = self.request(url, max_age=max_age)
        list.update(data)

    def fill_in_history(self, list):
        """Update the given list with current and historical data."""
        for date in list.all_dates:
            self.update(list, date, self.HISTORICAL_LIST_MAX_AGE)
            self._db.commit()

class NYTBestSellerList(list):

    def __init__(self, list_info, metadata_client):
        self.name = list_info['display_name']
        self.created = NYTAPI.parse_date(list_info['oldest_published_date'])
        self.updated = NYTAPI.parse_date(list_info['newest_published_date'])
        self.foreign_identifier = list_info['list_name_encoded']
        if list_info['updated'] == 'WEEKLY':
            frequency = 7
        elif list_info['updated'] == 'MONTHLY':
            frequency = 30
        self.frequency = timedelta(frequency)
        self.items_by_isbn = dict()
        self.metadata_client = metadata_client

    @property
    def all_dates(self):
        """Yield a list of estimated dates when new editions of this list were
        probably published.
        """
        date = self.updated
        end = self.created
        while date >= end:
            yield date
            old_date = date
            date = date - self.frequency  
            if old_date > end and date < end:
                # We overshot the end date.
                yield end

    def update(self, json_data):
        """Update the list with information from the given JSON structure."""
        for li_data in json_data.get('results', []):
            try:
                book = li_data['book_details'][0]
                key = (
                    book.get('primary_isbn13') or book.get('primary_isbn10'))
                if key in self.items_by_isbn:
                    item = self.items_by_isbn[key]
                    logging.debug("Previously seen ISBN: %r", key)
                else:
                    item = NYTBestSellerListTitle(li_data)
                    self.items_by_isbn[key] = item
                    self.append(item)
                    # logging.debug("Newly seen ISBN: %r, %s", key, len(self))
            except ValueError, e:
                # Should only happen when the book has no identifier, which...
                # should never happen.
                logging.wrror("No identifier for %r", li_data)
                item = None
                continue

            list_date = NYTAPI.parse_date(li_data['published_date'])
            if not item.first_appearance or list_date < item.first_appearance:
                item.first_appearance = list_date 
            if (not item.most_recent_appearance 
                or list_date > item.most_recent_appearance):
                item.most_recent_appearance = list_date


    def to_customlist(self, _db):
        """Turn this NYTBestSeller list into a CustomList object."""
        data_source = DataSource.lookup(_db, DataSource.NYT)
        l, was_new = get_one_or_create(
            _db, 
            CustomList,
            data_source=data_source,
            foreign_identifier=self.foreign_identifier,
            create_method_kwargs = dict(
                created=self.created,
            )
        )
        l.name = self.name
        l.updated = self.updated
        self.update_custom_list(l)
        return l

    def update_custom_list(self, custom_list):
        """Make sure the given CustomList's CustomListEntries reflect
        the current state of the NYTBestSeller list.
        """
        db = Session.object_session(custom_list)
    
        # Add new items to the list.
        for i in self:
            list_item, was_new = i.to_custom_list_entry(
                custom_list, self.metadata_client)

class NYTBestSellerListTitle(TitleFromExternalList):

    def __init__(self, data):
        data = data
        for i in ('bestsellers_date', 'published_date'):
            try:
                value = NYTAPI.parse_date(data.get(i))
            except ValueError, e:
                value = None
            setattr(self, i, value)

        if hasattr(self, 'bestsellers_date'):
            first_appearance = self.bestsellers_date
            most_recent_appearance = self.bestsellers_date
        else:
            first_appearance = None
            most_recent_appearance = None

        isbns = [
            (Identifier.ISBN, x['isbn13'])
            for x in data['isbns'] if 'isbn13' in x
        ]

        details = data['book_details']
        if len(details) > 0:
            for i in (
                    'publisher', 'description', 'primary_isbn10',
                    'primary_isbn13', 'title'):
                value = details[0].get(i, None)
                if value == 'None':
                    value = None
                setattr(self, i, value)

        primary_isbn = details[0].get('primary_isbn13')
        if not primary_isbn:
            primary_isbn = details[0].get('primary_isbn10')

        primary_isbn = (Identifier.ISBN, primary_isbn)

        # Don't call the display name of the author 'author'; it's
        # confusing.
        display_author = details[0].get('author', None)

        super(NYTBestSellerListTitle, self).__init__(
            DataSource.NYT, self.title, display_author,
            primary_isbn,
            self.published_date, first_appearance, most_recent_appearance,
            self.publisher, self.description, 'eng', isbns)
