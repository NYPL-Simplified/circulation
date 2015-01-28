"""Interface to the New York Times APIs."""
from nose.tools import set_trace
from datetime import datetime, timedelta
import os
import json
from model import (
    DataSource,
    Representation,
)

class NYTAPI(object):

    DATE_FORMAT = "%Y-%m-%d"

    @classmethod
    def parse_date(self, d):
        return datetime.strptime(d, self.DATE_FORMAT)


class NYTBestSellerAPI(NYTAPI):
    
    BASE_URL = "http://api.nytimes.com/svc/books/v3/lists"

    LIST_NAMES_URL = BASE_URL + "/names.json"
    LIST_URL = BASE_URL + ".json?list=%s"
    
    LIST_OF_LISTS_MAX_AGE = timedelta(days=1)
    LIST_MAX_AGE = timedelta(days=1)

    def __init__(self, _db, api_key=None, do_get=None):
        self._db = _db
        self.api_key = api_key or os.environ['NYT_BEST_SELLERS_API_KEY']
        self.do_get = do_get or Representation.simple_http_get
        self.source = DataSource.lookup(_db, DataSource.NYT)

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
            self._db, url, data_source=self.source, identifier=identifier,
            do_get=self.do_get, max_age=max_age, debug=True)
        content = json.loads(representation.content)
        return content

    def list_of_lists(self, max_age=LIST_OF_LISTS_MAX_AGE):
        return self.request(self.LIST_NAMES_URL, max_age=max_age)

    def best_seller_list(self, name):
        if isinstance(name, dict):
            name = name['list_name_encoded']
        data = self.request(
            self.LIST_URL % name, max_age=self.LIST_MAX_AGE)
        return self._make_list(data)

    def _make_list(self, data):
        return NYTBestSellerList(data)


class NYTBestSellerList(list):

    def __init__(self, json_data):
        for li_data in json_data.get('results', []):
            try:
                item = NYTBestSellerListTitle(li_data)
            except ValueError, e:
                # Should only happen when the book has no ISBN, which...
                # should never happen.
                item = None
            if item:
                self.append(item)

    def __iter__(self):
        return self.items.__iter__()

    def to_customlist(self, _db):
        """Turn this NYTBestSeller list into a CustomList object."""
        data_source = DataSource.lookup(_db, DataSource.BIBLIOCOMMONS)
        l, was_new = get_one_or_create(
            _db, 
            CustomList,
            data_source=data_source,
            foreign_identifier=self.id,
            create_method_kwargs = dict(
                created=self.created
            )
        )
        l.name = self.name
        l.description = self.description
        l.responsible_party = self.creator.get('name')
        l.updated = self.updated
        self.update_custom_list(l)
        return l

    def update_custom_list(self, custom_list):
        """Make sure the given CustomList's CustomListEntries reflect
        the current state of the NYTBestSeller list.
        """
        db = Session.object_session(custom_list)

        previous_contents = {}
        for entry in custom_list.entries:
            previous_contents[entry.edition.id] = entry
    
        # Add new items to the list.
        for i in self.items:
            list_item, was_new = i.to_custom_list_item(custom_list)
            if list_item.edition.id in previous_contents:
                del previous_contents[edition.id]

        # Mark items no longer on the list as removed.
        for entry in previous_contents.values():
            entry.removed = self.updated

class NYTBestSellerListTitle(object):

    def __init__(self, data):
        self.data = data
        for i in ('bestsellers_date', 'published_date'):
            try:
                value = NYTAPI.parse_date(data.get(i))
            except ValueError, e:
                value = None
            setattr(self, i, value)

        self.isbns = [x['isbn13'] for x in data['isbns'] if 'isbn13' in x]

        details = data['book_details']
        if len(details) > 0:
            for i in (
                    'publisher', 'description', 'primary_isbn10',
                    'primary_isbn13', 'title', 'author'):
                setattr(self, i, details[0].get(i, None))
        
        if not self.primary_isbn13:
            if not self.primary_isbn10:
                raise ValueError("No ISBN for book")
            self.primary_isbn13 = isbnlib.to_isbn13(self.primary_isbn10)

    def to_custom_list_item(self, custom_list):
        _db = Session.object_session(custom_list)        
        edition = self.to_edition(_db)
        return custom_list.add_entry(edition, added=self.bestsellers_date)

    def to_edition(self, _db):
        """Create or update a Simplified Edition object for this NYTBestSeller
        title.
       """
        if not self['id']:
            return None
        data_source = DataSource.lookup(_db, DataSource.NYT)
        edition, was_new = Edition.for_foreign_id(
            _db, data_source, Identifier.ISBN, self.primary_isbn13)

        edition.title = self.title
        edition.publisher = self.publisher
        edition.medium = Edition.BOOK_MEDIUM
        edition.language = 'eng'

        for i in self.isbns:
            other_identifier, ignore = Identifier.for_foreign_id(
                _db, Identifier.ISBN, i)
            edition.primary_identifier.equivalent_to(
                data_source, other_identifier, 1)

        if self.published_date:
            edition.published = self.published_date

        # Big TODO:
        # self.author is a name like "Paula Hawkins".
        # We need "Hawkins, Paula".
        #
        # We want to set sort_author so we can calculate permanent
        # work ID, but we don't want to call calculate_presentation(),
        # because we don't have confidence that we can find the
        # *right* "Paula Hawkins".
        #
        # If we just set sort_author, it doesn't matter if we find the
        # right Paula Hawkins, just so long as we get the correct
        # canonicalized name.
        #
        # If we can find an existing Contributor with "Paula Hawkins"
        # as their display_name, we will use their name.
        #
        # Otherwise we will ask VIAF about "Paula Hawkins" and find the
        # name it gives us that looks the most like what we expect.

        #
        # Smaller TODO: stop calculate_presentation() from doing
        # anything to Editions whose data source is DataSource.NYT.

        edition.calculate_permanent_work_id()
        return edition
