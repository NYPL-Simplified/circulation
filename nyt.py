"""Interface to the New York Times APIs."""
import isbnlib
from nose.tools import set_trace
from datetime import datetime, timedelta
from collections import Counter
import os
import json
from sqlalchemy.orm.session import Session
from sqlalchemy.orm.exc import (
    NoResultFound,
)

from model import (
    get_one_or_create,
    CustomList,
    CustomListEntry,
    Contributor,
    DataSource,
    Edition,
    Identifier,
    Representation,
    Resource,
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

    def list_info(self, list_name):
        list_of_lists = self.list_of_lists()
        list_info = [x for x in list_of_lists['results']
                     if x['list_name_encoded'] == list_name]
        if not list_info:
            raise ValueError("No such list: %s" % list_name)
        return list_info[0]

    def best_seller_list(self, list_info):
        if isinstance(list_info, basestring):
            list_info = self.list_info(list_info)
        name = list_info['list_name_encoded']
        data = self.request(
            self.LIST_URL % name, max_age=self.LIST_MAX_AGE)
        return self._make_list(list_info, data)

    def _make_list(self, list_info, data):
        return NYTBestSellerList(list_info, data)


class NYTBestSellerList(list):

    def __init__(self, list_info, json_data):
        self.name = list_info['display_name']
        self.created = NYTAPI.parse_date(list_info['oldest_published_date'])
        self.updated = NYTAPI.parse_date(list_info['newest_published_date'])
        self.foreign_identifier = list_info['list_name_encoded']
        for li_data in json_data.get('results', []):
            try:
                item = NYTBestSellerListTitle(li_data)
            except ValueError, e:
                # Should only happen when the book has no identifier, which...
                # should never happen.
                print "ERROR: No identifier for %r" % li_data
                item = None
            if item:
                self.append(item)

    def to_customlist(self, _db):
        """Turn this NYTBestSeller list into a CustomList object."""
        data_source = DataSource.lookup(_db, DataSource.BIBLIOCOMMONS)
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

        previous_contents = {}
        for entry in custom_list.entries:
            previous_contents[entry.edition.id] = entry
    
        # Add new items to the list.
        for i in self:
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
                    'primary_isbn13', 'title'):
                value = details[0].get(i, None)
                if value == 'None':
                    value = None
                setattr(self, i, value)

        # Don't call the display name of the author 'author'; it's
        # confusing.
        self.display_author = details[0].get('author', None)

        if not self.primary_isbn10 and not self.primary_isbn13:
            raise ValueError("Book has no identifier")

    def to_custom_list_item(self, custom_list):
        _db = Session.object_session(custom_list)        
        edition = self.to_edition(_db)
        return custom_list.add_entry(edition, added=self.bestsellers_date)

    def find_sort_name(self, _db):
        """Find the sort name for this book's author, assuming it's easy.

        'Easy' means we already have an established sort name for a
        Contributor with this exact display name.
        
        If it's not easy, this will be taken care of later with a call to
        the metadata wrangler's author canonicalization service.

        If we have a copy of this book in our collection (the only
        time a NYT bestseller list item is relevant), this will
        probably be easy.

        """
        contributors = _db.query(Contributor).filter(
            Contributor.display_name==self.display_author).filter(
                Contributor.name != None).all()
        if contributors:
            return contributors[0].name
        return None
       
    def to_edition(self, _db):
        """Create or update a Simplified Edition object for this NYTBestSeller
        title.
       """
        identifier = self.primary_isbn13 or self.primary_isbn10
        if not identifier:
            return None
        self.primary_identifier, ignore = Identifier.from_asin(_db, identifier)
        data_source = DataSource.lookup(_db, DataSource.NYT)

        edition, was_new = Edition.for_foreign_id(
            _db, data_source, self.primary_identifier.type,
            self.primary_identifier.identifier)

        if edition.title != self.title:
            edition.title = self.title
            edition.permanent_work_id = None
        edition.publisher = self.publisher
        edition.medium = Edition.BOOK_MEDIUM
        edition.language = 'eng'

        for i in self.isbns:
            other_identifier, ignore = Identifier.from_asin(_db, i)
            edition.primary_identifier.equivalent_to(
                data_source, other_identifier, 1)

        if self.published_date:
            edition.published = self.published_date

        if edition.author != self.display_author:
            edition.permanent_work_id = None
            edition.author = self.display_author
        if not edition.sort_author:
            edition.sort_author = self.find_sort_name(_db)
        # If find_sort_name returned a sort_name, we can calculate a
        # permanent work ID for this Edition, and be done with it.
        #
        # Otherwise, we'll have to ask the metadata wrangler to find
        # the canonicalized author name for this book.
        if edition.sort_author:
            edition.calculate_permanent_work_id()

        # Set or update the description.
        description, ignore = get_one_or_create(
            _db, Resource, rel=Resource.DESCRIPTION,
            data_source=data_source,
            identifier=self.primary_identifier)
        description.content = self.description
        description.media_type = "text/plain"

        print "%s - %s - %s" % (edition.title, edition.author, edition.sort_author)
        return edition

if __name__ == '__main__':
    from model import production_session
    db = production_session()
    api = NYTBestSellerAPI(db)
    names = api.list_of_lists()
    for l in names['results']:
        best = api.best_seller_list(l)
        best.to_customlist(db)
        for item in best:
            item.to_edition(db)
