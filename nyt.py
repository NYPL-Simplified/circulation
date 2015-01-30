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
)
from util import MetadataSimilarity
from util.personal_names import display_name_to_sort_name
from oclc import OCLCLinkedData

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

    def best_seller_list(self, list_info):
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
        print "I expect %s results and find %s" % (
            json_data['num_results'], len(json_data['results']))
        for li_data in json_data.get('results', []):
            try:
                item = NYTBestSellerListTitle(li_data)
            except ValueError, e:
                # Should only happen when the book has no ISBN, which...
                # should never happen.
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
                    'primary_isbn13', 'title', 'author'):
                value = details[0].get(i, None)
                if value == 'None':
                    value = None
                setattr(self, i, value)
    
        if self.primary_isbn13:
            if isbnlib.is_isbn13(self.primary_isbn13):
                self.primary_identifier_type = Identifier.ISBN
            else:
                self.primary_identifier_type = Identifier.ASIN
        else:
                if not self.primary_isbn10:
                    raise ValueError("No ISBN for book")
                if isbnlib.is_isbn10(self.primary_isbn10):
                    self.primary_isbn13 = isbnlib.to_isbn13(self.primary_isbn10)
                    self.primary_identifier_type = Identifier.ISBN
                else:
                    self.primary_isbn13 = self.primary_isbn10
                    self.primary_identifier_type = Identifier.ASIN


    def to_custom_list_item(self, custom_list):
        _db = Session.object_session(custom_list)        
        edition = self.to_edition(_db)
        return custom_list.add_entry(edition, added=self.bestsellers_date)

    def find_sort_name(self, _db):
        """Do whatever it takes to find a sortable author name for this book.
        
        self.author is a name like "Paula Hawkins". That's fine for
        edition.author, but to calculate permanent work ID we need
        to set edition.sort_author to "Hawkins, Paula".
        
        We don't want to call calculate_presentation(), because we
        don't have confidence that we can find the *right* person
        named "Paula Hawkins".

        But all people with the same name have the same
        canonicalized name, so if we can find *someone* with this
        name we can set edition.sort_author directly and not bother with
        calculate_presentation().
        """


    @classmethod
    def sort_name_from_oclc_linked_data(
            self, _db, oclc_client, display_name, primary_identifier_type,
            primary_identifier):
        """Try to find an author sort name for this book from
        OCLC Linked Data.
        """
        def comparable_name(s):
            return s.replace(",", "").replace(".", "")

        test_working_display_name = comparable_name(display_name)

        if primary_identifier_type != Identifier.ISBN:
            # We have no way of telling OCLC Linked Data which book
            # we're talking about. Don't bother.
            return None

        identifier, ignore = Identifier.for_foreign_id(
            _db, primary_identifier_type, primary_identifier)
        try:
            works = list(oclc_client.oclc_works_for_isbn(identifier))
        except IOError, e:
            works = []
        shortest_candidate = None
        for work in works:
            graph = oclc_client.graph(work)
            # TODO: Sometimes the creator graph includes VIAF
            # numbers. We should store these and use them
            # in preference to doing a name-based lookup.
            for field_name in ('creator', 'contributor'):
                for name in oclc_client.creator_names(graph, field_name):
                    if name.endswith(','):
                        name = name[:-1]
                    test_name = comparable_name(name)
                    sim = MetadataSimilarity.title_similarity(
                        test_name, test_working_display_name)
                    if sim > 0.6:
                        if (not shortest_candidate 
                            or len(name) < len(shortest_candidate)):
                            shortest_candidate = name
        return shortest_candidate

    def sort_name_from_viaf(self, viaf_client, display_name):
        #viaf, display_name, family_name, sort_name, wikipedia_name = (
        #        viaf_client.lookup_by_name(None, display_name))
        #return sort_name
        return None
        
    def to_edition(self, _db):
        """Create or update a Simplified Edition object for this NYTBestSeller
        title.
       """
        if not self.primary_isbn13:
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

        edition.author = self.author
        sort_name = self.find_sort_name(_db)
        edition.sort_author = sort_name

        print "%s - %s" % (edition.title, edition.sort_author)
        print "-" * 80
        edition.calculate_permanent_work_id()
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
