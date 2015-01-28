from nose.tools import set_trace

from datetime import (
    datetime,
    timedelta,
)
import os
import json
import urlparse
import isbnlib
from sqlalchemy.orm.session import Session

from core.model import (
    get_one_or_create,
    Contributor,
    CustomList,
    DataSource,
    Edition,
    Identifier,
    Representation,
)

class BibliocommonsBase(object):

    TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

    def _parse_time(self, t):
        return datetime.strptime(t, self.TIME_FORMAT)

    def parse_times_in_place(self, data):
        for i in ('updated', 'created'):
            if i in data:
                data[i] = self._parse_time(data[i])


class BibliocommonsAPI(BibliocommonsBase):

    LIST_MAX_AGE = timedelta(days=1)
    TITLE_MAX_AGE = timedelta(days=30)

    BASE_URL = "https://api.bibliocommons.com/v1"
    LIST_OF_USER_LISTS_URL = "users/{user_id}/lists"
    LIST_URL = "lists/{list_id}" 
    TITLE_URL = "titles/{title_id}" 

    def __init__(self, _db, api_key=None, do_get=None):
        self._db = _db
        self.api_key = api_key or os.environ['BIBLIOCOMMONS_API_KEY']
        self.source = DataSource.lookup(self._db, DataSource.BIBLIOCOMMONS)
        self.do_get = do_get or Representation.http_get_no_timeout

    def request(self, path, max_age=LIST_MAX_AGE, identifier=None,
                do_get=None):
        if not path.startswith("/"):
            path = "/" + path
        if not path.startswith(self.BASE_URL):
            url = self.BASE_URL + path
        joiner = '?'
        if '?' in url:
            joiner = '&'
        url += joiner + "api_key=" + self.api_key
        
        # TODO: huge library-specific hack here
        # url += "&library=nypl"

        representation, cached = Representation.get(
            self._db, url, data_source=self.source, identifier=identifier,
            do_get=self.do_get, max_age=max_age, pause_before=1, debug=True)
        content = json.loads(representation.content)
        return content

    def list_pages_for_user(self, user_id, max_age=LIST_MAX_AGE):
        url = self.LIST_OF_USER_LISTS_URL.format(user_id=user_id)
        first_page = self.request(url, max_age)
        yield first_page
        if first_page['pages'] > 1:
            max_page = first_page['pages']
            for page_num in range(2, max_page+1):
                page_arg = "?page=%d" % page_num
                page_url = url + page_arg
                next_page = self.request(page_url, max_age)
                yield next_page

    def list_data_for_user(self, user_id, max_age=LIST_MAX_AGE):
        for page in self.list_pages_for_user(user_id, max_age):
            for list_data in page['lists']:                
                self.parse_times_in_place(list_data)
                yield list_data

    def get_list(self, list_id):
        url = self.LIST_URL.format(list_id=list_id)
        return self._make_list(self.request(url))

    def get_title(self, title_id):
        url = self.TITLE_URL.format(title_id=title_id)
        data = self.request(url, max_age=self.TITLE_MAX_AGE)
        return self._make_title(data)

    def _make_title(self, data):
        if not 'title' in data:
            return None
        return BibliocommonsTitle(data['title'])

    def _make_list(self, data):
        return BibliocommonsList(data)

class BibliocommonsList(BibliocommonsBase):

    def __init__(self, json_data):
        self.items = []
        list_data = json_data['list']
        self.parse_times_in_place(list_data)
        for i in (
                'id', 'name', 'description', 'list_type', 'language',
                'created', 'updated'
        ):
            setattr(self, i, list_data.get(i, None))

        self.creator = list_data.get('user', {})

        self.items = []
        for li_data in list_data.get('list_items', []):
            item = BibliocommonsListItem(li_data)
            self.items.append(item)
        print list_data

    def __iter__(self):
        return self.items.__iter__()

    def to_customlist(self, _db):
        """Turn this Bibliocommons list into a CustomList object."""
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
        the current state of the Bibliocommons list.
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

class BibliocommonsListItem(BibliocommonsBase):

    TITLE_TYPE = "title"

    def __init__(self, item_data):
        self.annotation = item_data.get('annotation')
        self.type = item_data.get('list_item_type')
        if self.type == self.TITLE_TYPE and 'title' in item_data:
            self.item = BibliocommonsTitle(item_data['title'])
        else:
            self.item = item_data

    def to_custom_list_item(self, custom_list):
        _db = Session.object_session(custom_list)        
        edition = self.item.to_edition(_db)
        return custom_list.add_entry(edition, self.annotation, 
                                     custom_list.updated)

class BibliocommonsTitle(BibliocommonsBase):

    DATE_FORMAT = "%Y"

    # TODO: This needs to be greatly expanded and moved into core/util
    LANGUAGE_CODES = {
        "English": "eng",
        "Spanish": "spa",
        "French": "fra",
        "Japanese": "jpn",
        "Russian": "rus",
        "Undetermined": "eng" # This is almost always correct.
    }

    def __init__(self, data):
        self.data = data

    def __getitem__(self, k):
        return self.get(k)

    def get(self, k, default=None):
        return self.data.get(k, default)

    def to_edition(self, _db):
        """Create or update a Simplified Edition object for this Bibliocommons
        title.
       """
        if not self['id']:
            return None
        data_source = DataSource.lookup(_db, DataSource.BIBLIOCOMMONS)
        edition, was_new = Edition.for_foreign_id(
            _db, data_source, Identifier.BIBLIOCOMMONS_ID, self['id'])

        edition.title = self['title']
        edition.subtitle = self['sub_title']
        if self['publishers']:
            edition.publisher = self['publishers'][0]['name']

        format = self['format']['id']
        # TODO: We need a bigger collection here.
        if format in ('BK', 'PAPERBACK', 'EBOOK'):
            edition.medium = Edition.BOOK_MEDIUM
        elif format in ('MUSIC_CD'):
            edition.medium = Edition.MUSIC_MEDIUM
        elif format in ('AB', 'BOOK_CD'):
            edition.medium = Edition.AUDIO_MEDIUM
        elif format in ('DVD'):
            edition.medium = Edition.VIDEO_MEDIUM
        else:
            print self['format']
            set_trace()

        language = self['primary_language'].get('name', 'Undetermined')
        if language == 'Undetermined':
            language = 'eng'
        if language in self.LANGUAGE_CODES:
            language = self.LANGUAGE_CODES[language]
        else:
            language = None
        edition.language = language

        for i in self.get('isbns', []):
            if len(i) == 10:
                i = isbnlib.to_isbn13(i)
            elif len(i) != 13:
                continue
            other_identifier, ignore = Identifier.for_foreign_id(
                _db, Identifier.ISBN, i)
            edition.primary_identifier.equivalent_to(
                data_source, other_identifier, 1)

        if self['publication_date']:
            edition.published = datetime.strptime(
                self['publication_date'][:4], self.DATE_FORMAT)

        all_authors = self.get('authors', []) + self.get(
            'additional_contributors', [])

        primary_author = None
        for author in all_authors:
            if not author['name']:
                continue
            if len(author.keys()) > 1:
                set_trace()
            if primary_author is None:
                role = Contributor.PRIMARY_AUTHOR_ROLE
                primary_author = author                
            else:
                role = Contributor.AUTHOR_ROLE
            edition.add_contributor(author['name'], role)

        for performer in self.get('performers', []):
            edition.add_contributor(
                performer['name'], Contributor.PERFORMER_ROLE)
        edition.calculate_presentation()
        return edition
