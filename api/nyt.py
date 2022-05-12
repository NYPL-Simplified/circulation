"""Interface to the New York Times APIs."""
import isbnlib
from datetime import datetime, timedelta
from collections import Counter
import os
import json
import logging
from sqlalchemy.orm.session import Session
from sqlalchemy.orm.exc import (
    NoResultFound,
)
from flask_babel import lazy_gettext as _

from config import (
    CannotLoadConfiguration,
    IntegrationException,
)

from core.selftest import (
    HasSelfTests,
)
from core.opds_import import MetadataWranglerOPDSLookup
from core.metadata_layer import (
    Metadata,
    IdentifierData,
    ContributorData,
)
from core.model import (
    get_one_or_create,
    CustomList,
    DataSource,
    Edition,
    ExternalIntegration,
    Identifier,
    Representation,
)
from core.external_list import TitleFromExternalList

class NYTAPI(object):

    DATE_FORMAT = "%Y-%m-%d"

    @classmethod
    def parse_date(self, d):
        return datetime.strptime(d, self.DATE_FORMAT)

    @classmethod
    def date_string(self, d):
        return d.strftime(self.DATE_FORMAT)


class NYTBestSellerAPI(NYTAPI, HasSelfTests):

    PROTOCOL = ExternalIntegration.NYT
    GOAL = ExternalIntegration.METADATA_GOAL
    NAME = _("NYT Best Seller API")
    CARDINALITY = 1

    SETTINGS = [
        { "key": ExternalIntegration.PASSWORD, "label": _("API key"), "required": True },
    ]

    # An NYT integration is shared by all libraries in a circulation manager.
    SITEWIDE = True

    BASE_URL = "http://api.nytimes.com/svc/books/v3/lists"

    LIST_NAMES_URL = BASE_URL + "/names.json"
    LIST_URL = BASE_URL + ".json?list=%s"

    LIST_OF_LISTS_MAX_AGE = timedelta(days=1)
    LIST_MAX_AGE = timedelta(days=1)
    HISTORICAL_LIST_MAX_AGE = timedelta(days=365)

    @classmethod
    def from_config(cls, _db, **kwargs):
        integration = cls.external_integration(_db)

        if not integration:
            message = "No ExternalIntegration found for the NYT."
            raise CannotLoadConfiguration(message)

        return cls(_db, api_key=integration.password, **kwargs)

    def __init__(self, _db, api_key=None, do_get=None, metadata_client=None):
        self.log = logging.getLogger("NYT API")
        self._db = _db
        if not api_key:
            raise CannotLoadConfiguration("No NYT API key is specified")
        self.api_key = api_key
        self.do_get = do_get or Representation.simple_http_get
        if not metadata_client:
            try:
                metadata_client = MetadataWranglerOPDSLookup.from_config(
                    self._db
                )
            except CannotLoadConfiguration, e:
                self.log.error(
                    "Metadata wrangler integration is not configured, proceeding without one."
                )
        self.metadata_client = metadata_client

    @classmethod
    def external_integration(cls, _db):
        return ExternalIntegration.lookup(
            _db, ExternalIntegration.NYT,
            ExternalIntegration.METADATA_GOAL
        )

    def _run_self_tests(self, _db):
        yield self.run_test(
            "Getting list of best-seller lists", self.list_of_lists
        )

    @property
    def source(self):
        return DataSource.lookup(_db, DataSource.NYT)

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
        status = representation.status_code
        if status == 200:
            # Everything's fine.
            content = json.loads(representation.content)
            return content

        diagnostic = "Response from %s was: %r" % (
            url, representation.content
        )

        if status == 403:
            raise IntegrationException(
                "API authentication failed",
                "API key is most likely wrong. %s" % diagnostic
            )
        else:
            raise IntegrationException(
                "Unknown API error (status %s)" % status, diagnostic
            )

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
        self.log = logging.getLogger("NYT Best-seller list %s" % self.name)

    @property
    def medium(self):
        """What medium are the books on this list?

        Lists like "Audio Fiction" contain audiobooks; all others
        contain normal books. (TODO: this isn't quite right; the
        distinction between ebooks and print books here exists in a
        way it doesn't with most other sources of Editions.)
        """
        name = self.name
        if not name:
            return None
        if name.startswith("Audio "):
            return Edition.AUDIO_MEDIUM
        return Edition.BOOK_MEDIUM

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
                    self.log.debug("Previously seen ISBN: %r", key)
                else:
                    item = NYTBestSellerListTitle(li_data, self.medium)
                    self.items_by_isbn[key] = item
                    self.append(item)
                    # self.log.debug("Newly seen ISBN: %r, %s", key, len(self))
            except ValueError, e:
                # Should only happen when the book has no identifier, which...
                # should never happen.
                self.log.error("No identifier for %r", li_data)
                item = None
                continue

            # This is the date the *best-seller list* was published,
            # not the date the book was published.
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
            # If possible, associate the item with a Work.
            list_item.set_work()


class NYTBestSellerListTitle(TitleFromExternalList):

    def __init__(self, data, medium):
        data = data
        try:
            bestsellers_date = NYTAPI.parse_date(data.get('bestsellers_date'))
            first_appearance = bestsellers_date
            most_recent_appearance = bestsellers_date
        except ValueError, e:
            first_appearance = None
            most_recent_appearance = None

        try:
            published_date = NYTAPI.parse_date(data.get('published_date'))
        except ValueError, e:
            published_date = None

        details = data['book_details']
        other_isbns = []
        if len(details) == 0:
            publisher = annotation = primary_isbn10 = primary_isbn13 = title = None
            display_author = None
        else:
            d = details[0]
            title = d.get('title', None)
            display_author = d.get('author', None)
            publisher = d.get('publisher', None)
            annotation = d.get('description', None)
            primary_isbn10 = d.get('primary_isbn10', None)
            primary_isbn13 = d.get('primary_isbn13', None)

            # The list of other ISBNs frequently contains ISBNs for
            # other books in the same series, as well as ISBNs that
            # are just wrong. Assign these equivalencies at a low
            # level of confidence.
            for isbn in d.get('isbns', []):
                isbn13 = isbn.get('isbn13', None)
                if isbn13:
                    other_isbns.append(
                        IdentifierData(Identifier.ISBN, isbn13, 0.50)
                    )


        primary_isbn = primary_isbn13 or primary_isbn10
        if primary_isbn:
            primary_isbn = IdentifierData(Identifier.ISBN, primary_isbn, 0.90)

        contributors = []
        if display_author:
            contributors.append(
                ContributorData(display_name=display_author)
            )

        metadata = Metadata(
            data_source=DataSource.NYT,
            title=title,
            medium=medium,
            language='eng',
            published=published_date,
            publisher=publisher,
            contributors=contributors,
            primary_identifier=primary_isbn,
            identifiers=other_isbns,
        )

        super(NYTBestSellerListTitle, self).__init__(
            metadata, first_appearance, most_recent_appearance,
            annotation
        )
