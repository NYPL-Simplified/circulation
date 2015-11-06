import elasticsearch
import datetime

from model import (
    Edition,
    Work,
)

class WorkFeed(object):
    
    """Identify a feed of works in a way that can be used to generate
    CachedFeed objects.
    """

    COLLECTION_MAIN = 'main'
    COLLECTION_FULL = 'full'
    COLLECTION_TOP_QUALITY = 'top'
    COLLECTION_RECOMMENDED = 'recommended'

    AVAILABILITY_NOW = Work.CURRENTLY_AVAILABLE
    AVAILABILITY_ALL = Work.ALL
    AVAILABILITY_ALWAYS = 'open-access'

    # Define constants for use in URLs that need to explain how a feed
    # is ordered.
    ORDER_TITLE = 'title'
    ORDER_AUTHOR = 'author'
    ORDER_LAST_UPDATE = 'last_update'
    ORDER_WORK_ID = 'work_id'
    ORDER_RANDOM = 'random'

    # Define a mapping between those constants and database fields.
    order_facet_to_database_field = {
        ORDER_TITLE : Edition.sort_title,
        ORDER_AUTHOR : Edition.sort_author,
        ORDER_LAST_UPDATE : Work.last_update_time,
        ORDER_WORK_ID : Work.id,
    }

    # Define a reverse mapping.
    active_facet_for_field = {
        Edition.title : ORDER_TITLE,
        Edition.sort_title : ORDER_TITLE,
        Edition.sort_author : ORDER_AUTHOR,
        Edition.author : ORDER_AUTHOR,
    }

    # Setting an order facet will promote the corresponding field to
    # the top of this list but leave the rest of the list intact.
    default_sort_order = [Edition.sort_title, Edition.sort_author, Work.id]

    DEFAULT_AFTER = 0
    DEFAULT_SIZE = 50

    def __init__(self, languages, 
                 collection_name='main', 
                 availability='all', 
                 order_facet='author', 
                 sort_ascending=True,
    ):
        # We need to know about these variables because they
        # correspond to database fields. But subclasses are
        # responsible for populating them.
        self.lane = None
        self.custom_list = None
        self.custom_list_data_source = None
        self.list_duration = None

        self.collection_name = collection_name
        self.availability = availability

        # `languages` must end up a sorted list of languages.
        if isinstance(languages, basestring):
            if ',' in languages:
                languages = ','.split(languages)
            else:
                languages = [languages]
        elif not isinstance(languages, list):
            raise ValueError("Invalid value for languages: %r" % languages)
        self.languages = sorted(languages)

        # Convert `order_facet` (a constant) into `order_by` (a list
        # of database fields)
        self.order_facet = order_facet
        self.order_by = [self.order_facet_to_database_field[order_facet]]

        # Fill in `order_by` with the defaults to ensure a complete
        # ordering of books.
        for i in self.default_sort_order:
            if not i in self.order_by:
                self.order_by.append(i)

        if not isinstance(sort_ascending, bool):
            raise ValueError(
                "Invalid value for sort_ascending: %r" % sort_ascending
            )
        self.sort_ascending = sort_ascending

    @property
    def active_facet(self):
        """The active sort facet for this feed."""
        if not self.order_by:
            return None
        return self.active_facet_for_field.get(self.order_by[0], None)

    def page_feed(self, _db, offset, page_size, max_age, annotator):
        """Return an OPDS document for the given page of this feed.

        If possible, the feed will be obtained from a fresh CachedFeed
        object.  If not, a new feed will be generated and stuck in the
        database.
        """
        pass

    def base_query(self, _db):
        """A query that retrieves every work that should go in this feed.

        Subject to language and availability settings.

        This will be filtered down further by page_query.
        """
        # By default, return every Work in the entire database.
        base = Work.feed_query(_db, self.languages, self.availability)

    def page_query(self, _db, offset, page_size):
        """A query that retrieves a particular page of works.
        """
        query = self.base_query(_db)

        if self.sort_ascending:
            m = lambda x: x.asc()
        else:
            m = lambda x: x.desc()

        order_by = [m(x) for x in self.order_by]
        query = query.order_by(*order_by)
        query = query.distinct(*self.order_by)

        query = query.offset(offset)
        query = query.limit(page_size)

        return query

class LaneFeed(WorkFeed):

    """A WorkFeed where all the works come from a predefined lane."""

    def __init__(self, lane, languages, *args, **kwargs):        
        super(LaneFeed, self).__init__(languages, *args, **kwargs)
        self.lane = lane

    def base_query(self, _db):
        if self.lane is None:
            q = Work.feed_query(_db, self.languages, self.availability)
        else:
            q = self.lane.works(
                self.languages, availability=self.availability
            )
        return q

class CustomListFeed(LaneFeed):

    """A WorkFeed where all the works come from a given data source's
    custom lists.
    """

    # By default, consider a book to be a "best seller" if it was seen
    # on a best-seller list in the past two years.
    BEST_SELLER_LIST_DURATION = 730

    def __init__(self, lane, languages, custom_list_data_source, 
                 list_duration_days=None, **kwargs):
        super(CustomListFeed, self).__init__(lane, languages, **kwargs)
        self.custom_list_data_source = custom_list_data_source

        # `self.list_duration` must end up a timedelta
        if list_duration_days is None:
            self.list_duration = None
        else:
            self.list_duration = datetime.timedelta(days=list_duration_days)

    def base_query(self, _db):
        q = super(CustomListFeed, self).base_query(_db)
        return self.restrict(_db, q)

    def restrict(self, _db, q):
        if self.list_duration is None:
            on_list_as_of = None
        else:
            on_list_as_of = datetime.datetime.utcnow() - self.list_duration
        return Work.restrict_to_custom_lists_from_data_source(
            _db, q, self.custom_list_data_source, on_list_as_of)


class SingleCustomListFeed(CustomListFeed):

    def __init__(self, languages, custom_list, list_duration_days=None, 
                 **kwargs):
        super(SingleCustomListFeed, self).__init__(
            None, languages, list_duration_days, **kwargs)
        self.custom_list = custom_list

    def restrict(self, _db, q):
        if self.list_duration is None:
            on_list_as_of = None
        else:
            on_list_as_of = datetime.datetime.utcnow() - self.list_duration
        return Work.restrict_to_custom_lists(
            _db, q, [self.custom_list], on_list_as_of)
