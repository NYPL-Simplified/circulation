import random
import time
import logging

from psycopg2.extras import NumericRange

import classifier
from classifier import (
    Classifier,
    GenreData,
)

from config import Configuration

from sqlalchemy import (
    or_,
)

from sqlalchemy.orm import (
    contains_eager,
    lazyload,
)

from model import (
    DataSource,
    DeliveryMechanism,
    Edition,
    Genre,
    LicensePool,
    Work,
    WorkGenre,
)

class Facets(object):

    # Subset the collection, roughly, by quality.
    COLLECTION_FULL = "full"
    COLLECTION_MAIN = "main"
    COLLECTION_FEATURED = "featured"

    # Subset the collection by availability.
    AVAILABLE_NOW = "currently_available"
    AVAILABLE_ALL = "all"
    AVAILABLE_OPEN_ACCESS = "open_access"

    # The names of the order facets.
    ORDER_TITLE = 'title'
    ORDER_AUTHOR = 'author'
    ORDER_LAST_UPDATE = 'last_update'
    ORDER_WORK_ID = 'work_id'
    ORDER_RANDOM = 'random'

    ORDER_ASCENDING = "asc"
    ORDER_DESCENDING = "desc"

    @classmethod
    def order_facet_to_database_field(
            cls, order_facet, work_model, edition_model
    ):
        """Turn the name of an order facet into a database field
        for use in an ORDER BY clause.
        """
        if order_facet == cls.ORDER_WORK_ID:
            if work_model is Work:
                return work_model.id
            else:
                # This is a materialized view and the field name is
                # different.
                return work_model.works_id

        # In all other cases the field names are the same whether
        # we are using Work/Edition or a materialized view.
        order_facet_to_database_field = {
            cls.ORDER_TITLE : edition_model.sort_title,
            cls.ORDER_AUTHOR : edition_model.sort_author,
            cls.ORDER_LAST_UPDATE : work_model.last_update_time,
            cls.ORDER_RANDOM : work_model.random,
        }
        return order_facet_to_database_field[order_facet]

    @classmethod
    def database_field_to_order_facet(cls, database_field):
        """The inverse of order_facet_to_database_field.

        TODO: This method may not be necessary.
        """
        from model import (
            MaterializedWork as mw,
            MaterializedWorkWithGenre as mwg,
        )

        if database_field in (Edition.sort_title, mw.sort_title, 
                              mwg.sort_title):
            return cls.ORDER_TITLE

        if database_field in (Edition.sort_author, mw.sort_author,
                              mwg.sort_author):
            return cls.ORDER_AUTHOR

        if database_field in (Work.last_update_time, mw.last_update_time, 
                              mwg.last_update_time):
            return cls.ORDER_LAST_UPDATE

        if database_field in (Work.id, mw.works_id, mwg.works_id):
            return cls.ORDER_WORK_ID

        if database_field in (Work.random, mw.random, mwg.random):
            return cls.ORDER_RANDOM

        return None

    def __init__(self, collection, availability, order,
                 order_ascending=True):

        hold_policy = Configuration.hold_policy()
        if (availability == self.AVAILABLE_ALL and 
            hold_policy == Configuration.HOLD_POLICY_HIDE):
            # Under normal circumstances we would show all works, but
            # site configuration says to hide books that aren't
            # available.
            availability = self.AVAILABLE_NOW

        self.collection = collection
        self.availability = availability
        self.order = order
        if order_ascending == self.ORDER_ASCENDING:
            order_ascending = True
        elif order_ascending == self.ORDER_DESCENDING:
            order_ascending = False
        self.order_ascending = order_ascending

    def apply(self, _db, q, work_model=Work, edition_model=Edition):
        """Restrict a query so that it only matches works that fit
        the given facets, and the query is ordered appropriately.
        """
        if self.availability == self.AVAILABLE_NOW:
            availability_clause = or_(
                LicensePool.open_access==True,
                LicensePool.licenses_available > 0)
        elif self.availability == self.AVAILABLE_ALL:
            availability_clause = or_(
                LicensePool.open_access==True,
                LicensePool.licenses_owned > 0)
        elif self.availability == self.AVAILABLE_OPEN_ACCESS:
            availability_clause = LicensePool.open_access==True
        q = q.filter(availability_clause)

        if self.collection == self.COLLECTION_FULL:
            # Include everything.
            pass
        elif self.collection == self.COLLECTION_MAIN:
            # Exclude open-access books with a rating of less than
            # 0.3.
            or_clause = or_(
                LicensePool.open_access==False,
                model.rating >= 0.3
            )
            q = q.filter(or_clause)
        elif self.collection == self.COLLECTION_FEATURED:
            # Exclude books with a rating of less than
            # MINIMUM_FEATURED_QUALITY.
            q = q.filter(
                model.rating >= Configuration.minimum_featured_quality()
            )

        # Set the ORDER BY clause.
        order_by = self.order_by(self.order, work_model, edition_model)
        q = q.order_by(*order_by)

        return q

    def order_by(self, work_model, edition_model):
        """Establish a complete ORDER BY clause for books."""
        if work_model == Work:
            work_id = Work.id
        else:
            work_id = work_model.works_id
        default_sort_order = [
            edition_model.sort_title, edition_model.sort_author, work_id
        ]
    
        primary_order_by = self.order_facet_to_database_field(
            self.order, work_model, edition_model
        )
        if primary_order_by:
            # Promote the field designated by the sort facet to the top of
            # the order-by list.
            order_by = [primary_order_by]

            for i in default_sort_order:
                if i not in order_by:
                    order_by.append(i)
        else:
            # Use the default sort order
            order_by = default_order_by

        # Set each field in the sort order to ascending or descending.
        print [x.name for x in order_by]
        if self.order_ascending:
            order_by = [x.asc() for x in order_by]
        else:
            order_by = [x.desc() for x in order_by]
        return order_by


class Pagination(object):

    DEFAULT_SIZE = 50
    DEFAULT_FEATURED_SIZE = 10

    def __init__(self, offset=0, size=DEFAULT_SIZE):
        self.offset = offset
        self.size = size

    def apply(self, q):
        """Modify the given query with OFFSET and LIMIT."""
        return q.offset(self.offset).limit(self.size)


class Lane(object):

    """A set of books that would go together in a display."""

    UNCLASSIFIED = u"unclassified"
    BOTH_FICTION_AND_NONFICTION = u"both fiction and nonfiction"
    FICTION_DEFAULT_FOR_GENRE = u"fiction default for genre"

    # A book is considered a 'best seller' if it's been seen on
    # the best seller list sometime in the past two years.
    BEST_SELLER_LIST_DURATION = 730

    # Books classified in a subgenre of this lane's genre(s) will
    # be shown in separate lanes.
    IN_SUBLANES = u"separate"

    # Books classified in a subgenre of this lane's genre(s) will be
    # shown in this lane.
    IN_SAME_LANE = u"collapse"

    @property
    def url_name(self):
        """Return the name of this lane to be used in URLs.

        Basically, forward slash is changed to "__". This is necessary
        because Flask tries to route "feed/Suspense%2FThriller" to
        feed/Suspense/Thriller.
        """
        return self.name.replace("/", "__")

    def __repr__(self):
        template = "<Lane name=%(full_name), display=%(display_name), genre=%(genres)s, fiction=%(fiction)s, audience=%(audiences)s, age_range=%(age_range)r, language=%(languages)s, %(sublanes)d sublanes>"
        output = template % dict(
            full_name=self.name,
            display_name=self.display_name,
            genres = "+".join([g.name for g in self.genres]),
            fiction=self.fiction,
            audiences = "+".join(self.audiences),
            age_range = self.age_range,
            language="+".join(self.languages),
            sublanes = len(self.sublanes.lanes)
        )
        return output.encode("ascii", "replace")

    @classmethod
    def from_dict(cls, _db, d, parent_lane):
        """Turn a descriptive dictionary into a Lane object."""
        if isinstance(d, Lane):
            return d

        if d.get('suppress_lane'):
            return None

        name = d.get('name') or d.get('full_name')
        display_name = d.get('display_name')

        genres = []
        for x in d.get('genres', []):
            genre, new = Genre.lookup(_db, x)
            if genre:
                genres.append(genre)
        exclude_genres = []
        for x in d.get('exclude_genres', []):
            genre, new = Genre.lookup(_db, x)
            if genre:
                exclude_genres.append(genre)

        default_audience = None
        default_age_range = None
        default_languages = None
        if parent_lane:
            default_audience = parent_lane.audience
            default_age_range = parent_lane.age_range
            default_languages = parent_lane.languages

        languages = d.get('languages', default_languages)
        if not languages:
            raise ValueError(
                "No guidance as to which languages to use for %s!" % name
            )

        audience = d.get('audience', default_audience)        
        age_range = None
        if 'age_range' in d:
            age_range = d['age_range']
            if not (
                    isinstance(age_range, tuple) 
                    or isinstance(age_range, list)
            ):
                cls.log.warn("Invalid age range for %s: %r", name, age_range)
                age_range = None
        age_range = age_range or default_age_range

        appeal = d.get('appeal')

        subgenre_behavior = d.get('subgenre_behavior', Lane.IN_SUBLANES)
        fiction = d.get('fiction', Lane.FICTION_DEFAULT_FOR_GENRE)
        if fiction == 'default':
            fiction = Lane.FICTION_DEFAULT_FOR_GENRE
        if fiction == 'both':
            fiction = Lane.BOTH_FICTION_AND_NONFICTION        

        lane = Lane(
            _db, full_name=name, display_name=display_name,
            genres=genres, subgenre_behavior=subgenre_behavior,
            fiction=fiction, audience=audience, parent=parent_lane, 
            sublanes=[], appeal=appeal, age_range=age_range,
            exclude_genres=exclude_genres, languages=languages
        )

        # Now create sublanes, recursively.
        sublane_descs = d.get('sublanes', [])
        lane.sublanes = LaneList.from_description(_db, lane, sublane_descs)
            
        return lane

    @classmethod
    def everything(cls, _db, fiction=None,
                   audience=None):
        """Return a synthetic Lane that matches everything."""
        if fiction == True:
            what = 'fiction'
        elif fiction == False:
            what = 'nonfiction'
        else:
            what = 'books'
        if audience == Classifier.AUDIENCE_ADULT:
            what = 'adult ' + what
        elif audience == Classifier.AUDIENCE_YOUNG_ADULT:
            what = 'young adult ' + what
        elif audience == Classifier.AUDIENCE_CHILDREN:
            what = "childrens' " + what
            
        full_name = "All " + what
        return Lane(
            _db, full_name, genres=[], subgenre_behavior=Lane.IN_SAME_LANE,
            fiction=fiction,
            audiences=[audience])

    def __init__(self, 
                 _db, 
                 full_name,
                 genres,
                 subgenre_behavior=IN_SUBLANES,
                 fiction=True,
                 audience=Classifier.AUDIENCE_ADULT,
                 parent=None,
                 sublanes=[],
                 appeal=None,
                 display_name=None,
                 age_range=None,
                 exclude_genres=None,
                 languages=None,
                 media=Edition.BOOK_MEDIUM,
                 format=Edition.ELECTRONIC_FORMAT,
                 list_data_source=None,
                 list_identifier=None,
                 list_seen_in_previous_days=None,
                 ):
        self.sublanes = LaneList.from_description(
            _db, self, sublanes
        )

        self.name = full_name
        self.display_name = display_name or self.name
        self.parent = parent
        self._db = _db
        self.appeal = appeal

        self.age_range = age_range
        self.audiences = self.audience_list_for_age_range(audience, age_range)
        self.languages = languages

        if fiction is None:
            fiction = self.FICTION_DEFAULT_FOR_GENRE

        # The lane may be restricted to items in particular media
        # and/or formats.
        if isinstance(medium, basestring):
            medium = [medium]
        self.media = medium

        if isinstance(format, basestring):
            format = [format]
        self.formats = format

        # The lane may be restricted to books that are on a list
        # from a given data source.
        self.list_data_source, self.lists = self.custom_lists_for_identifier(
            list_data_source, list_identifier)
        self.list_seen_in_previous_days = list_seen_in_previous_days
      
        self.exclude_genres = set()
        if exclude_genres:
            for genre in exclude_genres:
                for l in genre.self_and_subgenres:
                    self.exclude_genres.add(l)
        self.subgenre_behavior = subgenre_behavior

        genres, sublanes = self.gather_genres(genres)
        self.genres, self.fiction = self.gather_matching_genres(
            genres, fiction
        )

        if sublanes:
            if self.sublanes.lanes:
                raise ValueError(
                    "Explicit list of sublanes was provided, but I'm also asked to turn subgenres into sublanes!"
                )
            else:
                self.sublanes = sublanes           

        # Run some sanity checks.
        ch = Classifier.AUDIENCE_CHILDREN
        ya = Classifier.AUDIENCE_YOUNG_ADULT
        if (
                self.age_range 
                and self.audience not in (ch, ya)
                and (not isinstance(self.audience, list)
                     or (ch not in self.audience and ya not in self.audience))
        ):
            raise ValueError(
                "Lane %s specifies age range but does not contain children's or young adult books." % self.name
            )

    def audience_list_for_age_range(self, audience, age_range):
        """Normalize a value for Work.audience based on .age_range

        If you set audience to Young Adult but age_range to 16-18,
        you're saying that books for 18-year-olds (i.e. adults) are
        okay.

        If you set age_range to Young Adult but age_range to 12-15, you're
        saying that books for 12-year-olds (i.e. children) are
        okay.
        """
        if not audience:
            audience = []
        if not isinstance(audience, list):
            audience = [audience]
        audiences = set(audience)
        if not age_range:
            return audiences

        if not isinstance(age_range, list):
            age_range = [age_range]

        if age_range[-1] >= 18:
            audiences.add(Classifier.AUDIENCE_ADULT)
        if age_range[0] < Classifier.YOUNG_ADULT_AGE_CUTOFF:
            audiences.add(Classifier.AUDIENCE_CHILDREN)
        if age_range[0] >= Classifier.YOUNG_ADULT_AGE_CUTOFF:
            audiences.add(Classifier.AUDIENCE_YOUNG_ADULT)
        return audiences      

    def custom_lists_for_identifier(self, list_data_source, list_identifier):
        """Turn a data source and an identifier into a specific list
        of CustomLists.
        """
        if isinstance(list_data_source, basestring):
            list_data_source = DataSource.lookup(self._db, list_data_source)

        # The lane may be restricted to books that are on one or
        # more specific lists.
        if not list_identifier:
            self.lists = None
        elif isinstance(list_identifier, CustomList):
            self.lists = [list_identifier]
        elif (isinstance(list_identifier, list) and
              isinstance(list_identifier[0], CustomList)):
            self.lists = list_identifier
        else:
            if isinstance(list_identifier, basestring):
                list_identifiers = [list_identifier]
            q = _db.query(CustomList).filter(
                CustomList.foreign_identifier.in_(list_identifiers))
            if list_data_source:
                q = q.filter(CustomList.data_source==self.list_data_source)
            lists = q.all()
        return list_data_source, lists

    def gather_genres(self, descriptor):
        """Turn a genre descriptor into a (genres, sublanes) 2-tuple."""
        if descriptor in (None, self.UNCLASSIFIED):
            # We will only be considering works that are not
            # classified under a genre.
            return None, None

        if not isinstance(descriptor, list):
            descriptor = [descriptor]

        genres = []
        sublanes = None
        for orig_genre in descriptor:
            # genre is a Genre database object; genredata is a 
            # non-database GenreData object.
            genre, genredata = self.load_genre(orig_genre)
            if self.exclude_genres and genredata in self.exclude_genres:
                continue

            genres.append(genre)
            if self.subgenre_behavior:
                if not genredata:
                    raise ValueError(
                        "Couldn't turn %r into GenreData object to find subgenres." % 
                        orig_genre
                    )

                if self.subgenre_behavior == self.IN_SAME_LANE:
                    # All subgenres of this genre go into the same
                    # lane as their parent.
                    for subgenre_data in genredata.all_subgenres:
                        subgenre, ignore = Genre.lookup(_db, subgenre_data)
                        # Incorporate this genre's subgenres,
                        # recursively, in this lane.
                        if (not self.exclude_genres
                            or subgenre_data not in self.exclude_genres):
                            genres.append(subgenre)
                elif self.subgenre_behavior == self.IN_SUBLANES:
                    # Each subgenre of this genre goes into its own sublane.
                    sublanes = LaneList.from_description(
                        _db, self, genredata.subgenres)
                else:
                    raise ValueError(
                        "Unknown subgenre behavior: %r" % self.subgenre_behavior
                    )
        return genres, sublanes

    def load_genre(self, descriptor):
        """Turn some kind of genre descriptor into a Genre object."""
        if isinstance(genre, tuple):
            if len(genre) == 2:
                genre, subgenres = genre
            else:
                genre, subgenres, audience_restriction = genre
            return genre

        if isinstance(genre, GenreData):
            genredata = genre
        else:
            if isinstance(genre, Genre):
                genre_name = genre.name
            else:
                genre_name = genre
            # It's in the database--just make sure it's not an old entry
            # that shouldn't be in the database anymore.
            genredata = classifier.genres.get(genre_name)
        if not isinstance(genre, Genre):
            genre, ignore = Genre.lookup(_db, genre)
        return genre

    def all_matching_genres(self, genres):
        genres = set()
        if genres:
            for genre in genres:
                genres = genres.union(genre.self_and_subgenres)
        return genres

    def gather_matching_genres(self, genres, fiction):
        """Find all subgenres of the given gnres which match the given fiction
        status.
        
        This may also turn into an additional restriction (or
        liberation) on the fiction status
        """
        fiction_default_by_genre = (fiction == self.FICTION_DEFAULT_FOR_GENRE)
        if fiction_default_by_genre:
            # Unset `fiction`. We'll set it again when we find out
            # whether we've got fiction or nonfiction genres.
            fiction = None
        genres = self.all_matching_genres(genres)
        for genre in genres:
            if fiction_default_by_genre:
                if fiction is None:
                    fiction = genre.default_fiction
                elif fiction != genre.default_fiction:
                    raise ValueError(
                        "I was told to use the default fiction restriction, but the genres %r include contradictory fiction restrictions."
                    )
        if fiction is None:
            # This is an impossible situation. Rather than eliminate all books
            # from consideration, allow both fiction and nonfiction.
            fiction = self.BOTH_FICTION_AND_NONFICTION
        return genres, fiction

    def works(self):
        """Find Works that will go together in this Lane.

        Works will:

        * Be in one of the languages listed in `languages`.

        * Be filed under of the genres listed in `self.genres` (or, if
          `self.include_subgenres` is True, any of those genres'
          subgenres).

        * Have the same appeal as `self.appeal`, if `self.appeal` is present.

        * Are intended for the audience in `self.audience`.

        * Are fiction (if `self.fiction` is True), or nonfiction (if fiction
          is false), or of the default fiction status for the genre
          (if fiction==FICTION_DEFAULT_FOR_GENRE and all genres have
          the same default fiction status). If fiction==None, no fiction
          restriction is applied.

        * Have a delivery mechanism that can be rendered by the
          default client.
        """

        q = _db.query(Work).join(Work.primary_edition)
        q = q.join(Work.license_pools).join(LicensePool.data_source).join(
            LicensePool.identifier
        )
        q = q.options(
            contains_eager(Work.license_pools),
            contains_eager(Work.primary_edition),
            contains_eager(Work.license_pools, LicensePool.data_source),
            contains_eager(Work.license_pools, LicensePool.edition),
            contains_eager(Work.license_pools, LicensePool.identifier),
            defer(Work.verbose_opds_entry),
            defer(Work.primary_edition, Edition.extra),
            defer(Work.license_pools, LicensePool.edition, Edition.extra),
        )

        if self.genres:
            q = q.join(Work.work_genres)
            q = q.options(contains_eager(Work.work_genres))
            q = q.filter(WorkGenre.genre_id.in_([g.id for g in self.genres]))

        q = self.apply_filters(q, Work, Edition)

    def materialized_works(self):
        """Find MaterializedWorks that will go together in this Lane."""
        from model import (
            MaterializedWork,
            MaterializedWorkWithGenre,
        )
        if self.genres:
            mw =MaterializedWorkWithGenre
            q = self._db.query(mw)
            q = q.filter(mw.genre_id.in_([g.id for g in genres]))            
        else:
            mw = MaterializedWork
            q = self._db.query(mw)

        # Avoid eager loading of objects that are contained in the 
        # materialized view.
        q = q.options(
            lazyload(mw.license_pool, LicensePool.data_source),
            lazyload(mw.license_pool, LicensePool.identifier),
            lazyload(mw.license_pool, LicensePool.edition),
        )

        q = q.join(LicensePool, LicensePool.id==mw.license_pool_id)
        q = q.options(contains_eager(mw.license_pool))
        q = self.apply_filters(q, mw, mw)

    def apply_filters(cls, q, work_model=Work, edition_model=Edition):
        """Apply filters to a base query against Work or a materialized view.

        :param work_model: Either Work, MaterializedWork, or MaterializedWorkWithGenre
        :param edition_model: Either Edition, MaterializedWork, or MaterializedWorkWithGenre
        """
        if self.languages:
            q = q.filter(edition_model.language.in_(self.languages))

        if self.audiences:
            q = q.filter(work_model.audience.in_(audiences))
            if (Classifier.AUDIENCE_CHILDREN in self.audience
                or Classifier.AUDIENCE_YOUNG_ADULT in self.audience):
                    gutenberg = DataSource.lookup(
                        self._db, DataSource.GUTENBERG)
                    # TODO: A huge hack to exclude Project Gutenberg
                    # books (which were deemed appropriate for
                    # pre-1923 children but are not necessarily so for
                    # 21st-century children.)
                    #
                    # This hack should be removed in favor of a
                    # whitelist system and some way of allowing adults
                    # to see books aimed at pre-1923 children.
                    q = q.filter(edition_model.data_source_id != gutenberg.id)

        if self.appeal != None:
            q = q.filter(work_model.primary_appeal==self.appeal)

        if self.age_range != None:
            if (Classifier.AUDIENCE_ADULT in audiences
                or Classifier.AUDIENCE_ADULTS_ONLY in audiences):
                # Books for adults don't have target ages. If we're including
                # books for adults, allow the target age to be empty.
                audience_has_no_target_age = work_model.target_age == None
            else:
                audience_has_no_target_age = False

            age_range = sorted(self.age_range)
            if len(age_range) == 1:
                # The target age must include this number.
                r = NumericRange(age_range[0], age_range[0], '[]')
                q = q.filter(
                    or_(
                        work_model.target_age.contains(r),
                        audience_has_no_target_age
                    )
                )
            else:
                # The target age range must overlap this age range
                r = NumericRange(age_range[0], age_range[-1], '[]')
                q = q.filter(
                    or_(
                        work_model.target_age.overlaps(r),
                        audience_has_no_target_age
                    )
                )

        if self.fiction == self.UNCLASSIFIED:
            q = q.filter(work_model.fiction==None)
        elif self.fiction != self.BOTH_FICTION_AND_NONFICTION:
            q = q.filter(work_model.fiction==fiction)

        q = q.filter(edition_model.medium.in_(self.media))
        
        # TODO: Also filter on formats.

        # TODO: Only find works with unsuppressed LicensePools.

        # Only find unmerged presentation-ready works.
        q = q.filter(
            work_model.was_merged_into == None,
            work_model.presentation_ready == True,
        )

        # Only find books the default client can fulfill.
        q = q.filter(LicensePool.delivery_mechanisms.any(
            DeliveryMechanism.default_client_can_fulfill==True)
        )
        return q


    def search(self, languages, query, search_client, limit=30):
        """Find works in this lane that match a search query.
        """        
        if isinstance(languages, basestring):
            languages = [languages]

        if self.fiction in (True, False):
            fiction = self.fiction
        else:
            fiction = None

        results = None
        if search_client:
            docs = None
            a = time.time()
            try:
                docs = search_client.query_works(
                    query, Edition.BOOK_MEDIUM, languages, fiction,
                    self.audience,
                    self.all_matching_genres,
                    fields=["_id", "title", "author", "license_pool_id"],
                    limit=limit
                )
            except elasticsearch.exceptions.ConnectionError, e:
                logging.error(
                    "Could not connect to Elasticsearch; falling back to database search."
                )
            b = time.time()
            logging.debug("Elasticsearch query completed in %.2fsec", b-a)

            results = []
            if docs:
                doc_ids = [
                    int(x['_id']) for x in docs['hits']['hits']
                ]
                if doc_ids:
                    from model import MaterializedWork as mw
                    q = self._db.query(mw).filter(mw.works_id.in_(doc_ids))
                    q = q.options(
                        lazyload(mw.license_pool, LicensePool.data_source),
                        lazyload(mw.license_pool, LicensePool.identifier),
                        lazyload(mw.license_pool, LicensePool.edition),
                    )
                    work_by_id = dict()
                    a = time.time()
                    works = q.all()
                    for mw in works:
                        work_by_id[mw.works_id] = mw
                    results = [work_by_id[x] for x in doc_ids if x in work_by_id]
                    b = time.time()
                    logging.debug(
                        "Obtained %d MaterializedWork objects in %.2fsec",
                        len(results), b-a
                    )

        if not results:
            results = self._search_database(languages, fiction, query).limit(limit)
        return results

    def _search_database(self, languages, fiction, query):
        """Do a really awful database search for a book using ILIKE.

        This is useful if an app server has no external search
        interface defined, or if the search interface isn't working
        for some reason.
        """
        k = "%" + query + "%"
        q = self.works().filter(
            or_(Edition.title.ilike(k),
                Edition.author.ilike(k)))
        #q = q.order_by(Work.quality.desc())
        return q

    def featured_works(self, size):
        """Find a random sample of `size` featured books.

        It's semi-okay for this to be slow, since it will only be run to
        create cached feeds.

        :return: A list of MaterializedWork or MaterializedWorkWithGenre
        objects.
        """
        books = []
        # Prefer to feature available books in the featured
        # collection, but if that fails, gradually degrade to
        # featuring all books, no matter what the availability.
        for (collection, availability) in (
                (Facets.COLLECTION_FEATURED, Facets.AVAILABLE_NOW),
                (Facets.COLLECTION_FEATURED, Facets.AVAILABLE_ALL),
                (Facets.COLLECTION_MAIN, Facets.AVAILABLE_NOW),
                (Facets.COLLECTION_MAIN, Facets.AVAILABLE_ALL),
                (Facets.COLLECTION_FULL, Facets.AVAILABLE_ALL),
        ):
            facets = Facets(collection=collection, availability=availability,
                            order=Facets.ORDER_RANDOM)
            desperate = (collection==Facets.COLLECTION_FULL
                         and availability == Facets.AVAILABLE_ALL)
            books = self.featured_works_for_facets(facets, size, desperate)
            if books:
                break
        return books

    def featured_works_for_facets(facets, size, desperate=False):
        """Find a random sample of `size` featured books matching
        the given facets.
        """
        query = self.materialized_works(facets)
        total_size = query.count()
        if total_size >= needed:
            # There are enough results that we can take a random
            # sample.
            offset = random.randint(0, total_size-size)
        else:
            if desperate:
                # There are not enough results that we can take a
                # random sample. But we're desperate. Use these books.
                offset = 0
            else:
                # We're not desperate. Just return nothing.
                return []
        works = query.offset(offset).limit(size).all()
        return random.shuffle(works)


class LaneList(object):
    """A list of lanes such as you might see in an OPDS feed."""

    log = logging.getLogger("Lane list")

    def __repr__(self):
        parent = ""
        if self.parent:
            parent = "parent=%s, " % self.parent.name

        return "<LaneList: %slanes=[%s]>" % (
            parent,
            ", ".join([repr(x) for x in self.lanes])
        )       

    @classmethod
    def from_description(cls, _db, parent_lane, description):
        lanes = LaneList(parent_lane)
        if parent_lane:
            default_fiction = parent_lane.fiction
            default_audience = parent_lane.audience
        else:
            default_fiction = Lane.FICTION_DEFAULT_FOR_GENRE
            default_audience = Classifier.AUDIENCES_ADULT

        description = description or []
        for lane_description in description:
            display_name=None
            if isinstance(lane_description, basestring):
                lane_description = classifier.genres[lane_description]
            elif isinstance(lane_description, tuple):
                if len(lane_description) == 2:
                    name, subdescriptions = lane_description
                elif len(lane_description) == 3:
                    name, subdescriptions, audience_restriction = lane_description
                    if (parent_lane and audience_restriction and 
                        parent_lane.audience and
                        parent_lane.audience != audience_restriction
                        and not audience_restriction in parent_lane.audience):
                        continue
                lane_description = classifier.genres[name]
            if isinstance(lane_description, dict):
                lane = Lane.from_dict(_db, lane_description, parent_lane)
            elif isinstance(lane_description, Genre):
                lane = Lane(_db, lane_description.name, [lane_description],
                            Lane.IN_SAME_LANE, default_fiction,
                            default_audience, parent_lane,
                            sublanes=genre.subgenres)
            elif isinstance(lane_description, GenreData):
                # This very simple lane is the default view for a genre.
                genre = lane_description
                lane = Lane(_db, genre.name, [genre], Lane.IN_SUBLANES,
                            default_fiction,
                            default_audience, parent_lane)
            elif isinstance(lane_description, Lane):
                # The Lane object has already been created.
                lane = lane_description
                lane.parent = parent_lane

            def _add_recursively(l):
                lanes.add(l)
                sublanes = l.sublanes.lanes
                for sl in sublanes:
                    _add_recursively(sl)
            if lane:
                _add_recursively(lane)

        return lanes

    def __init__(self, parent=None):
        self.parent = parent
        self.lanes = []
        self.by_name = dict()

    def __len__(self):
        return len(self.lanes)

    def __iter__(self):
        return self.lanes.__iter__()

    def add(self, lane):
        if lane.parent == self.parent:
            self.lanes.append(lane)
        if lane.name in self.by_name and self.by_name[lane.name] is not lane:
            raise ValueError("Duplicate lane: %s" % lane.name)
        self.by_name[lane.name] = lane


