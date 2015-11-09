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

class Lane(object):

    """A set of books that would go together in a display."""

    UNCLASSIFIED = u"unclassified"
    BOTH_FICTION_AND_NONFICTION = u"both fiction and nonfiction"
    FICTION_DEFAULT_FOR_GENRE = u"fiction default for genre"

    # Books classified in a subgenre of this lane's genre(s) will
    # be shown in separate lanes.
    IN_SUBLANES = u"separate"

    # Books classified in a subgenre of this lane's genre(s) will be
    # shown in this lane.
    IN_SAME_LANE = u"collapse"

    def __repr__(self):
        if self.sublanes.lanes:
            sublanes = " (sublanes=%d)" % len(self.sublanes.lanes)
        else:
            sublanes = ""
        return "<Lane %s%s>" % (self.name, sublanes)

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
        if parent_lane:
            default_audience = parent_lane.audience
            default_age_range = parent_lane.age_range
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
            exclude_genres=exclude_genres
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
            audience=audience)

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
                 list_data_source=None,
                 list_identifier=None,
                 ):
        self.name = full_name
        self.display_name = display_name or self.name
        self.parent = parent
        self._db = _db
        self.appeal = appeal
        self.age_range = age_range
        self.fiction = fiction
        self.audience = audience

        self.exclude_genres = set()
        if exclude_genres:
            for genre in exclude_genres:
                for l in genre.self_and_subgenres:
                    self.exclude_genres.add(l)
        self.subgenre_behavior=subgenre_behavior
        self.sublanes = LaneList.from_description(_db, self, sublanes)

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

        if genres in (None, self.UNCLASSIFIED):
            # We will only be considering works that are not
            # classified under a genre.
            self.genres = None
            self.subgenre_behavior
        else:
            if not isinstance(genres, list):
                genres = [genres]

            # Turn names or GenreData objects into Genre objects. 
            self.genres = []
            for genre in genres:
                if isinstance(genre, tuple):
                    if len(genre) == 2:
                        genre, subgenres = genre
                    else:
                        genre, subgenres, audience_restriction = genre
                if isinstance(genre, GenreData):
                    genredata = genre
                else:
                    if isinstance(genre, Genre):
                        genre_name = genre.name
                    else:
                        genre_name = genre
                    genredata = classifier.genres.get(genre_name)
                if not isinstance(genre, Genre):
                    genre, ignore = Genre.lookup(_db, genre)

                if exclude_genres and genredata in exclude_genres:
                    continue
                self.genres.append(genre)
                if subgenre_behavior:
                    if not genredata:
                        raise ValueError("Couldn't turn %r into GenreData object to find subgenres." % genre)

                    if subgenre_behavior == self.IN_SAME_LANE:
                        for subgenre_data in genredata.all_subgenres:
                            subgenre, ignore = Genre.lookup(_db, subgenre_data)
                            # Incorporate this genre's subgenres,
                            # recursively, in this lane.
                            if not exclude_genres or subgenre_data not in exclude_genres:
                                self.genres.append(subgenre)
                    elif subgenre_behavior == self.IN_SUBLANES:
                        if self.sublanes.lanes:
                            raise ValueError(
                                "Explicit list of sublanes was provided, but I'm also asked to turn subgenres into sublanes!")
                        self.sublanes = LaneList.from_description(
                                _db, self, genredata.subgenres)


    @property
    def url_name(self):
        """Return the name of this lane to be used in URLs.

        Basically, forward slash is changed to "__". This is necessary
        because Flask tries to route "feed/Suspense%2FThriller" to
        feed/Suspense/Thriller.
        """
        return self.name.replace("/", "__")

    @property
    def all_matching_genres(self):
        genres = set()
        if self.genres:
            for genre in self.genres:
                #if self.subgenre_behavior == self.IN_SAME_LANE:
                genres = genres.union(genre.self_and_subgenres)
                #else:
                #    genres.add(genre)
        return genres

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

    def gather_matching_genres(self, fiction):
        """Find all subgenres managed by this lane which match the
        given fiction status.
        
        This may also turn into an additional restriction (or
        liberation) on the fiction status
        """
        fiction_default_by_genre = (fiction == self.FICTION_DEFAULT_FOR_GENRE)
        if fiction_default_by_genre:
            # Unset `fiction`. We'll set it again when we find out
            # whether we've got fiction or nonfiction genres.
            fiction = None
        genres = self.all_matching_genres
        for genre in self.genres:
            if fiction_default_by_genre:
                if fiction is None:
                    fiction = genre.default_fiction
                elif fiction != genre.default_fiction:
                    raise ValueError(
                        "I was told to use the default fiction restriction, but the genres %r include contradictory fiction restrictions.")
        if fiction is None:
            # This is an impossible situation. Rather than eliminate all books
            # from consideration, allow both fiction and nonfiction.
            fiction = self.BOTH_FICTION_AND_NONFICTION
        return genres, fiction

    def works(self, languages, fiction=None, availability=Work.ALL):
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

        :param fiction: Override the fiction setting found in `self.fiction`.

        """
        hold_policy = Configuration.hold_policy()
        if (availability == Work.ALL and 
            hold_policy == Configuration.HOLD_POLICY_HIDE):
            # Under normal circumstances we would show all works, but
            # site configuration says to hide books that aren't
            # available.
            availability = Work.CURRENTLY_AVAILABLE

        q = Work.feed_query(self._db, languages, availability)
        audience = self.audience
        if fiction is None:
            if self.fiction is not None:
                fiction = self.fiction
            else:
                fiction = self.FICTION_DEFAULT_FOR_GENRE

        #if self.genres is None and fiction in (True, False, self.UNCLASSIFIED):
        #    # No genre plus a boolean value for `fiction` means
        #    # fiction or nonfiction not associated with any genre.
        #    q = Work.with_no_genres(q)
        if self.genres is not None:
            genres, fiction = self.gather_matching_genres(fiction)
            # logging.debug("Genres: %s" % ", ".join([x.name for x in genres]))
            if genres:
                q = q.join(Work.work_genres)
                q = q.options(contains_eager(Work.work_genres))
                q = q.filter(WorkGenre.genre_id.in_([g.id for g in genres]))

        if self.audience != None:
            audiences = self.audience_list_for_age_range(
                self.audience, self.age_range)
            q = q.filter(Work.audience.in_(audiences))
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
                    q = q.filter(Edition.data_source_id != gutenberg.id)

        if self.appeal != None:
            q = q.filter(Work.primary_appeal==self.appeal)

        if self.age_range != None:
            if (Classifier.AUDIENCE_ADULT in audiences
                or Classifier.AUDIENCE_ADULTS_ONLY in audiences):
                # Books for adults don't have target ages. If we're including
                # books for adults, allow the target age to be empty.
                audience_has_no_target_age = Work.target_age == None
            else:
                audience_has_no_target_age = False

            age_range = sorted(self.age_range)
            if len(age_range) == 1:
                # The target age must include this number.
                r = NumericRange(age_range[0], age_range[0], '[]')
                q = q.filter(
                    or_(
                        Work.target_age.contains(r),
                        audience_has_no_target_age
                    )
                )
            else:
                # The target age range must overlap this age range
                r = NumericRange(age_range[0], age_range[-1], '[]')
                q = q.filter(
                    or_(
                        Work.target_age.overlaps(r),
                        audience_has_no_target_age
                    )
                )

        if fiction == self.UNCLASSIFIED:
            q = q.filter(Work.fiction==None)
        elif fiction != self.BOTH_FICTION_AND_NONFICTION:
            q = q.filter(Work.fiction==fiction)

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
        q = self.works(languages=languages, fiction=fiction).filter(
            or_(Edition.title.ilike(k),
                Edition.author.ilike(k)))
        q = q.order_by(Work.quality.desc())
        return q

    def quality_sample(
            self, languages, quality_min_start,
            quality_min_rock_bottom, target_size, availability,
            random_sample=True):
        """Randomly select Works from this Lane that meet minimum quality
        criteria.

        Bring the quality criteria as low as necessary to fill a feed
        of the given size, but not below `quality_min_rock_bottom`.
        """
        if isinstance(languages, basestring):
            languages = [languages]

        quality_min = quality_min_start
        previous_quality_min = None
        results = []
        while (quality_min >= quality_min_rock_bottom
               and len(results) < target_size):
            remaining = target_size - len(results)
            query = self.works(languages=languages, availability=availability)
            total_size = query.count()
            if quality_min < 0.05:
                quality_min = 0

            query = query.filter(
                Work.quality >= quality_min,
            )
            if previous_quality_min is not None:
                query = query.filter(
                    Work.quality < previous_quality_min)

            query_without_random_sample = query

            if random_sample:
                offset = random.random()
                # logging.debug("Random offset=%.2f", offset)
                if offset < 0.5:
                    query = query.filter(Work.random >= offset)
                else:
                    query = query.filter(Work.random <= offset)

            start = time.time()
            # logging.debug(dump_query(query))
            max_results = int(remaining*1.3)
            query = query.limit(max_results)
            r = query.all()

            if random_sample and len(r) < (remaining-5):
                # Disable the random sample--there are not enough works for
                # it to operate properly.
                query = query_without_random_sample.limit(max_results)
                r = query.all()


            #for i in r[:remaining]:
            #    logging.debug("%s (random=%.2f quality=%.2f)", i.title, i.random, i.quality)
            results.extend(r[:remaining])

            if quality_min == quality_min_rock_bottom or quality_min == 0:
                # We can't lower the bar any more.
                break

            # Lower the bar, in case we didn't get enough results.
            previous_quality_min = quality_min

            if results or quality_min_rock_bottom < 0.1:
                quality_min *= 0.5
            else:
                # We got absolutely no results. Lower the bar all the
                # way immediately.
                quality_min = quality_min_rock_bottom

            if quality_min < quality_min_rock_bottom:
                quality_min = quality_min_rock_bottom

        logging.debug(
            "%s: %s Quality %.2f got us to %d results in %.2fsec",
            self.name, availability, quality_min, len(results), 
            time.time()-start
        )
        return results

    def materialized_works(self, languages=None, fiction=None, 
                           availability=Work.ALL):
        """Find MaterializedWorks that will go together in this Lane."""
        audience = self.audience

        from model import (
            MaterializedWork,
            MaterializedWorkWithGenre,
        )

        if fiction is None:
            if self.fiction is not None:
                fiction = self.fiction
            else:
                fiction = self.FICTION_DEFAULT_FOR_GENRE
        genres = []
        if self.genres is not None:
            genres, fiction = self.gather_matching_genres(fiction)

        if genres:
            mw =MaterializedWorkWithGenre
            q = self._db.query(mw)
            q = q.filter(mw.genre_id.in_([g.id for g in genres]))            
        else:
            mw = MaterializedWork
            q = self._db.query(mw)
        
        q = q.with_labels()

        if languages:
            q = q.filter(mw.language.in_(languages))

        # Avoid eager loading of objects that are contained in the 
        # materialized view.
        q = q.options(
            lazyload(mw.license_pool, LicensePool.data_source),
            lazyload(mw.license_pool, LicensePool.identifier),
            lazyload(mw.license_pool, LicensePool.edition),
        )
        if self.audience != None:
            audiences = self.audience_list_for_age_range(
                self.audience, self.age_range)
            if audiences:
                q = q.filter(mw.audience.in_(audiences))
                if (Classifier.AUDIENCE_CHILDREN in audiences 
                    or Classifier.AUDIENCE_YOUNG_ADULT in audiences):
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
                    q = q.filter(mw.data_source_id != gutenberg.id)

        if self.age_range != None:
            if (Classifier.AUDIENCE_ADULT in audiences
                or Classifier.AUDIENCE_ADULTS_ONLY in audiences):
                # Books for adults don't have target ages. If we're including
                # books for adults, allow the target age to be empty.
                audience_has_no_target_age = mw.target_age == None
            else:
                audience_has_no_target_age = False

            age_range = self.age_range
            if isinstance(age_range, int):
                age_range = [age_range]
            age_range = sorted(self.age_range)
            if len(age_range) == 1:
                # The target age must include this number.
                r = NumericRange(age_range[0], age_range[0], '[]')
                q = q.filter(
                    or_(
                        mw.target_age.contains(r), 
                        audience_has_no_target_age
                    )
                )
            else:
                # The target age range must overlap this age range
                r = NumericRange(age_range[0], age_range[-1], '[]')
                q = q.filter(
                    or_(
                        mw.target_age.overlaps(r),
                        audience_has_no_target_age
                    )
                )

        if fiction == self.UNCLASSIFIED:
            q = q.filter(mw.fiction==None)
        elif fiction != self.BOTH_FICTION_AND_NONFICTION:
            q = q.filter(mw.fiction==fiction)
        q = q.join(LicensePool, LicensePool.id==mw.license_pool_id)
        q = q.options(contains_eager(mw.license_pool))
        return q


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


