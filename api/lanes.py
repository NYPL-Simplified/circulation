from nose.tools import set_trace
from sqlalchemy import or_
from sqlalchemy.orm import aliased
from flask_babel import lazy_gettext as _
import time
import elasticsearch
import logging

import core.classifier as genres
from config import Configuration
from core.classifier import (
    Classifier,
    fiction_genres,
    nonfiction_genres,
    GenreData,
)
from core import classifier

from core.lane import (
    Facets,
    Pagination,
    Lane,
    WorkList,
)
from core.model import (
    get_one,
    create,
    Contribution,
    Contributor,
    DataSource,
    Edition,
    ExternalIntegration,
    LicensePool,
    Session,
    Work,
)

from core.util import LanguageCodes
from novelist import NoveListAPI

def load_lanes(_db, library):
    """Return a WorkList that reflects the current lane structure of the
    Library.

    If no top-level visible lanes are configured, the WorkList will be
    configured to show every book in the collection.

    If a single top-level Lane is configured, it will returned as the
    WorkList.

    Otherwise, a WorkList containing the visible top-level lanes is
    returned.
    """

    # Load all Lane objects from the database.
    lanes = _db.query(Lane).filter(Lane.library==library).order_by(Lane.priority)

    # But only the visible top-level Lanes go into the WorkList.
    top_level_lanes = [x for x in lanes if not x.parent and x.visible]

    # Expunge the Lanes from the database before they go into the WorkList,
    # since it will be used across sessions.
    map(_db.expunge, top_level_lanes)

    if len(top_level_lanes) == 1:
        return top_level_lanes[0]

    wl = WorkList()
    wl.initialize(
        library, display_name=_("Collection"), children=top_level_lanes
    )
    return wl

def create_default_lanes(_db, library):
    """Reset the lanes for the given library to the default.

    The database will have the following top-level lanes for
    each large-collection:
    'Adult Fiction', 'Adult Nonfiction', 'Young Adult Fiction',
    'Young Adult Nonfiction', and 'Children'.
    Each lane contains additional sublanes.
    If an NYT integration is configured, there will also be a
    'Best Sellers' top-level lane.

    The database will also have a top-level lane named after each
    small-collection language. Each such sublane contains "Adult
    Fiction", "Adult Nonfiction", and "Children/YA" sublanes.

    Finally the database includes an "Other Languages" top-level lane
    which covers all other languages known to the collection.

    If run on a Library that already has Lane configuration, this can
    be an extremely destructive method. All new Lanes will be visible
    and all Lanes based on CustomLists (but not the CustomLists
    themselves) will be destroyed.
    """
    # Delete existing lanes.
    for lane in _db.query(Lane).filter(Lane.library_id==library.id):
        _db.delete(lane)

    top_level_lanes = []

    large = Configuration.large_collection_languages(library) or []
    small = Configuration.small_collection_languages(library) or []
    tiny = Configuration.tiny_collection_languages(library) or []

    # If there are no language configuration settings, estimate the
    # current collection size to determine the lanes.
    if not large and not small and not tiny:
        estimates = library.estimated_holdings_by_language()
        [(ignore, largest)] = estimates.most_common(1)
        for language, count in estimates.most_common():
            if count > largest * 0.1:
                large.append(language)
            elif count > largest * 0.01:
                small.append(language)
            else:
                tiny.append(language)

    priority = 0
    for language in large:
        priority = create_lanes_for_large_collection(_db, library, language, priority=priority)

    for language in small:
        priority = create_lane_for_small_collection(_db, library, language, priority=priority)

    other_languages_lane = create_lane_for_tiny_collections(
        _db, library, tiny, priority=priority
    )

def lane_from_genres(_db, library, genres, display_name=None,
                     exclude_genres=None, priority=0, audiences=None, **extra_args):
    """Turn genre info into a Lane object."""

    genre_lane_instructions = {
        "Dystopian SF": dict(display_name="Dystopian"),
        "Erotica": dict(audiences=[Classifier.AUDIENCE_ADULTS_ONLY]),
        "Humorous Fiction" : dict(display_name="Humor"),
        "Media Tie-in SF" : dict(display_name="Movie and TV Novelizations"),
        "Suspense/Thriller" : dict(display_name="Thriller"),
        "Humorous Nonfiction" : dict(display_name="Humor"),
        "Political Science" : dict(display_name="Politics & Current Events"),
        "Periodicals" : dict(visible=False)
    }

    # Create sublanes first.
    sublanes = []
    for genre in genres:
        if isinstance(genre, dict):
            sublane_priority = 0
            for subgenre in genre.get("subgenres", []):
                sublanes.append(lane_from_genres(
                        _db, library, [subgenre], 
                        priority=sublane_priority, **extra_args))
                sublane_priority += 1

    # Now that we have sublanes we don't care about subgenres anymore.
    genres = [genre.get("name") if isinstance(genre, dict)
              else genre.name if isinstance(genre, GenreData)
              else genre
              for genre in genres]

    exclude_genres = [genre.get("name") if isinstance(genre, dict)
                      else genre.name if isinstance(genre, GenreData)
                      else genre
                      for genre in exclude_genres or []]

    fiction = None
    visible = True
    if len(genres) == 1:
        if classifier.genres.get(genres[0]):
            genredata = classifier.genres[genres[0]]
        else:
            genredata = GenreData(genres[0], False)
        fiction = genredata.is_fiction

        if genres[0] in genre_lane_instructions.keys():
            instructions = genre_lane_instructions[genres[0]]
            if not display_name and "display_name" in instructions:
                display_name = instructions.get('display_name')
            if "audiences" in instructions:
                audiences = instructions.get("audiences")
            if "visible" in instructions:
                visible = instructions.get("visible")

    if not display_name:
        display_name = ", ".join(sorted(genres))

    lane, ignore = create(_db, Lane, library_id=library.id,
                          display_name=display_name,
                          fiction=fiction, audiences=audiences,
                          sublanes=sublanes, priority=priority,
                          **extra_args)
    lane.visible = visible
    for genre in genres:
        lane.add_genre(genre)
    for genre in exclude_genres:
        lane.add_genre(genre, inclusive=False)
    return lane

def create_lanes_for_large_collection(_db, library, languages, priority=0):
    """Ensure that the lanes appropriate to a large collection are all
    present.

    This means:

    * A "%(language)s Adult Fiction" lane containing sublanes for each fiction
    genre.

    * A "%(language)s Adult Nonfiction" lane containing sublanes for
    each nonfiction genre.

    * A "%(language)s YA Fiction" lane containing sublanes for the
      most popular YA fiction genres.
    
    * A "%(language)s YA Nonfiction" lane containing sublanes for the
      most popular YA fiction genres.
    
    * A "%(language)s Children and Middle Grade" lane containing
      sublanes for childrens' books at different age levels.

    :param library: Newly created lanes will be associated with this
        library.
    :param languages: Newly created lanes will contain only books
        in these languages.
    :return: A list of top-level Lane objects.

    TODO: If there are multiple large collections, their top-level lanes do
    not have distinct display names.
    """
    if isinstance(languages, basestring):
        languages = [languages]

    ADULT = Classifier.AUDIENCES_ADULT
    YA = [Classifier.AUDIENCE_YOUNG_ADULT]
    CHILDREN = [Classifier.AUDIENCE_CHILDREN]

    include_best_sellers = False
    nyt_data_source = DataSource.lookup(_db, DataSource.NYT)
    nyt_integration = get_one(
        _db, ExternalIntegration,
        goal=ExternalIntegration.METADATA_GOAL,
        protocol=ExternalIntegration.NYT,
    )
    if nyt_integration:
        include_best_sellers = True

    language_identifier = LanguageCodes.name_for_languageset(languages)

    sublanes = []
    if include_best_sellers:
        best_sellers, ignore = create(
            _db, Lane, library=library,
            display_name="Best Sellers",
            priority=priority,
            languages=languages
        )
        priority += 1
        best_sellers.list_datasource = nyt_data_source
        sublanes.append(best_sellers)

    adult_common_args = dict(
        languages=languages,
        audiences=ADULT,
    )

    adult_fiction_sublanes = []
    adult_fiction_priority = 0
    if include_best_sellers:
        adult_fiction_best_sellers, ignore = create(
            _db, Lane, library=library,
            display_name="Best Sellers",
            fiction=True,
            priority=adult_fiction_priority,
            **adult_common_args
        )
        adult_fiction_priority += 1
        adult_fiction_best_sellers.list_datasource = nyt_data_source
        adult_fiction_sublanes.append(adult_fiction_best_sellers)

    for genre in fiction_genres:
        if isinstance(genre, basestring):
            genre_name = genre
        else:
            genre_name = genre.get("name")
        genre_lane = lane_from_genres(
            _db, library, [genre],
            priority=adult_fiction_priority,
            **adult_common_args)
        adult_fiction_priority += 1
        adult_fiction_sublanes.append(genre_lane)

    adult_fiction, ignore = create(
        _db, Lane, library=library,
        display_name="Fiction",
        genres=[],
        sublanes=adult_fiction_sublanes,
        fiction=True,
        priority=priority,
        **adult_common_args
    )
    priority += 1
    sublanes.append(adult_fiction)

    adult_nonfiction_sublanes = []
    adult_nonfiction_priority = 0
    if include_best_sellers:
        adult_nonfiction_best_sellers, ignore = create(
            _db, Lane, library=library,
            display_name="Best Sellers",
            fiction=False,
            priority=adult_nonfiction_priority,
            **adult_common_args
        )
        adult_nonfiction_priority += 1
        adult_nonfiction_best_sellers.list_datasource = nyt_data_source
        adult_nonfiction_sublanes.append(adult_nonfiction_best_sellers)

    for genre in nonfiction_genres:
        # "Life Strategies" is a YA-specific genre that should not be
        # included in the Adult Nonfiction lane.
        if genre != genres.Life_Strategies:
            if isinstance(genre, basestring):
                genre_name = genre
            else:
                genre_name = genre.get("name")
            genre_lane = lane_from_genres(
                _db, library, [genre],
                priority=adult_nonfiction_priority,
                **adult_common_args)
            adult_nonfiction_priority += 1
            adult_nonfiction_sublanes.append(genre_lane)

    adult_nonfiction, ignore = create(
        _db, Lane, library=library,
        display_name="Nonfiction",
        genres=[],
        sublanes=adult_nonfiction_sublanes,
        fiction=False,
        priority=priority,
        **adult_common_args
    )
    priority += 1
    sublanes.append(adult_nonfiction)

    ya_common_args = dict(
        audiences=YA,
        languages=languages,
    )

    ya_fiction, ignore = create(
        _db, Lane, library=library,
        display_name="Young Adult Fiction",
        genres=[], fiction=True,
        sublanes=[],
        priority=priority,
        **ya_common_args
    )
    priority += 1
    sublanes.append(ya_fiction)

    ya_fiction_priority = 0
    if include_best_sellers:
        ya_fiction_best_sellers, ignore = create(
            _db, Lane, library=library,
            display_name="Best Sellers",
            fiction=True,
            priority=ya_fiction_priority,
            **ya_common_args
        )
        ya_fiction_priority += 1
        ya_fiction_best_sellers.list_datasource = nyt_data_source
        ya_fiction.sublanes.append(ya_fiction_best_sellers)

    ya_fiction.sublanes.append(
        lane_from_genres(_db, library, [genres.Dystopian_SF],
                         priority=ya_fiction_priority, **ya_common_args))
    ya_fiction_priority += 1
    ya_fiction.sublanes.append(
        lane_from_genres(_db, library, [genres.Fantasy],
                         priority=ya_fiction_priority, **ya_common_args))
    ya_fiction_priority += 1
    ya_fiction.sublanes.append(
        lane_from_genres(_db, library, [genres.Comics_Graphic_Novels],
                         priority=ya_fiction_priority, **ya_common_args))
    ya_fiction_priority += 1
    ya_fiction.sublanes.append(
        lane_from_genres(_db, library, [genres.Literary_Fiction],
                         display_name="Contemporary Fiction",
                         priority=ya_fiction_priority, **ya_common_args))
    ya_fiction_priority += 1
    ya_fiction.sublanes.append(
        lane_from_genres(_db, library, [genres.LGBTQ_Fiction],
                         priority=ya_fiction_priority, **ya_common_args))
    ya_fiction_priority += 1
    ya_fiction.sublanes.append(
        lane_from_genres(_db, library, [genres.Suspense_Thriller, genres.Mystery],
                         display_name="Mystery & Thriller",
                         priority=ya_fiction_priority, **ya_common_args))
    ya_fiction_priority += 1
    ya_fiction.sublanes.append(
        lane_from_genres(_db, library, [genres.Romance],
                         priority=ya_fiction_priority, **ya_common_args))
    ya_fiction_priority += 1
    ya_fiction.sublanes.append(
        lane_from_genres(_db, library, [genres.Science_Fiction],
                         exclude_genres=[genres.Dystopian_SF, genres.Steampunk],
                         priority=ya_fiction_priority, **ya_common_args))
    ya_fiction_priority += 1
    ya_fiction.sublanes.append(
        lane_from_genres(_db, library, [genres.Steampunk],
                         priority=ya_fiction_priority, **ya_common_args))
    ya_fiction_priority += 1

    ya_nonfiction, ignore = create(
        _db, Lane, library=library,
        display_name="Young Adult Nonfiction",
        genres=[], fiction=False,
        sublanes=[],
        priority=priority,
        **ya_common_args
    )
    priority += 1
    sublanes.append(ya_nonfiction)

    ya_nonfiction_priority = 0
    if include_best_sellers:
        ya_nonfiction_best_sellers, ignore = create(
            _db, Lane, library=library,
            display_name="Best Sellers",
            fiction=False,
            priority=ya_nonfiction_priority,
            **ya_common_args
        )
        ya_nonfiction_priority += 1
        ya_nonfiction_best_sellers.list_datasource = nyt_data_source
        ya_nonfiction.sublanes.append(ya_nonfiction_best_sellers)

    ya_nonfiction.sublanes.append(
        lane_from_genres(_db, library, [genres.Biography_Memoir],
                         display_name="Biography",
                         priority=ya_nonfiction_priority, **ya_common_args))
    ya_nonfiction_priority += 1
    ya_nonfiction.sublanes.append(
        lane_from_genres(_db, library, [genres.History, genres.Social_Sciences],
                         display_name="History & Sociology",
                         priority=ya_nonfiction_priority, **ya_common_args))
    ya_nonfiction_priority += 1
    ya_nonfiction.sublanes.append(
        lane_from_genres(_db, library, [genres.Life_Strategies],
                         priority=ya_nonfiction_priority, **ya_common_args))
    ya_nonfiction_priority += 1
    ya_nonfiction.sublanes.append(
        lane_from_genres(_db, library, [genres.Religion_Spirituality],
                         priority=ya_nonfiction_priority, **ya_common_args))
    ya_nonfiction_priority += 1


    children_common_args = dict(
        audiences=CHILDREN,
        languages=languages,
    )

    children, ignore = create(
        _db, Lane, library=library,
        display_name="Children and Middle Grade",
        genres=[], fiction=None,
        sublanes=[],
        priority=priority,
        **children_common_args
    )
    priority += 1
    sublanes.append(children)

    children_priority = 0
    if include_best_sellers:
        children_best_sellers, ignore = create(
            _db, Lane, library=library,
            display_name="Best Sellers",
            priority=children_priority,
            **children_common_args
        )
        children_priority += 1
        children_best_sellers.list_datasource = nyt_data_source
        children.sublanes.append(children_best_sellers)

    picture_books, ignore = create(
        _db, Lane, library=library,
        display_name="Picture Books",
        target_age=(0,4), genres=[], fiction=None,
        priority=children_priority,
        languages=languages,
    )
    children_priority += 1
    children.sublanes.append(picture_books)

    easy_readers, ignore = create(
        _db, Lane, library=library,
        display_name="Easy Readers",
        target_age=(5,8), genres=[], fiction=None,
        priority=children_priority,
        languages=languages,
    )
    children_priority += 1
    children.sublanes.append(easy_readers)

    chapter_books, ignore = create(
        _db, Lane, library=library,
        display_name="Chapter Books",
        target_age=(9,12), genres=[], fiction=None,
        priority=children_priority,
        languages=languages,
    )
    children_priority += 1
    children.sublanes.append(chapter_books)

    children_poetry, ignore = create(
        _db, Lane, library=library,
        display_name="Poetry Books",
        priority=children_priority,
        **children_common_args
    )
    children_priority += 1
    children_poetry.add_genre(genres.Poetry.name)
    children.sublanes.append(children_poetry)

    children_folklore, ignore = create(
        _db, Lane, library=library,
        display_name="Folklore",
        priority=children_priority,
        **children_common_args
    )
    children_priority += 1
    children_folklore.add_genre(genres.Folklore.name)
    children.sublanes.append(children_folklore)

    children_fantasy, ignore = create(
        _db, Lane, library=library,
        display_name="Fantasy",
        fiction=True,
        priority=children_priority,
        **children_common_args
    )
    children_priority += 1
    children_fantasy.add_genre(genres.Fantasy.name)
    children.sublanes.append(children_fantasy)

    children_sf, ignore = create(
        _db, Lane, library=library,
        display_name="Science Fiction",
        fiction=True,
        priority=children_priority,
        **children_common_args
    )
    children_priority += 1
    children_sf.add_genre(genres.Science_Fiction.name)
    children.sublanes.append(children_sf)

    realistic_fiction, ignore = create(
        _db, Lane, library=library,
        display_name="Realistic Fiction",
        fiction=True,
        priority=children_priority,
        **children_common_args
    )
    children_priority += 1
    realistic_fiction.add_genre(genres.Literary_Fiction.name)
    children.sublanes.append(realistic_fiction)

    children_graphic_novels, ignore = create(
        _db, Lane, library=library,
        display_name="Comics & Graphic Novels",
        priority=children_priority,
        **children_common_args
    )
    children_priority += 1
    children_graphic_novels.add_genre(genres.Comics_Graphic_Novels.name)
    children.sublanes.append(children_graphic_novels)

    children_biography, ignore = create(
        _db, Lane, library=library,
        display_name="Biography",
        priority=children_priority,
        **children_common_args
    )
    children_priority += 1
    children_biography.add_genre(genres.Biography_Memoir.name)
    children.sublanes.append(children_biography)

    children_historical_fiction, ignore = create(
        _db, Lane, library=library,
        display_name="Historical Fiction",
        priority=children_priority,
        **children_common_args
    )
    children_priority += 1
    children_historical_fiction.add_genre(genres.Historical_Fiction.name)
    children.sublanes.append(children_historical_fiction)

    informational, ignore = create(
        _db, Lane, library=library,
        display_name="Informational Books",
        fiction=False, genres=[],
        priority=children_priority,
        **children_common_args
    )
    children_priority += 1
    informational.add_genre(genres.Biography_Memoir.name, inclusive=False)
    children.sublanes.append(informational)

    return priority

def create_lane_for_small_collection(_db, library, languages, priority=0):
    if isinstance(languages, basestring):
        languages = [languages]

    ADULT = Classifier.AUDIENCES_ADULT
    YA_CHILDREN = [Classifier.AUDIENCE_YOUNG_ADULT, Classifier.AUDIENCE_CHILDREN]

    common_args = dict(
        languages=languages,
        genres=[],
    )
    language_identifier = LanguageCodes.name_for_languageset(languages)
    sublane_priority = 0

    adult_fiction, ignore = create(
        _db, Lane, library=library,
        display_name="Fiction",
        fiction=True,
        audiences=ADULT,
        priority=sublane_priority,
        **common_args
    )
    sublane_priority += 1

    adult_nonfiction, ignore = create(
        _db, Lane, library=library,
        display_name="Nonfiction",
        fiction=False,
        audiences=ADULT,
        priority=sublane_priority,
        **common_args
    )
    sublane_priority += 1

    ya_children, ignore = create(
        _db, Lane, library=library,
        display_name="Children & Young Adult",
        fiction=None,
        audiences=YA_CHILDREN,
        priority=sublane_priority,
        **common_args
    )
    sublane_priority += 1

    lane, ignore = create(
        _db, Lane, library=library,
        display_name=language_identifier,
        sublanes=[adult_fiction, adult_nonfiction, ya_children],
        priority=priority,
        **common_args
    )
    priority += 1
    return priority

def create_lane_for_tiny_collections(_db, library, languages, priority=0):
    language_lanes = []

    if not languages:
        return None

    if isinstance(languages, basestring):
        languages = [languages]

    sublane_priority = 0
    for language_set in languages:
        name = LanguageCodes.name_for_languageset(language_set)
        language_lane, ignore = create(
            _db, Lane, library=library,
            display_name=name,
            genres=[],
            fiction=None,
            priority=sublane_priority,
            languages=[language_set],
        )
        sublane_priority += 1
        language_lanes.append(language_lane)

    lane, ignore = create(
        _db, Lane, library=library, 
        display_name="Other Languages", 
        sublanes=language_lanes,
        genres=[],
        fiction=None,
        languages=languages,
        priority=priority,
    )
    priority += 1
    return priority


class DynamicLane(WorkList):
    """A WorkList that's used to from an OPDS lane, but isn't a Lane
    in the database."""

class WorkBasedLane(DynamicLane):
    """A query-based lane connected on a particular Work"""

    DISPLAY_NAME = None
    ROUTE = None

    def __init__(self, library, work, display_name=None,
                 children=None, **kwargs):
        self.work = work
        self.edition = work.presentation_edition

        languages = [self.edition.language]

        self.source_audience = self.work.audience
        audiences = self.audiences_list_from_source()

        display_name = display_name or self.DISPLAY_NAME

        children = children or list()

        super(WorkBasedLane, self).initialize(
            library, display_name=display_name, children=children,
            languages=languages, audiences=audiences, **kwargs
        )

    @property
    def url_arguments(self):
        if not self.ROUTE:
            raise NotImplementedError()
        identifier = self.edition.primary_identifier
        kwargs = dict(
            identifier_type=identifier.type,
            identifier=identifier.identifier
        )
        return self.ROUTE, kwargs

    def audiences_list_from_source(self):
        if (not self.source_audience or
            self.source_audience in Classifier.AUDIENCES_ADULT):
            return Classifier.AUDIENCES
        if self.source_audience == Classifier.AUDIENCE_YOUNG_ADULT:
            return Classifier.AUDIENCES_JUVENILE
        else:
            return [Classifier.AUDIENCE_CHILDREN]


class RelatedBooksLane(WorkBasedLane):
    """A lane of Works all related to a given Work by various criteria.

    Current criteria--represented by sublanes--include a shared
    contributor (ContributorLane), same series (SeriesLane), or
    third-party recommendation relationship (Recommendationlane).
    """
    DISPLAY_NAME = "Related Books"
    ROUTE = 'related_books'

    def __init__(self, library, work, display_name=None,
                 novelist_api=None):
        super(RelatedBooksLane, self).__init__(
            library, work, display_name=display_name,
        )
        _db = Session.object_session(library)
        sublanes = self._get_sublanes(_db, novelist_api)
        if not sublanes:
            raise ValueError(
                "No related books for %s by %s" % (self.work.title, self.work.author)
            )
        self.children = sublanes

    def _get_sublanes(self, _db, novelist_api):
        sublanes = list()

        for contributor_lane in self._contributor_sublanes(_db):
            sublanes.append(contributor_lane)

        for recommendation_lane in self._recommendation_sublane(_db, novelist_api):
            sublanes.append(recommendation_lane)

        # Create a series sublane.
        series_name = self.edition.series
        if series_name:
            sublanes.append(SeriesLane(self.get_library(_db), series_name, parent=self, languages=self.languages))

        return sublanes

    def _contributor_sublanes(self, _db):
        """Create contributor sublanes"""
        viable_contributors = list()
        roles_by_priority = list(Contributor.author_contributor_tiers())[1:]

        while roles_by_priority and not viable_contributors:
            author_roles = roles_by_priority.pop(0)
            viable_contributors = [c.contributor
                                   for c in self.edition.contributions
                                   if c.role in author_roles]

        for contributor in viable_contributors:
            contributor_name = None
            if contributor.display_name:
                # Prefer display names over sort names for easier URIs
                # at the /works/contributor/<NAME> route.
                contributor_name = contributor.display_name
            else:
                contributor_name = contributor.sort_name

            contributor_lane = ContributorLane(
                self.get_library(_db), contributor_name, parent=self,
                languages=self.languages, audiences=self.audiences,
            )
            yield contributor_lane

    def _recommendation_sublane(self, _db, novelist_api):
        """Create a recommendations sublane."""
        try:
            lane_name = "Recommendations for %s by %s" % (
                self.work.title, self.work.author
            )
            recommendation_lane = RecommendationLane(
                self.get_library(_db), self.work, lane_name, novelist_api=novelist_api,
                parent=self,
            )
            if recommendation_lane.recommendations:
                yield recommendation_lane
        except ValueError, e:
            # NoveList isn't configured.
            pass


class RecommendationLane(WorkBasedLane):
    """A lane of recommended Works based on a particular Work"""

    DISPLAY_NAME = "Recommended Books"
    ROUTE = "recommendations"
    MAX_CACHE_AGE = 7*24*60*60      # one week

    def __init__(self, library, work, display_name=None,
                 novelist_api=None, parent=None):
        super(RecommendationLane, self).__init__(
            library, work, display_name=display_name,
        )
        _db = Session.object_session(library)
        self.api = novelist_api or NoveListAPI.from_config(library)
        self.recommendations = self.fetch_recommendations(_db)
        if parent:
            parent.children.append(self)

    def fetch_recommendations(self, _db):
        """Get identifiers of recommendations for this LicensePool"""

        metadata = self.api.lookup(self.edition.primary_identifier)
        if metadata:
            metadata.filter_recommendations(_db)
            return metadata.recommendations
        return []

    def apply_filters(self, _db, qu, work_model, facets, pagination, featured=False):
        if not self.recommendations:
            return None

        if work_model != Work:
            qu = qu.join(LicensePool.identifier)
        qu = Work.from_identifiers(
            _db, self.recommendations, base_query=qu
        )
        return super(RecommendationLane, self).apply_filters(
            _db, qu, work_model, facets, pagination, featured=featured)

class SeriesLane(DynamicLane):
    """A lane of Works in a particular series"""

    ROUTE = 'series'
    MAX_CACHE_AGE = 48*60*60    # 48 hours

    def __init__(self, library, series_name, parent=None, languages=None,
                 audiences=None):
        if not series_name:
            raise ValueError("SeriesLane can't be created without series")
        self.series = series_name
        display_name = self.series

        if parent and isinstance(parent, WorkBasedLane):
            # In an attempt to secure the accurate series, limit the
            # listing to the source's audience sourced from parent data.
            audiences = [parent.source_audience]

        super(SeriesLane, self).initialize(
            library, display_name=display_name,
            audiences=audiences, languages=languages,
        )
        if parent:
            parent.children.append(self)

    @property
    def url_arguments(self):
        kwargs = dict(
            series_name=self.series,
            languages=self.language_key,
            audiences=self.audience_key
        )
        return self.ROUTE, kwargs

    def featured_works(self, _db):
        qu = self.works(_db)

        # Aliasing Edition here allows this query to function
        # regardless of work_model and existing joins.
        work_edition = aliased(Edition)
        qu = qu.join(work_edition).order_by(work_edition.series_position, work_edition.title)
        target_size = self.get_library(_db).featured_lane_size
        qu = qu.limit(target_size)
        return qu.all()

    def apply_filters(self, _db, qu, work_model, facets, pagination, featured=False):
        if not self.series:
            return None

        # Aliasing Edition here allows this query to function
        # regardless of work_model and existing joins.
        work_edition = aliased(Edition)
        qu = qu.join(work_edition).filter(work_edition.series==self.series)
        return super(SeriesLane, self).apply_filters(
            _db, qu, work_model, facets, pagination, featured)


class ContributorLane(DynamicLane):
    """A lane of Works written by a particular contributor"""

    ROUTE = 'contributor'
    MAX_CACHE_AGE = 48*60*60    # 48 hours

    def __init__(self, library, contributor_name,
                 parent=None, languages=None, audiences=None):
        if not contributor_name:
            raise ValueError("ContributorLane can't be created without contributor")

        self.contributor_name = contributor_name
        display_name = contributor_name
        super(ContributorLane, self).initialize(
            library, display_name=display_name,
            audiences=audiences, languages=languages,
        )
        if parent:
            parent.children.append(self)
        _db = Session.object_session(library)
        self.contributors = _db.query(Contributor)\
                .filter(or_(*self.contributor_name_clauses)).all()

    @property
    def url_arguments(self):
        kwargs = dict(
            contributor_name=self.contributor_name,
            languages=self.language_key,
            audiences=self.audience_key
        )
        return self.ROUTE, kwargs

    @property
    def contributor_name_clauses(self):
        return [
            Contributor.display_name==self.contributor_name,
            Contributor.sort_name==self.contributor_name
        ]

    def apply_filters(self, _db, qu, work_model, facets, pagination, featured=False):
        if not self.contributor_name:
            return None

        work_edition = aliased(Edition)
        qu = qu.join(work_edition).join(work_edition.contributions)
        qu = qu.join(Contribution.contributor)

        # Run a number of queries against the Edition table based on the
        # available contributor information: name, display name, id, viaf.
        clauses = self.contributor_name_clauses

        if self.contributors:
            viafs = list(set([c.viaf for c in self.contributors if c.viaf]))
            if len(viafs) == 1:
                # If there's only one VIAF here, look for other
                # Contributors that share it. This helps catch authors
                # with pseudonyms.
                clauses.append(Contributor.viaf==viafs[0])
        or_clause = or_(*clauses)
        qu = qu.filter(or_clause)

        return super(ContributorLane, self).apply_filters(
            _db, qu, work_model, facets, pagination, featured=featured)
