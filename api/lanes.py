from nose.tools import set_trace
from sqlalchemy import (
    and_,
    func,
    or_,
)
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
    Library,
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
    top_level = WorkList.top_level_for_library(_db, library)

    # It's likely this WorkList will be used across sessions, so
    # expunge any data model objects from the database session.
    if isinstance(top_level, Lane):
        to_expunge = [top_level]
    else:
        to_expunge = [x for x in top_level.children if isinstance(x, Lane)]

    map(_db.expunge, to_expunge)
    return top_level


def _lane_configuration_from_collection_sizes(estimates):
    """Sort a library's collections into 'large', 'small', and 'tiny'
    subcollections based on language.

    :param estimates: A Counter.

    :return: A 3-tuple (large, small, tiny). 'large' will contain the
    collection with the largest language, and any languages with a
    collection more than 10% the size of the largest
    collection. 'small' will contain any languages with a collection
    more than 1% the size of the largest collection, and 'tiny' will
    contain all other languages represented in `estimates`.
    """
    if not estimates:
        # There are no holdings. Assume we have a large English
        # collection and nothing else.
        return [u'eng'], [], []

    large = []
    small = []
    tiny = []

    [(ignore, largest)] = estimates.most_common(1)
    for language, count in estimates.most_common():
        if count > largest * 0.1:
            large.append(language)
        elif count > largest * 0.01:
            small.append(language)
        else:
            tiny.append(language)
    return large, small, tiny


def create_default_lanes(_db, library):
    """Reset the lanes for the given library to the default.

    The database will have the following top-level lanes for
    each large-collection:
    'Adult Fiction', 'Adult Nonfiction', 'Young Adult Fiction',
    'Young Adult Nonfiction', and 'Children'.
    Each lane contains additional sublanes.
    If an NYT integration is configured, there will also be a
    'Best Sellers' top-level lane.

    If there are any small- or tiny-collection languages, the database
    will also have a top-level lane called 'World Languages'. The
    'World Languages' lane will have a sublane for every small- and
    tiny-collection languages. The small-collection languages will
    have "Adult Fiction", "Adult Nonfiction", and "Children/YA"
    sublanes; the tiny-collection languages will not have any sublanes.

    If run on a Library that already has Lane configuration, this can
    be an extremely destructive method. All new Lanes will be visible
    and all Lanes based on CustomLists (but not the CustomLists
    themselves) will be destroyed.

    """
    # Delete existing lanes.
    for lane in _db.query(Lane).filter(Lane.library_id==library.id):
        _db.delete(lane)

    top_level_lanes = []

    # Hopefully this library is configured with explicit guidance as
    # to how the languages should be set up.
    large = Configuration.large_collection_languages(library) or []
    small = Configuration.small_collection_languages(library) or []
    tiny = Configuration.tiny_collection_languages(library) or []

    # If there are no language configuration settings, we can estimate
    # the current collection size to determine the lanes.
    if not large and not small and not tiny:
        estimates = library.estimated_holdings_by_language()
        large, small, tiny = _lane_configuration_from_collection_sizes(estimates)
    priority = 0
    for language in large:
        priority = create_lanes_for_large_collection(_db, library, language, priority=priority)

    create_world_languages_lane(_db, library, small, tiny, priority)

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

    common_args = dict(
        languages=languages,
        media=None
    )
    adult_common_args = dict(common_args)
    adult_common_args['audiences'] = ADULT

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
            **common_args
        )
        priority += 1
        best_sellers.list_datasource = nyt_data_source
        sublanes.append(best_sellers)


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

    ya_common_args = dict(common_args)
    ya_common_args['audiences'] = YA

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


    children_common_args = dict(common_args)
    children_common_args['audiences'] = CHILDREN

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

def create_world_languages_lane(
        _db, library, small_languages, tiny_languages, priority=0,
):
    """Create a lane called 'World Languages' whose sublanes represent
    the non-large language collections available to this library.
    """
    if not small_languages and not tiny_languages:
        # All the languages on this system have large collections, so
        # there is no need for a 'World Languages' lane.
        return priority

    complete_language_set = set()
    for list in (small_languages, tiny_languages):
        for languageset in list:
            if isinstance(languageset, basestring):
                complete_language_set.add(languageset)
            else:
                complete_language_set.update(languageset)


    world_languages, ignore = create(
        _db, Lane, library=library,
        display_name="World Languages",
        fiction=None,
        priority=priority,
        languages=complete_language_set,
        media=[Edition.BOOK_MEDIUM],
        genres=[]
    )
    priority += 1

    language_priority = 0
    for small in small_languages:
        # Create a lane (with sublanes) for each small collection.
        language_priority = create_lane_for_small_collection(
            _db, library, world_languages, small, language_priority
        )
    for tiny in tiny_languages:
        # Create a lane (no sublanes) for each tiny collection.
        language_priority = create_lane_for_tiny_collection(
            _db, library, world_languages, tiny, language_priority
        )
    return priority

def create_lane_for_small_collection(_db, library, parent, languages, priority=0):
    """Create a lane (with sublanes) for a small collection based on language.

    :param parent: The parent of the new lane.
    """
    if isinstance(languages, basestring):
        languages = [languages]

    ADULT = Classifier.AUDIENCES_ADULT
    YA_CHILDREN = [Classifier.AUDIENCE_YOUNG_ADULT, Classifier.AUDIENCE_CHILDREN]

    common_args = dict(
        languages=languages,
        media=[Edition.BOOK_MEDIUM],
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
        parent=parent,
        sublanes=[adult_fiction, adult_nonfiction, ya_children],
        priority=priority,
        **common_args
    )
    priority += 1
    return priority

def create_lane_for_tiny_collection(_db, library, parent, languages, priority=0):
    """Create a single lane for a tiny collection based on language.

    :param parent: The parent of the new lane.
    """
    if not languages:
        return None

    if isinstance(languages, basestring):
        languages = [languages]

    name = LanguageCodes.name_for_languageset(languages)
    language_lane, ignore = create(
        _db, Lane, library=library,
        display_name=name,
        parent=parent,
        genres=[],
        media=[Edition.BOOK_MEDIUM],
        fiction=None,
        priority=priority,
        languages=languages,
    )
    return priority + 1


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

    def apply_filters(self, _db, qu, facets, pagination, featured=False):
        if not self.recommendations:
            return None
        from core.model import MaterializedWorkWithGenre as mw
        qu = qu.join(LicensePool.identifier)
        qu = Work.from_identifiers(
            _db, self.recommendations, base_query=qu,
            identifier_id_field=mw.identifier_id
        )
        return super(RecommendationLane, self).apply_filters(
            _db, qu, facets, pagination, featured=featured
        )


class FeaturedSeriesFacets(Facets):
    """A custom Facets object for ordering a lane based on series."""

    def order_by(self):
        """Order the query results by series position."""
        from core.model import MaterializedWorkWithGenre as mw
        fields = (mw.series_position, mw.sort_title)
        return [x.asc() for x in fields], fields


class SeriesLane(DynamicLane):
    """A lane of Works in a particular series."""

    ROUTE = 'series'
    MAX_CACHE_AGE = 48*60*60    # 48 hours

    def __init__(self, library, series_name, parent=None, languages=None,
                 audiences=None):
        if not series_name:
            raise ValueError("SeriesLane can't be created without series")
        self.series = series_name
        display_name = self.series

        if parent and parent.source_audience and isinstance(parent, WorkBasedLane):
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

    def featured_works(self, _db, facets=None):
        library = self.get_library(_db)
        new_facets = FeaturedSeriesFacets(
            library,
            # If a work is in the right series we don't care about its
            # quality.
            collection=FeaturedSeriesFacets.COLLECTION_FULL,
            availability=FeaturedSeriesFacets.AVAILABLE_ALL,
            order=None
        )
        if facets:
            new_facets.entrypoint = facets.entrypoint
        pagination = Pagination(size=library.featured_lane_size)
        qu = self.works(_db, facets=new_facets, pagination=pagination)
        return qu.all()

    def apply_filters(self, _db, qu, facets, pagination, featured=False):
        if not self.series:
            return None
        # We could filter on MaterializedWorkWithGenre.series, but
        # there's no index on that field, so it would cause a table
        # scan. Instead we add a join to Edition and filter on the
        # field there, where it is indexed.
        qu = qu.join(LicensePool.presentation_edition)
        qu = qu.filter(Edition.series==self.series)
        return super(SeriesLane, self).apply_filters(
            _db, qu, facets, pagination, featured)

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

    def apply_filters(self, _db, qu, facets, pagination, featured=False):
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
            _db, qu, facets, pagination, featured=featured)

class CrawlableFacets(Facets):
    """A special Facets class for crawlable feeds."""
    @classmethod
    def default(cls, library):
        enabled_facets = {
            Facets.ORDER_FACET_GROUP_NAME : [Facets.ORDER_LAST_UPDATE],
            Facets.AVAILABILITY_FACET_GROUP_NAME : [Facets.AVAILABLE_ALL],
            Facets.COLLECTION_FACET_GROUP_NAME : [Facets.COLLECTION_FULL],
        }
        return cls(
            library,
            collection=cls.COLLECTION_FULL,
            availability=cls.AVAILABLE_ALL,
            order=cls.ORDER_LAST_UPDATE,
            enabled_facets=enabled_facets,
            order_ascending=cls.ORDER_DESCENDING,
        )

    @classmethod
    def order_by(cls):
        """Order the search results by last update time."""
        from core.model import MaterializedWorkWithGenre as work_model
        # TODO: first_appearance is only necessary here if this is for a custom list.
        updated = func.greatest(work_model.availability_time, work_model.first_appearance, work_model.last_update_time)
        collection_id = work_model.collection_id
        work_id = work_model.works_id
        return ([updated.desc(), collection_id, work_id],
                [updated, collection_id, work_id])


class CrawlableCollectionBasedLane(DynamicLane):

    LIBRARY_ROUTE = "crawlable_library_feed"
    COLLECTION_ROUTE = "crawlable_collection_feed"

    def __init__(self, library, collections=None):
        """Create a lane that finds all books in the given collections.

        :param library: The Library to use for purposes of annotating
            this Lane's OPDS feed.
        :param collections: A list of Collections. If none are specified,
            all Collections associated with `library` will be used.
        """
        self.library_id = None
        if library:
            self.library_id = library.id
        self.collection_feed = False
        if collections:
            identifier = " / ".join(sorted([x.name for x in collections]))
            if len(collections) == 1:
                self.collection_feed = True
                self.collection_name = collections[0].name
        else:
            identifier = library.name
            collections = library.collections
        self.initialize(library, "Crawlable feed: %s" % identifier)
        if collections:
            # initialize() set the collection IDs to all collections
            # associated with the library. We may want to restrict that
            # further.
            self.collection_ids = [x.id for x in collections]

    def bibliographic_filter_clause(self, _db, qu, featured=False):
        """Filter out any books that aren't in the right collections."""
        # The normal behavior of works() is to put a restriction on
        # collection_ids, so we only need to do something if
        # there are no collections specified.
        if not self.collection_ids:
            # When no collection IDs are specified, there is no lane
            # whatsoever
            return None, None
        return super(
            CrawlableCollectionBasedLane, self).bibliographic_filter_clause(
                _db, qu, featured
            )

    @property
    def url_arguments(self):
        if not self.collection_feed:
            return self.LIBRARY_ROUTE, dict()
        else:
            kwargs = dict(
                collection_name=self.collection_name,
            )
            return self.COLLECTION_ROUTE, kwargs


class CrawlableCustomListBasedLane(DynamicLane):
    """A lane that consists of all works in a single CustomList."""

    ROUTE = "crawlable_list_feed"

    uses_customlists = True

    def initialize(self, library, list):
        super(CrawlableCustomListBasedLane, self).initialize(
            library, "Crawlable feed: %s" % list.name
        )
        self.customlists = [list]

    def bibliographic_filter_clause(self, _db, qu, featured=False):
        """Filter out any books that aren't in the list, in addition to
        the normal filters."""
        qu, clauses = super(CrawlableCustomListBasedLane, self).bibliographic_filter_clause(_db, qu, featured)

        from core.model import MaterializedWorkWithGenre as work_model
        customlist_clause = work_model.list_id==self.customlists[0].id

        if clauses:
            clause = and_(clauses, customlist_clause)
        else:
            clause = customlist_clause
        return qu, clause

    @property
    def url_arguments(self):
        kwargs = dict(
            list_name=self.customlists[0].name,
        )
        return self.ROUTE, kwargs
