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
from .config import (
    CannotLoadConfiguration,
    Configuration,
)
from core.classifier import (
    Classifier,
    fiction_genres,
    nonfiction_genres,
    GenreData,
)
from core import classifier

from core.lane import (
    BaseFacets,
    DatabaseBackedWorkList,
    DefaultSortOrderFacets,
    Facets,
    FacetsWithEntryPoint,
    Pagination,
    Lane,
    WorkList,
)
from core.model import (
    get_one,
    create,
    CachedFeed,
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
from .novelist import NoveListAPI

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
    #
    # TODO: This is the cause of a lot of problems in the cached OPDS
    # feed generator. There, these Lanes are used in a normal database
    # session and we end up needing hacks to merge them back into the
    # session.
    if isinstance(top_level, Lane):
        to_expunge = [top_level]
    else:
        to_expunge = [x for x in top_level.children if isinstance(x, Lane)]
    # TODO python3
    list(map(_db.expunge, to_expunge))
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
        return ['eng'], [], []

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

        if genres[0] in list(genre_lane_instructions.keys()):
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
    if isinstance(languages, (bytes, str)):
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
        if isinstance(genre, (bytes, str)):
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
            if isinstance(genre, (bytes, str)):
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
            if isinstance(languageset, (bytes, str)):
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
    """Create a lane (with sublanes) for a small collection based on language,
    if the language exists in the lookup table.

    :param parent: The parent of the new lane.
    """
    if isinstance(languages, (bytes, str)):
        languages = [languages]

    ADULT = Classifier.AUDIENCES_ADULT
    YA_CHILDREN = [Classifier.AUDIENCE_YOUNG_ADULT, Classifier.AUDIENCE_CHILDREN]

    common_args = dict(
        languages=languages,
        media=[Edition.BOOK_MEDIUM],
        genres=[],
    )

    try:
        language_identifier = LanguageCodes.name_for_languageset(languages)
    except ValueError as e:
        logging.getLogger().warn(
            "Could not create a lane for small collection with languages %s", languages
        )
        return 0

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
    """Create a single lane for a tiny collection based on language,
    if the language exists in the lookup table.

    :param parent: The parent of the new lane.
    """
    if not languages:
        return None

    if isinstance(languages, (bytes, str)):
        languages = [languages]
    
    try:
        name = LanguageCodes.name_for_languageset(languages)
    except ValueError as e:
        logging.getLogger().warn(
            "Could not create a lane for tiny collection with languages %s", languages
        )
        return 0

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

class DatabaseExclusiveWorkList(DatabaseBackedWorkList):
    """A DatabaseBackedWorkList that can _only_ get Works through the database."""
    def works(self, *args, **kwargs):
        return self.works_from_database(*args, **kwargs)

class WorkBasedLane(DynamicLane):
    """A lane that shows works related to one particular Work."""

    DISPLAY_NAME = None
    ROUTE = None

    def __init__(self, library, work, display_name=None,
                 children=None, **kwargs):
        self.work = work
        self.edition = work.presentation_edition

        # To avoid showing the same book in other languages, the value
        # of this lane's .languages is always derived from the
        # language of the work.  All children of this lane will be put
        # under a similar restriction.
        self.source_language = self.edition.language
        kwargs['languages'] = [self.source_language]

        # To avoid showing inappropriate material, the value of this
        # lane's .audiences setting is always derived from the
        # audience of the work. All children of this lane will be
        # under a similar restriction.
        self.source_audience = self.work.audience
        kwargs['audiences'] = self.audiences_list_from_source()

        display_name = display_name or self.DISPLAY_NAME

        children = children or list()

        super(WorkBasedLane, self).initialize(
            library, display_name=display_name, children=children,
            **kwargs
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

    def append_child(self, worklist):
        """Add another Worklist as a child of this one and change its
        configuration to make sure its results fit in with this lane.
        """
        super(WorkBasedLane, self).append_child(worklist)
        worklist.languages = self.languages
        worklist.audiences = self.audiences

    def accessible_to(self, patron):
        """In addition to the restrictions imposed by the superclass, a lane
        based on a specific Work is accessible to a Patron only if the
        Work itself is age-appropriate for the patron.

        :param patron: A Patron
        :return: A boolean
        """
        superclass_ok = super(WorkBasedLane, self).accessible_to(patron)
        return superclass_ok and (
            not self.work or self.work.age_appropriate_for_patron(patron)
        )


class RecommendationLane(WorkBasedLane):
    """A lane of recommended Works based on a particular Work"""

    DISPLAY_NAME = "Titles recommended by NoveList"
    ROUTE = "recommendations"

    # Cache for 24 hours -- would ideally be much longer but availability
    # information goes stale.
    MAX_CACHE_AGE = 24*60*60
    CACHED_FEED_TYPE = CachedFeed.RECOMMENDATIONS_TYPE

    def __init__(self, library, work, display_name=None,
                 novelist_api=None, parent=None):
        """Constructor.

        :raises: CannotLoadConfiguration if `novelist_api` is not provided
        and no Novelist integration is configured for this library.
        """
        super(RecommendationLane, self).__init__(
            library, work, display_name=display_name,
        )
        self.novelist_api = novelist_api or NoveListAPI.from_config(library)
        if parent:
            parent.append_child(self)
        _db = Session.object_session(library)
        self.recommendations = self.fetch_recommendations(_db)

    def fetch_recommendations(self, _db):
        """Get identifiers of recommendations for this LicensePool"""
        metadata = self.novelist_api.lookup(self.edition.primary_identifier)
        if metadata:
            metadata.filter_recommendations(_db)
            return metadata.recommendations
        return []

    def overview_facets(self, _db, facets):
        """Convert a generic FeaturedFacets to some other faceting object,
        suitable for showing an overview of this WorkList in a grouped
        feed.
        """
        # TODO: Since the purpose of the recommendation feed is to
        # suggest books that can be borrowed immediately, it would be
        # better to set availability=AVAILABLE_NOW. However, this feed
        # is cached for so long that we can't rely on the availability
        # information staying accurate. It would be especially bad if
        # people borrowed all of the recommendations that were
        # available at the time this feed was generated, and then
        # recommendations that were unavailable when the feed was
        # generated became available.
        #
        # For now, it's better to show all books and let people put
        # the unavailable ones on hold if they want.
        #
        # TODO: It would be better to order works in the same order
        # they come from the recommendation engine, since presumably
        # the best recommendations are in the front.
        return Facets.default(
            self.get_library(_db), collection=facets.COLLECTION_FULL,
            availability=facets.AVAILABLE_ALL, entrypoint=facets.entrypoint,
        )

    def modify_search_filter_hook(self, filter):
        """Find Works whose Identifiers include the ISBNs returned
        by an external recommendation engine.

        :param filter: A Filter object.
        """
        if not self.recommendations:
            # There are no recommendations. The search should not even
            # be executed.
            filter.match_nothing = True
        else:
            filter.identifiers = self.recommendations
        return filter


class SeriesFacets(DefaultSortOrderFacets):
    """A list with a series restriction is ordered by series position by
    default.
    """
    DEFAULT_SORT_ORDER = Facets.ORDER_SERIES_POSITION


class SeriesLane(DynamicLane):
    """A lane of Works in a particular series."""

    ROUTE = 'series'
    # Cache for 24 hours -- would ideally be longer but availability
    # information goes stale.
    MAX_CACHE_AGE = 24*60*60
    CACHED_FEED_TYPE = CachedFeed.SERIES_TYPE

    def __init__(self, library, series_name, parent=None, **kwargs):
        if not series_name:
            raise ValueError("SeriesLane can't be created without series")
        super(SeriesLane, self).initialize(
            library, display_name=series_name, **kwargs
        )
        self.series = series_name
        if parent:
            parent.append_child(self)
            if isinstance(parent, WorkBasedLane) and parent.source_audience:
                # WorkBasedLane forces self.audiences to values
                # compatible with the work in the WorkBasedLane, but
                # that's not enough for us. We want to force
                # self.audiences to *the specific audience* of the
                # work in the WorkBasedLane. If we're looking at a YA
                # series, we don't want to see books in a children's
                # series with the same name, even if it would be
                # appropriate to show those books.
                self.audiences = [parent.source_audience]

    @property
    def url_arguments(self):
        kwargs = dict(series_name=self.series)
        if self.language_key:
            kwargs['languages'] = self.language_key
        if self.audience_key:
            kwargs['audiences'] = self.audience_key
        return self.ROUTE, kwargs

    def overview_facets(self, _db, facets):
        """Convert a FeaturedFacets to a SeriesFacets suitable for
        use in a grouped feed. Our contribution to a grouped feed will
        be ordered by series position.
        """
        return SeriesFacets.default(
            self.get_library(_db), collection=facets.COLLECTION_FULL,
            availability=facets.AVAILABLE_ALL, entrypoint=facets.entrypoint,
        )

    def modify_search_filter_hook(self, filter):
        filter.series = self.series
        return filter


class ContributorFacets(DefaultSortOrderFacets):
    """A list with a contributor restriction is, by default, sorted by
    title.
    """
    DEFAULT_SORT_ORDER = Facets.ORDER_TITLE


class ContributorLane(DynamicLane):
    """A lane of Works written by a particular contributor"""

    ROUTE = 'contributor'
    # Cache for 24 hours -- would ideally be longer but availability
    # information goes stale.
    MAX_CACHE_AGE = 24*60*60
    CACHED_FEED_TYPE = CachedFeed.CONTRIBUTOR_TYPE

    def __init__(self, library, contributor,
                 parent=None, languages=None, audiences=None):
        """Constructor.

        :param library: A Library.
        :param contributor: A Contributor or ContributorData object.
        :param parent: A WorkList.
        :param languages: An extra restriction on the languages of Works.
        :param audiences: An extra restriction on the audience for Works.
        """
        if not contributor:
            raise ValueError(
                "ContributorLane can't be created without contributor"
            )

        self.contributor = contributor
        self.contributor_key = (
            self.contributor.display_name or self.contributor.sort_name
        )
        super(ContributorLane, self).initialize(
            library, display_name=self.contributor_key,
            audiences=audiences, languages=languages,
        )
        if parent:
            parent.append_child(self)

    @property
    def url_arguments(self):
        kwargs = dict(
            contributor_name=self.contributor_key,
            languages=self.language_key,
            audiences=self.audience_key
        )
        return self.ROUTE, kwargs

    def overview_facets(self, _db, facets):
        """Convert a FeaturedFacets to a ContributorFacets suitable for
        use in a grouped feed.
        """
        return ContributorFacets.default(
            self.get_library(_db), collection=facets.COLLECTION_FULL,
            availability=facets.AVAILABLE_ALL, entrypoint=facets.entrypoint,
        )

    def modify_search_filter_hook(self, filter):
        filter.author = self.contributor
        return filter


class RelatedBooksLane(WorkBasedLane):
    """A lane of Works all related to a given Work by various criteria.

    Each criterion is represented by another WorkBaseLane class:

    * ContributorLane: Works by one of the contributors to this work.
    * SeriesLane: Works in the same series.
    * RecommendationLane: Works provided by a third-party recommendation
      service.
    """
    CACHED_FEED_TYPE = CachedFeed.RELATED_TYPE
    DISPLAY_NAME = "Related Books"
    ROUTE = 'related_books'

    # Cache this lane for the shortest amount of time any of its
    # component lane should be cached.
    MAX_CACHE_AGE = min(ContributorLane.MAX_CACHE_AGE,
                        SeriesLane.MAX_CACHE_AGE,
                        RecommendationLane.MAX_CACHE_AGE)

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

    def works(self, _db, *args, **kwargs):
        """This lane never has works of its own.

        Only its sublanes have works.
        """
        return []

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

        library = self.get_library(_db)
        for contributor in viable_contributors:
            contributor_lane = ContributorLane(
                library, contributor, parent=self,
                languages=self.languages, audiences=self.audiences,
            )
            yield contributor_lane

    def _recommendation_sublane(self, _db, novelist_api):
        """Create a recommendations sublane."""
        lane_name = "Similar titles recommended by NoveList"
        try:
            recommendation_lane = RecommendationLane(
                library=self.get_library(_db), work=self.work,
                display_name=lane_name, novelist_api=novelist_api,
                parent=self,
            )
            if recommendation_lane.recommendations:
                yield recommendation_lane
        except CannotLoadConfiguration as e:
            # NoveList isn't configured. This isn't fatal -- we just won't
            # use this sublane.
            pass


class CrawlableFacets(Facets):
    """A special Facets class for crawlable feeds."""

    CACHED_FEED_TYPE = CachedFeed.CRAWLABLE_TYPE

    # These facet settings are definitive of a crawlable feed.
    # Library configuration settings don't matter.
    SETTINGS = {
        Facets.ORDER_FACET_GROUP_NAME : Facets.ORDER_LAST_UPDATE,
        Facets.AVAILABILITY_FACET_GROUP_NAME: Facets.AVAILABLE_ALL,
        Facets.COLLECTION_FACET_GROUP_NAME: Facets.COLLECTION_FULL,
    }

    @classmethod
    def available_facets(cls, config, facet_group_name):
        return [cls.SETTINGS[facet_group_name]]

    @classmethod
    def default_facet(cls, config, facet_group_name):
        return cls.SETTINGS[facet_group_name]


class CrawlableLane(DynamicLane):

    # By default, crawlable feeds are cached for 12 hours.
    MAX_CACHE_AGE = 12 * 60 * 60


class CrawlableCollectionBasedLane(CrawlableLane):

    # Since these collections may be shared collections, for which
    # recent information is very important, these feeds are only
    # cached for 2 hours.
    MAX_CACHE_AGE = 2 * 60 * 60

    LIBRARY_ROUTE = "crawlable_library_feed"
    COLLECTION_ROUTE = "crawlable_collection_feed"

    def initialize(self, library_or_collections):

        self.collection_feed = False

        if isinstance(library_or_collections, Library):
            # We're looking at all the collections in a given library.
            library = library_or_collections
            collections = library.collections
            identifier = library.name
        else:
            # We're looking at collections directly, without respect
            # to the libraries that might use them.
            library = None
            collections = library_or_collections
            identifier = " / ".join(sorted([x.name for x in collections]))
            if len(collections) == 1:
                self.collection_feed = True
                self.collection_name = collections[0].name

        super(CrawlableCollectionBasedLane, self).initialize(
            library, "Crawlable feed: %s" % identifier,
        )
        if collections is not None:
            # initialize() set the collection IDs to all collections
            # associated with the library. We may want to restrict that
            # further.
            self.collection_ids = [x.id for x in collections]

    @property
    def url_arguments(self):
        if not self.collection_feed:
            return self.LIBRARY_ROUTE, dict()
        else:
            kwargs = dict(
                collection_name=self.collection_name,
            )
            return self.COLLECTION_ROUTE, kwargs


class CrawlableCustomListBasedLane(CrawlableLane):
    """A lane that consists of all works in a single CustomList."""

    ROUTE = "crawlable_list_feed"

    uses_customlists = True

    def initialize(self, library, customlist):
        self.customlist_name = customlist.name
        super(CrawlableCustomListBasedLane, self).initialize(
            library, "Crawlable feed: %s" % self.customlist_name,
            customlists=[customlist]
        )

    @property
    def url_arguments(self):
        kwargs = dict(list_name=self.customlist_name)
        return self.ROUTE, kwargs

class KnownOverviewFacetsWorkList(WorkList):
    """A WorkList whose defining feature is that the Facets object
    to be used when generating a grouped feed is known in advance.
    """
    def __init__(self, facets, *args, **kwargs):
        """Constructor.

        :param facets: A Facets object to be used when generating a grouped
           feed.
        """
        super(KnownOverviewFacetsWorkList, self).__init__(*args, **kwargs)
        self.facets = facets

    def overview_facets(self, _db, facets):
        """Return the faceting object to be used when generating a grouped
        feed.
        
        :param _db: Ignored -- only present for API compatibility.
        :param facets: Ignored -- only present for API compatibility.
        """
        return self.facets


class JackpotFacets(Facets):
    """A faceting object for a jackpot feed.

    Unlike other faceting objects, AVAILABLE_NOT_NOW is an acceptable
    option for the availability facet.
    """

    @classmethod
    def default_facet(cls, config, facet_group_name):
        if facet_group_name != cls.AVAILABILITY_FACET_GROUP_NAME:
            return super(JackpotFacets, cls).default_facet(
                config, facet_group_name
            )
        return cls.AVAILABLE_NOW

    @classmethod
    def available_facets(cls, config, facet_group_name):
        if facet_group_name != cls.AVAILABILITY_FACET_GROUP_NAME:
            return super(JackpotFacets, cls).available_facets(
                config, facet_group_name
            )

        return [cls.AVAILABLE_NOW, cls.AVAILABLE_NOT_NOW,
                cls.AVAILABLE_ALL, cls.AVAILABLE_OPEN_ACCESS]

class HasSeriesFacets(Facets):
    """A faceting object for a feed containg books guaranteed
    to belong to _some_ series.
    """
    def modify_search_filter(self, filter):
        filter.series = True


class JackpotWorkList(WorkList):
    """A WorkList guaranteed to, so far as possible, contain the exact
    selection of books necessary to perform common QA tasks.

    This makes it easy to write integration tests that work on real
    circulation managers and real books.
    """

    def __init__(self, library, facets):
        """Constructor.

        :param library: A Library
        :param facets: A Facets object.
        """
        super(JackpotWorkList, self).initialize(library)

        # Initialize a list of child Worklists; one for each test that
        # a client might need to run.
        self.children = []

        # Add one or more WorkLists for every collection in the
        # system, so that a client can test borrowing a book from
        # every collection.
        for collection in sorted(library.collections, key=lambda x: x.name):
            for medium in Edition.FULFILLABLE_MEDIA:
                # Give each Worklist a name that is distinctive
                # and easy for a client to parse.
                if collection.data_source:
                    data_source_name = collection.data_source.name
                else:
                    data_source_name = "[Unknown]"
                display_name = "License source {%s} - Medium {%s} - Collection name {%s}" % (data_source_name, medium, collection.name)
                child = KnownOverviewFacetsWorkList(facets)
                child.initialize(
                    library, media=[medium], display_name=display_name
                )
                child.collection_ids = [collection.id]
                self.children.append(child)

    def works(self, _db, *args, **kwargs):
        """This worklist never has works of its own.

        Only its children have works.
        """
        return []
