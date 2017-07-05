from nose.tools import set_trace
from sqlalchemy import or_
from sqlalchemy.orm import aliased

import core.classifier as genres
from config import Configuration
from core.classifier import (
    Classifier,
    fiction_genres,
    nonfiction_genres,
)
from core import classifier

from core.lane import (
    Facets,
    Lane,
    LaneList,
    make_lanes as core_make_lanes,
    QueryGeneratedLane,
)
from core.model import (
    get_one,
    Contribution,
    Contributor,
    Edition,
    LicensePool,
    Work,
)

from core.util import LanguageCodes
from novelist import NoveListAPI

def make_lanes(_db, library, definitions=None):

    lanes = core_make_lanes(_db, library, definitions)
    if lanes:
        return lanes

    # There was no configuration to create the lanes,
    # so go with  the default configuration instead.
    lanes = make_lanes_default(_db, library)
    return LaneList.from_description(_db, library, None, lanes)

def make_lanes_default(_db, library):
    """Create the default layout of lanes for the server configuration."""

    # The top-level LaneList includes a hidden lane for each
    # large-collection language with a number of displayed 
    # sublanes: 'Adult Fiction', 'Adult Nonfiction',
    # 'Young Adult Fiction', 'Young Adult Nonfiction', and 'Children'
    # sublanes. These sublanes contain additional sublanes.
    #
    # The top-level LaneList also includes a sublane named after each
    # small-collection language. Each such sublane contains "Adult
    # Fiction", "Adult Nonfiction", and "Children/YA" sublanes.
    #
    # Finally the top-level LaneList includes an "Other Languages" sublane
    # which covers all other languages. This lane contains sublanes for each
    # of the tiny-collection languages in the configuration.
    seen_languages = set()

    top_level_lanes = []

    def language_list(x):
        if isinstance(x, basestring):
            return x.split(',')
        return x

    for language_set in Configuration.large_collection_languages():
        languages = language_list(language_set)
        seen_languages = seen_languages.union(set(languages))
        top_level_lanes.extend(lanes_for_large_collection(_db, library, language_set))

    for language_set in Configuration.small_collection_languages():
        languages = language_list(language_set)
        seen_languages = seen_languages.union(set(languages))
        top_level_lanes.append(lane_for_small_collection(_db, library, language_set))

    other_languages_lane = lane_for_other_languages(_db, library, seen_languages)
    if other_languages_lane:
        top_level_lanes.append(other_languages_lane)

    return LaneList.from_description(_db, library, None, top_level_lanes)

def lanes_from_genres(_db, library, genres, **extra_args):
    """Turn genre info into a list of Lane objects."""

    genre_lane_instructions = {
        "Humorous Fiction" : dict(display_name="Humor"),
        "Media Tie-in SF" : dict(display_name="Movie and TV Novelizations"),
        "Suspense/Thriller" : dict(display_name="Thriller"),
        "Humorous Nonfiction" : dict(display_name="Humor"),
        "Political Science" : dict(display_name="Politics & Current Events"),
        "Periodicals" : dict(invisible=True)
    }

    lanes = []
    for descriptor in genres:
        if isinstance(descriptor, dict):
            name = descriptor['name']
        else:
            name = descriptor
        if classifier.genres.get(name):
            genredata = classifier.genres[name]
        else:
            genredata = GenreData(name, False)
        lane_args = dict(extra_args)
        if name in genre_lane_instructions.keys():
            instructions = genre_lane_instructions[name]
            if "display_name" in instructions:
                lane_args['display_name']=instructions.get('display_name')
            if "invisible" in instructions:
                lane_args['invisible']=instructions.get("invisible")
        lanes.append(genredata.to_lane(_db, library, **lane_args))
    return lanes

def lanes_for_large_collection(_db, library, languages):

    YA = Classifier.AUDIENCE_YOUNG_ADULT
    CHILDREN = Classifier.AUDIENCE_CHILDREN

    common_args = dict(
        languages=languages,
        include_best_sellers=True,
        include_staff_picks=True,
    )

    adult_fiction = Lane(
        _db, library, full_name="Adult Fiction", display_name="Fiction",
        genres=None,
        sublanes=lanes_from_genres(
            _db, library, fiction_genres, languages=languages,
            audiences=Classifier.AUDIENCES_ADULT,
        ),
        fiction=True, 
        audiences=Classifier.AUDIENCES_ADULT,
        **common_args
    )
    adult_nonfiction = Lane(
        _db, library, full_name="Adult Nonfiction", display_name="Nonfiction",
        genres=None,
        sublanes=lanes_from_genres(
            _db, library, nonfiction_genres, languages=languages,
            audiences=Classifier.AUDIENCES_ADULT,
        ),
        fiction=False, 
        audiences=Classifier.AUDIENCES_ADULT,
        **common_args
    )

    ya_common_args = dict(
        audiences=YA,
        languages=languages,
    )

    ya_fiction = Lane(
        _db, library, full_name="Young Adult Fiction", genres=None, fiction=True,
        include_best_sellers=True,
        include_staff_picks=True,        
        sublanes=[
            Lane(_db, library, full_name="YA Dystopian",
                 display_name="Dystopian", genres=[genres.Dystopian_SF],
                 **ya_common_args),
            Lane(_db, library, full_name="YA Fantasy", display_name="Fantasy",
                 genres=[genres.Fantasy], 
                 subgenre_behavior=Lane.IN_SAME_LANE, **ya_common_args),
            Lane(_db, library, full_name="YA Graphic Novels",
                 display_name="Comics & Graphic Novels",
                 genres=[genres.Comics_Graphic_Novels], **ya_common_args),
            Lane(_db, library, full_name="YA Literary Fiction",
                 display_name="Contemporary Fiction",
                 genres=[genres.Literary_Fiction], **ya_common_args),
            Lane(_db, library, full_name="YA LGBTQ Fiction", 
                 display_name="LGBTQ Fiction",
                 genres=[genres.LGBTQ_Fiction],
                 **ya_common_args),
            Lane(_db, library, full_name="Mystery & Thriller",
                 genres=[genres.Suspense_Thriller, genres.Mystery],
                 subgenre_behavior=Lane.IN_SAME_LANE, **ya_common_args),
            Lane(_db, library, full_name="YA Romance", display_name="Romance",
                 genres=[genres.Romance],
                 subgenre_behavior=Lane.IN_SAME_LANE, **ya_common_args),
            Lane(_db, library, full_name="YA Science Fiction",
                 display_name="Science Fiction",
                 genres=[genres.Science_Fiction],
                 subgenre_behavior=Lane.IN_SAME_LANE,
                 exclude_genres=[genres.Dystopian_SF, genres.Steampunk],
                 **ya_common_args),
            Lane(_db, library, full_name="YA Steampunk", genres=[genres.Steampunk],
                 subgenre_behavior=Lane.IN_SAME_LANE,
                 display_name="Steampunk", **ya_common_args),
            # TODO:
            # Paranormal -- what is it exactly?
        ],
        **ya_common_args
    )

    ya_nonfiction = Lane(
        _db, library, full_name="Young Adult Nonfiction", genres=None, fiction=False,
        include_best_sellers=True,
        include_staff_picks=True,
        sublanes=[
            Lane(_db, library, full_name="YA Biography", 
                 genres=genres.Biography_Memoir,
                 display_name="Biography",
                 **ya_common_args
                 ),
            Lane(_db, library, full_name="YA History",
                 genres=[genres.History, genres.Social_Sciences],
                 display_name="History & Sociology", 
                 subgenre_behavior=Lane.IN_SAME_LANE,
                 **ya_common_args
             ),
            Lane(_db, library, full_name="YA Life Strategies",
                 display_name="Life Strategies",
                 genres=[genres.Life_Strategies], 
                 **ya_common_args
                 ),
            Lane(_db, library, full_name="YA Religion & Spirituality", 
                 display_name="Religion & Spirituality",
                 genres=genres.Religion_Spirituality,
                 subgenre_behavior=Lane.IN_SAME_LANE,
                 **ya_common_args
                 )
        ],
        **ya_common_args
    )

    children_common_args = dict(
        audiences=genres.Classifier.AUDIENCE_CHILDREN,
        languages=languages,
    )

    children = Lane(
        _db, library, full_name="Children and Middle Grade", genres=None,
        fiction=Lane.BOTH_FICTION_AND_NONFICTION,
        include_best_sellers=True,
        include_staff_picks=True,
        searchable=True,
        sublanes=[
            Lane(_db, library, full_name="Picture Books", age_range=[0,1,2,3,4],
                 genres=None, fiction=Lane.BOTH_FICTION_AND_NONFICTION,
                 **children_common_args
             ),
            Lane(_db, library, full_name="Easy readers", age_range=[5,6,7,8],
                 genres=None, fiction=Lane.BOTH_FICTION_AND_NONFICTION,
                 **children_common_args
             ),
            Lane(_db, library, full_name="Chapter books", age_range=[9,10,11,12],
                 genres=None, fiction=Lane.BOTH_FICTION_AND_NONFICTION,
                 **children_common_args
             ),
            Lane(_db, library, full_name="Children's Poetry", 
                 display_name="Poetry books", genres=[genres.Poetry],
                 **children_common_args
             ),
            Lane(_db, library, full_name="Children's Folklore", display_name="Folklore",
                 genres=[genres.Folklore],
                 subgenre_behavior=Lane.IN_SAME_LANE,
                 **children_common_args
             ),
            Lane(_db, library, full_name="Children's Fantasy", display_name="Fantasy",
                 fiction=True,
                 genres=[genres.Fantasy], 
                 subgenre_behavior=Lane.IN_SAME_LANE,
                 **children_common_args
             ),
            Lane(_db, library, full_name="Children's SF", display_name="Science Fiction",
                 fiction=True, genres=[genres.Science_Fiction],
                 subgenre_behavior=Lane.IN_SAME_LANE,
                 **children_common_args
             ),
            Lane(_db, library, full_name="Realistic fiction", 
                 fiction=True, genres=[genres.Literary_Fiction],
                 subgenre_behavior=Lane.IN_SAME_LANE,
                 **children_common_args
             ),
            Lane(_db, library, full_name="Children's Graphic Novels",
                 display_name="Comics & Graphic Novels",
                 genres=[genres.Comics_Graphic_Novels],
                 **children_common_args
             ),
            Lane(_db, library, full_name="Biography", 
                 genres=[genres.Biography_Memoir],
                 subgenre_behavior=Lane.IN_SAME_LANE,
                 **children_common_args
             ),
            Lane(_db, library, full_name="Historical fiction", 
                 genres=[genres.Historical_Fiction],
                 subgenre_behavior=Lane.IN_SAME_LANE, 
                 **children_common_args
             ),
            Lane(_db, library, full_name="Informational books", genres=None,
                 fiction=False, exclude_genres=[genres.Biography_Memoir],
                 subgenre_behavior=Lane.IN_SAME_LANE,
                 **children_common_args
             )
        ],
        **children_common_args
    )

    name = LanguageCodes.name_for_languageset(languages)
    lane = Lane(
        _db, library, full_name=name,
        genres=None,
        sublanes=[adult_fiction, adult_nonfiction, ya_fiction, ya_nonfiction, children],
        fiction=Lane.BOTH_FICTION_AND_NONFICTION,
        searchable=True,
        invisible=True,
        **common_args
    )

    return [lane]

def lane_for_small_collection(_db, library, languages):

    YA = Classifier.AUDIENCE_YOUNG_ADULT
    CHILDREN = Classifier.AUDIENCE_CHILDREN

    common_args = dict(
        include_best_sellers=False,
        include_staff_picks=False,
        languages=languages,
        genres=None,
    )

    adult_fiction = Lane(
        _db, library, full_name="Adult Fiction",
        display_name="Fiction",
        fiction=True, 
        audiences=Classifier.AUDIENCES_ADULT,
        **common_args
    )
    adult_nonfiction = Lane(
        _db, library, full_name="Adult Nonfiction", 
        display_name="Nonfiction",
        fiction=False, 
        audiences=Classifier.AUDIENCES_ADULT,
        **common_args
    )

    ya_children = Lane(
        _db, library, 
        full_name="Children & Young Adult", 
        fiction=Lane.BOTH_FICTION_AND_NONFICTION,
        audiences=[YA, CHILDREN],
        **common_args
    )

    name = LanguageCodes.name_for_languageset(languages)
    lane = Lane(
        _db, library, full_name=name, languages=languages, 
        sublanes=[adult_fiction, adult_nonfiction, ya_children],
        searchable=True
    )
    lane.default_for_language = True
    return lane

def lane_for_other_languages(_db, library, exclude_languages):
    """Make a lane for all books not in one of the given languages."""

    language_lanes = []
    other_languages = Configuration.tiny_collection_languages()

    if not other_languages:
        return None

    for language_set in other_languages:
        name = LanguageCodes.name_for_languageset(language_set)
        language_lane = Lane(
            _db, library, full_name=name,
            genres=None,
            fiction=Lane.BOTH_FICTION_AND_NONFICTION,
            searchable=True,
            languages=language_set,
        )
        language_lanes.append(language_lane)

    lane = Lane(
        _db, library, 
        full_name="Other Languages", 
        sublanes=language_lanes,
        exclude_languages=exclude_languages,
        searchable=True,
        genres=None,
    )
    lane.default_for_language = True
    return lane


class WorkBasedLane(QueryGeneratedLane):
    """A query-based lane connected on a particular Work"""

    DISPLAY_NAME = None
    ROUTE = None

    def __init__(self, _db, library, work, full_name, display_name=None,
                 sublanes=[], invisible=False, **kwargs):
        self.work = work
        self.edition = work.presentation_edition

        languages = [self.edition.language]

        self.source_audience = self.work.audience
        audiences = self.audiences_list_from_source()

        display_name = display_name or self.DISPLAY_NAME

        super(WorkBasedLane, self).__init__(
            _db, library, full_name, display_name=display_name, sublanes=sublanes,
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
            return Classifier.AUDIENCE_CHILDREN


class RelatedBooksLane(WorkBasedLane):
    """A lane of Works all related to a given Work by various criteria.

    Current criteria--represented by sublanes--include a shared
    contributor (ContributorLane), same series (SeriesLane), or
    third-party recommendation relationship (Recommendationlane).
    """
    DISPLAY_NAME = "Related Books"
    ROUTE = 'related_books'

    def __init__(self, _db, library, work, full_name, display_name=None,
                 novelist_api=None):
        super(RelatedBooksLane, self).__init__(
            _db, library, work, full_name, display_name=display_name,
            invisible=True, searchable=False,
        )
        sublanes = self._get_sublanes(_db, novelist_api)
        if not sublanes:
            raise ValueError(
                "No related books for %s by %s" % (self.work.title, self.work.author)
            )
        self.set_sublanes(self.library, sublanes, [])

    def _get_sublanes(self, _db, novelist_api):
        sublanes = list()

        for contributor_lane in self._contributor_sublanes(_db):
            sublanes.append(contributor_lane)

        for recommendation_lane in self._recommendation_sublane(_db, novelist_api):
            sublanes.append(recommendation_lane)

        # Create a series sublane.
        series_name = self.edition.series
        if series_name:
            sublanes.append(SeriesLane(_db, self.library, series_name, parent=self))

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
                _db, self.library, contributor_name, parent=self
            )
            yield contributor_lane

    def _recommendation_sublane(self, _db, novelist_api):
        """Create a recommendations sublane."""
        try:
            lane_name = "Recommendations for %s by %s" % (
                self.work.title, self.work.author
            )
            recommendation_lane = RecommendationLane(
                _db, self.library, self.work, lane_name, novelist_api=novelist_api,
                parent=self
            )
            if recommendation_lane.recommendations:
                yield recommendation_lane
        except ValueError, e:
            # NoveList isn't configured.
            pass

    def lane_query_hook(self, qu, **kwargs):
        # This lane is composed entirely of sublanes and
        # should only be used to create groups feeds.
        return None


class RecommendationLane(WorkBasedLane):
    """A lane of recommended Works based on a particular Work"""

    DISPLAY_NAME = "Recommended Books"
    ROUTE = "recommendations"
    MAX_CACHE_AGE = 7*24*60*60      # one week

    def __init__(self, _db, library, work, full_name, display_name=None,
                 novelist_api=None, parent=None):
        super(RecommendationLane, self).__init__(
            _db, library, work, full_name, display_name=display_name,
            parent=parent, searchable=False,
        )
        self.api = novelist_api or NoveListAPI.from_config(library)
        self.recommendations = self.fetch_recommendations()

    def fetch_recommendations(self):
        """Get identifiers of recommendations for this LicensePool"""

        metadata = self.api.lookup(self.edition.primary_identifier)
        if metadata:
            metadata.filter_recommendations(self._db)
            return metadata.recommendations
        return []

    def lane_query_hook(self, qu, work_model=Work):
        if not self.recommendations:
            return None

        if work_model != Work:
            qu = qu.join(LicensePool.identifier)
        qu = Work.from_identifiers(
            self._db, self.recommendations, base_query=qu
        )
        return qu


class SeriesLane(QueryGeneratedLane):
    """A lane of Works in a particular series"""

    ROUTE = 'series'
    MAX_CACHE_AGE = 48*60*60    # 48 hours

    def __init__(self, _db, library, series_name, parent=None, languages=None,
                 audiences=None):
        if not series_name:
            raise ValueError("SeriesLane can't be created without series")
        self.series = series_name
        full_name = display_name = self.series

        if parent:
            # In an attempt to secure the accurate series, limit the
            # listing to the source's audience sourced from parent data.
            audiences = [parent.source_audience]

        super(SeriesLane, self).__init__(
            _db, library, full_name, parent=parent, display_name=display_name,
            audiences=audiences, languages=languages, searchable=False,
        )

    @property
    def url_arguments(self):
        kwargs = dict(
            series_name=self.series,
            languages=self.language_key,
            audiences=self.audience_key
        )
        return self.ROUTE, kwargs

    def featured_works(self, use_materialized_works=True):
        if not use_materialized_works:
            qu = self.works()
        else:
            qu = self.materialized_works()

        # Aliasing Edition here allows this query to function
        # regardless of work_model and existing joins.
        work_edition = aliased(Edition)
        qu = qu.join(work_edition).order_by(work_edition.series_position, work_edition.title)
        target_size = self.library.featured_lane_size
        qu = qu.limit(target_size)
        return qu.all()

    def lane_query_hook(self, qu, **kwargs):
        if not self.series:
            return None

        # Aliasing Edition here allows this query to function
        # regardless of work_model and existing joins.
        work_edition = aliased(Edition)
        qu = qu.join(work_edition).filter(work_edition.series==self.series)
        return qu


class ContributorLane(QueryGeneratedLane):
    """A lane of Works written by a particular contributor"""

    ROUTE = 'contributor'
    MAX_CACHE_AGE = 48*60*60    # 48 hours

    def __init__(self, _db, library, contributor_name,
                 parent=None, languages=None, audiences=None):
        if not contributor_name:
            raise ValueError("ContributorLane can't be created without contributor")

        self.contributor_name = contributor_name
        full_name = display_name = contributor_name
        super(ContributorLane, self).__init__(
            _db, library, full_name, display_name=display_name, parent=parent,
            audiences=audiences, languages=languages, searchable=False,
        )
        self.contributors = self._db.query(Contributor)\
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

    def lane_query_hook(self, qu, **kwargs):
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

        return qu
