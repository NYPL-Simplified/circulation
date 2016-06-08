from nose.tools import set_trace
import core.classifier as genres
from config import Configuration
from core.classifier import (
    Classifier,
    fiction_genres,
    nonfiction_genres,
)
from core import classifier

from core.lane import (
    Lane,
    LaneList,
)
from core.model import (
    Work,
    Edition,
)

from core.util import LanguageCodes
from novelist import NoveListAPI

def make_lanes(_db, definitions=None):

    definitions = definitions or Configuration.policy(
        Configuration.LANES_POLICY
    )

    if not definitions:
        lanes = make_lanes_default(_db)
    else:
        lanes = [Lane(_db=_db, **definition) for definition in definitions]

    return LaneList.from_description(_db, None, lanes)

def make_lanes_default(_db):
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
        top_level_lanes.extend(lanes_for_large_collection(_db, language_set))

    for language_set in Configuration.small_collection_languages():
        languages = language_list(language_set)
        seen_languages = seen_languages.union(set(languages))
        top_level_lanes.append(lane_for_small_collection(_db, language_set))

    other_languages_lane = lane_for_other_languages(_db, seen_languages)
    if other_languages_lane:
        top_level_lanes.append(other_languages_lane)

    return LaneList.from_description(_db, None, top_level_lanes)

def lanes_from_genres(_db, genres, **extra_args):
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
        genredata = classifier.genres[name]
        lane_args = dict(extra_args)
        if name in genre_lane_instructions.keys():
            instructions = genre_lane_instructions[name]
            if "display_name" in instructions:
                lane_args['display_name']=instructions.get('display_name')
            if "invisible" in instructions:
                lane_args['invisible']=instructions.get("invisible")
        lanes.append(genredata.to_lane(_db, **lane_args))
    return lanes

def lanes_for_large_collection(_db, languages):

    YA = Classifier.AUDIENCE_YOUNG_ADULT
    CHILDREN = Classifier.AUDIENCE_CHILDREN

    common_args = dict(
        languages=languages,
        include_best_sellers=True,
        include_staff_picks=True,
    )

    adult_fiction = Lane(
        _db, full_name="Adult Fiction", display_name="Fiction",
        genres=None,
        sublanes=lanes_from_genres(
            _db, fiction_genres, languages=languages,
            audiences=Classifier.AUDIENCES_ADULT,
        ),
        fiction=True, 
        audiences=Classifier.AUDIENCES_ADULT,
        **common_args
    )
    adult_nonfiction = Lane(
        _db, full_name="Adult Nonfiction", display_name="Nonfiction",
        genres=None,
        sublanes=lanes_from_genres(
            _db, nonfiction_genres, languages=languages,
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
        _db, full_name="Young Adult Fiction", genres=None, fiction=True,
        include_best_sellers=True,
        include_staff_picks=True,        
        sublanes=[
            Lane(_db, full_name="YA Dystopian",
                 display_name="Dystopian", genres=[genres.Dystopian_SF],
                 **ya_common_args),
            Lane(_db, full_name="YA Fantasy", display_name="Fantasy",
                 genres=[genres.Fantasy], 
                 subgenre_behavior=Lane.IN_SAME_LANE, **ya_common_args),
            Lane(_db, full_name="YA Graphic Novels",
                 display_name="Comics & Graphic Novels",
                 genres=[genres.Comics_Graphic_Novels], **ya_common_args),
            Lane(_db, full_name="YA Literary Fiction",
                 display_name="Contemporary Fiction",
                 genres=[genres.Literary_Fiction], **ya_common_args),
            Lane(_db, full_name="YA LGBTQ Fiction", 
                 display_name="LGBTQ Fiction",
                 genres=[genres.LGBTQ_Fiction],
                 **ya_common_args),
            Lane(_db, full_name="Mystery & Thriller",
                 genres=[genres.Suspense_Thriller, genres.Mystery],
                 subgenre_behavior=Lane.IN_SAME_LANE, **ya_common_args),
            Lane(_db, full_name="YA Romance", display_name="Romance",
                 genres=[genres.Romance],
                 subgenre_behavior=Lane.IN_SAME_LANE, **ya_common_args),
            Lane(_db, full_name="YA Science Fiction",
                 display_name="Science Fiction",
                 genres=[genres.Science_Fiction],
                 subgenre_behavior=Lane.IN_SAME_LANE,
                 exclude_genres=[genres.Dystopian_SF, genres.Steampunk],
                 **ya_common_args),
            Lane(_db, full_name="YA Steampunk", genres=[genres.Steampunk],
                 subgenre_behavior=Lane.IN_SAME_LANE,
                 display_name="Steampunk", **ya_common_args),
            # TODO:
            # Paranormal -- what is it exactly?
        ],
        **ya_common_args
    )

    ya_nonfiction = Lane(
        _db, full_name="Young Adult Nonfiction", genres=None, fiction=False,
        include_best_sellers=True,
        include_staff_picks=True,
        sublanes=[
            Lane(_db, full_name="YA Biography", 
                 genres=genres.Biography_Memoir,
                 display_name="Biography",
                 **ya_common_args
                 ),
            Lane(_db, full_name="YA History",
                 genres=[genres.History, genres.Social_Sciences],
                 display_name="History & Sociology", 
                 subgenre_behavior=Lane.IN_SAME_LANE,
                 **ya_common_args
             ),
            Lane(_db, full_name="YA Life Strategies",
                 display_name="Life Strategies",
                 genres=[genres.Life_Strategies], 
                 **ya_common_args
                 ),
            Lane(_db, full_name="YA Religion & Spirituality", 
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
        _db, full_name="Children and Middle Grade", genres=None,
        fiction=Lane.BOTH_FICTION_AND_NONFICTION,
        include_best_sellers=True,
        include_staff_picks=True,
        sublanes=[
            Lane(_db, full_name="Picture Books", age_range=[0,1,2,3,4],
                 genres=None, fiction=Lane.BOTH_FICTION_AND_NONFICTION,
                 **children_common_args
             ),
            Lane(_db, full_name="Easy readers", age_range=[5,6,7,8],
                 genres=None, fiction=Lane.BOTH_FICTION_AND_NONFICTION,
                 **children_common_args
             ),
            Lane(_db, full_name="Chapter books", age_range=[9,10,11,12],
                 genres=None, fiction=Lane.BOTH_FICTION_AND_NONFICTION,
                 **children_common_args
             ),
            Lane(_db, full_name="Children's Poetry", 
                 display_name="Poetry books", genres=[genres.Poetry],
                 **children_common_args
             ),
            Lane(_db, full_name="Children's Folklore", display_name="Folklore",
                 genres=[genres.Folklore],
                 subgenre_behavior=Lane.IN_SAME_LANE,
                 **children_common_args
             ),
            Lane(_db, full_name="Children's Fantasy", display_name="Fantasy",
                 fiction=True,
                 genres=[genres.Fantasy], 
                 subgenre_behavior=Lane.IN_SAME_LANE,
                 **children_common_args
             ),
            Lane(_db, full_name="Children's SF", display_name="Science Fiction",
                 fiction=True, genres=[genres.Science_Fiction],
                 subgenre_behavior=Lane.IN_SAME_LANE,
                 **children_common_args
             ),
            Lane(_db, full_name="Realistic fiction", 
                 fiction=True, genres=[genres.Literary_Fiction],
                 subgenre_behavior=Lane.IN_SAME_LANE,
                 **children_common_args
             ),
            Lane(_db, full_name="Children's Graphic Novels",
                 display_name="Comics & Graphic Novels",
                 genres=[genres.Comics_Graphic_Novels],
                 **children_common_args
             ),
            Lane(_db, full_name="Biography", 
                 genres=[genres.Biography_Memoir],
                 subgenre_behavior=Lane.IN_SAME_LANE,
                 **children_common_args
             ),
            Lane(_db, full_name="Historical fiction", 
                 genres=[genres.Historical_Fiction],
                 subgenre_behavior=Lane.IN_SAME_LANE, 
                 **children_common_args
             ),
            Lane(_db, full_name="Informational books", genres=None,
                 fiction=False, exclude_genres=[genres.Biography_Memoir],
                 **children_common_args
             )
        ],
        **children_common_args
    )

    name = LanguageCodes.name_for_languageset(languages)
    lane = Lane(
        _db, full_name=name,
        genres=None,
        sublanes=[adult_fiction, adult_nonfiction, ya_fiction, ya_nonfiction, children],
        fiction=Lane.BOTH_FICTION_AND_NONFICTION,
        searchable=True,
        invisible=True,
        **common_args
    )

    return [lane]

def lane_for_small_collection(_db, languages):

    YA = Classifier.AUDIENCE_YOUNG_ADULT
    CHILDREN = Classifier.AUDIENCE_CHILDREN

    common_args = dict(
        include_best_sellers=False,
        include_staff_picks=False,
        languages=languages,
        genres=None,
    )

    adult_fiction = Lane(
        _db, full_name="Adult Fiction",
        display_name="Fiction",
        fiction=True, 
        audiences=Classifier.AUDIENCES_ADULT,
        **common_args
    )
    adult_nonfiction = Lane(
        _db, full_name="Adult Nonfiction", 
        display_name="Nonfiction",
        fiction=False, 
        audiences=Classifier.AUDIENCES_ADULT,
        **common_args
    )

    ya_children = Lane(
        _db, 
        full_name="Children & Young Adult", 
        fiction=Lane.BOTH_FICTION_AND_NONFICTION,
        audiences=[YA, CHILDREN],
        **common_args
    )

    name = LanguageCodes.name_for_languageset(languages)
    lane = Lane(
        _db, full_name=name, languages=languages, 
        sublanes=[adult_fiction, adult_nonfiction, ya_children],
        searchable=True
    )
    lane.default_for_language = True
    return lane

def lane_for_other_languages(_db, exclude_languages):
    """Make a lane for all books not in one of the given languages."""

    language_lanes = []
    other_languages = Configuration.tiny_collection_languages()

    if not other_languages:
        return None

    for language_set in other_languages:
        name = LanguageCodes.name_for_languageset(language_set)
        language_lane = Lane(
            _db, full_name=name,
            genres=None,
            fiction=Lane.BOTH_FICTION_AND_NONFICTION,
            searchable=True,
            languages=language_set,
        )
        language_lanes.append(language_lane)

    lane = Lane(
        _db, 
        full_name="Other Languages", 
        sublanes=language_lanes,
        exclude_languages=exclude_languages,
        searchable=True,
        genres=None,
    )
    lane.default_for_language = True
    return lane

class LicensePoolBasedLane(Lane):
    """A lane based on a particular LicensePool"""

    DISPLAY_NAME = None
    MAX_CACHE_AGE = 14*24*60*60      # two weeks

    def __init__(self, _db, license_pool, full_name,
                 display_name=None, sublanes=[]):
        self.license_pool = license_pool
        display_name = display_name or self.DISPLAY_NAME
        super(LicensePoolBasedLane, self).__init__(
            _db, full_name, display_name=display_name,
            sublanes=sublanes
        )

    def apply_filters(self, qu, facets=None, pagination=None,
            work_model=Work, edition_model=Edition):
        """Incorporates additional filters to be run on a query of all Works
        in the db or materialized view

        :return: query
        """
        raise NotImplementedError()


class RelatedBooksLane(LicensePoolBasedLane):
    """A lane of Works all related to the Work of a particular LicensePool

    Sublanes currently include a SeriesLane and a RecommendationLane"""

    DISPLAY_NAME = "Related Books"

    def __init__(self, _db, license_pool, full_name, display_name=None,
                 mock_api=None):
        sublanes = self._get_sublanes(_db, license_pool, mock_api=mock_api)
        if not sublanes:
            edition = license_pool.presentation_edition
            raise ValueError(
                "No related books for %s by %s" % (edition.title, edition.author)
            )
        super(RelatedBooksLane, self).__init__(
            _db, license_pool, full_name, display_name=display_name,
            sublanes=sublanes
        )

    def _get_sublanes(self, _db, license_pool, mock_api=None):
        sublanes = []

        # Create a recommendations sublane.
        try:
            lane_name = "Recommendations for %s by %s" % (
                license_pool.work.title, license_pool.work.author
            )
            sublanes.append(RecommendationLane(
                _db, license_pool, lane_name, mock_api=mock_api
            ))
        except ValueError, e:
            # NoveList isn't configured.
            pass

        # Create a series sublane.
        series = license_pool.presentation_edition.series
        if series:
            lane_name = SeriesLane.lane_name_from_series_title(series)
            sublanes.append(SeriesLane(_db, license_pool, lane_name))

        return sublanes

    def apply_filters(self, qu, facets=None, pagination=None, work_model=Work,
            edition_model=Edition):
        # This lane is composed entirely of sublanes.
        return None


class SeriesLane(LicensePoolBasedLane):
    """A lane of Works in a series based on a particular LicensePool"""

    DISPLAY_NAME = "Other Books in this Series"

    def apply_filters(self, qu, facets=None, pagination=None, work_model=Work,
            edition_model=Edition):
        edition = self.license_pool.presentation_edition
        series = edition.series
        if not series:
            return None

        qu = self.only_show_ready_deliverable_works(qu, work_model)
        qu = qu.filter(
            Edition.series==series,
            Edition.id!=edition.id
        )
        return qu

    @classmethod
    def lane_name_from_series_title(cls, series_title):
        feed_title = "Other Books in "
        if series_title[:3].lower() != 'the':
            feed_title += "the "
        feed_title += series_title
        if series_title.lower().endswith(' series'):
            return feed_title
        return feed_title + ' series'


class RecommendationLane(LicensePoolBasedLane):
    """A lane of recommended Works based on a particular LicensePool"""

    DISPLAY_NAME = "Recommended Books"
    MAX_CACHE_AGE = 7*24*60*60      # one week

    def __init__(self, _db, license_pool, full_name, display_name=None,
            mock_api=None):
        self.api = mock_api or NoveListAPI.from_config(_db)
        super(RecommendationLane, self).__init__(
            _db, license_pool, full_name, display_name=display_name
        )
        self.recommendations = self.fetch_recommendations()

    def fetch_recommendations(self):
        """Get identifiers of recommendations for this LicensePool"""

        metadata = self.api.lookup(self.license_pool.identifier)
        if metadata:
            metadata.filter_recommendations(self._db)
            return metadata.recommendations
        return []

    def apply_filters(self, qu, facets=None, pagination=None, work_model=Work,
            edition_model=Edition):
        identifier = self.license_pool.identifier

        qu = self.only_show_ready_deliverable_works(qu, work_model)
        if self.recommendations:
            qu = Work.from_identifiers(
                self._db, self.recommendations, base_query=qu
            )
            return qu
        return None
