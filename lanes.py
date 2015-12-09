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

from core.util import LanguageCodes

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
    # which covers all other languages. This lane contains "Adult Fiction",
    # "Adult Nonfiction", and "Children/YA" sublanes.
    seen_languages = set()

    top_level_lanes = []

    def language_list(x):
        if isinstance(language_set, basestring):
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

    top_level_lanes.append(lane_for_other_languages(_db, seen_languages))

    return LaneList.from_description(_db, None, top_level_lanes)

def lanes_from_genres(_db, genres, **extra_args):
    """Turn genre info into a list of Lane objects."""
    lanes = []
    for descriptor in genres:
        if isinstance(descriptor, tuple):
            name = descriptor[0]
        elif isinstance(descriptor, dict):
            name = descriptor['full_name']
        else:
            name = descriptor
        genredata = classifier.genres[name]
        lanes.append(genredata.to_lane(_db, **extra_args))
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
        sublanes=lanes_from_genres(_db, fiction_genres, languages=languages),
        fiction=True, 
        audiences=Classifier.AUDIENCES_ADULT,
        **common_args
    )
    adult_nonfiction = Lane(
        _db, full_name="Adult Nonfiction", display_name="Nonfiction",
        genres=None,
        sublanes=lanes_from_genres(_db, nonfiction_genres, languages=languages),
        fiction=False, 
        audiences=Classifier.AUDIENCES_ADULT,
        **common_args
    )

    ya_fiction = Lane(
        _db, full_name="Young Adult Fiction", genres=None, fiction=True,
        audiences=YA,
        sublanes=[
            Lane(_db, full_name="YA Dystopian",
                 display_name="Dystopian", genres=[genres.Dystopian_SF],
                 audiences=YA),
            Lane(_db, full_name="YA Fantasy", display_name="Fantasy",
                 genres=[genres.Fantasy], 
                 subgenre_behavior=Lane.IN_SAME_LANE, audiences=YA),
            Lane(_db, full_name="YA Graphic Novels",
                 display_name="Comics & Graphic Novels",
                 genres=[genres.Comics_Graphic_Novels], audiences=YA),
            Lane(_db, full_name="YA Literary Fiction",
                 display_name="Contemporary Fiction",
                 genres=[genres.Literary_Fiction], audiences=YA),
            Lane(_db, "LGBTQ Fiction", genres=[genres.LGBTQ_Fiction],
                 audiences=YA),
            Lane(_db, full_name="Mystery & Thriller",
                 genres=[genres.Suspense_Thriller, genres.Mystery],
                 subgenre_behavior=Lane.IN_SAME_LANE, audiences=YA),
            Lane(_db, full_name="YA Romance", display_name="Romance",
                 genres=[genres.Romance],
                 subgenre_behavior=Lane.IN_SAME_LANE, audiences=YA),
            Lane(_db, full_name="YA Science Fiction",
                 display_name="Science Fiction",
                 genres=[genres.Science_Fiction],
                 subgenre_behavior=Lane.IN_SAME_LANE,
                 exclude_genres=[genres.Dystopian_SF, genres.Steampunk],
                 audiences=YA),
            Lane(_db, full_name="YA Steampunk", genres=[genres.Steampunk],
                 subgenre_behavior=Lane.IN_SAME_LANE,
                 display_name="Steampunk", audiences=YA),
            # TODO:
            # Paranormal -- what is it exactly?
        ],
        **common_args
    )

    ya_nonfiction = Lane(
        _db, full_name="Young Adult Nonfiction", genres=None, fiction=False,
        languages=languages,
        audiences=YA,
        include_best_sellers=True,
        include_staff_picks=True,
        sublanes=[
            Lane(_db, full_name="YA Biography", 
                 genres=genres.Biography_Memoir,
                 display_name="Biography",
                 ),
            Lane(_db, full_name="YA History",
                 genres=[genres.History, genres.Social_Sciences],
                 display_name="History & Sociology", 
                 subgenre_behavior=Lane.IN_SAME_LANE,
             ),
            Lane(_db, full_name="Life Strategies",
                 genres=[genres.Life_Strategies], audiences=YA,
                 ),
            Lane(_db, full_name="YA Religion & Spirituality", 
                 display_name="Religion & Spirituality",
                 genres=genres.Religion_Spirituality,
                 subgenre_behavior=Lane.IN_SAME_LANE,
                 )
        ],
    )

    children = Lane(
        _db, full_name="Children and Middle Grade", genres=None,
        languages=languages,
        fiction=Lane.BOTH_FICTION_AND_NONFICTION,
        audiences=genres.Classifier.AUDIENCE_CHILDREN,
        include_best_sellers=True,
        include_staff_picks=True,
        sublanes=[
            Lane(_db, full_name="Picture Books", age_range=[0,1,2,3,4],
                 genres=None, fiction=Lane.BOTH_FICTION_AND_NONFICTION,
             ),
            Lane(_db, full_name="Easy readers", age_range=[5,6,7,8],
                 genres=None, fiction=Lane.BOTH_FICTION_AND_NONFICTION,
             ),
            Lane(_db, full_name="Chapter books", age_range=[9,10,11,12],
                 genres=None, fiction=Lane.BOTH_FICTION_AND_NONFICTION,
             ),
            Lane(_db, full_name="Children's Poetry", 
                 display_name="Poetry books", genres=[genres.Poetry],
             ),
            Lane(_db, full_name="Children's Folklore", display_name="Folklore",
                 genres=[genres.Folklore],
                 subgenre_behavior=Lane.IN_SAME_LANE
             ),
            Lane(_db, full_name="Children's Fantasy", display_name="Fantasy",
                 fiction=True,
                 genres=[genres.Fantasy], 
                 subgenre_behavior=Lane.IN_SAME_LANE
             ),
            Lane(_db, full_name="Children's SF", display_name="Science Fiction",
                 fiction=True, genres=[genres.Science_Fiction],
                 subgenre_behavior=Lane.IN_SAME_LANE
             ),
            Lane(_db, full_name="Realistic fiction", 
                 fiction=True, genres=[genres.Literary_Fiction],
                 subgenre_behavior=Lane.IN_SAME_LANE
             ),
            Lane(_db, full_name="Children's Graphic Novels",
                 display_name="Comics & Graphic Novels",
                 genres=[genres.Comics_Graphic_Novels]
             ),
            Lane(_db, full_name="Biography", 
                 genres=[genres.Biography_Memoir],
                 subgenre_behavior=Lane.IN_SAME_LANE
             ),
            Lane(_db, full_name="Historical fiction", 
                 genres=[genres.Historical_Fiction],
                 subgenre_behavior=Lane.IN_SAME_LANE, 
             ),
            Lane(_db, full_name="Informational books", genres=None,
                 fiction=False, exclude_genres=[genres.Biography_Memoir],
             )
        ],
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

    YA = Classifier.AUDIENCE_YOUNG_ADULT
    CHILDREN = Classifier.AUDIENCE_CHILDREN

    common_args = dict(
        exclude_languages=exclude_languages,
        genres=None,
    )

    adult_fiction = Lane(
        _db, 
        full_name="Adult Fiction",
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

    lane = Lane(
        _db, 
        full_name="Other Languages", 
        sublanes=[adult_fiction, adult_nonfiction, ya_children],
        searchable=True,
        **common_args
    )
    lane.default_for_language = True
    return lane
