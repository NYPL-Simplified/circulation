from nose.tools import set_trace
import core.classifier as genres
from core.classifier import (
    Classifier,
)
from core.model import (
    Lane,
    LaneList,
    Work,
)

def make_lanes(_db):

    # First let's create the complicated subgenres of fiction.
    non_erotica_romance = [
        genres.Romance, genres.Contemporary_Romance,
        genres.Historical_Romance, genres.Paranormal_Romance,
        genres.Regency_Romance, genres.Suspense_Romance
    ]
    romance = Lane(_db, name="Romance",
              genres=non_erotica_romance,
              include_subgenres=False,
              fiction=True,
              audience=Classifier.AUDIENCE_ADULT,
              sublanes=[
                  Lane(_db, name="General Romance",
                       genres=[genres.Romance, genres.Contemporary_Romance]),
                  #Lane(_db, name="Story-Driven Romance",
                  #     genres=non_erotica_romance, include_subgenres=False,
                  #     appeal=Work.STORY_APPEAL),
                  #Lane(_db, name="Character-Driven Romance",
                  #     genres=non_erotica_romance, include_subgenres=False,
                  #     appeal=Work.CHARACTER_APPEAL),
                  #Lane(_db, name="Setting-Driven Romance",
                  #     genres=non_erotica_romance, include_subgenres=False,
                  #     appeal=Work.SETTING_APPEAL),
                  #Lane(_db, name="Language-Driven Romance",
                  #     genres=non_erotica_romance, include_subgenres=False,
                  #     appeal=Work.LANGUAGE_APPEAL),

                  genres.Historical_Romance,
                  genres.Paranormal_Romance,
                  genres.Regency_Romance,
                  genres.Suspense_Romance,
                  genres.Erotica,
              ],
          )

    mystery = Lane(_db, name="Crime, Thrillers & Mystery",
                   genres = [genres.Crime_Thrillers_Mystery],
                   include_subgenres=True,
                   fiction=Lane.BOTH_FICTION_AND_NONFICTION,
                   audience=Classifier.AUDIENCE_ADULT,
                   sublanes=[
                       genres.Mystery,
                       genres.Women_Detectives,
                       genres.Police_Procedurals,
                       genres.Thrillers,
                       Lane(_db, name="True Crime",
                            genres=[genres.True_Crime], fiction=False),
                   ],
               )

    african_american = Lane(
        _db, name="African-American",
        fiction=True,
        genres=[genres.African_American, genres.Urban_Fiction],
    )

    adventure = Lane(_db, name="Adventure Fiction",
                     genres=[genres.Action_Adventure], fiction=True)

    religious_fiction = Lane(
        _db, name="Religious Fiction",
        genres=[genres.Religion_Spirituality,
                genres.Body_Mind_Spirit,
                genres.Religious_Fiction,
                genres.Christianity],
        include_subgenres=False,
        fiction=True,
    )

    #
    # Now we're ready to construct the Fiction lane itself.
    #
    fiction = Lane(
        _db, name="Fiction",
        fiction=True,
        audience=genres.Classifier.AUDIENCE_ADULT,
        genres=[],
        sublanes=[
            adventure,
            african_american,
            genres.Classics,
            genres.Fantasy,
            Lane(_db, name="General Fiction",
                 genres=Lane.UNCLASSIFIED),
            genres.Graphic_Novels_Comics,
            genres.Historical_Fiction,
            genres.Horror,
            Lane(_db, name="Literary Fiction",
                 genres=[genres.Literary_Fiction, genres.Literary_Collections],
                 include_subgenres=False
             ),
            mystery,
            religious_fiction,
            romance,
            genres.Science_Fiction,
        ],
    )

    #
    # Now let's create the subgenres of nonfiction.
    #

    # TODO: we are missing coverage for African-American nonfiction
    # now that "African-American" was moved to fiction.
     
    crafts_hobbies_games = Lane(
        _db, name="Crafts, Hobbies & Games",
        genres = [
            genres.Crafts_Hobbies_Games,
            genres.Antiques_Collectibles,
        ],
        include_subgenres=False,
        fiction=False,
    )
    hobbies_and_home = Lane(
        _db, name="Hobbies & Home",
        genres = [
            genres.Antiques_Collectibles,
            genres.Crafts_Cooking_Garden,
            genres.Crafts_Hobbies_Games,
            genres.Gardening,
            genres.House_Home,
            genres.Pets,
        ],
        include_subgenres=False,
        fiction=False,
        audience=Classifier.AUDIENCE_ADULT,
        sublanes=[
            genres.Gardening,
            crafts_hobbies_games,
            genres.House_Home,
            genres.Pets,
        ],
    )

    religion = Lane(
        _db, name="Religion & Spirituality",
        genres = [genres.Religion_Spirituality],
        include_subgenres=True,
        fiction=Lane.BOTH_FICTION_AND_NONFICTION,
        audience=Classifier.AUDIENCE_ADULT,
        sublanes=[
            genres.Buddhism,
            Lane(_db, name="Christianity",
                 genres=[genres.Christianity],
                 fiction=False),
            Lane(_db, name="General Religion & Spirituality",
                 genres=[genres.Religion_Spirituality, genres.Body_Mind_Spirit,
                         genres.Hinduism],
                 include_subgenres=False, fiction=False),
            genres.Islam,
            genres.Judaism,
            genres.New_Age,
        ],
    )

    science = Lane(_db, name="Science & Tech",
                   genres = [genres.Science_Technology_Nature],
                   include_subgenres=True,
                   audience=Classifier.AUDIENCE_ADULT,
                   sublanes=[
                       genres.Computers,
                       genres.Mathematics,
                       genres.Medical,
                       genres.Nature,
                       genres.Psychology,
                       genres.Science,
                       Lane(_db, name="Social Science",
                            genres=[genres.Social_Science],
                            include_subgenres=False),
                       Lane(_db, name="Technology",
                            genres=[genres.Technology_Engineering],
                            include_subgenres=False,
                            fiction=False),
                   ],
               )

    philosophy = Lane(
        _db, name="Criticism & Philosophy",
        genres = [genres.Criticism_Philosophy],
        include_subgenres=True,
        fiction=False,
        audience=Classifier.AUDIENCE_ADULT,
        sublanes=[
            genres.Language_Arts_Disciplines,
            genres.Literary_Criticism,
            genres.Philosophy,
        ])            

    food = Lane(_db, name="Food and Health",
                genres=[
                    genres.Cooking,
                    genres.Health_Diet],
                fiction=False,
                include_subgenres=True,
                sublanes=[
                    genres.Bartending_Cocktails,
                    Lane(_db, name="Cooking", genres=[genres.Cooking]),
                    genres.Health_Diet,
                    genres.Vegetarian_Vegan,
                ]
            )

    family = Lane(
        _db, name="Parenting & Family",
        genres=[genres.Parenting_Family],
        include_subgenres=True,
        fiction=False,
        sublanes=[
            genres.Education,
            genres.Family_Relationships,
            genres.Parenting,
        ]
    )

    reference = Lane(
        _db, name="Reference & Study Aids",
        genres=[genres.Reference],
        include_subgenres=True,
        fiction=False,
        sublanes=[
            genres.Dictionaries,
            genres.Foreign_Language_Study,
            Lane(_db, name="General Reference",
                 genres=[genres.Reference, genres.Encyclopedias],
                 include_subgenres=False),
            genres.Law,
            genres.Study_Aids,
        ],

    )

    business = Lane(
        _db, name="Personal Finance & Business",
        genres=[genres.Business_Economics],
        include_subgenres=True,
        fiction=False,
        sublanes=[
            Lane(_db, name="Business", genres=[genres.Business_Economics],
                 include_subgenres=False),
            genres.Economics,
            genres.Management_Leadership,
            genres.Personal_Finance_Investing,
        ],
    )

    humor = Lane(
        _db, name="Humor & Entertainment",
        genres=[genres.Humor_Entertainment],
        include_subgenres=True,
        fiction=False,
        sublanes=[
            Lane(_db, name="Film & TV", genres=[genres.Film_TV]),
            Lane(_db, name="Humor", genres=[genres.Humor],
                 fiction=Lane.BOTH_FICTION_AND_NONFICTION),
            Lane(_db, name="Music", genres=[genres.Music]),
            Lane(_db, name="Performing Arts",
                 genres=[genres.Performing_Arts, genres.Dance]),
        ],
    )

    travel = Lane(
        _db, name="Travel & Sports",
        genres=[genres.Travel_Adventure_Sports],
        include_subgenres=True,
        fiction=False,
        sublanes=[
            genres.Sports,
            genres.Transportation,
            Lane(_db, name="Travel", genres=[genres.Travel],
                 fiction=Lane.BOTH_FICTION_AND_NONFICTION),
        ],
    )

    poetry_drama = Lane(
        _db, name="Poetry & Drama",
        genres=[genres.Poetry, genres.Drama],
        fiction=Lane.BOTH_FICTION_AND_NONFICTION,
        sublanes=[
            genres.Drama,
            genres.Poetry,
        ],
    )

    #
    # Now let's set up the 'nonfiction' lane itself.
    #
    nonfiction = Lane(
        _db, name="Nonfiction", genres=[], 
        fiction=False, audience=Classifier.AUDIENCE_ADULT,
        sublanes=[
            dict(
                name="Art & Design",
                genres=[genres.Art_Architecture_Design],
            ),
            genres.Biography_Memoir,
            business,
            family,
            food,
            hobbies_and_home,
            humor,
            philosophy,
            poetry_drama,
            genres.Politics_Current_Events,
            reference,
            religion,
            science,
            genres.Self_Help,
            travel,
            dict(name="Unclassified Nonfiction",
                 fiction=False,
                 audience=genres.Classifier.AUDIENCE_ADULT,
                 genres=Lane.UNCLASSIFIED),
        ]
    )


    # Now let's set up lanes for young adults and childrens books.

    YA = Classifier.AUDIENCE_YOUNG_ADULT
    ya_fiction = Lane(
        _db, name="Young Adult Fiction", genres=Lane.UNCLASSIFIED,
        fiction=True, audience=YA,
        sublanes=[
            Lane(_db, name="Young Adult Fantasy", genres=[genres.Fantasy],
                 audience=YA),
            Lane(_db, name="Young Adult Graphic Novels & Comics",
                 genres=[genres.Graphic_Novels_Comics], audience=YA),
            Lane(_db, name="Young Adult Historical Fiction",
                 genres=[genres.Historical_Fiction],
                 audience=YA),
            Lane(_db, name="Young Adult Horror",
                 genres=[genres.Horror], audience=YA),
            Lane(_db, name="Young Adult Mystery",
                 genres=[genres.Crime_Thrillers_Mystery],
                 audience=YA),
            Lane(_db, name="Young Adult Romance", genres=[genres.Romance],
                 audience=YA),
            Lane(_db, name="Young Adult Science Fiction",
                 genres=[genres.Science_Fiction], audience=YA),
        ],
    )
        
    ya_nonfiction = Lane(_db, name="Young Adult Nonfiction",
                         fiction=False, genres=[], audience=YA)

    children = Lane(_db, name="Children's Books",
                    fiction=Lane.BOTH_FICTION_AND_NONFICTION,
                    audience=genres.Classifier.AUDIENCE_CHILDREN,
                    genres=[],
                )

    unclassified = dict(
             name="Unclassified",
             fiction=Lane.UNCLASSIFIED,
             genres=Lane.UNCLASSIFIED,
             audience=None)

    lanes = LaneList.from_description(
        _db,
        None,
        [
            fiction,
            nonfiction,
            ya_fiction,
            ya_nonfiction,
            children,
            unclassified,
        ]
    )
    return lanes
