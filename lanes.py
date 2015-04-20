from nose.tools import set_trace
import core.classifier as genres
from core.classifier import (
    Classifier,
    fiction_genres,
    nonfiction_genres,
)
from core.model import (
    Lane,
    LaneList,
    Work,
)

def make_lanes(_db):

    adult_fiction = Lane(
        _db, full_name="Adult Fiction", display_name="Fiction",
        genres=fiction_genres,
        fiction=True, audience=Classifier.AUDIENCES_ADULT
    )
    adult_nonfiction = Lane(
        _db, full_name="Adult Nonfiction", display_name="Nonfiction",
        genres=nonfiction_genres,
        fiction=True, audience=Classifier.AUDIENCES_ADULT
    )

    YA = Classifier.AUDIENCE_YOUNG_ADULT
    CHILDREN = Classifier.AUDIENCE_CHILDREN

    ya_fiction = Lane(
        _db, full_name="Young Adult Fiction", fiction=True, audience=YA,
        sublanes=[
            Lane(_db, full_name="Dystopian", genres=[genres.Dystopian_SF]),
            Lane(_db, genres=[genres.Fantasy], subgenre_books_go=Lane.IN_SAME_LANE),
            Lane(_db, genres=[genres.Graphic_Novels_Comics]),
            Lane(_db, genres=[genres.Literary_Fiction]),
            Lane(_db, genres=[genres.LGBTQ_Fiction]),
            Lane(_db, full_name="Mystery/Thriller",
                 genres=[genres.Suspense_Thriller, genres.Mystery],
                 subgenre_books_go=Lane.IN_SAME_LANE),
            Lane(_db, genres=[genres.Romance],
                 subgenre_books_go=Lane.IN_SAME_LANE),
            Lane(_db, genres=[genres.Science_Fiction],
                 subgenre_books_go=Lane.IN_SAME_LANE,
                 exclude_genres=[genres.Dystopian_SF, genres.Steampunk]),
            Lane(_db, full_name="Middle grade", audience=CHILDREN,
                 age_range=[9,10,11,12]),
            Lane(_db, full_name="Steampunk", genres=[genres.Steampunk]),
            # TODO:
            # Paranormal -- what is it exactly?
        ],
    )

    ya_nonfiction = Lane(
        _db, full_name="Young Adult Fiction", fiction=True, audience=YA,
        sublanes=[
            Lane(_db, "YA Biography", genres.Biography_Memoir,
                 display_name="Biography"),
            Lane(_db, "YA History",
                 [genres.History, genres.Social_Sciences],
                 display_name="History & Sociology", 
                 subgenre_books_go=Lane.IN_SAME_LANE
             ),
            Lane(_db, "Life Strategies", [genres.Life_Strategies]),
            Lane(_db, "YA Religion & Spirituality", 
                 genre.Religion_Spirituality,
                 subgenre_books_go=Lane.IN_SAME_LANE)
        ],
    ),

    children = Lane(
        _db, full_name="Children's Books",
        fiction=Lane.BOTH_FICTION_AND_NONFICTION,
        audience=genres.Classifier.AUDIENCE_CHILDREN,
        sublanes=[
            Lane(_db, full_name="Picture Books", age_range=[0,1,2,3,4]),
            Lane(_db, full_name="Easy readers", age_range=[5,6,7,8]),
            Lane(_db, full_name="Chapter books", age_range=[9,10,11,12]),
            Lane(_db, full_name="Poetry books", genres=[genres.Poetry]),
            Lane(_db, full_name="Folklore", genres=[genres.Folklore],
                 subgenre_books_go=Lane.IN_SAME_LANE),
            Lane(_db, fiction=True, genres=[genres.Fantasy], 
                 subgenre_books_go=Lane.IN_SAME_LANE),
            Lane(_db, fiction=True, genres=[genres.Science_Fiction],
                 subgenre_books_go=Lane.IN_SAME_LANE),
            Lane(_db, full_name="Realistic fiction", 
                 fiction=True, genres=[genres.Literary_Fiction],
                 subgenre_books_go=Lane.IN_SAME_LANE),
            Lane(_db, full_name="Biography and historical fiction", 
                 genres=[genres.Biography_Memoir, genres.Historical_Fiction],
                 subgenre_books_go=Lane.IN_SAME_LANE),
            Lane(_db, full_name="Informational books", 
                 fiction=False, exclude_genres=[genres.Biography_Memoir],
             )
        ],
    )

    lanes = LaneList.from_description(
        _db,
        None,
        [fiction, nonfiction, ya_fiction, ya_nonfiction, children]
    )
    set_trace()
    return lanes
