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
        genres=None,
        sublanes=fiction_genres,
        fiction=True, audience=Classifier.AUDIENCES_ADULT
    )
    adult_nonfiction = Lane(
        _db, full_name="Adult Nonfiction", display_name="Nonfiction",
        genres=None,
        sublanes=nonfiction_genres,
        fiction=True, audience=Classifier.AUDIENCES_ADULT
    )

    YA = Classifier.AUDIENCE_YOUNG_ADULT
    CHILDREN = Classifier.AUDIENCE_CHILDREN

    ya_fiction = Lane(
        _db, full_name="Young Adult Fiction", genres=None, fiction=True,
        audience=YA,
        sublanes=[
            Lane(_db, full_name="YA Dystopian",
                 display_name="Dystopian", genres=[genres.Dystopian_SF],
                 audience=YA),
            Lane(_db, full_name="YA Fantasy", display_name="Fantasy",
                 genres=[genres.Fantasy], 
                 subgenre_books_go=Lane.IN_SAME_LANE, audience=YA),
            Lane(_db, full_name="YA Graphic Novels",
                 display_name="Comics & Graphic Novels",
                 genres=[genres.Comics_Graphic_Novels], audience=YA),
            Lane(_db, full_name="YA Literary Fiction",
                 display_name="Contemporary Fiction",
                 genres=[genres.Literary_Fiction], audience=YA),
            Lane(_db, "LGBTQ Fiction", genres=[genres.LGBTQ_Fiction],
                 audience=YA),
            Lane(_db, full_name="Mystery/Thriller",
                 genres=[genres.Suspense_Thriller, genres.Mystery],
                 subgenre_books_go=Lane.IN_SAME_LANE, audience=YA),
            Lane(_db, full_name="YA Romance", display_name="Romance",
                 genres=[genres.Romance],
                 subgenre_books_go=Lane.IN_SAME_LANE, audience=YA),
            Lane(_db, full_name="YA Science Fiction",
                 display_name="Science Fiction",
                 genres=[genres.Science_Fiction],
                 subgenre_books_go=Lane.IN_SAME_LANE,
                 exclude_genres=[genres.Dystopian_SF, genres.Steampunk],
                 audience=YA),
            Lane(_db, full_name="Middle grade", audience=CHILDREN,
                 genres=None,
                 age_range=[9,10,11,12], fiction=True),
            Lane(_db, "YA Steampunk", [genres.Steampunk],
                 subgenre_books_go=Lane.IN_SAME_LANE,
                 display_name="Steampunk", audience=YA),
            # TODO:
            # Paranormal -- what is it exactly?
        ],
    )

    ya_nonfiction = Lane(
        _db, full_name="Young Adult Nonfiction", genres=None, fiction=True,
        audience=YA,
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
                 genres.Religion_Spirituality,
                 subgenre_books_go=Lane.IN_SAME_LANE)
        ],
    )

    children = Lane(
        _db, full_name="Children's Books", genres=None,
        fiction=Lane.BOTH_FICTION_AND_NONFICTION,
        audience=genres.Classifier.AUDIENCE_CHILDREN,
        sublanes=[
            Lane(_db, full_name="Picture Books", age_range=[0,1,2,3,4],
                 genres=None, fiction=Lane.BOTH_FICTION_AND_NONFICTION,
                 audience=CHILDREN),
            Lane(_db, full_name="Easy readers", age_range=[5,6,7,8],
                 genres=None, fiction=Lane.BOTH_FICTION_AND_NONFICTION,
                 audience=CHILDREN),
            Lane(_db, full_name="Chapter books", age_range=[9,10,11,12],
                 genres=None, fiction=Lane.BOTH_FICTION_AND_NONFICTION,
                 audience=CHILDREN),
            Lane(_db, full_name="Children's Poetry", 
                 display_name="Poetry books", genres=[genres.Poetry],
                 audience=CHILDREN),
            Lane(_db, full_name="Children's Folklore", display_name="Folklore",
                 genres=[genres.Folklore],
                 subgenre_books_go=Lane.IN_SAME_LANE, audience=CHILDREN),
            Lane(_db, full_name="Children's Fantasy", display_name="Fantasy",
                 fiction=True,
                 genres=[genres.Fantasy], 
                 subgenre_books_go=Lane.IN_SAME_LANE, audience=CHILDREN),
            Lane(_db, full_name="Children's SF", display_name="Science Fiction",
                 fiction=True, genres=[genres.Science_Fiction],
                 subgenre_books_go=Lane.IN_SAME_LANE, audience=CHILDREN),
            Lane(_db, full_name="Realistic fiction", 
                 fiction=True, genres=[genres.Literary_Fiction],
                 subgenre_books_go=Lane.IN_SAME_LANE, audience=CHILDREN),
            Lane(_db, full_name="Biography and historical fiction", 
                 genres=[genres.Biography_Memoir, genres.Historical_Fiction],
                 subgenre_books_go=Lane.IN_SAME_LANE, audience=CHILDREN),
            Lane(_db, full_name="Informational books", genres=None,
                 fiction=False, exclude_genres=[genres.Biography_Memoir],
                 audience=CHILDREN
             )
        ],
    )

    lanes = LaneList.from_description(
        _db,
        None,
        [adult_fiction, adult_nonfiction, ya_fiction, ya_nonfiction, children]
    )
    return lanes
