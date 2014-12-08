# encoding: utf-8

# "literary history" != "history"
# "Investigations -- nonfiction" != "Mystery"

# SQL to find commonly used DDC classifications
# select count(editions.id) as c, subjects.identifier from editions join identifiers on workrecords.primary_identifier_id=workidentifiers.id join classifications on workidentifiers.id=classifications.work_identifier_id join subjects on classifications.subject_id=subjects.id where subjects.type = 'DDC' and not subjects.identifier like '8%' group by subjects.identifier order by c desc;

# SQL to find commonly used classifications not assigned to a genre 
# select count(identifiers.id) as c, subjects.type, substr(subjects.identifier, 0, 20) as i, substr(subjects.name, 0, 20) as n from workidentifiers join classifications on workidentifiers.id=classifications.work_identifier_id join subjects on classifications.subject_id=subjects.id where subjects.genre_id is null and subjects.fiction is null group by subjects.type, i, n order by c desc;

import json
import os
import pkgutil
from collections import (
    Counter,
    defaultdict,
)
from nose.tools import set_trace
import re
from sqlalchemy.sql.expression import and_

base_dir = os.path.split(__file__)[0]
resource_dir = os.path.join(base_dir, "resources")

# This is the large-scale structure of our classification system,
# taken mostly from Zola. 
#
# "Children" and "Young Adult" are not here--they are the 'audience' facet
# of a genre.
#
# "Fiction" is not here--it's a seperate facet.
#
# If the name of a genre is a 2-tuple, the second item in the tuple is
# whether or not the genre contains fiction by default. If the name of
# a genre is a string, the genre inherits the default fiction status
# of its parent, or (if a top-level genre) is nonfiction by default.
#
# If the name of a genre is a dictionary, the 'name' key corresponds to its
# name and the 'subgenres' key contains a list of subgenres. 
#
# Genres and subgenres do *not* correspond to lanes and sublanes in
# the user-visible side of the circulation server. This is the
# structure used when classifying books. The circulation server is
# responsible for its own mapping of genres to lanes.
genre_structure = {
    "Art, Architecture, & Design" : [
        "Architecture",
        dict(name="Art", subgenres=[
            "Art Criticism & Theory",
            "Art History",
        ]),
        "Design",
        "Fashion",
        "Photography",
    ],
    "Biography & Memoir" : [],
    "Business & Economics" : [
        "Economics",
        "Management & Leadership",
        "Personal Finance & Investing",
        "Real Estate",
    ],
    ("Classics & Poetry", None) : [
        "Classics",
        "Poetry",
    ],
    "Crafts, Cooking & Garden" : [
        "Antiques & Collectibles",
        dict(name="Cooking", subgenres=[
            "Bartending & Cocktails",
            "Vegetarian & Vegan",
            ]
         ),
        "Crafts, Hobbies, & Games",
        "Gardening",
        "Health & Diet",
        "House & Home",
        "Pets",
    ],
    ("Crime, Thrillers & Mystery", True) : [
        "Action & Adventure",
        dict(name="Mystery", subgenres=[
            "Hard Boiled",
            "Police Procedurals",
            "Women Detectives",
        ]),
        dict(name="Thrillers", subgenres=[
            "Legal Thrillers",
            "Military Thrillers",
            "Supernatural Thrillers",
        ]),
        "Espionage",
        ("True Crime", False),
    ],
    "Criticism & Philosophy" : [
        "Language Arts & Disciplines",
        "Literary Criticism",
        "Philosophy",
    ],
    ("Graphic Novels & Comics", True) : [
        "Literary",
        "Manga",
        "Superhero",
    ],
    ("Historical Fiction", True) : [],
    "History" : [
        "African History",
        "Ancient History",
        "Asian History",
        "Civil War History",
        "European History",
        "Latin American History",
        "Medieval History",
        "Middle East History",
        "Military History",
        "Modern History",
        "Renaissance & Early Modern History",
        "United States History",
        "World History",
    ],
    "Humor & Entertainment" : [
        ("Humor", None),
        dict(name="Performing Arts", subgenres=[
                "Dance",
                "Drama",
                "Film & TV",
                "Music",
                ]),
    ],
    ("Literary Fiction", True) : ["Literary Collections"],
    "Parenting & Family" : [
        "Education",
        "Family & Relationships",
        "Parenting",
    ],
    "Periodicals" : [],
    "Politics & Current Events" : [
        "Political Science",
    ],
    "Reference" : [
        "Dictionaries",
        "Encyclopedias",
        "Foreign Language Study",
        "Law",
        "Study Aids",
    ],
    "Religion & Spirituality" : [
        "Body, Mind, & Spirit",
        "Buddhism",
        "Christianity",
        "Hinduism",
        "Islam",
        "Judaism",
        "New Age",
        ("Religious Fiction", True),
    ],
    ("Romance & Erotica", True) : [
        dict(name="Romance", subgenres=[
            "Contemporary Romance",
            "Historical Romance",
            "Paranormal Romance",
            "Regency Romance",
            "Suspense Romance",
        ]),
        "Erotica",
    ],
    ("Science Fiction & Fantasy", True) : [
        dict(name="Fantasy", subgenres=[
            "Epic Fantasy",
            "Urban Fantasy",
        ]),
        "Horror",
        "Movies/Gaming",
        dict(name="Science Fiction", subgenres=[
            "Military",
            "Space Opera",
            ]
         ),
    ],
    "Science, Technology, & Nature" : [
        dict(name="Technology & Engineering", subgenres=[
            "Computers",
        ]),
        dict(name="Social Science", subgenres=[
            "Psychology",
        ]),
        dict(name="Science", subgenres=[
            "Mathematics",
            "Medical",
        ]),
        "Nature",
    ],
    "Self-Help" : [],
    "Travel, Adventure & Sports" : [
        "Sports",
        "Transportation",
        "Travel",
    ],
    ("African-American", None) : [
        ("Urban Fiction", True)
    ],
    ("LGBT", None) : [],
}

class GenreData(object):
    def __init__(self, name, is_fiction, parent=None):
        self.name = name
        self.parent = parent
        self.is_fiction = is_fiction
        self.subgenres = []

    def __repr__(self):
        return "<GenreData: %s>" % self.name

    @property
    def self_and_subgenres(self):
        yield self
        for child in self.subgenres:
            for subgenre in child.self_and_subgenres:
                yield subgenre

    @property
    def parents(self):
        parents = []
        p = self.parent
        while p:
            parents.append(p)
            p = p.parent
        return reversed(parents)

    def has_subgenre(self, subgenre):
        for s in self.subgenres:
            if s == subgenre or s.has_subgenre(subgenre):
                return True
        return False

    @property
    def variable_name(self):
        return self.name.replace("-", "_").replace(", & ", "_").replace(", ", "_").replace(" & ", "_").replace(" ", "_").replace("/", "_")

    @classmethod
    def populate(cls, namespace, genres, source):
        """Create a GenreData object for every genre and subgenre in the given
        dictionary.
        """
        for name, subgenres in source.items():
            # Nonfiction is the default, because genres of
            # nonfiction outnumber genres of fiction.
            default_to_fiction=False
            cls.add_genre(
                namespace, genres, name, subgenres, default_to_fiction, None)

    @classmethod
    def add_genre(cls, namespace, genres, name, subgenres, default_to_fiction,
                  parent):
        """Create a GenreData object. Add it to a dictionary and a namespace.
        """
        if isinstance(name, tuple):
            name, default_to_fiction = name
        if isinstance(name, dict):
            subgenres = name['subgenres']
            name = name['name']
        if name in genres:
            raise ValueError("Duplicate genre name! %s" % name)

        # Create the GenreData object.
        genre_data = GenreData(name, default_to_fiction, parent)
        if parent:
            parent.subgenres.append(genre_data)

        # Add the genre to the given dictionary, keyed on name.
        genres[genre_data.name] = genre_data

        # Convert the name to a Python-safe variable name,
        # and add it to the given namespace.
        namespace[genre_data.variable_name] = genre_data

        # Do the same for subgenres.
        for sub in subgenres:
            cls.add_genre(namespace, genres, sub, [], default_to_fiction,
                          genre_data)

genres = dict()
GenreData.populate(globals(), genres, genre_structure)

class Classifier(object):

    """Turn an external classification into an internal genre, an
    audience, and a fiction status.
    """

    DDC = "DDC"
    LCC = "LCC"
    LCSH = "LCSH"
    FAST = "FAST"
    OVERDRIVE = "Overdrive"
    TAG = "tag"   # Folksonomic tags.
    GUTENBERG_BOOKSHELF = "gutenberg:bookshelf"
    TOPIC = "schema:Topic"
    PLACE = "schema:Place"
    PERSON = "schema:Person"
    ORGANIZATION = "schema:Organization"

    AUDIENCE_ADULT = "Adult"
    AUDIENCE_YOUNG_ADULT = "Young Adult"
    AUDIENCE_CHILDREN = "Children"

    # TODO: This is currently set in model.py in the Subject class.
    classifiers = dict()

    @classmethod
    def consolidate_weights(cls, weights, subgenre_swallows_parent_at=0.03):
        """If a genre and its subgenres both show up, examine the subgenre
        with the highest weight. If its weight exceeds a certain
        proportion of the weight of the parent genre, assign the
        parent's weight to the subgenre and remove the parent.
        """
        #print "Before consolidation:"
        #for genre, weight in weights.items():
        #    print "", genre, weight

        # Convert Genre objects to GenreData.
        consolidated = dict()
        for genre, weight in weights.items():
            if not isinstance(genre, GenreData):
                genre = genres[genre.name]
            consolidated[genre] = weight

        heaviest_child = dict()
        for genre, weight in consolidated.items():
            for parent in genre.parents:
                if parent in consolidated:
                    if ((not parent in heaviest_child) 
                        or weight > heaviest_child[parent][1]):
                        heaviest_child[parent] = (genre, weight)
        #print "Heaviest child:"
        #for parent, (genre, weight) in heaviest_child.items():
        #    print "", parent, genre, weight
        made_it = False
        while not made_it:
            for parent, (child, weight) in sorted(list(heaviest_child.items())):
                parent_weight = consolidated.get(parent, 0)
                if weight > (subgenre_swallows_parent_at * parent_weight):
                    consolidated[child] += parent_weight
                    del consolidated[parent]
                    changed = False
                    for parent in parent.parents:
                        if parent in heaviest_child:
                            heaviest_child[parent] = (child, consolidated[child])
                            changed = True
                    if changed:
                        # We changed the dict, so we need to restart
                        # the iteration.
                        break
            # We made it all the way through the dict without changing it.
            made_it = True
        #print "Final heaviest child:"
        #for parent, (genre, weight) in heaviest_child.items():
        #    print "", parent, genre, weight
        #print "After consolidation:"
        #for genre, weight in consolidated.items():
        #    print "", genre, weight
        return consolidated

    @classmethod
    def lookup(cls, scheme):
        """Look up a classifier for a classification scheme."""
        return cls.classifiers.get(scheme, None)

    @classmethod
    def name_for(cls, identifier):
        """Look up a human-readable name for the given identifier."""
        return None

    @classmethod
    def classify(cls, subject):
        """Try to determine genre, audience, and fiction status
        for the given Subject.
        """
        identifier = cls.scrub_identifier(subject.identifier)
        if subject.name:
            name = cls.scrub_name(subject.name)
        else:
            name = identifier
        return (cls.genre(identifier, name),
                cls.audience(identifier, name),
                cls.is_fiction(identifier, name))

    @classmethod
    def scrub_identifier(cls, identifier):
        """Prepare an identifier from within a call to classify().
        
        This may involve data normalization, conversion to lowercase,
        etc.
        """
        return identifier.lower()

    @classmethod
    def scrub_name(cls, name):
        """Prepare a name from within a call to classify()."""
        return name.lower()

    @classmethod
    def genre(cls, identifier, name):
        """Is this identifier associated with a particular Genre?"""
        return None

    @classmethod
    def is_fiction(cls, identifier, name):
        """Is this identifier+name particularly indicative of fiction?
        How about nonfiction?
        """
        n = name.lower()
        if "nonfiction" in n:
            return False
        if "fiction" in n:
            return True
        return None

    @classmethod
    def audience(cls, identifier, name):
        """What does this identifier+name say about the audience for
        this book?
        """
        n = name.lower()
        if 'juvenile' in n:
            return cls.AUDIENCE_CHILDREN
        elif 'young adult' in n or "YA" in name:
            return cls.AUDIENCE_YOUNG_ADULT
        return None


class OverdriveClassifier(Classifier):

    # Any classification that includes the string "Fiction" will be
    # counted as fiction. This is just the leftovers.
    FICTION = set([
        "Short Stories",
        "Fantasy",
        "Horror",
        "Mystery",
        "Romance",
        "Western",
        "Suspense",
        "Thriller",
        "Science Fiction & Fantasy",
        ])

    GENRES = {
        African_American : ["African American Fiction", "African American Nonfiction"],
        Antiques_Collectibles : "Antiques",
        Architecture : "Architecture",
        Art : "Art",
        Biography_Memoir : "Biography & Autobiography",
        Business_Economics : ["Business", "Marketing & Sales", "Careers"],
        Christianity : ["Christian Fiction", "Christian Nonfiction"],
        Computers : "Computer Technology",
        Classics : "Classic Literature",
        Cooking : "Cooking & Food",
        Crafts_Hobbies_Games : ["Crafts", "Games"],
        Drama : "Drama",
        Education : "Education",
        Erotica : "Erotic Literature",
        Fantasy : "Fantasy",
        Foreign_Language_Study : "Foreign Language Study",
        Gardening : "Gardening",
        Graphic_Novels_Comics : "Comic and Graphic Books",
        Health_Diet : "Health & Fitness",
        Historical_Fiction : "Historical Fiction",
        History : "History",
        Horror : "Horror",
        House_Home : u"Home Design & Décor",
        Humor : ["Humor (Fiction)", "Humor (Nonfiction)"],
        Humor_Entertainment : "Entertainment",
        Judaism : "Judaica",
        Language_Arts_Disciplines : ["Language Arts", "Grammar & Language Usage"],
        Law : "Law",
        Literary_Collections : "Literary Anthologies",
        Literary_Criticism : ["Literary Criticism", "Criticism"],
        Management_Leadership : "Management",
        Mathematics : "Mathematics",
        Medical : "Medical",
        Military_History : "Military",
        Music : "Music",
        Mystery : "Mystery",
        Nature : "Nature",
        New_Age : "New Age",
        Parenting_Family : "Family & Relationships",
        Performing_Arts : "Performing Arts",
        Personal_Finance_Investing : "Finance",
        Pets : "Pets",
        Philosophy : ["Philosophy", "Ethics"],
        Photography : "Photography",
        Poetry : "Poetry",
        Politics_Current_Events : ["Politics", "Current Events"],
        Psychology : ["Psychology", "Psychiatry", "Psychiatry & Psychology"],
        Reference : "Reference",
        Religion_Spirituality : "Religion & Spirituality",
        Romance : "Romance",
        Science : ["Science", "Physics", "Chemistry"],
        Science_Fiction : "Science Fiction",
        Science_Fiction_Fantasy : "Science Fiction & Fantasy",
        Self_Help : ["Self-Improvement", "Self-Help", "Self Help"],
        Social_Science : "Sociology",
        Sports : "Sports & Recreations",
        Study_Aids : "Study Aids & Workbooks",
        Technology_Engineering : ["Technology", "Engineering"],
        Thrillers : ["Suspense", "Thriller"],
        Transportation : "Transportation",
        Travel : ["Travel", "Travel Literature"],
        True_Crime : "True Crime",
        Urban_Fiction: "Urban Fiction", 
    }

    @classmethod
    def scrub_identifier(cls, identifier):
        return identifier

    @classmethod
    def is_fiction(cls, identifier, name):
        if (identifier in cls.FICTION
            or "Fiction" in identifier
            or "Literature" in identifier):
            # "Literature" on Overdrive seems to be synonymous with fiction,
            # but not necessarily "Literary Fiction".
            return True
        return False

    @classmethod
    def audience(cls, identifier, name):
        if ("Juvenile" in identifier or "Picture Book" in identifier
            or "Beginning Reader" in identifier):
            return cls.AUDIENCE_CHILDREN
        elif "Young Adult" in identifier:
            return cls.AUDIENCE_YOUNG_ADULT
        return cls.AUDIENCE_ADULT

    @classmethod
    def genre(cls, identifier, name):
        for l, v in cls.GENRES.items():
            if identifier == v or (isinstance(v, list) and identifier in v):
                return l
        return None


class DeweyDecimalClassifier(Classifier):

    NAMES = json.load(
        open(os.path.join(resource_dir, "dewey_1000.json")))

    # Add some other values commonly found in MARC records.
    NAMES["B"] = "Biography"
    NAMES["E"] = "Juvenile Fiction"
    NAMES["F"] = "Fiction"
    NAMES["FIC"] = "Juvenile Fiction"
    NAMES["J"] = "Juvenile Nonfiction"
    NAMES["Y"] = "Young Adult"

    FICTION = set([813, 823, 833, 843, 853, 863, 873, 883, "FIC", "E", "F"])
    NONFICTION = set(["J", "B"])

    # 791.4572 and 791.4372 is for recordings. 741.59 is for comic
    #  adaptations? This is a good sign that a identifier should
    #  not be considered, actually.
    # 428.6 - Primers, Readers, i.e. collections of stories
    # 700 - Arts - full of distinctions
    # 700.8996073 - African American arts
    # 700.9 - Art history
    # 700.71 Arts education
    # 398.7 Jokes and jests

    GENRES = {
        African_History : range(960, 970),
        Architecture : range(710, 720) + range(720, 730),
        Art : range(700, 710) + range(730, 770) + [774, 776],
        Art_Criticism_Theory : [701],
        Asian_History : range(950, 960) + [995, 996, 997],
        Biography_Memoir : ["B", 920],
        Business_Economics : range(330, 340),
        Christianity : [range(220, 230) + range(230, 290)],
        Cooking : [range(640, 642)],
        Crafts_Hobbies_Games : [790, 793, 794, 795],
        Drama : [812, 822, 832, 842, 852, 862, 872, 882],
        Education : range(370,380) + [707],
        European_History : range(940, 950),
        History : [900],
        Islam : [297],
        Judaism : [296],
        Language_Arts_Disciplines : range(410, 430),
        Latin_American_History : range(981, 990),
        Law : range(340, 350) + [364],
        Management_Leadership : [658],        
        Mathematics : range(510, 520),
        Medical : range(610, 620),
        Military_History : range(355, 360),
        Music : range(780, 789),
        Periodicals : range(50, 60) + [105, 205, 304, 405, 505, 605, 705, 805, 905],
        Philosophy : range(160, 200),
        Photography : [771, 772, 773, 775, 778, 779],
        Poetry : [811, 821, 831, 841, 851, 861, 871, 874, 881, 884],
        Political_Science : range(320, 330) + range(351, 355),
        Psychology : range(150, 160),
        Reference : range(10, 20) + range(30, 40) + [103, 203, 303, 403, 503, 603, 703, 803, 903],
        Religion_Spirituality : range(200, 220) + [290, 292, 293, 294, 295, 299],
        Science : ([500, 501, 502] + range(506, 510) + range(520, 530) 
                   + range(530, 540) + range(540, 550) + range(550, 560)
                   + range(560, 570) + range(570, 580) + range(580, 590)
                   + range(590, 600)),
        Social_Science : (range(300, 310) + range(360, 364) + range(390,400)), # 398=Folklore
        Sports : range(796, 800),
        Technology_Engineering : (
            [600, 601, 602, 604] + range(606, 610) + range(610, 640)
            + range(660, 670) + range(670, 680) + range(681, 690) + range(690, 700)),
        Travel : range(910, 920),
        United_States_History : range(973,980),
        World_History : [909],
    }

    @classmethod
    def name_for(cls, identifier):
        return cls.NAMES.get(identifier, None)

    @classmethod
    def scrub_identifier(cls, identifier):
        if isinstance(identifier, int):
            identifier = str(identifier).zfill(3)

        identifier = identifier.upper()

        if identifier.startswith('[') and identifier.endswith(']'):
            # This is just bad data.
            identifier = identifier[1:-1]

        if identifier.startswith('C') or identifier.startswith('A'):
            # A work from our Canadian neighbors or our Australian
            # friends.
            identifier = identifier[1:]
        elif identifier.startswith("NZ"):
            # A work from the good people of New Zealand.
            identifier = identifier[2:]

        # Trim everything after the first period. We don't know how to
        # deal with it.
        if '.' in identifier:
            identifier = identifier.split('.')[0]
        try:
            identifier = int(identifier)
        except ValueError:
            pass
        return identifier

    @classmethod
    def is_fiction(cls, identifier, name):
        """Is the given DDC classification likely to contain fiction?"""
        if identifier == 'Y':
            # Inconsistently used for young adult fiction and
            # young adult nonfiction.
            return None

        if (isinstance(identifier, basestring) and (
                identifier.startswith('Y') or identifier.startswith('J'))):
            # Young adult/children's literature--not necessarily fiction
            identifier = identifier[1:]
            try:
                identifier = int(identifier)
            except ValueError:
                pass

        if identifier in cls.FICTION:
            return True
        if identifier in cls.NONFICTION:
            return False

        # TODO: Make NONFICTION more comprehensive and return None if
        # not in there, instead of always returning False. Or maybe
        # returning False is fine here, who knows.
        return False

    @classmethod
    def audience(cls, identifier, name):
        if identifier in ('E', 'FIC'):
            # Juvenile fiction
            return cls.AUDIENCE_CHILDREN

        if isinstance(identifier, basestring) and identifier.startswith('J'):
            return cls.AUDIENCE_CHILDREN

        if isinstance(identifier, basestring) and identifier.startswith('Y'):
            return cls.AUDIENCE_YOUNG_ADULT

        return cls.AUDIENCE_ADULT

    @classmethod
    def genre(cls, identifier, name):
        for genre, identifiers in cls.GENRES.items():
            if identifier == identifiers or identifier in identifiers:
                return genre
        return None
    

class LCCClassifier(Classifier):

    TOP_LEVEL = re.compile("^([A-Z]{1,2})")
    FICTION = set(["PN", "PQ", "PR", "PS", "PT", "PZ"])
    JUVENILE = set(["PZ"])

    GENRES = {

        # Folklore: GR, placed into Social Sciences for now
        # Unclassified/complicated stuff.
        # "America": E11-E143
        # Ancient_History: D51-D90
        # Angling: SH401-SH691
        # Civil_War_History: E456-E655
        # Geography: leftovers of G
        # Islam: BP1-BP253
        # Latin_American_History: F1201-F3799
        # Medieval History: D111-D203
        # Military_History: D25-D27
        # Modern_History: ???
        # Renaissance_History: D219-D234 (1435-1648, so roughly)
        # Sports: GV557-1198.995
        # TODO: E and F are actually "the Americas".
        # United_States_History is E151-E909, F1-F975 but not E456-E655
        African_History : ["DT"],
        Ancient_History : ["DE"],
        Architecture : ["NA"],
        Art_Criticism_Theory : ["BH"],
        Asian_History : ["DS", "DU"],
        Biography_Memoir : ["CT"],
        Business_Economics : ["HB", "HC", "HF", "HJ"],      
        Christianity : ["BR", "BS", "BT", "BV", "BX"],
        Cooking : ["TX"],
        Crafts_Hobbies_Games : ["TT", "GV"],
        Education : ["L"],
        European_History : ["DA", "DAW", "DB", "DD", "DF", "DG", "DH", "DJ", "DK", "DL", "DP", "DQ", "DR"],
        Islam : ["BP"],
        Judaism : ["BM"],
        Language_Arts_Disciplines : ["Z"],
        Mathematics : ["QA", "HA", "GA"],
        Medical: ["QM", "R"],
        Military_History: ["U", "V"],
        Music: ["M"],
        Parenting_Family : ["HQ"],
        Periodicals : ["AP", "AN"],
        Philosophy : ["BC", "BD", "BJ"],
        Photography: ["TR"],
        Political_Science : ["J", "HX"],
        Psychology : ["BF"],
        Reference : ["AE", "AG", "AI"],
        Religion_Spirituality : ["BL", "BQ"],
        Science : ["QB", "QC", "QD", "QE", "QH", "QK", "QL", "QR", "CC", "GB", "GC", "QP"],
        Social_Science : ["HD", "HE", "HF", "HM", "HN", "HS", "HT", "HV", "GN", "GF", "GR", "GT"],
        Sports: ["SK"],
        World_History : ["CB"],
    }

    LEFTOVERS = dict(
        B=Philosophy,
        T=Technology_Engineering,
        Q=Science,
        S=Science,
        H=Social_Science,
        D=History,
        N=Art,
        L=Education,
        E=United_States_History,
        F=United_States_History,
        BP=Religion_Spirituality,
    )

    NAMES = json.load(open(os.path.join(resource_dir, "lcc_one_level.json")))

    @classmethod
    def scrub_identifier(cls, identifier):
        return identifier.upper()

    @classmethod
    def name_for(cls, identifier):
        return cls.NAMES.get(identifier, None)

    @classmethod
    def is_fiction(cls, identifier, name):
        if identifier == 'P':
            return True
        if not identifier.startswith('P'):
            return False
        for i in cls.FICTION:
            if identifier.startswith(i):
                return True
        return False

    @classmethod
    def genre(cls, identifier, name):
        for genre, strings in cls.GENRES.items():
            for s in strings:
                if identifier.startswith(s):
                    return genre
        for prefix, genre in cls.LEFTOVERS.items():
            if identifier.startswith(prefix):
                return genre
        return None

    @classmethod
    def audience(cls, identifier, name):
        if identifier.startswith("PZ"):
            return cls.AUDIENCE_CHILDREN
        # Everything else is implicitly for adults.
        return cls.AUDIENCE_ADULT

def match_kw(*l):
    """Turn a list of strings into a regular expression which matches
    any of those strings, so long as there's a word boundary on both ends.
    """
    if not l:
        return None
    any_keyword = "|".join([keyword for keyword in l])
    with_boundaries = r'\b(%s)\b' % any_keyword
    return re.compile(with_boundaries, re.I)

class KeywordBasedClassifier(Classifier):

    """Classify a book based on keywords."""
    
    FICTION_INDICATORS = match_kw(
        "fiction", "stories", "tales", "literature",
        "bildungsromans", "fictitious",
    )
    NONFICTION_INDICATORS = match_kw(
        "history", "biography", "histories", "biographies", "autobiography",
        "autobiographies", "nonfiction")
    JUVENILE_INDICATORS = match_kw(
        "for children", "children's", "juvenile",
        "nursery rhymes", "9-12")
    YOUNG_ADULT_INDICATORS = match_kw("young adult", "ya", "12-Up", 
                                      "teenage fiction")

    # These identifiers indicate that the string "children" or
    # "juvenile" in the identifier does not actually mean the work is
    # _for_ children.
    JUVENILE_BLACKLIST = set([
        "military participation",
        "services",
        "children's accidents",
        "children's voices",
        "juvenile delinquency",
        "children's television workshop",
        "missing children",
    ])

    GENRES = { 
        Action_Adventure : match_kw(
            "adventure",
            "adventurers",
            "adventure stories",
            "adventure fiction", 
            "western stories",
            "adventurers",
            "sea stories",
            "war stories", 
            "men's adventure",
        ), 

        African_American : match_kw(
            "african[^a-z]+americans", 
            "african[^a-z]+american", 
            "afro[^a-z]+americans", 
            "afro[^a-z]+american", 
            "black studies",
            "african-american studies",
        ),
               
        African_History: match_kw(
            "african history",
            "history.*africa",
        ),
               
        Ancient_History: match_kw(
            "ancient.*history",
            "history.*ancient",
            "civilization, classical",
        ),
               
        Antiques_Collectibles: match_kw(
            "antiques",
            "collectibles",
            "collectors",
            "collecting",
        ),
               
        Architecture: match_kw(
            "architecture",
            "architectural",
            "architect",
            "architects",
        ),
               
        Art: match_kw(
            "art",
            "artist",
            "artists",
            "artistic",
        ),
               
        Art_Architecture_Design: match_kw(
            # Pure super-category.
        ),
               
        Art_Criticism_Theory: match_kw(
            "art criticism",
            "art / criticism & theory",
        ),
               
        Art_History: match_kw(
            "art.*history",
        ),
               
        Asian_History: match_kw(
            "asian history",
            "history.*asia",
            "australasian & pacific history",
        ),
               
               Bartending_Cocktails: match_kw(
                   "cocktail",
                   "cocktails",
                   "bartending",
                   "beer",
                   "alcoholic beverages",
                   "wine",
                   "wine & spirits",
                   "spirits & cocktails",
               ),
               
               Biography_Memoir : match_kw(
                   "autobiographies",
                   "autobiography",
                   "biographies",
                   "biography",
               ),
               
               Body_Mind_Spirit: match_kw(
                   "body, mind & spirit",
               ),
               
               Buddhism: match_kw(
                   "buddhism",
                   "buddhist",
                   "buddha",
               ),
               
               Business_Economics: match_kw(
                   "business",
                   "businesspeople",
                   "businesswomen",
                   "businessmen",
                   "business & economics",
                   "business & financial",
                   "commerce",
                   "sales",
                   "selling",
                   "sales & selling",
                   "nonprofit",
               ),
               
               Christianity : match_kw(
                   "schema:creativework:bible",
                   "bible",
                   "sermons",
                   "devotional",
                   "theological",
                   "theology",
                   'biblical',
                   "christian",
                   "christianity",
                   "catholic",
                   "protestant",
                   "catholicism",
                   "protestantism",
                   "church",
               ),
               
               Civil_War_History: match_kw(
                   "american civil war",
                   "1861-1865",
               ),
               
               Classics: match_kw(
                   'classics',
               ),
               
               Classics_Poetry: match_kw(
                   # Pure supercategory
               ),
               
               Computers : match_kw(
                   "computer",
                   "computer science",
                   "computational",
                   "computers",
                   "computing",
                   "data",
                   "database",
                   "hardware",
                   "software",
                   "software development",
                   "information technology",
                   "web",
                   "world wide web",
               ),
               
               Contemporary_Romance: match_kw(
                   "contemporary romance",
                   "romance--contemporary",
                   "romance / contemporary",
                   "romance - contemporary",
               ),
               
               Cooking : match_kw(
                   "non-alcoholic",
                   "baking",
                   "cookbook",
                   "cooking",
                   "food",
                   "health & healing",
                   "home economics",
                   "cuisine",
               ),
               
               Crafts_Cooking_Garden: match_kw(
                   # Pure supercategory
               ),
               
               Crafts_Hobbies_Games: match_kw(
                   # ! "arts and crafts movement"
                   "arts & crafts",
                   "arts, crafts"
                   "beadwork",
                   "candle crafts",
                   "candle making",
                   "carving",
                   "ceramics",
                   "crafts & hobbies",
                   "crafts",
                   "crocheting",
                   "cross-stitch",
                   "decorative arts",
                   "flower arranging",
                   "folkcrafts",
                   "games",
                   "handicrafts",
                   "hobbies",
                   "hobby",
                   "hobbyist",
                   "hobbyists",
                   "jewelry",
                   "knitting",
                   "metal work",
                   "needlework",
                   "origami",
                   "paper crafts",
                   "pottery",
                   "quilting",
                   "quilts",
                   "scrapbooking",
                   "sewing",
                   "soap making",
                   "stamping",
                   "stenciling",
                   "textile crafts",
                   "toymaking",
                   "weaving",
                   "woodwork",
                   "video games",
                   "gaming",
                   "gambling",
               ),
               
               Crime_Thrillers_Mystery: match_kw(
                   # Pure supercategory
                   "crime",
                   "crimes",
               ),
               
               Criticism_Philosophy: match_kw(
                   # Pure supercategory
               ),
               
               Dance: match_kw(
                   "dance",
                   "dances",
                   "dancers",
                   "dancer",
               ),
               
               Design: match_kw(
                   "design",
                   "designer",
                   "designers",
                   "graphic design",
                   "typography"
               ),
               
               Dictionaries: match_kw(
                   "dictionaries",
                   "dictionary",
               ),              
               
               Drama : match_kw(
                   "comedies",
                   "drama",
                   "dramatist",
                   "dramatists",
                   # Removed so as not to conflict with 'space opera'
                   # "opera",
                   "operas",
                   "plays",
                   "shakespeare",
                   "tragedies",
                   "tragedy",
               ),
               
               Economics: match_kw(
                   "banking",
                   "economy",
                   "economies",
                   "economic",
                   "economics",
               ),
               
               Education: match_kw(
                   # a lot of these don't work well because of the
                   # huge amount of fiction about students.
                   "education",
                   "educational",
                   "educator",
                   "educators",
                   "principals",
                   "teacher",
                   "teachers",
                   "teaching",
                   #"schools",
                   #"high school",
                   "schooling",
                   #"student",
                   #"students",
                   #"college",
                   "university",
                   "universities",
               ),
               
               Encyclopedias: match_kw(
                   "encyclopaedias",
                   "encyclopaedia",
                   "encyclopedias",
                   "encyclopedia",            
               ),
               
               Epic_Fantasy: match_kw(
                   "epic fantasy",
                   "fantasy - epic",
                   "fantasy / epic",
                   "fantasy--epic",
                   "fantasy/epic",
               ),
               
               Espionage: match_kw(
                   "espionage",
                   "intrigue",
                   "spies",
                   "spy stories",
                   "spy novels",
                   "spy fiction",
                   "spy thriller",
               ),
               
               Erotica : match_kw(
                   'erotic',
                   'erotica',
               ),
               
               # TODO: history _plus_ a place
        European_History: match_kw(
            "europe.*history",
            "history.*europe",
            "france.*history",
            "history.*france",
            "england.*history",
            "history.*england",
            "ireland.*history",
            "history.*ireland",
            "germany.*history",
            "history.*germany",
            # etc. etc. etc.
        ),
               
               Family_Relationships: match_kw(
                   # ! human-animal relationships
                   "family & relationships",                   
                   # This is a little awkward because many (most?) of
                   # "relationships" go into fiction
                   "relationships",
                   "family relationships",
               ),
               
               Fantasy : match_kw(
                   "fantasy",
                   "magic",
                   "wizards",
                   "fairies",
                   "witches",
                   "dragons",
                   "sorcery",
                   "witchcraft",
                   "wizardry",
               ),
               
               Fashion: match_kw(
                   "fashion",
                   "fashion design",
                   "fashion designers",
               ),
               
               Film_TV: match_kw(
                   "director",
                   "directors",
                   "film",
                   "films",
                   "movies",
                   "movie",
                   "motion picture",
                   "motion pictures",
                   "moviemaker",
                   "moviemakers",
                   "producer",
                   "producers",
                   "television",
                   "tv",
                   "video",
               ),
               
               Foreign_Language_Study: match_kw(
                   "english as a foreign language",
                   "english as a second language",
                   "esl",
                   "foreign language study",
                   "multi-language dictionaries",
               ),
               
               Gardening: match_kw(
                   "gardening",
                   "horticulture",
               ),
               
               Graphic_Novels_Comics: match_kw(
                   "comics",
                   "comic strip",
                   "comic strips",
                   "comic book",
                   "comic books",
                   "graphic novels",
               ),
               
               Hard_Boiled: match_kw(
                   "hard-boiled",
                   "noir",
               ),
               
               Health_Diet: match_kw(
                   # ! "health services" ?
                   # ! "health care reform"
                   "fitness",
                   "health",
                   "health aspects",
                   "health & fitness",
                   "hygiene",
                   "nutrition",
                   "diet",
                   "diets",
                   "weight loss",
               ),
               
               Hinduism: match_kw(
                   "hinduism",
                   "hindu",
                   "hindus",
               ),
               
               Historical_Fiction : match_kw(
                   "historical fiction",
                   "fiction.*historical",
               ),
               
               Historical_Romance: match_kw(
                   "historical romance",
               ),
               
               History : match_kw(
                   "histories",
                   "history",
               ),
               
               Horror : match_kw(
                   "ghost stories",
                   "horror",
                   "vampires",
                   "paranormal fiction",
                   "occult fiction",
               ),
               
               House_Home: match_kw(
                   "house and home",
                   "house & home",
                   "remodeling",
                   "renovation",
                   "caretaking",
                   "interior decorating",
               ),
               
               Humor : match_kw(
                   "comedy",
                   "humor",
                   "humorous",
                   "humour",
                   "satire",
                   "wit",
               ),
               
               Humor_Entertainment: match_kw(
                   # Almost a pure top-level category 
                   "entertainment",
               ),
               
               # These might be a problem because they might pick up
        # hateful books. Not sure if this will be a problem.
        Islam : match_kw(
            'islam', 'islamic', 'muslim', 'muslims', 'halal',
            'islamic studies',
        ),
               
               Judaism: match_kw(
                   'judaism', 'jewish', 'kosher', 'jews',
                   'jewish studies',
               ),
               
               LGBT: match_kw(
                   'lesbian',
                   'lesbians',
                   'gay',
                   'gay studies',
                   'bisexual',
                   'lesbian studies',
                   'transgender',
                   'transsexual',
                   'transsexuals',
                   'homosexual',
                   'homosexuals',
                   'homosexuality',
                   'queer',
               ),
               
               Language_Arts_Disciplines: match_kw(
                   "alphabets",
                   "communication studies",
                   "composition",
                   "creative writing",
                   "grammar",
                   "handwriting",
                   "information sciences",
                   "journalism",
                   "language arts & disciplines",
                   "language arts and disciplines",
                   "language arts",
                   "library & information sciences",
                   "linguistics",
                   "literacy",
                   "public speaking",
                   "rhetoric",
                   "sign language",
                   "speech",
                   "spelling",
                   "style manuals",
                   "syntax",
                   "vocabulary",
                   "writing systems",
               ),
               
               Latin_American_History: match_kw(
               ),
               
               Law: match_kw(
            "court",
            "judicial",
                   "law",
                   "laws",
                   "legislation",
                   "legal",
               ),
               
               Legal_Thrillers: match_kw(
                   "legal thriller",
                   "legal thrillers",
               ),
               
               # This is 'literary' comic books, not literary fiction.
               Literary: match_kw(
               ),
               
               Literary_Collections: match_kw(
                   "literary collections",
               ),
               
               Literary_Criticism: match_kw(
                   "criticism, interpretation",
               ),
               
               Literary_Fiction: match_kw(
                   "literary",
                   "literary fiction",
                   "general fiction",
                   "fiction[^a-z]+general",
                   "fiction[^a-z]+literary",
               ),
               
               Management_Leadership: match_kw(
                   "management",
                   "business & economics / leadership",
                   "business & economics -- leadership",
                   "management science",
               ),
               
               Manga: match_kw(
                   "japanese comic books",
                   "japanese comics",
                   "manga",
                   "yaoi",
               ),
               
               Mathematics : match_kw(
                   "algebra",
                   "arithmetic",
                   "calculus",
                   "chaos theory",
                   "game theory",
                   "geometry",
                   "group theory",
                   "logic",
                   "math",
                   "mathematical",
                   "mathematician",
                   "mathematicians",
                   "mathematics",
                   "probability",
                   "statistical",
                   "statistics",
                   "trigonometry",
               ),
               
               Medical : match_kw(
                   "anatomy",
                   "disease",
                   "diseases",
                   "disorders",
                   "epidemiology",
                   "illness",
                   "illnesses",
                   "medical",
                   "medicine", 
                   "neuroscience",
                   "physiology",
                   "vaccines",
                   "virus",
               ),
               
               Medieval_History: match_kw(
                   "civilization, medieval",
                   "medieval period",
                   "history.*medieval",
               ),
               
               Middle_East_History: match_kw(
                   "middle east.*history",
                   "history.*middle east",
               ),

               # Military SF, not "military" in general.
        Military: match_kw(
            "science fiction.*military",
            "military.*science fiction",
        ),
               
               Military_History : match_kw(
                   "military science",
                   "warfare",
                   "military",
                   "1914-1918",
                   "1939-1945",
                   "world war",
               ),
               
               Military_Thrillers: match_kw(
                   "military thrillers",
                   "thrillers.*military",
               ),
               
               Modern_History: match_kw(
                   "1900 - 1999",
                   "2000-2099",
                   "modern history",
                   "history, modern",
                   "history (modern)",
                   "history--modern",
                   "history.*20th century",
                   "history.*21st century",
               ),
               
               # This is SF movie tie-ins, not movies & gaming per se.
        # This one is difficult because it takes effect if book
        # has subject "media tie-in" *and* "science fiction" or
        # "fantasy"
        Movies_Gaming: match_kw(
            "science fiction & fantasy gaming",
            "star trek",
            "star wars",
            "jedi",
        ),
               
               Music: match_kw(
                   "music",
                   "musician",
                   "musicians",
                   "musical",
                   "genres & styles"
                   "blues",
                   "jazz",
                   "rap",
                   "hip-hop",
                   "rock.*roll",
                   "rock music",
                   "punk rock",
               ),
               
               Mystery : match_kw(
                   "crime",
                   "detective",
                   "murder",
                   "mystery",
                   "mysteries",
                   "private investigators",
                   "holmes, sherlock",
                   "poirot, hercule",
                   "schema:person:holmes, sherlock",
               ),
               
               Nature : match_kw(
                   # TODO: not sure about this one
                   "nature",
               ),
               
               New_Age: match_kw(
                   "new age",
               ),
               
               Paranormal_Romance : match_kw(
                   "paranormal romance",
                   "romance.*paranormal",
               ),
               
               Parenting : match_kw(
                   # ! "children of"
                   # "family" doesn't work because of many specific
                   # families.
                   "parenting",
                   "parent",
                   "parents",
                   "motherhood",
                   "fatherhood",
               ),
               
               Parenting_Family: match_kw(
                   # Pure top-level category
               ),
               
               Performing_Arts: match_kw(
                   "theatre",
                   "theatrical",
                   "performing arts",
                   "entertainers",
               ),
               
               Periodicals : match_kw(
                   "periodicals",
                   "periodical",
               ),
               
               Personal_Finance_Investing: match_kw(
                   "personal finance",
                   "financial planning",
                   "investing",
                   "retirement planning",
                   "money management",
               ),
               
               Pets: match_kw(
                   "pets",
                   "dogs",
                   "cats",
                   "human-animal relationships",
               ),
               
               Philosophy : match_kw(
                   "philosophy",
                   "philosophical",
                   "philosopher",
                   "philosophers",
               ),
               
               Photography: match_kw(
                   "photography",
                   "photographer",
                   "photographers",
                   "photographic",
               ),
               
               Police_Procedurals: match_kw(
                   "police[^a-z]+procedural",
                   "police[^a-z]+procedurals",
               ),
               
               Poetry : match_kw(
                   "poetry",
                   "poet",
                   "poets",
                   "poem",
                   "poems",
                   "sonnet",
                   "sonnets",
               ),
               
               Political_Science : match_kw(
                   "american government",
                   "anarchism",
                   "censorship",
                   "citizenship",
                   "civics",
                   "communism",
                   "corruption",
                   "corrupt practices",
                   "democracy",
                   "geopolitics",
                   "goverment",
                   "human rights",
                   "international relations",
                   "political economy",
                   "political ideologies",
                   "political process",
                   "political science",
                   "public affairs",
                   "public policy",
               ),
               
               Politics_Current_Events: match_kw(
                   "politics",
                   "current events",
               ),
               
               Psychology: match_kw(
                   "psychology",
                   "psychiatry",
                   "psychological aspects",
                   "psychiatric",
               ),
               
               Real_Estate: match_kw(
                   "real estate",
               ),
               
               Reference : match_kw(
                   "catalogs",
                   "handbooks",
                   "manuals",
               ),
               
               Regency_Romance: match_kw(
                   "regency romance",
                   "romance.*regency",
               ),
               
               Religion_Spirituality : match_kw(
                   "religion",
                   "religious",
               ),
               
               Religious_Fiction: match_kw(
                   "christian fiction",
                   "fiction.*christian",
                   "religious fiction",
                   "fiction.*religious",
               ),

               Renaissance_Early_Modern_History: match_kw(
                   "early modern period",
                   "early modern history",
                   "early modern, 1500-1700",
                   "history.*early modern",
                   "renaissance.*history",
                   "history.*renaissance",
               ),
               
               Romance : match_kw(
                   # ! romance language
                   "love stories",
                   "romance",
                   "love & romance",
                   "romances",
               ),
               
               Romance_Erotica: match_kw(
                   # Pure super-category.
               ),
               
               Science : match_kw(
                   "aeronautics",
                   "astronomy",
                   "biology",
                   "biophysics",
                   "biochemistry", 
                   "botany",
                   "chemistry",
                   "ecology",
                   "entomology",
                   "evolution",
                   "geology",
                   "genetics",
                   "genetic engineering",
                   "genomics",
                   "ichthyology",
                   "herpetology", 
                   "life sciences",
                   "microbiology",
                   "microscopy",
                   "mycology",
                   "ornithology",
                   "natural history",
                   "natural history",
                   "physics",
                   "science",
                   "scientist",
                   "scientists",
                   "zoology",
                   "virology",
                   "cytology",
               ),
               
               Science_Fiction : match_kw(
                   "science fiction",
                   "time travel",
               ),
               
               Science_Fiction_Fantasy: match_kw(
                   "science fiction.*fantasy",
               ),
               
               Science_Technology_Nature: match_kw(
                   # Pure top-level category
               ),
               
               Self_Help: match_kw(
                   "self help",
                   "self-help",
               ),
               
               Social_Science: match_kw(
                   "folklore",
                   "myth",

                   "social sciences",
                   "social science",
                   "anthropology",
                   "archaology",
                   "sociology",
                   "ethnic studies",
                   "gender studies",
                   "media studies",
                   "minority studies",
                   "men's studies",
                   "regional studies",
                   "women's studies",
                   "demography",
               ),
               
               Space_Opera: match_kw(
                   "space opera",
               ),
               
               Sports: match_kw(
                   # Ton of specific sports here since 'players'
                   # doesn't work.
                   "sports",
                   "baseball",
                   "football",
                   "hockey",
                   "soccer",
                   "skating",
               ),
               
               Study_Aids: match_kw(
                   "act",
                   "advanced placement",
                   "bar exam",
                   "clep",
                   "college entrance",
                   "college guides",
                   "financial aid",
                   "certification",
                   "ged",
                   "gmat",
                   "gre",
                   "lsat",
                   "mat",
                   "mcat",
                   "nmsqt",
                   "nte",
                   "psat",
                   "sat",
                   "school guides",
                   "study guide",
                   "study guides",
                   "study aids",
                   "toefl",
                   "workbooks",
               ),
               
               Superhero: match_kw(
                   "superhero",
                   "superheroes",
               ),
               
               Supernatural_Thrillers: match_kw(
                   "thriller.*supernatural",
                   "supernatural.*thriller",
               ),
               
               Suspense_Romance : match_kw(
                   "romantic.*suspense",
                   "suspense.*romance",
                   "romance.*suspense",
                   "romantic.*thriller",
                   "romance.*thriller",
                   "thriller.*romance",
               ),
               
               Technology_Engineering: match_kw(
                   "technology",
                   "engineering",
                   "bioengineering",
               ),
               
               Thrillers: match_kw(
                   "thriller",
                   "thrillers",
                   "suspense",
                   "techno-thriller",
                   "technothriller",
                   "technothrillers",
               ),
               
               Transportation: match_kw(
                   "transportation",
                   "railroads",
                   "trains",
                   "automotive",
                   "ships & shipbuilding",
                   "cars & trucks",
               ),
               
               Travel : match_kw(
                   "discovery",
                   "exploration",
                   "travel",
                   "travels.*voyages",
                   "voyage.*travels",
                   "voyages",
                   "travelers",
                   "description.*travel",
               ),
               
               True_Crime: match_kw(
                   "true crime",
               ),
               
               United_States_History: match_kw(
                   "united states history",
                   "u.s. history",
                   "american revolution",
                   "1775-1783",
               ),
               
               Urban_Fantasy: match_kw(
                   "urban fantasy",
                   "fantasy.*urban",
               ),
               
               Urban_Fiction: match_kw(
                   # TODO: fiction.*urban but not fiction.*fantasy.*urban
                   "urban fiction",
                   "fiction.*african american.*urban",
                   "fiction / urban",
                   "fiction/urban",
               ),
               
               Vegetarian_Vegan: match_kw(
                   "vegetarian",
                   "vegan",
                   "veganism",
                   "vegetarianism",
               ),
               
               Women_Detectives : match_kw(
                   "women detectives",
                   "women detective",
                   "women private investigators",
                   "women private investigator",
                   "women sleuths",
                   "women sleuth",
               ),
               
               World_History: match_kw(
                   "world history",
                   "history[^a-z]*world",
               ),              
    }

    @classmethod
    def is_fiction(cls, identifier, name):
        if not name:
            return None
        if (cls.FICTION_INDICATORS.search(name)):
            return True
        if (cls.NONFICTION_INDICATORS.search(name)):
            return False
        return None

    @classmethod
    def audience(cls, identifier, name):
        if name is None:
            return None
        if cls.JUVENILE_INDICATORS.search(name):
            use = cls.AUDIENCE_CHILDREN
        elif cls.YOUNG_ADULT_INDICATORS.search(name):
            use = cls.AUDIENCE_YOUNG_ADULT
        else:
            return None

        # It may be for kids, or it may be about kids, e.g. "juvenile
        # delinquency".
        for i in cls.JUVENILE_BLACKLIST:
            if i in name:
                return None
        return use

    @classmethod
    def genre(cls, identifier, name):
        matches = Counter()
        match_against = [name]
        for genre, keywords in cls.GENRES.items():
            if keywords and keywords.search(name):
                matches[genre] += 1
        most_specific_genre = None
        most_specific_count = 0
        # The genre with the most regex matches wins.
        #
        # If a genre and a subgenre are tied, then the subgenre wins
        # because it's more specific.
        for genre, count in matches.most_common():
            if not most_specific_genre or (
                    most_specific_genre.has_subgenre(genre)
                    and count >= most_specific_count):
                most_specific_genre = genre
                most_specific_count = count
        return most_specific_genre

class LCSHClassifier(KeywordBasedClassifier):
    pass

class FASTClassifier(KeywordBasedClassifier):
    pass

class TAGClassifier(KeywordBasedClassifier):
    pass

class GutenbergBookshelfClassifier(Classifier):

    # Any classification that includes the string "Fiction" will be
    # counted as fiction. This is just the leftovers.
    FICTION = set([
        "Bestsellers, American, 1895-1923",
        "Adventure",
        "Fantasy",
        "Horror",
        "Mystery",
        "Western",
        "Suspense",
        "Thriller",
        ])

    GENRES = {
        Action_Adventure: [
            "Adventure",
            "Western",
            "Pirates, Buccaneers, Corsairs, etc.",
        ],
        African_American : ["African American Writers"],
        Ancient_History: ["Classical Antiquity"],
        Architecture : [
            "Architecture",
            "The American Architect and Building News",
        ],
        Art : ["Art"],
        Biography_Memoir : [
            "Biographies",
            "Children's Biography",
        ],
        Christianity : ["Christianity"],
        Civil_War_History: "US Civil War",
        Classics : [
            "Best Books Ever Listings",
            "Harvard Classics",
        ],
        Cooking : [
            "Armour's Monthly Cook Book",
            "Cookery",
        ],
        Drama : [
            "One Act Plays",
            "Opera",
            "Plays",
        ],
        Erotica : "Erotic Fiction",
        Fantasy : "Fantasy",
        Foreign_Language_Study : [
            "Language Education",
        ],
        Gardening : [
            "Garden and Forest",
            "Horticulture",
        ],
        Historical_Fiction : "Historical Fiction",
        History : [
            "Children's History",
        ],
        Horror : ["Gothic Fiction", "Horror"],
        Humor : ["Humor"],
        Islam : "Islam",
        Judaism : "Judaism",
        Law : [
            "British Law",
            "Noteworthy Trials",
            "United States Law",
        ],
        Literary_Collections : [
            "Children's Anthologies",
        ],
        Literary_Criticism : ["Bibliomania"],
        Mathematics : "Mathematics",
        Medical : [
            "Medicine",
            "The North American Medical and Surgical Journal",
            "Physiology",
        ],
        Military_History : [
            "American Revolutionary War",
            "World War I",
            "World War II",
            "Spanish American War",
            "Boer War",
            "Napoleonic",
        ],
        Modern_History: "Current History",
        Music : [
            "Music",
            "Child's Own Book of Great Musicians",
        ],
        Mystery : [
            "Crime Fiction",
            "Detective Fiction",
            "Mystery Fiction",
        ],
        Nature : [
            "Animal",
            "Animals-Wild",
            "Bird-Lore"
            "Birds, Illustrated by Color Photography",
        ],
        Periodicals : [
            "Ainslee's", 
            "Prairie Farmer",
            "Blackwood's Edinburgh Magazine",
            u"Barnavännen",
            "Buchanan's Journal of Man",
            "Bulletin de Lille",
            "Celtic Magazine",
            "Chambers's Edinburgh Journal",
            "Contemporary Reviews",
            "Continental Monthly",
            "De Aarde en haar Volken",
            "Dew Drops",
            "Donahoe's Magazine",
            "Golden Days for Boys and Girls",
            "Harper's New Monthly Magazine",
            "Harper's Young People",
            "Graham's Magazine",
            "Lippincott's Magazine",
            "L'Illustration",
            "McClure's Magazine",
            "Mrs Whittelsey's Magazine for Mothers and Daughters",
            "Northern Nut Growers Association",
            "Notes and Queries",
            "Our Young Folks",
            "The American Missionary",
            "The American Quarterly Review",
            "The Arena",
            "The Argosy",
            "The Atlantic Monthly",
            "The Baptist Magazine",
            "The Bay State Monthly",
            "The Botanical Magazine",
            "The Catholic World",
            "The Christian Foundation",
            "The Church of England Magazine",
            "The Contemporary Review",
            "The Economist",
            "The Esperantist",
            "The Girls Own Paper",
            "The Great Round World And What Is Going On In It",
            "The Idler",
            "The Illustrated War News",
            "The International Magazine of Literature, Art, and Science",
            "The Irish Ecclesiastical Record",
            "The Irish Penny Journal",
            "The Journal of Negro History",
            "The Knickerbocker",
            "The Mayflower",
            "The Menorah Journal",
            "The Mentor",
            "The Mirror of Literature, Amusement, and Instruction",
            "The Mirror of Taste, and Dramatic Censor",
            "The National Preacher",
            "The Aldine",
            "The Nursery",
            "St. Nicholas Magazine for Boys and Girls",
            "Punch",
            "Punchinello",
            "Scribner's Magazine",
            "The Scrap Book",
            "The Speaker",
            "The Stars and Stripes",
            "The Strand Magazine",
            "The Unpopular Review",
            "The Writer",
            "The Yellow Book",
            "Women's Travel Journals",
        ],
        Pets : ["Animals-Domestic"],
        Philosophy : ["Philosophy"],
        Photography : "Photography",
        Poetry : [
            "Poetry",
            "Poetry, A Magazine of Verse",
            "Children's Verse",
        ],
        Political_Science : [
            "Anarchism",
            "Politics",
        ],
        Psychology : ["Psychology"],
        Reference : [
            "Reference",
            "CIA World Factbooks",
        ],
        Religion_Spirituality : [
            "Atheism",
            "Bahá'í Faith",
            "Hinduism",
            "Paganism",
            "Children's Religion",
        ],
        Science : [
            "Astronomy",
            "Biology",
            "Botany",
            "Chemistry",
            "Ecology",
            "Geology",
            "Journal of Entomology and Zoology",
            "Microbiology",
            "Microscopy",
            "Natural History",
            "Mycology",
            "Popular Science Monthly",
            "Physics",
            "Scientific American",
        ],
        Science_Fiction : [
            "Astounding Stories",
            "Precursors of Science Fiction",
            "The Galaxy",
            "Science Fiction",
        ],
        Social_Science : [
            "Anthropology",
            "Archaeology",
            "The American Journal of Archaeology",
            "Sociology",
        ],
        Technology_Engineering : [
            "Engineering", 
            "Technology"
        ],
        Transportation : "Transportation",
        Travel : "Travel",
        True_Crime : "Crime Nonfiction",
    }

    @classmethod
    def scrub_identifier(cls, identifier):
        return identifier

    @classmethod
    def is_fiction(cls, identifier, name):
        if (identifier in cls.FICTION
            or "Fiction" in identifier or "Stories" in identifier):
            return True
        return None

    @classmethod
    def audience(cls, identifier, name):
        if ("Children's" in identifier):
            return cls.AUDIENCE_CHILDREN
        return cls.AUDIENCE_ADULT

    @classmethod
    def genre(cls, identifier, name):
        for l, v in cls.GENRES.items():
            if identifier == v or (isinstance(v, list) and identifier in v):
                return l
        return None

# Make a dictionary of classification schemes to classifiers.
Classifier.classifiers[Classifier.DDC] = DeweyDecimalClassifier
Classifier.classifiers[Classifier.LCC] = LCCClassifier
Classifier.classifiers[Classifier.FAST] = FASTClassifier
Classifier.classifiers[Classifier.LCSH] = LCSHClassifier
Classifier.classifiers[Classifier.TAG] = TAGClassifier
Classifier.classifiers[Classifier.OVERDRIVE] = OverdriveClassifier
Classifier.classifiers[Classifier.GUTENBERG_BOOKSHELF] = GutenbergBookshelfClassifier
