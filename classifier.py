# encoding: utf-8

# If the genre classification does not match the fiction classification, throw
# away the genre classifications.
#
# E.g. "Investigations -- nonfiction" maps to Mystery, but Mystery
# conflicts with Nonfiction.

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

class Classifier(object):

    """Turn an external classification into an internal genre, an
    audience, an age level, and a fiction status.
    """

    DDC = "DDC"
    LCC = "LCC"
    LCSH = "LCSH"
    FAST = "FAST"
    OVERDRIVE = "Overdrive"
    THREEM = "3M"
    BISAC = "BISAC"
    TAG = "tag"   # Folksonomic tags.

    GRADE_LEVEL = "Grade level" # "1-2", "Grade 4", "Kindergarten", etc.
    AGE_RANGE = "schema:typicalAgeRange" # "0-2", etc.
    AXIS_360_AUDIENCE = "AXIS 360 Audience"

    # We know this says something about the audience but we're not sure what.
    # Could be any of the values from GRADE_LEVEL or AGE_RANGE, plus
    # "YA", "Adult", etc.
    FREEFORM_AUDIENCE = "schema:audience"

    GUTENBERG_BOOKSHELF = "gutenberg:bookshelf"
    TOPIC = "schema:Topic"
    PLACE = "schema:Place"
    PERSON = "schema:Person"
    ORGANIZATION = "schema:Organization"
    LEXILE_SCORE = "Lexile"
    ATOS_SCORE = "ATOS"
    INTEREST_LEVEL = "Interest Level"

    AUDIENCE_ADULT = "Adult"
    AUDIENCE_ADULTS_ONLY = "Adults Only"
    AUDIENCE_YOUNG_ADULT = "Young Adult"
    AUDIENCE_CHILDREN = "Children"

    AUDIENCES_ADULT = [AUDIENCE_ADULT, AUDIENCE_ADULTS_ONLY]
    AUDIENCES = set([AUDIENCE_ADULT, AUDIENCE_ADULTS_ONLY, AUDIENCE_YOUNG_ADULT,
                     AUDIENCE_CHILDREN])

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
        consolidated = Counter()
        for genre, weight in weights.items():
            if not isinstance(genre, GenreData):
                genre = genres[genre.name]
            consolidated[genre] += weight

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
        """Try to determine genre, audience, target age, and fiction status
        for the given Subject.
        """
        identifier = cls.scrub_identifier(subject.identifier)
        if subject.name:
            name = cls.scrub_name(subject.name)
        else:
            name = identifier
        fiction = cls.is_fiction(identifier, name)
        audience = cls.audience(identifier, name)

        return (cls.genre(identifier, name, fiction, audience),
                audience,
                cls.target_age(identifier, name),
                fiction,
                )

    @classmethod
    def scrub_identifier(cls, identifier):
        """Prepare an identifier from within a call to classify().
        
        This may involve data normalization, conversion to lowercase,
        etc.
        """
        return Lowercased(identifier)

    @classmethod
    def scrub_name(cls, name):
        """Prepare a name from within a call to classify()."""
        return Lowercased(name)

    @classmethod
    def genre(cls, identifier, name, fiction=None, audience=None):
        """Is this identifier associated with a particular Genre?"""
        return None

    @classmethod
    def is_fiction(cls, identifier, name):
        """Is this identifier+name particularly indicative of fiction?
        How about nonfiction?
        """
        if "nonfiction" in name:
            return False
        if "fiction" in name:
            return True
        return None

    @classmethod
    def audience(cls, identifier, name):
        """What does this identifier+name say about the audience for
        this book?
        """
        if 'juvenile' in name:
            return cls.AUDIENCE_CHILDREN
        elif 'young adult' in name or "YA" in name.original:
            return cls.AUDIENCE_YOUNG_ADULT
        return None

    @classmethod
    def target_age(cls, identifier, name):
        """For children's books, what does this identifier+name say
        about the target age for this book?
        """
        return None

class GradeLevelClassifier(Classifier):
    # How old a kid is when they start grade N in the US.
    american_grade_to_age = {
        # Preschool: 3-4 years
        'preschool' : 3,
        'pre-school' : 3,
        'p' : 3,

        # Easy readers
        'kindergarten' : 5,
        'k' : 5,
        '0' : 5,
        'first' : 6,
        '1' : 6,
        'second' : 7,
        '2' : 7,

        # Chapter Books
        'third' : 8,
        '3' : 8,
        'fourth' : 9,
        '4' : 9,
        'fifth' : 10,
        '5' : 10,
        'sixth' : 11,
        '6' : 11,
        '7' : 12,
        '8' : 13,

        # YA
        '9' : 14,
        '10' : 15,
        '11' : 16,
        '12': 17,
    }

    # Regular expressions that match common ways of expressing grade
    # levels.
    grade_res = [
        re.compile(x, re.I) for x in [
            "grades? ([kp0-9]+)", 
            "gr\.? ([kp0-9]+)", 
            "([0-9]+)[tns][hdt] grade",
            "([a-z]+) grade",
            "^(kindergarten|preschool)\b",
        ]
    ]

    generic_grade_res = [
        re.compile(r"^([0-9]+)\b", re.I),
        re.compile(r"^([kp])\b", re.I),
        re.compile(r"([0-9]+) ?- ?[0-9]+", re.I),
    ]

    @classmethod
    def audience(cls, identifier, name, require_explicit_age_marker=False):
        a = cls.target_age(identifier, name, require_explicit_age_marker)
        if not a:
            return None
        if a < 12:
            return Classifier.AUDIENCE_CHILDREN
        elif a < 18:
            return Classifier.AUDIENCE_YOUNG_ADULT
        else:
            return Classifier.AUDIENCE_ADULT

    @classmethod
    def target_age(cls, identifier, name, require_explicit_grade_marker=False):

        if (identifier and "education" in identifier) or (name and 'education' in name):
            # This is a book about teaching, e.g. fifth grade.
            return None

        if (identifier and 'graders' in identifier) or (name and 'graders' in name):
            # This is a book about, e.g. fifth graders.
            return None

        if require_explicit_grade_marker:
            res = cls.grade_res
        else:
            res = cls.grade_res + cls.generic_grade_res

        for r in res:
            for k in identifier, name:
                if not k:
                    continue
                m = r.search(k)
                if m:
                    grade = m.groups()[0]
                    if grade in cls.american_grade_to_age:
                        return cls.american_grade_to_age[grade]
        return None


class InterestLevelClassifier(Classifier):

    @classmethod
    def audience(cls, identifier, name):
        if identifier in ('lg', 'mg+', 'mg'):
            return cls.AUDIENCE_CHILDREN
        elif identifier == 'ug':
            return cls.AUDIENCE_YOUNG_ADULT
        else:
            return None

    @classmethod
    def target_age(cls, identifier, name):
        if identifier == 'lg':
            return 5
        if identifier in ('mg+', 'mg'):
            return 9
        if identifier == 'ug':
            return 14
        return None


class AgeClassifier(Classifier):
    # Regular expressions that match common ways of expressing ages.
    age_res = [
        re.compile(x, re.I) for x in [
            "ages ([0-9]+) ?- ?[0-9]+",
            "([0-9]+) ?- ?[0-9]+ year",
            "([0-9]+) year",
            "([0-9]+) and up",
        ]
    ]

    generic_age_res = [
        re.compile(r"^([0-9]+)\b", re.I),
        re.compile("([0-9]+) ?- ?[0-9]+", re.I),
    ]

    baby_re = re.compile("^baby ?-")

    @classmethod
    def audience(cls, identifier, name, require_explicit_age_marker=False):
        a = cls.target_age(identifier, name, require_explicit_age_marker)
        if not a:
            return None
        if a < 12:
            return Classifier.AUDIENCE_CHILDREN
        elif a < 18:
            return Classifier.AUDIENCE_YOUNG_ADULT
        else:
            return Classifier.AUDIENCE_ADULT

    @classmethod
    def target_age(cls, identifier, name, require_explicit_age_marker=False):
        if require_explicit_age_marker:
            res = cls.age_res
        else:
            res = cls.age_res + cls.generic_age_res
            if identifier and cls.baby_re.search(identifier):
                return 0

        for r in res:
            for k in identifier, name:
                if not k:
                    continue
                m = r.search(k)
                if m:
                    age = m.groups()[0]
                    if age:
                        return int(age)
        return None


class Axis360AudienceClassifier(Classifier):

    TEEN_PREFIX = "Teen -"
    CHILDRENS_PREFIX = "Children's -"

    age_re = re.compile("Age ([0-9]+)-([0-9]+)$")

    @classmethod
    def audience(cls, identifier, name, require_explicit_age_marker=False):
        if not identifier:
            return None
        if identifier == 'General Adult':
            return Classifier.AUDIENCE_ADULT
        elif identifier.startswith(cls.TEEN_PREFIX):
            return Classifier.AUDIENCE_YOUNG_ADULT
        elif identifier.startswith(cls.CHILDRENS_PREFIX):
            return Classifier.AUDIENCE_CHILDREN
        return None

    @classmethod
    def target_age(cls, identifier, name, require_explicit_age_marker=False):
        if (not identifier.startswith(cls.TEEN_PREFIX)
            and not identifier.startswith(cls.CHILDRENS_PREFIX)):
            return None
        m = cls.age_re.search(identifier)
        if not m:
            return None
        return int(m.groups()[0])


# This is the large-scale structure of our classification system.
#
# If the name of a genre is a 2-tuple, the second item in the tuple is
# a list of names of subgenres.
#
# If the name of a genre is a 3-tuple, the genre is restricted to a
# specific audience (e.g. erotica is adults-only), and the third item
# in the tuple describes that audience.
fiction_genres = [
    "Adventure",
    "Classics",
    "Comics & Graphic Novels",
    "Drama",
    ("Erotica", [], Classifier.AUDIENCE_ADULTS_ONLY),
    ("Fantasy", [
        "Epic Fantasy", 
        "Historical Fantasy",
        "Urban Fantasy", 
    ]),
    "Folklore",
    "Historical Fiction",
    ("Horror", [
        "Gothic Horror",
        "Ghost Stories",
        "Vampires",
        "Werewolves",
        "Occult Horror",
    ]),
    dict(full_name="Humorous Fiction", display_name="Humor"),
    "Literary Fiction",
    ("LGBTQ Fiction", [], Classifier.AUDIENCE_YOUNG_ADULT),
    ("Mystery", [
        "Crime & Detective Stories",
        "Hard-Boiled Mystery",
        "Police Procedural",
        "Cozy Mystery",
        "Historical Mystery",
        "Paranormal Mystery",
        "Women Detectives",
    ]),
    "Poetry",
    "Religious Fiction",
    ("Romance", [
        "Contemporary Romance",
        "Gothic Romance",
        "Historical Romance",
        "Paranormal Romance",
        "Western Romance",
        "Romantic Suspense",
    ]),
    ("Science Fiction", [
        "Dystopian SF",
        "Space Opera",
        "Cyberpunk",
        "Military SF",
        "Alternative History",
        "Steampunk",
        "Romantic SF",
        dict(full_name="Media Tie-in SF", display_name="Movie and TV Novelizations")
    ]),
    "Short Stories",
    ("Suspense/Thriller", [
        "Historical Thriller",
        "Espionage",
        "Supernatural Thriller",
        "Medical Thriller",
        "Political Thriller",
        "Psychological Thriller",
        "Technothriller",
        "Legal Thriller",
        "Military Thriller",
    ]),
    "Urban Fiction",
    "Westerns",
    "Women's Fiction",
]

nonfiction_genres = [
    ("Art & Design", [
        "Architecture",
        "Art",
        "Art Criticism & Theory",
        "Art History",
        "Design",
        "Fashion",
        "Photography",
    ]),
    "Biography & Memoir",
    "Education",
    ("Personal Finance & Business", [
        "Business",
        "Economics",
        "Management & Leadership",
        "Personal Finance & Investing",
        "Real Estate",
    ]),
    ("Parenting & Family", [
        "Family & Relationships",
        "Parenting",
    ]),
    ("Food & Health", [
        "Bartending & Cocktails",
        "Cooking",
        "Health & Diet",
        "Vegetarian & Vegan",
    ]),
    ("History", [
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
    ]),
    ("Hobbies & Home", [
        "Antiques & Collectibles",
        "Crafts & Hobbies",
        "Gardening",
        "Games",
        "House & Home",
        "Pets",
    ]),
    dict(full_name="Humorous Nonfiction", display_name="Humor"),
    ("Entertainment", [
        "Film & TV",
        "Music",
        "Performing Arts",
    ]),
    ("Life Strategies", [], Classifier.AUDIENCE_YOUNG_ADULT),
    "Literary Criticism",
    "Periodicals",
    "Philosophy",
    ("Politics & Current Events", ["Political Science"]),
    ("Reference & Study Aids", [
        "Dictionaries",
        "Foreign Language Study",
        "Law",
        "Study Aids",
    ]),
    ("Religion & Spirituality", [
        "Body, Mind & Spirit",
        "Buddhism",
        "Christianity",
        "Hinduism",
        "Islam",
        "Judaism",
    ]),
    ("Science & Technology", [
        "Computers",
        "Mathematics",
        "Medical",
        "Nature",
        "Psychology",
        "Science",
        "Social Sciences",
        "Technology",
    ]),
    "Self-Help",
    "Sports",
    "Travel",
    "True Crime",
]


class GenreData(object):
    def __init__(self, name, is_fiction, parent=None, audience_restriction=None):
        self.name = name
        self.parent = parent
        self.is_fiction = is_fiction
        self.subgenres = []
        if isinstance(audience_restriction, basestring):
            audience_restriction = [audience_restriction]
        elif audience_restriction and not isinstance(audience_restriction, list):
            set_trace()
        self.audience_restriction = audience_restriction

    def __repr__(self):
        return "<GenreData: %s>" % self.name

    @property
    def self_and_subgenres(self):
        yield self
        for child in self.all_subgenres:
            yield child

    @property
    def all_subgenres(self):
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
        return self.name.replace("-", "_").replace(", & ", "_").replace(", ", "_").replace(" & ", "_").replace(" ", "_").replace("/", "_").replace("'", "")

    @classmethod
    def populate(cls, namespace, genres, fiction_source, nonfiction_source):
        """Create a GenreData object for every genre and subgenre in the given
        list of fiction and nonfiction genres.
        """
        for source in fiction_source, nonfiction_source:
            fiction = (source is fiction_source)
            for item in source:
                subgenres = []
                audience_restriction = None
                name = item
                if isinstance(item, tuple):
                    if len(item) == 2:
                        name, subgenres = item
                    elif len(item) == 3:
                        name, subgenres, audience_restriction = item
                elif isinstance(item, dict):
                    name = item['full_name']
                    subgenres = item.get('subgenres', [])
                    audience_restriction = item.get('audience_restriction')

                cls.add_genre(
                    namespace, genres, name, subgenres, fiction,
                    None, audience_restriction)

    @classmethod
    def add_genre(cls, namespace, genres, name, subgenres, fiction,
                  parent, audience_restriction):
        """Create a GenreData object. Add it to a dictionary and a namespace.
        """
        if isinstance(name, tuple):
            name, default_fiction = name
        default_fiction = None
        default_audience = None
        if parent:
            default_fiction = parent.is_fiction
            default_audience = parent.audience_restriction
        if isinstance(name, dict):
            data = name
            subgenres = data.get('subgenres', [])
            name = data['full_name']
            fiction = data.get('fiction', default_fiction)
            audience_restriction = data.get('audience', default_audience)
        if name in genres:
            raise ValueError("Duplicate genre name! %s" % name)

        # Create the GenreData object.
        genre_data = GenreData(name, fiction, parent, audience_restriction)
        if parent:
            parent.subgenres.append(genre_data)

        # Add the genre to the given dictionary, keyed on name.
        genres[genre_data.name] = genre_data

        # Convert the name to a Python-safe variable name,
        # and add it to the given namespace.
        namespace[genre_data.variable_name] = genre_data

        # Do the same for subgenres.
        for sub in subgenres:
            cls.add_genre(namespace, genres, sub, [], fiction,
                          genre_data, audience_restriction)

genres = dict()
GenreData.populate(globals(), genres, fiction_genres, nonfiction_genres)

class Lowercased(unicode):
    """A lowercased string that remembers its original value."""
    def __new__(cls, value):
        o = super(Lowercased, cls).__new__(cls, value.lower())
        o.original = value
        return o

class ThreeMClassifier(Classifier):

    # Any classification that starts with "FICTION" or "JUVENILE
    # FICTION" will be counted as fiction. This is just the leftovers.
    FICTION = set([
        "Magic",
        "Fables",
        "Unicorns & Mythical",
    ])

    # These are the most general categories, used if nothing more specific matches.
    CATCHALL_PREFIXES = {
        Adventure : [
            "Action & Adventure/",
            "FICTION/Adventure/",
            "FICTION/War/",
            "Men's Adventure/",
            "Sea Stories",
        ],
        Architecture : "ARCHITECTURE/",
        Art : "ART/",
        Antiques_Collectibles : "ANTIQUES & COLLECTIBLES/",
        Biography_Memoir : [
            "BIOGRAPHY & AUTOBIOGRAPHY/",
            "Biography & Autobiography/",
            ],
        Body_Mind_Spirit: [
            "BODY MIND & SPIRIT/",
            "MIND & SPIRIT/",
        ],
        Personal_Finance_Business : "BUSINESS & ECONOMICS/",
        Classics : [
            "Classics/",
        ],
        Cooking: [
            "COOKING/",
            "Cooking & Food",
            "Cooking/",
        ],
        Comics_Graphic_Novels : "COMICS & GRAPHIC NOVELS/",
        Computers: [
            "COMPUTERS/",
            "Computers/",
        ],
        Crafts_Hobbies : [ 
            "CRAFTS & HOBBIES/",
        ],
        Games : [
            "GAMES/",
        ],
        Design: "DESIGN/",
        Drama: "DRAMA/",
        Education : "EDUCATION/",
        Erotica: "Erotica/",
        Espionage : "Espionage/",
        Fantasy : [
            "Fantasy/",
            "Magic/",
            "Unicorns & Mythical/"
        ],
        Foreign_Language_Study : "FOREIGN LANGUAGE STUDY/",
        Gardening : "GARDENING/",
        Comics_Graphic_Novels : "Comics & Graphic Novels/",
        Health_Diet : [
            "HEALTH & FITNESS/",
            "Health/",
        ],
        Historical_Fiction : [
            "FICTION/Historical/",
            "JUVENILE FICTION/Historical/",
        ],
        History : "HISTORY/",
        Humorous_Fiction : [
            "FICTION/Humorous",
            "FICTION/Satire",
            "Humorous Stories/",
        ],
        Humorous_Nonfiction : [
            "HUMOR/",
            "Humor/",
        ],
        Horror : [
            "Horror/",
            "Horror & Ghost Stories/",
            "Occult/",
        ],
        Literary_Fiction : [
            "FICTION/Literary",
            "FICTION/Psychological",
            "FICTION/Coming of Age",
            "FICTION/Family Saga",
        ],
        Law : "LAW/",
        Mathematics : "MATHEMATICS/",
        Medical : "MEDICAL/",
        Music : "MUSIC/",
        Mystery : [
            "Mystery & Detective/",
            "FICTION/Crime/",
            "Mysteries & Detective Stories/"
        ],
        Nature : "NATURE/",
        Parenting_Family: "FAMILY & RELATIONSHIPS/",
        Performing_Arts : "PERFORMING ARTS/",
        Pets : "PETS/",
        Philosophy : "PHILOSOPHY/",
        Photography : "PHOTOGRAPHY/",
        Poetry : "POETRY/",
        Political_Science: "POLITICAL SCIENCE/",
        Psychology : "PSYCHOLOGY & PSYCHIATRY/",
        Reference_Study_Aids: "REFERENCE/",
        Religion_Spirituality : [
            "RELIGION/",
            "Religion/",
        ],
        Romance : [
            "ROMANCE/",
            "Romance/",
            "JUVENILE FICTION/Love & Romance/",
        ],
        Science : "SCIENCE/",
        Science_Fiction : "Science Fiction",
        Self_Help: "SELF-HELP/",
        Social_Sciences : "SOCIAL SCIENCE/",
        Sports : [
            "SPORTS & RECREATION/",
            "Sports & Recreation/",
        ],
        Study_Aids : "STUDY AIDS/",
        Suspense_Thriller : [
            "FICTION/Suspense/",
            "FICTION/Thrillers/",
        ],
        Technology : ["TECHNOLOGY/", "TRANSPORTATION/"],
        Travel : ["TRAVEL/", "Travel/"],
        True_Crime : "TRUE CRIME/",
        Westerns : "Westerns/",
        Urban_Fantasy: "Fantasy/Contemporary/",
        Urban_Fiction : [
            "FICTION/African American/",
            "FICTION/Urban/",
        ],
        Womens_Fiction : "FICTION/Contemporary Women/",
    }

    # These are more specific subcategories of the above categories that are checked first.
    LEVEL_2_PREFIXES = {
        Art_Criticism_Theory : "ART/Criticism",
        Art_History : "ART/History",
        Ancient_History : "HISTORY/Ancient",
        Bartending_Cocktails : "COOKING/Wine & Spirits",
        Buddhism : [
            "RELIGION/Buddhism (see also Zen Buddhism)/"
            "RELIGION/Zen Buddhism/",
        ],
        Christianity : [
            "RELIGION/Catholicism/",
            "RELIGION/Christian Life/",
            "RELIGION/Christanity/",
            "RELIGION/Christan Church/",
        ],
        Computers : [
            "BUSINESS & ECONOMICS/Industries/Computers & Information Technology/",
        ],
        Contemporary_Romance : [
            "Romance/Contemporary/",
        ],
        Games : [
            "Sports & Recreation/Games/",
        ],
        Erotica : [
            "African American/Erotica",
            "Romance/Adult",
        ],
        Fantasy: [
            "JUVENILE FICTION/Animals/Dragons",
        ],
        Film_TV: [
            "PERFORMING ARTS/Film/",
            "PERFORMING ARTS/Television/",
        ],
        Economics : [
            "BUSINESS & ECONOMICS/Economic History/",
            "BUSINESS & ECONOMICS/Economics/",
        ],
        European_History : [
            "HISTORY/Europe/",
            "HISTORY/Great Britain/",
            "HISTORY/Italy/",
            "HISTORY/Ireland/",
            "HISTORY/Russia (pre- & post-Soviet Union)/",
        ],
        Family_Relationships : [
            "FAMILY & RELATIONSHIPS/Love & Romance/",
            "FAMILY & RELATIONSHIPS/Marriage/",
        ],
        Fashion : [
            "DESIGN/Fashion/",
            "CRAFTS & HOBBIES/Fashion/",
            "SELF_HELP/Fashion & Style/",
            "Art/Fashion/",
        ],
        Hard_Boiled_Mystery : "Mystery & Detective/Hard Boiled",
        Health_Diet : "COOKING/Health",
        Hinduism : [
            "RELIGION/Hinduism",
        ],
        Horror : [
            "JUVENILE FICTION/Paranormal/",
        ],
        Historical_Romance : [
            "Romance/Historical/",
        ],
        Islam : [
            "RELIGION/Islam/",
        ],
        Judaism : [
            "RELIGION/Judaism/",
            "Religion/Judaism/",
        ],
        Latin_American_History : [
            "HISTORY/South America",
        ],
        Legal_Thriller : "Thrillers/Legal",
        LGBTQ_Fiction : [
            "LITERARY COLLECTIONS/Gay & Lesbian/",
            "FICTION/Gay/",
            "FICTION/Lesbian/",
            "JUVENILE FICTION/Gay & Lesbian/",
        ],
        Literary_Criticism : [
            "LANGUAGE ARTS & DISCIPLINES/",
            "LITERARY COLLECTIONS/",
            "LITERARY CRITICISM & COLLECTIONS/Books & Reading/",
            "LITERARY CRITICISM & COLLECTIONS/",
        ],
        Management_Leadership: [
            "BUSINESS & ECONOMICS/Management/",
            "BUSINESS & ECONOMICS/Leadership/",
        ],
        Comics_Graphic_Novels : "COMICS & GRAPHIC NOVELS/Manga/",
        Middle_East_History : [
            "HISTORY/Israel",
        ],
        Military_SF : "Science Fiction/Military",
        Military_History : "HISTORY/Military",
        Military_Thriller : "Thrillers/Military",
        Modern_History : "HISTORY/Modern",
        Music : [
            "Performing Arts/Music",
            "BIOGRAPHY & AUTOBIOGRAPHY/Composers & Musicians/",
        ],
        Paranormal_Romance : ["Romance/Paranormal"],
        Parenting: [
            "FAMILY & RELATIONSHIPS/Children with Special Needs/",
            "FAMILY & RELATIONSHIPS/Fatherhood/",
            "FAMILY & RELATIONSHIPS/Motherhood/",
            "FAMILY & RELATIONSHIPS/Adoption/",
            "FAMILY & RELATIONSHIPS/Infants & Toddlers/",
            "FAMILY & RELATIONSHIPS/Parenting/",
        ],
        Personal_Finance_Investing : [
            "BUSINESS & ECONOMICS/Investments & Securities/",
            "BUSINESS & ECONOMICS/Personal Finance/",
            "BUSINESS & ECONOMICS/Personal Success",
        ],
        Police_Procedural : "Mystery & Detective/Police Procedural",
        Political_Science : "POLITICAL SCIENCE/History & Theory/",
        Real_Estate : "BUSINESS & ECONOMICS/Real Estate/",
        Religious_Fiction : [
            "JUVENILE FICTION/Religious/",
            "FICTION/Religious/",
            "FICTION/Christian/",
            "Religious/Jewish/",
            "FICTION/Jewish/",
        ],
        Science_Fiction : [
            "LITERARY CRITICISM & COLLECTIONS/Science Fiction/",
        ],
        Space_Opera : "Science Fiction/Space Opera/",
        Romantic_Suspense : "Romance/Suspense/",
        United_States_History : [
            "HISTORY/United States",
            "HISTORY/Native American",
        ],
        Vegetarian_Vegan: "COOKING/Vegetarian",
        World_History : "HISTORY/Civilization",
        Women_Detectives : "Mystery & Detective/Women Sleuths",
    }

    LEVEL_3_PREFIXES = {
#        Regency_Romance : "Romance/Historical/Regency",
    }

    PREFIX_LISTS = [LEVEL_3_PREFIXES, LEVEL_2_PREFIXES, CATCHALL_PREFIXES]
   

    @classmethod
    def scrub_identifier(cls, identifier):
        if not identifier.endswith('/'):
            return identifier + '/'
        return identifier

    @classmethod
    def is_fiction(cls, identifier, name):    
        if identifier in cls.FICTION:
            return True
        if '/Essays/' in identifier or '/Letters/' in identifier:
            return False
        if identifier.startswith('FICTION'):
            return True
        if identifier.startswith('JUVENILE FICTION'):
            return True
        return False

    @classmethod
    def audience(cls, identifier, name):
        # We can't distinguish between young adult and children's
        # material solely based on 3M genres.  Classify it all as young
        # adult to be safe.
        if identifier.startswith("JUVENILE"):
            return cls.AUDIENCE_YOUNG_ADULT
        return cls.AUDIENCE_ADULT

    @classmethod
    def _match(cls, identifier, match_against):
        if isinstance(match_against, list):
            return any(identifier.startswith(x) for x in match_against)
        else:
            return identifier.startswith(match_against)

    @classmethod
    def genre(cls, identifier, name, fiction=None, audience=None):
        for prefixes in cls.PREFIX_LISTS:
            for l, v in prefixes.items():
                if cls._match(identifier, v):
                    return l
                for remove_prefix in [
                        'FICTION/', 'JUVENILE FICTION/', 'JUVENILE NONFICTION/']:
                    if identifier.startswith(remove_prefix):
                        check = identifier[len(remove_prefix):]
                        if cls._match(check, v):
                            return l

        return None

class BISACClassifier(ThreeMClassifier):

    def scrub_identifier(cls, identifier):
        identifier = super(Axis360Classifier, self).scrub_identifier(identifier)
        return identifier.replace(' / ', '/')


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
        Antiques_Collectibles : "Antiques",
        Architecture : "Architecture",
        Art : "Art",
        Biography_Memoir : "Biography & Autobiography",
        Business : ["Business", "Marketing & Sales", "Careers"],
        Christianity : "Christian Nonfiction",
        Computers : "Computer Technology",
        Classics : "Classic Literature",
        Cooking : "Cooking & Food",
        Crafts_Hobbies : "Crafts",
        Games : "Games",
        Drama : "Drama",
        Education : "Education",
        Erotica : "Erotic Literature",
        Fantasy : "Fantasy",
        Foreign_Language_Study : "Foreign Language Study",
        Gardening : "Gardening",
        Comics_Graphic_Novels : "Comic and Graphic Books",
        Health_Diet : "Health & Fitness",
        Historical_Fiction : "Historical Fiction",
        History : "History",
        Horror : "Horror",
        House_Home : u"Home Design & Décor",
        Humorous_Fiction : "Humor (Fiction)", 
        Humorous_Nonfiction : "Humor (Nonfiction)",
        Entertainment : "Entertainment",
        Judaism : "Judaica",
        Law : "Law",
        Literary_Criticism : [
            "Literary Criticism", "Criticism", "Literary Anthologies",
            "Language Arts"],
        Management_Leadership : "Management",
        Mathematics : "Mathematics",
        Medical : "Medical",
        Military_History : "Military",
        Music : "Music",
        Mystery : "Mystery",
        Nature : "Nature",
        Body_Mind_Spirit : "New Age",
        Parenting_Family : "Family & Relationships",
        Performing_Arts : "Performing Arts",
        Personal_Finance_Investing : "Finance",
        Pets : "Pets",
        Philosophy : ["Philosophy", "Ethics"],
        Photography : "Photography",
        Poetry : "Poetry",
        Politics_Current_Events : ["Politics", "Current Events"],
        Psychology : ["Psychology", "Psychiatry", "Psychiatry & Psychology"],
        Reference_Study_Aids : ["Reference", "Grammar & Language Usage"],
        Religious_Fiction : ["Christian Fiction"],
        Religion_Spirituality : "Religion & Spirituality",
        Romance : "Romance",
        Science : ["Science", "Physics", "Chemistry"],
        Science_Fiction : "Science Fiction",
        # Science_Fiction_Fantasy : "Science Fiction & Fantasy",
        Self_Help : ["Self-Improvement", "Self-Help", "Self Help"],
        Social_Sciences : ["Sociology", "Gender Studies"],
        Sports : "Sports & Recreations",
        Study_Aids : "Study Aids & Workbooks",
        Technology : ["Technology", "Engineering", "Transportation"],
        Suspense_Thriller : ["Suspense", "Thriller"],
        Travel : ["Travel", "Travel Literature"],
        True_Crime : "True Crime",
        Urban_Fiction: ["African American Fiction", "Urban Fiction"],
        Womens_Fiction: "Chick Lit Fiction",
    }

    @classmethod
    def scrub_identifier(cls, identifier):
        if identifier.startswith('Foreign Language Study'):
            return 'Foreign Language Study'
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
            or "Beginning Reader" in identifier or "Children's" in identifier):
            return cls.AUDIENCE_CHILDREN
        elif "Young Adult" in identifier:
            return cls.AUDIENCE_YOUNG_ADULT
        return cls.AUDIENCE_ADULT

    @classmethod
    def target_age(cls, identifier, name):
        if identifier.startswith('Picture Book'):
            return 0 # As early as possible
        elif identifier.startswith('Beginning Reader'):
            return 5 # Kindergarten
        return None

    @classmethod
    def genre(cls, identifier, name, fiction=None, audience=None):
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
    NAMES["FIC"] = "Fiction"
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
        Economics : range(330, 340),
        Christianity : [range(220, 230) + range(230, 290)],
        Cooking : [range(640, 642)],
        Performing_Arts : [790, 791, 792],
        Entertainment : 790,
        Games : [793, 794, 795],
        Drama : [812, 822, 832, 842, 852, 862, 872, 882],
        Education : range(370,380) + [707],
        European_History : range(940, 950),
        Folklore : [398],
        History : [900],
        Islam : [297],
        Judaism : [296],
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
        Politics_Current_Events : range(320, 330) + range(351, 355),
        Psychology : range(150, 160),
        Foreign_Language_Study : range(430,500),
        Reference_Study_Aids : range(10, 20) + range(30, 40) + [103, 203, 303, 403, 503, 603, 703, 803, 903] + range(410, 430),
        Religion_Spirituality : range(200, 220) + [290, 292, 293, 294, 295, 299],
        Science : ([500, 501, 502] + range(506, 510) + range(520, 530) 
                   + range(530, 540) + range(540, 550) + range(550, 560)
                   + range(560, 570) + range(570, 580) + range(580, 590)
                   + range(590, 600)),
        Social_Sciences : (range(300, 310) + range(360, 364) + range(390,397) + [399]),
        Sports : range(796, 800),
        Technology : (
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
        if identifier == 'E':
            # Juvenile fiction
            return cls.AUDIENCE_CHILDREN

        if isinstance(identifier, basestring) and identifier.startswith('J'):
            return cls.AUDIENCE_CHILDREN

        if isinstance(identifier, basestring) and identifier.startswith('Y'):
            return cls.AUDIENCE_YOUNG_ADULT

        return cls.AUDIENCE_ADULT

    @classmethod
    def genre(cls, identifier, name, fiction=None, audience=None):
        for genre, identifiers in cls.GENRES.items():
            if identifier == identifiers or (
                    isinstance(identifiers, list) 
                    and identifier in identifiers):
                return genre
        return None
    

class LCCClassifier(Classifier):

    TOP_LEVEL = re.compile("^([A-Z]{1,2})")
    FICTION = set(["PN", "PQ", "PR", "PS", "PT", "PZ"])
    JUVENILE = set(["PZ"])

    GENRES = {

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
        Business : ["HC", "HF", "HJ"],      
        Christianity : ["BR", "BS", "BT", "BV", "BX"],
        Cooking : ["TX"],
        Crafts_Hobbies : ["TT"],
        Economics : ["HB"],
        Education : ["L"],
        European_History : ["DA", "DAW", "DB", "DD", "DF", "DG", "DH", "DJ", "DK", "DL", "DP", "DQ", "DR"],
        Folklore : ["GR"],
        Games : ["GV"],
        Islam : ["BP"],
        Judaism : ["BM"],
        Literary_Criticism : ["Z"],
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
        Reference_Study_Aids : ["AE", "AG", "AI"],
        Religion_Spirituality : ["BL", "BQ"],
        Science : ["QB", "QC", "QD", "QE", "QH", "QK", "QL", "QR", "CC", "GB", "GC", "QP"],
        Social_Sciences : ["HD", "HE", "HF", "HM", "HN", "HS", "HT", "HV", "GN", "GF", "GT"],
        Sports: ["SK"],
        World_History : ["CB"],
    }

    LEFTOVERS = dict(
        B=Philosophy,
        T=Technology,
        Q=Science,
        S=Science,
        H=Social_Sciences,
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
    def genre(cls, identifier, name, fiction=None, audience=None):
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
        "autobiographies", "nonfiction", "essays", "letters")
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

    CATCHALL_KEYWORDS = { 
        Adventure : match_kw(
            "adventure",
            "adventurers",
            "adventure stories",
            "adventure fiction", 
            "adventurers",
            "sea stories",
            "war stories", 
            "men's adventure",
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
               
               Business: match_kw(
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
                             
               Crafts_Hobbies: match_kw(
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
                   # TODO: a lot of these don't work well because of
                   # the huge amount of fiction about students. This
                   # will be fixed when we institute the
                   # fiction/nonfiction split.
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
                   "family & relationships",                   
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

               Games : match_kw(
                   "games",
                   "video games",
                   "gaming",
                   "gambling",
               ),
               
               Gardening: match_kw(
                   "gardening",
                   "horticulture",
               ),
               
               Comics_Graphic_Novels: match_kw(
                   "comics",
                   "comic strip",
                   "comic strips",
                   "comic book",
                   "comic books",
                   "graphic novels",

                   # Formerly in 'Superhero'
                   "superhero",
                   "superheroes",

                   # Formerly in 'Manga'
                   "japanese comic books",
                   "japanese comics",
                   "manga",
                   "yaoi",

               ),
               
               Hard_Boiled_Mystery: match_kw(
                   "hard-boiled",
                   "noir",
               ),
               
               Health_Diet: match_kw(
                   # ! "health services" ?
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
                   "regency romance",
                   "romance.*regency",
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
               
        Humorous_Fiction : match_kw(
            "comedy",
            "humor",
            "humorous",
            "humourous",
            "humour",
            "satire",
            "wit",
        ),
        Humorous_Nonfiction : match_kw(
            "comedy",
            "humor",
            "humorous",
            "humour",
            "humourous",
            "wit",
        ),

               Entertainment: match_kw(
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
               
               LGBTQ_Fiction: match_kw(
                   'lesbian',
                   'lesbians',
                   'gay',
                   'bisexual',
                   'transgender',
                   'transsexual',
                   'transsexuals',
                   'homosexual',
                   'homosexuals',
                   'homosexuality',
                   'queer',
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
               
               Legal_Thriller: match_kw(
                   "legal thriller",
                   "legal thrillers",
               ),
                             
               Literary_Criticism: match_kw(
                   "criticism, interpretation",
                   "literary collections",
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

               
               Military_History : match_kw(
                   "military science",
                   "warfare",
                   "military",
                   "1914-1918",
                   "1939-1945",
                   "world war",
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
        Media_Tie_in_SF: match_kw(
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
               
               Body_Mind_Spirit: match_kw(
                   "new age",
               ),
               
               Paranormal_Romance : match_kw(
                   "paranormal romance",
                   "romance.*paranormal",
               ),
               
               Parenting : match_kw(
                   "children",
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
               
               Police_Procedural: match_kw(
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
               
               Reference_Study_Aids : match_kw(
                   "catalogs",
                   "handbooks",
                   "manuals",

                   # Formerly in 'Encyclopedias'
                   "encyclopaedias",
                   "encyclopaedia",
                   "encyclopedias",
                   "encyclopedia",            

                   # Formerly in 'Language Arts & Disciplines'
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
               
               Religion_Spirituality : match_kw(
                   "religion",
                   "religious",
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
                   "love stories",
                   "romance",
                   "love & romance",
                   "romances",
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
               
               #Science_Fiction_Fantasy: match_kw(
        #    "science fiction.*fantasy",
        #),
               
               Self_Help: match_kw(
                   "self help",
                   "self-help",
                   "self improvement",
                   "self-improvement",
               ),
               Folklore : match_kw(
                   "folklore",
                   "folktales",
                   "folk tales",
                   "myth",
                   "legends",
               ),
               Social_Sciences: match_kw(
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
                   'lesbian studies',
                   'gay studies',
                   "black studies",
                   "african-american studies",
               ),               
               
               Sports: match_kw(
                   # Ton of specific sports here since 'players'
                   # doesn't work. TODO: Why? I don't remember.
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
                            
               Romantic_Suspense : match_kw(
                   "romantic.*suspense",
                   "suspense.*romance",
                   "romance.*suspense",
                   "romantic.*thriller",
                   "romance.*thriller",
                   "thriller.*romance",
               ),
               
               Technology: match_kw(
                   "technology",
                   "engineering",
                   "bioengineering",

                   # Formerly in 'Transportation'
                   "transportation",
                   "railroads",
                   "trains",
                   "automotive",
                   "ships & shipbuilding",
                   "cars & trucks",
               ),
               
               Suspense_Thriller: match_kw(
                   "thriller",
                   "thrillers",
                   "suspense",
               ),

               Technothriller : match_kw(
                   "techno-thriller",
                   "technothriller",
                   "technothrillers",
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

               Westerns : match_kw(
                   "western stories",
                   "westerns",
               ),
               
               Women_Detectives : match_kw(
                   "women detectives",
                   "women detective",
                   "women private investigators",
                   "women private investigator",
                   "women sleuths",
                   "women sleuth",
               ),

               Womens_Fiction : match_kw(
                   "contemporary women",
                   "chick lit",
               ),
               
               World_History: match_kw(
                   "world history",
                   "history[^a-z]*world",
               ),              
    }

    LEVEL_2_KEYWORDS = {
        Design : match_kw(
            "arts and crafts movement",
        ),
        Drama : match_kw(
            "opera",
        ),
        Literary_Criticism : match_kw(
            "literary history", # Not History
            "romance language", # Not Romance
        ),

        # We need to match these first so that the 'military'
        # part doesn't match Military History.
        Military_SF: match_kw(
            "science fiction.*military",
            "military.*science fiction",
        ),
        Military_Thriller: match_kw(
            "military thrillers",
            "thrillers.*military",
        ),
        Pets : match_kw(
            "human-animal relationships",
        ),
        Politics_Current_Events : match_kw(
            "health care reform",
        ),

        # Stop the 'religious' from matching Religion/Spirituality.
        Religious_Fiction: match_kw(
            "christian fiction",
            "fiction.*christian",
            "religious fiction",
            "fiction.*religious",
        ),

        Romantic_Suspense : match_kw(
            "romantic.*suspense",
            "suspense.*romance",
            "romance.*suspense",
            "romantic.*thriller",
            "romance.*thriller",
            "thriller.*romance",
        ),

        Supernatural_Thriller: match_kw(
            "thriller.*supernatural",
            "supernatural.*thriller",
        ),

        # Otherwise fiction.*urban turns Urban Fantasy into Urban Fiction
        Urban_Fantasy : match_kw(
            "fiction.*fantasy.*urban",
        ),

        # Stop the 'children' in 'children of' from matching Parenting.
        None : match_kw(
            "children of",
        )
    }

    LEVEL_3_KEYWORDS = {
        Space_Opera: match_kw(
            "space opera",
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
    def genre(cls, identifier, name, fiction=None, audience=None):
        matches = Counter()
        match_against = [name]
        for l in [cls.LEVEL_3_KEYWORDS, cls.LEVEL_2_KEYWORDS, cls.CATCHALL_KEYWORDS]:
            for genre, keywords in l.items():
                if genre and fiction is not None and genre.is_fiction != fiction:
                    continue
                if (genre and audience and genre.audience_restriction
                    and audience not in genre.audience_restriction):
                    continue
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
            if most_specific_genre:
                break
        return most_specific_genre

    @classmethod
    def target_age(cls, identifier, name):
        """This tag might contain a grade level, an age in years, or nothing.
        We will try both a grade level and an age in years, but we
        will require that the tag indicate what's being measured. A
        tag like "9-12" will not match anything because we don't know if it's
        age 9-12 or grade 9-12.
        """
        age = AgeClassifier.target_age(identifier, name, True)
        if age is None:
            age = GradeLevelClassifier.target_age(identifier, name, True)
        return age

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
        "Children's Anthologies",
        ])

    GENRES = {
        Adventure: [
            "Adventure",
            "Pirates, Buccaneers, Corsairs, etc.",
        ],
        # African_American : ["African American Writers"],
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
        Humorous_Fiction : ["Humor"],
        Islam : "Islam",
        Judaism : "Judaism",
        Law : [
            "British Law",
            "Noteworthy Trials",
            "United States Law",
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
        Reference_Study_Aids : [
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
        Social_Sciences : [
            "Anthropology",
            "Archaeology",
            "The American Journal of Archaeology",
            "Sociology",
        ],
        Suspense_Thriller : [
            "Suspense",
            "Thriller",
        ],
        Technology : [
            "Engineering", 
            "Technology",
            "Transportation",
        ],
        Travel : "Travel",
        True_Crime : "Crime Nonfiction",
        Westerns : "Western",
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
    def genre(cls, identifier, name, fiction=None, audience=None):
        for l, v in cls.GENRES.items():
            if identifier == v or (isinstance(v, list) and identifier in v):
                return l
        return None

class FreeformAudienceClassifier(Classifier):
    @classmethod
    def audience(cls, identifier, name):
        if identifier == 'children':
            return cls.AUDIENCE_CHILDREN
        elif identifier in ('young adult','ya'):
            return cls.AUDIENCE_YOUNG_ADULT
        elif identifier == 'adult':
            return cls.AUDIENCE_ADULT
        elif identifier == 'adults only':
            return cls.AUDIENCE_ADULTS_ONLY


# TODO: This needs a lot of additions.
genre_publishers = {
    "Harlequin" : Romance,
    "Pocket Books/Star Trek" : Media_Tie_in_SF,
    "Kensington" : Urban_Fiction,
    "Fodor's Travel Publications" : Travel,
    "Marvel Entertainment, LLC" : Comics_Graphic_Novels,
}

genre_imprints = {
    "Harlequin Intrigue" : Romantic_Suspense,
    "Love Inspired Suspense" : Romantic_Suspense,
    "Harlequin Historical" : Historical_Romance,
    "Harlequin Historical Undone" : Historical_Romance,
    "Frommers" : Travel,
}

audience_imprints = {
    "Harlequin Teen" : Classifier.AUDIENCE_YOUNG_ADULT,
    "HarperTeen" : Classifier.AUDIENCE_YOUNG_ADULT,
    "Open Road Media Teen & Tween" : Classifier.AUDIENCE_YOUNG_ADULT,
    "Rosen Young Adult" : Classifier.AUDIENCE_YOUNG_ADULT,
}

not_adult_publishers = set([
    "Scholastic Inc.",
    "Random House Children's Books",
    "Little, Brown Books for Young Readers",
    "Penguin Young Readers Group",
    "Hachette Children's Books",
    "Nickelodeon Publishing",
])

not_adult_imprints = set([
    "Scholastic",
    "Random House Books for Young Readers",
    "HMH Books for Young Readers",
    "Knopf Books for Young Readers",
    "Delacorte Books for Young Readers",
    "Open Road Media Young Readers",
    "Macmillan Young Listeners",
    "Bloomsbury Childrens",
    "NYR Children's Collection",
    "Bloomsbury USA Childrens",
    "National Geographic Children's Books",
])

fiction_imprints = set(["Del Rey"])
nonfiction_imprints = set(["Harlequin Nonfiction"])

nonfiction_publishers = set(["Wiley"])
fiction_publishers = set([])


# Make a dictionary of classification schemes to classifiers.
Classifier.classifiers[Classifier.DDC] = DeweyDecimalClassifier
Classifier.classifiers[Classifier.LCC] = LCCClassifier
Classifier.classifiers[Classifier.FAST] = FASTClassifier
Classifier.classifiers[Classifier.LCSH] = LCSHClassifier
Classifier.classifiers[Classifier.TAG] = TAGClassifier
Classifier.classifiers[Classifier.OVERDRIVE] = OverdriveClassifier
Classifier.classifiers[Classifier.THREEM] = ThreeMClassifier
Classifier.classifiers[Classifier.BISAC] = BISACClassifier
Classifier.classifiers[Classifier.AGE_RANGE] = AgeClassifier
Classifier.classifiers[Classifier.GRADE_LEVEL] = GradeLevelClassifier
Classifier.classifiers[Classifier.FREEFORM_AUDIENCE] = FreeformAudienceClassifier
Classifier.classifiers[Classifier.GUTENBERG_BOOKSHELF] = GutenbergBookshelfClassifier
Classifier.classifiers[Classifier.INTEREST_LEVEL] = InterestLevelClassifier
