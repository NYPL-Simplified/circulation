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

import logging
import json
import os
import pkgutil
import re
import urllib
from collections import (
    Counter,
    defaultdict,
)
from nose.tools import set_trace
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.expression import and_

base_dir = os.path.split(__file__)[0]
resource_dir = os.path.join(base_dir, "..", "resources")

NO_VALUE = "NONE"
NO_NUMBER = -1

class Classifier(object):

    """Turn an external classification into an internal genre, an
    audience, an age level, and a fiction status.
    """

    DDC = "DDC"
    LCC = "LCC"
    LCSH = "LCSH"
    FAST = "FAST"
    OVERDRIVE = "Overdrive"
    RBDIGITAL = "RBdigital"
    BISAC = "BISAC"
    BIC = "BIC"
    TAG = "tag"   # Folksonomic tags.

    # Appeal controlled vocabulary developed by NYPL
    NYPL_APPEAL = "NYPL Appeal"

    GRADE_LEVEL = "Grade level" # "1-2", "Grade 4", "Kindergarten", etc.
    AGE_RANGE = "schema:typicalAgeRange" # "0-2", etc.
    AXIS_360_AUDIENCE = "Axis 360 Audience"
    RBDIGITAL_AUDIENCE = "RBdigital Audience"

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
    AUDIENCE_ALL_AGES = "All Ages"
    AUDIENCE_RESEARCH = "Research"

    # A book for a child younger than 14 is a children's book.
    # A book for a child 14 or older is a young adult book.
    YOUNG_ADULT_AGE_CUTOFF = 14

    ADULT_AGE_CUTOFF = 18

    # "All ages" actually means "all ages with reading fluency".
    ALL_AGES_AGE_CUTOFF = 8

    AUDIENCES_YOUNG_CHILDREN = [AUDIENCE_CHILDREN, AUDIENCE_ALL_AGES]
    AUDIENCES_JUVENILE = AUDIENCES_YOUNG_CHILDREN + [AUDIENCE_YOUNG_ADULT]
    AUDIENCES_ADULT = [AUDIENCE_ADULT, AUDIENCE_ADULTS_ONLY, AUDIENCE_ALL_AGES]
    AUDIENCES = set([AUDIENCE_ADULT, AUDIENCE_ADULTS_ONLY, AUDIENCE_YOUNG_ADULT,
                     AUDIENCE_CHILDREN, AUDIENCE_ALL_AGES, AUDIENCE_RESEARCH])
    AUDIENCES_NO_RESEARCH = [
        x for x in AUDIENCES if x != AUDIENCE_RESEARCH
    ]

    SIMPLIFIED_GENRE = "http://librarysimplified.org/terms/genres/Simplified/"
    SIMPLIFIED_FICTION_STATUS = "http://librarysimplified.org/terms/fiction/"

    classifiers = dict()

    @classmethod
    def range_tuple(cls, lower, upper):
        """Turn a pair of ages into a tuple that represents an age range.
        This may be turned into an inclusive postgres NumericRange later,
        but this code should not depend on postgres.
        """
        # Just in case the upper and lower ranges are mixed up,
        # and no prior code caught this, un-mix them.
        if lower and upper and lower > upper:
            lower, upper = upper, lower
        return (lower, upper)

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
        identifier, name = cls.scrub_identifier_and_name(
            subject.identifier, subject.name
        )
        fiction = cls.is_fiction(identifier, name)
        audience = cls.audience(identifier, name)

        target_age = cls.target_age(identifier, name)
        if target_age == cls.range_tuple(None, None):
            target_age = cls.default_target_age_for_audience(audience)

        return (cls.genre(identifier, name, fiction, audience),
                audience,
                target_age,
                fiction,
                )

    @classmethod
    def scrub_identifier_and_name(cls, identifier, name):
        """Prepare identifier and name from within a call to classify()."""
        identifier = cls.scrub_identifier(identifier)
        if isinstance(identifier, tuple):
            # scrub_identifier returned a canonical value for name as
            # well. Use it in preference to any name associated with
            # the subject.
            identifier, name = identifier
        elif not name:
            name = identifier
        name = cls.scrub_name(name)
        return identifier, name

    @classmethod
    def scrub_identifier(cls, identifier):
        """Prepare an identifier from within a call to classify().

        This may involve data normalization, conversion to lowercase,
        etc.
        """
        if identifier is None:
            return None
        return Lowercased(identifier)

    @classmethod
    def scrub_name(cls, name):
        """Prepare a name from within a call to classify()."""
        if name is None:
            return None
        return Lowercased(name)


    @classmethod
    def genre(cls, identifier, name, fiction=None, audience=None):
        """Is this identifier associated with a particular Genre?"""
        return None

    @classmethod
    def genre_match(cls, query):
        """Does this query string match a particular Genre, and which part
        of the query matches?"""
        return None, None

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
    def audience_match(cls, query):
        """Does this query string match a particular Audience, and which
        part of the query matches?"""
        return (None, None)

    @classmethod
    def target_age(cls, identifier, name):
        """For children's books, what does this identifier+name say
        about the target age for this book?
        """
        return cls.range_tuple(None, None)

    @classmethod
    def default_target_age_for_audience(cls, audience):
        """The default target age for a given audience.

        We don't know what age range a children's book is appropriate
        for, but we can make a decent guess for a YA book, for an
        'Adult' book it's pretty clear, and for an 'Adults Only' book
        it's very clear.
        """
        if audience == Classifier.AUDIENCE_YOUNG_ADULT:
            return cls.range_tuple(14, 17)
        elif audience in (
                Classifier.AUDIENCE_ADULT, Classifier.AUDIENCE_ADULTS_ONLY
        ):
            return cls.range_tuple(18, None)
        return cls.range_tuple(None, None)

    @classmethod
    def default_audience_for_target_age(cls, range):
        if range is None:
            return None
        lower = range[0]
        upper = range[1]
        if not lower and not upper:
            # You could interpret this as 'all ages' but it's more
            # likely the data is simply missing.
            return None
        if not lower:
            if upper >= cls.ADULT_AGE_CUTOFF:
                # e.g. "up to 20 years", though this doesn't
                # really make sense.
                #
                # The 'all ages' interpretation is more plausible here
                # but it's still more likely that this is simply a
                # book for grown-ups and no lower bound was provided.
                return cls.AUDIENCE_ADULT
            elif upper > cls.YOUNG_ADULT_AGE_CUTOFF:
                # e.g. "up to 15 years"
                return cls.AUDIENCE_YOUNG_ADULT
            else:
                # e.g. "up to 14 years"
                return cls.AUDIENCE_CHILDREN

        # At this point we can assume that lower is not None.
        if lower >= 18:
            return cls.AUDIENCE_ADULT
        elif lower >= cls.YOUNG_ADULT_AGE_CUTOFF:
            return cls.AUDIENCE_YOUNG_ADULT
        elif lower <= cls.ALL_AGES_AGE_CUTOFF and (
            upper is not None and upper >= cls.ADULT_AGE_CUTOFF
        ):
            # e.g. "for children ages 7-77". The 'all ages' reading
            # is here the most plausible.
            return cls.AUDIENCE_ALL_AGES
        elif lower >= 12 and (not upper or upper >= cls.YOUNG_ADULT_AGE_CUTOFF):
            # Although we treat "Young Adult" as starting at 14, many
            # outside sources treat it as starting at 12. As such we
            # treat "12 and up" or "12-14" as an indicator of a Young
            # Adult audience, with a target age that overlaps what we
            # consider a Children audience.
            return cls.AUDIENCE_YOUNG_ADULT
        else:
            return cls.AUDIENCE_CHILDREN

    @classmethod
    def and_up(cls, young, keyword):
        """Encapsulates the logic of what "[x] and up" actually means.

        Given the lower end of an age range, tries to determine the
        upper end of the range.
        """
        if young is None:
            return None
        if not any(
                [keyword.endswith(x) for x in
                 ("and up", "and up.", "+", "+.")
             ]
        ):
            return None

        if young >= 18:
            old = young
        elif young >= 12:
            # "12 and up", "14 and up", etc.  are
            # generally intended to cover the entire
            # YA span.
            old = 17
        elif young >= 8:
            # "8 and up" means something like "8-12"
            old = young + 4
        else:
            # Whereas "3 and up" really means more
            # like "3 to 5".
            old = young + 2
        return old

class GradeLevelClassifier(Classifier):
    # How old a kid is when they start grade N in the US.
    american_grade_to_age = {
        # Preschool: 3-4 years
        'preschool' : 3,
        'pre-school' : 3,
        'p' : 3,
        'pk' : 4,

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
            "grades? ([kp0-9]+) to ([kp0-9]+)?",
            "grades? ([kp0-9]+) ?-? ?([kp0-9]+)?",
            "gr\.? ([kp0-9]+) ?-? ?([kp0-9]+)?",
            "grades?: ([kp0-9]+) to ([kp0-9]+)",
            "grades?: ([kp0-9]+) ?-? ?([kp0-9]+)?",
            "gr\.? ([kp0-9]+)",
            "([0-9]+)[tnsr][hdt] grade",
            "([a-z]+) grade",
            r'\b(kindergarten|preschool)\b',
        ]
    ]

    generic_grade_res = [
        re.compile(r"([kp0-9]+) ?- ?([0-9]+)", re.I),
        re.compile(r"([kp0-9]+) ?to ?([0-9]+)", re.I),
        re.compile(r"^([0-9]+)\b", re.I),
        re.compile(r"^([kp])\b", re.I),
    ]

    @classmethod
    def audience(cls, identifier, name, require_explicit_age_marker=False):
        target_age = cls.target_age(identifier, name, require_explicit_age_marker)
        return cls.default_audience_for_target_age(target_age)


    @classmethod
    def target_age(cls, identifier, name, require_explicit_grade_marker=False):

        if (identifier and "education" in identifier) or (name and 'education' in name):
            # This is a book about teaching, e.g. fifth grade.
            return cls.range_tuple(None, None)

        if (identifier and 'grader' in identifier) or (name and 'grader' in name):
            # This is a book about, e.g. fifth graders.
            return cls.range_tuple(None, None)

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
                    gr = m.groups()
                    if len(gr) == 1:
                        young = gr[0]
                        old = None
                    else:
                        young, old = gr

                    # Strip leading zeros
                    if young and young.lstrip('0'):
                        young = young.lstrip("0")
                    if old and old.lstrip('0'):
                        old = old.lstrip("0")

                    young = cls.american_grade_to_age.get(young)
                    old = cls.american_grade_to_age.get(old)

                    if not young and not old:
                        return cls.range_tuple(None, None)

                    if young:
                        young = int(young)
                    if old:
                        old = int(old)
                    if old is None:
                        old = cls.and_up(young, k)
                    if old is None and young is not None:
                        old = young
                    if young is None and old is not None:
                        young = old
                    if old and young and  old < young:
                        young, old = old, young
                    return cls.range_tuple(young, old)
        return cls.range_tuple(None, None)

    @classmethod
    def target_age_match(cls, query):
        target_age = None
        grade_words = None
        target_age = cls.target_age(None, query, require_explicit_grade_marker=True)
        if target_age:
            for r in cls.grade_res:
                match = r.search(query)
                if match:
                    grade_words = match.group()
                    break
        return (target_age, grade_words)

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
            return cls.range_tuple(5,8)
        if identifier in ('mg+', 'mg'):
            return cls.range_tuple(9,13)
        if identifier == 'ug':
            return cls.range_tuple(14,17)
        return None


class AgeClassifier(Classifier):
    # Regular expressions that match common ways of expressing ages.
    age_res = [
        re.compile(x, re.I) for x in [
            "age ([0-9]+) ?-? ?([0-9]+)?",
            "age: ([0-9]+) ?-? ?([0-9]+)?",
            "age: ([0-9]+) to ([0-9]+)",
            "ages ([0-9]+) ?- ?([0-9]+)",
            "([0-9]+) ?- ?([0-9]+) years?",
            "([0-9]+) years?",
            "ages ([0-9]+)+",
            "([0-9]+) and up",
            "([0-9]+) years? and up",
        ]
    ]

    generic_age_res = [
        re.compile("([0-9]+) ?- ?([0-9]+)", re.I),
        re.compile(r"^([0-9]+)\b", re.I),
    ]

    baby_re = re.compile("^baby ?- ?([0-9]+) year", re.I)

    @classmethod
    def audience(cls, identifier, name, require_explicit_age_marker=False):

        target_age = cls.target_age(identifier, name, require_explicit_age_marker)
        return cls.default_audience_for_target_age(target_age)

    @classmethod
    def target_age(cls, identifier, name, require_explicit_age_marker=False):
        if require_explicit_age_marker:
            res = cls.age_res
        else:
            res = cls.age_res + cls.generic_age_res
        if identifier:
            match = cls.baby_re.search(identifier)
            if match:
                # This is for babies.
                upper_bound = int(match.groups()[0])
                return cls.range_tuple(0, upper_bound)

        for r in res:
            for k in identifier, name:
                if not k:
                    continue
                m = r.search(k)
                if m:
                    groups = m.groups()
                    young = old = None
                    if groups:
                        young = int(groups[0])
                        if len(groups) > 1 and groups[1] != None:
                            old = int(groups[1])
                    if old is None:
                        old = cls.and_up(young, k)
                    if old is None and young is not None:
                        old = young
                    if young is None and old is not None:
                        young = old
                    if old > 99:
                        # This is not an age at all.
                        old = None
                    if young > 99:
                        # This is not an age at all.
                        young = None
                    if young > old:
                        young, old = old, young
                    return cls.range_tuple(young, old)

        return cls.range_tuple(None, None)

    @classmethod
    def target_age_match(cls, query):
        target_age = None
        age_words = None
        target_age = cls.target_age(None, query, require_explicit_age_marker=True)
        if target_age:
            for r in cls.age_res:
                match = r.search(query)
                if match:
                    age_words = match.group()
                    break
        return (target_age, age_words)


# This is the large-scale structure of our classification system.
#
# If the name of a genre is a string, it's the name of the genre
# and there are no subgenres.
#
# If the name of a genre is a dictionary, the 'name' argument is the
# name of the genre, and the 'subgenres' argument is the list of the
# subgenres.

COMICS_AND_GRAPHIC_NOVELS = u"Comics & Graphic Novels"

fiction_genres = [
    u"Adventure",
    u"Classics",
    COMICS_AND_GRAPHIC_NOVELS,
    u"Drama",
    dict(name=u"Erotica", audiences=Classifier.AUDIENCE_ADULTS_ONLY),
    dict(name=u"Fantasy", subgenres=[
        u"Epic Fantasy",
        u"Historical Fantasy",
        u"Urban Fantasy",
    ]),
    u"Folklore",
    u"Historical Fiction",
    dict(name=u"Horror", subgenres=[
        u"Gothic Horror",
        u"Ghost Stories",
        u"Vampires",
        u"Werewolves",
        u"Occult Horror",
    ]),
    u"Humorous Fiction",
    u"Literary Fiction",
    u"LGBTQ Fiction",
    dict(name=u"Mystery", subgenres=[
        u"Crime & Detective Stories",
        u"Hard-Boiled Mystery",
        u"Police Procedural",
        u"Cozy Mystery",
        u"Historical Mystery",
        u"Paranormal Mystery",
        u"Women Detectives",
    ]),
    u"Poetry",
    u"Religious Fiction",
    dict(name=u"Romance", subgenres=[
        u"Contemporary Romance",
        u"Gothic Romance",
        u"Historical Romance",
        u"Paranormal Romance",
        u"Western Romance",
        u"Romantic Suspense",
    ]),
    dict(name=u"Science Fiction", subgenres=[
        u"Dystopian SF",
        u"Space Opera",
        u"Cyberpunk",
        u"Military SF",
        u"Alternative History",
        u"Steampunk",
        u"Romantic SF",
        u"Media Tie-in SF",
    ]),
    u"Short Stories",
    dict(name=u"Suspense/Thriller",
        subgenres=[
            u"Historical Thriller",
            u"Espionage",
            u"Supernatural Thriller",
            u"Medical Thriller",
            u"Political Thriller",
            u"Psychological Thriller",
            u"Technothriller",
            u"Legal Thriller",
            u"Military Thriller",
        ],
    ),
    u"Urban Fiction",
    u"Westerns",
    u"Women's Fiction",
]

nonfiction_genres = [
    dict(name=u"Art & Design", subgenres=[
        u"Architecture",
        u"Art",
        u"Art Criticism & Theory",
        u"Art History",
        u"Design",
        u"Fashion",
        u"Photography",
    ]),
    u"Biography & Memoir",
    u"Education",
    dict(name=u"Personal Finance & Business", subgenres=[
        u"Business",
        u"Economics",
        u"Management & Leadership",
        u"Personal Finance & Investing",
        u"Real Estate",
    ]),
    dict(name=u"Parenting & Family", subgenres=[
        u"Family & Relationships",
        u"Parenting",
    ]),
    dict(name=u"Food & Health", subgenres=[
        u"Bartending & Cocktails",
        u"Cooking",
        u"Health & Diet",
        u"Vegetarian & Vegan",
    ]),
    dict(name=u"History", subgenres=[
        u"African History",
        u"Ancient History",
        u"Asian History",
        u"Civil War History",
        u"European History",
        u"Latin American History",
        u"Medieval History",
        u"Middle East History",
        u"Military History",
        u"Modern History",
        u"Renaissance & Early Modern History",
        u"United States History",
        u"World History",
    ]),
    dict(name=u"Hobbies & Home", subgenres=[
        u"Antiques & Collectibles",
        u"Crafts & Hobbies",
        u"Gardening",
        u"Games",
        u"House & Home",
        u"Pets",
    ]),
    u"Humorous Nonfiction",
    dict(name=u"Entertainment", subgenres=[
        u"Film & TV",
        u"Music",
        u"Performing Arts",
    ]),
    u"Life Strategies",
    u"Literary Criticism",
    u"Periodicals",
    u"Philosophy",
    u"Political Science",
    dict(name=u"Reference & Study Aids", subgenres=[
        u"Dictionaries",
        u"Foreign Language Study",
        u"Law",
        u"Study Aids",
    ]),
    dict(name=u"Religion & Spirituality", subgenres=[
        u"Body, Mind & Spirit",
        u"Buddhism",
        u"Christianity",
        u"Hinduism",
        u"Islam",
        u"Judaism",
    ]),
    dict(name=u"Science & Technology", subgenres=[
        u"Computers",
        u"Mathematics",
        u"Medical",
        u"Nature",
        u"Psychology",
        u"Science",
        u"Social Sciences",
        u"Technology",
    ]),
    u"Self-Help",
    u"Sports",
    u"Travel",
    u"True Crime",
]


class GenreData(object):
    def __init__(self, name, is_fiction, parent=None, audience_restriction=None):
        self.name = name
        self.parent = parent
        self.is_fiction = is_fiction
        self.subgenres = []
        if isinstance(audience_restriction, basestring):
            audience_restriction = [audience_restriction]
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
        for source, default_fiction in (
                (fiction_source, True),
                (nonfiction_source, False)):
            for item in source:
                subgenres = []
                audience_restriction = None
                name = item
                fiction = default_fiction
                if isinstance(item, dict):
                    name = item['name']
                    subgenres = item.get('subgenres', [])
                    audience_restriction = item.get('audience_restriction')
                    fiction = item.get('fiction', default_fiction)

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
            name = data['name']
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
        if isinstance(value, Lowercased):
            # Nothing to do.
            return value
        if not isinstance(value, basestring):
            value = unicode(value)
        new_value = value.lower()
        if new_value.endswith('.'):
            new_value = new_value[:-1]
        o = super(Lowercased, cls).__new__(cls, new_value)
        o.original = value
        return o

    @classmethod
    def scrub_identifier(cls, identifier):
        if not identifier:
            return identifier

class AgeOrGradeClassifier(Classifier):

    @classmethod
    def audience(cls, identifier, name):
        audience = AgeClassifier.audience(identifier, name)
        if audience == None:
            audience = GradeLevelClassifier.audience(identifier, name)
        return audience

    @classmethod
    def target_age(cls, identifier, name):
        """This tag might contain a grade level, an age in years, or nothing.
        We will try both a grade level and an age in years, but we
        will require that the tag indicate what's being measured. A
        tag like "9-12" will not match anything because we don't know if it's
        age 9-12 or grade 9-12.
        """
        age = AgeClassifier.target_age(identifier, name, True)
        if age == cls.range_tuple(None, None):
            age = GradeLevelClassifier.target_age(identifier, name, True)
        return age

class FreeformAudienceClassifier(AgeOrGradeClassifier):
    # NOTE: In practice, subjects like "books for all ages" tend to be
    # more like advertising slogans than reliable indicators of an
    # ALL_AGES audience. So the only subject of this type we handle is
    # the literal string "all ages", as it would appear, e.g., in the
    # output of the metadata wrangler.

    @classmethod
    def audience(cls, identifier, name):
        if identifier in ('children', 'pre-adolescent', 'beginning reader'):
            return cls.AUDIENCE_CHILDREN
        elif identifier in ('young adult', 'ya', 'teenagers', 'adolescent',
                            'early adolescents'):
            return cls.AUDIENCE_YOUNG_ADULT
        elif identifier == 'adult':
            return cls.AUDIENCE_ADULT
        elif identifier == 'adults only':
            return cls.AUDIENCE_ADULTS_ONLY
        elif identifier == 'all ages':
            return cls.AUDIENCE_ALL_AGES
        elif identifier == 'research':
            return cls.AUDIENCE_RESEARCH
        return AgeOrGradeClassifier.audience(identifier, name)

    @classmethod
    def target_age(cls, identifier, name):
        if identifier == 'beginning reader':
            return cls.range_tuple(5,8)
        if identifier == 'pre-adolescent':
            return cls.range_tuple(9, 12)
        if identifier == 'early adolescents':
            return cls.range_tuple(13, 15)
        if identifier == 'all ages':
            return cls.range_tuple(
                cls.ALL_AGES_AGE_CUTOFF, None
            )
        strict_age = AgeClassifier.target_age(identifier, name, True)
        if strict_age[0] or strict_age[1]:
            return strict_age

        strict_grade = GradeLevelClassifier.target_age(identifier, name, True)
        if strict_grade[0] or strict_grade[1]:
            return strict_grade

        # Default to assuming it's an unmarked age.
        return AgeClassifier.target_age(identifier, name, False)


class WorkClassifier(object):
    """Boil down a bunch of Classification objects into a few values."""

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
        "LucasBooks": Media_Tie_in_SF,
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
        "Scholastic Paperbacks",
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

    def __init__(self, work, test_session=None, debug=False):
        self._db = Session.object_session(work)
        if test_session:
            self._db = test_session
        self.work = work
        self.fiction_weights = Counter()
        self.audience_weights = Counter()
        self.target_age_lower_weights = Counter()
        self.target_age_upper_weights = Counter()
        self.genre_weights = Counter()
        self.direct_from_license_source = set()
        self.prepared = False
        self.debug = debug
        self.classifications = []
        self.seen_classifications = set()
        self.log = logging.getLogger("Classifier (workid=%d)" % self.work.id)
        self.using_staff_genres = False
        self.using_staff_fiction_status = False
        self.using_staff_audience = False
        self.using_staff_target_age = False

        # Keep track of whether we've seen one of Overdrive's generic
        # "Juvenile" classifications, as well as its more specific
        # subsets like "Picture Books" and "Beginning Readers"
        self.overdrive_juvenile_generic = False
        self.overdrive_juvenile_with_target_age = False

    def add(self, classification):
        """Prepare a single Classification for consideration."""
        try:
            from ..model import DataSource, Subject
        except ValueError:
            from model import DataSource, Subject

        # We only consider a given classification once from a given
        # data source.
        key = (classification.subject, classification.data_source)
        if key in self.seen_classifications:
            return
        self.seen_classifications.add(key)
        if self.debug:
            self.classifications.append(classification)

        # Make sure the Subject is ready to be used in calculations.
        if not classification.subject.checked: # or self.debug
            classification.subject.assign_to_genre()

        if classification.comes_from_license_source:
            self.direct_from_license_source.add(classification)
        else:
            if classification.subject.describes_format:
                # TODO: This is a bit of a hack.
                #
                # Only accept a classification having to do with
                # format (e.g. 'comic books') if that classification
                # comes direct from the license source. Otherwise it's
                # really easy for a graphic adaptation of a novel to
                # get mixed up with the original novel, whereupon the
                # original book is classified as a graphic novel.
                return

        # Put the weight of the classification behind various
        # considerations.
        weight = classification.scaled_weight
        subject = classification.subject
        from_staff = classification.data_source.name == DataSource.LIBRARY_STAFF

        # if classification is genre or NONE from staff, ignore all non-staff genres
        is_genre = subject.genre != None
        is_none = (from_staff and subject.type == Subject.SIMPLIFIED_GENRE and subject.identifier == SimplifiedGenreClassifier.NONE)
        if is_genre or is_none:
            if not from_staff and self.using_staff_genres:
                return
            if from_staff and not self.using_staff_genres:
                # first encounter with staff genre, so throw out existing genre weights
                self.using_staff_genres = True
                self.genre_weights = Counter()
            if is_genre:
                self.weigh_genre(subject.genre, weight)

        # if staff classification is fiction or nonfiction, ignore all other fictions
        if not self.using_staff_fiction_status:
            if from_staff and subject.type == Subject.SIMPLIFIED_FICTION_STATUS:
                # encountering first staff fiction status,
                # so throw out existing fiction weights
                self.using_staff_fiction_status = True
                self.fiction_weights = Counter()
            self.fiction_weights[subject.fiction] += weight

        # if staff classification is about audience, ignore all other audience classifications
        if not self.using_staff_audience:
            if from_staff and subject.type == Subject.FREEFORM_AUDIENCE:
                self.using_staff_audience = True
                self.audience_weights = Counter()
                self.audience_weights[subject.audience] += weight
            else:
                if classification.generic_juvenile_audience:
                    # We have a generic 'juvenile' classification. The
                    # audience might say 'Children' or it might say 'Young
                    # Adult' but we don't actually know which it is.
                    #
                    # We're going to split the difference, with a slight
                    # preference for YA, to bias against showing
                    # age-inappropriate material to children. To
                    # counterbalance the fact that we're splitting up the
                    # weight this way, we're also going to treat this
                    # classification as evidence _against_ an 'adult'
                    # classification.
                    self.audience_weights[Classifier.AUDIENCE_YOUNG_ADULT] += (weight * 0.6)
                    self.audience_weights[Classifier.AUDIENCE_CHILDREN] += (weight * 0.4)
                    for audience in Classifier.AUDIENCES_ADULT:
                        if audience != Classifier.AUDIENCE_ALL_AGES:
                            # 'All Ages' is considered an adult audience,
                            # but a generic 'juvenile' classification
                            # is not evidence against it.
                            self.audience_weights[audience] -= weight * 0.5
                else:
                    self.audience_weights[subject.audience] += weight

        if not self.using_staff_target_age:
            if from_staff and subject.type == Subject.AGE_RANGE:
                self.using_staff_target_age = True
                self.target_age_lower_weights = Counter()
                self.target_age_upper_weights = Counter()
            if subject.target_age:
                # Figure out how reliable this classification really is as
                # an indicator of a target age.
                scaled_weight = classification.weight_as_indicator_of_target_age
                target_min = subject.target_age.lower
                target_max = subject.target_age.upper
                if target_min is not None:
                    if not subject.target_age.lower_inc:
                        target_min += 1
                    self.target_age_lower_weights[target_min] += scaled_weight
                if target_max is not None:
                    if not subject.target_age.upper_inc:
                        target_max -= 1
                    self.target_age_upper_weights[target_max] += scaled_weight

        if not self.using_staff_audience and not self.using_staff_target_age:
            if subject.type=='Overdrive' and subject.audience==Classifier.AUDIENCE_CHILDREN:
                if subject.target_age and (
                        subject.target_age.lower or subject.target_age.upper
                ):
                    # This is a juvenile classification like "Picture
                    # Books" which implies a target age.
                    self.overdrive_juvenile_with_target_age = classification
                else:
                    # This is a generic juvenile classification like
                    # "Juvenile Fiction".
                    self.overdrive_juvenile_generic = classification

    def weigh_metadata(self):
        """Modify the weights according to the given Work's metadata.

        Use work metadata to simulate classifications.

        This is basic stuff, like: Harlequin tends to publish
        romances.
        """
        if self.work.title and ('Star Trek:' in self.work.title
            or 'Star Wars:' in self.work.title
            or ('Jedi' in self.work.title
                and self.work.imprint=='Del Rey')
        ):
            self.weigh_genre(Media_Tie_in_SF, 100)

        publisher = self.work.publisher
        imprint = self.work.imprint
        if (imprint in self.nonfiction_imprints
            or publisher in self.nonfiction_publishers):
            self.fiction_weights[False] = 100
        elif (imprint in self.fiction_imprints
              or publisher in self.fiction_publishers):
            self.fiction_weights[True] = 100

        if imprint in self.genre_imprints:
            self.weigh_genre(self.genre_imprints[imprint], 100)
        elif publisher in self.genre_publishers:
            self.weigh_genre(self.genre_publishers[publisher], 100)

        if imprint in self.audience_imprints:
            self.audience_weights[self.audience_imprints[imprint]] += 100
        elif (publisher in self.not_adult_publishers
              or imprint in self.not_adult_imprints):
            for audience in [Classifier.AUDIENCE_ADULT,
                             Classifier.AUDIENCE_ADULTS_ONLY]:
                self.audience_weights[audience] -= 100

    def prepare_to_classify(self):
        """Called the first time classify() is called. Does miscellaneous
        one-time prep work that requires all data to be in place.
        """
        self.weigh_metadata()

        explicitly_indicated_audiences = (
            Classifier.AUDIENCE_CHILDREN,
            Classifier.AUDIENCE_YOUNG_ADULT,
            Classifier.AUDIENCE_ADULTS_ONLY)
        audiences_from_license_source = set(
            [classification.subject.audience
             for classification in self.direct_from_license_source]
        )
        if (self.direct_from_license_source
            and not self.using_staff_audience
            and not any(
                audience in explicitly_indicated_audiences
                for audience in audiences_from_license_source
        )):
            # If this was erotica, or a book for children or young
            # adults, the distributor would have given some indication
            # of that fact. In the absense of any such indication, we
            # can assume very strongly that this is a regular old book
            # for adults.
            #
            # 3M is terrible at distinguishing between childrens'
            # books and YA books, but books for adults can be
            # distinguished by their _lack_ of childrens/YA
            # classifications.
            self.audience_weights[Classifier.AUDIENCE_ADULT] += 500

        if (self.overdrive_juvenile_generic
            and not self.overdrive_juvenile_with_target_age):
            # This book is classified under 'Juvenile Fiction' but not
            # under 'Picture Books' or 'Beginning Readers'. The
            # implicit target age here is 9-12 (the portion of
            # Overdrive's 'juvenile' age range not covered by 'Picture
            # Books' or 'Beginning Readers'.
            weight = self.overdrive_juvenile_generic.weight_as_indicator_of_target_age
            self.target_age_lower_weights[9] += weight
            self.target_age_upper_weights[12] += weight

        self.prepared = True

    def classify(self, default_fiction=None, default_audience=None):
        # Do a little prep work.
        if not self.prepared:
            self.prepare_to_classify()

        if self.debug:
            for c in self.classifications:
                self.log.debug(
                    "%d %r (via %s)", c.weight, c.subject, c.data_source.name
                )

        # Actually figure out the classifications
        fiction = self.fiction(default_fiction=default_fiction)
        genres = self.genres(fiction)
        audience = self.audience(genres, default_audience=default_audience)
        target_age = self.target_age(audience)
        if self.debug:
            self.log.debug("Fiction weights:")
            for k, v in self.fiction_weights.most_common():
                self.log.debug(" %s: %s", v, k)
            self.log.debug("Genre weights:")
            for k, v in self.genre_weights.most_common():
                self.log.debug(" %s: %s", v, k)
            self.log.debug("Audience weights:")
            for k, v in self.audience_weights.most_common():
                self.log.debug(" %s: %s", v, k)
        return genres, fiction, audience, target_age

    def fiction(self, default_fiction=None):
        """Is it more likely this is a fiction or nonfiction book?"""
        if not self.fiction_weights:
            # We have absolutely no idea one way or the other, and it
            # would be irresponsible to guess.
            return default_fiction
        is_fiction = default_fiction
        if self.fiction_weights[True] > self.fiction_weights[False]:
            is_fiction = True
        elif self.fiction_weights[False] > 0:
            is_fiction = False
        return is_fiction

    def audience(self, genres=[], default_audience=None):
        """What's the most likely audience for this book?
        :param default_audience: To avoid embarassing situations we will
        classify works as being intended for adults absent convincing
        evidence to the contrary. In some situations (like the metadata
        wrangler), it's better to state that we have no information, so
        default_audience can be set to None.
        """

        # If we determined that Erotica was a significant enough
        # component of the classification to count as a genre, the
        # audience will always be 'Adults Only', even if the audience
        # weights would indicate something else.
        if Erotica in genres:
            return Classifier.AUDIENCE_ADULTS_ONLY

        w = self.audience_weights
        if not self.audience_weights:
            # We have absolutely no idea, and it would be
            # irresponsible to guess.
            return default_audience

        children_weight = w.get(Classifier.AUDIENCE_CHILDREN, 0)
        ya_weight = w.get(Classifier.AUDIENCE_YOUNG_ADULT, 0)
        adult_weight = w.get(Classifier.AUDIENCE_ADULT, 0)
        adults_only_weight = w.get(Classifier.AUDIENCE_ADULTS_ONLY, 0)
        all_ages_weight = w.get(Classifier.AUDIENCE_ALL_AGES, 0)
        research_weight = w.get(Classifier.AUDIENCE_RESEARCH, 0)

        total_adult_weight = adult_weight + adults_only_weight
        total_weight = sum(w.values())

        audience = default_audience

        # A book will be classified as a young adult or childrens'
        # book when the weight of that audience is more than twice the
        # combined weight of the 'adult' and 'adults only' audiences.
        # If that combined weight is zero, then any amount of evidence
        # is sufficient.
        threshold = total_adult_weight * 2

        # If both the 'children' weight and the 'YA' weight pass the
        # threshold, we go with the one that weighs more.
        # If the 'children' weight passes the threshold on its own
        # we go with 'children'.
        total_juvenile_weight = children_weight + ya_weight
        if (research_weight > (total_adult_weight + all_ages_weight) and
            research_weight > (total_juvenile_weight + all_ages_weight) and
            research_weight > threshold):
            audience = Classifier.AUDIENCE_RESEARCH
        elif (all_ages_weight > total_adult_weight and
            all_ages_weight > total_juvenile_weight):
            audience = Classifier.AUDIENCE_ALL_AGES
        elif children_weight > threshold and children_weight > ya_weight:
            audience = Classifier.AUDIENCE_CHILDREN
        elif ya_weight > threshold:
            audience = Classifier.AUDIENCE_YOUNG_ADULT
        elif total_juvenile_weight > threshold:
            # Neither weight passes the threshold on its own, but
            # combined they do pass the threshold. Go with
            # 'Young Adult' to be safe.
            audience = Classifier.AUDIENCE_YOUNG_ADULT
        elif total_adult_weight > 0:
            audience = Classifier.AUDIENCE_ADULT

        # If the 'adults only' weight is more than 1/4 of the total adult
        # weight, classify as 'adults only' to be safe.
        #
        # TODO: This has not been calibrated.
        if (audience==Classifier.AUDIENCE_ADULT
            and adults_only_weight > total_adult_weight/4):
            audience = Classifier.AUDIENCE_ADULTS_ONLY

        return audience

    @classmethod
    def top_tier_values(self, counter):
        """Given a Counter mapping values to their frequency of occurance,
        return all values that are as common as the most common value.
        """
        top_frequency = None
        top_tier = set()
        for age, freq in counter.most_common():
            if not top_frequency:
                top_frequency = freq
            if freq != top_frequency:
                # We've run out of candidates
                break
            else:
                # This candidate occurs with the maximum frequency.
                top_tier.add(age)
        return top_tier

    def target_age(self, audience):
        """Derive a target age from the gathered data."""
        if audience not in (
                Classifier.AUDIENCE_CHILDREN, Classifier.AUDIENCE_YOUNG_ADULT
        ):
            # This is not a children's or YA book. Assertions about
            # target age are irrelevant and the default value rules.
            return Classifier.default_target_age_for_audience(audience)

        # Only consider the most reliable classifications.

        # Try to reach consensus on the lower and upper bounds of the
        # age range.
        if self.debug:
            if self.target_age_lower_weights:
                self.log.debug("Possible target age minima:")
                for k, v in self.target_age_lower_weights.most_common():
                    self.log.debug(" %s: %s", v, k)
            if self.target_age_upper_weights:
                self.log.debug("Possible target age maxima:")
                for k, v in self.target_age_upper_weights.most_common():
                    self.log.debug(" %s: %s", v, k)

        target_age_min = None
        target_age_max = None
        if self.target_age_lower_weights:
            # Find the youngest age in the top tier of values.
            candidates = self.top_tier_values(self.target_age_lower_weights)
            target_age_min = min(candidates)

        if self.target_age_upper_weights:
            # Find the oldest age in the top tier of values.
            candidates = self.top_tier_values(self.target_age_upper_weights)
            target_age_max = max(candidates)

        if not target_age_min and not target_age_max:
            # We found no opinions about target age. Use the default.
            return Classifier.default_target_age_for_audience(audience)

        if target_age_min is None:
            target_age_min = target_age_max

        if target_age_max is None:
            target_age_max = target_age_min

        # Err on the side of setting the minimum age too high.
        if target_age_min > target_age_max:
            target_age_max = target_age_min
        return Classifier.range_tuple(target_age_min, target_age_max)

    def genres(self, fiction, cutoff=0.15):
        """Consolidate genres and apply a low-pass filter."""
        # Remove any genres whose fiction status is inconsistent with the
        # (independently determined) fiction status of the book.
        #
        # It doesn't matter if a book is classified as 'science
        # fiction' 100 times; if we know it's nonfiction, it can't be
        # science fiction. (It's probably a history of science fiction
        # or something.)
        genres = dict(self.genre_weights)
        if not genres:
            # We have absolutely no idea, and it would be
            # irresponsible to guess.
            return {}

        for genre in list(genres.keys()):
            # If we have a fiction determination, that lets us eliminate
            # possible genres that conflict with that determination.
            #
            # TODO: If we don't have a fiction determination, the
            # genres we end up with may help us make one.
            if fiction is not None and (genre.default_fiction != fiction):
                del genres[genre]

        # Consolidate parent genres into their heaviest subgenre.
        genres = self.consolidate_genre_weights(genres)
        total_weight = float(sum(genres.values()))

        # Strip out the stragglers.
        for g, score in list(genres.items()):
            affinity = score / total_weight
            if affinity < cutoff:
                total_weight -= score
                del genres[g]
        return genres

    def weigh_genre(self, genre_data, weight):
        """A helper method that ensure we always use database Genre
        objects, not GenreData objects, when weighting genres.
        """
        try:
            from ..model import Genre
        except ValueError:
            from model import Genre
        genre, ignore = Genre.lookup(self._db, genre_data.name)
        self.genre_weights[genre] += weight

    @classmethod
    def consolidate_genre_weights(
            cls, weights, subgenre_swallows_parent_at=0.03
    ):
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

# Make a dictionary of classification schemes to classifiers.

Classifier.classifiers[Classifier.FREEFORM_AUDIENCE] = FreeformAudienceClassifier
Classifier.classifiers[Classifier.AXIS_360_AUDIENCE] = AgeOrGradeClassifier

# Finally, import classifiers described in submodules.
from age import (
    GradeLevelClassifier,
    InterestLevelClassifier,
    AgeClassifier,
)
from bisac import BISACClassifier
from rbdigital import (
    RBDigitalAudienceClassifier,
    RBDigitalSubjectClassifier,
)
from ddc import DeweyDecimalClassifier
from lcc import LCCClassifier
from gutenberg import GutenbergBookshelfClassifier
from bic import BICClassifier
from simplified import (
    SimplifiedFictionClassifier,
    SimplifiedGenreClassifier,
)
from overdrive import OverdriveClassifier
from keyword import (
    KeywordBasedClassifier,
    LCSHClassifier,
    FASTClassifier,
    TAGClassifier,
    Eg,
)
