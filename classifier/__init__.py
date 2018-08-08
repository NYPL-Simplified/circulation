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

    # A book for a child younger than 14 is a children's book.
    # A book for a child 14 or older is a young adult book.
    YOUNG_ADULT_AGE_CUTOFF = 14

    ADULT_AGE_CUTOFF = 18

    AUDIENCES_JUVENILE = [AUDIENCE_CHILDREN, AUDIENCE_YOUNG_ADULT]
    AUDIENCES_ADULT = [AUDIENCE_ADULT, AUDIENCE_ADULTS_ONLY]
    AUDIENCES = set([AUDIENCE_ADULT, AUDIENCE_ADULTS_ONLY, AUDIENCE_YOUNG_ADULT,
                     AUDIENCE_CHILDREN])

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
            return None
        if not lower:
            if upper > 18:
                # e.g. "up to 20 years", though that doesn't
                # make much sense.
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
            return cls.range_tuple(None, None)
        m = cls.age_re.search(identifier)
        if not m:
            return cls.range_tuple(None, None)
        young, old = map(int, m.groups())
        if young > old:
            young, old = old, young
        return cls.range_tuple(young, old)


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
        Periodicals : range(50, 60) + [105, 405, 505, 605, 705, 805, 905],
        Philosophy : range(160, 200),
        Photography : [771, 772, 773, 775, 778, 779],
        Poetry : [811, 821, 831, 841, 851, 861, 871, 874, 881, 884],
        Political_Science : range(320, 330) + range(351, 355),
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
        if not identifier:
            return identifier
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

        # For our purposes, Dewey Decimal numbers are identifiers
        # without names.
        return identifier, None

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

        if isinstance(identifier, basestring) and identifier=='FIC':
            # FIC is used for all types of fiction.
            return None
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
        if not identifier:
            return identifier
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

def match_kw(*l):
    """Turn a list of strings into a function which uses a regular expression
    to match any of those strings, so long as there's a word boundary on both ends.
    The function will match all the strings by default, or can exclude the strings
    that are examples of the classification.
    """
    def match_term(term, exclude_examples=False):
        if not l:
            return None
        if exclude_examples:
            keywords = [keyword for keyword in l if not isinstance(keyword, Eg)]
        else:
            keywords = [str(keyword) for keyword in l]

        if not keywords:
            return None
        any_keyword = "|".join(keywords)
        with_boundaries = r'\b(%s)\b' % any_keyword
        return re.compile(with_boundaries, re.I).search(term)


    # This is a dictionary so it can be used as a class variable
    return {"search": match_term}

class Eg(object):
    """Mark this string as an example of a classification, rather than
    an exact identifier for that classification. For example, basketball
    is an an example of a sport, but athletics is an identifier for the sports
    classification.
    """

    def __init__(self, term):
        self.term = term

    def __str__(self):
        return self.term

class KeywordBasedClassifier(AgeOrGradeClassifier):

    """Classify a book based on keywords."""

    # We have to handle these first because otherwise '\bfiction\b'
    # will match it.
    LEVEL_1_NONFICTION_INDICATORS = match_kw(
        "non-fiction", "non fiction"
    )

    LEVEL_2_FICTION_INDICATORS = match_kw(
        "fiction", Eg("stories"), Eg("tales"), Eg("literature"),
        Eg("bildungsromans"), "fictitious",
    )
    LEVEL_2_NONFICTION_INDICATORS = match_kw(
        Eg("history"), Eg("biography"), Eg("histories"),
        Eg("biographies"), Eg("autobiography"), Eg("autobiographies"),
        "nonfiction", Eg("essays"), Eg("letters"), Eg("true story"),
        Eg("personal memoirs"))
    JUVENILE_INDICATORS = match_kw(
        "for children", "children's", "juvenile",
        Eg("nursery rhymes"), Eg("9-12"))
    YOUNG_ADULT_INDICATORS = match_kw(
        "young adult",
        "ya",
        "12-Up",
        "teenage .*fiction",
        "teens .*fiction",
        "teen books",
        Eg("teenage romance"),
    )

    # Children's books don't generally deal with romance, so although
    # "Juvenile Fiction" generally refers to children's fiction,
    # "Juvenile Fiction / Love & Romance" is almost certainly YA.
    JUVENILE_TERMS_THAT_IMPLY_YOUNG_ADULT = set([
        "love & romance",
        "romance",
        "romantic",
    ])

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
            Eg("sea stories"),
            Eg("war stories"),
            Eg("men's adventure"),
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
            "arts",
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
            Eg("beer"),
            "alcoholic beverages",
            Eg("wine"),
            Eg("wine & spirits"),
            "spirits & cocktails",
        ),

               Biography_Memoir : match_kw(
                   "autobiographies",
                   "autobiography",
                   "biographies",
                   "biography",
                   "biographical",
                   "personal memoirs",
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
                   Eg("nonprofit"),
               ),

               Christianity : match_kw(
                   Eg("schema:creativework:bible"),
                   Eg("baptist"),
                   Eg("bible"),
                   Eg("sermons"),
                   Eg("devotional"),
                   Eg("theological"),
                   Eg("theology"),
                   Eg('biblical'),
                   "christian",
                   "christianity",
                   Eg("catholic"),
                   Eg("protestant"),
                   Eg("catholicism"),
                   Eg("protestantism"),
                   Eg("church"),
                   Eg("christmas & advent"),
               ),

               Civil_War_History: match_kw(
                   "american civil war",
                   "1861-1865",
                   "civil war period",
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
                   Eg("data"),
                   Eg("database"),
                   Eg("hardware"),
                   Eg("software"),
                   Eg("software development"),
                   Eg("information technology"),
                   Eg("web"),
                   Eg("world wide web"),
               ),

               Contemporary_Romance: match_kw(
                   "contemporary romance",
                   "romance--contemporary",
                   "romance / contemporary",
                   "romance - contemporary",
               ),

               Cooking : match_kw(
                   Eg("non-alcoholic"),
                   Eg("baking"),
                   "cookbook",
                   "cooking",
                   "food",
                   Eg("health & healing"),
                   "home economics",
                   "cuisine",
               ),

               Crafts_Hobbies: match_kw(
                   "arts & crafts",
                   "arts, crafts",
                   Eg("beadwork"),
                   Eg("candle crafts"),
                   Eg("candle making"),
                   Eg("carving"),
                   Eg("ceramics"),
                   "crafts & hobbies",
                   "crafts",
                   Eg("crochet"),
                   Eg("crocheting"),
                   Eg("cross-stitch"),
                   "decorative arts",
                   Eg("flower arranging"),
                   "folkcrafts",
                   "handicrafts",
                   "hobbies",
                   "hobby",
                   "hobbyist",
                   "hobbyists",
                   Eg("jewelry"),
                   Eg("knitting"),
                   Eg("metal work"),
                   Eg("needlework"),
                   Eg("origami"),
                   Eg("paper crafts"),
                   Eg("pottery"),
                   Eg("quilting"),
                   Eg("quilts"),
                   Eg("scrapbooking"),
                   Eg("sewing"),
                   Eg("soap making"),
                   Eg("stamping"),
                   Eg("stenciling"),
                   Eg("textile crafts"),
                   Eg("toymaking"),
                   Eg("weaving"),
                   Eg("woodwork"),
               ),

               Design: match_kw(
                   "design",
                   "designer",
                   "designers",
                   Eg("graphic design"),
                   Eg("typography")
               ),

               Dictionaries: match_kw(
                   "dictionaries",
                   "dictionary",
               ),

               Drama : match_kw(
                   Eg("comedies"),
                   "drama",
                   "dramatist",
                   "dramatists",
                   Eg("operas"),
                   Eg("plays"),
                   Eg("shakespeare"),
                   Eg("tragedies"),
                   Eg("tragedy"),
               ),

               Economics: match_kw(
                   Eg("banking"),
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
                   Eg("principals"),
                   "teacher",
                   "teachers",
                   "teaching",
                   #"schools",
                   #"high school",
                   "schooling",
                   #"student",
                   #"students",
                   #"college",
                   Eg("university"),
                   Eg("universities"),
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
            Eg("france.*history"),
            Eg("history.*france"),
            Eg("england.*history"),
            Eg("history.*england"),
            Eg("ireland.*history"),
            Eg("history.*ireland"),
            Eg("germany.*history"),
            Eg("history.*germany"),
            # etc. etc. etc.
        ),

               Family_Relationships: match_kw(
                   "family & relationships",
                   "relationships",
                   "family relationships",
                   "human sexuality",
                   "sexuality",
               ),

               Fantasy : match_kw(
                   "fantasy",
                   Eg("magic"),
                   Eg("wizards"),
                   Eg("fairies"),
                   Eg("witches"),
                   Eg("dragons"),
                   Eg("sorcery"),
                   Eg("witchcraft"),
                   Eg("wizardry"),
                   Eg("unicorns"),
               ),

               Fashion: match_kw(
                   "fashion",
                   "fashion design",
                   "fashion designers",
               ),

               Film_TV: match_kw(
                   Eg("director"),
                   Eg("directors"),
                   "film",
                   "films",
                   "movies",
                   "movie",
                   "motion picture",
                   "motion pictures",
                   "moviemaker",
                   "moviemakers",
                   Eg("producer"),
                   Eg("producers"),
                   "television",
                   "tv",
                   "video",
               ),

               Foreign_Language_Study: match_kw(
                   Eg("english as a foreign language"),
                   Eg("english as a second language"),
                   Eg("esl"),
                   "foreign language study",
                   Eg("multi-language dictionaries"),
               ),

               Games : match_kw(
                   "games",
                   Eg("video games"),
                   "gaming",
                   Eg("gambling"),
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
                   "graphic novel",
                   "graphic novels",

                   # Formerly in 'Superhero'
                   Eg("superhero"),
                   Eg("superheroes"),

                   # Formerly in 'Manga'
                   Eg("japanese comic books"),
                   Eg("japanese comics"),
                   Eg("manga"),
                   Eg("yaoi"),

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
                   "^historical$",
               ),

               Historical_Romance: match_kw(
                   "historical romance",
                   Eg("regency romance"),
                   Eg("romance.*regency"),
               ),

               History : match_kw(
                   "histories",
                   "history",
                   "historiography",
                   "historical period",
                   Eg("pre-confederation"),
               ),

               Horror : match_kw(
                   "horror",
                   Eg("occult"),
                   Eg("ghost"),
                   Eg("ghost stories"),
                   Eg("vampires"),
                   Eg("paranormal fiction"),
                   Eg("occult fiction"),
                   Eg("supernatural"),
                   "scary",
               ),

               House_Home: match_kw(
                   "house and home",
                   "house & home",
                   Eg("remodeling"),
                   Eg("renovation"),
                   Eg("caretaking"),
                   Eg("interior decorating"),
               ),

        Humorous_Fiction : match_kw(
            "comedy",
            "funny",
            "humor",
            "humorous",
            "humourous",
            "humour",
            Eg("satire"),
            "wit",
        ),
        Humorous_Nonfiction : match_kw(
            "comedy",
            "funny",
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
            'islam', 'islamic', 'muslim', 'muslims', Eg('halal'),
            'islamic studies',
        ),

               Judaism: match_kw(
                   'judaism', 'jewish', Eg('kosher'), 'jews',
                   'jewish studies',
               ),

               LGBTQ_Fiction: match_kw(
                   'lgbt',
                   'lgbtq',
                   Eg('lesbian'),
                   Eg('lesbians'),
                   'gay',
                   Eg('bisexual'),
                   Eg('transgender'),
                   Eg('transsexual'),
                   Eg('transsexuals'),
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
                   Eg("algebra"),
                   Eg("arithmetic"),
                   Eg("calculus"),
                   Eg("chaos theory"),
                   Eg("game theory"),
                   Eg("geometry"),
                   Eg("group theory"),
                   Eg("logic"),
                   "math",
                   "mathematical",
                   "mathematician",
                   "mathematicians",
                   "mathematics",
                   Eg("probability"),
                   Eg("statistical"),
                   Eg("statistics"),
                   Eg("trigonometry"),
               ),

               Medical : match_kw(
                   Eg("anatomy"),
                   Eg("disease"),
                   Eg("diseases"),
                   Eg("disorders"),
                   Eg("epidemiology"),
                   Eg("illness"),
                   Eg("illnesses"),
                   "medical",
                   "medicine",
                   Eg("neuroscience"),
                   Eg("ophthalmology"),
                   Eg("physiology"),
                   Eg("vaccines"),
                   Eg("virus"),
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
                   Eg("1914-1918"),
                   Eg("1939-1945"),
                   Eg("world war"),
               ),

               Modern_History: match_kw(
                   Eg("1900 - 1999"),
                   Eg("2000-2099"),
                   "modern history",
                   "history, modern",
                   "history (modern)",
                   "history--modern",
                   Eg("history.*20th century"),
                   Eg("history.*21st century"),
               ),

               # This is SF movie tie-ins, not movies & gaming per se.
        # This one is difficult because it takes effect if book
        # has subject "media tie-in" *and* "science fiction" or
        # "fantasy"
        Media_Tie_in_SF: match_kw(
            "science fiction & fantasy gaming",
            Eg("star trek"),
            Eg("star wars"),
            Eg("jedi"),
        ),

               Music: match_kw(
                   "music",
                   "musician",
                   "musicians",
                   "musical",
                   Eg("genres & styles"),
                   Eg("blues"),
                   Eg("jazz"),
                   Eg("rap"),
                   Eg("hip-hop"),
                   Eg("rock.*roll"),
                   Eg("rock music"),
                   Eg("punk rock"),
               ),

               Mystery : match_kw(
                   Eg("crime"),
                   Eg("detective"),
                   Eg("murder"),
                   "mystery",
                   "mysteries",
                   Eg("private investigators"),
                   Eg("holmes, sherlock"),
                   Eg("poirot, hercule"),
                   Eg("schema:person:holmes, sherlock"),
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
                   # "children" isn't here because the vast majority of
                   # "children" tags indicate books _for_ children.

                   # "family" isn't here because the vast majority
                   # of "family" tags deal with specific families, e.g.
                   # the Kennedys.

                   "parenting",
                   "parent",
                   "parents",
                   Eg("motherhood"),
                   Eg("fatherhood"),
               ),

               Parenting_Family: match_kw(
                   # Pure top-level category
               ),

               Performing_Arts: match_kw(
                   "theatre",
                   "theatrical",
                   "performing arts",
                   "entertainers",
                   Eg("farce"),
                   Eg("tragicomedy"),
               ),

               Periodicals : match_kw(
                   "periodicals",
                   "periodical",
               ),

               Personal_Finance_Investing: match_kw(
                   "personal finance",
                   "financial planning",
                   "investing",
                   Eg("retirement planning"),
                   "money management",
               ),

               Pets: match_kw(
                   "pets",
                   Eg("dogs"),
                   Eg("cats"),
               ),

               Philosophy : match_kw(
                   "philosophy",
                   "philosophical",
                   "philosopher",
                   "philosophers",
                   Eg("epistemology"),
                   Eg("metaphysics"),
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
                   Eg("sonnet"),
                   Eg("sonnets"),
               ),

               Political_Science : match_kw(
                   Eg("american government"),
                   Eg("anarchism"),
                   Eg("censorship"),
                   Eg("citizenship"),
                   Eg("civics"),
                   Eg("communism"),
                   Eg("corruption"),
                   Eg("corrupt practices"),
                   Eg("democracy"),
                   Eg("geopolitics"),
                   "government",
                   Eg("human rights"),
                   Eg("international relations"),
                   Eg("political economy"),
                   "political ideologies",
                   "political process",
                   "political science",
                   Eg("public affairs"),
                   Eg("public policy"),
                   "politics",
                   "political",
                   Eg("current events"),
               ),

               Psychology: match_kw(
                   "psychology",
                   Eg("psychiatry"),
                   "psychological aspects",
                   Eg("psychiatric"),
                   Eg("psychoanalysis"),
               ),

               Real_Estate: match_kw(
                   "real estate",
               ),

               Reference_Study_Aids : match_kw(
                   Eg("catalogs"),
                   Eg("handbooks"),
                   Eg("manuals"),
                   Eg("reference"),

                   # Formerly in 'Encyclopedias'
                   Eg("encyclopaedias"),
                   Eg("encyclopaedia"),
                   Eg("encyclopedias"),
                   Eg("encyclopedia"),

                   # Formerly in 'Language Arts & Disciplines'
                   Eg("alphabets"),
                   Eg("communication studies"),
                   Eg("composition"),
                   Eg("creative writing"),
                   Eg("grammar"),
                   Eg("handwriting"),
                   Eg("information sciences"),
                   Eg("journalism"),
                   Eg("library & information sciences"),
                   Eg("linguistics"),
                   Eg("literacy"),
                   Eg("public speaking"),
                   Eg("rhetoric"),
                   Eg("sign language"),
                   Eg("speech"),
                   Eg("spelling"),
                   Eg("style manuals"),
                   Eg("syntax"),
                   Eg("vocabulary"),
                   Eg("writing systems"),
               ),

               Religion_Spirituality : match_kw(
                   "religion",
                   "religious",
                   Eg("taoism"),
                   Eg("taoist"),
                   Eg("confucianism"),
                   Eg("inspirational nonfiction"),
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
                   Eg("aeronautics"),
                   Eg("astronomy"),
                   Eg("biology"),
                   Eg("biophysics"),
                   Eg("biochemistry"),
                   Eg("botany"),
                   Eg("chemistry"),
                   Eg("earth sciences"),
                   Eg("ecology"),
                   Eg("entomology"),
                   Eg("evolution"),
                   Eg("geology"),
                   Eg("genetics"),
                   Eg("genetic engineering"),
                   Eg("genomics"),
                   Eg("ichthyology"),
                   Eg("herpetology"),
                   Eg("life sciences"),
                   Eg("microbiology"),
                   Eg("microscopy"),
                   Eg("mycology"),
                   Eg("ornithology"),
                   Eg("natural history"),
                   Eg("natural history"),
                   Eg("physics"),
                   "science",
                   "scientist",
                   "scientists",
                   Eg("zoology"),
                   Eg("virology"),
                   Eg("cytology"),
               ),

               Science_Fiction : match_kw(
                   "speculative fiction",
                   "sci-fi",
                   "sci fi",
                   Eg("time travel"),
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
                   "fables",
                   "folklore",
                   "folktales",
                   "folk tales",
                   "myth",
                   "legends",
               ),

                Short_Stories: match_kw(
                    "short stories",
                    Eg("literary collections"),
                ),

               Social_Sciences: match_kw(
                   Eg("anthropology"),
                   Eg("archaeology"),
                   Eg("sociology"),
                   Eg("ethnic studies"),
                   Eg("feminism & feminist theory"),
                   Eg("gender studies"),
                   Eg("media studies"),
                   Eg("minority studies"),
                   Eg("men's studies"),
                   Eg("regional studies"),
                   Eg("women's studies"),
                   Eg("demography"),
                   Eg('lesbian studies'),
                   Eg('gay studies'),
                   Eg("black studies"),
                   Eg("african-american studies"),
                   Eg("customs & traditions"),
                   Eg("criminology"),
               ),

               Sports: match_kw(
                   # Ton of specific sports here since 'players'
                   # doesn't work. TODO: Why? I don't remember.
                   "sports",
                   Eg("baseball"),
                   Eg("football"),
                   Eg("hockey"),
                   Eg("soccer"),
                   Eg("skating"),
               ),

               Study_Aids: match_kw(
                   Eg("act"),
                   Eg("advanced placement"),
                   Eg("bar exam"),
                   Eg("clep"),
                   Eg("college entrance"),
                   Eg("college guides"),
                   Eg("financial aid"),
                   Eg("certification"),
                   Eg("ged"),
                   Eg("gmat"),
                   Eg("gre"),
                   Eg("lsat"),
                   Eg("mat"),
                   Eg("mcat"),
                   Eg("nmsqt"),
                   Eg("nte"),
                   Eg("psat"),
                   Eg("sat"),
                   "school guides",
                   "study guide",
                   "study guides",
                   "study aids",
                   Eg("toefl"),
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
                   Eg("engineering"),
                   Eg("bioengineering"),
                   Eg("mechanics"),

                   # Formerly in 'Transportation'
                   Eg("transportation"),
                   Eg("railroads"),
                   Eg("trains"),
                   Eg("automotive"),
                   Eg("ships & shipbuilding"),
                   Eg("cars & trucks"),
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
                   Eg("discovery"),
                   "exploration",
                   "travel",
                   "travels.*voyages",
                   "voyage.*travels",
                   "voyages",
                   "travelers",
                   "description.*travel",
               ),

               United_States_History: match_kw(
                   "united states history",
                   "u.s. history",
                   Eg("american revolution"),
                   Eg("1775-1783"),
                   Eg("revolutionary period"),
               ),

               Urban_Fantasy: match_kw(
                   "urban fantasy",
                   "fantasy.*urban",
               ),

               Urban_Fiction: match_kw(
                   "urban fiction",
                   Eg("fiction.*african american.*urban"),
               ),

               Vegetarian_Vegan: match_kw(
                   "vegetarian",
                   Eg("vegan"),
                   Eg("veganism"),
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
                   "womens fiction",
                   "women's fiction",
               ),

               World_History: match_kw(
                   "world history",
                   "history[^a-z]*world",
               ),
    }

    LEVEL_2_KEYWORDS = {
        Reference_Study_Aids : match_kw(
            # Formerly in 'Language Arts & Disciplines'
            Eg("language arts & disciplines"),
            Eg("language arts and disciplines"),
            Eg("language arts"),
        ),
        Design : match_kw(
            "arts and crafts movement",
        ),
        Drama : match_kw(
            Eg("opera"),
        ),

        Erotica : match_kw(
            Eg("erotic poetry"),
            Eg("gay erotica"),
            Eg("lesbian erotica"),
            Eg("erotic photography"),
        ),

        Games : match_kw(
            Eg("games.*fantasy")
        ),

        Historical_Fiction : match_kw(
            Eg("arthurian romance.*"), # This is "romance" in the old
                                       # sense of a story.
        ),

        Literary_Criticism : match_kw(
            Eg("literary history"), # Not History
            Eg("romance language"), # Not Romance
        ),

        Media_Tie_in_SF : match_kw(
            'tv, movie, video game adaptations' # Not Film & TV
        ),

        # We need to match these first so that the 'military'/'warfare'
        # part doesn't match Military History.
        Military_SF: match_kw(
            "science fiction.*military",
            "military.*science fiction",
            Eg("space warfare"),            # Thankfully
            Eg("interstellar warfare"),
        ),
        Military_Thriller: match_kw(
            "military thrillers",
            "thrillers.*military",
        ),
        Pets : match_kw(
            "human-animal relationships",
        ),
        Political_Science : match_kw(
            Eg("health care reform"),
        ),

        # Stop the 'religious' from matching Religion/Spirituality.
        Religious_Fiction: match_kw(
            Eg("christian fiction"),
            Eg("inspirational fiction"),
            Eg("fiction.*christian"),
            "religious fiction",
            "fiction.*religious",
            Eg("Oriental religions and wisdom")
        ),

        Romantic_Suspense : match_kw(
            "romantic.*suspense",
            "suspense.*romance",
            "romance.*suspense",
            "romantic.*thriller",
            "romance.*thriller",
            "thriller.*romance",
        ),

        # Stop from showing up as 'science'
        Social_Sciences : match_kw(
            "social sciences",
            "social science",
            "human science",
        ),

        Science_Fiction : match_kw(
            "science fiction",
            "science fiction.*general",
        ),

        Supernatural_Thriller: match_kw(
            "thriller.*supernatural",
            "supernatural.*thriller",
        ),

        # Stop from going into Mystery due to 'crime'
        True_Crime: match_kw(
            "true crime",
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
    def is_fiction(cls, identifier, name, exclude_examples=False):
        if not name:
            return None
        if (cls.LEVEL_1_NONFICTION_INDICATORS["search"](name, exclude_examples)):
            return False
        if (cls.LEVEL_2_FICTION_INDICATORS["search"](name, exclude_examples)):
            return True
        if (cls.LEVEL_2_NONFICTION_INDICATORS["search"](name, exclude_examples)):
            return False
        return None

    @classmethod
    def audience(cls, identifier, name, exclude_examples=False):
        if name is None:
            return None
        if cls.YOUNG_ADULT_INDICATORS["search"](name, exclude_examples):
            use = cls.AUDIENCE_YOUNG_ADULT
        elif cls.JUVENILE_INDICATORS["search"](name, exclude_examples):
            use = cls.AUDIENCE_CHILDREN
        else:
            return None

        if use == cls.AUDIENCE_CHILDREN:
            for i in cls.JUVENILE_TERMS_THAT_IMPLY_YOUNG_ADULT:
                if i in name:
                    use = cls.AUDIENCE_YOUNG_ADULT

        # It may be for kids, or it may be about kids, e.g. "juvenile
        # delinquency".
        for i in cls.JUVENILE_BLACKLIST:
            if i in name:
                return None
        return use

    @classmethod
    def audience_match(cls, query):
        audience = None
        audience_words = None
        audience = cls.audience(None, query, exclude_examples=True)
        if audience:
            for audience_keywords in [cls.JUVENILE_INDICATORS, cls.YOUNG_ADULT_INDICATORS]:
                match = audience_keywords["search"](query, exclude_examples=True)
                if match:
                    audience_words = match.group()
                    break
        return (audience, audience_words)

    @classmethod
    def genre(cls, identifier, name, fiction=None, audience=None, exclude_examples=False):
        matches = Counter()
        match_against = [name]
        for l in [cls.LEVEL_3_KEYWORDS, cls.LEVEL_2_KEYWORDS, cls.CATCHALL_KEYWORDS]:
            for genre, keywords in l.items():
                if genre and fiction is not None and genre.is_fiction != fiction:
                    continue
                if (genre and audience and genre.audience_restriction
                    and audience not in genre.audience_restriction):
                    continue
                if keywords and keywords["search"](name, exclude_examples):
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
    def genre_match(cls, query):
        genre = None
        genre_words = None
        genre = cls.genre(None, query, exclude_examples=True)
        if genre:
            for kwlist in [cls.LEVEL_3_KEYWORDS, cls.LEVEL_2_KEYWORDS, cls.CATCHALL_KEYWORDS]:
                if genre in kwlist.keys():
                    genre_keywords = kwlist[genre]
                    match = genre_keywords["search"](query, exclude_examples=True)
                    if match:
                        genre_words = match.group()
                        break
        return (genre, genre_words)


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

class FreeformAudienceClassifier(AgeOrGradeClassifier):
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
        return AgeOrGradeClassifier.audience(identifier, name)

    @classmethod
    def target_age(cls, identifier, name):
        if identifier == 'beginning reader':
            return cls.range_tuple(5,8)
        if identifier == 'pre-adolescent':
            return cls.range_tuple(9, 12)
        if identifier == 'early adolescents':
            return cls.range_tuple(13, 15)

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
        if children_weight > threshold and children_weight > ya_weight:
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

class BICClassifier(Classifier):
    # These prefixes came from from http://editeur.dyndns.org/bic_categories

    LEVEL_1_PREFIXES = {
        Art_Design: 'A',
        Biography_Memoir: 'B',
        Foreign_Language_Study: 'C',
        Literary_Criticism: 'D',
        Reference_Study_Aids: 'G',
        Social_Sciences: 'J',
        Personal_Finance_Business: 'K',
        Law: 'L',
        Medical: 'M',
        Science_Technology: 'P',
        Technology: 'T',
        Computers: 'U',
    }

    LEVEL_2_PREFIXES = {
        Art_History: 'AC',
        Photography: 'AJ',
        Design: 'AK',
        Architecture: 'AM',
        Film_TV: 'AP',
        Performing_Arts: 'AS',
        Music: 'AV',
        Poetry: 'DC',
        Drama: 'DD',
        Classics: 'FC',
        Mystery: 'FF',
        Suspense_Thriller: 'FH',
        Adventure: 'FJ',
        Horror: 'FK',
        Science_Fiction: 'FL',
        Fantasy: 'FM',
        Erotica: 'FP',
        Romance: 'FR',
        Historical_Fiction: 'FV',
        Religious_Fiction: 'FW',
        Comics_Graphic_Novels: 'FX',
        History: 'HB',
        Philosophy: 'HP',
        Religion_Spirituality: 'HR',
        Psychology: 'JM',
        Education: 'JN',
        Political_Science: 'JP',
        Economics: 'KC',
        Business: 'KJ',
        Mathematics: 'PB',
        Science: 'PD',
        Self_Help: 'VS',
        Body_Mind_Spirit: 'VX',
        Food_Health: 'WB',
        Antiques_Collectibles: 'WC',
        Crafts_Hobbies: 'WF',
        Humorous_Nonfiction: 'WH',
        House_Home: 'WK',
        Gardening: 'WM',
        Nature: 'WN',
        Sports: 'WS',
        Travel: 'WT',
    }

    LEVEL_3_PREFIXES = {
        Historical_Mystery: 'FFH',
        Espionage: 'FHD',
        Westerns: 'FJW',
        Space_Opera: 'FLS',
        Historical_Romance: 'FRH',
        Short_Stories: 'FYB',
        World_History: 'HBG',
        Military_History: 'HBW',
        Christianity: 'HRC',
        Buddhism: 'HRE',
        Hinduism: 'HRG',
        Islam: 'HRH',
        Judaism: 'HRJ',
        Fashion: 'WJF',
        Poetry: 'YDP',
        Adventure: 'YFC',
        Horror: 'YFD',
        Science_Fiction: 'YFG',
        Fantasy: 'YFH',
        Romance: 'YFM',
        Humorous_Fiction: 'YFQ',
        Historical_Fiction: 'YFT',
        Comics_Graphic_Novels: 'YFW',
        Art: 'YNA',
        Music: 'YNC',
        Performing_Arts: 'YND',
        Film_TV: 'YNF',
        History: 'YNH',
        Nature: 'YNN',
        Religion_Spirituality: 'YNR',
        Science_Technology: 'YNT',
        Humorous_Nonfiction: 'YNU',
        Sports: 'YNW',
    }

    LEVEL_4_PREFIXES = {
        European_History: 'HBJD',
        Asian_History: 'HBJF',
        African_History: 'HBJH',
        Ancient_History: 'HBLA',
        Modern_History: 'HBLL',
        Drama: 'YNDS',
        Comics_Graphic_Novels: 'YNUC',
    }

    PREFIX_LISTS = [LEVEL_4_PREFIXES, LEVEL_3_PREFIXES, LEVEL_2_PREFIXES, LEVEL_1_PREFIXES]

    @classmethod
    def is_fiction(cls, identifier, name):
        if identifier.startswith('f') or identifier.startswith('yf'):
            return True
        return False

    @classmethod
    def audience(cls, identifier, name):
        # BIC doesn't distinguish children's and YA.
        # Classify it as YA to be safe.
        if identifier.startswith("y"):
            return cls.AUDIENCE_YOUNG_ADULT
        return cls.AUDIENCE_ADULT

    @classmethod
    def genre(cls, identifier, name, fiction=None, audience=None):
        for prefixes in cls.PREFIX_LISTS:
            for l, v in prefixes.items():
                if identifier.startswith(v.lower()):
                    return l
        return None

class SimplifiedGenreClassifier(Classifier):

    NONE = NO_VALUE

    @classmethod
    def scrub_identifier(cls, identifier):
        # If the identifier is a URI identifying a Simplified genre,
        # strip off the first part of the URI to get the genre name.
        if not identifier:
            return identifier
        if identifier.startswith(cls.SIMPLIFIED_GENRE):
            identifier = identifier[len(cls.SIMPLIFIED_GENRE):]
            identifier = urllib.unquote(identifier)
        return Lowercased(identifier)

    @classmethod
    def genre(cls, identifier, name, fiction=None, audience=None):
        if fiction == True:
            all_genres = fiction_genres
        elif fiction == False:
            all_genres = nonfiction_genres
        else:
            all_genres = fiction_genres + nonfiction_genres
        return cls._genre_by_name(identifier.original, all_genres)

    @classmethod
    def is_fiction(cls, identifier, name):
        if not globals()["genres"].get(identifier.original):
            return None
        return globals()["genres"][identifier.original].is_fiction

    @classmethod
    def _genre_by_name(cls, name, genres):
        for genre in genres:
            if genre == name:
                return globals()["genres"][name]
            elif isinstance(genre, dict):
                if name == genre["name"] or name in genre.get("subgenres", []):
                    return globals()["genres"][name]
        return None


class SimplifiedFictionClassifier(Classifier):

    @classmethod
    def scrub_identifier(cls, identifier):
        # If the identifier is a URI identifying a Simplified genre,
        # strip off the first part of the URI to get the genre name.
        if not identifier:
            return identifier
        if identifier.startswith(cls.SIMPLIFIED_FICTION_STATUS):
            identifier = identifier[len(cls.SIMPLIFIED_FICTION_STATUS):]
            identifier = urllib.unquote(identifier)
        return Lowercased(identifier)

    @classmethod
    def is_fiction(cls, identifier, name):
        if identifier == "fiction":
            return True
        elif identifier == "nonfiction":
            return False
        else:
            return None


# Make a dictionary of classification schemes to classifiers.
Classifier.classifiers[Classifier.DDC] = DeweyDecimalClassifier
Classifier.classifiers[Classifier.LCC] = LCCClassifier
Classifier.classifiers[Classifier.FAST] = FASTClassifier
Classifier.classifiers[Classifier.LCSH] = LCSHClassifier
Classifier.classifiers[Classifier.TAG] = TAGClassifier
Classifier.classifiers[Classifier.BIC] = BICClassifier
Classifier.classifiers[Classifier.AGE_RANGE] = AgeClassifier
Classifier.classifiers[Classifier.GRADE_LEVEL] = GradeLevelClassifier
Classifier.classifiers[Classifier.FREEFORM_AUDIENCE] = FreeformAudienceClassifier
Classifier.classifiers[Classifier.GUTENBERG_BOOKSHELF] = GutenbergBookshelfClassifier
Classifier.classifiers[Classifier.INTEREST_LEVEL] = InterestLevelClassifier
Classifier.classifiers[Classifier.AXIS_360_AUDIENCE] = AgeOrGradeClassifier
Classifier.classifiers[Classifier.SIMPLIFIED_GENRE] = SimplifiedGenreClassifier
Classifier.classifiers[Classifier.SIMPLIFIED_FICTION_STATUS] = SimplifiedFictionClassifier

# Finally, import classifiers described in submodules.
from bisac import BISACClassifier
from rbdigital import (
    RBDigitalAudienceClassifier,
    RBDigitalSubjectClassifier,
)
from overdrive import OverdriveClassifier
