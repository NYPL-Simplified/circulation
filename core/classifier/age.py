import re

from . import Classifier

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
        'seventh' : 12,
        '8' : 13,
        'eighth' : 13,

        # YA
        '9' : 14,
        'ninth' : 14,
        '10' : 15,
        'tenth': 15,
        '11' : 16,
        'eleventh' : 17,
        '12': 17,
        'twelfth': 17,
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
                    if (young is not None and old is not None and young > old):
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

Classifier.classifiers[Classifier.AGE_RANGE] = AgeClassifier
Classifier.classifiers[Classifier.GRADE_LEVEL] = GradeLevelClassifier
Classifier.classifiers[Classifier.INTEREST_LEVEL] = InterestLevelClassifier
