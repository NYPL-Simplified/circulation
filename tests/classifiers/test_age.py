from ...classifier import (
    Classifier,
    AgeOrGradeClassifier,
    LCSHClassifier as LCSH,
)

from ...classifier.age import (
    GradeLevelClassifier,
    InterestLevelClassifier,
    AgeClassifier,
)

class TestTargetAge(object):

    def test_range_tuple_swaps_mismatched_ages(self):
        """If for whatever reason a Classifier decides that something is from
        ages 6 to 5, the Classifier.range_tuple() method will automatically
        convert this to "ages 5 to 6".

        This sort of problem ought to be fixed inside the Classifier,
        but if it does happen, range_tuple() will stop it from causing
        downstream problems.
        """
        range1 = Classifier.range_tuple(5,6)
        range2 = Classifier.range_tuple(6,5)
        assert range2 == range1
        assert 5 == range2[0]
        assert 6 == range2[1]

        # If one of the target ages is None, it's left alone.
        r = Classifier.range_tuple(None,6)
        assert None == r[0]
        assert 6 == r[1]

        r = Classifier.range_tuple(18,None)
        assert 18 == r[0]
        assert None == r[1]


    def test_age_from_grade_classifier(self):
        def f(t):
            return GradeLevelClassifier.target_age(t, None)
        assert (
            Classifier.range_tuple(5,6) ==
            GradeLevelClassifier.target_age(None, "grades 0-1"))
        assert (4,7) == f("pk - 2")
        assert (5,7) == f("grades k-2")
        assert (6,6) == f("first grade")
        assert (6,6) == f("1st grade")
        assert (6,6) == f("grade 1")
        assert (7,7) == f("second grade")
        assert (7,7) == f("2nd grade")
        assert (8,8) == f("third grade")
        assert (9,9) == f("fourth grade")
        assert (10,10) == f("fifth grade")
        assert (11,11) == f("sixth grade")
        assert (12,12) == f("7th grade")
        assert (13,13) == f("grade 8")
        assert (14,14) == f("9th grade")
        assert (15,17) == f("grades 10-12")
        assert (6,6) == f("grades 00-01")
        assert (8,12) == f("grades 03-07")
        assert (8,12) == f("3-07")
        assert (8,10) == f("5 - 3")
        assert (17,17) == f("12th grade")

        # target_age() will assume that a number it sees is talking
        # about a grade level, unless require_explicit_grade_marker is
        # True.
        assert (14,17) == f("Children's Audio - 9-12")
        assert (7,9) == GradeLevelClassifier.target_age("2-4", None, False)
        assert (None,None) == GradeLevelClassifier.target_age("2-4", None, True)
        assert (None,None) == GradeLevelClassifier.target_age(
            "Children's Audio - 9-12", None, True)

        assert (None,None) == GradeLevelClassifier.target_age("grade 50", None)
        assert (None,None) == GradeLevelClassifier.target_age("road grades -- history", None)
        assert (None,None) == GradeLevelClassifier.target_age(None, None)

    def test_age_from_age_classifier(self):
        def f(t):
            return AgeClassifier.target_age(t, None)
        assert (9,12) == f("Ages 9-12")
        assert (9,13) == f("9 and up")
        assert (9,13) == f("9 and up.")
        assert (9,13) == f("9+")
        assert (9,13) == f("9+.")
        assert (None,None) == f("900-901")
        assert (9,12) == f("9-12")
        assert (9,9) == f("9 years")
        assert (9,12) == f("9 - 12 years")
        assert (12,14) == f("12 - 14")
        assert (12,14) == f("14 - 12")
        assert (0,3) == f("0-3")
        assert (5,8) == f("05 - 08")
        assert (None,None) == f("K-3")
        assert (18, 18) == f("Age 18+")

        # This could be improved but I've never actually seen a
        # classification like this.
        assert (16, 16) == f("up to age 16")

        assert (None,None) == AgeClassifier.target_age("K-3", None, True)
        assert (None,None) == AgeClassifier.target_age("9-12", None, True)
        assert (9,13) == AgeClassifier.target_age("9 and up", None, True)
        assert (7,9) == AgeClassifier.target_age("7 years and up.", None, True)

    def test_age_from_keyword_classifier(self):
        def f(t):
            return LCSH.target_age(t, None)
        assert (5,5) == f("Interest age: from c 5 years")
        assert (9,12) == f("Children's Books / 9-12 Years")
        assert (9,12) == f("Ages 9-12")
        assert (9,12) == f("Age 9-12")
        assert (9,12) == f("Children's Books/Ages 9-12 Fiction")
        assert (4,8) == f("Children's Books / 4-8 Years")
        assert (0,2) == f("For children c 0-2 years")
        assert (12,14) == f("Children: Young Adult (Gr. 7-9)")
        assert (8,10) == f("Grades 3-5 (Common Core History: The Alexandria Plan)")
        assert (9,11) == f("Children: Grades 4-6")

        assert (0,3) == f("Baby-3 Years")

        assert (None,None) == f("Children's Audio - 9-12") # Doesn't specify grade or years
        assert (None,None) == f("Children's 9-12 - Literature - Classics / Contemporary")
        assert (None,None) == f("Third-graders")
        assert (None,None) == f("First graders")
        assert (None,None) == f("Fifth grade (Education)--Curricula")

    def test_audience_from_age_classifier(self):
        def f(t):
            return AgeClassifier.audience(t, None)
        assert Classifier.AUDIENCE_CHILDREN == f("Age 5")
        assert Classifier.AUDIENCE_ADULT == f("Age 18+")
        assert None == f("Ages Of Man")
        assert None == f("Age -12")
        assert Classifier.AUDIENCE_YOUNG_ADULT == f("up to age 16")
        assert Classifier.AUDIENCE_YOUNG_ADULT == f("Age 12-14")
        assert Classifier.AUDIENCE_YOUNG_ADULT == f("Ages 13 and up")
        assert Classifier.AUDIENCE_CHILDREN == f("Age 12-13")

    def test_audience_from_age_or_grade_classifier(self):
        def f(t):
            return AgeOrGradeClassifier.audience(t, None)
        assert Classifier.AUDIENCE_CHILDREN == f(
            "Children's - Kindergarten, Age 5-6")

    def test_age_from_age_or_grade_classifier(self):
        def f(t):
            t = AgeOrGradeClassifier.scrub_identifier(t)
            return AgeOrGradeClassifier.target_age(t, None)
        assert (5,6) == f("Children's - Kindergarten, Age 5-6")
        assert (5,5) == f("Children's - Kindergarten")
        assert (9,12) == f("Ages 9-12")


class TestInterestLevelClassifier(object):

    def test_audience(self):
        def f(t):
            return InterestLevelClassifier.audience(t, None)
        assert Classifier.AUDIENCE_CHILDREN == f("lg")
        assert Classifier.AUDIENCE_CHILDREN == f("mg")
        assert Classifier.AUDIENCE_CHILDREN == f("mg+")
        assert Classifier.AUDIENCE_YOUNG_ADULT == f("ug")

    def test_target_age(self):
        def f(t):
            return InterestLevelClassifier.target_age(t, None)
        assert (5,8) == f("lg")
        assert (9,13) == f("mg")
        assert (9,13) == f("mg+")
        assert (14,17) == f("ug")
