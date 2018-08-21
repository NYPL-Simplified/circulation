from nose.tools import (
    eq_,
    set_trace,
)
from classifier import (
    Classifier,
    AgeOrGradeClassifier,
    LCSHClassifier as LCSH,
)

from classifier.age import (
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
        eq_(range2, range1)
        eq_(5, range2[0])
        eq_(6, range2[1])

        # If one of the target ages is None, it's left alone.
        r = Classifier.range_tuple(None,6)
        eq_(None, r[0])
        eq_(6, r[1])

        r = Classifier.range_tuple(18,None)
        eq_(18, r[0])
        eq_(None, r[1])


    def test_age_from_grade_classifier(self):
        def f(t):
            return GradeLevelClassifier.target_age(t, None)
        eq_(
            Classifier.range_tuple(5,6),
            GradeLevelClassifier.target_age(None, "grades 0-1")
        )
        eq_((4,7), f("pk - 2"))
        eq_((5,7), f("grades k-2"))
        eq_((6,6), f("first grade"))
        eq_((6,6), f("1st grade"))
        eq_((6,6), f("grade 1"))
        eq_((7,7), f("second grade"))
        eq_((7,7), f("2nd grade"))
        eq_((8,8), f("third grade"))
        eq_((9,9), f("fourth grade"))
        eq_((10,10), f("fifth grade"))
        eq_((11,11), f("sixth grade"))
        eq_((12,12), f("7th grade"))
        eq_((13,13), f("grade 8"))
        eq_((14,14), f("9th grade"))
        eq_((15,17), f("grades 10-12"))
        eq_((6,6), f("grades 00-01"))
        eq_((8,12), f("grades 03-07"))
        eq_((8,12), f("3-07"))
        eq_((8,10), f("5 - 3"))
        eq_((17,17), f("12th grade"))

        # target_age() will assume that a number it sees is talking
        # about a grade level, unless require_explicit_grade_marker is
        # True.
        eq_((14,17), f("Children's Audio - 9-12"))
        eq_((7,9), GradeLevelClassifier.target_age("2-4", None, False))
        eq_((None,None), GradeLevelClassifier.target_age("2-4", None, True))
        eq_((None,None), GradeLevelClassifier.target_age(
            "Children's Audio - 9-12", None, True))

        eq_((None,None), GradeLevelClassifier.target_age("grade 50", None))
        eq_((None,None), GradeLevelClassifier.target_age("road grades -- history", None))
        eq_((None,None), GradeLevelClassifier.target_age(None, None))

    def test_age_from_age_classifier(self):
        def f(t):
            return AgeClassifier.target_age(t, None)
        eq_((9,12), f("Ages 9-12"))
        eq_((9,13), f("9 and up"))
        eq_((9,13), f("9 and up."))
        eq_((9,13), f("9+"))
        eq_((9,13), f("9+."))
        eq_((None,None), f("900-901"))
        eq_((9,12), f("9-12"))
        eq_((9,9), f("9 years"))
        eq_((9,12), f("9 - 12 years"))
        eq_((12,14), f("12 - 14"))
        eq_((12,14), f("14 - 12"))
        eq_((0,3), f("0-3"))
        eq_((5,8), f("05 - 08"))
        eq_((None,None), f("K-3"))
        eq_((18, 18), f("Age 18+"))

        # This could be improved but I've never actually seen a
        # classification like this.
        eq_((16, 16), f("up to age 16"))

        eq_((None,None), AgeClassifier.target_age("K-3", None, True))
        eq_((None,None), AgeClassifier.target_age("9-12", None, True))
        eq_((9,13), AgeClassifier.target_age("9 and up", None, True))
        eq_((7,9), AgeClassifier.target_age("7 years and up.", None, True))

    def test_age_from_keyword_classifier(self):
        def f(t):
            return LCSH.target_age(t, None)
        eq_((5,5), f("Interest age: from c 5 years"))
        eq_((9,12), f("Children's Books / 9-12 Years"))
        eq_((9,12), f("Ages 9-12"))
        eq_((9,12), f("Age 9-12"))
        eq_((9,12), f("Children's Books/Ages 9-12 Fiction"))
        eq_((4,8), f("Children's Books / 4-8 Years"))
        eq_((0,2), f("For children c 0-2 years"))
        eq_((12,14), f("Children: Young Adult (Gr. 7-9)"))
        eq_((8,10), f("Grades 3-5 (Common Core History: The Alexandria Plan)"))
        eq_((9,11), f("Children: Grades 4-6"))

        eq_((0,3), f("Baby-3 Years"))

        eq_((None,None), f("Children's Audio - 9-12")) # Doesn't specify grade or years
        eq_((None,None), f("Children's 9-12 - Literature - Classics / Contemporary"))
        eq_((None,None), f("Third-graders"))
        eq_((None,None), f("First graders"))
        eq_((None,None), f("Fifth grade (Education)--Curricula"))

    def test_audience_from_age_classifier(self):
        def f(t):
            return AgeClassifier.audience(t, None)
        eq_(Classifier.AUDIENCE_CHILDREN, f("Age 5"))
        eq_(Classifier.AUDIENCE_ADULT, f("Age 18+"))
        eq_(None, f("Ages Of Man"))
        eq_(None, f("Age -12"))
        eq_(Classifier.AUDIENCE_YOUNG_ADULT, f("up to age 16"))
        eq_(Classifier.AUDIENCE_YOUNG_ADULT, f("Age 12-14"))
        eq_(Classifier.AUDIENCE_YOUNG_ADULT, f("Ages 13 and up"))
        eq_(Classifier.AUDIENCE_CHILDREN, f("Age 12-13"))

    def test_audience_from_age_or_grade_classifier(self):
        def f(t):
            return AgeOrGradeClassifier.audience(t, None)
        eq_(Classifier.AUDIENCE_CHILDREN, f(
            "Children's - Kindergarten, Age 5-6"))

    def test_age_from_age_or_grade_classifier(self):
        def f(t):
            t = AgeOrGradeClassifier.scrub_identifier(t)
            return AgeOrGradeClassifier.target_age(t, None)
        eq_((5,6), f("Children's - Kindergarten, Age 5-6"))
        eq_((5,5), f("Children's - Kindergarten"))
        eq_((9,12), f("Ages 9-12"))


class TestInterestLevelClassifier(object):

    def test_audience(self):
        def f(t):
            return InterestLevelClassifier.audience(t, None)
        eq_(Classifier.AUDIENCE_CHILDREN, f("lg"))
        eq_(Classifier.AUDIENCE_CHILDREN, f("mg"))
        eq_(Classifier.AUDIENCE_CHILDREN, f("mg+"))
        eq_(Classifier.AUDIENCE_YOUNG_ADULT, f("ug"))

    def test_target_age(self):
        def f(t):
            return InterestLevelClassifier.target_age(t, None)
        eq_((5,8), f("lg"))
        eq_((9,13), f("mg"))
        eq_((9,13), f("mg+"))
        eq_((14,17), f("ug"))
