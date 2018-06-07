"""Test logic surrounding classification schemes."""

from nose.tools import eq_, set_trace
from . import DatabaseTest
from collections import Counter
from psycopg2.extras import NumericRange
from model import (
    Genre,
    DataSource,
    Subject,
    Classification,
)
import classifier
from classifier import (
    Classifier,
    DeweyDecimalClassifier as DDC,
    LCCClassifier as LCC,
    BICClassifier as BIC,
    LCSHClassifier as LCSH,
    OverdriveClassifier as Overdrive,
    FASTClassifier as FAST,
    KeywordBasedClassifier as Keyword,
    SimplifiedGenreClassifier,
    GradeLevelClassifier,
    AgeClassifier,
    AgeOrGradeClassifier,
    InterestLevelClassifier,
    Axis360AudienceClassifier,
    Lowercased,
    WorkClassifier,
    Lowercased,
    fiction_genres,
    nonfiction_genres,
    GenreData,
    )

genres = dict()
GenreData.populate(globals(), genres, fiction_genres, nonfiction_genres)


class TestLowercased(object):

    def test_constructor(self):

        l = Lowercased("A string")

        # A string is lowercased.
        eq_("a string", l)

        # A Lowercased object is returned rather than creating a new
        # object.
        assert Lowercased(l) is l

        # A number such as a Dewey Decimal number is converted to a string.
        eq_(u"301", Lowercased(301))

        # A trailing period is removed.
        l = Lowercased("A string.")
        eq_("a string", l)

        # The original value is still available.
        eq_("A string.", l.original)


class TestGenreData(object):

    def test_fiction_default(self):
        # In general, genres are restricted to either fiction or
        # nonfiction.
        eq_(True, Science_Fiction.is_fiction)
        eq_(False, Science.is_fiction)


class TestClassifier(object):

    def test_default_target_age_for_audience(self):

        eq_(
            (None, None), 
            Classifier.default_target_age_for_audience(Classifier.AUDIENCE_CHILDREN)
        )
        eq_(
            (14, 17), 
            Classifier.default_target_age_for_audience(Classifier.AUDIENCE_YOUNG_ADULT)
        )
        eq_(
            (18, None), 
            Classifier.default_target_age_for_audience(Classifier.AUDIENCE_ADULT)
        )
        eq_(
            (18, None), 
            Classifier.default_target_age_for_audience(Classifier.AUDIENCE_ADULTS_ONLY)
        )

    def test_default_audience_for_target_age(self):
        def aud(low, high, expect):
            eq_(expect, Classifier.default_audience_for_target_age((low, high)))

        eq_(None, Classifier.default_audience_for_target_age(None))
        aud(None, None, None)
        aud(None, 17, Classifier.AUDIENCE_YOUNG_ADULT)
        aud(None, 4, Classifier.AUDIENCE_CHILDREN)
        aud(None, 44, Classifier.AUDIENCE_ADULT)
        aud(18, 44, Classifier.AUDIENCE_ADULT)
        aud(14, 14, Classifier.AUDIENCE_YOUNG_ADULT)
        aud(14, 19, Classifier.AUDIENCE_YOUNG_ADULT)
        aud(2, 14, Classifier.AUDIENCE_CHILDREN)
        aud(2, 8, Classifier.AUDIENCE_CHILDREN)

        # We treat this as YA because its target age range overlaps
        # our YA age range, and many external sources consider books
        # for twelve-year-olds to be "YA".
        aud(12, 15, Classifier.AUDIENCE_YOUNG_ADULT)

        # Whereas this is unambiguously 'Children' as far as we're concerned.
        aud(12, 13, Classifier.AUDIENCE_CHILDREN)

    def test_and_up(self):
        """Test the code that determines what "x and up" actually means."""
        def u(young, keyword):
            return Classifier.and_up(young, keyword)

        eq_(None, u(None, None))
        eq_(None, u(6, "6 years old only"))
        eq_(5, u(3, "3 and up"))
        eq_(8, u(6, "6+"))
        eq_(12, u(8, "8+"))
        eq_(14, u(10, "10+"))
        eq_(17, u(12, "12 and up"))
        eq_(17, u(14, "14+."))
        eq_(18, u(18, "18+"))


    def test_scrub_identifier_can_override_name(self):
        """Test the ability of scrub_identifier to override the name
        of the subject for classification purposes.

        This is used e.g. in the BISACClassifier to ensure that a known BISAC
        code is always mapped to its canonical name.
        """
        class SetsNameForOneIdentifier(Classifier):
            "A Classifier that insists on a certain name for one specific identifier"
            @classmethod
            def scrub_identifier(self, identifier):
                if identifier == 'A':
                    return ('A', 'Use this name!')
                else:
                    return identifier

            @classmethod
            def scrub_name(self, name):
                """This verifies that the override name still gets passed
                into scrub_name.
                """
                return name.upper()

        m = SetsNameForOneIdentifier.scrub_identifier_and_name
        eq_(("A", "USE THIS NAME!"), m("A", "name a"))
        eq_(("B", "NAME B"), m("B", "name b"))

    def test_scrub_identifier(self):
        m = Classifier.scrub_identifier
        eq_(None, m(None))
        eq_(Lowercased("Foo"), m("Foo"))

    def test_scrub_name(self):
        m = Classifier.scrub_name
        eq_(None, m(None))
        eq_(Lowercased("Foo"), m("Foo"))


class TestClassifierLookup(object):

    def test_lookup(self):
        eq_(DDC, Classifier.lookup(Classifier.DDC))
        eq_(LCC, Classifier.lookup(Classifier.LCC))
        eq_(LCSH, Classifier.lookup(Classifier.LCSH))
        eq_(FAST, Classifier.lookup(Classifier.FAST))
        eq_(GradeLevelClassifier, Classifier.lookup(Classifier.GRADE_LEVEL))
        eq_(AgeClassifier, Classifier.lookup(Classifier.AGE_RANGE))
        eq_(InterestLevelClassifier, Classifier.lookup(Classifier.INTEREST_LEVEL))
        eq_(None, Classifier.lookup('no-such-key'))

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


class TestDewey(object):

    def test_name_for(self):
        eq_("General statistics of Europe", DDC.name_for("314"))
        eq_("Biography", DDC.name_for("B"))
        eq_("Human physiology", DDC.name_for("612"))
        eq_("American speeches in English", DDC.name_for("815"))
        eq_("Juvenile Nonfiction", DDC.name_for("J"))
        eq_("Fiction", DDC.name_for("FIC"))
        eq_(None, DDC.name_for("Fic"))

    def test_audience(self):

        child = Classifier.AUDIENCE_CHILDREN
        adult = Classifier.AUDIENCE_ADULT
        young_adult = Classifier.AUDIENCE_YOUNG_ADULT

        def aud(identifier):
            return DDC.audience(*DDC.scrub_identifier(identifier))

        eq_(child, aud("JB"))
        eq_(child, aud("J300"))
        eq_(child, aud("NZJ300"))
        eq_(child, aud("E"))
        eq_(young_adult, aud("Y300"))
        eq_(None, aud("FIC"))
        eq_(None, aud("Fic"))
        eq_(adult, aud("B"))
        eq_(adult, aud("400"))


    def test_is_fiction(self):

        def fic(identifier):
            return DDC.is_fiction(*DDC.scrub_identifier(identifier))

        eq_(True, fic("FIC"))
        eq_(True, fic("E"))
        eq_(True, fic(813))

        eq_(False, fic("JB"))
        eq_(False, fic("400"))
        eq_(False, fic("616.9940092"))
        eq_(False, fic(615))
        eq_(False, fic(800))
        eq_(False, fic(814))

    def test_classification(self):
        def c(identifier):
            i, name = DDC.scrub_identifier(identifier)
            eq_(name, None)
            return DDC.genre(i, None)

        eq_(classifier.Folklore, c("398"))

class TestLCC(object):

    def test_name_for(self):

        child = Classifier.AUDIENCE_CHILDREN
        adult = Classifier.AUDIENCE_ADULT

        eq_("LANGUAGE AND LITERATURE", LCC.name_for("P"))
        eq_("English literature", LCC.name_for("PR"))
        eq_("Fiction and juvenile belles lettres", LCC.name_for("PZ"))
        eq_("HISTORY OF THE AMERICAS", LCC.name_for("E"))
        eq_('Literature (General)', LCC.name_for("PN"))
        eq_(None, LCC.name_for("no-such-key"))

    def test_audience(self):
        child = Classifier.AUDIENCE_CHILDREN
        adult = Classifier.AUDIENCE_ADULT
        young_adult = Classifier.AUDIENCE_YOUNG_ADULT

        def aud(identifier):
            return LCC.audience(LCC.scrub_identifier(identifier), None)

        eq_(adult, aud("PR"))
        eq_(adult, aud("P"))
        eq_(adult, aud("PA"))
        eq_(adult, aud("J821.8 CARRIKK"))
        eq_(child, aud("PZ"))
        eq_(child, aud("PZ2384 M68 2003"))
        eq_(child, aud("pz2384 m68 2003"))

    def test_is_fiction(self):
        def fic(lcc):
            return LCC.is_fiction(LCC.scrub_identifier(lcc), None)
        eq_(False, fic("A"))
        eq_(False, fic("AB"))
        eq_(False, fic("PA"))
        eq_(True, fic("P"))
        eq_(True, fic("p"))
        eq_(True, fic("PN"))
        eq_(True, fic("PQ"))
        eq_(True, fic("PR"))
        eq_(True, fic("PS"))
        eq_(True, fic("PT"))
        eq_(True, fic("PZ"))
        eq_(True, fic("PZ2384 M68 2003"))


class TestLCSH(object):

    def test_is_fiction(self):
        def fic(lcsh):
            return LCSH.is_fiction(None, LCSH.scrub_name(lcsh))

        eq_(True, fic("Science fiction"))
        eq_(True, fic("Science fiction, American"))
        eq_(True, fic("Fiction"))
        eq_(True, fic("Historical fiction"))
        eq_(True, fic("Biographical fiction"))
        eq_(True, fic("Detective and mystery stories"))
        eq_(True, fic("Horror tales"))
        eq_(True, fic("Classical literature"))
        eq_(False, fic("History and criticism"))
        eq_(False, fic("Biography"))
        eq_(None, fic("Kentucky"))
        eq_(None, fic("Social life and customs"))


    def test_audience(self):
        child = Classifier.AUDIENCE_CHILDREN
        def aud(lcsh):
            return LCSH.audience(None, LCSH.scrub_name(lcsh))

        eq_(child, aud("Children's stories"))
        eq_(child, aud("Picture books for children"))
        eq_(child, aud("Juvenile fiction"))
        eq_(child, aud("Juvenile poetry"))
        eq_(None, aud("Juvenile delinquency"))
        eq_(None, aud("Runaway children"))
        eq_(None, aud("Humor"))

class TestKeyword(object):
    def genre(self, keyword):
        scrub = Keyword.scrub_identifier(keyword)
        fiction = Keyword.is_fiction(None, scrub)
        audience = Keyword.audience(None, scrub)
        return Keyword.genre(None, scrub, fiction, audience)

    def test_higher_tier_wins(self):
        eq_(classifier.Space_Opera, self.genre("space opera"))
        eq_(classifier.Drama, self.genre("opera"))

        eq_(classifier.Historical_Fiction, self.genre("Arthurian romances"))
        eq_(classifier.Romance, self.genre("Regency romances"))

    def test_audience(self):
        eq_(Classifier.AUDIENCE_YOUNG_ADULT, 
            Keyword.audience(None, "Teens / Fiction"))

        eq_(Classifier.AUDIENCE_YOUNG_ADULT, 
            Keyword.audience(None, "teen books"))

    def test_subgenre_wins_over_genre(self):
        # Asian_History wins over History, even though they both
        # have the same number of matches, because Asian_History is more
        # specific.
        eq_(classifier.Asian_History, self.genre("asian history"))
        eq_(classifier.Asian_History, self.genre("history: asia"))

    def test_classification_may_depend_on_fiction_status(self):
        eq_(classifier.Humorous_Nonfiction, self.genre("Humor (Nonfiction)"))
        eq_(classifier.Humorous_Fiction, self.genre("Humorous stories"))

    def test_children_audience_implies_no_genre(self):
        eq_(None, self.genre("Children's Books"))

    def test_young_adult_wins_over_children(self):
        eq_(Classifier.AUDIENCE_YOUNG_ADULT, 
            Keyword.audience(None, "children's books - young adult fiction")
        )

    def test_juvenile_romance_means_young_adult(self):
        eq_(Classifier.AUDIENCE_YOUNG_ADULT, 
            Keyword.audience(None, "juvenile fiction / love & romance")
        )

        eq_(Classifier.AUDIENCE_YOUNG_ADULT,
            Keyword.audience(None, "teenage romance")
        )

    def test_audience_match(self):
        (audience, match) = Keyword.audience_match("teen books")
        eq_(Classifier.AUDIENCE_YOUNG_ADULT, audience)
        eq_("teen books", match)

        # This is a search for a specific example so it doesn't match
        (audience, match) = Keyword.audience_match("teen romance")
        eq_(None, audience)

    def test_genre_match(self):
        (genre, match) = Keyword.genre_match("pets")
        eq_(classifier.Pets, genre)
        eq_("pets", match)

        # This is a search for a specific example so it doesn't match
        (genre, match) = Keyword.genre_match("cats")
        eq_(None, genre)

    def test_improvements(self):
        """A place to put tests for miscellaneous improvements added 
        since the original work.
        """
        # was Literary Fiction
        eq_(classifier.Science_Fiction,
            Keyword.genre(None, "Science Fiction - General")
        )

        # Was General Fiction (!)
        eq_(classifier.Science_Fiction,
            Keyword.genre(None, "Science Fiction")
        )
        
        eq_(classifier.Science_Fiction,
            Keyword.genre(None, "Speculative Fiction")
        )

        eq_(classifier.Social_Sciences,
            Keyword.genre(None, "Social Sciences")
        )

        eq_(classifier.Social_Sciences,
            Keyword.genre(None, "Social Science")
        )
        
        eq_(classifier.Social_Sciences,
            Keyword.genre(None, "Human Science")
        )

        # was genreless
        eq_(classifier.Short_Stories,
            Keyword.genre(None, "Short Stories")
        )
        
        # was Military History
        eq_(classifier.Military_SF,
            Keyword.genre(None, "Interstellar Warfare")
        )

        # was Fantasy
        eq_(classifier.Games,
            Keyword.genre(None, "Games / Role Playing & Fantasy")
        )

        # This isn't perfect but it covers most cases.
        eq_(classifier.Media_Tie_in_SF,
            Keyword.genre(None, "TV, Movie, Video game adaptations")
        )

        # Previously only 'nonfiction' was recognized.
        eq_(False, Keyword.is_fiction(None, "Non-Fiction"))
        eq_(False, Keyword.is_fiction(None, "Non Fiction"))

        # "Historical" on its own means historical fiction, but a
        # string containing "Historical" does not mean anything in
        # particular.
        eq_(classifier.Historical_Fiction, Keyword.genre(None, "Historical"))
        eq_(None, Keyword.genre(None, "Historicals"))

        # The Fiction/Urban classification is different from the
        # African-American-focused "Urban Fiction" classification.
        eq_(None, Keyword.genre(None, "Fiction/Urban"))

        eq_(classifier.Folklore, Keyword.genre(None, "fables"))
        

class TestBIC(object):

    def test_is_fiction(self):
        def fic(bic):
            return BIC.is_fiction(BIC.scrub_identifier(bic), None)

        eq_(True, fic("FCA"))
        eq_(True, fic("YFL"))
        eq_(False, fic("YWR"))
        eq_(False, fic("HB"))

    def test_audience(self):
        young_adult = Classifier.AUDIENCE_YOUNG_ADULT
        adult = Classifier.AUDIENCE_ADULT
        def aud(bic):
            return BIC.audience(BIC.scrub_identifier(bic), None)
            
        eq_(adult, aud("DD"))
        eq_(young_adult, aud("YFA"))

    def test_genre(self):
        def gen(bic):
            return BIC.genre(BIC.scrub_identifier(bic), None)
        eq_(classifier.Art_Design,
            gen("A"))
        eq_(classifier.Art_Design,
            gen("AB"))
        eq_(classifier.Music,
            gen("AV"))
        eq_(classifier.Fantasy,
            gen("FM"))
        eq_(classifier.Economics,
            gen("KC"))
        eq_(classifier.Short_Stories,
           gen("FYB"))
        eq_(classifier.Music,
            gen("YNC"))
        eq_(classifier.European_History,
            gen("HBJD"))

class TestAxis360Classifier(object):

    def test_audience(self):
        def f(t):
            return Axis360AudienceClassifier.audience(t, None)
        eq_(Classifier.AUDIENCE_CHILDREN, 
            f("Children's - Kindergarten, Age 5-6"))
        eq_(Classifier.AUDIENCE_CHILDREN,
            f("Children's - Grade 2-3, Age 7-8"))
        eq_(Classifier.AUDIENCE_CHILDREN,
            f("Children's - Grade 4-6, Age 9-11"))
        eq_(Classifier.AUDIENCE_YOUNG_ADULT,
            f("Teen - Grade 7-9, Age 12-14"))
        eq_(Classifier.AUDIENCE_YOUNG_ADULT, 
            f("Teen - Grade 10-12, Age 15-18"))
        eq_(Classifier.AUDIENCE_ADULT, f("General Adult"))
        eq_(None, f(""))
        eq_(None, f(None))

    def test_age(self):
        def f(t):
            return Axis360AudienceClassifier.target_age(t, None)
        eq_((5,6), f("Children's - Kindergarten, Age 5-6"))
        eq_((7,8), f("Children's - Grade 2-3, Age 7-8"))
        eq_((9,11), f("Children's - Grade 4-6, Age 9-11"))
        eq_((12,14), f("Teen - Grade 7-9, Age 12-14"))
        eq_((12,14), f("Teen - Grade 7-9, Age 14-12"))
        eq_((15,18), f("Teen - Grade 10-12, Age 15-18"))
        eq_((None,None), f("General Adult"))

class TestNestedSubgenres(object):

    def test_parents(self):
        eq_([classifier.Romance],
            list(classifier.Romantic_Suspense.parents))

        #eq_([classifier.Crime_Thrillers_Mystery, classifier.Mystery],
        #    list(classifier.Police_Procedurals.parents))

    def test_self_and_subgenres(self):
        # Fantasy
        #  - Epic Fantasy
        #  - Historical Fantasy
        #  - Urban Fantasy
        eq_(
            set([classifier.Fantasy, classifier.Epic_Fantasy, 
                 classifier.Historical_Fantasy, classifier.Urban_Fantasy,
             ]),
            set(list(classifier.Fantasy.self_and_subgenres)))

class TestConsolidateWeights(object):

    def test_consolidate(self):
        # Asian History is a subcategory of the top-level category History.
        weights = dict()
        weights[classifier.History] = 10
        weights[classifier.Asian_History] = 4
        weights[classifier.Middle_East_History] = 1
        w2 = WorkClassifier.consolidate_genre_weights(weights)
        eq_(14, w2[classifier.Asian_History])
        eq_(1, w2[classifier.Middle_East_History])
        assert classifier.History not in w2

        # Paranormal Romance is a subcategory of Romance, which is itself
        # a subcategory.
        weights = dict()
        weights[classifier.Romance] = 100
        weights[classifier.Paranormal_Romance] = 4
        w2 = WorkClassifier.consolidate_genre_weights(weights)
        eq_(104, w2[classifier.Paranormal_Romance])
        assert classifier.Romance not in w2

    def test_consolidate_through_multiple_levels(self):
        # Romance is the parent of the parent of Paranormal
        # Romance, but its weight successfully flows down into
        # Paranormal Romance.
        weights = dict()
        weights[classifier.Romance] = 100
        weights[classifier.Paranormal_Romance] = 4
        w2 = WorkClassifier.consolidate_genre_weights(weights)
        eq_(104, w2[classifier.Paranormal_Romance])
        assert classifier.Romance not in w2

    def test_consolidate_through_multiple_levels_from_multiple_sources(self):
        # This test can't work anymore because we no longer have a
        # triply-nested category like Romance/Erotica -> Romance ->
        # Paranormal Romance.
        #
        # weights = dict()
        # weights[classifier.Romance_Erotica] = 50
        # weights[classifier.Romance] = 50
        # weights[classifier.Paranormal_Romance] = 4
        # w2 = WorkClassifier.consolidate_genre_weights(weights)
        # eq_(104, w2[classifier.Paranormal_Romance])
        # assert classifier.Romance not in w2
        pass

    def test_consolidate_fails_when_threshold_not_met(self):
        weights = dict()
        weights[classifier.History] = 100
        weights[classifier.Middle_East_History] = 1
        w2 = WorkClassifier.consolidate_genre_weights(weights)
        eq_(100, w2[classifier.History])
        eq_(1, w2[classifier.Middle_East_History])


class TestSimplifiedGenreClassifier(object):

    def test_scrub_identifier(self):
        """The URI for a Library Simplified genre is treated the same as
        the genre itself.
        """
        sf1 = SimplifiedGenreClassifier.scrub_identifier(
            SimplifiedGenreClassifier.SIMPLIFIED_GENRE + "Science%20Fiction"
        )
        sf2 = SimplifiedGenreClassifier.scrub_identifier("Science Fiction")
        eq_(sf1, sf2)
        eq_("Science Fiction", sf1.original)

    def test_genre(self):
        genre_name = "Space Opera"
        scrubbed = SimplifiedGenreClassifier.scrub_identifier(genre_name)

        genre = SimplifiedGenreClassifier.genre(scrubbed, genre_name, fiction=True)
        eq_(genre.name, globals()["genres"][genre_name].name)

        genre = SimplifiedGenreClassifier.genre(scrubbed, genre_name)
        eq_(genre.name, globals()["genres"][genre_name].name)

        genre = SimplifiedGenreClassifier.genre(scrubbed, genre_name, fiction=False)
        eq_(genre, None)

    def test_is_fiction(self):
        genre_name = "Space Opera"
        scrubbed = SimplifiedGenreClassifier.scrub_identifier(genre_name)
        is_fiction = SimplifiedGenreClassifier.is_fiction(scrubbed, genre_name)
        eq_(is_fiction, True)

        genre_name = "Cooking"
        scrubbed = SimplifiedGenreClassifier.scrub_identifier(genre_name)
        is_fiction = SimplifiedGenreClassifier.is_fiction(scrubbed, genre_name)
        eq_(is_fiction, False)

        genre_name = "Fake Genre"
        scrubbed = SimplifiedGenreClassifier.scrub_identifier(genre_name)
        is_fiction = SimplifiedGenreClassifier.is_fiction(scrubbed, genre_name)
        eq_(is_fiction, None)


class TestWorkClassifier(DatabaseTest):

    def setup(self):
        super(TestWorkClassifier, self).setup()
        self.work = self._work(with_license_pool=True)
        self.identifier = self.work.presentation_edition.primary_identifier
        self.classifier = WorkClassifier(self.work, test_session=self._db)

    def _genre(self, genre_data):
        expected_genre, ignore = Genre.lookup(self._db, genre_data.name)
        return expected_genre

    def test_no_assumptions(self):
        """If we have no data whatsoever, we make no assumptions
        about a work's classification.
        """
        self.classifier.weigh_metadata()
        eq_(None, self.classifier.fiction())
        eq_(None, self.classifier.audience())
        eq_({}, self.classifier.genres(None))
        eq_((None, None), self.classifier.target_age(None))

    def test_weight_metadata_title(self):
        self.work.presentation_edition.title = u"Star Trek: The Book"
        expected_genre = self._genre(classifier.Media_Tie_in_SF)
        self.classifier.weigh_metadata()
        eq_(100, self.classifier.genre_weights[expected_genre])

    def test_weight_metadata_publisher(self):
        # Genre publisher and imprint
        self.work.presentation_edition.publisher = u"Harlequin"
        expected_genre = self._genre(classifier.Romance)
        self.classifier.weigh_metadata()
        eq_(100, self.classifier.genre_weights[expected_genre])

    def test_weight_metadata_imprint(self):
        # Imprint is more specific than publisher, so it takes precedence.
        self.work.presentation_edition.publisher = u"Harlequin"
        self.work.presentation_edition.imprint = u"Harlequin Intrigue"
        expected_genre = self._genre(classifier.Romantic_Suspense)
        general_romance = self._genre(classifier.Romance)

        self.classifier.weigh_metadata()
        assert general_romance not in self.classifier.genre_weights
        eq_(100, self.classifier.genre_weights[expected_genre])

    def test_metadata_implies_audience_and_genre(self):
        # Genre and audience publisher 
        self.work.presentation_edition.publisher = u"Harlequin"
        self.work.presentation_edition.imprint = u"Harlequin Teen"
        expected_genre = self._genre(classifier.Romance)

        self.classifier.weigh_metadata()
        eq_(100, self.classifier.genre_weights[expected_genre])
        eq_(100, self.classifier.audience_weights[Classifier.AUDIENCE_YOUNG_ADULT])

    def test_metadata_implies_fiction_status(self):
        self.work.presentation_edition.publisher = u"Harlequin"
        self.work.presentation_edition.imprint = u"Harlequin Nonfiction"
        self.classifier.weigh_metadata()

        eq_(100, self.classifier.fiction_weights[False])
        assert True not in self.classifier.fiction_weights

    def test_publisher_excludes_adult_audience(self):
        # We don't know if this is a children's book or a young adult
        # book, but we're confident it's not a book for adults.
        self.work.presentation_edition.publisher = u"Scholastic Inc."

        self.classifier.weigh_metadata()
        eq_(-100, self.classifier.audience_weights[Classifier.AUDIENCE_ADULT])
        eq_(-100, self.classifier.audience_weights[Classifier.AUDIENCE_ADULTS_ONLY])

    def test_imprint_excludes_adult_audience(self):
        self.work.presentation_edition.imprint = u"Delacorte Books for Young Readers"

        self.classifier.weigh_metadata()
        eq_(-100, self.classifier.audience_weights[Classifier.AUDIENCE_ADULT])
        eq_(-100, self.classifier.audience_weights[Classifier.AUDIENCE_ADULTS_ONLY])

    def test_no_children_or_ya_signal_from_distributor_implies_book_is_for_adults(self):
        # Create some classifications that end up in
        # direct_from_license_source, but don't imply that the book is
        # from children or
        # YA. classifier.audience_weights[AUDIENCE_ADULT] will be set
        # to 500.
        i = self.identifier
        source = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        for subject in ('Nonfiction', 'Science Fiction', 'History'):
            c = i.classify(source, Subject.OVERDRIVE, subject, weight=1000)
            self.classifier.add(c)

        # There's a little bit of evidence that it's a children's book,
        # but not enough to outweight the distributor's silence.
        c2 = self.identifier.classify(
            source, Subject.TAG, u"Children's books", weight=1
        )
        self.classifier.add(c2)
        self.classifier.prepare_to_classify()
        # Overdrive classifications are regarded as 50 times more reliable
        # than their actual weight, as per Classification.scaled_weight
        eq_(50000, self.classifier.audience_weights[Classifier.AUDIENCE_ADULT])

    def test_adults_only_indication_from_distributor_has_no_implication_for_audience(self):
        # Create some classifications that end up in
        # direct_from_license_source, one of which implies the book is
        # for adults only.
        i = self.identifier
        source = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        for subject in ('Erotic Literature', 'Science Fiction', 'History'):
            c = i.classify(source, Subject.OVERDRIVE, subject, weight=1)
            self.classifier.add(c)

        self.classifier.prepare_to_classify()

        # Again, Overdrive classifications are regarded as 50 times
        # more reliable than their actual weight, as per
        # Classification.scaled_weight
        eq_(50, self.classifier.audience_weights[Classifier.AUDIENCE_ADULTS_ONLY])
        
        # No boost was given to AUDIENCE_ADULT, because a distributor
        # classification implied AUDIENCE_ADULTS_ONLY.
        eq_(0, self.classifier.audience_weights[Classifier.AUDIENCE_ADULT])

    def test_no_signal_from_distributor_has_no_implication_for_audience(self):
        # This work has no classifications that end up in
        # direct_from_license_source. In the absence of any such
        # classifications we cannot determine whether the
        # distributor's silence about the audience is because it's a
        # book for adults or because there's just no data from the
        # distributor.
        eq_({}, self.classifier.audience_weights)

    def test_children_or_ya_signal_from_distributor_has_no_immediate_implication_for_audience(self):
        # This work has a classification direct from the distributor
        # that implies the book is for children, so no conclusions are
        # drawn in the prepare_to_classify() step.
        source = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        c = self.identifier.classify(source, Subject.OVERDRIVE, u"Picture Books", weight=1000)
        self.classifier.prepare_to_classify()
        eq_({}, self.classifier.audience_weights)

        self.classifier.add(c)
        eq_(50000, self.classifier.audience_weights[Classifier.AUDIENCE_CHILDREN])

    def test_juvenile_classification_is_split_between_children_and_ya(self):

        # LCC files both children's and YA works under 'PZ'.
        # Here's how we deal with that.
        #
        i = self.identifier
        source = DataSource.lookup(self._db, DataSource.OCLC)
        c = i.classify(source, Subject.LCC, "PZ", weight=100)
        self.classifier.add(c)

        # (This classification has no bearing on audience and its
        # weight will be ignored.)
        c2 = i.classify(
            source, Subject.TAG, "Pets", 
            weight=1000
        )
        self.classifier.add(c2)
        self.classifier.prepare_to_classify
        genres, fiction, audience, target_age = self.classifier.classify()

        # Young Adult wins because we err on the side of showing books
        # to kids who are too old, rather than too young.
        eq_(Classifier.AUDIENCE_YOUNG_ADULT, audience)

        # But behind the scenes, more is going on. The weight of the
        # classifier has been split 60/40 between YA and children.
        weights = self.classifier.audience_weights
        eq_(60, weights[Classifier.AUDIENCE_YOUNG_ADULT])
        eq_(40, weights[Classifier.AUDIENCE_CHILDREN])
        # If this is in fact a children's book, this will make it
        # relatively easy for data from some other source to come in
        # and tip the balance.

        # The adult audiences have been reduced, to reduce the chance
        # that splitting up the weight between YA and Children will
        # cause the work to be mistakenly classified as Adult.
        for aud in Classifier.AUDIENCES_ADULT:
            eq_(-50, weights[aud])

    def test_childrens_book_when_evidence_is_overwhelming(self):
        # There is some evidence in the 'adult' and 'adults only'
        # bucket, but there's a lot more evidence that it's a
        # children's book, so we go with childrens or YA.

        # The evidence that this is a children's book is strong but
        # not overwhelming.
        self.classifier.audience_weights = {
            Classifier.AUDIENCE_ADULT : 10,
            Classifier.AUDIENCE_ADULTS_ONLY : 1,
            Classifier.AUDIENCE_CHILDREN : 22,
        }
        eq_(Classifier.AUDIENCE_ADULT, self.classifier.audience())
        
        # Now it's overwhelming. (the 'children' weight is more than twice
        # the combined 'adult' + 'adults only' weight.
        self.classifier.audience_weights[Classifier.AUDIENCE_CHILDREN] = 23
        eq_(Classifier.AUDIENCE_CHILDREN, self.classifier.audience())

        # Now it's overwhelmingly likely to be a YA book.
        del self.classifier.audience_weights[Classifier.AUDIENCE_CHILDREN]
        self.classifier.audience_weights[Classifier.AUDIENCE_YOUNG_ADULT] = 23
        eq_(Classifier.AUDIENCE_YOUNG_ADULT, self.classifier.audience())

    def test_ya_book_when_childrens_and_ya_combined_beat_adult(self):
        # Individually, the 'children' and 'ya' buckets don't beat the
        # combined 'adult' + 'adults only' bucket by the appropriate
        # factor, but combined they do.  In this case
        # we should classify the book as YA. It might be inaccurate,
        # but it's more accurate than 'adult' and less likely to be
        # a costly mistake than 'children'.
        self.classifier.audience_weights = {
            Classifier.AUDIENCE_ADULT : 9,
            Classifier.AUDIENCE_ADULTS_ONLY : 0,
            Classifier.AUDIENCE_CHILDREN : 10,
            Classifier.AUDIENCE_YOUNG_ADULT : 9,
        }
        eq_(Classifier.AUDIENCE_YOUNG_ADULT, self.classifier.audience())

    def test_genre_may_restrict_audience(self):

        # The audience info says this is a YA book.
        self.classifier.audience_weights = {
            Classifier.AUDIENCE_YOUNG_ADULT : 1000
        }

        # Without any genre information, it's classified as YA.
        eq_(Classifier.AUDIENCE_YOUNG_ADULT, self.classifier.audience())

        # But if it's Erotica, it is always classified as Adults Only.
        genres = { classifier.Erotica : 50,
                   classifier.Science_Fiction: 50}
        eq_(Classifier.AUDIENCE_ADULTS_ONLY, self.classifier.audience(genres))

    def test_format_classification_from_license_source_is_used(self):
        # This book will be classified as a comic book, because 
        # the "comic books" classification comes from its license source.
        source = self.work.license_pools[0].data_source
        self.identifier.classify(source, Subject.TAG, "Comic Books", weight=100)
        self.classifier.add(self.identifier.classifications[0])
        genres = self.classifier.genres(fiction=True)
        eq_([(classifier.Comics_Graphic_Novels, 100)], genres.items())

    def test_format_classification_not_from_license_source_is_ignored(self):
        # This book will be not classified as a comic book, because
        # the "comic books" classification does not come from its
        # license source.
        source = self.work.license_pools[0].data_source
        oclc = DataSource.lookup(self._db, DataSource.OCLC)
        self.identifier.classify(oclc, Subject.TAG, "Comic Books", weight=100)
        self.classifier.add(self.identifier.classifications[0])
        genres = self.classifier.genres(fiction=True)
        eq_([], genres.items())

    def test_childrens_book_when_no_evidence_for_adult_book(self):
        # There is no evidence in the 'adult' or 'adults only'
        # buckets, so minimal evidence in the 'children' bucket is
        # sufficient to be confident.
        self.classifier.audience_weights = {
            Classifier.AUDIENCE_ADULT : 0,
            Classifier.AUDIENCE_ADULTS_ONLY : 0,
            Classifier.AUDIENCE_CHILDREN : 1,
        }
        eq_(Classifier.AUDIENCE_CHILDREN, self.classifier.audience())

    def test_adults_only_threshold(self):
        # The 'adults only' weight here is not even close to a
        # majority, but it's high enough that we classify this work as
        # 'adults only' to be safe.
        self.classifier.audience_weights = {
            Classifier.AUDIENCE_ADULT : 4,
            Classifier.AUDIENCE_ADULTS_ONLY : 2,
            Classifier.AUDIENCE_CHILDREN : 4,
        }
        eq_(Classifier.AUDIENCE_ADULTS_ONLY, self.classifier.audience())
        
    def test_target_age_is_default_for_adult_books(self):
        # Target age data can't override an independently determined
        # audience.
        overdrive = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        c1 = self.identifier.classify(
            overdrive, Subject.OVERDRIVE, u"Picture Books", weight=10000
        )
        self.classifier.add(c1)

        target_age = self.classifier.target_age(Classifier.AUDIENCE_ADULT)
        eq_((18, None), target_age)

    def test_target_age_weight_scaling(self):
        # We have a weak but reliable signal that this is a book for
        # ages 5 to 7.
        overdrive = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        c1 = self.identifier.classify(
            overdrive, Subject.OVERDRIVE, u"Beginning Readers", weight=2
        )
        self.classifier.add(c1)

        # We have a louder but less reliable signal that this is a
        # book for eleven-year-olds.
        oclc = DataSource.lookup(self._db, DataSource.OCLC)
        c2 = self.identifier.classify(
            oclc, Subject.TAG, u"Grade 6", weight=3
        )
        self.classifier.add(c2)

        # Both signals make it into the dataset, but they are weighted
        # differently, and the more reliable signal becomes stronger.
        lower = self.classifier.target_age_lower_weights
        upper = self.classifier.target_age_upper_weights
        assert lower[5] > lower[11]
        assert upper[8] > upper[11]
        eq_(lower[11], upper[11])
        eq_(lower[5], upper[8])

        # And this affects the target age we choose.
        a = self.classifier.target_age(Classifier.AUDIENCE_CHILDREN)
        eq_(
            (5,8),
            self.classifier.target_age(Classifier.AUDIENCE_CHILDREN)
        )

    def test_target_age_errs_towards_wider_span(self):
        i = self._identifier()
        source = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        c1 = i.classify(source, Subject.AGE_RANGE, u"8-9", weight=1)
        c2 = i.classify(source, Subject.AGE_RANGE, u"6-7", weight=1)

        overdrive_edition, lp = self._edition(
            data_source_name=source.name, with_license_pool=True,
            identifier_id=i.identifier
        )
        self.classifier.work = self._work(presentation_edition=overdrive_edition)
        for classification in i.classifications:
            self.classifier.add(classification)
        genres, fiction, audience, target_age = self.classifier.classify()

        eq_(Classifier.AUDIENCE_CHILDREN, audience)
        eq_((6,9), target_age)

    def test_fiction_status_restricts_genre(self):
        # Classify a book to imply that it's 50% science fiction and
        # 50% history. Then call .genres() twice. With fiction=True,
        # it's 100% science fiction. With fiction=False, it's 100% history.

        # This book is classified as 50% science fiction and 50% history.
        fiction_genre = self._genre(classifier.Science_Fiction)
        nonfiction_genre = self._genre(classifier.History)
        self.classifier.genre_weights[fiction_genre] = 100
        self.classifier.genre_weights[nonfiction_genre] = 100

        # But any given book is either fiction or nonfiction. If we say this
        # book is fiction, it's classified as 100% SF.
        genres = self.classifier.genres(True)
        eq_([(fiction_genre.genredata, 100)], genres.items())

        # If we say it's nonfiction, it ends up 100% history.
        genres = self.classifier.genres(False)
        eq_([(nonfiction_genre.genredata, 100)], genres.items())

    def test_genres_consolidated_before_classification(self):
        # A book with Romance=100, Historical Romance=5, Romantic
        # Suspense=4 will be classified by .genres() as 100%
        # Historical Romance.
        historical_romance = self._genre(classifier.Historical_Romance)
        romance = self._genre(classifier.Romance)
        romantic_suspense = self._genre(classifier.Romantic_Suspense)
        nonfiction_genre = self._genre(classifier.History)

        self.classifier.genre_weights[romance] = 100

        # Give Historical Romance enough weight to 'swallow' its
        # parent genre.  (5% of the weight of its parent.)
        self.classifier.genre_weights[historical_romance] = 5

        # Romantic Suspense does pretty well but it doesn't have
        # enough weight to swallow the parent genre, and it's
        # eliminated by the low-pass filter.
        self.classifier.genre_weights[romantic_suspense] = 4

        [genre] = self.classifier.genres(True).items()        
        eq_((historical_romance.genredata, 105), genre)

        # TODO: This behavior is a little random. As in, it's
        # random which genre comes out on top.
        #
        # self.classifier.genre_weights[romantic_suspense] = 5
        # [genre] = self.classifier.genres(True).items()
        # eq_((historical_romance.genredata, 105), genre)

    def test_overdrive_juvenile_implicit_target_age(self):
        # An Overdrive book that is classified under "Juvenile" but
        # not under any more specific category is believed to have a
        # target age range of 9-12.
        i = self.identifier
        source = DataSource.lookup(self._db, DataSource.OVERDRIVE)        
        c = i.classify(source, Subject.OVERDRIVE, "Juvenile Fiction",
                       weight=1)
        self.classifier.add(c)
        self.classifier.prepare_to_classify()
        eq_([9], self.classifier.target_age_lower_weights.keys())
        eq_([12], self.classifier.target_age_upper_weights.keys())

    def test_overdrive_juvenile_explicit_target_age(self):
        # An Overdrive book that is classified under "Juvenile" and
        # also under some more specific category is believed to have
        # the target age range associated with that more specific
        # category.
        i = self.identifier
        source = DataSource.lookup(self._db, DataSource.OVERDRIVE)        
        for subject in ("Juvenile Fiction", "Picture Books"):
            c = i.classify(source, Subject.OVERDRIVE, subject, weight=1)
        self.classifier.add(c)
        self.classifier.prepare_to_classify()
        eq_([0], self.classifier.target_age_lower_weights.keys())
        eq_([4], self.classifier.target_age_upper_weights.keys())

    def test_genre_low_pass_filter(self):

        romance = self._genre(classifier.Romance)
        self.classifier.genre_weights[romance] = 100

        sf = self._genre(classifier.Science_Fiction)
        self.classifier.genre_weights[sf] = 15

        # The default cutoff value of 0.15 requires that a genre have
        # a weight of at least the total weight * 0.15 to qualify.  In
        # this case, the total weight is 115 and the cutoff weight is
        # 17.25.
        [[genre, weight]] = self.classifier.genres(True).items()
        eq_(romance.genredata, genre)

        # Increase SF's weight past the cutoff and we get both genres.
        self.classifier.genre_weights[sf] = 18

        [[g1, weight], [g2, weight]] = self.classifier.genres(True).items()
        eq_(set([g1, g2]), set([romance.genredata, sf.genredata]))

    def test_classify_sets_minimum_age_high_if_minimum_lower_than_maximum(self):

        # We somehow end up in a situation where the proposed low end
        # of the target age is higher than the proposed high end.
        self.classifier.audience_weights[Classifier.AUDIENCE_CHILDREN] = 1
        self.classifier.target_age_lower_weights[10] = 1
        self.classifier.target_age_upper_weights[4] = 1
        
        # We set the low end equal to the high end, erring on the side
        # of making the book available to fewer people.
        genres, fiction, audience, target_age = self.classifier.classify()
        eq_(10, target_age[0])
        eq_(10, target_age[1])

    def test_classify_uses_default_fiction_status(self):
        genres, fiction, audience, target_age = self.classifier.classify(default_fiction=True)
        eq_(True, fiction)
        genres, fiction, audience, target_age = self.classifier.classify(default_fiction=False)
        eq_(False, fiction)
        genres, fiction, audience, target_age = self.classifier.classify(default_fiction=None)
        eq_(None, fiction)

        # The default isn't used if there's any information about the fiction status.
        self.classifier.fiction_weights[False] = 1
        genres, fiction, audience, target_age = self.classifier.classify(default_fiction=None)
        eq_(False, fiction)

    def test_classify_uses_default_audience(self):
        genres, fiction, audience, target_age = self.classifier.classify()
        eq_(None, audience)
        genres, fiction, audience, target_age = self.classifier.classify(default_audience=Classifier.AUDIENCE_ADULT)
        eq_(Classifier.AUDIENCE_ADULT, audience)
        genres, fiction, audience, target_age = self.classifier.classify(default_audience=Classifier.AUDIENCE_CHILDREN)
        eq_(Classifier.AUDIENCE_CHILDREN, audience)

        # The default isn't used if there's any information about the audience.
        self.classifier.audience_weights[Classifier.AUDIENCE_ADULT] = 1
        genres, fiction, audience, target_age = self.classifier.classify(default_audience=None)
        eq_(Classifier.AUDIENCE_ADULT, audience)

    def test_classify(self):
        # At this point we've tested all the components of classify, so just
        # do an overall test to verify that classify() returns a 4-tuple
        # (genres, fiction, audience, target_age)

        self.work.presentation_edition.title = u"Science Fiction: A Comprehensive History"
        i = self.identifier
        source = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        c1 = i.classify(source, Subject.OVERDRIVE, u"History", weight=10)
        c2 = i.classify(source, Subject.OVERDRIVE, u"Science Fiction", weight=100)
        c3 = i.classify(source, Subject.OVERDRIVE, u"Young Adult Nonfiction", weight=100)
        for classification in i.classifications:
            self.classifier.add(classification)
        self.classifier.prepare_to_classify()

        genres, fiction, audience, target_age = self.classifier.classify()

        # This work really looks like science fiction (w=100), but it
        # looks *even more* like nonfiction (w=100+10), and science
        # fiction is not a genre of nonfiction. So this book can't be
        # science fiction. It must be history.
        eq_(u"History", genres.keys()[0].name)
        eq_(False, fiction)
        eq_(Classifier.AUDIENCE_YOUNG_ADULT, audience)
        eq_((12,17), target_age)

    def test_top_tier_values(self):
        c = Counter()
        eq_(set(), WorkClassifier.top_tier_values(c))

        c = Counter(["a"])
        eq_(set(["a"]), WorkClassifier.top_tier_values(c))

        c = Counter([1,1,1,2,2,3,4,4,4])
        eq_(set([1,4]), WorkClassifier.top_tier_values(c))
        c = Counter([1,1,1,2])
        eq_(set([1]), WorkClassifier.top_tier_values(c))

    def test_duplicate_classification_ignored(self):
        """A given classification is only used once from
        a given data source.
        """
        history = self._genre(classifier.History)
        i = self.identifier
        source = DataSource.lookup(self._db, DataSource.AMAZON)
        c1 = i.classify(source, Subject.TAG, u"History", weight=1)
        eq_([], self.classifier.classifications)

        self.classifier.add(c1)
        old_weight = self.classifier.genre_weights[history]

        c2 = i.classify(source, Subject.TAG, u"History", weight=100)
        self.classifier.add(c2)
        # No effect -- the weights are the same as before.
        eq_(old_weight, self.classifier.genre_weights[history])

        # The same classification can come in from another data source and
        # it will be taken into consideration.
        source2 = DataSource.lookup(self._db, DataSource.OCLC_LINKED_DATA)
        c3 = i.classify(source2, Subject.TAG, u"History", weight=1)
        self.classifier.add(c3)
        assert self.classifier.genre_weights[history] > old_weight

    def test_staff_genre_overrides_others(self):
        genre1, is_new = Genre.lookup(self._db, "Psychology")
        genre2, is_new = Genre.lookup(self._db, "Cooking")
        subject1 = self._subject(type="type1", identifier="subject1")
        subject1.genre = genre1
        subject2 = self._subject(type="type2", identifier="subject2")
        subject2.genre = genre2
        source = DataSource.lookup(self._db, DataSource.AXIS_360)
        staff_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        classification1 = self._classification(
            identifier=self.identifier, subject=subject1,
            data_source=source, weight=10)
        classification2 = self._classification(
            identifier=self.identifier, subject=subject2,
            data_source=staff_source, weight=1)
        self.classifier.add(classification1)
        self.classifier.add(classification2)
        (genre_weights, fiction, audience, target_age) = self.classifier.classify()
        eq_([genre2.name], [genre.name for genre in genre_weights.keys()])

    def test_staff_none_genre_overrides_others(self):
        source = DataSource.lookup(self._db, DataSource.AXIS_360)
        staff_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        genre1, is_new = Genre.lookup(self._db, "Poetry")
        subject1 = self._subject(type="type1", identifier="subject1")
        subject1.genre = genre1
        subject2 = self._subject(
            type=Subject.SIMPLIFIED_GENRE,
            identifier=SimplifiedGenreClassifier.NONE
        )
        classification1 = self._classification(
            identifier=self.identifier, subject=subject1,
            data_source=source, weight=10)
        classification2 = self._classification(
            identifier=self.identifier, subject=subject2,
            data_source=staff_source, weight=1)
        self.classifier.add(classification1)
        self.classifier.add(classification2)
        (genre_weights, fiction, audience, target_age) = self.classifier.classify()
        eq_(0, len(genre_weights.keys()))

    def test_staff_fiction_overrides_others(self):
        source = DataSource.lookup(self._db, DataSource.AXIS_360)
        staff_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        subject1 = self._subject(type="type1", identifier="Cooking")
        subject1.fiction = False
        subject2 = self._subject(type="type2", identifier="Psychology")
        subject2.fiction = False
        subject3 = self._subject(
            type=Subject.SIMPLIFIED_FICTION_STATUS,
            identifier="Fiction"
        )
        classification1 = self._classification(
            identifier=self.identifier, subject=subject1,
            data_source=source, weight=10)
        classification2 = self._classification(
            identifier=self.identifier, subject=subject2,
            data_source=source, weight=10)
        classification3 = self._classification(
            identifier=self.identifier, subject=subject3,
            data_source=staff_source, weight=1)
        self.classifier.add(classification1)
        self.classifier.add(classification2)
        self.classifier.add(classification3)
        (genre_weights, fiction, audience, target_age) = self.classifier.classify()
        eq_(True, fiction)

    def test_staff_audience_overrides_others(self):
        pool = self._licensepool(None, data_source_name=DataSource.AXIS_360)
        license_source = pool.data_source
        staff_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        subject1 = self._subject(type="type1", identifier="subject1")
        subject1.audience = "Adult"
        subject2 = self._subject(type="type2", identifier="subject2")
        subject2.audience = "Adult"
        subject3 = self._subject(
            type=Subject.FREEFORM_AUDIENCE,
            identifier="Children"
        )
        classification1 = self._classification(
            identifier=pool.identifier, subject=subject1,
            data_source=license_source, weight=10)
        classification2 = self._classification(
            identifier=pool.identifier, subject=subject2,
            data_source=license_source, weight=10)
        classification3 = self._classification(
            identifier=pool.identifier, subject=subject3,
            data_source=staff_source, weight=1)
        self.classifier.add(classification1)
        self.classifier.add(classification2)
        self.classifier.add(classification3)
        (genre_weights, fiction, audience, target_age) = self.classifier.classify()
        eq_("Children", audience)

    def test_staff_target_age_overrides_others(self):
        source = DataSource.lookup(self._db, DataSource.AXIS_360)
        staff_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        subject1 = self._subject(type="type1", identifier="subject1")
        subject1.target_age = NumericRange(6, 8, "[)")
        subject1.weight_as_indicator_of_target_age = 1
        subject2 = self._subject(type="type2", identifier="subject2")
        subject2.target_age = NumericRange(6, 8, "[)")
        subject2.weight_as_indicator_of_target_age = 1
        subject3 = self._subject(
            type=Subject.AGE_RANGE,
            identifier="10-13"
        )
        classification1 = self._classification(
            identifier=self.identifier, subject=subject1,
            data_source=source, weight=10)
        classification2 = self._classification(
            identifier=self.identifier, subject=subject2,
            data_source=source, weight=10)
        classification3 = self._classification(
            identifier=self.identifier, subject=subject3,
            data_source=staff_source, weight=1)
        self.classifier.add(classification1)
        self.classifier.add(classification2)
        self.classifier.add(classification3)
        (genre_weights, fiction, audience, target_age) = self.classifier.classify()
        eq_((10, 13), target_age)

    def test_not_inclusive_target_age(self):
        staff_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        subject = self._subject(
            type=Subject.AGE_RANGE,
            identifier="10-12"
        )
        subject.target_age = NumericRange(9, 13, "()")
        classification = self._classification(
            identifier=self.identifier, subject=subject,
            data_source=staff_source, weight=1)
        self.classifier.add(classification)
        (genre_weights, fiction, audience, target_age) = self.classifier.classify()
        eq_((10, 12), target_age)
