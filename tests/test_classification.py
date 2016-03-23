"""Test logic surrounding classification schemes."""

from nose.tools import eq_, set_trace
from . import DatabaseTest
from collections import Counter
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
    BISACClassifier as BISAC,
    LCSHClassifier as LCSH,
    OverdriveClassifier as Overdrive,
    FASTClassifier as FAST,
    KeywordBasedClassifier as Keyword,
    GradeLevelClassifier,
    AgeClassifier,
    AgeOrGradeClassifier,
    InterestLevelClassifier,
    Axis360AudienceClassifier,
    WorkClassifier,
    )

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


class TestClassifierLookup(object):

    def test_lookup(self):
        eq_(DDC, Classifier.lookup(Classifier.DDC))
        eq_(LCC, Classifier.lookup(Classifier.LCC))
        eq_(LCSH, Classifier.lookup(Classifier.LCSH))
        eq_(FAST, Classifier.lookup(Classifier.FAST))
        eq_(GradeLevelClassifier, Classifier.lookup(Classifier.GRADE_LEVEL))
        eq_(AgeClassifier, Classifier.lookup(Classifier.AGE_RANGE))
        eq_(InterestLevelClassifier, Classifier.lookup(Classifier.INTEREST_LEVEL))
        eq_(Overdrive, Classifier.lookup(Classifier.OVERDRIVE))
        eq_(None, Classifier.lookup('no-such-key'))

class TestTargetAge(object):
    def test_age_from_grade_classifier(self):
        def f(t):
            return GradeLevelClassifier.target_age(t, None)
        eq_((5,6), GradeLevelClassifier.target_age(None, "grades 0-1"))
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
        eq_((17,17), f("12th grade"))

        # target_age() will assume that a number it sees is talking
        # about a grade level, unless require_explicit_grade_marker is
        # True.
        eq_((7,9), GradeLevelClassifier.target_age("2-4", None, False))
        eq_((None,None), GradeLevelClassifier.target_age("2-4", None, True))
        eq_((14,17), f("Children's Audio - 9-12"))
        eq_((None,None), GradeLevelClassifier.target_age(
            "Children's Audio - 9-12", None, True))

        eq_((None,None), GradeLevelClassifier.target_age("grade 50", None))
        eq_((None,None), GradeLevelClassifier.target_age("road grades -- history", None))
        eq_((None,None), GradeLevelClassifier.target_age(None, None))

    def test_age_from_age_classifier(self):
        def f(t):
            return AgeClassifier.target_age(t, None)
        eq_((9,12), f("Ages 9-12"))
        eq_((9,11), f("9 and up"))
        eq_((9,11), f("9 and up."))
        eq_((9,11), f("9+"))
        eq_((9,11), f("9+."))
        eq_((None,None), f("900-901"))
        eq_((9,12), f("9-12"))
        eq_((9,9), f("9 years"))
        eq_((9,12), f("9 - 12 years"))
        eq_((12,14), f("12 - 14"))
        eq_((12,14), f("14 - 12"))
        eq_((0,3), f("0-3"))
        eq_((None,None), f("K-3"))

        eq_((None,None), AgeClassifier.target_age("K-3", None, True))
        eq_((None,None), AgeClassifier.target_age("9-12", None, True))
        eq_((9,11), AgeClassifier.target_age("9 and up", None, True))
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
            return DDC.audience(DDC.scrub_identifier(identifier), None)

        eq_(child, aud("JB"))
        eq_(child, aud("J300"))
        eq_(child, aud("NZJ300"))
        eq_(child, aud("E"))
        eq_(young_adult, aud("Y300"))
        eq_(adult, aud("FIC"))
        eq_(adult, aud("Fic"))
        eq_(adult, aud("B"))
        eq_(adult, aud("400"))


    def test_is_fiction(self):

        def fic(identifier):
            return DDC.is_fiction(DDC.scrub_identifier(identifier), None)

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
            i = DDC.scrub_identifier(identifier)
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

class TestBISAC(object):

    def test_is_fiction(self):
        def fic(bisac):
            return BISAC.is_fiction(BISAC.scrub_identifier(bisac), None)

        eq_(True, fic("FICTION / Classics"))
        eq_(True, fic("JUVENILE FICTION / Concepts / Date & Time"))
        eq_(False, fic("HISTORY / General"))


    def test_audience(self):

        young_adult = Classifier.AUDIENCE_YOUNG_ADULT
        adult = Classifier.AUDIENCE_ADULT
        def aud(bisac):
            return BISAC.audience(BISAC.scrub_identifier(bisac), None)
            
        eq_(adult, aud("FAMILY & RELATIONSHIPS / Love & Romance"))
        eq_(young_adult, aud("JUVENILE FICTION / Action & Adventure / General"))

    def test_default_age_range_for_audience(self):
        class DummySubject(object):
            def __init__(self, x):
                self.identifier = x
                self.name = None

        def target(bisac):
            dummy = DummySubject(bisac)
            return BISAC.classify(dummy)[2]
        
        eq_((14,17), target("JUVENILE FICTION / Action & Adventure / General"))
        eq_((18, None), target("Erotica / General"))

    def test_genre(self):
        def gen(bisac):
            return BISAC.genre(BISAC.scrub_identifier(bisac), None)
        eq_(classifier.Adventure, 
            gen("JUVENILE FICTION / Action & Adventure / General"))
        eq_(classifier.Erotica, gen("FICTION / Erotica"))
        eq_(classifier.Religion_Spirituality, 
            gen("RELIGION / Biblical Studies / Prophecy"))

        eq_(classifier.Dystopian_SF,
            gen("JUVENILE FICTION / Dystopian")
        )

        eq_(classifier.Folklore,
            gen("JUVENILE FICTION / Fairy Tales & Folklore / General")
        )

        eq_(classifier.Folklore,
            gen("JUVENILE FICTION / Legends, Myths, Fables / General")
        )

        eq_(classifier.Life_Strategies, 
            gen("JUVENILE NONFICTION / Social Issues / Friendship")
        )


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


class TestOverdriveClassifier(object):

    def test_foreign_languages(self):
        eq_("Foreign Language Study", 
            Overdrive.scrub_identifier("Foreign Language Study - Italian"))

    def test_target_age(self):
        a = Overdrive.target_age
        eq_((0,4), a("Picture Book Nonfiction", None))
        eq_((5,8), a("Beginning Reader", None))
        eq_((None,None), a("Fiction", None))


class TestWorkClassifier(DatabaseTest):

    def setup(self):
        super(TestWorkClassifier, self).setup()
        self.work = self._work(with_license_pool=True)
        self.identifier = self.work.primary_edition.primary_identifier
        self.classifier = WorkClassifier(self.work, test_session=self._db)

    def _genre(self, genre_data):
        expected_genre, ignore = Genre.lookup(self._db, genre_data.name)
        return expected_genre

    def test_weight_metadata_title(self):
        self.work.primary_edition.title = u"Star Trek: The Book"
        expected_genre = self._genre(classifier.Media_Tie_in_SF)
        self.classifier.weigh_metadata()
        eq_(100, self.classifier.genre_weights[expected_genre])

    def test_weight_metadata_publisher(self):
        # Genre publisher and imprint
        self.work.primary_edition.publisher = u"Harlequin"
        expected_genre = self._genre(classifier.Romance)
        self.classifier.weigh_metadata()
        eq_(100, self.classifier.genre_weights[expected_genre])

    def test_weight_metadata_imprint(self):
        # Imprint is more specific than publisher, so it takes precedence.
        self.work.primary_edition.publisher = u"Harlequin"
        self.work.primary_edition.imprint = u"Harlequin Intrigue"
        expected_genre = self._genre(classifier.Romantic_Suspense)
        general_romance = self._genre(classifier.Romance)

        self.classifier.weigh_metadata()
        assert general_romance not in self.classifier.genre_weights
        eq_(100, self.classifier.genre_weights[expected_genre])

    def test_metadata_implies_audience_and_genre(self):
        # Genre and audience publisher 
        self.work.primary_edition.publisher = u"Harlequin"
        self.work.primary_edition.imprint = u"Harlequin Teen"
        expected_genre = self._genre(classifier.Romance)

        self.classifier.weigh_metadata()
        eq_(100, self.classifier.genre_weights[expected_genre])
        eq_(100, self.classifier.audience_weights[Classifier.AUDIENCE_YOUNG_ADULT])

    def test_metadata_implies_fiction_status(self):
        self.work.primary_edition.publisher = u"Harlequin"
        self.work.primary_edition.imprint = u"Harlequin Nonfiction"
        self.classifier.weigh_metadata()

        eq_(100, self.classifier.fiction_weights[False])
        assert True not in self.classifier.fiction_weights

    def test_publisher_excludes_adult_audience(self):
        # We don't know if this is a children's book or a young adult
        # book, but we're confident it's not a book for adults.
        self.work.primary_edition.publisher = u"Scholastic Inc."

        self.classifier.weigh_metadata()
        eq_(-100, self.classifier.audience_weights[Classifier.AUDIENCE_ADULT])
        eq_(-100, self.classifier.audience_weights[Classifier.AUDIENCE_ADULTS_ONLY])

    def test_imprint_excludes_adult_audience(self):
        self.work.primary_edition.imprint = u"Delacorte Books for Young Readers"

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

    def test_default_nonfiction(self):
        # In the absence of any information we assume a book is nonfiction.
        eq_(False, self.classifier.fiction)

        # Put a tiny bit of evidence on the scale, and the balance tips.
        new_classifier = WorkClassifier(self.work, test_session=self._db) 
        source = DataSource.lookup(self._db, DataSource.OCLC)
        c = self.identifier.classify(source, Subject.TAG, u"Fiction", weight=1)
        new_classifier.add(c)
        eq_(True, new_classifier.fiction)

    def test_adult_book_by_default(self):
        eq_(Classifier.AUDIENCE_ADULT, self.classifier.audience())

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
        # buckets, but not enough evidence in the 'children' bucket to
        # be confident.

        self.classifier.audience_weights = {
            Classifier.AUDIENCE_ADULT : 0,
            Classifier.AUDIENCE_ADULTS_ONLY : 0,
            Classifier.AUDIENCE_CHILDREN : 10,
        }
        eq_(Classifier.AUDIENCE_ADULT, self.classifier.audience())

        # Now we're confident.
        self.classifier.audience_weights[Classifier.AUDIENCE_CHILDREN] = 11
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

    def test_most_reliable_target_age_subset(self):
        # We have a very weak but reliable signal that this is a book for
        # young children.
        overdrive = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        c1 = self.identifier.classify(
            overdrive, Subject.OVERDRIVE, u"Picture Books", weight=1
        )
        self.classifier.add(c1)

        # We have a very strong but unreliable signal that this is a
        # book for slightly older children.
        oclc = DataSource.lookup(self._db, DataSource.OCLC)
        c2 = self.identifier.classify(
            oclc, Subject.TAG, u"Grade 5", weight=10000
        )
        self.classifier.add(c2)

        # Only the reliable signal makes it into
        # most_reliable_target_age_subset.
        subset = self.classifier.most_reliable_target_age_subset
        eq_([c1], subset)

        # And only most_reliable_target_age_subset is used to calculate
        # the target age.
        eq_((0,3),  self.classifier.target_age(Classifier.AUDIENCE_CHILDREN))

    def test_target_age_errs_towards_wider_span(self):
        i = self._identifier()
        source = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        c1 = i.classify(source, Subject.AGE_RANGE, u"8-9", weight=1)
        c2 = i.classify(source, Subject.AGE_RANGE, u"6-7", weight=1)

        overdrive_edition, lp = self._edition(
            data_source_name=source.name, with_license_pool=True,
            identifier_id=i.identifier
        )
        self.classifier.work = self._work(primary_edition=overdrive_edition)
        for classification in i.classifications:
            self.classifier.add(classification)
        genres, fiction, audience, target_age = self.classifier.classify

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

    def test_classify(self):
        # At this point we've tested all the components of classify, so just
        # do an overall test to verify that classify() returns a 4-tuple
        # (genres, fiction, audience, target_age)

        self.work.primary_edition.title = u"Science Fiction: A Comprehensive History"
        i = self.identifier
        source = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        c1 = i.classify(source, Subject.OVERDRIVE, u"History", weight=10)
        c2 = i.classify(source, Subject.OVERDRIVE, u"Science Fiction", weight=100)
        c3 = i.classify(source, Subject.OVERDRIVE, u"Young Adult Nonfiction", weight=100)
        for classification in i.classifications:
            self.classifier.add(classification)
        self.classifier.prepare_to_classify()

        self.classifier.audience

        genres, fiction, audience, target_age = self.classifier.classify

        # This work really looks like science fiction (w=100), but it
        # looks *even more* like nonfiction (w=100+10), and science
        # fiction is not a genre of nonfiction. So this book can't be
        # science fiction. It must be history.
        eq_(u"History", genres.keys()[0].name)
        eq_(False, fiction)
        eq_(Classifier.AUDIENCE_YOUNG_ADULT, audience)
        eq_((14,17), target_age)

    def test_top_tier_values(self):
        c = Counter()
        eq_(set(), WorkClassifier.top_tier_values(c))

        c = Counter(["a"])
        eq_(set(["a"]), WorkClassifier.top_tier_values(c))

        c = Counter([1,1,1,2,2,3,4,4,4])
        eq_(set([1,4]), WorkClassifier.top_tier_values(c))
        c = Counter([1,1,1,2])
        eq_(set([1]), WorkClassifier.top_tier_values(c))

