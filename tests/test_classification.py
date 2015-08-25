"""Test logic surrounding classification schemes."""

from nose.tools import eq_, set_trace

from .. import classifier
from ..classifier import (
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
    InterestLevelClassifier,
    Axis360AudienceClassifier,
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
        eq_((9,12), f("9-12"))
        eq_((9,9), f("9 years"))
        eq_((9,12), f("9 - 12 years"))
        eq_((12,14), f("12 - 14"))
        eq_((0,3), f("0-3"))
        eq_((None,None), f("K-3"))

        eq_((None,None), AgeClassifier.target_age("K-3", None, True))
        eq_((None,None), AgeClassifier.target_age("9-12", None, True))
        eq_((9,11), AgeClassifier.target_age("9 and up", None, True))

    def test_age_from_keyword_classifier(self):
        def f(t):
            return LCSH.target_age(t, None)
        eq_((5,5), f("Interest age: from c 5 years"))
        eq_((9,12), f("Children's Books / 9-12 Years"))
        eq_((9,12), f("Ages 9-12"))
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

    def test_genre(self):
        def gen(bisac):
            return BISAC.genre(BISAC.scrub_identifier(bisac), None)
        eq_(classifier.Adventure, 
            gen("JUVENILE FICTION / Action & Adventure / General"))
        eq_(classifier.Erotica, gen("FICTION / Erotica"))
        eq_(classifier.Religion_Spirituality, 
            gen("RELIGION / Biblical Studies / Prophecy"))

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
        w2 = Classifier.consolidate_weights(weights)
        eq_(14, w2[classifier.Asian_History])
        eq_(1, w2[classifier.Middle_East_History])
        assert classifier.History not in w2

        # Paranormal Romance is a subcategory of Romance, which is itself
        # a subcategory.
        weights = dict()
        weights[classifier.Romance] = 100
        weights[classifier.Paranormal_Romance] = 4
        w2 = Classifier.consolidate_weights(weights)
        eq_(104, w2[classifier.Paranormal_Romance])
        assert classifier.Romance not in w2

    def test_consolidate_through_multiple_levels(self):
        # Romance is the parent of the parent of Paranormal
        # Romance, but its weight successfully flows down into
        # Paranormal Romance.
        weights = dict()
        weights[classifier.Romance] = 100
        weights[classifier.Paranormal_Romance] = 4
        w2 = Classifier.consolidate_weights(weights)
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
        # w2 = Classifier.consolidate_weights(weights)
        # eq_(104, w2[classifier.Paranormal_Romance])
        # assert classifier.Romance not in w2
        pass

    def test_consolidate_fails_when_threshold_not_met(self):
        weights = dict()
        weights[classifier.History] = 100
        weights[classifier.Middle_East_History] = 1
        w2 = Classifier.consolidate_weights(weights)
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

# TODO: This needs to be moved into model I guess?

# class TestClassifier(object):


#     def test_misc(self):
#         adult = Classifier.AUDIENCE_ADULT
#         child = Classifier.AUDIENCE_CHILDREN

#         data = {"DDC": [{"id": "813.4", "weight": 137}], "LCC": [{"id": "PR9199.2.B356", "weight": 48}], "FAST": [{"weight": 103, "id": "1719440", "value": "Mackenzie, Alexander, 1764-1820"}, {"weight": 25, "id": "969633", "value": "Indians of North America"}, {"weight": 22, "id": "1064447", "value": "Pioneers"}, {"weight": 17, "id": "918556", "value": "Explorers"}, {"weight": 17, "id": "936416", "value": "Fur traders"}, {"weight": 17, "id": "987694", "value": "Kings and rulers"}, {"weight": 7, "id": "797462", "value": "Adventure stories"}, {"weight": 5, "id": "1241420", "value": "Rocky Mountains"}]}
#         classified = Classifier.classify(data, True)

#         # This is pretty clearly fiction intended for an adult
#         # audience.
#         assert classified['audience'][Classifier.AUDIENCE_ADULT] > 0.6
#         assert classified['audience'][Classifier.AUDIENCE_CHILDREN] == 0
#         assert classified['fiction'][True] > 0.6
#         assert classified['fiction'][False] == 0

#         # Its LCC classifications are heavy on the literature.
#         names = classified['names']
#         eq_(0.5, names['LCC']['LANGUAGE AND LITERATURE'])
#         eq_(0.5, names['LCC']['English literature'])

#         # Alexander Mackenzie is more closely associated with this work
#         # than the Rocky Mountains.
#         assert (names['FAST']['Mackenzie, Alexander, 1764-1820'] > 
#                 names['FAST']['Rocky Mountains'])

#         # But the Rocky Mountains ain't chopped liver.
#         assert names['FAST']['Rocky Mountains'] > 0

#     def test_keyword_based_classification(self):

#         adult = Classifier.AUDIENCE_ADULT
#         child = Classifier.AUDIENCE_CHILDREN

#         classifier = Keyword

#         genre, audience, is_fiction = classifier.classify(
#             "World War, 1914-1918 -- Pictorial works")

#         set_trace()

#         genre, audience, is_fiction = Classifier.classify(
#             "Illustrated books")

#         # We're not sure it's nonfiction, but we have no indication
#         # whatsoever that it's fiction.
#         assert classified['fiction'][True] == 0
#         assert classified['fiction'][False] > 0.3

#         # We're not sure its for adults, but we have no indication
#         # whatsoever that it's for children.
#         assert classified['audience'][child] == 0
#         assert classified['audience'][adult] > 0.3

#         # It's more closely associated with "World War, 1914-1918"
#         # than with any other LCSH classification.
#         champ = None
#         for k, v in classified['codes']['LCSH'].items():
#             if not champ or v > champ[1]:
#                 champ = (k,v)
#         eq_(champ, ("World War, 1914-1918", 0.5))

