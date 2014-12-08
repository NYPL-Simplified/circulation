"""Test logic surrounding classification schemes."""

from nose.tools import eq_, set_trace

import classifier
from classifier import (
    Classifier,
    DeweyDecimalClassifier as DDC,
    LCCClassifier as LCC,
    LCSHClassifier as LCSH,
    OverdriveClassifier as Overdrive,
    FASTClassifier as FAST,
    KeywordBasedClassifier as Keyword,
    )

class TestClassifierLookup(object):

    def test_lookup(self):
        eq_(DDC, Classifier.lookup(Classifier.DDC))
        eq_(LCC, Classifier.lookup(Classifier.LCC))
        eq_(LCSH, Classifier.lookup(Classifier.LCSH))
        eq_(FAST, Classifier.lookup(Classifier.FAST))
        eq_(Overdrive, Classifier.lookup(Classifier.OVERDRIVE))
        eq_(None, Classifier.lookup('no-such-key'))

class TestDewey(object):

    def test_name_for(self):
        eq_("General statistics of Europe", DDC.name_for("314"))
        eq_("Biography", DDC.name_for("B"))
        eq_("Human physiology", DDC.name_for("612"))
        eq_("American speeches in English", DDC.name_for("815"))
        eq_("Juvenile Nonfiction", DDC.name_for("J"))
        eq_("Juvenile Fiction", DDC.name_for("FIC"))
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
        eq_(child, aud("FIC"))
        eq_(child, aud("Fic"))
        eq_(child, aud("E"))
        eq_(young_adult, aud("Y300"))
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

        eq_(classifier.Social_Science, c("398"))

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
        return Keyword.genre(None, Keyword.scrub_identifier(keyword))

    def test_subgenre_wins_over_genre(self):
        # Asian_History wins over History, even though they both
        # have the same number of matches, because Asian_History is more
        # specific.
        eq_(classifier.Asian_History, self.genre("asian history"))
        eq_(classifier.Asian_History, self.genre("history: asia"))

class TestNestedSubgenres(object):

    def test_parents(self):
        eq_([classifier.Romance_Erotica],
            list(classifier.Erotica.parents))

        eq_([classifier.Crime_Thrillers_Mystery, classifier.Mystery],
            list(classifier.Police_Procedurals.parents))

    def test_self_and_subgenres(self):
        # Romance and Erotica
        #  - Erotica
        #  - Romance
        #    - Contemporary Romance
        #    - etc.
        eq_(
            set([classifier.Romance_Erotica, classifier.Erotica, 
                 classifier.Romance, classifier.Contemporary_Romance,
                 classifier.Historical_Romance, classifier.Paranormal_Romance,
                 classifier.Regency_Romance, classifier.Suspense_Romance]),
            set(list(classifier.Romance_Erotica.self_and_subgenres)))

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
        # Romance & Erotica is the parent of the parent of Paranormal
        # Romance, but its weight successfully flows down into
        # Paranormal Romance.
        weights = dict()
        weights[classifier.Romance_Erotica] = 100
        weights[classifier.Paranormal_Romance] = 4
        w2 = Classifier.consolidate_weights(weights)
        eq_(104, w2[classifier.Paranormal_Romance])
        assert classifier.Romance_Erotica not in w2

    def test_consolidate_through_multiple_levels_from_multiple_sources(self):
        weights = dict()
        weights[classifier.Romance_Erotica] = 50
        weights[classifier.Romance] = 50
        weights[classifier.Paranormal_Romance] = 4
        w2 = Classifier.consolidate_weights(weights)
        eq_(104, w2[classifier.Paranormal_Romance])
        assert classifier.Romance_Erotica not in w2

    def test_consolidate_fails_when_threshold_not_met(self):
        weights = dict()
        weights[classifier.History] = 100
        weights[classifier.Middle_East_History] = 1
        w2 = Classifier.consolidate_weights(weights)
        eq_(100, w2[classifier.History])
        eq_(1, w2[classifier.Middle_East_History])


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

