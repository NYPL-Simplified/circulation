from ... import classifier
from ...classifier import *
from ...classifier.lcc import LCCClassifier as LCC

class TestLCC(object):

    def test_name_for(self):

        child = Classifier.AUDIENCE_CHILDREN
        adult = Classifier.AUDIENCE_ADULT

        assert "LANGUAGE AND LITERATURE" == LCC.name_for("P")
        assert "English literature" == LCC.name_for("PR")
        assert "Fiction and juvenile belles lettres" == LCC.name_for("PZ")
        assert "HISTORY OF THE AMERICAS" == LCC.name_for("E")
        assert 'Literature (General)' == LCC.name_for("PN")
        assert None == LCC.name_for("no-such-key")

    def test_audience(self):
        child = Classifier.AUDIENCE_CHILDREN
        adult = Classifier.AUDIENCE_ADULT
        young_adult = Classifier.AUDIENCE_YOUNG_ADULT

        def aud(identifier):
            return LCC.audience(LCC.scrub_identifier(identifier), None)

        assert child == aud("PZ")
        assert child == aud("PZ2384 M68 2003")
        assert child == aud("pz2384 m68 2003")

        # We could derive audience=Adult from this, but we've seen
        # this go wrong, and it's not terribly important overall, so
        # we don't.
        assert None == aud("PR")
        assert None == aud("P")
        assert None == aud("PA")
        assert None == aud("J821.8 CARRIKK")


    def test_is_fiction(self):
        def fic(lcc):
            return LCC.is_fiction(LCC.scrub_identifier(lcc), None)
        assert False == fic("A")
        assert False == fic("AB")
        assert False == fic("PA")
        assert True == fic("P")
        assert True == fic("p")
        assert True == fic("PN")
        assert True == fic("PQ")
        assert True == fic("PR")
        assert True == fic("PS")
        assert True == fic("PT")
        assert True == fic("PZ")
        assert True == fic("PZ2384 M68 2003")
