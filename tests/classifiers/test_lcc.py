from nose.tools import eq_, set_trace
from ... import classifier
from ...classifier import *
from ...classifier.lcc import LCCClassifier as LCC

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
