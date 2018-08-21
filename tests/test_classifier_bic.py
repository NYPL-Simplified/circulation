from nose.tools import eq_, set_trace
import classifier
from classifier import *
from classifier.bic import BICClassifier as BIC

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
