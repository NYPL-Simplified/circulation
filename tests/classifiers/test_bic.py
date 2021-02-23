from ... import classifier
from ...classifier import *
from ...classifier.bic import BICClassifier as BIC

class TestBIC(object):

    def test_is_fiction(self):
        def fic(bic):
            return BIC.is_fiction(BIC.scrub_identifier(bic), None)

        assert True == fic("FCA")
        assert True == fic("YFL")
        assert False == fic("YWR")
        assert False == fic("HB")

    def test_audience(self):
        young_adult = Classifier.AUDIENCE_YOUNG_ADULT
        adult = Classifier.AUDIENCE_ADULT
        def aud(bic):
            return BIC.audience(BIC.scrub_identifier(bic), None)

        assert adult == aud("DD")
        assert young_adult == aud("YFA")

    def test_genre(self):
        def gen(bic):
            return BIC.genre(BIC.scrub_identifier(bic), None)
        assert (classifier.Art_Design ==
            gen("A"))
        assert (classifier.Art_Design ==
            gen("AB"))
        assert (classifier.Music ==
            gen("AV"))
        assert (classifier.Fantasy ==
            gen("FM"))
        assert (classifier.Economics ==
            gen("KC"))
        assert (classifier.Short_Stories ==
           gen("FYB"))
        assert (classifier.Music ==
            gen("YNC"))
        assert (classifier.European_History ==
            gen("HBJD"))
