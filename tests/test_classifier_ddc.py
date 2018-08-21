from nose.tools import eq_, set_trace
from classifier.ddc import DeweyDecimalClassifier as DDC
from classifier import *
import classifier

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
