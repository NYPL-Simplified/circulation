from ...classifier.ddc import DeweyDecimalClassifier as DDC
from ...classifier import *
from ... import classifier

class TestDewey(object):

    def test_name_for(self):
        assert "General statistics of Europe" == DDC.name_for("314")
        assert "Biography" == DDC.name_for("B")
        assert "Human physiology" == DDC.name_for("612")
        assert "American speeches in English" == DDC.name_for("815")
        assert "Juvenile Nonfiction" == DDC.name_for("J")
        assert "Fiction" == DDC.name_for("FIC")
        assert None == DDC.name_for("Fic")

    def test_audience(self):

        child = Classifier.AUDIENCE_CHILDREN
        adult = Classifier.AUDIENCE_ADULT
        young_adult = Classifier.AUDIENCE_YOUNG_ADULT

        def aud(identifier):
            return DDC.audience(*DDC.scrub_identifier(identifier))

        assert child == aud("JB")
        assert child == aud("J300")
        assert child == aud("NZJ300")
        assert child == aud("E")
        assert young_adult == aud("Y300")
        assert None == aud("FIC")
        assert None == aud("Fic")


        # We could derive audience=Adult from the lack of a
        # distinguishing "J" or "E" here, but we've seen this go
        # wrong, and it's not terribly important overall, so we don't.
        assert None == aud("B")
        assert None == aud("400")


    def test_is_fiction(self):

        def fic(identifier):
            return DDC.is_fiction(*DDC.scrub_identifier(identifier))

        assert True == fic("FIC")
        assert True == fic("E")
        assert True == fic(813)

        assert False == fic("JB")
        assert False == fic("400")
        assert False == fic("616.9940092")
        assert False == fic(615)
        assert False == fic(800)
        assert False == fic(814)

    def test_classification(self):
        def c(identifier):
            i, name = DDC.scrub_identifier(identifier)
            assert name == None
            return DDC.genre(i, None)

        assert classifier.Folklore == c("398")
