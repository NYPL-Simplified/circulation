from ... import classifier
from ...classifier import *
from ...classifier.keyword import (
    KeywordBasedClassifier as Keyword,
    LCSHClassifier as LCSH,
    FASTClassifier as FAST,
)

class TestLCSH(object):

    def test_is_fiction(self):
        def fic(lcsh):
            return LCSH.is_fiction(None, LCSH.scrub_name(lcsh))

        assert True == fic("Science fiction")
        assert True == fic("Science fiction, American")
        assert True == fic("Fiction")
        assert True == fic("Historical fiction")
        assert True == fic("Biographical fiction")
        assert True == fic("Detective and mystery stories")
        assert True == fic("Horror tales")
        assert True == fic("Classical literature")
        assert False == fic("History and criticism")
        assert False == fic("Biography")
        assert None == fic("Kentucky")
        assert None == fic("Social life and customs")


    def test_audience(self):
        child = Classifier.AUDIENCE_CHILDREN
        def aud(lcsh):
            return LCSH.audience(None, LCSH.scrub_name(lcsh))

        assert child == aud("Children's stories")
        assert child == aud("Picture books for children")
        assert child == aud("Juvenile fiction")
        assert child == aud("Juvenile poetry")
        assert None == aud("Juvenile delinquency")
        assert None == aud("Runaway children")
        assert None == aud("Humor")

class TestKeyword(object):
    def genre(self, keyword):
        scrub = Keyword.scrub_identifier(keyword)
        fiction = Keyword.is_fiction(None, scrub)
        audience = Keyword.audience(None, scrub)
        return Keyword.genre(None, scrub, fiction, audience)

    def test_higher_tier_wins(self):
        assert classifier.Space_Opera == self.genre("space opera")
        assert classifier.Drama == self.genre("opera")

        assert classifier.Historical_Fiction == self.genre("Arthurian romances")
        assert classifier.Romance == self.genre("Regency romances")

    def test_audience(self):
        assert (Classifier.AUDIENCE_YOUNG_ADULT ==
            Keyword.audience(None, "Teens / Fiction"))

        assert (Classifier.AUDIENCE_YOUNG_ADULT ==
            Keyword.audience(None, "teen books"))

    def test_subgenre_wins_over_genre(self):
        # Asian_History wins over History, even though they both
        # have the same number of matches, because Asian_History is more
        # specific.
        assert classifier.Asian_History == self.genre("asian history")
        assert classifier.Asian_History == self.genre("history: asia")

    def test_classification_may_depend_on_fiction_status(self):
        assert classifier.Humorous_Nonfiction == self.genre("Humor (Nonfiction)")
        assert classifier.Humorous_Fiction == self.genre("Humorous stories")

    def test_children_audience_implies_no_genre(self):
        assert None == self.genre("Children's Books")

    def test_young_adult_wins_over_children(self):
        assert (Classifier.AUDIENCE_YOUNG_ADULT ==
            Keyword.audience(None, "children's books - young adult fiction"))

    def test_juvenile_romance_means_young_adult(self):
        assert (Classifier.AUDIENCE_YOUNG_ADULT ==
            Keyword.audience(None, "juvenile fiction / love & romance"))

        assert (Classifier.AUDIENCE_YOUNG_ADULT ==
            Keyword.audience(None, "teenage romance"))

    def test_audience_match(self):
        (audience, match) = Keyword.audience_match("teen books")
        assert Classifier.AUDIENCE_YOUNG_ADULT == audience
        assert "teen books" == match

        # This is a search for a specific example so it doesn't match
        (audience, match) = Keyword.audience_match("teen romance")
        assert None == audience

    def test_genre_match(self):
        (genre, match) = Keyword.genre_match("pets")
        assert classifier.Pets == genre
        assert "pets" == match

        # This is a search for a specific example so it doesn't match
        (genre, match) = Keyword.genre_match("cats")
        assert None == genre

    def test_improvements(self):
        """A place to put tests for miscellaneous improvements added
        since the original work.
        """
        # was Literary Fiction
        assert (classifier.Science_Fiction ==
            Keyword.genre(None, "Science Fiction - General"))

        # Was General Fiction (!)
        assert (classifier.Science_Fiction ==
            Keyword.genre(None, "Science Fiction"))

        assert (classifier.Science_Fiction ==
            Keyword.genre(None, "Speculative Fiction"))

        assert (classifier.Social_Sciences ==
            Keyword.genre(None, "Social Sciences"))

        assert (classifier.Social_Sciences ==
            Keyword.genre(None, "Social Science"))

        assert (classifier.Social_Sciences ==
            Keyword.genre(None, "Human Science"))

        # was genreless
        assert (classifier.Short_Stories ==
            Keyword.genre(None, "Short Stories"))

        # was Military History
        assert (classifier.Military_SF ==
            Keyword.genre(None, "Interstellar Warfare"))

        # was Fantasy
        assert (classifier.Games ==
            Keyword.genre(None, "Games / Role Playing & Fantasy"))

        # This isn't perfect but it covers most cases.
        assert (classifier.Media_Tie_in_SF ==
            Keyword.genre(None, "TV, Movie, Video game adaptations"))

        # Previously only 'nonfiction' was recognized.
        assert False == Keyword.is_fiction(None, "Non-Fiction")
        assert False == Keyword.is_fiction(None, "Non Fiction")

        # "Historical" on its own means historical fiction, but a
        # string containing "Historical" does not mean anything in
        # particular.
        assert classifier.Historical_Fiction == Keyword.genre(None, "Historical")
        assert None == Keyword.genre(None, "Historicals")

        # The Fiction/Urban classification is different from the
        # African-American-focused "Urban Fiction" classification.
        assert None == Keyword.genre(None, "Fiction/Urban")

        assert classifier.Folklore == Keyword.genre(None, "fables")
