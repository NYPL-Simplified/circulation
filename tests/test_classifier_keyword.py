from nose.tools import eq_, set_trace
import classifier
from classifier import *
from classifier.keyword import (
    KeywordBasedClassifier as Keyword,
    LCSHClassifier as LCSH,
    FASTClassifier as FAST,
)

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

        eq_(classifier.Historical_Fiction, self.genre("Arthurian romances"))
        eq_(classifier.Romance, self.genre("Regency romances"))

    def test_audience(self):
        eq_(Classifier.AUDIENCE_YOUNG_ADULT,
            Keyword.audience(None, "Teens / Fiction"))

        eq_(Classifier.AUDIENCE_YOUNG_ADULT,
            Keyword.audience(None, "teen books"))

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

    def test_young_adult_wins_over_children(self):
        eq_(Classifier.AUDIENCE_YOUNG_ADULT,
            Keyword.audience(None, "children's books - young adult fiction")
        )

    def test_juvenile_romance_means_young_adult(self):
        eq_(Classifier.AUDIENCE_YOUNG_ADULT,
            Keyword.audience(None, "juvenile fiction / love & romance")
        )

        eq_(Classifier.AUDIENCE_YOUNG_ADULT,
            Keyword.audience(None, "teenage romance")
        )

    def test_audience_match(self):
        (audience, match) = Keyword.audience_match("teen books")
        eq_(Classifier.AUDIENCE_YOUNG_ADULT, audience)
        eq_("teen books", match)

        # This is a search for a specific example so it doesn't match
        (audience, match) = Keyword.audience_match("teen romance")
        eq_(None, audience)

    def test_genre_match(self):
        (genre, match) = Keyword.genre_match("pets")
        eq_(classifier.Pets, genre)
        eq_("pets", match)

        # This is a search for a specific example so it doesn't match
        (genre, match) = Keyword.genre_match("cats")
        eq_(None, genre)

    def test_improvements(self):
        """A place to put tests for miscellaneous improvements added
        since the original work.
        """
        # was Literary Fiction
        eq_(classifier.Science_Fiction,
            Keyword.genre(None, "Science Fiction - General")
        )

        # Was General Fiction (!)
        eq_(classifier.Science_Fiction,
            Keyword.genre(None, "Science Fiction")
        )

        eq_(classifier.Science_Fiction,
            Keyword.genre(None, "Speculative Fiction")
        )

        eq_(classifier.Social_Sciences,
            Keyword.genre(None, "Social Sciences")
        )

        eq_(classifier.Social_Sciences,
            Keyword.genre(None, "Social Science")
        )

        eq_(classifier.Social_Sciences,
            Keyword.genre(None, "Human Science")
        )

        # was genreless
        eq_(classifier.Short_Stories,
            Keyword.genre(None, "Short Stories")
        )

        # was Military History
        eq_(classifier.Military_SF,
            Keyword.genre(None, "Interstellar Warfare")
        )

        # was Fantasy
        eq_(classifier.Games,
            Keyword.genre(None, "Games / Role Playing & Fantasy")
        )

        # This isn't perfect but it covers most cases.
        eq_(classifier.Media_Tie_in_SF,
            Keyword.genre(None, "TV, Movie, Video game adaptations")
        )

        # Previously only 'nonfiction' was recognized.
        eq_(False, Keyword.is_fiction(None, "Non-Fiction"))
        eq_(False, Keyword.is_fiction(None, "Non Fiction"))

        # "Historical" on its own means historical fiction, but a
        # string containing "Historical" does not mean anything in
        # particular.
        eq_(classifier.Historical_Fiction, Keyword.genre(None, "Historical"))
        eq_(None, Keyword.genre(None, "Historicals"))

        # The Fiction/Urban classification is different from the
        # African-American-focused "Urban Fiction" classification.
        eq_(None, Keyword.genre(None, "Fiction/Urban"))

        eq_(classifier.Folklore, Keyword.genre(None, "fables"))
