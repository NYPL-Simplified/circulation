from nose.tools import eq_, set_trace
import classifier
from classifier import *

class TestSimplifiedGenreClassifier(object):

    def test_scrub_identifier(self):
        """The URI for a Library Simplified genre is treated the same as
        the genre itself.
        """
        sf1 = SimplifiedGenreClassifier.scrub_identifier(
            SimplifiedGenreClassifier.SIMPLIFIED_GENRE + "Science%20Fiction"
        )
        sf2 = SimplifiedGenreClassifier.scrub_identifier("Science Fiction")
        eq_(sf1, sf2)
        eq_("Science Fiction", sf1.original)

    def test_genre(self):
        genre_name = "Space Opera"
        scrubbed = SimplifiedGenreClassifier.scrub_identifier(genre_name)

        genre = SimplifiedGenreClassifier.genre(scrubbed, genre_name, fiction=True)
        eq_(genre.name, globals()["genres"][genre_name].name)

        genre = SimplifiedGenreClassifier.genre(scrubbed, genre_name)
        eq_(genre.name, globals()["genres"][genre_name].name)

        genre = SimplifiedGenreClassifier.genre(scrubbed, genre_name, fiction=False)
        eq_(genre, None)

    def test_is_fiction(self):
        genre_name = "Space Opera"
        scrubbed = SimplifiedGenreClassifier.scrub_identifier(genre_name)
        is_fiction = SimplifiedGenreClassifier.is_fiction(scrubbed, genre_name)
        eq_(is_fiction, True)

        genre_name = "Cooking"
        scrubbed = SimplifiedGenreClassifier.scrub_identifier(genre_name)
        is_fiction = SimplifiedGenreClassifier.is_fiction(scrubbed, genre_name)
        eq_(is_fiction, False)

        genre_name = "Fake Genre"
        scrubbed = SimplifiedGenreClassifier.scrub_identifier(genre_name)
        is_fiction = SimplifiedGenreClassifier.is_fiction(scrubbed, genre_name)
        eq_(is_fiction, None)
