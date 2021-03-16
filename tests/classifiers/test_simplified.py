from ... import classifier
from ...classifier import *

class TestSimplifiedGenreClassifier(object):

    def test_scrub_identifier(self):
        """The URI for a Library Simplified genre is treated the same as
        the genre itself.
        """
        sf1 = SimplifiedGenreClassifier.scrub_identifier(
            SimplifiedGenreClassifier.SIMPLIFIED_GENRE + "Science%20Fiction"
        )
        sf2 = SimplifiedGenreClassifier.scrub_identifier("Science Fiction")
        assert sf1 == sf2
        assert "Science Fiction" == sf1.original

    def test_genre(self):
        genre_name = "Space Opera"
        scrubbed = SimplifiedGenreClassifier.scrub_identifier(genre_name)

        genre = SimplifiedGenreClassifier.genre(scrubbed, genre_name, fiction=True)
        assert genre.name == globals()["genres"][genre_name].name

        genre = SimplifiedGenreClassifier.genre(scrubbed, genre_name)
        assert genre.name == globals()["genres"][genre_name].name

        genre = SimplifiedGenreClassifier.genre(scrubbed, genre_name, fiction=False)
        assert genre == None

    def test_is_fiction(self):
        genre_name = "Space Opera"
        scrubbed = SimplifiedGenreClassifier.scrub_identifier(genre_name)
        is_fiction = SimplifiedGenreClassifier.is_fiction(scrubbed, genre_name)
        assert is_fiction == True

        genre_name = "Cooking"
        scrubbed = SimplifiedGenreClassifier.scrub_identifier(genre_name)
        is_fiction = SimplifiedGenreClassifier.is_fiction(scrubbed, genre_name)
        assert is_fiction == False

        genre_name = "Fake Genre"
        scrubbed = SimplifiedGenreClassifier.scrub_identifier(genre_name)
        is_fiction = SimplifiedGenreClassifier.is_fiction(scrubbed, genre_name)
        assert is_fiction == None
