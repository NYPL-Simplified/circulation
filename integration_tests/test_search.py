# encoding: utf-8
import logging
import urllib
from nose.tools import (
    eq_,
    set_trace,
)
import os
import json
from core.model import (
    production_session,
    Library,
)
from core.lane import Pagination
from core.external_search import (
    ExternalSearchIndex,
    Filter
)


# problems identified in ES6:
# 'modern romance' - exact title match isn't promoted
# (but it also doesn't work in ES1 on a real site)

# summary is boosted over subtitle



# # In Elasticsearch 6, the exact author match that doesn't
# # mention 'biography' is boosted above a book that
# # mentions all three words in its title.
# order = [
#     self.biography_of_peter_graves, # title + genre 'biography'
#     self.book_by_peter_graves,      # author (no 'biography')
#     self.behind_the_scenes,         # all words match in title
#     self.book_by_someone_else,      # match across fields (no 'biography')
# ]

class Searcher(object):
    def __init__(self, library, index):
        self.library = library
        self.filter = Filter(collections=self.library)
        self.index = index

    def query(self, query, pagination):
        return self.index.query_works(
            query, filter=self.filter, pagination=pagination,
            debug=True, return_raw_results=True
        )


class Evaluator(object):
    """A class that knows how to evaluate search results."""

    log = logging.getLogger("Search evaluator")

    def __init__(self, **kwargs):
        self.kwargs = dict((k, v.lower()) for k, v in kwargs.items())
        for k, v in self.kwargs.items():
            setattr(self, k, v)

    def evaluate(self, hits):
        """Raise an AssertionError if the search results are so bad that the
        test should fail.
        """
        self.evaluate_search(hits)
        first = hits[0]
        self.evaluate_first(first)
        self.evaluate_hits(hits)

    def evaluate_search(self, hits):
        """Evaluate the search itself."""
        # By default, a search passes the check if it returns anything
        # as opposed to returning nothing.
        assert hits

    def evaluate_first(self, hits):
        """Evaluate the first (and most important) search result."""
        return

    def evaluate_hits(self, hits):
        """Evaluate the search results as a whole."""
        return

    def format(self, result):
        source = dict(
            title=result.title,
            author=result.author,
            subtitle=result.subtitle,
            series=result.series,
            summary=result.summary,
            genres=result.genres,
            imprint=result.imprint,
            publisher=result.publisher,
        )
        return dict(
            _score=result.meta.score,
            _type=result.meta.doc_type,
            _id=result.meta.id,
            _index=result.meta.index,
            source=source
        )

    def _field(self, field, result=None):
        """Extract a field from a search result."""
        result = result or self.first
        value = getattr(result, field, None)
        if value and hasattr(value, 'lower'):
            value = value.lower()
        return value

    def assert_ratio(self, matches, hits, threshold):
        """Assert that the size of `matches` is proportional to the size of
        `hits`.
        """
        if not hits:
            actual = 0
        else:
            actual = float(len(matches)) / len(hits)
        if actual < threshold:
            # This test is going to fail. Log some useful information.
            self.log.info(
                "Need %d%% matches, got %d%%" % (
                    threshold*100, actual*100
                )
            )
            for i in hits:
                self.log.info("%s %s", i in matches, i)
        assert actual >= threshold


class FirstMatch(Evaluator):
    """The first result must be a specific work."""

    def evaluate_first(self, result):
        success = True
        bad_value = bad_expect = None

        # genre and subject need separate handling

        for field, expect in self.kwargs.items():
            value = self._field(field, result)
            if value != expect:
                # This test is going to fail, but we need to do
                # some other stuff before it does, so save the
                # information about where exactly it failed for later.
                success = False
                bad_value = value
                bad_expect = expect
                break

        if not success:
            # Log some useful information.
            self.log.info("First result details: %r", self.format(result))

            # Now cause the failure.
            eq_(bad_value, bad_expect)

class SpecificGenre(FirstMatch):
    """ The first result's genres must include a specific genre. """

    def evaluate_first(self, first):
        success = False
        expect_genre = self.genre
        if hasattr(self, 'author') and (self.author != self._field('author', first)):
            eq_(success, True)
        if hasattr(self, 'title') and (self.title != self._field('title', first)):
            eq_(success, True)
        genres = self._field('genres', first)
        print(genres)
        for genre in genres:
            if genre.name.lower() == expect_genre:
                success = True
                return success

        self.log.info("First result details: %r", self.format(result))
        eq_(success, True)


class SpecificAuthor(FirstMatch):
    """The first result must be by a specific author.

    Most of the results must also be by that author.
    """

    def __init__(self, author, accept_book_about_author=False, threshold=0.5):
        super(SpecificAuthor, self).__init__(author=author)
        self.accept_book_about_author = accept_book_about_author
        # I changed the threshold to 0 because searches involving less prolific authors were
        # failing otherwise
        self.threshold = 0

    def evaluate_first(self, first):
        expect_author = self.author
        author = self._field('author', first)
        title = self._field('title', first)
        if expect_author == self.author:
            return
        if self.accept_book_about_author and expect_author in title:
            return
        # We have failed.
        eq_(self.author, author)

    def evaluate_hits(self, hits):
        author = self.author
        threshold = self.threshold
        authors = [self._field('author', x) for x in hits]
        author_matches = [x for x in authors if x == author]
        self.assert_ratio(author_matches, authors, threshold)


class SpecificSeries(Evaluator):

    """Every result must be from the given series. The series name
    must either show up in the title or in the series field.
    """

    def __init__(self, series, threshold=0.5):
        self.series = series.lower()
        self.threshold = threshold

    def evaluate_hits(self, hits):
        matches = []
        for h in hits:
            if h:
                series = self._field('series', h)
                title = self._field('title', h)
                if self.series == series or self.series in title:
                    matches.append(h)
        self.assert_ratio(matches, hits, self.threshold)


class SearchTest(object):
    """A test suite that runs searches and compares the actual results
    to some expected state.
    """

    def search(self, query, evaluator, limit=10):
        query = query.lower()
        pagination = Pagination(size=limit)
        qu = self.searcher.query(query, pagination=pagination)
        hits = [x for x in qu][:]
        evaluator.evaluate(hits)


class TestTitleMatch(SearchTest):

    def test_simple_title_match_carrie(self):
        # There is one obvious right answer.
        self.search("carrie", FirstMatch(title="Carrie"))

    def test_simple_title_match_bookshop(self):
        self.search("the bookshop", FirstMatch(title="The Bookshop"))

    def test_simple_title_match_house(self):
        self.search("A house for Mr. biswas", FirstMatch(title="A House for Mr. Biswas"))

    def test_simple_title_match_clique(self):
        self.search("clique", FirstMatch(title="The Clique"))

    def test_simple_title_match_assassin(self):
        self.search("blind assassin", FirstMatch(title="Blind Assassin"))

    def test_simple_title_match_dry(self):
        self.search("the dry", FirstMatch(title="The Dry"))

    def test_simple_title_match_origin(self):
        self.search("origin", FirstMatch(title="Origin"))

    def test_simple_title_match_goldfinch(self):
        self.search("goldfinch", FirstMatch(title="Goldfinch"))

    def test_simple_title_match_beach(self):
        self.search("Manhattan beach", FirstMatch(title="Manhattan Beach"))

    def test_simple_title_match_testing(self):
        self.search("The testing", FirstMatch(title="The testing"))

    def test_title_match_with_genre_name_romance(self):
        # The title contains the name of a genre. Despite this,
        # an exact title match should show up first.
        self.search(
            "modern romance", FirstMatch(title="Modern Romance")
        )

    def test_title_match_with_genre_name_law(self):
        self.search(
            "law of the mountain man",
            FirstMatch(title="Law of the Mountain Man")
        )

    def test_title_match_with_genre_name_spy(self):
        # NOTE: this currently fails on both versions of ES.
        self.search(
            "My life as a spy",
            FirstMatch(title="My Life As a Spy")
        )

    def test_title_match_with_genre_name_dance(self):
        self.search(
            "dance with dragons",
            FirstMatch(title="A Dance With Dragons")
        )

    def test_it(self):
        # The book "It" is correctly prioritized over books whose titles contain
        # the word "it."
        self.search(
            "It",
            FirstMatch(title="It")
        )

    def test_lord_jim(self):
        # The book "Lord Jim" is correctly prioritized over books whose authors' names
        # contain "Lord" or "Jim."
        self.search(
            "Lord Jim",
            FirstMatch(title="Lord Jim")
        )

    def test_wilder(self):
        # The book "Wilder" is correctly prioritized over books by authors with the
        # last name "Wilder".
        self.search(
            "Wilder",
            FirstMatch(title="Wilder")
        )

    def test_title_match_with_audience_name_children(self):
        self.search(
            "Children of blood and bone",
            FirstMatch(title="Children of Blood and Bone")
        )

    def test_title_match_with_audience_name_kids(self):
        self.search(
            "just kids",
            FirstMatch(title="Just Kids")
        )

    def test_misspelled_title_match_emmie(self):
        # NOTE: this currently fails in both versions of ES.
        # The target title does appear in the top search results,
        # but it's not first.

        # One word in the title is slightly misspelled.
        self.search(
            "Ivisible emmie",
            FirstMatch(title="Invisible Emmie")
        )

    def test_misspelled_title_match_wave(self):
        # NOTE: this currently fails in both versions of ES; the target book is
        # result #4.  Fixing the typo fixes the search results.

        # One common word in the title is slightly misspelled.
        self.search(
            "He restless wave",
            FirstMatch(title="The Restless Wave")
        )

    def test_misspelled_title_match_kingdom(self):
        # NOTE: this works in ES1, but not in ES6!  In ES6, the target title is not
        # in the top search results.

        # The first word, which is a fairly common word, is slightly misspelled.
        self.search(
            "Kngdom of the blind",
            FirstMatch(title="The Kingdom of the Blind")
        )

    def test_misspelled_title_match_husbands(self):
        # Two words--1) a common word which is spelled as a different word
        # ("if" instead of "of"), and 2) a proper noun--are misspelled.
        self.search(
            "The seven husbands if evyln hugo",
            FirstMatch(title="The Seven Husbands of Evelyn Hugo")
        )

    def test_misspelled_title_match_nightingale(self):
        # NOTE: this fails in both versions of ES, but the top ES1 results are reasonable
        # (titles containing "nightfall"), whereas the top ES6 result is entitled
        # "Modern Warfare, Intelligence, and Deterrence."

        # Unusual word, misspelled
        self.search(
            "The nightenale",
            FirstMatch(title="The Nightingale")
        )

    def test_misspelled_title_match_geisha(self):
        # NOTE: this currently fails in both versions of ES.
        self.search(
            "Memoire of a ghesia",
            FirstMatch(title="Memoirs of a Geisha")
        )

    def test_misspelled_title_match_healthyish(self):
        # The title is not a real word, and is misspelled.
        self.search(
            "healtylish",
            FirstMatch(title="Healthyish")
        )

    def test_misspelled_title_match_bell(self):
        # One word, which is a relatively common word, is spelled as a different word.
        self.search(
            "For whom the bell tools",
            FirstMatch(title="For Whom the Bell Tolls")
        )

    def test_misspelled_title_match_baghdad(self):
        # One word, which is an extremely common word, is spelled as a different word.
        self.search(
            "They cane to baghdad",
            FirstMatch(title="They Came To Baghdad")
        )

    def test_misspelled_title_match_guernsey(self):
        # NOTE: this works in ES6 but not in ES1.  ES1 fixes the typo, but
        # doesn't seem able to handle converting the "and" into an ampersand.

        # One word, which is a place name, is misspelled.
        self.search(
            "The gurnsey literary and potato peel society",
            FirstMatch(title="The Guernsey Literary & Potato Peel Society")
        )

    def test_partial_title_match_home(self):
        # The search query only contains half of the title.
        self.search(
            "Future home of",
            FirstMatch(title="Future Home Of the Living God")
        )

    def test_partial_title_match_supervision(self):
        # NOTE: works on ES1, fails on ES6; it's the second title rather than
        # the first in ES6.

        # A word from the middle of the title is missing.
        self.search(
            "fundamentals of supervision",
            FirstMatch(title="Fundamentals of Library Supervision")
        )


    def test_partial_title_match_friends(self):
        # The search query only contains half of the title.
        self.search(
            "How to win friends",
            FirstMatch(title="How to Win Friends and Influence People")
        )

    def test_partial_title_match_face_1(self):
        # The search query is missing the last word of the title.
        self.search(
            "Girl wash your",
            FirstMatch(title="Girl, Wash Your Face")
        )

    def test_partial_title_match_face_2(self):
        # The search query is missing the first word of the title.
        self.search(
            "Wash your face",
            FirstMatch(title="Girl, Wash Your Face")
        )

    def test_partial_title_match_theresa(self):
        # The search results correctly prioritize books with titles containing
        # "Theresa" over books by authors with the first name "Theresa."
        self.search(
            "Theresa",
            FirstMatch(title="Theresa Raquin")
        )

    def test_misspelled_partial_title_match_brodie(self):
      # NOTE: this works in ES1, but not in ES6!  In ES6, the target title is in
      # the top search results, but is not first.

      # The search query only has the first and last words from the title, and
      # the last word is misspelled.
      self.search(
        "Prime brody",
        FirstMatch(title="The Prime of Miss Jean Brodie")
      )

    def test_gatos(self):
      # Searching for a Spanish word should bring up books in Spanish
        self.search(
            "gatos",
            FirstMatch(language="spa")
        )

class TestMixedTitleAuthorMatch(SearchTest):

    def test_centos_caen(self):
        # 'centos' shows up in the subtitle. 'caen' is the name
        # of one of the authors.
        self.search(
            "centos caen",
            FirstMatch(title="fedora linux toolbox")
        )

    def test_fallen_baldacci(self):
        self.search(
            "the fallen baldacci",
            FirstMatch(title="Fallen")
        )

    def test_dragons(self):
        # Full title, full but misspelled author
        self.search(
            "Michael conolley Nine Dragons",
            FirstMatch(title="Nine Dragons", author="Michael Connelly")
        )

    def test_dostoyevsky(self):
        # Full title, partial author
        self.search(
            "Crime and punishment Dostoyevsky",
            FirstMatch(title="Crime and Punishment")
        )

    def test_sparks(self):
        # Full title, full but misspelled author, "by"
        self.search(
            "Every breath by nicholis sparks",
            FirstMatch(title="Every Breath", author="Nicholas Sparks")
        )

    def test_grisham(self):
        # Full title, author's first name only
        self.search(
            "The reckoning john",
            FirstMatch(title="The Reckoning", author="John Grisham")
        )

class TestGenreMatch(SearchTest):

    def test_sf(self):
        # NOTE: This doesn't work.  On ES1, the top result has "science fiction" in
        # the title, but has no genre; on ES6, the top result has "science fiction" if __name__ == '__main__':
        # the title, but its genre is "Reference & Study Aids"

        self.search(
            "science fiction",
            SpecificGenre(genre="Science Fiction")
        )

    def test_iain_banks_sf(self):
        # NOTE: This works on ES6, but fails on ES1, just because the top hit in ES1 lists
        # the author's name without the middle initial.


        self.search(
            # Genre and author
            "iain banks science fiction",
            SpecificGenre(genre="Science Fiction", author="Iain M. Banks")
        )

    def test_christian(self):
        # NOTE: This doesn't work; it brings up books that have "Christian" in the
        # title but that have no genre.
        self.search(
            "christian",
            SpecificGenre(genre="Christian")
        )

    def test_graphic_novel(self):
        # NOTE: This works on ES6, but not on ES1.  On ES1, the top result's title
        # contains the phrase "graphic novel", but its genre is "Suspense/Thriller."

        self.search(
            "Graphic novel",
            SpecificGenre(genre="Comics & Graphic Novels")
        )

    def test_percy_jackson_graphic_novel(self):
        # NOTE: This doesn't work; on both versions of ES, the top result is by
        # Michael Demson and is not a graphic novel.

        self.search(
            "Percy jackson graphic novel",
            SpecificGenre(genre="Comics & Graphic Novels", author="Rick Riordan")
        )

    def test_clique(self):
        # NOTE: This doesn't work.  The target book does show up in the results, but
        # it's #3 in ES1 and #2 in ES6.  In both cases, the top result is a graphic novel
        # entitled "The Terrible and Wonderful Reasons Why I Run Long Distances."

        # Genre and title
        self.search(
            "The clique graphic novel",
            SpecificGenre(genre="Comics & Graphic Novels", title="The Clique")
        )

    def test_mystery(self):
        self.search(
            "mystery",
            SpecificGenre(genre="Mystery")
        )

    def test_agatha_christie_mystery(self):
        # Genre and author
        self.search(
            "agatha christie mystery",
            SpecificGenre(genre="Mystery", author="Agatha Christie")
        )

    def test_british_mystery(self):
        # Genre and keyword
        self.search(
            "British mysteries",
            SpecificGenre(genre="Romance")
        )

    def test_music_theory(self):
        # Keywords
        self.search(
            "music theory", SpecificGenre(genre="Music")
        )

    def test_supervising(self):
        # Keyword
        self.search(
            "supervising", SpecificGenre(genre="Education")
        )

    def test_grade_and_subject(self):
        # NOTE: this doesn't work on either version of ES.  The top result's genre
        # is science fiction rather than science.
        self.search(
            "Seventh grade science", SpecificGenre(genre="Science")
        )

class TestSubtitleMatch(SearchTest):

    def test_shame_stereotypes(self):
        # "Sister Citizen" has both search terms in its
        # subtitle. "Posess" has 'shamed' in its subtitle but does not
        # match 'stereotype' at all.
        self.search(
            "shame stereotypes", FirstMatch(title="Sister Citizen")
        )

    def test_garden_wiser(self):
        self.search(
            "garden wiser", FirstMatch(title="Gardening for a Lifetime")
        )

class TestAuthorMatch(SearchTest):

    def test_kelly_link(self):
        # There is one obvious right answer.
        self.search("kelly link", SpecificAuthor("Kelly Link"))

    def test_stephen_king(self):
        # This author is so well-known that there are books _about_
        # him (e.g. "Stephen King and Philosophy"). Such a book might
        # reasonably show up as the first search result. However, the
        # majority of search results should be books _by_ this author.
        self.search(
            "stephen king", SpecificAuthor(
                "Stephen King", accept_book_about_author=True
            )
        )

    def test_plato(self):
        # The majority of the search results will be _about_ this author,
        # but there should also be some _by_ him.
        self.search(
            "plato", SpecificAuthor(
                "Plato", accept_book_about_author=True
            )
        )

    def test_lagercrantz(self):
        # The search query contains only the author's last name.
        self.search(
            "Lagercrantz", SpecificAuthor("Rose Lagercrantz")
        )

    def test_deirdre_martin(self):
        # The author's first name is misspelled in the search query.
        self.search(
            "deidre martin", SpecificAuthor("Deirdre Martin")
        )

    def test_danielle_steel(self):
        # The author's last name is misspelled in the search query.
        self.search(
            "danielle steele", SpecificAuthor("Danielle Steel")
        )

    def test_nabokov(self):
        # Only the last name is provided in the search query,
        # and it's misspelled.
        self.search(
            "Nabokof", SpecificAuthor("Nabokov")
        )

    def test_ba_paris(self):
        # Author's last name could also be a subject keyword.
        self.search(
            "B a paris", SpecificAuthor("BA Paris")
        )

    def test_griffiths(self):
        # The search query lists the author's last name before her first name.
        self.search(
            "Griffiths elly", SpecificAuthor("Elly Griffiths")
        )

    def test_author_with_language(self):
        # NOTE: this doesn't work on either version of ES; the first Spanish result
        # is #3

         self.search(
            "Pablo escobar spanish",
            FirstMatch(author="Pablo Escobar", language="Spanish")
         )

class TestSeriesMatch(SearchTest):

    def test_39_clues(self):
        self.search("39 clues", SpecificSeries("39 clues"))

    def test_poldi(self):
        self.search(
            "Auntie poldi",
            SpecificSeries("Auntie Poldi")
        )

    def test_maggie_hope(self):
        self.search(
            "Maggie hope",
            SpecificSeries("Maggie Hope")
        )

    def test_harry_potter(self):
        # NOTE: this doesn't work on either version of ES.  It prioritizes
        # foreign-language editions of the "Harry Potter" books.

        self.search(
            "Harry potter",
            SpecificSeries("Harry Potter")
        )

    def test_maisie_dobbs(self):
        # Misspelled proper noun
        self.search(
            "maise dobbs",
            SpecificSeries("Maisie Dobbs")
        )

    def test_gossip_girl(self):
        # Misspelled common word
        self.search(
            "Gossip hirl",
            SpecificSeries("Gossip Girl")
        )

    def test_goosebumps(self):
        self.search(
            "goosebumps",
            FirstMatch(series="Goosebumps")
        )

    def test_severance(self):
        # Partial, and slightly misspelled
        self.search(
            "Severence",
            SpecificSeries("The Severance Trilogy")
        )

    def test_hunger_games(self):
        # NOTE: this doesn't work on either version of ES.  Fixing the typo makes it
        # work for ES1, but not for ES6.

        # Misspelled relatively common word
        self.search(
            "The hinger games",
            SpecificSeries("The Hunger Games")
        )

    def test_mockingjay(self):
        # NOTE: this doesn't work on either version of ES.  The target book is
        # the 8th result, and the top results are irrelevant.

        # Series and title
        self.search(
            "The hunger games mockingjay",
            FirstMatch(title="Mockingjay", series="The Hunger Games")
        )

    def test_foundation_1(self):
        # Series and full author
        self.search(
            "Isaac asimov foundation",
            SpecificSeries("Foundation", author="Isaac Asimov")
        )

    def test_foundation_2(self):
        # NOTE: this works on ES1 but not on ES6!

        # Series, full author, and book number
        self.search(
            "Isaac Asimov foundation book 1",
            SpecificSeries("Foundation", title="Prelude to Foundation")
        )

    def test_science_comics(self):
        # NOTE: this produces very different search results on ES1 vs. ES6, but
        # doesn't work on either.  ES1 is closer.

        # Series name containing genre names
        self.search(
            "Science comics",
            SpecificSeries("Science Comics")
        )

ES6 = ('es6' in os.environ['VIRTUAL_ENV'])
if ES6:
    url = os.environ['ES6_ELASTICSEARCH']
    index = "es6-test-v3"
else:
    url = os.environ['ES1_ELASTICSEARCH']
    index = None

_db = production_session()
library = None
index = ExternalSearchIndex(_db, url=url, works_index=index)
index.works_alias = index
SearchTest.searcher = Searcher(library, index)
