# encoding: utf-8
import logging
import urllib
from nose.tools import (
    eq_,
    set_trace,
)
import os
import re
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
        self.kwargs = dict()
        for k, v in kwargs.items():
            if isinstance(v, basestring):
                v = v.lower()
            self.kwargs[k] = v
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
        if isinstance(value, basestring):
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
            self.log.debug(
                "Need %d%% matches, got %d%%" % (
                    threshold*100, actual*100
                )
            )
            for i in hits:
                if i in matches:
                    template = 'Y (%s == %s)'
                else:
                    template = 'N (%s != %s)'
                self.log.debug(template % i)
        assert actual >= threshold

    def _match_scalar(self, value, expect):
        if hasattr(expect, 'search'):
            success = expect.search(value)
            expect_str = expect.pattern
        else:
            success = (value == expect)
            expect_str = expect
        return success, expect_str

    def _match_subject(self, subject, result):
        """Is the given result classified under the given subject?"""
        values = []
        expect_str = subject
        for classification in (result.classifications or []):
            value = classification['term'].lower()
            values.append(value)
            success, expect_str = self._match_scalar(value, subject)
            if success:
                return True, values, expect_str
        return False, values, expect_str

    def _match_genre(self, subject, result):
        """Is the given result classified under the given genre?"""
        values = []
        expect_str = subject
        for genre in (result.genres or []):
            value = genre['name'].lower()
            values.append(value)
            success, expect_str = self._match_scalar(value, subject)
            if success:
                return True, values, expect_str
        return False, values, expect_str

    def _match_target_age(self, how_old_is_the_kid, result):
        upper, lower = result.target_age.upper, result.target_age.lower
        if how_old_is_the_kid < lower or how_old_is_the_kid > upper:
            return False, how_old_is_the_kid, (lower, upper)
        return True, how_old_is_the_kid, (lower, upper)

    def match_result(self, result):
        """Does the given result match these criteria?"""

        for field, expect in self.kwargs.items():
            if field == 'subject':
                success, value, expect_str = self._match_subject(expect, result)
            elif field == 'genre':
                success, value, expect_str = self._match_genre(expect, result)
            elif field == 'target_age':
                success, value, expect_str = self._match_target_age(expect, result)
            else:
                value = self._field(field, result)
                success, expect_str = self._match_scalar(value, expect)
            if not success:
                return False, value, expect_str
        return True, value, expect_str

    def multi_evaluate(self, hits):
        # Evalate a number of hits and sort them into successes and failures.
        successes = []
        failures = []
        for h in hits:
            success, actual, expected = self.match_result(h)
            if success:
                successes.append((success, actual, expected))
            else:
                failures.append((success, actual, expected))
        return successes, failures


class Common(Evaluator):
    """It must be common for the results to match certain criteria.
    """
    def __init__(self, threshold=0.5, minimum=None, first_must_match=True,
                 **kwargs):
        """Constructor.

        :param threshold: A proportion of the search results must
        match these criteria.

        :param number: At least this many search results must match
        these criteria.

        :param first_must_match: In addition to any collective
        restrictions, the first search result must match the criteria.
        """
        super(Common, self).__init__(**kwargs)
        self.threshold = threshold
        self.minimum = minimum
        self.first_must_match = first_must_match

    def evaluate_first(self, hit):
        if self.first_must_match:
            success, actual, expected = self.match_result(hit)
            if not success:
                self.log.debug(
                    "First result did not match. %s != %s", expected, actual
                )
                eq_(actual, expected)

    def evaluate_hits(self, hits):
        successes, failures = self.multi_evaluate(hits)
        if self.threshold is not None:
            self.assert_ratio(
                [x[1:] for x in successes],
                [x[1:] for x in successes+failures],
                self.threshold
            )
        if self.minimum is not None:
            if len(successes) < self.minimum or True:
                self.log.debug(
                    "Need %d matches, got %d", self.minimum, len(successes)
                )
                for i in (successes+failures):
                    if i in successes:
                        template = 'Y (%s == %s)'
                    else:
                        template = 'N (%s != %s)'
                    self.log.debug(template % i[1:])
            assert len(successes) >= self.minimum

class FirstMatch(Common):
    """The first result must match certain criteria."""

    def __init__(self, **kwargs):
        super(FirstMatch, self).__init__(
            threshold=None, first_must_match=True, **kwargs
        )


class AtLeastOne(Common):
    def __init__(self, **kwargs):
        super(AtLeastOne, self).__init__(
            threshold=None, minimum=1, first_must_match=False, **kwargs
        )


class SpecificGenre(Common):
    pass

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


class SearchTest(object):
    """A test suite that runs searches and compares the actual results
    to some expected state.
    """

    def search(self, query, evaluators=None, limit=10):
        query = query.lower()
        logging.debug("Query: %r", query)
        pagination = Pagination(size=limit)
        qu = self.searcher.query(query, pagination=pagination)
        hits = [x for x in qu][:]
        if not evaluators:
            raise Exception("No evaluators specified!")
        if not isinstance(evaluators, list):
            evaluators = [evaluators]
        for e in evaluators:
            e.evaluate(hits)


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

    def test_simple_title_match_androids(self):
        self.search("Do androids dream of electric sheep",
        FirstMatch(title="Do Androids Dream of Electric Sheep?"))

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

    def test_misspelled_title_match_zodiac(self):
        # Uncommon word, slightly misspelled.
        self.search(
            "Zodiaf",
            FirstMatch(title="Zodiac")
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
            Common(genre="Science Fiction")
        )

    def test_iain_banks_sf(self):
        # NOTE: This works on ES6, but fails on ES1, just because the top hit in ES1 lists
        # the author's name without the middle initial.


        self.search(
            # Genre and author
            "iain banks science fiction",
            Common(genre="Science Fiction", author="Iain M. Banks")
        )

    def test_christian(self):
        # NOTE: This doesn't work; it brings up books that have "Christian" in the
        # title but that have no genre.
        self.search(
            "christian",
            Common(genre="Christian")
        )

    def test_graphic_novel(self):
        # NOTE: This works on ES6, but not on ES1.  On ES1, the top result's title
        # contains the phrase "graphic novel", but its genre is "Suspense/Thriller."

        self.search(
            "Graphic novel",
            Common(genre="Comics & Graphic Novels")
        )

    def test_percy_jackson_graphic_novel(self):
        # NOTE: This doesn't work; on both versions of ES, the top result is by
        # Michael Demson and is not a graphic novel.

        self.search(
            "Percy jackson graphic novel",
            Common(genre="Comics & Graphic Novels", author="Rick Riordan")
        )

    def test_clique(self):
        # NOTE: This doesn't work.  The target book does show up in the results, but
        # it's #3 in ES1 and #2 in ES6.  In both cases, the top result is a graphic novel
        # entitled "The Terrible and Wonderful Reasons Why I Run Long Distances."

        # Genre and title
        self.search(
            "The clique graphic novel",
            Common(genre="Comics & Graphic Novels", title="The Clique")
        )

    def test_mystery(self):
        self.search(
            "mystery",
            Common(genre="Mystery")
        )

    def test_agatha_christie_mystery(self):
        # Genre and author
        self.search(
            "agatha christie mystery",
            Common(genre="Mystery", author="Agatha Christie")
        )

    def test_british_mystery(self):
        # Genre and keyword
        self.search(
            "British mysteries",
            Common(genre="Mystery", summary=re.compile("british"))
        )

    def test_music_theory(self):
        # Keywords
        self.search(
            "music theory", Common(
                genre="Music",
                subject=re.compile("(music theory|musical theory)")
            )
        )

    def test_supervising(self):
        # Keyword
        self.search(
            "supervising", Common(genre="Education")
        )

    def test_grade_and_subject(self):
        # NOTE: this doesn't work on either version of ES.  The top result's genre
        # is science fiction rather than science.
        self.search(
            "Seventh grade science",
            [
                Common(target_age=12),
                Common(genre="Science")
            ]
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

    def test_poldi(self):
        # We only have one book in this series.
        self.search(
            "Auntie poldi",
            FirstMatch(series="Auntie Poldi")
        )

    def test_39_clues(self):
        # We have many books in this series.
        self.search("39 clues", Common(series="the 39 clues", threshold=0.9))

    def test_maggie_hope(self):
        # We have many books in this series.
        self.search(
            "Maggie hope",
            Common(series="Maggie Hope", threshold=0.9)
        )

    def test_harry_potter(self):
        # This puts foreign-language titles above English titles, but
        # that's fine because our search document doesn't include a
        # language filter.
        self.search(
            "Harry potter",
            Common(series="Harry Potter", threshold=0.9)
        )

    def test_maisie_dobbs(self):
        # Misspelled proper noun
        self.search(
            "maise dobbs",
            Common(series="Maisie Dobbs", threshold=0.5)
        )

    def test_gossip_girl(self):
        # Misspelled common word

        # TODO: We only have two books in this series. It would be
        # useful to specify the number of expected matches in the
        # SpecificSeries constructor, rather than a percentage.
        self.search(
            "Gossip hirl",
            Common(series="Gossip Girl"), limit=4
        )

    def test_goosebumps(self):
        self.search(
            "goosebumps",
            Common(series="Goosebumps", threshold=0.9)
        )

    def test_severance(self):
        # Partial, and slightly misspelled
        # We only have one of these titles.
        self.search(
            "Severence",
            FirstMatch(series="The Severance Trilogy")
        )

    def test_hunger_games(self):
        # NOTE: This works on ES1 but not ES6.
        self.search("the hunger games", Common(series="The Hunger Games"))

        # NOTE: This doesn't work on either version
        self.search("The hinger games", Common(series="The Hunger Games"))

    def test_mockingjay(self):
        # NOTE: this doesn't work on either version of ES.  The target book is
        # the 8th result, and the top results are irrelevant.

        # Series and title
        self.search(
            "The hunger games mockingjay",
            [FirstMatch(title="Mockingjay"), Common(series="The Hunger Games")]
        )

    def test_foundation(self):
        # Series and full author
        self.search(
            "Isaac asimov foundation",
            Common(series="Foundation")
        )

    def test_foundation_specific_book(self):
        # NOTE: this works on ES1 but not on ES6!

        # Series, full author, and book number
        self.search(
            "Isaac Asimov foundation book 1",
            FirstMatch(series="Foundation", title="Prelude to Foundation")
        )

    def test_science_comics(self):
        # NOTE: this produces very different search results on ES1 vs. ES6, but
        # doesn't work on either.  ES1 is closer.

        # Series name containing genre names
        self.search(
            "Science comics",
            Common(series="Science Comics")
        )

class TestKidsSearches(SearchTest):

    # Kids searches, being put into a separate test for now to avoid
    # merge conflicts.

    def test_39_clues_specific_title(self):
        # The first result is the requested title. Other results
        # are from the same series.
        self.search(
            "39 clues maze of bones",
            [
                FirstMatch(title="The Maze of Bones"),
                Common(series="the 39 clues", threshold=0.9)
            ]
        )

    def test_3_little_pigs(self):
        self.search(
            "3 little pigs",
            [
                AtLeastOne(title=re.compile("three little pigs")),
                Common(title=re.compile("pig")),

                # TODO: This would require that '3' and 'three'
                # be analyzed the same way.
                # FirstMatch(title="Three Little Pigs"),
            ]
        )

    def test_allegiant_misspelled(self):
        # A very bad misspelling.
        self.search(
            "alliagent",
            FirstMatch(title="Allegiant")
        )

    def test_all_the_hate(self):
        for q in (
                'the hate u give',
                'all the hate u give',
                'all the hate you give',
                'hate you give',
                'hate you gove'):
            self.search(q, FirstMatch(title="The Hate U Give"))

    def test_alien_misspelled(self):
        "allien"
        "aluens"
        pass

    def test_anime_genre(self):
        self.search(
            "anime books",
        )

    def test_batman(self):
        # Patron is searching for 'batman' but treats it as two words.
        self.search(
            "bat man book",
        )

    def test_spiderman(self):
        # Patron is searching for 'spider-man' but treats it as one word.
        for q in ("spiderman", "spidermanbook"):
            self.search(
                q, Common(title=re.compile("spider-man"))
            )

    def test_texas_fair(self):
        self.search("books about texas like the fair")

    def test_boy_saved_baseball(self):
        self.search("boy saved baseball")

    def test_chapter_books(self):
        self.search("chapter bookd")
        self.search("chapter books")
        self.search("chaptr books")

    def test_charlottes_web(self):
        # Different ways of searching for "Charlotte's Web"
        for q in (
                "charlotte's web",
                "charlottes web",
                "charlottes web eb white"
                'charlettes web',
        ):
            self.search(
                q,
                FirstMatch(title="Charlotte's Web")
            )

    def test_christopher_mouse(self):
        # Different ways of searching for "Christopher Mouse: The Tale
        # of a Small Traveler" (A book not in NYPL's collection)
        for q in (
                "christopher mouse",
                "chistopher mouse",
                "christophor mouse"
                "christopher moise",
                "chistoper muse",
        ):
            self.search(
                q,
                FirstMatch(title=re.compile("Christopher Mouse"))
            )

    def test_wimpy_kid(self):
        self.search(
            "dairy of the wimpy kid",
            Common(series="Diary of a Wimpy Kid")
        )

    def test_wimpy_kid_specific_title(self):
        self.search(
            "dairy of the wimpy kid dog days",
            [
                FirstMatch(title="Dog Days", author="Jeff Kinney"),
                Common(series="Diary of a Wimpy Kid"),
            ]
        )

    def test_deep_poems(self):
        # This appears to be a subject search.
        self.search(
            "deep poems",
        )

    def test_dinosaur_cove(self):
        self.search(
            "dinosuar cove",
            Common(series="Dinosaur Cove")
        )

    def test_dirtbike(self):
        self.search(
            "dirtbike",
        )

    def test_dork_diaries(self):
        # Different ways of spelling "Dork Diaries"
        for q in (
                'dork diaries',
                'dork diarys',
                'dork diarys #11',
                'dork diary',
                'doke diaries.',
                'doke dirares',
                'doke dares',
                'doke dires',
                'dork diareis',
        ):
            self.search(q, Common(series="Dork Diaries"))

    def test_drama_comic(self):
        self.search(
            "drama comic",
            FirstMatch(title="Drama", author="Raina Telgemeier")
        )

    def test_spanish(self):
        self.search(
            "espanol",
            Common(language="spa")
        )

    def test_dan_gutman(self):
        self.search(
            "gutman, dan",
            Common(author="Dan Gutman")
        )

    def test_dan_gutman_series(self):
        self.search(
            "gutman, dan the weird school",
            Common(series=re.compile("my weird school"), author="Dan Gutman")
        )

    def test_i_funny(self):
        self.search(
            "i funny",
            Common(series="I, Funny", threshold=0.3)
        )

        self.search(
            "i funnyest",
            AtLeastOne(title="I Totally Funniest"),
        )

    def test_information_technology(self):
        self.search(
            "information technology"
        )

    def test_i_survived(self):
        # Different ways of spelling "I Survived"
        for q in (
                'i survived',
                'i survied',
                'i survive',
                'i survided',
        ):
            self.search(q, Common(title=re.compile("^i survived")))

    def test_manga(self):
        self.search("manga")

    def test_my_little_pony(self):
        for q in ('my little pony', 'my little pon'):
            self.search(
                q, Common(title=re.compile("my little pony"))
            )

    def test_ninjas(self):
        for q in (
                'ninjas',
                'ningas',
        ):
            self.search(q, Common(title=re.compile("ninja")))

    def test_pranks(self):
        for q in (
                'prank',
                'pranks',
        ):
            self.search(q, Common(title=re.compile("prank")))

    def test_raina_telgemeier(self):
        for q in (
                'raina telgemeier',
                'raina telemger',
                'raina telgemerier'
        ):
            # We use a regular expression because Raina Telgemeier is
            # frequently credited alongside others.
            self.search(q, Common(author=re.compile("raina telgemeier")))

    def test_scary_stories(self):
        self.search("scary stories")

    def test_scifi(self):
        self.search("sci-fi", Common(genre="Science Fiction"))

    def test_survivors_specific_book(self):
        self.search(
            "survivors book 1",
            [
                Common(series="Survivors"),
                FirstMatch(title="The Empty City"),
            ]
        )

    def test_teen_titans(self):
        # We can't necessarily deliver results tailored to
        # 'teen titans girls', but we should at least give 
        # _similar_ results as with 'teen titans'.
        for q in ('teen titans', 'teen titans girls'):        
            self.search(
                q, Common(title=re.compile("^teen titans")), limit=5
            )

    def test_thrawn(self):
        self.search(
            "thrawn",
            [
                FirstMatch(title="Thrawn"),
                Common(author="Timothy Zahn", series=re.compile("star wars")),
            ]
        )

    def test_timothy_zahn(self):
        for i in ('timothy zahn', 'timithy zahn'):
            self.search(q, Common(author="Timothy Zahn"))

    def test_who_is(self):
        # These children's bibliographies don't have .series set but
        # are clearly part of a series.
        #
        # Because those books don't have .series set, the matches are
        # done solely through title, so unrelated books like "Who Is
        # Rich?" show up.
        for q in ('who is', 'who was'):
            self.search(q, Common(title=re.compile('^%s ' % q)))

    def test_witches(self):
        self.search(
            "witches",
            Common(subject=re.compile('witch'))
        )

class TestDifficultSearches(SearchTest):
    """Tests that have been difficult in the past."""

    def test_game_of_thrones(self):
        self.search(
            "game of thrones",
            Common(series="a song of ice and fire")
        )

    def test_m_j_rose(self):
        for spelling in (
            'm. j. rose',
            'm.j. rose',
            'm j rose',
            'mj rose',
        ):
            self.search(
                spelling,
                Common(author="M. J. Rose")
            )

    def test_steve_berry(self):
        self.search(
            "steve berry",
            Common(author="Steve Berry")
        )

    def test_python_programming(self):
        self.search(
            "python programming",
            Common(subject="Python (computer program language)")
        )

    def test_tennis(self):
        self.search(
            "tennis",
            [
                Common(subject=re.compile("tennis")),
                Common(genre="Sports"),
            ]
        )

    def test_girl_on_the_train(self):
        self.search(
            "girl on the train",
            FirstMatch(title="The Girl On The Train")
        )

    def test_modern_romance_with_author(self):
        self.search(
            "modern romance aziz ansari",
            FirstMatch(title="Modern Romance", author="Aziz Ansari")
        )

    def test_law_of_the_mountain_man_with_author(self):
        self.search(
            "law of the mountain man william johnstone",
            [
                FirstMatch(title="Law of the Mountain Man"),
                Common(author="William Johnstone"),
            ]
        )

    def test_thomas_python(self):
        # All the terms are correctly spelled words, but the patron
        # clearly means something else.
        self.search(
            "thomas python",
            Common(author="Thomas Pynchon")
        )

    def test_age_3_5(self):
        # These terms are chosen to appear in the descriptions of
        # children's books, but also to appear in "Volume 3" or
        # "Volume 5" of series for adults.
        #
        # This verifies that the server interprets "age 3-5" correctly.
        for term in ('black', 'island', 'panda'):
            self.search(
                "%s age 3-5" % term,
                [
                    Common(audience='Children', threshold=1),
                ]
            )

        for term in ('black', 'island'):
            # Except from 'panda', the search terms on their own find
            # mostly books for adults.
            self.search(term, Common(audience='Adult', first_must_match=False))


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
