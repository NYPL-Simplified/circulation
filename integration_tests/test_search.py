# encoding: utf-8
#
# These integration tests were written based primarily on real
# searches made in October 2018 against NYPL's circulation
# manager. Theoretically, most of the tests that pass on NYPL's index
# should pass when run against the search index of any public library
# with a similarly sized collection.
#
# These guidelines were used when writing the tests:
#
# * A search for a specific book should return that book as the first result.
#   This is true whether or not the search query names the book.
# * Results for a series search should be dominated by books from that series.
# * Results for a person search should be dominated by books by or (in some
#   cases) about that person.
# * A search for a topic or genre should return books on that topic or
#   in that genre.
#
# It's possible for a test to fail not because of a problem with the
# search engine but because a library's collection is incomplete.  The
# tests are written to minimize the chances that this will happen
# unnecessarily. (e.g. the search for "dirtbike" checks for books
# filed under certain subjects, not specific titles).
#
# To run the tests, put the URL to your Elasticsearch index in the
# ES1_ELASTICSEARCH environment variable and run this command:
#
# $ nosetests integration_tests/test_search.py

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

# A problem from the unit tests that we couldn't turn into a
# real integration test.
#
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
    """A class that knows how to perform searches."""
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

        if result.target_age.lower == 18:
            return False, how_old_is_the_kid, "18+"

        expect_upper, expect_lower = max(how_old_is_the_kid), min(how_old_is_the_kid)
        expect_set = set(range(expect_lower, expect_upper + 1))

        result_upper = result.target_age.upper
        result_lower = result.target_age.lower
        result_set = set(range(result_lower, result_upper + 1))

        if result_set and expect_set.intersection(result_set):
            return True, how_old_is_the_kid, (result_lower, result_upper)
        return False, how_old_is_the_kid, (result_lower, result_upper)

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


class ReturnsNothing(Evaluator):
    """This search should return no results at all."""

    def evaluate(self, hits):
        assert not hits


class Common(Evaluator):
    """It must be common for the results to match certain criteria.
    """
    def __init__(self, threshold=0.5, minimum=None, first_must_match=True,
                 **kwargs):
        """Constructor.

        :param threshold: A proportion of the search results must
        match these criteria.

        :param minimum: At least this many search results must match
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
        threshold = kwargs.pop('threshold', None)
        super(FirstMatch, self).__init__(
            threshold=threshold, first_must_match=True, **kwargs
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

    def __init__(self, author, accept_book_about_author=False, threshold=0):
        super(SpecificAuthor, self).__init__(author=author, threshold=threshold)
        self.accept_book_about_author = accept_book_about_author

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

class TestGibberish(SearchTest):
    # If you type junk into the search box you should get no results.

    def test_junk(self):
        # Test one long string
        self.search(
            "rguhriregiuh43pn5rtsadpfnsadfausdfhaspdiufnhwe42uhdsaipfh",
            ReturnsNothing()
        )

    def test_multi_word_junk(self):
        # Test several short strings
        self.search(
            "rguhriregiuh 43pn5rts adpfnsadfaus dfhaspdiufnhwe4 2uhdsaipfh",
            ReturnsNothing()
        )

    def test_wordlike_junk(self):
        # This test fails on ES1 and ES6. To a human eye it is
        # obviously gibberish, but it's close enough to English words
        # that it picks up a few results.
        self.search(
            "asdfza oiagher ofnalqk",
            ReturnsNothing()
        )


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
        self.search(
            "goldfinch",
            FirstMatch(
                title=re.compile("^(the )?goldfinch$"),
                author="Donna Tartt"
            )
        )

    def test_simple_title_match_beach(self):
        self.search("Manhattan beach", FirstMatch(title="Manhattan Beach"))

    def test_simple_title_match_testing(self):
        self.search(
            "The testing",
            FirstMatch(title="The testing", author='Joelle Charbonneau')
        )

    def test_simple_title_twentysomething(self):
        self.search(
            "Twentysomething",
            FirstMatch(title="Twentysomething")
        )

    def test_simple_title_match_bell_jar(self):
        # NOTE: this works on ES6.  On ES1, the top result is the Sparknotes for
        # "The Bell Jar," rather than the novel itself.

        self.search("bell jar", FirstMatch(author="Sylvia Plath"))

    def test_simple_title_match_androids(self):
        self.search("Do androids dream of electric sheep",
        FirstMatch(title="Do Androids Dream of Electric Sheep?"))

    def test_unowned_title_cat(self):
        # NOTE: this book isn't in the collection, but there are plenty of books
        # with "save" and/or "cat" in their titles.  This works on ES6 but not ES1.
        self.search("Save the Cat", Common(title=re.compile("(save|cat)"), threshold=1))

    def test_unowned_title_zombie(self):
        # NOTE: this fails on both versions, even though there's no shortage of
        # books with one of those search terms in their titles.
        self.search(
            "Diary of a minecraft zombie",
            Common(title=re.compile("(diar|minecraft|zombie)"))
        )

    def test_unowned_title_pie(self):
        # NOTE: "Pie Town Woman" isn't in the collection, but there's a book called
        # "Pie Town," which seems like the clear best option for the first result.
        # This works on ES6.  On ES1, the first result is instead a book entitled
        # "The Quiche and the Dead," which has "pie" only in the summary.
        self.search("Pie town woman", FirstMatch(title="Pie Town"))

    def test_unowned_title_divorce(self):
        self.search(
            "The truth about children and divorce", [
                Common(
                    audience="adult",
                    first_must_match=False
                ),
                AtLeastOne(
                    title=re.compile("divorce")
                )
            ]
        )

    def test_unowned_title_decluttering(self):
        # NOTE: this book isn't in the collection, but the top search results should
        # be reasonably relevant.  This works on ES6 but fails on ES1.

        self.search(
            "Decluttering at the speed of life", [
                AtLeastOne(title=re.compile("declutter")),
                Common(subject=re.compile(
                    "(house|self-help|self-improvement|decluttering|organization)"
                ), first_must_match=False)
            ]
        )

    def test_unowned_partial_title_rosetta_stone(self):
        # NOTE: the collection doesn't have any books with titles containing "rosetta"
        # stone," but it does have a few with titles containing "rosetta"; ideally,
        # one of those would be the first result.  A title containing "stone" would be
        # less relevant to the user, but still reasonable.  Instead, the first result
        # is a memoir by an author whose first name is "Rosetta."

        self.search(
            "Rosetta stone",
            FirstMatch(title=re.compile("(rosetta|stone)"))
        )

    def test_unowned_misspelled_partial_title_cosmetics(self):
        # NOTE: this fails on both versions of ES.  The user was presumably
        # looking for "Don't Go to the Cosmetics Counter Without Me," which
        # isn't in the collection.  Ideally, one of the results should have
        # something to do with cosmetics; instead, they're about comets.  Fixing
        # the typo makes the test pass.

        self.search(
            "Cometics counter", [
                AtLeastOne(title=re.compile("cosmetics")),
            ]
        )

    def test_nonexistent_title_tower(self):
        # NOTE: there is no book with this title.  The most
        # likely scenario is that the user meant "The Dark Tower."  This
        # doesn't currently work on either version of ES.

        self.search("The night tower", FirstMatch(title="The Dark Tower"))

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

    def test_title_match_with_genre_name_spy_unowned(self):
        # NOTE: this book is not in the system.
        self.search(
            "My life as a spy",
            Common(title=re.compile("(life|spy)"), threshold=.9)
        )

    def test_title_match_with_genre_name_spy(self):
        self.search(
            "spying on whales",
            FirstMatch(title="Spying on Whales")
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
        # last name "Wilder."
        self.search(
            "Wilder",
            FirstMatch(title="Wilder")
        )

    def test_alice(self):
        # The book "Alice" is correctly prioritized over books by authors with the
        # first name "Alice."
        self.search(
            "Alice",
            FirstMatch(title="Alice")
        )

    def test_alex_and_eliza(self):
        # The book "Alex and Eliza" is correctly prioritized over books by authors with the
        # first names "Alex" or "Eliza."
        self.search(
            "Alex and Eliza",
            FirstMatch(title="Alex and Eliza")
        )

    def test_disney(self):
        # The majority of the search results will be about Walt Disney and/or the
        # Disney Company, but there should also be some published by the Disney Book Group
        self.search(
            "disney",
                [ Common(title=re.compile("disney")),
                  AtLeastOne(title=re.compile("walt disney")),
                  AtLeastOne(author="Disney Book Group") ]
        )

    def test_bridge(self):
        # The search results correctly prioritize the book with this title over books
        # by authors whose names contain "Luis" or "Rey."
        self.search(
            "the bridge of san luis rey",
            FirstMatch(title="The Bridge of San Luis Rey")
        )

    def test_title_match_sewing(self):
        # NOTE: this works on ES1, but not ES6; in ES6, the first result is a
        # picture book biography with the word "sewing" in the title, rather than
        # a book about sewing.

        self.search(
            "Sewing",
            [ FirstMatch(title=re.compile("sewing")),
              Common(title=re.compile("sewing")),
              Common(subject=re.compile("crafts"))
            ]
        )

    def test_title_match_louis_xiii(self):
        # NOTE: this doesn't currently work.  There aren't very many books in the collection
        # about Louis XIII, but there are lots of biographies of other people named Louis
        # (including other kings of France), which should ideally show up before books from
        # the Louis Kincaid series.

        self.search(
            "Louis xiii",
            Common(title=re.compile("louis"), threshold=0.8)
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

    def test_da_vinci(self):
        self.search(
            "Da Vinci",
            Common(genre=re.compile("(biography|art)"), first_must_match=False)
        )

    def test_da_vinci_misspelled(self):
        # NOTE: fails on both versions.  The user is much more likely to be
        # looking for books about Da Vinci than to be looking for books in the
        # "Davina Graham" series, which is taking up most of the top search results.

        self.search(
            "Davinci",
            Common(
                Title=re.compile("(biography|art)"),
                first_must_match=False,
                threshold=0.3
            )
        )

    def test_misspelled_title_match_marriage(self):
        # NOTE: fails on both versions.  The first result is "The Marriage Contract,"
        # and "The Marriage Lie" (which is presumably what the user wanted, and
        # which is in the collection) isn't even in the top ten.
        self.search(
            "Marriage liez",
            FirstMatch(title="The Marriage Lie")
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

    def test_misspelled_title_match_karamazov(self):
        # NOTE: this works on ES1 but fails on ES6; not only is the target title
        # not the first result in ES6, it's not any of the top results.  Fixing
        # the typo makes it work.

        # Extremely uncommon proper noun, slightly misspelled
        self.search(
            "Brothers karamzov",
            FirstMatch(title="The Brothers Karamazov")
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

    def test_misspelled_title_match_genghis(self):
        # NOTE: this doesn't work.  The collection definitely contains books with
        # "Genghis Khan" in the title, but all of the top search results are books
        # by authors with the last name "Khan."

        self.search(
            "Ghangiz Khan",
            AtLeastOne(title=re.compile("Genghis Khan"))
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

    def test_british_spelling_color_of_our_sky(self):
        # NOTE: fails on both.  In ES1, the target book is the 3rd result; in
        # ES6, it's nowhere in the top results.

        self.search(
            "The colour of our sky",
            FirstMatch(title="The Color of Our Sky")
        )

    def test_partial_title_match_supervision(self):
        # NOTE: works on ES1, fails on ES6; it's the second title rather than
        # the first in ES6.

        # A word from the middle of the title is missing.

        # I think they might be searching for a different book
        # we don't have. - LR
        self.search(
            "fundamentals of supervision",
            FirstMatch(title="Fundamentals of Library Supervision")
        )

    def test_partial_title_match_hurin(self):
        # Successfully searches "Húrin" (even though that's not a real word, and
        # the query didn't have the accent mark) before trying to correct it.
        self.search(
            "Hurin",
            FirstMatch(author=re.compile("tolkien"))
        )

    def test_partial_title_match_open_wide(self):
        # Search query cuts off midway through the second word of the subtitle.
        self.search(
            "Open wide a radical",
            FirstMatch(
                title="Open Wide",
                subtitle="a radically real guide to deep love, rocking relationships, and soulful sex"
            )
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
      # Searching for a Spanish word should mostly bring up books in Spanish
        self.search(
            "gatos",
            Common(language="spa", threshold=0.9)
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

    def test_singh(self):
        # NOTE: this doesn't currently work on either version of ES6.  The
        # search results aren't sufficiently prioritizing titles containing
        # "archangel" over the author's other books.  Changing "arch" to
        # "archangel" in the search query helps only slightly.

        self.search(
            "Nalini singh archangel",
            [ Common(author="Nalini Singh", threshold=0.9),
              Common(title=re.compile("archangel")) ]
        )

    def test_sebald_1(self):
        # NOTE: this title isn't in the collection, but the author's other
        # books should still come up.

        # Partial, unowned title; author's last name only
        self.search(
            "Sebald after",
            Common(author=re.compile("(w. g. sebald|w.g. sebald)"))
        )

    def test_sebald_2(self):
        # NOTE: putting in the full title (in contrast to the previous test)
        # completely breaks the test; the top results are now books by
        # authors other than Sebald, with titles which contain none of the
        # search terms.
        #
        # This is because 'nature' is the name of a genre. -LR

        # Full, unowned title; author's last name only
        self.search(
            "Sebald after nature",
            Common(author=re.compile("(w. g. sebald|w.g. sebald)"))
        )

class TestGenreMatch(SearchTest):

    def test_sf(self):
        # NOTE: This doesn't work.  On ES1, the top result has "science fiction" in
        # the title, but has no genre; on ES6, the top result has "science fiction"
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
        # Fails because of the first result--on ES1, it's a book from 1839
        # entitled "Christian Phrenology," which doesn't have a genre or subject
        # listed, and in ES6 it's a novel about Fletcher Christian. The
        # subsequent results are fine; setting first_must_match to False
        # makes the test pass.
        self.search(
            "christian",
            Common(genre=re.compile("(christian|religion)"))
        )

    def test_christian_lust(self):
        # Passes
        self.search(
            "lust christian",
            Common(genre=re.compile("(christian|religion)"))
        )

    def test_christian_authors(self):
        # Passes
        self.search(
            "christian authors",
            Common(genre=re.compile("(christian|religion)"))
        )

    def test_christian_grey(self):
        # Fails.  The first two books are Christian-genre books with "grey" in
        # the title, which is a reasonable search result but is almost definitely
        # not what the user wanted.
        self.search(
            "christian grey",
            FirstMatch(author="E. L. James")
        )

    def test_christian_fiction(self):
        # Fails.  On ES6, the first few results are from the "Christian Gillette"
        # series. On ES6, most of the top results are books by Hans Christian Andersen.
        # Definitely not what the user meant.
        self.search(
            "christian fiction",
            Common(genre=re.compile("(christian|religion)"))
        )

    def test_christian_kracht(self):
        # Passes - author name
        self.search(
            "christian kracht",
            FirstMatch(author="Christian Kracht")
        )

    def test_graphic_novel(self):
        # NOTE: This works on ES6, but not on ES1.  On ES1, the top result's title
        # contains the phrase "graphic novel", but its genre is "Suspense/Thriller."

        self.search(
            "Graphic novel",
            Common(genre="Comics & Graphic Novels")
        )

    def test_horror(self):
        self.search(
            "Best horror story",
            Common(genre=re.compile("horror"))
        )

    def test_greek_romance(self):
        # NOTE: this fails.  All of the top results are romance novels with
        # "Greek" in the summary.  Not necessarily problematic...but the
        # user is probably more likely to be looking for something like "Essays
        # on the Greek Romances."  If you search "Greek romances" rather than
        # "Greek romance," then "Essays on the Greek Romances" becomes the first
        # result on ES1, but is still nowhere to be found on ES6.
        self.search(
            "Greek romance",
            AtLeastOne(title=re.compile("greek romance"))
        )

    def test_percy_jackson_graphic_novel(self):
        # NOTE: This doesn't work; on both versions of ES, the top result is by
        # Michael Demson and is not a graphic novel.

        self.search(
            "Percy jackson graphic novel",
            Common(genre="Comics & Graphic Novels", author="Rick Riordan")
        )


    def test_gossip_girl_manga(self):
        # A "Gossip Girl" manga series does exist, but it's not in the collection.
        # Instead, the results should include some "Gossip Girl" books (most of which
        # don't have .series set; hence searching by the author's name instead) and
        # also some books about manga.
        self.search(
            "Gossip girl Manga", [
                Common(
                    author=re.compile("cecily von ziegesar"),
                    first_must_match=False,
                    threshold=0.3
                ),
                Common(
                    title=re.compile("manga"),
                    first_must_match=False,
                    threshold=0.3
                )
            ]
        )

    def test_betty_neels_audiobooks(self):
        # NOTE: Even though there are no audiobooks, all of the search results should still
        # be books by this author.  This works on ES1, but the ES6 search results devolve
        # into Betty Crocker cookbooks.

        # Full author, unowned genre.
        self.search(
            "Betty neels audiobooks",
            Common(author="Betty Neels", genre="romance", threshold=1)
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

    def test_spy(self):
        self.search(
            "Spy",
            Common(genre=re.compile("(espionage|history|crime|thriller)"))
        )

    def test_espionage(self):
        self.search(
            "Espionage",
            Common(
                genre=re.compile("(espionage|history|crime|thriller)"),
                first_must_match=False
            )
        )

    def test_food(self):
        self.search(
            "food",
            Common(
                genre=re.compile("(cook|diet)"),
                first_must_match=False
            )
        )

    def test_genius_foods(self):
        # Search results should bring up the target book and at least one related book
        self.search(
            "genius foods", [
                FirstMatch(title="Genius Foods"),
                Common(
                    genre=re.compile("(cook|diet)"),
                    threshold=0.2
                )
            ]
        )

    def test_ice_cream(self):
        # There are a lot of books about making ice cream.  The search results
        # correctly present those before looking for non-cooking "artisan" books.
        self.search(
            "Artisan ice cream",
            Common(
                genre=re.compile("cook"),
                threshold=0.9
            )
        )

    def test_beauty_hacks(self):
        # NOTE: fails on both versions.  The user was obviously looking for a specific
        # type of book; ideally, the search results would return at least one relevant
        # one.  Instead, all of the top results are either books about computer hacking
        # or romance novels.
        self.search("beauty hacks",
        AtLeastOne(subject=re.compile("(self-help|style|grooming|personal)")))

    def test_anxiety(self):
        self.search(
            "anxiety",
            Common(
                genre=re.compile("(psychology|self-help)"),
                first_must_match=False
            )
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
            [ SpecificGenre(genre="Mystery", author="Agatha Christie"),
              Common(author="Agatha Christie", threshold=1) ]
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

    def test_astrophysics(self):
        # Keyword
        self.search(
            "Astrophysics",
            Common(
                genre="Science",
                subject=re.compile("(astrophysics|astronomy|physics|space|science)")
            )
        )

    def test_finance(self):
        # Keyword
        self.search(
            "Finance",
            Common(
                genre=re.compile("(business|finance)"), first_must_match=False
            )
        )

    def test_constitution(self):
        # Keyword
        self.search(
            "Constitution",
            Common(
                genre=re.compile("(politic|history)"), first_must_match=False
            )
        )

    def test_supervising(self):
        # Keyword
        self.search(
            "supervising", Common(genre="Education")
        )

    def test_travel_california(self):
        # NOTE: this doesn't work on either version of ES, but it's closer on ES6.

        # Keyword
        self.search(
            "California",
            Common(genre=re.compile("(travel|guide|fodors)"), first_must_match=False)
        )

    def test_travel_florida(self):
        # NOTE: this doesn't work on either version of ES, but it's closer on ES6.

        # Keyword
        self.search(
            "Florida",
            Common(subject=re.compile("(travel|guide|fodors)"), first_must_match=False)
        )

    def test_travel_toronto(self):
        # Keyword
        self.search(
            "Toronto",
            Common(subject=re.compile("(travel|guide|fodors)"), first_must_match=False)
        )

    def test_native_american(self):
        # Keyword
        self.search(
            "Native american", [
                Common(
                    genre=re.compile("history"),
                    subject=re.compile("(america|u.s.)"),
                    first_must_match=False
                )
            ]
        )

    def test_native_american_misspelled(self):
        # NOTE: this passes on ES1; the results aren't quite as good as they
        # would be if the search term were spelled correctly, but are still
        # very reasonable.  Fails on ES6.

        # Keyword, misspelled
        self.search(
            "Native amerixan", [
                Common(
                    genre=re.compile("history"),
                    subject=re.compile("(america|u.s.)"),
                    first_must_match=False,
                    threshold=0.4
                )
            ]
        )

    def test_presentations(self):
        self.search(
            "presentations",
            Common(
                subject=re.compile("(language arts|business presentations|business|management)")
            )
        )

    def test_managerial_skills(self):
        # NOTE: This works on ES6.  On ES1, the first few results are good, but then
        # it devolves into books from a fantasy series called "The Menagerie."
        self.search(
            "managerial skills",
            Common(
                subject=re.compile("(management)")
            )
        )

    def test_pattern_making(self):
        self.search(
            "Pattern making",
            AtLeastOne(subject=re.compile("crafts"))
        )

    def test_patterns_of_fashion(self):
        # NOTE: this specific title isn't in the collection, but the results
        # should still be reasonably relevant.  This works on ES1 but not ES6.

        self.search(
            "Patterns of fashion", [
                AtLeastOne(subject=re.compile("crafts")),
                Common(title=re.compile("(patterns|fashion)"))
            ]
        )

    def test_plant_based(self):
        self.search(
            "Plant based",
            Common(
                subject=re.compile("(cooking|food|nutrition|health)")
            )
        )

    def test_meditation(self):
        self.search(
            "Meditation",
            Common(
                genre=re.compile("(self-help|mind|spirit)")
            )
        )

    def test_college_essay(self):
        self.search(
            "College essay",
            Common(
                genre=re.compile("study aids"),
                subject=re.compile("college")
            )
        )

    def test_grade_and_subject(self):
        # NOTE: this doesn't work on either version of ES.  The top result's genre
        # is science fiction rather than science.
        self.search(
            "Seventh grade science",
            [
                Common(target_age=(12, 13)),
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
            "stephen king",
                [ SpecificAuthor("Stephen King", accept_book_about_author=True),
                  Common(author="Stephen King", threshold=0.7) ]
        )

    def test_fleming(self):
        # It's reasonable for there to be a biography of this author in the search
        # results, but the overwhelming majority of the results should be books by him.
        self.search(
            "ian fleming",
            [ SpecificAuthor("Ian Fleming", accept_book_about_author=True),
              Common(author="Ian Fleming", threshold=0.9) ]
        )

    def test_plato(self):
        # The majority of the search results will be _about_ this author,
        # but there should also be some _by_ him.
        self.search(
            "plato",
                [ SpecificAuthor("Plato", accept_book_about_author=True),
                  AtLeastOne(author="Plato") ]
        )

    def test_byron(self):
        # The user probably wants either a biography of Byron or a book of
        # his poetry.
        self.search(
            "Byron", [
                AtLeastOne(title=re.compile("byron"), genre=re.compile("biography")),
                AtLeastOne(author=re.compile("byron"), genre="poetry")
            ]
        )

    def test_hemingway(self):
        # NOTE: this doesn't work in either version of ES.  All of the top
        # results are books about, rather than by, Hemingway.  It makes sense
        # that the title is being boosted (this is not necessarily a problem!),
        # but on the other hand, I would imagine that most users searching "Hemingway"
        # are trying to find books _by_ him.  Maybe there would be a way to at least
        # boost the biographies over him over the novels which have him as a character?

        # The majority of the search results should be _by_ this author,
        # but there should also be at least one _about_ him.
        self.search(
            "Hemingway",
                [ Common(author="Ernest Hemingway"),
                  AtLeastOne(title=re.compile("Hemingway")) ]
        )

    def test_lagercrantz(self):
        # The search query contains only the author's last name.
        self.search(
            "Lagercrantz", SpecificAuthor("Rose Lagercrantz")
        )

    def test_burger(self):
        # The author is correctly prioritized above books whose titles contain
        # the word "burger."
        self.search(
            "wolfgang burger", SpecificAuthor("Wolfgang Burger")
        )

    def test_chase(self):
        # The author is correctly prioritized above the book "Emma."
        self.search(
            "Emma chase", SpecificAuthor("Emma Chase")
        )

    def test_deirdre_martin(self):
        # The author's first name is misspelled in the search query.
        self.search(
            "deidre martin", SpecificAuthor("Deirdre Martin")
        )

    def test_wharton(self):
        # The author's last name is misspelled in the search query.
        self.search(
            "edith warton", SpecificAuthor("Edith Wharton")
        )

    def test_danielle_steel(self):
        # NOTE: this works, but setting the threshold to anything higher than
        # the default 0.5 causes it to fail (even though she's written
        # a LOT of books!).  Fixing the typo makes the test work even with the
        # threshold set to 1.

        # The author's last name is slightly misspelled in the search query.
        self.search(
            "danielle steele",
            [   SpecificAuthor("Danielle Steel"),
                Common(author="Danielle Steel")
            ]
        )

    def test_nabokov(self):
        # Only the last name is provided in the search query,
        # and it's misspelled.
        self.search(
            "Nabokof", SpecificAuthor("Vladimir Nabokov")
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

    def test_mankel(self):
        # The search query lists the author's last name before his first name.
        self.search(
            "mankel henning", SpecificAuthor("Henning Mankel")
        )

    def test_author_with_language(self):
        # NOTE: this doesn't work on either version of ES; the first Spanish result
        # is #3

         self.search(
            "Pablo escobar spanish",
            FirstMatch(author="Pablo Escobar", language="spa")
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

    def test_harry_potter_1(self):
        # This puts foreign-language titles above English titles, but
        # that's fine because our search document doesn't include a
        # language filter.
        self.search(
            "Harry potter",
            Common(series="Harry Potter", threshold=0.9)
        )

    def test_harry_potter_2(self):
        # NOTE: this does bring up the target book as the first result, but, ideally,
        # other books from the same series would also show up at some point.  (Not
        # necessarily a problem; just would be nice to have, particularly since there
        # are other searches that do work that way.)
        self.search(
            "chamber of secrets", [
                FirstMatch(title="Harry Potter and the Chamber of Secrets"),
                Common(series="Harry Potter", threshold=0.3)
            ]
        )

    def test_maisie_dobbs(self):
        # Misspelled proper noun
        self.search(
            "maise dobbs",
            Common(series="Maisie Dobbs", threshold=0.5)
        )

    def test_gossip_girl(self):
        # Misspelled common word

        # TODO: We have a lot of books in this series, but only two of them have
        # .series set. It would be useful to specify the number of expected matches in the
        # SpecificSeries constructor, rather than a percentage.
        self.search(
            "Gossip hirl",
            Common(series="Gossip Girl"), limit=4
        )

    def test_magic(self):
        # This book isn't in the collection, but the results include other books from
        # the same series.
        self.search(
            "Frogs and french kisses",
            AtLeastOne(series="Magic in Manhattan")
        )

    def test_goosebumps(self):
        for q in ('goosebumps', 'goosebump'):
            self.search(q, Common(series="Goosebumps", threshold=0.9))

    def test_goosebumps_misspelled(self):
            self.search("gosbums", Common(series="Goosebumps"))

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

    def test_dark_tower(self):
        # Most of the books in this series don't have .series set--hence searching
        # by the author instead.  There exist two completely unrelated books which
        # happen to be entitled "The Dark Tower"--it's fine for one of those to be
        # the first result.
        self.search(
            "The dark tower", [
                Common(author="Stephen King", first_must_match=False),
                AtLeastOne(series="The Dark Tower")
            ]
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
        for q in ("allien", "aluens"):
            self.search(
                q,
                Common(
                    subject=re.compile("(alien|extraterrestrial)"),
                    first_must_match=False
                )
            )

    def test_anime_genre(self):
        # NOTE: this doesn't work; all of the top results in both versions of ES6
        # are for "animal" rather than "anime."
        #
        # 'anime' and 'manga' are not subject classifications we get
        # from our existing vendors. We have a lot of these books but
        # they're not classified under those terms. -LR
        self.search(
            "anime",
            Common(subject=re.compile("(manga|anime)"))
        )

    def test_batman(self):
        # NOTE: doesn't work.  (The results for "batman" as one word are as
        # expected, though.)

        # Patron is searching for 'batman' but treats it as two words.
        self.search(
            "bat man book",
            Common(title=re.compile("batman"))
        )

    def test_spiderman(self):
        # Patron is searching for 'spider-man' but treats it as one word.
        for q in ("spiderman", "spidermanbook"):
            self.search(
                q, Common(title=re.compile("spider-man"))
            )

    def test_texas_fair(self):
        # There exist a few books about the Texas state fair, but none of them
        # are in the collection, so the best outcome is that the results will
        # include a lot of books about Texas
        self.search(
            "books about texas like the fair",
            Common(title=re.compile("texas"))
        )

    def test_boy_saved_baseball(self):
        # NOTE: The target title ("The Boy who Saved Baseball") isn't in the collection,
        # but, ideally, most of the top results should still be about baseball.
        # This works on ES6, but not on ES1.  (On ES1, several of the top results
        # are romance novels.)
        self.search(
            "boy saved baseball",
            Common(subject=re.compile("baseball"))
        )

    def test_chapter_books(self):
        # This works:
        self.search(
            "chapter books", Common(target_age=(6, 10))
        )
        # This doesn't; all of the top results are stand-alone excerpts from
        # a travel book series, marked "Guidebook Chapter":
        self.search(
            "chapter bookd", Common(target_age=(6, 10))
        )
        # This doesn't either; the first few results are accurate, but the
        # subsequent ones are a mixture of picture books and books for adults.
        self.search(
            "chaptr books", Common(target_age=(6, 10))
        )

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
        # This appears to be a subject search, e.g. for poems which are deep.
        self.search(
            "deep poems",
            Common(genre="Poetry")
        )

    def test_dinosaur_cove(self):
        self.search(
            "dinosuar cove",
            Common(series="Dinosaur Cove")
        )

    def test_dirtbike(self):
        # NOTE: Not only are the results irrelevant, most of them are for
        # adult books ("dirtbike" is evidently getting corrected to "dirty").
        # (Not problematic in and of itself, since the lane would filter those
        # out on the front end, but maybe an indication that there's room for
        # improvement in the way that misspelled words are handled.)
        # Searching "dirt bike" (as two words) fixes the second problem and
        # renders more relevant results, but still not enough for the
        # test to pass.
        self.search(
            "dirtbike",
            Common(
                subject=re.compile("(bik|bicycle|sports|nature|travel)")
            )
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
            "information technology",
            Common(
                subject=re.compile("(information technology|computer)"),
            )
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
        self.search(
            "manga",
            [
                Common(title=re.compile("manga")),
                Common(subject=re.compile("(manga|art|comic)")),
            ]
        )

    def test_my_little_pony(self):
        # .series is not set for these titles.
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
        self.search("scary stories", Common(genre="Horror"))

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
        for q in ('timothy zahn', 'timithy zahn'):
            self.search(q, Common(author="Timothy Zahn"))

    def test_who_is(self):
        # These children's biographies don't have .series set but
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
