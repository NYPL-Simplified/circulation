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
# Run the tests with this command:
#
# $ nosetests integration_tests/test_search.py

from functools import wraps
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
from core.util.personal_names import (
    display_name_to_sort_name,
    sort_name_to_display_name,
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

def known_to_fail(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        try:
            ignore = f(*args, **kwargs)
        except Exception, e:
            logging.debug("Expected this test to fail, and it did: %r" % e)
            return
        raise Exception("Expected this test to fail, and it didn't! Congratulations?")
    return decorated

class Searcher(object):
    """A class that knows how to perform searches."""
    def __init__(self, library, index):
        self.library = library
        self.filter = Filter(collections=self.library)
        self.index = index

    def query(self, query, pagination):
        return self.index.query_works(
            query, filter=self.filter, pagination=pagination,
            debug=True
        )


class Evaluator(object):
    """A class that knows how to evaluate search results."""

    log = logging.getLogger("Search evaluator")

    def __init__(self, **kwargs):
        self.kwargs = dict()
        self.original_kwargs = dict()
        for k, v in kwargs.items():
            self.original_kwargs[k] = v
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
            print(
                "Need %d%% matches, got %d%%" % (
                    threshold*100, actual*100
                )
            )
            for hit in hits:
                print(hit)
        assert actual >= threshold

    def _match_scalar(self, value, expect):
        if hasattr(expect, 'search'):
            if expect:
                success = expect.search(value)
            else:
                success = False
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

    def _match_author(self, author, result):
        for contributor in result.contributors:
            if not contributor.role in Filter.AUTHOR_MATCH_ROLES:
                continue
            names = [
                contributor[field].lower() for field in ['display_name', 'sort_name']
                if contributor[field]
            ]
            if hasattr(author, 'match'):
                match = any(author.search(name) for name in names)
            else:
                match = any(author == name for name in names)
            if match:
                return True, author, contributor
        else:
            return False, author, None

    def match_result(self, result):
        """Does the given result match these criteria?"""

        for field, expect in self.kwargs.items():
            if field == 'subject':
                success, value, expect_str = self._match_subject(expect, result)
            elif field == 'genre':
                success, value, expect_str = self._match_genre(expect, result)
            elif field == 'target_age':
                success, value, expect_str = self._match_target_age(expect, result)
            elif field == 'author':
                success, value, expect_str = self._match_author(expect, result)
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
                 negate=False, **kwargs):
        """Constructor

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
        self.negate = negate

    def evaluate_first(self, hit):
        if self.first_must_match:
            success, actual, expected = self.match_result(hit)
            if hasattr(actual, 'match'):
                actual = actual.pattern
            if (not success) or (self.negate and success):
                if self.negate:
                    if actual == expected:
                        print(
                            "First result matched and shouldn't have. %s == %s", expected, actual
                        )
                    assert actual != expected
                else:
                    if actual != expected:
                        print(
                            "First result did not match. %s != %s" % (expected, actual)
                        )
                    eq_(actual, expected)

    def evaluate_hits(self, hits):
        successes, failures = self.multi_evaluate(hits)
        if self.threshold is not None:
            self.assert_ratio(
                [x[1:] for x in successes],
                [x[1:] for x in successes+failures],
                self.threshold,
            )
        if self.minimum is not None:
            overall_success = len(successes) >= self.minimum
            if not overall_success:
                print(
                    "Need %d matches, got %d" % (self.minimum, len(successes))
                )
                for i in (successes+failures):
                    if i in successes:
                        template = 'Y (%s == %s)'
                    else:
                        template = 'N (%s != %s)'
                    vars = []
                    for display in i[1:]:
                        if hasattr(display, 'match'):
                            display = display.pattern
                        vars.append(display)
                    print(template % tuple(vars))
            assert overall_success

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

    def __init__(self, author, accept_title=None, threshold=0):
        super(SpecificAuthor, self).__init__(author=author, threshold=threshold)
        if accept_title:
            self.accept_title = accept_title.lower()
        else:
            self.accept_title = None

    def author_role(self, expect_author, result):
        if hasattr(expect_author, 'match'):
            def match(author):
                return (
                    expect_author.search(author.display_name)
                    or expect_author.search(author.sort_name)
                )
        else:
            expect_author_sort = display_name_to_sort_name(expect_author)
            expect_author_display = sort_name_to_display_name(expect_author)
            def match(author):
                return (
                    contributor.display_name == expect_author
                    or contributor.sort_name == expect_author
                    or contributor.sort_name == expect_author_sort
                    or contributor.display_name == expect_author_display
                )
        for contributor in result.contributors or []:
            if match(contributor):
                return contributor.role
        else:
            return None

    def evaluate_first(self, first):
        expect = self.original_kwargs['author']
        if self.author_role(expect, first) is not None:
            return True

        title = self._field('title', first)
        if self.accept_title and self.accept_title in title:
            return True

        # We have failed.
        eq_(expect, first.contributors)

    def evaluate_hits(self, hits):
        last_role = None
        last_title = None
        author = self.original_kwargs['author']
        authors = [hit.contributors for hit in hits]
        author_matches = []
        for hit in hits:
            role = self.author_role(author, hit)
            author_matches.append(role is not None)
            last_role = role
            last_title = hit.title
        self.assert_ratio(author_matches, authors, self.threshold)


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

class VariantSearchTest(SearchTest):
    """A test suite that runs different searches but evaluates the
    results against the same evaluator every time.
    """
    EVALUATOR = None

    def search(self, query):
        return super(VariantSearchTest, self).search(query, self.EVALUATOR)


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
        # To a human eye this is obviously gibberish, but it's close
        # enough to English words that it could pick up a few results.
        self.search(
            "asdfza oiagher ofnalqk",
            ReturnsNothing()
        )


class TestTitleMatch(SearchTest):
    # A search for one specific book. We want that book to be the
    # first result. The rest of the results are usually irrelevant.

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
        self.search("blind assassin", FirstMatch(
            title=re.compile("^(the )?blind assassin$"),
            author="Margaret Atwood")
        )

    def test_simple_title_match_dry(self):
        self.search("the dry", FirstMatch(title="The Dry"))

    def test_simple_title_match_origin(self):
        self.search("origin", FirstMatch(title="Origin"))

    def test_simple_title_match_goldfinch(self):
        # This book is available as both "The Goldfinch" and "Goldfinch"
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

    def test_genius_foods(self):
        # In addition to an exact title match, we also check that
        # food-related books show up in the search results.
        self.search(
            "genius foods", [
                FirstMatch(title="Genius Foods"),
                Common(
                    genre=re.compile("(cook|diet)"),
                    threshold=0.2
                )
            ]
        )

    def test_possessives(self):
        # Verify that possessives are stemmed.
        self.search(
            "washington war",
            AtLeastOne(title="George Washington's War")
        )

        self.search(
            "washingtons war",
            AtLeastOne(title="George Washington's War")
        )


    def test_it(self):
        # The book "It" is correctly prioritized over books whose titles contain
        # the word "it."
        self.search(
            "It",
            FirstMatch(title="It")
        )

    def test_girl_on_the_train(self):
        # There's a different book called "The Girl in the Train".
        self.search(
            "girl on the train",
            FirstMatch(title="The Girl On The Train")
        )


class TestUnownedTitle(SearchTest):
    # These are title searches for books not owned by NYPL.
    # Because of this we check that _similar_ books are returned.
    #
    # If your library owns these titles, then your results may be
    # different for these tests.

    def test_boy_saved_baseball(self):
        # The target title ("The Boy who Saved Baseball") isn't in the
        # collection, but, ideally, most of the top results should
        # still be about baseball.
        self.search(
            "boy saved baseball",
            Common(subject=re.compile("baseball"))
        )

    def test_save_cat(self):
        # This specific book isn't in the collection, but there's a
        # book with a very similar title, which is the first result.
        self.search(
            "Save the Cat", 
            [Common(title=re.compile("save the cat"), threshold=0.1),
             Common(title=re.compile("(save|cat)"), threshold=1)]
        )

    def test_minecraft_zombie(self):
        # We don't have this specific title, but there's no shortage of
        # Minecraft books.
        self.search(
            "Diary of a minecraft zombie",
            Common(summary=re.compile("minecraft", re.I))

        )

    def test_pie(self):
        # NOTE: "Pie Town Woman" isn't in the collection, but there's
        # a book called "Pie Town," which seems like the clear best
        # option for the first result.
        self.search("Pie town woman", FirstMatch(title="Pie Town"))

    @known_to_fail
    def test_divorce(self):
        # This gets a large number of titles that start with
        # "The Truth About..."
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

    def test_patterns_of_fashion(self):
        # This specific title isn't in the collection, but the results
        # should still be reasonably relevant.
        self.search(
            "Patterns of fashion", [
                AtLeastOne(subject=re.compile("crafts")),
                Common(title=re.compile("(pattern|fashion)"),
                       first_must_match=False)
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

    def test_title_match_with_genre_name(self):
        # This book is unowned and its title includes a genre name.
        # We're going to get a lot of books with "life" or "spy" in
        # the title.
        #
        # The first result is a book that fills the intent of the
        # search query but doesn't say "spy" anywhere.
        self.search(
            "My life as a spy",
            Common(title=re.compile("life|spy"), first_must_match=False,
                   threshold=0.9)
        )

    @known_to_fail
    def test_nonexistent_title_tower(self):
        # NOTE: there is no book with this title.  The most likely
        # scenario is that the user meant "The Dark Tower." The only
        # way to get this to work in Elasticsearch might be to
        # institute a big synonym filter.
        self.search("The night tower", FirstMatch(title="The Dark Tower"))


class TestMisspelledTitleSearch(SearchTest):
    # Test title searches where the title is misspelled.

    @known_to_fail
    def test_allegiant(self):
        # A very bad misspelling.
        self.search(
            "alliagent",
            FirstMatch(title="Allegiant")
        )

    @known_to_fail
    def test_marriage_lie(self):
        # NOTE: "The Marriage Lie" (which is presumably what the user
        # wanted, and which is in the collection) isn't on the first
        # page of results.
        self.search(
            "Marriage liez",
            FirstMatch(title="The Marriage Lie")
        )

    @known_to_fail
    def test_invisible_emmie(self):
        # One word in the title is slightly misspelled. This makes
        # "Emmy & Oliver" show up before the correct title match, which
        # isn't too bad.
        self.search(
            "Ivisible emmie", FirstMatch(title="Invisible Emmie")
        )

    @known_to_fail
    def test_karamazov(self):
        # Extremely uncommon proper noun, slightly misspelled
        #
        # The desired book does not show up in the first page --
        # it's all books called "Brothers".
        self.search(
            "Brothers karamzov",
            FirstMatch(title="The Brothers Karamazov")
        )

    def test_restless_wave(self):
        # One common word in the title is slightly misspelled.
        self.search(
            "He restless wave",
            FirstMatch(title="The Restless Wave")
        )

    @known_to_fail
    def test_kingdom_of_the_blind(self):
        # The first word, which is a fairly common word, is slightly misspelled.
        #
        # The desired book is not on the first page.
        self.search(
            "Kngdom of the blind",
            FirstMatch(title="The Kingdom of the Blind")
        )

    def test_seven_husbands(self):
        # Two words--1) a common word which is spelled as a different word
        # ("if" instead of "of"), and 2) a proper noun--are misspelled.
        self.search(
            "The seven husbands if evyln hugo",
            FirstMatch(title="The Seven Husbands of Evelyn Hugo")
        )

    @known_to_fail
    def test_nightingale(self):
        # The top results are works like "Modern Warfare, Intelligence,
        # and Deterrence."

        # Unusual word, misspelled
        self.search(
            "The nightenale",
            FirstMatch(title="The Nightingale")
        )

    @known_to_fail
    def test_memoirs_geisha(self):
        # The desired work shows up on the first page, but it should
        # be first.
        self.search(
            "Memoire of a ghesia",
            FirstMatch(title="Memoirs of a Geisha")
        )

    def test_healthyish(self):
        # Misspelling of the title, which is a neologism.
        self.search(
            "healtylish", FirstMatch(title="Healthyish")
        )

    def test_zodiac(self):
        # Uncommon word, slightly misspelled.
        self.search(
            "Zodiaf", FirstMatch(title="Zodiac")
        )

    def test_for_whom_the_bell_tolls(self):
        # A relatively common word is spelled as a different, more common word.
        self.search(
            "For whom the bell tools",
            FirstMatch(title="For Whom the Bell Tolls")
        )

    @known_to_fail
    def test_came_to_baghdad(self):
        # An extremely common word is spelled as a different word.
        self.search(
            "They cane to baghdad",
            FirstMatch(title="They Came To Baghdad")
        )

    def test_genghis_khan(self):
        self.search(
            "Ghangiz Khan",
            AtLeastOne(title=re.compile("Genghis Khan", re.I))
        )

    def test_guernsey(self):
        # One word, which is a place name, is misspelled.
        self.search(
            "The gurnsey literary and potato peel society",
            FirstMatch(title="The Guernsey Literary & Potato Peel Society")
        )

    @known_to_fail
    def test_british_spelling_color_of_our_sky(self):
        # The book we're looking for is on the first page, but
        # below "The Weight of Our Sky"
        #
        # Note to pedants: the title of the book as published is
        # "The Color of Our Sky".
        self.search(
            "The colour of our sky",
            FirstMatch(title="The Color of Our Sky")
        )


class TestPartialTitleSearch(SearchTest):
    # Test title searches where only part of the title is provided.

    @known_to_fail
    def test_i_funnyest(self):
        # An important word from the middle of the title is omitted.
        self.search(
            "i funnyest",
            AtLeastOne(title="I Totally Funniest"),
        )

    def test_future_home(self):
        # The search query only contains half of the title.
        self.search(
            "Future home of",
            FirstMatch(title="Future Home Of the Living God")
        )

    def test_fundamentals_of_supervision(self):
        # A word from the middle of the title is missing.
        self.search(
            "fundamentals of supervision",
            FirstMatch(title="Fundamentals of Library Supervision")
        )

    def test_hurin(self):
        # A single word is so unusual that it can identify the book
        # we're looking for.
        for query in (
            "Hurin", u"Húrin"
        ):
            self.search(
                query,
                FirstMatch(
                    title=u"The Children of Húrin", author=re.compile("tolkien")
                )
            )

    @known_to_fail
    def test_open_wide(self):
        # Search query cuts off midway through the second word of the subtitle.
        #
        # The book we're looking for is on the first page, but underneath two
        # other books called "open wide" or "wide open".
        self.search(
            "Open wide a radical",
            FirstMatch(
                title="Open Wide",
                subtitle="a radically real guide to deep love, rocking relationships, and soulful sex"
            )
        )

    def test_how_to_win_friends(self):
        # The search query only contains half of the title.
        self.search(
            "How to win friends",
            FirstMatch(title="How to Win Friends and Influence People")
        )

    def test_wash_your_face_1(self):
        # The search query is missing the last word of the title.
        self.search(
            "Girl wash your",
            FirstMatch(title="Girl, Wash Your Face")
        )

    def test_wash_your_face_2(self):
        # The search query is missing the first word of the title.
        self.search(
            "Wash your face",
            FirstMatch(title="Girl, Wash Your Face")
        )

    def test_theresa(self):
        # The search results correctly prioritize books with titles containing
        # "Theresa" over books by authors with the first name "Theresa."
        self.search(
            "Theresa",
            FirstMatch(title=re.compile("Theresa"))
        )

    def test_prime_of_miss_jean_brodie(self):
      # The search query only has the first and last words from the title, and
      # the last word is misspelled.
      self.search(
        "Prime brody",
        FirstMatch(title="The Prime of Miss Jean Brodie")
      )


class TestTitleGenreConflict(SearchTest):
    # These tests address a longstanding problem of books whose titles
    # contain the names of genres.

    @known_to_fail
    def test_drama(self):
        # The title of the book is the name of a genre, and another
        # genre has been added to the search term to clarify it.
        #
        # NOTE: This probably fails because "Drama" is parsed as a
        # genre name but "comic" is not parsed as "Comics & Graphic
        # Novels"
        self.search(
            "drama comic",
            FirstMatch(title="Drama", author="Raina Telgemeier")
        )

    def test_title_match_with_genre_name_romance(self):
        # The title contains the name of a genre. Despite this,
        # an exact title match should show up first.
        self.search(
            "modern romance", FirstMatch(title="Modern Romance")
        )

    def test_modern_romance_with_author(self):
        self.search(
            "modern romance aziz ansari",
            FirstMatch(title="Modern Romance", author="Aziz Ansari")
        )

    def test_title_match_with_genre_name_law(self):
        self.search(
            "law of the mountain man",
            FirstMatch(title="Law of the Mountain Man")
        )

    @known_to_fail
    def test_law_of_the_mountain_man_with_author(self):
        # "Law of the Mountain Man" is the second result, but it
        # really should be first. Maybe the first result here has
        # William Johnstone as the primary author instead of a regular
        # author.
        self.search(
            "law of the mountain man william johnstone",
            [
                FirstMatch(title="Law of the Mountain Man"),
                Common(author="William Johnstone"),
            ]
        )

    def test_spy(self):
        self.search(
            "spying on whales",
            FirstMatch(title="Spying on Whales")
        )

    def test_dance(self):
        self.search(
            "dance with dragons",
            FirstMatch(title="A Dance With Dragons")
        )


class TestTitleAuthorConflict(SearchTest):
    # Test title searches for works whose titles contain words
    # that often show up in peoples' names.

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

        # NOTE: The first result is a book whose .series is the literal
        # string "Disney". This triggers a keyword series match which
        # bumps it to the top. That's why first_must_match=False.
        self.search(
            "disney",
                [ Common(title=re.compile("disney"), first_must_match=False),
                  AtLeastOne(title=re.compile("walt disney")),
                  AtLeastOne(author="Disney Book Group") ]
        )

    def test_bridge(self):
        # The search results correctly prioritize the book with this
        # title over books by authors whose names contain "Luis" or
        # "Rey."
        self.search(
            "the bridge of san luis rey",
            FirstMatch(title="The Bridge of San Luis Rey")
        )


class TestTitleAudienceConflict(SearchTest):
    # Test titles searches for books whose titles contain the
    # name of an audience or target age.

    def test_title_match_with_audience_name_children(self):
        self.search(
            "Children blood",
            FirstMatch(title="Children of Blood and Bone")
        )

    def test_title_match_with_audience_name_kids(self):
        self.search(
            "just kids",
            FirstMatch(title="Just Kids")
        )

    def test_tales_of_a_fourth_grade_nothing(self):
        self.search(
            "fourth grade nothing",
            FirstMatch(title="Tales of a Fourth Grade Nothing")
        )


class TestMixedTitleAuthorMatch(SearchTest):

    @known_to_fail
    def test_centos_caen(self):
        # 'centos' shows up in the subtitle. 'caen' is the name
        # of one of the authors.
        #
        # NOTE: The work we're looking for shows up on the first page
        # but it really ought to befirst.
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

    def test_dostoyevsky_partial_title(self):
        # Partial title, partial author
        self.search(
            "punishment Dostoyevsky",
            FirstMatch(title="Crime and Punishment")
        )

    @known_to_fail
    def test_sparks(self):
        # Full title, full but misspelled author, "by"
        # NOTE: Work shows up on first page but ought to be first.
        self.search(
            "Every breath by nicholis sparks",
            FirstMatch(title="Every Breath", author="Nicholas Sparks")
        )

    @known_to_fail
    def test_grisham(self):
        # Full title, author's first name only
        #
        # NOTE: Results are all books with "Reckoning" in the title
        # but the John Grisham one is not on the first page.
        self.search(
            "The reckoning john grisham",
            FirstMatch(title="The Reckoning", author="John Grisham")
        )

    @known_to_fail
    def test_singh(self):
        # NOTE: The search results aren't sufficiently prioritizing
        # titles containing "archangel" over the author's other books.
        self.search(
            "Nalini singh archangel",
            [ Common(author="Nalini Singh", threshold=0.9),
              Common(title=re.compile("archangel")) ]
        )

    def test_sebald_1(self):
        # This title isn't in the collection, but the author's other
        # books should still come up.
        self.search(
            "Sebald after",
            SpecificAuthor("W. G. Sebald", accept_title="Sebald")
        )

    def test_sebald_2(self):
        # Specifying the full title gets rid of the book about
        # this author, probably because "Nature" is the name of a genre.
        self.search(
            "Sebald after nature",
            SpecificAuthor("W. G. Sebald")
        )

# Classes that test many different variant searches for a specific
# title.
#
class TestTheHateUGive(VariantSearchTest):
    """Test various ways of searching for "The Hate U Give"."""

    # We check the start of the title because for some reason we have
    # a copy of the book that includes the author's name in the title.
    EVALUATOR = FirstMatch(title=re.compile("^The Hate U Give", re.I))

    def test_correct_spelling(self):
        self.search("the hate u give")

    def test_with_all(self):
        self.search("all the hate u give")

    def test_with_all_and_you(self):
        self.search("all the hate you give")

    def test_with_you(self):
        self.search("hate you give")

    @known_to_fail
    def test_with_you_misspelled(self):
        self.search("hate you gove")


class TestCharlottesWeb(VariantSearchTest):
    """Test various ways of searching for "Charlotte's Web"."""

    EVALUATOR = FirstMatch(title="Charlotte's Web")

    def test_with_apostrophe(self):
        self.search("charlotte's web")

    def test_without_possessive(self):
        self.search("charlotte web")

    def test_without_apostrophe(self):
        self.search("charlottes web")

    @known_to_fail
    def test_misspelled_no_apostrophe(self):
        self.search("charlettes web")

    def test_no_apostrophe_with_author(self):
        self.search("charlottes web eb white")

    @known_to_fail
    def test_no_apostrophe_with_author_space(self):
        # NOTE: This promotes several other E. B. White titles
        # over "Charlotte's Web".
        self.search("charlottes web e b white")


class TestChristopherMouse(VariantSearchTest):
    # Test various partial title spellings for "Christopher Mouse: The Tale
    # of a Small Traveler".
    #
    # This title is not in NYPL's collection, so we don't expect any of
    # these tests to pass.
    EVALUATOR = FirstMatch(title=re.compile("Christopher Mouse"))

    @known_to_fail
    def test_correct_spelling(self):
        self.search("christopher mouse")

    @known_to_fail
    def test_misspelled_1(self):
        self.search("chistopher mouse")

    @known_to_fail
    def test_misspelled_2(self):
        self.search("christopher moise")

    @known_to_fail
    def test_misspelled_3(self):
        self.search("chistoper muse")


class TestSubtitleMatch(SearchTest):
    # Test searches for words that show up based on a remembered
    # subtitle.

    def test_shame_stereotypes(self):
        # "Sister Citizen" has both search terms in its
        # subtitle.
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
                [ SpecificAuthor("Stephen King", accept_title="Stephen King"),
                  Common(author="Stephen King", threshold=0.7) ]
        )

    def test_fleming(self):
        # It's reasonable for there to be a biography of this author in the search
        # results, but the overwhelming majority of the results should be books by him.
        self.search(
            "ian fleming",
            [ SpecificAuthor("Ian Fleming", accept_title="Ian Fleming"),
              Common(author="Ian Fleming", threshold=0.9) ]
        )

    def test_plato(self):
        # The majority of the search results will be _about_ this author,
        # but there should also be some _by_ him.
        self.search(
            "plato",
                [ SpecificAuthor("Plato", accept_title="Plato"),
                  AtLeastOne(author="Plato") ]
        )

    def test_byron(self):
        # The user probably wants either a biography of Byron or a book of
        # his poetry.
        #
        # TODO: Books about Byron are consistently prioritized above books by him.
        self.search(
            "Byron", [
                AtLeastOne(title=re.compile("byron"), genre=re.compile("biography")),
                AtLeastOne(author=re.compile("byron"))
            ]
        )

    def test_hemingway(self):
        # TODO: Books about Hemingway are consistently prioritized above books by him.

        # The majority of the search results should be _by_ this author,
        # but there should also be at least one _about_ him.
        self.search(
            "Hemingway", [
                AtLeastOne(title=re.compile("hemingway"), genre=re.compile("biography")),
                AtLeastOne(author="Ernest Hemingway")
            ]
        )

    def test_lagercrantz(self):
        # The search query contains only the author's last name.
        # There are several people with this name, and there's no
        # information that would let us prefer one over the other.
        self.search(
            "Lagercrantz", SpecificAuthor(re.compile("Lagercrantz"))
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

    @known_to_fail
    def test_deirdre_martin(self):
        # The author's first name is misspelled in the search query.
        #
        # The search results are title matches against 'Martin'
        self.search(
            "deidre martin", SpecificAuthor("Deirdre Martin")
        )

    def test_wharton(self):
        self.search(
            "edith wharton",
            SpecificAuthor("Edith Wharton", accept_title="Edith Wharton")
        )

    def test_wharton_misspelled(self):
        # The author's last name is misspelled in the search query.
        #
        # TODO: Apparently this causes Elasticsearch to get hung up on a
        # subject match for 'England and Wales', but we get at least
        # one Edith Wharton book.
        self.search(
            "edith warton", AtLeastOne(author="Edith Wharton")
        )

    def test_danielle_steel(self):
        # NOTE: this works, but setting the threshold to anything higher than
        # the default 0.5 causes it to fail (even though she's written
        # a LOT of books!).  Fixing the typo makes the test work even with the
        # threshold set to 1.

        # Since "steel" is an English word, we don't do a fuzzy author search.

        # The author's last name is slightly misspelled in the search query.
        self.search(
            "danielle steele",
            [   SpecificAuthor("Danielle Steel"),
                Common(author="Danielle Steel")
            ]
        )

    def test_primary_author_with_coauthors(self):
        # This person is sometimes credited as primary author with
        # other authors, and sometimes as just a regular co-author.
        self.search(
            "steven peterman",
            SpecificAuthor("Steven Peterman")
        )

    def test_primary_author_with_coauthors_2(self):
        self.search(
            "jack cohen",
            SpecificAuthor("Jack Cohen")
        )

    def test_only_as_coauthor(self):
        # This person is inevitably credited co-equal with another
        # author.
        self.search(
            "stan berenstain",
            SpecificAuthor("Stan Berenstain")
        )

    def test_narrator(self):
        # This person is narrator for a lot of Stephen King
        # audiobooks. Searching for their name is likely to bring up
        # people with similar names and authorship roles, but they'll
        # show up pretty frequently.
        self.search(
            "will patton",
            Common(author="Will Patton", first_must_match=False)
        )

    def test_unknown_display_name(self):
        # In NYPL's dataset, we know the sort name for this author but
        # not the display name.
        self.search(
            "emma craigie",
            SpecificAuthor("Craigie, Emma")
        )

    def test_nabokov_misspelled(self):
        # Only the last name is provided in the search query,
        # and it's misspelled.
        self.search(
            "Nabokof",
            SpecificAuthor("Vladimir Nabokov", accept_title="Nabokov")
        )

    def test_ba_paris(self):
        # Author's last name could also be a subject keyword.
        self.search(
            "b a paris", SpecificAuthor("B. A. Paris")
        )

    def test_griffiths(self):
        # The search query gives the author's sort name.
        #
        # The first two matches are for "Doom of the Griffiths" by
        # Elizabeth Gaskell and "Ellis Island" by Kate Kerrigan.
        self.search(
            "Griffiths elly", SpecificAuthor("Elly Griffiths")
        )

    def test_christian_kracht(self):
        # The author's name contains a genre name.
        self.search(
            "christian kracht",
            FirstMatch(author="Christian Kracht")
        )

    def test_dan_gutman(self):
        self.search("gutman, dan", Common(author="Dan Gutman"))

    def test_dan_gutman_with_series(self):
        self.search(
            "gutman, dan the weird school",
            Common(series=re.compile("my weird school"), author="Dan Gutman")
        )

    def test_steve_berry(self):
        # This search has been difficult in the past.
        self.search("steve berry", Common(author="Steve Berry"))


    @known_to_fail
    def test_thomas_python(self):
        # All the terms are correctly spelled words, but the patron
        # clearly means something else.
        self.search(
            "thomas python",
            Common(author="Thomas Pynchon")
        )

    @known_to_fail
    def test_betty_neels_audiobooks(self):
        # NOTE: Even though there are no audiobooks, all of the search results should still
        # be books by this author.  This works on ES1, but the ES6 search results devolve
        # into Betty Crocker cookbooks.

        self.search(
            "Betty neels audiobooks",
            Common(author="Betty Neels", genre="romance", threshold=1)
        )

# Classes that test many different variant searches for a specific
# author.
#

class TestTimothyZahn(VariantSearchTest):
    # Test ways of searching for author Timothy Zahn.
    EVALUATOR = SpecificAuthor("Timothy Zahn")

    def test_correct_spelling(self):
        self.search("timothy zahn")

    def test_incorrect_1(self):
        self.search("timithy zahn")

    @known_to_fail
    def test_incorrect_2(self):
        # NOTE: This search turns up no results whatsoever.
        self.search("timithy zhan")


class TestRainaTelgemeier(VariantSearchTest):
    # Test ways of searching for author Raina Telgemeier.
    EVALUATOR = SpecificAuthor("Raina Telgemeier")

    def test_correct_spelling(self):
        self.search('raina telgemeier')

    @known_to_fail
    def test_misspelling_1(self):
        self.search('raina telemger')

    @known_to_fail
    def test_misspelling_2(self):
        self.search('raina telgemerier')

class TestHenningMankell(VariantSearchTest):
    # A few tests of searches for author Henning Mankell
    #
    # Among other things, these tests verify that we can resist the
    # temptation to stem "Henning" to "Hen".

    EVALUATOR = SpecificAuthor("Henning Mankell")

    def test_display_name(self):
        self.search("henning mankell")

    def test_sort_name(self):
        self.search("mankell henning")

    def test_display_name_misspelled(self):
        self.search("henning mankel")

    def test_sort_name_misspelled(self):
        self.search("mankel henning")


class TestMJRose(VariantSearchTest):
    # Test ways of searching for author M. J. Rose.
    # This highlights a lot of problems with the way we handle
    # punctuation and spaces.
    EVALUATOR = Common(author="M. J. Rose")

    # TODO: This is pretty bad given the work we do to normalize
    # author names during indexing. Maybe we need to normalize the
    # data going in to the search.

    def test_with_periods_and_spaces(self):
        # This proves that we do have the books and can find them.
        self.search("m. j. rose")

    @known_to_fail
    def test_with_periods(self):
        # This only gets three books by this author.
        self.search("m.j. rose")

    @known_to_fail
    def test_with_one_period(self):
        # This only three books by this author.
        self.search("m.j rose")

    @known_to_fail
    def test_with_spaces(self):
        # This only gets four books by this author.
        self.search("m j rose")

    @known_to_fail
    def test_with_no_periods_or_spaces(self):
        # The author name is indexed as "m j", and without a space
        # between the "m" and the "j" Elasticsearch won't match the
        # tokens.
        self.search("mj rose")


class TestGenreMatch(SearchTest):
    # A genre search is a search for books in a certain 'section'
    # of the library.

    def test_science_fiction(self):
        # NOTE: "Science Fiction" title matches (some of which are
        # also science fiction) are promoted highly. Genre matches
        # only show up in the front page if they also have "Science
        # Fiction" in the title.
        self.search(
            "science fiction",
            Common(genre="Science Fiction", first_must_match=False)
        )

    @known_to_fail
    def test_sf(self):
        # Shorthand for "Science Fiction"
        # NOTE: This fails because of a book of essays with "SF" in the subtitle
        self.search("sf", Common(genre="Science Fiction"))

    def test_scifi(self):
        # Shorthand for "Science Fiction"
        self.search("sci-fi", Common(genre="Science Fiction"))

    def test_iain_banks_sf(self):
        self.search(
            # Genre and author
            "iain banks science fiction",
            Common(genre="Science Fiction", author="Iain M. Banks")
        )

    @known_to_fail
    def test_christian(self):
        # NOTE: This fails because of a large number of title matches
        # classified under different genres.
        self.search(
            "christian",
            Common(genre=re.compile("(christian|religion)"),
                   first_must_match=False)
        )

    @known_to_fail
    def test_christian_authors(self):
        # NOTE: Again, title matches (e.g. "Authority") dominate
        # the results.
        self.search(
            "christian authors",
            Common(genre=re.compile("(christian|religion)"), first_must_match=False)
        )

    @known_to_fail
    def test_christian_lust(self):
        # It's not clear what this person is looking for, but
        # treating it as a genre search seems appropriate.
        #
        # The first result is religious fiction but most of the
        # others are not.
        self.search(
            "lust christian",
            Common(genre=re.compile("(christian|religion|religious fiction)"))
        )

    @known_to_fail
    def test_christian_fiction(self):
        # NOTE: The "fiction" part is basically ignored in favor of
        # partial title matches.
        self.search(
            "christian fiction",
            [
                Common(fiction=True),
                Common(genre=re.compile(
                    "(christian|religion|religious fiction)")
                )
            ]
        )

    @known_to_fail
    def test_graphic_novel(self):
        # NOTE: This fails for a spurious reason. Many of the results
        # have "Graphic Novel" in the title but are not classified as
        # such.
        self.search(
            "Graphic novel",
            Common(genre="Comics & Graphic Novels")
        )

    def test_horror(self):
        self.search(
            "Best horror story",
            Common(genre=re.compile("horror"))
        )

    @known_to_fail
    def test_scary_stories(self):
        # NOTE: This seems spurious. The first results have "Scary
        # Stories" in the title but are not necessarily classified as
        # horror.
        self.search("scary stories", Common(genre="Horror"))

    @known_to_fail
    def test_percy_jackson_graphic_novel(self):
        # NOTE: This doesn't work very well. The first few results are
        # by Rick Riordan and then works with "Graphic Novel" in the
        # title take over.

        self.search(
            "Percy jackson graphic novel",
            [Common(author="Rick Riordan"),
             AtLeastOne(genre="Comics & Graphic Novels", author="Rick Riordan")
            ]
        )


    def test_gossip_girl_manga(self):
        # A "Gossip Girl" manga series does exist, but it's not in
        # NYPL's collection.  Instead, the results should focus on
        # "Gossip Girl" non-manga (most of which don't have .series
        # set; hence searching by the author's name instead).
        self.search(
            "Gossip girl Manga", [
                Common(
                    author=re.compile("cecily von ziegesar"),
                    first_must_match=False,
                    threshold=0.5
                ),
            ]
        )

    @known_to_fail
    def test_clique(self):
        # NOTE: The target book does show up in the results, but the
        # top results are dominated by books with 'graphic novel' in
        # the title.

        # Genre and title
        self.search(
            "The clique graphic novel",
            Common(genre="Comics & Graphic Novels", title="The Clique")
        )

    @known_to_fail
    def test_spy(self):
        # NOTE: Results are dominated by title matches, which is
        # probably fine, since people don't really think of "Spy" as a
        # genre, and people who do type in "spy" looking for spy books
        # will find them.
        self.search(
            "Spy",
            Common(genre=re.compile("(espionage|history|crime|thriller)"))
        )

    def test_espionage(self):
        self.search(
            "Espionage",
            Common(
                genre=re.compile("(espionage|history|crime|thriller)"),
            )
        )

    def test_food(self):
        self.search(
            "food",
            Common(genre=re.compile("(cook|diet)"))
        )

    def test_mystery(self):
        self.search("mystery", Common(genre="Mystery"))

    def test_agatha_christie_mystery(self):
        # Genre and author -- we should get nothing but mysteries by
        # Agatha Christie.
        self.search(
            "agatha christie mystery",
            [ SpecificGenre(genre="Mystery", author="Agatha Christie"),
              Common(author="Agatha Christie", threshold=1) ]
        )

    def test_british_mystery(self):
        # Genre and keyword
        self.search(
            "British mysteries",
            Common(genre="Mystery", summary=re.compile("british|london"))
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

    @known_to_fail
    def test_deep_poems(self):
        # This appears to be a search for poems which are deep.
        #
        # NOTE: Results are dominated by title matches for "Deep",
        # only a couple results are poetry.
        self.search(
            "deep poems",
            Common(genre="Poetry")
        )


class TestSubjectMatch(SearchTest):
    # Search for a specific subject, more specific than a genre.

    def test_alien_misspelled(self):
        self.search(
            "allien",
            Common(
                subject=re.compile("(alien|extraterrestrial|science fiction)"),
                first_must_match=False
            )
        )

    def test_alien_misspelled_2(self):
        self.search(
            "aluens",
            Common(
                subject=re.compile("(alien|extraterrestrial|science fiction)"),
                first_must_match=False
            )
        )

    @known_to_fail
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

    def test_astrophysics(self):
        # Keyword
        self.search(
            "Astrophysics",
            Common(
                genre="Science",
                subject=re.compile("(astrophysics|astronomy|physics|space|science)")
            )
        )

    def test_anxiety(self):
        self.search(
            "anxiety",
            Common(
                genre=re.compile("(psychology|self-help)"),
                first_must_match=False
            )
        )

    def test_beauty_hacks(self):
        # NOTE: fails on both versions.  The user was obviously looking for a specific
        # type of book; ideally, the search results would return at least one relevant
        # one.  Instead, all of the top results are either books about computer hacking
        # or romance novels.
        self.search("beauty hacks",
        AtLeastOne(subject=re.compile("(self-help|style|grooming|personal)")))

    def test_character_classification(self):
        # Although we check a book's description, it's very difficult
        # to find a good query that singles this out.

        # However, by searching for a hyperspecific subject matter
        # classification, we can find a series of books that only has
        # one word of overlap with the subject matter classification.
        self.search(
            "Gastner, Sheriff (Fictitious character)",
            Common(series="Bill Gastner Mystery")
        )

    def test_college_essay(self):
        self.search(
            "College essay",
            Common(
                genre=re.compile("study aids"),
                subject=re.compile("college")
            )
        )

    def test_da_vinci(self):
        # Someone who searches for "da vinci" is almost certainly
        # looking entirely for books _about_ Da Vinci.
        self.search(
            "Da Vinci",
            Common(genre=re.compile("(biography|art)"), first_must_match=False)
        )

    @known_to_fail
    def test_da_vinci_missing_space(self):
        # NOTE: Books in the "Davina Graham" series are taking up most
        # of the top search results.
        self.search(
            "Davinci",
            Common(
                genre=re.compile("(biography|art)"),
                first_must_match=False,
                threshold=0.3
            )
        )

    @known_to_fail
    def test_dirtbike(self):
        # NOTE: This gets no results at all. Searching "dirt bike"
        # (two words) renders more relevant results, but still not
        # enough for the test to pass.
        self.search(
            "dirtbike",
            Common(
                subject=re.compile("(bik|bicycle|sports|nature|travel)")
            )
        )

    def test_greek_romance(self):
        # NOTE: this fails.  All of the top results are romance novels with
        # "Greek" in the summary.  Not necessarily problematic...but the
        # user is probably more likely to be looking for something like "Essays
        # on the Greek Romances."  If you search "Greek romances" rather than
        # "Greek romance," then "Essays on the Greek Romances" becomes the first
        # result on ES1, but is still nowhere to be found on ES6.
        #
        # I think this person might be searching for romance novels... -LR
        self.search(
            "Greek romance",
            AtLeastOne(title=re.compile("greek romance"))
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

    def test_information_technology(self):
        # The first result is a title match.
        self.search(
            "information technology",
            Common(
                subject=re.compile("(information technology|computer)"),
                first_must_match=False
            )
        )

    def test_louis_xiii(self):
        # There aren't very many books in the collection about Louis
        # XIII, but he is the basis for the king in "The Three
        # Musketeers", so that's not a bad answer.
        self.search(
            "Louis xiii",
            Common(title="The Three Musketeers", threshold=0.1)
        )

    def test_managerial_skills(self):
        # NOTE: This works on ES6.  On ES1, the first few results are good, but then
        # it devolves into books from a fantasy series called "The Menagerie."
        self.search(
            "managerial skills",
            Common(
                subject=re.compile("(business|management)")
            )
        )

    def test_manga(self):
        # This has the same problem as the 'anime' test above --
        # we have tons of manga but it's not labeled as "manga".
        self.search(
            "manga",
            [
                Common(title=re.compile("manga")),
                Common(subject=re.compile("(manga|art|comic)")),
            ]
        )

    def test_meditation(self):
        self.search(
            "Meditation",
            Common(
                genre=re.compile("(self-help|mind|spirit)")
            )
        )

    def test_music_theory(self):
        # Keywords
        self.search(
            "music theory", Common(
                genre="Music",
                subject=re.compile("(music theory|musical theory)")
            )
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

    @known_to_fail
    def test_native_american_misspelled(self):
        # NOTE: this passed back in the ES1, althoguh the results
        # weren't quite as good as they would be if the search term
        # were spelled correctly. Now it's dominated by title matches
        # for "native".

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


    def test_ninjas(self):
        self.search("ninjas", Common(title=re.compile("ninja")))

    def test_ninjas_misspelled(self):
        # NOTE: The first result is "Ningyo", which does look a
        # lot like "Ningas"...
        self.search(
            "ningas", Common(title=re.compile("ninja"), first_must_match=False)
        )

    def test_pattern_making(self):
        self.search(
            "Pattern making",
            AtLeastOne(subject=re.compile("crafts"))
        )

    def test_plant_based(self):
        self.search(
            "Plant based",
            Common(
                subject=re.compile("(cooking|food|nutrition|health)")
            )
        )

    def test_prank(self):
        self.search("prank", Common(title=re.compile("prank")))

    def test_prank_plural(self):
        self.search("pranks", Common(title=re.compile("prank")))

    def test_presentations(self):
        self.search(
            "presentations",
            Common(
                subject=re.compile("(language arts|business presentations|business|management)")
            )
        )

    def test_python_programming(self):
        # This is tricky because 'python' means a lot of different
        # things.
        self.search(
            "python programming",
            Common(subject="Python (computer program language)")
        )

    def test_sewing(self):
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

    def test_supervising(self):
        # Keyword
        self.search(
            "supervising", Common(genre="Business", first_must_match=False)
        )

    def test_tennis(self):
        self.search(
            "tennis",
            [
                Common(subject=re.compile("tennis")),
                Common(genre=re.compile("(Sports|Games)", re.I)),
            ]
        )

    @known_to_fail
    def test_texas_fair(self):
        # There exist a few books about the Texas state fair, but none of them
        # are in the collection, so the best outcome is that the results will
        # include a lot of books about Texas.
        #
        # TODO: "books about" really skews the results here -- lots of
        # title matches.
        self.search(
            "books about texas like the fair",
            Common(title=re.compile("texas"))
        )

    def test_witches(self):
        self.search(
            "witches",
            Common(subject=re.compile('witch'))
        )


class TestTravel(VariantSearchTest):
    # Searches for places that are likely to be searches for travel guides
    # (rather than history books, names of novels, etc).

    EVALUATOR = Common(
        subject=re.compile("(travel|guide|fodors)"), first_must_match=False
    )

    @known_to_fail
    def test_california(self):
        # NOTE: This fails due to a large number of title matches.
        self.search("California")

    def test_new_england(self):
        self.search("new england")

    def test_toronto(self):
        self.search("toronto")


class TestSeriesMatch(SearchTest):

    @known_to_fail
    def test_dinosaur_cove(self):
        # NYPL's collection doesn't have any books in this series .
        self.search(
            "dinosaur cove",
            Common(series="Dinosaur Cove")
        )

    def test_poldi(self):
        # NYPL's collection only has one book from this series.
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

    def test_game_of_thrones(self):
        # People often search for the name of the TV show, but
        # the series name is different.
        self.search(
            "game of thrones",
            Common(series="a song of ice and fire")
        )

    def test_harry_potter(self):
        # This puts foreign-language titles above English titles, but
        # that's fine because our search document doesn't include a
        # language filter.
        #
        # The very first result is an exact title match -- a guide to
        # the film series.
        self.search(
            "Harry potter",
            Common(series="Harry Potter", threshold=0.9, first_must_match=False)
        )

    @known_to_fail
    def test_maisie_dobbs(self):
        # Misspelled proper noun
        #
        # This gets a couple title matches but nothing else
        # that's relevant.
        self.search(
            "maise dobbs",
            Common(series="Maisie Dobbs", threshold=0.5)
        )

    def test_gossip_girl(self):
        # As with the search for 'gossip girl manga', we need to
        # check this via an author search because most of the
        # Gossip Girl books don't actually set .series.
        self.search(
            "Gossip girl",
            Common(
                author=re.compile("cecily von ziegesar"),
            ),
        )

    @known_to_fail
    def test_gossip_girl_misspelled(self):
        # This does very poorly because of a large number
        # of title matches for "Gossip" -- 'hirl' gets ignored.
        self.search(
            "Gossip hirl",
            Common(
                author=re.compile("cecily von ziegesar"),
            ),
        )

    def test_magic(self):
        # This book isn't in the collection, but the results include other books from
        # the same series.
        self.search(
            "Frogs and french kisses",
            AtLeastOne(series="Magic in Manhattan")
        )

    def test_goosebumps(self):
        self.search("goosebumps", Common(series="Goosebumps", threshold=0.9))

    def test_goosebump_singular(self):
        # R. L. Stine has a number of Goosebumps spin-off series, and
        # the typo means we won't get the boost for a keyword
        # match. So the best way to check this is by author.
        self.search(
            "goosebump", 
            Common(author="R. L. Stine")
        )

    @known_to_fail
    def test_goosebumps_misspelled(self):
        # NOTE: This gets no results at all.
        self.search("gosbums", Common(series="Goosebumps"))

    def test_severance(self):
        # We only have one of these titles.
        #
        # Searching for 'severance' alone is going to get title
        # matches, which is as it should be.
        self.search(
            "severance trilogy",
            AtLeastOne(series="The Severance Trilogy")
        )

    def test_severance_misspelled(self):
        # Slightly misspelled
        self.search(
            "severence trilogy",
            AtLeastOne(series="The Severance Trilogy")
        )

    def test_hunger_games(self):
        self.search("the hunger games", Common(series="The Hunger Games"))

    @known_to_fail
    def test_hunger_games_misspelled(self):
        # NOTE: This is really bad, dominated by title matches for "Game".
        self.search("The hinger games", Common(series="The Hunger Games"))

    @known_to_fail
    def test_mockingjay(self):
        # NOTE: This isn't too bad -- the top results are other books
        # from the series, and the book we're looking for is on the
        # first page. But the title match really should be
        # higher.
        # Series and title
        self.search(
            "The hunger games mockingjay",
            [FirstMatch(title="Mockingjay"), Common(series="The Hunger Games")]
        )

    def test_i_funny(self):
        # Although we get good results, we need to use an author match
        # to verify them. Many of the results don't have .series set
        # and match due to a partial title match.
        self.search(
            "i funny",
            SpecificAuthor("Chris Grabenstein"),
        )

    @known_to_fail
    def test_foundation(self):
        # Series and full author
        #
        # The results have a number of Foundation titles, both
        # by Asimov and others, but they also have "Isaac Asimov's X"
        # title matches, and "Foundation" is not the first result.
        # It would have been better to just search for "foundation".
        self.search(
            "Isaac asimov foundation",
            Common(series="Foundation")
        )

    def test_dark_tower(self):
        # Again, many of these books don't have .series set.
        #
        # There exist two completely unrelated books called "The Dark
        # Tower"--it's fine for one of those to be the first result.
        self.search(
            "The dark tower", [
                Common(author="Stephen King", first_must_match=False),
                AtLeastOne(series="The Dark Tower")
            ]
        )

    def test_science_comics(self):
        # We don't have a .series match for "science comics" but
        # we do have one title match, which shows up first.

        # TODO: Since this is two different genre names we should
        # test the hypothesis that the requestor wants the intersection
        # of two genres.
        self.search(
            "Science comics",
            [FirstMatch(title=re.compile("^science comics")),
            ]
        )

    @known_to_fail
    def test_who_is(self):
        # These children's biographies don't have .series set but
        # are clearly part of a series.
        #
        # Because those books don't have .series set, the matches are
        # done solely through title, so unrelated books like "Who Is
        # Rich?" also show up.
        #
        # NOTE: These used to work but now results are dominated by
        # title matches for books like "Who?"
        self.search("who is", Common(title=re.compile('^who is ')))

    @known_to_fail
    def test_who_was(self):
        # From the same series of biographies as test_who_is().
        # NOTE: Same failure reason as that test.
        self.search("who was", Common(title=re.compile('^who was ')))

    def test_wimpy_kid_misspelled(self):
        # Series name contains the wrong stopword ('the' vs 'a')
        self.search(
            "dairy of the wimpy kid",
            Common(series="Diary of a Wimpy Kid")
        )


class TestSeriesTitleMatch(SearchTest):
    """Test a search that tries to match a specific book in a series."""

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

    def test_harry_potter_specific_title(self):
        # The first result is the requested title. TODO: other results
        # should be from the same series, but this doesn't happen much
        # compared to other, similar tests.
        self.search(
            "chamber of secrets", [
                FirstMatch(title="Harry Potter and the Chamber of Secrets"),
                Common(series="Harry Potter", threshold=0.3)
            ]
        )

    def test_wimpy_kid_specific_title(self):
        # The first result is the requested title. Other results
        # are from the same series.
        self.search(
            "dairy of the wimpy kid dog days",
            [
                FirstMatch(title="Dog Days", author="Jeff Kinney"),
                Common(series="Diary of a Wimpy Kid"),
            ]
        )

    def test_foundation_specific_title_by_number(self):
        # NOTE: this works on ES1 but not on ES6!
        # Series, full author, and book number
        self.search(
            "Isaac Asimov foundation book 1",
            FirstMatch(series="Foundation", title="Prelude to Foundation")
        )

    def test_survivors_specific_title(self):
        self.search(
            "survivors book 1",
            [
                Common(series="Survivors"),
                FirstMatch(title="The Empty City"),
            ]
        )


# Classes that test many different kinds of searches for a particular
# series.
#
class TestISurvived(VariantSearchTest):
    # Test different ways of spelling "I Survived"
    # .series is not set for these books so we check the title.
    EVALUATOR = Common(title=re.compile('^i survived '))

    def test_correct_spelling(self):
        self.search("i survived")

    def test_incorrect_1(self):
        self.search("i survied")

    def test_incorrect_2(self):
        self.search("i survive")

    def test_incorrect_3(self):
        self.search("i survided")


class TestDorkDiaries(VariantSearchTest):
    # Test different ways of spelling "Dork Diaries"
    EVALUATOR = Common(series="Dork Diaries")

    def test_correct_spelling(self):
        self.search('dork diaries')

    def test_misspelling_and_number(self):
        self.search("dork diarys #11")

    def test_misspelling_with_punctuation(self):
        self.search('doke diaries.')

    def test_singular(self):
        self.search("dork diary")

    def test_misspelling_1(self):
        self.search('dork diarys')

    def test_misspelling_2(self):
        self.search('doke dirares')

    def test_misspelling_3(self):
        self.search('doke dares')

    def test_misspelling_4(self):
        self.search('doke dires')

    def test_misspelling_5(self):
        self.search('dork diareis')


class TestMyLittlePony(VariantSearchTest):
    # Test different ways of spelling "My Little Pony"

    # .series is not set for these books so we check the title.
    EVALUATOR = Common(title=re.compile("my little pony"))

    def test_correct_spelling(self):
        self.search("my little pony")

    def test_misspelling_1(self):
        self.search("my little pon")

    def test_misspelling_2(self):
        self.search("my little ponie")


class TestLanguageRestriction(SearchTest):
    # Verify that adding the name of a language restricts the results
    # to books in that language.

    def test_language(self):
        self.search("espanol", Common(language="spa"))

    def test_author_with_language(self):
        # NOTE: this doesn't work on either version of ES; the first
        # Spanish result is #3
        self.search(
            "Pablo escobar spanish",
            FirstMatch(author="Pablo Escobar", language="spa")
        )

    def test_gatos(self):
        # Searching for a Spanish word should mostly bring up books in Spanish
        self.search(
            "gatos",
            Common(language="spa", threshold=0.9)
        )


class TestCharacterMatch(SearchTest):
    # These searches are best understood as an attempt to find books
    # featuring certain fictional characters.
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

    def test_batman(self):
        # NOTE: doesn't work.  (The results for "batman" as one word are as
        # expected, though.)

        # Patron is searching for 'batman' but treats it as two words.
        self.search(
            "bat man book",
            Common(title=re.compile("batman"))
        )

    def test_christian_grey(self):
        # This search uses a character name to stand in for a series.
        #
        # NOTE: this fails. The first two books are Christian-genre
        # books with "grey" in the title, which is a reasonable search
        # result but is not what the patron wanted.
        self.search(
            "christian grey",
            FirstMatch(author="E. L. James")
        )

    def test_spiderman(self):
        # Patron is searching for 'spider-man' but treats it as one word.
        for q in ("spiderman", "spidermanbook"):
            self.search(
                q, Common(title=re.compile("spider-man"))
            )

    def test_teen_titans(self):
        self.search(
            "teen titans",
            Common(title=re.compile("^teen titans")), limit=5
        )

    def test_teen_titans_girls(self):
        # We don't gender books so we can't deliver results tailored
        # to 'teen titans girls', but we should at least give
        # _similar_ results to 'teen titans' and not go off
        # on tangents because of the 'girls' part.
        self.search(
            "teen titans girls",
            Common(title=re.compile("^teen titans")), limit=5
        )

    def test_thrawn(self):
        # "Thrawn" is a specific title but the patron may be referring
        # to a series of books featuring this character (though it's
        # not the official name of the series), so we check beyond the
        # first result.
        self.search(
            "thrawn",
            [
                FirstMatch(title="Thrawn"),
                Common(author="Timothy Zahn", series=re.compile("star wars")),
            ]
        )


class TestAgeRangeRestriction(SearchTest):
    # Verify that adding an age range restricts the results returned
    # to contain exclusively children's books.

    def all_children(self, q):
        # Verify that this search finds nothing but books for children.
        self.search(q, Common(audience='Children', threshold=1))

    def mostly_adult(self, q):
        # Verify that this search finds mostly books for grown-ups.
        self.search(q, Common(audience='Adult', first_must_match=False))

    def test_black(self):
        self.all_children("black age 3-5")
        self.mostly_adult("black")

    def test_island(self):
        self.all_children("island age 3-5")
        self.mostly_adult("island")

    def test_panda(self):
        self.all_children("panda age 3-5")
        # We don't call mostly_adult() because 'panda' on its own
        # finds mostly children's books.

    def test_chapter_books(self):
        # Chapter books are a book format aimed at a specific
        # age range.
        self.search(
            "chapter books", Common(target_age=(6, 10))
        )

    @known_to_fail
    def test_chapter_books_misspelled_1(self):
        # This doesn't work: we get 'chapter' title matches.
        #
        # We know this won't work because we don't do fuzzy matching
        # on things that would become filter terms.
        self.search(
            "chapter bookd", Common(target_age=(6, 10))
        )

    @known_to_fail
    def test_chapter_books_misspelled_2(self):
        # This doesn't work: we get 'book' title matches
        self.search(
            "chaptr books", Common(target_age=(6, 10))
        )

    @known_to_fail
    def test_grade_and_subject(self):
        # NOTE: this doesn't work because we don't parse grade numbers
        # when they're spelled out, only when they're provided as
        # numbers.
        self.search(
            "Seventh grade science",
            [
                Common(target_age=(12, 13)),
                Common(genre="Science")
            ]
        )


_db = production_session()
library = None

index = ExternalSearchIndex(_db)
SearchTest.searcher = Searcher(library, index)
