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
            SearchTest.expected_failures.append(f)
            logging.info("Expected this test to fail, and it did: %r" % e)
            return
        SearchTest.unexpected_successes.append(f)
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
            logging.info(
                "Need %d%% matches, got %d%%" % (
                    threshold*100, actual*100
                )
            )
            for hit in hits:
                logging.info(repr(hit))
        assert actual >= threshold

    def _match_scalar(self, value, expect, inclusive=False, case_sensitive=False):
        if hasattr(expect, 'search'):
            if expect and value is not None:
                success = expect.search(value)
            else:
                success = False
            expect_str = expect.pattern
        else:
            if value and not case_sensitive:
                value = value.lower()
            if inclusive:
                success = (expect in value)
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
            fields = None
            if field == 'subject':
                success, value, expect_str = self._match_subject(expect, result)
            elif field == 'genre':
                success, value, expect_str = self._match_genre(expect, result)
            elif field == 'target_age':
                success, value, expect_str = self._match_target_age(expect, result)
            elif field == 'author':
                success, value, expect_str = self._match_author(expect, result)
            elif field == 'title_or_subtitle':
                fields = ['title', 'subtitle']
            else:
                fields = [field]
            if fields:
                for field in fields:
                    value = self._field(field, result)
                    success, expect_str = self._match_scalar(value, expect)
                    if success:
                        break
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
                        logging.info(
                            "First result matched and shouldn't have. %s == %s", expected, actual
                        )
                    assert actual != expected
                else:
                    if actual != expected:
                        logging.info(
                            "First result did not match. %s != %s" % (expected, actual)
                        )
                    eq_(actual, expected)

    def evaluate_hits(self, hits):
        successes, failures = self.multi_evaluate(hits)
        if self.negate:
            failures, successes = successes, failures
        if self.threshold is not None:
            self.assert_ratio(
                [x[1:] for x in successes],
                [x[1:] for x in successes+failures],
                self.threshold
            )
        if self.minimum is not None:
            overall_success = len(successes) >= self.minimum
            if not overall_success:
                logging.info(
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
                    logging.info(template % tuple(vars))
            assert overall_success


class Uncommon(Common):
    """The given match must seldom or never happen."""
    def __init__(self, threshold=1, **kwargs):
        kwargs['negate'] = True
        super(Uncommon, self).__init__(threshold=threshold, **kwargs)


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
        subtitle = self._field('subtitle', first)
        if self.accept_title and (self.accept_title in title or self.accept_title in subtitle):
            return True

        # We have failed.
        if hasattr(expect, 'match'):
            expect = expect.pattern
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


class SpecificSeries(Common):
    """Verify that results come from a certain series of books."""

    def evaluate(self, results):
        successes = []
        diagnostics = []
        for result in results:
            success, should_have_matched = self.evaluate_one(result)
            if success:
                successes.append(result)
            diagnostics.append(should_have_matched)
        self.assert_ratio(successes, diagnostics, self.threshold)

    def evaluate_one(self, result):
        expect_author = self.kwargs.get('author')
        expect_series = self.kwargs.get('series')

        # Ideally a series match happens in the .series, but sometimes
        # it happens in the .title.

        actual_series = result.series or ""
        series_match, details = self._match_scalar(
            actual_series, expect_series, inclusive=True
        )
        actual_title = result.title
        title_match, details = self._match_scalar(
            actual_title, expect_series, inclusive=True
        )

        # Either way, if an author is specified, it means a book with
        # a matching title by a different author is not part of the
        # series.
        if expect_author:
            author_match, match, details = self._match_author(
                expect_author, result
            )
        else:
            author_match = True
        actual = (actual_series, actual_title, result.author,
                  result.sort_author, series_match, title_match, author_match)
        return (series_match or title_match) and author_match, actual


class SearchTest(object):
    """A test suite that runs searches and compares the actual results
    to some expected state.
    """

    expected_failures = []
    unexpected_successes = []

    def search(self, query, evaluators=None, limit=10):
        query = query.lower()
        logging.info("Query: %r", query)
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
        # enough to English words that it might pick up a few results
        # on a fuzzy match.
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

class TestPosessives(SearchTest):
    """Test searches for book titles that contain posessives."""

    def test_washington_partial(self):
        self.search(
            "washington war",
            AtLeastOne(title="George Washington's War")
        )

    def test_washington_full_no_apostrophe(self):
        self.search(
            "george washingtons war",
            FirstMatch(title="George Washington's War")
        )

    @known_to_fail
    def test_washington_partial_apostrophe(self):
        # The apostrophe is stripped and the 's' is stemmed. This is
        # normally a good thing, but here the query is parsed as
        # "washington war", and the first result is "The Washington
        # War". Parsing this as "washington ' s war" would give better
        # results here.
        #
        # Since most people don't type the apostrophe, the tradeoff is
        # worth it.
        self.search(
            "washington's war",
            FirstMatch(title="George Washington's War")
        )

    def test_washington_full_apostrophe(self):
        self.search(
            "george washington's war",
            FirstMatch(title="George Washington's War")
        )

    def test_bankers(self):
        self.search(
            "bankers wife",
            FirstMatch(title="The Banker's Wife")
        )

    def test_brother(self):
        # The entire posessive is omitted.
        self.search(
            "my brother shadow",
            FirstMatch(title="My Brother's Shadow"),
        )

    def test_police_women_apostrophe(self):
        self.search(
            "policewomen's bureau",
            FirstMatch(title="The Policewomen's Bureau"),
        )

    def test_police_women_no_apostrophe(self):
        self.search(
            "policewomens bureau",
            FirstMatch(title="The Policewomen's Bureau"),
        )

    def test_police_women_no_posessive(self):
        self.search(
            "policewomen bureau",
            FirstMatch(title="The Policewomen's Bureau"),
        )

    @known_to_fail
    def test_police_women_extra_space(self):
        # The extra space means this parses to 'police' and 'women',
        # two very common words, and not the relatively uncommon
        # 'policewomen'.
        self.search(
            "police womens bureau",
            FirstMatch(title="The Policewomen's Bureau"),
        )

class TestSynonyms(SearchTest):
    # Test synonyms that could be (but currently aren't) defined in
    # the search index.

    @known_to_fail
    def test_and_is_ampersand(self):
        # There are books called "Black & White" and books called
        # "Black And White". When '&' and 'and' are synonyms, all
        # these books should get the same score.
        self.search(
            "black and white",
            AtLeastOne(title="Black & White"),
        )

    @known_to_fail
    def test_ampersand_is_and(self):
        # The result we're looking for is second, behind "The
        # Cheesemaker's Apprentice".
        self.search(
            "master and apprentice",
            FirstMatch(title="Master & Apprentice (Star Wars)"),
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
             Common(title=re.compile("(save|cat)"), threshold=0.6)]
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

    def test_divorce(self):
        # Despite the 'children', we are not looking for children's
        # books. We're looking for books for grown-ups about divorce.
        self.search(
            "The truth about children and divorce", [
                Common(audience="adult"),
                AtLeastOne(subject=re.compile("divorce")),
            ]
        )

    def test_patterns_of_fashion(self):
        # This specific title isn't in the collection, but the results
        # should still be reasonably relevant.
        self.search(
            "Patterns of fashion", [
                Common(subject=re.compile("crafts"), first_must_match=False),
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

    @known_to_fail
    def test_unowned_misspelled_partial_title_cosmetics(self):
        # NOTE: The patron was presumably looking for "Don't Go to the
        # Cosmetics Counter Without Me," which isn't in the
        # collection.  Ideally, one of the results should have
        # something to do with cosmetics; instead, they're about
        # comets.
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
            Common(title_or_subtitle=re.compile("life|spy"),
                   threshold=0.5)
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

    def test_marriage_lie(self):
        self.search(
            "Marriage liez",
            FirstMatch(title="The Marriage Lie")
        )

    def test_invisible_emmie(self):
        # One word in the title is slightly misspelled.
        self.search(
            "Ivisible emmie", FirstMatch(title="Invisible Emmie")
        )

    def test_karamazov(self):
        # Extremely uncommon proper noun, slightly misspelled
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

    def test_kingdom_of_the_blind(self):
        # The first word, which is a fairly common word, is slightly misspelled.
        self.search(
            "Kngdom of the blind",
            FirstMatch(title="Kingdom of the Blind")
        )

    def test_seven_husbands(self):
        # Two words--1) a common word which is spelled as a different word
        # ("if" instead of "of"), and 2) a proper noun--are misspelled.
        self.search(
            "The seven husbands if evyln hugo",
            FirstMatch(title="The Seven Husbands of Evelyn Hugo")
        )

    def test_nightingale(self):
        # Unusual word misspelled.
        #
        # This might fail because a book by Florence Nightingale is
        # seen as a better match.
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

    def test_british_spelling_color_of_our_sky(self):
        # Note to pedants: the title of the book as published is
        # "The Color of Our Sky".

        self.search(
            "The colour of our sky",
            FirstMatch(title="The Color of Our Sky")
        )


class TestPartialTitleSearch(SearchTest):
    # Test title searches where only part of the title is provided.

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
        # Search query cuts off midway through the second word of the
        # subtitle.  NOTE: The book we're looking for is on the first
        # page, beneath other titles called "Open Wide!" and "Wide
        # Open", which ought to be worse matches because there's no
        # subtitle match.
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
            FirstMatch(title=re.compile("Theresa", re.I))
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

    def test_partial_title_match_with_genre_name_education(self):
        self.search(
            "education henry adams",
            FirstMatch(title="The Education of Henry Adams"),
        )

    def test_title_match_with_genre_name_law(self):
        self.search(
            "law of the mountain man",
            FirstMatch(title="Law of the Mountain Man")
        )

    @known_to_fail
    def test_law_of_the_mountain_man_with_author(self):
        # "Law of the Mountain Man" is the second result, but it
        # really should be first.
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
        # This works because of the stopword index.
        #
        # Otherwise "Dance of the Dragons" looks like an equally good
        # result.
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
        # It's an unusual situation so I think this is all right.
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
        # but it can't beat out title matches like "CentOS Bible"
        self.search(
            "centos caen",
            FirstMatch(title="fedora linux toolbox")
        )

    def test_fallen_baldacci(self):
        self.search(
            "fallen baldacci",
            FirstMatch(author="David Baldacci", title="The Fallen")
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
        # NOTE: Work shows up very high on first page but ought to be
        # first.  It's behind other books called "Every
        # Breath"
        self.search(
            "Every breath by nicholis sparks",
            FirstMatch(title="Every Breath", author="Nicholas Sparks")
        )

    def test_grisham(self):
        # Full title, author name misspelled
        self.search(
            "The reckoning john grisham",
            FirstMatch(title="The Reckoning", author="John Grisham")
        )

    def test_singh(self):
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

    def test_misspelled_no_apostrophe(self):
        self.search("charlettes web")

    def test_no_apostrophe_with_author(self):
        self.search("charlottes web eb white")

    def test_no_apostrophe_with_author_space(self):
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
        # The search results are books about characters named Diedre.
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
        self.search(
            "edith warton", Common(author="Edith Wharton")
        )

    def test_danielle_steel(self):
        # The author's last name is slightly misspelled in the search query.
        self.search(
            "danielle steele",
            SpecificAuthor("Danielle Steel", threshold=1)
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
        # audiobooks. Searching for their name may bring up people
        # with similar names and authorship roles, but they'll show up
        # pretty frequently.
        self.search(
            "will patton",
            Common(author="Will Patton")
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
        #
        # NOTE: These results are always very good, but sometimes the
        # first result is a title match with stopword removed:
        # "Escalier B, Paris 12".
        self.search(
            "b a paris", SpecificAuthor("B. A. Paris")
        )

    def test_griffiths(self):
        # The search query gives the author's sort name.
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
            SpecificSeries(
                series="My Weird School", author="Dan Gutman"
            )
        )

    def test_steve_berry(self):
        # This search looks like nothing special but it has been
        # difficult in the past, possibly because "berry" is an
        # English word.
        self.search("steve berry", Common(author="Steve Berry"))

    @known_to_fail
    def test_thomas_python(self):
        # All the terms are correctly spelled words, but the patron
        # clearly means something else.
        self.search(
            "thomas python",
            Common(author="Thomas Pynchon")
        )

    def test_betty_neels_audiobooks(self):
        # Even though there are no audiobooks, all of the search
        # results should still be books by this author.
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

    def test_incorrect_2(self):
        self.search("timithy zhan")


class TestRainaTelgemeier(VariantSearchTest):
    # Test ways of searching for author Raina Telgemeier.
    EVALUATOR = SpecificAuthor("Raina Telgemeier")

    def test_correct_spelling(self):
        self.search('raina telgemeier')

    def test_minor_misspelling(self):
        self.search('raina telegmeier')

    @known_to_fail
    def test_misspelling_1(self):
        self.search('raina telemger')

    def test_misspelling_2(self):
        self.search('raina telgemerier')

class TestHenningMankell(VariantSearchTest):
    # A few tests of searches for author Henning Mankell
    #
    # Among other things, these tests verify that we can resist the
    # temptation to stem "Henning" to "Hen".
    #
    # In NYPL's collection, the top result for a misspelled version of
    # this author's name is a book by a different author, with the
    # subtitle "A gripping thriller for fans of Jo Nesbo and Henning
    # Mankell". That's not perfect, but it's acceptable.

    EVALUATOR = SpecificAuthor("Henning Mankell", accept_title="Henning Mankell")

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

    def test_with_spaces(self):
        # This is how the author's name is indexed internally.
        self.search("m j rose")

    @known_to_fail
    def test_with_periods(self):
        # This only gets three books by this author.
        # Maybe 'm.j.' is parsed as a single token or something.
        self.search("m.j. rose")

    @known_to_fail
    def test_with_one_period(self):
        # This only gets three books by this author.
        self.search("m.j rose")

    @known_to_fail
    def test_with_no_periods_or_spaces(self):
        # The author name is indexed as "m j", and without a space
        # between the "m" and the "j" Elasticsearch won't match the
        # tokens.
        self.search("mj rose")


class TestPublisherMatch(SearchTest):
    # Test the ability to find books by a specific publisher or
    # imprint.

    def test_harlequin_romance(self):
        self.search(
            "harlequin romance", Common(publisher="harlequin", genre="Romance")
        )

    def test_harlequin_historical(self):
        self.search(
            "harlequin historical",
            # We may get some "harlequin historical classic", which is fine.
            Common(imprint=re.compile("harlequin historical"), genre="Romance")
        )

    def test_princeton_review(self):
        self.search(
            "princeton review",
            Common(imprint="princeton review")
        )

    @known_to_fail
    def test_wizards(self):
        self.search(
            "wizards coast", Common(publisher="wizards of the coast")
        )

    # We don't want to boost publisher/imprint matches _too_ highly
    # because publishers and imprints are often single words that
    # would be better matched against other fields.

    def test_penguin(self):
        # Searching for a word like 'penguin' should prioritize partial
        # matches in other fields over exact imprint matches.
        self.search(
            "penguin",
            [Common(title=re.compile("penguin", re.I)),
             Uncommon(imprint="Penguin")]
        )

    def test_vintage(self):
        self.search(
            "vintage",
            [Common(title=re.compile("vintage", re.I)),
             Uncommon(imprint="Vintage", threshold=0.5)]
        )

    def test_plympton(self):
        # This should prioritize books by George Plimpton (even though
        # it's not an exact string match) over books from the Plympton
        # publisher.
        self.search(
            "plympton",
            [Common(author=re.compile("plimpton", re.I)),
             Uncommon(publisher="Plympton")]
        )

    @known_to_fail
    def test_scholastic(self):
        # This gets under 50% matches -- there are test prep books and
        # the like in the mix.
        #
        # TODO: It would be nice to boost this publisher more, but
        # it's tough to know that "scholastic" is probably a publisher
        # search, where "penguin" is probably a topic search and
        # "plympton" is probably a misspelled author search.
        self.search(
            "scholastic", Common(publisher="scholastic inc.")
        )


class TestGenreMatch(SearchTest):
    # A genre search is a search for books in a certain 'section'
    # of the library.

    any_sf = re.compile("(Science Fiction|SF)", re.I)

    def test_science_fiction(self):
        # NOTE: "Science Fiction" title matches (some of which are
        # also science fiction) are promoted highly. Genre matches
        # only show up in the front page if they also have "Science
        # Fiction" in the title.
        self.search(
            "science fiction", Common(genre=self.any_sf, first_must_match=False)
        )

    def test_sf(self):
        # Shorthand for "Science Fiction"
        # NOTE: The first result is a book of essays with "SF" in the subtitle
        # -- a reasonable match.
        self.search("sf", Common(genre=self.any_sf, first_must_match=False))

    def test_scifi(self):
        # Shorthand for "Science Fiction"
        self.search("sci-fi", Common(genre=self.any_sf))

    def test_iain_banks_sf(self):
        self.search(
            # Genre and author
            "iain banks science fiction",
            Common(genre=self.any_sf, author="Iain M. Banks")
        )

    @known_to_fail
    def test_christian(self):
        # NOTE: This fails because of a large number of title matches
        # classified under other genres.
        self.search(
            "christian",
            Common(genre=re.compile("(christian|religion)"),
                   first_must_match=False)
        )

    def test_christian_authors(self):
        self.search(
            "christian authors",
            Common(genre=re.compile("(christian|religion)"))
        )

    @known_to_fail
    def test_christian_lust(self):
        # It's not clear what this person is looking for, but
        # treating it as a genre search seems appropriate.
        #
        # The first couple results are excellent, so this isn't
        # so bad.
        self.search(
            "lust christian",
            Common(genre=re.compile("(christian|religion|religious fiction)"))
        )

    @known_to_fail
    def test_christian_fiction(self):
        # NOTE: This fails for a spurious reason. These results are
        # pretty good, but they're not obvious genre matches.
        self.search(
            "christian fiction",
            [
                Common(fiction="fiction"),
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
        # Stories" in the title, so they should do fine, but are not
        # necessarily classified as horror.
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
        # the "Gossip Girl" series.
        self.search(
            "Gossip girl Manga", [
                SpecificSeries(
                    series="Gossip Girl",
                    author=re.compile("cecily von ziegesar"),
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

    def test_spy(self):
        # Results are dominated by title matches, which is probably
        # fine, since people don't really think of "Spy" as a genre,
        # and people who do type in "spy" looking for spy books will
        # find them.
        self.search(
            "Spy",
            Common(title=re.compile("(spy|spies)", re.I))
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
            Common(genre="Mystery", summary=re.compile("british|london|england|scotland"))
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

    def test_deep_poems(self):
        # This appears to be a search for poems which are deep.
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
        # 'anime' and 'manga' are not subject classifications we get
        # from our existing vendors. We have a lot of these books but
        # they're not classified under those terms.
        #
        # So we get a few title matches for "Anime" and then go into
        # books about animals.
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
            SpecificSeries(series="Bill Gastner Mystery")
        )

    def test_college_essay(self):
        self.search(
            "College essay",
            Common(
                genre=re.compile("study aids"),
                subject=re.compile("college")
            )
        )

    @known_to_fail
    def test_da_vinci(self):
        # Someone who searches for "da vinci" is almost certainly
        # looking entirely for books _about_ Da Vinci.
        #
        # TODO: The first few results are good but then we go into
        # "Da Vinci Code" territory. Maybe that's fine, though.
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
        # This person might be searching for romance novels or for
        # something like "Essays on the Greek Romances."
        self.search(
            "Greek romance",
            [Common(genre="Romance", first_must_match=False),
             AtLeastOne(title=re.compile("greek romance"))]
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
            AtLeastOne(title="The Three Musketeers")
        )

    def test_managerial_skills(self):
        self.search(
            "managerial skills",
            Common(
                subject=re.compile("(business|management)")
            )
        )

    def test_manga(self):
        # This has the same problem as the 'anime' test above --
        # we have tons of manga but it's not classified as "manga".
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

    def test_native_american_misspelled(self):
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
            [
                # Most works will show up because of a title match -- verify that we're talking about
                # Python as a programming language.
                Common(
                    title=re.compile("python", re.I), subject=re.compile("(computer technology|programming)", re.I), threshold=0.8,
                    first_must_match=False
                )
            ]
        )

    def test_sewing(self):
        self.search(
            "Sewing",
            [ FirstMatch(title=re.compile("sewing")),
              Common(title=re.compile("sewing")),
            ]
        )

    def test_supervising(self):
        # Keyword
        self.search(
            "supervising", Common(genre="Business", first_must_match=False)
        )

    def test_tennis(self):
        # We will get sports books with "Tennis" in the title.
        self.search(
            "tennis",
            Common(title=re.compile("Tennis", re.I),
                   genre=re.compile("(Sports|Games)", re.I))
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

class TestFuzzyConfounders(SearchTest):
    """Test searches on very distinct terms that are near each other in
    Levenstein distance.
    """

    # amulet / hamlet / harlem / tablet
    def test_amulet(self):
        self.search(
            "amulet",
            [Common(title_or_subtitle=re.compile("amulet")),
             Uncommon(title_or_subtitle=re.compile("hamlet|harlem|tablet"))
            ]
        )

    def test_hamlet(self):
        self.search(
            "Hamlet",
            [Common(title_or_subtitle="Hamlet"),
             Uncommon(title_or_subtitle=re.compile("amulet|harlem|tablet"))
            ]
        )

    def test_harlem(self):
        self.search(
            "harlem",
            [Common(title_or_subtitle=re.compile("harlem")),
             Uncommon(title_or_subtitle=re.compile("amulet|hamlet|tablet"))
            ]
        )

    def test_tablet(self):
        self.search(
            "tablet",
            [Common(title_or_subtitle=re.compile("tablet")),
             Uncommon(title_or_subtitle=re.compile("amulet|hamlet|harlem"))
            ]
        )

    # baseball / basketball
    def test_baseball(self):
        self.search(
            "baseball",
            [Common(title=re.compile("baseball")),
             Uncommon(title=re.compile("basketball"))]
        )

    def test_basketball(self):
        self.search(
            "basketball",
            [Common(title=re.compile("basketball")),
             Uncommon(title=re.compile("baseball"))]
        )

    # car / war
    def test_car(self):
        self.search(
            "car",
            # There is a book called "Car Wars", so we can't
            # completely prohibit 'war' from showing up.
            [Common(title=re.compile("car")),
             Uncommon(title=re.compile("war"), threshold=0.1)]
        )

    def test_war(self):
        self.search(
            "war",
            [Common(title=re.compile("war")),
             Uncommon(title=re.compile("car"))]
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
            SpecificSeries(series="Dinosaur Cove")
        )

    def test_poldi(self):
        # NYPL's collection only has one book from this series.
        self.search(
            "Auntie poldi",
            FirstMatch(series="Auntie Poldi")
        )

    def test_39_clues(self):
        # We have many books in this series.
        self.search("39 clues", SpecificSeries(series="the 39 clues"))

    def test_maggie_hope(self):
        # We have many books in this series.
        self.search(
            "Maggie hope",
            SpecificSeries(series="Maggie Hope", threshold=0.9)
        )

    def test_game_of_thrones(self):
        # People often search for the name of the TV show, but the
        # series name is different. There are so many books about the
        # TV show that results are dominated by title matches, but
        # there is also a novel called "A Game of Thrones", and we
        # find that.
        self.search(
            "game of thrones",
            [Common(title=re.compile("Game of Thrones", re.I)),
             AtLeastOne(series="a song of ice and fire")
            ]
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
            SpecificSeries(
                series="Harry Potter", threshold=0.9, first_must_match=False
            )
        )

    def test_maisie_dobbs(self):
        # Misspelled proper noun
        self.search(
            "maise dobbs",
            SpecificSeries(series="Maisie Dobbs", threshold=0.5)
        )

    def test_gossip_girl(self):
        self.search(
            "Gossip girl",
            SpecificSeries(
                series="Gossip Girl",
                author=re.compile("cecily von ziegesar"),
            ),
        )

    def test_gossip_girl_misspelled(self):
        # Typo in the first character of a word.
        self.search(
            "Gossip hirl",
            SpecificSeries(
                series="Gossip Girl",
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
        self.search(
            "goosebumps",
            SpecificSeries(
                series="Goosebumps", author="R. L. Stine",
            )
        )

    def test_goosebump_singular(self):
        self.search(
            "goosebump",
            SpecificSeries(
                series="Goosebumps", author="R. L. Stine",
            )
        )

    def test_goosebumps_misspelled(self):
        self.search(
            "goosebump",
            SpecificSeries(
                series="Goosebumps", author="R. L. Stine",
            )
        )

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
        self.search(
            "the hunger games", SpecificSeries(series="The Hunger Games")
        )

    def test_hunger_games_misspelled(self):
        self.search(
            "The hinger games",
            SpecificSeries(series="The Hunger Games")
        )

    def test_mockingjay(self):
        self.search(
            "The hunger games mockingjay",
            [FirstMatch(title="Mockingjay"),
             SpecificSeries(series="The Hunger Games")]
        )

    def test_i_funny(self):
        self.search(
            "i funny",
            SpecificSeries(series="I, Funny", author="Chris Grabenstein"),
        )

    def test_foundation(self):
        # Series and full author. This should only get Foundation
        # books *by Isaac Asimov*, not books in the same series by
        # other authors.
        self.search(
            "Isaac asimov foundation",
            [
                FirstMatch(title="Foundation"),
                SpecificSeries(series="Foundation", author="Isaac Asimov")
            ]
        )

    def test_dark_tower(self):
        # There exist two completely unrelated books called "The Dark
        # Tower"--it's fine for one of those to be the first result.
        self.search(
            "The dark tower", [
                SpecificSeries(
                    series="The Dark Tower",
                    author="Stephen King", first_must_match=False
                )
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

    def test_who_is(self):
        # These children's biographies don't have .series set but
        # are clearly part of a series.
        #
        # Because those books don't have .series set, the matches
        # happen solely through title, so unrelated books like "Who Is
        # Rich?" appear to be part of the series.
        self.search("who is", SpecificSeries(series="Who Is"))

    def test_who_was(self):
        # From the same series of biographies as test_who_is().
        self.search("who was", SpecificSeries(series="Who Was"))

    def test_wimpy_kid_misspelled(self):
        # Series name contains the wrong stopword ('the' vs 'a')
        self.search(
            "dairy of the wimpy kid",
            SpecificSeries(series="Diary of a Wimpy Kid")
        )


class TestSeriesTitleMatch(SearchTest):
    """Test a search that tries to match a specific book in a series."""

    def test_39_clues_specific_title(self):
        self.search(
            "39 clues maze of bones",
            [
                FirstMatch(title="The Maze of Bones"),
                SpecificSeries(series="the 39 clues")
            ]
        )

    def test_harry_potter_specific_title(self):
        # The first result is the requested title.
        #
        # NOTE: It would be good if other results came be from the
        # same series, but this doesn't happen much compared to other,
        # similar tests. We get more partial title matches.
        self.search(
            "chamber of secrets", [
                FirstMatch(title="Harry Potter and the Chamber of Secrets"),
                SpecificSeries(series="Harry Potter", threshold=0.2)
            ]
        )

    @known_to_fail
    def test_wimpy_kid_specific_title(self):
        # The first result is the requested title. Other results
        # are from the same series.
        #
        # NOTE: The title match is too powerful -- "Wimpy Kid"
        # overrides "Dog Days"
        self.search(
            "dairy of the wimpy kid dog days",
            [
                FirstMatch(title="Dog Days", author="Jeff Kinney"),
                SpecificSeries(
                    series="Diary of a Wimpy Kid", author="Jeff Kinney"
                )
            ]
        )

    @known_to_fail
    def test_foundation_specific_title_by_number(self):
        # NOTE: we don't have series position information for this series,
        # and we don't search it, so there's no way to make this work.
        self.search(
            "Isaac Asimov foundation book 1",
            FirstMatch(series="Foundation", title="Foundation")
        )

    @known_to_fail
    def test_survivors_specific_title(self):
        # NOTE: This gives a lot of title matches for "Survivor"
        # or "Survivors". Theoretically we could use "book 1"
        # as a signal that we only want a series match.
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

    @known_to_fail
    def test_incorrect_2(self):
        # NOTE: This gives good results overall but the first
        # match is "I Had to Survive", which is understandable
        # but not the best match.
        self.search("i survive")

    def test_incorrect_3(self):
        self.search("i survided")


class TestDorkDiaries(VariantSearchTest):
    # Test different ways of spelling "Dork Diaries"
    EVALUATOR = SpecificAuthor(re.compile(u"Rachel .* Russell", re.I))

    def test_correct_spelling(self):
        self.search('dork diaries')

    def test_misspelling_and_number(self):
        self.search("dork diarys #11")

    @known_to_fail
    def test_misspelling_with_punctuation(self):
        self.search('doke diaries.')

    def test_singular(self):
        self.search("dork diary")

    def test_misspelling_1(self):
        self.search('dork diarys')

    @known_to_fail
    def test_misspelling_2(self):
        self.search('doke dirares')

    @known_to_fail
    def test_misspelling_3(self):
        self.search('doke dares')

    @known_to_fail
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

    @known_to_fail
    def test_misspelling_1(self):
        # NOTE: This gets a title match on "Cousin Pons"
        self.search("my little pon")

    def test_misspelling_2(self):
        self.search("my little ponie")


class TestLanguageRestriction(SearchTest):
    # Verify that adding the name of a language restricts the results
    # to books in that language.
    #
    # NOTE: We don't parse language out of queries, so if any of these
    # work it's because the name of the language is present in some
    # other field.

    def test_language_espanol(self):
        # "Espanol" is itself a Spanish word, so it would mainly show
        # up in metadata for Spanish titles.
        self.search("espanol", Common(language="spa"))

    @known_to_fail
    def test_language_spanish(self):
        self.search("spanish", Common(language="spa"))

    @known_to_fail
    def test_author_with_language(self):
        self.search(
            "Pablo escobar spanish",
            FirstMatch(author="Pablo Escobar", language="spa")
        )

    def test_gatos(self):
        # Searching for a Spanish word should mostly bring up books in Spanish,
        # since that's where the word would be used.
        #
        # However, 'gatos' also shows up in English, e.g. in place names.
        self.search(
            "gatos",
            Common(language="spa", threshold=0.7)
        )


class TestAwardSearch(SearchTest):
    # Attempts to find books that won particular awards.

    @known_to_fail
    def test_hugo(self):
        # This has big problems because the name of the award is also
        # a very common personal name.
        self.search(
            "hugo award",
            [
                Common(summary=re.compile("hugo award")),
                Uncommon(author="Victor Hugo"),
                Uncommon(series=re.compile("hugo")),
            ]
        )

    def test_nebula(self):
        self.search(
            "nebula award",
            Common(summary=re.compile("nebula award"))
        )

    def test_nebula_no_award(self):
        # This one does great -- the award is the most common
        # use of the word "nebula".
        self.search(
            "nebula",
            Common(summary=re.compile("nebula award"))
        )

    def test_world_fantasy(self):
        # This award contains the name of a genre.
        self.search(
            "world fantasy award",
            Common(summary=re.compile("world fantasy award"),
                   first_must_match=False)
        )

    @known_to_fail
    def test_tiptree_award(self):
        # This award is named after an author. We don't want their
        # books -- we want the award winners.
        self.search(
            "tiptree award",
            [Common(summary=re.compile("tiptree award")),
             Uncommon(author=re.compile("james tiptree"))],
        )

    @known_to_fail
    def test_newberry(self):
        # Tends to get author matches.
        self.search(
            "newbery",
            Common(summary=re.compile("newbery medal"))
        )

    @known_to_fail
    def test_man_booker(self):
        # This gets author and title matches.
        self.search(
            "man booker prize",
            Common(summary=re.compile("man booker prize"),
                   first_must_match=False)
        )

    def test_award_winning(self):
        # NOTE: It's unclear how to validate these results, but it's
        # more likely an award-winning book will mention "award" in
        # its summary than in its title.
        self.search(
            "award-winning",
            [
                Common(summary=re.compile("award"), threshold=0.5),
                Uncommon(title=re.compile("award"), threshold=0.5),
            ]
        )

    @known_to_fail
    def test_staff_picks(self):
        # We're looking for books that are this library's staff picks,
        # not books attributed to some company's "staff".
        #
        # We don't know which books are staff picks, but we can check
        # that the obvious wrong answers don't show up.
        self.search(
            "staff picks",
            [
                Uncommon(author=re.compile("(staff|picks)")),
                Uncommon(title=re.compile("(staff|picks)"))
            ]
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
            ]
        )

    @known_to_fail
    def test_3_little_pigs_more_precise(self):
        # NOTE: This would require that '3' and 'three' be analyzed
        # the same way.
        self.search(
            "3 little pigs",
            FirstMatch(title="Three Little Pigs"),
        )


    def test_batman(self):
        self.search(
            "batman book",
            Common(title=re.compile("batman"))
        )

    @known_to_fail
    def test_batman_two_words(self):
        # Patron is searching for 'batman' but treats it as two words.
        self.search(
            "bat man book",
            Common(title=re.compile("batman"))
        )

    def test_christian_grey(self):
        # This search uses a character name to stand in for a series.
        self.search(
            "christian grey",
            FirstMatch(author=re.compile("E.\s*L.\s*James", re.I))
        )

    def test_spiderman_hyphenated(self):
        self.search(
            "spider-man", Common(title=re.compile("spider-man"))
        )

    @known_to_fail
    def test_spiderman_one_word(self):
        # NOTE: There are some Spider-Man titles but not as many as
        # with the hyphen.
        self.search(
            "spiderman", Common(title=re.compile("spider-man"))
        )

    @known_to_fail
    def test_spiderman_run_on(self):
        # NOTE: This gets no results at all.
        self.search(
            "spidermanbook", Common(title=re.compile("spider-man"))
        )


    def test_teen_titans(self):
        self.search(
            "teen titans",
            Common(title=re.compile("^teen titans")), limit=5
        )

    @known_to_fail
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
                Common(
                    author="Timothy Zahn", series=re.compile("star wars", re.I),
                    threshold=0.9
                ),
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

    def test_chapter_books_misspelled_1(self):
        # NOTE: We don't do fuzzy matching on things that would become
        # filter terms. When this works, it's because of fuzzy title
        # matches and description matches.
        self.search(
            "chapter bookd", Common(target_age=(6, 10))
        )

    @known_to_fail
    def test_chapter_books_misspelled_2(self):
        # This fails for a similar reason as misspelled_1, though it
        # actually does a little better -- only the first result is
        # bad.
        self.search(
            "chaptr books", Common(target_age=(6, 10))
        )

    @known_to_fail
    def test_grade_and_subject(self):
        # NOTE: this doesn't work because we don't parse grade numbers
        # when they're spelled out, only when they're provided as
        # digits.
        self.search(
            "Seventh grade science",
            [
                Common(target_age=(12, 13)),
                Common(genre="Science")
            ]
        )


class TestSearchOnStopwords(SearchTest):
    # These tests verify our ability to search, when necessary, using
    # words that are normally stripped out as stopwords.
    def test_black_and_the_blue(self):
        # This is a real book title that is almost entirely stopwords.
        # Putting in a few words of the title will find that specific
        # title even if most of the words are stopwords.
        self.search(
            "the black and",
            FirstMatch(title="The Black and the Blue")
        )

    @known_to_fail
    def test_the_real(self):
        # This is vague, but we get "The Real" results
        # over just "Real" results.
        #
        # NOTE: These results are very good, but the first result is
        # "Tiger: The Real Story", which is a subtitle match. A title match
        # should be better.
        self.search(
            "the real",
            Common(title=re.compile("The Real", re.I))
        )

    def test_nothing_but_stopwords(self):
        # If we always stripped stopwords, this would match nothing,
        # but we get the best results we can manage -- e.g.
        # "History of Florence and of the Affairs of Italy"
        self.search(
            "and of the",
            Common(title_or_subtitle=re.compile("and of the", re.I))
        )


_db = production_session()
library = None

index = ExternalSearchIndex(_db)
SearchTest.searcher = Searcher(library, index)

def teardown_module():
    failures = SearchTest.expected_failures
    if failures:
        logging.info(
            "%d tests were expected to fail, and did.", len(failures)
        )
    successes = SearchTest.unexpected_successes
    if successes:
        logging.info(
            "%d tests passed unexepectedly:", len(successes)
        )
        for success in successes:
            logging.info(
                "Line #%d: %s",
                success.func_code.co_firstlineno, success.func_name,
            )
