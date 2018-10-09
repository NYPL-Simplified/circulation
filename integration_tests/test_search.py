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
        if value:
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


class SpecificAuthor(FirstMatch):
    """The first result must be by a specific author.

    Most of the results must also be by that author.
    """

    def __init__(self, author, accept_book_about_author=False, threshold=0.5):
        super(SpecificAuthor, self).__init__(author=author)
        self.accept_book_about_author = accept_book_about_author
        self.threshold = threshold

    def evaluate_first(self, first):
        expect_author = self.author
        author = self._field('author', first)
        title = self._field('title', first)
        if expect_author == self.author:
            return
        if self.accept_book_about_author and expect_author in title:
            return
        set_trace()
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

    def search(self, query, evaluator, limit=10):
        query = query.lower()
        pagination = Pagination(size=limit)
        qu = self.searcher.query(query, pagination=pagination)
        hits = [x for x in qu][:]
        evaluator.evaluate(hits)

    
class TestTitleMatch(SearchTest):

    def test_simple_title_match(self):
        # There is one obvious right answer.
        self.search("carrie", FirstMatch(title="Carrie"))

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


class TestMixedTitleAuthorMatch(SearchTest):

    def test_centos_negus(self):
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

class GenreSearch(SearchTest):

    # Just search for a genre like "graphic novel"
    # Search for a title in a genre

    def test_iain_banks_sf(self):
        self.search(
            "iain banks science fiction",
            FirstMatch(author="Iain M. Banks", genre="Science Fiction")
        )

    # Search for author + genre


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

ES6 = ('es6' in os.environ['VIRTUAL_ENV'])
if ES6:
    url = os.environ['ES6_ELASTICSEARCH']
    index = "es6-test-v3"
else:
    # Use site settings.
    url = None
    index = None
        
_db = production_session()
library = Library.default(_db)
index = ExternalSearchIndex(_db, url=url, works_index=index)
index.works_alias = index
SearchTest.searcher = Searcher(library, index)
