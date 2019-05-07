from nose.tools import (
    assert_raises,
    assert_raises_regexp,
    eq_,
    set_trace,
)
import datetime
import json
import logging
import time
from psycopg2.extras import NumericRange

from . import (
    DatabaseTest,
)

from elasticsearch_dsl.query import Bool
from elasticsearch.exceptions import ElasticsearchException

from ..config import CannotLoadConfiguration
from ..lane import (
    Facets,
    Lane,
    Pagination,
)
from ..model import (
    Edition,
    ExternalIntegration,
    Genre,
    Work,
    WorkCoverageRecord,
)
from ..external_search import (
    ExternalSearchIndex,
    ExternalSearchIndexVersions,
    Filter,
    MAJOR_VERSION,
    MockExternalSearchIndex,
    MockSearchResult,
    Query,
    QueryParser,
    SearchBase,
    SearchIndexCoverageProvider,
    SearchIndexMonitor,
    SortKeyPagination,
)
# NOTE: external_search took care of handling the differences between
# Elasticsearch versions and making sure 'Q' and 'F' are set
# appropriately.  That's why we import them from external_search
# instead of elasticsearch_dsl.
from ..external_search import (
    Q,
    F,
)

from ..classifier import Classifier


class ClientForTesting(ExternalSearchIndex):
    """When creating an index, limit it to a single shard and disable
    replicas.

    This makes search results more predictable.
    """

    def setup_index(self, new_index=None):
        return super(ClientForTesting, self).setup_index(
            new_index, number_of_shards=1, number_of_replicas=0
        )


class ExternalSearchTest(DatabaseTest):
    """
    These tests require elasticsearch to be running locally. If it's not, or there's
    an error creating the index, the tests will pass without doing anything.

    Tests for elasticsearch are useful for ensuring that we haven't accidentally broken
    a type of search by changing analyzers or queries, but search needs to be tested manually
    to ensure that it works well overall, with a realistic index.
    """

    def setup(self):
        super(ExternalSearchTest, self).setup(mock_search=False)

        self.integration = self._external_integration(
            ExternalIntegration.ELASTICSEARCH,
            goal=ExternalIntegration.SEARCH_GOAL,
            url=u'http://localhost:9200',
            settings={
                ExternalSearchIndex.WORKS_INDEX_PREFIX_KEY : u'test_index',
                ExternalSearchIndex.TEST_SEARCH_TERM_KEY : u'test_search_term',
            }
        )

        try:
            self.search = ClientForTesting(self._db)
        except Exception as e:
            self.search = None
            print "Unable to set up elasticsearch index, search tests will be skipped."
            print e

    def teardown(self):
        if self.search:
            if self.search.works_index:
                self.search.indices.delete(self.search.works_index, ignore=[404])
            self.search.indices.delete('the_other_index', ignore=[404])
            self.search.indices.delete('test_index-v100', ignore=[404])
            self.search.indices.delete('test_index-v9999', ignore=[404])
            ExternalSearchIndex.reset()
        super(ExternalSearchTest, self).teardown()

    def default_work(self, *args, **kwargs):
        """Convenience method to create a work with a license pool
        in the default collection.
        """
        work = self._work(
            *args, with_license_pool=True,
            collection=self._default_collection, **kwargs
        )
        work.set_presentation_ready()
        return work


class TestExternalSearch(ExternalSearchTest):

    def test_constructor(self):
        # The configuration of the search ExternalIntegration becomes the
        # configuration of the ExternalSearchIndex.
        #
        # This basically just verifies that the test search term is taken
        # from the ExternalIntegration.
        class Mock(ExternalSearchIndex):
            def set_works_index_and_alias(self, _db):
                self.set_works_index_and_alias_called_with = _db

        index = Mock(self._db, in_testing=True)
        eq_(self._db, index.set_works_index_and_alias_called_with)
        eq_("test_search_term", index.test_search_term)

    def test_elasticsearch_error_in_constructor_becomes_cannotloadconfiguration(self):
        """If we're unable to establish a connection to the Elasticsearch
        server, CannotLoadConfiguration (which the circulation manager can
        understand) is raised instead of an Elasticsearch-specific exception.
        """

        # Unlike other tests in this module, this one runs even if no
        # ElasticSearch server is running, since it's testing what
        # happens if there's a problem communicating with that server.
        class Mock(ExternalSearchIndex):
            def set_works_index_and_alias(self, _db):
                raise ElasticsearchException("very bad")

        assert_raises_regexp(
            CannotLoadConfiguration,
            "Exception communicating with Elasticsearch server:.*very bad",
            Mock, self._db
        )

    def test_works_index_name(self):
        """The name of the search index is the prefix (defined in
        ExternalSearchTest.setup) plus a version number associated
        with this version of the core code.
        """
        if not self.search:
            return
        eq_("test_index-v4", self.search.works_index_name(self._db))

    def test_setup_index_creates_new_index(self):
        if not self.search:
            return

        current_index = self.search.works_index
        self.search.setup_index(new_index='the_other_index')

        # Both indices exist.
        eq_(True, self.search.indices.exists(current_index))
        eq_(True, self.search.indices.exists('the_other_index'))

        # The index for the app's search is still the original index.
        eq_(current_index, self.search.works_index)

        # The alias hasn't been passed over to the new index.
        alias = 'test_index-' + self.search.CURRENT_ALIAS_SUFFIX
        eq_(alias, self.search.works_alias)
        eq_(True, self.search.indices.exists_alias(current_index, alias))
        eq_(False, self.search.indices.exists_alias('the_other_index', alias))

    def test_set_works_index_and_alias(self):
        if not self.search:
            return

        # If the index or alias don't exist, set_works_index_and_alias
        # will create them.
        self.integration.set_setting(ExternalSearchIndex.WORKS_INDEX_PREFIX_KEY, u'banana')
        self.search.set_works_index_and_alias(self._db)

        expected_index = 'banana-' + ExternalSearchIndexVersions.latest()
        expected_alias = 'banana-' + self.search.CURRENT_ALIAS_SUFFIX
        eq_(expected_index, self.search.works_index)
        eq_(expected_alias, self.search.works_alias)

        # If the index and alias already exist, set_works_index_and_alias
        # does nothing.
        self.search.set_works_index_and_alias(self._db)
        eq_(expected_index, self.search.works_index)
        eq_(expected_alias, self.search.works_alias)

    def test_setup_current_alias(self):
        if not self.search:
            return

        # The index was generated from the string in configuration.
        version = ExternalSearchIndexVersions.VERSIONS[-1]
        index_name = 'test_index-' + version
        eq_(index_name, self.search.works_index)
        eq_(True, self.search.indices.exists(index_name))

        # The alias is also created from the configuration.
        alias = 'test_index-' + self.search.CURRENT_ALIAS_SUFFIX
        eq_(alias, self.search.works_alias)
        eq_(True, self.search.indices.exists_alias(index_name, alias))

        # If the -current alias is already set on a different index, it
        # won't be reassigned. Instead, search will occur against the
        # index itself.
        ExternalSearchIndex.reset()
        self.integration.set_setting(ExternalSearchIndex.WORKS_INDEX_PREFIX_KEY, u'my-app')
        self.search = ExternalSearchIndex(self._db)

        eq_('my-app-%s' % version, self.search.works_index)
        eq_('my-app-' + self.search.CURRENT_ALIAS_SUFFIX, self.search.works_alias)

    def test_transfer_current_alias(self):
        if not self.search:
            return

        # An error is raised if you try to set the alias to point to
        # an index that doesn't already exist.
        assert_raises(
            ValueError, self.search.transfer_current_alias, self._db,
            'no-such-index'
        )

        original_index = self.search.works_index

        # If the -current alias doesn't exist, it's created
        # and everything is updated accordingly.
        self.search.indices.delete_alias(
            index=original_index, name='test_index-current', ignore=[404]
        )
        self.search.setup_index(new_index='test_index-v9999')
        self.search.transfer_current_alias(self._db, 'test_index-v9999')
        eq_('test_index-v9999', self.search.works_index)
        eq_('test_index-current', self.search.works_alias)

        # If the -current alias already exists on the index,
        # it's used without a problem.
        self.search.transfer_current_alias(self._db, 'test_index-v9999')
        eq_('test_index-v9999', self.search.works_index)
        eq_('test_index-current', self.search.works_alias)

        # If the -current alias is being used on a different version of the
        # index, it's deleted from that index and placed on the new one.
        self.search.setup_index(original_index)
        self.search.transfer_current_alias(self._db, original_index)
        eq_(original_index, self.search.works_index)
        eq_('test_index-current', self.search.works_alias)

        # It has been removed from other index.
        eq_(False, self.search.indices.exists_alias(
            index='test_index-v9999', name='test_index-current'))

        # And only exists on the new index.
        alias_indices = self.search.indices.get_alias(name='test_index-current').keys()
        eq_([original_index], alias_indices)

        # If the index doesn't have the same base name, an error is raised.
        assert_raises(
            ValueError, self.search.transfer_current_alias, self._db,
            'banana-v10'
        )

    def test__run_self_tests(self):
        index = MockExternalSearchIndex()

        # First, see what happens when the search returns no results.
        test_results = [x for x in index._run_self_tests(self._db, in_testing=True)]

        eq_("Search results for 'a search term':", test_results[0].name)
        eq_(True, test_results[0].success)
        eq_([], test_results[0].result)

        eq_("Search document for 'a search term':", test_results[1].name)
        eq_(True, test_results[1].success)
        eq_("[]", test_results[1].result)

        eq_("Raw search results for 'a search term':", test_results[2].name)
        eq_(True, test_results[2].success)
        eq_([], test_results[2].result)

        eq_("Total number of search results for 'a search term':", test_results[3].name)
        eq_(True, test_results[3].success)
        eq_("0", test_results[3].result)

        eq_("Total number of documents in this search index:", test_results[4].name)
        eq_(True, test_results[4].success)
        eq_("0", test_results[4].result)

        eq_("Total number of documents per collection:", test_results[5].name)
        eq_(True, test_results[5].success)
        eq_("{}", test_results[5].result)

        # Set up the search index so it will return a result.
        collection = self._collection()

        search_result = MockSearchResult(
            "Sample Book Title", "author", {}, "id"
        )
        index.index("index", "doc type", "id", search_result)
        test_results = [x for x in index._run_self_tests(self._db, in_testing=True)]


        eq_("Search results for 'a search term':", test_results[0].name)
        eq_(True, test_results[0].success)
        eq_(["Sample Book Title (author)"], test_results[0].result)

        eq_("Search document for 'a search term':", test_results[1].name)
        eq_(True, test_results[1].success)
        result = json.loads(test_results[1].result)
        eq_({"author": "author", "meta": {"id": "id"}, "id": "id", "title": "Sample Book Title"}, result)

        eq_("Raw search results for 'a search term':", test_results[2].name)
        eq_(True, test_results[2].success)
        result = json.loads(test_results[2].result[0])
        eq_({"author": "author", "meta": {"id": "id"}, "id": "id", "title": "Sample Book Title"}, result)

        eq_("Total number of search results for 'a search term':", test_results[3].name)
        eq_(True, test_results[3].success)
        eq_("1", test_results[3].result)

        eq_("Total number of documents in this search index:", test_results[4].name)
        eq_(True, test_results[4].success)
        eq_("1", test_results[4].result)

        eq_("Total number of documents per collection:", test_results[5].name)
        eq_(True, test_results[5].success)
        result = json.loads(test_results[5].result)
        eq_({collection.name: 1}, result)

class EndToEndExternalSearchTest(ExternalSearchTest):
    """Subclasses of this class set up real works in a real
    search index and run searches against it.
    """

    def _expect_results(self, works, *query_args, **kwargs):
        """Helper function to call query() and verify that it
        returns certain work IDs.


        :param ordered: If this is True (the default), then the
        assertion will only succeed if the search results come in in
        the exact order specified in `works`. If this is False, then
        those exact results must come up, but their order is not
        what's being tested.
        """
        if isinstance(works, Work):
            works = [works]

        should_be_ordered = kwargs.pop('ordered', True)

        results = self.search.query_works(*query_args, debug=True, **kwargs)
        expect = [x.id for x in works]
        expect_ids = ", ".join(map(str, expect))
        expect_titles = ", ".join([x.title for x in works])
        result_works = self._db.query(Work).filter(Work.id.in_(results))
        result_works_dict = {}

        if not should_be_ordered:
            expect = set(expect)
            results = set(results)

        # Get the titles of the works that were actually returned, to
        # make comparisons easier.
        for work in result_works:
            result_works_dict[work.id] = work
        result_titles = []
        for id in results:
            work = result_works_dict.get(id)
            if work:
                result_titles.append(work.title)
            else:
                result_titles.append("[unknown]")

        eq_(
            expect, results,
            "Query args %r did not find %d works (%s/%s), instead found %d (%s/%s)" % (
                query_args, len(expect), expect_ids, expect_titles,
                len(results), ", ".join(map(str,results)),
                ", ".join(result_titles)
            )
        )


class TestExternalSearchWithWorks(EndToEndExternalSearchTest):
    """These tests run against a real search index with works in it.
    The setup is very slow, so all the tests are in the same method.
    Don't add new methods to this class - add more tests into test_query_works,
    or add a new test class.
    """

    def setup(self):
        super(TestExternalSearchWithWorks, self).setup()
        _work = self.default_work

        if self.search:

            self.moby_dick = _work(
                title="Moby Dick", authors="Herman Melville", fiction=True,
            )
            self.moby_dick.presentation_edition.subtitle = "Or, the Whale"
            self.moby_dick.presentation_edition.series = "Classics"
            self.moby_dick.summary_text = "Ishmael"
            self.moby_dick.presentation_edition.publisher = "Project Gutenberg"

            self.moby_duck = _work(title="Moby Duck", authors="Donovan Hohn", fiction=False)
            self.moby_duck.presentation_edition.subtitle = "The True Story of 28,800 Bath Toys Lost at Sea"
            self.moby_duck.summary_text = "A compulsively readable narrative"
            self.moby_duck.presentation_edition.publisher = "Penguin"
            # This book is not currently available. It will still show up
            # in search results unless the library's settings disable it.
            self.moby_duck.license_pools[0].licenses_available = 0

            self.title_match = _work(title="Match")

            self.subtitle_match = _work(title="SubtitleM")
            self.subtitle_match.presentation_edition.subtitle = "Match"

            self.summary_match = _work(title="SummaryM")
            self.summary_match.summary_text = "Match"

            self.publisher_match = _work(title="PublisherM")
            self.publisher_match.presentation_edition.publisher = "Match"

            self.tess = _work(title="Tess of the d'Urbervilles")

            self.tiffany = _work(title="Breakfast at Tiffany's")

            self.les_mis = _work()
            self.les_mis.presentation_edition.title = u"Les Mis\u00E9rables"

            self.modern_romance = _work()
            self.modern_romance.presentation_edition.title = u"Modern Romance"

            self.lincoln = _work(genre="Biography & Memoir", title="Abraham Lincoln")

            self.washington = _work(genre="Biography", title="George Washington")

            self.lincoln_vampire = _work(title="Abraham Lincoln: Vampire Hunter", genre="Fantasy")

            self.children_work = _work(title="Alice in Wonderland", audience=Classifier.AUDIENCE_CHILDREN)

            self.ya_work = _work(title="Go Ask Alice", audience=Classifier.AUDIENCE_YOUNG_ADULT)

            self.adult_work = _work(title="Still Alice", audience=Classifier.AUDIENCE_ADULT)

            self.ya_romance = _work(
                title="Gumby In Love",
                audience=Classifier.AUDIENCE_YOUNG_ADULT, genre="Romance"
            )
            self.ya_romance.presentation_edition.subtitle = (
                "Modern Fairytale Series, Volume 7"
            )

            self.no_age = _work()
            self.no_age.summary_text = "President Barack Obama's election in 2008 energized the United States"

            self.age_4_5 = _work()
            self.age_4_5.target_age = NumericRange(4, 5, '[]')
            self.age_4_5.summary_text = "President Barack Obama's election in 2008 energized the United States"

            self.age_5_6 = _work(fiction=False)
            self.age_5_6.target_age = NumericRange(5, 6, '[]')

            self.obama = _work(genre="Biography & Memoir")
            self.obama.target_age = NumericRange(8, 8, '[]')
            self.obama.summary_text = "President Barack Obama's election in 2008 energized the United States"

            self.dodger = _work()
            self.dodger.target_age = NumericRange(8, 8, '[]')
            self.dodger.summary_text = "Willie finds himself running for student council president"

            self.age_9_10 = _work()
            self.age_9_10.target_age = NumericRange(9, 10, '[]')
            self.age_9_10.summary_text = "President Barack Obama's election in 2008 energized the United States"

            self.age_2_10 = _work()
            self.age_2_10.target_age = NumericRange(2, 10, '[]')

            self.pride = _work(title="Pride and Prejudice")
            self.pride.presentation_edition.medium = Edition.BOOK_MEDIUM

            self.pride_audio = _work(title="Pride and Prejudice")
            self.pride_audio.presentation_edition.medium = Edition.AUDIO_MEDIUM

            self.sherlock = _work(
                title="The Adventures of Sherlock Holmes",
                with_open_access_download=True
            )
            self.sherlock.presentation_edition.language = "en"

            self.sherlock_spanish = _work(title="Las Aventuras de Sherlock Holmes")
            self.sherlock_spanish.presentation_edition.language = "es"

            # Create a custom list that contains a few books.
            self.presidential, ignore = self._customlist(
                name="Nonfiction about US Presidents", num_entries=0
            )
            for work in [self.washington, self.lincoln, self.obama]:
                self.presidential.add_entry(work)

            # Create a second collection that only contains a few books.
            self.tiny_collection = self._collection("A Tiny Collection")
            self.tiny_book = self._work(
                title="A Tiny Book", with_license_pool=True,
                collection=self.tiny_collection
            )

            # Both collections contain 'The Adventures of Sherlock
            # Holmes", but each collection licenses the book through a
            # different mechanism.
            self.sherlock_pool_2 = self._licensepool(
                edition=self.sherlock.presentation_edition,
                collection=self.tiny_collection
            )

            sherlock_2, is_new = self.sherlock_pool_2.calculate_work()
            eq_(self.sherlock, sherlock_2)
            eq_(2, len(self.sherlock.license_pools))

            # These books look good for some search results, but they
            # will be filtered out by the universal filters, and will
            # never show up in results.

            # We own no copies of this book.
            self.no_copies = _work(title="Moby Dick 2")
            self.no_copies.license_pools[0].licenses_owned = 0

            # This book's only license pool has been suppressed.
            self.suppressed = _work(title="Moby Dick 2")
            self.suppressed.license_pools[0].suppressed = True

            # This book is not presentation_ready.
            self.not_presentation_ready = _work(title="Moby Dick 2")
            self.not_presentation_ready.presentation_ready = False

        # Just a basic check to make sure the search document query
        # doesn't contain over-zealous joins. This is the main place
        # where we make a large number of works and generate search
        # documents for them.
        eq_(1, len(self.moby_dick.to_search_document()['licensepools']))
        eq_("Audio",
            self.pride_audio.to_search_document()['licensepools'][0]['medium'])

    def test_query_works(self):
        # An end-to-end test of the search functionality.
        #
        # Works created during setup are added to a real search index.
        # We then run actual Elasticsearch queries against the
        # search index and verify that the work IDs returned
        # are the ones we expect.
        if not self.search:
            logging.error(
                "Search is not configured, skipping test_query_works."
            )
            return

        # Add all the works created in the setup to the search index.
        SearchIndexCoverageProvider(
            self._db, search_index_client=self.search
        ).run_once_and_update_timestamp()

        # Sleep to give the index time to catch up.
        time.sleep(2)

        # Set up convenient aliases for methods we'll be calling a
        # lot.
        query = self.search.query_works
        expect = self._expect_results

        # First, test pagination.
        first_item = Pagination(size=1, offset=0)
        expect(self.moby_dick, "moby dick", None, first_item)

        second_item = first_item.next_page
        expect(self.moby_duck, "moby dick", None, second_item)

        two_per_page = Pagination(size=2, offset=0)
        expect(
            [self.moby_dick, self.moby_duck],
            "moby dick", None, two_per_page
        )

        # Now try some different search queries.

        # Search in title.
        eq_(2, len(query("moby")))

        # Search in author name
        expect(self.moby_dick, "melville")

        # Search in subtitle
        expect(self.moby_dick, "whale")

        # Search in series.
        expect(self.moby_dick, "classics")

        # Search in summary.
        expect(self.moby_dick, "ishmael")

        # Search in publisher name.
        expect(self.moby_dick, "gutenberg")

        # Title > subtitle > summary > publisher.
        if MAJOR_VERSION == 1:
            order = [
                self.title_match,
                self.subtitle_match,
                self.summary_match,
                self.publisher_match,
            ]
        else:
            # TODO: This is incorrect -- summary is boosted way too much.
            order = [
                self.title_match,
                self.summary_match,
                self.subtitle_match,
                self.publisher_match,
            ]
        expect(order, "match")

        # (title match + author match) > title match
        expect(
            [self.moby_dick, self.moby_duck],
            "moby melville"
        )

        # Match a quoted phrase
        # 'Moby-Dick' is the first result because it's an exact title
        # match. 'Moby Duck' is the second result because it's a fuzzy
        # match,
        expect([self.moby_dick, self.moby_duck], '"moby dick"')

        # Match a stemmed word: 'running' is stemmed to 'run', and
        # so is 'runs'.
        expect(self.dodger, "runs")

        # Match a misspelled phrase: 'movy' -> 'moby'.
        expect([self.moby_dick, self.moby_duck], "movy", ordered=False)

        # Match a misspelled author: 'mleville' -> 'melville'
        expect(self.moby_dick, "mleville")

        expect([self.moby_dick, self.moby_duck], "mo by dick")

        # A query without an apostrophe matches a word that contains one.
        # (NOTE: it's not clear whether this is a feature of the index or
        # done by the fuzzy match.)
        expect(self.tess, "durbervilles")
        expect(self.tiffany, "tiffanys")

        # A query with an 'e' matches a word that contains an
        # e-with-acute. (this is managed by the 'asciifolding' filter in
        # the analyzers)
        expect(self.les_mis, "les miserables")

        # Find results based on fiction status.
        #
        # Here, Moby-Dick (fiction) is privileged over Moby Duck
        # (nonfiction)
        expect([self.moby_dick, self.moby_duck], "fiction moby")

        # Here, Moby Duck is privileged over Moby-Dick.
        expect([self.moby_duck, self.moby_dick], "nonfiction moby")

        # Find results based on genre.

        if MAJOR_VERSION == 1:
            # The name of the genre also shows up in the title of a
            # book, but the genre boost means the romance novel is the
            # first result.
            expect([self.ya_romance, self.modern_romance], "romance")
        else:
            # In ES6, the title boost is higher (TODO: how?) so
            # the book with 'romance' in the title is the first result.
            expect([self.modern_romance, self.ya_romance], "romance")

        # Find results based on audience.
        expect(self.children_work, "children's")

        expect(
            [self.ya_work, self.ya_romance], "young adult", ordered=False
        )

        # Find results based on grade level or target age.
        for q in ('grade 4', 'grade 4-6', 'age 9'):
            # ages 9-10 is a better result because a book targeted
            # toward a narrow range is a better match than a book
            # targeted toward a wide range.
            expect([self.age_9_10, self.age_2_10], q)

        # TODO: The target age query only scores how big the overlap
        # is, it doesn't look at how large the non-overlapping part of
        # the range is. So the 2-10 book can show up before the 9-10
        # book. This could be improved.
        expect([self.age_9_10, self.age_2_10], "age 10-12", ordered=False)

        # Books whose target age are closer to the requested range
        # are ranked higher.
        expect([self.age_4_5, self.age_5_6, self.age_2_10], "age 3-5")

        # Search by a combination of genre and audience.

        # The book with 'Romance' in the title shows up, but it's
        # after the book whose audience matches 'young adult' and
        # whose genre matches 'romance'.
        expect([self.ya_romance, self.modern_romance], "young adult romance")

        # Search by a combination of target age and fiction
        #
        # Two books match the age range, but the one with a
        # tighter age range comes first.
        expect([self.age_4_5, self.age_2_10], "age 5 fiction")

        # Search by a combination of genre and title

        # Two books match 'lincoln', but the biography comes first.
        expect([self.lincoln, self.lincoln_vampire], "lincoln biography")

        # Search by age + genre + summary
        results = query("age 8 president biography")

        # There are a number of results, but the top one is a presidential
        # biography for 8-year-olds.
        eq_(5, len(results))
        eq_(self.obama.id, results[0])

        # Now we'll test filters.

        # Both self.pride and self.pride_audio match the search query,
        # but the filters eliminate one or the other from
        # consideration.
        book_filter = Filter(media=Edition.BOOK_MEDIUM)
        audio_filter = Filter(media=Edition.AUDIO_MEDIUM)
        expect(self.pride, "pride and prejudice", book_filter)
        expect(self.pride_audio, "pride and prejudice", audio_filter)

        # Filters on languages
        english = Filter(languages="en")
        spanish = Filter(languages="es")
        both = Filter(languages=["en", "es"])

        expect(self.sherlock, "sherlock", english)
        expect(self.sherlock_spanish, "sherlock", spanish)
        expect(
            [self.sherlock, self.sherlock_spanish], "sherlock", both,
            ordered=False
        )

        # Filters on fiction status
        fiction = Filter(fiction=True)
        nonfiction = Filter(fiction=False)
        both = Filter()

        expect(self.moby_dick, "moby dick", fiction)
        expect(self.moby_duck, "moby dick", nonfiction)
        expect([self.moby_dick, self.moby_duck], "moby dick", both)

        # Filters on audience
        adult = Filter(audiences=Classifier.AUDIENCE_ADULT)
        ya = Filter(audiences=Classifier.AUDIENCE_YOUNG_ADULT)
        children = Filter(audiences=Classifier.AUDIENCE_CHILDREN)
        ya_and_children = Filter(
            audiences=[Classifier.AUDIENCE_CHILDREN,
                       Classifier.AUDIENCE_YOUNG_ADULT]
        )

        expect(self.adult_work, "alice", adult)
        expect(self.ya_work, "alice", ya)
        expect(self.children_work, "alice", children)

        expect([self.children_work, self.ya_work], "alice", ya_and_children,
               ordered=False)

        # Filters on age range
        age_8 = Filter(target_age=8)
        age_5_8 = Filter(target_age=(5,8))
        age_5_10 = Filter(target_age=(5,10))
        age_8_10 = Filter(target_age=(8,10))

        # As the age filter changes, different books appear and
        # disappear. no_age is always present since it has no age
        # restrictions.
        expect(
            [self.no_age, self.obama, self.dodger],
            "president", age_8, ordered=False
        )

        expect(
            [self.no_age, self.age_4_5, self.obama, self.dodger],
            "president", age_5_8, ordered=False
        )

        expect(
            [self.no_age, self.age_4_5, self.obama, self.dodger,
             self.age_9_10],
            "president", age_5_10, ordered=False
        )

        expect(
            [self.no_age, self.obama, self.dodger, self.age_9_10],
            "president", age_8_10, ordered=False
        )

        # Filters on genre

        biography, ignore = Genre.lookup(self._db, "Biography & Memoir")
        fantasy, ignore = Genre.lookup(self._db, "Fantasy")
        biography_filter = Filter(genre_restriction_sets=[[biography]])
        fantasy_filter = Filter(genre_restriction_sets=[[fantasy]])
        both = Filter(genre_restriction_sets=[[fantasy, biography]])

        expect(self.lincoln, "lincoln", biography_filter)
        expect(self.lincoln_vampire, "lincoln", fantasy_filter)
        expect([self.lincoln, self.lincoln_vampire], "lincoln", both,
               ordered=False)

        # Filters on list membership.

        # This ignores 'Abraham Lincoln, Vampire Hunter' because that
        # book isn't on the self.presidential list.
        on_presidential_list = Filter(
            customlist_restriction_sets=[[self.presidential]]
        )
        expect(self.lincoln, "lincoln", on_presidential_list)

        # This filters everything, since the query is restricted to
        # an empty set of lists.
        expect([], "lincoln", Filter(customlist_restriction_sets=[[]]))

        # Filter based on collection ID.

        # "A Tiny Book" isn't in the default collection.
        default_collection_only = Filter(collections=self._default_collection)
        expect([], "a tiny book", default_collection_only)

        # It is in the tiny_collection.
        other_collection_only = Filter(collections=self.tiny_collection)
        expect(self.tiny_book, "a tiny book", other_collection_only)

        # If a book is present in two different collections which are
        # being searched, it only shows up in search results once.
        f = Filter(
            collections=[self._default_collection, self.tiny_collection],
            languages="en"
        )
        expect(self.sherlock, "sherlock holmes", f)

        # Filters that come from site or library settings.

        # The source for the 'Pride and Prejudice' audiobook has been
        # excluded, so it won't show up in search results.
        f = Filter(
            excluded_audiobook_data_sources=[
                self.pride_audio.license_pools[0].data_source
            ]
        )
        expect([self.pride], "pride and prejudice", f)

        # "Moby Duck" is not currently available, so it won't show up in
        # search results if allow_holds is False.
        f = Filter(allow_holds=False)
        expect([], "moby duck", f)


class TestFacetFilters(EndToEndExternalSearchTest):

    def setup(self):
        super(TestFacetFilters, self).setup()
        _work = self.default_work

        if self.search:

            # A low-quality open-access work.
            self.horse = _work(
                title="Diseases of the Horse", with_open_access_download=True
            )
            self.horse.quality = 0.2

            # A high-quality open-access work.
            self.moby = _work(
                title="Moby Dick", with_open_access_download=True
            )
            self.moby.quality = 0.8

            # A currently available commercially-licensed work.
            self.duck = _work(title='Moby Duck')
            self.duck.license_pools[0].licenses_available = 1
            self.duck.quality = 0.5
            
            # A currently unavailable commercially-licensed work.
            self.becoming = _work(title='Becoming')
            self.becoming.license_pools[0].licenses_available = 0
            self.becoming.quality = 0.9

    def test_facet_filtering(self):

        if not self.search:
            logging.error(
                "Search is not configured, skipping test_facet_filtering."
            )
            return

        # Add all the works created in the setup to the search index.
        SearchIndexCoverageProvider(
            self._db, search_index_client=self.search
        ).run_once_and_update_timestamp()

        # Sleep to give the index time to catch up.
        time.sleep(1)

        def expect(availability, collection, works):
            facets = Facets(
                self._default_library, availability, collection,
                order=Facets.ORDER_TITLE
            )
            self._expect_results(
                works, None, Filter(facets=facets), ordered=False
            )

        # Get all the books in alphabetical order by title.
        expect(Facets.COLLECTION_FULL, Facets.AVAILABLE_ALL, 
               [self.becoming, self.horse, self.moby, self.duck])

        # Show only works that can be borrowed right now.
        expect(Facets.COLLECTION_FULL, Facets.AVAILABLE_NOW,
               [self.horse, self.moby, self.duck])

        # Show only open-access works.
        expect(Facets.COLLECTION_FULL, Facets.AVAILABLE_OPEN_ACCESS, 
               [self.horse, self.moby])

        # Show only featured-quality works.
        expect(Facets.COLLECTION_FEATURED, Facets.AVAILABLE_ALL, 
               [self.becoming, self.moby])

        # Eliminate low-quality open-access works.
        expect(Facets.COLLECTION_MAIN, Facets.AVAILABLE_ALL, 
               [self.becoming, self.moby, self.duck])


class TestSearchOrder(EndToEndExternalSearchTest):

    def setup(self):
        super(TestSearchOrder, self).setup()
        _work = self.default_work

        if self.search:

            # Create two works -- this part is straightforward.
            self.moby_dick = _work(title="Moby Dick", authors="Herman Melville", fiction=True)
            self.moby_dick.presentation_edition.subtitle = "Or, the Whale"
            self.moby_dick.presentation_edition.series = "Classics"
            self.moby_dick.presentation_edition.series_position = 10
            self.moby_dick.summary_text = "Ishmael"
            self.moby_dick.presentation_edition.publisher = "Project Gutenberg"
            self.moby_dick.set_presentation_ready()
            self.moby_dick.random = 0.1
            self.moby_dick.last_update_time = datetime.datetime.now()

            self.moby_duck = _work(title="Moby Duck", authors="Donovan Hohn", fiction=False)
            self.moby_duck.presentation_edition.subtitle = "The True Story of 28,800 Bath Toys Lost at Sea"
            self.moby_duck.summary_text = "A compulsively readable narrative"
            self.moby_duck.presentation_edition.series_position = 1
            self.moby_duck.presentation_edition.publisher = "Penguin"
            self.moby_duck.set_presentation_ready()
            self.moby_duck.random = 0.9
            self.moby_duck.last_update_time = datetime.datetime.now()

            # Each work has one LicensePool associated with the default
            # collection.
            self.collection1 = self._default_collection
            [moby_dick_1] = self.moby_dick.license_pools
            [moby_duck_1] = self.moby_duck.license_pools

            # Since the "Moby-Dick" work was created first, the availability
            # time for its LicensePool is earlier.
            assert moby_dick_1.availability_time < moby_duck_1.availability_time

            # Now we're going to create a second collection with the
            # same two titles, but one big difference: "Moby Duck"
            # showed up earlier here than "Moby-Dick".
            self.collection2 = self._collection()
            moby_duck_2 = self._licensepool(edition=self.moby_duck.presentation_edition, collection=self.collection2)
            self.moby_duck.license_pools.append(moby_duck_2)
            moby_dick_2 = self._licensepool(edition=self.moby_dick.presentation_edition, collection=self.collection2)
            self.moby_dick.license_pools.append(moby_dick_2)

    def test_ordering(self):

        if not self.search:
            logging.error(
                "Search is not configured, skipping test_ordering."
            )
            return

        # Add all the works created in the setup to the search index.
        SearchIndexCoverageProvider(
            self._db, search_index_client=self.search
        ).run_once_and_update_timestamp()

        # Sleep to give the index time to catch up.
        time.sleep(1)

        def assert_order(sort_field, order, **filter_kwargs):
            """Verify that when the books created during test setup are ordered by
            the given `sort_field`, they show up in the given `order`.

            Also verify that when the search is ordered descending,
            the same books show up in the opposite order. This proves
            that `sort_field` isn't being ignored creating a test that
            only succeeds by chance.

            :param sort_field: Sort by this field.
            :param order: A list of books in the expected order.
            :param filter_kwargs: Extra keyword arguments to be passed
               into the `Filter` constructor.
            """
            expect = self._expect_results
            facets = Facets(
                self._default_library, Facets.COLLECTION_FULL,
                Facets.AVAILABLE_ALL, order=sort_field, order_ascending=True
            )
            expect(order, None, Filter(facets=facets, **filter_kwargs))

            facets.order_ascending = False
            expect(list(reversed(order)), None, Filter(facets=facets, **filter_kwargs))

            # Get each item in the list as a separate page. This proves
            # that pagination based on SortKeyPagination works for this
            # sort order.
            facets.order_ascending = True
            to_process = list(order) + [[]]
            results = []
            pagination = SortKeyPagination(size=1)
            while to_process:
                filter = Filter(facets=facets, **filter_kwargs)
                expect_result = to_process.pop(0)
                expect(expect_result, None, filter, pagination=pagination)
                pagination = pagination.next_page
            # We are now off the edge of the list -- we got an empty page
            # of results and there is no next page.
            eq_(None, pagination)

            # Now try the same test in reverse order.
            facets.order_ascending = False
            to_process = list(reversed(order)) + [[]]
            results = []
            pagination = SortKeyPagination(size=1)
            while to_process:
                filter = Filter(facets=facets, **filter_kwargs)
                expect_result = to_process.pop(0)
                expect(expect_result, None, filter, pagination=pagination)
                pagination = pagination.next_page
            # We are now off the edge of the list -- we got an empty page
            # of results and there is no next page.
            eq_(None, pagination)

        # We can sort by title.
        assert_order(Facets.ORDER_TITLE, [self.moby_dick, self.moby_duck])

        # We can sort by author; 'Hohn' sorts before 'Melville'.
        assert_order(Facets.ORDER_AUTHOR, [self.moby_duck, self.moby_dick])

        # We can sort by the value of work.random. 0.1 < 0.9
        assert_order(Facets.ORDER_RANDOM, [self.moby_dick, self.moby_duck])

        # We can sort by the last update time of the Work -- this would
        # be used when creating a crawlable feed.
        assert_order(Facets.ORDER_LAST_UPDATE, [self.moby_dick, self.moby_duck])

        # We can sort by series position. Here, the books aren't in
        # the same series; in a real scenario we would also filter on
        # the value of 'series'.
        assert_order(Facets.ORDER_SERIES_POSITION, [self.moby_duck, self.moby_dick])

        # We can sort by internal work ID, which isn't very useful.
        assert_order(Facets.ORDER_WORK_ID, [self.moby_dick, self.moby_duck])

        # We can sort by the time the Work's LicensePools were first
        # seen -- this would be used when showing patrons 'new' stuff.
        #
        # The LicensePools showed up in different orders in different
        # collections, so filtering by collection will give different
        # results.
        assert_order(
            Facets.ORDER_ADDED_TO_COLLECTION, [self.moby_dick, self.moby_duck],
            collections=[self.collection1]
        )

        assert_order(
            Facets.ORDER_ADDED_TO_COLLECTION, [self.moby_duck, self.moby_dick],
            collections=[self.collection2]
        )

        # If a work shows up with multiple availability times through
        # multiple collections, the earliest availability time for
        # that work is used. Since collection 1 was created before
        # collection 2, that means collection 1's ordering holds here.
        assert_order(
            Facets.ORDER_ADDED_TO_COLLECTION, [self.moby_dick, self.moby_duck],
            collections=[self.collection1, self.collection2]
        )


class TestExactMatches(EndToEndExternalSearchTest):
    """Verify that exact or near-exact title and author matches are
    privileged over matches that span fields.
    """

    def setup(self):
        super(TestExactMatches, self).setup()
        if not self.search:
            return
        _work = self.default_work

        # Here the title is 'Modern Romance'
        self.modern_romance = _work(
            title="Modern Romance",
            authors=["Aziz Ansari", "Eric Klinenberg"],
        )

        # Here 'Modern' is in the subtitle and 'Romance' is the genre.
        self.ya_romance = _work(
            title="Gumby In Love",
            authors="Pokey",
            audience=Classifier.AUDIENCE_YOUNG_ADULT, genre="Romance"
        )
        self.ya_romance.presentation_edition.subtitle = (
            "Modern Fairytale Series, Book 3"
        )

        self.parent_book = _work(
             title="Our Son Aziz",
             authors=["Fatima Ansari", "Shoukath Ansari"],
             genre="Biography & Memoir",
        )

        self.behind_the_scenes = _work(
            title="The Making of Biography With Peter Graves",
            genre="Entertainment",
        )

        self.biography_of_peter_graves = _work(
            "He Is Peter Graves",
            authors="Kelly Ghostwriter",
            genre="Biography & Memoir",
        )

        self.book_by_peter_graves = _work(
            title="My Experience At The University of Minnesota",
            authors="Peter Graves",
            genre="Entertainment",
        )

        self.book_by_someone_else = _work(
            title="The Deadly Graves",
            authors="Peter Ansari",
            genre="Mystery"
        )

        # Add all the works created in the setup to the search index.
        SearchIndexCoverageProvider(
            self._db, search_index_client=self.search
        ).run_once_and_update_timestamp()

        # Sleep to give the index time to catch up.
        time.sleep(2)


    def test_exact_matches(self):
        if not self.search:
            return

        expect = self._expect_results

        # A full title match takes precedence over a match that's
        # split across genre and subtitle.
        expect(
            [
                self.modern_romance, # "modern romance" in title
                self.ya_romance      # "modern" in subtitle, genre "romance"
            ],
            "modern romance"
        )

        # A full author match takes precedence over a partial author
        # match. A partial author match that matches the entire search
        # string takes precedence over a partial author match that doesn't.
        expect(
            [
                self.modern_romance,      # "Aziz Ansari" in author
                self.parent_book,         # "Aziz" in title, "Ansari" in author
                self.book_by_someone_else # "Ansari" in author
            ],
            "aziz ansari"
        )

        # The next two cases have slightly different outcomes in
        # Elasticsearch 1 and Elasticsearch 6, so we're only testing
        # the invariants between versions.

        # 'peter graves' is a string that has exact matches in both
        # title and author.

        # Books with 'Peter Graves' in the title are the top results,
        # ordered by how much other stuff is in the title. An exact
        # author match is next.  A partial match split across fields
        # ("peter" in author, "graves" in title) is the last result.
        order = [
            self.biography_of_peter_graves,
            self.behind_the_scenes,
            self.book_by_peter_graves,
            self.book_by_someone_else,
        ]
        expect(order, "peter graves")

        if MAJOR_VERSION == 1:
            # In Elasticsearch 1, 'The Making of Biography With Peter
            # Graves' does worse in a search for 'peter graves
            # biography' than a biography whose title includes the
            # phrase 'peter graves'. Although the title contains all
            # three search terms, it's not an exact token match. But
            # "The Making of..." still does better than books that
            # match 'peter graves' (or 'peter' and 'graves'), but not
            # 'biography'.
            order = [
                self.biography_of_peter_graves, # title + genre 'biography'
                self.behind_the_scenes,         # all words match in title
                self.book_by_peter_graves,      # author (no 'biography')
                self.book_by_someone_else,      # match across fields (no 'biography')
            ]
        else:
            # In Elasticsearch 6, the exact author match that doesn't
            # mention 'biography' is boosted above a book that
            # mentions all three words in its title.
            order = [
                self.biography_of_peter_graves, # title + genre 'biography'
                self.book_by_peter_graves,      # author (no 'biography')
                self.behind_the_scenes,         # all words match in title
                self.book_by_someone_else,      # match across fields (no 'biography')
            ]

        expect(order, "peter graves biography")


class TestSearchBase(object):

    def test__match_range(self):
        # Test the _match_range helper method.
        # This is used to create an Elasticsearch query term
        # that only matches if a value is in a given range.

        # This only matches if field.name has a value >= 5.
        r = SearchBase._match_range("field.name", "gte", 5)
        eq_(r, {'range': {'field.name': {'gte': 5}}})


class TestQuery(DatabaseTest):

    def test_constructor(self):
        # Verify that the Query constructor sets members with
        # no processing.
        query = Query("query string", "filter")
        eq_("query string", query.query_string)
        eq_("filter", query.filter)

    def test_build(self):
        # Verify that the build() method combines the 'query' part of
        # a Query and the 'filter' part to create a single
        # Elasticsearch Search object, complete with (if necessary)
        # subqueries and sort ordering.

        class MockSearch(object):
            """A mock of the Elasticsearch-DSL `Search` object.

            Calls to Search methods tend to create a new Search object
            based on the old one. This mock simulates that behavior.
            If necessary, you can look at all MockSearch objects
            created by to get to a certain point by following the
            .parent relation.
            """
            def __init__(
                    self, parent=None, query=None, nested_filter_calls=None,
                    order=None
            ):
                self.parent = parent
                self._query = query
                self.nested_filter_calls = nested_filter_calls or []
                self.order = order

            def filter(self, **kwargs):
                """Simulate the application of a nested filter.

                :return: A new MockSearch object.
                """
                new_filters = self.nested_filter_calls + [kwargs]
                return MockSearch(self, self._query, new_filters, self.order)

            def query(self, query):
                """Simulate the creation of an Elasticsearch-DSL `Search`
                object from an Elasticsearch-DSL `Query` object.

                :return: A New MockSearch object.
                """
                return MockSearch(
                    self, query, self.nested_filter_calls, self.order
                )

            def sort(self, *order_fields):
                """Simulate the application of a sort order."""
                return MockSearch(
                    self, self._query, self.nested_filter_calls, order_fields
                )

        class MockQuery(Query):
            # A Mock of the Query object from external_search
            # (not the one from Elasticsearch-DSL).
            def query(self):
                return Q("simple_query_string", query=self.query_string)

        class MockPagination(object):
            def modify_search_query(self, search):
                return search.filter(name_or_query="pagination modified")

        # Test the simple case where the Query has no filter.
        qu = MockQuery("query string", filter=None)
        search = MockSearch()
        pagination = MockPagination()
        built = qu.build(search, pagination)

        # The return value is a new MockSearch object based on the one
        # that was passed in.
        assert isinstance(built, MockSearch)
        eq_(search, built.parent.parent.parent)

        # The result of Query.query() is used as-is as the basis for
        # the Search object.
        eq_(qu.query(), built._query)

        # The modification introduced by the MockPagination was the
        # last filter applied.
        pagination_filter = built.nested_filter_calls.pop()
        eq_("pagination modified", pagination_filter['name_or_query'])

        # A nested filter is always applied, to filter out
        # LicensePools that were once part of a collection but
        # currently have no owned licenses.
        open_access = dict(term={'licensepools.open_access': True})
        def assert_ownership_filter(built):
            # Extract the call that created the ownership filter
            # and verify its structure.
            unowned_filter = built.nested_filter_calls.pop()

            # It's a nested filter...
            eq_('nested', unowned_filter['name_or_query'])

            # ...applied to the 'licensepools' subdocument.
            eq_('licensepools', unowned_filter['path'])

            # For a license pool to be counted, it either must be open
            # access or the collection must currently own licenses for
            # it.
            owned = dict(term={'licensepools.owned': True})
            expect = {'bool': {'filter': [{'bool': {'should': [owned, open_access]}}]}}
            eq_(expect, unowned_filter['query'].to_dict())

        # If there's a filter, a boolean Query object is created to
        # combine the original Query with the filter.
        filter = Filter(fiction=True)
        qu = MockQuery("query string", filter=filter)
        built = qu.build(search)

        # The 'must' part of this new Query came from calling
        # Query.query() on the original Query object.
        #
        # The 'filter' part came from calling Filter.build() on the
        # main filter.
        underlying_query = built._query
        main_filter, nested_filters = filter.build()
        eq_(underlying_query.must, [qu.query()])
        eq_(underlying_query.filter, [main_filter])

        # There are no nested filters (apart from the ownership
        # filter), and filter() was never called on the mock Search object.
        assert_ownership_filter(built)
        eq_({}, nested_filters)
        eq_([], built.nested_filter_calls)

        # Now let's try a combination of regular filters and nested filters.
        filter = Filter(
            fiction=True,
            collections=[self._default_collection]
        )
        qu = MockQuery("query string", filter=filter)
        built = qu.build(search)
        underlying_query = built._query

        # We get a main filter (for the fiction restriction) and two
        # nested filters (one for the collection restriction, one for
        # the ownership restriction).
        main_filter, nested_filters = filter.build()
        assert_ownership_filter(built)
        [nested_licensepool_filter] = nested_filters.pop('licensepools')
        eq_({}, nested_filters)

        # As before, the main filter has been applied to the underlying
        # query.
        eq_(underlying_query.filter, [main_filter])

        # The nested filter was converted into a Bool query and passed
        # into Search.filter(). This applied an additional filter on the
        # 'licensepools' subdocument.
        [filter_call] = built.nested_filter_calls
        eq_('nested', filter_call['name_or_query'])
        eq_('licensepools', filter_call['path'])
        filter_as_query = filter_call['query']
        eq_(Bool(filter=nested_licensepool_filter), filter_as_query)

        # Now we're going to test how queries are built to accommodate
        # various restrictions imposed by a Facets object.
        def from_facets(*args, **kwargs):
            """Build a Query object from a set of facets, then call
            build() on it.
            """
            facets = Facets(self._default_library, *args, **kwargs)
            filter = Filter(facets=facets)
            qu = MockQuery("query string", filter=filter)
            built = qu.build(search)

            # Verify and remove the ownership filter.
            assert_ownership_filter(built)

            # Return the rest to be verified in a test-specific way.
            return built

        # When using the 'main' collection...
        built = from_facets(Facets.COLLECTION_MAIN, None, None)

        # An additional nested filter is applied.
        [exclude_lq_open_access] = built.nested_filter_calls
        eq_('nested', exclude_lq_open_access['name_or_query'])
        eq_('licensepools', exclude_lq_open_access['path'])

        # It excludes open-access books known to be of low quality.
        nested_filter = exclude_lq_open_access['query']
        not_open_access = {'term': {'licensepools.open_access': False}}
        decent_quality = Filter._match_range('licensepools.quality', 'gte', 0.3)
        eq_(
            nested_filter.to_dict(),
            {'bool': {'filter': [{'bool': {'should': [not_open_access, decent_quality]}}]}}
        )

        # When using the 'featured' collection...
        built = from_facets(Facets.COLLECTION_FEATURED, None, None)

        # There is no nested filter.
        eq_([], built.nested_filter_calls)

        # A non-nested filter is applied on the 'quality' field.
        [quality_filter] = built._query.filter
        expect = Filter._match_range('quality', 'gte', self._default_library.minimum_featured_quality)
        eq_(expect, quality_filter.to_dict())

        # When using the AVAILABLE_OPEN_ACCESS availability restriction...
        built = from_facets(Facets.COLLECTION_FULL,
                            Facets.AVAILABLE_OPEN_ACCESS, None)

        # An additional nested filter is applied.
        [available_now] = built.nested_filter_calls
        eq_('nested', available_now['name_or_query'])
        eq_('licensepools', available_now['path'])

        # It finds only license pools that are open access.
        nested_filter = available_now['query']
        eq_(
            nested_filter.to_dict(),
            {'bool': {'filter': [open_access]}}
        )

        # When using the AVAILABLE_NOW restriction...
        built = from_facets(Facets.COLLECTION_FULL, Facets.AVAILABLE_NOW, None)

        # An additional nested filter is applied.
        [available_now] = built.nested_filter_calls
        eq_('nested', available_now['name_or_query'])
        eq_('licensepools', available_now['path'])

        # It finds only license pools that are open access *or* that have
        # active licenses.
        nested_filter = available_now['query']
        available = {'term': {'licensepools.available': True}}
        eq_(
            nested_filter.to_dict(),
            {'bool': {'filter': [{'bool': {'should': [open_access, available]}}]}}
        )

        # If the Filter specifies a sort order, Filter.sort_order is
        # used to convert it to appropriate Elasticsearch syntax, and
        # the MockSearch object is modified appropriately.
        built = from_facets(
            None, None, order=Facets.ORDER_RANDOM, order_ascending=False
        )

        # We asked for a random sort order, and that's the primary
        # sort field.
        order = list(built.order)
        eq_(dict(random="desc"), order.pop(0))

        # But a number of other sort fields are also employed to act
        # as tiebreakers.
        for tiebreaker_field in ('sort_author', 'sort_title', 'work_id'):
            eq_({tiebreaker_field: "asc"}, order.pop(0))
        eq_([], order)

    def test_query(self):
        # The query() method calls a number of other methods
        # to generate hypotheses, then creates a dis_max query
        # to find the most likely hypothesis for any given book.

        class Mock(Query):

            _match_phrase_called_with = []
            _boosts = {}
            _kwargs = {}

            def simple_query_string_query(self, query_string):
                self.simple_query_string_called_with = query_string
                return "simple"

            def minimal_stemming_query(
                    self, query_string, fields="default fields"
            ):
                self.minimal_stemming_called_with = (query_string, fields)
                return "minimal stemming"

            def _match_phrase(self, field, query_string):
                self._match_phrase_called_with.append((field, query_string))
                return "%s match phrase" % field

            def fuzzy_string_query(self, query_string):
                self.fuzzy_string_called_with = query_string
                return "fuzzy string"

            def _parsed_query_matches(self, query_string):
                self._parsed_query_matches_called_with = query_string
                return "parsed query matches"

            def _hypothesize(
                    self, hypotheses, new_hypothesis, boost="default",
                    **kwargs
            ):
                self._boosts[new_hypothesis] = boost
                self._kwargs[new_hypothesis] = kwargs
                hypotheses.append(new_hypothesis)
                return hypotheses

            def _combine_hypotheses(self, hypotheses):
                self._combine_hypotheses_called_with = hypotheses
                return hypotheses

        # Before we get started, try an easy case. If there is no query
        # string we get a match_all query that returns everything.
        query = Mock(None)
        result = query.query()
        eq_(dict(match_all=dict()), result.to_dict())

        # Now try a real query string.
        q = "query string"
        query = Mock(q)
        result = query.query()

        # The final result is the result of calling _combine_hypotheses
        # on a number of hypotheses. Our mock class just returns
        # the hypotheses as-is, for easier testing.
        eq_(result, query._combine_hypotheses_called_with)

        # We ended up with five hypotheses. The mock methods were called
        # once, except for _match_phrase, which was called once for title
        # and once for author.
        eq_(['simple', 'minimal stemming',
             'title.standard match phrase', 'author match phrase',
             'fuzzy string', 'parsed query matches'], result)

        # For the parsed query matches, the extra all_must_match=True
        # argument was passed into _boost() -- the boost only applies
        # if every element of the parsed query is a hit.
        eq_({'all_must_match': True},
            query._kwargs['parsed query matches'])

        # In each case, the original query string was used as the
        # input into the mocked method.
        eq_(q, query.simple_query_string_called_with)
        eq_((q, "default fields"),
            query.minimal_stemming_called_with)
        eq_([('title.standard', q), ('author', q)],
            query._match_phrase_called_with)
        eq_(q, query.fuzzy_string_called_with)
        eq_(q, query._parsed_query_matches_called_with)

        # Each call to _hypothesize included a boost factor indicating
        # how heavily to weight that hypothesis. Rather than do anything
        # with this information, we just stored it in _boosts.

        # Exact title or author matches are valued quite highly.
        eq_(200, query._boosts['title.standard match phrase'])
        eq_(50, query._boosts['author match phrase'])

        # A near-exact match is also valued highly.
        eq_(100, query._boosts['minimal stemming'])

        # The default query match has the default boost.
        eq_("default", query._boosts['simple'])

        # The fuzzy match has a boost that's very low, to encourage any
        # matches that are better to show up first.
        eq_(1, query._boosts['fuzzy string'])

        eq_(200, query._boosts['parsed query matches'])

    def test__hypothesize(self):
        # Verify that _hypothesize() adds a query to a list,
        # boosting it if necessary.
        class Mock(Query):
            @classmethod
            def _boost(cls, boost, query):
                return "%s boosted by %d" % (query, boost)

        hypotheses = []

        # _hypothesize() does nothing if it's not passed a real
        # query.
        Mock._hypothesize(hypotheses, None, 100)
        eq_([], hypotheses)

        # If it is passed a real query, _boost() is called on the
        # query object.
        Mock._hypothesize(hypotheses, "query object", 10)
        eq_(["query object boosted by 10"], hypotheses)

        Mock._hypothesize(hypotheses, "another query object", 1)
        eq_(["query object boosted by 10", "another query object boosted by 1"],
            hypotheses)


    def test__combine_hypotheses(self):
        # Verify that _combine_hypotheses creates a DisMax query object
        # that chooses the best one out of whichever queries it was passed.
        h1 = Q("simple_query_string", query="query 1")
        h2 = Q("simple_query_string", query="query 2")
        hypotheses = [h1, h2]
        combined = Query._combine_hypotheses(hypotheses)
        eq_("dis_max", combined.name)
        eq_(hypotheses, combined.queries)

    def test__boost(self):
        # Verify that _boost() converts a regular query (or list of queries)
        # into a boosted query.
        q1 = Q("simple_query_string", query="query 1")
        q2 = Q("simple_query_string", query="query 2")

        boosted_one = Query._boost(10, q1)
        eq_("bool", boosted_one.name)
        eq_(10.0, boosted_one.boost)
        eq_([q1], boosted_one.must)

        # By default, if you pass in multiple queries, only one of them
        # must match for the boost to apply.
        boosted_multiple = Query._boost(4.5, [q1, q2])
        eq_("bool", boosted_multiple.name)
        eq_(4.5, boosted_multiple.boost)
        eq_(1, boosted_multiple.minimum_should_match)
        eq_([q1, q2], boosted_multiple.should)

        # Here, every query must match for the boost to apply.
        boosted_multiple = Query._boost(4.5, [q1, q2], all_must_match=True)
        eq_("bool", boosted_multiple.name)
        eq_(4.5, boosted_multiple.boost)
        eq_([q1, q2], boosted_multiple.must)

        # A Bool query has its boost changed but is otherwise left alone.
        bool = Q("bool", boost=10)
        boosted_bool = Query._boost(1, bool)
        eq_(Q("bool", boost=1), boosted_bool)

        # Some other kind of query that's being given a "boost" of 1
        # is left alone.
        eq_(q1, Query._boost(1, q1))

    def test_simple_query_string_query(self):
        # Verify that simple_query_string_query() returns a
        # SimpleQueryString Elasticsearch object.
        qu = Query.simple_query_string_query("hello")
        eq_("simple_query_string", qu.name)
        eq_(Query.SIMPLE_QUERY_STRING_FIELDS, qu.fields)
        eq_("hello", qu.query)

        # It's possible to use your own set of fields instead of
        # the defaults.
        custom_fields = ['field1', 'field2']
        qu = Query.simple_query_string_query("hello", custom_fields)
        eq_(custom_fields, qu.fields)
        eq_("hello", qu.query)

    def test_fuzzy_string_query(self):
        # fuzzy_string_query() returns a MultiMatch Elasticsearch
        # object, unless the query string looks like a fuzzy search
        # will do poorly on it -- then it returns None.

        qu = Query.fuzzy_string_query("hello")
        eq_("multi_match", qu.name)
        eq_(Query.FUZZY_QUERY_STRING_FIELDS, qu.fields)
        eq_("best_fields", qu.type)
        eq_("AUTO", qu.fuzziness)
        eq_("hello", qu.query)
        eq_(1, qu.prefix_length)

        # This query string contains a string that is known to mess
        # with fuzzy searches.
        qu = Query.fuzzy_string_query("tennis players")

        # fuzzy_string_query does nothing, to avoid bad results.
        eq_(None, qu)

    def test__match(self):
        # match creates a Match Elasticsearch object which does a
        # match against a specific field.
        qu = Query._match("author", "flannery o'connor")
        eq_(
            {'match': {'author': "flannery o'connor"}},
            qu.to_dict()
        )

    def test__match_phrase(self):
        # match_phrase creates a MatchPhrase Elasticsearch
        # object which does a phrase match against a specific field.
        qu = Query._match_phrase("author", "flannery o'connor")
        eq_(
            {'match_phrase': {'author': "flannery o'connor"}},
            qu.to_dict()
        )

    def test_minimal_stemming_query(self):
        class Mock(Query):
            @classmethod
            def _match_phrase(cls, field, query_string):
                return "%s=%s" % (field, query_string)

        m = Mock.minimal_stemming_query

        # No fields, no queries.
        eq_([], m("query string", []))

        # If you pass in any fields, you get a _match_phrase
        # query for each one.
        results = m("query string", ["field1", "field2"])
        eq_(
            ["field1=query string", "field2=query string"],
            results
        )

        # The default fields are MINIMAL_STEMMING_QUERY_FIELDS.
        results = m("query string")
        eq_(
            ["%s=query string" % field
             for field in Mock.MINIMAL_STEMMING_QUERY_FIELDS],
            results
        )

    def test_make_target_age_query(self):

        # Search for material suitable for children between the
        # ages of 5 and 10.
        qu = Query.make_target_age_query((5,10), boost=50.1)

        # We get a boosted boolean query.
        eq_("bool", qu.name)
        eq_(50.1, qu.boost)

        # To match the query, the material's target age must overlap
        # the 5-10 age range.
        five_year_olds_not_too_old, ten_year_olds_not_too_young = qu.must
        eq_(
            {'range': {'target_age.upper': {'gte': 5}}},
            five_year_olds_not_too_old.to_dict()
        )
        eq_(
            {'range': {'target_age.lower': {'lte': 10}}},
            ten_year_olds_not_too_young.to_dict()
        )

        # To get the full boost, the target age must fit entirely within
        # the 5-10 age range. If a book would also work for older or younger
        # kids who aren't in this age range, it's not as good a match.
        would_work_for_older_kids, would_work_for_younger_kids = qu.should
        eq_(
            {'range': {'target_age.upper': {'lte': 10}}},
            would_work_for_older_kids.to_dict()
        )
        eq_(
            {'range': {'target_age.lower': {'gte': 5}}},
            would_work_for_younger_kids.to_dict()
        )

        # The default boost is 1.
        qu = Query.make_target_age_query((5,10))
        eq_(1, qu.boost)

    def test__parsed_query_matches(self):
        # _parsed_query_matches creates a QueryParser from
        # the query string and returns whatever it comes up with.
        #
        # This is a basic test to verify that a QueryParser
        # is in use. The QueryParser is tested in much greater detail
        # in TestQueryParser.

        [must_match] = Query._parsed_query_matches("nonfiction")
        eq_({'match': {'fiction': 'Nonfiction'}}, must_match.to_dict())


class TestQueryParser(DatabaseTest):
    """Test the class that tries to derive structure from freeform
    text search requests.
    """

    def test_constructor(self):
        # The constructor parses the query string, creates any
        # necessary query objects, and turns the remaining part of
        # the query into a 'simple query string'-type query.

        class MockQuery(object):
            """Create 'query' objects that are easier to test than
            the ones the Query class makes.
            """
            @classmethod
            def simple_query_string_query(cls, query_string, fields):
                return (query_string, fields)

            @classmethod
            def _match(cls, field, query):
                return (field, query)

            @classmethod
            def make_target_age_query(cls, query, boost):
                return (query, boost)

        parser = QueryParser("science fiction about dogs", MockQuery)

        # The original query string is always stored as .original_query_string.
        eq_("science fiction about dogs", parser.original_query_string)

        # The part of the query that couldn't be parsed is always stored
        # as final_query_string.
        eq_("about dogs", parser.final_query_string)

        # Leading and trailing whitespace is never regarded as
        # significant and it is stripped from the query string
        # immediately.
        whitespace = QueryParser(" abc ", MockQuery)
        eq_("abc", whitespace.original_query_string)

        # The query string becomes a series of Query objects
        # (simulated here by the tuples returned by the MockQuery
        # methods).
        #
        # parser.match_queries contains some number of field-match
        # queries, and then one final query that runs the unparseable
        # portion of the query through a simple multi-field match.
        query_string_fields = QueryParser.SIMPLE_QUERY_STRING_FIELDS
        eq_(
            [('genres.name', 'Science Fiction'),
             ('about dogs', query_string_fields)],
            parser.match_queries
        )

        # Now that you see how it works, let's define a helper
        # function that will let us easily verify that a certain
        # query string becomes a certain set of field matches plus a
        # certain string left over.
        def assert_parses_as(query_string, *matches):
            matches = list(matches)
            parser = QueryParser(query_string, MockQuery)
            remainder = matches.pop(-1)
            if remainder:
                remainder_match = MockQuery.simple_query_string_query(
                    remainder, query_string_fields
                )
                matches.append(remainder_match)
            eq_(matches, parser.match_queries)
            eq_(query_string, parser.original_query_string)
            eq_(remainder, parser.final_query_string)

        # Here's the same test from before, using the new
        # helper function.
        assert_parses_as(
            "science fiction about dogs",
            ("genres.name", "Science Fiction"),
            "about dogs"
        )

        # Test audiences.

        assert_parses_as(
            "children's picture books",
            ("audience", "Children"),
            "picture books"
        )

        # (It's possible for the entire query string to be eaten up,
        # such that there is no remainder match at all.)
        assert_parses_as(
            "young adult romance",
            ("genres.name", "Romance"),
            ("audience", "YoungAdult"),
            ''
        )

        # Test fiction/nonfiction status.
        assert_parses_as(
            "fiction dinosaurs",
            ("fiction", "Fiction"),
            "dinosaurs"
        )

        # (Genres are parsed before fiction/nonfiction; otherwise
        # "science fiction" would be chomped by a search for "fiction"
        # and "nonfiction" would not be picked up.)
        assert_parses_as(
            "science fiction or nonfiction dinosaurs",
            ("genres.name", "Science Fiction"), ("fiction", "Nonfiction"),
            "or  dinosaurs"
        )

        # Test target age.

        assert_parses_as(
            "grade 5 science",
            ("genres.name", "Science"), ((10, 10), 40),
            ''
        )

        assert_parses_as(
            'divorce ages 10 and up',
            ((10, 14), 40),
            'divorce  and up' # TODO: not ideal
        )

        # Nothing can be parsed out from this query--it's an author's name
        # and will be handled by another query.
        parser = QueryParser("octavia butler")
        eq_([], parser.match_queries)
        eq_("octavia butler", parser.final_query_string)

        # Finally, try parsing a query without using MockQuery.
        query = QueryParser("nonfiction asteroids")
        nonfiction, asteroids = query.match_queries

        # It creates real Elasticsearch-DSL query objects.
        eq_({'match': {'fiction': 'Nonfiction'}}, nonfiction.to_dict())

        eq_({'simple_query_string':
             {'query': 'asteroids',
              'fields': QueryParser.SIMPLE_QUERY_STRING_FIELDS }
            },
            asteroids.to_dict()
        )

    def test_add_match_query(self):
        # TODO: this method could use a standalone test, but it's
        # already covered by the test_constructor.
        pass

    def test_add_target_age_query(self):
        # TODO: this method could use a standalone test, but it's
        # already covered by the test_constructor.
        pass

    def test__without_match(self):
        # Test our ability to remove matched text from a string.
        m = QueryParser._without_match
        eq_(" fiction", m("young adult fiction", "young adult"))
        eq_(" of dinosaurs", m("science of dinosaurs", "science"))

        # If the match cuts off in the middle of a word, we remove
        # everything up to the end of the word.
        eq_(" books", m("children's books", "children"))
        eq_("", m("adulting", "adult"))


class TestFilter(DatabaseTest):

    def setup(self):
        super(TestFilter, self).setup()

        # Look up three Genre objects which can be used to make filters.
        self.literary_fiction, ignore = Genre.lookup(
            self._db, "Literary Fiction"
        )
        self.fantasy, ignore = Genre.lookup(self._db, "Fantasy")
        self.horror, ignore = Genre.lookup(self._db, "Horror")

        # Create two empty CustomLists which can be used to make filters.
        self.best_sellers, ignore = self._customlist(num_entries=0)
        self.staff_picks, ignore = self._customlist(num_entries=0)

    def test_constructor(self):
        # Verify that the Filter constructor sets members with
        # minimal processing.
        collection = self._default_collection

        media = object()
        languages = object()
        fiction = object()
        audiences = object()

        # Test the easy stuff -- these arguments just get stored on the
        # filter object. They'll be cleaned up later, during build().
        filter = Filter(
            media=media, languages=languages,
            fiction=fiction, audiences=audiences
        )
        eq_(media, filter.media)
        eq_(languages, filter.languages)
        eq_(fiction, filter.fiction)
        eq_(audiences, filter.audiences)

        # Test the `collections` argument.

        # If you pass in a library, you get all of its collections.
        library_filter = Filter(collections=self._default_library)
        eq_([self._default_collection.id], library_filter.collection_ids)

        # If the library has no collections, the collection filter
        # will filter everything out.
        self._default_library.collections = []
        library_filter = Filter(collections=self._default_library)
        eq_([], library_filter.collection_ids)

        # If you pass in Collection objects, you get their IDs.
        collection_filter = Filter(collections=self._default_collection)
        eq_([self._default_collection.id], collection_filter.collection_ids)
        collection_filter = Filter(collections=[self._default_collection])
        eq_([self._default_collection.id], collection_filter.collection_ids)

        # If you pass in IDs, they're left alone.
        ids = [10, 11, 22]
        collection_filter = Filter(collections=ids)
        eq_(ids, collection_filter.collection_ids)

        # If you pass in nothing, there is no collection filter. This
        # is different from the case above, where the library had no
        # collections and everything was filtered out.
        empty_filter = Filter()
        eq_(None, empty_filter.collection_ids)

        # Test the `target_age` argument.
        eq_(None, empty_filter.target_age)

        one_year = Filter(target_age=8)
        eq_((8,8), one_year.target_age)

        year_range = Filter(target_age=(8,10))
        eq_((8,10), year_range.target_age)

        year_range = Filter(target_age=NumericRange(3, 6, '()'))
        eq_((4, 5), year_range.target_age)

        # Test genre_restriction_sets

        # In these three cases, there are no restrictions on genre.
        eq_([], empty_filter.genre_restriction_sets)
        eq_([], Filter(genre_restriction_sets=[]).genre_restriction_sets)
        eq_([], Filter(genre_restriction_sets=None).genre_restriction_sets)

        # Restrict to books that are literary fiction AND (horror OR
        # fantasy).
        restricted = Filter(
            genre_restriction_sets = [
                [self.horror, self.fantasy],
                [self.literary_fiction],
            ]
        )
        eq_(
            [[self.horror.id, self.fantasy.id],
             [self.literary_fiction.id]],
            restricted.genre_restriction_sets
        )

        # This is a restriction: 'only books that have no genre'
        eq_([[]], Filter(genre_restriction_sets=[[]]).genre_restriction_sets)

        # Test customlist_restriction_sets

        # In these three cases, there are no restrictions.
        eq_([], empty_filter.customlist_restriction_sets)
        eq_([], Filter(customlist_restriction_sets=None).customlist_restriction_sets)
        eq_([], Filter(customlist_restriction_sets=[]).customlist_restriction_sets)

        # Restrict to books that are on *both* the best sellers list and the
        # staff picks list.
        restricted = Filter(
            customlist_restriction_sets = [
                [self.best_sellers],
                [self.staff_picks],
            ]
        )
        eq_(
            [[self.best_sellers.id],
             [self.staff_picks.id]],
            restricted.customlist_restriction_sets
        )

        # This is a restriction -- 'only books that are not on any lists'.
        eq_(
            [[]],
            Filter(customlist_restriction_sets=[[]]).customlist_restriction_sets
        )

        # If you pass in a Facets object, its modify_search_filter()
        # is called.
        class Mock(object):
            def modify_search_filter(self, filter):
                self.called_with = filter

        facets = Mock()
        filter = Filter(facets=facets)
        eq_(filter, facets.called_with)

    def test_from_worklist(self):
        # Any WorkList can be converted into a Filter.
        #
        # WorkList.inherited_value() and WorkList.inherited_values()
        # are used to determine what should go into the constructor.

        parent = self._lane(
            display_name="Parent Lane", library=self._default_library
        )
        parent.media = Edition.AUDIO_MEDIUM
        parent.languages = ["eng", "fra"]
        parent.fiction = True
        parent.audiences = [Classifier.AUDIENCE_CHILDREN]
        parent.target_age = NumericRange(10, 11, '[]')
        parent.genres = [self.horror, self.fantasy]
        parent.customlists = [self.best_sellers]

        # This lane inherits most of its configuration from its parent.
        inherits = self._lane(
            display_name="Child who inherits", parent=parent
        )
        inherits.genres = [self.literary_fiction]
        inherits.customlists = [self.staff_picks]

        class Mock(object):
            def modify_search_filter(self, filter):
                self.called_with = filter
        facets = Mock()

        filter = Filter.from_worklist(self._db, inherits, facets)
        eq_([self._default_collection.id], filter.collection_ids)
        eq_(parent.media, filter.media)
        eq_(parent.languages, filter.languages)
        eq_(parent.fiction, filter.fiction)
        eq_(parent.audiences, filter.audiences)
        eq_((parent.target_age.lower, parent.target_age.upper),
            filter.target_age)

        # Filter.from_worklist passed the mock Facets object in to
        # the Filter constructor, which called its modify_search_filter()
        # method.
        assert facets.called_with is not None

        # For genre and custom list restrictions, the child values are
        # appended to the parent's rather than replacing it.
        eq_([parent.genre_ids, inherits.genre_ids],
            [set(x) for x in filter.genre_restriction_sets]
        )

        eq_([parent.customlist_ids, inherits.customlist_ids],
            filter.customlist_restriction_sets
        )

        # If any other value is set on the child lane, the parent value
        # is overridden.
        inherits.media = Edition.BOOK_MEDIUM
        filter = Filter.from_worklist(self._db, inherits, facets)
        eq_(inherits.media, filter.media)

        # This lane doesn't inherit anything from its parent.
        does_not_inherit = self._lane(
            display_name="Child who does not inherit", parent=parent
        )
        does_not_inherit.inherit_parent_restrictions = False

        # Because of that, the final filter we end up with is
        # nearly empty. The only restriction here is the collection
        # restriction imposed by the fact that `does_not_inherit`
        # is, itself, associated with a specific library.
        filter = Filter.from_worklist(self._db, does_not_inherit, facets)

        built_filter, subfilters = filter.build()

        # The collection restriction is not reflected in the main
        # filter; rather it's in a subfilter that will be applied to the
        # 'licensepools' subdocument, where the collection ID lives.
        eq_(None, built_filter)
        [subfilter] = subfilters.pop('licensepools')
        eq_({'terms': {'licensepools.collection_id': [self._default_collection.id]}},
            subfilter.to_dict())

        # No other subfilters were specified.
        eq_({}, subfilters)

    def test_build(self):
        # Test the ability to turn a Filter into an ElasticSearch
        # filter object.

        # build() takes the information in the Filter object, scrubs
        # it, and uses _chain_filters to chain together a number of
        # alternate hypotheses. It returns a 2-tuple with a main Filter
        # and a dictionary describing additional filters to be applied
        # to subdocuments.
        #
        # Let's try it with some simple cases before mocking
        # _chain_filters for a more detailed test.

        def assert_filter(expect, filter, _chain_filters=None):
            """Helper method for the most common case, where a
            Filter.build() returns a main filter and no nested filters.
            """
            main, nested = filter.build(_chain_filters)
            eq_(expect, main.to_dict())
            eq_({}, nested)

        # Start with an empty filter. No filter is built and there are no
        # nested filters.
        filter = Filter()
        eq_((None, {}), filter.build())

        # Add a medium clause to the filter.
        filter.media = "a medium"
        medium_built = {'terms': {'medium': ['amedium']}}
        assert_filter(medium_built, filter)

        # Add a language clause to the filter.
        filter.languages = ["lang1", "LANG2"]
        language_built = {'terms': {'language': ['lang1', 'lang2']}}

        # Now both the medium clause and the language clause must match.
        assert_filter(
            {'bool': {'must': [medium_built, language_built]}},
            filter
        )

        # Now let's mock _chain_filters so we don't have to check
        # our test results against super-complicated Elasticsearch
        # filter objects.
        #
        # Instead, we'll get a list of smaller filter objects.
        def chain(filters, new_filter):
            if filters is None:
                # This is the first filter:
                filters = []
            filters.append(new_filter)
            return filters

        filter.collection_ids = [self._default_collection]
        filter.fiction = True
        filter.audiences = 'CHILDREN'
        filter.target_age = (2,3)

        # We want books that are literary fiction, *and* either
        # fantasy or horror.
        filter.genre_restriction_sets = [
            [self.literary_fiction], [self.fantasy, self.horror]
        ]

        # We want books that are on _both_ of the custom lists.
        filter.customlist_restriction_sets = [
            [self.best_sellers], [self.staff_picks]
        ]

        # At this point every item on this Filter that can be set, has been
        # set. When we run build, we'll end up with the output of our mocked
        # chain() method -- a list of small filters.
        built, nested = filter.build(_chain_filters=chain)

        # This time we do see a nested
        # filter. licensepools.collection_id is in the nested
        # licensepools document, so the 'current collection'
        # restriction must be described in terms of a nested filter on
        # that document.
        [licensepool_filter] = nested.pop('licensepools')
        eq_(
            {'terms': {'licensepools.collection_id': [self._default_collection.id]}},
            licensepool_filter.to_dict()
        )

        # There are no other nested filters.
        eq_({}, nested)

        # Every other restriction imposed on the Filter object becomes an
        # Elasticsearch filter object in this list.
        (medium, language, fiction, audience, target_age,
         literary_fiction_filter, fantasy_or_horror_filter,
         best_sellers_filter, staff_picks_filter) = built

        # Test them one at a time.
        #
        # Throughout this test, notice that the data model objects --
        # Collections (above), Genres, and CustomLists -- have been
        # replaced with their database IDs. This is done by
        # filter_ids.
        #
        # Also, audience, medium, and language have been run through
        # scrub_list, which turns scalar values into lists, removes
        # spaces, and converts to lowercase.

        # These we tested earlier -- we're just making sure the same
        # documents are put into the full filter.
        eq_(medium_built, medium.to_dict())
        eq_(language_built, language.to_dict())

        eq_({'term': {'fiction': 'fiction'}}, fiction.to_dict())
        eq_({'terms': {'audience': ['children']}}, audience.to_dict())

        # The contents of target_age_filter are tested below -- this
        # just tests that the target_age_filter is included.
        eq_(filter.target_age_filter, target_age)

        # There are two different restrictions on genre, because
        # genre_restriction_sets was set to two lists of genres.
        eq_({'terms': {'genres.term': [self.literary_fiction.id]}},
            literary_fiction_filter.to_dict())
        eq_({'terms': {'genres.term': [self.fantasy.id, self.horror.id]}},
            fantasy_or_horror_filter.to_dict())

        # Similarly, there are two different restrictions on custom
        # list membership.
        eq_({'terms': {'customlists.list_id': [self.best_sellers.id]}},
            best_sellers_filter.to_dict())
        eq_({'terms': {'customlists.list_id': [self.staff_picks.id]}},
            staff_picks_filter.to_dict())

        # We tried fiction; now try nonfiction.
        filter = Filter()
        filter.fiction = False
        assert_filter({'term': {'fiction': 'nonfiction'}}, filter)

    def test_sort_order(self):
        # Test the Filter.sort_order property.

        # No sort order.
        f = Filter()
        eq_([], f.sort_order)
        eq_(False, f.order_ascending)

        def validate_sort_order(filter, main_field):
            """Validate the 'easy' part of the sort order -- the tiebreaker
            fields. Return the 'difficult' part.

            :return: The first part of the sort order -- the field that
            is potentially difficult.
            """

            # The tiebreaker fields are always in the same order, but
            # if the main sort field is one of the tiebreaker fields,
            # it's removed from the list -- there's no need to sort on
            # that field a second time.
            default_sort_fields = [
                {x: "asc"} for x in ['sort_author', 'sort_title', 'work_id']
                if x != main_field
            ]
            eq_(default_sort_fields, filter.sort_order[1:])
            return filter.sort_order[0]

        # A simple field, either ascending or descending.
        f.order='field'
        eq_(False, f.order_ascending)
        first_field = validate_sort_order(f, 'field')

        eq_(dict(field='desc'), first_field)

        f.order_ascending = True
        first_field = validate_sort_order(f, 'field')
        eq_(dict(field='asc'), first_field)

        # You can't sort by some random subdocument field, because there's
        # not enough information to know how to aggregate multiple values.
        #
        # You _can_ sort by license pool availability time -- that's
        # tested below -- but it's complicated.
        f.order = 'subdocument.field'
        assert_raises_regexp(
            ValueError, "I don't know how to sort by subdocument.field",
            lambda: f.sort_order,
        )

        # It's possible to sort by every field in
        # Facets.SORT_ORDER_TO_ELASTICSEARCH_FIELD_NAME.
        used_orders = Facets.SORT_ORDER_TO_ELASTICSEARCH_FIELD_NAME
        added_to_collection = used_orders[Facets.ORDER_ADDED_TO_COLLECTION]
        for sort_field in used_orders.values():
            if sort_field == added_to_collection:
                # This is the complicated case, tested below.
                continue
            f.order = sort_field
            first_field = validate_sort_order(f, sort_field)
            eq_({sort_field: 'asc'}, first_field)

        # The only complicated case is when a feed is ordered by date
        # added to the collection. This requires an aggregate function
        # and potentially a nested filter.
        f.order = added_to_collection
        first_field = validate_sort_order(f, sort_field)

        # Here there's no nested filter but there is an aggregate
        # function. If a book is available through multiple
        # collections, we sort by the _earliest_ availability time.
        simple_nested_configuration = {
            'licensepools.availability_time': {'mode': 'min', 'order': 'asc'}
        }
        eq_(simple_nested_configuration, first_field)

        # Setting a collection ID restriction will add a nested filter.
        f.collection_ids = [self._default_collection]
        first_field = validate_sort_order(f, 'licensepools.availability_time')

        # The nested filter ensures that when sorting the results, we
        # only consider availability times from license pools that
        # match our collection filter.
        #
        # Filter.build() will apply the collection filter separately
        # to the 'filter' part of the query -- that's what actually
        # stops books from showing up if they're in the wrong collection.
        #
        # This just makes sure that the books show up in the right _order_
        # for any given set of collections.
        nested_filter = first_field['licensepools.availability_time'].pop('nested')
        eq_(
            {'path': 'licensepools',
             'filter': {
                 'terms': {
                     'licensepools.collection_id': [self._default_collection.id]
                 }
             }
            },
            nested_filter
        )

        # Apart from the nested filter, this is the same ordering
        # configuration as before.
        eq_(simple_nested_configuration, first_field)

    def test_target_age_filter(self):
        # Test an especially complex subfilter.

        # We're going to test the construction of this subfilter using
        # a number of inputs.

        # First, let's create a filter that matches "ages 2 to 5".
        two_to_five = Filter(target_age=(2,5))
        filter = two_to_five.target_age_filter

        # The result is the combination of two filters -- both must
        # match.
        #
        # One filter matches against the lower age range; the other
        # matches against the upper age range.
        eq_("bool", filter.name)
        lower_match, upper_match = filter.must

        # We must establish that two-year-olds are not too old
        # for the book.
        def dichotomy(filter):
            """Verify that `filter` is a boolean filter that
            matches one of a number of possibilities. Return those
            possibilities.
            """
            if MAJOR_VERSION == 1:
                eq_("or", filter.name)
                return filter.filters
            else:
                eq_("bool", filter.name)
                eq_(1, filter.minimum_should_match)
                return filter.should
        more_than_two, no_upper_limit = dichotomy(upper_match)


        # Either the upper age limit must be greater than two...
        eq_(
            {'range': {'target_age.upper': {'gte': 2}}},
            more_than_two.to_dict()
        )

        # ...or the upper age limit must be missing entirely.
        def assert_matches_nonexistent_field(f, field):
            """Verify that a filter only matches when there is
            no value for the given field.
            """
            eq_(
                f.to_dict(),
                {'bool': {'must_not': [{'exists': {'field': field}}]}},
            )
        assert_matches_nonexistent_field(no_upper_limit, 'target_age.upper')

        # We must also establish that five-year-olds are not too young
        # for the book. Again, there are two ways of doing this.
        less_than_five, no_lower_limit = dichotomy(lower_match)

        # Either the lower age limit must be less than five...
        eq_(
            {'range': {'target_age.lower': {'lte': 5}}},
            less_than_five.to_dict()
        )

        # ...or the lower age limit must be missing entirely.
        assert_matches_nonexistent_field(no_lower_limit, 'target_age.lower')

        # Now let's try a filter that matches "ten and under"
        ten_and_under = Filter(target_age=(None, 10))
        filter = ten_and_under.target_age_filter

        # There are two clauses, and one of the two must match.
        less_than_ten, no_lower_limit = dichotomy(filter)

        # Either the lower part of the age range must be <= ten, or
        # there must be no lower age limit. If neither of these are
        # true, then ten-year-olds are too young for the book.
        eq_({'range': {'target_age.lower': {'lte': 10}}},
            less_than_ten.to_dict())
        assert_matches_nonexistent_field(no_lower_limit, 'target_age.lower')

        # Next, let's try a filter that matches "twelve and up".
        twelve_and_up = Filter(target_age=(12, None))
        filter = twelve_and_up.target_age_filter

        # There are two clauses, and one of the two must match.
        more_than_twelve, no_upper_limit = dichotomy(filter)

        # Either the upper part of the age range must be >= twelve, or
        # there must be no upper age limit. If neither of these are true,
        # then twelve-year-olds are too old for the book.
        eq_({'range': {'target_age.upper': {'gte': 12}}},
            more_than_twelve.to_dict())
        assert_matches_nonexistent_field(no_upper_limit, 'target_age.upper')

        # Finally, test filters that put no restriction on target age.
        no_target_age = Filter()
        eq_(None, no_target_age.target_age_filter)

        no_target_age = Filter(target_age=(None, None))
        eq_(None, no_target_age.target_age_filter)

    def test__scrub(self):
        # Test the _scrub helper method, which transforms incoming strings
        # to the type of strings Elasticsearch uses.
        m = Filter._scrub
        eq_(None, m(None))
        eq_("foo", m("foo"))
        eq_("youngadult", m("Young Adult"))

    def test__scrub_list(self):
        # Test the _scrub_list helper method, which scrubs incoming
        # strings and makes sure they are in a list.
        m = Filter._scrub_list
        eq_([], m(None))
        eq_([], m([]))
        eq_(["foo"], m("foo"))
        eq_(["youngadult", "adult"], m(["Young Adult", "Adult"]))

    def test__filter_ids(self):
        # Test the _filter_ids helper method, which converts database
        # objects to their IDs.
        m = Filter._filter_ids
        eq_(None, m(None))
        eq_([], m([]))
        eq_([1,2,3], m([1,2,3]))

        library = self._default_library
        eq_([library.id], m([library]))

    def test__chain_filters(self):
        # Test the _chain_filters method, which combines
        # two Elasticsearch filter objects.
        f1 = F('term', key="value")
        f2 = F('term', key2="value2")

        m = Filter._chain_filters

        # If this filter is the start of the chain, it's returned unaltered.
        eq_(f1, m(None, f1))

        # Otherwise, a new filter is created.
        chained = m(f1, f2)

        # The chained filter is the conjunction of the two input
        # filters.
        eq_(chained, f1 & f2)


class TestSortKeyPagination(DatabaseTest):
    """Test the Elasticsearch-implementation of Pagination that does
    pagination by tracking the last item on the previous page,
    rather than by tracking the number of items seen so far.
    """
    def test_unimplemented_features(self):
        # Check certain features of a normal Pagination object that
        # are not implemented in SortKeyPagination.

        # Set up a realistic SortKeyPagination -- certain things
        # will remain undefined.
        pagination = SortKeyPagination(last_item_on_previous_page=object())
        pagination.this_page_size = 100
        pagination.last_item_on_this_page = object()

        # The offset is always zero.
        eq_(0, pagination.offset)

        # The total size is always undefined, even though we could
        # theoretically track it.
        eq_(None, pagination.total_size)

        # The previous page is always undefined, through theoretically
        # we could navigate backwards.
        eq_(None, pagination.previous_page)

        assert_raises_regexp(
            NotImplementedError,
            "SortKeyPagination does not work with database queries.",
            pagination.apply, object()
        )

    def test_modify_search_query(self):
        class MockSearch(object):
            called_with = "not called"
            def update_from_dict(self, dict):
                self.called_with = dict
                return "modified search object"

        search = MockSearch()

        # We start off in a state where we don't know the last item on the
        # previous page.
        pagination = SortKeyPagination()

        # In this case, modify_search_query does nothing but return
        # the object it was passed.
        eq_(search, pagination.modify_search_query(search))
        eq_("not called", search.called_with)

        # Now we find out the last item on the previous page -- in
        # real life, this is because we call page_loaded() and then
        # next_page().
        last_item = object()
        pagination.last_item_on_previous_page = last_item

        # Now, modify_search_query() calls update_from_dict() on our
        # mock ElasticSearch `Search` object, passing in the last item
        # on the previous page. The return value of
        # modify_search_query() becomes the active Search object.
        eq_("modified search object", pagination.modify_search_query(search))

        # The Elasticsearch object was modified to use the
        # 'search_after' feature.
        eq_(dict(search_after=last_item), search.called_with)

    def test_page_loaded(self):
        # Test what happens to a SortKeyPagination object when a page of
        # results is loaded.
        this_page = SortKeyPagination()

        # Mock an Elasticsearch 'hit' object -- we'll be accessing
        # hit.meta.sort.
        class MockMeta(object):
            def __init__(self, sort_key):
                self.sort = sort_key

        class MockItem(object):
            def __init__(self, sort_key):
                self.meta = MockMeta(sort_key)

        # Make a page of results, each with a unique sort key.
        hits = [
            MockItem(['sort', 'key', num]) for num in range(5)
        ]
        last_hit = hits[-1]

        # Tell the page about the results.
        this_page.page_loaded(hits)

        # We know the size.
        eq_(5, this_page.this_page_size)

        # We know the sort key of the last item in the page.
        eq_(last_hit.meta.sort, this_page.last_item_on_this_page)

        # This code has coverage elsewhere, but just so you see how it
        # works -- we can now get the next page...
        next_page = this_page.next_page

        # And it's defined in terms of the last item on its
        # predecessor. When we pass the new pagination object into
        # create_search_doc, it'll call this object's
        # modify_search_query method. The resulting search query will
        # pick up right where the previous page left off.
        eq_(last_hit.meta.sort, next_page.last_item_on_previous_page)

    def test_next_page(self):

        # To start off, we can't say anything about the next page,
        # because we don't know anything about _this_ page.
        first_page = SortKeyPagination()
        eq_(None, first_page.next_page)

        # Let's learn about this page.
        first_page.this_page_size = 10
        last_item = object()
        first_page.last_item_on_this_page = last_item

        # When we call next_page, the last item on this page becomes the
        # next page's "last item on previous_page"
        next_page = first_page.next_page
        eq_(last_item, next_page.last_item_on_previous_page)

        # Again, we know nothing about this page, since we haven't
        # loaded it yet.
        eq_(None, next_page.this_page_size)
        eq_(None, next_page.last_item_on_this_page)

        # In the unlikely event that we know the last item on the
        # page, but the page size is zero, there is no next page.
        first_page.this_page_size = 0
        eq_(None, first_page.next_page)


class TestBulkUpdate(DatabaseTest):

    def test_works_not_presentation_ready_removed_from_index(self):
        w1 = self._work()
        w1.set_presentation_ready()
        w2 = self._work()
        w2.set_presentation_ready()
        w3 = self._work()
        index = MockExternalSearchIndex()
        successes, failures = index.bulk_update([w1, w2, w3])

        # All three works are regarded as successes, because their
        # state was successfully mirrored to the index.
        eq_(set([w1, w2, w3]), set(successes))
        eq_([], failures)

        # But only the presentation-ready works are actually inserted
        # into the index.
        ids = set(x[-1] for x in index.docs.keys())
        eq_(set([w1.id, w2.id]), ids)

        # If a work stops being presentation-ready, it is removed from
        # the index, and its removal is treated as a success.
        w2.presentation_ready = False
        successes, failures = index.bulk_update([w1, w2, w3])
        eq_([w1.id], [x[-1] for x in index.docs.keys()])
        eq_(set([w1, w2, w3]), set(successes))
        eq_([], failures)

class TestSearchErrors(ExternalSearchTest):

    def test_search_connection_timeout(self):
        if not self.search:
            return

        attempts = []

        def bulk_with_timeout(docs, raise_on_error=False, raise_on_exception=False):
            attempts.append(docs)
            def error(doc):
                return dict(index=dict(status='TIMEOUT',
                                       exception='ConnectionTimeout',
                                       error='Connection Timeout!',
                                       _id=doc['_id'],
                                       data=doc))

            errors = map(error, docs)
            return 0, errors

        self.search.bulk = bulk_with_timeout

        work = self._work()
        work.set_presentation_ready()
        successes, failures = self.search.bulk_update([work])
        eq_([], successes)
        eq_(1, len(failures))
        eq_(work, failures[0][0])
        eq_("Connection Timeout!", failures[0][1])

        # When all the documents fail, it tries again once with the same arguments.
        eq_([work.id, work.id],
            [docs[0]['_id'] for docs in attempts])

    def test_search_single_document_error(self):
        if not self.search:
            return

        successful_work = self._work()
        successful_work.set_presentation_ready()
        failing_work = self._work()
        failing_work.set_presentation_ready()

        def bulk_with_error(docs, raise_on_error=False, raise_on_exception=False):
            failures = [dict(data=dict(_id=failing_work.id),
                             error="There was an error!",
                             exception="Exception")]
            success_count = 1
            return success_count, failures

        self.search.bulk = bulk_with_error

        successes, failures = self.search.bulk_update([successful_work, failing_work])
        eq_([successful_work], successes)
        eq_(1, len(failures))
        eq_(failing_work, failures[0][0])
        eq_("There was an error!", failures[0][1])


class TestSearchIndexCoverageProvider(DatabaseTest):

    def test_operation(self):
        index = MockExternalSearchIndex()
        provider = SearchIndexCoverageProvider(
            self._db, search_index_client=index
        )
        eq_(WorkCoverageRecord.UPDATE_SEARCH_INDEX_OPERATION,
            provider.operation)

    def test_success(self):
        work = self._work()
        work.set_presentation_ready()
        index = MockExternalSearchIndex()
        provider = SearchIndexCoverageProvider(
            self._db, search_index_client=index
        )
        results = provider.process_batch([work])

        # We got one success and no failures.
        eq_([work], results)

        # The work was added to the search index.
        eq_(1, len(index.docs))

    def test_failure(self):
        class DoomedExternalSearchIndex(MockExternalSearchIndex):
            """All documents sent to this index will fail."""
            def bulk(self, docs, **kwargs):
                return 0, [
                    dict(data=dict(_id=failing_work['_id']),
                         error="There was an error!",
                         exception="Exception")
                    for failing_work in docs
                ]

        work = self._work()
        work.set_presentation_ready()
        index = DoomedExternalSearchIndex()
        provider = SearchIndexCoverageProvider(
            self._db, search_index_client=index
        )
        results = provider.process_batch([work])

        # We have one transient failure.
        [record] = results
        eq_(work, record.obj)
        eq_(True, record.transient)
        eq_('There was an error!', record.exception)


class TestSearchIndexMonitor(DatabaseTest):

    def test_process_batch(self):
        index = MockExternalSearchIndex()

        # Here's a work.
        work = self._work()
        work.presentation_ready = True

        # There is no record that it has ever been indexed
        def _record(work):
            records = [
                x for x in work.coverage_records
                if x.operation==WorkCoverageRecord.UPDATE_SEARCH_INDEX_OPERATION
            ]
            if not records:
                return None
            [record] = records
            return record
        eq_(None, _record(work))

        # Here's a Monitor that can index it.
        monitor = SearchIndexMonitor(self._db, None, "works-index",
                                     index_client=index)
        eq_("Search index update (works)", monitor.service_name)

        # The first time we call process_batch we handle the one and
        # only work in the database. The ID of that work is returned
        # for next time, as is the number of works processed by
        # process_batch -- one.
        eq_((work.id, 1), monitor.process_batch(0))
        self._db.commit()

        # The work was added to the search index.
        eq_([('works', 'work-type', work.id)], index.docs.keys())

        # A WorkCoverageRecord was created for the Work.
        assert _record(work) is not None

        # The next time we call process_batch, the result is (0,0),
        # meaning we're done with every work in the system (the first 0)
        # and no work was done (the second 0).
        eq_((0,0), monitor.process_batch(work.id))
