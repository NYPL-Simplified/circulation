from nose.tools import (
    assert_raises,
    assert_raises_regexp,
    eq_,
    set_trace,
)
import logging
import time
from psycopg2.extras import NumericRange

from . import (
    DatabaseTest,
)

from elasticsearch_dsl import (
    Q,
    F,
)
from elasticsearch.exceptions import ElasticsearchException

from config import CannotLoadConfiguration
from lane import (
    Lane,
    Pagination
)
from model import (
    Edition,
    ExternalIntegration,
    Genre,
    Work,
    WorkCoverageRecord,
)
from external_search import (
    ExternalSearchIndex,
    ExternalSearchIndexVersions,
    Filter,
    MockExternalSearchIndex,
    Query,
    QueryParser,
    SearchBase,
    SearchIndexCoverageProvider,
    SearchIndexMonitor,
)
from classifier import Classifier


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
            settings={ExternalSearchIndex.WORKS_INDEX_PREFIX_KEY : u'test_index'}
        )

        try:
            self.search = ExternalSearchIndex(self._db)
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
        eq_("test_index-v3", self.search.works_index_name(self._db))

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
            index=original_index, name='test_index-current'
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

        results = self.search.query_works(*query_args)
        expect = [x.id for x in works]
        expect_ids = ", ".join(map(str, expect))
        expect_titles = ", ".join([x.title for x in works])
        if not kwargs.pop('ordered', True):
            expect = set(expect)
            results = set(results)
        eq_(
            expect, results,
            "Query args %r did not find %d works (%s/%s), instead found %d (%r)" % (
                query_args, len(expect), expect_ids, expect_titles,
                len(results), results
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

            self.moby_dick = _work(title="Moby Dick", authors="Herman Melville", fiction=True)
            self.moby_dick.presentation_edition.subtitle = "Or, the Whale"
            self.moby_dick.presentation_edition.series = "Classics"
            self.moby_dick.summary_text = "Ishmael"
            self.moby_dick.presentation_edition.publisher = "Project Gutenberg"
            self.moby_dick.set_presentation_ready()

            self.moby_duck = _work(title="Moby Duck", authors="Donovan Hohn", fiction=False)
            self.moby_duck.presentation_edition.subtitle = "The True Story of 28,800 Bath Toys Lost at Sea"
            self.moby_duck.summary_text = "A compulsively readable narrative"
            self.moby_duck.presentation_edition.publisher = "Penguin"
            self.moby_duck.set_presentation_ready()

            self.title_match = _work(title="Match")
            self.title_match.set_presentation_ready()

            self.subtitle_match = _work()
            self.subtitle_match.presentation_edition.subtitle = "Match"
            self.subtitle_match.set_presentation_ready()

            self.summary_match = _work()
            self.summary_match.summary_text = "Match"
            self.summary_match.set_presentation_ready()

            self.publisher_match = _work()
            self.publisher_match.presentation_edition.publisher = "Match"
            self.publisher_match.set_presentation_ready()

            self.tess = _work(title="Tess of the d'Urbervilles")
            self.tess.set_presentation_ready()

            self.tiffany = _work(title="Breakfast at Tiffany's")
            self.tiffany.set_presentation_ready()

            self.les_mis = _work()
            self.les_mis.presentation_edition.title = u"Les Mis\u00E9rables"
            self.les_mis.set_presentation_ready()

            self.modern_romance = _work()
            self.modern_romance.presentation_edition.title = u"Modern Romance"
            self.modern_romance.set_presentation_ready()

            self.lincoln = _work(genre="Biography & Memoir", title="Abraham Lincoln")
            self.lincoln.set_presentation_ready()

            self.washington = _work(genre="Biography", title="George Washington")
            self.washington.set_presentation_ready()

            self.lincoln_vampire = _work(title="Abraham Lincoln: Vampire Hunter", genre="Fantasy")
            self.lincoln_vampire.set_presentation_ready()

            self.children_work = _work(title="Alice in Wonderland", audience=Classifier.AUDIENCE_CHILDREN)
            self.children_work.set_presentation_ready()

            self.ya_work = _work(title="Go Ask Alice", audience=Classifier.AUDIENCE_YOUNG_ADULT)
            self.ya_work.set_presentation_ready()

            self.adult_work = _work(title="Still Alice", audience=Classifier.AUDIENCE_ADULT)
            self.adult_work.set_presentation_ready()

            self.ya_romance = _work(
                title="Gumby In Love",
                audience=Classifier.AUDIENCE_YOUNG_ADULT, genre="Romance"
            )
            self.ya_romance.presentation_edition.subtitle = (
                "Modern Fairytale Series, Volume 7"
            )
            self.ya_romance.set_presentation_ready()

            self.no_age = _work()
            self.no_age.summary_text = "President Barack Obama's election in 2008 energized the United States"
            self.no_age.set_presentation_ready()

            self.age_4_5 = _work()
            self.age_4_5.target_age = NumericRange(4, 5, '[]')
            self.age_4_5.summary_text = "President Barack Obama's election in 2008 energized the United States"
            self.age_4_5.set_presentation_ready()

            self.age_5_6 = _work(fiction=False)
            self.age_5_6.target_age = NumericRange(5, 6, '[]')
            self.age_5_6.set_presentation_ready()

            self.obama = _work(genre="Biography & Memoir")
            self.obama.target_age = NumericRange(8, 8, '[]')
            self.obama.summary_text = "President Barack Obama's election in 2008 energized the United States"
            self.obama.set_presentation_ready()

            self.dodger = _work()
            self.dodger.target_age = NumericRange(8, 8, '[]')
            self.dodger.summary_text = "Willie finds himself running for student council president"
            self.dodger.set_presentation_ready()

            self.age_9_10 = _work()
            self.age_9_10.target_age = NumericRange(9, 10, '[]')
            self.age_9_10.summary_text = "President Barack Obama's election in 2008 energized the United States"
            self.age_9_10.set_presentation_ready()

            self.age_2_10 = _work()
            self.age_2_10.target_age = NumericRange(2, 10, '[]')
            self.age_2_10.set_presentation_ready()

            self.pride = _work(title="Pride and Prejudice")
            self.pride.presentation_edition.medium = Edition.BOOK_MEDIUM
            self.pride.set_presentation_ready()

            self.pride_audio = _work(title="Pride and Prejudice")
            self.pride_audio.presentation_edition.medium = Edition.AUDIO_MEDIUM
            self.pride_audio.set_presentation_ready()

            self.sherlock = _work(
                title="The Adventures of Sherlock Holmes",
                with_open_access_download=True
            )
            self.sherlock.presentation_edition.language = "en"
            self.sherlock.set_presentation_ready()

            self.sherlock_spanish = _work(title="Las Aventuras de Sherlock Holmes")
            self.sherlock_spanish.presentation_edition.language = "es"
            self.sherlock_spanish.set_presentation_ready()

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
            self.tiny_book.set_presentation_ready()

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

    def test_query_works(self):
        # An end-to-end test of the search functionality.
        #
        # Works created during setup are added to a real search index.
        # We then run actual Elasticsearch queries against the
        # search index and verify that the work IDs returned
        # are the ones we expect.
        if not self.search:
            self.logging.error(
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
        expect(
            [self.title_match, self.subtitle_match,
             self.summary_match, self.publisher_match],
            "match"
        )

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
        # e-with-acute. (NOTE: it's not clear whether this is a
        # feature of the index or done by the fuzzy match.)
        expect(self.les_mis, "les miserables")

        # Find results based on fiction status.
        #
        # Here, Moby-Dick (fiction) is privileged over Moby Duck
        # (nonfiction)
        expect([self.moby_dick, self.moby_duck], "fiction moby")

        # Here, Moby Duck is privileged over Moby-Dick.
        expect([self.moby_duck, self.moby_dick], "nonfiction moby")

        # Find results based on genre.

        # The name of the genre also shows up in the title of a book,
        # but the genre boost means the romance novel is the first
        # result.
        expect([self.ya_romance, self.modern_romance], "romance")

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

        # TODO: I expected this to act like the other queries, but the
        # 2-10 book shows up above the 9-10 book. This might indicate
        # a bug.
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

        # When a string exactly matches both a title and an author,
        # the books that match exactly are promoted.
        expect(
            [self.biography_of_peter_graves, self.behind_the_scenes,
             self.book_by_peter_graves, self.book_by_someone_else],
            "peter graves"
        )

        # 'The Making of Biography With Peter Graves' does worse in a
        # search for 'peter graves biography' than a biography whose
        # title includes the phrase 'peter graves'. Although the title
        # contains all three search terms, it's not an exact token
        # match. But "The Making of..." still does better than
        # books that match 'peter graves' (or 'peter' and 'graves'),
        # but not 'biography'.
        expect(
            [self.biography_of_peter_graves, # title + genre 'biography'
             self.behind_the_scenes,         # all words match in title
             self.book_by_peter_graves,      # author (no 'biography')
             self.book_by_someone_else       # title + author (no 'biography')
            ],
            "peter graves biography"
        )

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
        # Verify that the build() method combines the 'query' part of the
        # Query and the 'filter' part to create a single Elasticsearch
        # query object.
        class Mock(Query):
            def query(self):
                return Q("simple_query_string", query=self.query_string)

        # If there's a filter, an ElasticSearch Filtered object is created.
        filter = Filter(fiction=True)
        m = Mock("query string", filter=filter)
        filtered = m.build()

        # The 'query' part came from calling Query.query()
        eq_(filtered.query, m.query())

        # The 'filter' part came from Filter.build()
        eq_(filtered.filter, filter.build())

        # If there's no filter, the return value of Query.query()
        # is used as-is.
        m.filter = None
        unfiltered = m.build()
        eq_(unfiltered, m.query())
        assert not hasattr(unfiltered, 'filter')

    def test_query(self):
        # The query() method calls a number of other methods
        # to generate hypotheses, then creates a dis_max query
        # to find the most likely hypothesis for any given book.

        class Mock(Query):

            _match_phrase_called_with = []
            _boosts = {}

            fuzzy_string_query_returns_something = True
            _parsed_query_matches_returns_something = True

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
                if self.fuzzy_string_query_returns_something:
                    return "fuzzy string"
                else:
                    return None

            def _parsed_query_matches(self, query_string):
                self._parsed_query_matches_called_with = query_string
                if self._parsed_query_matches_returns_something:
                    return "parsed query matches"
                else:
                    return None

            def _hypothesize(self, hypotheses, new_hypothesis, boost="default"):
                self._boosts[new_hypothesis] = boost
                hypotheses.append(new_hypothesis)
                return hypotheses

            def _combine_hypotheses(self, hypotheses):
                self._combine_hypotheses_called_with = hypotheses
                return hypotheses

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
             'title.standard match phrase', 'author.standard match phrase',
             'fuzzy string', 'parsed query matches'], result)

        # In each case, the original query string was used as the
        # input into the mocked method.
        eq_(q, query.simple_query_string_called_with)
        eq_((q, "default fields"),
            query.minimal_stemming_called_with)
        eq_([('title.standard', q), ('author.standard', q)],
            query._match_phrase_called_with)
        eq_(q, query.fuzzy_string_called_with)
        eq_(q, query._parsed_query_matches_called_with)

        # Each call to _hypothesize included a boost factor indicating
        # how heavily to weight that hypothesis. Rather than do anything
        # with this information, we just stored it in _boosts.

        # Exact title or author matches are valued quite highly.
        for field in 'title.standard', 'author.standard':
            key = field + " match phrase"
            eq_(200, query._boosts[key])

        # A near-exact match is also valued highly.
        eq_(100, query._boosts['minimal stemming'])

        # The default query match has the default boost.
        eq_("default", query._boosts['simple'])

        # The fuzzy match has a boost that's very low, to encourage any
        # matches that are better to show up first.
        eq_(1, query._boosts['fuzzy string'])

        # If fuzzy_string_query() or _parsed_query_matches()
        # returns None, then those hypotheses are not tested.
        query.fuzzy_string_query_returns_something = False
        query._parsed_query_matches_returns_something = False
        result = query.query()
        eq_(
            ['simple', 'minimal stemming',
             'title.standard match phrase', 'author.standard match phrase',],
            result
        )

    def test__hypothesize(self):
        # Verify that _hypothesize() adds a query to a list,
        # boosting it if necessary.
        class Mock(Query):
            @classmethod
            def _boost(cls, boost, query):
                return "%s boosted by %d" % (query, boost)

        hypotheses = []

        # If the boost is greater than 1, _boost() is called on the
        # query object.
        Mock._hypothesize(hypotheses, "query object", 10)
        eq_(["query object boosted by 10"], hypotheses)

        # If it's not greater than 1, _boost() is not called and the query
        # object is used as-is.
        Mock._hypothesize(hypotheses, "another query object", 1)
        eq_(["query object boosted by 10", "another query object"], hypotheses)

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
        eq_(1, boosted_one.minimum_should_match)
        eq_([q1], boosted_one.should)

        boosted_multiple = Query._boost(4.5, [q1, q2])
        eq_("bool", boosted_multiple.name)
        eq_(4.5, boosted_multiple.boost)
        eq_(1, boosted_multiple.minimum_should_match)
        eq_([q1, q2], boosted_multiple.should)

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
        # match_phrases creates a Match Elasticsearch object which
        # does a match against a specific field.
        qu = Query._match("author.standard", "flannery o'connor")
        eq_(
            {'match': {'author.standard': "flannery o'connor"}},
            qu.to_dict()
        )

    def test__match_phrase(self):
        # match_phrases creates a MatchPhrase Elasticsearch
        # object which does a phrase match against a specific field.
        qu = Query._match_phrase("author.standard", "flannery o'connor")
        eq_(
            {'match_phrase': {'author.standard': "flannery o'connor"}},
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

        qu = Query._parsed_query_matches("nonfiction")
        eq_('bool', qu.name)
        eq_(190, qu.boost)
        [must_match] = qu.must
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

    def test_query(self):
        # Test the ability to create an ElasticSearch query
        # out of a bunch of match_queries.

        # This query can't be parsed, so it has no .match_queries, so
        # its .query is none. It'll need to be handled some other way
        # (in this case, by an author match).
        parser = QueryParser("octavia butler")
        eq_([], parser.match_queries)
        eq_(None, parser.query)

        # If the query can be parsed, then .match_queries is set
        # and its .query is a Bool query.
        parser = QueryParser("romance")
        eq_(1, len(parser.match_queries))
        query = parser.query
        eq_("bool", query.name)

        # All of the .match_queries must match for this book to count
        # as 'matching' this query.
        eq_(parser.match_queries, query.must)

        # If the book does match, its score will be boosted pretty high.
        eq_(190, query.boost)

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

        year_range = Filter(target_age=NumericRange(4, 5, '[]'))
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

        # Because of that, the final filter we end up with is nearly
        # empty. The only restriction here is the collection
        # restriction imposed by the fact that does_not_inherit
        # is, itself, associated with a specific library.
        filter = Filter.from_worklist(self._db, does_not_inherit, facets)
        eq_({'terms': {'collection_id': [self._default_collection.id]}},
            filter.build().to_dict())

    def test_build(self):
        # Test the ability to turn a Filter into an ElasticSearch
        # filter object.

        # build() takes the information in the Filter object, scrubs
        # it, and uses _chain_filters to chain together a number of
        # sub-filters.
        #
        # Let's try it with some simple cases before mocking
        # _chain_filters for a more detailed test.

        # Start with an empty filter.
        filter = Filter()
        eq_(None, filter.build())

        # Add a medium clause to the filter.
        filter.media = "a medium"
        medium_built = {'terms': {'medium': ['amedium']}}
        eq_(medium_built, filter.build().to_dict())

        # Add a language clause to the filter.
        filter.languages = ["lang1", "LANG2"]
        language_built = {'terms': {'language': ['lang1', 'lang2']}}

        # Now both the medium clause and the language clause must match.
        eq_(
            {'bool': {'must': [medium_built, language_built]}},
            filter.build().to_dict()
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
        built = filter.build(_chain_filters=chain)

        # Every restriction imposed on the Filter object becomes an
        # Elasticsearch filter object in this list.
        (collection, medium, language, fiction, audience, target_age,
         literary_fiction_filter, fantasy_or_horror_filter,
         best_sellers_filter, staff_picks_filter) = built

        # Test them one at a time.
        #
        # Throughout this test, notice that the data model objects --
        # Collections, Genres, and CustomLists -- have been replaced with
        # their database IDs. This is done by filter_ids.
        #
        # Also, audience, medium, and language have been run through
        # scrub_list, which turns scalar values into lists, removes
        # spaces, and converts to lowercase.

        eq_(
            {'terms': {'collection_id': [self._default_collection.id]}},
            collection.to_dict()
        )

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
        eq_({'terms': {'list_id': [self.best_sellers.id]}},
            best_sellers_filter.to_dict())
        eq_({'terms': {'list_id': [self.staff_picks.id]}},
            staff_picks_filter.to_dict())

        # We tried fiction; now try nonfiction.
        filter = Filter()
        filter.fiction = False
        eq_({'term': {'fiction': 'nonfiction'}}, filter.build().to_dict())

    def test_target_age_filter(self):
        # Test an especially complex subfilter.

        # We're going to test the construction of this subfilter using
        # a number of inputs.

        # First, let's create a filter that matches "ages 2 to 5".
        two_to_five = Filter(target_age=(2,5))
        filter = two_to_five.target_age_filter

        # The result is the combination of two filters -- both must
        # match.
        eq_("and", filter.name)

        # One filter matches against the lower age range; the other
        # matches against the upper age range.
        lower_match, upper_match = filter.filters

        # We must establish that two-year-olds are not too old
        # for the book.
        eq_("or", upper_match.name)
        more_than_two, no_upper_limit = upper_match.filters

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
        eq_("or", lower_match.name)
        less_than_five, no_lower_limit = lower_match.filters

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
        eq_('or', filter.name)
        less_than_ten, no_lower_limit = filter.filters

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
        eq_('or', filter.name)
        more_than_twelve, no_upper_limit = filter.filters

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
        # only work in the database. The ID of that work is returned for
        # next time.
        eq_(work.id, monitor.process_batch(0))
        self._db.commit()

        # The work was added to the search index.
        eq_([('works', 'work-type', work.id)], index.docs.keys())

        # A WorkCoverageRecord was created for the Work.
        assert _record(work) is not None

        # The next time we call process_batch, no work is done and the
        # result is 0, meaning we're done with every work in the system.
        eq_(0, monitor.process_batch(work.id))
