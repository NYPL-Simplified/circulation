from nose.tools import (
    eq_,
    set_trace,
)
import logging
import time
from psycopg2.extras import NumericRange

from testing import DatabaseTest
from config import (
    temp_config,
    Configuration,
)
from external_search import (
    ExternalSearchIndex,
    DummyExternalSearchIndex,
)
from classifier import Classifier

class TestExternalSearch(DatabaseTest):

    def setup(self):
        super(TestExternalSearch, self).setup()
        with temp_config() as config:
            config[Configuration.INTEGRATIONS][Configuration.ELASTICSEARCH_INTEGRATION] = {}
            config[Configuration.INTEGRATIONS][Configuration.ELASTICSEARCH_INTEGRATION][Configuration.URL] = "http://localhost:9200"
            config[Configuration.INTEGRATIONS][Configuration.ELASTICSEARCH_INTEGRATION][Configuration.ELASTICSEARCH_INDEX_KEY] = "test_index"

            try:
                self.search = ExternalSearchIndex()
                # start with an empty index
                self.search.indices.delete(self.search.works_index)
                self.search.indices.create(self.search.works_index)
            except Exception:
                self.search = None
                print "Elasticsearch isn't running locally, search tests will be skipped."

    def teardown(self):
        if self.search:
            self.search.indices.delete(self.search.works_index)
        super(TestExternalSearch, self).teardown()

    def test_query_works_matches_all_main_fields(self):
        if not self.search:
            return

        work = self._work(title="Moby Dick", authors="Herman Melville")
        work.primary_edition.subtitle = "Or, the Whale"
        work.primary_edition.series = "Classics"
        work.summary_text = "Ishmael"
        work.set_presentation_ready()
        work.update_external_index(self.search)
        time.sleep(1)

        title_results = self.search.query_works("moby", None, None, None, None, None, None, None)
        eq_(1, len(title_results["hits"]["hits"]))

        author_results = self.search.query_works("melville", None, None, None, None, None, None, None)
        eq_(1, len(author_results["hits"]["hits"]))

        subtitle_results = self.search.query_works("whale", None, None, None, None, None, None, None)
        eq_(1, len(subtitle_results["hits"]["hits"]))

        series_results = self.search.query_works("classics", None, None, None, None, None, None, None)
        eq_(1, len(series_results["hits"]["hits"]))

        summary_results = self.search.query_works("ishmael", None, None, None, None, None, None, None)
        eq_(1, len(summary_results["hits"]["hits"]))

    def test_query_works_ranks_title_above_subtitle_above_summary(self):
        if not self.search:
            return

        title_match = self._work(title="Match")
        title_match.set_presentation_ready()
        title_match.update_external_index(self.search)

        subtitle_match = self._work()
        subtitle_match.primary_edition.subtitle = "Match"
        subtitle_match.set_presentation_ready()
        subtitle_match.update_external_index(self.search)

        summary_match = self._work()
        summary_match.summary_text = "Match"
        summary_match.set_presentation_ready()
        summary_match.update_external_index(self.search)
        
        time.sleep(1)
 
        results = self.search.query_works("match", None, None, None, None, None, None, None)
        hits = results["hits"]["hits"]
        eq_(3, len(hits))
        eq_(unicode(title_match.id), hits[0]['_id'])
        eq_(unicode(subtitle_match.id), hits[1]['_id'])
        eq_(unicode(summary_match.id), hits[2]['_id'])
        
    def test_query_works_ranks_closer_match_higher(self):
        if not self.search:
            return

        work = self._work(title="Moby Dick", authors="Herman Melville")
        work.set_presentation_ready()
        work.update_external_index(self.search)

        other_work = self._work(title="Moby", authors="Someone Else")
        other_work.set_presentation_ready()
        other_work.update_external_index(self.search)

        time.sleep(1)
        
        results = self.search.query_works("moby dick", None, None, None, None, None, None, None)
        hits = results["hits"]["hits"]
        eq_(2, len(hits))
        eq_(unicode(work.id), hits[0]['_id'])

    def test_query_works_ranks_both_title_and_author_higher(self):
        if not self.search:
            return

        title_author = self._work(title="Moby Dick", authors="Herman Melville")
        title_author.set_presentation_ready()
        title_author.update_external_index(self.search)

        title_only = self._work(title="Moby", authors="Someone Else")
        title_only.set_presentation_ready()
        title_only.update_external_index(self.search)

        time.sleep(1)
        
        results = self.search.query_works("moby melville", None, None, None, None, None, None, None)
        hits = results["hits"]["hits"]
        eq_(2, len(hits))
        eq_(unicode(title_author.id), hits[0]['_id'])

    def test_query_works_matches_misspelled_word(self):
        if not self.search:
            return

        work = self._work(title="Moby Dick", authors="Herman Melville")
        work.set_presentation_ready()
        work.update_external_index(self.search)

        time.sleep(1)
        
        results = self.search.query_works("mboy", None, None, None, None, None, None, None)
        hits = results["hits"]["hits"]
        eq_(1, len(hits))

        results = self.search.query_works("mo by dick", None, None, None, None, None, None, None)
        hits = results["hits"]["hits"]
        eq_(1, len(hits))

        results = self.search.query_works("mobydick", None, None, None, None, None, None, None)
        hits = results["hits"]["hits"]
        eq_(1, len(hits))

    def test_query_works_matches_word_with_apostrophe(self):
        if not self.search:
            return

        work1 = self._work(title="Tess of the d'Urbervilles")
        work1.set_presentation_ready()
        work1.update_external_index(self.search)

        work2 = self._work(title="Parade's End")
        work2.set_presentation_ready()
        work2.update_external_index(self.search)

        time.sleep(1)

        results = self.search.query_works("durbervilles", None, None, None, None, None, None, None)
        hits = results["hits"]["hits"]
        eq_(1, len(hits))
        eq_(unicode(work1.id), hits[0]['_id'])

        results = self.search.query_works("parades end", None, None, None, None, None, None, None)
        hits = results["hits"]["hits"]
        eq_(1, len(hits))
        eq_(unicode(work2.id), hits[0]['_id'])
        
    def test_query_works_matches_word_with_unicode_char(self):
        if not self.search:
            return

        work = self._work()
        work.primary_edition.title = u"Les Mis\u00E9rables"
        work.set_presentation_ready()
        work.update_external_index(self.search)

        time.sleep(1)

        results = self.search.query_works("les miserables", None, None, None, None, None, None, None)
        hits = results["hits"]["hits"]
        eq_(1, len(hits))

    def test_query_works_matches_fiction_only(self):
        if not self.search:
            return

        fiction_work = self._work(fiction=True)
        fiction_work.set_presentation_ready()
        fiction_work.update_external_index(self.search)

        nonfiction_work = self._work(fiction=False)
        nonfiction_work.set_presentation_ready()
        nonfiction_work.update_external_index(self.search)

        time.sleep(1)

        results = self.search.query_works("fiction", None, None, None, None, None, None, None)
        hits = results["hits"]["hits"]
        eq_(1, len(hits))
        eq_(unicode(fiction_work.id), hits[0]['_id'])

        results = self.search.query_works("nonfiction", None, None, None, None, None, None, None)
        hits = results["hits"]["hits"]
        eq_(1, len(hits))
        eq_(unicode(nonfiction_work.id), hits[0]['_id'])

    def test_query_works_matches_genre_only(self):
        if not self.search:
            return

        work = self._work(genre="Romance")
        work.set_presentation_ready()
        work.update_external_index(self.search)

        time.sleep(1)

        results = self.search.query_works("romance", None, None, None, None, None, None, None)
        hits = results["hits"]["hits"]
        eq_(1, len(hits))

    def test_query_works_matches_audience_only(self):
        if not self.search:
            return

        children_work = self._work(audience=Classifier.AUDIENCE_CHILDREN)
        children_work.set_presentation_ready()
        children_work.update_external_index(self.search)

        ya_work = self._work(audience=Classifier.AUDIENCE_YOUNG_ADULT)
        ya_work.set_presentation_ready()
        ya_work.update_external_index(self.search)

        adult_work = self._work(audience=Classifier.AUDIENCE_ADULT)
        adult_work.set_presentation_ready()
        adult_work.update_external_index(self.search)

        time.sleep(1)

        results = self.search.query_works("children's", None, None, None, None, None, None, None)
        hits = results["hits"]["hits"]
        eq_(1, len(hits))
        eq_(unicode(children_work.id), hits[0]['_id'])

        results = self.search.query_works("young adult", None, None, None, None, None, None, None)
        hits = results["hits"]["hits"]
        eq_(1, len(hits))
        eq_(unicode(ya_work.id), hits[0]['_id'])

    def test_query_works_matches_grade_only(self):
        if not self.search:
            return

        work = self._work(title="abc")
        work.target_age = NumericRange(10, 14, '[]')
        work.set_presentation_ready()
        work.update_external_index(self.search)

        other_work = self._work(title="def")
        other_work.target_age = NumericRange(3, 5, '[]')
        other_work.set_presentation_ready()
        other_work.update_external_index(self.search)

        time.sleep(1)

        results = self.search.query_works("grade 6", None, None, None, None, None, None, None)
        hits = results["hits"]["hits"]
        eq_(1, len(hits))
        eq_(unicode(work.id), hits[0]['_id'])
        
        results = self.search.query_works("grade 5-6", None, None, None, None, None, None, None)
        hits = results["hits"]["hits"]
        eq_(1, len(hits))
        eq_(unicode(work.id), hits[0]['_id'])

    def test_query_works_matches_age_only(self):
        if not self.search:
            return

        work = self._work(title="abc")
        work.target_age = NumericRange(10, 14, '[]')
        work.set_presentation_ready()
        work.update_external_index(self.search)

        other_work = self._work(title="def")
        other_work.target_age = NumericRange(3, 5, '[]')
        other_work.set_presentation_ready()
        other_work.update_external_index(self.search)

        time.sleep(1)

        results = self.search.query_works("age 11", None, None, None, None, None, None, None)
        hits = results["hits"]["hits"]
        eq_(1, len(hits))
        eq_(unicode(work.id), hits[0]['_id'])
        
        results = self.search.query_works("age 10-12", None, None, None, None, None, None, None)
        hits = results["hits"]["hits"]
        eq_(1, len(hits))
        eq_(unicode(work.id), hits[0]['_id'])

    def test_query_works_ranks_closest_target_age_range_highest(self):
        if not self.search:
            return

        work1 = self._work(title="abc")
        work1.target_age = NumericRange(5, 6)
        work1.set_presentation_ready()
        work1.update_external_index(self.search)

        work2 = self._work(title="abc")
        work2.target_age = NumericRange(6, 7)
        work2.set_presentation_ready()
        work2.update_external_index(self.search)

        work3 = self._work(title="abc")
        work3.target_age = NumericRange(3, 10)
        work3.set_presentation_ready()
        work3.update_external_index(self.search)

        time.sleep(1)

        results = self.search.query_works("age 4-6", None, None, None, None, None, None, None)
        hits = results["hits"]["hits"]
        eq_(3, len(hits))
        eq_(unicode(work1.id), hits[0]['_id'])
        eq_(unicode(work2.id), hits[1]['_id'])
        eq_(unicode(work3.id), hits[2]['_id'])

    def test_query_works_matches_genre_and_audience(self):
        if not self.search:
            return

        genre_and_audience = self._work(genre="Romance", audience=Classifier.AUDIENCE_YOUNG_ADULT)
        genre_and_audience.set_presentation_ready()
        genre_and_audience.update_external_index(self.search)

        genre_only = self._work(genre="Romance")
        genre_only.set_presentation_ready()
        genre_only.update_external_index(self.search)

        audience_only = self._work(audience=Classifier.AUDIENCE_YOUNG_ADULT)
        audience_only.set_presentation_ready()
        audience_only.update_external_index(self.search)

        time.sleep(1)

        results = self.search.query_works("young adult romance", None, None, None, None, None, None, None)
        hits = results["hits"]["hits"]
        eq_(1, len(hits))
        eq_(unicode(genre_and_audience.id), hits[0]['_id'])

    def test_query_works_matches_age_range_and_fiction(self):
        if not self.search:
            return

        age_range_and_fiction = self._work(fiction=True)
        age_range_and_fiction.target_age = NumericRange(5, 7, '[]')
        age_range_and_fiction.set_presentation_ready()
        age_range_and_fiction.update_external_index(self.search)

        age_range_only = self._work()
        age_range_only.target_age = NumericRange(7, 7)
        age_range_only.set_presentation_ready()
        age_range_only.update_external_index(self.search)

        fiction_only = self._work(fiction=True)
        fiction_only.set_presentation_ready()
        fiction_only.update_external_index(self.search)

        time.sleep(1)

        results = self.search.query_works("age 7 fiction", None, None, None, None, None, None, None)
        hits = results["hits"]["hits"]
        eq_(1, len(hits))
        eq_(unicode(age_range_and_fiction.id), hits[0]['_id'])

    def test_query_works_matches_genre_and_title(self):
        if not self.search:
            return

        genre_title = self._work(genre="Biography", title="Abraham Lincoln")
        genre_title.set_presentation_ready()
        genre_title.update_external_index(self.search)

        genre_only = self._work(genre="Biography")
        genre_only.set_presentation_ready()
        genre_only.update_external_index(self.search)

        title_only = self._work(title="Abraham Lincoln")
        title_only.set_presentation_ready()
        title_only.update_external_index(self.search)

        time.sleep(1)

        results = self.search.query_works("lincoln biography", None, None, None, None, None, None, None)
        hits = results["hits"]["hits"]
        eq_(2, len(hits))
        eq_(unicode(genre_title.id), hits[0]['_id'])
        eq_(unicode(title_only.id), hits[1]['_id'])

    def test_query_works_matches_audience_and_author(self):
        if not self.search:
            return

        audience_author = self._work(audience=Classifier.AUDIENCE_YOUNG_ADULT, authors="Sherman Alexie")
        audience_author.set_presentation_ready()
        audience_author.update_external_index(self.search)

        audience_only = self._work(audience=Classifier.AUDIENCE_YOUNG_ADULT)
        audience_only.set_presentation_ready()
        audience_only.update_external_index(self.search)

        author_only = self._work(audience=Classifier.AUDIENCE_ADULT, authors="Sherman Alexie")
        author_only.set_presentation_ready()
        author_only.update_external_index(self.search)

        time.sleep(1)

        results = self.search.query_works("sherman alexie ya", None, None, None, None, None, None, None)
        hits = results["hits"]["hits"]
        eq_(2, len(hits))
        eq_(unicode(audience_author.id), hits[0]['_id'])
        eq_(unicode(author_only.id), hits[1]['_id'])

    def test_query_works_matches_fiction_and_summary(self):
        if not self.search:
            return

        fiction_summary = self._work(fiction=True)
        fiction_summary.summary_text = "A book about a dog"
        fiction_summary.set_presentation_ready()
        fiction_summary.update_external_index(self.search)

        nonfiction_summary = self._work(fiction=False)
        nonfiction_summary.summary_text = "A book about a real dog"
        nonfiction_summary.set_presentation_ready()
        nonfiction_summary.update_external_index(self.search)

        time.sleep(1)

        results = self.search.query_works("dog nonfiction", None, None, None, None, None, None, None)
        hits = results["hits"]["hits"]
        eq_(2, len(hits))
        eq_(unicode(nonfiction_summary.id), hits[0]['_id'])
        eq_(unicode(fiction_summary.id), hits[1]['_id'])

    def test_query_works_matches_age_genre_and_summary(self):
        if not self.search:
            return

        age_genre_summary = self._work(genre="Biography")
        age_genre_summary.target_age = NumericRange(8, 9, '[]')
        age_genre_summary.summary_text = "President Barack Obama's election in 2008 energized the United States"
        age_genre_summary.set_presentation_ready()
        age_genre_summary.update_external_index(self.search)

        genre_only = self._work(genre="Biography")
        genre_only.set_presentation_ready()
        genre_only.update_external_index(self.search)

        age_summary = self._work()
        age_summary.target_age = NumericRange(8, 9, '[]')
        age_summary.summary_text = "Willie finds himself running for student council president"
        age_summary.set_presentation_ready()
        age_summary.update_external_index(self.search)

        time.sleep(1)

        results = self.search.query_works("age 8 president biography", None, None, None, None, None, None, None)
        hits = results["hits"]["hits"]
        eq_(2, len(hits))
        eq_(unicode(age_genre_summary.id), hits[0]['_id'])
        eq_(unicode(age_summary.id), hits[1]['_id'])

    def test_make_query(self):

        search = DummyExternalSearchIndex()

        # Basic query
        query = search.make_query("test")['bool']

        assert 'must' in query
        assert 'should' in query
        must = query['must']
        should = query['should']

        eq_(1, len(must))
        multi_match = must[0]['bool']['should'][0]['multi_match']
        eq_("test", multi_match['query'])
        assert "title^4" in multi_match['fields']
        
        eq_(1, len(should))
        multi_match = should[0]['multi_match']
        eq_("test", multi_match['query'])
        assert 'publisher' in multi_match['fields']


        # Query with genre
        query = search.make_query("test romance")['bool']
        
        assert 'must' in query
        assert 'should' in query
        must = query['must'][0]['bool']['should']
        should = query['should']

        eq_(2, len(must))
        full_query = must[0]['multi_match']
        eq_("test romance", full_query['query'])
        assert "title^4" in full_query['fields']

        classification_query = must[1]['bool']['must']
        eq_(2, len(classification_query))
        genre_query = classification_query[0]['multi_match']
        eq_('Romance', genre_query['query'])
        assert 'classifications.name' in genre_query['fields']
        remaining_query = classification_query[1]['multi_match']
        assert "test" in remaining_query['query']
        assert "romance" not in remaining_query['query']
        assert 'author^4' in remaining_query['fields']
        
        eq_(1, len(should))
        multi_match = should[0]['multi_match']
        eq_("test romance", multi_match['query'])
        assert 'publisher' in multi_match['fields']


        # Query with fiction
        query = search.make_query("test nonfiction")['bool']
        
        assert 'must' in query
        assert 'should' in query
        must = query['must'][0]['bool']['should']
        should = query['should']

        eq_(2, len(must))

        classification_query = must[1]['bool']['must']
        eq_(2, len(classification_query))
        fiction_query = classification_query[0]['multi_match']
        eq_('Nonfiction', fiction_query['query'])
        eq_(1, len(fiction_query['fields']))
        assert 'fiction' in fiction_query['fields']
        remaining_query = classification_query[1]['multi_match']
        assert "test" in remaining_query['query']
        assert "fiction" not in remaining_query['query']
        assert 'author^4' in remaining_query['fields']
        

        # Query with genre and fiction
        query = search.make_query("test romance fiction")['bool']
        
        assert 'must' in query
        assert 'should' in query
        must = query['must'][0]['bool']['should']
        should = query['should']

        eq_(2, len(must))

        classification_query = must[1]['bool']['must']
        eq_(3, len(classification_query))
        genre_query = classification_query[0]['multi_match']
        eq_('Romance', genre_query['query'])
        assert 'classifications.name' in genre_query['fields']
        fiction_query = classification_query[1]['multi_match']
        eq_('Fiction', fiction_query['query'])
        eq_(1, len(fiction_query['fields']))
        assert 'fiction' in fiction_query['fields']
        remaining_query = classification_query[2]['multi_match']
        assert "test" in remaining_query['query']
        assert "romance" not in remaining_query['query']
        assert "fiction" not in remaining_query['query']
        assert 'author^4' in remaining_query['fields']

        # Query with audience
        query = search.make_query("test young adult")['bool']
        
        assert 'must' in query
        assert 'should' in query
        must = query['must'][0]['bool']['should']
        should = query['should']

        eq_(2, len(must))
        full_query = must[0]['multi_match']
        eq_("test young adult", full_query['query'])

        classification_query = must[1]['bool']['must']
        eq_(2, len(classification_query))
        audience_query = classification_query[0]['multi_match']
        eq_('YoungAdult', audience_query['query'])
        assert 'audience' in audience_query['fields']
        remaining_query = classification_query[1]['multi_match']
        assert "test" in remaining_query['query']
        assert "young" not in remaining_query['query']
        
        # Query with grade
        query = search.make_query("test grade 6")['bool']
        
        assert 'must' in query
        assert 'should' in query
        must = query['must'][0]['bool']['should']
        should = query['should']

        eq_(2, len(must))
        full_query = must[0]['multi_match']
        eq_("test grade 6", full_query['query'])

        classification_query = must[1]['bool']['must']
        eq_(2, len(classification_query))
        grade_query = classification_query[0]['bool']
        assert 'must' in grade_query
        assert 'should' in grade_query
        age_must = grade_query['must']
        eq_(2, len(age_must))
        eq_(11, age_must[0]['range']['target_age.upper']['gte'])
        eq_(11, age_must[1]['range']['target_age.lower']['lte'])

        remaining_query = classification_query[1]['multi_match']
        assert "test" in remaining_query['query']
        assert "grade" not in remaining_query['query']
        
        # Query with age
        query = search.make_query("test 5-10 years")['bool']
        
        assert 'must' in query
        assert 'should' in query
        must = query['must'][0]['bool']['should']
        should = query['should']

        eq_(2, len(must))
        full_query = must[0]['multi_match']
        eq_("test 5-10 years", full_query['query'])

        classification_query = must[1]['bool']['must']
        eq_(2, len(classification_query))
        grade_query = classification_query[0]['bool']
        assert 'must' in grade_query
        assert 'should' in grade_query
        age_must = grade_query['must']
        eq_(2, len(age_must))
        eq_(5, age_must[0]['range']['target_age.upper']['gte'])
        eq_(10, age_must[1]['range']['target_age.lower']['lte'])

        remaining_query = classification_query[1]['multi_match']
        assert "test" in remaining_query['query']
        assert "5" not in remaining_query['query']
        assert "years" not in remaining_query['query']
        
