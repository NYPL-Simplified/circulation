from nose.tools import (
    eq_,
    set_trace,
)

from external_search import DummyExternalSearchIndex

class TestExternalSearch(object):

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
        
