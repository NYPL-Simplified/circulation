from nose.tools import (
    eq_,
    set_trace,
)

from external_search import ExternalSearchIndex

class TestExternalSearch(object):

    def test_make_query(self):

        search = ExternalSearchIndex(fallback_to_dummy=True)

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

        genre_and_remaining_query = must[1]['bool']['must']
        eq_(2, len(genre_and_remaining_query))
        genre_query = genre_and_remaining_query[0]['multi_match']
        eq_('Romance', genre_query['query'])
        assert 'classifications.name' in genre_query['fields']
        remaining_query = genre_and_remaining_query[1]['multi_match']
        assert "test" in remaining_query['query']
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

        fiction_and_remaining_query = must[1]['bool']['must']
        eq_(2, len(fiction_and_remaining_query))
        fiction_query = fiction_and_remaining_query[0]['multi_match']
        eq_('Nonfiction', fiction_query['query'])
        eq_(1, len(fiction_query['fields']))
        assert 'fiction' in fiction_query['fields']
        remaining_query = fiction_and_remaining_query[1]['multi_match']
        assert "test" in remaining_query['query']
        assert 'author^4' in remaining_query['fields']
        

        # Query with genre and fiction
        query = search.make_query("test romance fiction")['bool']
        
        assert 'must' in query
        assert 'should' in query
        must = query['must'][0]['bool']['should']
        should = query['should']

        eq_(2, len(must))

        genre_fiction_and_remaining_query = must[1]['bool']['must']
        eq_(3, len(genre_fiction_and_remaining_query))
        genre_query = genre_and_remaining_query[0]['multi_match']
        eq_('Romance', genre_query['query'])
        assert 'classifications.name' in genre_query['fields']
        fiction_query = genre_fiction_and_remaining_query[1]['multi_match']
        eq_('Fiction', fiction_query['query'])
        eq_(1, len(fiction_query['fields']))
        assert 'fiction' in fiction_query['fields']
        remaining_query = genre_fiction_and_remaining_query[2]['multi_match']
        assert "test" in remaining_query['query']
        assert 'author^4' in remaining_query['fields']

