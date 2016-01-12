from nose.tools import set_trace
from elasticsearch import Elasticsearch
from config import Configuration
from classifier import KeywordBasedClassifier
import os
import logging
import re

class ExternalSearchIndex(Elasticsearch):
    
    work_document_type = 'work-type'
    
    def __init__(self, url=None, works_index=None, fallback_to_dummy=True):
        integration = Configuration.integration(
            Configuration.ELASTICSEARCH_INTEGRATION, required=True)
        url = integration[Configuration.URL]
        self.log = logging.getLogger("External search index")
        self.works_index = works_index or integration[
            Configuration.ELASTICSEARCH_INDEX_KEY]
        use_ssl = url and url.startswith('https://')
        self.log.info("Connecting to Elasticsearch cluster at %s", url)
        super(ExternalSearchIndex, self).__init__(url, use_ssl=use_ssl)
        if not url and not fallback_to_dummy:
            raise Exception("Cannot connect to Elasticsearch cluster.")
        if self.works_index and not self.indices.exists(self.works_index):
            self.log.info("Creating index %s", self.works_index)
            self.indices.create(self.works_index)

    def query_works(self, query_string, media, languages, exclude_languages, fiction, audience,
                    age_range, in_any_of_these_genres=[], fields=None, limit=30):
        if not self.works_index:
            return []
        q = dict(
            filtered=dict(
                query=self.make_query(query_string),
                filter=self.make_filter(
                    media, languages, exclude_languages, fiction, audience,
                    age_range, in_any_of_these_genres),
            ),
        )
        body = dict(query=q)
        search_args = dict(
            index=self.works_index,
            body=dict(query=q),
            size=limit,
        )
        if fields is not None:
            search_args['fields'] = fields
        #print "Args looks like: %r" % args
        results = self.search(**search_args)
        #print "Results: %r" % results
        return results

    def make_query(self, query_string):

        def make_match_query(query_string, fields):
            return {
                'multi_match': {
                    'query': query_string,
                    'fields': fields,
                    'type': 'best_fields'
                }
            }

        main_fields = ['title^4', 'author^4', 'subtitle^3']

        # Find results that match the full query string in one of the main
        # fields.
        match_full_query = make_match_query(query_string, main_fields)
        must_match_options = [match_full_query]


        # If fiction or genre is in the query, results can match the fiction or 
        # genre value and the remaining words in the query string, instead of the
        # full query.

        fiction = None
        if re.compile(r"\bnonfiction\b", re.IGNORECASE).search(query_string):
            fiction = "Nonfiction"
        elif re.compile(r"\bfiction\b", re.IGNORECASE).search(query_string):
            fiction = "Fiction"
        
        # Get the genre and the words in the query that matched it, if any
        genre, genre_match = KeywordBasedClassifier.genre_match(query_string)

        # Get the audience and the words in the query that matched it, if any
        audience, audience_match = KeywordBasedClassifier.audience_match(query_string)

        if fiction or genre or audience:
            genre_audience_and_fiction_queries = []
            remaining_string = query_string

            # If the match was "children" and the query string was "children's",
            # we want to remove the "'s" as well as the match. We want to remove
            # everything up to the next word boundary that's not an apostrophe
            # or a dash.
            word_boundary_pattern = r"\b%s[\w'\-]*\b"

            # For children's, it could be the parenting genre or the children audience,
            # so only one of genre and audience must match.
            if genre and audience and (audience_match in genre_match or genre_match in audience_match):
                match_genre = make_match_query(genre.name, ['classifications.name'])
                match_audience = make_match_query(audience.replace(" ", ""), ['audience'])
                genre_or_audience_query = {
                    'bool': {
                        'should' : [
                            match_genre,
                            match_audience,
                        ],
                        'minimum_should_match': 1
                    }
                }
                genre_audience_and_fiction_queries.append(genre_or_audience_query)
                remaining_string = re.compile(word_boundary_pattern % genre_match, re.IGNORECASE).sub("", remaining_string)
                remaining_string = re.compile(word_boundary_pattern % audience_match, re.IGNORECASE).sub("", remaining_string)

            else:
                if genre:
                    match_genre = make_match_query(genre.name, ['classifications.name'])
                    genre_audience_and_fiction_queries.append(match_genre)
                    remaining_string = re.compile(word_boundary_pattern % genre_match, re.IGNORECASE).sub("", remaining_string)

                if audience:
                    match_audience = make_match_query(audience.replace(" ", ""), ['audience'])
                    genre_audience_and_fiction_queries.append(match_audience)
                    remaining_string = re.compile(word_boundary_pattern % audience_match, re.IGNORECASE).sub("", remaining_string)

            if fiction:
                match_fiction = make_match_query(fiction, ['fiction'])
                genre_audience_and_fiction_queries.append(match_fiction)
                remaining_string = re.compile(word_boundary_pattern % fiction, re.IGNORECASE).sub("", remaining_string)

            if len(remaining_string.strip()) > 0:
                # Someone who searches by genre is probably not looking for a specific book,
                # so title isn't included, but they might be looking for an author (eg, 
                # "science fiction iain banks").
                match_rest_of_query = make_match_query(remaining_string, ["author^4", "subtitle^3", "summary^5"])
                genre_audience_and_fiction_queries.append(match_rest_of_query)
            
            # If genre, audience, fiction, and the remaining string all match, the result will
            # have a higher score than results that match the full query in one of the 
            # main fields.
            match_genre_and_rest_of_query = {
                'bool': {
                    'must': genre_audience_and_fiction_queries,
                    'boost': 20.0
                }
            }

            must_match_options.append(match_genre_and_rest_of_query)

        # Results must match either the full query or the genre/fiction query.
        must_match = {
            'bool': {
                'should': must_match_options,
                'minimum_should_match': 1
            }
        }
        
        # Results don't have to match any of the secondary fields, but if they do,
        # they'll have a higher score.
        secondary_fields = ["summary^2", "publisher", "imprint"]
        match_secondary_fields = make_match_query(query_string, secondary_fields)
        
        return dict(bool=dict(must=[must_match],
                              should=[match_secondary_fields]),
        )

    def make_filter(self, media, languages, exclude_languages, fiction, audience, age_range, genres):
        def _f(s):
            if not s:
                return s
            return s.lower().replace(" ", "")

        clauses = []
        if languages:
            clauses.append(dict(terms=dict(language=languages)))
        if exclude_languages:
            clauses.append({'not': dict(terms=dict(language=exclude_languages))})
        if genres:
            genre_ids = [genre.id for genre in genres]
            clauses.append(dict(terms={"classifications.term": genre_ids}))
        if media:
            media = [_f(medium) for medium in media]
            clauses.append(dict(terms=dict(medium=media)))
        if fiction is not None:
            value = "fiction" if fiction == True else "nonfiction"
            clauses.append(dict(term=dict(fiction=value)))
        if audience:
            if isinstance(audience, list):
                audience = [_f(aud) for aud in audience]
                clauses.append(dict(terms=dict(audience=audience)))
        if age_range:
            lower = age_range[0]
            upper = age_range[-1]

            age_clause = {
                "and": [
                    {"range": {"target_age.upper": {"gte": lower}}},
                    {"range": {"target_age.lower": {"lte": upper}}},
                ]
            }
            clauses.append(age_clause)
        if len(clauses) > 0:
            return {'and': clauses}
        else:
            return {}


class DummyExternalSearchIndex(object):

    work_document_type = 'work-type'

    def __init__(self, url=None):
        self.url = url
        self.docs = {}
        self.works_index = "works"

    def index(self, index, doc_type, id, body):
        self.docs[(index, doc_type, id)] = body

    def delete(self, index, doc_type, id):
        key = (index, doc_type, id)
        if key in self.docs:
            del self.docs[key]

    def exists(self, index, doc_type, id):
        return id in self.docs

    def query_works(self, *args, **kwargs):
        doc_ids = [dict(_id=key[2]) for key in self.docs.keys()]
        return { "hits" : { "hits" : doc_ids }}

