from nose.tools import set_trace
from elasticsearch import Elasticsearch
from config import Configuration
from classifier import (
    KeywordBasedClassifier,
    GradeLevelClassifier,
    AgeClassifier,
)
import os
import logging
import re

class ExternalSearchIndex(Elasticsearch):
    
    work_document_type = 'work-type'

    def __init__(self, url=None, works_index=None):
    
        integration = Configuration.integration(
            Configuration.ELASTICSEARCH_INTEGRATION, 
        )
        self.log = logging.getLogger("External search index")
        self.works_index = works_index or integration.get(
            Configuration.ELASTICSEARCH_INDEX_KEY
        ) or None

        if not integration:
            return

        url = integration[Configuration.URL]
        use_ssl = url and url.startswith('https://')
        self.log.info("Connecting to Elasticsearch cluster at %s", url)
        super(ExternalSearchIndex, self).__init__(url, use_ssl=use_ssl)
        if not url:
            raise Exception("Cannot connect to Elasticsearch cluster.")
        if self.works_index and not self.indices.exists(self.works_index):
            self.log.info("Creating index %s", self.works_index)
            self.indices.create(self.works_index)

    def query_works(self, query_string, media, languages, exclude_languages, fiction, audience,
                    age_range, in_any_of_these_genres=[], fields=None, limit=30):
        if not self.works_index:
            return []
        filter = self.make_filter(
            media, languages, exclude_languages, fiction, audience,
            age_range, in_any_of_these_genres
        )
        q = dict(
            filtered=dict(
                query=self.make_query(query_string),
                filter=filter,
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

        def make_target_age_query(target_age):
            (lower, upper) = target_age
            return { 
                "bool" : {
                    # There must be some overlap with the range in the query
                    "must": [
                       {"range": {"target_age.upper": {"gte": lower}}},
                       {"range": {"target_age.lower": {"lte": upper}}},
                     ], 
                    # Results with ranges closer to the query are better
                    # e.g. for query 4-6, a result with 5-6 beats 6-7
                    "should": [
                       {"range": {"target_age.upper": {"lte": upper}}},
                       {"range": {"target_age.lower": {"gte": lower}}},
                     ], 
                    "boost": 40
                }
            }

        main_fields = ['title^4', 'author^4', "series^4", 'subtitle^3', 'summary']

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

        # Get the grade level and the words in the query that matched it, if any
        age_from_grade, grade_match = GradeLevelClassifier.target_age_match(query_string)
        if age_from_grade and age_from_grade[0] == None:
            age_from_grade = None

        # Get the age range and the words in the query that matched it, if any
        age, age_match = AgeClassifier.target_age_match(query_string)
        if age and age[0] == None:
            age = None

        if fiction or genre or audience or age_from_grade or age:
            remaining_string = query_string
            classification_queries = []

            def without_match(original_string, match):
                # If the match was "children" and the query string was "children's",
                # we want to remove the "'s" as well as the match. We want to remove
                # everything up to the next word boundary that's not an apostrophe
                # or a dash.
                word_boundary_pattern = r"\b%s[\w'\-]*\b"

                return re.compile(word_boundary_pattern % match, re.IGNORECASE).sub("", original_string)

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
                classification_queries.append(genre_or_audience_query)
                remaining_string = without_match(remaining_string, genre_match)
                remaining_string = without_match(remaining_string, audience_match)

            else:
                if genre:
                    match_genre = make_match_query(genre.name, ['classifications.name'])
                    classification_queries.append(match_genre)
                    remaining_string = without_match(remaining_string, genre_match)

                if audience:
                    match_audience = make_match_query(audience.replace(" ", ""), ['audience'])
                    classification_queries.append(match_audience)
                    remaining_string = without_match(remaining_string, audience_match)

            if fiction:
                match_fiction = make_match_query(fiction, ['fiction'])
                classification_queries.append(match_fiction)
                remaining_string = without_match(remaining_string, fiction)

            if age_from_grade:
                match_age_from_grade = make_target_age_query(age_from_grade)
                classification_queries.append(match_age_from_grade)
                remaining_string = without_match(remaining_string, grade_match)

            if age:
                match_age = make_target_age_query(age)
                classification_queries.append(match_age)
                remaining_string = without_match(remaining_string, age_match)

            if len(remaining_string.strip()) > 0:
                # Someone who searches by genre is probably not looking for a specific book,
                # but they might be looking for an author (eg, "science fiction iain banks").
                # However, it's possible that they're searching for a subject that's not
                # mentioned in the summary (eg, a person's name in a biography). So title
                # is a possible match, but is less important than author, subtitle, and summary.
                match_rest_of_query = make_match_query(remaining_string, ["author^4", "subtitle^3", "summary^5", "title^1", "series^1"])
                classification_queries.append(match_rest_of_query)
            
            # If classification queries and the remaining string all match, the result will
            # have a higher score than results that match the full query in one of the 
            # main fields.
            match_classification_and_rest_of_query = {
                'bool': {
                    'must': classification_queries,
                    'boost': 20.0
                }
            }

            must_match_options.append(match_classification_and_rest_of_query)

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
                    { 
                        "or" : [
                            {"range": {"target_age.upper": {"gte": lower}}},
                            {
                                "bool": {
                                    "must_not" : {
                                        "exists": {"field" : "target_age.upper"}
                                    }
                                }
                            }
                        ]
                    },
                    {
                        "or" : [
                            {"range": {"target_age.lower": {"lte": upper}}},
                            {
                                "bool": {
                                    "must_not" : {
                                        "exists": {"field" : "target_age.lower"}
                                    }
                                }
                            }
                        ]
                    }
                ]
            }
            clauses.append(age_clause)
        if len(clauses) > 0:
            return {'and': clauses}
        else:
            return {}


class DummyExternalSearchIndex(ExternalSearchIndex):

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

