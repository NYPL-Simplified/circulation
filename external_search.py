from nose.tools import set_trace
from elasticsearch import Elasticsearch
from config import Configuration
import os
import logging

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
                    in_any_of_these_genres=[], fields=None, limit=30):
        if not self.works_index:
            return []
        q = dict(
            filtered=dict(
                query=self.make_query(query_string),
                filter=self.make_filter(
                    media, languages, exclude_languages, fiction, audience,
                    in_any_of_these_genres),
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
        must_multi_match = dict(
            multi_match=dict(
            query=query_string,
            fields=["title^4", "author^4", "subtitle^3"],
            type="best_fields"
        ))
        should_multi_match = dict(
            multi_match = dict(
            query=query_string,
            fields=["summary^2", "publisher", "imprint"],
            type="best_fields"
        ))
        return dict(bool=dict(must=[must_multi_match],
                              should=[should_multi_match]),
        )

    def make_filter(self, media, languages, exclude_languages, fiction, audience, genres):
        def _f(s):
            if not s:
                return s
            return s.lower().replace(" ", "")

        clauses = []
        if languages:
            clauses.append({'or': [dict(term=dict(language=language)) for language in languages]})
        if exclude_languages:
            for language in exclude_languages:
                clauses.append({'not': dict(term=dict(language=language))})
        if genres:
            genre_ids = [genre.id for genre in genres]
            clauses.append(dict(terms={"classifications.term" : genre_ids}))
        if media:
            media = [_f(medium) for medium in media]
            clauses.append({'or': [dict(term=dict(medium=medium)) for medium in media]})
        if fiction is not None:
            value = "fiction" if fiction == True else "nonfiction"
            clauses.append(dict(term=dict(fiction=value)))
        if audience:
            if isinstance(audience, list):
                audience = [_f(aud) for aud in audience]
                clauses.append({'or': [dict(term=dict(audience=aud)) for aud in audience]})
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
