from nose.tools import set_trace
from elasticsearch import Elasticsearch
import os

class ExternalSearchIndex(Elasticsearch):
    
    work_document_type = 'work-type'
    
    def __init__(self, url=None, works_index=None):
        url = url or os.environ.get('SEARCH_SERVER_URL')
        self.works_index = works_index or os.environ.get('SEARCH_WORKS_INDEX')
        use_ssl = url and url.startswith('https://')
        super(ExternalSearchIndex, self).__init__(url, use_ssl=use_ssl)
        if self.works_index and not self.indices.exists(self.works_index):
            self.indices.create(self.works_index)

    def query_works(self, query_string, medium, languages, fiction, audience,
                    in_any_of_these_genres=[], fields=None):
        if not self.works_index:
            return []
        q = dict(
            filtered=dict(
                query=self.make_query(query_string),
                filter=self.make_filter(
                    medium, languages, fiction, audience,
                    in_any_of_these_genres),
            )
        )
        body = dict(query=q)
        args = dict(
            index=self.works_index,
            body=dict(query=q)
        )
        if fields is not None:
            args['fields'] = fields
        return self.search(**args)

    def make_query(self, query_string):
        must_multi_match = dict(
            multi_match=dict(
            query=query_string,
            fields=["title^2", "author^2", "subtitle"],
            type="best_fields"
        ))
        should_multi_match = dict(
            multi_match = dict(
            query=query_string,
            fields=["summary^2", "publisher", "imprint"],
            type="best_fields"
        ))
        return dict(bool=dict(must=[must_multi_match],
                              should=[should_multi_match]))

    def make_filter(self, medium, languages, fiction, audience, genres):
        def _f(s):
            if not s:
                return s
            return s.lower().replace(" ", "")

        clauses = []
        if languages:
            clauses.append(dict(terms=dict(language=languages)))
        if genres:
            clauses.append(dict(terms={"classifications.term" : genres}))
        if medium:
            clauses.append(dict(term=dict(medium=_f(medium))))
        if fiction is not None:
            value = "fiction" if fiction == True else "nonfiction"
            clauses.append(dict(term=dict(fiction=value)))
        if audience:
            clauses.append(dict(term=dict(audience=_f(audience))))
        return {"and" :clauses}


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
