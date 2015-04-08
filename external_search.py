from elasticsearch import Elasticsearch
import os

class ExternalSearchIndex(Elasticsearch):
    
    def __init__(self, url=None):
        url = url or os.environ['SEARCH_SERVER_URL']
        use_ssl = url.startswith('https://')
        super(ExternalSearchIndex, self).__init__(url, use_ssl=use_ssl)


class DummyExternalSearchIndex(object):

    def __init__(self, url=None):
        self.url = url
        self.docs = []

    def index(self, *args, **kwargs):
        self.docs.append((args, kwargs))
