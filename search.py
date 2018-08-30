from pdb import set_trace
import urllib
from core.model import (
    production_session,
    Library,
)
from core.external_search import (
    ExternalSearchIndex
)

class Searcher(object):
    def __init__(self, query_part, filter_part):
        self.query_part = query_part
        self.filter_part = filter_part
        if filter_part:
            self.full_query = self.query_part + " " + self.filter_part
        else:
            self.full_query = self.query_part

        _db = production_session()
        self.library = Library.default(_db)
        self.index = ExternalSearchIndex(_db)

    def rate(self, result):
        """Give an approximate letter grade to a search result.

        This is not precise, but in general results should go from better
        grades to worse grades.
        """
        def scrub(x):
            if not x:
                return x
            return x.lower().replace(" ", "")
        query_part = scrub(self.query_part)
        filter_part = scrub(self.filter_part)
        full_query = scrub(self.full_query)

        def m_(result, exact=False):
            if not result:
                return False
            result = scrub(result)
            if exact:
                return query_part ==result or filter_part == result or full_query == result
            else:
                return (query_part and query_part in result) or (filter_part and filter_part in result) or (full_query and full_query in result)

        rating = "F"

        genres = [x.get('name', '') for x in result.get('genres', []) or []]
        if m_(result.get('title')) or m_(result.get('author')):
            rating = "A"
        elif m_(result.get('subtitle')) or m_(result.get('series')):
            rating = 'B'
        elif m_(result.get('summary')):
            rating = "C"
        elif ('classifications' in result and result['classifications'] and any(
                m_(x['term'], False) for x in result['classifications']
        )):
            rating = "D+"
        elif genres:
            if any(m_(x, False) for x in genres):
                rating = "D"
        return rating

    def render(self, results, limit=50):
        a = 0
        for result in results['hits']['hits'][0:limit]:
            result = result['_source']
            rating = self.rate(result)
            out = '%3d %s "%s" (%s) by %s %s' % (a, rating, result['title'], result.get('subtitle', ''), result.get('author', ''), result.get('series', ''))
            print out.encode("utf8")
            a += 1

    def run_query(self, x):
        results = self.index.query_works(self.library, x, None, None, None, None, None)
        return results

    def execute(self):
        if self.filter_part:
            print "Searching for %s, and secretly looking for %s" % (self.query_part, self.filter_part)
            results = self.run_query(self.query_part)
            self.render(results)
            print

        print "Searching for %s" % (self.full_query)
        results = self.run_query(self.full_query)
        self.render(results)

import sys
query_part = sys.argv[1]
query_part = urllib.unquote_plus(query_part).decode("utf8")
filter_part = ""
if len(sys.argv) > 2:
    filter_part = sys.argv[2]
Searcher(query_part, filter_part).execute()
