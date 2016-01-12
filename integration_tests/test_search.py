from nose.tools import (
    set_trace,
    eq_,
)
from urllib import urlopen
import feedparser

from . import CirculationIntegrationTest

class TestSearch(CirculationIntegrationTest):

    def test_search(self):
        search_url = "%ssearch/?q=book" % self.url
        feed = urlopen(search_url).read()
        feed = feedparser.parse(unicode(feed))
        entries = feed['entries']

        # there should be some entries
        assert len(entries) > 20

        # spot-check an entry
        entry = entries[5]
        assert len(entry.get('title')) > 0
        assert len(entry.get('author')) > 0
        assert len(entry.get('links')) > 0
