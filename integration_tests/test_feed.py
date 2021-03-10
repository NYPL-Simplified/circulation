from nose.tools import (
    set_trace,
    eq_,
)
from urllib.request import urlopen
import feedparser
import os

from . import CirculationIntegrationTest

class TestFeed(CirculationIntegrationTest):

    def test_grouped_feed(self):
        feed_url = self.url
        feed = urlopen(feed_url).read()
        feed = feedparser.parse(str(feed))
        entries = feed['entries']
        assert len(entries) > 20
        # spot-check an entry
        entry = entries[5]
        assert len(entry.get('title')) > 0
        assert len(entry.get('author')) > 0
        links = entry.get('links')
        assert len(links) > 0
        # books on the first page should be available to borrow
        borrow_links = [link for link in links if link.rel == "http://opds-spec.org/acquisition/borrow"]
        eq_(1, len(borrow_links))

    def test_genre_feed(self):
        if 'TEST_FEED_PATH' in os.environ:
            path = os.environ['TEST_FEED_PATH']
        else:
            path = "eng/Romance"
        feed_url = "%sfeed/%s" % (self.url, path)
        feed = urlopen(feed_url).read()
        feed = feedparser.parse(str(feed))
        entries = feed['entries']
        assert len(entries) > 20
        # spot-check an entry
        entry = entries[5]
        assert len(entry.get('title')) > 0
        assert len(entry.get('author')) > 0
        links = entry.get('links')
        assert len(links) > 0

