from nose.tools import (
    set_trace,
    eq_,
)

from opensearch import OpenSearchDocument
from lane import Lane
from . import DatabaseTest

class TestOpenSearchDocument(DatabaseTest):

    def test_search_info(self):
        sublane = Lane(self._db, self._default_library, "Sublane")

        lane = Lane(self._db, self._default_library, "Lane", sublanes=[sublane])

        # Neither lane is searchable yet.

        info = OpenSearchDocument.search_info(lane)
        eq_("Search", info['name'])
        eq_("Search", info['description'])
        eq_("", info['tags'])

        info = OpenSearchDocument.search_info(sublane)
        eq_("Search", info['name'])
        eq_("Search", info['description'])
        eq_("", info['tags'])

        # Make the parent lane searchable.

        lane.searchable = True

        info = OpenSearchDocument.search_info(lane)
        eq_("Search", info['name'])
        eq_("Search Lane", info['description'])
        eq_("lane", info['tags'])

        info = OpenSearchDocument.search_info(sublane)
        eq_("Search", info['name'])
        eq_("Search Lane", info['description'])
        eq_("lane", info['tags'])

        # Make the sublane searchable.

        sublane.searchable = True

        info = OpenSearchDocument.search_info(lane)
        eq_("Search", info['name'])
        eq_("Search Lane", info['description'])
        eq_("lane", info['tags'])

        info = OpenSearchDocument.search_info(sublane)
        eq_("Search", info['name'])
        eq_("Search Sublane", info['description'])
        eq_("sublane", info['tags'])


    
