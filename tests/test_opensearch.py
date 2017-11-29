from nose.tools import (
    set_trace,
    eq_,
)

from model import Genre
from classifier import Classifier
from opensearch import OpenSearchDocument
from lane import Lane
from . import DatabaseTest

class TestOpenSearchDocument(DatabaseTest):

    def test_search_info(self):
        # Searching this lane will use the language
        # and audience restrictions from the lane.
        lane = self._lane()
        lane.display_name = "Fiction"
        lane.languages = ["eng", "ger"]
        lane.audiences = [Classifier.AUDIENCE_YOUNG_ADULT]
        lane.fiction = True

        info = OpenSearchDocument.search_info(lane)
        eq_("Search", info['name'])
        eq_("Search English/Deutsch Young Adult", info['description'])
        eq_("english/deutsch-young-adult", info['tags'])

        # This lane is the root for a patron type, so searching
        # it will use all the lane's restrictions.
        root_lane = self._lane()
        root_lane.root_for_patron_type = ['A']
        root_lane.display_name = "Science Fiction & Fantasy"
        sf, ignore = Genre.lookup(self._db, "Science Fiction")
        fantasy, ignore = Genre.lookup(self._db, "Fantasy")
        root_lane.add_genre(sf)
        root_lane.add_genre(fantasy)

        info = OpenSearchDocument.search_info(root_lane)
        eq_("Search", info['name'])
        eq_("Search Science Fiction &amp; Fantasy", info['description'])
        eq_("science-fiction-&amp;-fantasy", info['tags'])
    
