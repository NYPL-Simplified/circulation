from nose.tools import (
    set_trace,
    eq_,
)

from model import Genre
from opensearch import OpenSearchDocument
from lane import Lane
from . import DatabaseTest

class TestOpenSearchDocument(DatabaseTest):

    def test_search_info(self):
        # Create one lane inside another.
        lane = self._lane()
        lane.display_name = "This & That"

        sublane = self._lane(parent=lane)
        sublane.display_name = "Science Fiction"

        # Both lanes are searchable.

        info = OpenSearchDocument.search_info(lane)
        eq_("Search", info['name'])
        eq_("Search This &amp; That", info['description'])
        eq_("this-&amp;-that", info['tags'])

        info = OpenSearchDocument.search_info(sublane)
        eq_("Search", info['name'])
        eq_("Search Science Fiction", info['description'])
        eq_("science-fiction", info['tags'])

        # Make the sublane unsearchable by restricting it to a 
        # specific genre.
        sf, ignore = Genre.lookup(self._db, "Science Fiction")
        sublane.genres.append(sf)

        info = OpenSearchDocument.search_info(sublane)
        eq_("Search", info['name'])
        eq_("Search This &amp; That", info['description'])
        eq_("this-&amp;-that", info['tags'])

        # Return the sublane to searchability by setting it as the
        # root lane for a certain patron type.
        sublane.root_for_patron_type = ['A']

        info = OpenSearchDocument.search_info(sublane)
        eq_("Search", info['name'])
        eq_("Search Science Fiction", info['description'])
        eq_("science-fiction", info['tags'])


    
