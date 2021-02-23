from ..model import Genre
from ..classifier import Classifier
from ..opensearch import OpenSearchDocument
from ..lane import Lane
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
        assert "Search" == info['name']
        assert "Search English/Deutsch Young Adult" == info['description']
        assert "english/deutsch-young-adult" == info['tags']

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
        assert "Search" == info['name']
        assert "Search Science Fiction & Fantasy" == info['description']
        assert "science-fiction-&-fantasy" == info['tags']

    def test_escape_entities(self):
        """Verify that escape_entities properly escapes ampersands."""
        d = dict(k1="a", k2="b & c")
        expect = dict(k1="a", k2="b &amp; c")
        assert expect == OpenSearchDocument.escape_entities(d)

    def test_url_template(self):
        """Verify that url_template generates sensible URL templates."""
        m = OpenSearchDocument.url_template
        assert "http://url/?q={searchTerms}" == m("http://url/")
        assert "http://url/?key=val&q={searchTerms}" == m("http://url/?key=val")

    def test_for_lane(self):

        class Mock(OpenSearchDocument):
            """Mock methods called by for_lane."""
            @classmethod
            def search_info(cls, lane):
                return dict(
                    name="sf & fantasy",
                    description="description & stuff",
                    tags="sf-&-fantasy, tag2",
                )

            @classmethod
            def url_template(cls, base_url):
                return "http://template?key1=val1&key2=val2"

        # Here's the search document.
        doc = Mock.for_lane(object(), object())

        # It's just the result of calling search_info() and url_template(),
        # and using the resulting dict as arguments into TEMPLATE.
        expect = Mock.search_info(object())
        expect['url_template'] = Mock.url_template(object())
        expect = Mock.escape_entities(expect)
        assert Mock.TEMPLATE % expect == doc
