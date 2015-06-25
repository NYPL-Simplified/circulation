from nose.tools import eq_, set_trace

import pkgutil

from axis import (
    BibliographicParser,
)

class TestParsers(object):

    def test_bibliographic_parser(self):

        data = pkgutil.get_data("tests", "files/axis/tiny_collection.xml")

        [bib1, av1], [bib2, av2] = BibliographicParser().process_all(data)

        # If we turn off the parsing of one of these pieces of info,
        # that piece is not parsed.
        eq_([(None, None), (None, None)],
            list(BibliographicParser(False, False).process_all(data)))

        set_trace()
