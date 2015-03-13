import os
from StringIO import StringIO
from nose.tools import (
    set_trace,
    eq_,
)
import feedparser

from lxml import etree
import pkgutil

from . import (
    DatabaseTest,
)

from opds_import import (
    DetailedOPDSImporter,
)

class TestDetailedOPDSImporter(DatabaseTest):

    def setup(self):
        super(TestDetailedOPDSImporter, self).setup()
        base_path = os.path.split(__file__)[0]
        self.resource_path = os.path.join(base_path, "files", "opds")
        self.content_server_feed = open(
            os.path.join(self.resource_path, "content_server.opds")).read()

    def test_authors_by_id(self):

        parsed = etree.parse(StringIO(self.content_server_feed))
        contributors_by_id, subject_names, subject_weights = DetailedOPDSImporter.authors_and_subjects_by_id(self._db, parsed)
        eq_(70, len(contributors_by_id))
        spot_check_id = 'urn:librarysimplified.org/terms/id/Gutenberg%20ID/1022'
        [contributor] = contributors_by_id[spot_check_id]
        eq_("Thoreau, Henry David", contributor.name)
        eq_(None, contributor.display_name)

        names = subject_names[spot_check_id]
        eq_("American Literature", names[('LCC', 'PS')])
        weights = subject_weights[spot_check_id]
        eq_(10, weights[('LCC', 'PS')])

    def test_status_and_message(self):
        path = os.path.join(self.resource_path, "unrecognized_identifier.opds")
        feed = open(path).read()
        imported, messages = DetailedOPDSImporter(
            self._db, feed).import_from_feed()
        [[status_code, message]] = messages.values()
        eq_(404, status_code)
        eq_("I've never heard of this work.", message)
