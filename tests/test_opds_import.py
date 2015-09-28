import os
from StringIO import StringIO
from nose.tools import (
    set_trace,
    eq_,
)
import feedparser

from lxml import etree
import pkgutil
from psycopg2.extras import NumericRange
from . import (
    DatabaseTest,
)

from ..opds_import import (
    DetailedOPDSImporter,
)
from ..model import (
    DataSource,
    Edition,
    Measurement,
    Subject,
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
        medium_by_id, contributors_by_id, subject_names, subject_weights, ratings_by_id = DetailedOPDSImporter.authors_and_subjects_by_id(self._db, parsed)

        book_id = 'urn:librarysimplified.org/terms/id/Gutenberg%20ID/1022'
        periodical_id = 'urn:librarysimplified.org/terms/id/Gutenberg%20ID/10441'  
        eq_(76, len(medium_by_id))
        eq_(Edition.BOOK_MEDIUM, medium_by_id[book_id])
        eq_(Edition.PERIODICAL_MEDIUM, medium_by_id[periodical_id])

        spot_check_id = 'urn:librarysimplified.org/terms/id/Gutenberg%20ID/1022'
        eq_(70, len(contributors_by_id))
        [contributor] = contributors_by_id[spot_check_id]
        eq_("Thoreau, Henry David", contributor.name)
        eq_(None, contributor.display_name)

        names = subject_names[spot_check_id]
        eq_("American Literature", names[('LCC', 'PS')])
        weights = subject_weights[spot_check_id]
        eq_(10, weights[('LCC', 'PS')])

        has_ratings_id = 'urn:librarysimplified.org/terms/id/Gutenberg%20ID/10441'
        has_no_ratings_id = 'urn:librarysimplified.org/terms/id/Gutenberg%20ID/1022'
        eq_({}, ratings_by_id[has_no_ratings_id])
        ratings = ratings_by_id[has_ratings_id]
        eq_(0.6, ratings[None])
        eq_(0.25, ratings[Measurement.POPULARITY])
        eq_(0.3333, ratings[Measurement.QUALITY])

    def test_ratings_become_measurements(self):
        path = os.path.join(self.resource_path, "content_server_mini.opds")
        feed = open(path).read()
        imported, messages = DetailedOPDSImporter(
            self._db, feed).import_from_feed()

        eq_(DataSource.GUTENBERG, imported[0].data_source.name)
        eq_(Edition.PERIODICAL_MEDIUM, imported[0].medium)
        eq_(Edition.BOOK_MEDIUM, imported[1].medium)

        [has_rating, no_rating] = imported
        num_editions, pop, qual, rating = sorted(
            [x for x in has_rating.primary_identifier.measurements
             if x.is_most_recent],
            key=lambda x: x.quantity_measured)
        eq_(DataSource.OCLC_LINKED_DATA, num_editions.data_source.name)
        eq_(Measurement.PUBLISHED_EDITIONS, num_editions.quantity_measured)
        eq_(1, num_editions.value)

        eq_(DataSource.METADATA_WRANGLER, pop.data_source.name)
        eq_(Measurement.POPULARITY, pop.quantity_measured)
        eq_(0.25, pop.value)

        eq_(DataSource.METADATA_WRANGLER, qual.data_source.name)
        eq_(Measurement.QUALITY, qual.quantity_measured)
        eq_(0.3333, qual.value)

        eq_(DataSource.METADATA_WRANGLER, rating.data_source.name)
        eq_(Measurement.RATING, rating.quantity_measured)
        eq_(0.6, rating.value)

        # Not every imported edition has measurements.
        #no_measurements = [
        #    x for x in imported if not x.primary_identifier.measurements][0]
        #eq_([], x.primary_identifier.measurements)

        seven, children, courtship, fantasy, pz, magic, new_york = sorted(
            has_rating.primary_identifier.classifications,
            key=lambda x: x.subject.name)

        pz_s = pz.subject
        eq_("Juvenile Fiction", pz_s.name)
        eq_("PZ", pz_s.identifier)

        new_york_s = new_york.subject
        eq_("New York (N.Y.) -- Fiction", new_york_s.name)
        eq_("sh2008108377", new_york_s.identifier)

        eq_('7', seven.subject.identifier)
        eq_(100, seven.weight)
        eq_(Subject.AGE_RANGE, seven.subject.type)
        from classifier import Classifier
        classifier = Classifier.classifiers.get(seven.subject.type, None)
        classifier.classify(seven.subject)
        work = has_rating.work
        work.calculate_presentation()
        eq_(0.41415, work.quality)
        eq_(Classifier.AUDIENCE_CHILDREN, work.audience)
        eq_(NumericRange(7,7, '[]'), work.target_age)

    def test_status_and_message(self):
        path = os.path.join(self.resource_path, "unrecognized_identifier.opds")
        feed = open(path).read()
        imported, messages = DetailedOPDSImporter(
            self._db, feed).import_from_feed()
        [[status_code, message]] = messages.values()
        eq_(404, status_code)
        eq_("I've never heard of this work.", message)
