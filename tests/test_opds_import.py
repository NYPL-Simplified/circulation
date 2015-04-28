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
from model import (
    DataSource,
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
        contributors_by_id, subject_names, subject_weights, ratings_by_id = DetailedOPDSImporter.authors_and_subjects_by_id(self._db, parsed)
        eq_(70, len(contributors_by_id))
        spot_check_id = 'urn:librarysimplified.org/terms/id/Gutenberg%20ID/1022'
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
        path = os.path.join(self.resource_path, "content_server.opds")
        feed = open(path).read()
        imported, messages = DetailedOPDSImporter(
            self._db, feed).import_from_feed()

        eq_(DataSource.GUTENBERG, imported[0].data_source.name)

        [has_measurements] = [
            x for x in imported if x.primary_identifier.measurements]
        pop, qual, rating = sorted(
            has_measurements.primary_identifier.measurements,
            key=lambda x: x.quantity_measured)
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
        no_measurements = [
            x for x in imported if not x.primary_identifier.measurements][0]
        eq_([], x.primary_identifier.measurements)

        seven, children, courtship, fantasy, magic, new_york, pz = sorted(
            has_measurements.primary_identifier.classifications,
            key=lambda x: x.subject.identifier)
        eq_('7', seven.subject.identifier)
        eq_(Subject.AGE_RANGE, seven.subject.type)
        from classifier import Classifier
        classifier = Classifier.classifiers.get(seven.subject.type, None)
        classifier.classify(seven.subject)
        work = has_measurements.work
        work.calculate_presentation()
        eq_(0.41415, work.quality)
        eq_(Classifier.AUDIENCE_CHILDREN, work.audience)
        eq_(7, work.target_age)

    def test_status_and_message(self):
        path = os.path.join(self.resource_path, "unrecognized_identifier.opds")
        feed = open(path).read()
        imported, messages = DetailedOPDSImporter(
            self._db, feed).import_from_feed()
        [[status_code, message]] = messages.values()
        eq_(404, status_code)
        eq_("I've never heard of this work.", message)
