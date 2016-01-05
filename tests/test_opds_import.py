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

from opds_import import (
    OPDSImporter,
)
from metadata_layer import (
    LinkData
)
from model import (
    Contributor,
    DataSource,
    DeliveryMechanism,
    Hyperlink,
    Edition,
    Measurement,
    Representation,
    Subject,
)

class TestOPDSImporter(DatabaseTest):

    def setup(self):
        super(TestOPDSImporter, self).setup()
        base_path = os.path.split(__file__)[0]
        self.resource_path = os.path.join(base_path, "files", "opds")
        self.content_server_feed = open(
            os.path.join(self.resource_path, "content_server.opds")).read()
        self.content_server_mini_feed = open(
            os.path.join(self.resource_path, "content_server_mini.opds")).read()

    def test_extract_metadata(self):

        importer = OPDSImporter(self._db, DataSource.NYT)
        data, status_messages = importer.extract_metadata(
            self.content_server_mini_feed
        )
        m1, m2 = sorted(data, key=lambda x:x.title)
        eq_("The Green Mouse", m2.title)
        eq_("A Tale of Mousy Terror", m2.subtitle)

        # This entry specified a data source, which was passed along.
        eq_(DataSource.GUTENBERG, m2._data_source)

        # This entry didn't specify a data source, so the default was used.
        eq_(DataSource.NYT, m1._data_source)

        [message] = status_messages.values()
        eq_(202, message.status_code)
        eq_(u"I'm working to locate a source for this identifier.", message.message)


    def test_extract_metadata_from_feedparser(self):

        data, status_messages = OPDSImporter.extract_metadata_from_feedparser(
            self.content_server_mini_feed
        )        

        metadata = data['urn:librarysimplified.org/terms/id/Gutenberg%20ID/10441']
        eq_("The Green Mouse", metadata['title'])
        eq_("A Tale of Mousy Terror", metadata['subtitle'])
        eq_('en', metadata['language'])
        eq_('Project Gutenberg', metadata['publisher'])
        eq_(DataSource.GUTENBERG, metadata['data_source'])

        message = status_messages['http://www.gutenberg.org/ebooks/1984']
        eq_(202, message.status_code)
        eq_(u"I'm working to locate a source for this identifier.", message.message)

    def test_extract_metadata_from_elementtree(self):

        data = OPDSImporter.extract_metadata_from_elementtree(
            self.content_server_feed
        )

        # There are 76 entries in the feed, and we got metadata for
        # every one of them.
        eq_(76, len(data))

        # We're going to do spot checks on a book and a periodical.

        # First, the book.
        book_id = 'urn:librarysimplified.org/terms/id/Gutenberg%20ID/1022'
        book = data[book_id]
        eq_(Edition.BOOK_MEDIUM, book['medium'])

        [contributor] = book['contributors']
        eq_("Thoreau, Henry David", contributor.sort_name)
        eq_([Contributor.AUTHOR_ROLE], contributor.roles)

        subjects = book['subjects']
        eq_(['LCSH', 'LCSH', 'LCSH', 'LCC'], [x.type for x in subjects])
        eq_(
            ['Essays', 'Nature', 'Walking', 'PS'],
            [x.identifier for x in subjects]
        )
        eq_(
            [None, None, None, 'American Literature'],
            [x.name for x in book['subjects']]
        )
        eq_(
            [1, 1, 1, 10],
            [x.weight for x in book['subjects']]
        )

        eq_([], book['measurements'])

        [link] = book['links']
        eq_(Hyperlink.OPEN_ACCESS_DOWNLOAD, link.rel)
        eq_("http://www.gutenberg.org/ebooks/1022.epub.noimages", link.href)
        eq_(Representation.EPUB_MEDIA_TYPE, link.media_type)

        # And now, the periodical.
        periodical_id = 'urn:librarysimplified.org/terms/id/Gutenberg%20ID/10441'
        periodical = data[periodical_id]
        eq_(Edition.PERIODICAL_MEDIUM, periodical['medium'])

        subjects = periodical['subjects']
        eq_(
            ['LCSH', 'LCSH', 'LCSH', 'LCSH', 'LCC', 'schema:audience', 'schema:typicalAgeRange'], 
            [x.type for x in subjects]
        )
        eq_(
            ['Courtship -- Fiction', 'New York (N.Y.) -- Fiction', 'Fantasy fiction', 'Magic -- Fiction', 'PZ', 'Children', '7'],
            [x.identifier for x in subjects]
        )
        eq_([1, 1, 1, 1, 1, 100, 100], [x.weight for x in subjects])
        
        r1, r2 = periodical['measurements']

        eq_(Measurement.QUALITY, r1.quantity_measured)
        eq_(0.3333, r1.value)
        eq_(1, r1.weight)

        eq_(Measurement.POPULARITY, r2.quantity_measured)
        eq_(0.25, r2.value)
        eq_(1, r2.weight)


    def test_import(self):
        path = os.path.join(self.resource_path, "content_server_mini.opds")
        feed = open(path).read()
        imported, messages = OPDSImporter(self._db).import_from_feed(feed)

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

        # Bonus: make sure that delivery mechanisms are set appropriately.
        [mech] = imported[0].license_pool.delivery_mechanisms
        eq_(Representation.EPUB_MEDIA_TYPE, mech.delivery_mechanism.content_type)
        eq_(DeliveryMechanism.NO_DRM, mech.delivery_mechanism.drm_scheme)
        eq_('http://www.gutenberg.org/ebooks/10441.epub.images', 
            mech.resource.url)

    def test_status_and_message(self):
        path = os.path.join(self.resource_path, "unrecognized_identifier.opds")
        feed = open(path).read()
        imported, messages = OPDSImporter(self._db).import_from_feed(feed)
        [message] = messages.values()
        eq_(404, message.status_code)
        eq_("I've never heard of this work.", message.message)

    def test_consolidate_links(self):

        links = [LinkData(href=self._url, rel=rel, media_type="image/jpeg")
                 for rel in [Hyperlink.OPEN_ACCESS_DOWNLOAD,
                             Hyperlink.IMAGE,
                             Hyperlink.THUMBNAIL_IMAGE,
                             Hyperlink.OPEN_ACCESS_DOWNLOAD]
        ]
        old_link = links[2]
        links = OPDSImporter.consolidate_links(links)
        eq_([Hyperlink.OPEN_ACCESS_DOWNLOAD,
             Hyperlink.IMAGE,
             Hyperlink.OPEN_ACCESS_DOWNLOAD], [x.rel for x in links])
        link = links[1]
        eq_(old_link, link.thumbnail)
