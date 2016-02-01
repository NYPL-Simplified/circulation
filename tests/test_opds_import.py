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
    StatusMessage,
)
from metadata_layer import (
    LinkData
)
from model import (
    Contributor,
    DataSource,
    DeliveryMechanism,
    Hyperlink,
    Identifier,
    Edition,
    Measurement,
    Representation,
    Subject,
)

class TestStatusMessage(object):

    def test_constructor(self):

        message = StatusMessage(200, "success")
        eq_(True, message.success)
        eq_(False, message.transient)

        message = StatusMessage(201, "try later")
        eq_(False, message.success)
        eq_(True, message.transient)

        message = StatusMessage(500, "oops")
        eq_(False, message.success)
        eq_(True, message.transient)

        message = StatusMessage(404, "nope")
        eq_(False, message.success)
        eq_(False, message.transient)


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
        data, status_messages, next_link = importer.extract_metadata(
            self.content_server_mini_feed
        )
        m1, m2 = sorted(data, key=lambda x:x.title)
        eq_("The Green Mouse", m2.title)
        eq_("A Tale of Mousy Terror", m2.subtitle)

        eq_(None, m1._license_data_source)
        eq_(DataSource.GUTENBERG, m2._license_data_source)

        [message] = status_messages.values()
        eq_(202, message.status_code)
        eq_(u"I'm working to locate a source for this identifier.", message.message)

        eq_("http://localhost:5000/?after=327&size=100", next_link[0])


    def test_extract_metadata_from_feedparser(self):

        data, status_messages, next_link = OPDSImporter.extract_metadata_from_feedparser(
            self.content_server_mini_feed
        )        

        metadata = data['urn:librarysimplified.org/terms/id/Gutenberg%20ID/10441']
        eq_("The Green Mouse", metadata['title'])
        eq_("A Tale of Mousy Terror", metadata['subtitle'])
        eq_('en', metadata['language'])
        eq_('Project Gutenberg', metadata['publisher'])
        eq_(DataSource.GUTENBERG, metadata['license_data_source'])

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
        
        r1, r2, r3 = periodical['measurements']

        eq_(Measurement.QUALITY, r1.quantity_measured)
        eq_(0.3333, r1.value)
        eq_(1, r1.weight)

        eq_(Measurement.RATING, r2.quantity_measured)
        eq_(0.6, r2.value)
        eq_(1, r2.weight)

        eq_(Measurement.POPULARITY, r3.quantity_measured)
        eq_(0.25, r3.value)
        eq_(1, r3.weight)


    def test_import(self):
        path = os.path.join(self.resource_path, "content_server_mini.opds")
        feed = open(path).read()
        imported, messages, next_links = OPDSImporter(self._db).import_from_feed(feed)

        [crow, mouse] = sorted(imported, key=lambda x: x.title)

        eq_(DataSource.METADATA_WRANGLER, crow.data_source.name)
        eq_(Edition.BOOK_MEDIUM, crow.medium)
        eq_(Edition.PERIODICAL_MEDIUM, mouse.medium)

        editions, popularity, quality, rating = sorted(
            [x for x in mouse.primary_identifier.measurements
             if x.is_most_recent],
            key=lambda x: x.quantity_measured)

        eq_(DataSource.OCLC_LINKED_DATA, editions.data_source.name)
        eq_(Measurement.PUBLISHED_EDITIONS, editions.quantity_measured)
        eq_(1, editions.value)

        eq_(DataSource.METADATA_WRANGLER, popularity.data_source.name)
        eq_(Measurement.POPULARITY, popularity.quantity_measured)
        eq_(0.25, popularity.value)

        eq_(DataSource.METADATA_WRANGLER, quality.data_source.name)
        eq_(Measurement.QUALITY, quality.quantity_measured)
        eq_(0.3333, quality.value)

        eq_(DataSource.METADATA_WRANGLER, rating.data_source.name)
        eq_(Measurement.RATING, rating.quantity_measured)
        eq_(0.6, rating.value)

        seven, children, courtship, fantasy, pz, magic, new_york = sorted(
            mouse.primary_identifier.classifications,
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

        work = mouse.work
        work.calculate_presentation()
        eq_(0.4142, round(work.quality, 4))
        eq_(Classifier.AUDIENCE_CHILDREN, work.audience)
        eq_(NumericRange(7,7, '[]'), work.target_age)

        # The other book has no license pool and no work because we
        # could not figure out whether the license source was Project
        # Gutenberg or Project GITenberg.
        eq_(None, crow.work)
        eq_(None, crow.license_pool)

        # Bonus: make sure that delivery mechanisms are set appropriately.
        [mech] = mouse.license_pool.delivery_mechanisms
        eq_(Representation.EPUB_MEDIA_TYPE, mech.delivery_mechanism.content_type)
        eq_(DeliveryMechanism.NO_DRM, mech.delivery_mechanism.drm_scheme)
        eq_('http://www.gutenberg.org/ebooks/10441.epub.images', 
            mech.resource.url)

        # If we import the same file again, we get the same list of Editions.
        imported2, messages, next_links = OPDSImporter(self._db).import_from_feed(feed)
        eq_(imported2, imported)

    def test_import_updates_metadata(self):

        path = os.path.join(self.resource_path, "metadata_wrangler_overdrive.opds")
        feed = open(path).read()

        edition, is_new = self._edition(
            DataSource.OVERDRIVE, Identifier.OVERDRIVE_ID,
            with_license_pool=True
        )
        edition.license_pool.calculate_work()
        old_work = edition.work

        old_license_pool = edition.license_pool
        feed = feed.replace("{OVERDRIVE ID}", edition.primary_identifier.identifier)
        [imported], messages, next_links = OPDSImporter(self._db).import_from_feed(feed)
        
        # The edition we created has had its metadata updated.
        eq_(imported, edition)
        eq_("The Green Mouse", imported.title)

        # But the work and license pools have not changed.
        eq_(edition.license_pool, old_license_pool)
        eq_(edition.work.license_pools, [old_license_pool])

    def test_status_and_message(self):
        path = os.path.join(self.resource_path, "unrecognized_identifier.opds")
        feed = open(path).read()
        imported, messages, next_link = OPDSImporter(self._db).import_from_feed(feed)
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

        links = [LinkData(href=self._url, rel=rel, media_type="image/jpeg")
                 for rel in [Hyperlink.THUMBNAIL_IMAGE,
                             Hyperlink.IMAGE,
                             Hyperlink.THUMBNAIL_IMAGE,
                             Hyperlink.IMAGE]
        ]
        t1, i1, t2, i2 = links
        links = OPDSImporter.consolidate_links(links)
        eq_([Hyperlink.IMAGE,
             Hyperlink.IMAGE], [x.rel for x in links])
        eq_(t1, i1.thumbnail)
        eq_(t2, i2.thumbnail)

        links = [LinkData(href=self._url, rel=rel, media_type="image/jpeg")
                 for rel in [Hyperlink.THUMBNAIL_IMAGE,
                             Hyperlink.IMAGE,
                             Hyperlink.IMAGE]
        ]
        t1, i1, i2 = links
        links = OPDSImporter.consolidate_links(links)
        eq_([Hyperlink.IMAGE,
             Hyperlink.IMAGE], [x.rel for x in links])
        eq_(t1, i1.thumbnail)
        eq_(None, i2.thumbnail)
