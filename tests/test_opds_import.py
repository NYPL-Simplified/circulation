import os
import datetime
from StringIO import StringIO
from nose.tools import (
    set_trace,
    eq_,
    assert_raises
)
import feedparser

from lxml import etree
import pkgutil
from psycopg2.extras import NumericRange
from . import (
    DatabaseTest,
)
from config import (
    Configuration,
    temp_config,
    CannotLoadConfiguration
)
from opds_import import (
    SimplifiedOPDSLookup,
    OPDSImporter,
    OPDSImporterWithS3Mirror,
    OPDSImportMonitor,
)
from metadata_layer import (
    LinkData
)
from model import (
    Contributor,
    CoverageRecord,
    DataSource,
    DeliveryMechanism,
    Hyperlink,
    Identifier,
    Edition,
    Measurement,
    Representation,
    RightsStatus,
    Subject,
)
from coverage import CoverageFailure

from s3 import DummyS3Uploader
from testing import DummyHTTPClient


class DoomedOPDSImporter(OPDSImporter):
    def import_edition_from_metadata(self, metadata, *args):
        if metadata.title == "Johnny Crow's Party":
            # This import succeeds.
            return super(DoomedOPDSImporter, self).import_edition_from_metadata(metadata, *args)
        else:
            # Any other import fails.
            raise Exception("Utter failure!")

class DoomedWorkOPDSImporter(OPDSImporter):
    """An OPDS Importer that imports editions but can't create works."""
    def update_work_for_edition(self, edition, *args, **kwargs):
        if edition.title == "Johnny Crow's Party":
            # This import succeeds.
            return super(DoomedWorkOPDSImporter, self).update_work_for_edition(edition, *args, **kwargs)
        else:
            # Any other import fails.
            raise Exception("Utter work failure!")

class TestSimplifiedOPDSLookup(object):

    def test_authenticates_wrangler_requests(self):
        """Tests that the client_id and client_secret are set for any
        Metadata Wrangler lookups"""

        mw_integration = Configuration.METADATA_WRANGLER_INTEGRATION
        mw_client_id = Configuration.METADATA_WRANGLER_CLIENT_ID
        mw_client_secret = Configuration.METADATA_WRANGLER_CLIENT_SECRET

        with temp_config() as config:
            config['integrations'][mw_integration] = {
                Configuration.URL : "http://localhost",
                mw_client_id : "abc",
                mw_client_secret : "def"
            }
            importer = SimplifiedOPDSLookup.from_config()
            eq_("abc", importer.client_id)
            eq_("def", importer.client_secret)

            # An error is raised if only one value is set.
            del config['integrations'][mw_integration][mw_client_secret]
            assert_raises(CannotLoadConfiguration, SimplifiedOPDSLookup.from_config)

            # The details are None if client configuration isn't set at all.
            del config['integrations'][mw_integration][mw_client_id]
            importer = SimplifiedOPDSLookup.from_config()
            eq_(None, importer.client_id)
            eq_(None, importer.client_secret)

            # For other integrations, the details aren't created at all.
            config['integrations']["Content Server"] = dict(
                url = "http://whatevz"
            )
            importer = SimplifiedOPDSLookup.from_config("Content Server")
            eq_(None, importer.client_id)
            eq_(None, importer.client_secret)


class OPDSImporterTest(DatabaseTest):

    def setup(self):
        super(OPDSImporterTest, self).setup()
        base_path = os.path.split(__file__)[0]
        self.resource_path = os.path.join(base_path, "files", "opds")
        self.content_server_feed = open(
            os.path.join(self.resource_path, "content_server.opds")).read()
        self.content_server_mini_feed = open(
            os.path.join(self.resource_path, "content_server_mini.opds")).read()


class TestOPDSImporter(OPDSImporterTest):

    def test_extract_next_links(self):
        importer = OPDSImporter(self._db, DataSource.NYT)
        next_links = importer.extract_next_links(
            self.content_server_mini_feed
        )

        eq_(1, len(next_links))
        eq_("http://localhost:5000/?after=327&size=100", next_links[0])

    def test_extract_last_update_dates(self):
        importer = OPDSImporter(self._db, DataSource.NYT)
        last_update_dates = importer.extract_last_update_dates(
            self.content_server_mini_feed
        )

        eq_(3, len(last_update_dates))

        identifier1, updated1 = last_update_dates[0]
        identifier2, updated2 = last_update_dates[1]
        identifier3, updated3 = last_update_dates[2]

        eq_("urn:librarysimplified.org/terms/id/Gutenberg%20ID/10441", identifier1)
        eq_(datetime.datetime(2015, 1, 2, 16, 56, 40), updated1)

        eq_("urn:librarysimplified.org/terms/id/Gutenberg%20ID/10557", identifier2)
        eq_(datetime.datetime(2015, 1, 2, 16, 56, 40), updated2)

        eq_("http://www.gutenberg.org/ebooks/1984", identifier3)
        eq_(None, updated3)

    def test_extract_metadata(self):
        importer = OPDSImporter(self._db, DataSource.NYT)
        metadata, failures = importer.extract_feed_data(
            self.content_server_mini_feed
        )

        m1 = metadata['http://www.gutenberg.org/ebooks/10441']
        m2 = metadata['http://www.gutenberg.org/ebooks/10557']
        c1 = metadata['http://www.gutenberg.org/ebooks/10441']
        c2 = metadata['http://www.gutenberg.org/ebooks/10557']

        eq_("The Green Mouse", m1.title)
        eq_("A Tale of Mousy Terror", m1.subtitle)

        eq_(DataSource.NYT, m1._data_source)
        eq_(DataSource.NYT, m2._data_source)
        eq_(DataSource.NYT, c1._data_source)
        eq_(DataSource.NYT, c2._data_source)

        [failure] = failures.values()
        eq_(u"202: I'm working to locate a source for this identifier.", failure.exception)


    def test_extract_metadata_from_feedparser(self):

        data_source = DataSource.lookup(self._db, DataSource.OA_CONTENT_SERVER)
        values, failures = OPDSImporter.extract_data_from_feedparser(
            self.content_server_mini_feed, data_source
        )

        metadata = values['urn:librarysimplified.org/terms/id/Gutenberg%20ID/10441']
        eq_("The Green Mouse", metadata['title'])
        eq_("A Tale of Mousy Terror", metadata['subtitle'])
        eq_('en', metadata['language'])
        eq_('Project Gutenberg', metadata['publisher'])

        circulation = metadata['circulation']
        eq_(DataSource.GUTENBERG, circulation['data_source'])

        failure = failures['http://www.gutenberg.org/ebooks/1984']
        eq_(u"202: I'm working to locate a source for this identifier.", failure.exception)


    def test_extract_metadata_from_feedparser_handles_exception(self):
        class DoomedFeedparserOPDSImporter(OPDSImporter):
            """An importer that can't extract metadata from feedparser."""
            @classmethod
            def _data_detail_for_feedparser_entry(cls, entry, data_source):
                raise Exception("Utter failure!")

        data_source = DataSource.lookup(self._db, DataSource.OA_CONTENT_SERVER)

        values, failures = DoomedFeedparserOPDSImporter.extract_data_from_feedparser(
            self.content_server_mini_feed, data_source
        )

        # No metadata was extracted.
        eq_(0, len(values.keys()))

        # There are 3 failures, the 202 in the feed and 2 from exceptions.
        eq_(3, len(failures))

        # The regular status message is there.
        failure = failures['http://www.gutenberg.org/ebooks/1984']
        assert isinstance(failure, CoverageFailure)
        eq_(True, failure.transient)
        eq_(u"202: I'm working to locate a source for this identifier.", failure.exception)

        # The first error message is there.
        failure = failures['urn:librarysimplified.org/terms/id/Gutenberg%20ID/10441']
        assert isinstance(failure, CoverageFailure)
        eq_(True, failure.transient)
        assert "Utter failure!" in failure.exception

        # The second error message is there.
        failure = failures['urn:librarysimplified.org/terms/id/Gutenberg%20ID/10557']
        assert isinstance(failure, CoverageFailure)
        eq_(True, failure.transient)
        assert "Utter failure!" in failure.exception

    def test_extract_metadata_from_elementtree(self):

        data_source = DataSource.lookup(self._db, DataSource.OA_CONTENT_SERVER)

        data, failures = OPDSImporter.extract_metadata_from_elementtree(
            self.content_server_feed, data_source
        )

        # There are 76 entries in the feed, and we got metadata for
        # every one of them.
        eq_(76, len(data))
        eq_(0, len(failures))

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

    def test_extract_metadata_from_elementtree_handles_exception(self):
        class DoomedElementtreeOPDSImporter(OPDSImporter):
            """An importer that can't extract metadata from elementttree."""
            @classmethod
            def _detail_for_elementtree_entry(cls, *args, **kwargs):
                raise Exception("Utter failure!")

        data_source = DataSource.lookup(self._db, DataSource.OA_CONTENT_SERVER)

        values, failures = DoomedElementtreeOPDSImporter.extract_metadata_from_elementtree(
            self.content_server_mini_feed, data_source
        )

        # No metadata was extracted.
        eq_(0, len(values.keys()))

        # There are 3 messages - every entry threw an exception.
        eq_(3, len(failures))

        # The entry with the 202 message threw an exception.
        failure = failures['http://www.gutenberg.org/ebooks/1984']
        assert isinstance(failure, CoverageFailure)
        eq_(True, failure.transient)
        assert "Utter failure!" in failure.exception

        # And so did the other entries.
        failure = failures['urn:librarysimplified.org/terms/id/Gutenberg%20ID/10441']
        assert isinstance(failure, CoverageFailure)
        eq_(True, failure.transient)
        assert "Utter failure!" in failure.exception

        failure = failures['urn:librarysimplified.org/terms/id/Gutenberg%20ID/10557']
        assert isinstance(failure, CoverageFailure)
        eq_(True, failure.transient)
        assert "Utter failure!" in failure.exception

    def test_import_exception_if_unable_to_parse_feed(self):
        feed = "I am not a feed."
        importer = OPDSImporter(self._db)

        assert_raises(etree.XMLSyntaxError, importer.import_from_feed, feed)


    def test_import(self):
        feed = self.content_server_mini_feed

        imported_editions, pools, works, failures = (
            OPDSImporter(self._db).import_from_feed(feed)
        )

        [crow, mouse] = sorted(imported_editions, key=lambda x: x.title)

        # By default, this feed is treated as though it came from the
        # metadata wrangler. No Work has been created.
        eq_(DataSource.METADATA_WRANGLER, crow.data_source.name)
        eq_(None, crow.work)
        eq_(None, crow.license_pool)
        eq_(Edition.BOOK_MEDIUM, crow.medium)

        # not even the 'mouse'
        eq_(None, mouse.work)
        eq_(Edition.PERIODICAL_MEDIUM, mouse.medium)

        popularity, quality, rating = sorted(
            [x for x in mouse.primary_identifier.measurements
             if x.is_most_recent],
            key=lambda x: x.quantity_measured
        )

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

        # If we import the same file again, we get the same list of Editions.
        imported_editions_2, pools_2, works_2, failures_2 = (
            OPDSImporter(self._db).import_from_feed(feed)
        )
        eq_(imported_editions_2, imported_editions)

        # importing with a lendable data source makes license pools and works
        imported_editions, pools, works, failures = (
            OPDSImporter(self._db, data_source_name=DataSource.OA_CONTENT_SERVER).import_from_feed(feed)
        )

        [crow_pool, mouse_pool] = sorted(
            pools, key=lambda x: x.presentation_edition.title
        )

        # Work was created for both books.
        assert crow_pool.work is not None
        eq_(Edition.BOOK_MEDIUM, crow_pool.presentation_edition.medium)

        assert mouse_pool.work is not None
        eq_(Edition.PERIODICAL_MEDIUM, mouse_pool.presentation_edition.medium)

        work = mouse_pool.work
        work.calculate_presentation()
        eq_(0.4142, round(work.quality, 4))
        eq_(Classifier.AUDIENCE_CHILDREN, work.audience)
        eq_(NumericRange(7,7, '[]'), work.target_age)

        # Bonus: make sure that delivery mechanisms are set appropriately.
        [mech] = mouse_pool.delivery_mechanisms
        eq_(Representation.EPUB_MEDIA_TYPE, mech.delivery_mechanism.content_type)
        eq_(DeliveryMechanism.NO_DRM, mech.delivery_mechanism.drm_scheme)
        eq_('http://www.gutenberg.org/ebooks/10441.epub.images', 
            mech.resource.url)

    def test_import_with_lendability(self):
        # Tests that will create Edition, LicensePool, and Work objects, when appropriate.
        # For example, on a Metadata_Wrangler data source, it is only appropriate to create 
        # editions, but not pools or works.  On a lendable data source, should create 
        # pools and works as well as editions.
        # Tests that the number and contents of error messages are appropriate to the task.

        # will create editions, but not license pools or works, because the 
        # metadata wrangler data source is not lendable
        feed = self.content_server_mini_feed

        importer_mw = OPDSImporter(self._db, data_source_name=DataSource.METADATA_WRANGLER)
        imported_editions_mw, pools_mw, works_mw, failures_mw = (
            importer_mw.import_from_feed(feed)
        )

        # Both books were imported, because they were new.
        eq_(2, len(imported_editions_mw))

        # But pools and works weren't created, because the data source isn't lendable.
        # 1 error message, because correctly didn't even get to trying to create pools, 
        # so no messages there, but do have that entry stub at end of sample xml file, 
        # which should fail with a message.
        eq_(1, len(failures_mw))
        eq_(0, len(pools_mw))
        eq_(0, len(works_mw))

        # try again, with a license pool-acceptable data source
        importer_g = OPDSImporter(self._db, data_source_name=DataSource.GUTENBERG)
        imported_editions_g, pools_g, works_g, failures_g = (
            importer_g.import_from_feed(feed)
        )

        # we made new editions, because we're now creating edition per data source, not overwriting
        eq_(2, len(imported_editions_g))
        # TODO: and we also created presentation editions, with author and title set

        # now pools and works are in, too
        eq_(1, len(failures_g))
        eq_(2, len(pools_g))
        eq_(2, len(works_g))        

        # assert that bibframe datasource from feed was correctly overwritten
        # with data source I passed into the importer.
        for pool in pools_g:
            eq_(pool.data_source.name, DataSource.GUTENBERG)

    def test_import_with_unrecognized_distributor_fails(self):
        """We get a book from the open-access content server but the license
        comes from an unrecognized data source. We can't import the book
        because we can't record its provenance accurately.
        """
        feed = open(
            os.path.join(self.resource_path, "unrecognized_distributor.opds")).read()
        importer = OPDSImporter(
            self._db, 
            data_source_name=DataSource.OA_CONTENT_SERVER
        )
        imported_editions, pools, works, failures = (
            importer.import_from_feed(feed)
        )
        # No editions, licensepools, or works were imported.
        eq_([], imported_editions)
        eq_([], pools)
        eq_([], works)
        [failure] = failures.values()
        eq_(True, failure.transient)
        assert "Unrecognized circulation data source: Unknown Source" in failure.exception

    def test_import_with_cutoff(self):
        cutoff = datetime.datetime(2016, 1, 2, 16, 56, 40)
        feed = self.content_server_mini_feed
        importer = OPDSImporter(self._db, data_source_name=DataSource.GUTENBERG)
        imported_editions, pools, works, failures = (
            importer.import_from_feed(feed, cutoff_date=cutoff)
        )

        # Despite the cutoff, both books were imported, because they were new.
        eq_(2, len(imported_editions))
        eq_(2, len(pools))
        eq_(2, len(works))        

        # But if we try it again...
        imported_editions, pools, works, failures = (
            importer.import_from_feed(feed, cutoff_date=cutoff)
        )

        # None of the books were imported because they weren't updated
        # after the cutoff.
        eq_(0, len(imported_editions))
        eq_(0, len(pools))
        eq_(0, len(works))

        # And if we change the cutoff...
        cutoff = datetime.datetime(2013, 1, 2, 16, 56, 40)
        imported_editions, pools, works, failures = (
            importer.import_from_feed(feed, cutoff_date=cutoff)
        )

        # Both books were imported again.
        eq_(2, len(imported_editions))
        eq_(2, len(pools))
        eq_(2, len(works))

        assert (datetime.datetime.utcnow() - pools[0].last_checked) < datetime.timedelta(seconds=10)


    def test_import_updates_metadata(self):

        path = os.path.join(self.resource_path, "metadata_wrangler_overdrive.opds")
        feed = open(path).read()

        edition, is_new = self._edition(
            DataSource.OVERDRIVE, Identifier.OVERDRIVE_ID,
            with_license_pool=True
        )
        edition.license_pool.calculate_work()
        work = edition.license_pool.work

        old_license_pool = edition.license_pool
        feed = feed.replace("{OVERDRIVE ID}", edition.primary_identifier.identifier)

        imported_editions, imported_pools, imported_works, failures = (
            OPDSImporter(self._db, data_source_name=DataSource.OVERDRIVE).import_from_feed(feed)
        )

        # The edition we created has had its metadata updated.
        eq_(imported_editions[0], edition)
        eq_("The Green Mouse", imported_editions[0].title)

        # But the license pools have not changed.
        eq_(edition.license_pool, old_license_pool)
        eq_(work.license_pools, [old_license_pool])


    def test_import_from_license_source(self):
        # Instead of importing this data as though it came from the
        # metadata wrangler, let's import it as though it came from the
        # open-access content server.
        feed = self.content_server_mini_feed
        importer = OPDSImporter(
            self._db, data_source_name=DataSource.OA_CONTENT_SERVER
        )

        imported_editions, imported_pools, imported_works, failures = (
            importer.import_from_feed(feed)
        )

        # Two works have been created, because the content server
        # actually tells you how to get copies of these books.
        [crow, mouse] = sorted(imported_works, key=lambda x: x.title)

        # Each work has one license pool.
        [crow_pool] = crow.license_pools
        [mouse_pool] = mouse.license_pools

        # The OPDS importer sets the data source of the license pool
        # to Project Gutenberg, since that's the authority that grants
        # access to the book.
        eq_(DataSource.GUTENBERG, mouse_pool.data_source.name)

        # But the license pool's presentation edition has a data
        # source associated with the Library Simplified open-access
        # content server, since that's where the metadata comes from.
        eq_(DataSource.OA_CONTENT_SERVER, 
            mouse_pool.presentation_edition.data_source.name
        )

        # Since the 'mouse' book came with an open-access link, the license
        # pool delivery mechanism has been marked as open access.
        eq_(True, mouse_pool.open_access)
        eq_(RightsStatus.GENERIC_OPEN_ACCESS, 
            mouse_pool.delivery_mechanisms[0].rights_status.uri)

        # The 'mouse' work has not been marked presentation-ready,
        # because the OPDS importer was not told to make works
        # presentation-ready as they're imported.
        eq_(False, mouse_pool.work.presentation_ready)

        # The OPDS feed didn't actually say where the 'crow' book
        # comes from, but we did tell the importer to use the open access 
        # content server as the data source, so both a Work and a LicensePool 
        # were created, and their data source is the open access content server,
        # not Project Gutenberg.
        eq_(DataSource.OA_CONTENT_SERVER, crow_pool.data_source.name)


    def test_import_and_make_presentation_ready(self):
        # Now let's tell the OPDS importer to make works presentation-ready
        # as soon as they're imported.
        feed = self.content_server_mini_feed
        importer = OPDSImporter(
            self._db, data_source_name=DataSource.OA_CONTENT_SERVER
        )
        imported_editions, imported_pools, imported_works, failures = (
            importer.import_from_feed(feed, immediately_presentation_ready=True)
        )

        [crow, mouse] = sorted(imported_works, key=lambda x: x.title)

        # Both the 'crow' and the 'mouse' book had presentation-ready works created.
        eq_(True, crow.presentation_ready)
        eq_(True, mouse.presentation_ready)


    def test_status_and_message(self):
        path = os.path.join(self.resource_path, "unrecognized_identifier.opds")
        feed = open(path).read()
        imported_editions, imported_pools, imported_works, failures = (
            OPDSImporter(self._db).import_from_feed(feed)
        )

        [failure] = failures.values()
        assert isinstance(failure, CoverageFailure)
        eq_(True, failure.transient)
        eq_("404: I've never heard of this work.", failure.exception)


    def test_import_edition_failure_becomes_coverage_failure(self):
        # Make sure that an exception during import generates a
        # meaningful error message.

        feed = self.content_server_mini_feed

        imported_editions, pools, works, failures = (
            DoomedOPDSImporter(self._db).import_from_feed(feed)
        )

        # Only one book was imported, the other failed.
        eq_(1, len(imported_editions))

        # The other failed to import, and became a CoverageFailure
        failure = failures['http://www.gutenberg.org/ebooks/10441']
        assert isinstance(failure, CoverageFailure)
        eq_(False, failure.transient)
        assert "Utter failure!" in failure.exception

    def test_import_work_failure_becomes_coverage_failure(self):
        # Make sure that an exception while updating a work for an
        # imported edition generates a meaningful error message.

        feed = self.content_server_mini_feed
        importer = DoomedWorkOPDSImporter(self._db, data_source_name=DataSource.OA_CONTENT_SERVER)

        imported_editions, pools, works, failures = (
            importer.import_from_feed(feed)
        )

        # One work was created, the other failed.
        eq_(1, len(works))

        # There's an error message for the work that failed. 
        failure = failures['http://www.gutenberg.org/ebooks/10441']
        assert isinstance(failure, CoverageFailure)
        eq_(False, failure.transient)
        assert "Utter work failure!" in failure.exception

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



class TestOPDSImporterWithS3Mirror(OPDSImporterTest):

    def test_resources_are_mirrored_on_import(self):

        svg = """<!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN"
  "http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd">

<svg xmlns="http://www.w3.org/2000/svg" width="1000" height="500">
    <ellipse cx="50" cy="25" rx="50" ry="25" style="fill:blue;"/>
</svg>"""

        http = DummyHTTPClient()
        http.queue_response(
            200, content='I am 10557.epub.images',
            media_type=Representation.EPUB_MEDIA_TYPE,
        )
        http.queue_response(
            200, content=svg, media_type=Representation.SVG_MEDIA_TYPE
        )
        http.queue_response(
            200, content='I am 10441.epub.images',
            media_type=Representation.EPUB_MEDIA_TYPE
        )

        s3 = DummyS3Uploader()

        importer = OPDSImporter(
            self._db, data_source_name=DataSource.OA_CONTENT_SERVER,
            mirror=s3, http_get=http.do_get
        )

        imported_editions, pools, works, failures = (
            importer.import_from_feed(self.content_server_mini_feed)
        )
        e1 = imported_editions[0]
        e2 = imported_editions[1]

        # The import process requested each remote resource in the
        # order they appeared in the OPDS feed. The thumbnail
        # image was not requested, since we were going to make our own
        # thumbnail anyway.
        eq_(http.requests, [
            'http://www.gutenberg.org/ebooks/10441.epub.images',
            'https://s3.amazonaws.com/book-covers.nypl.org/Gutenberg-Illustrated/10441/cover_10441_9.png', 
            'http://www.gutenberg.org/ebooks/10557.epub.images',
        ])

        [e1_oa_link, e1_image_link, e1_description_link] = sorted(
            e1.primary_identifier.links, key=lambda x: x.rel
        )
        [e2_oa_link] = e2.primary_identifier.links

        # The two open-access links were mirrored to S3, as was the
        # original SVG image and its PNG thumbnail.
        imported_representations = [
            e1_oa_link.resource.representation,
            e1_image_link.resource.representation,
            e1_image_link.resource.representation.thumbnails[0],
            e2_oa_link.resource.representation,
        ]
        eq_(imported_representations, s3.uploaded)


        eq_(4, len(s3.uploaded))
        eq_("I am 10441.epub.images", s3.content[0])
        eq_(svg, s3.content[1])
        eq_("I am 10557.epub.images", s3.content[3])

        # Each resource was 'mirrored' to an Amazon S3 bucket.
        url0 = 'http://s3.amazonaws.com/test.content.bucket/Library%20Simplified%20Open%20Access%20Content%20Server/Gutenberg%20ID/10441/The%20Green%20Mouse.epub.images'
        url1 = u'http://s3.amazonaws.com/test.cover.bucket/Library%20Simplified%20Open%20Access%20Content%20Server/Gutenberg%20ID/10441/cover_10441_9.png'
        url2 = u'http://s3.amazonaws.com/test.cover.bucket/scaled/300/Library%20Simplified%20Open%20Access%20Content%20Server/Gutenberg%20ID/10441/cover_10441_9.png'
        url3 = 'http://s3.amazonaws.com/test.content.bucket/Library%20Simplified%20Open%20Access%20Content%20Server/Gutenberg%20ID/10557/Johnny%20Crow%27s%20Party.epub.images'
        uploaded_urls = [x.mirror_url for x in s3.uploaded]
        eq_([url0, url1, url2, url3], uploaded_urls)


        # If we fetch the feed again, and the entries have been updated since the
        # cutoff, but the content of the open access links hasn't changed, we won't mirror
        # them again.
        cutoff = datetime.datetime(2013, 1, 2, 16, 56, 40)

        http.queue_response(
            304, media_type=Representation.EPUB_MEDIA_TYPE
        )

        http.queue_response(
            304, media_type=Representation.SVG_MEDIA_TYPE
        )

        http.queue_response(
            304, media_type=Representation.EPUB_MEDIA_TYPE
        )

        imported_editions, pools, works, failures = (
            importer.import_from_feed(self.content_server_mini_feed, cutoff_date=cutoff)
        )

        eq_([e1, e2], imported_editions)

        # Nothing new has been uploaded
        eq_(4, len(s3.uploaded))

        # If the content has changed, it will be mirrored again.
        http.queue_response(
            200, content="I am a new version of 10557.epub.images",
            media_type=Representation.EPUB_MEDIA_TYPE
        )

        http.queue_response(
            200, content=svg,
            media_type=Representation.SVG_MEDIA_TYPE
        )

        http.queue_response(
            200, content="I am a new version of 10441.epub.images",
            media_type=Representation.EPUB_MEDIA_TYPE
        )

        imported_editions, pools, works, failures = (
            importer.import_from_feed(self.content_server_mini_feed, cutoff_date=cutoff)
        )

        eq_([e1, e2], imported_editions)
        eq_(8, len(s3.uploaded))
        eq_("I am a new version of 10441.epub.images", s3.content[4])
        eq_(svg, s3.content[5])
        eq_("I am a new version of 10557.epub.images", s3.content[7])


class TestOPDSImportMonitor(OPDSImporterTest):

    def setup(self):
        super(TestOPDSImportMonitor, self).setup()
        feed = self.content_server_mini_feed
        # Remove the last entry, since it's a message and will always be new.
        last_entry_start = feed.rfind('<entry>')
        last_entry_end = feed.rfind('</entry>') + len('</entry>')
        self.content_server_mini_feed_without_message = feed[0:last_entry_start] + feed[last_entry_end:]


    def test_check_for_new_data(self):
        monitor = OPDSImportMonitor(self._db, "http://url", DataSource.OA_CONTENT_SERVER, OPDSImporter)
        feed = self.content_server_mini_feed_without_message

        # Nothing has been imported yet, so all data is new.
        eq_(True, monitor.check_for_new_data(feed))

        # If there's a cutoff date that's after the updated
        # dates in the feed, the data still isn't new.
        eq_(False, monitor.check_for_new_data(feed, datetime.datetime(2016, 1, 1, 1, 1, 1)))

        # But if the cutoff is before the updated time...
        eq_(True, monitor.check_for_new_data(feed, datetime.datetime(1970, 1, 1, 1, 1, 1)))

        # Now import the editions.
        monitor.importer.import_from_feed(feed)
        eq_(2, self._db.query(Edition).count())

        editions = self._db.query(Edition).all()
        data_source = DataSource.lookup(self._db, DataSource.OA_CONTENT_SERVER)

        # If there are CoverageRecords, that are after the updated
        # dates, there's nothing new.
        record, ignore = CoverageRecord.add_for(
            editions[0], data_source, CoverageRecord.IMPORT_OPERATION
        )
        record.timestamp = datetime.datetime(2016, 1, 1, 1, 1, 1)

        record2, ignore = CoverageRecord.add_for(
            editions[1], data_source, CoverageRecord.IMPORT_OPERATION
        )
        record2.timestamp = datetime.datetime(2016, 1, 1, 1, 1, 1)

        eq_(False, monitor.check_for_new_data(feed))

        # If a CoverageRecord is before the updated date, there's
        # new data.
        record2.timestamp = datetime.datetime(1970, 1, 1, 1, 1, 1)
        eq_(True, monitor.check_for_new_data(feed))

        # If a CoverageRecord is a transient failure, we try again
        # regardless of whether it's been updated.
        for r in [record, record2]:
            r.timestamp = datetime.datetime(2016, 1, 1, 1, 1, 1)
            r.exception = "Failure!"
            r.status = CoverageRecord.TRANSIENT_FAILURE
        eq_(True, monitor.check_for_new_data(feed))

        # If a CoverageRecord is a persistent failure, we check
        # if the feed has changed since the timestamp.
        for r in [record, record2]:
            r.status = CoverageRecord.PERSISTENT_FAILURE
        eq_(False, monitor.check_for_new_data(feed))

        record.timestamp = datetime.datetime(1970, 1, 1, 1, 1, 1)
        eq_(True, monitor.check_for_new_data(feed))

        # If only one of the entries has a CoverageRecord, the other
        # uses the cutoff date.
        self._db.delete(record2)
        eq_(True, monitor.check_for_new_data(feed, datetime.datetime(1970, 1, 1, 1, 1, 1)))


    def test_follow_one_link(self):
        monitor = OPDSImportMonitor(self._db, "http://url", DataSource.OA_CONTENT_SERVER, OPDSImporter)
        feed = self.content_server_mini_feed_without_message

        # If there's new data, follow_one_link extracts the next links.

        http = DummyHTTPClient()
        http.queue_response(200, content=feed)

        next_links, content = monitor.follow_one_link("http://url", do_get=http.do_get)
        
        eq_(1, len(next_links))
        eq_("http://localhost:5000/?after=327&size=100", next_links[0])

        eq_(feed, content)

        # Now import the editions and add coverage records.
        monitor.importer.import_from_feed(feed)
        eq_(2, self._db.query(Edition).count())

        editions = self._db.query(Edition).all()
        data_source = DataSource.lookup(self._db, DataSource.OA_CONTENT_SERVER)

        for edition in editions:
            record, ignore = CoverageRecord.add_for(
                edition, data_source, CoverageRecord.IMPORT_OPERATION
            )
            record.timestamp = datetime.datetime(2016, 1, 1, 1, 1, 1)


        # If there's no new data, follow_one_link returns no next links and no content.
        http.queue_response(200, content=feed)

        next_links, content = monitor.follow_one_link("http://url", do_get=http.do_get)

        eq_(0, len(next_links))
        eq_(None, content)


    def test_import_one_feed(self):
        # Check coverage records are created.

        monitor = OPDSImportMonitor(self._db, "http://url", DataSource.OA_CONTENT_SERVER, DoomedOPDSImporter)
        data_source = DataSource.lookup(self._db, DataSource.OA_CONTENT_SERVER)

        feed = self.content_server_mini_feed

        monitor.import_one_feed(feed, None)
        
        editions = self._db.query(Edition).all()
        
        # One edition has been imported
        eq_(1, len(editions))

        # That edition has a CoverageRecord.
        record = CoverageRecord.lookup(
            editions[0].primary_identifier, data_source,
            operation=CoverageRecord.IMPORT_OPERATION
        )
        eq_(CoverageRecord.SUCCESS, record.status)
        eq_(None, record.exception)

        # The 202 status message in the feed caused a transient failure.
        # The exception caused a persistent failure.

        coverage_records = self._db.query(CoverageRecord).filter(
            CoverageRecord.operation==CoverageRecord.IMPORT_OPERATION,
            CoverageRecord.status != CoverageRecord.SUCCESS
        )
        eq_(
            sorted([CoverageRecord.TRANSIENT_FAILURE, 
                    CoverageRecord.PERSISTENT_FAILURE]),
            sorted([x.status for x in coverage_records])
        )
    
        identifier, ignore = Identifier.parse_urn(self._db, "urn:librarysimplified.org/terms/id/Gutenberg%20ID/10441")
        failure = CoverageRecord.lookup(
            identifier, data_source,
            operation=CoverageRecord.IMPORT_OPERATION
        )
        assert "Utter failure!" in failure.exception


    def test_run_once(self):
        class MockOPDSImportMonitor(OPDSImportMonitor):
            def __init__(self, *args, **kwargs):
                super(MockOPDSImportMonitor, self).__init__(*args, **kwargs)
                self.responses = []
                self.imports = []

            def queue_response(self, response):
                self.responses.append(response)

            def follow_one_link(self, link, cutoff_date=None, do_get=None):
                return self.responses.pop()

            def import_one_feed(self, feed, start):
                self.imports.append(feed)

        monitor = MockOPDSImportMonitor(self._db, "http://url", DataSource.OA_CONTENT_SERVER, OPDSImporter)
        
        monitor.queue_response([[], "last page"])
        monitor.queue_response([["second next link"], "second page"])
        monitor.queue_response([["next link"], "first page"])

        monitor.run_once(None, None)

        # Feeds are imported in reverse order
        eq_(["last page", "second page", "first page"], monitor.imports)
