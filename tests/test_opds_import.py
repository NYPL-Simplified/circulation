import os
import datetime
import urllib
from StringIO import StringIO
from lxml import builder
from nose.tools import (
    set_trace,
    eq_,
    assert_raises,
    assert_raises_regexp
)
import feedparser

from lxml import etree
import pkgutil
from psycopg2.extras import NumericRange

from . import (
    DatabaseTest,
)

from config import CannotLoadConfiguration
from opds_import import (
    AccessNotAuthenticated,
    MetadataWranglerOPDSLookup,
    OPDSImporter,
    OPDSImportMonitor,
    OPDSXMLParser,
    SimplifiedOPDSLookup,
)
from util.opds_writer import (
    AtomFeed,
    OPDSFeed,
    OPDSMessage,
)
from metadata_layer import (
    LinkData
)
from model import (
    Collection,
    Contributor,
    CoverageRecord,
    DataSource,
    DeliveryMechanism,
    ExternalIntegration,
    Hyperlink,
    Identifier,
    Edition,
    Measurement,
    Representation,
    RightsStatus,
    Subject,
    Work,
)
from coverage import CoverageFailure

from s3 import (
    S3Uploader,
    MockS3Uploader,
)
from testing import DummyHTTPClient
from util.http import BadResponseException


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


class TestMetadataWranglerOPDSLookup(DatabaseTest):

    def setup(self):
        super(TestMetadataWranglerOPDSLookup, self).setup()
        self.integration = self._external_integration(
            ExternalIntegration.METADATA_WRANGLER,
            goal=ExternalIntegration.METADATA_GOAL,
            password='secret', url="http://metadata.in"
        )
        self.collection = self._collection(
            protocol=ExternalIntegration.OVERDRIVE, external_account_id=u'library'
        )

    def test_authenticates_wrangler_requests(self):
        """Authenticated details are set for Metadata Wrangler requests
        when they configured for the ExternalIntegration
        """

        lookup = MetadataWranglerOPDSLookup.from_config(self._db)
        eq_("secret", lookup.shared_secret)
        eq_(True, lookup.authenticated)

        # The details are None if client configuration isn't set at all.
        self.integration.password = None
        lookup = MetadataWranglerOPDSLookup.from_config(self._db)
        eq_(None, lookup.shared_secret)
        eq_(False, lookup.authenticated)

    def test_add_args(self):
        lookup = MetadataWranglerOPDSLookup.from_config(self._db)
        args = 'greeting=hello'

        # If the base url doesn't have any arguments, args are created.
        base_url = self._url
        eq_(base_url + '?' + args, lookup.add_args(base_url, args))

        # If the base url has an argument already, additional args are appended.
        base_url = self._url + '?data_source=banana'
        eq_(base_url + '&' + args, lookup.add_args(base_url, args))

    def test_get_collection_url(self):
        lookup = MetadataWranglerOPDSLookup.from_config(self._db)

        # If the lookup client doesn't have a Collection, an error is
        # raised.
        assert_raises(
            ValueError, lookup.get_collection_url, 'banana'
        )

        # If the lookup client isn't authenticated, an error is raised.
        lookup.collection = self.collection
        lookup.shared_secret = None
        assert_raises(
            AccessNotAuthenticated, lookup.get_collection_url, 'banana'
        )

        # With both authentication and a specific Collection,
        # a URL is returned.
        lookup.shared_secret = 'secret'
        expected = '%s%s/banana' % (lookup.base_url, self.collection.metadata_identifier)
        eq_(expected, lookup.get_collection_url('banana'))

        # With an OPDS_IMPORT collection, a data source is included
        opds = self._collection(
            protocol=ExternalIntegration.OPDS_IMPORT,
            external_account_id=self._url,
            data_source_name=DataSource.OA_CONTENT_SERVER
        )
        lookup.collection = opds
        data_source_args = '?data_source=%s' % urllib.quote(opds.data_source.name)
        assert lookup.get_collection_url('banana').endswith(data_source_args)

    def test_lookup_endpoint(self):
        # A Collection-specific endpoint is returned if authentication
        # and a Collection is available.
        lookup = MetadataWranglerOPDSLookup.from_config(self._db, collection=self.collection)

        expected = self.collection.metadata_identifier + '/lookup'
        eq_(expected, lookup.lookup_endpoint)

        # Without a collection, an unspecific endpoint is returned.
        lookup.collection = None
        eq_('lookup', lookup.lookup_endpoint)

        # Without authentication, an unspecific endpoint is returned.
        lookup.shared_secret = None
        lookup.collection = self.collection
        eq_('lookup', lookup.lookup_endpoint)

        # With authentication and a collection, a specific endpoint is returned.
        lookup.shared_secret = 'secret'
        expected = '%s/lookup' % self.collection.metadata_identifier
        eq_(expected, lookup.lookup_endpoint)


class OPDSImporterTest(DatabaseTest):

    def setup(self):
        super(OPDSImporterTest, self).setup()
        base_path = os.path.split(__file__)[0]
        self.resource_path = os.path.join(base_path, "files", "opds")
        self.content_server_feed = open(
            os.path.join(self.resource_path, "content_server.opds")).read()
        self.content_server_mini_feed = open(
            os.path.join(self.resource_path, "content_server_mini.opds")).read()
        self._default_collection.external_integration.setting('data_source').value = (
            DataSource.OA_CONTENT_SERVER
        )

        # Set an ExternalIntegration for the metadata_client used
        # in the OPDSImporter.
        self.service = self._external_integration(
            ExternalIntegration.METADATA_WRANGLER,
            goal=ExternalIntegration.METADATA_GOAL,
            url="http://localhost"
        )
        

class TestOPDSImporter(OPDSImporterTest):

    def test_constructor(self):
        # The default way of making HTTP requests is with
        # Representation.cautious_http_get.
        importer = OPDSImporter(self._db, collection=None)
        eq_(Representation.cautious_http_get, importer.http_get)

        # But you can pass in anything you want.
        do_get = object()
        importer = OPDSImporter(self._db, collection=None, http_get=do_get)
        eq_(do_get, importer.http_get)

    def test_data_source_autocreated(self):
        name = "New data source " + self._str
        importer = OPDSImporter(
            self._db, collection=None, data_source_name=name
        )
        source1 = importer.data_source
        eq_(name, source1.name)

    def test_extract_next_links(self):
        importer = OPDSImporter(
            self._db, collection=None, data_source_name=DataSource.NYT
        )
        next_links = importer.extract_next_links(
            self.content_server_mini_feed
        )

        eq_(1, len(next_links))
        eq_("http://localhost:5000/?after=327&size=100", next_links[0])

    def test_extract_last_update_dates(self):
        importer = OPDSImporter(
            self._db, collection=None, data_source_name=DataSource.NYT
        )

        # This file has two <entry> tags and one <simplified:message> tag.
        # The <entry> tags have their last update dates extracted,
        # the message is ignored.
        last_update_dates = importer.extract_last_update_dates(
            self.content_server_mini_feed
        )

        eq_(2, len(last_update_dates))

        identifier1, updated1 = last_update_dates[0]
        identifier2, updated2 = last_update_dates[1]

        eq_("urn:librarysimplified.org/terms/id/Gutenberg%20ID/10441", identifier1)
        eq_(datetime.datetime(2015, 1, 2, 16, 56, 40), updated1)

        eq_("urn:librarysimplified.org/terms/id/Gutenberg%20ID/10557", identifier2)
        eq_(datetime.datetime(2015, 1, 2, 16, 56, 40), updated2)

    def test_extract_last_update_dates_ignores_entries_with_no_update(self):
        importer = OPDSImporter(
            self._db, collection=None, data_source_name=DataSource.NYT
        )

        # Rename the <updated> and <published> tags in the content
        # server so they don't show up.
        content = self.content_server_mini_feed.replace("updated>", "irrelevant>")
        content = content.replace("published>", "irrelevant>")
        last_update_dates = importer.extract_last_update_dates(content)

        # No updated dates!
        eq_([], last_update_dates)

    def test_extract_metadata(self):
        data_source_name = "Data source name " + self._str
        importer = OPDSImporter(
            self._db, collection=None, data_source_name=data_source_name
        )
        metadata, failures = importer.extract_feed_data(
            self.content_server_mini_feed
        )

        m1 = metadata['http://www.gutenberg.org/ebooks/10441']
        m2 = metadata['http://www.gutenberg.org/ebooks/10557']
        c1 = metadata['http://www.gutenberg.org/ebooks/10441']
        c2 = metadata['http://www.gutenberg.org/ebooks/10557']

        eq_("The Green Mouse", m1.title)
        eq_("A Tale of Mousy Terror", m1.subtitle)

        eq_(data_source_name, m1._data_source)
        eq_(data_source_name, m2._data_source)
        eq_(data_source_name, c1._data_source)
        eq_(data_source_name, c2._data_source)

        [failure] = failures.values()
        eq_(u"202: I'm working to locate a source for this identifier.", failure.exception)

    def test_extract_link(self):
        no_rel = AtomFeed.E.link(href="http://foo/")
        eq_(None, OPDSImporter.extract_link(no_rel))

        no_href = AtomFeed.E.link(href="", rel="foo")
        eq_(None, OPDSImporter.extract_link(no_href))

        good = AtomFeed.E.link(href="http://foo", rel="bar")
        link = OPDSImporter.extract_link(good)
        eq_("http://foo", link.href)
        eq_("bar", link.rel)

        relative = AtomFeed.E.link(href="/foo/bar", rel="self")
        link = OPDSImporter.extract_link(relative, "http://server")
        eq_("http://server/foo/bar", link.href)

    def test_extract_link_rights_uri(self):

        # Most of the time, a link's rights URI is inherited from the entry.
        entry_rights = RightsStatus.PUBLIC_DOMAIN_USA
        
        link_tag = AtomFeed.E.link(href="http://foo", rel="bar")
        link = OPDSImporter.extract_link(
            link_tag, entry_rights_uri=entry_rights
        )
        eq_(RightsStatus.PUBLIC_DOMAIN_USA, link.rights_uri)

        # But a dcterms:rights tag beneath the link can override this.
        rights_attr = "{%s}rights" % AtomFeed.DCTERMS_NS
        link_tag.attrib[rights_attr] = RightsStatus.IN_COPYRIGHT
        link = OPDSImporter.extract_link(
            link_tag, entry_rights_uri=entry_rights
        )
        eq_(RightsStatus.IN_COPYRIGHT, link.rights_uri)
        
    def test_extract_data_from_feedparser(self):

        data_source = DataSource.lookup(self._db, DataSource.OA_CONTENT_SERVER)
        values, failures = OPDSImporter.extract_data_from_feedparser(
            self.content_server_mini_feed, data_source
        )

        # The <entry> tag became a Metadata object.
        metadata = values['urn:librarysimplified.org/terms/id/Gutenberg%20ID/10441']
        eq_("The Green Mouse", metadata['title'])
        eq_("A Tale of Mousy Terror", metadata['subtitle'])
        eq_('en', metadata['language'])
        eq_('Project Gutenberg', metadata['publisher'])

        circulation = metadata['circulation']
        eq_(DataSource.GUTENBERG, circulation['data_source'])

        # The <simplified:message> tag did not become a
        # CoverageFailure -- that's handled by
        # extract_metadata_from_elementtree.
        eq_({}, failures)


    def test_extract_data_from_feedparser_handles_exception(self):
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

        # There are 2 failures, both from exceptions. The 202 message
        # found in content_server_mini.opds is not extracted
        # here--it's extracted by extract_metadata_from_elementtree.
        eq_(2, len(failures))

        # The first error message became a CoverageFailure.
        failure = failures['urn:librarysimplified.org/terms/id/Gutenberg%20ID/10441']
        assert isinstance(failure, CoverageFailure)
        eq_(True, failure.transient)
        assert "Utter failure!" in failure.exception

        # The second error message became a CoverageFailure.
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

        eq_('Animal Colors', periodical['series'])
        eq_('1', periodical['series_position'])

    def test_extract_metadata_from_elementtree_treats_message_as_failure(self):
        data_source = DataSource.lookup(self._db, DataSource.OA_CONTENT_SERVER)

        feed = open(
            os.path.join(self.resource_path, "unrecognized_identifier.opds")
        ).read()        
        values, failures = OPDSImporter.extract_metadata_from_elementtree(
            feed, data_source
        )

        # We have no Metadata objects and one CoverageFailure.
        eq_({}, values)

        # The CoverageFailure contains the information that was in a
        # <simplified:message> tag in unrecognized_identifier.opds.
        key = 'http://www.gutenberg.org/ebooks/100'
        eq_([key], failures.keys())
        failure = failures[key]
        eq_("404: I've never heard of this work.", failure.exception)
        eq_(key, failure.obj.urn)

    def test_extract_messages(self):
        parser = OPDSXMLParser()
        feed = open(
            os.path.join(self.resource_path, "unrecognized_identifier.opds")
        ).read()
        root = etree.parse(StringIO(feed))
        [message] = OPDSImporter.extract_messages(parser, root)
        eq_('urn:librarysimplified.org/terms/id/Gutenberg ID/100', message.urn)
        eq_(404, message.status_code)
        eq_("I've never heard of this work.", message.message)

    def test_handle_failure(self):
        axis_id = self._identifier(identifier_type=Identifier.AXIS_360_ID)
        axis_isbn = self._identifier(Identifier.ISBN, "9781453219539")
        identifier_mapping = {axis_isbn : axis_id}
        importer = OPDSImporter(
            self._db, collection=None, 
            data_source_name=DataSource.OA_CONTENT_SERVER,
            identifier_mapping = identifier_mapping
        )        

        # The simplest case -- an identifier associated with a
        # CoverageFailure. The Identifier and CoverageFailure are
        # returned as-is.
        input_failure = CoverageFailure(object(), "exception")

        urn = "urn:isbn:9781449358068"
        expect_identifier, ignore = Identifier.parse_urn(self._db, urn)
        identifier, output_failure = importer.handle_failure(
            urn, input_failure
        )
        eq_(expect_identifier, identifier)
        eq_(input_failure, output_failure)

        # A normal OPDSImporter would consider this a failure, but
        # because the 'failure' is an Identifier, not a
        # CoverageFailure, we're going to treat it as a success.
        identifier, not_a_failure = importer.handle_failure(
            "urn:isbn:9781449358068", self._identifier()
        )
        eq_(expect_identifier, identifier)
        eq_(identifier, not_a_failure)
        # Note that the 'failure' object retuned is the Identifier that 
        # was passed in, not the Identifier that substituted as the 'failure'.
        # (In real usage, though, they should be the same.)

        # An identifier that maps to some other identifier,
        # associated with a CoverageFailure.
        identifier, output_failure = importer.handle_failure(
            axis_isbn.urn, input_failure
        )
        eq_(axis_id, identifier)
        eq_(input_failure, output_failure)

        # An identifier that maps to some other identifier,
        # in a scenario where what OPDSImporter considers failure
        # is considered success.
        identifier, not_a_failure = importer.handle_failure(
            axis_isbn.urn, self._identifier()
        )
        eq_(axis_id, identifier)
        eq_(axis_id, not_a_failure)
        
        
    def test_coveragefailure_from_message(self):
        """Test all the different ways a <simplified:message> tag might
        become a CoverageFailure.
        """
        data_source = DataSource.lookup(self._db, DataSource.OA_CONTENT_SERVER)
        def f(*args):
            message = OPDSMessage(*args)
            return OPDSImporter.coveragefailure_from_message(
                data_source, message
            )

        # If the URN is invalid we can't create a CoverageFailure.
        invalid_urn = f("urnblah", "500", "description")
        eq_(invalid_urn, None)

        identifier = self._identifier()

        # If the 'message' is that everything is fine, no CoverageFailure
        # is created.
        this_is_fine = f(identifier.urn, "200", "description")
        eq_(None, this_is_fine)

        # Test the various ways the status code and message might be
        # transformed into CoverageFailure.exception.
        description_and_status_code = f(identifier.urn, "404", "description")
        eq_("404: description", description_and_status_code.exception)
        eq_(identifier, description_and_status_code.obj)
        
        description_only = f(identifier.urn, None, "description")
        eq_("description", description_only.exception)
        
        status_code_only = f(identifier.urn, "404", None)
        eq_("404", status_code_only.exception)
        
        no_information = f(identifier.urn, None, None)
        eq_("No detail provided.", no_information.exception)

    def test_coveragefailure_from_message_with_success_status_codes(self):
        """When an OPDSImporter defines SUCCESS_STATUS_CODES, messages with
        those status codes are always treated as successes.
        """
        class Mock(OPDSImporter):
            SUCCESS_STATUS_CODES = [200, 999]

        data_source = DataSource.lookup(self._db, DataSource.OVERDRIVE)

        def f(*args):
            message = OPDSMessage(*args)
            return Mock.coveragefailure_from_message(data_source, message)

        identifier = self._identifier()

        # If the status code is 999, then the identifier is returned
        # instead of a CoverageFailure -- we know that 999 means
        # coverage was in fact provided.
        failure = f(identifier.urn, "999", "hooray!")
        eq_(identifier, failure)

        # If the status code is 200, then the identifier is returned
        # instead of None.
        failure = f(identifier.urn, "200", "ok!")
        eq_(identifier, failure)

        # If the status code is anything else, a CoverageFailure
        # is returned.
        failure = f(identifier.urn, 500, "hooray???")
        assert isinstance(failure, CoverageFailure)
        eq_("500: hooray???", failure.exception)

    def test_extract_metadata_from_elementtree_handles_messages_that_become_identifiers(self):
        not_a_failure = self._identifier()
        class MockOPDSImporter(OPDSImporter):
            @classmethod
            def coveragefailures_from_messages(
                    cls, data_source, message, success_on_200=False):
                """No matter what input we get, we act as though there were 
                a single simplified:message tag in the OPDS feed, which we
                decided to treat as success rather than failure.
                """
                return [not_a_failure]

        data_source = DataSource.lookup(self._db, DataSource.OA_CONTENT_SERVER)

        values, failures = MockOPDSImporter.extract_metadata_from_elementtree(
            self.content_server_mini_feed, data_source
        )
        eq_({not_a_failure.urn: not_a_failure}, failures)
                
        
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

        # There are 3 CoverageFailures - every <entry> threw an
        # exception and the <simplified:message> indicated failure.
        eq_(3, len(failures))

        # The entry with the 202 message became an appropriate
        # CoverageFailure because its data was not extracted through
        # extract_metadata_from_elementtree.
        failure = failures['http://www.gutenberg.org/ebooks/1984']
        assert isinstance(failure, CoverageFailure)
        eq_(True, failure.transient)
        assert failure.exception.startswith('202')
        assert 'Utter failure!' not in failure.exception

        # The other entries became generic CoverageFailures due to the failure
        # of extract_metadata_from_elementtree.
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
        importer = OPDSImporter(self._db, collection=None)

        assert_raises(etree.XMLSyntaxError, importer.import_from_feed, feed)


    def test_import(self):
        feed = self.content_server_mini_feed

        imported_editions, pools, works, failures = (
            OPDSImporter(self._db, collection=None).import_from_feed(feed)
        )

        [crow, mouse] = sorted(imported_editions, key=lambda x: x.title)

        # By default, this feed is treated as though it came from the
        # metadata wrangler. No Work has been created.
        eq_(DataSource.METADATA_WRANGLER, crow.data_source.name)
        eq_(None, crow.work)
        eq_([], crow.license_pools)
        eq_(Edition.BOOK_MEDIUM, crow.medium)

        # not even the 'mouse'
        eq_(None, mouse.work)
        eq_(Edition.PERIODICAL_MEDIUM, mouse.medium)

        # Three links have been added to the identifier of the 'mouse'
        # edition.
        image, thumbnail, description = sorted(
            mouse.primary_identifier.links, key=lambda x: x.rel
        )
        
        # A Representation was imported for the summary with known
        # content.
        description_rep = description.resource.representation
        eq_("This is a summary!", description_rep.content)
        eq_(Representation.TEXT_PLAIN, description_rep.media_type)

        # A Representation was imported for the image with a media type
        # inferred from its URL.
        image_rep = image.resource.representation
        assert image_rep.url.endswith('_9.png')
        eq_(Representation.PNG_MEDIA_TYPE, image_rep.media_type)

        # The thumbnail was imported similarly, and its representation
        # was marked as a thumbnail of the full-sized image.
        thumbnail_rep = thumbnail.resource.representation
        eq_(Representation.PNG_MEDIA_TYPE, thumbnail_rep.media_type)
        eq_(image_rep, thumbnail_rep.thumbnail_of)

        # One link was added to the identifier of the 'crow' edition.
        [image] = crow.primary_identifier.links

        # Because this image did not have a specified media type or a
        # distinctive extension, and we have not actually retrieves
        # the URL yet, we were not able to determine its media type,
        # so it has no associated Representation.
        assert image.resource.url.endswith('/full-cover-image')
        eq_(None, image.resource.representation)

        # Three measurements have been added to the 'mouse' edition.
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
            OPDSImporter(self._db, collection=None).import_from_feed(feed)
        )
        eq_(imported_editions_2, imported_editions)

        # importing with a collection and a lendable data source makes
        # license pools and works.
        imported_editions, pools, works, failures = (
            OPDSImporter(
                self._db,
                collection=self._default_collection,
                data_source_name=DataSource.OA_CONTENT_SERVER
            ).import_from_feed(feed)
        )

        [crow_pool, mouse_pool] = sorted(
            pools, key=lambda x: x.presentation_edition.title
        )
        eq_(self._default_collection, crow_pool.collection)
        eq_(self._default_collection, mouse_pool.collection)
        
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
        """Test that OPDS import creates Edition, LicensePool, and Work
        objects, as appropriate.

        When there is no Collection, it is appropriate to create
        Editions, but not LicensePools or Works.  When there is a
        Collection, it is appropriate to create all three.
        """
        feed = self.content_server_mini_feed

        # This import will create Editions, but not LicensePools or
        # Works, because there is no Collection.
        importer_mw = OPDSImporter(
            self._db, collection=None,
            data_source_name=DataSource.METADATA_WRANGLER
        )
        imported_editions_mw, pools_mw, works_mw, failures_mw = (
            importer_mw.import_from_feed(feed)
        )

        # Both editions were imported, because they were new.
        eq_(2, len(imported_editions_mw))
        
        # But pools and works weren't created, because there is no Collection.
        eq_(0, len(pools_mw))
        eq_(0, len(works_mw))

        # 1 error message, corresponding to the <simplified:message> tag
        # at the end of content_server_mini.opds.
        eq_(1, len(failures_mw))
                
        # Try again, with a Collection to contain the LicensePools.
        importer_g = OPDSImporter(
            self._db, collection=self._default_collection,
        )
        imported_editions_g, pools_g, works_g, failures_g = (
            importer_g.import_from_feed(feed)
        )

        # now pools and works are in, too
        eq_(1, len(failures_g))
        eq_(2, len(pools_g))
        eq_(2, len(works_g))        

        # The pools have presentation editions.
        eq_(set(["The Green Mouse", "Johnny Crow's Party"]),
            set([x.presentation_edition.title for x in pools_g]))
        
        # The information used to create the first LicensePool said
        # that the licensing authority is Project Gutenberg, so that's used
        # as the DataSource for the first LicensePool. The information used
        # to create the second LicensePool didn't include a data source,
        # so the source of the OPDS feed (the open-access content server)
        # was used.
        sources = [pool.data_source.name for pool in pools_g]
        eq_([DataSource.GUTENBERG, DataSource.OA_CONTENT_SERVER], sources)
        
    def test_import_with_unrecognized_distributor_creates_distributor(self):
        """We get a book from a previously unknown data source, with a license
        that comes from a second previously unknown data source. The
        book is imported and both DataSources are created.
        """
        feed = open(
            os.path.join(self.resource_path, "unrecognized_distributor.opds")).read()
        self._default_collection.external_integration.setting('data_source').value = (
            "some new source"
        )
        importer = OPDSImporter(
            self._db,
            collection=self._default_collection,
        )
        imported_editions, pools, works, failures = (
            importer.import_from_feed(feed)
        )
        eq_({}, failures)
       
        # We imported an Edition because there was metadata.
        [edition] = imported_editions
        new_data_source = edition.data_source
        eq_("some new source", new_data_source.name)

        # We imported a LicensePool because there was an open-access
        # link, even though the ultimate source of the link was one
        # we'd never seen before.
        [pool] = pools
        eq_("Unknown Source", pool.data_source.name)

        # From an Edition and a LicensePool we created a Work.
        eq_(1, len(works))

    def test_import_updates_metadata(self):

        path = os.path.join(self.resource_path, "metadata_wrangler_overdrive.opds")
        feed = open(path).read()

        edition, is_new = self._edition(
            DataSource.OVERDRIVE, Identifier.OVERDRIVE_ID,
            with_license_pool=True
        )
        [old_license_pool] = edition.license_pools
        old_license_pool.calculate_work()
        work = old_license_pool.work

        feed = feed.replace("{OVERDRIVE ID}", edition.primary_identifier.identifier)

        self._default_collection.external_integration.setting('data_source').value = (
            DataSource.OVERDRIVE
        )
        imported_editions, imported_pools, imported_works, failures = (
            OPDSImporter(
                self._db,
                collection=self._default_collection,
            ).import_from_feed(feed)
        )

        # The edition we created has had its metadata updated.
        [new_edition] = imported_editions
        eq_(new_edition, edition)
        eq_("The Green Mouse", new_edition.title)
        eq_(DataSource.OVERDRIVE, new_edition.data_source.name)
        
        # But the license pools have not changed.
        eq_(edition.license_pools, [old_license_pool])
        eq_(work.license_pools, [old_license_pool])

    def test_import_from_license_source(self):
        # Instead of importing this data as though it came from the
        # metadata wrangler, let's import it as though it came from the
        # open-access content server.
        feed = self.content_server_mini_feed
        importer = OPDSImporter(
            self._db,
            collection=self._default_collection,
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
            self._db,
            collection=self._default_collection,
            data_source_name=DataSource.OA_CONTENT_SERVER
        )
        imported_editions, imported_pools, imported_works, failures = (
            importer.import_from_feed(feed, immediately_presentation_ready=True)
        )

        [crow, mouse] = sorted(imported_works, key=lambda x: x.title)

        # Both the 'crow' and the 'mouse' book had presentation-ready works created.
        eq_(True, crow.presentation_ready)
        eq_(True, mouse.presentation_ready)


    def test_import_from_feed_treats_message_as_failure(self):
        path = os.path.join(self.resource_path, "unrecognized_identifier.opds")
        feed = open(path).read()
        imported_editions, imported_pools, imported_works, failures = (
            OPDSImporter(
                self._db, collection=self._default_collection
            ).import_from_feed(feed)
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
            DoomedOPDSImporter(
                self._db,
                collection=self._default_collection,
            ).import_from_feed(feed)
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
        self._default_collection.external_integration.setting('data_source').value = (
            DataSource.OA_CONTENT_SERVER
        )
        importer = DoomedWorkOPDSImporter(
            self._db,
            collection=self._default_collection
        )

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

        # If a link turns out to be a dud, consolidate_links()
        # gets rid of it.
        links = [None, None]
        eq_([], OPDSImporter.consolidate_links(links))

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

    def test_import_book_that_offers_no_license(self):
        path = os.path.join(self.resource_path, "book_without_license.opds")
        feed = open(path).read()
        importer = OPDSImporter(self._db, self._default_collection)
        imported_editions, imported_pools, imported_works, failures = (
            importer.import_from_feed(feed)
        )

        # We got an Edition for this book, but no LicensePool and no Work.
        [edition] = imported_editions
        eq_("Howards End", edition.title)
        eq_([], imported_pools)
        eq_([], imported_works)

    def test_build_identifier_mapping(self):
        """Reverse engineers an identifier_mapping based on a list of URNs"""

        collection = self._collection(protocol=ExternalIntegration.AXIS_360)
        lp = self._licensepool(
            None, collection=collection,
            data_source_name=DataSource.AXIS_360
        )

        # Create a couple of ISBN equivalencies.
        isbn1 = self._identifier(
            identifier_type=Identifier.ISBN, foreign_id=self._isbn
        )
        isbn2 = self._identifier(
            identifier_type=Identifier.ISBN, foreign_id=self._isbn
        )
        source = DataSource.lookup(self._db, DataSource.AXIS_360)
        [lp.identifier.equivalent_to(source, isbn, 1) for isbn in [isbn1, isbn2]]

        # The importer is initialized without an identifier mapping.
        importer = OPDSImporter(self._db, collection)
        eq_(None, importer.identifier_mapping)

        # We can build one.
        importer.build_identifier_mapping([isbn1.urn])
        expected = { isbn1 : lp.identifier }
        eq_(expected, importer.identifier_mapping)

        # If we already have one, it's overwritten.
        importer.build_identifier_mapping([isbn2.urn])
        overwrite = { isbn2 : lp.identifier }
        eq_(importer.identifier_mapping, overwrite)

        # If the importer doesn't have a collection, we can't build
        # its mapping.
        importer = OPDSImporter(self._db, None)
        importer.build_identifier_mapping([isbn1])
        eq_(None, importer.identifier_mapping)

    def test_update_work_for_edition_having_multiple_license_pools(self):
        # There are two collections with a LicensePool associated with
        # this Edition.
        edition, lp = self._edition(with_license_pool=True)
        collection2 = self._collection()
        lp2 = self._licensepool(edition=edition, collection=collection2)
        importer = OPDSImporter(self._db, None)

        # Calling update_work_for_edition creates a Work and associates
        # it with the edition.
        eq_(None, edition.work)
        importer.update_work_for_edition(edition)
        work = edition.work
        assert isinstance(work, Work)

        # Both LicensePools are associated with that work.
        eq_(lp.work, work)
        eq_(lp2.work, work)
        

class TestCombine(object):
    """Test that OPDSImporter.combine combines dictionaries in sensible
    ways.
    """
        
    def test_combine(self):
        """An overall test that duplicates a lot of functionality
        in the more specific tests.
        """
        d1 = dict(
            a_list=[1],
            a_scalar="old value",
            a_dict=dict(key1=None, key2=[2], key3="value3")
        )

        d2 = dict(
            a_list=[2],
            a_scalar="new value",
            a_dict=dict(key1="finally a value", key4="value4", key2=[200])
        )

        combined = OPDSImporter.combine(d1, d2)

        # Dictionaries get combined recursively.
        d = combined['a_dict']
        
        # Normal scalar values can be overridden once set.
        eq_("new value", combined['a_scalar'])

        # Missing values are filled in.
        eq_('finally a value', d["key1"])
        eq_('value3', d['key3'])
        eq_('value4', d['key4'])
        
        # Lists get extended.
        eq_([1, 2], combined['a_list'])
        eq_([2, 200], d['key2'])

    def test_combine_null_cases(self):
        """Test combine()'s ability to handle empty and null dictionaries."""
        c = OPDSImporter.combine
        empty = dict()
        nonempty = dict(a=1)
        eq_(nonempty, c(empty, nonempty))
        eq_(empty, c(None, None))
        eq_(nonempty, c(nonempty, None))
        eq_(nonempty, c(None, nonempty))

    def test_combine_missing_value_is_replaced(self):
        c = OPDSImporter.combine
        a_is_missing = dict(b=None)
        a_is_present = dict(a=None, b=None)
        expect = dict(a=None, b=None)
        eq_(expect, c(a_is_missing, a_is_present))

        a_is_present['a'] = True
        expect = dict(a=True, b=None)
        eq_(expect, c(a_is_missing, a_is_present))

    def test_combine_present_value_replaced(self):
        """When both dictionaries define a scalar value, the second
        dictionary's value takes presedence.
        """
        c = OPDSImporter.combine
        a_is_true = dict(a=True)
        a_is_false = dict(a=False)
        eq_(a_is_false, c(a_is_true, a_is_false))
        eq_(a_is_true, c(a_is_false, a_is_true))

        a_is_old = dict(a="old value")
        a_is_new = dict(a="new value")
        eq_("new value", c(a_is_old, a_is_new)['a'])
        
    def test_combine_present_value_not_replaced_with_none(self):

        """When combining a dictionary where a key is set to None
        with a dictionary where that key is present, the value
        is left alone.
        """
        a_is_present = dict(a=True)
        a_is_none = dict(a=None, b=True)
        expect = dict(a=True, b=True)
        eq_(expect, OPDSImporter.combine(a_is_present, a_is_none))

    def test_combine_present_value_extends_list(self):
        """When both dictionaries define a list, the combined value
        is a combined list.
        """
        a_is_true = dict(a=[True])
        a_is_false = dict(a=[False])
        eq_(dict(a=[True, False]), OPDSImporter.combine(a_is_true, a_is_false))

    def test_combine_present_value_extends_dictionary(self):
        """When both dictionaries define a dictionary, the combined value is
        the result of combining the two dictionaries with a recursive
        combine() call.
        """
        a_is_true = dict(a=dict(b=[True]))
        a_is_false = dict(a=dict(b=[False]))
        eq_(dict(a=dict(b=[True, False])),
            OPDSImporter.combine(a_is_true, a_is_false))
        
class TestMirroring(OPDSImporterTest):

    def test_importer_gets_appropriate_mirror_for_collection(self):

        # The default collection is not configured to mirror the
        # resources it finds.
        collection = self._default_collection
        importer = OPDSImporter(self._db, collection=collection)
        eq_(None, importer.mirror)

        # Let's configure a mirror integration for it.
        integration = self._external_integration(
            ExternalIntegration.S3, ExternalIntegration.STORAGE_GOAL,
            username="username", password="password",
            settings = {S3Uploader.BOOK_COVERS_BUCKET_KEY : "some-covers"}
        )
        collection.mirror_integration = integration

        # Now an OPDSImporter created for this collection has an
        # appropriately configured MirrorUploader associated with it.
        importer = OPDSImporter(self._db, collection=collection)
        mirror = importer.mirror
        assert isinstance(mirror, S3Uploader)
        eq_("some-covers",
            mirror.get_bucket(S3Uploader.BOOK_COVERS_BUCKET_KEY))

    def test_resources_are_mirrored_on_import(self):

        svg = """<!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN"
  "http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd">

<svg xmlns="http://www.w3.org/2000/svg" width="1000" height="500">
    <ellipse cx="50" cy="25" rx="50" ry="25" style="fill:blue;"/>
</svg>"""

        http = DummyHTTPClient()
        http.queue_response(
        200, content='I am 10441.epub.images',
        media_type=Representation.EPUB_MEDIA_TYPE
        )
        http.queue_response(
        200, content=svg, media_type=Representation.SVG_MEDIA_TYPE
        )
        http.queue_response(
        200, content='I am 10557.epub.images',
        media_type=Representation.EPUB_MEDIA_TYPE,
        )
        # The request to http://root/full-cover-image
        # will result in a 404 error, and the image will not be mirrored.
        http.queue_response(404, media_type="text/plain")

        s3 = MockS3Uploader()

        importer = OPDSImporter(
            self._db, collection=self._default_collection,
            mirror=s3, http_get=http.do_get
        )

        imported_editions, pools, works, failures = (
            importer.import_from_feed(self.content_server_mini_feed, 
                                      feed_url='http://root')
        )
        e1 = imported_editions[0]
        e2 = imported_editions[1]

        eq_(2, len(pools))
        
        # The import process requested each remote resource in the
        # order they appeared in the OPDS feed. The thumbnail
        # image was not requested, since we were going to make our own
        # thumbnail anyway.
        eq_(http.requests, [
            'http://www.gutenberg.org/ebooks/10441.epub.images',
            'https://s3.amazonaws.com/book-covers.nypl.org/Gutenberg-Illustrated/10441/cover_10441_9.png', 
            'http://www.gutenberg.org/ebooks/10557.epub.images',
            'http://root/full-cover-image',
        ])

        [e1_oa_link, e1_image_link, e1_thumbnail_link, 
         e1_description_link ] = sorted(
            e1.primary_identifier.links, key=lambda x: x.rel
        )
        [e2_image_link, e2_oa_link] = e2.primary_identifier.links

        # The thumbnail image is associated with the Identifier, but
        # it's not used because it's associated with a representation
        # (cover_10441_9.png with media type "image/png") that no
        # longer has a resource associated with it.
        eq_(Hyperlink.THUMBNAIL_IMAGE, e1_thumbnail_link.rel)
        hypothetical_full_representation = e1_thumbnail_link.resource.representation.thumbnail_of
        eq_(None, hypothetical_full_representation.resource)
        eq_(Representation.PNG_MEDIA_TYPE, 
            hypothetical_full_representation.media_type)

        # That's because when we actually got cover_10441_9.png,
        # it turned out to be an SVG file, not a PNG, so we created a new
        # Representation. TODO: Obviously we could do better here.
        eq_(Representation.SVG_MEDIA_TYPE, 
            e1_image_link.resource.representation.media_type)

        # The two open-access links were mirrored to S3, as was the
        # original SVG image and the PNG thumbnail we generated. The
        # PNG image was not mirrored because our attempt to download
        # it resulted in a 404 error.
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
        #
        # The "mouse" book was mirrored to a bucket corresponding to
        # Project Gutenberg, its data source.
        #
        # The images were mirrored to a bucket corresponding to the
        # open-access content server, _their_ data source. Each image
        # has an extension befitting its media type.
        #
        # The "crow" book was mirrored to a bucket corresponding to
        # the open-access content source, the default data source used
        # when no distributor was specified for a book.
        url0 = 'https://s3.amazonaws.com/test.content.bucket/Gutenberg/Gutenberg+ID/10441/The+Green+Mouse.epub.images'
        url1 = u'https://s3.amazonaws.com/test.cover.bucket/Library+Simplified+Open+Access+Content+Server/Gutenberg+ID/10441/cover_10441_9.svg'
        url2 = u'https://s3.amazonaws.com/test.cover.bucket/scaled/300/Library+Simplified+Open+Access+Content+Server/Gutenberg+ID/10441/cover_10441_9.png'
        url3 = 'https://s3.amazonaws.com/test.content.bucket/Library+Simplified+Open+Access+Content+Server/Gutenberg+ID/10557/Johnny+Crow%27s+Party.epub.images'
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
            importer.import_from_feed(self.content_server_mini_feed)
        )

        eq_([e1, e2], imported_editions)

        # Nothing new has been uploaded
        eq_(4, len(s3.uploaded))

        # If the content has changed, it will be mirrored again.
        http.queue_response(
            200, content="I am a new version of 10441.epub.images",
            media_type=Representation.EPUB_MEDIA_TYPE
        )

        http.queue_response(
            200, content=svg,
            media_type=Representation.SVG_MEDIA_TYPE
        )

        http.queue_response(
            200, content="I am a new version of 10557.epub.images",
            media_type=Representation.EPUB_MEDIA_TYPE
        )

        imported_editions, pools, works, failures = (
            importer.import_from_feed(self.content_server_mini_feed)
        )

        eq_([e1, e2], imported_editions)
        eq_(8, len(s3.uploaded))
        eq_("I am a new version of 10441.epub.images", s3.content[4])
        eq_(svg, s3.content[5])
        eq_("I am a new version of 10557.epub.images", s3.content[7])


    def test_content_resources_not_mirrored_on_import_if_no_collection(self):
        """If you don't provide a Collection to the OPDSImporter, no
        LicensePools are created for the book and content resources
        (like EPUB editions of the book) are not mirrored. Only
        metadata resources (like the book cover) are mirrored.
        """
        
        svg = """<!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN"
  "http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd">

<svg xmlns="http://www.w3.org/2000/svg" width="1000" height="500">
    <ellipse cx="50" cy="25" rx="50" ry="25" style="fill:blue;"/>
</svg>"""

        http = DummyHTTPClient()
        # The request to http://root/full-cover-image
        # will result in a 404 error, and the image will not be mirrored.
        http.queue_response(404, media_type="text/plain")
        http.queue_response(
            200, content=svg, media_type=Representation.SVG_MEDIA_TYPE
        )

        s3 = MockS3Uploader()

        importer = OPDSImporter(
            self._db, collection=None,
            mirror=s3, http_get=http.do_get
        )

        imported_editions, pools, works, failures = (
            importer.import_from_feed(self.content_server_mini_feed, 
                                      feed_url='http://root')
        )

        # No LicensePools were created, since no Collection was
        # provided.
        eq_([], pools)
        
        # The import process requested each remote resource in the
        # order they appeared in the OPDS feed. The EPUB resources
        # were not requested because no Collection was provided to the
        # importer. The thumbnail image was not requested, since we
        # were going to make our own thumbnail anyway.
        eq_(http.requests, [
            'https://s3.amazonaws.com/book-covers.nypl.org/Gutenberg-Illustrated/10441/cover_10441_9.png', 
            'http://root/full-cover-image',
        ])


class TestOPDSImportMonitor(OPDSImporterTest):

    def test_constructor(self):
        assert_raises_regexp(
            ValueError,
            "OPDSImportMonitor can only be run in the context of a Collection.",
            OPDSImportMonitor,
            self._db,
            None,
            OPDSImporter,
        )

        self._default_collection.external_integration.protocol = ExternalIntegration.OVERDRIVE
        assert_raises_regexp(
            ValueError,
            "Collection .* is configured for protocol Overdrive, not OPDS Import.",
            OPDSImportMonitor,
            self._db,
            self._default_collection,
            OPDSImporter,
        )

        self._default_collection.external_integration.protocol = ExternalIntegration.OPDS_IMPORT
        self._default_collection.external_integration.setting('data_source').value = None
        assert_raises_regexp(
            ValueError,
            "Collection .* has no associated data source.",
            OPDSImportMonitor,
            self._db,
            self._default_collection,
            OPDSImporter,
        )

    def test_hook_methods(self):
        """By default, the OPDS URL and data source used by the importer 
        come from the collection configuration.
        """
        monitor = OPDSImportMonitor(
            self._db, self._default_collection,
            import_class=OPDSImporter,
        )
        eq_(self._default_collection.external_account_id,
            monitor.opds_url(self._default_collection))

        eq_(self._default_collection.data_source,
            monitor.data_source(self._default_collection))
        
    def test_feed_contains_new_data(self):
        feed = self.content_server_mini_feed

        class MockOPDSImportMonitor(OPDSImportMonitor):
            def _get(self, url, headers):
                return 200, {"content-type": AtomFeed.ATOM_TYPE}, feed

        monitor = OPDSImportMonitor(
            self._db, self._default_collection,
            import_class=OPDSImporter,
        )
        timestamp = monitor.timestamp()
        
        # Nothing has been imported yet, so all data is new.
        eq_(True, monitor.feed_contains_new_data(feed))
        eq_(None, timestamp.timestamp)
        
        # Now import the editions.
        monitor = MockOPDSImportMonitor(
            self._db,
            collection=self._default_collection,
            import_class=OPDSImporter,
        )
        monitor.run()

        # Editions have been imported.
        eq_(2, self._db.query(Edition).count())

        # The timestamp has been updated, although unlike most
        # Monitors the timestamp is purely informational.
        assert timestamp.timestamp != None

        editions = self._db.query(Edition).all()
        data_source = DataSource.lookup(self._db, DataSource.OA_CONTENT_SERVER)

        # If there are CoverageRecords that record work are after the updated
        # dates, there's nothing new.
        record, ignore = CoverageRecord.add_for(
            editions[0], data_source, CoverageRecord.IMPORT_OPERATION
        )
        record.timestamp = datetime.datetime(2016, 1, 1, 1, 1, 1)

        record2, ignore = CoverageRecord.add_for(
            editions[1], data_source, CoverageRecord.IMPORT_OPERATION
        )
        record2.timestamp = datetime.datetime(2016, 1, 1, 1, 1, 1)

        eq_(False, monitor.feed_contains_new_data(feed))

        # If the monitor is set up to force reimport, it doesn't
        # matter that there's nothing new--we act as though there is.
        monitor.force_reimport = True
        eq_(True, monitor.feed_contains_new_data(feed))
        monitor.force_reimport = False

        # If an entry was updated after the date given in that entry's
        # CoverageRecord, there's new data.
        record2.timestamp = datetime.datetime(1970, 1, 1, 1, 1, 1)
        eq_(True, monitor.feed_contains_new_data(feed))

        # If a CoverageRecord is a transient failure, we try again
        # regardless of whether it's been updated.
        for r in [record, record2]:
            r.timestamp = datetime.datetime(2016, 1, 1, 1, 1, 1)
            r.exception = "Failure!"
            r.status = CoverageRecord.TRANSIENT_FAILURE
        eq_(True, monitor.feed_contains_new_data(feed))

        # If a CoverageRecord is a persistent failure, we don't try again...
        for r in [record, record2]:
            r.status = CoverageRecord.PERSISTENT_FAILURE
        eq_(False, monitor.feed_contains_new_data(feed))

        # ...unless the feed updates.
        record.timestamp = datetime.datetime(1970, 1, 1, 1, 1, 1)
        eq_(True, monitor.feed_contains_new_data(feed))

    def http_with_feed(self, feed, content_type=OPDSFeed.ACQUISITION_FEED_TYPE):
        """Helper method to make a DummyHTTPClient with a
        successful OPDS feed response queued.
        """
        return http

    def test_follow_one_link(self):
        monitor = OPDSImportMonitor(
            self._db, collection=self._default_collection,
            import_class=OPDSImporter
        )
        feed = self.content_server_mini_feed

        http = DummyHTTPClient()
        # If there's new data, follow_one_link extracts the next links.
        def follow():
            return monitor.follow_one_link("http://url", do_get=http.do_get)
        http.queue_response(200, OPDSFeed.ACQUISITION_FEED_TYPE, content=feed)
        next_links, content = follow()
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


        # If there's no new data, follow_one_link returns no next
        # links and no content.
        #
        # Note that this works even when the media type is imprecisely
        # specified as Atom or bare XML.
        for imprecise_media_type in OPDSFeed.ATOM_LIKE_TYPES:
            http.queue_response(200, imprecise_media_type, content=feed)
            next_links, content = follow()
            eq_(0, len(next_links))
            eq_(None, content)

        http.queue_response(200, AtomFeed.ATOM_TYPE, content=feed)
        next_links, content = follow()
        eq_(0, len(next_links))
        eq_(None, content)

        # If the media type is missing or is not an Atom feed,
        # an exception is raised.
        http.queue_response(200, None, content=feed)
        assert_raises_regexp(
            BadResponseException, ".*Expected Atom feed, got None.*", follow
        )

        http.queue_response(200, "not/atom", content=feed)
        assert_raises_regexp(
            BadResponseException, ".*Expected Atom feed, got not/atom.*", follow
        )

    def test_import_one_feed(self):
        # Check coverage records are created.

        monitor = OPDSImportMonitor(
            self._db, collection=self._default_collection,
            import_class=DoomedOPDSImporter
        )
        self._default_collection.external_account_id = "http://root-url/index.xml"
        data_source = DataSource.lookup(self._db, DataSource.OA_CONTENT_SERVER)

        feed = self.content_server_mini_feed
        
        monitor.import_one_feed(feed)
        
        editions = self._db.query(Edition).all()
        
        # One edition has been imported
        eq_(1, len(editions))
        [edition] = editions

        # That edition has a CoverageRecord.
        record = CoverageRecord.lookup(
            editions[0].primary_identifier, data_source,
            operation=CoverageRecord.IMPORT_OPERATION
        )
        eq_(CoverageRecord.SUCCESS, record.status)
        eq_(None, record.exception)

        # The edition's primary identifier has a cover link whose
        # relative URL has been resolved relative to the Collection's
        # external_account_id.
        [cover]  = [x.resource.url for x in editions[0].primary_identifier.links
                    if x.rel==Hyperlink.IMAGE]
        eq_("http://root-url/full-cover-image", cover)

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

            def import_one_feed(self, feed):
                self.imports.append(feed)

        monitor = MockOPDSImportMonitor(
            self._db, collection=self._default_collection,
            import_class=OPDSImporter
        )
        
        monitor.queue_response([[], "last page"])
        monitor.queue_response([["second next link"], "second page"])
        monitor.queue_response([["next link"], "first page"])

        monitor.run_once(None, None)

        # Feeds are imported in reverse order
        eq_(["last page", "second page", "first page"], monitor.imports)

    def test_update_headers(self):
        """Test the _update_headers helper method."""
        monitor = OPDSImportMonitor(
            self._db, collection=self._default_collection,
            import_class=OPDSImporter
        )

        # _update_headers return a new dictionary. By default, only an
        # Accept header is added.
        headers = {'Some other': 'header'}
        new_headers = monitor._update_headers(headers)
        eq_(['Some other'], headers.keys())
        eq_(['Some other', 'Accept'], new_headers.keys())

        # If the monitor has a username and password, an Authorization
        # header using HTTP Basic Authentication is also added.
        monitor.username = "a user"
        monitor.password = "a password"
        headers = {}
        new_headers = monitor._update_headers(headers)
        assert new_headers['Authorization'].startswith('Basic')

        # However, if the Authorization and/or Accept headers have been
        # filled in by some other piece of code, _update_headers does
        # not touch them.
        expect = dict(Accept="text/html", Authorization="Bearer abc")
        headers = dict(expect)
        new_headers = monitor._update_headers(headers)
        eq_(headers, expect)
