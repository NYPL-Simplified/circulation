import os
import datetime
import random
from urllib.parse import quote
from io import StringIO
import feedparser
import pytest

from lxml import etree
import pkgutil
from psycopg2.extras import NumericRange

from ..testing import (
    DatabaseTest,
)

from ..config import (
    CannotLoadConfiguration,
    IntegrationException,
)
from ..opds_import import (
    AccessNotAuthenticated,
    MetadataWranglerOPDSLookup,
    OPDSImporter,
    OPDSImportMonitor,
    OPDSXMLParser,
    SimplifiedOPDSLookup,
)
from ..metadata_layer import (
    LinkData,
    CirculationData,
    Metadata,
    TimestampData,
)
from ..model import (
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
    MediaTypes,
    Representation,
    RightsStatus,
    Subject,
    Work,
    WorkCoverageRecord,
)
from ..model.configuration import ExternalIntegrationLink
from ..coverage import CoverageFailure
from ..s3 import (
    S3Uploader,
    MockS3Uploader,
    S3UploaderConfiguration)
from ..selftest import SelfTestResult
from ..testing import (
    DummyHTTPClient,
    MockRequestsRequest,
    MockRequestsResponse,
)
from ..util.http import BadResponseException
from ..util.opds_writer import (
    AtomFeed,
    OPDSFeed,
    OPDSMessage,
)
from ..util.datetime_helpers import datetime_utc, utc_now

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


class OPDSTest(DatabaseTest):
    """A unit test that knows how to find OPDS files for use in tests."""

    def sample_opds(self, filename, file_type="r"):
        base_path = os.path.split(__file__)[0]
        resource_path = os.path.join(base_path, "files", "opds")
        return open(os.path.join(resource_path, filename), file_type).read()


class TestMetadataWranglerOPDSLookup(OPDSTest):

    def setup_method(self):
        super(TestMetadataWranglerOPDSLookup, self).setup_method()
        self.integration = self._external_integration(
            ExternalIntegration.METADATA_WRANGLER,
            goal=ExternalIntegration.METADATA_GOAL,
            password='secret', url="http://metadata.in"
        )
        self.collection = self._collection(
            protocol=ExternalIntegration.OVERDRIVE, external_account_id='library'
        )

    def test_authenticates_wrangler_requests(self):
        """Authenticated details are set for Metadata Wrangler requests
        when they configured for the ExternalIntegration
        """

        lookup = MetadataWranglerOPDSLookup.from_config(self._db)
        assert "secret" == lookup.shared_secret
        assert True == lookup.authenticated

        # The details are None if client configuration isn't set at all.
        self.integration.password = None
        lookup = MetadataWranglerOPDSLookup.from_config(self._db)
        assert None == lookup.shared_secret
        assert False == lookup.authenticated

    def test_add_args(self):
        lookup = MetadataWranglerOPDSLookup.from_config(self._db)
        args = 'greeting=hello'

        # If the base url doesn't have any arguments, args are created.
        base_url = self._url
        assert base_url + '?' + args == lookup.add_args(base_url, args)

        # If the base url has an argument already, additional args are appended.
        base_url = self._url + '?data_source=banana'
        assert base_url + '&' + args == lookup.add_args(base_url, args)

    def test_get_collection_url(self):
        lookup = MetadataWranglerOPDSLookup.from_config(self._db)

        # If the lookup client doesn't have a Collection, an error is
        # raised.
        pytest.raises(
            ValueError, lookup.get_collection_url, 'banana'
        )

        # If the lookup client isn't authenticated, an error is raised.
        lookup.collection = self.collection
        lookup.shared_secret = None
        pytest.raises(
            AccessNotAuthenticated, lookup.get_collection_url, 'banana'
        )

        # With both authentication and a specific Collection,
        # a URL is returned.
        lookup.shared_secret = 'secret'
        expected = '%s%s/banana' % (lookup.base_url, self.collection.metadata_identifier)
        assert expected == lookup.get_collection_url('banana')

        # With an OPDS_IMPORT collection, a data source is included
        opds = self._collection(
            protocol=ExternalIntegration.OPDS_IMPORT,
            external_account_id=self._url,
            data_source_name=DataSource.OA_CONTENT_SERVER
        )
        lookup.collection = opds
        data_source_args = '?data_source=%s' % quote(opds.data_source.name)
        assert lookup.get_collection_url('banana').endswith(data_source_args)

    def test_lookup_endpoint(self):
        # A Collection-specific endpoint is returned if authentication
        # and a Collection is available.
        lookup = MetadataWranglerOPDSLookup.from_config(self._db, collection=self.collection)

        expected = self.collection.metadata_identifier + '/lookup'
        assert expected == lookup.lookup_endpoint

        # Without a collection, an unspecific endpoint is returned.
        lookup.collection = None
        assert 'lookup' == lookup.lookup_endpoint

        # Without authentication, an unspecific endpoint is returned.
        lookup.shared_secret = None
        lookup.collection = self.collection
        assert 'lookup' == lookup.lookup_endpoint

        # With authentication and a collection, a specific endpoint is returned.
        lookup.shared_secret = 'secret'
        expected = '%s/lookup' % self.collection.metadata_identifier
        assert expected == lookup.lookup_endpoint

    # Tests of the self-test framework.

    def test__run_self_tests(self):
        # MetadataWranglerOPDSLookup.run_self_tests() finds all the
        # collections with a metadata identifier, recursively
        # instantates a MetadataWranglerOPDSLookup for each, and calls
        # _run_self_tests_on_one_collection() on each.

        # Ensure there are two collections: one with a metadata
        # identifier and one without.
        no_unique_id = self._default_collection
        with_unique_id = self.collection
        with_unique_id.external_account_id = "unique id"

        # Here, we'll define a Mock class to take the place of the
        # recursively-instantiated MetadataWranglerOPDSLookup.
        class Mock(MetadataWranglerOPDSLookup):
            instances = []

            @classmethod
            def from_config(cls, _db, collection):
                lookup = Mock("http://mock-url/")
                cls.instances.append(lookup)
                lookup._db = _db
                lookup.collection = collection
                lookup.called = False
                return lookup

            def _run_collection_self_tests(self):
                self.called = True
                yield "Some self-test results for %s" % self.collection.name

        lookup = MetadataWranglerOPDSLookup("http://url/")

        # Running the self tests with no specific collection caused
        # them to be run on the one Collection we could find that has
        # a metadata identifier.

        # _run_self_tests returns a single test result
        [result] = lookup._run_self_tests(self._db, lookup_class=Mock)

        # That Collection is keyed to a list containing a single test
        # result, obtained by calling Mock._run_collection_self_tests().
        assert "Some self-test results for %s" % with_unique_id.name == result

        # Here's the Mock object whose _run_collection_self_tests()
        # was called. Let's make sure it was instantiated properly.
        [mock_lookup] = Mock.instances
        assert self._db == mock_lookup._db
        assert with_unique_id == mock_lookup.collection
        assert True == mock_lookup.called

    def test__run_collection_self_tests(self):
        # Verify that calling _run_collection_self_tests calls
        # _feed_self_test a couple of times, and yields a
        # SelfTestResult for each call.

        class Mock(MetadataWranglerOPDSLookup):
            feed_self_tests = []

            def _feed_self_test(self, title, method, *args):
                self.feed_self_tests.append((title, method, args))
                return "A feed self-test for %s: %s" % (
                    self.collection.unique_account_id, title
                )

        # If there is no associated collection, _run_collection_self_tests()
        # does nothing.
        no_collection = Mock("http://url/")
        assert [] == list(no_collection._run_collection_self_tests())

        # Same if there is an associated collection but it has no
        # metadata identifier.
        with_collection = Mock(
            "http://url/", collection=self._default_collection
        )
        assert [] == list(with_collection._run_collection_self_tests())

        # If there is a metadata identifier, our mocked
        # _feed_self_test is called twice. Here are the results.
        self._default_collection.external_account_id = "unique-id"
        [r1, r2] = with_collection._run_collection_self_tests()

        assert (
            'A feed self-test for unique-id: Metadata updates in last 24 hours' ==
            r1)
        assert (
            "A feed self-test for unique-id: Titles where we could (but haven't) provide information to the metadata wrangler" ==
            r2)

        # Let's make sure _feed_self_test() was called with the right
        # arguments.
        call1, call2 = with_collection.feed_self_tests

        # The first self-test wants to count updates for the last 24
        # hours.
        title1, method1, args1 = call1
        assert 'Metadata updates in last 24 hours' == title1
        assert with_collection.updates == method1
        [timestamp] = args1
        one_day_ago = utc_now() - datetime.timedelta(hours=24)
        assert (one_day_ago - timestamp).total_seconds() < 1

        # The second self-test wants to count work that the metadata
        # wrangler needs done but hasn't been done yet.
        title2, method2, args2 = call2
        assert (
            "Titles where we could (but haven't) provide information to the metadata wrangler" ==
            title2)
        assert with_collection.metadata_needed == method2
        assert () == args2

    def test__feed_self_test(self):
        # Test the _feed_self_test helper method. It grabs a
        # feed from the metadata wrangler, calls
        # _summarize_feed_response on the response object, and returns
        # a SelfTestResult explaining what happened.
        class Mock(MetadataWranglerOPDSLookup):
            requests = []
            annotated_responses = []
            @classmethod
            def _annotate_feed_response(cls, result, response):
                cls.annotated_responses.append((result, response))
                result.success = True
                result.result = ["I summarized", "the response"]

            def make_some_request(self, *args, **kwargs):
                self.requests.append((args, kwargs))
                return "A fake response"

        lookup = Mock("http://base-url/", collection=self._default_collection)
        request_method = lookup.make_some_request
        result = lookup._feed_self_test("Some test", request_method, 1, 2)

        # We got back a SelfTestResult associated with the Mock
        # object's collection.
        assert isinstance(result, SelfTestResult)
        assert self._default_collection == result.collection

        # It indicates some request was made, and the response
        # annotated using our mock _annotate_feed_response.
        assert "Some test" == result.name
        assert result.duration < 1
        assert True == result.success
        assert ["I summarized", "the response"] == result.result

        # But what request was made, exactly?

        # Here we see that Mock.make_some_request was called
        # with the positional arguments passed into _feed_self_test,
        # and a keyword argument indicating that 5xx responses should
        # be processed normally and not used as a reason to raise an
        # exception.
        assert (
            [((1, 2),
              {'allowed_response_codes': ['1xx', '2xx', '3xx', '4xx', '5xx']})
            ] ==
            lookup.requests)

        # That method returned "A fake response", which was passed
        # into _annotate_feed_response, along with the
        # SelfTestResult in progress.
        [(used_result, response)] = lookup.annotated_responses
        assert result == used_result
        assert "A fake response" == response

    def test__annotate_feed_response(self):
        # Test the _annotate_feed_response class helper method.
        m = MetadataWranglerOPDSLookup._annotate_feed_response
        def mock_response(url, authorization, response_code, content):
            request = MockRequestsRequest(
                url, headers=dict(Authorization=authorization)
            )
            response = MockRequestsResponse(
                response_code, content=content, request=request
            )
            return response

        # First, test success.
        url = "http://metadata-wrangler/",
        auth = "auth"
        test_result = SelfTestResult("success")
        response = mock_response(
            url, auth, 200,
            self.sample_opds("metadata_wrangler_overdrive.opds")
        )
        results = m(test_result, response)
        assert [
            'Request URL: %s' % url,
            'Request authorization: %s' % auth,
            'Status code: 200',
            'Total identifiers registered with this collection: 201',
            'Entries on this page: 1',
            ' The Green Mouse'
        ] == test_result.result
        assert True == test_result.success

        # Next, test failure.
        response = mock_response(
            url, auth, 401,
            "An error message."
        )
        test_result = SelfTestResult("failure")
        assert False == test_result.success
        m(test_result, response)
        assert [
            'Request URL: %s' % url,
            'Request authorization: %s' % auth,
            'Status code: 401',
        ] == test_result.result

    def test_external_integration(self):
        result = MetadataWranglerOPDSLookup.external_integration(self._db)
        assert result.protocol == ExternalIntegration.METADATA_WRANGLER
        assert result.goal == ExternalIntegration.METADATA_GOAL

class OPDSImporterTest(OPDSTest):

    def setup_method(self):
        super(OPDSImporterTest, self).setup_method()
        self.content_server_feed = self.sample_opds("content_server.opds")
        self.content_server_mini_feed = self.sample_opds("content_server_mini.opds")
        self.audiobooks_opds = self.sample_opds("audiobooks.opds")
        self.feed_with_id_and_dcterms_identifier = self.sample_opds("feed_with_id_and_dcterms_identifier.opds", "rb")
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
        assert Representation.cautious_http_get == importer.http_get

        # But you can pass in anything you want.
        do_get = object()
        importer = OPDSImporter(self._db, collection=None, http_get=do_get)
        assert do_get == importer.http_get

    def test_data_source_autocreated(self):
        name = "New data source " + self._str
        importer = OPDSImporter(
            self._db, collection=None, data_source_name=name
        )
        source1 = importer.data_source
        assert name == source1.name

    def test_extract_next_links(self):
        importer = OPDSImporter(
            self._db, collection=None, data_source_name=DataSource.NYT
        )
        next_links = importer.extract_next_links(
            self.content_server_mini_feed
        )

        assert 1 == len(next_links)
        assert "http://localhost:5000/?after=327&size=100" == next_links[0]

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

        assert 2 == len(last_update_dates)

        identifier1, updated1 = last_update_dates[0]
        identifier2, updated2 = last_update_dates[1]

        assert "urn:librarysimplified.org/terms/id/Gutenberg%20ID/10441" == identifier1
        assert datetime_utc(2015, 1, 2, 16, 56, 40) == updated1

        assert "urn:librarysimplified.org/terms/id/Gutenberg%20ID/10557" == identifier2
        assert datetime_utc(2015, 1, 2, 16, 56, 40) == updated2

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
        assert [] == last_update_dates

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

        assert "The Green Mouse" == m1.title
        assert "A Tale of Mousy Terror" == m1.subtitle

        assert data_source_name == m1._data_source
        assert data_source_name == m2._data_source
        assert data_source_name == c1._data_source
        assert data_source_name == c2._data_source

        [failure] = list(failures.values())
        assert "202: I'm working to locate a source for this identifier." == failure.exception

    def test_use_dcterm_identifier_as_id_with_id_and_dcterms_identifier(self):
        data_source_name = "Data source name " + self._str
        collection_to_test = self._default_collection
        collection_to_test.primary_identifier_source = ExternalIntegration.DCTERMS_IDENTIFIER
        importer = OPDSImporter(
            self._db, collection=collection_to_test, data_source_name=data_source_name,
        )

        metadata, failures = importer.extract_feed_data(
            self.feed_with_id_and_dcterms_identifier
        )

        # First book doesn't have <dcterms:identifier>, so <id> must be used as identifier
        book_1 = metadata.get('https://root.uri/1')
        assert book_1 != None
        # Second book have <id> and <dcterms:identifier>, so <dcters:identifier> must be used as id
        book_2 = metadata.get('urn:isbn:9781468316438')
        assert book_2 != None
        # Verify if id was add in the end of identifier
        book_2_identifiers = book_2.identifiers
        found = False
        for entry in book_2.identifiers:
            if entry.identifier == 'https://root.uri/2':
                found = True
                break
        assert found == True
        # Third book has more than one dcterms:identifers, all of then must be present as metadata identifier
        book_3 = metadata.get('urn:isbn:9781683351993')
        assert book_2 != None
        # Verify if id was add in the end of identifier
        book_3_identifiers = book_3.identifiers
        expected_identifier = [
            '9781683351993',
            'https://root.uri/3',
            '9781683351504',
            '9780312939458',
        ]
        result_identifier = [entry.identifier for entry in book_3.identifiers]
        assert set(expected_identifier) == set(result_identifier)

    def test_use_id_with_existing_dcterms_identifier(self):
        data_source_name = "Data source name " + self._str
        collection_to_test = self._default_collection
        collection_to_test.primary_identifier_source = None
        importer = OPDSImporter(
            self._db, collection=collection_to_test, data_source_name=data_source_name,
        )

        metadata, failures = importer.extract_feed_data(
            self.feed_with_id_and_dcterms_identifier
        )

        book_1 = metadata.get('https://root.uri/1')
        assert book_1 != None
        book_2 = metadata.get('https://root.uri/2')
        assert book_2 != None
        book_3 = metadata.get('https://root.uri/3')
        assert book_3 != None

    def test_extract_link(self):
        no_rel = AtomFeed.E.link(href="http://foo/")
        assert None == OPDSImporter.extract_link(no_rel)

        no_href = AtomFeed.E.link(href="", rel="foo")
        assert None == OPDSImporter.extract_link(no_href)

        good = AtomFeed.E.link(href="http://foo", rel="bar")
        link = OPDSImporter.extract_link(good)
        assert "http://foo" == link.href
        assert "bar" == link.rel

        relative = AtomFeed.E.link(href="/foo/bar", rel="self")
        link = OPDSImporter.extract_link(relative, "http://server")
        assert "http://server/foo/bar" == link.href

    def test_get_medium_from_links(self):
        audio_links = [
            LinkData(href="url", rel="http://opds-spec.org/acquisition/",
                     media_type="application/audiobook+json;param=value"
            ),
            LinkData(href="url", rel="http://opds-spec.org/image"),
        ]
        book_links = [
            LinkData(href="url", rel="http://opds-spec.org/image"),
            LinkData(
                href="url", rel="http://opds-spec.org/acquisition/",
                media_type=random.choice(
                    MediaTypes.BOOK_MEDIA_TYPES
                ) + ";param=value"
            ),
        ]

        m = OPDSImporter.get_medium_from_links

        assert m(audio_links) == "Audio"
        assert m(book_links) == "Book"

    def test_extract_link_rights_uri(self):

        # Most of the time, a link's rights URI is inherited from the entry.
        entry_rights = RightsStatus.PUBLIC_DOMAIN_USA

        link_tag = AtomFeed.E.link(href="http://foo", rel="bar")
        link = OPDSImporter.extract_link(
            link_tag, entry_rights_uri=entry_rights
        )
        assert RightsStatus.PUBLIC_DOMAIN_USA == link.rights_uri

        # But a dcterms:rights tag beneath the link can override this.
        rights_attr = "{%s}rights" % AtomFeed.DCTERMS_NS
        link_tag.attrib[rights_attr] = RightsStatus.IN_COPYRIGHT
        link = OPDSImporter.extract_link(
            link_tag, entry_rights_uri=entry_rights
        )
        assert RightsStatus.IN_COPYRIGHT == link.rights_uri

    def test_extract_data_from_feedparser(self):
        data_source = DataSource.lookup(self._db, DataSource.OA_CONTENT_SERVER)
        importer = OPDSImporter(self._db, None, data_source_name=data_source.name)
        values, failures = importer.extract_data_from_feedparser(
            self.content_server_mini_feed, data_source
        )

        # The <entry> tag became a Metadata object.
        metadata = values['urn:librarysimplified.org/terms/id/Gutenberg%20ID/10441']
        assert "The Green Mouse" == metadata['title']
        assert "A Tale of Mousy Terror" == metadata['subtitle']
        assert 'en' == metadata['language']
        assert 'Project Gutenberg' == metadata['publisher']

        circulation = metadata['circulation']
        assert DataSource.GUTENBERG == circulation['data_source']

        # The <simplified:message> tag did not become a
        # CoverageFailure -- that's handled by
        # extract_metadata_from_elementtree.
        assert {} == failures

    def test_extract_data_from_feedparser_handles_exception(self):
        class DoomedFeedparserOPDSImporter(OPDSImporter):
            """An importer that can't extract metadata from feedparser."""
            @classmethod
            def _data_detail_for_feedparser_entry(cls, entry, data_source):
                raise Exception("Utter failure!")

        data_source = DataSource.lookup(self._db, DataSource.OA_CONTENT_SERVER)
        importer = DoomedFeedparserOPDSImporter(self._db, None, data_source_name=data_source.name)
        values, failures = importer.extract_data_from_feedparser(
            self.content_server_mini_feed, data_source
        )

        # No metadata was extracted.
        assert 0 == len(list(values.keys()))

        # There are 2 failures, both from exceptions. The 202 message
        # found in content_server_mini.opds is not extracted
        # here--it's extracted by extract_metadata_from_elementtree.
        assert 2 == len(failures)

        # The first error message became a CoverageFailure.
        failure = failures['urn:librarysimplified.org/terms/id/Gutenberg%20ID/10441']
        assert isinstance(failure, CoverageFailure)
        assert True == failure.transient
        assert "Utter failure!" in failure.exception

        # The second error message became a CoverageFailure.
        failure = failures['urn:librarysimplified.org/terms/id/Gutenberg%20ID/10557']
        assert isinstance(failure, CoverageFailure)
        assert True == failure.transient
        assert "Utter failure!" in failure.exception

    def test_extract_metadata_from_elementtree(self):

        data_source = DataSource.lookup(self._db, DataSource.OA_CONTENT_SERVER)

        data, failures = OPDSImporter.extract_metadata_from_elementtree(
            self.content_server_feed, data_source
        )

        # There are 76 entries in the feed, and we got metadata for
        # every one of them.
        assert 76 == len(data)
        assert 0 == len(failures)

        # We're going to do spot checks on a book and a periodical.

        # First, the book.
        book_id = 'urn:librarysimplified.org/terms/id/Gutenberg%20ID/1022'
        book = data[book_id]
        assert Edition.BOOK_MEDIUM == book['medium']

        [contributor] = book['contributors']
        assert "Thoreau, Henry David" == contributor.sort_name
        assert [Contributor.AUTHOR_ROLE] == contributor.roles

        subjects = book['subjects']
        assert ['LCSH', 'LCSH', 'LCSH', 'LCC'] == [x.type for x in subjects]
        assert (
            ['Essays', 'Nature', 'Walking', 'PS'] ==
            [x.identifier for x in subjects])
        assert (
            [None, None, None, 'American Literature'] ==
            [x.name for x in book['subjects']])
        assert (
            [1, 1, 1, 10] ==
            [x.weight for x in book['subjects']])

        assert [] == book['measurements']

        assert datetime_utc(1862, 6, 1) == book["published"]

        [link] = book['links']
        assert Hyperlink.OPEN_ACCESS_DOWNLOAD == link.rel
        assert "http://www.gutenberg.org/ebooks/1022.epub.noimages" == link.href
        assert Representation.EPUB_MEDIA_TYPE == link.media_type

        # And now, the periodical.
        periodical_id = 'urn:librarysimplified.org/terms/id/Gutenberg%20ID/10441'
        periodical = data[periodical_id]
        assert Edition.PERIODICAL_MEDIUM == periodical['medium']

        subjects = periodical['subjects']
        assert (
            ['LCSH', 'LCSH', 'LCSH', 'LCSH', 'LCC', 'schema:audience', 'schema:typicalAgeRange'] ==
            [x.type for x in subjects])
        assert (
            ['Courtship -- Fiction', 'New York (N.Y.) -- Fiction', 'Fantasy fiction', 'Magic -- Fiction', 'PZ', 'Children', '7'] ==
            [x.identifier for x in subjects])
        assert [1, 1, 1, 1, 1, 1, 1] == [x.weight for x in subjects]

        r1, r2, r3 = periodical['measurements']

        assert Measurement.QUALITY == r1.quantity_measured
        assert 0.3333 == r1.value
        assert 1 == r1.weight

        assert Measurement.RATING == r2.quantity_measured
        assert 0.6 == r2.value
        assert 1 == r2.weight

        assert Measurement.POPULARITY == r3.quantity_measured
        assert 0.25 == r3.value
        assert 1 == r3.weight

        assert 'Animal Colors' == periodical['series']
        assert '1' == periodical['series_position']

        assert datetime_utc(1910, 1, 1) == periodical["published"]

    def test_extract_metadata_from_elementtree_treats_message_as_failure(self):
        data_source = DataSource.lookup(self._db, DataSource.OA_CONTENT_SERVER)

        feed = self.sample_opds("unrecognized_identifier.opds")
        values, failures = OPDSImporter.extract_metadata_from_elementtree(
            feed, data_source
        )

        # We have no Metadata objects and one CoverageFailure.
        assert {} == values

        # The CoverageFailure contains the information that was in a
        # <simplified:message> tag in unrecognized_identifier.opds.
        key = 'http://www.gutenberg.org/ebooks/100'
        assert [key] == list(failures.keys())
        failure = failures[key]
        assert "404: I've never heard of this work." == failure.exception
        assert key == failure.obj.urn

    def test_extract_messages(self):
        parser = OPDSXMLParser()
        feed = self.sample_opds("unrecognized_identifier.opds")
        root = etree.parse(StringIO(feed))
        [message] = OPDSImporter.extract_messages(parser, root)
        assert 'urn:librarysimplified.org/terms/id/Gutenberg ID/100' == message.urn
        assert 404 == message.status_code
        assert "I've never heard of this work." == message.message

    def test_extract_medium(self):
        m = OPDSImporter.extract_medium

        # No tag -- the default is used.
        assert "Default" == m(None, "Default")

        def medium(additional_type, format, default="Default"):
            # Make an <atom:entry> tag with the given tags.
            # Parse it and call extract_medium on it.
            entry= '<entry xmlns:schema="http://schema.org/" xmlns:dcterms="http://purl.org/dc/terms/"'
            if additional_type:
                entry += ' schema:additionalType="%s"' % additional_type
            entry += '>'
            if format:
                entry += '<dcterms:format>%s</dcterms:format>' % format
            entry += '</entry>'
            tag = etree.parse(StringIO(entry))
            return m(tag.getroot(), default=default)

        audio_type = random.choice(MediaTypes.AUDIOBOOK_MEDIA_TYPES) + ";param=value"
        ebook_type = random.choice(MediaTypes.BOOK_MEDIA_TYPES) + ";param=value"

        # schema:additionalType is checked first. If present, any
        # potentially contradictory information in dcterms:format is
        # ignored.
        assert (
            Edition.AUDIO_MEDIUM ==
            medium("http://bib.schema.org/Audiobook", ebook_type))
        assert (
            Edition.BOOK_MEDIUM ==
            medium("http://schema.org/EBook", audio_type))

        # When schema:additionalType is missing or not useful, the
        # value of dcterms:format is mapped to a medium using
        # Edition.medium_from_media_type.
        assert Edition.AUDIO_MEDIUM == medium("something-else", audio_type)
        assert Edition.BOOK_MEDIUM == medium(None, ebook_type)

        # If both pieces of information are missing or useless, the
        # default is used.
        assert "Default" == medium(None, None)
        assert "Default" == medium("something-else", "image/jpeg")

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
        assert expect_identifier == identifier
        assert input_failure == output_failure

        # A normal OPDSImporter would consider this a failure, but
        # because the 'failure' is an Identifier, not a
        # CoverageFailure, we're going to treat it as a success.
        identifier, not_a_failure = importer.handle_failure(
            "urn:isbn:9781449358068", self._identifier()
        )
        assert expect_identifier == identifier
        assert identifier == not_a_failure
        # Note that the 'failure' object retuned is the Identifier that
        # was passed in, not the Identifier that substituted as the 'failure'.
        # (In real usage, though, they should be the same.)

        # An identifier that maps to some other identifier,
        # associated with a CoverageFailure.
        identifier, output_failure = importer.handle_failure(
            axis_isbn.urn, input_failure
        )
        assert axis_id == identifier
        assert input_failure == output_failure

        # An identifier that maps to some other identifier,
        # in a scenario where what OPDSImporter considers failure
        # is considered success.
        identifier, not_a_failure = importer.handle_failure(
            axis_isbn.urn, self._identifier()
        )
        assert axis_id == identifier
        assert axis_id == not_a_failure


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
        assert invalid_urn == None

        identifier = self._identifier()

        # If the 'message' is that everything is fine, no CoverageFailure
        # is created.
        this_is_fine = f(identifier.urn, "200", "description")
        assert None == this_is_fine

        # Test the various ways the status code and message might be
        # transformed into CoverageFailure.exception.
        description_and_status_code = f(identifier.urn, "404", "description")
        assert "404: description" == description_and_status_code.exception
        assert identifier == description_and_status_code.obj

        description_only = f(identifier.urn, None, "description")
        assert "description" == description_only.exception

        status_code_only = f(identifier.urn, "404", None)
        assert "404" == status_code_only.exception

        no_information = f(identifier.urn, None, None)
        assert "No detail provided." == no_information.exception

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
        assert identifier == failure

        # If the status code is 200, then the identifier is returned
        # instead of None.
        failure = f(identifier.urn, "200", "ok!")
        assert identifier == failure

        # If the status code is anything else, a CoverageFailure
        # is returned.
        failure = f(identifier.urn, 500, "hooray???")
        assert isinstance(failure, CoverageFailure)
        assert "500: hooray???" == failure.exception

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
        assert {not_a_failure.urn: not_a_failure} == failures


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
        assert 0 == len(list(values.keys()))

        # There are 3 CoverageFailures - every <entry> threw an
        # exception and the <simplified:message> indicated failure.
        assert 3 == len(failures)

        # The entry with the 202 message became an appropriate
        # CoverageFailure because its data was not extracted through
        # extract_metadata_from_elementtree.
        failure = failures['http://www.gutenberg.org/ebooks/1984']
        assert isinstance(failure, CoverageFailure)
        assert True == failure.transient
        assert failure.exception.startswith('202')
        assert 'Utter failure!' not in failure.exception

        # The other entries became generic CoverageFailures due to the failure
        # of extract_metadata_from_elementtree.
        failure = failures['urn:librarysimplified.org/terms/id/Gutenberg%20ID/10441']
        assert isinstance(failure, CoverageFailure)
        assert True == failure.transient
        assert "Utter failure!" in failure.exception

        failure = failures['urn:librarysimplified.org/terms/id/Gutenberg%20ID/10557']
        assert isinstance(failure, CoverageFailure)
        assert True == failure.transient
        assert "Utter failure!" in failure.exception

    def test_import_exception_if_unable_to_parse_feed(self):
        feed = "I am not a feed."
        importer = OPDSImporter(self._db, collection=None)

        pytest.raises(etree.XMLSyntaxError, importer.import_from_feed, feed)


    def test_import(self):
        feed = self.content_server_mini_feed

        imported_editions, pools, works, failures = (
            OPDSImporter(self._db, collection=None).import_from_feed(feed)
        )

        [crow, mouse] = sorted(imported_editions, key=lambda x: x.title)

        # By default, this feed is treated as though it came from the
        # metadata wrangler. No Work has been created.
        assert DataSource.METADATA_WRANGLER == crow.data_source.name
        assert None == crow.work
        assert [] == crow.license_pools
        assert Edition.BOOK_MEDIUM == crow.medium

        # not even the 'mouse'
        assert None == mouse.work
        assert Edition.PERIODICAL_MEDIUM == mouse.medium

        # Three links have been added to the identifier of the 'mouse'
        # edition.
        image, thumbnail, description = sorted(
            mouse.primary_identifier.links, key=lambda x: x.rel
        )

        # A Representation was imported for the summary with known
        # content.
        description_rep = description.resource.representation
        assert b"This is a summary!" == description_rep.content
        assert Representation.TEXT_PLAIN == description_rep.media_type

        # A Representation was imported for the image with a media type
        # inferred from its URL.
        image_rep = image.resource.representation
        assert image_rep.url.endswith('_9.png')
        assert Representation.PNG_MEDIA_TYPE == image_rep.media_type

        # The thumbnail was imported similarly, and its representation
        # was marked as a thumbnail of the full-sized image.
        thumbnail_rep = thumbnail.resource.representation
        assert Representation.PNG_MEDIA_TYPE == thumbnail_rep.media_type
        assert image_rep == thumbnail_rep.thumbnail_of

        # Two links were added to the identifier of the 'crow' edition.
        [broken_image, working_image] = sorted(
            crow.primary_identifier.links, key=lambda x: x.resource.url
        )

        # Because these images did not have a specified media type or a
        # distinctive extension, and we have not actually retrieved
        # the URLs yet, we were not able to determine their media type,
        # so they have no associated Representation.
        assert broken_image.resource.url.endswith('/broken-cover-image')
        assert working_image.resource.url.endswith('/working-cover-image')
        assert None == broken_image.resource.representation
        assert None == working_image.resource.representation

        # Three measurements have been added to the 'mouse' edition.
        popularity, quality, rating = sorted(
            [x for x in mouse.primary_identifier.measurements
             if x.is_most_recent],
            key=lambda x: x.quantity_measured
        )

        assert DataSource.METADATA_WRANGLER == popularity.data_source.name
        assert Measurement.POPULARITY == popularity.quantity_measured
        assert 0.25 == popularity.value

        assert DataSource.METADATA_WRANGLER == quality.data_source.name
        assert Measurement.QUALITY == quality.quantity_measured
        assert 0.3333 == quality.value

        assert DataSource.METADATA_WRANGLER == rating.data_source.name
        assert Measurement.RATING == rating.quantity_measured
        assert 0.6 == rating.value

        seven, children, courtship, fantasy, pz, magic, new_york = sorted(
            mouse.primary_identifier.classifications,
            key=lambda x: x.subject.name)

        pz_s = pz.subject
        assert "Juvenile Fiction" == pz_s.name
        assert "PZ" == pz_s.identifier

        new_york_s = new_york.subject
        assert "New York (N.Y.) -- Fiction" == new_york_s.name
        assert "sh2008108377" == new_york_s.identifier

        assert '7' == seven.subject.identifier
        assert 100 == seven.weight
        assert Subject.AGE_RANGE == seven.subject.type
        from ..classifier import Classifier
        classifier = Classifier.classifiers.get(seven.subject.type, None)
        classifier.classify(seven.subject)

        # If we import the same file again, we get the same list of Editions.
        imported_editions_2, pools_2, works_2, failures_2 = (
            OPDSImporter(self._db, collection=None).import_from_feed(feed)
        )
        assert imported_editions_2 == imported_editions

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
        assert self._default_collection == crow_pool.collection
        assert self._default_collection == mouse_pool.collection

        # Work was created for both books.
        assert crow_pool.work is not None
        assert Edition.BOOK_MEDIUM == crow_pool.presentation_edition.medium

        assert mouse_pool.work is not None
        assert Edition.PERIODICAL_MEDIUM == mouse_pool.presentation_edition.medium

        work = mouse_pool.work
        work.calculate_presentation()
        assert 0.4142 == round(work.quality, 4)
        assert Classifier.AUDIENCE_CHILDREN == work.audience
        assert NumericRange(7,7, '[]') == work.target_age

        # Bonus: make sure that delivery mechanisms are set appropriately.
        [mech] = mouse_pool.delivery_mechanisms
        assert Representation.EPUB_MEDIA_TYPE == mech.delivery_mechanism.content_type
        assert DeliveryMechanism.NO_DRM == mech.delivery_mechanism.drm_scheme
        assert ('http://www.gutenberg.org/ebooks/10441.epub.images' ==
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
        assert 2 == len(imported_editions_mw)

        # But pools and works weren't created, because there is no Collection.
        assert 0 == len(pools_mw)
        assert 0 == len(works_mw)

        # 1 error message, corresponding to the <simplified:message> tag
        # at the end of content_server_mini.opds.
        assert 1 == len(failures_mw)

        # Try again, with a Collection to contain the LicensePools.
        importer_g = OPDSImporter(
            self._db, collection=self._default_collection,
        )
        imported_editions_g, pools_g, works_g, failures_g = (
            importer_g.import_from_feed(feed)
        )

        # now pools and works are in, too
        assert 1 == len(failures_g)
        assert 2 == len(pools_g)
        assert 2 == len(works_g)

        # The pools have presentation editions.
        assert (set(["The Green Mouse", "Johnny Crow's Party"]) ==
            set([x.presentation_edition.title for x in pools_g]))

        # The information used to create the first LicensePool said
        # that the licensing authority is Project Gutenberg, so that's used
        # as the DataSource for the first LicensePool. The information used
        # to create the second LicensePool didn't include a data source,
        # so the source of the OPDS feed (the open-access content server)
        # was used.
        assert set([DataSource.GUTENBERG, DataSource.OA_CONTENT_SERVER]) == \
            set([pool.data_source.name for pool in pools_g])

    def test_import_with_unrecognized_distributor_creates_distributor(self):
        """We get a book from a previously unknown data source, with a license
        that comes from a second previously unknown data source. The
        book is imported and both DataSources are created.
        """
        feed = self.sample_opds("unrecognized_distributor.opds")
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
        assert {} == failures

        # We imported an Edition because there was metadata.
        [edition] = imported_editions
        new_data_source = edition.data_source
        assert "some new source" == new_data_source.name

        # We imported a LicensePool because there was an open-access
        # link, even though the ultimate source of the link was one
        # we'd never seen before.
        [pool] = pools
        assert "Unknown Source" == pool.data_source.name

        # From an Edition and a LicensePool we created a Work.
        assert 1 == len(works)

    def test_import_updates_metadata(self):

        feed = self.sample_opds("metadata_wrangler_overdrive.opds")

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
        assert new_edition == edition
        assert "The Green Mouse" == new_edition.title
        assert DataSource.OVERDRIVE == new_edition.data_source.name

        # But the license pools have not changed.
        assert edition.license_pools == [old_license_pool]
        assert work.license_pools == [old_license_pool]

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
        assert DataSource.GUTENBERG == mouse_pool.data_source.name

        # But the license pool's presentation edition has a data
        # source associated with the Library Simplified open-access
        # content server, since that's where the metadata comes from.
        assert (DataSource.OA_CONTENT_SERVER ==
            mouse_pool.presentation_edition.data_source.name)

        # Since the 'mouse' book came with an open-access link, the license
        # pool delivery mechanism has been marked as open access.
        assert True == mouse_pool.open_access
        assert (RightsStatus.GENERIC_OPEN_ACCESS ==
            mouse_pool.delivery_mechanisms[0].rights_status.uri)

        # The 'mouse' work was marked presentation-ready immediately.
        assert True == mouse_pool.work.presentation_ready

        # The OPDS feed didn't actually say where the 'crow' book
        # comes from, but we did tell the importer to use the open access
        # content server as the data source, so both a Work and a LicensePool
        # were created, and their data source is the open access content server,
        # not Project Gutenberg.
        assert DataSource.OA_CONTENT_SERVER == crow_pool.data_source.name

    def test_import_from_feed_treats_message_as_failure(self):
        feed = self.sample_opds("unrecognized_identifier.opds")
        imported_editions, imported_pools, imported_works, failures = (
            OPDSImporter(
                self._db, collection=self._default_collection
            ).import_from_feed(feed)
        )

        [failure] = list(failures.values())
        assert isinstance(failure, CoverageFailure)
        assert True == failure.transient
        assert "404: I've never heard of this work." == failure.exception


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
        assert 1 == len(imported_editions)

        # The other failed to import, and became a CoverageFailure
        failure = failures['http://www.gutenberg.org/ebooks/10441']
        assert isinstance(failure, CoverageFailure)
        assert False == failure.transient
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
        assert 1 == len(works)

        # There's an error message for the work that failed.
        failure = failures['http://www.gutenberg.org/ebooks/10441']
        assert isinstance(failure, CoverageFailure)
        assert False == failure.transient
        assert "Utter work failure!" in failure.exception

    def test_consolidate_links(self):

        # If a link turns out to be a dud, consolidate_links()
        # gets rid of it.
        links = [None, None]
        assert [] == OPDSImporter.consolidate_links(links)

        links = [LinkData(href=self._url, rel=rel, media_type="image/jpeg")
                 for rel in [Hyperlink.OPEN_ACCESS_DOWNLOAD,
                             Hyperlink.IMAGE,
                             Hyperlink.THUMBNAIL_IMAGE,
                             Hyperlink.OPEN_ACCESS_DOWNLOAD]
        ]
        old_link = links[2]
        links = OPDSImporter.consolidate_links(links)
        assert [Hyperlink.OPEN_ACCESS_DOWNLOAD,
             Hyperlink.IMAGE,
             Hyperlink.OPEN_ACCESS_DOWNLOAD] == [x.rel for x in links]
        link = links[1]
        assert old_link == link.thumbnail

        links = [LinkData(href=self._url, rel=rel, media_type="image/jpeg")
                 for rel in [Hyperlink.THUMBNAIL_IMAGE,
                             Hyperlink.IMAGE,
                             Hyperlink.THUMBNAIL_IMAGE,
                             Hyperlink.IMAGE]
        ]
        t1, i1, t2, i2 = links
        links = OPDSImporter.consolidate_links(links)
        assert [Hyperlink.IMAGE,
             Hyperlink.IMAGE] == [x.rel for x in links]
        assert t1 == i1.thumbnail
        assert t2 == i2.thumbnail

        links = [LinkData(href=self._url, rel=rel, media_type="image/jpeg")
                 for rel in [Hyperlink.THUMBNAIL_IMAGE,
                             Hyperlink.IMAGE,
                             Hyperlink.IMAGE]
        ]
        t1, i1, i2 = links
        links = OPDSImporter.consolidate_links(links)
        assert [Hyperlink.IMAGE,
             Hyperlink.IMAGE] == [x.rel for x in links]
        assert t1 == i1.thumbnail
        assert None == i2.thumbnail

    def test_import_book_that_offers_no_license(self):
        feed = self.sample_opds("book_without_license.opds")
        importer = OPDSImporter(self._db, self._default_collection)
        imported_editions, imported_pools, imported_works, failures = (
            importer.import_from_feed(feed)
        )

        # We got an Edition for this book, but no LicensePool and no Work.
        [edition] = imported_editions
        assert "Howards End" == edition.title
        assert [] == imported_pools
        assert [] == imported_works

        # We were able to figure out the medium of the Edition
        # based on its <dcterms:format> tag.
        assert Edition.AUDIO_MEDIUM == edition.medium

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
        assert None == importer.identifier_mapping

        # We can build one.
        importer.build_identifier_mapping([isbn1.urn])
        expected = { isbn1 : lp.identifier }
        assert expected == importer.identifier_mapping

        # If we already have one, it's overwritten.
        importer.build_identifier_mapping([isbn2.urn])
        overwrite = { isbn2 : lp.identifier }
        assert importer.identifier_mapping == overwrite

        # If the importer doesn't have a collection, we can't build
        # its mapping.
        importer = OPDSImporter(self._db, None)
        importer.build_identifier_mapping([isbn1])
        assert None == importer.identifier_mapping

    def test_update_work_for_edition_having_no_work(self):
        # We have an Edition and a LicensePool but no Work.
        edition, lp = self._edition(with_license_pool=True)
        assert None == lp.work

        importer = OPDSImporter(self._db, None)
        returned_pool, returned_work = importer.update_work_for_edition(edition)

        # We now have a presentation-ready work.
        work = lp.work
        assert True == work.presentation_ready

        # The return value of update_work_for_edition is the affected
        # LicensePool and Work.
        assert returned_pool == lp
        assert returned_work == work

        # That happened because LicensePool.calculate_work() was
        # called. But now that there's a presentation-ready work,
        # further presentation recalculation happens in the
        # background. Calling update_work_for_edition() will not
        # immediately call LicensePool.calculate_work().
        def explode():
            raise Exception("boom!")
        lp.calculate_work = explode
        importer.update_work_for_edition(edition)

    def test_update_work_for_edition_having_incomplete_work(self):
        # We have a work, but it's not presentation-ready because
        # the title is missing.
        work = self._work(with_license_pool=True)
        [pool] = work.license_pools
        edition = work.presentation_edition
        edition.title = None
        work.presentation_ready = False

        # Fortunately, new data has come in that includes a title.
        i = edition.primary_identifier
        new_edition = self._edition(
            data_source_name=DataSource.METADATA_WRANGLER,
            identifier_type=i.type, identifier_id=i.identifier,
            title="A working title"
        )

        importer = OPDSImporter(self._db, None)
        returned_pool, returned_work = importer.update_work_for_edition(
            edition
        )
        assert returned_pool == pool
        assert returned_work == work

        # We now have a presentation-ready work.
        assert "A working title" == work.title
        assert True == work.presentation_ready

    def test_update_work_for_edition_having_presentation_ready_work(self):
        # We have a presentation-ready work.
        work = self._work(with_license_pool=True, title="The old title")
        edition = work.presentation_edition
        [pool] = work.license_pools

        # The work's presentation edition has been chosen.
        work.calculate_presentation()
        op = WorkCoverageRecord.CHOOSE_EDITION_OPERATION

        # But we're about to find out a new title for the book.
        i = edition.primary_identifier
        new_edition = self._edition(
            data_source_name=DataSource.LIBRARY_STAFF,
            identifier_type=i.type, identifier_id=i.identifier,
            title="A new title"
        )

        importer = OPDSImporter(self._db, None)
        returned_pool, returned_work = importer.update_work_for_edition(
            new_edition
        )

        # The existing LicensePool and Work were returned.
        assert returned_pool == pool
        assert returned_work == work

        # The work is still presentation-ready.
        assert True == work.presentation_ready

    def test_update_work_for_edition_having_multiple_license_pools(self):
        # There are two collections with a LicensePool associated with
        # this Edition.
        edition, lp = self._edition(with_license_pool=True)
        collection2 = self._collection()
        lp2 = self._licensepool(edition=edition, collection=collection2)
        importer = OPDSImporter(self._db, None)

        # Calling update_work_for_edition creates a Work and associates
        # it with the edition.
        assert None == edition.work
        importer.update_work_for_edition(edition)
        work = edition.work
        assert isinstance(work, Work)

        # Both LicensePools are associated with that work.
        assert lp.work == work
        assert lp2.work == work

    def test_assert_importable_content(self):

        class Mock(OPDSImporter):
            """An importer that may or may not be able to find
            real open-access content.
            """
            # Set this variable to control whether any open-access links
            # are "found" in the OPDS feed.
            open_access_links = None

            extract_feed_data_called_with = None
            _is_open_access_link_called_with = []

            def extract_feed_data(self, feed, feed_url):
                # There's no need to return realistic metadata,
                # since _open_access_links is also mocked.
                self.extract_feed_data_called_with = (feed, feed_url)
                return {"some": "metadata"}, {}

            def _open_access_links(self, metadatas):
                self._open_access_links_called_with = metadatas
                for i in self.open_access_links:
                    yield i

            def _is_open_access_link(self, url, type):
                self._is_open_access_link_called_with.append((url, type))
                return False

        class NoLinks(Mock):
            "Simulate an OPDS feed that contains no open-access links."
            open_access_links = []

        # We don't be making any HTTP requests, even simulated ones.
        do_get = object()

        # Here, there are no links at all.
        importer = NoLinks(self._db, None, do_get)
        with pytest.raises(IntegrationException) as excinfo:
            importer.assert_importable_content("feed", "url")
        assert "No open-access links were found in the OPDS feed." in str(excinfo.value)

        # We extracted 'metadata' from the feed and URL.
        assert ("feed", "url") == importer.extract_feed_data_called_with

        # But there were no open-access links in the 'metadata',
        # so we had nothing to check.
        assert [] == importer._is_open_access_link_called_with

        oa = Hyperlink.OPEN_ACCESS_DOWNLOAD
        class BadLinks(Mock):
            """Simulate an OPDS feed that contains open-access links that
            don't actually work, because _is_open_access always returns False
            """
            open_access_links = [
                LinkData(href="url1", rel=oa, media_type="text/html"),
                LinkData(href="url2", rel=oa, media_type="application/json"),
                LinkData(href="I won't be tested", rel=oa,
                         media_type="application/json")
            ]

        importer = BadLinks(self._db, None, do_get)
        with pytest.raises(IntegrationException) as excinfo:
            importer.assert_importable_content("feed", "url", max_get_attempts=2)
        assert "Was unable to GET supposedly open-access content such as url2 (tried 2 times)" in str(excinfo.value)

        # We called _is_open_access_link on the first and second links
        # found in the 'metadata', but failed both times.
        #
        # We didn't bother with the third link because max_get_attempts was
        # set to 2.
        try1, try2 = importer._is_open_access_link_called_with
        assert ("url1", "text/html") == try1
        assert ("url2", "application/json") == try2

        class GoodLink(Mock):
            """Simulate an OPDS feed that contains two bad open-access links
            and one good one.
            """
            _is_open_access_link_called_with = []
            open_access_links = [
                LinkData(href="bad", rel=oa, media_type="text/html"),
                LinkData(href="good", rel=oa, media_type="application/json"),
                LinkData(href="also bad", rel=oa, media_type="text/html"),
            ]
            def _is_open_access_link(self, url, type):
                self._is_open_access_link_called_with.append((url, type))
                if url == 'bad':
                    return False
                return "this is a book"
        importer = GoodLink(self._db, None, do_get)
        result = importer.assert_importable_content(
            "feed", "url", max_get_attempts=5
        )
        assert "this is a book" == result

        # The first link didn't work, but the second one did,
        # so we didn't try the third one.
        try1, try2 = importer._is_open_access_link_called_with
        assert ("bad", "text/html") == try1
        assert ("good", "application/json") == try2

    def test__open_access_links(self):
        """Test our ability to find open-access links in Metadata objects."""
        m = OPDSImporter._open_access_links

        # No Metadata objects, no links.
        assert [] == list(m([]))

        # This Metadata has no associated CirculationData and will be
        # ignored.
        no_circulation = Metadata(DataSource.GUTENBERG)

        # This CirculationData has no open-access links, so it will be
        # ignored.
        circulation = CirculationData(DataSource.GUTENBERG, self._identifier())
        no_open_access_links = Metadata(
            DataSource.GUTENBERG, circulation=circulation
        )

        # This has three links, but only the open-access links
        # will be returned.
        circulation = CirculationData(DataSource.GUTENBERG, self._identifier())
        oa = Hyperlink.OPEN_ACCESS_DOWNLOAD
        for rel in [oa, Hyperlink.IMAGE, oa]:
            circulation.links.append(
                LinkData(href=self._url, rel=rel)
            )
        two_open_access_links = Metadata(
            DataSource.GUTENBERG, circulation=circulation
        )

        oa_only = [x for x in circulation.links if x.rel==oa]
        assert oa_only == list(m([no_circulation, two_open_access_links,
                             no_open_access_links]))

    def test__is_open_access_link(self):
        http = DummyHTTPClient()

        # We only check that the response entity-body isn't tiny. 11
        # kilobytes of data is enough.
        enough_content = "a" * (1024*11)

        # Set up an HTTP response that looks enough like a book
        # to convince _is_open_access_link.
        http.queue_response(200, content=enough_content)
        monitor = OPDSImporter(self._db, None, http_get=http.do_get)

        url = self._url
        type = "text/html"
        assert ("Found a book-like thing at %s" % url ==
            monitor._is_open_access_link(url, type))

        # We made a GET request to the appropriate URL.
        assert url == http.requests.pop()

        # This HTTP response looks OK but it's not big enough to be
        # any kind of book.
        http.queue_response(200, content="not enough content")
        monitor = OPDSImporter(self._db, None, http_get=http.do_get)
        assert False == monitor._is_open_access_link(url, None)

        # This HTTP response is clearly an error page.
        http.queue_response(404, content=enough_content)
        monitor = OPDSImporter(self._db, None, http_get=http.do_get)
        assert False == monitor._is_open_access_link(url, None)

    def test_import_open_access_audiobook(self):
        feed = self.audiobooks_opds
        download_manifest_url = 'https://api.archivelab.org/books/kniga_zitij_svjatyh_na_mesjac_avgust_eu_0811_librivox/opds_audio_manifest'

        importer = OPDSImporter(
            self._db,
            collection=self._default_collection,
        )

        imported_editions, imported_pools, imported_works, failures = (
            importer.import_from_feed(feed)
        )

        assert 1 == len(imported_editions)

        [august] = imported_editions
        assert "Zhitiia Sviatykh, v. 12 - August" == august.title

        [august_pool] = imported_pools
        assert True == august_pool.open_access
        assert download_manifest_url == august_pool._open_access_download_url

        [lpdm] = august_pool.delivery_mechanisms
        assert Representation.AUDIOBOOK_MANIFEST_MEDIA_TYPE == lpdm.delivery_mechanism.content_type
        assert DeliveryMechanism.NO_DRM == lpdm.delivery_mechanism.drm_scheme


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
        assert "new value" == combined['a_scalar']

        # Missing values are filled in.
        assert 'finally a value' == d["key1"]
        assert 'value3' == d['key3']
        assert 'value4' == d['key4']

        # Lists get extended.
        assert [1, 2] == combined['a_list']
        assert [2, 200] == d['key2']

    def test_combine_null_cases(self):
        """Test combine()'s ability to handle empty and null dictionaries."""
        c = OPDSImporter.combine
        empty = dict()
        nonempty = dict(a=1)
        assert nonempty == c(empty, nonempty)
        assert empty == c(None, None)
        assert nonempty == c(nonempty, None)
        assert nonempty == c(None, nonempty)

    def test_combine_missing_value_is_replaced(self):
        c = OPDSImporter.combine
        a_is_missing = dict(b=None)
        a_is_present = dict(a=None, b=None)
        expect = dict(a=None, b=None)
        assert expect == c(a_is_missing, a_is_present)

        a_is_present['a'] = True
        expect = dict(a=True, b=None)
        assert expect == c(a_is_missing, a_is_present)

    def test_combine_present_value_replaced(self):
        """When both dictionaries define a scalar value, the second
        dictionary's value takes presedence.
        """
        c = OPDSImporter.combine
        a_is_true = dict(a=True)
        a_is_false = dict(a=False)
        assert a_is_false == c(a_is_true, a_is_false)
        assert a_is_true == c(a_is_false, a_is_true)

        a_is_old = dict(a="old value")
        a_is_new = dict(a="new value")
        assert "new value" == c(a_is_old, a_is_new)['a']

    def test_combine_present_value_not_replaced_with_none(self):

        """When combining a dictionary where a key is set to None
        with a dictionary where that key is present, the value
        is left alone.
        """
        a_is_present = dict(a=True)
        a_is_none = dict(a=None, b=True)
        expect = dict(a=True, b=True)
        assert expect == OPDSImporter.combine(a_is_present, a_is_none)

    def test_combine_present_value_extends_list(self):
        """When both dictionaries define a list, the combined value
        is a combined list.
        """
        a_is_true = dict(a=[True])
        a_is_false = dict(a=[False])
        assert dict(a=[True, False]) == OPDSImporter.combine(a_is_true, a_is_false)

    def test_combine_present_value_extends_dictionary(self):
        """When both dictionaries define a dictionary, the combined value is
        the result of combining the two dictionaries with a recursive
        combine() call.
        """
        a_is_true = dict(a=dict(b=[True]))
        a_is_false = dict(a=dict(b=[False]))
        assert (dict(a=dict(b=[True, False])) ==
            OPDSImporter.combine(a_is_true, a_is_false))

class TestMirroring(OPDSImporterTest):

    @pytest.fixture()
    def http(self):
        class DummyHashedHttpClient(object):
            def __init__(self):
                self.responses = {}
                self.requests = []

            def queue_response(self, url, response_code, media_type='text_html', other_headers=None, content=''):
                headers = {}
                if media_type:
                    headers["content-type"] = media_type
                if other_headers:
                    for k, v in other_headers.items():
                        headers[k.lower()] = v
                self.responses[url] = (response_code, headers, content)

            def do_get(self, url, *args, **kwargs):
                self.requests.append(url)
                return self.responses.pop(url)
        return DummyHashedHttpClient()

    @pytest.fixture()
    def svg(self):
        svg = """<!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN"
          "http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd">

        <svg xmlns="http://www.w3.org/2000/svg" width="1000" height="500">
            <ellipse cx="50" cy="25" rx="50" ry="25" style="fill:blue;"/>
        </svg>"""
        return svg

    @pytest.fixture()
    def png(self):
        with open(self.sample_cover_path("test-book-cover.png"), "rb") as png_file:
            png = png_file.read()
        return png

    @pytest.fixture()
    def epub10441(self):
        return {
            'url': 'http://www.gutenberg.org/ebooks/10441.epub.images',
            'response_code': 200,
            'content': b'I am 10441.epub.images',
            'media_type': Representation.EPUB_MEDIA_TYPE
        }

    @pytest.fixture()
    def epub10441_cover(self, svg):
        return {
            'url': 'https://s3.amazonaws.com/book-covers.nypl.org/Gutenberg-Illustrated/10441/cover_10441_9.png',
            'response_code': 200,
            'content': svg,
            'media_type': Representation.SVG_MEDIA_TYPE
        }

    @pytest.fixture()
    def epub10557(self):
        return {
            'url': 'http://www.gutenberg.org/ebooks/10557.epub.images',
            'response_code': 200,
            'content': b'I am 10557.epub.images',
            'media_type': Representation.EPUB_MEDIA_TYPE
        }

    @pytest.fixture()
    def epub10557_cover_broken(self):
        return {
            'url':  'http://root/broken-cover-image',
            'response_code': 404,
            'media_type': "text/plain"
        }

    @pytest.fixture()
    def epub10557_cover_working(self, png):
        return {
            'url': 'http://root/working-cover-image',
            'response_code': 200,
            'content': png,
            'media_type': Representation.PNG_MEDIA_TYPE
        }

    def test_importer_gets_appropriate_mirror_for_collection(self):
        # The default collection is not configured to mirror the
        # resources it finds for either its books or covers.
        collection = self._default_collection
        importer = OPDSImporter(self._db, collection=collection)
        assert None == importer.mirrors[ExternalIntegrationLink.OPEN_ACCESS_BOOKS]
        assert None == importer.mirrors[ExternalIntegrationLink.COVERS]

        # Let's configure mirrors integration for it.

        # First set up a storage integration.
        integration = self._external_integration(
            ExternalIntegration.S3, ExternalIntegration.STORAGE_GOAL,
            username="username", password="password",
            settings = {S3UploaderConfiguration.BOOK_COVERS_BUCKET_KEY : "some-covers"}
        )
        # Associate the collection's integration with the storage integration
        # for the purpose of 'covers'.
        integration_link = self._external_integration_link(
            integration=collection._external_integration,
            other_integration=integration,
            purpose=ExternalIntegrationLink.COVERS
        )

        # Now an OPDSImporter created for this collection has an
        # appropriately configured MirrorUploader associated with it for the
        # 'covers' purpose.
        importer = OPDSImporter(self._db, collection=collection)
        mirrors = importer.mirrors

        assert isinstance(mirrors[ExternalIntegrationLink.COVERS], S3Uploader)
        assert "some-covers" == mirrors[ExternalIntegrationLink.COVERS].get_bucket(
            S3UploaderConfiguration.BOOK_COVERS_BUCKET_KEY)
        assert mirrors[ExternalIntegrationLink.OPEN_ACCESS_BOOKS] == None


        # An OPDSImporter can have two types of mirrors.
        integration = self._external_integration(
            ExternalIntegration.S3, ExternalIntegration.STORAGE_GOAL,
            username="username", password="password",
            settings={S3UploaderConfiguration.OA_CONTENT_BUCKET_KEY : "some-books"}
        )
        # Associate the collection's integration with the storage integration
        # for the purpose of 'covers'.
        integration_link = self._external_integration_link(
            integration=collection._external_integration,
            other_integration=integration,
            purpose=ExternalIntegrationLink.OPEN_ACCESS_BOOKS
        )

        importer = OPDSImporter(self._db, collection=collection)
        mirrors = importer.mirrors

        assert isinstance(mirrors[ExternalIntegrationLink.OPEN_ACCESS_BOOKS], S3Uploader)
        assert "some-books" == mirrors[ExternalIntegrationLink.OPEN_ACCESS_BOOKS].get_bucket(
            S3UploaderConfiguration.OA_CONTENT_BUCKET_KEY)
        assert "some-covers" == mirrors[ExternalIntegrationLink.COVERS].get_bucket(
            S3UploaderConfiguration.BOOK_COVERS_BUCKET_KEY)

    def test_resources_are_mirrored_on_import(self, http, png, svg, epub10441, epub10557, epub10441_cover,
                                              epub10557_cover_broken, epub10557_cover_working):
        http.queue_response(**epub10441)
        http.queue_response(**epub10441_cover)
        http.queue_response(**epub10557)
        # The request to http://root/broken-cover-image
        # will result in a 404 error, and the image will not be mirrored.
        http.queue_response(**epub10557_cover_broken)
        http.queue_response(**epub10557_cover_working)

        s3_for_books = MockS3Uploader()
        s3_for_covers = MockS3Uploader()
        mirrors = dict(books_mirror=s3_for_books, covers_mirror=s3_for_covers)

        importer = OPDSImporter(
            self._db, collection=self._default_collection,
            mirrors=mirrors, http_get=http.do_get
        )

        imported_editions, pools, works, failures = (
            importer.import_from_feed(self.content_server_mini_feed,
                                      feed_url='http://root')
        )

        assert 2 == len(pools)

        # Both items were requested
        assert epub10441['url'] in http.requests
        assert epub10557['url'] in http.requests

        # The import process requested each remote resource in the feed. The thumbnail
        # image was not requested, since we never trust foreign thumbnails. The order they
        # are requested in is not deterministic, but after requesting the epub the images
        # should be requested.
        index = http.requests.index(epub10441['url'])
        assert http.requests[index+1] == epub10441_cover['url']

        index = http.requests.index(epub10557['url'])
        assert http.requests[index:index+3] == [
            epub10557['url'],
            epub10557_cover_broken['url'],
            epub10557_cover_working['url']
        ]

        e_10441 = next(e for e in imported_editions if e.primary_identifier.identifier == '10441')
        e_10557 = next(e for e in imported_editions if e.primary_identifier.identifier == '10557')

        [e_10441_oa_link, e_10441_image_link, e_10441_thumbnail_link,
         e_10441_description_link] = sorted(
            e_10441.primary_identifier.links, key=lambda x: x.rel
        )
        [e_10557_broken_image_link, e_10557_working_image_link, e_10557_oa_link] = sorted(
           e_10557.primary_identifier.links, key=lambda x: x.resource.url
        )

        # The thumbnail image is associated with the Identifier, but
        # it's not used because it's associated with a representation
        # (cover_10441_9.png with media type "image/png") that no
        # longer has a resource associated with it.
        assert Hyperlink.THUMBNAIL_IMAGE == e_10441_thumbnail_link.rel
        hypothetical_full_representation = e_10441_thumbnail_link.resource.representation.thumbnail_of
        assert None == hypothetical_full_representation.resource
        assert (Representation.PNG_MEDIA_TYPE ==
            hypothetical_full_representation.media_type)

        # That's because when we actually got cover_10441_9.png,
        # it turned out to be an SVG file, not a PNG, so we created a new
        # Representation. TODO: Obviously we could do better here.
        assert (Representation.SVG_MEDIA_TYPE ==
            e_10441_image_link.resource.representation.media_type)

        # The two open-access links were mirrored to S3, as were the
        # original SVG image, the working PNG image, and its thumbnail, which we generated. The
        # The broken PNG image was not mirrored because our attempt to download
        # it resulted in a 404 error.
        imported_book_representations = {e_10441_oa_link.resource.representation,
                                         e_10557_oa_link.resource.representation}
        imported_cover_representations = {e_10441_image_link.resource.representation,
                                          e_10557_working_image_link.resource.representation,
                                          e_10557_working_image_link.resource.representation.thumbnails[0]}

        assert imported_book_representations == set(s3_for_books.uploaded)
        assert imported_cover_representations == set(s3_for_covers.uploaded)

        assert 2 == len(s3_for_books.uploaded)
        assert 3 == len(s3_for_covers.uploaded)

        assert epub10441['content'] in s3_for_books.content
        assert epub10557['content'] in s3_for_books.content

        svg_bytes = svg.encode("utf8")
        covers_content = s3_for_covers.content[:]
        assert svg_bytes in covers_content
        covers_content.remove(svg_bytes)
        assert png in covers_content
        covers_content.remove(png)

        # We don't know what the thumbnail is, but we know it's smaller than the original cover image.
        assert(len(png) > len(covers_content[0]))

        # Each resource was 'mirrored' to an Amazon S3 bucket.
        #
        # The "mouse" book was mirrored to a book bucket corresponding to
        # Project Gutenberg, its data source.
        #
        # The images were mirrored to a covers bucket corresponding to the
        # open-access content server, _their_ data source. Each image
        # has an extension befitting its media type.
        #
        # The "crow" book was mirrored to a bucket corresponding to
        # the open-access content source, the default data source used
        # when no distributor was specified for a book.
        book1_url = 'https://test-content-bucket.s3.amazonaws.com/Gutenberg/Gutenberg%20ID/10441/The%20Green%20Mouse.epub.images'
        book1_svg_cover = 'https://test-cover-bucket.s3.amazonaws.com/Library%20Simplified%20Open%20Access%20Content%20Server/Gutenberg%20ID/10441/cover_10441_9.svg'
        book2_url = 'https://test-content-bucket.s3.amazonaws.com/Library%20Simplified%20Open%20Access%20Content%20Server/Gutenberg%20ID/10557/Johnny%20Crow%27s%20Party.epub.images'
        book2_png_cover = 'https://test-cover-bucket.s3.amazonaws.com/Library%20Simplified%20Open%20Access%20Content%20Server/Gutenberg%20ID/10557/working-cover-image.png'
        book2_png_thumbnail = 'https://test-cover-bucket.s3.amazonaws.com/scaled/300/Library%20Simplified%20Open%20Access%20Content%20Server/Gutenberg%20ID/10557/working-cover-image.png'
        uploaded_urls = {x.mirror_url for x in s3_for_covers.uploaded}
        uploaded_book_urls = {x.mirror_url for x in s3_for_books.uploaded}
        assert {book1_svg_cover, book2_png_cover, book2_png_thumbnail} == uploaded_urls
        assert {book1_url, book2_url} == uploaded_book_urls


        # If we fetch the feed again, and the entries have been updated since the
        # cutoff, but the content of the open access links hasn't changed, we won't mirror
        # them again.
        cutoff = datetime_utc(2013, 1, 2, 16, 56, 40)

        http.queue_response(
            epub10441['url'],
            304, media_type=Representation.EPUB_MEDIA_TYPE
        )

        http.queue_response(
            epub10441_cover['url'],
            304, media_type=Representation.SVG_MEDIA_TYPE
        )

        http.queue_response(
            epub10557['url'],
            304, media_type=Representation.EPUB_MEDIA_TYPE
        )

        imported_editions, pools, works, failures = (
            importer.import_from_feed(self.content_server_mini_feed)
        )

        assert {e_10441, e_10557} == set(imported_editions)

        # Nothing new has been uploaded
        assert 2 == len(s3_for_books.uploaded)

        # If the content has changed, it will be mirrored again.
        epub10441_updated = epub10441.copy()
        epub10441_updated['content'] = b"I am a new version of 10441.epub.images"
        http.queue_response(**epub10441_updated)
        http.queue_response(**epub10441_cover)
        epub10557_updated = epub10557.copy()
        epub10557_updated['content'] = b"I am a new version of 10557.epub.images"
        http.queue_response(**epub10557_updated)

        imported_editions, pools, works, failures = (
            importer.import_from_feed(self.content_server_mini_feed)
        )

        assert {e_10441, e_10557} == set(imported_editions)
        assert 4 == len(s3_for_books.uploaded)
        assert epub10441_updated['content'] in s3_for_books.content[-2:]
        assert svg_bytes == s3_for_covers.content.pop()
        assert epub10557_updated['content'] in s3_for_books.content[-2:]


    def test_content_resources_not_mirrored_on_import_if_no_collection(self, http, svg, epub10557_cover_broken,
                                                                       epub10557_cover_working, epub10441_cover):
        # If you don't provide a Collection to the OPDSImporter, no
        # LicensePools are created for the book and content resources
        # (like EPUB editions of the book) are not mirrored. Only
        # metadata resources (like the book cover) are mirrored.


        # The request to http://root/broken-cover-image
        # will result in a 404 error, and the image will not be mirrored.
        http.queue_response(**epub10557_cover_broken)
        http.queue_response(**epub10557_cover_working)
        http.queue_response(**epub10441_cover)

        s3 = MockS3Uploader()
        mirrors = dict(covers_mirror=s3)

        importer = OPDSImporter(
            self._db, collection=None,
            mirrors=mirrors, http_get=http.do_get
        )

        imported_editions, pools, works, failures = (
            importer.import_from_feed(self.content_server_mini_feed,
                                      feed_url='http://root')
        )

        # No LicensePools were created, since no Collection was
        # provided.
        assert [] == pools

        # The import process requested each remote resource in the
        # order they appeared in the OPDS feed. The EPUB resources
        # were not requested because no Collection was provided to the
        # importer. The thumbnail image was not requested, since we
        # were going to make our own thumbnail anyway.
        assert len(http.requests) == 3
        assert set(http.requests) == {
            epub10441_cover['url'],
            epub10557_cover_broken['url'],
            epub10557_cover_working['url']
        }


class TestOPDSImportMonitor(OPDSImporterTest):

    def test_constructor(self):
        with pytest.raises(ValueError) as excinfo:
            OPDSImportMonitor(self._db, None, OPDSImporter)
        assert "OPDSImportMonitor can only be run in the context of a Collection." in str(excinfo.value)

        self._default_collection.external_integration.protocol = ExternalIntegration.OVERDRIVE
        with pytest.raises(ValueError) as excinfo:
            OPDSImportMonitor(self._db, self._default_collection, OPDSImporter)
        assert "Collection Default Collection is configured for protocol Overdrive, not OPDS Import." in str(excinfo.value)

        self._default_collection.external_integration.protocol = ExternalIntegration.OPDS_IMPORT
        self._default_collection.external_integration.setting('data_source').value = None
        with pytest.raises(ValueError) as excinfo:
            OPDSImportMonitor(self._db, self._default_collection, OPDSImporter)
        assert "Collection Default Collection has no associated data source." in str(excinfo.value)

    def test_external_integration(self):
        monitor = OPDSImportMonitor(
            self._db, self._default_collection,
            import_class=OPDSImporter,
        )
        assert (self._default_collection.external_integration ==
            monitor.external_integration(self._db))

    def test__run_self_tests(self):
        """Verify the self-tests of an OPDS collection."""

        class MockImporter(OPDSImporter):
            def assert_importable_content(self, content, url):
                self.assert_importable_content_called_with = (content, url)
                return "looks good"

        class Mock(OPDSImportMonitor):
            follow_one_link_called_with = []

            # First we will get the first page of the OPDS feed.
            def follow_one_link(self, url):
                self.follow_one_link_called_with.append(url)
                return ([], "some content")

        feed_url = self._url
        self._default_collection.external_account_id = feed_url
        monitor = Mock(self._db, self._default_collection,
                       import_class=MockImporter)
        [first_page, found_content] = monitor._run_self_tests(self._db)
        expect = "Retrieve the first page of the OPDS feed (%s)" % feed_url
        assert expect == first_page.name
        assert True == first_page.success
        assert ([], "some content") == first_page.result

        # follow_one_link was called once.
        [link] = monitor.follow_one_link_called_with
        assert monitor.feed_url == link

        # Then, assert_importable_content was called on the importer.
        assert "Checking for importable content" == found_content.name
        assert True == found_content.success
        assert (("some content", feed_url) ==
            monitor.importer.assert_importable_content_called_with)
        assert "looks good" == found_content.result

    def test_hook_methods(self):
        """By default, the OPDS URL and data source used by the importer
        come from the collection configuration.
        """
        monitor = OPDSImportMonitor(
            self._db, self._default_collection,
            import_class=OPDSImporter,
        )
        assert (self._default_collection.external_account_id ==
            monitor.opds_url(self._default_collection))

        assert (self._default_collection.data_source ==
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
        assert True == monitor.feed_contains_new_data(feed)
        assert None == timestamp.start

        # Now import the editions.
        monitor = MockOPDSImportMonitor(
            self._db,
            collection=self._default_collection,
            import_class=OPDSImporter,
        )
        monitor.run()

        # Editions have been imported.
        assert 2 == self._db.query(Edition).count()

        # The timestamp has been updated, although unlike most
        # Monitors the timestamp is purely informational.
        assert timestamp.finish != None

        editions = self._db.query(Edition).all()
        data_source = DataSource.lookup(self._db, DataSource.OA_CONTENT_SERVER)

        # If there are CoverageRecords that record work are after the updated
        # dates, there's nothing new.
        record, ignore = CoverageRecord.add_for(
            editions[0], data_source, CoverageRecord.IMPORT_OPERATION
        )
        record.timestamp = datetime_utc(2016, 1, 1, 1, 1, 1)

        record2, ignore = CoverageRecord.add_for(
            editions[1], data_source, CoverageRecord.IMPORT_OPERATION
        )
        record2.timestamp = datetime_utc(2016, 1, 1, 1, 1, 1)

        assert False == monitor.feed_contains_new_data(feed)

        # If the monitor is set up to force reimport, it doesn't
        # matter that there's nothing new--we act as though there is.
        monitor.force_reimport = True
        assert True == monitor.feed_contains_new_data(feed)
        monitor.force_reimport = False

        # If an entry was updated after the date given in that entry's
        # CoverageRecord, there's new data.
        record2.timestamp = datetime_utc(1970, 1, 1, 1, 1, 1)
        assert True == monitor.feed_contains_new_data(feed)

        # If a CoverageRecord is a transient failure, we try again
        # regardless of whether it's been updated.
        for r in [record, record2]:
            r.timestamp = datetime_utc(2016, 1, 1, 1, 1, 1)
            r.exception = "Failure!"
            r.status = CoverageRecord.TRANSIENT_FAILURE
        assert True == monitor.feed_contains_new_data(feed)

        # If a CoverageRecord is a persistent failure, we don't try again...
        for r in [record, record2]:
            r.status = CoverageRecord.PERSISTENT_FAILURE
        assert False == monitor.feed_contains_new_data(feed)

        # ...unless the feed updates.
        record.timestamp = datetime_utc(1970, 1, 1, 1, 1, 1)
        assert True == monitor.feed_contains_new_data(feed)

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
        assert 1 == len(next_links)
        assert "http://localhost:5000/?after=327&size=100" == next_links[0]

        assert feed == content

        # Now import the editions and add coverage records.
        monitor.importer.import_from_feed(feed)
        assert 2 == self._db.query(Edition).count()

        editions = self._db.query(Edition).all()
        data_source = DataSource.lookup(self._db, DataSource.OA_CONTENT_SERVER)

        for edition in editions:
            record, ignore = CoverageRecord.add_for(
                edition, data_source, CoverageRecord.IMPORT_OPERATION
            )
            record.timestamp = datetime_utc(2016, 1, 1, 1, 1, 1)


        # If there's no new data, follow_one_link returns no next
        # links and no content.
        #
        # Note that this works even when the media type is imprecisely
        # specified as Atom or bare XML.
        for imprecise_media_type in OPDSFeed.ATOM_LIKE_TYPES:
            http.queue_response(200, imprecise_media_type, content=feed)
            next_links, content = follow()
            assert 0 == len(next_links)
            assert None == content

        http.queue_response(200, AtomFeed.ATOM_TYPE, content=feed)
        next_links, content = follow()
        assert 0 == len(next_links)
        assert None == content

        # If the media type is missing or is not an Atom feed,
        # an exception is raised.
        http.queue_response(200, None, content=feed)
        with pytest.raises(BadResponseException) as excinfo:
            follow()
        assert "Expected Atom feed, got None" in str(excinfo.value)

        http.queue_response(200, "not/atom", content=feed)
        with pytest.raises(BadResponseException) as excinfo:
            follow()
        assert "Expected Atom feed, got not/atom" in str(excinfo.value)

    def test_import_one_feed(self):
        # Check coverage records are created.

        monitor = OPDSImportMonitor(
            self._db, collection=self._default_collection,
            import_class=DoomedOPDSImporter
        )
        self._default_collection.external_account_id = "http://root-url/index.xml"
        data_source = DataSource.lookup(self._db, DataSource.OA_CONTENT_SERVER)

        feed = self.content_server_mini_feed

        imported, failures = monitor.import_one_feed(feed)

        editions = self._db.query(Edition).all()

        # One edition has been imported
        assert 1 == len(editions)
        [edition] = editions

        # The return value of import_one_feed includes the imported
        # editions.
        assert [edition] == imported

        # That edition has a CoverageRecord.
        record = CoverageRecord.lookup(
            editions[0].primary_identifier, data_source,
            operation=CoverageRecord.IMPORT_OPERATION
        )
        assert CoverageRecord.SUCCESS == record.status
        assert None == record.exception

        # The edition's primary identifier has some cover links whose
        # relative URL have been resolved relative to the Collection's
        # external_account_id.
        covers  = set([x.resource.url for x in editions[0].primary_identifier.links
                    if x.rel==Hyperlink.IMAGE])
        assert covers == set(["http://root-url/broken-cover-image",
                        "http://root-url/working-cover-image"]
                    )

        # The 202 status message in the feed caused a transient failure.
        # The exception caused a persistent failure.

        coverage_records = self._db.query(CoverageRecord).filter(
            CoverageRecord.operation==CoverageRecord.IMPORT_OPERATION,
            CoverageRecord.status != CoverageRecord.SUCCESS
        )
        assert (
            sorted([CoverageRecord.TRANSIENT_FAILURE,
                    CoverageRecord.PERSISTENT_FAILURE]) ==
            sorted([x.status for x in coverage_records]))

        identifier, ignore = Identifier.parse_urn(self._db, "urn:librarysimplified.org/terms/id/Gutenberg%20ID/10441")
        failure = CoverageRecord.lookup(
            identifier, data_source,
            operation=CoverageRecord.IMPORT_OPERATION
        )
        assert "Utter failure!" in failure.exception

        # Both failures were reported in the return value from
        # import_one_feed
        assert 2 == len(failures)

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
                # Simulate two successes and one failure on every page.
                self.imports.append(feed)
                return [object(), object()], { "identifier": "Failure" }

        monitor = MockOPDSImportMonitor(
            self._db, collection=self._default_collection,
            import_class=OPDSImporter
        )

        monitor.queue_response([[], "last page"])
        monitor.queue_response([["second next link"], "second page"])
        monitor.queue_response([["next link"], "first page"])

        progress = monitor.run_once(object())

        # Feeds are imported in reverse order
        assert ["last page", "second page", "first page"] == monitor.imports

        # Every page of the import had two successes and one failure.
        assert "Items imported: 6. Failures: 3." == progress.achievements

        # The TimestampData returned by run_once does not include any
        # timing information; that's provided by run().
        assert None == progress.start
        assert None == progress.finish

    def test_update_headers(self):
        # Test the _update_headers helper method.
        monitor = OPDSImportMonitor(
            self._db, collection=self._default_collection,
            import_class=OPDSImporter
        )

        # _update_headers return a new dictionary. An Accept header will be setted
        # using the value of custom_accept_header. If the value is not set a
        # default value will be used.
        headers = {'Some other': 'header'}
        new_headers = monitor._update_headers(headers)
        assert ['Some other'] == list(headers.keys())
        assert ['Accept', 'Some other'] == sorted(list(new_headers.keys()))

        # If a custom_accept_header exist, will be used instead a default value
        new_headers = monitor._update_headers(headers)
        old_value = new_headers['Accept']
        target_value = old_value + "more characters"
        monitor.custom_accept_header = target_value
        new_headers = monitor._update_headers(headers)
        assert new_headers['Accept'] == target_value
        assert old_value != target_value

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
        assert headers == expect

