from nose.tools import (
    assert_raises_regexp,
    set_trace,
    eq_,
)

from core.testing import (
    DatabaseTest,
)

from core.testing import MockRequestsResponse

from core.config import (
    CannotLoadConfiguration,
    Configuration,
    temp_config,
)
from core.model import (
    Collection,
    DataSource,
    ExternalIntegration,
    Identifier,
    LicensePool,
)
from core.util.opds_writer import (
    OPDSFeed,
)
from core.opds_import import (
    MockSimplifiedOPDSLookup,
    OPDSImporter,
)
from core.coverage import (
    CoverageFailure,
)

from core.util.http import BadResponseException

from api.coverage import (
    MockOPDSImportCoverageProvider,
    OPDSImportCoverageProvider,
    ReaperImporter,
    RegistrarImporter,
)

class TestImporterSubclasses(DatabaseTest):
    """Test the subclasses of OPDSImporter."""

    def test_success_status_codes(self):
        """Validate the status codes that different importers
        will treat as successes.
        """
        assert [200, 201, 202] == RegistrarImporter.SUCCESS_STATUS_CODES
        assert [200, 404] == ReaperImporter.SUCCESS_STATUS_CODES


class TestOPDSImportCoverageProvider(DatabaseTest):

    def _provider(self):
        """Create a generic MockOPDSImportCoverageProvider for testing purposes."""
        return MockOPDSImportCoverageProvider(self._default_collection)

    def test_badresponseexception_on_non_opds_feed(self):
        """If the lookup protocol sends something that's not an OPDS
        feed, refuse to go any further.
        """
        provider = self._provider()
        provider.lookup_client = MockSimplifiedOPDSLookup(self._url)

        response = MockRequestsResponse(200, {"content-type" : "text/plain"}, "Some data")
        provider.lookup_client.queue_response(response)
        assert_raises_regexp(
            BadResponseException, "Wrong media type: text/plain",
            provider.import_feed_response, response, None
        )

    def test_process_batch_with_identifier_mapping(self):
        """Test that internal identifiers are mapped to and from the form used
        by the external service.
        """

        # Unlike other tests in this class, we are using a real
        # implementation of OPDSImportCoverageProvider.process_batch.
        class TestProvider(OPDSImportCoverageProvider):
            SERVICE_NAME = "Test provider"
            DATA_SOURCE_NAME = DataSource.OA_CONTENT_SERVER

            # Mock the identifier mapping
            def create_identifier_mapping(self, batch):
                return self.mapping

        # This means we need to mock the lookup client instead.
        lookup = MockSimplifiedOPDSLookup(self._url)

        # And create an ExternalIntegration for the metadata_client object.
        self._external_integration(
            ExternalIntegration.METADATA_WRANGLER,
            goal=ExternalIntegration.METADATA_GOAL, url=self._url
        )

        self._default_collection.external_integration.set_setting(
            Collection.DATA_SOURCE_NAME_SETTING, DataSource.OA_CONTENT_SERVER
        )
        provider = TestProvider(self._default_collection, lookup)

        # Create a hard-coded mapping. We use id1 internally, but the
        # foreign data source knows the book as id2.
        id1 = self._identifier()
        id2 = self._identifier()
        provider.mapping = { id2 : id1 }

        feed = "<feed><entry><id>%s</id><title>Here's your title!</title></entry></feed>" % id2.urn
        headers = {"content-type" : OPDSFeed.ACQUISITION_FEED_TYPE}
        lookup.queue_response(200, headers=headers, content=feed)
        [identifier] = provider.process_batch([id1])

        # We wanted to process id1. We sent id2 to the server, the
        # server responded with an <entry> for id2, and it was used to
        # modify the Edition associated with id1.
        assert id1 == identifier

        [edition] = id1.primarily_identifies
        assert "Here's your title!" == edition.title

    def test_process_batch(self):
        provider = self._provider()

        # Here are an Edition and a LicensePool for the same identifier but
        # from different data sources. We would expect this to happen
        # when talking to the open-access content server.
        edition = self._edition(data_source_name=DataSource.OA_CONTENT_SERVER)
        identifier = edition.primary_identifier

        license_source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        pool, is_new = LicensePool.for_foreign_id(
            self._db, license_source, identifier.type, identifier.identifier,
            collection=self._default_collection
        )
        assert None == pool.work

        # Here's a second Edition/LicensePool that's going to cause a
        # problem: the LicensePool will show up in the results, but
        # the corresponding Edition will not.
        edition2, pool2 = self._edition(with_license_pool=True)

        # Here's an identifier that can't be looked up at all,
        # and an identifier that shows up in messages_by_id because
        # its simplified:message was determined to indicate success
        # rather than failure.
        error_identifier = self._identifier()
        not_an_error_identifier = self._identifier()
        messages_by_id = {
            error_identifier.urn : CoverageFailure(
                error_identifier, "500: internal error"
            ),
            not_an_error_identifier.urn : not_an_error_identifier,
        }

        # When we call CoverageProvider.process_batch(), it's going to
        # return the information we just set up: a matched
        # Edition/LicensePool pair, a mismatched LicensePool, and an
        # error message.
        provider.queue_import_results(
            [edition], [pool, pool2], [], messages_by_id
        )

        # Make the CoverageProvider do its thing.
        fake_batch = [object()]
        (success_import, failure_mismatched, failure_message,
         success_message) = provider.process_batch(
            fake_batch
        )

        # The fake batch was provided to lookup_and_import_batch.
        assert [fake_batch] == provider.batches

        # The matched Edition/LicensePool pair was returned.
        assert success_import == edition.primary_identifier

        # The LicensePool of that pair was passed into finalize_license_pool.
        # The mismatched LicensePool was not.
        assert [pool] == provider.finalized

        # The mismatched LicensePool turned into a CoverageFailure
        # object.
        assert isinstance(failure_mismatched, CoverageFailure)
        assert ('OPDS import operation imported LicensePool, but no Edition.' ==
            failure_mismatched.exception)
        assert pool2.identifier == failure_mismatched.obj
        assert True == failure_mismatched.transient

        # The OPDSMessage with status code 500 was returned as a
        # CoverageFailure object.
        assert isinstance(failure_message, CoverageFailure)
        assert "500: internal error" == failure_message.exception
        assert error_identifier == failure_message.obj
        assert True == failure_message.transient

        # The identifier that had a treat-as-success OPDSMessage was returned
        # as-is.
        assert not_an_error_identifier == success_message

    def test_process_batch_success_even_if_no_licensepool_exists(self):
        """This shouldn't happen since CollectionCoverageProvider
        only operates on Identifiers that are licensed through a Collection.
        But if a lookup should return an Edition but no LicensePool,
        that counts as a success.
        """
        provider = self._provider()
        edition, pool = self._edition(with_license_pool=True)
        provider.queue_import_results([edition], [], [], {})
        fake_batch = [object()]
        [success] = provider.process_batch(fake_batch)

        # The Edition's primary identifier was returned to indicate
        # success.
        assert edition.primary_identifier == success

        # However, since there is no LicensePool, nothing was finalized.
        assert [] == provider.finalized

    def test_process_item(self):
        """To process a single item we process a batch containing
        only that item.
        """
        provider = self._provider()
        edition = self._edition()
        provider.queue_import_results([edition], [], [], {})
        item = object()
        result = provider.process_item(item)
        assert edition.primary_identifier == result
        assert [[item]] == provider.batches

    def test_import_feed_response(self):
        """Verify that import_feed_response instantiates the
        OPDS_IMPORTER_CLASS subclass and calls import_from_feed
        on it.
        """

        class MockOPDSImporter(OPDSImporter):
            def import_from_feed(self, text):
                """Return information that's useful for verifying
                that the OPDSImporter was instantiated with the
                right values.
                """
                return (
                    text, self.collection,
                    self.identifier_mapping, self.data_source_name
                )

        class MockProvider(MockOPDSImportCoverageProvider):
            OPDS_IMPORTER_CLASS = MockOPDSImporter

        provider = MockProvider(self._default_collection)
        provider.lookup_client = MockSimplifiedOPDSLookup(self._url)

        response = MockRequestsResponse(
            200, {'content-type': OPDSFeed.ACQUISITION_FEED_TYPE}, "some data"
        )
        id_mapping = object()
        (text, collection, mapping,
         data_source_name) = provider.import_feed_response(response, id_mapping)
        assert "some data" == text
        assert provider.collection == collection
        assert id_mapping == mapping
        assert provider.data_source.name == data_source_name
