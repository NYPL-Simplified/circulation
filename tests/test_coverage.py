import datetime

from nose.tools import (
    assert_raises,
    assert_raises_regexp,
    set_trace,
    eq_,
)
import feedparser

from . import (
    DatabaseTest,
    sample_data,
)

from core.scripts import RunCollectionCoverageProviderScript
from core.testing import MockRequestsResponse

from core.config import (
    CannotLoadConfiguration,
    Configuration,
    temp_config,
)
from core.model import (
    Collection,
    CoverageRecord,
    DataSource,
    Edition,
    ExternalIntegration,
    Hyperlink,
    Identifier,
    LicensePool,
    Work,
    WorkCoverageRecord,
)
from core.util.opds_writer import (
    OPDSFeed,
    OPDSMessage,
)
from core.opds_import import (
    MockSimplifiedOPDSLookup,
    MockMetadataWranglerOPDSLookup,
    OPDSImporter,
)
from core.coverage import (
    CoverageFailure,
)

from core.util.http import BadResponseException
from core.util.opds_writer import OPDSMessage

from api.coverage import (
    BaseMetadataWranglerCoverageProvider,
    MetadataUploadCoverageProvider,
    MetadataWranglerCollectionReaper,
    MetadataWranglerCollectionRegistrar,
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
        eq_([200, 201, 202], RegistrarImporter.SUCCESS_STATUS_CODES)
        eq_([200, 404], ReaperImporter.SUCCESS_STATUS_CODES)


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
        eq_(id1, identifier)

        [edition] = id1.primarily_identifies
        eq_("Here's your title!", edition.title)

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
        eq_(None, pool.work)

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
        eq_([fake_batch], provider.batches)

        # The matched Edition/LicensePool pair was returned.
        eq_(success_import, edition.primary_identifier)
        
        # The LicensePool of that pair was passed into finalize_license_pool.
        # The mismatched LicensePool was not.
        eq_([pool], provider.finalized)

        # The mismatched LicensePool turned into a CoverageFailure
        # object.
        assert isinstance(failure_mismatched, CoverageFailure)
        eq_('OPDS import operation imported LicensePool, but no Edition.',
            failure_mismatched.exception)
        eq_(pool2.identifier, failure_mismatched.obj)
        eq_(True, failure_mismatched.transient)
        
        # The OPDSMessage with status code 500 was returned as a
        # CoverageFailure object.
        assert isinstance(failure_message, CoverageFailure)
        eq_("500: internal error", failure_message.exception)
        eq_(error_identifier, failure_message.obj)
        eq_(True, failure_message.transient)

        # The identifier that had a treat-as-success OPDSMessage was returned
        # as-is.
        eq_(not_an_error_identifier, success_message)
        
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
        eq_(edition.primary_identifier, success)

        # However, since there is no LicensePool, nothing was finalized.
        eq_([], provider.finalized)

    def test_process_item(self):
        """To process a single item we process a batch containing
        only that item.
        """
        provider = self._provider()
        edition = self._edition()
        provider.queue_import_results([edition], [], [], {})
        item = object()
        result = provider.process_item(item)
        eq_(edition.primary_identifier, result)
        eq_([[item]], provider.batches)

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
        eq_("some data", text)
        eq_(provider.collection, collection)
        eq_(id_mapping, mapping)
        eq_(provider.data_source.name, data_source_name)
            

class MetadataWranglerCoverageProviderTest(DatabaseTest):

    def create_provider(self, **kwargs):
        lookup = MockMetadataWranglerOPDSLookup.from_config(self._db, self.collection)
        return self.TEST_CLASS(self.collection, lookup, **kwargs)

    def setup(self):
        super(MetadataWranglerCoverageProviderTest, self).setup()
        self.integration = self._external_integration(
            ExternalIntegration.METADATA_WRANGLER,
            goal=ExternalIntegration.METADATA_GOAL, url=self._url,
            username=u'abc', password=u'def'
        )
        self.source = DataSource.lookup(self._db, DataSource.METADATA_WRANGLER)
        self.collection = self._collection(
            protocol=ExternalIntegration.BIBLIOTHECA, external_account_id=u'lib'
        )
        self.provider = self.create_provider()
        self.lookup_client = self.provider.lookup_client

    def opds_feed_identifiers(self):
        """Creates three Identifiers to use for testing with sample OPDS files."""

        # An identifier directly represented in the OPDS response.
        valid_id = self._identifier(foreign_id=u'2020110')

        # An identifier mapped to an identifier represented in the OPDS
        # response.
        source = DataSource.lookup(self._db, DataSource.AXIS_360)
        mapped_id = self._identifier(
            identifier_type=Identifier.AXIS_360_ID, foreign_id=u'0015187876'
        )
        equivalent_id = self._identifier(
            identifier_type=Identifier.ISBN, foreign_id='9781936460236'
        )
        mapped_id.equivalent_to(source, equivalent_id, 1)

        # An identifier that's not represented in the OPDS response.
        lost_id = self._identifier()

        return valid_id, mapped_id, lost_id


class TestBaseMetadataWranglerCoverageProvider(MetadataWranglerCoverageProviderTest):

    class Mock(BaseMetadataWranglerCoverageProvider):
        SERVICE_NAME = "Mock"
        DATA_SOURCE_NAME = DataSource.OVERDRIVE

    TEST_CLASS = Mock

    def test_must_be_authenticated(self):
        """CannotLoadConfiguration is raised if you try to create a
        metadata wrangler coverage provider that can't authenticate
        with the metadata wrangler.
        """
        class UnauthenticatedLookupClient(object):
            authenticated = False

        assert_raises_regexp(
            CannotLoadConfiguration,
            "Authentication for the Library Simplified Metadata Wrangler ",
            self.Mock, self.collection, UnauthenticatedLookupClient()
        )

    def test_input_identifier_types(self):
        """Verify all the different types of identifiers we send
        to the metadata wrangler.
        """
        eq_(
            set([
                Identifier.OVERDRIVE_ID,
                Identifier.BIBLIOTHECA_ID,
                Identifier.AXIS_360_ID,
                Identifier.ONECLICK_ID,
                Identifier.URI,
            ]), 
            set(BaseMetadataWranglerCoverageProvider.INPUT_IDENTIFIER_TYPES)
        )

    def test_create_identifier_mapping(self):
        # Most identifiers map to themselves.
        overdrive = self._identifier(Identifier.OVERDRIVE_ID)

        # But Axis 360 and 3M identifiers map to equivalent ISBNs.
        axis = self._identifier(Identifier.AXIS_360_ID)
        threem = self._identifier(Identifier.THREEM_ID)
        isbn_axis = self._identifier(Identifier.ISBN)
        isbn_threem = self._identifier(Identifier.ISBN)

        who_says = DataSource.lookup(self._db, DataSource.AXIS_360)

        axis.equivalent_to(who_says, isbn_axis, 1)
        threem.equivalent_to(who_says, isbn_threem, 1)

        mapping = self.provider.create_identifier_mapping([overdrive, axis, threem])
        eq_(overdrive, mapping[overdrive])
        eq_(axis, mapping[isbn_axis])
        eq_(threem, mapping[isbn_threem])

    def test_coverage_records_for_unhandled_items_include_collection(self):
        """NOTE: This could be made redundant by adding test coverage to
        CoverageProvider.process_batch_and_handle_results in core.

        """
        data = sample_data('metadata_sync_response.opds', 'opds')
        self.lookup_client.queue_response(
            200, {'content-type': OPDSFeed.ACQUISITION_FEED_TYPE}, data
        )

        identifier = self._identifier()
        self.provider.process_batch_and_handle_results([identifier])
        [record] = identifier.coverage_records
        eq_(CoverageRecord.TRANSIENT_FAILURE, record.status)
        eq_(self.provider.data_source, record.data_source)
        eq_(self.provider.operation, record.operation)
        eq_(self.provider.collection, record.collection)


class TestMetadataWranglerCollectionRegistrar(MetadataWranglerCoverageProviderTest):

    TEST_CLASS = MetadataWranglerCollectionRegistrar

    def test_constants(self):
        # This CoverageProvider runs Identifiers through the 'lookup'
        # endpoint and marks success with CoverageRecords that have
        # the IMPORT_OPERATION operation.
        eq_(self.provider.lookup_client.lookup, self.provider.api_method)
        eq_(CoverageRecord.IMPORT_OPERATION, self.TEST_CLASS.OPERATION)

    def test_process_batch(self):
        """End-to-end test of the registrar's process_batch() implementation.
        """
        data = sample_data('metadata_sync_response.opds', 'opds')
        self.lookup_client.queue_response(
            200, {'content-type': OPDSFeed.ACQUISITION_FEED_TYPE}, data
        )

        valid_id, mapped_id, lost_id = self.opds_feed_identifiers()
        results = self.provider.process_batch(
            [valid_id, mapped_id, lost_id]
        )

        # The Identifier that resulted in a 200 message was returned.
        #
        # The Identifier that resulted in a 201 message was returned.
        #
        # The Identifier that was ignored by the server was not
        # returned.
        #
        # The Identifier that was not requested but was sent back by
        # the server anyway was ignored.
        eq_(sorted([valid_id, mapped_id]), sorted(results))

    def test_process_batch_errors(self):
        """When errors are raised during batch processing, an exception is
        raised and no CoverageRecords are created.
        """
        # This happens if the 'server' sends data with the wrong media
        # type.
        self.lookup_client.queue_response(
            200, {'content-type': 'json/application'}, u'{ "title": "It broke." }'
        )

        id1 = self._identifier()
        id2 = self._identifier()
        assert_raises_regexp(
            BadResponseException, 'Wrong media type', 
            self.provider.process_batch, [id1, id2]
        )
        eq_([], id1.coverage_records)
        eq_([], id2.coverage_records)

        # Of if the 'server' sends an error response code.
        self.lookup_client.queue_response(
            500, {'content-type': OPDSFeed.ACQUISITION_FEED_TYPE},
            'Internal Server Error'
        )
        assert_raises_regexp(
            BadResponseException, "Got status code 500", 
            self.provider.process_batch, [id1, id2]
        )
        eq_([], id1.coverage_records)
        eq_([], id2.coverage_records)

        # If a message comes back with an unexpected status, a
        # CoverageFailure is created.
        data = sample_data('unknown_message_status_code.opds', 'opds')
        valid_id = self.opds_feed_identifiers()[0]
        self.lookup_client.queue_response(
            200, {'content-type': OPDSFeed.ACQUISITION_FEED_TYPE}, data
        )
        [result] = self.provider.process_batch([valid_id])
        eq_(True, isinstance(result, CoverageFailure))
        eq_(valid_id, result.obj)
        eq_('418: Mad Hatter', result.exception)

        # The OPDS importer didn't know which Collection to associate
        # with this CoverageFailure, but the CoverageProvider does,
        # and it set .collection appropriately.
        eq_(self.provider.collection, result.collection)

    def test_items_that_need_coverage_excludes_unavailable_items(self):
        """A LicensePool that's not actually available doesn't need coverage.
        """
        edition, pool = self._edition(
            with_license_pool=True, collection=self.collection,
            identifier_type=Identifier.BIBLIOTHECA_ID
        )
        pool.licenses_owned = 0
        eq_(0, self.provider.items_that_need_coverage().count())

        # Open-access titles _do_ need coverage.
        pool.open_access = True
        eq_([pool.identifier], self.provider.items_that_need_coverage().all())

    def test_items_that_need_coverage_removes_reap_records_for_relicensed_items(self):
        """A LicensePool that's not actually available doesn't need coverage.
        """
        edition, pool = self._edition(
            with_license_pool=True, collection=self.collection,
            identifier_type=Identifier.BIBLIOTHECA_ID
        )

        identifier = pool.identifier
        original_coverage_records = list(identifier.coverage_records)

        # This identifier was reaped...
        cr = self._coverage_record(
            pool.identifier, self.provider.data_source,
            operation=CoverageRecord.REAP_OPERATION, 
            collection=self.collection
        )
        eq_(
            set(original_coverage_records + [cr]), 
            set(identifier.coverage_records)
        )

        # ... but then it was relicensed.
        pool.licenses_owned = 10

        eq_([identifier], self.provider.items_that_need_coverage().all())

        # The now-inaccurate REAP record has been removed.
        eq_(original_coverage_records, identifier.coverage_records)

    def test_identifier_covered_in_one_collection_not_covered_in_another(self):
        edition, pool = self._edition(
            with_license_pool=True, collection=self.collection,
            identifier_type=Identifier.BIBLIOTHECA_ID
        )

        identifier = pool.identifier
        other_collection = self._collection()

        # This Identifier needs coverage.
        qu = self.provider.items_that_need_coverage()
        eq_([identifier], qu.all())

        # Adding coverage for an irrelevant collection won't fix that.
        cr = self._coverage_record(
            pool.identifier, self.provider.data_source,
            operation=self.provider.OPERATION, 
            collection=other_collection
        )
        eq_([identifier], qu.all())

        # Adding coverage for the relevant collection will.
        cr = self._coverage_record(
            pool.identifier, self.provider.data_source,
            operation=self.provider.OPERATION, 
            collection=self.provider.collection
        )
        eq_([], qu.all())

    def test_identifier_reaped_from_one_collection_covered_in_another(self):
        """An Identifier can be reaped from one collection but still
        need coverage in another.
        """
        edition, pool = self._edition(
            with_license_pool=True, collection=self.collection,
            identifier_type=Identifier.BIBLIOTHECA_ID
        )

        identifier = pool.identifier
        other_collection = self._collection()

        # This identifier was reaped from other_collection, but not
        # from self.provider.collection.
        cr = self._coverage_record(
            pool.identifier, self.provider.data_source,
            operation=CoverageRecord.REAP_OPERATION, 
            collection=other_collection
        )

        # It still needs to be covered in self.provider.collection.
        eq_([identifier], self.provider.items_that_need_coverage().all())

    def test_items_that_need_coverage_respects_cutoff(self):
        """Verify that this coverage provider respects the cutoff_time
        argument.
        """

        edition, pool = self._edition(
            with_license_pool=True, collection=self.collection,
            identifier_type=Identifier.BIBLIOTHECA_ID
        )
        cr = self._coverage_record(
            pool.identifier, self.provider.data_source,
            operation=self.provider.OPERATION, collection=self.collection
        )

        # We have a coverage record already, so this book doesn't show
        # up in items_that_need_coverage
        items = self.provider.items_that_need_coverage().all()
        eq_([], items)

        # But if we send a cutoff_time that's later than the time
        # associated with the coverage record...
        one_hour_from_now = (
            datetime.datetime.utcnow() + datetime.timedelta(seconds=3600)
        )
        provider_with_cutoff = self.create_provider(
            cutoff_time=one_hour_from_now
        )

        # The book starts showing up in items_that_need_coverage.
        eq_([pool.identifier], 
            provider_with_cutoff.items_that_need_coverage().all())

    def test_items_that_need_coverage_respects_count_as_covered(self):
        # Here's a coverage record with a transient failure.
        edition, pool = self._edition(
            with_license_pool=True, collection=self.collection,
            identifier_type=Identifier.OVERDRIVE_ID,
        )
        cr = self._coverage_record(
            pool.identifier, self.provider.data_source, 
            operation=self.provider.operation,
            status=CoverageRecord.TRANSIENT_FAILURE,
            collection=self.collection
        )
        
        # Ordinarily, a transient failure does not count as coverage.
        [needs_coverage] = self.provider.items_that_need_coverage().all()
        eq_(needs_coverage, pool.identifier)

        # But if we say that transient failure counts as coverage, it
        # does count.
        eq_([],
            self.provider.items_that_need_coverage(
                count_as_covered=CoverageRecord.TRANSIENT_FAILURE
            ).all()
        )

    def test_isbn_covers_are_imported_from_mapped_identifiers(self):
        # Now that we pass ISBN equivalents instead of Bibliotheca identifiers
        # to the Metadata Wrangler, they're not getting covers. Let's confirm
        # that the problem isn't on the Circulation Manager import side of things.

        # Create a Bibliotheca identifier with a license pool.
        source = DataSource.lookup(self._db, DataSource.BIBLIOTHECA)
        identifier = self._identifier(identifier_type=Identifier.BIBLIOTHECA_ID)
        LicensePool.for_foreign_id(
            self._db, source, identifier.type, identifier.identifier,
            collection=self.provider.collection
        )

        # Create an ISBN and set it equivalent.
        isbn = self._identifier(identifier_type=Identifier.ISBN)
        isbn.identifier = '9781594632556'
        identifier.equivalent_to(source, isbn, 1)

        opds = sample_data('metadata_isbn_response.opds', 'opds')
        self.provider.lookup_client.queue_response(
            200, {'content-type': 'application/atom+xml;profile=opds-catalog;kind=acquisition'}, opds
        )

        result = self.provider.process_item(identifier)
        # The lookup is successful
        eq_(result, identifier)
        # The appropriate cover links are transferred.
        identifier_uris = [l.resource.url for l in identifier.links
                           if l.rel in [Hyperlink.IMAGE, Hyperlink.THUMBNAIL_IMAGE]]
        expected = [
            'http://book-covers.nypl.org/Content%20Cafe/ISBN/9781594632556/cover.jpg',
            'http://book-covers.nypl.org/scaled/300/Content%20Cafe/ISBN/9781594632556/cover.jpg'
        ]

        eq_(sorted(identifier_uris), sorted(expected))

        # The ISBN doesn't get any information.
        eq_(isbn.links, [])

class MetadataWranglerCollectionManagerTest(DatabaseTest):

    def setup(self):
        super(MetadataWranglerCollectionManagerTest, self).setup()
        self.integration = self._external_integration(
            ExternalIntegration.METADATA_WRANGLER,
            goal=ExternalIntegration.METADATA_GOAL, url=self._url,
            username=u'abc', password=u'def'
        )
        self.source = DataSource.lookup(self._db, DataSource.METADATA_WRANGLER)
        self.collection = self._collection(
            protocol=ExternalIntegration.BIBLIOTHECA, external_account_id=u'lib'
        )
        self.lookup = MockMetadataWranglerOPDSLookup.from_config(
            self._db, collection=self.collection
        )


class TestMetadataWranglerCollectionReaper(MetadataWranglerCoverageProviderTest):

    TEST_CLASS = MetadataWranglerCollectionReaper

    def test_constants(self):
        # This CoverageProvider runs Identifiers through the 'remove'
        # endpoint and marks success with CoverageRecords that have
        # the REAP_OPERATION operation.
        eq_(CoverageRecord.REAP_OPERATION, self.TEST_CLASS.OPERATION)
        eq_(self.provider.lookup_client.remove, self.provider.api_method)

    def test_items_that_need_coverage(self):
        """The reaper only returns identifiers with no-longer-licensed
        license_pools that have been synced with the Metadata
        Wrangler.
        """
        # Create an item that was imported into the Wrangler-side
        # collection but no longer has any owned licenses
        covered_unlicensed_lp = self._licensepool(
            None, open_access=False, set_edition_as_presentation=True,
            collection=self.collection
        )
        covered_unlicensed_lp.update_availability(0, 0, 0, 0)
        cr = self._coverage_record(
            covered_unlicensed_lp.presentation_edition, self.source,
            operation=CoverageRecord.IMPORT_OPERATION,
            collection=self.provider.collection,
        )

        # Create an unsynced item that doesn't have any licenses
        uncovered_unlicensed_lp = self._licensepool(None, open_access=False)
        uncovered_unlicensed_lp.update_availability(0, 0, 0, 0)

        # And an unsynced item that has licenses.
        licensed_lp = self._licensepool(None, open_access=False)

        # Create an open access license pool
        open_access_lp = self._licensepool(None)

        items = self.provider.items_that_need_coverage().all()
        eq_(1, len(items))

        # Items that are licensed are ignored.
        assert licensed_lp.identifier not in items

        # Items with open access license pools are ignored.
        assert open_access_lp.identifier not in items

        # Items that haven't been synced with the Metadata Wrangler are
        # ignored, even if they don't have licenses.
        assert uncovered_unlicensed_lp.identifier not in items

        # Only synced items without owned licenses are returned.
        eq_([covered_unlicensed_lp.identifier], items)

        # Items that had unsuccessful syncs are not returned.
        cr.status = CoverageRecord.TRANSIENT_FAILURE
        eq_([], self.provider.items_that_need_coverage().all())

    def test_process_batch(self):
        data = sample_data('metadata_reaper_response.opds', 'opds')
        self.lookup_client.queue_response(
            200, {'content-type': OPDSFeed.ACQUISITION_FEED_TYPE}, data
        )

        valid_id, mapped_id, lost_id = self.opds_feed_identifiers()
        results = self.provider.process_batch([valid_id, mapped_id, lost_id])

        # The valid_id and mapped_id were handled successfully.
        # The server ignored lost_id, so nothing happened to it,
        # and the server sent a fourth ID we didn't ask for,
        # which we ignored.
        eq_(sorted(results), sorted([valid_id, mapped_id]))

    def test_finalize_batch(self):
        """Metadata Wrangler sync coverage records are deleted from the db
        when the the batch is finalized if the item has been reaped.
        """

        # Create an identifier that has been imported and one that's
        # been reaped.
        sync_cr = self._coverage_record(
            self._edition(), self.source,
            operation=CoverageRecord.IMPORT_OPERATION,
            collection=self.provider.collection
        )
        reaped_cr = self._coverage_record(
            self._edition(), self.source,
            operation=CoverageRecord.REAP_OPERATION,
            collection=self.provider.collection
        )

        # Create coverage records for an Identifier that has been both synced
        # and reaped.
        doubly_covered = self._edition()
        doubly_sync_record = self._coverage_record(
            doubly_covered, self.source,
            operation=CoverageRecord.IMPORT_OPERATION,
            collection=self.provider.collection
        )
        doubly_reap_record = self._coverage_record(
            doubly_covered, self.source,
            operation=CoverageRecord.REAP_OPERATION,
            collection=self.provider.collection,
        )

        self.provider.finalize_batch()
        remaining_records = self._db.query(CoverageRecord).all()

        # The syncing record has been deleted from the database
        assert doubly_sync_record not in remaining_records
        eq_(sorted([sync_cr, reaped_cr, doubly_reap_record]), sorted(remaining_records))


class TestMetadataUploadCoverageProvider(DatabaseTest):

    def create_provider(self, **kwargs):
        upload_client = MockMetadataWranglerOPDSLookup.from_config(self._db, self.collection)
        return MetadataUploadCoverageProvider(
            self.collection, upload_client, **kwargs
        )

    def setup(self):
        super(TestMetadataUploadCoverageProvider, self).setup()
        self.integration = self._external_integration(
            ExternalIntegration.METADATA_WRANGLER,
            goal=ExternalIntegration.METADATA_GOAL, url=self._url,
            username=u'abc', password=u'def'
        )
        self.source = DataSource.lookup(self._db, DataSource.METADATA_WRANGLER)
        self.collection = self._collection(
            protocol=ExternalIntegration.BIBLIOTHECA, external_account_id=u'lib'
        )
        self.provider = self.create_provider()

    def test_items_that_need_coverage_only_finds_transient_failures(self):
        """Verify that this coverage provider only covers items that have
        transient failure CoverageRecords.
        """

        edition, pool = self._edition(
            with_license_pool=True, collection=self.collection,
            identifier_type=Identifier.BIBLIOTHECA_ID
        )
        # We don't have a CoverageRecord yet, so the book doesn't show up.
        items = self.provider.items_that_need_coverage().all()
        eq_([], items)
        
        cr = self._coverage_record(
            pool.identifier, self.provider.data_source,
            operation=self.provider.OPERATION, collection=self.collection
        )

        # With a successful or persistent failure CoverageRecord, it still doesn't show up.
        cr.status = CoverageRecord.SUCCESS
        items = self.provider.items_that_need_coverage().all()
        eq_([], items)

        cr.status = CoverageRecord.PERSISTENT_FAILURE
        items = self.provider.items_that_need_coverage().all()
        eq_([], items)

        # But with a transient failure record it does.
        cr.status = CoverageRecord.TRANSIENT_FAILURE
        items = self.provider.items_that_need_coverage().all()
        eq_([edition.primary_identifier], items)

    def test_process_batch_uploads_metadata(self):
        class MockMetadataClient(object):
            metadata_feed = None
            authenticated = True
            def canonicalize_author_name(self, identifier, working_display_name):
                return working_display_name
            def add_with_metadata(self, feed):
                self.metadata_feed = feed
        metadata_client = MockMetadataClient()

        provider = MetadataUploadCoverageProvider(
            self.collection, metadata_client
        )


        edition, pool = self._edition(
            with_license_pool=True, collection=self.collection,
            identifier_type=Identifier.BIBLIOTHECA_ID
        )
        work = pool.calculate_work()

        # This identifier has no Work.
        no_work = self._identifier()


        results = provider.process_batch([pool.identifier, no_work])

        # An OPDS feed of metadata was sent to the metadata wrangler.
        assert metadata_client.metadata_feed != None
        feed = feedparser.parse(unicode(metadata_client.metadata_feed))
        urns = [entry.get("id") for entry in feed.get("entries", [])]
        # Only the identifier work a work ends up in the feed.
        eq_([pool.identifier.urn], urns)

        # There are two results: the identifier with a work and a CoverageFailure.
        eq_(2, len(results))
        assert pool.identifier in results
        [failure] = [r for r in results if isinstance(r, CoverageFailure)]
        eq_(no_work, failure.obj)
