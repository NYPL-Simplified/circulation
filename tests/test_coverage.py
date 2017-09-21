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
from core.util.opds_writer import OPDSFeed
from core.opds_import import (
    MockSimplifiedOPDSLookup,
    MockMetadataWranglerOPDSLookup,
)
from core.coverage import (
    CoverageFailure,
)

from core.util.http import BadResponseException
from core.util.opds_writer import OPDSMessage

from api.coverage import (
    ContentServerBibliographicCoverageProvider,
    MetadataWranglerCoverageProvider,
    MetadataWranglerCollectionSync,
    MetadataWranglerCollectionReaper,
    MetadataUploadCoverageProvider,
    OPDSImportCoverageProvider,
    MockOPDSImportCoverageProvider,
)

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
        
        # Here's an identifier that can't be looked up at all.
        identifier = self._identifier()
        messages_by_id = {
            identifier.urn : CoverageFailure(identifier, "201: try again later")
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
        success, failure1, failure2 = provider.process_batch(fake_batch)
        
        # The fake batch was provided to lookup_and_import_batch.
        eq_([fake_batch], provider.batches)

        # The matched Edition/LicensePool pair was returned.
        eq_(success, edition.primary_identifier)
        
        # The LicensePool of that pair was passed into finalize_license_pool.
        # The mismatched LicensePool was not.
        eq_([pool], provider.finalized)

        # The mismatched LicensePool turned into a CoverageFailure
        # object.
        assert isinstance(failure1, CoverageFailure)
        eq_('OPDS import operation imported LicensePool, but no Edition.',
            failure1.exception)
        eq_(pool2.identifier, failure1.obj)
        eq_(True, failure1.transient)
        
        # The failure was returned as a CoverageFailure object.
        assert isinstance(failure2, CoverageFailure)
        eq_(identifier, failure2.obj)
        eq_(True, failure2.transient)
        
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


class TestMetadataWranglerCoverageProvider(DatabaseTest):

    def create_provider(self, **kwargs):
        lookup = MockMetadataWranglerOPDSLookup.from_config(self._db, self.collection)
        return MetadataWranglerCoverageProvider(
            self.collection, lookup, **kwargs
        )

    def setup(self):
        super(TestMetadataWranglerCoverageProvider, self).setup()
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
            operation=self.provider.OPERATION
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
            status=CoverageRecord.TRANSIENT_FAILURE
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

    def opds_feed_identifiers(self):
        """Creates three Identifiers to use for testing with a sample OPDS file."""

        # Straightforward identifier that's represented in the OPDS response.
        valid_id = self._identifier(foreign_id=u'2020110')

        # Mapped identifier.
        source = DataSource.lookup(self._db, DataSource.AXIS_360)
        mapped_id = self._identifier(
            identifier_type=Identifier.AXIS_360_ID, foreign_id=u'0015187876'
        )
        equivalent_id = self._identifier(
            identifier_type=Identifier.ISBN, foreign_id=self._isbn
        )
        mapped_id.equivalent_to(source, equivalent_id, 1)

        # An identifier that's not represented in the OPDS response.
        lost_id = self._identifier()

        return valid_id, mapped_id, lost_id

    def test_add_coverage_record_for(self):
        identifier = self._identifier()
        record, _is_new = self.provider.add_coverage_record_for(identifier)

        eq_(True, isinstance(record, CoverageRecord))
        eq_(identifier, record.identifier)
        eq_(CoverageRecord.SUCCESS, record.status)
        eq_(self.provider.collection, record.collection)
        eq_(self.provider.OPERATION, record.operation)

    def test_process_feed_response(self):
        data = sample_data("metadata_reaper_response.opds", "opds")
        response = MockRequestsResponse(
            200, {'content-type': OPDSFeed.ACQUISITION_FEED_TYPE}, data
        )

        # A successful response gives us OPDSMessage objects.
        values = list(self.provider.process_feed_response(response, {}))
        for x in values:
            assert isinstance(x, OPDSMessage)

        eq_(['Successfully removed', 'Not in collection catalog',
             "I've never heard of this work."],
            [x.message for x in values]
        )

    def test_process_batch_errors(self):
        """When errors are raised during batch processing, the entire batch
        gets CoverageFailures.
        """
        # If the 'server' sends data with the wrong media type, the whole
        # batch gets CoverageFailures.
        self.lookup.queue_response(
            200, {'content-type': 'json/application'}, u'{ "title": "It broke." }'
        )

        id1 = self._identifier()
        id2 = self._identifier()
        results = self.provider.process_batch([id1, id2])
        for result in results:
            eq_(True, isinstance(result, CoverageFailure))
            assert result.obj in [id1, id2]
            eq_(True, result.transient)
            eq_(self.provider.collection, result.collection)
            assert "It broke." in result.exception

        # If the 'server' is down, the whole batch gets CoverageFailures.
        self.lookup.queue_response(
            500, {'content-type': OPDSFeed.ACQUISITION_FEED_TYPE}, 'Internal Server Error'
        )
        results = self.provider.process_batch([id1, id2])
        for result in results:
            eq_(True, isinstance(result, CoverageFailure))
            assert result.obj in [id1, id2]
            eq_(True, result.transient)
            assert 'Internal Server Error' in result.exception

        # If a message comes back with an unexpected status, a
        # CoverageFailure is created.
        data = sample_data('unknown_message_status_code.opds', 'opds')
        valid_id = self.opds_feed_identifiers()[0]
        self.lookup.queue_response(
            200, {'content-type': OPDSFeed.ACQUISITION_FEED_TYPE}, data
        )
        [result] = self.provider.process_batch([valid_id])
        eq_(True, isinstance(result, CoverageFailure))
        eq_(valid_id, result.obj)
        eq_(self.provider.collection, result.collection)
        eq_('Unknown OPDSMessage status: 418', result.exception)

    def test_coverage_records_for_unhandled_items_include_collection(self):
        data = sample_data('metadata_sync_response.opds', 'opds')
        self.lookup.queue_response(
            200, {'content-type': OPDSFeed.ACQUISITION_FEED_TYPE}, data
        )

        identifier = self._identifier()
        self.provider.process_batch_and_handle_results([identifier])
        [record] = identifier.coverage_records
        eq_(CoverageRecord.TRANSIENT_FAILURE, record.status)
        eq_(self.provider.data_source, record.data_source)
        eq_(self.provider.operation, record.operation)
        eq_(self.provider.collection, record.collection)


class TestMetadataWranglerCollectionSync(MetadataWranglerCollectionManagerTest):

    def setup(self):
        super(TestMetadataWranglerCollectionSync, self).setup()
        self.provider = MetadataWranglerCollectionSync(
            self.collection, self.lookup
        )

    def test_items_that_need_coverage(self):
        source = self.provider.data_source
        other_collection = self._collection(protocol=ExternalIntegration.OVERDRIVE)

        # An item that has been synced for some other Collection, but not
        # this one.
        e1, other_covered = self._edition(
            with_license_pool=True, collection=other_collection,
            identifier_type=Identifier.AXIS_360_ID,
        )
        uncovered = self._licensepool(
            e1, with_open_access_download=True,
            collection=self.provider.collection
        )
        cr = self._coverage_record(
            uncovered.identifier, source, collection=other_collection
        )

        # We've lost our license to this item and it has been covered
        # by the reaper operation already.
        e2, reaped = self._edition(
            with_license_pool=True, collection=self.provider.collection,
            identifier_type=Identifier.ONECLICK_ID,
        )
        reaped.update_availability(0, 0, 0, 0)
        reaper_cr = self._coverage_record(
            reaped.identifier, source,
            operation=self.provider.OPERATION,
            collection=self.provider.collection
        )

        # An item that has been covered by the reaper operation, but has
        # had its license repurchased.
        e3, relicensed = self._edition(
            with_license_pool=True, collection=self.provider.collection,
            identifier_type=Identifier.BIBLIOTHECA_ID
        )
        relicensed_coverage_record = self._coverage_record(
            relicensed.identifier, source,
            operation=CoverageRecord.REAP_OPERATION,
            collection=self.provider.collection
        )
        relicensed.update_availability(1, 0, 0, 0)

        # An item in the Collection that doesn't have any licenses.
        e4, uncovered_unlicensed = self._edition(
            with_license_pool=True,
            collection=self.provider.collection,
            identifier_type=Identifier.OVERDRIVE_ID
        )
        uncovered_unlicensed.update_availability(0, 0, 0, 0)

        items = self.provider.items_that_need_coverage().all()

        # Provider ignores anything that doesn't have licenses.
        assert uncovered_unlicensed.identifier not in items
        assert reaped.identifier not in items

        # But it picks up anything that hasn't been covered at all and anything
        # that's been licensed anew even if its already been reaped.
        assert uncovered.identifier in items
        assert relicensed.identifier in items
        eq_(2, len(items))

        # The REAP coverage record for the repurchased book has been
        # deleted, and committing will remove it from the database.
        assert relicensed_coverage_record in relicensed.identifier.coverage_records
        self._db.commit()
        assert relicensed_coverage_record not in relicensed.identifier.coverage_records

    def test_process_batch(self):
        data = sample_data('metadata_sync_response.opds', 'opds')
        self.lookup.queue_response(
            200, {'content-type': OPDSFeed.ACQUISITION_FEED_TYPE}, data
        )

        valid_id, mapped_id, lost_id = self.opds_feed_identifiers()
        results = self.provider.process_batch([valid_id, mapped_id, lost_id])
        eq_(valid_id, results[0])
        eq_(mapped_id, results[1])


class TestMetadataWranglerCollectionReaper(MetadataWranglerCollectionManagerTest):

    def setup(self):
        super(TestMetadataWranglerCollectionReaper, self).setup()
        self.provider = MetadataWranglerCollectionReaper(
            self.collection, self.lookup
        )

    def test_items_that_need_coverage(self):
        """The reaper only returns identifiers with unlicensed license_pools
        that have been synced with the Metadata Wrangler.
        """
        # Create a Wrangler-synced item that doesn't have any owned licenses
        covered_unlicensed_lp = self._licensepool(
            None, open_access=False, set_edition_as_presentation=True,
            collection=self.collection
        )
        covered_unlicensed_lp.update_availability(0, 0, 0, 0)
        cr = self._coverage_record(
            covered_unlicensed_lp.presentation_edition, self.source,
            operation=CoverageRecord.SYNC_OPERATION,
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
        # Queue up a feed with different possible Metadata Wrangler
        # responses.
        data = sample_data('metadata_reaper_response.opds', 'opds')
        self.lookup.queue_response(
            200, {'content-type': OPDSFeed.ACQUISITION_FEED_TYPE}, data
        )

        valid_id, mapped_id, lost_id = self.opds_feed_identifiers()
        results = self.provider.process_batch([valid_id, mapped_id, lost_id])

        eq_(valid_id, results[0])
        eq_(mapped_id, results[1])

    def test_finalize_batch(self):
        """Metadata Wrangler sync coverage records are deleted from the db
        when the the batch is finalized if the item has been reaped.
        """
        # Create two identifiers that have been either synced or reaped.
        sync_cr = self._coverage_record(
            self._edition(), self.source, operation=CoverageRecord.SYNC_OPERATION,
            collection=self.provider.collection
        )
        reaped_cr = self._coverage_record(
            self._edition(), self.source, operation=CoverageRecord.REAP_OPERATION,
            collection=self.provider.collection
        )

        # Create coverage records for an Identifier that has been both synced
        # and reaped.
        doubly_covered = self._edition()
        doubly_sync_record = self._coverage_record(
            doubly_covered, self.source, operation=CoverageRecord.SYNC_OPERATION,
            collection=self.provider.collection
        )
        doubly_reap_record = self._coverage_record(
            doubly_covered, self.source, operation=CoverageRecord.REAP_OPERATION,
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
            operation=self.provider.OPERATION
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

class TestContentServerBibliographicCoverageProvider(DatabaseTest):

    def test_script_instantiation(self):
        """Test that RunCollectionCoverageProviderScript can instantiate
        the coverage provider.
        """
        # Create a Collection to be found.
        collection = self._collection(
            name="OA Content", data_source_name=DataSource.OA_CONTENT_SERVER
        )

        script = RunCollectionCoverageProviderScript(
            ContentServerBibliographicCoverageProvider,
            _db=self._db, lookup_client=object()
        )
        assert isinstance(script.providers[0],
                          ContentServerBibliographicCoverageProvider)
        eq_(collection, script.providers[0].collection)

    def test_finalize_license_pool(self):

        identifier = self._identifier()
        license_source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        data_source = DataSource.lookup(self._db, DataSource.OA_CONTENT_SERVER)

        # Here's a LicensePool with no presentation edition.
        pool, is_new = LicensePool.for_foreign_id(
            self._db, license_source, identifier.type, identifier.identifier,
            collection=self._default_collection
        )
        eq_(None, pool.presentation_edition)

        # Here's an Edition for the same book as the LicensePool but
        # from a different data source.
        edition, is_new = Edition.for_foreign_id(
            self._db, data_source, identifier.type, identifier.identifier
        )
        edition.title = self._str

        # Although Edition and LicensePool share an identifier, they
        # are not otherwise related.
        eq_(None, pool.presentation_edition)

        # finalize_license_pool() will create a Work and update the
        # LicensePool's presentation edition, based on the brand-new
        # Edition.
        lookup = MockSimplifiedOPDSLookup(self._url)        
        provider = ContentServerBibliographicCoverageProvider(
            self._default_collection, lookup
        )
        provider.finalize_license_pool(pool)
        work = pool.work
        eq_(edition.title, pool.presentation_edition.title)
        eq_(True, work.presentation_ready)
        
    def test_only_open_access_books_considered(self):

        lookup = MockSimplifiedOPDSLookup(self._url)        
        provider = ContentServerBibliographicCoverageProvider(
            self._default_collection, lookup
        )

        # Here's an open-access work.
        w1 = self._work(with_license_pool=True, with_open_access_download=True)

        # Here's a work that's not open-access.
        w2 = self._work(with_license_pool=True, with_open_access_download=False)
        w2.license_pools[0].open_access = False

        # Only the open-access work needs coverage.
        eq_([w1.license_pools[0].identifier],
            provider.items_that_need_coverage().all())
