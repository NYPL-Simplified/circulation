import datetime

from nose.tools import (
    assert_raises_regexp,
    set_trace,
    eq_,
)

from . import (
    DatabaseTest,
    sample_data,
)

from core.scripts import RunCoverageProviderScript
from core.external_search import DummyExternalSearchIndex
from core.testing import MockRequestsResponse

from core.config import (
    Configuration,
    temp_config,
)
from core.model import (
    CoverageRecord,
    DataSource,
    Edition,
    Identifier,
    LicensePool,
    Work,
    WorkCoverageRecord,
)
from core.util.opds_writer import OPDSFeed
from core.opds_import import (
    MockSimplifiedOPDSLookup,
)
from core.coverage import (
    CoverageFailure,
)

from core.util.http import BadResponseException

from api.coverage import (
    ContentServerBibliographicCoverageProvider,
    MetadataWranglerCoverageProvider,
    MetadataWranglerCollectionReaper,
    OPDSImportCoverageProvider,
    MockOPDSImportCoverageProvider,
    SearchIndexCoverageProvider,
)

class TestOPDSImportCoverageProvider(DatabaseTest):

    def _provider(self, presentation_ready_on_success=True):
        """Create a generic MockOPDSImportCoverageProvider for testing purposes."""
        source = DataSource.lookup(self._db, DataSource.OA_CONTENT_SERVER)
        return MockOPDSImportCoverageProvider(
            "mock provider", [], source,
            presentation_ready_on_success=presentation_ready_on_success
        )

    def test_badresponseexception_on_non_opds_feed(self):

        response = MockRequestsResponse(200, {"content-type" : "text/plain"}, "Some data")
        
        provider = self._provider()
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

            # Mock the identifier mapping
            def create_identifier_mapping(self, batch):
                return self.mapping

        # This means we need to mock the lookup client instead.
        lookup = MockSimplifiedOPDSLookup(self._url)

        source = DataSource.lookup(self._db, DataSource.OA_CONTENT_SERVER)
        provider = TestProvider(
            "test provider", [], source, lookup=lookup
        )

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
        eq_(id1, edition.primary_identifier)

    def test_finalize_license_pool(self):

        provider_no_presentation_ready = self._provider(presentation_ready_on_success=False)
        provider_presentation_ready = self._provider(presentation_ready_on_success=True)
        identifier = self._identifier()
        license_source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        data_source = DataSource.lookup(self._db, DataSource.OA_CONTENT_SERVER)

        # Here's a LicensePool with no presentation edition.
        pool, is_new = LicensePool.for_foreign_id(
            self._db, license_source, identifier.type, identifier.identifier
        )
        eq_(None, pool.presentation_edition)

        # Calling finalize_license_pool() here won't do much.
        provider_no_presentation_ready.finalize_license_pool(pool)

        # A presentation edition has been created for the LicensePool,
        # but it has no title (in fact it has no data at all), so no
        # Work was created.
        eq_(None, pool.presentation_edition.title)
        eq_(0, self._db.query(Work).count())

        # Here's an Edition for the same book as the LicensePool but
        # from a different data source.
        edition, is_new = Edition.for_foreign_id(
            self._db, data_source, identifier.type, identifier.identifier
        )
        edition.title = self._str

        # Although Edition and LicensePool share an identifier, they
        # are not otherwise related.
        eq_(None, pool.presentation_edition.title)

        # finalize_license_pool() will create a Work and update the
        # LicensePool's presentation edition, based on the brand-new
        # Edition.
        provider_no_presentation_ready.finalize_license_pool(pool)
        work = pool.work
        eq_(edition.title, pool.presentation_edition.title)
        eq_(False, work.presentation_ready)

        # If the provider is configured to do so, finalize_license_pool()
        # will also set the Work as presentation-ready.
        provider_presentation_ready.finalize_license_pool(pool)
        eq_(True, work.presentation_ready)

    def test_process_batch(self):
        provider = self._provider()

        # Here are an Edition and a LicensePool for the same identifier but
        # from different data sources. We would expect this to happen
        # when talking to the open-access content server.
        edition = self._edition(data_source_name=DataSource.OA_CONTENT_SERVER)
        identifier = edition.primary_identifier

        license_source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        pool, is_new = LicensePool.for_foreign_id(
            self._db, license_source, identifier.type, identifier.identifier
        )
        eq_(None, pool.work)

        # Here's a second identifier that's doomed to failure.
        identifier = self._identifier()
        messages_by_id = {identifier.urn : CoverageFailure(identifier, "201: try again later")}

        provider.queue_import_results([edition], [pool], [], messages_by_id)

        fake_batch = [object()]
        success, failure = provider.process_batch(fake_batch)

        # The batch was provided to lookup_and_import_batch.
        eq_([fake_batch], provider.batches)

        # The Edition and LicensePool have been knitted together into
        # a Work.
        eq_(edition, pool.presentation_edition)
        assert pool.work != None

        # The license pool was finalized.
        eq_([pool], provider.finalized)

        # The failure stayed a CoverageFailure object.
        eq_(identifier, failure.obj)
        eq_(True, failure.transient)

    def test_process_batch_success_even_if_no_licensepool_created(self):
        provider = self._provider()
        edition, pool = self._edition(with_license_pool=True)
        provider.queue_import_results([edition], [], [], {})
        fake_batch = [object()]
        [success] = provider.process_batch(fake_batch)
        eq_(edition.primary_identifier, success)

    def test_process_batch_fails_if_licensepool_created_but_no_edition(self):
        provider = self._provider()
        edition, pool = self._edition(with_license_pool=True)
        provider.queue_import_results([], [pool], [], {})
        fake_batch = [object()]
        [failure] = provider.process_batch(fake_batch)
        eq_('OPDS import operation imported LicensePool, but no Edition.',
            failure.exception)
        eq_(pool.identifier, failure.obj)
        eq_(provider.output_source, failure.data_source)


class TestMetadataWranglerCoverageProvider(DatabaseTest):

    def create_provider(self, **kwargs):
        with temp_config() as config:
            config[Configuration.INTEGRATIONS][Configuration.METADATA_WRANGLER_INTEGRATION] = {
                Configuration.URL : "http://url.gov"
            }
            return MetadataWranglerCoverageProvider(self._db, **kwargs)

    def setup(self):
        super(TestMetadataWranglerCoverageProvider, self).setup()
        self.source = DataSource.lookup(self._db, DataSource.METADATA_WRANGLER)
        self.provider = self.create_provider()

    def test_create_identifier_mapping(self):
        # Most identifiers map to themselves.
        overdrive = self._identifier(Identifier.OVERDRIVE_ID)

        # But Axis 360 identifiers map to equivalent ISBNs.
        axis = self._identifier(Identifier.AXIS_360_ID)
        isbn = self._identifier(Identifier.ISBN)

        who_says = DataSource.lookup(self._db, DataSource.AXIS_360)

        axis.equivalent_to(who_says, isbn, 1)

        mapping = self.provider.create_identifier_mapping([overdrive, axis])
        eq_(overdrive, mapping[overdrive])
        eq_(axis, mapping[isbn])

    def test_items_that_need_coverage(self):
        source = DataSource.lookup(self._db, DataSource.METADATA_WRANGLER)
        other_source = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        
        # An item that hasn't been covered by the provider yet
        cr = self._coverage_record(self._edition(), other_source)
        
        # An item that has been covered by the reaper operation already
        reaper_cr = self._coverage_record(
            self._edition(), source, operation=CoverageRecord.REAP_OPERATION
        )
        
        # An item that has been covered by the reaper operation, but has
        # had its license repurchased.
        relicensed_edition, relicensed_licensepool = self._edition(with_license_pool=True)
        relicensed_coverage_record = self._coverage_record(
            relicensed_edition, source, operation=CoverageRecord.REAP_OPERATION
        )
        relicensed_licensepool.update_availability(1, 0, 0, 0)

        items = self.provider.items_that_need_coverage().all()
        # Provider ignores anything that has been reaped and doesn't have
        # licenses.
        assert reaper_cr.identifier not in items
        # But it picks up anything that hasn't been covered at all and anything
        # that's been licensed anew even if its already been reaped.
        eq_(2, len(items))
        assert relicensed_licensepool.identifier in items
        assert cr.identifier in items
        # The Wrangler Reaper coverage record is removed from the db
        # when it's committed.
        assert relicensed_coverage_record in relicensed_licensepool.identifier.coverage_records
        self._db.commit()
        assert relicensed_coverage_record not in relicensed_licensepool.identifier.coverage_records

    def test_items_that_need_coverage_respects_cutoff(self):
        """Verify that this coverage provider respects the cutoff_time
        argument.
        """

        source = DataSource.lookup(self._db, DataSource.METADATA_WRANGLER)
        edition = self._edition()
        cr = self._coverage_record(edition, source, operation='sync')

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
        eq_([edition.primary_identifier], 
            provider_with_cutoff.items_that_need_coverage().all())

    def test_items_that_need_coverage_respects_count_as_covered(self):
        # Here's a coverage record with a transient failure.
        identifier = self._identifier()
        cr = self._coverage_record(
            identifier, self.provider.output_source, 
            operation=self.provider.operation,
            status=CoverageRecord.TRANSIENT_FAILURE
        )
        
        # Ordinarily, a transient failure does not count as coverage.
        [needs_coverage] = self.provider.items_that_need_coverage().all()
        eq_(needs_coverage, identifier)

        # But if we say that transient failure counts as coverage, it
        # does count.
        eq_([],
            self.provider.items_that_need_coverage(
                count_as_covered=CoverageRecord.TRANSIENT_FAILURE
            ).all()
        )


class TestMetadataWranglerCollectionReaper(DatabaseTest):

    def setup(self):
        super(TestMetadataWranglerCollectionReaper, self).setup()
        self.source = DataSource.lookup(self._db, DataSource.METADATA_WRANGLER)
        with temp_config() as config:
            config[Configuration.INTEGRATIONS][Configuration.METADATA_WRANGLER_INTEGRATION] = {
                Configuration.URL : "http://url.gov"
            }
            self.reaper = MetadataWranglerCollectionReaper(self._db)

    def test_items_that_need_coverage(self):
        """The reaper only returns identifiers with unlicensed license_pools
        that have been synced with the Metadata Wrangler.
        """
        # A Wrangler-synced item that doesn't have any owned licenses
        covered_unlicensed_lp = self._licensepool(None, open_access=False, set_edition_as_presentation=True)
        covered_unlicensed_lp.update_availability(0, 0, 0, 0)
        cr = self._coverage_record(
            covered_unlicensed_lp.presentation_edition, self.source,
            operation=CoverageRecord.SYNC_OPERATION
        )
        # An unsynced item that doesn't have any licenses
        uncovered_unlicensed_lp = self._licensepool(None, open_access=False)
        uncovered_unlicensed_lp.update_availability(0, 0, 0, 0)
        licensed_lp = self._licensepool(None, open_access=False)
        # An open access license pool
        open_access_lp = self._licensepool(None)

        items = self.reaper.items_that_need_coverage().all()
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
        eq_([], self.reaper.items_that_need_coverage().all())

    def test_finalize_batch(self):
        """Metadata Wrangler sync coverage records are deleted from the db
        when the the batch is finalized if the item has been reaped.
        """
        # Create two identifiers that have been either synced or reaped.
        sync_cr = self._coverage_record(
            self._edition(), self.source, operation=CoverageRecord.SYNC_OPERATION
        )
        reaped_cr = self._coverage_record(
            self._edition(), self.source, operation=CoverageRecord.REAP_OPERATION
        )

        # Create coverage records for an Identifier that has been both synced
        # and reaped.
        doubly_covered = self._edition()
        doubly_sync_record = self._coverage_record(
            doubly_covered, self.source, operation=CoverageRecord.SYNC_OPERATION
        )
        doubly_reap_record = self._coverage_record(
            doubly_covered, self.source, operation=CoverageRecord.REAP_OPERATION
        )

        self.reaper.finalize_batch()
        remaining_records = self._db.query(CoverageRecord).all()

        # The syncing record has been deleted from the database
        assert doubly_sync_record not in remaining_records
        eq_([sync_cr, reaped_cr, doubly_reap_record], remaining_records)


class TestContentServerBibliographicCoverageProvider(DatabaseTest):

    def test_script_instantiation(self):
        """Test that RunCoverageProviderScript can instantiate
        the coverage provider.
        """
        script = RunCoverageProviderScript(
            ContentServerBibliographicCoverageProvider, self._db,
            lookup=object()
        )
        assert isinstance(script.provider, 
                          ContentServerBibliographicCoverageProvider)

    def test_only_open_access_books_considered(self):

        lookup = MockSimplifiedOPDSLookup(self._url)        
        provider = ContentServerBibliographicCoverageProvider(
            self._db, lookup=lookup
        )

        # Here's an open-access work.
        w1 = self._work(with_license_pool=True, with_open_access_download=True)

        # Here's a work that's not open-access.
        w2 = self._work(with_license_pool=True, with_open_access_download=False)
        w2.license_pools[0].open_access = False

        # Only the open-access work needs coverage.
        eq_([w1.license_pools[0].identifier],
            provider.items_that_need_coverage().all())


class TestSearchIndexCoverageProvider(DatabaseTest):

    def test_run(self):
        index = DummyExternalSearchIndex()

        # Here's a work.
        work = self._work()
        work.presentation_ready = True

        # Here's a CoverageProvider that can index it.
        provider = SearchIndexCoverageProvider(self._db, "works-index", index)

        # Let's run the provider.
        provider.run()

        # We've got a coverage record.
        [record] = [x for x in work.coverage_records if
                    x.operation == provider.operation_name]

        eq_(record.work, work)
        timestamp = record.timestamp

        # And the work was actually added to the search index.
        eq_([('works', 'work-type', work.id)], index.docs.keys())

        # Running the provider again does nothing -- does not create
        # a new WorkCoverageRecord and does not update the timestamp.
        provider.run()
        [record2] = [x for x in work.coverage_records if
                     x.operation == provider.operation_name]
        eq_(record2, record)
        eq_(timestamp, record2.timestamp)

        # However, if we create a CoverageProvider that updates a
        # different index (e.g. because the index format has changed
        # and we're recreating the search index), we can get a second
        # WorkCoverageRecord for _that_ index.
        index.works_index = 'works-index-2'
        provider2 = SearchIndexCoverageProvider(self._db, "works-index-2", index)
        provider2.run()

        [record3] = [x for x in work.coverage_records if
                     x.operation == provider2.operation_name]

        eq_(record3.work, work)
        assert record3.timestamp > timestamp

