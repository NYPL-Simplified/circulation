from nose.tools import (
    assert_raises_regexp,
    set_trace,
    eq_,
)

from . import (
    DatabaseTest,
    sample_data,
)

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
)
from core.opds_import import (
    StatusMessage,
)
from core.coverage import (
    CoverageFailure,
)

from core.util.http import BadResponseException

from api.coverage import (
    MetadataWranglerCoverageProvider,
    MetadataWranglerCollectionReaper,
    OPDSImportCoverageProvider,
    MockOPDSImportCoverageProvider,
)

class TestOPDSImportCoverageProvider(DatabaseTest):

    def test_handle_import_messages(self):
        data_source = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        provider = OPDSImportCoverageProvider("name", [], data_source)

        message = StatusMessage(201, "try again later")
        message2 = StatusMessage(404, "we're doomed")
        message3 = StatusMessage(200, "everything's fine")

        identifier = self._identifier()
        identifier2 = self._identifier()
        identifier3 = self._identifier()

        messages_by_id = { identifier.urn: message,
                           identifier2.urn: message2,
                           identifier3.urn: message3,
        }

        [f1, f2] = sorted(list(provider.handle_import_messages(messages_by_id)),
                          key=lambda x: x.exception)
        eq_(identifier, f1.obj)
        eq_("201: try again later", f1.exception)
        eq_(True, f1.transient)

        eq_(identifier2, f2.obj)
        eq_("404: we're doomed", f2.exception)
        eq_(False, f2.transient)

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

    def test_finalize_edition(self):

        provider_no_presentation_ready = self._provider(presentation_ready_on_success=False)
        provider_presentation_ready = self._provider(presentation_ready_on_success=True)
        identifier = self._identifier()
        source = DataSource.lookup(self._db, DataSource.GUTENBERG)

        # Here's an Edition with no LicensePool.
        edition, is_new = Edition.for_foreign_id(
            self._db, source, identifier.type, identifier.identifier
        )
        edition.title = self._str

        # This will effectively do nothing.
        provider_no_presentation_ready.finalize_edition(edition)

        # No Works have been created.
        eq_(0, self._db.query(Work).count())

        # But if there's also a LicensePool...
        pool, is_new = LicensePool.for_foreign_id(
            self._db, source, identifier.type, identifier.identifier
        )

        # finalize_edition() will create a Work.
        provider_no_presentation_ready.finalize_edition(edition)

        work = pool.work
        eq_(work, edition.work)
        eq_(False, work.presentation_ready)

        # If the provider is configured to do so, finalize_edition()
        # will also set the Work as presentation-ready.
        provider_presentation_ready.finalize_edition(edition)
        eq_(True, work.presentation_ready)

    def test_import_batch(self):
        provider = self._provider()

        edition = self._edition()

        identifier = self._identifier()
        messages_by_id = {identifier.urn : StatusMessage(201, "try again later")}

        provider.queue_import_results([edition], messages_by_id)

        fake_batch = [object()]
        success, failure = provider.import_batch(fake_batch)

        # The batch was provided to lookup_and_import_batch.
        eq_([fake_batch], provider.batches)

        # The edition was finalized.
        eq_([success], [e.primary_identifier for e in provider.finalized])

        # The failure was converted to a CoverageFailure object.
        eq_(identifier, failure.obj)
        eq_(True, failure.transient)


class TestMetadataWranglerCoverageProvider(DatabaseTest):

    def setup(self):
        super(TestMetadataWranglerCoverageProvider, self).setup()
        self.source = DataSource.lookup(self._db, DataSource.METADATA_WRANGLER)
        with temp_config() as config:
            config[Configuration.INTEGRATIONS][Configuration.METADATA_WRANGLER_INTEGRATION] = {
                Configuration.URL : "http://url.gov"
            }
            self.provider = MetadataWranglerCoverageProvider(self._db)

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

        items = self.provider.items_that_need_coverage.all()
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
        self._coverage_record(
            covered_unlicensed_lp.presentation_edition, self.source,
            operation=CoverageRecord.SYNC_OPERATION
        )
        # An unsynced item that doesn't have any licenses
        uncovered_unlicensed_lp = self._licensepool(None, open_access=False)
        uncovered_unlicensed_lp.update_availability(0, 0, 0, 0)
        licensed_lp = self._licensepool(None, open_access=False)
        # An open access license pool
        open_access_lp = self._licensepool(None)

        items = self.reaper.items_that_need_coverage.all()
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
