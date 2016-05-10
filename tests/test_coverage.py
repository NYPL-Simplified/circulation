from nose.tools import (
    set_trace,
    eq_,
)

from . import (
    DatabaseTest,
)

from core.config import (
    Configuration,
    temp_config,
)
from core.model import (
    CoverageRecord,
    DataSource,
    Identifier,
)
from core.opds_import import (
    StatusMessage,
)
from core.coverage import (
    CoverageFailure,
)

from api.coverage import (
    MetadataWranglerCoverageProvider,
    MetadataWranglerCollectionReaper,
    OPDSImportCoverageProvider,
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


class TestMetadataWranglerCoverageProvider(DatabaseTest):

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

        with temp_config() as config:
            config[Configuration.INTEGRATIONS][Configuration.METADATA_WRANGLER_INTEGRATION] = {
                Configuration.URL : "http://url.gov"
            }
            provider = MetadataWranglerCoverageProvider(self._db)
        items = provider.items_that_need_coverage.all()
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
