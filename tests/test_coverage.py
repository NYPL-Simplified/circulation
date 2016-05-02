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
        reaper_source = DataSource.lookup(
            self._db, DataSource.METADATA_WRANGLER_COLLECTION
        )
        other_source = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        cr = self._coverage_record(self._edition(), other_source)
        reaper_cr = self._coverage_record(self._edition(), reaper_source)
        relicensed, relicensed_lp = self._edition(with_license_pool=True)
        self._coverage_record(relicensed, reaper_source)
        relicensed_lp.update_availability(1, 0, 0, 0)

        with temp_config() as config:
            config[Configuration.INTEGRATIONS][Configuration.METADATA_WRANGLER_INTEGRATION] = {
                Configuration.URL : "http://url.gov"
            }
            provider = MetadataWranglerCoverageProvider(self._db)
        items = provider.items_that_need_coverage.all()
        assert reaper_cr.identifier not in items
        eq_(2, len(items))
        eq_([relicensed_lp.identifier, cr.identifier], sorted(items))
        # The Wrangler Reaper coverage record has been removed from the
        # relicensed identifier.
        eq_([], relicensed_lp.identifier.coverage_records)


class TestMetadataWranglerCollectionReaper(DatabaseTest):

    def setup(self):
        super(TestMetadataWranglerCollectionReaper, self).setup()
        self.wrangler_source = DataSource.lookup(
            self._db, DataSource.METADATA_WRANGLER
        )

        with temp_config() as config:
            config[Configuration.INTEGRATIONS][Configuration.METADATA_WRANGLER_INTEGRATION] = {
                Configuration.URL : "http://url.gov"
            }
            self.reaper = MetadataWranglerCollectionReaper(self._db)

    def test_items_that_need_coverage(self):
        covered_unlicensed_lp = self._licensepool(None, open_access=False)
        covered_unlicensed_lp.update_availability(0, 0, 0, 0)
        self._coverage_record(covered_unlicensed_lp.edition, self.wrangler_source)

        # Identifiers that haven't been looked up on the Metadata Wrangler
        # are ignored, even if they don't have licenses.
        uncovered_unlicensed_lp = self._licensepool(None, open_access=False)
        uncovered_unlicensed_lp.update_availability(0, 0, 0, 0)
        # Identifiers that have owned licenses are ignored.
        licensed_lp = self._licensepool(None, open_access=False)
        # Identifiers that represent open access identifiers are ignored.
        open_access_lp = self._licensepool(None)

        items = self.reaper.items_that_need_coverage.all()
        eq_(1, len(items))
        assert licensed_lp.identifier not in items
        assert open_access_lp.identifier not in items
        assert uncovered_unlicensed_lp.identifier not in items
        eq_([covered_unlicensed_lp.identifier], items)

    def test_finalize_batch(self):
        reaper_source = DataSource.lookup(self._db, DataSource.METADATA_WRANGLER_COLLECTION)
        cr_wrangler = self._coverage_record(self._edition(), self.wrangler_source)
        cr_reaper = self._coverage_record(self._edition(), reaper_source)

        # Create coverage records for an Identifier that is double-covered.
        doubly_covered = self._edition()
        doubly_wrangler = self._coverage_record(doubly_covered, self.wrangler_source)
        doubly_reaper = self._coverage_record(doubly_covered, reaper_source)

        self.reaper.finalize_batch()
        remaining_records = self._db.query(CoverageRecord).all()
        assert doubly_wrangler not in remaining_records
        eq_([cr_wrangler, cr_reaper, doubly_reaper], remaining_records)
