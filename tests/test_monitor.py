from nose.tools import (
    set_trace,
    eq_,
)

from . import (
    DatabaseTest,
    sample_data,
)

from core.model import (
    Collection,
    DataSource,
    ExternalIntegration,
    Identifier,
)
from core.opds_import import MockMetadataWranglerOPDSLookup
from core.util.opds_writer import OPDSFeed

from api.monitor import (
    MetadataWranglerCollectionUpdateMonitor,
)


class TestMetadataWranglerCollectionUpdateMonitor(DatabaseTest):

    def test_run_once(self):
        # Setup authentication and Metadata Wrangler details.
        self._external_integration(
            ExternalIntegration.METADATA_WRANGLER,
            ExternalIntegration.METADATA_GOAL,
            username=u'abc', password=u'def', url=self._url
        )

        # Create an identifier and its equivalent to work with the OPDS
        # feed.
        collection = self._collection(
            protocol=ExternalIntegration.BIBLIOTHECA, external_account_id=u'lib'
        )
        lp = self._licensepool(
            None, data_source_name=DataSource.BIBLIOTHECA, collection=collection
        )
        lp.identifier.type = Identifier.BIBLIOTHECA_ID
        isbn = Identifier.parse_urn(self._db, u'urn:isbn:9781594632556')[0]
        lp.identifier.equivalent_to(
            DataSource.lookup(self._db, DataSource.BIBLIOTHECA), isbn, 1
        )
        eq_([], lp.identifier.links)
        eq_([], lp.identifier.measurements)

        # Queue some data to be found.
        data = sample_data('metadata_isbn_response.opds', 'opds')
        lookup = MockMetadataWranglerOPDSLookup.from_config(self._db, collection)
        lookup.queue_response(
            200, {'content-type' : OPDSFeed.ACQUISITION_FEED_TYPE}, data
        )

        monitor = MetadataWranglerCollectionUpdateMonitor(
            self._db, collection, lookup)
        monitor.run_once(None, None)

        # The original Identifier has information from the
        # mock Metadata Wrangler.
        mw_source = DataSource.lookup(self._db, DataSource.METADATA_WRANGLER)
        eq_(3, len(lp.identifier.links))
        [quality] = lp.identifier.measurements
        eq_(mw_source, quality.data_source)
