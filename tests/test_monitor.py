from nose.tools import (
    set_trace,
    eq_,
)

from . import (
    DatabaseTest,
    sample_data,
)

from core.external_search import DummyExternalSearchIndex
from core.model import (
    Collection,
    DataSource,
    ExternalIntegration,
    Identifier,
)
from core.opds_import import MockMetadataWranglerOPDSLookup
from core.util.opds_writer import OPDSFeed

from api.monitor import (
    SearchIndexMonitor,
    MetadataWranglerCollectionUpdateMonitor,
)


class TestSearchIndexMonitor(DatabaseTest):

    def test_process_batch(self):
        index = DummyExternalSearchIndex()

        # Here's a work.
        work = self._work()
        work.presentation_ready = True

        # Here's a Monitor that can index it.
        monitor = SearchIndexMonitor(self._db, None, "works-index", index)
        eq_("Search index update (works)", monitor.service_name)

        # The first time we call process_batch we handle the one and
        # only work in the database. The ID of that work is returned for
        # next time.
        eq_(work.id, monitor.process_batch(0))

        # The work was added to the search index.
        eq_([('works', 'work-type', work.id)], index.docs.keys())

        # The next time we call process_batch, no work is done and the
        # result is 0, meaning we're done with every work in the system.
        eq_(0, monitor.process_batch(work.id))

class TestMetadataWranglerCollectionUpdateMonitor(DatabaseTest):

    def test_run_once(self):
        # Setup authentication and Metadata Wrangler details.
        self._external_integration(
            ExternalIntegration.METADATA_WRANGLER,
            username=u'abc', password=u'def', url=self._url
        )

        # Create an identifier and its equivalent to work with the OPDS
        # feed.
        collection = self._collection(
            protocol=Collection.BIBLIOTHECA, external_account_id=u'lib'
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
        lookup = MockMetadataWranglerOPDSLookup(self._db, collection)
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
