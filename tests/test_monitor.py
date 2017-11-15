import datetime
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
from core.testing import MockRequestsResponse
from core.util.opds_writer import OPDSFeed

from api.monitor import (
    MetadataWranglerCollectionUpdateMonitor,
)


class InstrumentedMetadataWranglerCollectionUpdateMonitor(MetadataWranglerCollectionUpdateMonitor):
    
    def __init__(self, *args, **kwargs):
        super(InstrumentedMetadataWranglerCollectionUpdateMonitor, self).__init__(*args, **kwargs)
        self.imports = []

    def import_one_feed(self, timestamp, url):
        self.imports.append((timestamp, url))
        return super(InstrumentedMetadataWranglerCollectionUpdateMonitor, 
                     self).import_one_feed(timestamp, url)


class TestMetadataWranglerCollectionUpdateMonitor(DatabaseTest):

    def setup(self):
        super(TestMetadataWranglerCollectionUpdateMonitor, self).setup()
        self._external_integration(
            ExternalIntegration.METADATA_WRANGLER,
            ExternalIntegration.METADATA_GOAL,
            username=u'abc', password=u'def', url=self._url
        )

        self.collection = self._collection(
            protocol=ExternalIntegration.BIBLIOTHECA, external_account_id=u'lib'
        )

        self.lookup = MockMetadataWranglerOPDSLookup.from_config(
            self._db, self.collection
        )

        self.monitor = InstrumentedMetadataWranglerCollectionUpdateMonitor(
            self._db, self.collection, self.lookup
        )

    def test_import_one_feed(self):
        data = sample_data('metadata_updates_response.opds', 'opds')
        self.lookup.queue_response(
            200, {'content-type' : OPDSFeed.ACQUISITION_FEED_TYPE}, data
        )

        next_links, editions, timestamp = self.monitor.import_one_feed(
            None, None
        )

        # The 'next' links found in the OPDS feed are returned.
        eq_([u'http://next-link/'], next_links)

        # Insofar as is possible, all <entry> tags are converted into
        # Editions.
        eq_([u'9781594632556'], [x.primary_identifier.identifier
                                 for x in editions])

        # The earliest time found in the OPDS feed is returned as a
        # candidate for the Monitor's timestamp.
        eq_(datetime.datetime(2016, 9, 20, 19, 37, 2), timestamp)

    def test_empty_feed_stops_import(self):
        """We don't follow the 'next' link of an empty feed."""
        data = sample_data('metadata_updates_empty_response.opds', 'opds')
        self.lookup.queue_response(
            200, {'content-type' : OPDSFeed.ACQUISITION_FEED_TYPE}, data
        )

        new_timestamp = self.monitor.run_once(None, None)

        # We could have followed the 'next' link, but we chose not to.
        eq_([(None, None)], self.monitor.imports)
        eq_(1, len(self.lookup.requests))

        # The timestamp was not updated because nothing was in the feed.
        eq_(None, new_timestamp)
        eq_(None, self.monitor.timestamp().timestamp)

    def test_run_once(self):
        # Setup authentication and Metadata Wrangler details.
        lp = self._licensepool(
            None, data_source_name=DataSource.BIBLIOTHECA, 
            collection=self.collection
        )
        lp.identifier.type = Identifier.BIBLIOTHECA_ID
        isbn = Identifier.parse_urn(self._db, u'urn:isbn:9781594632556')[0]
        lp.identifier.equivalent_to(
            DataSource.lookup(self._db, DataSource.BIBLIOTHECA), isbn, 1
        )
        eq_([], lp.identifier.links)
        eq_([], lp.identifier.measurements)

        # Queue some data to be found.
        responses = (
            'metadata_updates_response.opds',
            'metadata_updates_empty_response.opds',
        )
        for filename in responses:
            data = sample_data(filename, 'opds')
            self.lookup.queue_response(
                200, {'content-type' : OPDSFeed.ACQUISITION_FEED_TYPE}, data
            )

        new_timestamp = self.monitor.run_once(None, None)

        # We have a new value to use for the Monitor's timestamp -- the
        # earliest date seen in the last OPDS feed that contained
        # any entries.
        eq_(datetime.datetime(2016, 9, 20, 19, 37, 2), new_timestamp)

        # Normally run_once() doesn't update the monitor's timestamp,
        # but this implementation does, so that work isn't redone if
        # run_once() crashes or the monitor is killed.
        eq_(new_timestamp, self.monitor.timestamp().timestamp)

        # The original Identifier has information from the
        # mock Metadata Wrangler.
        mw_source = DataSource.lookup(self._db, DataSource.METADATA_WRANGLER)
        eq_(3, len(lp.identifier.links))
        [quality] = lp.identifier.measurements
        eq_(mw_source, quality.data_source)

        # Check the URLs we processed.
        url1, url2 = [x[0] for x in self.lookup.requests]

        # The first URL processed was the default one for the
        # MetadataWranglerOPDSLookup.
        eq_(self.lookup.get_collection_url(self.lookup.UPDATES_ENDPOINT), url1)

        # The second URL processed was whatever we saw in the 'next' link.
        eq_("http://next-link/", url2)

        # Since that URL didn't contain any new imports, we didn't process
        # its 'next' link, http://another-next-link/.

    def test_no_changes_means_no_timestamp_update(self):
        before = datetime.datetime.utcnow()
        self.monitor.timestamp().timestamp = before

        # We're going to ask the metadata wrangler for updates, but
        # there will be none.
        data = sample_data('metadata_updates_empty_response.opds', 'opds')
        self.lookup.queue_response(
            200, {'content-type' : OPDSFeed.ACQUISITION_FEED_TYPE}, data
        )
        new_timestamp = self.monitor.run_once(None, None)

        # run_once() returned the original timestamp, and the 
        # Timestamp object was not updated.
        eq_(before, new_timestamp)
        eq_(before, self.monitor.timestamp().timestamp)

    def test_no_import_loop(self):
        """We stop processing a feed's 'next' link if it links to a URL we've
        already seen.
        """

        data = sample_data('metadata_updates_response.opds', 'opds')
        self.lookup.queue_response(
            200, {'content-type' : OPDSFeed.ACQUISITION_FEED_TYPE}, data
        )
        data = data.replace("http://next-link/", "http://different-link/")
        self.lookup.queue_response(
            200, {'content-type' : OPDSFeed.ACQUISITION_FEED_TYPE}, data
        )

        # This introduces a loop.
        data = data.replace("http://next-link/", "http://next-link/")
        self.lookup.queue_response(
            200, {'content-type' : OPDSFeed.ACQUISITION_FEED_TYPE}, data
        )
        new_timestamp = self.monitor.run_once(None, None)

        # Even though all these pages had the same content, we kept
        # processing them until we encountered a 'next' link we had
        # seen before; then we stopped.
        first, second, third = self.monitor.imports
        eq_((None, None), first)
        eq_((None, u'http://next-link/'), second)
        eq_((None, u'http://different-link/'), third)

        eq_(datetime.datetime(2016, 9, 20, 19, 37, 2), new_timestamp)

    def test_get_response(self):

        class Mock(MockMetadataWranglerOPDSLookup):
            def __init__(self):
                self.last_timestamp = None
                self.urls = []

            def updates(self, timestamp):
                self.last_timestamp = timestamp
                return MockRequestsResponse(
                    200, {"content-type": OPDSFeed.ACQUISITION_FEED_TYPE}
                )

            def _get(self, _url):
                self.urls.append(_url)
                return MockRequestsResponse(
                    200, {"content-type": OPDSFeed.ACQUISITION_FEED_TYPE}
                )

        # If you pass in None for the URL, it passes the timestamp into
        # updates()
        lookup = Mock()
        monitor = MetadataWranglerCollectionUpdateMonitor(
            self._db, self.collection, lookup
        )
        timestamp = object()
        response = monitor.get_response(timestamp, None)
        eq_(200, response.status_code)
        eq_(timestamp, lookup.last_timestamp)
        eq_([], lookup.urls)

        # If you pass in a URL, the timestamp is ignored and
        # the URL is passed into _get().
        lookup = Mock()
        monitor = MetadataWranglerCollectionUpdateMonitor(
            self._db, self.collection, lookup
        )
        response = monitor.get_response(None, 'http://now used/')
        eq_(200, response.status_code)
        eq_(None, lookup.last_timestamp)
        eq_(['http://now used/'], lookup.urls)
