"""Tests of the Monitors and CoverageProviders associated with the metadata
wrangler.
"""

import datetime
import feedparser
import pytest

from core.config import (
    CannotLoadConfiguration,
    Configuration,
    temp_config,
)
from core.coverage import (
    CoverageFailure,
)
from core.model import (
    CoverageRecord,
    DataSource,
    ExternalIntegration,
    Hyperlink,
    Identifier,
    LicensePool,
    Timestamp,
)
from core.opds_import import MockMetadataWranglerOPDSLookup
from core.testing import (
    MockRequestsResponse,
    AlwaysSuccessfulCoverageProvider,
)
from core.util.datetime_helpers import (
    datetime_utc,
    utc_now,
)
from core.util.http import BadResponseException
from core.util.opds_writer import OPDSFeed

from api.metadata_wrangler import (
    BaseMetadataWranglerCoverageProvider,
    MetadataUploadCoverageProvider,
    MetadataWranglerCollectionReaper,
    MetadataWranglerCollectionRegistrar,
    MWAuxiliaryMetadataMonitor,
    MWCollectionUpdateMonitor,
)
from api.testing import MonitorTest
from core.testing import DatabaseTest
from . import sample_data

class InstrumentedMWCollectionUpdateMonitor(MWCollectionUpdateMonitor):

    def __init__(self, *args, **kwargs):
        super(InstrumentedMWCollectionUpdateMonitor, self).__init__(*args, **kwargs)
        self.imports = []

    def import_one_feed(self, timestamp, url):
        self.imports.append((timestamp, url))
        return super(InstrumentedMWCollectionUpdateMonitor,
                     self).import_one_feed(timestamp, url)

class TestMWCollectionUpdateMonitor(MonitorTest):

    def setup_method(self):
        super(TestMWCollectionUpdateMonitor, self).setup_method()
        self._external_integration(
            ExternalIntegration.METADATA_WRANGLER,
            ExternalIntegration.METADATA_GOAL,
            username='abc', password='def', url=self._url
        )

        self.collection = self._collection(
            protocol=ExternalIntegration.BIBLIOTHECA, external_account_id='lib'
        )

        self.lookup = MockMetadataWranglerOPDSLookup.from_config(
            self._db, self.collection
        )

        self.monitor = InstrumentedMWCollectionUpdateMonitor(
            self._db, self.collection, self.lookup
        )

    def test_monitor_requires_authentication(self):
        class Mock(object):
            authenticated = False
        self.monitor.lookup = Mock()
        with pytest.raises(Exception) as excinfo:
            self.monitor.run_once(self.ts)
        assert "no authentication credentials" in str(excinfo.value)

    def test_import_one_feed(self):
        data = sample_data('metadata_updates_response.opds', 'opds')
        self.lookup.queue_response(
            200, {'content-type' : OPDSFeed.ACQUISITION_FEED_TYPE}, data
        )

        next_links, editions, timestamp = self.monitor.import_one_feed(
            None, None
        )

        # The 'next' links found in the OPDS feed are returned.
        assert ['http://next-link/'] == next_links

        # Insofar as is possible, all <entry> tags are converted into
        # Editions.
        assert ['9781594632556'] == [x.primary_identifier.identifier
                                 for x in editions]

        # The earliest time found in the OPDS feed is returned as a
        # candidate for the Monitor's timestamp.
        assert datetime_utc(2016, 9, 20, 19, 37, 2) == timestamp

    def test_empty_feed_stops_import(self):
        # We don't follow the 'next' link of an empty feed.
        data = sample_data('metadata_updates_empty_response.opds', 'opds')
        self.lookup.queue_response(
            200, {'content-type' : OPDSFeed.ACQUISITION_FEED_TYPE}, data
        )

        new_timestamp = self.monitor.run()

        # We could have followed the 'next' link, but we chose not to.
        assert [(None, None)] == self.monitor.imports
        assert 1 == len(self.lookup.requests)

        # Since there were no <entry> tags, the timestamp's finish
        # date was set to the <updated> date of the feed itself, minus
        # one day (to avoid race conditions).
        assert (datetime_utc(2016, 9, 19, 19, 37, 10) ==
            self.monitor.timestamp().finish)

    def test_run_once(self):
        # Setup authentication and Metadata Wrangler details.
        lp = self._licensepool(
            None, data_source_name=DataSource.BIBLIOTHECA,
            collection=self.collection
        )
        lp.identifier.type = Identifier.BIBLIOTHECA_ID
        isbn = Identifier.parse_urn(self._db, 'urn:isbn:9781594632556')[0]
        lp.identifier.equivalent_to(
            DataSource.lookup(self._db, DataSource.BIBLIOTHECA), isbn, 1
        )
        assert [] == lp.identifier.links
        assert [] == lp.identifier.measurements

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

        timestamp = self.ts
        new_timestamp = self.monitor.run_once(timestamp)

        # We have a new value to use for the Monitor's timestamp -- the
        # earliest date seen in the last OPDS feed that contained
        # any entries.
        assert datetime_utc(2016, 9, 20, 19, 37, 2) == new_timestamp.finish
        assert "Editions processed: 1" == new_timestamp.achievements

        # Normally run_once() doesn't update the monitor's timestamp,
        # but this implementation does, so that work isn't redone if
        # run_once() crashes or the monitor is killed.
        assert new_timestamp.finish == self.monitor.timestamp().finish

        # The original Identifier has information from the
        # mock Metadata Wrangler.
        mw_source = DataSource.lookup(self._db, DataSource.METADATA_WRANGLER)
        assert 3 == len(lp.identifier.links)
        [quality] = lp.identifier.measurements
        assert mw_source == quality.data_source

        # Check the URLs we processed.
        url1, url2 = [x[0] for x in self.lookup.requests]

        # The first URL processed was the default one for the
        # MetadataWranglerOPDSLookup.
        assert self.lookup.get_collection_url(self.lookup.UPDATES_ENDPOINT) == url1

        # The second URL processed was whatever we saw in the 'next' link.
        assert "http://next-link/" == url2

        # Since that URL didn't contain any new imports, we didn't process
        # its 'next' link, http://another-next-link/.

    def test_no_changes_means_no_timestamp_update(self):
        before = utc_now()
        self.monitor.timestamp().finish = before

        # We're going to ask the metadata wrangler for updates, but
        # there will be none -- not even a feed-level update
        data = sample_data('metadata_updates_empty_response_no_feed_timestamp.opds', 'opds')
        self.lookup.queue_response(
            200, {'content-type' : OPDSFeed.ACQUISITION_FEED_TYPE}, data
        )
        new_timestamp = self.monitor.run_once(self.ts)

        # run_once() returned a TimestampData referencing the original
        # timestamp, and the Timestamp object was not updated.
        assert before == new_timestamp.finish
        assert before == self.monitor.timestamp().finish

        # If timestamp.finish is None before the update is run, and
        # there are no updates, the timestamp will be set
        # to None.
        self.monitor.timestamp().finish = None
        self.lookup.queue_response(
            200, {'content-type' : OPDSFeed.ACQUISITION_FEED_TYPE}, data
        )
        new_timestamp = self.monitor.run_once(self.ts)
        assert Timestamp.CLEAR_VALUE == new_timestamp.finish

    def test_no_import_loop(self):
        # We stop processing a feed's 'next' link if it links to a URL we've
        # already seen.

        data = sample_data('metadata_updates_response.opds', 'opds')
        self.lookup.queue_response(
            200, {'content-type' : OPDSFeed.ACQUISITION_FEED_TYPE}, data
        )
        data = data.replace(b"http://next-link/", b"http://different-link/")
        self.lookup.queue_response(
            200, {'content-type' : OPDSFeed.ACQUISITION_FEED_TYPE}, data
        )

        # This introduces a loop.
        data = data.replace(b"http://next-link/", b"http://next-link/")
        self.lookup.queue_response(
            200, {'content-type' : OPDSFeed.ACQUISITION_FEED_TYPE}, data
        )
        new_timestamp = self.monitor.run_once(self.ts)

        # Even though all these pages had the same content, we kept
        # processing them until we encountered a 'next' link we had
        # seen before; then we stopped.
        first, second, third = self.monitor.imports
        assert (None, None) == first
        assert (None, 'http://next-link/') == second
        assert (None, 'http://different-link/') == third

        assert datetime_utc(2016, 9, 20, 19, 37, 2) == new_timestamp.finish

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
        monitor = MWCollectionUpdateMonitor(
            self._db, self.collection, lookup
        )
        timestamp = object()
        response = monitor.get_response(timestamp=timestamp, url=None)
        assert 200 == response.status_code
        assert timestamp == lookup.last_timestamp
        assert [] == lookup.urls

        # If you pass in a URL, the timestamp is ignored and
        # the URL is passed into _get().
        lookup = Mock()
        monitor = MWCollectionUpdateMonitor(
            self._db, self.collection, lookup
        )
        response = monitor.get_response(timestamp=None, url='http://now used/')
        assert 200 == response.status_code
        assert None == lookup.last_timestamp
        assert ['http://now used/'] == lookup.urls


class TestMWAuxiliaryMetadataMonitor(MonitorTest):

    def setup_method(self):
        super(TestMWAuxiliaryMetadataMonitor, self).setup_method()

        self._external_integration(
            ExternalIntegration.METADATA_WRANGLER,
            ExternalIntegration.METADATA_GOAL,
            username='abc', password='def', url=self._url
        )

        self.collection = self._collection(
            protocol=ExternalIntegration.OVERDRIVE, external_account_id='lib'
        )

        self.lookup = MockMetadataWranglerOPDSLookup.from_config(
            self._db, self.collection
        )

        provider = AlwaysSuccessfulCoverageProvider(self._db)

        self.monitor = MWAuxiliaryMetadataMonitor(
            self._db, self.collection, lookup=self.lookup, provider=provider
        )

    def test_monitor_requires_authentication(self):
        class Mock(object):
            authenticated = False
        self.monitor.lookup = Mock()
        with pytest.raises(Exception) as excinfo:
            self.monitor.run_once(self.ts)
        assert "no authentication credentials" in str(excinfo.value)

    def prep_feed_identifiers(self):
        ignored = self._identifier()

        # Create an Overdrive ID to match the one in the feed.
        overdrive = self._identifier(
            identifier_type=Identifier.OVERDRIVE_ID,
            foreign_id='4981c34f-d518-48ff-9659-2601b2b9bdc1'
        )

        # Create an ISBN to match the one in the feed.
        isbn = self._identifier(
            identifier_type=Identifier.ISBN, foreign_id='9781602835740'
        )

        # Create a Axis 360 ID equivalent to the other ISBN in the feed.
        axis_360 = self._identifier(
            identifier_type=Identifier.AXIS_360_ID, foreign_id='fake'
        )
        axis_360_isbn = self._identifier(
            identifier_type=Identifier.ISBN, foreign_id='9781569478295'
        )
        axis_source = DataSource.lookup(self._db, DataSource.AXIS_360)
        axis_360.equivalent_to(axis_source, axis_360_isbn, 1)
        self._db.commit()

        # Put all of the identifiers in the collection.
        for identifier in [overdrive, isbn, axis_360]:
            self._edition(
                data_source_name=axis_source.name,
                with_license_pool=True,
                identifier_type=identifier.type,
                identifier_id=identifier.identifier,
                collection=self.collection,
            )

        return overdrive, isbn, axis_360

    def test_get_identifiers(self):
        overdrive, isbn, axis_360 = self.prep_feed_identifiers()
        data = sample_data('metadata_data_needed_response.opds', 'opds')
        self.lookup.queue_response(
            200, {'content-type' : OPDSFeed.ACQUISITION_FEED_TYPE}, data
        )
        identifiers, next_links = self.monitor.get_identifiers()

        # The expected identifiers are returned, including the mapped axis_360
        # identifier.
        assert sorted([overdrive, axis_360, isbn]) == sorted(identifiers)

        assert ['http://next-link'] == next_links

    def test_run_once(self):
        overdrive, isbn, axis_360 = self.prep_feed_identifiers()

        # Give one of the identifiers a full work.
        self._work(presentation_edition=overdrive.primarily_identifies[0])
        # And another identifier a work without entries.
        w = self._work(presentation_edition=isbn.primarily_identifies[0])
        w.simple_opds_entry = w.verbose_opds_entry = None

        # Queue some response feeds.
        feed1 = sample_data('metadata_data_needed_response.opds', 'opds')
        feed2 = sample_data('metadata_data_needed_empty_response.opds', 'opds')
        for feed in [feed1, feed2]:
            self.lookup.queue_response(
                200, {'content-type' : OPDSFeed.ACQUISITION_FEED_TYPE}, feed
            )

        progress = self.monitor.run_once(self.ts)

        # Only the identifier with a work has been given coverage.
        assert "Identifiers processed: 1" == progress.achievements

        # The TimestampData returned by run_once() does not include
        # any timing information -- that will be applied by run().
        assert None == progress.start
        assert None == progress.finish

        record = CoverageRecord.lookup(
            overdrive, self.monitor.provider.data_source,
            operation=self.monitor.provider.operation
        )
        assert record

        for identifier in [axis_360, isbn]:
            record = CoverageRecord.lookup(
                identifier, self.monitor.provider.data_source,
                operation=self.monitor.provider.operation
            )
            assert None == record


class MetadataWranglerCoverageProviderTest(DatabaseTest):

    def create_provider(self, **kwargs):
        lookup = MockMetadataWranglerOPDSLookup.from_config(self._db, self.collection)
        return self.TEST_CLASS(self.collection, lookup, **kwargs)

    def setup_method(self):
        super(MetadataWranglerCoverageProviderTest, self).setup_method()
        self.integration = self._external_integration(
            ExternalIntegration.METADATA_WRANGLER,
            goal=ExternalIntegration.METADATA_GOAL, url=self._url,
            username='abc', password='def'
        )
        self.source = DataSource.lookup(self._db, DataSource.METADATA_WRANGLER)
        self.collection = self._collection(
            protocol=ExternalIntegration.BIBLIOTHECA, external_account_id='lib'
        )
        self.provider = self.create_provider()
        self.lookup_client = self.provider.lookup_client

    def opds_feed_identifiers(self):
        """Creates three Identifiers to use for testing with sample OPDS files."""

        # An identifier directly represented in the OPDS response.
        valid_id = self._identifier(foreign_id='2020110')

        # An identifier mapped to an identifier represented in the OPDS
        # response.
        source = DataSource.lookup(self._db, DataSource.AXIS_360)
        mapped_id = self._identifier(
            identifier_type=Identifier.AXIS_360_ID, foreign_id='0015187876'
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

        with pytest.raises(CannotLoadConfiguration) as excinfo:
            self.Mock(self.collection, UnauthenticatedLookupClient())
        assert "Authentication for the Library Simplified Metadata Wrangler " in str(excinfo.value)

    def test_input_identifier_types(self):
        """Verify all the different types of identifiers we send
        to the metadata wrangler.
        """
        assert (
            set([
                Identifier.OVERDRIVE_ID,
                Identifier.BIBLIOTHECA_ID,
                Identifier.AXIS_360_ID,
                Identifier.ONECLICK_ID,
                Identifier.URI,
            ]) ==
            set(BaseMetadataWranglerCoverageProvider.INPUT_IDENTIFIER_TYPES))

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
        assert overdrive == mapping[overdrive]
        assert axis == mapping[isbn_axis]
        assert threem == mapping[isbn_threem]

    def test_coverage_records_for_unhandled_items_include_collection(self):
        # NOTE: This could be made redundant by adding test coverage to
        # CoverageProvider.process_batch_and_handle_results in core.
        data = sample_data('metadata_sync_response.opds', 'opds')
        self.lookup_client.queue_response(
            200, {'content-type': OPDSFeed.ACQUISITION_FEED_TYPE}, data
        )

        identifier = self._identifier()
        self.provider.process_batch_and_handle_results([identifier])
        [record] = identifier.coverage_records
        assert CoverageRecord.TRANSIENT_FAILURE == record.status
        assert self.provider.data_source == record.data_source
        assert self.provider.operation == record.operation
        assert self.provider.collection == record.collection


class TestMetadataWranglerCollectionRegistrar(MetadataWranglerCoverageProviderTest):

    TEST_CLASS = MetadataWranglerCollectionRegistrar

    def test_constants(self):
        # This CoverageProvider runs Identifiers through the 'lookup'
        # endpoint and marks success with CoverageRecords that have
        # the IMPORT_OPERATION operation.
        assert self.provider.lookup_client.lookup == self.provider.api_method
        assert CoverageRecord.IMPORT_OPERATION == self.TEST_CLASS.OPERATION

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
        assert sorted([valid_id, mapped_id]) == sorted(results)

    def test_process_batch_errors(self):
        """When errors are raised during batch processing, an exception is
        raised and no CoverageRecords are created.
        """
        # This happens if the 'server' sends data with the wrong media
        # type.
        self.lookup_client.queue_response(
            200, {'content-type': 'json/application'}, '{ "title": "It broke." }'
        )

        id1 = self._identifier()
        id2 = self._identifier()
        with pytest.raises(BadResponseException) as excinfo:
            self.provider.process_batch([id1, id2])
        assert 'Wrong media type' in str(excinfo.value)
        assert [] == id1.coverage_records
        assert [] == id2.coverage_records

        # Of if the 'server' sends an error response code.
        self.lookup_client.queue_response(
            500, {'content-type': OPDSFeed.ACQUISITION_FEED_TYPE},
            'Internal Server Error'
        )
        with pytest.raises(BadResponseException) as excinfo:
            self.provider.process_batch([id1, id2])
        assert "Got status code 500" in str(excinfo.value)
        assert [] == id1.coverage_records
        assert [] == id2.coverage_records

        # If a message comes back with an unexpected status, a
        # CoverageFailure is created.
        data = sample_data('unknown_message_status_code.opds', 'opds')
        valid_id = self.opds_feed_identifiers()[0]
        self.lookup_client.queue_response(
            200, {'content-type': OPDSFeed.ACQUISITION_FEED_TYPE}, data
        )
        [result] = self.provider.process_batch([valid_id])
        assert True == isinstance(result, CoverageFailure)
        assert valid_id == result.obj
        assert '418: Mad Hatter' == result.exception

        # The OPDS importer didn't know which Collection to associate
        # with this CoverageFailure, but the CoverageProvider does,
        # and it set .collection appropriately.
        assert self.provider.collection == result.collection

    def test_items_that_need_coverage_excludes_unavailable_items(self):
        """A LicensePool that's not actually available doesn't need coverage.
        """
        edition, pool = self._edition(
            with_license_pool=True, collection=self.collection,
            identifier_type=Identifier.BIBLIOTHECA_ID
        )
        pool.licenses_owned = 0
        assert 0 == self.provider.items_that_need_coverage().count()

        # Open-access titles _do_ need coverage.
        pool.open_access = True
        assert [pool.identifier] == self.provider.items_that_need_coverage().all()

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
        assert (
            set(original_coverage_records + [cr]) ==
            set(identifier.coverage_records))

        # ... but then it was relicensed.
        pool.licenses_owned = 10

        assert [identifier] == self.provider.items_that_need_coverage().all()

        # The now-inaccurate REAP record has been removed.
        assert original_coverage_records == identifier.coverage_records

    def test_identifier_covered_in_one_collection_not_covered_in_another(self):
        edition, pool = self._edition(
            with_license_pool=True, collection=self.collection,
            identifier_type=Identifier.BIBLIOTHECA_ID
        )

        identifier = pool.identifier
        other_collection = self._collection()

        # This Identifier needs coverage.
        qu = self.provider.items_that_need_coverage()
        assert [identifier] == qu.all()

        # Adding coverage for an irrelevant collection won't fix that.
        cr = self._coverage_record(
            pool.identifier, self.provider.data_source,
            operation=self.provider.OPERATION,
            collection=other_collection
        )
        assert [identifier] == qu.all()

        # Adding coverage for the relevant collection will.
        cr = self._coverage_record(
            pool.identifier, self.provider.data_source,
            operation=self.provider.OPERATION,
            collection=self.provider.collection
        )
        assert [] == qu.all()

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
        assert [identifier] == self.provider.items_that_need_coverage().all()

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
        assert [] == items

        # But if we send a cutoff_time that's later than the time
        # associated with the coverage record...
        one_hour_from_now = (
            utc_now() + datetime.timedelta(seconds=3600)
        )
        provider_with_cutoff = self.create_provider(
            cutoff_time=one_hour_from_now
        )

        # The book starts showing up in items_that_need_coverage.
        assert ([pool.identifier] ==
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
        assert needs_coverage == pool.identifier

        # But if we say that transient failure counts as coverage, it
        # does count.
        assert ([] ==
            self.provider.items_that_need_coverage(
                count_as_covered=CoverageRecord.TRANSIENT_FAILURE
            ).all())

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
        assert result == identifier
        # The appropriate cover links are transferred.
        identifier_uris = [l.resource.url for l in identifier.links
                           if l.rel in [Hyperlink.IMAGE, Hyperlink.THUMBNAIL_IMAGE]]
        expected = [
            'http://book-covers.nypl.org/Content%20Cafe/ISBN/9781594632556/cover.jpg',
            'http://book-covers.nypl.org/scaled/300/Content%20Cafe/ISBN/9781594632556/cover.jpg'
        ]

        assert sorted(identifier_uris) == sorted(expected)

        # The ISBN doesn't get any information.
        assert isbn.links == []

class MetadataWranglerCollectionManagerTest(DatabaseTest):

    def setup_method(self):
        super(MetadataWranglerCollectionManagerTest, self).setup_method()
        self.integration = self._external_integration(
            ExternalIntegration.METADATA_WRANGLER,
            goal=ExternalIntegration.METADATA_GOAL, url=self._url,
            username='abc', password='def'
        )
        self.source = DataSource.lookup(self._db, DataSource.METADATA_WRANGLER)
        self.collection = self._collection(
            protocol=ExternalIntegration.BIBLIOTHECA, external_account_id='lib'
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
        assert CoverageRecord.REAP_OPERATION == self.TEST_CLASS.OPERATION
        assert self.provider.lookup_client.remove == self.provider.api_method

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
        assert 1 == len(items)

        # Items that are licensed are ignored.
        assert licensed_lp.identifier not in items

        # Items with open access license pools are ignored.
        assert open_access_lp.identifier not in items

        # Items that haven't been synced with the Metadata Wrangler are
        # ignored, even if they don't have licenses.
        assert uncovered_unlicensed_lp.identifier not in items

        # Only synced items without owned licenses are returned.
        assert [covered_unlicensed_lp.identifier] == items

        # Items that had unsuccessful syncs are not returned.
        cr.status = CoverageRecord.TRANSIENT_FAILURE
        assert [] == self.provider.items_that_need_coverage().all()

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
        assert sorted(results) == sorted([valid_id, mapped_id])

    def test_finalize_batch(self):
        # Metadata Wrangler sync coverage records are deleted from the db
        # when the the batch is finalized if the item has been reaped.

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
        assert (sorted([sync_cr, reaped_cr, doubly_reap_record], key=lambda x: x.id) ==
                sorted(remaining_records, key=lambda x: x.id))


class TestMetadataUploadCoverageProvider(DatabaseTest):

    def create_provider(self, **kwargs):
        upload_client = MockMetadataWranglerOPDSLookup.from_config(self._db, self.collection)
        return MetadataUploadCoverageProvider(
            self.collection, upload_client, **kwargs
        )

    def setup_method(self):
        super(TestMetadataUploadCoverageProvider, self).setup_method()
        self.integration = self._external_integration(
            ExternalIntegration.METADATA_WRANGLER,
            goal=ExternalIntegration.METADATA_GOAL, url=self._url,
            username='abc', password='def'
        )
        self.source = DataSource.lookup(self._db, DataSource.METADATA_WRANGLER)
        self.collection = self._collection(
            protocol=ExternalIntegration.BIBLIOTHECA, external_account_id='lib'
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
        assert [] == items

        cr = self._coverage_record(
            pool.identifier, self.provider.data_source,
            operation=self.provider.OPERATION, collection=self.collection
        )

        # With a successful or persistent failure CoverageRecord, it still doesn't show up.
        cr.status = CoverageRecord.SUCCESS
        items = self.provider.items_that_need_coverage().all()
        assert [] == items

        cr.status = CoverageRecord.PERSISTENT_FAILURE
        items = self.provider.items_that_need_coverage().all()
        assert [] == items

        # But with a transient failure record it does.
        cr.status = CoverageRecord.TRANSIENT_FAILURE
        items = self.provider.items_that_need_coverage().all()
        assert [edition.primary_identifier] == items

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
        feed = feedparser.parse(str(metadata_client.metadata_feed))
        urns = [entry.get("id") for entry in feed.get("entries", [])]
        # Only the identifier work a work ends up in the feed.
        assert [pool.identifier.urn] == urns

        # There are two results: the identifier with a work and a CoverageFailure.
        assert 2 == len(results)
        assert pool.identifier in results
        [failure] = [r for r in results if isinstance(r, CoverageFailure)]
        assert no_work == failure.obj
