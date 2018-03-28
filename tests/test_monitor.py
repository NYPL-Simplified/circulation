import datetime
import random
from nose.tools import (
    set_trace,
    eq_,
)

from . import (
    DatabaseTest,
    sample_data,
)

from core.model import (
    Annotation,
    Collection,
    CoverageRecord,
    DataSource,
    ExternalIntegration,
    Identifier,
)
from core.opds_import import MockMetadataWranglerOPDSLookup
from core.testing import (
    MockRequestsResponse,
    AlwaysSuccessfulCoverageProvider,
)
from core.util.opds_writer import OPDSFeed

from api.monitor import (
    HoldReaper,
    IdlingAnnotationReaper,
    LoanlikeReaperMonitor,
    LoanReaper,
    MWAuxiliaryMetadataMonitor,
    MWCollectionUpdateMonitor,
)

from api.odl import (
    ODLWithConsolidatedCopiesAPI,
    SharedODLAPI,
)


class InstrumentedMWCollectionUpdateMonitor(MWCollectionUpdateMonitor):
    
    def __init__(self, *args, **kwargs):
        super(InstrumentedMWCollectionUpdateMonitor, self).__init__(*args, **kwargs)
        self.imports = []

    def import_one_feed(self, timestamp, url):
        self.imports.append((timestamp, url))
        return super(InstrumentedMWCollectionUpdateMonitor,
                     self).import_one_feed(timestamp, url)


class TestMWCollectionUpdateMonitor(DatabaseTest):

    def setup(self):
        super(TestMWCollectionUpdateMonitor, self).setup()
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

        self.monitor = InstrumentedMWCollectionUpdateMonitor(
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
        monitor = MWCollectionUpdateMonitor(
            self._db, self.collection, lookup
        )
        timestamp = object()
        response = monitor.get_response(timestamp=timestamp, url=None)
        eq_(200, response.status_code)
        eq_(timestamp, lookup.last_timestamp)
        eq_([], lookup.urls)

        # If you pass in a URL, the timestamp is ignored and
        # the URL is passed into _get().
        lookup = Mock()
        monitor = MWCollectionUpdateMonitor(
            self._db, self.collection, lookup
        )
        response = monitor.get_response(timestamp=None, url='http://now used/')
        eq_(200, response.status_code)
        eq_(None, lookup.last_timestamp)
        eq_(['http://now used/'], lookup.urls)


class TestMWAuxiliaryMetadataMonitor(DatabaseTest):

    def setup(self):
        super(TestMWAuxiliaryMetadataMonitor, self).setup()

        self._external_integration(
            ExternalIntegration.METADATA_WRANGLER,
            ExternalIntegration.METADATA_GOAL,
            username=u'abc', password=u'def', url=self._url
        )

        self.collection = self._collection(
            protocol=ExternalIntegration.OVERDRIVE, external_account_id=u'lib'
        )

        self.lookup = MockMetadataWranglerOPDSLookup.from_config(
            self._db, self.collection
        )

        provider = AlwaysSuccessfulCoverageProvider(self._db)

        self.monitor = MWAuxiliaryMetadataMonitor(
            self._db, self.collection, lookup=self.lookup, provider=provider
        )

    def prep_feed_identifiers(self):
        ignored = self._identifier()

        # Create an Overdrive ID to match the one in the feed.
        overdrive = self._identifier(
            identifier_type=Identifier.OVERDRIVE_ID,
            foreign_id=u'4981c34f-d518-48ff-9659-2601b2b9bdc1'
        )

        # Create an ISBN to match the one in the feed.
        isbn = self._identifier(
            identifier_type=Identifier.ISBN, foreign_id=u'9781602835740'
        )

        # Create a Axis 360 ID equivalent to the other ISBN in the feed.
        axis_360 = self._identifier(
            identifier_type=Identifier.AXIS_360_ID, foreign_id=u'fake'
        )
        axis_360_isbn = self._identifier(
            identifier_type=Identifier.ISBN, foreign_id=u'9781569478295'
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
        eq_(sorted([overdrive, axis_360, isbn]), sorted(identifiers))

        eq_(['http://next-link'], next_links)

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

        self.monitor.run_once(None, None)

        # Only the identifier with a work has been given coverage.
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
            eq_(None, record)


class TestLoanlikeReaperMonitor(DatabaseTest):
    """Tests the loan and hold reapers."""

    def test_source_of_truth_protocols(self):
        """Verify that well-known source of truth protocols
        will be exempt from the reaper.
        """
        for i in (
                ODLWithConsolidatedCopiesAPI.NAME,
                SharedODLAPI.NAME,
                ExternalIntegration.OPDS_FOR_DISTRIBUTORS,
        ):
            assert i in LoanlikeReaperMonitor.SOURCE_OF_TRUTH_PROTOCOLS


    def test_reaping(self):
        # This patron stopped using the circulation manager a long time
        # ago.
        inactive_patron = self._patron()

        # This patron is still using the circulation manager.
        current_patron = self._patron()

        # We're going to give these patrons some loans and holds.
        edition, open_access = self._edition(
            with_license_pool=True, with_open_access_download=True)

        not_open_access_1 = self._licensepool(edition,
            open_access=False, data_source_name=DataSource.OVERDRIVE)
        not_open_access_2 = self._licensepool(edition,
            open_access=False, data_source_name=DataSource.BIBLIOTHECA)
        not_open_access_3 = self._licensepool(edition,
            open_access=False, data_source_name=DataSource.AXIS_360)
        not_open_access_4 = self._licensepool(edition,
            open_access=False, data_source_name=DataSource.ONECLICK)

        # Here's a collection that is the source of truth for its
        # loans and holds, rather than mirroring loan and hold information
        # from some remote source.
        sot_collection = self._collection(
            "Source of Truth",
            protocol=random.choice(LoanReaper.SOURCE_OF_TRUTH_PROTOCOLS)
        )

        edition2 = self._edition(with_license_pool=False)

        sot_lp1 = self._licensepool(
            edition2, open_access=False,
            data_source_name=DataSource.OVERDRIVE,
            collection=sot_collection
        )

        sot_lp2 = self._licensepool(
            edition2, open_access=False,
            data_source_name=DataSource.BIBLIOTHECA,
            collection=sot_collection
        )

        now = datetime.datetime.utcnow()
        a_long_time_ago = now - datetime.timedelta(days=1000)
        not_very_long_ago = now - datetime.timedelta(days=60)
        even_longer = now - datetime.timedelta(days=2000)
        the_future = now + datetime.timedelta(days=1)

        # This loan has expired.
        not_open_access_1.loan_to(
            inactive_patron, start=even_longer, end=a_long_time_ago
        )

        # This hold expired without ever becoming a loan (that we saw).
        not_open_access_2.on_hold_to(
            inactive_patron,
            start=even_longer,
            end=a_long_time_ago
        )

        # This hold has no end date and is older than a year.
        not_open_access_3.on_hold_to(
            inactive_patron, start=a_long_time_ago, end=None,
        )

        # This loan has no end date and is older than 90 days.
        not_open_access_4.loan_to(
            inactive_patron, start=a_long_time_ago, end=None,
        )

        # This loan has no end date, but it's for an open-access work.
        open_access_loan, ignore = open_access.loan_to(
            inactive_patron, start=a_long_time_ago, end=None,
        )

        # This loan has not expired yet.
        not_open_access_1.loan_to(
            current_patron, start=now, end=the_future
        )

        # This hold has not expired yet.
        not_open_access_2.on_hold_to(
            current_patron, start=now, end=the_future
        )

        # This loan has no end date but is pretty recent.
        not_open_access_3.loan_to(
            current_patron, start=not_very_long_ago, end=None
        )

        # This hold has no end date but is pretty recent.
        not_open_access_4.on_hold_to(
            current_patron, start=not_very_long_ago, end=None
        )

        # Reapers will not touch loans or holds from the
        # source-of-truth collection, even ones that have 'obviously'
        # expired.
        sot_loan, ignore = sot_lp1.loan_to(
            inactive_patron, start=a_long_time_ago, end=a_long_time_ago
        )

        sot_hold, ignore = sot_lp2.on_hold_to(
            inactive_patron, start=a_long_time_ago, end=a_long_time_ago
        )

        eq_(4, len(inactive_patron.loans))
        eq_(3, len(inactive_patron.holds))

        eq_(2, len(current_patron.loans))
        eq_(2, len(current_patron.holds))

        # Now we fire up the loan reaper.
        monitor = LoanReaper(self._db)
        monitor.run()

        # All of the inactive patron's loans have been reaped,
        # except for the loans for which the circulation manager is the
        # source of truth (the SOT loan and the open-access loan),
        # which will never be reaped.
        #
        # Holds are unaffected.
        eq_(set([open_access_loan, sot_loan]), set(inactive_patron.loans))
        eq_(3, len(inactive_patron.holds))

        # The active patron's loans and holds are unaffected, either
        # because they have not expired or because they have no known
        # expiration date and were created relatively recently.
        eq_(2, len(current_patron.loans))
        eq_(2, len(current_patron.holds))

        # Now fire up the hold reaper.
        monitor = HoldReaper(self._db)
        monitor.run()

        # All of the inactive patron's holds have been reaped,
        # except for the one from the source-of-truth collection.
        # The active patron is unaffected.
        eq_([sot_hold], inactive_patron.holds)
        eq_(2, len(current_patron.holds))


class TestIdlingAnnotationReaper(DatabaseTest):

    def test_where_clause(self):

        # Two books.
        ignore, lp1 = self._edition(with_license_pool=True)
        ignore, lp2 = self._edition(with_license_pool=True)

        # Two patrons who sync their annotations.
        p1 = self._patron()
        p2 = self._patron()
        for p in [p1, p2]:
            p.synchronize_annotations = True
        now = datetime.datetime.utcnow()
        not_that_old = now - datetime.timedelta(days=59)
        very_old = now - datetime.timedelta(days=61)

        def _annotation(patron, pool, content, motivation=Annotation.IDLING,
                        timestamp=very_old):
            annotation, ignore = Annotation.get_one_or_create(
                self._db,
                patron=patron,
                identifier=pool.identifier,
                motivation=motivation,
            )
            annotation.timestamp = timestamp
            annotation.content = content
            return annotation

        # The first patron will not be affected by the
        # reaper. Although their annotations are very old, they have
        # an active loan for one book and a hold on the other.
        loan = lp1.loan_to(p1)
        old_loan = _annotation(p1, lp1, "old loan")

        hold = lp2.on_hold_to(p1)
        old_hold = _annotation(p1, lp2, "old hold")

        # The second patron has a very old annotation for the first
        # book. This is the only annotation that will be affected by
        # the reaper.
        reapable = _annotation(p2, lp1, "abandoned")

        # The second patron also has a very old non-idling annotation
        # for the first book, which will not be reaped because only
        # idling annotations are reaped.
        not_idling = _annotation(
            p2, lp1, "not idling", motivation="some other motivation"
        )

        # The second patron has a non-old idling annotation for the
        # second book, which will not be reaped (even though there is
        # no active loan or hold) because it's not old enough.
        new_idling = _annotation(
            p2, lp2, "recent", timestamp=not_that_old
        )
        reaper = IdlingAnnotationReaper(self._db)
        qu = self._db.query(Annotation).filter(reaper.where_clause)
        eq_([reapable], qu.all())
