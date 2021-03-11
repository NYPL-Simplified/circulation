# encoding: utf-8
from nose.tools import (
    set_trace,
    eq_,
    assert_raises,
    assert_raises_regexp,

)
from datetime import datetime, timedelta
import json
import os
import pkgutil
import random

from core.testing import DatabaseTest
from . import sample_data

from core.metadata_layer import (
    ReplacementPolicy,
    TimestampData,
)
from core.mock_analytics_provider import MockAnalyticsProvider
from core.model import (
    CirculationEvent,
    Collection,
    Contributor,
    DataSource,
    DeliveryMechanism,
    Edition,
    ExternalIntegration,
    Hold,
    Hyperlink,
    Identifier,
    LicensePool,
    Loan,
    Measurement,
    Resource,
    Representation,
    Subject,
    Timestamp,
    Work,
    WorkCoverageRecord,
    create,
)
from core.util.http import (
    BadResponseException,
)
from core.util.web_publication_manifest import AudiobookManifest
from core.scripts import RunCollectionCoverageProviderScript

from api.authenticator import BasicAuthenticationProvider
from api.circulation import (
    CirculationAPI,
    FulfillmentInfo,
    HoldInfo,
    LoanInfo,
)
from api.circulation_exceptions import *
from api.bibliotheca import (
    BibliothecaCirculationSweep,
    CheckoutResponseParser,
    ErrorParser,
    EventParser,
    MockBibliothecaAPI,
    PatronCirculationParser,
    BibliothecaAPI,
    BibliothecaEventMonitor,
    BibliothecaParser,
    ItemListParser,
    BibliothecaBibliographicCoverageProvider,
)
from api.web_publication_manifest import FindawayManifest


class BibliothecaAPITest(DatabaseTest):

    def setup_method(self):
        super(BibliothecaAPITest,self).setup_method()
        self.collection = MockBibliothecaAPI.mock_collection(self._db)
        self.api = MockBibliothecaAPI(self._db, self.collection)

    base_path = os.path.split(__file__)[0]
    resource_path = os.path.join(base_path, "files", "bibliotheca")

    @classmethod
    def sample_data(self, filename):
        return sample_data(filename, 'bibliotheca')

class TestBibliothecaAPI(BibliothecaAPITest):

    def setup_method(self):
        super(TestBibliothecaAPI, self).setup_method()
        self.collection = MockBibliothecaAPI.mock_collection(self._db)
        self.api = MockBibliothecaAPI(self._db, self.collection)


    def test_external_integration(self):
        eq_(
            self.collection.external_integration,
            self.api.external_integration(object())
        )

    def test__run_self_tests(self):
        """Verify that BibliothecaAPI._run_self_tests() calls the right
        methods.
        """
        class Mock(MockBibliothecaAPI):
            "Mock every method used by BibliothecaAPI._run_self_tests."

            # First we will count the circulation events that happened in the
            # last five minutes.
            def get_events_between(self, start, finish):
                self.get_events_between_called_with = (start, finish)
                return [1,2,3]

            # Then we will count the loans and holds for the default
            # patron.
            def patron_activity(self, patron, pin):
                self.patron_activity_called_with = (patron, pin)
                return ["loan", "hold"]

        # Now let's make sure two Libraries have access to this
        # Collection -- one library with a default patron and one
        # without.
        no_default_patron = self._library()
        self.collection.libraries.append(no_default_patron)

        with_default_patron = self._default_library
        integration = self._external_integration(
            "api.simple_authentication",
            ExternalIntegration.PATRON_AUTH_GOAL,
            libraries=[with_default_patron]
        )
        p = BasicAuthenticationProvider
        integration.setting(p.TEST_IDENTIFIER).value = "username1"
        integration.setting(p.TEST_PASSWORD).value = "password1"

        # Now that everything is set up, run the self-test.
        api = Mock(self._db, self.collection)
        now = datetime.utcnow()
        [no_patron_credential, recent_circulation_events, patron_activity] = sorted(
            api._run_self_tests(self._db), key=lambda x: x.name
        )

        eq_(
            "Acquiring test patron credentials for library %s" % no_default_patron.name,
            no_patron_credential.name
        )
        eq_(False, no_patron_credential.success)
        eq_("Library has no test patron configured.",
            no_patron_credential.exception.message)

        eq_("Asking for circulation events for the last five minutes",
            recent_circulation_events.name)
        eq_(True, recent_circulation_events.success)
        eq_("Found 3 event(s)", recent_circulation_events.result)
        start, end = api.get_events_between_called_with
        eq_(5*60, (end-start).total_seconds())
        assert (end-now).total_seconds() < 2

        eq_("Checking activity for test patron for library %s" % with_default_patron.name,
            patron_activity.name)
        eq_("Found 2 loans/holds", patron_activity.result)
        patron, pin = api.patron_activity_called_with
        eq_("username1", patron.authorization_identifier)
        eq_("password1", pin)

    def test_full_path(self):
        id = self.api.library_id
        eq_("/cirrus/library/%s/foo" % id, self.api.full_path("foo"))
        eq_("/cirrus/library/%s/foo" % id, self.api.full_path("/foo"))
        eq_("/cirrus/library/%s/foo" % id,
            self.api.full_path("/cirrus/library/%s/foo" % id)
        )

    def test_full_url(self):
        id = self.api.library_id
        eq_("http://bibliotheca.test/cirrus/library/%s/foo" % id,
            self.api.full_url("foo"))
        eq_("http://bibliotheca.test/cirrus/library/%s/foo" % id,
            self.api.full_url("/foo"))

    def test_request_signing(self):
        """Confirm a known correct result for the Bibliotheca request signing
        algorithm.
        """
        self.api.queue_response(200)
        response = self.api.request("some_url")
        [request] = self.api.requests
        headers = request[-1]['headers']
        eq_('Fri, 01 Jan 2016 00:00:00 GMT', headers['3mcl-Datetime'])
        eq_('2.0', headers['3mcl-Version'])
        expect = '3MCLAUTH a:HZHNGfn6WVceakGrwXaJQ9zIY0Ai5opGct38j9/bHrE='
        eq_(expect, headers['3mcl-Authorization'])

        # Tweak one of the variables that go into the signature, and
        # the signature changes.
        self.api.library_id = self.api.library_id + "1"
        self.api.queue_response(200)
        response = self.api.request("some_url")
        request = self.api.requests[-1]
        headers = request[-1]['headers']
        assert headers['3mcl-Authorization'] != expect

    def test_replacement_policy(self):
        mock_analytics = object()
        policy = self.api.replacement_policy(self._db, analytics=mock_analytics)
        assert isinstance(policy, ReplacementPolicy)
        eq_(mock_analytics, policy.analytics)

    def test_bibliographic_lookup_request(self):
        self.api.queue_response(200, content="some data")
        response = self.api.bibliographic_lookup_request(["id1", "id2"])
        [request] = self.api.requests
        url = request[1]

        # The request URL is the /items endpoint with the IDs concatenated.
        eq_(url, self.api.full_url("items") + "/id1,id2")

        # The response string is returned directly.
        eq_("some data", response)

    def test_bibliographic_lookup(self):

        class MockItemListParser(object):
            def parse(self, data):
                self.parse_called_with = data
                yield "item1"
                yield "item2"

        class Mock(MockBibliothecaAPI):
            """Mock the functionality used by bibliographic_lookup_request."""
            def __init__(self):
                self.item_list_parser = MockItemListParser()

            def bibliographic_lookup_request(self, identifier_strings):
                self.bibliographic_lookup_request_called_with = identifier_strings
                return "parse me"
        api = Mock()

        identifier = self._identifier()
        # We can pass in a list of identifier strings, a list of
        # Identifier objects, or a single example of each:
        for identifier, identifier_string in (
                ("id1", "id1"),
                (identifier, identifier.identifier)
        ):
            for identifier_list in ([identifier], identifier):
                api.item_list_parser.parse_called_with = None

                results = list(api.bibliographic_lookup(identifier_list))

                # A list of identifier strings is passed into
                # bibliographic_lookup_request().
                eq_(
                    [identifier_string],
                    api.bibliographic_lookup_request_called_with
                )

                # The response content is passed into parse()
                eq_("parse me", api.item_list_parser.parse_called_with)

                # The results of parse() are yielded.
                eq_(["item1", "item2"], results)

    def test_bad_response_raises_exception(self):
        self.api.queue_response(500, content="oops")
        identifier = self._identifier()
        assert_raises_regexp(
            BadResponseException,
            ".*Got status code 500.*",
            self.api.bibliographic_lookup, identifier
        )

    def test_put_request(self):
        """This is a basic test to make sure the method calls line up
        right--there are more thorough tests in the circulation
        manager, which actually uses this functionality.
        """
        self.api.queue_response(200, content="ok, you put something")
        response = self.api.request('checkout', "put this!", method="PUT")

        # The PUT request went through to the correct URL and the right
        # payload was sent.
        [[method, url, args, kwargs]] = self.api.requests
        eq_("PUT", method)
        eq_(self.api.full_url("checkout"), url)
        eq_('put this!', kwargs['data'])

        # The response is what we'd expect.
        eq_(200, response.status_code)
        eq_("ok, you put something", response.content)

    def test_get_events_between_success(self):
        data = self.sample_data("empty_end_date_event.xml")
        self.api.queue_response(200, content=data)
        now = datetime.now()
        an_hour_ago = now - timedelta(minutes=3600)
        response = self.api.get_events_between(an_hour_ago, now)
        [event] = list(response)
        eq_('d5rf89', event[0])

    def test_get_events_between_failure(self):
        self.api.queue_response(500)
        now = datetime.now()
        an_hour_ago = now - timedelta(minutes=3600)
        assert_raises(
            BadResponseException,
            self.api.get_events_between, an_hour_ago, now
        )

    def test_update_availability(self):
        # Test the Bibliotheca implementation of the update_availability
        # method defined by the CirculationAPI interface.

        # Create an analytics integration so we can make sure
        # events are tracked.
        integration, ignore = create(
            self._db, ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol="core.local_analytics_provider",
        )

        # Create a LicensePool that needs updating.
        edition, pool = self._edition(
            identifier_type=Identifier.THREEM_ID,
            data_source_name=DataSource.THREEM,
            with_license_pool=True,
            collection=self.collection
        )

        # We have never checked the circulation information for this
        # LicensePool. Put some random junk in the pool to verify
        # that it gets changed.
        pool.licenses_owned = 10
        pool.licenses_available = 5
        pool.patrons_in_hold_queue = 3
        eq_(None, pool.last_checked)

        # We do have a Work hanging around, but things are about to
        # change for it.
        work, is_new = pool.calculate_work()
        assert any(
            x for x in work.coverage_records
            if x.operation==WorkCoverageRecord.CLASSIFY_OPERATION
        )

        # Prepare availability information.
        data = self.sample_data("item_metadata_single.xml")
        # Change the ID in the test data so it looks like it's talking
        # about the LicensePool we just created.
        data = data.replace("ddf4gr9", pool.identifier.identifier)

        # Update availability using that data.
        self.api.queue_response(200, content=data)
        self.api.update_availability(pool)

        # The availability information has been updated, as has the
        # date the availability information was last checked.
        eq_(1, pool.licenses_owned)
        eq_(1, pool.licenses_available)
        eq_(0, pool.patrons_in_hold_queue)

        circulation_events = self._db.query(CirculationEvent).join(LicensePool).filter(LicensePool.id==pool.id)
        eq_(3, circulation_events.count())
        types = [e.type for e in circulation_events]
        eq_(sorted([CirculationEvent.DISTRIBUTOR_LICENSE_REMOVE,
                    CirculationEvent.DISTRIBUTOR_CHECKOUT,
                    CirculationEvent.DISTRIBUTOR_HOLD_RELEASE]),
            sorted(types))

        old_last_checked = pool.last_checked
        assert old_last_checked is not None

        # The work's CLASSIFY_OPERATION coverage record has been
        # removed. In the near future its coverage will be
        # recalculated to accommodate the new metadata.
        assert any(
            x for x in work.coverage_records
            if x.operation==WorkCoverageRecord.CLASSIFY_OPERATION
        )

        # Now let's try update_availability again, with a file that
        # makes it look like the book has been removed from the
        # collection.
        data = self.sample_data("empty_item_bibliographic.xml")
        self.api.queue_response(200, content=data)

        self.api.update_availability(pool)

        eq_(0, pool.licenses_owned)
        eq_(0, pool.licenses_available)
        eq_(0, pool.patrons_in_hold_queue)

        assert pool.last_checked is not old_last_checked

        circulation_events = self._db.query(CirculationEvent).join(LicensePool).filter(LicensePool.id==pool.id)
        eq_(5, circulation_events.count())

    def test_sync_bookshelf(self):
        patron = self._patron()
        circulation = CirculationAPI(self._db, self._default_library, api_map={
            self.collection.protocol : MockBibliothecaAPI
        })

        api = circulation.api_for_collection[self.collection.id]
        api.queue_response(200, content=self.sample_data("checkouts.xml"))
        circulation.sync_bookshelf(patron, "dummy pin")

        # The patron should have two loans and two holds.
        l1, l2 = patron.loans
        h1, h2 = patron.holds

        eq_(datetime(2015, 3, 20, 18, 50, 22), l1.start)
        eq_(datetime(2015, 4, 10, 18, 50, 22), l1.end)

        eq_(datetime(2015, 3, 13, 13, 38, 19), l2.start)
        eq_(datetime(2015, 4, 3, 13, 38, 19), l2.end)

        # The patron is fourth in line. The end date is an estimate
        # of when the hold will be available to check out.
        eq_(datetime(2015, 3, 24, 15, 6, 56), h1.start)
        eq_(datetime(2015, 3, 24, 15, 7, 51), h1.end)
        eq_(4, h1.position)

        # The hold has an end date. It's time for the patron to decide
        # whether or not to check out this book.
        eq_(datetime(2015, 5, 25, 17, 5, 34), h2.start)
        eq_(datetime(2015, 5, 27, 17, 5, 34), h2.end)
        eq_(0, h2.position)

    def test_place_hold(self):
        patron = self._patron()
        edition, pool = self._edition(with_license_pool=True)
        self.api.queue_response(200, content=self.sample_data("successful_hold.xml"))
        response = self.api.place_hold(patron, 'pin', pool)
        eq_(pool.identifier.type, response.identifier_type)
        eq_(pool.identifier.identifier, response.identifier)

    def test_place_hold_fails_if_exceeded_hold_limit(self):
        patron = self._patron()
        edition, pool = self._edition(with_license_pool=True)
        self.api.queue_response(400, content=self.sample_data("error_exceeded_hold_limit.xml"))
        assert_raises(PatronHoldLimitReached, self.api.place_hold,
                      patron, 'pin', pool)

    def test_get_audio_fulfillment_file(self):
        """Verify that get_audio_fulfillment_file sends the
        request we expect.
        """
        self.api.queue_response(200, content="A license")
        response = self.api.get_audio_fulfillment_file("patron id", "bib id")

        [[method, url, args, kwargs]] = self.api.requests
        eq_("POST", method)
        assert url.endswith('GetItemAudioFulfillment')
        eq_('<AudioFulfillmentRequest><ItemId>bib id</ItemId><PatronId>patron id</PatronId></AudioFulfillmentRequest>', kwargs['data'])

        eq_(200, response.status_code)
        eq_("A license", response.content)

    def test_fulfill(self):
        patron = self._patron()

        # This miracle book is available either as an audiobook or as
        # an EPUB.
        work = self._work(
            data_source_name=DataSource.BIBLIOTHECA, with_license_pool=True
        )
        [pool] = work.license_pools

        # Let's fulfill the EPUB first.
        self.api.queue_response(
            200, headers={"Content-Type": "presumably/an-acsm"},
            content="this is an ACSM"
        )
        fulfillment = self.api.fulfill(
            patron, 'password', pool, internal_format='ePub'
        )
        assert isinstance(fulfillment, FulfillmentInfo)
        eq_("this is an ACSM", fulfillment.content)
        eq_(pool.identifier.identifier, fulfillment.identifier)
        eq_(pool.identifier.type, fulfillment.identifier_type)
        eq_(pool.data_source.name, fulfillment.data_source_name)

        # The media type reported by the server is passed through.
        eq_("presumably/an-acsm", fulfillment.content_type)

        # Now let's try the audio version.
        license = self.sample_data("sample_findaway_audiobook_license.json")
        self.api.queue_response(
            200, headers={"Content-Type": "application/json"},
            content=license
        )
        fulfillment = self.api.fulfill(
            patron, 'password', pool, internal_format='MP3'
        )
        assert isinstance(fulfillment, FulfillmentInfo)

        # Here, the media type reported by the server is not passed
        # through; it's replaced by a more specific media type
        eq_(DeliveryMechanism.FINDAWAY_DRM, fulfillment.content_type)

        # The document sent by the 'Findaway' server has been
        # converted into a web publication manifest.
        manifest = json.loads(fulfillment.content)

        # The conversion process is tested more fully in
        # test_findaway_license_to_webpub_manifest. This just verifies
        # that the manifest contains information from the 'Findaway'
        # document as well as information from the Work.
        metadata = manifest['metadata']
        eq_('abcdef01234789abcdef0123', metadata['encrypted']['findaway:checkoutId'])
        eq_(work.title, metadata['title'])

        # Now let's see what happens to fulfillment when 'Findaway' or
        # 'Bibliotheca' sends bad information.
        bad_media_type = "application/error+json"
        bad_content = "This is not my beautiful license document!"
        self.api.queue_response(
            200, headers={"Content-Type": bad_media_type},
            content=bad_content
        )
        fulfillment = self.api.fulfill(
            patron, 'password', pool, internal_format='MP3'
        )
        assert isinstance(fulfillment, FulfillmentInfo)

        # The (apparently) bad document is just passed on to the
        # client as part of the FulfillmentInfo, in the hopes that the
        # client will know what to do with it.
        eq_(bad_media_type, fulfillment.content_type)
        eq_(bad_content, fulfillment.content)

    def test_findaway_license_to_webpub_manifest(self):
        work = self._work(with_license_pool=True)
        [pool] = work.license_pools
        document = self.sample_data("sample_findaway_audiobook_license.json")

        # Randomly scramble the Findaway manifest to make sure it gets
        # properly sorted when converted to a Webpub-like manifest.
        document = json.loads(document)
        document['items'].sort(key=lambda x: random.random())
        document = json.dumps(document)

        m = BibliothecaAPI.findaway_license_to_webpub_manifest
        media_type, manifest = m(pool, document)
        eq_(DeliveryMechanism.FINDAWAY_DRM, media_type)
        manifest = json.loads(manifest)

        # We use the default context for Web Publication Manifest
        # files, but we also define an extension context called
        # 'findaway', which lets us include terms coined by Findaway
        # in a normal Web Publication Manifest document.
        context = manifest['@context']
        default, findaway = context
        eq_(AudiobookManifest.DEFAULT_CONTEXT, default)
        eq_({"findaway" : FindawayManifest.FINDAWAY_EXTENSION_CONTEXT},
           findaway)

        metadata = manifest['metadata']

        # Information about the book has been added to metadata.
        # (This is tested more fully in
        # core/tests/util/test_util_web_publication_manifest.py.)
        eq_(work.title, metadata['title'])
        eq_(pool.identifier.urn, metadata['identifier'])
        eq_('en', metadata['language'])

        # Information about the license has been added to an 'encrypted'
        # object within metadata.
        encrypted = metadata['encrypted']
        eq_(u'http://librarysimplified.org/terms/drm/scheme/FAE',
            encrypted['scheme'])
        eq_(u'abcdef01234789abcdef0123', encrypted[u'findaway:checkoutId'])
        eq_(u'1234567890987654321ababa', encrypted[u'findaway:licenseId'])
        eq_(u'3M', encrypted[u'findaway:accountId'])
        eq_(u'123456', encrypted[u'findaway:fulfillmentId'])
        eq_(u'aaaaaaaa-4444-cccc-dddd-666666666666',
            encrypted[u'findaway:sessionKey'])

        # Every entry in the license document's 'items' list has
        # become a readingOrder item in the manifest.
        reading_order = manifest['readingOrder']
        eq_(79, len(reading_order))

        # The duration of each readingOrder item has been converted to
        # seconds.
        first = reading_order[0]
        eq_(16.201, first['duration'])
        eq_("Track 1", first['title'])

        # There is no 'href' value for the readingOrder items because the
        # files must be obtained through the Findaway SDK rather than
        # through regular HTTP requests.
        #
        # Since this is a relatively small book, it only has one part,
        # part #0. Within that part, the items have been sorted by
        # their sequence.
        for i, item in enumerate(reading_order):
            eq_(None, item.get('href', None))
            eq_(Representation.MP3_MEDIA_TYPE, item['type'])
            eq_(0, item['findaway:part'])
            eq_(i+1, item['findaway:sequence'])

        # The total duration, in seconds, has been added to metadata.
        eq_(28371, int(metadata['duration']))


class TestBibliothecaCirculationSweep(BibliothecaAPITest):

    def test_circulation_sweep_discovers_work(self):
        # Test what happens when BibliothecaCirculationSweep discovers a new
        # work.

        # Create an analytics integration so we can make sure
        # events are tracked.
        integration, ignore = create(
            self._db, ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol="core.local_analytics_provider",
        )

        # We know about an identifier, but nothing else.
        identifier = self._identifier(
            identifier_type=Identifier.BIBLIOTHECA_ID, foreign_id="ddf4gr9"
        )

        # We're about to get information about that identifier from
        # the API.
        data = self.sample_data("item_metadata_single.xml")

        # Update availability using that data.
        self.api.queue_response(200, content=data)
        monitor = BibliothecaCirculationSweep(
            self._db, self.collection, api_class=self.api
        )
        monitor.process_items([identifier])

        # Validate that the HTTP request went to the /items endpoint.
        request = self.api.requests.pop()
        url = request[1]
        eq_(url, self.api.full_url("items") + "/" + identifier.identifier)

        # A LicensePool has been created for the previously mysterious
        # identifier.
        [pool] = identifier.licensed_through
        eq_(self.collection, pool.collection)
        eq_(False, pool.open_access)

        # Three circulation events were created for this license pool,
        # marking the creation of the license pool, the addition of
        # licenses owned, and the making of those licenses available.
        circulation_events = self._db.query(CirculationEvent).join(LicensePool).filter(LicensePool.id==pool.id)
        eq_(3, circulation_events.count())
        types = [e.type for e in circulation_events]
        eq_(sorted([CirculationEvent.DISTRIBUTOR_LICENSE_ADD,
                    CirculationEvent.DISTRIBUTOR_TITLE_ADD,
                    CirculationEvent.DISTRIBUTOR_CHECKIN
        ]),
            sorted(types))


# Tests of the various parser classes.
#

class TestBibliothecaParser(BibliothecaAPITest):

    def test_parse_date(self):
        parser = BibliothecaParser()
        v = parser.parse_date("2016-01-02T12:34:56")
        eq_(datetime(2016, 1, 2, 12, 34, 56), v)

        eq_(None, parser.parse_date(None))
        eq_(None, parser.parse_date("Some weird value"))


class TestEventParser(BibliothecaAPITest):

    def test_parse_empty_list(self):
        data = self.sample_data("empty_event_batch.xml")

        # By default, we consider an empty batch of events not
        # as an error.
        events = list(EventParser().process_all(data))
        eq_([], events)

        # But if we consider not having events for a certain time
        # period, then an exception should be raised.
        no_events_error = True
        assert_raises_regexp(
            RemoteInitiatedServerError,
            "No events returned from server. This may not be an error, but treating it as one to be safe.",
            list, EventParser().process_all(data, no_events_error)
        )

    def test_parse_empty_end_date_event(self):
        data = self.sample_data("empty_end_date_event.xml")
        [event] = list(EventParser().process_all(data))
        (threem_id, isbn, patron_id, start_time, end_time,
         internal_event_type) = event
        eq_('d5rf89', threem_id)
        eq_(u'9781101190623', isbn)
        eq_(None, patron_id)
        eq_(datetime(2016, 4, 28, 11, 4, 6), start_time)
        eq_(None, end_time)
        eq_('distributor_license_add', internal_event_type)


class TestPatronCirculationParser(BibliothecaAPITest):

    def test_parse(self):
        data = self.sample_data("checkouts.xml")
        collection = self.collection
        loans_and_holds = PatronCirculationParser(collection).process_all(data)
        loans = [x for x in loans_and_holds if isinstance(x, LoanInfo)]
        holds = [x for x in loans_and_holds if isinstance(x, HoldInfo)]
        eq_(2, len(loans))
        eq_(2, len(holds))
        [l1, l2] = sorted(loans, key=lambda x: x.identifier)
        eq_("1ad589", l1.identifier)
        eq_("cgaxr9", l2.identifier)
        expect_loan_start = datetime(2015, 3, 20, 18, 50, 22)
        expect_loan_end = datetime(2015, 4, 10, 18, 50, 22)
        eq_(expect_loan_start, l1.start_date)
        eq_(expect_loan_end, l1.end_date)

        [h1, h2] = sorted(holds, key=lambda x: x.identifier)

        # This is the book on reserve.
        eq_(collection.id, h1.collection_id)
        eq_(DataSource.BIBLIOTHECA, h1.data_source_name)
        eq_("9wd8", h1.identifier)
        expect_hold_start = datetime(2015, 5, 25, 17, 5, 34)
        expect_hold_end = datetime(2015, 5, 27, 17, 5, 34)
        eq_(expect_hold_start, h1.start_date)
        eq_(expect_hold_end, h1.end_date)
        eq_(0, h1.hold_position)

        # This is the book on hold.
        eq_("d4o8r9", h2.identifier)
        eq_(collection.id, h2.collection_id)
        eq_(DataSource.BIBLIOTHECA, h2.data_source_name)
        expect_hold_start = datetime(2015, 3, 24, 15, 6, 56)
        expect_hold_end = datetime(2015, 3, 24, 15, 7, 51)
        eq_(expect_hold_start, h2.start_date)
        eq_(expect_hold_end, h2.end_date)
        eq_(4, h2.hold_position)


class TestCheckoutResponseParser(BibliothecaAPITest):
    def test_parse(self):
        data = self.sample_data("successful_checkout.xml")
        due_date = CheckoutResponseParser().process_all(data)
        eq_(datetime(2015, 4, 16, 0, 32, 36), due_date)


class TestErrorParser(BibliothecaAPITest):

    def test_exceeded_limit(self):
        """The normal case--we get a helpful error message which we turn into
        an appropriate circulation exception.
        """
        msg=self.sample_data("error_exceeded_limit.xml")
        error = ErrorParser().process_all(msg)
        assert isinstance(error, PatronLoanLimitReached)
        eq_(u'Patron cannot loan more than 12 documents', error.message)

    def test_exceeded_hold_limit(self):
        msg=self.sample_data("error_exceeded_hold_limit.xml")
        error = ErrorParser().process_all(msg)
        assert isinstance(error, PatronHoldLimitReached)
        eq_(u'Patron cannot have more than 15 holds', error.message)

    def test_wrong_status(self):
        msg=self.sample_data("error_no_licenses.xml")
        error = ErrorParser().process_all(msg)
        assert isinstance(error, NoLicenses)
        eq_(
            u'the patron document status was CAN_WISH and not one of CAN_LOAN,RESERVATION',
            error.message
        )

        problem = error.as_problem_detail_document()
        eq_("The library currently has no licenses for this book.",
            problem.detail)
        eq_(404, problem.status_code)

    def test_internal_server_error_beomces_remote_initiated_server_error(self):
        """Simulate the message we get when the server goes down."""
        msg = "The server has encountered an error"
        error = ErrorParser().process_all(msg)
        assert isinstance(error, RemoteInitiatedServerError)
        eq_(BibliothecaAPI.SERVICE_NAME, error.service_name)
        eq_(502, error.status_code)
        eq_(msg, error.message)
        doc = error.as_problem_detail_document()
        eq_(502, doc.status_code)
        eq_("Integration error communicating with Bibliotheca", doc.detail)

    def test_unknown_error_becomes_remote_initiated_server_error(self):
        """Simulate the message we get when ¯\_(ツ)_/¯."""
        msg=self.sample_data("error_unknown.xml")
        error = ErrorParser().process_all(msg)
        assert isinstance(error, RemoteInitiatedServerError)
        eq_(BibliothecaAPI.SERVICE_NAME, error.service_name)
        eq_("Unknown error", error.message)

    def test_remote_authentication_failed_becomes_remote_initiated_server_error(self):
        """Simulate the message we get when the error message is
        'Authentication failed' but our authentication information is
        set up correctly.
        """
        msg=self.sample_data("error_authentication_failed.xml")
        error = ErrorParser().process_all(msg)
        assert isinstance(error, RemoteInitiatedServerError)
        eq_(BibliothecaAPI.SERVICE_NAME, error.service_name)
        eq_("Authentication failed", error.message)

    def test_malformed_error_message_becomes_remote_initiated_server_error(self):
        msg = """<weird>This error does not follow the standard set out by Bibliotheca.</weird>"""
        error = ErrorParser().process_all(msg)
        assert isinstance(error, RemoteInitiatedServerError)
        eq_(BibliothecaAPI.SERVICE_NAME, error.service_name)
        eq_("Unknown error", error.message)

    def test_blank_error_message_becomes_remote_initiated_server_error(self):
        msg = """<Error xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><Message/></Error>"""
        error = ErrorParser().process_all(msg)
        assert isinstance(error, RemoteInitiatedServerError)
        eq_(BibliothecaAPI.SERVICE_NAME, error.service_name)
        eq_("Unknown error", error.message)

class TestBibliothecaEventParser(object):

    # Sample event feed to test out the parser.
    TWO_EVENTS = """<LibraryEventBatch xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <PublishId>1b0d6667-a10e-424a-9f73-fb6f6d41308e</PublishId>
  <PublishDateTimeInUTC>2014-04-14T13:59:05.6920303Z</PublishDateTimeInUTC>
  <LastEventDateTimeInUTC>2014-04-03T00:00:34</LastEventDateTimeInUTC>
  <Events>
    <CloudLibraryEvent>
      <LibraryId>test-library</LibraryId>
      <EventId>event-1</EventId>
      <EventType>CHECKIN</EventType>
      <EventStartDateTimeInUTC>2014-04-03T00:00:23</EventStartDateTimeInUTC>
      <EventEndDateTimeInUTC>2014-04-03T00:00:23</EventEndDateTimeInUTC>
      <ItemId>theitem1</ItemId>
      <ISBN>900isbn1</ISBN>
      <PatronId>patronid1</PatronId>
      <EventPublishDateTimeInUTC>2014-04-14T13:59:05</EventPublishDateTimeInUTC>
    </CloudLibraryEvent>
    <CloudLibraryEvent>
      <LibraryId>test-library</LibraryId>
      <EventId>event-2</EventId>
      <EventType>CHECKOUT</EventType>
      <EventStartDateTimeInUTC>2014-04-03T00:00:34</EventStartDateTimeInUTC>
      <EventEndDateTimeInUTC>2014-04-02T23:57:37</EventEndDateTimeInUTC>
      <ItemId>theitem2</ItemId>
      <ISBN>900isbn2</ISBN>
      <PatronId>patronid2</PatronId>
      <EventPublishDateTimeInUTC>2014-04-14T13:59:05</EventPublishDateTimeInUTC>
    </CloudLibraryEvent>
  </Events>
</LibraryEventBatch>
"""

    def test_parse_event_batch(self):
        # Parsing the XML gives us two events.
        event1, event2 = EventParser().process_all(self.TWO_EVENTS)

        (threem_id, isbn, patron_id, start_time, end_time,
         internal_event_type) = event1

        eq_("theitem1", threem_id)
        eq_("900isbn1", isbn)
        eq_("patronid1", patron_id)
        eq_(CirculationEvent.DISTRIBUTOR_CHECKIN, internal_event_type)
        eq_(start_time, end_time)

        (threem_id, isbn, patron_id, start_time, end_time,
         internal_event_type) = event2
        eq_("theitem2", threem_id)
        eq_("900isbn2", isbn)
        eq_("patronid2", patron_id)
        eq_(CirculationEvent.DISTRIBUTOR_CHECKOUT, internal_event_type)

        # Verify that start and end time were parsed correctly.
        correct_start = datetime(2014, 4, 3, 0, 0, 34)
        correct_end = datetime(2014, 4, 2, 23, 57, 37)
        eq_(correct_start, start_time)
        eq_(correct_end, end_time)


class TestErrorParser(object):

    # Some sample error documents.

    NOT_LOANABLE = '<Error xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><Code>Gen-001</Code><Message>the patron document status was CAN_HOLD and not one of CAN_LOAN,RESERVATION</Message></Error>'

    ALREADY_ON_LOAN = '<Error xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><Code>Gen-001</Code><Message>the patron document status was LOAN and not one of CAN_LOAN,RESERVATION</Message></Error>'

    TRIED_TO_RETURN_UNLOANED_BOOK = '<Error xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><Code>Gen-001</Code><Message>The patron has no eBooks checked out</Message></Error>'

    TRIED_TO_HOLD_LOANABLE_BOOK = '<Error xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><Code>Gen-001</Code><Message>the patron document status was CAN_LOAN and not one of CAN_HOLD</Message></Error>'

    TRIED_TO_HOLD_BOOK_ON_LOAN = '<Error xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><Code>Gen-001</Code><Message>the patron document status was LOAN and not one of CAN_HOLD</Message></Error>'

    ALREADY_ON_HOLD = '<Error xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><Code>Gen-001</Code><Message>the patron document status was HOLD and not one of CAN_HOLD</Message></Error>'

    TRIED_TO_CANCEL_NONEXISTENT_HOLD = '<Error xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><Code>Gen-001</Code><Message>The patron does not have the book on hold</Message></Error>'

    TOO_MANY_LOANS = '<Error xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><Code>Gen-001</Code><Message>Patron cannot loan more than 12 documents</Message></Error>'

    def test_exception(self):
        parser = ErrorParser()

        error = parser.process_all(self.NOT_LOANABLE)
        assert isinstance(error, NoAvailableCopies)

        error = parser.process_all(self.ALREADY_ON_LOAN)
        assert isinstance(error, AlreadyCheckedOut)

        error = parser.process_all(self.ALREADY_ON_HOLD)
        assert isinstance(error, AlreadyOnHold)

        error = parser.process_all(self.TOO_MANY_LOANS)
        assert isinstance(error, PatronLoanLimitReached)

        error = parser.process_all(self.TRIED_TO_CANCEL_NONEXISTENT_HOLD)
        assert isinstance(error, NotOnHold)

        error = parser.process_all(self.TRIED_TO_RETURN_UNLOANED_BOOK)
        assert isinstance(error, NotCheckedOut)

        error = parser.process_all(self.TRIED_TO_HOLD_LOANABLE_BOOK)
        assert isinstance(error, CurrentlyAvailable)

        # This is such a weird case we don't have a special
        # exception for it.
        error = parser.process_all(self.TRIED_TO_HOLD_BOOK_ON_LOAN)
        assert isinstance(error, CannotHold)


class TestBibliothecaEventMonitor(BibliothecaAPITest):

    def test_default_start_time(self):
        monitor = BibliothecaEventMonitor(
            self._db, self.collection, api_class=MockBibliothecaAPI
        )
        expected = datetime.utcnow() - monitor.DEFAULT_START_TIME

        # When the monitor has never been run before, the default
        # start time is a date long in the past.
        assert abs((expected-monitor.default_start_time).total_seconds()) <= 1
        default_start_time = monitor.create_default_start_time(self._db, [])
        assert abs((expected-default_start_time).total_seconds()) <= 1

        # It's possible to override this by instantiating
        # BibliothecaEventMonitor with a specific date.
        monitor = BibliothecaEventMonitor(
            self._db, self.collection, api_class=MockBibliothecaAPI,
            cli_date="2011-01-01"
        )
        expected = datetime(year=2011, month=1, day=1)
        eq_(expected, monitor.default_start_time)
        for cli_date in ('2011-01-01', ['2011-01-01']):
            default_start_time = monitor.create_default_start_time(
                self._db, cli_date
            )
            eq_(expected, default_start_time)

        # After Bibliotheca has been initialized,
        # create_default_start_time returns None, rather than a date
        # far in the bast, if no cli_date is passed in.
        Timestamp.stamp(
            self._db, service=monitor.service_name,
            service_type=Timestamp.MONITOR_TYPE, collection=self.collection
        )
        eq_(None, monitor.create_default_start_time(self._db, []))

        # Returns a date several years ago if args are formatted
        # improperly or the monitor has never been run before
        not_date_args = ['initialize']
        too_many_args = ['2013', '04', '02']
        for args in [not_date_args, too_many_args]:
            actual_default_start_time = monitor.create_default_start_time(self._db, args)
            eq_(True, isinstance(actual_default_start_time, datetime))
            assert (default_start_time - actual_default_start_time).total_seconds() <= 1

        # Returns an appropriate date if command line arguments are passed
        # as expected
        proper_args = ['2013-04-02']
        default_start_time = monitor.create_default_start_time(self._db, proper_args)
        eq_(datetime(2013, 4, 2), default_start_time)

    def test_run_once(self):
        # run_once() slices the time between its start date
        # and the current time into five-minute intervals, and asks for
        # data about one day at a time.

        now = datetime.utcnow()
        one_hour_ago = now - timedelta(hours=1)
        two_hours_ago = now - timedelta(hours=2)

        # Simulate that this script last ran 24 hours ago
        before_timestamp = TimestampData(
            start=two_hours_ago, finish=one_hour_ago
        )

        api = MockBibliothecaAPI(self._db, self.collection)
        api.queue_response(
            200, content=self.sample_data("item_metadata_single.xml")
        )
        # Setting up making requests in 5-minute intervals in the hour slice.
        for i in range(1, 15):
            api.queue_response(
                200, content=self.sample_data("empty_end_date_event.xml")
            )

        monitor = BibliothecaEventMonitor(
            self._db, self.collection, api_class=api
        )

        after_timestamp = monitor.run_once(before_timestamp)
        # Fifteen requests were made to the API:
        #
        # 1. Looking up detailed information about the single book
        #    whose event we found.
        #
        # 2. Retrieving the 'slices' of events between 2 hours ago and
        #    1 hours ago in 5 minute intervals.
        eq_(15, len(api.requests))

        # There is no second 'detailed information' lookup because both events
        # relate to the same book.

        # A LicensePool was created for the identifier referred to
        # in empty_end_date_event.xml.
        [pool] = self.collection.licensepools
        eq_("d5rf89", pool.identifier.identifier)

        # But since the metadata retrieved in the follow-up request
        # was for a different book, no Work and no Edition have been
        # created. (See test_handle_event for what happens when the
        # API cooperates.)
        eq_(None, pool.work)
        eq_(None, pool.presentation_edition)

        # The timeframe covered by that run starts a little before the
        # 'finish' date associated with the old timestamp, and ends
        # around the time run_once() was called.
        #
        # The events we found were both from 2016, but that's not
        # considered when setting the timestamp.
        eq_(one_hour_ago-monitor.OVERLAP, after_timestamp.start)
        self.time_eq(after_timestamp.finish, now)
        # The timestamp's achivements have been updated.
        eq_("Events handled: 13.", after_timestamp.achievements)

        # After the initial run, the progress timestamp's `counter` property
        # is set to `1`. This means we are now in "catch up" mode where we
        # consider no events to be an error. The timespan to check for events
        # also expands to a 70-hour slice of time.
        api.queue_response(
            200, content=self.sample_data("empty_event_batch.xml")
        )
        
        assert after_timestamp.counter == 1

        assert_raises_regexp(
            RemoteInitiatedServerError,
            "No events returned from server. This may not be an error, but treating it as one to be safe.",
            monitor.run_once, after_timestamp
        )

        # One request was made but no events were found.
        eq_(16, len(api.requests))
        
        # If we are in "catch up" mode and the timespan to check for events
        # is longer than 70 hours, we revert back to checking for events
        # in 5-minute intervals.
        now = datetime.utcnow()
        two_days_ago = now - timedelta(days=2)
        seven_days_ago = now - timedelta(days=5)
        before_timestamp = TimestampData(
            start=seven_days_ago, finish=two_days_ago
        )
        before_timestamp.counter = 1

        # All the requests triggered in the 3 day span in 5-minute intervals.
        for i in range(1, 600):
            api.queue_response(
                200, content=self.sample_data("empty_end_date_event.xml")
            )

        after_timestamp = monitor.run_once(before_timestamp)
        
        assert after_timestamp.counter == 0

    def test_handle_event(self):
        api = MockBibliothecaAPI(self._db, self.collection)
        api.queue_response(
            200, content=self.sample_data("item_metadata_single.xml")
        )
        analytics = MockAnalyticsProvider()
        monitor = BibliothecaEventMonitor(
            self._db, self.collection, api_class=api,
            analytics=analytics
        )

        now = datetime.utcnow()
        monitor.handle_event("ddf4gr9", "9781250015280", None, now, None,
                             CirculationEvent.DISTRIBUTOR_LICENSE_ADD)

        # The collection now has a LicensePool corresponding to the book
        # we just loaded.
        [pool] = self.collection.licensepools
        eq_("ddf4gr9", pool.identifier.identifier)

        # The book has a presentation-ready work and we know its
        # bibliographic metadata.
        eq_(True, pool.work.presentation_ready)
        eq_("The Incense Game", pool.work.title)

        # The LicensePool's circulation information has been changed
        # to reflect what we know about the book -- that we have one
        # license which (as of the instant the event happened) is
        # available.
        eq_(1, pool.licenses_owned)
        eq_(1, pool.licenses_available)

        # Four analytics events were collected: one for the license add
        # event itself, one for the 'checkin' that made the new
        # license available, one for the first appearance of a new
        # LicensePool, and a redundant 'license add' event
        # which was registered with analytics but which did not
        # affect the counts.
        eq_(4, analytics.count)


class TestItemListParser(BibliothecaAPITest):

    def test_contributors_for_string(cls):
        authors = list(ItemListParser.contributors_from_string(
            "Walsh, Jill Paton; Sayers, Dorothy L."))
        eq_([x.sort_name for x in authors],
            ["Walsh, Jill Paton", "Sayers, Dorothy L."]
        )
        eq_([x.roles for x in authors],
            [[Contributor.AUTHOR_ROLE], [Contributor.AUTHOR_ROLE]]
        )

        # Parentheticals are stripped.
        [author] = ItemListParser.contributors_from_string(
            "Baum, Frank L. (Frank Lyell)")
        eq_("Baum, Frank L.", author.sort_name)

        # It's possible to specify some role other than AUTHOR_ROLE.
        narrators = list(
            ItemListParser.contributors_from_string(
                "Callow, Simon; Mann, Bruce; Hagon, Garrick",
                Contributor.NARRATOR_ROLE
            )
        )
        for narrator in narrators:
            eq_([Contributor.NARRATOR_ROLE], narrator.roles)
        eq_(["Callow, Simon", "Mann, Bruce", "Hagon, Garrick"],
            [narrator.sort_name for narrator in narrators])

    def test_parse_genre_string(self):
        def f(genre_string):
            genres = ItemListParser.parse_genre_string(genre_string)
            assert all([x.type == Subject.BISAC for x in genres])
            return [x.name for x in genres]

        eq_(["Children's Health", "Health"],
            f("Children&amp;#39;s Health,Health,"))

        eq_(["Action & Adventure", "Science Fiction", "Fantasy", "Magic",
             "Renaissance"],
            f("Action &amp;amp; Adventure,Science Fiction, Fantasy, Magic,Renaissance,"))

    def test_item_list(cls):
        data = cls.sample_data("item_metadata_list_mini.xml")
        data = list(ItemListParser().parse(data))

        # There should be 2 items in the list.
        eq_(2, len(data))

        cooked = data[0]

        eq_("The Incense Game", cooked.title)
        eq_("A Novel of Feudal Japan", cooked.subtitle)
        eq_(Edition.BOOK_MEDIUM, cooked.medium)
        eq_("eng", cooked.language)
        eq_("St. Martin's Press", cooked.publisher)
        eq_(datetime(year=2012, month=9, day=17),
            cooked.published
        )

        primary = cooked.primary_identifier
        eq_("ddf4gr9", primary.identifier)
        eq_(Identifier.THREEM_ID, primary.type)

        identifiers = sorted(
            cooked.identifiers, key=lambda x: x.identifier
        )
        eq_([u'9781250015280', u'9781250031112', u'ddf4gr9'],
            [x.identifier for x in identifiers])

        [author] = cooked.contributors
        eq_("Rowland, Laura Joh", author.sort_name)
        eq_([Contributor.AUTHOR_ROLE], author.roles)

        subjects = [x.name for x in cooked.subjects]
        eq_(["Children's Health", "Mystery & Detective"], sorted(subjects))

        [pages] = cooked.measurements
        eq_(Measurement.PAGE_COUNT, pages.quantity_measured)
        eq_(304, pages.value)

        [alternate, image, description] = sorted(
            cooked.links, key = lambda x: x.rel)
        eq_("alternate", alternate.rel)
        assert alternate.href.startswith("http://ebook.3m.com/library")

        # We have a full-size image...
        eq_(Hyperlink.IMAGE, image.rel)
        eq_(Representation.JPEG_MEDIA_TYPE, image.media_type)
        assert image.href.startswith("http://ebook.3m.com/delivery")
        assert 'documentID=ddf4gr9' in image.href
        assert '&size=NORMAL' not in image.href

        # ... and a thumbnail, which we obtained by adding an argument
        # to the main image URL.
        thumbnail = image.thumbnail
        eq_(Hyperlink.THUMBNAIL_IMAGE, thumbnail.rel)
        eq_(Representation.JPEG_MEDIA_TYPE, thumbnail.media_type)
        eq_(thumbnail.href, image.href + "&size=NORMAL")

        # We have a description.
        eq_(Hyperlink.DESCRIPTION, description.rel)
        assert description.content.startswith("<b>Winner")

    def test_multiple_contributor_roles(self):
        data = self.sample_data("item_metadata_audio.xml")
        [data] = list(ItemListParser().parse(data))
        names_and_roles = []
        for c in data.contributors:
            [role] = c.roles
            names_and_roles.append((c.sort_name, role))

        # We found one author and three narrators.
        eq_(
            sorted([(u'Riggs, Ransom', u'Author'),
                    (u'Callow, Simon', u'Narrator'),
                    (u'Mann, Bruce', u'Narrator'),
                    (u'Hagon, Garrick', u'Narrator')]),
            sorted(names_and_roles)
        )

class TestBibliographicCoverageProvider(TestBibliothecaAPI):

    """Test the code that looks up bibliographic information from Bibliotheca."""

    def test_script_instantiation(self):
        """Test that RunCollectionCoverageProviderScript can instantiate
        this coverage provider.
        """
        script = RunCollectionCoverageProviderScript(
            BibliothecaBibliographicCoverageProvider, self._db,
            api_class=MockBibliothecaAPI
        )
        [provider] = script.providers
        assert isinstance(provider,
                          BibliothecaBibliographicCoverageProvider)
        assert isinstance(provider.api, MockBibliothecaAPI)

    def test_process_item_creates_presentation_ready_work(self):
        # Test the normal workflow where we ask Bibliotheca for data,
        # Bibliotheca provides it, and we create a presentation-ready work.
        identifier = self._identifier(identifier_type=Identifier.BIBLIOTHECA_ID)
        identifier.identifier = 'ddf4gr9'

        # This book has no LicensePools.
        eq_([], identifier.licensed_through)

        # Run it through the BibliothecaBibliographicCoverageProvider
        provider = BibliothecaBibliographicCoverageProvider(
            self.collection, api_class=MockBibliothecaAPI
        )
        data = self.sample_data("item_metadata_single.xml")

        # We can't use self.api because that's not the same object
        # as the one created by the coverage provider.
        provider.api.queue_response(200, content=data)

        [result] = provider.process_batch([identifier])
        eq_(identifier, result)

        # A LicensePool was created and populated with format and availability
        # information.
        [pool] = identifier.licensed_through
        eq_(1, pool.licenses_owned)
        eq_(1, pool.licenses_available)
        [lpdm] = pool.delivery_mechanisms
        eq_(
            'application/epub+zip (application/vnd.adobe.adept+xml)',
            lpdm.delivery_mechanism.name
        )

        # A Work was created and made presentation ready.
        eq_("The Incense Game", pool.work.title)
        eq_(True, pool.work.presentation_ready)

    def test_internal_formats(self):

        m = ItemListParser.internal_formats
        def _check_format(input, expect_medium, expect_format, expect_drm):
            medium, formats = m(input)
            eq_(medium, expect_medium)
            [format] = formats
            eq_(expect_format, format.content_type)
            eq_(expect_drm, format.drm_scheme)

        rep = Representation
        adobe = DeliveryMechanism.ADOBE_DRM
        findaway = DeliveryMechanism.FINDAWAY_DRM
        book = Edition.BOOK_MEDIUM

        # Verify that we handle the known strings from Bibliotheca
        # appropriately.
        _check_format("EPUB", book, rep.EPUB_MEDIA_TYPE, adobe)
        _check_format("EPUB3", book, rep.EPUB_MEDIA_TYPE, adobe)
        _check_format("PDF", book, rep.PDF_MEDIA_TYPE, adobe)
        _check_format("MP3", Edition.AUDIO_MEDIUM, None, findaway)

        # Now Try a string we don't recognize from Bibliotheca.
        medium, formats = m("Unknown")

        # We assume it's a book.
        eq_(Edition.BOOK_MEDIUM, medium)

        # But we don't know which format.
        eq_([], formats)
