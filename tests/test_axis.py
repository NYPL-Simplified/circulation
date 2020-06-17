import datetime
import json
import os
from lxml import etree
from StringIO import StringIO
from nose.tools import (
    eq_,
    assert_raises,
    assert_raises_regexp,
    set_trace,
)

from core.analytics import Analytics
from core.mock_analytics_provider import MockAnalyticsProvider
from core.coverage import CoverageFailure

from core.model import (
    ConfigurationSetting,
    Contributor,
    DataSource,
    DeliveryMechanism,
    Edition,
    ExternalIntegration,
    Hyperlink,
    Identifier,
    LicensePool,
    LinkRelations,
    MediaTypes,
    Representation,
    Subject,
    create,
)

from core.metadata_layer import (
    Metadata,
    CirculationData,
    IdentifierData,
    ContributorData,
    SubjectData,
    TimestampData,
)

from core.scripts import RunCollectionCoverageProviderScript
from core.testing import MockRequestsResponse

from core.util.http import (
    RemoteIntegrationException,
    HTTP,
)

from api.authenticator import BasicAuthenticationProvider

from api.axis import (
    AudiobookFulfillmentInfo,
    AudiobookFulfillmentInfoResponseParser,
    AudiobookMetadataParser,
    AvailabilityResponseParser,
    Axis360API,
    Axis360BibliographicCoverageProvider,
    Axis360CirculationMonitor,
    AxisCollectionReaper,
    BibliographicParser,
    CheckoutResponseParser,
    EbookFulfillmentInfo,
    HoldReleaseResponseParser,
    HoldResponseParser,
    JSONResponseParser,
    MockAxis360API,
    ResponseParser,
)

from . import (
    DatabaseTest,
    sample_data
)

from api.circulation import (
    LoanInfo,
    HoldInfo,
    FulfillmentInfo,
)

from api.circulation_exceptions import *

from api.config import (
    Configuration,
    temp_config,
)

from api.web_publication_manifest import (
    FindawayManifest,
    SpineItem,
)


class Axis360Test(DatabaseTest):

    def setup(self):
        super(Axis360Test,self).setup()
        self.collection = MockAxis360API.mock_collection(self._db)
        self.api = MockAxis360API(self._db, self.collection)

    @classmethod
    def sample_data(self, filename):
        return sample_data(filename, 'axis')

    # Sample bibliographic and availability data you can use in a test
    # without having to parse it from an XML file.
    BIBLIOGRAPHIC_DATA = Metadata(
        DataSource.AXIS_360,
        publisher=u'Random House Inc',
        language='eng',
        title=u'Faith of My Fathers : A Family Memoir',
        imprint=u'Random House Inc2',
        published=datetime.datetime(2000, 3, 7, 0, 0),
        primary_identifier=IdentifierData(
            type=Identifier.AXIS_360_ID,
            identifier=u'0003642860'
        ),
        identifiers = [
            IdentifierData(type=Identifier.ISBN, identifier=u'9780375504587')
        ],
        contributors = [
            ContributorData(sort_name=u"McCain, John",
                            roles=[Contributor.PRIMARY_AUTHOR_ROLE]
                        ),
            ContributorData(sort_name=u"Salter, Mark",
                            roles=[Contributor.AUTHOR_ROLE]
                        ),
        ],
        subjects = [
            SubjectData(type=Subject.BISAC,
                        identifier=u'BIOGRAPHY & AUTOBIOGRAPHY / Political'),
            SubjectData(type=Subject.FREEFORM_AUDIENCE,
                        identifier=u'Adult'),
        ],
    )

    AVAILABILITY_DATA = CirculationData(
        data_source=DataSource.AXIS_360,
        primary_identifier=BIBLIOGRAPHIC_DATA.primary_identifier,
        licenses_owned=9,
        licenses_available=8,
        licenses_reserved=0,
        patrons_in_hold_queue=0,
        last_checked=datetime.datetime(2015, 5, 20, 2, 9, 8),
    )


class TestAxis360API(Axis360Test):

    def test_external_integration(self):
        eq_(
            self.collection.external_integration,
            self.api.external_integration(object())
        )

    def test__run_self_tests(self):
        """Verify that BibliothecaAPI._run_self_tests() calls the right
        methods.
        """
        class Mock(MockAxis360API):
            "Mock every method used by Axis360API._run_self_tests."

            # First we will refresh the bearer token.
            def refresh_bearer_token(self):
                return "the new token"

            # Then we will count the number of events in the past
            # give minutes.
            def recent_activity(self, since):
                self.recent_activity_called_with = since
                return [(1,"a"),(2, "b"), (3, "c")]

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
        now = datetime.datetime.utcnow()
        [no_patron_credential, recent_circulation_events, patron_activity,
         pools_without_delivery, refresh_bearer_token] = sorted(
            api._run_self_tests(self._db), key=lambda x: x.name
        )
        eq_("Refreshing bearer token", refresh_bearer_token.name)
        eq_(True, refresh_bearer_token.success)
        eq_("the new token", refresh_bearer_token.result)

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
        since = api.recent_activity_called_with
        five_minutes_ago = datetime.datetime.utcnow() - datetime.timedelta(minutes=5)
        assert (five_minutes_ago-since).total_seconds() < 2

        eq_("Checking activity for test patron for library %s" % with_default_patron.name,
            patron_activity.name)
        eq_(True, patron_activity.success)
        eq_("Found 2 loans/holds", patron_activity.result)
        patron, pin = api.patron_activity_called_with
        eq_("username1", patron.authorization_identifier)
        eq_("password1", pin)

        eq_("Checking for titles that have no delivery mechanisms.",
            pools_without_delivery.name)
        eq_(True, pools_without_delivery.success)
        eq_("All titles in this collection have delivery mechanisms.",
            pools_without_delivery.result)

    def test__run_self_tests_short_circuit(self):
        """If we can't refresh the bearer token, the rest of the
        self-tests aren't even run.
        """
        class Mock(MockAxis360API):
            def refresh_bearer_token(self):
                raise Exception("no way")

        # Now that everything is set up, run the self-test. Only one
        # test will be run.
        api = Mock(self._db, self.collection)
        [failure] = api._run_self_tests(self._db)
        eq_("Refreshing bearer token", failure.name)
        eq_(False, failure.success)
        eq_("no way", failure.exception.message)

    def test_create_identifier_strings(self):
        identifier = self._identifier()
        values = Axis360API.create_identifier_strings(["foo", identifier])
        eq_(["foo", identifier.identifier], values)

    def test_availability_no_timeout(self):
        # The availability API request has no timeout set, because it
        # may take time proportinate to the total size of the
        # collection.
        self.api.queue_response(200)
        self.api.availability()
        request = self.api.requests.pop()
        kwargs = request[-1]
        eq_(None, kwargs['timeout'])

    def test_availability_exception(self):

        self.api.queue_response(500)
        assert_raises_regexp(
            RemoteIntegrationException, "Bad response from http://axis.test/availability/v2: Got status code 500 from external server, cannot continue.",
            self.api.availability
        )

    def test_refresh_bearer_token_after_401(self):
        """If we get a 401, we will fetch a new bearer token and try the
        request again.
        """
        self.api.queue_response(401)
        self.api.queue_response(
            200, content=json.dumps(dict(access_token="foo"))
        )
        self.api.queue_response(200, content="The data")
        response = self.api.request("http://url/")
        eq_("The data", response.content)

    def test_refresh_bearer_token_error(self):
        """Raise an exception if we don't get a 200 status code when
        refreshing the bearer token.
        """
        api = MockAxis360API(self._db, self.collection, with_token=False)
        api.queue_response(412)
        assert_raises_regexp(
            RemoteIntegrationException, "Bad response from http://axis.test/accesstoken: Got status code 412 from external server, but can only continue on: 200.",
            api.refresh_bearer_token
        )

    def test_exception_after_401_with_fresh_token(self):
        """If we get a 401 immediately after refreshing the token, we will
        raise an exception.
        """
        self.api.queue_response(401)
        self.api.queue_response(
            200, content=json.dumps(dict(access_token="foo"))
        )
        self.api.queue_response(401)

        self.api.queue_response(301)

        assert_raises_regexp(
            RemoteIntegrationException,
            ".*Got status code 401 from external server, cannot continue.",
            self.api.request, "http://url/"
        )

        # The fourth request never got made.
        eq_([301], [x.status_code for x in self.api.responses])

    def test_update_availability(self):
        """Test the Axis 360 implementation of the update_availability method
        defined by the CirculationAPI interface.
        """

        # Create a LicensePool that needs updating.
        edition, pool = self._edition(
            identifier_type=Identifier.AXIS_360_ID,
            data_source_name=DataSource.AXIS_360,
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

        # Prepare availability information.
        data = self.sample_data("availability_with_loans.xml")

        # Modify the data so that it appears to be talking about the
        # book we just created.
        new_identifier = pool.identifier.identifier.encode("ascii")
        data = data.replace("0012533119", new_identifier)

        self.api.queue_response(200, content=data)

        self.api.update_availability(pool)

        # The availability information has been udpated, as has the
        # date the availability information was last checked.
        eq_(2, pool.licenses_owned)
        eq_(1, pool.licenses_available)
        eq_(0, pool.patrons_in_hold_queue)
        assert pool.last_checked is not None

    def test_place_hold(self):
        edition, pool = self._edition(
            identifier_type=Identifier.AXIS_360_ID,
            data_source_name=DataSource.AXIS_360,
            with_license_pool=True
        )
        data = self.sample_data("place_hold_success.xml")
        self.api.queue_response(200, content=data)
        patron = self._patron()
        ConfigurationSetting.for_library(
            Configuration.DEFAULT_NOTIFICATION_EMAIL_ADDRESS,
            self._default_library).value = "notifications@example.com"
        response = self.api.place_hold(patron, 'pin', pool, None)
        eq_(1, response.hold_position)
        eq_(response.identifier_type, pool.identifier.type)
        eq_(response.identifier, pool.identifier.identifier)
        [request] = self.api.requests
        params = request[-1]['params']
        eq_('notifications@example.com', params['email'])

    def test_fulfill(self):
        # Test our ability to fulfill an Axis 360 title.
        edition, pool = self._edition(
            identifier_type=Identifier.AXIS_360_ID,
            identifier_id='0015176429',
            data_source_name=DataSource.AXIS_360,
            with_license_pool=True
        )

        patron = self._patron()
        patron.authorization_identifier = "a barcode"

        def fulfill(internal_format="not AxisNow"):
            return self.api.fulfill(
                patron, "pin", licensepool=pool,
                internal_format=internal_format
            )

        # If Axis 360 says a patron does not have a title checked out,
        # an attempt to fulfill that title will fail with NoActiveLoan.
        data = self.sample_data("availability_with_audiobook_fulfillment.xml")
        self.api.queue_response(200, content=data)
        assert_raises(NoActiveLoan, fulfill)

        # If the title is checked out and Axis provides fulfillment
        # information, that information becomes an
        # EbookFulfillmentInfo object with a content link.
        data = self.sample_data("availability_with_loan_and_hold.xml")
        self.api.queue_response(200, content=data)
        fulfillment = fulfill()
        assert isinstance(fulfillment, FulfillmentInfo)
        eq_(DeliveryMechanism.ADOBE_DRM, fulfillment.content_type)
        eq_("http://fulfillment/", fulfillment.content_link)
        eq_(None, fulfillment.content)

        # If we ask for AxisNow format, we get an EbookFulfillmentInfo
        # containing an AxisNow manifest document.
        data = self.sample_data("availability_with_loan_and_hold.xml")
        self.api.queue_response(200, content=data)
        fulfillment = fulfill("AxisNow")
        assert isinstance(fulfillment, EbookFulfillmentInfo)
        eq_(MediaTypes.AXISNOW_MANIFEST_MEDIA_TYPE, fulfillment.content_type)
        eq_(fulfillment.axisnow_manifest_document, fulfillment.content)
        eq_(None, fulfillment.content_link)

        # If the title is checked out but Axis provides no fulfillment
        # info, the exception is CannotFulfill.
        pool.identifier.identifier = '0015176429'
        data = self.sample_data("availability_without_fulfillment.xml")
        self.api.queue_response(200, content=data)
        assert_raises(CannotFulfill, fulfill)

        # If we ask to fulfill an audiobook, we get an AudiobookFulfillmentInfo.
        #
        # Change our test LicensePool's identifier to match the data we're about
        # to load into the API.
        pool.identifier, ignore = Identifier.for_foreign_id(
            self._db, Identifier.AXIS_360_ID, "0012244222"
        )
        data = self.sample_data("availability_with_audiobook_fulfillment.xml")
        self.api.queue_response(200, content=data)
        fulfillment = fulfill()
        assert isinstance(fulfillment, AudiobookFulfillmentInfo)

    def test_patron_activity(self):
        """Test the method that locates all current activity
        for a patron.
        """
        data = self.sample_data("availability_with_loan_and_hold.xml")
        self.api.queue_response(200, content=data)
        patron = self._patron()
        patron.authorization_identifier = "a barcode"

        results = self.api.patron_activity(patron, "pin")

        # We made a request that included the authorization identifier
        # of the patron in question.
        [url, args, kwargs] = self.api.requests.pop()
        eq_(patron.authorization_identifier, kwargs['params']['patronId'])

        # We got three results -- two holds and one loan.
        [hold1, loan, hold2] = sorted(
            results, key=lambda x: x.identifier
        )
        assert isinstance(hold1, HoldInfo)
        assert isinstance(hold2, HoldInfo)
        assert isinstance(loan, LoanInfo)

    def test_update_licensepools_for_identifiers(self):

        class Mock(MockAxis360API):
            """Simulates an Axis 360 API that knows about some
            books but not others.
            """
            updated = []
            reaped = []

            def _fetch_remote_availability(self, identifiers):
                for i, identifier in enumerate(identifiers):
                    # The first identifer in the list is still
                    # available.
                    identifier_data = IdentifierData(
                        type=identifier.type,
                        identifier=identifier.identifier
                    )
                    metadata = Metadata(
                        data_source=DataSource.AXIS_360,
                        primary_identifier=identifier_data
                    )
                    availability = CirculationData(
                        data_source=DataSource.AXIS_360,
                        primary_identifier=identifier_data,
                        licenses_owned=7,
                        licenses_available=6
                    )
                    yield metadata, availability

                    # The rest have been 'forgotten' by Axis 360.
                    break

            def _reap(self, identifier):
                self.reaped.append(identifier)

        api = Mock(self._db, self.collection)
        still_in_collection = self._identifier(
            identifier_type=Identifier.AXIS_360_ID
        )
        no_longer_in_collection = self._identifier(
            identifier_type=Identifier.AXIS_360_ID
        )
        api.update_licensepools_for_identifiers(
            [still_in_collection, no_longer_in_collection]
        )

        # The LicensePool for the first identifier was updated.
        [lp] = still_in_collection.licensed_through
        eq_(7, lp.licenses_owned)
        eq_(6, lp.licenses_available)

        # The second was reaped.
        eq_([no_longer_in_collection], api.reaped)

    def test_fetch_remote_availability(self):
        """Test the _fetch_remote_availability method, as
        used by update_licensepools_for_identifiers.
        """
        id1 = self._identifier(identifier_type=Identifier.AXIS_360_ID)
        id2 = self._identifier(identifier_type=Identifier.AXIS_360_ID)
        data = self.sample_data("availability_with_loans.xml")
        # Modify the sample data so that it appears to be talking
        # about one of the books we're going to request.
        data = data.replace("0012533119", id1.identifier.encode("ascii"))
        self.api.queue_response(200, {}, data)
        results = [x for x in self.api._fetch_remote_availability([id1, id2])]

        # We asked for information on two identifiers.
        [request] = self.api.requests
        kwargs = request[-1]
        eq_({'titleIds': u'2001,2002'}, kwargs['params'])

        # We got information on only one.
        [(metadata, circulation)] = results
        eq_((id1, False), metadata.primary_identifier.load(self._db))
        eq_(u'El caso de la gracia : Un periodista explora las evidencias de unas vidas transformadas', metadata.title)
        eq_(2, circulation.licenses_owned)

    def test_reap(self):
        """Test the _reap method, as used by
        update_licensepools_for_identifiers.
        """
        id1 = self._identifier(identifier_type=Identifier.AXIS_360_ID)
        eq_([], id1.licensed_through)

        # If there is no LicensePool to reap, nothing happens.
        self.api._reap(id1)
        eq_([], id1.licensed_through)

        # If there is a LicensePool but it has no owned licenses,
        # it's already been reaped, so nothing happens.
        edition, pool, = self._edition(
            data_source_name=DataSource.AXIS_360,
            identifier_type=id1.type, identifier_id=id1.identifier,
            with_license_pool=True, collection=self.collection
        )

        # This LicensePool has licenses, but it's not in a different
        # collection from the collection associated with this
        # Axis360API object, so it's not affected.
        collection2 = self._collection()
        edition2, pool2, = self._edition(
            data_source_name=DataSource.AXIS_360,
            identifier_type=id1.type, identifier_id=id1.identifier,
            with_license_pool=True, collection=collection2
        )

        pool.licenses_owned = 0
        pool2.licenses_owned = 10
        self._db.commit()
        updated = pool.last_checked
        updated2 = pool2.last_checked
        self.api._reap(id1)

        eq_(updated, pool.last_checked)
        eq_(0, pool.licenses_owned)
        eq_(updated2, pool2.last_checked)
        eq_(10, pool2.licenses_owned)

        # If the LicensePool did have licenses, then reaping it
        # reflects the fact that the licenses are no longer owned.
        pool.licenses_owned = 10
        pool.licenses_available = 9
        pool.licenses_reserved = 8
        pool.patrons_in_hold_queue = 7
        self.api._reap(id1)
        eq_(0, pool.licenses_owned)
        eq_(0, pool.licenses_available)
        eq_(0, pool.licenses_reserved)
        eq_(0, pool.patrons_in_hold_queue)

    def test_get_fulfillment_info(self):
        # Test the get_fulfillment_info method, which makes an API request.

        api = MockAxis360API(self._db, self.collection)
        api.queue_response(200, {}, "the response")

        # Make a request and check the response.
        response = api.get_fulfillment_info("transaction ID")
        eq_("the response", response.content)

        # Verify that the 'HTTP request' was made to the right URL
        # with the right keyword arguments and the right HTTP method.
        url, args, kwargs = api.requests.pop()
        assert url.endswith(api.fulfillment_endpoint)
        eq_('POST', kwargs['method'])
        eq_('transaction ID', kwargs['params']['TransactionID'])

    def test_get_audiobook_metadata(self):
        # Test the get_audiobook_metadata method, which makes an API request.

        api = MockAxis360API(self._db, self.collection)
        api.queue_response(200, {}, "the response")

        # Make a request and check the response.
        response = api.get_audiobook_metadata("Findaway content ID")
        eq_("the response", response.content)

        # Verify that the 'HTTP request' was made to the right URL
        # with the right keyword arguments and the right HTTP method.
        url, args, kwargs = api.requests.pop()
        assert url.endswith(api.audiobook_metadata_endpoint)
        eq_('POST', kwargs['method'])
        eq_('Findaway content ID', kwargs['params']['fndcontentid'])

    def test_update_book(self):
        # Verify that the update_book method takes a Metadata and a
        # CirculationData object, and creates appropriate data model
        # objects.

        analytics = MockAnalyticsProvider()
        api = MockAxis360API(self._db, self.collection)
        e, e_new, lp, lp_new = api.update_book(
            self.BIBLIOGRAPHIC_DATA, self.AVAILABILITY_DATA,
            analytics=analytics
        )
        # A new LicensePool and Edition were created.
        eq_(True, lp_new)
        eq_(True, e_new)

        # The LicensePool reflects what it said in AVAILABILITY_DATA
        eq_(9, lp.licenses_owned)

        # There's a presentation-ready Work created for the
        # LicensePool.
        eq_(True, lp.work.presentation_ready)
        eq_(e, lp.work.presentation_edition)

        # The Edition reflects what it said in BIBLIOGRAPHIC_DATA
        eq_(u'Faith of My Fathers : A Family Memoir', e.title)

        # Three analytics events were sent out.
        #
        # It's not super important to test which ones, but they are:
        # 1. The creation of the LicensePool
        # 2. The setting of licenses_owned to 9
        # 3. The setting of licenses_available to 8
        eq_(3, analytics.count)

        # Now change a bit of the data and call the method again.
        new_circulation = CirculationData(
            data_source=DataSource.AXIS_360,
            primary_identifier=self.BIBLIOGRAPHIC_DATA.primary_identifier,
            licenses_owned=8,
            licenses_available=7,
        )

        e2, e_new, lp2, lp_new = api.update_book(
            self.BIBLIOGRAPHIC_DATA, new_circulation,
            analytics=analytics
        )

        # The same LicensePool and Edition are returned -- no new ones
        # are created.
        eq_(e2, e)
        eq_(False, e_new)
        eq_(lp2, lp)
        eq_(False, lp_new)

        # The LicensePool has been updated to reflect the new
        # CirculationData
        eq_(8, lp.licenses_owned)
        eq_(7, lp.licenses_available)

        # Two more circulation events have been sent out -- one for
        # the licenses_owned change and one for the licenses_available
        # change.
        eq_(5, analytics.count)


class TestCirculationMonitor(Axis360Test):

    def test_run(self):
        class Mock(Axis360CirculationMonitor):
            def catch_up_from(self, start, cutoff, progress):
                self.called_with = (start, cutoff, progress)
        monitor = Mock(self._db, self.collection, api_class=MockAxis360API)

        # The first time run() is called, catch_up_from() is asked to
        # find events between DEFAULT_START_TIME and the current time.
        monitor.run()
        start, cutoff, progress = monitor.called_with
        now = datetime.datetime.utcnow()
        eq_(monitor.DEFAULT_START_TIME, start)
        assert (now - cutoff).total_seconds() < 2

        # The second time run() is called, catch_up_from() is asked
        # to find events between five minutes before the last cutoff,
        # and what is now the current time.
        monitor.run()
        new_start, new_cutoff, new_progress = monitor.called_with
        now = datetime.datetime.utcnow()
        before_old_cutoff = cutoff - monitor.OVERLAP
        eq_(before_old_cutoff, new_start)
        assert (now - new_cutoff).total_seconds() < 2

    def test_catch_up_from(self):
        class MockAPI(MockAxis360API):
            def recent_activity(self, since):
                self.recent_activity_called_with = since
                return [(1,"a"),(2, "b")]

        class MockMonitor(Axis360CirculationMonitor):
            processed = []
            def process_book(self, bibliographic, circulation):
                self.processed.append((bibliographic, circulation))

        monitor = MockMonitor(self._db, self.collection, api_class=MockAPI)
        data = self.sample_data("single_item.xml")
        self.api.queue_response(200, content=data)
        progress = TimestampData()
        monitor.catch_up_from("start", "cutoff", progress)

        # The start time was passed into recent_activity.
        eq_("start", monitor.api.recent_activity_called_with)

        # process_book was called on each item returned by recent_activity.
        eq_([(1,"a"),(2, "b")], monitor.processed)

        # The number of books processed was stored in
        # TimestampData.achievements.
        eq_("Modified titles: 2.", progress.achievements)

    def test_process_book(self):
        integration, ignore = create(
            self._db, ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol="core.local_analytics_provider",
        )

        monitor = Axis360CirculationMonitor(
            self._db, self.collection, api_class=MockAxis360API,
        )
        edition, license_pool = monitor.process_book(
            self.BIBLIOGRAPHIC_DATA, self.AVAILABILITY_DATA)
        eq_(u'Faith of My Fathers : A Family Memoir', edition.title)
        eq_(u'eng', edition.language)
        eq_(u'Random House Inc', edition.publisher)
        eq_(u'Random House Inc2', edition.imprint)

        eq_(Identifier.AXIS_360_ID, edition.primary_identifier.type)
        eq_(u'0003642860', edition.primary_identifier.identifier)

        [isbn] = [x for x in edition.equivalent_identifiers()
                  if x is not edition.primary_identifier]
        eq_(Identifier.ISBN, isbn.type)
        eq_(u'9780375504587', isbn.identifier)

        eq_(["McCain, John", "Salter, Mark"],
            sorted([x.sort_name for x in edition.contributors]),
        )

        subs = sorted(
            (x.subject.type, x.subject.identifier)
            for x in edition.primary_identifier.classifications
        )
        eq_([(Subject.BISAC, u'BIOGRAPHY & AUTOBIOGRAPHY / Political'),
             (Subject.FREEFORM_AUDIENCE, u'Adult')], subs)

        eq_(9, license_pool.licenses_owned)
        eq_(8, license_pool.licenses_available)
        eq_(0, license_pool.patrons_in_hold_queue)
        eq_(datetime.datetime(2015, 5, 20, 2, 9, 8), license_pool.last_checked)

        # Three circulation events were created, backdated to the
        # last_checked date of the license pool.
        events = license_pool.circulation_events
        eq_([u'distributor_title_add', u'distributor_check_in', u'distributor_license_add'],
            [x.type for x in events])
        for e in events:
            eq_(e.start, license_pool.last_checked)

        # A presentation-ready work has been created for the LicensePool.
        work = license_pool.work
        eq_(True, work.presentation_ready)
        eq_("Faith of My Fathers : A Family Memoir", work.title)

        # A CoverageRecord has been provided for this book in the Axis
        # 360 bibliographic coverage provider, so that in the future
        # it doesn't have to make a separate API request to ask about
        # this book.
        records = [x for x in license_pool.identifier.coverage_records
                   if x.data_source.name == DataSource.AXIS_360
                   and x.operation is None]
        eq_(1, len(records))

        # Now, another collection with the same book shows up.
        collection2 = MockAxis360API.mock_collection(self._db, "coll2")
        monitor = Axis360CirculationMonitor(
            self._db, collection2, api_class=MockAxis360API,
        )
        edition2, license_pool2 = monitor.process_book(
            self.BIBLIOGRAPHIC_DATA, self.AVAILABILITY_DATA)

        # Both license pools have the same Work and the same presentation
        # edition.
        eq_(license_pool.work, license_pool2.work)
        eq_(license_pool.presentation_edition, license_pool2.presentation_edition)

    def test_process_book_updates_old_licensepool(self):
        """If the LicensePool already exists, the circulation monitor
        updates it.
        """
        edition, licensepool = self._edition(
            with_license_pool=True, identifier_type=Identifier.AXIS_360_ID,
            identifier_id=u'0003642860'
        )
        # We start off with availability information based on the
        # default for test data.
        eq_(1, licensepool.licenses_owned)

        identifier = IdentifierData(
            type=licensepool.identifier.type,
            identifier=licensepool.identifier.identifier
        )
        metadata = Metadata(DataSource.AXIS_360, primary_identifier=identifier)
        monitor = Axis360CirculationMonitor(
            self._db, self.collection, api_class=MockAxis360API,
        )
        edition, licensepool = monitor.process_book(
            metadata, self.AVAILABILITY_DATA
        )

        # Now we have information based on the CirculationData.
        eq_(9, licensepool.licenses_owned)


class TestReaper(Axis360Test):

    def test_instantiate(self):
        # Validate the standard CollectionMonitor interface.
        monitor = AxisCollectionReaper(
            self._db, self.collection,
            api_class=MockAxis360API
        )

class TestParsers(Axis360Test):

    def test_bibliographic_parser(self):
        """Make sure the bibliographic information gets properly
        collated in preparation for creating Edition objects.
        """
        data = self.sample_data("tiny_collection.xml")

        [bib1, av1], [bib2, av2] = BibliographicParser(
            False, True).process_all(data)

        # We didn't ask for availability information, so none was provided.
        eq_(None, av1)
        eq_(None, av2)

        eq_(u'Faith of My Fathers : A Family Memoir', bib1.title)
        eq_('eng', bib1.language)
        eq_(datetime.datetime(2000, 3, 7, 0, 0), bib1.published)

        eq_(u'Simon & Schuster', bib2.publisher)
        eq_(u'Pocket Books', bib2.imprint)

        eq_(Edition.BOOK_MEDIUM, bib1.medium)

        # TODO: Would be nicer if we could test getting a real value
        # for this.
        eq_(None, bib2.series)

        # Book #1 has two links -- a description and a cover image.
        [description, cover] = bib1.links
        eq_(Hyperlink.DESCRIPTION, description.rel)
        eq_(Representation.TEXT_PLAIN, description.media_type)
        assert description.content.startswith(
            "John McCain's deeply moving memoir"
        )

        # The cover image simulates the current state of the B&T cover
        # service, where we get a thumbnail-sized image URL in the
        # Axis 360 API response and we can hack the URL to get the
        # full-sized image URL.
        eq_(LinkRelations.IMAGE, cover.rel)
        eq_("http://contentcafecloud.baker-taylor.com/Jacket.svc/D65D0665-050A-487B-9908-16E6D8FF5C3E/9780375504587/Large/Empty",
            cover.href)
        eq_(MediaTypes.JPEG_MEDIA_TYPE, cover.media_type)

        eq_(LinkRelations.THUMBNAIL_IMAGE, cover.thumbnail.rel)
        eq_("http://contentcafecloud.baker-taylor.com/Jacket.svc/D65D0665-050A-487B-9908-16E6D8FF5C3E/9780375504587/Medium/Empty",
            cover.thumbnail.href)
        eq_(MediaTypes.JPEG_MEDIA_TYPE, cover.thumbnail.media_type)


        # Book #1 has a primary author, another author and a narrator.
        #
        # TODO: The narrator data is simulated. we haven't actually
        # verified that Axis 360 sends narrator information in the
        # same format as author information.
        [cont1, cont2, narrator] = bib1.contributors
        eq_("McCain, John", cont1.sort_name)
        eq_([Contributor.PRIMARY_AUTHOR_ROLE], cont1.roles)

        eq_("Salter, Mark", cont2.sort_name)
        eq_([Contributor.AUTHOR_ROLE], cont2.roles)

        eq_("McCain, John S. III", narrator.sort_name)
        eq_([Contributor.NARRATOR_ROLE], narrator.roles)

        # Book #2 only has a primary author.
        [cont] = bib2.contributors
        eq_("Pollero, Rhonda", cont.sort_name)
        eq_([Contributor.PRIMARY_AUTHOR_ROLE], cont.roles)

        axis_id, isbn = sorted(bib1.identifiers, key=lambda x: x.identifier)
        eq_(u'0003642860', axis_id.identifier)
        eq_(u'9780375504587', isbn.identifier)

        # Check the subjects for #2 because it includes an audience,
        # unlike #1.
        subjects = sorted(bib2.subjects, key = lambda x: x.identifier)
        eq_([Subject.BISAC, Subject.BISAC, Subject.BISAC,
             Subject.AXIS_360_AUDIENCE], [x.type for x in subjects])
        general_fiction, women_sleuths, romantic_suspense = sorted([
            x.name for x in subjects if x.type==Subject.BISAC])
        eq_(u'FICTION / General', general_fiction)
        eq_(u'FICTION / Mystery & Detective / Women Sleuths', women_sleuths)
        eq_(u'FICTION / Romance / Suspense', romantic_suspense)

        [adult] = [x.identifier for x in subjects
                   if x.type==Subject.AXIS_360_AUDIENCE]
        eq_(u'General Adult', adult)

        # The second book has a cover image simulating some possible
        # future case, where B&T change their cover service so that
        # the size URL hack no longer works. In this case, we treat
        # the image URL as both the full-sized image and the
        # thumbnail.
        [cover] = bib2.links
        eq_(LinkRelations.IMAGE, cover.rel)
        eq_("http://some-other-server/image.jpg", cover.href)
        eq_(MediaTypes.JPEG_MEDIA_TYPE, cover.media_type)

        eq_(LinkRelations.THUMBNAIL_IMAGE, cover.thumbnail.rel)
        eq_("http://some-other-server/image.jpg", cover.thumbnail.href)
        eq_(MediaTypes.JPEG_MEDIA_TYPE, cover.thumbnail.media_type)

        # The first book is available in two formats -- "ePub" and "AxisNow"
        [adobe, axisnow] = bib1.circulation.formats
        eq_(Representation.EPUB_MEDIA_TYPE, adobe.content_type)
        eq_(DeliveryMechanism.ADOBE_DRM, adobe.drm_scheme)

        eq_(MediaTypes.UNZIPPED_EPUB_MEDIA_TYPE, axisnow.content_type)
        eq_(DeliveryMechanism.AXISNOW_DRM, axisnow.drm_scheme)

        # The second book is only available in 'Blio' format, which
        # we can't use.
        eq_([], bib2.circulation.formats)

    def test_bibliographic_parser_audiobook(self):
        # TODO - we need a real example to test from. The example we were
        # given is a hacked-up ebook. Ideally we would be able to check
        # narrator information here.
        data = self.sample_data("availability_with_audiobook_fulfillment.xml")

        [[bib, av]] = BibliographicParser(False, True).process_all(data)
        eq_("Back Spin", bib.title)
        eq_(Edition.AUDIO_MEDIUM, bib.medium)

        # The audiobook has one DeliveryMechanism, in which the Findaway licensing document
        # acts as both the content type and the DRM scheme.
        [findaway] = bib.circulation.formats
        eq_(None, findaway.content_type)
        eq_(DeliveryMechanism.FINDAWAY_DRM, findaway.drm_scheme)

        # Although the audiobook is also available in the "AxisNow"
        # format, no second delivery mechanism was created for it, the
        # way it would have been for an ebook.
        assert '<formatName>AxisNow</formatName>' in data

    def test_bibliographic_parser_unsupported_format(self):
        data = self.sample_data("availability_with_audiobook_fulfillment.xml")
        data = data.replace('Acoustik', 'Blio')

        [[bib, av]] = BibliographicParser(False, True).process_all(data)

        # We don't support the Blio format, but we know Blio titles
        # are ebooks, not audiobooks.
        eq_(Edition.BOOK_MEDIUM, bib.medium)

    def test_parse_author_role(self):
        """Suffixes on author names are turned into roles."""
        author = "Dyssegaard, Elisabeth Kallick (TRN)"
        parse = BibliographicParser.parse_contributor
        c = parse(author)
        eq_("Dyssegaard, Elisabeth Kallick", c.sort_name)
        eq_([Contributor.TRANSLATOR_ROLE], c.roles)

        # A corporate author is given a normal author role.
        author = "Bob, Inc. (COR)"
        c = parse(author, primary_author_found=False)
        eq_("Bob, Inc.", c.sort_name)
        eq_([Contributor.PRIMARY_AUTHOR_ROLE], c.roles)

        c = parse(author, primary_author_found=True)
        eq_("Bob, Inc.", c.sort_name)
        eq_([Contributor.AUTHOR_ROLE], c.roles)

        # An unknown author type is given an unknown role
        author = "Eve, Mallory (ZZZ)"
        c = parse(author, primary_author_found=False)
        eq_("Eve, Mallory", c.sort_name)
        eq_([Contributor.UNKNOWN_ROLE], c.roles)

        # force_role overwrites whatever other role might be
        # assigned.
        author = "Bob, Inc. (COR)"
        c = parse(author, primary_author_found=False,
                  force_role=Contributor.NARRATOR_ROLE)
        eq_([Contributor.NARRATOR_ROLE], c.roles)


    def test_availability_parser(self):
        """Make sure the availability information gets properly
        collated in preparation for updating a LicensePool.
        """

        data = self.sample_data("tiny_collection.xml")

        [bib1, av1], [bib2, av2] = BibliographicParser(
            True, False).process_all(data)

        # We didn't ask for bibliographic information, so none was provided.
        eq_(None, bib1)
        eq_(None, bib2)

        eq_("0003642860", av1.primary_identifier(self._db).identifier)
        eq_(9, av1.licenses_owned)
        eq_(9, av1.licenses_available)
        eq_(0, av1.patrons_in_hold_queue)


class BaseParserTest(object):

    @classmethod
    def sample_data(self, filename):
        return sample_data(filename, 'axis')


class TestResponseParser(BaseParserTest):

    def setup(self):
        # We don't need an actual Collection object to test most of
        # these classes, but we do need to test that whatever object
        # we _claim_ is a Collection will have its id put into the
        # right spot of HoldInfo and LoanInfo objects.
        class MockCollection(object):
            pass
        self._default_collection = MockCollection()
        self._default_collection.id = object()

class TestRaiseExceptionOnError(TestResponseParser):

    def test_internal_server_error(self):
        data = self.sample_data("internal_server_error.xml")
        parser = HoldReleaseResponseParser(None)
        assert_raises_regexp(
            RemoteInitiatedServerError, "Internal Server Error",
            parser.process_all, data
        )

    def test_internal_server_error(self):
        data = self.sample_data("invalid_error_code.xml")
        parser = HoldReleaseResponseParser(None)
        assert_raises_regexp(
            RemoteInitiatedServerError, "Invalid response code from Axis 360: abcd",
            parser.process_all, data
        )

    def test_missing_error_code(self):
        data = self.sample_data("missing_error_code.xml")
        parser = HoldReleaseResponseParser(None)
        assert_raises_regexp(
            RemoteInitiatedServerError, "No status code!",
            parser.process_all, data
        )


class TestCheckoutResponseParser(TestResponseParser):

    def test_parse_checkout_success(self):
        data = self.sample_data("checkout_success.xml")
        parser = CheckoutResponseParser(self._default_collection)
        parsed = parser.process_all(data)
        assert isinstance(parsed, LoanInfo)
        eq_(self._default_collection.id, parsed.collection_id)
        eq_(DataSource.AXIS_360, parsed.data_source_name)
        eq_(Identifier.AXIS_360_ID, parsed.identifier_type)
        eq_(datetime.datetime(2015, 8, 11, 18, 57, 42),
            parsed.end_date)

        # There is no FulfillmentInfo associated with the LoanInfo,
        # because we don't need it (checkout and fulfillment are
        # separate steps).
        eq_(parsed.fulfillment_info, None)

    def test_parse_already_checked_out(self):
        data = self.sample_data("already_checked_out.xml")
        parser = CheckoutResponseParser(None)
        assert_raises(AlreadyCheckedOut, parser.process_all, data)

    def test_parse_not_found_on_remote(self):
        data = self.sample_data("not_found_on_remote.xml")
        parser = CheckoutResponseParser(None)
        assert_raises(NotFoundOnRemote, parser.process_all, data)

class TestHoldResponseParser(TestResponseParser):

    def test_parse_hold_success(self):
        data = self.sample_data("place_hold_success.xml")
        parser = HoldResponseParser(self._default_collection)
        parsed = parser.process_all(data)
        assert isinstance(parsed, HoldInfo)
        eq_(1, parsed.hold_position)

        # The HoldInfo is given the Collection object we passed into
        # the HoldResponseParser.
        eq_(self._default_collection.id, parsed.collection_id)

    def test_parse_already_on_hold(self):
        data = self.sample_data("already_on_hold.xml")
        parser = HoldResponseParser(None)
        assert_raises(AlreadyOnHold, parser.process_all, data)

class TestHoldReleaseResponseParser(TestResponseParser):

    def test_success(self):
        data = self.sample_data("release_hold_success.xml")
        parser = HoldReleaseResponseParser(None)
        eq_(True, parser.process_all(data))

    def test_failure(self):
        data = self.sample_data("release_hold_failure.xml")
        parser = HoldReleaseResponseParser(None)
        assert_raises(NotOnHold, parser.process_all, data)

class TestAvailabilityResponseParser(Axis360Test, BaseParserTest):
    """Unlike other response parser tests, this one needs
    access to a real database session, because it needs a real Collection
    to put into its MockAxis360API.
    """

    def test_parse_loan_and_hold(self):
        data = self.sample_data("availability_with_loan_and_hold.xml")
        parser = AvailabilityResponseParser(self.api)
        activity = list(parser.process_all(data))
        hold, loan, reserved = sorted(activity, key=lambda x: x.identifier)
        eq_(self.api.collection.id, hold.collection_id)
        eq_(Identifier.AXIS_360_ID, hold.identifier_type)
        eq_("0012533119", hold.identifier)
        eq_(1, hold.hold_position)
        eq_(None, hold.end_date)

        eq_(self.api.collection.id, loan.collection_id)
        eq_("0015176429", loan.identifier)
        eq_("http://fulfillment/", loan.fulfillment_info.content_link)
        eq_(datetime.datetime(2015, 8, 12, 17, 40, 27), loan.end_date)

        eq_(self.api.collection.id, reserved.collection_id)
        eq_("1111111111", reserved.identifier)
        eq_(datetime.datetime(2015, 1, 1, 13, 11, 11), reserved.end_date)
        eq_(0, reserved.hold_position)

    def test_parse_loan_no_availability(self):
        data = self.sample_data("availability_without_fulfillment.xml")
        parser = AvailabilityResponseParser(self.api)
        [loan] = list(parser.process_all(data))

        eq_(self.api.collection.id, loan.collection_id)
        eq_("0015176429", loan.identifier)
        eq_(None, loan.fulfillment_info)
        eq_(datetime.datetime(2015, 8, 12, 17, 40, 27), loan.end_date)

    def test_parse_audiobook_fulfillmentinfo(self):
        data = self.sample_data("availability_with_audiobook_fulfillment.xml")
        parser = AvailabilityResponseParser(self.api)
        [loan] = list(parser.process_all(data))
        fulfillment = loan.fulfillment_info
        assert isinstance(fulfillment, AudiobookFulfillmentInfo)

        # The transaction ID is stored as the .key. If we actually
        # need to make a manifest for this audiobook, the key will be
        # used in one more API request. (See TestAudiobookFulfillmentInfo
        # for that.)
        eq_("C3F71F8D-1883-2B34-068F-96570678AEB0", fulfillment.key)

        # The API object is present in the FulfillmentInfo and ready to go.
        eq_(self.api, fulfillment.api)


class TestJSONResponseParser(object):

    def test__required_key(self):
        m = JSONResponseParser._required_key
        parsed = dict(key="value")

        # If the value is present, _required_key acts just like get().
        eq_("value", m("key", parsed))

        # If not, it raises a RemoteInitiatedServerError.
        assert_raises_regexp(
            RemoteInitiatedServerError,
            "Required key absent not present in Axis 360 fulfillment document: {'key': 'value'}",
            m, "absent", parsed
        )

    def test_verify_status_code(self):
        success = dict(Status=dict(Code=0000))
        failure = dict(Status=dict(Code=1000, Message="A message"))
        missing = dict()

        m = JSONResponseParser.verify_status_code

        # If the document's Status object indicates success, nothing
        # happens.
        m(success)

        # If it indicates failure, an appropriate exception is raised.
        assert_raises_regexp(
            PatronAuthorizationFailedException, "A message",
            m, failure
        )

        # If the Status object is missing, a more generic exception is
        # raised.
        assert_raises_regexp(
            RemoteInitiatedServerError,
            "Required key Status not present in Axis 360 fulfillment document",
            m, missing
        )

    def test_parse(self):

        class Mock(JSONResponseParser):

            def _parse(self, parsed, *args, **kwargs):
                self.called_with = parsed, args, kwargs
                return "success"

        parser = Mock(object())

        # Test success.
        doc = dict(Status=dict(Code=0000))

        # The JSON will be parsed and passed in to _parse(); all other
        # arguments to parse() will be passed through to _parse().
        result = parser.parse(json.dumps(doc), "value1", arg2="value2")
        eq_("success", result)
        eq_(
            (doc, ("value1",), dict(arg2="value2")),
            parser.called_with
        )

        # It also works if the JSON was already parsed.
        result = parser.parse(doc, "new_value")
        eq_(
            (doc, ("new_value",), {}), parser.called_with
        )

        # Non-JSON input causes an error.
        assert_raises_regexp(
            RemoteInitiatedServerError,
            "Invalid response from Axis 360 \(was expecting JSON\): I'm not JSON",
            parser.parse, "I'm not JSON"
        )



class TestAudiobookFulfillmentInfoResponseParser(Axis360Test):

    def test__parse(self):
        # _parse will create a valid FindawayManifest given a
        # complete document.

        parser = AudiobookFulfillmentInfoResponseParser(api=self.api)
        m = parser._parse

        edition, pool = self._edition(with_license_pool=True)
        def get_data():
            # We'll be modifying this document to simulate failures,
            # so make it easy to load a fresh copy.
            return json.loads(
                self.sample_data("audiobook_fulfillment_info.json")
            )

        # This is the data we just got from a call to Axis 360's
        # getfulfillmentInfo endpoint.
        data = get_data()

        # When we call _parse, the API is going to fire off an
        # additional request to the getaudiobookmetadata endpoint, so
        # it can create a complete FindawayManifest. Queue up the
        # response to that request.
        audiobook_metadata = self.sample_data("audiobook_metadata.json")
        self.api.queue_response(200, {}, audiobook_metadata)

        manifest, expires = m(data, pool)

        assert isinstance(manifest, FindawayManifest)
        metadata = manifest.metadata

        # The manifest contains information from the LicensePool's presentation
        # edition
        eq_(edition.title, metadata['title'])

        # It contains DRM licensing information from Findaway via the
        # Axis 360 API.
        encrypted = metadata['encrypted']
        eq_(
            '0f547af1-38c1-4b1c-8a1a-169d353065d0',
            encrypted['findaway:sessionKey']
        )
        eq_('5babb89b16a4ed7d8238f498', encrypted['findaway:checkoutId'])
        eq_('04960', encrypted['findaway:fulfillmentId'])
        eq_('58ee81c6d3d8eb3b05597cdc', encrypted['findaway:licenseId'])

        # The spine items and duration have been filled in by the call to
        # the getaudiobookmetadata endpoint.
        eq_(8150.87, metadata['duration'])
        eq_(5, len(manifest.readingOrder))

        # We also know when the licensing document expires.
        eq_(datetime.datetime(2018, 9, 29, 18, 34), expires)

        # Now strategically remove required information from the
        # document and verify that extraction fails.
        #
        for field in (
                'FNDContentID', 'FNDLicenseID', 'FNDSessionKey',
                'ExpirationDate'
        ):
            missing_field = get_data()
            del missing_field[field]
            assert_raises_regexp(
                RemoteInitiatedServerError,
                "Required key %s not present" % field,
                m, missing_field, pool
            )

        # Try with a bad expiration date.
        bad_date = get_data()
        bad_date['ExpirationDate'] = 'not-a-date'
        assert_raises_regexp(
            RemoteInitiatedServerError,
            "Could not parse expiration date: not-a-date",
            m, bad_date, pool
        )


class TestAudiobookMetadataParser(Axis360Test):

    def test__parse(self):
        # _parse will find the Findaway account ID and
        # the spine items.
        class Mock(AudiobookMetadataParser):
            @classmethod
            def _extract_spine_item(self, part):
                return part + " (extracted)"

        metadata = dict(
            fndaccountid="An account ID",
            readingOrder=["Spine item 1", "Spine item 2"]
        )
        account_id, spine_items = Mock(None)._parse(metadata)

        eq_("An account ID", account_id)
        eq_(["Spine item 1 (extracted)",
             "Spine item 2 (extracted)"],
            spine_items
        )

        # No data? Nothing will be parsed.
        account_id, spine_items = Mock(None)._parse({})
        eq_(None, account_id)
        eq_([], spine_items)

    def test__extract_spine_item(self):
        # _extract_spine_item will turn data from Findaway into
        # a SpineItem object.
        m = AudiobookMetadataParser._extract_spine_item
        item = m(
            dict(duration=100.4, fndpart=2, fndsequence=3,
                 title="The Gathering Storm"
            )
        )
        assert isinstance(item, SpineItem)
        eq_("The Gathering Storm", item.title)
        eq_(2, item.part)
        eq_(3, item.sequence)
        eq_(100.4, item.duration)
        eq_(Representation.MP3_MEDIA_TYPE, item.media_type)

        # We get a SpineItem even if all the data about the spine item
        # is missing -- these are the default values.
        item = m({})
        eq_(None, item.title)
        eq_(0, item.part)
        eq_(0, item.sequence)
        eq_(0, item.duration)
        eq_(Representation.MP3_MEDIA_TYPE, item.media_type)


class TestEbookFulfillmentInfo(Axis360Test):
    """An EbookFulfillmentInfo contains all information necessary to
    fulfill an ebook in any supported format -- but it only exposes
    one format at a time.
    """

    def setup(self):
        super(TestEbookFulfillmentInfo, self).setup()

        # For formats other than AxisNow
        self.content_link = "http://content/"
        self.content_type = "content/type"

        # For the AxisNow format we need two extra pieces of
        # information -- the book vault ID and the ISBN. Both of these
        # pieces come directly from Axis 360.
        self.book_vault_id = "a-book-vault-id"
        self.isbn = "an-isbn"

        self.identifier = self._identifier()

        self.info = EbookFulfillmentInfo(
            collection=self._default_collection, data_source_name=DataSource.AXIS_360,
            identifier_type=self.identifier.type, identifier=self.identifier.identifier,
            content_link=self.content_link, content_type=self.content_type,
            content="some content", content_expires=object(),
            book_vault_id=self.book_vault_id, isbn=self.isbn
        )

    def test_axisnow_manifest_document(self):
        manifest_document = self.info.axisnow_manifest_document
        parsed = json.loads(manifest_document)

        links = sorted(
            parsed.pop("links"), key=lambda x: x['rel']
        )
        eq_({}, parsed)

        eq_(["container", "encryption", "license"], [x['rel'] for x in links])
        container, encryption, license = links

        container, encryption, license = [x['href'] for x in links]
        eq_("https://node.axisnow.com/content/stream/an-isbn/META-INF/container.xml", container)
        eq_("https://node.axisnow.com/content/stream/an-isbn/META-INF/encryption.xml", encryption)

        # The license 'link' is a URI Template, not a URL.
        eq_(
            "https://node.axisnow.com/license/a-book-vault-id/{deviceId}/{clientIp}/an-isbn/{modulus}/{exponent}",
            license
        )
        eq_(True, links[-1]['templated'])

    def test_configure_for_internal_format(self):

        # By default, we're configured to send the content link.
        eq_(self.content_link, self.info.content_link)
        eq_(self.content_type, self.info.content_type)

        # info.content is redundant and will soon be destroyed.
        eq_("some content", self.info.content)

        # Configure for the "AxisNow" format, and we stop sending the link
        # and start sending an AxisNow manifest document.
        self.info.configure_for_internal_format(Axis360API.AXISNOW)
        eq_(MediaTypes.AXISNOW_MANIFEST_MEDIA_TYPE, self.info.content_type)
        eq_(self.info.axisnow_manifest_document, self.info.content)
        eq_(None, self.info.content_link)

        # Reconfigure to send out the content link again. We get the
        # old behavior back, except that info.content (which was
        # redundant) has been cleared out.
        self.info.configure_for_internal_format("ePub")
        eq_(self.content_link, self.info.content_link)
        eq_(self.content_type, self.info.content_type)
        eq_(None, self.info.content)


class TestAudiobookFulfillmentInfo(Axis360Test):
    def test_fetch(self):

        fulfillment_info = self.sample_data("audiobook_fulfillment_info.json")
        self.api.queue_response(200, {}, fulfillment_info)

        metadata = self.sample_data("audiobook_metadata.json")
        self.api.queue_response(200, {}, metadata)

        # Setup.
        edition, pool = self._edition(with_license_pool=True)
        identifier = pool.identifier
        fulfillment = AudiobookFulfillmentInfo(
            self.api, pool.data_source.name,
            identifier.type, identifier.identifier, 'transaction_id'
        )
        eq_(None, fulfillment._content_type)

        # Turn the crank.
        fulfillment.fetch()

        # The AudiobookFufillmentInfo now contains a Findaway manifest
        # document.
        eq_(DeliveryMechanism.FINDAWAY_DRM, fulfillment.content_type)
        assert isinstance(fulfillment.content, unicode)

        # The manifest document combines information from the
        # fulfillment document and the metadata document.
        for required in (
            '"findaway:sessionKey": "0f547af1-38c1-4b1c-8a1a-169d353065d0"',
            '"duration": 8150.87',
        ):
            assert required in fulfillment.content

        # The content expiration date also comes from the fulfillment
        # document.
        eq_(
            datetime.datetime(2018, 9, 29, 18, 34), fulfillment.content_expires
        )

class TestAxis360BibliographicCoverageProvider(Axis360Test):
    """Test the code that looks up bibliographic information from Axis 360."""

    def setup(self):
        super(TestAxis360BibliographicCoverageProvider, self).setup()
        self.provider = Axis360BibliographicCoverageProvider(
            self.collection, api_class=MockAxis360API
        )
        self.api = self.provider.api

    def test_script_instantiation(self):
        """Test that RunCoverageProviderScript can instantiate
        the coverage provider.
        """
        script = RunCollectionCoverageProviderScript(
            Axis360BibliographicCoverageProvider, self._db,
            api_class=MockAxis360API
        )
        [provider] = script.providers
        assert isinstance(provider, Axis360BibliographicCoverageProvider)
        assert isinstance(provider.api, MockAxis360API)

    def test_process_item_creates_presentation_ready_work(self):
        """Test the normal workflow where we ask Axis for data,
        Axis provides it, and we create a presentation-ready work.
        """
        data = self.sample_data("single_item.xml")
        self.api.queue_response(200, content=data)

        # Here's the book mentioned in single_item.xml.
        identifier = self._identifier(identifier_type=Identifier.AXIS_360_ID)
        identifier.identifier = '0003642860'

        # This book has no LicensePool.
        eq_([], identifier.licensed_through)

        # Run it through the Axis360BibliographicCoverageProvider
        [result] = self.provider.process_batch([identifier])
        eq_(identifier, result)

        # A LicensePool was created. We know both how many copies of this
        # book are available, and what formats it's available in.
        [pool] = identifier.licensed_through
        eq_(9, pool.licenses_owned)
        [lpdm] = pool.delivery_mechanisms
        eq_('application/epub+zip (application/vnd.adobe.adept+xml)',
            lpdm.delivery_mechanism.name)

        # A Work was created and made presentation ready.
        eq_('Faith of My Fathers : A Family Memoir', pool.work.title)
        eq_(True, pool.work.presentation_ready)

    def test_transient_failure_if_requested_book_not_mentioned(self):
        """Test an unrealistic case where we ask Axis 360 about one book and
        it tells us about a totally different book.
        """
        # We're going to ask about abcdef
        identifier = self._identifier(identifier_type=Identifier.AXIS_360_ID)
        identifier.identifier = 'abcdef'

        # But we're going to get told about 0003642860.
        data = self.sample_data("single_item.xml")
        self.api.queue_response(200, content=data)

        [result] = self.provider.process_batch([identifier])

        # Coverage failed for the book we asked about.
        assert isinstance(result, CoverageFailure)
        eq_(identifier, result.obj)
        eq_("Book not in collection", result.exception)

        # And nothing major was done about the book we were told
        # about. We created an Identifier record for its identifier,
        # but no LicensePool or Edition.
        wrong_identifier = Identifier.for_foreign_id(
            self._db, Identifier.AXIS_360_ID, "0003642860"
        )
        eq_([], identifier.licensed_through)
        eq_([], identifier.primarily_identifies)
