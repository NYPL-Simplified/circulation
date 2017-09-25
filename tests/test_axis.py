import datetime
from lxml import etree
from StringIO import StringIO
from nose.tools import (
    eq_, 
    assert_raises,
    assert_raises_regexp,
    set_trace,
)

from core.model import (
    ConfigurationSetting,
    DataSource,
    Edition,
    ExternalIntegration,
    Identifier,
    Subject,
    Contributor,
    LicensePool,
    create,
)

from core.metadata_layer import (
    Metadata,
    CirculationData,
    IdentifierData,
    ContributorData,
    SubjectData,
)

from core.opds_import import(
    MockMetadataWranglerOPDSLookup
)

from api.axis import (
    AxisCollectionReaper,
    Axis360CirculationMonitor,
    Axis360API,
    AvailabilityResponseParser,
    CheckoutResponseParser,
    HoldResponseParser,
    HoldReleaseResponseParser,
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

from core.analytics import Analytics


class Axis360Test(DatabaseTest):

    def setup(self):
        super(Axis360Test,self).setup()
        self.collection = MockAxis360API.mock_collection(self._db)
        self.api = MockAxis360API(self._db, self.collection)

    @classmethod
    def sample_data(self, filename):
        return sample_data(filename, 'axis')

        
class TestAxis360API(Axis360Test):

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

class TestCirculationMonitor(Axis360Test):

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

    def test_process_book(self):
        integration, ignore = create(
            self._db, ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol="core.local_analytics_provider",
        )

        monitor = Axis360CirculationMonitor(
            self._db, self.collection, api_class=MockAxis360API,
            metadata_client=MockMetadataWranglerOPDSLookup('url')
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
            metadata_client=MockMetadataWranglerOPDSLookup('url')
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
        

class TestResponseParser(object):

    @classmethod
    def sample_data(self, filename):
        return sample_data(filename, 'axis')

    def setup(self):
        # We don't need an actual Collection object to test this
        # class, but we do need to test that whatever object we
        # _claim_ is a Collection will have its id put into the 
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

        assert isinstance(parsed.fulfillment_info, FulfillmentInfo)
        eq_("http://axis360api.baker-taylor.com/Services/VendorAPI/GetAxisDownload/v2?blahblah", 
            parsed.fulfillment_info.content_link)


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

class TestAvailabilityResponseParser(TestResponseParser):

    def test_parse_loan_and_hold(self):
        data = self.sample_data("availability_with_loan_and_hold.xml")
        parser = AvailabilityResponseParser(self._default_collection)
        activity = list(parser.process_all(data))
        hold, loan, reserved = sorted(activity, key=lambda x: x.identifier)
        eq_(self._default_collection.id, hold.collection_id)
        eq_(Identifier.AXIS_360_ID, hold.identifier_type)
        eq_("0012533119", hold.identifier)
        eq_(1, hold.hold_position)
        eq_(None, hold.end_date)

        eq_(self._default_collection.id, loan.collection_id)
        eq_("0015176429", loan.identifier)
        eq_("http://fulfillment/", loan.fulfillment_info.content_link)
        eq_(datetime.datetime(2015, 8, 12, 17, 40, 27), loan.end_date)

        eq_(self._default_collection.id, reserved.collection_id)
        eq_("1111111111", reserved.identifier)
        eq_(datetime.datetime(2015, 1, 1, 13, 11, 11), reserved.end_date)
        eq_(0, reserved.hold_position)

    def test_parse_loan_no_availability(self):
        data = self.sample_data("availability_without_fulfillment.xml")
        parser = AvailabilityResponseParser(self._default_collection)
        [loan] = list(parser.process_all(data))

        eq_(self._default_collection.id, loan.collection_id)
        eq_("0015176429", loan.identifier)
        eq_(None, loan.fulfillment_info)
        eq_(datetime.datetime(2015, 8, 12, 17, 40, 27), loan.end_date)
