import datetime
import json
from lxml import etree
from StringIO import StringIO
from nose.tools import (
    eq_, 
    assert_raises,
    assert_raises_regexp,
    set_trace,
)

from core.model import (
    DataSource,
    Edition,
    Identifier,
    Subject,
    Contributor,
    LicensePool,
)

from core.metadata_layer import (
    Metadata,
    CirculationData,
    IdentifierData,
    ContributorData,
    SubjectData,
)

from api.axis import (
    Axis360CirculationMonitor,
    Axis360API,
    AxisCollectionReaper,
    AvailabilityResponseParser,
    CheckoutResponseParser,
    HoldResponseParser,
    HoldReleaseResponseParser,
    MockAxis360API,
    ResponseParser,
    TitleLicenseResponseParser,
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


class UsesSampleData(object):
    @classmethod
    def sample_data(self, filename):
        return sample_data(filename, 'axis')


class TestAxis360API(DatabaseTest, UsesSampleData):

    def setup(self):
        super(TestAxis360API,self).setup()
        self.api = MockAxis360API(self._db)

    def test_update_availability(self):
        """Test the Axis 360 implementation of the update_availability method
        defined by the CirculationAPI interface.
        """

        # Create a LicensePool that needs updating.
        edition, pool = self._edition(
            identifier_type=Identifier.AXIS_360_ID,
            data_source_name=DataSource.AXIS_360,
            with_license_pool=True
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
        with temp_config() as config:
            config['default_notification_email_address'] = "notifications@example.com"
            response = self.api.place_hold(patron, 'pin', pool, None)
            eq_(1, response.hold_position)
            eq_(response.identifier_type, pool.identifier.type)
            eq_(response.identifier, pool.identifier.identifier)
            [request] = self.api.requests
            params = request[-1]['params']
            eq_('notifications@example.com', params['email'])

    def test_licensing_changes(self):
        data = self.sample_data("licenses_with_titles.json")
        self.api.queue_response(200, content=data)
        since = datetime.datetime.utcnow()

        result = self.api.licensing_changes(since=since)
        expect = ([u'0012436005'], [u'0012436003'])
        eq_(expect, result)

        [request] = self.api.requests
        (url, args, kwargs) = request

        # The request went to the right URL.
        eq_('http://axis.test/titleLicense/v2', url)

        # The date we passed in as `since` became the `modifiedSince`
        # query parameter and was passed in to `request()`.
        formatted_date = since.strftime(self.api.ISO_DATE_FORMAT)
        eq_(formatted_date, kwargs['params']['modifiedSince'])

    def test_reap_license_pool_for(self):

        has_no_pool = self._identifier()
        edition, pool = self._edition(with_license_pool=True)
        pool.licenses_owned = 10
        pool.licenses_available = 10
        pool.patrons_in_hold_queue = 10
        pool.licenses_reserved = 10

        # If an Identifier has no LicensePool, reap_license_pool does nothing.
        self.api.reap_license_pool_for(None)
        self.api.reap_license_pool_for(has_no_pool)

        eq_(10, pool.licenses_owned)

        # If an Identifier does have a LicensePool, reap_license_pool
        # clears it out of the collection.
        self.api.reap_license_pool_for(pool.identifier)
        eq_(0, pool.licenses_owned)
        eq_(0, pool.licenses_available)
        eq_(0, pool.licenses_reserved)
        eq_(0, pool.patrons_in_hold_queue)
        

class TestCirculationMonitor(DatabaseTest, UsesSampleData):

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
        config = {
            Configuration.POLICIES: {
                Configuration.ANALYTICS_POLICY: ["core.local_analytics_provider"]
            }
        }
        with temp_config() as config:
            Analytics.initialize(
                ['core.local_analytics_provider'], config
            )
            monitor = Axis360CirculationMonitor(self._db)
            monitor.api = None
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
        monitor = Axis360CirculationMonitor(self._db)
        monitor.api = None
        edition, licensepool = monitor.process_book(
            metadata, self.AVAILABILITY_DATA
        )

        # Now we have information based on the CirculationData.
        eq_(9, licensepool.licenses_owned)

class TestAxisCollectionReaper(DatabaseTest, UsesSampleData):

    def setup(self):
        super(TestAxisCollectionReaper,self).setup()
        self.api = MockAxis360API(self._db)
    
    def test_run_once(self):

        # Unbeknownst to us, this LicensePool has been removed from
        # the collection.
        edition = self._edition(
            data_source_name=DataSource.AXIS_360,
            identifier_type=Identifier.AXIS_360_ID,
            identifier_id=u'0012436003'
        )
        pool = self._licensepool(
            edition=edition, data_source_name=DataSource.AXIS_360
        )
        pool.licenses_owned = 10
        pool.licenses_available = 10
        
        data = self.sample_data("licenses_with_titles.json")
        self.api.queue_response(200, content=data)
        
        # Let's face the music.
        reaper = AxisCollectionReaper(self._db)
        now = datetime.datetime.utcnow()
        reaper.run_once(now, None, self.api)

        # It's gone!
        eq_(0, pool.licenses_owned)
        eq_(0, pool.licenses_available)

        
class TestRaiseExceptionOnError(UsesSampleData):

    def test_internal_server_error(self):
        data = self.sample_data("internal_server_error.xml")
        parser = HoldReleaseResponseParser()
        assert_raises_regexp(
            RemoteInitiatedServerError, "Internal Server Error", 
            parser.process_all, data
        )

    def test_internal_server_error(self):
        data = self.sample_data("invalid_error_code.xml")
        parser = HoldReleaseResponseParser()
        assert_raises_regexp(
            RemoteInitiatedServerError, "Invalid response code from Axis 360: abcd", 
            parser.process_all, data
        )

    def test_missing_error_code(self):
        data = self.sample_data("missing_error_code.xml")
        parser = HoldReleaseResponseParser()
        assert_raises_regexp(
            RemoteInitiatedServerError, "No status code!", 
            parser.process_all, data
        )


class TestCheckoutResponseParser(UsesSampleData):

    def test_parse_checkout_success(self):
        data = self.sample_data("checkout_success.xml")
        parser = CheckoutResponseParser()
        parsed = parser.process_all(data)
        assert isinstance(parsed, LoanInfo)
        eq_(Identifier.AXIS_360_ID, parsed.identifier_type)
        eq_(datetime.datetime(2015, 8, 11, 18, 57, 42), 
            parsed.end_date)

        assert isinstance(parsed.fulfillment_info, FulfillmentInfo)
        eq_("http://axis360api.baker-taylor.com/Services/VendorAPI/GetAxisDownload/v2?blahblah", 
            parsed.fulfillment_info.content_link)

    def test_parse_already_checked_out(self):
        data = self.sample_data("already_checked_out.xml")
        parser = CheckoutResponseParser()
        assert_raises(AlreadyCheckedOut, parser.process_all, data)

    def test_parse_not_found_on_remote(self):
        data = self.sample_data("not_found_on_remote.xml")
        parser = CheckoutResponseParser()
        assert_raises(NotFoundOnRemote, parser.process_all, data)

class TestHoldResponseParser(UsesSampleData):

    def test_parse_hold_success(self):
        data = self.sample_data("place_hold_success.xml")
        parser = HoldResponseParser()
        parsed = parser.process_all(data)
        assert isinstance(parsed, HoldInfo)
        eq_(1, parsed.hold_position)

    def test_parse_already_on_hold(self):
        data = self.sample_data("already_on_hold.xml")
        parser = HoldResponseParser()
        assert_raises(AlreadyOnHold, parser.process_all, data)

class TestHoldReleaseResponseParser(UsesSampleData):

    def test_success(self):
        data = self.sample_data("release_hold_success.xml")
        parser = HoldReleaseResponseParser()
        eq_(True, parser.process_all(data))

    def test_failure(self):
        data = self.sample_data("release_hold_failure.xml")
        parser = HoldReleaseResponseParser()
        assert_raises(NotOnHold, parser.process_all, data)

class TestAvailabilityResponseParser(UsesSampleData):

    def test_parse_loan_and_hold(self):
        data = self.sample_data("availability_with_loan_and_hold.xml")
        parser = AvailabilityResponseParser()
        activity = list(parser.process_all(data))
        hold, loan, reserved = sorted(activity, key=lambda x: x.identifier)
        eq_(Identifier.AXIS_360_ID, hold.identifier_type)
        eq_("0012533119", hold.identifier)
        eq_(1, hold.hold_position)
        eq_(None, hold.end_date)

        eq_("0015176429", loan.identifier)
        eq_("http://fulfillment/", loan.fulfillment_info.content_link)
        eq_(datetime.datetime(2015, 8, 12, 17, 40, 27), loan.end_date)

        eq_("1111111111", reserved.identifier)
        eq_(datetime.datetime(2015, 1, 1, 13, 11, 11), reserved.end_date)
        eq_(0, reserved.hold_position)

    def test_parse_loan_no_availability(self):
        data = self.sample_data("availability_without_fulfillment.xml")
        parser = AvailabilityResponseParser()
        [loan] = list(parser.process_all(data))

        eq_("0015176429", loan.identifier)
        eq_(None, loan.fulfillment_info)
        eq_(datetime.datetime(2015, 8, 12, 17, 40, 27), loan.end_date)


class TestTitleLicenseResponseParser(UsesSampleData):

    def test_not_json(self):
        data = "This is not JSON"
        parser = TitleLicenseResponseParser()
        assert_raises_regexp(
            RemoteInitiatedServerError, "Bad response: This is not JSON",
            parser.process_all, data
        )

    def test_not_a_dict(self):
        data = '"foo"'
        parser = TitleLicenseResponseParser()
        assert_raises_regexp(
            RemoteInitiatedServerError, 'Bad response: "foo"',
            parser.process_all, data
        )
        
    def test_no_status_code(self):
        data = '{}'
        parser = TitleLicenseResponseParser()
        assert_raises_regexp(
            RemoteInitiatedServerError, "No status code: {}",
            parser.process_all, data
        )

    def test_failure_condition(self):
        data = self.sample_data("server_error.json")
        parser = TitleLicenseResponseParser()
        assert_raises_regexp(
            RemoteInitiatedServerError, "Internal Server Error",
            parser.process_all, data
        )

    def test_no_titles(self):
        data = self.sample_data("licenses_without_titles.json")
        parser = TitleLicenseResponseParser()
        eq_(
            ([], []), parser.process_all(data)
        )
        
    def test_some_titles(self):
        data = self.sample_data("licenses_with_titles.json")
        parser = TitleLicenseResponseParser()
        added, removed = parser.process_all(data)

        # One title was added, one removed.
        eq_([u'0012436005'], added)
        eq_([u'0012436003'], removed)
