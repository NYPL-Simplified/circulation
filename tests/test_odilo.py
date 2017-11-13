# encoding: utf-8
import json

from nose.tools import (
    eq_, ok_, assert_raises,
)

from api.odilo import (
    MockOdiloAPI,
    RecentOdiloCollectionMonitor,
    FullOdiloCollectionMonitor
)

from api.circulation import (
    CirculationAPI,
)

from api.circulation_exceptions import *

from . import (
    DatabaseTest,
    sample_data
)

from core.model import (
    DataSource,
    ExternalIntegration,
    Identifier,
)


class OdiloAPITest(DatabaseTest):
    PATRON = '0001000265'
    PIN = 'c4ca4238a0b923820dcc509a6f75849b'
    RECORD_ID = '00010982'

    def setup(self):
        super(OdiloAPITest, self).setup()
        library = self._default_library
        self.collection = MockOdiloAPI.mock_collection(self._db)
        self.circulation = CirculationAPI(
            self._db, library, api_map={ExternalIntegration.ODILO: MockOdiloAPI}
        )
        self.api = self.circulation.api_for_collection[self.collection.id]

        self.edition, self.licensepool = self._edition(
            data_source_name=DataSource.ODILO,
            identifier_type=Identifier.ODILO_ID,
            collection=self.collection,
            identifier_id=self.RECORD_ID,
            with_license_pool=True
        )

    @classmethod
    def sample_data(cls, filename):
        return sample_data(filename, 'odilo')

    @classmethod
    def sample_json(cls, filename):
        data = cls.sample_data(filename)
        return data, json.loads(data)

    def error_message(self, error_code, message=None, token=None):
        """Create a JSON document that simulates the message served by
        Odilo given a certain error condition.
        """
        message = message or self._str
        token = token or self._str
        data = dict(errorCode=error_code, message=message, token=token)
        return json.dumps(data)


class TestOdiloCirculationAPI(OdiloAPITest):
    #################
    # General tests
    #################

    # Test 404 Not Found --> patron not found --> 'patronNotFound'
    def test_01_patron_not_found(self):
        patron_not_found_data, patron_not_found_json = self.sample_json("error_patron_not_found.json")
        self.api.queue_response(404, content=patron_not_found_json)

        assert_raises(PatronNotFoundOnRemote, self.api.checkout, '123456789', self.PIN, self.licensepool, 'ACSM')
        print 'Test patron not found ok!'

    # Test 404 Not Found --> record not found --> 'ERROR_DATA_NOT_FOUND'
    def test_02_data_not_found(self):
        data_not_found_data, data_not_found_json = self.sample_json("error_data_not_found.json")
        self.api.queue_response(404, content=data_not_found_json)

        self.licensepool.identifier.identifier = '12345678'
        assert_raises(NotFoundOnRemote, self.api.checkout, self.PATRON, self.PIN, self.licensepool, 'ACSM')
        print 'Test resource not found on remote ok!'

    #################
    # Checkout tests
    #################

    # Test 400 Bad Request --> Invalid format for that resource
    def test_11(self):
        self.api.queue_response(400, content="")
        assert_raises(NoAcceptableFormat, self.api.checkout, self.PATRON, self.PIN, self.licensepool, 'FAKE_FORMAT')
        print 'Test invalid format for resource ok!'

    def test_12_checkout_acsm(self):
        checkout_data, checkout_json = self.sample_json("checkout_acsm_ok.json")
        self.api.queue_response(200, content=checkout_json)
        self.checkout('ACSM')

    def test_13_checkout_ebook_streaming_epub(self):
        checkout_data, checkout_json = self.sample_json("checkout_ebook_streaming_ok_epub.json")
        self.api.queue_response(200, content=checkout_json)
        self.checkout('EBOOK_STREAMING')

    def test_14_checkout_ebook_streaming_pdf(self):
        self.api.queue_response(400, content="")

        checkout_data, checkout_json = self.sample_json("checkout_ebook_streaming_ok_pdf.json")
        self.api.queue_response(200, content=checkout_json)

        self.checkout('EBOOK_STREAMING')

    def checkout(self, internal_format):
        print('Checkout test ' + internal_format)

        loan_info = self.api.checkout(self.PATRON, self.PIN, self.licensepool, internal_format)
        ok_(loan_info, msg="LoanInfo null --> checkout failed!")
        print 'Loan ok: %s' % loan_info.identifier

    #################
    # Fulfill tests
    #################

    def test_21_fulfill_acsm(self):
        checkout_data, checkout_json = self.sample_json("patron_checkouts.json")
        self.api.queue_response(200, content=checkout_json)

        acsm_data = self.sample_data("fulfill_ok_acsm.acsm")
        self.api.queue_response(200, content=acsm_data)

        self.fulfill('ACSM')

    def test_22_fulfill_ebook_streaming(self):
        checkout_data, checkout_json = self.sample_json("patron_checkouts.json")
        self.api.queue_response(200, content=checkout_json)

        self.licensepool.identifier.identifier = '00011055'
        self.fulfill('EBOOK_STREAMING')

    def fulfill(self, internal_format):
        print('Fulfill test: ' + internal_format)

        fulfillment_info = self.api.fulfill(self.PATRON, self.PIN, self.licensepool, internal_format)
        ok_(fulfillment_info, msg='Cannot Fulfill !!')

        if fulfillment_info.content_link:
            print 'Fulfill link: %s' % fulfillment_info.content_link
        if fulfillment_info.content:
            print 'Fulfill content: %s' % fulfillment_info.content

    #################
    # Hold tests
    #################

    def test_31_already_on_hold(self):
        already_on_hold_data, already_on_hold_json = self.sample_json("error_hold_already_in_hold.json")
        self.api.queue_response(403, content=already_on_hold_json)

        assert_raises(AlreadyOnHold, self.api.place_hold, self.PATRON, self.PIN, self.licensepool,
                      'ejcepas@odilotid.es')

        print 'Test hold already on hold ok!'

    def test_32_place_hold(self):
        print('Place hold test...')
        hold_ok_data, hold_ok_json = self.sample_json("place_hold_ok.json")
        self.api.queue_response(200, content=hold_ok_json)

        hold_info = self.api.place_hold(self.PATRON, self.PIN, self.licensepool, 'ejcepas@odilotid.es')
        ok_(hold_info, msg="HoldInfo null --> place hold failed!")
        print 'Hold ok: %s' % hold_info.identifier

    #################
    # Patron Activity tests
    #################

    def test_41_patron_activity_invalid_patron(self):
        patron_not_found_data, patron_not_found_json = self.sample_json("error_patron_not_found.json")
        self.api.queue_response(404, content=patron_not_found_json)

        assert_raises(PatronNotFoundOnRemote, self.api.patron_activity, self.PATRON, self.PIN)

        print 'Test patron activity --> invalid partron ok!'

    def test_42_patron_activity(self):
        print ('Patron activity test...')
        patron_checkouts_data, patron_checkouts_json = self.sample_json("patron_checkouts.json")
        patron_holds_data, patron_holds_json = self.sample_json("patron_holds.json")
        self.api.queue_response(200, content=patron_checkouts_json)
        self.api.queue_response(200, content=patron_holds_json)

        loans_and_holds = self.api.patron_activity(self.PATRON, self.PIN)
        if loans_and_holds:
            print 'Found: %i loans and holds' % len(loans_and_holds)
        else:
            print 'No loans or holds found'

    #################
    # Checkin tests
    #################

    def test_51_checkin_patron_not_found(self):
        patron_not_found_data, patron_not_found_json = self.sample_json("error_patron_not_found.json")
        self.api.queue_response(404, content=patron_not_found_json)

        assert_raises(PatronNotFoundOnRemote, self.api.checkin, self.PATRON, self.PIN, self.licensepool)

        print 'Test checkin --> invalid patron ok!'

    def test_52_checkin_checkout_not_found(self):
        checkout_not_found_data, checkout_not_found_json = self.sample_json("error_checkout_not_found.json")
        self.api.queue_response(404, content=checkout_not_found_json)

        assert_raises(NotCheckedOut, self.api.checkin, self.PATRON, self.PIN, self.licensepool)

        print 'Test checkin --> invalid checkout ok!'

    def test_53_checkin(self):
        print('Checkin test...')
        checkout_data, checkout_json = self.sample_json("patron_checkouts.json")
        self.api.queue_response(200, content=checkout_json)

        checkin_data, checkin_json = self.sample_json("checkin_ok.json")
        self.api.queue_response(200, content=checkin_json)

        response = self.api.checkin(self.PATRON, self.PIN, self.licensepool)
        eq_(response.status_code, 200,
            msg="Response code != 200, cannot perform checkin for record: " + self.licensepool.identifier.identifier
                + " patron: " + self.PATRON)

        checkout_returned = response.json()
        print 'Checkout returned: %s' % checkout_returned['id']

    #################
    # Patron Activity tests
    #################

    def test_61_return_hold_patron_not_found(self):
        patron_not_found_data, patron_not_found_json = self.sample_json("error_patron_not_found.json")
        self.api.queue_response(404, content=patron_not_found_json)

        assert_raises(PatronNotFoundOnRemote, self.api.release_hold, self.PATRON, self.PIN, self.licensepool)

        print 'Test release hold --> invalid patron ok!'

    def test_62_return_hold_not_found(self):
        holds_data, holds_json = self.sample_json("patron_holds.json")
        self.api.queue_response(200, content=holds_json)

        checkin_data, checkin_json = self.sample_json("error_hold_not_found.json")
        self.api.queue_response(404, content=checkin_json)

        response = self.api.release_hold(self.PATRON, self.PIN, self.licensepool)
        eq_(response, True,
            msg="Cannot release hold, response false " + self.licensepool.identifier.identifier + " patron: "
                + self.PATRON)

        print 'Hold returned: %s' % self.licensepool.identifier.identifier

    def test_63_return_hold(self):
        print('Return hold test...')

        holds_data, holds_json = self.sample_json("patron_holds.json")
        self.api.queue_response(200, content=holds_json)

        release_hold_ok_data, release_hold_ok_json = self.sample_json("release_hold_ok.json")
        self.api.queue_response(200, content=release_hold_ok_json)

        response = self.api.release_hold(self.PATRON, self.PIN, self.licensepool)
        eq_(response, True,
            msg="Cannot release hold, response false " + self.licensepool.identifier.identifier + " patron: "
                + self.PATRON)

        print 'Hold returned: %s' % self.licensepool.identifier.identifier


class TestOdiloDiscoveryAPI(OdiloAPITest):
    def test_1_odilo_recent_circulation_monitor(self):
        print 'Testing recent library products...'

        monitor = RecentOdiloCollectionMonitor(self._db, self.collection, api_class=MockOdiloAPI)
        ok_(monitor, 'Monitor null !!')
        eq_(ExternalIntegration.ODILO, monitor.protocol, 'Wat??')

        records_metadata_data = self.sample_data("records_metadata.json")
        monitor.api.queue_response(200, content=records_metadata_data)

        availability_data = self.sample_data("record_availability.json")
        monitor.api.queue_response(200, content=availability_data)
        monitor.api.queue_response(200, content=availability_data)
        monitor.api.queue_response(200, content=availability_data)
        monitor.api.queue_response(200, content=availability_data)
        monitor.api.queue_response(200, content=availability_data)
        monitor.api.queue_response(200, content=availability_data)
        monitor.api.queue_response(200, content=availability_data)
        monitor.api.queue_response(200, content=availability_data)
        monitor.api.queue_response(200, content=availability_data)
        monitor.api.queue_response(200, content=availability_data)

        monitor.api.queue_response(200, content='[]')  # No more resources retrieved

        monitor.run_once(start="2017-09-01", cutoff=None)

        print 'Finished !!'

    def test_2_odilo_full_circulation_monitor(self):
        print 'Testing all library products...'

        monitor = FullOdiloCollectionMonitor(self._db, self.collection, api_class=MockOdiloAPI)
        ok_(monitor, 'Monitor null !!')
        eq_(ExternalIntegration.ODILO, monitor.protocol, 'Wat??')

        records_metadata_data = self.sample_data("records_metadata.json")
        monitor.api.queue_response(200, content=records_metadata_data)

        availability_data = self.sample_data("record_availability.json")
        monitor.api.queue_response(200, content=availability_data)
        monitor.api.queue_response(200, content=availability_data)
        monitor.api.queue_response(200, content=availability_data)
        monitor.api.queue_response(200, content=availability_data)
        monitor.api.queue_response(200, content=availability_data)
        monitor.api.queue_response(200, content=availability_data)
        monitor.api.queue_response(200, content=availability_data)
        monitor.api.queue_response(200, content=availability_data)
        monitor.api.queue_response(200, content=availability_data)
        monitor.api.queue_response(200, content=availability_data)

        monitor.api.queue_response(200, content='[]')  # No more resources retrieved

        monitor.run_once()

        print 'Finished !!'
