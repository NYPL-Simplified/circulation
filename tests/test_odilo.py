# encoding: utf-8
import os
import json
import unittest

from flask import (
    Flask,
)

from api.odilo import (
    OdiloAPI,
    MockOdiloAPI,
    OdiloCirculationMonitor,
    FullOdiloCollectionMonitor,
    RecentOdiloCollectionMonitor,
)

from api.circulation import (
    CirculationAPI,
    CirculationException
)

from sqlalchemy.ext.declarative import declarative_base
from flask_sqlalchemy_session import flask_scoped_session
from api.config import Configuration

from . import (
    DatabaseTest,
    sample_data
)

from core.model import (
    Collection,
    ConfigurationSetting,
    DataSource,
    DeliveryMechanism,
    ExternalIntegration,
    Identifier,
    Library,
    LicensePool,
    Representation,
    RightsStatus,
    SessionManager
)

Base = declarative_base()
app = Flask(__name__)

testing = True
# testing = 'TESTING' in os.environ
db_url = Configuration.database_url(testing)
# Initialize a new database session unless we were told not to
# (e.g. because a script already initialized it).
autoinitialize = os.environ.get('AUTOINITIALIZE') != 'False'
if autoinitialize:
    SessionManager.initialize(db_url)
session_factory = SessionManager.sessionmaker(db_url)
_db = flask_scoped_session(session_factory, app)
if autoinitialize:
    SessionManager.initialize_data(_db)


class OdiloAPITest(DatabaseTest, unittest.TestCase):
    def setup(self):
        super(OdiloAPITest, self).setup()
        library = self._default_library
        self._db = _db
        self.collection = MockOdiloAPI.mock_collection(self._db)
        self.circulation = CirculationAPI(self._db, library, api_map={ExternalIntegration.ODILO: MockOdiloAPI})
        self.api = self.circulation.api_for_collection[self.collection.id]

    def setUp(self):
        self.setUp_common()
        self._db = _db

        # Load from DB or fake collection
        self.collection = None
        self.fake_collection()
        self.api = OdiloAPI(self._db, self.collection)

    def fake_collection(self):
        collection = Collection
        collection.protocol = ExternalIntegration.ODILO
        collection.id = 1

        collection.external_integration = ExternalIntegration
        collection.external_integration.username = 'Odilo'
        collection.external_integration.password = 'secret'

        # external_integration = collection.create_external_integration(ExternalIntegration.ODILO, self._db)

        collection.external_integration.settings.info = {
            OdiloAPI.LIBRARY_API_BASE_URL: 'http://localhost:8080/api/v2',
            'username': 'Odilo',
            'password': 'secret'
        }

        self.collection = collection

    @classmethod
    def setUp_common(cls):
        print 'setUp common'

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


if __name__ == '__main__':
    unittest.main()

patron = '001000265'
pin = 'c4ca4238a0b923820dcc509a6f75849b'
record_id = '00010982'
# internal_format = 'EPUB'
internal_format = 'ACSM'
checkout_id = ''
hold_id = ''

licensepool = LicensePool
# licensepool.collection = 'Odilo Test Collection'
# licensepool.collection.id = 1
licensepool.data_source.name = DataSource.ODILO
licensepool.identifier.type = Identifier.ODILO_ID,
licensepool.identifier.identifier = record_id  # Record_id


# licensepool.identifier.identifier = '00010953'  # Record_id 2
# licensepool.identifier.identifier = '00011052'  # Record_id 2
# licensepool.identifier.identifier = '0001098345342'  # fake Record_id


class TestOdiloCirculationAPI(OdiloAPITest):
    def test_1_checkout(self):
        print('Checkout test')
        licensepool.collection = self.collection

        loan_info = self.api.checkout(patron, pin, licensepool, internal_format)
        self.assertIsNotNone(loan_info, msg="LoanInfo null --> checkout failed!")
        print 'Loan ok: %s' % loan_info.identifier

        global checkout_id
        checkout_id = loan_info.identifier  # Checkout id

    def test_2_fulfill(self):
        print('Fulfill test')

        licensepool.collection = self.collection
        licensepool.identifier.identifier = checkout_id

        fulfillment_info = self.api.fulfill(patron, pin, licensepool, internal_format)
        self.assertIsNotNone(fulfillment_info, msg='Cannot Fulfill !!')

        if fulfillment_info.content_link:
            print 'Fulfill link: %s' % fulfillment_info.content_link
        if fulfillment_info.content:
            print 'Fulfill content: %s' % fulfillment_info.content

    def test_3_place_hold(self):
        print('Place hold test')

        licensepool.collection = self.collection
        licensepool.identifier.identifier = record_id
        # licensepool.identifier.identifier = '3942796523745'  # Test Non exists

        hold_info = self.api.place_hold(patron, pin, licensepool, 'ejcepas@odilotid.es')
        self.assertIsNotNone(hold_info, msg="HoldInfo null --> place hold failed!")
        print 'Hold ok: %s' % hold_info.identifier

        global hold_id
        hold_id = hold_info.identifier

    def test_4_patron_activity(self):
        print ('Patron activity')

        loans_and_holds = self.api.patron_activity(patron, pin)
        if loans_and_holds:
            print 'Found: %i loans and holds' % len(loans_and_holds)
        else:
            print 'No loans or holds found'

    def test_5_checkin(self):
        print('Checkin test')

        licensepool.identifier.identifier = checkout_id
        licensepool.collection = self.collection
        # licensepool.identifier.identifier = '4218888'  # Fake Checkout id

        try:
            response = self.api.checkin(patron, pin, licensepool)
            self.assertEqual(response.status_code, 200, msg="Response code != 200, cannot perform checkin for record: "
                                                            + licensepool.identifier.identifier + " patron: " + patron)
            checkout_returned = response.json()
            print 'Checkout returned: %s' % checkout_returned['id']
        except CirculationException as e:
            print 'CirculationException happend: ' + e.message

    def test_6_return_hold(self):
        print('Return hold test')

        licensepool.identifier.identifier = hold_id
        licensepool.collection = self.collection
        # licensepool.identifier.identifier = '000000140'  # Fake hold id

        try:
            response = self.api.release_hold(patron, pin, licensepool)
            self.assertTrue(response,
                            msg="Cannot release hold, response false " + licensepool.identifier.identifier + " patron: " + patron)
            print 'Hold returned: %s' % licensepool.identifier.identifier

        except CirculationException as e:
            print 'CirculationException happend: ' + e.message


class TestOdiloDiscoveryAPI(OdiloAPITest):
    def test_1_odilo_recent_circulation_monitor(self):
        print 'Testing all library products'

        monitor = RecentOdiloCollectionMonitor(self._db, self.collection)
        self.assertIsNotNone(monitor, 'Monitor null !!')
        self.assertEqual(ExternalIntegration.ODILO, monitor.protocol, 'Wat??')

        monitor.run_once(start="2017-09-01", cutoff=None)

        print 'Finished !!'

    def test_2_odilo_full_circulation_monitor(self):
        print 'Testing all library products'

        monitor = FullOdiloCollectionMonitor(self._db, self.collection)
        self.assertIsNotNone(monitor, 'Monitor null !!')
        self.assertEqual(ExternalIntegration.ODILO, monitor.protocol, 'Wat??')

        monitor.run_once()

        print 'Finished !!'
