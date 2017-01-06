import logging
from collections import defaultdict
from core.model import (
    DataSource,
    Identifier,
    Loan,
    Hold,
)
from api.circulation import (
    BaseCirculationAPI,
    CirculationAPI,
    LoanInfo,
    HoldInfo,
)
from api.config import Configuration
from api.adobe_vendor_id import AuthdataUtility

class MockAdobeConfiguration(object):
    """Contains the constants necessary to set up an Adobe
    AuthdataUtility.  This is used in test_adobe_vendor_id.py (to test
    the basic functionality) and test_controller.py (to create an
    AdobeVendorIDController for use in testing.)
    """
    
    TEST_NODE_VALUE = 114740953091845
    TEST_VENDOR_ID = "vendor id"
    TEST_LIBRARY_URI = "http://me/"
    TEST_LIBRARY_SHORT_NAME = "Lbry"
    TEST_SECRET = "some secret"
    TEST_OTHER_LIBRARY_URI = "http://you/"
    TEST_OTHER_LIBRARIES  = {TEST_OTHER_LIBRARY_URI: ("you", "secret2")}

    MOCK_ADOBE_CONFIGURATION = {
        Configuration.ADOBE_VENDOR_ID: TEST_VENDOR_ID,
        Configuration.ADOBE_VENDOR_ID_NODE_VALUE: TEST_NODE_VALUE,
        AuthdataUtility.LIBRARY_URI_KEY: TEST_LIBRARY_URI,
        AuthdataUtility.LIBRARY_SHORT_NAME_KEY: TEST_LIBRARY_SHORT_NAME,
        AuthdataUtility.AUTHDATA_SECRET_KEY: TEST_SECRET,
        AuthdataUtility.OTHER_LIBRARIES_KEY: TEST_OTHER_LIBRARIES,
    }


class MockRemoteAPI(BaseCirculationAPI):
    def __init__(self, set_delivery_mechanism_at, can_revoke_hold_when_reserved):
        self.SET_DELIVERY_MECHANISM_AT = set_delivery_mechanism_at
        self.CAN_REVOKE_HOLD_WHEN_RESERVED = can_revoke_hold_when_reserved
        self.responses = defaultdict(list)
        self.log = logging.getLogger("Mock remote API")
        self.availability_updated_for = []

    def checkout(
            self, patron_obj, patron_password, licensepool, 
            delivery_mechanism
    ):
        # Should be a LoanInfo.
        return self._return_or_raise('checkout')

    def update_availability(self, licensepool):
        """Simply record the fact that update_availability was called."""
        self.availability_updated_for.append(licensepool)
                
    def place_hold(self, patron, pin, licensepool, 
                   hold_notification_email=None):
        # Should be a HoldInfo.
        return self._return_or_raise('hold')

    def fulfill(self, patron, password, pool, delivery_mechanism):
        # Should be a FulfillmentInfo.
        return self._return_or_raise('fulfill')

    def checkin(self, patron, pin, licensepool):
        # Return value is not checked.
        return self._return_or_raise('checkin')

    def release_hold(self, patron, pin, licensepool):
        # Return value is not checked.
        return self._return_or_raise('release_hold')

    def internal_format(self, delivery_mechanism):
        return delivery_mechanism

    def queue_checkout(self, response):
        self._queue('checkout', response)

    def queue_hold(self, response):
        self._queue('hold', response)

    def queue_fulfill(self, response):
        self._queue('fulfill', response)

    def queue_checkin(self, response):
        self._queue('checkin', response)

    def queue_release_hold(self, response):
        self._queue('release_hold', response)

    def _queue(self, k, v):
        self.responses[k].append(v)

    def _return_or_raise(self, k):
        self.log.debug(k)
        l = self.responses[k]
        v = l.pop()
        if isinstance(v, Exception):
            raise v
        return v

class MockCirculationAPI(CirculationAPI):

    def __init__(self, _db):
        super(MockCirculationAPI, self).__init__(_db)
        self.responses = defaultdict(list)
        self.remote_loans = []
        self.remote_holds = []
        self.identifier_type_to_data_source_name = {
            Identifier.GUTENBERG_ID: DataSource.GUTENBERG,
            Identifier.OVERDRIVE_ID: DataSource.OVERDRIVE,
            Identifier.THREEM_ID: DataSource.THREEM,
            Identifier.AXIS_360_ID: DataSource.AXIS_360,
        }
        self.data_source_ids_for_sync = [
            DataSource.lookup(self._db, name).id for name in 
            self.identifier_type_to_data_source_name.values()
        ]
        self.remotes = {}

    def local_loans(self, patron):
        return self._db.query(Loan).filter(Loan.patron==patron)

    def local_holds(self, patron):
        return self._db.query(Hold).filter(Hold.patron==patron)

    def add_remote_loan(self, *args, **kwargs):
        self.remote_loans.append(LoanInfo(*args, **kwargs))

    def add_remote_hold(self, *args, **kwargs):
        self.remote_holds.append(HoldInfo(*args, **kwargs))

    def patron_activity(self, patron, pin):
        """Return a 2-tuple (loans, holds)."""
        return self.remote_loans, self.remote_holds, True

    def queue_checkout(self, licensepool, response):
        self._queue('checkout', licensepool, response)

    def queue_hold(self, licensepool, response):
        self._queue('hold', licensepool, response)

    def queue_fulfill(self, licensepool, response):
        self._queue('fulfill', licensepool, response)

    def queue_checkin(self, licensepool, response):
        self._queue('checkin', licensepool, response)

    def queue_release_hold(self, licensepool, response):
        self._queue('release_hold', licensepool, response)

    def _queue(self, method, licensepool, response):
        mock = self.api_for_license_pool(licensepool)
        return mock._queue(method, response)

    def api_for_license_pool(self, licensepool):
        source = licensepool.data_source.name
        if source not in self.remotes:
            set_delivery_mechanism_at = BaseCirculationAPI.FULFILL_STEP
            can_revoke_hold_when_reserved = True
            if source == DataSource.AXIS_360:
                set_delivery_mechanism_at = BaseCirculationAPI.BORROW_STEP
            if source == DataSource.THREEM:
                can_revoke_hold_when_reserved = False
            remote = MockRemoteAPI(
                set_delivery_mechanism_at, can_revoke_hold_when_reserved
            )
            self.remotes[source] = remote
        return self.remotes[source]
