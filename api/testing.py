import contextlib
import json
import logging
from collections import defaultdict
from nose.tools import set_trace

from core.testing import DatabaseTest

from core.model import (
    DataSource,
    ExternalIntegration,
    Identifier,
    Library,
    Loan,
    Hold,
)
from api.circulation import (
    BaseCirculationAPI,
    CirculationAPI,
    LoanInfo,
    HoldInfo,
)
from api.config import (
    Configuration,
    temp_config,
)
from api.adobe_vendor_id import AuthdataUtility

class VendorIDTest(DatabaseTest):
    """Sets up an Adobe Vendor ID integration."""

    TEST_NODE_VALUE = 114740953091845
    TEST_VENDOR_ID = u"vendor id"
    TEST_LIBRARY_URI = u"http://me/"
    TEST_OTHER_LIBRARY_URI = u"http://you/"
    TEST_OTHER_LIBRARIES  = {TEST_OTHER_LIBRARY_URI: ("you", "secret2")}

    LIBRARY_REGISTRY_SHORT_NAME = u'LBRY'
    LIBRARY_REGISTRY_SHARED_SECRET = u'some secret'

    def setup(self, _db=None):
        super(VendorIDTest, self).setup()

        if not _db:
            # So long as we're not testing a scoped session, create
            # the Adobe Vendor ID credentials.
            self.adobe_vendor_id = self._external_integration(
                ExternalIntegration.ADOBE_VENDOR_ID,
                ExternalIntegration.DRM_GOAL, username=self.TEST_VENDOR_ID)
            self.set_main_library_adobe_config(self._default_library)

        _db = _db or self._db
        self.initialize_library(_db)

    def initialize_library(self, _db):
        """Initialize the Library object with default data."""
        library = Library.instance(_db)
        library.library_registry_short_name = self.LIBRARY_REGISTRY_SHORT_NAME
        library.library_registry_shared_secret = self.LIBRARY_REGISTRY_SHARED_SECRET
        return library

    def set_main_library_adobe_config(self, library):
        library.library_registry_short_name = self.LIBRARY_REGISTRY_SHORT_NAME
        self.adobe_vendor_id.password = self.TEST_NODE_VALUE
        self.adobe_vendor_id.url = self.TEST_LIBRARY_URI

        if library not in self.adobe_vendor_id.libraries:
            self.adobe_vendor_id.libraries.append(library)

        other_libraries = json.dumps(self.TEST_OTHER_LIBRARIES)
        self.adobe_vendor_id.set_setting(
            AuthdataUtility.OTHER_LIBRARIES_KEY, other_libraries
        )

    def set_dependent_library_adobe_config(self, library):
        library.library_registry_short_name = u'you'
        self.adobe_vendor_id.password = None
        self.adobe_vendor_id.url = self.TEST_OTHER_LIBRARY_URI

        if library not in self.adobe_vendor_id.libraries:
            self.adobe_vendor_id.libraries.append(library)

        self.adobe_vendor_id.set_setting(
            AuthdataUtility.OTHER_LIBRARIES_KEY, None
        )


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

    def __init__(self, *args, **kwargs):
        super(MockCirculationAPI, self).__init__(*args, **kwargs)
        self.responses = defaultdict(list)
        self.remote_loans = []
        self.remote_holds = []
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
        """Return a 3-tuple (loans, holds, completeness)."""
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
