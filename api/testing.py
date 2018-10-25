import contextlib
import json
import logging
from collections import defaultdict
from nose.tools import set_trace

from core.testing import DatabaseTest

from core.model import (
    ConfigurationSetting,
    DataSource,
    ExternalIntegration,
    Identifier,
    Library,
    Loan,
    Hold,
    Session,
)
from api.circulation import (
    BaseCirculationAPI,
    CirculationAPI,
    LoanInfo,
    HoldInfo,
)
from api.shared_collection import (
    SharedCollectionAPI,
)
from api.config import (
    Configuration,
    temp_config,
)

from api.adobe_vendor_id import AuthdataUtility

class VendorIDTest(DatabaseTest):
    """A DatabaseTest that knows how to set up an Adobe Vendor ID
    integration.
    """

    TEST_VENDOR_ID = u"vendor id"
    TEST_NODE_VALUE = 114740953091845

    def initialize_adobe(self, vendor_id_library, short_token_libraries=[]):
        """Initialize an Adobe Vendor ID integration and a
        Short Client Token integration with a number of libraries.

        :param vendor_id_library: The Library that should have an
        Adobe Vendor ID integration.

        :param short_token_libraries: The Libraries that should have a
        Short Client Token integration.
        """
        short_token_libraries = list(short_token_libraries)
        if not vendor_id_library in short_token_libraries:
            short_token_libraries.append(vendor_id_library)
        # The first library acts as an Adobe Vendor ID server.
        self.adobe_vendor_id = self._external_integration(
            ExternalIntegration.ADOBE_VENDOR_ID,
            ExternalIntegration.DRM_GOAL, username=self.TEST_VENDOR_ID,
            libraries=[vendor_id_library]
        )

        # The other libraries will share a registry integration.
        self.registry = self._external_integration(
            ExternalIntegration.OPDS_REGISTRATION,
            ExternalIntegration.DISCOVERY_GOAL,
            libraries=short_token_libraries
        )
        # The integration knows which Adobe Vendor ID server it
        # gets its Adobe IDs from.
        self.registry.set_setting(
            AuthdataUtility.VENDOR_ID_KEY,
            self.adobe_vendor_id.username
        )

        # As we give libraries their Short Client Token settings,
        # we build the 'other_libraries' setting we'll apply to the
        # Adobe Vendor ID integration.
        other_libraries = dict()

        # Every library in the system can generate Short Client
        # Tokens.
        for library in short_token_libraries:
            # Each library will get a slightly different short
            # name and secret for generating Short Client Tokens.
            library_uri = self._url
            short_name = library.short_name + "token"
            secret = library.short_name + " token secret"
            ConfigurationSetting.for_library_and_externalintegration(
                self._db, ExternalIntegration.USERNAME, library, self.registry
            ).value = short_name
            ConfigurationSetting.for_library_and_externalintegration(
                self._db, ExternalIntegration.PASSWORD, library, self.registry
            ).value = secret

            library.setting(Configuration.WEBSITE_URL).value = library_uri

            # Each library's Short Client Token configuration will be registered
            # with that Adobe Vendor ID server.
            if library != vendor_id_library:
                other_libraries[library_uri] = (short_name, secret)

        # Tell the Adobe Vendor ID server about the other libraries.
        other_libraries = json.dumps(other_libraries)
        self.adobe_vendor_id.set_setting(
            AuthdataUtility.OTHER_LIBRARIES_KEY, other_libraries
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

    def update_loan(self, loan, status_doc):
        self.availability_updated_for.append(loan.license_pool)

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

class MockSharedCollectionAPI(SharedCollectionAPI):
    def __init__(self, *args, **kwargs):
        super(MockSharedCollectionAPI, self).__init__(*args, **kwargs)
        self.responses = defaultdict(list)

    def _queue(self, k, v):
        self.responses[k].append(v)

    def _return_or_raise(self, k):
        self.log.debug(k)
        l = self.responses[k]
        v = l.pop(0)
        if isinstance(v, Exception):
            raise v
        return v

    def queue_register(self, response):
        self._queue('register', response)

    def register(self, collection, url):
        return self._return_or_raise('register')

    def queue_borrow(self, response):
        self._queue('borrow', response)

    def borrow(self, collection, client, pool, hold=None):
        return self._return_or_raise('borrow')

    def queue_revoke_loan(self, response):
        self._queue('revoke-loan', response)

    def revoke_loan(self, collection, client, loan):
        return self._return_or_raise('revoke-loan')

    def queue_fulfill(self, response):
        self._queue('fulfill', response)

    def fulfill(self, collection, client, loan, mechanism):
        return self._return_or_raise('fulfill')

    def queue_revoke_hold(self, response):
        self._queue('revoke-hold', response)

    def revoke_hold(self, collection, client, hold):
        return self._return_or_raise('revoke-hold')
