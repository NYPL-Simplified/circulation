# encoding: utf-8
from nose.tools import (
    set_trace, eq_,
    assert_raises,
)
import pkgutil
import json
from datetime import (
    datetime,
    timedelta,
)
from api.enki import (
    MockEnkiAPI,
    EnkicirculationMonitor,
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
    DeliveryMechanism,
    Identifier,
    LicensePool,
    Representation,
    RightsStatus,
)

from api.config import temp_config

class EnkiAPITest(DatabaseTest):

    @classmethod
    def sample_data(self, filename):
        return sample_data(filename, 'enki')

    @classmethod
    def sample_json(self, filename):
        data = self.sample_data(filename)
        return data, json.loads(data)

    def error_message(self, error_code, message=None, token=None):
        """Create a JSON document that simulates the message served by
        Overdrive given a certain error condition.
        """
        message = message or self._str
        token = token or self._str
        data = dict(errorCode=error_code, message=message, token=token)
        return json.dumps(data)

class TestEnkiAPI(EnkiAPITest):

    def test_update_availability_deleted(self):
        """Test the Overdrive implementation of the update_availability
        method defined by the CirculationAPI interface.
        """

        # Create a LicensePool that needs updating.
        edition, pool = self._edition(
            identifier_type=Identifier.ENKI_ID,
            data_source_name=DataSource.ENKI,
            with_license_pool=True
        )

        # We have never checked the circulation information for this
        # LicensePool. Put some random junk in the pool to make sure
        # it gets replaced.
        pool.licenses_owned = 10
        pool.licenses_available = 4
        pool.patrons_in_hold_queue = 3
        eq_(None, pool.last_checked)

        # Prepare availability information.
        ignore, availability = self.sample_data(
            "item_deleted.html"
        )

        api = MockEnkiAPI(self._db)
        api.queue_response(200, content=availability)

        api.reaper_request("4204206969")

        # The availability information has been udpated, as has the
        # date the availability information was last checked.
        eq_(0, pool.licenses_owned)
        eq_(0, pool.licenses_available)
        eq_(0, pool.patrons_in_hold_queue)
        assert pool.last_checked is not None
