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
    EnkiCirculationMonitor,
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

class TestEnkiAPI(EnkiAPITest):

    def test_reaper_deleted_book(self):
        api = MockEnkiAPI(self._db)
        api.queue_response(200, content=availability)

        response = api.reaper_request("nonexistant")

        eq_(None, response)

    def test_reaper_license_removed(self):
        '''test the license pool after it's reaped to make sure there are
        zero licenses owned and available'''
