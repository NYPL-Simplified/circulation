from nose.tools import (
    assert_raises_regexp,
    set_trace,
    eq_,
    assert_not_equal,
)
import datetime
import os
import pkgutil
import json
from core.model import (
    CirculationEvent,
    Contributor,
    DataSource,
    LicensePool,
    Resource,
    Hyperlink,
    Identifier,
    Edition,
    Timestamp,
    Subject,
    Measurement,
    Work,
)
from . import DatabaseTest
from api.circulation_exceptions import *
from api.enki import (
    EnkiAPI,
    MockEnkiAPI,
    EnkiBibliographicCoverageProvider,
    EnkiImport,
    BibliographicParser,
)
from core.scripts import RunCollectionCoverageProviderScript
from core.util.http import BadResponseException
from core.testing import MockRequestsResponse

class BaseEnkiTest(object):

    base_path = os.path.split(__file__)[0]
    resource_path = os.path.join(base_path, "files", "enki")

    @classmethod
    def get_data(cls, filename):
        path = os.path.join(cls.resource_path, filename)
        return open(path).read()


class TestEnkiAPI(DatabaseTest, BaseEnkiTest):

    def setup(self):
        super(TestEnkiAPI, self).setup()
        self.collection = self._collection(protocol=EnkiAPI.ENKI)
        self.api = MockEnkiAPI(self._db)

    def test_create_identifier_strings(self):
        identifier = self._identifier(identifier_type=Identifier.ENKI_ID)
        values = EnkiAPI.create_identifier_strings(["foo", identifier])
        eq_(["foo", identifier.identifier], values)

    def test_import_instantiation(self):
        """Test that EnkiImport can be instantiated"""
        imp = EnkiImport(self._db, self.collection, api_class=self.api.__class__)
        assert_not_equal(None, imp)

    def test_fulfillment_open_access(self):
        """Test that fulfillment info for non-ACS Enki books is parsed correctly."""
        data = self.get_data("checked_out_direct.json")
        self.api.queue_response(200, content=data)
        result = json.loads(data)
        fulfill_data = self.api.parse_fulfill_result(result['result'])
        eq_(fulfill_data[0], """http://cccl.enkilibrary.org/API/UserAPI?method=downloadEContentFile&username=21901000008080&password=deng&lib=1&recordId=2""")
        eq_(fulfill_data[1], "epub")

    def test_fulfillment_acs(self):
        """Test that fulfillment info for ACS Enki books is parsed correctly."""
        data = self.get_data("checked_out_acs.json")
        self.api.queue_response(200, content=data)
        result = json.loads(data)
        fulfill_data = self.api.parse_fulfill_result(result['result'])
        eq_(fulfill_data[0], """http://afs.enkilibrary.org/fulfillment/URLLink.acsm?action=enterloan&ordersource=Califa&orderid=ACS4-9243146841581187248119581&resid=urn%3Auuid%3Ad5f54da9-8177-43de-a53d-ef521bc113b4&gbauthdate=Wed%2C+23+Aug+2017+19%3A42%3A35+%2B0000&dateval=1503517355&rights=%24lat%231505331755%24&gblver=4&auth=8604f0fc3f014365ea8d3c4198c721ed7ed2c16d""")
        eq_(fulfill_data[1], "epub")

class TestBibliographicCoverageProvider(TestEnkiAPI):

    """Test the code that looks up bibliographic information from Enki."""

    def test_process_item_creates_presentation_ready_work(self):
        """Test the normal workflow where we ask Enki for data,
        Enki provides it, and we create a presentation-ready work.
        """

        data = self.get_data("item_metadata_single.json")
        self.api.queue_response(200, content=data)

        identifier = self._identifier(identifier_type=Identifier.ENKI_ID)
        identifier.identifier = 'econtentRecord1'

        # This book has no LicensePool.
        eq_([], identifier.licensed_through)

        # Run it through the EnkiBibliographicCoverageProvider
        provider = EnkiBibliographicCoverageProvider(
            self.collection, api_class=self.api
        )
        [result] = provider.process_batch([identifier])
        eq_(identifier, result)

        # A LicensePool was created, not because we know anything
        # about how we've licensed this book, but to have a place to
        # store the information about what formats the book is
        # available in.
        pool = identifier.licensed_through[0]
        eq_(999, pool.licenses_owned)
        # A Work was created and made presentation ready.
        eq_("1984", pool.work.title)
        eq_(True, pool.work.presentation_ready)

class TestEnkiCollectionReaper(TestEnkiAPI):

    def test_reaped_book_has_zero_licenses(self):
        data = "<html></html>"

        # Create a LicensePool that needs updating.
        edition, pool = self._edition(
            identifier_type=Identifier.ENKI_ID,
            data_source_name=DataSource.ENKI,
            with_license_pool=True
        )

        # This is a specific record ID that should never exist
        nonexistent_id = "econtentRecord0"

        # We have never checked the circulation information for this
        # LicensePool. Put some random junk in the pool to verify
        # that it gets changed.
        pool.licenses_owned = 10
        pool.licenses_available = 5
        pool.patrons_in_hold_queue = 3
        pool.identifier.identifier = nonexistent_id
        eq_(None, pool.last_checked)

        # Modify the data so that it appears to be talking about the
        # book we just created.

        self.api.queue_response(200, content=data)

        circulationdata = self.api.reaper_request(pool.identifier)

        eq_(0, circulationdata.licenses_owned)
        eq_(0, circulationdata.licenses_available)
        eq_(0, circulationdata.patrons_in_hold_queue)

