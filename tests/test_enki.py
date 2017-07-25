from nose.tools import (
    assert_raises_regexp,
    set_trace,
    eq_,
)
import datetime
import os
import pkgutil
from api.threem import (
    CirculationParser,
    EventParser,
    ErrorParser,
)
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
)
from scripts import RunCoverageProviderScript
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
        self.api = MockEnkiAPI(self._db)

    def test_create_identifier_strings(self):
        identifier = self._identifier(identifier_type=Identifier.ENKI_ID)
        values = EnkiAPI.create_identifier_strings(["foo", identifier])
        eq_(["foo", identifier.identifier], values)
        eq_([identifier.type], [Identifier.ENKI_ID])


class TestBibliographicCoverageProvider(TestEnkiAPI):

    """Test the code that looks up bibliographic information from Enki."""

    def test_script_instantiation(self):
        """Test that RunCoverageProviderScript can instantiate
        the coverage provider.
        """
        script = RunCoverageProviderScript(
            EnkiBibliographicCoverageProvider, self._db, [],
            enki_api=self.api
        )
        assert isinstance(script.provider,
                          EnkiBibliographicCoverageProvider)
        eq_(script.provider.api, self.api)

    def test_process_item_creates_presentation_ready_work(self):
        """Test the normal workflow where we ask Enki for data,
        Enki provides it, and we create a presentation-ready work.
        """

        data = self.get_data("item_metadata_single.json")
        self.api.queue_response(200, content=data)

        identifier = self._identifier(identifier_type=Identifier.ENKI_ID)
        identifier.identifier = 'econtentRecord1'

        # This book has no LicensePool.
        eq_(None, identifier.licensed_through)

        # Run it through the EnkiBibliographicCoverageProvider
        provider = EnkiBibliographicCoverageProvider(
            self._db, enki_api=self.api
        )
        [result] = provider.process_batch([identifier])
        eq_(identifier, result)

        # A LicensePool was created, not because we know anything
        # about how we've licensed this book, but to have a place to
        # store the information about what formats the book is
        # available in.
        pool = identifier.licensed_through
        eq_(999, pool.licenses_owned)
        # A Work was created and made presentation ready.
        eq_("1984", pool.work.title)
        eq_(True, pool.work.presentation_ready)

class TestEnkiCollectionReaper(TestEnkiAPI):

    def test_reaped_book_has_zero_licenses(self):
        data = self.get_data("item_removed_from_enki.xml")

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

        self.api.reaper_request(pool.identifier)

        eq_(0, pool.licenses_owned)
        eq_(0, pool.licenses_available)
        eq_(0, pool.licenses_reserved)
        eq_(0, pool.patrons_in_hold_queue)
