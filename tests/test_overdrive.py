# encoding: utf-8
from nose.tools import (
    set_trace, eq_,
    assert_raises,
)
import os
import pkgutil
import json
from ..overdrive import (
    DummyOverdriveAPI,
)

from . import (
    DatabaseTest,
)

from ..core.model import (
    DataSource,
    Identifier,
)

class TestOverdriveAPI(DatabaseTest):

    base_path = os.path.split(__file__)[0]
    resource_path = os.path.join(base_path, "files", "overdrive")

    @classmethod
    def sample_data(self, filename):
        path = os.path.join(self.resource_path, filename)
        data = open(path).read()
        return data

    @classmethod
    def sample_json(self, filename):
        data = self.sample_data(filename)
        return data, json.loads(data)

    def test_update_new_licensepool(self):
        data, raw = self.sample_json("overdrive_availability_information.json")

        # Create an identifier
        identifier = self._identifier(
            identifier_type=Identifier.OVERDRIVE_ID
        )

        # Make it look like the availability information is for the
        # newly created Identifier.
        raw['id'] = identifier.identifier

        api = DummyOverdriveAPI(self._db)
        pool, was_new, changed = api.update_licensepool_with_book_info(raw)
        eq_(True, was_new)
        eq_(True, changed)

        # The title of the corresponding Edition has been filled
        # in, just to provide some basic human-readable metadata.
        eq_("Blah blah blah", pool.edition().title)
        eq_(raw['copiesOwned'], pool.licenses_owned)
        eq_(raw['copiesAvailable'], pool.licenses_available)
        eq_(0, pool.licenses_reserved)
        eq_(raw['numberOfHolds'], pool.patrons_in_hold_queue)

    def test_update_existing_licensepool(self):
        data, raw = self.sample_json("overdrive_availability_information.json")

        # Create a LicensePool.
        wr, pool = self._edition(
            data_source_name=DataSource.OVERDRIVE,
            identifier_type=Identifier.OVERDRIVE_ID,
            with_license_pool=True
        )

        # Make it look like the availability information is for the
        # newly created LicensePool.
        raw['id'] = pool.identifier.identifier

        wr.title = "The real title."
        eq_(0, pool.licenses_owned)
        eq_(0, pool.licenses_available)
        eq_(0, pool.licenses_reserved)
        eq_(0, pool.patrons_in_hold_queue)

        api = DummyOverdriveAPI(self._db)
        p2, was_new, changed = api.update_licensepool_with_book_info(raw)
        eq_(False, was_new)
        eq_(True, changed)
        eq_(p2, pool)
        # The title didn't change to that title given in the availability
        # information, because we already set a title for that work.
        eq_("The real title.", wr.title)
        eq_(raw['copiesOwned'], pool.licenses_owned)
        eq_(raw['copiesAvailable'], pool.licenses_available)
        eq_(0, pool.licenses_reserved)
        eq_(raw['numberOfHolds'], pool.patrons_in_hold_queue)

    def test_update_licensepool_with_holds(self):
        data, raw = self.sample_json("overdrive_availability_information_holds.json")
        identifier = self._identifier(
            identifier_type=Identifier.OVERDRIVE_ID
        )
        raw['id'] = identifier.identifier

        api = DummyOverdriveAPI(self._db)
        pool, was_new, changed = api.update_licensepool_with_book_info(raw)
        eq_(10, pool.patrons_in_hold_queue)
        eq_(True, changed)

    def test_get_download_link(self):
        data, json = self.sample_json("checkout_response_locked_in_format.json")
        url = DummyOverdriveAPI.get_download_link(
            json, "ebook-epub-adobe", "http://foo.com/")
        eq_("http://patron.api.overdrive.com/v1/patrons/me/checkouts/76C1B7D0-17F4-4C05-8397-C66C17411584/formats/ebook-epub-adobe/downloadlink?errorpageurl=http://foo.com/", url)
        
        assert_raises(IOError, DummyOverdriveAPI.get_download_link,
            json, "no-such-format", "http://foo.com/")

    def test_extract_data_from_checkout_resource(self):
        data, json = self.sample_json("checkout_response_locked_in_format.json")
        expires, url = DummyOverdriveAPI.extract_data_from_checkout_response(
            json, "ebook-epub-adobe", "http://foo.com/")
        eq_(2013, expires.year)
        eq_(10, expires.month)
        eq_(4, expires.day)
        eq_("http://patron.api.overdrive.com/v1/patrons/me/checkouts/76C1B7D0-17F4-4C05-8397-C66C17411584/formats/ebook-epub-adobe/downloadlink?errorpageurl=http://foo.com/", url)

    def test_sync_bookshelf_creates_local_loans(self):
        loans, json_loans = self.sample_json("shelf_with_some_checked_out_books.json")
        holds, json_holds = self.sample_json("no_holds.json")

        # All four loans in the sample data were created.
        patron = self.default_patron
        loans, holds = DummyOverdriveAPI.sync_bookshelf(patron, json_loans, json_holds)
        eq_(4, len(loans))
        eq_(loans, patron.loans)

        eq_([], holds)

        # Running the sync again leaves all four loans in place.
        loans, holds = DummyOverdriveAPI.sync_bookshelf(patron, json_loans, json_holds)
        eq_(4, len(loans))
        eq_(loans, patron.loans)        

    def test_sync_bookshelf_removes_loans_not_present_on_remote(self):
        data, json_loans = self.sample_json("shelf_with_some_checked_out_books.json")
        data, json_holds = self.sample_json("no_holds.json")
        
        patron = self.default_patron
        overdrive, new = self._edition(data_source_name=DataSource.OVERDRIVE,
                                       with_license_pool=True)
        overdrive_loan, new = overdrive.license_pool.loan_to(patron)

        # The loan not present in the sample data has been removed
        loans, holds = DummyOverdriveAPI.sync_bookshelf(patron, json_loans, json_holds)
        eq_(4, len(loans))
        eq_(loans, patron.loans)
        assert overdrive_loan not in patron.loans

    def test_sync_bookshelf_ignores_loans_from_other_sources(self):
        patron = self.default_patron
        gutenberg, new = self._edition(data_source_name=DataSource.GUTENBERG,
                                       with_license_pool=True)
        gutenberg_loan, new = gutenberg.license_pool.loan_to(patron)
        data, json_loans = self.sample_json("shelf_with_some_checked_out_books.json")
        data, json_holds = self.sample_json("no_holds.json")     
   
        # Overdrive doesn't know about the Gutenberg loan, but it was
        # not destroyed, because it came from another source.
        loans, holds = DummyOverdriveAPI.sync_bookshelf(patron, json_loans, json_holds)
        eq_(5, len(patron.loans))
        assert gutenberg_loan in patron.loans

    def test_sync_bookshelf_creates_local_holds(self):
        
        loans, json_loans = self.sample_json("no_loans.json")
        holds, json_holds = self.sample_json("holds.json")

        # All four loans in the sample data were created.
        patron = self.default_patron
        loans, holds = DummyOverdriveAPI.sync_bookshelf(
            patron, json_loans, json_holds)
        eq_(4, len(holds))
        eq_(holds, patron.holds)

        # Running the sync again leaves all four holds in place.
        loans = DummyOverdriveAPI.sync_bookshelf(patron, json_loans, json_holds)
        eq_(4, len(holds))
        eq_(holds, patron.holds)        

    def test_sync_bookshelf_removes_holds_not_present_on_remote(self):
        loans, json_loans = self.sample_json("no_loans.json")
        holds, json_holds = self.sample_json("holds.json")
        
        patron = self.default_patron
        overdrive, new = self._edition(data_source_name=DataSource.OVERDRIVE,
                                       with_license_pool=True)
        overdrive_hold, new = overdrive.license_pool.on_hold_to(patron)

        # The hold not present in the sample data has been removed
        loans, holds = DummyOverdriveAPI.sync_bookshelf(patron, json_loans, json_holds)
        eq_(4, len(holds))
        eq_(holds, patron.holds)
        assert overdrive_hold not in patron.loans

    def test_sync_bookshelf_ignores_holds_from_other_sources(self):
        loans, json_loans = self.sample_json("no_loans.json")
        holds, json_holds = self.sample_json("holds.json")

        patron = self.default_patron
        threem, new = self._edition(data_source_name=DataSource.THREEM,
                                    with_license_pool=True)
        threem_hold, new = threem.license_pool.on_hold_to(patron)
   
        # Overdrive doesn't know about the 3M hold, but it was
        # not destroyed, because it came from another source.
        loans, holds = DummyOverdriveAPI.sync_bookshelf(patron, json_loans, json_holds)
        eq_(5, len(patron.holds))
        assert threem_hold in patron.holds
