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
from api.overdrive import (
    DummyOverdriveAPI,
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
    Identifier,
    LicensePool,
)

from api.config import temp_config

class OverdriveAPITest(DatabaseTest):

    @classmethod
    def sample_data(self, filename):
        return sample_data(filename, 'overdrive')

    @classmethod
    def sample_json(self, filename):
        data = self.sample_data(filename)
        return data, json.loads(data)

class TestOverdriveAPI(OverdriveAPITest):

    def test_default_notification_email_address(self):
        """Test the ability of the Overdrive API to detect an email address
        previously given by the patron to Overdrive for the purpose of
        notifications.
        """
        ignore, patron_with_email = self.sample_json(
            "patron_info.json"
        )
        api = DummyOverdriveAPI(self._db)
        api.queue_response(content=patron_with_email)
        patron = self.default_patron
        eq_("foo@bar.com", 
            api.default_notification_email_address(patron, 'pin'))

        # If the patron has never before put an Overdrive book on
        # hold, their JSON object has no `lastHoldEmail` key. In this
        # case we use the site default.
        patron_with_no_email = dict(patron_with_email)
        del patron_with_no_email['lastHoldEmail']
        api.queue_response(content=patron_with_no_email)
        with temp_config() as config:
            config['default_notification_email_address'] = "notifications@example.com"
            eq_("notifications@example.com", 
                api.default_notification_email_address(patron, 'pin'))

            # If there's an error getting the information, use the
            # site default.
            api.queue_response(404)
            eq_("notifications@example.com", 
                api.default_notification_email_address(patron, 'pin'))

    def test_update_availability(self):
        """Test the Overdrive implementation of the update_availability
        method defined by the CirculationAPI interface.
        """

        # Create a LicensePool that needs updating.
        edition, pool = self._edition(
            identifier_type=Identifier.OVERDRIVE_ID,
            # TODO: If this line is commented out, we get an error later
            # on which might or might not be worrisome.
            data_source_name=DataSource.OVERDRIVE,
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
        ignore, availability = self.sample_json(
            "overdrive_availability_information.json"
        )
        # Since this is the first time we've seen this book,
        # we'll also be updating the bibliographic information.
        ignore, bibliographic = self.sample_json(
            "bibliographic_information.json"
        )

        # To avoid a mismatch, make it look like the information is
        # for the new pool's Identifier.
        availability['id'] = pool.identifier.identifier
        bibliographic['id'] = pool.identifier.identifier

        api = DummyOverdriveAPI(self._db)
        api.queue_response(content=bibliographic)
        api.queue_response(content=availability)

        api.update_availability(pool)

        # The availability information has been udpated, as has the
        # date the availability information was last checked.
        eq_(5, pool.licenses_owned)
        eq_(5, pool.licenses_available)
        eq_(0, pool.patrons_in_hold_queue)
        assert pool.last_checked is not None

    def test_update_licensepool_provides_bibliographic_coverage(self):
        # Create an identifier.
        identifier = self._identifier(
            identifier_type=Identifier.OVERDRIVE_ID
        )

        # Prepare bibliographic and availability information 
        # for this identifier.
        ignore, availability = self.sample_json(
            "overdrive_availability_information.json"
        )
        ignore, bibliographic = self.sample_json(
            "bibliographic_information.json"
        )

        # To avoid a mismatch, make it look like the information is
        # for the newly created Identifier.
        availability['id'] = identifier.identifier
        bibliographic['id'] = identifier.identifier

        api = DummyOverdriveAPI(self._db)
        api.queue_response(content=bibliographic)
        api.queue_response(content=availability)

        # Now we're ready. When we call update_licensepool, the
        # OverdriveAPI will retrieve the availability information,
        # then the bibliographic information. It will then trigger the
        # OverdriveBibliographicCoverageProvider, which will
        # create an Edition and a presentation-ready Work.
        pool, was_new, changed = api.update_licensepool(identifier.identifier)
        eq_(True, was_new)        
        eq_(availability['copiesOwned'], pool.licenses_owned)

        edition = pool.presentation_edition
        eq_("Ancillary Justice", edition.title)

        eq_(True, pool.work.presentation_ready)
        assert pool.work.cover_thumbnail_url.startswith(
            'http://images.contentreserve.com/'
        )

        # The book has been run through the bibliographic coverage
        # provider.
        coverage = [
            x for x in identifier.coverage_records 
            if x.operation is None
            and x.data_source.name == DataSource.OVERDRIVE
        ]
        eq_(1, len(coverage))

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
        pool, was_new = LicensePool.for_foreign_id(
            self._db, DataSource.OVERDRIVE, 
            identifier.type, identifier.identifier
        )
        
        pool, was_new, changed = api.update_licensepool_with_book_info(
            raw, pool, was_new
        )
        eq_(True, was_new)
        eq_(True, changed)

        self._db.commit()

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
        eq_(1, pool.licenses_owned)
        eq_(1, pool.licenses_available)
        eq_(0, pool.licenses_reserved)
        eq_(0, pool.patrons_in_hold_queue)

        api = DummyOverdriveAPI(self._db)
        p2, was_new, changed = api.update_licensepool_with_book_info(
            raw, pool, False
        )
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
        license_pool, is_new = LicensePool.for_foreign_id(
            self._db, DataSource.OVERDRIVE, identifier.type, 
            identifier.identifier
        )
        pool, was_new, changed = api.update_licensepool_with_book_info(
            raw, license_pool, is_new
        )
        eq_(10, pool.patrons_in_hold_queue)
        eq_(True, changed)


class TestExtractData(OverdriveAPITest):

    def test_get_download_link(self):
        data, json = self.sample_json("checkout_response_locked_in_format.json")
        url = DummyOverdriveAPI.get_download_link(
            json, "ebook-epub-adobe", "http://foo.com/")
        eq_("http://patron.api.overdrive.com/v1/patrons/me/checkouts/76C1B7D0-17F4-4C05-8397-C66C17411584/formats/ebook-epub-adobe/downloadlink?errorpageurl=http://foo.com/", url)
        
        assert_raises(
            NoAcceptableFormat, 
            DummyOverdriveAPI.get_download_link,
            json, "no-such-format", "http://foo.com/"
        )

    def test_get_download_link_raises_exception_if_loan_fulfilled_on_incompatible_platform(self):
        data, json = self.sample_json("checkout_response_book_fulfilled_on_kindle.json")
        assert_raises(
            FulfilledOnIncompatiblePlatform,
            DummyOverdriveAPI.get_download_link,
            json, "ebook-epub-adobe", "http://foo.com/"            
        )

    def test_extract_data_from_checkout_resource(self):
        data, json = self.sample_json("checkout_response_locked_in_format.json")
        expires, url = DummyOverdriveAPI.extract_data_from_checkout_response(
            json, "ebook-epub-adobe", "http://foo.com/")
        eq_(2013, expires.year)
        eq_(10, expires.month)
        eq_(4, expires.day)
        eq_("http://patron.api.overdrive.com/v1/patrons/me/checkouts/76C1B7D0-17F4-4C05-8397-C66C17411584/formats/ebook-epub-adobe/downloadlink?errorpageurl=http://foo.com/", url)

    def test_process_checkout_data(self):
        data, json = self.sample_json("shelf_with_book_already_fulfilled_on_kindle.json")
        [on_kindle, not_on_kindle] = json["checkouts"]

        # The book already fulfilled on Kindle doesn't get turned into
        # LoanInfo.
        eq_(None, DummyOverdriveAPI.process_checkout_data(on_kindle))

        # The book not yet fulfilled does show up as a LoanInfo.
        loan_info = DummyOverdriveAPI.process_checkout_data(not_on_kindle)
        eq_("2fadd2ac-a8ec-4938-a369-4c3260e8922b", loan_info.identifier)

        data, format_locked_in = self.sample_json("checkout_response_locked_in_format.json")

        # A book that's on loan with a format locked in shows up.
        loan_info = DummyOverdriveAPI.process_checkout_data(format_locked_in)
        assert loan_info != None

        data, no_format_locked_in = self.sample_json("checkout_response_no_format_locked_in.json")

        # A book that's on loan with no format locked in also shows up.
        loan_info = DummyOverdriveAPI.process_checkout_data(no_format_locked_in)
        assert loan_info != None

        # TODO: In the future both of these tests should return a
        # LoanInfo with appropriate FulfillmentInfo. The calling code
        # would then decide whether or not to show the loan.

class TestSyncBookshelf(OverdriveAPITest):

    def test_sync_bookshelf_creates_local_loans(self):
        loans_data, json_loans = self.sample_json("shelf_with_some_checked_out_books.json")
        holds_data, json_holds = self.sample_json("no_holds.json")

        overdrive = DummyOverdriveAPI(self._db)
        overdrive.queue_response(content=holds_data)
        overdrive.queue_response(content=loans_data)

        patron = self.default_patron
        circulation = CirculationAPI(self._db, overdrive=overdrive)
        loans, holds = circulation.sync_bookshelf(patron, "dummy pin")

        # All four loans in the sample data were created.
        eq_(4, len(loans))
        eq_(loans, patron.loans)

        eq_([], holds)

        # Running the sync again leaves all four loans in place.
        overdrive.queue_response(content=holds_data)
        overdrive.queue_response(content=loans_data)
        loans, holds = circulation.sync_bookshelf(patron, "dummy pin")
        eq_(4, len(loans))
        eq_(loans, patron.loans)        

    def test_sync_bookshelf_removes_loans_not_present_on_remote(self):
        loans_data, json_loans = self.sample_json("shelf_with_some_checked_out_books.json")
        holds_data, json_holds = self.sample_json("no_holds.json")

        overdrive = DummyOverdriveAPI(self._db)
        overdrive.queue_response(content=holds_data)
        overdrive.queue_response(content=loans_data)

        # Create a loan not present in the sample data.
        patron = self.default_patron
        overdrive_edition, new = self._edition(
            data_source_name=DataSource.OVERDRIVE,
            with_license_pool=True
        )
        overdrive_loan, new = overdrive_edition.license_pool.loan_to(patron)
        yesterday = datetime.utcnow() - timedelta(days=1)
        overdrive_loan.start = yesterday

        # Sync with Overdrive, and the loan not present in the sample
        # data is removed.
        circulation = CirculationAPI(self._db, overdrive=overdrive)
        loans, holds = circulation.sync_bookshelf(patron, "dummy pin")

        eq_(4, len(loans))
        eq_(loans, patron.loans)
        assert overdrive_loan not in patron.loans

    def test_sync_bookshelf_ignores_loans_from_other_sources(self):
        patron = self.default_patron
        gutenberg, new = self._edition(data_source_name=DataSource.GUTENBERG,
                                       with_license_pool=True)
        gutenberg_loan, new = gutenberg.license_pool.loan_to(patron)
        loans_data, json_loans = self.sample_json("shelf_with_some_checked_out_books.json")
        holds_data, json_holds = self.sample_json("no_holds.json")     
   
        # Overdrive doesn't know about the Gutenberg loan, but it was
        # not destroyed, because it came from another source.
        overdrive = DummyOverdriveAPI(self._db)
        overdrive.queue_response(content=holds_data)
        overdrive.queue_response(content=loans_data)
        circulation = CirculationAPI(self._db, overdrive=overdrive)
        patron = self.default_patron
        
        loans, holds = circulation.sync_bookshelf(patron, "dummy pin")
        eq_(5, len(patron.loans))
        assert gutenberg_loan in patron.loans

    def test_sync_bookshelf_creates_local_holds(self):
        
        loans_data, json_loans = self.sample_json("no_loans.json")
        holds_data, json_holds = self.sample_json("holds.json")

        overdrive = DummyOverdriveAPI(self._db)
        overdrive.queue_response(content=holds_data)
        overdrive.queue_response(content=loans_data)
        circulation = CirculationAPI(self._db, overdrive=overdrive)
        patron = self.default_patron

        loans, holds = circulation.sync_bookshelf(patron, "dummy pin")
        # All four loans in the sample data were created.
        eq_(4, len(holds))
        eq_(holds, patron.holds)

        # Running the sync again leaves all four holds in place.
        overdrive.queue_response(content=holds_data)
        overdrive.queue_response(content=loans_data)
        circulation = CirculationAPI(self._db, overdrive=overdrive)
        loans, holds = circulation.sync_bookshelf(patron, "dummy pin")
        eq_(4, len(holds))
        eq_(holds, patron.holds)        

    def test_sync_bookshelf_removes_holds_not_present_on_remote(self):
        loans_data, json_loans = self.sample_json("no_loans.json")
        holds_data, json_holds = self.sample_json("holds.json")
        
        patron = self.default_patron
        overdrive_edition, new = self._edition(data_source_name=DataSource.OVERDRIVE,
                                       with_license_pool=True)
        overdrive_hold, new = overdrive_edition.license_pool.on_hold_to(patron)


        overdrive = DummyOverdriveAPI(self._db)
        overdrive.queue_response(content=holds_data)
        overdrive.queue_response(content=loans_data)

        # The hold not present in the sample data has been removed
        circulation = CirculationAPI(self._db, overdrive=overdrive)
        loans, holds = circulation.sync_bookshelf(patron, "dummy pin")
        eq_(4, len(holds))
        eq_(holds, patron.holds)
        assert overdrive_hold not in patron.loans

    def test_sync_bookshelf_ignores_holds_from_other_sources(self):
        loans_data, json_loans = self.sample_json("no_loans.json")
        holds_data, json_holds = self.sample_json("holds.json")

        patron = self.default_patron
        threem, new = self._edition(data_source_name=DataSource.THREEM,
                                    with_license_pool=True)
        threem_hold, new = threem.license_pool.on_hold_to(patron)
   
        overdrive = DummyOverdriveAPI(self._db)
        overdrive.queue_response(content=holds_data)
        overdrive.queue_response(content=loans_data)

        # Overdrive doesn't know about the 3M hold, but it was
        # not destroyed, because it came from another source.
        circulation = CirculationAPI(self._db, overdrive=overdrive)
        loans, holds = circulation.sync_bookshelf(patron, "dummy pin")
        eq_(5, len(patron.holds))
        assert threem_hold in patron.holds
