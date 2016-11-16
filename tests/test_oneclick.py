import datetime
import json
from lxml import etree
from nose.tools import (
    eq_, 
    assert_raises,
    assert_raises_regexp,
    set_trace,
)
import os
from StringIO import StringIO

from core.config import (
    #Configuration, 
    temp_config,
)

from core.model import (
    get_one_or_create,
    Contributor,
    DataSource,
    Edition,
    Identifier,
    LicensePool,
    Patron,
    Subject,
)

from core.metadata_layer import (
    CirculationData,
    ContributorData,
    IdentifierData,
    Metadata,
    SubjectData,
)

from api.oneclick import (
    OneClickAPI,
    OneClickCirculationMonitor, 
    MockOneClickAPI,
)

from api.circulation import (
    LoanInfo,
    HoldInfo,
    FulfillmentInfo,
)

from api.circulation_exceptions import *

from . import (
    DatabaseTest,
)



class OneClickAPITest(DatabaseTest):

    def setup(self):
        super(OneClickAPITest, self).setup()

        self.api = OneClickAPI.from_config(self._db)
        base_path = os.path.split(__file__)[0]
        self.resource_path = os.path.join(base_path, "files", "oneclick")

        self.default_patron = self._patron(external_identifier="oneclick_testuser")
        self.default_patron.authorization_identifier="13057226"


    def get_data(self, filename):
        # returns contents of sample file as string and as dict
        path = os.path.join(self.resource_path, filename)
        data = open(path).read()
        return data, json.loads(data)



class TestOneClickAPI(OneClickAPITest):

    def test_get_patron_internal_id(self):
        datastr, datadict = self.get_data("response_patron_internal_id_not_found.json")
        self.api.queue_response(status_code=200, content=datastr)
        oneclick_patron_id = self.api.get_patron_internal_id(patron_cardno='9305722621')
        eq_(None, oneclick_patron_id)

        datastr, datadict = self.get_data("response_patron_internal_id_error.json")
        self.api.queue_response(status_code=500, content=datastr)
        assert_raises_regexp(
            InvalidInputException, "patron_id:", 
            self.api.get_patron_internal_id, patron_cardno='130572262x'
        )

        datastr, datadict = self.get_data("response_patron_internal_id_found.json")
        self.api.queue_response(status_code=200, content=datastr)
        oneclick_patron_id = self.api.get_patron_internal_id(patron_cardno='1305722621')
        eq_(939981, oneclick_patron_id)


    def test_get_patron_information(self):
        datastr, datadict = self.get_data("response_patron_info_not_found.json")
        self.api.queue_response(status_code=404, content=datastr)
        assert_raises_regexp(
            NotFoundOnRemote, "patron_info:", 
            self.api.get_patron_information, patron_id='939987'
        )

        datastr, datadict = self.get_data("response_patron_info_error.json")
        self.api.queue_response(status_code=400, content=datastr)
        assert_raises_regexp(
            InvalidInputException, "patron_info:", 
            self.api.get_patron_information, patron_id='939981fdsfdsf'
        )

        datastr, datadict = self.get_data("response_patron_info_found.json")
        self.api.queue_response(status_code=200, content=datastr)
        patron = self.api.get_patron_information(patron_id='939981')
        eq_(u'1305722621', patron['libraryCardNumber'])
        eq_(u'Mic', patron['firstName'])
        eq_(u'Mouse', patron['lastName'])
        eq_(u'mickeymouse1', patron['userName'])
        eq_(u'mickey1@mouse.com', patron['email'])


    def test_circulate_item(self):
        edition, pool = self._edition(
            identifier_type=Identifier.ONECLICK_ID,
            data_source_name=DataSource.ONECLICK,
            with_license_pool=True, 
            identifier_id = '9781441260468'
        )
        datastr, datadict = self.get_data("response_checkout_success.json")
        self.api.queue_response(status_code=200, content=datastr)

        patron = self.default_patron
        # TODO: decide if want to add oneclick_id as Credential to PatronData db object
        patron.oneclick_id = 939981

        # borrow functionality checks
        response_dictionary = self.api.circulate_item(patron.oneclick_id, edition.primary_identifier.identifier)
        assert('error_code' not in response_dictionary)
        eq_("9781441260468", response_dictionary['isbn'])
        eq_("SUCCESS", response_dictionary['output'])
        eq_(False, response_dictionary['canRenew'])
        #eq_(9828517, response_dictionary['transactionId'])
        eq_(939981, response_dictionary['patronId'])
        eq_(1931, response_dictionary['libraryId'])

        datastr, datadict = self.get_data("response_checkout_unavailable.json")
        self.api.queue_response(status_code=409, content=datastr)
        assert_raises_regexp(
            CannotLoan, "checkout:", 
            self.api.circulate_item, patron.oneclick_id, edition.primary_identifier.identifier
        )

        # book return functionality checks
        self.api.queue_response(status_code=200, content="")

        response_dictionary = self.api.circulate_item(patron.oneclick_id, edition.primary_identifier.identifier, 
            return_item=True)
        eq_({}, response_dictionary)

        datastr, datadict = self.get_data("response_return_unavailable.json")
        self.api.queue_response(status_code=409, content=datastr)
        assert_raises_regexp(
            NotCheckedOut, "checkin:", 
            self.api.circulate_item, patron.oneclick_id, edition.primary_identifier.identifier, 
            return_item=True
        )
        


    def test_checkin(self):
        # Returning a book is, for now, more of a "notify OneClick that we've 
        # returned through Adobe" formality than critical functionality.
        # There's no information returned from the server on success, so we use a 
        # boolean success flag.

        patron = self.default_patron
        patron.oneclick_id = 939981

        edition, pool = self._edition(
            identifier_type=Identifier.ONECLICK_ID,
            data_source_name=DataSource.ONECLICK,
            with_license_pool=True, 
            identifier_id = '9781441260468'
        )

        # queue patron id 
        datastr, datadict = self.get_data("response_patron_internal_id_found.json")
        self.api.queue_response(status_code=200, content=datastr)
        # queue checkin success
        self.api.queue_response(status_code=200, content="")

        success = self.api.checkin(patron, None, pool)
        eq_(True, success)


    def test_checkout(self):
        patron = self.default_patron
        patron.oneclick_id = 939981

        edition, pool = self._edition(
            identifier_type=Identifier.ONECLICK_ID,
            data_source_name=DataSource.ONECLICK,
            with_license_pool=True, 
            identifier_id = '9781441260468'
        )

        # queue patron id 
        datastr, datadict = self.get_data("response_patron_internal_id_found.json")
        self.api.queue_response(status_code=200, content=datastr)
        # queue checkout success
        datastr, datadict = self.get_data("response_checkout_success.json")
        self.api.queue_response(status_code=200, content=datastr)

        loan_info = self.api.checkout(patron, None, pool, None)
        eq_('OneClick ID', loan_info.identifier_type)
        eq_(pool.identifier.identifier, loan_info.identifier)
        today = datetime.datetime.now()
        assert (loan_info.start_date - today).total_seconds() < 20
        assert (loan_info.end_date - today).days < 60
        eq_(None, loan_info.fulfillment_info)


    def test_create_patron(self):
        patron = self.default_patron
        patron.oneclick_id = 939981

        # queue patron id 
        datastr, datadict = self.get_data("response_patron_create_fail_already_exists.json")
        self.api.queue_response(status_code=409, content=datastr)
        assert_raises_regexp(
            RemotePatronCreationFailedException, 'create_patron: http=409, response={"message":"A patron account with the specified username, email address, or card number already exists for this library."}', 
            self.api.create_patron, patron
        )

        datastr, datadict = self.get_data("response_patron_create_success.json")
        self.api.queue_response(status_code=201, content=datastr)
        patron_oneclick_id = self.api.create_patron(patron)

        eq_(940000, patron_oneclick_id)


    def test_fulfill(self):
        patron = self.default_patron
        patron.oneclick_id = 939981

        identifier = self._identifier(
            identifier_type=Identifier.ONECLICK_ID, 
            foreign_id='9781426893483')

        edition, pool = self._edition(
            identifier_type=Identifier.ONECLICK_ID,
            data_source_name=DataSource.ONECLICK,
            with_license_pool=True, 
            identifier_id = '9781426893483'
        )

        # queue patron id 
        datastr, datadict = self.get_data("response_patron_internal_id_found.json")
        self.api.queue_response(status_code=200, content=datastr)

        # queue checkouts list
        datastr, datadict = self.get_data("response_patron_checkouts_200_list.json")
        self.api.queue_response(status_code=200, content=datastr)

        # TODO: Add a separate future method to follow the download link and get the acsm file
        #datastr, datadict = self.get_data("response_fulfillment_sample_acsm_linking_page.json")

        found_fulfillment = self.api.fulfill(patron, None, pool, None)

        eq_('OneClick ID', found_fulfillment.identifier_type)
        eq_(u'9781426893483', found_fulfillment.identifier.identifier)
        eq_(u'http://api.oneclickdigital.us/v1/media/133504/parts/133504/download-url?f=EB00014158.epub&ff=EPUB&acsRId=urn%3Auuid%3A76fca044-0b31-47f7-8ac5-ee0befbda698&tId=9828560&expDt=1479531600', found_fulfillment.content_link)
        eq_(u'application/epub+zip', found_fulfillment.content_type)
        eq_(None, found_fulfillment.content)


    def test_patron_activity(self):
        # Get patron's current checkouts and holds.
        # Make sure LoanInfo objects were created and filled 
        # with FulfillmentInfo objects.  Make sure HoldInfo objects 
        # were created.

        patron = self.default_patron
        patron.oneclick_id = 939981

        identifier = self._identifier(
            identifier_type=Identifier.ONECLICK_ID, 
            foreign_id='9781456103859')

        identifier = self._identifier(
            identifier_type=Identifier.ONECLICK_ID, 
            foreign_id='9781426893483')

        # queue patron id 
        patron_datastr, datadict = self.get_data("response_patron_internal_id_found.json")
        self.api.queue_response(status_code=200, content=patron_datastr)

        # queue checkouts list
        datastr, datadict = self.get_data("response_patron_checkouts_200_list.json")
        self.api.queue_response(status_code=200, content=datastr)

        # queue holds list
        datastr, datadict = self.get_data("response_patron_holds_200_list.json")
        self.api.queue_response(status_code=200, content=datastr)

        (checkouts_list, holds_list) = self.api.patron_activity(patron, None)

        eq_('OneClick ID', checkouts_list[0].identifier_type)
        eq_(u'9781456103859', checkouts_list[0].identifier)
        eq_(None, checkouts_list[0].start_date)
        eq_(datetime.date(2016, 11, 19), checkouts_list[0].end_date)
        eq_(u'http://api.oneclickdigital.us/v1/media/9781456103859/parts/1646772/download-url?s3=78226&f=78226_007_P004', checkouts_list[0].fulfillment_info.content_link)
        eq_(u'audio/mpeg', checkouts_list[0].fulfillment_info.content_type)
                 
        eq_('OneClick ID', checkouts_list[1].identifier_type)
        eq_(u'9781426893483', checkouts_list[1].identifier)
        eq_(None, checkouts_list[1].start_date)
        eq_(datetime.date(2016, 11, 19), checkouts_list[1].end_date)
        eq_(u'http://api.oneclickdigital.us/v1/media/133504/parts/133504/download-url?f=EB00014158.epub&ff=EPUB&acsRId=urn%3Auuid%3A76fca044-0b31-47f7-8ac5-ee0befbda698&tId=9828560&expDt=1479531600', checkouts_list[1].fulfillment_info.content_link)
        eq_(u'application/epub+zip', checkouts_list[1].fulfillment_info.content_type)
                 
        eq_('OneClick ID', holds_list[0].identifier_type)
        eq_('9781426893483', holds_list[0].identifier)
        eq_(None, holds_list[0].start_date)
        eq_(datetime.date(2050, 12, 31), holds_list[0].end_date)
        eq_(0, holds_list[0].hold_position)



    def test_place_hold(self):
        # Test reserving a book.

        patron = self.default_patron
        patron.oneclick_id = 939981

        edition, pool = self._edition(
            identifier_type=Identifier.ONECLICK_ID,
            data_source_name=DataSource.ONECLICK,
            with_license_pool=True, 
            identifier_id = '9781441260468'
        )

        # queue patron id 
        patron_datastr, datadict = self.get_data("response_patron_internal_id_found.json")

        self.api.queue_response(status_code=200, content=patron_datastr)
        datastr, datadict = self.get_data("response_patron_hold_fail_409_already_exists.json")
        self.api.queue_response(status_code=409, content=datastr)
        assert_raises_regexp(
            CannotHold, ".*Hold or Checkout already exists.", 
            self.api.place_hold, patron, None, pool, None
        )

        self.api.queue_response(status_code=200, content=patron_datastr)
        datastr, datadict = self.get_data("response_patron_hold_fail_409_reached_limit.json")
        self.api.queue_response(status_code=409, content=datastr)
        assert_raises_regexp(
            CannotHold, ".*You have reached your checkout limit and therefore are unable to place additional holds.", 
            self.api.place_hold, patron, None, pool, None
        )

        self.api.queue_response(status_code=200, content=patron_datastr)
        datastr, datadict = self.get_data("response_patron_hold_success.json")
        self.api.queue_response(status_code=200, content=datastr)

        hold_info = self.api.place_hold(patron, None, pool, None)

        eq_('OneClick ID', hold_info.identifier_type)
        eq_(pool.identifier.identifier, hold_info.identifier)
        today = datetime.datetime.now()
        assert (hold_info.start_date - today).total_seconds() < 20


    def test_release_hold(self):
        # Test releasing a book resevation early.

        patron = self.default_patron
        patron.oneclick_id = 939981

        edition, pool = self._edition(
            identifier_type=Identifier.ONECLICK_ID,
            data_source_name=DataSource.ONECLICK,
            with_license_pool=True, 
            identifier_id = '9781441260468'
        )

        # queue patron id 
        datastr, datadict = self.get_data("response_patron_internal_id_found.json")
        self.api.queue_response(status_code=200, content=datastr)
        # queue release success
        self.api.queue_response(status_code=200, content="")

        success = self.api.release_hold(patron, None, pool)
        eq_(True, success)


    def test_update_availability(self):
        """Test the OneClick implementation of the update_availability method
        defined by the CirculationAPI interface.
        """

        # Create a LicensePool that needs updating.
        edition, pool = self._edition(
            identifier_type=Identifier.ONECLICK_ID,
            data_source_name=DataSource.ONECLICK,
            with_license_pool=True
        )

        # We have never checked the circulation information for this
        # LicensePool. Put some random junk in the pool to verify
        # that it gets changed.
        pool.licenses_owned = 10
        pool.licenses_available = 5
        pool.patrons_in_hold_queue = 3
        eq_(None, pool.last_checked)

        # Prepare availability information.
        datastr, datadict = self.get_data("response_availability_single_ebook.json")

        # Modify the data so that it appears to be talking about the
        # book we just created.
        new_identifier = pool.identifier.identifier.encode("ascii")
        datastr = datastr.replace("9781781107041", new_identifier)

        self.api.queue_response(status_code=200, content=datastr)

        self.api.update_availability(pool)

        # The availability information has been udpated, as has the
        # date the availability information was last checked.
        eq_(2, pool.licenses_owned)
        eq_(1, pool.licenses_available)
        eq_(0, pool.patrons_in_hold_queue)
        assert pool.last_checked is not None


    def test_update_licensepool_error(self):
        # Create an identifier.
        identifier = self._identifier(
            identifier_type=Identifier.OVERDRIVE_ID
        )
        ignore, availability = self.sample_json(
            "overdrive_availability_information.json"
        )
        api = DummyOverdriveAPI(self._db)
        api.queue_response(response_code=500, content="An error occured.")
        book = dict(id=identifier.identifier, availability_link=self._url)
        pool, was_new, changed = api.update_licensepool(book)
        eq_(None, pool)


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
            identifier_type=Identifier.ONECLICK_ID
        )

        # Make it look like the availability information is for the
        # newly created Identifier.
        raw['id'] = identifier.identifier

        api = DummyOverdriveAPI(self._db)
        pool, was_new = LicensePool.for_foreign_id(
            self._db, DataSource.ONECLICK, 
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



class TestCirculationMonitor(DatabaseTest):

    BIBLIOGRAPHIC_DATA = Metadata(
        DataSource.ONECLICK,
        publisher=u'Random House Inc',
        language='eng', 
        title=u'Faith of My Fathers : A Family Memoir', 
        imprint=u'Random House Inc2',
        published=datetime.datetime(2000, 3, 7, 0, 0),
        primary_identifier=IdentifierData(
            type=Identifier.ONECLICK_ID,
            identifier=u'0003642860'
        ),
        identifiers = [
            IdentifierData(type=Identifier.ISBN, identifier=u'9780375504587')
        ],
        contributors = [
            ContributorData(sort_name=u"McCain, John", 
                            roles=[Contributor.PRIMARY_AUTHOR_ROLE]
                        ),
            ContributorData(sort_name=u"Salter, Mark", 
                            roles=[Contributor.AUTHOR_ROLE]
                        ),
        ],
        subjects = [
            SubjectData(type=Subject.BISAC,
                        identifier=u'BIOGRAPHY & AUTOBIOGRAPHY / Political'),
            SubjectData(type=Subject.FREEFORM_AUDIENCE,
                        identifier=u'Adult'),
        ],
    )

    AVAILABILITY_DATA = CirculationData(
        data_source=DataSource.ONECLICK,
        primary_identifier=BIBLIOGRAPHIC_DATA.primary_identifier, 
        licenses_owned=9,
        licenses_available=8,
        licenses_reserved=0,
        patrons_in_hold_queue=0,
        last_checked=datetime.datetime(2015, 5, 20, 2, 9, 8),
    )


    def test_process_book(self):
        with temp_config() as config:
            monitor = OneClickCirculationMonitor(self._db)
            monitor.api = None
            edition, license_pool = monitor.process_book(
                self.BIBLIOGRAPHIC_DATA, self.AVAILABILITY_DATA)
            eq_(u'Faith of My Fathers : A Family Memoir', edition.title)
            eq_(u'eng', edition.language)
            eq_(u'Random House Inc', edition.publisher)
            eq_(u'Random House Inc2', edition.imprint)

            eq_(Identifier.AXIS_360_ID, edition.primary_identifier.type)
            eq_(u'0003642860', edition.primary_identifier.identifier)

            [isbn] = [x for x in edition.equivalent_identifiers()
                      if x is not edition.primary_identifier]
            eq_(Identifier.ISBN, isbn.type)
            eq_(u'9780375504587', isbn.identifier)

            eq_(["McCain, John", "Salter, Mark"], 
                sorted([x.sort_name for x in edition.contributors]),
            )

            subs = sorted(
                (x.subject.type, x.subject.identifier)
                for x in edition.primary_identifier.classifications
            )
            eq_([(Subject.BISAC, u'BIOGRAPHY & AUTOBIOGRAPHY / Political'), 
                 (Subject.FREEFORM_AUDIENCE, u'Adult')], subs)

            eq_(9, license_pool.licenses_owned)
            eq_(8, license_pool.licenses_available)
            eq_(0, license_pool.patrons_in_hold_queue)
            eq_(datetime.datetime(2015, 5, 20, 2, 9, 8), license_pool.last_checked)

            # Three circulation events were created, backdated to the
            # last_checked date of the license pool.
            events = license_pool.circulation_events
            eq_([u'title_add', u'check_in', u'license_add'], 
                [x.type for x in events])
            for e in events:
                eq_(e.start, license_pool.last_checked)

            # A presentation-ready work has been created for the LicensePool.
            work = license_pool.work
            eq_(True, work.presentation_ready)
            eq_("Faith of My Fathers : A Family Memoir", work.title)

            # A CoverageRecord has been provided for this book in the Axis
            # 360 bibliographic coverage provider, so that in the future
            # it doesn't have to make a separate API request to ask about
            # this book.
            records = [x for x in license_pool.identifier.coverage_records
                       if x.data_source.name == DataSource.ONECLICK
                       and x.operation is None]
            eq_(1, len(records))

    def test_process_book_updates_old_licensepool(self):
        """If the LicensePool already exists, the circulation monitor
        updates it.
        """
        edition, licensepool = self._edition(
            with_license_pool=True, identifier_type=Identifier.ONECLICK_ID,
            identifier_id=u'0003642860'
        )
        # We start off with availability information based on the
        # default for test data.
        eq_(1, licensepool.licenses_owned)

        identifier = IdentifierData(
            type=licensepool.identifier.type,
            identifier=licensepool.identifier.identifier
        )
        metadata = Metadata(DataSource.ONECLICK, primary_identifier=identifier)
        monitor = OneClickCirculationMonitor(self._db)
        monitor.api = None
        edition, licensepool = monitor.process_book(
            metadata, self.AVAILABILITY_DATA
        )

        # Now we have information based on the CirculationData.
        eq_(9, licensepool.licenses_owned)



