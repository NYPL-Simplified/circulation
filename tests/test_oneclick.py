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
    Configuration, 
    temp_config,
)

from core.model import (
    get_one_or_create,
    Contributor,
    DataSource,
    DeliveryMechanism,
    Edition,
    Identifier,
    LicensePool,
    Patron,
    Representation,
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

        self.base_path = os.path.split(__file__)[0]
        self.collection = MockOneClickAPI.mock_collection(self._db)
        self.api = MockOneClickAPI(
            self._db, self.collection, base_path=self.base_path
        )

        self.default_patron = self._patron(external_identifier="oneclick_testuser")
        self.default_patron.authorization_identifier="13057226"



class TestOneClickAPI(OneClickAPITest):

    def test_get_patron_internal_id(self):
        datastr, datadict = self.api.get_data("response_patron_internal_id_not_found.json")
        self.api.queue_response(status_code=200, content=datastr)
        oneclick_patron_id = self.api.get_patron_internal_id(patron_cardno='9305722621')
        eq_(None, oneclick_patron_id)

        datastr, datadict = self.api.get_data("response_patron_internal_id_error.json")
        self.api.queue_response(status_code=500, content=datastr)
        assert_raises_regexp(
            InvalidInputException, "patron_id:", 
            self.api.get_patron_internal_id, patron_cardno='130572262x'
        )

        datastr, datadict = self.api.get_data("response_patron_internal_id_found.json")
        self.api.queue_response(status_code=200, content=datastr)
        oneclick_patron_id = self.api.get_patron_internal_id(patron_cardno='1305722621')
        eq_(939981, oneclick_patron_id)


    def test_get_patron_information(self):
        datastr, datadict = self.api.get_data("response_patron_info_not_found.json")
        self.api.queue_response(status_code=404, content=datastr)
        assert_raises_regexp(
            NotFoundOnRemote, "patron_info:", 
            self.api.get_patron_information, patron_id='939987'
        )

        datastr, datadict = self.api.get_data("response_patron_info_error.json")
        self.api.queue_response(status_code=400, content=datastr)
        assert_raises_regexp(
            InvalidInputException, "patron_info:", 
            self.api.get_patron_information, patron_id='939981fdsfdsf'
        )

        datastr, datadict = self.api.get_data("response_patron_info_found.json")
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
        datastr, datadict = self.api.get_data("response_checkout_success.json")
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
        request_url, request_args, request_kwargs = self.api.requests[-1]
        assert "checkouts" in request_url
        eq_("post", request_kwargs.get("method"))

        datastr, datadict = self.api.get_data("response_checkout_unavailable.json")
        self.api.queue_response(status_code=409, content=datastr)
        assert_raises_regexp(
            CannotLoan, "checkout:", 
            self.api.circulate_item, patron.oneclick_id, edition.primary_identifier.identifier
        )
        request_url, request_args, request_kwargs = self.api.requests[-1]
        assert "checkouts" in request_url
        eq_("post", request_kwargs.get("method"))

        # book return functionality checks
        self.api.queue_response(status_code=200, content="")

        response_dictionary = self.api.circulate_item(patron.oneclick_id, edition.primary_identifier.identifier, 
            return_item=True)
        eq_({}, response_dictionary)
        request_url, request_args, request_kwargs = self.api.requests[-1]
        assert "checkouts" in request_url
        eq_("delete", request_kwargs.get("method"))

        datastr, datadict = self.api.get_data("response_return_unavailable.json")
        self.api.queue_response(status_code=409, content=datastr)
        assert_raises_regexp(
            NotCheckedOut, "checkin:", 
            self.api.circulate_item, patron.oneclick_id, edition.primary_identifier.identifier, 
            return_item=True
        )
        request_url, request_args, request_kwargs = self.api.requests[-1]
        assert "checkouts" in request_url
        eq_("delete", request_kwargs.get("method"))

        # hold functionality checks
        datastr, datadict = self.api.get_data("response_patron_hold_success.json")
        self.api.queue_response(status_code=200, content=datastr)

        response = self.api.circulate_item(patron.oneclick_id, edition.primary_identifier.identifier,
                                           hold=True)
        eq_(9828560, response)
        request_url, request_args, request_kwargs = self.api.requests[-1]
        assert "holds" in request_url
        eq_("post", request_kwargs.get("method"))

        datastr, datadict = self.api.get_data("response_patron_hold_fail_409_reached_limit.json")
        self.api.queue_response(status_code=409, content=datastr)

        response = self.api.circulate_item(patron.oneclick_id, edition.primary_identifier.identifier,
                                           hold=True)
        eq_("You have reached your checkout limit and therefore are unable to place additional holds.",
            response)
        request_url, request_args, request_kwargs = self.api.requests[-1]
        assert "holds" in request_url
        eq_("post", request_kwargs.get("method"))

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
        work = self._work(presentation_edition=edition)

        # queue patron id 
        datastr, datadict = self.api.get_data("response_patron_internal_id_found.json")
        self.api.queue_response(status_code=200, content=datastr)
        # queue checkin success
        self.api.queue_response(status_code=200, content="")

        success = self.api.checkin(patron, None, pool)
        eq_(True, success)

        # queue patron id
        self.api.queue_response(status_code=200, content=datastr)
        # queue unexpected non-empty response from the server
        self.api.queue_response(status_code=200, content=json.dumps({"error_code": "error"}))

        assert_raises(CirculationException, self.api.checkin,
                      patron, None, pool)


    def test_checkout(self):
        patron = self.default_patron
        patron.oneclick_id = 939981

        edition, pool = self._edition(
            identifier_type=Identifier.ONECLICK_ID,
            data_source_name=DataSource.ONECLICK,
            with_license_pool=True, 
            identifier_id = '9781441260468'
        )
        work = self._work(presentation_edition=edition)

        # queue patron id 
        datastr, datadict = self.api.get_data("response_patron_internal_id_found.json")
        self.api.queue_response(status_code=200, content=datastr)
        # queue checkout success
        datastr, datadict = self.api.get_data("response_checkout_success.json")
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

        # queue patron id 
        datastr, datadict = self.api.get_data("response_patron_create_fail_already_exists.json")
        self.api.queue_response(status_code=409, content=datastr)
        assert_raises_regexp(
            RemotePatronCreationFailedException, 'create_patron: http=409, response={"message":"A patron account with the specified username, email address, or card number already exists for this library."}', 
            self.api.create_patron, patron
        )

        datastr, datadict = self.api.get_data("response_patron_create_success.json")
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
        datastr, datadict = self.api.get_data("response_patron_internal_id_found.json")
        self.api.queue_response(status_code=200, content=datastr)

        # queue checkouts list
        datastr, datadict = self.api.get_data("response_patron_checkouts_200_list.json")
        self.api.queue_response(status_code=200, content=datastr)

        epub_manifest = json.dumps({ "url": 'http://api.oneclickdigital.us/v1/media/133504/parts/133504/download-url?f=EB00014158.epub&ff=EPUB&acsRId=urn%3Auuid%3A76fca044-0b31-47f7-8ac5-ee0befbda698&tId=9828560&expDt=1479531600',
                                     "type": Representation.EPUB_MEDIA_TYPE })
        self.api.queue_response(status_code=200, content=epub_manifest)

        found_fulfillment = self.api.fulfill(patron, None, pool, None)

        eq_('OneClick ID', found_fulfillment.identifier_type)
        eq_(u'9781426893483', found_fulfillment.identifier.identifier)
        eq_(u'http://api.oneclickdigital.us/v1/media/133504/parts/133504/download-url?f=EB00014158.epub&ff=EPUB&acsRId=urn%3Auuid%3A76fca044-0b31-47f7-8ac5-ee0befbda698&tId=9828560&expDt=1479531600', found_fulfillment.content_link)
        eq_(u'application/epub+zip', found_fulfillment.content_type)
        eq_(None, found_fulfillment.content)

        # Here's another pool that the patron doesn't have checked out.
        edition2, pool2  = self._edition(
            identifier_type=Identifier.ONECLICK_ID,
            data_source_name=DataSource.ONECLICK,
            with_license_pool=True, 
            identifier_id = '123456789'
        )

        # queue patron id 
        datastr, datadict = self.api.get_data("response_patron_internal_id_found.json")
        self.api.queue_response(status_code=200, content=datastr)

        # queue checkouts list
        datastr, datadict = self.api.get_data("response_patron_checkouts_200_list.json")
        self.api.queue_response(status_code=200, content=datastr)

        epub_manifest = json.dumps({ "url": 'http://api.oneclickdigital.us/v1/media/133504/parts/133504/download-url?f=EB00014158.epub&ff=EPUB&acsRId=urn%3Auuid%3A76fca044-0b31-47f7-8ac5-ee0befbda698&tId=9828560&expDt=1479531600',
                                     "type": Representation.EPUB_MEDIA_TYPE })
        self.api.queue_response(status_code=200, content=epub_manifest)

        # The patron can't fulfill the book if it's not one of their checkouts.
        assert_raises(NoActiveLoan, self.api.fulfill,
                      patron, None, pool2, None)

        # queue patron id 
        datastr, datadict = self.api.get_data("response_patron_internal_id_found.json")
        self.api.queue_response(status_code=200, content=datastr)

        # queue checkouts list
        datastr, datadict = self.api.get_data("response_patron_checkouts_200_emptylist.json")
        self.api.queue_response(status_code=200, content=datastr)

        # The patron also can't fulfill the book if they have no checkouts.
        assert_raises(NoActiveLoan, self.api.fulfill,
                      patron, None, pool, None)


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
        patron_datastr, datadict = self.api.get_data("response_patron_internal_id_found.json")
        self.api.queue_response(status_code=200, content=patron_datastr)

        # queue checkouts list
        datastr, datadict = self.api.get_data("response_patron_checkouts_200_list.json")
        self.api.queue_response(status_code=200, content=datastr)

        # queue a manifest for each checkout
        audio_manifest = json.dumps({ "url": 'http://api.oneclickdigital.us/v1/media/9781456103859/parts/1646772/download-url?s3=78226&f=78226_007_P004',
                                      "type": Representation.MP3_MEDIA_TYPE })
        epub_manifest = json.dumps({ "url": 'http://api.oneclickdigital.us/v1/media/133504/parts/133504/download-url?f=EB00014158.epub&ff=EPUB&acsRId=urn%3Auuid%3A76fca044-0b31-47f7-8ac5-ee0befbda698&tId=9828560&expDt=1479531600',
                                     "type": Representation.EPUB_MEDIA_TYPE })
        self.api.queue_response(status_code=200, content=audio_manifest)
        self.api.queue_response(status_code=200, content=epub_manifest)

        # queue holds list
        datastr, datadict = self.api.get_data("response_patron_holds_200_list.json")
        self.api.queue_response(status_code=200, content=datastr)

        patron_activity = self.api.patron_activity(patron, None)

        eq_('OneClick ID', patron_activity[0].identifier_type)
        eq_(u'9781456103859', patron_activity[0].identifier)
        eq_(None, patron_activity[0].start_date)
        eq_(datetime.date(2016, 11, 19), patron_activity[0].end_date)
        eq_(u'http://api.oneclickdigital.us/v1/media/9781456103859/parts/1646772/download-url?s3=78226&f=78226_007_P004', patron_activity[0].fulfillment_info.content_link)
        eq_(u'audio/mpeg', patron_activity[0].fulfillment_info.content_type)
                 
        eq_('OneClick ID', patron_activity[1].identifier_type)
        eq_(u'9781426893483', patron_activity[1].identifier)
        eq_(None, patron_activity[1].start_date)
        eq_(datetime.date(2016, 11, 19), patron_activity[1].end_date)
        eq_(u'http://api.oneclickdigital.us/v1/media/133504/parts/133504/download-url?f=EB00014158.epub&ff=EPUB&acsRId=urn%3Auuid%3A76fca044-0b31-47f7-8ac5-ee0befbda698&tId=9828560&expDt=1479531600', patron_activity[1].fulfillment_info.content_link)
        eq_(u'application/epub+zip', patron_activity[1].fulfillment_info.content_type)
                 
        eq_('OneClick ID', patron_activity[2].identifier_type)
        eq_('9781426893483', patron_activity[2].identifier)
        eq_(None, patron_activity[2].start_date)
        eq_(datetime.date(2050, 12, 31), patron_activity[2].end_date)
        eq_(None, patron_activity[2].hold_position)



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
        patron_datastr, datadict = self.api.get_data("response_patron_internal_id_found.json")

        self.api.queue_response(status_code=200, content=patron_datastr)
        datastr, datadict = self.api.get_data("response_patron_hold_fail_409_already_exists.json")
        self.api.queue_response(status_code=409, content=datastr)
        assert_raises_regexp(
            CannotHold, ".*Hold or Checkout already exists.", 
            self.api.place_hold, patron, None, pool, None
        )

        self.api.queue_response(status_code=200, content=patron_datastr)
        datastr, datadict = self.api.get_data("response_patron_hold_fail_409_reached_limit.json")
        self.api.queue_response(status_code=409, content=datastr)
        assert_raises_regexp(
            CannotHold, ".*You have reached your checkout limit and therefore are unable to place additional holds.", 
            self.api.place_hold, patron, None, pool, None
        )

        self.api.queue_response(status_code=200, content=patron_datastr)
        datastr, datadict = self.api.get_data("response_patron_hold_success.json")
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
        datastr, datadict = self.api.get_data("response_patron_internal_id_found.json")
        self.api.queue_response(status_code=200, content=datastr)
        # queue release success
        self.api.queue_response(status_code=200, content="")

        success = self.api.release_hold(patron, None, pool)
        eq_(True, success)

        # queue patron id
        self.api.queue_response(status_code=200, content=datastr)
        # queue unexpected non-empty response from the server
        self.api.queue_response(status_code=200, content=json.dumps({"error_code": "error"}))

        assert_raises(CirculationException, self.api.release_hold,
                      patron, None, pool)


    def test_update_licensepool_for_identifier(self):
        """Test the OneClick implementation of the update_availability method
        defined by the CirculationAPI interface.
        """

        # Update a LicensePool that doesn't exist yet, and it gets created.
        identifier = self._identifier(identifier_type=Identifier.ONECLICK_ID)
        isbn = identifier.identifier.encode("ascii")

        # The BibliographicCoverageProvider gets called for a new license pool.
        self.api.queue_response(200, content=json.dumps({}))

        pool, is_new, circulation_changed = self.api.update_licensepool_for_identifier(
            isbn, True, 'ebook'
        )
        eq_(True, is_new)
        eq_(True, circulation_changed)
        eq_(999, pool.licenses_owned)
        eq_(999, pool.licenses_available)
        [lpdm] = pool.delivery_mechanisms
        eq_(Representation.EPUB_MEDIA_TYPE, lpdm.delivery_mechanism.content_type)
        eq_(DeliveryMechanism.ADOBE_DRM, lpdm.delivery_mechanism.drm_scheme)

        # Create a LicensePool that needs updating.
        edition, pool = self._edition(
            identifier_type=Identifier.ONECLICK_ID,
            data_source_name=DataSource.ONECLICK,
            with_license_pool=True, collection=self.collection
        )

        # We have never checked the circulation information for this
        # LicensePool. Put some random junk in the pool to verify
        # that it gets changed.
        pool.licenses_owned = 5
        pool.licenses_available = 3
        pool.patrons_in_hold_queue = 3
        eq_(None, pool.last_checked)

        isbn = pool.identifier.identifier.encode("ascii")

        pool, is_new, circulation_changed = self.api.update_licensepool_for_identifier(
            isbn, False, 'eaudio'
        )
        eq_(False, is_new)
        eq_(True, circulation_changed)

        # The availability information has been updated, as has the
        # date the availability information was last checked.
        eq_(0, pool.licenses_owned)
        eq_(0, pool.licenses_available)
        eq_(3, pool.patrons_in_hold_queue)
        assert pool.last_checked is not None

        # A delivery mechanism was also added to the pool.
        [lpdm] = pool.delivery_mechanisms
        eq_(Representation.MP3_MEDIA_TYPE, lpdm.delivery_mechanism.content_type)
        eq_(None, lpdm.delivery_mechanism.drm_scheme)

        self.api.update_licensepool_for_identifier(isbn, True, 'ebook')
        eq_(999, pool.licenses_owned)
        eq_(999, pool.licenses_available)
        eq_(3, pool.patrons_in_hold_queue)


class TestCirculationMonitor(OneClickAPITest):

    def test_process_availability(self):
        monitor = OneClickCirculationMonitor(
            self._db, self.collection, api_class=MockOneClickAPI, 
            api_class_kwargs=dict(base_path=self.base_path)
        )

        # Create a LicensePool that needs updating.
        edition_ebook, pool_ebook = self._edition(
            identifier_type=Identifier.ONECLICK_ID,
            data_source_name=DataSource.ONECLICK,
            with_license_pool=True, collection=self.collection
        )
        pool_ebook.licenses_owned = 3
        pool_ebook.licenses_available = 2
        pool_ebook.patrons_in_hold_queue = 1
        eq_(None, pool_ebook.last_checked)

        # Prepare availability information.
        datastr, datadict = monitor.api.get_data("response_availability_single_ebook.json")

        # Modify the data so that it appears to be talking about the
        # book we just created.
        new_identifier = pool_ebook.identifier.identifier.encode("ascii")
        datastr = datastr.replace("9781781107041", new_identifier)
        monitor.api.queue_response(status_code=200, content=datastr)

        item_count = monitor.process_availability()
        eq_(1, item_count)
        pool_ebook.licenses_available = 0
