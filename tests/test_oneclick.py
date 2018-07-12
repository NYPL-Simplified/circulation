import datetime
import json
import uuid
from lxml import etree
from nose.tools import (
    eq_, 
    assert_raises,
    assert_raises_regexp,
    set_trace,
)
import os
from StringIO import StringIO

from api.authenticator import BasicAuthenticationProvider
from api.config import (
    Configuration, 
    temp_config,
)

from core.model import (
    get_one_or_create,
    ConfigurationSetting,
    Contributor,
    Credential,
    DataSource,
    DeliveryMechanism,
    Edition,
    ExternalIntegration,
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
    AudiobookManifest,
    OneClickAPI,
    OneClickCirculationMonitor, 
    MockOneClickAPI,
    RBFulfillmentInfo,
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
        # Make sure the default library is created so that it will
        # be configured properly with the mock collection.
        self._default_library
        self.collection = MockOneClickAPI.mock_collection(self._db)
        self.api = MockOneClickAPI(
            self._db, self.collection, base_path=self.base_path
        )
        self.default_patron = self._patron(external_identifier="oneclick_testuser")
        self.default_patron.authorization_identifier="13057226"


class TestOneClickAPI(OneClickAPITest):

    def test__run_self_tests(self):
        class Mock(MockOneClickAPI):
            """Mock the methods invoked by the self-test."""

            # We're going to count the number of items in the
            # eBook and eAudio collections.
            def get_ebook_availability_info(self, media_type):
                if media_type=='eBook':
                    return []
                elif media_type=='eAudio':
                    # Three titles - one available, one unavailable, and
                    # one with availability missing.
                    return [
                        dict(availability=False),
                        dict(availability=True),
                        dict(),
                    ]

            # Then for each collection with a default patron, we're
            # going to see how many loans and holds the default patron
            # has.
            patron_activity_called_with = []
            def patron_activity(self, patron, pin):
                self.patron_activity_called_with.append(
                    (patron.authorization_identifier, pin)
                )
                return [1,2,3]

        # Now let's make sure two Libraries have access to this
        # Collection -- one library with a default patron and one
        # without.
        no_default_patron = self._library()
        self.collection.libraries.append(no_default_patron)

        with_default_patron = self._default_library
        integration = self._external_integration(
            "api.simple_authentication",
            ExternalIntegration.PATRON_AUTH_GOAL,
            libraries=[with_default_patron]
        )
        p = BasicAuthenticationProvider
        integration.setting(p.TEST_IDENTIFIER).value = "username1"
        integration.setting(p.TEST_PASSWORD).value = "password1"

        # Now that everything is set up, run the self-test.
        api = Mock(self._db, self.collection)
        results = sorted(
            api._run_self_tests(self._db), key=lambda x: x.name
        )
        [no_patron_credential, patron_activity, audio_count, ebook_count] = results

        # Verify that each test method was called and returned the
        # expected SelfTestResult object.
        eq_(
            "Acquiring test patron credentials for library %s" % no_default_patron.name,
            no_patron_credential.name
        )
        eq_(False, no_patron_credential.success)
        eq_("Library has no test patron configured.",
            no_patron_credential.exception.message)

        eq_("Checking patron activity, using test patron for library %s" % with_default_patron.name,
            patron_activity.name)
        eq_(True, patron_activity.success)
        eq_("Total loans and holds: 3", patron_activity.result)
        eq_([("username1", "password1")], api.patron_activity_called_with)

        eq_("Counting audiobooks in collection", audio_count.name)
        eq_(True, audio_count.success)
        eq_("Total items: 3 (1 currently loanable, 2 currently not loanable)",
            audio_count.result)

        eq_("Counting ebooks in collection", ebook_count.name)
        eq_(True, ebook_count.success)
        eq_("Total items: 0 (0 currently loanable, 0 currently not loanable)",
            ebook_count.result)

    def test__run_self_tests_short_circuit(self):
        """Simulate a self-test run on an improperly configured
        site.
        """
        error = dict(message='Invalid library id is provided or permission denied')
        class Mock(MockOneClickAPI):
            def get_ebook_availability_info(self, media_type):
                return error

        api = Mock(self._db, self.collection)
        [result] = api._run_self_tests(self._db)

        # We gave up after the first test failed.
        eq_("Counting ebooks in collection", result.name)
        eq_("Invalid library id is provided or permission denied", result.exception.message)
        eq_(repr(error), result.exception.debug_message)

    def test_external_integration(self):
        eq_(self.collection.external_integration,
            self.api.external_integration(self._db))

    def queue_initial_patron_id_lookup(self):
        """All the OneClickAPI methods that take a Patron object call
        self.patron_remote_identifier() immediately, to find the
        patron's RBdigital ID.
        
        Since the default_patron starts out without a Credential
        containing that ID, this means making a request to the
        RBdigital API to look up an existing ID. If that lookup fails,
        it means a call to create_patron() and another API call.

        It's important to test that all these methods call
        patron_remote_identifier(), so this helper method queues up a
        response to the "lookup" request that makes it look like the
        Patron has an RBdigital ID but for whatever reason they are
        missing their Credential.
        """
        patron_datastr, datadict = self.api.get_data(
            "response_patron_internal_id_found.json"
        )
        self.api.queue_response(status_code=200, content=patron_datastr)

    def _assert_patron_has_remote_identifier_credential(
            self, patron, external_id
    ):
        """Assert that the given Patron has a permanent Credential
        storing their RBdigital ID.
        """
        [credential] = patron.credentials
        eq_(DataSource.RB_DIGITAL, credential.data_source.name)
        eq_(Credential.IDENTIFIER_FROM_REMOTE_SERVICE, credential.type)
        eq_(external_id, credential.credential)
        eq_(None, credential.expires)

    def _set_notification_address(self, library):
        """Set the default notification address for the given library.

        This is necessary to create RBdigital user accounts for its
        patrons.
        """
        ConfigurationSetting.for_library(
            Configuration.DEFAULT_NOTIFICATION_EMAIL_ADDRESS, library
        ).value = 'genericemail@library.org'

    def test_patron_remote_identifier_new_patron(self):

        class NeverHeardOfYouAPI(OneClickAPI):
            """A mock OneClickAPI that has never heard of any patron
            and returns a known ID as a way of registering them.
            """
            def patron_remote_identifier_lookup(self, patron):
                """This API has never heard of any patron."""
                return None

            def create_patron(self, patron):
                return "generic id"

        api = NeverHeardOfYouAPI(self._db, self.collection)

        patron = self.default_patron

        # If it turns out the API has never heard of a given patron, a
        # second call is made to create_patron().
        eq_("generic id", api.patron_remote_identifier(patron))

        # A permanent Credential has been created for the remote
        # identifier.
        self._assert_patron_has_remote_identifier_credential(
            patron, "generic id"
        )

    def test_patron_remote_identifier_existing_patron(self):

        class IKnowYouAPI(OneClickAPI):
            """A mock OneClickAPI that has heard of any given
            patron but will refuse to register a new patron.
            """
            def patron_remote_identifier_lookup(self, patron):
                return "i know you"

            def create_patron(self, patron):
                raise Exception("No new patrons!")

        api = IKnowYouAPI(self._db, self.collection)

        patron = self.default_patron

        # If it turns out the API has heard of a given patron, no call
        # is made to create_patron() -- if it happened here the test
        # would explode.
        eq_("i know you", api.patron_remote_identifier(patron))

        # A permanent Credential has been created for the remote
        # identifier.
        self._assert_patron_has_remote_identifier_credential(
            patron, "i know you"
        )

    def test_patron_remote_email_address(self):

        patron = self.default_patron

        # Without a setting for DEFAULT_NOTIFICATION_EMAIL_ADDRESS, we
        # can't calculate the email address to send RBdigital for a
        # patron.
        assert_raises_regexp(
            RemotePatronCreationFailedException,
            "Cannot create remote account for patron because library's default notification address is not set.",
            self.api.remote_email_address, patron
        )

        self._set_notification_address(patron.library)
        address = self.api.remote_email_address(patron)

        # A credential was created to use when talking to RBdigital
        # about this patron.
        [credential] = patron.credentials

        # The credential and default notification email address were
        # used to construct the patron's
        eq_("genericemail+rbdigital-%s@library.org" % credential.credential,
            address)

    def test_patron_remote_identifier_lookup(self):

        patron = self.default_patron

        # Get the identifier we use when announcing this patron to 
        # the remote service.
        patron_identifier = patron.identifier_to_remote_service(
            DataSource.RB_DIGITAL
        )

        # If that identifier is not already registered with the remote
        # service, patron_remote_identifier_lookup returns None.
        datastr, datadict = self.api.get_data(
            "response_patron_internal_id_not_found.json"
        )
        self.api.queue_response(status_code=200, content=datastr)
        oneclick_patron_id = self.api.patron_remote_identifier_lookup(patron)
        eq_(None, oneclick_patron_id)

        # The patron's otherwise meaningless
        # identifier-to-remote-service was used to identify the patron
        # to RBdigital, as opposed to any library-specific identifier.
        [request] = self.api.requests
        url = request[0]
        assert patron_identifier in url

        # If no identifier is provided, the server sends an exception
        # which is converted to an InvalidInputException.
        datastr, datadict = self.api.get_data(
            "response_patron_internal_id_error.json"
        )
        self.api.queue_response(status_code=500, content=datastr)
        assert_raises_regexp(
            InvalidInputException, "patron_id:", 
            self.api.patron_remote_identifier_lookup, patron
        )

        # When the patron's identifier is already registered with
        # RBdigital (due to an earlier create_patron() call),
        # patron_remote_identifier_lookup returns the patron's
        # RBdigital ID.
        self.queue_initial_patron_id_lookup()
        oneclick_patron_id = self.api.patron_remote_identifier_lookup(patron)
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
            identifier_type=Identifier.RB_DIGITAL_ID,
            data_source_name=DataSource.RB_DIGITAL,
            with_license_pool=True, 
            identifier_id = '9781441260468'
        )
        datastr, datadict = self.api.get_data("response_checkout_success.json")
        self.api.queue_response(status_code=200, content=datastr)

        patron = self.default_patron

        # We don't need to go through the process of establishing this
        # patron's RBdigital ID -- just make one up.
        oneclick_id = self._str

        # borrow functionality checks
        response_dictionary = self.api.circulate_item(oneclick_id, edition.primary_identifier.identifier)
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
            NoAvailableCopies, "Title is not available for checkout", 
            self.api.circulate_item, oneclick_id, edition.primary_identifier.identifier
        )
        request_url, request_args, request_kwargs = self.api.requests[-1]
        assert "checkouts" in request_url
        eq_("post", request_kwargs.get("method"))

        # book return functionality checks
        self.api.queue_response(status_code=200, content="")

        response_dictionary = self.api.circulate_item(oneclick_id, edition.primary_identifier.identifier, 
            return_item=True)
        eq_({}, response_dictionary)
        request_url, request_args, request_kwargs = self.api.requests[-1]
        assert "checkouts" in request_url
        eq_("delete", request_kwargs.get("method"))

        datastr, datadict = self.api.get_data("response_return_unavailable.json")
        self.api.queue_response(status_code=409, content=datastr)
        assert_raises_regexp(
            NotCheckedOut, "checkin:", 
            self.api.circulate_item, oneclick_id, edition.primary_identifier.identifier, 
            return_item=True
        )
        request_url, request_args, request_kwargs = self.api.requests[-1]
        assert "checkouts" in request_url
        eq_("delete", request_kwargs.get("method"))

        # hold functionality checks
        datastr, datadict = self.api.get_data("response_patron_hold_success.json")
        self.api.queue_response(status_code=200, content=datastr)

        response = self.api.circulate_item(oneclick_id, edition.primary_identifier.identifier,
                                           hold=True)
        eq_(9828560, response)
        request_url, request_args, request_kwargs = self.api.requests[-1]
        assert "holds" in request_url
        eq_("post", request_kwargs.get("method"))

        datastr, datadict = self.api.get_data("response_patron_hold_fail_409_reached_limit.json")
        self.api.queue_response(status_code=409, content=datastr)

        response = self.api.circulate_item(oneclick_id, edition.primary_identifier.identifier,
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
        self.queue_initial_patron_id_lookup()

        edition, pool = self._edition(
            identifier_type=Identifier.RB_DIGITAL_ID,
            data_source_name=DataSource.RB_DIGITAL,
            with_license_pool=True, 
            identifier_id = '9781441260468'
        )
        work = self._work(presentation_edition=edition)

        # queue checkin success
        self.api.queue_response(status_code=200, content='{"message": "success"}')

        success = self.api.checkin(patron, None, pool)
        eq_(True, success)

        # queue unexpected non-empty response from the server
        self.api.queue_response(status_code=200, content=json.dumps({"error_code": "error"}))

        assert_raises(CirculationException, self.api.checkin,
                      patron, None, pool)

    def test_checkout(self):

        # Ebooks and audiobooks have different loan durations.
        ebook_period = self.api.collection.default_loan_period(
            self._default_library, Edition.BOOK_MEDIUM
        )
        audio_period = self.api.collection.default_loan_period(
            self._default_library, Edition.AUDIO_MEDIUM
        )
        assert ebook_period != audio_period

        patron = self.default_patron
        self.queue_initial_patron_id_lookup()

        edition, pool = self._edition(
            identifier_type=Identifier.RB_DIGITAL_ID,
            data_source_name=DataSource.RB_DIGITAL,
            with_license_pool=True, 
            identifier_id = '9781441260468'
        )
        work = self._work(presentation_edition=edition)

        # The second request will actually check out the book.
        datastr, datadict = self.api.get_data("response_checkout_success.json")
        self.api.queue_response(status_code=200, content=datastr)

        loan_info = self.api.checkout(patron, None, pool, None)

        checkout_url = self.api.requests[-1][0]
        assert "days=%s" % ebook_period in checkout_url

        # Now we have a LoanInfo that describes the remote loan.
        eq_(Identifier.RB_DIGITAL_ID, loan_info.identifier_type)
        eq_(pool.identifier.identifier, loan_info.identifier)
        today = datetime.datetime.utcnow()
        assert (loan_info.start_date - today).total_seconds() < 20
        assert (loan_info.end_date - today).days <= ebook_period

        # But we can only get a FulfillmentInfo by calling
        # get_patron_checkouts().
        eq_(None, loan_info.fulfillment_info)

        # Try the checkout again but pretend that we're checking out
        # an audiobook.
        #
        edition.medium = Edition.AUDIO_MEDIUM
        self.api.queue_response(status_code=200, content=datastr)
        loan_info = self.api.checkout(patron, None, pool, None)

        # We requested a different loan duration.
        checkout_url = self.api.requests[-1][0]
        assert "days=%s" % audio_period in checkout_url
        assert (loan_info.end_date - today).days <= audio_period

    def test_create_patron(self):
        """Test the method that creates an account for a library patron
        on the RBdigital side.
        """
        patron = self.default_patron
        self._set_notification_address(patron.library)

        # If the patron already has an account, a
        # RemotePatronCreationFailedException is raised.
        datastr, datadict = self.api.get_data("response_patron_create_fail_already_exists.json")
        self.api.queue_response(status_code=409, content=datastr)
        assert_raises_regexp(
            RemotePatronCreationFailedException, 'create_patron: http=409, response={"message":"A patron account with the specified username, email address, or card number already exists for this library."}', 
            self.api.create_patron, patron
        )

        # Otherwise, the account is created.
        datastr, datadict = self.api.get_data(
            "response_patron_create_success.json"
        )
        self.api.queue_response(status_code=201, content=datastr)
        patron_oneclick_id = self.api.create_patron(patron)

        # The patron's remote account ID is returned.
        eq_(940000, patron_oneclick_id)

        # The data sent to RBdigital is based on the patron's
        # identifier-to-remote-service.
        remote = patron.identifier_to_remote_service(
            DataSource.RB_DIGITAL
        )

        form_data = json.loads(self.api.requests[-1][-1]['data'])

        # No identifying information was sent to RBdigital, only information
        # based on the RBdigital-specific identifier.
        eq_(self.api.library_id, form_data['libraryId'])
        eq_(remote, form_data['libraryCardNumber'])
        eq_("username" + (remote.replace("-", '')), form_data['userName'])
        eq_("genericemail+rbdigital-%s@library.org" % remote, form_data['email'])
        eq_("Patron", form_data['firstName'])
        eq_("Reader", form_data['lastName'])


    def test_fulfill(self):
        patron = self.default_patron
        self.queue_initial_patron_id_lookup()

        identifier = self._identifier(
            identifier_type=Identifier.RB_DIGITAL_ID, 
            foreign_id='9781426893483')

        edition, pool = self._edition(
            identifier_type=Identifier.RB_DIGITAL_ID,
            data_source_name=DataSource.RB_DIGITAL,
            with_license_pool=True, 
            identifier_id = '9781426893483'
        )

        # The first request will look up the patron's current loans.
        datastr, datadict = self.api.get_data("response_patron_checkouts_200_list.json")
        self.api.queue_response(status_code=200, content=datastr)

        found_fulfillment = self.api.fulfill(patron, None, pool, None)
        assert isinstance(found_fulfillment, RBFulfillmentInfo)

        # We have a FulfillmentInfo-like object, but it hasn't yet
        # made the second request that will give us the actual URL to
        # download. (We know this, because the response to that
        # request has not been queued yet.)

        # Let's queue it up now.
        download_url  = u"http://download_url/"
        epub_manifest = json.dumps({ "url": download_url,
                                     "type": Representation.EPUB_MEDIA_TYPE })
        self.api.queue_response(status_code=200, content=epub_manifest)

        # Since the book being fulfilled is an EPUB, the
        # FulfillmentInfo returned contains a direct link to the EPUB.
        eq_(Identifier.RB_DIGITAL_ID, found_fulfillment.identifier_type)
        eq_(u'9781426893483', found_fulfillment.identifier)
        eq_(download_url, found_fulfillment.content_link)
        eq_(u'application/epub+zip', found_fulfillment.content_type)
        eq_(None, found_fulfillment.content)
        
        # The fulfillment link expires in about 14 minutes -- rather
        # than testing this exactly we estimate it.
        expires = found_fulfillment.content_expires
        now = datetime.datetime.utcnow()
        thirteen_minutes = now + datetime.timedelta(minutes=13)
        fifteen_minutes = now + datetime.timedelta(minutes=15)
        assert expires > thirteen_minutes
        assert expires < fifteen_minutes

        # Here's another pool that the patron doesn't have checked out.
        edition2, pool2  = self._edition(
            identifier_type=Identifier.RB_DIGITAL_ID,
            data_source_name=DataSource.RB_DIGITAL,
            with_license_pool=True, 
            identifier_id = '123456789'
        )

        # Since the Patron now has a Credential containing their
        # RBdigital ID, there will be no initial request looking up their
        # RBdigital ID.

        # Instead we'll go right to the list of active loans, where we'll 
        # find out that the patron does not have an active loan for the 
        # requested book.
        datastr, datadict = self.api.get_data("response_patron_checkouts_200_list.json")
        self.api.queue_response(status_code=200, content=datastr)

        # The patron can't fulfill the book if it's not one of their checkouts.
        assert_raises(NoActiveLoan, self.api.fulfill,
                      patron, None, pool2, None)

        # Try again with a scenario where the patron has no active
        # loans at all.
        datastr, datadict = self.api.get_data("response_patron_checkouts_200_emptylist.json")
        self.api.queue_response(status_code=200, content=datastr)

        assert_raises(NoActiveLoan, self.api.fulfill,
                      patron, None, pool, None)


    def test_fulfill_audiobook(self):
        """Verify that fulfilling an audiobook results in a manifest
        document.

        The manifest document is not currently in a standard
        form, but we'll add that later.
        """
        patron = self.default_patron
        self.queue_initial_patron_id_lookup()

        audiobook_id = '9781449871789'
        identifier = self._identifier(
            identifier_type=Identifier.RB_DIGITAL_ID, 
            foreign_id=audiobook_id)

        edition, pool = self._edition(
            identifier_type=Identifier.RB_DIGITAL_ID,
            data_source_name=DataSource.RB_DIGITAL,
            with_license_pool=True, 
            identifier_id = audiobook_id
        )

        # The only request we will make will be to look up the
        # patron's current loans.
        datastr, datadict = self.api.get_data(
            "response_patron_checkouts_with_audiobook.json"
        )
        self.api.queue_response(status_code=200, content=datastr)

        found_fulfillment = self.api.fulfill(patron, None, pool, None)
        assert isinstance(found_fulfillment, RBFulfillmentInfo)

        # Without making any further HTTP requests, we were able to get
        # a Readium Web Publication manifest for the loan.
        eq_(Representation.AUDIOBOOK_MANIFEST_MEDIA_TYPE, 
            found_fulfillment.content_type)

        manifest = json.loads(found_fulfillment.content)
        eq_('http://readium.org/webpub/default.jsonld', manifest['@context'])
        eq_('http://bib.schema.org/Audiobook', manifest['metadata']['@type'])

    def test_patron_activity(self):
        # Get patron's current checkouts and holds.
        # Make sure LoanInfo objects were created and filled 
        # with FulfillmentInfo objects.  Make sure HoldInfo objects 
        # were created.

        patron = self.default_patron
        self.queue_initial_patron_id_lookup()

        identifier = self._identifier(
            identifier_type=Identifier.RB_DIGITAL_ID, 
            foreign_id='9781456103859')

        identifier = self._identifier(
            identifier_type=Identifier.RB_DIGITAL_ID, 
            foreign_id='9781426893483')

        # queue checkouts list
        datastr, datadict = self.api.get_data("response_patron_checkouts_200_list.json")
        self.api.queue_response(status_code=200, content=datastr)

        # queue holds list
        datastr, datadict = self.api.get_data("response_patron_holds_200_list.json")
        self.api.queue_response(status_code=200, content=datastr)

        patron_activity = self.api.patron_activity(patron, None)

        eq_(Identifier.RB_DIGITAL_ID, patron_activity[0].identifier_type)
        eq_(u'9781456103859', patron_activity[0].identifier)
        eq_(None, patron_activity[0].start_date)
        eq_(datetime.date(2016, 11, 19), patron_activity[0].end_date)
                 
        eq_(Identifier.RB_DIGITAL_ID, patron_activity[1].identifier_type)
        eq_(u'9781426893483', patron_activity[1].identifier)
        eq_(None, patron_activity[1].start_date)
        eq_(datetime.date(2016, 11, 19), patron_activity[1].end_date)
                 
        eq_(Identifier.RB_DIGITAL_ID, patron_activity[2].identifier_type)
        eq_('9781426893483', patron_activity[2].identifier)
        eq_(None, patron_activity[2].start_date)
        eq_(datetime.date(2050, 12, 31), patron_activity[2].end_date)
        eq_(None, patron_activity[2].hold_position)

    def test_place_hold(self):
        "Test reserving a book."

        patron = self.default_patron
        self.queue_initial_patron_id_lookup()

        edition, pool = self._edition(
            identifier_type=Identifier.RB_DIGITAL_ID,
            data_source_name=DataSource.RB_DIGITAL,
            with_license_pool=True, 
            identifier_id = '9781441260468'
        )

        # If the book is already on hold or already checked out,
        # CannotHold is raised. (It's not AlreadyOnHold/AlreadyCheckedOut
        # because we can't distinguish between the two cases.)
        datastr, datadict = self.api.get_data("response_patron_hold_fail_409_already_exists.json")
        self.api.queue_response(status_code=409, content=datastr)
        assert_raises_regexp(
            CannotHold, ".*Hold or Checkout already exists.", 
            self.api.place_hold, patron, None, pool, None
        )

        # If the patron has reached a limit and cannot place any more holds,
        # CannotHold is raised.
        datastr, datadict = self.api.get_data("response_patron_hold_fail_409_reached_limit.json")
        self.api.queue_response(status_code=409, content=datastr)
        assert_raises_regexp(
            CannotHold, ".*You have reached your checkout limit and therefore are unable to place additional holds.", 
            self.api.place_hold, patron, None, pool, None
        )

        # Finally let's test a successful hold.
        datastr, datadict = self.api.get_data("response_patron_hold_success.json")
        self.api.queue_response(status_code=200, content=datastr)

        hold_info = self.api.place_hold(patron, None, pool, None)

        eq_(Identifier.RB_DIGITAL_ID, hold_info.identifier_type)
        eq_(pool.identifier.identifier, hold_info.identifier)
        today = datetime.datetime.now()
        assert (hold_info.start_date - today).total_seconds() < 20


    def test_release_hold(self):
        "Test releasing a book resevation early."

        patron = self.default_patron
        self.queue_initial_patron_id_lookup()

        edition, pool = self._edition(
            identifier_type=Identifier.RB_DIGITAL_ID,
            data_source_name=DataSource.RB_DIGITAL,
            with_license_pool=True, 
            identifier_id = '9781441260468'
        )

        # queue release success
        self.api.queue_response(status_code=200, content='{"message": "success"}')

        success = self.api.release_hold(patron, None, pool)
        eq_(True, success)

        # queue unexpected non-empty response from the server
        self.api.queue_response(status_code=200, content=json.dumps({"error_code": "error"}))

        assert_raises(CirculationException, self.api.release_hold,
                      patron, None, pool)


    def test_update_licensepool_for_identifier(self):
        """Test the OneClick implementation of the update_availability method
        defined by the CirculationAPI interface.
        """

        # Update a LicensePool that doesn't exist yet, and it gets created.
        identifier = self._identifier(identifier_type=Identifier.RB_DIGITAL_ID)
        isbn = identifier.identifier.encode("ascii")

        # The BibliographicCoverageProvider gets called for a new license pool.
        self.api.queue_response(200, content=json.dumps({}))

        pool, is_new, circulation_changed = self.api.update_licensepool_for_identifier(
            isbn, True, 'ebook'
        )
        eq_(True, is_new)
        eq_(True, circulation_changed)
        eq_(1, pool.licenses_owned)
        eq_(1, pool.licenses_available)
        [lpdm] = pool.delivery_mechanisms
        eq_(Representation.EPUB_MEDIA_TYPE, lpdm.delivery_mechanism.content_type)
        eq_(DeliveryMechanism.ADOBE_DRM, lpdm.delivery_mechanism.drm_scheme)

        # Create a LicensePool that needs updating.
        edition, pool = self._edition(
            identifier_type=Identifier.RB_DIGITAL_ID,
            data_source_name=DataSource.RB_DIGITAL,
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
        #
        # We still own a license, but it's no longer available for
        # checkout.
        eq_(1, pool.licenses_owned)
        eq_(0, pool.licenses_available)
        eq_(3, pool.patrons_in_hold_queue)
        assert pool.last_checked is not None

        # A delivery mechanism was also added to the pool.
        [lpdm] = pool.delivery_mechanisms
        eq_(Representation.AUDIOBOOK_MANIFEST_MEDIA_TYPE,
            lpdm.delivery_mechanism.content_type)
        eq_(None, lpdm.delivery_mechanism.drm_scheme)

        self.api.update_licensepool_for_identifier(isbn, True, 'ebook')
        eq_(1, pool.licenses_owned)
        eq_(1, pool.licenses_available)
        eq_(3, pool.patrons_in_hold_queue)


class TestCirculationMonitor(OneClickAPITest):

    def test_process_availability(self):
        monitor = OneClickCirculationMonitor(
            self._db, self.collection, api_class=MockOneClickAPI, 
            api_class_kwargs=dict(base_path=self.base_path)
        )
        eq_(ExternalIntegration.RB_DIGITAL, monitor.protocol)

        # Create a LicensePool that needs updating.
        edition_ebook, pool_ebook = self._edition(
            identifier_type=Identifier.RB_DIGITAL_ID,
            data_source_name=DataSource.RB_DIGITAL,
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


class TestAudiobookManifest(OneClickAPITest):

    def test_constructor(self):
        """A reasonable RBdigital manifest becomes a reasonable
        AudiobookManifest object.
        """
        ignore, [book] = self.api.get_data(
            "response_patron_checkouts_with_audiobook.json"
        )
        manifest = AudiobookManifest(book)

        # We know about a lot of metadata.
        eq_('http://bib.schema.org/Audiobook', manifest.metadata['@type'])
        eq_(u'Sharyn McCrumb', manifest.metadata['author'])
        eq_(u'Award-winning, New York Times best-selling novelist Sharyn McCrumb crafts absorbing, lyrical tales featuring the rich culture and lore of Appalachia. In the compelling...', manifest.metadata['description'])
        eq_(52710.0, manifest.metadata['duration'])
        eq_(u'9781449871789', manifest.metadata['identifier'])
        eq_(u'Barbara Rosenblat', manifest.metadata['narrator'])
        eq_(u'Recorded Books, Inc.', manifest.metadata['publisher'])
        eq_(u'', manifest.metadata['rbdigital:encryptionKey'])
        eq_(False, manifest.metadata['rbdigital:hasDrm'])
        eq_(316314528, manifest.metadata['schema:contentSize'])
        eq_(u'The Ballad of Frankie Silver', manifest.metadata['title'])

        # We know about 21 spine items.
        eq_(21, len(manifest.spine))

        # Let's spot check one.
        first = manifest.spine[0]
        eq_("358456", first['rbdigital:id'])
        eq_("https://download-piece/1", first['href'])
        eq_("audio/mpeg", first['type'])
        eq_(417200, first['schema:contentSize'])
        eq_("Introduction", first['title'])
        eq_(69.0, first['duration'])

        # An alternate link and a cover link were imported.
        alternate, cover = manifest.links
        eq_("alternate", alternate['rel'])
        eq_("https://download/full-book.zip", alternate['href'])
        eq_("application/zip", alternate['type'])

        eq_("cover", cover['rel'])
        assert "image_512x512" in cover['href']
        eq_("image/png", cover['type'])

    def test_empty_constructor(self):
        """An empty RBdigital manifest becomes an empty AudioManifest
        object.

        The manifest will not be useful -- this is just to test that
        the constructor can move forward in the absence of any
        particular input.
        """
        manifest = AudiobookManifest({})

        # We know it's an audiobook, and that's it.
        eq_(
            {'@context': 'http://readium.org/webpub/default.jsonld', 
             'metadata': {'@type': 'http://bib.schema.org/Audiobook'}},
            manifest.as_dict
        )

    def test_best_cover(self):
        m = AudiobookManifest.best_cover

        # If there are no covers, or no URLs, None is returned.
        eq_(None, m(None))
        eq_(None, m([]))
        eq_(None, m([{'nonsense': 'value'}]))
        eq_(None, m([{'name': 'xx-large'}]))
        eq_(None, m([{'url': 'somewhere'}]))

        # No image with a name other than 'large', 'x-large', or
        # 'xx-large' will be accepted.
        eq_(None, m([{'name': 'xx-small', 'url': 'foo'}]))

        # Of those, the largest sized image will be used.
        eq_('yep', m([
            {'name': 'small', 'url': 'no way'},
            {'name': 'large', 'url': 'nope'},
            {'name': 'x-large', 'url': 'still nope'},
            {'name': 'xx-large', 'url': 'yep'},
        ]))
