import datetime
from dateutil.relativedelta import relativedelta
import json
from lxml import etree
import os
import uuid

from nose.tools import (
    eq_,
    assert_raises,
    assert_raises_regexp,
    set_trace,
)

from StringIO import StringIO

from api.authenticator import BasicAuthenticationProvider

from api.circulation import (
    LoanInfo,
    HoldInfo,
    FulfillmentInfo,
)

from api.config import (
    Configuration,
    temp_config,
)

from api.circulation_exceptions import *

from api.rbdigital import (
    AudiobookManifest,
    RBDigitalAPI,
    RBDigitalBibliographicCoverageProvider,
    RBDigitalCirculationMonitor,
    RBDigitalDeltaMonitor,
    RBDigitalImportMonitor,
    RBDigitalRepresentationExtractor,
    MockRBDigitalAPI,
    RBFulfillmentInfo,
)

from core.classifier import Classifier
from core.coverage import CoverageFailure

from core.metadata_layer import (
    CirculationData,
    ContributorData,
    IdentifierData,
    Metadata,
    SubjectData,
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
    Hyperlink,
    Identifier,
    LicensePool,
    Patron,
    Representation,
    Subject,
    Work,
)

from core.scripts import RunCollectionCoverageProviderScript
from core.testing import MockRequestsResponse

from core.util.http import (
    BadResponseException,
    RemoteIntegrationException,
    HTTP,
)

from . import (
    DatabaseTest,
)

class RBDigitalAPITest(DatabaseTest):

    def setup(self):
        super(RBDigitalAPITest, self).setup()

        self.base_path = os.path.split(__file__)[0]
        # Make sure the default library is created so that it will
        # be configured properly with the mock collection.
        self._default_library
        self.collection = MockRBDigitalAPI.mock_collection(self._db)
        self.api = MockRBDigitalAPI(
            self._db, self.collection, base_path=self.base_path
        )
        self.default_patron = self._patron(external_identifier="rbdigital_testuser")
        self.default_patron.authorization_identifier="13057226"


class TestRBDigitalAPI(RBDigitalAPITest):

    def test__run_self_tests(self):
        class Mock(MockRBDigitalAPI):
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
        class Mock(MockRBDigitalAPI):
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
        """All the RBDigitalAPI methods that take a Patron object call
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

    def test_create_identifier_strings(self):
        identifier = self._identifier()
        values = RBDigitalAPI.create_identifier_strings(["foo", identifier])
        eq_(["foo", identifier.identifier], values)

    def test_availability_exception(self):
        self.api.queue_response(500)
        assert_raises_regexp(
            BadResponseException, "Bad response from availability_search",
            self.api.get_all_available_through_search
        )

    def test_search(self):
        datastr, datadict = self.api.get_data("response_search_one_item_1.json")
        self.api.queue_response(status_code=200, content=datastr)

        response = self.api.search(mediatype='ebook', author="Alexander Mccall Smith", title="Tea Time for the Traditionally Built")
        response_dictionary = response.json()
        eq_(1, response_dictionary['pageCount'])
        eq_(u'Tea Time for the Traditionally Built', response_dictionary['items'][0]['item']['title'])

    def test_get_all_available_through_search(self):
        datastr, datadict = self.api.get_data("response_search_five_items_1.json")
        self.api.queue_response(status_code=200, content=datastr)

        response_dictionary = self.api.get_all_available_through_search()
        eq_(1, response_dictionary['pageCount'])
        eq_(5, response_dictionary['resultSetCount'])
        eq_(5, len(response_dictionary['items']))
        returned_titles = [iteminterest['item']['title'] for iteminterest in response_dictionary['items']]
        assert (u'Unusual Uses for Olive Oil' in returned_titles)

    def test_get_all_catalog(self):
        datastr, datadict = self.api.get_data("response_catalog_all_sample.json")
        self.api.queue_response(status_code=200, content=datastr)

        catalog = self.api.get_all_catalog()
        eq_(8, len(catalog))
        eq_("Challenger Deep", catalog[7]['title'])

    def test_get_delta(self):
        datastr, datadict = self.api.get_data("response_catalog_delta.json")
        self.api.queue_response(status_code=200, content=datastr)

        assert_raises_regexp(
            ValueError, 'from_date 2000-01-01 00:00:00 must be real, in the past, and less than 6 months ago.',
            self.api.get_delta, from_date="2000-01-01", to_date="2000-02-01"
        )

        today = datetime.datetime.now()
        three_months = relativedelta(months=3)
        assert_raises_regexp(
            ValueError, "from_date .* - to_date .* asks for too-wide date range.",
            self.api.get_delta, from_date=(today - three_months), to_date=today
        )

        delta = self.api.get_delta()
        eq_(1931, delta[0]["libraryId"])
        eq_("Wethersfield Public Library", delta[0]["libraryName"])
        eq_("2016-10-17", delta[0]["beginDate"])
        eq_("2016-10-18", delta[0]["endDate"])
        eq_(0, delta[0]["eBookAddedCount"])
        eq_(0, delta[0]["eBookRemovedCount"])
        eq_(1, delta[0]["eAudioAddedCount"])
        eq_(1, delta[0]["eAudioRemovedCount"])
        eq_(1, delta[0]["titleAddedCount"])
        eq_(1, delta[0]["titleRemovedCount"])
        eq_(1, len(delta[0]["addedTitles"]))
        eq_(1, len(delta[0]["removedTitles"]))

    def test_patron_remote_identifier_new_patron(self):

        class NeverHeardOfYouAPI(RBDigitalAPI):
            """A mock RBDigitalAPI that has never heard of any patron
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

        class IKnowYouAPI(RBDigitalAPI):
            """A mock RBDigitalAPI that has heard of any given
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
        rbdigital_patron_id = self.api.patron_remote_identifier_lookup(patron)
        eq_(None, rbdigital_patron_id)

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
        rbdigital_patron_id = self.api.patron_remote_identifier_lookup(patron)
        eq_(939981, rbdigital_patron_id)

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

    def test_get_ebook_availability_info(self):
        datastr, datadict = self.api.get_data("response_availability_ebook_1.json")
        self.api.queue_response(status_code=200, content=datastr)

        response_list = self.api.get_ebook_availability_info()
        eq_(u'9781420128567', response_list[0]['isbn'])
        eq_(False, response_list[0]['availability'])

    def test_get_metadata_by_isbn(self):
        datastr, datadict = self.api.get_data("response_isbn_notfound_1.json")
        self.api.queue_response(status_code=200, content=datastr)

        response_dictionary = self.api.get_metadata_by_isbn('97BADISBNFAKE')
        eq_(None, response_dictionary)


        self.api.queue_response(status_code=404, content="{}")
        assert_raises_regexp(
            BadResponseException,
            "Bad response from .*",
            self.api.get_metadata_by_isbn, identifier='97BADISBNFAKE'
        )

        datastr, datadict = self.api.get_data("response_isbn_found_1.json")
        self.api.queue_response(status_code=200, content=datastr)
        response_dictionary = self.api.get_metadata_by_isbn('9780307378101')
        eq_(u'9780307378101', response_dictionary['isbn'])
        eq_(u'Anchor', response_dictionary['publisher'])

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
        rbdigital_id = self._str

        # borrow functionality checks
        response_dictionary = self.api.circulate_item(rbdigital_id, edition.primary_identifier.identifier)
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
            self.api.circulate_item, rbdigital_id, edition.primary_identifier.identifier
        )
        request_url, request_args, request_kwargs = self.api.requests[-1]
        assert "checkouts" in request_url
        eq_("post", request_kwargs.get("method"))

        # book return functionality checks
        self.api.queue_response(status_code=200, content="")

        response_dictionary = self.api.circulate_item(rbdigital_id, edition.primary_identifier.identifier,
            return_item=True)
        eq_({}, response_dictionary)
        request_url, request_args, request_kwargs = self.api.requests[-1]
        assert "checkouts" in request_url
        eq_("delete", request_kwargs.get("method"))

        datastr, datadict = self.api.get_data("response_return_unavailable.json")
        self.api.queue_response(status_code=409, content=datastr)
        assert_raises_regexp(
            NotCheckedOut, "checkin:",
            self.api.circulate_item, rbdigital_id, edition.primary_identifier.identifier,
            return_item=True
        )
        request_url, request_args, request_kwargs = self.api.requests[-1]
        assert "checkouts" in request_url
        eq_("delete", request_kwargs.get("method"))

        # hold functionality checks
        datastr, datadict = self.api.get_data("response_patron_hold_success.json")
        self.api.queue_response(status_code=200, content=datastr)

        response = self.api.circulate_item(rbdigital_id, edition.primary_identifier.identifier,
                                           hold=True)
        eq_(9828560, response)
        request_url, request_args, request_kwargs = self.api.requests[-1]
        assert "holds" in request_url
        eq_("post", request_kwargs.get("method"))

        datastr, datadict = self.api.get_data("response_patron_hold_fail_409_reached_limit.json")
        self.api.queue_response(status_code=409, content=datastr)

        response = self.api.circulate_item(rbdigital_id, edition.primary_identifier.identifier,
                                           hold=True)
        eq_("You have reached your checkout limit and therefore are unable to place additional holds.",
            response)
        request_url, request_args, request_kwargs = self.api.requests[-1]
        assert "holds" in request_url
        eq_("post", request_kwargs.get("method"))

    def test_checkin(self):
        # Returning a book is, for now, more of a "notify RBDigital that we've
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
        patron_rbdigital_id = self.api.create_patron(patron)

        # The patron's remote account ID is returned.
        eq_(940000, patron_rbdigital_id)

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
        "Test releasing a book reservation early."

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
        """Test the RBDigital implementation of the update_availability method
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

class TestCirculationMonitor(RBDigitalAPITest):

    def test_process_availability(self):
        monitor = RBDigitalCirculationMonitor(
            self._db, self.collection, api_class=MockRBDigitalAPI,
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

class TestAudiobookManifest(RBDigitalAPITest):

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

        # We know about 21 items in the reading order.
        eq_(21, len(manifest.readingOrder))

        # Let's spot check one.
        first = manifest.readingOrder[0]
        eq_("358456", first['rbdigital:id'])
        eq_("https://download-piece/1", first['href'])
        eq_(manifest.INTERMEDIATE_LINK_MEDIA_TYPE, first['type'])
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

class TestRBDigitalRepresentationExtractor(RBDigitalAPITest):

    def test_book_info_with_metadata(self):
        # Tests that can convert a RBDigital json block into a Metadata object.

        datastr, datadict = self.api.get_data("response_isbn_found_1.json")
        metadata = RBDigitalRepresentationExtractor.isbn_info_to_metadata(datadict)

        eq_("Tea Time for the Traditionally Built", metadata.title)
        eq_(None, metadata.sort_title)
        eq_(None, metadata.subtitle)
        eq_(Edition.BOOK_MEDIUM, metadata.medium)
        eq_("No. 1 Ladies Detective Agency", metadata.series)
        eq_(10, metadata.series_position)
        eq_("eng", metadata.language)
        eq_("Anchor", metadata.publisher)
        eq_(None, metadata.imprint)
        eq_(2013, metadata.published.year)
        eq_(12, metadata.published.month)
        eq_(27, metadata.published.day)

        [author1, author2, narrator] = metadata.contributors
        eq_(u"Mccall Smith, Alexander", author1.sort_name)
        eq_(u"Alexander Mccall Smith", author1.display_name)
        eq_([Contributor.AUTHOR_ROLE], author1.roles)
        eq_(u"Wilder, Thornton", author2.sort_name)
        eq_(u"Thornton Wilder", author2.display_name)
        eq_([Contributor.AUTHOR_ROLE], author2.roles)

        eq_(u"Guskin, Laura Flanagan", narrator.sort_name)
        eq_(u"Laura Flanagan Guskin", narrator.display_name)
        eq_([Contributor.NARRATOR_ROLE], narrator.roles)

        subjects = sorted(metadata.subjects, key=lambda x: x.identifier)

        eq_([(None, u"FICTION / Humorous / General", Subject.BISAC, 100),

            (u'adult', None, Classifier.RBDIGITAL_AUDIENCE, 500),

            (u'humorous-fiction', None, Subject.RBDIGITAL, 200),
            (u'mystery', None, Subject.RBDIGITAL, 200),
            (u'womens-fiction', None, Subject.RBDIGITAL, 200)
         ],
            [(x.identifier, x.name, x.type, x.weight) for x in subjects]
        )

        # Related IDs.
        eq_((Identifier.RB_DIGITAL_ID, '9780307378101'),
            (metadata.primary_identifier.type, metadata.primary_identifier.identifier))

        ids = [(x.type, x.identifier) for x in metadata.identifiers]

        # We made exactly one RBDigital and one ISBN-type identifiers.
        eq_(
            [(Identifier.ISBN, "9780307378101"), (Identifier.RB_DIGITAL_ID, "9780307378101")],
            sorted(ids)
        )

        # Available formats.
        [epub] = sorted(metadata.circulation.formats, key=lambda x: x.content_type)
        eq_(Representation.EPUB_MEDIA_TYPE, epub.content_type)
        eq_(DeliveryMechanism.ADOBE_DRM, epub.drm_scheme)

        # Links to various resources.
        shortd, image = sorted(
            metadata.links, key=lambda x:x.rel
        )

        eq_(Hyperlink.SHORT_DESCRIPTION, shortd.rel)
        assert shortd.content.startswith("THE NO. 1 LADIES' DETECTIVE AGENCY")

        eq_(Hyperlink.IMAGE, image.rel)
        eq_('http://images.oneclickdigital.com/EB00148140/EB00148140_image_128x192.jpg', image.href)

        thumbnail = image.thumbnail

        eq_(Hyperlink.THUMBNAIL_IMAGE, thumbnail.rel)
        eq_('http://images.oneclickdigital.com/EB00148140/EB00148140_image_95x140.jpg', thumbnail.href)

        # Note: For now, no measurements associated with the book.

        # Request only the bibliographic information.
        metadata = RBDigitalRepresentationExtractor.isbn_info_to_metadata(datadict, include_bibliographic=True, include_formats=False)
        eq_("Tea Time for the Traditionally Built", metadata.title)
        eq_(None, metadata.circulation)

        # Request only the format information.
        metadata = RBDigitalRepresentationExtractor.isbn_info_to_metadata(datadict, include_bibliographic=False, include_formats=True)
        eq_(None, metadata.title)
        [epub] = sorted(metadata.circulation.formats, key=lambda x: x.content_type)
        eq_(Representation.EPUB_MEDIA_TYPE, epub.content_type)
        eq_(DeliveryMechanism.ADOBE_DRM, epub.drm_scheme)


    def test_book_info_metadata_no_series(self):
        """'Default Blank' is not a series -- it's a string representing
        the absence of a series.
        """

        datastr, datadict = self.api.get_data("response_isbn_found_no_series.json")
        metadata = RBDigitalRepresentationExtractor.isbn_info_to_metadata(datadict)

        eq_("Tea Time for the Traditionally Built", metadata.title)
        eq_(None, metadata.series)
        eq_(None, metadata.series_position)

class TestRBDigitalBibliographicCoverageProvider(RBDigitalAPITest):
    """Test the code that looks up bibliographic information from RBDigital."""

    def setup(self):
        super(TestRBDigitalBibliographicCoverageProvider, self).setup()

        self.provider = RBDigitalBibliographicCoverageProvider(
            self.collection, api_class=MockRBDigitalAPI,
            api_class_kwargs=dict(base_path=os.path.split(__file__)[0])
        )
        self.api = self.provider.api

    def test_script_instantiation(self):
        """Test that RunCoverageProviderScript can instantiate
        the coverage provider.
        """
        script = RunCollectionCoverageProviderScript(
            RBDigitalBibliographicCoverageProvider, self._db,
            api_class=MockRBDigitalAPI
        )
        [provider] = script.providers
        assert isinstance(provider,
                          RBDigitalBibliographicCoverageProvider)
        assert isinstance(provider.api, MockRBDigitalAPI)
        eq_(self.collection, provider.collection)

    def test_invalid_or_unrecognized_guid(self):
        # A bad or malformed ISBN can't get coverage.

        identifier = self._identifier()
        identifier.identifier = 'ISBNbadbad'

        datastr, datadict = self.api.get_data("response_isbn_notfound_1.json")
        self.api.queue_response(status_code=200, content=datastr)

        failure = self.provider.process_item(identifier)
        assert isinstance(failure, CoverageFailure)
        eq_(True, failure.transient)
        assert failure.exception.startswith('Cannot find RBDigital metadata')

    def test_process_item_creates_presentation_ready_work(self):
        # Test the normal workflow where we ask RBDigital for data,
        # RBDigital provides it, and we create a presentation-ready work.

        datastr, datadict = self.api.get_data("response_isbn_found_1.json")
        self.api.queue_response(200, content=datastr)

        # Here's the book mentioned in response_isbn_found_1.
        identifier = self._identifier(identifier_type=Identifier.RB_DIGITAL_ID)
        identifier.identifier = '9780307378101'

        # This book has no LicensePool.
        eq_([], identifier.licensed_through)

        # Run it through the RBDigitalBibliographicCoverageProvider
        result = self.provider.process_item(identifier)
        eq_(identifier, result)

        # A LicensePool was created. But we do NOT know how many copies of this
        # book are available, only what formats it's available in.
        [pool] = identifier.licensed_through
        eq_(0, pool.licenses_owned)
        [lpdm] = pool.delivery_mechanisms
        eq_('application/epub+zip (application/vnd.adobe.adept+xml)', lpdm.delivery_mechanism.name)

        # A Work was created and made presentation ready.
        eq_('Tea Time for the Traditionally Built', pool.work.title)
        eq_(True, pool.work.presentation_ready)

class TestRBDigitalSyncMonitor(DatabaseTest):

    # TODO: The only thing this should test is that the monitors can
    # be instantiated using the constructor arguments used by
    # RunCollectionMonitorScript, and that calling run_once() results
    # in a call to the appropriate RBDigitalAPI method.
    #
    # However, there's no other code that tests populate_all_catalog()
    # or populate_delta(), so we can't just remove the code; we need to
    # refactor the tests.

    def setup(self):
        super(TestRBDigitalSyncMonitor, self).setup()
        self.base_path = os.path.split(__file__)[0]
        self.resource_path = os.path.join(self.base_path, "files", "rbdigital")
        self.collection = MockRBDigitalAPI.mock_collection(self._db)

    def get_data(self, filename):
        # returns contents of sample file as string and as dict
        path = os.path.join(self.resource_path, filename)
        data = open(path).read()
        return data, json.loads(data)

    def test_import(self):

        # Create a RBDigitalImportMonitor, which will take the current
        # state of a RBDigital collection and mirror the whole thing to
        # a local database.
        monitor = RBDigitalImportMonitor(
            self._db, self.collection, api_class=MockRBDigitalAPI,
            api_class_kwargs=dict(base_path=self.base_path)
        )
        datastr, datadict = self.get_data("response_catalog_all_sample.json")
        monitor.api.queue_response(status_code=200, content=datastr)
        monitor.run()

        # verify that we created Works, Editions, LicensePools
        works = self._db.query(Work).all()
        work_titles = [work.title for work in works]
        expected_titles = ["Tricks", "Emperor Mage: The Immortals",
            "In-Flight Russian", "Road, The", "Private Patient, The",
            "Year of Magical Thinking, The", "Junkyard Bot: Robots Rule, Book 1, The",
            "Challenger Deep"]
        eq_(set(expected_titles), set(work_titles))

        # make sure we created some Editions
        edition = Edition.for_foreign_id(self._db, DataSource.RB_DIGITAL, Identifier.RB_DIGITAL_ID, "9780062231727", create_if_not_exists=False)
        assert(edition is not None)
        edition = Edition.for_foreign_id(self._db, DataSource.RB_DIGITAL, Identifier.RB_DIGITAL_ID, "9781615730186", create_if_not_exists=False)
        assert(edition is not None)

        # make sure we created some LicensePools
        pool, made_new = LicensePool.for_foreign_id(
            self._db, DataSource.RB_DIGITAL, Identifier.RB_DIGITAL_ID,
            "9780062231727", collection=self.collection
        )
        eq_(1, pool.licenses_owned)
        eq_(1, pool.licenses_available)

        eq_(False, made_new)
        pool, made_new = LicensePool.for_foreign_id(
            self._db, DataSource.RB_DIGITAL, Identifier.RB_DIGITAL_ID,
            "9781615730186", collection=self.collection
        )
        eq_(False, made_new)
        eq_(1, pool.licenses_owned)
        eq_(1, pool.licenses_available)

        # make sure there are 8 LicensePools
        pools = self._db.query(LicensePool).all()
        eq_(8, len(pools))

        #
        # Now we're going to run the delta monitor to change things
        # around a bit.
        #

        # set license numbers on test pool to match what's in the
        # delta document.
        pool, made_new = LicensePool.for_foreign_id(
            self._db, DataSource.RB_DIGITAL, Identifier.RB_DIGITAL_ID,
            "9781615730186", collection=self.collection
        )
        eq_(False, made_new)
        pool.licenses_owned = 10
        pool.licenses_available = 9
        pool.licenses_reserved = 2
        pool.patrons_in_hold_queue = 1

        # now update that library with a sample delta
        delta_monitor = RBDigitalDeltaMonitor(
            self._db, self.collection, api_class=MockRBDigitalAPI,
            api_class_kwargs=dict(base_path=self.base_path)
        )
        datastr, datadict = self.get_data("response_catalog_delta.json")
        delta_monitor.api.queue_response(status_code=200, content=datastr)
        delta_monitor.run()

        # "Tricks" did not get deleted, but did get its pools set to "nope".
        # "Emperor Mage: The Immortals" got new metadata.
        works = self._db.query(Work).all()
        work_titles = [work.title for work in works]
        expected_titles = ["Tricks", "Emperor Mage: The Immortals",
            "In-Flight Russian", "Road, The", "Private Patient, The",
            "Year of Magical Thinking, The", "Junkyard Bot: Robots Rule, Book 1, The",
            "Challenger Deep"]
        eq_(set(expected_titles), set(work_titles))

        eq_("Tricks", pool.presentation_edition.title)
        eq_(0, pool.licenses_owned)
        eq_(0, pool.licenses_available)
        eq_(0, pool.licenses_reserved)
        eq_(0, pool.patrons_in_hold_queue)
        assert (datetime.datetime.utcnow() - pool.last_checked) < datetime.timedelta(seconds=20)

        # make sure we updated fields
        edition = Edition.for_foreign_id(self._db, DataSource.RB_DIGITAL, Identifier.RB_DIGITAL_ID, "9781934180723", create_if_not_exists=False)
        eq_("Recorded Books, Inc.", edition.publisher)

        # make sure there are still 8 LicensePools
        pools = self._db.query(LicensePool).all()
        eq_(8, len(pools))

        # Running the monitor again does nothing. Since no more responses
        # are queued, doing any work at this point would crash the test.
        eq_((0,0), monitor.invoke())
