# encoding: utf-8
from nose.tools import (
    set_trace, eq_, ok_,
    assert_raises,
)
import pkgutil
import json
from datetime import (
    datetime,
    timedelta,
)
from api.overdrive import (
    MockOverdriveAPI,
    NewTitlesOverdriveCollectionMonitor,
    OverdriveAPI,
    OverdriveCirculationMonitor,
    OverdriveCollectionReaper,
    OverdriveFormatSweep,
    RecentOverdriveCollectionMonitor
)

from api.authenticator import BasicAuthenticationProvider
from api.circulation import (
    CirculationAPI,
    HoldInfo,
)
from api.circulation_exceptions import *
from api.config import Configuration

from . import (
    DatabaseTest,
    sample_data
)

from core.metadata_layer import TimestampData
from core.model import (
    Collection,
    CirculationEvent,
    ConfigurationSetting,
    DataSource,
    DeliveryMechanism,
    Edition,
    ExternalIntegration,
    Identifier,
    LicensePool,
    Representation,
    RightsStatus,
)
from core.testing import (
    DummyHTTPClient,
    MockRequestsResponse,
)

from api.config import temp_config

class OverdriveAPITest(DatabaseTest):

    def setup(self):
        super(OverdriveAPITest, self).setup()
        library = self._default_library
        self.collection = MockOverdriveAPI.mock_collection(self._db)
        self.circulation = CirculationAPI(
            self._db, library, api_map={ExternalIntegration.OVERDRIVE:MockOverdriveAPI}
        )
        self.api = self.circulation.api_for_collection[self.collection.id]

    @classmethod
    def sample_data(self, filename):
        return sample_data(filename, 'overdrive')

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

class TestOverdriveAPI(OverdriveAPITest):

    def test_external_integration(self):
        eq_(self.collection.external_integration,
            self.api.external_integration(self._db))

    def test__run_self_tests(self):
        """Verify that OverdriveAPI._run_self_tests() calls the right
        methods.
        """
        class Mock(MockOverdriveAPI):
            "Mock every method used by OverdriveAPI._run_self_tests."

            # First we will call check_creds() to get a fresh credential.
            mock_credential = object()
            def check_creds(self, force_refresh=False):
                self.check_creds_called_with = force_refresh
                return self.mock_credential

            # Then we will call get_advantage_accounts().
            mock_advantage_accounts = [object(), object()]
            def get_advantage_accounts(self):
                return self.mock_advantage_accounts

            # Then we will call get() on the _all_products_link.
            def get(self, url, extra_headers, exception_on_401=False):
                self.get_called_with = (url, extra_headers, exception_on_401)
                return 200, {}, json.dumps(dict(totalItems=2010))

            # Finally, for every library associated with this
            # collection, we'll call get_patron_credential() using
            # the credentials of that library's test patron.
            mock_patron_credential = object()
            get_patron_credential_called_with = []
            def get_patron_credential(self, patron, pin):
                self.get_patron_credential_called_with.append((patron, pin))
                return self.mock_patron_credential

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
        [no_patron_credential, default_patron_credential,
         global_privileges, collection_size, advantage] = results

        # Verify that each test method was called and returned the
        # expected SelfTestResult object.
        eq_('Checking global Client Authentication privileges',
            global_privileges.name)
        eq_(True, global_privileges.success)
        eq_(api.mock_credential, global_privileges.result)

        eq_('Looking up Overdrive Advantage accounts', advantage.name)
        eq_(True, advantage.success)
        eq_('Found 2 Overdrive Advantage account(s).', advantage.result)

        eq_('Counting size of collection', collection_size.name)
        eq_(True, collection_size.success)
        eq_('2010 item(s) in collection', collection_size.result)
        url, headers, error_on_401 = api.get_called_with
        eq_(api._all_products_link, url)

        eq_(
            "Acquiring test patron credentials for library %s" % no_default_patron.name,
            no_patron_credential.name
        )
        eq_(False, no_patron_credential.success)
        eq_("Library has no test patron configured.",
            no_patron_credential.exception.message)

        eq_(
            "Checking Patron Authentication privileges, using test patron for library %s" % with_default_patron.name,
            default_patron_credential.name
        )
        eq_(True, default_patron_credential.success)
        eq_(api.mock_patron_credential, default_patron_credential.result)

        # Although there are two libraries associated with this
        # collection, get_patron_credential was only called once, because
        # one of the libraries doesn't have a default patron.
        [(patron1, password1)] = api.get_patron_credential_called_with
        eq_("username1", patron1.authorization_identifier)
        eq_("password1", password1)

    def test_run_self_tests_short_circuit(self):
        """If OverdriveAPI.check_creds can't get credentials, the rest of
        the self-tests aren't even run.

        This probably doesn't matter much, because if check_creds doesn't
        work we won't be able to instantiate the OverdriveAPI class.
        """
        def explode(*args, **kwargs):
            raise Exception("Failure!")
        self.api.check_creds = explode

        # Only one test will be run.
        [check_creds] = self.api._run_self_tests(self._db)
        eq_("Failure!", check_creds.exception.message)

    def test_default_notification_email_address(self):
        """Test the ability of the Overdrive API to detect an email address
        previously given by the patron to Overdrive for the purpose of
        notifications.
        """
        ignore, patron_with_email = self.sample_json(
            "patron_info.json"
        )
        self.api.queue_response(200, content=patron_with_email)
        patron = self._patron()

        # The site default for notification emails will never be used.
        configuration_setting = ConfigurationSetting.for_library(
            Configuration.DEFAULT_NOTIFICATION_EMAIL_ADDRESS,
            self._default_library
        )
        configuration_setting.value = "notifications@example.com"

        # If the patron has used a particular email address to put
        # books on hold, use that email address, not the site default.
        eq_("foo@bar.com",
            self.api.default_notification_email_address(patron, 'pin'))

        # If the patron's email address according to Overdrive _is_
        # the site default, it is ignored. This can only happen if
        # this patron placed a hold using an older version of the
        # circulation manager.
        patron_with_email['lastHoldEmail'] = configuration_setting.value
        self.api.queue_response(200, content=patron_with_email)
        eq_(None,
            self.api.default_notification_email_address(patron, 'pin'))

        # If the patron has never before put an Overdrive book on
        # hold, their JSON object has no `lastHoldEmail` key. In this
        # case we return None -- again, ignoring the site default.
        patron_with_no_email = dict(patron_with_email)
        del patron_with_no_email['lastHoldEmail']
        self.api.queue_response(200, content=patron_with_no_email)
        eq_(None,
            self.api.default_notification_email_address(patron, 'pin'))

        # If there's an error getting the information from Overdrive,
        # we return None.
        self.api.queue_response(404)
        eq_(None,
            self.api.default_notification_email_address(patron, 'pin'))

    def test_place_hold(self):
        # Verify that an appropriate request is made to HOLDS_ENDPOINT
        # to create a hold.
        #
        # The request will include different form fields depending on
        # whether default_notification_email_address returns something.
        class Mock(MockOverdriveAPI):
            def __init__(self, *args, **kwargs):
                super(Mock, self).__init__(*args, **kwargs)
                self.DEFAULT_NOTIFICATION_EMAIL_ADDRESS = None

            def default_notification_email_address(self, patron, pin):
                self.default_notification_email_address_called_with = (
                    patron, pin
                )
                return self.DEFAULT_NOTIFICATION_EMAIL_ADDRESS

            def fill_out_form(self, **form_fields):
                # Record the form fields and return some dummy values.
                self.fill_out_form_called_with = form_fields
                return "headers", "filled-out form"

            def patron_request(self, *args, **kwargs):
                # Pretend to make a request to an API endpoint.
                self.patron_request_called_with = (args, kwargs)
                return "A mock response"

            def process_place_hold_response(
                self, response, patron, pin, licensepool
            ):
                self.process_place_hold_response_called_with = (
                    response, patron, pin, licensepool
                )
                return "OK, I processed it."

        # First, test the case where no notification email address is
        # provided and there is no default.
        patron = object()
        pin = object()
        pool = self._licensepool(edition=None, collection=self.collection)
        api = Mock(self._db, self.collection)
        response = api.place_hold(patron, pin, pool, None)

        # Now we can trace the path of the input through the method calls.

        # The patron and PIN were passed into
        # default_notification_email_address.
        eq_((patron, pin), api.default_notification_email_address_called_with)

        # The return value was None, and so 'ignoreHoldEmail' was
        # added to the form to be filled out, rather than
        # 'emailAddress' being added.
        fields = api.fill_out_form_called_with
        identifier = str(pool.identifier.identifier)
        eq_(dict(ignoreHoldEmail=True, reserveId=identifier), fields)

        # patron_request was called with the filled-out form and other
        # information necessary to authenticate the request.
        args, kwargs = api.patron_request_called_with
        eq_((patron, pin, api.HOLDS_ENDPOINT, 'headers', 'filled-out form'),
            args)
        eq_({}, kwargs)

        # Finally, process_place_hold_response was called on
        # the return value of patron_request
        eq_(
            ("A mock response", patron, pin, pool),
             api.process_place_hold_response_called_with
        )
        eq_("OK, I processed it.", response)

        # Now we need to test two more cases.
        #
        # First, the patron has a holds notification address
        # registered with Overdrive.
        email = "holds@patr.on"
        api.DEFAULT_NOTIFICATION_EMAIL_ADDRESS = email
        response = api.place_hold(patron, pin, pool, None)

        # Same result.
        eq_("OK, I processed it.", response)

        # Different variables were passed in to fill_out_form.
        fields = api.fill_out_form_called_with
        eq_(dict(emailAddress=email, reserveId=identifier), fields)

        # Finally, test that when a specific address is passed in, it
        # takes precedence over the patron's holds notification address.

        response = api.place_hold(patron, pin, pool, "another@addre.ss")
        eq_("OK, I processed it.", response)
        fields = api.fill_out_form_called_with
        eq_(dict(emailAddress="another@addre.ss", reserveId=identifier), fields)

    def test_process_place_hold_response(self):
        # Verify that we can handle various error and non-error responses
        # to a HOLDS_ENDPOINT request.

        ignore, successful_hold = self.sample_json("successful_hold.json")
        class Mock(MockOverdriveAPI):
            def get_hold(self, patron, pin, overdrive_id):
                # Return a sample hold representation rather than
                # making another API request.
                self.get_hold_called_with = (patron, pin, overdrive_id)
                return successful_hold

        api = Mock(self._db, self.collection)

        def process_error_response(message):
            # Attempt to process a response that resulted in an error.
            if isinstance(message, basestring):
                data = dict(errorCode=message)
            else:
                data = message
            response = MockRequestsResponse(400, content=data)
            return api.process_place_hold_response(response, None, None, None)

        # Some error messages result in specific CirculationExceptions.
        assert_raises(
            CannotRenew, process_error_response, "NotWithinRenewalWindow"
        )
        assert_raises(
            PatronHoldLimitReached, process_error_response,
            "PatronExceededHoldLimit"
        )

        # An unrecognized error message results in a generic
        # CannotHold.
        assert_raises(CannotHold, process_error_response, "SomeOtherError")

        # Same if the error message is missing or the response can't be
        # processed.
        assert_raises(CannotHold, process_error_response, dict())
        assert_raises(CannotHold, process_error_response, None)

        # Same if the error code isn't in the 4xx or 2xx range
        # (which shouldn't happen in real life).
        response = MockRequestsResponse(999)
        assert_raises(
            CannotHold, api.process_place_hold_response,
            response, None, None, None
        )

        # At this point patron and book details become important --
        # we're going to return a HoldInfo object and potentially make
        # another API request.
        patron = self._patron()
        pin = object()
        licensepool = self._licensepool(edition=None)

        # The remaining tests will end up running the same code on the
        # same data, so they will return the same HoldInfo. Define a
        # helper method to make this easier.
        def assert_correct_holdinfo(x):
            assert isinstance(x, HoldInfo)
            eq_(licensepool.collection, x.collection(self._db))
            eq_(licensepool.data_source.name, x.data_source_name)
            eq_(identifier.identifier, x.identifier)
            eq_(identifier.type, x.identifier_type)
            eq_(datetime(2015, 3, 26, 11, 30, 29), x.start_date)
            eq_(None, x.end_date)
            eq_(1, x.hold_position)

        # Test the case where the 'error' is that the book is already
        # on hold.
        already_on_hold = dict(errorCode="AlreadyOnWaitList")
        response = MockRequestsResponse(400, content=already_on_hold)
        result = api.process_place_hold_response(
            response, patron, pin, licensepool
        )

        # get_hold() was called with the arguments we expect.
        identifier = licensepool.identifier
        eq_((patron, pin, identifier.identifier),
            api.get_hold_called_with)

        # The result was converted into a HoldInfo object. The
        # effective result is exactly as if we had successfully put
        # the book on hold.
        assert_correct_holdinfo(result)

        # Finally, let's test the case where there was no hold and now
        # there is.
        api.get_hold_called_with = None
        response = MockRequestsResponse(200, content=successful_hold)
        result = api.process_place_hold_response(
            response, patron, pin, licensepool
        )
        assert_correct_holdinfo(result)

        # Here, get_hold was _not_ called, because the hold didn't
        # already exist.
        eq_(None, api.get_hold_called_with)

    def test_checkin(self):

        class Mock(MockOverdriveAPI):
            EARLY_RETURN_SUCCESS = False

            def perform_early_return(self, *args):
                self.perform_early_return_call = args
                return self.EARLY_RETURN_SUCCESS

            def patron_request(self, *args, **kwargs):
                self.patron_request_call = (args, kwargs)

        overdrive = Mock(self._db, self.collection)
        overdrive.perform_early_return_call = None

        # In most circumstances we do not bother calling
        # perform_early_return; we just call patron_request.
        pool = self._licensepool(None)
        patron = self._patron()
        pin = object()
        expect_url = overdrive.endpoint(
            overdrive.CHECKOUT_ENDPOINT,
            overdrive_id=pool.identifier.identifier
        )

        def assert_no_early_return():
            """Call this to verify that patron_request is
            called within checkin() instead of perform_early_return.
            """
            overdrive.checkin(patron, pin, pool)

            # perform_early_return was not called.
            eq_(None, overdrive.perform_early_return_call)

            # patron_request was called in an attempt to
            # DELETE an active loan.
            args, kwargs = overdrive.patron_request_call
            eq_((patron, pin, expect_url), args)
            eq_(dict(method="DELETE"), kwargs)
            overdrive.patron_request_call = None

        # If there is no loan, there is no perform_early_return.
        assert_no_early_return()

        # Same if the loan is not fulfilled...
        loan, ignore = pool.loan_to(patron)
        assert_no_early_return()

        # If the loan is fulfilled but its LicensePoolDeliveryMechanism has
        # no DeliveryMechanism for some reason...
        loan.fulfillment = pool.delivery_mechanisms[0]
        dm = loan.fulfillment.delivery_mechanism
        loan.fulfillment.delivery_mechanism = None
        assert_no_early_return()

        # If the loan is fulfilled but the delivery mechanism uses DRM...
        loan.fulfillment.delivery_mechanism = dm
        assert_no_early_return()

        # If the loan is fulfilled with a DRM-free delivery mechanism,
        # perform_early_return _is_ called.
        dm.drm_scheme = DeliveryMechanism.NO_DRM
        overdrive.checkin(patron, pin, pool)

        eq_((patron, pin, loan), overdrive.perform_early_return_call)

        # But if it fails, patron_request is _also_ called.
        args, kwargs = overdrive.patron_request_call
        eq_((patron, pin, expect_url), args)
        eq_(dict(method="DELETE"), kwargs)

        # Finally, if the loan is fulfilled with a DRM-free delivery mechanism
        # and perform_early_return succeeds, patron_request_call is not
        # called -- the title was already returned.
        overdrive.patron_request_call = None
        overdrive.EARLY_RETURN_SUCCESS = True
        overdrive.checkin(patron, pin, pool)
        eq_((patron, pin, loan), overdrive.perform_early_return_call)
        eq_(None, overdrive.patron_request_call)

    def test_perform_early_return(self):

        class Mock(MockOverdriveAPI):

            EARLY_RETURN_URL = "http://early-return/"

            def get_fulfillment_link(self, *args):
                self.get_fulfillment_link_call = args
                return ("http://fulfillment/", "content/type")

            def _extract_early_return_url(self, *args):
                self._extract_early_return_url_call = args
                return self.EARLY_RETURN_URL

        overdrive = Mock(self._db, self.collection)

        # This patron has a loan.
        pool = self._licensepool(None)
        patron = self._patron()
        pin = object()
        loan, ignore = pool.loan_to(patron)

        # The loan has been fulfilled and now the patron wants to
        # do early return.
        loan.fulfillment = pool.delivery_mechanisms[0]

        # Our mocked perform_early_return will make two HTTP requests.
        # The first will be to the fulfill link returned by our mock
        # get_fulfillment_link. The response to this request is a
        # redirect that includes an early return link.
        http = DummyHTTPClient()
        http.responses.append(
            MockRequestsResponse(
                302, dict(location="http://fulfill-this-book/?or=return-early")
            )
        )

        # The second HTTP request made will be to the early return
        # link 'extracted' from that link by our mock
        # _extract_early_return_url. The response here is a copy of
        # the actual response Overdrive sends in this situation.
        http.responses.append(MockRequestsResponse(200, content="Success"))

        # Do the thing.
        success = overdrive.perform_early_return(patron, pin, loan, http.do_get)

        # The title was 'returned'.
        eq_(True, success)

        # It worked like this:
        #
        # get_fulfillment_link was called with appropriate arguments.
        eq_(
            (patron, pin, pool.identifier.identifier, 'ebook-epub-adobe'),
            overdrive.get_fulfillment_link_call
        )

        # The URL returned by that method was 'requested'.
        eq_('http://fulfillment/', http.requests.pop(0))

        # The resulting URL was passed into _extract_early_return_url.
        eq_(
            ('http://fulfill-this-book/?or=return-early',),
            overdrive._extract_early_return_url_call
        )

        # Then the URL returned by _that_ method was 'requested'.
        eq_('http://early-return/', http.requests.pop(0))

        # If no early return URL can be extracted from the fulfillment URL,
        # perform_early_return has no effect.
        #
        overdrive._extract_early_return_url_call = None
        overdrive.EARLY_RETURN_URL = None
        http.responses.append(
            MockRequestsResponse(
                302, dict(location="http://fulfill-this-book/")
            )
        )
        success = overdrive.perform_early_return(patron, pin, loan, http.do_get)
        eq_(False, success)

        # extract_early_return_url_call was called, but since it returned
        # None, no second HTTP request was made.
        eq_('http://fulfillment/', http.requests.pop(0))
        eq_(("http://fulfill-this-book/",),
            overdrive._extract_early_return_url_call)
        eq_([], http.requests)

        # If we can't map the delivery mechanism to one of Overdrive's
        # internal formats, perform_early_return has no effect.
        #
        loan.fulfillment.delivery_mechanism.content_type = "not-in/overdrive"
        success = overdrive.perform_early_return(patron, pin, loan, http.do_get)
        eq_(False, success)

        # In this case, no HTTP requests were made at all, since we
        # couldn't figure out which arguments to pass into
        # get_fulfillment_link.
        eq_([], http.requests)

        # If the final attempt to hit the return URL doesn't result
        # in a 200 status code, perform_early_return has no effect.
        http.responses.append(
            MockRequestsResponse(
                302, dict(location="http://fulfill-this-book/?or=return-early")
            )
        )
        http.responses.append(
            MockRequestsResponse(401, content="Unauthorized!")
        )
        success = overdrive.perform_early_return(patron, pin, loan, http.do_get)
        eq_(False, success)

    def test_extract_early_return_url(self):
        m = OverdriveAPI._extract_early_return_url
        eq_(None, m("http://no-early-return/"))
        eq_(None, m(""))
        eq_(None, m(None))

        # This is based on a real Overdrive early return URL.
        has_early_return = 'https://openepub-gk.cdn.overdrive.com/OpenEPUBStore1/1577-1/%7B5880F6D0-48AC-44DE-8BF1-FD1CE62E97A8%7DFzr418.epub?e=1518753718&loanExpirationDate=2018-03-01T17%3a12%3a33Z&loanEarlyReturnUrl=https%3a%2f%2fnotifications-ofs.contentreserve.com%2fEarlyReturn%2fnypl%2f037-1374147-00279%2f5480F6E1-48F3-00DE-96C1-FD3CE32D94FD-312%3fh%3dVgvxBQHdQxtsbgb43AH6%252bEmpni9LoffkPczNiUz7%252b10%253d&sourceId=nypl&h=j7nGk7qxE71X2ZcdLw%2bqa04jqEw%3d'
        eq_('https://notifications-ofs.contentreserve.com/EarlyReturn/nypl/037-1374147-00279/5480F6E1-48F3-00DE-96C1-FD3CE32D94FD-312?h=VgvxBQHdQxtsbgb43AH6%2bEmpni9LoffkPczNiUz7%2b10%3d', m(has_early_return))

    def test_place_hold_raises_exception_if_patron_over_hold_limit(self):
        over_hold_limit = self.error_message(
            "PatronExceededHoldLimit",
            "Patron cannot place any more holds, already has maximum holds placed."
        )

        edition, pool = self._edition(
            identifier_type=Identifier.OVERDRIVE_ID,
            data_source_name=DataSource.OVERDRIVE,
            with_license_pool=True
        )
        self.api.queue_response(400, content=over_hold_limit)
        assert_raises(
            PatronHoldLimitReached,
            self.api.place_hold, self._patron(), 'pin', pool,
            notification_email_address='foo@bar.com'
        )

    def test_place_hold_looks_up_notification_address(self):
        edition, pool = self._edition(
            identifier_type=Identifier.OVERDRIVE_ID,
            data_source_name=DataSource.OVERDRIVE,
            with_license_pool=True
        )

        # The first request we make will be to get patron info,
        # so that we know that the most recent email address used
        # to put a book on hold is foo@bar.com.
        ignore, patron_with_email = self.sample_json(
            "patron_info.json"
        )

        # The second request we make will be to put a book on hold,
        # and when we do so we will ask for the notification to be
        # sent to foo@bar.com.
        ignore, successful_hold = self.sample_json(
            "successful_hold.json"
        )

        self.api.queue_response(200, content=patron_with_email)
        self.api.queue_response(200, content=successful_hold)
        with temp_config() as config:
            config['default_notification_email_address'] = "notifications@example.com"
            hold = self.api.place_hold(self._patron(), 'pin', pool,
                                       notification_email_address=None)

        # The book was placed on hold.
        eq_(1, hold.hold_position)
        eq_(pool.identifier.identifier, hold.identifier)

        # And when we placed it on hold, we passed in foo@bar.com
        # as the email address -- not notifications@example.com.
        url, positional_args, kwargs = self.api.requests[-1]
        headers, body = positional_args
        assert '{"name": "emailAddress", "value": "foo@bar.com"}' in body

    def test_fulfill_raises_exception_and_updates_formats_for_outdated_format(self):
        edition, pool = self._edition(
            identifier_type=Identifier.OVERDRIVE_ID,
            data_source_name=DataSource.OVERDRIVE,
            with_license_pool=True
        )

        # This pool has a format that's no longer available from overdrive.
        pool.set_delivery_mechanism(Representation.PDF_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM,
                                    RightsStatus.IN_COPYRIGHT, None)

        ignore, loan = self.sample_json(
            "single_loan.json"
        )

        ignore, lock_in_format_not_available = self.sample_json(
            "lock_in_format_not_available.json"
        )

        # We will get the loan, try to lock in the format, and fail.
        self.api.queue_response(200, content=loan)
        self.api.queue_response(400, content=lock_in_format_not_available)

        # Trying to get a fulfillment link raises an exception.
        assert_raises(
            FormatNotAvailable,
            self.api.get_fulfillment_link,
            self._patron(), 'pin', pool.identifier.identifier,
            'ebook-epub-adobe'
        )

        # Fulfill will also update the formats.
        ignore, bibliographic = self.sample_json(
            "bibliographic_information.json"
        )

        # To avoid a mismatch, make it look like the information is
        # for the correct Identifier.
        bibliographic['id'] = pool.identifier.identifier

        # If we have the LicensePool available (as opposed to just the
        # identifier), we will get the loan, try to lock in the
        # format, fail, and then update the bibliographic information.
        self.api.queue_response(200, content=loan)
        self.api.queue_response(400, content=lock_in_format_not_available)
        self.api.queue_response(200, content=bibliographic)

        assert_raises(
            FormatNotAvailable,
            self.api.fulfill,
            self._patron(), 'pin', pool,
            'ebook-epub-adobe'
        )

        # The delivery mechanisms have been updated.
        eq_(3, len(pool.delivery_mechanisms))
        eq_(set([Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.KINDLE_CONTENT_TYPE, DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE]),
            set([lpdm.delivery_mechanism.content_type for lpdm in pool.delivery_mechanisms]))
        eq_(set([DeliveryMechanism.ADOBE_DRM, DeliveryMechanism.KINDLE_DRM, DeliveryMechanism.OVERDRIVE_DRM]),
            set([lpdm.delivery_mechanism.drm_scheme for lpdm in pool.delivery_mechanisms]))

    def test_get_fulfillment_link_from_download_link(self):
        patron = self._patron()

        ignore, streaming_fulfill_link = self.sample_json(
            "streaming_fulfill_link_response.json"
        )

        self.api.queue_response(200, content=streaming_fulfill_link)

        href, type = self.api.get_fulfillment_link_from_download_link(patron, '1234', "http://download-link", fulfill_url="http://fulfill")
        eq_("https://fulfill.contentreserve.com/PerfectLife9780345530967.epub-sample.overdrive.com?RetailerID=nypl&Expires=1469825647&Token=dd0e19b4-eb70-439d-8c50-a65201060f4c&Signature=asl67/G154KeeUsL1mHPwEbZfgc=",
            href)
        eq_("text/html", type)

    def test_update_formats(self):
        # Create a LicensePool with an inaccurate delivery mechanism
        # and the wrong medium.
        edition, pool = self._edition(
            data_source_name=DataSource.OVERDRIVE,
            identifier_type=Identifier.OVERDRIVE_ID,
            with_license_pool=True
        )
        edition.medium = Edition.PERIODICAL_MEDIUM

        # Add the bad delivery mechanism.
        pool.set_delivery_mechanism(Representation.PDF_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM,
                                    RightsStatus.IN_COPYRIGHT, None)

        # Prepare the bibliographic information.
        ignore, bibliographic = self.sample_json(
            "bibliographic_information.json"
        )

        # To avoid a mismatch, make it look like the information is
        # for the new pool's Identifier.
        bibliographic['id'] = pool.identifier.identifier

        self.api.queue_response(200, content=bibliographic)

        self.api.update_formats(pool)

        # The delivery mechanisms have been updated.
        eq_(3, len(pool.delivery_mechanisms))
        eq_(set([Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.KINDLE_CONTENT_TYPE, DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE]),
            set([lpdm.delivery_mechanism.content_type for lpdm in pool.delivery_mechanisms]))
        eq_(set([DeliveryMechanism.ADOBE_DRM, DeliveryMechanism.KINDLE_DRM, DeliveryMechanism.OVERDRIVE_DRM]),
            set([lpdm.delivery_mechanism.drm_scheme for lpdm in pool.delivery_mechanisms]))

        # The Edition's medium has been corrected.
        eq_(Edition.BOOK_MEDIUM, edition.medium)

    def test_update_availability(self):
        """Test the Overdrive implementation of the update_availability
        method defined by the CirculationAPI interface.
        """

        # Create a LicensePool that needs updating.
        edition, pool = self._edition(
            identifier_type=Identifier.OVERDRIVE_ID,
            data_source_name=DataSource.OVERDRIVE,
            with_license_pool=True,
            collection=self.collection
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

        self.api.queue_response(200, content=availability)
        self.api.queue_response(200, content=bibliographic)

        self.api.update_availability(pool)

        # The availability information has been updated, as has the
        # date the availability information was last checked.
        eq_(5, pool.licenses_owned)
        eq_(5, pool.licenses_available)
        eq_(0, pool.patrons_in_hold_queue)
        assert pool.last_checked is not None

    def test_circulation_lookup(self):
        """Test the method that actually looks up Overdrive circulation
        information.
        """
        self.api.queue_response(200, content="foo")

        book, (status_code, headers, content) = self.api.circulation_lookup(
            "an identifier"
        )
        eq_(dict(id="an identifier"), book)
        eq_(200, status_code)
        eq_("foo", content)

    def test_update_licensepool_error(self):
        # Create an identifier.
        identifier = self._identifier(
            identifier_type=Identifier.OVERDRIVE_ID
        )
        ignore, availability = self.sample_json(
            "overdrive_availability_information.json"
        )
        self.api.queue_response(500, content="An error occured.")
        book = dict(id=identifier.identifier, availability_link=self._url)
        pool, was_new, changed = self.api.update_licensepool(book)
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

        self.api.queue_response(200, content=availability)
        self.api.queue_response(200, content=bibliographic)

        # Now we're ready. When we call update_licensepool, the
        # OverdriveAPI will retrieve the availability information,
        # then the bibliographic information. It will then trigger the
        # OverdriveBibliographicCoverageProvider, which will
        # create an Edition and a presentation-ready Work.
        pool, was_new, changed = self.api.update_licensepool(identifier.identifier)
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

        # Call update_licensepool on an identifier that is missing a work and make
        # sure that it provides bibliographic coverage in that case.
        self._db.delete(pool.work)
        self._db.commit()
        pool, is_new = LicensePool.for_foreign_id(
            self._db, DataSource.OVERDRIVE, Identifier.OVERDRIVE_ID, identifier.identifier,
            collection=self.collection
        )
        ok_(not pool.work)
        self.api.queue_response(200, content=availability)
        self.api.queue_response(200, content=bibliographic)
        pool, was_new, changed = self.api.update_licensepool(identifier.identifier)
        eq_(False, was_new)
        eq_(True, pool.work.presentation_ready)

    def test_update_new_licensepool(self):
        data, raw = self.sample_json("overdrive_availability_information.json")

        # Create an identifier
        identifier = self._identifier(
            identifier_type=Identifier.OVERDRIVE_ID
        )

        # Make it look like the availability information is for the
        # newly created Identifier.
        raw['id'] = identifier.identifier

        pool, was_new = LicensePool.for_foreign_id(
            self._db, DataSource.OVERDRIVE,
            identifier.type, identifier.identifier,
            collection=self.collection
        )

        pool, was_new, changed = self.api.update_licensepool_with_book_info(
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

        wr.title = u"The real title."
        eq_(1, pool.licenses_owned)
        eq_(1, pool.licenses_available)
        eq_(0, pool.licenses_reserved)
        eq_(0, pool.patrons_in_hold_queue)

        p2, was_new, changed = self.api.update_licensepool_with_book_info(
            raw, pool, False
        )
        eq_(False, was_new)
        eq_(True, changed)
        eq_(p2, pool)
        # The title didn't change to that title given in the availability
        # information, because we already set a title for that work.
        eq_(u"The real title.", wr.title)
        eq_(raw['copiesOwned'], pool.licenses_owned)
        eq_(raw['copiesAvailable'], pool.licenses_available)
        eq_(0, pool.licenses_reserved)
        eq_(raw['numberOfHolds'], pool.patrons_in_hold_queue)

    def test_update_new_licensepool_when_same_book_has_pool_in_different_collection(self):
        old_edition, old_pool = self._edition(
            data_source_name=DataSource.OVERDRIVE,
            identifier_type=Identifier.OVERDRIVE_ID,
            with_license_pool=True,
        )
        old_pool.calculate_work()
        collection = self._collection()

        data, raw = self.sample_json("overdrive_availability_information.json")

        # Make it look like the availability information is for the
        # old pool's Identifier.
        identifier = old_pool.identifier
        raw['id'] = identifier.identifier

        new_pool, was_new = LicensePool.for_foreign_id(
            self._db, DataSource.OVERDRIVE,
            identifier.type, identifier.identifier,
            collection=collection
        )
        # The new pool doesn't have a presentation edition yet,
        # but it will be updated to share the old pool's edition.
        eq_(None, new_pool.presentation_edition)

        new_pool, was_new, changed = self.api.update_licensepool_with_book_info(
            raw, new_pool, was_new
        )
        eq_(True, was_new)
        eq_(True, changed)
        eq_(old_edition, new_pool.presentation_edition)
        eq_(old_pool.work, new_pool.work)

    def test_update_licensepool_with_holds(self):
        data, raw = self.sample_json("overdrive_availability_information_holds.json")
        identifier = self._identifier(
            identifier_type=Identifier.OVERDRIVE_ID
        )
        raw['id'] = identifier.identifier

        license_pool, is_new = LicensePool.for_foreign_id(
            self._db, DataSource.OVERDRIVE, identifier.type,
            identifier.identifier, collection=self._default_collection
        )
        pool, was_new, changed = self.api.update_licensepool_with_book_info(
            raw, license_pool, is_new
        )
        eq_(10, pool.patrons_in_hold_queue)
        eq_(True, changed)

    def test_refresh_patron_access_token(self):
        """Verify that patron information is included in the request
        when refreshing a patron access token.
        """
        patron = self._patron()
        patron.authorization_identifier = 'barcode'
        credential = self._credential(patron=patron)

        data, raw = self.sample_json("patron_token.json")
        self.api.queue_response(200, content=raw)

        # Try to refresh the patron access token with a PIN, and
        # then without a PIN.
        self.api.refresh_patron_access_token(credential, patron, "a pin")

        self.api.refresh_patron_access_token(credential, patron, None)

        # Verify that the requests that were made correspond to what
        # Overdrive is expecting.
        with_pin, without_pin = self.api.access_token_requests
        url, payload, headers, kwargs = with_pin
        eq_("https://oauth-patron.overdrive.com/patrontoken", url)
        eq_("barcode", payload['username'])
        expect_scope = "websiteid:%s authorizationname:%s" % (
            self.api.website_id, self.api.ils_name(patron.library)
        )
        eq_(expect_scope, payload['scope'])
        eq_("a pin", payload['password'])
        assert not 'password_required' in payload

        url, payload, headers, kwargs = without_pin
        eq_("https://oauth-patron.overdrive.com/patrontoken", url)
        eq_("barcode", payload['username'])
        eq_(expect_scope, payload['scope'])
        eq_("false", payload['password_required'])
        eq_("[ignore]", payload['password'])

class TestExtractData(OverdriveAPITest):

    def test_get_download_link(self):
        data, json = self.sample_json("checkout_response_locked_in_format.json")
        url = MockOverdriveAPI.get_download_link(
            json, "ebook-epub-adobe", "http://foo.com/")
        eq_("http://patron.api.overdrive.com/v1/patrons/me/checkouts/76C1B7D0-17F4-4C05-8397-C66C17411584/formats/ebook-epub-adobe/downloadlink?errorpageurl=http://foo.com/", url)

        assert_raises(
            NoAcceptableFormat,
            MockOverdriveAPI.get_download_link,
            json, "no-such-format", "http://foo.com/"
        )

    def test_get_download_link_raises_exception_if_loan_fulfilled_on_incompatible_platform(self):
        data, json = self.sample_json("checkout_response_book_fulfilled_on_kindle.json")
        assert_raises(
            FulfilledOnIncompatiblePlatform,
            MockOverdriveAPI.get_download_link,
            json, "ebook-epub-adobe", "http://foo.com/"
        )

    def test_extract_data_from_checkout_resource(self):
        data, json = self.sample_json("checkout_response_locked_in_format.json")
        expires, url = MockOverdriveAPI.extract_data_from_checkout_response(
            json, "ebook-epub-adobe", "http://foo.com/")
        eq_(2013, expires.year)
        eq_(10, expires.month)
        eq_(4, expires.day)
        eq_("http://patron.api.overdrive.com/v1/patrons/me/checkouts/76C1B7D0-17F4-4C05-8397-C66C17411584/formats/ebook-epub-adobe/downloadlink?errorpageurl=http://foo.com/", url)

    def test_process_checkout_data(self):
        data, json = self.sample_json("shelf_with_book_already_fulfilled_on_kindle.json")
        [on_kindle, not_on_kindle] = json["checkouts"]

        # The book already fulfilled on Kindle doesn't get turned into
        # LoanInfo at all.
        eq_(None, MockOverdriveAPI.process_checkout_data(on_kindle, self.collection))

        # The book not yet fulfilled does show up as a LoanInfo.
        loan_info = MockOverdriveAPI.process_checkout_data(not_on_kindle, self.collection)
        eq_("2fadd2ac-a8ec-4938-a369-4c3260e8922b", loan_info.identifier)

        # Since there are two usable formats (Adobe EPUB and Adobe
        # PDF), the LoanInfo is not locked to any particular format.
        eq_(None, loan_info.locked_to)

        # A book that's on loan and locked to a specific format has a
        # DeliveryMechanismInfo associated with that format.
        data, format_locked_in = self.sample_json("checkout_response_locked_in_format.json")
        loan_info = MockOverdriveAPI.process_checkout_data(format_locked_in, self.collection)
        delivery = loan_info.locked_to
        eq_(Representation.EPUB_MEDIA_TYPE, delivery.content_type)
        eq_(DeliveryMechanism.ADOBE_DRM, delivery.drm_scheme)

        # This book is on loan and the choice between Kindle and Adobe
        # EPUB has not yet been made, but as far as we're concerned,
        # Adobe EPUB is the only *usable* format, so it's effectively
        # locked.
        data, no_format_locked_in = self.sample_json("checkout_response_no_format_locked_in.json")
        loan_info = MockOverdriveAPI.process_checkout_data(no_format_locked_in, self.collection)
        assert loan_info != None
        delivery = loan_info.locked_to
        eq_(Representation.EPUB_MEDIA_TYPE, delivery.content_type)
        eq_(DeliveryMechanism.ADOBE_DRM, delivery.drm_scheme)

        # TODO: In the future both of these tests should return a
        # LoanInfo with appropriate FulfillmentInfo. The calling code
        # would then decide whether or not to show the loan.

class TestSyncBookshelf(OverdriveAPITest):

    def test_sync_bookshelf_creates_local_loans(self):
        loans_data, json_loans = self.sample_json("shelf_with_some_checked_out_books.json")
        holds_data, json_holds = self.sample_json("no_holds.json")

        self.api.queue_response(200, content=loans_data)
        self.api.queue_response(200, content=holds_data)

        patron = self._patron()
        loans, holds = self.circulation.sync_bookshelf(patron, "dummy pin")

        # All four loans in the sample data were created.
        eq_(4, len(loans))
        eq_(loans.sort(), patron.loans.sort())

        # We have created previously unknown LicensePools and
        # Identifiers.
        identifiers = [loan.license_pool.identifier.identifier
                       for loan in loans]
        eq_(sorted([u'a5a3d737-34d4-4d69-aad8-eba4e46019a3',
                    u'99409f99-45a5-4238-9e10-98d1435cde04',
                    u'993e4b33-823c-40af-8f61-cac54e1cba5d',
                    u'a2ec6f3a-ebfe-4c95-9638-2cb13be8de5a']),
            sorted(identifiers)
        )

        # We have recorded a new DeliveryMechanism associated with
        # each loan.
        mechanisms = []
        for loan in loans:
            if loan.fulfillment:
                mechanism = loan.fulfillment.delivery_mechanism
                mechanisms.append(
                    (mechanism.content_type, mechanism.drm_scheme)
                )
        eq_(
            [
                (Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.NO_DRM),
                (Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM),
                (Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM),
                (Representation.PDF_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM),
            ],
            sorted(mechanisms)
        )

        # There are no holds.
        eq_([], holds)

        # Running the sync again leaves all four loans in place.
        self.api.queue_response(200, content=loans_data)
        self.api.queue_response(200, content=holds_data)
        loans, holds = self.circulation.sync_bookshelf(patron, "dummy pin")
        eq_(4, len(loans))
        eq_(loans.sort(), patron.loans.sort())

    def test_sync_bookshelf_removes_loans_not_present_on_remote(self):
        loans_data, json_loans = self.sample_json("shelf_with_some_checked_out_books.json")
        holds_data, json_holds = self.sample_json("no_holds.json")

        self.api.queue_response(200, content=loans_data)
        self.api.queue_response(200, content=holds_data)

        # Create a loan not present in the sample data.
        patron = self._patron()
        overdrive_edition, new = self._edition(
            data_source_name=DataSource.OVERDRIVE,
            with_license_pool=True, collection=self.collection
        )
        [pool] = overdrive_edition.license_pools
        overdrive_loan, new = pool.loan_to(patron)
        yesterday = datetime.utcnow() - timedelta(days=1)
        overdrive_loan.start = yesterday

        # Sync with Overdrive, and the loan not present in the sample
        # data is removed.
        loans, holds = self.circulation.sync_bookshelf(patron, "dummy pin")

        eq_(4, len(loans))
        eq_(set(loans), set(patron.loans))
        assert overdrive_loan not in patron.loans

    def test_sync_bookshelf_ignores_loans_from_other_sources(self):
        patron = self._patron()
        gutenberg, new = self._edition(data_source_name=DataSource.GUTENBERG,
                                       with_license_pool=True)
        [pool] = gutenberg.license_pools
        gutenberg_loan, new = pool.loan_to(patron)
        loans_data, json_loans = self.sample_json("shelf_with_some_checked_out_books.json")
        holds_data, json_holds = self.sample_json("no_holds.json")

        # Overdrive doesn't know about the Gutenberg loan, but it was
        # not destroyed, because it came from another source.
        self.api.queue_response(200, content=loans_data)
        self.api.queue_response(200, content=holds_data)

        loans, holds = self.circulation.sync_bookshelf(patron, "dummy pin")
        eq_(5, len(patron.loans))
        assert gutenberg_loan in patron.loans

    def test_sync_bookshelf_creates_local_holds(self):

        loans_data, json_loans = self.sample_json("no_loans.json")
        holds_data, json_holds = self.sample_json("holds.json")

        self.api.queue_response(200, content=loans_data)
        self.api.queue_response(200, content=holds_data)
        patron = self._patron()

        loans, holds = self.circulation.sync_bookshelf(patron, "dummy pin")
        # All four loans in the sample data were created.
        eq_(4, len(holds))
        eq_(sorted(holds), sorted(patron.holds))

        # Running the sync again leaves all four holds in place.
        self.api.queue_response(200, content=loans_data)
        self.api.queue_response(200, content=holds_data)
        loans, holds = self.circulation.sync_bookshelf(patron, "dummy pin")
        eq_(4, len(holds))
        eq_(sorted(holds), sorted(patron.holds))

    def test_sync_bookshelf_removes_holds_not_present_on_remote(self):
        loans_data, json_loans = self.sample_json("no_loans.json")
        holds_data, json_holds = self.sample_json("holds.json")

        patron = self._patron()
        overdrive_edition, new = self._edition(
            data_source_name=DataSource.OVERDRIVE,
            with_license_pool=True,
            collection=self.collection
        )
        [pool] = overdrive_edition.license_pools
        overdrive_hold, new = pool.on_hold_to(patron)


        self.api.queue_response(200, content=loans_data)
        self.api.queue_response(200, content=holds_data)

        # The hold not present in the sample data has been removed
        loans, holds = self.circulation.sync_bookshelf(patron, "dummy pin")
        eq_(4, len(holds))
        eq_(holds, patron.holds)
        assert overdrive_hold not in patron.loans

    def test_sync_bookshelf_ignores_holds_from_other_collections(self):
        loans_data, json_loans = self.sample_json("no_loans.json")
        holds_data, json_holds = self.sample_json("holds.json")

        patron = self._patron()

        # This patron has an Overdrive book on hold, but it derives
        # from an Overdrive Collection that's not managed by
        # self.circulation.
        overdrive, new = self._edition(
            data_source_name=DataSource.OVERDRIVE,
            with_license_pool=True,
            collection=self._collection()
        )
        [pool] = overdrive.license_pools
        overdrive_hold, new = pool.on_hold_to(patron)

        self.api.queue_response(200, content=loans_data)
        self.api.queue_response(200, content=holds_data)

        # self.api doesn't know about the hold, but it was not
        # destroyed, because it came from a different collection.
        loans, holds = self.circulation.sync_bookshelf(patron, "dummy pin")
        eq_(5, len(patron.holds))
        assert overdrive_hold in patron.holds

class TestOverdriveCirculationMonitor(OverdriveAPITest):

    def test_run(self):
        # An end-to-end test verifying that this Monitor manages its
        # state across multiple runs.
        #
        # This tests a lot of code that's technically not in Monitor,
        # but when the Monitor API changes, it may require changes to
        # this particular monitor, and it's good to have a test that
        # will fail if that's true.
        class Mock(OverdriveCirculationMonitor):
            def catch_up_from(self, start, cutoff, progress):
                self.catch_up_from_called_with = (start, cutoff, progress)

        monitor = Mock(self._db, self.collection)

        monitor.run()
        start, cutoff, progress = monitor.catch_up_from_called_with
        now = datetime.utcnow()

        # The first time this Monitor is called, its 'start time' is
        # the current time, and we ask for an overlap of one minute.
        # This isn't very effective, but we have to start somewhere.
        #
        # (This isn't how the Overdrive collection is initially
        # populated, BTW -- that's NewTitlesOverdriveCollectionMonitor.)
        self.time_eq(start, now-monitor.OVERLAP)
        self.time_eq(cutoff, now)
        timestamp = monitor.timestamp()
        eq_(start, timestamp.start)
        eq_(cutoff, timestamp.finish)

        # The second time the Monitor is called, its 'start time'
        # is one minute before the previous cutoff time.
        monitor.run()
        new_start, new_cutoff, new_progress = monitor.catch_up_from_called_with
        now = datetime.utcnow()
        eq_(new_start, cutoff-monitor.OVERLAP)
        self.time_eq(new_cutoff, now)

    def test_catch_up_from(self):
        # catch_up_from() asks Overdrive about recent changes by
        # calling recently_changed_ids().
        #
        # It mirrors those changes locally by calling
        # update_licensepool().
        #
        # If this is our first time encountering a book, a
        # DISTRIBUTOR_TITLE_ADD analytics event is sent out.
        #
        # The method stops when should_stop() -- called on every book
        # -- returns True.
        class MockAPI(object):
            def __init__(self, *ignore, **kwignore):
                self.licensepools = []
                self.update_licensepool_calls = []

            def update_licensepool(self, book_id):
                pool, is_new, is_changed = self.licensepools.pop(0)
                self.update_licensepool_calls.append((book_id, pool))
                return pool, is_new, is_changed

        class MockAnalytics(object):
            def __init__(self, _db):
                self._db= _db
                self.events = []

            def collect_event(self, *args):
                self.events.append(args)

        class MockMonitor(OverdriveCirculationMonitor):

            recently_changed_ids_called_with = None
            should_stop_calls = []
            def recently_changed_ids(self, start, cutoff):
                self.recently_changed_ids_called_with = (start, cutoff)
                return [1, 2, None, 3, 4]

            def should_stop(self, start, book, is_changed):
                # We're going to stop after the third valid book,
                # ensuring that we never ask 'Overdrive' for the
                # fourth book.
                self.should_stop_calls.append((start, book, is_changed))
                if book == 3:
                    return True
                return False

        monitor = MockMonitor(self._db, self.collection, api_class=MockAPI,
                              analytics_class=MockAnalytics)
        api = monitor.api

        # A MockAnalytics object was created and is ready to receive analytics
        # events.
        assert isinstance(monitor.analytics, MockAnalytics)
        eq_(self._db, monitor.analytics._db)

        # The 'Overdrive API' is ready to tell us about four books,
        # but only one of them (the first) represents a change from what
        # we already know.
        lp1 = self._licensepool(None)
        lp1.last_checked = datetime.utcnow()
        lp2 = self._licensepool(None)
        lp3 = self._licensepool(None)
        lp4 = object()
        api.licensepools.append((lp1, True, True))
        api.licensepools.append((lp2, False, False))
        api.licensepools.append((lp3, False, True))
        api.licensepools.append(lp4)

        progress = TimestampData()
        start = object()
        cutoff = object()
        monitor.catch_up_from(start, cutoff, progress)

        # The monitor called recently_changed_ids with the start and
        # cutoff times. It returned five 'books', one of which was None --
        # simulating a lack of data from Overdrive.
        eq_((start, cutoff), monitor.recently_changed_ids_called_with)

        # The monitor ignored the empty book and called
        # update_licensepool on the first three valid 'books'. The
        # mock API delivered the first three LicensePools from the
        # queue.
        eq_([(1, lp1),(2, lp2),(3, lp3)], api.update_licensepool_calls)

        # After each book was processed, should_stop was called, using
        # the LicensePool, the start date, plus information about
        # whether the LicensePool was changed (or created) during
        # update_licensepool().
        eq_(
            [(start, 1, True),
             (start, 2, False),
             (start, 3, True)],
            monitor.should_stop_calls
        )

        # should_stop returned True on the third call, and at that
        # point we gave up.

        # The fourth (bogus) LicensePool is still in api.licensepools,
        # because we never asked for it.
        eq_([lp4], api.licensepools)

        # A single analytics event was sent out, for the first LicensePool,
        # the one that update_licensepool said was new.
        [[library, licensepool, event, last_checked]] = monitor.analytics.events

        # The event commemerates the addition of this LicensePool to the
        # collection.
        eq_(lp1.collection.libraries, [library])
        eq_(lp1, licensepool)
        eq_(CirculationEvent.DISTRIBUTOR_TITLE_ADD, event)
        eq_(lp1.last_checked, last_checked)

        # The incoming TimestampData object was updated with
        # a summary of what happened.
        #
        # We processed four books: 1, 2, None (which was ignored)
        # and 3.
        eq_("Books processed: 4.", progress.achievements)


class TestNewTitlesOverdriveCollectionMonitor(OverdriveAPITest):

    def test_recently_changed_ids(self):
        class MockAPI(object):
            def __init__(self, *args, **kwargs):
                pass
            def all_ids(self):
                return "all of the ids"

        monitor = NewTitlesOverdriveCollectionMonitor(
            self._db, self.collection, api_class=MockAPI
        )
        eq_("all of the ids", monitor.recently_changed_ids(object(), object()))

    def test_should_stop(self):
        monitor = NewTitlesOverdriveCollectionMonitor(
            self._db, self.collection, api_class=MockOverdriveAPI
        )

        m = monitor.should_stop

        # If the monitor has never run before, we need to keep going
        # until we run out of books.
        eq_(False, m(None, object(), object()))
        eq_(False, m(monitor.NEVER, object(), object()))

        # If information is missing or invalid, we assume that we
        # should keep going.
        start = datetime(2018, 1, 1)
        eq_(False, m(start, {}, object()))
        eq_(False, m(start, {'date_added': None}, object()))
        eq_(False, m(start, {'date_added': "Not a date"}, object()))

        # Here, we're actually comparing real dates, using the date
        # format found in the Overdrive API. A date that's after the
        # `start` date means we should keep going backwards. A date before
        # the `start` date means we should stop.
        eq_(False, m(start, {'date_added': '2019-07-12T11:06:38.157+01:00'}, object()))
        eq_(True, m(start, {'date_added': '2017-07-12T11:06:38.157-04:00'}, object()))


class TestNewTitlesOverdriveCollectionMonitor(OverdriveAPITest):

    def test_should_stop(self):
        monitor = RecentOverdriveCollectionMonitor(
            self._db, self.collection, api_class=MockOverdriveAPI
        )
        eq_(0, monitor.consecutive_unchanged_books)
        m = monitor.should_stop

        # This book hasn't been changed, but we're under the limit, so we should
        # keep going.
        eq_(False, m(object(), object(), False))
        eq_(1, monitor.consecutive_unchanged_books)

        eq_(False, m(object(), object(), False))
        eq_(2, monitor.consecutive_unchanged_books)

        # This book has changed, so our counter gets reset.
        eq_(False, m(object(), object(), True))
        eq_(0, monitor.consecutive_unchanged_books)

        # When we're at the limit, and another book comes along that hasn't
        # been changed, _then_ we decide to stop.
        monitor.consecutive_unchanged_books = monitor.MAXIMUM_CONSECUTIVE_UNCHANGED_BOOKS
        eq_(True, m(object(), object(), False))
        eq_(monitor.MAXIMUM_CONSECUTIVE_UNCHANGED_BOOKS+1,
            monitor.consecutive_unchanged_books)


class TestOverdriveFormatSweep(OverdriveAPITest):

    def test_process_item(self):
        # Validate the standard CollectionMonitor interface.
        monitor = OverdriveFormatSweep(
            self._db, self.collection,
            api_class=MockOverdriveAPI
        )
        monitor.api.queue_collection_token()
        # We're not testing that the work actually gets done (that's
        # tested in test_update_formats), only that the monitor
        # implements the expected process_item API without crashing.
        monitor.api.queue_response(404)
        edition, pool = self._edition(with_license_pool=True)
        monitor.process_item(pool.identifier)

    def test_process_item_multiple_licence_pools(self):
        # Make sure that we only call update_formats once when an item
        # is part of multiple licensepools.

        class MockApi(MockOverdriveAPI):
            update_format_calls = 0
            def update_formats(self, licensepool):
                self.update_format_calls += 1

        monitor = OverdriveFormatSweep(
            self._db, self.collection,
            api_class=MockApi
        )
        monitor.api.queue_collection_token()
        monitor.api.queue_response(404)

        edition = self._edition()
        collection1 = self._collection(name="Collection 1")
        pool1 = self._licensepool(edition, collection=collection1)

        collection2 = self._collection(name="Collection 2")
        pool2 = self._licensepool(edition, collection=collection2)

        monitor.process_item(pool1.identifier)
        eq_(1, monitor.api.update_format_calls)


class TestReaper(OverdriveAPITest):

    def test_instantiate(self):
        # Validate the standard CollectionMonitor interface.
        monitor = OverdriveCollectionReaper(
            self._db, self.collection,
            api_class=MockOverdriveAPI
        )
