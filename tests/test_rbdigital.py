import datetime
from dateutil.relativedelta import relativedelta
import json
from lxml import etree
import os
import random
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
    RBDigitalSyncMonitor,
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
    TimestampData,
)

from core.model import (
    get_one_or_create,
    Classification,
    ConfigurationSetting,
    Contributor,
    Credential,
    Collection,
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
        self.resource_path = os.path.join(self.base_path, "files", "rbdigital")

        # Make sure the default library is created so that it will
        # be configured properly with the mock collection.
        self._default_library
        self.collection = MockRBDigitalAPI.mock_collection(self._db)
        self.api = MockRBDigitalAPI(
            self._db, self.collection, base_path=self.base_path
        )

    def get_data(self, filename):
        # returns contents of sample file as string and as dict
        path = os.path.join(self.resource_path, filename)
        data = open(path).read()
        return data, json.loads(data)

    @property
    def default_patron(self):
        """Create a default patron on demand."""
        if not hasattr(self, '_default_patron'):
            self._default_patron = self._patron(
                external_identifier="rbdigital_testuser"
            )
            self._default_patron.authorization_identifier="13057226"
        return self._default_patron

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
        eq_(
            [u'Tricks', u'Emperor Mage: The Immortals', u'In-Flight Russian'],
            [x['title'] for x in catalog]
        )


    def test_fuzzy_binary_searcher(self):
        # A fuzzy binary searcher sorts an array by its key, and then must either:
        # - find an exact match, if one exists; or
        # - return an "adjacent" index and the direction in which a match
        #   would have been found, had one existed.
        array = [5, 3, 10, 19, -1, 8, -7]  # => [-7, -1, 3, 5, 8, 10, 19]
        search = self.api._FuzzyBinarySearcher(array)

        nine_idx, nine_rel = search(9)
        eq_((nine_idx == 4 and nine_rel == search.INDEXED_LESS_THAN_MATCH) or
            (nine_idx == 4 and nine_rel == search.INDEXED_GREATER_THAN_MATCH), True)

        ten = search(10)
        eq_(True, ten == (5, search.INDEXED_EQUALS_MATCH))

        neg5 = search(-5)
        eq_(True, neg5 == (0, search.INDEXED_LESS_THAN_MATCH) or (1, search.INDEXED_GREATER_THAN_MATCH))

        # make sure we can hit the edges
        neg7 = search(-7)
        nineteen = search(19)
        eq_(True, neg7 == (0, search.INDEXED_EQUALS_MATCH))
        eq_(True, nineteen == (6, search.INDEXED_EQUALS_MATCH))

        # and beyond the edges
        neg100 = search(-100)
        pos100 = search(100)
        eq_(True, neg100 == (0, search.INDEXED_GREATER_THAN_MATCH))
        eq_(True, pos100 == (6, search.INDEXED_LESS_THAN_MATCH))

        # Lookups in more complicated objects
        _, snapshots = self.api.get_data("response_catalog_availability_dates_multi.json")
        snapshots_max_index = len(snapshots) -1
        # The following are the earliest and latest dates in the snapshot test file.
        first_snapshot = "2016-04-01"
        last_snapshot = "2020-04-14"
        # dates that are well before and well after any available snapshot
        neg_infinity = "1960-01-01"
        pos_infinity = "2999-12-31"

        # create the searcher object
        snap_date_searcher = self.api._FuzzyBinarySearcher(snapshots, key=lambda s: s["asOf"])
        sorted_snapshots = snap_date_searcher.sorted_list
        eq_(first_snapshot, sorted_snapshots[0]["asOf"])
        eq_(last_snapshot, sorted_snapshots[snapshots_max_index]["asOf"])

        first = snap_date_searcher(first_snapshot)
        last = snap_date_searcher(last_snapshot)
        eq_(first, (0, snap_date_searcher.INDEXED_EQUALS_MATCH))
        eq_(last, (snapshots_max_index, snap_date_searcher.INDEXED_EQUALS_MATCH))

        very_neg = snap_date_searcher(neg_infinity)
        very_pos = snap_date_searcher(pos_infinity)
        eq_(very_neg, (0, snap_date_searcher.INDEXED_GREATER_THAN_MATCH))
        eq_(very_pos, (snapshots_max_index, snap_date_searcher.INDEXED_LESS_THAN_MATCH))

        assert_raises_regexp(
            TypeError, ".*'key' must be 'None' or a callable.",
            self.api._FuzzyBinarySearcher, snapshots, key="not a callable"
        )

    def test_align_delta_dates_to_available_snapshots(self):
        datastr, datadict = self.api.get_data("response_catalog_availability_dates_multi.json")
        # The following are the earliest and latest dates in the snapshot test file.
        first_snapshot = "2016-04-01"
        last_snapshot = "2020-04-14"

        # A missing begin date should be assigned the date of the earliest
        # snapshot; a missing end date, should get the date of the latest.
        self.api.queue_response(status_code=200, content=datastr)
        from_date, to_date = self.api.align_dates_to_available_snapshots()
        eq_(first_snapshot, from_date)
        eq_(last_snapshot, to_date)

        # Items at the temporal beginning and end of
        # the snapshot list should match when specified
        self.api.queue_response(status_code=200, content=datastr)
        from_date, to_date = self.api.align_dates_to_available_snapshots(from_date=first_snapshot, to_date=last_snapshot)
        eq_(first_snapshot, from_date)
        eq_(last_snapshot, to_date)

        # A unmatched from_date should be assigned the date of the previous
        # snapshot (or the first snapshot, if there is not an earlier one).
        # An unmatched to_date should be assigned the date of the next
        # snapshot (or the last snapshot, if there is not a later one).
        self.api.queue_response(status_code=200, content=datastr)
        from_date, to_date = self.api.align_dates_to_available_snapshots(from_date="2016-06-15", to_date="2020-03-22")
        eq_("2016-06-01", from_date)
        eq_("2020-03-22", to_date)

        self.api.queue_response(status_code=200, content=datastr)
        from_date, to_date = self.api.align_dates_to_available_snapshots(from_date="2016-05-31", to_date="2016-09-02")
        eq_("2016-05-01", from_date)
        eq_("2016-10-01", to_date)

        self.api.queue_response(status_code=200, content=datastr)
        from_date, to_date = self.api.align_dates_to_available_snapshots(from_date="1960-01-01", to_date="2999-12-31")
        eq_(first_snapshot, from_date)
        eq_(last_snapshot, to_date)

        # date alignment cannot work without at least one snapshot
        self.api.queue_response(status_code=200, content=u"[]")
        assert_raises_regexp(
            BadResponseException, ".*RBDigital available-dates response contains no snapshots.",
            self.api.align_dates_to_available_snapshots, from_date="2000-02-02", to_date="2000-01-01"
        )
        self.api.queue_response(status_code=200, content=u"[]")
        assert_raises_regexp(
            BadResponseException, ".*RBDigital available-dates response contains no snapshots.",
            self.api.align_dates_to_available_snapshots
        )

        # exception for invalid json
        self.api.queue_response(status_code=200, content="this is not JSON")
        assert_raises_regexp(
            BadResponseException, ".*RBDigital available-dates response not parsable.",
            self.api.align_dates_to_available_snapshots
        )


    def test_get_delta(self):
        assert_raises_regexp(
            ValueError, 'from_date 2000-02-02 cannot be after to_date 2000-01-01.',
            self.api.get_delta, from_date="2000-02-02", to_date="2000-01-01"
        )

        # The effective begin and end snapshot dates (after availability alignment)
        # cannot be the same.
        # This can happen when from_date and to_date from the call were the same
        # and there is an exact snapshot date match, ...
        available_dates_string, datadict = self.api.get_data("response_catalog_availability_dates_multi.json")
        self.api.queue_response(status_code=200, content=available_dates_string)
        assert_raises_regexp(
            ValueError, 'The effective begin and end RBDigital catalog snapshot dates cannot be the same.',
            self.api.get_delta, from_date="2020-04-01", to_date="2020-04-01"
        )
        # but can also occur when:
        # - both dates are less than the date of the first snapshot, ...
        self.api.queue_response(status_code=200, content=available_dates_string)
        assert_raises_regexp(
            ValueError, 'The effective begin and end RBDigital catalog snapshot dates cannot be the same.',
            self.api.get_delta, from_date="1960-01-01", to_date="1960-01-02"
        )
        # - both dates are greater than the date of the last snapshot, or ...
        self.api.queue_response(status_code=200, content=available_dates_string)
        assert_raises_regexp(
            ValueError, 'The effective begin and end RBDigital catalog snapshot dates cannot be the same.',
            self.api.get_delta, from_date="2999-12-31", to_date="2999-12-31"
        )
        # - only a single snapshot is available
        datastr, datadict = self.api.get_data("response_catalog_availability_dates_only_one.json")
        self.api.queue_response(status_code=200, content=datastr)
        assert_raises_regexp(
            ValueError, 'The effective begin and end RBDigital catalog snapshot dates cannot be the same.',
            self.api.get_delta, from_date="1960-01-01", to_date="2999-12-31"
        )
        self.api.queue_response(status_code=200, content=datastr)
        assert_raises_regexp(
            ValueError, 'The effective begin and end RBDigital catalog snapshot dates cannot be the same.',
            self.api.get_delta
        )

        # Retrieving a delta requires first retrieving a list of dated
        # snapshots, then retrieving the changes between those dates.
        datastr, datadict = self.api.get_data("response_catalog_availability_dates_multi.json")
        self.api.queue_response(status_code=200, content=datastr)
        datastr, datadict = self.api.get_data("response_catalog_delta.json")
        self.api.queue_response(status_code=200, content=datastr)

        delta = self.api.get_delta()
        eq_(1931, delta["tenantId"])
        eq_("2020-03-14", delta["beginDate"])
        eq_("2020-04-14", delta["endDate"])
        eq_(1, delta["booksAddedCount"])
        eq_(1, delta["booksRemovedCount"])
        eq_([{u'isbn': u'9781934180723', u'id': 1301944, u'mediaType': u'eAudio'}], delta["addedBooks"])
        eq_([{u'isbn': u'9780590543439', u'id': 1031919, u'mediaType': u'eAudio'}], delta["removedBooks"])

    def test_patron_remote_identifier_new_patron(self):
        # End-to-end test of patron_remote_identifier, in the case
        # where we are able to register the patron.

        class NeverHeardOfYouAPI(RBDigitalAPI):
            """A mock RBDigitalAPI that has never heard of any patron
            and returns a known ID as a way of registering them.
            """
            def patron_remote_identifier_lookup(self, patron):
                """This API has never heard of any patron."""
                return None

            def create_patron(self, *args):
                self.called_with = args
                return "rbdigital internal id"

        api = NeverHeardOfYouAPI(self._db, self.collection)

        patron = self.default_patron

        # If it turns out the API has never heard of a given patron, a
        # second call is made to create_patron().
        eq_("rbdigital internal id", api.patron_remote_identifier(patron))

        library, authorization_identifier, email_address = api.called_with

        # A permanent Credential has been created for the remote
        # identifier.
        self._assert_patron_has_remote_identifier_credential(
            patron, "rbdigital internal id"
        )

        # The patron's library and authorization identifier were passed
        # into create_patron.
        eq_(patron.library, library)
        eq_(patron.authorization_identifier, authorization_identifier)

        # We didn't set up the patron with a fake email address,
        # so we weren't able to find anything and no email address
        # was passed into create_patron.
        eq_(None, email_address)

    def test_patron_remote_identifier_existing_patron(self):
        # End-to-end test of patron_remote_identifier, in the case
        # where we already know the patron's internal RBdigital ID.

        class IKnowYouAPI(RBDigitalAPI):
            """A mock RBDigitalAPI that has heard of any given
            patron but will refuse to register a new patron.
            """
            def patron_remote_identifier_lookup(self, patron):
                return "i know you"

            def create_patron(self, *args):
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

    def test_patron_remote_identifier(self):
        # Mocked-up test of patron_remote_identifier, as opposed to
        # the tests above, which mock only the methods that would
        # access the RBdigital API.
        class Mock(MockRBDigitalAPI):
            called_with = None
            def _find_or_create_remote_account(self, patron):
                if self.called_with:
                    raise Exception("I was already called!")
                self.called_with = patron
                return "rbdigital internal id"

        # The first time we call patron_remote_identifier,
        # _find_or_create_remote_account is called, and the result is
        # associated with a Credential for the patron.
        api = Mock(self._db, self.collection, base_path=self.base_path)
        patron = self._patron()
        eq_("rbdigital internal id", api.patron_remote_identifier(patron))
        self._assert_patron_has_remote_identifier_credential(
            patron, "rbdigital internal id"
        )
        eq_(patron, api.called_with)

        # The second time, _find_or_create_remove_account is _not_
        # called -- calling the mock method again would raise an
        # exception. Instead, the cached Credential is returned.
        eq_("rbdigital internal id", api.patron_remote_identifier(patron))

    def test__find_or_create_remote_account(self):
        # If the remote lookup succeeds (because the patron already
        # made an account using their barcode), create_patron() is not
        # called.
        class RemoteLookupSucceeds(MockRBDigitalAPI):

            def patron_remote_identifier_lookup(self, identifier):
                self.patron_remote_identifier_lookup_called_with = identifier
                return "an internal ID"

            def create_patron(self):
                raise Exception("I'll never be called.")

        api = RemoteLookupSucceeds(
            self._db, self.collection, base_path=self.base_path
        )
        patron = self._patron("a barcode")
        patron.authorization_identifier = "a barcode"
        eq_("an internal ID", api._find_or_create_remote_account(patron))
        eq_("a barcode", api.patron_remote_identifier_lookup_called_with)

        # If the remote lookup fails, create_patron() is called
        # with the patron's library, authorization identifier, and
        # email address.
        class RemoteLookupFails(MockRBDigitalAPI):
            def patron_remote_identifier_lookup(self, identifier):
                self.patron_remote_identifier_lookup_called_with = identifier
                return None

            def create_patron(self, *args):
                self.create_patron_called_with = args
                return "an internal ID"

            def patron_email_address(self, patron):
                self.patron_email_address_called_with = patron
                return "mock email address"

        api = RemoteLookupFails(
            self._db, self.collection, base_path=self.base_path
        )
        eq_("an internal ID", api._find_or_create_remote_account(patron))
        eq_("a barcode", api.patron_remote_identifier_lookup_called_with)
        eq_(patron, api.patron_email_address_called_with)
        eq_((patron.library, patron.authorization_identifier,
             "mock email address"), api.create_patron_called_with)

        # If a remote lookup fails, and create patron fails with a
        # RemotePatronCreationFailedException we will try to do a patron
        # lookup with the email address instead.
        class RemoteLookupFailAndRecovery(MockRBDigitalAPI):
            patron_remote_identifier_lookup_called_with = []
            patron_email_address_called_with = []

            def patron_remote_identifier_lookup(self, identifier):
                self.patron_remote_identifier_lookup_called_with.append(identifier)
                if len(self.patron_remote_identifier_lookup_called_with) == 1:
                    return None
                else:
                    return "an internal ID"

            def create_patron(self, *args):
                raise RemotePatronCreationFailedException

            def patron_email_address(self, patron):
                self.patron_email_address_called_with.append(patron)
                return "mock email address"

        api = RemoteLookupFailAndRecovery(
            self._db, self.collection, base_path=self.base_path
        )
        eq_("an internal ID", api._find_or_create_remote_account(patron))
        eq_(["a barcode", "mock email address"], api.patron_remote_identifier_lookup_called_with)
        eq_([patron, patron], api.patron_email_address_called_with)


        # If a remote lookup fails, and create patron fails with a
        # RemotePatronCreationFailedException we will try to do a patron
        # lookup with the email address instead, but if that fails as well
        # we just pass on the exception.
        class RemoteLookupFailAndRecoveryAndFail(MockRBDigitalAPI):
            patron_remote_identifier_lookup_called_with = []
            patron_email_address_called_with = []

            def patron_remote_identifier_lookup(self, identifier):
                self.patron_remote_identifier_lookup_called_with.append(identifier)
                return None

            def create_patron(self, *args):
                raise RemotePatronCreationFailedException

            def patron_email_address(self, patron):
                self.patron_email_address_called_with.append(patron)
                return "mock email address"

        api = RemoteLookupFailAndRecoveryAndFail(
            self._db, self.collection, base_path=self.base_path
        )
        assert_raises(RemotePatronCreationFailedException, api._find_or_create_remote_account, patron)
        eq_(["a barcode", "mock email address"], api.patron_remote_identifier_lookup_called_with)
        eq_([patron, patron], api.patron_email_address_called_with)

    def test_create_patron(self):
        # Test the method that creates an RBdigital account for a
        # library patron.

        class Mock(MockRBDigitalAPI):
            def _create_patron_body(
                    self, library, authorization_identifier, email_address
            ):
                self.called_with = (
                    library, authorization_identifier, email_address
                )
        api = Mock(self._db, self.collection, base_path=self.base_path)

        # Test the case where the patron can be created.
        datastr, datadict = api.get_data(
            "response_patron_create_success.json"
        )
        api.queue_response(status_code=201, content=datastr)
        args = "library", "auth", "email"
        patron_rbdigital_id = api.create_patron(*args)

        # The arguments we passed in were propagated to _create_patron_body.
        eq_(args, api.called_with)

        # The return value is the internal ID RBdigital established for this
        # patron.
        eq_(940000, patron_rbdigital_id)

        # Test the case where the patron already exists.
        datastr, datadict = api.get_data("response_patron_create_fail_already_exists.json")
        api.queue_response(status_code=409, content=datastr)
        assert_raises_regexp(
            RemotePatronCreationFailedException, 'create_patron: http=409, response={"message":"A patron account with the specified username, email address, or card number already exists for this library."}',
            api.create_patron, *args
        )

    def test_patron_remote_identifier_exception(self):
        # Make sure if there is an exception while creating the patron we don't
        # create empty credentials in the database.

        class ApiThrowsException(MockRBDigitalAPI):
            def _find_or_create_remote_account(self, patron):
                raise CirculationException

        patron = self._patron("a barcode")
        api = ApiThrowsException(self._db, self.collection, base_path=self.base_path)
        assert_raises(CirculationException, api.patron_remote_identifier, patron)
        data_source = DataSource.lookup(self._db, DataSource.RB_DIGITAL)
        credential, new = get_one_or_create(
            self._db,
            Credential,
            data_source=data_source,
            type=Credential.IDENTIFIER_FROM_REMOTE_SERVICE,
            patron=patron,
            collection=api.collection
        )
        eq_(True, new)



    def test__create_patron_body(self):
        # Test the method that builds the data (possibly fake, possibly not)
        # for an RBdigital patron creation call.

        class Mock(MockRBDigitalAPI):
            dummy_patron_identifier_called_with = None
            dummy_email_address_called_with = None

            def dummy_patron_identifier(self, authorization_identifier):
                self.dummy_patron_identifier_called_with = (
                    authorization_identifier
                )
                return "dummyid"

            def dummy_email_address(self, library, authorization_identifier):
                self.dummy_email_address_called_with = (
                    library, authorization_identifier
                )
                return "dummy@email"

        api = Mock(self._db, self.collection, base_path=self.base_path)

        # Test the case where a 'real' email address is provided.
        library = object()
        identifier = "auth_identifier"
        email = "me@email"
        body = api._create_patron_body(library, identifier, email)

        # We can't test the password, even by seeding the random
        # number generator, because it's generated with os.urandom(),
        # but we can verify that it's the right length.
        password = body.pop("password")
        eq_(16, len(password))

        # And we can directly check every other value.
        expect = {
            'userName': identifier,
            'firstName': 'Library',
            'libraryCard': identifier,
            'lastName': 'Simplified',
            'postalCode': '11111',
            'libraryId': api.library_id,
            'email': email
        }
        eq_(expect, body)

        # dummy_patron_identifier and dummy_email_address were not called,
        # since we're able to create an RBdigital account that the patron
        # can use through other means.
        eq_(None, api.dummy_patron_identifier_called_with)
        eq_(None, api.dummy_email_address_called_with)

        # Test the case where no 'real' email address is provided.
        body = api._create_patron_body(library, identifier, None)
        body.pop("password")
        expect = {
            'userName': 'dummyid',
            'firstName': 'Library',
            'libraryCard': 'dummyid',
            'lastName': 'Simplified',
            'postalCode': '11111',
            'libraryId': api.library_id,
            'email': 'dummy@email'
        }
        eq_(expect, body)

        # dummy_patron_identifier and dummy_email_address were called.
        eq_(identifier, api.dummy_patron_identifier_called_with)
        eq_((library, identifier), api.dummy_email_address_called_with)

    def test_dummy_patron_identifier(self):
        random.seed(42)
        patron = self.default_patron
        auth = patron.authorization_identifier
        remote_auth = self.api.dummy_patron_identifier(auth)

        # The dummy identifier is the input identifier plus
        # 6 random characters.
        eq_(auth + "N098QO", remote_auth)

        # It's different every time.
        remote_auth = self.api.dummy_patron_identifier(auth)
        eq_(auth + "W3F17I", remote_auth)

    def test_dummy_email_address(self):

        patron = self.default_patron
        library = patron.library
        auth = patron.authorization_identifier
        m = self.api.dummy_email_address

        # Without a setting for DEFAULT_NOTIFICATION_EMAIL_ADDRESS, we
        # can't calculate the email address to send RBdigital for a
        # patron.
        assert_raises_regexp(
            RemotePatronCreationFailedException,
            "Cannot create remote account for patron because library's default notification address is not set.",
            m, patron, auth
        )

        self._set_notification_address(patron.library)
        address = m(patron, auth)
        eq_("genericemail+rbdigital-%s@library.org" % auth,
            address)

    def test_patron_remote_identifier_lookup(self):
        # Test the method that tries to convert a patron identifier
        # (e.g. the one the patron uses to authenticate with their
        # library) to an internal RBdigital patron ID.
        m = self.api.patron_remote_identifier_lookup
        identifier = self._str

        # Test the case where RBdigital doesn't recognize the identifier
        # we're using.
        datastr, datadict = self.api.get_data(
            "response_patron_internal_id_not_found.json"
        )
        self.api.queue_response(status_code=200, content=datastr)
        rbdigital_patron_id = m(identifier)
        eq_(None, rbdigital_patron_id)

        # Test the case where RBdigital recognizes the identifier
        # we're using.
        self.queue_initial_patron_id_lookup()
        rbdigital_patron_id = m(identifier)
        eq_(939981, rbdigital_patron_id)

        # Test the case where RBdigital sends an error because it
        # doesn't like our input.
        datastr, datadict = self.api.get_data(
            "response_patron_internal_id_error.json"
        )
        self.api.queue_response(status_code=500, content=datastr)
        assert_raises_regexp(
            InvalidInputException, "patron_id:",
            m, identifier
        )

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

    def test_populate_all_catalog(self):
        # Test the method that retrieves the entire catalog from RBdigital
        # and mirrors it locally.

        datastr, datadict = self.get_data("response_catalog_all_sample.json")
        self.api.queue_response(status_code=200, content=datastr)
        result = self.api.populate_all_catalog()

        # populate_all_catalog returns two numbers, as required by
        # RBDigitalSyncMonitor.
        eq_((3, 3), result)

        # We created three presentation-ready works.
        works = sorted(
            self._db.query(Work).all(), key=lambda x: x.title
        )
        emperor, russian, tricks = works
        eq_("Emperor Mage: The Immortals", emperor.title)
        eq_("In-Flight Russian", russian.title)
        eq_("Tricks", tricks.title)

        eq_(["9781934180723", "9781400024018", "9781615730186"],
            [x.license_pools[0].identifier.identifier for x in works])

        for w in works:
            [pool] = w.license_pools
            # We know we own licenses for this book.
            eq_(1, pool.licenses_owned)

            # We _presume_ that this book is lendable. We may find out
            # differently the next time we run the availability
            # monitor.
            eq_(1, pool.licenses_available)

    def test_populate_delta(self):

        # A title we don't know about -- "Emperor Mage: The Immortals"
        # is about to be added to the collection.

        # This title ("Greatest: Muhammad Ali, The") is about to be
        # removed from the collection.
        ali, ignore = LicensePool.for_foreign_id(
            self._db, DataSource.RB_DIGITAL, Identifier.RB_DIGITAL_ID,
            "9780590543439", collection=self.collection
        )
        ali.licenses_owned = 10
        ali.licenses_available = 9
        ali.licenses_reserved = 2
        ali.patrons_in_hold_queue = 1

        # This title ("Tricks") is not mentioned in the delta, so it
        # will be left alone.
        tricks, ignore = LicensePool.for_foreign_id(
            self._db, DataSource.RB_DIGITAL, Identifier.RB_DIGITAL_ID,
            "9781615730186", collection=self.collection
        )
        tricks.licenses_owned = 10
        tricks.licenses_available = 5

        # Retrieving a delta requires first retrieving a list of dated
        # snapshots, then retrieving the changes between those dates.
        datastr, datadict = self.get_data("response_catalog_availability_dates_multi.json")
        self.api.queue_response(status_code=200, content=datastr)
        datastr, datadict = self.get_data("response_catalog_delta.json")
        self.api.queue_response(status_code=200, content=datastr)
        # RBDigitalAPI.populate_delta then retrieves a complete media entry
        # for the ISBN of each added item. This is not needed for removals.
        datastr, datadict = self.get_data("response_catalog_media_isbn.json")
        self.api.queue_response(status_code=200, content=datastr)
        result = self.api.populate_delta(
            today=datetime.datetime(2020,04,30)
        )

        # populate_delta returns two numbers, as required by
        # RBDigitalSyncMonitor.
        eq_((2, 2), result)

        # "Tricks" has not been modified.
        eq_(10, tricks.licenses_owned)
        eq_(5, tricks.licenses_available)

        # "Greatest: Muhammad Ali, The" is still known to the system,
        # but its circulation data has been updated to indicate the
        # fact that this collection has no licenses.
        eq_(0, ali.licenses_owned)
        eq_(0, ali.licenses_available)
        eq_(0, ali.licenses_reserved)
        eq_(0, ali.patrons_in_hold_queue)

        # "Emperor Mage: The Immortals" is now known to the system
        emperor, ignore = LicensePool.for_foreign_id(
            self._db, DataSource.RB_DIGITAL, Identifier.RB_DIGITAL_ID,
            "9781934180723", collection=self.collection
        )
        work = emperor.work
        eq_("Emperor Mage", work.title)
        eq_(True, work.presentation_ready)

        # However, we have not set availability information on this
        # title.  That will happen (for all titles) the next time
        # RBDigitalAPI.process_availability is called.
        eq_(0, emperor.licenses_owned)
        eq_(0, emperor.licenses_available)

    def test_populate_delta_remove_item_missing_metadata(self):
        item_media_str, item_media = self.get_data("response_catalog_media_isbn.json")

        _, add_remove_same_delta = self.get_data("response_catalog_delta.json")
        add_remove_same_delta["addedBooks"] = [
            {
                "id": 1301944,
                "isbn": item_media["isbn"],
                "mediaType": item_media["mediaType"]
            }
        ]
        add_remove_same_delta["booksAddedCount"] = 1
        add_remove_same_delta["removedBooks"] = add_remove_same_delta["addedBooks"]
        add_remove_same_delta["booksRemovedCount"] = add_remove_same_delta["booksAddedCount"]

        # ensure test conditions are valid
        eq_(item_media["isbn"], add_remove_same_delta["addedBooks"][0]["isbn"])
        eq_(item_media["isbn"], add_remove_same_delta["removedBooks"][0]["isbn"])
        eq_(1, len(add_remove_same_delta["removedBooks"]))

        add_remove_same_delta["addedBooks"] = add_remove_same_delta["removedBooks"]
        add_remove_same_delta["booksAddedCount"] = add_remove_same_delta["booksRemovedCount"]

        delta_no_remove_isbn = json.loads(json.dumps(add_remove_same_delta))
        _ = delta_no_remove_isbn["removedBooks"][0].pop("isbn")

        class GoodMetaRBDigitalAPI(MockRBDigitalAPI):
            def get_delta(self, *args, **kwargs):
                return add_remove_same_delta

        api = GoodMetaRBDigitalAPI(self._db, self.collection, base_path=self.base_path)
        api.queue_response(status_code=200, content=item_media_str)
        items_transmitted, items_updated = api.populate_delta()
        eq_(2, items_transmitted)
        eq_(2, items_updated)

        # Exercise RBDigitalAPI.populate_delta when attempting to
        # remove item with no metadata.
        class NoneMetaRBDigitalAPI(MockRBDigitalAPI):
            def get_delta(self, *args, **kwargs):
                return delta_no_remove_isbn

        api = NoneMetaRBDigitalAPI(self._db, self.collection, base_path=self.base_path)
        api.queue_response(status_code=200, content=item_media_str)
        items_transmitted, items_updated = api.populate_delta()
        eq_(2, items_transmitted)
        eq_(1, items_updated)

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

        def make_part_url(part):
            return "http://give-me-part/%s" % part

        found_fulfillment = self.api.fulfill(
            patron, None, pool, None, fulfill_part_url=make_part_url
        )
        assert isinstance(found_fulfillment, RBFulfillmentInfo)

        # Without making any further HTTP requests, we were able to get
        # a Readium Web Publication manifest for the loan.
        eq_(Representation.AUDIOBOOK_MANIFEST_MEDIA_TYPE,
            found_fulfillment.content_type)

        # A manifest is associated with the FulfillmentInfo.
        manifest = found_fulfillment.manifest

        # The Unicode representation of the manifest is used as the
        # content to be sent to the client.
        output = json.loads(found_fulfillment.content)
        eq_('http://readium.org/webpub/default.jsonld', output['@context'])
        eq_('http://bib.schema.org/Audiobook', output['metadata']['@type'])

        # Each item in the manifest's readingOrder has an alternate
        # link generated by calling make_part_url().
        #
        # This represents a reliable (but slower) way of obtaining an
        # MP3 file directly from the manifest, without having to know
        # how to process an RBdigital access document.
        for i, part in enumerate(manifest.readingOrder):
            [alternate] = part['alternates']
            eq_(make_part_url(i), alternate['href'])
            eq_("audio/mpeg", alternate['type'])

        # Now let's try fulfilling one of those parts.
        #
        # We're going to make two requests this time -- one to get the
        # patron's current loans and one to get the RBdigital access
        # document.

        for filename in (
            "response_patron_checkouts_with_audiobook.json",
            "audiobook_chapter_access_document.json"
        ):
            datastr, datadict = self.api.get_data(filename)
            self.api.queue_response(status_code=200, content=datastr)

        # We end up with a FulfillmentInfo that includes the link
        # mentioned in audiobook_chapter_access_document.json.
        chapter = self.api.fulfill(patron, None, pool, None, part=3)
        assert isinstance(chapter, FulfillmentInfo)
        eq_("http://book/chapter1.mp3", chapter.content_link)
        eq_("audio/mpeg", chapter.content_type)

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

    def test_run_once(self):
        # run_once() calls process_availability twice, once for
        # ebooks and once for audiobooks.

        class Mock(RBDigitalCirculationMonitor):
            process_availability_calls = []

            def process_availability(self, media_type):
                self.process_availability_calls.append(media_type)
                # Pretend we processed three titles.
                return 3

        monitor = Mock(
            self._db, self.collection, api_class=MockRBDigitalAPI,
        )
        timestamp = monitor.timestamp().to_data()
        progress = monitor.run_once(timestamp)
        eq_(['eBook', 'eAudio'], monitor.process_availability_calls)

        # The TimestampData returned by run_once() describes its
        # achievements.
        eq_("Ebooks processed: 3. Audiobooks processed: 3.",
            progress.achievements)

        # The TimestampData does not include any timing information
        # -- that will be applied by run().
        eq_(None, progress.start)
        eq_(None, progress.finish)

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

class TestRBFulfillmentInfo(RBDigitalAPITest):


    def test_fulfill_part(self):
        get_data = self.api.get_data

        ignore, [book] = get_data(
            "response_patron_checkouts_with_audiobook.json"
        )
        manifest = AudiobookManifest(book)

        part_files = book['files']

        class MockFulfillmentRequestTracker():
            def fulfillment_request(self, url):
                self.fulfillment_request_last_called_with = url
                data, ignore = get_data(
                    "audiobook_chapter_access_document.json"
                )
                return MockRequestsResponse(200, {}, data)

        # We have an RBFulfillmentInfo object and the underlying
        # AudiobookManifest has already been created.
        fulfill_part_url = object()
        request_tracker = MockFulfillmentRequestTracker()
        info = RBFulfillmentInfo(
            fulfill_part_url, request_tracker.fulfillment_request,
            self.api, "data source",
            "identifier type", "identifier", "key"
        )

        # We don't be using fulfill_part_url, since it's only used
        # when we're fulfilling the audiobook as a whole, but let's
        # check to make sure it was set correctly.
        eq_(fulfill_part_url, info.fulfill_part_url)

        # Prepopulate the manifest so that we don't go over the
        # network trying to get it.
        info._fetched = True
        info.manifest = manifest

        # Now we're going to try various situations where partial
        # fulfillment is impossible. Each one will raise
        # CannotPartiallyFulfill.
        m = info.fulfill_part

        info._content_type = "not/an/audiobook"
        assert_raises_regexp(
            CannotPartiallyFulfill,
            "This work does not support partial fulfillment."
        )

        info._content_type = Representation.AUDIOBOOK_MANIFEST_MEDIA_TYPE
        assert_raises_regexp(
            CannotPartiallyFulfill,
            '"not a number" is not a valid part number', m, "not a number"
        )

        assert_raises_regexp(
            CannotPartiallyFulfill, 'Could not locate part number -1', m, -1
        )

        # There are 21 parts in this audiobook, numbered from 0 to 20.
        assert_raises_regexp(
            CannotPartiallyFulfill, 'Could not locate part number 21', m,
            len(manifest.readingOrder)
        )

        # Finally, let's fulfill a part that does exist.
        part_index = 10
        fulfillment = m(part_index)
        assert isinstance(fulfillment, FulfillmentInfo)
        eq_(part_files[part_index]["downloadUrl"], request_tracker.fulfillment_request_last_called_with)
        eq_("http://book/chapter1.mp3", fulfillment.content_link)
        eq_("audio/mpeg", fulfillment.content_type)


class TestAudiobookManifest(RBDigitalAPITest):

    def test_constructor(self):
        """A reasonable RBdigital manifest becomes a reasonable
        AudiobookManifest object.
        """

        def fulfill_part_url(part):
            return "http://fulfill-part/%s" % part

        ignore, [book] = self.api.get_data(
            "response_patron_checkouts_with_audiobook.json"
        )
        manifest = AudiobookManifest(book, fulfill_part_url)

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

        # fulfill_part_url was used to create an alternate link
        # that goes directly (from the client's perspective) to
        # an MP3 file.
        [alternate] = first['alternates']
        eq_("http://fulfill-part/0", alternate['href'])
        eq_("audio/mpeg", alternate['type'])

        # An alternate link and a cover link were imported.
        alternate, cover = manifest.links
        eq_("alternate", alternate['rel'])
        eq_("https://download/full-book.zip", alternate['href'])
        eq_("application/zip", alternate['type'])

        eq_("cover", cover['rel'])
        assert "image_512x512" in cover['href']
        eq_("image/png", cover['type'])

        # If we don't pass in a fulfill_part_url function, then
        # the parts do not have any 'alternate' representations.
        manifest = AudiobookManifest(book)
        first = manifest.readingOrder[0]
        assert 'alternates' not in first

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

        weight = Classification.TRUSTED_DISTRIBUTOR_WEIGHT
        eq_([(None, u"FICTION / Humorous / General", Subject.BISAC, weight),

            (u'adult', None, Classifier.RBDIGITAL_AUDIENCE, weight),

            (u'humorous-fiction', None, Subject.RBDIGITAL, weight),
            (u'mystery', None, Subject.RBDIGITAL, weight),
            (u'womens-fiction', None, Subject.RBDIGITAL, weight)
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

class TestRBDigitalSyncMonitor(RBDigitalAPITest):
    """Test the superclass of most of the RBDigital monitors."""

    def setup(self):
        super(TestRBDigitalSyncMonitor, self).setup()
        self.base_path = os.path.split(__file__)[0]
        self.collection = MockRBDigitalAPI.mock_collection(self._db)

    def test_run_once(self):
        # Calling run_once calls invoke(), and invoke() is
        # expected to return two numbers.
        class Mock(RBDigitalSyncMonitor):
            SERVICE_NAME = "A service"

            def invoke(self):
                self.invoked = True
                return 10, 5

        monitor = Mock(
            self._db, self.collection, api_class=MockRBDigitalAPI,
        )
        progress = monitor.run_once(monitor.timestamp().to_data())

        # invoke() was called.
        eq_(True, monitor.invoked)

        # The TimestampData returned by run_once() describes its
        # achievements.
        eq_(
            "Records received from vendor: 10. Records written to database: 5",
            progress.achievements
        )

        # The TimestampData does not include any timing information --
        # that will be applied by run().
        eq_(None, progress.start)
        eq_(None, progress.finish)


class TestRBDigitalImportMonitor(RBDigitalAPITest):

    def test_invoke(self):
        class MockAPI(RBDigitalAPI):
            def __init__(self):
                self.called = False

            def populate_all_catalog(self):
                self.called = True
        api = MockAPI()

        monitor = RBDigitalImportMonitor(
            self._db, self.collection, api_class=api
        )
        timestamp = monitor.timestamp()
        eq_(None, timestamp.counter)
        monitor.invoke()

        # This monitor is for performing the initial import, and it
        # can only be invoked once.
        eq_(True, api.called)
        eq_(1, timestamp.counter)

        # Invoking the monitor a second time will do nothing.
        api.called = False
        result = monitor.invoke()
        eq_((0, 0), result)
        eq_(False, api.called)


class TestRBDigitalDeltaMonitor(RBDigitalAPITest):

    def test_invoke(self):
        # This monitor calls RBDigitalAPI.populate_delta() when
        # invoked.
        class MockAPI(RBDigitalAPI):
            def __init__(self):
                self.called = False

            def populate_delta(self):
                self.called = True
        api = MockAPI()
        monitor = RBDigitalDeltaMonitor(
            self._db, self.collection, api_class=api
        )
        monitor.invoke()
        eq_(True, api.called)

