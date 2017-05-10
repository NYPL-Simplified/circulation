import json

from . import (
    DatabaseTest,
    sample_data
)
from nose.tools import set_trace, eq_
from api.problem_details import EXPIRED_CREDENTIALS
from api.services import ServiceStatus
from api.config import (
    Configuration,
    temp_config,
)

from api.authenticator import (
    Authenticator
)
from api.mock_authentication import (
    MockAuthenticationProvider
)

from core.model import (
    Collection,
    DataSource,
    Library,
)

class TestServiceStatusMonitor(DatabaseTest):

    def test_select_log_level(self):
        SUCCESS = "SUCCESS: %fsec"
        def level_name(message):
            return ServiceStatus.select_log_level(message).__name__

        # A request failure results in an error log
        status_message = 'FAILURE: It hurts.'
        eq_('error', level_name(status_message))

        # Request times above 10 secs also results in an error log
        status_message = SUCCESS%24.03
        eq_('error', level_name(status_message))

        # Request times between 3 and 10 secs results in a warn log
        status_message = SUCCESS%7.82
        eq_('warning', level_name(status_message))
        status_message = SUCCESS%3.0001
        eq_('warning', level_name(status_message))

        # Request times below 3 secs are set as info
        status_message = SUCCESS%2.32
        eq_('info', level_name(status_message))

    def test_init(self):
        # Test that ServiceStatus can create an Authenticator.
        with temp_config() as config:
            config[Configuration.POLICIES] = {
                Configuration.AUTHENTICATION_POLICY: {
                    "providers": [
                        {"module": 'api.mock_authentication'}
                    ]
                }
            }
            service_status = ServiceStatus(self._default_library)
            assert service_status.auth != None
            assert service_status.auth.basic_auth_provider != None

    @property
    def mock_auth(self):
        library = self._default_library
        provider = MockAuthenticationProvider(
            library,
            patrons={"user": "pass"},
            test_username="user",
            test_password="pass",
        )
        return Authenticator(library, provider)

    def test_test_patron(self):
        """Verify that test_patron() returns credentials determined
        by the basic auth provider.
        """
        auth = self.mock_auth
        provider = auth.basic_auth_provider
        status = ServiceStatus(self._default_library, auth=auth)
        patron, password = status.test_patron
        eq_(provider.test_username, patron.authorization_identifier)
        eq_(provider.test_password, password)
        
    def test_loans_status(self):
        auth = self.mock_auth

        class MockPatronActivitySuccess(object):
            def __init__(self, *args, **kwargs):
                pass
            
            def patron_activity(self, patron, pin):
                "Simulate a patron with nothing going on."
                return

        class MockPatronActivityFailure(object):                
            def __init__(self, *args, **kwargs):
                pass

            def patron_activity(self, patron, pin):
                "Simulate an integration failure."
                raise ValueError("Doomed to fail!")        
                
        # Create a variety of Collections for this library.
        overdrive_collection = self._collection(protocol=Collection.OVERDRIVE)
        axis_collection = self._collection(protocol=Collection.AXIS_360)
        self._default_library.collections.append(overdrive_collection)
        self._default_library.collections.append(axis_collection)

        # Test a scenario where we get information for every
        # relevant collection in the library.
        everything_succeeds = {
            Collection.OVERDRIVE : MockPatronActivitySuccess,
            Collection.AXIS_360 : MockPatronActivitySuccess
        }
        
        status = ServiceStatus(
            self._default_library, auth=auth, api_map=everything_succeeds
        )
        response = status.loans_status(response=True)
        for value in response.values():
            assert value.startswith('SUCCESS')

        # Simulate a failure in one of the providers.
        overdrive_fails = {
            Collection.OVERDRIVE : MockPatronActivityFailure,
            Collection.AXIS_360 : MockPatronActivitySuccess
        }
        status = ServiceStatus(
            self._default_library, auth=auth, api_map=overdrive_fails
        )
        response = status.loans_status(response=True)
        key = '%s patron account (Overdrive)' % overdrive_collection.name
        eq_("FAILURE: Doomed to fail!", response[key])

        # Simulate failures on the ILS level.
        def test_with_broken_basic_auth_provider(value):
            class BrokenBasicAuthProvider(object):
                def testing_patron(self, _db):
                    return value
        
            auth.basic_auth_provider = BrokenBasicAuthProvider()
            response = status.loans_status(response=True)
            eq_({'Patron authentication':
                 'Could not create patron with configured credentials.'},
                response)

        # Test patron can't authenticate
        test_with_broken_basic_auth_provider(
            (None, "password that didn't work")
        )

        # Auth provider is just totally broken.
        test_with_broken_basic_auth_provider(None)

        # If the auth process returns a problem detail, the problem
        # detail is used as the basis for the error message.
        class ExpiredPatronProvider(object):
            def testing_patron(self, _db):
                return EXPIRED_CREDENTIALS, None

        auth.basic_auth_provider = ExpiredPatronProvider()
        response = status.loans_status(response=True)
        eq_({'Patron authentication': EXPIRED_CREDENTIALS.response[0]},
            response
        )

    def test_checkout_status(self):

        # Create a Collection to test.
        overdrive_collection = self._collection(protocol=Collection.OVERDRIVE)
        edition, lp = self._edition(
            with_license_pool=True, collection=overdrive_collection
        )
        self._default_library.collections.append(overdrive_collection)

        # Test a scenario where we get information for every
        # relevant collection in the library.
        class CheckoutSuccess(object):
            def __init__(self, *args, **kwargs):
                self.borrowed = False
                self.fulfilled = False
                self.revoked = False
            
            def borrow(self, patron, password, license_pool, *args, **kwargs):
                "Simulate a successful borrow."
                self.borrowed = True
                return object(), None, True
                
            def fulfill(self, *args, **kwargs):
                "Simulate a successful fulfillment."
                self.fulfilled = True
                
            def revoke_loan(self, *args, **kwargs):
                "Simulate a successful loan revocation."
                self.revoked = True
                
        everything_succeeds = {Collection.OVERDRIVE : CheckoutSuccess}

        auth = self.mock_auth
        status = ServiceStatus(
            self._default_library, auth=auth, api_map=everything_succeeds
        )
        with temp_config() as config:
            config[Configuration.DEFAULT_NOTIFICATION_EMAIL_ADDRESS] = "a@b"
            response = status.checkout_status(lp.identifier)

        # The ServiceStatus object was able to run its test.
        for value in response.values():
            assert value.startswith('SUCCESS')

        # The mock Overdrive API had all its methods called.
        api = status.circulation.api_for_collection[overdrive_collection]
        eq_(True, api.borrowed)
        eq_(True, api.fulfilled)
        eq_(True, api.revoked)

        # Now try some failure conditions.

        # First: the 'borrow' operation succeeds on an API level but
        # it doesn't create a loan.
        class NoLoanCreated(CheckoutSuccess):
            def borrow(self, patron, password, license_pool, *args, **kwargs):
                "Oops! We put the book on hold instead of borrowing it."
                return None, object(), True
        no_loan_created = {Collection.OVERDRIVE : NoLoanCreated}
        status = ServiceStatus(
            self._default_library, auth=auth, api_map=no_loan_created
        )            
        with temp_config() as config:
            config[Configuration.DEFAULT_NOTIFICATION_EMAIL_ADDRESS] = "a@b"
            response = status.checkout_status(lp.identifier)
        assert 'FAILURE: No loan created during checkout' in response.values()

        # Next: The 'revoke' operation fails on an API level.
        class RevokeFail(CheckoutSuccess):
            def revoke_loan(self, *args, **kwargs):
                "Simulate an error during loan revocation."
                raise Exception("Doomed to fail!")
        revoke_fail = {Collection.OVERDRIVE : RevokeFail}
        status = ServiceStatus(
            self._default_library, auth=auth, api_map=revoke_fail
        )            
        with temp_config() as config:
            config[Configuration.DEFAULT_NOTIFICATION_EMAIL_ADDRESS] = "a@b"
            response = status.checkout_status(lp.identifier)
        assert 'FAILURE: Doomed to fail!' in response.values()

        # But at least we got through the borrow and fulfill steps.
        api = status.circulation.api_for_collection[overdrive_collection]
        eq_(True, api.borrowed)
        eq_(True, api.fulfilled)
        eq_(False, api.revoked)
