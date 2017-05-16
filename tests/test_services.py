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
            service_status = ServiceStatus(self._db)
            assert service_status.auth != None
            assert service_status.auth.basic_auth_provider != None

    def test_loans_status(self):
        
        provider = MockAuthenticationProvider(
            patrons={"user": "pass"},
            test_username="user",
            test_password="pass",
        )
        library = Library.instance(self._db)
        auth = Authenticator(library, provider)

        class MockPatronActivity(object):
            def __init__(self, _db, data_source_name):
                self.source = DataSource.lookup(_db, data_source_name)
                self.succeed = True
                
            def patron_activity(self, patron, pin):
                if self.succeed:
                    # Simulate a patron with nothing going on.
                    return
                else:
                    raise ValueError("Doomed to fail!")
        
        overdrive = MockPatronActivity(self._db, DataSource.OVERDRIVE)
        threem = MockPatronActivity(self._db, DataSource.BIBLIOTHECA)
        axis = MockPatronActivity(self._db, DataSource.AXIS_360)

        # Test a scenario where all providers succeed.
        status = ServiceStatus(self._db, auth, overdrive, threem, axis)
        response = status.loans_status(response=True)
        for value in response.values():
            assert value.startswith('SUCCESS')

        # Simulate a failure in one of the providers.
        overdrive.succeed = False
        response = status.loans_status(response=True)
        eq_("FAILURE: Doomed to fail!", response['Overdrive patron account'])

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
