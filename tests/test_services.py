from . import DatabaseTest
from nose.tools import set_trace, eq_
from api.services import ServiceStatus

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
        service_status = ServiceStatus(self._db)
        assert service_status.auth != None
