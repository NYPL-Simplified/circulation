from . import DatabaseTest
from nose.tools import set_trace, eq_
from api.services import ServiceStatusMonitor

class TestServiceStatusMonitor(DatabaseTest):

    def test_select_log_level(self):
        monitor = ServiceStatusMonitor(self._db)
        SUCCESS = "SUCCESS: %fsec"

        # A request failure results in an error log
        status = { 'api2': SUCCESS%0.00, 'api1': 'FAILURE: It hurts.' }
        eq_('error', monitor.select_log_level(status).__name__)

        # Request times above 10 secs also results in an error log
        status['api1'] = SUCCESS%24.03
        eq_('error', monitor.select_log_level(status).__name__)

        # Request times between 3 and 10 secs results in a warn log
        status['api1'] = SUCCESS%7.82
        eq_('warning', monitor.select_log_level(status).__name__)
        status['api1'] = SUCCESS%3.0001
        eq_('warning', monitor.select_log_level(status).__name__)

        # Request times below 3 secs are set as info
        status['api1'] = SUCCESS%2.32
        eq_('info', monitor.select_log_level(status).__name__)
