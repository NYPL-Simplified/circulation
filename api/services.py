import time
import re
import logging
from nose.tools import set_trace

from core.monitor import Monitor
from core.scripts import IdentifierInputScript

from config import Configuration
from authenticator import Authenticator
from overdrive import OverdriveAPI
from threem import ThreeMAPI
from axis import Axis360API
from circulation import CirculationAPI

class ServiceStatus(object):
    """Checks response times for third-party services."""

    log = logging.getLogger('Third-Party Service Status')

    SUCCESS_MSG = re.compile('^SUCCESS: ([0-9]+.[0-9]+)sec')

    def __init__(self, _db):
        self._db = _db

    def loans_status(self, response=False):
        conf = Configuration.authentication_policy()
        username = conf[Configuration.AUTHENTICATION_TEST_USERNAME]
        password = conf[Configuration.AUTHENTICATION_TEST_PASSWORD]

        status = dict()
        patrons = []

        def do_patron():
            auth = Authenticator.initialize(self._db)
            patron = auth.authenticated_patron(self._db, username, password)
            patrons.append(patron)
            if patron:
                return patron
            else:
                raise ValueError("Could not authenticate test patron!")
        self._add_timing(status, 'Patron authentication', do_patron)

        if not patrons:
            return status
        patron = patrons[0]

        def do_overdrive():
            overdrive = OverdriveAPI.from_environment(self._db)
            if not overdrive:
                raise ValueError("Overdrive not configured")
            return overdrive.patron_activity(patron, password)
        self._add_timing(status, 'Overdrive patron account', do_overdrive)

        def do_threem():
            threem = ThreeMAPI.from_environment(self._db)
            if not threem:
                raise ValueError("3M not configured")
            return threem.patron_activity(patron, password)
        self._add_timing(status, '3M patron account', do_threem)

        def do_axis():
            axis = Axis360API.from_environment(self._db)
            if not axis:
                raise ValueError("Axis not configured")
            return axis.patron_activity(patron, password)
        self._add_timing(status, 'Axis patron account', do_axis)

        if response:
            return status
        self.log_status(status)

    def checkout_status(self, identifier):
        pass

    def _add_timing(self, status, service, service_action):
        try:
            start_time = time.time()
            service_action()
            end_time = time.time()
            result = end_time-start_time
        except Exception, e:
            result = e
        if isinstance(result, float):
            status_message = "SUCCESS: %.2fsec" % result
        else:
            status_message = "FAILURE: %s" % result
        status[service] = status_message

    def log_status(self, status):
        for service, message in status.items():
            log_level = self.select_log_level(message)
            log_level("%s: %s", service, message)

    def select_log_level(self, message):
        if message.startswith("FAILURE"):
            return self.log.error

        time = float(self.SUCCESS_MSG.match(message).groups()[0])

        if time > 10:
            return self.log.error
        elif time > 3:
            return self.log.warn
        return self.log.info


class ServiceLoanStatusMonitor(Monitor):
    """Monitor and log third-party service loan response times."""

    def __init__(self, _db):
        super(ServiceLoanStatusMonitor, self).__init__(
            _db, "Third-Party Service Status Monitor"
        )

    def run_once(self, start, cutoff):
        ServiceStatus(self._db).loans_status()


