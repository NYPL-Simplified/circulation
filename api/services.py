import time
import re
from nose.tools import set_trace

from core.monitor import Monitor

from config import Configuration
from authenticator import Authenticator
from overdrive import OverdriveAPI
from threem import ThreeMAPI
from axis import Axis360API

class ServiceStatus(object):
    """Checks response times for third-party services."""

    def __init__(self, _db):
        self._db = _db

    def loans_status(self):
        conf = Configuration.authentication_policy()
        username = conf[Configuration.AUTHENTICATION_TEST_USERNAME]
        password = conf[Configuration.AUTHENTICATION_TEST_PASSWORD]

        timings = dict()

        patrons = []
        def _add_timing(k, x):
            try:
                a = time.time()
                x()
                b = time.time()
                result = b-a
            except Exception, e:
                result = e
            if isinstance(result, float):
                timing = "SUCCESS: %.2fsec" % result
            else:
                timing = "FAILURE: %s" % result
            timings[k] = timing

        def do_patron():
            auth = Authenticator.initialize(self._db)
            patron = auth.authenticated_patron(self._db, username, password)
            patrons.append(patron)
            if patron:
                return patron
            else:
                raise ValueError("Could not authenticate test patron!")
        _add_timing('Patron authentication', do_patron)

        if not patrons:
            return timings
        patron = patrons[0]

        def do_overdrive():
            overdrive = OverdriveAPI.from_environment(self._db)
            if not overdrive:
                raise ValueError("Overdrive not configured")
            return overdrive.patron_activity(patron, password)
        _add_timing('Overdrive patron account', do_overdrive)

        def do_threem():
            threem = ThreeMAPI.from_environment(self._db)
            if not threem:
                raise ValueError("3M not configured")
            return threem.patron_activity(patron, password)
        _add_timing('3M patron account', do_threem)

        def do_axis():
            axis = Axis360API.from_environment(self._db)
            if not axis:
                raise ValueError("Axis not configured")
            return axis.patron_activity(patron, password)
        _add_timing('Axis patron account', do_axis)

        return timings


class ServiceStatusMonitor(Monitor):
    """Monitor and log third-party service response times."""

    SUCCESS_MSG = re.compile('^SUCCESS: ([0-9]+.[0-9]+)sec')

    def __init__(self, _db):
        super(ServiceStatusMonitor, self).__init__(
            _db, "Third-Party Service Status Monitor"
        )

    def run_once(self, start, cutoff):
        status = ServiceStatus(self._db).loans_status()
        logger = self.select_log_level(status)
        logger(status)

    def select_log_level(self, status):
        messages = [msg for api, msg in status.items()]

        failures = [msg.startswith("FAILURE") for msg in messages]
        if any(failures):
            return self.log.error

        request_times = [float(self.SUCCESS_MSG.match(msg).groups()[0])
                         for msg in messages]

        if any(time > 10 for time in request_times):
            return self.log.error
        elif any(time > 3 for time in request_times):
            return self.log.warn
        return self.log.info
