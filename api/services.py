import time
import re
import logging
from nose.tools import set_trace

from core.scripts import (
    Script,
    IdentifierInputScript,
)
from core.util.problem_detail import ProblemDetail

from config import Configuration
from authenticator import Authenticator
from overdrive import OverdriveAPI
from threem import ThreeMAPI
from axis import Axis360API
from enki import EnkiAPI
from circulation import CirculationAPI

class ServiceStatus(object):
    """Checks response times for third-party services."""

    log = logging.getLogger('Third-Party Service Status')

    SUCCESS_MSG = re.compile('^SUCCESS: ([0-9]+.[0-9]+)sec')

    def __init__(self, _db, auth=None, overdrive=None, threem=None, axis=None, enki=None):
        self._db = _db
        self.auth = auth or Authenticator.from_config(self._db)
        self.overdrive = overdrive or OverdriveAPI.from_environment(self._db)
        self.threem = threem or ThreeMAPI.from_environment(self._db)
        self.axis = axis or Axis360API.from_environment(self._db)
        self.enki = enki or EnkiAPI.from_environment(self._db)

    def loans_status(self, response=False):
        """Checks the length of request times for patron activity.

        Returns a dict if response is set to true.
        """
        status = dict()
        if not self.auth.basic_auth_provider:
            self.log.error(
                "Basic auth not configured, cannot perform timing tests."
            )
            return status

        patron_info = []
        def do_patron():
            patron, password = self.auth.basic_auth_provider.testing_patron(
                self._db
            )
            # Stick it in a list so we can use it once we leave the function.
            patron_info.append((patron, password))

        # Look up the test patron and verify their credentials. If
        # this doesn't work, nothing else will work, either.
        service = 'Patron authentication'
        self._add_timing(status, service, do_patron)
        success = False
        patron = password = None
        error = "Could not create patron with configured credentials."
        if patron_info:
            [(patron, password)] = patron_info
            if patron:
                if isinstance(patron, ProblemDetail):
                    response = patron.response
                    error = response[0] # The JSON representation of the ProblemDetail
                else:
                    success = True
                    error = None
        if not success:
            self.log.error(error)
            status[service] = error
            return status
        for api in [self.overdrive, self.threem, self.axis, self.enki]:
            if not api:
                continue
            name = api.source.name
            service = "%s patron account" % name
            def do_patron_activity(api, name, patron):
                return api.patron_activity(patron, password)

            self._add_timing(
                status, service, do_patron_activity,
                api, name, patron
            )

        if response:
            return status
        self.log_status(status)

    def checkout_status(self, identifier):
        """Times request rates related to checking out a book.

        Intended to be run with an identifier without license restrictions.
        """
        status = dict()
        patron, password = self.get_patron()
        api = CirculationAPI(
            self._db, overdrive=self.overdrive, threem=self.threem,
            axis=self.axis, enki=self.enki
        )

        license_pool = identifier.licensed_through
        if not license_pool:
            raise ValueError("No license pool for this identifier")
        delivery_mechanism = None
        if license_pool.delivery_mechanisms:
            delivery_mechanism = license_pool.delivery_mechanisms[0]
        loans = []

        service = "Checkout IDENTIFIER: %r" % identifier
        def do_checkout():
            loan, hold, is_new = api.borrow(
                patron, password, license_pool, delivery_mechanism,
                Configuration.default_notification_email_address()
            )
            loans.append(loan)
        self._add_timing(status, service, do_checkout)

        # There's no reason to continue checking without a loan.
        if not loans:
            self.log.error("No loan created during checkout")
            self.log_status(status)
            return

        service = "Fulfill IDENTIFIER: %r" % identifier
        def do_fulfillment():
            api.fulfill(
                patron, password, license_pool, delivery_mechanism
            )
        self._add_timing(status, service, do_fulfillment)

        service = "Checkin IDENTIFIER: %r" % identifier
        def do_checkin():
            api.revoke_loan(patron, password, license_pool)
        self._add_timing(status, service, do_checkin)

        self.log_status(status)

    def _add_timing(self, status, service, service_action, *args):
        try:
            start_time = time.time()
            service_action(*args)
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

    @classmethod
    def select_log_level(self, message):
        if message.startswith("FAILURE"):
            return self.log.error

        time = float(self.SUCCESS_MSG.match(message).groups()[0])
        if time > 10:
            return self.log.error
        elif time > 3:
            return self.log.warn
        return self.log.info


class PatronActivityTimingScript(Script):
    """Log third-party service loan response times."""

    def run(self):
        ServiceStatus(self._db).loans_status()


class BorrowTimingScript(IdentifierInputScript):
    """Log third-party service checkout, fulfillment, and checkin times."""

    def run(self):
        identifiers = self.parse_identifiers()
        service_status = ServiceStatus(self._db)
        for identifier in identifiers:
            service_status.checkout_status(identifier)
