from nose.tools import set_trace
from datetime import datetime, timedelta

from sqlalchemy.orm import contains_eager

from lxml import etree
from core.enki import (
    EnkiAPI as BaseEnkiAPI,
    MockEnkiAPI as BaseMockEnkiAPI,
    EnkiParser,
    BibliographicParser,
    EnkiBibliographicCoverageProvider
)

from core.metadata_layer import (
    CirculationData,
    ReplacementPolicy,
)

from core.monitor import (
    Monitor,
    IdentifierSweepMonitor,
)

from core.opds_import import (
    SimplifiedOPDSLookup,
)

from core.model import (
    CirculationEvent,
    get_one_or_create,
    Contributor,
    DataSource,
    DeliveryMechanism,
    Edition,
    Identifier,
    LicensePool,
    Representation,
    Subject,
)

from core.coverage import (
    BibliographicCoverageProvider,
    CoverageFailure,
)

from authenticator import Authenticator
from config import Configuration
from circulation import (
    LoanInfo,
    FulfillmentInfo,
    HoldInfo,
    BaseCirculationAPI
)
from circulation_exceptions import *

#TODO: Remove unnecessary imports (once the classes are more or less complete)

class EnkiAPI(BaseEnkiAPI, BaseCirculationAPI):
    #TODO
    SET_DELIVERY_MECHANISM_AT = BaseCirculationAPI.BORROW_STEP
    SERVICE_NAME = "Enki"

class EnkiCirculationMonitor(Monitor):
    """Maintain LicensePools for Enki titles.
    """

    VERY_LONG_AGO = datetime(1970, 1, 1)
    FIVE_MINUTES = timedelta(minutes=5)

    def __init__(self, _db, name="Enki Circulation Monitor",
                 interval_seconds=60, batch_size=50, api=None):
        print "We made it to init in EnkiCircMonitor"
	super(EnkiCirculationMonitor, self).__init__(
            _db, name, interval_seconds=interval_seconds,
            default_start_time = self.VERY_LONG_AGO
        )
        self.batch_size = batch_size
        metadata_wrangler_url = Configuration.integration_url(
                Configuration.METADATA_WRANGLER_INTEGRATION
        )
        if metadata_wrangler_url:
            self.metadata_wrangler = SimplifiedOPDSLookup(metadata_wrangler_url)
        else:
            # This should only happen during a test.
            self.metadata_wrangler = None
        self.api = api or EnkiAPI.from_environment(self._db)
	print "api is %s" % api
	print "from_environment is %s" % EnkiAPI.from_environment(self._db)
	print "selfapi is %s" % self.api
        self.bibliographic_coverage_provider = (
            EnkiBibliographicCoverageProvider(self._db, enki_api=api)
        )

    def run(self):
	print "Chris was here at run(self) calling super(CircMon)"
        super(EnkiCirculationMonitor, self).run()

    def run_once(self, start, cutoff):
        # Give us five minutes of overlap because it's very important
        # we don't miss anything.
        since = start-self.FIVE_MINUTES
	print "Here is where we try and get availability"
        availability = self.api.availability(since=since)
        print "The response is: %s" % availability.content
	status_code = availability.status_code
	print "The status code is %s" % status_code 
        content = availability.content
        count = 0
        for bibliographic, circulation in BibliographicParser().process_all(
                content):
            self.process_book(bibliographic, circulation)
            count += 1
            if count % self.batch_size == 0:
                self._db.commit()

    def process_book(self, bibliographic, availability):

        license_pool, new_license_pool = availability.license_pool(self._db)
        edition, new_edition = bibliographic.edition(self._db)
        license_pool.edition = edition
        policy = ReplacementPolicy(
            identifiers=False,
            subjects=True,
            contributions=True,
            formats=True,
        )
        availability.apply(
            pool=license_pool,
            replace=policy,
        )
        if new_edition:
            bibliographic.apply(edition, replace=policy)

        if new_license_pool or new_edition:
            # At this point we have done work equivalent to that done by
            # the EnkiBibliographicCoverageProvider. Register that the
            # work has been done so we don't have to do it again.
            identifier = edition.primary_identifier
            self.bibliographic_coverage_provider.handle_success(identifier)
            self.bibliographic_coverage_provider.add_coverage_record_for(
                identifier
            )

        return edition, license_pool

class MockEnkiAPI(BaseMockEnkiAPI, EnkiAPI):
    #TODO
    pass

class EnkiCollectionReaper(IdentifierSweepMonitor):
    #TODO
    pass

class ResponseParser(EnkiParser):
    id_type = Identifier.ENKI_ID

    SERVICE_NAME = "Enki"

    def raise_exception_on_error(self, e, ns, custom_error_classes={}):
        #TODO: Handle failure response here

        """code = self._xpath1(e, '//axis:status/axis:code', ns)
        message = self._xpath1(e, '//axis:status/axis:statusMessage', ns)
        if message is None:
            message = etree.tostring(e)
        else:
            message = message.text

        if code is None:
            # Something is so wrong that we don't know what to do.
            raise RemoteInitiatedServerError(message, self.SERVICE_NAME)
        code = code.text
        try:
            code = int(code)
        except ValueError:
            # Non-numeric code? Inconcievable!
            raise RemoteInitiatedServerError(
                "Invalid response code from Axis 360: %s" % code,
                self.SERVICE_NAME
            )

        for d in custom_error_classes, self.code_to_exception:
            if (code, message) in d:
                raise d[(code, message)]
            elif code in d:
                # Something went wrong and we know how to turn it into a
                # specific exception.
                cls = d[code]
                if cls is RemoteInitiatedServerError:
                    e = cls(message, self.SERVICE_NAME)
                else:
                    e = cls(message)
                raise e
        return code, message"""

class CheckoutResponseParser(ResponseParser):
    #TODO??
    pass

class HoldResponseParser(ResponseParser):
    #TODO??
    pass

class HoldReleaseResponseParser(ResponseParser):
    #TODO??
    pass

class AvailabilityResponseParser(ResponseParser):
    #TODO??
    pass
