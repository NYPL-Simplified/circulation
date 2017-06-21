from nose.tools import set_trace
import datetime

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
    #copied/moved from core/enki.py since the Enki API probably doesn't need to use core
    PRODUCTION_BASE_URL = "http://enkilibrary.org/API/"
    availability_endpoint = "ListAPI"
    item_endpoint = "ItemAPI"
    lib = 1

    SET_DELIVERY_MECHANISM_AT = BaseCirculationAPI.BORROW_STEP
    SERVICE_NAME = "Enki"

    def reaper_request(self, identifier):
        print "Checking availability for " + str(identifier)
        url = str(self.base_url) + str(self.item_endpoint)
        args = dict()
        args['method'] = "getItem"
        args['recordid'] = identifier
        args['size'] = "small"
        args['lib'] = self.lib
        response = self.request(url, method='get', params=args)
        if not(response.content.startswith("{\"result\":{\"id\":\"")):
            response = None
            print "This book is no longer available."
        return response

class EnkiCirculationMonitor(Monitor):
    """Maintain LicensePools for Enki titles.
    """

    VERY_LONG_AGO = datetime.datetime(1970, 1, 1)
    FIVE_MINUTES = datetime.timedelta(minutes=5)

    def __init__(self, _db, name="Enki Circulation Monitor",
                 interval_seconds=60, batch_size=50, api=None):
	super(EnkiCirculationMonitor, self).__init__(
            _db, name, interval_seconds=interval_seconds,
            default_start_time = self.VERY_LONG_AGO
        )
        self.batch_size = batch_size
        #line 83-90 should be removable during refactoring
        metadata_wrangler_url = Configuration.integration_url(
                Configuration.METADATA_WRANGLER_INTEGRATION
        )
        if metadata_wrangler_url:
            self.metadata_wrangler = SimplifiedOPDSLookup(metadata_wrangler_url)
        else:
            # This should only happen during a test.
            self.metadata_wrangler = None
        self.api = api or EnkiAPI.from_environment(self._db)
        self.bibliographic_coverage_provider = (
            EnkiBibliographicCoverageProvider(self._db, enki_api=api)
        )

    def run(self):
        super(EnkiCirculationMonitor, self).run()

    def run_once(self, start, cutoff):
        # Give us five minutes of overlap because it's very important
        # we don't miss anything.
        since = start-self.FIVE_MINUTES
        x=0
        step=2000
        while x < 80000:
            availability = self.api.availability(since=since, strt=x, qty=step)
	    status_code = availability.status_code
            content = availability.content
            count = 0
            for bibliographic, circulation in BibliographicParser().process_all(
                    content):
                self.process_book(bibliographic, circulation)
                count += 1
                if count % self.batch_size == 0:
                    self._db.commit()
            x += step

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

#Copied from 3M. Eventually we might want to refactor
class EnkiCollectionReaper(IdentifierSweepMonitor):
    """Check for books that are in the local collection but have left the Enki collection."""
    def __init__(self, _db, api=None, interval_seconds=3600*4):
        super(EnkiCollectionReaper, self).__init__(_db, "Enki Collection Reaper", interval_seconds)
        self._db = _db
        if not api:
            api = EnkiAPI.from_environment(_db)
        self.api = api
        self.data_source = DataSource.lookup(self._db, DataSource.ENKI)

    def run(self):
        self.api = EnkiAPI.from_environment(self._db)
        self.data_source = DataSource.lookup(self._db, DataSource.ENKI)
        super(EnkiCollectionReaper, self).run()

    def identifier_query(self):
        return self._db.query(Identifier).filter(
            Identifier.type==Identifier.ENKI_ID)

    def process_batch(self, identifiers):
        enki_ids = set()
        for identifier in identifiers:
            enki_ids.add(identifier.identifier)

        identifiers_not_mentioned_by_enki= set(identifiers)
        now = datetime.datetime.utcnow()

        for identifier in identifiers:
            result = self.api.reaper_request(identifier.identifier)
            if not result:
                print "skipping this deleted book"
                continue
            print "keeping this existing book"
            enki_id = result
            identifiers_not_mentioned_by_enki.remove(identifier)

            pool = identifier.licensed_through
            if not pool:
                # We don't have a license pool for this work. That
                # shouldn't happen--how did we know about the
                # identifier?--but it shouldn't be a big deal to
                # create one.
                pool, ignore = LicensePool.for_foreign_id(
                    self._db, self.data_source, identifier.type,
                    identifier.identifier)

                # Enki books are never open-access.
                pool.open_access = False
                Analytics.collect_event(
                    self._db, pool, CirculationEvent.DISTRIBUTOR_TITLE_ADD, now)

        # At this point there may be some license pools left over
        # that Enki doesn't know about.  This is a pretty reliable
        # indication that we no longer own any licenses to the
        # book.
        for identifier in identifiers_not_mentioned_by_enki:
            pool = identifier.licensed_through
            if not pool:
                continue
            if pool.licenses_owned > 0:
                if pool.presentation_edition:
                    self.log.warn("Removing %s (%s) from circulation",
                                  pool.presentation_edition.title, pool.presentation_edition.author)
                else:
                    self.log.warn(
                        "Removing unknown work %s from circulation.",
                        identifier.identifier
                    )
            pool.licenses_owned = 0
            pool.licenses_available = 0
            pool.licenses_reserved = 0
            pool.patrons_in_hold_queue = 0
            pool.last_checked = now

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
