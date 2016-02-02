from nose.tools import set_trace
from datetime import datetime, timedelta

from sqlalchemy.orm import contains_eager

from lxml import etree
from core.axis import (
    Axis360API as BaseAxis360API,
    Axis360Parser,
    BibliographicParser,
)

from core.metadata_layer import (
    CirculationData,
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
    DeliveryMechanism,
    Edition,
    Identifier,
    LicensePool,
    Representation,
    Subject,
)

from core.coverage import BibliographicCoverageProvider

from authenticator import Authenticator
from config import Configuration
from circulation import (
    LoanInfo,
    FulfillmentInfo,
    HoldInfo,
    BaseCirculationAPI
)
from circulation_exceptions import *


class Axis360API(BaseAxis360API, Authenticator, BaseCirculationAPI):

    SET_DELIVERY_MECHANISM_AT = BaseCirculationAPI.BORROW_STEP

    # Create a lookup table between common DeliveryMechanism identifiers
    # and Overdrive format types.
    epub = Representation.EPUB_MEDIA_TYPE
    pdf = Representation.PDF_MEDIA_TYPE
    adobe_drm = DeliveryMechanism.ADOBE_DRM
    no_drm = DeliveryMechanism.NO_DRM

    delivery_mechanism_to_internal_format = {
        (epub, no_drm): 'ePub',
        (epub, adobe_drm): 'ePub',
        (pdf, no_drm): 'PDF',
        (pdf, adobe_drm): 'PDF',
    }

    def checkout(self, patron, pin, licensepool, internal_format):

        url = self.base_url + "checkout/v2" 
        title_id = licensepool.identifier.identifier
        patron_id = patron.authorization_identifier
        args = dict(titleId=title_id, patronId=patron_id, 
                    format=internal_format)
        response = self.request(url, data=args, method="POST")
        try:
            return CheckoutResponseParser().process_all(response.content)
        except etree.XMLSyntaxError, e:
            raise InternalServerError(response.content)

    def fulfill(self, patron, pin, licensepool, format_type):
        """Fulfill a patron's request for a specific book.
        """
        identifier = licensepool.identifier
        # This should include only one 'activity'.
        activities = self.patron_activity(patron, pin, licensepool.identifier)
        
        for loan in activities:
            if not isinstance(loan, LoanInfo):
                continue
            if not (loan.identifier_type == identifier.type
                    and loan.identifier == identifier.identifier):
                continue
            # We've found the remote loan corresponding to this
            # license pool.
            fulfillment = loan.fulfillment_info            
            if not fulfillment or not isinstance(fulfillment, FulfillmentInfo):
                raise CannotFulfill()
            return fulfillment
        # If we made it to this point, the patron does not have this
        # book checked out.
        raise NoActiveLoan()

    def checkin(self, patron, pin, licensepool):
        pass

    def place_hold(self, patron, pin, licensepool, format_type,
                   hold_notification_email):
        url = self.base_url + "addtoHold/v2" 
        identifier = licensepool.identifier
        title_id = identifier.identifier
        patron_id = patron.authorization_identifier
        params = dict(titleId=title_id, patronId=patron_id, format=format_type,
                      email=hold_notification_email)
        response = self.request(url, params=params)
        return HoldResponseParser().process_all(
                response.content)

    def release_hold(self, patron, pin, licensepool):
        url = self.base_url + "removeHold/v2"
        identifier = licensepool.identifier
        title_id = identifier.identifier
        patron_id = patron.authorization_identifier
        params = dict(titleId=title_id, patronId=patron_id)
        response = self.request(url, params=params)
        try:
            HoldReleaseResponseParser().process_all(
                response.content)
        except NotOnHold:
            # Fine, it wasn't on hold and now it's still not on hold.
            pass
        # If we didn't raise an exception, we're fine.
        return True

    def patron_activity(self, patron, pin, identifier=None):
        if identifier:
            title_ids = [identifier.identifier]
        else:
            title_ids = None
        availability = self.availability(
            patron_id=patron.authorization_identifier, 
            title_ids=title_ids)
        return list(AvailabilityResponseParser().process_all(
            availability.content))

    def update_licensepools_for_identifiers(self, identifiers):
        """Update availability information for a list of books.

        If the book has never been seen before, a new LicensePool
        will be created for the book.

        The book's LicensePool will be updated with current
        circulation information.
        """
        identifier_strings = self.create_identifier_strings(identifiers)
        response = self.availability(title_ids=identifier_strings)
        parser = BibliographicParser()
        remainder = set(identifiers)
        for bibliographic, availability in parser.process_all(response.content):
            identifier, is_new = bibliographic.primary_identifier.load(self._db)
            if identifier in remainder:
                remainder.remove(identifier)
            pool, is_new = bibliographic.license_pool(self._db)
            availability.update(pool, is_new)

        # We asked Axis about n books. It sent us n-k responses. Those
        # k books are the identifiers in `remainder`. These books have
        # been removed from the collection without us being notified.
        for removed_identifier in remainder:
            pool = removed_identifier.licensed_through
            if not pool:
                self.log.warn(
                    "Was about to reap %r but no local license pool.",
                    removed_identifier
                )
                continue
            if pool.licenses_owned == 0:
                # Already reaped.
                continue
            self.log.info(
                "Reaping %r", removed_identifier
            )
            availability = CirculationData(
                licenses_owned=0,
                licenses_available=0,
                licenses_reserved=0,
                patrons_in_hold_queue=0
            )
            availability.update(pool, False)

    def create_identifier_strings(identifiers):
        identifier_strings = []
        for i in identifiers:
            if isinstance(i, Identifier):
                value = i.identifier
            else:
                value = i
            identifier_strings.append(value)

        return identifier_strings

class Axis360CirculationMonitor(Monitor):

    """Maintain LicensePools for Axis 360 titles.
    """

    VERY_LONG_AGO = datetime(1970, 1, 1)
    FIVE_MINUTES = timedelta(minutes=5)

    def __init__(self, _db, name="Axis 360 Circulation Monitor",
                 interval_seconds=60, batch_size=50):
        super(Axis360CirculationMonitor, self).__init__(
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

    def run(self):
        self.api = Axis360API(self._db)
        super(Axis360CirculationMonitor, self).run()

    def run_once(self, start, cutoff):
        # Give us five minutes of overlap because it's very important
        # we don't miss anything.
        since = start-self.FIVE_MINUTES
        availability = self.api.availability(since=since)
        status_code = availability.status_code
        content = availability.content
        if status_code != 200:
            raise Exception(
                "Got status code %d from API: %s" % (status_code, content))
        count = 0
        for bibliographic, circulation in BibliographicParser().process_all(
                content):
            self.process_book(bibliographic, circulation)
            count += 1
            if count % self.batch_size == 0:
                self._db.commit()

    def process_book(self, bibliographic, availability):
        license_pool, new_license_pool = bibliographic.license_pool(self._db)
        edition, new_edition = bibliographic.edition(self._db)
        license_pool.edition = edition
        if new_license_pool or new_edition:
            bibliographic.apply(
                edition, 
                replace_identifiers=False,
                replace_subjects=True, 
                replace_contributions=True,
                replace_formats=True,
            )
        availability.update(license_pool, new_license_pool)
        return edition, license_pool


class AxisCollectionReaper(IdentifierSweepMonitor):
    """Check for books that are in the local collection but have left our
    Axis 360 collection.
    """

    def __init__(self, _db, interval_seconds=3600*12):
        super(AxisCollectionReaper, self).__init__(
            _db, "Axis Collection Reaper", interval_seconds)

    def run(self):
        self.api = Axis360API(self._db)
        super(AxisCollectionReaper, self).run()

    def identifier_query(self):
        return self._db.query(Identifier).join(
            Identifier.licensed_through).filter(
                Identifier.type==Identifier.AXIS_360_ID).options(
                    contains_eager(Identifier.licensed_through))

    def process_batch(self, identifiers):
        self.api.update_licensepools_for_identifiers(identifiers)


class ResponseParser(Axis360Parser):

    id_type = Identifier.AXIS_360_ID

    # Map Axis 360 error codes to our circulation exceptions.
    code_to_exception = {
        315  : InvalidInputException, # Bad password
        316  : InvalidInputException, # DRM account already exists
        1000 : PatronAuthorizationFailedException,
        1001 : PatronAuthorizationFailedException,
        1002 : PatronAuthorizationFailedException,
        1003 : PatronAuthorizationFailedException,
        2000 : LibraryAuthorizationFailedException,
        2001 : LibraryAuthorizationFailedException,
        2002 : LibraryAuthorizationFailedException,
        2003 : LibraryAuthorizationFailedException, # "Encoded input parameters exceed limit", whatever that meaus
        2004 : LibraryAuthorizationFailedException,
        2005 : LibraryAuthorizationFailedException, # Invalid credentials
        2005 : LibraryAuthorizationFailedException, # Wrong library ID
        2007 : LibraryAuthorizationFailedException, # Invalid library ID
        2008 : LibraryAuthorizationFailedException, # Invalid library ID
        3100 : LibraryInvalidInputException, # Missing title ID
        3101 : LibraryInvalidInputException, # Missing patron ID
        3102 : LibraryInvalidInputException, # Missing email address (for hold notification)
        3103 : NotFoundOnRemote, # Invalid title ID
        3104 : LibraryInvalidInputException, # Invalid Email Address (for hold notification)
        3105 : PatronAuthorizationFailedException, # Invalid Account Credentials
        3106 : InvalidInputException, # Loan Period is out of bounds
        3108 : InvalidInputException, # DRM Credentials Required
        3109 : InvalidInputException, # Hold already exists or hold does not exist, depending.
        3110 : AlreadyCheckedOut,
        3111 : CouldCheckOut,
        3112 : CannotFulfill,
        3113 : CannotLoan,
        (3113, "Title ID is not available for checkout") : NoAvailableCopies,
        3114 : PatronLoanLimitReached, 
        3115 : LibraryInvalidInputException, # Missing DRM format
        3117 : LibraryInvalidInputException, # Invalid DRM format
        3118 : LibraryInvalidInputException, # Invalid Patron credentials
        3119 : LibraryAuthorizationFailedException, # No Blio account
        3120 : LibraryAuthorizationFailedException, # No Acoustikaccount
        3123 : PatronAuthorizationFailedException, # Patron Session ID expired
        3126 : LibraryInvalidInputException, # Invalid checkout format
        3127 : InvalidInputException, # First name is required
        3128 : InvalidInputException, # Last name is required
        3130 : LibraryInvalidInputException, # Invalid hold format (?)
        3131 : InternalServerError, # Custom error message (?)
        3132 : LibraryInvalidInputException, # Invalid delta datetime format
        3134 : LibraryInvalidInputException, # Delta datetime format must not be in the future
        3135 : NoAcceptableFormat,
        3136 : LibraryInvalidInputException, # Missing checkout format
        5000 : InternalServerError,
    }

    def raise_exception_on_error(self, e, ns, custom_error_classes={}):
        """Raise an error if the given lxml node represents an Axis 360 error
        condition.
        """
        code = self._xpath1(e, '//axis:status/axis:code', ns)
        message = self._xpath1(e, '//axis:status/axis:statusMessage', ns)
        if message is None:
            message = etree.tostring(e)
        else:
            message = message.text

        if code is None:
            # Something is so wrong that we don't know what to do.
            raise InternalServerError(message)
        code = code.text
        try:
            code = int(code)
        except ValueError:
            # Non-numeric code? Inconcievable!
            raise InternalServerError(
                "Invalid response code from Axis 360: %s" % code)

        for d in custom_error_classes, self.code_to_exception:
            if (code, message) in d:
                raise d[(code, message)]
            elif code in d:
                # Something went wrong and we know how to turn it into a
                # specific exception.
                raise d[code](message)
        return code, message


class CheckoutResponseParser(ResponseParser):

    def process_all(self, string):
        for i in super(CheckoutResponseParser, self).process_all(
                string, "//axis:checkoutResult", self.NS):
            return i

    def process_one(self, e, namespaces):

        """Either turn the given document into a LoanInfo
        object, or raise an appropriate exception.
        """
        self.raise_exception_on_error(e, namespaces)

        # If we get to this point it's because the checkout succeeded.
        expiration_date = self._xpath1(e, '//axis:expirationDate', namespaces)
        fulfillment_url = self._xpath1(e, '//axis:url', namespaces)
        if fulfillment_url is not None:
            fulfillment_url = fulfillment_url.text

        if expiration_date is not None:
            expiration_date = expiration_date.text
            expiration_date = datetime.strptime(
                expiration_date, self.FULL_DATE_FORMAT)
            
        fulfillment = FulfillmentInfo(
            identifier_type=self.id_type,
            identifier=None, content_link=fulfillment_url,
            content_type=None, content=None, content_expires=None)
        loan_start = datetime.utcnow()
        loan = LoanInfo(
            identifier_type=self.id_type, identifier=None,
            start_date=loan_start,
            end_date=expiration_date,
            fulfillment_info=fulfillment
        )
        return loan

class HoldResponseParser(ResponseParser):

    def process_all(self, string):
        for i in super(HoldResponseParser, self).process_all(
                string, "//axis:addtoholdResult", self.NS):
            return i

    def process_one(self, e, namespaces):
        """Either turn the given document into a HoldInfo
        object, or raise an appropriate exception.
        """
        self.raise_exception_on_error(
            e, namespaces, {3109 : AlreadyOnHold})

        # If we get to this point it's because the hold place succeeded.
        queue_position = self._xpath1(
            e, '//axis:holdsQueuePosition', namespaces)
        if queue_position is None:
            queue_position = None
        else:
            try:
                queue_position = int(queue_position.text)
            except ValueError:
                print "Invalid queue position: %s" % queue_position
                queue_position = None

        hold_start = datetime.utcnow()
        hold = HoldInfo(
            identifier_type=self.id_type, identifier=None,
            start_date=hold_start, end_date=None, hold_position=queue_position)
        return hold

class HoldReleaseResponseParser(ResponseParser):

    def process_all(self, string):
        for i in super(HoldReleaseResponseParser, self).process_all(
                string, "//axis:removeholdResult", self.NS):
            return i

    def process_one(self, e, namespaces):
        # There's no data to gather here. Either there was an error
        # or we were successful.
        self.raise_exception_on_error(
            e, namespaces, {3109 : NotOnHold})
        return True

class AvailabilityResponseParser(ResponseParser):
   
    def process_all(self, string):
        for info in super(AvailabilityResponseParser, self).process_all(
                string, "//axis:title", self.NS):
            # Filter out books where nothing in particular is
            # happening.
            if info:
                yield info

    def process_one(self, e, ns):

        # Figure out which book we're talking about.
        axis_identifier = self.text_of_subtag(e, "axis:titleId", ns)
        availability = self._xpath1(e, 'axis:availability', ns)
        if availability is None:
            return None
        reserved = self._xpath1_boolean(availability, 'axis:isReserved', ns)
        checked_out = self._xpath1_boolean(availability, 'axis:isCheckedout', ns)
        on_hold = self._xpath1_boolean(availability, 'axis:isInHoldQueue', ns)

        info = None
        if checked_out:
            start_date = self._xpath1_date(
                availability, 'axis:checkoutStartDate', ns)
            end_date = self._xpath1_date(
                availability, 'axis:checkoutEndDate', ns)
            download_url = self.text_of_optional_subtag(
                availability, 'axis:downloadUrl', ns)
            if download_url:
                fulfillment = FulfillmentInfo(
                    identifier_type=self.id_type,
                    identifier=axis_identifier,
                    content_link=download_url, content_type=None,
                    content=None, content_expires=None)
            info = LoanInfo(
                identifier_type=self.id_type,
                identifier=axis_identifier,
                start_date=start_date, end_date=end_date,
                fulfillment_info=fulfillment)

        elif reserved:
            end_date = self._xpath1_date(
                availability, 'axis:reservedEndDate', ns)
            info = HoldInfo(
                identifier_type=self.id_type,
                identifier=axis_identifier,
                start_date=None, 
                end_date=end_date,
                hold_position=0
            )
        elif on_hold:
            position = self.int_of_optional_subtag(
                availability, 'axis:holdsQueuePosition', ns)
            info = HoldInfo(
                identifier_type=self.id_type,
                identifier=axis_identifier,
                start_date=None, end_date=None,
                hold_position=position)
        return info


class Axis360BibliographicCoverageProvider(BibliographicCoverageProvider):
    """Fill in bibliographic metadata for Axis360 records."""

    cls_log = logging.getLogger("Axis360 Bibliographic Coverage Provider")

    def __init__(self, _db):
        self.parser = BibliographicParser()
        super(Axis360BibliographicCoverageProvider, self).__init__(_db,
                Axis360API(_db), DataSource.AXIS_360)

    def process_batch(self, identifiers):
        identifier_strings = self.api.create_identifier_strings(identifiers)
        response = self.api.availability(title_ids=identifier_strings)
        batch_results = []
        for bibliographic, availability in self.parser.process_all(response.content):
            identifier, is_new = bibliographic.primary_identifier.load(self._db)
            result = self.set_presentation_ready(identifier, bibliographic)
            batch_results.append(result)
        return batch_results
