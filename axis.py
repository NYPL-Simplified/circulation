from nose.tools import set_trace
from datetime import datetime

from lxml import etree
from core.axis import (
    Axis360API as BaseAxis360API,
    BibliographicParser,
)

from core.monitor import Monitor

from core.model import (
    CirculationEvent,
    get_one_or_create,
    Contributor,
    DataSource,
    Edition,
    Identifier,
    LicensePool,
    Subject,
)

from core.util.xmlparser import XMLParser

from circulation import FulfillmentInfo
from circulation_exceptions import *


class Axis360API(BaseAxis360API):

    allowable_formats = ["ePub"]

    def checkout(self, patron, pin, licensepool, format_type):
        title_id = licensepool.identifier.identifier
        patron_id = patron.authorization_identifier
        args = dict(titleId=title_id, patronId=patron_id, format=format_type,
                    loanPeriod=1)
        url = self.base_url + "checkout/v2" 
        response = self.request(url, data=args, method="POST")
        set_trace()
        pass

    def checkin(self, patron, pin, licensepool):
        pass

    def fulfill(self, patron, pin, licensepool, format_type):
        pass

    def place_hold(self, patron, pin, licensepool, hold_notification_email):
        pass

    def release_hold(self, patron, pin, licensepool):
        pass
    

class Axis360CirculationMonitor(Monitor):

    """Maintain LicensePools for Axis 360 titles.
    """

    def __init__(self, _db, name="Axis 360 Circulation Monitor",
                 interval_seconds=60, batch_size=50):
        super(Axis360CirculationMonitor, self).__init__(
            _db, name, interval_seconds=interval_seconds,
            default_start_time = datetime.utcnow() - Monitor.ONE_YEAR_AGO)
        self.batch_size = batch_size

    def run(self):
        self.api = Axis360API(self._db)
        super(Axis360CirculationMonitor, self).run()

    def run_once(self, start, cutoff):
        availability = self.api.availability(start)
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
        [axis_id] = bibliographic[Identifier][Identifier.AXIS_360_ID]
        axis_id = axis_id[Identifier.identifier]

        license_pool, new_license_pool = LicensePool.for_foreign_id(
            self._db, self.api.source, Identifier.AXIS_360_ID, axis_id)

        # The Axis 360 identifier is exactly equivalent to each ISBN.
        any_new_isbn = False
        isbns = []
        for i in bibliographic[Identifier].get(Identifier.ISBN):
            isbn_id = i[Identifier.identifier]
            isbn, was_new = Identifier.for_foreign_id(
                self._db, Identifier.ISBN, isbn_id)
            isbns.append(isbn)
            any_new_isbn = any_new_isbn or was_new

        edition, new_edition = Edition.for_foreign_id(
            self._db, self.api.source, Identifier.AXIS_360_ID, axis_id)

        axis_id = license_pool.identifier

        if any_new_isbn or new_license_pool or new_edition:
            for isbn in isbns:
                axis_id.equivalent_to(self.api.source, isbn, strength=1)

        if new_license_pool or new_edition:
            # Add bibliographic information to the Edition.
            edition.title = bibliographic.get(Edition.title)
            print "NEW EDITION: %s" % edition.title
            edition.subtitle = bibliographic.get(Edition.subtitle)
            edition.series = bibliographic.get(Edition.series)
            edition.published = bibliographic.get(Edition.published)
            edition.publisher = bibliographic.get(Edition.publisher)
            edition.imprint = bibliographic.get(Edition.imprint)
            edition.language = bibliographic.get(Edition.language)

            # Contributors!
            contributors_by_role = bibliographic.get(Contributor, {})
            for role, contributors in contributors_by_role.items():
                for name in contributors:
                    edition.add_contributor(name, role)

            # Subjects!
            for subject in bibliographic.get(Subject, []):
                s_type = subject[Subject.type]
                s_identifier = subject[Subject.identifier]

                axis_id.classify(
                    self.api.source, s_type, s_identifier)

        # Update the license pool with new availability information
        new_licenses_owned = availability.get(LicensePool.licenses_owned, 0)
        new_licenses_available = availability.get(
            LicensePool.licenses_available, 0)
        new_licenses_reserved = 0
        new_patrons_in_hold_queue = availability.get(
            LicensePool.patrons_in_hold_queue, 0)

        last_checked = availability.get(
            LicensePool.last_checked, datetime.utcnow())

        # If this is our first time seeing this LicensePool, log its
        # occurance as a separate event
        if new_license_pool:
            event = get_one_or_create(
                self._db, CirculationEvent,
                type=CirculationEvent.TITLE_ADD,
                license_pool=license_pool,
                create_method_kwargs=dict(
                    start=last_checked,
                    delta=1,
                    end=last_checked,
                )
            )

        license_pool.update_availability(
            new_licenses_owned, new_licenses_available, new_licenses_reserved,
            new_patrons_in_hold_queue, last_checked)

        return edition, license_pool

class ResponseParser(XMLParser):

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
        3103 : LibraryInvalidInputException, # Invalid title ID
        3104 : LibraryInvalidInputException, # Invalid Email Address (for hold notification)
        3105 : PatronAuthorizationFailedException, # Invalid Account Credentials
        3106 : InvalidInputException, # Loan Period is out of bounds
        3108 : InvalidInputException, # DRM Credentials Required
        3109 : AlreadyOnHold,
        3110 : AlreadyCheckedOut,
        3111 : CouldCheckOut,
        3112 : CannotFulfill,
        3113 : CannotLoan,
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


class CheckoutResponseParser(ResponseParser):

    NAMESPACES = {"axis" : "http://axis360api.baker-taylor.com/vendorAPI"}

    def process_all(self, string):
        for i in super(CheckoutResponseParser, self).process_all(
                string, "//axis:checkoutResult", self.NAMESPACES):
            return i

    def process_one(self, e, namespaces):
        """Either turn the given document into a FulfillmentInfo
        object, or raise an appropriate exception.
        """
        code = self._xpath1(e, '//axis:status/axis:code', namespaces)
        message = self._xpath1(e, '//axis:status/axis:statusMessage', namespaces)
        expiration_date = self._xpath1(e, '//axis:expirationDate', namespaces)
        fulfillment_url = self._xpath1(e, '//axis:url', namespaces)

        if message:
            message = message.text
        else:
            message = etree.tostring(e)

        if code is None:
            # Something is so wrong that we don't know what to do.
            raise InternalServerError(message)
        code = code.text
        try:
            code = int(code)
        except ValueError:
            # Non-numeric code? Inconcievable!
            raise InternalServerError(message)
        if code in self.code_to_exception:
            # Something went wrong and we know how to turn it into a
            # specific exception.
            raise self.code_to_exception[code](message)

        # We have a non-error condition, which means the checkout succeeded.
        # Set up a FulfillmentInfo.
        if fulfillment_url is not None:
            fulfillment_url = fulfillment_url.text

        if expiration_date is not None:
            expiration_date = expiration_date.text
            expiration_date = datetime.strptime(
                expiration_date, BibliographicParser.FULL_DATE_FORMAT)
            
        return FulfillmentInfo(
            content_link=fulfillment_url, content_type=None, content=None,
            content_expires=expiration_date)
