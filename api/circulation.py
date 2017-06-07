from nose.tools import set_trace
from circulation_exceptions import *
import datetime
from collections import defaultdict
from threading import Thread
import logging
import re
import time
from flask.ext.babel import lazy_gettext as _

from core.analytics import Analytics
from core.model import (
    get_one,
    CirculationEvent,
    Identifier,
    DataSource,
    LicensePoolDeliveryMechanism,
    LicensePool,
    Loan,
    Hold,
)
from util.patron import PatronUtility
from core.util.cdn import cdnify
from config import Configuration

class CirculationInfo(object):
    def fd(self, d):
        # Stupid method to format a date
        if not d:
            return None
        else:
            return datetime.datetime.strftime(d, "%Y/%m/%d %H:%M:%S")

class FulfillmentInfo(CirculationInfo):

    """A record of an attempt to fulfill a loan.

    :param identifier_type Ex: Third party provider, ISBN, URI, etc...
    :param identifier Contains ISBN or third party item id, etc., and links to LicensePool, Work, etc..
    :param content_link Either URL to download ACSM file from or URL to streaming content.
    :param content_type Media type of the book version we're getting.  
        Ex: "text/html" or Representation.TEXT_HTML_MEDIA_TYPE + DeliveryMechanism.STREAMING_PROFILE 
        or EPUB_MEDIA_TYPE.
    :param content Body of acsm file or empty.  Would have either content or content_link filled in.
    :param content_expires Download link expiration datetime.
    """

    def __init__(self, identifier_type, identifier, content_link, content_type, 
                 content, content_expires):
        self.identifier_type = identifier_type
        self.identifier = identifier
        self.content_link = content_link
        self.content_type = content_type
        self.content = content
        self.content_expires = content_expires
    
    def __repr__(self):
        if self.content:
            blength = len(self.content)
        else:
            blength = 0
        return "<FulfillmentInfo: content_link: %r, content_type: %r, content: %d bytes, expires: %r>" % (
            self.content_link, self.content_type, blength,
            self.fd(self.content_expires))

class LoanInfo(CirculationInfo):

    """A record of a loan.

    :param identifier_type Ex.: Identifier.ONECLICK_ID.
    :param identifier Expected to be the unicode string of the isbn, etc..
    :param start_date When the patron checked the book out.
    :param end_date When checked-out book is due.  Expected to be passed in 
    date, not unicode format.
    :param fulfillment_info A FulfillmentInfo object.
    """

    def __init__(self, identifier_type, identifier, start_date, end_date,
                 fulfillment_info=None):
        self.identifier_type = identifier_type
        self.identifier = identifier
        self.start_date = start_date
        self.end_date = end_date
        self.fulfillment_info = fulfillment_info

    def __repr__(self):
        if self.fulfillment_info:
            fulfillment = " Fulfilled by: " + repr(self.fulfillment_info)
        else:
            fulfillment = ""
        f = "%Y/%m/%d"
        return "<LoanInfo for %s/%s, start=%s end=%s>%s" % (
            self.identifier_type, self.identifier,
            self.fd(self.start_date), self.fd(self.end_date), 
            fulfillment
        )

class HoldInfo(CirculationInfo):

    """A record of a hold.

    :param identifier_type Ex.: Identifier.ONECLICK_ID.
    :param identifier Expected to be the unicode string of the isbn, etc..
    :param start_date When the patron made the reservation.
    :param end_date When reserved book is expected to become available.  Expected to be passed in 
        date, not unicode format.
    :param hold_position  Patron's place in the hold line.  
        When not available, default to be passed is None, which is equivalent to "first in line".
    """

    def __init__(self, identifier_type, identifier, start_date, end_date, 
                 hold_position):
        self.identifier_type = identifier_type
        self.identifier = identifier
        self.start_date = start_date
        self.end_date = end_date
        self.hold_position = hold_position

    def __repr__(self):
        return "<HoldInfo for %s/%s, start=%s end=%s, position=%s>" % (
            self.identifier_type, self.identifier,
            self.fd(self.start_date), self.fd(self.end_date), 
            self.hold_position
        )



class CirculationAPI(object):
    """Implement basic circulation logic and abstract away the details
    between different circulation APIs.
    """

    def __init__(self, _db, overdrive=None, threem=None, axis=None, enki=None):
        self._db = _db
        self.overdrive = overdrive
        self.threem = threem
        self.axis = axis
        self.enki = enki
        self.apis = [x for x in (overdrive, threem, axis, enki) if x]
        self.log = logging.getLogger("Circulation API")

        # When we get our view of a patron's loans and holds, we need
        # to include loans from all licensed data sources.  We do not
        # need to include loans from open-access sources because we
        # are the authorities on those.
        data_sources_for_sync = []
        if self.overdrive:
            data_sources_for_sync.append(
                DataSource.lookup(_db, DataSource.OVERDRIVE)
            )
        if self.threem:
            data_sources_for_sync.append(
                DataSource.lookup(_db, DataSource.THREEM)
            )
        if self.axis:
            data_sources_for_sync.append(
                DataSource.lookup(_db, DataSource.AXIS_360)
            )
        if self.enki:
            data_sources_for_sync.append(
                DataSource.lookup(_db, DataSource.ENKI)
            )


        h = dict()
        for ds in data_sources_for_sync:
            type = ds.primary_identifier_type 
            h[type] = ds.name
            if type in Identifier.DEPRECATED_NAMES:
                new_name = Identifier.DEPRECATED_NAMES[type]
                h[new_name] = ds.name
        self.identifier_type_to_data_source_name = h
        self.data_source_ids_for_sync = [
            x.id for x in data_sources_for_sync
        ]

    def api_for_license_pool(self, licensepool):
        """Find the API to use for the given license pool."""
        if licensepool.data_source.name==DataSource.OVERDRIVE:
            api = self.overdrive
        elif licensepool.data_source.name==DataSource.THREEM:
            api = self.threem
        elif licensepool.data_source.name==DataSource.AXIS_360:
            api = self.axis
        elif licensepool.data_source.name==DataSource.ENKI:
            api = self.enki
        else:
            return None

        return api

    def can_revoke_hold(self, licensepool, hold):
        """Some circulation providers allow you to cancel a hold
        when the book is reserved to you. Others only allow you to cancel
        a hole while you're in the hold queue.
        """
        if hold.position is None or hold.position > 0:
            return True
        api = self.api_for_license_pool(licensepool)
        if api.CAN_REVOKE_HOLD_WHEN_RESERVED:
            return True
        return False

    def borrow(self, patron, pin, licensepool, delivery_mechanism,
               hold_notification_email=None):
        """Either borrow a book or put it on hold. Don't worry about fulfilling
        the loan yet.
        
        :return: A 3-tuple (`Loan`, `Hold`, `is_new`). Either `Loan`
        or `Hold` must be None, but not both.
        """
        # Short-circuit the request if the patron lacks borrowing
        # privileges.
        PatronUtility.assert_borrowing_privileges(patron)        
        
        now = datetime.datetime.utcnow()
        if licensepool.open_access:
            # We can 'loan' open-access content ourselves just by
            # putting a row in the database.
            now = datetime.datetime.utcnow()
            __transaction = self._db.begin_nested()
            loan, is_new = licensepool.loan_to(patron, start=now, end=None)
            __transaction.commit()
            self._collect_checkout_event(licensepool)
            return loan, None, is_new

        # Okay, it's not an open-access book. This means we need to go
        # to an external service to get the book. 
        #
        # This also means that our internal model of whether this book
        # is currently on loan or on hold might be wrong.
        api = self.api_for_license_pool(licensepool)

        must_set_delivery_mechanism = (
            api.SET_DELIVERY_MECHANISM_AT == BaseCirculationAPI.BORROW_STEP)

        if must_set_delivery_mechanism and not delivery_mechanism:
            raise DeliveryMechanismMissing()
    
        content_link = content_expires = None

        internal_format = api.internal_format(delivery_mechanism)

        if patron.fines:
            max_fines = Configuration.max_outstanding_fines()
            if patron.fines >= max_fines.amount:
                raise OutstandingFines()

        # Do we (think we) already have this book out on loan?
        existing_loan = get_one(
             self._db, Loan, patron=patron, license_pool=licensepool,
             on_multiple='interchangeable'
        )
        
        loan_info = None
        hold_info = None
        if existing_loan:
            # Sync with the API to see if the loan still exists.  If
            # it does, we still want to perform a 'checkout' operation
            # on the API, because that's how loans are renewed, but
            # certain error conditions (like NoAvailableCopies) mean
            # something different if you already have a confirmed
            # active loan.
            self.sync_bookshelf(patron, pin)
            existing_loan = get_one(
                self._db, Loan, patron=patron, license_pool=licensepool,
                on_multiple='interchangeable'
            )

        new_loan = False
        try:
            loan_info = api.checkout(
                patron, pin, licensepool, internal_format
            )

            # We asked the API to create a loan and it gave us a
            # LoanInfo object, rather than raising an exception like
            # AlreadyCheckedOut.
            #
            # For record-keeping purposes we're going to treat this as
            # a newly transacted loan, although it's possible that the
            # API does something unusual like return LoanInfo instead
            # of raising AlreadyCheckedOut.
            new_loan = True
        except AlreadyCheckedOut:
            # This is good, but we didn't get the real loan info.
            # Just fake it.
            identifier = licensepool.identifier            
            loan_info = LoanInfo(
                identifier.type, 
                identifier,
                start_date=None, 
                end_date=now + datetime.timedelta(hours=1)
            )
        except AlreadyOnHold:
            # We're trying to check out a book that we already have on hold.
            hold_info = HoldInfo(
                licensepool.identifier.type, licensepool.identifier.identifier,
                None, None, None
            )
        except NoAvailableCopies:
            if existing_loan:
                # The patron tried to renew a loan but there are
                # people waiting in line for them to return the book,
                # so renewals are not allowed.
                raise CannotRenew(
                    _("You cannot renew a loan if other patrons have the work on hold.")
                )
            else:
                # That's fine, we'll just (try to) place a hold.
                #
                # Since the patron incorrectly believed there were
                # copies available, update availability information
                # immediately.
                api.update_availability(licensepool)
        except NoLicenses, e:
            # Since the patron incorrectly believed there were
            # licenses available, update availability information
            # immediately.
            api.update_availability(licensepool)
            raise e

        if loan_info:
            # We successfuly secured a loan.  Now create it in our
            # database.
            __transaction = self._db.begin_nested()
            loan, new_loan_record = licensepool.loan_to(
                patron, start=loan_info.start_date or now,
                end=loan_info.end_date)

            if must_set_delivery_mechanism:
                loan.fulfillment = delivery_mechanism
            existing_hold = get_one(
                self._db, Hold, patron=patron, license_pool=licensepool,
                on_multiple='interchangeable'
            )
            if existing_hold:
                # The book was on hold, and now we have a loan.
                # Delete the record of the hold.
                self._db.delete(existing_hold)
            __transaction.commit()

            if loan and new_loan:
                # Send out an analytics event to record the fact that
                # a loan was initiated through the circulation
                # manager.
                self._collect_checkout_event(licensepool)
            return loan, None, new_loan_record

        # At this point we know that we neither successfully
        # transacted a loan, nor discovered a preexisting loan.

        # Checking out a book didn't work, so let's try putting
        # the book on hold.
        if not hold_info:
            try:
                hold_info = api.place_hold(
                    patron, pin, licensepool,
                    hold_notification_email
                )
            except AlreadyOnHold, e:
                hold_info = HoldInfo(
                    licensepool.identifier.type, licensepool.identifier.identifier,
                    None, None, None
                )

        # It's pretty rare that we'd go from having a loan for a book
        # to needing to put it on hold, but we do check for that case.
        __transaction = self._db.begin_nested()
        hold, is_new = licensepool.on_hold_to(
            patron,
            hold_info.start_date or now,
            hold_info.end_date, 
            hold_info.hold_position
        )

        if hold and is_new:
            # Send out an analytics event to record the fact that
            # a hold was initiated through the circulation
            # manager.
            Analytics.collect_event(
                self._db, licensepool,
                CirculationEvent.CM_HOLD_PLACE,
            )

        if existing_loan:
            self._db.delete(existing_loan)
        __transaction.commit()
        return None, hold, is_new

    def _collect_checkout_event(self, licensepool):
        """Collect an analytics event indicating the given LicensePool
        was checked out via the circulation manager.
        """
        Analytics.collect_event(
            self._db, licensepool,
            CirculationEvent.CM_CHECKOUT,
        )
    
    def fulfill(self, patron, pin, licensepool, delivery_mechanism, sync_on_failure=True):
        """Fulfil a book that a patron has previously checked out.

        :param delivery_mechanism: A LicensePoolDeliveryMechanism
        explaining how the patron wants the book to be delivered. If
        the book has previously been delivered through some other
        mechanism, this parameter is ignored and the previously used
        mechanism takes precedence.

        :return: A FulfillmentInfo object.
        """
        fulfillment = None
        loan = get_one(
            self._db, Loan, patron=patron, license_pool=licensepool,
            on_multiple='interchangeable'
        )
        if not loan:
            if sync_on_failure:
                # Sync and try again.
                self.sync_bookshelf(patron, pin)
                return self.fulfill(
                    patron, pin, licensepool=licensepool,
                    delivery_mechanism=delivery_mechanism,
                    sync_on_failure=False
                )
            else:
                raise NoActiveLoan(_("Cannot find your active loan for this work."))
        if loan.fulfillment is not None and loan.fulfillment != delivery_mechanism and not delivery_mechanism.delivery_mechanism.is_streaming:
            raise DeliveryMechanismConflict(
                _("You already fulfilled this loan as %(loan_delivery_mechanism)s, you can't also do it as %(requested_delivery_mechanism)s",
                  loan_delivery_mechanism=loan.fulfillment.delivery_mechanism.name, 
                  requested_delivery_mechanism=delivery_mechanism.delivery_mechanism.name)
            )

        if licensepool.open_access:
            fulfillment = self.fulfill_open_access(
                licensepool, delivery_mechanism.delivery_mechanism
            )
        else:
            api = self.api_for_license_pool(licensepool)
            internal_format = api.internal_format(delivery_mechanism)
            fulfillment = api.fulfill(
                patron, pin, licensepool, internal_format
            )
            if not fulfillment or not (
                    fulfillment.content_link or fulfillment.content
            ):
                raise NoAcceptableFormat()

        # Send out an analytics event to record the fact that
        # a fulfillment was initiated through the circulation
        # manager.
        Analytics.collect_event(
            self._db, licensepool,
            CirculationEvent.CM_FULFILL,
        )

        # Make sure the delivery mechanism we just used is associated
        # with the loan.
        if loan.fulfillment is None and not delivery_mechanism.delivery_mechanism.is_streaming:
            __transaction = self._db.begin_nested()
            loan.fulfillment = delivery_mechanism
            __transaction.commit()
        return fulfillment

    def fulfill_open_access(self, licensepool, delivery_mechanism):
        """Fulfill an open-access LicensePool through the requested
        DeliveryMechanism.

        :param licensepool: The title to be fulfilled.
        :param delivery_mechanism: A DeliveryMechanism.
        """
        if isinstance(delivery_mechanism, LicensePoolDeliveryMechanism):
            self.log.warn("LicensePoolDeliveryMechanism passed into fulfill_open_access, should be DeliveryMechanism.")
            delivery_mechanism = delivery_mechanism.delivery_mechanism
        fulfillment = None
        for lpdm in licensepool.delivery_mechanisms:
            if not (lpdm.resource and lpdm.resource.representation
                    and lpdm.resource.representation.url):
                # This LicensePoolDeliveryMechanism can't actually
                # be used for fulfillment.
                continue
            if lpdm.delivery_mechanism == delivery_mechanism:
                # We found it! This is how the patron wants
                # the book to be delivered.
                fulfillment = lpdm
                break

        if not fulfillment:
            # There is just no way to fulfill this loan the way the
            # patron wants.
            raise FormatNotAvailable()

        rep = fulfillment.resource.representation
        cdns = Configuration.cdns()
        content_link = cdnify(rep.url, cdns)
        media_type = rep.media_type
        return FulfillmentInfo(
            identifier_type=licensepool.identifier.type,
            identifier=licensepool.identifier.identifier,
            content_link=content_link, content_type=media_type, content=None, 
            content_expires=None
        )

    def revoke_loan(self, patron, pin, licensepool):
        """Revoke a patron's loan for a book."""
        loan = get_one(
            self._db, Loan, patron=patron, license_pool=licensepool,
            on_multiple='interchangeable'
        )
        if loan:
            __transaction = self._db.begin_nested()
            logging.info("In revoke_loan(), deleting loan #%d" % loan.id)
            self._db.delete(loan)
            __transaction.commit()

            # Send out an analytics event to record the fact that
            # a loan was revoked through the circulation
            # manager.
            Analytics.collect_event(
                self._db, licensepool,
                CirculationEvent.CM_CHECKIN,
            )

        if not licensepool.open_access:
            api = self.api_for_license_pool(licensepool)
            try:
                api.checkin(patron, pin, licensepool)
            except NotCheckedOut, e:
                # The book wasn't checked out in the first
                # place. Everything's fine.
                pass
        # Any other CannotReturn exception will be propagated upwards
        # at this point.
        return True

    def release_hold(self, patron, pin, licensepool):
        """Remove a patron's hold on a book."""
        hold = get_one(
            self._db, Hold, patron=patron, license_pool=licensepool,
            on_multiple='interchangeable'
        )
        if not licensepool.open_access:
            api = self.api_for_license_pool(licensepool)
            try:
                api.release_hold(patron, pin, licensepool)
            except NotOnHold, e:
                # The book wasn't on hold in the first place. Everything's
                # fine.
                pass
        # Any other CannotReleaseHold exception will be propagated
        # upwards at this point
        if hold:
            __transaction = self._db.begin_nested()
            self._db.delete(hold)
            __transaction.commit()

            # Send out an analytics event to record the fact that
            # a hold was revoked through the circulation
            # manager.
            Analytics.collect_event(
                self._db, licensepool,
                CirculationEvent.CM_HOLD_RELEASE,
            )

        return True

    def patron_activity(self, patron, pin):
        """Return a record of the patron's current activity
        vis-a-vis all data sources.

        We check each data source in a separate thread for speed.

        :return: A 2-tuple (loans, holds) containing `HoldInfo` and
        `LoanInfo` objects.
        """
        log = self.log
        class PatronActivityThread(Thread):
            def __init__(self, api, patron, pin):
                self.api = api
                self.patron = patron
                self.pin = pin
                self.activity = None
                self.exception = None
                super(PatronActivityThread, self).__init__()

            def run(self):
                before = time.time()
                try:
                    self.activity = self.api.patron_activity(
                        self.patron, self.pin)
                except Exception, e:
                    self.exception = e
                after = time.time()
                log.debug(
                    "Synced %s in %.2f sec", self.api.__class__.__name__,
                    after-before
                )

        threads = []
        before = time.time()
        for api in self.apis:
            thread = PatronActivityThread(api, patron, pin)
            threads.append(thread)
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        loans = []
        holds = []
        complete = True
        for thread in threads:
            if thread.exception:
                # Something went wrong, so we don't have a complete
                # picture of the patron's loans.
                complete = False
                self.log.error(
                    "%s errored out: %s", thread.api.__class__.__name__,
                    thread.exception,
                    exc_info=thread.exception
                )
            if thread.activity:
                for i in thread.activity:
                    l = None
                    if isinstance(i, LoanInfo):
                        l = loans
                    elif isinstance(i, HoldInfo):
                        l = holds
                    else:
                        self.log.warn(
                            "value %r from patron_activity is neither a loan nor a hold.", 
                            i
                        )
                    if l is not None:
                        l.append(i)
        after = time.time()
        self.log.debug("Full sync took %.2f sec", after-before)
        return loans, holds, complete

    def local_loans(self, patron):
        return self._db.query(Loan).join(Loan.license_pool).filter(
            LicensePool.data_source_id.in_(self.data_source_ids_for_sync)
        ).filter(
            Loan.patron==patron
        )

    def local_holds(self, patron):
        return self._db.query(Hold).join(Hold.license_pool).filter(
            LicensePool.data_source_id.in_(self.data_source_ids_for_sync)
        ).filter(
            Hold.patron==patron
        )

    def sync_bookshelf(self, patron, pin):

        # Get the external view of the patron's current state.
        remote_loans, remote_holds, complete = self.patron_activity(patron, pin)

        # Get our internal view of the patron's current state.
        __transaction = self._db.begin_nested()
        local_loans = self.local_loans(patron)
        local_holds = self.local_holds(patron)

        now = datetime.datetime.utcnow()
        local_loans_by_identifier = {}
        local_holds_by_identifier = {}
        for l in local_loans:
            if not l.license_pool:
                self.log.error("Active loan with no license pool!")
                continue
            i = l.license_pool.identifier
            if not i:
                self.log.error(
                    "Active loan on license pool %s, which has no identifier!",
                    l.license_pool
                )
                continue
            key = (i.type, i.identifier)
            local_loans_by_identifier[key] = l
        for h in local_holds:
            if not h.license_pool:
                self.log.error("Active hold with no license pool!")
                continue
            i = h.license_pool.identifier
            if not i:
                self.log.error(
                    "Active hold on license pool %r, which has no identifier!",
                    h.license_pool
                )
                continue
            key = (i.type, i.identifier)
            local_holds_by_identifier[key] = h

        active_loans = []
        active_holds = []
        for loan in remote_loans:
            # This is a remote loan. Find or create the corresponding
            # local loan.
            source_name = self.identifier_type_to_data_source_name[
                loan.identifier_type
            ]
            source = DataSource.lookup(self._db, source_name)
            key = (loan.identifier_type, loan.identifier)
            pool, ignore = LicensePool.for_foreign_id(
                self._db, source, loan.identifier_type,
                loan.identifier)
            start = loan.start_date or now
            end = loan.end_date
            local_loan, new = pool.loan_to(patron, start, end)
            active_loans.append(local_loan)

            # Remove the local loan from the list so that we don't
            # delete it later.
            if key in local_loans_by_identifier:
                del local_loans_by_identifier[key]

        for hold in remote_holds:
            # This is a remote hold. Find or create the corresponding
            # local hold.
            key = (hold.identifier_type, hold.identifier)
            source_name = self.identifier_type_to_data_source_name[
                hold.identifier_type
            ]
            source = DataSource.lookup(self._db, source_name)
            pool, ignore = LicensePool.for_foreign_id(
                self._db, source, hold.identifier_type,
                hold.identifier)
            start = hold.start_date or now
            end = hold.end_date
            position = hold.hold_position
            local_hold, new = pool.on_hold_to(patron, start, end, position)
            active_holds.append(local_hold)

            # Remove the local hold from the list so that we don't
            # delete it later.
            if key in local_holds_by_identifier:
                del local_holds_by_identifier[key]

        # We only want to delete local loans and holds if we were able to
        # successfully sync with all the providers. If there was an error,
        # the provider might still know about a loan or hold that we don't
        # have in the remote lists.
        if complete:
            # Every loan remaining in loans_by_identifier is a hold that
            # the provider doesn't know about. This usually means it's expired
            # and we should get rid of it, but it's possible the patron is
            # borrowing a book and syncing their bookshelf at the same time,
            # and the local loan was created after we got the remote loans.
            # If the loan's start date is less than a minute ago, we'll keep it.
            for loan in local_loans_by_identifier.values():
                if loan.license_pool.data_source.id in self.data_source_ids_for_sync:
                    one_minute_ago = datetime.datetime.utcnow() - datetime.timedelta(minutes=1)
                    if loan.start < one_minute_ago:
                        logging.info("In sync_bookshelf for patron %s, deleting loan %d (patron %s)" % (patron.authorization_identifier, loan.id, loan.patron.authorization_identifier))
                        self._db.delete(loan)
                    else:
                        logging.info("In sync_bookshelf for patron %s, found local loan %d created in the past minute that wasn't in remote loans" % (patron.authorization_identifier, loan.id))

            # Every hold remaining in holds_by_identifier is a hold that
            # the provider doesn't know about, which means it's expired
            # and we should get rid of it.
            for hold in local_holds_by_identifier.values():
                if hold.license_pool.data_source.id in self.data_source_ids_for_sync:
                    self._db.delete(hold)

        __transaction.commit()
        return active_loans, active_holds


class BaseCirculationAPI(object):
    """Encapsulates logic common to all circulation APIs."""

    BORROW_STEP = 'borrow'
    FULFILL_STEP = 'fulfill'

    # In 3M only, when a book is in the 'reserved' state the patron
    # cannot revoke their hold on the book.
    CAN_REVOKE_HOLD_WHEN_RESERVED = True

    # If the client must set a delivery mechanism at the point of
    # checkout (Axis 360), set this to BORROW_STEP. If the client may
    # wait til the point of fulfillment to set a delivery mechanism
    # (Overdrive), set this to FULFILL_STEP. If there is no choice of
    # delivery mechanisms (3M), set this to None.
    SET_DELIVERY_MECHANISM_AT = FULFILL_STEP

    # Different APIs have different internal names for delivery
    # mechanisms. This is a mapping of (content_type, drm_type)
    # 2-tuples to those internal names.
    #
    # For instance, the combination ("application/epub+zip",
    # "vnd.adobe/adept+xml") is called "ePub" in Axis 360 and 3M, but
    # is called "ebook-epub-adobe" in Overdrive.
    delivery_mechanism_to_internal_format = {}

    def internal_format(self, delivery_mechanism):
        """Look up the internal format for this delivery mechanism or
        raise an exception.
        """
        if not delivery_mechanism:
            return None
        d = delivery_mechanism.delivery_mechanism
        key = (d.content_type, d.drm_scheme)
        internal_format = self.delivery_mechanism_to_internal_format.get(key)
        if not internal_format:
            raise DeliveryMechanismError(
                _("Could not map Simplified delivery mechanism %(mechanism_name)s to internal delivery mechanism!", mechanism_name=d.name)
            )
        return internal_format

    def default_notification_email_address(self, patron, pin):
        """What email address should be used to notify this patron
        of changes?
        """
        return Configuration.default_notification_email_address()


    def checkin(self, patron, pin, licensepool):
        """  Return a book early.  

        :param patron: a Patron object for the patron who wants
        to check out the book.
        :param pin: The patron's alleged password.
        :param licensepool: Contains lending info as well as link to parent Identifier.
        """
        pass


    def checkout(self, patron, pin, licensepool, internal_format):
        """Check out a book on behalf of a patron.

        :param patron: a Patron object for the patron who wants
        to check out the book.
        :param pin: The patron's alleged password.
        :param licensepool: Contains lending info as well as link to parent Identifier.
        :param internal_format: Represents the patron's desired book format.

        :return: a LoanInfo object.
        """
        raise NotImplementedException()


    def fulfill(self, patron, pin, licensepool, internal_format):
        """ Get the actual resource file to the patron.
        :return a FulfillmentInfo object.
        """
        raise NotImplementedException()


    def patron_activity(self, patron, pin):
        """ Return a patron's current checkouts and holds.
        """
        raise NotImplementedException()


    def place_hold(self, patron, pin, licensepool, notification_email_address):
        """Place a book on hold.

        :return: A HoldInfo object
        """
        raise NotImplementedException()


    def release_hold(self, patron, pin, licensepool):
        """Release a patron's hold on a book.

        :raises CannotReleaseHold: If there is an error communicating
        with the provider, or the provider refuses to release the hold for
        any reason.
        """
        raise NotImplementedException()


