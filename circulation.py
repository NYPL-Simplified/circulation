from nose.tools import set_trace
from circulation_exceptions import *
import datetime
from threading import Thread

from core.model import (
    get_one,
    DataSource,
    Loan,
    Hold,
)

class CirculationInfo(object):
    def fd(self, d):
        # Stupid method to format a date
        if not d:
            return None
        else:
            return datetime.datetime.strftime(d, "%Y/%m/%d %H:%M:%S")

class FulfillmentInfo(CirculationInfo):

    """A record of an attempt to fulfil a loan."""

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

    """A record of a loan."""

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

    """A record of a hold."""

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

    def __init__(self, _db, overdrive, threem, axis):
        self._db = _db
        self.overdrive = overdrive
        self.threem = threem
        self.axis = axis
        self.apis = [overdrive, threem, axis]

    def api_for_license_pool(self, licensepool):
        """Find the API to use for the given license pool."""
        if licensepool.data_source.name==DataSource.OVERDRIVE:
            api = self.overdrive
            possible_formats = ["ebook-epub-adobe", "ebook-epub-open"]
        elif licensepool.data_source.name==DataSource.THREEM:
            api = self.threem
            possible_formats = [None]
        elif licensepool.data_source.name==DataSource.AXIS_360:
            api = self.axis
            possible_formats = api.allowable_formats
        else:
            return None, None

        return api, possible_formats

    def borrow(self, patron, pin, licensepool, hold_notification_email):
        """Either borrow a book or put it on hold. If the book is borrowed,
        also fulfill the loan.
        
        :return: A 4-tuple (`Loan`, `Hold`, `FulfillmentInfo`,
        `is_new`). Either `Loan` or `Hold` must be None, but not
        both. If `Loan` is present, `FulfillmentInfo` must also be
        present.
        """
        now = datetime.datetime.utcnow()

        if licensepool.open_access:
            # We can fulfill open-access content ourselves.
            best_pool, best_link = licensepool.best_license_link
            if not best_link:
                raise NoOpenAccessDownload()
            now = datetime.datetime.utcnow()
            __transaction = self._db.begin_nested()
            loan, is_new = licensepool.loan_to(patron, start=now, end=None)
            __transaction.commit()
            fulfillment = self.fulfill_open_access(licensepool, best_link)
            return loan, None, fulfillment, is_new

        # Okay, it's not an open-access book. This means we need to go
        # to an external service to get the book. 
        #
        # This also means that our internal model of whether this book
        # is currently on loan or on hold might be wrong.
        api, possible_formats = self.api_for_license_pool(licensepool)
    
        content_link = content_expires = None

        # First, try to check out the book.
        format_to_use = possible_formats[0]
        loan_info = None
        try:
            loan_info = api.checkout(
                 patron, pin, licensepool,
                 format_type=format_to_use)
        except NoAvailableCopies:
            # That's fine, we'll just place a hold.
            pass

        if loan_info:
            # We successfuly secured a loan.  Now create it in our
            # database.
            __transaction = self._db.begin_nested()
            loan, is_new = licensepool.loan_to(
                patron, start=loan_info.start_date or now,
                end=loan_info.end_date)
            existing_hold = get_one(
                self._db, Hold, patron=patron, license_pool=licensepool,
                on_multiple='interchangeable'
            )
            if existing_hold:
                # The book was on hold, and now we have a loan.
                # Delete the record of the hold.
                self._db.delete(existing_hold)
            __transaction.commit()
            if loan_info.fulfillment_info:
                fulfillment = loan_info.fulfillment_info
            else:
                # The checkout operation did not get us fulfillment
                # information. We must fulfill as a separate step.
                fulfillment = self.fulfill(
                    patron, pin, licensepool, format_to_use)
            return loan, None, fulfillment, is_new

        # Checking out a book didn't work, so let's try putting
        # the book on hold.
        hold_info = api.place_hold(
            patron, pin, licensepool, format_to_use, 
            hold_notification_email)

        # It's pretty rare that we'd go from having a loan for a book
        # to needing to put it on hold, but we do check for that case.
        existing_loan = get_one(
             self._db, Loan, patron=patron, license_pool=licensepool,
             on_multiple='interchangeable'
        )

        __transaction = self._db.begin_nested()
        hold, is_new = licensepool.on_hold_to(
            patron,
            hold_info.start_date or now,
            hold_info.end_date, 
            hold_info.hold_position
        )
        if existing_loan:
            self._db.delete(existing_loan)
        __transaction.commit()
        return None, hold, None, is_new

    def fulfill(self, patron, pin, licensepool):
        """Fulfil a book that a patron has checked out.

        :return: A FulfillmentInfo object.
        """

        # The patron must have a loan for this book. We'll try
        # fulfilling it even if the loan has expired--they may have
        # renewed it out-of-band.
        #loan = get_one(self._db, Loan, patron=patron, license_pool=licensepool)
        #if not loan:
        #    raise NoActiveLoan()

        fulfillment = None
        if licensepool.open_access:
            fulfillment = self.fulfill_open_access(licensepool)
        else:
            api, possible_formats = self.api_for_license_pool(licensepool)
            for f in possible_formats:
                fulfillment = api.fulfill(patron, pin, licensepool, f)
                if fulfillment and (
                        fulfillment.content_link or fulfillment.content):
                    break
            else:
                raise NoAcceptableFormat()
        return fulfillment

    def fulfill_open_access(self, licensepool, cached_best_link=None):
        if cached_best_link:
            best_pool, best_link = licensepool, cached_best_link
        else:
            best_pool, best_link = licensepool.best_license_link
        if not best_link:
            raise NoOpenAccessDownload()

        r = best_link.representation
        if r.url:
            content_link = r.url

        media_type = best_link.representation.media_type
        return FulfillmentInfo(
            identifier_type=licensepool.identifier.type,
            identifier=licensepool.identifier.identifier,
            content_link=content_link, content_type=media_type, content=None, 
            content_expires=None)

    def revoke_loan(self, patron, pin, licensepool):
        """Revoke a patron's loan for a book."""
        loan = get_one(
            self._db, Loan, patron=patron, license_pool=licensepool,
            on_multiple='interchangeable'
        )
        if loan:
            __transaction = self._db.begin_nested()
            self._db.delete(loan)
            __transaction.commit()
        if not licensepool.open_access:
            api, possible_formats = self.api_for_license_pool(licensepool)
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
            api, possible_formats = self.api_for_license_pool(licensepool)
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
        return True

    def patron_activity(self, patron, pin):
        """Return a record of the patron's current activity
        vis-a-vis all data sources.

        We check each data source in a separate thread for speed.

        :return: A consolidated list of `HoldInfo` and `LoanInfo` objects.
        """
        class PatronActivityThread(Thread):
            def __init__(self, api, patron, pin):
                self.api = api
                self.patron = patron
                self.pin = pin
                self.activity = None
                super(PatronActivityThread, self).__init__()

            def run(self):
                self.activity = self.api.patron_activity(
                    self.patron, self.pin)

        threads = []
        import time
        for api in self.apis:
            thread = PatronActivityThread(api, patron, pin)
            threads.append(thread)
        for thread in threads:
            thread.start()
            thread.join()
        info_objects = []
        for thread in threads:
            if thread.activity:
                info_objects.extend(list(thread.activity))
        return info_objects

