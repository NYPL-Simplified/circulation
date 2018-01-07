from nose.tools import (
    set_trace,
    eq_,
    assert_raises,
)
import os
import json
import datetime
import re

from . import DatabaseTest
from core.model import (
    Collection,
    Credential,
    DataSource,
    DeliveryMechanism,
    Hold,
    Identifier,
    Loan,
    Representation,
    RightsStatus,
    get_one,
)
from api.odl import (
    ODLBibliographicImporter,
    ODLConsolidatedCopiesMonitor,
    ODLHoldReaper,
    MockODLWithConsolidatedCopiesAPI,
)
from api.circulation_exceptions import *
from core.util.http import BadResponseException

class BaseODLTest(object):
    base_path = os.path.split(__file__)[0]
    resource_path = os.path.join(base_path, "files", "odl")

    @classmethod
    def get_data(cls, filename):
        path = os.path.join(cls.resource_path, filename)
        return open(path).read()

class TestODLWithConsolidatedCopiesAPI(DatabaseTest, BaseODLTest):

    def setup(self):
        super(TestODLWithConsolidatedCopiesAPI, self).setup()
        self.collection = MockODLWithConsolidatedCopiesAPI.mock_collection(self._db)
        self.collection.external_integration.set_setting(
            Collection.DATA_SOURCE_NAME_SETTING,
            "Feedbooks"
        )
        self.api = MockODLWithConsolidatedCopiesAPI(self._db, self.collection)
        self.pool = self._licensepool(None, collection=self.collection)
        self.patron = self._patron()

    def test_get_license_status_document_success(self):
        # With a new loan.
        loan, ignore = self.pool.loan_to(self.patron)
        self.api.queue_response(200, content=json.dumps(dict(status="ready")))
        response = self.api.get_license_status_document(loan)
        requested_url = self.api.requests[0][0]

        expected_url_re = re.compile("(.*)\?id=(.*)&checkout_id=(.*)&patron_id=(.*)&expires=(.*)&notification_url=(.*)")
        match = expected_url_re.match(requested_url)
        assert match != None
        (base_url, id, checkout_id, patron_id, expires, notification_url) = match.groups()

        eq_("http://loan", base_url)
        eq_(self.pool.identifier.identifier, id)

        # The checkout id and patron id are random UUIDs.
        assert len(checkout_id) > 0
        assert len(patron_id) > 0

        # Loans expire in 21 days by default.
        now = datetime.datetime.utcnow()
        after_expiration = now + datetime.timedelta(days=23)
        expires = datetime.datetime.strptime(expires, "%Y-%m-%dT%H:%M:%S.%fZ")
        assert expires > now
        assert expires < after_expiration

        assert 'http://odl_notify' in notification_url
        assert 'library_short_name=%s' % self._default_library.short_name in notification_url
        assert 'loan_id=%s' % loan.id in notification_url

        # With an existing loan.
        loan, ignore = self.pool.loan_to(self.patron)
        loan.external_identifier = self._str

        self.api.queue_response(200, content=json.dumps(dict(status="active")))
        doc = self.api.get_license_status_document(loan)
        requested_url = self.api.requests[1][0]
        eq_(loan.external_identifier, requested_url)

    def test_get_license_status_document_errors(self):
        loan, ignore = self.pool.loan_to(self.patron)

        self.api.queue_response(200, content="not json")
        assert_raises(
            BadResponseException, self.api.get_license_status_document, loan,
        )

        self.api.queue_response(200, content=json.dumps(dict(status="unknown")))
        assert_raises(
            BadResponseException, self.api.get_license_status_document, loan,
        )

    def test_checkin_success(self):
        self.pool.licenses_available = 6
        loan, ignore = self.pool.loan_to(self.patron)
        loan.external_identifier = "http://loan/" + self._str
        loan.end = datetime.datetime.utcnow() + datetime.timedelta(days=3)

        lsd = json.dumps({
            "status": "ready",
            "links": {
                "return": {
                    "href": "http://return",
                },
            },
        })
        returned_lsd = json.dumps({
            "status": "returned",
        })

        self.api.queue_response(200, content=lsd)
        self.api.queue_response(200)
        self.api.queue_response(200, content=returned_lsd)
        self.api.checkin(self.patron, "pin", self.pool)
        eq_(3, len(self.api.requests))
        assert "http://loan" in self.api.requests[0][0]
        eq_("http://return", self.api.requests[1][0])
        assert "http://loan" in self.api.requests[2][0]

        # The pool's availability has increased, and the local loan has
        # been deleted.
        eq_(7, self.pool.licenses_available)
        eq_(0, self._db.query(Loan).count())

    def test_checkin_success_with_holds_queue(self):
        self.pool.licenses_available = 0
        loan, ignore = self.pool.loan_to(self.patron)
        loan.external_identifier = "http://loan/" + self._str
        loan.end = datetime.datetime.utcnow() + datetime.timedelta(days=3)

        patron_with_hold = self._patron()
        self.pool.patrons_in_hold_queue = 1
        hold, ignore = self.pool.on_hold_to(patron_with_hold, start=datetime.datetime.utcnow(), end=None, position=1)

        lsd = json.dumps({
            "status": "ready",
            "links": {
                "return": {
                    "href": "http://return",
                },
            },
        })
        returned_lsd = json.dumps({
            "status": "returned",
        })

        self.api.queue_response(200, content=lsd)
        self.api.queue_response(200)
        self.api.queue_response(200, content=returned_lsd)
        self.api.checkin(self.patron, "pin", self.pool)
        eq_(3, len(self.api.requests))
        assert "http://loan" in self.api.requests[0][0]
        eq_("http://return", self.api.requests[1][0])
        assert "http://loan" in self.api.requests[2][0]

        # The license is reserved for the next patron.
        eq_(0, self.pool.licenses_available)
        eq_(1, self.pool.licenses_reserved)
        eq_(1, self.pool.patrons_in_hold_queue)
        eq_(0, self._db.query(Loan).count())
        eq_(0, hold.position)

    def test_checkin_already_fulfilled(self):
        # The loan is already fulfilled. Attempting to check in
        # won't do anything, but won't raise an exception.
        self.pool.licenses_available = 6
        loan, ignore = self.pool.loan_to(self.patron)
        loan.external_identifier = self._str
        loan.end = datetime.datetime.utcnow() + datetime.timedelta(days=3)

        lsd = json.dumps({
            "status": "active",
        })

        self.api.queue_response(200, content=lsd)
        self.api.checkin(self.patron, "pin", self.pool)
        eq_(1, len(self.api.requests))
        eq_(6, self.pool.licenses_available)
        eq_(1, self._db.query(Loan).count())

    def test_checkin_not_checked_out(self):
        # Not checked out locally.
        assert_raises(
            NotCheckedOut, self.api.checkin,
            self.patron, "pin", self.pool,
        )

        # Not checked out according to the distributor.
        loan, ignore = self.pool.loan_to(self.patron)
        loan.external_identifier = self._str
        loan.end = datetime.datetime.utcnow() + datetime.timedelta(days=3)

        lsd = json.dumps({
            "status": "revoked",
        })

        self.api.queue_response(200, content=lsd)
        assert_raises(
            NotCheckedOut, self.api.checkin,
            self.patron, "pin", self.pool,
        )

    def test_checkin_cannot_return(self):
        # Not fulfilled yet, but no return link from the distributor.
        loan, ignore = self.pool.loan_to(self.patron)
        loan.external_identifier = self._str
        loan.end = datetime.datetime.utcnow() + datetime.timedelta(days=3)

        lsd = json.dumps({
            "status": "ready",
        })

        self.api.queue_response(200, content=lsd)
        assert_raises(
            CannotReturn, self.api.checkin,
            self.patron, "pin", self.pool,
        )

    def test_checkout_success(self):
        self.pool.licenses_available = 6
        loan_url = self._str
        lsd = json.dumps({
            "status": "ready",
            "potential_rights": {
                "end": "2017-10-21T11:12:13Z"
            },
            "links": {
                "self": { "href": loan_url }
            },
        })

        self.api.queue_response(200, content=lsd)
        loan = self.api.checkout(self.patron, "pin", self.pool, Representation.EPUB_MEDIA_TYPE)
        eq_(self.collection, loan.collection(self._db))
        eq_(self.pool.data_source.name, loan.data_source_name)
        eq_(self.pool.identifier.type, loan.identifier_type)
        eq_(self.pool.identifier.identifier, loan.identifier)
        assert loan.start_date > datetime.datetime.utcnow() - datetime.timedelta(minutes=1)
        assert loan.start_date < datetime.datetime.utcnow() + datetime.timedelta(minutes=1)
        eq_(datetime.datetime(2017, 10, 21, 11, 12, 13), loan.end_date)
        eq_(loan_url, loan.external_identifier)
        eq_(1, self._db.query(Loan).count())

        db_loan = self._db.query(Loan).one()
        eq_(self.pool, db_loan.license_pool)
        eq_(loan.start_date, db_loan.start)
        eq_(loan.end_date, db_loan.end)

        # The pool's availability has decreased.
        eq_(5, self.pool.licenses_available)

    def test_checkout_success_with_hold(self):
        self.pool.licenses_reserved = 1
        self.pool.on_hold_to(self.patron, start=datetime.datetime.utcnow() - datetime.timedelta(days=1), position=0)

        loan_url = self._str
        lsd = json.dumps({
            "status": "ready",
            "potential_rights": {
                "end": "2017-10-21T11:12:13Z"
            },
            "links": {
                "self": { "href": loan_url }
            },
        })

        self.api.queue_response(200, content=lsd)
        loan = self.api.checkout(self.patron, "pin", self.pool, Representation.EPUB_MEDIA_TYPE)
        eq_(self.collection, loan.collection(self._db))
        eq_(self.pool.data_source.name, loan.data_source_name)
        eq_(self.pool.identifier.type, loan.identifier_type)
        eq_(self.pool.identifier.identifier, loan.identifier)
        assert loan.start_date > datetime.datetime.utcnow() - datetime.timedelta(minutes=1)
        assert loan.start_date < datetime.datetime.utcnow() + datetime.timedelta(minutes=1)
        eq_(datetime.datetime(2017, 10, 21, 11, 12, 13), loan.end_date)
        eq_(loan_url, loan.external_identifier)
        eq_(1, self._db.query(Loan).count())

        eq_(0, self.pool.licenses_reserved)
        eq_(0, self._db.query(Hold).count())

    def test_checkout_already_checked_out(self):
        existing_loan, ignore = self.pool.loan_to(self.patron)
        existing_loan.external_identifier = self._str
        existing_loan.end = datetime.datetime.utcnow() + datetime.timedelta(days=3)

        assert_raises(
            AlreadyCheckedOut, self.api.checkout,
            self.patron, "pin", self.pool, Representation.EPUB_MEDIA_TYPE,
        )

        eq_(1, self._db.query(Loan).count())

    def test_checkout_expired_hold(self):
        self.pool.licenses_available = 0
        self.pool.licenses_reserved = 1
        yesterday = datetime.datetime.utcnow() - datetime.timedelta(days=1)
        hold, ignore = self.pool.on_hold_to(self.patron, start=yesterday, end=yesterday, position=0)

        assert_raises(
            NoAvailableCopies, self.api.checkout,
            self.patron, "pin", self.pool, Representation.EPUB_MEDIA_TYPE,
        )

    def test_checkout_no_available_copies(self):
        self.pool.licenses_available = 0

        assert_raises(
            NoAvailableCopies, self.api.checkout,
            self.patron, "pin", self.pool, Representation.EPUB_MEDIA_TYPE,
        )

        eq_(0, self._db.query(Loan).count())

    def test_checkout_no_licenses(self):
        self.pool.licenses_owned = 0

        assert_raises(
            NoLicenses, self.api.checkout,
            self.patron, "pin", self.pool, Representation.EPUB_MEDIA_TYPE,
        )

        eq_(0, self._db.query(Loan).count())

    def test_checkout_cannot_loan(self):
        lsd = json.dumps({
            "status": "revoked",
        })

        self.api.queue_response(200, content=lsd)
        assert_raises(
            CannotLoan, self.api.checkout,
            self.patron, "pin", self.pool, Representation.EPUB_MEDIA_TYPE,
        )

        eq_(0, self._db.query(Loan).count())

        # No external identifier.
        lsd = json.dumps({
            "status": "ready",
            "potential_rights": {
                "end": "2017-10-21T11:12:13Z"
            },
        })

        self.api.queue_response(200, content=lsd)
        assert_raises(
            CannotLoan, self.api.checkout,
            self.patron, "pin", self.pool, Representation.EPUB_MEDIA_TYPE,
        )

        eq_(0, self._db.query(Loan).count())

    def test_fulfill_success(self):
        loan, ignore = self.pool.loan_to(self.patron)
        loan.external_identifier = self._str
        loan.end = datetime.datetime.utcnow() + datetime.timedelta(days=3)

        lsd = json.dumps({
            "status": "ready",
            "potential_rights": {
                "end": "2017-10-21T11:12:13Z"
            },
            "links": {
                "license": {
                    "href": "http://license",
                    "type": DeliveryMechanism.ADOBE_DRM,
                },
            },
        })

        self.api.queue_response(200, content=lsd)
        fulfillment = self.api.fulfill(self.patron, "pin", self.pool, Representation.EPUB_MEDIA_TYPE)
        eq_(self.collection, fulfillment.collection(self._db))
        eq_(self.pool.data_source.name, fulfillment.data_source_name)
        eq_(self.pool.identifier.type, fulfillment.identifier_type)
        eq_(self.pool.identifier.identifier, fulfillment.identifier)
        eq_(datetime.datetime(2017, 10, 21, 11, 12, 13), fulfillment.content_expires)
        eq_("http://license", fulfillment.content_link)
        eq_(DeliveryMechanism.ADOBE_DRM, fulfillment.content_type)

    def test_fulfill_cannot_fulfill(self):
        self.pool.licenses_available = 6
        loan, ignore = self.pool.loan_to(self.patron)
        loan.external_identifier = self._str
        loan.end = datetime.datetime.utcnow() + datetime.timedelta(days=3)

        lsd = json.dumps({
            "status": "revoked",
        })

        self.api.queue_response(200, content=lsd)
        assert_raises(
            CannotFulfill, self.api.fulfill,
            self.patron, "pin", self.pool, Representation.EPUB_MEDIA_TYPE,
        )

        # The pool's availability has been updated and the local
        # loan has been deleted, since we found out the loan is
        # no longer active.
        eq_(7, self.pool.licenses_available)
        eq_(0, self._db.query(Loan).count())

    def test_update_hold_end_date(self):
        now = datetime.datetime.utcnow()
        tomorrow = now + datetime.timedelta(days=1)
        yesterday = now - datetime.timedelta(days=1)
        next_week = now + datetime.timedelta(days=7)
        last_week = now - datetime.timedelta(days=7)

        self.pool.licenses_reserved = 1

        hold, ignore = self.pool.on_hold_to(self.patron, start=now, position=0)

        # Set the reservation period and loan period.
        self.collection.external_integration.set_setting(
            Collection.DEFAULT_RESERVATION_PERIOD_KEY, 3
        )
        self.collection.external_integration.set_setting(
            Collection.EBOOK_LOAN_DURATION_KEY, 6
        )

        # A hold that's already reserved and has an end date doesn't change.
        hold.end = tomorrow
        self.api._update_hold_end_date(hold)
        eq_(tomorrow, hold.end)
        hold.end = yesterday
        self.api._update_hold_end_date(hold)
        eq_(yesterday, hold.end)

        # Updating a hold that's reserved but doesn't have an end date starts the
        # reservation period.
        hold.end = None
        self.api._update_hold_end_date(hold)
        assert hold.end < next_week
        assert hold.end > now

        # When there's a holds queue, the end date is the maximum time it could take for
        # a license to become available.
        
        # One copy, one loan, hold position 1.
        # The hold will be available as soon as the loan expires.
        self.pool.licenses_available = 0
        self.pool.licenses_reserved = 0
        self.pool.licenses_owned = 1
        loan, ignore = self.pool.loan_to(self._patron(), end=tomorrow)
        self.api._update_hold_end_date(hold)
        eq_(tomorrow, hold.end)

        # One copy, one loan, hold position 2.
        # The hold will be available after the loan expires + 1 cycle.
        first_hold, ignore = self.pool.on_hold_to(self._patron(), start=last_week)
        self.api._update_hold_end_date(hold)
        eq_(tomorrow + datetime.timedelta(days=9), hold.end)

        # Two copies, one loan, one reserved hold, hold position 2. 
        # The hold will be available after the loan expires.
        self.pool.licenses_reserved = 1
        self.pool.licenses_owned = 2
        self.api._update_hold_end_date(hold)
        eq_(tomorrow, hold.end)

        # Two copies, one loan, one reserved hold, hold position 3.
        # The hold will be available after the reserved hold is checked out
        # at the latest possible time and that loan expires.
        second_hold, ignore = self.pool.on_hold_to(self._patron(), start=yesterday)
        first_hold.end = next_week
        self.api._update_hold_end_date(hold)
        eq_(next_week + datetime.timedelta(days=6), hold.end)

        # One copy, no loans, one reserved hold, hold position 3.
        # The hold will be available after the reserved hold is checked out
        # at the latest possible time and that loan expires + 1 cycle.
        self._db.delete(loan)
        self.pool.licenses_owned = 1
        self.api._update_hold_end_date(hold)
        eq_(next_week + datetime.timedelta(days=15), hold.end)

        # One copy, no loans, one reserved hold, hold position 2.
        # The hold will be available after the reserved hold is checked out
        # at the latest possible time and that loan expires.
        self._db.delete(second_hold)
        self.pool.licenses_owned = 1
        self.api._update_hold_end_date(hold)
        eq_(next_week + datetime.timedelta(days=6), hold.end)

        self._db.delete(first_hold)

        # Ten copies, seven loans, three reserved holds, hold position 9.
        # The hold will be available after the sixth loan expires.
        self.pool.licenses_owned = 10
        for i in range(5):
            self.pool.loan_to(self._patron(), end=next_week)
        self.pool.loan_to(self._patron(), end=next_week + datetime.timedelta(days=1))
        self.pool.loan_to(self._patron(), end=next_week + datetime.timedelta(days=2))
        self.pool.licenses_reserved = 3
        for i in range(3):
            self.pool.on_hold_to(self._patron(), start=last_week + datetime.timedelta(days=i), end=next_week + datetime.timedelta(days=i), position=0)
        for i in range(5):
            self.pool.on_hold_to(self._patron(), start=yesterday)
        self.api._update_hold_end_date(hold)
        eq_(next_week + datetime.timedelta(days=1), hold.end)

        # Ten copies, seven loans, three reserved holds, hold position 12.
        # The hold will be available after the second reserved hold is checked
        # out and that loan expires.
        for i in range(3):
            self.pool.on_hold_to(self._patron(), start=yesterday)
        self.api._update_hold_end_date(hold)
        eq_(next_week + datetime.timedelta(days=7), hold.end)

        # Ten copies, seven loans, three reserved holds, hold position 29.
        # The hold will be available after the sixth loan expires + 2 cycles.
        for i in range(17):
            self.pool.on_hold_to(self._patron(), start=yesterday)
        self.api._update_hold_end_date(hold)
        eq_(next_week + datetime.timedelta(days=19), hold.end)

        # Ten copies, seven loans, three reserved holds, hold position 32.
        # The hold will be available after the second reserved hold is checked
        # out and that loan expires + 2 cycles.
        for i in range(3):
            self.pool.on_hold_to(self._patron(), start=yesterday)
        self.api._update_hold_end_date(hold)
        eq_(next_week + datetime.timedelta(days=25), hold.end)

    def test_update_hold_position(self):
        now = datetime.datetime.utcnow()
        yesterday = now - datetime.timedelta(days=1)
        tomorrow = now + datetime.timedelta(days=1)

        hold, ignore = self.pool.on_hold_to(self.patron, start=now)

        # When there are no other holds and no licenses reserved,
        # hold position is 1.
        self.api._update_hold_position(hold)
        eq_(1, hold.position)

        # When a license is reserved, position is 0.
        self.pool.licenses_reserved = 1
        self.api._update_hold_position(hold)
        eq_(0, hold.position)

        # If another hold has the reserved licenses, position is 2.
        self.pool.on_hold_to(self._patron(), start=yesterday)
        self.api._update_hold_position(hold)
        eq_(2, hold.position)

        # If another license is reserved, position goes back to 0.
        self.pool.licenses_reserved = 2
        self.api._update_hold_position(hold)
        eq_(0, hold.position)

        # If there's an earlier hold but it expired, it doesn't
        # affect the position.
        self.pool.on_hold_to(self._patron(), start=yesterday, end=yesterday, position=0)
        self.api._update_hold_position(hold)
        eq_(0, hold.position)

        # Hold position is after all earlier non-expired holds...
        for i in range(3):
            self.pool.on_hold_to(self._patron(), start=yesterday)
        self.api._update_hold_position(hold)
        eq_(5, hold.position)

        # and before any later holds.
        for i in range(2):
            self.pool.on_hold_to(self._patron(), start=tomorrow)
        self.api._update_hold_position(hold)
        eq_(5, hold.position)

    def test_update_hold_queue(self):
        self.collection.external_integration.set_setting(
            Collection.DEFAULT_RESERVATION_PERIOD_KEY, 3
        )

        # If there's no holds queue when we try to update the queue, it
        # will remove a reserved license and make it available instead.
        self.pool.licenses_available = 0
        self.pool.licenses_reserved = 1
        self.pool.patrons_in_hold_queue = 0
        self.api.update_hold_queue(self.pool)
        eq_(1, self.pool.licenses_available)
        eq_(0, self.pool.licenses_reserved)

        # If there are holds, a license will get reserved for the next hold
        # and its end date will be set.
        hold, ignore = self.pool.on_hold_to(self.patron, start=datetime.datetime.utcnow(), position=1)
        later_hold, ignore = self.pool.on_hold_to(self._patron(), start=datetime.datetime.utcnow() + datetime.timedelta(days=1), position=2)

        self.pool.licenses_available = 0
        self.pool.licenses_reserved = 1

        self.api.update_hold_queue(self.pool)

        # The pool's licenses didn't change.
        eq_(0, self.pool.licenses_available)
        eq_(1, self.pool.licenses_reserved)

        # But the first hold did.
        eq_(0, hold.position)
        assert hold.end - datetime.datetime.utcnow() - datetime.timedelta(days=3) < datetime.timedelta(hours=1)

        # The later hold is the same.
        eq_(2, later_hold.position)

        # Now there's a reserved hold. If we make another license reserved,
        # the later hold is also updated.
        self.pool.licenses_owned = 2
        self.pool.licenses_reserved = 2
        self.api.update_hold_queue(self.pool)

        eq_(0, later_hold.position)
        assert later_hold.end - datetime.datetime.utcnow() - datetime.timedelta(days=3) < datetime.timedelta(hours=1)

        # Now there are no more holds. If we make another license reserved,
        # it ends up being available.
        self.pool.licenses_owned = 3
        self.pool.licenses_reserved = 3
        self.api.update_hold_queue(self.pool)
        eq_(1, self.pool.licenses_available)
        eq_(2, self.pool.licenses_reserved)

    def test_place_hold_success(self):
        tomorrow = datetime.datetime.utcnow() + datetime.timedelta(days=1)
        self.pool.licenses_available = 0
        self.pool.loan_to(self._patron(), end=tomorrow)

        hold = self.api.place_hold(self.patron, "pin", self.pool, "notifications@librarysimplified.org")

        eq_(1, self.pool.patrons_in_hold_queue)
        eq_(self.collection, hold.collection(self._db))
        eq_(self.pool.data_source.name, hold.data_source_name)
        eq_(self.pool.identifier.type, hold.identifier_type)
        eq_(self.pool.identifier.identifier, hold.identifier)
        assert hold.start_date > datetime.datetime.utcnow() - datetime.timedelta(minutes=1)
        assert hold.start_date < datetime.datetime.utcnow() + datetime.timedelta(minutes=1)
        eq_(tomorrow, hold.end_date)
        eq_(1, hold.hold_position)
        eq_(1, self._db.query(Hold).count())

    def test_place_hold_already_on_hold(self):
        self.pool.on_hold_to(self.patron)
        assert_raises(
            AlreadyOnHold, self.api.place_hold,
            self.patron, "pin", self.pool, "notifications@librarysimplified.org",
        )

    def test_release_hold_success(self):
        self.pool.on_hold_to(self.patron, position=1)
        self.pool.licenses_available = 0
        self.pool.licenses_reserved = 0
        self.pool.patrons_in_hold_queue = 1

        eq_(True, self.api.release_hold(self.patron, "pin", self.pool))
        eq_(0, self.pool.licenses_available)
        eq_(0, self.pool.licenses_reserved)
        eq_(0, self.pool.patrons_in_hold_queue)
        eq_(0, self._db.query(Hold).count())

        self.pool.on_hold_to(self.patron, position=0)
        self.pool.licenses_available = 0
        self.pool.licenses_reserved = 1
        self.pool.patrons_in_hold_queue = 1

        eq_(True, self.api.release_hold(self.patron, "pin", self.pool))
        eq_(1, self.pool.licenses_available)
        eq_(0, self.pool.licenses_reserved)
        eq_(0, self.pool.patrons_in_hold_queue)
        eq_(0, self._db.query(Hold).count())

        self.pool.on_hold_to(self.patron, position=0)
        other_hold, ignore = self.pool.on_hold_to(self._patron(), position=2)
        self.pool.licenses_available = 0
        self.pool.licenses_reserved = 1
        self.pool.patrons_in_hold_queue = 2

        eq_(True, self.api.release_hold(self.patron, "pin", self.pool))
        eq_(0, self.pool.licenses_available)
        eq_(1, self.pool.licenses_reserved)
        eq_(1, self.pool.patrons_in_hold_queue)
        eq_(1, self._db.query(Hold).count())
        eq_(0, other_hold.position)

    def test_release_hold_not_on_hold(self):
        assert_raises(
            NotOnHold, self.api.release_hold,
            self.patron, "pin", self.pool,
        )

    def test_patron_activity(self):
        # No loans yet.
        eq_([], self.api.patron_activity(self.patron, "pin"))

        # One loan.
        loan, ignore = self.pool.loan_to(self.patron)
        loan.external_identifier = self._str
        loan.start = datetime.datetime.utcnow() - datetime.timedelta(days=1)
        loan.end = loan.start + datetime.timedelta(days=20)

        activity = self.api.patron_activity(self.patron, "pin")
        eq_(1, len(activity))
        eq_(self.collection, activity[0].collection(self._db))
        eq_(self.pool.data_source.name, activity[0].data_source_name)
        eq_(self.pool.identifier.type, activity[0].identifier_type)
        eq_(self.pool.identifier.identifier, activity[0].identifier)
        eq_(loan.start, activity[0].start_date)
        eq_(loan.end, activity[0].end_date)
        eq_(loan.external_identifier, activity[0].external_identifier)

        # Two loans.
        pool2 = self._licensepool(None, collection=self.collection)
        loan2, ignore = pool2.loan_to(self.patron)
        loan2.external_identifier = self._str
        loan2.start = datetime.datetime.utcnow() - datetime.timedelta(days=4)
        loan2.end = loan2.start + datetime.timedelta(days=14)

        activity = self.api.patron_activity(self.patron, "pin")
        eq_(2, len(activity))
        [l2, l1] = sorted(activity, key=lambda x: x.start_date)

        eq_(self.collection, l1.collection(self._db))
        eq_(self.pool.data_source.name, l1.data_source_name)
        eq_(self.pool.identifier.type, l1.identifier_type)
        eq_(self.pool.identifier.identifier, l1.identifier)
        eq_(loan.start, l1.start_date)
        eq_(loan.end, l1.end_date)
        eq_(loan.external_identifier, l1.external_identifier)

        eq_(self.collection, l2.collection(self._db))
        eq_(pool2.data_source.name, l2.data_source_name)
        eq_(pool2.identifier.type, l2.identifier_type)
        eq_(pool2.identifier.identifier, l2.identifier)
        eq_(loan2.start, l2.start_date)
        eq_(loan2.end, l2.end_date)
        eq_(loan2.external_identifier, l2.external_identifier)

        # If a loan is expired already, it's left out.
        loan2.end = datetime.datetime.utcnow() - datetime.timedelta(days=2)
        activity = self.api.patron_activity(self.patron, "pin")
        eq_(1, len(activity))
        eq_(self.pool.identifier.identifier, activity[0].identifier)

        # One hold.
        hold, ignore = pool2.on_hold_to(self.patron)
        hold.start = datetime.datetime.utcnow() - datetime.timedelta(days=2)
        hold.end = hold.start + datetime.timedelta(days=3)
        hold.position = 3
        activity = self.api.patron_activity(self.patron, "pin")
        eq_(2, len(activity))
        [h1, l1] = sorted(activity, key=lambda x: x.start_date)

        eq_(self.collection, h1.collection(self._db))
        eq_(pool2.data_source.name, h1.data_source_name)
        eq_(pool2.identifier.type, h1.identifier_type)
        eq_(pool2.identifier.identifier, h1.identifier)
        eq_(hold.start, h1.start_date)
        eq_(hold.end, h1.end_date)
        # Hold position was updated.
        eq_(1, h1.hold_position)
        eq_(1, hold.position)

        # If the hold is expired, it's deleted right away and the license
        # is made available again.
        pool2.licenses_available = 0
        pool2.licenses_reserved = 1
        hold.end = datetime.datetime.utcnow() - datetime.timedelta(days=1)
        hold.position = 0
        activity = self.api.patron_activity(self.patron, "pin")
        eq_(1, len(activity))
        eq_(0, self._db.query(Hold).count())
        eq_(1, pool2.licenses_available)
        eq_(0, pool2.licenses_reserved)

    def test_update_consolidated_copy(self):
        edition, pool = self._edition(
            with_license_pool=True,
            data_source_name="Feedbooks",
            identifier_type=Identifier.URI,
            collection=self.collection,
        )
        pool.licenses_owned = 3
        pool.licenses_available = 2

        consolidated_copy_info = dict(
            identifier=pool.identifier.identifier,
            licenses=11,
            available=6,
        )
        self.api.update_consolidated_copy(self._db, consolidated_copy_info)
        eq_(11, pool.licenses_owned)
        eq_(6, pool.licenses_available)

    def test_update_loan_still_active(self):
        self.pool.licenses_available = 6
        loan, ignore = self.pool.loan_to(self.patron)
        loan.external_identifier = self._str
        status_doc = {
            "status": "active",
        }

        self.api.update_loan(loan, status_doc)
        # Availability hasn't changed, and the loan still exists.
        eq_(6, self.pool.licenses_available)
        eq_(1, self._db.query(Loan).count())

    def test_update_loan_removes_loan(self):
        self.pool.licenses_available = 6
        loan, ignore = self.pool.loan_to(self.patron)
        loan.external_identifier = self._str
        status_doc = {
            "status": "cancelled",
        }

        self.api.update_loan(loan, status_doc)
        # Availability has increased, and the loan is gone.
        eq_(7, self.pool.licenses_available)
        eq_(0, self._db.query(Loan).count())

    def test_update_loan_removes_loan_with_hold_queue(self):
        self.pool.licenses_available = 0
        self.pool.licenses_reserved = 0
        self.pool.patrons_in_hold_queue = 1
        loan, ignore = self.pool.loan_to(self.patron)
        loan.external_identifier = self._str
        hold, ignore = self.pool.on_hold_to(self._patron(), position=1)
        status_doc = {
            "status": "cancelled",
        }

        self.api.update_loan(loan, status_doc)
        # The license is reserved for the next patron, and the loan is gone.
        eq_(0, self.pool.licenses_available)
        eq_(1, self.pool.licenses_reserved)
        eq_(0, hold.position)
        eq_(0, self._db.query(Loan).count())
        

class TestODLBibliographicImporter(DatabaseTest, BaseODLTest):

    def test_import(self):
        feed = self.get_data("feedbooks_bibliographic.atom")
        data_source = DataSource.lookup(self._db, "Feedbooks", autocreate=True)
        collection = MockODLWithConsolidatedCopiesAPI.mock_collection(self._db)
        collection.external_integration.set_setting(
            Collection.DATA_SOURCE_NAME_SETTING,
            data_source.name
        )

        class MockMetadataClient(object):
            def canonicalize_author_name(self, identifier, working_display_name):
                return working_display_name
        metadata_client = MockMetadataClient()
        importer = ODLBibliographicImporter(
            self._db, collection=collection,
            metadata_client=metadata_client,
        )

        imported_editions, imported_pools, imported_works, failures = (
            importer.import_from_feed(feed)
        )

        # This importer works the same as the base OPDSImporter, except that
        # it extracts format information from 'odl:license' tags and creates
        # LicensePoolDeliveryMechanisms.

        # The importer created 4 editions, pools, and works.
        eq_(4, len(imported_editions))
        eq_(4, len(imported_pools))
        eq_(4, len(imported_works))

        [canadianity, warrior, blazing, midnight] = sorted(imported_editions, key=lambda x: x.title)
        eq_("The Blazing World", blazing.title)
        eq_("Sun Warrior", warrior.title)
        eq_("Canadianity", canadianity.title)
        eq_("The Midnight Dance", midnight.title)

        # This book is open access and has no 'odl:license' tag.
        [blazing_pool] = [p for p in imported_pools if p.identifier == blazing.primary_identifier]
        eq_(True, blazing_pool.open_access)
        [lpdm] = blazing_pool.delivery_mechanisms
        eq_(Representation.EPUB_MEDIA_TYPE, lpdm.delivery_mechanism.content_type)
        eq_(DeliveryMechanism.NO_DRM, lpdm.delivery_mechanism.drm_scheme)

        # This book has a single 'odl:license' tag.
        [warrior_pool] = [p for p in imported_pools if p.identifier == warrior.primary_identifier]
        eq_(False, warrior_pool.open_access)
        [lpdm] = warrior_pool.delivery_mechanisms
        eq_(Representation.EPUB_MEDIA_TYPE, lpdm.delivery_mechanism.content_type)
        eq_(DeliveryMechanism.ADOBE_DRM, lpdm.delivery_mechanism.drm_scheme)
        eq_(RightsStatus.IN_COPYRIGHT, lpdm.rights_status.uri)

        # This book has two 'odl:license' tags for the same format and drm scheme
        # (this happens if the library purchases two copies).
        [canadianity_pool] = [p for p in imported_pools if p.identifier == canadianity.primary_identifier]
        eq_(False, canadianity_pool.open_access)
        [lpdm] = canadianity_pool.delivery_mechanisms
        eq_(Representation.EPUB_MEDIA_TYPE, lpdm.delivery_mechanism.content_type)
        eq_(DeliveryMechanism.ADOBE_DRM, lpdm.delivery_mechanism.drm_scheme)
        eq_(RightsStatus.IN_COPYRIGHT, lpdm.rights_status.uri)

        # This book has two 'odl:license' tags, and they have different formats.
        [midnight_pool] = [p for p in imported_pools if p.identifier == midnight.primary_identifier]
        eq_(False, midnight_pool.open_access)
        lpdms = midnight_pool.delivery_mechanisms
        eq_(2, lpdms.count())
        eq_(set([Representation.EPUB_MEDIA_TYPE, Representation.PDF_MEDIA_TYPE]),
            set([lpdm.delivery_mechanism.content_type for lpdm in lpdms]))
        eq_([DeliveryMechanism.ADOBE_DRM, DeliveryMechanism.ADOBE_DRM],
            [lpdm.delivery_mechanism.drm_scheme for lpdm in lpdms])
        eq_([RightsStatus.IN_COPYRIGHT, RightsStatus.IN_COPYRIGHT],
            [lpdm.rights_status.uri for lpdm in lpdms])

class TestODLConsolidatedCopiesMonitor(DatabaseTest, BaseODLTest):

    def test_run_once(self):
        data_source = DataSource.lookup(self._db, "Feedbooks", autocreate=True)
        collection = MockODLWithConsolidatedCopiesAPI.mock_collection(self._db)
        collection.external_integration.set_setting(
            Collection.DATA_SOURCE_NAME_SETTING,
            data_source.name
        )
        api = MockODLWithConsolidatedCopiesAPI(self._db, collection)
        monitor = ODLConsolidatedCopiesMonitor(self._db, collection, api=api)

        # Two pools already exist and will be updated by the monitor.
        edition1, pool1 = self._edition(
            with_license_pool=True,
            identifier_type=Identifier.URI,
            collection=collection,
            data_source_name=data_source.name,
        )
        pool1.licenses_owned = 7
        pool1.licenses_available = 7
        edition2, pool2 = self._edition(
            with_license_pool=True,
            identifier_type=Identifier.URI,
            collection=collection,
            data_source_name=data_source.name,
        )
        pool2.licenses_owned = 7
        pool2.licenses_available = 7

        # One of the identifiers also has a pool in a different collection.
        pool_from_other_collection = self._licensepool(edition1, collection=self._default_collection)
        pool_from_other_collection.licenses_owned = 6
        pool_from_other_collection.licenses_available = 5

        # One additional identifier will appear in the feed, but doesn't have a pool yet.
        identifier = self._identifier(identifier_type=Identifier.URI)

        page1 = {
            "links": [
                { "href": "/page2",
                  "type": "application/json",
                  "rel": "next",
                },
            ],
            "copies": [
                { "identifier": pool1.identifier.identifier,
                  "licenses": 1,
                  "available": 1,
                },
                { "identifier": identifier.identifier,
                  "licenses": 4,
                  "available": 0,
                }
            ],
        }
        page2 = {
            "links": [],
            "copies": [
                { "identifier": pool2.identifier.identifier,
                  "licenses": 10,
                  "available": 4,
                },
            ]
        }
        api.queue_response(200, content=json.dumps(page1))
        api.queue_response(200, content=json.dumps(page2))

        monitor.run_once(None, None)

        # The monitor got both pages of the feed.
        eq_(2, len(api.requests))
        eq_("http://copies", api.requests[0][0])
        eq_("http://copies/page2", api.requests[1][0])

        # The two existing pools were updated.
        eq_(1, pool1.licenses_owned)
        eq_(1, pool1.licenses_available)
        eq_(10, pool2.licenses_owned)
        eq_(4, pool2.licenses_available)

        # The pool from the other collection wasn't changed.
        eq_(6, pool_from_other_collection.licenses_owned)
        eq_(5, pool_from_other_collection.licenses_available)

        # The new identifier got a pool even though we don't
        # have other information about it yet.
        eq_(1, len(identifier.licensed_through))
        eq_(collection, identifier.licensed_through[0].collection)
        eq_(4, identifier.licensed_through[0].licenses_owned)
        eq_(0, identifier.licensed_through[0].licenses_available)

        # If the monitor is run with a start time, it will subtract 5 minutes
        # and add a date to the end of the url.
        api.queue_response(200, content=json.dumps(page2))
        yesterday = datetime.datetime.utcnow() - datetime.timedelta(days=1)
        monitor.run_once(yesterday, None)

        eq_(3, len(api.requests))
        expected_time = yesterday - datetime.timedelta(minutes=5)
        expected_url = "http://copies?since=%sZ" % expected_time.isoformat()
        eq_(expected_url, api.requests[2][0])

class TestODLHoldReaper(DatabaseTest, BaseODLTest):

    def test_run_once(self):
        data_source = DataSource.lookup(self._db, "Feedbooks", autocreate=True)
        collection = MockODLWithConsolidatedCopiesAPI.mock_collection(self._db)
        collection.external_integration.set_setting(
            Collection.DATA_SOURCE_NAME_SETTING,
            data_source.name
        )
        api = MockODLWithConsolidatedCopiesAPI(self._db, collection)
        reaper = ODLHoldReaper(self._db, collection, api=api)

        now = datetime.datetime.utcnow()
        yesterday = now - datetime.timedelta(days=1)

        pool = self._licensepool(None, collection=collection)
        pool.licenses_owned = 1
        pool.licenses_available = 0
        pool.licenses_reserved = 1
        expired_hold, ignore = pool.on_hold_to(self._patron(), end=yesterday, position=0)
        current_hold, ignore = pool.on_hold_to(self._patron(), position=2)

        reaper.run_once(None, None)

        # The expired hold has been deleted and the other hold has been updated.
        eq_(1, self._db.query(Hold).count())
        eq_([current_hold], self._db.query(Hold).all())
        eq_(0, current_hold.position)
