from nose.tools import (
    set_trace,
    eq_,
    assert_raises,
)
import os
import json
import datetime
import re
import base64
import urllib.request, urllib.parse, urllib.error
import urllib.parse

from . import DatabaseTest
from core.metadata_layer import TimestampData
from core.model import (
    Collection,
    ConfigurationSetting,
    Credential,
    DataSource,
    DeliveryMechanism,
    Edition,
    ExternalIntegration,
    Hold,
    Hyperlink,
    Identifier,
    Loan,
    MediaTypes,
    Representation,
    RightsStatus,
    get_one,
)
from api.odl import (
    ODLImporter,
    ODLHoldReaper,
    MockODLAPI,
    SharedODLAPI,
    MockSharedODLAPI,
    SharedODLImporter,
)
from api.circulation_exceptions import *
from core.util.http import (
    BadResponseException,
    RemoteIntegrationException,
)

class BaseODLTest(object):
    base_path = os.path.split(__file__)[0]
    resource_path = os.path.join(base_path, "files", "odl")

    @classmethod
    def get_data(cls, filename):
        path = os.path.join(cls.resource_path, filename)
        return open(path).read()

class TestODLAPI(DatabaseTest, BaseODLTest):

    def setup(self):
        super(TestODLAPI, self).setup()
        self.collection = MockODLAPI.mock_collection(self._db)
        self.collection.external_integration.set_setting(
            Collection.DATA_SOURCE_NAME_SETTING,
            "Feedbooks"
        )
        self.api = MockODLAPI(self._db, self.collection)
        self.work = self._work(with_license_pool=True, collection=self.collection)
        self.pool = self.work.license_pools[0]
        self.license = self._license(
            self.pool, checkout_url="https://loan.feedbooks.net/loan/get/{?id,checkout_id,expires,patron_id,notification_url}",
            concurrent_checkouts=1,
        )
        self.patron = self._patron()
        self.client = self._integration_client()

    def test_get_license_status_document_success(self):
        # With a new loan.
        loan, ignore = self.license.loan_to(self.patron)
        self.api.queue_response(200, content=json.dumps(dict(status="ready")))
        response = self.api.get_license_status_document(loan)
        requested_url = self.api.requests[0][0]

        parsed = urllib.parse.urlparse(requested_url)
        eq_("https", parsed.scheme)
        eq_("loan.feedbooks.net", parsed.netloc)
        params = urllib.parse.parse_qs(parsed.query)

        eq_(self.license.identifier, params.get("id")[0])

        # The checkout id and patron id are random UUIDs.
        checkout_id = params.get("checkout_id")[0]
        assert len(checkout_id) > 0
        patron_id = params.get("patron_id")[0]
        assert len(patron_id) > 0

        # Loans expire in 21 days by default.
        now = datetime.datetime.utcnow()
        after_expiration = now + datetime.timedelta(days=23)
        expires = urllib.parse.unquote_plus(params.get("expires")[0])
        expires = datetime.datetime.strptime(expires, "%Y-%m-%dT%H:%M:%S.%fZ")
        assert expires > now
        assert expires < after_expiration

        notification_url = urllib.parse.unquote_plus(params.get("notification_url")[0])
        eq_("http://odl_notify?loan_id=%s&library_short_name=%s" % (loan.id, self._default_library.short_name),
            notification_url)

        # With an existing loan.
        loan, ignore = self.license.loan_to(self.patron)
        loan.external_identifier = self._str

        self.api.queue_response(200, content=json.dumps(dict(status="active")))
        doc = self.api.get_license_status_document(loan)
        requested_url = self.api.requests[1][0]
        eq_(loan.external_identifier, requested_url)

    def test_get_license_status_document_errors(self):
        loan, ignore = self.license.loan_to(self.patron)

        self.api.queue_response(200, content="not json")
        assert_raises(
            BadResponseException, self.api.get_license_status_document, loan,
        )

        self.api.queue_response(200, content=json.dumps(dict(status="unknown")))
        assert_raises(
            BadResponseException, self.api.get_license_status_document, loan,
        )

    def test_checkin_success(self):
        # A patron has a copy of this book checked out.
        self.pool.licenses_owned = 7
        self.pool.licenses_available = 6
        loan, ignore = self.license.loan_to(self.patron)
        loan.external_identifier = "http://loan/" + self._str
        loan.end = datetime.datetime.utcnow() + datetime.timedelta(days=3)

        # The patron returns the book successfully.
        lsd = json.dumps({
            "status": "ready",
            "links": [{
                "rel": "return",
                "href": "http://return",
            }],
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
        # A patron has the only copy of this book checked out.
        self.pool.licenses_owned = 1
        self.pool.licenses_available = 0
        loan, ignore = self.license.loan_to(self.patron)
        loan.external_identifier = "http://loan/" + self._str
        loan.end = datetime.datetime.utcnow() + datetime.timedelta(days=3)

        # Another patron has the book on hold.
        patron_with_hold = self._patron()
        self.pool.patrons_in_hold_queue = 1
        hold, ignore = self.pool.on_hold_to(patron_with_hold, start=datetime.datetime.utcnow(), end=None, position=1)

        # The first patron returns the book successfully.
        lsd = json.dumps({
            "status": "ready",
            "links": [{
                "rel": "return",
                "href": "http://return",
            }],
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

        # Now the license is reserved for the next patron.
        eq_(0, self.pool.licenses_available)
        eq_(1, self.pool.licenses_reserved)
        eq_(1, self.pool.patrons_in_hold_queue)
        eq_(0, self._db.query(Loan).count())
        eq_(0, hold.position)

    def test_checkin_already_fulfilled(self):
        # The loan is already fulfilled.
        self.pool.licenses_owned = 7
        self.pool.licenses_available = 6
        loan, ignore = self.license.loan_to(self.patron)
        loan.external_identifier = self._str
        loan.end = datetime.datetime.utcnow() + datetime.timedelta(days=3)

        lsd = json.dumps({
            "status": "active",
        })

        self.api.queue_response(200, content=lsd)
        # Checking in the book silently does nothing.
        self.api.checkin(self.patron, "pinn", self.pool)
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
        loan, ignore = self.license.loan_to(self.patron)
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
        loan, ignore = self.license.loan_to(self.patron)
        loan.external_identifier = self._str
        loan.end = datetime.datetime.utcnow() + datetime.timedelta(days=3)

        lsd = json.dumps({
            "status": "ready",
        })

        self.api.queue_response(200, content=lsd)
        # Checking in silently does nothing.
        self.api.checkin(self.patron, "pin", self.pool)

        # If the return link doesn't change the status, it still
        # silently ignores the problem.
        lsd = json.dumps({
            "status": "ready",
            "links": [{
                "rel": "return",
                "href": "http://return",
            }],
        })

        self.api.queue_response(200, content=lsd)
        self.api.queue_response(200, content="Deleted")
        self.api.queue_response(200, content=lsd)
        self.api.checkin(self.patron, "pin", self.pool)

    def test_checkout_success(self):
        # This book is available to check out.
        self.pool.licenses_owned = 6
        self.pool.licenses_available = 6
        self.license.concurrent_checkouts = 6
        self.license.remaining_checkouts = 30

        # A patron checks out the book successfully.
        loan_url = self._str
        lsd = json.dumps({
            "status": "ready",
            "potential_rights": {
                "end": "3017-10-21T11:12:13Z"
            },
            "links": [{
                "rel": "self",
                "href": loan_url,
            }],
        })

        self.api.queue_response(200, content=lsd)
        loan = self.api.checkout(self.patron, "pin", self.pool, Representation.EPUB_MEDIA_TYPE)
        eq_(self.collection, loan.collection(self._db))
        eq_(self.pool.data_source.name, loan.data_source_name)
        eq_(self.pool.identifier.type, loan.identifier_type)
        eq_(self.pool.identifier.identifier, loan.identifier)
        assert loan.start_date > datetime.datetime.utcnow() - datetime.timedelta(minutes=1)
        assert loan.start_date < datetime.datetime.utcnow() + datetime.timedelta(minutes=1)
        eq_(datetime.datetime(3017, 10, 21, 11, 12, 13), loan.end_date)
        eq_(loan_url, loan.external_identifier)
        eq_(1, self._db.query(Loan).count())

        # Now the patron has a loan in the database that matches the LoanInfo
        # returned by the API.
        db_loan = self._db.query(Loan).one()
        eq_(self.pool, db_loan.license_pool)
        eq_(self.license, db_loan.license)
        eq_(loan.start_date, db_loan.start)
        eq_(loan.end_date, db_loan.end)

        # The pool's availability and the license's remaining checkouts have decreased.
        eq_(5, self.pool.licenses_available)
        eq_(29, self.license.remaining_checkouts)

    def test_checkout_success_with_hold(self):
        # A patron has this book on hold, and the book just became available to check out.
        self.pool.licenses_owned = 1
        self.pool.licenses_available = 0
        self.pool.licenses_reserved = 1
        self.pool.patrons_in_hold_queue = 1
        self.license.remaining_checkouts = 5
        self.pool.on_hold_to(self.patron, start=datetime.datetime.utcnow() - datetime.timedelta(days=1), position=0)

        # The patron checks out the book.
        loan_url = self._str
        lsd = json.dumps({
            "status": "ready",
            "potential_rights": {
                "end": "3017-10-21T11:12:13Z"
            },
            "links": [{
                "rel": "self",
                "href": loan_url,
            }],
        })

        self.api.queue_response(200, content=lsd)

        # The patron gets a loan successfully.
        loan = self.api.checkout(self.patron, "pin", self.pool, Representation.EPUB_MEDIA_TYPE)
        eq_(self.collection, loan.collection(self._db))
        eq_(self.pool.data_source.name, loan.data_source_name)
        eq_(self.pool.identifier.type, loan.identifier_type)
        eq_(self.pool.identifier.identifier, loan.identifier)
        assert loan.start_date > datetime.datetime.utcnow() - datetime.timedelta(minutes=1)
        assert loan.start_date < datetime.datetime.utcnow() + datetime.timedelta(minutes=1)
        eq_(datetime.datetime(3017, 10, 21, 11, 12, 13), loan.end_date)
        eq_(loan_url, loan.external_identifier)
        eq_(1, self._db.query(Loan).count())

        db_loan = self._db.query(Loan).one()
        eq_(self.pool, db_loan.license_pool)
        eq_(self.license, db_loan.license)
        eq_(4, self.license.remaining_checkouts)

        # The book is no longer reserved for the patron, and the hold has been deleted.
        eq_(0, self.pool.licenses_reserved)
        eq_(0, self.pool.licenses_available)
        eq_(0, self.pool.patrons_in_hold_queue)
        eq_(0, self._db.query(Hold).count())

    def test_checkout_already_checked_out(self):
        self.pool.licenses_owned = 2
        self.pool.licenses_available = 1
        existing_loan, ignore = self.license.loan_to(self.patron)
        existing_loan.external_identifier = self._str
        existing_loan.end = datetime.datetime.utcnow() + datetime.timedelta(days=3)

        assert_raises(
            AlreadyCheckedOut, self.api.checkout,
            self.patron, "pin", self.pool, Representation.EPUB_MEDIA_TYPE,
        )

        eq_(1, self._db.query(Loan).count())

    def test_checkout_expired_hold(self):
        # The patron was at the beginning of the hold queue, but the hold already expired.
        self.pool.licenses_owned = 1
        yesterday = datetime.datetime.utcnow() - datetime.timedelta(days=1)
        hold, ignore = self.pool.on_hold_to(self.patron, start=yesterday, end=yesterday, position=0)
        other_hold, ignore = self.pool.on_hold_to(self._patron(), start=datetime.datetime.utcnow())

        assert_raises(
            NoAvailableCopies, self.api.checkout,
            self.patron, "pin", self.pool, Representation.EPUB_MEDIA_TYPE,
        )

    def test_checkout_no_available_copies(self):
        # A different patron has the only copy checked out.
        self.pool.licenses_owned = 1
        self.pool.licenses_available = 0
        existing_loan, ignore = self.license.loan_to(self._patron())

        assert_raises(
            NoAvailableCopies, self.api.checkout,
            self.patron, "pin", self.pool, Representation.EPUB_MEDIA_TYPE,
        )

        eq_(1, self._db.query(Loan).count())

        self._db.delete(existing_loan)

        now = datetime.datetime.utcnow()
        yesterday = now - datetime.timedelta(days=1)
        last_week = now - datetime.timedelta(weeks=1)

        # A different patron has the only copy reserved.
        other_patron_hold, ignore = self.pool.on_hold_to(self._patron(), position=0, start=last_week)

        assert_raises(
            NoAvailableCopies, self.api.checkout,
            self.patron, "pin", self.pool, Representation.EPUB_MEDIA_TYPE,
        )

        eq_(0, self._db.query(Loan).count())

        # The patron has a hold, but another patron is ahead in the holds queue.
        hold, ignore = self.pool.on_hold_to(self._patron(), position=1, start=yesterday)

        assert_raises(
            NoAvailableCopies, self.api.checkout,
            self.patron, "pin", self.pool, Representation.EPUB_MEDIA_TYPE,
        )

        eq_(0, self._db.query(Loan).count())

        # The patron has the first hold, but it's expired.
        hold.start = last_week - datetime.timedelta(days=1)
        hold.end = yesterday

        assert_raises(
            NoAvailableCopies, self.api.checkout,
            self.patron, "pin", self.pool, Representation.EPUB_MEDIA_TYPE,
        )

        eq_(0, self._db.query(Loan).count())


    def test_checkout_no_licenses(self):
        self.pool.licenses_owned = 0
        self.license.remaining_checkouts = 0

        assert_raises(
            NoLicenses, self.api.checkout,
            self.patron, "pin", self.pool, Representation.EPUB_MEDIA_TYPE,
        )

        eq_(0, self._db.query(Loan).count())

    def test_checkout_when_all_licenses_expired(self):
        # license expired by expiration date
        self.pool.licenses_owned = 1
        self.pool.licenses_available = 1
        self.license.remaining_checkouts = 1
        self.license.expires = datetime.datetime.utcnow() - datetime.timedelta(weeks=1)

        assert_raises(
            NoLicenses, self.api.checkout,
            self.patron, "pin", self.pool, Representation.EPUB_MEDIA_TYPE,
        )

        # license expired by no remaining checkouts
        self.pool.licenses_owned = 1
        self.pool.licenses_available = 1
        self.license.remaining_checkouts = 0
        self.license.expires = datetime.datetime.utcnow() + datetime.timedelta(weeks=1)

        assert_raises(
            NoLicenses, self.api.checkout,
            self.patron, "pin", self.pool, Representation.EPUB_MEDIA_TYPE,
        )

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

    def test_fulfill_success_license(self):
        # Fulfill a loan in a way that gives access to a license file.
        loan, ignore = self.license.loan_to(self.patron)
        loan.external_identifier = self._str
        loan.end = datetime.datetime.utcnow() + datetime.timedelta(days=3)

        lsd = json.dumps({
            "status": "ready",
            "potential_rights": {
                "end": "2017-10-21T11:12:13Z"
            },
            "links": [{
                "rel": "license",
                "href": "http://acsm",
                "type": DeliveryMechanism.ADOBE_DRM,
            }],
        })

        self.api.queue_response(200, content=lsd)
        fulfillment = self.api.fulfill(self.patron, "pin", self.pool, Representation.EPUB_MEDIA_TYPE)
        eq_(self.collection, fulfillment.collection(self._db))
        eq_(self.pool.data_source.name, fulfillment.data_source_name)
        eq_(self.pool.identifier.type, fulfillment.identifier_type)
        eq_(self.pool.identifier.identifier, fulfillment.identifier)
        eq_(datetime.datetime(2017, 10, 21, 11, 12, 13), fulfillment.content_expires)
        eq_("http://acsm", fulfillment.content_link)
        eq_(DeliveryMechanism.ADOBE_DRM, fulfillment.content_type)

    def test_fulfill_success_manifest(self):
        # Fulfill a loan in a way that gives access to a manifest
        # file.
        loan, ignore = self.license.loan_to(self.patron)
        loan.external_identifier = self._str
        loan.end = datetime.datetime.utcnow() + datetime.timedelta(days=3)

        audiobook = MediaTypes.AUDIOBOOK_MANIFEST_MEDIA_TYPE

        lsd = json.dumps({
            "status": "ready",
            "potential_rights": {
                "end": "2017-10-21T11:12:13Z"
            },
            "links": [{
                "rel": "manifest",
                "href": "http://manifest",
                "type": audiobook,
            }],
        })

        self.api.queue_response(200, content=lsd)
        fulfillment = self.api.fulfill(self.patron, "pin", self.pool, audiobook)
        eq_(self.collection, fulfillment.collection(self._db))
        eq_(self.pool.data_source.name, fulfillment.data_source_name)
        eq_(self.pool.identifier.type, fulfillment.identifier_type)
        eq_(self.pool.identifier.identifier, fulfillment.identifier)
        eq_(datetime.datetime(2017, 10, 21, 11, 12, 13), fulfillment.content_expires)
        eq_("http://manifest", fulfillment.content_link)
        eq_(audiobook, fulfillment.content_type)


    def test_fulfill_cannot_fulfill(self):
        self.pool.licenses_owned = 7
        self.pool.licenses_available = 6
        self.license.concurrent_checkouts = 7
        loan, ignore = self.license.loan_to(self.patron)
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

    def test_count_holds_before(self):
        now = datetime.datetime.utcnow()
        yesterday = now - datetime.timedelta(days=1)
        tomorrow = now + datetime.timedelta(days=1)
        last_week = now - datetime.timedelta(weeks=1)

        hold, ignore = self.pool.on_hold_to(self.patron, start=now)

        eq_(0, self.api._count_holds_before(hold))

        # A previous hold.
        self.pool.on_hold_to(self._patron(), start=yesterday)
        eq_(1, self.api._count_holds_before(hold))

        # Expired holds don't count.
        self.pool.on_hold_to(self._patron(), start=last_week, end=yesterday, position=0)
        eq_(1, self.api._count_holds_before(hold))

        # Later holds don't count.
        self.pool.on_hold_to(self._patron(), start=tomorrow)
        eq_(1, self.api._count_holds_before(hold))

        # Holds on another pool don't count.
        other_pool = self._licensepool(None)
        other_pool.on_hold_to(self.patron, start=yesterday)
        eq_(1, self.api._count_holds_before(hold))

        for i in range(3):
            self.pool.on_hold_to(self._patron(), start=yesterday, end=tomorrow, position=1)
        eq_(4, self.api._count_holds_before(hold))

    def test_update_hold_end_date(self):
        now = datetime.datetime.utcnow()
        tomorrow = now + datetime.timedelta(days=1)
        yesterday = now - datetime.timedelta(days=1)
        next_week = now + datetime.timedelta(days=7)
        last_week = now - datetime.timedelta(days=7)

        self.pool.licenses_owned = 1
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

        # Updating a hold that has an end date but just became reserved starts
        # the reservation period.
        hold.end = yesterday
        hold.position = 1
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
        loan, ignore = self.license.loan_to(self._patron(), end=tomorrow)
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
        self.license.concurrent_checkouts = 2
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

        self.pool.licenses_owned = 1

        # When there are no other holds and no licenses reserved,
        # hold position is 1.
        loan, ignore = self.license.loan_to(self._patron())
        self.api._update_hold_position(hold)
        eq_(1, hold.position)

        # When a license is reserved, position is 0.
        self._db.delete(loan)
        self.api._update_hold_position(hold)
        eq_(0, hold.position)

        # If another hold has the reserved licenses, position is 2.
        self.pool.on_hold_to(self._patron(), start=yesterday)
        self.api._update_hold_position(hold)
        eq_(2, hold.position)

        # If another license is reserved, position goes back to 0.
        self.pool.licenses_owned = 2
        self.license.concurrent_checkouts = 2
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
        self.pool.licenses_owned = 1
        self.pool.licenses_available = 0
        self.pool.licenses_reserved = 1
        self.pool.patrons_in_hold_queue = 0
        last_update = datetime.datetime.utcnow() - datetime.timedelta(minutes=5)
        self.work.last_update_time = last_update
        self.api.update_hold_queue(self.pool)
        eq_(1, self.pool.licenses_available)
        eq_(0, self.pool.licenses_reserved)
        eq_(0, self.pool.patrons_in_hold_queue)
        # The work's last update time is changed so it will be moved up in the crawlable OPDS feed.
        assert self.work.last_update_time > last_update

        # If there are holds, a license will get reserved for the next hold
        # and its end date will be set.
        hold, ignore = self.pool.on_hold_to(self.patron, start=datetime.datetime.utcnow(), position=1)
        later_hold, ignore = self.pool.on_hold_to(self._patron(), start=datetime.datetime.utcnow() + datetime.timedelta(days=1), position=2)
        self.api.update_hold_queue(self.pool)

        # The pool's licenses were updated.
        eq_(0, self.pool.licenses_available)
        eq_(1, self.pool.licenses_reserved)
        eq_(2, self.pool.patrons_in_hold_queue)

        # And the first hold changed.
        eq_(0, hold.position)
        assert hold.end - datetime.datetime.utcnow() - datetime.timedelta(days=3) < datetime.timedelta(hours=1)

        # The later hold is the same.
        eq_(2, later_hold.position)

        # Now there's a reserved hold. If we add another license, it's reserved and,
        # the later hold is also updated.
        self.pool.licenses_owned = 2
        self.license.concurrent_checkouts = 2
        self.api.update_hold_queue(self.pool)

        eq_(0, self.pool.licenses_available)
        eq_(2, self.pool.licenses_reserved)
        eq_(2, self.pool.patrons_in_hold_queue)
        eq_(0, later_hold.position)
        assert later_hold.end - datetime.datetime.utcnow() - datetime.timedelta(days=3) < datetime.timedelta(hours=1)

        # Now there are no more holds. If we add another license,
        # it ends up being available.
        self.pool.licenses_owned = 3
        self.license.concurrent_checkouts = 3
        self.api.update_hold_queue(self.pool)
        eq_(1, self.pool.licenses_available)
        eq_(2, self.pool.licenses_reserved)
        eq_(2, self.pool.patrons_in_hold_queue)

        self._db.delete(hold)
        self._db.delete(later_hold)

        # We can also make multiple licenses reserved at once.
        loans = []
        holds = []
        for i in range(3):
            loan, ignore = self.license.loan_to(self._patron(), end=datetime.datetime.utcnow() + datetime.timedelta(days=1))
            loans.append(loan)
        for i in range(3):
            hold, ignore = self.pool.on_hold_to(self._patron(), start=datetime.datetime.utcnow() - datetime.timedelta(days=3-i), position=i+1)
            holds.append(hold)
        self.pool.licenses_owned = 5
        self.pool.licenses_available = 0
        self.pool.licenses_reserved = 2
        self.license.concurrent_checkouts = 5
        self.api.update_hold_queue(self.pool)
        eq_(2, self.pool.licenses_reserved)
        eq_(0, self.pool.licenses_available)
        eq_(3, self.pool.patrons_in_hold_queue)
        eq_(0, holds[0].position)
        eq_(0, holds[1].position)
        eq_(3, holds[2].position)
        assert holds[0].end - datetime.datetime.utcnow() - datetime.timedelta(days=3) < datetime.timedelta(hours=1)
        assert holds[1].end - datetime.datetime.utcnow() - datetime.timedelta(days=3) < datetime.timedelta(hours=1)

        # If there are more licenses that change than holds, some of them become available.
        loans[0].end = datetime.datetime.utcnow() - datetime.timedelta(days=1)
        loans[1].end = datetime.datetime.utcnow() - datetime.timedelta(days=1)
        self.api.update_hold_queue(self.pool)
        eq_(3, self.pool.licenses_reserved)
        eq_(1, self.pool.licenses_available)
        eq_(3, self.pool.patrons_in_hold_queue)
        for hold in holds:
            eq_(0, hold.position)
            assert hold.end - datetime.datetime.utcnow() - datetime.timedelta(days=3) < datetime.timedelta(hours=1)

    def test_place_hold_success(self):
        tomorrow = datetime.datetime.utcnow() + datetime.timedelta(days=1)
        self.pool.licenses_owned = 1
        self.license.loan_to(self._patron(), end=tomorrow)

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

    def test_place_hold_currently_available(self):
        self.pool.licenses_owned = 1
        assert_raises(
            CurrentlyAvailable, self.api.place_hold,
            self.patron, "pin", self.pool, "notifications@librarysimplified.org",
        )

    def test_release_hold_success(self):
        self.pool.licenses_owned = 1
        loan, ignore = self.license.loan_to(self._patron())
        self.pool.on_hold_to(self.patron, position=1)

        eq_(True, self.api.release_hold(self.patron, "pin", self.pool))
        eq_(0, self.pool.licenses_available)
        eq_(0, self.pool.licenses_reserved)
        eq_(0, self.pool.patrons_in_hold_queue)
        eq_(0, self._db.query(Hold).count())

        self._db.delete(loan)
        self.pool.on_hold_to(self.patron, position=0)

        eq_(True, self.api.release_hold(self.patron, "pin", self.pool))
        eq_(1, self.pool.licenses_available)
        eq_(0, self.pool.licenses_reserved)
        eq_(0, self.pool.patrons_in_hold_queue)
        eq_(0, self._db.query(Hold).count())

        self.pool.on_hold_to(self.patron, position=0)
        other_hold, ignore = self.pool.on_hold_to(self._patron(), position=2)

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
        loan, ignore = self.license.loan_to(self.patron)
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
        license2 = self._license(pool2)
        loan2, ignore = license2.loan_to(self.patron)
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
        pool2.licenses_owned = 1
        other_patron_loan, ignore = license2.loan_to(self._patron(), end=datetime.datetime.utcnow() + datetime.timedelta(days=1))
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
        self._db.delete(other_patron_loan)
        pool2.licenses_available = 0
        pool2.licenses_reserved = 1
        hold.end = datetime.datetime.utcnow() - datetime.timedelta(days=1)
        hold.position = 0
        activity = self.api.patron_activity(self.patron, "pin")
        eq_(1, len(activity))
        eq_(0, self._db.query(Hold).count())
        eq_(1, pool2.licenses_available)
        eq_(0, pool2.licenses_reserved)

    def test_update_loan_still_active(self):
        self.pool.licenses_available = 6
        self.license.concurrent_checkouts = 6
        loan, ignore = self.license.loan_to(self.patron)
        loan.external_identifier = self._str
        status_doc = {
            "status": "active",
        }

        self.api.update_loan(loan, status_doc)
        # Availability hasn't changed, and the loan still exists.
        eq_(6, self.pool.licenses_available)
        eq_(1, self._db.query(Loan).count())

    def test_update_loan_removes_loan(self):
        self.pool.licenses_owned = 7
        self.pool.licenses_available = 6
        self.license.concurrent_checkouts = 7
        loan, ignore = self.license.loan_to(self.patron)
        loan.external_identifier = self._str
        status_doc = {
            "status": "cancelled",
        }

        self.api.update_loan(loan, status_doc)
        # Availability has increased, and the loan is gone.
        eq_(7, self.pool.licenses_available)
        eq_(0, self._db.query(Loan).count())

    def test_update_loan_removes_loan_with_hold_queue(self):
        self.pool.licenses_owned = 1
        self.pool.licenses_available = 0
        self.pool.licenses_reserved = 0
        self.pool.patrons_in_hold_queue = 1
        loan, ignore = self.license.loan_to(self.patron)
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

    def test_checkout_from_external_library(self):
        # This book is available to check out.
        self.pool.licenses_owned = 6
        self.pool.licenses_available = 6
        self.license.concurrent_checkouts = 6
        self.license.remaining_checkouts = 10

        # An integration client checks out the book successfully.
        loan_url = self._str
        lsd = json.dumps({
            "status": "ready",
            "potential_rights": {
                "end": "3017-10-21T11:12:13Z"
            },
            "links": [{
                "rel": "self",
                "href": loan_url,
            }],
        })

        self.api.queue_response(200, content=lsd)
        loan = self.api.checkout_to_external_library(self.client, self.pool)
        eq_(self.client, loan.integration_client)
        eq_(self.pool, loan.license_pool)
        assert loan.start > datetime.datetime.utcnow() - datetime.timedelta(minutes=1)
        assert loan.start < datetime.datetime.utcnow() + datetime.timedelta(minutes=1)
        eq_(datetime.datetime(3017, 10, 21, 11, 12, 13), loan.end)
        eq_(loan_url, loan.external_identifier)
        eq_(1, self._db.query(Loan).count())

        # The pool's availability and the license's remaining checkouts have decreased.
        eq_(5, self.pool.licenses_available)
        eq_(9, self.license.remaining_checkouts)

        # The book can also be placed on hold to an external library,
        # if there are no copies available.
        self.pool.licenses_owned = 1
        self.license.concurrent_checkouts = 1

        hold = self.api.checkout_to_external_library(self.client, self.pool)

        eq_(1, self.pool.patrons_in_hold_queue)
        eq_(self.client, hold.integration_client)
        eq_(self.pool, hold.license_pool)
        assert hold.start > datetime.datetime.utcnow() - datetime.timedelta(minutes=1)
        assert hold.start < datetime.datetime.utcnow() + datetime.timedelta(minutes=1)
        assert hold.end > datetime.datetime.utcnow() + datetime.timedelta(days=7)
        eq_(1, hold.position)
        eq_(1, self._db.query(Hold).count())

    def test_checkout_from_external_library_with_hold(self):
        # An integration client has this book on hold, and the book just became available to check out.
        self.pool.licenses_owned = 1
        self.pool.licenses_available = 0
        self.pool.licenses_reserved = 1
        self.pool.patrons_in_hold_queue = 1
        hold, ignore = self.pool.on_hold_to(self.client, start=datetime.datetime.utcnow() - datetime.timedelta(days=1), position=0)

        # The patron checks out the book.
        loan_url = self._str
        lsd = json.dumps({
            "status": "ready",
            "potential_rights": {
                "end": "3017-10-21T11:12:13Z"
            },
            "links": [{
                "rel": "self",
                "href": loan_url,
            }],
        })

        self.api.queue_response(200, content=lsd)

        # The patron gets a loan successfully.
        loan = self.api.checkout_to_external_library(self.client, self.pool, hold)
        eq_(self.client, loan.integration_client)
        eq_(self.pool, loan.license_pool)
        assert loan.start > datetime.datetime.utcnow() - datetime.timedelta(minutes=1)
        assert loan.start < datetime.datetime.utcnow() + datetime.timedelta(minutes=1)
        eq_(datetime.datetime(3017, 10, 21, 11, 12, 13), loan.end)
        eq_(loan_url, loan.external_identifier)
        eq_(1, self._db.query(Loan).count())

        # The book is no longer reserved for the patron, and the hold has been deleted.
        eq_(0, self.pool.licenses_reserved)
        eq_(0, self.pool.licenses_available)
        eq_(0, self.pool.patrons_in_hold_queue)
        eq_(0, self._db.query(Hold).count())

    def test_checkin_from_external_library(self):
        # An integration client has a copy of this book checked out.
        self.pool.licenses_owned = 7
        self.pool.licenses_available = 6
        self.license.concurrent_checkouts = 7
        loan, ignore = self.license.loan_to(self.client)
        loan.external_identifier = "http://loan/" + self._str
        loan.end = datetime.datetime.utcnow() + datetime.timedelta(days=3)

        # The patron returns the book successfully.
        lsd = json.dumps({
            "status": "ready",
            "links": [{
                "rel": "return",
                "href": "http://return",
            }],
        })
        returned_lsd = json.dumps({
            "status": "returned",
        })

        self.api.queue_response(200, content=lsd)
        self.api.queue_response(200)
        self.api.queue_response(200, content=returned_lsd)
        self.api.checkin_from_external_library(self.client, loan)
        eq_(3, len(self.api.requests))
        assert "http://loan" in self.api.requests[0][0]
        eq_("http://return", self.api.requests[1][0])
        assert "http://loan" in self.api.requests[2][0]

        # The pool's availability has increased, and the local loan has
        # been deleted.
        eq_(7, self.pool.licenses_available)
        eq_(0, self._db.query(Loan).count())

    def test_fulfill_for_external_library(self):
        loan, ignore = self.license.loan_to(self.client)
        loan.external_identifier = self._str
        loan.end = datetime.datetime.utcnow() + datetime.timedelta(days=3)

        lsd = json.dumps({
            "status": "ready",
            "potential_rights": {
                "end": "2017-10-21T11:12:13Z"
            },
            "links": [{
                "rel": "license",
                "href": "http://acsm",
                "type": DeliveryMechanism.ADOBE_DRM,
            }],
        })

        self.api.queue_response(200, content=lsd)
        fulfillment = self.api.fulfill_for_external_library(self.client, loan, None)
        eq_(self.collection, fulfillment.collection(self._db))
        eq_(self.pool.data_source.name, fulfillment.data_source_name)
        eq_(self.pool.identifier.type, fulfillment.identifier_type)
        eq_(self.pool.identifier.identifier, fulfillment.identifier)
        eq_(datetime.datetime(2017, 10, 21, 11, 12, 13), fulfillment.content_expires)
        eq_("http://acsm", fulfillment.content_link)
        eq_(DeliveryMechanism.ADOBE_DRM, fulfillment.content_type)

    def test_release_hold_from_external_library(self):
        self.pool.licenses_owned = 1
        loan, ignore = self.license.loan_to(self._patron())
        hold, ignore = self.pool.on_hold_to(self.client, position=1)

        eq_(True, self.api.release_hold_from_external_library(self.client, hold))
        eq_(0, self.pool.licenses_available)
        eq_(0, self.pool.licenses_reserved)
        eq_(0, self.pool.patrons_in_hold_queue)
        eq_(0, self._db.query(Hold).count())

        self._db.delete(loan)
        hold, ignore = self.pool.on_hold_to(self.client, position=0)

        eq_(True, self.api.release_hold_from_external_library(self.client, hold))
        eq_(1, self.pool.licenses_available)
        eq_(0, self.pool.licenses_reserved)
        eq_(0, self.pool.patrons_in_hold_queue)
        eq_(0, self._db.query(Hold).count())

        hold, ignore = self.pool.on_hold_to(self.client, position=0)
        other_hold, ignore = self.pool.on_hold_to(self._patron(), position=2)

        eq_(True, self.api.release_hold_from_external_library(self.client, hold))
        eq_(0, self.pool.licenses_available)
        eq_(1, self.pool.licenses_reserved)
        eq_(1, self.pool.patrons_in_hold_queue)
        eq_(1, self._db.query(Hold).count())
        eq_(0, other_hold.position)


class TestODLImporter(DatabaseTest, BaseODLTest):

    def test_import(self):
        feed = self.get_data("feedbooks_bibliographic.atom")
        data_source = DataSource.lookup(self._db, "Feedbooks", autocreate=True)
        collection = MockODLAPI.mock_collection(self._db)
        collection.external_integration.set_setting(
            Collection.DATA_SOURCE_NAME_SETTING,
            data_source.name
        )

        class MockMetadataClient(object):
            def canonicalize_author_name(self, identifier, working_display_name):
                return working_display_name
        metadata_client = MockMetadataClient()

        warrior_time_limited = dict(checkouts=dict(available=1))
        canadianity_loan_limited = dict(checkouts=dict(left=40, available=10))
        canadianity_perpetual = dict(checkouts=dict(available=1))
        midnight_loan_limited_1 = dict(checkouts=dict(left=20, available=1))
        midnight_loan_limited_2 = dict(checkouts=dict(left=52, available=1))
        everglades_loan = dict(checkouts=dict(left=10, available=5))
        poetry_loan = dict(checkouts=dict(left=10, available=5))
        mock_responses = [json.dumps(r) for r in [
            warrior_time_limited, canadianity_loan_limited, canadianity_perpetual,
            midnight_loan_limited_1, midnight_loan_limited_2, everglades_loan, poetry_loan
        ]]

        def do_get(url, headers):
            return 200, {}, mock_responses.pop(0)

        importer = ODLImporter(
            self._db, collection=collection,
            metadata_client=metadata_client,
            http_get=do_get,
        )

        imported_editions, imported_pools, imported_works, failures = (
            importer.import_from_feed(feed)
        )
        self._db.commit()

        # This importer works the same as the base OPDSImporter, except that
        # it extracts format information from 'odl:license' tags and creates
        # LicensePoolDeliveryMechanisms.


        # The importer created 6 editions, pools, and works.
        eq_({}, failures)
        eq_(6, len(imported_editions))
        eq_(6, len(imported_pools))
        eq_(6, len(imported_works))

        [canadianity, everglades, dragons, warrior, blazing, midnight,] = sorted(imported_editions, key=lambda x: x.title)
        eq_("The Blazing World", blazing.title)
        eq_("Sun Warrior", warrior.title)
        eq_("Canadianity", canadianity.title)
        eq_("The Midnight Dance", midnight.title)
        eq_("Everglades Wildguide", everglades.title)
        eq_("Rise of the Dragons, Book 1", dragons.title)

        # This book is open access and has no applicable DRM
        [blazing_pool] = [p for p in imported_pools if p.identifier == blazing.primary_identifier]
        eq_(True, blazing_pool.open_access)
        [lpdm] = blazing_pool.delivery_mechanisms
        eq_(Representation.EPUB_MEDIA_TYPE, lpdm.delivery_mechanism.content_type)
        eq_(DeliveryMechanism.NO_DRM, lpdm.delivery_mechanism.drm_scheme)

        # # This book has a single 'odl:license' tag.
        [warrior_pool] = [p for p in imported_pools if p.identifier == warrior.primary_identifier]
        eq_(False, warrior_pool.open_access)
        [lpdm] = warrior_pool.delivery_mechanisms
        eq_(Edition.BOOK_MEDIUM, warrior_pool.presentation_edition.medium)
        eq_(Representation.EPUB_MEDIA_TYPE, lpdm.delivery_mechanism.content_type)
        eq_(DeliveryMechanism.ADOBE_DRM, lpdm.delivery_mechanism.drm_scheme)
        eq_(RightsStatus.IN_COPYRIGHT, lpdm.rights_status.uri)
        eq_(1, warrior_pool.licenses_owned)
        eq_(1, warrior_pool.licenses_available)
        [license] = warrior_pool.licenses
        eq_("1", license.identifier)
        eq_("https://loan.feedbooks.net/loan/get/{?id,checkout_id,expires,patron_id,notification_url}",
            license.checkout_url)
        eq_("https://license.feedbooks.net/license/status/?uuid=1",
            license.status_url)
        eq_(datetime.datetime(2019, 3, 31, 0o3, 13, 35), license.expires)
        eq_(None, license.remaining_checkouts)
        eq_(1, license.concurrent_checkouts)

        # This item is an open access audiobook.
        [everglades_pool] = [p for p in imported_pools if p.identifier == everglades.primary_identifier]
        eq_(True, everglades_pool.open_access)
        [lpdm] = everglades_pool.delivery_mechanisms
        eq_(Edition.AUDIO_MEDIUM, everglades_pool.presentation_edition.medium)

        eq_(Representation.AUDIOBOOK_MANIFEST_MEDIA_TYPE, lpdm.delivery_mechanism.content_type)
        eq_(DeliveryMechanism.NO_DRM, lpdm.delivery_mechanism.drm_scheme)

        # This is a non-open access audiobook. There is no
        # <odl:protection> tag; the drm_scheme is implied by the value
        # of <dcterms:format>.
        [dragons_pool] = [
            p for p in imported_pools
            if p.identifier == dragons.primary_identifier
        ]
        eq_(Edition.AUDIO_MEDIUM, dragons_pool.presentation_edition.medium)
        eq_(False, dragons_pool.open_access)
        [lpdm] = dragons_pool.delivery_mechanisms

        eq_(Representation.AUDIOBOOK_MANIFEST_MEDIA_TYPE, lpdm.delivery_mechanism.content_type)
        eq_(DeliveryMechanism.FEEDBOOKS_AUDIOBOOK_DRM, lpdm.delivery_mechanism.drm_scheme)

        # This book has two 'odl:license' tags for the same format and drm scheme
        # (this happens if the library purchases two copies).
        [canadianity_pool] = [p for p in imported_pools if p.identifier == canadianity.primary_identifier]
        eq_(False, canadianity_pool.open_access)
        [lpdm] = canadianity_pool.delivery_mechanisms
        eq_(Representation.EPUB_MEDIA_TYPE, lpdm.delivery_mechanism.content_type)
        eq_(DeliveryMechanism.ADOBE_DRM, lpdm.delivery_mechanism.drm_scheme)
        eq_(RightsStatus.IN_COPYRIGHT, lpdm.rights_status.uri)
        eq_(11, canadianity_pool.licenses_owned)
        eq_(11, canadianity_pool.licenses_available)
        [license1, license2] = sorted(canadianity_pool.licenses, key=lambda x: x.identifier)
        eq_("2", license1.identifier)
        eq_("https://loan.feedbooks.net/loan/get/{?id,checkout_id,expires,patron_id,notification_url}",
            license1.checkout_url)
        eq_("https://license.feedbooks.net/license/status/?uuid=2",
            license1.status_url)
        eq_(None, license1.expires)
        eq_(40, license1.remaining_checkouts)
        eq_(10, license1.concurrent_checkouts)
        eq_("3", license2.identifier)
        eq_("https://loan.feedbooks.net/loan/get/{?id,checkout_id,expires,patron_id,notification_url}",
            license2.checkout_url)
        eq_("https://license.feedbooks.net/license/status/?uuid=3",
            license2.status_url)
        eq_(None, license2.expires)
        eq_(None, license2.remaining_checkouts)
        eq_(1, license2.concurrent_checkouts)

        # This book has two 'odl:license' tags, and they have different formats.
        # TODO: the format+license association is not handled yet.
        [midnight_pool] = [p for p in imported_pools if p.identifier == midnight.primary_identifier]
        eq_(False, midnight_pool.open_access)
        lpdms = midnight_pool.delivery_mechanisms
        eq_(2, len(lpdms))
        eq_(set([Representation.EPUB_MEDIA_TYPE, Representation.PDF_MEDIA_TYPE]),
            set([lpdm.delivery_mechanism.content_type for lpdm in lpdms]))
        eq_([DeliveryMechanism.ADOBE_DRM, DeliveryMechanism.ADOBE_DRM],
            [lpdm.delivery_mechanism.drm_scheme for lpdm in lpdms])
        eq_([RightsStatus.IN_COPYRIGHT, RightsStatus.IN_COPYRIGHT],
            [lpdm.rights_status.uri for lpdm in lpdms])
        eq_(2, midnight_pool.licenses_owned)
        eq_(2, midnight_pool.licenses_available)
        [license1, license2] = sorted(midnight_pool.licenses, key=lambda x: x.identifier)
        eq_("4", license1.identifier)
        eq_("https://loan.feedbooks.net/loan/get/{?id,checkout_id,expires,patron_id,notification_url}",
            license1.checkout_url)
        eq_("https://license.feedbooks.net/license/status/?uuid=4",
            license1.status_url)
        eq_(None, license1.expires)
        eq_(20, license1.remaining_checkouts)
        eq_(1, license1.concurrent_checkouts)
        eq_("5", license2.identifier)
        eq_("https://loan.feedbooks.net/loan/get/{?id,checkout_id,expires,patron_id,notification_url}",
            license2.checkout_url)
        eq_("https://license.feedbooks.net/license/status/?uuid=5",
            license2.status_url)
        eq_(None, license2.expires)
        eq_(52, license2.remaining_checkouts)
        eq_(1, license2.concurrent_checkouts)


class TestODLHoldReaper(DatabaseTest, BaseODLTest):

    def test_run_once(self):
        data_source = DataSource.lookup(self._db, "Feedbooks", autocreate=True)
        collection = MockODLAPI.mock_collection(self._db)
        collection.external_integration.set_setting(
            Collection.DATA_SOURCE_NAME_SETTING,
            data_source.name
        )
        api = MockODLAPI(self._db, collection)
        reaper = ODLHoldReaper(self._db, collection, api=api)

        now = datetime.datetime.utcnow()
        yesterday = now - datetime.timedelta(days=1)

        pool = self._licensepool(None, collection=collection)
        pool.licenses_owned = 3
        pool.licenses_available = 0
        pool.licenses_reserved = 3
        expired_hold1, ignore = pool.on_hold_to(self._patron(), end=yesterday, position=0)
        expired_hold2, ignore = pool.on_hold_to(self._patron(), end=yesterday, position=0)
        expired_hold3, ignore = pool.on_hold_to(self._patron(), end=yesterday, position=0)
        current_hold, ignore = pool.on_hold_to(self._patron(), position=3)
        # This hold has an end date in the past, but its position is greater than 0
        # so the end date is not reliable.
        bad_end_date, ignore = pool.on_hold_to(self._patron(), end=yesterday, position=4)

        progress = reaper.run_once(reaper.timestamp().to_data())

        # The expired holds have been deleted and the other holds have been updated.
        eq_(2, self._db.query(Hold).count())
        eq_([current_hold, bad_end_date], self._db.query(Hold).order_by(Hold.start).all())
        eq_(0, current_hold.position)
        eq_(0, bad_end_date.position)
        assert current_hold.end > now
        assert bad_end_date.end > now
        eq_(1, pool.licenses_available)
        eq_(2, pool.licenses_reserved)

        # The TimestampData returned reflects what work was done.
        eq_('Holds deleted: 3. License pools updated: 1', progress.achievements)

        # The TimestampData does not include any timing information --
        # that will be applied by run().
        eq_(None, progress.start)
        eq_(None, progress.finish)



class TestSharedODLAPI(DatabaseTest, BaseODLTest):

    def setup(self):
        super(TestSharedODLAPI, self).setup()
        self.collection = MockSharedODLAPI.mock_collection(self._db)
        self.collection.external_integration.set_setting(
            Collection.DATA_SOURCE_NAME_SETTING,
            "Feedbooks"
        )
        self.api = MockSharedODLAPI(self._db, self.collection)
        self.pool = self._licensepool(None, collection=self.collection)
        self.pool.identifier.add_link(Hyperlink.BORROW, self._str, self.collection.data_source)
        self.patron = self._patron()

    def test_get(self):
        # Create a SharedODLAPI to test the _get method. The other tests use a
        # mock API class that overrides _get.
        api = SharedODLAPI(self._db, self.collection)

        # The library has not registered with the remote collection yet.
        def do_get(url, headers=None, allowed_response_codes=None):
            raise Exception("do_get should not be called")
        assert_raises(LibraryAuthorizationFailedException, api._get,
                      "test url", patron=self.patron, do_get=do_get)

        # Once the library registers, it gets a shared secret that is included
        # in request headers.
        ConfigurationSetting.for_library_and_externalintegration(
            self._db, ExternalIntegration.PASSWORD, self.patron.library,
            self.collection.external_integration).value = "secret"
        def do_get(url, headers=None, allowed_response_codes=None):
            eq_("test url", url)
            eq_("test header value", headers.get("test_key"))
            eq_("Bearer " + base64.b64encode("secret"), headers.get("Authorization"))
            eq_(["200"], allowed_response_codes)
        api._get("test url", headers=dict(test_key="test header value"),
                 patron=self.patron, allowed_response_codes=["200"],
                 do_get=do_get)

    def test_checkout_success(self):
        response = self.get_data("shared_collection_borrow_success.opds")
        self.api.queue_response(200, content=response)

        loan = self.api.checkout(self.patron, "pin", self.pool, Representation.EPUB_MEDIA_TYPE)
        eq_(self.collection, loan.collection(self._db))
        eq_(self.pool.data_source.name, loan.data_source_name)
        eq_(self.pool.identifier.type, loan.identifier_type)
        eq_(self.pool.identifier.identifier, loan.identifier)
        eq_(datetime.datetime(2018, 3, 8, 17, 41, 31), loan.start_date)
        eq_(datetime.datetime(2018, 3, 29, 17, 41, 30), loan.end_date)
        eq_("http://localhost:6500/AL/collections/DPLA%20Exchange/loans/31", loan.external_identifier)

        eq_([self.pool.identifier.links[0].resource.url],
             self.api.requests)

    def test_checkout_from_hold(self):
        hold, ignore = self.pool.on_hold_to(self.patron, external_identifier=self._str)
        hold_info_response = self.get_data("shared_collection_hold_info_ready.opds")
        self.api.queue_response(200, content=hold_info_response)
        borrow_response = self.get_data("shared_collection_borrow_success.opds")
        self.api.queue_response(200, content=borrow_response)

        loan = self.api.checkout(self.patron, "pin", self.pool, Representation.EPUB_MEDIA_TYPE)
        eq_(self.collection, loan.collection(self._db))
        eq_(self.pool.data_source.name, loan.data_source_name)
        eq_(self.pool.identifier.type, loan.identifier_type)
        eq_(self.pool.identifier.identifier, loan.identifier)
        eq_(datetime.datetime(2018, 3, 8, 17, 41, 31), loan.start_date)
        eq_(datetime.datetime(2018, 3, 29, 17, 41, 30), loan.end_date)
        eq_("http://localhost:6500/AL/collections/DPLA%20Exchange/loans/31", loan.external_identifier)

        eq_([hold.external_identifier,
             "http://localhost:6500/AL/collections/DPLA%20Exchange/holds/17/borrow"],
            self.api.requests)

    def test_checkout_already_checked_out(self):
        loan, ignore = self.pool.loan_to(self.patron)
        assert_raises(AlreadyCheckedOut, self.api.checkout, self.patron, "pin",
                      self.pool, Representation.EPUB_MEDIA_TYPE)
        eq_([], self.api.requests)

    def test_checkout_no_available_copies(self):
        self.api.queue_response(403)
        assert_raises(NoAvailableCopies, self.api.checkout, self.patron, "pin",
                      self.pool, Representation.EPUB_MEDIA_TYPE)
        eq_([self.pool.identifier.links[0].resource.url],
             self.api.requests)

    def test_checkout_no_licenses(self):
        self.api.queue_response(
            NO_LICENSES.response[1],
            headers=NO_LICENSES.response[2],
            content=NO_LICENSES.response[0],
        )
        assert_raises(NoLicenses, self.api.checkout, self.patron, "pin",
                      self.pool, Representation.EPUB_MEDIA_TYPE)
        eq_([self.pool.identifier.links[0].resource.url],
             self.api.requests)

    def test_checkout_from_hold_not_available(self):
        hold, ignore = self.pool.on_hold_to(self.patron)
        hold_info_response = self.get_data("shared_collection_hold_info_reserved.opds")
        self.api.queue_response(200, content=hold_info_response)
        assert_raises(NoAvailableCopies, self.api.checkout, self.patron, "pin",
                      self.pool, Representation.EPUB_MEDIA_TYPE)
        eq_([hold.external_identifier], self.api.requests)

    def test_checkout_cannot_loan(self):
        self.api.queue_response(500)
        assert_raises(CannotLoan, self.api.checkout, self.patron, "pin",
                      self.pool, Representation.EPUB_MEDIA_TYPE)
        eq_([self.pool.identifier.links[0].resource.url],
             self.api.requests)

        # This pool has no borrow link.
        pool = self._licensepool(None, collection=self.collection)
        assert_raises(CannotLoan, self.api.checkout, self.patron, "pin",
                      pool, Representation.EPUB_MEDIA_TYPE)

    def test_checkin_success(self):
        loan, ignore = self.pool.loan_to(self.patron, external_identifier=self._str)
        loan_info_response = self.get_data("shared_collection_loan_info.opds")
        self.api.queue_response(200, content=loan_info_response)
        self.api.queue_response(200, content="Deleted")
        response = self.api.checkin(self.patron, "pin", self.pool)
        eq_(True, response)
        eq_([loan.external_identifier,
             "http://localhost:6500/AL/collections/DPLA%20Exchange/loans/33/revoke"],
            self.api.requests)

    def test_checkin_not_checked_out(self):
        assert_raises(NotCheckedOut, self.api.checkin, self.patron, "pin", self.pool)
        eq_([], self.api.requests)

        loan, ignore = self.pool.loan_to(self.patron, external_identifier=self._str)
        self.api.queue_response(404)
        assert_raises(NotCheckedOut, self.api.checkin, self.patron, "pin", self.pool)
        eq_([loan.external_identifier], self.api.requests)

    def test_checkin_cannot_return(self):
        loan, ignore = self.pool.loan_to(self.patron, external_identifier=self._str)
        self.api.queue_response(500)
        assert_raises(CannotReturn, self.api.checkin, self.patron, "pin", self.pool)
        eq_([loan.external_identifier], self.api.requests)


        loan_info_response = self.get_data("shared_collection_loan_info.opds")
        self.api.queue_response(200, content=loan_info_response)
        self.api.queue_response(500)
        assert_raises(CannotReturn, self.api.checkin, self.patron, "pin", self.pool)
        eq_([loan.external_identifier,
             "http://localhost:6500/AL/collections/DPLA%20Exchange/loans/33/revoke"],
            self.api.requests[1:])

    def test_fulfill_success(self):
        loan, ignore = self.pool.loan_to(self.patron, external_identifier=self._str)
        loan_info_response = self.get_data("shared_collection_loan_info.opds")
        self.api.queue_response(200, content=loan_info_response)
        self.api.queue_response(200, content="An ACSM file")
        fulfillment = self.api.fulfill(self.patron, "pin", self.pool, self.pool.delivery_mechanisms[0])
        eq_(self.collection, fulfillment.collection(self._db))
        eq_(self.pool.data_source.name, fulfillment.data_source_name)
        eq_(self.pool.identifier.type, fulfillment.identifier_type)
        eq_(self.pool.identifier.identifier, fulfillment.identifier)
        eq_(None, fulfillment.content_link)
        eq_("An ACSM file", fulfillment.content)
        eq_(datetime.datetime(2018, 3, 29, 17, 44, 11), fulfillment.content_expires)

        eq_([loan.external_identifier,
             "http://localhost:6500/AL/collections/DPLA%20Exchange/loans/33/fulfill/2"],
            self.api.requests)

    def test_fulfill_not_checked_out(self):
        assert_raises(NotCheckedOut, self.api.fulfill, self.patron, "pin",
                      self.pool, self.pool.delivery_mechanisms[0])
        eq_([], self.api.requests)

        loan, ignore = self.pool.loan_to(self.patron, external_identifier=self._str)
        self.api.queue_response(404)
        assert_raises(NotCheckedOut, self.api.fulfill, self.patron, "pin",
                      self.pool, self.pool.delivery_mechanisms[0])
        eq_([loan.external_identifier], self.api.requests)

    def test_fulfill_cannot_fulfill(self):
        loan, ignore = self.pool.loan_to(self.patron, external_identifier=self._str)
        self.api.queue_response(500)
        assert_raises(CannotFulfill, self.api.fulfill, self.patron, "pin",
                      self.pool, self.pool.delivery_mechanisms[0])
        eq_([loan.external_identifier], self.api.requests)

        self.api.queue_response(200, content="not opds")
        assert_raises(CannotFulfill, self.api.fulfill, self.patron, "pin",
                      self.pool, self.pool.delivery_mechanisms[0])
        eq_([loan.external_identifier], self.api.requests[1:])

        loan_info_response = self.get_data("shared_collection_loan_info.opds")
        self.api.queue_response(200, content=loan_info_response)
        self.api.queue_response(500)
        assert_raises(CannotFulfill, self.api.fulfill, self.patron, "pin",
                      self.pool, self.pool.delivery_mechanisms[0])
        eq_([loan.external_identifier,
             "http://localhost:6500/AL/collections/DPLA%20Exchange/loans/33/fulfill/2"],
            self.api.requests[2:])

    def test_fulfill_format_not_available(self):
        loan, ignore = self.pool.loan_to(self.patron)
        loan_info_response = self.get_data("shared_collection_loan_info_no_epub.opds")
        self.api.queue_response(200, content=loan_info_response)
        assert_raises(FormatNotAvailable, self.api.fulfill, self.patron, "pin",
                      self.pool, self.pool.delivery_mechanisms[0])
        eq_([loan.external_identifier], self.api.requests)

    def test_place_hold_success(self):
        hold_response = self.get_data("shared_collection_hold_info_reserved.opds")
        self.api.queue_response(200, content=hold_response)
        hold = self.api.place_hold(self.patron, "pin", self.pool, "notifications@librarysimplified.org")
        eq_(self.collection, hold.collection(self._db))
        eq_(self.pool.data_source.name, hold.data_source_name)
        eq_(self.pool.identifier.type, hold.identifier_type)
        eq_(self.pool.identifier.identifier, hold.identifier)
        eq_(datetime.datetime(2018, 3, 8, 18, 50, 18), hold.start_date)
        eq_(datetime.datetime(2018, 3, 29, 17, 44, 0o1), hold.end_date)
        eq_(1, hold.hold_position)
        eq_("http://localhost:6500/AL/collections/DPLA%20Exchange/holds/18", hold.external_identifier)

        eq_([self.pool.identifier.links[0].resource.url],
             self.api.requests)

    def test_place_hold_already_checked_out(self):
        loan, ignore = self.pool.loan_to(self.patron)
        assert_raises(AlreadyCheckedOut, self.api.place_hold, self.patron, "pin",
                      self.pool, "notification@librarysimplified.org")
        eq_([], self.api.requests)

    def test_release_hold_success(self):
        hold, ignore = self.pool.on_hold_to(self.patron, external_identifier=self._str)
        hold_response = self.get_data("shared_collection_hold_info_reserved.opds")
        self.api.queue_response(200, content=hold_response)
        self.api.queue_response(200, content="Deleted")
        response = self.api.release_hold(self.patron, "pin", self.pool)
        eq_(True, response)
        eq_([hold.external_identifier,
             "http://localhost:6500/AL/collections/DPLA%20Exchange/holds/18/revoke"],
            self.api.requests)

    def test_release_hold_not_on_hold(self):
        assert_raises(NotOnHold, self.api.release_hold, self.patron, "pin", self.pool)
        eq_([], self.api.requests)

        hold, ignore = self.pool.on_hold_to(self.patron, external_identifier=self._str)
        self.api.queue_response(404)
        assert_raises(NotOnHold, self.api.release_hold, self.patron, "pin", self.pool)
        eq_([hold.external_identifier], self.api.requests)

    def test_release_hold_cannot_release_hold(self):
        hold, ignore = self.pool.on_hold_to(self.patron, external_identifier=self._str)
        self.api.queue_response(500)
        assert_raises(CannotReleaseHold, self.api.release_hold, self.patron, "pin", self.pool)
        eq_([hold.external_identifier], self.api.requests)

        hold_response = self.get_data("shared_collection_hold_info_reserved.opds")
        self.api.queue_response(200, content=hold_response)
        self.api.queue_response(500)
        assert_raises(CannotReleaseHold, self.api.release_hold, self.patron, "pin", self.pool)
        eq_([hold.external_identifier,
             "http://localhost:6500/AL/collections/DPLA%20Exchange/holds/18/revoke"],
            self.api.requests[1:])

    def test_patron_activity_success(self):
        # The patron has one loan, and the remote circ manager returns it.
        loan, ignore = self.pool.loan_to(self.patron, external_identifier=self._str)
        loan_response = self.get_data("shared_collection_loan_info.opds")
        self.api.queue_response(200, content=loan_response)
        activity = self.api.patron_activity(self.patron, "pin")
        eq_(1, len(activity))
        [loan_info] = activity
        eq_(self.collection, loan_info.collection(self._db))
        eq_(self.pool.data_source.name, loan_info.data_source_name)
        eq_(self.pool.identifier.type, loan_info.identifier_type)
        eq_(self.pool.identifier.identifier, loan_info.identifier)
        eq_(datetime.datetime(2018, 3, 8, 17, 44, 12), loan_info.start_date)
        eq_(datetime.datetime(2018, 3, 29, 17, 44, 11), loan_info.end_date)
        eq_([loan.external_identifier], self.api.requests)

        # The _get method was passed a patron - this is necessary because
        # the patron_activity method may be called from a thread without
        # access to the flask request.
        eq_(self.patron, self.api.request_args[0][0])

        # The patron's loan has been deleted on the remote.
        self.api.queue_response(404, content="No loan here")
        activity = self.api.patron_activity(self.patron, "pin")
        eq_(0, len(activity))
        eq_([loan.external_identifier], self.api.requests[1:])

        # Now the patron has a hold instead.
        self._db.delete(loan)
        hold, ignore = self.pool.on_hold_to(self.patron, external_identifier=self._str)
        hold_response = self.get_data("shared_collection_hold_info_reserved.opds")
        self.api.queue_response(200, content=hold_response)
        activity = self.api.patron_activity(self.patron, "pin")
        eq_(1, len(activity))
        [hold_info] = activity
        eq_(self.collection, hold_info.collection(self._db))
        eq_(self.pool.data_source.name, hold_info.data_source_name)
        eq_(self.pool.identifier.type, hold_info.identifier_type)
        eq_(self.pool.identifier.identifier, hold_info.identifier)
        eq_(datetime.datetime(2018, 3, 8, 18, 50, 18), hold_info.start_date)
        eq_(datetime.datetime(2018, 3, 29, 17, 44, 0o1), hold_info.end_date)
        eq_([hold.external_identifier], self.api.requests[2:])

        # The patron's hold has been deleted on the remote.
        self.api.queue_response(404, content="No hold here")
        activity = self.api.patron_activity(self.patron, "pin")
        eq_(0, len(activity))
        eq_([hold.external_identifier], self.api.requests[3:])

    def test_patron_activity_remote_integration_exception(self):
        loan, ignore = self.pool.loan_to(self.patron, external_identifier=self._str)
        self.api.queue_response(500)
        assert_raises(RemoteIntegrationException, self.api.patron_activity, self.patron, "pin")
        eq_([loan.external_identifier], self.api.requests)
        self._db.delete(loan)

        hold, ignore = self.pool.on_hold_to(self.patron, external_identifier=self._str)
        self.api.queue_response(500)
        assert_raises(RemoteIntegrationException, self.api.patron_activity, self.patron, "pin")
        eq_([hold.external_identifier], self.api.requests[1:])

class TestSharedODLImporter(DatabaseTest, BaseODLTest):

    def test_get_fulfill_url(self):
        entry = self.get_data("shared_collection_loan_info.opds")
        eq_("http://localhost:6500/AL/collections/DPLA%20Exchange/loans/33/fulfill/2",
            SharedODLImporter.get_fulfill_url(entry, "application/epub+zip", "application/vnd.adobe.adept+xml"))
        eq_(None, SharedODLImporter.get_fulfill_url(entry, "application/pdf", "application/vnd.adobe.adept+xml"))
        eq_(None, SharedODLImporter.get_fulfill_url(entry, "application/epub+zip", None))

    def test_import(self):
        feed = self.get_data("shared_collection_feed.opds")
        data_source = DataSource.lookup(self._db, "DPLA Exchange", autocreate=True)
        collection = MockSharedODLAPI.mock_collection(self._db)
        collection.external_integration.set_setting(
            Collection.DATA_SOURCE_NAME_SETTING,
            data_source.name
        )

        class MockMetadataClient(object):
            def canonicalize_author_name(self, identifier, working_display_name):
                return working_display_name
        metadata_client = MockMetadataClient()
        importer = SharedODLImporter(
            self._db, collection=collection,
            metadata_client=metadata_client,
        )

        imported_editions, imported_pools, imported_works, failures = (
            importer.import_from_feed(feed)
        )

        # This importer works the same as the base OPDSImporter, except that
        # it extracts license pool information from acquisition links.

        # The importer created 3 editions, pools, and works.
        eq_(3, len(imported_editions))
        eq_(3, len(imported_pools))
        eq_(3, len(imported_works))

        [six_months, essex, gatsby] = sorted(imported_editions, key=lambda x: x.title)
        eq_("Six Months, Three Days, Five Others", six_months.title)
        eq_("The Essex Serpent", essex.title)
        eq_("The Great Gatsby", gatsby.title)

        # This book is open access.
        [gatsby_pool] = [p for p in imported_pools if p.identifier == gatsby.primary_identifier]
        eq_(True, gatsby_pool.open_access)
        # This pool has two delivery mechanisms, from a borrow link and an open-access link.
        # Both are DRM-free epubs.
        lpdms = gatsby_pool.delivery_mechanisms
        eq_(2, len(lpdms))
        for lpdm in lpdms:
            eq_(Representation.EPUB_MEDIA_TYPE, lpdm.delivery_mechanism.content_type)
            eq_(DeliveryMechanism.NO_DRM, lpdm.delivery_mechanism.drm_scheme)

        # This book is already checked out and has a hold.
        [six_months_pool] = [p for p in imported_pools if p.identifier == six_months.primary_identifier]
        eq_(False, six_months_pool.open_access)
        eq_(1, six_months_pool.licenses_owned)
        eq_(0, six_months_pool.licenses_available)
        eq_(1, six_months_pool.patrons_in_hold_queue)
        [lpdm] = six_months_pool.delivery_mechanisms
        eq_(Representation.EPUB_MEDIA_TYPE, lpdm.delivery_mechanism.content_type)
        eq_(DeliveryMechanism.ADOBE_DRM, lpdm.delivery_mechanism.drm_scheme)
        eq_(RightsStatus.IN_COPYRIGHT, lpdm.rights_status.uri)
        [borrow_link] = [l for l in six_months_pool.identifier.links if l.rel == Hyperlink.BORROW]
        eq_('http://localhost:6500/AL/works/URI/http://www.feedbooks.com/item/2493650/borrow', borrow_link.resource.url)

        # This book is currently available.
        [essex_pool] = [p for p in imported_pools if p.identifier == essex.primary_identifier]
        eq_(False, essex_pool.open_access)
        eq_(4, essex_pool.licenses_owned)
        eq_(4, essex_pool.licenses_available)
        eq_(0, essex_pool.patrons_in_hold_queue)
        [lpdm] = essex_pool.delivery_mechanisms
        eq_(Representation.EPUB_MEDIA_TYPE, lpdm.delivery_mechanism.content_type)
        eq_(DeliveryMechanism.ADOBE_DRM, lpdm.delivery_mechanism.drm_scheme)
        eq_(RightsStatus.IN_COPYRIGHT, lpdm.rights_status.uri)
        [borrow_link] = [l for l in essex_pool.identifier.links if l.rel == Hyperlink.BORROW]
        eq_('http://localhost:6500/AL/works/URI/http://www.feedbooks.com/item/1946289/borrow', borrow_link.resource.url)


