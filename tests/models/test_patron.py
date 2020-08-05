# encoding: utf-8
from nose.tools import (
    assert_raises,
    assert_raises_regexp,
    eq_,
    set_trace,
)
import datetime
from .. import DatabaseTest
from ...model import create
from ...model.credential import Credential
from ...model.datasource import DataSource
from ...model.library import Library
from ...model.licensing import PolicyException
from ...model.patron import (
    Annotation,
    Hold,
    Loan,
    Patron,
    PatronProfileStorage,
)

class TestAnnotation(DatabaseTest):
    def test_set_inactive(self):
        pool = self._licensepool(None)
        annotation, ignore = create(
            self._db, Annotation,
            patron=self._patron(),
            identifier=pool.identifier,
            motivation=Annotation.IDLING,
            content="The content",
            active=True,
        )
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        annotation.timestamp = yesterday

        annotation.set_inactive()
        eq_(False, annotation.active)
        eq_(None, annotation.content)
        assert annotation.timestamp > yesterday

    def test_patron_annotations_are_descending(self):
        pool1 = self._licensepool(None)
        pool2 = self._licensepool(None)
        patron = self._patron()
        annotation1, ignore = create(
            self._db, Annotation,
            patron=patron,
            identifier=pool2.identifier,
            motivation=Annotation.IDLING,
            content="The content",
            active=True,
        )
        annotation2, ignore = create(
            self._db, Annotation,
            patron=patron,
            identifier=pool2.identifier,
            motivation=Annotation.IDLING,
            content="The content",
            active=True,
        )

        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        today = datetime.datetime.now()
        annotation1.timestamp = yesterday
        annotation2.timestamp = today

        eq_(2, len(patron.annotations))
        eq_(annotation2, patron.annotations[0])
        eq_(annotation1, patron.annotations[1])

class TestHold(DatabaseTest):

    def test_on_hold_to(self):
        now = datetime.datetime.utcnow()
        later = now + datetime.timedelta(days=1)
        patron = self._patron()
        edition = self._edition()
        pool = self._licensepool(edition)
        self._default_library.setting(Library.ALLOW_HOLDS).value = True
        hold, is_new = pool.on_hold_to(patron, now, later, 4)
        eq_(True, is_new)
        eq_(now, hold.start)
        eq_(later, hold.end)
        eq_(4, hold.position)

        # Now update the position to 0. It's the patron's turn
        # to check out the book.
        hold, is_new = pool.on_hold_to(patron, now, later, 0)
        eq_(False, is_new)
        eq_(now, hold.start)
        # The patron has until `hold.end` to actually check out the book.
        eq_(later, hold.end)
        eq_(0, hold.position)

        # Make sure we can also hold this book for an IntegrationClient.
        client = self._integration_client()
        hold, was_new = pool.on_hold_to(client)
        eq_(True, was_new)
        eq_(client, hold.integration_client)
        eq_(pool, hold.license_pool)

        # Holding the book twice for the same IntegrationClient creates two holds,
        # since they might be for different patrons on the client.
        hold2, was_new = pool.on_hold_to(client)
        eq_(True, was_new)
        eq_(client, hold2.integration_client)
        eq_(pool, hold2.license_pool)
        assert hold != hold2

    def test_holds_not_allowed(self):
        patron = self._patron()
        edition = self._edition()
        pool = self._licensepool(edition)

        self._default_library.setting(Library.ALLOW_HOLDS).value = False
        assert_raises_regexp(
            PolicyException,
            "Holds are disabled for this library.",
            pool.on_hold_to, patron, datetime.datetime.now(), 4
        )

    def test_work(self):
        # We don't need to test the functionality--that's tested in
        # Loan--just that Hold also has access to .work.
        patron = self._patron()
        work = self._work(with_license_pool=True)
        pool = work.license_pools[0]
        hold, is_new = pool.on_hold_to(patron)
        eq_(work, hold.work)

    def test_until(self):

        one_day = datetime.timedelta(days=1)
        two_days = datetime.timedelta(days=2)

        now = datetime.datetime.utcnow()
        the_past = now - datetime.timedelta(seconds=1)
        the_future = now + two_days

        patron = self._patron()
        pool = self._licensepool(None)
        pool.patrons_in_hold_queue = 100
        hold, ignore = pool.on_hold_to(patron)
        hold.position = 10

        m = hold.until

        # If the value in Hold.end is in the future, it's used, no
        # questions asked.
        hold.end = the_future
        eq_(the_future, m(object(), object()))

        # If Hold.end is not specified, or is in the past, it's more
        # complicated.

        # If no default_loan_period or default_reservation_period is
        # specified, a Hold has no particular end date.
        hold.end = the_past
        eq_(None, m(None, one_day))
        eq_(None, m(one_day, None))

        hold.end = None
        eq_(None, m(None, one_day))
        eq_(None, m(one_day, None))

        # Otherwise, the answer is determined by _calculate_until.
        def _mock__calculate_until(self, *args):
            """Track the arguments passed into _calculate_until."""
            self.called_with = args
            return "mock until"
        old__calculate_until = hold._calculate_until
        Hold._calculate_until = _mock__calculate_until

        eq_("mock until", m(one_day, two_days))

        (calculate_from, position, licenses_available, default_loan_period,
         default_reservation_period) = hold.called_with

        assert (calculate_from-now).total_seconds() < 5
        eq_(hold.position, position)
        eq_(pool.licenses_available, licenses_available)
        eq_(one_day, default_loan_period)
        eq_(two_days, default_reservation_period)

        # If we don't know the patron's position in the hold queue, we
        # assume they're at the end.
        hold.position = None
        eq_("mock until", m(one_day, two_days))
        (calculate_from, position, licenses_available, default_loan_period,
         default_reservation_period) = hold.called_with
        eq_(pool.patrons_in_hold_queue, position)

        Hold._calculate_until = old__calculate_until

    def test_calculate_until(self):
        start = datetime.datetime(2010, 1, 1)

        # The cycle time is one week.
        default_loan = datetime.timedelta(days=6)
        default_reservation = datetime.timedelta(days=1)

        # I'm 20th in line for 4 books.
        #
        # After 7 days, four copies are released and I am 16th in line.
        # After 14 days, those copies are released and I am 12th in line.
        # After 21 days, those copies are released and I am 8th in line.
        # After 28 days, those copies are released and I am 4th in line.
        # After 35 days, those copies are released and get my notification.
        a = Hold._calculate_until(
            start, 20, 4, default_loan, default_reservation)
        eq_(a, start + datetime.timedelta(days=(7*5)))

        # If I am 21st in line, I need to wait six weeks.
        b = Hold._calculate_until(
            start, 21, 4, default_loan, default_reservation)
        eq_(b, start + datetime.timedelta(days=(7*6)))

        # If I am 3rd in line, I only need to wait seven days--that's when
        # I'll get the notification message.
        b = Hold._calculate_until(
            start, 3, 4, default_loan, default_reservation)
        eq_(b, start + datetime.timedelta(days=7))

        # A new person gets the book every week. Someone has the book now
        # and there are 3 people ahead of me in the queue. I will get
        # the book in 7 days + 3 weeks
        c = Hold._calculate_until(
            start, 3, 1, default_loan, default_reservation)
        eq_(c, start + datetime.timedelta(days=(7*4)))

        # I'm first in line for 1 book. After 7 days, one copy is
        # released and I'll get my notification.
        a = Hold._calculate_until(
            start, 1, 1, default_loan, default_reservation)
        eq_(a, start + datetime.timedelta(days=7))

        # The book is reserved to me. I need to hurry up and check it out.
        d = Hold._calculate_until(
            start, 0, 1, default_loan, default_reservation)
        eq_(d, start + datetime.timedelta(days=1))

        # If there are no licenses, I will never get the book.
        e = Hold._calculate_until(
            start, 10, 0, default_loan, default_reservation)
        eq_(e, None)


    def test_vendor_hold_end_value_takes_precedence_over_calculated_value(self):
        """If the vendor has provided an estimated availability time,
        that is used in preference to the availability time we
        calculate.
        """
        now = datetime.datetime.utcnow()
        tomorrow = now + datetime.timedelta(days=1)

        patron = self._patron()
        pool = self._licensepool(edition=None)
        hold, is_new = pool.on_hold_to(patron)
        hold.position = 1
        hold.end = tomorrow

        default_loan = datetime.timedelta(days=1)
        default_reservation = datetime.timedelta(days=2)
        eq_(tomorrow, hold.until(default_loan, default_reservation))

        calculated_value = hold._calculate_until(
            now, hold.position, pool.licenses_available,
            default_loan, default_reservation
        )

        # If the vendor value is not in the future, it's ignored
        # and the calculated value is used instead.
        def assert_calculated_value_used():
            result = hold.until(default_loan, default_reservation)
            assert (result-calculated_value).seconds < 5
        hold.end = now
        assert_calculated_value_used()

        # The calculated value is also used there is no
        # vendor-provided value.
        hold.end = None
        assert_calculated_value_used()

class TestLoans(DatabaseTest):

    def test_open_access_loan(self):
        patron = self._patron()
        work = self._work(with_license_pool=True)
        pool = work.license_pools[0]
        pool.is_open_access = True

        # The patron has no active loans.
        eq_([], patron.loans)

        # Loan them the book
        fulfillment = pool.delivery_mechanisms[0]
        loan, was_new = pool.loan_to(patron, fulfillment=fulfillment)

        # Now they have a loan!
        eq_([loan], patron.loans)
        eq_(loan.patron, patron)
        eq_(loan.license_pool, pool)
        eq_(fulfillment, loan.fulfillment)
        assert (datetime.datetime.utcnow() - loan.start) < datetime.timedelta(seconds=1)

        # TODO: At some future point it may be relevant that loan.end
        # is None here, but before that happens the loan process will
        # become more complicated, so there's no point in writing
        # a bunch of test code now.

        # Try getting another loan for this book.
        loan2, was_new = pool.loan_to(patron)

        # They're the same!
        eq_(loan, loan2)
        eq_(False, was_new)

        # Make sure we can also loan this book to an IntegrationClient.
        client = self._integration_client()
        loan, was_new = pool.loan_to(client)
        eq_(True, was_new)
        eq_(client, loan.integration_client)
        eq_(pool, loan.license_pool)

        # Loaning the book to the same IntegrationClient twice creates two loans,
        # since these loans could be on behalf of different patrons on the client.
        loan2, was_new = pool.loan_to(client)
        eq_(True, was_new)
        eq_(client, loan2.integration_client)
        eq_(pool, loan2.license_pool)
        assert loan != loan2

    def test_work(self):
        """Test the attribute that finds the Work for a Loan or Hold."""
        patron = self._patron()
        work = self._work(with_license_pool=True)
        pool = work.license_pools[0]

        # The easy cases.
        loan, is_new = pool.loan_to(patron)
        eq_(work, loan.work)

        loan.license_pool = None
        eq_(None, loan.work)

        # If pool.work is None but pool.edition.work is valid, we use that.
        loan.license_pool = pool
        pool.work = None
        # Presentation_edition is not representing a lendable object,
        # but it is on a license pool, and a pool has lending capacity.
        eq_(pool.presentation_edition.work, loan.work)

        # If that's also None, we're helpless.
        pool.presentation_edition.work = None
        eq_(None, loan.work)

    def test_library(self):
        patron = self._patron()
        work = self._work(with_license_pool=True)
        pool = work.license_pools[0]

        loan, is_new = pool.loan_to(patron)
        eq_(self._default_library, loan.library)

        loan.patron = None
        client = self._integration_client()
        loan.integration_client = client
        eq_(None, loan.library)

        loan.integration_client = None
        eq_(None, loan.library)

        patron.library = self._library()
        loan.patron = patron
        eq_(patron.library, loan.library)

class TestPatron(DatabaseTest):

    def test_repr(self):

        patron = self._patron(external_identifier="a patron")

        patron.authorization_expires=datetime.datetime(2018, 1, 2, 3, 4, 5)
        patron.last_external_sync=None
        eq_(
            "<Patron authentication_identifier=None expires=2018-01-02 sync=None>",
            repr(patron)
        )

    def test_identifier_to_remote_service(self):

        # Here's a patron.
        patron = self._patron()

        # Get identifiers to use when identifying that patron on two
        # different remote services.
        axis = DataSource.AXIS_360
        axis_identifier = patron.identifier_to_remote_service(axis)

        rb_digital = DataSource.lookup(self._db, DataSource.RB_DIGITAL)
        rb_identifier = patron.identifier_to_remote_service(rb_digital)

        # The identifiers are different.
        assert axis_identifier != rb_identifier

        # But they're both 36-character UUIDs.
        eq_(36, len(axis_identifier))
        eq_(36, len(rb_identifier))

        # They're persistent.
        eq_(rb_identifier, patron.identifier_to_remote_service(rb_digital))
        eq_(axis_identifier, patron.identifier_to_remote_service(axis))

        # You can customize the function used to generate the
        # identifier, in case the data source won't accept a UUID as a
        # patron identifier.
        def fake_generator():
            return "fake string"
        bib = DataSource.BIBLIOTHECA
        eq_("fake string",
            patron.identifier_to_remote_service(bib, fake_generator)
        )

        # Once the identifier is created, specifying a different generator
        # does nothing.
        eq_("fake string",
            patron.identifier_to_remote_service(bib)
        )
        eq_(
            axis_identifier,
            patron.identifier_to_remote_service(axis, fake_generator)
        )

    def test_set_synchronize_annotations(self):
        # Two patrons.
        p1 = self._patron()
        p2 = self._patron()

        identifier = self._identifier()

        for patron in [p1, p2]:
            # Each patron decides they want to synchronize annotations
            # to a library server.
            eq_(None, patron.synchronize_annotations)
            patron.synchronize_annotations = True

            # Each patron gets one annotation.
            annotation, ignore = Annotation.get_one_or_create(
                self._db,
                patron=patron,
                identifier=identifier,
                motivation=Annotation.IDLING,
            )
            annotation.content="The content for %s" % patron.id,

            eq_(1, len(patron.annotations))

        # Patron #1 decides they don't want their annotations stored
        # on a library server after all. This deletes their
        # annotation.
        p1.synchronize_annotations = False
        self._db.commit()
        eq_(0, len(p1.annotations))

        # Patron #1 can no longer use Annotation.get_one_or_create.
        assert_raises(
            ValueError, Annotation.get_one_or_create,
            self._db, patron=p1, identifier=identifier,
            motivation=Annotation.IDLING,
        )

        # Patron #2's annotation is unaffected.
        eq_(1, len(p2.annotations))

        # But patron #2 can use Annotation.get_one_or_create.
        i2, is_new = Annotation.get_one_or_create(
            self._db, patron=p2, identifier=self._identifier(),
            motivation=Annotation.IDLING,
        )
        eq_(True, is_new)

        # Once you make a decision, you can change your mind, but you
        # can't go back to not having made the decision.
        def try_to_set_none(patron):
            patron.synchronize_annotations = None
        assert_raises(ValueError, try_to_set_none, p2)

    def test_cascade_delete(self):
        # Create a patron and check that it has  been created
        patron = self._patron()
        eq_(len(self._db.query(Patron).all()), 1)

        # Give the patron a loan, and check that it has been created
        work_for_loan = self._work(with_license_pool=True)
        pool = work_for_loan.license_pools[0]
        loan, is_new = pool.loan_to(patron)
        eq_([loan], patron.loans)
        eq_(len(self._db.query(Loan).all()), 1)

        # Give the patron a hold and check that it has been created
        work_for_hold = self._work(with_license_pool=True)
        pool = work_for_hold.license_pools[0]
        hold, is_new = pool.on_hold_to(patron)
        eq_([hold], patron.holds)
        eq_(len(self._db.query(Hold).all()), 1)

        # Give the patron an annotation and check that it has been created
        annotation, is_new = create(self._db, Annotation, patron=patron)
        eq_([annotation], patron.annotations)
        eq_(len(self._db.query(Annotation).all()), 1)

        # Give the patron a credential and check that it has been created
        credential, is_new = create(self._db, Credential, patron=patron)
        eq_([credential], patron.credentials)
        eq_(len(self._db.query(Credential).all()), 1)

        # Delete the patron and check that it has been deleted
        self._db.delete(patron)
        eq_(len(self._db.query(Patron).all()), 0)

        # The patron's loan, hold, annotation, and credential should also be gone
        eq_(self._db.query(Loan).all(), [])
        eq_(self._db.query(Hold).all(), [])
        eq_(self._db.query(Annotation).all(), [])
        eq_(self._db.query(Credential).all(), [])

    def test_loan_activity_max_age(self):
        # Currently, patron.loan_activity_max_age is a constant
        # and cannot be changed.
        eq_(30*60, self._patron().loan_activity_max_age)

    def test_last_loan_activity_sync(self):
        # Verify that last_loan_activity_sync is cleared out
        # beyond a certain point.
        patron = self._patron()
        now = datetime.datetime.utcnow()
        max_age = patron.loan_activity_max_age
        recently = now - datetime.timedelta(seconds=max_age/2)
        long_ago = now - datetime.timedelta(seconds=max_age*2)

        # So long as last_loan_activity_sync is relatively recent,
        # it's treated as a normal piece of data.
        patron.last_loan_activity_sync = recently
        eq_(recently, patron._last_loan_activity_sync)
        eq_(recently, patron.last_loan_activity_sync)
        
        # If it's _not_ relatively recent, attempting to access it
        # clears it out.
        patron.last_loan_activity_sync = long_ago
        eq_(long_ago, patron._last_loan_activity_sync)
        eq_(None, patron.last_loan_activity_sync)
        eq_(None, patron._last_loan_activity_sync)


class TestPatronProfileStorage(DatabaseTest):

    def setup(self):
        super(TestPatronProfileStorage, self).setup()
        self.patron = self._patron()
        self.store = PatronProfileStorage(self.patron)

    def test_writable_setting_names(self):
        """Only one setting is currently writable."""
        eq_(set([self.store.SYNCHRONIZE_ANNOTATIONS]),
            self.store.writable_setting_names)

    def test_profile_document(self):
        # synchronize_annotations always shows up as settable, even if
        # the current value is None.
        self.patron.authorization_identifier = "abcd"
        eq_(None, self.patron.synchronize_annotations)
        rep = self.store.profile_document
        eq_(
            {
             'simplified:authorization_identifier': 'abcd',
             'settings': {'simplified:synchronize_annotations': None}
            },
            rep
        )

        self.patron.synchronize_annotations = True
        self.patron.authorization_expires = datetime.datetime(
            2016, 1, 1, 10, 20, 30
        )
        rep = self.store.profile_document
        eq_(
            {
             'simplified:authorization_expires': '2016-01-01T10:20:30Z',
             'simplified:authorization_identifier': 'abcd',
             'settings': {'simplified:synchronize_annotations': True}
            },
            rep
        )

    def test_update(self):
        # This is a no-op.
        self.store.update({}, {})
        eq_(None, self.patron.synchronize_annotations)

        # This is not.
        self.store.update({self.store.SYNCHRONIZE_ANNOTATIONS : True}, {})
        eq_(True, self.patron.synchronize_annotations)
