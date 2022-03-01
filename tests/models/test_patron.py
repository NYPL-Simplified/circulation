# encoding: utf-8
import pytest
import datetime
from mock import (
    call,
    MagicMock,
)

from ...classifier import Classifier
from ...model import (
    create,
    tuple_to_numericrange,
)
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
from ...util.datetime_helpers import datetime_utc, utc_now


class TestAnnotation:

    def test_set_inactive(self, db_session, create_edition, create_licensepool, create_patron):
        """
        GIVEN: An Annotation
        WHEN:  Setting the Annotation to inactive
        THEN:  Annotation is not active
        """
        patron = create_patron(db_session)
        edition = create_edition(db_session)
        pool = create_licensepool(db_session, edition)
        annotation, _ = create(
            db_session, Annotation,
            patron=patron,
            identifier=pool.identifier,
            motivation=Annotation.IDLING,
            content="The content",
            active=True,
        )
        yesterday = utc_now() - datetime.timedelta(days=1)
        annotation.timestamp = yesterday

        annotation.set_inactive()
        assert annotation.active is False
        assert annotation.content is None
        assert annotation.timestamp > yesterday

    def test_patron_annotations_are_descending(self, db_session, create_edition, create_licensepool, create_patron):
        """
        GIVEN: Two Annotations for a Patron
        WHEN:  Checking the Patron's Annotations
        THEN:  Annotations are ordered by their timestamp
        """
        patron = create_patron(db_session)
        edition = create_edition(db_session)
        pool = create_licensepool(db_session, edition)

        annotation1, _ = create(
            db_session, Annotation,
            patron=patron,
            identifier=pool.identifier,
            motivation=Annotation.IDLING,
            content="The content",
            active=True,
        )
        annotation2, _ = create(
            db_session, Annotation,
            patron=patron,
            identifier=pool.identifier,
            motivation=Annotation.IDLING,
            content="The content",
            active=True,
        )

        yesterday = utc_now() - datetime.timedelta(days=1)
        today = utc_now()
        annotation1.timestamp = yesterday
        annotation2.timestamp = today

        assert 2 == len(patron.annotations)
        assert annotation2 == patron.annotations[0]
        assert annotation1 == patron.annotations[1]


class TestHold:

    def test_on_hold_to_patron(self, db_session, create_edition, create_licensepool, create_patron, default_library):
        """
        GIVEN: A Hold for a Patron through a LicensePool
        WHEN:  The book becomes available
        THEN:  Hold position is 0 and the end of the Hold is set
        """
        now = utc_now()
        later = now + datetime.timedelta(days=1)
        patron = create_patron(db_session)
        edition = create_edition(db_session)
        pool = create_licensepool(db_session, edition)
        default_library.setting(Library.ALLOW_HOLDS).value = True
        hold, is_new = pool.on_hold_to(patron, now, later, 4)

        assert is_new is True
        assert hold.start == now
        assert hold.end == later
        assert hold.position == 4

        # Now update the position to 0. It's the patron's turn
        # to check out the book.
        hold, is_new = pool.on_hold_to(patron, now, later, 0)
        assert is_new is False
        assert hold.start == now
        # The patron has until `hold.end` to actually check out the book.
        assert hold.end == later
        assert hold.position == 0

    def test_on_hold_to_integration_client(self, db_session, create_edition,
                                           create_integration_client, create_licensepool):
        """
        GIVEN: A Hold for an IntegrationClient through a LicensePool
        WHEN:  The book becomes available and the IntegrationClient places two Holds
        THEN:  The two Holds are different
        """
        edition = create_edition(db_session)
        pool = create_licensepool(db_session, edition)
        client = create_integration_client(db_session)
        hold, was_new = pool.on_hold_to(client)

        assert was_new is True
        assert hold.integration_client == client
        assert hold.license_pool == pool

        # Holding the book twice for the same IntegrationClient creates two holds,
        # since they might be for different patrons on the client.
        hold2, was_new = pool.on_hold_to(client)
        assert was_new is True
        assert hold2.integration_client == client
        assert hold2.license_pool == pool
        assert hold2 != hold

    def test_holds_not_allowed(self, db_session, create_edition, create_licensepool, create_patron, default_library):
        """
        GIVEN: A Library that doesn't allow Holds
        WHEN:  Creating a Hold for a Patron
        THEN:  A PolicyException is raised
        """
        patron = create_patron(db_session)
        edition = create_edition(db_session)
        pool = create_licensepool(db_session, edition)

        default_library.setting(Library.ALLOW_HOLDS).value = False
        with pytest.raises(PolicyException) as excinfo:
            pool.on_hold_to(patron, utc_now(), 4)
        assert "Holds are disabled for this library." in str(excinfo.value)

    def test_work(self, db_session, create_patron, create_work):
        """
        GIVEN: A Patron and LicensePool with a Work
        WHEN:  Creating a Hold through the LicensePool for the Patron
        THEN:  The Hold has access to the Work
        """
        # We don't need to test the functionality--that's tested in
        # Loan--just that Hold also has access to .work.
        patron = create_patron(db_session)
        work = create_work(db_session, with_license_pool=True)
        pool = work.license_pools[0]
        hold, _ = pool.on_hold_to(patron)
        assert hold.work == work

    def test_until(self, db_session, create_edition, create_licensepool, create_patron):
        """
        GIVEN: A Hold on an Edition for a Patron through a LicensePool
        WHEN:  Estimating the time at which the book will be available for the Patron
        THEN:  An estimated date is returned
        """
        one_day = datetime.timedelta(days=1)
        two_days = datetime.timedelta(days=2)

        now = utc_now()
        the_past = now - datetime.timedelta(seconds=1)
        the_future = now + two_days

        patron = create_patron(db_session)
        edition = create_edition(db_session)
        pool = create_licensepool(db_session, edition)
        pool.patrons_in_hold_queue = 100
        hold, _ = pool.on_hold_to(patron)
        hold.position = 10

        m = hold.until

        # If the value in Hold.end is in the future, it's used, no
        # questions asked.
        hold.end = the_future
        assert m(object(), object()) == the_future

        # If Hold.end is not specified, or is in the past, it's more
        # complicated.

        # If no default_loan_period or default_reservation_period is
        # specified, a Hold has no particular end date.
        hold.end = the_past
        assert m(None, one_day) is None
        assert m(one_day, None) is None

        hold.end = None
        assert m(None, one_day) is None
        assert m(one_day, None) is None

        # Otherwise, the answer is determined by _calculate_until.
        def _mock__calculate_until(self, *args):
            """Track the arguments passed into _calculate_until."""
            self.called_with = args
            return "mock until"
        old__calculate_until = hold._calculate_until
        Hold._calculate_until = _mock__calculate_until

        assert "mock until" == m(one_day, two_days)

        (calculate_from, position, licenses_available, default_loan_period,
         default_reservation_period) = hold.called_with

        assert (calculate_from-now).total_seconds() < 5
        assert hold.position == position
        assert pool.licenses_available == licenses_available
        assert default_loan_period == one_day
        assert default_reservation_period == two_days

        # If we don't know the patron's position in the hold queue, we
        # assume they're at the end.
        hold.position = None
        assert m(one_day, two_days) == "mock until"
        (calculate_from, position, licenses_available, default_loan_period,
         default_reservation_period) = hold.called_with
        assert pool.patrons_in_hold_queue == position

        Hold._calculate_until = old__calculate_until

    def test_calculate_until(self):
        """
        GIVEN: A Hold
        WHEN:  Estimating the time at which a book will be available to a patron
        THEN:  An estimated date is returned
        """
        start = datetime_utc(2010, 1, 1)

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
        a = Hold._calculate_until(start, 20, 4, default_loan, default_reservation)
        assert a == start + datetime.timedelta(days=(7*5))

        # If I am 21st in line, I need to wait six weeks.
        b = Hold._calculate_until(start, 21, 4, default_loan, default_reservation)
        assert b == start + datetime.timedelta(days=(7*6))

        # If I am 3rd in line, I only need to wait seven days--that's when
        # I'll get the notification message.
        b = Hold._calculate_until(start, 3, 4, default_loan, default_reservation)
        assert b == start + datetime.timedelta(days=7)

        # A new person gets the book every week. Someone has the book now
        # and there are 3 people ahead of me in the queue. I will get
        # the book in 7 days + 3 weeks
        c = Hold._calculate_until(start, 3, 1, default_loan, default_reservation)
        assert c == start + datetime.timedelta(days=(7*4))

        # I'm first in line for 1 book. After 7 days, one copy is
        # released and I'll get my notification.
        a = Hold._calculate_until(start, 1, 1, default_loan, default_reservation)
        assert a == start + datetime.timedelta(days=7)

        # The book is reserved to me. I need to hurry up and check it out.
        d = Hold._calculate_until(start, 0, 1, default_loan, default_reservation)
        assert d == start + datetime.timedelta(days=1)

        # If there are no licenses, I will never get the book.
        e = Hold._calculate_until(start, 10, 0, default_loan, default_reservation)
        assert e is None

    def test_vendor_hold_end_value_takes_precedence_over_calculated_value(
            self, db_session, create_edition, create_licensepool, create_patron):
        """
        GIVEN: A vendor provided estimated availability time for a book
        WHEN:  Estimating the availibility time
        THEN:  The vendor provided time is used in preference to the time we calculate
        """
        now = utc_now()
        tomorrow = now + datetime.timedelta(days=1)

        patron = create_patron(db_session)
        edition = create_edition(db_session)
        pool = create_licensepool(db_session, edition)
        hold, _ = pool.on_hold_to(patron)
        hold.position = 1
        hold.end = tomorrow

        default_loan = datetime.timedelta(days=1)
        default_reservation = datetime.timedelta(days=2)
        assert hold.until(default_loan, default_reservation) == tomorrow

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


class TestLoans:

    def test_open_access_loan_to_patron(self, db_session, create_patron, create_work):
        """
        GIVEN: A Patron and a Work with a LicensePool and a delivery mechanism
        WHEN:  Loaning the Work to the Patron
        THEN:  A Loan is created and fulfilled through the LicensePool and delivery mechanism to the Patron
        """
        patron = create_patron(db_session)
        work = create_work(db_session, with_license_pool=True)
        [pool] = work.license_pools
        pool.is_open_access = True

        # The patron has no active loans.
        assert patron.loans == []

        # Loan them the book
        fulfillment = pool.delivery_mechanisms[0]
        loan, was_new = pool.loan_to(patron, fulfillment=fulfillment)

        # Now they have a loan!
        assert patron.loans == [loan]
        assert loan.patron == patron
        assert loan.license_pool == pool
        assert loan.fulfillment == fulfillment
        assert (utc_now() - loan.start) < datetime.timedelta(seconds=1)

        # TODO: At some future point it may be relevant that loan.end
        # is None here, but before that happens the loan process will
        # become more complicated, so there's no point in writing
        # a bunch of test code now.

        # Try getting another loan for this book.
        loan2, was_new = pool.loan_to(patron)

        # They're the same!
        assert loan2 == loan
        assert was_new is False

    def test_open_access_loan_to_integration_client(
            self, db_session, create_integration_client, create_licensepool, create_work):
        """
        GIVEN: An IntegrationClient and a Work with a LicensePool
        WHEN:  Loaning the Work to the IntegrationClient
        THEN:  A Loan is created for the IntegrationClient
        """
        work = create_work(db_session, with_license_pool=True)
        [pool] = work.license_pools
        client = create_integration_client(db_session)
        loan, was_new = pool.loan_to(client)
        assert was_new is True
        assert loan.integration_client == client
        assert loan.license_pool == pool

        # Loaning the book to the same IntegrationClient twice creates two loans,
        # since these loans could be on behalf of different patrons on the client.
        loan2, was_new = pool.loan_to(client)
        assert was_new is True
        assert loan2.integration_client == client
        assert loan2.license_pool == pool
        assert loan2 != loan

    def test_work(self, db_session, create_patron, create_work):
        """
        GIVEN: A Loan to a Patron through a LicensePool
        WHEN:  Finding the Work for a Loan
        THEN:  The Work is accessible through the LicensePool
        """
        patron = create_patron(db_session)
        work = create_work(db_session, with_license_pool=True)
        pool = work.license_pools[0]

        # The easy cases.
        loan, _ = pool.loan_to(patron)
        assert loan.work == work

        loan.license_pool = None
        assert loan.work is None

        # If pool.work is None but pool.edition.work is valid, we use that.
        loan.license_pool = pool
        pool.work = None
        # Presentation_edition is not representing a lendable object,
        # but it is on a license pool, and a pool has lending capacity.
        assert pool.presentation_edition.work == loan.work

        # If that's also None, we're helpless.
        pool.presentation_edition.work = None
        assert loan.work is None

    def test_library(self, db_session, create_integration_client, create_library,
                     create_patron, create_work, default_library):
        """
        GIVEN: A Patron, a Loan, a Work with a LicensePool, an IntegrationClient, and a Library
        WHEN:  Getting the Loan's Library
        THEN:  The Loan's Library is either the None or a Library
        """
        patron = create_patron(db_session)
        work = create_work(db_session, with_license_pool=True)
        pool = work.license_pools[0]

        loan, _ = pool.loan_to(patron)
        assert default_library == loan.library

        loan.patron = None
        client = create_integration_client(db_session)
        loan.integration_client = client
        assert loan.library is None

        loan.integration_client = None
        assert loan.library is None

        patron.library = create_library(db_session)
        loan.patron = patron
        assert patron.library == loan.library


class TestPatron:

    def test_repr(self, db_session, create_patron):
        """
        GIVEN: A Patron
        WHEN:  Getting the Patron's string representation through __repr__
        THEN:  The string representation is correctly defined
        """
        patron = create_patron(db_session, external_identifier="a patron")

        patron.authorization_expires = datetime_utc(2018, 1, 2, 3, 4, 5)
        patron.last_external_sync = None
        assert (
            "<Patron authentication_identifier=None expires=2018-01-02 sync=None>" ==
            repr(patron))

    def test_identifier_to_remote_service(self, db_session, create_patron, init_datasource_and_genres):
        """
        GIVEN: A Patron and a DataSource
        WHEN:  Getting an Identifier to use when identifying this Patron to a remote service
        THEN:  Returns an Identifier that is either found or created
        """
        # Here's a patron.
        patron = create_patron(db_session)

        # Get identifiers to use when identifying that patron on two
        # different remote services.
        axis = DataSource.AXIS_360
        axis_identifier = patron.identifier_to_remote_service(axis)

        rb_digital = DataSource.lookup(db_session, DataSource.RB_DIGITAL)
        rb_identifier = patron.identifier_to_remote_service(rb_digital)

        # The identifiers are different.
        assert axis_identifier != rb_identifier

        # But they're both 36-character UUIDs.
        assert len(axis_identifier) == 36
        assert len(rb_identifier) == 36

        # They're persistent.
        assert rb_identifier == patron.identifier_to_remote_service(rb_digital)
        assert axis_identifier == patron.identifier_to_remote_service(axis)

        # You can customize the function used to generate the
        # identifier, in case the data source won't accept a UUID as a
        # patron identifier.
        def fake_generator():
            return "fake string"
        bib = DataSource.BIBLIOTHECA
        assert patron.identifier_to_remote_service(bib, fake_generator) == "fake string"

        # Once the identifier is created, specifying a different generator
        # does nothing.
        assert patron.identifier_to_remote_service(bib) == "fake string"
        assert (
            axis_identifier ==
            patron.identifier_to_remote_service(axis, fake_generator))

    def test_set_synchronize_annotations(self, db_session, create_identifier, create_patron):
        """
        GIVEN: A Patron and Annotation
        WHEN:  Determining if a Patron wants to store their Annotations on a library server
        THEN:  Annotations are either stored or not
        """
        # Two patrons.
        p1 = create_patron(db_session)
        p2 = create_patron(db_session)

        identifier = create_identifier(db_session)

        for patron in [p1, p2]:
            # Each patron decides they want to synchronize annotations
            # to a library server.
            assert patron.synchronize_annotations is None
            patron.synchronize_annotations = True

            # Each patron gets one annotation.
            annotation, _ = Annotation.get_one_or_create(
                db_session,
                patron=patron,
                identifier=identifier,
                motivation=Annotation.IDLING,
            )
            annotation.content = "The content for %s" % patron.id,

            assert len(patron.annotations) == 1

        # Patron #1 decides they don't want their annotations stored
        # on a library server after all. This deletes their
        # annotation.
        p1.synchronize_annotations = False
        db_session.commit()
        assert len(p1.annotations) == 0

        # Patron #1 can no longer use Annotation.get_one_or_create.
        pytest.raises(
            ValueError, Annotation.get_one_or_create,
            db_session, patron=p1, identifier=identifier,
            motivation=Annotation.IDLING,
        )

        # Patron #2's annotation is unaffected.
        assert len(p2.annotations) == 1

        # But patron #2 can use Annotation.get_one_or_create.
        _, is_new = Annotation.get_one_or_create(
            db_session, patron=p2, identifier=create_identifier(db_session),
            motivation=Annotation.IDLING,
        )
        assert is_new is True

        # Once you make a decision, you can change your mind, but you
        # can't go back to not having made the decision.
        def try_to_set_none(patron):
            patron.synchronize_annotations = None
        pytest.raises(ValueError, try_to_set_none, p2)

    def test_cascade_delete(self, db_session, create_patron, create_work):
        """
        GIVEN: A Patron with a Loan, Hold, Annotation, and Credential
        WHEN:  Deleting the Patron
        THEN:  The Patron's Loan, Hood, Annotation, and Credential are all deleted
        """
        # Create a patron and check that it has  been created
        patron = create_patron(db_session)
        assert len(db_session.query(Patron).all()) == 1

        # Give the patron a loan, and check that it has been created
        work_for_loan = create_work(db_session, with_license_pool=True)
        pool = work_for_loan.license_pools[0]
        loan, _ = pool.loan_to(patron)
        assert [loan] == patron.loans
        assert len(db_session.query(Loan).all()) == 1

        # Give the patron a hold and check that it has been created
        work_for_hold = create_work(db_session, with_license_pool=True)
        pool = work_for_hold.license_pools[0]
        hold, _ = pool.on_hold_to(patron)
        assert [hold] == patron.holds
        assert len(db_session.query(Hold).all()) == 1

        # Give the patron an annotation and check that it has been created
        annotation, _ = create(db_session, Annotation, patron=patron)
        assert [annotation] == patron.annotations
        assert len(db_session.query(Annotation).all()) == 1

        # Give the patron a credential and check that it has been created
        credential, _ = create(db_session, Credential, patron=patron)
        assert [credential] == patron.credentials
        assert len(db_session.query(Credential).all()) == 1

        # Delete the patron and check that it has been deleted
        db_session.delete(patron)
        assert len(db_session.query(Patron).all()) == 0

        # The patron's loan, hold, annotation, and credential should also be gone
        assert db_session.query(Loan).all() == []
        assert db_session.query(Hold).all() == []
        assert db_session.query(Annotation).all() == []
        assert db_session.query(Credential).all() == []

    def test_loan_activity_max_age(self, db_session, create_patron):
        """
        GIVEN: A Patron
        WHEN:  Checking the loan activity max age
        THEN:  Returns the constant set (15 * 60)
        """
        # Currently, patron.loan_activity_max_age is a constant
        # and cannot be changed.
        patron = create_patron(db_session)
        assert patron.loan_activity_max_age == 15*60

    def test_last_loan_activity_sync(self, db_session, create_patron):
        """
        GIVEN: A Patron with a last loan activity sync attribute
        WHEN:  Accessing the last loan activity sync attribute
        THEN:  The attribute is treated as normal if it's recent, otherwise it's None
        """
        # Verify that last_loan_activity_sync is cleared out
        # beyond a certain point.
        patron = create_patron(db_session)
        now = utc_now()
        max_age = patron.loan_activity_max_age
        recently = now - datetime.timedelta(seconds=max_age/2)
        long_ago = now - datetime.timedelta(seconds=max_age*2)

        # So long as last_loan_activity_sync is relatively recent,
        # it's treated as a normal piece of data.
        patron.last_loan_activity_sync = recently
        assert patron._last_loan_activity_sync == recently
        assert patron.last_loan_activity_sync == recently

        # If it's _not_ relatively recent, attempting to access it
        # clears it out.
        patron.last_loan_activity_sync = long_ago
        assert patron._last_loan_activity_sync == long_ago
        assert patron.last_loan_activity_sync is None
        assert patron._last_loan_activity_sync is None

    def test_root_lane(self, db_session, create_lane, create_patron):
        """
        GIVEN: A Patron and two Lanes
        WHEN:  Getting the Patron's root Lane
        THEN:  A Lane is returned if a library has a root lane or the Patron's external type associates
               them with a specific lane. Otherwise None is returned.
        """
        root_1 = create_lane(db_session, display_name="root_1")
        root_2 = create_lane(db_session, display_name="root_2")

        # If a library has no root lanes, its patrons have no root
        # lanes.
        patron = create_patron(db_session)
        patron.external_type = "x"
        assert patron.root_lane is None

        # Patrons of external type '1' and '2' have a certain root lane.
        root_1.root_for_patron_type = ["1", "2"]

        # Patrons of external type '3' have a different root.
        root_2.root_for_patron_type = ["3"]

        # Flush the database to clear the Library._has_root_lane_cache.
        db_session.flush()

        # A patron with no external type has no root lane.
        assert patron.root_lane is None

        # If a patron's external type associates them with a specific lane, that
        # lane is their root lane.
        patron.external_type = "1"
        assert patron.root_lane == root_1

        patron.external_type = "2"
        assert patron.root_lane == root_1

        patron.external_type = "3"
        assert patron.root_lane == root_2

        # This shouldn't happen, but if two different lanes are the
        # root lane for a single patron type, the one with the lowest
        # database ID is chosen.  This way we avoid denying service to
        # a patron based on a server misconfiguration.
        root_1.root_for_patron_type = ["1", "2", "3"]
        assert patron.root_lane == root_1

    def test_work_is_age_appropriate(self, db_session, create_lane, create_patron):
        """
        GIVEN: A Patron and a Lane with target audiences
        WHEN:  Checking if the audience is age appropriate
        THEN:  Returns True/False depending on the criteria
        """
        # The target audience and age of a patron's root lane controls
        # whether a given book is 'age-appropriate' for them.
        lane = create_lane(db_session)
        lane.audiences = [Classifier.AUDIENCE_CHILDREN, Classifier.AUDIENCE_YOUNG_ADULT]
        lane.target_age = (9, 14)
        lane.root_for_patron_type = ["1"]
        db_session.flush()

        def mock_age_appropriate(work_audience, work_target_age, reader_audience, reader_target_age):
            """Returns True only if reader_audience is the preconfigured
            expected value.
            """
            if reader_audience == self.return_true_for:
                return True
            return False

        patron = create_patron(db_session)
        mock = MagicMock(side_effect=mock_age_appropriate)
        patron.age_appropriate_match = mock
        self.calls = []
        self.return_true_for = None

        # If the patron has no root lane, age_appropriate_match is not
        # even called -- all works are age-appropriate.
        m = patron.work_is_age_appropriate
        work_audience = object()
        work_target_age = object()
        assert m(work_audience, work_target_age) is True
        assert mock.call_count == 0

        # Give the patron a root lane and try again.
        patron.external_type = "1"
        assert m(work_audience, work_target_age) is False

        # age_appropriate_match method was called on
        # each audience associated with the patron's root lane.
        mock.assert_has_calls([
            call(work_audience, work_target_age,
                 Classifier.AUDIENCE_CHILDREN, lane.target_age),
            call(work_audience, work_target_age,
                 Classifier.AUDIENCE_YOUNG_ADULT, lane.target_age)
        ])

        # work_is_age_appropriate() will only return True if at least
        # one of the age_appropriate_match() calls returns True.
        #
        # Simulate this by telling our mock age_appropriate_match() to
        # return True only when passed a specific reader audience. Our
        # Mock lane has two audiences, and at most one can match.
        self.return_true_for = Classifier.AUDIENCE_CHILDREN
        assert m(work_audience, work_target_age) is True

        self.return_true_for = Classifier.AUDIENCE_YOUNG_ADULT
        assert m(work_audience, work_target_age) is True

        self.return_true_for = Classifier.AUDIENCE_ADULT
        assert m(work_audience, work_target_age) is False

    def test_age_appropriate_match(self):
        """
        GIVEN: A Patron
        WHEN:  Checking if the target age of a Work matches the Patron's specifications
        THEN:  Returns True/False depending on the criteria
        """
        # Check whether there's any overlap between a work's target age
        # and a reader's age.
        m = Patron.age_appropriate_match

        ya = Classifier.AUDIENCE_YOUNG_ADULT
        children = Classifier.AUDIENCE_CHILDREN
        adult = Classifier.AUDIENCE_ADULT
        all_ages = Classifier.AUDIENCE_ALL_AGES

        # A reader with no particular audience can see everything.
        assert m(object(), object(), None, object()) is True

        # A reader associated with a non-juvenile audience, such as
        # AUDIENCE_ADULT, can see everything.
        for reader_audience in Classifier.AUDIENCES:
            if reader_audience in Classifier.AUDIENCES_JUVENILE:
                # Tested later.
                continue
            assert m(object(), object(), reader_audience, object()) is True

        # Everyone can see 'all-ages' books.
        for reader_audience in Classifier.AUDIENCES:
            assert m(all_ages, object(), reader_audience, object()) is True

        # Children cannot see YA or adult books.
        for work_audience in (ya, adult):
            assert m(work_audience, object(), children, None) is False

            # This is true even if the "child's" target age is set to
            # a value that would allow for this (as can happen when
            # the patron's root lane is set up to show both children's
            # and YA titles).
            assert m(work_audience, object(), children, (14, 18)) is False

        # YA readers can see any children's title.
        assert m(children, object(), ya, object()) is True

        # A YA reader is treated as an adult (with no reading
        # restrictions) if they have no associated age range, or their
        # age range includes ADULT_AGE_CUTOFF.
        for reader_age in [
            None, 18, (14, 18), tuple_to_numericrange((14, 18))
        ]:
            assert m(adult, object(), ya, reader_age) is True

        # Otherwise, YA readers cannot see books for adults.
        for reader_age in [16, (14, 17)]:
            assert m(adult, object(), ya, reader_age) is False

        # Now let's consider the most complicated cases. First, a
        # child who wants to read a children's book.
        work_audience = children
        for reader_audience in Classifier.AUDIENCES_YOUNG_CHILDREN:
            # If the work has no target age, it's fine (or at least
            # we don't have the information necessary to say it's not
            # fine).
            work_target_age = None
            assert m(work_audience, work_target_age, reader_audience, object()) is True

            # Now give the work a specific target age range.
            for work_target_age in [(5, 7), tuple_to_numericrange((5, 7))]:
                # The lower end of the age range is old enough.
                for age in range(5, 9):
                    for reader_age in (
                        age, (age-1, age), tuple_to_numericrange((age-1, age))
                    ):
                        assert m(work_audience, work_target_age, reader_audience, reader_age) is True

                # Anything lower than that is not.
                for age in range(2, 5):
                    for reader_age in (
                        age, (age-1, age), tuple_to_numericrange((age-1, age))
                    ):
                        assert m(work_audience, work_target_age, reader_audience, reader_age) is False

        # Similar rules apply for a YA reader who wants to read a YA
        # book.
        work_audience = ya
        reader_audience = ya

        # If there's no target age, it's fine (or at least we don't
        # have the information necessary to say it's not fine).
        work_target_age = None
        assert m(work_audience, work_target_age, reader_audience, object()) is True

        # Now give the work a specific target age range.
        for work_target_age in ((14, 16), tuple_to_numericrange((14, 16))):
            # The lower end of the age range is old enough
            for age in range(14, 20):
                for reader_age in (
                    age, (age-1, age), tuple_to_numericrange((age-1, age))
                ):
                    assert m(work_audience, work_target_age, reader_audience, reader_age) is True

            # Anything lower than that is not.
            for age in range(7, 14):
                for reader_age in (
                    age, (age-1, age), tuple_to_numericrange((age-1, age))
                ):
                    assert m(work_audience, work_target_age, reader_audience, reader_age) is False


class TestPatronProfileStorage:

    def test_writable_setting_names(self, db_session, create_patron):
        """
        GIVEN: A Patron and PatronProfileStorage
        WHEN:  Getting the writiable setting names
        THEN:  Only one setting is currently writable
        """
        patron = create_patron(db_session)
        storage = PatronProfileStorage(patron)
        assert storage.writable_setting_names == set([storage.SYNCHRONIZE_ANNOTATIONS])

    def test_profile_document(self, db_session, create_patron):
        """
        GIVEN: A Patron and PatronProfileStorage
        WHEN:  Getting the Profile document
        THEN:  A Profile document is created that represents the Patron's current status
        """
        # synchronize_annotations always shows up as settable, even if
        # the current value is None.
        patron = create_patron(db_session)
        storage = PatronProfileStorage(patron)
        patron.authorization_identifier = "abcd"

        assert patron.synchronize_annotations is None
        assert (
            storage.profile_document ==
            {
             'simplified:authorization_identifier': 'abcd',
             'settings': {'simplified:synchronize_annotations': None}
            }
        )

        patron.synchronize_annotations = True
        patron.authorization_expires = datetime_utc(2016, 1, 1, 10, 20, 30)
        assert (
            storage.profile_document ==
            {
             'simplified:authorization_expires': '2016-01-01T10:20:30Z',
             'simplified:authorization_identifier': 'abcd',
             'settings': {'simplified:synchronize_annotations': True}
            }
        )

    def test_update(self, db_session, create_patron):
        """
        GIVEN: A Patron and PatronProfileStorage
        WHEN:  Updating the storage settings
        THEN:  Setting is updated if it's settable
        """
        patron = create_patron(db_session)
        storage = PatronProfileStorage(patron)
        # This is a no-op.
        storage.update({}, {})
        assert patron.synchronize_annotations is None

        # This is not.
        storage.update({storage.SYNCHRONIZE_ANNOTATIONS: True}, {})
        assert patron.synchronize_annotations is True
