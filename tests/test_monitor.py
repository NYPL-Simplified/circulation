import datetime
import random
from nose.tools import (
    set_trace,
    eq_,
)

from core.testing import (
    DatabaseTest,
)

from core.metadata_layer import TimestampData
from core.model import (
    Annotation,
    Collection,
    CoverageRecord,
    DataSource,
    ExternalIntegration,
    Identifier,
)

from api.monitor import (
    HoldReaper,
    IdlingAnnotationReaper,
    LoanlikeReaperMonitor,
    LoanReaper,
)

from api.odl import (
    ODLAPI,
    SharedODLAPI,
)
from api.testing import MonitorTest


class TestLoanlikeReaperMonitor(DatabaseTest):
    """Tests the loan and hold reapers."""

    def test_source_of_truth_protocols(self):
        """Verify that well-known source of truth protocols
        will be exempt from the reaper.
        """
        for i in (
                ODLAPI.NAME,
                SharedODLAPI.NAME,
                ExternalIntegration.OPDS_FOR_DISTRIBUTORS,
        ):
            assert i in LoanlikeReaperMonitor.SOURCE_OF_TRUTH_PROTOCOLS


    def test_reaping(self):
        # This patron stopped using the circulation manager a long time
        # ago.
        inactive_patron = self._patron()

        # This patron is still using the circulation manager.
        current_patron = self._patron()

        # We're going to give these patrons some loans and holds.
        edition, open_access = self._edition(
            with_license_pool=True, with_open_access_download=True)

        not_open_access_1 = self._licensepool(edition,
            open_access=False, data_source_name=DataSource.OVERDRIVE)
        not_open_access_2 = self._licensepool(edition,
            open_access=False, data_source_name=DataSource.BIBLIOTHECA)
        not_open_access_3 = self._licensepool(edition,
            open_access=False, data_source_name=DataSource.AXIS_360)
        not_open_access_4 = self._licensepool(edition,
            open_access=False, data_source_name=DataSource.RB_DIGITAL)

        # Here's a collection that is the source of truth for its
        # loans and holds, rather than mirroring loan and hold information
        # from some remote source.
        sot_collection = self._collection(
            "Source of Truth",
            protocol=random.choice(LoanReaper.SOURCE_OF_TRUTH_PROTOCOLS)
        )

        edition2 = self._edition(with_license_pool=False)

        sot_lp1 = self._licensepool(
            edition2, open_access=False,
            data_source_name=DataSource.OVERDRIVE,
            collection=sot_collection
        )

        sot_lp2 = self._licensepool(
            edition2, open_access=False,
            data_source_name=DataSource.BIBLIOTHECA,
            collection=sot_collection
        )

        now = datetime.datetime.utcnow()
        a_long_time_ago = now - datetime.timedelta(days=1000)
        not_very_long_ago = now - datetime.timedelta(days=60)
        even_longer = now - datetime.timedelta(days=2000)
        the_future = now + datetime.timedelta(days=1)

        # This loan has expired.
        not_open_access_1.loan_to(
            inactive_patron, start=even_longer, end=a_long_time_ago
        )

        # This hold expired without ever becoming a loan (that we saw).
        not_open_access_2.on_hold_to(
            inactive_patron,
            start=even_longer,
            end=a_long_time_ago
        )

        # This hold has no end date and is older than a year.
        not_open_access_3.on_hold_to(
            inactive_patron, start=a_long_time_ago, end=None,
        )

        # This loan has no end date and is older than 90 days.
        not_open_access_4.loan_to(
            inactive_patron, start=a_long_time_ago, end=None,
        )

        # This loan has no end date, but it's for an open-access work.
        open_access_loan, ignore = open_access.loan_to(
            inactive_patron, start=a_long_time_ago, end=None,
        )

        # This loan has not expired yet.
        not_open_access_1.loan_to(
            current_patron, start=now, end=the_future
        )

        # This hold has not expired yet.
        not_open_access_2.on_hold_to(
            current_patron, start=now, end=the_future
        )

        # This loan has no end date but is pretty recent.
        not_open_access_3.loan_to(
            current_patron, start=not_very_long_ago, end=None
        )

        # This hold has no end date but is pretty recent.
        not_open_access_4.on_hold_to(
            current_patron, start=not_very_long_ago, end=None
        )

        # Reapers will not touch loans or holds from the
        # source-of-truth collection, even ones that have 'obviously'
        # expired.
        sot_loan, ignore = sot_lp1.loan_to(
            inactive_patron, start=a_long_time_ago, end=a_long_time_ago
        )

        sot_hold, ignore = sot_lp2.on_hold_to(
            inactive_patron, start=a_long_time_ago, end=a_long_time_ago
        )

        assert 4 == len(inactive_patron.loans)
        assert 3 == len(inactive_patron.holds)

        assert 2 == len(current_patron.loans)
        assert 2 == len(current_patron.holds)

        # Now we fire up the loan reaper.
        monitor = LoanReaper(self._db)
        monitor.run()

        # All of the inactive patron's loans have been reaped,
        # except for the loans for which the circulation manager is the
        # source of truth (the SOT loan and the open-access loan),
        # which will never be reaped.
        #
        # Holds are unaffected.
        assert set([open_access_loan, sot_loan]) == set(inactive_patron.loans)
        assert 3 == len(inactive_patron.holds)

        # The active patron's loans and holds are unaffected, either
        # because they have not expired or because they have no known
        # expiration date and were created relatively recently.
        assert 2 == len(current_patron.loans)
        assert 2 == len(current_patron.holds)

        # Now fire up the hold reaper.
        monitor = HoldReaper(self._db)
        monitor.run()

        # All of the inactive patron's holds have been reaped,
        # except for the one from the source-of-truth collection.
        # The active patron is unaffected.
        assert [sot_hold] == inactive_patron.holds
        assert 2 == len(current_patron.holds)


class TestIdlingAnnotationReaper(DatabaseTest):

    def test_where_clause(self):

        # Two books.
        ignore, lp1 = self._edition(with_license_pool=True)
        ignore, lp2 = self._edition(with_license_pool=True)

        # Two patrons who sync their annotations.
        p1 = self._patron()
        p2 = self._patron()
        for p in [p1, p2]:
            p.synchronize_annotations = True
        now = datetime.datetime.utcnow()
        not_that_old = now - datetime.timedelta(days=59)
        very_old = now - datetime.timedelta(days=61)

        def _annotation(patron, pool, content, motivation=Annotation.IDLING,
                        timestamp=very_old):
            annotation, ignore = Annotation.get_one_or_create(
                self._db,
                patron=patron,
                identifier=pool.identifier,
                motivation=motivation,
            )
            annotation.timestamp = timestamp
            annotation.content = content
            return annotation

        # The first patron will not be affected by the
        # reaper. Although their annotations are very old, they have
        # an active loan for one book and a hold on the other.
        loan = lp1.loan_to(p1)
        old_loan = _annotation(p1, lp1, "old loan")

        hold = lp2.on_hold_to(p1)
        old_hold = _annotation(p1, lp2, "old hold")

        # The second patron has a very old annotation for the first
        # book. This is the only annotation that will be affected by
        # the reaper.
        reapable = _annotation(p2, lp1, "abandoned")

        # The second patron also has a very old non-idling annotation
        # for the first book, which will not be reaped because only
        # idling annotations are reaped.
        not_idling = _annotation(
            p2, lp1, "not idling", motivation="some other motivation"
        )

        # The second patron has a non-old idling annotation for the
        # second book, which will not be reaped (even though there is
        # no active loan or hold) because it's not old enough.
        new_idling = _annotation(
            p2, lp2, "recent", timestamp=not_that_old
        )
        reaper = IdlingAnnotationReaper(self._db)
        qu = self._db.query(Annotation).filter(reaper.where_clause)
        assert [reapable] == qu.all()
