import logging
import os
import sys

from sqlalchemy import (
    and_,
    or_,
)

from core.monitor import (
    EditionSweepMonitor,
    ReaperMonitor,
)
from core.model import (
    Annotation,
    Collection,
    DataSource,
    Edition,
    ExternalIntegration,
    Hold,
    Identifier,
    LicensePool,
    Loan,
)
from core.util.datetime_helpers import utc_now
from .odl import (
    ODLAPI,
    SharedODLAPI,
)


class LoanlikeReaperMonitor(ReaperMonitor):

    SOURCE_OF_TRUTH_PROTOCOLS = [
        ODLAPI.NAME,
        SharedODLAPI.NAME,
        ExternalIntegration.OPDS_FOR_DISTRIBUTORS,
    ]

    @property
    def where_clause(self):
        """We never want to automatically reap loans or holds for situations
        where the circulation manager is the source of truth. If we
        delete something we shouldn't have, we won't be able to get
        the 'real' information back.

        This means loans of open-access content and loans from
        collections based on a protocol found in
        SOURCE_OF_TRUTH_PROTOCOLS.

        Subclasses will append extra clauses to this filter.
        """
        source_of_truth = or_(
            LicensePool.open_access==True,
            ExternalIntegration.protocol.in_(
                self.SOURCE_OF_TRUTH_PROTOCOLS
            )
        )

        source_of_truth_subquery = self._db.query(self.MODEL_CLASS.id).join(
            self.MODEL_CLASS.license_pool).join(
                LicensePool.collection).join(
                    ExternalIntegration,
                    Collection.external_integration_id==ExternalIntegration.id
                ).filter(
                    source_of_truth
                )
        return ~self.MODEL_CLASS.id.in_(source_of_truth_subquery)


class LoanReaper(LoanlikeReaperMonitor):
    """Remove expired and abandoned loans from the database."""
    MODEL_CLASS = Loan
    MAX_AGE = 90

    @property
    def where_clause(self):
        """Find loans that have either expired, or that were created a long
        time ago and have no definite end date.
        """
        start_field = self.MODEL_CLASS.start
        end_field = self.MODEL_CLASS.end
        superclause = super(LoanReaper, self).where_clause
        now = utc_now()
        expired = end_field < now
        very_old_with_no_clear_end_date = and_(
            start_field < self.cutoff,
            end_field == None
        )
        return and_(superclause, or_(expired, very_old_with_no_clear_end_date))
ReaperMonitor.REGISTRY.append(LoanReaper)


class HoldReaper(LoanlikeReaperMonitor):
    """Remove seemingly abandoned holds from the database."""
    MODEL_CLASS = Hold
    MAX_AGE = 365

    @property
    def where_clause(self):
        """Find holds that were created a long time ago and either have
        no end date or have an end date in the past.

        The 'end date' for a hold is just an estimate, but if the estimate
        is in the future it's better to keep the hold around.
        """
        start_field = self.MODEL_CLASS.start
        end_field = self.MODEL_CLASS.end
        superclause = super(HoldReaper, self).where_clause
        end_date_in_past = end_field < utc_now()
        probably_abandoned = and_(
            start_field < self.cutoff,
            or_(end_field == None, end_date_in_past)
        )
        return and_(superclause, probably_abandoned)
ReaperMonitor.REGISTRY.append(HoldReaper)


class IdlingAnnotationReaper(ReaperMonitor):
    """Remove idling annotations for inactive loans."""

    MODEL_CLASS = Annotation
    TIMESTAMP_FIELD = 'timestamp'
    MAX_AGE = 60

    @property
    def where_clause(self):
        """The annotation must have motivation=IDLING, must be at least 60
        days old (meaning there has been no attempt to read the book
        for 60 days), and must not be associated with one of the
        patron's active loans or holds.
        """
        superclause = super(IdlingAnnotationReaper, self).where_clause

        restrictions = []
        for t in Loan, Hold:
            active_subquery = self._db.query(
                Annotation.id
            ).join(
                t,
                t.patron_id==Annotation.patron_id
            ).join(
                LicensePool,
                and_(LicensePool.id==t.license_pool_id,
                     LicensePool.identifier_id==Annotation.identifier_id)
            )
            restrictions.append(
                ~Annotation.id.in_(active_subquery)
            )
        return and_(
            superclause,
            Annotation.motivation==Annotation.IDLING,
            *restrictions
        )
ReaperMonitor.REGISTRY.append(IdlingAnnotationReaper)
