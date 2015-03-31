from nose.tools import set_trace
from flask import url_for

from core.opds import (
    Annotator,
    AcquisitionFeed,
    OPDSFeed,
)
from core.model import (
    Session
)

class CirculationManagerAnnotator(Annotator):

    def __init__(self, lane, active_loans_by_work={}, active_holds_by_work={}):
        self.lane = lane
        self.active_loans_by_work = active_loans_by_work
        self.active_holds_by_work = active_holds_by_work

    def facet_url(self, order):
        if not self.lane:
            return None
        return url_for(
            'feed', lane=self.lane.name, order=order, _external=True)

    def permalink_for(self, work, license_pool, identifier):
        return url_for('work', urn=identifier.urn, _external=True)

    @classmethod
    def featured_feed_url(cls, lane, order=None):
        return url_for('feed', lane=lane.name, order=order, _external=True)

    @classmethod
    def navigation_feed_url(self, lane):
        if not lane:
            lane_name = None
        else:
            lane_name = lane.name
        return url_for('navigation_feed', lane=lane_name, _external=True)

    def active_licensepool_for(self, work):
        loan = (self.active_loans_by_work.get(work) or
                self.active_holds_by_work.get(work))
        if loan:
            # The active license pool is the one associated with
            # the loan/hold.
            return loan.license_pool
        else:

            # There is no active loan. Use the default logic for
            # determining the active license pool.
            return super(
                CirculationManagerAnnotator, self).active_licensepool_for(work)

    def annotate_work_entry(self, work, active_license_pool, edition, identifier, feed, entry):
        active_loan = self.active_loans_by_work.get(work)
        active_hold = self.active_holds_by_work.get(work)
        identifier = active_license_pool.identifier
        if active_loan:
            entry.extend([feed.loan_tag(active_loan)])
            rel = OPDSFeed.ACQUISITION_REL
        elif active_hold:
            entry.extend([feed.hold_tag(active_hold)])
            if active_hold.position == 0:
                # The patron is at the front of the hold queue and
                # has the ability decision to borrow
                rel = OPDSFeed.BORROW_REL
            else:
                # The patron cannot do anything but wait.
                rel = None
        else:
            # The patron has no existing relationship with this
            # work. Give them the opportunity to check out the work
            # or put it on hold.
            if active_license_pool.open_access:
                # It's an open-access work, so they can just download it.
                rel = OPDSFeed.OPEN_ACCESS_REL
            else:
                # This will transact a loan if there are available
                # licenses, or put the book on hold if there are no
                # available licenses.
                rel = OPDSFeed.BORROW_REL

        if not active_license_pool.open_access:
            entry.extend(feed.license_tags(active_license_pool))

        if rel:
            checkout_url = url_for(
                "checkout", data_source=active_license_pool.data_source.name,
                identifier=identifier.identifier, _external=True)
            feed.add_link_to_entry(entry, rel=rel, href=checkout_url)

    def summary(self, work):
        """Return an HTML summary of this work."""
        if work.summary_text:
            summary = work.summary_text
            if work.summary:
                qualities.append(("Summary quality", work.summary.quality))
        elif work.summary:
            work.summary_text = work.summary.content
            summary = work.summary_text
        else:
            summary = ""
        # summary += "<ul>"
        # for name, value in qualities:
        #     if isinstance(value, basestring):
        #         summary += "<li>%s: %s</li>" % (name, value)
        #     else:
        #         summary += "<li>%s: %.1f</li>" % (name, value)
        # summary += "<li>License Source: %s</li>" % active_license_pool.data_source.name
        # summary += "</ul>"
        return summary


class CirculationManagerLoanAndHoldAnnotator(CirculationManagerAnnotator):

    def permalink_for(self, work, license_pool, identifier):
        ds = license_pool.data_source.name
        return url_for(
            'loan_or_hold_detail', data_source=ds,
            identifier=identifier.identifier, _external=True)

    @classmethod
    def active_loans_for(cls, patron):
        db = Session.object_session(patron)
        url = url_for('active_loans', _external=True)
        active_loans_by_work = {}
        for loan in patron.loans:
            active_loans_by_work[loan.license_pool.work] = loan
        active_holds_by_work = {}
        for hold in patron.holds:
            active_holds_by_work[hold.license_pool.work] = hold
        annotator = cls(None, active_loans_by_work, active_holds_by_work)
        works = patron.works_on_loan_or_on_hold()
        return AcquisitionFeed(db, "Active loans", url, works, annotator)

    @classmethod
    def single_loan_feed(cls, loan):
        db = Session.object_session(loan)
        work = loan.license_pool.work
        url = url_for(
            'loan_or_hold_detail', data_source=loan.license_pool.data_source,
            identifier=loan.license_pool.identifier, _external=True)
        active_loans_by_work = { work : loan }
        annotator = cls(None, active_loans_by_work, {})
        works = [work]
        return AcquisitionFeed(
            db, "Active loan for %s" % work.title, url, works, annotator)

    @classmethod
    def single_hold_feed(cls, hold):
        db = Session.object_session(hold)
        work = hold.license_pool.work
        url = url_for(
            'loan_or_hold_detail', data_source=hold.license_pool.data_source,
            identifier=hold.license_pool.identifier, _external=True)
        active_holds_by_work = { work : hold }
        annotator = cls(None, {}, active_holds_by_work)
        works = [work]
        return AcquisitionFeed(
            db, "Active hold for %s" % work.title, url, works, annotator)
