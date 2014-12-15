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

    def __init__(self, lane, active_loans_by_work={}):
        self.lane = lane
        self.active_loans_by_work = active_loans_by_work

    def facet_url(self, order):
        if not self.lane:
            return None
        return url_for(
            'feed', lane=self.lane.name, order=order, _external=True)

    def permalink_for(self, license_pool):
        identifier = license_pool.identifier
        return url_for("work", identifier_type=identifier.type,
                       identifier=identifier.identifier, _external=True)

    def featured_feed_url(cls, lane, order=None):
        return url_for('feed', lane=lane.name, order=order, _external=True)

    def navigation_feed_url(self, lane):
        if not lane:
            lane_name = None
        else:
            lane_name = lane.name
        return url_for('navigation_feed', lane=lane_name, _external=True)

    def active_licensepool_for(self, work):
        loan = self.active_loans_by_work.get(work)
        if loan:
            # The active license pool is the one associated with
            # the loan.
            return loan.license_pool
        else:
            # There is no active loan. Use the default logic for
            # determining the active license pool.
            return super(
                CirculationManagerAnnotator, self).active_licensepool_for(work)

    def annotate_work_entry(self, work, active_license_pool, feed, entry, links):
        active_loan = self.active_loans_by_work.get(work)
        identifier = active_license_pool.identifier
        if active_loan:
            entry.extend([feed.loan_tag(active_loan)])
            rel = None
        else:
            #Include a checkout URL
            if active_license_pool.open_access:
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
        summary += "<ul>"
        for name, value in qualities:
            if isinstance(value, basestring):
                summary += "<li>%s: %s</li>" % (name, value)
            else:
                summary += "<li>%s: %.1f</li>" % (name, value)
        summary += "<li>License Source: %s</li>" % active_license_pool.data_source.name
        summary += "</ul>"
        return summary


    @classmethod
    def active_loans_for(cls, patron):
        db = Session.object_session(patron)
        url = url_for('active_loans', _external=True)
        active_loans_by_work = {}
        for loan in patron.loans:
            active_loans_by_work[loan.license_pool.work] = loan
        annotator = cls(None, active_loans_by_work)
        works = patron.works_on_loan()
        return AcquisitionFeed(db, "Active loans", url, works, annotator)
