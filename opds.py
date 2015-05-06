from nose.tools import set_trace
from flask import url_for

from core.opds import (
    Annotator,
    AcquisitionFeed,
    E,
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
        self.lane_by_work = dict()

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

    def block_uri(self, work, license_pool, identifier):
        if not work in self.lane_by_work:
            return None, ""

        lane = self.lane_by_work[work]
        if not lane:
            # I don't think this should ever happen?
            lane_name = None
            url = url_for('acquisition_blocks', lane=None, _external=True)
            title = "All Books"
            return url, title
        if isinstance(lane, basestring):
            # A block URI has been provided directly.
            return lane
        lane_name = lane.name
        # If the lane has sublanes, the URL identifying the block will
        # take the user to another set of blocks for the
        # sublanes. Otherwise it will take the user to a list of the
        # books in the lane by author.
        if lane.sublanes:
            url = url_for('acquisition_blocks', lane=lane.display_name, _external=True)
        else:
            url = url_for('feed', lane=lane.display_name, order='author', _external=True)
        return url, lane_name

    def annotate_work_entry(self, work, active_license_pool, edition, identifier, feed, entry):
        active_loan = self.active_loans_by_work.get(work)
        active_hold = self.active_holds_by_work.get(work)
        identifier = active_license_pool.identifier
        data_source = active_license_pool.data_source

        can_borrow = False
        can_fulfill = False
        can_revoke = False

        if active_loan:
            entry.extend([feed.loan_tag(active_loan)])
            can_fulfill = True
            can_revoke = True
        elif active_hold:
            can_revoke = True
            entry.extend([feed.hold_tag(active_hold)])
            if active_hold.position == 0:
                # The patron is at the front of the hold queue and
                # has the ability decision to borrow
                can_borrow = True
        else:
            # The patron has no existing relationship with this
            # work. Give them the opportunity to check out the work
            # or put it on hold.
            can_borrow = True


        if active_license_pool.open_access:
            rel = OPDSFeed.OPEN_ACCESS_REL
            best_pool, best_link = active_license_pool.best_license_link
            if best_link:
                representation = best_link.representation
                if representation.mirror_url:
                    feed.add_link_to_entry(
                        entry, rel=rel, href=representation.mirror_url,
                        type=representation.media_type)
        else:
            entry.extend(feed.license_tags(active_license_pool))

        if can_fulfill:
            fulfill_url = url_for(
                "fulfill", data_source=data_source.name,
                identifier=identifier.identifier, _external=True)
            feed.add_link_to_entry(entry, rel=OPDSFeed.ACQUISITION_REL,
                                   href=fulfill_url)

        if can_borrow:
            borrow_url = url_for(
                "borrow", data_source=data_source.name,
                identifier=identifier.identifier, _external=True)
            feed.add_link_to_entry(entry, rel=OPDSFeed.BORROW_REL,
                                   href=borrow_url)

        if can_revoke:
            url = url_for(
                'revoke_loan_or_hold', data_source=data_source.name,
                identifier=identifier.identifier, _external=True)

            feed.add_link_to_entry(entry, rel=OPDSFeed.REVOKE_LOAN_REL,
                                   href=url)


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

    # def permalink_for(self, work, license_pool, identifier):
    #     ds = license_pool.data_source.name
    #     return url_for(
    #         'loan_or_hold_detail', data_source=ds,
    #         identifier=identifier.identifier, _external=True)

    @classmethod
    def active_loans_for(cls, patron):
        db = Session.object_session(patron)
        url = url_for('active_loans', _external=True)
        active_loans_by_work = {}
        for loan in patron.loans:
            if loan.license_pool.work:
                active_loans_by_work[loan.license_pool.work] = loan
        active_holds_by_work = {}
        for hold in patron.holds:
            if hold.license_pool.work:
                active_holds_by_work[hold.license_pool.work] = hold
        annotator = cls(None, active_loans_by_work, active_holds_by_work)
        works = patron.works_on_loan_or_on_hold()
        return AcquisitionFeed(db, "Active loans and holds", url, works, annotator)

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
